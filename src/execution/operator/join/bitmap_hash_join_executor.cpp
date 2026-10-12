#include "duckdb/execution/operator/join/bitmap_hash_join_executor.hpp"

#include <atomic>
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// BitmapJoinLocalState
//===--------------------------------------------------------------------===//
void BitmapJoinLocalState::Initialize(idx_t bitmap_size) {
	if (initialized) {
		return;
	}
    // ValidityMask类型
	bitmap.Initialize(bitmap_size);
	bitmap.SetAllInvalid(bitmap_size);
	valid_count = 0;
	initialized = true;
}

void BitmapJoinLocalState::EnsureScratch(idx_t count) {
	const idx_t required_capacity = MaxValue<idx_t>(count, STANDARD_VECTOR_SIZE);
	if (scratch_capacity >= required_capacity) {
		return;
	}
	valid_rows.Initialize(required_capacity);
	rowids.resize(required_capacity);
	scratch_capacity = required_capacity;
}

void BitmapJoinLocalState::InitializeCompact(ClientContext &context, const vector<LogicalType> &types) {
	if (compact_initialized) {
		return;
	}
	compact_collection = make_uniq<ColumnDataCollection>(context, types, ColumnDataAllocatorType::BUFFER_MANAGER_ALLOCATOR);
	compact_collection->InitializeAppend(compact_append_state);
	compact_append_chunk.Initialize(Allocator::Get(context), types);
	compact_initialized = true;
}

//===--------------------------------------------------------------------===//
// Construction
//===--------------------------------------------------------------------===//
static idx_t BitmapJoinTypeWidth(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::VARCHAR:
	case PhysicalType::LIST:
	case PhysicalType::ARRAY:
	case PhysicalType::STRUCT:
		return sizeof(string_t);
	default:
		return MaxValue<idx_t>(GetTypeIdSize(type.InternalType()), 1);
	}
}

const char *BitmapJoinExecutor::PayloadModeString() const {
	switch (payload_mode) {
	case BitmapJoinPayloadMode::DENSE_ROWID:
		return "dense";
	case BitmapJoinPayloadMode::COMPACT_ROWID_RANK:
		return "compact";
	default:
		return "unknown";
	}
}

bool BitmapJoinExecutor::ShouldUseCompactPayload() const {
	// Keep the original dense BHJ path for small/medium PK domains and moderately dense joins.
	// Compact mode pays extra build/finalize work (collection append + rank materialization), so only enable it for
	// sparse, large-domain cases where dense O(C * bitmap_size) payload allocation is likely the bottleneck.
	static constexpr idx_t COMPACT_MIN_BITMAP_SIZE = 4194304; // 4M rowids
	static constexpr idx_t COMPACT_MIN_SPARSITY_FACTOR = 16;  // estimated_build_count <= bitmap_size / 16
	if (join.rhs_output_columns.col_types.empty() || bitmap_size < COMPACT_MIN_BITMAP_SIZE) {
		return false;
	}
	const auto estimated_build_count = join.children[1].get().estimated_cardinality;
	if (estimated_build_count == 0 || estimated_build_count == DConstants::INVALID_INDEX) {
		return false;
	}
	if (estimated_build_count > bitmap_size / COMPACT_MIN_SPARSITY_FACTOR) {
		return false;
	}

	idx_t payload_width = 0;
	bool has_variable_width = false;
	for (auto &type : join.rhs_output_columns.col_types) {
		payload_width += BitmapJoinTypeWidth(type);
		auto internal = type.InternalType();
		has_variable_width = has_variable_width || internal == PhysicalType::VARCHAR || internal == PhysicalType::ARRAY ||
		                     internal == PhysicalType::LIST || internal == PhysicalType::STRUCT;
	}
	payload_width = MaxValue<idx_t>(payload_width, 1);
	if (has_variable_width) {
		return true;
	}

	const double dense_payload_bytes = static_cast<double>(payload_width) * static_cast<double>(bitmap_size);
	const double compact_payload_bytes = static_cast<double>(payload_width) * static_cast<double>(estimated_build_count) +
	                                    static_cast<double>(ValidityMask::EntryCount(bitmap_size)) *
	                                        static_cast<double>(sizeof(idx_t) + sizeof(validity_t));
	return dense_payload_bytes > compact_payload_bytes * 4.0;
}

void BitmapJoinExecutor::InitializePayloadColumns(idx_t alloc_size) {
	if (!payload_columns.empty()) {
		return;
	}
	const idx_t payload_alloc_size = MaxValue<idx_t>(alloc_size, 1);
	for (idx_t i = 0; i < join.rhs_output_columns.col_types.size(); i++) {
		auto &col_type = join.rhs_output_columns.col_types[i];
		auto col = make_uniq<Vector>(col_type, payload_alloc_size);
		FlatVector::Validity(*col).Initialize(payload_alloc_size);
		payload_columns.push_back(std::move(col));
	}
}

void BitmapJoinExecutor::InitializeCompactTypes() {
	compact_collection_types.clear();
	compact_collection_types.push_back(LogicalType::UBIGINT);
	for (auto &type : join.rhs_output_columns.col_types) {
		compact_collection_types.push_back(type);
	}
}

BitmapJoinExecutor::BitmapJoinExecutor(const PhysicalHashJoin &join_p) : join(join_p) {
	auto &resolved = join.bitmap_join_resolved;
	if (!resolved.pk) {
		throw InternalException("BitmapJoinExecutor: use_bitmap_join was set but no PK binding was resolved");
	}
	if (!resolved.build_is_pk_side) {
		throw NotImplementedException("BitmapJoinExecutor: only build-is-PK-side joins are supported (MVP §6.4)");
	}
	if (join.join_type != JoinType::INNER) {
		throw NotImplementedException("BitmapJoinExecutor: only INNER joins are supported (MVP §6.4)");
	}
	if (join.conditions.size() != 1) {
		throw NotImplementedException("BitmapJoinExecutor: only a single equi-condition is supported (MVP §6.4)");
	}

	bitmap_size = resolved.pk->row_count;
	build_rowid_offset = resolved.pk->rowid_offset;

	// Global bitmap: all-invalid, sized to the PK row count.
	global_bitmap.Initialize(bitmap_size);
	global_bitmap.SetAllInvalid(bitmap_size);

	// Reconstruct, for each RHS output column, its source column index within the build chunk.
	for (idx_t i = 0; i < join.rhs_output_columns.col_idxs.size(); i++) {
		const idx_t out_idx = join.rhs_output_columns.col_idxs[i];
		idx_t source_col;
		if (out_idx < join.condition_types.size()) {
			// This output column is a join key; its build-chunk source is the bound reference
			// of the build-side (right) condition expression.
			auto &right = *join.conditions[out_idx].right;
			if (right.GetExpressionClass() != ExpressionClass::BOUND_REF) {
				throw NotImplementedException(
				    "BitmapJoinExecutor: build-side key output is not a direct column reference (MVP §6.4)");
			}
			source_col = right.Cast<BoundReferenceExpression>().index;
		} else {
			// This output column is a payload column.
			const idx_t payload_pos = out_idx - join.condition_types.size();
			source_col = join.payload_columns.col_idxs[payload_pos];
		}
		payload_source_columns.push_back(source_col);

		// 条目8: 分流定长/变长 payload 列。
		//   - 定长列: scatter 写不同 rowid, 天然无竞争, lock-free。
		//   - VARCHAR: 走每线程本地 StringHeap 无锁写入 (见 SinkBitmapDense), 消除全局锁串行化。
		//   - ARRAY/LIST/STRUCT 等其它变长: 仍走全局加锁深拷贝 (深拷贝需操作共享 StringHeap, 且非 Q14 瓶颈)。
		auto internal = join.rhs_output_columns.col_types[i].InternalType();
		if (internal == PhysicalType::VARCHAR) {
			varchar_payload_indices.push_back(i);
		} else if (internal == PhysicalType::ARRAY || internal == PhysicalType::LIST ||
		           internal == PhysicalType::STRUCT) {
			other_var_payload_indices.push_back(i);
			var_payload_locks.push_back(make_uniq<mutex>());
		} else {
			fixed_payload_indices.push_back(i);
		}
	}

	payload_mode = ShouldUseCompactPayload() ? BitmapJoinPayloadMode::COMPACT_ROWID_RANK
	                                      : BitmapJoinPayloadMode::DENSE_ROWID;
	if (payload_mode == BitmapJoinPayloadMode::DENSE_ROWID) {
		InitializePayloadColumns(bitmap_size);
	} else {
		InitializeCompactTypes();
	}
}

//===--------------------------------------------------------------------===//
// Build: rowid extraction + payload scatter
//===--------------------------------------------------------------------===//
// perf/sf5/combine锁 优化 (§建议1 精简): 之前这里维护了一份独立的 touched_words dirty-bitmap，
// 用于让 CombineBitmap 跳过全 0 的 word。但 "某个 word 是否非零" 本就是 local_bitmap 自身数据
// 的信息 (local_data[w] != 0)，不需要在这里的热路径里再额外做一次分支 + 写入来重复记录同样的
// 信息。CombineBitmap 直接读 local_bitmap 的底层数据判断即可，效果完全等价且更省。
template <class T>
static void ExtractBuildRowids(Vector &keys, idx_t count, int64_t offset, idx_t bitmap_size,
                               ValidityMask &local_bitmap, SelectionVector &valid_rows, idx_t *rowids,
                               idx_t &valid_count) {
	UnifiedVectorFormat kdata;
	keys.ToUnifiedFormat(count, kdata);
	const auto data = kdata.GetData<T>();
	idx_t vc = 0;
	for (idx_t i = 0; i < count; i++) {
		const idx_t kidx = kdata.sel->get_index(i);
		if (!kdata.validity.RowIsValid(kidx)) {
			continue;
		}
		// 这里直接使用了全局的offset，也就是每个2048 chunk的数据，实际上都持有了完整长度的local_bitmap
		// 后续通过or合并到全局的bitmap中，这样没有锁的开销，但是引入的构造和合并开销很难对比。
		const int64_t v = static_cast<int64_t>(data[kidx]) - offset; 
		if (v < 0 || static_cast<idx_t>(v) >= bitmap_size) {
			continue;
		}
		const idx_t rowid = static_cast<idx_t>(v);
		local_bitmap.SetValidUnsafe(rowid);
		valid_rows.set_index(vc, i);  //这里记录了哪些行是有效的。
		rowids[vc] = rowid;    //跟上边构成了一个双重索引，即假定i于rowid不一定相同（rowid有可能从1开始），但vc几乎一定与i相同，两个continue几乎不可能走到
		vc++;
	}
	valid_count = vc;
}

static void ExtractBuildRowidsSwitch(Vector &keys, idx_t count, int64_t offset, idx_t bitmap_size,
                                     ValidityMask &local_bitmap, SelectionVector &valid_rows, idx_t *rowids,
                                     idx_t &valid_count) {
	switch (keys.GetType().InternalType()) {
	case PhysicalType::INT8:
		ExtractBuildRowids<int8_t>(keys, count, offset, bitmap_size, local_bitmap, valid_rows, rowids, valid_count);
		break;
	case PhysicalType::INT16:
		ExtractBuildRowids<int16_t>(keys, count, offset, bitmap_size, local_bitmap, valid_rows, rowids, valid_count);
		break;
	case PhysicalType::INT32:
		ExtractBuildRowids<int32_t>(keys, count, offset, bitmap_size, local_bitmap, valid_rows, rowids, valid_count);
		break;
	case PhysicalType::INT64:
		ExtractBuildRowids<int64_t>(keys, count, offset, bitmap_size, local_bitmap, valid_rows, rowids, valid_count);
		break;
	case PhysicalType::UINT8:
		ExtractBuildRowids<uint8_t>(keys, count, offset, bitmap_size, local_bitmap, valid_rows, rowids, valid_count);
		break;
	case PhysicalType::UINT16:
		ExtractBuildRowids<uint16_t>(keys, count, offset, bitmap_size, local_bitmap, valid_rows, rowids, valid_count);
		break;
	case PhysicalType::UINT32:
		ExtractBuildRowids<uint32_t>(keys, count, offset, bitmap_size, local_bitmap, valid_rows, rowids, valid_count);
		break;
	case PhysicalType::UINT64:
		ExtractBuildRowids<uint64_t>(keys, count, offset, bitmap_size, local_bitmap, valid_rows, rowids, valid_count);
		break;
	default:
		throw NotImplementedException("BitmapJoinExecutor: unsupported build key type '%s' (must be integral)",
		                              keys.GetType().ToString());
	}
}

static inline void AtomicSetBitmapRow(std::atomic<validity_t> *bitmap_data, idx_t rowid) {
	idx_t entry_idx, idx_in_entry;
	ValidityMask::GetEntryIndex(rowid, entry_idx, idx_in_entry);
	const auto bit = validity_t(1) << validity_t(idx_in_entry);
	bitmap_data[entry_idx].fetch_or(bit, std::memory_order_relaxed);
}

template <class T>
static void ExtractBuildRowidsAtomic(Vector &keys, idx_t count, int64_t offset, idx_t bitmap_size,
                                     std::atomic<validity_t> *global_bitmap, SelectionVector &valid_rows,
                                     idx_t *rowids, idx_t &valid_count) {
	UnifiedVectorFormat kdata;
	keys.ToUnifiedFormat(count, kdata);
	const auto data = kdata.GetData<T>();
	idx_t vc = 0;
	for (idx_t i = 0; i < count; i++) {
		const idx_t kidx = kdata.sel->get_index(i);
		if (!kdata.validity.RowIsValid(kidx)) {
			continue;
		}
		const int64_t v = static_cast<int64_t>(data[kidx]) - offset;
		if (v < 0 || static_cast<idx_t>(v) >= bitmap_size) {
			continue;
		}
		const idx_t rowid = static_cast<idx_t>(v);
		AtomicSetBitmapRow(global_bitmap, rowid);
		valid_rows.set_index(vc, i);
		rowids[vc] = rowid;
		vc++;
	}
	valid_count = vc;
}

static void ExtractBuildRowidsAtomicSwitch(Vector &keys, idx_t count, int64_t offset, idx_t bitmap_size,
                                           std::atomic<validity_t> *global_bitmap, SelectionVector &valid_rows,
                                           idx_t *rowids, idx_t &valid_count) {
	switch (keys.GetType().InternalType()) {
	case PhysicalType::INT8:
		ExtractBuildRowidsAtomic<int8_t>(keys, count, offset, bitmap_size, global_bitmap, valid_rows, rowids,
		                                valid_count);
		break;
	case PhysicalType::INT16:
		ExtractBuildRowidsAtomic<int16_t>(keys, count, offset, bitmap_size, global_bitmap, valid_rows, rowids,
		                                 valid_count);
		break;
	case PhysicalType::INT32:
		ExtractBuildRowidsAtomic<int32_t>(keys, count, offset, bitmap_size, global_bitmap, valid_rows, rowids,
		                                 valid_count);
		break;
	case PhysicalType::INT64:
		ExtractBuildRowidsAtomic<int64_t>(keys, count, offset, bitmap_size, global_bitmap, valid_rows, rowids,
		                                 valid_count);
		break;
	case PhysicalType::UINT8:
		ExtractBuildRowidsAtomic<uint8_t>(keys, count, offset, bitmap_size, global_bitmap, valid_rows, rowids,
		                                 valid_count);
		break;
	case PhysicalType::UINT16:
		ExtractBuildRowidsAtomic<uint16_t>(keys, count, offset, bitmap_size, global_bitmap, valid_rows, rowids,
		                                  valid_count);
		break;
	case PhysicalType::UINT32:
		ExtractBuildRowidsAtomic<uint32_t>(keys, count, offset, bitmap_size, global_bitmap, valid_rows, rowids,
		                                  valid_count);
		break;
	case PhysicalType::UINT64:
		ExtractBuildRowidsAtomic<uint64_t>(keys, count, offset, bitmap_size, global_bitmap, valid_rows, rowids,
		                                  valid_count);
		break;
	default:
		throw NotImplementedException("BitmapJoinExecutor: unsupported build key type '%s' (must be integral)",
		                              keys.GetType().ToString());
	}
}

template <class T>
static void ScatterFixed(UnifiedVectorFormat &s, const SelectionVector &valid_rows, const idx_t *rowids, idx_t n,
                         Vector &target) {
	const auto sdata = s.GetData<T>();
	const auto tdata = FlatVector::GetData<T>(target);
	auto &tmask = FlatVector::Validity(target);
	for (idx_t k = 0; k < n; k++) {
		const idx_t i = valid_rows.get_index(k);
		const idx_t sidx = s.sel->get_index(i);
		const idx_t rowid = rowids[k];
		if (!s.validity.RowIsValid(sidx)) {
			tmask.SetInvalid(rowid);
			continue;
		}
		tdata[rowid] = sdata[sidx];
	}
}

//! Scatter source[valid_rows[k]] -> target[rowids[k]] for k in [0, n).
static void ScatterColumnUnified(Vector &source, UnifiedVectorFormat &s, const SelectionVector &valid_rows,
                                 const idx_t *rowids, idx_t n, Vector &target) {
	switch (target.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		ScatterFixed<int8_t>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::INT16:
		ScatterFixed<int16_t>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::INT32:
		ScatterFixed<int32_t>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::INT64:
		ScatterFixed<int64_t>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::INT128:
		ScatterFixed<hugeint_t>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::UINT8:
		ScatterFixed<uint8_t>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::UINT16:
		ScatterFixed<uint16_t>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::UINT32:
		ScatterFixed<uint32_t>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::UINT64:
		ScatterFixed<uint64_t>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::UINT128:
		ScatterFixed<uhugeint_t>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::FLOAT:
		ScatterFixed<float>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::DOUBLE:
		ScatterFixed<double>(s, valid_rows, rowids, n, target);
		break;
	case PhysicalType::VARCHAR: {
		const auto sdata = s.GetData<string_t>();
		const auto tdata = FlatVector::GetData<string_t>(target);
		auto &tmask = FlatVector::Validity(target);
		for (idx_t k = 0; k < n; k++) {
			const idx_t i = valid_rows.get_index(k);
			const idx_t sidx = s.sel->get_index(i);
			const idx_t rowid = rowids[k];
			if (!s.validity.RowIsValid(sidx)) {
				tmask.SetInvalid(rowid);
				continue;
			}
			tdata[rowid] = StringVector::AddStringOrBlob(target, sdata[sidx]);
		}
		break;
	}
	default: {
		// Correct (but slow) fallback for nested / unsupported physical types.
		auto &tmask = FlatVector::Validity(target);
		for (idx_t k = 0; k < n; k++) {
			const idx_t i = valid_rows.get_index(k);
			const idx_t rowid = rowids[k];
			auto value = source.GetValue(i);
			if (value.IsNull()) {
				tmask.SetInvalid(rowid);
				continue;
			}
			target.SetValue(rowid, value);
		}
		break;
	}
	}
}

static void ScatterColumn(Vector &source, const SelectionVector &valid_rows, const idx_t *rowids, idx_t n,
                          idx_t src_count, Vector &target) {
	UnifiedVectorFormat s;
	source.ToUnifiedFormat(src_count, s);
	ScatterColumnUnified(source, s, valid_rows, rowids, n, target);
}

void BitmapJoinExecutor::SinkBitmap(ExecutionContext &context, DataChunk &chunk, DataChunk &build_keys,
                                    BitmapJoinLocalState &lstate) {
	if (payload_mode == BitmapJoinPayloadMode::COMPACT_ROWID_RANK) {
		SinkBitmapCompact(context, chunk, build_keys, lstate);
	} else {
		SinkBitmapDense(chunk, build_keys, lstate);
	}
}

void BitmapJoinExecutor::SinkBitmapDense(DataChunk &chunk, DataChunk &build_keys, BitmapJoinLocalState &lstate) {
	lstate.Initialize(bitmap_size);

	const idx_t count = chunk.size();
	if (count == 0) {
		return;
	}

	// 1) Extract rowids and set the thread-local bitmap bits (lock-free hot path).
	lstate.EnsureScratch(count);
	auto &valid_rows = lstate.valid_rows;
	auto &rowids = lstate.rowids;
	idx_t valid_count = 0;
	// 条目3: when a materialized `_rowid` column was injected into the build chunk (PK column
	// is not itself a dense rowid), read it directly instead of reusing the join key value.
	Vector &rowid_vec = (join.bitmap_build_rowid_idx != DConstants::INVALID_INDEX)
	                         ? chunk.data[join.bitmap_build_rowid_idx]
	                         : build_keys.data[0];
	const int64_t rowid_offset =
	    (join.bitmap_build_rowid_idx != DConstants::INVALID_INDEX) ? 0 : build_rowid_offset;
	ExtractBuildRowidsSwitch(rowid_vec, count, rowid_offset, bitmap_size, lstate.bitmap, valid_rows, rowids.data(),
	                         valid_count);
	// perf/sf5/combine锁 优化 (§建议2): 本线程累计有效 build 行数，供 CombineBitmap 汇总到
	// total_valid_count，FinalizeBitmap 借此以 O(线程数) 求和替代 CountValid 全量扫描。
	lstate.valid_count += valid_count;

	if (valid_count == 0 || payload_columns.empty()) {
		return;
	}

	// 2) Scatter the RHS payload columns into the global payload vectors.
	for (idx_t c : fixed_payload_indices) {
		auto &source = chunk.data[payload_source_columns[c]];
		ScatterColumn(source, valid_rows, rowids.data(), valid_count, count, *payload_columns[c]);
	}

	// 3) VARCHAR payload 列: 每线程把字符串深拷贝进本线程的本地 StringHeap 向量, 再无锁把 string_t
	//    写到全局 payload_columns[c] 的对应 rowid 槽位。不同线程写不同 rowid (data 缓冲非重叠) 且只
	//    碰自己的本地堆 → 完全无锁, 消除了 Q14 上 var_payload_locks 把整列深拷贝串行化的瓶颈。
	//    关键: 这里是"深拷贝到本地堆"而非浅引用 source —— source 是临时 build chunk, 其堆生命周期
	//    不归 join 所有; 本地堆在 CombineBitmapDense 时被移入 executor 的 var_payload_heap_keepers
	//    全局保活, string_t 指向的字节在整个 join 期间都有效 (对齐 perfect 的 collection 模型)。
	for (idx_t vpos = 0; vpos < varchar_payload_indices.size(); vpos++) {
		const idx_t c = varchar_payload_indices[vpos];
		auto &source = chunk.data[payload_source_columns[c]];
		auto &target = *payload_columns[c];
		UnifiedVectorFormat s;
		source.ToUnifiedFormat(count, s);
		// 取/建本线程的本地堆向量 (只有本线程访问, 无锁)。
		if (lstate.varchar_payload_heaps.size() <= vpos) {
			lstate.varchar_payload_heaps.resize(vpos + 1);
		}
		if (!lstate.varchar_payload_heaps[vpos]) {
			lstate.varchar_payload_heaps[vpos] =
			    make_uniq<Vector>(join.rhs_output_columns.col_types[c], STANDARD_VECTOR_SIZE);
		}
		auto &heap_vec = *lstate.varchar_payload_heaps[vpos];
		auto sdata = s.GetData<string_t>();
		auto tdata = FlatVector::GetData<string_t>(target);
		auto &tmask = FlatVector::Validity(target);
		for (idx_t k = 0; k < valid_count; k++) {
			const idx_t i = valid_rows.get_index(k);
			const idx_t sidx = s.sel->get_index(i);
			const idx_t rowid = rowids[k];
			if (!s.validity.RowIsValid(sidx)) {
				tmask.SetInvalid(rowid);  // 与 ScatterFixed 同款: 非原子 RMW, 仅 NULL 行触发, Q14 不命中
				continue;
			}
			tdata[rowid] = StringVector::AddStringOrBlob(heap_vec, sdata[sidx]);
		}
	}

	//    其它变长 (ARRAY/LIST/STRUCT): 仍走全局加锁逐行深拷贝 (深拷贝需操作共享 StringHeap)。
	for (idx_t vpos = 0; vpos < other_var_payload_indices.size(); vpos++) {
		const idx_t c = other_var_payload_indices[vpos];
		auto &source = chunk.data[payload_source_columns[c]];
		UnifiedVectorFormat s;
		source.ToUnifiedFormat(count, s);
		lock_guard<mutex> guard(*var_payload_locks[vpos]);
		ScatterColumnUnified(source, s, valid_rows, rowids.data(), valid_count, *payload_columns[c]);
	}
}

void BitmapJoinExecutor::SinkBitmapCompact(ExecutionContext &context, DataChunk &chunk, DataChunk &build_keys,
                                           BitmapJoinLocalState &lstate) {
	const idx_t count = chunk.size();
	if (count == 0) {
		return;
	}

	lstate.EnsureScratch(count);
	lstate.InitializeCompact(context.client, compact_collection_types);
	auto &valid_rows = lstate.valid_rows;
	auto &rowids = lstate.rowids;
	idx_t valid_count = 0;

	Vector &rowid_vec = (join.bitmap_build_rowid_idx != DConstants::INVALID_INDEX)
	                         ? chunk.data[join.bitmap_build_rowid_idx]
	                         : build_keys.data[0];
	const int64_t rowid_offset =
	    (join.bitmap_build_rowid_idx != DConstants::INVALID_INDEX) ? 0 : build_rowid_offset;
	auto *global_data = reinterpret_cast<std::atomic<validity_t> *>(global_bitmap.GetData());
	ExtractBuildRowidsAtomicSwitch(rowid_vec, count, rowid_offset, bitmap_size, global_data, valid_rows, rowids.data(),
	                               valid_count);
	lstate.valid_count += valid_count;

	if (valid_count == 0 || join.rhs_output_columns.col_types.empty()) {
		return;
	}

	auto &append_chunk = lstate.compact_append_chunk;
	append_chunk.Reset();
	auto rowid_data = FlatVector::GetData<uint64_t>(append_chunk.data[0]);
	for (idx_t i = 0; i < valid_count; i++) {
		rowid_data[i] = UnsafeNumericCast<uint64_t>(rowids[i]);
	}
	for (idx_t c = 0; c < payload_source_columns.size(); c++) {
		auto &source = chunk.data[payload_source_columns[c]];
		append_chunk.data[c + 1].Slice(source, valid_rows, valid_count);
	}
	append_chunk.SetCardinality(valid_count);
	lstate.compact_collection->Append(lstate.compact_append_state, append_chunk);
}

void BitmapJoinExecutor::CombineBitmap(BitmapJoinLocalState &lstate) {
	if (payload_mode == BitmapJoinPayloadMode::COMPACT_ROWID_RANK) {
		CombineBitmapCompact(lstate);
	} else {
		CombineBitmapDense(lstate);
	}
}

void BitmapJoinExecutor::CombineBitmapDense(BitmapJoinLocalState &lstate) {
	if (!lstate.initialized) {
		return; // this thread saw no build data
	}
	auto *local_data = lstate.bitmap.GetData();
	if (!local_data) {
		return; // all-invalid: nothing to OR
	}

	// 条目8 (CombineBitmap优化): 方案1(atomic fetch_or) + 方案2(稀疏合并)
	//
	// 方案1: 用 std::atomic<validity_t>::fetch_or 替代 mutex，实现无锁合并。
	// 方案2: 只遍历本线程实际非 0 的 word，跳过全 0 的 word。
	auto *global_data = global_bitmap.GetData();
	auto *atomic_global = reinterpret_cast<std::atomic<validity_t> *>(global_data);
	D_ASSERT(atomic_global);

	const idx_t word_count = ValidityMask::EntryCount(bitmap_size);
	for (idx_t w = 0; w < word_count; w++) {
		if (local_data[w] != 0) {
			atomic_global[w].fetch_or(local_data[w], std::memory_order_relaxed);
		}
	}

	// perf/sf5/combine锁 优化 (§建议2): 汇总本线程累计的有效 build 行数到全局计数，
	// 供 FinalizeBitmap 以 O(1) 读取替代 O(bitmap_size/64) 的 CountValid 全量扫描。
	total_valid_count.fetch_add(lstate.valid_count, std::memory_order_relaxed);

	// 把本线程的本地 VARCHAR 堆移入 executor 全局保活集合, 使 payload_columns 中 string_t 指向的
	// 字节在整个 join 期间有效。这些堆只归本线程所有, 此处独占访问 (安全); 移入共享 keeper 列表时
	// 用 var_payload_keep_lock 保护 (每线程一次性, 不在 build 热路径上)。
	if (!lstate.varchar_payload_heaps.empty()) {
		lock_guard<mutex> guard(var_payload_keep_lock);
		for (auto &heap : lstate.varchar_payload_heaps) {
			var_payload_heap_keepers.push_back(std::move(heap));
		}
		lstate.varchar_payload_heaps.clear();
	}
}

void BitmapJoinExecutor::CombineBitmapCompact(BitmapJoinLocalState &lstate) {
	if (lstate.valid_count == 0) {
		return;
	}
	total_valid_count.fetch_add(lstate.valid_count, std::memory_order_relaxed);
	if (!lstate.compact_collection || lstate.compact_collection->Count() == 0) {
		return;
	}
	lock_guard<mutex> guard(compact_collection_lock);
	if (!compact_build_collection) {
		compact_build_collection = std::move(lstate.compact_collection);
	} else {
		compact_build_collection->Combine(*lstate.compact_collection);
	}
}

static inline idx_t BitmapJoinPopcount(validity_t value) {
	return static_cast<idx_t>(__builtin_popcountll(value));
}

idx_t BitmapJoinExecutor::RowidToCompactIndex(idx_t rowid) const {
	idx_t entry_idx, idx_in_entry;
	ValidityMask::GetEntryIndex(rowid, entry_idx, idx_in_entry);
	const auto word = global_bitmap.GetData()[entry_idx];
	const auto mask = idx_in_entry == 0 ? validity_t(0) : ((validity_t(1) << validity_t(idx_in_entry)) - validity_t(1));
	return compact_word_base[entry_idx] + BitmapJoinPopcount(word & mask);
}

void BitmapJoinExecutor::FinalizeCompactPayload() {
	const idx_t word_count = ValidityMask::EntryCount(bitmap_size);
	compact_word_base.resize(word_count);
	auto *global_data = global_bitmap.GetData();
	idx_t running_count = 0;
	for (idx_t w = 0; w < word_count; w++) {
		compact_word_base[w] = running_count;
		running_count += BitmapJoinPopcount(global_data[w]);
	}
	compact_payload_count = running_count;
	InitializePayloadColumns(compact_payload_count);

	if (!compact_build_collection || compact_build_collection->Count() == 0 || payload_columns.empty()) {
		compact_build_collection.reset();
		return;
	}

	ColumnDataScanState scan_state;
	compact_build_collection->InitializeScan(scan_state, ColumnDataScanProperties::ALLOW_ZERO_COPY);
	DataChunk scan_chunk;
	compact_build_collection->InitializeScanChunk(scan_chunk);
	SelectionVector identity_sel;
	vector<idx_t> compact_indices(STANDARD_VECTOR_SIZE);

	while (compact_build_collection->Scan(scan_state, scan_chunk)) {
		const idx_t count = scan_chunk.size();
		UnifiedVectorFormat rowid_format;
		scan_chunk.data[0].ToUnifiedFormat(count, rowid_format);
		auto rowid_data = rowid_format.GetData<uint64_t>();
		for (idx_t i = 0; i < count; i++) {
			const idx_t ridx = rowid_format.sel->get_index(i);
			if (!rowid_format.validity.RowIsValid(ridx)) {
				continue;
			}
			compact_indices[i] = RowidToCompactIndex(UnsafeNumericCast<idx_t>(rowid_data[ridx]));
		}
		for (idx_t c = 0; c < payload_columns.size(); c++) {
			ScatterColumn(scan_chunk.data[c + 1], identity_sel, compact_indices.data(), count, count, *payload_columns[c]);
		}
	}
	compact_build_collection.reset();
}

void BitmapJoinExecutor::FinalizeBitmap() {
	// 5.1 (perf/sf5/combine锁 / 根因分析-详细版.md §5.1): 稠密 fast-path。
	// bitmap 全部置位 (popcount == bitmap_size) 时, probe 可跳过逐行位测试。
	//
	// perf/sf5/combine锁 优化 (§建议2): 不再对 global_bitmap 做一次全量 CountValid 扫描
	// (O(bitmap_size/64) 次 popcount)，而是直接用 CombineBitmap 阶段各线程原子累加好的
	// total_valid_count。
	is_build_dense = (total_valid_count.load(std::memory_order_relaxed) == bitmap_size);
	if (payload_mode == BitmapJoinPayloadMode::COMPACT_ROWID_RANK) {
		FinalizeCompactPayload();
	}
	ready = true;
}

//===--------------------------------------------------------------------===//
// Probe
//===--------------------------------------------------------------------===//
class BitmapJoinOperatorState : public CachingOperatorState {
public:
	BitmapJoinOperatorState(ClientContext &context, const PhysicalHashJoin &join) : probe_executor(context) {
		auto &allocator = Allocator::Get(context);
		if (!join.lhs_output_columns.col_types.empty()) {
			lhs_output.Initialize(allocator, join.lhs_output_columns.col_types);
		}
		join_keys.Initialize(allocator, join.condition_types);
		for (auto &cond : join.conditions) {
			probe_executor.AddExpression(*cond.left);
		}
		probe_sel_vec.Initialize(STANDARD_VECTOR_SIZE);
		build_sel_vec.Initialize(STANDARD_VECTOR_SIZE);
	}

	DataChunk lhs_output;
	DataChunk join_keys;
	ExpressionExecutor probe_executor;
	SelectionVector probe_sel_vec; // matched probe row indices
	SelectionVector build_sel_vec; // matched build rowids (dictionary indices)
};

unique_ptr<OperatorState> BitmapJoinExecutor::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<BitmapJoinOperatorState>(context.client, join);
}

static inline idx_t RowidToCompactIndex(const ValidityMask &bitmap, const vector<idx_t> &word_base, idx_t rowid) {
	idx_t entry_idx, idx_in_entry;
	ValidityMask::GetEntryIndex(rowid, entry_idx, idx_in_entry);
	const auto word = bitmap.GetData()[entry_idx];
	const auto mask = idx_in_entry == 0 ? validity_t(0) : ((validity_t(1) << validity_t(idx_in_entry)) - validity_t(1));
	return word_base[entry_idx] + BitmapJoinPopcount(word & mask);
}

template <class T>
static void FillProbeSelection(Vector &keys, idx_t count, int64_t offset, idx_t bitmap_size,
                               const ValidityMask &bitmap, bool is_build_dense, const vector<idx_t> *word_base,
                               SelectionVector &probe_sel, SelectionVector &build_sel, idx_t &result_count) {
	UnifiedVectorFormat kdata;
	keys.ToUnifiedFormat(count, kdata);
	const auto data = kdata.GetData<T>();
	const bool use_compact_payload = word_base != nullptr;
	idx_t sel = 0;
	if (is_build_dense) {
		// 5.1 稠密 fast-path: bitmap 全置位, 命中即 rowid, 不访问 bitmap, 无位测试分支。
		// 仅保留 NULL 与越界 (v<0 / v>=bitmap_size) 两个必需的正确性检查, 内部不含 bitmap 位测试。
		for (idx_t i = 0; i < count; i++) {
			const idx_t kidx = kdata.sel->get_index(i);
			if (!kdata.validity.RowIsValid(kidx)) {
				continue;
			}
			const int64_t v = static_cast<int64_t>(data[kidx]) - offset;
			if (v < 0 || static_cast<idx_t>(v) >= bitmap_size) {
				continue;
			}
			const idx_t rowid = static_cast<idx_t>(v);
			probe_sel.set_index(sel, i);
			build_sel.set_index(sel, use_compact_payload ? RowidToCompactIndex(bitmap, *word_base, rowid) : rowid);
			sel++;
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			const idx_t kidx = kdata.sel->get_index(i);
			if (!kdata.validity.RowIsValid(kidx)) {
				continue;
			}
			const int64_t v = static_cast<int64_t>(data[kidx]) - offset;
			if (v < 0 || static_cast<idx_t>(v) >= bitmap_size) {
				continue;
			}
			const idx_t rowid = static_cast<idx_t>(v);
			if (bitmap.RowIsValidUnsafe(rowid)) {
				probe_sel.set_index(sel, i);
				build_sel.set_index(sel, use_compact_payload ? RowidToCompactIndex(bitmap, *word_base, rowid) : rowid);
				sel++;
			}
		}
	}
	result_count = sel;
}

static void FillProbeSelectionSwitch(Vector &keys, idx_t count, int64_t offset, idx_t bitmap_size,
                                     const ValidityMask &bitmap, bool is_build_dense, const vector<idx_t> *word_base,
                                     SelectionVector &probe_sel, SelectionVector &build_sel, idx_t &result_count) {
	switch (keys.GetType().InternalType()) {
	case PhysicalType::INT8:
		FillProbeSelection<int8_t>(keys, count, offset, bitmap_size, bitmap, is_build_dense, word_base, probe_sel,
		                           build_sel, result_count);
		break;
	case PhysicalType::INT16:
		FillProbeSelection<int16_t>(keys, count, offset, bitmap_size, bitmap, is_build_dense, word_base, probe_sel,
		                            build_sel, result_count);
		break;
	case PhysicalType::INT32:
		FillProbeSelection<int32_t>(keys, count, offset, bitmap_size, bitmap, is_build_dense, word_base, probe_sel,
		                            build_sel, result_count);
		break;
	case PhysicalType::INT64:
		FillProbeSelection<int64_t>(keys, count, offset, bitmap_size, bitmap, is_build_dense, word_base, probe_sel,
		                            build_sel, result_count);
		break;
	case PhysicalType::UINT8:
		FillProbeSelection<uint8_t>(keys, count, offset, bitmap_size, bitmap, is_build_dense, word_base, probe_sel,
		                            build_sel, result_count);
		break;
	case PhysicalType::UINT16:
		FillProbeSelection<uint16_t>(keys, count, offset, bitmap_size, bitmap, is_build_dense, word_base, probe_sel,
		                             build_sel, result_count);
		break;
	case PhysicalType::UINT32:
		FillProbeSelection<uint32_t>(keys, count, offset, bitmap_size, bitmap, is_build_dense, word_base, probe_sel,
		                             build_sel, result_count);
		break;
	case PhysicalType::UINT64:
		FillProbeSelection<uint64_t>(keys, count, offset, bitmap_size, bitmap, is_build_dense, word_base, probe_sel,
		                             build_sel, result_count);
		break;
	default:
		throw NotImplementedException("BitmapJoinExecutor: unsupported probe key type '%s' (must be integral)",
		                              keys.GetType().ToString());
	}
}

OperatorResultType BitmapJoinExecutor::ProbeBitmap(ExecutionContext &context, DataChunk &input, DataChunk &result,
                                                   OperatorState &state_p) const {
	auto &state = state_p.Cast<BitmapJoinOperatorState>();

	// LHS output columns (zero-copy reference into the probe chunk).
	state.lhs_output.ReferenceColumns(input, join.lhs_output_columns.col_idxs);
	// 修复 probe 结果组装时的 SelectionVector::Slice 越界 (perf/sf5/combine锁 段错误):
	// 上游算子可能产生 DICTIONARY 类型的 lhs 列, 当它的 selection count 小于本批 probe
	// 行数时, result.Slice(lhs_output_dict, probe_sel, ...) 会做字典链式切片并越界读。
	// 这里 flatten 成 FLAT 向量, 后续切片都是干净的字典, 不会越界。
	state.lhs_output.Flatten();

	// 条目3: when a materialized `*_ref` column was injected into the probe chunk, read it
	// directly and skip evaluating the join-key expression entirely.
	idx_t count;
	Vector *ref_vec;
	int64_t ref_offset;
	if (join.bitmap_probe_ref_idx != DConstants::INVALID_INDEX) {
		count = input.size();
		ref_vec = &input.data[join.bitmap_probe_ref_idx];
		ref_offset = 0;
	} else {
		// Fast path: resolve the probe-side join key (= rowid + offset) and test the bitmap.
		state.join_keys.Reset();
		state.probe_executor.Execute(input, state.join_keys);
		count = state.join_keys.size();
		ref_vec = &state.join_keys.data[0];
		ref_offset = build_rowid_offset;
	}

	idx_t result_count = 0;
	const auto *compact_word_base_ptr =
	    payload_mode == BitmapJoinPayloadMode::COMPACT_ROWID_RANK ? &compact_word_base : nullptr;
	FillProbeSelectionSwitch(*ref_vec, count, ref_offset, bitmap_size, global_bitmap, is_build_dense,
	                         compact_word_base_ptr, state.probe_sel_vec, state.build_sel_vec, result_count);

	// LHS columns: reference directly when every row matched (inner join), else slice.
	if (result_count == count) {
		result.Reference(state.lhs_output);
	} else {
		result.Slice(state.lhs_output, state.probe_sel_vec, result_count, 0);
	}

	// RHS columns: dictionary-reference the materialized payload vectors by rowid.
	const idx_t lhs_count = state.lhs_output.ColumnCount();
	for (idx_t i = 0; i < payload_columns.size(); i++) {
		result.data[lhs_count + i].Slice(*payload_columns[i], state.build_sel_vec, result_count);
	}

	// 条目8 (b_idea/6.4遗漏问题 任务2): append passthrough columns from the probe input after
	// the normal [lhs_output][rhs_payload] columns. These are hidden columns that bypass the
	// intermediate join's [left][right] layout. They come from the probe (LHS) input and must
	// be sliced to match the matched rows (state.probe_sel_vec).
	if (!join.passthrough_lhs_col_idxs.empty() && result_count > 0) {
		idx_t passthrough_start = lhs_count + payload_columns.size();
		for (idx_t i = 0; i < join.passthrough_lhs_col_idxs.size(); i++) {
			result.data[passthrough_start + i].Slice(
			    input.data[join.passthrough_lhs_col_idxs[i]], state.probe_sel_vec, result_count);
		}
	}

	result.SetCardinality(result_count);
	return OperatorResultType::NEED_MORE_INPUT;
}

} // namespace duckdb
