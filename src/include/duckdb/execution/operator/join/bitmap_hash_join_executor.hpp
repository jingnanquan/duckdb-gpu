//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/bitmap_hash_join_executor.hpp
//
// Bitmap-Join (BHJ) executor (design doc §6.4, module M-E).
//
// The BitmapJoinExecutor implements an equi-join between a primary-key
// (dimension) build side and a foreign-key (fact) probe side WITHOUT building a
// JoinHashTable. The build side rowids are recorded in a dense ValidityMask
// (the "bitmap"); the probe side tests a single bit per row. The build side RHS
// output columns are materialized once into per-rowid payload vectors, so the
// probe can reference them as dictionary vectors (zero-copy gather).
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class PhysicalHashJoin;

enum class BitmapJoinPayloadMode : uint8_t { DENSE_ROWID, COMPACT_ROWID_RANK };

//! Per-thread build state, held inside HashJoinLocalSinkState. Each build thread
//! accumulates set rowids into its own bitmap with no synchronization
//! (SetValidUnsafe); the thread-local bitmaps are OR-merged in CombineBitmap.
struct BitmapJoinLocalState {
	//! Allocate the thread-local bitmap (all-invalid) lazily. Safe to call repeatedly.
	void Initialize(idx_t bitmap_size);
	//! Reuse per-chunk build scratch buffers instead of allocating them for every SinkBitmap call.
	void EnsureScratch(idx_t count);
	//! Initialize per-thread compact build collection used by sparse/compact payload mode.
	void InitializeCompact(ClientContext &context, const vector<LogicalType> &types);

	//! all-invalid mask sized to the PK table row count; bit i == 1 means build row i passed.
	ValidityMask bitmap;
	bool initialized = false;

	SelectionVector valid_rows;
	vector<idx_t> rowids;
	idx_t scratch_capacity = 0;

	unique_ptr<ColumnDataCollection> compact_collection;
	ColumnDataAppendState compact_append_state;
	DataChunk compact_append_chunk;
	bool compact_initialized = false;

	//! perf/sf5/combine锁 优化: 本线程累计的有效 build 行数 (跨该线程处理过的所有 chunk 累加)。
	//! CombineBitmap 时汇总到 BitmapJoinExecutor::total_valid_count，用来在 FinalizeBitmap 里
	//! 以 O(线程数) 的求和替代对 global_bitmap 的一次全量 CountValid 扫描 (见 §5.1 稠密判定)。
	//! 依赖 BHJ 的前提: build 侧是 PK(唯一)侧, 同一 rowid 不会被置位两次, 故
	//! sum(valid_count) == bitmap_size <=> popcount(global_bitmap) == bitmap_size。
	idx_t valid_count = 0;

	//! 稠密模式 VARCHAR payload: 每个 build 线程的本地 StringHeap 向量 (每 VARCHAR 列一个)。
	//! 字符串字节深拷贝进各自的本地堆, 再写全局 payload_columns 的 string_t (指向本地堆),
	//! 从而多线程写各自堆 → 无锁; build 结束后在 CombineBitmapDense 移入 executor 全局保活。
	vector<unique_ptr<Vector>> varchar_payload_heaps;
};

//! Global Bitmap-Join executor, owned by HashJoinGlobalSinkState. Holds the merged
//! bitmap and the materialized RHS payload columns (one Vector per output column,
//! each sized to the PK row count and indexed by rowid).
class BitmapJoinExecutor {
public:
	explicit BitmapJoinExecutor(const PhysicalHashJoin &join);

	//===------------------------------------------------------------------===//
	// Build
	//===------------------------------------------------------------------===//
	//! Record the build rowids of `chunk` into the thread-local bitmap and scatter
	//! the RHS payload columns into the global payload vectors.
	//! `build_keys` is the result of evaluating the build-side join key expression
	//! (i.e. join_keys.data[0] carries the PK column value).
	void SinkBitmap(ExecutionContext &context, DataChunk &chunk, DataChunk &build_keys, BitmapJoinLocalState &lstate);
	//! OR-merge a thread-local bitmap into the global bitmap (called under the sink lock).
	void CombineBitmap(BitmapJoinLocalState &lstate);
	//! Mark the build as ready (no partitioning / pointer table needed).
	void FinalizeBitmap();

	//===------------------------------------------------------------------===//
	// Probe
	//===------------------------------------------------------------------===//
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const;
	OperatorResultType ProbeBitmap(ExecutionContext &context, DataChunk &input, DataChunk &result,
	                                OperatorState &state) const;

	//===------------------------------------------------------------------===//
	// Diagnostics
	//===------------------------------------------------------------------===//
	idx_t BitmapSize() const {
		return bitmap_size;
	}
	bool IsReady() const {
		return ready;
	}
	const char *PayloadModeString() const;

private:
	void InitializePayloadColumns(idx_t alloc_size);
	void InitializeCompactTypes();
	bool ShouldUseCompactPayload() const;
	void SinkBitmapDense(DataChunk &chunk, DataChunk &build_keys, BitmapJoinLocalState &lstate);
	void SinkBitmapCompact(ExecutionContext &context, DataChunk &chunk, DataChunk &build_keys,
	                       BitmapJoinLocalState &lstate);
	void CombineBitmapDense(BitmapJoinLocalState &lstate);
	void CombineBitmapCompact(BitmapJoinLocalState &lstate);
	void FinalizeCompactPayload();
	idx_t RowidToCompactIndex(idx_t rowid) const;

private:
	const PhysicalHashJoin &join;

	//! Number of rows in the PK table => number of bits in the bitmap.
	idx_t bitmap_size = 0;
	//! rowid = pk_value - build_rowid_offset (1-based PK columns reuse rowid_offset == 1).
	int64_t build_rowid_offset = 0;

	BitmapJoinPayloadMode payload_mode = BitmapJoinPayloadMode::DENSE_ROWID;

	//! Union of all build-side rowids (filled during build).
	ValidityMask global_bitmap;
	//! Materialized RHS output columns; dense mode indexes by rowid, compact mode indexes by bitmap-rank payload id.
	vector<unique_ptr<Vector>> payload_columns;
	//! For each RHS output column i, the source column index within the build chunk.
	vector<idx_t> payload_source_columns;

	//! Compact mode collection schema: [rowid][rhs output columns...].
	vector<LogicalType> compact_collection_types;
	unique_ptr<ColumnDataCollection> compact_build_collection;
	mutex compact_collection_lock;
	vector<idx_t> compact_word_base;
	idx_t compact_payload_count = 0;

	//! 条目8: payload 列按物理类型分流。定长列 scatter 写不同 rowid 位置，lock-free 安全；
	//! VARCHAR 走每线程本地 StringHeap 无锁写入 (消除全局锁串行化); 其它变长(ARRAY/LIST/STRUCT)
	//! 仍走全局加锁深拷贝 (非 Q14 瓶颈)。
	vector<idx_t> fixed_payload_indices;
	//! VARCHAR payload 列: 走每线程本地 StringHeap 无锁写入。
	vector<idx_t> varchar_payload_indices;
	//! 其它变长 payload 列 (ARRAY/LIST/STRUCT 等): 仍走全局加锁深拷贝。
	vector<idx_t> other_var_payload_indices;
	//! One lock per "other" variable-width payload column to avoid serializing independent StringHeaps.
	vector<unique_ptr<mutex>> var_payload_locks;  // 与 other_var_payload_indices 对齐
	//! 各 build 线程本地 VARCHAR 堆的全局保活集合: CombineBitmapDense 时从 local state 移入。
	//! 存放着 payload_columns 中 string_t 实际指向的字符串字节, 必须跟随整个 join 生命周期。
	vector<unique_ptr<Vector>> var_payload_heap_keepers;
	mutex var_payload_keep_lock;

	bool ready = false;
	//! 5.1 (perf/sf5/combine锁 / 根因分析-详细版.md §5.1): 稠密 fast-path 标志。
	//! FinalizeBitmap 时若 bitmap 全部置位 (popcount == bitmap_size) 则置 true，
	//! probe 据此跳过逐行位测试（与 perfect hash join 稠密 fast-path 对称）。
	bool is_build_dense = false;
	//! perf/sf5/combine锁 优化 (§建议2): 各线程 BitmapJoinLocalState::valid_count 之和，
	//! 由 CombineBitmap 原子累加。BHJ 的前提是 build 侧为 PK(唯一)侧，故同一 rowid 不会被
	//! 两个线程重复置位，sum(valid_count) == bitmap_size 等价于 popcount(global_bitmap) ==
	//! bitmap_size；FinalizeBitmap 据此以 O(线程数) 的读取替代对 global_bitmap 的一次全量
	//! CountValid 扫描 (O(bitmap_size/64))。
	std::atomic<idx_t> total_valid_count {0};
};

} // namespace duckdb
