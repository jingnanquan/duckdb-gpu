#include "join_microbench.hpp"

#include "duckdb/catalog/catalog_entry/bitmap_join_meta.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/join/bitmap_hash_join_executor.hpp"
#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"
#include "duckdb/execution/unchained_hash_table.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <thread>

namespace duckdb {

//===--------------------------------------------------------------------===//
// MurmurHash64 求逆
//===--------------------------------------------------------------------===//
uint64_t MurmurHash64Inverse(uint64_t h) {
	static const uint64_t C = 0xd6e8feb86659fd93ULL;
	// C 在 mod 2^64 下的乘法逆元 (Newton 迭代, 每轮有效位翻倍)
	static const uint64_t C_INV = []() -> uint64_t {
		uint64_t inv = 1; // 对 mod 2 正确
		for (idx_t i = 0; i < 6; i++) {
			inv *= 2ULL - C * inv;
		}
		return inv;
	}();

	auto undo_shift_xor = [](uint64_t x) -> uint64_t {
		const uint64_t high = x & 0xFFFFFFFF00000000ULL;
		return high | ((x ^ (high >> 32)) & 0xFFFFFFFFULL);
	};

	// MurmurHash64: A(x) = x ^ (x>>32), B(x) = x * C, 正向为 A B A B A (5 步)
	// 所以逆是 A⁻¹ B⁻¹ A⁻¹ B⁻¹ A⁻¹: 3 次 undo_shift_xor + 2 次乘逆元
	return undo_shift_xor(undo_shift_xor(undo_shift_xor(h) * C_INV) * C_INV);
}

//===--------------------------------------------------------------------===//
// 小工具
//===--------------------------------------------------------------------===//
namespace {

using Clock = std::chrono::steady_clock;

double ElapsedMs(Clock::time_point begin) {
	return std::chrono::duration<double, std::milli>(Clock::now() - begin).count();
}

template <class FUNC>
void RunParallel(idx_t n_threads, idx_t n_items, FUNC &&func) {
	if (n_items == 0) {
		return;
	}
	n_threads = MaxValue<idx_t>(MinValue<idx_t>(n_threads, n_items), 1);
	if (n_threads <= 1) {
		func(0, 0, n_items);
		return;
	}
	vector<std::thread> threads;
	threads.reserve(n_threads);
	const idx_t per_thread = (n_items + n_threads - 1) / n_threads;
	for (idx_t t = 0; t < n_threads; t++) {
		const idx_t from = MinValue<idx_t>(t * per_thread, n_items);
		const idx_t to = MinValue<idx_t>(from + per_thread, n_items);
		threads.emplace_back([&func, t, from, to]() { func(t, from, to); });
	}
	for (auto &thread : threads) {
		thread.join();
	}
}

//! 与 JoinHashTable::PointerTableCapacity() 完全一致的容量公式, 供"构造重复 hash"时使用
idx_t BenchPointerTableCapacity(idx_t count) {
	const auto capacity = NextPowerOfTwo(static_cast<idx_t>(static_cast<double>(count) *
	                                                        JoinHashTable::DEFAULT_LOAD_FACTOR));
	return MaxValue<idx_t>(capacity, JoinHashTable::MINIMUM_CAPACITY);
}

idx_t BitsForCapacity(idx_t capacity) {
	idx_t bits = 0;
	while ((idx_t(1) << bits) < capacity) {
		bits++;
	}
	return bits;
}

//! 写入一个整数 key (按 key 的物理宽度)
void WriteKey(Vector &vec, const LogicalType &type, idx_t index, int64_t value) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
		FlatVector::GetData<int8_t>(vec)[index] = static_cast<int8_t>(value);
		break;
	case PhysicalType::INT16:
		FlatVector::GetData<int16_t>(vec)[index] = static_cast<int16_t>(value);
		break;
	case PhysicalType::INT32:
		FlatVector::GetData<int32_t>(vec)[index] = static_cast<int32_t>(value);
		break;
	case PhysicalType::INT64:
		FlatVector::GetData<int64_t>(vec)[index] = value;
		break;
	default:
		throw NotImplementedException("JoinMicroBench: unsupported key type %s", type.ToString());
	}
}

int64_t ReadKey(Vector &vec, const LogicalType &type, idx_t index) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
		return FlatVector::GetData<int8_t>(vec)[index];
	case PhysicalType::INT16:
		return FlatVector::GetData<int16_t>(vec)[index];
	case PhysicalType::INT32:
		return FlatVector::GetData<int32_t>(vec)[index];
	case PhysicalType::INT64:
		return FlatVector::GetData<int64_t>(vec)[index];
	default:
		throw NotImplementedException("JoinMicroBench: unsupported key type %s", type.ToString());
	}
}

Value MakeKeyValue(const LogicalType &type, int64_t value) {
	switch (type.InternalType()) {
	case PhysicalType::INT8:
		return Value::TINYINT(static_cast<int8_t>(value));
	case PhysicalType::INT16:
		return Value::SMALLINT(static_cast<int16_t>(value));
	case PhysicalType::INT32:
		return Value::INTEGER(static_cast<int32_t>(value));
	case PhysicalType::INT64:
		return Value::BIGINT(value);
	default:
		return Value::BIGINT(value);
	}
}

//! 列求和 (用于跨算法的结果校验和)。输出向量的实际物理类型始终是 payload 的类型 (INTEGER)。
int64_t SumColumnInt64(DataChunk &chunk, idx_t col) {
	UnifiedVectorFormat vdata;
	chunk.data[col].ToUnifiedFormat(chunk.size(), vdata);
	int64_t sum = 0;
	const auto count = chunk.size();
	switch (chunk.data[col].GetType().InternalType()) {
	case PhysicalType::INT32: {
		auto data = vdata.GetData<int32_t>();
		for (idx_t i = 0; i < count; i++) {
			const auto k = vdata.sel->get_index(i);
			if (vdata.validity.RowIsValid(k)) {
				sum += data[k];
			}
		}
		break;
	}
	case PhysicalType::INT64: {
		auto data = vdata.GetData<int64_t>();
		for (idx_t i = 0; i < count; i++) {
			const auto k = vdata.sel->get_index(i);
			if (vdata.validity.RowIsValid(k)) {
				sum += data[k];
			}
		}
		break;
	}
	default:
		break;
	}
	return sum;
}

//! 汇总耗时: join_only = build + finalize + probe, total = scan + join_only
void FinishTimings(JoinBenchResult &result) {
	result.join_ms = result.build_ms + result.finalize_ms + result.probe_ms;
	result.total_ms = result.scan_ms + result.join_ms;
}

//! CanDoPerfectHashJoin 返回 false 时给出精确原因 (判据照抄 perfect_hash_join_executor.cpp)
string PerfectJoinSkipReason(const PhysicalHashJoin &op, idx_t build_count, int64_t min_value, int64_t max_value) {
	static constexpr int64_t MAX_BUILD_SIZE = 1048576;
	if (op.join_type != JoinType::INNER || op.conditions.size() != 1 ||
	    !TypeIsInteger(op.conditions[0].left->return_type.InternalType())) {
		return "非 INNER / 非单列整型等值条件";
	}
	const int64_t range = max_value - min_value;
	if (range < 0) {
		return "max < min";
	}
	if (range > MAX_BUILD_SIZE) {
		return "build_range=" + to_string(range) + " > " + to_string(MAX_BUILD_SIZE);
	}
	if (static_cast<int64_t>(build_count) > range) {
		return "count=" + to_string(build_count) + " > range=" + to_string(range);
	}
	return "未知";
}

//! splitmix64: 便宜且确定的"每行一个随机数"
inline uint64_t BenchRng(uint64_t seed, idx_t row) {
	uint64_t x = seed + static_cast<uint64_t>(row) * 0x9E3779B97F4A7C15ULL;
	x ^= x >> 30;
	x *= 0xBF58476D1CE4E5B9ULL;
	x ^= x >> 27;
	x *= 0x94D049BB133111EBULL;
	x ^= x >> 31;
	return x;
}

} // namespace

//===--------------------------------------------------------------------===//
// 名字
//===--------------------------------------------------------------------===//
string JoinBenchAlgoName(JoinBenchAlgo algo) {
	switch (algo) {
	case JoinBenchAlgo::HASH_JOIN:
		return "hash-join";
	case JoinBenchAlgo::PERFECT_JOIN:
		return "perfect-hash-join";
	case JoinBenchAlgo::BITMAP_JOIN:
		return "bitmap-join";
	case JoinBenchAlgo::UNCHAINED_JOIN:
		return "unchained-join";
	default:
		return "unknown";
	}
}

string JoinBenchKeyPatternName(JoinBenchKeyPattern pattern) {
	switch (pattern) {
	case JoinBenchKeyPattern::UNIQUE_DENSE:
		return "unique_dense";
	case JoinBenchKeyPattern::UNIQUE_SPARSE:
		return "unique_sparse";
	case JoinBenchKeyPattern::DUPLICATE_KEYS:
		return "duplicate_keys";
	case JoinBenchKeyPattern::HASH_COLLISION:
		return "hash_collision";
	case JoinBenchKeyPattern::CONSTANT_KEY:
		return "constant_key";
	case JoinBenchKeyPattern::RUNS_OF_SAME_KEY:
		return "runs_of_same_key";
	case JoinBenchKeyPattern::UNCHAINED_SLOT_COLLISION:
		return "unchained_slot_collision";
	default:
		return "unknown";
	}
}

bool JoinBenchConfig::BitmapJoinEligible() const {
	// BHJ 的 build 侧必须是 PK(唯一)侧, 且 rowid = key - key_offset 必须稠密落在 [0, PK 行数)
	return build_pattern == JoinBenchKeyPattern::UNIQUE_DENSE && key_stride == 1;
}

//===--------------------------------------------------------------------===//
// 结果
//===--------------------------------------------------------------------===//
double JoinBenchResult::ProbeThroughputMrowsPerSec() const {
	if (probe_ms <= 0) {
		return 0;
	}
	return static_cast<double>(probe_rows) / probe_ms / 1000.0;
}

string JoinBenchResult::ToString() const {
	std::ostringstream ss;
	ss << std::fixed << std::setprecision(2);
	ss << algo_name << ": ";
	if (!applicable) {
		ss << "N/A (" << note << ")";
		return ss.str();
	}
	ss << "scan=" << scan_ms << "ms build=" << build_ms << "ms finalize=" << finalize_ms
	   << "ms probe=" << probe_ms << "ms join_only=" << join_ms << "ms total=" << total_ms << "ms"
	   << " | rows(build/probe/matched)=" << build_rows << "/" << probe_rows << "/" << matched_rows
	   << " | " << ProbeThroughputMrowsPerSec() << " M probe-rows/s";
	if (!note.empty()) {
		ss << " | " << note;
	}
	return ss.str();
}

//===--------------------------------------------------------------------===//
// 算子构造
//===--------------------------------------------------------------------===//

//! 只提供"输出类型 + 基数"的占位子算子, 不参与执行 (chunk 由框架直接喂给 join)
class BenchFeederOperator : public PhysicalOperator {
public:
	BenchFeederOperator(PhysicalPlan &plan, vector<LogicalType> types, idx_t cardinality)
	    : PhysicalOperator(plan, PhysicalOperatorType::DUMMY_SCAN, std::move(types), cardinality) {
	}
};

JoinMicroBench::JoinMicroBench(ClientContext &context_p, const JoinBenchConfig &config_p)
    : context(context_p), config(config_p), plan(Allocator::DefaultAllocator()) {
	config.build_rows = MaxValue<idx_t>(config.build_rows, 1);
	config.probe_rows = MaxValue<idx_t>(config.probe_rows, 1);
	config.build_distinct = MaxValue<idx_t>(config.build_distinct, 1);
	config.key_stride = MaxValue<idx_t>(config.key_stride, 1);
	config.chunk_size = MinValue<idx_t>(config.chunk_size, STANDARD_VECTOR_SIZE);
	config.build_threads = MaxValue<idx_t>(config.build_threads, 1);
	config.probe_threads = MaxValue<idx_t>(config.probe_threads, 1);
	config.probe_hit_ratio = std::min(std::max(config.probe_hit_ratio, 0.0), 1.0);
	config.probe_hot_fraction = std::min(std::max(config.probe_hot_fraction, 0.0), 1.0);

	// HASH_COLLISION 需要知道 pointer-table 有多少位
	collision_bucket_bits = BitsForCapacity(BenchPointerTableCapacity(config.build_rows));
	collision_keys_per_group = MaxValue<idx_t>(config.build_rows / MaxValue<idx_t>(config.build_distinct, 1), 1);
	unchained_slot_bits = UnchainedHashTable::SlotBitsForCount(config.build_rows);
	unchained_slot_shift = 48 - unchained_slot_bits;

	BuildOperator();
	MaterializeDataset();
}

JoinMicroBench::~JoinMicroBench() {
}

void JoinMicroBench::BuildOperator() {
	// schema: build/probe 都是 [key][payload...], payload 用 INTEGER
	dataset.build_types.clear();
	dataset.build_types.push_back(config.key_type);
	for (idx_t i = 0; i < config.build_payload_cols; i++) {
		dataset.build_types.push_back(LogicalType::INTEGER);
	}
	dataset.probe_types.clear();
	dataset.probe_types.push_back(config.key_type);
	for (idx_t i = 0; i < config.probe_payload_cols; i++) {
		dataset.probe_types.push_back(LogicalType::INTEGER);
	}

	auto &probe_child = plan.Make<BenchFeederOperator>(dataset.probe_types, config.probe_rows);
	auto &build_child = plan.Make<BenchFeederOperator>(dataset.build_types, config.build_rows);

	// 输出类型 = [probe 列][build key][build payload], 与 PhysicalHashJoin 的投影规则一致
	logical_op.types = dataset.probe_types;
	logical_op.types.insert(logical_op.types.end(), dataset.build_types.begin(), dataset.build_types.end());

	JoinCondition condition;
	condition.comparison = ExpressionType::COMPARE_EQUAL;
	condition.left = make_uniq<BoundReferenceExpression>(config.key_type, 0);
	condition.right = make_uniq<BoundReferenceExpression>(config.key_type, 0);
	vector<JoinCondition> conditions;
	conditions.push_back(std::move(condition));

	auto &op = plan.Make<PhysicalHashJoin>(logical_op, probe_child, build_child, std::move(conditions),
	                                       JoinType::INNER, config.probe_rows);
	join = &op.Cast<PhysicalHashJoin>();

	// 真正的输出类型由 PhysicalHashJoin 自己算出来
	dataset.output_types = join->lhs_output_columns.col_types;
	dataset.output_types.insert(dataset.output_types.end(), join->rhs_output_columns.col_types.begin(),
	                            join->rhs_output_columns.col_types.end());

	// BHJ 的 PK 绑定 (BitmapJoinExecutor 只保存指针, 所以要活得比它久)
	pk_binding.pk_table = "bench_dim";
	pk_binding.pk_column = "pk";
	pk_binding.rowid_column = "pk";
	pk_binding.rowid_offset = config.key_offset;
	pk_binding.row_count = config.PKRowCount();
	join->bitmap_join_resolved.pk = &pk_binding;
	join->bitmap_join_resolved.fk = nullptr;
	join->bitmap_join_resolved.build_is_pk_side = true;
	join->use_bitmap_join = true;
}

//===--------------------------------------------------------------------===//
// key 生成
//===--------------------------------------------------------------------===//
int64_t JoinMicroBench::BuildKey(idx_t row) const {
	switch (config.build_pattern) {
	case JoinBenchKeyPattern::UNIQUE_DENSE:
		return config.key_offset + static_cast<int64_t>(row);
	case JoinBenchKeyPattern::UNIQUE_SPARSE:
		return config.key_offset + static_cast<int64_t>(row) * static_cast<int64_t>(config.key_stride);
	case JoinBenchKeyPattern::DUPLICATE_KEYS:
		return config.key_offset + static_cast<int64_t>(row % config.build_distinct);
	case JoinBenchKeyPattern::CONSTANT_KEY:
		return config.key_offset;
	case JoinBenchKeyPattern::RUNS_OF_SAME_KEY:
		return config.key_offset + static_cast<int64_t>(row / MaxValue<idx_t>(config.build_distinct, 1));
	case JoinBenchKeyPattern::HASH_COLLISION: {
		// 目标 hash: [63:48] = 固定 salt, [47:bucket_bits] = 组内序号, [bucket_bits-1:0] = 桶号
		// => 同一组内所有 key 的 (hash & bitmask) 与 salt 完全相同 => 线性探测链最长
		const idx_t group = MinValue<idx_t>(row / collision_keys_per_group,
		                                   (idx_t(1) << collision_bucket_bits) - 1);
		const idx_t index_in_group = row % collision_keys_per_group;
		const uint64_t target_hash = (static_cast<uint64_t>(0x1234) << 48) |
		                             (static_cast<uint64_t>(index_in_group) << collision_bucket_bits) |
		                             static_cast<uint64_t>(group);
		return static_cast<int64_t>(MurmurHash64Inverse(target_hash));
	}
	case JoinBenchKeyPattern::UNCHAINED_SLOT_COLLISION: {
		// 目标 hash: [63:48] = 任意 salt, [47:unchained_slot_shift] = 冲突组号,
		// [unchained_slot_shift-1:0] = 行号(保证 hash 互不相同)
		// => 同组的所有 key 落进 unchained 的同一个 slot, 且 Bloom tag (bit[0:10]) 也不同
		const idx_t group = MinValue<idx_t>(row / collision_keys_per_group,
		                                   (idx_t(1) << unchained_slot_bits) - 1);
		const uint64_t low = static_cast<uint64_t>(row) & ((uint64_t(1) << unchained_slot_shift) - 1);
		const uint64_t target_hash = (static_cast<uint64_t>(0x1234) << 48) |
		                             (static_cast<uint64_t>(group) << unchained_slot_shift) | low;
		return static_cast<int64_t>(MurmurHash64Inverse(target_hash));
	}
	default:
		return config.key_offset + static_cast<int64_t>(row);
	}
}

int64_t JoinMicroBench::ProbeKey(idx_t row) const {
	const auto rng = BenchRng(config.seed, row);
	const bool hit = config.probe_hit_ratio >= 1.0 ||
	                 (static_cast<double>(rng & 0xFFFFFF) / static_cast<double>(0x1000000)) < config.probe_hit_ratio;
	if (!hit) {
		// 一定不在 build 域内的 key (key_offset 之下)
		return config.key_offset - 1 - static_cast<int64_t>((rng >> 24) % 1024);
	}
	const idx_t hot_rows = MaxValue<idx_t>(
	    static_cast<idx_t>(static_cast<double>(config.build_rows) * config.probe_hot_fraction), 1);
	const idx_t build_row = static_cast<idx_t>((rng >> 24) % hot_rows);
	return BuildKey(build_row);
}

//! build 侧某个 key 出现了几次 (INNER join 的放大倍数)
int64_t JoinMicroBench::BuildKeyMultiplicity(int64_t key) const {
	switch (config.build_pattern) {
	case JoinBenchKeyPattern::UNIQUE_DENSE: {
		const int64_t row = key - config.key_offset;
		return (row >= 0 && row < static_cast<int64_t>(config.build_rows)) ? 1 : 0;
	}
	case JoinBenchKeyPattern::UNIQUE_SPARSE: {
		const int64_t delta = key - config.key_offset;
		if (delta < 0 || delta % static_cast<int64_t>(config.key_stride) != 0) {
			return 0;
		}
		const int64_t row = delta / static_cast<int64_t>(config.key_stride);
		return row < static_cast<int64_t>(config.build_rows) ? 1 : 0;
	}
	case JoinBenchKeyPattern::DUPLICATE_KEYS: {
		const int64_t j = key - config.key_offset;
		if (j < 0 || j >= static_cast<int64_t>(config.build_distinct)) {
			return 0;
		}
		const int64_t n = static_cast<int64_t>(config.build_rows);
		if (j >= n) {
			return 0;
		}
		return (n - 1 - j) / static_cast<int64_t>(config.build_distinct) + 1;
	}
	case JoinBenchKeyPattern::CONSTANT_KEY:
		return key == config.key_offset ? static_cast<int64_t>(config.build_rows) : 0;
	case JoinBenchKeyPattern::RUNS_OF_SAME_KEY: {
		const int64_t j = key - config.key_offset;
		const int64_t run = static_cast<int64_t>(config.build_distinct);
		const int64_t n = static_cast<int64_t>(config.build_rows);
		if (j < 0 || j * run >= n) {
			return 0;
		}
		return MinValue<int64_t>(run, n - j * run);
	}
	case JoinBenchKeyPattern::HASH_COLLISION:
	case JoinBenchKeyPattern::UNCHAINED_SLOT_COLLISION: {
		// key 唯一, 用二分判定是否存在
		if (build_keys_sorted.empty()) {
			return 0;
		}
		return std::binary_search(build_keys_sorted.begin(), build_keys_sorted.end(), key) ? 1 : 0;
	}
	default:
		return 0;
	}
}

void JoinMicroBench::FillBuildChunk(DataChunk &chunk, idx_t first_row, idx_t count) const {
	chunk.Reset();
	for (idx_t i = 0; i < count; i++) {
		const idx_t row = first_row + i;
		WriteKey(chunk.data[0], config.key_type, i, BuildKey(row));
	}
	for (idx_t c = 1; c < chunk.ColumnCount(); c++) {
		auto data = FlatVector::GetData<int32_t>(chunk.data[c]);
		for (idx_t i = 0; i < count; i++) {
			data[i] = static_cast<int32_t>(first_row + i);
		}
	}
	chunk.SetCardinality(count);
}

void JoinMicroBench::FillProbeChunk(DataChunk &chunk, idx_t first_row, idx_t count) const {
	chunk.Reset();
	for (idx_t i = 0; i < count; i++) {
		const idx_t row = first_row + i;
		WriteKey(chunk.data[0], config.key_type, i, ProbeKey(row));
	}
	for (idx_t c = 1; c < chunk.ColumnCount(); c++) {
		auto data = FlatVector::GetData<int32_t>(chunk.data[c]);
		for (idx_t i = 0; i < count; i++) {
			data[i] = static_cast<int32_t>(first_row + i);
		}
	}
	chunk.SetCardinality(count);
}

idx_t JoinMicroBench::ChecksumColumn() const {
	const idx_t lhs_count = join->lhs_output_columns.col_idxs.size();
	const idx_t rhs_count = join->rhs_output_columns.col_idxs.size();
	if (rhs_count == 0) {
		return 0;
	}
	return lhs_count + (rhs_count > 1 ? 1 : 0);
}

void JoinMicroBench::MaterializeDataset() {
	auto &data = dataset;
	data.build_chunks.clear();
	data.probe_chunks.clear();
	data.build_rows = config.build_rows;
	data.probe_rows = config.probe_rows;

	const auto begin = Clock::now();

	// build 侧 key 物化一份 (用于 HASH_COLLISION 的二分查找 + 统计 min/max)
	build_keys.resize(config.build_rows);
	int64_t min_key = NumericLimits<int64_t>::Maximum();
	int64_t max_key = NumericLimits<int64_t>::Minimum();
	for (idx_t r = 0; r < config.build_rows; r++) {
		const auto key = BuildKey(r);
		build_keys[r] = key;
		min_key = MinValue(min_key, key);
		max_key = MaxValue(max_key, key);
	}
	build_keys_sorted.clear();
	if (config.build_pattern == JoinBenchKeyPattern::HASH_COLLISION ||
	    config.build_pattern == JoinBenchKeyPattern::UNCHAINED_SLOT_COLLISION) {
		build_keys_sorted = build_keys;
		std::sort(build_keys_sorted.begin(), build_keys_sorted.end());
	}
	data.build_min = min_key;
	data.build_max = max_key;
	if (config.perfect_use_pk_domain && config.build_pattern == JoinBenchKeyPattern::UNIQUE_DENSE) {
		data.perfect_min = config.key_offset;
		data.perfect_max = config.key_offset + static_cast<int64_t>(config.PKRowCount()) - 1;
	} else {
		data.perfect_min = min_key;
		data.perfect_max = max_key;
	}

	// 物化 chunk (这一步就是被计时的 "scan")
	const auto chunk_size = config.chunk_size;
	const idx_t build_chunk_count = (config.build_rows + chunk_size - 1) / chunk_size;
	data.build_chunks.reserve(build_chunk_count);
	for (idx_t c = 0; c < build_chunk_count; c++) {
		auto chunk = make_uniq<DataChunk>();
		chunk->Initialize(Allocator::DefaultAllocator(), data.build_types);
		const idx_t first_row = c * chunk_size;
		const idx_t count = MinValue<idx_t>(chunk_size, config.build_rows - first_row);
		FillBuildChunk(*chunk, first_row, count);
		data.build_chunks.push_back(std::move(chunk));
	}

	const idx_t probe_chunk_count = (config.probe_rows + chunk_size - 1) / chunk_size;
	data.probe_chunks.reserve(probe_chunk_count);
	for (idx_t c = 0; c < probe_chunk_count; c++) {
		auto chunk = make_uniq<DataChunk>();
		chunk->Initialize(Allocator::DefaultAllocator(), data.probe_types);
		const idx_t first_row = c * chunk_size;
		const idx_t count = MinValue<idx_t>(chunk_size, config.probe_rows - first_row);
		FillProbeChunk(*chunk, first_row, count);
		data.probe_chunks.push_back(std::move(chunk));
	}

	data.scan_ms = ElapsedMs(begin);

	// 期望的 INNER join 输出行数 (与被测实现无关, 独立算出)
	idx_t expected = 0;
	for (idx_t r = 0; r < config.probe_rows; r++) {
		expected += static_cast<idx_t>(BuildKeyMultiplicity(ProbeKey(r)));
	}
	data.expected_matches = expected;
}

//===--------------------------------------------------------------------===//
// Hash Join / Perfect Hash Join 的公共 build
//===--------------------------------------------------------------------===//
namespace {

//! 与 HashJoinLocalSinkState 等价的每线程 sink 状态
struct BenchHashJoinLocalSink {
	BenchHashJoinLocalSink(const PhysicalHashJoin &op, ClientContext &context) : join_key_executor(context) {
		auto &allocator = BufferAllocator::Get(context);
		for (auto &cond : op.conditions) {
			join_key_executor.AddExpression(*cond.right);
		}
		join_keys.Initialize(allocator, op.condition_types);
		payload_chunk.Initialize(allocator, op.payload_columns.col_types);
		hash_table = op.InitializeHashTable(context);
		hash_table->GetSinkCollection().InitializeAppendState(append_state);
	}

	PartitionedTupleDataAppendState append_state;
	ExpressionExecutor join_key_executor;
	DataChunk join_keys;
	DataChunk payload_chunk;
	unique_ptr<JoinHashTable> hash_table;
};

//! 与 HashJoinOperatorState 等价的每线程 probe 状态
struct BenchHashJoinProbeState {
	BenchHashJoinProbeState(ClientContext &context, const PhysicalHashJoin &op, JoinHashTable &ht)
	    : probe_executor(context), scan_structure(ht, join_key_state) {
		auto &allocator = BufferAllocator::Get(context);
		lhs_join_keys.Initialize(allocator, op.condition_types);
		lhs_output.Initialize(allocator, op.lhs_output_columns.col_types);
		TupleDataCollection::InitializeChunkState(join_key_state, op.condition_types);
		for (auto &cond : op.conditions) {
			probe_executor.AddExpression(*cond.left);
		}
	}

	DataChunk lhs_join_keys;
	TupleDataChunkState join_key_state;
	DataChunk lhs_output;
	ExpressionExecutor probe_executor;
	JoinHashTable::ScanStructure scan_structure;
	JoinHashTable::ProbeState probe_state;
};

} // namespace

void JoinMicroBench::SinkHashJoin(unique_ptr<JoinHashTable> &ht, const vector<unique_ptr<DataChunk>> &chunks,
                                  double &build_ms_out, vector<idx_t> &partition_offsets) {
	const auto begin = Clock::now();
	auto &op = *join;

	const idx_t thread_count = MinValue<idx_t>(config.build_threads, chunks.size());
	vector<unique_ptr<JoinHashTable>> local_hash_tables(thread_count);

	//=== Sink (多线程) ===//
	RunParallel(thread_count, chunks.size(), [&](idx_t thread_idx, idx_t from, idx_t to) {
		ThreadContext thread_context(context);
		ExecutionContext exec_context(context, thread_context, nullptr);
		BenchHashJoinLocalSink local(op, context);
		for (idx_t c = from; c < to; c++) {
			auto &chunk = *chunks[c];
			// 完全照抄 PhysicalHashJoin::Sink(): 解 build key + reference payload + Build
			local.join_keys.Reset();
			local.join_key_executor.Execute(chunk, local.join_keys);
			if (op.payload_columns.col_types.empty()) {
				local.payload_chunk.SetCardinality(chunk.size());
			} else {
				local.payload_chunk.ReferenceColumns(chunk, op.payload_columns.col_idxs);
			}
			local.hash_table->Build(local.append_state, local.join_keys, local.payload_chunk);
		}
		// Combine(): 把本线程的 HT 交给全局
		local.hash_table->GetSinkCollection().FlushAppendState(local.append_state);
		local_hash_tables[thread_idx] = std::move(local.hash_table);
	});

	//=== Combine: 把各线程的 HT 合并进全局 (对应 PhysicalHashJoin::Finalize 的内存路径) ===//
	for (auto &local : local_hash_tables) {
		if (local) {
			ht->Merge(*local);
		}
	}
	local_hash_tables.clear();

	// 记录每个 radix 分区的起始行号: Unpartition() 会把分区按序拼接成一个平面集合,
	// unchained 的构建正是靠这个「分区 p 覆盖一段连续 slot 区间」的性质做到零同步并行。
	partition_offsets.clear();
	auto &partitions = ht->GetSinkCollection().GetPartitions();
	partition_offsets.reserve(partitions.size() + 1);
	idx_t offset = 0;
	partition_offsets.push_back(0);
	for (auto &partition : partitions) {
		offset += partition->Count();
		partition_offsets.push_back(offset);
	}

	build_ms_out = ElapsedMs(begin);
}

void JoinMicroBench::FinalizeHashTable(JoinHashTable &ht, idx_t build_threads) {
	// 对应 HashJoinGlobalSinkState::ScheduleFinalize() + HashJoinTableInitEvent
	// + HashJoinFinalizeEvent 两个 event 的 task 体, 这里同步执行。
	if (ht.Count() == 0) {
		ht.finalized = true;
		return;
	}
	ht.AllocatePointerTable();
	ht.InitializePointerTable(0, ht.capacity);

	static constexpr idx_t PARALLEL_CONSTRUCT_THRESHOLD = 1048576;
	const bool parallel = build_threads > 1 && ht.Count() >= PARALLEL_CONSTRUCT_THRESHOLD;
	ht.Finalize(0, ht.GetDataCollection().ChunkCount(), parallel);
	ht.finalized = true;
}

JoinBenchResult JoinMicroBench::ProbeHashJoin(JoinHashTable &ht, double &probe_ms_out, int64_t &checksum_out) {
	JoinBenchResult result;
	auto &op = *join;
	const idx_t checksum_col = ChecksumColumn();
	std::atomic<idx_t> matched_rows {0};
	std::atomic<int64_t> checksum {0};

	const auto begin = Clock::now();
	const idx_t thread_count = MinValue<idx_t>(config.probe_threads, dataset.probe_chunks.size());
	RunParallel(thread_count, dataset.probe_chunks.size(), [&](idx_t, idx_t from, idx_t to) {
		ThreadContext thread_context(context);
		ExecutionContext exec_context(context, thread_context, nullptr);
		BenchHashJoinProbeState state(context, op, ht);
		DataChunk result_chunk;
		result_chunk.Initialize(Allocator::DefaultAllocator(), dataset.output_types);
		idx_t local_matched = 0;
		int64_t local_checksum = 0;

		for (idx_t c = from; c < to; c++) {
			auto &input = *dataset.probe_chunks[c];
			// 完全照抄 PhysicalHashJoin::ExecuteInternal() 的 HAVE_MORE_OUTPUT 循环
			while (true) {
				if (state.scan_structure.is_null) {
					state.lhs_join_keys.Reset();
					state.probe_executor.Execute(input, state.lhs_join_keys);
					ht.Probe(state.scan_structure, state.lhs_join_keys, state.join_key_state, state.probe_state);
				}
				state.lhs_output.ReferenceColumns(input, op.lhs_output_columns.col_idxs);
				result_chunk.Reset();
				state.scan_structure.Next(state.lhs_join_keys, state.lhs_output, result_chunk);
				local_matched += result_chunk.size();
				if (config.verify) {
					local_checksum += SumColumnInt64(result_chunk, checksum_col);
				}
				if (state.scan_structure.PointersExhausted() && result_chunk.size() == 0) {
					state.scan_structure.is_null = true;
					break;
				}
			}
		}
		matched_rows.fetch_add(local_matched, std::memory_order_relaxed);
		checksum.fetch_add(local_checksum, std::memory_order_relaxed);
		(void)exec_context;
	});
	probe_ms_out = ElapsedMs(begin);

	result.matched_rows = matched_rows.load();
	checksum_out = checksum.load();
	return result;
}

JoinBenchResult JoinMicroBench::ProbePerfectJoin(PerfectHashJoinExecutor &executor, double &probe_ms_out,
                                                 int64_t &checksum_out) {
	auto &op = *join;
	const idx_t checksum_col = ChecksumColumn();
	std::atomic<idx_t> matched_rows {0};
	std::atomic<int64_t> checksum {0};

	const auto begin = Clock::now();
	const idx_t thread_count = MinValue<idx_t>(config.probe_threads, dataset.probe_chunks.size());
	RunParallel(thread_count, dataset.probe_chunks.size(), [&](idx_t, idx_t from, idx_t to) {
		ThreadContext thread_context(context);
		ExecutionContext exec_context(context, thread_context, nullptr);
		auto state = executor.GetOperatorState(exec_context);
		DataChunk lhs_output;
		lhs_output.Initialize(BufferAllocator::Get(context), op.lhs_output_columns.col_types);
		DataChunk result_chunk;
		result_chunk.Initialize(Allocator::DefaultAllocator(), dataset.output_types);
		idx_t local_matched = 0;
		int64_t local_checksum = 0;

		for (idx_t c = from; c < to; c++) {
			auto &input = *dataset.probe_chunks[c];
			lhs_output.ReferenceColumns(input, op.lhs_output_columns.col_idxs);
			result_chunk.Reset();
			executor.ProbePerfectHashTable(exec_context, input, lhs_output, result_chunk, *state);
			local_matched += result_chunk.size();
			if (config.verify) {
				local_checksum += SumColumnInt64(result_chunk, checksum_col);
			}
		}
		matched_rows.fetch_add(local_matched, std::memory_order_relaxed);
		checksum.fetch_add(local_checksum, std::memory_order_relaxed);
	});
	probe_ms_out = ElapsedMs(begin);

	JoinBenchResult result;
	result.matched_rows = matched_rows.load();
	checksum_out = checksum.load();
	return result;
}

//===--------------------------------------------------------------------===//
// 三条路径
//===--------------------------------------------------------------------===//
JoinBenchResult JoinMicroBench::RunHashJoin() {
	JoinBenchResult result;
	result.algo = JoinBenchAlgo::HASH_JOIN;
	result.algo_name = JoinBenchAlgoName(result.algo);
	result.build_rows = dataset.build_rows;
	result.probe_rows = dataset.probe_rows;
	result.expected_matches = dataset.expected_matches;
	result.scan_ms = dataset.scan_ms;

	auto global_ht = join->InitializeHashTable(context);
	vector<idx_t> partition_offsets;

	double build_ms = 0;
	SinkHashJoin(global_ht, dataset.build_chunks, build_ms, partition_offsets);
	global_ht->Unpartition();
	result.build_ms = build_ms;

	const auto finalize_begin = Clock::now();
	FinalizeHashTable(*global_ht, config.build_threads);
	result.finalize_ms = ElapsedMs(finalize_begin);

	int64_t checksum = 0;
	double probe_ms = 0;
	auto probe_result = ProbeHashJoin(*global_ht, probe_ms, checksum);
	result.probe_ms = probe_ms;
	result.matched_rows = probe_result.matched_rows;
	result.payload_checksum = checksum;
	result.checksum_computed = config.verify;
	FinishTimings(result);
	result.matches_expected = result.matched_rows == result.expected_matches;

	if (!result.matches_expected) {
		result.note = "MISMATCH matched=" + to_string(result.matched_rows) +
		              " expected=" + to_string(result.expected_matches);
	}
	return result;
}

JoinBenchResult JoinMicroBench::RunPerfectJoin() {
	JoinBenchResult result;
	result.algo = JoinBenchAlgo::PERFECT_JOIN;
	result.algo_name = JoinBenchAlgoName(result.algo);
	result.build_rows = dataset.build_rows;
	result.probe_rows = dataset.probe_rows;
	result.expected_matches = dataset.expected_matches;
	result.scan_ms = dataset.scan_ms;

	auto global_ht = join->InitializeHashTable(context);
	vector<idx_t> partition_offsets;

	double build_ms = 0;
	SinkHashJoin(global_ht, dataset.build_chunks, build_ms, partition_offsets);
	global_ht->Unpartition();
	result.build_ms = build_ms;

	//=== Finalize: 与 PhysicalHashJoin::Finalize() 中 perfect-hash 分支一致 ===//
	const auto finalize_begin = Clock::now();
	PerfectHashJoinExecutor executor(*join, *global_ht);
	const auto min_value = MakeKeyValue(config.key_type, dataset.perfect_min);
	const auto max_value = MakeKeyValue(config.key_type, dataset.perfect_max);
	bool perfect_applied = false;
	if (executor.CanDoPerfectHashJoin(*join, min_value, max_value)) {
		auto key_type = global_ht->equality_types[0];
		perfect_applied = executor.BuildPerfectHashTable(key_type);
		if (!perfect_applied) {
			result.note = "BuildPerfectHashTable 失败 (build 侧有重复 key 或 key 越界)";
		}
	} else {
		result.note = "CanDoPerfectHashJoin=false: " +
		              PerfectJoinSkipReason(*join, global_ht->Count(), dataset.perfect_min, dataset.perfect_max);
	}
	result.finalize_ms = ElapsedMs(finalize_begin);

	if (!perfect_applied) {
		result.applicable = false;
		result.matched_rows = 0;
		FinishTimings(result);
		return result;
	}

	int64_t checksum = 0;
	double probe_ms = 0;
	auto probe_result = ProbePerfectJoin(executor, probe_ms, checksum);
	result.probe_ms = probe_ms;
	result.matched_rows = probe_result.matched_rows;
	result.payload_checksum = checksum;
	result.checksum_computed = config.verify;
	FinishTimings(result);
	result.matches_expected = result.matched_rows == result.expected_matches;
	if (!result.matches_expected) {
		result.note = "MISMATCH matched=" + to_string(result.matched_rows) +
		              " expected=" + to_string(result.expected_matches);
	}
	return result;
}

JoinBenchResult JoinMicroBench::RunBitmapJoin() {
	JoinBenchResult result;
	result.algo = JoinBenchAlgo::BITMAP_JOIN;
	result.algo_name = JoinBenchAlgoName(result.algo);
	result.build_rows = dataset.build_rows;
	result.probe_rows = dataset.probe_rows;
	result.expected_matches = dataset.expected_matches;
	result.scan_ms = dataset.scan_ms;

	if (!config.BitmapJoinEligible()) {
		result.applicable = false;
		result.note = "BHJ 需要 build 侧为稠密唯一 PK (build_pattern=UNIQUE_DENSE, key_stride=1)";
		return result;
	}

	BitmapJoinExecutor executor(*join);

	//=== Sink (多线程), 对应 PhysicalHashJoin::Sink() 的 BHJ 分支 ===//
	const auto build_begin = Clock::now();
	const idx_t thread_count = MinValue<idx_t>(config.build_threads, dataset.build_chunks.size());
	RunParallel(thread_count, dataset.build_chunks.size(), [&](idx_t, idx_t from, idx_t to) {
		ThreadContext thread_context(context);
		ExecutionContext exec_context(context, thread_context, nullptr);
		ExpressionExecutor join_key_executor(context);
		for (auto &cond : join->conditions) {
			join_key_executor.AddExpression(*cond.right);
		}
		DataChunk join_keys;
		join_keys.Initialize(BufferAllocator::Get(context), join->condition_types);
		BitmapJoinLocalState local;
		for (idx_t c = from; c < to; c++) {
			auto &chunk = *dataset.build_chunks[c];
			join_keys.Reset();
			join_key_executor.Execute(chunk, join_keys);
			executor.SinkBitmap(exec_context, chunk, join_keys, local);
		}
		// 对应 PhysicalHashJoin::Combine() 的 BHJ 分支
		executor.CombineBitmap(local);
	});
	result.build_ms = ElapsedMs(build_begin);

	//=== Finalize ===//
	const auto finalize_begin = Clock::now();
	executor.FinalizeBitmap();
	result.finalize_ms = ElapsedMs(finalize_begin);

	//=== Probe (多线程) ===//
	const idx_t checksum_col = ChecksumColumn();
	std::atomic<idx_t> matched_rows {0};
	std::atomic<int64_t> checksum {0};
	const auto probe_begin = Clock::now();
	const idx_t probe_thread_count = MinValue<idx_t>(config.probe_threads, dataset.probe_chunks.size());
	RunParallel(probe_thread_count, dataset.probe_chunks.size(), [&](idx_t, idx_t from, idx_t to) {
		ThreadContext thread_context(context);
		ExecutionContext exec_context(context, thread_context, nullptr);
		auto state = executor.GetOperatorState(exec_context);
		DataChunk result_chunk;
		result_chunk.Initialize(Allocator::DefaultAllocator(), dataset.output_types);
		idx_t local_matched = 0;
		int64_t local_checksum = 0;
		for (idx_t c = from; c < to; c++) {
			auto &input = *dataset.probe_chunks[c];
			result_chunk.Reset();
			executor.ProbeBitmap(exec_context, input, result_chunk, *state);
			local_matched += result_chunk.size();
			if (config.verify) {
				local_checksum += SumColumnInt64(result_chunk, checksum_col);
			}
		}
		matched_rows.fetch_add(local_matched, std::memory_order_relaxed);
		checksum.fetch_add(local_checksum, std::memory_order_relaxed);
	});
	result.probe_ms = ElapsedMs(probe_begin);

	result.matched_rows = matched_rows.load();
	result.payload_checksum = checksum;
	result.checksum_computed = config.verify;
	FinishTimings(result);
	result.matches_expected = result.matched_rows == result.expected_matches;
	result.note = "payload=" + string(executor.PayloadModeString()) +
	              " bitmap_size=" + to_string(executor.BitmapSize());
	if (!result.matches_expected) {
		result.note += " | MISMATCH matched=" + to_string(result.matched_rows) +
		               " expected=" + to_string(result.expected_matches);
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Unchained Hash Join (论文复现)
//===--------------------------------------------------------------------===//
JoinBenchResult JoinMicroBench::RunUnchainedJoin() {
	JoinBenchResult result;
	result.algo = JoinBenchAlgo::UNCHAINED_JOIN;
	result.algo_name = JoinBenchAlgoName(result.algo);
	result.build_rows = dataset.build_rows;
	result.probe_rows = dataset.probe_rows;
	result.expected_matches = dataset.expected_matches;
	result.scan_ms = dataset.scan_ms;

	auto global_ht = join->InitializeHashTable(context);
	vector<idx_t> partition_offsets;

	//=== Sink: 与普通 hash join 完全共用 ===//
	double build_ms = 0;
	SinkHashJoin(global_ht, dataset.build_chunks, build_ms, partition_offsets);
	result.build_ms = build_ms;

	//=== Finalize: Counting -> Prefix sum -> Copy, 替代 AllocatePointerTable + Finalize ===//
	// 必须在 Unpartition() 之前: 需要 sink_collection 的物理分区做零同步并行
	const auto finalize_begin = Clock::now();
	global_ht->unchained = make_uniq<UnchainedHashTable>();
	global_ht->unchained->Build(*global_ht, partition_offsets, config.build_threads);
	global_ht->Unpartition();
	result.finalize_ms = ElapsedMs(finalize_begin);

	if (!global_ht->unchained->IsBuilt()) {
		result.applicable = false;
		result.note = "UnchainedHashTable 未构建 (build 侧为空)";
		FinishTimings(result);
		return result;
	}

	//=== Probe: JoinHashTable::Probe 会分派到 unchained, 因此直接复用普通 HJ 的探针 ===//
	int64_t checksum = 0;
	double probe_ms = 0;
	auto probe_result = ProbeHashJoin(*global_ht, probe_ms, checksum);
	result.probe_ms = probe_ms;
	result.matched_rows = probe_result.matched_rows;
	result.payload_checksum = checksum;
	result.checksum_computed = config.verify;
	FinishTimings(result);
	result.matches_expected = result.matched_rows == result.expected_matches;

	auto &unchained = *global_ht->unchained;
	result.note = "slots=" + to_string(unchained.SlotCount()) + " max_slot_len=" + to_string(unchained.MaxSlotCount()) +
	              " dir=" + to_string(unchained.DirectorySizeInBytes() / (1024 * 1024)) + "MB adj=" +
	              to_string(unchained.AdjacencySizeInBytes() / (1024 * 1024)) + "MB";
	if (!result.matches_expected) {
		result.note += " | MISMATCH matched=" + to_string(result.matched_rows) +
		               " expected=" + to_string(result.expected_matches);
	}
	return result;
}

JoinBenchResult JoinMicroBench::Run(JoinBenchAlgo algo) {
	if (config.rematerialize) {
		MaterializeDataset();
	}
	switch (algo) {
	case JoinBenchAlgo::HASH_JOIN:
		return RunHashJoin();
	case JoinBenchAlgo::PERFECT_JOIN:
		return RunPerfectJoin();
	case JoinBenchAlgo::BITMAP_JOIN:
		return RunBitmapJoin();
	case JoinBenchAlgo::UNCHAINED_JOIN:
		return RunUnchainedJoin();
	default:
		throw InternalException("Unknown JoinBenchAlgo");
	}
}

vector<JoinBenchResult> JoinMicroBench::RunAll() {
	vector<JoinBenchResult> results;
	results.push_back(Run(JoinBenchAlgo::HASH_JOIN));
	results.push_back(Run(JoinBenchAlgo::PERFECT_JOIN));
	results.push_back(Run(JoinBenchAlgo::BITMAP_JOIN));
	results.push_back(Run(JoinBenchAlgo::UNCHAINED_JOIN));
	return results;
}

} // namespace duckdb
