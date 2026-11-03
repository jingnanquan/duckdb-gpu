//===----------------------------------------------------------------------===//
//
// join_microbench.hpp -- Hash-Join / Perfect-Hash-Join / Bitmap-Join 微基准框架
//
// 目的
// ----
// 把 `src/execution/operator/join/physical_hash_join.cpp` 里并存的 3 条执行路径
// (普通 Hash Join / Perfect Hash Join / Bitmap Join (BHJ)) 从完整的 DuckDB
// 流水线中"抽"出来, 单独进行微基准测试:
//
//   * 不再经过 SQL/优化器/TableScan/聚合, 而是直接驱动 join 的
//     build(finalize) / probe 接口;
//   * 输入 chunk 由本框架在外部合成, 可以任意构造
//     (唯一 key / 重复 key / 重复 hash / 常量 key / 倾斜 / build-probe 规模比);
//   * build 与 probe 都可以用 N 个线程并行喂 chunk, 模拟 DuckDB 的并行 sink /
//     并行 operator;
//   * 埋点输出 scan / build / finalize / probe 的分段耗时。
//
// 与 DuckDB 真实执行的一致性
// --------------------------
// PhysicalHashJoin 本身只是一个"调度壳": 它的 Sink/Combine/Finalize/ExecuteInternal
// 全部转派给 3 个公开的执行器
//
//     JoinHashTable              (src/execution/join_hashtable.cpp)
//     PerfectHashJoinExecutor    (src/execution/operator/join/perfect_hash_join_executor.cpp)
//     BitmapJoinExecutor         (src/execution/operator/join/bitmap_hash_join_executor.cpp)
//
// 本框架直接驱动这 3 个执行器, 调用序列严格照抄 physical_hash_join.cpp:
//
//   build   : Sink()      -> join_key_executor.Execute(chunk) + ht->Build()      (Hash/Perfect)
//             Sink()      -> join_key_executor.Execute(chunk) + SinkBitmap()     (BHJ)
//   combine : Combine()   -> 各线程本地 HT 保留, BHJ 走 CombineBitmap()
//   finalize: Finalize()  -> Merge + Unpartition + AllocatePointerTable
//                            + InitializePointerTable(0, capacity)
//                            + Finalize(0, chunk_count, parallel)                (Hash)
//             Finalize()  -> CanDoPerfectHashJoin + BuildPerfectHashTable        (Perfect)
//             Finalize()  -> FinalizeBitmap()                                    (BHJ)
//   probe   : ExecuteInternal() -> ht->Probe() + ScanStructure::Next() 循环      (Hash)
//             ExecuteInternal() -> ProbePerfectHashTable()                       (Perfect)
//             ExecuteInternal() -> ProbeBitmap()                                 (BHJ)
//
// 之所以不走 PhysicalHashJoin::Finalize(), 是因为 HashJoinGlobalSinkState 定义在
// .cpp 内部(外部拿不到其 hash_table), 而 Hash Join 的 finalize 在真实执行中是通过
// HashJoinTableInitEvent/HashJoinFinalizeEvent 异步调度的; 框架在这里按同样的调用
// 顺序同步执行这两步(见 RunHashJoin / FinalizeHashTable)。
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// 配置
//===--------------------------------------------------------------------===//

//! 被测的 4 条 join 路径
enum class JoinBenchAlgo : uint8_t {
	HASH_JOIN = 0,
	PERFECT_JOIN = 1,
	BITMAP_JOIN = 2,
	//! 论文复现: unchained hash table (directory + adjacency)
	UNCHAINED_JOIN = 3
};

string JoinBenchAlgoName(JoinBenchAlgo algo);

//! build(维度/PK) 侧 key 的分布形态
enum class JoinBenchKeyPattern : uint8_t {
	//! key = key_offset + row: 稠密唯一, 与 BHJ 的 rowid 语义完全一致
	UNIQUE_DENSE = 0,
	//! key = key_offset + row * key_stride: 唯一但域很稀疏 (perfect hash join 会被打回)
	UNIQUE_SPARSE,
	//! 只有 build_distinct 个不同 key, 大量重复 -> hash 链变长
	DUPLICATE_KEYS,
	//! key 互不相同, 但 DuckDB 的 hash 全部落到同一个 pointer-table 桶(且 salt 相同)
	//! -> 线性探测链最长, 用于"构造重复 hash"
	HASH_COLLISION,
	//! 所有行同一个 key
	CONSTANT_KEY,
	//! 每 run_length 行换一次 key (块内重复)
	RUNS_OF_SAME_KEY,
	//! key 互不相同, 但 DuckDB hash 的 bit [48-K, 47] 全部相同
	//! -> 全部落进 unchained hash table 的同一个 slot, adjacency 区间被拉到最长
	//! (这是 unchained 特有的最坏情况: 区间扫描变长, 且 Bloom tag 也相同)
	UNCHAINED_SLOT_COLLISION
};

string JoinBenchKeyPatternName(JoinBenchKeyPattern pattern);

//! MurmurHash64 在 mod 2^64 上是双射, 因此可逆。
//! 有了它就能"点名"构造出 hash 落到指定桶 / 指定 slot 的 key (见 KeyPattern::HASH_COLLISION)。
uint64_t MurmurHash64Inverse(uint64_t hash);

struct JoinBenchConfig {
	string name = "default";

	//=== 规模 ===//
	idx_t build_rows = 1000000;  //!< build (RHS) 行数
	idx_t probe_rows = 8000000;  //!< probe (LHS) 行数
	idx_t chunk_size = STANDARD_VECTOR_SIZE;

	//=== key 分布 ===//
	LogicalType key_type = LogicalType::INTEGER;
	JoinBenchKeyPattern build_pattern = JoinBenchKeyPattern::UNIQUE_DENSE;
	//! DUPLICATE_KEYS: 不同 key 个数; CONSTANT_KEY: 忽略; RUNS_OF_SAME_KEY: run 长度;
	//! HASH_COLLISION: hash 冲突组的个数(即有多少个桶被"撞")
	idx_t build_distinct = 1000000;
	idx_t key_stride = 1;  //!< UNIQUE_SPARSE 的步长
	//! rowid = key - key_offset (BHJ 的 rowid_offset); 0-based PK 用 0, TPC-H 1-based PK 用 1
	int64_t key_offset = 0;
	//! BHJ 的 PK 表总行数 (= bitmap 位数)。0 表示等于 build_rows (密度 100%)
	idx_t pk_row_count = 0;
	//! 喂给 PerfectHashJoinExecutor::CanDoPerfectHashJoin 的 min/max 取哪里:
	//!  * true  = 整个 PK 域 [key_offset, key_offset + PKRowCount() - 1]
	//!            (等价于 DuckDB 用列统计/statistics 判断, 或 build 侧是维度表的一个子集)
	//!  * false = build 数据的观测 min/max (等价于 filter pushdown 算出的 min/max)
	//! 注意 DuckDB 的判据是 `ht.Count() <= max - min`, 所以当 build 恰好铺满整个 PK 域时
	//! (count == N, range == N-1) 会差 1 而被打回 —— 这是 DuckDB 的真实行为, 不是 bug。
	bool perfect_use_pk_domain = true;

	//=== probe 侧命中行为 ===//
	double probe_hit_ratio = 1.0;    //!< 命中 build 的 probe 行比例
	double probe_hot_fraction = 1.0; //!< 命中集中在前 hot_fraction * build_rows 行 (1.0 = 均匀)

	//=== payload ===//
	idx_t build_payload_cols = 2;  //!< 除 key 以外的 build 列数
	idx_t probe_payload_cols = 2;  //!< 除 key 以外的 probe 列数

	//=== 并发 ===//
	idx_t build_threads = 4;
	idx_t probe_threads = 4;

	//=== 校验 ===//
	bool verify = true;  //!< 校验行数 + payload 校验和 (会给 probe 增加一点开销)
	//! 是否在每次 Run 前重新物化 chunk (false = 复用第一次物化结果, 更省内存/时间)
	bool rematerialize = false;
	bool verbose = true;

	uint64_t seed = 42;

	idx_t PKRowCount() const {
		return pk_row_count == 0 ? build_rows : pk_row_count;
	}
	//! BHJ 要求 build 侧是 PK(唯一/稠密)侧
	bool BitmapJoinEligible() const;
};

//===--------------------------------------------------------------------===//
// 数据集
//===--------------------------------------------------------------------===//

struct JoinBenchDataset {
	vector<LogicalType> build_types;  //!< [key, build payload...]
	vector<LogicalType> probe_types;  //!< [key, probe payload...]
	vector<LogicalType> output_types; //!< [probe cols][build key][build payload...]

	//! 预先物化好的 chunk 流 (build / probe)
	vector<unique_ptr<DataChunk>> build_chunks;
	vector<unique_ptr<DataChunk>> probe_chunks;

	idx_t build_rows = 0;
	idx_t probe_rows = 0;

	//! INNER join 的期望输出行数 (与被测实现无关, 独立算出)
	idx_t expected_matches = 0;
	//! build 数据的观测 min/max
	int64_t build_min = 0;
	int64_t build_max = 0;
	//! 喂给 CanDoPerfectHashJoin 的 min/max (见 JoinBenchConfig::perfect_use_pk_domain)
	int64_t perfect_min = 0;
	int64_t perfect_max = 0;

	//! 物化 chunk 的耗时, 即 "scan" 时间
	double scan_ms = 0;
};

//===--------------------------------------------------------------------===//
// 结果
//===--------------------------------------------------------------------===//

struct JoinBenchResult {
	JoinBenchAlgo algo = JoinBenchAlgo::HASH_JOIN;
	string algo_name;
	bool applicable = true;  //!< false = 该算法对本数据集不适用 (例如 perfect/BHJ 前置条件不满足)
	string note;             //!< 不适用原因 / 附加说明

	//=== 分段耗时 (ms, wall clock) ===//
	double scan_ms = 0;      //!< 物化输入 chunk (模拟 scan)
	double build_ms = 0;     //!< 多线程 Sink + Combine
	double finalize_ms = 0;  //!< 单线程 finalize
	double probe_ms = 0;     //!< 多线程 probe
	double total_ms = 0;
	//! 只算 join 本身 (build + finalize + probe), 不含 scan —— 三条路径对比时看这个
	double join_ms = 0;

	idx_t build_rows = 0;
	idx_t probe_rows = 0;
	idx_t matched_rows = 0;
	idx_t expected_matches = 0;
	int64_t payload_checksum = 0;
	bool checksum_computed = false;

	bool matches_expected = true;

	double ProbeThroughputMrowsPerSec() const;
	string ToString() const;
};

//===--------------------------------------------------------------------===//
// Runner
//===--------------------------------------------------------------------===//

//! 占位 LogicalOperator: PhysicalJoin 只从它身上取 `types` (输出 schema), 不参与执行。
class BenchLogicalJoin : public LogicalOperator {
public:
	BenchLogicalJoin() : LogicalOperator(LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
	}
	void ResolveTypes() override {
	}
	string GetName() const override {
		return "BENCH_JOIN";
	}
};

//! 一次 JoinMicroBench 持有一份固定的 PhysicalHashJoin 算子 + 一份数据集,
//! 然后对 3 条路径分别计时。数据集只物化一次, 保证 3 条路径吃到的输入完全一致。
class JoinMicroBench {
public:
	JoinMicroBench(ClientContext &context, const JoinBenchConfig &config);
	~JoinMicroBench();

	const JoinBenchConfig &GetConfig() const {
		return config;
	}
	const JoinBenchDataset &GetDataset() const {
		return dataset;
	}
	PhysicalHashJoin &GetJoin() {
		return *join;
	}

	JoinBenchResult Run(JoinBenchAlgo algo);
	JoinBenchResult RunHashJoin();
	JoinBenchResult RunPerfectJoin();
	JoinBenchResult RunBitmapJoin();
	JoinBenchResult RunUnchainedJoin();
	//! 依次跑 4 条路径, 返回 {hash, perfect, bitmap, unchained}
	vector<JoinBenchResult> RunAll();

private:
	//=== 构造 ===//
	void BuildOperator();
	void MaterializeDataset();
	//=== key 生成 ===//
	int64_t BuildKey(idx_t row) const;
	int64_t ProbeKey(idx_t row) const;
	int64_t BuildKeyMultiplicity(int64_t key) const;
	void FillBuildChunk(DataChunk &chunk, idx_t first_row, idx_t count) const;
	void FillProbeChunk(DataChunk &chunk, idx_t first_row, idx_t count) const;
	//! 用来做跨算法校验和的列: 优先取第一个 build payload 列, build 侧没有 payload 时退化为 build key
	idx_t ChecksumColumn() const;
	//=== 各路径 ===//
	//! 多线程 Sink + Merge, 顺便输出每个 radix 分区的起始行号。
	//! 注意这里**不**调用 Unpartition(): unchained 的构建需要 sink_collection 的物理分区,
	//! 所以 Unpartition() 由各路径自己在合适的时机调用。
	void SinkHashJoin(unique_ptr<JoinHashTable> &ht, const vector<unique_ptr<DataChunk>> &chunks,
	                  double &build_ms_out, vector<idx_t> &partition_offsets);
	void FinalizeHashTable(JoinHashTable &ht, idx_t build_threads);
	JoinBenchResult ProbeHashJoin(JoinHashTable &ht, double &probe_ms_out, int64_t &checksum_out);
	JoinBenchResult ProbePerfectJoin(PerfectHashJoinExecutor &exec, double &probe_ms_out, int64_t &checksum_out);

private:
	ClientContext &context;
	JoinBenchConfig config;
	JoinBenchDataset dataset;

	// 算子的宿主 (必须比 join 活得久)
	PhysicalPlan plan;
	BenchLogicalJoin logical_op;
	PhysicalHashJoin *join = nullptr;
	//! BHJ 需要的 PK 绑定 (BitmapJoinExecutor 只持有指针)
	BitmapJoinPKBinding pk_binding;

	// hash-collision 场景的辅助量
	idx_t collision_bucket_bits = 0;
	idx_t collision_keys_per_group = 1;
	idx_t unchained_slot_bits = 0;
	idx_t unchained_slot_shift = 0;

	// build key 的多重集 (用于算期望行数)
	vector<int64_t> build_keys;
	vector<int64_t> build_keys_sorted;
};

} // namespace duckdb
