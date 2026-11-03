//===----------------------------------------------------------------------===//
//
// Hash Join / Perfect Hash Join / Bitmap Join 的微基准测试入口。
//
// 这些用例默认不跑 (tag 里带 `[.]`), 需要显式指定:
//
//     ./build/release/test/unittest "[join_microbench]"
//     ./build/release/test/unittest "Join micro-bench: *"
//
// 每个用例都会构造一份合成数据集, 用 N 个线程把 chunk 喂给 3 条 join 路径,
// 然后打印 scan / build / finalize / probe 的分段耗时, 并交叉校验结果行数与
// payload 校验和完全一致。
//
//===----------------------------------------------------------------------===//

#include "join_microbench.hpp"

#include "catch.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types/hash.hpp"

#include <iostream>

using namespace duckdb;

namespace {

//! 跑一个场景: 构造数据集 -> 3 条路径各跑一遍 -> 打印 -> 交叉校验
vector<JoinBenchResult> RunScenario(ClientContext &context, const JoinBenchConfig &config) {
	JoinMicroBench bench(context, config);
	auto results = bench.RunAll();

	std::cout << "\n=== [join-microbench] " << config.name << " ===" << std::endl;
	std::cout << "  dataset: build=" << config.build_rows << " rows ("
	          << JoinBenchKeyPatternName(config.build_pattern) << ", distinct=" << config.build_distinct
	          << ", stride=" << config.key_stride << "), probe=" << config.probe_rows
	          << " rows, hit_ratio=" << config.probe_hit_ratio
	          << ", hot_fraction=" << config.probe_hot_fraction << ", pk_row_count=" << config.PKRowCount()
	          << ", key_offset=" << config.key_offset
	          << ", threads(build/probe)=" << config.build_threads << "/" << config.probe_threads
	          << ", expected_matches=" << bench.GetDataset().expected_matches << std::endl;
	for (auto &result : results) {
		std::cout << "  " << result.ToString() << std::endl;
	}

	//=== 交叉校验 ===//
	bool have_reference = false;
	idx_t reference_rows = 0;
	int64_t reference_checksum = 0;
	for (auto &result : results) {
		if (!result.applicable) {
			continue;
		}
		REQUIRE(result.matches_expected);
		if (!have_reference) {
			reference_rows = result.matched_rows;
			reference_checksum = result.payload_checksum;
			have_reference = true;
		} else {
			REQUIRE(result.matched_rows == reference_rows);
			if (result.checksum_computed && results[0].checksum_computed) {
				REQUIRE(result.payload_checksum == reference_checksum);
			}
		}
	}
	REQUIRE(have_reference);
	return results;
}

ClientContext &TestContext() {
	static DuckDB db(nullptr);
	static Connection con(db);
	return *con.context;
}

JoinBenchConfig BaseConfig(const string &name) {
	JoinBenchConfig config;
	config.name = name;
	return config;
}

} // namespace

//===--------------------------------------------------------------------===//
// 1. 基线: 维度表子集 (TPC-H part/lineitem 形态) -- 3 条路径都能跑
//    build 侧是 1M 行 PK 表里被过滤出来的 500K 行 -> perfect(hash 域)/BHJ/普通 HJ 全可用
//===--------------------------------------------------------------------===//
TEST_CASE("Join micro-bench: dense unique PK (hash vs perfect vs bitmap)", "[join_microbench][.]") {
	auto config = BaseConfig("dense-unique-pk");
	config.build_pattern = JoinBenchKeyPattern::UNIQUE_DENSE;
	config.build_rows = 500000;
	config.pk_row_count = 1000000; // PK 域 1M, build 命中 500K (密度 50%)
	config.probe_rows = 8000000;
	config.build_threads = 4;
	config.probe_threads = 4;
	auto results = RunScenario(TestContext(), config);

	REQUIRE(results[0].applicable);
	REQUIRE(results[1].applicable);
	REQUIRE(results[2].applicable);
	REQUIRE(results[3].applicable); // unchained
}

//===--------------------------------------------------------------------===//
// 1c. unchained hash table 的最坏情况: 所有 key 的 hash 前缀相同, 全部落进
//     同一个 slot -> adjacency 区间被拉长 (论文 §3.1 的区间扫描代价)
//===--------------------------------------------------------------------===//
TEST_CASE("Join micro-bench: unchained slot collision", "[join_microbench][.]") {
	auto config = BaseConfig("unchained-slot-collision");
	config.key_type = LogicalType::BIGINT;
	config.build_pattern = JoinBenchKeyPattern::UNCHAINED_SLOT_COLLISION;
	config.build_rows = 100000;
	config.build_distinct = 256; // 256 个冲突组, 每组 ~390 个 key 挤在一个 slot
	config.probe_rows = 2000000;
	auto results = RunScenario(TestContext(), config);
	REQUIRE(results[0].applicable);
	REQUIRE(results[3].applicable);
}

//===--------------------------------------------------------------------===//
// 1b. 同一份数据, 但 build 恰好铺满整个 PK 域: DuckDB 的 `count <= max - min` 判据
//     差 1, perfect hash join 会被打回 (真实行为, 用来说明为什么上面的场景要留空隙)
//===--------------------------------------------------------------------===//
TEST_CASE("Join micro-bench: fully dense PK domain rejects perfect hash join", "[join_microbench][.]") {
	auto config = BaseConfig("fully-dense-pk");
	config.build_pattern = JoinBenchKeyPattern::UNIQUE_DENSE;
	config.build_rows = 500000;
	config.pk_row_count = 0; // == build_rows -> 域被铺满
	config.perfect_use_pk_domain = false;
	config.probe_rows = 2000000;
	auto results = RunScenario(TestContext(), config);

	REQUIRE(results[0].applicable);
	REQUIRE(!results[1].applicable); // count(500000) > range(499999)
	REQUIRE(results[2].applicable);
}

//===--------------------------------------------------------------------===//
// 2. 1-based PK (TPC-H 的 p_partkey 从 1 开始): 验证 BHJ 的 rowid_offset
//===--------------------------------------------------------------------===//
TEST_CASE("Join micro-bench: 1-based PK (rowid_offset=1)", "[join_microbench][.]") {
	auto config = BaseConfig("one-based-pk");
	config.build_pattern = JoinBenchKeyPattern::UNIQUE_DENSE;
	config.key_offset = 1; // rowid = key - 1
	config.build_rows = 500000;
	config.pk_row_count = 1000000;
	config.probe_rows = 4000000;
	auto results = RunScenario(TestContext(), config);
	REQUIRE(results[2].applicable);
}

//===--------------------------------------------------------------------===//
// 3. 重复 key: build 侧只有 distinct 个 key -> hash 链变长, perfect/BHJ 自动失效
//===--------------------------------------------------------------------===//
TEST_CASE("Join micro-bench: duplicate build keys", "[join_microbench][.]") {
	auto config = BaseConfig("duplicate-keys");
	config.build_pattern = JoinBenchKeyPattern::DUPLICATE_KEYS;
	config.build_rows = 1000000;
	config.build_distinct = 100000; // 每个 key 重复 10 次
	config.probe_rows = 1000000;
	auto results = RunScenario(TestContext(), config);

	REQUIRE(results[0].applicable);  // hash join 一定可用
	REQUIRE(!results[1].applicable); // count > range -> perfect hash join 打回
	REQUIRE(!results[2].applicable); // build 侧非 PK -> BHJ 打回
	REQUIRE(results[3].applicable);  // unchained 支持重复 key (区间里放多条同 hash 记录)
}

//===--------------------------------------------------------------------===//
// 4. 重复 hash: 通过 MurmurHash64 求逆, 让一批互不相同的 BIGINT key 的 hash
//    全部落在同一个 pointer-table 桶且 salt 相同 -> 线性探测链最长
//===--------------------------------------------------------------------===//
TEST_CASE("Join micro-bench: forced hash collisions", "[join_microbench][.]") {
	auto config = BaseConfig("hash-collision");
	config.key_type = LogicalType::BIGINT;
	// 规模刻意压小: 全部 key 挤在同一个桶时, DuckDB 的插入与探测都是 O(链长) × O(chunk),
	// 大一点就会跑几十秒
	config.build_pattern = JoinBenchKeyPattern::HASH_COLLISION;
	config.build_rows = 10000;
	config.build_distinct = 128; // 128 个冲突组, 每组 ~78 个 key 撞在同一个桶
	config.probe_rows = 100000;
	auto results = RunScenario(TestContext(), config);
	REQUIRE(results[0].applicable);
}

//===--------------------------------------------------------------------===//
// 5. 常量 key (极端倾斜)
//===--------------------------------------------------------------------===//
TEST_CASE("Join micro-bench: constant build key", "[join_microbench][.]") {
	auto config = BaseConfig("constant-key");
	config.build_pattern = JoinBenchKeyPattern::CONSTANT_KEY;
	config.build_rows = 64;
	config.probe_rows = 100000;
	auto results = RunScenario(TestContext(), config);
	REQUIRE(results[0].applicable);
}

//===--------------------------------------------------------------------===//
// 6. build / probe 规模比扫描
//===--------------------------------------------------------------------===//
TEST_CASE("Join micro-bench: build vs probe scale sweep", "[join_microbench][.]") {
	ClientContext &context = TestContext();
	for (idx_t build_rows : {idx_t(1000), idx_t(100000), idx_t(1000000)}) {
		for (idx_t probe_rows : {idx_t(1000000), idx_t(8000000)}) {
			auto config = BaseConfig("scale-build" + to_string(build_rows) + "-probe" + to_string(probe_rows));
			config.build_pattern = JoinBenchKeyPattern::UNIQUE_DENSE;
			config.build_rows = build_rows;
			config.probe_rows = probe_rows;
			config.build_threads = 4;
			config.probe_threads = 4;
			RunScenario(context, config);
		}
	}
}

//===--------------------------------------------------------------------===//
// 7. 稀疏 PK / 低密度: PK 表很大但只有少量行被 build 命中 (BHJ payload 稀疏场景)
//===--------------------------------------------------------------------===//
TEST_CASE("Join micro-bench: sparse PK domain (low bitmap density)", "[join_microbench][.]") {
	ClientContext &context = TestContext();
	for (idx_t pk_row_count : {idx_t(100000), idx_t(1000000), idx_t(4194304)}) {
		auto config = BaseConfig("sparse-pk-" + to_string(pk_row_count));
		config.build_pattern = JoinBenchKeyPattern::UNIQUE_DENSE;
		config.build_rows = 100000;
		config.pk_row_count = pk_row_count; // bitmap 大小, 密度 = build_rows / pk_row_count
		config.probe_rows = 2000000;
		RunScenario(context, config);
	}
}

//===--------------------------------------------------------------------===//
// 8. 稀疏 key 域 (唯一但跨度大) -> perfect hash join 打回, BHJ 也打回
//===--------------------------------------------------------------------===//
TEST_CASE("Join micro-bench: wide sparse key domain", "[join_microbench][.]") {
	auto config = BaseConfig("unique-sparse");
	config.build_pattern = JoinBenchKeyPattern::UNIQUE_SPARSE;
	config.key_stride = 1000;
	config.build_rows = 200000;
	config.probe_rows = 2000000;
	auto results = RunScenario(TestContext(), config);
	REQUIRE(results[0].applicable);
	REQUIRE(!results[1].applicable); // key 跨度 = 200M > 1M
	REQUIRE(!results[2].applicable); // 非稠密 rowid
}

//===--------------------------------------------------------------------===//
// 9. 多线程 chunk 输入扫描
//===--------------------------------------------------------------------===//
TEST_CASE("Join micro-bench: thread scaling", "[join_microbench][.]") {
	ClientContext &context = TestContext();
	for (idx_t threads : {idx_t(1), idx_t(2), idx_t(4), idx_t(8), idx_t(16)}) {
		auto config = BaseConfig("threads-" + to_string(threads));
		config.build_pattern = JoinBenchKeyPattern::UNIQUE_DENSE;
		config.build_rows = 1000000;
		config.probe_rows = 4000000;
		config.build_threads = threads;
		config.probe_threads = threads;
		RunScenario(context, config);
	}
}

//===--------------------------------------------------------------------===//
// 10. probe 命中率 / 倾斜扫描
//===--------------------------------------------------------------------===//
TEST_CASE("Join micro-bench: probe hit ratio and skew", "[join_microbench][.]") {
	ClientContext &context = TestContext();
	for (double hit_ratio : {1.0, 0.5, 0.1, 0.0}) {
		for (double hot_fraction : {1.0, 0.01}) {
			auto config = BaseConfig("hit" + to_string(hit_ratio) + "-hot" + to_string(hot_fraction));
			config.build_pattern = JoinBenchKeyPattern::UNIQUE_DENSE;
			config.build_rows = 500000;
			config.probe_rows = 4000000;
			config.probe_hit_ratio = hit_ratio;
			config.probe_hot_fraction = hot_fraction;
			RunScenario(context, config);
		}
	}
}

//===--------------------------------------------------------------------===//
// 11. payload 宽度: BHJ 的 payload 物化开销 vs hash join 的 HT 加宽
//===--------------------------------------------------------------------===//
TEST_CASE("Join micro-bench: payload width sweep", "[join_microbench][.]") {
	ClientContext &context = TestContext();
	for (idx_t build_payload_cols : {idx_t(0), idx_t(2), idx_t(8)}) {
		auto config = BaseConfig("build-payload-" + to_string(build_payload_cols));
		config.build_pattern = JoinBenchKeyPattern::UNIQUE_DENSE;
		config.build_rows = 1000000;
		config.probe_rows = 4000000;
		config.build_payload_cols = build_payload_cols;
		config.probe_payload_cols = 2;
		RunScenario(context, config);
	}
}

//===--------------------------------------------------------------------===//
// 12. 框架自检: 极小数据集, 逐条校验期望值与校验和
//===--------------------------------------------------------------------===//
TEST_CASE("Join micro-bench: harness self-check (tiny dataset)", "[join_microbench]") {
	ClientContext &context = TestContext();

	// "构造重复 hash" 完全依赖 MurmurHash64 的可逆性, 先自检求逆正确
	for (uint64_t v = 1; v < 4096; v += 37) {
		REQUIRE(MurmurHash64Inverse(MurmurHash64(v)) == v);
	}

	auto config = BaseConfig("self-check");
	config.build_pattern = JoinBenchKeyPattern::UNIQUE_DENSE;
	config.build_rows = 10000;
	config.pk_row_count = 20000; // PK 域留空隙, 让 perfect hash join 也能通过
	config.probe_rows = 50000;
	config.build_threads = 2;
	config.probe_threads = 2;
	config.verify = true;

	JoinMicroBench bench(context, config);
	REQUIRE(bench.GetDataset().expected_matches == 50000);
	REQUIRE(bench.GetDataset().build_chunks.size() == 5);
	REQUIRE(bench.GetDataset().probe_chunks.size() == 25);

	auto hash = bench.Run(JoinBenchAlgo::HASH_JOIN);
	auto perfect = bench.Run(JoinBenchAlgo::PERFECT_JOIN);
	auto bitmap = bench.Run(JoinBenchAlgo::BITMAP_JOIN);
	auto unchained = bench.Run(JoinBenchAlgo::UNCHAINED_JOIN);

	for (auto *result : {&hash, &perfect, &bitmap, &unchained}) {
		REQUIRE(result->applicable);
		REQUIRE(result->matched_rows == 50000);
		REQUIRE(result->matches_expected);
		REQUIRE(result->build_ms >= 0);
		REQUIRE(result->probe_ms >= 0);
	}
	REQUIRE(hash.payload_checksum == perfect.payload_checksum);
	REQUIRE(hash.payload_checksum == bitmap.payload_checksum);
	REQUIRE(hash.payload_checksum == unchained.payload_checksum);

	// probe 未命中时输出必须为空
	auto miss_config = BaseConfig("self-check-miss");
	miss_config.build_pattern = JoinBenchKeyPattern::UNIQUE_DENSE;
	miss_config.build_rows = 10000;
	miss_config.probe_rows = 10000;
	miss_config.probe_hit_ratio = 0.0;
	JoinMicroBench miss_bench(context, miss_config);
	REQUIRE(miss_bench.GetDataset().expected_matches == 0);
	REQUIRE(miss_bench.Run(JoinBenchAlgo::HASH_JOIN).matched_rows == 0);
	REQUIRE(miss_bench.Run(JoinBenchAlgo::BITMAP_JOIN).matched_rows == 0);
	REQUIRE(miss_bench.Run(JoinBenchAlgo::UNCHAINED_JOIN).matched_rows == 0);
}
