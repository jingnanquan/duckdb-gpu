// Performance & correctness regression for the Bitmap-Join (BHJ) execution path.
//
// Runs the same TPC-H-style equi-join three times against the SF=5 parquet dataset:
//   1. Plain DuckDB hash join          (open_perfect_join = false, open_bitmap_join = false)
//   2. Perfect-hash join                (open_perfect_join = true,  open_bitmap_join = false)
//   3. Bitmap-Join (BHJ)                (open_perfect_join = false, open_bitmap_join = true)
//
// All three runs share the same single-equality INNER join (so the BHJ executor is exercised),
// and the result rows are required to match exactly across paths.
//
// The SF=5 parquet files (and the bitmap_join_meta.json) live at:
//   /home/featurize/workspace/duckdb-cuda/data/tpch_sf5_bitmap/
// If that directory is not present (e.g. CI without the dataset) the test is skipped.

#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/bitmap_join_meta.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/query_result.hpp"

#include <chrono>
#include <iostream>
#include <string>

using namespace duckdb;
using namespace std;

namespace {

constexpr const char *kDataDir = "/home/featurize/workspace/duckdb-cuda/data/tpch_sf5_bitmap";

// Build the test query. We deliberately keep it as a single equi-join INNER join with the
// dimension table on the build side so all three execution paths apply.
//   * build = part      (PK p_partkey, rowid_offset=1, ~1M rows for SF=5)
//   * probe = lineitem  (FK l_partkey, ~30 million rows for SF=5)
// We aggregate count, an LHS sum, and an RHS sum so that all three paths' payload-handling
// is exercised (RHS payload column scatter + dictionary slice on the BHJ path).
string MakeQuery() {
	return "SELECT count(*) AS cnt, "
	       "       sum(l_extendedprice) AS sum_price, "
	       "       sum(p_size) AS sum_size "
	       "FROM read_parquet('" +
	       string(kDataDir) +
	       "/lineitem.parquet') li "
	       "JOIN read_parquet('" +
	       string(kDataDir) +
	       "/part.parquet') p "
	       "ON li.l_partkey = p.p_partkey;";
}

struct RunResult {
	double elapsed_ms = 0.0;
	int64_t cnt = 0;
	double sum_price = 0.0;
	int64_t sum_size = 0;
};

RunResult RunOnce(Connection &con, const string &label, bool open_perfect, bool open_bitmap, bool force_bhj) {
	// Configure the per-connection switches.
	REQUIRE_NO_FAIL(*con.Query(string("SET open_perfect_join=") + (open_perfect ? "true" : "false")));
	REQUIRE_NO_FAIL(*con.Query(string("SET open_bitmap_join=") + (open_bitmap ? "true" : "false")));
	// Toggle the process-wide BHJ force switch (drives plan_comparison_join.cpp).
	BitmapJoinMetaRegistry::GetInstance().SetForceBitmapJoin(force_bhj);

	const auto query = MakeQuery();

	// Warm any plan caches with a single execution before timing (so we measure execution, not
	// extension load / parquet metadata fetch).
	auto warmup = con.Query(query);
	REQUIRE_NO_FAIL(*warmup);

	auto start = std::chrono::steady_clock::now();
	auto result = con.Query(query);
	auto end = std::chrono::steady_clock::now();
	REQUIRE_NO_FAIL(*result);

	RunResult out;
	out.elapsed_ms = std::chrono::duration<double, std::milli>(end - start).count();

	REQUIRE(result->RowCount() == 1);
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() == 1);
	out.cnt = chunk->GetValue(0, 0).GetValue<int64_t>();
	auto sum_price_val = chunk->GetValue(1, 0);
	out.sum_price = sum_price_val.IsNull() ? 0.0 : sum_price_val.GetValue<double>();
	auto sum_size_val = chunk->GetValue(2, 0);
	out.sum_size = sum_size_val.IsNull() ? 0 : sum_size_val.GetValue<int64_t>();

	std::cout << "[bitmap-join-perf] " << label << ": "
	          << "elapsed=" << out.elapsed_ms << " ms, "
	          << "cnt=" << out.cnt << ", "
	          << "sum_price=" << out.sum_price << ", "
	          << "sum_size=" << out.sum_size << std::endl;
	return out;
}

//! Renders EXPLAIN output as a single string so tests can grep it for plan markers (e.g.
//! "Bitmap Join").
string ExplainText(Connection &con, const string &query) {
	auto result = con.Query("EXPLAIN " + query);
	REQUIRE(!result->HasError());
	auto &materialized = result->Cast<MaterializedQueryResult>();
	string text;
	for (idx_t r = 0; r < materialized.RowCount(); r++) {
		for (idx_t c = 0; c < materialized.ColumnCount(); c++) {
			text += materialized.GetValue(c, r).ToString();
			text += " ";
		}
	}
	return text;
}

} // namespace

TEST_CASE("Bitmap-Join (BHJ) vs Perfect-Hash vs DuckDB hash join (TPCH SF=5 parquet)", "[bitmap_join][.]") {
	// Skip silently if the SF=5 parquet dataset is not present.
	{
		auto fs = FileSystem::CreateLocal();
		if (!fs->DirectoryExists(kDataDir)) {
			std::cerr << "[bitmap-join-perf] dataset directory '" << kDataDir
			          << "' not found - skipping test." << std::endl;
			return;
		}
		const string meta_path = string(kDataDir) + "/bitmap_join_meta.json";
		if (!fs->FileExists(meta_path)) {
			std::cerr << "[bitmap-join-perf] meta file '" << meta_path << "' not found - skipping test."
			          << std::endl;
			return;
		}
	}

	DuckDB db(nullptr);
	Connection con(db);

	// Load bitmap-join metadata (PK/FK bindings) into the process-wide registry.
	auto &registry = BitmapJoinMetaRegistry::GetInstance();
	registry.Clear();
	registry.LoadFromJson(string(kDataDir) + "/bitmap_join_meta.json");

	// For BHJ: the build side will be `part`; configure the planner override to point at the
	// part-PK binding. (Module 6.6 BitmapJoinRule will eventually do this resolution from the
	// bound expressions; until then we use this test-only override.)
    // 在这里设置，设置之后查询计划那里才能查询到
	registry.SetForceResolvedPK("part", "p_partkey");   // 

	// Run all three configurations.
	auto baseline = RunOnce(con, "duckdb-hash-join", /*open_perfect=*/false, /*open_bitmap=*/false,
	                        /*force_bhj=*/false);
	auto perfect = RunOnce(con, "perfect-hash-join", /*open_perfect=*/true, /*open_bitmap=*/false,
	                       /*force_bhj=*/false);
	auto bitmap = RunOnce(con, "bitmap-hash-join", /*open_perfect=*/false, /*open_bitmap=*/true,
	                      /*force_bhj=*/true);
	auto bitmap_auto = RunOnce(con, "bitmap-hash-join", /*open_perfect=*/false, /*open_bitmap=*/true,
	                      /*force_bhj=*/false);

	// Reset the global force switch so subsequent tests are not affected.
	registry.SetForceBitmapJoin(false);
	registry.SetForceResolvedPK("", "");
	registry.Clear();

	// Correctness: all three paths must produce the same aggregate.
	REQUIRE(baseline.cnt == perfect.cnt);
	REQUIRE(baseline.cnt == bitmap.cnt);
	REQUIRE(baseline.sum_size == perfect.sum_size);
	REQUIRE(baseline.sum_size == bitmap.sum_size);
	// `sum_price` is a DECIMAL aggregate that we read as a double; tolerate FP rounding.
	REQUIRE(std::abs(baseline.sum_price - perfect.sum_price) < 1e-3);
	REQUIRE(std::abs(baseline.sum_price - bitmap.sum_price) < 1e-3);

	std::cout << "[bitmap-join-perf] summary | duckdb=" << baseline.elapsed_ms
	          << " ms, perfect=" << perfect.elapsed_ms << " ms, bitmap=" << bitmap.elapsed_ms
	          << " ms" << std::endl;
}

// Same query/dataset as above, but *without* the test-only SetForceResolvedPK / SetForceBitmapJoin
// overrides. This exercises the automatic resolution path (BitmapJoinResolver optimizer pass,
// design doc 条目1/2): the join condition `li.l_partkey = p.p_partkey` is reverse-mapped from
// bound column references back to catalog table/column names, looked up in the loaded
// bitmap_join_meta.json registry, and - because `part` (the PK/dimension side) lands on the
// build side for this query - automatically wired up as a Bitmap-Join purely from
// `open_bitmap_join=true` plus the registry metadata.
TEST_CASE("Bitmap-Join (BHJ) automatic PK/FK resolution without SetForceResolvedPK (TPCH SF=5 parquet)",
          "[bitmap_join][.]") {
	// Skip silently if the SF=5 parquet dataset is not present.
	{
		auto fs = FileSystem::CreateLocal();
		if (!fs->DirectoryExists(kDataDir)) {
			std::cerr << "[bitmap-join-perf] dataset directory '" << kDataDir
			          << "' not found - skipping test." << std::endl;
			return;
		}
		const string meta_path = string(kDataDir) + "/bitmap_join_meta.json";
		if (!fs->FileExists(meta_path)) {
			std::cerr << "[bitmap-join-perf] meta file '" << meta_path << "' not found - skipping test."
			          << std::endl;
			return;
		}
	}

	DuckDB db(nullptr);
	Connection con(db);

	// Load bitmap-join metadata (PK/FK bindings) into the process-wide registry. Deliberately do
	// NOT call SetForceResolvedPK / SetForceBitmapJoin: the BHJ path below must be reached purely
	// through automatic resolution.
	auto &registry = BitmapJoinMetaRegistry::GetInstance();
	registry.Clear();
	registry.LoadFromJson(string(kDataDir) + "/bitmap_join_meta.json");
	registry.SetForceBitmapJoin(false);
	registry.SetForceResolvedPK("", "");

	const auto query = MakeQuery();

	// Baseline: BHJ disabled entirely (open_bitmap_join=false).
	auto baseline = RunOnce(con, "duckdb-hash-join (auto-resolve baseline)", /*open_perfect=*/false,
	                        /*open_bitmap=*/false, /*force_bhj=*/false);

	// Automatic BHJ: the only switch flipped is open_bitmap_join=true; force_bhj stays false so
	// the registry's force override never fires - if BHJ activates here, it can only be because
	// BitmapJoinResolver resolved the join condition on its own.
	auto automatic = RunOnce(con, "bitmap-hash-join (auto-resolve)", /*open_perfect=*/false,
	                         /*open_bitmap=*/true, /*force_bhj=*/false);

	// Sanity check: confirm the optimizer actually picked the BHJ path (not silently falling back
	// to a plain hash join), i.e. this test is not vacuously true.
	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));
	auto plan = ExplainText(con, query);
	REQUIRE(StringUtil::Contains(plan, "Bitmap Join"));

	registry.Clear();

	// Correctness: automatic BHJ must produce the same aggregate as the plain hash join.
	REQUIRE(baseline.cnt == automatic.cnt);
	REQUIRE(baseline.sum_size == automatic.sum_size);
	REQUIRE(std::abs(baseline.sum_price - automatic.sum_price) < 1e-3);

	std::cout << "[bitmap-join-perf] auto-resolve summary | duckdb=" << baseline.elapsed_ms
	          << " ms, bitmap(auto)=" << automatic.elapsed_ms << " ms" << std::endl;
}
