// End-to-end TPC-H test for the Bitmap-Join (BHJ) execution path (design doc
// b_idea/6.4/6.4后续进展-实现与测试方案.md, 条目5).
//
// Unlike test_bitmap_join_perf.cpp (a single hand-picked equi-join) or test_bitmap_join_chain.cpp
// (small synthetic multi-join chains), this test runs the *unmodified* standard TPC-H queries
// against the real SF=5 dataset in three modes:
//   1. baseline: open_perfect_join=false, open_bitmap_join=false (plain DuckDB hash join)
//   2. perfect:  open_perfect_join=true,  open_bitmap_join=false (perfect-hash join)
//   3. bitmap:   open_perfect_join=false, open_bitmap_join=true  (BHJ, fully automatic - no
//      SetForceResolvedPK/SetForceBitmapJoin, exercising 条目1-4 end-to-end)
//
// Queries are fetched from TpchExtension (the `tpch` extension's dbgen-embedded query texts).
// By default it runs Q5, Q9, Q10; override with the BHJ_QUERIES environment variable
// (comma-separated list of query numbers, e.g. `BHJ_QUERIES=1,2,3`).
//
// For each (query, mode) it measures total wall-clock time and counts how many HASH_JOIN nodes
// in the plan actually hit BHJ (via EXPLAIN's "Bitmap Join: yes" marker). Correctness (identical
// results across all three modes) is the only hard `REQUIRE`; hit-rate and performance are
// printed as diagnostics only (see design doc §5.3: "软性打印，不作为CI失败条件").
//
// The SF=5 parquet files (and bitmap_join_meta.json) live at:
//   /home/featurize/workspace/duckdb-cuda/data/tpch_sf5_bitmap/
// If that directory (or its 8 tables) is not present, the test is skipped. Hidden behind the
// `[.]` tag (like test_bitmap_join_perf.cpp) since it depends on that multi-GB external dataset
// and is comparatively slow - run explicitly with `./unittest "[bitmap_join_tpch]"`.

#include "catch.hpp"
#include "test_helpers.hpp"
#include "tpch_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/bitmap_join_meta.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/query_result.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>

using namespace duckdb;
using namespace std;

namespace {

constexpr const char *kBitmapDataDir = "/home/featurize/workspace/duckdb-cuda/data/tpch_sf5_bitmap";
constexpr const char *kRawDataDir = "/home/featurize/workspace/duckdb-cuda/data/tpch_sf5";
constexpr const char *kTables[] = {"region",  "nation",   "customer", "orders",
                                   "part",    "partsupp", "supplier", "lineitem"};

bool DatasetAvailable(const string &dir) {
	auto fs = FileSystem::CreateLocal();
	if (!fs->DirectoryExists(dir)) {
		return false;
	}
	for (auto &table : kTables) {
		if (!fs->FileExists(dir + "/" + table + ".parquet")) {
			return false;
		}
	}
	return true;
}

//! Creates one VIEW per parquet file, named after the table - this both keeps the query text
//! readable *and* satisfies 条目1's "logical table name" resolution constraint (a read_parquet
//! LogicalGet falls back to the parquet file's own base name, see ResolveLogicalTableName in
//! bitmap_join_resolver.cpp - VIEWs are inlined at bind time, so the underlying LogicalGet is
//! unaffected by the VIEW name itself).
void CreateViews(Connection &con, const string &dir) {
	for (auto &table : kTables) {
		auto sql = StringUtil::Format("CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet('%s/%s.parquet')",
		                              table, dir, table);
		REQUIRE_NO_FAIL(con.Query(sql));
	}
}

//! Returns the list of TPC-H query numbers to run.  Defaults to {5, 9, 10}.
//! Override via the BHJ_QUERIES environment variable (comma-separated, e.g. "1,2,3").
std::vector<int> GetQueryIds() {
	std::vector<int> defaults = {5, 9, 10};
	const char *env = std::getenv("BHJ_QUERIES");
	if (!env || strlen(env) == 0) {
		return defaults;
	}
	std::vector<int> ids;
	string token;
	for (const char *p = env; *p; p++) {
		char c = *p;
		if (c == ',') {
			if (!token.empty()) {
				ids.push_back(std::stoi(token));
				token.clear();
			}
		} else if (c != ' ') {
			token += c;
		}
	}
	if (!token.empty()) {
		ids.push_back(std::stoi(token));
	}
	if (ids.empty()) {
		return defaults;
	}
	std::sort(ids.begin(), ids.end());
	ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
	return ids;
}

struct QuerySpec {
	string name;
	string sql;
	//! Rough, hand-counted expectation of how many of this query's equi-joins are BHJ-eligible
	//! under 条目1-4's static rules (single equality condition, INNER/RIGHT_SEMI) - queries with
	//! multi-column join conditions (e.g. Q9's ps_partkey=l_partkey AND ps_suppkey=l_suppkey) are
	//! *not* eligible (see design doc Q6 open question) and are excluded from this count.
	idx_t expected_bhj_hits;
};

// Some queries have hand-counted expected_bhj_hits for soft diagnostics.
static idx_t KnownExpectedBHJHits(int q) {
	// clang-format off
	switch (q) {
	case 5:  return 3;
	case 9:  return 2;
	case 10: return 2;
	default: return 0;
	}
	// clang-format on
}

duckdb::vector<QuerySpec> GetQueries(const std::vector<int> &query_ids) {
	duckdb::vector<QuerySpec> result;
	for (int q : query_ids) {
		result.push_back({string("Q") + std::to_string(q), TpchExtension::GetQuery(q), KnownExpectedBHJHits(q)});
	}
	return result;
}

//! Renders EXPLAIN output as a single string so it can be grepped for plan markers.
string ExplainText(Connection &con, const string &query) {
	auto result = con.Query("EXPLAIN " + query);
	if (result->HasError()) {
		fprintf(stderr, "EXPLAIN ERROR: %s\n", result->GetError().c_str());
	}
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

idx_t CountBitmapJoinHits(const string &explain_text) {
	idx_t count = 0;
	idx_t pos = 0;
	const string needle = "Bitmap Join: yes";
	while ((pos = explain_text.find(needle, pos)) != string::npos) {
		count++;
		pos += needle.size();
	}
	return count;
}

idx_t CountHashJoins(const string &explain_text) {
	idx_t count = 0;
	idx_t pos = 0;
	const string needle = "HASH_JOIN";
	while ((pos = explain_text.find(needle, pos)) != string::npos) {
		count++;
		pos += needle.size();
	}
	return count;
}

struct RunOutcome {
	double cold_ms = 0.0;      //! First run (cold: includes parquet metadata load, plan construction, BHJ executor allocation)
	double warm_avg_ms = 0.0;  //! Average of 3 subsequent warm runs (OS file cache hot, plan cached)
	double warm_min_ms = 0.0;  //! Best warm run (min noise)
	double warm_max_ms = 0.0;  //! Worst warm run (max noise)
	idx_t bhj_hits = 0;
	idx_t hash_join_count = 0;
	duckdb::unique_ptr<MaterializedQueryResult> result;
};

static double TimedQuery(Connection &con, const string &sql) {
	auto start = std::chrono::steady_clock::now();
	auto result = con.Query(sql);
	auto end = std::chrono::steady_clock::now();
	REQUIRE_NO_FAIL(*result);
	return std::chrono::duration<double, std::milli>(end - start).count();
}

RunOutcome RunQuery(Connection &con, const string &sql, bool open_perfect, bool open_bitmap) {
	REQUIRE_NO_FAIL(*con.Query(string("SET open_perfect_join=") + (open_perfect ? "true" : "false")));
	REQUIRE_NO_FAIL(*con.Query(string("SET open_bitmap_join=") + (open_bitmap ? "true" : "false")));

	RunOutcome out;
	auto plan = ExplainText(con, sql);
	out.bhj_hits = CountBitmapJoinHits(plan);
	out.hash_join_count = CountHashJoins(plan);

	// Cold run: first execution - includes parquet metadata load, plan optimization,
	// BitmapJoinExecutor construction (bitmap + payload_columns allocation), etc.
	out.cold_ms = TimedQuery(con, sql);

	// Warm runs: 3 subsequent executions with OS file cache hot and plan cached.
	double warm_times[3];
	for (int i = 0; i < 3; i++) {
		warm_times[i] = TimedQuery(con, sql);
	}
	out.warm_min_ms = std::min({warm_times[0], warm_times[1], warm_times[2]});
	out.warm_max_ms = std::max({warm_times[0], warm_times[1], warm_times[2]});
	out.warm_avg_ms = (warm_times[0] + warm_times[1] + warm_times[2]) / 3.0;

	// Keep the last result for correctness comparison.
	auto result = con.Query(sql);
	REQUIRE_NO_FAIL(*result);
	out.result = duckdb::unique_ptr<MaterializedQueryResult>(static_cast<MaterializedQueryResult *>(result.release()));
	return out;
}

bool ValuesApproxEqual(const Value &a, const Value &b) {
	if (a.IsNull() || b.IsNull()) {
		return a.IsNull() == b.IsNull();
	}
	if (a.type().IsNumeric() && b.type().IsNumeric()) {
		double da, db;
		try {
			da = a.GetValue<double>();
			db = b.GetValue<double>();
		} catch (...) {
			return a.ToString() == b.ToString();
		}
		double scale = std::max<double>(1.0, std::max(std::fabs(da), std::fabs(db)));
		return std::fabs(da - db) <= 1e-4 * scale;
	}
	return a.ToString() == b.ToString();
}

//! Hard correctness check: identical row/column counts and (tolerant-for-floats) cell values.
void RequireResultsMatch(const string &label_a, MaterializedQueryResult &a, const string &label_b,
                         MaterializedQueryResult &b) {
	INFO("Comparing " << label_a << " vs " << label_b);
	REQUIRE(a.ColumnCount() == b.ColumnCount());
	REQUIRE(a.RowCount() == b.RowCount());
	for (idx_t r = 0; r < a.RowCount(); r++) {
		for (idx_t c = 0; c < a.ColumnCount(); c++) {
			auto va = a.GetValue(c, r);
			auto vb = b.GetValue(c, r);
			if (!ValuesApproxEqual(va, vb)) {
				INFO("Mismatch at row " << r << " col " << c << ": " << va.ToString() << " vs " << vb.ToString());
				REQUIRE(false);
			}
		}
	}
}

void WriteCsvRow(std::ofstream &csv, const string &query, const string &mode, idx_t hash_join_count, idx_t bhj_hits,
                 idx_t rows_out, double cold_ms, double warm_avg_ms, double warm_min_ms, double warm_max_ms) {
	csv << query << "," << mode << "," << hash_join_count << "," << bhj_hits << "," << rows_out << ","
	    << cold_ms << "," << warm_avg_ms << "," << warm_min_ms << "," << warm_max_ms << "\n";
}

} // namespace

TEST_CASE("Bitmap-Join (BHJ) end-to-end TPC-H queries (SF=5 parquet)", "[bitmap_join_tpch][.]") {
	if (!DatasetAvailable(kBitmapDataDir)) {
		std::cerr << "[bitmap-join-tpch] dataset directory '" << kBitmapDataDir << "' not found/incomplete - "
		          << "skipping test." << std::endl;
		return;
	}
	const string meta_path = string(kBitmapDataDir) + "/bitmap_join_meta.json";
	{
		auto fs = FileSystem::CreateLocal();
		if (!fs->FileExists(meta_path)) {
			std::cerr << "[bitmap-join-tpch] meta file '" << meta_path << "' not found - skipping test."
			          << std::endl;
			return;
		}
	}

	DuckDB db(nullptr);
	Connection con(db);
	CreateViews(con, kBitmapDataDir);

	auto &registry = BitmapJoinMetaRegistry::GetInstance();
	registry.Clear();
	registry.SetForceBitmapJoin(false);
	registry.SetForceResolvedPK("", "");
	registry.LoadFromJson(meta_path);

	auto query_ids = GetQueryIds();
	auto specs = GetQueries(query_ids);

	printf("Running TPC-H queries: ");
	for (size_t i = 0; i < query_ids.size(); i++) {
		if (i) {
			printf(", ");
		}
		printf("Q%d", query_ids[i]);
	}
	printf("\n");

	std::ofstream csv;
	{
		auto fs = FileSystem::CreateLocal();
		const string csv_dir = "/home/featurize/workspace/duckdb-cuda/b_idea/perf/sf5";
		fs->CreateDirectoriesRecursive(csv_dir);
		csv.open(csv_dir + "/tpch_q5_q9_q10.csv", std::ios::out | std::ios::trunc);
		csv << "query,mode,hash_join_count,bhj_hits,rows_out,cold_ms,warm_avg_ms,warm_min_ms,warm_max_ms\n";
	}

	printf("\n%-6s %-10s %10s %10s %10s %8s %8s %10s\n", "Query", "Mode", "Cold(ms)", "WarmAvg", "WarmMin", "#HJ", "#BHJ", "Rows");
	printf("------------------------------------------------------------------------\n");

	for (auto &spec : specs) {
		auto baseline = RunQuery(con, spec.sql, /*open_perfect=*/false, /*open_bitmap=*/false);
		auto perfect = RunQuery(con, spec.sql, /*open_perfect=*/true, /*open_bitmap=*/false);
		auto bitmap = RunQuery(con, spec.sql, /*open_perfect=*/false, /*open_bitmap=*/true);

		printf("%-6s %-10s %10.1f %10.1f %10.1f %8llu %8llu %10llu\n", spec.name.c_str(), "baseline",
		       baseline.cold_ms, baseline.warm_avg_ms, baseline.warm_min_ms,
		       (unsigned long long)baseline.hash_join_count, (unsigned long long)baseline.bhj_hits,
		       (unsigned long long)baseline.result->RowCount());
		printf("%-6s %-10s %10.1f %10.1f %10.1f %8llu %8llu %10llu\n", spec.name.c_str(), "perfect",
		       perfect.cold_ms, perfect.warm_avg_ms, perfect.warm_min_ms,
		       (unsigned long long)perfect.hash_join_count, (unsigned long long)perfect.bhj_hits,
		       (unsigned long long)perfect.result->RowCount());
		printf("%-6s %-10s %10.1f %10.1f %10.1f %8llu %8llu %10llu\n", spec.name.c_str(), "bitmap",
		       bitmap.cold_ms, bitmap.warm_avg_ms, bitmap.warm_min_ms,
		       (unsigned long long)bitmap.hash_join_count, (unsigned long long)bitmap.bhj_hits,
		       (unsigned long long)bitmap.result->RowCount());

		WriteCsvRow(csv, spec.name, "baseline", baseline.hash_join_count, baseline.bhj_hits,
		           baseline.result->RowCount(), baseline.cold_ms, baseline.warm_avg_ms, baseline.warm_min_ms, baseline.warm_max_ms);
		WriteCsvRow(csv, spec.name, "perfect", perfect.hash_join_count, perfect.bhj_hits,
		           perfect.result->RowCount(), perfect.cold_ms, perfect.warm_avg_ms, perfect.warm_min_ms, perfect.warm_max_ms);
		WriteCsvRow(csv, spec.name, "bitmap", bitmap.hash_join_count, bitmap.bhj_hits,
		           bitmap.result->RowCount(), bitmap.cold_ms, bitmap.warm_avg_ms, bitmap.warm_min_ms, bitmap.warm_max_ms);

		// --- 1. Correctness (hard requirement) ---
		RequireResultsMatch(spec.name + "/baseline", *baseline.result, spec.name + "/perfect", *perfect.result);
		RequireResultsMatch(spec.name + "/baseline", *baseline.result, spec.name + "/bitmap", *bitmap.result);

		// --- 2. BHJ hit-rate (soft diagnostic only) ---
		if (bitmap.bhj_hits < spec.expected_bhj_hits) {
			std::cerr << "[bitmap-join-tpch] WARNING: " << spec.name << " expected >= " << spec.expected_bhj_hits
			          << " Bitmap Join hits, got " << bitmap.bhj_hits << " (out of " << bitmap.hash_join_count
			          << " HASH_JOIN nodes). This does not fail the test - see design doc 条目5 §5.3 point 2."
			          << std::endl;
		}

		// --- 3. Performance (soft diagnostic only) ---
		if (baseline.warm_avg_ms > 0 && bitmap.warm_avg_ms > baseline.warm_avg_ms * 1.2) {
			std::cerr << "[bitmap-join-tpch] WARNING: " << spec.name << " bitmap mode (warm_avg=" << bitmap.warm_avg_ms
			          << " ms, cold=" << bitmap.cold_ms << " ms) is >20% slower than baseline (warm_avg="
			          << baseline.warm_avg_ms << " ms, cold=" << baseline.cold_ms << " ms)." << std::endl;
		}
	}

	printf("--------------------------------------------------------------\n");
	printf("CSV written to b_idea/perf/sf5/tpch_q5_q9_q10.csv\n");

	registry.Clear();
}

// Cross-validates the bitmap-augmented dataset's baseline (open_perfect_join=false,
// open_bitmap_join=false) results against the *original*, un-augmented SF=5 dataset
// (data/tpch_sf5, no `_rowid`/`*_ref` columns, no VIEWs/registry involved at all) - the strongest
// available anchor that add_bitmap_columns.py's preprocessing didn't itself corrupt any data
// (design doc §5.3 point 5).
TEST_CASE("Bitmap-Join (BHJ) TPC-H queries cross-validation against the raw (non-bitmap) SF=5 dataset",
          "[bitmap_join_tpch][.]") {
	if (!DatasetAvailable(kBitmapDataDir) || !DatasetAvailable(kRawDataDir)) {
		std::cerr << "[bitmap-join-tpch] one of the two SF=5 dataset directories is missing/incomplete - "
		          << "skipping cross-validation test." << std::endl;
		return;
	}

	DuckDB bitmap_db(nullptr);
	Connection bitmap_con(bitmap_db);
	CreateViews(bitmap_con, kBitmapDataDir);
	REQUIRE_NO_FAIL(*bitmap_con.Query("SET open_perfect_join=false"));
	REQUIRE_NO_FAIL(*bitmap_con.Query("SET open_bitmap_join=false"));

	DuckDB raw_db(nullptr);
	Connection raw_con(raw_db);
	CreateViews(raw_con, kRawDataDir);

	auto specs = GetQueries(GetQueryIds());
	for (auto &spec : specs) {
		auto bitmap_result = bitmap_con.Query(spec.sql);
		REQUIRE_NO_FAIL(*bitmap_result);
		auto raw_result = raw_con.Query(spec.sql);
		REQUIRE_NO_FAIL(*raw_result);
		RequireResultsMatch(spec.name + "/bitmap-dataset-baseline", bitmap_result->Cast<MaterializedQueryResult>(),
		                    spec.name + "/raw-dataset", raw_result->Cast<MaterializedQueryResult>());
	}
}
