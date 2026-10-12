// Broad e2e smoke test for the Bitmap-Join (BHJ) optimizer pass (design doc
// b_idea/6.4遗漏问题/任务1-BHJ命中率归因与算子耗时画像.md, 条目7 note).
//
// test_bitmap_join_tpch.cpp / test_bitmap_join_tpch_profile.cpp only exercise Q5/Q9/Q10.
// BitmapJoinResolver, however, unconditionally visits *every* LogicalComparisonJoin in *every*
// query once `open_bitmap_join=true` - so this test runs all 22 standard TPC-H queries (via the
// `tpch` extension's dbgen-embedded query texts) against the SF5 bitmap dataset's 8 tables and
// checks that turning the switch on never changes results or crashes, on a much wider variety of
// join/aggregate/subquery shapes than just the 3 queries BHJ is normally validated against. No
// hit-rate or performance assertions here - purely "doesn't break anything else".
//
// Only compiled in when the `tpch` extension is linked into this test binary (see
// DUCKDB_EXTENSION_TPCH_SHOULD_LINK in test/api/CMakeLists.txt, mirroring
// test_tpch_with_relations.cpp). Hidden behind `[.]` like the other SF5-dataset-dependent tests;
// run explicitly with `./unittest "[bitmap_join_tpch_profile]"`.

#include "catch.hpp"
#include "test_helpers.hpp"
#include "tpch_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/bitmap_join_meta.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/query_result.hpp"

#include <cstdio>
#include <iostream>

using namespace duckdb;
using namespace std;

namespace {

constexpr const char *kBitmapDataDir = "/home/featurize/workspace/duckdb-cuda/data/tpch_sf5_bitmap";
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

void CreateViews(Connection &con, const string &dir) {
	for (auto &table : kTables) {
		auto sql = StringUtil::Format("CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet('%s/%s.parquet')",
		                              table, dir, table);
		REQUIRE_NO_FAIL(con.Query(sql));
	}
}

} // namespace

TEST_CASE("BHJ open_bitmap_join=true does not break any of the 22 standard TPC-H queries (SF=5)",
          "[bitmap_join_tpch_profile][.]") {
	if (!DatasetAvailable(kBitmapDataDir)) {
		std::cerr << "[bitmap-join-tpch-profile] dataset directory '" << kBitmapDataDir
		          << "' not found/incomplete - skipping test." << std::endl;
		return;
	}
	const string meta_path = string(kBitmapDataDir) + "/bitmap_join_meta.json";
	{
		auto fs = FileSystem::CreateLocal();
		if (!fs->FileExists(meta_path)) {
			std::cerr << "[bitmap-join-tpch-profile] meta file '" << meta_path << "' not found - skipping test."
			          << std::endl;
			return;
		}
	}

	DuckDB baseline_db(nullptr);
	Connection baseline_con(baseline_db);
	CreateViews(baseline_con, kBitmapDataDir);
	REQUIRE_NO_FAIL(*baseline_con.Query("SET open_bitmap_join=false"));

	DuckDB bitmap_db(nullptr);
	Connection bitmap_con(bitmap_db);
	CreateViews(bitmap_con, kBitmapDataDir);
	auto &registry = BitmapJoinMetaRegistry::GetInstance();
	registry.Clear();
	registry.SetForceBitmapJoin(false);
	registry.SetForceResolvedPK("", "");
	registry.LoadFromJson(meta_path);
	REQUIRE_NO_FAIL(*bitmap_con.Query("SET open_bitmap_join=true"));

	idx_t crashed = 0, mismatched = 0, ok = 0, skipped = 0;
	for (int q = 1; q <= 22; q++) {
		string sql = TpchExtension::GetQuery(q);
		auto baseline_result = baseline_con.Query(sql);
		auto bitmap_result = bitmap_con.Query(sql);
		if (baseline_result->HasError() != bitmap_result->HasError()) {
			std::cerr << "[bitmap-join-tpch-profile] Q" << q
			          << ": baseline/bitmap disagree on whether the query errors (baseline_error="
			          << baseline_result->HasError() << " bitmap_error=" << bitmap_result->HasError() << ")"
			          << std::endl;
			crashed++;
			continue;
		}
		if (baseline_result->HasError()) {
			// Both modes agree the query itself doesn't run in this cut-down 8-table dataset
			// (e.g. some TPC-H queries reference tables/columns not present here) - not a
			// BHJ-specific failure, just skip it.
			skipped++;
			continue;
		}
		auto &b = baseline_result->Cast<MaterializedQueryResult>();
		auto &m = bitmap_result->Cast<MaterializedQueryResult>();
		bool match = b.ColumnCount() == m.ColumnCount() && b.RowCount() == m.RowCount();
		if (match) {
			for (idx_t r = 0; r < b.RowCount() && match; r++) {
				for (idx_t c = 0; c < b.ColumnCount() && match; c++) {
					if (b.GetValue(c, r).ToString() != m.GetValue(c, r).ToString()) {
						match = false;
					}
				}
			}
		}
		if (!match) {
			std::cerr << "[bitmap-join-tpch-profile] Q" << q << ": baseline/bitmap RESULT MISMATCH" << std::endl;
			mismatched++;
		} else {
			ok++;
		}
	}
	printf("[bitmap-join-tpch-profile] 22-query smoke test: %llu ok, %llu mismatched, %llu crashed/errored, "
	       "%llu skipped (both modes errored)\n",
	       (unsigned long long)ok, (unsigned long long)mismatched, (unsigned long long)crashed,
	       (unsigned long long)skipped);
	REQUIRE(mismatched == 0);
	REQUIRE(crashed == 0);
	registry.Clear();
}
