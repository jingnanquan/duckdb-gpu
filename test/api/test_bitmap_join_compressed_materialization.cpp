// Test for 条目8b (b_idea/6.4遗漏问题 任务2): CompressedMaterialization must not compress a
// single-equality INNER join's own condition columns when `open_bitmap_join=true`, so that
// BitmapJoinResolver::TraceBindingToGet (which runs later, as the very last built-in optimizer
// pass) still sees a plain column reference instead of an `__internal_compress_integral_*`
// wrapper - this is exactly what silently disabled BHJ for 3 real join nodes on TPCH SF5 Q9/Q10
// (see 任务1 条目6/7). The user explicitly prioritized query performance (BHJ) over
// intermediate-result compression for any join CompressedMaterialization *could* plausibly hand
// off to BHJ (compress_comparison_join.cpp's `skip_join_key_compression_for_bhj`).
//
// CompressComparisonJoin only even attempts to compress a join's condition columns once the
// build side's estimated cardinality reaches JOIN_BUILD_CARDINALITY_THRESHOLD (1,048,576, see
// compressed_materialization.hpp) - both tables below are sized past that on purpose so the
// baseline (open_bitmap_join=false) case is a real, triggered compression, not a vacuously-true
// "nothing to compress anyway" case.

#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/query_result.hpp"

using namespace duckdb;

namespace {

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

TEST_CASE("条目8b: CompressedMaterialization skips compressing an INNER join's equality-condition "
          "columns when open_bitmap_join=true",
          "[bitmap_join]") {
	DuckDB db(nullptr);
	Connection con(db);

	// >1M dense integer rows on both sides so CompressComparisonJoin's cardinality gate
	// (JOIN_BUILD_CARDINALITY_THRESHOLD) is actually crossed.
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE bhj_cm_dim AS SELECT range AS id FROM range(1100000)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE bhj_cm_fact AS SELECT range AS id FROM range(1100000)"));

	const string query = "SELECT count(*) FROM bhj_cm_fact f JOIN bhj_cm_dim d ON f.id = d.id";

	// Regression guard: with open_bitmap_join=false (the default), 条目8b's change must be a
	// complete no-op - compression should still trigger exactly as before.
	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=false"));
	{
		auto plan = ExplainText(con, query);
		REQUIRE(StringUtil::Contains(plan, "__internal_compress"));
	}

	// With open_bitmap_join=true, the join's own equality-condition columns must no longer be
	// compressed - this is the actual 条目8b behavior change.
	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));
	{
		auto plan = ExplainText(con, query);
		REQUIRE_FALSE(StringUtil::Contains(plan, "__internal_compress"));
	}
}

TEST_CASE("条目8b: query results are unaffected by the join-key-compression skip", "[bitmap_join]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE bhj_cm_dim AS SELECT range AS id, range * 2 AS val FROM range(1100000)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE bhj_cm_fact AS SELECT range % 1100000 AS id FROM range(1500000)"));

	const string query =
	    "SELECT count(*) AS cnt, sum(d.val) AS sum_val FROM bhj_cm_fact f JOIN bhj_cm_dim d ON f.id = d.id";

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=false"));
	auto baseline = con.Query(query);
	REQUIRE_NO_FAIL(*baseline);
	REQUIRE(baseline->RowCount() == 1);

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));
	auto with_flag = con.Query(query);
	REQUIRE_NO_FAIL(*with_flag);
	REQUIRE(with_flag->RowCount() == 1);

	auto &baseline_mat = baseline->Cast<MaterializedQueryResult>();
	auto &flag_mat = with_flag->Cast<MaterializedQueryResult>();
	REQUIRE(baseline_mat.GetValue(0, 0) == flag_mat.GetValue(0, 0));
	REQUIRE(baseline_mat.GetValue(1, 0) == flag_mat.GetValue(1, 0));
}
