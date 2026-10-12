// Tests for the automatic Bitmap-Join (BHJ) resolution path (design doc
// b_idea/6.4/6.4后续进展-实现与测试方案.md, 条目1/2).
//
// Unlike test_bitmap_join_perf.cpp (which depends on the SF=5 parquet dataset and the
// test-only BitmapJoinMetaRegistry::SetForceResolvedPK / SetForceBitmapJoin overrides), these
// tests exercise the real BitmapJoinResolver optimizer pass end-to-end:
//   * PK/FK bindings are registered via RegisterPK/RegisterFK (as add_bitmap_columns.py would
//     via LoadFromJson), never via the test-only force override.
//   * BHJ is enabled purely via `SET open_bitmap_join=true`.
//   * The PK (dimension) table's key column is dense starting at rowid_offset == 0, so the
//     BitmapJoinExecutor fast path (rowid_column == pk_column) applies without needing a
//     materialized `_rowid` column.
//
// All tests are self-contained (plain CREATE TABLE, no external dataset) and run by default
// (no `[.]` hidden tag).

#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/bitmap_join_meta.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/query_result.hpp"

using namespace duckdb;

namespace {

//! RAII guard guaranteeing the process-global registry is reset even if a REQUIRE throws.
struct RegistryResetGuard {
	RegistryResetGuard() {
		Reset();
	}
	~RegistryResetGuard() {
		Reset();
	}
	static void Reset() {
		auto &reg = BitmapJoinMetaRegistry::GetInstance();
		reg.SetForceBitmapJoin(false);
		reg.SetForceResolvedPK("", "");
		reg.Clear();
	}
};

string ExplainText(Connection &con, const string &query) {
	// FORMAT JSON avoids the box-drawing renderer's line-wrapping of long extra_info values
	// (e.g. "Bitmap Join: no (condition_not_plain_column)" would otherwise get split across
	// multiple rendered cells/lines), so plain substring checks on the concatenated text stay
	// reliable regardless of value length.
	auto result = con.Query("EXPLAIN (FORMAT JSON) " + query);
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

struct AggResult {
	int64_t cnt = 0;
	int64_t sum_v = 0;
};

AggResult RunAgg(Connection &con, const string &query) {
	auto result = con.Query(query);
	REQUIRE(!result->HasError());
	REQUIRE(result->RowCount() == 1);
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() == 1);
	AggResult out;
	out.cnt = chunk->GetValue(0, 0).GetValue<int64_t>();
	auto sum_val = chunk->GetValue(1, 0);
	out.sum_v = sum_val.IsNull() ? 0 : sum_val.GetValue<int64_t>();
	return out;
}

} // namespace

TEST_CASE("BitmapJoinResolver auto-detects an eligible join (positive path)", "[bitmap_join]") {
	RegistryResetGuard guard;
	auto &reg = BitmapJoinMetaRegistry::GetInstance();

	DuckDB db(nullptr);
	Connection con(db);

	// PK (dimension) table: 3 rows, dense key 0..2 - the BitmapJoinExecutor fast path applies
	// directly to `k` (rowid_column == pk_column, rowid_offset == 0).
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE bhj_auto_dim(k INTEGER, name VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO bhj_auto_dim VALUES (0,'a'),(1,'b'),(2,'c')"));

	// FK (fact) table: larger than the dimension table, so BuildProbeSideOptimizer naturally
	// keeps/places the dimension table on the build side.
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE bhj_auto_fact(k INTEGER, v INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO bhj_auto_fact VALUES "
	                          "(0,10),(1,20),(2,30),(0,40),(1,50),(2,60),"
	                          "(0,70),(1,80),(2,90),(0,100),(1,110),(2,120)"));

	const string query = "SELECT count(*) AS cnt, sum(bhj_auto_fact.v) AS sum_v "
	                      "FROM bhj_auto_fact JOIN bhj_auto_dim ON bhj_auto_fact.k = bhj_auto_dim.k";

	// Baseline: open_bitmap_join off entirely.
	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=false"));
	auto baseline = RunAgg(con, query);
	REQUIRE(baseline.cnt == 12);
	REQUIRE(baseline.sum_v == 780);
	REQUIRE(!StringUtil::Contains(ExplainText(con, query), "Bitmap Join"));

	// Register the real PK/FK bindings (mirrors add_bitmap_columns.py's LoadFromJson output) and
	// flip on the user-facing switch - no SetForceBitmapJoin / SetForceResolvedPK involved.
	BitmapJoinPKBinding pk;
	pk.pk_table = "bhj_auto_dim";
	pk.pk_column = "k";
	pk.rowid_column = "k";
	pk.rowid_offset = 0;
	pk.row_count = 3;
	reg.RegisterPK(pk);

	BitmapJoinFKBinding fk;
	fk.fk_table = "bhj_auto_fact";
	fk.fk_column = "k";
	fk.ref_column = "k_ref"; // unused by the fast path, but required to describe the binding.
	fk.pk_table = "bhj_auto_dim";
	fk.pk_column = "k";
	reg.RegisterFK(fk);

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));
	REQUIRE(StringUtil::Contains(ExplainText(con, query), "Bitmap Join"));

	auto bhj = RunAgg(con, query);
	REQUIRE(bhj.cnt == baseline.cnt);
	REQUIRE(bhj.sum_v == baseline.sum_v);
}

TEST_CASE("BitmapJoinResolver falls back safely when the FK table lands on the build side", "[bitmap_join]") {
	RegistryResetGuard guard;
	auto &reg = BitmapJoinMetaRegistry::GetInstance();

	DuckDB db(nullptr);
	Connection con(db);

	// PK (dimension) table: deliberately large (1000 rows, dense key 0..999).
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE bhj_neg_dim(k INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO bhj_neg_dim SELECT range FROM range(1000)"));

	// FK (fact) table: deliberately tiny, so BuildProbeSideOptimizer places *it* on the build
	// side (the cheaper table to build a hash table on), which violates BHJ's precondition that
	// the PK table is on the build side.
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE bhj_neg_fk(k INTEGER, v INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO bhj_neg_fk VALUES (0,1),(1,2),(2,3),(3,4),(4,5)"));

	BitmapJoinPKBinding pk;
	pk.pk_table = "bhj_neg_dim";
	pk.pk_column = "k";
	pk.rowid_column = "k";
	pk.rowid_offset = 0;
	pk.row_count = 1000;
	reg.RegisterPK(pk);

	BitmapJoinFKBinding fk;
	fk.fk_table = "bhj_neg_fk";
	fk.fk_column = "k";
	fk.ref_column = "k_ref";
	fk.pk_table = "bhj_neg_dim";
	fk.pk_column = "k";
	reg.RegisterFK(fk);

	const string query = "SELECT count(*) AS cnt, sum(bhj_neg_fk.v) AS sum_v "
	                      "FROM bhj_neg_fk JOIN bhj_neg_dim ON bhj_neg_fk.k = bhj_neg_dim.k";

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));

	// The resolver must detect build_is_pk_side == false and leave bhj_hint unset: no "yes"
	// marker, no exception, correct (regular hash join) results. EXPLAIN now (b_idea/6.4遗漏
	// 问题 条目6) does show a diagnostic "no (fk_on_build_side)" reason instead of nothing at
	// all - assert that specific reason to double-check the diagnostic itself is accurate.
	// FORMAT JSON renders extra_info as `"Bitmap Join": "no (...)"` (quote-colon-space, not
	// "Bitmap Join: ") - match that exact shape.
	auto plan = ExplainText(con, query);
	REQUIRE(!StringUtil::Contains(plan, "\"Bitmap Join\": \"yes\""));
	REQUIRE(StringUtil::Contains(plan, "\"Bitmap Join\""));
	REQUIRE(StringUtil::Contains(plan, "no (fk_on_build_side)"));

	auto result = RunAgg(con, query);
	REQUIRE(result.cnt == 5);
	REQUIRE(result.sum_v == 15);
}

TEST_CASE("BitmapJoinResolver has no effect on unrelated (unregistered) joins", "[bitmap_join]") {
	RegistryResetGuard guard;

	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE bhj_neutral_a(x INTEGER, v INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE bhj_neutral_b(x INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO bhj_neutral_a VALUES (0,1),(1,2),(2,3)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO bhj_neutral_b VALUES (0),(1),(2)"));

	const string query = "SELECT count(*) AS cnt, sum(bhj_neutral_a.v) AS sum_v "
	                      "FROM bhj_neutral_a JOIN bhj_neutral_b ON bhj_neutral_a.x = bhj_neutral_b.x";

	// The registry is empty (no bindings registered) - open_bitmap_join=true must be a pure
	// no-op for this query: no "yes" marker, no crash, correct results. Diagnostic reason
	// (条目6) should be catalog_not_registered.
	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));
	auto plan = ExplainText(con, query);
	REQUIRE(!StringUtil::Contains(plan, "\"Bitmap Join\": \"yes\""));
	REQUIRE(StringUtil::Contains(plan, "\"Bitmap Join\""));
	REQUIRE(StringUtil::Contains(plan, "catalog_not_registered"));

	auto result = RunAgg(con, query);
	REQUIRE(result.cnt == 3);
	REQUIRE(result.sum_v == 6);
}

TEST_CASE("BitmapJoinResolver falls back safely for non-column-reference join conditions", "[bitmap_join]") {
	RegistryResetGuard guard;
	auto &reg = BitmapJoinMetaRegistry::GetInstance();

	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE bhj_expr_dim(k INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO bhj_expr_dim VALUES (0),(1),(2)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE bhj_expr_fact(k INTEGER, v INTEGER)"));
	// fact.k + 1 == dim.k for every row (k = -1 -> 0, 0 -> 1, 1 -> 2).
	REQUIRE_NO_FAIL(con.Query("INSERT INTO bhj_expr_fact VALUES (-1,10),(0,20),(1,30)"));

	BitmapJoinPKBinding pk;
	pk.pk_table = "bhj_expr_dim";
	pk.pk_column = "k";
	pk.rowid_column = "k";
	pk.rowid_offset = 0;
	pk.row_count = 3;
	reg.RegisterPK(pk);

	BitmapJoinFKBinding fk;
	fk.fk_table = "bhj_expr_fact";
	fk.fk_column = "k";
	fk.ref_column = "k_ref";
	fk.pk_table = "bhj_expr_dim";
	fk.pk_column = "k";
	reg.RegisterFK(fk);

	// The FK side is wrapped in an arithmetic expression (not a plain column reference):
	// GetPlainColumnBinding() must reject it, so the resolver leaves bhj_hint unset even though
	// the tables/columns are otherwise bound.
	const string query = "SELECT count(*) AS cnt, sum(bhj_expr_fact.v) AS sum_v "
	                      "FROM bhj_expr_fact JOIN bhj_expr_dim "
	                      "ON bhj_expr_fact.k + 1 = bhj_expr_dim.k";

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));
	auto plan = ExplainText(con, query);
	REQUIRE(!StringUtil::Contains(plan, "\"Bitmap Join\": \"yes\""));
	REQUIRE(StringUtil::Contains(plan, "\"Bitmap Join\""));
	REQUIRE(StringUtil::Contains(plan, "condition_not_plain_column"));

	auto result = RunAgg(con, query);
	REQUIRE(result.cnt == 3);
	REQUIRE(result.sum_v == 60);
}
