// Tests for the Bitmap-Join (BHJ) hidden `_rowid`/`*_ref` column path (design doc
// b_idea/6.4/6.4后续进展-实现与测试方案.md, 条目3).
//
// Unlike test_bitmap_join_auto_resolve.cpp (条目1/2), the PK (dimension) table's join column is
// deliberately NOT dense (rowid_column != pk_column): the true "rowid" role is carried by a
// separately materialized `rid` column on the build side and a matching `ref` column on the
// probe (fact) side. This exercises BitmapJoinResolver's Step B/F: injecting the hidden columns
// via LogicalGet::AddColumnId and propagating them up through LOGICAL_FILTER / LOGICAL_PROJECTION
// chains, and PhysicalHashJoin::bitmap_build_rowid_idx / bitmap_probe_ref_idx actually being wired
// up and consumed by BitmapJoinExecutor.
//
// All tests are self-contained (plain CREATE TABLE, no external dataset) and run by default (no
// `[.]` hidden tag).

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

//! Creates the dimension ("orders"-like: sparse pk, dense hidden rid) and fact ("lineitem"-like:
//! fk mirrors the sparse pk, ref mirrors the dense rid) tables, and registers the corresponding
//! PK/FK bindings. Shared setup for all cases in this file.
void SetupSchema(Connection &con, BitmapJoinMetaRegistry &reg) {
	// Dimension table: business key `k` is sparse (100, 200, 300) - NOT usable directly as a
	// rowid. `rid` is the separately materialized dense rowid (0-based).
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE bhj_rr_dim(k INTEGER, rid INTEGER, name VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO bhj_rr_dim VALUES (100,0,'a'),(200,1,'b'),(300,2,'c')"));

	// Fact table: `k` mirrors the dimension's sparse business key, `ref` mirrors `rid`.
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE bhj_rr_fact(k INTEGER, ref INTEGER, v INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO bhj_rr_fact VALUES "
	                          "(100,0,10),(200,1,20),(300,2,30),(100,0,40),(200,1,50),(300,2,60),"
	                          "(100,0,70),(200,1,80),(300,2,90),(100,0,100),(200,1,110),(300,2,120)"));

	BitmapJoinPKBinding pk;
	pk.pk_table = "bhj_rr_dim";
	pk.pk_column = "k";
	pk.rowid_column = "rid"; // != pk_column -> triggers the 条目3 hidden-column path.
	pk.rowid_offset = 0;
	pk.row_count = 3;
	reg.RegisterPK(pk);

	BitmapJoinFKBinding fk;
	fk.fk_table = "bhj_rr_fact";
	fk.fk_column = "k";
	fk.ref_column = "ref";
	fk.pk_table = "bhj_rr_dim";
	fk.pk_column = "k";
	reg.RegisterFK(fk);
}

} // namespace

TEST_CASE("BHJ hidden rowid/ref: no filter/projection (orders-like sparse PK)", "[bitmap_join]") {
	RegistryResetGuard guard;
	auto &reg = BitmapJoinMetaRegistry::GetInstance();

	DuckDB db(nullptr);
	Connection con(db);
	SetupSchema(con, reg);

	const string query = "SELECT count(*) AS cnt, sum(bhj_rr_fact.v) AS sum_v "
	                      "FROM bhj_rr_fact JOIN bhj_rr_dim ON bhj_rr_fact.k = bhj_rr_dim.k";

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=false"));
	auto baseline = RunAgg(con, query);
	REQUIRE(baseline.cnt == 12);
	REQUIRE(baseline.sum_v == 780);

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));
	REQUIRE(StringUtil::Contains(ExplainText(con, query), "Bitmap Join"));

	auto bhj = RunAgg(con, query);
	REQUIRE(bhj.cnt == baseline.cnt);
	REQUIRE(bhj.sum_v == baseline.sum_v);
}

TEST_CASE("BHJ hidden rowid/ref: build side has filter+projection", "[bitmap_join]") {
	RegistryResetGuard guard;
	auto &reg = BitmapJoinMetaRegistry::GetInstance();

	DuckDB db(nullptr);
	Connection con(db);
	SetupSchema(con, reg);

	// The dimension side is wrapped in a derived table that filters and explicitly projects away
	// `rid` - the hidden `_rowid` column must be re-injected and propagate through both the
	// LOGICAL_FILTER and the LOGICAL_PROJECTION back up to the join.
	const string query =
	    "SELECT count(*) AS cnt, sum(f.v) AS sum_v "
	    "FROM bhj_rr_fact f JOIN (SELECT k, name FROM bhj_rr_dim WHERE name <> 'zzz') d ON f.k = d.k";

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=false"));
	auto baseline = RunAgg(con, query);
	REQUIRE(baseline.cnt == 12);
	REQUIRE(baseline.sum_v == 780);

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));
	REQUIRE(StringUtil::Contains(ExplainText(con, query), "Bitmap Join"));

	auto bhj = RunAgg(con, query);
	REQUIRE(bhj.cnt == baseline.cnt);
	REQUIRE(bhj.sum_v == baseline.sum_v);
}

TEST_CASE("BHJ hidden rowid/ref: probe side has filter+projection", "[bitmap_join]") {
	RegistryResetGuard guard;
	auto &reg = BitmapJoinMetaRegistry::GetInstance();

	DuckDB db(nullptr);
	Connection con(db);
	SetupSchema(con, reg);
	// Pad the fact table with rows that fail `v > 0` (excluded from every aggregate below) so
	// its *pre-filter* cardinality stays comfortably above the dimension table's 3 rows even
	// after the optimizer's default filter-selectivity guess - otherwise BuildProbeSideOptimizer
	// can (correctly, per its own cost model) flip which side lands on build, which would make
	// this test exercise the pre-existing "FK landed on build side -> fall back" path instead of
	// the filter+projection propagation path it's meant to cover.
	REQUIRE_NO_FAIL(con.Query("INSERT INTO bhj_rr_fact SELECT 100, 0, -1 FROM range(20)"));

	// The fact side is wrapped in a derived table that filters and explicitly projects away
	// `ref` - the hidden `*_ref` column must be re-injected and propagate the same way.
	const string query = "SELECT count(*) AS cnt, sum(f.v) AS sum_v "
	                      "FROM (SELECT k, v FROM bhj_rr_fact WHERE v > 0) f JOIN bhj_rr_dim d ON f.k = d.k";

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=false"));
	auto baseline = RunAgg(con, query);
	REQUIRE(baseline.cnt == 12);
	REQUIRE(baseline.sum_v == 780);

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));
	REQUIRE(StringUtil::Contains(ExplainText(con, query), "Bitmap Join"));

	auto bhj = RunAgg(con, query);
	REQUIRE(bhj.cnt == baseline.cnt);
	REQUIRE(bhj.sum_v == baseline.sum_v);
}

TEST_CASE("BHJ hidden rowid/ref: both sides have filter+projection", "[bitmap_join]") {
	RegistryResetGuard guard;
	auto &reg = BitmapJoinMetaRegistry::GetInstance();

	DuckDB db(nullptr);
	Connection con(db);
	SetupSchema(con, reg);

	const string query =
	    "SELECT count(*) AS cnt, sum(f.v) AS sum_v "
	    "FROM (SELECT k, v FROM bhj_rr_fact WHERE v > 0) f "
	    "JOIN (SELECT k, name FROM bhj_rr_dim WHERE name <> 'zzz') d ON f.k = d.k";

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=false"));
	auto baseline = RunAgg(con, query);
	REQUIRE(baseline.cnt == 12);
	REQUIRE(baseline.sum_v == 780);

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));
	REQUIRE(StringUtil::Contains(ExplainText(con, query), "Bitmap Join"));

	auto bhj = RunAgg(con, query);
	REQUIRE(bhj.cnt == baseline.cnt);
	REQUIRE(bhj.sum_v == baseline.sum_v);
}

TEST_CASE("BHJ hidden rowid/ref: repeated planning is idempotent", "[bitmap_join]") {
	RegistryResetGuard guard;
	auto &reg = BitmapJoinMetaRegistry::GetInstance();

	DuckDB db(nullptr);
	Connection con(db);
	SetupSchema(con, reg);

	const string query = "SELECT count(*) AS cnt, sum(bhj_rr_fact.v) AS sum_v "
	                      "FROM bhj_rr_fact JOIN bhj_rr_dim ON bhj_rr_fact.k = bhj_rr_dim.k";

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));

	// Re-plan and re-execute the same query several times (each call independently invokes the
	// binder + optimizer, including BitmapJoinResolver, from scratch): AddColumnId's idempotency
	// guard (EnsureGetColumn in bitmap_join_resolver.cpp) must never end up scanning `rid`/`ref`
	// twice or otherwise corrupting the plan, so results must stay identical every time.
	for (int i = 0; i < 5; i++) {
		REQUIRE(StringUtil::Contains(ExplainText(con, query), "Bitmap Join"));
		auto result = RunAgg(con, query);
		REQUIRE(result.cnt == 12);
		REQUIRE(result.sum_v == 780);
	}
}

TEST_CASE("BHJ hidden rowid/ref: SELECT * does not leak the hidden columns", "[bitmap_join]") {
	RegistryResetGuard guard;
	auto &reg = BitmapJoinMetaRegistry::GetInstance();

	DuckDB db(nullptr);
	Connection con(db);
	SetupSchema(con, reg);

	// The dimension-side derived table explicitly excludes `rid` from its SELECT list. Even
	// though BitmapJoinResolver forces a hidden re-scan of `rid` and appends it to the
	// LogicalProjection's own expression list to propagate it up to the join,
	// that append must stay internal (never surface in `d`'s visible column bindings) - so a
	// `SELECT *` over the whole join must not show it.
	const string query =
	    "SELECT * FROM bhj_rr_fact f JOIN (SELECT k, name FROM bhj_rr_dim WHERE name <> 'zzz') d ON f.k = d.k";

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));
	REQUIRE(StringUtil::Contains(ExplainText(con, query), "Bitmap Join"));

	auto result = con.Query(query);
	REQUIRE(!result->HasError());
	// Expected visible columns: fact.(k, ref, v) + d.(k, name) = 5 - no extra hidden column.
	REQUIRE(result->ColumnCount() == 5);
	for (auto &name : result->names) {
		REQUIRE(name != "rid");
		REQUIRE(name != "_rowid");
	}
	REQUIRE(result->RowCount() == 12);
}
