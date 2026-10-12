// Tests for multi-join-chain hidden `_rowid`/`*_ref` propagation (design doc
// b_idea/6.4/6.4后续进展-实现与测试方案.md, 条目4): a Bitmap-Join (BHJ) candidate join whose
// build (or probe) side is not a bare LogicalGet but the *output of another, intermediate INNER
// join* still needs its hidden rowid/ref column to survive that intermediate join untouched, and
// two independently-scanned instances of the very same PK table (e.g. a self-join) must not let
// their "same physical column name" hidden columns collide (P0: everything keyed off
// ColumnBinding, never off column-name strings across tables).
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

//! Count how many times "Bitmap Join" shows up in an EXPLAIN listing - i.e. how many
//! PhysicalHashJoin nodes actually got wired up as BHJ (each hit join prints exactly one
//! "Bitmap Join: yes" line in its ParamsToString()).
idx_t CountBitmapJoinHits(const string &explain_text) {
	idx_t count = 0;
	idx_t pos = 0;
	const string needle = "Bitmap Join";
	while ((pos = explain_text.find(needle, pos)) != string::npos) {
		count++;
		pos += needle.size();
	}
	return count;
}

struct AggResult {
	int64_t cnt = 0;
	int64_t sum1 = 0;
	int64_t sum2 = 0;
};

AggResult RunAgg3(Connection &con, const string &query) {
	auto result = con.Query(query);
	REQUIRE(!result->HasError());
	REQUIRE(result->RowCount() == 1);
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	REQUIRE(chunk->size() == 1);
	AggResult out;
	out.cnt = chunk->GetValue(0, 0).GetValue<int64_t>();
	auto sum1_val = chunk->GetValue(1, 0);
	out.sum1 = sum1_val.IsNull() ? 0 : sum1_val.GetValue<int64_t>();
	auto sum2_val = chunk->GetValue(2, 0);
	out.sum2 = sum2_val.IsNull() ? 0 : sum2_val.GetValue<int64_t>();
	return out;
}

//! Registers a customer-like PK table with a *sparse* PK column (rowid_column != pk_column, so
// the hidden-column path - 条目3/4 - is always exercised, never the dense fast path) plus an
// orders-like FK table pointing at it, and creates/populates both. `nation` is optional scaffolding
// used by the chain test to build an extra intermediate join level.
void SetupChainSchema(Connection &con, BitmapJoinMetaRegistry &reg, bool with_nation) {
	if (with_nation) {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE bhj_chain_nation(nk INTEGER, name VARCHAR)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO bhj_chain_nation VALUES (0,'A'),(1,'B')"));
		REQUIRE_NO_FAIL(
		    con.Query("CREATE TABLE bhj_chain_customer(nk INTEGER, ck INTEGER, crid INTEGER, cname VARCHAR)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO bhj_chain_customer VALUES "
		                          "(0,100,0,'c1'),(1,200,1,'c2'),(0,300,2,'c3')"));

		BitmapJoinPKBinding nation_pk;
		nation_pk.pk_table = "bhj_chain_nation";
		nation_pk.pk_column = "nk";
		nation_pk.rowid_column = "nk"; // dense fast path - not the focus of this test
		nation_pk.rowid_offset = 0;
		nation_pk.row_count = 2;
		reg.RegisterPK(nation_pk);

		BitmapJoinFKBinding customer_nk_fk;
		customer_nk_fk.fk_table = "bhj_chain_customer";
		customer_nk_fk.fk_column = "nk";
		customer_nk_fk.ref_column = "nk_ref"; // unused (dense PK side), just needs to be non-empty
		customer_nk_fk.pk_table = "bhj_chain_nation";
		customer_nk_fk.pk_column = "nk";
		reg.RegisterFK(customer_nk_fk);
	} else {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE bhj_chain_customer(ck INTEGER, crid INTEGER, cname VARCHAR)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO bhj_chain_customer VALUES (100,0,'c1'),(200,1,'c2'),(300,2,'c3')"));
	}

	// customer.ck (PK) is deliberately *sparse*: crid (0,1,2) is the true dense rowid, mirroring
	// orders (rowid_column == "_rowid") in the real TPCH bitmap dataset.
	BitmapJoinPKBinding customer_pk;
	customer_pk.pk_table = "bhj_chain_customer";
	customer_pk.pk_column = "ck";
	customer_pk.rowid_column = "crid";
	customer_pk.rowid_offset = 0;
	customer_pk.row_count = 3;
	reg.RegisterPK(customer_pk);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE bhj_chain_orders(ck INTEGER, cref INTEGER, amt INTEGER)"));
	// 4 orders per customer (12 total) so the fact side clearly outweighs the (tiny) dimension
	// side(s) for BuildProbeSideOptimizer's cardinality-based build/probe assignment.
	REQUIRE_NO_FAIL(con.Query("INSERT INTO bhj_chain_orders VALUES "
	                          "(100,0,10),(200,1,20),(300,2,30),"
	                          "(100,0,40),(200,1,50),(300,2,60),"
	                          "(100,0,70),(200,1,80),(300,2,90),"
	                          "(100,0,100),(200,1,110),(300,2,120)"));

	BitmapJoinFKBinding orders_fk;
	orders_fk.fk_table = "bhj_chain_orders";
	orders_fk.fk_column = "ck";
	orders_fk.ref_column = "cref"; // exercised: customer's PK is sparse, so BHJ needs this.
	orders_fk.pk_table = "bhj_chain_customer";
	orders_fk.pk_column = "ck";
	reg.RegisterFK(orders_fk);
}

} // namespace

TEST_CASE("BHJ hidden column propagates through an intermediate INNER join", "[bitmap_join]") {
	RegistryResetGuard guard;
	auto &reg = BitmapJoinMetaRegistry::GetInstance();

	DuckDB db(nullptr);
	Connection con(db);
	SetupChainSchema(con, reg, /*with_nation=*/true);

	// The BHJ-candidate join's build side is not a bare table scan but the *output of another
	// INNER join* (`nation JOIN customer`) - customer's hidden `crid` column has to survive that
	// intermediate join to reach this outer join's PhysicalHashJoin.
	const string query = "SELECT count(*) AS cnt, sum(o.amt) AS sum_amt, 0 AS unused "
	                      "FROM bhj_chain_orders o "
	                      "JOIN (bhj_chain_nation n JOIN bhj_chain_customer c ON n.nk = c.nk) "
	                      "ON o.ck = c.ck";

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=false"));
	auto baseline = RunAgg3(con, query);
	REQUIRE(baseline.cnt == 12);
	REQUIRE(baseline.sum1 == 780);
	REQUIRE(!StringUtil::Contains(ExplainText(con, query), "Bitmap Join"));

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));
	auto plan = ExplainText(con, query);
	// At least the outer join (orders <-> nation/customer subtree) must hit BHJ - the whole
	// point of this test. The inner (nation<->customer) join may or may not also hit BHJ
	// (nation's PK is dense, so it's eligible too), but that's incidental, not asserted either
	// way here.
	REQUIRE(StringUtil::Contains(plan, "Bitmap Join"));

	auto bhj = RunAgg3(con, query);
	REQUIRE(bhj.cnt == baseline.cnt);
	REQUIRE(bhj.sum1 == baseline.sum1);

	// The hidden `crid`/`cref` columns must not leak into a user-visible `SELECT *`, even when
	// every table along the chain is wrapped in an explicit column-pruning subquery (so the
	// hidden columns have to be re-propagated through both a LogicalProjection *and* an
	// intermediate INNER join to reach the outer BHJ join - 条目3 Step F + 条目4 combined).
	const string star_query =
	    "SELECT * FROM (SELECT ck, amt FROM bhj_chain_orders) o "
	    "JOIN ((SELECT nk, name FROM bhj_chain_nation) n "
	    "      JOIN (SELECT nk, ck FROM bhj_chain_customer) c ON n.nk = c.nk) "
	    "ON o.ck = c.ck";
	auto star_result = con.Query(star_query);
	REQUIRE(!star_result->HasError());
	auto &materialized = star_result->Cast<MaterializedQueryResult>();
	REQUIRE(materialized.ColumnCount() == 6); // ck,amt,nk,name,nk,ck - no crid/cref.
	for (idx_t c = 0; c < materialized.ColumnCount(); c++) {
		const auto &name = materialized.names[c];
		REQUIRE(!StringUtil::Contains(name, "crid"));
		REQUIRE(!StringUtil::Contains(name, "cref"));
	}
	REQUIRE(materialized.RowCount() == 12);
}

TEST_CASE("BHJ hidden column injection does not collide across two independent scans of the "
          "same PK table (self-join)",
          "[bitmap_join]") {
	RegistryResetGuard guard;
	auto &reg = BitmapJoinMetaRegistry::GetInstance();

	DuckDB db(nullptr);
	Connection con(db);
	SetupChainSchema(con, reg, /*with_nation=*/false);

	// `bhj_chain_customer` is scanned *twice* (c1, c2) - two entirely separate LogicalGet nodes,
	// each independently needing its own hidden `crid` column injected. Both physical columns
	// are literally named "crid" (same catalog column, same string) - if anything in the
	// resolver ever compared column identity by name instead of by ColumnBinding, this would
	// misfire (wrong rowid values / crashes / wrong aggregate results).
	const string query = "SELECT count(*) AS cnt, sum(c1.ck) AS sum1, sum(c2.ck) AS sum2 "
	                      "FROM bhj_chain_orders o "
	                      "JOIN bhj_chain_customer c1 ON o.ck = c1.ck "
	                      "JOIN bhj_chain_customer c2 ON o.ck = c2.ck";

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=false"));
	auto baseline = RunAgg3(con, query);
	REQUIRE(baseline.cnt == 12);
	REQUIRE(baseline.sum1 == 2400);
	REQUIRE(baseline.sum2 == 2400);

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));
	// NOTE: whether this particular plan shape actually hits BHJ depends on which side of the
	// intermediate `c1 JOIN c2` node ends up holding the table BitmapJoinResolver needs to reach
	// (TraceBindingToGet only ever supports growing a join's *right* side - see 条目4 Step A -
	// since growing the left side would shift the absolute positions of anything an unrelated
	// ancestor already references on the right side of that same join). Not asserted either way
	// here; what this test actually guards is that regardless of whether BHJ engages, nothing
	// crashes or produces wrong results when the *same* catalog table is scanned twice with the
	// *same* physical hidden-column name.
	ExplainText(con, query);

	auto bhj = RunAgg3(con, query);
	REQUIRE(bhj.cnt == baseline.cnt);
	REQUIRE(bhj.sum1 == baseline.sum1);
	REQUIRE(bhj.sum2 == baseline.sum2);
}

TEST_CASE("BHJ hidden column re-injection across two joins sharing the same underlying Get is "
          "idempotent",
          "[bitmap_join]") {
	// A variant of the self-join test above where the *same* table (orders) needs its hidden
	// `cref` column for two separate BHJ-eligible joins in the same plan - EnsureGetColumn's
	// dedup logic must kick in on the second join instead of scanning `cref` twice.
	RegistryResetGuard guard;
	auto &reg = BitmapJoinMetaRegistry::GetInstance();

	DuckDB db(nullptr);
	Connection con(db);
	SetupChainSchema(con, reg, /*with_nation=*/false);

	const string query = "SELECT count(*) AS cnt, sum(c1.ck) AS sum1, sum(c2.ck) AS sum2 "
	                      "FROM bhj_chain_orders o "
	                      "JOIN bhj_chain_customer c1 ON o.ck = c1.ck "
	                      "JOIN bhj_chain_customer c2 ON o.ck = c2.ck";

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));
	// Plan (and execute) the very same query several times over the same Connection: if
	// AddColumnId/projection_ids.push_back were ever non-idempotent across repeated planning of
	// the same catalog objects, this would either crash, duplicate columns, or drift results.
	AggResult last;
	for (int i = 0; i < 5; i++) {
		last = RunAgg3(con, query);
		REQUIRE(last.cnt == 12);
		REQUIRE(last.sum1 == 2400);
		REQUIRE(last.sum2 == 2400);
	}
}

TEST_CASE("BHJ safely falls back to a regular hash join for RIGHT_SEMI (does not crash)",
          "[bitmap_join]") {
	// Regression test for a real crash found via the TPC-H Q20 smoke test (b_idea/6.4遗漏问题
	// 任务1 §7.6 / 任务2 条目9): an `IN` subquery decorrelates into a RIGHT_SEMI
	// LogicalComparisonJoin. BitmapJoinResolver used to allow RIGHT_SEMI into bhj_eligible, but
	// BitmapJoinExecutor's constructor only ever supported JoinType::INNER - so if the RIGHT_SEMI
	// join's condition also happened to match a registered PK/FK binding (exactly as in Q20,
	// where `partsupp` is both semi-joined via IN *and* has a materialized rowid binding), the
	// mismatch threw NotImplementedException instead of safely falling back, crashing the whole
	// query. Fixed by tightening bhj_eligible (in both BitmapJoinResolver::ResolveJoin and
	// plan_comparison_join.cpp) to JoinType::INNER only. This test pins that fix down: even when
	// the PK table (`bhj_chain_customer`, ck is registered as a PK) is on the semi-joined side, a
	// query must never throw - not even NotImplementedException.
	RegistryResetGuard guard;
	auto &reg = BitmapJoinMetaRegistry::GetInstance();

	DuckDB db(nullptr);
	Connection con(db);
	SetupChainSchema(con, reg, /*with_nation=*/false);

	// `o.ck IN (SELECT ck FROM bhj_chain_customer)` decorrelates into a semi join whose
	// non-probe side is `bhj_chain_customer` - the same PK table registered via
	// SetupChainSchema, so BitmapJoinResolver will actually attempt to resolve it (not skip it
	// for lack of a registered binding), exercising the exact code path that used to crash.
	const string query = "SELECT count(*) AS cnt FROM bhj_chain_orders o "
	                      "WHERE o.ck IN (SELECT ck FROM bhj_chain_customer)";

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=false"));
	auto baseline_result = con.Query(query);
	REQUIRE_NO_FAIL(*baseline_result);
	auto baseline_cnt = baseline_result->Cast<MaterializedQueryResult>().GetValue(0, 0).GetValue<int64_t>();

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));
	// The key assertion: must not throw (NotImplementedException or otherwise), regardless of
	// whether the underlying join type ends up being SEMI, RIGHT_SEMI, or something else.
	auto bhj_result = con.Query(query);
	REQUIRE_NO_FAIL(*bhj_result);
	REQUIRE(bhj_result->Cast<MaterializedQueryResult>().GetValue(0, 0).GetValue<int64_t>() == baseline_cnt);
}
