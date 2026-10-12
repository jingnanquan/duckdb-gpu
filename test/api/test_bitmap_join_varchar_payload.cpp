// Regression test for the dense-path VARCHAR payload scatter in BitmapHashJoinExecutor
// (src/execution/operator/join/bitmap_hash_join_executor.cpp).
//
// The fix makes VARCHAR payloads in the DENSE build path use a *per-build-thread* local
// StringHeap (written lock-free into the global rowid-indexed payload vector), and keeps
// those local heaps alive in the executor until the join finishes. This must produce output
// byte-for-byte identical to the regular (non-BHJ) hash join path: the same rows, the same
// columns, and VARCHAR payloads that are NOT corrupted (no dangling reference into the
// transient build chunk, no cross-row mix-up from the now lock-free scatter).
//
// Self-contained (plain CREATE TABLE + generated data, no external dataset). Runs by default.

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

//! Full row-for-row, column-for-column equality (including VARCHAR string content).
void RequireSameResults(MaterializedQueryResult &a, MaterializedQueryResult &b) {
	REQUIRE(a.ColumnCount() == b.ColumnCount());
	REQUIRE(a.RowCount() == b.RowCount());
	for (idx_t r = 0; r < a.RowCount(); r++) {
		for (idx_t c = 0; c < a.ColumnCount(); c++) {
			CAPTURE(r);
			CAPTURE(c);
			REQUIRE(a.GetValue(c, r) == b.GetValue(c, r));
		}
	}
}

//! PK (build) side has several VARCHAR payload columns with deliberately varied content:
//!   a - short ascii
//!   b - medium, with repeating runs + non-ascii (中文) + the row number
//!   c - fixed-padded code, varying length of the underlying number to exercise heap growth
void SetupVarcharSchema(Connection &con, BitmapJoinMetaRegistry &reg) {
	REQUIRE_NO_FAIL(con.Query(
	    "CREATE TABLE bhj_varchar_dim(pk INTEGER, rid INTEGER, a VARCHAR, b VARCHAR, c VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query(
	    "INSERT INTO bhj_varchar_dim "
	    "SELECT i, i, "
	    "       'name_' || i::VARCHAR, "
	    "       repeat('x', 10 + (i % 50)) || '_中文_' || i::VARCHAR, "
	    "       'code_' || lpad(i::VARCHAR, 6, '0') "
	    "FROM range(5000) AS t(i)"));

	//! FK (probe) side: 20000 rows, each dim pk referenced ~4 times (so the build side clearly
	//! outweighs / is the dimension, and the build spans multiple input chunks -> multi-thread).
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE bhj_varchar_fact(fk INTEGER, fref INTEGER, val INTEGER)"));
	//! fref must already hold the matched dim row's rowid (== dim.pk == dim.rid == fk here, since
	//! rid==pk==0..4999). BHJ indexes the build payload by fact.fref, so it MUST be the correct
	//! rowid - mirroring how the reference tests populate the `*_ref` column. (A regular hash join
	//! ignores fref and joins on fk directly, which is why the baseline is always correct regardless.)
	REQUIRE_NO_FAIL(con.Query(
	    "INSERT INTO bhj_varchar_fact "
	    "SELECT (i % 5000) AS fk, (i % 5000) AS fref, i AS val "
	    "FROM range(20000) AS t(i)"));

	BitmapJoinPKBinding pk;
	pk.pk_table = "bhj_varchar_dim";
	pk.pk_column = "pk";
	pk.rowid_column = "rid"; // dense: rid == pk == 0..4999, so bitmap_size < 4M -> DENSE payload path
	pk.rowid_offset = 0;
	pk.row_count = 5000;
	reg.RegisterPK(pk);

	BitmapJoinFKBinding fk;
	fk.fk_table = "bhj_varchar_fact";
	fk.fk_column = "fk";
	fk.ref_column = "fref"; // rowid of matched dim row is injected here
	fk.pk_table = "bhj_varchar_dim";
	fk.pk_column = "pk";
	reg.RegisterFK(fk);
}

} // namespace

TEST_CASE("BHJ dense VARCHAR payload matches non-BHJ output (multiple VARCHAR columns, real rows)",
          "[bitmap_join]") {
	RegistryResetGuard guard;
	auto &reg = BitmapJoinMetaRegistry::GetInstance();

	DuckDB db(nullptr);
	Connection con(db);
	//! Force parallelism so the dense build goes through multiple build threads (each with its
	//! own local StringHeap) - this is exactly the lock-free scatter path under test.
	REQUIRE_NO_FAIL(*con.Query("SET threads=4"));
	SetupVarcharSchema(con, reg);

	//! Select multiple VARCHAR columns from the build (PK) side, plus a fact-side integer.
	//! Ordered by the unique fact key so both runs produce an identical canonical row order.
	const string query = "SELECT d.a, d.b, d.c, f.val "
	                      "FROM bhj_varchar_fact f "
	                      "JOIN bhj_varchar_dim d ON f.fk = d.pk "
	                      "ORDER BY f.val";

	//! --- Baseline: regular hash join (no BHJ) ---
	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=false"));
	REQUIRE(!StringUtil::Contains(ExplainText(con, query), "Bitmap Join"));
	auto baseline_res = con.Query(query);
	REQUIRE_NO_FAIL(*baseline_res);
	auto &baseline = baseline_res->Cast<MaterializedQueryResult>();
	REQUIRE(baseline.RowCount() == 20000);

	//! --- BHJ enabled ---
	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));
	REQUIRE(StringUtil::Contains(ExplainText(con, query), "Bitmap Join"));
	auto bhj_res = con.Query(query);
	REQUIRE_NO_FAIL(*bhj_res);
	auto &bhj = bhj_res->Cast<MaterializedQueryResult>();

	//! Row count, column count, and every value (incl. VARCHAR content) must match exactly.
	RequireSameResults(bhj, baseline);

	//! Spot-check specific VARCHAR payloads against ground truth, sampled at the edges and a
	//! middle row to catch first/last-rowid and alignment issues in the lock-free scatter.
	for (const int pk : {0, 1, 42, 4998, 4999}) {
		const string spot_q = "SELECT d.a, d.b, d.c "
		                       "FROM bhj_varchar_fact f JOIN bhj_varchar_dim d ON f.fk = d.pk "
		                       "WHERE f.val = " +
		                       to_string(pk);
		auto spot_res = con.Query(spot_q);
		REQUIRE_NO_FAIL(*spot_res);
		auto &spot = spot_res->Cast<MaterializedQueryResult>();
		REQUIRE(spot.RowCount() == 1);
		CAPTURE(pk);
		REQUIRE(spot.GetValue(0, 0).GetValue<string>() == "name_" + to_string(pk));
		REQUIRE(spot.GetValue(1, 0).GetValue<string>() == string(10 + pk % 50, 'x') + "_中文_" + to_string(pk));
		REQUIRE(spot.GetValue(2, 0).GetValue<string>() == "code_" + string(6 - to_string(pk).size(), '0') + to_string(pk));
	}
}

TEST_CASE("BHJ dense VARCHAR payload with a second independent dim join matches non-BHJ", "[bitmap_join]") {
	//! Extra coverage: a second build (PK) side with its own VARCHAR payloads, joined in the same
	//! query, to confirm the per-thread scatter keeps each build's payloads distinct and uncorrupted.
	RegistryResetGuard guard;
	auto &reg = BitmapJoinMetaRegistry::GetInstance();

	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(*con.Query("SET threads=4"));
	SetupVarcharSchema(con, reg);
	//! Second dimension on the probe side: g2 (pk2) referenced by fact via fk2.
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE bhj_varchar_dim2(pk2 INTEGER, rid2 INTEGER, a2 VARCHAR, b2 VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query(
	    "INSERT INTO bhj_varchar_dim2 "
	    "SELECT i, i, 'dim2_name_' || i::VARCHAR, 'dim2_' || lpad(i::VARCHAR, 4, '0') "
	    "FROM range(3000) AS t(i)"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE bhj_varchar_fact ADD COLUMN fk2 INTEGER"));
	REQUIRE_NO_FAIL(con.Query("ALTER TABLE bhj_varchar_fact ADD COLUMN fref2 INTEGER"));
	REQUIRE_NO_FAIL(con.Query("UPDATE bhj_varchar_fact SET fk2 = val % 3000, fref2 = val % 3000"));
	BitmapJoinPKBinding pk2;
	pk2.pk_table = "bhj_varchar_dim2";
	pk2.pk_column = "pk2";
	pk2.rowid_column = "rid2";
	pk2.rowid_offset = 0;
	pk2.row_count = 3000;
	reg.RegisterPK(pk2);
	BitmapJoinFKBinding fk2;
	fk2.fk_table = "bhj_varchar_fact";
	fk2.fk_column = "fk2";
	fk2.ref_column = "fref2";
	fk2.pk_table = "bhj_varchar_dim2";
	fk2.pk_column = "pk2";
	reg.RegisterFK(fk2);

	const string query = "SELECT d.a, d.b, d.c, g.a2, g.b2, f.val "
	                      "FROM bhj_varchar_fact f "
	                      "JOIN bhj_varchar_dim d ON f.fk = d.pk "
	                      "JOIN bhj_varchar_dim2 g ON f.fk2 = g.pk2 "
	                      "ORDER BY f.val";

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=false"));
	REQUIRE(!StringUtil::Contains(ExplainText(con, query), "Bitmap Join"));
	auto baseline_res = con.Query(query);
	REQUIRE_NO_FAIL(*baseline_res);
	auto &baseline = baseline_res->Cast<MaterializedQueryResult>();

	REQUIRE_NO_FAIL(*con.Query("SET open_bitmap_join=true"));
	REQUIRE(StringUtil::Contains(ExplainText(con, query), "Bitmap Join"));
	auto bhj_res = con.Query(query);
	REQUIRE_NO_FAIL(*bhj_res);
	auto &bhj = bhj_res->Cast<MaterializedQueryResult>();

	RequireSameResults(bhj, baseline);
}
