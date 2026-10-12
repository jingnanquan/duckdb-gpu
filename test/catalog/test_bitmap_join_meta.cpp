#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/bitmap_join_meta.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/query_result.hpp"

using namespace duckdb;

namespace {

const char *const kSampleMeta = R"JSON({
  "version": 1,
  "rowid_column_default": "_rowid",
  "ref_column_suffix": "_ref",
  "pk_bindings": [
    {"pk_table": "region",   "pk_column": "r_regionkey", "rowid_column": "r_regionkey", "rowid_offset": 0, "row_count": 5},
    {"pk_table": "customer", "pk_column": "c_custkey",   "rowid_column": "c_custkey",   "rowid_offset": 1, "row_count": 750000}
  ],
  "fk_bindings": [
    {"fk_table": "nation", "fk_column": "n_regionkey", "ref_column": "n_regionkey_ref", "pk_table": "region",   "pk_column": "r_regionkey"},
    {"fk_table": "orders", "fk_column": "o_custkey",   "ref_column": "o_custkey_ref",   "pk_table": "customer", "pk_column": "c_custkey"}
  ]
})JSON";

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

} // namespace

TEST_CASE("BitmapJoinMetaRegistry load/lookup/resolve", "[bitmap_join]") {
	RegistryResetGuard guard;
	auto &reg = BitmapJoinMetaRegistry::GetInstance();

	reg.LoadFromJsonString(kSampleMeta);

	// PK lookups
	auto region_pk = reg.FindPK("region", "r_regionkey");
	REQUIRE(region_pk);
	REQUIRE(region_pk->row_count == 5);
	REQUIRE(region_pk->rowid_offset == 0);
	REQUIRE(region_pk->rowid_column == "r_regionkey");

	auto customer_pk = reg.FindPK("customer", "c_custkey");
	REQUIRE(customer_pk);
	REQUIRE(customer_pk->rowid_offset == 1);
	REQUIRE(customer_pk->row_count == 750000);

	REQUIRE(!reg.FindPK("does_not_exist", "x"));

	// FK lookups
	auto nation_fk = reg.FindFK("nation", "n_regionkey");
	REQUIRE(nation_fk);
	REQUIRE(nation_fk->ref_column == "n_regionkey_ref");
	REQUIRE(nation_fk->pk_table == "region");

	REQUIRE(!reg.FindFK("nation", "not_a_column"));

	// Resolve: build = PK side
	BitmapJoinResolved resolved;
	REQUIRE(reg.TryResolve("region", "r_regionkey", "nation", "n_regionkey", resolved));
	REQUIRE(resolved.build_is_pk_side == true);
	REQUIRE(resolved.pk != nullptr);
	REQUIRE(resolved.fk != nullptr);
	REQUIRE(resolved.pk->row_count == 5);
	REQUIRE(resolved.fk->ref_column == "n_regionkey_ref");

	// Resolve: build = FK side (reversed orientation)
	BitmapJoinResolved resolved2;
	REQUIRE(reg.TryResolve("nation", "n_regionkey", "region", "r_regionkey", resolved2));
	REQUIRE(resolved2.build_is_pk_side == false);
	REQUIRE(resolved2.pk->row_count == 5);

	// Non-binding pairs do not resolve
	BitmapJoinResolved miss;
	REQUIRE(!reg.TryResolve("region", "r_regionkey", "orders", "o_custkey", miss));
	REQUIRE(!reg.TryResolve("foo", "bar", "baz", "qux", miss));

	// Manual registration overrides existing PK with same key
	BitmapJoinPKBinding updated;
	updated.pk_table = "region";
	updated.pk_column = "r_regionkey";
	updated.rowid_column = "r_regionkey";
	updated.row_count = 42;
	reg.RegisterPK(updated);
	auto region_pk2 = reg.FindPK("region", "r_regionkey");
	REQUIRE(region_pk2);
	REQUIRE(region_pk2->row_count == 42);
}

TEST_CASE("PhysicalHashJoin bitmap-join hook (scaffolding)", "[bitmap_join]") {
	RegistryResetGuard guard;
	auto &reg = BitmapJoinMetaRegistry::GetInstance();

	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE bhj_dim(k INTEGER, name VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE bhj_fact(k INTEGER, v INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO bhj_dim VALUES (0,'a'),(1,'b')"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO bhj_fact VALUES (0,10),(1,20),(0,30)"));

	const string join_query = "SELECT count(*) FROM bhj_fact JOIN bhj_dim ON bhj_fact.k = bhj_dim.k";

	// Default path: switch off -> normal results, no EXPLAIN marker.
	{
		auto result = con.Query(join_query);
		REQUIRE(!result->HasError());
		REQUIRE(CHECK_COLUMN(result, 0, {3}));
		auto plan = ExplainText(con, join_query);
		REQUIRE(!StringUtil::Contains(plan, "Bitmap Join"));
	}

	// Switch on the force override: this requires the PK binding to actually be registered
	// (GetForceResolvedPK() looks it up by table/column), so register it first. The plan-time
	// hook flags the join (EXPLAIN marker) and BHJ now executes for real - the executor is no
	// longer a stub - producing the same results as the regular hash-join path.
	BitmapJoinPKBinding dim_pk;
	dim_pk.pk_table = "bhj_dim";
	dim_pk.pk_column = "k";
	dim_pk.rowid_column = "k";
	dim_pk.rowid_offset = 0;
	dim_pk.row_count = 2;
	reg.RegisterPK(dim_pk);
	reg.SetForceResolvedPK("bhj_dim", "k");
	reg.SetForceBitmapJoin(true);
	{
		auto plan = ExplainText(con, join_query);
		REQUIRE(StringUtil::Contains(plan, "Bitmap Join"));

		auto result = con.Query(join_query);
		REQUIRE(!result->HasError());
		REQUIRE(CHECK_COLUMN(result, 0, {3}));
	}

	// Switch off again: behaviour returns to normal.
	reg.SetForceBitmapJoin(false);
	reg.SetForceResolvedPK("", "");
	{
		auto result = con.Query(join_query);
		REQUIRE(!result->HasError());
		REQUIRE(CHECK_COLUMN(result, 0, {3}));
	}
}
