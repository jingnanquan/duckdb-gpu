// Operator-level profiling for the Bitmap-Join (BHJ) execution path (design doc
// b_idea/6.4遗漏问题/任务1-BHJ命中率归因与算子耗时画像.md, 条目7).
//
// Unlike test_bitmap_join_tpch.cpp (which only measures *query-level* wall-clock time and a
// "how many joins hit BHJ" count), this test drills down to *each individual HASH_JOIN node's*
// own operator_timing (via `PRAGMA enable_profiling='json'`) across all three modes
// (baseline / perfect / bitmap).
//
// Queries are fetched from TpchExtension (the `tpch` extension's dbgen-embedded query texts).
// By default it runs Q5, Q9, Q10; override with the BHJ_QUERIES environment variable
// (comma-separated list of query numbers, e.g. `BHJ_QUERIES=1,2,3`).
//
// Results are written to b_idea/perf/sf5/tpch_operator_timing.csv (one row per HASH_JOIN node
// per mode) for the attribution table in the design doc. Hidden behind `[.]` like the other
// SF5-dataset-dependent tests; run explicitly with `./unittest "[bitmap_join_tpch_profile]"`.

#include "catch.hpp"
#include "test_helpers.hpp"
#include "tpch_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/bitmap_join_meta.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/query_result.hpp"
#include "yyjson.hpp"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <vector>

using namespace duckdb;
using namespace duckdb_yyjson;
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

string SafeStr(yyjson_val *v) {
	if (!v || !yyjson_is_str(v)) {
		return "";
	}
	auto p = yyjson_get_str(v);
	return p ? string(p) : "";
}

//! The SF5 bitmap dataset is scanned via read_parquet(...) (no catalog table, so no "Table"
//! field in extra_info) - infer which of the 8 TPCH tables a scan belongs to from a projected
//! column's unique-enough prefix. Diagnostic-only; never affects any BHJ decision.
string GuessTableFromColumn(const string &col) {
	static const std::vector<pair<string, string>> prefixes = {
	    {"l_", "lineitem"}, {"ps_", "partsupp"}, {"p_", "part"}, {"o_", "orders"},
	    {"c_", "customer"}, {"s_", "supplier"},  {"n_", "nation"}, {"r_", "region"},
	};
	for (auto &pr : prefixes) {
		if (col.size() > pr.first.size() && col.compare(0, pr.first.size(), pr.first) == 0) {
			return pr.second;
		}
	}
	return "";
}

void CollectTableNames(yyjson_val *node, std::vector<string> &out) {
	if (!node) {
		return;
	}
	string name = SafeStr(yyjson_obj_get(node, "operator_name"));
	if (name.empty()) {
		name = SafeStr(yyjson_obj_get(node, "name"));
	}
	if (name.find("SCAN") != string::npos || name.find("PARQUET") != string::npos) {
		auto extra = yyjson_obj_get(node, "extra_info");
		if (extra) {
			auto table_val = yyjson_obj_get(extra, "Table");
			if (table_val) {
				out.push_back(SafeStr(table_val));
			} else {
				auto proj = yyjson_obj_get(extra, "Projections");
				string col;
				if (proj) {
					if (yyjson_is_str(proj)) {
						col = SafeStr(proj);
					} else if (yyjson_is_arr(proj) && yyjson_arr_size(proj) > 0) {
						col = SafeStr(yyjson_arr_get(proj, 0));
					}
				}
				auto guess = GuessTableFromColumn(col);
				out.push_back(guess.empty() ? ("?" + col) : guess);
			}
		}
	}
	auto children = yyjson_obj_get(node, "children");
	if (children) {
		size_t idx, max;
		yyjson_val *child;
		yyjson_arr_foreach(children, idx, max, child) {
			CollectTableNames(child, out);
		}
	}
}

string JoinNames(const std::vector<string> &v) {
	string s;
	for (size_t i = 0; i < v.size(); i++) {
		if (i) {
			s += "+";
		}
		auto pos = v[i].rfind('.');
		s += pos == string::npos ? v[i] : v[i].substr(pos + 1);
	}
	return s;
}

struct JoinTiming {
	string label; //! "<condition> [<probe-side tables> <-> <build-side tables>]"
	double timing_s = 0.0;
	string bitmap_join = "(none)";    //! ParamsToString's "Bitmap Join" extra_info value, if any.
	string bitmap_payload = "(none)"; //! BitmapJoinExecutor payload mode: dense/compact, if any.
};

void WalkHashJoins(yyjson_val *node, std::vector<JoinTiming> &out) {
	if (!node) {
		return;
	}
	string name = SafeStr(yyjson_obj_get(node, "operator_name"));
	if (name.empty()) {
		name = SafeStr(yyjson_obj_get(node, "name"));
	}
	if (name == "HASH_JOIN") {
		JoinTiming jt;
		auto timing_val = yyjson_obj_get(node, "operator_timing");
		if (timing_val && yyjson_is_real(timing_val)) {
			jt.timing_s = yyjson_get_real(timing_val);
		} else if (timing_val && yyjson_is_int(timing_val)) {
			jt.timing_s = static_cast<double>(yyjson_get_int(timing_val));
		}
		auto extra = yyjson_obj_get(node, "extra_info");
		string cond;
		if (extra) {
			cond = SafeStr(yyjson_obj_get(extra, "Conditions"));
			auto bj = yyjson_obj_get(extra, "Bitmap Join");
			if (bj) {
				auto s = SafeStr(bj);
				if (!s.empty()) {
					jt.bitmap_join = s;
				}
			}
			auto bp = yyjson_obj_get(extra, "Bitmap Payload");
			if (bp) {
				auto s = SafeStr(bp);
				if (!s.empty()) {
					jt.bitmap_payload = s;
				}
			}
		}
		std::vector<string> lt, rt;
		auto children = yyjson_obj_get(node, "children");
		if (children && yyjson_arr_size(children) == 2) {
			CollectTableNames(yyjson_arr_get(children, 0), lt);
			CollectTableNames(yyjson_arr_get(children, 1), rt);
		}
		jt.label = cond + " [" + JoinNames(lt) + " <-> " + JoinNames(rt) + "]";
		out.push_back(jt);
	}
	auto children = yyjson_obj_get(node, "children");
	if (children) {
		size_t idx, max;
		yyjson_val *child;
		yyjson_arr_foreach(children, idx, max, child) {
			WalkHashJoins(child, out);
		}
	}
}

//! Runs `sql` once (untimed, to warm the OS/parquet metadata cache), then once more with JSON
//! profiling enabled, and returns each HASH_JOIN node's own (non-cumulative) operator_timing -
//! in the same top-down traversal order used elsewhere in this file, which - for a fixed SQL
//! text/statistics/join-order-optimizer input - is expected to be identical across the three
//! modes (only whether each PhysicalHashJoin is *marked* bitmap/perfect differs, not the plan
//! shape), giving a stable, positional way to line HASH_JOIN[i] up across baseline/perfect/bitmap.
std::vector<JoinTiming> RunWithProfiling(Connection &con, const string &sql, const string &tmp_path, bool open_perfect,
                                    bool open_bitmap) {
	REQUIRE_NO_FAIL(*con.Query(string("SET open_perfect_join=") + (open_perfect ? "true" : "false")));
	REQUIRE_NO_FAIL(*con.Query(string("SET open_bitmap_join=") + (open_bitmap ? "true" : "false")));
	REQUIRE_NO_FAIL(*con.Query("PRAGMA disable_profiling"));
	REQUIRE_NO_FAIL(*con.Query(sql)); // warm-up, untimed
	REQUIRE_NO_FAIL(*con.Query("PRAGMA enable_profiling='json'"));
	REQUIRE_NO_FAIL(*con.Query("PRAGMA profiling_output='" + tmp_path + "'"));
	auto result = con.Query(sql);
	REQUIRE_NO_FAIL(*result);
	REQUIRE_NO_FAIL(*con.Query("PRAGMA disable_profiling"));

	std::ifstream f(tmp_path);
	std::stringstream ss;
	ss << f.rdbuf();
	string json_text = ss.str();
	yyjson_doc *doc = yyjson_read(json_text.c_str(), json_text.size(), 0);
	REQUIRE(doc != nullptr);
	std::vector<JoinTiming> out;
	WalkHashJoins(yyjson_doc_get_root(doc), out);
	yyjson_doc_free(doc);
	return out;
}

void RunAndReport(Connection &con, const string &name, const string &sql, std::ofstream &csv, const string &tmp_dir) {
	auto baseline = RunWithProfiling(con, sql, tmp_dir + "/" + name + "_baseline.json", false, false);
	auto perfect = RunWithProfiling(con, sql, tmp_dir + "/" + name + "_perfect.json", true, false);
	auto bitmap = RunWithProfiling(con, sql, tmp_dir + "/" + name + "_bitmap.json", false, true);

	printf("\n=== %s ===\n", name.c_str());
	printf("%-70s %12s %12s %12s %10s %10s\n", "Join (condition [probe <-> build])", "baseline(ms)",
	       "perfect(ms)", "bitmap(ms)", "BHJ?", "payload");
	size_t n = std::max({baseline.size(), perfect.size(), bitmap.size()});
	for (size_t i = 0; i < n; i++) {
		string label = i < bitmap.size() ? bitmap[i].label : (i < baseline.size() ? baseline[i].label : "?");
		double b = i < baseline.size() ? baseline[i].timing_s * 1000.0 : -1;
		double p = i < perfect.size() ? perfect[i].timing_s * 1000.0 : -1;
		double m = i < bitmap.size() ? bitmap[i].timing_s * 1000.0 : -1;
		string bj = i < bitmap.size() ? bitmap[i].bitmap_join : "?";
		string bp = i < bitmap.size() ? bitmap[i].bitmap_payload : "?";
		printf("%-70s %12.3f %12.3f %12.3f %10s %10s\n", label.c_str(), b, p, m, bj.c_str(),
		       bp.c_str());
		csv << name << ",\"" << label << "\"," << b << "," << p << "," << m << ",\"" << bj << "\",\""
		    << bp << "\"\n";
	}
}

} // namespace

TEST_CASE("BHJ operator-level timing across baseline/perfect/bitmap for TPC-H queries (SF=5)",
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

	DuckDB db(nullptr);
	Connection con(db);
	CreateViews(con, kBitmapDataDir);

	auto &registry = BitmapJoinMetaRegistry::GetInstance();
	registry.Clear();
	registry.SetForceBitmapJoin(false);
	registry.SetForceResolvedPK("", "");
	registry.LoadFromJson(meta_path);

	auto fs = FileSystem::CreateLocal();
	const string csv_dir = "/home/featurize/workspace/duckdb-cuda/b_idea/perf/sf5";
	fs->CreateDirectoriesRecursive(csv_dir);
	std::ofstream csv(csv_dir + "/tpch_operator_timing.csv", std::ios::out | std::ios::trunc);
	csv << "query,join_label,baseline_ms,perfect_ms,bitmap_ms,bitmap_join_status,bitmap_payload_mode\n";

	auto query_ids = GetQueryIds();
	printf("Running TPC-H queries: ");
	for (size_t i = 0; i < query_ids.size(); i++) {
		if (i) {
			printf(", ");
		}
		printf("Q%d", query_ids[i]);
	}
	printf("\n");

	for (int q : query_ids) {
		string name = "Q" + std::to_string(q);
		string sql = TpchExtension::GetQuery(q);
		RunAndReport(con, name, sql, csv, "/tmp");
	}

	printf("\nCSV written to b_idea/perf/sf5/tpch_operator_timing.csv\n");
	registry.Clear();
}
