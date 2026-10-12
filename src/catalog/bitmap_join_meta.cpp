#include "duckdb/catalog/catalog_entry/bitmap_join_meta.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

#include "yyjson.hpp"

#include <fstream>
#include <sstream>

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

//===--------------------------------------------------------------------===//
// Diagnostics (b_idea/6.4遗漏问题, 条目6)
//===--------------------------------------------------------------------===//
string BitmapJoinSkipReasonToString(BitmapJoinSkipReason reason) {
	switch (reason) {
	case BitmapJoinSkipReason::NOT_PROCESSED:
		return "not_processed";
	case BitmapJoinSkipReason::HIT:
		return "hit";
	case BitmapJoinSkipReason::NOT_SINGLE_EQUALITY:
		return "not_single_equality";
	case BitmapJoinSkipReason::NOT_INNER_OR_RIGHT_SEMI:
		return "not_inner_or_right_semi";
	case BitmapJoinSkipReason::CONDITION_NOT_PLAIN_COLUMN:
		return "condition_not_plain_column";
	case BitmapJoinSkipReason::CONDITION_WRAPPED_BY_COMPRESSED_MATERIALIZATION:
		return "condition_wrapped_by_compressed_materialization";
	case BitmapJoinSkipReason::TRACE_TO_GET_FAILED:
		return "trace_to_get_failed";
	case BitmapJoinSkipReason::CATALOG_NAME_RESOLUTION_FAILED:
		return "catalog_name_resolution_failed";
	case BitmapJoinSkipReason::CATALOG_NOT_REGISTERED:
		return "catalog_not_registered";
	case BitmapJoinSkipReason::FK_ON_BUILD_SIDE:
		return "fk_on_build_side";
	case BitmapJoinSkipReason::ROWID_MODE_MISMATCH:
		return "rowid_mode_mismatch";
	case BitmapJoinSkipReason::PATH_INCOMPLETE_LEFT_SIDE:
		return "path_incomplete_left_side";
	case BitmapJoinSkipReason::PATH_INCOMPLETE_OTHER:
		return "path_incomplete_other";
	case BitmapJoinSkipReason::LOW_DENSITY:
		return "low_density";
	default:
		return "unknown";
	}
}

//===--------------------------------------------------------------------===//
// Singleton
//===--------------------------------------------------------------------===//
BitmapJoinMetaRegistry &BitmapJoinMetaRegistry::GetInstance() {
	// Process-level singleton (design §6.2.3, decision Q2-A). Constructed on first use and never
	// destroyed before process exit, so pointers handed out by TryResolve remain valid.
	static BitmapJoinMetaRegistry instance;
	return instance;
}

BitmapJoinMetaRegistry &BitmapJoinMetaRegistry::Get(ClientContext &context) {
	return GetInstance();
}

//===--------------------------------------------------------------------===//
// Registration
//===--------------------------------------------------------------------===//
void BitmapJoinMetaRegistry::RegisterPK(BitmapJoinPKBinding binding) {
	lock_guard<mutex> guard(lock);
	for (auto &existing : pk_bindings) {
		if (existing.pk_table == binding.pk_table && existing.pk_column == binding.pk_column) {
			existing = std::move(binding);
			return;
		}
	}
	pk_bindings.push_back(std::move(binding));
}

void BitmapJoinMetaRegistry::RegisterFK(BitmapJoinFKBinding binding) {
	lock_guard<mutex> guard(lock);
	for (auto &existing : fk_bindings) {
		if (existing.fk_table == binding.fk_table && existing.fk_column == binding.fk_column) {
			existing = std::move(binding);
			return;
		}
	}
	fk_bindings.push_back(std::move(binding));
}

void BitmapJoinMetaRegistry::Clear() {
	lock_guard<mutex> guard(lock);
	pk_bindings.clear();
	fk_bindings.clear();
}

//===--------------------------------------------------------------------===//
// JSON loading
//===--------------------------------------------------------------------===//
static string JsonGetStr(yyjson_val *obj, const char *key, const string &def = string()) {
	auto val = yyjson_obj_get(obj, key);
	if (val && yyjson_is_str(val)) {
		return string(yyjson_get_str(val), yyjson_get_len(val));
	}
	return def;
}

static int64_t JsonGetInt(yyjson_val *obj, const char *key, int64_t def = 0) {
	auto val = yyjson_obj_get(obj, key);
	if (!val) {
		return def;
	}
	if (yyjson_is_uint(val)) {
		return static_cast<int64_t>(yyjson_get_uint(val));
	}
	if (yyjson_is_sint(val) || yyjson_is_int(val)) {
		return yyjson_get_sint(val);
	}
	return def;
}

void BitmapJoinMetaRegistry::LoadFromJson(const string &path) {
	std::ifstream stream(path, std::ios::binary);
	if (!stream.good()) {
		throw IOException("BitmapJoinMetaRegistry: could not open bitmap_join_meta file \"%s\"", path);
	}
	std::stringstream buffer;
	buffer << stream.rdbuf();
	LoadFromJsonString(buffer.str());
}

void BitmapJoinMetaRegistry::LoadFromJsonString(const string &json_text) {
	auto doc = yyjson_read(json_text.c_str(), json_text.size(), 0);
	if (!doc) {
		throw InvalidInputException("BitmapJoinMetaRegistry: failed to parse bitmap_join_meta JSON");
	}
	// RAII-style cleanup guard for the document.
	struct DocGuard {
		yyjson_doc *doc;
		~DocGuard() {
			yyjson_doc_free(doc);
		}
	} doc_guard {doc};

	auto root = yyjson_doc_get_root(doc);
	if (!root || !yyjson_is_obj(root)) {
		throw InvalidInputException("BitmapJoinMetaRegistry: bitmap_join_meta JSON root must be an object");
	}

	vector<BitmapJoinPKBinding> parsed_pk;
	vector<BitmapJoinFKBinding> parsed_fk;

	auto pk_arr = yyjson_obj_get(root, "pk_bindings");
	if (pk_arr && yyjson_is_arr(pk_arr)) {
		size_t idx, max;
		yyjson_val *item;
		yyjson_arr_foreach(pk_arr, idx, max, item) {
			if (!yyjson_is_obj(item)) {
				continue;
			}
			BitmapJoinPKBinding binding;
			binding.pk_table = JsonGetStr(item, "pk_table");
			binding.pk_column = JsonGetStr(item, "pk_column");
			binding.rowid_column = JsonGetStr(item, "rowid_column", binding.pk_column);
			binding.rowid_offset = JsonGetInt(item, "rowid_offset", 0);
			binding.row_count = static_cast<idx_t>(JsonGetInt(item, "row_count", 0));
			if (binding.pk_table.empty() || binding.pk_column.empty()) {
				throw InvalidInputException("BitmapJoinMetaRegistry: pk_binding missing pk_table/pk_column");
			}
			parsed_pk.push_back(std::move(binding));
		}
	}

	auto fk_arr = yyjson_obj_get(root, "fk_bindings");
	if (fk_arr && yyjson_is_arr(fk_arr)) {
		size_t idx, max;
		yyjson_val *item;
		yyjson_arr_foreach(fk_arr, idx, max, item) {
			if (!yyjson_is_obj(item)) {
				continue;
			}
			BitmapJoinFKBinding binding;
			binding.fk_table = JsonGetStr(item, "fk_table");
			binding.fk_column = JsonGetStr(item, "fk_column");
			binding.ref_column = JsonGetStr(item, "ref_column");
			binding.pk_table = JsonGetStr(item, "pk_table");
			binding.pk_column = JsonGetStr(item, "pk_column");
			if (binding.fk_table.empty() || binding.fk_column.empty()) {
				throw InvalidInputException("BitmapJoinMetaRegistry: fk_binding missing fk_table/fk_column");
			}
			parsed_fk.push_back(std::move(binding));
		}
	}

	// Merge under the lock.
	for (auto &binding : parsed_pk) {
		RegisterPK(std::move(binding));
	}
	for (auto &binding : parsed_fk) {
		RegisterFK(std::move(binding));
	}
}

//===--------------------------------------------------------------------===//
// Lookup
//===--------------------------------------------------------------------===//
const BitmapJoinPKBinding *BitmapJoinMetaRegistry::FindPKUnsafe(const string &pk_table, const string &pk_column) const {
	for (auto &binding : pk_bindings) {
		if (binding.pk_table == pk_table && binding.pk_column == pk_column) {
			return &binding;
		}
	}
	return nullptr;
}

const BitmapJoinFKBinding *BitmapJoinMetaRegistry::FindFKUnsafe(const string &fk_table, const string &fk_column) const {
	for (auto &binding : fk_bindings) {
		if (binding.fk_table == fk_table && binding.fk_column == fk_column) {
			return &binding;
		}
	}
	return nullptr;
}

optional_ptr<const BitmapJoinPKBinding> BitmapJoinMetaRegistry::FindPK(const string &pk_table,
                                                                       const string &pk_column) const {
	lock_guard<mutex> guard(lock);
	return FindPKUnsafe(pk_table, pk_column);
}

optional_ptr<const BitmapJoinFKBinding> BitmapJoinMetaRegistry::FindFK(const string &fk_table,
                                                                       const string &fk_column) const {
	lock_guard<mutex> guard(lock);
	return FindFKUnsafe(fk_table, fk_column);
}

bool BitmapJoinMetaRegistry::TryResolve(const string &build_table, const string &build_col, const string &probe_table,
                                        const string &probe_col, BitmapJoinResolved &out) const {
	lock_guard<mutex> guard(lock);

	// Orientation 1: build side is the PK (dimension) table, probe side is the FK (fact) table.
	auto fk = FindFKUnsafe(probe_table, probe_col);
	if (fk && fk->pk_table == build_table && fk->pk_column == build_col) {
		auto pk = FindPKUnsafe(build_table, build_col);
		if (pk) {
			out.pk = pk;
			out.fk = fk;
			out.build_is_pk_side = true;
			return true;
		}
	}

	// Orientation 2: build side is the FK (fact) table, probe side is the PK (dimension) table.
	fk = FindFKUnsafe(build_table, build_col);
	if (fk && fk->pk_table == probe_table && fk->pk_column == probe_col) {
		auto pk = FindPKUnsafe(probe_table, probe_col);
		if (pk) {
			out.pk = pk;
			out.fk = fk;
			out.build_is_pk_side = false;
			return true;
		}
	}

	return false;
}

vector<BitmapJoinPKBinding> BitmapJoinMetaRegistry::GetPKBindings() const {
	lock_guard<mutex> guard(lock);
	return pk_bindings;
}

vector<BitmapJoinFKBinding> BitmapJoinMetaRegistry::GetFKBindings() const {
	lock_guard<mutex> guard(lock);
	return fk_bindings;
}

//===--------------------------------------------------------------------===//
// Force switch
//===--------------------------------------------------------------------===//
void BitmapJoinMetaRegistry::SetForceBitmapJoin(bool enabled) {
	lock_guard<mutex> guard(lock);
	force_bitmap_join = enabled;
}

bool BitmapJoinMetaRegistry::IsForceBitmapJoin() const {
	lock_guard<mutex> guard(lock);
	return force_bitmap_join;
}

//===--------------------------------------------------------------------===//
// Test-only resolution override
//===--------------------------------------------------------------------===//
void BitmapJoinMetaRegistry::SetForceResolvedPK(const string &pk_table, const string &pk_column) {
	lock_guard<mutex> guard(lock);
	force_pk_table = pk_table;
	force_pk_column = pk_column;
}

const BitmapJoinPKBinding *BitmapJoinMetaRegistry::GetForceResolvedPK() const {
	lock_guard<mutex> guard(lock);
	if (force_pk_table.empty() || force_pk_column.empty()) {
		return nullptr;
	}
	return FindPKUnsafe(force_pk_table, force_pk_column);
}

} // namespace duckdb
