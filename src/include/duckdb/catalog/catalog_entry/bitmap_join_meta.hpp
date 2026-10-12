//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/catalog/catalog_entry/bitmap_join_meta.hpp
//
// Bitmap-Join (BHJ) catalog metadata registry (design doc §6.2, module M-C).
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class ClientContext;

//! Describes a primary-key (dimension) table whose rows are addressable by a dense rowid.
struct BitmapJoinPKBinding {
	string pk_table;
	string pk_column;    //! original PK column
	string rowid_column; //! column that actually carries the rowid role (may == pk_column)
	int64_t rowid_offset = 0; //! value such that rowid = pk_value - rowid_offset (when reusing the PK column)
	idx_t row_count = 0;      //! number of rows in the PK table (determines bitmap size)
};

//! Describes a foreign-key (fact) column that has a materialized *_ref column pointing at PK rowids.
struct BitmapJoinFKBinding {
	string fk_table;
	string fk_column;  //! original FK column
	string ref_column; //! materialized *_ref column (0-based rowid into the PK table)
	string pk_table;
	string pk_column;
};

//! Result of resolving a single equi-join condition against the registry.
struct BitmapJoinResolved {
	const BitmapJoinPKBinding *pk = nullptr; //! build (dimension) side
	const BitmapJoinFKBinding *fk = nullptr; //! probe (fact) side
	//! true  -> the PK table is on the build side (RHS), FK table is on the probe side (LHS)
	//! false -> reversed
	bool build_is_pk_side = true;
};

//! Diagnostic-only (b_idea/6.4遗漏问题, 条目6): why BitmapJoinResolver did or did not resolve a
//! given LogicalComparisonJoin into a BHJ hint. Never read by any execution-path decision - it
//! exists purely so that EXPLAIN can show a human-readable reason for every HASH_JOIN node,
//! without needing one-off debug prints each time someone wants to know why a join didn't hit
//! BHJ. Stored on both LogicalComparisonJoin::bhj_skip_reason and PhysicalHashJoin::bhj_skip_reason
//! (the latter is just a copy taken at PlanComparisonJoin time).
enum class BitmapJoinSkipReason : uint8_t {
	//! BitmapJoinResolver never visited this node (pass didn't run, i.e. open_bitmap_join=false).
	NOT_PROCESSED = 0,
	//! Successfully resolved: bhj_hint was set.
	HIT,
	//! Join condition is not a single equality condition.
	NOT_SINGLE_EQUALITY,
	//! join_type is not INNER (BitmapJoinExecutor only ever supports INNER; RIGHT_SEMI was
	//! tightened out of eligibility after it was found to crash - see b_idea/6.4遗漏问题 任务1
	//! 条目7/条目9, real TPCH Q20 - rather than throw NotImplementedException at construction
	//! time, the *planner* now simply never proposes it in the first place).
	NOT_INNER_OR_RIGHT_SEMI,
	//! One side of the equality condition is not a plain column reference (e.g. a cast/expression).
	CONDITION_NOT_PLAIN_COLUMN,
	//! Same as CONDITION_NOT_PLAIN_COLUMN, but specifically because the CompressedMaterialization
	//! optimizer pass (runs earlier, during statistics propagation) wrapped the join key in an
	//! `__internal_compress_integral_*`/`__internal_decompress_integral_*` scalar function based
	//! on observed value-range statistics - a distinct, *diagnosable* cause from a genuine
	//! computed expression (see b_idea/6.4遗漏问题 任务1 条目6: real SF5 Q9/Q10 joins hit this,
	//! not the generic case).
	CONDITION_WRAPPED_BY_COMPRESSED_MATERIALIZATION,
	//! Could not trace one/both sides of the condition back to an underlying LogicalGet (e.g.
	//! buried under an aliased subquery, an Aggregate, or some other unsupported operator).
	TRACE_TO_GET_FAILED,
	//! Traced back to a LogicalGet, but couldn't resolve its catalog table/column name (e.g. a
	//! table function other than read_parquet, or an out-of-range column index).
	CATALOG_NAME_RESOLUTION_FAILED,
	//! The (table,column) pair on both sides isn't a registered PK/FK binding.
	CATALOG_NOT_REGISTERED,
	//! The FK (fact) table landed on the build side - BHJ's build-side-unique precondition would
	//! be violated, so BHJ is never attempted (by design, see design doc 条目2 - not a bug).
	FK_ON_BUILD_SIDE,
	//! PK column is non-dense (needs a hidden `_rowid` column) but the matching FK binding has no
	//! `ref_column` to compute the probe side consistently.
	ROWID_MODE_MISMATCH,
	//! Non-dense rowid propagation needed but the trace path crossed the LEFT side of an
	//! intermediate INNER join (known, documented limitation - see b_idea/6.4遗漏问题 条目8).
	PATH_INCOMPLETE_LEFT_SIDE,
	//! Non-dense rowid propagation needed but the trace path crossed some other operator type
	//! PropagateHiddenColumn doesn't support (e.g. Aggregate, non-INNER join, ASOF/DELIM join).
	PATH_INCOMPLETE_OTHER,
	//! BHJ was eligible and auto-resolved, but the estimated build density (build child
	//! cardinality / PK dimension row count) is below the low-density threshold, so BHJ is
	//! skipped in favour of a regular/perfect hash join (perf optimization, mode-1 early phase,
	//! perf/sf5/combine锁, 见 根因分析-详细版.md §4.2).
	LOW_DENSITY,
};

//! 初期方案 4.2 (perf/sf5/combine锁, 根因分析-详细版.md §4.2): 计划期低密度阈值。
//! 当估计 build 密度 (= build 子节点基数 / PK 维度表行数) 低于此值时, BHJ 回退到
//! 普通/perfect 哈希 join。初期用常量实现, 后续应改为可调 setting (见文档 §5.1 的
//! BitmapJoinMaxPayloadBytesSetting / 密度阈值建议)。
constexpr double BHJ_LOW_DENSITY_THRESHOLD = 0.3;

//! Human-readable (snake_case) name for a BitmapJoinSkipReason, used in EXPLAIN output.
DUCKDB_API string BitmapJoinSkipReasonToString(BitmapJoinSkipReason reason);

//! Process-wide registry of bitmap-join bindings.
//!
//! The registry is a process-level singleton (design decision: §6.2.3 / Q2-A). It is populated
//! either offline via LoadFromJson (parquet extension load hook) or directly in tests via
//! RegisterPK / RegisterFK. All accessors are thread-safe.
class BitmapJoinMetaRegistry {
public:
	//! Returns the process-wide singleton. The ClientContext argument is accepted to match the
	//! design signature but is currently unused (the registry is not scoped to a database instance).
	DUCKDB_API static BitmapJoinMetaRegistry &Get(ClientContext &context);
	//! Context-free accessor (used by tests and code paths without a ClientContext).
	DUCKDB_API static BitmapJoinMetaRegistry &GetInstance();

	//! Load (and merge) bindings from a bitmap_join_meta.json file produced by add_bitmap_columns.py.
	DUCKDB_API void LoadFromJson(const string &path);
	//! Load (and merge) bindings from an in-memory JSON string.
	DUCKDB_API void LoadFromJsonString(const string &json_text);

	//! Manual registration (used by tests).
	DUCKDB_API void RegisterPK(BitmapJoinPKBinding binding);
	DUCKDB_API void RegisterFK(BitmapJoinFKBinding binding);

	//! Remove all bindings.
	DUCKDB_API void Clear();

	DUCKDB_API optional_ptr<const BitmapJoinPKBinding> FindPK(const string &pk_table, const string &pk_column) const;
	DUCKDB_API optional_ptr<const BitmapJoinFKBinding> FindFK(const string &fk_table, const string &fk_column) const;

	//! Given an equi-join condition (build_table.build_col = probe_table.probe_col), determine whether
	//! it is pre-bound (PK <-> FK). On success, fills `out` and returns true.
	DUCKDB_API bool TryResolve(const string &build_table, const string &build_col, const string &probe_table,
	                           const string &probe_col, BitmapJoinResolved &out) const;

	//! Snapshot accessors for the diagnostic PRAGMA.
	DUCKDB_API vector<BitmapJoinPKBinding> GetPKBindings() const;
	DUCKDB_API vector<BitmapJoinFKBinding> GetFKBindings() const;

	//! Global "force bitmap join" switch (design §6.3 hook enablement). Default false so that the
	//! default execution path is never affected.
	DUCKDB_API void SetForceBitmapJoin(bool enabled);
	DUCKDB_API bool IsForceBitmapJoin() const;

	//===------------------------------------------------------------------===//
	// Test-only resolution override
	//===------------------------------------------------------------------===//
	//! For tests / forced execution: set a single PK binding to use whenever the planner needs
	//! to fill in a `BitmapJoinResolved` but column-binding -> catalog reverse-resolution is not
	//! available yet (module 6.6 is the proper resolver). When the override is set and the global
	//! force switch is true, every BHJ-eligible PhysicalHashJoin is wired to this PK binding.
	//! Pass an empty table/column to clear the override.
	DUCKDB_API void SetForceResolvedPK(const string &pk_table, const string &pk_column);
	//! Returns the currently configured override PK binding, or nullptr if none is set.
	DUCKDB_API const BitmapJoinPKBinding *GetForceResolvedPK() const;

private:
	BitmapJoinMetaRegistry() = default;

	//! Internal, assumes the lock is already held.
	const BitmapJoinPKBinding *FindPKUnsafe(const string &pk_table, const string &pk_column) const;
	const BitmapJoinFKBinding *FindFKUnsafe(const string &fk_table, const string &fk_column) const;

	mutable mutex lock;
	vector<BitmapJoinPKBinding> pk_bindings;
	vector<BitmapJoinFKBinding> fk_bindings;
	bool force_bitmap_join = false;
	//! Optional override: when non-empty, points at the PK binding to use for forced BHJ execution.
	string force_pk_table;
	string force_pk_column;
};

} // namespace duckdb
