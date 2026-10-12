//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_comparison_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/enums/joinref_type.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/joinside.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/execution/operator/join/join_filter_pushdown.hpp"
#include "duckdb/catalog/catalog_entry/bitmap_join_meta.hpp"

namespace duckdb {

//! LogicalComparisonJoin represents a join that involves comparisons between the LHS and RHS
class LogicalComparisonJoin : public LogicalJoin {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_INVALID;

public:
	explicit LogicalComparisonJoin(JoinType type,
	                               LogicalOperatorType logical_type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN);

	//! The conditions of the join
	vector<JoinCondition> conditions;
	//! Used for duplicate-eliminated MARK joins
	vector<LogicalType> mark_types;
	//! The set of columns that will be duplicate eliminated from the LHS and pushed into the RHS
	vector<unique_ptr<Expression>> duplicate_eliminated_columns;
	//! If this is a DelimJoin, whether it has been flipped to de-duplicating the RHS instead
	bool delim_flipped = false;
	//! (If join_type == MARK) can this comparison join be converted from a mark join to semi
	bool convert_mark_to_semi = true;
	//! Scans where we should push generated filters into (if any)
	unique_ptr<JoinFilterPushdownInfo> filter_pushdown;
	//! Filtering predicate from the ON clause with expressions that don't reference both sides
	unique_ptr<Expression> predicate;
	//! Bitmap-Join (BHJ) auto-resolution hint (design doc b_idea/6.4, module 6.6 M-R prototype).
	//! Filled in by BitmapJoinResolver (the last built-in optimizer pass, running right before
	//! ColumnBindingResolver flattens column bindings) whenever this join's single equality
	//! condition matches a PK/FK binding registered in BitmapJoinMetaRegistry AND the PK
	//! (dimension) table is confirmed to land on the build side. PlanComparisonJoin reads this
	//! to wire up PhysicalHashJoin::use_bitmap_join without relying on the test-only
	//! SetForceResolvedPK override. Left null (the common case / open_bitmap_join=false) this
	//! has zero effect on planning.
	unique_ptr<BitmapJoinResolved> bhj_hint;
	//! Hidden build-side `_rowid` column reference (design doc b_idea/6.4, 条目3). Filled in by
	//! BitmapJoinResolver alongside `bhj_hint`, only when `bhj_hint->pk->rowid_column !=
	//! bhj_hint->pk->pk_column` (i.e. the PK column is not itself a dense rowid, so a separately
	//! materialized `_rowid` column must be scanned/propagated to this join instead). Starts out
	//! as a plain BoundColumnRefExpression pointing at the (possibly newly injected) hidden
	//! column on the build side; ColumnBindingResolver flattens it into a
	//! BoundReferenceExpression the same way it does `conditions[i].right`, so
	//! PlanComparisonJoin can read off the physical chunk index and wire up
	//! PhysicalHashJoin::bitmap_build_rowid_idx. Left null in the common fast-path case (rowid ==
	//! pk column) or when BHJ is not engaged at all.
	unique_ptr<Expression> bhj_build_rowid_ref;
	//! Hidden probe-side `*_ref` column reference (条目3), mirroring bhj_build_rowid_ref but for
	//! the FK (probe) side and feeding PhysicalHashJoin::bitmap_probe_ref_idx. Always set/unset
	//! in lockstep with bhj_build_rowid_ref (build/probe must agree on which rowid scheme is in
	//! use - see BitmapJoinResolver::ResolveJoin).
	unique_ptr<Expression> bhj_probe_ref_ref;
	//! Diagnostic-only (b_idea/6.4遗漏问题, 条目6): why BitmapJoinResolver did/didn't produce
	//! bhj_hint for this node. NOT_PROCESSED if the pass never ran (open_bitmap_join=false).
	//! Purely informational - never influences any planning/execution decision - so that
	//! EXPLAIN can show a reason for every HASH_JOIN node without one-off debug prints.
	BitmapJoinSkipReason bhj_skip_reason = BitmapJoinSkipReason::NOT_PROCESSED;

	//! 条目8 (b_idea/6.4遗漏问题 任务2): "passthrough" hidden columns that bypass this join's
	//! normal [left][right] output layout and are appended at the physical END of the join's
	//! output, so they don't shift any existing positions. Populated by PropagateHiddenColumn
	//! when a hidden column (_rowid/*_ref) needs to cross this join's LEFT side to reach an
	//! ancestor BHJ candidate join. Growing the LEFT side in the normal [left][right] layout
	//! would insert in the middle and shift all RIGHT-side positions, corrupting any ancestor
	//! projection_map that already references this join's RIGHT-side output by absolute position
	//! (see §8.2 root cause analysis). Appending at the end is always safe: nothing follows it.
	//! Each entry is a BoundColumnRefExpression referencing the hidden column's current binding
	//! (from the child subtree below this join); ColumnBindingResolver will flatten it into a
	//! BoundReferenceExpression just like any other column reference. Only populated for INNER
	//! joins (the only type BHJ supports). Empty (zero effect) in the common case.
	vector<unique_ptr<Expression>> bhj_passthrough_refs;
	//! 条目8: parallel to bhj_passthrough_refs - stores the original ColumnBinding of each
	//! passthrough column. Needed because ColumnBindingResolver replaces the BoundColumnRefExpression
	//! with a BoundReferenceExpression (losing the original binding), but GetColumnBindings()
	//! still needs to return the original binding for ancestor operators to find.
	vector<ColumnBinding> bhj_passthrough_bindings;

public:
	InsertionOrderPreservingMap<string> ParamsToString() const override;
	vector<ColumnBinding> GetColumnBindings() override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

protected:
	void ResolveTypes() override;

public:
	static unique_ptr<LogicalOperator> CreateJoin(ClientContext &context, JoinType type, JoinRefType ref_type,
	                                              unique_ptr<LogicalOperator> left_child,
	                                              unique_ptr<LogicalOperator> right_child,
	                                              unique_ptr<Expression> condition);
	static unique_ptr<LogicalOperator> CreateJoin(JoinType type, JoinRefType ref_type,
	                                              unique_ptr<LogicalOperator> left_child,
	                                              unique_ptr<LogicalOperator> right_child,
	                                              vector<JoinCondition> conditions,
	                                              vector<unique_ptr<Expression>> arbitrary_expressions);

	static void ExtractJoinConditions(ClientContext &context, JoinType type, JoinRefType ref_type,
	                                  unique_ptr<LogicalOperator> &left_child, unique_ptr<LogicalOperator> &right_child,
	                                  unique_ptr<Expression> condition, vector<JoinCondition> &conditions,
	                                  vector<unique_ptr<Expression>> &arbitrary_expressions);
	static void ExtractJoinConditions(ClientContext &context, JoinType type, JoinRefType ref_type,
	                                  unique_ptr<LogicalOperator> &left_child, unique_ptr<LogicalOperator> &right_child,
	                                  vector<unique_ptr<Expression>> &expressions, vector<JoinCondition> &conditions,
	                                  vector<unique_ptr<Expression>> &arbitrary_expressions);
	static void ExtractJoinConditions(ClientContext &context, JoinType type, JoinRefType ref_type,
	                                  unique_ptr<LogicalOperator> &left_child, unique_ptr<LogicalOperator> &right_child,
	                                  const unordered_set<idx_t> &left_bindings,
	                                  const unordered_set<idx_t> &right_bindings,
	                                  vector<unique_ptr<Expression>> &expressions, vector<JoinCondition> &conditions,
	                                  vector<unique_ptr<Expression>> &arbitrary_expressions);

	bool HasEquality(idx_t &range_count) const;
};

} // namespace duckdb
