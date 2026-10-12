#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"
#include "duckdb/execution/operator/join/physical_blockwise_nl_join.hpp"
#include "duckdb/execution/operator/join/physical_cross_product.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/join/physical_iejoin.hpp"
#include "duckdb/execution/operator/join/physical_nested_loop_join.hpp"
#include "duckdb/execution/operator/join/physical_piecewise_merge_join.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/catalog/catalog_entry/bitmap_join_meta.hpp"

namespace duckdb {

static void RewriteJoinCondition(unique_ptr<Expression> &root_expr, idx_t offset) {
	ExpressionIterator::VisitExpressionMutable<BoundReferenceExpression>(
	    root_expr, [&](BoundReferenceExpression &ref, unique_ptr<Expression> &expr) { ref.index += offset; });
}

PhysicalOperator &PhysicalPlanGenerator::PlanComparisonJoin(LogicalComparisonJoin &op) {
	// now visit the children
	D_ASSERT(op.children.size() == 2);
	idx_t lhs_cardinality = op.children[0]->EstimateCardinality(context);
	idx_t rhs_cardinality = op.children[1]->EstimateCardinality(context);
	auto &left = CreatePlan(*op.children[0]);
	auto &right = CreatePlan(*op.children[1]);
	left.estimated_cardinality = lhs_cardinality;
	right.estimated_cardinality = rhs_cardinality;

	if (op.conditions.empty()) {
		// no conditions: insert a cross product
		return Make<PhysicalCrossProduct>(op.types, left, right, op.estimated_cardinality);
	}

	idx_t has_range = 0;
	bool has_equality = op.HasEquality(has_range);
	bool can_merge = has_range > 0;
	bool can_iejoin = has_range >= 2 && recursive_cte_tables.empty();
	switch (op.join_type) {
	case JoinType::SEMI:
	case JoinType::ANTI:
	case JoinType::RIGHT_ANTI:
	case JoinType::RIGHT_SEMI:
	case JoinType::MARK:
		can_merge = can_merge && op.conditions.size() == 1;
		can_iejoin = false;
		break;
	default:
		break;
	}
	//	TODO: Extend PWMJ to handle all comparisons and projection maps
	bool prefer_range_joins = Settings::Get<PreferRangeJoinsSetting>(context);
	prefer_range_joins = prefer_range_joins && can_iejoin;
	if (has_equality && !prefer_range_joins) {
		// Detect Bitmap-Join (BHJ) eligibility BEFORE op.conditions is moved into the operator
		// (design §6.3): single equality condition on an INNER join.
		// NOTE (b_idea/6.4遗漏问题 任务1 条目7/条目9): this used to also allow RIGHT_SEMI, but
		// BitmapJoinExecutor's constructor only ever supported JoinType::INNER (see
		// bitmap_hash_join_executor.cpp) - so a RIGHT_SEMI join that BitmapJoinResolver had
		// wired up a bhj_hint for (e.g. real TPCH Q20's `s_suppkey IN (SELECT ps_suppkey FROM
		// partsupp WHERE ...)` decorrelates into exactly this shape) would hit a
		// NotImplementedException at construction time instead of safely falling back to a
		// regular hash join - a real, reproduced crash on open_bitmap_join=true, caught by the
		// 22-query TPC-H smoke test. Tightened to INNER-only here (and in
		// BitmapJoinResolver::ResolveJoin) to match what the executor actually supports; see
		// task2 条目9 for whether RIGHT_SEMI support is worth adding properly in the future.
		bool bhj_eligible =
		    op.conditions.size() == 1 && op.conditions[0].comparison == ExpressionType::COMPARE_EQUAL &&
		    op.join_type == JoinType::INNER;

		// Equality join with small number of keys : possible perfect join optimization
		auto &join = Make<PhysicalHashJoin>(op, left, right, std::move(op.conditions), op.join_type,
		                                    op.left_projection_map, op.right_projection_map, std::move(op.mark_types),
		                                    op.estimated_cardinality, std::move(op.filter_pushdown));
		auto &hash_join = join.Cast<PhysicalHashJoin>();
		hash_join.join_stats = std::move(op.join_stats);
		// 条目6 (b_idea/6.4遗漏问题): carry the diagnostic-only skip reason over to the physical
		// operator so EXPLAIN can display it via ParamsToString.
		hash_join.bhj_skip_reason = op.bhj_skip_reason;

		// 条目8 (b_idea/6.4遗漏问题 任务2): carry over the passthrough hidden columns
		// (bhj_passthrough_refs, already flattened to BoundReferenceExpression by
		// ColumnBindingResolver) as physical LHS input indices. This is independent of whether
		// this join itself ends up using BHJ - both the regular HashJoin/perfect-hash-join path
		// and the BHJ ProbeBitmap path append these columns after the normal output columns
		// (see PhysicalHashJoin::ExecuteInternal / BitmapJoinExecutor::ProbeBitmap).
		for (auto &expr : op.bhj_passthrough_refs) {
			hash_join.passthrough_lhs_col_idxs.push_back(expr->Cast<BoundReferenceExpression>().index);
		}

		// Bitmap-Join hook: plan-time enablement. Two paths feed into the same PK binding:
		//   1. Test-only force override (BitmapJoinMetaRegistry::SetForceResolvedPK), kept as a
		//      short-circuit so white-box tests can exercise BHJ without depending on the
		//      BitmapJoinResolver optimizer pass.
		//   2. Automatic path (design doc 条目1/2): BitmapJoinResolver already reverse-mapped
		//      the join condition to catalog table/column names and, if the PK (dimension)
		//      table was confirmed to be on the build side, stashed the result on
		//      `op.bhj_hint`. When absent (open_bitmap_join=false, no registry match, or the FK
		//      table landed on the build side) we simply fall back to a regular hash join.
	auto &registry = BitmapJoinMetaRegistry::Get(context);
	optional_ptr<const BitmapJoinPKBinding> pk;
	bool bhj_forced = false;
	if (bhj_eligible && registry.IsForceBitmapJoin() && registry.GetForceResolvedPK()) {
		pk = registry.GetForceResolvedPK();
		bhj_forced = true;
	} else if (bhj_eligible && op.bhj_hint) {
		// Defensive double-check (条目2): even if some future code path bypassed
		// BitmapJoinResolver and set bhj_hint directly, never wire up a reversed binding -
		// BitmapJoinExecutor only supports build_is_pk_side == true.
		D_ASSERT(op.bhj_hint->build_is_pk_side);
		if (op.bhj_hint->build_is_pk_side) {
			pk = op.bhj_hint->pk;
		}
	}
	if (pk) {
		// 初期方案 4.2 (perf/sf5/combine锁 / 根因分析-详细版.md §4.2): 低密度回退。
		// 估计 build 侧密度 = build(右)子节点基数 / 整张 PK 维度表行数。低密度意味着 bitmap
		// 稀疏、payload 仍按全维度行数分配 → gather 散布 cache miss；此时 BHJ 反而不如普通/
		// perfect 哈希 join，直接回退。仅对自动解析路径生效；force 覆盖(测试)不受影响。
		if (!bhj_forced && false) {
			const double est_density = (pk->row_count > 0)
			                             ? static_cast<double>(right.estimated_cardinality) /
			                                   static_cast<double>(pk->row_count)
			                             : 1.0;
			if (est_density < BHJ_LOW_DENSITY_THRESHOLD) {
				hash_join.bhj_skip_reason = BitmapJoinSkipReason::LOW_DENSITY;
				pk = nullptr; // 不走 BHJ，下方回退到普通哈希 join
			}
		}
	}
	if (pk) {
			hash_join.use_bitmap_join = true;
			hash_join.bitmap_join_resolved.pk = pk.get();
			hash_join.bitmap_join_resolved.fk = nullptr; // not needed for BHJ build/probe
			hash_join.bitmap_join_resolved.build_is_pk_side = true;
			// Bitmap-Join (BHJ) hidden `_rowid`/`*_ref` columns (design doc 条目3): only set
			// when the PK column itself is not a dense rowid. BitmapJoinResolver has already
			// flattened these (alongside cond.left/cond.right) via ColumnBindingResolver, so
			// they are now BoundReferenceExpressions pointing at a physical position within the
			// build/probe chunk respectively. Left at DConstants::INVALID_INDEX (the default) in
			// the common fast-path case, which BitmapJoinExecutor interprets as "reuse the join
			// key value directly".
			if (op.bhj_build_rowid_ref) {
				hash_join.bitmap_build_rowid_idx = op.bhj_build_rowid_ref->Cast<BoundReferenceExpression>().index;
			}
			if (op.bhj_probe_ref_ref) {
				hash_join.bitmap_probe_ref_idx = op.bhj_probe_ref_ref->Cast<BoundReferenceExpression>().index;
			}
		}
		return join;
	}

	D_ASSERT(op.left_projection_map.empty());
	idx_t nested_loop_join_threshold = Settings::Get<NestedLoopJoinThresholdSetting>(context);
	if (left.estimated_cardinality < nested_loop_join_threshold ||
	    right.estimated_cardinality < nested_loop_join_threshold) {
		can_iejoin = false;
		can_merge = false;
	}

	if (can_merge && can_iejoin) {
		idx_t merge_join_threshold = Settings::Get<MergeJoinThresholdSetting>(context);
		if (left.estimated_cardinality < merge_join_threshold || right.estimated_cardinality < merge_join_threshold) {
			can_iejoin = false;
		}
	}

	if (can_iejoin) {
		return Make<PhysicalIEJoin>(op, left, right, std::move(op.conditions), op.join_type, op.estimated_cardinality,
		                            std::move(op.filter_pushdown));
	}
	if (can_merge) {
		// range join: use piecewise merge join
		return Make<PhysicalPiecewiseMergeJoin>(op, left, right, std::move(op.conditions), op.join_type,
		                                        op.estimated_cardinality, std::move(op.filter_pushdown));
	}
	if (PhysicalNestedLoopJoin::IsSupported(op.conditions, op.join_type)) {
		// inequality join: use nested loop
		return Make<PhysicalNestedLoopJoin>(op, left, right, std::move(op.conditions), op.join_type,
		                                    op.estimated_cardinality, std::move(op.filter_pushdown));
	}

	for (auto &cond : op.conditions) {
		RewriteJoinCondition(cond.right, left.types.size());
	}
	auto condition = JoinCondition::CreateExpression(std::move(op.conditions));
	return Make<PhysicalBlockwiseNLJoin>(op, left, right, std::move(condition), op.join_type, op.estimated_cardinality);
}

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalComparisonJoin &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		return PlanAsOfJoin(op);
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
		return PlanComparisonJoin(op);
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
		return PlanDelimJoin(op);
	default:
		throw InternalException("Unrecognized operator type for LogicalComparisonJoin");
	}
}

} // namespace duckdb
