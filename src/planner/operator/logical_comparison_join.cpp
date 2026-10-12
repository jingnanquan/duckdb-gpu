#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/common/enum_util.hpp"

namespace duckdb {

LogicalComparisonJoin::LogicalComparisonJoin(JoinType join_type, LogicalOperatorType logical_type)
    : LogicalJoin(join_type, logical_type) {
}

vector<ColumnBinding> LogicalComparisonJoin::GetColumnBindings() {
	// Start with the normal [left][right] layout from LogicalJoin.
	auto result = LogicalJoin::GetColumnBindings();
	// 条目8: append passthrough hidden columns at the physical END of the join's output.
	// Use the stored bindings (bhj_passthrough_bindings) rather than reading from the
	// expressions, because ColumnBindingResolver may have already replaced the
	// BoundColumnRefExpression with a BoundReferenceExpression (losing the original binding).
	result.insert(result.end(), bhj_passthrough_bindings.begin(), bhj_passthrough_bindings.end());
	return result;
}

void LogicalComparisonJoin::ResolveTypes() {
	// Start with the normal [left][right] types from LogicalJoin.
	LogicalJoin::ResolveTypes();
	// 条目8: append types for passthrough hidden columns, matching GetColumnBindings().
	for (auto &expr : bhj_passthrough_refs) {
		types.push_back(expr->return_type);
	}
}

InsertionOrderPreservingMap<string> LogicalComparisonJoin::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Join Type"] = EnumUtil::ToChars(join_type);

	string conditions_info;
	for (idx_t i = 0; i < conditions.size(); i++) {
		if (i > 0) {
			conditions_info += "\n";
		}
		auto &condition = conditions[i];
		auto expr =
		    make_uniq<BoundComparisonExpression>(condition.comparison, condition.left->Copy(), condition.right->Copy());
		conditions_info += expr->ToString();
	}
	if (predicate) {
		if (!conditions.empty()) {
			conditions_info += "\n";
		}
		conditions_info += predicate->ToString();
	}
	result["Conditions"] = conditions_info;
	SetParamsEstimatedCardinality(result);

	return result;
}

bool LogicalComparisonJoin::HasEquality(idx_t &range_count) const {
	bool result = false;
	for (size_t c = 0; c < conditions.size(); ++c) {
		auto &cond = conditions[c];
		switch (cond.comparison) {
		case ExpressionType::COMPARE_EQUAL:
		case ExpressionType::COMPARE_NOT_DISTINCT_FROM:
			result = true;
			break;
		case ExpressionType::COMPARE_LESSTHAN:
		case ExpressionType::COMPARE_GREATERTHAN:
		case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
			++range_count;
			break;
		case ExpressionType::COMPARE_NOTEQUAL:
		case ExpressionType::COMPARE_DISTINCT_FROM:
			break;
		default:
			throw NotImplementedException("Unimplemented comparison join");
		}
	}
	return result;
}

} // namespace duckdb
