//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/bitmap_join_resolver.hpp
//
// Bitmap-Join (BHJ) auto-resolution pass (design doc b_idea/6.4 后续进展-实现与测试方案.md,
// 条目1/2; module 6.6 M-R prototype).
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {
class ClientContext;
class LogicalComparisonJoin;

//! BitmapJoinResolver is the last built-in optimizer pass (runs right before
//! ColumnBindingResolver flattens BoundColumnRefExpression -> BoundReferenceExpression). For
//! every BHJ-eligible LogicalComparisonJoin (single equality condition, INNER / RIGHT_SEMI), it
//! attempts to resolve the join condition's underlying tables/columns against
//! BitmapJoinMetaRegistry and, on success, stashes the result on
//! LogicalComparisonJoin::bhj_hint for PlanComparisonJoin to consume.
//!
//! The pass is a pure read + annotate: it never rewrites expressions, so the default
//! LogicalOperatorVisitor::VisitReplace(BoundColumnRefExpression&, ...) (a no-op "don't
//! replace") is left untouched.
class BitmapJoinResolver : public LogicalOperatorVisitor {
public:
	explicit BitmapJoinResolver(ClientContext &context) : context(context) {
	}

	void VisitOperator(LogicalOperator &op) override;

private:
	void ResolveJoin(LogicalComparisonJoin &op);

	ClientContext &context;
};

} // namespace duckdb
