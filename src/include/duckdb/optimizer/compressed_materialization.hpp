//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/compressed_materialization.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

class Optimizer;
class ClientContext;
class LogicalOperator;

struct CMChildInfo {
public:
	CMChildInfo(LogicalOperator &op, const column_binding_set_t &referenced_bindings);

public:
	//! Bindings and types before compressing
	vector<ColumnBinding> bindings_before;
	vector<LogicalType> &types;
	//! Whether the input binding is eligible for compression
	vector<bool> can_compress;

	//! Bindings after compressing (projection on top)
	vector<ColumnBinding> bindings_after;
};

struct CMBindingInfo {
public:
	explicit CMBindingInfo(ColumnBinding binding, const LogicalType &type);

public:
	ColumnBinding binding;

	//! Type before compressing
	LogicalType type;
	bool needs_decompression;
	unique_ptr<BaseStatistics> stats;
};

struct CompressedMaterializationInfo {
public:
	CompressedMaterializationInfo(LogicalOperator &op, vector<idx_t> &&child_idxs,
	                              const column_binding_set_t &referenced_bindings);

public:
	//! Mapping from incoming bindings to outgoing bindings
	column_binding_map_t<CMBindingInfo> binding_map;

	//! Operator child info
	vector<idx_t> child_idxs;
	vector<CMChildInfo> child_info;
};

struct CompressExpression {
public:
	CompressExpression(unique_ptr<Expression> expression, unique_ptr<BaseStatistics> stats);

public:
	unique_ptr<Expression> expression;
	unique_ptr<BaseStatistics> stats;
};

typedef column_binding_map_t<unique_ptr<BaseStatistics>> statistics_map_t;

//! The CompressedMaterialization optimizer compressed columns using projections, based on available statistics,
//! but only if the data enters a materializing operator
class CompressedMaterialization {
private:
	//! Somewhat defensive constants that try to limit when compressed materialization is triggered for joins
	//! We only consider compressed materialization for joins when the build cardinality is greater than this
	static constexpr idx_t JOIN_BUILD_CARDINALITY_THRESHOLD = 1048576;
	//! If the cardinality > threshold, we always perform compressed materialization if there are this many columns
	static constexpr idx_t JOIN_BUILD_COLUMN_COUNT_THRESHOLD = 20;
	//! If not, we perform compressed materialization if join result cardinality > build cardinality * this threshold
	static constexpr double JOIN_CARDINALITY_RATIO_THRESHOLD = 8;

public:
	CompressedMaterialization(Optimizer &optimizer, LogicalOperator &root, statistics_map_t &statistics_map,
	                          const column_binding_set_t &bhj_protected_bindings);

	void Compress(unique_ptr<LogicalOperator> &op);

	//! 条目8b (b_idea/6.4遗漏问题 任务2): one-time, read-only, whole-plan walk (called once by
	//! StatisticsPropagator, before any compression begins) collecting every ColumnBinding that
	//! participates as either side of some single-equality INNER join's condition *anywhere* in
	//! the plan - not just the join currently being compressed. This is necessary because
	//! CompressedMaterialization runs bottom-up, one materializing operator at a time: a column
	//! that is merely a *payload* column at some lower join/aggregate (and would otherwise be
	//! compressed there as an ordinary optimization) may still be the literal join-key operand of
	//! some *ancestor* join further up the plan - e.g. real SF5 Q10's `orders.o_custkey` is a
	//! payload column of the `lineitem JOIN orders` join, but the literal equality-condition
	//! operand of the ancestor `customer JOIN (...)` join. Compressing it at the lower join wraps
	//! it in an `__internal_compress_integral_*` function that BitmapJoinResolver::
	//! TraceBindingToGet (running after this entire pass) cannot see through, silently disabling
	//! BHJ for the ancestor join even though that ancestor join's *own* condition was never
	//! touched. See bhj_protected_bindings for how the result is consumed.
	static void CollectBhjProtectedBindings(LogicalOperator &op, column_binding_set_t &out);

private:
	//! Compress materializing operators
	void CompressAggregate(unique_ptr<LogicalOperator> &op);
	void CompressComparisonJoin(unique_ptr<LogicalOperator> &op);
	void CompressDistinct(unique_ptr<LogicalOperator> &op);
	void CompressOrder(unique_ptr<LogicalOperator> &op);

	//! Update statistics after compressing
	void UpdateAggregateStats(unique_ptr<LogicalOperator> &op);
	void UpdateComparisonJoinStats(unique_ptr<LogicalOperator> &op);
	void UpdateOrderStats(unique_ptr<LogicalOperator> &op);

	//! Adds bindings referenced in expression to referenced_bindings
	static void GetReferencedBindings(const Expression &expression, column_binding_set_t &referenced_bindings);
	//! Updates CMBindingInfo in the binding_map in info
	void UpdateBindingInfo(CompressedMaterializationInfo &info, const ColumnBinding &binding, bool needs_decompression);

	//! Create (de)compress projections around the operator
	void CreateProjections(unique_ptr<LogicalOperator> &op, CompressedMaterializationInfo &info);
	bool TryCompressChild(CompressedMaterializationInfo &info, const CMChildInfo &child_info,
	                      vector<unique_ptr<CompressExpression>> &compress_expressions);
	void CreateCompressProjection(unique_ptr<LogicalOperator> &child_op,
	                              vector<unique_ptr<CompressExpression>> compress_exprs,
	                              CompressedMaterializationInfo &info, CMChildInfo &child_info);
	void CreateDecompressProjection(unique_ptr<LogicalOperator> &op, CompressedMaterializationInfo &info);

	//! Create expressions that apply a scalar compression function
	unique_ptr<CompressExpression> GetCompressExpression(const ColumnBinding &binding, const LogicalType &type,
	                                                     const bool &can_compress);
	unique_ptr<CompressExpression> GetCompressExpression(unique_ptr<Expression> input, const BaseStatistics &stats);
	unique_ptr<CompressExpression> GetIntegralCompress(unique_ptr<Expression> input, const BaseStatistics &stats);
	unique_ptr<CompressExpression> GetStringCompress(unique_ptr<Expression> input, const BaseStatistics &stats);

	//! Create an expression that applies a scalar decompression function
	unique_ptr<Expression> GetDecompressExpression(unique_ptr<Expression> input, const LogicalType &result_type,
	                                               const BaseStatistics &stats);
	unique_ptr<Expression> GetIntegralDecompress(unique_ptr<Expression> input, const LogicalType &result_type,
	                                             const BaseStatistics &stats);
	unique_ptr<Expression> GetStringDecompress(unique_ptr<Expression> input, const LogicalType &result_type,
	                                           const BaseStatistics &stats);

private:
	Optimizer &optimizer;
	ClientContext &context;
	//! The root of the query plan
	optional_ptr<LogicalOperator> root;
	//! The map of ColumnBinding -> statistics for the various nodes
	statistics_map_t &statistics_map;
	//! 条目8b (b_idea/6.4遗漏问题 任务2): bindings that must never be compressed anywhere in the
	//! plan because some INNER join *somewhere* (not necessarily the join currently being
	//! compressed) uses them as an equality-condition operand and open_bitmap_join=true - see
	//! CollectBhjProtectedBindings. Computed once by StatisticsPropagator before any compression
	//! begins; empty (and thus zero-overhead) whenever open_bitmap_join=false.
	const column_binding_set_t &bhj_protected_bindings;
};

} // namespace duckdb
