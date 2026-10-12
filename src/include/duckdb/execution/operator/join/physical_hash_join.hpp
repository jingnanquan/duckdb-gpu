//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_hash_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/catalog/catalog_entry/bitmap_join_meta.hpp"
#include "duckdb/execution/join_hashtable.hpp"
#include "duckdb/execution/operator/join/perfect_hash_join_executor.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {

//! PhysicalHashJoin represents a hash loop join between two tables
class PhysicalHashJoin : public PhysicalComparisonJoin {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::HASH_JOIN;

	struct JoinProjectionColumns {
		vector<idx_t> col_idxs;
		vector<LogicalType> col_types;
	};

public:
	PhysicalHashJoin(PhysicalPlan &physical_plan, LogicalOperator &op, PhysicalOperator &left, PhysicalOperator &right,
	                 vector<JoinCondition> cond, JoinType join_type, const vector<idx_t> &left_projection_map,
	                 const vector<idx_t> &right_projection_map, vector<LogicalType> delim_types,
	                 idx_t estimated_cardinality, unique_ptr<JoinFilterPushdownInfo> pushdown_info);
	PhysicalHashJoin(PhysicalPlan &physical_plan, LogicalOperator &op, PhysicalOperator &left, PhysicalOperator &right,
	                 vector<JoinCondition> cond, JoinType join_type, idx_t estimated_cardinality);

	//! Initialize HT for this operator
	unique_ptr<JoinHashTable> InitializeHashTable(ClientContext &context) const;

	//! The types of the join keys
	vector<LogicalType> condition_types;

	//! The indices/types of the payload columns
	JoinProjectionColumns payload_columns;
	//! The indices/types of the lhs columns that need to be output
	JoinProjectionColumns lhs_output_columns;
	//! The indices/types of the rhs columns that need to be output
	JoinProjectionColumns rhs_output_columns;

	//! Duplicate eliminated types; only used for delim_joins (i.e. correlated subqueries)
	vector<LogicalType> delim_types;

	//! Join Keys statistics (optional)
	vector<unique_ptr<BaseStatistics>> join_stats;

	//===------------------------------------------------------------------===//
	// Bitmap-Join (BHJ) hook (design doc §6.3, module M-O)
	//===------------------------------------------------------------------===//
	//! Whether this join should use the bitmap-join path. Set at plan time (see
	//! PlanComparisonJoin) only when the global force switch is enabled and the join resolves
	//! against BitmapJoinMetaRegistry. Default false => the existing execution path is unchanged.
	//! NOTE: the BitmapJoinExecutor itself is module 6.4; when this flag is true the execution
	//! path currently raises NotImplementedException.
	bool use_bitmap_join = false;
	//! Resolved PK/FK binding for this join (valid only when use_bitmap_join is true).
	BitmapJoinResolved bitmap_join_resolved {};
	//! Index of the build-side rowid column within the build chunk.
	idx_t bitmap_build_rowid_idx = DConstants::INVALID_INDEX;
	//! Index of the probe-side *_ref column within the probe chunk.
	idx_t bitmap_probe_ref_idx = DConstants::INVALID_INDEX;
	//! Diagnostic-only (b_idea/6.4遗漏问题, 条目6): copy of
	//! LogicalComparisonJoin::bhj_skip_reason, taken at PlanComparisonJoin time. Only consumed
	//! by ParamsToString for EXPLAIN output; never affects execution.
	BitmapJoinSkipReason bhj_skip_reason = BitmapJoinSkipReason::NOT_PROCESSED;

	//! 条目8 (b_idea/6.4遗漏问题 任务2): indices into the probe (LHS) input chunk for columns
	//! that must be passed through to this join's output AFTER the normal [lhs_output]
	//! [rhs_output] columns. Populated from LogicalComparisonJoin::bhj_passthrough_refs (already
	//! resolved to BoundReferenceExpression by ColumnBindingResolver) when the physical plan is
	//! created. Each index is the position of a hidden column (_rowid/*_ref) in the LHS child's
	//! output that was injected by BitmapJoinResolver::PropagateHiddenColumn. The execution
	//! paths (both regular HashJoin and BHJ) append these columns to the output chunk after
	//! the normal [lhs][rhs] columns. Empty (zero effect) in the common case.
	vector<idx_t> passthrough_lhs_col_idxs;

public:
	InsertionOrderPreservingMap<string> ParamsToString() const override;

public:
	// Operator Interface
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	bool ParallelOperator() const override {
		return true;
	}

protected:
	// CachingOperator Interface
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const override;

	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	ProgressData GetProgress(ClientContext &context, GlobalSourceState &gstate) const override;

	//! Becomes a source when it is an external join
	bool IsSource() const override {
		return true;
	}

	bool ParallelSource() const override {
		return true;
	}

public:
	// Sink Interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	void PrepareFinalize(ClientContext &context, GlobalSinkState &global_state) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}
};

} // namespace duckdb
