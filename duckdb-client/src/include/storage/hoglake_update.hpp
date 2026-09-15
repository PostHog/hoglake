//===----------------------------------------------------------------------===//
// HoglakeUpdate: UPDATE = delete + insert. A streaming operator that
// (a) sinks the deleted row ids into an embedded HoglakeDelete and
// (b) emits the updated row values into the insert copy above it.
// NOTE: unlike ducklake, updated rows get NEW hoglake row ids — the
// wire contract has no client-registered explicit-row-id files.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {
class HoglakeTableEntry;

class HoglakeUpdate : public PhysicalOperator {
public:
	//! (filename, file_index, file_row_number)
	static constexpr uint8_t DELETION_INFO_SIZE = 3;

	HoglakeUpdate(PhysicalPlan &physical_plan, HoglakeTableEntry &table, vector<PhysicalIndex> columns,
	              PhysicalOperator &child, PhysicalOperator &delete_op, vector<unique_ptr<Expression>> &expressions);

	HoglakeTableEntry &table;
	vector<PhysicalIndex> columns;
	PhysicalOperator &delete_op;
	vector<unique_ptr<Expression>> expressions;
	idx_t row_id_index;

public:
	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const override;
	OperatorFinalizeResultType FinalExecute(ExecutionContext &context, DataChunk &chunk, GlobalOperatorState &gstate,
	                                        OperatorState &state) const override;
	OperatorFinalResultType OperatorFinalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                                         OperatorFinalizeInput &input) const override;

	bool RequiresFinalExecute() const override {
		return true;
	}
	bool RequiresOperatorFinalize() const override {
		return true;
	}
	bool ParallelOperator() const override {
		return false;
	}

	string GetName() const override;
};

} // namespace duckdb
