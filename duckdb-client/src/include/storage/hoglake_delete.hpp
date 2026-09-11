//===----------------------------------------------------------------------===//
// HoglakeDelete: DELETE via deletion vectors. The child scan emits the
// row-id columns (filename, file_index, file_row_number); the sink
// groups deleted positions per data file, merges them into the file's
// existing live DV (vectors only grow), writes a superseding puffin
// file, and buffers the DeleteFileRegistration on the transaction.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/set.hpp"
#include "common/hoglake_wire.hpp"

namespace duckdb {
class HoglakeTableEntry;
class PhysicalPlanGenerator;

class HoglakeDeleteGlobalState : public GlobalSinkState {
public:
	//! data file path -> deleted positions (0-based physical ordinals)
	map<string, set<idx_t>> new_deletes;
	idx_t deleted_count = 0;
};

class HoglakeDelete : public PhysicalOperator {
public:
	HoglakeDelete(PhysicalPlan &physical_plan, const vector<LogicalType> &types, HoglakeTableEntry &table,
	              vector<idx_t> row_id_indexes);

	HoglakeTableEntry &table;
	//! indexes of (filename, file_index, file_row_number) in the input
	vector<idx_t> row_id_indexes;

public:
	//! wire_child: push child_plan as a pipeline child (standalone
	//! DELETE); false when an update operator drives the sink manually.
	static PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
	                                    HoglakeTableEntry &table, PhysicalOperator &child_plan,
	                                    vector<idx_t> row_id_indexes, bool wire_child);

public:
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}

	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return false;
	}

	string GetName() const override;
};

} // namespace duckdb
