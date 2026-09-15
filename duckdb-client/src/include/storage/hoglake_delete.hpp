//===----------------------------------------------------------------------===//
// HoglakeDelete: DELETE via deletion vectors. The child scan emits the
// row-id columns; the sink groups deleted positions per LOGICAL data
// file — keyed by (filename, rowid - file_row_number), because the
// catalog legally holds multiple live registrations of one physical
// path (writer retries) and only the rowid base distinguishes them —
// merges each group into that registration's existing live DV
// (vectors only grow), writes a superseding puffin file per
// data_file_id, and buffers the DeleteFileRegistrations on the
// transaction.
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
	//! (data file path, rowid base = rowid - file_row_number) ->
	//! deleted positions (0-based physical ordinals). The base is the
	//! registration's row_id_start for positional files, letting
	//! Finalize attribute positions to the RIGHT logical copy of a
	//! duplicate-registered path; explicit-row-id files produce
	//! non-constant bases, which Finalize tolerates for
	//! single-registration paths (all groups union) and refuses,
	//! typed, for duplicate-registered ones.
	map<string, map<int64_t, set<idx_t>>> new_deletes;
	idx_t deleted_count = 0;
};

class HoglakeDelete : public PhysicalOperator {
public:
	HoglakeDelete(PhysicalPlan &physical_plan, const vector<LogicalType> &types, HoglakeTableEntry &table,
	              vector<idx_t> row_id_indexes);

	HoglakeTableEntry &table;
	//! indexes of (rowid, filename, file_row_number) in the input
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
