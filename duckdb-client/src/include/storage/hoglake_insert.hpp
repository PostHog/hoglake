//===----------------------------------------------------------------------===//
// HoglakeInsert: INSERT / CTAS sink. The child is a PhysicalCopyToFile
// (parquet with hoglake field ids, hive-partitioned under the live
// spec, rotating at the target file size) whose WRITTEN_FILE_STATISTICS
// rows this operator turns into buffered FileRegistrations on the
// transaction; the footer-shipping commit happens at COMMIT.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "common/hoglake_wire.hpp"

namespace duckdb {
class HoglakeTableEntry;
class HoglakeSchemaEntry;

class HoglakeInsertGlobalState : public GlobalSinkState {
public:
	explicit HoglakeInsertGlobalState(HoglakeTableEntry &table);

	HoglakeTableEntry &table;
	//! files written by the child copy, in arrival order
	vector<HoglakeFileRegistration> written_files;
	idx_t total_insert_count = 0;
	//! top-level NOT NULL column names (client-side constraint check
	//! from the written null_count stats, ducklake-style)
	case_insensitive_set_t not_null_columns;
};

class HoglakeInsert : public PhysicalOperator {
public:
	HoglakeInsert(PhysicalPlan &physical_plan, const vector<LogicalType> &types, HoglakeTableEntry &table);

	//! The table to insert into
	HoglakeTableEntry &table;

public:
	static PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
	                                    HoglakeTableEntry &table, optional_ptr<PhysicalOperator> plan);

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}

	// Sink interface
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
