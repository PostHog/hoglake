#include "storage/hoglake_update.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "storage/hoglake_delete.hpp"
#include "storage/hoglake_table_entry.hpp"

namespace duckdb {

namespace {

struct FileRowId {
	uint64_t file_index;
	int64_t row_number;

	bool operator==(const FileRowId &other) const {
		return file_index == other.file_index && row_number == other.row_number;
	}
};

struct FileRowIdHash {
	hash_t operator()(const FileRowId &id) const {
		return CombineHash(Hash(id.file_index), Hash(id.row_number));
	}
};

} // namespace

HoglakeUpdate::HoglakeUpdate(PhysicalPlan &physical_plan, HoglakeTableEntry &table_p, vector<PhysicalIndex> columns_p,
                             PhysicalOperator &child, PhysicalOperator &delete_op_p,
                             vector<unique_ptr<Expression>> &expressions_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {}, 1), table(table_p),
      columns(std::move(columns_p)), delete_op(delete_op_p), expressions(std::move(expressions_p)) {
	children.push_back(child);
	row_id_index = columns.size();
}

string HoglakeUpdate::GetName() const {
	return "HOGLAKE_UPDATE";
}

class HoglakeUpdateGlobalState : public GlobalOperatorState {
public:
	atomic<idx_t> total_updated_count {0};
	//! duplicate row detection (first-write-wins)
	mutex seen_rows_lock;
	unordered_set<FileRowId, FileRowIdHash> seen_rows;
};

class HoglakeUpdateLocalState : public OperatorState {
public:
	unique_ptr<LocalSinkState> delete_local_state;
	unique_ptr<ExpressionExecutor> expression_executor;
	DataChunk update_expression_chunk;
	DataChunk delete_chunk;
};

unique_ptr<GlobalOperatorState> HoglakeUpdate::GetGlobalOperatorState(ClientContext &context) const {
	auto result = make_uniq<HoglakeUpdateGlobalState>();
	delete_op.sink_state = delete_op.GetGlobalSinkState(context);
	return std::move(result);
}

unique_ptr<OperatorState> HoglakeUpdate::GetOperatorState(ExecutionContext &context) const {
	auto result = make_uniq<HoglakeUpdateLocalState>();
	result->delete_local_state = delete_op.GetLocalSinkState(context);

	vector<LogicalType> expression_types;
	result->expression_executor = make_uniq<ExpressionExecutor>(context.client, expressions);
	for (auto &expr : result->expression_executor->expressions) {
		expression_types.push_back(expr->GetReturnType());
	}
	result->update_expression_chunk.Initialize(context.client, expression_types);

	vector<LogicalType> delete_types;
	delete_types.emplace_back(LogicalType::VARCHAR);
	delete_types.emplace_back(LogicalType::UBIGINT);
	delete_types.emplace_back(LogicalType::BIGINT);
	result->delete_chunk.Initialize(context.client, delete_types);
	return std::move(result);
}

OperatorResultType HoglakeUpdate::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                          GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &gstate = gstate_p.Cast<HoglakeUpdateGlobalState>();
	auto &lstate = state_p.Cast<HoglakeUpdateLocalState>();

	// deletion info rides the last 3 columns of the input
	idx_t delete_idx_start = input.ColumnCount() - DELETION_INFO_SIZE;
	auto &file_index_vec = input.data[delete_idx_start + 1];
	auto &row_number_vec = input.data[delete_idx_start + 2];

	UnifiedVectorFormat file_index_data, row_number_data;
	file_index_vec.ToUnifiedFormat(file_index_data);
	row_number_vec.ToUnifiedFormat(row_number_data);
	auto file_indices = UnifiedVectorFormat::GetData<uint64_t>(file_index_data);
	auto row_numbers = UnifiedVectorFormat::GetData<int64_t>(row_number_data);

	SelectionVector sel(input.size());
	idx_t sel_count = 0;
	{
		lock_guard<mutex> guard(gstate.seen_rows_lock);
		for (idx_t i = 0; i < input.size(); i++) {
			auto file_idx = file_index_data.sel->get_index(i);
			auto row_idx = row_number_data.sel->get_index(i);
			FileRowId key {file_indices[file_idx], row_numbers[row_idx]};
			if (gstate.seen_rows.insert(key).second) {
				sel.set_index(sel_count++, i);
			}
		}
	}
	if (sel_count == 0) {
		return OperatorResultType::NEED_MORE_INPUT;
	}
	input.Slice(sel, sel_count);

	// new row values -> the insert copy above us
	lstate.expression_executor->Execute(input, lstate.update_expression_chunk);
	chunk.Reference(lstate.update_expression_chunk);

	// old row ids -> the embedded delete sink
	auto &delete_chunk = lstate.delete_chunk;
	for (idx_t i = 0; i < DELETION_INFO_SIZE; i++) {
		delete_chunk.data[i].Reference(input.data[delete_idx_start + i]);
	}
	delete_chunk.SetCardinality(input.size());

	InterruptState interrupt_state;
	OperatorSinkInput delete_input {*delete_op.sink_state, *lstate.delete_local_state, interrupt_state};
	delete_op.Sink(context, delete_chunk, delete_input);

	gstate.total_updated_count += input.size();
	return OperatorResultType::NEED_MORE_INPUT;
}

OperatorFinalizeResultType HoglakeUpdate::FinalExecute(ExecutionContext &context, DataChunk &chunk,
                                                       GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	auto &lstate = state_p.Cast<HoglakeUpdateLocalState>();
	InterruptState interrupt_state;
	OperatorSinkCombineInput combine_input {*delete_op.sink_state, *lstate.delete_local_state, interrupt_state};
	auto result = delete_op.Combine(context, combine_input);
	if (result != SinkCombineResultType::FINISHED) {
		throw InternalException("HoglakeUpdate::FinalExecute does not support async child operators");
	}
	return OperatorFinalizeResultType::FINISHED;
}

OperatorFinalResultType HoglakeUpdate::OperatorFinalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                        OperatorFinalizeInput &input) const {
	OperatorSinkFinalizeInput finalize_input {*delete_op.sink_state, input.interrupt_state};
	auto result = delete_op.Finalize(pipeline, event, context, finalize_input);
	if (result != SinkFinalizeType::READY) {
		throw InternalException("HoglakeUpdate::OperatorFinalize does not support async child operators");
	}
	return OperatorFinalResultType::FINISHED;
}

} // namespace duckdb
