#include "storage/hoglake_delete.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "storage/hoglake_catalog.hpp"
#include "storage/hoglake_puffin.hpp"
#include "storage/hoglake_table_entry.hpp"
#include "storage/hoglake_transaction.hpp"

namespace duckdb {

HoglakeDelete::HoglakeDelete(PhysicalPlan &physical_plan, const vector<LogicalType> &types, HoglakeTableEntry &table_p,
                             vector<idx_t> row_id_indexes_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, types, 1), table(table_p),
      row_id_indexes(std::move(row_id_indexes_p)) {
}

unique_ptr<GlobalSinkState> HoglakeDelete::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<HoglakeDeleteGlobalState>();
}

string HoglakeDelete::GetName() const {
	return "HOGLAKE_DELETE";
}

SinkResultType HoglakeDelete::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<HoglakeDeleteGlobalState>();

	auto &file_name_vector = chunk.data[row_id_indexes[0]];
	auto &file_row_number_vector = chunk.data[row_id_indexes[2]];

	UnifiedVectorFormat file_name_data;
	file_name_vector.ToUnifiedFormat(file_name_data);
	auto file_names = UnifiedVectorFormat::GetData<string_t>(file_name_data);

	UnifiedVectorFormat row_number_data;
	file_row_number_vector.ToUnifiedFormat(row_number_data);
	auto row_numbers = UnifiedVectorFormat::GetData<int64_t>(row_number_data);

	for (idx_t i = 0; i < chunk.size(); i++) {
		auto name_idx = file_name_data.sel->get_index(i);
		auto row_idx = row_number_data.sel->get_index(i);
		if (!file_name_data.validity.RowIsValid(name_idx) || !row_number_data.validity.RowIsValid(row_idx)) {
			throw InternalException("hoglake: NULL row-id column in DELETE input");
		}
		auto inserted =
		    gstate.new_deletes[file_names[name_idx].GetString()].insert(NumericCast<idx_t>(row_numbers[row_idx]));
		if (inserted.second) {
			gstate.deleted_count++;
		}
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType HoglakeDelete::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                         OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<HoglakeDeleteGlobalState>();
	if (gstate.new_deletes.empty()) {
		return SinkFinalizeType::READY;
	}
	auto &transaction = HoglakeTransaction::Get(context, table.ParentCatalog());
	auto &catalog = table.ParentCatalog().Cast<HoglakeCatalog>();
	auto ns = table.ParentSchema().name.GetIdentifierName();
	// the server's exact table name (identifier-case policy: all wire
	// calls use the resolved name)
	auto table_name = table.GetWireInfo().name;

	// resolve data files (ids + live DVs) at the entry's read travel —
	// the same plan the scan that produced these row ids used
	auto scan_files = transaction.Api().PlanScan(ns, table_name, table.GetReadTravel());
	map<string, const HoglakeScanFile *> by_path;
	for (auto &file : scan_files) {
		by_path.emplace(file.data_file.path, &file);
	}

	auto &fs = FileSystem::GetFileSystem(context);
	auto data_path = catalog.DataPath();
	if (!data_path.empty() && data_path.back() != '/') {
		data_path += "/";
	}

	vector<HoglakeDeleteFileRegistration> registrations;
	for (auto &entry : gstate.new_deletes) {
		auto &data_file_path = entry.first;
		auto file_entry = by_path.find(data_file_path);
		if (file_entry == by_path.end()) {
			throw TransactionException("hoglake: DELETE targets data file \"%s\" which is not live at the pinned "
			                           "snapshot (concurrent compaction or drop?) — re-run the DELETE",
			                           data_file_path);
		}
		auto &scan_file = *file_entry->second;

		// vectors only grow: the superseding DV = (server-live DV) ∪
		// (positions already buffered by EARLIER statements of this
		// transaction) ∪ (this statement's new positions) — one commit
		// must carry exactly one DV per data file, containing everything
		set<idx_t> positions = entry.second;
		if (scan_file.has_delete_file) {
			auto existing = HoglakePuffin::ReadDeletionVector(context, scan_file.delete_file.path);
			positions.insert(existing.begin(), existing.end());
		}
		auto buffered = transaction.GetBufferedDeletePositions(ns, table_name, scan_file.data_file.data_file_id);
		positions.insert(buffered.begin(), buffered.end());
		auto record_count = NumericCast<idx_t>(scan_file.data_file.record_count);
		if (!positions.empty() && *positions.rbegin() >= record_count) {
			throw InternalException("hoglake: deleted position %llu out of range for data file \"%s\" (%llu rows)",
			                        *positions.rbegin(), data_file_path, record_count);
		}

		auto puffin = HoglakePuffin::WritePuffinFile(positions, data_file_path);
		auto dv_path = data_path + "data/" + ns + "/" + table_name + "/hoglake-dv-" +
		               UUID::ToString(UUID::GenerateRandomUUID()) + ".puffin";
		auto handle = fs.OpenFile(dv_path, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		handle->Write(puffin.data(), puffin.size());
		handle->Close();

		HoglakeDeleteFileRegistration registration;
		registration.data_file_id = scan_file.data_file.data_file_id;
		registration.path = dv_path;
		registration.delete_count = NumericCast<int64_t>(positions.size());
		registration.file_size_bytes = NumericCast<int64_t>(puffin.size());
		// client-side: lets the NEXT statement in this transaction merge
		// and supersede this registration (AddDeletes replaces by
		// data_file_id)
		registration.positions = std::move(positions);
		registrations.push_back(std::move(registration));
	}
	transaction.AddDeletes(ns, table_name, table.GetTableUUID(), std::move(registrations));
	return SinkFinalizeType::READY;
}

SourceResultType HoglakeDelete::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<HoglakeDeleteGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.deleted_count)));
	return SourceResultType::FINISHED;
}

PhysicalOperator &HoglakeDelete::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner,
                                            HoglakeTableEntry &table, PhysicalOperator &child_plan,
                                            vector<idx_t> row_id_indexes, bool wire_child) {
	if (table.IsTravelPinned()) {
		throw BinderException("hoglake: cannot DELETE from a table pinned with AT (VERSION/TIMESTAMP)");
	}
	auto &transaction = HoglakeTransaction::Get(context, table.ParentCatalog());
	transaction.RequireDMLAllowed(table.ParentSchema().name.GetIdentifierName(), table.GetWireInfo().name,
	                              true /* is_delete */);
	vector<LogicalType> return_types;
	return_types.emplace_back(LogicalType::BIGINT);
	auto &delete_op = planner.Make<HoglakeDelete>(return_types, table, std::move(row_id_indexes));
	if (wire_child) {
		delete_op.children.push_back(child_plan);
	}
	return delete_op;
}

} // namespace duckdb
