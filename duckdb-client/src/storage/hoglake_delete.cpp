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

	auto &rowid_vector = chunk.data[row_id_indexes[0]];
	auto &file_name_vector = chunk.data[row_id_indexes[1]];
	auto &file_row_number_vector = chunk.data[row_id_indexes[2]];

	UnifiedVectorFormat rowid_data;
	rowid_vector.ToUnifiedFormat(rowid_data);
	auto rowids = UnifiedVectorFormat::GetData<int64_t>(rowid_data);

	UnifiedVectorFormat file_name_data;
	file_name_vector.ToUnifiedFormat(file_name_data);
	auto file_names = UnifiedVectorFormat::GetData<string_t>(file_name_data);

	UnifiedVectorFormat row_number_data;
	file_row_number_vector.ToUnifiedFormat(row_number_data);
	auto row_numbers = UnifiedVectorFormat::GetData<int64_t>(row_number_data);

	for (idx_t i = 0; i < chunk.size(); i++) {
		auto rid_idx = rowid_data.sel->get_index(i);
		auto name_idx = file_name_data.sel->get_index(i);
		auto row_idx = row_number_data.sel->get_index(i);
		// VALIDITY MASKS on wire/object-store-derived vectors are
		// external input too (a registered parquet can hold a NULL in
		// the column the rowid comes from): refuse typed, naming the
		// table, never an instance-invalidating InternalException
		if (!rowid_data.validity.RowIsValid(rid_idx) || !file_name_data.validity.RowIsValid(name_idx) ||
		    !row_number_data.validity.RowIsValid(row_idx)) {
			auto file_name = file_name_data.validity.RowIsValid(name_idx)
			                     ? file_names[name_idx].GetString()
			                     : string("<unknown>");
			throw InvalidInputException(
			    "hoglake: DELETE/UPDATE on table \"%s.%s\" read a NULL row id from data file \"%s\" - the file's "
			    "row ids are not usable (a registered parquet holding NULLs in the reserved _hog_row_id column, or "
			    "a registration inconsistent with the file). Repair it via another client",
			    table.ParentSchema().name.GetIdentifierName(), table.GetWireInfo().name, file_name);
		}
		auto row_number = row_numbers[row_idx];
		if (row_number < 0) {
			throw InvalidInputException(
			    "hoglake: DELETE/UPDATE on table \"%s.%s\" read a negative row position (%lld) from data file "
			    "\"%s\" - repair the file or its registration via another client",
			    table.ParentSchema().name.GetIdentifierName(), table.GetWireInfo().name, row_number,
			    file_names[name_idx].GetString());
		}
		// the rowid base distinguishes duplicate registrations of one
		// physical path (positional files: base == row_id_start)
		auto base = rowids[rid_idx] - row_number;
		auto inserted = gstate.new_deletes[file_names[name_idx].GetString()][base].insert(
		    NumericCast<idx_t>(row_number));
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
	// the catalog LEGALLY holds multiple live logical files registered
	// for one physical path (writer retries; the server's commit path
	// admits duplicates by design). The scan's filename column cannot
	// distinguish them, and the positions are physical ordinals of the
	// shared bytes — so the superseding DV must be registered for EVERY
	// live data_file_id of the path, or the duplicate logical file's
	// rows silently survive the DELETE.
	map<string, vector<const HoglakeScanFile *>> by_path;
	for (auto &file : scan_files) {
		by_path[file.data_file.path].push_back(&file);
	}

	auto &fs = FileSystem::GetFileSystem(context);
	auto data_path = catalog.DataPath();
	if (!data_path.empty() && data_path.back() != '/') {
		data_path += "/";
	}

	vector<HoglakeDeleteFileRegistration> registrations;
	for (auto &entry : gstate.new_deletes) {
		auto &data_file_path = entry.first;
		auto &base_groups = entry.second;
		auto file_entry = by_path.find(data_file_path);
		if (file_entry == by_path.end()) {
			throw TransactionException("hoglake: DELETE targets data file \"%s\" which is not live at the pinned "
			                           "snapshot (concurrent compaction or drop?) — re-run the DELETE",
			                           data_file_path);
		}
		auto &copies = file_entry->second;

		// attribute each (rowid base -> positions) group to the RIGHT
		// live registration of the path:
		//  - one registration: every group belongs to it (explicit-
		//    row-id files produce varying bases; harmless here)
		//  - several registrations (writer-retry duplicates): only
		//    positional files carry a meaningful base (== their
		//    row_id_start); attribute exactly, refuse anything
		//    ambiguous with a typed error — deleting a position from a
		//    copy whose rows did NOT match the predicate would commit
		//    removal of unmatched rows
		map<int64_t, set<idx_t>> per_file_positions; // data_file_id -> new positions
		if (copies.size() == 1) {
			auto &only = per_file_positions[copies[0]->data_file.data_file_id];
			for (auto &group : base_groups) {
				only.insert(group.second.begin(), group.second.end());
			}
		} else {
			map<int64_t, int64_t> by_start; // row_id_start -> data_file_id
			for (auto copy : copies) {
				auto &data_file = copy->data_file;
				if (data_file.explicit_row_ids) {
					throw InvalidInputException(
					    "hoglake: data file \"%s\" of table \"%s.%s\" has multiple live registrations and at "
					    "least one carries explicit row ids — positions cannot be attributed to a specific "
					    "registration. Repair the duplicate registration via another client",
					    data_file_path, ns, table_name);
				}
				if (!by_start.emplace(data_file.row_id_start, data_file.data_file_id).second) {
					throw InvalidInputException(
					    "hoglake: data file \"%s\" of table \"%s.%s\" has multiple live registrations sharing "
					    "row_id_start %lld — positions cannot be attributed unambiguously. Repair the duplicate "
					    "registration via another client",
					    data_file_path, ns, table_name, data_file.row_id_start);
				}
			}
			for (auto &group : base_groups) {
				auto match = by_start.find(group.first);
				if (match == by_start.end()) {
					throw TransactionException(
					    "hoglake: DELETE positions for data file \"%s\" resolve to rowid base %lld, which matches "
					    "no live registration at the pinned snapshot — re-run the DELETE",
					    data_file_path, group.first);
				}
				per_file_positions[match->second].insert(group.second.begin(), group.second.end());
			}
		}

		for (auto scan_file_ptr : copies) {
			auto &scan_file = *scan_file_ptr;
			auto data_file_id = scan_file.data_file.data_file_id;
			auto attributed = per_file_positions.find(data_file_id);
			if (attributed == per_file_positions.end()) {
				// no matched rows in this logical copy: its DV must not
				// grow (sequential semantics for rowid/snapshot-scoped
				// predicates over duplicate registrations)
				continue;
			}

			// vectors only grow: the superseding DV = (this logical
			// file's server-live DV) ∪ (positions already buffered by
			// EARLIER statements of this transaction for it) ∪ (this
			// statement's attributed positions) — one commit carries
			// exactly one DV per data_file_id, containing everything
			set<idx_t> positions = std::move(attributed->second);
			if (scan_file.has_delete_file) {
				auto existing = HoglakePuffin::ReadDeletionVector(context, scan_file.delete_file.path);
				positions.insert(existing.begin(), existing.end());
			}
			auto buffered = transaction.GetBufferedDeletePositions(ns, table_name, data_file_id);
			positions.insert(buffered.begin(), buffered.end());
			// out-of-range positions mean the CATALOG's metadata is
			// inconsistent (a duplicate registration declaring fewer
			// rows than the physical parquet holds, or another client's
			// committed DV holding positions beyond record_count — the
			// server validates neither). That is the other party's
			// corruption: refuse this statement with a TYPED error
			// naming the file and position, never an instance-
			// invalidating InternalException, and never silently clamp
			// (DESIGN.md, "Write path")
			auto record_count = scan_file.data_file.record_count;
			if (!positions.empty() && NumericCast<int64_t>(*positions.rbegin()) >= record_count) {
				throw InvalidInputException(
				    "hoglake: deleted position %llu is out of range for data file \"%s\" of table \"%s.%s\" "
				    "(registration data_file_id %lld declares %lld rows). The registration or its committed "
				    "deletion vector is inconsistent with the physical file — repair it via another client",
				    *positions.rbegin(), data_file_path, ns, table_name, data_file_id, record_count);
			}

			auto puffin = HoglakePuffin::WritePuffinFile(positions, data_file_path);
			auto dv_path = data_path + "data/" + ns + "/" + table_name + "/hoglake-dv-" +
			               UUID::ToString(UUID::GenerateRandomUUID()) + ".puffin";
			auto handle =
			    fs.OpenFile(dv_path, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
			handle->Write(puffin.data(), puffin.size());
			handle->Close();

			HoglakeDeleteFileRegistration registration;
			registration.data_file_id = data_file_id;
			registration.path = dv_path;
			registration.delete_count = NumericCast<int64_t>(positions.size());
			registration.file_size_bytes = NumericCast<int64_t>(puffin.size());
			// client-side: lets the NEXT statement in this transaction
			// merge and supersede this registration (AddDeletes
			// replaces by data_file_id)
			registration.positions = std::move(positions);
			registrations.push_back(std::move(registration));
		}
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
