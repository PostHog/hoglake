//===----------------------------------------------------------------------===//
// Metadata / maintenance table functions. All take the attached
// hoglake catalog name as a VARCHAR first argument (ducklake's
// convention for its metadata functions).
//===----------------------------------------------------------------------===//

#include "functions/hoglake_metadata_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/function/table_function.hpp"
#include "storage/hoglake_catalog.hpp"
#include "storage/hoglake_transaction.hpp"

#include "yyjson.hpp"

namespace duckdb {

using namespace duckdb_yyjson; // NOLINT

static HoglakeCatalog &GetHoglakeCatalog(ClientContext &context, const Value &input) {
	if (input.IsNull()) {
		throw BinderException("hoglake function requires an attached catalog name");
	}
	auto catalog_name = StringValue::Get(input);
	auto &catalog = Catalog::GetCatalog(context, Identifier(catalog_name));
	if (catalog.GetCatalogType() != "hoglake") {
		throw BinderException("Catalog \"%s\" is not a hoglake catalog", catalog_name);
	}
	return catalog.Cast<HoglakeCatalog>();
}

//===--------------------------------------------------------------------===//
// hoglake_snapshots(catalog)
//===--------------------------------------------------------------------===//

struct HoglakeSnapshotsData : public TableFunctionData {
	explicit HoglakeSnapshotsData(HoglakeCatalog &catalog) : catalog(catalog) {
	}
	HoglakeCatalog &catalog;
	//! page size for /snapshots (default 1000; settable for tests to
	//! force page-boundary crossings)
	idx_t page_size = 1000;
};

struct HoglakeSnapshotsState : public GlobalTableFunctionState {
	vector<HoglakeSnapshotInfo> buffered;
	idx_t buffer_offset = 0;
	idx_t after = 0;
	bool exhausted = false;
};

static unique_ptr<FunctionData> SnapshotsBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &catalog = GetHoglakeCatalog(context, input.inputs[0]);
	auto result = make_uniq<HoglakeSnapshotsData>(catalog);
	auto page_size_entry = input.named_parameters.find("page_size");
	if (page_size_entry != input.named_parameters.end() && !page_size_entry->second.IsNull()) {
		auto page_size = BigIntValue::Get(page_size_entry->second.DefaultCastAs(LogicalType::BIGINT));
		if (page_size < 1 || page_size > 10000) {
			throw BinderException("hoglake_snapshots: page_size must be in [1, 10000]");
		}
		result->page_size = NumericCast<idx_t>(page_size);
	}
	names = StringsToIdentifiers({"snapshot_id", "snapshot_time", "schema_version", "author", "message", "changes"});
	return_types = {LogicalType::BIGINT,
	                LogicalType::TIMESTAMP_TZ,
	                LogicalType::BIGINT,
	                LogicalType::VARCHAR,
	                LogicalType::VARCHAR,
	                LogicalType::LIST(LogicalType::STRUCT(
	                    {{"kind", LogicalType::VARCHAR}, {"object_id", LogicalType::BIGINT}}))};
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> SnapshotsInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<HoglakeSnapshotsState>();
}

static void SnapshotsExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<HoglakeSnapshotsData>();
	auto &state = data.global_state->Cast<HoglakeSnapshotsState>();

	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE) {
		if (state.buffer_offset >= state.buffered.size()) {
			if (state.exhausted) {
				break;
			}
			auto page = bind_data.catalog.Api().ListSnapshots(state.after, bind_data.page_size);
			state.buffered = std::move(page.snapshots);
			state.buffer_offset = 0;
			if (!state.buffered.empty()) {
				state.after = NumericCast<idx_t>(state.buffered.back().snapshot_id);
			}
			if (!page.has_more || state.buffered.empty()) {
				state.exhausted = true;
			}
			if (state.buffered.empty()) {
				break;
			}
		}
		auto &snap = state.buffered[state.buffer_offset++];
		output.SetValue(0, count, Value::BIGINT(snap.snapshot_id));
		output.SetValue(1, count,
		                snap.snapshot_time.empty()
		                    ? Value(LogicalType::TIMESTAMP_TZ)
		                    : Value(snap.snapshot_time).DefaultCastAs(LogicalType::TIMESTAMP_TZ));
		output.SetValue(2, count, Value::BIGINT(snap.schema_version));
		output.SetValue(3, count, snap.author.empty() ? Value(LogicalType::VARCHAR) : Value(snap.author));
		output.SetValue(4, count, snap.message.empty() ? Value(LogicalType::VARCHAR) : Value(snap.message));
		vector<Value> changes;
		for (auto &change : snap.changes) {
			child_list_t<Value> struct_values;
			struct_values.emplace_back("kind", Value(change.kind));
			struct_values.emplace_back("object_id", Value::BIGINT(change.object_id));
			changes.push_back(Value::STRUCT(std::move(struct_values)));
		}
		output.SetValue(5, count,
		                Value::LIST(LogicalType::STRUCT(
		                                {{"kind", LogicalType::VARCHAR}, {"object_id", LogicalType::BIGINT}}),
		                            std::move(changes)));
		count++;
	}
	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// hoglake_table_info(catalog)
//===--------------------------------------------------------------------===//

struct HoglakeTableInfoRow {
	string namespace_name;
	string table_name;
	string table_uuid;
	int64_t record_count;
	int64_t file_count;
	int64_t file_size_bytes;
};

struct HoglakeTableInfoData : public TableFunctionData {
	explicit HoglakeTableInfoData(HoglakeCatalog &catalog) : catalog(catalog) {
	}
	HoglakeCatalog &catalog;
};

struct HoglakeTableInfoState : public GlobalTableFunctionState {
	vector<HoglakeTableInfoRow> rows;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> TableInfoBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &catalog = GetHoglakeCatalog(context, input.inputs[0]);
	names = StringsToIdentifiers(
	    {"namespace", "table_name", "table_uuid", "record_count", "file_count", "file_size_bytes"});
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::UUID,
	                LogicalType::BIGINT,  LogicalType::BIGINT,  LogicalType::BIGINT};
	return make_uniq<HoglakeTableInfoData>(catalog);
}

static unique_ptr<GlobalTableFunctionState> TableInfoInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<HoglakeTableInfoData>();
	auto &catalog = bind_data.catalog;
	auto &transaction = HoglakeTransaction::Get(context, catalog);
	auto result = make_uniq<HoglakeTableInfoState>();
	for (auto &ns : catalog.Api().ListNamespaces()) {
		for (auto &summary : catalog.Api().ListTables(ns)) {
			auto table = catalog.Api().TryGetTable(ns, summary.name, transaction.Travel());
			if (!table) {
				continue;
			}
			HoglakeTableInfoRow row;
			row.namespace_name = ns;
			row.table_name = table->name;
			row.table_uuid = table->table_uuid;
			row.record_count = table->record_count;
			row.file_count = table->file_count;
			row.file_size_bytes = table->file_size_bytes;
			result->rows.push_back(std::move(row));
		}
	}
	return std::move(result);
}

static void TableInfoExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<HoglakeTableInfoState>();
	idx_t count = 0;
	while (count < STANDARD_VECTOR_SIZE && state.offset < state.rows.size()) {
		auto &row = state.rows[state.offset++];
		output.SetValue(0, count, Value(row.namespace_name));
		output.SetValue(1, count, Value(row.table_name));
		output.SetValue(2, count, Value(row.table_uuid).DefaultCastAs(LogicalType::UUID));
		output.SetValue(3, count, Value::BIGINT(row.record_count));
		output.SetValue(4, count, Value::BIGINT(row.file_count));
		output.SetValue(5, count, Value::BIGINT(row.file_size_bytes));
		count++;
	}
	output.SetCardinality(count);
}

//===--------------------------------------------------------------------===//
// hoglake_current_snapshot(catalog)
//===--------------------------------------------------------------------===//

struct HoglakeCurrentSnapshotData : public TableFunctionData {
	explicit HoglakeCurrentSnapshotData(HoglakeCatalog &catalog) : catalog(catalog) {
	}
	HoglakeCatalog &catalog;
};

struct HoglakeOneRowState : public GlobalTableFunctionState {
	bool done = false;
	//! the JSON payload / snapshot id produced at INIT time
	string result_json;
	idx_t snapshot_id = 0;
};

static unique_ptr<FunctionData> CurrentSnapshotBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &catalog = GetHoglakeCatalog(context, input.inputs[0]);
	names = StringsToIdentifiers({"id"});
	return_types = {LogicalType::UBIGINT};
	return make_uniq<HoglakeCurrentSnapshotData>(catalog);
}

static unique_ptr<GlobalTableFunctionState> CurrentSnapshotInit(ClientContext &context,
                                                                TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<HoglakeCurrentSnapshotData>();
	auto result = make_uniq<HoglakeOneRowState>();
	result->snapshot_id = HoglakeTransaction::Get(context, bind_data.catalog).GetSnapshot();
	return std::move(result);
}

static void CurrentSnapshotExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<HoglakeOneRowState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;
	output.SetValue(0, 0, Value::UBIGINT(state.snapshot_id));
	output.SetCardinality(1);
}

//===--------------------------------------------------------------------===//
// hoglake_expire / hoglake_compact / hoglake_cleanup (catalog [, batch])
// server-side maintenance passthroughs; result = the raw JSON report
//===--------------------------------------------------------------------===//

struct HoglakeMaintenanceData : public TableFunctionData {
	HoglakeMaintenanceData(HoglakeCatalog &catalog, const char *verb) : catalog(catalog), verb(verb) {
	}
	HoglakeCatalog &catalog;
	const char *verb;
	optional_idx batch;
};

static unique_ptr<FunctionData> MaintenanceBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<Identifier> &names,
                                                const char *verb) {
	auto &catalog = GetHoglakeCatalog(context, input.inputs[0]);
	auto result = make_uniq<HoglakeMaintenanceData>(catalog, verb);
	auto entry = input.named_parameters.find("batch");
	if (entry != input.named_parameters.end() && !entry->second.IsNull()) {
		result->batch = NumericCast<idx_t>(BigIntValue::Get(entry->second.DefaultCastAs(LogicalType::BIGINT)));
	}
	names = StringsToIdentifiers({"result"});
	return_types = {LogicalType::JSON()};
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> MaintenanceInit(ClientContext &context, TableFunctionInitInput &input) {
	// the server-side mutation runs HERE (execution), never at bind:
	// EXPLAIN / prepare of a maintenance function must not mutate the
	// catalog
	auto &bind_data = input.bind_data->Cast<HoglakeMaintenanceData>();
	auto result = make_uniq<HoglakeOneRowState>();
	result->result_json = bind_data.catalog.Api().RunMaintenance(bind_data.verb, bind_data.batch);
	return std::move(result);
}

static void MaintenanceExecute(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<HoglakeOneRowState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	state.done = true;
	output.SetValue(0, 0, Value(state.result_json).DefaultCastAs(LogicalType::JSON()));
	output.SetCardinality(1);
}

template <const char *VERB>
static unique_ptr<FunctionData> MaintenanceBindFor(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
	return MaintenanceBind(context, input, return_types, names, VERB);
}

static constexpr const char EXPIRE_VERB[] = "expire";
static constexpr const char COMPACT_VERB[] = "compact";
static constexpr const char CLEANUP_VERB[] = "cleanup";
static constexpr const char VERIFY_VERB[] = "verify";

//===--------------------------------------------------------------------===//
// Registration
//===--------------------------------------------------------------------===//

void HoglakeMetadataFunctions::Register(ExtensionLoader &loader) {
	TableFunction snapshots("hoglake_snapshots", {LogicalType::VARCHAR}, SnapshotsExecute, SnapshotsBind,
	                        SnapshotsInit);
	snapshots.named_parameters["page_size"] = LogicalType::BIGINT;
	loader.RegisterFunction(snapshots);

	TableFunction table_info("hoglake_table_info", {LogicalType::VARCHAR}, TableInfoExecute, TableInfoBind,
	                         TableInfoInit);
	loader.RegisterFunction(table_info);

	TableFunction current_snapshot("hoglake_current_snapshot", {LogicalType::VARCHAR}, CurrentSnapshotExecute,
	                               CurrentSnapshotBind, CurrentSnapshotInit);
	loader.RegisterFunction(current_snapshot);

	TableFunction expire("hoglake_expire", {LogicalType::VARCHAR}, MaintenanceExecute,
	                     MaintenanceBindFor<EXPIRE_VERB>, MaintenanceInit);
	expire.named_parameters["batch"] = LogicalType::BIGINT;
	loader.RegisterFunction(expire);

	TableFunction compact("hoglake_compact", {LogicalType::VARCHAR}, MaintenanceExecute,
	                      MaintenanceBindFor<COMPACT_VERB>, MaintenanceInit);
	compact.named_parameters["batch"] = LogicalType::BIGINT;
	loader.RegisterFunction(compact);

	TableFunction cleanup("hoglake_cleanup", {LogicalType::VARCHAR}, MaintenanceExecute,
	                      MaintenanceBindFor<CLEANUP_VERB>, MaintenanceInit);
	cleanup.named_parameters["batch"] = LogicalType::BIGINT;
	loader.RegisterFunction(cleanup);

	TableFunction verify("hoglake_verify", {LogicalType::VARCHAR}, MaintenanceExecute, MaintenanceBindFor<VERIFY_VERB>,
	                     MaintenanceInit);
	loader.RegisterFunction(verify);
}

} // namespace duckdb
