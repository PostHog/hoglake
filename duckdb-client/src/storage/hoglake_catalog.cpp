#include "storage/hoglake_catalog.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_create_table.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "storage/hoglake_delete.hpp"
#include "storage/hoglake_insert.hpp"
#include "storage/hoglake_table_entry.hpp"
#include "storage/hoglake_update.hpp"
#include "storage/hoglake_schema_entry.hpp"
#include "storage/hoglake_transaction.hpp"

namespace duckdb {

HoglakeCatalog::HoglakeCatalog(AttachedDatabase &db_p, HoglakeOptions options_p)
    : Catalog(db_p), options(std::move(options_p)) {
	api_client = make_uniq<HoglakeApiClient>(options.endpoint, options.catalog_name);
}

HoglakeCatalog::~HoglakeCatalog() {
}

void HoglakeCatalog::Initialize(bool load_builtin) {
	// resolve the catalog on the server; optionally create it
	auto info = api_client->TryGetCatalog();
	if (!info) {
		if (!options.create_if_not_exists) {
			throw CatalogException("hoglake: catalog \"%s\" does not exist at %s (pass CREATE_IF_NOT_EXISTS true "
			                       "with a DATA_PATH to create it)",
			                       options.catalog_name, options.endpoint);
		}
		if (options.data_path.empty()) {
			throw InvalidInputException(
			    "hoglake: CREATE_IF_NOT_EXISTS requires DATA_PATH (an s3:// URI the catalog owns)");
		}
		auto created = api_client->CreateCatalog(options.data_path);
		data_path = created.data_path;
		return;
	}
	data_path = info->data_path;
}

optional_ptr<CatalogEntry> HoglakeCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	auto &hoglake_transaction = transaction.transaction->Cast<HoglakeTransaction>();
	auto schema_name = info.SchemaName().GetIdentifierName();
	if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT ||
	    info.on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
		auto existing = hoglake_transaction.GetSchema(schema_name);
		if (existing) {
			if (info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
				return existing.get();
			}
			throw CatalogException("hoglake: namespace \"%s\" already exists", schema_name);
		}
	} else if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		throw NotImplementedException("CREATE OR REPLACE SCHEMA is not supported for hoglake (no namespace drop on "
		                              "the wire contract)");
	}
	// eager DDL: the server commit happens NOW and survives a rollback
	// of the surrounding transaction (documented divergence, DESIGN.md)
	api_client->CreateNamespace(schema_name);
	auto entry = make_uniq<HoglakeSchemaEntry>(*this, info, hoglake_transaction);
	return &hoglake_transaction.AddSchema(std::move(entry));
}

void HoglakeCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	throw NotImplementedException(
	    "DROP SCHEMA is not supported for hoglake: the wire contract has no namespace-drop operation");
}

void HoglakeCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	auto &transaction = HoglakeTransaction::Get(context, *this);
	transaction.ScanSchemas([&](HoglakeSchemaEntry &entry) { callback(entry); });
}

optional_ptr<SchemaCatalogEntry> HoglakeCatalog::LookupSchema(CatalogTransaction transaction,
                                                              const EntryLookupInfo &schema_lookup,
                                                              OnEntryNotFound if_not_found) {
	auto &schema_name = schema_lookup.GetEntryName();
	if (schema_lookup.GetAtClause()) {
		throw NotImplementedException("hoglake: per-lookup AT (VERSION/TIMESTAMP) is not supported yet; "
		                              "attach with SNAPSHOT_VERSION / SNAPSHOT_TIME instead");
	}
	auto &hoglake_transaction = transaction.transaction->Cast<HoglakeTransaction>();
	auto entry = hoglake_transaction.GetSchema(schema_name);
	if (!entry) {
		if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
			throw CatalogException("hoglake: namespace \"%s\" not found in catalog \"%s\"", schema_name,
			                       options.catalog_name);
		}
		return nullptr;
	}
	return entry.get();
}

PhysicalOperator &HoglakeCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
                                                    LogicalCreateTable &op, PhysicalOperator &plan) {
	// eager DDL: the table is created NOW (its own server snapshot, not
	// undone by rollback; the inserted rows still commit atomically at
	// COMMIT). Caveat: re-executing a cached plan (prepared statement)
	// re-runs the create and errors on the duplicate.
	auto &schema = op.schema.Cast<HoglakeSchemaEntry>();
	auto transaction = GetCatalogTransaction(context);
	auto entry = schema.CreateTable(transaction, *op.info);
	if (!entry) {
		throw CatalogException("hoglake: CREATE TABLE AS failed to create the table");
	}
	auto &table = entry->Cast<HoglakeTableEntry>();
	return HoglakeInsert::PlanInsert(context, planner, table, &plan);
}

PhysicalOperator &HoglakeCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
                                             optional_ptr<PhysicalOperator> plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for insertion into hoglake tables");
	}
	if (op.on_conflict_info.action_type != OnConflictAction::THROW) {
		throw BinderException("ON CONFLICT clause not supported for insertion into hoglake tables");
	}
	if (!op.column_index_map.empty()) {
		plan = planner.ResolveDefaultsProjection(op, *plan);
	}
	auto &table = op.table.Cast<HoglakeTableEntry>();
	return HoglakeInsert::PlanInsert(context, planner, table, plan);
}

PhysicalOperator &HoglakeCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
                                             PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for deletion from hoglake tables");
	}
	// op.expressions = [rowid, filename, file_index, file_row_number]
	// (GetRowIdColumns order); the delete sink needs the last three
	vector<idx_t> row_id_indexes;
	for (idx_t i = 0; i < 3; i++) {
		auto &bound_ref = op.expressions[i + 1]->Cast<BoundReferenceExpression>();
		row_id_indexes.push_back(bound_ref.Index());
	}
	return HoglakeDelete::PlanDelete(context, planner, op.table.Cast<HoglakeTableEntry>(), plan,
	                                 std::move(row_id_indexes), true);
}

PhysicalOperator &HoglakeCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                             PhysicalOperator &plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for updates of hoglake tables");
	}
	for (auto &expr : op.expressions) {
		if (expr->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
			throw BinderException("SET DEFAULT is not supported for updates of hoglake tables");
		}
	}
	auto &table = op.table.Cast<HoglakeTableEntry>();

	// embedded delete sink over the last 3 input columns of the delete
	// chunk the update operator builds
	vector<idx_t> row_id_indexes {0, 1, 2};
	auto &delete_op = HoglakeDelete::PlanDelete(context, planner, table, plan, std::move(row_id_indexes), false);

	// update expressions ordered by physical column index
	vector<unique_ptr<Expression>> expressions;
	unordered_map<idx_t, idx_t> expression_map;
	for (idx_t i = 0; i < op.columns.size(); i++) {
		expression_map[op.columns[i].index] = i;
	}
	for (idx_t i = 0; i < op.columns.size(); i++) {
		expressions.push_back(op.expressions[expression_map[i]]->Copy());
	}

	auto &update_op =
	    planner.Make<HoglakeUpdate>(table, op.columns, plan, delete_op, expressions).Cast<HoglakeUpdate>();
	vector<LogicalType> update_output_types;
	for (auto &expr : update_op.expressions) {
		update_output_types.push_back(expr->GetReturnType());
	}
	update_op.types = std::move(update_output_types);

	// updated rows flow into the ordinary insert path (copy + register)
	return HoglakeInsert::PlanInsert(context, planner, table, &update_op);
}

DatabaseSize HoglakeCatalog::GetDatabaseSize(ClientContext &context) {
	DatabaseSize size;
	size.free_blocks = 0;
	size.total_blocks = 0;
	size.used_blocks = 0;
	size.wal_size = 0;
	size.block_size = 0;
	size.bytes = 0;
	return size;
}

bool HoglakeCatalog::InMemory() {
	return false;
}

string HoglakeCatalog::GetDBPath() {
	return options.endpoint + "/" + options.catalog_name;
}

} // namespace duckdb
