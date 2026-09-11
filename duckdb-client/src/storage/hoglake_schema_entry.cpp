#include "storage/hoglake_schema_entry.hpp"

#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include <algorithm>
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "common/hoglake_types.hpp"
#include "storage/hoglake_catalog.hpp"
#include "storage/hoglake_table_entry.hpp"
#include "storage/hoglake_transaction.hpp"

namespace duckdb {

//! Column names with this prefix are reserved for hoglake internals
//! (_hog_row_id is compaction's row-id carrier); the server 422s them.
//! Fast-fail client-side with a better message.
static constexpr const char *RESERVED_COLUMN_PREFIX = "_hog";

HoglakeSchemaEntry::HoglakeSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, HoglakeTransaction &transaction)
    : SchemaCatalogEntry(catalog, info) {
}

HoglakeTransaction &HoglakeSchemaEntry::Transaction(CatalogTransaction &transaction) {
	return transaction.transaction->Cast<HoglakeTransaction>();
}

//===--------------------------------------------------------------------===//
// Table loading
//===--------------------------------------------------------------------===//

CatalogEntry &HoglakeSchemaEntry::CacheTable(HoglakeTransaction &transaction, HoglakeTableInfo table_info) {
	auto columns = table_info.columns;
	std::sort(columns.begin(), columns.end(),
	          [](const HoglakeColumn &a, const HoglakeColumn &b) { return a.ordinal < b.ordinal; });

	auto table_name = table_info.name;
	CreateTableInfo create_info(*this, Identifier(table_name));
	vector<LogicalIndex> not_null;
	for (auto &col : columns) {
		ColumnDefinition column(Identifier(col.name), HoglakeTypes::ToDuckDBType(col));
		if (!col.nullable) {
			not_null.push_back(LogicalIndex(create_info.columns.LogicalColumnCount()));
		}
		create_info.columns.AddColumn(std::move(column));
	}
	for (auto idx : not_null) {
		create_info.constraints.push_back(make_uniq<NotNullConstraint>(idx));
	}
	auto entry = make_uniq<HoglakeTableEntry>(catalog, *this, create_info, std::move(table_info));
	auto &result = *entry;
	tables[table_name] = std::move(entry);
	return result;
}

optional_ptr<CatalogEntry> HoglakeSchemaEntry::LookupTable(HoglakeTransaction &transaction, const string &entry_name) {
	auto existing = tables.find(entry_name);
	if (existing != tables.end()) {
		return existing->second.get();
	}
	if (all_tables_loaded) {
		return nullptr;
	}
	auto ns = name.GetIdentifierName();
	auto table_info = transaction.Api().TryGetTable(ns, entry_name, transaction.Travel());
	if (!table_info) {
		return nullptr;
	}
	return &CacheTable(transaction, std::move(*table_info));
}

void HoglakeSchemaEntry::LoadAllTables(HoglakeTransaction &transaction) {
	if (all_tables_loaded) {
		return;
	}
	auto ns = name.GetIdentifierName();
	// NOTE (wire): the table listing is head-only; per-table fetches are
	// at the pinned snapshot (DESIGN.md server findings).
	auto summaries = transaction.Api().ListTables(ns);
	for (auto &summary : summaries) {
		if (tables.find(summary.name) != tables.end()) {
			continue;
		}
		auto table_info = transaction.Api().TryGetTable(ns, summary.name, transaction.Travel());
		if (!table_info) {
			// listed at head but missing at the pinned snapshot
			continue;
		}
		CacheTable(transaction, std::move(*table_info));
	}
	all_tables_loaded = true;
}

//===--------------------------------------------------------------------===//
// Lookup / scan
//===--------------------------------------------------------------------===//

optional_ptr<CatalogEntry> HoglakeSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                           const EntryLookupInfo &lookup_info) {
	auto catalog_type = lookup_info.GetCatalogType();
	if (catalog_type != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}
	auto at_clause = lookup_info.GetAtClause();
	if (at_clause) {
		auto &hoglake_transaction = Transaction(transaction);
		auto travel = hoglake_transaction.TravelFor(at_clause);
		return LookupTableAt(hoglake_transaction, lookup_info.GetEntryName(), travel);
	}
	return LookupTable(Transaction(transaction), lookup_info.GetEntryName());
}

optional_ptr<CatalogEntry> HoglakeSchemaEntry::LookupTableAt(HoglakeTransaction &transaction,
                                                             const string &entry_name, const HoglakeTravel &travel) {
	auto key = entry_name + "@" + travel.CacheKey();
	auto existing = travel_tables.find(key);
	if (existing != travel_tables.end()) {
		return existing->second.get();
	}
	auto ns = name.GetIdentifierName();
	// a 410 (below the expiry floor) surfaces as InvalidInputException
	auto table_info = transaction.Api().TryGetTable(ns, entry_name, travel);
	if (!table_info) {
		return nullptr;
	}
	// build the entry at the historical schema; reads plan at `travel`
	// and writes are refused
	auto columns = table_info->columns;
	std::sort(columns.begin(), columns.end(),
	          [](const HoglakeColumn &a, const HoglakeColumn &b) { return a.ordinal < b.ordinal; });
	auto table_name = table_info->name;
	CreateTableInfo create_info(*this, Identifier(table_name));
	vector<LogicalIndex> not_null;
	for (auto &col : columns) {
		ColumnDefinition column(Identifier(col.name), HoglakeTypes::ToDuckDBType(col));
		if (!col.nullable) {
			not_null.push_back(LogicalIndex(create_info.columns.LogicalColumnCount()));
		}
		create_info.columns.AddColumn(std::move(column));
	}
	for (auto idx : not_null) {
		create_info.constraints.push_back(make_uniq<NotNullConstraint>(idx));
	}
	auto entry = make_uniq<HoglakeTableEntry>(catalog, *this, create_info, std::move(*table_info), travel, true);
	auto &result = *entry;
	travel_tables[key] = std::move(entry);
	return &result;
}

void HoglakeSchemaEntry::Scan(ClientContext &context, CatalogType type,
                              const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	auto &transaction = HoglakeTransaction::Get(context, catalog);
	LoadAllTables(transaction);
	for (auto &entry : tables) {
		if (entry.second) {
			callback(*entry.second);
		}
	}
}

void HoglakeSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	// context-free scan: only what is already cached
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	for (auto &entry : tables) {
		if (entry.second) {
			callback(*entry.second);
		}
	}
}

//===--------------------------------------------------------------------===//
// DDL
//===--------------------------------------------------------------------===//

optional_ptr<CatalogEntry> HoglakeSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                           BoundCreateTableInfo &info) {
	auto &hoglake_transaction = Transaction(transaction);
	auto &base_info = info.Base();
	auto table_name = base_info.GetTableName().GetIdentifierName();

	auto existing = LookupTable(hoglake_transaction, table_name);
	if (existing) {
		if (base_info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			return nullptr;
		}
		if (base_info.on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw CatalogException("hoglake: table \"%s.%s\" already exists", name.GetIdentifierName(), table_name);
		}
		throw NotImplementedException("CREATE OR REPLACE TABLE is not supported for hoglake");
	}

	// collect NOT NULL columns
	unordered_set<idx_t> not_null;
	for (auto &constraint : base_info.constraints) {
		if (constraint->type == ConstraintType::NOT_NULL) {
			auto &nn = constraint->Cast<NotNullConstraint>();
			not_null.insert(nn.index.index);
		} else {
			throw NotImplementedException("hoglake tables only support NOT NULL constraints");
		}
	}

	vector<HoglakeColumnDef> defs;
	for (auto &col : base_info.columns.Logical()) {
		auto col_name = col.Name().GetIdentifierName();
		if (StringUtil::StartsWith(col_name, RESERVED_COLUMN_PREFIX)) {
			throw InvalidInputException("hoglake: column name \"%s\" uses the reserved \"%s\" prefix "
			                            "(hoglake internal columns, e.g. _hog_row_id)",
			                            col_name, RESERVED_COLUMN_PREFIX);
		}
		auto nullable = not_null.find(col.Logical().index) == not_null.end();
		defs.push_back(HoglakeTypes::FromDuckDBType(col_name, col.Type(), nullable));
	}

	// eager DDL: the server commit happens NOW (its own snapshot) and
	// survives a rollback of the surrounding transaction (DESIGN.md)
	auto created = hoglake_transaction.Api().CreateTable(name.GetIdentifierName(), table_name, defs);
	return &CacheTable(hoglake_transaction, std::move(created));
}

void HoglakeSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	if (info.cascade) {
		throw NotImplementedException("DROP ... CASCADE is not supported for hoglake");
	}
	auto entry_name = info.GetQualifiedName().Name().GetIdentifierName();
	auto &transaction = HoglakeTransaction::Get(context, catalog);
	switch (info.type) {
	case CatalogType::TABLE_ENTRY: {
		auto entry = LookupTable(transaction, entry_name);
		if (!entry) {
			if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
				return;
			}
			throw CatalogException("hoglake: table \"%s.%s\" does not exist", name.GetIdentifierName(), entry_name);
		}
		transaction.Api().DropTable(name.GetIdentifierName(), entry_name);
		tables.erase(entry_name);
		return;
	}
	default:
		throw NotImplementedException("hoglake: DROP of this entry type is not supported yet");
	}
}

//===--------------------------------------------------------------------===//
// ALTER
//===--------------------------------------------------------------------===//

static int64_t FindFieldId(const HoglakeTableInfo &wire, const string &column_name) {
	for (auto &col : wire.columns) {
		if (StringUtil::CIEquals(col.name, column_name)) {
			return col.field_id;
		}
	}
	throw BinderException("hoglake: column \"%s\" does not exist in table \"%s\"", column_name, wire.name);
}

//! Parse a partition-key expression into a wire PartitionField:
//! a bare column ref = identity; year/month/day/hour(col) and
//! bucket(col, n) map to the transform vocabulary.
static HoglakePartitionField ParsePartitionExpression(const HoglakeTableInfo &wire, ParsedExpression &expr) {
	HoglakePartitionField field;
	if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &col_ref = expr.Cast<ColumnRefExpression>();
		field.source_field_id = FindFieldId(wire, col_ref.GetColumnName().GetIdentifierName());
		field.transform = "identity";
		return field;
	}
	if (expr.GetExpressionClass() == ExpressionClass::FUNCTION) {
		auto &function = expr.Cast<FunctionExpression>();
		auto function_name = StringUtil::Lower(function.FunctionName().GetIdentifierName());
		auto &args = function.GetArguments();
		if (function_name == "year" || function_name == "month" || function_name == "day" ||
		    function_name == "hour") {
			if (args.size() != 1 || args[0].GetExpression().GetExpressionClass() != ExpressionClass::COLUMN_REF) {
				throw BinderException("hoglake: %s(...) partition transform takes exactly one column", function_name);
			}
			auto &col_ref = args[0].GetExpression().Cast<ColumnRefExpression>();
			field.source_field_id = FindFieldId(wire, col_ref.GetColumnName().GetIdentifierName());
			field.transform = function_name;
			return field;
		}
		if (function_name == "bucket") {
			if (args.size() != 2 || args[0].GetExpression().GetExpressionClass() != ExpressionClass::CONSTANT ||
			    args[1].GetExpression().GetExpressionClass() != ExpressionClass::COLUMN_REF) {
				throw BinderException("hoglake: bucket partitioning is bucket(<n>, <column>)");
			}
			auto &n = args[0].GetExpression().Cast<ConstantExpression>();
			auto &col_ref = args[1].GetExpression().Cast<ColumnRefExpression>();
			field.source_field_id = FindFieldId(wire, col_ref.GetColumnName().GetIdentifierName());
			field.transform = "bucket";
			field.transform_param =
			    NumericCast<int32_t>(BigIntValue::Get(n.GetValue().DefaultCastAs(LogicalType::BIGINT)));
			return field;
		}
	}
	throw BinderException("hoglake: unsupported partition expression \"%s\" (supported: <column>, "
	                      "year/month/day/hour(<column>), bucket(<n>, <column>))",
	                      expr.ToString());
}

void HoglakeSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	if (info.type != AlterType::ALTER_TABLE) {
		throw NotImplementedException("hoglake: only ALTER TABLE is supported");
	}
	auto &hoglake_transaction = Transaction(transaction);
	auto &alter_table = info.Cast<AlterTableInfo>();
	auto table_name = alter_table.GetQualifiedName().Name().GetIdentifierName();
	auto entry = LookupTable(hoglake_transaction, table_name);
	if (!entry) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw CatalogException("hoglake: table \"%s.%s\" does not exist", name.GetIdentifierName(), table_name);
	}
	auto &table = entry->Cast<HoglakeTableEntry>();
	auto &wire = table.GetWireInfo();

	HoglakeAlterOp op;
	(void)table;
	string new_entry_name = table_name;
	switch (alter_table.alter_table_type) {
	case AlterTableType::RENAME_TABLE: {
		auto &rename = alter_table.Cast<RenameTableInfo>();
		op.op = "rename_table";
		op.new_name = rename.new_table_name.GetIdentifierName();
		new_entry_name = op.new_name;
		break;
	}
	case AlterTableType::RENAME_COLUMN: {
		auto &rename = alter_table.Cast<RenameColumnInfo>();
		op.op = "rename_column";
		op.from = rename.old_name.GetIdentifierName();
		op.to = rename.new_name.GetIdentifierName();
		break;
	}
	case AlterTableType::ADD_COLUMN: {
		auto &add = alter_table.Cast<AddColumnInfo>();
		auto &col = add.new_column;
		if (add.if_column_not_exists && table.ColumnExists(col.Name())) {
			return;
		}
		if (col.HasDefaultValue()) {
			throw NotImplementedException("hoglake: ADD COLUMN with DEFAULT is not supported (the wire contract "
			                              "has no column defaults; added columns read as NULL)");
		}
		op.op = "add_column";
		op.column = HoglakeTypes::FromDuckDBType(col.Name().GetIdentifierName(), col.Type(), true);
		break;
	}
	case AlterTableType::REMOVE_COLUMN: {
		auto &remove = alter_table.Cast<RemoveColumnInfo>();
		if (remove.if_column_exists && !table.ColumnExists(remove.removed_column)) {
			return;
		}
		op.op = "drop_column";
		op.name = remove.removed_column.GetIdentifierName();
		break;
	}
	case AlterTableType::ALTER_COLUMN_TYPE: {
		auto &change = alter_table.Cast<ChangeColumnTypeInfo>();
		op.op = "promote_column";
		op.name = change.column_name.GetIdentifierName();
		// the server's promotion lattice validates the target
		op.to = HoglakeTypes::FromDuckDBType(op.name, change.target_type, true).type;
		break;
	}
	case AlterTableType::SET_PARTITIONED_BY: {
		auto &set_partitioned = alter_table.Cast<SetPartitionedByInfo>();
		op.op = "set_partition_spec";
		for (auto &expr : set_partitioned.partition_keys) {
			op.fields.push_back(ParsePartitionExpression(wire, *expr));
		}
		break;
	}
	case AlterTableType::SET_SORTED_BY: {
		auto &set_sorted = alter_table.Cast<SetSortedByInfo>();
		op.op = "set_sort_order";
		for (auto &order : set_sorted.orders) {
			if (order.expression->GetExpressionClass() != ExpressionClass::COLUMN_REF) {
				throw BinderException("hoglake: SET SORTED BY supports plain columns only");
			}
			auto &col_ref = order.expression->Cast<ColumnRefExpression>();
			HoglakeSortField sort_field;
			sort_field.source_field_id = FindFieldId(wire, col_ref.GetColumnName().GetIdentifierName());
			sort_field.direction = order.type == OrderType::DESCENDING ? "desc" : "asc";
			sort_field.null_order = order.null_order == OrderByNullType::NULLS_FIRST ? "nulls_first" : "nulls_last";
			op.sort_fields.push_back(sort_field);
		}
		break;
	}
	default:
		throw NotImplementedException("hoglake: this ALTER TABLE operation is not supported by the wire contract "
		                              "(supported: RENAME TABLE/COLUMN, ADD/DROP COLUMN, ALTER COLUMN TYPE "
		                              "promotions, SET PARTITIONED BY, SET SORTED BY)");
	}

	// eager DDL: one atomic /alter commit (its own server snapshot)
	auto ns = name.GetIdentifierName();
	auto altered = hoglake_transaction.Api().AlterTable(ns, table_name, {op});

	// swap the cached entry for the evolved table; the old entry stays
	// alive (retired) because the statement may still reference it
	auto existing = tables.find(table_name);
	if (existing != tables.end()) {
		retired.push_back(std::move(existing->second));
		tables.erase(existing);
	}
	CacheTable(hoglake_transaction, std::move(altered));
	if (!StringUtil::CIEquals(new_entry_name, table_name)) {
		// rename: the response carries the new name; drop the old key
		tables.erase(table_name);
	}
}

//===--------------------------------------------------------------------===//
// Unsupported entry types
//===--------------------------------------------------------------------===//

optional_ptr<CatalogEntry> HoglakeSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw NotImplementedException("CREATE VIEW is not supported for hoglake yet (lands in M5)");
}

optional_ptr<CatalogEntry> HoglakeSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                              CreateFunctionInfo &info) {
	throw NotImplementedException("hoglake does not support CREATE FUNCTION");
}

optional_ptr<CatalogEntry> HoglakeSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                           TableCatalogEntry &table) {
	throw NotImplementedException("hoglake does not support indexes");
}

optional_ptr<CatalogEntry> HoglakeSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                              CreateSequenceInfo &info) {
	throw NotImplementedException("hoglake does not support sequences");
}

optional_ptr<CatalogEntry> HoglakeSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                   CreateTableFunctionInfo &info) {
	throw NotImplementedException("hoglake does not support table functions");
}

optional_ptr<CatalogEntry> HoglakeSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                  CreateCopyFunctionInfo &info) {
	throw NotImplementedException("hoglake does not support copy functions");
}

optional_ptr<CatalogEntry> HoglakeSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                    CreatePragmaFunctionInfo &info) {
	throw NotImplementedException("hoglake does not support pragma functions");
}

optional_ptr<CatalogEntry> HoglakeSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                               CreateCollationInfo &info) {
	throw NotImplementedException("hoglake does not support collations");
}

optional_ptr<CatalogEntry> HoglakeSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw NotImplementedException("hoglake does not support user-defined types");
}

} // namespace duckdb
