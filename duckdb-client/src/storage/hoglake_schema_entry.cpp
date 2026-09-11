#include "storage/hoglake_schema_entry.hpp"

#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
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
	if (lookup_info.GetAtClause()) {
		throw NotImplementedException("hoglake: per-lookup AT (VERSION/TIMESTAMP) is not supported yet; "
		                              "attach with SNAPSHOT_VERSION / SNAPSHOT_TIME instead");
	}
	return LookupTable(Transaction(transaction), lookup_info.GetEntryName());
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

void HoglakeSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	throw NotImplementedException("ALTER is not supported for hoglake yet (lands in M4)");
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
