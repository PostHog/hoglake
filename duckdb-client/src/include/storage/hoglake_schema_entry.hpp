//===----------------------------------------------------------------------===//
// HoglakeSchemaEntry: one hoglake namespace. Table entries are loaded
// lazily (per-name on lookup, full list on scan) and owned by this
// entry, which itself lives in the transaction's catalog cache.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"

namespace duckdb {
class HoglakeCatalog;
class HoglakeTransaction;

class HoglakeSchemaEntry : public SchemaCatalogEntry {
public:
	HoglakeSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, HoglakeTransaction &transaction);

public:
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;
	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;

private:
	HoglakeTransaction &Transaction(CatalogTransaction &transaction);
	optional_ptr<CatalogEntry> LookupTable(HoglakeTransaction &transaction, const string &entry_name);
	void LoadAllTables(HoglakeTransaction &transaction);
	//! Wrap a wire table into a HoglakeTableEntry and cache it.
	CatalogEntry &CacheTable(HoglakeTransaction &transaction, struct HoglakeTableInfo table_info);

private:
	//! loaded table entries (name -> entry); nullptr value = known-missing
	case_insensitive_map_t<unique_ptr<CatalogEntry>> tables;
	bool all_tables_loaded = false;
};

} // namespace duckdb
