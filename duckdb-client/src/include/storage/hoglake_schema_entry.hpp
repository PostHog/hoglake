//===----------------------------------------------------------------------===//
// HoglakeSchemaEntry: one hoglake namespace.
//
// Identifier policy: DuckDB identifiers are case-insensitive and
// case-preserving; the hoglake server matches names case-sensitively.
// The bridge: table names are resolved through the (case-preserved)
// server listing — every lookup / DDL call CI-resolves the user-typed
// identifier to the server's exact name and uses THAT on the wire.
// Creates send the typed case (case-preserving); CI conflicts are
// duplicates, DuckDB-style. Two server tables whose names differ only
// by case are reported ambiguous unless the typed case matches one
// exactly.
//
// Locking: `entry_lock` (recursive) guards every cache below. Lock
// order: entry_lock may be taken before the transaction lock (methods
// call transaction.Travel()); never the reverse. Entries are retired,
// never destroyed, while the transaction lives.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest/hoglake_api_client.hpp"

#include <mutex>

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

	//! CI-resolve a typed table identifier to the server's exact name
	//! (empty when no such table). Public for the write paths.
	string ResolveTableName(HoglakeTransaction &transaction, const string &typed_name);

private:
	HoglakeTransaction &Transaction(CatalogTransaction &transaction);
	// -- all *Internal methods require entry_lock held ---------------------
	//! load the CI name -> exact server name index from the listing
	void EnsureTableNamesInternal(HoglakeTransaction &transaction);
	string ResolveTableNameInternal(HoglakeTransaction &transaction, const string &typed_name);
	optional_ptr<CatalogEntry> LookupTableInternal(HoglakeTransaction &transaction, const string &typed_name);
	optional_ptr<CatalogEntry> LookupTableAtInternal(HoglakeTransaction &transaction, const string &typed_name,
	                                                 const HoglakeTravel &travel);
	void LoadAllTablesInternal(HoglakeTransaction &transaction);
	//! Wrap a wire table into a HoglakeTableEntry and cache it (keyed by
	//! the server's exact name). An existing entry under that key is
	//! retired, never destroyed.
	CatalogEntry &CacheTableInternal(HoglakeTransaction &transaction, struct HoglakeTableInfo table_info,
	                                 const HoglakeTravel &read_travel, bool writes_refused);

private:
	//! guards every member below (recursive: DDL paths re-enter lookup)
	std::recursive_mutex entry_lock;
	//! loaded table entries (exact server name -> entry)
	case_insensitive_map_t<unique_ptr<CatalogEntry>> tables;
	bool all_tables_loaded = false;
	//! CI identifier index: lowercase-agnostic key -> exact server
	//! name(s) sharing that CI key
	case_insensitive_map_t<vector<string>> table_names;
	bool table_names_loaded = false;
	//! tables dropped by THIS transaction (eager server drop): the pin
	//! predates the drop, so server reads would resurrect them —
	//! filtered out of every lookup/listing (exact server names)
	case_insensitive_map_t<bool> dropped_tables;
	//! superseded entries (ALTER/DROP replace them); kept alive because
	//! the statement may still hold references
	vector<unique_ptr<CatalogEntry>> retired;
	//! AT (VERSION/TIMESTAMP) entries, keyed "<name>@<travel key>"
	case_insensitive_map_t<unique_ptr<CatalogEntry>> travel_tables;
};

} // namespace duckdb
