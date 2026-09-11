//===----------------------------------------------------------------------===//
// HoglakeTransaction: snapshot pinning + the per-transaction catalog
// entry cache. One snapshot id is pinned at first catalog touch and
// every metadata read in the transaction carries it (multi-table
// consistent reads — DuckLake's property, reproduced over REST).
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "rest/hoglake_api_client.hpp"

namespace duckdb {
class HoglakeCatalog;
class HoglakeSchemaEntry;

class HoglakeTransaction : public Transaction {
public:
	HoglakeTransaction(HoglakeCatalog &hoglake_catalog, TransactionManager &manager, ClientContext &context);
	~HoglakeTransaction() override;

	static HoglakeTransaction &Get(ClientContext &context, Catalog &catalog);

	HoglakeCatalog &GetCatalog() {
		return hoglake_catalog;
	}
	HoglakeApiClient &Api();

	//! The pinned snapshot (pins head on first call, unless the attach
	//! carries a snapshot/timestamp pin).
	idx_t GetSnapshot();
	//! The travel selector for metadata reads in this transaction.
	HoglakeTravel Travel();

	//! Commit buffered work (M3+: the footer-shipping commit); currently
	//! DDL is eager, so this is a no-op.
	void Commit();
	void Rollback();

	// -- catalog entry cache ----------------------------------------------
	optional_ptr<HoglakeSchemaEntry> GetSchema(const string &name);
	void ScanSchemas(const std::function<void(HoglakeSchemaEntry &)> &callback);
	//! Register a schema entry created inside this transaction (eager DDL).
	HoglakeSchemaEntry &AddSchema(unique_ptr<HoglakeSchemaEntry> schema);
	//! Drop a cached schema entry (after an eager server-side drop).
	void EraseSchema(const string &name);

private:
	void LoadSchemas();

private:
	HoglakeCatalog &hoglake_catalog;
	//! pinned snapshot; invalid until first use
	optional_idx pinned_snapshot;
	bool schemas_loaded = false;
	case_insensitive_map_t<unique_ptr<HoglakeSchemaEntry>> schemas;
};

} // namespace duckdb
