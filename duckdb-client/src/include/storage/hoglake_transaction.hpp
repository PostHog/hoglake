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

class HoglakeTransaction : public Transaction, public enable_shared_from_this<HoglakeTransaction> {
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

	//! Buffer a table append (files already uploaded to object storage);
	//! shipped as ONE CommitRequest at COMMIT (multi-statement,
	//! multi-table atomicity comes from the wire contract).
	void AddAppend(const string &ns, const string &table, const string &expected_table_uuid,
	               vector<HoglakeFileRegistration> files);
	bool HasBufferedWrites() const {
		return !buffered_appends.empty();
	}

	//! Commit buffered work: the footer-shipping OCC commit with the
	//! retry loop (409 conflict / 503 backpressure). DDL is eager and
	//! not part of this.
	void Commit(ClientContext &context);
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
	//! buffered appends, one entry per (namespace, table)
	vector<HoglakeTableAppend> buffered_appends;
};

} // namespace duckdb
