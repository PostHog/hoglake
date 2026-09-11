//===----------------------------------------------------------------------===//
// HoglakeTransaction: snapshot pinning + the per-transaction catalog
// entry cache. One snapshot id is pinned at first catalog touch and
// every metadata read in the transaction carries it (multi-table
// consistent reads — DuckLake's property, reproduced over REST).
//
// Locking: `transaction_lock` (recursive) guards the pin, the schema
// cache, and the write buffers. Lock order is schema-entry lock BEFORE
// transaction lock (schema-entry methods call GetSnapshot/Travel);
// ScanSchemas therefore snapshots the entry list under the lock and
// runs callbacks unlocked.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/set.hpp"
#include "rest/hoglake_api_client.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

#include <mutex>

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
	//! Travel for an optional AT (VERSION/TIMESTAMP) clause; nullptr =
	//! the transaction's pinned travel.
	HoglakeTravel TravelFor(optional_ptr<BoundAtClause> at_clause);

	//! Buffer a table append (files already uploaded to object storage);
	//! shipped as ONE CommitRequest at COMMIT (multi-statement,
	//! multi-table atomicity comes from the wire contract).
	void AddAppend(const string &ns, const string &table, const string &expected_table_uuid,
	               vector<HoglakeFileRegistration> files);
	//! Buffer superseding deletion vectors (puffin files already
	//! uploaded). Registrations REPLACE any earlier buffered
	//! registration for the same data_file_id — a statement's DV must
	//! already contain the earlier statements' positions (the delete
	//! sink merges via GetBufferedDeletePositions), so one commit ships
	//! exactly one live DV per data file.
	void AddDeletes(const string &ns, const string &table, const string &expected_table_uuid,
	                vector<HoglakeDeleteFileRegistration> files);
	//! Positions already buffered in this transaction for one data file
	//! (empty set when none).
	set<idx_t> GetBufferedDeletePositions(const string &ns, const string &table, int64_t data_file_id);
	bool HasBufferedWrites();
	//! Whether this transaction buffered appends or deletes for (ns,
	//! table) — eager DDL on such a table is refused (see DESIGN.md
	//! "Transactions and eager DDL").
	bool HasBufferedWritesFor(const string &ns, const string &table);
	//! Throw a clear TransactionException when DDL on (ns, table) is not
	//! allowed in this transaction's current state.
	void RequireDDLAllowed(const string &ns, const string &table, const char *what);

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

private:
	//! caller must hold transaction_lock
	void LoadSchemas();
	idx_t GetSnapshotInternal();

private:
	HoglakeCatalog &hoglake_catalog;
	//! guards everything below (recursive: Travel -> GetSnapshot etc.)
	std::recursive_mutex transaction_lock;
	//! pinned snapshot; invalid until first use
	optional_idx pinned_snapshot;
	bool schemas_loaded = false;
	case_insensitive_map_t<unique_ptr<HoglakeSchemaEntry>> schemas;
	//! buffered appends, one entry per (namespace, table)
	vector<HoglakeTableAppend> buffered_appends;
	//! buffered deletes, one entry per (namespace, table); at most one
	//! file registration per data_file_id
	vector<HoglakeTableDeletes> buffered_deletes;
};

} // namespace duckdb
