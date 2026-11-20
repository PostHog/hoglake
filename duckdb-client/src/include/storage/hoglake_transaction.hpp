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
	//! The first get-table of a table in a REMOTE schema (DESIGN.md
	//! "Remote schemas"): head, or the AT / attach timestamp, never this
	//! catalog's snapshot id. A version (AT (VERSION) or a SNAPSHOT_VERSION
	//! attach) names this catalog's history: refused before any request.
	HoglakeTravel RemoteFirstTravel(const string &ns, optional_ptr<BoundAtClause> at_clause);
	//! The travel an entry fetched at `fetched` reads at from then on:
	//! `fetched` in a local schema; in a remote one, the table's own
	//! read_snapshot_id. A table answered as remote in a local schema
	//! fails (the read carried this catalog's snapshot).
	static HoglakeTravel EntryTravel(const string &ns, bool remote, const HoglakeTableInfo &table,
	                                 const HoglakeTravel &fetched);

	//! Buffer a table append (files already uploaded to object storage);
	//! shipped as ONE CommitRequest at COMMIT (multi-statement,
	//! multi-table atomicity comes from the wire contract).
	void AddAppend(const string &ns, const string &table, const string &expected_table_uuid,
	               idx_t read_snapshot, vector<HoglakeFileRegistration> files);
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
	//! Record that this transaction ran an eager ALTER on (ns, table)
	//! (renames record both names): its table_altered change postdates
	//! the pin, so later commits carrying deletes must not touch it.
	void RecordAlteredTable(const string &ns, const string &table);
	bool IsAlteredTable(const string &ns, const string &table);
	//! Throw when a DML statement would make the eventual commit
	//! self-conflict with this transaction's own eager DDL.
	void RequireDMLAllowed(const string &ns, const string &table, bool is_delete);

	//! Commit buffered work: the footer-shipping OCC commit with the
	//! retry loop (409 conflict / 503 backpressure). DDL is eager and
	//! not part of this.
	void Commit(ClientContext &context);
	void Rollback();

	//! Default for hoglake_commit_deadline_ms: the wall time one commit's
	//! retry loop may take across every attempt and backoff. Without it
	//! retried read timeouts (hoglake_commit_timeout_ms each) could hold
	//! COMMIT for max_retries × timeout, ~22 min at the defaults, against
	//! a hung server.
	static constexpr idx_t DEFAULT_COMMIT_DEADLINE_MS = 300000;
	//! Ceiling on hoglake_commit_deadline_ms: half the server's floor on
	//! HOGLAKE_RECEIPT_RETENTION_SECONDS (one hour, Config's
	//! MIN_RECEIPT_RETENTION_SECONDS). A retry under the key replays the
	//! receipt only while the receipt exists; one sent after cleanup
	//! purged it publishes the same files again and reports success.
	static constexpr idx_t MAX_COMMIT_DEADLINE_MS = 1800000;

	// -- catalog entry cache ----------------------------------------------
	//! A name the listing does not hold is asked of the server once per
	//! transaction (`probe`), and the answer says whether it is remote.
	optional_ptr<HoglakeSchemaEntry> GetSchema(const string &name, bool probe = true);
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
	//! CI-colliding namespace groups: lookups throw ambiguity; listings
	//! skip them (same policy as tables)
	case_insensitive_map_t<vector<string>> ambiguous_schemas;
	//! get-namespace answers for names the listing does not hold (null =
	//! 404); kept out of `schemas`, so ScanSchemas lists only the listing
	case_insensitive_map_t<unique_ptr<HoglakeSchemaEntry>> probed_schemas;
	//! buffered appends, one entry per (namespace, table)
	vector<HoglakeTableAppend> buffered_appends;
	optional_idx append_read_snapshot;
	//! buffered deletes, one entry per (namespace, table); at most one
	//! file registration per data_file_id
	vector<HoglakeTableDeletes> buffered_deletes;
	//! tables this transaction ran eager ALTERs on ("ns.table" keys;
	//! renames record both names)
	case_insensitive_map_t<bool> altered_tables;
};

} // namespace duckdb
