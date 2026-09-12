#include "storage/hoglake_transaction.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include <chrono>
#include <thread>
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "common/hoglake_types.hpp"
#include "storage/hoglake_catalog.hpp"
#include "storage/hoglake_schema_entry.hpp"

namespace duckdb {

HoglakeTransaction::HoglakeTransaction(HoglakeCatalog &hoglake_catalog_p, TransactionManager &manager,
                                       ClientContext &context)
    : Transaction(manager, context), hoglake_catalog(hoglake_catalog_p) {
}

HoglakeTransaction::~HoglakeTransaction() {
}

HoglakeTransaction &HoglakeTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<HoglakeTransaction>();
}

HoglakeApiClient &HoglakeTransaction::Api() {
	return hoglake_catalog.Api();
}

idx_t HoglakeTransaction::GetSnapshotInternal() {
	if (pinned_snapshot.IsValid()) {
		return pinned_snapshot.GetIndex();
	}
	if (!hoglake_catalog.AttachSnapshotTime().empty()) {
		// a SNAPSHOT_TIME attach has no client-resolved snapshot id (the
		// wire offers no timestamp->snapshot resolution; reads re-send
		// at_timestamp and the server resolves it consistently). Pinning
		// head here would LIE to anything that asks for the snapshot id.
		throw NotImplementedException(
		    "hoglake: this attach is pinned by SNAPSHOT_TIME; the wire cannot resolve a timestamp to a snapshot id "
		    "client-side (reads are still consistently pinned server-side). Attach with SNAPSHOT_VERSION for an "
		    "explicit snapshot id");
	}
	auto attach_pin = hoglake_catalog.AttachSnapshotVersion();
	if (attach_pin.IsValid()) {
		pinned_snapshot = attach_pin;
		return pinned_snapshot.GetIndex();
	}
	// pin head at first touch: every later metadata read in this
	// transaction carries this snapshot for multi-table consistency
	auto info = Api().GetCatalog();
	pinned_snapshot = NumericCast<idx_t>(info.head_snapshot_id);
	return pinned_snapshot.GetIndex();
}

idx_t HoglakeTransaction::GetSnapshot() {
	std::lock_guard<std::recursive_mutex> guard(transaction_lock);
	return GetSnapshotInternal();
}

HoglakeTravel HoglakeTransaction::Travel() {
	if (!hoglake_catalog.AttachSnapshotTime().empty()) {
		HoglakeTravel travel;
		travel.at_timestamp = hoglake_catalog.AttachSnapshotTime();
		return travel;
	}
	return HoglakeTravel::AtSnapshot(GetSnapshot());
}

HoglakeTravel HoglakeTransaction::TravelFor(optional_ptr<BoundAtClause> at_clause) {
	if (!at_clause) {
		return Travel();
	}
	auto unit = StringUtil::Lower(at_clause->Unit().GetIdentifierName());
	if (unit == "version") {
		auto version = BigIntValue::Get(at_clause->GetValue().DefaultCastAs(LogicalType::BIGINT));
		if (version < 0) {
			throw BinderException("hoglake: AT (VERSION => %lld): snapshot versions are non-negative", version);
		}
		return HoglakeTravel::AtSnapshot(NumericCast<idx_t>(version));
	}
	if (unit == "timestamp") {
		// server wants an ISO-8601 instant WITH offset; parse with
		// INSTANT semantics through the session's TIMESTAMPTZ cast so
		// explicit offsets convert (never dropped) and naive inputs
		// mean what they mean everywhere else in the session
		auto context_ptr = context.lock();
		if (!context_ptr) {
			throw InternalException("hoglake: transaction context expired while resolving AT (TIMESTAMP)");
		}
		HoglakeTravel travel;
		travel.at_timestamp = HoglakeTypes::CanonicalInstant(*context_ptr, at_clause->GetValue());
		return travel;
	}
	throw BinderException("hoglake: unsupported AT unit \"%s\" (VERSION or TIMESTAMP)", unit);
}

//===--------------------------------------------------------------------===//
// Write buffers
//===--------------------------------------------------------------------===//

void HoglakeTransaction::AddAppend(const string &ns, const string &table, const string &expected_table_uuid,
                                   vector<HoglakeFileRegistration> files) {
	if (files.empty()) {
		return;
	}
	std::lock_guard<std::recursive_mutex> guard(transaction_lock);
	for (auto &append : buffered_appends) {
		if (StringUtil::CIEquals(append.namespace_name, ns) && StringUtil::CIEquals(append.table_name, table)) {
			for (auto &file : files) {
				append.files.push_back(std::move(file));
			}
			return;
		}
	}
	HoglakeTableAppend append;
	append.namespace_name = ns;
	append.table_name = table;
	append.expected_table_uuid = expected_table_uuid;
	append.files = std::move(files);
	buffered_appends.push_back(std::move(append));
}

void HoglakeTransaction::AddDeletes(const string &ns, const string &table, const string &expected_table_uuid,
                                    vector<HoglakeDeleteFileRegistration> files) {
	if (files.empty()) {
		return;
	}
	std::lock_guard<std::recursive_mutex> guard(transaction_lock);
	for (auto &deletes : buffered_deletes) {
		if (StringUtil::CIEquals(deletes.namespace_name, ns) && StringUtil::CIEquals(deletes.table_name, table)) {
			for (auto &file : files) {
				// one live DV per data file per commit: a later
				// statement's registration (whose positions are a
				// superset — the delete sink merged via
				// GetBufferedDeletePositions) REPLACES the earlier one
				bool replaced = false;
				for (auto &existing : deletes.files) {
					if (existing.data_file_id == file.data_file_id) {
						D_ASSERT(file.positions.size() >= existing.positions.size());
						existing = std::move(file);
						replaced = true;
						break;
					}
				}
				if (!replaced) {
					deletes.files.push_back(std::move(file));
				}
			}
			return;
		}
	}
	HoglakeTableDeletes deletes;
	deletes.namespace_name = ns;
	deletes.table_name = table;
	deletes.expected_table_uuid = expected_table_uuid;
	deletes.files = std::move(files);
	buffered_deletes.push_back(std::move(deletes));
}

set<idx_t> HoglakeTransaction::GetBufferedDeletePositions(const string &ns, const string &table,
                                                          int64_t data_file_id) {
	std::lock_guard<std::recursive_mutex> guard(transaction_lock);
	for (auto &deletes : buffered_deletes) {
		if (!StringUtil::CIEquals(deletes.namespace_name, ns) || !StringUtil::CIEquals(deletes.table_name, table)) {
			continue;
		}
		for (auto &file : deletes.files) {
			if (file.data_file_id == data_file_id) {
				return file.positions;
			}
		}
	}
	return set<idx_t>();
}

bool HoglakeTransaction::HasBufferedWrites() {
	std::lock_guard<std::recursive_mutex> guard(transaction_lock);
	return !buffered_appends.empty() || !buffered_deletes.empty();
}

bool HoglakeTransaction::HasBufferedWritesFor(const string &ns, const string &table) {
	std::lock_guard<std::recursive_mutex> guard(transaction_lock);
	for (auto &append : buffered_appends) {
		if (StringUtil::CIEquals(append.namespace_name, ns) && StringUtil::CIEquals(append.table_name, table)) {
			return true;
		}
	}
	for (auto &deletes : buffered_deletes) {
		if (StringUtil::CIEquals(deletes.namespace_name, ns) && StringUtil::CIEquals(deletes.table_name, table)) {
			return true;
		}
	}
	return false;
}

void HoglakeTransaction::RecordAlteredTable(const string &ns, const string &table) {
	std::lock_guard<std::recursive_mutex> guard(transaction_lock);
	altered_tables[ns + "." + table] = true;
}

bool HoglakeTransaction::IsAlteredTable(const string &ns, const string &table) {
	std::lock_guard<std::recursive_mutex> guard(transaction_lock);
	return altered_tables.find(ns + "." + table) != altered_tables.end();
}

//! Self-conflict prevention: the server's commit conflict check (runs
//! iff the commit carries deletes) scans table_dropped/table_altered
//! changes after read_snapshot over every TOUCHED table (appends and
//! deletes alike). An eager ALTER inside this transaction mints such a
//! change AFTER the pin, so a later commit-with-deletes touching that
//! table would deterministically 409 against our own DDL. These rules
//! keep touched∩altered empty whenever deletes are present.
void HoglakeTransaction::RequireDMLAllowed(const string &ns, const string &table, bool is_delete) {
	std::lock_guard<std::recursive_mutex> guard(transaction_lock);
	if (is_delete) {
		// uncommitted INSERTS (from INSERT or an earlier UPDATE's
		// rewrites) are invisible to this transaction's scans, so a
		// predicate-bearing DELETE/UPDATE over a table with buffered
		// appends can silently miss rows it should affect (partial
		// predicate overlap reports a plausible nonzero count while
		// committing sequentially-wrong data). Wrong data beats no
		// data: refuse loudly.
		for (auto &append : buffered_appends) {
			if (StringUtil::CIEquals(append.namespace_name, ns) && StringUtil::CIEquals(append.table_name, table)) {
				throw TransactionException(
				    "hoglake: cannot DELETE/UPDATE table \"%s.%s\": this transaction already inserted or "
				    "updated rows in it, and uncommitted inserts are invisible to the transaction's own scans — "
				    "the statement could silently miss them. COMMIT or ROLLBACK first (see DESIGN.md, "
				    "\"Transactions and snapshot pinning\")",
				    ns, table);
			}
		}
		if (altered_tables.find(ns + "." + table) != altered_tables.end()) {
			throw TransactionException(
			    "hoglake: cannot DELETE/UPDATE table \"%s.%s\": this transaction already ran DDL on it, and a "
			    "delete commit would conflict with the transaction's own (eager, already-persisted) DDL. COMMIT or "
			    "ROLLBACK first (see DESIGN.md, \"Transactions and eager DDL\")",
			    ns, table);
		}
		for (auto &append : buffered_appends) {
			auto key = append.namespace_name + "." + append.table_name;
			if (altered_tables.find(key) != altered_tables.end()) {
				throw TransactionException(
				    "hoglake: cannot DELETE/UPDATE in this transaction: it inserted into table \"%s\" after "
				    "running DDL on it, and a commit carrying deletes would conflict with the transaction's own "
				    "(eager, already-persisted) DDL. COMMIT or ROLLBACK first (see DESIGN.md, \"Transactions and "
				    "eager DDL\")",
				    key);
			}
		}
		return;
	}
	// insert: only a problem when the commit will carry deletes AND the
	// target table was altered in this transaction
	if (!buffered_deletes.empty() && altered_tables.find(ns + "." + table) != altered_tables.end()) {
		throw TransactionException(
		    "hoglake: cannot INSERT into table \"%s.%s\": this transaction ran DDL on it and also holds buffered "
		    "deletes — the commit's conflict check would 409 against the transaction's own (eager, "
		    "already-persisted) DDL. COMMIT or ROLLBACK first (see DESIGN.md, \"Transactions and eager DDL\")",
		    ns, table);
	}
}

void HoglakeTransaction::RequireDDLAllowed(const string &ns, const string &table, const char *what) {
	if (HasBufferedWritesFor(ns, table)) {
		throw TransactionException(
		    "hoglake: cannot %s table \"%s.%s\": this transaction holds uncommitted writes for it. hoglake DDL is "
		    "eager (each DDL is its own server commit, outside this transaction), so mixing it with buffered writes "
		    "to the same table cannot commit atomically. COMMIT or ROLLBACK first (see DESIGN.md, \"Transactions "
		    "and eager DDL\")",
		    what, ns, table);
	}
}

//===--------------------------------------------------------------------===//
// Commit
//===--------------------------------------------------------------------===//

//! The server's commit 409 for a mismatched expected_table_uuid says
//! this in its message/detail; it distinguishes a recreation refusal
//! (never retryable) from an ordinary commit conflict (retryable) —
//! same discrimination as pyhoglake.
static constexpr const char *RECREATED_MARKER = "the table was recreated";

template <class T>
static T GetSettingOrDefault(ClientContext &context, const char *name, T default_value) {
	Value value;
	if (context.TryGetCurrentSetting(name, value) && !value.IsNull()) {
		return value.GetValue<T>();
	}
	return default_value;
}

void HoglakeTransaction::Commit(ClientContext &context) {
	HoglakeCommitRequest request;
	bool has_deletes;
	{
		std::lock_guard<std::recursive_mutex> guard(transaction_lock);
		if (buffered_appends.empty() && buffered_deletes.empty()) {
			return;
		}
		request.appends = std::move(buffered_appends);
		request.deletes = std::move(buffered_deletes);
		buffered_appends.clear();
		buffered_deletes.clear();
		// append-only commits are blind (no read_snapshot: no conflict
		// window; the incarnation guard rides expected_table_uuid).
		// Deletes REQUIRE a read_snapshot (they always conflict-check) —
		// and a delete conflict is never auto-retried: the superseded
		// deletion vector must be rebuilt against the new state, so the
		// statement has to be re-run.
		has_deletes = !request.deletes.empty();
		if (has_deletes) {
			request.read_snapshot = GetSnapshotInternal();
		}
	}

	// clamp the retry knobs: they are user settings and the arithmetic
	// below must never hit float->unsigned UB or absurd sleeps
	auto max_retries = MinValue<uint64_t>(GetSettingOrDefault<uint64_t>(context, "hoglake_max_retry_count", 10), 1000);
	auto wait_ms = MinValue<uint64_t>(GetSettingOrDefault<uint64_t>(context, "hoglake_retry_wait_ms", 100), 600000);
	auto backoff = GetSettingOrDefault<double>(context, "hoglake_retry_backoff", 1.5);
	if (!(backoff >= 1.0) || backoff > 10.0) { // NaN-safe
		backoff = 1.5;
	}
	static constexpr double MAX_WAIT_MS = 600000.0;

	double current_wait = static_cast<double>(wait_ms);
	for (idx_t attempt = 0;; attempt++) {
		auto outcome = Api().TryCommit(request);
		if (outcome.success) {
			return;
		}
		auto message = StringUtil::Format("hoglake: commit failed: %s%s%s", outcome.error,
		                                  outcome.detail.empty() ? "" : " — ", outcome.detail);
		bool retryable = false;
		idx_t sleep_ms = 0;
		if (outcome.status == 409) {
			auto text = StringUtil::Lower(outcome.error + " " + outcome.detail);
			if (StringUtil::Contains(text, RECREATED_MARKER)) {
				// incarnation guard: atomic refusal, never retryable
				throw TransactionException("%s", message);
			}
			if (has_deletes) {
				throw TransactionException("%s (a conflicting commit superseded this transaction's deletion "
				                           "vectors; re-run the statement)",
				                           message);
			}
			retryable = true;
		} else if (outcome.status == 503) {
			// explicit commit backpressure; honor Retry-After
			retryable = true;
			if (outcome.retry_after_seconds > 0) {
				sleep_ms = MinValue<idx_t>(outcome.retry_after_seconds, 600) * 1000;
			}
		}
		if (!retryable || attempt >= max_retries) {
			throw TransactionException("%s%s", message, retryable ? " (retries exhausted)" : "");
		}
		if (sleep_ms == 0) {
			current_wait = MinValue<double>(current_wait, MAX_WAIT_MS);
			sleep_ms = static_cast<idx_t>(current_wait);
			current_wait = MinValue<double>(current_wait * backoff, MAX_WAIT_MS);
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
	}
}

void HoglakeTransaction::Rollback() {
	// buffered registrations are dropped; parquet/puffin already
	// uploaded for them is orphaned (cleanup's problem, never the
	// catalog's)
	std::lock_guard<std::recursive_mutex> guard(transaction_lock);
	buffered_appends.clear();
	buffered_deletes.clear();
}

//===--------------------------------------------------------------------===//
// Schema cache
//===--------------------------------------------------------------------===//

void HoglakeTransaction::LoadSchemas() {
	if (schemas_loaded) {
		return;
	}
	// NOTE (wire): /namespaces is head-only — the listing itself is not
	// versioned by the pinned snapshot. Tables ARE fetched at the pinned
	// snapshot. Documented as a server finding in DESIGN.md.
	auto names = Api().ListNamespaces();
	// group CI-equal names first: a case-colliding namespace pair is
	// unaddressable from DuckDB and must error as ambiguous (same
	// policy as tables), never silently bind first-listed-wins
	case_insensitive_map_t<vector<string>> grouped;
	for (auto &ns : names) {
		grouped[ns].push_back(ns);
	}
	for (auto &group : grouped) {
		auto &exact_names = group.second;
		if (exact_names.size() > 1) {
			ambiguous_schemas[exact_names[0]] = exact_names;
			continue;
		}
		auto &ns = exact_names[0];
		if (schemas.find(ns) != schemas.end()) {
			continue;
		}
		CreateSchemaInfo schema_info;
		schema_info.SetQualifiedName(QualifiedName(schema_info.GetQualifiedName().Catalog(), Identifier(ns),
		                                           schema_info.GetQualifiedName().Name()));
		auto entry = make_uniq<HoglakeSchemaEntry>(hoglake_catalog, schema_info, *this);
		schemas.emplace(ns, std::move(entry));
	}
	schemas_loaded = true;
}

optional_ptr<HoglakeSchemaEntry> HoglakeTransaction::GetSchema(const string &name) {
	std::lock_guard<std::recursive_mutex> guard(transaction_lock);
	if (!schemas_loaded) {
		LoadSchemas();
	}
	auto ambiguous = ambiguous_schemas.find(name);
	if (ambiguous != ambiguous_schemas.end()) {
		throw CatalogException("hoglake: namespace identifier \"%s\" is ambiguous: the server holds multiple "
		                       "namespaces whose names differ only by case (%s). Rename one via another client to "
		                       "make them addressable from DuckDB",
		                       name, StringUtil::Join(ambiguous->second, ", "));
	}
	auto entry = schemas.find(name);
	if (entry != schemas.end()) {
		return entry->second.get();
	}
	return nullptr;
}

void HoglakeTransaction::ScanSchemas(const std::function<void(HoglakeSchemaEntry &)> &callback) {
	// snapshot the entry list under the lock, run callbacks unlocked:
	// callbacks re-enter schema entries (their lock is ordered BEFORE
	// this one — holding ours here would be an ABBA deadlock). Entries
	// are never destroyed during the transaction, so the pointers stay
	// valid.
	vector<HoglakeSchemaEntry *> entries;
	{
		std::lock_guard<std::recursive_mutex> guard(transaction_lock);
		LoadSchemas();
		for (auto &entry : schemas) {
			entries.push_back(entry.second.get());
		}
	}
	for (auto entry : entries) {
		callback(*entry);
	}
}

HoglakeSchemaEntry &HoglakeTransaction::AddSchema(unique_ptr<HoglakeSchemaEntry> schema) {
	std::lock_guard<std::recursive_mutex> guard(transaction_lock);
	auto name = schema->name.GetIdentifierName();
	auto &result = *schema;
	schemas[name] = std::move(schema);
	return result;
}

} // namespace duckdb
