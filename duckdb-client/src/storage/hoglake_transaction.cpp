#include "storage/hoglake_transaction.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/uuid.hpp"
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
		pinned_snapshot = NumericCast<idx_t>(Api().ResolveSnapshot(hoglake_catalog.AttachSnapshotTime()).snapshot_id);
		return pinned_snapshot.GetIndex();
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

HoglakeTravel HoglakeTransaction::RemoteFirstTravel(const string &ns, optional_ptr<BoundAtClause> at_clause) {
	// an AT clause overrides the attach pin, as in TravelFor
	bool version = at_clause ? StringUtil::CIEquals(at_clause->Unit().GetIdentifierName(), "version")
	                         : hoglake_catalog.AttachSnapshotVersion().IsValid();
	if (version) {
		throw BinderException("hoglake: schema \"%s\" is remote (served by another Hoglake), so it cannot be read at a "
		                      "snapshot version (AT (VERSION) or ATTACH ... SNAPSHOT_VERSION): the version is this "
		                      "catalog's, which means nothing on the remote side. Read it at head or with a timestamp",
		                      ns);
	}
	if (at_clause) {
		return TravelFor(at_clause);
	}
	HoglakeTravel travel;
	// empty = head; otherwise the same instant GetSnapshotInternal resolves
	travel.at_timestamp = hoglake_catalog.AttachSnapshotTime();
	return travel;
}

HoglakeTravel HoglakeTransaction::EntryTravel(const string &ns, bool remote, const HoglakeTableInfo &table,
                                              const HoglakeTravel &fetched) {
	if (!remote) {
		if (table.remote) {
			throw TransactionException("hoglake: table \"%s.%s\" was answered as remote, but schema \"%s\" is not "
			                           "listed as remote: the read carried this catalog's snapshot, which means "
			                           "nothing to the remote side",
			                           ns, table.name, ns);
		}
		return fetched;
	}
	if (!table.has_read_snapshot_id) {
		throw InvalidInputException("hoglake: table \"%s.%s\" of remote schema \"%s\" came without "
		                            "read_snapshot_id, the snapshot to pin it at",
		                            ns, table.name, ns);
	}
	return HoglakeTravel::AtSnapshot(NumericCast<idx_t>(table.read_snapshot_id));
}

//===--------------------------------------------------------------------===//
// Write buffers
//===--------------------------------------------------------------------===//

void HoglakeTransaction::AddAppend(const string &ns, const string &table, const string &expected_table_uuid,
                                   idx_t read_snapshot, vector<HoglakeFileRegistration> files) {
	if (files.empty()) {
		return;
	}
	std::lock_guard<std::recursive_mutex> guard(transaction_lock);
	if (!append_read_snapshot.IsValid() || read_snapshot < append_read_snapshot.GetIndex()) {
		append_read_snapshot = read_snapshot;
	}
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
//! whenever read_snapshot is present) scans table_dropped/table_altered
//! changes after read_snapshot over every TOUCHED table (appends and
//! deletes alike). An eager ALTER inside this transaction mints such a
//! change AFTER the pin, so a later commit using that pin and touching that
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

//! Catalog capability under which the server honors `idempotency_key`
//! on /commit and serves GET /commit/receipts/{key}. Without it the key
//! is ignored and a retry of an unknown outcome is blind.
static constexpr const char *IDEMPOTENT_APPEND = "idempotent-append-v1";

//! The terminal commit error's suffix once ANY attempt's outcome was
//! unknown. It REPLACES the definite-outcome suffix (a 409's "re-run the
//! statement", "retries exhausted", the deadline) so the error carries
//! one instruction: `ended` says how the loop ended (empty when the
//! unknown answer itself ended it) and `new_transaction` that the last
//! answer needs one. What the server contract guarantees (openapi
//! getCommitReceipt): a receipt answers 200 with the snapshot once the
//! commit published; a 404 proves nothing either way — an attempt may
//! still be in flight, and the server names NO time after which one has
//! surely stopped — and receipts are purged after
//! HOGLAKE_RECEIPT_RETENTION_SECONDS. The safe recovery is a retry under
//! the SAME key, which this extension cannot do (the key is minted per
//! commit, never caller-chosen), so the advice is the receipt lookup
//! and, on a 404, the table's contents.
static string UnknownOutcomeAdvice(HoglakeApiClient &api, idx_t attempts, const string &ended, bool new_transaction,
                                   const string &key) {
	auto lead = StringUtil::Format("%s%safter %llu attempt(s) the commit MAY be durable", ended, ended.empty() ? "" : "; ",
	                               attempts);
	auto rerun = new_transaction ? "re-running the statement in a new transaction" : "re-running the statement";
	if (key.empty()) {
		return StringUtil::Format(
		    " (%s: an attempt got no definite answer, so the server may have published it or may still be publishing "
		    "it. This server does not advertise %s, so the extension sent no idempotency_key, did not retry — a blind "
		    "retry could register the files twice — and has no receipt to point at. Do not re-run the statement blind: "
		    "check the table's contents or the catalog's newest snapshots (hoglake_snapshots) for this commit before %s)",
		    lead, IDEMPOTENT_APPEND, rerun);
	}
	return StringUtil::Format(
	    " (%s: an attempt got no definite answer, so the server may have published it or may still be publishing it. "
	    "Do not re-run the statement blind — the files would register twice — and this extension cannot retry under "
	    "the same idempotency_key. Look up the receipt first: GET %s/v1/catalogs/%s/commit/receipts/%s answers 200 "
	    "with the snapshot once the commit published. A 404 is not proof either way: an attempt may still be in "
	    "flight, and receipts are purged after HOGLAKE_RECEIPT_RETENTION_SECONDS (7 days by default). On a 404, check "
	    "the table's contents or hoglake_snapshots for this commit before %s)",
	    lead, api.GetEndpoint(), HoglakeApiClient::EncodeSegment(api.GetCatalogName()), key, rerun);
}

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
		request.read_snapshot = append_read_snapshot;
		append_read_snapshot = optional_idx();
		has_deletes = !request.deletes.empty();
		if (has_deletes) {
			auto delete_snapshot = GetSnapshotInternal();
			if (!request.read_snapshot.IsValid() || delete_snapshot < request.read_snapshot.GetIndex()) {
				request.read_snapshot = delete_snapshot;
			}
		}
	}
	// One key for every attempt below, when the server honors one. A 503
	// can mean the commit never reached the catalog (commit_queue_timeout,
	// backend_unavailable) or that it may be durable
	// (commit_outcome_unknown — Hoglite's lost acknowledgement); the
	// client cannot tell which, and a blind retry of the second registers
	// the same parquet twice (duplicate paths are legal) and reports
	// success. The same holds when the answer is lost in transport (a
	// reset connection, the read timeout, a proxy's 502/504) and for the
	// server's catch-all 500: on the PostgreSQL backend a COMMIT whose
	// acknowledgement the database connection lost is durable yet
	// surfaces as a generic 500 (only Hoglite raises
	// commit_outcome_unknown). Keyed, the server replays the receipt of a
	// commit it published, so every retry is exactly-once. A server that
	// does not advertise idempotent-append-v1 ignores the key, so against
	// one no key is sent and the unknown outcomes are not retried (below):
	// the pre-key behavior for a lost answer, the safe one for
	// commit_outcome_unknown. The definite 503s retry either way.
	auto keyed = hoglake_catalog.HasCapability(IDEMPOTENT_APPEND);
	if (keyed) {
		request.idempotency_key = UUID::ToString(UUID::GenerateRandomUUID());
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
	// how long one attempt waits for the server's answer: above the
	// server's commit-lock wait (HOGLAKE_COMMIT_LOCK_TIMEOUT_MS, 30 s by
	// default) or a queued commit's clean 503 commit_queue_timeout turns
	// into a lost answer, an unknown outcome
	auto commit_timeout_ms = MaxValue<uint64_t>(
	    MinValue<uint64_t>(GetSettingOrDefault<uint64_t>(context, "hoglake_commit_timeout_ms",
	                                                     HoglakeApiClient::DEFAULT_COMMIT_TIMEOUT_MS),
	                       3600000),
	    1000);
	// the whole loop's wall-time budget, attempts and backoff included:
	// retried read timeouts at commit_timeout_ms each would otherwise
	// hold COMMIT for max_retries × commit_timeout_ms (~22 min at the
	// defaults) against a hung server. No attempt waits past it, and no
	// attempt starts that could not end before it.
	auto deadline_ms = MaxValue<uint64_t>(
	    MinValue<uint64_t>(
	        GetSettingOrDefault<uint64_t>(context, "hoglake_commit_deadline_ms", DEFAULT_COMMIT_DEADLINE_MS), 86400000),
	    1000);
	auto started = std::chrono::steady_clock::now();
	auto elapsed_ms = [&started]() {
		auto elapsed = std::chrono::steady_clock::now() - started;
		return NumericCast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count());
	};

	// Sticky across attempts: once ANY attempt's outcome is unknown the
	// commit may be durable whatever the later attempts said. A definite
	// failure on a retry does not prove the earlier attempt dead — it may
	// still be queued on the server's commit lock and publish after the
	// retry's answer — so the terminal error must never read as "nothing
	// applied" once this is set.
	bool outcome_unknown = false;
	double current_wait = static_cast<double>(wait_ms);
	for (idx_t attempt = 0;; attempt++) {
		// the check before each sleep leaves time on the clock here
		auto remaining_ms = deadline_ms - MinValue<uint64_t>(elapsed_ms(), deadline_ms - 1);
		auto outcome = Api().TryCommit(request, MinValue<uint64_t>(commit_timeout_ms, remaining_ms));
		if (outcome.success) {
			return;
		}
		auto message = StringUtil::Format("hoglake: commit failed: %s%s%s", outcome.error,
		                                  outcome.detail.empty() ? "" : " — ", outcome.detail);
		bool retryable = false;
		// set when this attempt's answer ends the commit on its own,
		// whatever the retry budget says: the suffix when every attempt
		// got a definite answer
		string stop;
		// how the loop ended, for the unknown-outcome advice that replaces
		// `stop` once an attempt got no definite answer
		string ended;
		bool new_transaction = false;
		idx_t sleep_ms = 0;
		if (outcome.status == 409) {
			auto text = StringUtil::Lower(outcome.error + " " + outcome.detail);
			if (outcome.error != "commit_conflict" || StringUtil::Contains(text, RECREATED_MARKER)) {
				stop = " (start a new transaction and re-run the statement)";
				ended = "this refusal needs a new transaction and ended the retries";
				new_transaction = true;
			} else if (has_deletes) {
				stop = " (a conflicting commit superseded this transaction's deletion vectors; re-run the statement)";
				ended = "a conflicting commit superseded this transaction's deletion vectors and ended the retries";
			} else {
				retryable = true;
			}
		} else if (outcome.status == 503) {
			// backpressure (commit_queue_timeout) and a backend that did
			// not answer (backend_unavailable) applied nothing and retry
			// as they are. commit_outcome_unknown may have published, so
			// it retries only under a key the server honors (see above).
			// Honor Retry-After.
			if (outcome.error == "commit_outcome_unknown") {
				outcome_unknown = true;
				retryable = keyed;
			} else {
				retryable = true;
			}
			if (outcome.retry_after_seconds > 0) {
				sleep_ms = MinValue<idx_t>(outcome.retry_after_seconds, 600) * 1000;
			}
		} else if (outcome.status == 0 || outcome.status == 500 || outcome.status == 502 || outcome.status == 504) {
			// the acknowledgement was lost: no answer at all (status 0:
			// connection reset, read timeout), a proxy in front of the
			// server gave up on the request (502/504), or the server's
			// catch-all 500 — on PostgreSQL a lost COMMIT acknowledgement
			// is a durable commit reported as internal_error (server
			// ErrorMapping, the Throwable arm). The server may have
			// published; only the keyed retry is exactly-once.
			outcome_unknown = true;
			retryable = keyed;
			if (outcome.retry_after_seconds > 0) {
				sleep_ms = MinValue<idx_t>(outcome.retry_after_seconds, 600) * 1000;
			}
		}
		if (sleep_ms == 0) {
			current_wait = MinValue<double>(current_wait, MAX_WAIT_MS);
			sleep_ms = static_cast<idx_t>(current_wait);
			current_wait = MinValue<double>(current_wait * backoff, MAX_WAIT_MS);
		}
		if (stop.empty() && retryable && attempt < max_retries && elapsed_ms() + sleep_ms >= deadline_ms) {
			// the next attempt could not start before the deadline
			stop = StringUtil::Format(" (hoglake_commit_deadline_ms=%llu passed)", deadline_ms);
			ended = StringUtil::Format("the commit deadline (hoglake_commit_deadline_ms=%llu) passed", deadline_ms);
		}
		if (!stop.empty() || !retryable || attempt >= max_retries) {
			if (stop.empty() && retryable) {
				stop = " (retries exhausted)";
				ended = "retries exhausted";
			}
			// once any attempt was unknown the user must not re-run the
			// statement blind: the files may already be registered. The
			// advice replaces `stop` so the error gives one instruction
			if (outcome_unknown) {
				throw TransactionException(
				    "%s%s", message,
				    UnknownOutcomeAdvice(Api(), attempt + 1, ended, new_transaction, request.idempotency_key));
			}
			throw TransactionException("%s%s", message, stop);
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
	append_read_snapshot = optional_idx();
}

//===--------------------------------------------------------------------===//
// Schema cache
//===--------------------------------------------------------------------===//

static unique_ptr<HoglakeSchemaEntry> NewSchemaEntry(HoglakeCatalog &catalog, HoglakeTransaction &transaction,
                                                     const string &ns, bool remote) {
	CreateSchemaInfo schema_info;
	schema_info.SetQualifiedName(QualifiedName(schema_info.GetQualifiedName().Catalog(), Identifier(ns),
	                                           schema_info.GetQualifiedName().Name()));
	return make_uniq<HoglakeSchemaEntry>(catalog, schema_info, transaction, remote);
}

void HoglakeTransaction::LoadSchemas() {
	if (schemas_loaded) {
		return;
	}
	// NOTE (wire): /namespaces is head-only — the listing itself is not
	// versioned by the pinned snapshot. Tables ARE fetched at the pinned
	// snapshot. Documented as a server finding in DESIGN.md.
	auto listed = Api().ListNamespaces();
	// group CI-equal names first: a case-colliding namespace pair is
	// unaddressable from DuckDB and must error as ambiguous (same
	// policy as tables), never silently bind first-listed-wins
	case_insensitive_map_t<vector<string>> grouped;
	case_insensitive_map_t<bool> remote;
	for (auto &ns : listed) {
		grouped[ns.name].push_back(ns.name);
		remote[ns.name] = ns.remote;
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
		schemas.emplace(ns, NewSchemaEntry(hoglake_catalog, *this, ns, remote[ns]));
	}
	schemas_loaded = true;
}

optional_ptr<HoglakeSchemaEntry> HoglakeTransaction::GetSchema(const string &name, bool probe) {
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
	// not listed (a `*` caller lists no team schemas): ask once, and let
	// the answer, never the name, say whether the schema is remote
	auto probed = probed_schemas.find(name);
	if (probed == probed_schemas.end()) {
		if (!probe) {
			return nullptr;
		}
		unique_ptr<HoglakeSchemaEntry> schema;
		auto info = Api().TryGetNamespace(name);
		if (info) {
			schema = NewSchemaEntry(hoglake_catalog, *this, name, info->remote);
		}
		probed = probed_schemas.emplace(name, std::move(schema)).first;
	}
	return probed->second.get();
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
