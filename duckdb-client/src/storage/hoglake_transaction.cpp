#include "storage/hoglake_transaction.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
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
		auto version = at_clause->GetValue().DefaultCastAs(LogicalType::BIGINT);
		return HoglakeTravel::AtSnapshot(NumericCast<idx_t>(BigIntValue::Get(version)));
	}
	if (unit == "timestamp") {
		// server wants an ISO-8601 instant WITH offset; normalize the
		// TIMESTAMPTZ to a canonical UTC instant string
		auto utc = at_clause->GetValue().DefaultCastAs(LogicalType::TIMESTAMP);
		HoglakeTravel travel;
		travel.at_timestamp = HoglakeTypes::CanonicalTimestamp(TimestampValue::Get(utc)) + "Z";
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
	for (auto &ns : names) {
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
	auto entry = schemas.find(name);
	if (entry != schemas.end()) {
		return entry->second.get();
	}
	if (schemas_loaded) {
		return nullptr;
	}
	LoadSchemas();
	entry = schemas.find(name);
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
