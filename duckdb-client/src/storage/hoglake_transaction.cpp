#include "storage/hoglake_transaction.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include <chrono>
#include <thread>
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
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

idx_t HoglakeTransaction::GetSnapshot() {
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

HoglakeTravel HoglakeTransaction::Travel() {
	if (!hoglake_catalog.AttachSnapshotTime().empty()) {
		HoglakeTravel travel;
		travel.at_timestamp = hoglake_catalog.AttachSnapshotTime();
		return travel;
	}
	return HoglakeTravel::AtSnapshot(GetSnapshot());
}

void HoglakeTransaction::AddAppend(const string &ns, const string &table, const string &expected_table_uuid,
                                   vector<HoglakeFileRegistration> files) {
	if (files.empty()) {
		return;
	}
	for (auto &append : buffered_appends) {
		if (append.namespace_name == ns && append.table_name == table) {
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
	for (auto &deletes : buffered_deletes) {
		if (deletes.namespace_name == ns && deletes.table_name == table) {
			for (auto &file : files) {
				deletes.files.push_back(std::move(file));
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
	if (buffered_appends.empty() && buffered_deletes.empty()) {
		return;
	}
	HoglakeCommitRequest request;
	request.appends = std::move(buffered_appends);
	request.deletes = std::move(buffered_deletes);
	buffered_appends.clear();
	buffered_deletes.clear();
	// append-only commits are blind (no read_snapshot: no conflict
	// window; the incarnation guard rides expected_table_uuid). Deletes
	// REQUIRE a read_snapshot (they always conflict-check) — and a
	// delete conflict is never auto-retried: the superseded deletion
	// vector must be rebuilt against the new state, so the statement
	// has to be re-run.
	bool has_deletes = !request.deletes.empty();
	if (has_deletes) {
		request.read_snapshot = GetSnapshot();
	}

	auto max_retries = GetSettingOrDefault<uint64_t>(context, "hoglake_max_retry_count", 10);
	auto wait_ms = GetSettingOrDefault<uint64_t>(context, "hoglake_retry_wait_ms", 100);
	auto backoff = GetSettingOrDefault<double>(context, "hoglake_retry_backoff", 1.5);

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
			sleep_ms = static_cast<idx_t>(current_wait);
			current_wait *= backoff;
		} else if (outcome.status == 503) {
			// explicit commit backpressure; honor Retry-After
			retryable = true;
			sleep_ms = outcome.retry_after_seconds > 0 ? outcome.retry_after_seconds * 1000
			                                           : static_cast<idx_t>(current_wait);
			current_wait *= backoff;
		}
		if (!retryable || attempt >= max_retries) {
			throw TransactionException("%s%s", message,
			                           retryable ? " (retries exhausted)" : "");
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
	}
}

void HoglakeTransaction::Rollback() {
	// buffered registrations are dropped; parquet/puffin already
	// uploaded for them is orphaned (cleanup's problem, never the
	// catalog's)
	buffered_appends.clear();
	buffered_deletes.clear();
}

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
	LoadSchemas();
	for (auto &entry : schemas) {
		callback(*entry.second);
	}
}

HoglakeSchemaEntry &HoglakeTransaction::AddSchema(unique_ptr<HoglakeSchemaEntry> schema) {
	auto name = schema->name.GetIdentifierName();
	auto &result = *schema;
	schemas[name] = std::move(schema);
	return result;
}

void HoglakeTransaction::EraseSchema(const string &name) {
	schemas.erase(name);
}

} // namespace duckdb
