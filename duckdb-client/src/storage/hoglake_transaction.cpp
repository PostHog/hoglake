#include "storage/hoglake_transaction.hpp"

#include "duckdb/common/exception.hpp"
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

void HoglakeTransaction::Commit() {
	// M1: DDL is eager (each server DDL call is its own snapshot) and
	// the write path is not implemented yet — nothing to flush.
}

void HoglakeTransaction::Rollback() {
	// nothing buffered yet (see Commit)
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
