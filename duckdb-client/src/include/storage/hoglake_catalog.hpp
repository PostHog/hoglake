//===----------------------------------------------------------------------===//
// HoglakeCatalog: duckdb::Catalog over the hoglake REST control plane.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/attached_database.hpp"
#include "rest/hoglake_api_client.hpp"

namespace duckdb {

struct HoglakeOptions {
	//! server base URL (scheme://host[:port])
	string endpoint;
	//! catalog name on the server
	string catalog_name;
	//! attach-scoped snapshot pin (SNAPSHOT_VERSION); forces read-only
	optional_idx snapshot_version;
	//! attach-scoped timestamp pin (SNAPSHOT_TIME, ISO-8601); forces read-only
	string snapshot_time;
	//! create the catalog on attach when missing (requires data_path)
	bool create_if_not_exists = false;
	string data_path;
	AccessMode access_mode = AccessMode::AUTOMATIC;
};

class HoglakeCatalog : public Catalog {
public:
	HoglakeCatalog(AttachedDatabase &db, HoglakeOptions options);
	~HoglakeCatalog() override;

	string GetCatalogType() override {
		return "hoglake";
	}
	HoglakeApiClient &Api() {
		return *api_client;
	}
	const HoglakeOptions &GetOptions() const {
		return options;
	}
	const string &DataPath() const {
		return data_path;
	}
	//! Attach-scoped pin: valid when SNAPSHOT_VERSION was given.
	optional_idx AttachSnapshotVersion() const {
		return options.snapshot_version;
	}
	const string &AttachSnapshotTime() const {
		return options.snapshot_time;
	}

public:
	void Initialize(bool load_builtin) override;

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;
	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;
	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;

	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	bool InMemory() override;
	string GetDBPath() override;

	bool SupportsTimeTravel() const override {
		return true;
	}

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;

private:
	HoglakeOptions options;
	unique_ptr<HoglakeApiClient> api_client;
	//! catalog data_path as recorded on the server (fetched at attach)
	string data_path;
};

} // namespace duckdb
