//===----------------------------------------------------------------------===//
// HoglakeTableEntry: a hoglake table bound at the transaction's pinned
// snapshot. Carries the wire identity (table_uuid, field ids, specs)
// the scan/write paths need.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "common/hoglake_wire.hpp"
#include "rest/hoglake_api_client.hpp"

namespace duckdb {
class HoglakeCatalog;

class HoglakeTableEntry : public TableCatalogEntry {
public:
	HoglakeTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
	                  HoglakeTableInfo table_info, HoglakeTravel travel = HoglakeTravel(), bool travel_pinned = false);

	const HoglakeTableInfo &GetWireInfo() const {
		return table_info;
	}
	const string &GetTableUUID() const {
		return table_info.table_uuid;
	}
	//! set for entries bound via AT (VERSION/TIMESTAMP): reads plan at
	//! this travel and writes/DDL are refused
	const HoglakeTravel &GetTravel() const {
		return travel;
	}
	bool IsTravelPinned() const {
		return travel_pinned;
	}

public:
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id) override;
	TableFunction GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) override;
	TableStorageInfo GetStorageInfo(ClientContext &context) override;
	virtual_column_map_t GetVirtualColumns() const override;
	vector<column_t> GetRowIdColumns() const override;
	void BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj, LogicalUpdate &update,
	                           ClientContext &context) override;

private:
	HoglakeTableInfo table_info;
	HoglakeTravel travel;
	bool travel_pinned = false;
};

} // namespace duckdb
