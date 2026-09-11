//===----------------------------------------------------------------------===//
// The hoglake table scan: DuckDB's parquet_scan with a hoglake
// MultiFileReader injected (DuckLake's pattern). The file list comes
// from GET .../scan at the transaction's pinned snapshot.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"
#include "rest/hoglake_api_client.hpp"

namespace duckdb {
class HoglakeTableEntry;
class HoglakeTransaction;

struct HoglakeFunctionInfo : public TableFunctionInfo {
	HoglakeFunctionInfo(HoglakeTableEntry &table, HoglakeTransaction &transaction);

	static shared_ptr<HoglakeFunctionInfo> Create(HoglakeTableEntry &table, HoglakeTransaction &transaction);

	HoglakeTableEntry &table;
	weak_ptr<HoglakeTransaction> transaction;
	string table_name;
	vector<string> column_names;
	vector<LogicalType> column_types;
	//! the travel the scan is planned at (the transaction pin, or the
	//! entry's AT (VERSION/TIMESTAMP) pin)
	HoglakeTravel travel;

	shared_ptr<HoglakeTransaction> GetTransaction();
};

class HoglakeFunctions {
public:
	static TableFunction GetHoglakeScanFunction(DatabaseInstance &instance);
	static unique_ptr<FunctionData> BindHoglakeScan(ClientContext &context, TableFunction &function);
};

} // namespace duckdb
