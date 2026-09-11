#include "storage/hoglake_table_entry.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "storage/hoglake_multi_file_reader.hpp"
#include "storage/hoglake_scan.hpp"
#include "storage/hoglake_transaction.hpp"

namespace duckdb {

HoglakeTableEntry::HoglakeTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                     HoglakeTableInfo table_info_p)
    : TableCatalogEntry(catalog, schema, info), table_info(std::move(table_info_p)) {
}

unique_ptr<BaseStatistics> HoglakeTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	// no per-column global stats on the wire (only per-file stats,
	// which are not exposed by /scan) — nothing to report
	return nullptr;
}

TableFunction HoglakeTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	auto function = HoglakeFunctions::GetHoglakeScanFunction(*context.db);
	auto &transaction = HoglakeTransaction::Get(context, ParentCatalog());
	function.function_info = HoglakeFunctionInfo::Create(*this, transaction);

	bind_data = HoglakeFunctions::BindHoglakeScan(context, function);
	auto &multi_file_bind_data = bind_data->Cast<MultiFileBindData>();
	multi_file_bind_data.virtual_columns = GetVirtualColumns();
	return function;
}

virtual_column_map_t HoglakeTableEntry::GetVirtualColumns() const {
	virtual_column_map_t result;
	result.insert(make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILENAME, TableColumn("filename", LogicalType::VARCHAR)));
	result.insert(make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER,
	                        TableColumn("file_row_number", LogicalType::BIGINT)));
	result.insert(
	    make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX, TableColumn("file_index", LogicalType::UBIGINT)));
	result.insert(make_pair(COLUMN_IDENTIFIER_ROW_ID, TableColumn("rowid", LogicalType::BIGINT)));
	result.insert(make_pair(HoglakeMultiFileReader::COLUMN_IDENTIFIER_SNAPSHOT_ID,
	                        TableColumn("snapshot_id", LogicalType::BIGINT)));
	result.insert(make_pair(COLUMN_IDENTIFIER_EMPTY, TableColumn("", LogicalType::BOOLEAN)));
	return result;
}

vector<column_t> HoglakeTableEntry::GetRowIdColumns() const {
	vector<column_t> result;
	result.push_back(COLUMN_IDENTIFIER_ROW_ID);
	result.push_back(MultiFileReader::COLUMN_IDENTIFIER_FILENAME);
	result.push_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_INDEX);
	result.push_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER);
	return result;
}

TableStorageInfo HoglakeTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo info;
	info.cardinality = NumericCast<idx_t>(table_info.record_count);
	return info;
}

} // namespace duckdb
