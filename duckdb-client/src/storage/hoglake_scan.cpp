#include "storage/hoglake_scan.hpp"

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "storage/hoglake_multi_file_list.hpp"
#include "storage/hoglake_multi_file_reader.hpp"
#include "storage/hoglake_table_entry.hpp"
#include "storage/hoglake_transaction.hpp"

namespace duckdb {

static BindInfo HoglakeBindInfo(const optional_ptr<FunctionData> bind_data) {
	auto &multi_file_data = bind_data->Cast<MultiFileBindData>();
	auto &file_list = multi_file_data.file_list->Cast<HoglakeMultiFileList>();
	return BindInfo(file_list.GetTable());
}

static virtual_column_map_t HoglakeVirtualColumns(ClientContext &context, optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<MultiFileBindData>();
	auto &file_list = bind_data.file_list->Cast<HoglakeMultiFileList>();
	auto result = file_list.GetTable().GetVirtualColumns();
	bind_data.virtual_columns = result;
	return result;
}

static vector<column_t> HoglakeGetRowIdColumn(ClientContext &context, optional_ptr<FunctionData> bind_data) {
	vector<column_t> result;
	result.emplace_back(MultiFileReader::COLUMN_IDENTIFIER_FILENAME);
	result.emplace_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER);
	return result;
}

TableFunction HoglakeFunctions::GetHoglakeScanFunction(DatabaseInstance &instance) {
	// grab parquet_scan and inject the hoglake MultiFileReader
	ExtensionHelper::TryAutoLoadExtension(instance, "parquet");
	ExtensionLoader loader(instance, "hoglake");

	TableFunction function("hoglake_scan", {LogicalType::VARCHAR}, nullptr, nullptr);
	auto parquet_entry = loader.TryGetTableFunction("parquet_scan");
	if (!parquet_entry) {
		throw MissingExtensionException("hoglake requires the parquet extension to be loaded");
	}
	auto &parquet_scan = parquet_entry->Cast<TableFunctionCatalogEntry>();
	function = parquet_scan.functions.GetFunctionByOffset(0);
	function.get_multi_file_reader = HoglakeMultiFileReader::CreateInstance;

	function.get_bind_info = HoglakeBindInfo;
	function.get_virtual_columns = HoglakeVirtualColumns;
	function.get_row_id_columns = HoglakeGetRowIdColumn;
	// plan serialization (needed for dependent contexts) is a follow-up;
	// clear the parquet callbacks so we never serialize a hoglake scan
	// as a parquet scan
	function.serialize = nullptr;
	function.deserialize = nullptr;

	function.SetName("hoglake_scan");
	return function;
}

unique_ptr<FunctionData> HoglakeFunctions::BindHoglakeScan(ClientContext &context, TableFunction &function) {
	vector<Value> inputs {Value("")};
	named_parameter_map_t param_map;
	vector<LogicalType> return_types;
	vector<Identifier> input_table_names;
	TableFunctionRef empty_ref;

	TableFunctionBindInput bind_input(inputs, param_map, return_types, input_table_names, nullptr, nullptr, function,
	                                  empty_ref);
	vector<Identifier> bind_names;
	return function.bind(context, bind_input, return_types, bind_names);
}

HoglakeFunctionInfo::HoglakeFunctionInfo(HoglakeTableEntry &table_p, HoglakeTransaction &transaction_p)
    : table(table_p), transaction(transaction_p.shared_from_this()), snapshot_id(0) {
}

shared_ptr<HoglakeFunctionInfo> HoglakeFunctionInfo::Create(HoglakeTableEntry &table, HoglakeTransaction &transaction) {
	auto result = make_shared_ptr<HoglakeFunctionInfo>(table, transaction);
	result->table_name = table.name.GetIdentifierName();
	for (auto &col : table.GetColumns().Logical()) {
		result->column_names.push_back(col.Name().GetIdentifierName());
		result->column_types.push_back(col.Type());
	}
	result->snapshot_id = transaction.GetSnapshot();
	return result;
}

shared_ptr<HoglakeTransaction> HoglakeFunctionInfo::GetTransaction() {
	auto result = transaction.lock();
	if (!result) {
		throw NotImplementedException(
		    "Scanning a hoglake table after the transaction has ended - this use case is not supported");
	}
	return result;
}

} // namespace duckdb
