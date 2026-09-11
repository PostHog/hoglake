#include "storage/hoglake_table_entry.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "storage/hoglake_multi_file_reader.hpp"
#include "storage/hoglake_scan.hpp"
#include "storage/hoglake_transaction.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

namespace duckdb {

HoglakeTableEntry::HoglakeTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info,
                                     HoglakeTableInfo table_info_p, HoglakeTravel read_travel_p,
                                     bool writes_refused_p)
    : TableCatalogEntry(catalog, schema, info), table_info(std::move(table_info_p)),
      read_travel(std::move(read_travel_p)), writes_refused(writes_refused_p) {
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

void HoglakeTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj,
                                              LogicalUpdate &update, ClientContext &context) {
	// all hoglake updates are deletes + inserts (rewrite semantics);
	// project every physical column so the rewrite can materialize the
	// full row (ducklake's approach)
	update.update_is_del_and_insert = true;

	auto &column_ids = get.GetColumnIds();
	for (auto &column : columns.Physical()) {
		auto physical_index = column.Physical();
		bool found = false;
		for (auto &col : update.columns) {
			if (col == physical_index) {
				found = true;
				break;
			}
		}
		if (found) {
			continue;
		}
		optional_idx column_id_index;
		for (idx_t i = 0; i < column_ids.size(); i++) {
			if (column_ids[i].GetPrimaryIndex() == physical_index.index) {
				column_id_index = i;
				break;
			}
		}
		if (!column_id_index.IsValid()) {
			column_id_index = column_ids.size();
			get.AddColumnId(physical_index.index);
		}
		update.expressions.push_back(make_uniq<BoundColumnRefExpression>(
		    column.Type(), ColumnBinding(proj.table_index, ProjectionIndex(proj.expressions.size()))));
		proj.expressions.push_back(make_uniq<BoundColumnRefExpression>(
		    column.Type(), ColumnBinding(get.table_index, ProjectionIndex(column_id_index.GetIndex()))));
		get.AddColumnId(physical_index.index);
		update.columns.push_back(physical_index);
	}
}

TableStorageInfo HoglakeTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo info;
	info.cardinality = NumericCast<idx_t>(table_info.record_count);
	return info;
}

} // namespace duckdb
