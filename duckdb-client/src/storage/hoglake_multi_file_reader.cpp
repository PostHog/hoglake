#include "storage/hoglake_multi_file_reader.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "common/hoglake_types.hpp"
#include "storage/hoglake_delete_filter.hpp"
#include "storage/hoglake_multi_file_list.hpp"
#include "storage/hoglake_table_entry.hpp"
#include "storage/hoglake_transaction.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"

namespace duckdb {

HoglakeMultiFileReader::HoglakeMultiFileReader(HoglakeFunctionInfo &read_info_p) : read_info(read_info_p) {
	row_id_column = make_uniq<MultiFileColumnDefinition>("_hog_row_id", LogicalType::BIGINT);
	row_id_column->identifier = Value::INTEGER(HOG_ROW_ID_FIELD_ID);
}

HoglakeMultiFileReader::~HoglakeMultiFileReader() {
}

unique_ptr<MultiFileReader> HoglakeMultiFileReader::Copy() const {
	return make_uniq<HoglakeMultiFileReader>(read_info);
}

unique_ptr<MultiFileReader> HoglakeMultiFileReader::CreateInstance(const TableFunction &table_function) {
	auto &function_info = table_function.function_info->Cast<HoglakeFunctionInfo>();
	return make_uniq<HoglakeMultiFileReader>(function_info);
}

shared_ptr<MultiFileList> HoglakeMultiFileReader::CreateFileList(ClientContext &context, const vector<string> &paths,
                                                                 const FileGlobInput &options) {
	return make_shared_ptr<HoglakeMultiFileList>(read_info);
}

bool HoglakeMultiFileReader::Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
                                  vector<Identifier> &names, MultiFileReaderBindData &bind_data) {
	// global columns: the table's live (flat) columns with parquet field
	// ids as identifiers - schema evolution binds by field id
	auto &wire = read_info.table.GetWireInfo();
	auto columns = wire.columns;
	std::sort(columns.begin(), columns.end(),
	          [](const HoglakeColumn &a, const HoglakeColumn &b) { return a.ordinal < b.ordinal; });
	for (auto &col : columns) {
		MultiFileColumnDefinition column(col.name, HoglakeTypes::ToDuckDBType(col));
		// columns added after a file was written read as NULL from it
		column.default_expression = make_uniq<ConstantExpression>(Value(column.type));
		column.identifier = Value::INTEGER(NumericCast<int32_t>(col.field_id));
		bind_data.schema.push_back(std::move(column));
	}
	bind_data.mapping = MultiFileColumnMappingMode::BY_FIELD_ID;
	names = StringsToIdentifiers(read_info.column_names);
	return_types = read_info.column_types;
	return true;
}

void HoglakeMultiFileReader::BindOptions(MultiFileOptions &options, MultiFileList &files,
                                         vector<LogicalType> &return_types, vector<Identifier> &names,
                                         MultiFileReaderBindData &bind_data) {
}

ReaderInitializeType HoglakeMultiFileReader::InitializeReader(MultiFileReaderData &reader_data,
                                                              const MultiFileBindData &bind_data,
                                                              const vector<MultiFileColumnDefinition> &global_columns,
                                                              const vector<ColumnIndex> &global_column_ids,
                                                              optional_ptr<TableFilterSet> table_filters,
                                                              ClientContext &context, MultiFileGlobalState &gstate) {
	auto &file_list = gstate.file_list.Cast<HoglakeMultiFileList>();
	auto &reader = *reader_data.reader;
	auto file_idx = reader.file_list_idx.GetIndex();
	auto &file_entry = file_list.GetFileEntry(file_idx);

	// read-your-own-DELETES: merge the positions this transaction has
	// already buffered for the file (earlier DELETE/UPDATE statements)
	// into the scan's delete mask — without this, a later DML statement
	// re-reads rows the transaction deleted and COMMITS resurrected /
	// doubled data. AT-clause (travel-pinned) entries stay historical:
	// uncommitted deletes never apply to them.
	set<idx_t> buffered;
	if (!read_info.table.IsTravelPinned()) {
		auto transaction = read_info.GetTransaction();
		auto ns = read_info.table.ParentSchema().name.GetIdentifierName();
		buffered =
		    transaction->GetBufferedDeletePositions(ns, read_info.table_name, file_entry.data_file.data_file_id);
	}
	if (file_entry.has_delete_file || !buffered.empty()) {
		auto delete_filter = make_uniq<HoglakeDeleteFilter>();
		if (file_entry.has_delete_file) {
			delete_filter->Initialize(context, file_entry.delete_file);
		}
		if (!buffered.empty()) {
			delete_filter->MergePositions(buffered);
		}
		reader.deletion_filter = std::move(delete_filter);
	}
	return MultiFileReader::InitializeReader(reader_data, bind_data, global_columns, global_column_ids, table_filters,
	                                         context, gstate);
}

static bool ColumnsHaveFieldIds(const vector<MultiFileColumnDefinition> &columns) {
	for (auto &col : columns) {
		if (col.identifier.IsNull()) {
			return false;
		}
	}
	return !columns.empty();
}

ReaderInitializeType HoglakeMultiFileReader::CreateMapping(
    ClientContext &context, MultiFileReaderData &reader_data, const vector<MultiFileColumnDefinition> &global_columns,
    const vector<ColumnIndex> &global_column_ids, optional_ptr<TableFilterSet> filters, MultiFileList &multi_file_list,
    const MultiFileReaderBindData &bind_data, const virtual_column_map_t &virtual_columns) {
	if (!ColumnsHaveFieldIds(reader_data.reader->columns)) {
		// a registered external file without parquet field ids
		// (hog_data_file.missing_field_ids): the server binds such files
		// by NAME - mirror that here
		return MultiFileReader::CreateMapping(context, reader_data, global_columns, global_column_ids, filters,
		                                      multi_file_list, bind_data, virtual_columns,
		                                      MultiFileColumnMappingMode::BY_NAME);
	}
	return MultiFileReader::CreateMapping(context, reader_data, global_columns, global_column_ids, filters,
	                                      multi_file_list, bind_data, virtual_columns);
}

static bool TryFindColumnByFieldId(const vector<MultiFileColumnDefinition> &local_columns, int32_t field_id) {
	for (auto &col : local_columns) {
		if (col.identifier.IsNull()) {
			continue;
		}
		if (col.identifier.type().id() != LogicalTypeId::INTEGER) {
			continue;
		}
		if (col.identifier.GetValue<int32_t>() == field_id) {
			return true;
		}
	}
	return false;
}

MultiFileReaderVirtualColumnBinding HoglakeMultiFileReader::GetVirtualColumnExpression(
    ClientContext &context, MultiFileReaderData &reader_data, const vector<MultiFileColumnDefinition> &local_columns,
    const idx_t column_id, const LogicalType &type, MultiFileLocalIndex local_idx) {
	if (column_id == COLUMN_IDENTIFIER_ROW_ID) {
		// stable hoglake row id: the physical _hog_row_id column when the
		// file carries one (compaction outputs), else
		// row_id_start + file_row_number
		if (TryFindColumnByFieldId(local_columns, HOG_ROW_ID_FIELD_ID)) {
			return MultiFileReaderVirtualColumnBinding(*row_id_column);
		}
		if (!reader_data.file_to_be_opened.extended_info) {
			throw InternalException("hoglake: extended file info missing for row id column");
		}
		auto &options = reader_data.file_to_be_opened.extended_info->options;
		auto entry = options.find("row_id_start");
		if (entry == options.end()) {
			throw InvalidInputException("hoglake: file \"%s\" has no row_id_start and no _hog_row_id column - "
			                            "row id cannot be read",
			                            reader_data.file_to_be_opened.path);
		}
		auto row_id_expr = make_uniq<BoundConstantExpression>(entry->second.DefaultCastAs(LogicalType::BIGINT));
		auto file_row_number = make_uniq<BoundReferenceExpression>(type, local_idx.GetIndex());
		vector<unique_ptr<Expression>> children;
		children.push_back(std::move(row_id_expr));
		children.push_back(std::move(file_row_number));
		FunctionBinder binder(context);
		ErrorData error;
		auto function_expr =
		    binder.BindScalarFunction(Identifier::DefaultSchema(), "+", std::move(children), error, true, nullptr);
		if (error.HasError()) {
			error.Throw();
		}
		vector<column_t> column_ids;
		column_ids.push_back(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER);
		return MultiFileReaderVirtualColumnBinding(std::move(function_expr), std::move(column_ids));
	}
	if (column_id == COLUMN_IDENTIFIER_SNAPSHOT_ID) {
		if (!reader_data.file_to_be_opened.extended_info) {
			throw InternalException("hoglake: extended file info missing for snapshot id column");
		}
		auto &options = reader_data.file_to_be_opened.extended_info->options;
		auto entry = options.find("snapshot_id");
		if (entry == options.end()) {
			throw InternalException("hoglake: snapshot_id not found for reading snapshot_id column");
		}
		return MultiFileReaderVirtualColumnBinding(entry->second);
	}
	return MultiFileReader::GetVirtualColumnExpression(context, reader_data, local_columns, column_id, type,
	                                                   local_idx);
}

} // namespace duckdb
