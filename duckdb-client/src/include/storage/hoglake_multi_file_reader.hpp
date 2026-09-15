//===----------------------------------------------------------------------===//
// HoglakeMultiFileReader: injects the hoglake file list, field-id
// column mapping, deletion-vector filters, and the row_id/snapshot_id
// virtual columns into DuckDB's parquet scan.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "storage/hoglake_scan.hpp"

namespace duckdb {

struct HoglakeMultiFileReader : public MultiFileReader {
public:
	//! virtual column id for snapshot_id (mirrors ducklake's)
	static constexpr column_t COLUMN_IDENTIFIER_SNAPSHOT_ID = UINT64_C(10000000000000000000);
	//! the reserved parquet field id of the explicit _hog_row_id column
	static constexpr int32_t HOG_ROW_ID_FIELD_ID = 2147483646;

	explicit HoglakeMultiFileReader(HoglakeFunctionInfo &read_info);
	~HoglakeMultiFileReader() override;

	HoglakeFunctionInfo &read_info;

public:
	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &table_function);

	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         const FileGlobInput &options) override;

	bool Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	          vector<Identifier> &names, MultiFileReaderBindData &bind_data) override;

	void BindOptions(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	                 vector<Identifier> &names, MultiFileReaderBindData &bind_data) override;

	ReaderInitializeType InitializeReader(MultiFileReaderData &reader_data, const MultiFileBindData &bind_data,
	                                      const vector<MultiFileColumnDefinition> &global_columns,
	                                      const vector<ColumnIndex> &global_column_ids,
	                                      optional_ptr<TableFilterSet> table_filters, ClientContext &context,
	                                      MultiFileGlobalState &gstate) override;

	ReaderInitializeType CreateMapping(ClientContext &context, MultiFileReaderData &reader_data,
	                                   const vector<MultiFileColumnDefinition> &global_columns,
	                                   const vector<ColumnIndex> &global_column_ids,
	                                   optional_ptr<TableFilterSet> filters, MultiFileList &multi_file_list,
	                                   const MultiFileReaderBindData &bind_data,
	                                   const virtual_column_map_t &virtual_columns) override;

	MultiFileReaderVirtualColumnBinding
	GetVirtualColumnExpression(ClientContext &context, MultiFileReaderData &reader_data,
	                           const vector<MultiFileColumnDefinition> &local_columns, const idx_t column_id,
	                           const LogicalType &type, MultiFileLocalIndex local_index) override;

	unique_ptr<MultiFileReader> Copy() const override;

private:
	//! _hog_row_id as a physical column definition (explicit-row-id files)
	unique_ptr<MultiFileColumnDefinition> row_id_column;
};

} // namespace duckdb
