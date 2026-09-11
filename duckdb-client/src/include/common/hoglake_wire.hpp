//===----------------------------------------------------------------------===//
// hoglake wire DTOs — mirror the schemas of openapi/hoglake.yaml.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/value.hpp"

namespace duckdb {

struct HoglakeCatalogInfo {
	string name;
	string data_path;
	int64_t head_snapshot_id = 0;
	int64_t schema_version = 0;
	//! null when expiry never advanced the floor
	string earliest_snapshot_time;
};

//! Column (wire): ColumnDef + field_id + ordinal.
struct HoglakeColumn {
	string name;
	//! hoglake type enum string (boolean,int,long,float,double,decimal,
	//! date,time,timestamp,timestamptz,string,uuid,binary)
	string type;
	//! decimal only: type_params.precision / .scale
	int32_t precision = 0;
	int32_t scale = 0;
	bool nullable = true;
	int64_t field_id = 0;
	int32_t ordinal = 0;
};

struct HoglakePartitionField {
	int64_t source_field_id = 0;
	//! identity, bucket, year, month, day, hour
	string transform;
	//! bucket(n) only
	int32_t transform_param = 0;
};

struct HoglakePartitionSpec {
	int64_t spec_id = 0;
	vector<HoglakePartitionField> fields;
};

struct HoglakeSortField {
	int64_t source_field_id = 0;
	//! asc | desc
	string direction;
	//! nulls_first | nulls_last
	string null_order;
};

struct HoglakeSortSpec {
	int64_t sort_id = 0;
	vector<HoglakeSortField> fields;
};

struct HoglakeTableInfo {
	string name;
	string namespace_name;
	string table_uuid;
	vector<HoglakeColumn> columns;
	int64_t record_count = 0;
	int64_t file_count = 0;
	int64_t file_size_bytes = 0;
	bool has_partition_spec = false;
	HoglakePartitionSpec partition_spec;
	bool has_sort_spec = false;
	HoglakeSortSpec sort_spec;
};

struct HoglakeTableSummary {
	string name;
	string table_uuid;
};

struct HoglakeViewInfo {
	string name;
	string namespace_name;
	string view_uuid;
	string dialect;
	string sql;
};

struct HoglakeCommitResult {
	int64_t snapshot_id = 0;
	int64_t schema_version = 0;
};

//! DataFile (wire) — scan planning unit.
struct HoglakeDataFile {
	int64_t data_file_id = 0;
	string path;
	string file_format;
	int64_t record_count = 0;
	int64_t file_size_bytes = 0;
	int64_t footer_size = 0;
	int64_t row_id_start = 0;
	//! provided | pending | failed
	string stats_state;
	int64_t begin_snapshot = 0;
	optional_idx spec_id;
	//! transformed partition values by key_index; entries may be null
	vector<Value> partition_values;
	//! compaction outputs: row ids ride the _hog_row_id column
	bool explicit_row_ids = false;
};

//! DeleteFile (wire) — a live puffin deletion vector over one data file.
struct HoglakeDeleteFile {
	int64_t delete_file_id = 0;
	int64_t data_file_id = 0;
	string path;
	string file_format;
	int64_t delete_count = 0;
	int64_t file_size_bytes = 0;
	int64_t begin_snapshot = 0;
};

struct HoglakeScanFile {
	HoglakeDataFile data_file;
	bool has_delete_file = false;
	HoglakeDeleteFile delete_file;
};

struct HoglakeSnapshotChange {
	string kind;
	int64_t object_id = 0;
};

struct HoglakeSnapshotInfo {
	int64_t snapshot_id = 0;
	//! ISO-8601 instant
	string snapshot_time;
	int64_t schema_version = 0;
	string author;
	string message;
	vector<HoglakeSnapshotChange> changes;
};

//! FileRegistration (client -> server): one client-written parquet
//! file in a footer-shipping commit. column_stats omitted for now
//! (stats_mode=deferred: the server hydrator reads footers async).
struct HoglakeFileRegistration {
	string path;
	int64_t record_count = 0;
	int64_t file_size_bytes = 0;
	optional_idx footer_size;
	//! set when the table is partitioned: transformed values by
	//! key_index (VARCHAR values; null partition value = NULL Value)
	bool has_partition_values = false;
	vector<Value> partition_values;
};

//! TableAppend (client -> server).
struct HoglakeTableAppend {
	string namespace_name;
	string table_name;
	//! incarnation guard; empty = name-only resolution
	string expected_table_uuid;
	vector<HoglakeFileRegistration> files;
};

//! TableDeletes file entry (client -> server): a puffin DV superseding
//! the data file's current one (vectors only grow).
struct HoglakeDeleteFileRegistration {
	int64_t data_file_id = 0;
	string path;
	//! TOTAL deleted positions in the DV (cumulative)
	int64_t delete_count = 0;
	int64_t file_size_bytes = 0;
};

struct HoglakeTableDeletes {
	string namespace_name;
	string table_name;
	string expected_table_uuid;
	vector<HoglakeDeleteFileRegistration> files;
};

//! CommitRequest (client -> server). read_snapshot is REQUIRED when
//! deletes are present (deletes always conflict-check).
struct HoglakeCommitRequest {
	//! invalid = blind append (no conflict window)
	optional_idx read_snapshot;
	vector<HoglakeTableAppend> appends;
	vector<HoglakeTableDeletes> deletes;
	string author;
	string message;
};

//! Outcome of a commit attempt (the retry loop decides what to do).
struct HoglakeCommitOutcome {
	bool success = false;
	int status = 0;
	HoglakeCommitResult result;
	string error;
	string detail;
	//! Retry-After (503 backpressure); 0 when absent
	idx_t retry_after_seconds = 0;
};

//! CreateTableRequest column def (client -> server; no field_id yet).
struct HoglakeColumnDef {
	string name;
	string type;
	int32_t precision = 0;
	int32_t scale = 0;
	bool nullable = true;
};

//! AlterOp (client -> server): one typed schema-evolution op;
//! discriminated by `op` per the OpenAPI AlterOp schema.
struct HoglakeAlterOp {
	//! add_column, drop_column, rename_column, promote_column,
	//! rename_table, set_partition_spec, set_sort_order
	string op;
	//! add_column
	HoglakeColumnDef column;
	//! drop_column, promote_column
	string name;
	//! rename_column
	string from;
	//! rename_column target / promote_column target type
	string to;
	//! rename_table
	string new_name;
	//! set_partition_spec ([] = unpartitioned)
	vector<HoglakePartitionField> fields;
	//! set_sort_order ([] = unsorted)
	vector<HoglakeSortField> sort_fields;
};


} // namespace duckdb
