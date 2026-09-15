#include "storage/hoglake_multi_file_list.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/optimizer/filter_combiner.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/types/blob.hpp"
#include "common/hoglake_types.hpp"
#include "storage/hoglake_table_entry.hpp"
#include "storage/hoglake_transaction.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Partition pruning
//===--------------------------------------------------------------------===//

namespace {

struct PartitionKeyFilter {
	//! index into a file's partition_values (key_index of the live spec)
	idx_t key_index;
	//! the source column's type (partition values are wire strings)
	LogicalType type;
	//! the pushed-down filter over the source column
	reference<TableFilter> filter;
};

//! Evaluate `filter` against a single (identity-transformed) partition
//! value: build filter.ToExpression(constant) and constant-fold it.
//! Returns true when the file can be PRUNED (filter definitely false
//! for every row of the file); any doubt keeps the file.
bool CanPruneOnValue(ClientContext &context, TableFilter &filter, const Value &partition_value) {
	try {
		BoundConstantExpression value_expr(partition_value);
		auto filter_expr = filter.ToExpression(value_expr);
		if (!filter_expr) {
			return false;
		}
		Value result;
		if (!ExpressionExecutor::TryEvaluateScalar(context, *filter_expr, result)) {
			return false;
		}
		if (result.IsNull()) {
			// filter evaluates to NULL for the partition value -> no
			// row in the file can pass
			return true;
		}
		auto as_bool = result.DefaultCastAs(LogicalType::BOOLEAN);
		return !BooleanValue::Get(as_bool);
	} catch (...) {
		// never let pruning break a scan - just read the file
		return false;
	}
}

} // namespace

unique_ptr<MultiFileList> HoglakeMultiFileList::ComplexFilterPushdown(ClientContext &context,
                                                                      const MultiFileOptions &options,
                                                                      MultiFilePushdownInfo &info,
                                                                      vector<unique_ptr<Expression>> &filters) const {
	if (filters.empty()) {
		return nullptr;
	}
	auto &wire = read_info.table.GetWireInfo();
	if (!wire.has_partition_spec || wire.partition_spec.fields.empty()) {
		return nullptr;
	}
	// identity-transform partition sources: field_id -> key_index
	unordered_map<int64_t, idx_t> identity_keys;
	for (idx_t key_index = 0; key_index < wire.partition_spec.fields.size(); key_index++) {
		auto &field = wire.partition_spec.fields[key_index];
		if (field.transform == "identity") {
			identity_keys.emplace(field.source_field_id, key_index);
		}
	}
	if (identity_keys.empty()) {
		return nullptr;
	}

	// combine the filter expressions into per-column table filters
	FilterCombiner combiner(context);
	for (auto &filter : filters) {
		combiner.AddFilter(filter->Copy());
	}
	vector<FilterPushdownResult> pushdown_results;
	auto table_filter_set = combiner.GenerateTableScanFilters(info.column_indexes, pushdown_results);
	if (!table_filter_set.HasFilters()) {
		return nullptr;
	}

	// bind-schema order = wire columns sorted by ordinal
	auto columns = wire.columns;
	std::sort(columns.begin(), columns.end(),
	          [](const HoglakeColumn &a, const HoglakeColumn &b) { return a.ordinal < b.ordinal; });

	vector<PartitionKeyFilter> key_filters;
	for (auto &entry : table_filter_set) {
		auto scan_idx = entry.GetIndex().GetIndex();
		if (scan_idx >= info.column_ids.size()) {
			continue;
		}
		auto column_id = info.column_ids[scan_idx];
		if (column_id >= columns.size()) {
			continue; // virtual column (rowid etc.)
		}
		auto &column = columns[column_id];
		auto key_entry = identity_keys.find(column.field_id);
		if (key_entry == identity_keys.end()) {
			continue;
		}
		PartitionKeyFilter key_filter {key_entry->second, HoglakeTypes::ToDuckDBType(column), entry.Filter()};
		key_filters.push_back(key_filter);
	}
	if (key_filters.empty()) {
		return nullptr;
	}

	auto live_spec_id = wire.partition_spec.spec_id;
	auto &all_files = GetFiles();
	vector<HoglakeScanFile> kept;
	for (auto &file : all_files) {
		auto &data_file = file.data_file;
		bool prune = false;
		// only files written under the live spec have interpretable
		// partition_values for these key indexes
		if (data_file.spec_id.IsValid() && NumericCast<int64_t>(data_file.spec_id.GetIndex()) == live_spec_id) {
			for (auto &key_filter : key_filters) {
				if (key_filter.key_index >= data_file.partition_values.size()) {
					continue;
				}
				auto &wire_value = data_file.partition_values[key_filter.key_index];
				Value typed_value;
				if (wire_value.IsNull()) {
					typed_value = Value(key_filter.type);
				} else if (key_filter.type.id() == LogicalTypeId::BLOB) {
					// pyhoglake's wire encoding for binary identity
					// partition values is BASE64 (transforms.wire_string);
					// a VARCHAR->BLOB cast would keep the base64 TEXT
					// bytes and mis-prune. Decode; fail open on garbage.
					try {
						auto &b64 = StringValue::Get(wire_value);
						string_t b64_str(b64.c_str(), NumericCast<uint32_t>(b64.size()));
						auto decoded_size = Blob::FromBase64Size(b64_str);
						auto decoded = make_unsafe_uniq_array<data_t>(decoded_size);
						Blob::FromBase64(b64_str, decoded.get(), decoded_size);
						typed_value = Value::BLOB(decoded.get(), decoded_size);
					} catch (...) {
						continue;
					}
				} else if (!wire_value.DefaultTryCastAs(key_filter.type, typed_value, nullptr)) {
					continue;
				}
				if (CanPruneOnValue(context, key_filter.filter.get(), typed_value)) {
					prune = true;
					break;
				}
			}
		}
		if (!prune) {
			kept.push_back(file);
		}
	}
	if (kept.size() == all_files.size()) {
		return nullptr;
	}
	return make_uniq<HoglakeMultiFileList>(read_info, std::move(kept));
}

HoglakeMultiFileList::HoglakeMultiFileList(HoglakeFunctionInfo &read_info_p) : read_info(read_info_p) {
}

HoglakeMultiFileList::HoglakeMultiFileList(HoglakeFunctionInfo &read_info_p, vector<HoglakeScanFile> files_p)
    : read_info(read_info_p), files(std::move(files_p)), read_file_list(true) {
}

HoglakeTableEntry &HoglakeMultiFileList::GetTable() {
	return read_info.table;
}

void HoglakeMultiFileList::LoadFileList() const {
	if (read_file_list) {
		return;
	}
	auto transaction = read_info.GetTransaction();
	auto &table = read_info.table;
	auto ns = table.ParentSchema().name.GetIdentifierName();
	files = transaction->Api().PlanScan(ns, read_info.table_name, read_info.travel);
	read_file_list = true;
}

const vector<HoglakeScanFile> &HoglakeMultiFileList::GetFiles() const {
	lock_guard<mutex> guard(file_lock);
	LoadFileList();
	return files;
}

const HoglakeScanFile &HoglakeMultiFileList::GetFileEntry(idx_t file_idx) const {
	auto &file_list = GetFiles();
	if (file_idx >= file_list.size()) {
		throw InternalException("hoglake: file index out of range in scan plan");
	}
	return file_list[file_idx];
}

vector<OpenFileInfo> HoglakeMultiFileList::GetAllFiles() const {
	vector<OpenFileInfo> result;
	auto count = GetFiles().size();
	for (idx_t i = 0; i < count; i++) {
		result.push_back(GetFile(i));
	}
	return result;
}

FileExpandResult HoglakeMultiFileList::GetExpandResult() const {
	auto count = GetFiles().size();
	if (count > 1) {
		return FileExpandResult::MULTIPLE_FILES;
	}
	return count == 1 ? FileExpandResult::SINGLE_FILE : FileExpandResult::NO_FILES;
}

idx_t HoglakeMultiFileList::GetTotalFileCount() const {
	return GetFiles().size();
}

unique_ptr<NodeStatistics> HoglakeMultiFileList::GetCardinality(ClientContext &context) const {
	idx_t total = 0;
	for (auto &entry : GetFiles()) {
		auto records = NumericCast<idx_t>(entry.data_file.record_count);
		idx_t deletes = entry.has_delete_file ? NumericCast<idx_t>(entry.delete_file.delete_count) : 0;
		total += records - MinValue<idx_t>(records, deletes);
	}
	return make_uniq<NodeStatistics>(total);
}

OpenFileInfo HoglakeMultiFileList::GetFile(idx_t i) const {
	auto &file_list = GetFiles();
	if (i >= file_list.size()) {
		return OpenFileInfo();
	}
	auto &entry = file_list[i];
	auto &file = entry.data_file;
	OpenFileInfo result(file.path);
	auto extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
	extended_info->options["file_size"] = Value::UBIGINT(NumericCast<idx_t>(file.file_size_bytes));
	if (file.footer_size > 0) {
		extended_info->options["footer_size"] = Value::UBIGINT(NumericCast<idx_t>(file.footer_size));
	}
	// THE WIRE decides the rowid source, never the file's own field ids:
	// for explicit-row-id files row_id_start has no positional meaning
	// and the reader takes ids from the physical _hog_row_id column;
	// for positional files the reserved field id must not appear at all
	extended_info->options["explicit_row_ids"] = Value::BOOLEAN(file.explicit_row_ids);
	if (!file.explicit_row_ids) {
		extended_info->options["row_id_start"] = Value::UBIGINT(NumericCast<idx_t>(file.row_id_start));
	}
	extended_info->options["snapshot_id"] = Value::BIGINT(file.begin_snapshot);
	// hoglake data files are immutable once registered - cache freely
	extended_info->options["validate_external_file_cache"] = Value::BOOLEAN(false);
	extended_info->options["etag"] = Value("");
	extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
	if (entry.has_delete_file) {
		extended_info->options["has_deletes"] = Value::BOOLEAN(true);
	}
	result.extended_info = std::move(extended_info);
	return result;
}

unique_ptr<MultiFileList> HoglakeMultiFileList::Copy() const {
	auto result = make_uniq<HoglakeMultiFileList>(read_info, GetFiles());
	return std::move(result);
}

} // namespace duckdb
