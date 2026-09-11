#include "storage/hoglake_insert.hpp"

#include "duckdb/catalog/catalog_entry/copy_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/copy_info.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "common/hoglake_types.hpp"
#include "storage/hoglake_catalog.hpp"
#include "storage/hoglake_schema_entry.hpp"
#include "storage/hoglake_table_entry.hpp"
#include "storage/hoglake_transaction.hpp"

namespace duckdb {

HoglakeInsert::HoglakeInsert(PhysicalPlan &physical_plan, const vector<LogicalType> &types, HoglakeTableEntry &table_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, types, 1), table(table_p) {
}

HoglakeInsertGlobalState::HoglakeInsertGlobalState(HoglakeTableEntry &table_p) : table(table_p) {
	for (auto &constraint : table.GetConstraints()) {
		if (constraint->type != ConstraintType::NOT_NULL) {
			continue;
		}
		auto &nn = constraint->Cast<NotNullConstraint>();
		not_null_columns.insert(table.GetColumn(LogicalIndex(nn.index.index)).Name().GetIdentifierName());
	}
}

unique_ptr<GlobalSinkState> HoglakeInsert::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<HoglakeInsertGlobalState>(table);
}

string HoglakeInsert::GetName() const {
	return "HOGLAKE_INSERT";
}

//===--------------------------------------------------------------------===//
// Wire partition-value encoding (matches pyhoglake transforms.wire_string
// for identity transforms; partitions written by different clients must
// land in the same string groups)
//===--------------------------------------------------------------------===//

static Value IdentityWireString(const HoglakeColumn &column, const Value &value) {
	if (value.IsNull()) {
		return Value(LogicalType::VARCHAR);
	}
	auto &type = column.type;
	if (type == "boolean") {
		return Value(BooleanValue::Get(value.DefaultCastAs(LogicalType::BOOLEAN)) ? "true" : "false");
	}
	if (type == "int" || type == "long") {
		return Value(to_string(BigIntValue::Get(value.DefaultCastAs(LogicalType::BIGINT))));
	}
	if (type == "string") {
		return Value(StringValue::Get(value.DefaultCastAs(LogicalType::VARCHAR)));
	}
	if (type == "date") {
		return Value(value.DefaultCastAs(LogicalType::VARCHAR).ToString());
	}
	if (type == "timestamp") {
		// pyhoglake uses datetime.isoformat(): 'T' separator
		auto str = value.DefaultCastAs(LogicalType::VARCHAR).ToString();
		return Value(StringUtil::Replace(str, " ", "T"));
	}
	throw NotImplementedException(
	    "hoglake: INSERT into a table identity-partitioned on a %s column is not supported yet "
	    "(wire partition-value encoding for this type is not implemented)",
	    type);
}

//===--------------------------------------------------------------------===//
// Sink: consume WRITTEN_FILE_STATISTICS rows from the child copy
//===--------------------------------------------------------------------===//

SinkResultType HoglakeInsert::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<HoglakeInsertGlobalState>();
	auto &wire = gstate.table.GetWireInfo();

	// identity partition sources by key_index (plan-time enforced: all
	// live partition fields are identity when we get here)
	vector<HoglakeColumn> key_columns;
	if (wire.has_partition_spec) {
		for (auto &field : wire.partition_spec.fields) {
			for (auto &col : wire.columns) {
				if (col.field_id == field.source_field_id) {
					key_columns.push_back(col);
				}
			}
		}
	}

	for (idx_t r = 0; r < chunk.size(); r++) {
		HoglakeFileRegistration file;
		file.path = chunk.GetValue(0, r).GetValue<string>();
		file.record_count = NumericCast<int64_t>(chunk.GetValue(1, r).GetValue<idx_t>());
		file.file_size_bytes = NumericCast<int64_t>(chunk.GetValue(2, r).GetValue<idx_t>());
		auto footer_size = chunk.GetValue(3, r);
		if (!footer_size.IsNull()) {
			file.footer_size = footer_size.GetValue<idx_t>();
		}
		if (file.record_count == 0) {
			continue;
		}

		// client-side NOT NULL verification from the written null counts
		auto column_stats = chunk.GetValue(4, r);
		if (!column_stats.IsNull() && !gstate.not_null_columns.empty()) {
			for (auto &entry : MapValue::GetChildren(column_stats)) {
				auto &entry_children = StructValue::GetChildren(entry);
				auto col_name = StringValue::Get(entry_children[0]);
				// stats keys are quoted column paths (e.g. "id"); unquote
				// the flat top-level name
				if (col_name.size() >= 2 && col_name.front() == '"' && col_name.back() == '"') {
					col_name = StringUtil::Replace(col_name.substr(1, col_name.size() - 2), "\"\"", "\"");
				}
				if (!gstate.not_null_columns.count(col_name)) {
					continue;
				}
				for (auto &stat : MapValue::GetChildren(entry_children[1])) {
					auto &stat_children = StructValue::GetChildren(stat);
					if (StringValue::Get(stat_children[0]) == "null_count" &&
					    StringUtil::ToUnsigned(StringValue::Get(stat_children[1])) > 0) {
						throw ConstraintException("NOT NULL constraint failed: %s.%s",
						                          gstate.table.name.GetIdentifierName(), col_name);
					}
				}
			}
		}

		// partition values from the copy's partition keys, re-encoded to
		// the wire convention, ordered by the live spec's key_index
		if (wire.has_partition_spec && !wire.partition_spec.fields.empty()) {
			auto partition_info = chunk.GetValue(5, r);
			case_insensitive_map_t<Value> key_values;
			if (!partition_info.IsNull()) {
				for (auto &entry : MapValue::GetChildren(partition_info)) {
					auto &entry_children = StructValue::GetChildren(entry);
					key_values[StringValue::Get(entry_children[0])] = entry_children[1];
				}
			}
			file.has_partition_values = true;
			for (auto &key_column : key_columns) {
				auto entry = key_values.find(key_column.name);
				if (entry == key_values.end()) {
					throw InternalException("hoglake: partition column \"%s\" missing from written-file "
					                        "partition keys",
					                        key_column.name);
				}
				auto &raw = entry->second;
				if (raw.IsNull() || StringValue::Get(raw) == "__HIVE_DEFAULT_PARTITION__") {
					file.partition_values.push_back(Value(LogicalType::VARCHAR));
					continue;
				}
				// hive value string -> typed value -> wire string
				auto typed = raw.DefaultCastAs(HoglakeTypes::ToDuckDBType(key_column));
				file.partition_values.push_back(IdentityWireString(key_column, typed));
			}
		}

		gstate.total_insert_count += NumericCast<idx_t>(file.record_count);
		gstate.written_files.push_back(std::move(file));
	}
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType HoglakeInsert::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                         OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<HoglakeInsertGlobalState>();
	auto &transaction = HoglakeTransaction::Get(context, gstate.table.ParentCatalog());
	auto ns = gstate.table.ParentSchema().name.GetIdentifierName();
	transaction.AddAppend(ns, gstate.table.name.GetIdentifierName(), gstate.table.GetTableUUID(),
	                      std::move(gstate.written_files));
	return SinkFinalizeType::READY;
}

SourceResultType HoglakeInsert::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<HoglakeInsertGlobalState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.total_insert_count)));
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Plan
//===--------------------------------------------------------------------===//

static Value WrittenFieldIds(const vector<HoglakeColumn> &columns) {
	child_list_t<Value> values;
	for (auto &col : columns) {
		values.emplace_back(col.name, Value::BIGINT(col.field_id));
	}
	return Value::STRUCT(std::move(values));
}

PhysicalOperator &HoglakeInsert::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner,
                                            HoglakeTableEntry &table, optional_ptr<PhysicalOperator> plan) {
	auto &catalog = table.ParentCatalog().Cast<HoglakeCatalog>();
	auto &wire = table.GetWireInfo();

	auto columns = wire.columns;
	std::sort(columns.begin(), columns.end(),
	          [](const HoglakeColumn &a, const HoglakeColumn &b) { return a.ordinal < b.ordinal; });

	// partition columns: identity transforms only for now
	vector<idx_t> partition_columns;
	if (wire.has_partition_spec) {
		for (auto &field : wire.partition_spec.fields) {
			if (field.transform != "identity") {
				throw NotImplementedException(
				    "hoglake: INSERT into a table partitioned with the %s transform is not supported yet "
				    "(only identity partitioning; use pyhoglake for transformed partitions)",
				    field.transform);
			}
			bool found = false;
			for (idx_t col_idx = 0; col_idx < columns.size(); col_idx++) {
				if (columns[col_idx].field_id == field.source_field_id) {
					partition_columns.push_back(col_idx);
					found = true;
					break;
				}
			}
			if (!found) {
				throw InternalException("hoglake: partition source field %lld is not a live column",
				                        field.source_field_id);
			}
		}
	}

	// bind the parquet copy with the table's field ids
	auto info = make_uniq<CopyInfo>();
	auto data_path = catalog.DataPath();
	if (!data_path.empty() && data_path.back() != '/') {
		data_path += "/";
	}
	auto ns = table.ParentSchema().name.GetIdentifierName();
	auto file_path = data_path + "data/" + ns + "/" + table.name.GetIdentifierName();
	info->file_path = file_path;
	info->format = "parquet";
	info->is_from = false;
	vector<Value> field_input;
	field_input.push_back(WrittenFieldIds(columns));
	info->options["field_ids"] = std::move(field_input);

	auto &copy_entry =
	    Catalog::GetEntry<CopyFunctionCatalogEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, Identifier("parquet"));
	auto copy_function = copy_entry.function;

	vector<string> names_to_write;
	vector<LogicalType> types_to_write;
	for (auto &col : columns) {
		names_to_write.push_back(col.name);
		types_to_write.push_back(HoglakeTypes::ToDuckDBType(col));
	}

	CopyFunctionBindInput bind_input(*info);
	auto bind_data =
	    copy_function.copy_to_bind(context, bind_input, StringsToIdentifiers(names_to_write), types_to_write);

	auto copy_return_types = GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType::WRITTEN_FILE_STATISTICS);
	auto &physical_copy =
	    planner.Make<PhysicalCopyToFile>(copy_return_types, std::move(copy_function), std::move(bind_data), 1)
	        .Cast<PhysicalCopyToFile>();
	physical_copy.file_path = file_path;
	physical_copy.use_tmp_file = false;
	FilenamePattern pattern;
	pattern.SetFilenamePattern("hoglake-{uuidv7}");
	physical_copy.filename_pattern = std::move(pattern);
	physical_copy.file_extension = "parquet";
	physical_copy.overwrite_mode = CopyOverwriteMode::COPY_OVERWRITE_OR_IGNORE;
	physical_copy.per_thread_output = false;
	// target file size: rotate output files (hoglake's compaction target
	// default; make configurable via a setting later)
	physical_copy.file_size_bytes = idx_t(1) << 29;
	physical_copy.return_type = CopyFunctionReturnType::WRITTEN_FILE_STATISTICS;
	physical_copy.partition_output = !partition_columns.empty();
	physical_copy.write_partition_columns = true;
	physical_copy.write_empty_file = false;
	physical_copy.partition_columns = std::move(partition_columns);
	physical_copy.names = StringsToIdentifiers(names_to_write);
	physical_copy.expected_types = types_to_write;
	physical_copy.parallel = true;
	physical_copy.hive_file_pattern = true;
	if (plan) {
		physical_copy.children.push_back(*plan);
	}

	vector<LogicalType> return_types;
	return_types.emplace_back(LogicalType::BIGINT);
	auto &insert = planner.Make<HoglakeInsert>(return_types, table);
	insert.children.push_back(physical_copy);
	return insert;
}

} // namespace duckdb
