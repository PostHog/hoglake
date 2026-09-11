#include "storage/hoglake_table_entry.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/function/table_function.hpp"

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

namespace {
struct HoglakeScanStubData : public TableFunctionData {
	explicit HoglakeScanStubData(HoglakeTableEntry &table) : table(table) {
	}
	HoglakeTableEntry &table;
};
} // namespace

TableFunction HoglakeTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
	// M1 stub: bindable (DESCRIBE/SHOW plan a scan at bind time) but not
	// executable — the real parquet-backed scan lands in M2.
	TableFunction function("hoglake_scan", {}, [](ClientContext &, TableFunctionInput &, DataChunk &) {
		throw NotImplementedException("hoglake table scan is not implemented yet (read path lands in M2)");
	});
	// let DESCRIBE / SHOW trace the plan back to this table entry
	// (constraint reporting goes through BindInfo)
	function.get_bind_info = [](const optional_ptr<FunctionData> data) {
		return BindInfo(data->Cast<HoglakeScanStubData>().table);
	};
	bind_data = make_uniq<HoglakeScanStubData>(*this);
	return function;
}

TableStorageInfo HoglakeTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo info;
	info.cardinality = NumericCast<idx_t>(table_info.record_count);
	return info;
}

} // namespace duckdb
