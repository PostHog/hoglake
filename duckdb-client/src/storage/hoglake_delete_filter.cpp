#include "storage/hoglake_delete_filter.hpp"

#include "duckdb/common/exception.hpp"
#include "storage/hoglake_puffin.hpp"

#include <algorithm>

namespace duckdb {

void HoglakeDeleteFilter::Initialize(ClientContext &context, const HoglakeDeleteFile &delete_file) {
	if (delete_file.file_format != "puffin-dv") {
		throw InvalidInputException("hoglake: unknown delete file format \"%s\" for \"%s\" (expected puffin-dv)",
		                            delete_file.file_format, delete_file.path);
	}
	deleted_rows = HoglakePuffin::ReadDeletionVector(context, delete_file.path);
	if (NumericCast<int64_t>(deleted_rows.size()) != delete_file.delete_count) {
		throw InvalidInputException("hoglake: deletion vector \"%s\" holds %llu positions but the catalog registered "
		                            "delete_count %lld",
		                            delete_file.path, deleted_rows.size(), delete_file.delete_count);
	}
}

idx_t HoglakeDeleteFilter::Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) {
	if (count == 0) {
		return 0;
	}
	auto entry = std::lower_bound(deleted_rows.begin(), deleted_rows.end(), NumericCast<idx_t>(start_row_index));
	if (entry == deleted_rows.end()) {
		return count;
	}
	idx_t end_pos = NumericCast<idx_t>(start_row_index) + count;
	auto delete_idx = NumericCast<idx_t>(entry - deleted_rows.begin());
	if (deleted_rows[delete_idx] >= end_pos) {
		// nothing in this range is deleted
		return count;
	}
	result_sel.Initialize(STANDARD_VECTOR_SIZE);
	idx_t result_count = 0;
	for (idx_t i = 0; i < count; i++) {
		if (delete_idx < deleted_rows.size() && NumericCast<idx_t>(start_row_index) + i == deleted_rows[delete_idx]) {
			delete_idx++;
			continue;
		}
		result_sel.set_index(result_count++, i);
	}
	return result_count;
}

} // namespace duckdb
