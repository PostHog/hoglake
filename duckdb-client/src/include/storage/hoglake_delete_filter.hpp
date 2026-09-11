//===----------------------------------------------------------------------===//
// HoglakeDeleteFilter: applies a data file's live puffin deletion
// vector as a positional filter on the scan. Positions are physical
// row ordinals within the data file (never hoglake row ids).
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_data.hpp"
#include "common/hoglake_wire.hpp"

namespace duckdb {

class HoglakeDeleteFilter : public DeleteFilter {
public:
	//! sorted deleted positions
	vector<idx_t> deleted_rows;

	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) override;
	void Initialize(ClientContext &context, const HoglakeDeleteFile &delete_file);
};

} // namespace duckdb
