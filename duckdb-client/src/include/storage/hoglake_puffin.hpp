//===----------------------------------------------------------------------===//
// hoglake puffin deletion vectors: an Iceberg v3 puffin container with
// exactly one uncompressed `deletion-vector-v1` blob (the server's
// PuffinDeletionVector.kt is the reference reader; ducklake's puffin
// code is the C++ ancestor of this file). Positions are 0-based
// physical row ordinals within one data file.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class ClientContext;

struct HoglakePuffin {
	//! Read a puffin DV file and return the deleted positions, sorted
	//! ascending. Throws InvalidInputException on any structural
	//! violation (never a partial decode).
	static vector<idx_t> ReadDeletionVector(ClientContext &context, const string &path);

	//! Decode a deletion-vector-v1 blob (length prefix + magic + 64-bit
	//! portable roaring + CRC) into `out`.
	static void DecodeBlob(const_data_ptr_t blob, idx_t blob_length, const string &path, set<idx_t> &out);

	//! Serialize positions as a deletion-vector-v1 blob (M4 write path).
	static vector<data_t> EncodeBlob(const set<idx_t> &positions);

	//! Build a complete puffin container holding one deletion-vector-v1
	//! blob for `data_file_path` (referenced-data-file property).
	//! Returns the serialized file bytes; cardinality via positions.
	static vector<data_t> WritePuffinFile(const set<idx_t> &positions, const string &data_file_path);
};

} // namespace duckdb
