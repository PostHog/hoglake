//===----------------------------------------------------------------------===//
// HoglakeMultiFileList: MultiFileList over the /scan plan (data files
// paired with their live deletion vectors at the pinned snapshot).
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "common/hoglake_wire.hpp"
#include "storage/hoglake_scan.hpp"

namespace duckdb {

class HoglakeMultiFileList : public MultiFileList {
public:
	explicit HoglakeMultiFileList(HoglakeFunctionInfo &read_info);
	HoglakeMultiFileList(HoglakeFunctionInfo &read_info, vector<HoglakeScanFile> files);

	//! Partition pruning: filters on identity-transform partition
	//! columns are evaluated against each file's partition_values; a
	//! pruned copy of the list is returned. Files written under a spec
	//! other than the table's live one are never pruned.
	unique_ptr<MultiFileList> ComplexFilterPushdown(ClientContext &context, const MultiFileOptions &options,
	                                                MultiFilePushdownInfo &info,
	                                                vector<unique_ptr<Expression>> &filters) const override;

	vector<OpenFileInfo> GetAllFiles() const override;
	FileExpandResult GetExpandResult() const override;
	idx_t GetTotalFileCount() const override;
	unique_ptr<NodeStatistics> GetCardinality(ClientContext &context) const override;
	unique_ptr<MultiFileList> Copy() const override;

	HoglakeTableEntry &GetTable();
	const vector<HoglakeScanFile> &GetFiles() const;
	const HoglakeScanFile &GetFileEntry(idx_t file_idx) const;

protected:
	OpenFileInfo GetFile(idx_t i) const override;

private:
	void LoadFileList() const;

private:
	mutable mutex file_lock;
	HoglakeFunctionInfo &read_info;
	mutable vector<HoglakeScanFile> files;
	mutable bool read_file_list = false;
};

} // namespace duckdb
