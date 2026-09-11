#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class HoglakeStorageExtension : public StorageExtension {
public:
	HoglakeStorageExtension();
};

} // namespace duckdb
