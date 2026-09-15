#include "hoglake_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "storage/hoglake_storage.hpp"
#include "functions/hoglake_metadata_functions.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	loader.SetDescription("DuckDB client for hoglake, PostHog's Postgres-native lakehouse-catalog control plane");

	auto &instance = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(instance);
	StorageExtension::Register(config, "hoglake", make_shared_ptr<HoglakeStorageExtension>());

	config.AddExtensionOption("hoglake_default_endpoint",
	                          "Default hoglake server endpoint (http://host:port) used when ATTACH has no ENDPOINT",
	                          LogicalType::VARCHAR, Value(""), nullptr, SetScope::GLOBAL);
	config.AddExtensionOption("hoglake_max_retry_count", "Maximum retry attempts for a hoglake commit",
	                          LogicalType::UBIGINT, Value::UBIGINT(10), nullptr, SetScope::GLOBAL);
	config.AddExtensionOption("hoglake_retry_wait_ms", "Time between hoglake commit retries", LogicalType::UBIGINT,
	                          Value::UBIGINT(100), nullptr, SetScope::GLOBAL);
	config.AddExtensionOption("hoglake_retry_backoff",
	                          "Backoff factor for exponentially increasing commit retry wait time",
	                          LogicalType::DOUBLE, Value::DOUBLE(1.5), nullptr, SetScope::GLOBAL);

	HoglakeMetadataFunctions::Register(loader);
}

void HoglakeExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string HoglakeExtension::Name() {
	return "hoglake";
}

std::string HoglakeExtension::Version() const {
#ifdef EXT_VERSION_HOGLAKE
	return EXT_VERSION_HOGLAKE;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(hoglake, loader) {
	duckdb::LoadInternal(loader);
}
}
