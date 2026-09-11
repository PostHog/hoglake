#include "storage/hoglake_storage.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "common/hoglake_types.hpp"
#include "storage/hoglake_catalog.hpp"
#include "storage/hoglake_transaction_manager.hpp"

namespace duckdb {

static void HandleHoglakeOption(HoglakeOptions &options, const string &option, const Value &value) {
	auto lcase = StringUtil::Lower(option);
	if (lcase == "endpoint") {
		options.endpoint = value.ToString();
	} else if (lcase == "data_path") {
		options.data_path = value.ToString();
	} else if (lcase == "create_if_not_exists") {
		options.create_if_not_exists = BooleanValue::Get(value.DefaultCastAs(LogicalType::BOOLEAN));
	} else if (lcase == "snapshot_version") {
		if (!options.snapshot_time.empty()) {
			throw InvalidInputException("Cannot specify both SNAPSHOT_VERSION and SNAPSHOT_TIME");
		}
		options.snapshot_version = NumericCast<idx_t>(BigIntValue::Get(value.DefaultCastAs(LogicalType::BIGINT)));
	} else if (lcase == "snapshot_time") {
		if (options.snapshot_version.IsValid()) {
			throw InvalidInputException("Cannot specify both SNAPSHOT_VERSION and SNAPSHOT_TIME");
		}
		// normalize to the ISO-8601 instant the server's Instant.parse
		// accepts, with INSTANT semantics (explicit offsets convert to
		// UTC instead of being dropped; naive timestamps follow the
		// session TimeZone — UTC by default)
		options.snapshot_time = HoglakeTypes::CanonicalInstant(value);
	} else {
		throw NotImplementedException("Unsupported option %s for hoglake", option);
	}
}

//! ATTACH 'hoglake:<catalog>' (ENDPOINT 'http://host:port') — the
//! catalog NAME is the attach payload, never a URI (a URI payload would
//! trip DuckDB's remote-file detection and drag httpfs into the attach
//! path). The endpoint comes from the ENDPOINT option or the
//! hoglake_default_endpoint setting.
static void ParseAttachPath(HoglakeOptions &options, const string &path) {
	if (path.find("://") != string::npos || path.find('/') != string::npos) {
		throw InvalidInputException(
		    "hoglake: the attach path is the catalog NAME, not a URI: "
		    "ATTACH 'hoglake:%s' AS lake (ENDPOINT 'http://host:port') — or SET hoglake_default_endpoint",
		    path.substr(path.find_last_of('/') + 1));
	}
	options.catalog_name = path;
}

static unique_ptr<Catalog> HoglakeAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                         AttachedDatabase &db, const string &name, AttachInfo &info,
                                         AttachOptions &attach_options) {
	HoglakeOptions options;
	ParseAttachPath(options, info.path);
	for (auto &entry : attach_options.options) {
		HandleHoglakeOption(options, entry.first, entry.second);
	}
	if (options.endpoint.empty()) {
		Value endpoint_setting;
		if (context.TryGetCurrentSetting("hoglake_default_endpoint", endpoint_setting) &&
		    !endpoint_setting.IsNull() && !endpoint_setting.ToString().empty()) {
			options.endpoint = endpoint_setting.ToString();
		}
	}
	if (options.endpoint.empty()) {
		throw InvalidInputException("hoglake: no endpoint: pass ENDPOINT 'http://host:port' on ATTACH or "
		                            "SET hoglake_default_endpoint = 'http://host:port'");
	}
	if (options.catalog_name.empty()) {
		throw InvalidInputException("hoglake: no catalog name in attach path");
	}
	options.access_mode = attach_options.access_mode;
	if (options.snapshot_version.IsValid() || !options.snapshot_time.empty()) {
		if (attach_options.access_mode == AccessMode::READ_WRITE) {
			throw InvalidInputException("SNAPSHOT_VERSION / SNAPSHOT_TIME can only be used in read-only mode");
		}
		attach_options.access_mode = AccessMode::READ_ONLY;
		db.SetReadOnlyDatabase();
	}
	return make_uniq<HoglakeCatalog>(db, std::move(options));
}

static unique_ptr<TransactionManager> HoglakeCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                      AttachedDatabase &db, Catalog &catalog) {
	auto &hoglake_catalog = catalog.Cast<HoglakeCatalog>();
	return make_uniq<HoglakeTransactionManager>(db, hoglake_catalog);
}

HoglakeStorageExtension::HoglakeStorageExtension() {
	attach = HoglakeAttach;
	create_transaction_manager = HoglakeCreateTransactionManager;
}

} // namespace duckdb
