//===----------------------------------------------------------------------===//
// The hoglake REST client: typed calls over the OpenAPI wire contract.
// Transport is duckdb's vendored cpp-httplib; codec is duckdb's yyjson.
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "common/hoglake_wire.hpp"

namespace duckdb {

//! Time-travel selector: at most one of snapshot / at_timestamp.
struct HoglakeTravel {
	optional_idx snapshot;
	string at_timestamp;

	static HoglakeTravel AtSnapshot(idx_t snapshot_id) {
		HoglakeTravel t;
		t.snapshot = snapshot_id;
		return t;
	}
	static HoglakeTravel Head() {
		return HoglakeTravel();
	}
	bool IsHead() const {
		return !snapshot.IsValid() && at_timestamp.empty();
	}
};

//! One client per attached catalog. Every method throws a duckdb
//! exception mapped from the hoglake error taxonomy on failure:
//!   400/422 -> InvalidInputException     (never retryable)
//!   404     -> CatalogException          (or nullptr on Try* methods)
//!   409     -> TransactionException      (commit conflicts; retryable upstream)
//!   410     -> InvalidInputException     (below the expiry floor)
//!   503     -> IOException               (commit_queue_timeout backpressure)
class HoglakeApiClient {
public:
	HoglakeApiClient(string endpoint_url, string catalog_name);

	const string &GetEndpoint() const {
		return endpoint;
	}
	const string &GetCatalogName() const {
		return catalog_name;
	}

public:
	// -- catalogs ----------------------------------------------------------
	HoglakeCatalogInfo GetCatalog();
	//! nullptr when the catalog does not exist
	unique_ptr<HoglakeCatalogInfo> TryGetCatalog();
	HoglakeCatalogInfo CreateCatalog(const string &data_path);

	// -- namespaces --------------------------------------------------------
	vector<string> ListNamespaces();
	void CreateNamespace(const string &name);

	// -- tables ------------------------------------------------------------
	vector<HoglakeTableSummary> ListTables(const string &ns);
	//! nullptr on 404
	unique_ptr<HoglakeTableInfo> TryGetTable(const string &ns, const string &table, const HoglakeTravel &travel);
	HoglakeTableInfo CreateTable(const string &ns, const string &name, const vector<HoglakeColumnDef> &columns);
	HoglakeCommitResult DropTable(const string &ns, const string &table);

	// -- views -------------------------------------------------------------
	vector<HoglakeViewInfo> ListViews(const string &ns);
	HoglakeViewInfo CreateView(const string &ns, const string &name, const string &sql, const string &dialect);
	HoglakeCommitResult DropView(const string &ns, const string &view);

	// -- read planning -----------------------------------------------------
	vector<HoglakeScanFile> PlanScan(const string &ns, const string &table, const HoglakeTravel &travel);

public:
	//! Percent-encode one URL path segment (identifiers are user data).
	static string EncodeSegment(const string &segment);

private:
	struct Response {
		int status = 0;
		string body;
	};

	Response Request(const string &method, const string &path, const string &json_body);
	//! Throws the mapped duckdb exception for a non-2xx response.
	[[noreturn]] void ThrowFor(const Response &response, const string &what);
	string CatalogPath(const string &suffix) const;
	static string TravelQuery(const HoglakeTravel &travel);

private:
	//! scheme://host[:port] (no trailing slash); /v1 is appended per call
	string endpoint;
	string catalog_name;
};

} // namespace duckdb
