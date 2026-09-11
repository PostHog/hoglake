#include "rest/hoglake_api_client.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"

// duckdb-vendored header-only HTTP client (namespace duckdb_httplib;
// duckdb's copy pins its own namespace, distinct from httpfs' OpenSSL
// instantiation duckdb_httplib_openssl).
#include "httplib.hpp"

// duckdb-vendored yyjson (compiled into duckdb core).
#include "yyjson.hpp"

namespace duckdb {

using namespace duckdb_yyjson; // NOLINT

//===--------------------------------------------------------------------===//
// JSON helpers
//===--------------------------------------------------------------------===//

namespace {

struct JsonDoc {
	explicit JsonDoc(const string &body) : doc(yyjson_read(body.c_str(), body.size(), 0)) {
	}
	~JsonDoc() {
		if (doc) {
			yyjson_doc_free(doc);
		}
	}
	yyjson_val *Root() const {
		return doc ? yyjson_doc_get_root(doc) : nullptr;
	}
	yyjson_doc *doc;
};

struct JsonMutDoc {
	JsonMutDoc() : doc(yyjson_mut_doc_new(nullptr)) {
	}
	~JsonMutDoc() {
		if (doc) {
			yyjson_mut_doc_free(doc);
		}
	}
	string Write() const {
		auto data = yyjson_mut_write(doc, 0, nullptr);
		if (!data) {
			throw InternalException("hoglake: failed to serialize JSON request body");
		}
		string result(data);
		free(data); // NOLINT: yyjson allocates with malloc
		return result;
	}
	yyjson_mut_doc *doc;
};

string GetString(yyjson_val *obj, const char *key, const string &default_value = string()) {
	auto val = yyjson_obj_get(obj, key);
	if (!val || yyjson_is_null(val)) {
		return default_value;
	}
	if (!yyjson_is_str(val)) {
		throw IOException("hoglake: expected string for field \"%s\" in server response", key);
	}
	return string(yyjson_get_str(val), yyjson_get_len(val));
}

int64_t GetInt(yyjson_val *obj, const char *key, int64_t default_value = 0) {
	auto val = yyjson_obj_get(obj, key);
	if (!val || yyjson_is_null(val)) {
		return default_value;
	}
	if (!yyjson_is_int(val) && !yyjson_is_uint(val)) {
		throw IOException("hoglake: expected integer for field \"%s\" in server response", key);
	}
	return yyjson_get_sint(val);
}

bool GetBool(yyjson_val *obj, const char *key, bool default_value = false) {
	auto val = yyjson_obj_get(obj, key);
	if (!val || yyjson_is_null(val)) {
		return default_value;
	}
	return yyjson_get_bool(val);
}

HoglakeCatalogInfo ParseCatalogInfo(yyjson_val *obj) {
	HoglakeCatalogInfo info;
	info.name = GetString(obj, "name");
	info.data_path = GetString(obj, "data_path");
	info.head_snapshot_id = GetInt(obj, "head_snapshot_id");
	info.schema_version = GetInt(obj, "schema_version");
	info.earliest_snapshot_time = GetString(obj, "earliest_snapshot_time");
	return info;
}

HoglakeColumn ParseColumn(yyjson_val *obj) {
	HoglakeColumn col;
	col.name = GetString(obj, "name");
	col.type = GetString(obj, "type");
	col.nullable = GetBool(obj, "nullable", true);
	col.field_id = GetInt(obj, "field_id");
	col.ordinal = NumericCast<int32_t>(GetInt(obj, "ordinal"));
	auto params = yyjson_obj_get(obj, "type_params");
	if (params && yyjson_is_obj(params)) {
		col.precision = NumericCast<int32_t>(GetInt(params, "precision"));
		col.scale = NumericCast<int32_t>(GetInt(params, "scale"));
	}
	return col;
}

HoglakePartitionSpec ParsePartitionSpec(yyjson_val *obj) {
	HoglakePartitionSpec spec;
	spec.spec_id = GetInt(obj, "spec_id");
	auto fields = yyjson_obj_get(obj, "fields");
	size_t idx, max;
	yyjson_val *field;
	yyjson_arr_foreach(fields, idx, max, field) {
		HoglakePartitionField pf;
		pf.source_field_id = GetInt(field, "source_field_id");
		pf.transform = GetString(field, "transform");
		pf.transform_param = NumericCast<int32_t>(GetInt(field, "transform_param"));
		spec.fields.push_back(std::move(pf));
	}
	return spec;
}

HoglakeSortSpec ParseSortSpec(yyjson_val *obj) {
	HoglakeSortSpec spec;
	spec.sort_id = GetInt(obj, "sort_id");
	auto fields = yyjson_obj_get(obj, "fields");
	size_t idx, max;
	yyjson_val *field;
	yyjson_arr_foreach(fields, idx, max, field) {
		HoglakeSortField sf;
		sf.source_field_id = GetInt(field, "source_field_id");
		sf.direction = GetString(field, "direction");
		sf.null_order = GetString(field, "null_order");
		spec.fields.push_back(std::move(sf));
	}
	return spec;
}

HoglakeTableInfo ParseTableInfo(yyjson_val *obj) {
	HoglakeTableInfo info;
	info.name = GetString(obj, "name");
	info.namespace_name = GetString(obj, "namespace");
	info.table_uuid = GetString(obj, "table_uuid");
	info.record_count = GetInt(obj, "record_count");
	info.file_count = GetInt(obj, "file_count");
	info.file_size_bytes = GetInt(obj, "file_size_bytes");
	auto columns = yyjson_obj_get(obj, "columns");
	size_t idx, max;
	yyjson_val *col;
	yyjson_arr_foreach(columns, idx, max, col) {
		info.columns.push_back(ParseColumn(col));
	}
	auto spec = yyjson_obj_get(obj, "partition_spec");
	if (spec && yyjson_is_obj(spec)) {
		info.has_partition_spec = true;
		info.partition_spec = ParsePartitionSpec(spec);
	}
	auto sort = yyjson_obj_get(obj, "sort_spec");
	if (sort && yyjson_is_obj(sort)) {
		info.has_sort_spec = true;
		info.sort_spec = ParseSortSpec(sort);
	}
	return info;
}

HoglakeViewInfo ParseViewInfo(yyjson_val *obj) {
	HoglakeViewInfo info;
	info.name = GetString(obj, "name");
	info.namespace_name = GetString(obj, "namespace");
	info.view_uuid = GetString(obj, "view_uuid");
	info.dialect = GetString(obj, "dialect");
	info.sql = GetString(obj, "sql");
	return info;
}

HoglakeCommitResult ParseCommitResult(yyjson_val *obj) {
	HoglakeCommitResult result;
	result.snapshot_id = GetInt(obj, "snapshot_id");
	result.schema_version = GetInt(obj, "schema_version");
	return result;
}

HoglakeDataFile ParseDataFile(yyjson_val *obj) {
	HoglakeDataFile file;
	file.data_file_id = GetInt(obj, "data_file_id");
	file.path = GetString(obj, "path");
	file.file_format = GetString(obj, "file_format");
	file.record_count = GetInt(obj, "record_count");
	file.file_size_bytes = GetInt(obj, "file_size_bytes");
	file.footer_size = GetInt(obj, "footer_size");
	file.row_id_start = GetInt(obj, "row_id_start");
	file.stats_state = GetString(obj, "stats_state");
	file.begin_snapshot = GetInt(obj, "begin_snapshot");
	auto spec_id = yyjson_obj_get(obj, "spec_id");
	if (spec_id && !yyjson_is_null(spec_id)) {
		file.spec_id = NumericCast<idx_t>(yyjson_get_sint(spec_id));
	}
	auto values = yyjson_obj_get(obj, "partition_values");
	if (values && yyjson_is_arr(values)) {
		size_t idx, max;
		yyjson_val *val;
		yyjson_arr_foreach(values, idx, max, val) {
			if (yyjson_is_null(val)) {
				file.partition_values.push_back(Value(LogicalType::VARCHAR));
			} else {
				file.partition_values.push_back(Value(string(yyjson_get_str(val), yyjson_get_len(val))));
			}
		}
	}
	file.explicit_row_ids = GetBool(obj, "explicit_row_ids", false);
	return file;
}

HoglakeDeleteFile ParseDeleteFile(yyjson_val *obj) {
	HoglakeDeleteFile file;
	file.delete_file_id = GetInt(obj, "delete_file_id");
	file.data_file_id = GetInt(obj, "data_file_id");
	file.path = GetString(obj, "path");
	file.file_format = GetString(obj, "file_format");
	file.delete_count = GetInt(obj, "delete_count");
	file.file_size_bytes = GetInt(obj, "file_size_bytes");
	file.begin_snapshot = GetInt(obj, "begin_snapshot");
	return file;
}

yyjson_val *ParseObjectResponse(JsonDoc &doc, const string &what) {
	auto root = doc.Root();
	if (!root || !yyjson_is_obj(root)) {
		throw IOException("hoglake: malformed JSON object response for %s", what);
	}
	return root;
}

yyjson_val *ParseArrayResponse(JsonDoc &doc, const string &what) {
	auto root = doc.Root();
	if (!root || !yyjson_is_arr(root)) {
		throw IOException("hoglake: malformed JSON array response for %s", what);
	}
	return root;
}

} // namespace

//===--------------------------------------------------------------------===//
// Transport
//===--------------------------------------------------------------------===//

HoglakeApiClient::HoglakeApiClient(string endpoint_url, string catalog_name_p)
    : endpoint(std::move(endpoint_url)), catalog_name(std::move(catalog_name_p)) {
	while (!endpoint.empty() && endpoint.back() == '/') {
		endpoint.pop_back();
	}
	if (StringUtil::StartsWith(endpoint, "https://")) {
		throw NotImplementedException("hoglake: https endpoints are not supported yet (the vendored HTTP client is "
		                              "built without TLS); use a plaintext in-cluster endpoint");
	}
	if (!StringUtil::StartsWith(endpoint, "http://")) {
		throw InvalidInputException("hoglake: endpoint must be an http:// URL, got \"%s\"", endpoint);
	}
}

string HoglakeApiClient::EncodeSegment(const string &segment) {
	static const char *hex = "0123456789ABCDEF";
	string result;
	for (auto c : segment) {
		auto uc = static_cast<unsigned char>(c);
		bool unreserved = (uc >= 'A' && uc <= 'Z') || (uc >= 'a' && uc <= 'z') || (uc >= '0' && uc <= '9') ||
		                  uc == '-' || uc == '_' || uc == '.' || uc == '~';
		if (unreserved) {
			result += static_cast<char>(c);
		} else {
			result += '%';
			result += hex[uc >> 4];
			result += hex[uc & 0xF];
		}
	}
	return result;
}

string HoglakeApiClient::CatalogPath(const string &suffix) const {
	return "/v1/catalogs/" + EncodeSegment(catalog_name) + suffix;
}

string HoglakeApiClient::TravelQuery(const HoglakeTravel &travel) {
	if (travel.snapshot.IsValid()) {
		return "?snapshot=" + to_string(travel.snapshot.GetIndex());
	}
	if (!travel.at_timestamp.empty()) {
		return "?at_timestamp=" + EncodeSegment(travel.at_timestamp);
	}
	return string();
}

HoglakeApiClient::Response HoglakeApiClient::Request(const string &method, const string &path,
                                                     const string &json_body) {
	duckdb_httplib::Client client(endpoint);
	client.set_connection_timeout(10, 0);
	client.set_read_timeout(30, 0);
	client.set_write_timeout(30, 0);

	duckdb_httplib::Result result;
	if (method == "GET") {
		result = client.Get(path);
	} else if (method == "POST") {
		result = client.Post(path, json_body, "application/json");
	} else if (method == "PUT") {
		result = client.Put(path, json_body, "application/json");
	} else if (method == "PATCH") {
		result = client.Patch(path, json_body, "application/json");
	} else if (method == "DELETE") {
		result = client.Delete(path);
	} else {
		throw InternalException("hoglake: unknown HTTP method %s", method);
	}
	if (!result) {
		throw IOException("hoglake: %s %s%s failed: %s", method, endpoint, path,
		                  duckdb_httplib::to_string(result.error()));
	}
	Response response;
	response.status = result->status;
	response.body = result->body;
	if (result->has_header("Retry-After")) {
		auto retry_after = result->get_header_value("Retry-After");
		char *end = nullptr;
		auto parsed = std::strtoull(retry_after.c_str(), &end, 10);
		if (end == retry_after.c_str() + retry_after.size()) {
			response.retry_after_seconds = parsed;
		}
	}
	return response;
}

void HoglakeApiClient::ThrowFor(const Response &response, const string &what) {
	string error = StringUtil::Format("HTTP %d", response.status);
	string detail;
	{
		JsonDoc doc(response.body);
		auto root = doc.Root();
		if (root && yyjson_is_obj(root)) {
			auto err = GetString(root, "error");
			if (!err.empty()) {
				error = err;
			}
			detail = GetString(root, "detail");
		}
	}
	auto message = StringUtil::Format("hoglake: %s failed: %s%s%s", what, error, detail.empty() ? "" : " — ", detail);
	switch (response.status) {
	case 400:
	case 422:
		throw InvalidInputException("%s", message);
	case 404:
		throw CatalogException("%s", message);
	case 409:
		throw TransactionException("%s", message);
	case 410:
		throw InvalidInputException("%s (this history is below the catalog's expiry floor)", message);
	case 503:
		throw IOException("%s (retryable commit backpressure)", message);
	default:
		throw IOException("%s", message);
	}
}

//===--------------------------------------------------------------------===//
// Catalogs
//===--------------------------------------------------------------------===//

HoglakeCatalogInfo HoglakeApiClient::GetCatalog() {
	auto catalog = TryGetCatalog();
	if (!catalog) {
		throw CatalogException("hoglake: catalog \"%s\" does not exist at %s", catalog_name, endpoint);
	}
	return *catalog;
}

unique_ptr<HoglakeCatalogInfo> HoglakeApiClient::TryGetCatalog() {
	auto response = Request("GET", CatalogPath(""), string());
	if (response.status == 404) {
		return nullptr;
	}
	if (response.status != 200) {
		ThrowFor(response, "GET catalog");
	}
	JsonDoc doc(response.body);
	return make_uniq<HoglakeCatalogInfo>(ParseCatalogInfo(ParseObjectResponse(doc, "GET catalog")));
}

HoglakeCatalogInfo HoglakeApiClient::CreateCatalog(const string &data_path) {
	JsonMutDoc body;
	auto root = yyjson_mut_obj(body.doc);
	yyjson_mut_doc_set_root(body.doc, root);
	yyjson_mut_obj_add_strcpy(body.doc, root, "name", catalog_name.c_str());
	yyjson_mut_obj_add_strcpy(body.doc, root, "data_path", data_path.c_str());
	auto response = Request("POST", "/v1/catalogs", body.Write());
	if (response.status != 201) {
		ThrowFor(response, "create catalog");
	}
	JsonDoc doc(response.body);
	return ParseCatalogInfo(ParseObjectResponse(doc, "create catalog"));
}

//===--------------------------------------------------------------------===//
// Namespaces
//===--------------------------------------------------------------------===//

vector<string> HoglakeApiClient::ListNamespaces() {
	auto response = Request("GET", CatalogPath("/namespaces"), string());
	if (response.status != 200) {
		ThrowFor(response, "list namespaces");
	}
	JsonDoc doc(response.body);
	auto root = ParseArrayResponse(doc, "list namespaces");
	vector<string> result;
	size_t idx, max;
	yyjson_val *ns;
	yyjson_arr_foreach(root, idx, max, ns) {
		result.push_back(GetString(ns, "name"));
	}
	return result;
}

void HoglakeApiClient::CreateNamespace(const string &name) {
	JsonMutDoc body;
	auto root = yyjson_mut_obj(body.doc);
	yyjson_mut_doc_set_root(body.doc, root);
	yyjson_mut_obj_add_strcpy(body.doc, root, "name", name.c_str());
	auto response = Request("POST", CatalogPath("/namespaces"), body.Write());
	if (response.status != 201) {
		ThrowFor(response, "create namespace \"" + name + "\"");
	}
}

//===--------------------------------------------------------------------===//
// Tables
//===--------------------------------------------------------------------===//

vector<HoglakeTableSummary> HoglakeApiClient::ListTables(const string &ns) {
	auto response = Request("GET", CatalogPath("/namespaces/" + EncodeSegment(ns) + "/tables"), string());
	if (response.status != 200) {
		ThrowFor(response, "list tables in \"" + ns + "\"");
	}
	JsonDoc doc(response.body);
	auto root = ParseArrayResponse(doc, "list tables");
	vector<HoglakeTableSummary> result;
	size_t idx, max;
	yyjson_val *table;
	yyjson_arr_foreach(root, idx, max, table) {
		HoglakeTableSummary summary;
		summary.name = GetString(table, "name");
		summary.table_uuid = GetString(table, "table_uuid");
		result.push_back(std::move(summary));
	}
	return result;
}

unique_ptr<HoglakeTableInfo> HoglakeApiClient::TryGetTable(const string &ns, const string &table,
                                                           const HoglakeTravel &travel) {
	auto path = CatalogPath("/namespaces/" + EncodeSegment(ns) + "/tables/" + EncodeSegment(table)) +
	            TravelQuery(travel);
	auto response = Request("GET", path, string());
	if (response.status == 404) {
		return nullptr;
	}
	if (response.status != 200) {
		ThrowFor(response, "GET table \"" + ns + "." + table + "\"");
	}
	JsonDoc doc(response.body);
	return make_uniq<HoglakeTableInfo>(ParseTableInfo(ParseObjectResponse(doc, "GET table")));
}

HoglakeTableInfo HoglakeApiClient::CreateTable(const string &ns, const string &name,
                                               const vector<HoglakeColumnDef> &columns) {
	JsonMutDoc body;
	auto root = yyjson_mut_obj(body.doc);
	yyjson_mut_doc_set_root(body.doc, root);
	yyjson_mut_obj_add_strcpy(body.doc, root, "name", name.c_str());
	auto cols = yyjson_mut_obj_add_arr(body.doc, root, "columns");
	for (auto &col : columns) {
		auto col_obj = yyjson_mut_arr_add_obj(body.doc, cols);
		yyjson_mut_obj_add_strcpy(body.doc, col_obj, "name", col.name.c_str());
		yyjson_mut_obj_add_strcpy(body.doc, col_obj, "type", col.type.c_str());
		yyjson_mut_obj_add_bool(body.doc, col_obj, "nullable", col.nullable);
		if (col.type == "decimal") {
			auto params = yyjson_mut_obj_add_obj(body.doc, col_obj, "type_params");
			yyjson_mut_obj_add_int(body.doc, params, "precision", col.precision);
			yyjson_mut_obj_add_int(body.doc, params, "scale", col.scale);
		}
	}
	auto response = Request("POST", CatalogPath("/namespaces/" + EncodeSegment(ns) + "/tables"), body.Write());
	if (response.status != 201) {
		ThrowFor(response, "create table \"" + ns + "." + name + "\"");
	}
	JsonDoc doc(response.body);
	return ParseTableInfo(ParseObjectResponse(doc, "create table"));
}

HoglakeCommitResult HoglakeApiClient::DropTable(const string &ns, const string &table) {
	auto response =
	    Request("DELETE", CatalogPath("/namespaces/" + EncodeSegment(ns) + "/tables/" + EncodeSegment(table)),
	            string());
	if (response.status != 200) {
		ThrowFor(response, "drop table \"" + ns + "." + table + "\"");
	}
	JsonDoc doc(response.body);
	return ParseCommitResult(ParseObjectResponse(doc, "drop table"));
}

//===--------------------------------------------------------------------===//
// Views
//===--------------------------------------------------------------------===//

vector<HoglakeViewInfo> HoglakeApiClient::ListViews(const string &ns) {
	auto response = Request("GET", CatalogPath("/namespaces/" + EncodeSegment(ns) + "/views"), string());
	if (response.status != 200) {
		ThrowFor(response, "list views in \"" + ns + "\"");
	}
	JsonDoc doc(response.body);
	auto root = ParseArrayResponse(doc, "list views");
	vector<HoglakeViewInfo> result;
	size_t idx, max;
	yyjson_val *view;
	yyjson_arr_foreach(root, idx, max, view) {
		result.push_back(ParseViewInfo(view));
	}
	return result;
}

HoglakeViewInfo HoglakeApiClient::CreateView(const string &ns, const string &name, const string &sql,
                                             const string &dialect) {
	JsonMutDoc body;
	auto root = yyjson_mut_obj(body.doc);
	yyjson_mut_doc_set_root(body.doc, root);
	yyjson_mut_obj_add_strcpy(body.doc, root, "name", name.c_str());
	yyjson_mut_obj_add_strcpy(body.doc, root, "sql", sql.c_str());
	yyjson_mut_obj_add_strcpy(body.doc, root, "dialect", dialect.c_str());
	auto response = Request("POST", CatalogPath("/namespaces/" + EncodeSegment(ns) + "/views"), body.Write());
	if (response.status != 201) {
		ThrowFor(response, "create view \"" + ns + "." + name + "\"");
	}
	JsonDoc doc(response.body);
	return ParseViewInfo(ParseObjectResponse(doc, "create view"));
}

HoglakeCommitResult HoglakeApiClient::DropView(const string &ns, const string &view) {
	auto response = Request(
	    "DELETE", CatalogPath("/namespaces/" + EncodeSegment(ns) + "/views/" + EncodeSegment(view)), string());
	if (response.status != 200) {
		ThrowFor(response, "drop view \"" + ns + "." + view + "\"");
	}
	JsonDoc doc(response.body);
	return ParseCommitResult(ParseObjectResponse(doc, "drop view"));
}

//===--------------------------------------------------------------------===//
// Read planning
//===--------------------------------------------------------------------===//

vector<HoglakeScanFile> HoglakeApiClient::PlanScan(const string &ns, const string &table,
                                                   const HoglakeTravel &travel) {
	auto path = CatalogPath("/namespaces/" + EncodeSegment(ns) + "/tables/" + EncodeSegment(table) + "/scan") +
	            TravelQuery(travel);
	auto response = Request("GET", path, string());
	if (response.status != 200) {
		ThrowFor(response, "plan scan of \"" + ns + "." + table + "\"");
	}
	JsonDoc doc(response.body);
	auto root = ParseArrayResponse(doc, "plan scan");
	vector<HoglakeScanFile> result;
	size_t idx, max;
	yyjson_val *entry;
	yyjson_arr_foreach(root, idx, max, entry) {
		HoglakeScanFile scan_file;
		auto data_file = yyjson_obj_get(entry, "data_file");
		if (!data_file || !yyjson_is_obj(data_file)) {
			throw IOException("hoglake: scan plan entry without data_file");
		}
		scan_file.data_file = ParseDataFile(data_file);
		auto delete_file = yyjson_obj_get(entry, "delete_file");
		if (delete_file && yyjson_is_obj(delete_file)) {
			scan_file.has_delete_file = true;
			scan_file.delete_file = ParseDeleteFile(delete_file);
		}
		result.push_back(std::move(scan_file));
	}
	return result;
}


//===--------------------------------------------------------------------===//
// Commits
//===--------------------------------------------------------------------===//

HoglakeCommitOutcome HoglakeApiClient::TryCommit(const HoglakeCommitRequest &request) {
	JsonMutDoc body;
	auto root = yyjson_mut_obj(body.doc);
	yyjson_mut_doc_set_root(body.doc, root);
	if (request.read_snapshot.IsValid()) {
		yyjson_mut_obj_add_int(body.doc, root, "read_snapshot",
		                       NumericCast<int64_t>(request.read_snapshot.GetIndex()));
	}
	auto appends = yyjson_mut_obj_add_arr(body.doc, root, "appends");
	for (auto &append : request.appends) {
		auto append_obj = yyjson_mut_arr_add_obj(body.doc, appends);
		yyjson_mut_obj_add_strcpy(body.doc, append_obj, "namespace", append.namespace_name.c_str());
		yyjson_mut_obj_add_strcpy(body.doc, append_obj, "table", append.table_name.c_str());
		if (!append.expected_table_uuid.empty()) {
			yyjson_mut_obj_add_strcpy(body.doc, append_obj, "expected_table_uuid",
			                          append.expected_table_uuid.c_str());
		}
		auto files = yyjson_mut_obj_add_arr(body.doc, append_obj, "files");
		for (auto &file : append.files) {
			auto file_obj = yyjson_mut_arr_add_obj(body.doc, files);
			yyjson_mut_obj_add_strcpy(body.doc, file_obj, "path", file.path.c_str());
			yyjson_mut_obj_add_int(body.doc, file_obj, "record_count", file.record_count);
			yyjson_mut_obj_add_int(body.doc, file_obj, "file_size_bytes", file.file_size_bytes);
			if (file.footer_size.IsValid()) {
				yyjson_mut_obj_add_int(body.doc, file_obj, "footer_size",
				                       NumericCast<int64_t>(file.footer_size.GetIndex()));
			}
			if (file.has_partition_values) {
				auto values = yyjson_mut_obj_add_arr(body.doc, file_obj, "partition_values");
				for (auto &value : file.partition_values) {
					if (value.IsNull()) {
						yyjson_mut_arr_add_null(body.doc, values);
					} else {
						yyjson_mut_arr_add_strcpy(body.doc, values, StringValue::Get(value).c_str());
					}
				}
			}
		}
	}
	if (!request.deletes.empty()) {
		auto deletes = yyjson_mut_obj_add_arr(body.doc, root, "deletes");
		for (auto &table_deletes : request.deletes) {
			auto deletes_obj = yyjson_mut_arr_add_obj(body.doc, deletes);
			yyjson_mut_obj_add_strcpy(body.doc, deletes_obj, "namespace", table_deletes.namespace_name.c_str());
			yyjson_mut_obj_add_strcpy(body.doc, deletes_obj, "table", table_deletes.table_name.c_str());
			if (!table_deletes.expected_table_uuid.empty()) {
				yyjson_mut_obj_add_strcpy(body.doc, deletes_obj, "expected_table_uuid",
				                          table_deletes.expected_table_uuid.c_str());
			}
			auto files = yyjson_mut_obj_add_arr(body.doc, deletes_obj, "files");
			for (auto &file : table_deletes.files) {
				auto file_obj = yyjson_mut_arr_add_obj(body.doc, files);
				yyjson_mut_obj_add_int(body.doc, file_obj, "data_file_id", file.data_file_id);
				yyjson_mut_obj_add_strcpy(body.doc, file_obj, "path", file.path.c_str());
				yyjson_mut_obj_add_int(body.doc, file_obj, "delete_count", file.delete_count);
				yyjson_mut_obj_add_int(body.doc, file_obj, "file_size_bytes", file.file_size_bytes);
			}
		}
	}
	if (!request.author.empty()) {
		yyjson_mut_obj_add_strcpy(body.doc, root, "author", request.author.c_str());
	}
	if (!request.message.empty()) {
		yyjson_mut_obj_add_strcpy(body.doc, root, "message", request.message.c_str());
	}

	auto response = Request("POST", CatalogPath("/commit"), body.Write());
	HoglakeCommitOutcome outcome;
	outcome.status = response.status;
	outcome.retry_after_seconds = response.retry_after_seconds;
	if (response.status == 200) {
		outcome.success = true;
		JsonDoc doc(response.body);
		outcome.result = ParseCommitResult(ParseObjectResponse(doc, "commit"));
		return outcome;
	}
	JsonDoc doc(response.body);
	auto error_root = doc.Root();
	if (error_root && yyjson_is_obj(error_root)) {
		outcome.error = GetString(error_root, "error");
		outcome.detail = GetString(error_root, "detail");
	}
	if (outcome.error.empty()) {
		outcome.error = StringUtil::Format("HTTP %d", response.status);
	}
	return outcome;
}


HoglakeTableInfo HoglakeApiClient::AlterTable(const string &ns, const string &table,
                                              const vector<HoglakeAlterOp> &ops) {
	JsonMutDoc body;
	auto root = yyjson_mut_obj(body.doc);
	yyjson_mut_doc_set_root(body.doc, root);
	auto ops_arr = yyjson_mut_obj_add_arr(body.doc, root, "ops");
	for (auto &op : ops) {
		auto op_obj = yyjson_mut_arr_add_obj(body.doc, ops_arr);
		yyjson_mut_obj_add_strcpy(body.doc, op_obj, "op", op.op.c_str());
		if (op.op == "add_column") {
			auto col_obj = yyjson_mut_obj_add_obj(body.doc, op_obj, "column");
			yyjson_mut_obj_add_strcpy(body.doc, col_obj, "name", op.column.name.c_str());
			yyjson_mut_obj_add_strcpy(body.doc, col_obj, "type", op.column.type.c_str());
			yyjson_mut_obj_add_bool(body.doc, col_obj, "nullable", op.column.nullable);
			if (op.column.type == "decimal") {
				auto params = yyjson_mut_obj_add_obj(body.doc, col_obj, "type_params");
				yyjson_mut_obj_add_int(body.doc, params, "precision", op.column.precision);
				yyjson_mut_obj_add_int(body.doc, params, "scale", op.column.scale);
			}
		} else if (op.op == "drop_column" || op.op == "promote_column") {
			yyjson_mut_obj_add_strcpy(body.doc, op_obj, "name", op.name.c_str());
			if (op.op == "promote_column") {
				yyjson_mut_obj_add_strcpy(body.doc, op_obj, "to", op.to.c_str());
			}
		} else if (op.op == "rename_column") {
			yyjson_mut_obj_add_strcpy(body.doc, op_obj, "from", op.from.c_str());
			yyjson_mut_obj_add_strcpy(body.doc, op_obj, "to", op.to.c_str());
		} else if (op.op == "rename_table") {
			yyjson_mut_obj_add_strcpy(body.doc, op_obj, "new_name", op.new_name.c_str());
		} else if (op.op == "set_partition_spec") {
			auto fields = yyjson_mut_obj_add_arr(body.doc, op_obj, "fields");
			for (auto &field : op.fields) {
				auto field_obj = yyjson_mut_arr_add_obj(body.doc, fields);
				yyjson_mut_obj_add_int(body.doc, field_obj, "source_field_id", field.source_field_id);
				yyjson_mut_obj_add_strcpy(body.doc, field_obj, "transform", field.transform.c_str());
				if (field.transform == "bucket") {
					yyjson_mut_obj_add_int(body.doc, field_obj, "transform_param", field.transform_param);
				}
			}
		} else if (op.op == "set_sort_order") {
			auto fields = yyjson_mut_obj_add_arr(body.doc, op_obj, "sort_fields");
			for (auto &field : op.sort_fields) {
				auto field_obj = yyjson_mut_arr_add_obj(body.doc, fields);
				yyjson_mut_obj_add_int(body.doc, field_obj, "source_field_id", field.source_field_id);
				yyjson_mut_obj_add_strcpy(body.doc, field_obj, "direction", field.direction.c_str());
				yyjson_mut_obj_add_strcpy(body.doc, field_obj, "null_order", field.null_order.c_str());
			}
		} else {
			throw InternalException("hoglake: unknown alter op %s", op.op);
		}
	}
	auto response = Request(
	    "POST", CatalogPath("/namespaces/" + EncodeSegment(ns) + "/tables/" + EncodeSegment(table) + "/alter"),
	    body.Write());
	if (response.status != 200) {
		ThrowFor(response, "alter table \"" + ns + "." + table + "\"");
	}
	JsonDoc doc(response.body);
	return ParseTableInfo(ParseObjectResponse(doc, "alter table"));
}

} // namespace duckdb
