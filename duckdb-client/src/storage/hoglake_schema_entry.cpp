#include "storage/hoglake_schema_entry.hpp"

#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include <algorithm>
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "common/hoglake_types.hpp"
#include "storage/hoglake_catalog.hpp"
#include "storage/hoglake_table_entry.hpp"
#include "storage/hoglake_transaction.hpp"

namespace duckdb {

//! Column names with this prefix are reserved for hoglake internals
//! (_hog_row_id is compaction's row-id carrier). The server does NOT
//! enforce the reservation (its name validation is pattern-only —
//! DESIGN.md server finding 9), so THIS CLIENT IS THE ENFORCEMENT
//! POINT: weakening these checks commits user _hog columns into the
//! shared catalog and breaks name-based readers fleet-wide.
static constexpr const char *RESERVED_COLUMN_PREFIX = "_hog";

HoglakeSchemaEntry::HoglakeSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, HoglakeTransaction &transaction)
    : SchemaCatalogEntry(catalog, info) {
}

HoglakeTransaction &HoglakeSchemaEntry::Transaction(CatalogTransaction &transaction) {
	return transaction.transaction->Cast<HoglakeTransaction>();
}

//===--------------------------------------------------------------------===//
// Identifier resolution (DuckDB CI semantics over a case-sensitive
// server; see the header comment)
//===--------------------------------------------------------------------===//

void HoglakeSchemaEntry::EnsureTableNamesInternal(HoglakeTransaction &transaction) {
	if (table_names_loaded) {
		return;
	}
	auto ns = name.GetIdentifierName();
	// NOTE (wire): the table listing is head-only; per-table fetches are
	// at the pinned travel (DESIGN.md server findings)
	for (auto &summary : transaction.Api().ListTables(ns)) {
		if (dropped_tables.find(summary.name) != dropped_tables.end()) {
			continue;
		}
		table_names[summary.name].push_back(summary.name);
	}
	table_names_loaded = true;
}

string HoglakeSchemaEntry::ResolveTableNameInternal(HoglakeTransaction &transaction, const string &typed_name) {
	EnsureTableNamesInternal(transaction);
	auto entry = table_names.find(typed_name);
	if (entry == table_names.end() || entry->second.empty()) {
		return string();
	}
	auto &candidates = entry->second;
	if (candidates.size() > 1) {
		// two server-side tables whose names differ only by case:
		// DuckDB identifiers cannot distinguish them
		throw CatalogException("hoglake: identifier \"%s\" is ambiguous in namespace \"%s\": the server holds "
		                       "multiple tables whose names differ only by case (%s). Rename one via another "
		                       "client to make the namespace addressable from DuckDB",
		                       typed_name, name.GetIdentifierName(), StringUtil::Join(candidates, ", "));
	}
	return candidates[0];
}

string HoglakeSchemaEntry::ResolveTableName(HoglakeTransaction &transaction, const string &typed_name) {
	std::lock_guard<std::recursive_mutex> guard(entry_lock);
	return ResolveTableNameInternal(transaction, typed_name);
}

//===--------------------------------------------------------------------===//
// Table loading
//===--------------------------------------------------------------------===//

CatalogEntry &HoglakeSchemaEntry::CacheTableInternal(HoglakeTransaction &transaction, HoglakeTableInfo table_info,
                                                     const HoglakeTravel &read_travel, bool writes_refused) {
	auto columns = table_info.columns;
	std::sort(columns.begin(), columns.end(),
	          [](const HoglakeColumn &a, const HoglakeColumn &b) { return a.ordinal < b.ordinal; });

	auto table_name = table_info.name;
	// a server table with case-colliding column names cannot be
	// represented by DuckDB's case-insensitive ColumnList; throw a
	// TARGETED error before ColumnList::AddColumn does (listings skip
	// such tables; direct lookups surface this message)
	{
		case_insensitive_map_t<bool> seen_columns;
		for (auto &col : columns) {
			if (!seen_columns.emplace(col.name, true).second) {
				throw CatalogException(
				    "hoglake: table \"%s.%s\" has columns whose names differ only by case (\"%s\"), which DuckDB "
				    "cannot represent. Repair the table via another client",
				    name.GetIdentifierName(), table_name, col.name);
			}
		}
	}
	CreateTableInfo create_info(*this, Identifier(table_name));
	vector<LogicalIndex> not_null;
	for (auto &col : columns) {
		ColumnDefinition column(Identifier(col.name), HoglakeTypes::ToDuckDBType(col));
		if (!col.nullable) {
			not_null.push_back(LogicalIndex(create_info.columns.LogicalColumnCount()));
		}
		create_info.columns.AddColumn(std::move(column));
	}
	for (auto idx : not_null) {
		create_info.constraints.push_back(make_uniq<NotNullConstraint>(idx));
	}
	auto entry =
	    make_uniq<HoglakeTableEntry>(catalog, *this, create_info, std::move(table_info), read_travel, writes_refused);
	auto &result = *entry;
	if (writes_refused) {
		// AT-clause entries live in the travel map, keyed by the caller
		throw InternalException("hoglake: travel entries are cached by LookupTableAtInternal");
	}
	auto existing = tables.find(table_name);
	if (existing != tables.end()) {
		// never destroy an entry mid-transaction: another thread (or the
		// current statement) may still hold a reference
		retired.push_back(std::move(existing->second));
		tables.erase(existing);
	}
	tables[table_name] = std::move(entry);
	return result;
}

optional_ptr<CatalogEntry> HoglakeSchemaEntry::LookupTableInternal(HoglakeTransaction &transaction,
                                                                   const string &typed_name) {
	auto resolved = ResolveTableNameInternal(transaction, typed_name);
	if (resolved.empty()) {
		return nullptr;
	}
	auto existing = tables.find(resolved);
	if (existing != tables.end()) {
		return existing->second.get();
	}
	auto unrepresentable = unrepresentable_tables.find(resolved);
	if (unrepresentable != unrepresentable_tables.end()) {
		// a listing skipped this table with a targeted error; the
		// direct lookup must surface THAT error (never "does not
		// exist", and DROP IF EXISTS must not silently no-op)
		throw CatalogException("%s", unrepresentable->second);
	}
	if (all_tables_loaded) {
		// listed at head but not visible at the pinned travel
		return nullptr;
	}
	auto ns = name.GetIdentifierName();
	auto travel = transaction.Travel();
	try {
		auto table_info = transaction.Api().TryGetTable(ns, resolved, travel);
		if (!table_info) {
			return nullptr;
		}
		return &CacheTableInternal(transaction, std::move(*table_info), travel, false);
	} catch (CatalogException &ex) {
		unrepresentable_tables[resolved] = ErrorData(ex).RawMessage();
		throw;
	} catch (InvalidInputException &ex) {
		unrepresentable_tables[resolved] = ErrorData(ex).RawMessage();
		throw;
	}
}

void HoglakeSchemaEntry::LoadAllTablesInternal(HoglakeTransaction &transaction) {
	if (all_tables_loaded) {
		return;
	}
	EnsureTableNamesInternal(transaction);
	auto ns = name.GetIdentifierName();
	auto travel = transaction.Travel();
	for (auto &entry : table_names) {
		if (entry.second.size() != 1) {
			// CI-ambiguous pair: unaddressable from DuckDB (lookup
			// throws); skip in listings for consistency
			continue;
		}
		auto &exact = entry.second[0];
		if (tables.find(exact) != tables.end()) {
			continue;
		}
		// the try covers BOTH the fetch (parse-time wire validation,
		// e.g. out-of-range decimal params, throws there) and entry
		// construction (case-colliding columns): an unrepresentable
		// table must never break the LISTING. Remember the targeted
		// error so a DIRECT lookup after this listing rethrows it
		// instead of reporting "does not exist".
		try {
			auto table_info = transaction.Api().TryGetTable(ns, exact, travel);
			if (!table_info) {
				// listed at head but missing at the pinned travel
				continue;
			}
			CacheTableInternal(transaction, std::move(*table_info), travel, false);
		} catch (CatalogException &ex) {
			unrepresentable_tables[exact] = ErrorData(ex).RawMessage();
			continue;
		} catch (InvalidInputException &ex) {
			unrepresentable_tables[exact] = ErrorData(ex).RawMessage();
			continue;
		}
	}
	all_tables_loaded = true;
}

//===--------------------------------------------------------------------===//
// Lookup / scan
//===--------------------------------------------------------------------===//

optional_ptr<CatalogEntry> HoglakeSchemaEntry::LookupEntry(CatalogTransaction transaction,
                                                           const EntryLookupInfo &lookup_info) {
	auto catalog_type = lookup_info.GetCatalogType();
	if (catalog_type != CatalogType::TABLE_ENTRY) {
		return nullptr;
	}
	auto &hoglake_transaction = Transaction(transaction);
	std::lock_guard<std::recursive_mutex> guard(entry_lock);
	auto at_clause = lookup_info.GetAtClause();
	if (at_clause) {
		auto travel = hoglake_transaction.TravelFor(at_clause);
		return LookupTableAtInternal(hoglake_transaction, lookup_info.GetEntryName(), travel);
	}
	return LookupTableInternal(hoglake_transaction, lookup_info.GetEntryName());
}

optional_ptr<CatalogEntry> HoglakeSchemaEntry::LookupTableAtInternal(HoglakeTransaction &transaction,
                                                                     const string &typed_name,
                                                                     const HoglakeTravel &travel) {
	// resolve via the head listing; fall back to the typed name for
	// tables that no longer exist at head under that name
	auto resolved = ResolveTableNameInternal(transaction, typed_name);
	if (resolved.empty()) {
		resolved = typed_name;
	}
	auto key = resolved + "@" + travel.CacheKey();
	auto existing = travel_tables.find(key);
	if (existing != travel_tables.end()) {
		return existing->second.get();
	}
	auto ns = name.GetIdentifierName();
	// a 410 (below the expiry floor) surfaces as InvalidInputException
	auto table_info = transaction.Api().TryGetTable(ns, resolved, travel);
	if (!table_info) {
		return nullptr;
	}
	// build the entry at the historical schema; reads plan at `travel`
	// and writes are refused
	auto columns = table_info->columns;
	std::sort(columns.begin(), columns.end(),
	          [](const HoglakeColumn &a, const HoglakeColumn &b) { return a.ordinal < b.ordinal; });
	auto table_name = table_info->name;
	CreateTableInfo create_info(*this, Identifier(table_name));
	vector<LogicalIndex> not_null;
	for (auto &col : columns) {
		ColumnDefinition column(Identifier(col.name), HoglakeTypes::ToDuckDBType(col));
		if (!col.nullable) {
			not_null.push_back(LogicalIndex(create_info.columns.LogicalColumnCount()));
		}
		create_info.columns.AddColumn(std::move(column));
	}
	for (auto idx : not_null) {
		create_info.constraints.push_back(make_uniq<NotNullConstraint>(idx));
	}
	auto entry = make_uniq<HoglakeTableEntry>(catalog, *this, create_info, std::move(*table_info), travel, true);
	auto &result = *entry;
	travel_tables[key] = std::move(entry);
	return &result;
}

void HoglakeSchemaEntry::Scan(ClientContext &context, CatalogType type,
                              const std::function<void(CatalogEntry &)> &callback) {
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	auto &transaction = HoglakeTransaction::Get(context, catalog);
	// snapshot the entry pointers under the lock, run callbacks
	// unlocked: callbacks (duckdb_columns etc.) may re-enter lookups.
	// Entries are retired, never destroyed, so the pointers stay valid.
	vector<CatalogEntry *> entries;
	{
		std::lock_guard<std::recursive_mutex> guard(entry_lock);
		LoadAllTablesInternal(transaction);
		for (auto &entry : tables) {
			if (entry.second) {
				entries.push_back(entry.second.get());
			}
		}
	}
	for (auto entry : entries) {
		callback(*entry);
	}
}

void HoglakeSchemaEntry::Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) {
	// context-free scan: only what is already cached
	if (type != CatalogType::TABLE_ENTRY) {
		return;
	}
	vector<CatalogEntry *> entries;
	{
		std::lock_guard<std::recursive_mutex> guard(entry_lock);
		for (auto &entry : tables) {
			if (entry.second) {
				entries.push_back(entry.second.get());
			}
		}
	}
	for (auto entry : entries) {
		callback(*entry);
	}
}

//===--------------------------------------------------------------------===//
// DDL
//===--------------------------------------------------------------------===//

//! read travel for an entry whose table was created/altered by THIS
//! transaction: the pin predates the DDL, so those entries read at the
//! post-DDL head instead (DESIGN.md, "Transactions and eager DDL")
static HoglakeTravel PostDDLTravel(HoglakeTransaction &transaction) {
	auto info = transaction.Api().GetCatalog();
	return HoglakeTravel::AtSnapshot(NumericCast<idx_t>(info.head_snapshot_id));
}

optional_ptr<CatalogEntry> HoglakeSchemaEntry::CreateTable(CatalogTransaction transaction,
                                                           BoundCreateTableInfo &info) {
	auto &hoglake_transaction = Transaction(transaction);
	auto &base_info = info.Base();
	auto table_name = base_info.GetTableName().GetIdentifierName();
	auto ns = name.GetIdentifierName();

	std::lock_guard<std::recursive_mutex> guard(entry_lock);
	auto resolved = ResolveTableNameInternal(hoglake_transaction, table_name);
	if (!resolved.empty()) {
		if (base_info.on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT) {
			return nullptr;
		}
		if (base_info.on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw CatalogException("hoglake: table \"%s.%s\" already exists", ns, resolved);
		}
		throw NotImplementedException("CREATE OR REPLACE TABLE is not supported for hoglake");
	}
	hoglake_transaction.RequireDDLAllowed(ns, table_name, "CREATE");

	// collect NOT NULL columns
	unordered_set<idx_t> not_null;
	for (auto &constraint : base_info.constraints) {
		if (constraint->type == ConstraintType::NOT_NULL) {
			auto &nn = constraint->Cast<NotNullConstraint>();
			not_null.insert(nn.index.index);
		} else {
			throw NotImplementedException("hoglake tables only support NOT NULL constraints");
		}
	}

	vector<HoglakeColumnDef> defs;
	for (auto &col : base_info.columns.Logical()) {
		auto col_name = col.Name().GetIdentifierName();
		if (StringUtil::StartsWith(StringUtil::Lower(col_name), RESERVED_COLUMN_PREFIX)) {
			throw InvalidInputException("hoglake: column name \"%s\" uses the reserved \"%s\" prefix "
			                            "(hoglake internal columns, e.g. _hog_row_id)",
			                            col_name, RESERVED_COLUMN_PREFIX);
		}
		auto nullable = not_null.find(col.Logical().index) == not_null.end();
		defs.push_back(HoglakeTypes::FromDuckDBType(col_name, col.Type(), nullable));
	}

	// eager DDL: the server commit happens NOW (its own snapshot) and
	// survives a rollback of the surrounding transaction (DESIGN.md).
	// The typed case is preserved on the wire (DuckDB semantics).
	auto created = hoglake_transaction.Api().CreateTable(ns, table_name, defs);
	// the transaction pin predates this create; the new entry reads at
	// the post-create head so same-transaction SELECTs work
	auto read_travel = PostDDLTravel(hoglake_transaction);
	dropped_tables.erase(created.name);
	table_names[created.name].push_back(created.name);
	return &CacheTableInternal(hoglake_transaction, std::move(created), read_travel, false);
}

void HoglakeSchemaEntry::DropEntry(ClientContext &context, DropInfo &info) {
	if (info.cascade) {
		throw NotImplementedException("DROP ... CASCADE is not supported for hoglake");
	}
	auto typed_name = info.GetQualifiedName().Name().GetIdentifierName();
	auto &transaction = HoglakeTransaction::Get(context, catalog);
	auto ns = name.GetIdentifierName();
	switch (info.type) {
	case CatalogType::TABLE_ENTRY: {
		std::lock_guard<std::recursive_mutex> guard(entry_lock);
		auto entry = LookupTableInternal(transaction, typed_name);
		if (!entry) {
			if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
				return;
			}
			throw CatalogException("hoglake: table \"%s.%s\" does not exist", ns, typed_name);
		}
		// the entry's wire name is the server's exact name
		auto &exact = entry->Cast<HoglakeTableEntry>().GetWireInfo().name;
		transaction.RequireDDLAllowed(ns, exact, "DROP");
		transaction.Api().DropTable(ns, exact);
		// forget the name: the pin predates the drop, so server reads
		// would resurrect it without this
		auto names_entry = table_names.find(exact);
		if (names_entry != table_names.end()) {
			table_names.erase(names_entry);
		}
		dropped_tables[exact] = true;
		auto existing = tables.find(exact);
		if (existing != tables.end()) {
			retired.push_back(std::move(existing->second));
			tables.erase(existing);
		}
		return;
	}
	default:
		throw NotImplementedException("hoglake: DROP of this entry type is not supported yet");
	}
}

//===--------------------------------------------------------------------===//
// ALTER
//===--------------------------------------------------------------------===//

//! CI-resolve a typed column identifier to the wire column's exact name
static const HoglakeColumn &ResolveWireColumn(const HoglakeTableInfo &wire, const string &typed_name) {
	optional_ptr<const HoglakeColumn> match;
	for (auto &col : wire.columns) {
		if (StringUtil::CIEquals(col.name, typed_name)) {
			if (match) {
				throw CatalogException("hoglake: column identifier \"%s\" is ambiguous in table \"%s\" (names "
				                       "differing only by case)",
				                       typed_name, wire.name);
			}
			match = &col;
		}
	}
	if (!match) {
		throw BinderException("hoglake: column \"%s\" does not exist in table \"%s\"", typed_name, wire.name);
	}
	return *match;
}

static int64_t FindFieldId(const HoglakeTableInfo &wire, const string &column_name) {
	return ResolveWireColumn(wire, column_name).field_id;
}

//! Parse a partition-key expression into a wire PartitionField:
//! a bare column ref = identity; year/month/day/hour(col) and
//! bucket(n, col) map to the transform vocabulary.
static HoglakePartitionField ParsePartitionExpression(const HoglakeTableInfo &wire, ParsedExpression &expr) {
	HoglakePartitionField field;
	if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &col_ref = expr.Cast<ColumnRefExpression>();
		field.source_field_id = FindFieldId(wire, col_ref.GetColumnName().GetIdentifierName());
		field.transform = "identity";
		return field;
	}
	if (expr.GetExpressionClass() == ExpressionClass::FUNCTION) {
		auto &function = expr.Cast<FunctionExpression>();
		auto function_name = StringUtil::Lower(function.FunctionName().GetIdentifierName());
		auto &args = function.GetArguments();
		if (function_name == "year" || function_name == "month" || function_name == "day" ||
		    function_name == "hour") {
			if (args.size() != 1 || args[0].GetExpression().GetExpressionClass() != ExpressionClass::COLUMN_REF) {
				throw BinderException("hoglake: %s(...) partition transform takes exactly one column", function_name);
			}
			auto &col_ref = args[0].GetExpression().Cast<ColumnRefExpression>();
			field.source_field_id = FindFieldId(wire, col_ref.GetColumnName().GetIdentifierName());
			field.transform = function_name;
			return field;
		}
		if (function_name == "bucket") {
			if (args.size() != 2 || args[0].GetExpression().GetExpressionClass() != ExpressionClass::CONSTANT ||
			    args[1].GetExpression().GetExpressionClass() != ExpressionClass::COLUMN_REF) {
				throw BinderException("hoglake: bucket partitioning is bucket(<n>, <column>)");
			}
			auto &n = args[0].GetExpression().Cast<ConstantExpression>();
			auto &col_ref = args[1].GetExpression().Cast<ColumnRefExpression>();
			field.source_field_id = FindFieldId(wire, col_ref.GetColumnName().GetIdentifierName());
			field.transform = "bucket";
			auto bucket_count = BigIntValue::Get(n.GetValue().DefaultCastAs(LogicalType::BIGINT));
			if (bucket_count < 1 || bucket_count > 2147483647) {
				throw BinderException("hoglake: bucket count must be in [1, 2^31), got %lld", bucket_count);
			}
			field.transform_param = NumericCast<int32_t>(bucket_count);
			return field;
		}
	}
	throw BinderException("hoglake: unsupported partition expression \"%s\" (supported: <column>, "
	                      "year/month/day/hour(<column>), bucket(<n>, <column>))",
	                      expr.ToString());
}

//! True iff the ALTER COLUMN TYPE expression is the implicit cast of
//! the column itself (possibly nested casts). Anything else (USING with
//! a real expression) must be rejected: promote_column is metadata-only
//! and would silently skip the transform.
static bool IsSimpleCast(const ParsedExpression &expr, const string &column_name) {
	if (expr.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &col_ref = expr.Cast<ColumnRefExpression>();
		return StringUtil::CIEquals(col_ref.GetColumnName().GetIdentifierName(), column_name);
	}
	if (expr.GetExpressionClass() == ExpressionClass::CAST) {
		auto &cast = expr.Cast<CastExpression>();
		return IsSimpleCast(cast.Child(), column_name);
	}
	return false;
}

void HoglakeSchemaEntry::Alter(CatalogTransaction transaction, AlterInfo &info) {
	if (info.type != AlterType::ALTER_TABLE) {
		throw NotImplementedException("hoglake: only ALTER TABLE is supported");
	}
	auto &hoglake_transaction = Transaction(transaction);
	auto &alter_table = info.Cast<AlterTableInfo>();
	auto typed_name = alter_table.GetQualifiedName().Name().GetIdentifierName();
	auto ns = name.GetIdentifierName();

	std::lock_guard<std::recursive_mutex> guard(entry_lock);
	auto entry = LookupTableInternal(hoglake_transaction, typed_name);
	if (!entry) {
		if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
			return;
		}
		throw CatalogException("hoglake: table \"%s.%s\" does not exist", ns, typed_name);
	}
	auto &table = entry->Cast<HoglakeTableEntry>();
	auto &wire = table.GetWireInfo();
	//! the server's exact table name
	auto exact_name = wire.name;
	hoglake_transaction.RequireDDLAllowed(ns, exact_name, "ALTER");

	HoglakeAlterOp op;
	string new_entry_name = exact_name;
	switch (alter_table.alter_table_type) {
	case AlterTableType::RENAME_TABLE: {
		auto &rename = alter_table.Cast<RenameTableInfo>();
		op.op = "rename_table";
		op.new_name = rename.new_table_name.GetIdentifierName();
		// CI-conflict check on the TARGET (the server checks exact-case
		// only): renaming onto a name that CI-resolves to a DIFFERENT
		// table would create a case-colliding pair the extension then
		// reports ambiguous and hides — refuse up front. A CI-self
		// resolve is a plain case-change rename and is fine.
		auto target_resolved = ResolveTableNameInternal(hoglake_transaction, op.new_name);
		if (!target_resolved.empty() && !StringUtil::CIEquals(target_resolved, exact_name)) {
			throw CatalogException("hoglake: cannot rename table \"%s.%s\" to \"%s\": table \"%s\" already "
			                       "exists (identifiers are case-insensitive)",
			                       ns, exact_name, op.new_name, target_resolved);
		}
		new_entry_name = op.new_name;
		break;
	}
	case AlterTableType::RENAME_COLUMN: {
		auto &rename = alter_table.Cast<RenameColumnInfo>();
		op.op = "rename_column";
		op.from = ResolveWireColumn(wire, rename.old_name.GetIdentifierName()).name;
		op.to = rename.new_name.GetIdentifierName();
		if (StringUtil::StartsWith(StringUtil::Lower(op.to), RESERVED_COLUMN_PREFIX)) {
			// see ADD COLUMN: the client is the reservation's
			// enforcement point
			throw InvalidInputException("hoglake: column name \"%s\" uses the reserved \"%s\" prefix "
			                            "(hoglake internal columns, e.g. _hog_row_id)",
			                            op.to, RESERVED_COLUMN_PREFIX);
		}
		// CI-conflict check on the target (server checks exact-case
		// only; a committed collision makes the table unrepresentable
		// in DuckDB). Renaming a column onto its own name with a case
		// change is fine.
		for (auto &col : wire.columns) {
			if (StringUtil::CIEquals(col.name, op.to) && !StringUtil::CIEquals(col.name, op.from)) {
				throw CatalogException("hoglake: cannot rename column \"%s\" to \"%s\" in table \"%s.%s\": "
				                       "column \"%s\" already exists (identifiers are case-insensitive)",
				                       op.from, op.to, ns, exact_name, col.name);
			}
		}
		break;
	}
	case AlterTableType::ADD_COLUMN: {
		auto &add = alter_table.Cast<AddColumnInfo>();
		auto &col = add.new_column;
		if (add.if_column_not_exists && table.ColumnExists(col.Name())) {
			return;
		}
		// reserved-prefix check: the server does NOT enforce the _hog
		// reservation (its name validation is pattern-only), so the
		// client is the enforcement point — a committed _hog_row_id
		// user column collides with compaction's reserved row-id
		// carrier and breaks name-based readers fleet-wide
		if (StringUtil::StartsWith(StringUtil::Lower(col.Name().GetIdentifierName()), RESERVED_COLUMN_PREFIX)) {
			throw InvalidInputException("hoglake: column name \"%s\" uses the reserved \"%s\" prefix "
			                            "(hoglake internal columns, e.g. _hog_row_id)",
			                            col.Name().GetIdentifierName(), RESERVED_COLUMN_PREFIX);
		}
		// CI-conflict check BEFORE the eager server commit (the server
		// checks exact-case only; a committed CI collision poisons the
		// table for every DuckDB client)
		for (auto &wire_col : wire.columns) {
			if (StringUtil::CIEquals(wire_col.name, col.Name().GetIdentifierName())) {
				throw CatalogException("hoglake: column \"%s\" already exists in table \"%s.%s\" (identifiers "
				                       "are case-insensitive)",
				                       col.Name().GetIdentifierName(), ns, exact_name);
			}
		}
		if (col.HasDefaultValue()) {
			throw NotImplementedException("hoglake: ADD COLUMN with DEFAULT is not supported (the wire contract "
			                              "has no column defaults; added columns read as NULL)");
		}
		op.op = "add_column";
		op.column = HoglakeTypes::FromDuckDBType(col.Name().GetIdentifierName(), col.Type(), true);
		break;
	}
	case AlterTableType::REMOVE_COLUMN: {
		auto &remove = alter_table.Cast<RemoveColumnInfo>();
		if (remove.if_column_exists && !table.ColumnExists(remove.removed_column)) {
			return;
		}
		op.op = "drop_column";
		op.name = ResolveWireColumn(wire, remove.removed_column.GetIdentifierName()).name;
		break;
	}
	case AlterTableType::ALTER_COLUMN_TYPE: {
		auto &change = alter_table.Cast<ChangeColumnTypeInfo>();
		auto typed_column = change.column_name.GetIdentifierName();
		if (change.expression && !IsSimpleCast(*change.expression, typed_column)) {
			// promote_column is metadata-only: a USING expression would
			// be silently dropped, leaving every row untransformed
			throw NotImplementedException(
			    "hoglake: ALTER COLUMN ... TYPE with a USING expression is not supported (the wire's "
			    "promote_column is metadata-only and cannot transform data); only plain type promotions "
			    "(e.g. INTEGER -> BIGINT) are possible");
		}
		op.op = "promote_column";
		op.name = ResolveWireColumn(wire, typed_column).name;
		// the server's promotion lattice validates the target
		op.to = HoglakeTypes::FromDuckDBType(op.name, change.target_type, true).type;
		break;
	}
	case AlterTableType::SET_PARTITIONED_BY: {
		auto &set_partitioned = alter_table.Cast<SetPartitionedByInfo>();
		op.op = "set_partition_spec";
		for (auto &expr : set_partitioned.partition_keys) {
			op.fields.push_back(ParsePartitionExpression(wire, *expr));
		}
		break;
	}
	case AlterTableType::SET_SORTED_BY: {
		auto &set_sorted = alter_table.Cast<SetSortedByInfo>();
		op.op = "set_sort_order";
		for (auto &order : set_sorted.orders) {
			if (order.expression->GetExpressionClass() != ExpressionClass::COLUMN_REF) {
				throw BinderException("hoglake: SET SORTED BY supports plain columns only");
			}
			auto &col_ref = order.expression->Cast<ColumnRefExpression>();
			HoglakeSortField sort_field;
			sort_field.source_field_id = FindFieldId(wire, col_ref.GetColumnName().GetIdentifierName());
			sort_field.direction = order.type == OrderType::DESCENDING ? "desc" : "asc";
			sort_field.null_order = order.null_order == OrderByNullType::NULLS_FIRST ? "nulls_first" : "nulls_last";
			op.sort_fields.push_back(sort_field);
		}
		break;
	}
	default:
		throw NotImplementedException("hoglake: this ALTER TABLE operation is not supported by the wire contract "
		                              "(supported: RENAME TABLE/COLUMN, ADD/DROP COLUMN, ALTER COLUMN TYPE "
		                              "promotions, SET PARTITIONED BY, SET SORTED BY)");
	}

	// eager DDL: one atomic /alter commit (its own server snapshot)
	auto altered = hoglake_transaction.Api().AlterTable(ns, exact_name, {op});
	// the table now carries a post-pin table_altered change: commits
	// with deletes must not touch it (RequireDMLAllowed)
	hoglake_transaction.RecordAlteredTable(ns, exact_name);
	if (!StringUtil::CIEquals(new_entry_name, exact_name)) {
		hoglake_transaction.RecordAlteredTable(ns, altered.name);
	}

	// the pin predates the alter; the evolved entry reads at the
	// post-alter head so the new schema and its files line up
	auto read_travel = PostDDLTravel(hoglake_transaction);

	// swap the cached entry for the evolved table; the old entry stays
	// alive (retired) because the statement may still reference it
	auto altered_name = altered.name;
	auto existing = tables.find(exact_name);
	if (existing != tables.end()) {
		retired.push_back(std::move(existing->second));
		tables.erase(existing);
	}
	if (!StringUtil::CIEquals(new_entry_name, exact_name)) {
		// rename: re-key the CI name index
		auto names_entry = table_names.find(exact_name);
		if (names_entry != table_names.end()) {
			table_names.erase(names_entry);
		}
		table_names[altered_name].push_back(altered_name);
	}
	CacheTableInternal(hoglake_transaction, std::move(altered), read_travel, false);
}

//===--------------------------------------------------------------------===//
// Unsupported entry types
//===--------------------------------------------------------------------===//

optional_ptr<CatalogEntry> HoglakeSchemaEntry::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	throw NotImplementedException("CREATE VIEW is not supported for hoglake yet");
}

optional_ptr<CatalogEntry> HoglakeSchemaEntry::CreateFunction(CatalogTransaction transaction,
                                                              CreateFunctionInfo &info) {
	throw NotImplementedException("hoglake does not support CREATE FUNCTION");
}

optional_ptr<CatalogEntry> HoglakeSchemaEntry::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
                                                           TableCatalogEntry &table) {
	throw NotImplementedException("hoglake does not support indexes");
}

optional_ptr<CatalogEntry> HoglakeSchemaEntry::CreateSequence(CatalogTransaction transaction,
                                                              CreateSequenceInfo &info) {
	throw NotImplementedException("hoglake does not support sequences");
}

optional_ptr<CatalogEntry> HoglakeSchemaEntry::CreateTableFunction(CatalogTransaction transaction,
                                                                   CreateTableFunctionInfo &info) {
	throw NotImplementedException("hoglake does not support table functions");
}

optional_ptr<CatalogEntry> HoglakeSchemaEntry::CreateCopyFunction(CatalogTransaction transaction,
                                                                  CreateCopyFunctionInfo &info) {
	throw NotImplementedException("hoglake does not support copy functions");
}

optional_ptr<CatalogEntry> HoglakeSchemaEntry::CreatePragmaFunction(CatalogTransaction transaction,
                                                                    CreatePragmaFunctionInfo &info) {
	throw NotImplementedException("hoglake does not support pragma functions");
}

optional_ptr<CatalogEntry> HoglakeSchemaEntry::CreateCollation(CatalogTransaction transaction,
                                                               CreateCollationInfo &info) {
	throw NotImplementedException("hoglake does not support collations");
}

optional_ptr<CatalogEntry> HoglakeSchemaEntry::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	throw NotImplementedException("hoglake does not support user-defined types");
}

} // namespace duckdb
