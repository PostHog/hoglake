//===----------------------------------------------------------------------===//
// hoglake column type <-> DuckDB LogicalType mapping (the closed flat
// type set of the wire contract; pyhoglake types.py is the sibling).
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "common/hoglake_wire.hpp"

namespace duckdb {
class ClientContext;
}

namespace duckdb {

struct HoglakeTypes {
	//! Wire column -> DuckDB type. Throws on unknown type strings.
	static LogicalType ToDuckDBType(const HoglakeColumn &column);
	//! DuckDB type -> wire column def (name/type/precision/scale).
	//! Throws InvalidInputException for types outside the hoglake set
	//! (nested types included — the wire schema is flat).
	static HoglakeColumnDef FromDuckDBType(const string &name, const LogicalType &type, bool nullable);

	//! Canonical wire timestamp string, byte-identical to Python's
	//! datetime.isoformat() (pyhoglake's wire_string convention):
	//! YYYY-MM-DDTHH:MM:SS, plus .%06d only when microseconds != 0.
	static string CanonicalTimestamp(timestamp_t timestamp);
	//! Canonical wire date string (date.isoformat(): YYYY-MM-DD).
	static string CanonicalDate(date_t date);
	//! Canonical ISO-8601 UTC instant ("...Z") from ANY timestamp-ish
	//! Value, parsed with INSTANT semantics through the SESSION's
	//! TIMESTAMPTZ cast (context-aware, so ICU's session-TimeZone cast
	//! applies when loaded): explicit numeric offsets are honored
	//! (never dropped); naive inputs mean exactly what the same
	//! literal's ::TIMESTAMPTZ cast means in this session — the
	//! session TimeZone under ICU, UTC in ICU-less builds.
	static string CanonicalInstant(ClientContext &context, const Value &value);
};

} // namespace duckdb
