//===----------------------------------------------------------------------===//
// hoglake column type <-> DuckDB LogicalType mapping (the closed flat
// type set of the wire contract; pyhoglake types.py is the sibling).
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"
#include "common/hoglake_wire.hpp"

namespace duckdb {

struct HoglakeTypes {
	//! Wire column -> DuckDB type. Throws on unknown type strings.
	static LogicalType ToDuckDBType(const HoglakeColumn &column);
	//! DuckDB type -> wire column def (name/type/precision/scale).
	//! Throws InvalidInputException for types outside the hoglake set
	//! (nested types included — the wire schema is flat).
	static HoglakeColumnDef FromDuckDBType(const string &name, const LogicalType &type, bool nullable);
};

} // namespace duckdb
