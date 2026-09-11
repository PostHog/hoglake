#include "common/hoglake_types.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

LogicalType HoglakeTypes::ToDuckDBType(const HoglakeColumn &column) {
	auto &type = column.type;
	if (type == "boolean") {
		return LogicalType::BOOLEAN;
	}
	if (type == "int") {
		return LogicalType::INTEGER;
	}
	if (type == "long") {
		return LogicalType::BIGINT;
	}
	if (type == "float") {
		return LogicalType::FLOAT;
	}
	if (type == "double") {
		return LogicalType::DOUBLE;
	}
	if (type == "string") {
		return LogicalType::VARCHAR;
	}
	if (type == "binary") {
		return LogicalType::BLOB;
	}
	if (type == "date") {
		return LogicalType::DATE;
	}
	if (type == "time") {
		return LogicalType::TIME;
	}
	if (type == "timestamp") {
		return LogicalType::TIMESTAMP;
	}
	if (type == "timestamptz") {
		return LogicalType::TIMESTAMP_TZ;
	}
	if (type == "uuid") {
		return LogicalType::UUID;
	}
	if (type == "decimal") {
		if (column.precision <= 0) {
			throw InvalidInputException("hoglake decimal column \"%s\" is missing precision/scale type_params",
			                            column.name);
		}
		return LogicalType::DECIMAL(NumericCast<uint8_t>(column.precision), NumericCast<uint8_t>(column.scale));
	}
	throw InvalidInputException("Unknown hoglake column type \"%s\" for column \"%s\"", type, column.name);
}

HoglakeColumnDef HoglakeTypes::FromDuckDBType(const string &name, const LogicalType &type, bool nullable) {
	HoglakeColumnDef def;
	def.name = name;
	def.nullable = nullable;
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		def.type = "boolean";
		break;
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
		def.type = "int";
		break;
	case LogicalTypeId::BIGINT:
		def.type = "long";
		break;
	case LogicalTypeId::FLOAT:
		def.type = "float";
		break;
	case LogicalTypeId::DOUBLE:
		def.type = "double";
		break;
	case LogicalTypeId::VARCHAR:
		def.type = "string";
		break;
	case LogicalTypeId::BLOB:
		def.type = "binary";
		break;
	case LogicalTypeId::DATE:
		def.type = "date";
		break;
	case LogicalTypeId::TIME:
		def.type = "time";
		break;
	case LogicalTypeId::TIMESTAMP:
		def.type = "timestamp";
		break;
	case LogicalTypeId::TIMESTAMP_TZ:
		def.type = "timestamptz";
		break;
	case LogicalTypeId::UUID:
		def.type = "uuid";
		break;
	case LogicalTypeId::DECIMAL:
		def.type = "decimal";
		def.precision = DecimalType::GetWidth(type);
		def.scale = DecimalType::GetScale(type);
		break;
	default:
		throw InvalidInputException(
		    "Type %s is not supported in hoglake tables (the hoglake column-type set is flat: "
		    "boolean, int, long, float, double, decimal, date, time, timestamp, timestamptz, string, uuid, binary)",
		    type.ToString());
	}
	return def;
}

} // namespace duckdb
