#include "common/hoglake_types.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

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

string HoglakeTypes::CanonicalTimestamp(timestamp_t timestamp) {
	date_t date;
	dtime_t time;
	Timestamp::Convert(timestamp, date, time);
	int32_t hh, mm, ss, micros;
	Time::Convert(time, hh, mm, ss, micros);
	auto result = StringUtil::Format("%sT%02d:%02d:%02d", CanonicalDate(date), hh, mm, ss);
	if (micros != 0) {
		result += StringUtil::Format(".%06d", micros);
	}
	return result;
}

string HoglakeTypes::CanonicalDate(date_t date) {
	int32_t yyyy, mm, dd;
	Date::Convert(date, yyyy, mm, dd);
	if (yyyy < 0 || yyyy > 9999) {
		// python datetime cannot represent these either; refuse rather
		// than silently diverge from the isoformat convention
		throw InvalidInputException("hoglake: partition value year %d is outside the wire's isoformat range", yyyy);
	}
	return StringUtil::Format("%04d-%02d-%02d", yyyy, mm, dd);
}

} // namespace duckdb
