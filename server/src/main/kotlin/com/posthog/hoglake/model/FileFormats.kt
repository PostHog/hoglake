package com.posthog.hoglake.model

/** Storage-format contract shared by table metadata, registration and maintenance paths. */
object FileFormats {
    const val TABLE_PROPERTY: String = "write.format.default"
    const val PARQUET: String = "parquet"
    const val CLICKHOUSE_MERGETREE_PACKED: String = "clickhouse-mergetree-packed"

    val allowed: Set<String> = setOf(PARQUET, CLICKHOUSE_MERGETREE_PACKED)

    /**
     * The types the first packed-part adapter maps losslessly between Hoglake, Arrow and ClickHouse.
     * Adding a type requires an adapter round-trip test before the server admits it.
     */
    val packedColumnTypes: Set<ColType> =
        setOf(
            ColType.BOOLEAN,
            ColType.INT8,
            ColType.INT16,
            ColType.INT,
            ColType.LONG,
            ColType.UINT8,
            ColType.UINT16,
            ColType.UINT32,
            ColType.UINT64,
            ColType.FLOAT,
            ColType.DOUBLE,
            ColType.DATE,
            ColType.TIMESTAMP_S,
            ColType.TIMESTAMP_MS,
            ColType.TIMESTAMP,
            ColType.TIMESTAMP_NS,
            ColType.TIMESTAMPTZ,
            ColType.STRING,
            ColType.BINARY,
        )

    fun tableFormat(properties: Map<String, String>): String = properties[TABLE_PROPERTY]?.lowercase() ?: PARQUET

    fun isPacked(properties: Map<String, String>): Boolean = tableFormat(properties) == CLICKHOUSE_MERGETREE_PACKED

    /**
     * [properties] as every read reports them: the format key is never stored
     * (`hog_table.file_format` is authoritative), so it is absent for Parquet
     * and the canonical lowercase value otherwise. Write paths that answer with
     * the properties they were given go through this so the response equals
     * the next read.
     */
    fun canonicalProperties(properties: Map<String, String>): Map<String, String> {
        val format = tableFormat(properties)
        val rest = properties - TABLE_PROPERTY
        return if (format == PARQUET) rest else rest + (TABLE_PROPERTY to format)
    }
}

/** Jackson value filter that keeps legacy Parquet registrations absent on stored payloads. */
class ParquetFileFormatFilter {
    override fun equals(other: Any?): Boolean = other == FileFormats.PARQUET

    override fun hashCode(): Int = 0
}
