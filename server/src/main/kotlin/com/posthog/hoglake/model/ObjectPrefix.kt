package com.posthog.hoglake.model

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.UUID

/**
 * A literal startsWith is not enough: `s3://b/demo/../victim/x` starts
 * with `s3://b/demo/` but some reader stacks normalize dot segments,
 * re-addressing the object OUTSIDE the prefix. Whitespace/control
 * characters are refused outright (they survive into removal-queue
 * rows and reader URIs), as are empty and dot segments. So are their
 * ENCODED spellings — `%2E`, `%2F` — and backslashes: S3 stores them
 * literally, but this check is the only tenant boundary the maintenance
 * plane has (it runs under one IAM identity for every tenant), and it must
 * not depend on no reader stack ever decoding a key.
 */
internal fun pathStaysUnder(
    path: String,
    prefix: String,
): Boolean {
    if (!path.startsWith(prefix)) return false
    if (path.any { it.isWhitespace() || it.isISOControl() || it == '\\' }) return false
    if (ENCODED_DOT_OR_SLASH.containsMatchIn(path)) return false
    val relative = path.removePrefix(prefix)
    if (relative.isEmpty()) return false
    return relative.split('/').none { it.isEmpty() || it == "." || it == ".." }
}

private val ENCODED_DOT_OR_SLASH = Regex("%2[eEfF]")

/**
 * A table's object-prefix layout: the top-level integer column whose
 * identity partition value names each file's prefix.
 */
data class ObjectPrefixLayout(
    val fieldId: Long,
    val fieldName: String,
    val colType: ColType,
)

/**
 * The per-value object layout of a table with an object-prefix field:
 * every data file and deletion vector of the table lives under
 * `<data_path>/data/<ns>/<table>/<field>=<value>/`, where value is the
 * file's identity partition value for that field. An IAM statement whose
 * Resource is the prefix's ARN (`arn:aws:s3:::` + the prefix without
 * `s3://`) with the value replaced by a principal tag, followed by `*`,
 * then grants exactly one value's objects.
 *
 * This is the ONE path function. Compaction builds its output paths
 * here, commit validation's containment check ([pathStaysUnder]) lives
 * here, and the layout checks that follow use the same functions, so
 * the layout cannot drift between them. The value is the canonical decimal
 * string of the partition value, never percent-encoded: namespace, table
 * and column names cannot contain `=` (`Identifiers.PATTERN`,
 * `^[A-Za-z_][A-Za-z0-9_-]{0,127}$`, which [NAME] repeats), so a value
 * segment never collides with a name. The trailing `/` on every prefix,
 * together with [pathStaysUnder], is what keeps `team_id=4/` from
 * matching `team_id=42/`.
 *
 * THE STATS PROOF ([checkFieldStats]) has two callers. The commit makes
 * it on a file's inline stats and refuses the file when it fails. A file
 * with deferred stats registers `pending`, and the hydrator makes it on
 * the footer: a file that fails is still flipped to `provided` with its
 * truthful stats, and `hog_data_file.prefix_quarantined` is set.
 *
 * THE MOUNT PREDICATE: a reader that serves one value's files of a table
 * with the option (a per-value mount) includes only files with
 * `stats_state = 'provided' AND NOT prefix_quarantined`. A pending file
 * has not been proven yet, a failed one cannot be, and a quarantined one
 * was disproven; all three stay out, so the read is fail-closed.
 *
 * Shared vectors: `pyhoglake/tests/vectors/object_prefix_vectors.json`.
 */
object ObjectPrefix {
    /**
     * Column types an object-prefix field may have: the integer types
     * whose values are non-negative longs. uint64 is excluded because it
     * maps to Iceberg decimal(20,0), and every allowed promotion stays
     * inside this set.
     */
    val ALLOWED_TYPES: Set<ColType> =
        setOf(
            ColType.INT8,
            ColType.INT16,
            ColType.INT,
            ColType.LONG,
            ColType.UINT8,
            ColType.UINT16,
            ColType.UINT32,
        )

    /**
     * Canonical non-negative decimal: no sign, no leading zero, ASCII
     * digits only. The regex runs before parsing because
     * `String.toLongOrNull` accepts non-ASCII digits ("٤٢" parses as 42).
     */
    private val CANONICAL = Regex("^(0|[1-9][0-9]{0,18})$")

    /** Must equal `Identifiers.PATTERN` (service package), the name CHECK in schema.sql. */
    internal val NAME = Regex("^[A-Za-z_][A-Za-z0-9_-]{0,127}$")

    /**
     * The value [raw] names as a long, or null when [raw] is not a
     * canonical decimal in [type]'s non-negative range. Null means the
     * value cannot name a prefix, and a caller refuses the file.
     */
    fun parseValue(
        raw: String?,
        type: ColType,
    ): Long? {
        if (raw == null || type !in ALLOWED_TYPES || !CANONICAL.matches(raw)) return null
        val value = raw.toLongOrNull() ?: return null
        return if (value <= maxValue(type)) value else null
    }

    private fun maxValue(type: ColType): Long =
        when (type) {
            ColType.INT8 -> Byte.MAX_VALUE.toLong()
            ColType.INT16 -> Short.MAX_VALUE.toLong()
            ColType.INT -> Int.MAX_VALUE.toLong()
            ColType.UINT8 -> 255L
            ColType.UINT16 -> 65_535L
            ColType.UINT32 -> 4_294_967_295L
            ColType.LONG -> Long.MAX_VALUE
            else -> throw IllegalArgumentException("$type cannot be an object-prefix field")
        }

    /**
     * The key_index of the one identity partition field on [fieldId] in
     * [fields] (ordered by key_index), or null when the spec has none or
     * more than one.
     */
    fun keyIndex(
        fields: List<PartitionFieldDef>,
        fieldId: Long,
    ): Int? =
        fields.withIndex()
            .filter { it.value.sourceFieldId == fieldId && it.value.transform == Transform.IDENTITY }
            .map { it.index }
            .singleOrNull()

    /** `<data_path>/data/<ns>/<table>/`, the table's root under its catalog. */
    fun tableRoot(
        dataPath: String,
        namespace: String,
        table: String,
    ): String = "${dataPath.trimEnd('/')}/data/$namespace/$table/"

    /**
     * `<table root><field>=<value>/`, always with the trailing `/`.
     * [value] must already be canonical for the layout's type; a value
     * that is not is a caller bug, not a client error.
     */
    fun prefix(
        dataPath: String,
        namespace: String,
        table: String,
        layout: ObjectPrefixLayout,
        value: String,
    ): String {
        require(NAME.matches(namespace) && NAME.matches(table) && NAME.matches(layout.fieldName)) {
            "names in an object prefix must match ${NAME.pattern}: ns=$namespace table=$table field=${layout.fieldName}"
        }
        require(parseValue(value, layout.colType) != null) {
            "object-prefix value '$value' is not canonical for ${layout.colType}"
        }
        return tableRoot(dataPath, namespace, table) + "${layout.fieldName}=$value/"
    }

    /**
     * A new data file's path: under the value's prefix when the table
     * has a [layout], else `<table root><uuid>.parquet`, the path
     * compaction writes today. Other writers already put files in
     * subdirectories of the root (pyhoglake prepared commits under
     * `<idempotency_key>/`, millrace under `<build>/`, DuckDB INSERT under
     * hive `<col>=<value>/` directories), so a path's shape does not show
     * whether it predates the layout.
     */
    fun dataFilePath(
        dataPath: String,
        namespace: String,
        table: String,
        layout: ObjectPrefixLayout?,
        value: String?,
        uuid: UUID = UUID.randomUUID(),
    ): String =
        if (layout == null) {
            tableRoot(dataPath, namespace, table) + "$uuid.parquet"
        } else {
            val prefixValue = requireNotNull(value) { "an object-prefix file needs a value" }
            prefix(dataPath, namespace, table, layout, prefixValue) + "$uuid.parquet"
        }

    /** Whether [path] is an object under [prefix] (see [pathStaysUnder]). */
    fun isUnder(
        path: String,
        prefix: String,
    ): Boolean = pathStaysUnder(path, prefix)

    /**
     * The Iceberg single-value encoding of [value] for [type]: 4-byte
     * little-endian for the types Iceberg reads as int, 8-byte for uint32
     * and long, as `IcebergSingleValue.encode` writes them (ObjectPrefixTest
     * compares the two for every allowed type). Encoded here so this
     * package does not depend on `stats`. [value] must be in [type]'s range.
     */
    fun expectedBound(
        type: ColType,
        value: Long,
    ): ByteArray {
        require(value in 0..maxValue(type)) { "$value is outside the object-prefix range of $type" }
        return when (type) {
            ColType.INT8, ColType.INT16, ColType.UINT8, ColType.UINT16, ColType.INT ->
                ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value.toInt()).array()
            else -> ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(value).array()
        }
    }

    /** The outcome of [checkFieldStats]; only [OK] lets a file through. */
    enum class Verdict {
        OK,
        MISSING_STATS,
        NULL_BOUNDS,
        NULLS_PRESENT,
        BOUND_MISMATCH,
    }

    /**
     * Whether a file's stats for the prefix field prove that every row
     * holds [value]: both bounds present and equal to the value's
     * encoding, and no nulls. [stats] is the field's sanitized column
     * stats, or null when the file has none for the field; stats for
     * another column count as none.
     */
    fun checkFieldStats(
        layout: ObjectPrefixLayout,
        value: Long,
        stats: ColumnStats?,
    ): Verdict {
        if (stats == null || stats.fieldId != layout.fieldId) return Verdict.MISSING_STATS
        val lower = stats.lowerBound
        val upper = stats.upperBound
        if (lower == null || upper == null) return Verdict.NULL_BOUNDS
        if (stats.nullCount != 0L) return Verdict.NULLS_PRESENT
        if (value !in 0..maxValue(layout.colType)) return Verdict.BOUND_MISMATCH
        val expected = expectedBound(layout.colType, value)
        val matches = lower.contentEquals(expected) && upper.contentEquals(expected)
        return if (matches) Verdict.OK else Verdict.BOUND_MISMATCH
    }
}
