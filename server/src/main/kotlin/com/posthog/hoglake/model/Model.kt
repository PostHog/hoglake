package com.posthog.hoglake.model

import java.time.Instant
import java.util.UUID

/**
 * Domain model. These are the shapes the persistence layer returns and
 * the API layer serializes; they mirror the OpenAPI schemas
 * (src/main/resources/openapi/hoglake.yaml) and the tables in
 * db/migration/V1__init.sql.
 *
 * [ColType] below is the closed column-type vocabulary. Every member has
 * a defined Iceberg facade mapping ([ColType.icebergType],
 * docs/iceberg-federation.md §2) — that is the membership rule, not a
 * nice-to-have: a type whose facade story is undefined may not be used
 * in a hoglake table. Extending the set is a migration (the
 * hog_column.col_type CHECK) plus an §2 table row, never an ad-hoc enum
 * entry. Type names DuckLake has and hoglake permanently refuses live
 * in [ColType.REFUSALS], each with the reason, because "we will never
 * support this, and here is why" is a different answer from "typo".
 *
 * The last three members — [ColType.LIST], [ColType.STRUCT],
 * [ColType.MAP] — are CONTAINERS: they carry no values of their own,
 * they have children ([ColumnDef.children]), and they never carry
 * stats bounds. [ColType.isNested] is the one predicate every surface
 * asks; see docs/iceberg-federation.md §2.8.
 *
 * Member ORDER is load-bearing twice over: the migration CHECK and the
 * OpenAPI enum must list the same names in the same order
 * (`ScalarTypeParityTest`), and the fuzz seed corpus encodes
 * `ColType.ordinal` as its first byte — which is why new members are
 * APPENDED, never inserted (phase 1 inserted, and re-pointed every
 * committed seed at a different type).
 */
enum class ColType {
    BOOLEAN,
    INT8,
    INT16,
    INT,
    LONG,
    UINT8,
    UINT16,
    UINT32,
    UINT64,
    FLOAT,
    DOUBLE,
    DECIMAL,
    DATE,
    TIME,
    TIMESTAMP_S,
    TIMESTAMP_MS,
    TIMESTAMP,
    TIMESTAMP_NS,
    TIMESTAMPTZ,
    STRING,
    JSON,
    UUID_T,
    BINARY,

    /**
     * Iceberg V3 variant. A catalog SCALAR — no children, no
     * [ColumnDef.children] — whose PARQUET storage is nonetheless a
     * group (`metadata`/`value`/`typed_value`). "Scalar" here means one
     * catalog node, not one parquet node, and every surface that walks
     * parquet has to know the difference: see FooterStats' variant arm
     * and the compaction exclusion.
     */
    VARIANT,

    // ---- containers (phase 2) ----
    LIST,
    STRUCT,
    MAP,
    ;

    /** Wire/DB name (lowercase; UUID_T stored as "uuid"). */
    val wire: String get() = if (this == UUID_T) "uuid" else name.lowercase()

    /**
     * True for the three container types. A nested column holds no
     * values, so it gets no parquet leaf, no stats row and no bounds;
     * its scalar descendants are what data and statistics live on.
     */
    val isNested: Boolean get() = this == LIST || this == STRUCT || this == MAP

    /**
     * How many children this type requires, or null when the count is
     * not fixed (struct: one or more). `list` is exactly one (the
     * element); `map` is exactly two (key, value) — the parquet and
     * Iceberg shapes both say so, and a bare `list`/`map` with the wrong
     * child count is a named 422, never a 500 further down.
     */
    val requiredChildCount: Int?
        get() =
            when (this) {
                LIST -> 1
                MAP -> 2
                else -> null
            }

    companion object {
        /** The DuckLake geometry family — one refusal reason, many names. */
        private val GEOMETRY_TYPES =
            listOf(
                "point",
                "linestring",
                "polygon",
                "multipoint",
                "multilinestring",
                "multipolygon",
                "linestring_z",
                "geometrycollection",
            )

        /** The synthetic child name Iceberg (and parquet) give a list's element. */
        const val LIST_ELEMENT = "element"

        /** The synthetic child names Iceberg (and parquet) give a map's entries. */
        const val MAP_KEY = "key"
        const val MAP_VALUE = "value"

        /**
         * The canonical child names for a container, by ordinal. Struct
         * children keep the user's names, so struct is absent.
         */
        fun syntheticChildNames(type: ColType): List<String>? =
            when (type) {
                LIST -> listOf(LIST_ELEMENT)
                MAP -> listOf(MAP_KEY, MAP_VALUE)
                else -> null
            }

        /**
         * DuckLake type names hoglake refuses PERMANENTLY, mapped to the
         * 422 detail that names the type and the reason. These are not
         * "not yet": each is unmappable to Iceberg, so no facade story
         * exists to write (docs/iceberg-federation.md §2).
         */
        val REFUSALS: Map<String, String> =
            buildMap {
                for (t in listOf("int128", "uint128")) {
                    put(
                        t,
                        "type '$t' needs 39 decimal digits, which exceeds Iceberg's widest exact " +
                            "numeric type decimal(38), and is not supported",
                    )
                }
                for (t in listOf("timetz", "interval")) {
                    put(t, "type '$t' has no Iceberg mapping and is not supported")
                }
                for (t in GEOMETRY_TYPES) {
                    put(
                        t,
                        "type '$t' is a DuckLake geometry type; geometry is out of scope for " +
                            "hoglake and is not supported",
                    )
                }
            }

        /**
         * Strict parse: throws [IllegalArgumentException] on anything
         * outside the vocabulary. The persistence layer's reader (the
         * DB CHECK already narrowed the input) and internal callers use
         * this; request surfaces use [parseWire] so a refused name gets
         * its named reason instead of "unknown type".
         */
        fun fromWire(s: String): ColType {
            // Normalise ONCE, then decide. The previous shape
            // (`if (s == "uuid") UUID_T else valueOf(s.uppercase())`) made
            // uuid the single case-SENSITIVE name — "UUID" missed the
            // literal and fell into valueOf, which has no UUID entry — and
            // simultaneously let the internal spelling "UUID_T" through as
            // a valid wire name, because valueOf accepts it verbatim.
            val name = s.uppercase()
            if (name == "UUID_T") {
                throw IllegalArgumentException("'$s' is the internal enum name, not a wire type name")
            }
            return if (name == "UUID") UUID_T else valueOf(name)
        }

        /**
         * Wire parse for request surfaces. A name in [REFUSALS] fails
         * with that entry's reason; anything else unknown fails with
         * [unknown]'s message. Both are 422 Validation — the difference
         * is whether the caller should fix a typo or stop trying.
         *
         * The refusal lookup lowercases first because [fromWire] is
         * case-insensitive (it uppercases before `valueOf`). Without
         * that, "INT" was accepted while "INT128" fell through to the
         * generic unknown-type message — the one answer the refusals
         * exist to prevent.
         */
        fun parseWire(
            s: String,
            unknown: () -> String,
        ): ColType {
            REFUSALS[s.lowercase()]?.let { throw HoglakeException.Validation(it) }
            return try {
                fromWire(s)
            } catch (_: IllegalArgumentException) {
                throw HoglakeException.Validation(unknown())
            }
        }
    }
}

/**
 * The Iceberg type a [ColType] presents as through the read-only facade
 * (docs/iceberg-federation.md §2). Two invariants ride on this mapping:
 *
 *  1. `hog_file_column_stats.lower_bound`/`upper_bound` hold the Iceberg
 *     single-value serialization of the MAPPED type, so manifest
 *     generation is a mechanical copy. timestamp_s/timestamp_ms bounds
 *     are therefore microseconds, like plain timestamp.
 *  2. A type promotion is legal only when the induced Iceberg schema
 *     evolution is legal — same mapped type, or one of Iceberg's own
 *     widenings (int->long, float->double, decimal precision).
 */
enum class IcebergType {
    BOOLEAN,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    DECIMAL,
    DATE,
    TIME,
    TIMESTAMP,

    /** Iceberg V3 `timestamp_ns`; its single-value encoding is nanos, not micros. */
    TIMESTAMP_NS,
    TIMESTAMPTZ,
    STRING,
    UUID,
    BINARY,

    /**
     * Iceberg V3 variant. [isScalar] is true for it — it is ONE catalog
     * node with one field id — but it has no single-value encoding
     * either, so nothing ever writes a variant bound. The bounds paths
     * reach it through [ColType.VARIANT]'s own arms, which refuse.
     */
    VARIANT,

    /**
     * The three Iceberg V2 container types. They exist here so
     * [ColType.icebergType] stays TOTAL — every hoglake type names its
     * facade shape — but they carry neither a single-value encoding nor
     * a promotion: [isScalar] is the predicate the bounds and promotion
     * paths ask.
     */
    LIST,
    STRUCT,
    MAP,
    ;

    val wire: String get() = name.lowercase()

    /** False for the three containers, which have no single-value encoding. */
    val isScalar: Boolean get() = this != LIST && this != STRUCT && this != MAP
}

/**
 * The widest parquet INT(w, unsigned) leaf this type can read: the
 * largest w for which the type's own domain contains [0, 2^w), or -1
 * for "none" — either the type is not integral, or it cannot even hold
 * an 8-bit magnitude (int8 tops out at 127).
 *
 * Shared by the hydrator's footer decode and the compaction rewriter
 * ON PURPOSE. They used to disagree: the hydrator refused an
 * unsigned-annotated leaf it could not represent while the rewriter
 * copied it through IDENTITY and re-stamped it with the live column's
 * annotation, so compaction laundered exactly the files the hydrator
 * had rejected. One definition, two callers, no drift.
 *
 * Signed-width narrowing (an INT(16, signed) file under an int8 column)
 * is deliberately NOT covered here: no legal promotion produces it —
 * promotions only ever widen the COLUMN — so it can only come from a
 * writer disagreeing with its own DDL, which is the pre-existing
 * behaviour for int and long too.
 */
val ColType.maxUnsignedParquetWidth: Int
    get() =
        when (this) {
            ColType.UINT64 -> 64
            ColType.UINT32, ColType.LONG, ColType.TIME,
            ColType.TIMESTAMP_S, ColType.TIMESTAMP_MS, ColType.TIMESTAMP,
            ColType.TIMESTAMP_NS, ColType.TIMESTAMPTZ,
            -> 32
            ColType.UINT16, ColType.INT, ColType.DATE -> 16
            ColType.UINT8, ColType.INT16 -> 8
            else -> -1
        }

/** This type's Iceberg facade mapping — see [IcebergType]. */
val ColType.icebergType: IcebergType
    get() =
        when (this) {
            ColType.BOOLEAN -> IcebergType.BOOLEAN
            // int8/int16 and the small unsigned widths all fit int32 exactly.
            ColType.INT8, ColType.INT16, ColType.UINT8, ColType.UINT16, ColType.INT -> IcebergType.INT
            // uint32 needs 33 bits to stay value-preserving, so it maps to long.
            ColType.UINT32, ColType.LONG -> IcebergType.LONG
            // uint64 has no signed 64-bit home: decimal(20,0) is the narrowest
            // Iceberg type that holds [0, 2^64).
            ColType.UINT64 -> IcebergType.DECIMAL
            ColType.FLOAT -> IcebergType.FLOAT
            ColType.DOUBLE -> IcebergType.DOUBLE
            ColType.DECIMAL -> IcebergType.DECIMAL
            ColType.DATE -> IcebergType.DATE
            ColType.TIME -> IcebergType.TIME
            ColType.TIMESTAMP_S, ColType.TIMESTAMP_MS, ColType.TIMESTAMP -> IcebergType.TIMESTAMP
            ColType.TIMESTAMP_NS -> IcebergType.TIMESTAMP_NS
            ColType.TIMESTAMPTZ -> IcebergType.TIMESTAMPTZ
            ColType.STRING, ColType.JSON -> IcebergType.STRING
            ColType.UUID_T -> IcebergType.UUID
            ColType.BINARY -> IcebergType.BINARY
            ColType.VARIANT -> IcebergType.VARIANT
            // Native, one for one: an Iceberg list/struct/map with the
            // SAME field ids on element/key/value (docs/iceberg-federation.md
            // §2.8). No conversion, no synthesized ids — which is what
            // makes the round trip through the facade an identity.
            ColType.LIST -> IcebergType.LIST
            ColType.STRUCT -> IcebergType.STRUCT
            ColType.MAP -> IcebergType.MAP
        }

enum class StatsState {
    PROVIDED,
    PENDING,
    FAILED,
    ;

    val wire: String get() = name.lowercase()

    companion object {
        fun fromWire(s: String) = valueOf(s.uppercase())
    }
}

/** Typed conflict vocabulary — mirrors the CHECK constraint on hog_snapshot_change. */
enum class ChangeKind {
    NAMESPACE_CREATED,
    NAMESPACE_DROPPED,
    TABLE_CREATED,
    TABLE_DROPPED,
    TABLE_ALTERED,
    TABLE_INSERTED_INTO,
    TABLE_DELETED_FROM,
    TABLE_COMPACTED,
    VIEW_CREATED,
    VIEW_DROPPED,
    ;

    val wire: String get() = name.lowercase()

    companion object {
        fun fromWire(s: String) = valueOf(s.uppercase())
    }
}

/** Iceberg-semantics partition transforms (docs/iceberg-federation.md §3). */
enum class Transform {
    IDENTITY,
    BUCKET,
    YEAR,
    MONTH,
    DAY,
    HOUR,
    ;

    val wire: String get() = name.lowercase()

    companion object {
        fun fromWire(s: String) = valueOf(s.uppercase())
    }
}

data class PartitionFieldDef(
    val sourceFieldId: Long,
    val transform: Transform,
    /** bucket(n): required for BUCKET, forbidden otherwise. */
    val transformParam: Int? = null,
)

data class PartitionSpec(
    val specId: Long,
    val fields: List<PartitionFieldDef>,
)

/** Sort direction for one sort-spec field (hog_sort_field.direction). */
enum class SortDirection {
    ASC,
    DESC,
    ;

    val wire: String get() = name.lowercase()

    companion object {
        fun fromWire(s: String) = valueOf(s.uppercase())
    }
}

/** Null placement for one sort-spec field (hog_sort_field.null_order). */
enum class NullOrder {
    NULLS_FIRST,
    NULLS_LAST,
    ;

    val wire: String get() = name.lowercase()

    companion object {
        fun fromWire(s: String) = valueOf(s.uppercase())
    }
}

data class SortFieldDef(
    val sourceFieldId: Long,
    val direction: SortDirection,
    val nullOrder: NullOrder,
)

/**
 * A table's versioned sort order. ADVISORY for writers (the server
 * never verifies file sortedness); BINDING for compaction rewrites.
 */
data class SortSpec(
    val sortId: Long,
    val fields: List<SortFieldDef>,
)

/**
 * The promotions ALTER permits: exactly the INTERSECTION of two
 * independently-owned sets, neither of which hoglake gets to invent.
 *
 *  1. DuckLake's documented promotion table
 *     (https://ducklake.select/docs/stable/duckdb/usage/schema_evolution
 *     — "Only type promotions are supported. Type promotions must be
 *     lossless"): int8 -> int16/int32/int64; int16 -> int32/int64;
 *     int32 -> int64; uint8 -> uint16/uint32/uint64;
 *     uint16 -> uint32/uint64; uint32 -> uint64; float32 -> float64.
 *  2. Iceberg schema-evolution legality of the induced facade change
 *     ([icebergType]): same mapped type, or int -> long, float ->
 *     double, decimal precision widening.
 *
 * Being an intersection is the point, and it cuts both ways:
 *
 *  - Value-preserving is NOT sufficient. uint8 -> int and
 *    uint16 -> long lose nothing, and timestamp_s -> timestamp_ms ->
 *    timestamp is pure metadata, but DuckLake does not offer any of
 *    them, so neither do we — a hoglake catalog must never accept a
 *    DDL a DuckLake client would reject, or the two disagree about
 *    what the table IS.
 *  - DuckLake-legal is NOT sufficient either. uint8/uint16/uint32 ->
 *    uint64 are all in DuckLake's table, but uint64 maps to
 *    decimal(20,0), and int -> decimal / long -> decimal are not
 *    Iceberg evolutions: the promotion would silently make the lake
 *    unservable through the facade.
 *
 * `ScalarTypeParityTest` rebuilds both sets independently and asserts
 * this map equals their intersection, so a hand-edited entry fails.
 */
private val PROMOTIONS: Map<ColType, Set<ColType>> =
    mapOf(
        // Signed ladder: every step stays int-mapped or widens int -> long.
        ColType.INT8 to setOf(ColType.INT16, ColType.INT, ColType.LONG),
        ColType.INT16 to setOf(ColType.INT, ColType.LONG),
        ColType.INT to setOf(ColType.LONG),
        // Unsigned ladder, which stops at uint32: uint8/uint16 -> uint32
        // is int -> long in Iceberg terms, but anything -> uint64 would
        // be a jump into decimal(20,0).
        ColType.UINT8 to setOf(ColType.UINT16, ColType.UINT32),
        ColType.UINT16 to setOf(ColType.UINT32),
        ColType.FLOAT to setOf(ColType.DOUBLE),
    )

/** Widening promotions the ALTER path permits — see [PROMOTIONS]. */
fun ColType.canPromoteTo(target: ColType): Boolean = PROMOTIONS[this]?.contains(target) == true

/** What a promotion must do to the column's already-stored stats bounds. */
enum class BoundReencode {
    /** The mapped Iceberg type is unchanged, so the stored bytes already are correct. */
    NONE,

    /** 4-byte int bound -> 8-byte long bound, value preserved. */
    INT_TO_LONG,

    /** 4-byte float bound -> 8-byte double bound, value preserved. */
    FLOAT_TO_DOUBLE,
}

/**
 * Which re-encode a promotion forces on `hog_file_column_stats`.
 *
 * Keyed on the MAPPED Iceberg type rather than on the type names,
 * because the bound encoding is the mapped type's (§2.1). That is what
 * makes `uint8 -> uint32` widen (int -> long, 4 bytes to 8) while
 * `uint8 -> uint16` does not (both int), without either being a special
 * case — and it is why a hardcoded `INT -> LONG` pair would silently
 * leave the unsigned ladder's bounds at the wrong width.
 *
 * Extracted from AlterService so it can be tested over every promotion
 * edge without a database.
 */
fun boundReencodeFor(
    from: ColType,
    to: ColType,
): BoundReencode =
    when {
        from.icebergType == to.icebergType -> BoundReencode.NONE
        from.icebergType == IcebergType.INT && to.icebergType == IcebergType.LONG ->
            BoundReencode.INT_TO_LONG
        from.icebergType == IcebergType.FLOAT && to.icebergType == IcebergType.DOUBLE ->
            BoundReencode.FLOAT_TO_DOUBLE
        else -> BoundReencode.NONE
    }

/**
 * One typed ALTER TABLE operation.
 *
 * Column-addressing ops name their target by a DOTTED PATH of column
 * names — `addr` for a top-level column, `addr.zip` for a field of the
 * struct `addr`. The identifier policy forbids `.` in a name, so the
 * path is unambiguous. Only STRUCT interiors are addressable: a path
 * that steps through a `list` or a `map` is a named 422, because Iceberg
 * has no rename/drop/add for an element, a key or a value.
 */
sealed class AlterOp {
    /**
     * Add a column. [parent] null appends a top-level column; a dotted
     * path names an existing STRUCT to append a field to (new field id,
     * ordinal = max sibling + 1).
     */
    data class AddColumn(val def: ColumnDef, val parent: String? = null) : AlterOp()

    data class DropColumn(val name: String) : AlterOp()

    data class RenameColumn(val from: String, val to: String) : AlterOp()

    data class PromoteColumn(val name: String, val to: ColType) : AlterOp()

    data class RenameTable(val newName: String) : AlterOp()

    data class SetTableComment(val comment: String?) : AlterOp()

    data class SetColumnComment(val name: String, val comment: String?) : AlterOp()

    data class SetProperties(val properties: Map<String, String>) : AlterOp()

    /** Replace the partition spec (empty list = unpartitioned). */
    data class SetPartitionSpec(val fields: List<PartitionFieldDef>) : AlterOp()

    /** Replace the sort order (empty list = unsorted). */
    data class SetSortOrder(val fields: List<SortFieldDef>) : AlterOp()
}

data class CatalogInfo(
    val catalogId: Long,
    val name: String,
    val dataPath: String,
    val headSnapshotId: Long,
    val schemaVersion: Long,
    /**
     * snapshot_time of the expiry floor snapshot, captured when the
     * sweep advanced the floor; null until expiry first advances it.
     * The reconciliation anchor: 410s below the floor cite this.
     */
    val earliestSnapshotTime: Instant? = null,
    /**
     * The lifecycle slice starts here (options/expiry/cleanup read it):
     * one hog_catalog row mapping serves every reader — the
     * OptionsService.LifecycleCatalog duplicate that drifted behind this
     * mapper is gone. null = snapshot expiry disabled.
     */
    val snapshotRetentionSeconds: Long? = null,
    /** Expiry never passes the min consumer offset when true. */
    val consumerFloor: Boolean = true,
    val earliestSnapshotId: Long = 0,
)

data class NamespaceInfo(
    val namespaceId: Long,
    val name: String,
)

/**
 * A requested column shape. Recursive: a container type
 * ([ColType.isNested]) carries its [children], which are themselves
 * [ColumnDef]s, and the server assigns their field ids at create/alter
 * time. Scalars carry `children = null`.
 *
 * Child naming follows Iceberg: a `list`'s single child is `element`, a
 * `map`'s two children are `key` and `value` (and the key is required),
 * while `struct` children keep the user's names. The server NORMALISES
 * nothing silently — a synthetic child sent under a different name is a
 * named 422, because a client that thinks it named the element
 * something else would then read a schema that disagrees with its own
 * DDL.
 */
data class ColumnDef(
    val name: String,
    val type: ColType,
    val typeParams: Map<String, Any?>? = null,
    val nullable: Boolean = true,
    val children: List<ColumnDef>? = null,
    val comment: String? = null,
)

/**
 * A materialized catalog column: the assigned [fieldId], the [ordinal]
 * that orders it AMONG ITS SIBLINGS (top-level columns share the null
 * parent), and, for containers, the materialized [children].
 *
 * [def]`.children` is deliberately NOT populated on a materialized
 * column — [children] is the one source of truth for the subtree, so
 * that "which field id is this child" has exactly one answer.
 */
data class Column(
    val fieldId: Long,
    val ordinal: Int,
    val def: ColumnDef,
    val children: List<Column> = emptyList(),
) {
    /** This node and every descendant, parents before children (depth-first). */
    fun selfAndDescendants(): List<Column> = buildList { collectInto(this) }

    private fun collectInto(out: MutableList<Column>) {
        out.add(this)
        for (c in children) c.collectInto(out)
    }
}

/**
 * Every node of a column FOREST, parents before children.
 *
 * The one thing callers reach for when "the table's columns" has to mean
 * every field id rather than every top-level column — which is what
 * hog_file_column_stats is keyed on, since bounds are per LEAF.
 */
fun List<Column>.allNodes(): List<Column> = flatMap { it.selfAndDescendants() }

/**
 * Depth of the deepest node in [defs], counting a top-level column as
 * depth 1, stopping at [cap]. Used by the create/alter depth cap.
 *
 * ITERATIVE, level by level, and that is the whole point: this function
 * is the depth CHECK, so it is the one place that must survive input the
 * check exists to refuse. The recursive version overflowed the stack at
 * roughly twenty thousand levels — a StackOverflowError out of the
 * validator, before the named 422 the caller was owed, from the code
 * whose documented job was preventing exactly that.
 *
 * [cap] bails early: past it the exact depth stops mattering, since
 * every answer means the same refusal. The return is therefore
 * `min(depth, cap)`, and a caller that needs to know whether it bailed
 * compares against [cap].
 */
fun columnDefDepth(
    defs: List<ColumnDef>,
    cap: Int = Int.MAX_VALUE,
): Int {
    var depth = 0
    var level = defs
    while (level.isNotEmpty() && depth < cap) {
        depth++
        level = level.flatMap { it.children ?: emptyList() }
    }
    return depth
}

/**
 * The maximum nesting depth a hoglake column may have, top-level
 * counting as 1. Eight is not a physical limit — parquet and Iceberg
 * have none — it is a blast-radius limit: every level multiplies the
 * field ids allocated, the parquet definition/repetition levels, and
 * the recursion every surface (DDL, footer stats, the compaction
 * rewriter, the client) performs per row. A request past it is a named
 * 422, refused BEFORE any field id is allocated.
 */
const val MAX_COLUMN_NESTING_DEPTH: Int = 8

data class TableInfo(
    val tableId: Long,
    val tableUuid: UUID,
    val namespace: String,
    val name: String,
    val columns: List<Column>,
    val recordCount: Long,
    val fileCount: Long,
    val fileSizeBytes: Long,
    /** Live partition spec; null = unpartitioned. */
    val partitionSpec: PartitionSpec? = null,
    /** Live sort order; null = unsorted. */
    val sortSpec: SortSpec? = null,
    val comment: String? = null,
    val properties: Map<String, String> = emptyMap(),
)

data class Snapshot(
    val snapshotId: Long,
    val snapshotTime: Instant,
    val schemaVersion: Long,
    val author: String?,
    val message: String?,
    val changes: List<SnapshotChange> = emptyList(),
)

data class SnapshotChange(
    val kind: ChangeKind,
    /** table_id, namespace_id, or view_id — every change kind names an object. */
    val objectId: Long,
)

data class DataFile(
    val dataFileId: Long,
    val tableId: Long,
    val path: String,
    val fileFormat: String,
    val recordCount: Long,
    val fileSizeBytes: Long,
    val footerSize: Long?,
    val rowIdStart: Long,
    val statsState: StatsState,
    val beginSnapshot: Long,
    val specId: Long? = null,
    /** Transformed partition values by key_index; null when unpartitioned. */
    val partitionValues: List<String?>? = null,
    /**
     * True for compaction outputs: row ids ride an explicit physical
     * `_hog_row_id` column (reserved parquet field id 2147483646) because
     * merged inputs need not be row-id-contiguous. When true,
     * row_id_start is min(input row ids) and has no positional meaning.
     */
    val explicitRowIds: Boolean = false,
    /**
     * The file's ORDERING-KEY range, populated by the files LISTING
     * alone (GET .../files). The changefeed and the scan plan leave it
     * null: they answer "what changed" and "what to read", neither of
     * which is a question about one column's span, and filling it there
     * would cost a stats join on every consumer poll.
     */
    val orderingBounds: FileOrderingBounds? = null,
)

/**
 * The range a file covers along the key its rows are ORDERED by — the
 * one bound pair worth carrying beside a file, because it is the range
 * a reader prunes on and the range that says whether two files overlap.
 *
 * Which key that is follows the table, not the file:
 *
 *  - a table with a sort spec is ordered by its sort key, and
 *    [SortKey] carries the LEADING field's stored bounds. Row ids say
 *    nothing about such a table's layout — a sorted rewrite remaps
 *    them — so they are not offered.
 *  - an unsorted table has exactly one ordering: the row id, which is
 *    the append order. [RowIds] carries it.
 */
sealed class FileOrderingBounds {
    /**
     * The leading sort-spec field's stats row, kept in its STORED form
     * (Iceberg single-value bytes, plus the column identity to decode
     * them under) — decoding belongs to the wire layer, as it does for
     * the per-file stats endpoint, so there is one decode path and not
     * two.
     */
    data class SortKey(val column: FileColumnStats) : FileOrderingBounds()

    /**
     * The file's row-id span. [upper] is null when it CANNOT be known:
     * a compaction output carries explicit row ids, where row_id_start
     * is only min(input row ids) and `row_id_start + record_count - 1`
     * is arithmetic on a meaning the file does not have. The ids live
     * in the file's own `_hog_row_id` column, which is not a catalog
     * column and therefore has no `hog_file_column_stats` row to read
     * a maximum out of — so the honest answer is "unknown", not a
     * computed number that would be wrong by exactly the amount the
     * inputs were non-contiguous.
     */
    data class RowIds(val lower: Long, val upper: Long?) : FileOrderingBounds()
}

/** A registered deletion-vector file (one live DV per data file). */
data class DeleteFile(
    val deleteFileId: Long,
    val dataFileId: Long,
    val path: String,
    val fileFormat: String,
    val deleteCount: Long,
    val fileSizeBytes: Long,
    val beginSnapshot: Long,
)

/** A data file paired with its live deletion vector, for read planning. */
data class ScanFile(
    val dataFile: DataFile,
    val deleteFile: DeleteFile?,
)

data class ColumnStats(
    val fieldId: Long,
    val valueCount: Long,
    val nullCount: Long,
    val nanCount: Long?,
    val sizeBytes: Long?,
    val lowerBound: ByteArray?,
    val upperBound: ByteArray?,
) {
    override fun equals(other: Any?): Boolean =
        other is ColumnStats &&
            fieldId == other.fieldId && valueCount == other.valueCount &&
            nullCount == other.nullCount && nanCount == other.nanCount &&
            sizeBytes == other.sizeBytes &&
            lowerBound.contentEquals(other.lowerBound) &&
            upperBound.contentEquals(other.upperBound)

    override fun hashCode(): Int = fieldId.hashCode()
}

/**
 * One data file's per-column statistics, resolved against the columns
 * visible at the requested snapshot (GET .../files/{fileId}/stats).
 * [columns] carries one entry per stored `hog_file_column_stats` row
 * whose field id resolves to a visible LEAF — nothing is fabricated:
 * variant columns and containers never have rows, and a row whose field
 * id is not visible at the snapshot (a dropped column) is omitted.
 */
data class FileStats(
    val dataFileId: Long,
    val statsState: StatsState,
    val columns: List<FileColumnStats>,
)

/** One stats row joined to its column identity at the resolved snapshot. */
data class FileColumnStats(
    val fieldId: Long,
    /** The leaf's own name (synthetic element/key/value included). */
    val name: String,
    /** Dotted path from the top level, e.g. `addr.zip` or `l.element`. */
    val path: String,
    val type: ColType,
    val typeParams: Map<String, Any?>?,
    val stats: ColumnStats,
)

/** One file offered to the commit endpoint. */
data class FileRegistration(
    val path: String,
    val recordCount: Long,
    val fileSizeBytes: Long,
    val footerSize: Long? = null,
    /** null = deferred stats: the file registers as PENDING for the hydrator. */
    val columnStats: List<ColumnStats>? = null,
    /**
     * Transformed partition values by key_index of the table's live spec.
     * Required (with matching arity) when the table is partitioned;
     * forbidden when it is not.
     */
    val partitionValues: List<String?>? = null,
)

data class TableAppend(
    val namespace: String,
    val table: String,
    val files: List<FileRegistration>,
    /**
     * Optional incarnation guard: when present, the commit fails with
     * CommitConflict if the live table resolved by name does not carry
     * this table_uuid — the atomic answer to the name-rebind race where
     * a table is dropped and recreated between a replicator's read and
     * its commit.
     */
    val expectedTableUuid: UUID? = null,
)

/** One deletion-vector registration: supersedes the file's live DV. */
data class DeleteFileRegistration(
    val dataFileId: Long,
    val path: String,
    val deleteCount: Long,
    val fileSizeBytes: Long,
)

data class TableDeletes(
    val namespace: String,
    val table: String,
    val files: List<DeleteFileRegistration>,
    /** Optional incarnation guard; see [TableAppend.expectedTableUuid]. */
    val expectedTableUuid: UUID? = null,
)

data class CommitRequest(
    val readSnapshot: Long? = null,
    val appends: List<TableAppend> = emptyList(),
    val deletes: List<TableDeletes> = emptyList(),
    val author: String? = null,
    val message: String? = null,
    val idempotencyKey: UUID? = null,
    /** Prepared mutations require the entire target read set to remain unchanged. */
    val requireUnchangedTables: Boolean = false,
)

data class CommitResult(
    val snapshotId: Long,
    val schemaVersion: Long,
)

data class ConsumerOffset(
    val consumerId: String,
    val tableUuid: UUID,
    val committedSnapshot: Long,
    val updatedAt: Instant,
)

data class ViewInfo(
    val viewId: Long,
    val viewUuid: UUID,
    val namespace: String,
    val name: String,
    val dialect: String,
    val sql: String,
)

/** Catalog retention/behavior options (the options API surface). */
data class CatalogOptions(
    /** null = snapshot expiry disabled. */
    val snapshotRetentionSeconds: Long?,
    /** Expiry never passes the min consumer offset when true. */
    val consumerFloor: Boolean,
    val earliestSnapshotId: Long,
)

/** One expiry sweep's outcome. */
data class ExpiryResult(
    val snapshotsExpired: Long,
    val dataFilesQueued: Long,
    val deleteFilesQueued: Long,
    val newEarliestSnapshotId: Long,
    /** Non-null when the consumer floor capped the sweep (page-worthy). */
    val flooredByConsumer: String?,
)

/** One compaction run's outcome (POST /maintenance/compact + the loop). */
data class CompactionResult(
    val groupsCompacted: Long,
    val filesIn: Long,
    val filesOut: Long,
    val bytesIn: Long,
    val bytesOut: Long,
    /**
     * Groups planned but aborted at commit time because an input file
     * was no longer live (or the compactor's staged output claim was
     * reclaimed by a concurrent cleanup drain) — resolved by re-planning
     * on the next run, never by blocking foreground.
     */
    val skippedConflicts: Long,
    /**
     * Groups aborted at commit time because an input's deletion-vector
     * state changed since planning (a DV appeared, or the planned DV was
     * superseded by a grown one). The rewrite applied the PLANNED
     * vectors, so committing would resurrect rows deleted after the
     * plan's read — the group skips and re-plans instead. Never a lost
     * delete.
     */
    val dvSuperseded: Long = 0,
    /**
     * Groups skipped because some live column's type cannot be produced
     * from an input file's parquet type (anything outside identity or
     * the int->long / float->double promotions). Deterministic until the
     * schema or the file set changes; skip-with-reason, not a failure.
     */
    val unconvertibleSchema: Long = 0,
    /**
     * Groups skipped for a fault that is DURABLE and the writer's:
     * a value that cannot exist under the type its own file declares (an
     * empty byte array under a decimal, an unscaled value wider than the
     * destination precision), a row large enough to threaten the heap,
     * or a file whose schema contradicts its own registration (the
     * reserved row-id field id present or absent against
     * `explicit_row_ids`).
     *
     * Skip-with-reason, like [unconvertibleSchema] — but that one clears
     * when the schema or the file set moves, and this one never does, so
     * the group is re-planned and re-refused every sweep. Durability and
     * fault are the axis, not values-versus-schema: nonzero means a
     * WRITER produced something its own registration or schema forbids.
     */
    val invalidData: Long = 0,
    /**
     * Groups the SORTED path declined because materializing them would
     * not fit CompactionConfig.sortedHeapBytes — groupable by bytes,
     * too many rows for the heap.
     *
     * Almost always refused in METADATA, at planning, from
     * hog_data_file.record_count: exact, free, and before any IO. The
     * remainder is an OutOfMemoryError actually caught mid-rewrite,
     * which means the per-node heap estimate is wrong for that table's
     * shape and ends the sweep.
     *
     * Durable like [invalidData] — the same table re-plans and
     * re-refuses every sweep — but the fault is neither the writer's nor
     * the schema's: it is a table whose sort order plus row width
     * exceeds the heap this process was given. It clears by raising
     * HOGLAKE_COMPACTION_SORTED_HEAP_BYTES (with a POD sized for it) or
     * by dropping the sort order, which puts the table on the streaming
     * path where group size costs no heap at all.
     *
     * TEMPORARY, and a nonzero count should be read that way. The
     * ceiling exists only because the sorted rewrite sorts a whole group
     * in memory; an external merge sort removes it, and compaction's own
     * outputs are already sorted runs, so a group of them barely needs
     * one. See CompactionConfig.sortedHeapBytes for the full argument.
     */
    val heapBudgetExceeded: Long = 0,
    /**
     * Groups that FAILED outright (unreadable input, S3 error, corrupt
     * DV): the group is retried next run, and unlike the skip flavors
     * this is not self-healing signal — a nonzero count here with a
     * healthy-looking sweep was the "compactor silently chokes on S3"
     * failure mode; count it so the run ledger shows it.
     */
    val failedGroups: Long = 0,
)

/**
 * One invariant check inside a verify run (POST /maintenance/verify).
 * [violations] is the TRUE count; [samples] is capped detail
 * (VerifyService.MAX_SAMPLES) so a badly broken catalog cannot produce
 * an unbounded response.
 */
data class VerifyCheck(
    val check: String,
    val status: String,
    val violations: Long,
    val samples: List<String>,
)

/** One verify run's report: per-check status + overall rollup. */
data class VerifyReport(
    val catalog: String,
    /** "pass" iff every check passed. */
    val status: String,
    val checks: List<VerifyCheck>,
)

/**
 * One rehydrate request's outcome (POST /maintenance/rehydrate):
 * how many 'failed' files were flipped back to 'pending' for the
 * hydrator to retry.
 */
data class RehydrateResult(
    val requeued: Long,
)

/**
 * One hydrator sweep's outcome for ONE catalog (the ledger row's result
 * payload): files the sweep claimed, and how the claims resolved.
 * Transient failures stay 'pending' (retried next sweep); structural
 * ones are marked 'failed' (the rehydrate endpoint's population).
 */
data class HydratorSweepResult(
    val claimed: Long,
    val hydrated: Long,
    val failed: Long,
    val transient: Long,
)

/** One cleanup drain's outcome. */
data class CleanupResult(
    val removed: Long,
    val missing: Long,
    /** Entries skipped because the path is still referenced — an
     *  invariant violation worth alerting on, never a deletion. */
    val stillReferenced: Long,
)

// ---- the maintenance run ledger (hog_maintenance_run) ---------------------

/** The maintenance-task vocabulary (hog_maintenance_run.task's CHECK). */
enum class MaintenanceTask(
    /** False for the manual-only task: no background loop ever drives it. */
    val hasLoop: Boolean,
    /**
     * True when EVERY loop sweep records a run row, which is what makes
     * the gaps between recorded runs the loop's cadence. The hydrator's
     * sweep is instance-wide and records only for the catalogs it
     * claimed files for (V2__maintenance.sql), so its gaps measure when
     * work arrived, not how often the loop ran.
     */
    val loopRecordsEverySweep: Boolean,
) {
    HYDRATOR(hasLoop = true, loopRecordsEverySweep = false),
    EXPIRY(hasLoop = true, loopRecordsEverySweep = true),
    CLEANUP(hasLoop = true, loopRecordsEverySweep = true),
    COMPACTION(hasLoop = true, loopRecordsEverySweep = true),
    VERIFY(hasLoop = false, loopRecordsEverySweep = false),
    ;

    val wire: String get() = name.lowercase()

    companion object {
        /** Null-tolerant parse (routes turn an unknown name into a 422). */
        fun fromWire(s: String): MaintenanceTask? = entries.firstOrNull { it.wire == s }
    }
}

/** Who drove a recorded run (hog_maintenance_run.run_trigger's CHECK). */
enum class MaintenanceTrigger {
    LOOP,
    MANUAL,
    ;

    val wire: String get() = name.lowercase()

    companion object {
        fun fromWire(s: String) = valueOf(s.uppercase())
    }
}

enum class MaintenanceRunStatus {
    OK,
    FAILED,
    ;

    val wire: String get() = name.lowercase()

    companion object {
        fun fromWire(s: String) = valueOf(s.uppercase())
    }
}

/**
 * One recorded maintenance run. [resultJson] is the task's result
 * payload as raw JSON — serialized with the wire's snake_case shape (the
 * matching POST maintenance-trigger response body) so the ledger reads
 * exactly like the API; null for failed runs.
 */
data class MaintenanceRun(
    val runId: Long,
    /** The catalog the run acted on (the ledger is per-catalog). */
    val catalog: String,
    val task: MaintenanceTask,
    val trigger: MaintenanceTrigger,
    val startedAt: Instant,
    val finishedAt: Instant,
    val status: MaintenanceRunStatus,
    val error: String?,
    val resultJson: String?,
)

data class MaintenanceRunPage(
    val runs: List<MaintenanceRun>,
    val hasMore: Boolean,
)

/**
 * Task-specific live backlog, computed from the catalog at read time —
 * never derived from the ledger (a run from before boot is history, not
 * state).
 */
sealed interface MaintenanceBacklog {
    /** stats_state counts: files awaiting hydration, files that failed loudly. */
    data class HydratorBacklog(
        val pendingFiles: Long?,
        val failedFiles: Long?,
    ) : MaintenanceBacklog

    data class ExpiryBacklog(
        /** null = snapshot expiry disabled. */
        val snapshotRetentionSeconds: Long?,
        val consumerFloor: Boolean,
        val earliestSnapshotId: Long,
        val headSnapshotId: Long,
    ) : MaintenanceBacklog

    data class CleanupBacklog(
        /** Undrained hog_file_removal entries. */
        val queuedRemovals: Long?,
        /** Age of the oldest undrained entry; null on an empty queue. */
        val oldestQueuedAgeSeconds: Double?,
    ) : MaintenanceBacklog

    data class CompactionBacklog(
        /** Live files under the compaction target = the debt a sweep plans against. */
        val smallFiles: Long?,
        val targetBytes: Long,
    ) : MaintenanceBacklog

    data object VerifyBacklog : MaintenanceBacklog
}

/**
 * What the run ledger says about a task's background loop. Read from
 * hog_maintenance_run, so it describes the FLEET: the loop may live in a
 * different deployment from the one answering the request (gigahog runs
 * compaction only on gigahog-maintenance), and that process's local
 * config cannot answer "is this task running".
 */
data class LoopObservation(
    /**
     * The gap the loop is currently keeping, or null when the ledger
     * cannot say: fewer than two recorded loop runs, a task whose sweeps
     * are not all recorded ([MaintenanceTask.loopRecordsEverySweep]), or
     * a loop that has since stopped — a cadence is a claim about now, so
     * a dead loop must not keep advertising the rhythm it used to keep.
     */
    val intervalMs: Long?,
    /** Most recent loop run; null = none inside the ledger's retention. */
    val lastRunAt: Instant?,
    /**
     * [MaintenanceTask.loopRecordsEverySweep], carried so a reader knows
     * how to read SILENCE here. Where it is true, no runs means no loop
     * is running the task; where it is false, it only means no work
     * arrived for this catalog, and the loop may be perfectly healthy.
     */
    val recordsEverySweep: Boolean,
)

data class MaintenanceTaskStatus(
    val task: MaintenanceTask,
    /**
     * The RESPONDING PROCESS's configured cadence; 0 = disabled here,
     * null = the task has no loop at all (verify). Says nothing about
     * any other process, so it must not be read as "the task is
     * disabled" — [loop] is the fleet-wide answer.
     */
    val loopIntervalMs: Long?,
    /** The task's most recent recorded run; null = never recorded. */
    val lastRun: MaintenanceRun?,
    val backlog: MaintenanceBacklog,
    /** Ledger evidence about the loop; null for a task with no loop. */
    val loop: LoopObservation? = null,
)

data class MaintenanceStatus(
    val catalog: String,
    /** All five tasks, always present. */
    val tasks: List<MaintenanceTaskStatus>,
    /** Absent until the first complete async sample; backlogs then remain unknown. */
    val sampledAt: Instant? = null,
    val sampleStartedAt: Instant? = null,
    val sampledSnapshotId: Long? = null,
)

/** Instance-wide rollup (GET /v1/maintenance/status): every catalog, by name. */
data class InstanceMaintenanceStatus(
    val catalogs: List<MaintenanceStatus>,
    val hasMore: Boolean = false,
    val nextAfter: String? = null,
)

/** Changefeed plan for (from, to]: appended files + DVs registered in range. */
data class ChangesPlan(
    val tableUuid: UUID,
    val fromSnapshot: Long,
    val toSnapshot: Long,
    val files: List<DataFile>,
    val deleteFiles: List<DeleteFile>,
)

/** Service-level failures the API layer maps to status codes. */
sealed class HoglakeException(message: String) : RuntimeException(message) {
    class NotFound(what: String) : HoglakeException(what)

    class AlreadyExists(what: String) : HoglakeException(what)

    /** A changefeed window crosses a deletion it cannot represent; never blindly retry or skip it. */
    class ReconciliationRequired(detail: String) : HoglakeException(detail)

    class CommitConflict(detail: String) : HoglakeException(detail)

    class Validation(detail: String) : HoglakeException(detail)

    class OffsetRegression(detail: String) : HoglakeException(detail)

    /** Requested range fell below the catalog's expiry floor -> HTTP 410. */
    class Expired(detail: String) : HoglakeException(detail)

    /**
     * The commit transaction's lock_timeout expired while queuing on the
     * per-catalog advisory commit lock (B2 admission control) -> HTTP
     * 503 `commit_queue_timeout` with Retry-After. Retryable
     * backpressure — the catalog is convoyed, not broken.
     */
    class CommitQueueTimeout(detail: String) : HoglakeException(detail)

    /**
     * A column rename was refused because live data files without
     * parquet field ids exist (`hog_data_file.missing_field_ids`):
     * id-less files bind columns by name, so the rename would silently
     * NULL their history in readers -> HTTP 409.
     */
    class IdlessFilesPresent(detail: String) : HoglakeException(detail)

    /**
     * A namespace drop was refused because live tables or views remain
     * (the emptiness precondition — no CASCADE, matching DROP TABLE's
     * no-CASCADE position) -> HTTP 409.
     */
    class NamespaceNotEmpty(detail: String) : HoglakeException(detail)
}
