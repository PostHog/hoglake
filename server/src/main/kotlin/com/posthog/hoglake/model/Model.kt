package com.posthog.hoglake.model

import com.fasterxml.jackson.annotation.JsonInclude
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

        /**
         * The kinds whose `hog_snapshot_change.object_id` is a TABLE id.
         *
         * `object_id` is one column over three disjoint id spaces
         * (table, namespace, view — the schema comment says so), so any
         * query that reads a change row BY object id must say which
         * space it means or it silently counts a namespace's history
         * against a table that happens to share its id.
         *
         * WRITTEN OUT, not derived. The first version of this was
         * `entries.filter { it.name.startsWith("TABLE_") }` with a test
         * that compared it against the schema's kinds filtered by the
         * SAME prefix rule — so both sides agreed by construction and a
         * hypothetical `TABLE_NAMESPACE_MOVED` (a namespace-scoped kind
         * that happens to start with the word) would have joined this
         * set and passed the test. A membership decision that cannot be
         * wrong cannot be checked either.
         *
         * So: adding a change kind means deciding, HERE, whether its
         * `object_id` is a table id, and `TableSummaryVocabularyTest`
         * reds on any kind in schema.sql that this set has not
         * classified either way — it cannot tell you the right answer,
         * but it can refuse to let the question go unasked.
         */
        val TABLE_SCOPED: Set<ChangeKind> =
            setOf(
                TABLE_CREATED,
                TABLE_DROPPED,
                TABLE_ALTERED,
                TABLE_INSERTED_INTO,
                TABLE_DELETED_FROM,
                TABLE_COMPACTED,
            )

        /**
         * The kinds whose `object_id` is NOT a table id.
         *
         * Its only purpose is to make [TABLE_SCOPED] a decision about
         * every kind rather than about six of them: the two sets must
         * partition the vocabulary exactly, which is what the
         * vocabulary test asserts. A new kind that lands in neither
         * reds, and the author has to say which it is.
         */
        val NON_TABLE_SCOPED: Set<ChangeKind> =
            setOf(
                NAMESPACE_CREATED,
                NAMESPACE_DROPPED,
                VIEW_CREATED,
                VIEW_DROPPED,
            )
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

/**
 * One partition field with the distinct stored values it takes across a
 * table's live files — the building block of a "filter by partition" UI.
 * [values] are the TRANSFORMED strings the writer stored (a day ordinal,
 * a bucket index, an identity value), capped at [PARTITION_VALUES_CAP]
 * and ordered most-frequent first; [truncated] marks a field whose
 * cardinality exceeded the cap, so the UI renders "too many to list"
 * rather than a misleading partial dropdown.
 */
data class PartitionFieldValues(
    val sourceFieldId: Long,
    val transform: Transform,
    val transformParam: Int?,
    val values: List<String?>,
    val truncated: Boolean,
)

data class PartitionValues(
    val specId: Long,
    val fields: List<PartitionFieldValues>,
) {
    companion object {
        const val PARTITION_VALUES_CAP = 500
    }
}

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

/**
 * One row of a namespace's table listing.
 *
 * Deliberately NOT a [TableInfo] with empty columns, which is what the
 * listing used to return: the two answer different questions. A
 * TableInfo is one table read AT a snapshot, columns and specs
 * included; this is the per-table rollup a browser shows in a row, and
 * it carries two numbers a TableInfo has no business holding —
 * [snapshotCount] and [earliestSnapshotId], which are properties of a
 * table's HISTORY rather than of its state at any one snapshot.
 *
 * Every field here is resolved at the catalog's head, in ONE query (see
 * `TableRepo.listLiveSummaries`): the listing is head-only, so the
 * comment, the file aggregates and the history counts all answer for
 * the same snapshot without a per-table round trip.
 */
data class TableSummaryInfo(
    val tableId: Long,
    val tableUuid: UUID,
    val name: String,
    val comment: String?,
    /** Rows in the data files live at head — invariant 7, never hog_table_stats. */
    val recordCount: Long,
    val fileCount: Long,
    val fileSizeBytes: Long,
    /**
     * RETAINED snapshots that carry a change row for this table.
     *
     * Snapshots are catalog-wide, so "this table's snapshots" only
     * means anything through `hog_snapshot_change`: the count is of
     * distinct snapshot ids at or above the catalog's
     * `earliest_snapshot_id` whose change row names this table. Expiry
     * deletes snapshots below the floor, so this number SHRINKS as the
     * catalog ages — it is what a reader can still time-travel to, not
     * how many commits the table has ever taken.
     */
    val snapshotCount: Long,
    /** The smallest such snapshot id; null when the table has no retained change row. */
    val earliestSnapshotId: Long?,
)

data class TableInfo(
    val tableId: Long,
    val tableUuid: UUID,
    val namespace: String,
    val name: String,
    val columns: List<Column>,
    /**
     * The snapshot this whole response was RESOLVED AT: head when the
     * caller named no snapshot, the resolved one otherwise. Always set,
     * on every path including `totals=false`, because it describes the
     * read rather than the totals.
     *
     * WHAT IT IS FOR: a writer can cache a `TableInfo` and send this
     * back as a commit's `read_snapshot`, and the server's existing OCC
     * then validates the cache — a table GET per flush becomes zero
     * table GETs per flush. It is the identity read's other half.
     *
     * NOT [snapshotId], and the distinction is load-bearing rather than
     * cosmetic. That field is set ONLY by createTable/alterTable, and
     * its ABSENCE on a read is a contract PYHOGLAKE depends on:
     * `Table.__init__` stores a non-null `snapshot_id` as a DDL pin and
     * `_travel_for` makes it the default snapshot for every later
     * `files()` / `scan_plan()` (`client.py` `_ddl_snapshot_id`), so a
     * server that started sending it on reads would freeze a long-lived
     * `Table` at the snapshot of its first `info()` — appends land and
     * the client stops seeing them. Silent, in a shipped PyPI release.
     *
     * duckdb-client is NOT a second witness, and the earlier draft of
     * this comment was wrong to name it: it does branch on
     * `has_snapshot_id`, but only in `PostDDLTravel`, which is reached
     * solely from its create-table and alter paths — never from a read.
     * pyhoglake alone carries the argument, and it is enough.
     *
     * So this is its own field, and [snapshotId] keeps meaning exactly
     * what it meant.
     */
    val readSnapshotId: Long,
    /*
     * The three totals, and they are NULLABLE for three reasons (#232).
     *
     * A head read serves them from the MAINTENANCE SAMPLER's published
     * generation (`hog_maintenance_summary_tier`, summed per table), not
     * from the manifest — AGENT.md's rule that a dashboard read never
     * scans the manifest. Null then means "the published generation does
     * not cover this table": the sampler has never published for the
     * catalog, or the table was created after the published generation's
     * snapshot. Never 0, because an unsampled table and an empty one are
     * different facts and only one of them is worth a number.
     *
     * `GET ...?totals=false` asks for the table's identity without them
     * at all: name, UUID, columns, partition and sort specs, and no file
     * read of any kind.
     *
     * A time-travel read (`snapshot` / `at_timestamp`) is the one path
     * that still aggregates the manifest, so its numbers are EXACT at
     * that snapshot and never null. See [totalsSnapshotId].
     */
    val recordCount: Long?,
    val fileCount: Long?,
    val fileSizeBytes: Long?,
    /**
     * The snapshot the totals above are exact AS OF — the snapshot the
     * sampler captured when the published generation's scan began.
     *
     * THE PRIMARY FRESHNESS FIELD, because this repo dates a sample by
     * snapshot rather than by clock: `PartitionListingService`'s KDoc
     * says so outright ("the sample is at the sampler's snapshot, which
     * the response states"), and a snapshot is the only form a caller
     * can reconcile against a time-travel read of the same table.
     *
     * It is THE SAME VALUE `PartitionListing.sampledSnapshotId` carries,
     * from the same `hog_maintenance_summary` row. The `totals` prefix
     * scopes it to these three fields, which is necessary here and not
     * there: a `TableInfo` mixes measures from two sources — these from
     * the sample, everything else resolved at [readSnapshotId] — while a
     * `PartitionListing` is all sample.
     *
     * Null on every path where the numbers are not a sample: a
     * time-travel read (exact at the snapshot the caller named), a
     * create/alter receipt (exact at the snapshot the DDL just made),
     * `totals=false` (no numbers), an uncovered table (nothing to date).
     */
    val totalsSnapshotId: Long? = null,
    /**
     * When [totalsSnapshotId] was captured — the instant the published
     * generation's scan STARTED, not the instant it published.
     *
     * The distinction is load-bearing and `PartitionListing`'s own
     * comment is where it is written down: a generation runs for tens of
     * minutes, so `hog_maintenance_summary.sampled_at` (the publish) is
     * up to that far AFTER the numbers were true. `sample.startedAt` is
     * the instant the scan's snapshot was taken, so it is the instant
     * these totals describe — pairing it with [totalsSnapshotId] makes
     * one claim rather than two.
     *
     * It is THE SAME INSTANT `PartitionListing.sampleStarted` carries
     * (both read `sample.startedAt`), and deliberately NOT its
     * `sampledAt` twin: a `TableInfo` reports the age of its numbers and
     * nothing else, so it omits the publish instant rather than offering
     * a reader two timestamps and letting them pick the wrong one.
     *
     * Null exactly when [totalsSnapshotId] is.
     */
    val totalsAsOf: Instant? = null,
    /** Live partition spec; null = unpartitioned. */
    val partitionSpec: PartitionSpec? = null,
    /** Live sort order; null = unsorted. */
    val sortSpec: SortSpec? = null,
    val comment: String? = null,
    val properties: Map<String, String> = emptyMap(),
    /**
     * The snapshot this DDL commit just created — set ONLY by createTable
     * and alterTable, where the snapshot is allocated in the same
     * transaction. Null on every READ (getTable resolves an arbitrary
     * snapshot; stamping it here would dress a read up as a commit).
     *
     * One producer is not a wire response: the internal createTable behind
     * TableCreationService.publish also returns a snapshotId-bearing
     * TableInfo, but publish consumes only its tableId/columns and never
     * serializes it — so the invariant "snapshotId set ⟺ a create/alter
     * wire receipt" holds today by construction, not by type. Do not take
     * a TableInfo from that path and toDto() it.
     */
    val snapshotId: Long? = null,
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
     * which is a question about one column's span. The changefeed would
     * pay a stats join on every consumer poll; the scan plan can be
     * ASKED for bounds instead, in [columnStats] — opt-in, and only for
     * the columns `stats_fields` names when it is given, so a plan
     * carries the leading sort key's bounds only if the caller asked
     * for that column.
     */
    val orderingBounds: FileOrderingBounds? = null,
    /**
     * The file's stored stats rows, resolved against the columns visible
     * at the read snapshot — populated by the SCAN PLAN alone (GET
     * .../scan with include=column_stats, optionally narrowed to the
     * requested field ids), so a query engine can prune files at planning
     * time, and only for a `provided` file: pending and failed files have
     * no rows, and null here is "no stats", which is different from an
     * empty list. The listing and the changefeed leave it null.
     */
    val columnStats: List<FileColumnStats>? = null,
    /**
     * The file's row-group start offsets ([SplitOffsets]), ascending, one
     * per row group — populated by the SCAN PLAN alone (GET .../scan with
     * include=split_offsets), and only when the catalog has them: null
     * means "unknown, cut evenly". The listing and the changefeed leave
     * it null.
     */
    val splitOffsets: List<Long>? = null,
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
 * visible at the requested snapshot (GET .../files/{fileId}/stats; the
 * scan plan carries the same entries as [DataFile.columnStats]).
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
    /**
     * Optional row-group start offsets from a writer that holds the
     * footer ([SplitOffsets]); validated at commit, stored verbatim.
     * Absent leaves `hog_data_file.split_offsets` NULL — for a pending
     * file the hydrator fills it from the footer; a provided-stats file
     * registered without it simply has none, and readers cut it evenly.
     * NON_NULL so a stored payload (commit receipt, table-creation
     * files) written without it reads back identically on a replica
     * that predates it.
     */
    @get:JsonInclude(JsonInclude.Include.NON_NULL)
    val splitOffsets: List<Long>? = null,
    /** Physical storage format. Omitted legacy payloads decode as parquet. */
    @get:JsonInclude(
        value = JsonInclude.Include.CUSTOM,
        valueFilter = ParquetFileFormatFilter::class,
    )
    val fileFormat: String = FileFormats.PARQUET,
)

data class TableAppend(
    val namespace: String,
    val table: String,
    val files: List<FileRegistration>,
    /**
     * Optional incarnation guard: when present, the commit fails with
     * [HoglakeException.TableRecreated] (409 `table_recreated`) if the
     * live table resolved by name does not carry this table_uuid — the
     * atomic answer to the name-rebind race where a table is dropped and
     * recreated between a replicator's read and its commit.
     *
     * It is also the ONLY thing that closes that race:
     * CommitService.checkConflicts keys on the RESOLVED table id and
     * treats 'table_created' as a conflict only for a guarded request's
     * delete targets, so a drop+recreate is outside an append's conflict
     * window however fresh its read_snapshot is.
     */
    val expectedTableUuid: UUID? = null,
)

/** One deletion-vector registration: supersedes the file's live DV. */
data class DeleteFileRegistration(
    val dataFileId: Long,
    val path: String,
    val deleteCount: Long,
    val fileSizeBytes: Long,
    /** Transaction-only reference to an append in this same commit; dataFileId must be zero. */
    @get:com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL)
    val dataFilePath: String? = null,
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
    /**
     * Prepared mutations require the entire target read set to remain
     * unchanged; the requirement binds the tables this request DELETES
     * from (see CommitService.checkConflicts).
     *
     * NON_DEFAULT, like [allowPendingDeletes]: a commit receipt stores
     * this canonical payload, and a receipt that spells out every
     * defaulted field is a receipt an OLDER replica cannot decode during
     * a rolling deploy. Serializing only what was actually asked for
     * keeps the stored payload as small as the contract the writer used.
     */
    @get:com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_DEFAULT)
    val requireUnchangedTables: Boolean = false,
    @get:com.fasterxml.jackson.annotation.JsonInclude(com.fasterxml.jackson.annotation.JsonInclude.Include.NON_DEFAULT)
    val allowPendingDeletes: Boolean = false,
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
    /**
     * Consumer offsets the sweep DELETED because atomic replacement had
     * superseded them (OffsetRepo.releaseSupersededOffsets).
     *
     * Counted because it is the only work a retention-null catalog's
     * sweep can do, and an uncounted deletion of consumer positions is
     * exactly the kind of thing that should never be silent: without it
     * the run is logged as "nothing to do" and the ledger row says the
     * sweep changed nothing.
     *
     * Defaulted, and `@JsonInclude(NON_DEFAULT)` for the stored-payload
     * rule: a ledger row written before this counter existed replays
     * without it, and a rolling deploy has both versions reading each
     * other's rows.
     */
    @get:JsonInclude(JsonInclude.Include.NON_DEFAULT)
    val offsetsReleased: Long = 0,
    /**
     * `hog_data_file` rows phase B deleted below the floor.
     *
     * EQUAL TO [dataFilesQueued] BY CONSTRUCTION, and both are kept
     * because they answer different questions and the identity between
     * them is the thing worth being able to see. The page's delete and
     * its `hog_file_removal` insert are ONE statement (the
     * `RETURNING`-fed CTE in `ExpiryService.DATA_FILE_EXPIRY_SQL`), so a
     * row cannot be purged without its path being queued for the
     * cleanup drain or the other way round — which is what makes a
     * crash between the two phases, or a page that times out, leave no
     * orphaned object. If these two ever disagree on a ledger row,
     * something has split that statement.
     */
    @get:JsonInclude(JsonInclude.Include.NON_DEFAULT)
    val dataFilesPurged: Long = 0,
    /**
     * Pages phase B ran, across its data-file arm (whose pages also take
     * the vectors riding their files) and its superseded-vector arm.
     * Pages x `HOGLAKE_EXPIRY_PURGE_PAGE` is the row bound the sweep
     * actually spent.
     *
     * AN EMPTY PAGE IS NOT COUNTED, and that is load-bearing rather than
     * tidy: each arm must run one page to discover that nothing is
     * eligible, so counting those made every idle sweep report 2 — which
     * defeated the console's hide-quiet filter for every expiry row on a
     * fleet sweeping every 15 seconds.
     */
    @get:JsonInclude(JsonInclude.Include.NON_DEFAULT)
    val purgePages: Long = 0,
    /**
     * Pages that THREW — a statement bound fired, or the database was
     * unreachable.
     *
     * Separate from the row counters because 0 rows is the same number
     * for an idle sweep and for one whose every page timed out, and the
     * second is the state that matters: the purge would attempt the same
     * first page forever while the floor kept advancing and the console
     * kept reading healthy. Pages already committed are still counted as
     * purged — they are separate transactions, so a failure on page 11
     * does not un-delete pages 1-10. Rides
     * `hoglake_expiry_purge_failures_total`.
     */
    @get:JsonInclude(JsonInclude.Include.NON_DEFAULT)
    val purgeFailures: Long = 0,
    /**
     * True when phase B stopped with work possibly left: the run budget
     * expired, a page failed even after halving its way down to one row,
     * or the catalog's floor could not be read.
     *
     * A SUPERSET OF "the budget fired", deliberately. Every one of those
     * stops has the same consequence for an operator — rows below the
     * floor survived this sweep — and a flag that only covered the
     * budget would read `false` for the two cases that are worth acting
     * on. The log line names which one it was, and
     * `hoglake_expiry_purge_truncated_total` is the alertable form.
     *
     * A BOOLEAN, so it is NOT in `MaintenanceDto`'s
     * `EXPIRY_COUNTERS_ADDED_LATER` list: that normalizer fills a
     * missing field with the integer 0, which is not a boolean, and a
     * ledger row written before this field existed is better left
     * absent (the console's guard is undefined-safe) than filled with a
     * type its schema does not declare.
     */
    @get:JsonInclude(JsonInclude.Include.NON_DEFAULT)
    val purgeTruncated: Boolean = false,
    /**
     * File rows still eligible when a truncated phase B stopped — BOTH
     * tables, summed — each SATURATING at
     * `ExpiryService.PURGE_REMAINING_CAP`. Counting only `hog_data_file`
     * made a purge stopped inside the vector arm report
     * `truncated = true, remaining = 0`, which reads as "stopped,
     * nothing left".
     *
     * Counted only when [purgeTruncated] — an uncounted "how far behind
     * am I" is a backlog nobody sees until the table is the problem, and
     * an UNCAPPED count is itself the unbounded read this change exists
     * to remove. A value at the cap means "behind by more than any sweep
     * will catch up", which is the only reading that changes what an
     * operator does.
     *
     * NULL IS UNKNOWN, AND IS NOT 0. The count is best-effort: its
     * vector half has no index yet, so on a catalog with a large
     * `hog_delete_file` it is a scan, and it runs under the purge's own
     * 5 s bound rather than the session's 60 s so that one catalog's
     * unindexed count cannot delay every catalog behind it in the serial
     * fleet sweep. When that bound fires the answer is absent, because
     * "0" beside `purge_truncated = true` is the one thing that cannot
     * be true and is exactly the reading this field exists to prevent.
     * `@JsonInclude(NON_NULL)`: an unknown is an absent field on the
     * wire and in the ledger, and every console guard on it is
     * undefined-safe.
     */
    @get:JsonInclude(JsonInclude.Include.NON_NULL)
    val purgeRemaining: Long? = 0,
    /**
     * Times phase A halved `HOGLAKE_EXPIRY_BATCH` after its statement
     * bound fired, and retried inside the same run.
     *
     * ON THE WIRE AND NOT ONLY IN PROMETHEUS, because for one failure
     * mode the halvings are the ONLY evidence: a purge whose every page
     * times out spends its budget on rungs and reports no rows, and an
     * operator reading `GET /maintenance/runs` would otherwise see a
     * truncated sweep with no explanation of where the time went. The
     * counters say "a knob is too large for this catalog's work" and the
     * ledger is where that is read per run.
     */
    @get:JsonInclude(JsonInclude.Include.NON_DEFAULT)
    val advanceHalvings: Long = 0,
    /**
     * Times a phase-B page halved after failing, across both arms. See
     * [advanceHalvings] for why this is on the wire, and
     * `ExpiryService.settledPage` for why the reduced size survives the
     * sweep — a standing nonzero here with a falling
     * `data_files_purged` is a ladder that is not converging.
     */
    @get:JsonInclude(JsonInclude.Include.NON_DEFAULT)
    val purgeHalvings: Long = 0,
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
    /**
     * Groups this sweep planned and then dropped because ANOTHER
     * maintenance replica holds a live claim over one or more of their
     * input files (`hog_compaction_claim`, V15).
     *
     * Not a conflict, not a failure, and not work lost: the sibling
     * replica is rewriting those files right now, and everything this
     * sweep would have spent on them would have been discarded at its
     * own commit — which is exactly the production shape the claims were
     * built for (a 547 s sweep on gigahog-prod-us committed 34 groups
     * and lost 30 to the other replica's commits, each one a full
     * rewrite and upload thrown away).
     *
     * Read it as the feature working. It spends no object-store IO, so
     * it does NOT consume HOGLAKE_COMPACTION_MAX_GROUPS_PER_RUN. A
     * standing ZERO on a fleet with two or more maintenance replicas
     * means either HOGLAKE_COMPACTION_CLAIMS_ENABLED is off or the
     * replicas are not in fact planning the same groups.
     *
     * Defaulted, and `@JsonInclude(NON_DEFAULT)` for the stored-payload
     * rule: the maintenance ledger holds rows an older replica wrote and
     * a rolling deploy has both versions replaying each other's, so a
     * counter that postdates a row must be absent from it rather than
     * asserted as zero.
     */
    @get:JsonInclude(JsonInclude.Include.NON_DEFAULT)
    val claimedElsewhere: Long = 0,
    /**
     * Candidate file rows the sweep's planners READ, summed over every
     * table planned.
     *
     * Not a group outcome — a plan measure, and the one the planner's
     * own defect had no series for. Compaction planning used to select
     * every live file of a table under the target into a Kotlin list:
     * ~9.9M rows per table per sweep on gigahog-prod-us's
     * `ingest.events_raw`, with the packing that followed running long
     * enough inside the planning transaction that Postgres killed the
     * connection on `idle_in_transaction_session_timeout`. The fetch is
     * bounded now, per table plan, by
     * CompactionConfig.candidateBudget — and the number of STATEMENTS
     * one plan may issue is bounded as well, which rows alone would not
     * have covered. This is the number that says so.
     */
    @get:JsonInclude(JsonInclude.Include.NON_DEFAULT)
    val candidatesFetched: Long = 0,
    /**
     * Partition buckets the sweep's planners fetched candidates for,
     * of the [bucketsAvailable] their samples offered.
     *
     * Groups never span a (spec_id, partition_values) bucket, so the
     * planner picks BUCKETS before files, from the maintenance
     * sampler's published generation, best compaction value first.
     *
     * SUMMED ACROSS THE SWEEP'S TABLES, so the pair is a ratio and not
     * an equality: `considered` well below `available` means the
     * candidate budget is the binding constraint and the remaining
     * buckets wait for a later sweep, which is correct and is why the
     * planner rotates the bucket it starts at
     * (`CompactionService.bucketCursor`) rather than re-visiting the
     * top of a fixed order. Equality would only mean "every table saw
     * every bucket", which is a per-table statement this sum cannot
     * make.
     */
    @get:JsonInclude(JsonInclude.Include.NON_DEFAULT)
    val bucketsConsidered: Long = 0,
    /**
     * Partition buckets the published sample credits the sweep's tables
     * with enough ACTIONABLE debt to form a group — files the packer
     * would take (the sampler's `selected` accumulator), not files that
     * merely exist.
     *
     * Zero for a catalog whose sampler has published nothing yet, which
     * takes the bounded whole-table fallback. A published generation
     * that credits a table with no qualifying bucket means there is no
     * work, and the planner reads nothing at all — a different state
     * from the fallback, and one the first version of this change
     * conflated with it.
     */
    @get:JsonInclude(JsonInclude.Include.NON_DEFAULT)
    val bucketsAvailable: Long = 0,
    /**
     * Table plans whose candidate fetch hit its own cap, so the table
     * has compactable debt the plan could not see.
     *
     * Never an error: the next sweep sees the next-smallest files,
     * because the fetch is ordered by size and compaction consumes
     * candidates in that order. A STANDING nonzero on a table that
     * never drains is the signal to raise
     * HOGLAKE_COMPACTION_MAX_GROUPS_PER_RUN (so the run can use more of
     * what it reads) rather than the caps themselves.
     */
    @get:JsonInclude(JsonInclude.Include.NON_DEFAULT)
    val candidatesTruncated: Long = 0,
    /**
     * Wall-clock milliseconds the sweep spent PLANNING, summed over
     * every table it planned.
     *
     * Here because the planner's failure was a time failure: the in-JVM
     * packing grew past the session's 30 s idle-in-transaction bound and
     * nothing in the ledger said planning had become slow until every
     * sweep started dying on the statement after it.
     */
    @get:JsonInclude(JsonInclude.Include.NON_DEFAULT)
    val planMs: Long = 0,
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
    /**
     * Queue ROWS settled `'deleted'`. Not a count of objects: two
     * undrained rows over one path are legitimate state (nothing makes a
     * file path unique — see V16), and one batched delete settles both.
     * [objectsRemoved] is the physical count.
     */
    val removed: Long,
    /** Queue rows settled `'absent'` — staging tickets only, since every
     *  other reason drains through a batched delete that cannot report a
     *  key that was not there. */
    val missing: Long,
    /** Entries skipped because the path is still referenced — an
     *  invariant violation worth alerting on, never a deletion. */
    val stillReferenced: Long,
    /**
     * DISTINCT paths whose object this run physically deleted — what
     * `hoglake_files_removed_total` counts, and one `file_deleted` audit
     * event each. Always <= [removed].
     *
     * Defaulted, and `@JsonInclude(NON_DEFAULT)` for the stored-payload
     * rule: the maintenance ledger holds rows an older replica wrote and
     * a rolling deploy has both versions replaying each other's, so a
     * counter that postdates a row must be absent from it rather than
     * asserted as zero.
     */
    @get:JsonInclude(JsonInclude.Include.NON_DEFAULT)
    val objectsRemoved: Long = 0,
    /**
     * Rows a sub-batch could not settle because the row was no longer its
     * own: another writer settled it (a compaction group's commit
     * settling its own staging ticket `'registered'`, which is that group
     * committing normally), or this worker's claim lease lapsed and
     * another worker took the row. Not a failure, and deliberately NOT a
     * `still_referenced` violation, which is what it used to be counted
     * as.
     *
     * THE ONE ALARMING MISS IS NOT COUNTED HERE. A row settled
     * `'registered'` over a path this sub-batch had already DELETED is a
     * live file row pointing at a deleted object; the drain reads the
     * missed rows back, and that case is counted in [stillReferenced]
     * with an ERROR and a `cleanup_violation` audit event. It is
     * unreachable while compaction refuses any ticket cleanup has
     * touched; it is counted because nothing else in the system could see
     * it.
     *
     * Defaulted and NON_DEFAULT for the same reason as [objectsRemoved].
     */
    @get:JsonInclude(JsonInclude.Include.NON_DEFAULT)
    val settledElsewhere: Long = 0,
    /**
     * ALWAYS 0, and retained only because the ledger remembers it.
     *
     * It counted rows a sub-batch's hold budget stopped short of, and
     * that budget existed to bound a per-catalog commit-lock hold the
     * drain no longer takes (it claims rows instead and holds no lock at
     * all). The counter stays on the wire because the maintenance run
     * ledger holds rows that carry it and every client already reads it;
     * removing it from the schema would break a read of those rows.
     *
     * Defaulted and NON_DEFAULT for the same reason as [objectsRemoved].
     */
    @get:JsonInclude(JsonInclude.Include.NON_DEFAULT)
    val deadlineSkipped: Long = 0,
    /**
     * Commit receipts this run purged for the catalog — rows past
     * `HOGLAKE_RECEIPT_RETENTION_SECONDS` (#240, V24).
     *
     * ZERO IS NOT "NOTHING TO DO" ON ITS OWN, which is what
     * [receiptsPurgeFailures] beside it is for: the purge is fenced so it
     * can never fail the drain, so a failed page also reports 0 purged.
     * A run whose budget expired before its first page reports 0 too, and
     * THAT one is distinguished only by the app log — deliberately, since
     * it is the benign case and a third counter on a hygiene task earns
     * nothing.
     *
     * Defaulted and NON_DEFAULT for [objectsRemoved]'s reason — the
     * ledger holds rows written before this counter existed, and a
     * counter that postdates a row must be absent from it rather than
     * asserted as zero.
     */
    @get:JsonInclude(JsonInclude.Include.NON_DEFAULT)
    val receiptsPurged: Long = 0,
    /**
     * Pages of the commit-receipt purge that THREW — a statement bound
     * fired, the pool failed, anything.
     *
     * IT EXISTS BECAUSE [receiptsPurged] ALONE IS AMBIGUOUS. The purge is
     * fenced so it can never fail the drain it rides, so a failing page
     * and an idle one both report 0 purged, and the failing one is the
     * state that matters: for the first retention window after V24 the
     * only eligible rows are the legacy ones, each carrying ~82 TOAST
     * chunk rows that `heap_delete` removes synchronously, and a page
     * that cannot finish inside its bound would otherwise retry forever
     * while every surface read exactly like a healthy instance and the
     * 58 GiB never went.
     *
     * Standing nonzero = the page size or the bound needs looking at,
     * not that anything is corrupt; the rows are still there and the
     * next run starts again from the oldest eligible one.
     * `hoglake_commit_receipt_purge_failures_total` is the same number.
     *
     * Defaulted and NON_DEFAULT for [objectsRemoved]'s reason.
     */
    @get:JsonInclude(JsonInclude.Include.NON_DEFAULT)
    val receiptsPurgeFailures: Long = 0,
)

/**
 * One retirement run's outcome for ONE catalog: what the paced deletion
 * of dropped tables' file rows got through this time.
 *
 * A run is bounded three ways — the queue ceiling, the wall-clock run
 * budget and the interval — so a partial result is the NORMAL result on
 * a large table. Nothing here is a failure; the next run continues from
 * where this one stopped, because the victim select is "whatever is
 * still live on this dropped table" and carries no cursor.
 */
data class RetirementResult(
    /**
     * Eligible dropped tables this run committed at least one batch
     * against.
     *
     * A table the run budget interrupted is counted HERE and in
     * [tablesRemaining] both, and that is not double counting: this one
     * says work was done on it, that one says work is left on it, and
     * on an interrupted table both are true. `tables +
     * tables_remaining` is therefore not the candidate count and must
     * not be read as one.
     */
    val tables: Long,
    /** hog_data_file rows deleted. */
    val rowsRetired: Long,
    /** hog_delete_file rows deleted — DVs, superseded ones included. */
    val dvsRetired: Long,
    /**
     * Paths queued into hog_file_removal with reason `table_drop_gc`:
     * [rowsRetired] + [dvsRetired], since every deleted row queues its
     * path exactly once. Carried separately because it is what the
     * cleanup drain has to absorb, and the two must agree — a gap
     * between them is an object that lost its only reference.
     */
    val pathsQueued: Long,
    /** Batch transactions that committed. */
    val batches: Long,
    /**
     * Batches cancelled by their own `statement_timeout` and rolled
     * back whole. The batch size is NOT changed in response (#263): the
     * per-row cost is flat in the batch, so a cancelled batch was cold
     * rather than big, and the table is simply left for the next run at
     * the same size, its cancelled statement having warmed the pages it
     * touched.
     *
     * So an occasional nonzero here is a cache miss, not a
     * misconfiguration. The shape that needs a human is the SAME TABLE
     * timing out run after run, which this per-run total cannot show
     * and `hoglake_retirement_consecutive_timeouts{catalog,table}` is
     * the series for. The remedy there is a smaller
     * HOGLAKE_RETIREMENT_BATCH for that catalog.
     */
    val timeouts: Long,
    /**
     * Tables whose batch selected rows and deleted none, which ends the
     * run for that table. Structurally impossible on a healthy catalog
     * — the select and the delete name the same primary keys — so a
     * nonzero here is a concurrent writer or a broken cascade, and the
     * point of the counter is that the loop stops rather than spins.
     */
    val skippedTables: Long,
    /**
     * 1 when this run declined to start because the catalog's undrained
     * cleanup queue was already over HOGLAKE_RETIREMENT_QUEUE_CEILING,
     * 0 otherwise. Retirement's output IS cleanup's input, so a run
     * that ignored a backed-up drain would trade a bounded metadata
     * problem for an unbounded queue.
     */
    val skippedQueueFull: Long,
    /**
     * 1 when another maintainer already held this catalog's retirement
     * lock and this run stepped aside, 0 otherwise.
     *
     * Not a failure and not contention to fix: single flight per
     * catalog is the design (queueing W maintainers behind each other's
     * commit-lock holds taxes every foreground commit by
     * `(W - 0.5) x hold` and buys nothing, the work being idempotent).
     * It is counted because a run row that says "0 rows retired" with
     * no reason attached is indistinguishable from a broken loop.
     */
    val skippedLocked: Long,
    /**
     * Runs that gave up because the per-catalog COMMIT LOCK was not
     * available inside `HOGLAKE_COMMIT_LOCK_TIMEOUT_MS` (0 or 1 — a
     * convoy ends the run).
     *
     * Separate from [timeouts] because the two ask for opposite
     * remedies. A timeout says the BATCH is too big for the table and
     * the answer is a smaller batch; a convoy says somebody else holds
     * the catalog's lock and the answer is to look at what. Summed into
     * one counter they cancel each other out as a signal.
     */
    val convoyed: Long,
    /**
     * Eligible tables this run did not FINISH — the one it was
     * interrupted part way through, if any, plus the ones after it that
     * the run budget never reached. The honest reading of a run that
     * "did nothing": the loop is pacing itself, not idle.
     *
     * "Did not finish" rather than "did not reach", because an
     * interrupted table is both reached and remaining; see [tables].
     * There is no cursor to carry, so the next run simply re-selects
     * what is still live and continues.
     */
    val tablesRemaining: Long,
)

// ---- the maintenance run ledger (hog_maintenance_run) ---------------------

/** The maintenance-task vocabulary (hog_maintenance_run.task's CHECK). */
enum class MaintenanceTask(
    /**
     * False for a task no background loop ever drives: today only
     * [VERIFY], whose subsystem is gone. It is what keeps
     * MaintenanceRunStore.recentLoopRunsAll from asking for
     * `run_trigger = 'loop'` rows that cannot exist.
     */
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

    /**
     * REMOVED SUBSYSTEM, RETAINED VOCABULARY. #261 deleted
     * `VerifyService`, its loop, its route and its console panel;
     * nothing writes a `verify` row any more and
     * `MaintenanceStatusService` does not list the task.
     *
     * The value stays because `hog_maintenance_run.task`'s CHECK
     * (V2__maintenance.sql, in the frozen append-only chain) still
     * admits `'verify'` and the ledger still HOLDS those rows for
     * `HOGLAKE_MAINTENANCE_LEDGER_RETENTION_SECONDS` (7 days by
     * default, longer wherever it is tuned up).
     * `MaintenanceRunStore.runMapper` does `fromWire(...) ?: error(...)`,
     * so deleting the value would not degrade one row — it would make
     * every unfiltered `GET /maintenance/runs` throw for as long as one
     * historical verify row survives, which is the console's main
     * screen. It is removable once the retention window has passed
     * everywhere.
     *
     * `hasLoop = false` is now the truth and is load-bearing: it keeps
     * `recentLoopRunsAll` from asking for `run_trigger = 'loop'` rows
     * that nothing can write.
     */
    VERIFY(hasLoop = false, loopRecordsEverySweep = false),

    // Every retirement sweep records a row too — including the ones that
    // retire nothing because no drop has sunk under the floor yet, and
    // the ones the queue ceiling or the run budget cut short. That is
    // the point: a catalog whose dropped tables are not shrinking needs
    // to be distinguishable from a catalog nobody is sweeping, and only
    // a row per sweep can do that.
    RETIREMENT(hasLoop = true, loopRecordsEverySweep = true),
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

    /**
     * No backlog number, deliberately: an empty object on the wire.
     *
     * The honest backlog here is "live file rows on dropped tables",
     * and that is a count over the MANIFEST — the one thing the
     * dashboard path may never do (MaintenanceStatusService's KDoc).
     * A count of dropped TABLES would be cheap but would answer a
     * different question: a catalog with one dropped 3M-row table and
     * one with forty dropped empty ones read identically. So this
     * carries nothing, and what an operator reads instead is the run
     * ledger's own counters (`rows_retired`, `tables_remaining`),
     * which are already per-run facts rather than a per-request scan.
     * The verify subsystem's orphans check was the second source until
     * #261 removed it.
     */
    data object RetirementBacklog : MaintenanceBacklog
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
     * null = the task has no loop at all. Every task has one today, so
     * null now only means an older server. Says nothing about any other
     * process, so it must not be read as "the task is disabled" —
     * [loop] is the fleet-wide answer.
     */
    val loopIntervalMs: Long?,
    /** The task's most recent recorded run; null = never recorded. */
    val lastRun: MaintenanceRun?,
    val backlog: MaintenanceBacklog,
    /** Ledger evidence about the loop; null for a task with no loop (none today). */
    val loop: LoopObservation? = null,
)

data class MaintenanceStatus(
    val catalog: String,
    /** All six tasks, always present. */
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

    /**
     * OPEN, for [DdlSinceReadSnapshot] alone. Subclassing is what keeps
     * the re-typing backwards compatible: every client that discriminates
     * with an `isinstance` ladder (millpond's `is_retryable` does, and it
     * is wired into its retry budget) keeps classifying the DDL refusal as
     * it does today, while the WIRE code changes so a client that wants
     * the finer answer can have it. The same trade as
     * `ReadSnapshotExpiredError(ExpiredError)` on the pyhoglake side.
     *
     * Every `when` over this hierarchy must therefore put the SUBCLASS
     * arm first — `ErrorMapping`, `Metrics.commitFailureResult` and
     * `Audit.failureOutcome` all do, and each says so.
     *
     * THE RULE for a future refusal, because the asymmetry here is
     * principled and not arbitrary: **what decides whether a new refusal
     * subclasses this is the CLIENT-side mapping, not the server's.**
     * Nothing in `server/src/main` catches `CommitConflict` at all, so
     * the server cannot tell. [DdlSinceReadSnapshot] subclasses because
     * pyhoglake maps it into `CommitConflictError`, which sits in
     * millpond's `isinstance` ladder ahead of its "any other 4xx"
     * re-raise — un-subclassing would have bypassed a working recovery
     * arm. [TableRecreated] does NOT subclass because pyhoglake maps it
     * to `IncarnationChangedError`, which was never a
     * `CommitConflictError` and already has its own ladder arm. Split a
     * refusal out only after checking which client arm it lands in.
     */
    open class CommitConflict(detail: String) : HoglakeException(detail)

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

    /**
     * A commit named a table that EXISTS but has been dropped -> HTTP
     * 409 `table_dropped`, carrying the snapshot it was dropped in.
     *
     * Typed because the two cases a writer has to tell apart used to
     * arrive as the same 422 `unknown table`: a typo or a stale
     * namespace (fix the request) and a table someone dropped out from
     * under a running writer (stop, or recreate — and the drop snapshot
     * says when it happened, which the writer can line up against its
     * own last successful commit). 409 rather than 404 for the same
     * reason [CommitConflict] is: the request was well-formed against
     * the catalog it was planned against, and the catalog moved.
     *
     * Raised only after the live resolution MISSES, so it costs nothing
     * on the commit path (one extra statement on a path that is already
     * failing).
     */
    class TableDropped(detail: String) : HoglakeException(detail)

    /**
     * A request's `expected_table_uuid` is not the live table's ->
     * HTTP 409 `table_recreated`: the name resolved, but to a different
     * INCARNATION, so the table was dropped and recreated since the
     * caller read it.
     *
     * The same family as [DdlSinceReadSnapshot] and for the same reason:
     * the expected incarnation is gone and its history does not carry
     * over, so replaying a payload that names it can only be refused
     * again. It was a [CommitConflict] — which the clients that matter
     * map to a RETRYABLE error — and pyhoglake could only tell the two
     * apart by looking for the phrase "the table was recreated" in the
     * detail string. A code is the contract; prose is not.
     */
    class TableRecreated(
        detail: String,
        /** Qualified `namespace.table`, so a client knows what to re-read. */
        val table: String,
        val expectedTableUuid: UUID,
        val currentTableUuid: UUID,
    ) : HoglakeException(detail)

    /**
     * DDL landed on a touched table after the commit's `read_snapshot`
     * -> HTTP 409 `ddl_since_read_snapshot`, naming the tables and the
     * snapshot.
     *
     * This is the DDL arm of [CommitService.checkConflicts], split out
     * of [CommitConflict] — as a SUBCLASS of it, see there — because the
     * two answers need opposite client behaviour and used to arrive as
     * the same code. A CommitConflict is
     * REPLAYABLE: the read set moved, so refresh the read snapshot and
     * send the same files. This one is not, and cannot be — a prepared
     * request's `read_snapshot` is part of a durable payload that must
     * be replayed byte-identically, so the condition that refused it is
     * permanent and a retry loop on it is a livelock. The recovery is to
     * re-prepare: read the table again and build a new request.
     *
     * THAT is what makes a prepared append a check-and-set on the
     * table's shape. A partition-spec change between the writer's read
     * and its commit arrives here, atomically and with zero writes —
     * which is why #233 needs no new wire field: `read_snapshot` was
     * always the token, it was only ever answered with the wrong code.
     *
     * Named for the wire code rather than for appends, because a DELETE
     * commit takes the same refusal for the same reason: its
     * `read_snapshot` is fixed too.
     *
     * Not all of [CommitService.checkConflicts]' refusals are this:
     * ROW-CONTENT conflicts ('table_created' / 'table_inserted_into' /
     * 'table_deleted_from' on a guarded request's delete targets) stay
     * [CommitConflict], because re-reading and replanning IS the
     * recovery there and the same payload can succeed on the next try.
     * A refusal that mixes both is the conservative answer, i.e. also
     * [CommitConflict].
     */
    class DdlSinceReadSnapshot(
        detail: String,
        /** The qualified `namespace.table` names the DDL landed on. */
        val tables: List<String>,
        val readSnapshot: Long,
    ) : CommitConflict(detail)
}
