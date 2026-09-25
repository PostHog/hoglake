package com.posthog.hoglake.hydrator

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.StatsSanity
import com.posthog.hoglake.model.maxUnsignedParquetWidth
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.stats.IcebergSingleValue
import io.github.oshai.kotlinlogging.KotlinLogging
import org.apache.parquet.column.statistics.Statistics
import org.apache.parquet.hadoop.metadata.BlockMetaData
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import java.math.BigInteger
import java.nio.ByteBuffer
import java.nio.ByteOrder

/**
 * A live catalog column (hog_column, end_snapshot IS NULL) as the
 * hydrator sees it. Recursive since phase 2: a container carries its
 * [children], and only the LEAVES (the scalar descendants) ever produce
 * a stats row — `hog_file_column_stats` is field-id-keyed, and a
 * container has no values to count or bound.
 */
data class CatalogColumn(
    val fieldId: Long,
    val name: String,
    val type: ColType,
    /** From type_params for decimal columns; null when absent. */
    val decimalScale: Int?,
    val children: List<CatalogColumn> = emptyList(),
)

/**
 * Pure footer-to-stats aggregation: takes a parquet footer
 * ([ParquetMetadata], parquet-java — the project's one parquet library)
 * and the table's live catalog columns, and produces per-field
 * aggregates in Iceberg single-value bound encoding. Footer only — no
 * data pages.
 *
 * Column mapping prefers the parquet schema's field ids
 * (`PARQUET:field_id`) when the file carries any; otherwise it falls
 * back to name matching (logged as a warning — files written without
 * field ids lose rename-safety, and [missingFieldIds] flags them for
 * the rename guard).
 *
 * Bounds are only produced when every column chunk contributes reliable
 * statistics (present with a decodable min/max under the catalog type,
 * not NaN; parquet-java itself refuses unreliable pre-TYPE_DEFINED_ORDER
 * deprecated min/max at footer decode). Anything else leaves the bounds
 * NULL — never guessed.
 */
object FooterStats {
    private val log = KotlinLogging.logger {}

    /** 2^64, for reading an unsigned int64 out of its signed bit pattern. */
    private val TWO_POW_64: BigInteger = BigInteger.ONE.shiftLeft(64)

    data class ColumnAgg(
        val fieldId: Long,
        val valueCount: Long,
        val nullCount: Long,
        val nanCount: Long?,
        val sizeBytes: Long?,
        val lowerBound: ByteArray?,
        val upperBound: ByteArray?,
    )

    /**
     * A primitive leaf of the parquet schema, at any depth. [path] is
     * the full chunk path (`ColumnChunkMetaData.path.toArray()`), which
     * for a top-level column is just its name and for a list element is
     * `[name, "list", "element"]`.
     */
    private data class Leaf(
        val path: List<String>,
        val fieldId: Int?,
        val primitive: PrimitiveType,
    )

    /**
     * The field-id contract check: true when ANY node of the schema that
     * binds to a catalog column lacks a `PARQUET:field_id` — every
     * primitive leaf AND every CONTAINER wrapper group. Such files bind
     * that node by name, so a later column rename would silently NULL its
     * history in readers — hog_data_file.missing_field_ids records the
     * hazard and AlterService refuses renames while a flagged file is
     * live. The reserved `_hog_row_id` id 2147483646 on compacted files
     * is an id like any other and never trips this.
     *
     * **Containers count, and the omission was a data-loss bug.** A
     * foreign writer that puts ids on every leaf but none on the struct
     * group holding them used to pass this check. Nothing flagged the
     * file, so the rename guard did not fire; after `rename_column
     * addr -> location` the compaction rewriter could match the subtree
     * neither by id (the group has none) nor by name (it changed), and
     * [com.posthog.hoglake.compaction.ParquetRewriter] null-filled the
     * WHOLE subtree — then end-snapshotted the input, which expiry
     * eventually deleted. Silent, permanent, uncounted.
     *
     * **The synthetic repetition layers are exempt, and must be.** The
     * `repeated group list` inside a LIST and the `repeated group
     * key_value` inside a MAP are parquet's own structure, not columns:
     * Iceberg has no id to match against one, pyarrow does not write one,
     * and neither does hoglake's own compaction output. Flagging them
     * would make every rewrite produce a file that instantly fails its
     * own contract check and blocks renames on its own table forever.
     *
     * The exemption is by SHAPE, not by name: the single REPEATED GROUP
     * child of a LIST/MAP-annotated wrapper. A legacy 2-level list
     * (`repeated <primitive> element` directly under the wrapper) is NOT
     * exempt — there the repeated node IS the element, a real column
     * that needs its id.
     */
    fun missingFieldIds(schema: MessageType): Boolean = anyBindingNodeWithoutId(schema.fields)

    /**
     * Whether [aggregate] will map columns by field id for this schema:
     * true when any node that BINDS to a catalog column — a leaf, a
     * container wrapper, or a VARIANT group, at any depth — carries a
     * `PARQUET:field_id`. False = the name-fallback path, whose column
     * set the hydrator must resolve at the FILE's begin_snapshot, not
     * live-at-hydration.
     *
     * Binding NODES, not primitive leaves, and that distinction is the
     * whole point: a file that stamps ids on its list and struct
     * wrappers and leaves its leaves bare answers TRUE here and binds by
     * id, where a leaf-only scan called it id-less and produced no stats
     * at all while the rewriter copied it happily. The exempt set is the
     * same one [missingFieldIds] exempts — parquet's synthetic
     * repetition layers — so the two questions are asked about one set
     * of nodes.
     *
     * A VARIANT group is such a node and is TERMINAL: it is one catalog
     * column with one field id, and `metadata`/`value`/`typed_value`
     * are variant-internal storage carrying no ids of their own, which
     * must not be mistaken for id-less columns. #77 said this by
     * special-casing variant beside the top-level leaf scan; saying it
     * once, inside the definition of "binding node", covers the nested
     * case the containers make reachable.
     *
     * "Anywhere", not "top level", for the same reason: a file whose
     * only columns are nested has no top-level leaf. A flat schema is
     * unaffected either way — every one of its fields is both a leaf and
     * a binding node.
     */
    fun usesFieldIds(schema: MessageType): Boolean = anyBindingNodeWithId(schema.fields)

    /**
     * True when any node that BINDS to a catalog column carries an id —
     * the same node set [missingFieldIds] checks, and deliberately so.
     *
     * Leaves alone was wrong, and wrong in the direction that loses
     * data quietly. A file with ids on its container WRAPPERS and none
     * on its leaves answered false here, which sent it down the
     * name-binding path — where [findField]'s fallback requires
     * `id == null` and the wrapper HAS one, so nothing matched and the
     * whole subtree produced ZERO stats. Meanwhile the rewriter, which
     * had no file-level gate at all, bound the same file happily and
     * copied every value. Two surfaces, one file, opposite answers.
     */
    private fun anyBindingNodeWithId(fields: List<Type>): Boolean =
        fields.any { field ->
            // A VARIANT group is TERMINAL here, exactly as it is in
            // [anyBindingNodeWithoutId]: one catalog column, one field
            // id, and metadata/value/typed_value beneath it carry none.
            // The two helpers must agree about the node set or the file
            // gate and the contract flag answer about different files.
            if (
                field.isPrimitive ||
                field.logicalTypeAnnotation is LogicalTypeAnnotation.VariantLogicalTypeAnnotation
            ) {
                field.id != null
            } else {
                val group = field.asGroupType()
                val synthetic = syntheticRepetitionLayer(group)
                group.id != null ||
                    anyBindingNodeWithId((synthetic ?: group).fields)
            }
        }

    /**
     * THE binding rule, shared with
     * [com.posthog.hoglake.compaction.ParquetRewriter] so the reader and
     * the rewriter cannot disagree about which field of a file is which
     * catalog column.
     *
     * By id when the file binds by id AND the candidate declares one;
     * by NAME only for a candidate that declares no id at all. An
     * id-bearing field never answers to a name — that is what makes a
     * rename safe on an id-bearing file, and it is why the file-level
     * gate above has to see the same nodes this does.
     */
    fun bindsTo(
        field: Type,
        fieldId: Long,
        name: String,
        useFieldIds: Boolean,
    ): Boolean {
        val id = field.id
        return if (id != null) useFieldIds && id.intValue().toLong() == fieldId else field.name == name
    }

    /**
     * The index of the field [fieldId]/[name] binds to among [fields],
     * or -1.
     *
     * The SEARCH, not just the predicate, and the difference is a
     * correctness bug: [bindsTo] answers "could this field be it", and
     * scanning with it first-match-wins lets an id-less field that
     * merely shares the NAME beat a later field that actually CARRIES
     * the id. Measured, catalog column `fieldId=1 name="b"` against a
     * file `[b (no id) = 999, x (field_id 1) = 42]`: the reader bounded
     * 999 and compaction WROTE 999 into output field 1, then
     * end-snapshotted the input. Silent substitution of one column's
     * data for another's.
     *
     * So: ids across ALL fields first, names second and only among
     * candidates that declare no id. Field ids are the binding contract;
     * a name is what is left when a file does not participate in it, and
     * it can never outrank one. Files that stamp ids on only some
     * columns — foreign writers, mostly — are the whole affected
     * population, and they are also the ones least able to notice.
     *
     * One function so the two surfaces cannot drift: the reader and the
     * rewriter both call it. Unifying them behind the PREDICATE alone
     * was what made both of them wrong in the same way, which is
     * precisely what an agreement check cannot see.
     */
    fun bindIndex(
        fields: List<Type>,
        fieldId: Long,
        name: String,
        useFieldIds: Boolean,
    ): Int {
        if (useFieldIds) {
            val byId = fields.indexOfFirst { it.id?.intValue()?.toLong() == fieldId }
            if (byId >= 0) return byId
        }
        return fields.indexOfFirst { it.id == null && it.name == name }
    }

    private fun anyBindingNodeWithoutId(fields: List<Type>): Boolean =
        fields.any { field ->
            if (
                field.isPrimitive ||
                field.logicalTypeAnnotation is LogicalTypeAnnotation.VariantLogicalTypeAnnotation
            ) {
                field.id == null
            } else {
                val group = field.asGroupType()
                val synthetic = syntheticRepetitionLayer(group)
                // The wrapper itself always binds; only the repetition
                // layer under it is exempt, and only its CHILDREN are
                // then checked.
                (group.id == null) ||
                    if (synthetic != null) {
                        anyBindingNodeWithoutId(synthetic.fields)
                    } else {
                        anyBindingNodeWithoutId(group.fields)
                    }
            }
        }

    /**
     * The synthetic repetition group parquet inserts between a LIST/MAP
     * wrapper and its element/key/value, or null when [group] is not
     * that shape (a struct, or a legacy 2-level list whose repeated
     * child is the element itself).
     */
    private fun syntheticRepetitionLayer(group: GroupType): GroupType? {
        if (!isContainerAnnotation(group.logicalTypeAnnotation)) return null
        val only = group.fields.singleOrNull() ?: return null
        if (only.isPrimitive || !only.isRepetition(Type.Repetition.REPEATED)) return null
        return only.asGroupType()
    }

    /**
     * Per-LEAF aggregates for the catalog columns present in [footer].
     *
     * The walk is structural: each catalog column is matched against the
     * parquet field with its id (name, for id-less files), and a
     * container recurses into the parquet shape its type implies — the
     * 3-level LIST encoding, the MAP `key_value` group, a plain group
     * for a struct. A node whose parquet counterpart has the WRONG SHAPE
     * (a struct over a primitive, a list over a bare group) takes its
     * whole subtree out of the results with a warning: a stats row bound
     * to the wrong physical column is worse than no row, and the
     * "NULL, never guessed" rule is what the caller relies on.
     *
     * Only leaves produce rows. Containers get none — Iceberg's
     * `value_counts`/`lower_bounds` are keyed on leaf ids too, and a
     * bound on "the list" has no meaning. Leaves UNDER a list or map do
     * get counts and bounds, which is exactly Iceberg's rule for
     * elements, keys and values.
     */
    fun aggregate(
        footer: ParquetMetadata,
        columns: List<CatalogColumn>,
        filePath: String,
    ): List<ColumnAgg> {
        val schema = footer.fileMetaData.schema
        val useFieldIds = usesFieldIds(schema)
        if (!useFieldIds && columns.isNotEmpty()) {
            log.warn {
                "parquet schema of $filePath carries no field ids; " +
                    "falling back to column-name matching"
            }
        }

        // DUPLICATE IDS, before any binding. Every lookup below picks
        // the FIRST field with a matching id, so a file carrying one id
        // on two fields would have had whichever came first silently
        // elected — measured: bounds from `first` recorded for a column
        // the file also declares as `second`. There is no rule that says
        // which is right, so there is no binding to make: the whole
        // file's stats are refused, loudly, and the file keeps whatever
        // it already had rather than gaining something invented.
        //
        // The file still flips to 'provided' with ZERO stats rows, and
        // the hydrator never revisits it — deliberate, and worth naming.
        // The alternative is 'failed', which buys a rehydrate lever for
        // a condition rehydrating cannot fix (the FILE is malformed, not
        // the read) at the cost of an alert that never clears. Missing
        // bounds cost pruning, not correctness; the warn is the signal,
        // and a rewrite or an expiry is the remedy.
        val duplicates = duplicateFieldIds(schema)
        if (duplicates.isNotEmpty()) {
            log.warn {
                "parquet schema of $filePath declares field id(s) ${duplicates.sorted()} more than " +
                    "once; field ids are the binding contract and a duplicate has no correct " +
                    "resolution, so no stats are produced for this file"
            }
            return emptyList()
        }

        // SYMMETRIC with the duplicate-id refusal above, and it was
        // missing. A name is the binding contract for an id-less field,
        // so two siblings sharing one have no correct resolution either
        // — and the consequence here was worse than a wrong pick: the
        // chunk walk matches by PATH, both columns have the same path,
        // so their statistics were SUMMED. A one-row file with two
        // id-less `b` columns produced `valueCount = 4` for field 1 and
        // stored it. The rewriter refuses this file, so the table was
        // left permanently uncompactable carrying permanently wrong
        // stats — and a value count above the row count is the kind of
        // number a planner divides by.
        val duplicateNames = duplicateSiblingNames(schema)
        if (duplicateNames.isNotEmpty()) {
            log.warn {
                "parquet schema of $filePath names more than one field ${duplicateNames.sorted()} " +
                    "inside one group; a name is the binding contract for an id-less field and a " +
                    "duplicate has no correct resolution, so no stats are produced for this file"
            }
            return emptyList()
        }

        val matched = LinkedHashMap<Long, Pair<CatalogColumn, Leaf>>()
        for (col in columns) {
            val field = findField(schema.fields, col, useFieldIds)
            if (field == null) {
                // WARN, not debug, when a CONTAINER goes unmatched: an
                // absent scalar is ordinary (a column added after the
                // file was written), but an absent container silently
                // takes its whole subtree's stats with it — several
                // columns' worth of pruning, gone, and the same
                // unmatchability that made the compaction rewriter
                // null-fill the subtree. If it is in the log, somebody
                // can find it.
                val detail = "column ${col.name} (field ${col.fieldId}) not present in $filePath; no stats"
                if (col.type.isNested) {
                    log.warn { "$detail for it or any of its ${col.children.size} child field(s)" }
                } else {
                    log.debug { detail }
                }
                continue
            }
            matchInto(col, field, emptyList(), useFieldIds, filePath, matched)
        }

        val out = ArrayList<ColumnAgg>(matched.size)
        for ((col, leaf) in matched.values) {
            // The leaf's path was built from this schema's own field
            // names (see matchInto), so the lookup cannot miss.
            val maxDefinitionLevel = schema.getMaxDefinitionLevel(*leaf.path.toTypedArray())
            aggregateColumn(footer.blocks, col, leaf, maxDefinitionLevel, filePath)
                ?.let { out.add(sane(it, col, filePath)) }
        }
        return out
    }

    /**
     * One aggregate through [StatsSanity] before it leaves this object.
     *
     * HERE rather than in the caller because a footer is written by the
     * same client that writes the data, one layer down, and this
     * function's output is trusted as ground truth by everything
     * downstream. A hostile (or merely broken) writer shipping
     * `min=500, max=100` for a date column got that pair copied through
     * verbatim — the exact shape a pruner reads as "no rows here", which
     * drops the file out of every scan silently. Same for a null_count
     * above the value_count it is a subset of.
     *
     * The commit path runs the identical rule on client-supplied
     * `column_stats`; one contract, both doors.
     */
    private fun sane(
        agg: ColumnAgg,
        col: CatalogColumn,
        filePath: String,
    ): ColumnAgg {
        val checked =
            StatsSanity.check(
                ColumnStats(
                    fieldId = agg.fieldId,
                    valueCount = agg.valueCount,
                    nullCount = agg.nullCount,
                    nanCount = agg.nanCount,
                    sizeBytes = agg.sizeBytes,
                    lowerBound = agg.lowerBound,
                    upperBound = agg.upperBound,
                ),
                col.type,
            )
        // The sanitizer's output is what gets stored, reported or not:
        // it also canonicalizes signed zeros, which is a conformance
        // rewrite rather than a repair and so carries no warning.
        if (checked.repairs.isNotEmpty()) {
            Metrics.statsRepaired("hydrator")
            log.warn {
                "footer stats for column ${col.name} (field ${col.fieldId}) in $filePath are not " +
                    "internally consistent (${checked.repairs.joinToString("; ")}); " +
                    "storing the repaired row"
            }
        }
        val fixed = checked.stats
        return ColumnAgg(
            fieldId = fixed.fieldId,
            valueCount = fixed.valueCount,
            nullCount = fixed.nullCount,
            nanCount = fixed.nanCount,
            sizeBytes = fixed.sizeBytes,
            lowerBound = fixed.lowerBound,
            upperBound = fixed.upperBound,
        )
    }

    /**
     * Match one catalog column against one parquet field, recursing
     * through containers and recording every LEAF pairing in [out].
     * Silent about children the file simply does not have (a column
     * added after the file was written); loud about shape disagreements.
     */
    private fun matchInto(
        col: CatalogColumn,
        field: Type,
        parentPath: List<String>,
        useFieldIds: Boolean,
        filePath: String,
        out: MutableMap<Long, Pair<CatalogColumn, Leaf>>,
    ) {
        val path = parentPath + field.name

        fun shapeMismatch(detail: String) {
            log.warn {
                "column ${col.name} (field ${col.fieldId}) is '${col.type.wire}' but the parquet " +
                    "field at ${path.joinToString(".")} in $filePath $detail; skipping its stats"
            }
        }

        // VARIANT first, because it is the one catalog SCALAR whose
        // parquet shape is a GROUP — so every group-shaped rule below
        // (the container-annotation refusal, the struct shape check, the
        // repetition guard) would misread it.
        //
        // It produces NO stats at any depth: #77 established that a
        // variant has no trustworthy scalar counts or bounds, and that
        // is a property of the type, not of where it sits. What this
        // does add is the shape CHECK, degraded to a warning like every
        // other disagreement here — thrown, it cost the whole file's
        // other columns their bounds for a column that was never going
        // to produce any.
        if (col.type == ColType.VARIANT) {
            variantFault(field)?.let { shapeMismatch(it) }
            return
        }

        // And the converse, which is #77's second prologue check —
        // deleted in the merge because the group-vs-primitive fallthrough
        // appeared to cover it. It covers a SCALAR column. It does not
        // cover a CONTAINER one: `isContainerAnnotation` knows only
        // LIST/MAP/MAP_KEY_VALUE, so a catalog STRUCT bound to a variant
        // group walked straight into `metadata`/`value`/`typed_value` —
        // and since those carry no field ids, the name fallback bound a
        // struct field literally named `value` (a legal identifier) to
        // the variant's binary payload. Measured: bounds AAA..zzz stored
        // for a column that has no such values, with and without file
        // ids. Fabricated bounds are what pruners act on, and this
        // object's rule is absent-never-guessed.
        if (field.logicalTypeAnnotation is LogicalTypeAnnotation.VariantLogicalTypeAnnotation) {
            shapeMismatch("is a native VARIANT group, which only a 'variant' column can bind to")
            return
        }

        // REPETITION, before anything else. Every catalog type reachable
        // here holds AT MOST ONE value per row: a scalar, a struct, or a
        // container whose repetition lives in its own synthetic layer
        // (which matchInto is never handed — it descends THROUGH it).
        // A REPEATED node binding to any of them is a file saying "many
        // per row" where the catalog says "one", and the two read paths
        // answer that differently and both wrongly: this one counts
        // every repetition as a value, and the rewriter copies only
        // repetition 0 and drops the rest — measured, a 2-repetition
        // struct lost half its values with rowsWritten still matching
        // the record count, and the inputs then expired.
        if (field.isRepetition(Type.Repetition.REPEATED)) {
            shapeMismatch(
                "is REPEATED, but '${col.type.wire}' holds one value per row",
            )
            return
        }

        if (!col.type.isNested) {
            if (!field.isPrimitive) {
                shapeMismatch("is a group, not a primitive leaf")
                return
            }
            out[col.fieldId] = col to Leaf(path, field.id?.intValue(), field.asPrimitiveType())
            return
        }
        if (field.isPrimitive) {
            shapeMismatch("is a primitive leaf, not a group")
            return
        }
        val group = field.asGroupType()
        when (col.type) {
            ColType.STRUCT -> {
                // Same rule the rewriter applies: a struct's counterpart
                // is a PLAIN group, and a LIST/MAP wrapper carrying the
                // struct's id is a type mismatch. Without this the
                // children simply fail to match one by one and the
                // subtree goes quiet — safe, but silent, and the two
                // surfaces would disagree about a file the rewriter
                // refuses outright.
                // CONTAINER annotations only. Rejecting ANY annotation
                // took bounds away from a struct-shaped group carrying a
                // stray unrelated one (ENUM, say) that used to produce
                // them perfectly well — and since the rewriter refused
                // the same file, such a table became permanently
                // uncompactable. A LIST/MAP annotation means the file
                // says "container" where the catalog says "struct"; any
                // other annotation on a plain group says nothing about
                // its shape, so bind by shape.
                if (isContainerAnnotation(group.logicalTypeAnnotation)) {
                    shapeMismatch(
                        "is a '${group.logicalTypeAnnotation}' group, not a struct",
                    )
                    return
                }
                for (child in col.children) {
                    val sub = findField(group.fields, child, useFieldIds)
                    if (sub == null) {
                        // Same rule one level down: a missing scalar
                        // field is ordinary schema evolution, a missing
                        // CONTAINER quietly drops everything under it.
                        val detail =
                            "struct field ${child.name} (field ${child.fieldId}) absent from " +
                                "${path.joinToString(".")} in $filePath; no stats"
                        if (child.type.isNested) log.warn { detail } else log.debug { detail }
                        continue
                    }
                    matchInto(child, sub, path, useFieldIds, filePath, out)
                }
            }
            ColType.LIST -> {
                // The parquet 3-level LIST encoding: one repeated group
                // holding exactly one field, the element. Names are not
                // significant (the spec says so), the SHAPE is.
                val repeated = group.fields.singleOrNull()
                if (repeated == null || repeated.isPrimitive || !repeated.isRepetition(Type.Repetition.REPEATED)) {
                    shapeMismatch(
                        "is not the 3-level LIST encoding (one repeated group holding the element)",
                    )
                    return
                }
                val entry = repeated.asGroupType()
                val element = entry.fields.singleOrNull()
                if (element == null) {
                    shapeMismatch("has a repeated group with ${entry.fieldCount} fields, not 1 (element)")
                    return
                }
                // The CATALOG's arity, not just the file's. A container
                // row with the wrong child count is a corrupt or
                // hand-edited catalog, and indexing into it threw
                // IndexOutOfBounds straight out of the hydrator sweep.
                // Degrade like every other disagreement.
                if (col.children.size != 1) {
                    shapeMismatch(
                        "is a list whose CATALOG row has ${col.children.size} children, not 1",
                    )
                    return
                }
                val child = col.children.single()
                if (!childBinds(child, element, useFieldIds)) {
                    shapeMismatch(
                        "has an element with field id ${element.id?.intValue()}, not the live " +
                            "element's ${child.fieldId}",
                    )
                    return
                }
                matchInto(child, element, path + repeated.name, useFieldIds, filePath, out)
            }
            ColType.MAP -> {
                // MAP / MAP_KEY_VALUE: one repeated group with exactly
                // two fields, key then value.
                val repeated = group.fields.singleOrNull()
                if (repeated == null || repeated.isPrimitive || !repeated.isRepetition(Type.Repetition.REPEATED)) {
                    shapeMismatch("is not the MAP encoding (one repeated key_value group)")
                    return
                }
                val entry = repeated.asGroupType()
                if (entry.fieldCount != 2) {
                    shapeMismatch("has a key_value group with ${entry.fieldCount} fields, not 2 (key, value)")
                    return
                }
                if (col.children.size != 2) {
                    shapeMismatch(
                        "is a map whose CATALOG row has ${col.children.size} children, not 2",
                    )
                    return
                }
                // The rewriter refuses an OPTIONAL key (the output key is
                // REQUIRED, so a row with none would fail the write
                // halfway through the group). The two surfaces have to
                // agree about which files they accept — that agreement is
                // the whole reason maxUnsignedParquetWidth is shared —
                // and a file the rewriter will never compact should not
                // be accumulating stats as though it will.
                if (!entry.getType(0).isRepetition(Type.Repetition.REQUIRED)) {
                    shapeMismatch("has an OPTIONAL key; Iceberg map keys are non-nullable")
                    return
                }
                col.children.forEachIndexed { i, child ->
                    if (!childBinds(child, entry.getType(i), useFieldIds)) {
                        shapeMismatch(
                            "has a ${if (i == 0) "key" else "value"} with field id " +
                                "${entry.getType(i).id?.intValue()}, not the live one's ${child.fieldId}",
                        )
                        return
                    }
                }
                val entryPath = path + repeated.name
                matchInto(col.children[0], entry.getType(0), entryPath, useFieldIds, filePath, out)
                matchInto(col.children[1], entry.getType(1), entryPath, useFieldIds, filePath, out)
            }
            else -> error("unreachable: ${col.type} is not a container")
        }
    }

    /**
     * Whether the synthetic child [field] — a list's element, a map's
     * key or value — really IS [col].
     *
     * The synthetic children have no useful names (every list's element
     * is called `element`), so they are reached by POSITION. Position is
     * not identity: a file whose wrapper matches but whose element
     * carries a different id would have its counts and bounds recorded
     * under a catalog field the file never claimed, and a pruner would
     * then skip files on a range that describes other data. When the
     * file declares an id, that id decides.
     *
     * An id-LESS child keeps the positional binding, and must: that is
     * the same exemption `missingFieldIds` grants the repetition layer,
     * and such a file is already flagged, so renames on its table are
     * blocked and position cannot drift out from under it.
     */
    private fun childBinds(
        col: CatalogColumn,
        field: Type,
        useFieldIds: Boolean,
    ): Boolean {
        if (!useFieldIds) return true
        val id = field.id ?: return true
        return id.intValue().toLong() == col.fieldId
    }

    /**
     * Whether [annotation] declares a parquet CONTAINER — the three that
     * mean "this group is a list or a map", as opposed to the many that
     * decorate a value (STRING, ENUM, DECIMAL...). One definition, two
     * callers: the field-id exemption and the struct shape check have to
     * mean the same thing by "container" or a file can be a container to
     * one and a struct to the other.
     */
    fun isContainerAnnotation(annotation: LogicalTypeAnnotation?): Boolean =
        annotation is LogicalTypeAnnotation.ListLogicalTypeAnnotation ||
            annotation is LogicalTypeAnnotation.MapLogicalTypeAnnotation ||
            annotation is LogicalTypeAnnotation.MapKeyValueTypeAnnotation

    /**
     * The parquet field for [col] among [fields] — [bindIndex]'s search,
     * ids before names. Per-node rather than per-file, because a nested
     * file can carry ids on its leaves and none on a synthesized
     * container group.
     */

    private fun findField(
        fields: List<Type>,
        col: CatalogColumn,
        useFieldIds: Boolean,
    ): Type? = bindIndex(fields, col.fieldId, col.name, useFieldIds).takeIf { it >= 0 }?.let { fields[it] }

    /**
     * Sibling names repeated at any level — the same question
     * [duplicateFieldIds] asks about ids, asked about the other half of
     * the binding contract.
     */
    private fun duplicateSiblingNames(schema: MessageType): Set<String> {
        val dupes = HashSet<String>()

        fun walk(fields: List<Type>) {
            dupes += fields.groupingBy { it.name }.eachCount().filterValues { it > 1 }.keys
            for (field in fields) if (!field.isPrimitive) walk(field.asGroupType().fields)
        }
        walk(schema.fields)
        return dupes
    }

    /**
     * Why [field] is not a legal native VARIANT, or null when it is.
     *
     * #77's rule, kept whole — spec version 1, a non-repeated group of
     * `metadata`/`value`/`typed_value` with a REQUIRED binary
     * `metadata` and at least one of `value`/`typed_value` — but
     * REPORTED instead of thrown.
     *
     * Thrown, it was the one shape check in this object that could take
     * a whole file's stats down: `aggregate` is documented total, every
     * other disagreement degrades the offending subtree with a warning,
     * and a variant column produces no stats either way. So an invalid
     * variant cost every OTHER column in the file its bounds, for no
     * gain.
     *
     * BY CHILD NAME, deliberately, where the rest of this object refuses
     * to trust synthetic names (see [syntheticRepetitionLayer], which
     * matches the list/map layers by SHAPE precisely because the parquet
     * spec says their names are insignificant). The asymmetry is the
     * specs': the variant spec MANDATES these three names as the
     * addressing scheme, so here the name IS the contract. hoglake#70.
     */
    private fun variantFault(field: Type): String? {
        val annotation = field.logicalTypeAnnotation as? LogicalTypeAnnotation.VariantLogicalTypeAnnotation
        if (field.isPrimitive || annotation == null || annotation.specVersion.toInt() != 1) {
            return "is not a native parquet VARIANT of spec version 1"
        }
        if (field.isRepetition(Type.Repetition.REPEATED)) return "is a REPEATED variant group"
        val children = field.asGroupType().fields
        if (children.map { it.name }.distinct().size != children.size) {
            return "has duplicate child names ${children.map { it.name }}"
        }
        if (children.any { it.name !in setOf("metadata", "value", "typed_value") }) {
            return "has children ${children.map { it.name }} outside metadata/value/typed_value"
        }

        fun binary(type: Type?): Boolean =
            type != null && type.isPrimitive &&
                type.asPrimitiveType().primitiveTypeName == PrimitiveType.PrimitiveTypeName.BINARY
        val metadata = children.find { it.name == "metadata" }
        val value = children.find { it.name == "value" }
        if (!binary(metadata) || !metadata!!.isRepetition(Type.Repetition.REQUIRED)) {
            return "has no REQUIRED binary 'metadata'"
        }
        if (value == null && children.none { it.name == "typed_value" }) {
            return "has neither 'value' nor 'typed_value'"
        }
        if (value != null && (!binary(value) || value.isRepetition(Type.Repetition.REPEATED))) {
            return "has a 'value' that is not an optional/required binary"
        }
        return null
    }

    /**
     * Counts and bounds for one leaf across every row group.
     *
     * [maxDefinitionLevel] is the leaf's, from the file schema. At 0 the
     * leaf is REQUIRED and so is every ancestor, and the parquet format
     * then guarantees no value can be null at any level: a definition
     * level counts how many optional/repeated nodes on the path are
     * defined, and with a maximum of 0 there is none that could be
     * undefined. Such a leaf's null count is 0 whether or not the writer
     * recorded it — and ClickHouse writes its non-Nullable columns
     * REQUIRED and omits their `null_count`, so without this those
     * columns hydrated to 'provided' with no rows, and so no zone maps.
     */
    private fun aggregateColumn(
        blocks: List<BlockMetaData>,
        col: CatalogColumn,
        leaf: Leaf,
        maxDefinitionLevel: Int,
        filePath: String,
    ): ColumnAgg? {
        val nullFree = maxDefinitionLevel == 0
        var valueCount = 0L
        var nullCount = 0L
        var nullCountKnown = true
        var sizeBytes = 0L
        var boundsOk = true
        var min: Any? = null
        var max: Any? = null
        var chunks = 0

        for (block in blocks) {
            for (chunk in block.columns) {
                // Full path equality, not just the leaf name: two
                // different structs can both hold a field called `id`,
                // and a name-only match would sum them together.
                if (!chunk.path.toArray().contentEquals(leaf.path.toTypedArray())) continue
                chunks++
                valueCount += chunk.valueCount
                sizeBytes += chunk.totalSize
                val st: Statistics<*>? = chunk.statistics
                when {
                    st != null && st.isNumNullsSet -> nullCount += st.numNulls
                    // Parquet spec: a max definition level of 0 means no
                    // value can be null at any level, so the omitted
                    // count is exactly 0. value_count needs no such
                    // argument — it comes from the chunk, not the stats.
                    nullFree -> Unit
                    else -> nullCountKnown = false
                }
                if (boundsOk) {
                    val bounds = chunkBounds(col, leaf, st)
                    if (bounds == null) {
                        boundsOk = false
                    } else {
                        val (lo, hi) = bounds
                        if (min == null || compare(col.type, lo, min!!) < 0) min = lo
                        if (max == null || compare(col.type, hi, max!!) > 0) max = hi
                    }
                }
            }
        }

        if (chunks == 0) {
            log.debug { "column ${col.name} has no chunks in $filePath; no stats" }
            return null
        }
        if (!nullCountKnown) {
            // null_count is NOT NULL in hog_file_column_stats; without it we
            // cannot write an honest row for this column. Only a NULLABLE
            // leaf gets here: a required one's count is known to be 0.
            log.warn {
                "column ${col.name} (field ${col.fieldId}) in $filePath is nullable (max definition " +
                    "level $maxDefinitionLevel) and its writer omitted null_count from at least one " +
                    "column chunk; skipping its stats row"
            }
            return null
        }
        return ColumnAgg(
            fieldId = col.fieldId,
            valueCount = valueCount,
            nullCount = nullCount,
            // Parquet footers carry no NaN counts; honest null over a guess.
            nanCount = null,
            sizeBytes = sizeBytes,
            lowerBound = if (boundsOk && min != null) encodeBound(col.type, min!!) else null,
            upperBound = if (boundsOk && max != null) encodeBound(col.type, max!!) else null,
        )
    }

    /** Decoded (lower, upper) for one chunk, or null when unreliable. */
    private fun chunkBounds(
        col: CatalogColumn,
        leaf: Leaf,
        st: Statistics<*>?,
    ): Pair<Any, Any>? {
        // hasNonNullValue is false when the footer carried no reliable
        // min/max (parquet-java already dropped deprecated min/max whose
        // sort order is untrustworthy) or the chunk was all-null.
        if (st == null || !st.hasNonNullValue()) return null
        val rawMin = st.minBytes ?: return null
        val rawMax = st.maxBytes ?: return null
        val lo = decode(col, leaf, rawMin, upper = false) ?: return null
        val hi = decode(col, leaf, rawMax, upper = true) ?: return null
        if (isNan(lo) || isNan(hi)) return null
        return lo to hi
    }

    private fun isNan(v: Any): Boolean = (v is Float && v.isNaN()) || (v is Double && v.isNaN())

    /**
     * Decode one parquet statistics value into the typed representation for
     * the catalog column type, or null when the physical/logical shape does
     * not decode safely under that type.
     */
    private fun decode(
        col: CatalogColumn,
        leaf: Leaf,
        raw: ByteArray,
        upper: Boolean,
    ): Any? {
        val physical = leaf.primitive.primitiveTypeName

        // An unsigned INT annotation is not decoration. It says the bits
        // are a magnitude rather than a signed value, AND that parquet
        // ordered this chunk's min/max unsigned. A catalog type may read
        // one only if its own domain CONTAINS [0, 2^width): otherwise the
        // top of the file's range does not fit the bound, and the
        // orderings disagree — which is how a bound pair comes back with
        // lower > upper, the shape a pruner reads as "no rows here",
        // silently dropping the file from every scan.
        //
        // The rule is per-width rather than per-type, because the same
        // annotation is fine or fatal depending on both: int16 reads
        // INT(8, unsigned) happily (255 fits) and must refuse
        // INT(16, unsigned) (65535 does not), and int8 refuses even the
        // 8-bit one.
        val unsignedWidth = unsignedIntWidth(leaf)
        if (unsignedWidth != null && unsignedWidth > col.type.maxUnsignedParquetWidth) {
            // Loud, like the decimal-scale mismatch below: a fleet-wide
            // loss of bounds on a whole column type is a pruning
            // regression, and silence would make it invisible until
            // someone noticed scans got slower.
            log.warn {
                "column ${col.name} is '${col.type.wire}' but the parquet leaf is " +
                    "INT($unsignedWidth, unsigned), whose values do not all fit that type; " +
                    "skipping bounds (counts are unaffected)"
            }
            return null
        }

        return when (col.type) {
            ColType.VARIANT -> null
            ColType.BOOLEAN ->
                if (physical == PrimitiveType.PrimitiveTypeName.BOOLEAN && raw.size == 1) {
                    raw[0] != 0.toByte()
                } else {
                    null
                }
            // int8/int16/uint8/uint16 all ride parquet INT32 (with an
            // INT(width, signed) annotation writers add): the physical
            // int32 already holds the true value, and for widths <= 16 the
            // signed and unsigned parquet sort orders agree, so the
            // footer's min/max are trustworthy either way. All four map to
            // Iceberg int, hence the 4-byte bound. (A full-width unsigned
            // int32 never reaches here — the guard above sent it away.)
            ColType.INT8, ColType.INT16, ColType.UINT8, ColType.UINT16, ColType.INT ->
                if (physical == PrimitiveType.PrimitiveTypeName.INT32) readIntLE(raw) else null
            // uint32 maps to Iceberg long. hoglake's own writers emit INT64
            // (see docs/iceberg-federation.md §2), but pyarrow/DuckDB emit
            // INT32 + INT(32, unsigned) natively, so both are read. The
            // INT32 form REQUIRES the unsigned annotation: without it
            // parquet computed the chunk's min/max in SIGNED order, and
            // reinterpreting those bounds as unsigned would invert them.
            ColType.UINT32 ->
                when (physical) {
                    PrimitiveType.PrimitiveTypeName.INT64 -> readLongLE(raw)
                    PrimitiveType.PrimitiveTypeName.INT32 ->
                        if (isUnsignedInt(leaf, 32)) readIntLE(raw)?.let { it.toLong() and 0xFFFFFFFFL } else null
                    else -> null
                }
            // uint64 maps to decimal(20,0); the bound is a BigInteger in
            // [0, 2^64). Same sort-order argument as uint32's INT32 form,
            // and here it bites at 2^63, so the unsigned annotation is
            // mandatory — an unannotated INT64's footer bounds are
            // signed-ordered and simply are not uint64 bounds.
            ColType.UINT64 ->
                if (physical == PrimitiveType.PrimitiveTypeName.INT64 && isUnsignedInt(leaf, 64)) {
                    readLongLE(raw)?.let { unsignedLong(it) }
                } else {
                    null
                }
            // The read-path twin of ParquetRewriter's UINT32_TO_LONG.
            // No PROMOTION produces this pairing (DuckLake has no
            // unsigned -> signed rung), but a foreign writer does: arrow
            // and DuckDB both emit unsigned 32-bit data as INT32 +
            // INT(32, unsigned), and a client is free to declare that
            // column `long`, whose domain contains every uint32 value.
            // Sign-extending one turns 0xFFFFFFFF into -1, putting the
            // upper bound BELOW the lower and making every pruner drop the
            // file. Zero-extend at any width instead; the annotation is
            // also what tells us parquet ordered the chunk unsigned, so
            // the bounds are the unsigned min/max we want.
            ColType.LONG ->
                when (physical) {
                    PrimitiveType.PrimitiveTypeName.INT64 -> readLongLE(raw)
                    PrimitiveType.PrimitiveTypeName.INT32 ->
                        readIntLE(raw)?.let { bits ->
                            if (isUnsignedInt(leaf)) bits.toLong() and 0xFFFFFFFFL else bits.toLong()
                        }
                    else -> null
                }
            ColType.FLOAT ->
                if (physical == PrimitiveType.PrimitiveTypeName.FLOAT) {
                    readIntLE(raw)?.let { Float.fromBits(it) }
                } else {
                    null
                }
            ColType.DOUBLE ->
                when (physical) {
                    PrimitiveType.PrimitiveTypeName.DOUBLE -> readLongLE(raw)?.let { Double.fromBits(it) }
                    PrimitiveType.PrimitiveTypeName.FLOAT ->
                        readIntLE(raw)?.let { Float.fromBits(it).toDouble() }
                    else -> null
                }
            ColType.DATE ->
                if (physical == PrimitiveType.PrimitiveTypeName.INT32) readIntLE(raw) else null
            ColType.TIME -> decodeTime(leaf, raw)
            // Unit-driven, always: the parquet annotation is authoritative
            // for what a file's int64s MEAN, and the catalog type only says
            // what the column was declared as. (Parquet has no seconds
            // timestamp unit at all — pyarrow 25 coerces timestamp[s] to
            // TIMESTAMP(MILLIS) on write, verified — so a timestamp_s
            // column's files are physically millis and decode through the
            // same arm.) All of these map to Iceberg timestamp, so the
            // bound is micros.
            ColType.TIMESTAMP_S, ColType.TIMESTAMP_MS, ColType.TIMESTAMP, ColType.TIMESTAMPTZ ->
                decodeTimestamp(leaf, raw, upper)
            // The one temporal type whose bound unit is NOT micros.
            ColType.TIMESTAMP_NS -> decodeTimestampNanos(leaf, raw)
            // json maps to Iceberg string and rides the same BYTE_ARRAY;
            // the JSON logical annotation is metadata we do not require,
            // because it changes neither the bytes nor their sort order.
            ColType.STRING, ColType.JSON ->
                if (physical == PrimitiveType.PrimitiveTypeName.BINARY && bytesSortUnsigned(col, leaf)) {
                    raw
                } else {
                    null
                }
            // The annotation gate applies here too, and its absence was
            // an arm missed rather than a decision: a DECIMAL(38,0)
            // FLBA(16) under a `uuid` column had parquet's SIGNED
            // two's-complement ordering taken as an unsigned-byte uuid
            // bound. The pair only stopped being stored because
            // StatsSanity caught the inversion afterwards — which also
            // reported it as a repaired row, blaming the writer for a
            // rule this surface had not applied.
            ColType.UUID_T ->
                if (physical == PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY &&
                    raw.size == 16 &&
                    bytesSortUnsigned(col, leaf)
                ) {
                    raw
                } else {
                    null
                }
            ColType.BINARY ->
                when (physical) {
                    PrimitiveType.PrimitiveTypeName.BINARY,
                    PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
                    -> if (bytesSortUnsigned(col, leaf)) raw else null
                    else -> null
                }
            ColType.DECIMAL -> decodeDecimal(col, leaf, raw)
            // Unreachable: only leaves reach decode, and a container is
            // never a leaf. Refusing rather than erroring keeps the
            // "bounds NULL, never guessed" contract total.
            ColType.LIST, ColType.STRUCT, ColType.MAP -> null
        }
    }

    /**
     * Whether a byte-array leaf's bounds may be taken VERBATIM as an
     * unsigned-byte-ordered bound.
     *
     * The bytes are only half the story; the leaf's annotation decides
     * what ORDER parquet put them in, and a string/json/binary column
     * stores its bound unsigned-lexicographic. Take the min/max of a
     * DECIMAL-annotated BINARY leaf verbatim and you get parquet's
     * SIGNED two's-complement ordering reinterpreted as unsigned —
     * measured, a column holding 0x01 and 0xff stored
     * lower=ff upper=01, an INVERTED pair, which every pruner reads as
     * "no rows here" and drops the file from every scan. The counts look
     * healthy the whole time.
     *
     * Same family as the INT(w, unsigned) rule above, and a positive
     * ALLOWLIST for the same reason: an annotation nobody has thought
     * about must not inherit "bytes are bytes" by default.
     *
     *  - none / STRING / JSON / BSON / ENUM: unsigned byte order. Yes.
     *  - UUID: 16 big-endian bytes, unsigned lexicographic. Yes.
     *  - DECIMAL: signed two's-complement. No.
     *  - FLOAT16: IEEE half order, not byte order. No.
     *  - INTERVAL: parquet defines no order for it at all. No.
     */
    fun bytesSortUnsigned(annotation: LogicalTypeAnnotation?): Boolean =
        annotation == null ||
            annotation is LogicalTypeAnnotation.StringLogicalTypeAnnotation ||
            annotation is LogicalTypeAnnotation.JsonLogicalTypeAnnotation ||
            annotation is LogicalTypeAnnotation.BsonLogicalTypeAnnotation ||
            annotation is LogicalTypeAnnotation.EnumLogicalTypeAnnotation ||
            annotation is LogicalTypeAnnotation.UUIDLogicalTypeAnnotation

    private fun bytesSortUnsigned(
        col: CatalogColumn,
        leaf: Leaf,
    ): Boolean {
        val annotation = leaf.primitive.logicalTypeAnnotation
        val ok = bytesSortUnsigned(annotation)
        if (!ok) {
            log.warn {
                "column ${col.name} (field ${col.fieldId}) is '${col.type.wire}', whose bounds are " +
                    "unsigned byte order, but the parquet leaf at ${leaf.path.joinToString(".")} is " +
                    "annotated '$annotation', which parquet sorted differently; skipping bounds " +
                    "(counts are unaffected)"
            }
        }
        return ok
    }

    private fun decodeTime(
        leaf: Leaf,
        raw: ByteArray,
    ): Long? {
        val unit =
            (leaf.primitive.logicalTypeAnnotation as? LogicalTypeAnnotation.TimeLogicalTypeAnnotation)
                ?.unit ?: return null
        return when (unit) {
            LogicalTypeAnnotation.TimeUnit.MICROS ->
                if (leaf.primitive.primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT64) {
                    readLongLE(raw)
                } else {
                    null
                }
            LogicalTypeAnnotation.TimeUnit.MILLIS ->
                if (leaf.primitive.primitiveTypeName == PrimitiveType.PrimitiveTypeName.INT32) {
                    readIntLE(raw)?.let { it * 1_000L }
                } else {
                    null
                }
            LogicalTypeAnnotation.TimeUnit.NANOS -> null // sub-micro truncation of a time bound: skip
        }
    }

    private fun decodeTimestamp(
        leaf: Leaf,
        raw: ByteArray,
        upper: Boolean,
    ): Long? {
        if (leaf.primitive.primitiveTypeName != PrimitiveType.PrimitiveTypeName.INT64) return null
        val unit =
            (leaf.primitive.logicalTypeAnnotation as? LogicalTypeAnnotation.TimestampLogicalTypeAnnotation)
                ?.unit ?: return null
        val v = readLongLE(raw) ?: return null
        return try {
            when (unit) {
                LogicalTypeAnnotation.TimeUnit.MICROS -> v
                LogicalTypeAnnotation.TimeUnit.MILLIS -> Math.multiplyExact(v, 1_000L)
                // Nanos truncate: floor for the lower bound, ceil for the upper,
                // so the bound stays valid for the true values.
                LogicalTypeAnnotation.TimeUnit.NANOS ->
                    if (upper) Math.floorDiv(Math.addExact(v, 999L), 1_000L) else Math.floorDiv(v, 1_000L)
            }
        } catch (_: ArithmeticException) {
            // The unit conversion overflows int64 micros (a millis bound
            // near Long.MAX_VALUE — hostile-writer craftable). The decode
            // contract is "bounds NULL, never guessed" — an overflow must
            // degrade to a null bound, never escape and fail the file.
            null
        }
    }

    /**
     * timestamp_ns bounds are NANOS (Iceberg V3 single-value
     * serialization), so the conversion runs the other way from
     * [decodeTimestamp]: millis and micros scale UP, which is exact.
     * Overflow degrades to a null bound, never an escaping failure —
     * same contract as every other decode here.
     */
    private fun decodeTimestampNanos(
        leaf: Leaf,
        raw: ByteArray,
    ): Long? {
        if (leaf.primitive.primitiveTypeName != PrimitiveType.PrimitiveTypeName.INT64) return null
        val unit =
            (leaf.primitive.logicalTypeAnnotation as? LogicalTypeAnnotation.TimestampLogicalTypeAnnotation)
                ?.unit ?: return null
        val v = readLongLE(raw) ?: return null
        return try {
            when (unit) {
                LogicalTypeAnnotation.TimeUnit.NANOS -> v
                LogicalTypeAnnotation.TimeUnit.MICROS -> Math.multiplyExact(v, 1_000L)
                LogicalTypeAnnotation.TimeUnit.MILLIS -> Math.multiplyExact(v, 1_000_000L)
            }
        } catch (_: ArithmeticException) {
            null
        }
    }

    /**
     * True when the leaf carries parquet INT(*, isSigned = false) at ANY
     * width. Two things follow from an unsigned annotation and both
     * matter: the physical bits are a magnitude rather than a signed
     * value, and parquet computed the chunk's min/max in UNSIGNED order.
     */
    private fun isUnsignedInt(leaf: Leaf): Boolean =
        (leaf.primitive.logicalTypeAnnotation as? LogicalTypeAnnotation.IntLogicalTypeAnnotation)
            ?.isSigned == false

    /** The leaf's unsigned INT width, or null when it is not unsigned-annotated. */
    private fun unsignedIntWidth(leaf: Leaf): Int? =
        (leaf.primitive.logicalTypeAnnotation as? LogicalTypeAnnotation.IntLogicalTypeAnnotation)
            ?.takeIf { !it.isSigned }
            ?.bitWidth

    /** True when the leaf carries parquet INT(width, isSigned = false). */
    private fun isUnsignedInt(
        leaf: Leaf,
        width: Int,
    ): Boolean {
        val a =
            leaf.primitive.logicalTypeAnnotation as? LogicalTypeAnnotation.IntLogicalTypeAnnotation
                ?: return false
        return a.bitWidth == width && !a.isSigned
    }

    /** The unsigned value of a 64-bit pattern, as a BigInteger in [0, 2^64). */
    private fun unsignedLong(bits: Long): BigInteger =
        if (bits >= 0) {
            BigInteger.valueOf(bits)
        } else {
            BigInteger.valueOf(bits).add(TWO_POW_64)
        }

    private fun decodeDecimal(
        col: CatalogColumn,
        leaf: Leaf,
        raw: ByteArray,
    ): BigInteger? {
        val parquetScale =
            (leaf.primitive.logicalTypeAnnotation as? LogicalTypeAnnotation.DecimalLogicalTypeAnnotation)
                ?.scale ?: return null
        val catalogScale = col.decimalScale ?: return null
        if (parquetScale != catalogScale) {
            log.warn {
                "decimal scale mismatch for ${col.name}: parquet=$parquetScale catalog=$catalogScale; skipping bounds"
            }
            return null
        }
        return when (leaf.primitive.primitiveTypeName) {
            PrimitiveType.PrimitiveTypeName.INT32 -> readIntLE(raw)?.let { BigInteger.valueOf(it.toLong()) }
            PrimitiveType.PrimitiveTypeName.INT64 -> readLongLE(raw)?.let { BigInteger.valueOf(it) }
            PrimitiveType.PrimitiveTypeName.BINARY,
            PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
            ->
                if (raw.isEmpty()) null else BigInteger(raw)
            else -> null
        }
    }

    private fun encodeBound(
        type: ColType,
        v: Any,
    ): ByteArray =
        when (type) {
            ColType.VARIANT -> error("variant has no scalar bounds")
            // These four decode to raw bytes that ARE the Iceberg encoding.
            ColType.STRING, ColType.JSON, ColType.UUID_T, ColType.BINARY -> (v as ByteArray).copyOf()
            else -> IcebergSingleValue.encode(type, v)
        }

    @Suppress("UNCHECKED_CAST")
    private fun compare(
        type: ColType,
        a: Any,
        b: Any,
    ): Int =
        when (type) {
            ColType.VARIANT -> error("variant has no scalar bounds")
            ColType.STRING, ColType.JSON, ColType.UUID_T, ColType.BINARY ->
                java.util.Arrays.compareUnsigned(a as ByteArray, b as ByteArray)
            // uint32 decodes to a non-negative Long and uint64 to a
            // non-negative BigInteger, so natural order is unsigned order.
            else -> (a as Comparable<Any>).compareTo(b)
        }

    private fun readIntLE(raw: ByteArray): Int? =
        if (raw.size == 4) ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN).int else null

    private fun readLongLE(raw: ByteArray): Long? =
        if (raw.size == 8) ByteBuffer.wrap(raw).order(ByteOrder.LITTLE_ENDIAN).long else null

    /**
     * Field ids the schema declares more than once, over EVERY node
     * (leaves and container wrappers alike — both bind).
     */
    private fun duplicateFieldIds(schema: MessageType): Set<Int> {
        val seen = HashSet<Int>()
        val dupes = HashSet<Int>()

        fun walk(fields: List<Type>) {
            for (field in fields) {
                field.id?.intValue()?.let { if (!seen.add(it)) dupes.add(it) }
                if (!field.isPrimitive) walk(field.asGroupType().fields)
            }
        }
        walk(schema.fields)
        return dupes
    }
}
