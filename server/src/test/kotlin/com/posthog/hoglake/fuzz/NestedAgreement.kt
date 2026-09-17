package com.posthog.hoglake.fuzz

import com.posthog.hoglake.compaction.InvalidDataException
import com.posthog.hoglake.compaction.ParquetRewriter
import com.posthog.hoglake.compaction.UnconvertibleSchemaException
import com.posthog.hoglake.hydrator.CatalogColumn
import com.posthog.hoglake.hydrator.FooterParse
import com.posthog.hoglake.hydrator.FooterStats
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.assignFieldIds
import com.posthog.hoglake.service.ColumnTrees
import com.posthog.hoglake.stats.IcebergSingleValue
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.SeekableInputStream
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.Type
import java.nio.file.Files
import java.nio.file.Path

/** One oracle violation. [kind] is the dedupe key. */
class Finding(
    val kind: String,
    val detail: String,
    val cause: Throwable? = null,
) {
    /** Root frame of the cause, for dedupe. */
    val frame: String
        get() =
            cause?.let { c ->
                val t = generateSequence(c) { it.cause }.last()
                "${t.javaClass.name}@" +
                    (
                        t.stackTrace.firstOrNull()
                            ?.let { "${it.className}.${it.methodName}:${it.lineNumber}" } ?: "?"
                    )
            } ?: ""

    override fun toString(): String = "[$kind] $detail${if (frame.isEmpty()) "" else " | $frame"}"
}

/**
 * Campaign 1: the reader/rewriter AGREEMENT fuzzer.
 *
 * One iteration builds a random VALID catalog column tree, derives a
 * parquet MessageType from it (canonical, or heavily mutated: any
 * repetition, any annotation, ids present/absent/wrong/duplicated,
 * synthetic layers renamed, arities broken, physical types swapped),
 * writes a small data file under that schema, and then runs BOTH
 * surfaces over it:
 *
 *  - [FooterStats.aggregate] (the hydrator's read side)
 *  - [ParquetRewriter.rewrite] (compaction's write side)
 *
 * Oracles: typed-refusal-only on both surfaces; bound sanity; no stats
 * for a field id the catalog does not own; per-leaf value conservation
 * for accepted rewrites; and agreement — a leaf the rewriter copied real
 * values for must have produced stats on the read side.
 */
object NestedAgreement {
    fun runOne(
        e: Entropy,
        tmp: Path,
        sink: (Finding) -> Unit,
    ) {
        val depth = e.int(1, NestedFuzz.maxDepth())
        val defs = NestedFuzz.genDefs(e, e.int(1, 4), depth)

        // Oracle 0: a tree built to the documented rules must validate.
        try {
            ColumnTrees.validate(defs)
        } catch (t: Throwable) {
            sink(Finding("validate-refused-valid-tree", "defs=$defs", t))
            return
        }

        val live: List<Column> = assignFieldIds(defs, 1L)
        val catalog: List<CatalogColumn> = NestedFuzz.toCatalogColumns(live)

        val canonical = e.int(0, 9) < 3
        val derived = NestedFuzz.deriveSchema(e, live, if (canonical) 0 else e.int(5, 40))
        val schema = derived.schema

        val src = tmp.resolve("in.parquet")
        val rows = e.int(1, 6)
        val maxRep = if (e.int(0, 99) < 3) e.int(20, 120) else e.int(0, 4)
        try {
            NestedFuzz.writeFile(e, schema, src, rows, maxRep)
        } catch (t: Throwable) {
            // Generator could not express data for this shape (e.g. INT96
            // in an odd slot). Not a finding: the subject never saw it.
            return
        }

        val footer: ParquetMetadata =
            try {
                FooterParse.parse(fileOf(src))
            } catch (t: Throwable) {
                sink(Finding("own-output-unreadable", "schema=$schema", t))
                return
            }

        // ---- reader surface, whole-catalog -------------------------------
        val allAggs =
            try {
                FooterStats.missingFieldIds(schema)
                FooterStats.usesFieldIds(schema)
                FooterStats.aggregate(footer, catalog, src.toString())
            } catch (t: Throwable) {
                sink(Finding("reader-raw-throw", "schema=$schema catalog=$catalog", t))
                return
            }

        // ---- CORRECTNESS, not agreement ----------------------------------
        checkBindingGroundTruth(catalog, schema, emptyList(), sink)
        checkReaderReadTheBoundColumn(catalog, schema, footer, allAggs, sink)

        val catalogLeafIds = leafIds(catalog)
        val catalogNodeIds = allNodeIds(catalog)
        for (agg in allAggs) {
            if (agg.fieldId !in catalogLeafIds) {
                sink(
                    Finding(
                        "stats-for-unowned-field",
                        "field ${agg.fieldId} not a catalog LEAF (nodes=$catalogNodeIds) schema=$schema",
                    ),
                )
            }
            checkBounds(agg, catalog, sink, schema)
        }

        // ---- per top-level column: rewrite + agreement --------------------
        val duplicates = NestedFuzz.duplicateIds(schema)
        val duplicateNames = duplicateSiblingNames(schema)
        val partlyIdless = FooterStats.missingFieldIds(schema)
        for ((i, col) in live.withIndex()) {
            val out = tmp.resolve("out$i.parquet")
            val sortFields = maybeSort(e, col)
            // Which of the two typed refusals fired, if either. The
            // agreement oracles below are about SCHEMA agreement — do
            // the two surfaces accept the same shapes — so a refusal
            // over a VALUE (an empty blob under a decimal, a row past
            // the node budget) is outside what they can judge, and
            // reporting one as a shape disagreement would be noise.
            var dataRefused = false
            val result =
                try {
                    ParquetRewriter.rewrite(
                        listOf(ParquetRewriter.Input(src, 0L, null)),
                        listOf(col),
                        sortFields,
                        out,
                    )
                } catch (t: UnconvertibleSchemaException) {
                    null
                } catch (t: InvalidDataException) {
                    dataRefused = true
                    // The rewriter's SECOND typed refusal: the schema
                    // pairing is fine but a VALUE is not (an empty blob
                    // under a decimal, an over-precision unscaled value,
                    // a row past the node budget). Typed is the contract;
                    // which of the two it is, is not.
                    null
                } catch (t: Throwable) {
                    sink(
                        Finding(
                            "rewriter-raw-throw",
                            "live=${col.def.type.wire} name=${col.def.name} sort=$sortFields schema=$schema",
                            t,
                        ),
                    )
                    continue
                }

            if (result == null) {
                // REVERSE DIRECTION: the rewriter refused the column, but
                // the reader bound and BOUNDED every leaf under it — it saw
                // nothing wrong at all. Sanctioned only when the reader's
                // rows carry no bounds (the counts-without-bounds
                // physical-mismatch case in the documented matrix).
                val perColRefused =
                    try {
                        FooterStats.aggregate(footer, listOf(catalogOf(catalog, col.fieldId)!!), src.toString())
                    } catch (t: Throwable) {
                        emptyList()
                    }
                val wanted = leafIdsUnder(catalogOf(catalog, col.fieldId)!!)
                val bounded = perColRefused.filter { it.lowerBound != null }.map { it.fieldId }.toSet()
                // Duplicate NAMES are the same class of sanctioned
                // asymmetry as duplicate IDS, and for a concrete reason:
                // the reader only READS, and reads by id, so a file with
                // two `z1` fields carrying distinct ids is unambiguous to
                // it. The rewriter has to BUILD a schema and then address
                // its fields, and parquet-java addresses a group's fields
                // by name — so the same file is a coin flip on the write
                // side. Refusing it there is the fix, not a disagreement.
                if (!dataRefused && duplicateNames.isEmpty() && !unitDeferral(col, schema) &&
                    !decimalDeferral(col, schema) &&
                    wanted.isNotEmpty() && bounded.containsAll(wanted)
                ) {
                    sink(
                        Finding(
                            "agreement-reader-bounded-writer-refused",
                            "rewriter refused '${col.def.name}' (${col.def.type.wire}) but the reader " +
                                "produced BOUNDS for every leaf under it ($wanted).\n    liveColumn=$col" +
                                "\n    inSchema=$schema",
                        ),
                    )
                }
                if (canonical && !dataRefused && duplicates.isEmpty() && duplicateNames.isEmpty() &&
                    !unitDeferral(col, schema) && !decimalDeferral(col, schema)
                ) {
                    sink(
                        Finding(
                            "canonical-schema-refused",
                            "rewriter refused its OWN canonical shape for ${col.def.type.wire} " +
                                "'${col.def.name}' params=${col.def.typeParams} schema=$schema",
                        ),
                    )
                }
                continue
            }

            if (result.rowsWritten != rows.toLong()) {
                sink(
                    Finding(
                        "row-count-loss",
                        "rowsWritten=${result.rowsWritten} input rows=$rows schema=$schema",
                    ),
                )
            }

            val (outSchema, outLeaves) =
                try {
                    NestedFuzz.leafStats(out)
                } catch (t: Throwable) {
                    sink(Finding("rewrite-output-unreadable", "outSchema-from=$col schema=$schema", t))
                    continue
                }

            // Round trip: the output must read back through the reader too.
            try {
                val outFooter = FooterParse.parse(fileOf(out))
                FooterStats.aggregate(outFooter, listOf(catalogOf(catalog, col.fieldId)!!), out.toString())
            } catch (t: Throwable) {
                sink(Finding("roundtrip-reader-throw", "outSchema=$outSchema", t))
            }

            val perCol =
                try {
                    FooterStats.aggregate(footer, listOf(catalogOf(catalog, col.fieldId)!!), src.toString())
                } catch (t: Throwable) {
                    sink(Finding("reader-raw-throw-percol", "col=$col schema=$schema", t))
                    continue
                }
            val readLeaves = perCol.associateBy { it.fieldId }

            val inLeaves = NestedFuzz.leafStats(src).second
            val inById = inLeaves.filter { it.fieldId != null }.groupBy { it.fieldId!! }

            for (leaf in outLeaves) {
                val fid = leaf.fieldId ?: continue
                if (fid == ParquetRewriter.ROW_ID_FIELD_ID) continue
                val nonNull = leaf.valueCount - (leaf.nullCount ?: 0L)
                if (nonNull <= 0L) continue

                // AGREEMENT: the rewriter copied real values into this
                // leaf, so the reader must have bound it too.
                if (fid.toLong() !in readLeaves.keys) {
                    sink(
                        Finding(
                            "agreement-writer-copied-reader-silent",
                            "field $fid copied ($nonNull values, outPath=${leaf.path}) by the rewriter " +
                                "but the reader produced no stats row.\n    liveColumn=$col\n    " +
                                "readerLeaves=${readLeaves.keys}\n    inSchema=$schema\n    outSchema=$outSchema",
                        ),
                    )
                }

                // CONSERVATION: bound by a unique id on the input side —
                // and ONLY when every binding node of the input carries
                // one. In a partly id-less file "the leaf with parquet id
                // N" and "the column the binding rule chose for catalog
                // field N" are different columns: an id-less field
                // matching by NAME is a legal binding, and the same
                // number may sit on some unrelated nested leaf. Comparing
                // those two is comparing different columns, not measuring
                // loss.
                val srcLeaf = inById[fid]?.singleOrNull()
                if (srcLeaf != null && duplicates.isEmpty() && !partlyIdless && srcLeaf.nullCount != null) {
                    val srcNonNull = srcLeaf.valueCount - srcLeaf.nullCount
                    if (srcNonNull != nonNull) {
                        sink(
                            Finding(
                                "conservation-loss",
                                "field $fid: input non-null=$srcNonNull output non-null=$nonNull " +
                                    "(srcPath=${srcLeaf.path} outPath=${leaf.path}) schema=$schema",
                            ),
                        )
                    }
                }
            }
        }
    }

    /**
     * The binding rule, restated from the SPEC and deliberately not
     * shared with the subject: field ids across all candidates first,
     * then an exact name match among candidates that declare no id at
     * all.
     *
     * A second, independent statement of the rule is the whole point.
     * Every other oracle in this file asks whether the two surfaces
     * AGREE, and agreement stopped being a correctness signal the moment
     * they were unified behind one implementation: a bug in the shared
     * rule makes both of them wrong in the same way, and 1.07M
     * executions saw nothing while an id-less field that merely shared a
     * column's NAME outranked the field carrying its ID — the reader
     * bounding one column and compaction copying it into the slot of
     * another.
     */
    private fun expectedBindIndex(
        fields: List<Type>,
        fieldId: Long,
        name: String,
        useFieldIds: Boolean,
    ): Int {
        if (useFieldIds) {
            for ((i, f) in fields.withIndex()) {
                if (f.id?.intValue()?.toLong() == fieldId) return i
            }
        }
        for ((i, f) in fields.withIndex()) {
            if (f.id == null && f.name == name) return i
        }
        return -1
    }

    /**
     * Every sibling group of the catalog forest, checked against the
     * file's corresponding group: the subject's binding decision must
     * equal [expectedBindIndex]'s, at every level.
     */

    private fun checkBindingGroundTruth(
        columns: List<CatalogColumn>,
        group: GroupType,
        path: List<String>,
        sink: (Finding) -> Unit,
    ) {
        val useFieldIds = FooterStats.usesFieldIds(group as? MessageType ?: return)

        fun walk(
            cols: List<CatalogColumn>,
            fields: List<Type>,
            where: List<String>,
        ) {
            for (col in cols) {
                val want = expectedBindIndex(fields, col.fieldId, col.name, useFieldIds)
                val got = FooterStats.bindIndex(fields, col.fieldId, col.name, useFieldIds)
                if (got != want) {
                    sink(
                        Finding(
                            "binding-ground-truth",
                            "column '${col.name}' (field ${col.fieldId}) at " +
                                "${if (where.isEmpty()) "<root>" else where.joinToString(".")} " +
                                "bound index $got, but the rule says $want " +
                                "(${fields.map { "${it.name}#${it.id?.intValue()}" }}) schema=$group",
                        ),
                    )
                    continue
                }
                if (want < 0 || col.children.isEmpty()) continue
                val bound = fields[want]
                if (!bound.isPrimitive) {
                    walk(col.children, bound.asGroupType().fields, where + col.name)
                }
            }
        }
        walk(columns, group.fields, path)
    }

    /**
     * The end-to-end half: for a top-level SCALAR column, the stats row
     * the reader produced must describe the column the rule says it
     * bound to — not merely the one the rewriter also chose.
     *
     * Counts identify the physical column without touching bound
     * encodings: two different leaves almost never share a value/null
     * count under randomly generated data, and when they do the check is
     * simply not discriminating rather than wrong.
     */
    private fun checkReaderReadTheBoundColumn(
        catalog: List<CatalogColumn>,
        schema: MessageType,
        footer: ParquetMetadata,
        aggs: List<FooterStats.ColumnAgg>,
        sink: (Finding) -> Unit,
    ) {
        val useFieldIds = FooterStats.usesFieldIds(schema)
        val byId = aggs.associateBy { it.fieldId }

        // Every catalog LEAF, at any depth — not just the top-level
        // scalars. This is the genuinely method-independent half of the
        // pair: it reads the FOOTER's own per-chunk counts instead of
        // re-running a binding rule, so it cannot be satisfied by a
        // consistent pair of wrong rules. Restricting it to depth 1 threw
        // away most of that value on exactly the shapes this branch is
        // about.
        fun walk(
            cols: List<CatalogColumn>,
            fields: List<Type>,
            path: List<String>,
        ) {
            for (col in cols) {
                val want = expectedBindIndex(fields, col.fieldId, col.name, useFieldIds)
                if (want < 0) {
                    if (byId.containsKey(col.fieldId)) {
                        sink(
                            Finding(
                                "stats-for-unbound-column",
                                "column '${col.name}' (field ${col.fieldId}) binds to nothing, yet " +
                                    "the reader produced a stats row for it; schema=$schema",
                            ),
                        )
                    }
                    continue
                }
                val field = fields[want]
                val here = path + field.name
                if (!field.isPrimitive) {
                    // Struct interiors only, for the same reason the
                    // binding walk stops there: a list/map child's path
                    // runs through the synthetic repetition layer, which
                    // this does not model.
                    if (col.type == ColType.STRUCT) walk(col.children, field.asGroupType().fields, here)
                    continue
                }
                val agg = byId[col.fieldId] ?: continue
                val expectedValues =
                    footer.blocks.sumOf { b ->
                        b.columns.filter { it.path.toArray().contentEquals(here.toTypedArray()) }
                            .sumOf { it.valueCount }
                    }
                if (expectedValues != agg.valueCount) {
                    sink(
                        Finding(
                            "reader-read-the-wrong-column",
                            "column '${col.name}' (field ${col.fieldId}) should bind to " +
                                "'${here.joinToString(".")}#${field.id?.intValue()}' " +
                                "($expectedValues values) but its stats row counts " +
                                "${agg.valueCount}; schema=$schema",
                        ),
                    )
                }
            }
        }
        walk(catalog, schema.fields, emptyList())
    }

    /** The parquet time/timestamp unit a catalog type's own files carry. */
    private fun nativeUnit(type: ColType): LogicalTypeAnnotation.TimeUnit? =
        when (type) {
            // Parquet has no seconds unit, so timestamp_s files are millis.
            ColType.TIMESTAMP_S, ColType.TIMESTAMP_MS -> LogicalTypeAnnotation.TimeUnit.MILLIS
            ColType.TIMESTAMP, ColType.TIMESTAMPTZ, ColType.TIME -> LogicalTypeAnnotation.TimeUnit.MICROS
            ColType.TIMESTAMP_NS -> LogicalTypeAnnotation.TimeUnit.NANOS
            else -> null
        }

    /**
     * Whether the rewriter's refusal of [col] is the DOCUMENTED
     * non-native-unit deferral (AGENT.md, iceberg-federation.md §2.8)
     * rather than a disagreement with the reader.
     *
     * The two surfaces really do differ here, and on purpose. A bound is
     * eight bytes of metadata, so the reader converts a millis or nanos
     * footer bound into the micros an Iceberg `timestamp` bound is
     * defined to be — exact, and the only way a bound can be correct at
     * all. A REWRITE would have to convert every value of every row,
     * which is a data rewrite the compactor deliberately does not do:
     * no legal promotion produces a unit mismatch (PROMOTIONS has no
     * timestamp rungs), so the only way to reach one is a writer
     * disagreeing with its own DDL, and refusing that is the job.
     */
    private fun unitDeferral(
        col: Column,
        schema: GroupType,
    ): Boolean {
        // Over the whole catalog SUBTREE, not just the top-level column:
        // the mismatch is as likely to sit on a map key four levels down
        // as on the column itself, and a list's own type has no unit at
        // all.
        val want = col.selfAndDescendants().mapNotNull { nativeUnit(it.def.type) }.toSet()
        if (want.isEmpty()) return false

        fun walk(g: GroupType): Boolean {
            for (f in g.fields) {
                if (f.isPrimitive) {
                    val unit =
                        when (val a = f.logicalTypeAnnotation) {
                            is LogicalTypeAnnotation.TimestampLogicalTypeAnnotation -> a.unit
                            is LogicalTypeAnnotation.TimeLogicalTypeAnnotation -> a.unit
                            else -> null
                        }
                    if (unit != null && unit !in want) return true
                } else if (walk(f.asGroupType())) {
                    return true
                }
            }
            return false
        }
        return walk(schema)
    }

    /**
     * Whether the rewriter's refusal of [col] is the DOCUMENTED decimal
     * deferral: a source annotation whose scale differs from the live
     * column's, or whose declared precision exceeds it.
     *
     * The surfaces differ here for the same reason as the unit case. A
     * decimal BOUND is the unscaled value, so it depends on the scale
     * and not on the precision — the reader can encode one correctly
     * from a wider-precision file. A REWRITE would be writing values
     * from a domain the destination column does not have, so it refuses
     * the shape instead. Only a writer disagreeing with its own DDL
     * produces the pairing.
     */
    private fun decimalDeferral(
        col: Column,
        schema: GroupType,
    ): Boolean {
        val decimals = col.selfAndDescendants().filter { it.def.type == ColType.DECIMAL }
        if (decimals.isEmpty()) return false
        val scales = decimals.map { (it.def.typeParams?.get("scale") as? Number)?.toInt() ?: 0 }.toSet()
        val maxPrecision =
            decimals.maxOf { (it.def.typeParams?.get("precision") as? Number)?.toInt() ?: 0 }

        fun walk(g: GroupType): Boolean {
            for (f in g.fields) {
                if (f.isPrimitive) {
                    val a = f.logicalTypeAnnotation as? LogicalTypeAnnotation.DecimalLogicalTypeAnnotation
                    if (a != null && (a.scale !in scales || a.precision > maxPrecision)) return true
                } else if (walk(f.asGroupType())) {
                    return true
                }
            }
            return false
        }
        return walk(schema)
    }

    /**
     * Sibling names repeated at any level of [group] — the shape the
     * rewriter refuses (parquet addresses a group's fields by name, so
     * a duplicate makes every write-side lookup a guess) and the reader
     * tolerates (it binds by id).
     */
    private fun duplicateSiblingNames(group: GroupType): Set<String> {
        val out = HashSet<String>()

        fun walk(g: GroupType) {
            out += g.fields.groupingBy { it.name }.eachCount().filterValues { it > 1 }.keys
            for (f in g.fields) if (!f.isPrimitive) walk(f.asGroupType())
        }
        walk(group)
        return out
    }

    private fun maybeSort(
        e: Entropy,
        col: Column,
    ): List<SortFieldDef> {
        if (e.int(0, 9) >= 3) return emptyList()
        // Only sortable sources: a top-level scalar or a scalar struct leaf.
        val candidates = ArrayList<Long>()

        fun walk(
            c: Column,
            underRepeated: Boolean,
        ) {
            if (!c.def.type.isNested) {
                if (!underRepeated) candidates.add(c.fieldId)
                return
            }
            val rep = c.def.type == ColType.LIST || c.def.type == ColType.MAP
            for (k in c.children) walk(k, underRepeated || rep)
        }
        walk(col, false)
        if (candidates.isEmpty()) return emptyList()
        return listOf(
            SortFieldDef(
                candidates[e.int(0, candidates.size - 1)],
                if (e.bool()) SortDirection.ASC else SortDirection.DESC,
                if (e.bool()) NullOrder.NULLS_FIRST else NullOrder.NULLS_LAST,
            ),
        )
    }

    private fun checkBounds(
        agg: FooterStats.ColumnAgg,
        catalog: List<CatalogColumn>,
        sink: (Finding) -> Unit,
        schema: Any,
    ) {
        val lo = agg.lowerBound
        val hi = agg.upperBound
        if (lo == null && hi == null) return
        if (lo == null || hi == null) {
            sink(Finding("half-bound", "field ${agg.fieldId} lo=${lo?.size} hi=${hi?.size}"))
            return
        }
        val col = catalogOf(catalog, agg.fieldId) ?: return
        // string/json/uuid/binary bounds ARE their bytes; a pruner compares
        // them unsigned-lexicographically, so that is what the invariant
        // has to be checked on. (Decoding a string bound through a JVM
        // String is lossy for non-UTF-8 bytes and would invert spuriously.)
        if (col.type == ColType.STRING || col.type == ColType.JSON ||
            col.type == ColType.UUID_T || col.type == ColType.BINARY
        ) {
            if (java.util.Arrays.compareUnsigned(lo, hi) > 0) {
                sink(
                    Finding(
                        "bound-inverted",
                        "field ${agg.fieldId} type=${col.type.wire} lo=${lo.toHex()} > hi=${hi.toHex()} schema=$schema",
                    ),
                )
            }
            // A non-UTF-8 string bound is LEGAL — the encoding is bytes,
            // and a mislabelled file produces one. What must hold is that
            // it survives the codec: compaction's bounds merge is
            // decode -> compare -> encode, so a lossy decode rewrites a
            // file's bound during a rewrite. Check the property, not the
            // proxy: this oracle used to flag every non-UTF-8 bound on
            // the premise that decode mangled it, which it did until
            // decode started returning the raw bytes for exactly these.
            if (col.type == ColType.STRING || col.type == ColType.JSON) {
                for (bound in listOf(lo, hi)) {
                    val through =
                        IcebergSingleValue.encode(col.type, IcebergSingleValue.decode(col.type, bound))
                    if (!through.contentEquals(bound)) {
                        sink(
                            Finding(
                                "string-bound-not-round-trippable",
                                "field ${agg.fieldId} type=${col.type.wire} in=${bound.toHex()} " +
                                    "out=${through.toHex()}; the codec changed a bound's bytes",
                            ),
                        )
                    }
                }
            }
            if (col.type == ColType.UUID_T && (lo.size != 16 || hi.size != 16)) {
                sink(Finding("bound-width", "uuid field ${agg.fieldId} lo=${lo.size}B hi=${hi.size}B"))
            }
            return
        }
        val decoded =
            try {
                IcebergSingleValue.decode(col.type, lo) to IcebergSingleValue.decode(col.type, hi)
            } catch (t: Throwable) {
                sink(
                    Finding(
                        "bound-not-decodable",
                        "field ${agg.fieldId} type=${col.type.wire} lo=${lo.toHex()} hi=${hi.toHex()} schema=$schema",
                        t,
                    ),
                )
                return
            }
        val c =
            try {
                IcebergSingleValue.compareValues(col.type, decoded.first, decoded.second)
            } catch (t: Throwable) {
                sink(Finding("bound-not-comparable", "field ${agg.fieldId} type=${col.type.wire}", t))
                return
            }
        val nan =
            (decoded.first as? Double)?.isNaN() == true || (decoded.second as? Double)?.isNaN() == true ||
                (decoded.first as? Float)?.isNaN() == true || (decoded.second as? Float)?.isNaN() == true
        if (c > 0 && !nan) {
            sink(
                Finding(
                    "bound-inverted",
                    "field ${agg.fieldId} type=${col.type.wire} lower=${decoded.first} > upper=${decoded.second} " +
                        "(lo=${lo.toHex()} hi=${hi.toHex()}) schema=$schema",
                ),
            )
        }
        if (agg.valueCount < 0 || agg.nullCount < 0 || agg.nullCount > agg.valueCount) {
            sink(
                Finding(
                    "count-invariant",
                    "field ${agg.fieldId} valueCount=${agg.valueCount} nullCount=${agg.nullCount}",
                ),
            )
        }
    }

    private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it) }

    private fun isUtf8(b: ByteArray): Boolean =
        try {
            java.nio.charset.StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
                .decode(java.nio.ByteBuffer.wrap(b))
            true
        } catch (_: java.nio.charset.CharacterCodingException) {
            false
        }

    fun catalogOf(
        cols: List<CatalogColumn>,
        fieldId: Long,
    ): CatalogColumn? {
        for (c in cols) {
            if (c.fieldId == fieldId) return c
            catalogOf(c.children, fieldId)?.let { return it }
        }
        return null
    }

    private fun leafIdsUnder(col: CatalogColumn): Set<Long> = leafIds(listOf(col))

    private fun leafIds(cols: List<CatalogColumn>): Set<Long> =
        buildSet {
            fun walk(c: CatalogColumn) {
                if (c.children.isEmpty() && !c.type.isNested) add(c.fieldId) else c.children.forEach { walk(it) }
            }
            cols.forEach { walk(it) }
        }

    private fun allNodeIds(cols: List<CatalogColumn>): Set<Long> =
        buildSet {
            fun walk(c: CatalogColumn) {
                add(c.fieldId)
                c.children.forEach { walk(it) }
            }
            cols.forEach { walk(it) }
        }

    fun fileOf(p: Path): InputFile = LocalInputFile(p)

    fun freshTmp(): Path = Files.createTempDirectory("hoglake-nested-fuzz")

    private fun unused(s: SeekableInputStream) = s
}
