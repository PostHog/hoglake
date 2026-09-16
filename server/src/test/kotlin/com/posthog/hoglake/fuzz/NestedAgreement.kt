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
                if (!dataRefused && wanted.isNotEmpty() && bounded.containsAll(wanted)) {
                    sink(
                        Finding(
                            "agreement-reader-bounded-writer-refused",
                            "rewriter refused '${col.def.name}' (${col.def.type.wire}) but the reader " +
                                "produced BOUNDS for every leaf under it ($wanted).\n    liveColumn=$col" +
                                "\n    inSchema=$schema",
                        ),
                    )
                }
                if (canonical && !dataRefused && duplicates.isEmpty()) {
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

                // CONSERVATION: bound by a unique id on the input side.
                val srcLeaf = inById[fid]?.singleOrNull()
                if (srcLeaf != null && duplicates.isEmpty() && srcLeaf.nullCount != null) {
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
            if ((col.type == ColType.STRING || col.type == ColType.JSON) && (!isUtf8(lo) || !isUtf8(hi))) {
                sink(
                    Finding(
                        "non-utf8-string-bound",
                        "field ${agg.fieldId} type=${col.type.wire} lo=${lo.toHex()} hi=${hi.toHex()}; " +
                            "IcebergSingleValue.decode(${col.type.wire}, .) round-trips through a JVM String " +
                            "and mangles these bytes",
                    ),
                )
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
