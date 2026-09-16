package com.posthog.hoglake.fuzz

import com.fasterxml.jackson.core.JacksonException
import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.api.CreateTableRequestDto
import com.posthog.hoglake.hydrator.FooterParse
import com.posthog.hoglake.hydrator.FooterStats
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.assignFieldIds
import com.posthog.hoglake.service.ColumnTrees
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.service.TableCreationDefinitionCodec
import com.posthog.hoglake.wireObjectMapper
import io.ktor.server.plugins.BadRequestException
import org.apache.parquet.io.InputFile
import org.apache.parquet.io.SeekableInputStream
import java.io.EOFException
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * Deterministic seed-loop soak runner for the phase-2 nested campaigns.
 *
 * `./gradlew nestedSoak -Pcampaign=agreement -Pseeds=0..100000 -PtimeBudget=3600`
 *
 * Findings are printed with their seed, so every one replays with
 * `-Pseeds=<seed>..<seed>`.
 */
object NestedFuzzSoak {
    private val mapper = wireObjectMapper()

    /** How often a worker rewrites its findings.txt snapshot. */
    private val REPORT_EVERY_NANOS = TimeUnit.SECONDS.toNanos(120)

    @JvmStatic
    fun main(args: Array<String>) {
        val campaign = args.getOrElse(0) { "agreement" }
        val from = args.getOrElse(1) { "0" }.toLong()
        val to = args.getOrElse(2) { "10000" }.toLong()
        val budgetSeconds = args.getOrElse(3) { "600" }.toLong()
        val perIterationTimeout = args.getOrElse(4) { "30" }.toLong()
        NestedFuzz.strictDomains = args.getOrElse(5) { "true" }.toBoolean()

        val tmp = Files.createTempDirectory("hoglake-soak-$campaign")
        val started = System.nanoTime()
        val deadline = started + TimeUnit.SECONDS.toNanos(budgetSeconds)
        val counts = LinkedHashMap<String, Int>()
        val firstSeed = LinkedHashMap<String, Long>()
        val examples = LinkedHashMap<String, String>()
        var executions = 0L
        var slowest = 0L
        var slowestSeed = -1L
        var lastReport = System.nanoTime()
        val exec = Executors.newSingleThreadExecutor { r -> Thread(r).also { it.isDaemon = true } }

        var seed = from
        while (seed < to && System.nanoTime() < deadline) {
            val s = seed
            val found = ArrayList<Finding>()
            val t0 = System.nanoTime()
            val fut =
                exec.submit {
                    val e = RandomEntropy(s)
                    when (campaign) {
                        "agreement" -> NestedAgreement.runOne(e, tmp) { found.add(it) }
                        "footer" -> footerCampaign(e, tmp) { found.add(it) }
                        "data" -> dataCampaign(e, tmp) { found.add(it) }
                        "trees" -> treeCampaign(e) { found.add(it) }
                        "codec" -> codecCampaign(e) { found.add(it) }
                        else -> error("unknown campaign $campaign")
                    }
                }
            try {
                fut.get(perIterationTimeout, TimeUnit.SECONDS)
            } catch (t: java.util.concurrent.TimeoutException) {
                found.add(Finding("HANG", "seed $s exceeded ${perIterationTimeout}s in campaign $campaign"))
                println("HANG at seed $s — aborting campaign (a hang is a finding)")
                report(campaign, executions, started, counts, firstSeed, examples, slowest, slowestSeed, found, s)
                return
            } catch (t: java.util.concurrent.ExecutionException) {
                found.add(Finding("harness-escape", "seed $s", t.cause ?: t))
            }
            val dt = System.nanoTime() - t0
            if (dt > slowest) {
                slowest = dt
                slowestSeed = s
            }
            executions++
            for (f in found) {
                val key = if (f.frame.isEmpty()) f.kind else "${f.kind}|${f.frame}"
                counts[key] = (counts[key] ?: 0) + 1
                if (key !in firstSeed) {
                    firstSeed[key] = s
                    examples[key] =
                        "seed=$s " + f.detail.take(1800) +
                        (
                            f.cause?.let { c ->
                                "\n    CAUSE: " +
                                    java.io.StringWriter().also { w ->
                                        c.printStackTrace(java.io.PrintWriter(w))
                                    }.toString().lines().take(18).joinToString("\n    ")
                            } ?: ""
                        )
                }
            }
            if (executions % 2000 == 0L) {
                System.err.println("  ... $executions execs, seed $s, ${counts.size} distinct findings")
            }
            // Periodic FULL report to findings.txt so a long-running
            // worker is never a black box: the file is rewritten whole,
            // so the collector always reads a consistent snapshot.
            if (System.nanoTime() - lastReport > REPORT_EVERY_NANOS) {
                lastReport = System.nanoTime()
                val snapshot = java.io.File("findings.txt.tmp")
                val out = java.io.PrintStream(snapshot.outputStream(), true)
                val saved = System.out
                System.setOut(out)
                report(campaign, executions, started, counts, firstSeed, examples, slowest, slowestSeed, emptyList(), s)
                System.setOut(saved)
                out.close()
                snapshot.renameTo(java.io.File("findings.txt"))
            }
            seed++
        }
        report(campaign, executions, started, counts, firstSeed, examples, slowest, slowestSeed, emptyList(), seed)
    }

    @Suppress("LongParameterList")
    private fun report(
        campaign: String,
        executions: Long,
        started: Long,
        counts: Map<String, Int>,
        firstSeed: Map<String, Long>,
        examples: Map<String, String>,
        slowest: Long,
        slowestSeed: Long,
        tail: List<Finding>,
        lastSeed: Long,
    ) {
        val wall = (System.nanoTime() - started) / 1_000_000_000.0
        println("=== CAMPAIGN $campaign ===")
        println("executions=$executions wall=${"%.1f".format(wall)}s lastSeed=$lastSeed")
        println("slowest iteration=${"%.2f".format(slowest / 1e9)}s at seed $slowestSeed")
        println("distinct findings=${counts.size}")
        for ((k, v) in counts.entries.sortedByDescending { it.value }) {
            println("--- $k  hits=$v firstSeed=${firstSeed[k]}")
            println("    ${examples[k]}")
        }
        for (f in tail) println("TAIL $f")
    }

    // ---- campaign 2: footer mutation over NESTED footers ------------------

    private fun footerCampaign(
        e: Entropy,
        tmp: Path,
        sink: (Finding) -> Unit,
    ) {
        val defs = NestedFuzz.genDefs(e, e.int(1, 3), e.int(2, NestedFuzz.maxDepth()))
        try {
            ColumnTrees.validate(defs)
        } catch (t: Throwable) {
            return
        }
        val live = assignFieldIds(defs, 1L)
        val catalog = NestedFuzz.toCatalogColumns(live)
        val derived = NestedFuzz.deriveSchema(e, live, e.int(0, 25))
        val src = tmp.resolve("footer.parquet")
        try {
            NestedFuzz.writeFile(e, derived.schema, src, e.int(1, 4), e.int(0, 3))
        } catch (t: Throwable) {
            return
        }
        val bytes = Files.readAllBytes(src)
        // Structured + bit-level mutation of a REAL nested footer.
        val mutated = bytes.copyOf()
        val flips = e.int(1, 12)
        repeat(flips) {
            // Mutate inside the footer region (last ~4 KiB), where the
            // thrift schema tree lives, not in the data pages.
            val lo = maxOf(4, mutated.size - minOf(mutated.size - 8, 4096))
            val at = e.int(lo, mutated.size - 9)
            mutated[at] =
                when (e.int(0, 3)) {
                    0 -> (mutated[at].toInt() xor (1 shl e.int(0, 7))).toByte()
                    1 -> e.bytes(1)[0]
                    2 -> 0
                    else -> -1
                }
        }
        val footer =
            try {
                FooterParse.parse(BytesFile(mutated))
            } catch (t: Throwable) {
                if (t !is IOException) sink(Finding("footer-parse-untyped", "mutations=$flips", t))
                return
            }
        try {
            val schema = footer.fileMetaData.schema
            FooterStats.missingFieldIds(schema)
            FooterStats.usesFieldIds(schema)
            val aggs = FooterStats.aggregate(footer, catalog, "fuzz://mutated")
            val leafIds = leafIdsOf(catalog)
            for (a in aggs) {
                if (a.fieldId !in leafIds) {
                    sink(Finding("stats-for-unowned-field", "field ${a.fieldId} not a catalog leaf; schema=$schema"))
                }
                if (a.nullCount > a.valueCount || a.valueCount < 0 || a.nullCount < 0) {
                    sink(Finding("count-invariant", "field ${a.fieldId} v=${a.valueCount} n=${a.nullCount}"))
                }
                val lo = a.lowerBound
                val hi = a.upperBound
                if (lo != null && hi != null) {
                    val col = NestedAgreement.catalogOf(catalog, a.fieldId)!!
                    try {
                        val d1 = com.posthog.hoglake.stats.IcebergSingleValue.decode(col.type, lo)
                        val d2 = com.posthog.hoglake.stats.IcebergSingleValue.decode(col.type, hi)
                        val nan =
                            (d1 as? Double)?.isNaN() == true || (d2 as? Double)?.isNaN() == true ||
                                (d1 as? Float)?.isNaN() == true || (d2 as? Float)?.isNaN() == true
                        if (!nan &&
                            com.posthog.hoglake.stats.IcebergSingleValue.compareValues(col.type, d1, d2) > 0
                        ) {
                            sink(
                                Finding(
                                    "bound-inverted",
                                    "field ${a.fieldId} type=${col.type.wire} lo=$d1 hi=$d2 schema=$schema",
                                ),
                            )
                        }
                    } catch (t: Throwable) {
                        sink(Finding("bound-not-decodable", "field ${a.fieldId} type=${col.type.wire}", t))
                    }
                } else if ((lo == null) != (hi == null)) {
                    sink(Finding("half-bound", "field ${a.fieldId}"))
                }
            }
        } catch (t: Throwable) {
            sink(Finding("reader-raw-throw", "mutated nested footer", t))
        }
    }

    private fun leafIdsOf(cols: List<com.posthog.hoglake.hydrator.CatalogColumn>): Set<Long> =
        buildSet {
            fun walk(c: com.posthog.hoglake.hydrator.CatalogColumn) {
                if (!c.type.isNested) add(c.fieldId) else c.children.forEach { walk(it) }
            }
            cols.forEach { walk(it) }
        }

    // ---- campaign 3: valid schema pairs, hostile VALUES --------------------

    private fun dataCampaign(
        e: Entropy,
        tmp: Path,
        sink: (Finding) -> Unit,
    ) {
        val defs = NestedFuzz.genDefs(e, e.int(1, 3), e.int(1, 5))
        try {
            ColumnTrees.validate(defs)
        } catch (t: Throwable) {
            return
        }
        val live = assignFieldIds(defs, 1L)
        val catalog = NestedFuzz.toCatalogColumns(live)
        // CANONICAL schema only: the rewriter must accept its own shape.
        val derived = NestedFuzz.deriveSchema(e, live, 0)
        val src = tmp.resolve("data.parquet")
        val rows = e.int(1, 8)
        val maxRep = if (e.int(0, 9) < 2) e.int(50, 400) else e.int(0, 5)
        try {
            NestedFuzz.writeFile(e, derived.schema, src, rows, maxRep)
        } catch (t: Throwable) {
            sink(Finding("write-canonical-failed", "schema=${derived.schema}", t))
            return
        }
        val out = tmp.resolve("data-out.parquet")
        val result =
            try {
                ParquetRewriterCall.rewrite(src, live, out, e)
            } catch (t: com.posthog.hoglake.compaction.UnconvertibleSchemaException) {
                sink(
                    Finding(
                        "canonical-schema-refused",
                        "rewriter refused its own canonical shape: ${t.message} schema=${derived.schema}",
                        t,
                    ),
                )
                return
            } catch (t: Throwable) {
                sink(Finding("rewriter-raw-throw", "canonical schema=${derived.schema}", t))
                return
            }
        if (result.rowsWritten != rows.toLong()) {
            sink(Finding("row-count-loss", "wrote=${result.rowsWritten} expected=$rows"))
        }
        val inLeaves = NestedFuzz.leafStats(src).second.filter { it.fieldId != null }.associateBy { it.fieldId!! }
        val outLeaves = NestedFuzz.leafStats(out).second
        for (l in outLeaves) {
            val fid = l.fieldId ?: continue
            if (fid == 2147483646) continue
            val srcLeaf = inLeaves[fid] ?: continue
            if (srcLeaf.nullCount == null || l.nullCount == null) continue
            val a = srcLeaf.valueCount - srcLeaf.nullCount
            val b = l.valueCount - l.nullCount
            if (a != b) {
                sink(
                    Finding(
                        "conservation-loss",
                        "field $fid input non-null=$a output non-null=$b schema=${derived.schema}",
                    ),
                )
            }
        }
        try {
            val f = FooterParse.parse(BytesFile(Files.readAllBytes(out)))
            FooterStats.aggregate(f, catalog, out.toString())
        } catch (t: Throwable) {
            sink(Finding("roundtrip-reader-throw", "rewrite output unreadable", t))
        }
    }

    // ---- campaign 4: ColumnTrees.validate + wire DTO over hostile trees ----

    private fun treeCampaign(
        e: Entropy,
        sink: (Finding) -> Unit,
    ) {
        val defs = hostileDefs(e, e.int(1, 3), e.int(1, 12))
        try {
            ColumnTrees.validate(defs)
        } catch (t: HoglakeException.Validation) {
            // The only sanctioned refusal.
        } catch (t: Throwable) {
            sink(Finding("validate-untyped-refusal", "defs=${defs.toString().take(500)}", t))
        }

        // Depth bomb through the model helper the cap itself calls.
        if (e.int(0, 99) < 5) {
            val bomb = deepChain(e.int(1000, 40000))
            try {
                ColumnTrees.validate(listOf(bomb))
            } catch (t: HoglakeException.Validation) {
                // fine
            } catch (t: Throwable) {
                sink(Finding("validate-depth-bomb", "chain depth bomb", t))
            }
        }

        // Wire JSON: recursive children, unicode names, null-in-list.
        val json = hostileJson(e, 0)
        val body = """{"name":"t","columns":$json}"""
        val dto =
            try {
                mapper.readValue<CreateTableRequestDto>(body)
            } catch (t: Exception) {
                null
            }
        if (dto != null) {
            try {
                val models = dto.columns.map { it.toModel() }
                ColumnTrees.validate(models)
            } catch (t: JacksonException) {
                // mapped
            } catch (t: BadRequestException) {
                // mapped
            } catch (t: HoglakeException) {
                // mapped
            } catch (t: Throwable) {
                sink(Finding("wire-unmapped-throw", "body=${body.take(400)}", t))
            }
        }
    }

    private fun deepChain(depth: Int): ColumnDef {
        var d = ColumnDef("element", ColType.LONG)
        repeat(depth) { d = ColumnDef(if (it == depth - 1) "c0" else "element", ColType.LIST, null, true, listOf(d)) }
        return d
    }

    private fun hostileDefs(
        e: Entropy,
        count: Int,
        depthLeft: Int,
    ): List<ColumnDef> = (0 until count).map { hostileDef(e, depthLeft, "c$it") }

    private fun hostileDef(
        e: Entropy,
        depthLeft: Int,
        name: String,
    ): ColumnDef {
        val t = ColType.entries[e.int(0, ColType.entries.size - 1)]
        val n = if (e.int(0, 9) < 2) hostileName(e) else name
        val params =
            when (e.int(0, 5)) {
                0 -> mapOf("precision" to e.int(-5, 60), "scale" to e.int(-5, 60))
                1 -> emptyMap()
                2 -> mapOf("precision" to "x", "scale" to null)
                else -> null
            }
        val kids: List<ColumnDef>? =
            when (e.int(0, 5)) {
                0 -> null
                1 -> emptyList()
                else ->
                    if (depthLeft <= 1) {
                        emptyList()
                    } else {
                        (0 until e.int(1, 3)).map {
                            hostileDef(
                                e,
                                depthLeft - 1,
                                listOf("element", "key", "value", "f$it")[e.int(0, 3)],
                            )
                        }
                    }
            }
        return ColumnDef(n, t, params, e.bool(), kids)
    }

    private fun hostileName(e: Entropy): String =
        listOf("", " ", " ", "a".repeat(300), "élement", "💩", "ELEMENT", "key ", "_hog_row_id", "1")[
            e.int(0, 9),
        ]

    private fun hostileJson(
        e: Entropy,
        depth: Int,
    ): String {
        if (depth > 14) return "[]"
        val n = e.int(0, 3)
        val items =
            (0 until n).joinToString(",") {
                if (e.int(0, 19) == 0) {
                    "null"
                } else {
                    val type =
                        listOf("list", "struct", "map", "long", "string", "decimal", "nope", "", "LIST")[e.int(0, 8)]
                    val kids = if (e.int(0, 9) < 6) ""","children":${hostileJson(e, depth + 1)}""" else ""
                    val tp = if (e.int(0, 9) < 3) ""","type_params":{"precision":${e.int(-2, 60)}}""" else ""
                    """{"name":"${jsonName(e, it)}","type":"$type"$tp$kids}"""
                }
            }
        return "[$items]"
    }

    private fun jsonName(
        e: Entropy,
        i: Int,
    ): String =
        when (e.int(0, 6)) {
            0 -> "element"
            1 -> "key"
            2 -> "value"
            3 -> "\\u00e9l"
            4 -> ""
            else -> "f$i"
        }

    // ---- campaign 5: creation-receipt codec round trip ---------------------

    private fun codecCampaign(
        e: Entropy,
        sink: (Finding) -> Unit,
    ) {
        val defs = NestedFuzz.genDefs(e, e.int(1, 4), e.int(1, NestedFuzz.maxDepth()))
        val def = TableCreationDefinition("ns${e.int(0, 9)}", "t${e.int(0, 9)}", defs)
        val encoded =
            try {
                TableCreationDefinitionCodec.encode(def)
            } catch (t: Throwable) {
                sink(Finding("codec-encode-throw", "def=${def.columns}", t))
                return
            }
        val version = mapper.readTree(encoded)["version"].asInt()
        val anyChildren = defs.any { hasChildren(it) }
        val expected = if (anyChildren) 2 else 1
        if (version != expected) {
            sink(Finding("codec-version-selection", "version=$version expected=$expected encoded=$encoded"))
        }
        val decoded =
            try {
                TableCreationDefinitionCodec.decode(encoded)
            } catch (t: Throwable) {
                sink(Finding("codec-decode-throw", "encoded=${encoded.take(600)}", t))
                return
            }
        if (decoded != def) {
            sink(Finding("codec-roundtrip-inequality", "in=${def.columns}\n     out=${decoded.columns}"))
        }
        val re = TableCreationDefinitionCodec.encode(decoded)
        if (re != encoded) {
            sink(Finding("codec-not-fixpoint", "first=${encoded.take(500)}\n     second=${re.take(500)}"))
        }

        // Hostile blobs: typed failure or success, never a raw escape.
        val blob = hostileBlob(e, encoded)
        try {
            TableCreationDefinitionCodec.decode(blob)
        } catch (t: IllegalStateException) {
            // the codec's own version check
        } catch (t: JacksonException) {
            // parse failure
        } catch (t: HoglakeException) {
            // typed
        } catch (t: Throwable) {
            sink(Finding("codec-decode-raw-throw", "blob=${blob.take(400)}", t))
        }
    }

    /** The codec's own `anyChildren` predicate, restated. */
    private fun hasChildren(d: ColumnDef): Boolean = d.children != null

    private fun hostileBlob(
        e: Entropy,
        valid: String,
    ): String =
        when (e.int(0, 9)) {
            0 -> "{}"
            1 -> """{"version":9,"namespace":"a","name":"b","columns":[]}"""
            2 -> """{"version":2,"name":"b","columns":[]}"""
            3 -> """{"version":2,"namespace":"a","name":"b"}"""
            4 -> """{"version":2,"namespace":"a","name":"b","columns":[{"type":"long"}]}"""
            5 -> """{"version":2,"namespace":"a","name":"b","columns":[{"name":"x"}]}"""
            6 -> """{"version":"two","namespace":"a","name":"b","columns":[]}"""
            7 -> """{"version":2,"namespace":"a","name":"b","columns":{"x":1}}"""
            8 -> "[1,2,3]"
            else -> mutateJson(e, valid)
        }

    private fun mutateJson(
        e: Entropy,
        s: String,
    ): String {
        if (s.length < 4) return s
        val b = StringBuilder(s)
        repeat(e.int(1, 4)) {
            val at = e.int(0, b.length - 1)
            b.setCharAt(at, "{}[]\",:0nulltrue"[e.int(0, 15)])
        }
        return b.toString()
    }

    /** Minimal in-memory InputFile (the ParquetFooterFuzzTest pattern). */
    class BytesFile(private val bytes: ByteArray) : InputFile {
        override fun getLength(): Long = bytes.size.toLong()

        override fun newStream(): SeekableInputStream =
            object : SeekableInputStream() {
                private var pos = 0L

                override fun getPos(): Long = pos

                override fun seek(p: Long) {
                    pos = p
                }

                override fun read(): Int {
                    if (pos >= bytes.size) return -1
                    return (bytes[pos.toInt()].toInt() and 0xFF).also { pos += 1 }
                }

                override fun read(
                    b: ByteArray,
                    off: Int,
                    len: Int,
                ): Int {
                    if (len == 0) return 0
                    if (pos >= bytes.size) return -1
                    val n = minOf(len.toLong(), bytes.size - pos).toInt()
                    System.arraycopy(bytes, pos.toInt(), b, off, n)
                    pos += n
                    return n
                }

                override fun readFully(b: ByteArray) = readFully(b, 0, b.size)

                override fun readFully(
                    b: ByteArray,
                    start: Int,
                    len: Int,
                ) {
                    if (pos + len > bytes.size) throw EOFException("past end")
                    System.arraycopy(bytes, pos.toInt(), b, start, len)
                    pos += len
                }

                override fun read(buf: ByteBuffer): Int {
                    val len = buf.remaining()
                    if (len == 0) return 0
                    if (pos >= bytes.size) return -1
                    val n = minOf(len.toLong(), bytes.size - pos).toInt()
                    buf.put(bytes, pos.toInt(), n)
                    pos += n
                    return n
                }

                override fun readFully(buf: ByteBuffer) {
                    val len = buf.remaining()
                    if (pos + len > bytes.size) throw EOFException("past end")
                    buf.put(bytes, pos.toInt(), len)
                    pos += len
                }
            }
    }
}

/** Small indirection so the data campaign can vary the sort spec. */
object ParquetRewriterCall {
    fun rewrite(
        src: Path,
        live: List<com.posthog.hoglake.model.Column>,
        out: Path,
        e: Entropy,
    ): com.posthog.hoglake.compaction.ParquetRewriter.RewriteResult {
        val sortable = ArrayList<Long>()

        fun walk(
            c: com.posthog.hoglake.model.Column,
            underRepeated: Boolean,
        ) {
            if (!c.def.type.isNested) {
                if (!underRepeated) sortable.add(c.fieldId)
                return
            }
            val rep = c.def.type == ColType.LIST || c.def.type == ColType.MAP
            for (k in c.children) walk(k, underRepeated || rep)
        }
        live.forEach { walk(it, false) }
        val sort =
            if (sortable.isNotEmpty() && e.int(0, 9) < 4) {
                listOf(
                    com.posthog.hoglake.model.SortFieldDef(
                        sortable[e.int(0, sortable.size - 1)],
                        if (e.bool()) {
                            com.posthog.hoglake.model.SortDirection.ASC
                        } else {
                            com.posthog.hoglake.model.SortDirection.DESC
                        },
                        if (e.bool()) {
                            com.posthog.hoglake.model.NullOrder.NULLS_FIRST
                        } else {
                            com.posthog.hoglake.model.NullOrder.NULLS_LAST
                        },
                    ),
                )
            } else {
                emptyList()
            }
        return com.posthog.hoglake.compaction.ParquetRewriter.rewrite(
            listOf(com.posthog.hoglake.compaction.ParquetRewriter.Input(src, 0L, null)),
            live,
            sort,
            out,
        )
    }
}
