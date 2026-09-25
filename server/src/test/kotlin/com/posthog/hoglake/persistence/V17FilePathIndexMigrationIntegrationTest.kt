package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.service.CleanupService
import com.posthog.hoglake.service.RemovalStore
import com.posthog.hoglake.service.UploadService
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.TestImages
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.MinIOContainer
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList

/**
 * V17 against the statement it exists for: the cleanup drain's
 * liveness check (invariant 4), which runs once per sub-batch while it
 * holds the per-catalog commit lock.
 *
 * The migration RUNS here — the database arrives at V16, the rows are
 * seeded, and then [Database.migrate] applies V17 — so the before half
 * is a MEASUREMENT on the same rows rather than a claim about a file
 * that is not there. That is V16's shape and AGENT.md's rule; what this
 * class adds is where the statement comes from.
 *
 * THE STATEMENT IS CAPTURED, NOT RESTATED. `CleanupService`'s reference
 * check is private and inline, so a test that retyped its SQL would
 * EXPLAIN a lookalike and stay green through any change to the real
 * one — V14's first index was proven against a predicate no code path
 * issues. Instead a REAL drain runs against a real object store, with a
 * [SqlLogger] on the Jdbi, and the three-table statement it issues is
 * the text every plan below is taken from. Move the check into Kotlin,
 * or drop a leg of the UNION, and the capture fails rather than the
 * assertions passing over a statement nobody sends.
 *
 * The same capture serves the OTHER hot statement these indexes reach:
 * `UploadService.register`'s retained-path probe, which UNIONs the same
 * two tables by path on the FOREGROUND commit path — every commit,
 * inside the commit transaction, under the same lock.
 *
 * What the fixture has to get right:
 *
 *  - **A SECOND CATALOG, interleaved.** `catalog_id` leads both new
 *    indexes; with one catalog "this catalog's files" and "every file"
 *    are the same rows and a plan that ignores the column measures the
 *    same as one that honours it.
 *  - **ENDED AND LIVE ROWS MIXED.** The check covers "any file row,
 *    live or not" (its own KDoc), which is why V17's index is not
 *    partial. A fixture of live rows only would let a partial index
 *    pass this test and delete a retained snapshot's objects in
 *    production.
 *  - **SCATTERED 175-BYTE PATHS.** A commit's output paths share no
 *    prefix — writers hash-spread object keys — and a probe set that
 *    shares one lets the btree's array scan descend twice and walk,
 *    which made V16's first measurement four times kinder than the
 *    statement it modelled.
 *  - **A PROBE OF [CleanupService.SUB_BATCH] PATHS**, read from the
 *    production constant rather than written down: the sub-batch size
 *    IS the probe size, so a change to one moves this test with it.
 *  - **BOTH PATH SHAPES.** A sub-batch drains expiry's data files and
 *    its deletion vectors together, so the probe carries both and each
 *    leg of the UNION does real work.
 *  - **PROBES THAT HIT.** Three probe paths are live file rows, so the
 *    statement returns rows under either plan — and so the drain
 *    reports them as the invariant violation they are.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class V17FilePathIndexMigrationIntegrationTest {
    private companion object {
        const val DATA_INDEX = "hog_data_file_path"
        const val DV_INDEX = "hog_delete_file_path"

        /** V12's `UNIQUE (catalog_id, path)`, which V17 deliberately does not duplicate. */
        const val UPLOAD_INDEX = "hog_upload_catalog_id_path_key"

        /**
         * The manifest. Split between two catalogs, ended and live rows
         * mixed. Production is millions (one dropped table alone held
         * 3M ended rows when #205 shipped); what this fixture needs is
         * a relation whose sequential scan costs enough that the probe
         * is visibly cheaper, which it does at this size — measured
         * 19,231 heap pages against ~3,800 buffers for the probe.
         */
        const val DATA_FILES = 500_000

        const val DELETE_FILES = 50_000

        const val UPLOADS = 20_000

        /**
         * A SMALL table in the same catalog, and the manifest read's
         * subject. The bulk tables hold a quarter of the manifest each,
         * where a sequential scan is the right plan and the comparison
         * below would be two nulls — an assertion that cannot fail. A
         * few hundred files is where `FileRepo.listAt` is index-driven,
         * which is the state a steal would be visible in.
         */
        const val SMALL_TABLE = 5L

        const val SMALL_TABLE_FILES = 200

        /** The probe: exactly one production sub-batch of paths. */
        val PROBE = CleanupService.SUB_BATCH

        /** How many of the probe paths are deletion vectors, as expiry queues them. */
        const val PROBE_DV = 200

        /** Probe paths that really are live file rows: the invariant violation. */
        const val PROBE_DATA_HITS = 2

        const val PROBE_DV_HITS = 1

        /**
         * Probe paths an upload claim holds — one ACTIVE, which IS a
         * reference, and one ABANDONED, which is not. The pair is what
         * makes the third leg's `state = 'active'` load-bearing in the
         * ANSWER: a plan assertion cannot see a predicate, and dropping
         * that one would let the drain delete an object a live writer
         * is about to register.
         */
        const val PROBE_CLAIMED_ACTIVE = 1

        const val PROBE_CLAIMED_ABANDONED = 1

        /**
         * Buffers one probe path may cost in one scan node: a B-tree
         * descent (root, two internal levels, leaf) with slack. The
         * measured cost on this fixture is ~3.8 for the data files;
         * the plan this has to exclude reads the whole relation, which
         * is 19,231 pages — so the budget sits an order of magnitude
         * below the shape it excludes rather than beside it.
         */
        const val BUFFERS_PER_PROBE = 6

        /** Per-statement overhead a single-path probe's budget forgives. */
        const val BUFFER_FLOOR = 64

        /** The owner of the seeded upload claims; `register` fences on it. */
        val OWNER: UUID = UUID.fromString("00000000-0000-4000-8000-00000000d00d")

        /** Put the history back to before V17 (V16's helper, and its reasoning). */
        const val REAPPLY_V17 = "DELETE FROM flyway_schema_history WHERE version::numeric >= 17"

        val minio: MinIOContainer by lazy { TestImages.minio().also { it.start() } }

        /** Read/put side, for the one thing the drain cannot do: make the bucket. */
        val objects: ObjectStore by lazy {
            ObjectStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            )
        }
    }

    // productionSession: V17 builds CONCURRENTLY with statement_timeout
    // lifted for the build and restored after, and a booting pod's
    // session carries the 60 s bound. The fixture reaches that behaviour
    // rather than running on a session with no bounds at all.
    private val db = PgTestSupport.freshDatabaseAt("16", productionSession = true)
    private var catalogId = 0L
    private var otherCatalogId = 0L

    private lateinit var referenceCheckSql: String
    private lateinit var retainedProbeSql: String
    private lateinit var listAtSql: String

    private lateinit var checkPlanBefore: String
    private lateinit var checkPlanAfter: String
    private lateinit var retainedPlanBefore: String
    private lateinit var retainedPlanAfter: String
    private lateinit var listAtPlanBefore: String
    private lateinit var listAtPlanAfter: String
    private var drainStillReferenced = -1L

    @AfterAll
    fun tearDown() = db.close()

    /**
     * The probe set, scattered and of both shapes. The first
     * [PROBE_DATA_HITS] data paths and the first [PROBE_DV_HITS] DV
     * paths are seeded as live file rows; the rest exist nowhere.
     */
    private val probePaths: List<String> by lazy {
        List(PROBE - PROBE_DV) { i ->
            if (i < PROBE_DATA_HITS) dataPath(1, 2L * i + 1) else dataPath(1, 900_001L + 2L * i)
        } +
            List(PROBE_DV) { i ->
                if (i < PROBE_DV_HITS) dvPath(1, 2L * i + 1) else dvPath(1, 900_001L + 2L * i)
            }
    }

    private val hitPaths: List<String> by lazy {
        (0 until PROBE_DATA_HITS).map { dataPath(1, 2L * it + 1) } +
            (0 until PROBE_DV_HITS).map { dvPath(1, 2L * it + 1) } +
            claimedActivePaths
    }

    /** Probe paths an ACTIVE upload claim holds: references, via the third leg. */
    private val claimedActivePaths: List<String> by lazy {
        probePaths.subList(PROBE_DATA_HITS, PROBE_DATA_HITS + PROBE_CLAIMED_ACTIVE)
    }

    /** Probe paths an ABANDONED claim holds: NOT references, and the mutation that proves it. */
    private val claimedAbandonedPaths: List<String> by lazy {
        probePaths.subList(
            PROBE_DATA_HITS + PROBE_CLAIMED_ACTIVE,
            PROBE_DATA_HITS + PROBE_CLAIMED_ACTIVE + PROBE_CLAIMED_ABANDONED,
        )
    }

    /** 1,000 paths that ARE live data files: what a commit's register probes. */
    private val livePaths: List<String> by lazy {
        List(PROBE) { i -> dataPath(1, 2L * i + 1) }
    }

    // ---- fixture -----------------------------------------------------------

    /**
     * A writer's path, the shape production produces: a hash segment
     * that scatters the key space, and 175 bytes of it.
     */
    private fun dataPath(
        catalogSlot: Int,
        g: Long,
    ): String =
        "s3://${bucket()}/c$catalogSlot/main/events_raw/${md5("$g")}/${md5("${g + 1}")}" +
            "/part-${g.toString().padStart(10, '0')}-${md5("${g + 2}")}.parquet"

    private fun dvPath(
        catalogSlot: Int,
        g: Long,
    ): String =
        "s3://${bucket()}/c$catalogSlot/main/events_raw/dv/${md5("dv$g")}/${md5("dv${g + 1}")}" +
            "/del-${g.toString().padStart(10, '0')}.puffin"

    private fun bucket() = "hoglake-v17"

    private fun md5(text: String): String =
        java.security.MessageDigest.getInstance("MD5")
            .digest(text.toByteArray())
            .joinToString("") { "%02x".format(it) }

    @BeforeAll
    fun seedThenMigrate() {
        catalogId = createCatalog("v17")
        otherCatalogId = createCatalog("v17-neighbour")
        seedManifest()
        seedQueue()
        analyze()

        assertThat(indexDef(DATA_INDEX)).describedAs("%s absent before V17", DATA_INDEX).isNull()
        assertThat(indexDef(DV_INDEX)).describedAs("%s absent before V17", DV_INDEX).isNull()

        // The statements, off the services that issue them.
        referenceCheckSql = captureReferenceCheck()
        retainedProbeSql = captureRetainedProbe()
        listAtSql = captureListAt()

        checkPlanBefore = explainReferenceCheck()
        retainedPlanBefore = explainRetainedProbe()
        listAtPlanBefore = explainListAt()

        Database.migrate(db.dataSource)

        // VACUUM and not merely ANALYZE: a bulk insert leaves the
        // visibility map unset, so an Index Only Scan would pay a heap
        // fetch per row and the budget below would be measuring the
        // absence of autovacuum rather than the plan.
        analyze()
        checkPlanAfter = explainReferenceCheck()
        retainedPlanAfter = explainRetainedProbe()
        listAtPlanAfter = explainListAt()
    }

    private fun createCatalog(name: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "INSERT INTO hog_catalog (name, data_path) VALUES (:n, :p) RETURNING catalog_id",
            ).bind("n", name).bind("p", "s3://${bucket()}/$name").mapTo(Long::class.java).one()
        }

    /**
     * Both catalogs' manifests from ONE series, alternating, so neither
     * catalog's rows are clustered in the heap; a quarter of them ended,
     * because the check covers historical rows and the index must too.
     */
    private fun seedManifest() =
        db.jdbi.useHandleUnchecked { h ->
            for (c in listOf(catalogId, otherCatalogId)) {
                h.execute(
                    "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) " +
                        "SELECT ?, g, 0 FROM generate_series(1, 5) g",
                    c,
                )
            }
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           end_snapshot, path, record_count, file_size_bytes,
                                           row_id_start)
                SELECT CASE WHEN g % 10 = 0 THEN :other ELSE :c END, g, 1 + (g % 4), 1,
                       CASE WHEN g % 4 = 0 THEN 2 ELSE NULL END,
                       's3://' || :bucket || '/c' || (CASE WHEN g % 10 = 0 THEN 2 ELSE 1 END)
                         || '/main/events_raw/' || md5(g::text) || '/' || md5((g + 1)::text)
                         || '/part-' || lpad(g::text, 10, '0') || '-' || md5((g + 2)::text)
                         || '.parquet',
                       120000, 268435456, g::bigint * 120000
                FROM generate_series(0, :n) g
                """,
            ).bind("c", catalogId).bind("other", otherCatalogId)
                .bind("bucket", bucket()).bind("n", DATA_FILES).execute()
            h.createUpdate(
                """
                INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id,
                                             begin_snapshot, end_snapshot, path, delete_count,
                                             file_size_bytes)
                SELECT CASE WHEN g % 10 = 0 THEN :other ELSE :c END, g, 1 + (g % 4), g, 1,
                       CASE WHEN g % 4 = 0 THEN 2 ELSE NULL END,
                       's3://' || :bucket || '/c' || (CASE WHEN g % 10 = 0 THEN 2 ELSE 1 END)
                         || '/main/events_raw/dv/' || md5('dv' || g::text) || '/'
                         || md5('dv' || (g + 1)::text) || '/del-' || lpad(g::text, 10, '0')
                         || '.puffin',
                       17, 4096
                FROM generate_series(0, :n) g
                """,
            ).bind("c", catalogId).bind("other", otherCatalogId)
                .bind("bucket", bucket()).bind("n", DELETE_FILES).execute()
            // The small table, seeded after the bulk so its rows sit at
            // the end of the heap exactly as a young table's do.
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start)
                SELECT :c, 800000 + g, :t, 1,
                       's3://' || :bucket || '/c1/main/small/' || md5('small' || g::text) || '/'
                         || md5('small' || (g + 1)::text) || '/part-' || lpad(g::text, 10, '0')
                         || '.parquet',
                       120000, 268435456, (800000 + g)::bigint * 120000
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("t", SMALL_TABLE).bind("bucket", bucket())
                .bind("n", SMALL_TABLE_FILES).execute()
            // Upload claims: the third leg's table. A mixed state
            // vocabulary, because the check reads only the active ones.
            h.createUpdate(
                """
                INSERT INTO hog_upload (catalog_id, upload_id, owner, prefix, path, file_kind, state)
                SELECT :c, gen_random_uuid(), gen_random_uuid(), 's3://' || :bucket,
                       's3://' || :bucket || '/c1/main/events_raw/upload/' || md5('up' || g::text)
                         || '/' || md5('up' || (g + 1)::text) || '/part-'
                         || lpad(g::text, 10, '0') || '.parquet',
                       'data',
                       CASE WHEN g % 3 = 0 THEN 'active'
                            WHEN g % 3 = 1 THEN 'registered' ELSE 'abandoned' END
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("bucket", bucket()).bind("n", UPLOADS).execute()
            // The registered claims `UploadService.register` re-reads on
            // the commit path, over paths that are live data files.
            val claims =
                h.prepareBatch(
                    """
                INSERT INTO hog_upload (catalog_id, upload_id, owner, prefix, path, file_kind, state)
                VALUES (:c, gen_random_uuid(), :owner, :prefix, :path, 'data', 'registered')
                """,
                )
            for (path in livePaths) {
                claims.bind("c", catalogId).bind("owner", OWNER)
                    .bind("prefix", "s3://${bucket()}").bind("path", path).add()
            }
            claims.execute()
            // One probe path claimed ACTIVE (a reference the third leg
            // must return) and one claimed ABANDONED (a claim that owns
            // nothing, which it must not).
            val probeClaims =
                h.prepareBatch(
                    """
                    INSERT INTO hog_upload (catalog_id, upload_id, owner, prefix, path, file_kind, state)
                    VALUES (:c, gen_random_uuid(), gen_random_uuid(), :prefix, :path, 'data', :state)
                    """,
                )
            for (path in claimedActivePaths) {
                probeClaims.bind("c", catalogId).bind("prefix", "s3://${bucket()}")
                    .bind("path", path).bind("state", "active").add()
            }
            for (path in claimedAbandonedPaths) {
                probeClaims.bind("c", catalogId).bind("prefix", "s3://${bucket()}")
                    .bind("path", path).bind("state", "abandoned").add()
            }
            probeClaims.execute()
        }

    /** One sub-batch of queue rows, over the probe paths. */
    private fun seedQueue() =
        db.jdbi.useHandleUnchecked { h ->
            val batch =
                h.prepareBatch(
                    "INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason) " +
                        "VALUES (:c, :path, :kind, 'snapshot_expiry')",
                )
            for (path in probePaths) {
                batch.bind("c", catalogId).bind("path", path)
                    .bind("kind", if (path.endsWith(".puffin")) "delete" else "data").add()
            }
            batch.execute()
        }

    private fun analyze() =
        db.jdbi.useHandleUnchecked { h ->
            for (relation in listOf("hog_data_file", "hog_delete_file", "hog_upload", "hog_file_removal")) {
                h.execute("VACUUM (ANALYZE) $relation")
            }
        }

    // ---- capture -----------------------------------------------------------

    /** Every statement one block of work issued, in order. */
    private fun captured(block: (org.jdbi.v3.core.Jdbi) -> Unit): List<String> {
        val issued = CopyOnWriteArrayList<String>()
        val instrumented = Database.jdbi(db.dataSource)
        instrumented.setSqlLogger(
            object : SqlLogger {
                override fun logAfterExecution(context: StatementContext) {
                    issued += context.renderedSql
                }
            },
        )
        block(instrumented)
        return issued.toList()
    }

    /**
     * The reference check, taken off a REAL drain: one sub-batch, the
     * real [RemovalStore] against a real object store, the real
     * commit-lock hold. Nothing here restates the SQL.
     *
     * The drain settles its rows, which is what a drain does; the probe
     * set this test EXPLAINs with is independent of them.
     */
    private fun captureReferenceCheck(): String {
        objects.createBucket(bucket())
        val store =
            RemovalStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            )
        val statements =
            store.use { s ->
                captured { jdbi ->
                    val result = CleanupService(jdbi, s).runOnce("v17", batchSize = PROBE)
                    drainStillReferenced = result.stillReferenced
                }
            }
        val reference =
            statements.filter {
                it.contains("hog_data_file") && it.contains("hog_delete_file") && it.contains("hog_upload")
            }
        assertThat(reference)
            .describedAs(
                "the drain must issue exactly one statement that asks all three tables about " +
                    "these paths; it issued %d. Statements:%n%s",
                reference.size,
                statements.joinToString("\n---\n"),
            )
            .hasSize(1)
        return reference.single()
    }

    /**
     * `UploadService.register`'s retained-path probe — the one it runs
     * on the foreground commit path, over the paths of the registered
     * claims it just read. Captured the same way, from the real
     * (internal) function rather than from a copy of its SQL.
     */
    private fun captureRetainedProbe(): String {
        val statements =
            captured { jdbi ->
                jdbi.useHandleUnchecked { h ->
                    h.begin()
                    try {
                        UploadService.register(h, catalogId, OWNER, livePaths.map { it to "data" })
                    } finally {
                        h.rollback()
                    }
                }
            }
        val retained =
            statements.filter {
                it.contains("hog_data_file") && it.contains("hog_delete_file")
            }
        assertThat(retained)
            .describedAs(
                "register must probe the manifest for its registered claims' paths; it issued " +
                    "%d such statements:%n%s",
                retained.size,
                statements.joinToString("\n---\n"),
            )
            .hasSize(1)
        return retained.single()
    }

    /** The manifest read V17 must NOT steal: `FileRepo.listAt`, the scan path. */
    private fun captureListAt(): String {
        val statements =
            captured { jdbi ->
                jdbi.useHandleUnchecked { h ->
                    FileRepo.listAt(h, catalogId, tableId = SMALL_TABLE, snapshot = 1)
                }
            }
        return statements.single { it.contains("FROM hog_data_file") }
    }

    // ---- EXPLAIN -----------------------------------------------------------

    /**
     * Serial plans only. A Gather reports its workers' scans with
     * `loops=` equal to the worker count and splits the buffer counts
     * across them, which would make every number below depend on the
     * machine's core count.
     */
    private fun <T> explain(block: (Handle) -> T): T =
        db.jdbi.inTransactionUnchecked { h ->
            h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
            block(h)
        }

    private fun explainReferenceCheck(): String =
        explain { h ->
            h.createQuery(
                "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) $referenceCheckSql",
            )
                .bind("catalogId", catalogId)
                .bindArray("paths", String::class.java, probePaths)
                .mapTo(String::class.java).list().joinToString("\n")
        }

    private fun explainRetainedProbe(): String =
        explain { h ->
            h.createQuery(
                "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) $retainedProbeSql",
            )
                .bind("catalog", catalogId)
                .bindArray("paths", String::class.java, livePaths)
                .mapTo(String::class.java).list().joinToString("\n")
        }

    private fun explainListAt(): String =
        explain { h ->
            h.createQuery(
                "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) $listAtSql",
            )
                .bind("catalogId", catalogId)
                .bind("tableId", SMALL_TABLE)
                .bind("snapshot", 1L)
                .mapTo(String::class.java).list().joinToString("\n")
        }

    /**
     * ONE scan node of [relation]: how it was driven, what it threw
     * away, how many descents it made, and what it cost.
     *
     * Scoped to the node rather than read off the root, which is V16's
     * lesson: EXPLAIN's `Planning:` section carries a `Buffers:` line
     * that dwarfs a well-indexed scan's, so a budget taken from the
     * plan's maximum measures catalog lookups instead of the statement.
     */
    private data class ScanNode(
        val line: String,
        val index: String?,
        val buffers: Long,
        val rowsRemovedByFilter: Long?,
        val indexSearches: Long?,
        val seqScan: Boolean,
    )

    private fun scanNode(
        text: String,
        relation: String,
    ): ScanNode {
        val lines = text.lines()

        fun indent(l: String) = l.length - l.trimStart().length
        val i = lines.indexOfFirst { Regex("""Scan.* on $relation\b""").containsMatchIn(it) }
        if (i < 0) throw AssertionError("no scan of $relation in:\n$text")
        val line = lines[i]
        val detail = lines.drop(i + 1).takeWhile { it.isNotBlank() && indent(it) > indent(line) }
        val buffersLine = detail.firstOrNull { it.trim().startsWith("Buffers:") }
        // `\bhit=` and `\bread=`, not "shared hit=" / "shared read=":
        // one line carries BOTH as `shared hit=2608 read=15450`, so a
        // pattern anchored on "shared read=" silently reads zero and a
        // cold scan of the whole manifest measures as its cache hits.
        val hit = buffersLine?.let { Regex("""\bhit=(\d+)""").find(it)?.groupValues?.get(1)?.toLong() } ?: 0L
        val read = buffersLine?.let { Regex("""\bread=(\d+)""").find(it)?.groupValues?.get(1)?.toLong() } ?: 0L

        fun detailLong(prefix: String): Long? =
            detail.firstOrNull { it.trim().startsWith(prefix) }
                ?.let { Regex("""(\d+)""").find(it.substringAfter(prefix))?.value?.toLong() }
        return ScanNode(
            line = line.trim(),
            index = Regex("""Scan(?: Backward)? using (\S+) on""").find(line)?.groupValues?.get(1),
            buffers = hit + read,
            rowsRemovedByFilter = detailLong("Rows Removed by Filter:"),
            indexSearches = detailLong("Index Searches:"),
            seqScan = line.contains("Seq Scan on $relation"),
        )
    }

    /**
     * The index the upload leg is driven from, whichever plan shape it
     * takes: `Index Scan using <index> on hog_upload`, or the
     * `Bitmap Index Scan on <index>` that feeds a
     * `Bitmap Heap Scan on hog_upload`. Null means it was driven from
     * nothing — a sequential scan.
     */
    private fun uploadDriver(text: String): String? {
        Regex("""Scan(?: Backward)? using (\S+) on hog_upload\b""").find(text)
            ?.let { return it.groupValues[1] }
        val lines = text.lines()
        val heap = lines.indexOfFirst { it.contains("Bitmap Heap Scan on hog_upload") }
        if (heap < 0) return null
        return lines.drop(heap + 1)
            .firstOrNull { it.contains("Bitmap Index Scan on ") }
            ?.let { Regex("""Bitmap Index Scan on (\S+)""").find(it)?.groupValues?.get(1) }
    }

    /** Rows of [relation] this catalog owns, counted rather than assumed. */
    private fun catalogRows(relation: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT count(*) FROM $relation WHERE catalog_id = :c")
                .bind("c", catalogId).mapTo(Long::class.java).one()
        }

    /** Heap pages of [relation], from the catalog rather than a constant. */
    private fun heapPages(relation: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT relpages FROM pg_class WHERE relname = :n")
                .bind("n", relation).mapTo(Long::class.java).one()
        }

    private fun indexDef(name: String): String? =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT pg_get_indexdef(oid) FROM pg_class WHERE relname = :n")
                .bind("n", name).mapTo(String::class.java).findOne().orElse(null)
        }

    /**
     * The buffer bound, DERIVED FROM THE PROBE rather than written
     * down: the statement's work is one descent per probe path, so its
     * budget has to scale with the probe the way the statement does.
     */
    private val probeBudget get() = PROBE.toLong() * BUFFERS_PER_PROBE

    // ---- the reference check ------------------------------------------------

    @Test
    fun `the captured statement is the drain's own three-table reference check`() {
        // The capture is load-bearing for every plan below, so what it
        // captured is asserted rather than assumed: all three legs, the
        // catalog and the path array, and no interpolated values
        // (invariant 9).
        assertThat(referenceCheckSql)
            .describedAs("the drain's reference check:%n%s", referenceCheckSql)
            .contains("hog_data_file")
            .contains("hog_delete_file")
            .contains("hog_upload")
            .contains(":catalogId")
            .contains(":paths")
        assertThat(referenceCheckSql.lowercase()).doesNotContain("s3://")
    }

    @Test
    fun `before V17 the reference check reads the catalog's whole manifest`() {
        // The red half, measured on the same rows the after half runs
        // on. THE ASSERTION IS THE WORK, NOT THE PLAN NODE. On this
        // fixture — one dominant catalog, as production has — it is a
        // `Seq Scan on hog_data_file` with `Rows Removed by Filter:
        // 499,999`, the same shape the 5M-row production fixture gives
        // (190,884 buffers, 4,999,998 rows discarded). Split the
        // catalogs evenly instead and the planner takes a Bitmap Heap
        // Scan driven by `hog_data_file_changefeed`'s leading
        // `catalog_id` with `path` demoted to a Filter: a different
        // node, the same defect, and only one of the two carries a
        // `Seq Scan` line. So what is pinned is that the scan THREW
        // AWAY the catalog's manifest to answer a 1,000-path question,
        // and read far more than the probe to do it.
        for (relation in listOf("hog_data_file", "hog_delete_file")) {
            val scan = scanNode(checkPlanBefore, relation)
            assertThat(scan.rowsRemovedByFilter)
                .describedAs(
                    "%s should be read and discarded whole at V16 (%d rows in this catalog):%n%s",
                    relation,
                    catalogRows(relation),
                    checkPlanBefore,
                )
                .isNotNull()
                .isGreaterThanOrEqualTo(catalogRows(relation) / 2)
            assertThat(scan.index)
                .describedAs("nothing keyed on `path` exists yet:%n%s", checkPlanBefore)
                .isNotIn(DATA_INDEX, DV_INDEX)
        }
        assertThat(scanNode(checkPlanBefore, "hog_data_file").buffers)
            .describedAs(
                "the V16 check should read about the whole manifest (%d heap pages):%n%s",
                heapPages("hog_data_file"),
                checkPlanBefore,
            )
            .isGreaterThan(probeBudget)
    }

    @Test
    fun `V17 adds nothing for the upload leg, which has its own index and a bounded working set`() {
        // Two separate reasons, and both are asserted, because "it
        // already has an index" alone would be a claim about the
        // planner's taste at this fixture's size.
        //
        //  1. hog_upload carries `UNIQUE (catalog_id, path)` from V12 —
        //     the exact key V17 adds to the other two tables — so a
        //     third index would be one nobody can choose. Measured on a
        //     200,000-claim fixture, the planner does choose it (935
        //     index searches, 3,743 buffers).
        //  2. Where it does NOT — as here, where the catalog's active
        //     claims are few enough that a bitmap scan of them is
        //     cheaper than 1,000 descents — the fallback is bounded by
        //     the ACTIVE claims, which is a live working set that
        //     settles at commit. hog_data_file and hog_delete_file have
        //     no such bound: they are append-only ledgers that keep
        //     every historical row, which is why they get an index and
        //     this table does not.
        //
        // V17 must also not CHANGE this leg, so before and after are
        // compared rather than named.
        val key =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_class c ON c.oid = i.indexrelid
                    JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                    WHERE c.relname = :n AND i.indisunique
                    ORDER BY k.ord
                    """,
                ).bind("n", UPLOAD_INDEX).mapTo(String::class.java).list()
            }
        assertThat(key)
            .describedAs("hog_upload's own unique key, resolved through pg_index.indkey")
            .containsExactly("catalog_id", "path")

        val active =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_upload WHERE catalog_id = :c AND state = 'active'",
                ).bind("c", catalogId).mapTo(Long::class.java).one()
            }
        val before = scanNode(checkPlanBefore, "hog_upload")
        val after = scanNode(checkPlanAfter, "hog_upload")
        // The index that DROVE the leg, which for a bitmap plan is named
        // on the child `Bitmap Index Scan` and not on the heap node.
        // Reading `ScanNode.index` alone compared null to null here and
        // could not fail; asserting it is present is what makes the
        // comparison below an assertion.
        val beforeDriver = uploadDriver(checkPlanBefore)
        val afterDriver = uploadDriver(checkPlanAfter)
        assertThat(beforeDriver)
            .describedAs("the upload leg must be index-driven at V16:%n%s", checkPlanBefore)
            .isNotNull()
        assertThat(afterDriver)
            .describedAs(
                "V17 changed the index the upload leg is driven from.%nbefore:%n%s%nafter:%n%s",
                checkPlanBefore,
                checkPlanAfter,
            )
            .isEqualTo(beforeDriver)
        assertThat(after.line.substringBefore(" on hog_upload"))
            .describedAs(
                "V17 changed the upload leg's node type.%nbefore:%n%s%nafter:%n%s",
                checkPlanBefore,
                checkPlanAfter,
            )
            .isEqualTo(before.line.substringBefore(" on hog_upload"))
        for ((label, scan) in listOf("before" to before, "after" to after)) {
            assertThat(scan.seqScan)
                .describedAs("the upload leg must never scan the table (%s V17)", label)
                .isFalse()
            assertThat(scan.rowsRemovedByFilter ?: 0L)
                .describedAs(
                    "the upload leg %s V17 discarded %s rows; the catalog has %d ACTIVE claims, " +
                        "and that — never the table's history — is what bounds this leg.%n%s",
                    label,
                    scan.rowsRemovedByFilter,
                    active,
                    if (label == "before") checkPlanBefore else checkPlanAfter,
                )
                .isLessThanOrEqualTo(active)
        }
    }

    @Test
    fun `after V17 both manifest legs probe by path and neither scans`() {
        val data = scanNode(checkPlanAfter, "hog_data_file")
        assertThat(data.index)
            .describedAs("reference check at V17:%n%s", checkPlanAfter)
            .isEqualTo(DATA_INDEX)
        val dv = scanNode(checkPlanAfter, "hog_delete_file")
        assertThat(dv.index)
            .describedAs("reference check at V17:%n%s", checkPlanAfter)
            .isEqualTo(DV_INDEX)
        assertThat(checkPlanAfter)
            .describedAs("reference check at V17:%n%s", checkPlanAfter)
            .doesNotContain("Seq Scan on hog_data_file")
            .doesNotContain("Seq Scan on hog_delete_file")
            .contains("Index Cond:")
    }

    @Test
    fun `the check descends once per probe path and filters nothing`() {
        // THE discrimination, and the reason the buffer budget is not
        // carrying it alone (V16's lesson): an index on `(catalog_id)`
        // alone, with `path` demoted to a Filter, emits exactly the same
        // rows — after reading every file row of the catalog and
        // throwing them away. A plan that honours `path` in the INDEX
        // CONDITION descends once per probe key and removes nothing.
        val data = scanNode(checkPlanAfter, "hog_data_file")
        // One descent per probe key, give or take the ranges a btree
        // array scan can skip between keys — against ONE for any plan
        // that demotes `path` to a Filter, which is the shape this
        // excludes.
        assertThat(data.indexSearches)
            .describedAs(
                "the data-file leg made %s descents for %d probe paths.%n%s",
                data.indexSearches,
                PROBE,
                checkPlanAfter,
            )
            .isNotNull()
            .isBetween((PROBE - PROBE_DV) * 3L / 4, PROBE + 2L)
        val dvSearches = scanNode(checkPlanAfter, "hog_delete_file").indexSearches
        assertThat(dvSearches)
            .describedAs(
                "the DV leg made %s descents for %d deletion-vector probe paths.%n%s",
                dvSearches,
                PROBE_DV,
                checkPlanAfter,
            )
            .isNotNull()
            .isBetween(PROBE_DV * 3L / 4, PROBE + 2L)
        assertThat(data.rowsRemovedByFilter)
            .describedAs(
                "the data-file leg discarded %s rows; every predicate belongs in the index " +
                    "condition.%n%s",
                data.rowsRemovedByFilter,
                checkPlanAfter,
            )
            .isNull()
        assertThat(scanNode(checkPlanAfter, "hog_delete_file").rowsRemovedByFilter)
            .describedAs("the DV leg must filter nothing:%n%s", checkPlanAfter)
            .isNull()
    }

    @Test
    fun `the check costs the probe rather than the manifest`() {
        val dataBefore = scanNode(checkPlanBefore, "hog_data_file")
        val dataAfter = scanNode(checkPlanAfter, "hog_data_file")
        assertThat(dataAfter.buffers)
            .describedAs(
                "the data-file leg read %d buffers for %d probe paths; the budget is %d (%d per " +
                    "probe) and the manifest is %d heap pages. Sizing with the MANIFEST rather " +
                    "than the PROBE is the defect V17 removes.%n%s",
                dataAfter.buffers,
                PROBE,
                probeBudget,
                BUFFERS_PER_PROBE,
                heapPages("hog_data_file"),
                checkPlanAfter,
            )
            .isLessThanOrEqualTo(probeBudget)
        assertThat(dataAfter.buffers)
            .describedAs("the check must still read something:%n%s", checkPlanAfter)
            .isGreaterThan(0)
        assertThat(dataAfter.buffers)
            .describedAs(
                "V17 must actually move the number: %d buffers before, %d after%n%s",
                dataBefore.buffers,
                dataAfter.buffers,
                checkPlanAfter,
            )
            .isLessThan(dataBefore.buffers / 4)
        val dvAfter = scanNode(checkPlanAfter, "hog_delete_file")
        assertThat(dvAfter.buffers)
            .describedAs(
                "the DV leg read %d buffers against a %d-page relation:%n%s",
                dvAfter.buffers,
                heapPages("hog_delete_file"),
                checkPlanAfter,
            )
            .isLessThanOrEqualTo(probeBudget)
    }

    @Test
    fun `the check answers with the live references and nothing else`() {
        // The plan is half the property; the ANSWER is the other half,
        // and it carries the predicates no plan can show. Two probe
        // paths are live data files, one is a live DV, one is held by an
        // ACTIVE upload claim — and one is held by an ABANDONED claim,
        // which owns nothing. Drop a leg of the UNION and a path
        // disappears from this list; drop the third leg's
        // `state = 'active'` and the abandoned one joins it, which in
        // production is the drain deleting an object a live writer is
        // about to register.
        val answer =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(referenceCheckSql)
                    .bind("catalogId", catalogId)
                    .bindArray("paths", String::class.java, probePaths)
                    .mapTo(String::class.java).list().sorted()
            }
        assertThat(answer)
            .describedAs("the reference check's answer over the seeded probe set")
            .containsExactlyElementsOf(hitPaths.sorted())
    }

    @Test
    fun `the real drain reported those references as the invariant violation`() {
        // The capture ran a real drain; this is what it did with the
        // rows the check returned. `still_referenced > 0` is invariant
        // 4's alert, and it must count the seeded hits exactly.
        assertThat(drainStillReferenced)
            .describedAs("still_referenced from the drain that issued the captured statement")
            .isEqualTo((PROBE_DATA_HITS + PROBE_DV_HITS + PROBE_CLAIMED_ACTIVE).toLong())
    }

    // ---- the commit path's own path probe ------------------------------------

    @Test
    fun `the commit path's retained-claim probe rides the same indexes`() {
        // `UploadService.register` runs inside EVERY commit transaction,
        // under the same per-catalog lock, and UNIONs the same two
        // tables by path. Before V17 it was two sequential scans of the
        // manifest per commit that carried registered claims.
        val dataBefore = scanNode(retainedPlanBefore, "hog_data_file")
        assertThat(dataBefore.rowsRemovedByFilter)
            .describedAs(
                "at V16 every commit that carried registered claims read and discarded the " +
                    "catalog's whole manifest here:%n%s",
                retainedPlanBefore,
            )
            .isNotNull()
            .isGreaterThanOrEqualTo(catalogRows("hog_data_file") / 2)
        val dataAfter = scanNode(retainedPlanAfter, "hog_data_file")
        assertThat(dataAfter.index)
            .describedAs("register's retained probe at V17:%n%s", retainedPlanAfter)
            .isEqualTo(DATA_INDEX)
        assertThat(scanNode(retainedPlanAfter, "hog_delete_file").index)
            .describedAs("register's retained probe at V17:%n%s", retainedPlanAfter)
            .isEqualTo(DV_INDEX)
        assertThat(dataAfter.buffers)
            .describedAs(
                "register probed %d paths and read %d buffers; %d before.%n%s",
                livePaths.size,
                dataAfter.buffers,
                dataBefore.buffers,
                retainedPlanAfter,
            )
            .isLessThanOrEqualTo(probeBudget + BUFFER_FLOOR)
    }

    // ---- what V17 must NOT change --------------------------------------------

    @Test
    fun `V17 does not steal the manifest read`() {
        // AGENT.md's twin failure: a new index that takes a query it
        // serves worse. `FileRepo.listAt` is the scan path's manifest
        // read — (catalog_id, table_id, snapshot), with no path
        // predicate at all — and a `(catalog_id, path)` index can only
        // serve it by reading the catalog's whole manifest in path
        // order. The property is that V17 leaves the plan ALONE, so it
        // is asserted as a before/after comparison rather than by
        // naming an index the fixture's size chose.
        fun driver(text: String): String? =
            Regex("""Index (?:Only )?Scan(?: Backward)? using (\S+) on hog_data_file\b""")
                .find(text)?.groupValues?.get(1)
        // The comparison is only worth making where there IS an access
        // path to change: on a table whose slice is most of the
        // manifest the planner scans, both sides read `null`, and two
        // nulls are an assertion that cannot fail. Hence the small
        // table, and hence this line.
        assertThat(driver(listAtPlanBefore))
            .describedAs(
                "the manifest read must be index-driven for the comparison below to mean " +
                    "anything:%n%s",
                listAtPlanBefore,
            )
            .isNotNull()
        assertThat(driver(listAtPlanAfter))
            .describedAs(
                "V17 changed the manifest read's access path.%nbefore:%n%s%nafter:%n%s",
                listAtPlanBefore,
                listAtPlanAfter,
            )
            .isEqualTo(driver(listAtPlanBefore))
        assertThat(listAtPlanAfter)
            .describedAs("the new index must not take the manifest read:%n%s", listAtPlanAfter)
            .doesNotContain(DATA_INDEX)
    }

    // ---- the shape of the indexes themselves ----------------------------------

    @Test
    fun `both indexes are keyed (catalog_id, path), not partial and not unique`() {
        for (index in listOf(DATA_INDEX to "hog_data_file", DV_INDEX to "hog_delete_file")) {
            val (name, relation) = index
            assertThat(indexDef(name)).describedAs("%s must exist after V17", name).isNotNull()
            val (partial, unique) =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        """
                        SELECT (i.indpred IS NOT NULL) AS partial, i.indisunique AS is_unique
                        FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
                        WHERE c.relname = :n
                        """,
                    ).bind("n", name)
                        .map { rs, _ -> rs.getBoolean("partial") to rs.getBoolean("is_unique") }.one()
                }
            // Through pg_index.indpred, not by counting rows (AGENT.md).
            // NOT PARTIAL is the load-bearing half: the check covers
            // "any file row, live or not", so a partial index on
            // liveness would hide exactly the rows whose absence
            // authorizes a physical delete.
            assertThat(partial)
                .describedAs(
                    "%s must cover ended rows too — the liveness check reads historical rows on " +
                        "purpose, because they still claim their objects at retained snapshots",
                    name,
                )
                .isFalse()
            // Nothing makes a file path unique in this schema — two
            // tables may register one object, and V16's file carries the
            // full argument — so a unique index here would reject legal
            // state.
            assertThat(unique).describedAs("pg_index.indisunique for %s", name).isFalse()
            val key =
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        """
                        SELECT a.attname
                        FROM pg_index i
                        JOIN pg_class c ON c.oid = i.indexrelid
                        JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
                        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                        WHERE c.relname = :n
                        ORDER BY k.ord
                        """,
                    ).bind("n", name).mapTo(String::class.java).list()
                }
            // Resolved through pg_index.indkey, never a substring of
            // pg_get_indexdef: the index is NAMED hog_data_file_path, so
            // a `contains("catalog_id", "path")` is satisfied by the
            // name alone. Order is the half that matters — (path,
            // catalog_id) would serve this one statement and nothing
            // else the table asks.
            assertThat(key)
                .describedAs("pg_index.indkey for %s on %s, in order", name, relation)
                .containsExactly("catalog_id", "path")
        }
    }

    @Test
    fun `two file rows may share a path, which is why neither index is unique`() {
        // The reachability proof behind the non-unique decision, and its
        // mutation: make either index UNIQUE and this reds on the INSERT
        // rather than on an assertion. One object registered into two
        // tables is legal state — CommitService says so explicitly
        // ("Duplicate paths against live/historical file rows stay
        // legal") — and the drain is what resolves it, because the queue
        // is a suggestion and never an authorization.
        val shared = dataPath(1, 777_777_777L)
        db.jdbi.useHandleUnchecked { h ->
            for (fileId in listOf(900_000_001L, 900_000_002L)) {
                h.execute(
                    """
                    INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                               path, record_count, file_size_bytes, row_id_start)
                    VALUES (?, ?, 1, 1, ?, 10, 100, ?)
                    """,
                    catalogId,
                    fileId,
                    shared,
                    fileId * 10,
                )
            }
        }
        val rows =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_data_file WHERE catalog_id = :c AND path = :p",
                ).bind("c", catalogId).bind("p", shared).mapTo(Int::class.java).one()
            }
        assertThat(rows).describedAs("two file rows over one path are legal state").isEqualTo(2)
    }

    // ---- re-entry -------------------------------------------------------------

    @Test
    fun `re-entry is a clean no-op - an existing index is not rebuilt`() {
        // V17 runs with executeInTransaction=false, so a partial failure
        // leaves a `success = false` history row that fails Flyway's
        // validate on every replica until an operator repairs it — which
        // is why every statement in the file is idempotent. What must
        // hold is that a re-run over an index that already exists finds
        // it and skips: the rebuild is 26 s of concurrent build at
        // production size, and it is budgeted for exactly once.
        PgTestSupport.freshDatabase(productionSession = true).use { fresh ->
            val before = listOf(DATA_INDEX, DV_INDEX).map { indexNode(fresh, it) }
            assertThat(before).allSatisfy { assertThat(it).isNotZero() }
            fresh.jdbi.useHandleUnchecked { h -> h.execute(REAPPLY_V17) }
            Database.migrate(fresh.dataSource)
            assertThat(listOf(DATA_INDEX, DV_INDEX).map { indexNode(fresh, it) })
                .describedAs("an existing index must not be dropped and rebuilt")
                .isEqualTo(before)
        }
    }

    @Test
    fun `an INVALID remnant is cleared rather than skipped forever`() {
        PgTestSupport.freshDatabase(productionSession = true).use { fresh ->
            // Exactly what a cancelled `CREATE INDEX CONCURRENTLY`
            // leaves, and at 26 s a build has time to be cancelled: a
            // killed pod, a statement timeout, an operator's Ctrl-C.
            // Forged, because killing a real concurrent build
            // mid-flight is not reproducible in a test. Without the DO
            // block, `CREATE INDEX CONCURRENTLY IF NOT EXISTS` matches
            // on NAME, sees the remnant, skips, and leaves an index
            // every INSERT maintains and no query may use.
            fresh.jdbi.useHandleUnchecked { h ->
                for (name in listOf(DATA_INDEX, DV_INDEX)) {
                    h.execute(
                        "UPDATE pg_index SET indisvalid = false WHERE indexrelid = " +
                            "(SELECT oid FROM pg_class WHERE relname = ?)",
                        name,
                    )
                }
                h.execute(REAPPLY_V17)
            }
            assertThat(listOf(DATA_INDEX, DV_INDEX).map { indexValid(fresh, it) })
                .containsExactly(false, false)

            Database.migrate(fresh.dataSource)

            assertThat(listOf(DATA_INDEX, DV_INDEX).map { indexValid(fresh, it) })
                .describedAs("the remnants must be dropped and rebuilt, not skipped")
                .containsExactly(true, true)
        }
    }

    private fun indexNode(
        target: PgTestSupport.TestDb,
        name: String,
    ): Long =
        target.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT relfilenode FROM pg_class WHERE relname = :n")
                .bind("n", name).mapTo(Long::class.java).findOne().orElse(0L)
        }

    private fun indexValid(
        target: PgTestSupport.TestDb,
        name: String,
    ): Boolean =
        target.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid " +
                    "WHERE c.relname = :n",
            ).bind("n", name).mapTo(Boolean::class.javaObjectType).findOne().orElse(false)
        }
}
