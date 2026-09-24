package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.service.CleanupService
import com.posthog.hoglake.service.ExpiryService
import com.posthog.hoglake.service.UploadService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * V16 against the statement it exists for, on a queue the size of the
 * one that produced #199.
 *
 * The migration RUNS here — arriving at V15 first, seeding the rows it
 * will run against, and then calling [Database.migrate] — rather than
 * being inspected on a fully-migrated database. Two reasons, both
 * AGENT.md's: the schema-equivalence gate already folds every migration
 * onto a virgin database, so a test that only looks at the result
 * asserts nothing about what this file did; and **an index proves
 * itself against the query it serves**, which means EXPLAINing the
 * repo's own SQL (`internal` for exactly this) rather than a
 * hand-written lookalike. V14's first index was on the wrong column of
 * a join, and its test was green because it EXPLAINed a predicate no
 * code path issues.
 *
 * RED BEFORE GREEN IS STRUCTURAL HERE. The guard is EXPLAINed twice on
 * the same database and the same rows — once at V15 and once after V16
 * — so the "before" half is not a claim about a deleted file but a
 * measurement, and the plan text that proves it is in the failure
 * message of every assertion below.
 *
 * What the fixture has to get right for any of it to mean anything:
 *
 *  - **A SECOND CATALOG with its own queue, INTERLEAVED in the id
 *    space.** `catalog_id` leads both indexes, and with one catalog
 *    "this catalog's undrained rows" and "every undrained row" are the
 *    same rows — so a plan that ignores `catalog_id` measures
 *    identically to one that honours it. Interleaved, because a
 *    neighbour's rows packed after ours would let a `removal_id` scan
 *    find this catalog's rows immediately and look like a good plan for
 *    a reason the fixture invented.
 *  - **A DRAINED LEDGER.** The index is partial on `drained_at IS
 *    NULL`; without settled rows in the table the partial predicate
 *    costs nothing to get wrong. One probe path is seeded DRAINED-ONLY
 *    for the same reason: a guard that lost the clause would return it.
 *  - **A PROBE THAT HITS.** Some of the commit's candidate paths really
 *    are queued, so the guard returns rows: a query whose driving scan
 *    finds nothing plans beautifully and proves nothing.
 *  - **BUFFERS, not rows.** The guard emits a handful of rows under
 *    either plan — the whole defect is how many pages it reads to find
 *    them. Buffers are also what an operator notices, because they are
 *    IO.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class V16FileRemovalPathIndexMigrationIntegrationTest {
    private companion object {
        const val INDEX = "hog_file_removal_undrained_path"
        const val DRAIN_INDEX = "hog_file_removal_drain"

        /** The catalog under test: prod-us held 164,307 undrained rows (#198). */
        const val UNDRAINED = 100_000

        /** A NEIGHBOUR catalog's queue, interleaved — see the class KDoc. */
        const val OTHER_UNDRAINED = 100_000

        /** Settled ledger rows, so the index's partial predicate is load-bearing. */
        const val DRAINED = 25_000

        /** A commit's worth of candidate paths: what the guard probes with. */
        const val PROBE = 64

        /** Probe paths that are really queued, so the guard returns rows. */
        const val PROBE_HITS = 3

        /**
         * One probe path with a DRAINED row and no undrained one. A
         * guard that lost `drained_at IS NULL` — which is also the
         * index's partial predicate — returns it, and nothing about the
         * plan would notice.
         */
        const val PROBE_DRAINED_ONLY = 1

        /** File rows the upload reclaim's other two `NOT EXISTS` probe. */
        const val MANIFEST = 2_000

        /**
         * The SPARSE queue: [SPARSE_ROWS] ledger rows of which one in
         * [SPARSE_EVERY] is undrained, which is the shape a 30-day
         * ledger gives a catalog that drains. There the primary key
         * cannot serve the drain and `hog_file_removal_drain` is the
         * planner's own choice.
         */
        const val SPARSE_ROWS = 60_000

        const val SPARSE_EVERY = 20

        /**
         * Buffers one probe path may cost: a B-tree descent into a
         * partial index, with slack. Measured at 175 buffers for 64
         * scattered probes (2.7 each) against a degraded plan's 733, so
         * the budget sits an order of magnitude clear of the shape it
         * excludes instead of the 1.27x a constant gave it.
         */
        const val BUFFERS_PER_PROBE = 4

        /** Per-statement overhead the upload reclaim's budget forgives. */
        const val BUFFER_FLOOR = 64

        /**
         * Put the history back to the point where V16 has yet to
         * succeed. Every row from 16 ONWARD goes, not just 16's, which
         * is V14's helper verbatim and the lesson V15's re-entry test
         * had to learn from this change: an interrupted V16 is the last
         * thing that ran, so nothing after it can be applied, and Flyway
         * agrees — with a later migration recorded and 16 missing,
         * `validate` refuses the whole run as an out-of-order resolved
         * migration rather than re-applying 16. The tail re-runs, which
         * is free: a migration that can be retried is idempotent by
         * construction.
         */
        const val REAPPLY_V16 = "DELETE FROM flyway_schema_history WHERE version::numeric >= 16"
    }

    // productionSession: the migration runs on a session carrying the
    // real `statement_timeout` / `idle_in_transaction_session_timeout`,
    // so this fixture reaches the session behaviour a booting pod has.
    // V16 builds its index with a BLOCKING `CREATE INDEX` inside a 5 s
    // lock_timeout; on a session with no statement bound a build that
    // stalled would hang the suite instead of failing the way a pod
    // does.
    private val db = PgTestSupport.freshDatabaseAt("15", productionSession = true)
    private var catalogId = 0L
    private var otherCatalogId = 0L

    /**
     * The probe set: [PROBE_HITS] queued paths, then one that is only
     * in the drained ledger, then paths the catalog has never seen.
     *
     * SCATTERED, and that is the whole point of the hash segment. A
     * commit's output paths do not share a prefix — writers spread
     * object-store keys deliberately — and a probe set that DOES share
     * one is not the query production sends: sixty-four keys inside
     * three narrow ranges let the btree's array scan descend twice and
     * walk, which reported `Index Searches: 2` and seven buffers and
     * made the measurement far kinder than the statement it claimed to
     * model. With the keys spread across the index the same probe costs
     * a descent apiece, and THAT is the number the budget below bounds.
     */
    private val probePaths: List<String> =
        List(PROBE) { i ->
            val kind =
                when {
                    i < PROBE_HITS -> "queued"
                    i < PROBE_HITS + PROBE_DRAINED_ONLY -> "settled"
                    else -> "fresh"
                }
            "s3://v16/" + md5("probe-$kind-$i") + "/commit.parquet"
        }

    /** The writers' hash segment, mirrored so the fixture can compute one. */
    private fun md5(text: String): String =
        java.security.MessageDigest.getInstance("MD5")
            .digest(text.toByteArray())
            .joinToString("") { "%02x".format(it) }

    private val queuedProbePaths get() = probePaths.take(PROBE_HITS).sorted()

    private lateinit var guardPlanBeforeV16: String
    private lateinit var guardPlanAfterV16: String
    private lateinit var uploadPlanAfterV16: String
    private lateinit var drainPlanBeforeV16: String
    private lateinit var drainPlanAfterV16: String

    @AfterAll
    fun tearDown() = db.close()

    @BeforeAll
    fun seedThenMigrate() {
        catalogId = createCatalog("v16")
        otherCatalogId = createCatalog("v16-neighbour")
        seedQueues()
        seedProbes()
        seedManifest()
        db.jdbi.useHandleUnchecked { h -> h.execute("VACUUM (ANALYZE) hog_file_removal") }

        assertThat(indexDef()).describedAs("%s absent before V16", INDEX).isNull()
        guardPlanBeforeV16 = explainGuard()
        drainPlanBeforeV16 = explainDrain()

        Database.migrate(db.dataSource)

        // VACUUM and not merely ANALYZE: a bulk insert leaves the
        // visibility map unset, so an Index Only Scan would pay a heap
        // fetch per row and the budget below would measure the absence
        // of autovacuum rather than the plan.
        db.jdbi.useHandleUnchecked { h -> h.execute("VACUUM (ANALYZE) hog_file_removal") }
        guardPlanAfterV16 = explainGuard()
        uploadPlanAfterV16 = explainUploadReclaim()
        drainPlanAfterV16 = explainDrain()
    }

    // ---- fixture ----------------------------------------------------------

    private fun createCatalog(name: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "INSERT INTO hog_catalog (name, data_path) VALUES (:n, :p) RETURNING catalog_id",
            ).bind("n", name).bind("p", "s3://v16/$name").mapTo(Long::class.java).one()
        }

    /**
     * Both catalogs' queues, from ONE series alternating between them,
     * so neither catalog's rows are clustered in `removal_id` order; and
     * this catalog's settled ledger on top.
     */
    private fun seedQueues() =
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                SELECT CASE WHEN g % 2 = 0 THEN :c ELSE :other END,
                       's3://v16/' || md5(g::text) || '/queue.parquet', 'data', 'snapshot_expiry'
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("other", otherCatalogId)
                .bind("n", UNDRAINED + OTHER_UNDRAINED)
                .execute()
            h.createUpdate(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason,
                                              drained_at, drained_outcome)
                SELECT :c, 's3://v16/' || md5('settled' || g::text) || '/gone.parquet', 'data', 'snapshot_expiry',
                       now() - interval '1 day', 'deleted'
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("n", DRAINED).execute()
        }

    private fun seedProbes() =
        db.jdbi.useHandleUnchecked { h ->
            // Each hit is seeded TWICE — once undrained, once settled —
            // so the guard's answer cannot be explained by "this path
            // has a row".
            for (i in 0 until PROBE_HITS) {
                h.execute(
                    "INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason) " +
                        "VALUES (?, ?, 'data', 'snapshot_expiry')",
                    catalogId,
                    probePaths[i],
                )
                h.execute(
                    "INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason, " +
                        "drained_at, drained_outcome) " +
                        "VALUES (?, ?, 'data', 'snapshot_expiry', now(), 'deleted')",
                    catalogId,
                    probePaths[i],
                )
            }
            // The drained-only path, and the same path undrained in the
            // NEIGHBOUR catalog: the two halves of the guard's predicate
            // that a plan assertion cannot see.
            for (i in PROBE_HITS until PROBE_HITS + PROBE_DRAINED_ONLY) {
                h.execute(
                    "INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason, " +
                        "drained_at, drained_outcome) " +
                        "VALUES (?, ?, 'data', 'snapshot_expiry', now(), 'deleted')",
                    catalogId,
                    probePaths[i],
                )
                h.execute(
                    "INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason) " +
                        "VALUES (?, ?, 'data', 'snapshot_expiry')",
                    otherCatalogId,
                    probePaths[i],
                )
            }
        }

    /** What the upload reclaim's other two `NOT EXISTS` probe. */
    private fun seedManifest() =
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 0)",
                catalogId,
            )
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start)
                SELECT :c, g, 1, 0, 's3://v16/data/' || g || '.parquet', 10, 1024, g * 10
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("n", MANIFEST).execute()
            h.createUpdate(
                """
                INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id,
                                             begin_snapshot, path, delete_count, file_size_bytes)
                SELECT :c, g, 1, g, 0, 's3://v16/dv/' || g || '.puffin', 1, 16
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("n", MANIFEST).execute()
            h.execute("VACUUM (ANALYZE) hog_data_file")
            h.execute("VACUUM (ANALYZE) hog_delete_file")
        }

    // ---- EXPLAIN ----------------------------------------------------------

    /**
     * Serial plans only. A Gather reports its workers' scans with
     * `loops=` equal to the worker count and splits the buffer counts
     * across them, which would make every number below depend on the
     * machine's core count.
     */
    private fun explain(block: (Handle) -> String): String =
        db.jdbi.inTransactionUnchecked { h ->
            h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
            block(h)
        }

    private fun explainGuard(): String =
        explain { h ->
            h.createQuery(
                "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " +
                    CommitService.REMOVAL_QUEUE_COLLISION_SQL,
            )
                .bind("catalogId", catalogId)
                .bindArray("paths", String::class.java, probePaths)
                .mapTo(String::class.java).list().joinToString("\n")
        }

    /**
     * The reclaim INSERT, rolled back: EXPLAIN ANALYZE executes the
     * statement, and the fixture has to survive for the assertions that
     * follow. The statement under EXPLAIN is the one production runs —
     * the bare `NOT EXISTS` fragment plans differently, because the
     * three of them are one chain of sub-plans over a Result node.
     */
    private fun explainUploadReclaim(): String =
        db.jdbi.withHandleUnchecked { h ->
            h.begin()
            try {
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                h.createQuery(
                    "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " +
                        UploadService.QUEUE_ABANDONED_UPLOAD_SQL,
                )
                    .bind("catalog", catalogId)
                    .bind("path", "s3://v16/abandoned/upload.parquet")
                    .bind("kind", "data")
                    .mapTo(String::class.java).list().joinToString("\n")
            } finally {
                h.rollback()
            }
        }

    private fun explainDrain(): String =
        explain { h ->
            h.createQuery(
                "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " +
                    CleanupService.DRAIN_BATCH_SQL,
            )
                .bind("catalogId", catalogId)
                .bind("limit", CleanupService.SUB_BATCH)
                .mapTo(String::class.java).list().joinToString("\n")
        }

    /**
     * ONE scan node of `hog_file_removal`: how it was driven, what it
     * threw away, how many descents it made, and what it cost.
     *
     * Scoped to the node rather than read off the root, and that is not
     * fussiness. The root's cumulative `Buffers:` line is not even the
     * largest in the output — EXPLAIN's `Planning:` section carries one
     * too, and on the indexed plan the planner's own 29 buffers dwarf
     * the scan's. A budget taken from the maximum was therefore
     * measuring catalog lookups, not the statement.
     *
     * A scan node is a LEAF, so its detail block runs to the next line
     * at its indentation or shallower and no nested node can steal a
     * line from it.
     */
    private data class ScanNode(
        val line: String,
        val index: String?,
        val buffers: Long,
        val rowsRemovedByFilter: Long?,
        val indexSearches: Long?,
        val seqScan: Boolean,
    )

    private fun scanNode(text: String): ScanNode? {
        val lines = text.lines()

        fun indent(l: String) = l.length - l.trimStart().length
        val i = lines.indexOfFirst { Regex("""Scan.* on hog_file_removal\b""").containsMatchIn(it) }
        if (i < 0) return null
        val line = lines[i]
        val detail = lines.drop(i + 1).takeWhile { it.isNotBlank() && indent(it) > indent(line) }
        val buffersLine = detail.firstOrNull { it.trim().startsWith("Buffers:") }
        val hit = buffersLine?.let { Regex("""shared hit=(\d+)""").find(it)?.groupValues?.get(1)?.toLong() } ?: 0L
        val read = buffersLine?.let { Regex("""shared read=(\d+)""").find(it)?.groupValues?.get(1)?.toLong() } ?: 0L

        fun detailLong(prefix: String): Long? =
            detail.firstOrNull { it.trim().startsWith(prefix) }
                ?.let { Regex("""(\d+)""").find(it.substringAfter(prefix))?.value?.toLong() }
        return ScanNode(
            line = line.trim(),
            index = Regex("""Scan(?: Backward)? using (\S+) on""").find(line)?.groupValues?.get(1),
            buffers = hit + read,
            rowsRemovedByFilter = detailLong("Rows Removed by Filter:"),
            indexSearches = detailLong("Index Searches:"),
            seqScan = line.contains("Seq Scan on hog_file_removal"),
        )
    }

    private fun guardScan(text: String): ScanNode =
        scanNode(text)
            ?: throw AssertionError("no scan of hog_file_removal in:\n" + text)

    /** Heap pages of the queue, from the catalog rather than a constant. */
    private fun heapPages(): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT relpages FROM pg_class WHERE relname = 'hog_file_removal'")
                .mapTo(Long::class.java).one()
        }

    private fun indexDef(): String? =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT pg_get_indexdef(oid) FROM pg_class WHERE relname = :n")
                .bind("n", INDEX).mapTo(String::class.java).findOne().orElse(null)
        }

    /**
     * The buffer bound, DERIVED FROM THE PROBE rather than written
     * down. A constant is only as good as its distance from the worst
     * plan it must exclude, and the constant this replaces (576) sat
     * 1.27x below that plan's 733 — close enough that a fixture tweak
     * or a Postgres release could walk the degraded plan straight under
     * it without anyone noticing. Four buffers per probe path is a
     * B-tree descent's worth, and it scales with the probe the way the
     * statement does.
     */
    private val probeBudget get() = PROBE.toLong() * BUFFERS_PER_PROBE

    // ---- the guard --------------------------------------------------------

    @Test
    fun `before V16 the path guard sequentially scans the catalog's whole queue`() {
        // The red half, measured rather than asserted from a deleted
        // file: this is the plan #199 reports as 83,569 sequential scans
        // of hog_file_removal on gigahog-prod-us since one restart.
        val scan = guardScan(guardPlanBeforeV16)
        assertThat(scan.seqScan)
            .describedAs("guard plan at V15:%n%s", guardPlanBeforeV16)
            .isTrue()
        assertThat(scan.buffers)
            .describedAs(
                "the V15 guard should read about the whole queue (%d heap pages):%n%s",
                heapPages(),
                guardPlanBeforeV16,
            )
            .isGreaterThan(probeBudget)
    }

    @Test
    fun `after V16 the guard probes the queue by path and never scans it`() {
        val scan = guardScan(guardPlanAfterV16)
        assertThat(scan.index)
            .describedAs("guard plan at V16:%n%s", guardPlanAfterV16)
            .isEqualTo(INDEX)
        assertThat(guardPlanAfterV16)
            .describedAs("guard plan at V16:%n%s", guardPlanAfterV16)
            .contains("Index Cond:")
        assertThat(scan.seqScan)
            .describedAs("guard plan at V16:%n%s", guardPlanAfterV16)
            .isFalse()
    }

    @Test
    fun `the guard descends once per probe path and filters nothing`() {
        // THE discrimination, and the reason the buffer budget below is
        // no longer carrying it alone. Two plans emit exactly the right
        // three rows and are both wrong:
        //
        //  * an index on `(catalog_id)` alone, with `path` demoted to a
        //    Filter — ONE descent, then every undrained row of the
        //    catalog read and thrown away (measured: 733 buffers,
        //    `Rows Removed by Filter: 99936`);
        //  * a guard rewritten to `path LIKE ANY(...)`, which no btree
        //    under a non-C collation can drive at all — same shape.
        //
        // Both are invisible to a row-count assertion and only 1.27x
        // away from the old constant budget. Neither survives these two
        // numbers: a plan that honours `path` in the INDEX CONDITION
        // descends once per probe key and removes nothing by filter.
        val scan = guardScan(guardPlanAfterV16)
        assertThat(scan.indexSearches)
            .describedAs(
                "the scan made %s descents for %d probe paths; a plan that demotes `path` to a " +
                    "Filter makes one and reads the queue.%n%s",
                scan.indexSearches,
                PROBE,
                guardPlanAfterV16,
            )
            .isNotNull()
            .isBetween(PROBE * 3L / 4, PROBE + 2L)
        assertThat(scan.rowsRemovedByFilter)
            .describedAs(
                "the scan discarded %s rows; every predicate belongs in the index condition, so " +
                    "there is nothing left for a Filter to remove.%n%s",
                scan.rowsRemovedByFilter,
                guardPlanAfterV16,
            )
            .isNull()
    }

    @Test
    fun `the guard costs the probe rather than the queue`() {
        val before = guardScan(guardPlanBeforeV16)
        val after = guardScan(guardPlanAfterV16)
        assertThat(after.buffers)
            .describedAs(
                "the guard read %d buffers for %d probe paths; the budget is %d (%d per probe) " +
                    "and the queue is %d heap pages. Sizing with the QUEUE rather than the PROBE " +
                    "is the defect V16 exists to remove.%n%s",
                after.buffers,
                PROBE,
                probeBudget,
                BUFFERS_PER_PROBE,
                heapPages(),
                guardPlanAfterV16,
            )
            .isLessThanOrEqualTo(probeBudget)
        assertThat(after.buffers)
            .describedAs("the guard must still read something:%n%s", guardPlanAfterV16)
            .isGreaterThan(0)
        assertThat(after.buffers)
            .describedAs(
                "V16 must actually move the number: %d buffers before, %d after%n%s",
                before.buffers,
                after.buffers,
                guardPlanAfterV16,
            )
            .isLessThan(before.buffers / 4)
    }

    @Test
    fun `the guard returns the undrained hits and neither settled rows nor a neighbour's`() {
        // The plan is half the property; the ANSWER is the other half,
        // and it is where the two halves of the predicate that a plan
        // cannot show are pinned. Each hit also has a settled twin, one
        // probe path is settled-only, and that same path is UNDRAINED in
        // the neighbour catalog — so dropping `drained_at IS NULL` or
        // `catalog_id = :catalogId` each adds exactly one path here.
        val hits =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(CommitService.REMOVAL_QUEUE_COLLISION_SQL)
                    .bind("catalogId", catalogId)
                    .bindArray("paths", String::class.java, probePaths)
                    .mapTo(String::class.java).list().sorted()
            }
        assertThat(hits)
            .describedAs("the guard's answer over the seeded probe set")
            .containsExactlyElementsOf(queuedProbePaths)
    }

    // ---- the upload reclaim ------------------------------------------------

    @Test
    fun `the upload reclaim's queue probe rides the index too`() {
        // Only the FIRST of the three `NOT EXISTS` is V16's business.
        // The other two probe hog_data_file and hog_delete_file by
        // `path`, and those tables carry no path index by design
        // (VerifyQueryPlanIntegrationTest states the argument: it would
        // be paid for by every commit, on the hottest insert in the
        // system). They stay sequential scans of the MANIFEST — a
        // different and bounded cost — and asserting that they are not
        // would pin a decision this change did not make.
        assertThat(uploadPlanAfterV16)
            .describedAs("upload reclaim plan:%n%s", uploadPlanAfterV16)
            .contains(INDEX)
        assertThat(uploadPlanAfterV16)
            .describedAs("upload reclaim plan:%n%s", uploadPlanAfterV16)
            .doesNotContain("Seq Scan on hog_file_removal")
    }

    @Test
    fun `the upload reclaim costs the manifest it probes, never the queue`() {
        // Bounded by the two manifest scans this change does not touch,
        // plus one probe of the queue — never by the queue's own depth.
        // Read from pg_class rather than written down.
        val manifestPages =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT sum(relpages) FROM pg_class " +
                        "WHERE relname IN ('hog_data_file', 'hog_delete_file')",
                ).mapTo(Long::class.java).one()
            }
        // The queue probe is scoped to its own scan node; the manifest
        // scans are read off the root because they are what the rest of
        // the statement is.
        val queueProbe = guardScan(uploadPlanAfterV16)
        assertThat(queueProbe.buffers)
            .describedAs(
                "the reclaim's queue probe read %d buffers; it is ONE path, so it is one " +
                    "descent. The queue is %d heap pages and must not appear in this number.%n%s",
                queueProbe.buffers,
                heapPages(),
                uploadPlanAfterV16,
            )
            .isLessThanOrEqualTo(BUFFERS_PER_PROBE + BUFFER_FLOOR.toLong())
        assertThat(queueProbe.rowsRemovedByFilter)
            .describedAs("the queue probe must filter nothing:%n%s", uploadPlanAfterV16)
            .isNull()
        // And the manifest halves stay what they were: bounded by the
        // manifest, which this change does not touch.
        val manifestBuffers =
            uploadPlanAfterV16.lines()
                .filter { it.trim().startsWith("Buffers:") }
                .maxOfOrNull { line ->
                    val hit = Regex("""shared hit=(\d+)""").find(line)?.groupValues?.get(1)?.toLong() ?: 0L
                    val read = Regex("""shared read=(\d+)""").find(line)?.groupValues?.get(1)?.toLong() ?: 0L
                    hit + read
                } ?: 0L
        assertThat(manifestBuffers)
            .describedAs(
                "the reclaim read %d buffers in total; the two manifest scans are worth %d " +
                    "pages and the queue probe one descent.%n%s",
                manifestBuffers,
                manifestPages,
                uploadPlanAfterV16,
            )
            .isLessThanOrEqualTo(manifestPages + BUFFERS_PER_PROBE + BUFFER_FLOOR)
    }

    // ---- the index that was already there ----------------------------------

    /** The index a plan actually drove `hog_file_removal` from. */
    private fun scannedIndex(text: String): String? =
        Regex("""Index (?:Only )?Scan(?: Backward)? using (\S+) on hog_file_removal\b""")
            .find(text)?.groupValues?.get(1)

    @Test
    fun `V16 does not change how the cleanup drain reads the queue`() {
        // AGENT.md: an index nothing chooses must not be added — and the
        // twin failure is a new index that STEALS a query it serves
        // worse. `hog_file_removal_undrained_path` orders by `path`, so
        // the planner choosing it here would put a Sort over every
        // undrained row of the catalog underneath the LIMIT.
        //
        // The property is that V16 leaves this plan ALONE, and it is
        // asserted as the before/after comparison rather than by naming
        // an index: WHICH index serves the drain depends on how dense
        // this catalog's undrained rows are in `removal_id` order, and
        // that is a property of the fixture, not of V16. On this one the
        // planner takes `hog_file_removal_pkey` and filters — the
        // catalog's rows are every other row, so it discards 25 to emit
        // 25 — and it takes it both before and after. The next test
        // covers the sparse shape `hog_file_removal_drain` exists for.
        assertThat(scannedIndex(drainPlanAfterV16))
            .describedAs(
                "V16 changed the drain's access path.%nbefore:%n%s%nafter:%n%s",
                drainPlanBeforeV16,
                drainPlanAfterV16,
            )
            .isEqualTo(scannedIndex(drainPlanBeforeV16))
        assertThat(drainPlanAfterV16)
            .describedAs("the new index must not steal the drain:%n%s", drainPlanAfterV16)
            .doesNotContain(INDEX)
        assertThat(drainPlanAfterV16)
            .describedAs("the LIMIT must stop the scan, not trim a sort:%n%s", drainPlanAfterV16)
            .doesNotContain("Sort")
        assertThat(drainPlanAfterV16)
            .describedAs("drain plan at V16:%n%s", drainPlanAfterV16)
            .doesNotContain("Seq Scan on hog_file_removal")
    }

    @Test
    fun `where the drain index is the planner's choice, V16 does not take it away`() {
        // The shape `hog_file_removal_drain` was built for, and the one
        // the test above cannot show: a catalog whose UNDRAINED rows are
        // sparse in `removal_id` order, because the settled ledger sits
        // between them (30 days of it, by default). There the primary
        // key would discard nineteen rows per row it emits, and the
        // partial index is the only thing that supplies the predicate
        // AND the ordering. V16 must not displace it.
        PgTestSupport.freshDatabase(productionSession = true).use { fresh ->
            val cat =
                fresh.jdbi.withHandleUnchecked { h ->
                    val id =
                        h.createQuery(
                            "INSERT INTO hog_catalog (name, data_path) " +
                                "VALUES ('v16-sparse', 's3://v16/sparse') RETURNING catalog_id",
                        ).mapTo(Long::class.java).one()
                    h.createUpdate(
                        """
                        INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason,
                                                      drained_at, drained_outcome)
                        SELECT :c, 's3://v16/sparse/' || g || '.parquet', 'data', 'snapshot_expiry',
                               CASE WHEN g % :every = 0 THEN NULL ELSE now() - interval '1 day' END,
                               CASE WHEN g % :every = 0 THEN NULL ELSE 'deleted' END
                        FROM generate_series(1, :n) g
                        """,
                    ).bind("c", id).bind("n", SPARSE_ROWS).bind("every", SPARSE_EVERY).execute()
                    h.execute("VACUUM (ANALYZE) hog_file_removal")
                    id
                }
            val plan =
                fresh.jdbi.inTransactionUnchecked { h ->
                    h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                    h.createQuery(
                        "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " +
                            CleanupService.DRAIN_BATCH_SQL,
                    )
                        .bind("catalogId", cat)
                        .bind("limit", CleanupService.SUB_BATCH)
                        .mapTo(String::class.java).list().joinToString("\n")
                }
            assertThat(scannedIndex(plan))
                .describedAs("drain plan on a sparse queue:%n%s", plan)
                .isEqualTo(DRAIN_INDEX)
            assertThat(plan)
                .describedAs("the new index must not steal the drain:%n%s", plan)
                .doesNotContain(INDEX)
            assertThat(plan)
                .describedAs("the LIMIT must stop the scan, not trim a sort:%n%s", plan)
                .doesNotContain("Sort")
        }
    }

    // ---- the shape of the index itself -------------------------------------

    @Test
    fun `the index is partial on drained_at and is deliberately NOT unique`() {
        val def = indexDef()
        assertThat(def).describedAs("%s must exist after V16", INDEX).isNotNull()
        val (partial, unique) =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT (i.indpred IS NOT NULL) AS partial, i.indisunique AS is_unique
                    FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
                    WHERE c.relname = :n
                    """,
                ).bind("n", INDEX)
                    .map { rs, _ -> rs.getBoolean("partial") to rs.getBoolean("is_unique") }.one()
            }
        // Asserted through pg_index.indpred rather than by counting rows
        // (AGENT.md).
        assertThat(partial).describedAs("pg_index.indpred").isTrue()
        assertThat(def).contains("drained_at IS NULL")
        // The KEY, in order, resolved through pg_index.indkey. NOT a
        // substring of pg_get_indexdef: the index is NAMED
        // `hog_file_removal_undrained_path`, so `contains("catalog_id",
        // "path")` — which is what this asserted first — was satisfied
        // by the name alone and would have passed over an index on any
        // columns at all, in any order. Order is the half that matters:
        // `(path, catalog_id)` would serve the guard and be useless to
        // every other predicate this table has.
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
                ).bind("n", INDEX).mapTo(String::class.java).list()
            }
        assertThat(key)
            .describedAs("pg_index.indkey, resolved to attribute names in order")
            .containsExactly("catalog_id", "path")
        // The decision, pinned where a future reader will find it. The
        // next test is WHY; the migration carries the full argument.
        assertThat(unique)
            .describedAs("pg_index.indisunique — a unique index here would wedge the expiry sweep")
            .isFalse()
    }

    @Test
    fun `an expiry sweep can queue one path twice, which is why the index is not unique`() {
        // The reachability proof behind the non-unique decision, and the
        // mutation test for it: make the V16 index UNIQUE and this reds
        // inside the sweep's INSERT rather than on the assertion.
        //
        // Nothing makes a file path unique. hog_data_file is keyed on
        // (catalog_id, data_file_id) and carries no uniqueness on
        // `path`; CommitService's guard rejects only paths the removal
        // queue currently owns, and says so ("Duplicate paths against
        // live/historical file rows stay legal"). So two file rows can
        // share a path, and ExpiryService queues by
        // `DELETE ... RETURNING path` — one statement, two rows, one
        // path. No writer carries `ON CONFLICT`, so under a unique index
        // that is not a skipped row: it aborts the sweep transaction,
        // deterministically, on every retry, and the retention floor
        // stops advancing for good.
        PgTestSupport.freshDatabase(productionSession = true).use { fresh ->
            val path = "s3://v16/shared/one.parquet"
            val cat =
                fresh.jdbi.withHandleUnchecked { h ->
                    val id =
                        h.createQuery(
                            """
                            INSERT INTO hog_catalog (name, data_path, last_snapshot_id,
                                                     snapshot_retention_seconds)
                            VALUES ('v16-dup', 's3://v16/dup', 3, 60) RETURNING catalog_id
                            """,
                        ).mapTo(Long::class.java).one()
                    for (s in 0..3) {
                        h.execute(
                            "INSERT INTO hog_snapshot (catalog_id, snapshot_id, snapshot_time, " +
                                "schema_version) VALUES (?, ?, now() - interval '1 day', 0)",
                            id,
                            s,
                        )
                    }
                    h.execute(
                        "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 0)",
                        id,
                    )
                    // TWO file rows, ONE path, both unreachable at the
                    // floor this sweep will set.
                    for (fileId in 1..2) {
                        h.execute(
                            """
                            INSERT INTO hog_data_file (catalog_id, data_file_id, table_id,
                                                       begin_snapshot, end_snapshot, path,
                                                       record_count, file_size_bytes, row_id_start)
                            VALUES (?, ?, 1, 0, 1, ?, 10, 100, ?)
                            """,
                            id,
                            fileId,
                            path,
                            fileId * 10,
                        )
                    }
                    id
                }

            val result = ExpiryService(fresh.jdbi).runOnce("v16-dup", batchSize = 10)
            assertThat(result.dataFilesQueued)
                .describedAs("one sweep, one INSERT ... SELECT, two rows over one path")
                .isEqualTo(2)
            val undrained =
                fresh.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        "SELECT count(*) FROM hog_file_removal " +
                            "WHERE catalog_id = :c AND path = :p AND drained_at IS NULL",
                    ).bind("c", cat).bind("p", path).mapTo(Int::class.java).one()
                }
            assertThat(undrained)
                .describedAs(
                    "two undrained rows over one path are legitimate state; the drain settles " +
                        "the first 'deleted' and the second 'absent', because the queue is a " +
                        "suggestion and never an authorization",
                )
                .isEqualTo(2)
        }
    }

    // ---- re-entry ----------------------------------------------------------

    @Test
    fun `re-entry is a clean no-op - a repaired history row must not rebuild the index`() {
        // The file runs IN a transaction (V13's shape, no `.conf`), so
        // a failure part way through writes no history row at all and
        // the next pod just runs V16 again. What still has to hold is
        // that a re-run over an index that already exists is a NO-OP:
        // `IF NOT EXISTS` must find it and skip, not drop and rebuild,
        // because the rebuild is the 1.4 s of blocked INSERTs the file
        // is budgeted for exactly once.
        PgTestSupport.freshDatabase(productionSession = true).use { fresh ->
            val before = indexNode(fresh)
            assertThat(before).describedAs("%s exists on a fully-migrated database", INDEX).isNotZero()
            fresh.jdbi.useHandleUnchecked { h ->
                h.execute(REAPPLY_V16)
            }
            Database.migrate(fresh.dataSource)
            assertThat(indexNode(fresh))
                .describedAs("an existing index is not dropped and rebuilt")
                .isEqualTo(before)
        }
    }

    @Test
    fun `an INVALID remnant is cleared rather than skipped forever`() {
        PgTestSupport.freshDatabase(productionSession = true).use { fresh ->
            // Exactly what a cancelled `CREATE INDEX CONCURRENTLY`
            // leaves. V16 does not BUILD concurrently — see the file —
            // but it can still find such a remnant: an operator who
            // pre-built this index by hand and cancelled it, or a pod
            // that ran this file's first (CONCURRENTLY) draft and was
            // killed mid-build. Forged, because killing a real
            // concurrent build mid-flight is not reproducible in a
            // test. Without the DO block, `CREATE INDEX IF NOT EXISTS`
            // matches on NAME, sees the remnant, skips, and leaves an
            // index that every insert maintains and no query may use.
            fresh.jdbi.useHandleUnchecked { h ->
                h.execute(
                    "UPDATE pg_index SET indisvalid = false WHERE indexrelid = " +
                        "(SELECT oid FROM pg_class WHERE relname = ?)",
                    INDEX,
                )
                h.execute(REAPPLY_V16)
            }
            assertThat(indexValid(fresh)).isFalse()

            Database.migrate(fresh.dataSource)

            assertThat(indexValid(fresh))
                .describedAs("the remnant must be dropped and rebuilt, not skipped")
                .isTrue()
        }
    }

    private fun indexNode(target: PgTestSupport.TestDb): Long =
        target.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT relfilenode FROM pg_class WHERE relname = :n")
                .bind("n", INDEX).mapTo(Long::class.java).findOne().orElse(0L)
        }

    private fun indexValid(target: PgTestSupport.TestDb): Boolean =
        target.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid " +
                    "WHERE c.relname = :n",
            ).bind("n", INDEX).mapTo(Boolean::class.javaObjectType).findOne().orElse(false)
        }
}
