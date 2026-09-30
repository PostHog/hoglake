package com.posthog.hoglake.compaction

import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.MaintenanceSummarySampler
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * The planner's candidate fetch on a PRODUCTION-SHAPED fixture — the
 * test the doctrine's "prove the plan on a production-shaped fixture"
 * clause asks for, and the one whose absence hid the first version of
 * this change's defect.
 *
 * # What production looks like, and what the old fixture did not
 *
 * gigahog-prod-us, 2026-09-30: ONE catalog, many tables, ~10M live
 * files on `ingest.events_raw` across ~2,800 partitions — **and the
 * partitions are not the same size**. ~2,790 stray-day buckets hold
 * ~3.5k files each of ~12 KiB (200 rows), and a handful of fresh-day
 * buckets hold ~100k files each of up to 200 MB. Every day-partitioned
 * table in the catalog writes the SAME day strings.
 *
 * `V23PartitionValueLookupMigrationIntegrationTest`'s fixture is
 * 200,000 files in 2,000 uniform buckets of 100, one table per catalog.
 * It is the right fixture for proving what V23's index does to a probe,
 * and it is blind to three things that only appear with skew:
 *
 *  - a bucket that is a large FRACTION of the table, where a plan
 *    bounded "by the bucket" is not bounded by anything useful;
 *  - `hog_file_partition_value` having no `table_id`, so a probe by day
 *    value is CATALOG-scoped and returns every table's files for that
 *    day;
 *  - the interaction between the two, which is what decides whether
 *    Postgres drives the ordered scan or the semi-join.
 *
 * So: three tables in one catalog sharing day values, and four bucket
 * kinds on the subject table — [TINY_BUCKETS] stray-day buckets of
 * [TINY_PER_BUCKET] tiny files, ONE stray-day bucket of [BIG_FILES]
 * tiny files (bigger than the fetch cap), [FRESH_BUCKETS] fresh-day
 * buckets of [FRESH_PER_BUCKET] files three orders of magnitude bigger,
 * and [totalFiles] file rows in all.
 *
 * # THE PROPERTY BEING PROVEN
 *
 * `CompactionService.candidateSql` has plans available and Postgres
 * picks between them: drive V10's ordered size index and filter by the
 * `EXISTS` arms (bounded by the statement's `LIMIT`), or rewrite the
 * arms into a semi-join and drive from V23's index (bounded by the
 * bucket). The assertion is therefore NOT "the plan is X" — pinning one
 * plan is what the first version's test did, and it pinned the
 * unbounded one — but that whatever Postgres picks, the work is bounded
 * by the SMALLER of those two bounds and never by the table.
 *
 * # THE ONE SHAPE THAT IS NOT BOUNDED, AND WHY IT IS UNREACHABLE
 *
 * Measured here: a probe for a bucket whose files are the table's
 * LARGEST, with a cap smaller than the bucket, gets neither bound. The
 * ordered scan would walk past every smaller file in the table, so
 * Postgres costs it correctly and picks a whole-table plan instead (a
 * hash join over a sequential scan). That combination is real and this
 * fixture can produce it.
 *
 * What makes it unreachable in the planner is the BUCKET ORDERING:
 * `CompactionService.sampledBuckets` orders by bytes-per-file
 * ascending, so a bucket of large files is probed only once the table
 * has nothing smaller left — at which point its files ARE the smallest
 * and the ordered scan stops at the cap. The ordering and the plan's
 * bound are one decision, and the test at the end of this class is what
 * pins it.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompactionCandidateFetchPlanIntegrationTest {
    private companion object {
        const val CATALOG = "fetchplan"

        /**
         * Rows of the subject table, assigned to a bucket kind by
         * `data_file_id % 7` so the three populations are INTERLEAVED in
         * the heap rather than clustered. gcd(7, TINY_BUCKETS) = 1, so
         * every (residue mod 7, residue mod 2000) pair occurs exactly
         * ten times in 140,000 rows and the per-bucket counts below are
         * EXACT rather than approximate.
         */
        const val SUBJECT_FILES = 140_000

        /** Residues 4,5,6: stray-day buckets, 200 rows / 12 KiB each. */
        const val TINY_BUCKETS = 2_000
        const val TINY_PER_BUCKET = 30
        const val TINY_BYTES = 12L * 1024
        const val TINY_ROWS = 200L

        /**
         * Residue 3: ONE stray-day bucket bigger than the fetch cap.
         * Tiny files, so the ordering reaches it FIRST — which is the
         * only way a bucket larger than the cap can be probed.
         */
        const val BIG_BUCKET = "day-big"
        const val BIG_FILES = 20_000

        /**
         * Residues 0,1,2: fresh-day buckets, three orders of magnitude
         * bigger per file. Ordered LAST by bytes-per-file, which is what
         * keeps the unbounded plan shape unreachable — see the test that
         * asserts it.
         */
        const val FRESH_BUCKETS = 3
        const val FRESH_PER_BUCKET = 20_000
        const val FRESH_BYTES = 200L * 1024 * 1024
        const val FRESH_ROWS = 4_000_000L

        /** Files per NEIGHBOUR table, over the same day values. */
        const val NEIGHBOUR_FILES = 35_000

        /** Neighbour tables sharing the subject's day values. */
        val NEIGHBOURS = listOf("t2", "t3")

        val totalFiles: Int
            get() = SUBJECT_FILES + NEIGHBOURS.size * NEIGHBOUR_FILES
    }

    private val db = PgTestSupport.freshDatabase()

    /** Planning is metadata-only; any object-store contact must fail loudly. */
    private val store =
        ObjectStore(
            endpoint = "http://127.0.0.1:9",
            region = "us-east-1",
            accessKey = "unused",
            secretKey = "unused",
            pathStyle = true,
        )

    /**
     * Production's knobs, except `maxCandidates`, which is lowered so
     * the cap is REACHABLE on a fixture a test can seed: the property is
     * "the read stops at the cap", and a cap above the fixture's file
     * count would prove nothing.
     */
    private val cfg =
        CompactionConfig(
            targetBytes = 512L * 1024 * 1024,
            minInputFiles = 5,
            maxInputFiles = 64,
            maxGroupsPerRun = 64,
            maxCandidates = 8_192,
        )

    private var catalogId = 0L
    private var tableId = 0L

    @AfterAll
    fun tearDown() {
        store.close()
        db.close()
    }

    @BeforeAll
    fun seed() {
        catalogId =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path, last_snapshot_id) " +
                        "VALUES (:n, 's3://fetchplan', 0) RETURNING catalog_id",
                ).bind("n", CATALOG).mapTo(Long::class.java).one()
            }
        db.jdbi.useHandleUnchecked { h ->
            h.execute("INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version) VALUES (?, 0, 0)", catalogId)
        }
        val catalogs = CatalogService(db.jdbi)
        catalogs.createNamespace(CATALOG, "ns")
        for (name in listOf("t1") + NEIGHBOURS) {
            catalogs.createTable(
                CATALOG,
                "ns",
                name,
                listOf(ColumnDef("id", ColType.LONG), ColumnDef("day", ColType.STRING)),
            )
        }
        tableId = tableIdOf("t1")
        db.jdbi.useHandleUnchecked { h ->
            for (id in listOf(tableId) + NEIGHBOURS.map { tableIdOf(it) }) {
                h.execute(
                    "INSERT INTO hog_partition_spec (catalog_id, table_id, spec_id, begin_snapshot) " +
                        "VALUES (?, ?, 1, 0)",
                    catalogId,
                    id,
                )
                h.execute(
                    "INSERT INTO hog_partition_field " +
                        "(catalog_id, table_id, spec_id, key_index, source_field_id, transform) " +
                        "VALUES (?, ?, 1, 0, 2, 'identity')",
                    catalogId,
                    id,
                )
            }
            // THE SUBJECT TABLE, in one generate_series so the three
            // populations are INTERLEAVED in the heap rather than
            // clustered — AGENT.md records what a clustered fixture does
            // to a measurement of this kind.
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start,
                                           spec_id)
                SELECT :c,
                       g,
                       :t,
                       0,
                       's3://fetchplan/' || md5(g::text) || '/part-' || g || '.parquet',
                       CASE WHEN g % 7 < 3 THEN :freshRows ELSE :tinyRows END,
                       CASE WHEN g % 7 < 3 THEN :freshBytes ELSE :tinyBytes END,
                       g::bigint * 1000000,
                       1
                FROM generate_series(1, :n) g
                """,
            )
                .bind("c", catalogId).bind("t", tableId)
                .bind("freshRows", FRESH_ROWS).bind("tinyRows", TINY_ROWS)
                .bind("freshBytes", FRESH_BYTES).bind("tinyBytes", TINY_BYTES)
                .bind("n", SUBJECT_FILES)
                .execute()
            // The neighbours, over the same day values, so a probe by
            // day value is not table-scoped by accident.
            var nextId = SUBJECT_FILES.toLong() + 1
            for (name in NEIGHBOURS) {
                val id = tableIdOf(name)
                h.createUpdate(
                    """
                    INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                               path, record_count, file_size_bytes, row_id_start,
                                               spec_id)
                    SELECT :c, :base + g, :t, 0,
                           's3://fetchplan/' || md5((:base + g)::text) || '/n-' || g || '.parquet',
                           :rows, :bytes, g::bigint * 1000000, 1
                    FROM generate_series(1, :n) g
                    """,
                )
                    .bind("c", catalogId).bind("t", id).bind("base", nextId - 1)
                    .bind("rows", TINY_ROWS).bind("bytes", TINY_BYTES)
                    .bind("n", NEIGHBOUR_FILES).execute()
                nextId += NEIGHBOUR_FILES
            }
            // One partition value per file, from `data_file_id % 7` on
            // the subject table and the tiny day strings on the
            // neighbours — so the day values are SHARED across tables
            // and a probe by value is not table-scoped by accident.
            h.createUpdate(
                """
                INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value)
                SELECT f.catalog_id, f.data_file_id, 0,
                       CASE
                           WHEN f.table_id <> :subject
                               THEN 'day-' || lpad((f.data_file_id % :tinyBuckets)::text, 5, '0')
                           WHEN f.data_file_id % 7 < 3
                               THEN 'fresh-' || (f.data_file_id % 7)::text
                           WHEN f.data_file_id % 7 = 3
                               THEN :bigBucket
                           ELSE 'day-' || lpad((f.data_file_id % :tinyBuckets)::text, 5, '0')
                       END
                FROM hog_data_file f
                """,
            )
                .bind("subject", tableId)
                .bind("tinyBuckets", TINY_BUCKETS)
                .bind("bigBucket", BIG_BUCKET)
                .execute()
            for (relation in listOf("hog_data_file", "hog_file_partition_value")) {
                h.execute("VACUUM (ANALYZE) $relation")
            }
        }
    }

    private fun tableIdOf(name: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT tv.table_id FROM hog_table_version tv WHERE tv.catalog_id = :c AND tv.name = :n",
            ).bind("c", catalogId).bind("n", name).mapTo(Long::class.java).one()
        }

    /** How many live candidate files the subject table has in [value]'s bucket. */
    private fun bucketSize(value: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT count(*) FROM hog_data_file f
                JOIN hog_file_partition_value pv
                  ON pv.catalog_id = f.catalog_id AND pv.data_file_id = f.data_file_id
                WHERE f.catalog_id = :c AND f.table_id = :t AND pv.key_index = 0 AND pv.value = :v
                """,
            ).bind("c", catalogId).bind("t", tableId).bind("v", value).mapTo(Long::class.java).one()
        }

    /** The same bucket across the WHOLE catalog, which is what a value probe sees. */
    private fun catalogBucketSize(value: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT count(*) FROM hog_file_partition_value WHERE catalog_id = :c " +
                    "AND key_index = 0 AND value = :v",
            ).bind("c", catalogId).bind("v", value).mapTo(Long::class.java).one()
        }

    /** EXPLAIN the planner's own statement for one bucket value. */
    private fun explainBucket(
        value: String,
        limit: Int = cfg.candidateBudget,
    ): String {
        val svc = CompactionService(db.jdbi, store, cfg)
        val sql =
            svc.candidateSql(
                arms = listOf(0),
                nullArms = emptySet(),
                scopeToSpec = true,
                withPartitionValues = false,
                boundRows = false,
            )
        return db.jdbi.withHandleUnchecked { h ->
            h.begin()
            try {
                // Serial plans only: a Gather splits the buffer counts
                // across workers, and the budgets below would then be
                // measuring the worker count.
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                h.createQuery("EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) $sql")
                    .bind("catalogId", catalogId)
                    .bind("tableId", tableId)
                    .bind("targetBytes", cfg.targetBytes)
                    .bind("specId", 1L)
                    .bind("scanLimit", limit)
                    .bind("limit", limit)
                    .bind("k0", 0)
                    .bind("v0", value)
                    .mapTo(String::class.java).list().joinToString("\n")
            } finally {
                h.rollback()
            }
        }
    }

    /** EXPLAIN the no-published-generation fallback. */
    private fun explainFallback(): String {
        val svc = CompactionService(db.jdbi, store, cfg)
        val sql =
            svc.candidateSql(
                arms = emptyList(),
                nullArms = emptySet(),
                scopeToSpec = false,
                withPartitionValues = true,
                boundRows = false,
            )
        return db.jdbi.withHandleUnchecked { h ->
            h.begin()
            try {
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                h.createQuery("EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) $sql")
                    .bind("catalogId", catalogId)
                    .bind("tableId", tableId)
                    .bind("targetBytes", cfg.targetBytes)
                    .bind("scanLimit", cfg.maxCandidates)
                    .bind("limit", cfg.maxCandidates)
                    .mapTo(String::class.java).list().joinToString("\n")
            } finally {
                h.rollback()
            }
        }
    }

    /**
     * The bucket values in the order `CompactionService.sampledBuckets`
     * returns them, read through the same SQL ordering rather than
     * restated — the planner probes in this order, so this IS the
     * fairness and boundedness property.
     */
    private fun sampledOrder(): List<String> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT p.partition_values[1] AS value
                  FROM hog_maintenance_summary_tier p
                  JOIN hog_maintenance_summary s
                    ON s.catalog_id = p.catalog_id AND s.published_generation = p.generation
                 WHERE p.catalog_id = :c AND p.table_id = :t
                 GROUP BY p.spec_id, p.partition_values
                HAVING sum(p.selected) >= :min
                 ORDER BY sum(p.small_bytes)::float8 / GREATEST(sum(p.small_count), 1) ASC,
                          sum(p.selected) DESC,
                          sum(p.small_count) DESC
                """,
            ).bind("c", catalogId).bind("t", tableId).bind("min", cfg.minInputFiles.toLong())
                .mapTo(String::class.java).list()
        }

    /**
     * Rows the plan's SCAN nodes produced, excluding the root's — the
     * work a filter above them then discarded or the `LIMIT` cut off.
     *
     * `Rows Removed by Filter` alone misses it: a `LIMIT` that stops a
     * scan early leaves rows that were read, matched, and never
     * returned by the outer node.
     */
    private fun scannedRows(plan: String): Long =
        plan.split("\n")
            .filter { it.contains("Scan on hog_data_file") || it.contains("Scan using hog_data_file") }
            .sumOf { line ->
                Regex("""actual rows=([\d.]+)""").find(line)?.groupValues?.get(1)?.toDouble()?.toLong() ?: 0L
            }

    private fun rowsRemoved(plan: String): Long =
        Regex("""Rows Removed by Filter: (\d+)""").findAll(plan).sumOf { it.groupValues[1].toLong() }

    /** Total shared buffers the PLAN's own root reports (its whole cost). */
    private fun buffers(plan: String): Long {
        // The root node's Buffers line, which is the statement's total.
        // Taken from the root rather than summed across nodes, because
        // a child's buffers are included in its parent's.
        val line =
            plan.split("\n").firstOrNull { it.contains("Buffers:") }
                ?: error("no Buffers line in:\n$plan")
        return Regex("""shared (?:hit|read)=(\d+)""").findAll(line).sumOf { it.groupValues[1].toLong() }
    }

    private fun actualRows(plan: String): Long =
        Regex("""actual rows=([\d.]+)""").find(plan)!!.groupValues[1].toDouble().toLong()

    private fun heapPages(relation: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT relpages FROM pg_class WHERE relname = :n")
                .bind("n", relation).mapTo(Long::class.java).one()
        }

    // ---- the fixture is the shape it claims to be -----------------------------

    @Test
    fun `the fixture has production's skew, shared day values and at least 200k files`() {
        // Asserted rather than assumed: every bound below is stated
        // against these numbers, so a fixture that quietly stopped being
        // skewed would make the bounds vacuous.
        assertThat(totalFiles).describedAs("the doctrine's 100k floor, doubled").isGreaterThanOrEqualTo(200_000)
        assertThat(
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT count(*) FROM hog_data_file WHERE catalog_id = :c")
                    .bind("c", catalogId).mapTo(Long::class.java).one()
            },
        ).isEqualTo(totalFiles.toLong())

        val tiny = bucketSize("day-00007")
        val big = bucketSize(BIG_BUCKET)
        val fresh = bucketSize("fresh-1")
        assertThat(tiny).describedAs("a stray-day bucket").isEqualTo(TINY_PER_BUCKET.toLong())
        assertThat(big).describedAs("the over-cap stray bucket").isEqualTo(BIG_FILES.toLong())
        assertThat(fresh).describedAs("a fresh-day bucket").isEqualTo(FRESH_PER_BUCKET.toLong())
        assertThat(big)
            .describedAs("the big bucket must exceed the fetch cap, or it proves nothing")
            .isGreaterThan(cfg.candidateBudget.toLong())
        assertThat(fresh / tiny)
            .describedAs("the population skew: production's is ~28x, this fixture's is stronger")
            .isGreaterThan(100)
        assertThat(FRESH_BYTES / TINY_BYTES)
            .describedAs("and the per-file skew, which is what the ordering sorts on")
            .isGreaterThan(1_000)

        // The day strings repeat across tables, so a probe by VALUE is
        // catalog-scoped and returns more than the subject table's
        // files. This is the property that made the INTERSECT form's
        // bound wrong, and nothing in a one-table-per-catalog fixture
        // can see it.
        assertThat(catalogBucketSize("day-00007"))
            .describedAs("a day value is shared by three tables")
            .isGreaterThan(tiny)
    }

    // ---- the bound, on both bucket shapes -------------------------------------

    @Test
    fun `a selective bucket's fetch is bounded by the bucket, not by the table`() {
        val value = "day-00007"
        val plan = explainBucket(value)
        val examined = actualRows(plan) + rowsRemoved(plan)

        assertThat(plan)
            .describedAs("the manifest is never scanned end to end:\n%s", plan)
            .doesNotContain("Seq Scan on hog_data_file")
        assertThat(actualRows(plan))
            .describedAs("the whole bucket, which is under the cap")
            .isEqualTo(TINY_PER_BUCKET.toLong())
        // Bounded by the CATALOG-WIDE bucket (three tables share the day
        // value) plus the cap, and derived from the fixture rather than
        // written as a constant. What matters is the comparison below.
        assertThat(examined)
            .describedAs("rows examined must be bounded by the bucket, not the table:\n%s", plan)
            .isLessThanOrEqualTo(catalogBucketSize(value) + cfg.candidateBudget)
        assertThat(examined)
            .describedAs("and that bound must be far under the table's file count")
            .isLessThan(totalFiles / 10L)
        assertThat(buffers(plan))
            .describedAs("buffers must not approach a scan of the manifest:\n%s", plan)
            .isLessThan(heapPages("hog_data_file"))
    }

    @Test
    fun `a bucket larger than the cap stops at the cap, not at the bucket`() {
        // The case the uniform fixture cannot pose: 20,000 candidate
        // files against a cap of 8,192.
        //
        // WHAT IS BOUNDED, and it is not "no sequential scan". The
        // manifest may well be scanned — that is often the cheapest way
        // to find one bucket's rows — and what matters is that the scan
        // STOPS. With no inner `ORDER BY` the `LIMIT` propagates into
        // whichever plan Postgres picks, so the work is
        // `min(the bucket, the cap / the bucket's selectivity)` and
        // never the table twice. The first version of this statement had
        // an inner sort, so the same probe read all 140,000 rows and
        // spilled an external merge sort to disk before the `LIMIT` saw
        // anything.
        val plan = explainBucket(BIG_BUCKET)
        val bucket = bucketSize(BIG_BUCKET)

        assertThat(actualRows(plan))
            .describedAs("the statement returns exactly its cap")
            .isEqualTo(cfg.candidateBudget.toLong())
        // The analytic bound, derived from the fixture rather than
        // written down: to find `cap` rows of a bucket holding
        // `bucket / subject` of the table, a manifest-driven plan reads
        // `cap x subject / bucket` of them, and a bucket-driven plan
        // reads the bucket. Postgres may pick either; neither is the
        // table.
        val analytic = cfg.candidateBudget.toLong() * SUBJECT_FILES / bucket + bucket
        val examined = actualRows(plan) + rowsRemoved(plan) + scannedRows(plan)
        assertThat(examined)
            .describedAs(
                "rows examined against the analytic bound %d (cap %d, bucket %d, table %d):\n%s",
                analytic,
                cfg.candidateBudget,
                bucket,
                SUBJECT_FILES,
                plan,
            )
            .isLessThanOrEqualTo(analytic * 2)
        // And in buffers: at most ONE pass of the manifest, which is the
        // ceiling on any plan. The pre-fix shape was 5,429 buffers plus
        // 379 temp blocks written for the spilled sort; this is ~1,656
        // with nothing spilled.
        assertThat(buffers(plan))
            .describedAs("no plan may cost more than one pass of the manifest:\n%s", plan)
            .isLessThanOrEqualTo(heapPages("hog_data_file"))
        assertThat(plan)
            .describedAs("and nothing may spill: the inner sort is what used to:\n%s", plan)
            .doesNotContain("external merge")
    }

    @Test
    fun `a bucket of the table's LARGEST files has no cheap plan, which is what the ordering avoids`() {
        // THE HONEST NEGATIVE RESULT, recorded rather than hidden.
        //
        // Probe a fresh-day bucket — 20,000 files of 200 MB, the
        // table's largest — with a cap below the bucket. Neither bound
        // applies: the ordered scan would walk every smaller file in the
        // table to reach these, so Postgres costs it correctly and picks
        // a whole-table plan instead. Measured on this fixture: a hash
        // join whose outer side is a sequential scan of the subject
        // table, ~140,000 rows read.
        //
        // This is not a defect in the statement; it is the statement
        // being asked for the one thing a size-ordered access path
        // cannot do cheaply. What removes it is the BUCKET ORDERING,
        // which is asserted below — so this test exists to pin the cost
        // of the combination and to fail if a future ordering change
        // makes it reachable while smaller files exist.
        val plan = explainBucket("fresh-1")
        val examined = actualRows(plan) + rowsRemoved(plan)
        assertThat(examined)
            .describedAs("recorded, not asserted as good: this probe reads the table:\n%s", plan)
            .isGreaterThan(SUBJECT_FILES / 2L)

        // AND THE ORDERING MAKES IT UNREACHABLE. The sample's own order
        // is what the planner probes in, so a bucket of 200 MB files
        // must come after every bucket of 12 KiB files.
        val order = sampledOrder()
        val firstFresh = order.indexOfFirst { it.startsWith("fresh-") }
        val lastTiny = order.indexOfLast { it.startsWith("day-") }
        assertThat(firstFresh).describedAs("the fresh buckets are in the sample").isNotEqualTo(-1)
        assertThat(lastTiny).describedAs("so are the stray ones").isNotEqualTo(-1)
        assertThat(firstFresh)
            .describedAs(
                "every 12 KiB bucket must be ordered before every 200 MB one, or the plan above " +
                    "becomes reachable while cheaper work exists (order head: %s)",
                order.take(5),
            )
            .isGreaterThan(lastTiny)
        // And the bucket the cap cannot finish is ordered FIRST among
        // the tiny ones it ties with, because it has the most actionable
        // work — `selected DESC` is the second key.
        assertThat(order.first())
            .describedAs("the biggest actionable stray bucket leads: %s", order.take(5))
            .isEqualTo(BIG_BUCKET)
    }

    @Test
    fun `the no-sample fallback stops at its cap and runs the tuple aggregate at most that often`() {
        val plan = explainFallback()
        assertThat(actualRows(plan))
            .describedAs("the fallback returns exactly its cap")
            .isEqualTo(cfg.maxCandidates.toLong())
        assertThat(plan)
            .describedAs("V10's index serves the order, so the scan stops at the cap:\n%s", plan)
            .contains("hog_data_file_maintenance_size_scan")
        assertThat(plan)
            .describedAs("no sequential scan of the manifest:\n%s", plan)
            .doesNotContain("Seq Scan on hog_data_file")
        // The correlated tuple aggregate is what made the replaced
        // statement 9.9M subqueries. It now sits OUTSIDE the inner
        // LIMIT, so it runs at most `maxCandidates` times: the lateral's
        // loop count is the assertion, and it is the number the cap
        // sets rather than the table's file count.
        assertThat(plan)
            .describedAs("the tuple aggregate must be bounded by the limit, not by the table:\n%s", plan)
            .contains("loops=${cfg.maxCandidates}")
        assertThat(plan)
            .describedAs("and it reaches the values by their primary key:\n%s", plan)
            .contains("hog_file_partition_value_pkey")
    }

    // ---- and the planner as a whole -------------------------------------------

    @Test
    fun `a whole table plan reads at most its budget and does it in a sweep's worth of time`() {
        // The sampler publishes a generation over the fixture, so the
        // planner takes the bucket-scoped path it takes in production.
        val sampler =
            MaintenanceSummarySampler(db.jdbi, cfg.targetBytes, cfg.minInputFiles, cfg.maxInputFiles, 3600)
        var steps = 0
        while (sampler.runOnce(50_000)) check(++steps < 2_000)
        db.jdbi.useHandleUnchecked { h -> h.execute("VACUUM (ANALYZE) hog_maintenance_summary_tier") }

        val svc = CompactionService(db.jdbi, store, cfg)
        val started = System.nanoTime()
        val plan = svc.planTable(CATALOG, "ns", "t1", cfg)
        val elapsedMs = (System.nanoTime() - started) / 1_000_000

        assertThat(plan.candidatesFetched)
            .describedAs("bounded by the budget, whatever the table holds")
            .isLessThanOrEqualTo(cfg.candidateBudget.toLong())
        assertThat(plan.groups).describedAs("and it plans real work").isNotEmpty()
        assertThat(plan.bucketsConsidered)
            .describedAs("the statement count is bounded too")
            .isLessThanOrEqualTo((cfg.candidateBudget / cfg.minInputFiles).toLong())
        // WALL TIME, not a throughput claim: a group costs a measured
        // ~8.5 s of object-store latency, so a plan is only the cheap
        // half of a sweep if it stays far under one group. The bound is
        // deliberately loose — this runs on a laptop and in CI — and its
        // job is to red if planning ever becomes the expensive half
        // again, which is what the incident was.
        assertThat(elapsedMs)
            .describedAs("plan wall time on a %d-file table", totalFiles)
            .isLessThan(CompactionService.PLAN_STATEMENT_TIMEOUT_MS)
        // And every group is inside one bucket, which is what the
        // bucket-first design rests on.
        for (group in plan.groups) {
            assertThat(group.partitionValues).hasSize(1)
        }
    }
}
