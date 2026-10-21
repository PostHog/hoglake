package com.posthog.hoglake.compaction

import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.MaintenanceSummarySampler
import com.posthog.hoglake.testing.ExplainPlan
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
 * `CompactionService.candidateSql` with arms plans as ONE shape on PG
 * 18: the bucket's ids come from V23's index as an InitPlan, and the
 * manifest is reached from them by an index condition
 * (`data_file_id = ANY (...)`), with the `LIMIT` stopping that scan.
 * The statement it replaced was a statistics call between that direction
 * and a MANIFEST-DRIVEN plan with the arms as filters, and the
 * manifest-driven plan is O(table) for any bucket that is small or late
 * in the walk — which is what the bucket loop multiplied into 18-37
 * minute plans on gigahog-prod-us (2026-10-21).
 *
 * WHY IT IS ONE SHAPE, and it is costing, not plan space: a
 * manifest-driven plan over the InitPlan still exists (with index scans
 * disabled PG 18 plans a `Seq Scan on hog_data_file` filtering on the
 * array), but the array is a Param with no statistics, and
 * `estimate_array_length()` returns its default of 10 for one, so the
 * `= ANY` scan is always costed as ~10 descents and no statistic on
 * `value` can flip it. That is a PG 18 property: a Postgres that
 * estimates Param arrays from the subplan's rows restores a
 * statistics-driven choice, and these assertions are what would notice.
 *
 * So the assertion here is the shape AND the bound: the probe drives
 * from the lookup index in its custom and its generic plan, and its work
 * is bounded by the bucket (catalog-wide, because the arms are) and the
 * cap, never by the table.
 *
 * # THE SHAPE THAT USED TO BE UNBOUNDED
 *
 * Measured here: a probe for a bucket whose files are the table's
 * LARGEST, with a cap smaller than the bucket. The replaced statement
 * planned it as a hash join whose outer side is a `Seq Scan on
 * hog_data_file`, walked until the `LIMIT` filled — 57,342 rows for
 * 8,192 here, and the whole table for a bucket under the cap. The
 * replacement reads the bucket's 20,000 index entries and stops the
 * primary-key scan at the cap. The bucket ORDERING
 * (`CompactionService.sampledBuckets`, bytes-per-file ascending) used
 * to be what kept that probe unreachable; it is now a VALUE ordering
 * only, and the test at the end of this class still pins it.
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
            // LIVE DVs on every third file, so the DV join's per-row cost
            // is measured on real statistics. With `hog_delete_file` empty
            // and unanalyzed its plan flipped between one materialized
            // scan and a probe per row (QE, #397), and the buffer bounds
            // below were passing on whichever the planner happened to pick.
            h.createUpdate(
                """
                INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id,
                                             begin_snapshot, path, delete_count, file_size_bytes)
                SELECT f.catalog_id, f.data_file_id, f.table_id, f.data_file_id, 1,
                       's3://fetchplan/dv-' || f.data_file_id || '.puffin', 3, 100
                  FROM hog_data_file f
                 WHERE f.catalog_id = :c AND f.data_file_id % 3 = 0
                """,
            ).bind("c", catalogId).execute()
            for (relation in listOf("hog_data_file", "hog_file_partition_value", "hog_delete_file")) {
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

    /** The planner's own one-key bucket probe. */
    private val shippedSql: String
        get() =
            CompactionService(db.jdbi, store, cfg).candidateSql(
                arms = listOf(0),
                nullArms = emptySet(),
                scopeToSpec = true,
                withPartitionValues = false,
            )

    /**
     * EXPLAIN [sql] for one bucket value: the planner's own statement by
     * default, or [ReplacedCandidateSql] for the before measurement.
     */
    private fun explainBucket(
        value: String,
        limit: Int = cfg.candidateBudget,
        sql: String = shippedSql,
    ): String {
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

    /**
     * The GENERIC plan of the shipped probe, forced through PREPARE +
     * `force_generic_plan` (NamespaceListingQueryPlanIntegrationTest's
     * mechanism). Production never runs it — the probe carries
     * `Pg.CUSTOM_PLANS_ONLY` — but its shape must not depend on that.
     */
    private fun explainBucketGeneric(value: String): String =
        db.jdbi.withHandleUnchecked { h ->
            h.begin()
            try {
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                h.execute("SET LOCAL plan_cache_mode = force_generic_plan")
                val positional =
                    listOf("catalogId", "tableId", "targetBytes", "specId", "scanLimit", "limit", "k0", "v0")
                        .foldIndexed(shippedSql) { i, s, name -> s.replace(Regex(":$name\\b"), "\\$${i + 1}") }
                h.execute("PREPARE fetch_generic(bigint, bigint, bigint, bigint, int, int, int, text) AS $positional")
                try {
                    val limit = cfg.candidateBudget
                    // Literals, not binds: EXPLAIN EXECUTE takes no parameters.
                    h.createQuery(
                        "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " +
                            "EXECUTE fetch_generic($catalogId, $tableId, ${cfg.targetBytes}, 1, " +
                            "$limit, $limit, 0, '$value')",
                    ).mapTo(String::class.java).list().joinToString("\n")
                } finally {
                    // PREPARE is not transactional: the rollback below
                    // would leave it on the pooled connection.
                    h.execute("DEALLOCATE fetch_generic")
                }
            } finally {
                h.rollback()
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
    private fun sampledOrder(): List<String> {
        publishSample()
        return sampledOrderOfPublished()
    }

    /**
     * Run the real sampler to a published generation. Called by every
     * test that reads the sample, rather than relying on another test
     * having run first: JUnit's method order is not this class's to
     * assume, and a rename once made the ordering test read an empty
     * sample. Idempotent: a second pass publishes the same buckets.
     */
    private fun publishSample() {
        val sampler =
            MaintenanceSummarySampler(db.jdbi, cfg.targetBytes, cfg.minInputFiles, cfg.maxInputFiles, 3600)
        var steps = 0
        while (sampler.runOnce(50_000)) check(++steps < 2_000)
        db.jdbi.useHandleUnchecked { h -> h.execute("VACUUM (ANALYZE) hog_maintenance_summary_tier") }
    }

    private fun sampledOrderOfPublished(): List<String> =
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
        assertBucketDriven(plan)
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

    /**
     * The one shape an arm'd probe has: the bucket's ids from V23's index
     * as an InitPlan, and the manifest scan DRIVEN by them — the id set is
     * an Index Cond of that scan, not a filter on a walk.
     *
     * Which index carries the Index Cond is the planner's: usually the
     * primary key, and on THIS fixture's generic plan for a small bucket
     * V10's size index, with the id array as a condition on its last
     * column (a Postgres 18 skip scan, 63 index searches for 30 rows).
     * Both are bounded by the id set; the second also by the table's
     * distinct file sizes, which here are one per bucket kind. Production
     * runs the custom plan (`Pg.CUSTOM_PLANS_ONLY`), and on
     * `CompactionBucketProbePlanIntegrationTest`'s varied sizes both plans
     * take the primary key.
     */
    private fun assertBucketDriven(plan: String) {
        assertThat(plan)
            .describedAs("the bucket's ids come from V23's index, once:\n%s", plan)
            .contains("InitPlan 1")
            .contains("Index Only Scan using hog_file_partition_value_lookup")
        val lines = plan.lines()
        val at = lines.indexOfFirst { it.contains(" on hog_data_file f0") }
        assertThat(at).describedAs("no manifest scan in:\n%s", plan).isNotEqualTo(-1)
        // MUTATION: put the per-key `EXISTS` arms back on the manifest
        // scan (ReplacedCandidateSql) and this fails: no InitPlan, and
        // for the fresh-day probe below a sequential scan.
        assertThat(lines[at])
            .describedAs("the manifest is an index scan, never walked:\n%s", plan)
            .contains("Index Scan using hog_data_file_")
        assertThat(lines[at + 1])
            .describedAs("driven BY the id set, as an index condition:\n%s", plan)
            .contains("Index Cond:")
            .contains("data_file_id = ANY ((InitPlan 1).col1)")
        assertThat(plan)
            .describedAs("no manifest-driven plan anywhere:\n%s", plan)
            .doesNotContain("Seq Scan on hog_data_file")
    }

    @Test
    fun `the generic plan has the same shape, for every bucket kind`() {
        // The probe is never server-prepared (`Pg.CUSTOM_PLANS_ONLY`), so
        // production runs the custom plan. The generic plan is asserted
        // anyway, because the SHAPE must not depend on that: a generic
        // plan cannot see the value's statistics, and the replaced
        // statement's choice between directions was a statistics call.
        for (value in listOf("day-00007", BIG_BUCKET, "fresh-1")) {
            val plan = explainBucketGeneric(value)
            assertBucketDriven(plan)
            assertThat(actualRows(plan))
                .describedAs("generic %s returns what custom does:\n%s", value, plan)
                .isEqualTo(minOf(bucketSize(value), cfg.candidateBudget.toLong()))
        }
    }

    @Test
    fun `a bucket larger than the cap stops at the cap, not at the bucket`() {
        // The case the uniform fixture cannot pose: 20,000 candidate
        // files against a cap of 8,192.
        //
        // WHAT IS BOUNDED: the InitPlan reads the bucket's index entries
        // (all of them — an array is built whole), and the primary-key
        // scan over them STOPS at the cap, because there is no inner
        // `ORDER BY` for the `LIMIT` to wait on. The first version of this
        // statement had an inner sort, so the same probe read all 140,000
        // rows and spilled an external merge sort to disk before the
        // `LIMIT` saw anything.
        val plan = explainBucket(BIG_BUCKET)
        val bucket = bucketSize(BIG_BUCKET)

        assertThat(actualRows(plan))
            .describedAs("the statement returns exactly its cap")
            .isEqualTo(cfg.candidateBudget.toLong())
        assertBucketDriven(plan)
        // The analytic bound, derived from the fixture rather than
        // written down: the bucket's entries plus the cap's manifest
        // rows. Kept at the replaced statement's ceiling (which also
        // admitted a manifest-driven `cap x subject / bucket`), so the
        // two are measured against the same number.
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
        // And in buffers, in TWO terms, because the new statement's cost
        // at the cap is not one term.
        //
        // The CANDIDATE READ — everything but the DV join — is at most
        // one pass of the manifest, the ceiling on any plan. The
        // inner-sort shape was 5,429 buffers plus 379 temp blocks for the
        // spilled sort; this is ~1,880 (20,000 index entries and the
        // cap's 8,192 primary-key rows), nothing spilled.
        //
        // The DV JOIN is a nested loop at any result size, because the
        // outer estimate is the Param-array default (~3 rows) —
        // `CompactionService.candidateSql` regime (c). Measured with a
        // live DV on a third of the files: 19,115 buffers for 8,192
        // probes, ~2.3 per row, where the replaced statement hash-joined
        // (2,860 buffers in all, recorded by the largest-files test). It
        // is bounded per ROW, not by the manifest, and this pins that
        // bound rather than pretending it is the other.
        val dvJoin =
            ExplainPlan.nodes(plan).filter { it.line.contains("on hog_delete_file dv") }.sumOf { it.buffers }
        assertThat(buffers(plan) - dvJoin)
            .describedAs("the candidate read may cost no more than one pass of the manifest:\n%s", plan)
            .isLessThanOrEqualTo(heapPages("hog_data_file"))
        assertThat(dvJoin)
            .describedAs("the DV join is a bounded probe per returned row:\n%s", plan)
            .isLessThanOrEqualTo(4L * actualRows(plan) + 64)
        println("RECORDED over-cap probe: buffers ${buffers(plan)}, of which DV join $dvJoin")
        assertThat(plan)
            .describedAs("and nothing may spill: the inner sort is what used to:\n%s", plan)
            .doesNotContain("external merge")
    }

    @Test
    fun `a bucket of the table's LARGEST files is bounded by the bucket, which the replaced probe was not`() {
        // THE MANIFEST WALK, measured on both statements.
        //
        // Probe a fresh-day bucket — 20,000 files of 200 MB, the table's
        // largest — with a cap below the bucket. The REPLACED statement
        // drives the manifest: a hash join whose outer side is a
        // `Seq Scan on hog_data_file`, walked until the `LIMIT` fills.
        // Measured: 57,342 manifest rows for 8,192 returned. Here the
        // walk stops because the bucket is a seventh of the table; for a
        // bucket under the cap it does not stop until the table ends,
        // which matches gigahog-prod-us's 2026-10-21 symptoms (1,064,028
        // sequential scans of hog_data_file, 193 of 200 sweeps over the
        // plan statement timeout).
        val before = explainBucket("fresh-1", sql = ReplacedCandidateSql.sql(listOf(0)))
        val plan = explainBucket("fresh-1")
        assertThat(actualRows(plan)).isEqualTo(cfg.candidateBudget.toLong())
        assertThat(actualRows(before)).describedAs("same answer from both").isEqualTo(cfg.candidateBudget.toLong())
        assertBucketDriven(plan)
        // The bucket's entries, then the cap's rows by primary key: the
        // manifest rows read are the cap, not a walk.
        assertThat(scannedRows(plan))
            .describedAs("manifest rows read are the cap's:\n%s", plan)
            .isEqualTo(cfg.candidateBudget.toLong())
        // RECORDED, not asserted: what Postgres does with the replaced
        // statement guards nothing in hoglake, and a point release that
        // plans it better must not fail this suite. Measured on PG 18.6:
        // a hash join over `Seq Scan on hog_data_file`, 57,342 rows.
        println(
            "RECORDED replaced fresh-1 probe: manifest rows ${scannedRows(before)}, " +
                "buffers ${ExplainPlan.nodes(before).first().buffers}; new: manifest rows " +
                "${scannedRows(plan)}, buffers ${ExplainPlan.nodes(plan).first().buffers}",
        )

        // AND THE ORDERING STILL PUTS IT LAST. It used to be what kept
        // the walk above unreachable; it is now a VALUE ordering only
        // (files retired per byte rewritten), and it is still the order
        // the planner probes in, so a bucket of 200 MB files must come
        // after every bucket of 12 KiB files.
        val order = sampledOrder()
        val firstFresh = order.indexOfFirst { it.startsWith("fresh-") }
        val lastTiny = order.indexOfLast { it.startsWith("day-") }
        assertThat(firstFresh).describedAs("the fresh buckets are in the sample").isNotEqualTo(-1)
        assertThat(lastTiny).describedAs("so are the stray ones").isNotEqualTo(-1)
        assertThat(firstFresh)
            .describedAs(
                "every 12 KiB bucket must be ordered before every 200 MB one, or a run spends its " +
                    "budget on the least valuable work while cheaper work exists (order head: %s)",
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
        publishSample()

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
