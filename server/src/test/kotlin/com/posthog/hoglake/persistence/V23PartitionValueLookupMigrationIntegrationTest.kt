package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
import com.posthog.hoglake.compaction.CompactionConfig
import com.posthog.hoglake.compaction.CompactionService
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.MaintenanceSummarySampler
import com.posthog.hoglake.testing.Measuring
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
 * V23 against the statement it exists for: the compaction planner's
 * BUCKET-SCOPED candidate fetch.
 *
 * THE MIGRATION RUNS HERE — the database arrives at V22, 200,000 file
 * rows and their partition values are seeded, and only then does
 * [Database.migrate] apply V23 — so the BEFORE half is a measurement on
 * the same rows rather than a claim about a file that is not there. That
 * is V16's, V17's, V19's and V22's shape, and AGENT.md's rule.
 *
 * WHAT THE INDEX IS FOR. `hog_file_partition_value`'s primary key is
 * `(catalog_id, data_file_id, key_index)`, which answers "what are THIS
 * FILE's partition values". A compaction group never spans a
 * `(spec_id, partition_values)` bucket, so the planner needs the
 * inverse — "which files are in THIS BUCKET" — and before V23 nothing
 * answered it. What the planner did instead is the defect: it selected
 * every live file of the table under the target, with a correlated
 * `array_agg` over this table per row, and bucketed them in the JVM.
 *
 * THE BUCKETS ARE SCATTERED, AND THAT IS THE WHOLE FIXTURE. Partition
 * values are assigned round-robin over one `generate_series`, so a
 * bucket's 100 files are spread through both heaps exactly as ingest
 * leaves them. AGENT.md records what a clustered fixture does to a
 * measurement of this kind: V19's first draft read 91x on contiguous
 * rows and the honest number on scattered ones was 2.2x.
 *
 * TWO CATALOGS, for the reason V17's and V19's fixtures have two:
 * `catalog_id` leads both the primary key and the new index, so with one
 * catalog "this catalog's partition values" and "every partition value"
 * are the same rows and the BEFORE plan would look far better than it
 * is.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class V23PartitionValueLookupMigrationIntegrationTest {
    private companion object {
        const val INDEX = "hog_file_partition_value_lookup"

        /** Put the history back to before V23 (V16's helper, and its reasoning). */
        const val REAPPLY_V23 = "DELETE FROM flyway_schema_history WHERE version::numeric >= 23"

        /** File rows in the subject catalog's one table. */
        const val FILES = 200_000

        /** Distinct partition values, i.e. buckets. 100 files each. */
        const val BUCKETS = 2_000

        /** Every this-many'th file row goes to the neighbour catalog. */
        const val NEIGHBOUR_EVERY = 10

        /**
         * Registered bytes per file. Well under the 512 MiB compaction
         * target, so every file is a candidate — which is the state that
         * made the old statement read the whole manifest.
         */
        const val FILE_BYTES = 4L * 1024 * 1024

        /** Rows per file. Unsorted table, so no row ceiling applies. */
        const val FILE_RECORDS = 50_000L
    }

    // productionSession, like every prior index migration test (V16,
    // V17, V19, V20, V21): V23 SAVEs and RESTOREs `lock_timeout` and
    // `statement_timeout`, and against a session carrying 0/0 the
    // restore is trivially satisfied. Here it restores the 5s/60s a pod
    // really carries, which is what the file's non-transactional shape
    // makes load-bearing.
    private val db = PgTestSupport.freshDatabaseAt("22", productionSession = true)

    /**
     * Planning is metadata-only and this suite never executes a group,
     * so the store points at a dead port: any object-store contact would
     * surface as a connection failure rather than pass quietly.
     */
    private val store =
        ObjectStore(
            endpoint = "http://127.0.0.1:9",
            region = "us-east-1",
            accessKey = "unused",
            secretKey = "unused",
            pathStyle = true,
        )

    private val cfg =
        CompactionConfig(
            targetBytes = 512L * 1024 * 1024,
            maxGroupsPerRun = 64,
            maxInputFiles = 64,
            minInputFiles = 5,
        )

    private var catalogId = 0L
    private var neighbourId = 0L
    private var tableId = 0L

    private lateinit var planBefore: String
    private lateinit var planAfter: String
    private lateinit var aggregateBefore: String
    private var buildMillis = 0L

    @AfterAll
    fun tearDown() {
        store.close()
        db.close()
    }

    @BeforeAll
    fun seedThenMigrate() {
        catalogId = createCatalog("v23")
        neighbourId = createCatalog("v23-neighbour")
        seedManifest()
        analyze()

        assertThat(indexDef()).describedAs("%s absent before V23", INDEX).isNull()
        planBefore = explainBucketFetch()
        // The two index-free alternatives, measured HERE — while there
        // genuinely is no index — because that is what "index-free"
        // means. See the test that reads them.
        aggregateBefore = explainAlternative(aggregateAndFilterSql)

        val start = System.nanoTime()
        Database.migrate(db.dataSource)
        buildMillis = (System.nanoTime() - start) / 1_000_000

        // VACUUM, not merely ANALYZE: a bulk insert leaves the visibility
        // map unset, so the new index's Index Only Scan would pay a heap
        // fetch per row and the buffer budget below would be measuring
        // the absence of autovacuum rather than the plan.
        analyze()
        planAfter = explainBucketFetch()
    }

    private fun createCatalog(name: String): Long {
        // Raw, like the other planner fixtures: CatalogService's
        // creation-time shape rules reject the shared data_path these
        // synthetic paths sit under.
        val id =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path, last_snapshot_id) " +
                        "VALUES (:n, 's3://v23/' || :n, 0) RETURNING catalog_id",
                ).bind("n", name).mapTo(Long::class.java).one()
            }
        db.jdbi.useHandleUnchecked { h ->
            h.execute("INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version) VALUES (?, 0, 0)", id)
        }
        val catalogs = CatalogService(db.jdbi)
        catalogs.createNamespace(name, "ns")
        catalogs.createTable(
            name,
            "ns",
            "t",
            listOf(ColumnDef("id", ColType.LONG), ColumnDef("day", ColType.STRING)),
        )
        return id
    }

    /**
     * ONE `generate_series` for both catalogs and all 2,000 buckets, so
     * every row set this measures is INTERLEAVED in the heap rather
     * than clustered. See the class KDoc.
     */
    private fun seedManifest() {
        // The spec every file binds to, so the planner's
        // `spec_id IS NOT DISTINCT FROM` filter has something to match
        // and the bucket identity is the pair the sampler keys on.
        db.jdbi.useHandleUnchecked { h ->
            for (c in listOf(catalogId, neighbourId)) {
                h.execute(
                    "INSERT INTO hog_partition_spec (catalog_id, table_id, spec_id, begin_snapshot) " +
                        "VALUES (?, 1, 1, 0)",
                    c,
                )
                h.execute(
                    "INSERT INTO hog_partition_field " +
                        "(catalog_id, table_id, spec_id, key_index, source_field_id, transform) " +
                        "VALUES (?, 1, 1, 0, 2, 'identity')",
                    c,
                )
            }
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start,
                                           spec_id)
                SELECT CASE WHEN g % :every = 0 THEN :other ELSE :c END, g, 1, 0,
                       's3://v23/' || md5(g::text) || '/part-' || g || '.parquet',
                       :records, :bytes, g::bigint * :records, 1
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("other", neighbourId)
                .bind("every", NEIGHBOUR_EVERY).bind("records", FILE_RECORDS)
                .bind("bytes", FILE_BYTES).bind("n", FILES).execute()
            // One partition value per file, round-robin over the
            // buckets. `day-NNNN` is date-shaped on purpose: the index
            // holds the value, so its WIDTH is what the per-row cost in
            // V23's header is measured against.
            h.createUpdate(
                """
                INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value)
                SELECT f.catalog_id, f.data_file_id, 0,
                       'day-' || lpad((f.data_file_id % :buckets)::text, 6, '0')
                FROM hog_data_file f
                """,
            ).bind("buckets", BUCKETS).execute()
        }
        tableId =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT table_id FROM hog_table WHERE catalog_id = :c",
                ).bind("c", catalogId).mapTo(Long::class.java).one()
            }
    }

    private fun analyze() =
        db.jdbi.useHandleUnchecked { h ->
            for (relation in listOf("hog_data_file", "hog_file_partition_value", "hog_maintenance_summary_tier")) {
                h.execute("VACUUM (ANALYZE) $relation")
            }
        }

    private fun indexValid(target: PgTestSupport.TestDb = db): Boolean? =
        target.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT i.indisvalid FROM pg_index i
                JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = :n
                """,
            ).bind("n", INDEX).mapTo(Boolean::class.javaObjectType).findOne().orElse(null)
        }

    /**
     * The index's physical relation, so "not rebuilt" is an assertion
     * about the same FILE rather than about a definition that matches.
     */
    private fun indexNode(target: PgTestSupport.TestDb = db): Long =
        target.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT relfilenode FROM pg_class WHERE relname = :n")
                .bind("n", INDEX).mapTo(Long::class.java).findOne().orElse(0L)
        }

    private fun indexDef(): String? =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT indexdef FROM pg_indexes WHERE indexname = :n")
                .bind("n", INDEX).mapTo(String::class.java).findOne().orElse(null)
        }

    /**
     * EXPLAIN the planner's OWN statement, taken off
     * [CompactionService.candidateSql] rather than retyped — a
     * plan test that restates its query asserts the plan of something no
     * code path runs.
     *
     * Serial plans only: a Gather splits the buffer counts across
     * workers and the budgets below would be measuring the worker count.
     */
    private fun explainBucketFetch(): String {
        val svc = CompactionService(db.jdbi, store, cfg)
        val sql =
            svc.candidateSql(
                arms = listOf(0),
                nullArms = emptySet(),
                scopeToSpec = true,
                withPartitionValues = false,
            )
        return db.jdbi.withHandleUnchecked { h ->
            h.begin()
            try {
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                h.createQuery(
                    "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) $sql",
                )
                    .bind("catalogId", catalogId)
                    .bind("tableId", tableId)
                    .bind("targetBytes", cfg.targetBytes)
                    .bind("specId", 1L)
                    .bind("scanLimit", cfg.candidateBudget)
                    .bind("limit", cfg.candidateBudget)
                    .bind("k0", 0)
                    .bind("v0", "day-000007")
                    .mapTo(String::class.java).list().joinToString("\n")
            } finally {
                h.rollback()
            }
        }
    }

    /** Buffers on the scan node of [relation], never the plan's maximum. */
    private fun scanBuffers(
        plan: String,
        relation: String,
    ): Long {
        val lines = plan.split("\n")
        val at = lines.indexOfFirst { it.contains(" on $relation") || it.contains(" using ") && it.contains(relation) }
        assertThat(at).describedAs("no scan node for %s in:\n%s", relation, plan).isNotEqualTo(-1)
        // The `Buffers:` line belongs to the node above it. Taking the
        // plan's maximum instead would pick up EXPLAIN's own `Planning:`
        // section, which AGENT.md records dwarfing a well-indexed scan.
        val buffers = lines.drop(at + 1).firstOrNull { it.contains("Buffers:") }
        assertThat(buffers).describedAs("no Buffers line under %s's scan in:\n%s", relation, plan).isNotNull()
        return Regex("""shared (?:hit|read)=(\d+)""").findAll(buffers!!)
            .sumOf { it.groupValues[1].toLong() }
    }

    private fun rowsRemoved(plan: String): Long =
        Regex("""Rows Removed by Filter: (\d+)""").findAll(plan)
            .sumOf { it.groupValues[1].toLong() }

    /**
     * `Index Searches` on [relation]'s own scan node — one btree descent
     * per probe key, not one for the whole statement.
     *
     * Read off THAT node rather than matched anywhere in the plan,
     * because `contains("Index Searches: 1")` is also true of
     * `Index Searches: 100` and would pass on the plan it exists to
     * exclude.
     */
    private fun indexSearches(
        plan: String,
        relation: String,
    ): Long {
        val lines = plan.split("\n")
        val at = lines.indexOfFirst { it.contains(" on $relation ") || it.endsWith(" on $relation") }
        assertThat(at).describedAs("no scan node for %s in:\n%s", relation, plan).isNotEqualTo(-1)
        val line = lines.drop(at + 1).firstOrNull { it.contains("Index Searches:") }
        assertThat(line).describedAs("no Index Searches under %s in:\n%s", relation, plan).isNotNull()
        return Regex("""Index Searches: (\d+)""").find(line!!)!!.groupValues[1].toLong()
    }

    /**
     * THE REJECTED ALTERNATIVE: lead on `hog_data_file` and compare the
     * AGGREGATED tuple, which needs no new index at all.
     *
     * This is the statement being replaced, narrowed to one bucket. It
     * is measured rather than argued about because "EXPLAIN both and
     * ship the one that is an index range" is the only way to choose.
     *
     * The OTHER shape once listed here — walk V10's size index and
     * filter by an `EXISTS` per key — is no longer an alternative: it is
     * what ships (`CompactionService.candidateSql`), because the
     * `INTERSECT` form that was measured against it bounds the rows
     * returned and not the work. Its plan is `planAfter`.
     */
    private val aggregateAndFilterSql: String =
        """
        SELECT f.data_file_id, f.path, f.record_count, f.file_size_bytes
          FROM hog_data_file f
         WHERE f.catalog_id = :catalogId AND f.table_id = :tableId
           AND f.end_snapshot IS NULL
           AND f.file_size_bytes < :targetBytes
           AND (SELECT array_agg(pv.value ORDER BY pv.key_index)
                  FROM hog_file_partition_value pv
                 WHERE pv.catalog_id = f.catalog_id
                   AND pv.data_file_id = f.data_file_id) = ARRAY[:v0]::text[]
         ORDER BY f.file_size_bytes, f.data_file_id
         LIMIT :limit
        """

    private fun explainAlternative(sql: String): String =
        db.jdbi.withHandleUnchecked { h ->
            h.begin()
            try {
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                h.createQuery(
                    "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) $sql",
                )
                    .bind("catalogId", catalogId)
                    .bind("tableId", tableId)
                    .bind("targetBytes", cfg.targetBytes)
                    .bind("limit", cfg.candidateBudget)
                    .bind("v0", "day-000007")
                    .mapTo(String::class.java).list().joinToString("\n")
            } finally {
                h.rollback()
            }
        }

    // ---- the index itself ----------------------------------------------------

    @Test
    fun `the index keys exactly the columns each EXISTS arm probes and returns`() {
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
        // Resolved through pg_index.indkey, not by grepping indexdef — a
        // `contains("value")` over the definition is satisfied by the
        // index's own NAME (AGENT.md). `data_file_id` last is what makes
        // an arm an Index ONLY Scan; drop it and every match costs a heap
        // fetch on the largest child table in the schema.
        assertThat(key).containsExactly("catalog_id", "key_index", "value", "data_file_id")
    }

    @Test
    fun `the index is not partial - every live file of a partitioned table is a bucket member`() {
        val predicate =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT pg_get_expr(i.indpred, i.indrelid)
                    FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
                    WHERE c.relname = :n
                    """,
                ).bind("n", INDEX).mapTo(String::class.java).findOne().orElse(null)
            }
        // Asserted through pg_index.indpred rather than by counting
        // rows. V19's index IS partial and V17's is not, and the reason
        // is per index: there is no predicate here that could exclude
        // anything, because the planner asks about every live file's
        // bucket. A future partial predicate would silently hide files
        // from the planner, which is why this is pinned.
        assertThat(predicate).describedAs("V23's index must cover every row").isNull()
    }

    // ---- the plan -------------------------------------------------------------

    @Test
    fun `before V23 a bucket-scoped fetch reads the catalog's whole partition-value population`() {
        // Only `catalog_id` leads the primary key, so `key_index` and
        // `value` are Filters: the probe reads every partition value of
        // the catalog to find one bucket's.
        assertThat(rowsRemoved(planBefore))
            .describedAs("the pre-V23 probe filters the catalog's whole population:\n%s", planBefore)
            .isGreaterThanOrEqualTo((FILES - FILES / NEIGHBOUR_EVERY - FILES / BUCKETS).toLong())
        assertThat(scanBuffers(planBefore, "hog_file_partition_value"))
            .describedAs("and pays for it in buffers:\n%s", planBefore)
            .isGreaterThan(1_000)
    }

    @Test
    fun `after V23 a bucket-scoped fetch is one index range and touches no more than the bucket`() {
        assertThat(planAfter)
            .describedAs("V23's index must serve the probe:\n%s", planAfter)
            .contains(INDEX)
        // The ABSENCE of a filter is the assertion that separates "an
        // index was used" from "the index did the work" — AGENT.md
        // records V16's case, where a guard no btree could drive still
        // produced an index scan because the leading column alone was
        // enough and the rest was demoted to a Filter.
        assertThat(rowsRemoved(planAfter))
            .describedAs("nothing may be read and discarded:\n%s", planAfter)
            .isZero()
        assertThat(indexSearches(planAfter, "hog_file_partition_value"))
            .describedAs("one descent for the arm, not one per row:\n%s", planAfter)
            .isEqualTo(1)
        // Bounded by THE BUCKET, derived from the fixture rather than
        // written as a constant: the arm reads the bucket's own entries
        // and nothing else, so a page's worth of them plus the btree
        // descent is the budget.
        //
        // NOT asserted here: how many times the plan probes
        // `hog_data_file`. An earlier version of this test asserted
        // exactly `FILES / BUCKETS` — "one primary-key probe per bucket
        // member" — which is the UNBOUNDED shape stated as the desired
        // property: it is true only because this fixture's buckets are
        // uniformly 100 files, and it is false the moment a bucket is
        // 100k. What bounds the manifest probes is the statement's
        // `LIMIT`, and `CompactionCandidateFetchPlanIntegrationTest`
        // is where that is measured, on a fixture with the skew to
        // measure it against.
        val perBucket = (FILES / BUCKETS).toLong()
        assertThat(scanBuffers(planAfter, "hog_file_partition_value"))
            .describedAs("buffers must scale with the bucket, not the catalog:\n%s", planAfter)
            .isLessThanOrEqualTo(perBucket)
    }

    @Test
    fun `no sequential scan of the manifest anywhere in the bucket fetch`() {
        // The whole point: the statement being replaced read
        // hog_data_file end to end. The intersected ids reach it by its
        // PRIMARY KEY.
        assertThat(planAfter)
            .describedAs("the manifest must be reached by key, never scanned:\n%s", planAfter)
            .doesNotContain("Seq Scan on hog_data_file")
        assertThat(planAfter)
            .describedAs("nor may the partition values be:\n%s", planAfter)
            .doesNotContain("Seq Scan on hog_file_partition_value")
    }

    // ---- what the planner reads through it ------------------------------------

    @Test
    fun `the planner's candidate read is bounded by the run's own capacity`() {
        // The defect, in one number. 200,000 candidate files over 2,000
        // buckets, against a run that can rewrite 64 groups of 64 files
        // — 4,096 files. The old planner fetched all 200,000 (9.9M on
        // gigahog-prod-us); this one fetches at most
        // headroom x maxGroupsPerRun x maxInputFiles.
        //
        // The SAMPLE is what scopes it, so the real sampler produces it:
        // seeding tier rows by SQL here would assert the rule this test
        // is written from rather than the system. It publishes one
        // generation over the fixture, and the planner then reads
        // buckets out of it.
        val sampler =
            MaintenanceSummarySampler(db.jdbi, cfg.targetBytes, cfg.minInputFiles, cfg.maxInputFiles, 3600)
        var steps = 0
        while (sampler.runOnce(50_000)) check(++steps < 1_000)
        analyze()

        val svc = CompactionService(db.jdbi, store, cfg)
        val plan = svc.planTable("v23", "ns", "t", cfg)

        assertThat(plan.candidatesFetched)
            .describedAs("the bound the planner's own defect had no series for")
            .isLessThanOrEqualTo(cfg.candidateBudget.toLong())
        assertThat(plan.candidatesFetched)
            .describedAs("and it must actually read enough to fill the run")
            .isGreaterThanOrEqualTo(cfg.maxGroupsPerRun.toLong() * cfg.maxInputFiles)
        // Every bucket the SUBJECT catalog has files in has debt, and
        // that is not all 2,000: the neighbour takes every tenth file
        // id, and `id % BUCKETS` maps exactly those onto one bucket in
        // ten — so 200 of the buckets hold only the neighbour's files
        // and this catalog has 1,800. Derived rather than written down,
        // because the arithmetic is the fixture's and a constant would
        // hide a change to it.
        assertThat(plan.bucketsAvailable)
            .describedAs("every bucket this catalog has files in has enough of them to group")
            .isEqualTo((BUCKETS - BUCKETS / NEIGHBOUR_EVERY).toLong())
        assertThat(plan.bucketsConsidered)
            .describedAs("only as many buckets as the candidate budget covers")
            .isLessThan(plan.bucketsAvailable)
        assertThat(plan.groups)
            .describedAs("and the plan is a full run's worth of work, not a truncated one")
            .hasSizeGreaterThanOrEqualTo(cfg.maxGroupsPerRun)
        // Every group is inside ONE bucket: the fetch is per bucket, so
        // this is true by construction, and it is the property the whole
        // bucket-first design rests on.
        for (group in plan.groups) {
            assertThat(group.partitionValues).hasSize(1)
        }
    }

    @Test
    fun `the index-free alternative does not bound the read, which is why the index exists`() {
        // AGENT.md: an index proves itself against the query it serves,
        // and a new index has to beat the plan that needs no index at
        // all. This one answers the same question about the same bucket,
        // on the same rows, with NO index — which is the state it was
        // proposed for. It is the statement being replaced, narrowed to
        // one bucket: it reads every live file of the table whatever the
        // bucket holds.
        val expected = (FILES - FILES / NEIGHBOUR_EVERY - FILES / BUCKETS).toLong()
        assertThat(rowsRemoved(aggregateBefore))
            .describedAs("aggregate-and-filter reads every live file of the table:\n%s", aggregateBefore)
            .isGreaterThanOrEqualTo(expected)
        // Against which the shipped statement reads the bucket.
        assertThat(scanBuffers(planAfter, "hog_file_partition_value"))
            .describedAs("the shipped probe:\n%s", planAfter)
            .isLessThan(scanBuffers(aggregateBefore, "hog_data_file"))
    }

    // ---- re-entry, which the runbook depends on -------------------------------

    @Test
    fun `re-entry is a clean no-op - an existing index is not rebuilt`() {
        // The out-of-band pre-build the header sells to the operator
        // DEPENDS on this: step 2 of the runbook builds the index by
        // hand, after which the migration must find it and do nothing.
        // `CREATE INDEX CONCURRENTLY IF NOT EXISTS` matches on NAME, so
        // "nothing" means the same relfilenode, not merely an index with
        // the same definition. V17's shape: put the history back and let
        // `Database.migrate` re-run the real file — `applyMigrationFile`
        // cannot, because it wraps the script in a transaction and a
        // concurrent build refuses to run inside one.
        //
        // Its OWN database, small and fresh, so the re-run is
        // milliseconds and the 200k-row fixture the rest of this class
        // measures on is left alone.
        PgTestSupport.freshDatabase(productionSession = true).use { fresh ->
            val before = indexNode(fresh)
            assertThat(before).describedAs("the index exists after V23").isNotZero()
            fresh.jdbi.useHandleUnchecked { h -> h.execute(REAPPLY_V23) }
            Database.migrate(fresh.dataSource)
            assertThat(indexNode(fresh))
                .describedAs("an existing index must not be dropped and rebuilt")
                .isEqualTo(before)
            assertThat(indexValid(fresh)).isTrue()
        }
    }

    @Test
    fun `an INVALID remnant is cleared rather than skipped forever`() {
        // A cancelled concurrent build — a killed pod, an operator's
        // Ctrl-C, a statement timeout — leaves an INVALID index that
        // `IF NOT EXISTS` would find by name and skip, leaving one that
        // every partition-value INSERT maintains and no query may use.
        // The DO block at the head of the file is what clears it, and
        // this is the only test that reaches that block. Forged, because
        // killing a real concurrent build mid-flight is not
        // reproducible in a test.
        PgTestSupport.freshDatabase(productionSession = true).use { fresh ->
            fresh.jdbi.useHandleUnchecked { h ->
                h.execute(
                    "UPDATE pg_index SET indisvalid = false WHERE indexrelid = " +
                        "(SELECT oid FROM pg_class WHERE relname = ?)",
                    INDEX,
                )
                h.execute(REAPPLY_V23)
            }
            assertThat(indexValid(fresh)).describedAs("the remnant is in place").isFalse()

            Database.migrate(fresh.dataSource)

            assertThat(indexValid(fresh))
                .describedAs("the remnant must be dropped and rebuilt, not skipped")
                .isTrue()
        }
    }

    // ---- the build ------------------------------------------------------------

    @Test
    fun `the concurrent build is SLOWER than a plain one, which is the trade being made`() {
        // AGENT.md: CREATE INDEX CONCURRENTLY is not the safe default,
        // it is a trade, and it is measured PER MIGRATION. The reversal
        // is never about which build is FASTER — a concurrent build is
        // two heap passes and is always slower at any size — it is about
        // what a plain build BLOCKS. This records the cost side so V23's
        // header is quoting a run rather than the neighbouring
        // migration.
        //
        // Both builds on the same rows, in the same session, one after
        // the other. Warm: the relation was just VACUUM ANALYZEd, so
        // these are the BEST case and not the deploy case.
        fun build(concurrently: Boolean): Long =
            db.jdbi.withHandleUnchecked { h ->
                h.execute("DROP INDEX $INDEX")
                val start = System.nanoTime()
                h.execute(
                    "CREATE INDEX ${if (concurrently) "CONCURRENTLY" else ""} $INDEX " +
                        "ON hog_file_partition_value (catalog_id, key_index, value, data_file_id)",
                )
                (System.nanoTime() - start) / 1_000_000
            }
        val concurrent = build(concurrently = true)
        val plain = build(concurrently = false)
        println("V23 build: concurrent $concurrent ms against plain $plain ms on $FILES rows")
        // Not a threshold on either number — a machine-dependent
        // millisecond budget would flake and teach nothing. What is
        // asserted is the ORDERING the trade rests on, a property of two
        // heap passes instead of one — and on a quiet host only: a shared
        // CI runner measured concurrent 232 ms against plain 617 ms once,
        // so even the ordering is a measuring-host statement (Measuring).
        if (Measuring.enabled) {
            assertThat(concurrent)
                .describedAs("concurrent %d ms against plain %d ms on %d rows", concurrent, plain, FILES)
                .isGreaterThan(plain)
        }
        // And the index is valid afterwards, because this test leaves
        // the fixture behind for the ones that share the class.
        assertThat(indexDef()).isNotNull()
    }

    @Test
    fun `the build and the index's size are recorded, not guessed`() {
        val size =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT pg_relation_size(:n::regclass)")
                    .bind("n", INDEX).mapTo(Long::class.java).one()
            }
        val rows =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT count(*) FROM hog_file_partition_value").mapTo(Long::class.java).one()
            }
        // Not a threshold — the numbers in V23's header. A test that
        // asserted a millisecond budget would fail on a loaded machine
        // and teach nothing; what this pins is that the header's figures
        // came from a run, and it prints them when it is looked at.
        assertThat(size / rows)
            .describedAs("bytes per value row (V23's header says 50); index %d B over %d rows", size, rows)
            .isBetween(20L, 120L)
        assertThat(buildMillis)
            .describedAs("whole V23 migration, concurrent build included")
            .isGreaterThan(0)
    }
}
