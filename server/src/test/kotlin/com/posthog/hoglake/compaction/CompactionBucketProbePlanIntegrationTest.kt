package com.posthog.hoglake.compaction

import com.posthog.hoglake.Database
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.MaintenanceSummarySampler
import com.posthog.hoglake.testing.ExplainPlan
import com.posthog.hoglake.testing.PgTestSupport
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * ONE bucket probe of the compaction planner's bucket loop
 * (`CompactionService.candidateSql` with arms), on a fixture shaped like
 * gigahog-prod-us's `identity(team_id), month(timestamp)` tables: many
 * small buckets in one table, and partition values SHARED across the
 * catalog's tables.
 *
 * # What production showed
 *
 * 2026-10-21: a table with 1.33M live files below target in >50k
 * buckets (median 66 small files), in a catalog of 41M live files and
 * 67M partition-value rows. Plans of 18-37 minutes for 240-480 probes,
 * and 193 of 200 sweeps failing the 15 s plan statement timeout on one
 * probe. A probe that walks the table's rows until its `LIMIT` fills is
 * O(table) per bucket, and the bucket loop multiplies it by the bucket
 * count.
 *
 * # What this fixture poses
 *
 * [BUCKETS] buckets of [PER_BUCKET] tiny files on the subject table
 * ([TEAMS] team values x [MONTHS] month values, interleaved in the heap
 * by `data_file_id % 4` and `% BUCKETS`), and [NEIGHBOURS] neighbour
 * tables of [NEIGHBOUR_FILES] files each over the SAME month values and
 * an overlapping team range. So, for one bucket:
 *
 *  - the TEAM arm is selective catalog-wide (a few hundred entries);
 *  - the MONTH arm is not (a fifth of the catalog: [monthArm]) — an arm
 *    is catalog-scoped, because `hog_file_partition_value` has no
 *    `table_id`;
 *  - the bucket itself is [PER_BUCKET] files of the subject table, plus
 *    the few neighbour files that share both values;
 *  - and one BIG TEAM ([BIG_TEAM]) holding a third of every neighbour's
 *    files, as a large customer does in every table of a catalog, whose
 *    stray bucket on the subject is still [PER_BUCKET] files.
 *
 * The property: a probe's work is bounded by the SELECTIVE ARM and the
 * catalog-wide bucket, and by neither the table nor the common arm.
 * Both are asserted in buffers, derived from the fixture. For the big
 * team there is no selective arm, and the bound is the catalog-wide
 * bucket — stated, measured and compared with the replaced statement.
 *
 * WHAT THIS FIXTURE DOES NOT REPRODUCE: production's O(table) walk on a
 * two-key bucket. On these rows Postgres 18 drove the REPLACED statement
 * from V23's index too (1,056 buffers for the selective bucket, against
 * 957 now, measured before DVs were seeded). The manifest-driven plans it could pick are measured in
 * `CompactionCandidateFetchPlanIntegrationTest` (a `Seq Scan on
 * hog_data_file` under a hash join, walked until the `LIMIT` fills). The
 * new statement's point is that PG 18 costs its id-set InitPlan as a
 * handful of primary-key descents whatever the statistics say
 * (`estimate_array_length()` defaults a Param array to 10), so no
 * misestimate makes that walk look cheaper — see
 * `CompactionService.candidateSql`, which also states the regimes where
 * the new statement costs more.
 *
 * Plus one bucket whose month is NULL (`value IS NULL` arm), and a
 * tuple-less vintage ([TUPLELESS] files, `spec_id` NULL, no partition
 * values) whose no-arm probe must keep its ordered-scan plan.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompactionBucketProbePlanIntegrationTest {
    private companion object {
        const val CATALOG = "bucketprobe"

        /** Subject-table buckets: [TEAMS] x [MONTHS]. */
        const val BUCKETS = 2_000
        const val PER_BUCKET = 50
        const val TEAMS = BUCKETS / 5
        const val MONTHS = 5
        const val FIRST_MONTH = 680

        /** Neighbour tables, each [NEIGHBOUR_FILES] files. */
        val NEIGHBOURS = listOf("t2", "t3", "t4")
        const val NEIGHBOUR_FILES = BUCKETS * PER_BUCKET

        /** Neighbour team values range over this many, overlapping the subject's [TEAMS]. */
        const val NEIGHBOUR_TEAMS = 20_000

        /** One generate_series over every partitioned file, `g % 4 == 0` being the subject's. */
        const val PARTITIONED_FILES = 4 * BUCKETS * PER_BUCKET

        /** The tuple-less vintage on the subject table. */
        const val TUPLELESS = 1_000

        /** The SELECTIVE probe: team 8, month 682 (subject bucket 42). */
        const val TEAM = "8"
        const val MONTH = "682"

        /**
         * The BIG TEAM: a third of every neighbour's files, as a large
         * customer's are in every table of a catalog. Its stray bucket
         * (7, 682) on the subject is still [PER_BUCKET] files.
         */
        const val BIG_TEAM = "7"

        /** The NULL-month bucket: team 0's fifth month (bucket 4). */
        const val NULL_TEAM = "0"

        const val TINY_BYTES = 12L * 1024

        /** A live DV on every this-many'th file: ~133k live DVs. */
        const val DV_EVERY = 3
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
                        "VALUES (:n, 's3://bucketprobe', 0) RETURNING catalog_id",
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
                listOf(
                    ColumnDef("team_id", ColType.LONG),
                    ColumnDef("month", ColType.INT),
                ),
            )
        }
        tableId = tableIdOf("t1")
        val tables = listOf(tableId) + NEIGHBOURS.map { tableIdOf(it) }
        db.jdbi.useHandleUnchecked { h ->
            for (id in tables) {
                h.execute(
                    "INSERT INTO hog_partition_spec (catalog_id, table_id, spec_id, begin_snapshot) " +
                        "VALUES (?, ?, 1, 0)",
                    catalogId,
                    id,
                )
                for ((key, source) in listOf(0 to 1, 1 to 2)) {
                    h.execute(
                        "INSERT INTO hog_partition_field " +
                            "(catalog_id, table_id, spec_id, key_index, source_field_id, transform) " +
                            "VALUES (?, ?, 1, ?, ?, 'identity')",
                        catalogId,
                        id,
                        key,
                        source,
                    )
                }
            }
            // ONE generate_series for every table, so the subject's
            // buckets and the neighbours' files are INTERLEAVED in both
            // heaps — AGENT.md records what a clustered fixture does to
            // a measurement of this kind.
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start, spec_id)
                SELECT :c, g, (ARRAY[:t1, :t2, :t3, :t4])[g % 4 + 1], 0,
                       's3://bucketprobe/' || md5(g::text) || '/part-' || g || '.parquet',
                       200, :bytes + (g::bigint * 7919) % 4096, g::bigint * 1000, 1
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId)
                .bind("t1", tables[0]).bind("t2", tables[1]).bind("t3", tables[2]).bind("t4", tables[3])
                .bind("bytes", TINY_BYTES).bind("n", PARTITIONED_FILES).execute()
            // The tuple-less vintage: no spec, no partition values.
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start, spec_id)
                SELECT :c, :base + g, :t, 0, 's3://bucketprobe/v0/' || g || '.parquet',
                       200, :bytes, (:base + g)::bigint * 1000, NULL
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("t", tableId).bind("base", PARTITIONED_FILES)
                .bind("bytes", TINY_BYTES).bind("n", TUPLELESS).execute()
            // Subject: bucket b = (g / 4) % BUCKETS, team b / 5, month
            // FIRST_MONTH + b % 5, and bucket 4's month NULL. Neighbours:
            // team g % NEIGHBOUR_TEAMS, month FIRST_MONTH + g % 5.
            h.createUpdate(
                """
                INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value)
                SELECT :c, g, 0,
                       CASE WHEN g % 4 = 0 THEN (((g / 4) % :buckets) / 5)::text
                            WHEN g % 3 = 1 THEN :bigTeam
                            ELSE (g % :neighbourTeams)::text END
                  FROM generate_series(1, :n) g
                UNION ALL
                SELECT :c, g, 1,
                       CASE WHEN g % 4 = 0 AND (g / 4) % :buckets = 4 THEN NULL
                            WHEN g % 4 = 0 THEN (:firstMonth + ((g / 4) % :buckets) % 5)::text
                            ELSE (:firstMonth + g % 5)::text END
                  FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("buckets", BUCKETS).bind("neighbourTeams", NEIGHBOUR_TEAMS)
                .bind("bigTeam", BIG_TEAM)
                .bind("firstMonth", FIRST_MONTH).bind("n", PARTITIONED_FILES).execute()
            // LIVE DVs on every [DV_EVERY]th file, and an ENDED one on a
            // few more. With `hog_delete_file` empty and unanalyzed the
            // DV join's plan flipped on default statistics between one
            // materialized scan (1 buffer) and a probe per row (2 per
            // row) — a ±100-buffer wobble at 50 rows and ±16k at the cap
            // (QE, #397) — so the join's per-row term was assumed, not
            // measured.
            h.createUpdate(
                """
                INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id,
                                             begin_snapshot, end_snapshot, path, delete_count, file_size_bytes)
                SELECT f.catalog_id, f.data_file_id, f.table_id, f.data_file_id, 1,
                       CASE WHEN f.data_file_id % (:every * 7) = 1 THEN 2 END,
                       's3://bucketprobe/dv-' || f.data_file_id || '.puffin', 3, 100
                  FROM hog_data_file f
                 WHERE f.catalog_id = :c
                   AND (f.data_file_id % :every = 0 OR f.data_file_id % (:every * 7) = 1)
                """,
            ).bind("c", catalogId).bind("every", DV_EVERY).execute()
            for (relation in listOf("hog_data_file", "hog_file_partition_value", "hog_delete_file")) {
                h.execute("VACUUM (ANALYZE) $relation")
            }
        }
    }

    private fun tableIdOf(name: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT tv.table_id FROM hog_table_version tv WHERE tv.catalog_id = :c AND tv.name = :n")
                .bind("c", catalogId).bind("n", name).mapTo(Long::class.java).one()
        }

    private fun count(
        sql: String,
        vararg binds: Pair<String, Any>,
    ): Long =
        db.jdbi.withHandleUnchecked { h ->
            val q = h.createQuery(sql).bind("c", catalogId)
            binds.forEach { (k, v) -> q.bind(k, v) }
            q.mapTo(Long::class.java).one()
        }

    /** One arm's entries across the WHOLE catalog, which is what the lookup index ranges over. */
    private fun armSize(
        keyIndex: Int,
        value: String,
    ): Long =
        count(
            "SELECT count(*) FROM hog_file_partition_value WHERE catalog_id = :c AND key_index = :k AND value = :v",
            "k" to keyIndex,
            "v" to value,
        )

    private val monthArm: Long get() = armSize(1, MONTH)
    private val teamArm: Long get() = armSize(0, TEAM)
    private val bigTeamArm: Long get() = armSize(0, BIG_TEAM)

    /** The (team, MONTH) bucket across the whole catalog: every id the InitPlan returns. */
    private fun catalogBucket(team: String): Long =
        count(
            """
            SELECT count(*) FROM hog_file_partition_value a
              JOIN hog_file_partition_value b
                ON b.catalog_id = a.catalog_id AND b.data_file_id = a.data_file_id AND b.key_index = 1
             WHERE a.catalog_id = :c AND a.key_index = 0 AND a.value = :t AND b.value = :m
            """,
            "t" to team,
            "m" to MONTH,
        )

    /** The same bucket on the subject table only: what the probe returns. */
    private fun tableBucket(team: String): Long =
        count(
            """
            SELECT count(*) FROM hog_data_file f
              JOIN hog_file_partition_value a
                ON a.catalog_id = f.catalog_id AND a.data_file_id = f.data_file_id AND a.key_index = 0
              JOIN hog_file_partition_value b
                ON b.catalog_id = f.catalog_id AND b.data_file_id = f.data_file_id AND b.key_index = 1
             WHERE f.catalog_id = :c AND f.table_id = :t AND a.value = :team AND b.value = :m
            """,
            "t" to tableId,
            "team" to team,
            "m" to MONTH,
        )

    private fun relpages(relation: String): Long =
        count("SELECT relpages::bigint FROM pg_class WHERE relname = :n AND :c = :c", "n" to relation)

    private val explain = "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) "

    private fun sql(arms: List<Int>) =
        CompactionService(db.jdbi, store, cfg).candidateSql(
            arms = arms,
            nullArms = emptySet(),
            scopeToSpec = true,
            withPartitionValues = false,
        )

    private fun nullSql() =
        CompactionService(db.jdbi, store, cfg).candidateSql(
            arms = listOf(0, 1),
            nullArms = setOf(1),
            scopeToSpec = true,
            withPartitionValues = false,
        )

    /**
     * The CUSTOM plan of [statement] for the (team, month) bucket: what
     * production runs, because the probe is never server-prepared
     * (`Pg.CUSTOM_PLANS_ONLY`). EXPLAIN of a bound query plans custom.
     */
    private fun custom(
        statement: String,
        team: String = TEAM,
        month: String? = MONTH,
        specId: Long? = 1L,
        scanLimit: Int = cfg.candidateBudget + 1,
    ): String =
        db.jdbi.withHandleUnchecked { h ->
            serial(h) {
                val q =
                    h.createQuery(explain + statement)
                        .bind("catalogId", catalogId).bind("tableId", tableId)
                        .bind("targetBytes", cfg.targetBytes)
                        .bindByType("specId", specId, Long::class.javaObjectType)
                        .bind("scanLimit", scanLimit).bind("limit", scanLimit)
                        .bind("k0", 0).bind("v0", team).bind("k1", 1)
                if (month != null) q.bind("v1", month)
                q.mapTo(String::class.java).list().joinToString("\n")
            }
        }

    /**
     * The GENERIC plan, forced through PREPARE + `force_generic_plan`
     * (NamespaceListingQueryPlanIntegrationTest's mechanism): what
     * Postgres would switch the probe to if pgjdbc server-prepared it.
     */
    private fun generic(statement: String): String =
        db.jdbi.withHandleUnchecked { h ->
            serial(h) {
                h.execute("SET LOCAL plan_cache_mode = force_generic_plan")
                val positional =
                    listOf(
                        "catalogId", "tableId", "targetBytes", "specId", "scanLimit", "limit", "k0", "v0", "k1", "v1",
                    ).foldIndexed(statement) { i, s, name -> s.replace(Regex(":$name\\b"), "\\$${i + 1}") }
                h.execute(
                    "PREPARE probe_generic(bigint, bigint, bigint, bigint, int, int, int, text, int, text) " +
                        "AS $positional",
                )
                try {
                    val limit = cfg.candidateBudget + 1
                    h.createQuery(
                        "${explain}EXECUTE probe_generic($catalogId, $tableId, ${cfg.targetBytes}, 1, " +
                            "$limit, $limit, 0, '$TEAM', 1, '$MONTH')",
                    ).mapTo(String::class.java).list().joinToString("\n")
                } finally {
                    h.execute("DEALLOCATE probe_generic")
                }
            }
        }

    /** Serial, inside a rolled-back transaction: a Gather splits buffer counts across workers. */
    private fun <T> serial(
        h: Handle,
        body: () -> T,
    ): T {
        h.begin()
        try {
            h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
            return body()
        } finally {
            h.rollback()
        }
    }

    /** The statement's whole cost: the root node's buffers (hit + read). */
    private fun buffers(plan: String): Long = ExplainPlan.nodes(plan).first().buffers

    private fun rootRows(plan: String): Long =
        Regex("""actual rows=([\d.]+)""").find(plan)!!.groupValues[1].toDouble().toLong()

    /** The scan node on [relation] whose Index Cond mentions `key_index = <k>` or `$<param>`. */
    private fun armNode(
        plan: String,
        keyCond: String,
    ): String {
        val lines = plan.lines()
        val at =
            lines.indices.firstOrNull { i ->
                lines[i].contains("hog_file_partition_value_lookup") &&
                    lines.getOrNull(i + 1)?.contains(keyCond) == true
            }
        assertThat(at).describedAs("no lookup-index arm with %s in:\n%s", keyCond, plan).isNotNull()
        return lines[at!!]
    }

    /** Rows an arm node touched in all: rows x loops. */
    private fun touched(node: String): Long {
        val rows = Regex("""actual rows=([\d.]+)""").find(node)!!.groupValues[1].toDouble()
        val loops = Regex("""loops=(\d+)""").find(node)!!.groupValues[1].toLong()
        return (rows * loops).toLong()
    }

    private val replacedSql: String = ReplacedCandidateSql.sql(listOf(0, 1))

    /**
     * Per-entry budget for a btree probe: a descent of the lookup index or
     * the manifest's primary key is three levels at this size, plus the
     * heap page the primary-key probe reads to check `table_id`. Derived
     * rather than fitted, and the same constant for every assertion.
     */
    private val perProbe = 4L

    /**
     * Print a budgeted plan's measured buffers, its budget and the DV
     * join's share, so a margin is read rather than inferred.
     */
    private fun recordBudget(
        plan: String,
        budget: Long,
    ) {
        val dv = ExplainPlan.nodes(plan).filter { it.line.contains("on hog_delete_file dv") }.sumOf { it.buffers }
        println(
            "RECORDED budget: buffers ${buffers(plan)} of $budget " +
                "(${"%.2f".format(budget.toDouble() / buffers(plan))}x), DV join $dv",
        )
    }

    /**
     * The DV left join's per-returned-row probe and the plan's own fixed
     * pages. The join is a nested loop at any result size (the outer
     * estimate is the Param-array default, ~3 rows; see
     * `CompactionService.candidateSql` regime (c)), and each probe is a
     * descent of `hog_delete_file_data_lookup` plus the heap page of a
     * match — [perProbe], like every other probe here. Measured with
     * ~133k live DVs seeded: 3.4 buffers per returned row (169 for 50,
     * 27,700 for 8,193).
     */
    private fun overhead(rows: Long) = perProbe * rows + 64

    // ---- the fixture is the shape it claims to be -----------------------------

    @Test
    fun `the fixture has many small buckets, a selective arm, a common arm and a big team`() {
        // Asserted rather than assumed: every budget below is derived
        // from these numbers, and a fixture that stopped posing them would
        // make the budgets vacuous.
        assertThat(count("SELECT count(*) FROM hog_data_file WHERE catalog_id = :c"))
            .isEqualTo((PARTITIONED_FILES + TUPLELESS).toLong())
        assertThat(tableBucket(TEAM)).describedAs("the selective bucket").isEqualTo(PER_BUCKET.toLong())
        assertThat(tableBucket(BIG_TEAM)).describedAs("the big team's stray bucket").isEqualTo(PER_BUCKET.toLong())
        assertThat(monthArm)
            .describedAs("the month arm is catalog-scoped and common: a probe must not range over it")
            .isGreaterThan(100 * teamArm)
        assertThat(catalogBucket(BIG_TEAM))
            .describedAs("the big team's bucket is mostly OTHER tables' files")
            .isGreaterThan(100L * PER_BUCKET)
    }

    // ---- the selective bucket: bounded by the selective arm -------------------

    @Test
    fun `a selective bucket's probe drives from the lookup index and reads the bucket, not the table`() {
        val plan = custom(sql(listOf(0, 1)))
        assertThat(rootRows(plan)).describedAs("the whole bucket:\n%s", plan).isEqualTo(PER_BUCKET.toLong())
        assertThat(plan)
            .describedAs("the arms are an InitPlan over V23's index:\n%s", plan)
            .contains("InitPlan 1")
            .contains("hog_file_partition_value_lookup")
            .contains("data_file_id = ANY ((InitPlan 1).col1)")
        assertThat(plan)
            .describedAs("and the manifest is reached by primary key only:\n%s", plan)
            .doesNotContain("Seq Scan on hog_data_file")
            .doesNotContain("hog_data_file_maintenance_size_scan")
            .doesNotContain("hog_data_file_maintenance_scan")
        // THE COMMON ARM IS PROBED, NOT RANGED: the team arm drives and
        // each of its entries probes the month arm once. A range over the
        // month arm would touch [monthArm] entries — every table's files
        // for that month — which is what the generic plan below does.
        assertThat(touched(armNode(plan, "key_index = 1")))
            .describedAs("month-arm entries touched, against a selective arm of %d:\n%s", teamArm, plan)
            .isLessThanOrEqualTo(teamArm)
        // In BUFFERS, derived: one probe per selective-arm entry, one per
        // catalog-wide bucket id, the DV join per returned row. Measured
        // 1,125 (169 of it the DV join) against this budget's 1,464; the
        // manifest's heap is 10,019 pages, and the budget must stay under
        // a fifth of it.
        val budget = perProbe * (teamArm + catalogBucket(TEAM)) + overhead(PER_BUCKET.toLong())
        assertThat(buffers(plan))
            .describedAs("buffers against the bucket's budget %d:\n%s", budget, plan)
            .isLessThanOrEqualTo(budget)
        recordBudget(plan, budget)
        assertThat(budget)
            .describedAs("and that budget is far under the table")
            .isLessThan(relpages("hog_data_file") / 5)
    }

    @Test
    fun `the common key first in the spec is still probed, not ranged`() {
        // A table declared `month(ts), identity(team_id)` puts the COMMON
        // key first, so `candidateSql` starts its SQL from it. That order
        // is syntactic only: the planner pulls the `EXISTS` up into a
        // semi-join inside the InitPlan and picks the driving side by
        // cost. Same bucket, same budget as the test above, arms reversed.
        // Measured: the identical plan — the team key ranged (250), the
        // month key probed 250 times, the same buffers as the test above.
        val plan = custom(sql(listOf(1, 0)))
        assertThat(rootRows(plan)).describedAs("the whole bucket:\n%s", plan).isEqualTo(PER_BUCKET.toLong())
        assertThat(plan)
            .describedAs("the arms are an InitPlan over V23's index:\n%s", plan)
            .contains("InitPlan 1")
            .contains("data_file_id = ANY ((InitPlan 1).col1)")
            .doesNotContain("Seq Scan on hog_data_file")
        // MUTATION: make the team key's `EXISTS` unpullable (`OFFSET 0`
        // in it), so the month key drives as written, and this test fails
        // — measured: 240,912 buffers, the month key's catalog-wide range
        // read and the team key a correlated SubPlan per entry (the plan
        // then renumbers its subplans, so the InitPlan check trips first).
        assertThat(touched(armNode(plan, "key_index = 1")))
            .describedAs("month-key entries touched, against a selective key of %d:\n%s", teamArm, plan)
            .isLessThanOrEqualTo(teamArm)
        val budget = perProbe * (teamArm + catalogBucket(TEAM)) + overhead(PER_BUCKET.toLong())
        assertThat(buffers(plan))
            .describedAs("buffers against the bucket's budget %d:\n%s", budget, plan)
            .isLessThanOrEqualTo(budget)
        recordBudget(plan, budget)
    }

    @Test
    fun `the generic plan also drives from the lookup index, but ranges over the common arm`() {
        // What Postgres would switch to if pgjdbc server-prepared the
        // probe. Its id set still comes from V23's index and the manifest
        // is still reached by primary key — the array is costed at its
        // default length in this plan too — so it is bounded by the ARMS.
        // But a generic plan cannot see which arm is selective, so on PG
        // 18.6 it merge-joins both and reads every table's files for the
        // month. That is why the probe carries `Pg.CUSTOM_PLANS_ONLY`;
        // the next test pins that it does.
        val plan = generic(sql(listOf(0, 1)))
        assertThat(plan)
            .describedAs("generic: the id set is still the lookup index:\n%s", plan)
            .contains("InitPlan 1")
            .contains("hog_file_partition_value_lookup")
            .contains("data_file_id = ANY ((InitPlan 1).col1)")
            .doesNotContain("Seq Scan on hog_data_file")
            .doesNotContain("hog_data_file_maintenance_size_scan")
        assertThat(rootRows(plan)).isEqualTo(PER_BUCKET.toLong())
        // RECORDED, not asserted: how the generic plan intersects the
        // arms guards nothing (production never runs it), and a point
        // release that plans it better must not fail this suite.
        // Measured on PG 18.6: 78,432 month-arm entries of 80,000.
        println(
            "RECORDED generic month-arm entries touched: " +
                "${touched(armNode(plan, "key_index = \$9"))} of $monthArm",
        )
    }

    @Test
    fun `the probe is never server-prepared, so it never gets the generic plan`() {
        // ONE connection, so pg_prepared_statements sees every statement
        // the planner sent. pgjdbc names a statement on its fifth
        // execution on a connection; a published sample makes
        // `planTable` probe far more buckets than that.
        val sampler =
            MaintenanceSummarySampler(db.jdbi, cfg.targetBytes, cfg.minInputFiles, cfg.maxInputFiles, 3600)
        var steps = 0
        while (sampler.runOnce(50_000)) check(++steps < 2_000)
        val single =
            HikariDataSource(
                HikariConfig().apply {
                    jdbcUrl = db.jdbcUrl
                    username = db.dataSource.username
                    password = db.dataSource.password
                    maximumPoolSize = 1
                    poolName = "bucketprobe-single"
                },
            )
        single.use { ds ->
            val jdbi = Database.jdbi(ds)

            fun prepared(): List<String> =
                jdbi.withHandleUnchecked { h ->
                    h.createQuery("SELECT statement FROM pg_prepared_statements")
                        .mapTo(String::class.java).list()
                }
            val plan = CompactionService(jdbi, store, cfg).planTable(CATALOG, "ns", "t1", cfg)
            assertThat(plan.bucketsConsidered)
                .describedAs("enough probes for pgjdbc to have named the statement")
                .isGreaterThan(5)
            // MUTATION: drop `.addCustomizer(Pg.CUSTOM_PLANS_ONLY)` from
            // `bucketCandidates` and the probe appears here.
            assertThat(prepared().filter { it.contains("= ANY (ARRAY(") })
                .describedAs("the bucket probe must never be server-prepared")
                .isEmpty()

            // POLARITY: the same statement WITHOUT the customizer, run
            // past pgjdbc's threshold on the same connection, does
            // appear — so the empty list above is the customizer's doing,
            // not a check that cannot see prepared statements.
            jdbi.useHandleUnchecked { h ->
                repeat(6) {
                    h.createQuery(sql(listOf(0, 1)))
                        .bind("catalogId", catalogId).bind("tableId", tableId)
                        .bind("targetBytes", cfg.targetBytes).bind("specId", 1L)
                        .bind("scanLimit", 10).bind("limit", 10)
                        .bind("k0", 0).bind("v0", TEAM).bind("k1", 1).bind("v1", MONTH)
                        .mapToMap().list()
                }
            }
            assertThat(prepared().filter { it.contains("= ANY (ARRAY(") })
                .describedAs("control: an uncustomized probe IS server-prepared")
                .isNotEmpty()
        }
    }

    // ---- the big team: bounded by the catalog-wide bucket ---------------------

    @Test
    fun `a big team's stray bucket is bounded by the catalog-wide bucket`() {
        // THE CASE THE ARMS CANNOT MAKE CHEAP, measured honestly. Both
        // arms are common catalog-wide, so the id set is the big team's
        // (team, month) files in EVERY table: `hog_file_partition_value`
        // carries no `table_id`, and the `table_id` filter can only apply
        // at the manifest. What the new form bounds is the manifest side:
        // one sorted `= ANY` primary-key scan over those ids, instead of
        // one primary-key descent per id.
        //
        // Measured: the replaced statement 84,490 buffers (merge join of
        // both arms, then 20,064 nested-loop descents), this one 13,305
        // (the same merge join, then one sorted array scan).
        val plan = custom(sql(listOf(0, 1)), team = BIG_TEAM)
        val before = custom(replacedSql, team = BIG_TEAM)
        val bucket = catalogBucket(BIG_TEAM)
        assertThat(rootRows(plan)).isEqualTo(PER_BUCKET.toLong())
        assertThat(rootRows(before)).describedAs("same answer from both").isEqualTo(PER_BUCKET.toLong())
        assertThat(plan)
            .describedAs("no manifest-driven plan:\n%s", plan)
            .doesNotContain("Seq Scan on hog_data_file")
            .doesNotContain("hog_data_file_maintenance_size_scan")
        // Bounded by the ARMS' index entries plus one buffer per id the
        // sorted array scan visits — the arms' entries are index-only and
        // pack ~250 to a page, so the arm term is bounded by their count
        // over 100, generously.
        val budget = (bigTeamArm + monthArm) / 100 + bucket + overhead(PER_BUCKET.toLong())
        assertThat(buffers(plan))
            .describedAs("buffers against the catalog-wide bucket's budget %d:\n%s", budget, plan)
            .isLessThanOrEqualTo(budget)
        recordBudget(plan, budget)
        // RECORDED, not asserted: the replaced statement's plan guards
        // nothing in hoglake. Measured on PG 18.6: a descent per id,
        // 84,490 buffers against 13,305.
        println("RECORDED big-team probe buffers: replaced ${buffers(before)}, new ${buffers(plan)}")
    }

    /** EXPLAIN a ONE-key probe on [keyIndex] = [value] at the cap, as `bucketCandidates` binds it. */
    private fun customOneKey(
        statement: String,
        keyIndex: Int,
        value: String,
    ): String =
        db.jdbi.withHandleUnchecked { h ->
            serial(h) {
                val cap = cfg.candidateBudget + 1
                h.createQuery(explain + statement)
                    .bind("catalogId", catalogId).bind("tableId", tableId)
                    .bind("targetBytes", cfg.targetBytes).bind("specId", 1L)
                    .bind("scanLimit", cap).bind("limit", cap)
                    .bind("k$keyIndex", keyIndex).bind("v$keyIndex", value)
                    .mapTo(String::class.java).list().joinToString("\n")
            }
        }

    /** Files with [keyIndex] = [value] on the subject table only. */
    private fun tableKeyCount(
        keyIndex: Int,
        value: String,
    ): Long =
        count(
            """
            SELECT count(*) FROM hog_data_file f
              JOIN hog_file_partition_value pv
                ON pv.catalog_id = f.catalog_id AND pv.data_file_id = f.data_file_id
             WHERE f.catalog_id = :c AND f.table_id = :t AND pv.key_index = :k AND pv.value = :v
            """,
            "t" to tableId,
            "k" to keyIndex,
            "v" to value,
        )

    @Test
    fun `a one-key probe on a widely shared value is bounded by that value's catalog-wide files`() {
        // THE DOMINATING-DRIVER CASE, which the KDoc states and this pins:
        // ONE key, so there is no selective key to drive from, and the id
        // set is every file in the CATALOG with that value. Two values:
        //
        //  - month 682: 80,000 files catalog-wide, 20,000 on this table —
        //    OVER the cap, so the primary-key scan stops at the cap;
        //  - the big team: 100,000+ catalog-wide, 250 on this table —
        //    under the cap, so the scan visits every id.
        //
        // The bound is the value's catalog-wide entries (index-only,
        // ~100 to a page, generously), one buffer per id the sorted
        // `= ANY` scan visits, and the DV join per returned row. It is
        // NOT the replaced statement's cost: the new statement costs MORE
        // here. Measured with DVs seeded: month 682 33,137 vs 7,524 (27,700
        // of the new figure the per-row DV join, regime (c)); the big team
        // 13,270 vs 13,625. QE, before DVs: 21,823 vs 4,891 and 12,925 vs
        // 10,992. The old statement walked this table and stopped at the
        // cap; the new one first builds the catalog-wide id set and then
        // pays the DV join per row. Recorded, not asserted.
        for ((key, value) in listOf(1 to MONTH, 0 to BIG_TEAM)) {
            val plan = customOneKey(sql(listOf(key)), key, value)
            val before = customOneKey(ReplacedCandidateSql.sql(listOf(key)), key, value)
            val cap = (cfg.candidateBudget + 1).toLong()
            val rows = minOf(tableKeyCount(key, value), cap)
            val entries = armSize(key, value)
            assertThat(rootRows(plan)).describedAs("key %d = %s:\n%s", key, value, plan).isEqualTo(rows)
            assertThat(rootRows(before)).describedAs("same answer from both").isEqualTo(rows)
            assertThat(plan)
                .describedAs("driven by the id set:\n%s", plan)
                .contains("InitPlan 1")
                .contains("data_file_id = ANY ((InitPlan 1).col1)")
                .doesNotContain("Seq Scan on hog_data_file")
            val budget = entries / 100 + entries + overhead(rows)
            assertThat(buffers(plan))
                .describedAs("key %d = %s against the catalog-wide budget %d:\n%s", key, value, budget, plan)
                .isLessThanOrEqualTo(budget)
            recordBudget(plan, budget)
            println(
                "RECORDED one-key probe key=$key value=$value (catalog $entries, table rows $rows): " +
                    "replaced ${buffers(before)}, new ${buffers(plan)}",
            )
        }
    }

    // ---- the NULL arm, and the tuple-less bucket ------------------------------

    @Test
    fun `a NULL partition value is an IS NULL range on the lookup index`() {
        // `schema.sql`: NULL is a legal partition value. A btree indexes
        // NULLs, so `value IS NULL` is an ordinary range over the index's
        // null entries; `IS NOT DISTINCT FROM` would drive nothing.
        val plan = custom(nullSql(), team = NULL_TEAM, month = null)
        assertThat(rootRows(plan)).describedAs("the NULL-month bucket:\n%s", plan).isEqualTo(PER_BUCKET.toLong())
        assertThat(plan)
            .describedAs("the NULL arm is an index condition, not a filter:\n%s", plan)
            .contains("(key_index = 1) AND (value IS NULL)")
            .doesNotContain("Seq Scan on hog_data_file")
        val nullArm =
            count(
                "SELECT count(*) FROM hog_file_partition_value " +
                    "WHERE catalog_id = :c AND key_index = 1 AND value IS NULL",
            )
        val budget =
            perProbe * (armSize(0, NULL_TEAM) + nullArm + PER_BUCKET) + overhead(PER_BUCKET.toLong())
        assertThat(buffers(plan))
            .describedAs("bounded by the bucket's arms %d:\n%s", budget, plan)
            .isLessThanOrEqualTo(budget)
        recordBudget(plan, budget)
    }

    @Test
    fun `the tuple-less bucket keeps its ordered scan of the size index`() {
        // No arms: the statement is unchanged — V10's size index, the
        // inner ORDER BY, the `LIMIT` stopping the scan at the cap, and
        // `spec_id` filtered outside it (see `bucketCandidates`).
        val plan = custom(sql(emptyList()), specId = null, scanLimit = cfg.maxCandidates)
        assertThat(plan)
            .describedAs("the no-arm probe is the ordered scan:\n%s", plan)
            .contains("Index Scan using hog_data_file_maintenance_size_scan on hog_data_file f0")
            .doesNotContain("InitPlan")
            .doesNotContain("hog_file_partition_value")
            .doesNotContain("Seq Scan on hog_data_file")
        assertThat(rootRows(plan)).describedAs("the whole vintage, under the cap").isEqualTo(TUPLELESS.toLong())
        // The inner scan stops at its cap: the cap's rows, not the table's.
        val inner = ExplainPlan.nodes(plan).first { it.line.contains("hog_data_file_maintenance_size_scan") }
        assertThat(Regex("""actual rows=([\d.]+)""").find(inner.line)!!.groupValues[1].toDouble().toLong())
            .isEqualTo(cfg.maxCandidates.toLong())
    }
}
