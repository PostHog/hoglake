package com.posthog.hoglake.service

import com.posthog.hoglake.testing.Measuring
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * WHERE `HOGLAKE_EXPIRY_PURGE_PAGE`'s default comes from, measured —
 * and what the per-row cost actually consists of.
 *
 * AGENT.md: "Measure per-row cost, not per-statement cost. A statement's
 * plan says which index it uses; the per-row cost on production's access
 * pattern is what decides whether a 50k-row batch takes 1 s or 50 s.
 * Expiry's 700 us/file versus retirement's 20 us/row for the same CTE
 * was the whole 2026-09-28 incident." This class is that measurement for
 * the statement the 2026-10-01 incident was.
 *
 * # What is measured
 *
 * One PAGE of `ExpiryService.DATA_FILE_EXPIRY_SQL` — the production
 * statement, the production page — over a 100,000-row manifest, in two
 * regimes that differ ONLY in whether the rows carry
 * `hog_file_column_stats`:
 *
 *  - WITHOUT the cascade: the heap delete, its index entries, the WAL,
 *    and the three RI triggers finding nothing to cascade.
 *  - WITH it: [STATS_PER_FILE] `hog_file_column_stats` rows each —
 *    production's 25-column tables plus the reserved row-id field, into
 *    a relation that is 66 GiB on prod-us — AND one
 *    `hog_file_partition_value` row each, under V23's index.
 *
 * ALL THREE CASCADES, because a review found this class measuring two.
 * `hog_delete_file`, `hog_file_column_stats` and
 * `hog_file_partition_value` all FK to `hog_data_file`, so a page's
 * delete fires three RI triggers per row whatever is on the other end —
 * an `EXPLAIN ANALYZE` of the production statement prints all three with
 * `calls=1000`. Seeding only the stats rows understated the cascade and,
 * worse, measured the RATIO — the figure explicitly nominated as the
 * transferable one — against the wrong denominator. Partition values are
 * a tenth of the row count of stats and carry an index of their own, so
 * they are a small term, but "small" is a measurement rather than an
 * assumption.
 *
 * The rows are SCATTERED (one ended row in every [ENDED_EVERY]), which
 * is the only honest shape: production's ended rows are whatever
 * compaction retired, so the page pays a heap fetch per row rather than
 * sharing pages. V19's header has the measurement that made a clustered
 * fixture claim 91x.
 *
 * # What it does NOT measure, and why the figure is a floor
 *
 * The fixture's `hog_file_column_stats` is a few hundred megabytes and
 * fits the container's cache; production's is 66 GiB and does not. The
 * 2026-09-28 figure for this statement against the real relation was
 * 700-870 us per file, which is roughly an order of magnitude above
 * anything a container will print. So the RATIO between the two regimes
 * is the transferable number — it says what fraction of the cost is the
 * cascade and therefore how the page should scale with a table's column
 * count — and the absolute figures are a floor.
 *
 * # Why it asserts almost nothing
 *
 * The measurement prints on every host, because a changed number is
 * worth seeing; the wall-clock assertion runs only under
 * `HOGLAKE_MEASURE=1` ([Measuring]), because a shared CI runner's
 * microseconds teach nothing. What IS asserted everywhere is the
 * relative property: a page with the stats cascade costs strictly more
 * per row than one without, and both stay inside
 * `ExpiryService.PURGE_STATEMENT_TIMEOUT`. A build in which the cascade
 * became free would mean the FK had stopped cascading, which is a
 * correctness change wearing a performance number's clothes.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExpiryPurgeCostMeasurement {
    private companion object {
        /** Files per regime. 100,000, the fixture size the doctrine asks for. */
        const val FILES = 100_000

        /**
         * One ended row in every this many, interleaved, so each costs
         * its own heap fetch. A tenth rather than production's
         * hundredth: the page needs [ExpiryService.PURGE_PAGE] eligible
         * rows to exist in a 100,000-row fixture, and 10,000 of them is
         * ten pages' worth of walk to measure over.
         */
        const val ENDED_EVERY = 10

        /**
         * Stats rows per file in the expensive regime: 25 columns plus
         * the reserved `_hog_row_id` field id, which is prod-us's
         * `main.events_raw` shape.
         */
        const val STATS_PER_FILE = 26

        /**
         * Partition values per file. One, which is `main.events_raw`'s
         * shape (a day partition) rather than a worst case — the table
         * carries ~2,800 partitions but each FILE belongs to one.
         */
        const val PARTITION_VALUES_PER_FILE = 1

        /** The relations one page reads, evicted before the #419 comparison. */
        val EVICTED =
            listOf(
                "hog_data_file",
                "hog_data_file_pkey",
                "hog_data_file_ended",
                "hog_file_column_stats",
                "hog_file_column_stats_pkey",
                "hog_file_partition_value",
                "hog_file_partition_value_pkey",
                "hog_file_partition_value_lookup",
            )

        /** Wall-clock assertions run on a measuring host only; see [Measuring]. */
        val MEASURING: Boolean = Measuring.enabled
    }

    private val db = PgTestSupport.freshDatabase()

    /** Per regime: microseconds per row for one production-sized page. */
    private val perRowMicros = linkedMapOf<String, Double>()

    /** The regime with all three cascades, kept for the #419 buffer comparison. */
    private var cascadingCatalog: Long = 0

    @AfterAll
    fun tearDown() = db.close()

    @BeforeAll
    fun seedAndMeasure() {
        val bare = seed("purge-cost-bare", cascade = false)
        val cascading = seed("purge-cost-stats", cascade = true)
        cascadingCatalog = cascading
        db.jdbi.useHandleUnchecked { h ->
            // VACUUM, not only ANALYZE: a bulk insert leaves the
            // visibility map unset, and the page would then be measuring
            // the absence of autovacuum.
            h.execute("VACUUM (ANALYZE) hog_data_file")
            h.execute("VACUUM (ANALYZE) hog_file_column_stats")
            h.execute("VACUUM (ANALYZE) hog_file_partition_value")
        }
        // One discarded page per regime first, so the figures are the
        // steady-state cost rather than the first touch.
        pageMicros(bare)
        pageMicros(cascading)
        perRowMicros["without the cascade"] = pageMicros(bare)
        perRowMicros["with $STATS_PER_FILE stats rows + $PARTITION_VALUES_PER_FILE partition value"] =
            pageMicros(cascading)
    }

    private fun seed(
        name: String,
        cascade: Boolean,
    ): Long =
        db.jdbi.withHandleUnchecked { h ->
            val catalogId =
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path, last_snapshot_id, earliest_snapshot_id) " +
                        "VALUES (:name, 's3://cost', 1000, 100) RETURNING catalog_id",
                ).bind("name", name).mapTo(Long::class.java).one()
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 0)",
                catalogId,
            )
            h.createUpdate(
                """
                INSERT INTO hog_data_file
                    (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot, path,
                     record_count, file_size_bytes, row_id_start)
                SELECT :c, g, 1, 0,
                       CASE WHEN g % :every = 0 THEN 1 ELSE NULL END,
                       's3://cost/' || md5(g::text) || '/part-' || g || '.parquet',
                       200, 12288, g::bigint * 200
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("n", FILES).bind("every", ENDED_EVERY).execute()
            if (cascade) {
                h.createUpdate(
                    """
                    INSERT INTO hog_file_column_stats
                        (catalog_id, data_file_id, field_id, value_count, null_count)
                    SELECT :c, f.data_file_id, s, 200, 0
                    FROM hog_data_file f, generate_series(1, :cols) s
                    WHERE f.catalog_id = :c
                    """,
                ).bind("c", catalogId).bind("cols", STATS_PER_FILE).execute()
                h.createUpdate(
                    """
                    INSERT INTO hog_file_partition_value
                        (catalog_id, data_file_id, key_index, value)
                    SELECT :c, f.data_file_id, k - 1, '2026-10-01'
                    FROM hog_data_file f, generate_series(1, :keys) k
                    WHERE f.catalog_id = :c
                    """,
                ).bind("c", catalogId).bind("keys", PARTITION_VALUES_PER_FILE).execute()
            }
            catalogId
        }

    /**
     * One page, timed, then rolled back so the next measurement meets
     * the same rows. The rollback is why this is a measurement rather
     * than a sweep: a committed page would leave the second regime
     * measuring a smaller eligible set than the first.
     */
    private fun pageMicros(catalogId: Long): Double =
        db.jdbi.inTransactionUnchecked { h ->
            try {
                h.execute("SET LOCAL statement_timeout = '${ExpiryService.PURGE_STATEMENT_TIMEOUT}'")
                val start = System.nanoTime()
                val rows =
                    h.createQuery(ExpiryService.DATA_FILE_EXPIRY_SQL)
                        .bind("catalogId", catalogId)
                        .bind("newEarliest", 100L)
                        .bind("page", ExpiryService.PURGE_PAGE)
                        .map { rs, _ -> rs.getInt("data_purged") }
                        .one()
                val micros = (System.nanoTime() - start) / 1_000.0
                assertThat(rows)
                    .describedAs("the page must be full, or the figure is not per-page")
                    .isEqualTo(ExpiryService.PURGE_PAGE)
                micros / rows
            } finally {
                h.rollback()
            }
        }

    @Test
    fun `the cascade is the cost, and the page fits its statement bound`() {
        val bare = perRowMicros.values.first()
        val cascading = perRowMicros.values.last()
        println(
            buildString {
                append("[expiry purge] per-row cost of one ${ExpiryService.PURGE_PAGE}-row page ")
                append("over a $FILES-row manifest (PG ${PgTestSupport.image}, warm, all three ")
                append("RI cascades present):\n")
                perRowMicros.forEach { (regime, micros) ->
                    append("  %-46s %6.1f us/row  (%5.0f ms per page)%n".format(regime, micros, micros))
                }
                val ratioLine =
                    "  ratio %.1fx — the share of the cost that is the cascade " +
                        "(hog_file_column_stats + hog_file_partition_value)%n"
                append(ratioLine.format(cascading / bare))
                append(
                    "  production's figure for this statement is 700-870 us/row (2026-09-28, a " +
                        "66 GiB stats relation that does not fit cache); this fixture's stats " +
                        "relation does fit, so the absolute numbers are a FLOOR and the ratio is " +
                        "the transferable part",
                )
            },
        )
        // Everywhere: the cascade is not free. A build where it were
        // would mean an FK had stopped cascading.
        assertThat(cascading)
            .describedAs("the stats and partition-value cascade must cost something per row")
            .isGreaterThan(bare)
        // Everywhere: a page fits its own statement bound with room. The
        // budget is a page's cost times ten, so a page that needed its
        // whole bound would make the budget meaningless.
        perRowMicros.forEach { (regime, micros) ->
            assertThat(micros * ExpiryService.PURGE_PAGE / 1_000)
                .describedAs("a page of %s must fit well inside 5 s", regime)
                .isLessThan(2_500.0)
        }
        Assumptions.assumeTrue(MEASURING, "set HOGLAKE_MEASURE=1 to apply the wall-clock budget")
        // On a quiet host only: the figures the page size was chosen
        // against. A container with a cached stats relation should be an
        // order of magnitude under production's 700-870 us.
        assertThat(cascading)
            .describedAs("a cached stats relation should be well under production's 700-870 us/row")
            .isLessThan(200.0)
    }

    /**
     * Every buffer one page touches, RI trigger work included, plus the
     * per-constraint trigger lines of its `EXPLAIN ANALYZE`. Rolled back.
     *
     * NOT the plan's `Buffers:` lines: an RI cascade runs as SPI queries
     * after the statement's plan finishes, and its buffers are attributed
     * to no node. The transaction-local relation counters
     * (`pg_stat_get_xact_blocks_fetched` / `_hit`) see every access, so
     * the figure is their delta across the page, summed over the schema's
     * relations and indexes; `read` is fetched minus hit.
     */
    private fun pageCost(
        catalogId: Long,
        sql: String,
    ): PageCost =
        db.jdbi.withHandleUnchecked { h ->
            h.begin()
            try {
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                h.execute(
                    "CREATE TEMP TABLE purge_cost_before ON COMMIT DROP AS " +
                        "SELECT oid, pg_stat_get_xact_blocks_fetched(oid) AS f, " +
                        "pg_stat_get_xact_blocks_hit(oid) AS hit FROM pg_class " +
                        "WHERE relnamespace = 'public'::regnamespace",
                )
                val start = System.nanoTime()
                val plan =
                    h.createQuery("EXPLAIN (ANALYZE, BUFFERS, COSTS false) $sql")
                        .bind("catalogId", catalogId)
                        .bind("newEarliest", 100L)
                        .bind("page", ExpiryService.PURGE_PAGE)
                        .mapTo(String::class.java)
                        .list()
                val millis = (System.nanoTime() - start) / 1_000_000.0
                val (fetched, read) =
                    h.createQuery(
                        """
                        SELECT coalesce(sum(pg_stat_get_xact_blocks_fetched(c.oid) - b.f), 0),
                               coalesce(sum((pg_stat_get_xact_blocks_fetched(c.oid) - b.f)
                                          - (pg_stat_get_xact_blocks_hit(c.oid) - b.hit)), 0)
                        FROM pg_class c JOIN purge_cost_before b ON b.oid = c.oid
                        """,
                    ).map { rs, _ -> rs.getLong(1) to rs.getLong(2) }.one()
                PageCost(fetched, read, millis, plan.filter { it.startsWith("Trigger for constraint") })
            } finally {
                h.rollback()
            }
        }

    private data class PageCost(
        val buffers: Long,
        val read: Long,
        val millis: Double,
        val triggers: List<String>,
    )

    /** Evicts the page's relations from shared buffers (`pg_buffercache`, PG 18). */
    private fun evict() =
        db.jdbi.useHandleUnchecked { h ->
            h.execute("CREATE EXTENSION IF NOT EXISTS pg_buffercache")
            for (rel in EVICTED) {
                h.createQuery("SELECT count(*) FROM pg_buffercache_evict_relation('$rel'::regclass)")
                    .mapTo(Long::class.java)
                    .one()
            }
        }

    /** The three file-dependent FKs into hog_data_file, all ON DELETE CASCADE. */
    private val dependentFks =
        listOf(
            "hog_file_column_stats" to "hog_file_column_stats_catalog_id_data_file_id_fkey",
            "hog_file_partition_value" to "hog_file_partition_value_catalog_id_data_file_id_fkey",
            "hog_delete_file" to "hog_delete_file_catalog_id_data_file_id_fkey",
        )

    private fun enforceDependentFks(enforced: Boolean) =
        db.jdbi.useHandleUnchecked { h ->
            for ((table, fk) in dependentFks) {
                h.execute("ALTER TABLE $table ALTER CONSTRAINT $fk ${if (enforced) "ENFORCED" else "NOT ENFORCED"}")
            }
        }

    /**
     * #419: what a purge page costs, and what each way of deleting a
     * page's dependents does to it. MEASURED, and nothing shipped: the
     * schema keeps the three cascades. This test disables them only on
     * its own database, for the third shape, and re-enforces them after.
     *
     * Measured on this class's fixture (100,000 files, one in ten ended,
     * 26 stats rows + 1 partition value each), one page of 1,000 files on
     * PG 18 from a pool with the page's relations evicted, counting every
     * buffer the transaction touched, RI trigger work included:
     *
     * | shape | buffers | read | page |
     * |---|---|---|---|
     * | production: the cascade deletes | 83,436 | 3,922 | 28.1 ms |
     * | explicit `= ANY (ARRAY(SELECT … FROM doomed))` beside the cascade | 112,564 | 3,947 | 34.1 ms |
     * | explicit, the three FKs `NOT ENFORCED` (no trigger) | 73,515 | 3,947 | 22.4 ms |
     *
     * Beside the cascade, an explicit delete does not retire the trigger:
     * it still fires once per file and re-reads the tuples the explicit
     * delete killed. Without the trigger the per-row probe goes — 12% of
     * the buffers — and the READS do not move: ~3.9 cold pages per file,
     * the dependents themselves, on a stats relation that does not fit
     * cache. That is the purge's cost and no delete shape changes it.
     * `NOT ENFORCED` was not shipped for its 12%: the `ALTER` takes ACCESS
     * EXCLUSIVE on `hog_data_file`, every deleter of file rows would have
     * to delete dependents itself, and a pod on the older code would
     * orphan them during a rollout.
     *
     * The assertions pin the MECHANISM, so a Postgres that changes it
     * reds this and invites the re-measure.
     */
    @Test
    fun `no way of deleting a page's dependents changes the reads`() {
        // `ALTER CONSTRAINT … NOT ENFORCED` and `pg_buffercache_evict_relation`
        // are PG 18; an older rehearsal image skips the measurement.
        Assumptions.assumeTrue(
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT current_setting('server_version_num')::int >= 180000")
                    .mapTo(Boolean::class.java).one()
            },
            "PG 18 or later",
        )
        val explicit =
            ExpiryService.DATA_FILE_EXPIRY_SQL.replace(
                "            vectors AS (",
                """
            stats AS (
                DELETE FROM hog_file_column_stats
                WHERE catalog_id = :catalogId
                  AND data_file_id = ANY (ARRAY(SELECT data_file_id FROM doomed))
            ),
            partition_values AS (
                DELETE FROM hog_file_partition_value
                WHERE catalog_id = :catalogId
                  AND data_file_id = ANY (ARRAY(SELECT data_file_id FROM doomed))
            ),
            vectors AS (""",
            )
        assertThat(explicit).describedAs("the variant must splice into the production text").contains("stats AS (")
        pageCost(cascadingCatalog, ExpiryService.DATA_FILE_EXPIRY_SQL)
        pageCost(cascadingCatalog, explicit)
        evict()
        val production = pageCost(cascadingCatalog, ExpiryService.DATA_FILE_EXPIRY_SQL)
        evict()
        val beside = pageCost(cascadingCatalog, explicit)
        val unenforced: PageCost
        try {
            enforceDependentFks(false)
            pageCost(cascadingCatalog, explicit)
            evict()
            unenforced = pageCost(cascadingCatalog, explicit)
        } finally {
            enforceDependentFks(true)
        }
        println(
            buildString {
                append("[expiry purge #419] one ${ExpiryService.PURGE_PAGE}-file page from an evicted pool, ")
                append("every buffer including RI triggers:\n")
                for ((name, cost) in listOf(
                    "cascade" to production,
                    "explicit+cascade" to beside,
                    "explicit, not enforced" to unenforced,
                )) {
                    val line = "  %-22s %7d buffers, %5d read, %5.1f ms%n"
                    append(line.format(name, cost.buffers, cost.read, cost.millis))
                    cost.triggers.forEach { append("      $it\n") }
                }
            },
        )
        val fileDependentTriggers = dependentFks.map { "Trigger for constraint ${it.second}" }

        fun dependentTriggers(c: PageCost) = c.triggers.filter { t -> fileDependentTriggers.any { t.startsWith(it) } }
        val statsTrigger = fileDependentTriggers.first()
        assertThat(dependentTriggers(beside).single { it.startsWith(statsTrigger) })
            .describedAs("beside an explicit delete the cascade still fires once per deleted file")
            .endsWith("calls=${ExpiryService.PURGE_PAGE}")
        assertThat(beside.buffers)
            .describedAs("and re-reads the tuples it finds deleted, so it costs more buffers")
            .isGreaterThan(production.buffers)
        assertThat(dependentTriggers(unenforced)).describedAs("no RI trigger without enforcement").isEmpty()
        assertThat(unenforced.buffers).describedAs("the per-row probe is all it saves").isLessThan(production.buffers)
        for ((name, cost) in listOf("beside" to beside, "unenforced" to unenforced)) {
            assertThat(cost.read)
                .describedAs(
                    "%s reads as many pages as the cascade (%d vs %d), within 5%%",
                    name,
                    cost.read,
                    production.read,
                )
                .isGreaterThanOrEqualTo(production.read - production.read / 20)
        }
    }
}
