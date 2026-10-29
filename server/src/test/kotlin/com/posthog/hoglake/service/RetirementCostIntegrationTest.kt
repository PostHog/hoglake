package com.posthog.hoglake.service

import com.posthog.hoglake.testing.Measuring
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * WHAT A RETIREMENT BATCH COSTS, measured rather than asserted.
 *
 * Every knob on this loop is a TIME knob —
 * `HOGLAKE_RETIREMENT_BATCH` is a hold length, `HOGLAKE_RETIREMENT_
 * PAUSE_MS` is a duty cycle — so the only thing that makes them
 * tunable is a per-row cost and a per-row WAL figure an operator can
 * scale. This class produces both, on a fixture carrying the
 * PRODUCTION CASCADE RATIO, and prints them for the PR body and the
 * migration header.
 *
 * THE RATIO IS THE FIXTURE. A data-file row is a few hundred bytes; the
 * cost of deleting it is the row PLUS its cascade, which at
 * gigahog-prod-us's shape is about nine `hog_file_column_stats` rows
 * and a partition value or two. A fixture without the children measures
 * a different statement, and would make the batch default look safe at
 * a size that holds the commit lock for seconds.
 *
 * The linearity case asserts no CONSTANT, and no wall clock outside a
 * measuring host: a wall-clock bound in a test suite is a flake on a
 * busy machine, and its 1.5x microsecond spread was (#407's restore PR,
 * #443). What CI asserts is the SHAPE the design depends on — that the
 * cost is LINEAR in the batch, so the per-row cost is FLAT in it — on
 * the two measures that belong to the statement and the fixture rather
 * than the machine: shared buffers per row and WAL bytes per row. The
 * microseconds are printed everywhere and held to the same flatness
 * only under `HOGLAKE_MEASURE=1` (see [Measuring]). That flatness is
 * load-bearing twice over: it is what makes
 * HOGLAKE_RETIREMENT_BATCH a hold-length knob an operator can set, and
 * it is the whole argument for why the loop does NOT resize itself on a
 * timeout (#263 — a cancelled batch is cold, not big, and halving it
 * would halve the work with the hold).
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RetirementCostIntegrationTest {
    private companion object {
        /** Data files on the dropped table. */
        const val FILES = 120_000

        /** Per-column stats rows per file: gigahog-prod-us's 9:1 ratio. */
        const val STATS_PER_FILE = 9

        /** Partition values per file. */
        const val PARTITION_VALUES_PER_FILE = 2

        /** Deletion vectors seeded for the FK-cascade case. */
        const val DELETE_FILES = 100_000

        /** The batch sizes measured, so linearity is observable. */
        val BATCHES = listOf(2_000, 4_000, 8_000)

        /** Wall-clock assertions run on a measuring host only; see [Measuring]. */
        val MEASURING: Boolean = Measuring.enabled
    }

    private val db = PgTestSupport.freshDatabase()
    private var catalogId = 0L
    private val tableId = 1L

    @AfterAll
    fun tearDown() = db.close()

    @BeforeAll
    fun seed() {
        db.jdbi.useHandleUnchecked { h ->
            catalogId =
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path, earliest_snapshot_id, last_snapshot_id) " +
                        "VALUES ('ret-cost', 's3://ret-cost', 10, 20) RETURNING catalog_id",
                ).mapTo(Long::class.java).one()
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot, dropped_snapshot) " +
                    "VALUES (?, ?, 1, 5)",
                catalogId,
                tableId,
            )
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start)
                SELECT :c, g, :t, 1,
                       's3://ret-cost/events_raw/' || md5(g::text) || '/' || md5((g + 1)::text)
                         || '/part-' || lpad(g::text, 10, '0') || '-' || md5((g + 2)::text)
                         || '.parquet',
                       120000, 268435456, g::bigint * 120000
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("t", tableId).bind("n", FILES).execute()
            h.createUpdate(
                """
                INSERT INTO hog_file_column_stats (catalog_id, data_file_id, field_id,
                                                   value_count, null_count, lower_bound, upper_bound)
                SELECT :c, g, f, 120000, 0, decode(md5(g::text), 'hex'), decode(md5((g + 1)::text), 'hex')
                FROM generate_series(1, :n) g, generate_series(1, :fields) f
                """,
            ).bind("c", catalogId).bind("n", FILES).bind("fields", STATS_PER_FILE).execute()
            h.createUpdate(
                """
                INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value)
                SELECT :c, g, k, 'team-' || (g % 1000)
                FROM generate_series(1, :n) g, generate_series(0, :k) k
                """,
            ).bind("c", catalogId).bind("n", FILES).bind("k", PARTITION_VALUES_PER_FILE - 1).execute()
            h.execute("VACUUM (ANALYZE) hog_data_file")
            h.execute("VACUUM (ANALYZE) hog_file_column_stats")
            h.execute("VACUUM (ANALYZE) hog_file_partition_value")
            h.execute("CHECKPOINT")
        }
    }

    private data class Cost(val batch: Int, val millis: Long, val walBytes: Long, val buffers: Long)

    /**
     * Shared buffers this backend has requested from every relation in
     * the schema — heap and index, hit or read alike — as counted by
     * `pg_stat_get_xact_blocks_fetched`.
     *
     * WHY THIS COUNTER, out of the three that could carry the figure:
     *  - `pg_stat_statements` is not loaded: [PgTestSupport] starts the
     *    stock image with no `shared_preload_libraries`.
     *  - `EXPLAIN (ANALYZE, BUFFERS)` attributes buffers to PLAN NODES,
     *    and the FK cascades — `hog_file_column_stats`,
     *    `hog_file_partition_value`, `hog_delete_file` — run as RI
     *    trigger queries after the plan finishes, under no node. It
     *    would report the data-file DELETE and miss the cascade, which
     *    is most of the batch and the part a rescan would live in.
     *  - `pg_statio_user_tables` deltas see the cascade, but each backend
     *    flushes into them on its own schedule (at most once a second,
     *    and only when idle) and they sum EVERY backend, so a delta
     *    around the batch is late and carries autovacuum.
     *
     * The xact form is the third one done exactly: it reads THIS
     * backend's pending counters, which are bumped at every buffer
     * request (a hit is a fetch that found the page) by any statement
     * the backend runs, RI trigger queries included, and are not
     * flushed inside a transaction block. A delta taken between BEGIN
     * and COMMIT is therefore the batch and nothing else. FETCHED rather
     * than read, because hit-versus-read is the cache — the machine —
     * and the total is the statement and the fixture.
     */
    private fun buffersFetched(h: Handle): Long =
        h.createQuery(
            """
            SELECT coalesce(sum(pg_stat_get_xact_blocks_fetched(c.oid)), 0)::bigint
              FROM pg_class c
             WHERE c.relnamespace = current_schema()::regnamespace AND c.relkind IN ('r', 'i')
            """,
        ).mapTo(Long::class.java).one()

    /**
     * One batch, timed, WAL-measured and buffer-counted, exactly as the
     * service issues it.
     */
    private fun oneBatch(n: Int): Cost =
        db.jdbi.withHandleUnchecked { h ->
            val before =
                h.createQuery("SELECT pg_current_wal_insert_lsn()").mapTo(String::class.java).one()
            h.begin()
            val buffersBefore = buffersFetched(h)
            val start = System.nanoTime()
            val victims =
                h.createQuery(RetirementService.VICTIM_SELECT_SQL)
                    .bind("catalogId", catalogId).bind("tableId", tableId).bind("n", n)
                    .mapTo(Long::class.javaObjectType).list()
            check(victims.size == n) { "the fixture ran out of rows: wanted $n, got ${victims.size}" }
            h.createQuery(RetirementService.DV_DELETE_SQL)
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bindArray("victims", Long::class.javaObjectType, victims)
                .mapTo(Long::class.java)
                .first()
            val deleted =
                h.createQuery(RetirementService.DATA_DELETE_SQL)
                    .bind("catalogId", catalogId)
                    .bind("tableId", tableId)
                    .bindArray("victims", Long::class.javaObjectType, victims)
                    .mapTo(Long::class.java)
                    .first()
            // A measurement of a batch that deleted nothing would be the
            // flattest of all.
            check(deleted == n.toLong()) { "deleted $deleted of $n victims" }
            // Read BEFORE the commit: the counters are this transaction's
            // until then. The read is one catalog scan inside the timed
            // span, and touches no schema relation.
            val buffers = buffersFetched(h) - buffersBefore
            h.commit()
            val millis = (System.nanoTime() - start) / 1_000_000
            val wal =
                h.createQuery(
                    "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), CAST(:before AS pg_lsn))::bigint",
                ).bind("before", before).mapTo(Long::class.java).one()
            Cost(n, millis, wal, buffers)
        }

    /**
     * THE FK CASCADE, MEASURED RATHER THAN ASSUMED.
     *
     * `hog_delete_file` references `hog_data_file (catalog_id,
     * data_file_id)` `ON DELETE CASCADE`, so every deleted data-file
     * row queues an RI trigger that issues its own DELETE against
     * hog_delete_file. At 8,000 rows that is 8,000 of them, inside the
     * batch transaction, under the commit lock — and the fixture the
     * linearity case above runs on has NO deletion vectors, so it
     * measures a zero-page relation and would hide the cost entirely.
     * That is exactly the shape of measurement that makes a knob look
     * safe until production.
     *
     * THE ANSWER IS THAT IT IS INDEX-DRIVEN, and the test asks rather
     * than asserts it from reading the schema: `V2__maintenance.sql`
     * added `hog_delete_file_data_lookup (catalog_id, data_file_id)`;
     * V32 replaces it with `hog_delete_file_snapshot_lookup`, retaining
     * the same non-partial prefix for the RI query's key. (The partial
     * `hog_delete_file_one_live_per_data_file` could not have served
     * it — the trigger's query carries no `end_snapshot` predicate for
     * the planner to match — and `_live`, `_path` and the primary key
     * all lead on other columns. Nothing in V2's file says it is the
     * FK's index, which is why this case looks.)
     *
     * The same cascade is PRE-EXISTING on `ExpiryService`'s step 2,
     * which deletes data files the same way; this is where it gets a
     * number.
     */
    @Test
    fun `the DV cascade is index-driven, so a batch's cost is flat in hog_delete_file's size`() {
        val empty = oneBatch(2_000)
        // The vectors go on the HIGHEST data-file ids, which the victim
        // select (ordered by begin_snapshot, all equal here, so
        // effectively by id) reaches last. So the batch below deletes
        // rows that have NO deletion vector — and the RI trigger still
        // fires for every one of them. That is the case being measured:
        // the cost of a cascade that finds nothing, times the batch.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id,
                                             begin_snapshot, path, delete_count, file_size_bytes)
                SELECT :c, g, :t, :n - g + 1, 1,
                       's3://ret-cost/dv/' || md5(g::text) || '/del-' || g || '.puffin', 17, 4096
                FROM generate_series(1, :dvs) g
                """,
            ).bind("c", catalogId).bind("t", tableId).bind("n", FILES).bind("dvs", DELETE_FILES).execute()
            h.execute("VACUUM (ANALYZE) hog_delete_file")
            h.execute("CHECKPOINT")
        }
        val populated = oneBatch(2_000)
        val pages =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT relpages FROM pg_class WHERE relname = 'hog_delete_file'")
                    .mapTo(Long::class.java).one()
            }
        // WHAT THE RI TRIGGER ACTUALLY DOES, asked rather than assumed:
        // the planner's chosen shape for the referencing-side query
        // Postgres issues per deleted row.
        val riPlan =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "EXPLAIN (COSTS false) DELETE FROM ONLY hog_delete_file x " +
                        "WHERE :c = x.catalog_id AND :d = x.data_file_id",
                ).bind("c", catalogId).bind("d", 1L).mapTo(String::class.java).list().joinToString(" | ")
            }
        println("[#193] DV cascade RI plan: $riPlan")
        // MUTATION: drop the non-partial catalog/file lookup index
        // and this reds — the RI query falls to a sequential scan of
        // hog_delete_file, once per deleted data-file row, and the
        // timing assertion below goes with it.
        val driverIndex = "hog_delete_file_snapshot_lookup"
        assertThat(riPlan)
            .describedAs("the FK's referencing-side query must be index-driven:%n%s", riPlan)
            .contains("Index Scan using $driverIndex")
        assertThat(riPlan.substringAfter("Index Cond:", "").substringBefore(" | "))
            .describedAs("the RI index must constrain both catalog and data file:%n%s", riPlan)
            .contains("catalog_id =", "data_file_id =")
        assertThat(riPlan).doesNotContain("Seq Scan")
        println(
            "[#193] DV cascade: a 2,000-row batch costs ${empty.millis}ms against an EMPTY " +
                "hog_delete_file and ${populated.millis}ms against $DELETE_FILES rows " +
                "($pages heap pages); the FK's referencing side is served by " +
                "$driverIndex, non-partial with prefix (catalog_id, data_file_id)",
        )
        // The claim being pinned is a SHAPE, not a constant: without
        // either lookup index the RI trigger scans the relation per deleted row,
        // so the cost would grow with hog_delete_file's SIZE rather
        // than with the batch, and this bound would be nowhere near
        // enough.
        assertThat(populated.millis)
            .describedAs(
                "a batch's cost must not grow with hog_delete_file's size: %d ms empty, " +
                    "%d ms against %d rows in %d pages",
                empty.millis,
                populated.millis,
                DELETE_FILES,
                pages,
            )
            .isLessThan(maxOf(empty.millis * 4, 200L))
    }

    /**
     * The warm-up and the three measured batches, run ONCE and shared by
     * the linearity case (buffers and WAL, everywhere) and the
     * wall-clock case (microseconds, on a measuring host). Lazy so that
     * a CI run, which skips the wall-clock case before touching it,
     * consumes the fixture's rows exactly once whichever case JUnit
     * runs first.
     */
    private val measured: List<Cost> by lazy {
        // ONE DISCARDED WARM-UP BATCH, and it is the difference between
        // a test that measures the code and one that measures the first
        // batch's cache misses. The fixture is seeded, VACUUMed and
        // CHECKPOINTed and then never read, so the FIRST measured batch
        // pays every miss the later ones do not: observed 30.0, 19.8,
        // 19.0 us/row across 2,000/4,000/8,000 — monotonically
        // DECREASING, a 1.58x spread, and the two-sided 1.5x wall-clock
        // bound red about one run in seven. (Buffers and WAL
        // do not see the miss — a read and a hit are both a fetch — so
        // the warm-up is for the microseconds.) The cost being measured is
        // the per-row cascade, not the buffer cache, and the batch the
        // warm-up runs is `BATCHES.first()` so the smallest measured
        // batch is the one whose pages are already touched.
        //
        // It is DISCARDED rather than reported, because a warm-up's
        // number is not a measurement of anything: averaging it in
        // would put the cold miss back into the figure the KDoc quotes.
        oneBatch(BATCHES.first())
        val costs = BATCHES.map { oneBatch(it) }
        for (c in costs) {
            println(
                "[#193] retirement batch of ${c.batch} rows " +
                    "($STATS_PER_FILE stats + $PARTITION_VALUES_PER_FILE partition values each): " +
                    "${c.millis}ms hold (${"%.1f".format(c.millis * 1000.0 / c.batch)} us/row), " +
                    "${c.walBytes} bytes of WAL (${c.walBytes / c.batch} B/row), " +
                    "${c.buffers} shared buffers (${"%.1f".format(c.buffers.toDouble() / c.batch)}/row)",
            )
        }
        costs
    }

    private fun perRowMicros(costs: List<Cost>): List<Double> = costs.map { it.millis * 1000.0 / it.batch }

    @Test
    fun `a batch's cost is linear in its size, and here is the per-row constant`() {
        val costs = measured
        val perRowMicros = perRowMicros(costs)
        val perRowWal = costs.map { it.walBytes.toDouble() / it.batch }
        val perRowBuffers = costs.map { it.buffers.toDouble() / it.batch }
        println(
            "[#193] retirement per-row cost across batches ${BATCHES.joinToString()}: " +
                perRowMicros.joinToString { "%.1f us".format(it) } + "; WAL " +
                perRowWal.joinToString { "%.0f B".format(it) } + "; buffers " +
                perRowBuffers.joinToString { "%.1f".format(it) },
        )

        // LINEARITY is what the design leans on: the batch size is a
        // HOLD LENGTH knob, and the per-row cost being flat in it is
        // what says a cancelled batch was COLD rather than too big —
        // which is why a timeout is counted and retried at the same
        // size instead of halved (#263). A superlinear cost (a plan
        // that degrades with the array size, a cascade that rescans)
        // would break that reading and make the default unsafe: the
        // batch would genuinely be the problem, and the knob would have
        // to come down.
        //
        // WHAT CI ASSERTS IS FLATNESS IN THE TWO MEASURES THAT ARE THE
        // STATEMENT'S, NOT THE MACHINE'S: shared buffers per row (every
        // heap and index page request the batch makes, cascade included
        // — see [buffersFetched]) and WAL bytes per row. Both are fixed
        // by the plan and the fixture; neither moves with the
        // scheduler, the cache or a noisy neighbour, so a bound on them
        // fails only when the statement changed. The wall-clock spread
        // is the same claim in microseconds, and it is asserted in its
        // own case below, on a measuring host only (see [Measuring]): on
        // a shared runner it is scheduler noise, and it flaked here.
        //
        // THE BOUND IS TIGHTER THAN THE RANGE IT SPANS, which is the
        // only way it discriminates. The batch sizes span 4x, so a
        // tolerance of 4x would be satisfied by a cost that grows
        // exactly LINEARLY IN THE BATCH — i.e. quadratically in total,
        // the very shape being excluded. 1.5x across a 4x range is a
        // real claim about flatness, and it is the bound WAL carries.
        //
        // BUFFERS CARRY A MUCH TIGHTER ONE, because they are a count
        // rather than a measurement: 100,850 / 201,631 / 403,231 at
        // 2,000 / 4,000 / 8,000 (50.4 per row, a 1.0005x spread — the
        // ~50-buffer remainder is the per-batch fixed cost), and it
        // does not move between runs. 1.5x would miss the regression
        // that actually threatens this loop, which is not a superlinear
        // cascade but a PER-BATCH RESCAN: a fixed cost per batch makes
        // the per-row figure FALL with the batch, so its spread is
        // bounded by how big the rescan is against 50 buffers a row.
        //
        // MUTATION: `(data_file_id + 0) = ANY(:victims)` in
        // DATA_DELETE_SQL (the victim column wrapped, so the pkey can
        // no longer be probed per victim and the DELETE walks every row
        // of the catalog instead) costs ~3,900 extra buffers a batch on
        // this fixture: 52.4 / 51.4 / 50.9 per row, a 1.03x spread —
        // green at 1.5, red here. The fixture, the WAL (2,966 / 2,968 /
        // 2,965 B/row) and the row counts are unchanged by it; only the
        // plan moved. On production's 13.9M-row tables the same mutation
        // is a full rescan per 8,000-row batch.
        val bufferSpread = perRowBuffers.max() / perRowBuffers.min()
        assertThat(bufferSpread)
            .describedAs(
                "shared buffers per row must be FLAT across a 4x range of batch sizes: %s",
                perRowBuffers.joinToString { "%.1f".format(it) },
            )
            .isLessThan(1.01)
        val walSpread = perRowWal.max() / perRowWal.min()
        assertThat(walSpread)
            .describedAs(
                "WAL per row must be FLAT across a 4x range of batch sizes: %s B/row",
                perRowWal.joinToString { "%.0f".format(it) },
            )
            .isLessThan(1.5)
        // WAL per row is the number an operator sizes max_wal_size and
        // free storage against. It is bytes rather than scheduling, so
        // it is held to a much tighter bound — and ADJACENT sizes are
        // what is compared, not the extremes: WAL per row DRIFTS with
        // the batch (bigger batches amortise the per-transaction
        // records and dirty more full pages per commit), so a max/min
        // over a 4x range folds three separate facts into one number
        // and would go soft exactly where the default sits. Each step
        // doubles the batch, so each comparison is a doubling.
        for ((smaller, bigger) in perRowWal.zipWithNext()) {
            assertThat(bigger / smaller)
                .describedAs(
                    "WAL per row must not move materially across a doubling of the batch: %s",
                    perRowWal.joinToString { "%.0f".format(it) },
                )
                .isBetween(0.8, 1.25)
        }
        // THE NUMBER AN OPERATOR BUDGETS WITH is the one at the
        // DEFAULT batch, not the smallest: it is the biggest of the
        // three, and a plan sized on the 2,000-row figure would be
        // short.
        val atDefaultBatch = perRowWal.last()
        println(
            "[#193] retirement WAL at the largest measured batch (${BATCHES.last()}): " +
                "${"%.0f".format(atDefaultBatch)} B/row => " +
                "${"%.1f".format(atDefaultBatch * 3_008_849 / 1e9)} GB for gigahog-prod-us's " +
                "3,008,849-row main.events_raw, RETIREMENT ALONE (cleanup's settles, the audit " +
                "stream and the post-retirement VACUUM are on top of it)",
        )

        // Every cascade row really went, which is what makes the cost
        // above the cost of the whole operation rather than of one
        // DELETE.
        val remaining =
            db.jdbi.withHandleUnchecked { h ->
                val files =
                    h.createQuery("SELECT count(*) FROM hog_data_file WHERE catalog_id = :c")
                        .bind("c", catalogId).mapTo(Long::class.java).one()
                val stats =
                    h.createQuery("SELECT count(*) FROM hog_file_column_stats WHERE catalog_id = :c")
                        .bind("c", catalogId).mapTo(Long::class.java).one()
                val values =
                    h.createQuery("SELECT count(*) FROM hog_file_partition_value WHERE catalog_id = :c")
                        .bind("c", catalogId).mapTo(Long::class.java).one()
                Triple(files, stats, values)
            }
        // Derived from what is LEFT rather than from a running total:
        // the two cases share a fixture and JUnit does not promise an
        // order, so an absolute expectation would couple them.
        assertThat(remaining.second)
            .describedAs("every retired file's stats rows went with it")
            .isEqualTo(remaining.first * STATS_PER_FILE)
        assertThat(remaining.third)
            .describedAs("and its partition values")
            .isEqualTo(remaining.first * PARTITION_VALUES_PER_FILE)
        // `+ BATCHES.first()` for the linearity case's discarded
        // warm-up batch, which consumes rows like any other.
        assertThat(remaining.first)
            .isLessThanOrEqualTo(FILES - BATCHES.sum().toLong() - BATCHES.first().toLong())
    }

    /**
     * The linearity case's claim in MICROSECONDS, on a quiet host only
     * (see [Measuring]): the figure HOGLAKE_RETIREMENT_BATCH is a hold
     * length in, and the one the 19.5 us/row in the README and AGENT.md
     * was read off. Same batches, same 1.5x across the 4x range, and the
     * same reason the bound is tighter than the range. On a shared CI
     * runner it is scheduler noise — it red #407's restore PR and #443
     * while buffers and WAL per row did not move — so CI skips it and
     * carries the shape on [buffersFetched] and WAL instead.
     */
    @Test
    fun `on a measuring host, a batch's wall-clock cost per row is flat in its size`() {
        Assumptions.assumeTrue(MEASURING, "set HOGLAKE_MEASURE=1 to apply the wall-clock spread")
        val perRowMicros = perRowMicros(measured)
        assertThat(perRowMicros.max() / perRowMicros.min())
            .describedAs(
                "per-row cost must be FLAT across a 4x range of batch sizes, not merely " +
                    "sub-quadratic: %s us/row",
                perRowMicros.joinToString { "%.1f".format(it) },
            )
            .isLessThan(1.5)
    }
}
