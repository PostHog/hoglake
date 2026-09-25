package com.posthog.hoglake.service

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
 * It ASSERTS almost nothing, deliberately: a wall-clock bound in a test
 * suite is a flake on a busy machine. What it asserts is the SHAPE the
 * design depends on — that the cost is linear in the batch, so halving
 * the batch really halves the hold — and it prints the constants.
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

    private data class Cost(val batch: Int, val millis: Long, val walBytes: Long)

    /** One batch, timed and WAL-measured, exactly as the service issues it. */
    private fun oneBatch(n: Int): Cost =
        db.jdbi.withHandleUnchecked { h ->
            val before =
                h.createQuery("SELECT pg_current_wal_insert_lsn()").mapTo(String::class.java).one()
            h.begin()
            val start = System.nanoTime()
            val victims =
                h.createQuery(RetirementService.VICTIM_SELECT_SQL)
                    .bind("catalogId", catalogId).bind("tableId", tableId).bind("n", n)
                    .mapTo(Long::class.javaObjectType).list()
            check(victims.size == n) { "the fixture ran out of rows: wanted $n, got ${victims.size}" }
            h.createUpdate(RetirementService.DV_DELETE_SQL)
                .bind("catalogId", catalogId)
                .bindArray("victims", Long::class.javaObjectType, victims)
                .execute()
            h.createUpdate(RetirementService.DATA_DELETE_SQL)
                .bind("catalogId", catalogId)
                .bindArray("victims", Long::class.javaObjectType, victims)
                .execute()
            h.commit()
            val millis = (System.nanoTime() - start) / 1_000_000
            val wal =
                h.createQuery(
                    "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), CAST(:before AS pg_lsn))::bigint",
                ).bind("before", before).mapTo(Long::class.java).one()
            Cost(n, millis, wal)
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
     * added `hog_delete_file_data_lookup (catalog_id, data_file_id)`,
     * NON-PARTIAL, which is exactly the RI query's key. (The partial
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
        // MUTATION: `DROP INDEX hog_delete_file_data_lookup` and this
        // reds — the RI query falls to a sequential scan of
        // hog_delete_file, once per deleted data-file row, and the
        // timing assertion below goes with it.
        assertThat(riPlan)
            .describedAs("the FK's referencing-side query must be index-driven:%n%s", riPlan)
            .contains("Index Scan using hog_delete_file_data_lookup")
        assertThat(riPlan).doesNotContain("Seq Scan")
        println(
            "[#193] DV cascade: a 2,000-row batch costs ${empty.millis}ms against an EMPTY " +
                "hog_delete_file and ${populated.millis}ms against $DELETE_FILES rows " +
                "($pages heap pages); the FK's referencing side is served by " +
                "hog_delete_file_data_lookup (V2), non-partial on (catalog_id, data_file_id)",
        )
        // The claim being pinned is a SHAPE, not a constant: without
        // that index the RI trigger scans the relation per deleted row,
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

    @Test
    fun `a batch's cost is linear in its size, and here is the per-row constant`() {
        val costs = BATCHES.map { oneBatch(it) }
        for (c in costs) {
            println(
                "[#193] retirement batch of ${c.batch} rows " +
                    "($STATS_PER_FILE stats + $PARTITION_VALUES_PER_FILE partition values each): " +
                    "${c.millis}ms hold (${"%.1f".format(c.millis * 1000.0 / c.batch)} us/row), " +
                    "${c.walBytes} bytes of WAL (${c.walBytes / c.batch} B/row)",
            )
        }
        val perRowMicros = costs.map { it.millis * 1000.0 / it.batch }
        val perRowWal = costs.map { it.walBytes.toDouble() / it.batch }
        println(
            "[#193] retirement per-row cost across batches ${BATCHES.joinToString()}: " +
                perRowMicros.joinToString { "%.1f us".format(it) } + "; WAL " +
                perRowWal.joinToString { "%.0f B".format(it) },
        )

        // LINEARITY is what the design leans on: the batch size is a
        // HOLD LENGTH knob, and halving it on a timeout only helps if
        // the cost really is per-row. A superlinear cost (a plan that
        // degrades with the array size, a cascade that rescans) would
        // make the adaptive halving useless and the default unsafe.
        //
        // THE BOUND IS TIGHTER THAN THE RANGE IT SPANS, which is the
        // only way it discriminates. The batch sizes span 4x, so a
        // tolerance of 4x would be satisfied by a cost that grows
        // exactly LINEARLY IN THE BATCH — i.e. quadratically in total,
        // the very shape being excluded. 1.5x across a 4x range is a
        // real claim about flatness and still leaves room for a CI
        // machine's scheduler, which is the noise this has to survive.
        val spread = perRowMicros.max() / perRowMicros.min()
        assertThat(spread)
            .describedAs(
                "per-row cost must be FLAT across a 4x range of batch sizes, not merely " +
                    "sub-quadratic: %s us/row",
                perRowMicros.joinToString { "%.1f".format(it) },
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
        assertThat(remaining.first).isLessThanOrEqualTo(FILES - BATCHES.sum().toLong())
    }
}
