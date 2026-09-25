package com.posthog.hoglake.service

import com.posthog.hoglake.compaction.CompactionGrouping
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * The debt sampler against a dropped table that has not been retired
 * yet (#193).
 *
 * THE COST IS THE POINT. The sampler walks a catalog's whole manifest
 * in keyset pages of `HOGLAKE_MAINTENANCE_SUMMARY_BATCH` rows (10,000)
 * at one tick per second, and it publishes NOTHING until a generation
 * completes. Since a drop stopped end-snapshotting file rows, a dropped
 * three-million-row table stays in that key space in full: three
 * hundred ticks, five minutes of wall clock, during which every
 * dashboard on the instance shows a stale sample — and the compaction
 * planner would never have planned one of those files anyway, because
 * `liveTables` excludes dropped tables.
 *
 * So the sampler skips a dropped table BY KEY RANGE: when a page's
 * first row belongs to one, the page is discarded unread and the cursor
 * jumps to `(table_id, +inf, +inf)`. The table costs ONE page however
 * many rows it holds, and that is what the tick count below measures.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MaintenanceSummaryDroppedTableIntegrationTest {
    private companion object {
        /**
         * Rows on the dropped table. Production is millions; this is
         * the size at which the UNSKIPPED tick count (rows / batch) is
         * unmistakably larger than the skipped one while the fixture
         * still seeds in a second. The assertion is derived from both
         * numbers rather than written down.
         */
        const val DROPPED_FILES = 300_000

        const val LIVE_FILES = 500

        const val BATCH = 1_000

        /** A tick cap, so a regression is a failed assertion and not a hung suite. */
        const val TICK_CAP = 400
    }

    private val db = PgTestSupport.freshDatabase()

    @AfterAll
    fun tearDown() = db.close()

    private fun sampler() =
        MaintenanceSummarySampler(
            db.jdbi,
            targetBytes = 512L * 1024 * 1024,
            minInputFiles = CompactionGrouping.DEFAULT_MIN_INPUT_FILES,
            maxInputFiles = CompactionGrouping.DEFAULT_MAX_INPUT_FILES,
            refreshSeconds = 3_600,
        )

    private fun seed(
        catalog: String,
        dropped: Boolean,
    ): Long =
        db.jdbi.withHandleUnchecked { h ->
            val catalogId =
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path, last_snapshot_id) " +
                        "VALUES (:n, 's3://' || :n, 9) RETURNING catalog_id",
                ).bind("n", catalog).mapTo(Long::class.java).one()
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) VALUES (?, 1, 1)",
                catalogId,
            )
            h.execute(
                "INSERT INTO hog_table (catalog_id, table_id, created_snapshot, dropped_snapshot) " +
                    "VALUES (?, 2, 1, ?)",
                catalogId,
                if (dropped) 5L else null,
            )
            // Table 1 (live) sorts FIRST in (table_id, size, file_id)
            // order, so the scan reaches the big table only after it has
            // done real work — which is the shape that makes the skip
            // observable rather than an artifact of where the scan
            // starts.
            for ((tableId, count) in listOf(1L to LIVE_FILES, 2L to DROPPED_FILES)) {
                h.createUpdate(
                    """
                    INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                               path, record_count, file_size_bytes, row_id_start)
                    SELECT :c, :t * 10000000 + g, :t, 1,
                           's3://' || :n || '/t' || :t || '/part-' || g || '.parquet',
                           100, 1024 + g, g
                    FROM generate_series(1, :cnt) g
                    """,
                ).bind("c", catalogId).bind("t", tableId).bind("n", catalog).bind("cnt", count).execute()
            }
            h.execute("VACUUM (ANALYZE) hog_data_file")
            catalogId
        }

    /** Tick until [catalogId] publishes a sample, or [TICK_CAP] ticks. */
    private fun ticksToPublish(catalogId: Long): Int {
        val sampler = sampler()
        var ticks = 0
        while (ticks < TICK_CAP) {
            ticks++
            sampler.runOnce(BATCH)
            val published =
                db.jdbi.withHandleUnchecked { h -> MaintenanceSummarySampler.read(h, listOf(catalogId)) }
            if (published.containsKey(catalogId)) return ticks
        }
        return ticks
    }

    @Test
    fun `a dropped table costs one page, not one page per ten thousand rows`() {
        val droppedCatalog = seed("sampler-dropped", dropped = true)
        val liveCatalog = seed("sampler-live", dropped = false)

        val skipped = ticksToPublish(droppedCatalog)
        val unskipped = ticksToPublish(liveCatalog)
        println(
            "[#193] debt sampler over $DROPPED_FILES rows at batch $BATCH: " +
                "$skipped ticks when the table is DROPPED, $unskipped when it is LIVE",
        )

        // The unskipped run is the control, and it is what the bound is
        // derived FROM: a scan that has to page the big table needs at
        // least rows/batch ticks, so a bound written as a constant would
        // have to be re-guessed every time the batch size moved.
        assertThat(unskipped)
            .describedAs("the control must really page the big table, or the comparison is empty")
            .isGreaterThan(DROPPED_FILES / BATCH / 2)
        // MUTATION: delete the `leadsInDroppedTable` branch and this
        // reds — the dropped table is paged in full and the two counts
        // converge. In production that is five minutes of every
        // dashboard on the instance showing a stale sample.
        assertThat(skipped)
            .describedAs(
                "a dropped table must cost ONE page; it cost %d ticks against the live " +
                    "table's %d",
                skipped,
                unskipped,
            )
            .isLessThan(unskipped / 10)

        // The published sample counts the LIVE table only: debt the
        // compaction planner would never plan is not debt.
        val sample =
            db.jdbi.withHandleUnchecked { h ->
                MaintenanceSummarySampler.read(h, listOf(droppedCatalog)).getValue(droppedCatalog).sample
            }
        val liveSample =
            db.jdbi.withHandleUnchecked { h ->
                MaintenanceSummarySampler.read(h, listOf(liveCatalog)).getValue(liveCatalog).sample
            }
        assertThat(sample.smallFiles)
            .describedAs("the dropped table's files must not be reported as compaction debt")
            .isLessThan(liveSample.smallFiles)
    }

    @Test
    fun `a table dropped mid-scan is skipped from the next tick`() {
        val catalogId = seed("sampler-mid", dropped = false)
        val sampler = sampler()

        // One tick starts the generation on the live table's pages.
        assertThat(sampler.runOnce(BATCH)).isTrue()

        // The drop lands mid-generation. The set of dropped tables is
        // read FRESH EVERY TICK rather than at `begin()`, so this is
        // picked up immediately instead of at the next generation —
        // which on an hourly refresh is an hour of paging rows nothing
        // will ever plan.
        //
        // MUTATION: hoist the dropped-table read into `begin()` and this
        // reds; the scan pages the whole table and the tick count
        // matches the live control above.
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "UPDATE hog_table SET dropped_snapshot = 5 WHERE catalog_id = ? AND table_id = 2",
                catalogId,
            )
        }

        var ticks = 1
        while (ticks < TICK_CAP) {
            ticks++
            sampler.runOnce(BATCH)
            val published =
                db.jdbi.withHandleUnchecked { h -> MaintenanceSummarySampler.read(h, listOf(catalogId)) }
            if (published.containsKey(catalogId)) break
        }
        println("[#193] debt sampler with a drop landing mid-generation: $ticks ticks")
        assertThat(ticks)
            .describedAs("the mid-scan drop must be picked up on the next tick, not the next generation")
            .isLessThan(DROPPED_FILES / BATCH)
    }
}
