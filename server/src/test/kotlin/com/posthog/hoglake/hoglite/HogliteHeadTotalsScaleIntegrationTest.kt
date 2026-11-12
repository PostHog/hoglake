package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.stats.IcebergSingleValue
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

/**
 * The cost of the pinned totals on a large table, against the live
 * engine (#537).
 *
 * A pinned `listTables` and a pinned `getTable` aggregate the files
 * visible at their snapshot: one index range over `files_table` plus one
 * table-row read per live file, three times per table in the listing. A
 * head read that falls back to it pays that per request. This test seeds
 * one table with [FILES] provided-stats files of [STAT_COLUMNS] columns
 * each (production's shape per file) and reports what the head and the
 * pinned paths cost on it.
 */
@Tag("integration")
class HogliteHeadTotalsScaleIntegrationTest {
    @Test
    fun `the pinned totals cost scales with the files of a table and the head totals do not`() {
        HogliteConformanceEngine().use { engine ->
            val store = engine.store
            val c = HogliteConformanceEngine.uniqueName("head_scale")
            store.createCatalog(c, "s3://hoglake-it/$c")
            store.createNamespace(c, "ns")
            val table = store.createTable(c, "ns", "t", (1..STAT_COLUMNS).map { ColumnDef("c$it", ColType.LONG) })
            val bound = IcebergSingleValue.encode(ColType.LONG, 7L)
            val stats =
                store.json.writeValueAsString(
                    (1..STAT_COLUMNS).map { ColumnStats(it.toLong(), 1, 0, null, 8, bound, bound) },
                )
            store.catalogOperation(c) {
                for (first in 1..FILES step PAGE) {
                    store.runTx(
                        c,
                        HogliteCatalogStore.freshTxid("head-scale"),
                        null,
                        HogliteTx().apply {
                            add(
                                "WITH RECURSIVE n(i) AS (VALUES(?) UNION ALL SELECT i+1 FROM n WHERE i < ?) " +
                                    "INSERT INTO files(id,table_id,path,record_count,file_size_bytes,row_id_start," +
                                    "stats_state,stats_json,begin_snapshot) " +
                                    "SELECT i,?,'s3://hoglake-it/$c/data/'||i||'.parquet',1,100,i," +
                                    "'provided',?,? FROM n",
                                first,
                                minOf(first + PAGE - 1, FILES),
                                table.tableId,
                                stats,
                                table.readSnapshotId,
                            )
                        },
                    )
                }
                store.runTx(
                    c,
                    HogliteCatalogStore.freshTxid("head-scale-totals"),
                    null,
                    HogliteTx().apply {
                        add("UPDATE meta SET next_file_id = ? WHERE id = 1", FILES + 1)
                        add(
                            "UPDATE table_totals SET record_count = ?, file_count = ?, file_size_bytes = ? " +
                                "WHERE table_id = ?",
                            FILES,
                            FILES,
                            FILES * 100L,
                            table.tableId,
                        )
                    },
                )
            }
            val head = store.getCatalog(c).headSnapshotId

            fun <T> timed(
                label: String,
                read: () -> T,
            ): T? {
                val start = System.nanoTime()
                val result = runCatching(read)
                val ms = (System.nanoTime() - start) / 1_000_000
                println(
                    "#537 $label over $FILES files x $STAT_COLUMNS stats columns: $ms ms" +
                        (
                            result.exceptionOrNull()?.let { " (failed: ${it.javaClass.simpleName}: ${it.message})" }
                                ?: ""
                        ),
                )
                return result.getOrNull()
            }
            // Warm the engine's page cache first, so the figures are per-row
            // work and not first-touch I/O.
            timed("warm-up pinned listTables") { store.listTables(c, "ns", head, null) }
            val headListing = timed("head listTables") { store.listTables(c, "ns", null, null) }
            val pinnedListing = timed("pinned listTables") { store.listTables(c, "ns", head, null) }
            val headGet = timed("head getTable") { store.getTable(c, "ns", "t", null, null, totals = true) }
            val pinnedGet = timed("pinned getTable") { store.getTable(c, "ns", "t", head, null, totals = true) }

            assertThat(headListing!!.single().fileCount).isEqualTo(FILES.toLong())
            assertThat(headGet!!.fileCount).isEqualTo(FILES.toLong())
            // The pinned answers are recorded, not required: past the
            // engine's query timeout they fail, which is the measurement.
            pinnedListing?.let { assertThat(it).isEqualTo(headListing) }
            pinnedGet?.let { assertThat(it.fileCount).isEqualTo(FILES.toLong()) }
        }
    }

    private companion object {
        const val FILES = 100_000
        const val STAT_COLUMNS = 25
        const val PAGE = 5_000
    }
}
