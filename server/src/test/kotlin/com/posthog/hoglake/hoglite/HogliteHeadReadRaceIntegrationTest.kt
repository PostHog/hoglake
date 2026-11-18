package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableInfo
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

/**
 * Head reads of the hoglite adapter while commits land, against the live
 * engine (#537).
 *
 * `table_totals` and the head (`meta.next_snapshot`) are the only state
 * that no begin/end snapshot pins: both are exact at head only. A head
 * `getTable`/`listTables` that reads the head in one engine snapshot and
 * `table_totals` in a later one sees totals of a snapshot it does not
 * return. The aggregate over `files` that repairs this is O(files of a
 * table) per table and crosses the engine's 5 s query timeout on a large
 * table, at a race that ~1.7 commits/s makes common.
 *
 * Each test lands a commit through [HogliteInterposer] just before the
 * operation's `table_totals` read reaches the engine. The read must then
 * still come from one snapshot with the head it returns, with no
 * aggregate over `files`.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteHeadReadRaceIntegrationTest {
    private lateinit var engine: HogliteConformanceEngine
    private lateinit var proxy: HogliteInterposer

    /** Straight to the engine: the hooks commit through it, and the pinned references read through it. */
    private val store get() = engine.store

    /** Through the interposer. */
    private val pstore get() = proxy.store

    @BeforeAll
    fun start() {
        engine = HogliteConformanceEngine()
        proxy = HogliteInterposer(engine.url())
    }

    @AfterAll
    fun stop() {
        if (::proxy.isInitialized) proxy.close()
        if (::engine.isInitialized) engine.close()
    }

    // ---- helpers ------------------------------------------------------

    private fun setup(vararg tables: String): Pair<String, List<TableInfo>> {
        val c = HogliteConformanceEngine.uniqueName("headread")
        store.createCatalog(c, "s3://hoglake-it/$c", null, null)
        store.createNamespace(c, "ns", null, null)
        return c to tables.map { store.createTable(c, "ns", it, listOf(ColumnDef("id", ColType.LONG)), null, null) }
    }

    private fun append(
        c: String,
        table: String,
        path: String,
        rows: Long,
    ) = store.commit(
        c,
        CommitRequest(
            idempotencyKey = UUID.randomUUID(),
            appends =
                listOf(
                    TableAppend(
                        "ns",
                        table,
                        listOf(FileRegistration("s3://hoglake-it/$c/data/$path", rows, rows * 10, 50)),
                    ),
                ),
        ),
        null,
    )

    /** Before the first read whose body names `table_totals`, run [action]. */
    private fun beforeTotalsRead(action: () -> Unit) =
        proxy.hook({ m, _, b -> m == "POST" && "table_totals" in b }, action)

    /**
     * The request bodies since [from] that aggregate the manifest per
     * table: getTable's `FROM files WHERE table_id = ?` and the pinned
     * listing's correlated `SUM(f.record_count)`.
     */
    private fun aggregateReads(from: Int) =
        proxy.requests.drop(from).filter { "FROM files WHERE table_id = ?" in it || "SUM(f.record_count)" in it }

    private fun TableInfo.totals() = listOf(recordCount, fileCount, fileSizeBytes)

    // ---- getTable -----------------------------------------------------

    @Test
    fun `a commit before the head totals read never forces the getTable aggregate`() {
        val (c, _) = setup("t")
        append(c, "t", "a.parquet", 10)
        var landed = -1L
        val hook = beforeTotalsRead { landed = append(c, "t", "b.parquet", 7).snapshotId }
        val from = proxy.requests.size

        val info = pstore.getTable(c, "ns", "t", null, null, totals = true)

        assertThat(hook.fired).isTrue()
        assertThat(landed).isPositive()
        // The totals read came from the snapshot the head was read in, so
        // nothing had to re-derive them from the files.
        assertThat(aggregateReads(from)).isEmpty()
        val pinned = store.getTable(c, "ns", "t", info.readSnapshotId, null, totals = true)
        assertThat(info.totals()).isEqualTo(pinned.totals())
    }

    // ---- listTables ---------------------------------------------------

    @Test
    fun `a truncate before the head listing never forces the listTables aggregate`() {
        val (c, tables) = setup("a", "b")
        append(c, "a", "a.parquet", 10)
        append(c, "b", "b.parquet", 20)
        val before = store.getCatalog(c).headSnapshotId
        var landed = -1L
        val hook =
            beforeTotalsRead {
                landed = store.truncateTable(c, "ns", "a", tables[0].tableUuid, "test", "race").snapshotId
            }
        val from = proxy.requests.size

        val listing = pstore.listTables(c, "ns", null, null)

        assertThat(hook.fired).isTrue()
        assertThat(landed).isGreaterThan(before)
        assertThat(aggregateReads(from)).isEmpty()
        // A listing returns no snapshot id, so it must equal the pinned
        // listing at one of the two heads: never a mix of both.
        assertThat(listing).isIn(store.listTables(c, "ns", before, null), store.listTables(c, "ns", landed, null))
    }
}
