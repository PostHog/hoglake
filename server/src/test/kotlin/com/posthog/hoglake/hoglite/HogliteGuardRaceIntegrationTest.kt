package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.TableInfo
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

/**
 * The commit guards of the hoglite adapter, against the live engine.
 *
 * A race test puts a concurrent write between a store call's pre-flight
 * reads and its transaction, through [HogliteInterposer]. Only the
 * in-transaction guard can then refuse the call. A boundary test pins a
 * strict comparison (`>`), which a `>=` would break.
 *
 * Each test kills a mutation that the unit tests let survive (see #534).
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteGuardRaceIntegrationTest {
    private lateinit var engine: HogliteConformanceEngine
    private lateinit var proxy: HogliteInterposer

    /** Straight to the engine. */
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

    private fun setup(): Pair<String, TableInfo> {
        val c = HogliteConformanceEngine.uniqueName("race")
        store.createCatalog(c, "s3://hoglake-it/$c", null, null)
        store.createNamespace(c, "ns", null, null)
        return c to store.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.LONG)), null, null)
    }

    private fun appendReq(
        c: String,
        path: String,
        key: UUID? = UUID.randomUUID(),
        read: Long? = null,
    ) = CommitRequest(
        readSnapshot = read,
        idempotencyKey = key,
        appends =
            listOf(
                TableAppend("ns", "t", listOf(FileRegistration("s3://hoglake-it/$c/data/$path", 10, 100, 50))),
            ),
    )

    private fun dvReq(
        c: String,
        fileId: Long,
        read: Long?,
        path: String,
        count: Long,
        requireUnchanged: Boolean = false,
    ) = CommitRequest(
        readSnapshot = read,
        idempotencyKey = UUID.randomUUID(),
        requireUnchangedTables = requireUnchanged,
        deletes =
            listOf(
                TableDeletes(
                    "ns",
                    "t",
                    listOf(DeleteFileRegistration(fileId, "s3://hoglake-it/$c/data/$path", count, 10)),
                ),
            ),
    )

    private fun HogliteCatalogStore.commit(
        c: String,
        r: CommitRequest,
    ): CommitResult = commit(c, r, null)

    private fun files(c: String) = store.listFiles(c, "ns", "t", null, null, null, false, null, 0, emptyMap())

    private fun count(
        c: String,
        sql: String,
        vararg args: Any?,
    ): Long = store.catalogOperation(c) { store.queryCatalog(c, sql, *args).firstRow()!![0].asLong() }

    /** A direct write on [c]'s tenant, outside every store guard. */
    private fun rawWrite(
        c: String,
        sql: String,
        vararg args: Any?,
    ) {
        store.catalogOperation(c) {
            store.runTx(c, HogliteCatalogStore.freshTxid("test"), null, HogliteTx().apply { add(sql, *args) })
        }
    }

    /** Before the write `/tx` whose body carries [marker], run [action]. */
    private fun beforeWriteTx(
        marker: String,
        action: () -> Unit,
    ) = proxy.hook({ m, p, b -> HogliteInterposer.isWriteTx(m, p, b) && marker in b }, action)

    // ---- expiry floor: re-checked inside the transaction ---------------

    @Test
    fun `a commit re-checks its read snapshot against the in-transaction expiry floor`() {
        val (c, _) = setup()
        repeat(3) { store.commit(c, appendReq(c, "floor$it.parquet")) }
        // An expiry sweep advances the floor past the read snapshot after the pre-flight check.
        val hook =
            beforeWriteTx(
                "expired-read.parquet",
            ) { rawWrite(c, "UPDATE meta SET earliest_snapshot_id = 2 WHERE id = 1") }
        assertThatThrownBy { pstore.commit(c, appendReq(c, "expired-read.parquet", read = 1)) }
            .isInstanceOf(HoglakeException.Expired::class.java)
        assertThat(hook.fired).isTrue()
        assertThat(count(c, "SELECT COUNT(*) FROM files WHERE path LIKE '%expired-read.parquet'")).isZero()
    }

    @Test
    fun `an offset commit re-checks the expiry floor inside its transaction`() {
        val (c, t) = setup()
        repeat(3) { store.commit(c, appendReq(c, "o$it.parquet")) }
        val hook =
            beforeWriteTx(
                "INSERT INTO offsets",
            ) { rawWrite(c, "UPDATE meta SET earliest_snapshot_id = 2 WHERE id = 1") }
        assertThatThrownBy { pstore.commitOffset(c, "consumer-1", t.tableUuid, 1) }
            .isInstanceOf(HoglakeException.Expired::class.java)
        assertThat(hook.fired).isTrue()
        assertThatThrownBy { store.getOffset(c, "consumer-1", t.tableUuid) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }

    // ---- read snapshot pre-flight ------------------------------------

    @Test
    fun `a read snapshot at the head is current and one past the head is refused`() {
        val (c, _) = setup()
        val head = store.commit(c, appendReq(c, "head.parquet")).snapshotId
        val atHead = store.commit(c, appendReq(c, "at-head.parquet", read = head))
        assertThat(atHead.snapshotId).isEqualTo(head + 1)
        assertThatThrownBy { store.commit(c, appendReq(c, "ahead.parquet", read = atHead.snapshotId + 1)) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("ahead of head")
    }

    // ---- content conflict window -------------------------------------

    @Test
    fun `a delete does not conflict with the change that made its read snapshot`() {
        val (c, _) = setup()
        val append = store.commit(c, appendReq(c, "window.parquet")).snapshotId
        val file = files(c).single().dataFileId
        val dv = store.commit(c, dvReq(c, file, append, "window.puffin", 1, requireUnchanged = true))
        assertThat(dv.snapshotId).isEqualTo(append + 1)
    }

    @Test
    fun `a content change between the pre-flight and the transaction is a commit conflict`() {
        val (c, _) = setup()
        val append = store.commit(c, appendReq(c, "target.parquet")).snapshotId
        val file = files(c).single().dataFileId
        val hook = beforeWriteTx("race.puffin") { store.commit(c, appendReq(c, "concurrent.parquet")) }
        assertThatThrownBy { pstore.commit(c, dvReq(c, file, append, "race.puffin", 1, requireUnchanged = true)) }
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThat(hook.fired).isTrue()
        assertThat(count(c, "SELECT COUNT(*) FROM dvs")).isZero()
    }

    // ---- deletion vector staleness -----------------------------------

    @Test
    fun `a DV supersession ends its predecessor before inserting the next live epoch`() {
        val (c, _) = setup()
        val append = store.commit(c, appendReq(c, "ordered.parquet"))
        val file = files(c).single().dataFileId
        val first = store.commit(c, dvReq(c, file, append.snapshotId, "one.puffin", 1))
        // The read snapshot is the one that registered the predecessor: not stale.
        store.commit(c, dvReq(c, file, first.snapshotId, "two.puffin", 2))
        assertThat(
            count(c, "SELECT COUNT(*) FROM dvs WHERE data_file_id = ? AND end_snapshot IS NULL", file),
        ).isEqualTo(1)
        assertThat(count(c, "SELECT COUNT(*) FROM dvs WHERE data_file_id = ?", file)).isEqualTo(2)

        fun liveCount(snapshot: Long?) =
            store.planScan(c, "ns", "t", snapshot, null, null, false).single().deleteFile!!.deleteCount
        assertThat(liveCount(first.snapshotId)).isEqualTo(1)
        assertThat(liveCount(null)).isEqualTo(2)
    }

    @Test
    fun `a vector registered between the pre-flight and the transaction makes the delete stale`() {
        val (c, _) = setup()
        val append = store.commit(c, appendReq(c, "stale.parquet")).snapshotId
        val file = files(c).single().dataFileId
        val hook = beforeWriteTx("late.puffin") { store.commit(c, dvReq(c, file, append, "early.puffin", 1)) }
        assertThatThrownBy { pstore.commit(c, dvReq(c, file, append, "late.puffin", 2)) }
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessageContaining("superseded after read snapshot")
        assertThat(hook.fired).isTrue()
        assertThat(count(c, "SELECT COUNT(*) FROM dvs WHERE path LIKE '%late.puffin'")).isZero()
    }

    // ---- receipts ------------------------------------------------------

    @Test
    fun `a same-key different-payload race is the clean typed 422`() {
        val (c, _) = setup()
        val key = UUID.randomUUID()
        val hook = beforeWriteTx("payloadA") { store.commit(c, appendReq(c, "payloadB.parquet", key = key)) }
        assertThatThrownBy { pstore.commit(c, appendReq(c, "payloadA.parquet", key = key)) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("already used with a different payload")
        assertThat(hook.fired).isTrue()
        assertThat(files(c).map { it.path }).containsExactly("s3://hoglake-it/$c/data/payloadB.parquet")
    }

    @Test
    fun `a same-key same-payload race replays the published commit`() {
        val (c, _) = setup()
        val request = appendReq(c, "same.parquet")
        var published: CommitResult? = null
        val hook = beforeWriteTx("same.parquet") { published = store.commit(c, request) }
        val replay = pstore.commit(c, request)
        assertThat(hook.fired).isTrue()
        assertThat(replay).isEqualTo(published)
        assertThat(files(c)).hasSize(1)
    }

    // ---- engine refusals -------------------------------------------------

    @Test
    fun `an engine refusal is a typed validation error with the engine's message`() {
        val (c, _) = setup()
        val head = store.getCatalog(c).headSnapshotId
        // A constraint violation that no store guard names: the engine refuses it with a labelled 422.
        assertThatThrownBy { rawWrite(c, "INSERT INTO meta(id, name, data_path) VALUES (1, 'x', 'y')") }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageStartingWith("hoglite refused the statement: ")
            .hasMessageContaining("constraint")
            .hasMessageNotContaining("{")
        assertThat(store.getCatalog(c).headSnapshotId).isEqualTo(head)
    }
}
