package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Instant
import java.util.UUID

/** Every pinned read resolves the namespace incarnation at its snapshot, as a pinned listTables does. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HoglitePinnedNamespaceIntegrationTest {
    private lateinit var engine: HogliteConformanceEngine
    private val store get() = engine.store

    @BeforeAll
    fun start() {
        engine = HogliteConformanceEngine()
    }

    @AfterAll
    fun close() {
        if (::engine.isInitialized) engine.close()
    }

    private fun freshCatalog(): String {
        val name = HogliteConformanceEngine.uniqueName("pinned-ns")
        store.createCatalog(name, "s3://hoglake-it/$name")
        return name
    }

    @Test
    fun `a pinned getTable resolves the namespace incarnation at its snapshot`() {
        val catalog = freshCatalog()
        val first = store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val pinned = table.snapshotId!!
        assertThat(store.getTable(catalog, "ns", "t", snapshot = pinned).tableUuid).isEqualTo(table.tableUuid)

        // A namespace drops only when empty; the recreate is a new incarnation.
        store.dropTable(catalog, "ns", "t", expectedTableUuid = table.tableUuid, author = "test", message = "drop")
        val dropped = store.dropNamespace(catalog, "ns", author = "test", message = "drop").snapshotId
        val second = store.createNamespace(catalog, "ns")
        assertThat(second.namespaceId).isNotEqualTo(first.namespaceId)
        val t2 = store.createTable(catalog, "ns", "t2", listOf(ColumnDef("id", ColType.LONG)))
        assertThat(store.getTable(catalog, "ns", "t2").tableUuid).isEqualTo(t2.tableUuid)

        val read = store.getTable(catalog, "ns", "t", snapshot = pinned)
        assertThat(read.tableUuid).isEqualTo(table.tableUuid)
        assertThat(read.readSnapshotId).isEqualTo(pinned)
        assertThat(store.listTables(catalog, "ns", snapshot = pinned).map { it.name }).containsExactly("t")

        // At the drop snapshot no incarnation is alive: the old one is dropped, the new one not yet created.
        assertThatThrownBy { store.getTable(catalog, "ns", "t", snapshot = dropped) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
            .hasMessageContaining("namespace 'ns' in catalog '$catalog' at snapshot $dropped")
    }

    @Test
    fun `a pinned getTable picks the incarnation at its snapshot when both hold the same table name`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val first = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val pinned = first.snapshotId!!

        store.dropTable(catalog, "ns", "t", expectedTableUuid = first.tableUuid, author = "test", message = "drop")
        store.dropNamespace(catalog, "ns", author = "test", message = "drop")
        store.createNamespace(catalog, "ns")
        val recreated = store.getCatalog(catalog).headSnapshotId
        val second = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        assertThat(second.tableUuid).isNotEqualTo(first.tableUuid)

        assertThat(store.getTable(catalog, "ns", "t").tableUuid).isEqualTo(second.tableUuid)
        val read = store.getTable(catalog, "ns", "t", snapshot = pinned)
        assertThat(read.tableUuid).isEqualTo(first.tableUuid)
        assertThat(read.readSnapshotId).isEqualTo(pinned)
        assertThat(store.listTables(catalog, "ns", snapshot = pinned).map { it.name }).containsExactly("t")

        // The new incarnation is alive at its creation snapshot, but its "t" comes later: the table is missing.
        assertThatThrownBy { store.getTable(catalog, "ns", "t", snapshot = recreated) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
            .hasMessageContaining("table 'ns.t' in catalog '$catalog' at snapshot $recreated")
    }

    @Test
    fun `a pinned getTable finds a table whose namespace was dropped and never recreated`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val pinned = table.snapshotId!!
        store.dropTable(catalog, "ns", "t", expectedTableUuid = table.tableUuid, author = "test", message = "drop")
        store.dropNamespace(catalog, "ns", author = "test", message = "drop")

        // Unlike a live lookup, which is NotFound, a pinned read sees the table as it was at its snapshot.
        assertThatThrownBy { store.getTable(catalog, "ns", "t") }.isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(store.getTable(catalog, "ns", "t", snapshot = pinned).tableUuid).isEqualTo(table.tableUuid)
    }

    @Test
    fun `a pinned getTable before the namespace existed is not found at that snapshot`() {
        val catalog = freshCatalog()
        val before = store.getCatalog(catalog).headSnapshotId
        store.createNamespace(catalog, "ns")
        store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))

        assertThatThrownBy { store.getTable(catalog, "ns", "t", snapshot = before) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
            .hasMessageContaining("namespace 'ns' in catalog '$catalog' at snapshot $before")
    }

    // Every pinned read resolves the namespace incarnation at its snapshot: the
    // file reads, the changefeed and scan planning agree with getTable/listTables.

    /** ns.t holds one file, then t and ns are dropped; with [recreated], ns comes back with a new, empty t. */
    private class PinnedHistory(
        val catalog: String,
        val tableUuid: UUID,
        val fileId: Long,
        /** The head before ns existed. */
        val before: Long,
        /** The snapshot that landed the file. */
        val pinned: Long,
        /** [pinned]'s snapshot time; every later snapshot is strictly after it. */
        val pinnedAt: Instant,
        /** The snapshot that dropped ns. */
        val dropped: Long,
        /** The snapshot that recreated ns, before its t exists. */
        val recreated: Long?,
    )

    private fun pinnedHistory(recreate: Boolean): PinnedHistory {
        val catalog = freshCatalog()
        val before = store.getCatalog(catalog).headSnapshotId
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val file = FileRegistration("s3://hoglake-it/$catalog/data/f1.parquet", 10, 1000, 50)
        val pinned =
            store.commit(catalog, CommitRequest(appends = listOf(TableAppend("ns", "t", listOf(file))))).snapshotId
        val fileId = store.listFiles(catalog, "ns", "t").single().dataFileId
        val pinnedAt =
            store.listSnapshots(catalog, after = 0, limit = 100).first.first { it.snapshotId == pinned }.snapshotTime
        // The drop must land strictly after the pin's time, so at_timestamp = pinnedAt resolves to the pin.
        while (!Instant.now().isAfter(pinnedAt.plusMillis(1))) Thread.sleep(1)
        store.dropTable(catalog, "ns", "t", expectedTableUuid = table.tableUuid, author = "test", message = "drop")
        val dropped = store.dropNamespace(catalog, "ns", author = "test", message = "drop").snapshotId
        var recreated: Long? = null
        if (recreate) {
            store.createNamespace(catalog, "ns")
            recreated = store.getCatalog(catalog).headSnapshotId
            store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        }
        return PinnedHistory(catalog, table.tableUuid, fileId, before, pinned, pinnedAt, dropped, recreated)
    }

    /** The reads that take `at_timestamp`, by name, each pinned at [at]. */
    private fun timestampReads(
        h: PinnedHistory,
        at: Instant,
    ): Map<String, () -> Any> =
        mapOf(
            "getTable" to { store.getTable(h.catalog, "ns", "t", atTimestamp = at) },
            "listTables" to { store.listTables(h.catalog, "ns", atTimestamp = at) },
            "listFiles" to { store.listFiles(h.catalog, "ns", "t", atTimestamp = at) },
            "partitionValues" to { store.partitionValues(h.catalog, "ns", "t", atTimestamp = at) },
            "fileStats" to { store.fileStats(h.catalog, "ns", "t", h.fileId, atTimestamp = at) },
            "planScan" to { store.planScan(h.catalog, "ns", "t", atTimestamp = at) },
        )

    /** The table-level reads that take a pin, by name, each pinned at [at] (null: a head read). */
    private fun tableReads(
        h: PinnedHistory,
        at: Long?,
    ): Map<String, () -> Any> =
        mapOf(
            "getTable" to { store.getTable(h.catalog, "ns", "t", snapshot = at) },
            "listFiles" to { store.listFiles(h.catalog, "ns", "t", snapshot = at) },
            "partitionValues" to { store.partitionValues(h.catalog, "ns", "t", snapshot = at) },
            "fileStats" to { store.fileStats(h.catalog, "ns", "t", h.fileId, snapshot = at) },
            "planScan" to { store.planScan(h.catalog, "ns", "t", snapshot = at) },
            "changes" to { store.changes(h.catalog, "ns", "t", fromSnapshot = 0, toSnapshot = at) },
        )

    private fun assertReadsNotFound(
        reads: Map<String, () -> Any>,
        message: String,
    ) {
        for ((name, read) in reads) {
            assertThatThrownBy { read() }
                .describedAs(name)
                .isInstanceOf(HoglakeException.NotFound::class.java)
                .hasMessageContaining(message)
        }
    }

    /** At the pin, every path serves the first incarnation's t and its one file. */
    private fun assertPinnedReadsServeTheFile(h: PinnedHistory) {
        assertThat(store.getTable(h.catalog, "ns", "t", snapshot = h.pinned).tableUuid).isEqualTo(h.tableUuid)
        assertThat(store.listTables(h.catalog, "ns", snapshot = h.pinned).map { it.tableUuid })
            .containsExactly(h.tableUuid)
        assertThat(store.listFiles(h.catalog, "ns", "t", snapshot = h.pinned).map { it.dataFileId })
            .containsExactly(h.fileId)
        assertThat(store.partitionValues(h.catalog, "ns", "t", snapshot = h.pinned).fields).isEmpty()
        assertThat(store.fileStats(h.catalog, "ns", "t", h.fileId, snapshot = h.pinned).dataFileId).isEqualTo(h.fileId)
        assertThat(store.planScan(h.catalog, "ns", "t", snapshot = h.pinned).map { it.dataFile.dataFileId })
            .containsExactly(h.fileId)
        val feed = store.changes(h.catalog, "ns", "t", fromSnapshot = 0, toSnapshot = h.pinned)
        assertThat(feed.tableUuid).isEqualTo(h.tableUuid)
        assertThat(feed.files.map { it.dataFileId }).containsExactly(h.fileId)
    }

    @Test
    fun `pinned file reads, changefeed and scan planning resolve the namespace incarnation at their snapshot`() {
        val h = pinnedHistory(recreate = true)
        assertPinnedReadsServeTheFile(h)

        // Head reads see the new incarnation: a t with no files, so the pinned file is not there.
        assertThat(store.getTable(h.catalog, "ns", "t").tableUuid).isNotEqualTo(h.tableUuid)
        assertThat(store.listFiles(h.catalog, "ns", "t")).isEmpty()
        assertThat(store.planScan(h.catalog, "ns", "t")).isEmpty()
        assertThatThrownBy { store.fileStats(h.catalog, "ns", "t", h.fileId) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
            .hasMessageContaining("file ${h.fileId}")
    }

    @Test
    fun `an at_timestamp pin resolves the namespace incarnation at its snapshot, as the snapshot pin does`() {
        val h = pinnedHistory(recreate = true)
        assertThat(store.resolveSnapshot(h.catalog, h.pinnedAt).snapshotId).isEqualTo(h.pinned)
        assertThat(store.getTable(h.catalog, "ns", "t", atTimestamp = h.pinnedAt).tableUuid).isEqualTo(h.tableUuid)
        val bySnapshot =
            tableReads(h, h.pinned) + ("listTables" to { store.listTables(h.catalog, "ns", snapshot = h.pinned) })
        for ((name, read) in timestampReads(h, h.pinnedAt)) {
            assertThat(read()).describedAs(name).isEqualTo(bySnapshot.getValue(name)())
        }
    }

    @Test
    fun `pinned file reads at the drop snapshot and before the namespace existed are NotFound naming the snapshot`() {
        val h = pinnedHistory(recreate = true)
        // At the drop snapshot no incarnation is alive; before `before` none existed yet.
        for (at in listOf(h.dropped, h.before)) {
            val message = "namespace 'ns' in catalog '${h.catalog}' at snapshot $at"
            assertReadsNotFound(tableReads(h, at), message)
            assertReadsNotFound(mapOf("listTables" to { store.listTables(h.catalog, "ns", snapshot = at) }), message)
        }
    }

    @Test
    fun `pinned file reads at the recreate snapshot see the new incarnation, which has no t yet`() {
        val h = pinnedHistory(recreate = true)
        val at = h.recreated!!
        assertThat(store.listTables(h.catalog, "ns", snapshot = at)).isEmpty()
        assertReadsNotFound(tableReads(h, at), "table 'ns.t' in catalog '${h.catalog}' at snapshot $at")
    }

    @Test
    fun `pinned file reads find a table whose namespace was dropped and never recreated`() {
        val h = pinnedHistory(recreate = false)
        // Live, the namespace is gone ...
        assertReadsNotFound(tableReads(h, at = null), "namespace 'ns' in catalog '${h.catalog}'")
        // ... but a pinned reader still sees it as it was.
        assertPinnedReadsServeTheFile(h)
    }
}
