package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/** A pinned getTable resolves the namespace incarnation at its snapshot, as a pinned listTables does. */
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
}
