package com.posthog.hoglake.service

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Instant
import java.util.concurrent.atomic.AtomicInteger

/**
 * `snapshot` / `at_timestamp` on the namespace and table listings (#28):
 * a pinned listing must agree with the pinned per-table reads, so it
 * names what existed at the pin — including what was dropped after it —
 * and nothing created after it.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ListingAtSnapshotIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val counter = AtomicInteger(0)

    @AfterAll
    fun tearDown() = db.close()

    private val cols = listOf(ColumnDef("id", ColType.LONG))

    private fun catalog(): String {
        val cat = "pin-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        return cat
    }

    private fun head(cat: String) = catalogs.getCatalog(cat).headSnapshotId

    private fun tables(
        cat: String,
        ns: String,
        snapshot: Long? = null,
    ) = catalogs.listTables(cat, ns, snapshot).map { it.name }

    private fun namespaces(
        cat: String,
        snapshot: Long? = null,
    ) = catalogs.listNamespaces(cat, snapshot).map { it.name }

    @Test
    fun `a pinned table listing keeps tables dropped after the pin and hides ones created after it`() {
        val cat = catalog()
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "old", cols)
        val pin = head(cat)
        catalogs.createTable(cat, "ns", "new", cols)
        catalogs.dropTable(cat, "ns", "old")

        assertThat(tables(cat, "ns", pin)).containsExactly("old")
        assertThat(tables(cat, "ns")).containsExactly("new")
        // Pinned at head explicitly: the same answer as a head read.
        assertThat(tables(cat, "ns", head(cat))).containsExactly("new")
    }

    @Test
    fun `a pinned namespace listing keeps namespaces dropped after the pin and hides ones created after it`() {
        val cat = catalog()
        catalogs.createNamespace(cat, "a")
        val pin = head(cat)
        catalogs.createNamespace(cat, "b")
        catalogs.dropNamespace(cat, "a")

        assertThat(namespaces(cat, pin)).containsExactly("a")
        assertThat(namespaces(cat)).containsExactly("b")
        assertThat(namespaces(cat, head(cat))).containsExactly("b")
    }

    @Test
    fun `a dropped and recreated namespace resolves to the incarnation live at the pin`() {
        val cat = catalog()
        catalogs.createNamespace(cat, "x")
        catalogs.createTable(cat, "x", "t", cols)
        val first = head(cat)
        catalogs.dropTable(cat, "x", "t")
        catalogs.dropNamespace(cat, "x")
        val gap = head(cat)
        catalogs.createNamespace(cat, "x")
        catalogs.createTable(cat, "x", "u", cols)

        assertThat(tables(cat, "x", first)).containsExactly("t")
        assertThat(tables(cat, "x")).containsExactly("u")
        assertThat(namespaces(cat, gap)).isEmpty()
        assertThatThrownBy { tables(cat, "x", gap) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
            .hasMessageContaining("at snapshot $gap")
    }

    @Test
    fun `expired change rows still answer correctly above the floor`() {
        val cat = catalog()
        catalogs.createNamespace(cat, "kept")
        catalogs.createNamespace(cat, "gone")
        catalogs.dropNamespace(cat, "gone")
        val floor = head(cat) + 1
        catalogs.createNamespace(cat, "later")
        // What expiry leaves behind: change rows below the floor are
        // cascaded away with their snapshots, so neither the create of
        // "kept" nor the drop of "gone" is on record any more.
        db.jdbi.useHandleUnchecked { h ->
            val id = catalogs.getCatalog(cat).catalogId
            h.execute("DELETE FROM hog_snapshot_change WHERE catalog_id = ? AND snapshot_id < ?", id, floor)
            h.execute("UPDATE hog_catalog SET earliest_snapshot_id = ? WHERE catalog_id = ?", floor, id)
        }

        assertThat(namespaces(cat, floor)).containsExactly("kept", "later")
    }

    @Test
    fun `the time-travel contract matches the single-table reads`() {
        val cat = catalog()
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", cols)
        val future = Instant.parse("2999-01-01T00:00:00Z")

        assertThat(catalogs.listTables(cat, "ns", atTimestamp = future).map { it.name }).containsExactly("t")
        assertThat(catalogs.listNamespaces(cat, atTimestamp = future).map { it.name }).containsExactly("ns")
        assertThatThrownBy { catalogs.listTables(cat, "ns", 1, future) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { catalogs.listNamespaces(cat, 1, future) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { catalogs.listTables(cat, "ns", head(cat) + 1) }
            .isInstanceOf(HoglakeException.Validation::class.java)

        db.jdbi.useHandleUnchecked { h ->
            h.execute("UPDATE hog_catalog SET earliest_snapshot_id = 2 WHERE name = ?", cat)
        }
        assertThatThrownBy { catalogs.listTables(cat, "ns", 1) }
            .isInstanceOf(HoglakeException.Expired::class.java)
        assertThatThrownBy { catalogs.listNamespaces(cat, 1) }
            .isInstanceOf(HoglakeException.Expired::class.java)
    }
}
