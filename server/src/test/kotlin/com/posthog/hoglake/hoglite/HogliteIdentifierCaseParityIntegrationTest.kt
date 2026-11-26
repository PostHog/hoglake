package com.posthog.hoglake.hoglite

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.service.AlterBackend
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.service.ViewBackend
import com.posthog.hoglake.service.ViewService
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.PostgresCatalogStore
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Letter case in identifiers, the same scenario on both stores.
 *
 * The contract pinned here: catalog names are lowercase only; namespace,
 * table, view and column names and consumer ids allow mixed case and are
 * compared exactly (byte for byte) on create, uniqueness, lookup and
 * rename. `Foo` and `foo` are two objects, and `FOO` finds neither.
 * Nothing on either store folds case (ReservedNamespaces' KDoc: "Both
 * backends compare namespace names case-sensitively"; docs/auth-tenancy.md).
 *
 * Listing order is the one known divergence (#671): PostgreSQL sorts by the
 * database collation, SQLite by bytes. Its test is HogliteListingOrderParityTest,
 * outside the live-class pattern, because a skipped live test fails hoglite-live.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteIdentifierCaseParityIntegrationTest {
    private val db by lazy { PgTestSupport.freshDatabase().also { pgStarted = true } }
    private var pgStarted = false
    private val engine by lazy { HogliteConformanceEngine().also { engineStarted = true } }
    private var engineStarted = false
    private var counter = 0

    @AfterAll
    fun close() {
        if (pgStarted) db.close()
        if (engineStarted) engine.close()
    }

    private class Backend(
        val store: CatalogStore,
        val alters: AlterBackend,
        val views: ViewBackend,
    )

    private fun backend(lite: Boolean): Backend =
        if (lite) {
            Backend(engine.store, HogliteAlters(engine.store), HogliteViews(engine.store))
        } else {
            Backend(
                PostgresCatalogStore(CatalogService(db.jdbi), CommitService(db.jdbi), ScanService(db.jdbi)),
                AlterService(db.jdbi),
                ViewService(db.jdbi),
            )
        }

    private fun fixture(
        lite: Boolean,
        body: (Backend, String) -> Unit,
    ) {
        val b = backend(lite)
        val cat = "ident-case${counter++}"
        b.store.createCatalog(cat, "s3://hoglake-it/$cat")
        body(b, cat)
    }

    private fun notFound(call: () -> Unit) {
        assertThatThrownBy { call() }.isInstanceOf(HoglakeException.NotFound::class.java)
    }

    // ---- catalogs ---------------------------------------------------------

    @Test fun `PostgreSQL catalog names are lowercase only and looked up exactly`() = catalogs(false)

    @Test fun `hoglite catalog names are lowercase only and looked up exactly`() = catalogs(true)

    private fun catalogs(lite: Boolean) =
        fixture(lite) { b, cat ->
            assertThatThrownBy { b.store.createCatalog("Ident-Upper$counter", "s3://hoglake-it/upper$counter") }
                .isInstanceOf(HoglakeException.Validation::class.java)
                .hasMessageContaining("invalid catalog name")
            assertThat(b.store.getCatalog(cat).name).isEqualTo(cat)
            notFound { b.store.getCatalog(cat.uppercase()) }
        }

    // ---- namespaces -------------------------------------------------------

    @Test fun `PostgreSQL namespaces differing only by case are distinct`() = namespaces(false)

    @Test fun `hoglite namespaces differing only by case are distinct`() = namespaces(true)

    private fun namespaces(lite: Boolean) =
        fixture(lite) { b, cat ->
            val upper = b.store.createNamespace(cat, "Events")
            val lower = b.store.createNamespace(cat, "events")
            assertThat(upper.namespaceId).isNotEqualTo(lower.namespaceId)
            assertThat(b.store.getNamespace(cat, "Events").namespaceId).isEqualTo(upper.namespaceId)
            assertThat(b.store.getNamespace(cat, "events").namespaceId).isEqualTo(lower.namespaceId)
            notFound { b.store.getNamespace(cat, "EVENTS") }
            assertThatThrownBy { b.store.createNamespace(cat, "Events") }
                .isInstanceOf(HoglakeException.AlreadyExists::class.java)
            assertThat(b.store.listNamespaces(cat).map { it.name }).containsExactlyInAnyOrder("Events", "events")
        }

    // ---- tables -----------------------------------------------------------

    @Test fun `PostgreSQL tables differing only by case are distinct, and rename is exact`() = tables(false)

    @Test fun `hoglite tables differing only by case are distinct, and rename is exact`() = tables(true)

    private fun tables(lite: Boolean) =
        fixture(lite) { b, cat ->
            b.store.createNamespace(cat, "ns")
            val cols = listOf(ColumnDef("id", ColType.LONG))
            val upper = b.store.createTable(cat, "ns", "Clicks", cols)
            val lower = b.store.createTable(cat, "ns", "clicks", cols)
            assertThat(upper.tableUuid).isNotEqualTo(lower.tableUuid)
            assertThat(b.store.getTable(cat, "ns", "Clicks").tableUuid).isEqualTo(upper.tableUuid)
            notFound { b.store.getTable(cat, "ns", "CLICKS") }
            // The namespace segment of a lookup is exact too.
            notFound { b.store.getTable(cat, "NS", "clicks") }
            assertThatThrownBy { b.store.createTable(cat, "ns", "Clicks", cols) }
                .isInstanceOf(HoglakeException.AlreadyExists::class.java)

            // Renaming onto a name that is taken exactly is refused...
            assertThatThrownBy { b.alters.alterTable(cat, "ns", "clicks", listOf(AlterOp.RenameTable("Clicks"))) }
                .isInstanceOf(HoglakeException.AlreadyExists::class.java)
            // ...and a case-only rename is a rename to a new name.
            val renamed = b.alters.alterTable(cat, "ns", "Clicks", listOf(AlterOp.RenameTable("CLICKS")))
            assertThat(renamed.name).isEqualTo("CLICKS")
            assertThat(b.store.getTable(cat, "ns", "CLICKS").tableUuid).isEqualTo(upper.tableUuid)
            notFound { b.store.getTable(cat, "ns", "Clicks") }
            assertThat(b.store.listTables(cat, "ns").map { it.name }).containsExactlyInAnyOrder("CLICKS", "clicks")
        }

    // ---- views ------------------------------------------------------------

    @Test fun `PostgreSQL views differing only by case are distinct`() = views(false)

    @Test fun `hoglite views differing only by case are distinct`() = views(true)

    private fun views(lite: Boolean) =
        fixture(lite) { b, cat ->
            b.store.createNamespace(cat, "ns")
            val upper = b.views.create(cat, "ns", "Daily", "SELECT 1")
            val lower = b.views.create(cat, "ns", "daily", "SELECT 2")
            assertThat(upper.viewUuid).isNotEqualTo(lower.viewUuid)
            assertThat(b.views.get(cat, "ns", "Daily").sql).isEqualTo("SELECT 1")
            notFound { b.views.get(cat, "ns", "DAILY") }
            assertThatThrownBy { b.views.create(cat, "ns", "Daily", "SELECT 3") }
                .isInstanceOf(HoglakeException.AlreadyExists::class.java)
        }

    // ---- columns ----------------------------------------------------------

    /**
     * Pins CURRENT behaviour; intent unknown. Columns differing only by case
     * are accepted, unlike variant shredded fields (VariantShredding refuses
     * those because readers look fields up by lowercase name). Both stores
     * agree; whether hoglake should refuse them is an open question.
     */
    @Test fun `PostgreSQL columns differing only by case are distinct and addressed exactly`() = columns(false)

    @Test fun `hoglite columns differing only by case are distinct and addressed exactly`() = columns(true)

    private fun columns(lite: Boolean) =
        fixture(lite) { b, cat ->
            b.store.createNamespace(cat, "ns")
            val t =
                b.store.createTable(
                    cat,
                    "ns",
                    "t",
                    listOf(ColumnDef("Id", ColType.LONG), ColumnDef("id", ColType.STRING)),
                )
            assertThat(t.columns.map { it.def.name }).containsExactly("Id", "id")
            assertThat(t.columns.map { it.fieldId }.toSet()).hasSize(2)

            assertThatThrownBy { b.alters.alterTable(cat, "ns", "t", listOf(AlterOp.RenameColumn("ID", "x"))) }
                .isInstanceOf(HoglakeException.Validation::class.java)
                .hasMessageContaining("does not exist")
            assertThatThrownBy { b.alters.alterTable(cat, "ns", "t", listOf(AlterOp.RenameColumn("Id", "id"))) }
                .isInstanceOf(HoglakeException.Validation::class.java)
                .hasMessageContaining("already exists")
            val renamed = b.alters.alterTable(cat, "ns", "t", listOf(AlterOp.RenameColumn("Id", "ID")))
            assertThat(renamed.columns.map { it.def.name }).containsExactly("ID", "id")
            assertThat(renamed.columns.first().fieldId).isEqualTo(t.columns.first().fieldId)
        }

    // ---- consumer ids -----------------------------------------------------

    @Test fun `PostgreSQL consumer ids differing only by case are distinct`() = consumers(false)

    @Test fun `hoglite consumer ids differing only by case are distinct`() = consumers(true)

    private fun consumers(lite: Boolean) =
        fixture(lite) { b, cat ->
            b.store.createNamespace(cat, "ns")
            val first = b.store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
            b.store.createTable(cat, "ns", "u", listOf(ColumnDef("id", ColType.LONG)))
            val later = b.store.getCatalog(cat).headSnapshotId
            b.store.commitOffset(cat, "Reader", first.tableUuid, first.readSnapshotId)
            b.store.commitOffset(cat, "reader", first.tableUuid, later)
            assertThat(b.store.getOffset(cat, "Reader", first.tableUuid).committedSnapshot)
                .isEqualTo(first.readSnapshotId)
            assertThat(b.store.getOffset(cat, "reader", first.tableUuid).committedSnapshot).isEqualTo(later)
            assertThat(b.store.listOffsets(cat, "READER")).isEmpty()
            assertThat(b.store.listConsumers(cat).map { it.consumerId }.toSet()).containsExactlyInAnyOrder(
                "Reader",
                "reader",
            )
        }
}
