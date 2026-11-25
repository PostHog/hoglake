package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceContainment
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

/** Catalog create and drop on the live engine: the tenant PUT, the bootstrap guard, the fence and the delete. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteCatalogLifecycleIntegrationTest {
    private lateinit var engine: HogliteConformanceEngine

    private val store get() = engine.store

    @BeforeAll
    fun start() {
        engine = HogliteConformanceEngine()
    }

    @AfterAll
    fun stop() {
        if (::engine.isInitialized) engine.close()
    }

    @Test
    fun `a created catalog is served, a second create is AlreadyExists, and a drop removes it`() {
        // A digit-leading name, as PostgreSQL's catalog name rule (V13) allows.
        val c = HogliteConformanceEngine.uniqueName("7cat")
        val created = store.createCatalog(c, "s3://hoglake-it/$c", "test", "create")
        assertThat(created.name).isEqualTo(c)
        assertThat(created.headSnapshotId).isZero()
        assertThat(store.getCatalog(c).dataPath).isEqualTo("s3://hoglake-it/$c")

        assertThatThrownBy { store.createCatalog(c, "s3://hoglake-it/$c", null, null) }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)

        store.dropCatalog(c, "test", "drop")
        assertThatThrownBy { store.getCatalog(c) }.isInstanceOf(HoglakeException.NotFound::class.java)
        assertThatThrownBy { store.dropCatalog(c, null, null) }.isInstanceOf(HoglakeException.NotFound::class.java)

        // The name is free again after the drop.
        assertThat(store.createCatalog(c, "s3://hoglake-it/$c", null, null).headSnapshotId).isZero()
    }

    @Test
    fun `a create whose data_path overlaps another backend's catalog is refused and makes no tenant`() {
        val c = HogliteConformanceEngine.uniqueName("ovl")
        val peer = HogliteConformanceEngine.uniqueName("pg")
        // The selector's view of the other backend, as it hands it to the store.
        val peers = listOf(CatalogInfo(1, peer, "s3://hoglake-it/$peer", 0, 0))
        val checked = HogliteCatalogStore(engine.client, HogliteConformanceEngine.TENANTS, peerCatalogs = { peers })

        // Equal, trailing-slash, nested-under and parent-of (bucket root).
        val theirs = "s3://hoglake-it/$peer"
        for (bad in listOf(theirs, "$theirs/", "$theirs/x", "s3://hoglake-it")) {
            assertThatThrownBy { checked.createCatalog(c, bad, null, null) }
                .`as`(bad)
                .isInstanceOf(HoglakeException.Validation::class.java)
                .hasMessageContaining("overlaps catalog '$peer'")
        }
        // Refused before the tenant PUT: the name resolves to nothing on the engine.
        assertThatThrownBy { checked.getCatalog(c) }.isInstanceOf(HoglakeException.NotFound::class.java)

        // A disjoint data_path creates as before.
        assertThat(checked.createCatalog(c, "s3://hoglake-it/$c", null, null).dataPath).isEqualTo("s3://hoglake-it/$c")
    }

    @Test
    fun `a catalog with a live namespace refuses the drop and stays writable`() {
        val c = HogliteConformanceEngine.uniqueName("busy")
        store.createCatalog(c, "s3://hoglake-it/$c", null, null)
        store.createNamespace(c, "ns", null, null)
        assertThatThrownBy { store.dropCatalog(c, null, null) }
            .isInstanceOf(HoglakeException.CatalogNotEmpty::class.java)
        // The refused drop left no fence behind.
        store.createNamespace(c, "ns2", null, null)
        assertThat(store.listNamespaces(c, null, null).map { it.name }).contains("ns", "ns2")
    }

    /**
     * The resolver is the caller's mapping (a token's `cat` and `rtg.t`),
     * and a wrong one must not reach the catalog the tenant holds under
     * the requested name (#670): reads, writes and the drop are the
     * missing-catalog 404, naming only the requested catalog, and the
     * real catalog is left as it was. Both halves of the check run here:
     * the meta-row comparison on the read paths and the engine-evaluated
     * name guard of every mutating transaction. The writes whose
     * pre-flight reads would otherwise answer first (a stale expected id
     * or table UUID, a contained namespace) are driven with those
     * arguments, so the 404 is asserted to win over the 409 and 403.
     */
    @Test
    fun `a catalog whose tenant holds another catalog is NotFound on every path and is left untouched`() {
        val c = HogliteConformanceEngine.uniqueName("globex")
        store.createCatalog(c, "s3://hoglake-it/$c", null, null)
        store.createNamespace(c, "ns", null, null)
        store.createNamespace(c, "team_42", null, null)
        store.setNamespaceContainment(c, "team_42", NamespaceContainment("team_id", 42), null)
        val table = store.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.LONG)), null, null)
        store.views.create(c, "ns", "v", "SELECT 1", "duckdb", null, null)
        val head = store.getCatalog(c).headSnapshotId
        val wrong = HogliteConformanceEngine.uniqueName("acme")
        // acme's name resolves to globex's tenant.
        val misrouted = HogliteCatalogStore(engine.client, { HogliteConformanceEngine.TENANTS.tenantFor(c) })

        val staleUuid = UUID.randomUUID()
        val refusals =
            listOf<() -> Any?>(
                { misrouted.getCatalog(wrong) },
                { misrouted.listNamespaces(wrong, null, null) },
                { misrouted.getNamespace(wrong, "ns") },
                { misrouted.listTables(wrong, "ns", null, null) },
                { misrouted.getTable(wrong, "ns", "t") },
                { misrouted.views.get(wrong, "ns", "v") },
                { misrouted.storageUsage(wrong) },
                { misrouted.createNamespace(wrong, "ns2", null, null) },
                { misrouted.dropNamespace(wrong, "ns", null, "test", "drop", true) },
                // Would be CommitConflict on globex's namespace id.
                { misrouted.dropNamespace(wrong, "ns", 999, "test", "drop", true) },
                // Would be Forbidden on globex's containment mark.
                { misrouted.dropNamespace(wrong, "team_42", null, "test", "drop", false) },
                { misrouted.createTable(wrong, "ns", "t2", listOf(ColumnDef("id", ColType.LONG)), null, null) },
                // Would be TableRecreated, carrying globex's live table UUID.
                { misrouted.dropTable(wrong, "ns", "t", staleUuid, "test", "drop") },
                { misrouted.truncateTable(wrong, "ns", "t", staleUuid, "test", "truncate") },
                { misrouted.views.create(wrong, "ns", "v2", "SELECT 1", "duckdb", null, null) },
                { misrouted.views.drop(wrong, "ns", "v", "test", "drop") },
                { misrouted.dropCatalog(wrong, "test", "drop") },
            )
        for ((i, refusal) in refusals.withIndex()) {
            assertThatThrownBy { refusal() }
                .describedAs("refusal $i")
                .isExactlyInstanceOf(HoglakeException.NotFound::class.java)
                .hasMessageContaining("'$wrong'")
                .satisfies({ assertThat(it.message).doesNotContain(c, table.tableUuid.toString()) })
        }
        assertThat(misrouted.liveContainment(wrong, "team_42")).isNull()
        assertThat(misrouted.listCatalogs(setOf(wrong))).isEmpty()

        // globex is intact: still served under its own name, its objects live, nothing fenced or written.
        assertThat(store.getCatalog(c).headSnapshotId).isEqualTo(head)
        assertThat(store.listNamespaces(c, null, null).map { it.name }).containsExactly("ns", "team_42")
        assertThat(store.getTable(c, "ns", "t").tableUuid).isEqualTo(table.tableUuid)
        assertThat(store.views.list(c, "ns").map { it.name }).containsExactly("v")
        store.createNamespace(c, "ns2", null, null)
    }

    @Test
    fun `listCatalogs reads many tenants and skips names without a catalog`() {
        val names = (1..3).map { HogliteConformanceEngine.uniqueName("list") }
        names.forEach { store.createCatalog(it, "s3://hoglake-it/$it", null, null) }
        val missing = HogliteConformanceEngine.uniqueName("absent")
        assertThat(
            store.listCatalogs((names + missing).toSet()).map { it.name },
        ).containsExactlyInAnyOrderElementsOf(names)
    }
}
