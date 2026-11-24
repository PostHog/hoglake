package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.HoglakeException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

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
