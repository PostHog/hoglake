package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
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

/** Large and unusual collections through the adapter's real statements, on the live engine. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteCommitCollectionsIntegrationTest {
    private lateinit var engine: HogliteConformanceEngine
    private lateinit var proxy: HogliteInterposer
    private val json = jacksonObjectMapper()

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

    private fun fixture(body: (HogliteCatalogStore, String) -> Unit) {
        val store = proxy.store
        val cat = HogliteConformanceEngine.uniqueName("bind")
        store.createCatalog(cat, "s3://hoglake-it/$cat", null, null)
        store.createNamespace(cat, "ns", null, null)
        store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)), null, null)
        proxy.requests.clear()
        body(store, cat)
    }

    @Test
    fun `JSON collection arguments keep their values and types`() =
        fixture { store, cat ->
            store.catalogOperation(cat) {
                val values = listOf<Any?>("a'\"\\\n雪?;--", null, "null", 9_007_199_254_740_993L, 7L, 7L)
                val rows =
                    store.queryCatalog(
                        cat,
                        "SELECT value, type FROM json_each(?) ORDER BY key",
                        json.writeValueAsString(values),
                    ).rows
                assertThat(
                    rows.map { if (it[0] is Number) it[0].asLong() else it[0] },
                ).containsExactlyElementsOf(values)
                assertThat(rows.map { it[1] }).containsExactly("text", "null", "text", "integer", "integer", "integer")
                assertThat(store.queryCatalog(cat, "SELECT value FROM json_each(?)", "[]").rows).isEmpty()
            }
        }

    @Test
    fun `a commit of 32767 files reaches file validation instead of a bind variable overflow`() =
        fixture { store, cat ->
            val head = store.getCatalog(cat).headSnapshotId
            val files =
                (1..32_767).map { n ->
                    FileRegistration("s3://hoglake-it/$cat/$n", if (n == 32_767) -1 else 1, 100, 50)
                }
            assertThatThrownBy {
                store.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", files))), null)
            }.isInstanceOf(HoglakeException.Validation::class.java).hasMessageContaining("record_count")
            assertThat(proxy.requests.filter { "INSERT INTO files" in it }).isEmpty()
            assertThat(store.getCatalog(cat).headSnapshotId).isEqualTo(head)
        }
}
