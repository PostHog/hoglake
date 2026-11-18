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

/** Engine limits and adapter paging use real responses, without raised engine limits. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteReadLimitsIntegrationTest {
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

    private fun catalog(): String {
        val cat = HogliteConformanceEngine.uniqueName("limits")
        store.createCatalog(cat, "s3://hoglake-it/$cat")
        return cat
    }

    @Test
    fun `file listings and changefeeds return more than the engine row cap without gaps`() {
        val cat = catalog()
        store.createNamespace(cat, "ns")
        val table = store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val count = 100_001
        val to = table.readSnapshotId
        store.catalogOperation(cat) {
            for (first in 1..count step 5000) {
                val last = minOf(first + 4999, count)
                store.runTx(
                    cat,
                    HogliteCatalogStore.freshTxid("paging-fixture"),
                    null,
                    HogliteTx().apply {
                        add(
                            """WITH RECURSIVE n(i) AS (VALUES(?) UNION ALL SELECT i+1 FROM n WHERE i < ?)
                            INSERT INTO files(id,table_id,path,record_count,file_size_bytes,row_id_start,
                                              stats_state,begin_snapshot)
                            SELECT i,?,'s3://hoglake-it/$cat/'||i,2,100,i/2,'pending',? FROM n""",
                            first,
                            last,
                            table.tableId,
                            to,
                        )
                        add(
                            """WITH RECURSIVE n(i) AS (VALUES(?) UNION ALL SELECT i+1 FROM n WHERE i < ?)
                            INSERT INTO dvs(delete_file_id,data_file_id,table_id,path,delete_count,
                                            file_size_bytes,begin_snapshot,epoch)
                            SELECT i,i,?,'s3://hoglake-it/$cat/dv-'||i,1,10,?,1 FROM n""",
                            first,
                            last,
                            table.tableId,
                            to,
                        )
                    },
                )
            }
        }
        val expected = (1..count).map(Int::toLong)
        val listed = store.listFiles(cat, "ns", "t")
        assertThat(listed.map { it.dataFileId }).containsExactlyElementsOf(expected)
        val plan = store.changes(cat, "ns", "t", to - 1, to)
        assertThat(plan.files.map { it.dataFileId }).containsExactlyElementsOf(expected)
        assertThat(plan.deleteFiles.map { it.deleteFileId }).containsExactlyElementsOf(expected)
        assertThat(plan.fromSnapshot).isEqualTo(to - 1)
        assertThat(plan.toSnapshot).isEqualTo(to)
    }

    @Test
    fun `invalid query and read transaction SQL have final validation errors`() {
        val cat = catalog()
        store.catalogOperation(cat) {
            assertThatThrownBy { store.queryCatalog(cat, "SELECT missing_column FROM meta") }
                .isExactlyInstanceOf(HoglakeException.Validation::class.java)
                .hasMessageContaining("missing_column")
            assertThatThrownBy {
                store.readCatalog(cat, listOf(HogliteStatement("SELECT missing_column FROM meta")))
            }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
                .hasMessageContaining("missing_column")
        }
    }

    @Test
    fun `a query above the engine row cap has a final result limit error`() {
        val cat = catalog()
        store.catalogOperation(cat) {
            assertThatThrownBy {
                store.queryCatalog(
                    cat,
                    "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<100001) SELECT i FROM n",
                )
            }.isExactlyInstanceOf(HoglakeException.BackendResultTooLarge::class.java)
        }
    }

    @Test
    fun `a write request above the engine body cap has a final validation error`() {
        val cat = catalog()
        val before = store.getCatalog(cat).headSnapshotId
        store.catalogOperation(cat) {
            assertThatThrownBy {
                store.runTx(
                    cat,
                    HogliteCatalogStore.freshTxid("body-cap"),
                    null,
                    HogliteTx().apply { add("SELECT ?", "x".repeat(64 * 1024 * 1024)) },
                )
            }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
        }
        assertThat(store.getCatalog(cat).headSnapshotId).isEqualTo(before)
    }

    @Test
    fun `an engine query deadline has a final timeout error`() {
        val cat = catalog()
        // The engine deadline is five seconds. Let it answer before the HTTP timeout.
        HogliteClient(engine.url(), readTimeoutMs = 15_000, retryBudgetMs = 20_000).use { client ->
            val selected = HogliteCatalogStore(client, HogliteConformanceEngine.TENANTS)
            selected.catalogOperation(cat) {
                assertThatThrownBy {
                    selected.queryCatalog(
                        cat,
                        "WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<1000000000) " +
                            "SELECT sum(i) FROM n",
                    )
                }.isExactlyInstanceOf(HoglakeException.BackendQueryTimeout::class.java)
            }
        }
    }
}
