package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.stats.IcebergSingleValue
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.Base64

/** A width-changing promote writes bound patches, so its `/tx` fits hoglited's 64 MiB body cap (#684). */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HoglitePromotionTxBodyIntegrationTest {
    private var started = false
    private val engine by lazy { HogliteConformanceEngine().also { started = true } }
    private val store get() = engine.store

    @AfterAll
    fun close() {
        if (started) engine.close()
    }

    private fun catalog(): String =
        HogliteConformanceEngine.uniqueName("promote_body").also {
            store.createCatalog(it, "s3://hoglake-it/$it")
            store.createNamespace(it, "ns")
        }

    @Test
    fun `a promote over more stats than one tx body can carry patches only the bounds`() {
        val cat = catalog()
        val table = store.createTable(cat, "ns", "t", listOf(ColumnDef("a", ColType.INT)))
        val before = store.getCatalog(cat).headSnapshotId
        val count = 300
        val bound = IcebergSingleValue.encode(ColType.INT, 7)
        val wide = ByteArray(48_000) { it.toByte() }
        // ~128 KB of stats per file, ~38 MB in all: a whole-stats rewrite sends
        // it twice, so its /tx is over hoglited's 64 MiB cap on every attempt.
        val stats =
            store.json.writeValueAsString(
                listOf(
                    ColumnStats(1, 1, 0, null, null, bound, bound),
                    ColumnStats(2, 1, 3, null, null, wide, wide),
                ),
            )
        store.catalogOperation(cat) {
            for (first in 1..count step 100) {
                store.runTx(
                    cat,
                    HogliteCatalogStore.freshTxid("promote-body"),
                    null,
                    HogliteTx().apply {
                        add(
                            "WITH RECURSIVE n(i) AS (VALUES(?) UNION ALL SELECT i+1 FROM n WHERE i < ?) " +
                                "INSERT INTO files(id,table_id,path,record_count,file_size_bytes,row_id_start," +
                                "stats_state,stats_json,begin_snapshot) " +
                                "SELECT i,?,'s3://hoglake-it/$cat/'||i,1,100,i,'provided',?,? FROM n",
                            first,
                            minOf(first + 99, count),
                            table.tableId,
                            stats,
                            table.readSnapshotId,
                        )
                    },
                )
            }
            store.runTx(
                cat,
                HogliteCatalogStore.freshTxid("promote-body-meta"),
                null,
                HogliteTx().apply {
                    add("UPDATE meta SET next_file_id = ? WHERE id = 1", count + 1)
                    add(
                        "UPDATE table_totals SET record_count = ?,file_count = ?,file_size_bytes = ? " +
                            "WHERE table_id = ?",
                        count,
                        count,
                        count * 100L,
                        table.tableId,
                    )
                },
            )
        }

        val result = store.alters.alterTable(cat, "ns", "t", listOf(AlterOp.PromoteColumn("a", ColType.LONG)))

        assertThat(result.columns.single().def.type).isEqualTo(ColType.LONG)
        assertThat(result.readSnapshotId).isEqualTo(before + 1)
        store.catalogOperation(cat) {
            val row =
                store.queryCatalog(
                    cat,
                    "SELECT COUNT(*), " +
                        "SUM(json_extract(stats_json,'$[0].lowerBound') = ? " +
                        "AND json_extract(stats_json,'$[0].upperBound') = ?), " +
                        "SUM(json_extract(stats_json,'$[1].lowerBound') = ? " +
                        "AND json_extract(stats_json,'$[1].nullCount') = 3) FROM files",
                    stored(IcebergSingleValue.encode(ColType.LONG, 7L)),
                    stored(IcebergSingleValue.encode(ColType.LONG, 7L)),
                    stored(wide),
                ).firstRow()!!
            assertThat(row.map { it.asLong() }).containsExactly(count.toLong(), count.toLong(), count.toLong())
        }
    }

    @Test
    fun `two promotions in one alter patch both entries of every file`() {
        val cat = catalog()
        store.createTable(cat, "ns", "t", listOf(ColumnDef("a", ColType.INT), ColumnDef("b", ColType.FLOAT)))
        val i = IcebergSingleValue.encode(ColType.INT, -5)
        val f = IcebergSingleValue.encode(ColType.FLOAT, 1.5f)
        store.commit(
            cat,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            (1..3).map { id ->
                                FileRegistration(
                                    "s3://hoglake-it/$cat/t/$id",
                                    1,
                                    100,
                                    20,
                                    listOf(
                                        ColumnStats(1, 1, 0, null, null, i, i),
                                        ColumnStats(2, 1, 0, 0, null, f, null),
                                    ),
                                )
                            },
                        ),
                    ),
            ),
        )

        store.alters.alterTable(
            cat,
            "ns",
            "t",
            listOf(AlterOp.PromoteColumn("a", ColType.LONG), AlterOp.PromoteColumn("b", ColType.DOUBLE)),
        )

        val files = store.listFiles(cat, "ns", "t")
        assertThat(files).hasSize(3)
        files.forEach { file ->
            val columns = store.fileStats(cat, "ns", "t", file.dataFileId).columns.associateBy { it.fieldId }
            assertThat(IcebergSingleValue.decode(ColType.LONG, columns.getValue(1).stats.lowerBound!!)).isEqualTo(-5L)
            assertThat(IcebergSingleValue.decode(ColType.LONG, columns.getValue(1).stats.upperBound!!)).isEqualTo(-5L)
            assertThat(IcebergSingleValue.decode(ColType.DOUBLE, columns.getValue(2).stats.lowerBound!!))
                .isEqualTo(1.5)
            assertThat(columns.getValue(2).stats.upperBound).isNull()
            assertThat(columns.getValue(2).stats.nanCount).isEqualTo(0L)
        }
    }

    /** A bound as `stats_json` stores it: Jackson's base64 of the bytes. */
    private fun stored(bytes: ByteArray): String = Base64.getEncoder().encodeToString(bytes)
}
