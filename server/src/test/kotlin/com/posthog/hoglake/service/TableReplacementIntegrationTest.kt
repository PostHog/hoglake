package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TableReplacementIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val creations = TableCreationService(db.jdbi, catalogs, commits)
    private val columns = listOf(ColumnDef("id", ColType.LONG))

    @AfterAll
    fun close() = db.close()

    private fun fixture(): String {
        val cat = "replacement-${UUID.randomUUID()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        return cat
    }

    private fun prepare(
        cat: String,
        uuid: UUID?,
    ) = creations.prepare(
        cat,
        UUID.randomUUID(),
        TableCreationDefinition("ns", "t", columns, ReplacementTarget(uuid, catalogs.getCatalog(cat).headSnapshotId)),
    )

    private fun append(cat: String) =
        CommitRequest(
            readSnapshot = catalogs.getCatalog(cat).headSnapshotId,
            idempotencyKey = UUID.randomUUID(),
            appends =
                listOf(
                    TableAppend(
                        "ns",
                        "t",
                        listOf(FileRegistration("s3://bucket/$cat/${UUID.randomUUID()}.parquet", 5, 100, 20)),
                        expectedTableUuid = catalogs.getTable(cat, "ns", "t").tableUuid,
                    ),
                ),
        )

    @Test
    fun `replacement publishes one snapshot preserves history and fences stale writers and feeds`() {
        val cat = fixture()
        val old = catalogs.createTable(cat, "ns", "t", columns)
        val request = append(cat)
        val oldReceipt = commits.commit(cat, request)
        val stale = append(cat)
        val before = catalogs.getCatalog(cat).headSnapshotId
        val prepared = prepare(cat, old.tableUuid)
        assertThat(catalogs.getTable(cat, "ns", "t").tableUuid).isEqualTo(old.tableUuid)
        assertThat(catalogs.getTable(cat, "ns", "t").recordCount).isEqualTo(5)
        val files = listOf(FileRegistration(prepared.writePath + "new.parquet", 2, 100, 20))
        val receipt = creations.publish(cat, prepared.operationId, files)
        assertThat(receipt.snapshotId).isEqualTo(before + 1)
        val current = catalogs.getTable(cat, "ns", "t")
        assertThat(current.tableUuid).isEqualTo(prepared.tableUuid).isNotEqualTo(old.tableUuid)
        assertThat(current.recordCount).isEqualTo(2)
        assertThat(catalogs.getTable(cat, "ns", "t", before).tableUuid).isEqualTo(old.tableUuid)
        assertThat(catalogs.listFiles(cat, "ns", "t", before)).hasSize(1)
        assertThat(creations.publish(cat, prepared.operationId, files)).isEqualTo(receipt)
        assertThat(commits.commit(cat, request)).isEqualTo(oldReceipt)
        assertThatThrownBy { commits.commit(cat, stale) }.isInstanceOf(HoglakeException.CommitConflict::class.java)
        for (from in listOf(0L, before)) {
            assertThatThrownBy { catalogs.changes(cat, "ns", "t", from) }
                .isInstanceOf(HoglakeException.ReconciliationRequired::class.java)
        }
        assertThat(catalogs.changes(cat, "ns", "t", before + 1).files).isEmpty()
        assertThat(catalogs.getTable(cat, "ns", "t").recordCount).isEqualTo(2)
    }

    @Test
    fun `replacement feed barrier survives expiry of old version at the floor`() {
        val cat = fixture()
        val old = catalogs.createTable(cat, "ns", "t", columns)
        val prepared = prepare(cat, old.tableUuid)
        val snapshot = creations.publish(cat, prepared.operationId, emptyList()).snapshotId!!
        db.jdbi.useHandle<Exception> { h ->
            h.createUpdate(
                "UPDATE hog_catalog SET snapshot_retention_seconds = 3600, consumer_floor = false WHERE name = :cat",
            )
                .bind("cat", cat).execute()
            h.createUpdate("UPDATE hog_snapshot SET snapshot_time = now() - interval '2 days' WHERE catalog_id = :id")
                .bind("id", catalogs.getCatalog(cat).catalogId).execute()
        }
        ExpiryService(db.jdbi).runOnce(cat, batchSize = 1000)
        assertThat(catalogs.getCatalog(cat).earliestSnapshotId).isEqualTo(snapshot)
        assertThatThrownBy { catalogs.changes(cat, "ns", "t", snapshot - 1, snapshot) }
            .isInstanceOf(HoglakeException.ReconciliationRequired::class.java)
        assertThat(catalogs.changes(cat, "ns", "t", snapshot, snapshot).files).isEmpty()
    }

    @Test
    fun `absent target cannot overwrite a table created before publication`() {
        val cat = fixture()
        val prepared = prepare(cat, null)
        val winner = catalogs.createTable(cat, "ns", "t", columns)
        assertThat(creations.publish(cat, prepared.operationId, emptyList()).reason).isEqualTo("target_changed")
        assertThat(catalogs.getTable(cat, "ns", "t")).isEqualTo(winner)
    }

    @Test
    fun `absent target and empty replacement use the same durable receipt`() {
        val cat = fixture()
        val creation = prepare(cat, null)
        assertThat(creations.publish(cat, creation.operationId, emptyList()).state).isEqualTo("committed")
        val replacement = prepare(cat, creation.tableUuid)
        val receipt = creations.publish(cat, replacement.operationId, emptyList())
        assertThat(receipt.state).isEqualTo("committed")
        assertThat(catalogs.getTable(cat, "ns", "t").tableUuid).isEqualTo(replacement.tableUuid)
        assertThat(catalogs.getTable(cat, "ns", "t").recordCount).isZero()
    }

    @Test
    fun `abort and invalid publication leave the original intact`() {
        val cat = fixture()
        val original = catalogs.createTable(cat, "ns", "t", columns)
        val prepared = prepare(cat, original.tableUuid)
        assertThatThrownBy {
            creations.publish(
                cat,
                prepared.operationId,
                listOf(FileRegistration(prepared.writePath + "bad", -1, 100, 20)),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
        assertThat(catalogs.getTable(cat, "ns", "t")).isEqualTo(original)
        assertThat(creations.abort(cat, prepared.operationId).state).isEqualTo("aborted")
        assertThat(creations.publish(cat, prepared.operationId, emptyList()).state).isEqualTo("aborted")
        assertThat(catalogs.getTable(cat, "ns", "t")).isEqualTo(original)
    }

    @Test
    fun `target mutations after preparation reject replacement without overwriting them`() {
        for (mutation in listOf("insert", "rename", "drop", "truncate", "replace", "reuse")) {
            val cat = fixture()
            val original = catalogs.createTable(cat, "ns", "t", columns)
            val prepared = prepare(cat, original.tableUuid)
            when (mutation) {
                "insert" -> commits.commit(cat, append(cat))
                "rename" -> AlterService(db.jdbi).alterTable(cat, "ns", "t", listOf(AlterOp.RenameTable("renamed")))
                "drop" -> catalogs.dropTable(cat, "ns", "t")
                "truncate" -> catalogs.truncateTable(cat, "ns", "t", original.tableUuid)
                "replace" -> creations.publish(cat, prepare(cat, original.tableUuid).operationId, emptyList())
                "reuse" -> {
                    catalogs.dropTable(cat, "ns", "t")
                    catalogs.createTable(cat, "ns", "t", columns)
                }
            }
            val head = catalogs.getCatalog(cat).headSnapshotId
            val rejected = creations.publish(cat, prepared.operationId, emptyList())
            assertThat(rejected.state).describedAs(mutation).isEqualTo("rejected")
            assertThat(rejected.reason).isEqualTo("target_changed")
            assertThat(catalogs.getCatalog(cat).headSnapshotId).isEqualTo(head)
            assertThat(creations.publish(cat, prepared.operationId, emptyList())).isEqualTo(rejected)
        }
    }
}
