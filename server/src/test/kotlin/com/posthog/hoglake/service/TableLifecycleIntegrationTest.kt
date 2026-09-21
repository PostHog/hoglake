package com.posthog.hoglake.service

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TableLifecycleIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val counter = AtomicInteger()

    @AfterAll
    fun close() = db.close()

    private fun fixture(): String {
        val cat = "lifecycle-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        return cat
    }

    private fun append(
        cat: String,
        path: String = "data.parquet",
        snapshot: Long? = null,
    ): CommitRequest =
        CommitRequest(
            readSnapshot = snapshot,
            idempotencyKey = UUID.randomUUID(),
            appends =
                listOf(
                    TableAppend(
                        "ns",
                        "t",
                        listOf(FileRegistration("s3://bucket/$cat/$path", 10, 100)),
                        expectedTableUuid = catalogs.getTable(cat, "ns", "t").tableUuid,
                    ),
                ),
        )

    @Test
    fun `truncate preserves identity metadata history and row allocation and ends DVs`() {
        val cat = fixture()
        val request = append(cat)
        val receipt = commits.commit(cat, request)
        val file = catalogs.listFiles(cat, "ns", "t").single()
        commits.commit(
            cat,
            CommitRequest(
                readSnapshot = receipt.snapshotId,
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            "t",
                            listOf(DeleteFileRegistration(file.dataFileId, "s3://bucket/$cat/dv", 2, 16)),
                        ),
                    ),
            ),
        )
        alter.alterTable(
            cat,
            "ns",
            "t",
            listOf(
                AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY))),
                AlterOp.SetSortOrder(listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_LAST))),
            ),
        )
        val before = catalogs.getTable(cat, "ns", "t")
        val beforeFiles = catalogs.listFiles(cat, "ns", "t")
        val beforeSnapshot = catalogs.getCatalog(cat).headSnapshotId
        val result = catalogs.truncateTable(cat, "ns", "t", before.tableUuid)
        assertThat(
            catalogs.getTable(cat, "ns", "t"),
        ).isEqualTo(before.copy(recordCount = 0, fileCount = 0, fileSizeBytes = 0))
        assertThat(catalogs.getTable(cat, "ns", "t", beforeSnapshot)).isEqualTo(before)
        assertThat(catalogs.listFiles(cat, "ns", "t")).isEmpty()
        assertThat(catalogs.listFiles(cat, "ns", "t", beforeSnapshot)).containsExactlyElementsOf(beforeFiles)
        db.jdbi.withHandleUnchecked { h ->
            assertThat(
                h.createQuery("SELECT end_snapshot FROM hog_delete_file WHERE path = :path")
                    .bind("path", "s3://bucket/$cat/dv").mapTo(Long::class.java).one(),
            ).isEqualTo(result.snapshotId)
            assertThat(
                h.createQuery("SELECT count(*) FROM hog_file_removal WHERE catalog_id = :id")
                    .bind("id", catalogs.getCatalog(cat).catalogId).mapTo(Long::class.java).one(),
            ).isZero()
        }
        assertThat(commits.commit(cat, request)).isEqualTo(receipt)
        assertThat(catalogs.listFiles(cat, "ns", "t")).isEmpty()
        // Reset specs only to make this append fixture valid; allocation survives truncate.
        alter.alterTable(
            cat,
            "ns",
            "t",
            listOf(AlterOp.SetPartitionSpec(emptyList()), AlterOp.SetSortOrder(emptyList())),
        )
        commits.commit(cat, append(cat, "next.parquet"))
        assertThat(catalogs.listFiles(cat, "ns", "t").single().rowIdStart).isEqualTo(10)
    }

    @Test
    fun `rename and drop preserve receipts and history and stale UUID cannot affect reused names`() {
        val cat = fixture()
        val original = catalogs.getTable(cat, "ns", "t")
        val request = append(cat)
        val receipt = commits.commit(cat, request)
        val renamed = alter.alterTable(cat, "ns", "t", listOf(AlterOp.RenameTable("renamed")), original.tableUuid)
        assertThat(renamed.tableUuid).isEqualTo(original.tableUuid)
        assertThat(renamed.columns).isEqualTo(original.columns)
        assertThat(catalogs.getTable(cat, "ns", "t", receipt.snapshotId).recordCount).isEqualTo(10)
        assertThat(commits.commit(cat, request)).isEqualTo(receipt)
        catalogs.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val replacement = catalogs.getTable(cat, "ns", "t")
        assertThatThrownBy {
            catalogs.dropTable(cat, "ns", "t", original.tableUuid)
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThatThrownBy {
            catalogs.truncateTable(cat, "ns", "t", original.tableUuid)
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThatThrownBy {
            alter.alterTable(cat, "ns", "t", listOf(AlterOp.RenameTable("wrong")), original.tableUuid)
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)
        assertThat(catalogs.getTable(cat, "ns", "t")).isEqualTo(replacement)
        catalogs.dropTable(cat, "ns", "renamed", original.tableUuid)
        assertThat(commits.commit(cat, request)).isEqualTo(receipt)
        assertThat(catalogs.getTable(cat, "ns", "t")).isEqualTo(replacement)
        assertThat(catalogs.getTable(cat, "ns", "t", receipt.snapshotId).recordCount).isEqualTo(10)
    }

    @Test
    fun `concurrent insert either precedes truncate or conflicts without publishing`() {
        repeat(5) {
            val cat = fixture()
            val uuid = catalogs.getTable(cat, "ns", "t").tableUuid
            val request = append(cat, snapshot = catalogs.getCatalog(cat).headSnapshotId)
            val start = CountDownLatch(1)
            val pool = Executors.newFixedThreadPool(2)
            try {
                val insert =
                    pool.submit<Boolean> {
                        start.await()
                        try {
                            commits.commit(cat, request)
                            true
                        } catch (_: HoglakeException.CommitConflict) {
                            false
                        }
                    }
                val truncate =
                    pool.submit<Long> {
                        start.await()
                        catalogs.truncateTable(cat, "ns", "t", uuid).snapshotId
                    }
                start.countDown()
                insert.get(10, TimeUnit.SECONDS)
                truncate.get(10, TimeUnit.SECONDS)
                assertThat(catalogs.listFiles(cat, "ns", "t")).isEmpty()
                val fresh = append(cat, "fresh.parquet", catalogs.getCatalog(cat).headSnapshotId)
                commits.commit(cat, fresh)
                assertThat(catalogs.getTable(cat, "ns", "t").recordCount).isEqualTo(10)
            } finally {
                pool.shutdownNow()
            }
        }
    }

    @Test
    fun `insert planned before truncate conflicts and blind insert after truncate follows commit order`() {
        val cat = fixture()
        val old = append(cat, snapshot = catalogs.getCatalog(cat).headSnapshotId)
        val uuid = catalogs.getTable(cat, "ns", "t").tableUuid
        catalogs.truncateTable(cat, "ns", "t", uuid)
        assertThatThrownBy { commits.commit(cat, old) }.isInstanceOf(HoglakeException.CommitConflict::class.java)
        commits.commit(cat, append(cat, "blind.parquet"))
        assertThat(catalogs.getTable(cat, "ns", "t").recordCount).isEqualTo(10)
    }

    @Test
    fun `failed truncate publication rolls back files DVs and snapshot`() {
        val cat = fixture()
        val appended = commits.commit(cat, append(cat))
        val file = catalogs.listFiles(cat, "ns", "t").single()
        commits.commit(
            cat,
            CommitRequest(
                readSnapshot = appended.snapshotId,
                deletes =
                    listOf(
                        TableDeletes(
                            "ns",
                            "t",
                            listOf(DeleteFileRegistration(file.dataFileId, "s3://bucket/$cat/dv", 2, 16)),
                        ),
                    ),
            ),
        )
        val before = catalogs.getTable(cat, "ns", "t")
        val head = catalogs.getCatalog(cat).headSnapshotId
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                CREATE FUNCTION reject_lifecycle_end() RETURNS trigger LANGUAGE plpgsql AS
                'BEGIN RAISE EXCEPTION ''publication failure''; END'
                """,
            )
            h.execute(
                """
                CREATE TRIGGER reject_lifecycle_end BEFORE UPDATE ON hog_data_file
                FOR EACH ROW EXECUTE FUNCTION reject_lifecycle_end()
                """,
            )
        }
        try {
            assertThatThrownBy {
                catalogs.truncateTable(cat, "ns", "t", before.tableUuid)
            }.hasMessageContaining("publication failure")
            assertThat(catalogs.getCatalog(cat).headSnapshotId).isEqualTo(head)
            assertThat(catalogs.getTable(cat, "ns", "t")).isEqualTo(before)
            db.jdbi.withHandleUnchecked { h ->
                assertThat(
                    h.createQuery("SELECT count(*) FROM hog_delete_file WHERE path = :path AND end_snapshot IS NULL")
                        .bind("path", "s3://bucket/$cat/dv").mapTo(Long::class.java).one(),
                ).isEqualTo(1)
            }
        } finally {
            db.jdbi.useHandleUnchecked {
                    h ->
                h.execute("DROP TRIGGER reject_lifecycle_end ON hog_data_file")
                h.execute("DROP FUNCTION reject_lifecycle_end()")
            }
        }
    }
}
