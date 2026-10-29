package com.posthog.hoglake.service

import com.posthog.hoglake.Database
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.PartitionLookupInvalidRequest
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.testing.PartitionLookupFixture
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PartitionLookupSnapshotIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val service = PartitionLookupService(db.jdbi)
    private val tuple = listOf("a", "b")

    @AfterAll
    fun close() = db.close()

    @Test
    fun `retained cursor resolves its original namespace and table after recreation`() {
        val f = PartitionLookupFixture(db)
        f.append(tuple, tuple)
        val first = service.lookup(f.name, "ns", "t", f.specId, tuple, limit = 1)
        f.catalogs.dropTable(f.name, "ns", "t", author = "test", message = "drop original")
        f.catalogs.dropNamespace(f.name, "ns", author = "test", message = "drop original namespace")
        f.catalogs.createNamespace(f.name, "ns")
        val replacement =
            f.catalogs.createTable(
                f.name,
                "ns",
                "t",
                listOf(ColumnDef("a", ColType.STRING), ColumnDef("b", ColType.STRING)),
            )
        f.alter.alterTable(
            f.name,
            "ns",
            "t",
            listOf(
                AlterOp.SetPartitionSpec(
                    listOf(PartitionFieldDef(1, Transform.IDENTITY), PartitionFieldDef(2, Transform.IDENTITY)),
                ),
            ),
        )
        f.append(tuple)

        val second = service.lookup(f.name, "ns", "t", f.specId, tuple, limit = 1, cursor = first.nextCursor)
        assertThat(second.tableUuid).isEqualTo(first.tableUuid).isNotEqualTo(replacement.tableUuid)
        assertThat(second.snapshotId).isEqualTo(first.snapshotId)
        assertThat(second.files.single().dataFileId).isNotEqualTo(first.files.single().dataFileId)
        assertThat(second.nextCursor).isNull()
        val current = f.catalogs.getTable(f.name, "ns", "t", totals = false)
        val currentSpec = current.partitionSpec!!.specId
        val head = service.lookup(f.name, "ns", "t", currentSpec, tuple)
        assertThat(head.tableUuid).isEqualTo(replacement.tableUuid)
        assertThat(head.files).hasSize(1)
        val moved = PartitionLookupCursor.parse(first.nextCursor!!).copy(snapshot = head.snapshotId).encode()
        assertThatThrownBy {
            service.lookup(f.name, "ns", "t", currentSpec, tuple, limit = 1, cursor = moved)
        }.isInstanceOf(PartitionLookupInvalidRequest::class.java)
    }

    @Test
    fun `table rename preserves old URL only at retained snapshot`() {
        val f = PartitionLookupFixture(db)
        val snapshot = f.append(tuple)
        f.alter.alterTable(f.name, "ns", "t", listOf(AlterOp.RenameTable("renamed")))
        assertThat(service.lookup(f.name, "ns", "t", f.specId, tuple, snapshot).files).hasSize(1)
        assertThat(service.lookup(f.name, "ns", "renamed", f.specId, tuple).files).hasSize(1)
        assertThatThrownBy { service.lookup(f.name, "ns", "t", f.specId, tuple) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }

    @Test
    fun `file visibility includes begin and excludes end snapshot`() {
        val f = PartitionLookupFixture(db)
        val begin = f.append(tuple)
        val uuid = service.lookup(f.name, "ns", "t", f.specId, tuple).tableUuid
        val end =
            f.catalogs.truncateTable(
                f.name,
                "ns",
                "t",
                uuid,
                author = "test",
                message = "boundary test",
            ).snapshotId
        assertThat(service.lookup(f.name, "ns", "t", f.specId, tuple, begin).files).hasSize(1)
        val ended = service.lookup(f.name, "ns", "t", f.specId, tuple, end)
        assertThat(ended.files).isEmpty()
        assertThat(ended.aggregates.totals!!.fileCount).isZero()
        assertThat(ended.nextCursor).isNull()
    }

    @Test
    fun `DV visibility is pinned while physical rows remain unchanged`() {
        val f = PartitionLookupFixture(db)
        val before = f.append(tuple)
        val file = service.lookup(f.name, "ns", "t", f.specId, tuple).files.single()

        fun delete(count: Long): Long =
            f.commits.commit(
                f.name,
                CommitRequest(
                    readSnapshot = db.head(f.name),
                    deletes =
                        listOf(
                            TableDeletes(
                                "ns",
                                "t",
                                listOf(
                                    DeleteFileRegistration(
                                        file.dataFileId,
                                        "s3://bucket/${f.name}/$count.puffin",
                                        count,
                                        64,
                                    ),
                                ),
                            ),
                        ),
                ),
            ).snapshotId

        val firstDelete = delete(2)
        delete(4)
        for ((snapshot, count) in listOf(before to 0L, firstDelete to 1L, db.head(f.name) to 1L)) {
            val totals = service.lookup(f.name, "ns", "t", f.specId, tuple, snapshot).aggregates.totals!!
            assertThat(totals.dvCount).isEqualTo(count)
            assertThat(totals.physicalRows).isEqualTo(10)
            assertThat(totals.lastFileWriteSnapshot).isEqualTo(before)
        }
    }

    @Test
    fun `concurrent truncate and expiry cannot mix floor check with reclaimed file metadata`() {
        val f = PartitionLookupFixture(db)
        val snapshot = f.append(tuple)
        val original = service.lookup(f.name, "ns", "t", f.specId, tuple)
        val uuid = original.tableUuid
        val instrumented = Database.jdbi(db.dataSource)
        val fired = AtomicBoolean()
        instrumented.setSqlLogger(
            object : SqlLogger {
                override fun logBeforeExecution(context: StatementContext) {
                    if (context.rawSql.contains("WITH probes AS MATERIALIZED") &&
                        fired.compareAndSet(false, true)
                    ) {
                        // A separate thread gets a different JDBI handle and transaction.
                        Executors.newSingleThreadExecutor().use { executor ->
                            executor.submit {
                                f.catalogs.truncateTable(
                                    f.name,
                                    "ns",
                                    "t",
                                    uuid,
                                    author = "test",
                                    message = "concurrent truncate",
                                )
                                f.append(tuple)
                                db.jdbi.useHandleUnchecked { h ->
                                    h.execute(
                                        "UPDATE hog_catalog SET snapshot_retention_seconds = 3600, " +
                                            "consumer_floor = false WHERE catalog_id = ?",
                                        f.catalogId,
                                    )
                                    h.execute(
                                        "UPDATE hog_snapshot SET snapshot_time = now() - interval '2 days' " +
                                            "WHERE catalog_id = ?",
                                        f.catalogId,
                                    )
                                }
                                ExpiryService(db.jdbi).runOnce(f.name, batchSize = 1000)
                            }.get(10, TimeUnit.SECONDS)
                        }
                    }
                }
            },
        )
        val result = PartitionLookupService(instrumented).lookup(f.name, "ns", "t", f.specId, tuple, snapshot)
        assertThat(fired.get()).isTrue()
        assertThat(result.files).hasSize(1)
        assertThat(result.aggregates.totals!!.physicalRows).isEqualTo(10)
        assertThat(result.files.single().beginSnapshot).isEqualTo(snapshot)
        val remaining =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT (SELECT count(*) FROM hog_data_file WHERE catalog_id = :catalog AND data_file_id = :file),
                           (SELECT count(*) FROM hog_file_partition_value WHERE catalog_id = :catalog AND data_file_id = :file)
                    """,
                ).bind("catalog", f.catalogId).bind("file", original.files.single().dataFileId)
                    .map { rs, _ -> rs.getLong(1) to rs.getLong(2) }.one()
            }
        assertThat(remaining).isEqualTo(0L to 0L)
        assertThatThrownBy { service.lookup(f.name, "ns", "t", f.specId, tuple, snapshot) }
            .isInstanceOf(HoglakeException.Expired::class.java)
    }
}
