package com.posthog.hoglake.commit

import com.posthog.hoglake.compaction.CompactionConfig
import com.posthog.hoglake.compaction.CompactionService
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableFileEnds
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.ExpiryService
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
import java.util.concurrent.atomic.AtomicInteger

/**
 * What only PostgreSQL has: snapshot expiry and compaction (hoglite has
 * neither), against a replacement. The parity suite covers the rest.
 *
 * Compaction's COMMIT is simulated here with the rows it writes (a new file,
 * its inputs ended, a `table_compacted` change), because running it needs
 * parquet objects; what is under test is the replacement's reaction to those
 * rows, and compaction's planner on the files a replacement ended.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReplacementMaintenanceIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi
    private val catalogs = CatalogService(jdbi)
    private val alter = AlterService(jdbi)
    private val commits = CommitService(jdbi)
    private val expiry = ExpiryService(jdbi)
    private val counter = AtomicInteger(0)
    private val deadStore =
        ObjectStore(
            endpoint = "http://127.0.0.1:9",
            region = "us-east-1",
            accessKey = "unused",
            secretKey = "unused",
            pathStyle = true,
        )
    private val cfg = CompactionConfig(targetBytes = 1000, minInputFiles = 2, maxGroupsPerRun = 10)

    @AfterAll
    fun tearDown() {
        deadStore.close()
        db.close()
    }

    private fun fixture(): String {
        val cat = "repl-maint-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://hoglake-it/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(cat, "ns", "ev", listOf(ColumnDef("day", ColType.LONG), ColumnDef("v", ColType.LONG)))
        alter.alterTable(
            cat,
            "ns",
            "ev",
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY)))),
        )
        return cat
    }

    private fun head(cat: String) = catalogs.getCatalog(cat).headSnapshotId

    private fun uuid(cat: String) = catalogs.getTable(cat, "ns", "ev").tableUuid

    private fun file(
        cat: String,
        name: String,
        day: String,
        bytes: Long = 100,
    ) = FileRegistration("s3://hoglake-it/$cat/$name", 10, bytes, 50, partitionValues = listOf(day))

    private fun seed(
        cat: String,
        vararg files: FileRegistration,
    ) = commits.commit(
        cat,
        CommitRequest(
            readSnapshot = head(cat),
            idempotencyKey = UUID.randomUUID(),
            appends = listOf(TableAppend("ns", "ev", files.toList())),
        ),
    )

    private fun replace(
        cat: String,
        end: TableFileEnds,
        appends: List<FileRegistration> = emptyList(),
        read: Long = head(cat),
    ) = commits.commit(
        cat,
        CommitRequest(
            readSnapshot = read,
            idempotencyKey = UUID.randomUUID(),
            author = "t",
            message = "m",
            appends = if (appends.isEmpty()) emptyList() else listOf(TableAppend("ns", "ev", appends)),
            fileEnds = listOf(end),
        ),
    )

    private fun days(
        cat: String,
        vararg days: String,
    ) = TableFileEnds("ns", "ev", uuid(cat), partitions = days.map { listOf<String?>(it) })

    private fun all(cat: String) = TableFileEnds("ns", "ev", uuid(cat), allFiles = true)

    private fun live(
        cat: String,
        snapshot: Long? = null,
    ) = catalogs.listFiles(cat, "ns", "ev", snapshot = snapshot).map { it.path.substringAfterLast('/') }

    private fun recordedFiles(
        cat: String,
        name: String,
    ): Long =
        jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT count(*) FROM hog_data_file f JOIN hog_catalog c USING (catalog_id) " +
                    "WHERE c.name = :cat AND f.path LIKE :path",
            ).bind("cat", cat).bind("path", "%/$name").mapTo(Long::class.java).one()
        }

    /** Age every snapshot past retention and run the sweep: the floor moves to head. */
    private fun expireAll(cat: String): Long {
        jdbi.useHandleUnchecked { h ->
            val id = catalogs.getCatalog(cat).catalogId
            h.execute("UPDATE hog_snapshot SET snapshot_time = now() - interval '2 days' WHERE catalog_id = ?", id)
            h.execute("UPDATE hog_catalog SET snapshot_retention_seconds = 3600 WHERE catalog_id = ?", id)
        }
        return expiry.runOnce(cat, batchSize = 1000).newEarliestSnapshotId
    }

    /** The rows a compaction commit writes: one output in [day] replacing [inputs], and its change row. */
    private fun simulateCompaction(
        cat: String,
        day: String,
        output: String,
        vararg inputs: String,
    ): Long =
        jdbi.withHandleUnchecked { h ->
            val catalogId = catalogs.getCatalog(cat).catalogId
            val tableId = catalogs.getTable(cat, "ns", "ev").tableId
            val (snapshot, fileId) =
                h.createQuery(
                    "UPDATE hog_catalog SET last_snapshot_id = last_snapshot_id + 1, next_file_id = next_file_id + 1 " +
                        "WHERE catalog_id = :c RETURNING last_snapshot_id, next_file_id - 1",
                ).bind("c", catalogId).map { rs, _ -> rs.getLong(1) to rs.getLong(2) }.one()
            h.execute(
                "INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version) " +
                    "SELECT catalog_id, ?, schema_version FROM hog_catalog WHERE catalog_id = ?",
                snapshot,
                catalogId,
            )
            h.execute(
                "INSERT INTO hog_snapshot_change (catalog_id, snapshot_id, kind, object_id) " +
                    "VALUES (?, ?, 'table_compacted', ?)",
                catalogId,
                snapshot,
                tableId,
            )
            val specId =
                h.createQuery("SELECT spec_id FROM hog_data_file WHERE catalog_id = ? AND path LIKE ? LIMIT 1")
                    .bind(0, catalogId).bind(1, "%/${inputs.first()}").mapTo(Long::class.java).one()
            h.createUpdate(
                "UPDATE hog_data_file SET end_snapshot = :s " +
                    "WHERE catalog_id = :c AND path LIKE ANY(CAST(:paths AS text[]))",
            ).bind("s", snapshot).bind("c", catalogId)
                .bind("paths", inputs.map { "%/$it" }.toTypedArray()).execute()
            h.execute(
                "INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot, path, record_count, " +
                    "file_size_bytes, row_id_start, spec_id, explicit_row_ids) " +
                    "VALUES (?, ?, ?, ?, ?, 20, 200, 1000, ?, true)",
                catalogId,
                fileId,
                tableId,
                snapshot,
                "s3://hoglake-it/$cat/$output",
                specId,
            )
            h.execute(
                "INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value) VALUES (?, ?, 0, ?)",
                catalogId,
                fileId,
                day,
            )
            snapshot
        }

    @Test
    fun `the changefeed barrier holds when expiry has purged the ended files at the floor`() {
        val cat = fixture()
        seed(cat, file(cat, "d1", "20401"))
        val before = head(cat)
        val replaced = replace(cat, days(cat, "20401"), listOf(file(cat, "n1", "20401")))
        assertThat(replaced.snapshotId).isEqualTo(before + 1)

        // The floor reaches the replacement's own snapshot, which purges the file it ended: the
        // trace the barrier used to be inferred from.
        assertThat(expireAll(cat)).isEqualTo(replaced.snapshotId)
        assertThat(recordedFiles(cat, "d1")).describedAs("the ended row is purged").isZero()

        // A consumer that last read the snapshot before it is still inside the retained window,
        // and must still be told the feed cannot carry the deletion.
        assertThatThrownBy { catalogs.changes(cat, "ns", "ev", before) }
            .isExactlyInstanceOf(HoglakeException.ReconciliationRequired::class.java)
        assertThat(catalogs.changes(cat, "ns", "ev", replaced.snapshotId).files).isEmpty()
    }

    @Test
    fun `time travel to before a replacement is refused once expiry has moved past it`() {
        val cat = fixture()
        seed(cat, file(cat, "d1", "20401"))
        val before = head(cat)
        val replaced = replace(cat, days(cat, "20401"), listOf(file(cat, "n1", "20401")))
        assertThat(live(cat, before)).containsExactly("d1")
        assertThat(live(cat, replaced.snapshotId)).containsExactly("n1")
        expireAll(cat)
        assertThatThrownBy { live(cat, before) }.isExactlyInstanceOf(HoglakeException.Expired::class.java)
        assertThat(live(cat, replaced.snapshotId)).containsExactly("n1")
    }

    @Test
    fun `an ended path cannot be re-appended before expiry, and not while cleanup still owns it after`() {
        val cat = fixture()
        seed(cat, file(cat, "x", "20401"))
        replace(cat, days(cat, "20401"), listOf(file(cat, "y", "20401")))
        // Before expiry the ended row is recorded: refused as a recorded path.
        assertThatThrownBy { replace(cat, days(cat, "20401"), listOf(file(cat, "x", "20401"))) }
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("$cat/x")

        // Expiry purges the ended row and queues the object for removal, which is its own refusal
        // until the drain has run.
        expireAll(cat)
        assertThat(recordedFiles(cat, "x")).isZero()
        assertThatThrownBy { replace(cat, days(cat, "20401"), listOf(file(cat, "x", "20401"))) }
            .isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessageContaining("scheduled for deletion by the cleanup queue")
    }

    @Test
    fun `a compaction since read_snapshot is never a conflict, for a partition or an all_files end`() {
        for (byAll in listOf(false, true)) {
            val cat = fixture()
            seed(cat, file(cat, "a", "20401"), file(cat, "b", "20401"), file(cat, "other", "20402"))
            val read = head(cat)
            simulateCompaction(cat, "20401", "ab", "a", "b")
            val end = if (byAll) all(cat) else days(cat, "20401")
            // The ends are the files live at commit: the compaction's output, not its dead inputs.
            val result = replace(cat, end, listOf(file(cat, "n", "20401")), read = read)
            assertThat(result.endedFiles).describedAs("all_files=$byAll").isEqualTo(if (byAll) 2 else 1)
            assertThat(live(cat)).describedAs("all_files=$byAll")
                .containsExactlyInAnyOrder(*(if (byAll) arrayOf("n") else arrayOf("other", "n")))
        }
    }

    @Test
    fun `a replacement's ended files are not compaction candidates and its new ones are`() {
        val cat = fixture()
        seed(cat, file(cat, "a", "20401"), file(cat, "b", "20401"), file(cat, "c", "20401"))
        val svc = CompactionService(jdbi, deadStore, cfg)
        assertThat(svc.planTable(cat, "ns", "ev", cfg).groups.single().files.map { it.path.substringAfterLast('/') })
            .containsExactly("a", "b", "c")
        replace(cat, days(cat, "20401"), listOf(file(cat, "n1", "20401"), file(cat, "n2", "20401")))
        val plan = svc.planTable(cat, "ns", "ev", cfg)
        assertThat(plan.groups.single().files.map { it.path.substringAfterLast('/') }).containsExactly("n1", "n2")
    }
}
