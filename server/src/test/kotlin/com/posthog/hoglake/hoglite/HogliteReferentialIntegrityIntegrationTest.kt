package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.TableFileEnds
import com.posthog.hoglake.model.TableInfo
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

/**
 * The tenant schema's referential integrity on the published engine:
 * hoglite runs every tenant connection with `PRAGMA foreign_keys` on, so
 * the FOREIGN KEYs and CHECKs HogliteSchema declares (schema.sql's, for
 * one tenant) refuse orphans, run their cascades, and hold the deferred
 * namespace edges at commit. The raw SQL here stands in for a store bug;
 * everything the store itself writes goes through the real API first, so
 * its statement order (snapshot row before change rows, receipt before
 * its ended count, table before its files) is what those calls prove.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteReferentialIntegrityIntegrationTest {
    private val engine by lazy { HogliteConformanceEngine() }
    private val store get() = engine.store

    @AfterAll
    fun close() = engine.close()

    private fun catalog(): String {
        val cat = "fk" + UUID.randomUUID().toString().replace("-", "")
        store.createCatalog(cat, "s3://hoglake-it/$cat")
        store.createNamespace(cat, "ns")
        return cat
    }

    private fun write(
        cat: String,
        sql: String,
        vararg args: Any?,
    ) = store.catalogOperation(cat) {
        store.runTx(cat, HogliteCatalogStore.freshTxid("fixture"), null, HogliteTx().apply { add(sql, *args) })
    }

    private fun query(
        cat: String,
        sql: String,
        vararg args: Any?,
    ) = store.catalogOperation(cat) {
        store.queryCatalog(cat, sql, *args)
    }

    private fun count(
        cat: String,
        sql: String,
        vararg args: Any?,
    ): Long = query(cat, sql, *args).firstRow()!![0].asLong()

    private fun scalar(
        cat: String,
        sql: String,
        vararg args: Any?,
    ): Any? = query(cat, sql, *args).firstRow()!![0]

    /** A table with one data file and one live deletion vector over it, through the commit API. */
    private fun tableWithFileAndVector(
        cat: String,
        name: String,
    ): Pair<TableInfo, Long> {
        val table = store.createTable(cat, "ns", name, listOf(ColumnDef("id", ColType.LONG)))
        val file = FileRegistration("s3://hoglake-it/$cat/$name.parquet", 10, 100)
        store.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", name, listOf(file)))))
        val fileId = store.listFiles(cat, "ns", name).single().dataFileId
        val vector = DeleteFileRegistration(fileId, "s3://hoglake-it/$cat/$name.puffin", 1, 20)
        store.commit(
            cat,
            CommitRequest(
                readSnapshot = store.getCatalog(cat).headSnapshotId,
                deletes = listOf(TableDeletes("ns", name, listOf(vector))),
            ),
        )
        assertThat(count(cat, "SELECT COUNT(*) FROM dvs WHERE data_file_id = ?", fileId)).isEqualTo(1)
        return table to fileId
    }

    @Test
    fun `a bootstrapped tenant enforces the foreign keys its schema declares`() {
        val cat = catalog()
        assertThat(count(cat, HogliteCatalogStore.FOREIGN_KEYS_PROBE_SQL)).isEqualTo(1)
        val files = scalar(cat, "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'files'")
        assertThat(files.toString()).contains("REFERENCES tables(id) ON DELETE CASCADE")
    }

    /** Every DDL and commit path writes its parents first; under enforcement each would otherwise refuse. */
    @Test
    fun `the store's own transactions hold under enforcement and leave no orphan`() {
        val cat = catalog()
        val (table, fileId) = tableWithFileAndVector(cat, "t")
        store.truncateTable(cat, "ns", "t", table.tableUuid, "test", "truncate")
        // A replacement with an idempotency key writes the receipt and then its ended count.
        val key = UUID.randomUUID()
        store.commit(
            cat,
            CommitRequest(
                readSnapshot = store.getCatalog(cat).headSnapshotId,
                idempotencyKey = key,
                author = "test",
                message = "replace",
                appends = listOf(TableAppend("ns", "t", listOf(FileRegistration("s3://hoglake-it/$cat/t2", 5, 50)))),
                fileEnds = listOf(TableFileEnds("ns", "t", table.tableUuid, allFiles = true)),
            ),
        )
        assertThat(count(cat, "SELECT COUNT(*) FROM receipt_ended WHERE idempotency_key = ?", key.toString()))
            .isEqualTo(1)
        store.dropTable(cat, "ns", "t", table.tableUuid, "test", "drop")
        store.createNamespace(cat, "empty")
        store.dropNamespace(cat, "empty", author = "test", message = "drop")

        val orphanChanges =
            "SELECT COUNT(*) FROM changes c LEFT JOIN snapshots s ON s.id = c.snapshot_id WHERE s.id IS NULL"
        val orphanFiles = "SELECT COUNT(*) FROM files f LEFT JOIN tables t ON t.id = f.table_id WHERE t.id IS NULL"
        val orphanVectors =
            "SELECT COUNT(*) FROM dvs d LEFT JOIN files f ON f.id = d.data_file_id WHERE f.id IS NULL"
        assertThat(count(cat, orphanChanges)).isZero()
        assertThat(count(cat, orphanFiles)).isZero()
        assertThat(count(cat, orphanVectors)).isZero()
        // The truncated vector is ended, not gone: history keeps it until expiry.
        assertThat(count(cat, "SELECT COUNT(*) FROM dvs WHERE data_file_id = ?", fileId)).isEqualTo(1)
        assertThat(count(cat, "SELECT COUNT(*) FROM changes")).isGreaterThan(5)
    }

    @Test
    fun `a file row for a missing table is refused as a store bug`() {
        val cat = catalog()
        val before = count(cat, "SELECT COUNT(*) FROM files")

        val failure =
            catchThrowable {
                write(cat, "$INSERT_FILE VALUES (999, 1, 1, 1, 0, 'pending', 's3://hoglake-it/$cat/orphan')")
            }

        assertThat(failure)
            .isExactlyInstanceOf(HogliteIntegrityViolation::class.java)
            .isNotInstanceOf(HoglakeException::class.java)
            .hasMessageContaining("FOREIGN KEY constraint failed")
            .hasMessageContaining("store bug")
        assertThat(count(cat, "SELECT COUNT(*) FROM files")).isEqualTo(before)
    }

    @Test
    fun `a change row for a missing snapshot and a vector for a missing file are refused`() {
        val cat = catalog()
        val table = store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))

        val change =
            catchThrowable {
                write(
                    cat,
                    "INSERT INTO changes(snapshot_id, kind, object_id) VALUES (999, 'table_altered', ?)",
                    table.tableId,
                )
            }
        assertThat(change).isExactlyInstanceOf(HogliteIntegrityViolation::class.java)
        val vector =
            catchThrowable {
                write(
                    cat,
                    "INSERT INTO dvs(data_file_id, table_id, delete_file_id, path, delete_count, file_size_bytes, " +
                        "begin_snapshot, epoch) VALUES (999, ?, 1, 's3://hoglake-it/$cat/v', 1, 1, 1, 1)",
                    table.tableId,
                )
            }
        assertThat(vector).isExactlyInstanceOf(HogliteIntegrityViolation::class.java)
        assertThat(count(cat, "SELECT COUNT(*) FROM dvs")).isZero()
    }

    /** hog_table_version's deferred no-action edge: checked at the end of the transaction, and it holds. */
    @Test
    fun `a namespace with versioned history under it cannot be deleted`() {
        val cat = catalog()
        store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val nsId = count(cat, "SELECT id FROM namespaces WHERE name = 'ns'")

        val failure = catchThrowable { write(cat, "DELETE FROM namespaces WHERE id = ?", nsId) }

        assertThat(failure).isExactlyInstanceOf(HogliteIntegrityViolation::class.java)
        assertThat(count(cat, "SELECT COUNT(*) FROM namespaces WHERE id = ?", nsId)).isEqualTo(1)
        // Deferred: a child may precede its parent inside one transaction.
        store.catalogOperation(cat) {
            store.runTx(
                cat,
                HogliteCatalogStore.freshTxid("fixture"),
                null,
                HogliteTx().apply {
                    add(
                        "INSERT INTO tables(id, namespace_id, namespace, name, table_uuid, created_snapshot) " +
                            "VALUES (500, 77, 'late', 'early', ?, 1)",
                        UUID.randomUUID().toString(),
                    )
                    add("INSERT INTO namespaces(id, name, created_snapshot) VALUES (77, 'late', 1)")
                },
            )
        }
        assertThat(count(cat, "SELECT COUNT(*) FROM tables WHERE id = 500")).isEqualTo(1)
    }

    @Test
    fun `deleting a table row cascades to its versions, columns, totals, files and vectors`() {
        val cat = catalog()
        val (table, fileId) = tableWithFileAndVector(cat, "t")
        val children = listOf("table_versions", "columns", "table_totals", "files", "dvs")
        for (child in children) {
            assertThat(count(cat, "SELECT COUNT(*) FROM $child WHERE table_id = ?", table.tableId))
                .describedAs(child)
                .isGreaterThanOrEqualTo(1)
        }

        write(cat, "DELETE FROM tables WHERE id = ?", table.tableId)

        for (child in children) {
            assertThat(count(cat, "SELECT COUNT(*) FROM $child WHERE table_id = ?", table.tableId))
                .describedAs(child)
                .isZero()
        }
        assertThat(count(cat, "SELECT COUNT(*) FROM dvs WHERE data_file_id = ?", fileId)).isZero()
    }

    @Test
    fun `deleting a file, a snapshot or a receipt takes its vectors, change rows or ended count with it`() {
        val cat = catalog()
        val (table, fileId) = tableWithFileAndVector(cat, "t")
        write(cat, "DELETE FROM files WHERE id = ?", fileId)
        assertThat(count(cat, "SELECT COUNT(*) FROM dvs WHERE data_file_id = ?", fileId)).isZero()

        val created = count(cat, "SELECT created_snapshot FROM tables WHERE id = ?", table.tableId)
        assertThat(count(cat, "SELECT COUNT(*) FROM changes WHERE snapshot_id = ?", created)).isEqualTo(1)
        write(cat, "DELETE FROM snapshots WHERE id = ?", created)
        assertThat(count(cat, "SELECT COUNT(*) FROM changes WHERE snapshot_id = ?", created)).isZero()

        write(
            cat,
            "INSERT INTO receipts(idempotency_key, fingerprint, snapshot_id, schema_version, created_at) " +
                "VALUES ('k', 'fp', 1, 1, ?)",
            HogliteSchema.now(),
        )
        write(cat, "INSERT INTO receipt_ended(idempotency_key, ended_files) VALUES ('k', 3)")
        write(cat, "DELETE FROM receipts WHERE idempotency_key = 'k'")
        assertThat(count(cat, "SELECT COUNT(*) FROM receipt_ended WHERE idempotency_key = 'k'")).isZero()
    }

    /**
     * Retirement deletes vectors before files and queues each path from the
     * row it is about to delete, so what it queues never depends on the
     * files -> dvs cascade: both objects are queued, and both rows are gone.
     */
    @Test
    fun `retirement of a dropped table queues its vector and data paths before the rows go`() {
        val cat = catalog()
        val (table, fileId) = tableWithFileAndVector(cat, "t")
        val dropped = store.dropTable(cat, "ns", "t", table.tableUuid, "test", "retire").snapshotId
        write(cat, "UPDATE meta SET earliest_snapshot_id = ? WHERE id = 1", dropped)

        val result = store.catalogOperation(cat) { HogliteRetirement(store).runOnce(cat) }

        assertThat(result.rowsRetired).isEqualTo(1)
        assertThat(result.dvsRetired).isEqualTo(1)
        assertThat(result.pathsQueued).isEqualTo(2)
        val queued = query(cat, "SELECT path, file_kind FROM removals ORDER BY path").rows
        assertThat(queued.map { it[0].asString() to it[1].asString() })
            .containsExactly("s3://hoglake-it/$cat/t.parquet" to "data", "s3://hoglake-it/$cat/t.puffin" to "delete")
        assertThat(count(cat, "SELECT COUNT(*) FROM files WHERE id = ?", fileId)).isZero()
        assertThat(count(cat, "SELECT COUNT(*) FROM dvs WHERE data_file_id = ?", fileId)).isZero()
        // The identity row stays: retirement drains files, never tables.
        assertThat(count(cat, "SELECT COUNT(*) FROM tables WHERE id = ?", table.tableId)).isEqualTo(1)
    }

    /**
     * The identifier policy as GLOB + length, at its edges: what the regex
     * accepts is accepted, what it refuses is refused.
     */
    @Test
    fun `identifier and catalog-name checks accept the policy's edges and refuse everything outside it`() {
        val cat = catalog()
        val accepted = listOf("a".repeat(128), "_", "a-", "A_z-9")
        for ((i, name) in accepted.withIndex()) {
            write(cat, "INSERT INTO namespaces(id, name, created_snapshot) VALUES (?, ?, 1)", 100L + i, name)
        }
        assertThat(count(cat, "SELECT COUNT(*) FROM namespaces WHERE id >= 100")).isEqualTo(accepted.size.toLong())
        for (name in listOf("a".repeat(129), "-a", "1a", "", "é", "a b", "a\n")) {
            val refusal =
                catchThrowable {
                    write(cat, "INSERT INTO namespaces(id, name, created_snapshot) VALUES (999, ?, 1)", name)
                }
            assertThat(refusal).describedAs("'%s'", name).isExactlyInstanceOf(HoglakeException.Validation::class.java)
        }
        assertThat(count(cat, "SELECT COUNT(*) FROM namespaces WHERE id = 999")).isZero()

        write(cat, "UPDATE meta SET name = ? WHERE id = 1", "a".repeat(63))
        assertThat(catchThrowable { write(cat, "UPDATE meta SET name = ? WHERE id = 1", "a".repeat(64)) })
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
        write(cat, "UPDATE meta SET name = ? WHERE id = 1", cat)

        val upper = "UPDATE namespaces SET contained_field = 'Team', contained_value = 1 WHERE id = 100"
        assertThat(catchThrowable { write(cat, upper) }).isExactlyInstanceOf(HoglakeException.Validation::class.java)
        write(cat, "UPDATE namespaces SET contained_field = 'team_id', contained_value = 1 WHERE id = 100")
    }

    /** hog_table_version's and hog_view's deferred namespace edges, on the live engine. */
    @Test
    fun `table versions and views may precede their namespace in one transaction but need it at commit`() {
        val cat = catalog()
        store.catalogOperation(cat) {
            store.runTx(
                cat,
                HogliteCatalogStore.freshTxid("fixture"),
                null,
                HogliteTx().apply {
                    add(
                        "INSERT INTO tables(id, namespace_id, namespace, name, table_uuid, created_snapshot) " +
                            "VALUES (600, 88, 'late', 't', ?, 1)",
                        UUID.randomUUID().toString(),
                    )
                    add(
                        "INSERT INTO table_versions(table_id, namespace_id, namespace, name, begin_snapshot) " +
                            "VALUES (600, 88, 'late', 't', 1)",
                    )
                    add(
                        "INSERT INTO views(id, namespace_id, namespace, name, view_uuid, dialect, sql_text, " +
                            "created_snapshot) VALUES (7, 88, 'late', 'v', ?, 'trino', 'SELECT 1', 1)",
                        UUID.randomUUID().toString(),
                    )
                    add("INSERT INTO namespaces(id, name, created_snapshot) VALUES (88, 'late', 1)")
                },
            )
        }
        assertThat(count(cat, "SELECT COUNT(*) FROM table_versions WHERE namespace_id = 88")).isEqualTo(1)
        assertThat(count(cat, "SELECT COUNT(*) FROM views WHERE namespace_id = 88")).isEqualTo(1)

        val orphanVersion =
            catchThrowable {
                store.catalogOperation(cat) {
                    store.runTx(
                        cat,
                        HogliteCatalogStore.freshTxid("fixture"),
                        null,
                        HogliteTx().apply {
                            add(
                                "INSERT INTO tables(id, namespace_id, namespace, name, table_uuid, " +
                                    "created_snapshot) VALUES (601, 99, 'none', 't2', ?, 1)",
                                UUID.randomUUID().toString(),
                            )
                            add(
                                "INSERT INTO table_versions(table_id, namespace_id, namespace, name, " +
                                    "begin_snapshot) VALUES (601, 99, 'none', 't2', 1)",
                            )
                        },
                    )
                }
            }
        assertThat(orphanVersion).isExactlyInstanceOf(HogliteIntegrityViolation::class.java)
        val orphanView =
            catchThrowable {
                write(
                    cat,
                    "INSERT INTO views(id, namespace_id, namespace, name, view_uuid, dialect, sql_text, " +
                        "created_snapshot) VALUES (8, 99, 'none', 'v2', ?, 'trino', 'SELECT 1', 1)",
                    UUID.randomUUID().toString(),
                )
            }
        assertThat(orphanView).isExactlyInstanceOf(HogliteIntegrityViolation::class.java)
        assertThat(count(cat, "SELECT COUNT(*) FROM table_versions WHERE namespace_id = 99")).isZero()
        assertThat(count(cat, "SELECT COUNT(*) FROM tables WHERE namespace_id = 99")).isZero()
        assertThat(count(cat, "SELECT COUNT(*) FROM views WHERE namespace_id = 99")).isZero()
    }

    /** The ledger's `(status = 'failed') = (error IS NOT NULL)`: a message-less failure still records. */
    @Test
    fun `a failed maintenance run with no exception message still records its ledger row`() {
        val cat = catalog()

        val failure =
            catchThrowable {
                store.recorded(cat, MaintenanceTask.EXPIRY, MaintenanceTrigger.MANUAL) { throw IllegalStateException() }
            }

        assertThat(failure).isExactlyInstanceOf(IllegalStateException::class.java)
        val row = query(cat, "SELECT status, error FROM maintenance_runs ORDER BY id DESC LIMIT 1").rows.single()
        assertThat(row[0]).isEqualTo("failed")
        assertThat(row[1]).isEqualTo("java.lang.IllegalStateException")
    }

    /** schema.sql's CHECKs, ported: a row of the wrong shape is a refusal of the statement, not a write. */
    @Test
    fun `a check violation is refused as a validation error`() {
        val cat = catalog()
        val table = store.createTable(cat, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))

        val negative =
            catchThrowable {
                write(cat, "$INSERT_FILE VALUES (?, 1, -1, 1, 0, 'pending', 's3://hoglake-it/$cat/neg')", table.tableId)
            }
        assertThat(negative)
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("CHECK constraint failed")
        val emptyWindow =
            catchThrowable {
                write(cat, "UPDATE table_versions SET end_snapshot = begin_snapshot WHERE table_id = ?", table.tableId)
            }
        assertThat(emptyWindow).isExactlyInstanceOf(HoglakeException.Validation::class.java)
        val badName =
            catchThrowable { write(cat, "UPDATE columns SET name = '1bad' WHERE table_id = ?", table.tableId) }
        assertThat(badName).isExactlyInstanceOf(HoglakeException.Validation::class.java)
        val live = "SELECT COUNT(*) FROM table_versions WHERE table_id = ? AND end_snapshot IS NULL"
        assertThat(count(cat, live, table.tableId)).isEqualTo(1)
    }

    companion object {
        private const val INSERT_FILE =
            "INSERT INTO files(table_id, begin_snapshot, record_count, file_size_bytes, row_id_start, " +
                "stats_state, path)"
    }
}
