package com.posthog.hoglake.hoglite

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.posthog.hoglake.RequestAdmission
import com.posthog.hoglake.model.CatalogNotEmptyReason
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.RetirementResult
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.persistence.PartialResult
import com.posthog.hoglake.service.PatchField
import com.posthog.hoglake.service.RemovalStore
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import java.util.UUID

/** Run retirement against the published engine and MinIO. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteRetirementIntegrationTest {
    private val engine by lazy { HogliteConformanceEngine() }
    private val store get() = engine.store

    @AfterAll
    fun close() = engine.close()

    private fun catalog(): String {
        val cat = "retire" + UUID.randomUUID().toString().replace("-", "")
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

    @Test
    fun `drained drop history cannot starve a later retirement with vectors`() {
        val cat = catalog()
        val seed = store.createTable(cat, "ns", "seed", listOf(ColumnDef("id", ColType.LONG)))
        val drop = store.dropTable(cat, "ns", "seed", seed.tableUuid, "test", "seed").snapshotId
        // SQL seeds only valid, already drained history. All target work below uses the real API.
        // 1001 older identities exceed the 1000-candidate page, without 1001 HTTP DDLs.
        write(
            cat,
            """WITH RECURSIVE n(i) AS (VALUES(2) UNION ALL SELECT i + 1 FROM n WHERE i < 1002)
            INSERT INTO tables(id, namespace_id, namespace, name, table_uuid,
                               next_field_id, created_snapshot, dropped_snapshot)
            SELECT i, namespace_id, namespace, 'old' || i, '00000000-0000-0000-0000-' || printf('%012d', i),
                   next_field_id, created_snapshot, dropped_snapshot FROM n CROSS JOIN tables WHERE tables.id = ?""",
            seed.tableId,
        )
        write(
            cat,
            """INSERT INTO table_versions(table_id, namespace_id, namespace, name, begin_snapshot, end_snapshot)
            SELECT id, namespace_id, namespace, name, created_snapshot, dropped_snapshot FROM tables WHERE id > 1""",
        )
        write(
            cat,
            """INSERT INTO columns(table_id, field_id, ordinal, name, type, nullable, begin_snapshot, end_snapshot)
            SELECT id, 1, 0, 'id', 'long', 1, created_snapshot, dropped_snapshot FROM tables WHERE id > 1""",
        )
        write(
            cat,
            "INSERT INTO changes(snapshot_id, kind, object_id) " +
                "SELECT created_snapshot, 'table_created', id FROM tables WHERE id > 1",
        )
        write(
            cat,
            "INSERT INTO changes(snapshot_id, kind, object_id) " +
                "SELECT dropped_snapshot, 'table_dropped', id FROM tables WHERE id > 1",
        )
        write(
            cat,
            """INSERT INTO files(id, table_id, path, record_count, file_size_bytes, row_id_start,
            stats_state, begin_snapshot, end_snapshot)
            SELECT id, id, 's3://hoglake-it/$cat/ended-' || id, 1, 100, 0, 'pending',
                   created_snapshot, dropped_snapshot FROM tables""",
        )
        write(cat, "INSERT INTO table_totals(table_id) SELECT id FROM tables WHERE id > 1")
        write(cat, "UPDATE tables SET next_row_id = 1")
        write(cat, "UPDATE meta SET next_file_id = 1003, next_table_id = 1003 WHERE id = 1")
        val target = store.createTable(cat, "ns", "target", listOf(ColumnDef("id", ColType.LONG)))
        val path = "s3://hoglake-it/$cat/target.parquet"
        store.commit(
            cat,
            CommitRequest(appends = listOf(TableAppend("ns", "target", listOf(FileRegistration(path, 10, 100))))),
        )
        val fileId = store.listFiles(cat, "ns", "target").single().dataFileId
        val vectors = (1..2).map { "s3://hoglake-it/$cat/vector$it.puffin" }
        for ((i, vector) in vectors.withIndex()) {
            store.commit(
                cat,
                CommitRequest(
                    readSnapshot = store.getCatalog(cat).headSnapshotId,
                    deletes =
                        listOf(
                            TableDeletes(
                                "ns",
                                "target",
                                listOf(DeleteFileRegistration(fileId, vector, (i + 1).toLong(), 20)),
                            ),
                        ),
                ),
            )
        }
        val targetDrop = store.dropTable(cat, "ns", "target", target.tableUuid, "test", "retire").snapshotId
        // Above-floor drops must not be touched, even with live files.
        val future = store.createTable(cat, "ns", "future", listOf(ColumnDef("id", ColType.LONG)))
        store.commit(
            cat,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "future",
                            listOf(FileRegistration("s3://hoglake-it/$cat/future", 1, 100)),
                        ),
                    ),
            ),
        )
        store.dropTable(cat, "ns", "future", future.tableUuid, "test", "not yet")
        write(cat, "UPDATE meta SET earliest_snapshot_id = ? WHERE id = 1", targetDrop)
        assertThat(
            query(cat, "SELECT COUNT(*) FROM tables WHERE dropped_snapshot <= ?", drop).firstRow()!![0].asLong(),
        ).isEqualTo(1002)
        val paused = store.catalogOperation(cat) { HogliteRetirement(store, runBudgetMs = 0).runOnce(cat) }
        assertThat(paused.rowsRetired).isZero()
        assertThat(paused.tablesRemaining).isEqualTo(1)
        assertLedger(cat, paused)
        val result = store.catalogOperation(cat) { HogliteRetirement(store).runOnce(cat) }
        assertThat(result.tables).isEqualTo(1)
        assertThat(result.rowsRetired).isEqualTo(1)
        assertThat(result.dvsRetired).isEqualTo(2)
        assertThat(result.pathsQueued).isEqualTo(3)
        assertThat(result.tablesRemaining).isZero()
        assertLedger(cat, result)
        assertThat(query(cat, "SELECT id FROM files WHERE table_id = ?", target.tableId).rows).isEmpty()
        assertThat(query(cat, "SELECT delete_file_id FROM dvs WHERE table_id = ?", target.tableId).rows).isEmpty()
        assertThat(query(cat, "SELECT path FROM removals ORDER BY path").rows.map { it[0].asString() })
            .containsExactlyElementsOf((vectors + path).sorted())
        assertThat(query(cat, "SELECT id FROM files WHERE table_id = ?", future.tableId).rows).hasSize(1)
        val again = store.catalogOperation(cat) { HogliteRetirement(store).runOnce(cat) }
        assertThat(again.rowsRetired).isZero()
        assertThat(again.tablesRemaining).isZero()
        assertLedger(cat, again)
    }

    private fun assertLedger(
        cat: String,
        result: com.posthog.hoglake.model.RetirementResult,
    ) {
        val row =
            query(
                cat,
                "SELECT status, result_json FROM maintenance_runs WHERE task = 'retirement' ORDER BY id DESC LIMIT 1",
            )
                .rows.single()
        assertThat(row[0]).isEqualTo("ok")
        val mapper = com.posthog.hoglake.wireObjectMapper()
        assertThat(
            mapper.readTree(row[1].asString()),
        ).isEqualTo(mapper.readTree(mapper.writeValueAsString(result)))
    }

    @Test
    fun `retirement production eligibility statements use a table keyed file probe`() {
        val cat = catalog()
        val table = store.createTable(cat, "ns", "files", listOf(ColumnDef("id", ColType.LONG)))
        val dropped = store.dropTable(cat, "ns", "files", table.tableUuid, "test", "plan").snapshotId
        // 100001 metadata rows, mostly live. No stats payload: this probes eligibility,
        // not IO cost, production cardinality, retirement batching, or reader capacity.
        write(
            cat,
            """WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i + 1 FROM n WHERE i < 100001)
            INSERT INTO files(id, table_id, path, record_count, file_size_bytes, row_id_start,
                              stats_state, begin_snapshot, end_snapshot)
            SELECT i, ?, 's3://hoglake-it/$cat/file-' || i, 1, 100, i, 'pending', ?,
                   CASE WHEN i > 96000 THEN ? ELSE NULL END FROM n""",
            table.tableId,
            table.readSnapshotId,
            dropped,
        )
        for (sql in listOf(HogliteRetirement.CANDIDATES_SQL, HogliteRetirement.REMAINING_SQL)) {
            val plan = query(cat, "EXPLAIN QUERY PLAN $sql", dropped).rows.joinToString { it.last().asString() }
            println("retirement eligibility: 100001 file rows; $sql; $plan")
            assertThat(
                plan,
            ).containsPattern("SEARCH f (?:EXISTS )?USING INDEX files_(?:table|scan|listing) \\(table_id=\\?\\)")
                .doesNotContain("SCAN f").doesNotContain("TEMP B-TREE")
        }
        val victimsPlan =
            query(cat, "EXPLAIN QUERY PLAN ${HogliteRetirement.VICTIMS_SQL}", table.tableId, -1L, 0L, 1000)
                .rows.joinToString { it.last().asString() }
        println("retirement victims: 100001 file rows; $victimsPlan")
        assertThat(
            victimsPlan,
        ).contains("USING INDEX files_scan").doesNotContain("SCAN files").doesNotContain("TEMP B-TREE")
        assertThat(query(cat, HogliteRetirement.CANDIDATES_SQL, dropped).rows.map { it[0].asLong() })
            .containsExactly(table.tableId)
        assertThat(query(cat, HogliteRetirement.REMAINING_SQL, dropped).firstRow()!![0].asLong()).isEqualTo(1)
    }

    private fun droppedFiles(
        cat: String,
        count: Int = 3,
        name: String = "t",
    ): Long {
        val table = store.createTable(cat, "ns", name, listOf(ColumnDef("id", ColType.LONG)))
        store.commit(
            cat,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            name,
                            (1..count).map {
                                FileRegistration("s3://hoglake-it/$cat/${if (name == "t") "" else name}file$it", 1, 100)
                            },
                        ),
                    ),
            ),
        )
        val drop = store.dropTable(cat, "ns", name, table.tableUuid, "test", "retire").snapshotId
        write(cat, "UPDATE meta SET earliest_snapshot_id = ? WHERE id = 1", drop)
        return table.tableId
    }

    @Test
    fun `small batches drain all files and queue ceiling stops work`() {
        val cat = catalog()
        droppedFiles(cat)
        val paused = HogliteRetirement(store, queueCeiling = 0).runOnce(cat)
        assertThat(paused.skippedQueueFull).isEqualTo(1)
        assertThat(paused.rowsRetired).isZero()
        val result = HogliteRetirement(store, batchSize = 1).runOnce(cat)
        assertThat(result.rowsRetired).isEqualTo(3)
        assertThat(result.batches).isEqualTo(3)
        assertThat(result.tables).isEqualTo(1)
        assertThat(result.pathsQueued).isEqualTo(3)
        assertThat(result.tablesRemaining).isZero()
        assertLedger(cat, result)
    }

    @Test
    fun `a vector inserted after victim selection is queued in the deletion transaction`() {
        val cat = catalog()
        val table = droppedFiles(cat, 1)
        HogliteInterposer(engine.url()).use { proxy ->
            val hook =
                proxy.hook({ m, p, b -> HogliteInterposer.isWriteTx(m, p, b) && "INSERT INTO removals" in b }) {
                    write(
                        cat,
                        "INSERT INTO dvs(delete_file_id, table_id, data_file_id, path, " +
                            "delete_count, file_size_bytes, begin_snapshot, epoch) VALUES (1, ?, 1, ?, 1, 20, 1, 1)",
                        table,
                        "s3://hoglake-it/$cat/late.puffin",
                    )
                }
            val result = HogliteRetirement(proxy.store).runOnce(cat)
            assertThat(hook.fired).isTrue()
            assertThat(result.dvsRetired).isEqualTo(1)
            assertThat(result.pathsQueued).isEqualTo(2)
            assertThat(query(cat, "SELECT * FROM dvs").rows).isEmpty()
            assertThat(query(cat, "SELECT path FROM removals").rows.map { it[0].asString() })
                .containsExactlyInAnyOrder("s3://hoglake-it/$cat/file1", "s3://hoglake-it/$cat/late.puffin")
        }
    }

    @Test
    fun `failed later batch preserves committed progress in the run ledger`() {
        val cat = catalog()
        droppedFiles(cat)
        HogliteInterposer(engine.url()).use { proxy ->
            var batches = 0
            proxy.hook({ m, p, b ->
                HogliteInterposer.isWriteTx(m, p, b) && "INSERT INTO removals" in b && ++batches == 2
            }) { write(cat, "UPDATE tables SET dropped_snapshot = NULL WHERE name = 't'") }
            val failure = runCatching { HogliteRetirement(proxy.store, batchSize = 1).runOnce(cat) }.exceptionOrNull()
            assertThat(failure).isInstanceOf(PartialResult::class.java)
            val partial =
                (failure as PartialResult).partial as RetirementResult
            assertThat(partial.rowsRetired).isEqualTo(1)
            assertThat(partial.pathsQueued).isEqualTo(1)
            assertThat(partial.tables).isEqualTo(1)
            val ledger =
                query(
                    cat,
                    "SELECT status, result_json FROM maintenance_runs ORDER BY id DESC LIMIT 1",
                ).rows.single()
            assertThat(ledger[0]).isEqualTo("failed")
            assertThat(store.json.readTree(ledger[1].asString())["rows_retired"].asLong()).isEqualTo(1)
            assertThat(query(cat, "SELECT id FROM files").rows).hasSize(2)
            assertThat(query(cat, "SELECT path FROM removals").rows).hasSize(1)
        }
    }

    @Test
    fun `foreign vectors prevent any deletion or removal queue entry`() {
        val cat = catalog()
        droppedFiles(cat, 1)
        // The vector's table must exist (dvs.table_id REFERENCES tables); a live one with no files.
        val other = store.createTable(cat, "ns", "other", listOf(ColumnDef("id", ColType.LONG))).tableId
        write(
            cat,
            "INSERT INTO dvs(delete_file_id, table_id, data_file_id, path, " +
                "delete_count, file_size_bytes, begin_snapshot, epoch) VALUES (1, ?, 1, ?, 1, 20, 1, 1)",
            other,
            "s3://hoglake-it/$cat/foreign",
        )
        val result = HogliteRetirement(store).runOnce(cat)
        assertThat(result.skippedTables).isEqualTo(1)
        assertThat(result.rowsRetired).isZero()
        assertThat(result.tablesRemaining).isEqualTo(1)
        assertThat(query(cat, "SELECT id FROM files").rows).hasSize(1)
        assertThat(query(cat, "SELECT delete_file_id FROM dvs").rows).hasSize(1)
        assertThat(query(cat, "SELECT path FROM removals").rows).isEmpty()
    }

    @Test
    fun `queue ceiling is checked for each batch and again inside the transaction`() {
        val cat = catalog()
        droppedFiles(cat)
        val first = HogliteRetirement(store, batchSize = 1, queueCeiling = 1).runOnce(cat)
        assertThat(first.rowsRetired).isEqualTo(1)
        assertThat(first.skippedQueueFull).isEqualTo(1)
        assertThat(first.tablesRemaining).isEqualTo(1)
        val raced = catalog()
        droppedFiles(raced, 1)
        HogliteInterposer(engine.url()).use { proxy ->
            val hook =
                proxy.hook({ m, p, b -> HogliteInterposer.isWriteTx(m, p, b) && "INSERT INTO removals" in b }) {
                    write(
                        raced,
                        "INSERT INTO removals(path, file_kind, reason, queued_snapshot, queued_at) " +
                            "VALUES (?, 'data', 'snapshot_expiry', 1, ?)",
                        "s3://hoglake-it/$raced/already",
                        HogliteSchema.now(),
                    )
                }
            val result = HogliteRetirement(proxy.store, queueCeiling = 1).runOnce(raced)
            assertThat(hook.fired).isTrue()
            assertThat(result.skippedQueueFull).isEqualTo(1)
            assertThat(result.rowsRetired).isZero()
            assertThat(query(raced, "SELECT id FROM files").rows).hasSize(1)
            assertThat(query(raced, "SELECT path FROM removals").rows).hasSize(1)
        }
    }

    @Test
    fun `victim keyset progresses past a concurrently ended batch and equal row ids`() {
        val cat = catalog()
        droppedFiles(cat)
        write(cat, "UPDATE files SET row_id_start = 0")
        HogliteInterposer(engine.url()).use { proxy ->
            val hook =
                proxy.hook({ m, p, b -> HogliteInterposer.isWriteTx(m, p, b) && "INSERT INTO removals" in b }) {
                    write(cat, "UPDATE files SET end_snapshot = begin_snapshot + 1 WHERE id = 1")
                }
            val result = HogliteRetirement(proxy.store, batchSize = 1).runOnce(cat)
            assertThat(hook.fired).isTrue()
            assertThat(result.rowsRetired).isEqualTo(2)
            assertThat(result.pathsQueued).isEqualTo(2)
            assertThat(result.tablesRemaining).isZero()
            assertThat(query(cat, "SELECT id FROM files").rows.map { it[0].asLong() }).containsExactly(1L)
            assertThat(query(cat, "SELECT path FROM removals").rows.map { it[0].asString() })
                .containsExactlyInAnyOrder("s3://hoglake-it/$cat/file2", "s3://hoglake-it/$cat/file3")
        }
    }

    @Test
    fun `partial results exclude a fully retired earlier table from remaining work`() {
        val cat = catalog()
        droppedFiles(cat, 1)
        droppedFiles(cat, 1, "second")
        HogliteInterposer(engine.url()).use { proxy ->
            var batches = 0
            proxy.hook({ m, p, b ->
                HogliteInterposer.isWriteTx(m, p, b) && "INSERT INTO removals" in b && ++batches == 2
            }) { error("injected later table failure") }
            val failure = runCatching { HogliteRetirement(proxy.store).runOnce(cat) }.exceptionOrNull()
            assertThat(failure).isInstanceOf(PartialResult::class.java)
            val partial = (failure as PartialResult).partial as RetirementResult
            assertThat(partial.rowsRetired).isEqualTo(1)
            assertThat(partial.tablesRemaining).isEqualTo(1)
            val ledger = query(cat, "SELECT result_json FROM maintenance_runs ORDER BY id DESC LIMIT 1").rows.single()
            assertThat(store.json.readTree(ledger[0].asString())["tables_remaining"].asLong()).isEqualTo(1)
        }
    }

    /** A table dropped with live files, below a floor that only expiry can advance. */
    private fun droppedBehindRetention(
        cat: String,
        vararg paths: String = arrayOf("s3://hoglake-it/$cat/live.parquet"),
    ): Long = droppedWithFiles(cat, "t", *paths).also { behindRetention(cat) }

    private fun droppedWithFiles(
        cat: String,
        name: String,
        vararg paths: String,
    ): Long {
        val table = store.createTable(cat, "ns", name, listOf(ColumnDef("id", ColType.LONG)))
        store.commit(
            cat,
            CommitRequest(appends = listOf(TableAppend("ns", name, paths.map { FileRegistration(it, 1, 100) }))),
        )
        store.dropTable(cat, "ns", name, table.tableUuid, "test", "retire")
        return table.tableId
    }

    /** Leave every snapshot but head stale, so the next expiry advances the floor past every drop. */
    private fun behindRetention(cat: String) {
        store.dropNamespace(cat, "ns", author = "test", message = "empty it")
        store.options.patch(cat, PatchField.Set(1L), null, "test", "short retention")
        write(cat, "UPDATE snapshots SET ts = '2001-01-01T00:00:00.000Z'")
    }

    /** Run [body] as a request that arrived [agoMs] ago, as the dispatch seam would admit it. */
    private fun <T> arrivedAgo(
        agoMs: Long,
        body: () -> T,
    ): T {
        RequestAdmission.holder.set(RequestAdmission(System.nanoTime() - agoMs * 1_000_000))
        try {
            return body()
        } finally {
            RequestAdmission.holder.remove()
        }
    }

    private fun tasks(cat: String) =
        query(cat, "SELECT task, run_trigger, status FROM maintenance_runs ORDER BY id")
            .rows.map { row -> row.map { it.asString() } }

    private fun dropRefusal(cat: String) =
        (runCatching { store.dropCatalog(cat, "test", "drop") }.exceptionOrNull() as HoglakeException.CatalogNotEmpty)
            .reason

    @Test
    fun `manual expiry drives retirement so a catalog with a dropped table can be dropped`() {
        val cat = catalog()
        val tableId = droppedBehindRetention(cat)
        assertThat(dropRefusal(cat)).isEqualTo(CatalogNotEmptyReason.TABLES_PENDING_RETIREMENT)
        val expired = store.expiry.runOnce(cat, 100)
        assertThat(expired.newEarliestSnapshotId).isEqualTo(store.getCatalog(cat).headSnapshotId)
        assertThat(query(cat, "SELECT id FROM files WHERE table_id = ?", tableId).rows).isEmpty()
        assertThat(query(cat, "SELECT path, reason FROM removals").rows.map { it[0].asString() to it[1].asString() })
            .containsExactly("s3://hoglake-it/$cat/live.parquet" to HogliteRetirement.REASON)
        assertThat(
            query(cat, "SELECT task, run_trigger, status FROM maintenance_runs ORDER BY id")
                .rows.map { row -> row.map { it.asString() } },
        ).containsExactly(listOf("expiry", "manual", "ok"), listOf("retirement", "manual", "ok"))
        // What is left is cleanup's queue; once cleanup drains it, the drop that answered 409 goes through.
        assertThat(dropRefusal(cat)).isEqualTo(CatalogNotEmptyReason.REMOVALS_PENDING)
        RemovalStore(engine.config()).use { objects ->
            assertThat(HogliteCleanup(engine.config(), store, objects).runOnce(cat, 10).removed).isEqualTo(1)
        }
        store.dropCatalog(cat, "test", "drop")
        assertThatThrownBy { store.getCatalog(cat) }.isInstanceOf(HoglakeException.NotFound::class.java)
    }

    @Test
    fun `retirement after expiry attempts one batch even under a zero purge budget`() {
        val cat = catalog()
        val second = "s3://hoglake-it/$cat/second.parquet"
        val tableId = droppedBehindRetention(cat, "s3://hoglake-it/$cat/first.parquet", second)
        // The purge budget of 0 leaves the hand-off no window: only its owed batch, of one row, runs.
        val zero =
            HogliteCatalogStore(
                engine.client,
                HogliteConformanceEngine.TENANTS,
                expiryPurgeBudgetMs = 0,
                retirementBatch = 1,
            )
        val expired = zero.expiry.runOnce(cat, 100)
        assertThat(expired.newEarliestSnapshotId).isEqualTo(store.getCatalog(cat).headSnapshotId)
        val ledger =
            query(cat, "SELECT task, run_trigger, status, result_json FROM maintenance_runs ORDER BY id").rows
        assertThat(ledger.map { row -> row.take(3).map { it.asString() } })
            .containsExactly(listOf("expiry", "manual", "ok"), listOf("retirement", "manual", "ok"))
        val retired = store.json.readTree(ledger[1][3].asString())
        assertThat(retired["batches"].asLong()).isEqualTo(1)
        assertThat(retired["tables_remaining"].asLong()).isEqualTo(1)
        assertThat(query(cat, "SELECT path FROM files WHERE table_id = ?", tableId).rows.map { it[0].asString() })
            .containsExactly(second)
        assertThat(dropRefusal(cat)).isEqualTo(CatalogNotEmptyReason.TABLES_PENDING_RETIREMENT)
    }

    @Test
    fun `a stuck lower table does not take the owed batch from a retirable one`() {
        val cat = catalog()
        val stuck = droppedWithFiles(cat, "stuck", "s3://hoglake-it/$cat/stuck.parquet")
        val tableId = droppedWithFiles(cat, "t", "s3://hoglake-it/$cat/live.parquet")
        behindRetention(cat)
        // The vector's table must exist (dvs.table_id REFERENCES tables); a dropped one does.
        write(
            cat,
            "INSERT INTO dvs(delete_file_id, table_id, data_file_id, path, delete_count, file_size_bytes, " +
                "begin_snapshot, epoch) SELECT 1, ?, id, ?, 1, 20, 1, 1 FROM files WHERE table_id = ?",
            tableId,
            "s3://hoglake-it/$cat/foreign.puffin",
            stuck,
        )
        val zero = HogliteCatalogStore(engine.client, HogliteConformanceEngine.TENANTS, expiryPurgeBudgetMs = 0)
        zero.expiry.runOnce(cat, 100)
        val retired =
            query(cat, "SELECT result_json FROM maintenance_runs WHERE task = 'retirement'").rows.single()
                .let { store.json.readTree(it[0].asString()) }
        // MUTATION: clear the debt before each batch and the stuck table spends it; nothing is retired.
        assertThat(retired["skipped_tables"].asLong()).isEqualTo(1)
        assertThat(retired["rows_retired"].asLong()).isEqualTo(1)
        assertThat(query(cat, "SELECT id FROM files WHERE table_id = ?", tableId).rows).isEmpty()
        assertThat(query(cat, "SELECT id FROM files WHERE table_id = ?", stuck).rows).hasSize(1)
    }

    @Test
    fun `the retirement window closes by its cap whatever the purge budget`() {
        val cat = catalog()
        val second = "s3://hoglake-it/$cat/second.parquet"
        val tableId = droppedBehindRetention(cat, "s3://hoglake-it/$cat/first.parquet", second)
        // Uncapped, a 30 s purge budget leaves a 60 s window open, and both one-row batches run.
        val long =
            HogliteCatalogStore(
                engine.client,
                HogliteConformanceEngine.TENANTS,
                expiryPurgeBudgetMs = 30_000,
                retirementBatch = 1,
            )
        arrivedAgo(HogliteExpiry.RETIREMENT_WINDOW_MAX_MS + 200) { long.expiry.runOnce(cat, 100) }
        assertThat(tasks(cat)).containsExactly(listOf("expiry", "manual", "ok"), listOf("retirement", "manual", "ok"))
        // MUTATION: drop the cap and the second file is retired too.
        assertThat(query(cat, "SELECT path FROM files WHERE table_id = ?", tableId).rows.map { it[0].asString() })
            .containsExactly(second)
    }

    @Test
    fun `an expiry that ends too late for retirement to land skips it`() {
        val cat = catalog()
        val tableId = droppedBehindRetention(cat)
        val latestStart =
            HogliteExpiry.RETIREMENT_END_MS - HogliteRetirement.OVERRUN_MS - HogliteRetirement.BOOKKEEPING_MS
        val capture = ListAppender<ILoggingEvent>().apply { start() }
        val audit = LoggerFactory.getLogger(Audit.LOGGER_NAME) as Logger
        audit.addAppender(capture)
        val expired =
            try {
                arrivedAgo(latestStart + 200) { store.expiry.runOnce(cat, 100) }
            } finally {
                audit.detachAppender(capture)
                capture.stop()
            }
        assertThat(expired.newEarliestSnapshotId).isEqualTo(store.getCatalog(cat).headSnapshotId)
        // MUTATION: drop the late-start check and retirement runs and records itself.
        assertThat(tasks(cat)).containsExactly(listOf("expiry", "manual", "ok"))
        assertThat(query(cat, "SELECT id FROM files WHERE table_id = ?", tableId).rows).hasSize(1)
        // MUTATION: skip without the audit event and nothing durable says retirement did not run.
        assertThat(capture.list.map { it.formattedMessage }.filter { it.startsWith("retirement ") })
            .containsExactly("retirement skipped_late")
    }

    @Test
    fun `a run of stuck tables stops paying its debt and still records an ok run`() {
        val cat = catalog()
        // Each table's file carries a vector of the next table's, so every batch is refused as stuck.
        val tables = (0 until 6).map { droppedWithFiles(cat, "stuck$it", "s3://hoglake-it/$cat/stuck$it.parquet") }
        behindRetention(cat)
        tables.forEachIndexed { i, table ->
            write(
                cat,
                "INSERT INTO dvs(delete_file_id, table_id, data_file_id, path, delete_count, file_size_bytes, " +
                    "begin_snapshot, epoch) SELECT ?, ?, id, ?, 1, 20, 1, 1 FROM files WHERE table_id = ?",
                1_000 + i,
                tables[(i + 1) % tables.size],
                "s3://hoglake-it/$cat/foreign$i.puffin",
                table,
            )
        }
        HogliteInterposer(engine.url()).use { proxy ->
            // Six stuck batches take 3 s, past the run's 2 s work bound; the debt may start none after 1 s.
            val hooks =
                tables.map {
                    proxy.hook({ m, p, b -> HogliteInterposer.isWriteTx(m, p, b) && "retire-batch:" in b }) {
                        Thread.sleep(500)
                    }
                }
            val zero = HogliteCatalogStore(proxy.client, HogliteConformanceEngine.TENANTS, expiryPurgeBudgetMs = 0)
            zero.expiry.runOnce(cat, 100)
            // MUTATION: keep the debt until a batch commits rows, uncapped, and the run is cut off as failed.
            assertThat(tasks(cat))
                .containsExactly(listOf("expiry", "manual", "ok"), listOf("retirement", "manual", "ok"))
            val retired =
                query(cat, "SELECT result_json FROM maintenance_runs WHERE task = 'retirement'").rows.single()
                    .let { store.json.readTree(it[0].asString()) }
            // More than one: the first stuck table did not end the run.
            assertThat(retired["skipped_tables"].asLong()).isBetween(2, tables.size - 1L)
            assertThat(hooks.count { it.fired }.toLong()).isEqualTo(retired["skipped_tables"].asLong())
            assertThat(retired["rows_retired"].asLong()).isZero()
            assertThat(query(cat, "SELECT id FROM files").rows).hasSize(tables.size)
        }
    }

    @Test
    fun `a retirement that fails before reading its floor sends no backlog count`() {
        val cat = catalog()
        droppedFiles(cat)
        HogliteInterposer(engine.url()).use { proxy ->
            val hook = proxy.hook({ _, _, b -> "FROM meta WHERE id = 1" in b }) { error("injected meta failure") }
            val failure = runCatching { HogliteRetirement(proxy.store).runOnce(cat) }.exceptionOrNull()
            assertThat(hook.fired).isTrue()
            assertThat(failure).isNotNull().isNotInstanceOf(PartialResult::class.java)
            assertThat(failure!!.suppressed).isEmpty()
            // MUTATION: count from floor 0 when no floor was read and the probe goes out.
            assertThat(proxy.requests.filter { HogliteRetirement.REMAINING_SQL in it }).isEmpty()
            assertThat(tasks(cat)).containsExactly(listOf("retirement", "manual", "failed"))
            assertThat(query(cat, "SELECT id FROM files").rows).hasSize(3)
        }
    }

    @Test
    fun `a retirement failure after expiry does not fail the committed sweep`() {
        val cat = catalog()
        val tableId = droppedBehindRetention(cat)
        // A restricting reference makes the real retirement batch fail; expiry does not touch live files.
        write(cat, "CREATE TABLE hold_file(file_id INTEGER REFERENCES files(id) ON DELETE RESTRICT)")
        write(cat, "INSERT INTO hold_file SELECT id FROM files WHERE table_id = ?", tableId)
        val expired = store.expiry.runOnce(cat, 100)
        assertThat(expired.newEarliestSnapshotId).isEqualTo(store.getCatalog(cat).headSnapshotId)
        assertThat(
            query(cat, "SELECT task, run_trigger, status FROM maintenance_runs ORDER BY id")
                .rows.map { row -> row.map { it.asString() } },
        ).containsExactly(listOf("expiry", "manual", "ok"), listOf("retirement", "manual", "failed"))
        assertThat(query(cat, "SELECT id FROM files WHERE table_id = ?", tableId).rows).hasSize(1)
        assertThat(dropRefusal(cat)).isEqualTo(CatalogNotEmptyReason.TABLES_PENDING_RETIREMENT)
    }

    @Test
    fun `a positive budget that its reads use up still attempts one batch`() {
        val cat = catalog()
        droppedFiles(cat)
        // The metadata, queue and candidate reads alone take longer than 1 ms on any engine.
        val result = HogliteRetirement(store, batchSize = 1, runBudgetMs = 1).runOnce(cat)
        assertThat(result.batches).isEqualTo(1)
        assertThat(result.rowsRetired).isEqualTo(1)
        assertThat(result.tablesRemaining).isEqualTo(1)
        assertThat(query(cat, "SELECT id FROM files").rows).hasSize(2)
    }

    @Test
    fun `a stalled batch after expiry ends by the request deadline, not the client's retry budget`() {
        val cat = catalog()
        val tableId = droppedBehindRetention(cat)
        HogliteInterposer(engine.url()).use { proxy ->
            val hook =
                proxy.hook({ m, p, b -> HogliteInterposer.isWriteTx(m, p, b) && "retire-batch:" in b }) {
                    Thread.sleep(15_000)
                }
            val budget = 200L
            val budgeted =
                HogliteCatalogStore(proxy.client, HogliteConformanceEngine.TENANTS, expiryPurgeBudgetMs = budget)
            val started = System.nanoTime()
            val expired = budgeted.expiry.runOnce(cat, 100)
            val elapsedMs = (System.nanoTime() - started) / 1_000_000
            assertThat(hook.fired).isTrue()
            assertThat(expired.newEarliestSnapshotId).isEqualTo(store.getCatalog(cat).headSnapshotId)
            // Unbounded, the stalled batch alone waits out the client's 8 s retry budget.
            assertThat(elapsedMs)
                .isLessThan(2 * budget + HogliteRetirement.OVERRUN_MS + HogliteRetirement.BOOKKEEPING_MS + 1_500)
            assertThat(
                query(cat, "SELECT task, run_trigger, status FROM maintenance_runs ORDER BY id")
                    .rows.map { row -> row.map { it.asString() } },
            ).containsExactly(listOf("expiry", "manual", "ok"), listOf("retirement", "manual", "failed"))
            assertThat(query(cat, "SELECT id FROM files WHERE table_id = ?", tableId).rows).hasSize(1)
        }
    }

    @Test
    fun `a stalled backlog count after expiry leaves the retirement run its ledger row`() {
        val cat = catalog()
        val tableId = droppedBehindRetention(cat)
        HogliteInterposer(engine.url()).use { proxy ->
            val hook =
                proxy.hook({ _, _, b -> HogliteRetirement.REMAINING_SQL in b }) { Thread.sleep(15_000) }
            val budget = 200L
            val budgeted =
                HogliteCatalogStore(proxy.client, HogliteConformanceEngine.TENANTS, expiryPurgeBudgetMs = budget)
            val started = System.nanoTime()
            budgeted.expiry.runOnce(cat, 100)
            val elapsedMs = (System.nanoTime() - started) / 1_000_000
            assertThat(hook.fired).isTrue()
            assertThat(elapsedMs)
                .isLessThan(2 * budget + HogliteRetirement.OVERRUN_MS + HogliteRetirement.BOOKKEEPING_MS + 1_500)
            // MUTATION: let the count run to the run's last deadline and the ledger INSERT is refused unsent.
            assertThat(
                query(cat, "SELECT task, run_trigger, status FROM maintenance_runs ORDER BY id")
                    .rows.map { row -> row.map { it.asString() } },
            ).containsExactly(listOf("expiry", "manual", "ok"), listOf("retirement", "manual", "failed"))
            assertThat(query(cat, "SELECT id FROM files WHERE table_id = ?", tableId).rows).isEmpty()
        }
    }
}
