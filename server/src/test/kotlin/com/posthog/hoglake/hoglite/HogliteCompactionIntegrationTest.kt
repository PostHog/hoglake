package com.posthog.hoglake.hoglite

import com.posthog.hoglake.compaction.CompactionConfig
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.persistence.PartialResult
import com.posthog.hoglake.testing.TestImages
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.MinIOContainer
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import java.net.URI
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteCompactionIntegrationTest {
    private lateinit var engine: HogliteConformanceEngine
    private lateinit var minio: MinIOContainer
    private lateinit var objects: ObjectStore
    private val store get() = engine.store
    private val alters get() = store.alters

    @BeforeAll
    fun start() {
        engine = HogliteConformanceEngine()
        minio = TestImages.minio()
        minio.start()
        s3Client().use { s3 -> s3.createBucket { it.bucket("hoglake-it") } }
        objects = ObjectStore(cfg())
    }

    @AfterAll
    fun stop() {
        if (::objects.isInitialized) objects.close()
        if (::minio.isInitialized) minio.stop()
        if (::engine.isInitialized) engine.close()
    }

    private fun s3Client(): S3Client =
        S3Client.builder()
            .endpointOverride(URI.create("http://127.0.0.1:${minio.firstMappedPort}"))
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create(minio.userName, minio.password)),
            )
            .region(Region.US_EAST_1)
            .forcePathStyle(true)
            .build()

    private fun cfg() =
        com.posthog.hoglake.Config(
            s3Endpoint = "http://127.0.0.1:${minio.firstMappedPort}",
            s3Region = "us-east-1",
            s3AccessKey = minio.userName,
            s3SecretKey = minio.password,
            s3PathStyle = true,
            hydratorIntervalMs = 0,
            expiryIntervalMs = 0,
            cleanupIntervalMs = 0,
            compactionIntervalMs = 0,
            retirementIntervalMs = 0,
        )

    private val columns = listOf(ColumnDef("team_id", ColType.LONG), ColumnDef("v", ColType.LONG))
    private val teamSpec = listOf(PartitionFieldDef(1, Transform.IDENTITY))

    private fun fixture(): String {
        val cat = HogliteConformanceEngine.uniqueName("compact")
        store.createCatalog(cat, "s3://hoglake-it/$cat")
        store.createNamespace(cat, "ingest")
        for (table in listOf("events", "plain")) {
            store.createTable(cat, "ingest", table, columns)
            alters.alterTable(cat, "ingest", table, listOf(AlterOp.SetPartitionSpec(teamSpec)))
        }
        alters.alterTable(cat, "ingest", "events", listOf(AlterOp.SetObjectPrefixField("team_id")))
        return cat
    }

    private fun root(
        cat: String,
        table: String,
    ) = "s3://hoglake-it/$cat/data/ingest/$table/"

    private fun under(
        cat: String,
        table: String,
        value: String,
    ) = root(cat, table) + "team_id=$value/"

    private fun le8(v: Long): ByteArray = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(v).array()

    private fun stats(
        lower: ByteArray?,
        upper: ByteArray?,
        nulls: Long = 0,
    ) = listOf(
        ColumnStats(1, 10, nulls, null, null, lower, upper),
        ColumnStats(2, 10, 0, null, null, le8(1), le8(9)),
    )

    private fun file(
        path: String,
        value: String?,
        columnStats: List<ColumnStats>?,
        rows: Long = 10,
        size: Long = 1_000,
        footer: Long? = 20,
    ) = FileRegistration(path, rows, size, footer, columnStats, listOf(value))

    private fun good(
        cat: String,
        table: String = "events",
        value: Long = 4,
    ) = file(under(cat, table, "$value") + "${UUID.randomUUID()}.parquet", "$value", stats(le8(value), le8(value)))

    private fun append(
        cat: String,
        table: String,
        vararg files: FileRegistration,
    ) = store.commit(
        cat,
        CommitRequest(
            idempotencyKey = UUID.randomUUID(),
            appends = listOf(TableAppend("ingest", table, files.toList())),
        ),
    )

    private fun q(
        cat: String,
        sql: String,
        vararg args: Any?,
    ) = store.catalogOperation(cat) { store.queryCatalog(cat, sql, *args) }

    private fun scalar(
        cat: String,
        sql: String,
        vararg args: Any?,
    ): Long = q(cat, sql, *args).firstRow()!![0].asLong()

    private fun rawWrite(
        cat: String,
        vararg sql: String,
    ) {
        store.catalogOperation(cat) {
            val tx = HogliteTx()
            for (s in sql) tx.add(s)
            store.runTx(cat, HogliteCatalogStore.freshTxid("it-raw"), null, tx)
        }
    }

    private fun fileId(
        cat: String,
        path: String,
    ) = scalar(cat, "SELECT id FROM files WHERE path = ?", path)

    private fun compaction(
        beforeCommit: (String, com.posthog.hoglake.compaction.CompactionGroup) -> Unit = { _, _ -> },
    ) = HogliteCompaction(
        store,
        objects,
        CompactionConfig(
            targetBytes = 1L shl 30,
            minInputFiles = 2,
            maxInputFiles = 2,
            maxFanIn = 2,
            maxGroupsPerRun = 10,
            maxCandidates = 100,
        ),
        beforeCommit,
    )

    private fun appendReal(
        cat: String,
        name: String,
        team: Long,
        claim: Long = team,
        inlineStats: Boolean = true,
    ): String {
        val (path, size) = putParquet(cat, under(cat, "events", "$claim") + name, team)
        append(
            cat,
            "events",
            file(
                path,
                "$claim",
                if (inlineStats) stats(le8(claim), le8(claim)) else null,
                rows = 2,
                size = size,
                footer = null,
            ),
        )
        return path
    }

    @Test
    fun `compaction writes object-prefix outputs under the group's prefix, from verified inputs only`() {
        val cat = fixture()
        appendReal(cat, "a.parquet", 4)
        appendReal(cat, "b.parquet", 4)
        appendReal(cat, "c.parquet", 6)
        appendReal(cat, "d.parquet", 6, inlineStats = false)
        appendReal(cat, "e.parquet", 8)
        val quarantined = appendReal(cat, "f.parquet", 8)
        rawWrite(cat, "UPDATE files SET prefix_quarantined = 1 WHERE path = '$quarantined'")

        val result = compaction().runOnce(cat, null, MaintenanceTrigger.MANUAL)
        assertThat(result.groupsCompacted).isEqualTo(1L)
        assertThat(result.objectPrefixUnresolvable).isEqualTo(0L)
        val output =
            q(
                cat,
                "SELECT path, stats_state, record_count FROM files WHERE end_snapshot IS NULL AND explicit_row_ids = 1",
            )
                .rows.single()
        assertThat(output[0].asString()).startsWith(under(cat, "events", "4")).endsWith(".parquet")
        assertThat(output[1].asString()).isEqualTo("provided")
        assertThat(output[2].asLong()).isEqualTo(4L)
        assertThat(scalar(cat, "SELECT COUNT(*) FROM files WHERE end_snapshot IS NULL")).isEqualTo(5L)
    }

    @Test
    fun `compaction never registers an output whose footer does not prove the value`() {
        val cat = fixture()
        appendReal(cat, "a.parquet", team = 5, claim = 4)
        appendReal(cat, "b.parquet", team = 5, claim = 4)
        val before = store.getCatalog(cat).headSnapshotId
        val result = compaction().runOnce(cat, null, MaintenanceTrigger.MANUAL)
        assertThat(result.invalidData).isEqualTo(1L)
        assertThat(result.groupsCompacted).isEqualTo(0L)
        assertThat(store.getCatalog(cat).headSnapshotId).isEqualTo(before)
        assertThat(scalar(cat, "SELECT COUNT(*) FROM files WHERE end_snapshot IS NULL")).isEqualTo(2L)
        assertThat(
            scalar(cat, "SELECT COUNT(*) FROM removals WHERE reason = 'compaction_staging' AND drained_at IS NULL"),
        ).isEqualTo(1L)
    }

    @Test
    fun `compaction refuses groups whose prefix cannot be named, uncharged, before any IO`() {
        val cat = fixture()
        val a = appendReal(cat, "a.parquet", 4)
        val b = appendReal(cat, "b.parquet", 4)
        rawWrite(cat, "UPDATE files SET partition_values_json = '[\"04\"]' WHERE path IN ('$a', '$b')")
        val result = compaction().runOnce(cat, null, MaintenanceTrigger.MANUAL)
        assertThat(result.objectPrefixUnresolvable).isEqualTo(1L)
        assertThat(result.groupsCompacted + result.failedGroups + result.invalidData).isEqualTo(0L)
        assertThat(scalar(cat, "SELECT COUNT(*) FROM removals WHERE reason = 'compaction_staging'")).isEqualTo(0L)
    }

    @Test
    fun `the commit re-checks the live layout and skips a group written under a prefix it no longer names`() {
        val cat = fixture()
        appendReal(cat, "a.parquet", 4)
        appendReal(cat, "b.parquet", 4)
        val moved =
            compaction { c, _ ->
                rawWrite(c, "UPDATE table_layouts SET object_prefix_field_id = 2 WHERE end_snapshot IS NULL")
            }.runOnce(cat, null, MaintenanceTrigger.MANUAL)
        assertThat(moved.skippedConflicts).isEqualTo(1L)
        assertThat(moved.groupsCompacted).isEqualTo(0L)
        assertThat(scalar(cat, "SELECT COUNT(*) FROM files WHERE end_snapshot IS NULL")).isEqualTo(2L)
        val plain = fixture()
        for (n in listOf("a", "b")) {
            val (path, size) = putParquet(plain, root(plain, "plain") + "$n.parquet", 4)
            append(plain, "plain", file(path, "4", stats(le8(4), le8(4)), rows = 2, size = size, footer = null))
        }
        val arrived =
            compaction { c, _ ->
                rawWrite(
                    c,
                    "INSERT INTO table_layouts(table_id, begin_snapshot, object_prefix_field_id) " +
                        "SELECT table_id, (SELECT next_snapshot - 1 FROM meta WHERE id = 1), 1 FROM table_versions " +
                        "WHERE name = 'plain' AND end_snapshot IS NULL",
                )
            }.runOnce(plain, null, MaintenanceTrigger.MANUAL)
        assertThat(arrived.skippedConflicts).isEqualTo(1L)
        assertThat(arrived.groupsCompacted).isEqualTo(0L)
        rawWrite(plain, "DELETE FROM table_layouts")
        val ok = compaction().runOnce(plain, null, MaintenanceTrigger.MANUAL)
        assertThat(ok.groupsCompacted).isEqualTo(1L)
        val out = q(plain, "SELECT path FROM files WHERE end_snapshot IS NULL").rows.single()[0].asString()
        assertThat(out).startsWith(root(plain, "plain")).doesNotContain("team_id=")
    }

    private fun explain(
        cat: String,
        sql: String,
        vararg args: Any?,
    ): HogliteQueryResult =
        store.catalogOperation(cat) {
            store.runTx(
                cat,
                HogliteCatalogStore.freshTxid("explain"),
                null,
                HogliteTx().apply { add("EXPLAIN QUERY PLAN $sql", *args) },
            ).single()
        }

    private fun defaults() =
        CompactionConfig(
            targetBytes = 1L shl 30,
            minInputFiles = 2,
            maxInputFiles = 2,
            maxFanIn = 2,
            maxGroupsPerRun = 10,
        )

    private fun dv(
        cat: String,
        path: String,
        positions: List<Long>,
    ) {
        val bytes = com.posthog.hoglake.compaction.PuffinTestFiles.deletionVector(positions)
        val vector = under(cat, "events", "4") + UUID.randomUUID() + ".puffin"
        objects.put(vector, bytes)
        store.commit(
            cat,
            CommitRequest(
                readSnapshot = store.getCatalog(cat).headSnapshotId,
                deletes =
                    listOf(
                        TableDeletes(
                            "ingest",
                            "events",
                            listOf(
                                DeleteFileRegistration(
                                    fileId(cat, path),
                                    vector,
                                    positions.size.toLong(),
                                    bytes.size.toLong(),
                                ),
                            ),
                        ),
                    ),
            ),
        )
    }

    private fun rowIds(path: String): List<Long> {
        val tmp = Files.createTempFile("compact-output", ".parquet")
        try {
            Files.write(tmp, objects.get(path))
            val ids = mutableListOf<Long>()
            org.apache.parquet.hadoop.ParquetFileReader.open(org.apache.parquet.io.LocalInputFile(tmp)).use { reader ->
                val schema = reader.footer.fileMetaData.schema
                val io = org.apache.parquet.io.ColumnIOFactory().getColumnIO(schema)
                var pages = reader.readNextRowGroup()
                while (pages != null) {
                    val records =
                        io.getRecordReader(
                            pages,
                            org.apache.parquet.example.data.simple.convert.GroupRecordConverter(schema),
                        )
                    repeat(pages.rowCount.toInt()) { ids += records.read().getLong("_hog_row_id", 0) }
                    pages = reader.readNextRowGroup()
                }
            }
            return ids
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    @Test
    fun `rewrite applies vectors preserves row ids totals and historical consumer windows`() {
        val cat = fixture()
        val a = appendReal(cat, "a.parquet", 4)
        appendReal(cat, "b.parquet", 4)
        dv(cat, a, listOf(0))
        val before = store.getCatalog(cat).headSnapshotId
        val result = compaction().runOnce(cat, null, MaintenanceTrigger.MANUAL)
        assertThat(result.groupsCompacted).isEqualTo(1)
        assertThat(result.filesIn).isEqualTo(2)
        val output = q(cat, "SELECT path, id FROM files WHERE end_snapshot IS NULL").rows.single()
        assertThat(rowIds(output[0].asString())).containsExactly(1L, 2L, 3L)
        assertThat(scalar(cat, "SELECT COUNT(*) FROM dvs WHERE end_snapshot IS NULL")).isZero()
        assertThat(
            scalar(
                cat,
                "SELECT COUNT(*) FROM files WHERE begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?)",
                before,
                before,
            ),
        ).isEqualTo(2)
        val totals =
            q(
                cat,
                "SELECT record_count, file_count, file_size_bytes FROM table_totals " +
                    "WHERE table_id = (SELECT table_id FROM files WHERE id = ?)",
                output[1].asLong(),
            ).rows.single()
        val aggregate =
            q(
                cat,
                "SELECT SUM(record_count), COUNT(*), SUM(file_size_bytes) FROM files WHERE end_snapshot IS NULL",
            ).rows.single()
        assertThat(totals.map { it.asLong() }).containsExactlyElementsOf(aggregate.map { it.asLong() })
        assertThat(scalar(cat, "SELECT COUNT(*) FROM removals WHERE drained_outcome = 'registered'")).isEqualTo(1)
        assertThat(scalar(cat, "SELECT COUNT(*) FROM changes WHERE kind = 'table_compacted'")).isEqualTo(1)
        assertLedger(cat, result)
        val again = compaction().runOnce(cat, null, MaintenanceTrigger.MANUAL)
        assertThat(again.groupsCompacted).isZero()
    }

    private fun assertLedger(
        cat: String,
        result: com.posthog.hoglake.model.CompactionResult,
    ) {
        val run =
            q(
                cat,
                "SELECT task, run_trigger, status, result_json FROM maintenance_runs ORDER BY id DESC LIMIT 1",
            ).rows.single()
        assertThat(run.take(3).map { it.asString() }).containsExactly("compaction", "manual", "ok")
        assertThat(
            store.json.readTree(run[3].asString())["groups_compacted"].asLong(),
        ).isEqualTo(result.groupsCompacted)
        assertThat(store.json.readTree(run[3].asString())["failed_groups"].asLong()).isEqualTo(result.failedGroups)
    }

    @Test
    fun `fully deleted group retires inputs and vectors without registering an empty output`() {
        val cat = fixture()
        val a = appendReal(cat, "a.parquet", 4)
        val b = appendReal(cat, "b.parquet", 4)
        dv(cat, a, listOf(0, 1))
        dv(cat, b, listOf(0, 1))
        val result = compaction().runOnce(cat, null, MaintenanceTrigger.MANUAL)
        assertThat(result.groupsRetired).isEqualTo(1)
        assertThat(result.groupsCompacted).isZero()
        assertThat(scalar(cat, "SELECT COUNT(*) FROM files WHERE end_snapshot IS NULL")).isZero()
        assertThat(scalar(cat, "SELECT COUNT(*) FROM dvs WHERE end_snapshot IS NULL")).isZero()
        assertThat(scalar(cat, "SELECT SUM(file_count) FROM table_totals")).isZero()
        assertThat(scalar(cat, "SELECT COUNT(*) FROM removals WHERE drained_at IS NULL")).isEqualTo(1)
        assertLedger(cat, result)
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(
        strings = ["claimed", "drained", "attempted", "table", "input", "sort", "dv", "grown_dv"],
    )
    fun `publication guards reject mutations in the rewrite window`(mutation: String) {
        val cat = fixture()
        val a = appendReal(cat, "a.parquet", 4)
        appendReal(cat, "b.parquet", 4)
        if (mutation == "grown_dv") dv(cat, a, listOf(0))
        var guardedHead = 0L
        val result =
            compaction { c, _ ->
                when (mutation) {
                    "claimed" -> rawWrite(c, "UPDATE removals SET claimed_at = '2026-01-01T00:00:00.000Z'")
                    "drained" -> rawWrite(c, "UPDATE removals SET drained_at = '2026-01-01T00:00:00.000Z'")
                    "attempted" -> rawWrite(c, "UPDATE removals SET attempts = 1")
                    "table" -> rawWrite(c, "UPDATE tables SET dropped_snapshot = 999 WHERE name = 'events'")
                    "input" -> rawWrite(c, "UPDATE files SET end_snapshot = 999 WHERE path = '$a'")
                    "sort" ->
                        rawWrite(
                            c,
                            "INSERT INTO sort_specs(table_id, spec_id, fields_json, begin_snapshot) " +
                                "SELECT table_id, 1, '[]', 1 FROM files WHERE path = '$a'",
                        )
                    "dv" -> dv(c, a, listOf(0))
                    "grown_dv" -> dv(c, a, listOf(0, 1))
                }
                guardedHead = store.getCatalog(c).headSnapshotId
            }.runOnce(cat, null, MaintenanceTrigger.MANUAL)
        assertThat(result.groupsCompacted).isZero()
        if (mutation == "dv" || mutation == "grown_dv") {
            assertThat(result.dvSuperseded).isEqualTo(1)
        } else {
            assertThat(result.skippedConflicts).isEqualTo(1)
        }
        assertThat(result.failedGroups).isZero()
        assertThat(store.getCatalog(cat).headSnapshotId).isEqualTo(guardedHead)
        assertThat(scalar(cat, "SELECT COUNT(*) FROM files WHERE explicit_row_ids = 1")).isZero()
        assertThat(scalar(cat, "SELECT COUNT(*) FROM removals WHERE drained_at IS NULL")).isGreaterThanOrEqualTo(1)
    }

    @Test
    fun `overlapping different groups cannot acquire simultaneous claims`() {
        val cat = fixture()
        appendReal(cat, "a.parquet", 4)
        appendReal(cat, "b.parquet", 4)
        val written = CountDownLatch(1)
        val publish = CountDownLatch(1)
        val pool = Executors.newSingleThreadExecutor()
        val first =
            pool.submit<com.posthog.hoglake.model.CompactionResult> {
                compaction { _, _ ->
                    written.countDown()
                    check(publish.await(30, TimeUnit.SECONDS))
                }.runOnce(cat, null, MaintenanceTrigger.MANUAL)
            }
        try {
            check(written.await(30, TimeUnit.SECONDS))
            appendReal(cat, "c.parquet", 4)
            val other =
                HogliteCompaction(store, objects, defaults().copy(maxInputFiles = 3, maxFanIn = 3))
                    .runOnce(cat, null, MaintenanceTrigger.MANUAL)
            assertThat(other.claimedElsewhere).isEqualTo(1)
            assertThat(other.groupsCompacted).isZero()
            assertThat(scalar(cat, "SELECT COUNT(*) FROM removals")).isEqualTo(1)
            publish.countDown()
            assertThat(first.get(30, TimeUnit.SECONDS).groupsCompacted).isEqualTo(1)
            assertThat(scalar(cat, "SELECT COUNT(*) FROM compaction_claims")).isZero()
            assertThat(scalar(cat, "SELECT COUNT(*) FROM files WHERE end_snapshot IS NULL")).isEqualTo(2)
        } finally {
            publish.countDown()
            pool.shutdownNow()
        }
    }

    @Test
    fun `a failed group does not poison later groups or the ledger`() {
        val cat = fixture()
        val bad = appendReal(cat, "a.parquet", 4)
        appendReal(cat, "b.parquet", 4)
        appendReal(cat, "c.parquet", 6)
        appendReal(cat, "d.parquet", 6)
        s3Client().use { s3 -> s3.deleteObject { it.bucket("hoglake-it").key(bad.removePrefix("s3://hoglake-it/")) } }
        val result = compaction().runOnce(cat, null, MaintenanceTrigger.MANUAL)
        assertThat(result.failedGroups).isEqualTo(1)
        assertThat(result.groupsCompacted).isEqualTo(1)
        assertThat(scalar(cat, "SELECT COUNT(*) FROM files WHERE end_snapshot IS NULL")).isEqualTo(3)
        assertLedger(cat, result)
    }

    @Test
    fun `merge and spill refusals happen in metadata without spending the group budget`() {
        for (merge in listOf(false, true)) {
            val cat = fixture()
            appendReal(cat, "a.parquet", 4)
            appendReal(cat, "b.parquet", 4)
            alters.alterTable(
                cat,
                "ingest",
                "events",
                listOf(
                    AlterOp.SetSortOrder(
                        listOf(
                            com.posthog.hoglake.model.SortFieldDef(
                                1,
                                com.posthog.hoglake.model.SortDirection.ASC,
                                com.posthog.hoglake.model.NullOrder.NULLS_LAST,
                            ),
                        ),
                    ),
                ),
            )
            val config =
                if (merge) {
                    defaults().copy(
                        sortedHeapBytes = 1,
                    )
                } else {
                    defaults().copy(spillBytes = 1, sortedHeapBytes = 1024)
                }
            val result = HogliteCompaction(store, objects, config).runOnce(cat, null, MaintenanceTrigger.MANUAL)
            assertThat(result.groupsCompacted + result.failedGroups).isZero()
            if (merge) {
                assertThat(result.mergeBudgetExceeded).isEqualTo(1)
            } else {
                assertThat(result.spillBudgetExceeded).isEqualTo(1)
            }
            assertThat(scalar(cat, "SELECT COUNT(*) FROM removals")).isZero()
        }
    }

    @Test
    fun `interruption after an earlier publication preserves partial counters`() {
        val cat = fixture()
        appendReal(cat, "a.parquet", 4)
        appendReal(cat, "b.parquet", 4)
        appendReal(cat, "c.parquet", 6)
        appendReal(cat, "d.parquet", 6)
        var groups = 0
        try {
            val failure =
                org.assertj.core.api.Assertions.catchThrowable {
                    compaction { _, _ -> if (++groups == 2) throw InterruptedException("cancelled") }
                        .runOnce(cat, null, MaintenanceTrigger.MANUAL)
                }
            assertThat(failure).isInstanceOf(PartialResult::class.java)
            val partial = (failure as PartialResult).partial as com.posthog.hoglake.model.CompactionResult
            assertThat(partial.groupsCompacted).isEqualTo(1)
            assertThat(Thread.currentThread().isInterrupted).isTrue()
        } finally {
            Thread.interrupted()
        }
        assertThat(scalar(cat, "SELECT COUNT(*) FROM files WHERE explicit_row_ids = 1")).isEqualTo(1)
    }

    @Test
    fun `planner crosses one hundred thousand candidates without starving a valid tail group`() {
        val cat = fixture()
        val table = store.getTable(cat, "ingest", "events")
        rawWrite(
            cat,
            """WITH RECURSIVE n(i) AS (VALUES(0) UNION ALL SELECT i + 1 FROM n WHERE i < 100001)
            INSERT INTO files(id, table_id, path, record_count, file_size_bytes, row_id_start,
                              spec_id, partition_values_json, stats_state, begin_snapshot)
            SELECT i + 1000, ${table.tableId}, 's3://hoglake-it/$cat/fake-' || i, 2, 10, i * 2,
                   1, '["04"]', 'provided', 1 FROM n""",
            "UPDATE meta SET next_file_id = 102000 WHERE id = 1",
            "UPDATE tables SET next_row_id = 200004 WHERE id = ${table.tableId}",
            "UPDATE table_totals SET record_count = 200004, file_count = 100002, " +
                "file_size_bytes = 1000020 WHERE table_id = ${table.tableId}",
        )
        appendReal(cat, "tail-a.parquet", 4)
        appendReal(cat, "tail-b.parquet", 4)
        HogliteInterposer(engine.url()).use { proxy ->
            val compactor =
                HogliteCompaction(
                    proxy.store,
                    objects,
                    defaults().copy(maxGroupsPerRun = 25000, maxCandidates = 100000),
                )
            val first = compactor.runOnce(cat, null, MaintenanceTrigger.MANUAL)
            assertThat(first.candidatesFetched).isEqualTo(100000)
            assertThat(first.candidatesTruncated).isEqualTo(1)
            assertThat(first.objectPrefixUnresolvable).isEqualTo(50000)
            assertThat(first.groupsCompacted).isZero()
            val result = compactor.runOnce(cat, null, MaintenanceTrigger.MANUAL)
            assertThat(result.candidatesFetched).isEqualTo(4)
            assertThat(result.objectPrefixUnresolvable).isEqualTo(1)
            assertThat(result.groupsCompacted).isEqualTo(1)
            val statements =
                proxy.requests.filter { it.contains("\"statements\"") }.flatMap {
                    store.json.readTree(it)["statements"].toList()
                }
            val writes =
                statements.filter {
                    val sql = it["sql"].asText()
                    sql.startsWith("SELECT") || sql.contains("UPDATE table_totals") ||
                        sql.contains("UPDATE removals") || sql.contains("UPDATE files") || sql.contains("UPDATE dvs") ||
                        sql.startsWith("DELETE FROM compaction_claims") ||
                        sql.startsWith("INSERT INTO compaction_claims")
                }
            assertThat(writes).isNotEmpty()
            for (statement in writes) {
                val sql = statement["sql"].asText()
                val args = statement["args"]?.map { store.json.convertValue(it, Any::class.java) } ?: emptyList()
                val plan =
                    explain(
                        cat,
                        sql,
                        *args.toTypedArray(),
                    ).rows.joinToString { it.last().asString() }
                assertThat(plan).describedAs(sql).doesNotContain(
                    "SCAN files",
                    "SCAN dvs",
                    "SCAN removals",
                    "SCAN c ",
                    "files_compaction",
                    "files_table",
                    "files_scan",
                    "files_listing",
                )
            }
        }
    }

    @Test
    fun `busy input rolls back all reservations without a release write`() {
        val cat = fixture()
        appendReal(cat, "a.parquet", 4)
        val b = appendReal(cat, "b.parquet", 4)
        val tableId = store.getTable(cat, "ingest", "events").tableId
        val id = fileId(cat, b)
        rawWrite(
            cat,
            "INSERT INTO compaction_claims VALUES ($tableId, 'file:$id', '[$id]', 'sibling', " +
                "'2000-01-01', '2999-01-01')",
        )
        HogliteInterposer(engine.url()).use { proxy ->
            val result =
                HogliteCompaction(
                    proxy.store,
                    objects,
                    defaults(),
                ).runOnce(cat, null, MaintenanceTrigger.MANUAL)
            assertThat(result.claimedElsewhere).isEqualTo(1)
            assertThat(result.groupsCompacted).isZero()
            assertThat(scalar(cat, "SELECT COUNT(*) FROM compaction_claims")).isEqualTo(1)
            assertThat(proxy.requests.filter { it.contains("compact-release") }).isEmpty()
            assertThat(proxy.requests.filter { it.contains("compact-refresh") }).isEmpty()
        }
    }

    @Test
    fun `expired reservations are purged in bounded primary key batches and rechecked against renewal`() {
        val cat = fixture()
        rawWrite(
            cat,
            """WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i + 1 FROM n WHERE i < 100001)
            INSERT INTO compaction_claims SELECT 999, 'file:' || printf('%06d', i), '[' || i || ']',
                'dead', '2000-01-01', '2001-01-01' FROM n""",
        )
        HogliteInterposer(engine.url()).use { proxy ->
            val hook =
                proxy.hook({ m, p, b -> HogliteInterposer.isWriteTx(m, p, b) && b.contains("compact-purge") }) {
                    rawWrite(
                        cat,
                        "UPDATE compaction_claims SET expires_at = '2999-01-01' " +
                            "WHERE table_id = 999 AND group_key = 'file:000001'",
                    )
                }
            val result =
                HogliteCompaction(
                    proxy.store,
                    objects,
                    defaults(),
                ).runOnce(cat, null, MaintenanceTrigger.MANUAL)
            assertThat(hook.fired).isTrue()
            assertThat(result.groupsCompacted + result.failedGroups).isZero()
            assertThat(scalar(cat, "SELECT COUNT(*) FROM compaction_claims")).isEqualTo(99002)
            assertThat(
                scalar(cat, "SELECT COUNT(*) FROM compaction_claims WHERE group_key = 'file:000001'"),
            ).isEqualTo(1)
            val plan =
                explain(
                    cat,
                    "DELETE FROM compaction_claims WHERE table_id = ? AND group_key = ? " +
                        "AND expires_at <= ?",
                    999,
                    "file:000002",
                    HogliteSchema.now(),
                ).rows.joinToString { it.last().asString() }
            assertThat(plan).contains("SEARCH compaction_claims").doesNotContain("SCAN compaction_claims")
        }
    }

    @Test
    fun `release and ledger failures cannot replace a committed group result`() {
        val cat = fixture()
        appendReal(cat, "a.parquet", 4)
        appendReal(cat, "b.parquet", 4)
        HogliteInterposer(engine.url()).use { proxy ->
            val release = proxy.hook({ _, _, body -> body.contains("compact-release") }) { error("release failed") }
            val ledger =
                proxy.hook(
                    { _, _, body -> body.contains("INSERT INTO maintenance_runs") },
                ) { error("ledger failed") }
            val result =
                HogliteCompaction(
                    proxy.store,
                    objects,
                    defaults(),
                ).runOnce(cat, null, MaintenanceTrigger.MANUAL)
            assertThat(release.fired && ledger.fired).isTrue()
            assertThat(result.groupsCompacted).isEqualTo(1)
            assertThat(result.failedGroups).isZero()
            assertThat(scalar(cat, "SELECT COUNT(*) FROM files WHERE end_snapshot IS NULL")).isEqualTo(1)
        }
    }

    @Test
    fun `one table with corrupt planning metadata cannot poison another table`() {
        val cat = fixture()
        appendReal(cat, "a.parquet", 4)
        appendReal(cat, "b.parquet", 4)
        rawWrite(
            cat,
            "INSERT INTO sort_specs(table_id, spec_id, fields_json, begin_snapshot) " +
                "SELECT id, 1, 'not-json', 1 FROM tables WHERE name = 'events'",
        )
        for (name in listOf("a", "b")) {
            val (path, size) = putParquet(cat, root(cat, "plain") + "$name.parquet", 4)
            append(cat, "plain", file(path, "4", stats(le8(4), le8(4)), rows = 2, size = size, footer = null))
        }
        val result = compaction().runOnce(cat, null, MaintenanceTrigger.MANUAL)
        assertThat(result.failedGroups).isEqualTo(1)
        assertThat(result.groupsCompacted).isEqualTo(1)
        assertLedger(cat, result)
    }

    @Test
    fun `nested variants are refused before candidate reads when disabled or without a derivable layout`() {
        for (enabled in listOf(false, true)) {
            val cat = fixture()
            appendReal(cat, "a.parquet", 4)
            appendReal(cat, "b.parquet", 4)
            alters.alterTable(
                cat,
                "ingest",
                "events",
                listOf(
                    AlterOp.AddColumn(
                        ColumnDef("s", ColType.STRUCT, children = listOf(ColumnDef("v", ColType.VARIANT))),
                    ),
                ),
            )
            if (enabled) {
                rawWrite(
                    cat,
                    "UPDATE columns SET type_params_json = '{\"shredding\":{\"type\":\"not-a-type\"}}' " +
                        "WHERE name = 'v'",
                )
            }
            HogliteInterposer(engine.url()).use { proxy ->
                val result =
                    HogliteCompaction(proxy.store, objects, defaults().copy(variantCompaction = enabled))
                        .runOnce(cat, null, MaintenanceTrigger.MANUAL)
                assertThat(result.groupsCompacted + result.failedGroups + result.unconvertibleSchema).isZero()
                assertThat(result.candidatesFetched).isZero()
                assertThat(
                    proxy.requests.filter {
                        it.contains("SELECT f.id, f.path") && store.json.readTree(it)["args"][0].asLong() == 1L
                    },
                ).isEmpty()
            }
        }
    }

    @Test
    fun `consumer offsets and pinned windows retain original appends but never emit compaction outputs`() {
        val cat = fixture()
        val table = store.getTable(cat, "ingest", "events")
        val start = store.getCatalog(cat).headSnapshotId
        store.commitOffset(cat, "replica", table.tableUuid, start)
        val paths = listOf(appendReal(cat, "a.parquet", 4), appendReal(cat, "b.parquet", 4))
        val first = store.changes(cat, "ingest", "events", start)
        assertThat(first.files.map { it.path }).containsExactlyElementsOf(paths)
        store.commitOffset(cat, "replica", table.tableUuid, first.toSnapshot)
        assertThat(compaction().runOnce(cat, null, MaintenanceTrigger.MANUAL).groupsCompacted).isEqualTo(1)
        val head = store.getCatalog(cat).headSnapshotId
        val output = store.listFiles(cat, "ingest", "events").single()
        assertThat(output.path).isNotIn(paths)
        assertThat(output.statsState).isEqualTo(com.posthog.hoglake.model.StatsState.PROVIDED)
        assertThat(output.explicitRowIds).isTrue()
        assertThat(store.listFiles(cat, "ingest", "events", snapshot = first.toSnapshot)).hasSize(2)
        assertThat(store.getTable(cat, "ingest", "events").fileCount).isEqualTo(1)
        assertThat(store.getTable(cat, "ingest", "events", snapshot = first.toSnapshot).fileCount).isEqualTo(2)
        val rewrite = store.changes(cat, "ingest", "events", first.toSnapshot, head)
        assertThat(rewrite.files).isEmpty()
        store.commitOffset(cat, "replica", table.tableUuid, rewrite.toSnapshot)
        val tail = appendReal(cat, "c.parquet", 4)
        val next =
            store.changes(
                cat,
                "ingest",
                "events",
                store.getOffset(cat, "replica", table.tableUuid).committedSnapshot,
            )
        assertThat(next.files.map { it.path }).containsExactly(tail)
        assertThat(store.changes(cat, "ingest", "events", start).files.map { it.path })
            .containsExactlyElementsOf(paths + tail)
        assertThat(store.changes(cat, "ingest", "events", start, first.toSnapshot).files.map { it.path })
            .containsExactlyElementsOf(paths)
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = ["retired", "output", "overcount"])
    fun `ordered deletion validation remains atomic after a real compaction`(kind: String) {
        for (durableFirst in listOf(true, false)) {
            val cat = HogliteConformanceEngine.uniqueName("ordered")
            store.createCatalog(cat, "s3://hoglake-it/$cat")
            store.createNamespace(cat, "ns")
            val table = store.createTable(cat, "ns", "t", columns)
            for (name in listOf("a", "b")) {
                val (path, size) = putParquet(cat, "s3://hoglake-it/$cat/$name.parquet", 4)
                store.commit(
                    cat,
                    CommitRequest(appends = listOf(TableAppend("ns", "t", listOf(FileRegistration(path, 2, size))))),
                )
            }
            val read = store.getCatalog(cat).headSnapshotId
            val inputs = store.listFiles(cat, "ns", "t")
            val result = compaction().runOnce(cat, null, MaintenanceTrigger.MANUAL)
            assertThat(result.groupsCompacted).isEqualTo(1)
            val target = if (kind == "retired") inputs.first() else store.listFiles(cat, "ns", "t").single()
            val count = if (kind == "overcount") target.recordCount + 1 else 1L
            val path = "s3://hoglake-it/$cat/ambiguous.parquet"
            val durable = DeleteFileRegistration(target.dataFileId, "$path.durable.dv", count, 10)
            val pending = DeleteFileRegistration(0, "$path.pending.dv", 1, 10, dataFilePath = path)
            val registrations = if (durableFirst) listOf(durable, pending) else listOf(pending, durable)
            val file = FileRegistration(path, 2, 100, 50)
            val request =
                CommitRequest(
                    idempotencyKey = UUID.randomUUID(),
                    readSnapshot = if (kind == "overcount") store.getCatalog(cat).headSnapshotId else read,
                    requireUnchangedTables = true,
                    allowPendingDeletes = true,
                    appends = listOf(TableAppend("ns", "t", listOf(file, file), table.tableUuid)),
                    deletes = registrations.map { TableDeletes("ns", "t", listOf(it), table.tableUuid) },
                )
            val before = store.getCatalog(cat).headSnapshotId
            val filesBefore = store.listFiles(cat, "ns", "t")
            val failure = org.assertj.core.api.Assertions.catchThrowable { store.commit(cat, request) }
            if (!durableFirst) {
                assertThat(failure).isInstanceOf(com.posthog.hoglake.model.HoglakeException.Validation::class.java)
                    .hasMessageContaining("pending delete must target exactly one")
            } else if (kind == "overcount") {
                assertThat(failure).isInstanceOf(com.posthog.hoglake.model.HoglakeException.Validation::class.java)
                    .hasMessageContaining("delete_count $count exceeds record_count")
            } else {
                assertThat(failure).isInstanceOf(com.posthog.hoglake.model.HoglakeException.CommitConflict::class.java)
                    .hasMessageContaining("data_file_id ${target.dataFileId}")
            }
            assertThat(store.getCatalog(cat).headSnapshotId).isEqualTo(before)
            assertThat(store.listFiles(cat, "ns", "t")).isEqualTo(filesBefore)
        }
    }

    @Test
    fun `planning excludes a post DDL append from a pre DDL column projection`() {
        val cat = fixture()
        appendReal(cat, "a.parquet", 4)
        appendReal(cat, "b.parquet", 4)
        var later = ""
        HogliteInterposer(engine.url()).use { proxy ->
            val hook =
                proxy.hook({ _, _, body -> body.contains("SELECT f.id, f.path") }) {
                    alters.alterTable(
                        cat,
                        "ingest",
                        "events",
                        listOf(AlterOp.AddColumn(ColumnDef("extra", ColType.LONG))),
                    )
                    later = appendReal(cat, "post-ddl.parquet", 4)
                }
            val result =
                HogliteCompaction(
                    proxy.store,
                    objects,
                    defaults().copy(maxInputFiles = 3, maxFanIn = 3),
                ).runOnce(cat, null, MaintenanceTrigger.MANUAL)
            assertThat(hook.fired).isTrue()
            assertThat(result.candidatesFetched).isEqualTo(2)
            assertThat(result.groupsCompacted).isZero()
            assertThat(result.skippedConflicts).isEqualTo(1)
            assertThat(result.filesIn).isZero()
            assertThat(store.listFiles(cat, "ingest", "events").map { it.path }).contains(later).hasSize(3)
        }
    }

    @Test
    fun `schema fence rejects a type promotion after the rewrite produced old width stats`() {
        val cat = HogliteConformanceEngine.uniqueName("promote")
        store.createCatalog(cat, "s3://hoglake-it/$cat")
        store.createNamespace(cat, "ns")
        store.createTable(cat, "ns", "t", listOf(columns[0], ColumnDef("v", ColType.INT)))
        for (name in listOf("a", "b")) {
            val (path, size) = putParquet(cat, "s3://hoglake-it/$cat/$name.parquet", 4, vInt = true)
            store.commit(
                cat,
                CommitRequest(appends = listOf(TableAppend("ns", "t", listOf(FileRegistration(path, 2, size))))),
            )
        }
        val result =
            compaction { _, _ ->
                alters.alterTable(cat, "ns", "t", listOf(AlterOp.PromoteColumn("v", ColType.LONG)))
            }.runOnce(cat, null, MaintenanceTrigger.MANUAL)
        assertThat(result.skippedConflicts).isEqualTo(1)
        assertThat(result.groupsCompacted).isZero()
        assertThat(result.failedGroups).isZero()
        assertThat(store.listFiles(cat, "ns", "t")).hasSize(2)
        assertThat(scalar(cat, "SELECT COUNT(*) FROM files WHERE explicit_row_ids = 1")).isZero()
        assertThat(compaction().runOnce(cat, null, MaintenanceTrigger.MANUAL).groupsCompacted).isEqualTo(1)
        val output = store.listFiles(cat, "ns", "t").single()
        assertThat(
            store.fileStats(cat, "ns", "t", output.dataFileId).columns.single { it.fieldId == 2L }.stats.lowerBound,
        )
            .hasSize(8)
    }

    @Test
    fun `sort definition is pinned to the same head as the column projection`() {
        val cat = fixture()
        appendReal(cat, "a.parquet", 4)
        appendReal(cat, "b.parquet", 4)
        HogliteInterposer(engine.url()).use { proxy ->
            val hook =
                proxy.hook({ _, _, body -> body.contains("FROM sort_specs WHERE") }) {
                    alters.alterTable(
                        cat,
                        "ingest",
                        "events",
                        listOf(
                            AlterOp.AddColumn(ColumnDef("extra", ColType.LONG)),
                            AlterOp.SetSortOrder(
                                listOf(
                                    com.posthog.hoglake.model.SortFieldDef(
                                        3,
                                        com.posthog.hoglake.model.SortDirection.ASC,
                                        com.posthog.hoglake.model.NullOrder.NULLS_LAST,
                                    ),
                                ),
                            ),
                        ),
                    )
                }
            val result =
                HogliteCompaction(
                    proxy.store,
                    objects,
                    defaults(),
                ).runOnce(cat, null, MaintenanceTrigger.MANUAL)
            assertThat(hook.fired).isTrue()
            assertThat(result.skippedConflicts).isEqualTo(1)
            assertThat(result.failedGroups).isZero()
            assertThat(scalar(cat, "SELECT COUNT(*) FROM removals WHERE reason = 'compaction_staging'")).isEqualTo(1)
        }
    }

    @Test
    fun `planning head advances between tables in one manual call`() {
        val cat = fixture()
        appendReal(cat, "a.parquet", 4)
        appendReal(cat, "b.parquet", 4)
        var calls = 0
        val result =
            compaction { _, _ ->
                if (++calls == 1) {
                    appendPlain(cat, "later-a.parquet")
                    appendPlain(cat, "later-b.parquet")
                }
            }.runOnce(cat, null, MaintenanceTrigger.MANUAL)
        assertThat(result.groupsCompacted).isEqualTo(2)
        assertThat(calls).isEqualTo(2)
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = [true, false])
    fun `planner uses one bounded fallback read when the size index is absent`(indexed: Boolean) {
        val cat = fixture()
        appendReal(cat, "a.parquet", 4)
        appendReal(cat, "b.parquet", 4)
        if (!indexed) rawWrite(cat, "DROP INDEX files_compaction")
        val config = defaults().copy(maxGroupsPerRun = 1000)
        HogliteInterposer(engine.url()).use { proxy ->
            val result = HogliteCompaction(proxy.store, objects, config).runOnce(cat, null, MaintenanceTrigger.MANUAL)
            assertThat(result.groupsCompacted).isEqualTo(1)
            val reads =
                proxy.requests.filter { it.contains("SELECT f.id, f.path") }
                    .map { store.json.readTree(it) }.filter { it["args"][0].asLong() == 1L }
            assertThat(reads).hasSize(1)
            val statement = reads.single()
            val args = statement["args"].map { store.json.convertValue(it, Any::class.java) }
            assertThat(args.last()).isEqualTo(if (indexed) HogliteCompaction.PAGE_SIZE else config.candidateBudget)
            val plan =
                explain(cat, statement["sql"].asText(), *args.toTypedArray())
                    .rows.joinToString { it.last().asString() }
            if (indexed) {
                assertThat(plan).contains("files_compaction").doesNotContain("TEMP B-TREE")
            } else {
                assertThat(plan).doesNotContain("files_compaction").contains("TEMP B-TREE")
            }
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = ["missing", "unavailable", "unknown"])
    fun `terminal group errors stop the call and retain completed group counts`(kind: String) {
        val cat = fixture()
        for (team in listOf(4L, 6L, 8L)) {
            appendReal(cat, "$team-a.parquet", team)
            appendReal(cat, "$team-b.parquet", team)
        }
        val terminal =
            when (kind) {
                "missing" -> com.posthog.hoglake.model.HoglakeException.NotFound("catalog dropped")
                "unavailable" -> com.posthog.hoglake.model.HoglakeException.BackendUnavailable("tenant unavailable")
                else -> com.posthog.hoglake.model.HoglakeException.CommitOutcomeUnknown("test-tx", "answer lost")
            }
        var calls = 0
        val error =
            org.assertj.core.api.Assertions.catchThrowable {
                compaction { _, _ -> if (++calls == 2) throw terminal }
                    .runOnce(cat, null, MaintenanceTrigger.MANUAL)
            }
        assertThat(error).isInstanceOf(PartialResult::class.java)
        assertThat(error.cause).isSameAs(terminal)
        val partial = (error as PartialResult).partial as com.posthog.hoglake.model.CompactionResult
        assertThat(partial.groupsCompacted).isEqualTo(1)
        assertThat(partial.failedGroups).isZero()
        assertThat(calls).isEqualTo(2)
        assertThat(store.listFiles(cat, "ingest", "events")).hasSize(5)
        assertThat(q(cat, "SELECT status FROM maintenance_runs ORDER BY id DESC LIMIT 1").firstRow()!![0].asString())
            .isEqualTo("failed")
    }

    @Test
    fun `claim purge failure does not block a valid compaction`() {
        val cat = fixture()
        appendReal(cat, "a.parquet", 4)
        appendReal(cat, "b.parquet", 4)
        HogliteInterposer(engine.url()).use { proxy ->
            val hook =
                proxy.hook({ _, _, body -> body.contains(HogliteCompaction.EXPIRED_CLAIMS_SQL) }) {
                    error("purge unavailable")
                }
            val result =
                HogliteCompaction(
                    proxy.store,
                    objects,
                    defaults(),
                ).runOnce(cat, null, MaintenanceTrigger.MANUAL)
            assertThat(hook.fired).isTrue()
            assertThat(result.groupsCompacted).isEqualTo(1)
            assertLedger(cat, result)
        }
    }

    private fun appendPlain(
        cat: String,
        name: String,
    ): String {
        val (path, size) = putParquet(cat, "s3://hoglake-it/$cat/plain/$name", 4)
        store.commit(
            cat,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "ingest",
                            "plain",
                            listOf(
                                FileRegistration(path, 2, size, partitionValues = listOf("4")),
                            ),
                        ),
                    ),
            ),
        )
        return path
    }

    private val parquetSchema =
        org.apache.parquet.schema.Types
            .buildMessage()
            .addField(
                org.apache.parquet.schema.Types
                    .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64)
                    .id(1)
                    .named("team_id"),
            ).addField(
                org.apache.parquet.schema.Types
                    .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64)
                    .id(2)
                    .named("v"),
            ).named("t")

    private fun putParquet(
        cat: String,
        path: String,
        team: Long,
        vInt: Boolean = false,
    ): Pair<String, Long> {
        check(path.startsWith("s3://hoglake-it/$cat/"))
        val schema =
            if (vInt) {
                org.apache.parquet.schema.MessageTypeParser.parseMessageType(
                    "message t { required int64 team_id = 1; required int32 v = 2; }",
                )
            } else {
                parquetSchema
            }
        val tmp = Files.createTempFile("hoglite-prefix", ".parquet")
        val bytes =
            try {
                Files.delete(tmp)
                val factory = org.apache.parquet.example.data.simple.SimpleGroupFactory(schema)
                org.apache.parquet.hadoop.example.ExampleParquetWriter
                    .builder(org.apache.parquet.io.LocalOutputFile(tmp))
                    .withType(schema)
                    .withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED)
                    .build()
                    .use { w ->
                        for (v in listOf(1L, 2L)) {
                            val g = factory.newGroup()
                            g.add("team_id", team)
                            if (vInt) g.add("v", v.toInt()) else g.add("v", v)
                            w.write(g)
                        }
                    }
                Files.readAllBytes(tmp)
            } finally {
                Files.deleteIfExists(tmp)
            }
        s3Client().use { s3 ->
            s3.putObject(
                { it.bucket("hoglake-it").key(path.removePrefix("s3://hoglake-it/")) },
                RequestBody.fromBytes(bytes),
            )
        }
        return path to bytes.size.toLong()
    }
}
