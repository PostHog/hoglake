package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.hydrator.Hydrator
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceRunStatus
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.storage.StorageAccess
import com.posthog.hoglake.storage.StorageCredentialException
import com.posthog.hoglake.storage.StorageCredentialProvider
import com.posthog.hoglake.storage.StorageCredentialProviders
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.io.TempDir
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.model.S3Exception
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.nio.file.Path
import java.security.interfaces.ECPublicKey
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

/**
 * Requeue failed files through the live engine. With an object store the requeue then hydrates pending
 * files from their footers (#675); the store's own instance has none and only requeues.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteHydratorMaintenanceIntegrationTest {
    private val engine by lazy { HogliteConformanceEngine() }
    private val store get() = engine.store

    private val objects by lazy { ObjectStore(engine.config()) }

    @AfterAll
    fun close() {
        objects.close()
        engine.close()
    }

    private fun catalog(): String {
        val c = HogliteConformanceEngine.uniqueName("rehydrate")
        store.createCatalog(c, "s3://hoglake-it/$c")
        store.createNamespace(c, "ns")
        for (name in listOf("a", "b", "dropped")) {
            store.createTable(c, "ns", name, listOf(ColumnDef("id", ColType.LONG)))
            store.commit(
                c,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                name,
                                (1..3).map { FileRegistration("s3://hoglake-it/$c/$name$it", 1, 100) },
                            ),
                        ),
                ),
            )
        }
        write(c, "UPDATE files SET stats_state = 'failed'")
        write(c, "UPDATE files SET end_snapshot = begin_snapshot + 1 WHERE id = 2")
        write(c, "UPDATE files SET stats_state = 'provided' WHERE id = 3")
        val dropped = store.getTable(c, "ns", "dropped")
        store.dropTable(c, "ns", "dropped", dropped.tableUuid, "test", "drop")
        return c
    }

    private fun write(
        c: String,
        sql: String,
    ) = store.catalogOperation(c) {
        store.runTx(c, HogliteCatalogStore.freshTxid("fixture"), null, HogliteTx().apply { add(sql) })
    }

    private fun states(c: String) =
        store.catalogOperation(c) {
            store.queryCatalog(c, "SELECT stats_state FROM files ORDER BY id").rows.map { it[0].asString() }
        }

    @Test
    fun `table and catalog requeue exclude ended provided and dropped files and count real changes`() {
        val c = catalog()
        assertThat(store.hydratorMaintenance.rehydrateFailed(c, "ns", "a").requeued).isEqualTo(1)
        assertThat(
            states(c),
        ).containsExactly("pending", "failed", "provided", "failed", "failed", "failed", "failed", "failed", "failed")
        assertThat(store.hydratorMaintenance.rehydrateFailed(c).requeued).isEqualTo(3)
        assertThat(store.hydratorMaintenance.rehydrateFailed(c).requeued).isZero()
        assertThat(
            states(c),
        ).containsExactly(
            "pending", "failed", "provided", "pending", "pending", "pending", "failed", "failed", "failed",
        )
        val runs =
            store.catalogOperation(c) {
                store.queryCatalog(
                    c,
                    "SELECT task, run_trigger, status, result_json FROM maintenance_runs ORDER BY id",
                ).rows
            }
        assertThat(runs).hasSize(3)
        assertThat(runs.map { it.take(3) }).allSatisfy { assertThat(it).containsExactly("hydrator", "manual", "ok") }
        assertThat(runs.map { store.json.readTree(it[3].asString())["requeued"].asLong() }).containsExactly(1L, 3L, 0L)
    }

    @Test
    fun `requeue validates scope and requires a live table and catalog`() {
        val c = catalog()
        for ((ns, table) in listOf("ns" to null, null to "a")) {
            assertThatThrownBy { store.hydratorMaintenance.rehydrateFailed(c, ns, table) }
                .isInstanceOf(HoglakeException.Validation::class.java)
        }
        for ((ns, table) in listOf("ns" to "dropped", "missing" to "a", "ns" to "missing")) {
            assertThatThrownBy { store.hydratorMaintenance.rehydrateFailed(c, ns, table) }
                .isInstanceOf(HoglakeException.NotFound::class.java)
        }
        assertThatThrownBy { store.hydratorMaintenance.rehydrateFailed("missing") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }

    @Test
    fun `table dropped between lookup and requeue stays failed`() {
        val c = catalog()
        HogliteInterposer(engine.url()).use { proxy ->
            val hook =
                proxy.hook({ m, p, b -> HogliteInterposer.isWriteTx(m, p, b) && "UPDATE files SET stats_state" in b }) {
                    val t = store.getTable(c, "ns", "a")
                    store.dropTable(c, "ns", "a", t.tableUuid, "test", "race")
                }
            assertThat(proxy.store.hydratorMaintenance.rehydrateFailed(c, "ns", "a").requeued).isZero()
            assertThat(hook.fired).isTrue()
            assertThat(states(c).first()).isEqualTo("failed")
        }
    }

    private fun le8(v: Long): ByteArray = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(v).array()

    /** A parquet file of [ids] (and v = 0) in [schema], by default int64 columns with field ids 1 and 2. */
    private fun parquet(
        ids: List<Long>,
        schema: String = WITH_IDS,
    ): ByteArray {
        val type = org.apache.parquet.schema.MessageTypeParser.parseMessageType(schema)
        val intId = "int32 id" in schema
        val tmp = Files.createTempFile(dir, "hydrate", ".parquet")
        Files.delete(tmp)
        val factory = org.apache.parquet.example.data.simple.SimpleGroupFactory(type)
        org.apache.parquet.hadoop.example.ExampleParquetWriter
            .builder(org.apache.parquet.io.LocalOutputFile(tmp))
            .withType(type)
            .build()
            .use { w ->
                ids.forEach {
                    val row = factory.newGroup()
                    if (intId) row.append("id", it.toInt()) else row.append("id", it)
                    w.write(row.append("v", 0L))
                }
            }
        return Files.readAllBytes(tmp)
    }

    /** [parquet] stored at [path]; returns its size. */
    private fun putParquet(
        path: String,
        ids: List<Long>,
        schema: String = WITH_IDS,
    ): Long {
        val bytes = parquet(ids, schema)
        engine.s3Client().use { s3 ->
            s3.putObject(
                { it.bucket(HogliteConformanceEngine.DATA_BUCKET).key(path.removePrefix("s3://hoglake-it/")) },
                RequestBody.fromBytes(bytes),
            )
        }
        return bytes.size.toLong()
    }

    private val idv = listOf(ColumnDef("id", ColType.LONG), ColumnDef("v", ColType.LONG))

    /** A catalog with table ns.t (id, v); files appended without stats register `pending`. */
    private fun pendingCatalog(): String {
        val c = HogliteConformanceEngine.uniqueName("hydrate")
        store.createCatalog(c, "s3://hoglake-it/$c")
        store.createNamespace(c, "ns")
        store.createTable(c, "ns", "t", idv)
        return c
    }

    private fun append(
        c: String,
        table: String,
        vararg files: FileRegistration,
    ) = store.commit(c, CommitRequest(appends = listOf(TableAppend("ns", table, files.toList()))))

    private fun real(
        c: String,
        name: String,
        ids: List<Long>,
        rows: Long = ids.size.toLong(),
    ): FileRegistration {
        val path = "s3://hoglake-it/$c/data/ns/t/$name.parquet"
        return FileRegistration(path, rows, putParquet(path, ids))
    }

    private fun hydrator(
        minFileBytes: Long = 0,
        on: HogliteCatalogStore = store,
        reads: ObjectStore = objects,
        sweepLimit: Int = Hydrator.DEFAULT_SWEEP_LIMIT,
        pageRows: Int = 100,
    ) = HogliteHydrator(
        on,
        { reads },
        minFileBytes = minFileBytes,
        sweepLimit = sweepLimit,
        dataPathTemplate = TEMPLATE,
        pageRows = pageRows,
    )

    /**
     * A store over MinIO under a scoped provider standing in for `broker`: it records each session asked
     * for (catalog, prefix), and refuses one whose prefix is not the catalog's placement, as hogtower's
     * broker does (docs/auth.md). Lazy like [com.posthog.hoglake.storage.BrokerProvider]: the broker is
     * asked when a request resolves the credentials, and after [vends] resolutions it refuses them all.
     */
    private fun brokered(
        asked: MutableList<Pair<String, String>>,
        vends: Int = Int.MAX_VALUE,
    ): ObjectStore {
        val minio = StorageCredentialProviders.staticOrAmbient(engine.config().s3AccessKey, engine.config().s3SecretKey)
        val left = AtomicInteger(vends)
        val broker =
            object : StorageCredentialProvider by minio {
                override val scoped = true

                override fun credentialsFor(
                    catalog: String,
                    prefix: String,
                    access: StorageAccess,
                ): AwsCredentialsProvider {
                    asked += catalog to prefix
                    return AwsCredentialsProvider {
                        if (prefix != "s3://hoglake-it/$catalog") {
                            throw StorageCredentialException("broker answered 403 prefix_outside_catalog")
                        }
                        if (left.getAndDecrement() <= 0) throw StorageCredentialException("broker answered 503")
                        minio.unscoped().resolveCredentials()
                    }
                }
            }
        val cfg = engine.config()
        return ObjectStore(cfg.s3Endpoint, cfg.s3Region, cfg.s3PathStyle, broker)
    }

    private fun fileRow(
        c: String,
        id: Long,
    ) = store.catalogOperation(c) {
        store.queryCatalog(
            c,
            "SELECT stats_state, stats_json, split_offsets_json, missing_field_ids, prefix_quarantined " +
                "FROM files WHERE id = ?",
            id,
        ).firstRow()!!
    }

    private fun stats(row: List<Any?>): List<ColumnStats> = store.json.readValue(row[1].asString())

    private fun lastRun(c: String) =
        store.catalogOperation(c) {
            store.queryCatalog(
                c,
                "SELECT task, run_trigger, status, result_json FROM maintenance_runs ORDER BY id DESC LIMIT 1",
            ).firstRow()!!
        }

    @Test
    fun `requeue then hydrates pending files from their footers and records the sweep`() {
        val c = pendingCatalog()
        append(
            c,
            "t",
            real(c, "good", listOf(3, 1, 2)),
            FileRegistration("s3://hoglake-it/$c/data/ns/t/absent.parquet", 1, 100),
            real(c, "short", listOf(1, 2), rows = 5),
        )
        // The store's own instance has no object store: it only requeues, as before.
        assertThat(store.hydratorMaintenance.rehydrateFailed(c).requeued).isZero()
        assertThat((1L..3L).map { fileRow(c, it)[0] }).containsOnly("pending")

        assertThat(hydrator().rehydrateFailed(c).requeued).isZero()
        val good = fileRow(c, 1)
        assertThat(good[0]).isEqualTo("provided")
        val byField = stats(good).associateBy { it.fieldId }
        assertThat(byField.keys).containsExactlyInAnyOrder(1L, 2L)
        assertThat(byField.getValue(1).lowerBound).isEqualTo(le8(1))
        assertThat(byField.getValue(1).upperBound).isEqualTo(le8(3))
        assertThat(byField.getValue(1).valueCount).isEqualTo(3)
        assertThat(good[2]).isNotNull()
        assertThat(good[3].asLong()).isZero()
        assertThat(fileRow(c, 2)[0]).isEqualTo("failed")
        assertThat(fileRow(c, 3)[0]).isEqualTo("failed")
        val run = lastRun(c)
        assertThat(run.take(3)).containsExactly("hydrator", "manual", "ok")
        val result = store.json.readTree(run[3].asString())
        assertThat(listOf("claimed", "hydrated", "failed", "transient").map { result[it].asLong() })
            .containsExactly(3L, 1L, 2L, 0L)

        // The scan sees the stats the sweep wrote.
        val scanned =
            store.planScan(c, "ns", "t", columnStats = ScanService.ColumnStatsRequest(setOf(1)))
                .single { it.dataFile.path.endsWith("good.parquet") }
        assertThat(scanned.dataFile.columnStats!!.single().stats.upperBound).isEqualTo(le8(3))
    }

    @Test
    fun `a named-table requeue hydrates only that table`() {
        val c = pendingCatalog()
        store.createTable(c, "ns", "u", idv)
        append(c, "t", real(c, "t1", listOf(1)))
        append(c, "u", FileRegistration("s3://hoglake-it/$c/data/ns/u/u1.parquet", 1, 100))
        hydrator().rehydrateFailed(c, "ns", "t")
        assertThat(fileRow(c, 1)[0]).isEqualTo("provided")
        assertThat(fileRow(c, 2)[0]).isEqualTo("pending")
    }

    @Test
    fun `a dropped table's pending files are not claimed`() {
        val c = pendingCatalog()
        append(c, "t", real(c, "f", listOf(1)))
        store.dropTable(c, "ns", "t", store.getTable(c, "ns", "t").tableUuid, "test", "drop")
        assertThat(hydrator().hydratePending(c, null).claimed).isZero()
        assertThat(fileRow(c, 1)[0]).isEqualTo("pending")
    }

    @Test
    fun `an alter between the footer read and the flip leaves the file pending for the next run`() {
        val c = pendingCatalog()
        append(c, "t", real(c, "f", listOf(1, 2)))
        HogliteInterposer(engine.url()).use { proxy ->
            val hook =
                proxy.hook({ m, p, b -> HogliteInterposer.isWriteTx(m, p, b) && "stats_state = 'provided'" in b }) {
                    store.alters.alterTable(
                        c,
                        "ns",
                        "t",
                        listOf(AlterOp.AddColumn(ColumnDef("w", ColType.LONG))),
                        null,
                        null,
                        "test",
                        "race",
                    )
                }
            val result = hydrator(on = proxy.store).hydratePending(c, null)
            assertThat(hook.fired).isTrue()
            // A lost fence is claimed only: no object-store failure happened.
            assertThat(listOf(result.claimed, result.hydrated, result.failed, result.transient))
                .containsExactly(1L, 0L, 0L, 0L)
            assertThat(fileRow(c, 1)[0]).isEqualTo("pending")
        }
        assertThat(hydrator().hydratePending(c, null).hydrated).isEqualTo(1)
        assertThat(fileRow(c, 1)[0]).isEqualTo("provided")
    }

    @Test
    fun `a file under the size floor is provided without stats`() {
        val c = pendingCatalog()
        append(c, "t", real(c, "small", listOf(1)))
        val result = hydrator(minFileBytes = Long.MAX_VALUE).hydratePending(c, null)
        assertThat(listOf(result.hydrated, result.statsSkippedBelowFloor)).containsExactly(1L, 1L)
        val row = fileRow(c, 1)
        assertThat(row[0]).isEqualTo("provided")
        assertThat(stats(row)).isEmpty()
        assertThat(row[2]).isNotNull()
    }

    @Test
    fun `an object-prefix file its footer does not prove is quarantined`() {
        val c = pendingCatalog()
        store.alters.alterTable(
            c,
            "ns",
            "t",
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY)))),
            null,
            null,
            "test",
            "spec",
        )
        store.alters.alterTable(c, "ns", "t", listOf(AlterOp.SetObjectPrefixField("id")), null, null, "test", "prefix")
        val under = "s3://hoglake-it/$c/data/ns/t/id=4/"
        val proved = under + "proved.parquet"
        val lying = under + "lying.parquet"
        append(
            c,
            "t",
            FileRegistration(proved, 2, putParquet(proved, listOf(4, 4)), partitionValues = listOf("4")),
            FileRegistration(lying, 2, putParquet(lying, listOf(4, 5)), partitionValues = listOf("4")),
        )
        val result = hydrator().hydratePending(c, null)
        assertThat(listOf(result.hydrated, result.quarantined)).containsExactly(2L, 1L)
        assertThat(fileRow(c, 1).let { listOf(it[0], it[4].asLong()) }).containsExactly("provided", 0L)
        assertThat(fileRow(c, 2).let { listOf(it[0], it[4].asLong()) }).containsExactly("provided", 1L)
    }

    @Test
    fun `a hydration landing while a promote plans is re-encoded by the replan`() {
        val c = HogliteConformanceEngine.uniqueName("hydrate")
        store.createCatalog(c, "s3://hoglake-it/$c")
        store.createNamespace(c, "ns")
        store.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.INT), ColumnDef("v", ColType.LONG)))
        val path = "s3://hoglake-it/$c/data/ns/t/f.parquet"
        append(c, "t", FileRegistration(path, 2, putParquet(path, listOf(1, 2), INT_ID)))
        HogliteInterposer(engine.url()).use { proxy ->
            var hydrated = -1L
            // The promote has read the table's stats (none) and the file is still pending; it gains
            // 4-byte INT bounds before the promote's transaction lands.
            val hook =
                proxy.hook({ m, p, b ->
                    HogliteInterposer.isWriteTx(m, p, b) && "schema_version = schema_version + 1" in b
                }) { hydrated = hydrator().hydratePending(c, null).hydrated }
            proxy.store.alters.alterTable(
                c,
                "ns",
                "t",
                listOf(AlterOp.PromoteColumn("id", ColType.LONG)),
                null,
                null,
                "test",
                "promote",
            )
            assertThat(hook.fired).isTrue()
            assertThat(hydrated).isEqualTo(1)
        }
        val row = fileRow(c, 1)
        assertThat(row[0]).isEqualTo("provided")
        val id = stats(row).single { it.fieldId == 1L }
        assertThat(listOf(id.lowerBound, id.upperBound)).containsExactly(le8(1), le8(2))
    }

    @Test
    fun `an id-less file pending across a promote gets the promoted type's bounds`() {
        val c = HogliteConformanceEngine.uniqueName("hydrate")
        store.createCatalog(c, "s3://hoglake-it/$c")
        store.createNamespace(c, "ns")
        store.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.INT), ColumnDef("v", ColType.LONG)))
        val path = "s3://hoglake-it/$c/data/ns/t/idless.parquet"
        append(c, "t", FileRegistration(path, 2, putParquet(path, listOf(1, 2), INT_IDLESS)))
        // The promote re-encodes only stats that exist; this file gets its own in the sweep, bound by
        // name at its begin_snapshot, where id is still an INT.
        store.alters.alterTable(
            c,
            "ns",
            "t",
            listOf(AlterOp.PromoteColumn("id", ColType.LONG)),
            null,
            null,
            "test",
            "promote",
        )
        assertThat(hydrator().hydratePending(c, null).hydrated).isEqualTo(1)
        val row = fileRow(c, 1)
        assertThat(row[0]).isEqualTo("provided")
        val id = stats(row).single { it.fieldId == 1L }
        assertThat(listOf(id.lowerBound, id.upperBound)).containsExactly(le8(1), le8(2))
    }

    @Test
    fun `an id-less file is audited, bound at its begin_snapshot, and then blocks add and rename`() {
        val c = pendingCatalog()
        val path = "s3://hoglake-it/$c/data/ns/t/idless.parquet"
        append(
            c,
            "t",
            FileRegistration(
                path,
                2,
                putParquet(path, listOf(5, 6), "message t { required int64 id; required int64 v; }"),
            ),
        )
        // v is field 2 when the file lands and field 3 by the time it hydrates: bound by name at its
        // begin_snapshot, its v stats belong to field 2.
        for (op in listOf(AlterOp.DropColumn("v"), AlterOp.AddColumn(ColumnDef("v", ColType.LONG)))) {
            store.alters.alterTable(c, "ns", "t", listOf(op), null, null, "test", "readd")
        }
        assertThat(hydrator().hydratePending(c, null).hydrated).isEqualTo(1)
        val row = fileRow(c, 1)
        assertThat(row[0]).isEqualTo("provided")
        assertThat(row[3].asLong()).isEqualTo(1)
        assertThat(stats(row).map { it.fieldId }).containsExactlyInAnyOrder(1L, 2L)
        for (op in listOf(AlterOp.AddColumn(ColumnDef("w", ColType.LONG)), AlterOp.RenameColumn("id", "k"))) {
            assertThatThrownBy { store.alters.alterTable(c, "ns", "t", listOf(op), null, null, "test", "guard") }
                .isInstanceOf(HoglakeException.IdlessFilesPresent::class.java)
        }
    }

    @Test
    fun `a transient object-store failure leaves the file pending for the next run`() {
        val c = pendingCatalog()
        val tailed = "s3://hoglake-it/$c/data/ns/t/tailed.parquet"
        val whole = "s3://hoglake-it/$c/data/ns/t/whole.parquet"
        val bytes = parquet(listOf(1))
        val footerLength = ByteBuffer.wrap(bytes, bytes.size - 8, 4).order(ByteOrder.LITTLE_ENDIAN).int.toLong()
        putParquet(tailed, listOf(1))
        putParquet(whole, listOf(1))
        append(
            c,
            "t",
            FileRegistration(tailed, 1, bytes.size.toLong(), footerSize = footerLength),
            FileRegistration(whole, 1, bytes.size.toLong()),
        )
        // The tail GET meets a closed port; the whole-object GET a 503: neither is a missing object.
        object : ObjectStore("http://127.0.0.1:9", "us-east-1", "unused", "unused", true) {
            override fun getHead(
                pathUri: String,
                maxBytes: Int,
            ): ByteArray = throw S3Exception.builder().statusCode(503).message("SlowDown").build()
        }.use { broken ->
            val result = hydrator(reads = broken).hydratePending(c, null)
            assertThat(listOf(result.claimed, result.hydrated, result.failed, result.transient))
                .containsExactly(2L, 0L, 0L, 2L)
        }
        assertThat((1L..2L).map { fileRow(c, it)[0] }).containsOnly("pending")
        assertThat(hydrator().hydratePending(c, null).hydrated).isEqualTo(2)
    }

    @Test
    fun `files that fail on every requeue do not hold back newer pending files`() {
        val c = pendingCatalog()
        append(
            c,
            "t",
            *(1..3).map {
                FileRegistration("s3://hoglake-it/$c/data/ns/t/absent$it.parquet", 1, 100)
            }.toTypedArray(),
        )
        assertThat(hydrator().hydratePending(c, null).failed).isEqualTo(3)
        append(c, "t", real(c, "late", listOf(1)))
        // One call's sweep covers two files: it starts after the highest failure, so the late file goes
        // ahead of the requeued three.
        assertThat(hydrator(sweepLimit = 2).rehydrateFailed(c).requeued).isEqualTo(3)
        assertThat(fileRow(c, 4)[0]).isEqualTo("provided")
        val sweep = store.json.readTree(lastRun(c)[3].asString())
        assertThat(listOf("claimed", "hydrated", "failed").map { sweep[it].asLong() }).containsExactly(2L, 1L, 1L)
    }

    @Test
    fun `one file that cannot be hydrated is failed and the sweep goes on`() {
        val c = pendingCatalog()
        store.alters.alterTable(
            c,
            "ns",
            "t",
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY)))),
            null,
            null,
            "test",
            "spec",
        )
        store.alters.alterTable(c, "ns", "t", listOf(AlterOp.SetObjectPrefixField("id")), null, null, "test", "prefix")
        val under = "s3://hoglake-it/$c/data/ns/t/id=4/"
        val paths = listOf(under + "bad.parquet", under + "good.parquet")
        append(
            c,
            "t",
            *paths.map { FileRegistration(it, 2, putParquet(it, listOf(4, 4)), partitionValues = listOf("4")) }
                .toTypedArray(),
        )
        write(c, "UPDATE files SET partition_values_json = 'not json' WHERE id = 1")
        val result = hydrator().hydratePending(c, null)
        assertThat(listOf(result.claimed, result.hydrated, result.failed)).containsExactly(2L, 1L, 1L)
        assertThat(listOf(fileRow(c, 1)[0], fileRow(c, 2)[0])).containsExactly("failed", "provided")
    }

    @Test
    fun `a file whose path left the catalog's data_path is failed unread`() {
        val c = pendingCatalog()
        append(c, "t", real(c, "f", listOf(1)))
        // A tenant-written row (#668) pointing at another catalog's object.
        val foreign = "s3://hoglake-it/elsewhere-$c/f.parquet"
        putParquet(foreign, listOf(7))
        write(c, "UPDATE files SET path = '$foreign' WHERE id = 1")
        assertThat(hydrator().hydratePending(c, null).failed).isEqualTo(1)
        val row = fileRow(c, 1)
        assertThat(row[0]).isEqualTo("failed")
        assertThat(row[1]).isNull()
    }

    @Test
    fun `a data_path the tenant rewrote does not move the footer reads`() {
        val c = pendingCatalog()
        // Tenant-written rows (#668): the catalog's data_path and a file aimed at another catalog's object.
        val foreign = "s3://hoglake-it/victim-$c/data/ns/t/f.parquet"
        val size = putParquet(foreign, listOf(7))
        write(c, "UPDATE meta SET data_path = 's3://hoglake-it/victim-$c' WHERE id = 1")
        append(c, "t", FileRegistration(foreign, 1, size))
        // A catalog off its placement is not swept: one failed run naming both, no file marked failed.
        // MUTATION: drop the placement check and the file is marked failed as outside the bound.
        assertThatThrownBy { hydrator().hydratePending(c, null) }
            .hasMessageContaining("'s3://hoglake-it/victim-$c'")
            .hasMessageContaining("'s3://hoglake-it/$c'")
        val row = fileRow(c, 1)
        assertThat(row[0]).isEqualTo("pending")
        assertThat(row[1]).isNull()
        assertThat(lastRun(c).take(3)).containsExactly("hydrator", "manual", "failed")
    }

    @Test
    fun `under a template a catalog is created only at its placement`() {
        val templated = HogliteCatalogStore(store.client, HogliteConformanceEngine.TENANTS, dataPathTemplate = TEMPLATE)
        val c = HogliteConformanceEngine.uniqueName("placed")
        // MUTATION: drop the check and `c` is created at another catalog's placement, inside its read bound.
        for (elsewhere in listOf("s3://hoglake-it/other-$c", "s3://hoglake-it/$c/sub")) {
            assertThatThrownBy { templated.createCatalog(c, elsewhere) }
                .isInstanceOf(HoglakeException.Validation::class.java)
                .hasMessageContaining("HOGLAKE_HOGLITE_DATA_PATH_TEMPLATE")
        }
        assertThat(templated.createCatalog(c, "s3://hoglake-it/$c/").name).isEqualTo(c)
    }

    @Test
    fun `a catalog created at its placement with a trailing slash is swept`() {
        val templated = HogliteCatalogStore(store.client, HogliteConformanceEngine.TENANTS, dataPathTemplate = TEMPLATE)
        val c = HogliteConformanceEngine.uniqueName("placed")
        // The create accepts the slash and stores the data_path as given.
        templated.createCatalog(c, "s3://hoglake-it/$c/")
        assertThat(templated.catalogOperation(c) { templated.meta(c).dataPath }).isEqualTo("s3://hoglake-it/$c/")
        templated.createNamespace(c, "ns")
        templated.createTable(c, "ns", "t", idv)
        append(c, "t", real(c, "f", listOf(1)))
        // MUTATION: compare the stored data_path untrimmed and every rehydrate skips the catalog.
        assertThat(hydrator(on = templated).rehydrateFailed(c).requeued).isZero()
        assertThat(fileRow(c, 1)[0]).isEqualTo("provided")
        assertThat(lastRun(c).take(3)).containsExactly("hydrator", "manual", "ok")
    }

    @Test
    fun `without a template only the catalog's broker session reads footers`() {
        val c = pendingCatalog()
        append(c, "t", real(c, "f", listOf(1)))
        // An unscoped provider reaches every catalog: the requeue runs alone.
        val unconfined = HogliteHydrator(store, { objects })
        assertThat(unconfined.rehydrateFailed(c).requeued).isZero()
        assertThat(fileRow(c, 1)[0]).isEqualTo("pending")
        assertThatThrownBy { unconfined.hydratePending(c, null) }.isInstanceOf(IllegalStateException::class.java)
        // MUTATION: skip the sweep without recording it and the ledger and status show only the requeue.
        val hydratorRuns =
            store.catalogOperation(c) {
                store.queryCatalog(c, "SELECT status FROM maintenance_runs WHERE task = 'hydrator' ORDER BY id")
                    .rows.map { it[0].asString() }
            }
        assertThat(hydratorRuns).describedAs("the requeue, then each skipped sweep").containsExactly(
            "ok",
            "failed",
            "failed",
        )
        val last =
            HogliteMaintenanceStatus(store, 0, 0, 0, 0, 0).status(c).tasks
                .single { it.task == MaintenanceTask.HYDRATOR }.lastRun!!
        assertThat(last.status).isEqualTo(MaintenanceRunStatus.FAILED)
        assertThat(last.error).contains("HOGLAKE_HOGLITE_DATA_PATH_TEMPLATE")
        // A scoped provider asks for the catalog's session; the broker, not data_path, bounds it.
        val asked = mutableListOf<Pair<String, String>>()
        brokered(asked).use { scoped -> HogliteHydrator(store, { scoped }).rehydrateFailed(c) }
        assertThat(fileRow(c, 1)[0]).isEqualTo("provided")
        assertThat(asked).containsExactly(c to "s3://hoglake-it/$c")
    }

    @Test
    fun `without a template a data_path the tenant rewrote is the broker's to refuse`() {
        val c = pendingCatalog()
        val foreign = "s3://hoglake-it/victim-$c/data/ns/t/f.parquet"
        val size = putParquet(foreign, listOf(7))
        write(c, "UPDATE meta SET data_path = 's3://hoglake-it/victim-$c' WHERE id = 1")
        append(c, "t", FileRegistration(foreign, 1, size))
        // The rewritten data_path reaches the broker as the prefix asked for: only its refusal keeps the
        // read off the victim's objects (#668). The session is resolved before any claim, so the refusal
        // fails the run once instead of counting every file's GET transient.
        // MUTATION: drop the resolve before the sweep and the file is claimed, its refused GET in the run.
        val asked = mutableListOf<Pair<String, String>>()
        brokered(asked).use { scoped ->
            assertThatThrownBy { HogliteHydrator(store, { scoped }).hydratePending(c, null) }
                .isInstanceOf(StorageCredentialException::class.java)
                .hasMessageContaining("prefix_outside_catalog")
        }
        assertThat(asked).containsExactly(c to "s3://hoglake-it/victim-$c")
        val row = fileRow(c, 1)
        assertThat(row[0]).isEqualTo("pending")
        assertThat(row[1]).isNull()
        val run = lastRun(c)
        assertThat(run.take(3)).containsExactly("hydrator", "manual", "failed")
        assertThat(run[3]).describedAs("no file claimed").isNull()
    }

    @Test
    fun `a storage session refused mid-sweep stops it with the files pending`() {
        val c = pendingCatalog()
        append(c, "t", real(c, "f1", listOf(1)), real(c, "f2", listOf(2)))
        // One vend, the check before the sweep; then the session cannot be had, as with a broker down.
        // MUTATION: count the refusal transient and both files are claimed and the run recorded ok.
        brokered(mutableListOf(), vends = 1).use { scoped ->
            assertThatThrownBy { HogliteHydrator(store, { scoped }).hydratePending(c, null) }
                .hasRootCauseInstanceOf(StorageCredentialException::class.java)
        }
        assertThat(listOf(fileRow(c, 1)[0], fileRow(c, 2)[0])).containsExactly("pending", "pending")
        val run = lastRun(c)
        assertThat(run.take(3)).containsExactly("hydrator", "manual", "failed")
        // MUTATION: keep the refused file claimed and the run records a partial result claiming it.
        assertThat(run[3]).describedAs("the refused file read nothing: none claimed").isNull()
    }

    @Test
    fun `stats over the scan's per-file limit are not stored`() {
        val c = HogliteConformanceEngine.uniqueName("hydrate")
        store.createCatalog(c, "s3://hoglake-it/$c")
        store.createNamespace(c, "ns")
        store.createTable(c, "ns", "t", (1..WIDE).map { ColumnDef("c$it", ColType.LONG) })
        val schema = "message t { " + (1..WIDE).joinToString(" ") { "required int64 c$it = $it;" } + " }"
        val type = org.apache.parquet.schema.MessageTypeParser.parseMessageType(schema)
        val tmp = Files.createTempFile(dir, "wide", ".parquet")
        Files.delete(tmp)
        org.apache.parquet.hadoop.example.ExampleParquetWriter
            .builder(org.apache.parquet.io.LocalOutputFile(tmp))
            .withType(type)
            .build()
            .use { w ->
                val row = org.apache.parquet.example.data.simple.SimpleGroupFactory(type).newGroup()
                (1..WIDE).forEach { row.append("c$it", it.toLong()) }
                w.write(row)
            }
        val path = "s3://hoglake-it/$c/data/ns/t/wide.parquet"
        val bytes = Files.readAllBytes(tmp)
        engine.s3Client().use { s3 ->
            s3.putObject(
                { it.bucket(HogliteConformanceEngine.DATA_BUCKET).key(path.removePrefix("s3://hoglake-it/")) },
                RequestBody.fromBytes(bytes),
            )
        }
        append(c, "t", FileRegistration(path, 1, bytes.size.toLong()))
        assertThat(hydrator().hydratePending(c, null).hydrated).isEqualTo(1)
        val row = fileRow(c, 1)
        assertThat(row[0]).isEqualTo("provided")
        assertThat(stats(row)).isEmpty()
        // The scan that would have refused the file reads it without bounds.
        val scanned = store.planScan(c, "ns", "t", columnStats = ScanService.ColumnStatsRequest(setOf(1)))
        assertThat(scanned.single().dataFile.columnStats.orEmpty()).isEmpty()
    }

    @Test
    fun `files that fail on every call rotate, so a newer file is reached`() {
        val c = pendingCatalog()
        append(
            c,
            "t",
            *(1..4).map {
                FileRegistration("s3://hoglake-it/$c/data/ns/t/absent$it.parquet", 1, 100)
            }.toTypedArray(),
        )
        assertThat(hydrator().hydratePending(c, null).failed).isEqualTo(4)
        val limited = hydrator(sweepLimit = 2)
        limited.rehydrateFailed(c)
        append(c, "t", real(c, "late", listOf(1)))
        // Sweeps of two: 1-2, then 3-4, then the late file 5. Restarting at the lowest pending id
        // alternates 1-2 and 3-4 forever.
        repeat(2) { limited.rehydrateFailed(c) }
        assertThat(fileRow(c, 5)[0]).isEqualTo("provided")
    }

    @Test
    fun `a failed file of a dropped table does not pin the rotation`() {
        val c = pendingCatalog()
        append(
            c,
            "t",
            *(1..4).map {
                FileRegistration("s3://hoglake-it/$c/data/ns/t/absent$it.parquet", 1, 100)
            }.toTypedArray(),
        )
        append(c, "t", real(c, "good", listOf(1)))
        store.createTable(c, "ns", "t2", idv)
        append(c, "t2", FileRegistration("s3://hoglake-it/$c/data/ns/t2/absent.parquet", 1, 100))
        write(c, "UPDATE files SET stats_state = 'failed' WHERE id = 6")
        store.dropTable(c, "ns", "t2", store.getTable(c, "ns", "t2").tableUuid, "test", "drop")
        // File 6 stays live and failed on its dropped table, above every live id. Counted as the last
        // failure, it would start each sweep past every pending file, and the wrap would retry 1-2 forever.
        // MUTATION: drop the live-table scope from the cursor and file 5 stays pending.
        val limited = hydrator(sweepLimit = 2)
        repeat(3) { limited.rehydrateFailed(c) }
        assertThat(fileRow(c, 5)[0]).isEqualTo("provided")
        assertThat(fileRow(c, 6)[0]).isEqualTo("failed")
    }

    @Test
    fun `a sweep reads every pending page, resuming after a halved one`() {
        val c = pendingCatalog()
        append(c, "t", real(c, "f1", listOf(1)), real(c, "f2", listOf(2)), real(c, "f3", listOf(3)))
        HogliteInterposer(engine.url()).use { proxy ->
            proxy.refuse(
                { _, _, b -> "ORDER BY f.id LIMIT ?" in b },
                413,
                """{"code":"result_too_large","error":"injected"}""",
            )
            // MUTATION: end the sweep after its first page and only file 1 is claimed.
            assertThat(hydrator(on = proxy.store, pageRows = 2).hydratePending(c, null).hydrated).isEqualTo(3)
            val pages =
                proxy.requests.flatMap { body ->
                    val root = store.json.readTree(body)
                    root["statements"]?.toList() ?: listOf(root)
                }.filter { it.has("sql") && "ORDER BY f.id LIMIT ?" in it["sql"].asText() }
            // (after, rows): the refused page, its half, then a full page after file 1, then the empty end.
            assertThat(pages.map { p -> p["args"].map { it.asLong() } })
                .containsExactly(listOf(2L), listOf(1L), listOf(1L, 2L), listOf(3L, 2L))
        }
        assertThat((1L..3L).map { fileRow(c, it)[0] }).containsOnly("provided")
    }

    @Test
    fun `an object longer than its registration is failed without being buffered`() {
        val c = pendingCatalog()
        val whole = "s3://hoglake-it/$c/data/ns/t/whole.parquet"
        val tailed = "s3://hoglake-it/$c/data/ns/t/tailed.parquet"
        val size = putParquet(whole, listOf(1, 2, 3))
        putParquet(tailed, listOf(1, 2, 3))
        val bytes = parquet(listOf(1, 2, 3))
        val footerLength = ByteBuffer.wrap(bytes, bytes.size - 8, 4).order(ByteOrder.LITTLE_ENDIAN).int.toLong()
        val heads = mutableListOf<Int>()
        val ranges = mutableListOf<Int>()
        val counting =
            object : ObjectStore(engine.config()) {
                override fun getHead(
                    pathUri: String,
                    maxBytes: Int,
                ): ByteArray = super.getHead(pathUri, maxBytes).also { heads += it.size }

                override fun getRange(
                    pathUri: String,
                    startInclusive: Long,
                    length: Int,
                ): ByteArray = super.getRange(pathUri, startInclusive, length).also { ranges += it.size }
            }
        counting.use {
            // Registered at 16 bytes and at one byte short: neither read takes more than that plus one.
            append(
                c,
                "t",
                FileRegistration(whole, 3, 16),
                FileRegistration(tailed, 3, size - 1, footerSize = footerLength),
            )
            assertThat(hydrator(reads = counting).hydratePending(c, null).failed).isEqualTo(2)
            assertThat(ranges).containsExactly(Math.toIntExact(footerLength + 8))
            assertThat(heads).containsExactly(17, Math.toIntExact(size))
            assertThat(counting.getHead(whole, Math.toIntExact(size + 100)).size.toLong()).isEqualTo(size)
        }
        assertThat((1L..2L).map { fileRow(c, it)[0] }).containsOnly("failed")
    }

    @Test
    fun `an object shorter than its registration is failed, so it rotates past a newer file`() {
        val c = pendingCatalog()
        val tailed = "s3://hoglake-it/$c/data/ns/t/tailed.parquet"
        val size = putParquet(tailed, listOf(1))
        val bytes = parquet(listOf(1))
        val footerLength = ByteBuffer.wrap(bytes, bytes.size - 8, 4).order(ByteOrder.LITTLE_ENDIAN).int.toLong()
        // The tail starts past the object's end: S3 answers 416.
        append(c, "t", FileRegistration(tailed, 1, size * 100, footerSize = footerLength), real(c, "good", listOf(1)))
        // MUTATION: classify the 416 transient and both files stay pending: the sweep of one claims file 1
        // on every call.
        val limited = hydrator(sweepLimit = 1)
        limited.rehydrateFailed(c)
        assertThat(fileRow(c, 1)[0]).isEqualTo("failed")
        // The next call requeues file 1 and starts its sweep after it.
        limited.rehydrateFailed(c)
        assertThat(listOf(fileRow(c, 1)[0], fileRow(c, 2)[0])).containsExactly("pending", "provided")
    }

    @Test
    fun `a failure found against an altered table leaves the file pending and uncounted`() {
        val c = pendingCatalog()
        append(c, "t", FileRegistration("s3://hoglake-it/$c/data/ns/t/absent.parquet", 1, 100))
        HogliteInterposer(engine.url()).use { proxy ->
            val hook =
                proxy.hook({ m, p, b -> HogliteInterposer.isWriteTx(m, p, b) && "stats_state = 'failed'" in b }) {
                    store.alters.alterTable(
                        c,
                        "ns",
                        "t",
                        listOf(AlterOp.AddColumn(ColumnDef("w", ColType.LONG))),
                        null,
                        null,
                        "test",
                        "race",
                    )
                }
            val result = hydrator(on = proxy.store).hydratePending(c, null)
            assertThat(hook.fired).isTrue()
            assertThat(listOf(result.claimed, result.failed)).containsExactly(1L, 0L)
        }
        assertThat(fileRow(c, 1)[0]).isEqualTo("pending")
        assertThat(hydrator().hydratePending(c, null).failed).isEqualTo(1)
    }

    @Test
    fun `a sweep stopped by the engine records what it committed`() {
        val c = pendingCatalog()
        append(c, "t", real(c, "f1", listOf(1)), real(c, "f2", listOf(2)))
        HogliteInterposer(engine.url()).use { proxy ->
            var flips = 0
            val hook =
                proxy.refuse(
                    { m, p, b ->
                        HogliteInterposer.isWriteTx(m, p, b) && "stats_state = 'provided'" in b && ++flips == 2
                    },
                    504,
                    """{"code":"query_timeout","error":"injected"}""",
                )
            assertThatThrownBy { hydrator(on = proxy.store).hydratePending(c, null) }
                .hasMessageContaining("injected")
            assertThat(hook.fired).isTrue()
        }
        val run = lastRun(c)
        assertThat(run.take(3)).containsExactly("hydrator", "manual", "failed")
        val partial = store.json.readTree(run[3].asString())
        assertThat(listOf("claimed", "hydrated").map { partial[it].asLong() }).containsExactly(2L, 1L)
        assertThat(listOf(fileRow(c, 1)[0], fileRow(c, 2)[0])).containsExactly("provided", "pending")
    }

    @Test
    fun `a pending page the result cap refuses is read again at half the rows`() {
        val c = pendingCatalog()
        append(c, "t", real(c, "f", listOf(1)))
        HogliteInterposer(engine.url()).use { proxy ->
            val hook =
                proxy.refuse(
                    { _, _, b -> "ORDER BY f.id LIMIT ?" in b },
                    413,
                    """{"code":"result_too_large","error":"injected"}""",
                )
            assertThat(hydrator(on = proxy.store).hydratePending(c, null).hydrated).isEqualTo(1)
            assertThat(hook.fired).isTrue()
            assertThat(proxy.requests.filter { "ORDER BY f.id LIMIT ?" in it }).hasSize(2)
        }
    }

    @Test
    fun `the promote's stat-less fence reads the hydration backlog, not the manifest`() {
        val c = HogliteConformanceEngine.uniqueName("hydrate")
        store.createCatalog(c, "s3://hoglake-it/$c")
        store.createNamespace(c, "ns")
        store.createTable(c, "ns", "t", listOf(ColumnDef("id", ColType.INT), ColumnDef("v", ColType.LONG)))
        val path = "s3://hoglake-it/$c/data/ns/t/f.parquet"
        append(c, "t", FileRegistration(path, 2, putParquet(path, listOf(1, 2), INT_ID)))
        HogliteInterposer(engine.url()).use { proxy ->
            proxy.store.alters.alterTable(
                c,
                "ns",
                "t",
                listOf(AlterOp.PromoteColumn("id", ColType.LONG)),
                null,
                null,
                "test",
                "promote",
            )
            val fences =
                proxy.requests.flatMap { body ->
                    val root = store.json.readTree(body)
                    root["statements"]?.toList() ?: listOf(root)
                }.filter { it.has("sql") && "stats_state IN ('pending', 'failed')" in it["sql"].asText() }
            assertThat(fences).describedAs("the read and the in-transaction guard").hasSize(2)
            for (fence in fences) {
                val args = fence["args"]?.map { store.json.convertValue(it, Any::class.java) }.orEmpty()
                val plan =
                    store.catalogOperation(c) {
                        store.queryCatalog(c, "EXPLAIN QUERY PLAN ${fence["sql"].asText()}", *args.toTypedArray())
                            .rows.joinToString { it.last().asString() }
                    }
                assertThat(plan).contains("files_live_stats").doesNotContain("SCAN files")
            }
        }
    }

    @TempDir
    lateinit var dir: Path

    /**
     * [block] against an [App] that routes [c] to the live engine by token, with no PostgreSQL, under
     * [configure]d settings: the selector builds the Hoglite backends, its hydrator included.
     */
    private fun routed(
        c: String,
        configure: (Config) -> Config = { it },
        block: suspend ApplicationTestBuilder.(token: String) -> Unit,
    ) {
        val id = UUID.randomUUID()
        val file = Files.createTempFile(dir, "backends", ".json")
        Files.writeString(file, """{"backends":[{"id":"$id","kind":"hoglite","url":"${engine.url()}"}]}""")
        val cfg =
            configure(
                engine.config().copy(
                    defaultBackend = "none",
                    backendsFile = file.toString(),
                    authMode = "enforce",
                    jdbcUrl = "jdbc:no-such-driver:must-not-open",
                    hogliteDataPathTemplate = TEMPLATE,
                ),
            )
        val pair = TestTokens.keyPair()
        val auth =
            AuthService(
                AuthMode.ENFORCE,
                JwtVerifier(
                    TestTokens.ISSUER,
                    TestTokens.AUDIENCE,
                    StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
                    backends = cfg.backendKinds,
                ),
            )
        val tenant = HogliteConformanceEngine.TENANTS.tenantFor(c)
        val token =
            TestTokens.mint(
                pair,
                TestTokens.operatorClaims().dropLast(1) +
                    """, "cat":{"$c":["admin"]}, "rtg":{"b":"hoglite","be":"$id","t":"$tenant"}}""",
            )
        App.build(cfg).use { app ->
            testApplication {
                application { app.module(this, auth) }
                block(token)
            }
        }
    }

    @Test
    fun `routed requeue hydrates, needs no PostgreSQL, and the retirement route stays absent`() {
        val c = catalog()
        routed(c) { token ->
            val result =
                client.post("/v1/catalogs/$c/maintenance/rehydrate?namespace=ns&table=a") {
                    header(HttpHeaders.Authorization, "Bearer $token")
                }
            assertThat(result.status).isEqualTo(HttpStatusCode.OK)
            assertThat(store.json.readTree(result.bodyAsText())["requeued"].asLong()).isEqualTo(1)
            // The routed instance then swept the requeued file: its object is absent, so it failed again.
            assertThat(states(c).first()).isEqualTo("failed")
            val sweep = store.json.readTree(lastRun(c)[3].asString())
            assertThat(listOf(sweep["claimed"].asLong(), sweep["failed"].asLong())).containsExactly(1L, 1L)
            val invalid =
                client.post("/v1/catalogs/$c/maintenance/rehydrate?namespace=ns") {
                    header(HttpHeaders.Authorization, "Bearer $token")
                }
            assertThat(invalid.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
            val other =
                client.post("/v1/catalogs/other/maintenance/rehydrate") {
                    header(HttpHeaders.Authorization, "Bearer $token")
                }
            assertThat(other.status).isEqualTo(HttpStatusCode.NotFound)
            val absent =
                client.post("/v1/catalogs/$c/maintenance/retire") {
                    header(HttpHeaders.Authorization, "Bearer $token")
                }
            assertThat(absent.status).isEqualTo(HttpStatusCode.Unauthorized)
        }
    }

    @Test
    fun `the routed sweep holds the tail read to HOGLAKE_HOGLITE_MAX_FOOTER_BYTES`() {
        val c = pendingCatalog()
        val path = "s3://hoglake-it/$c/data/ns/t/f.parquet"
        val size = putParquet(path, listOf(1, 2, 3))
        val bytes = parquet(listOf(1, 2, 3))
        val footerLength = ByteBuffer.wrap(bytes, bytes.size - 8, 4).order(ByteOrder.LITTLE_ENDIAN).int.toLong()
        append(c, "t", FileRegistration(path, 3, size, footerSize = footerLength))
        // A footer one byte over the cap is not usable, and the whole-object cap, one byte past the tail,
        // refuses the fallback: the file fails.
        // MUTATION: drop maxFooterBytes from the selector's HogliteHydrator and the file is provided.
        routed(c, {
            it.copy(hogliteMaxFooterBytes = footerLength - 1, hydratorMaxWholeObjectBytes = footerLength + 8)
        }) { token ->
            val result =
                client.post("/v1/catalogs/$c/maintenance/rehydrate") {
                    header(HttpHeaders.Authorization, "Bearer $token")
                }
            assertThat(result.status).isEqualTo(HttpStatusCode.OK)
        }
        assertThat(fileRow(c, 1)[0]).isEqualTo("failed")
        assertThat(lastRun(c).take(3)).containsExactly("hydrator", "manual", "ok")
    }

    private companion object {
        const val TEMPLATE = "s3://hoglake-it/{catalog}"

        /** Columns enough for one file's stats to pass the scan's 64 KiB per-file limit. */
        const val WIDE = 1000
        const val WITH_IDS = "message t { required int64 id = 1; required int64 v = 2; }"
        const val INT_ID = "message t { required int32 id = 1; required int64 v = 2; }"
        const val INT_IDLESS = "message t { required int32 id; required int64 v; }"
    }
}
