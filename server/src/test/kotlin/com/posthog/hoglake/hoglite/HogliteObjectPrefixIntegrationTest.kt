package com.posthog.hoglake.hoglite

import com.posthog.hoglake.App
import com.posthog.hoglake.HealthProbe
import com.posthog.hoglake.compaction.CompactionConfig
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.PartitionLookupInvalidRequest
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.PatchField
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.testing.TestImages
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assumptions.assumeTrue
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
import java.net.ServerSocket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.time.Duration
import java.util.UUID

/**
 * The hoglite side of the features main added while this backend was in
 * review, each against a real `hoglited` (HogliteBackendIntegrationTest's
 * harness): the object-prefix layout (`set_object_prefix_field`, its DDL
 * refusals, the commit rule for files and DVs), the hydrator's
 * object-prefix quarantine, compaction under each group's prefix, retired
 * partition specs surviving expiry, exact partition inspection, and the
 * tenant-schema migration that brings an older tenant up to the columns
 * those features need.
 *
 * The Postgres twins are AlterObjectPrefixIntegrationTest,
 * CommitObjectPrefixIntegrationTest, HydratorObjectPrefixIntegrationTest,
 * CompactionObjectPrefixIntegrationTest, ExpiryServiceIntegrationTest's
 * retired-spec cases and PartitionLookupServiceIntegrationTest; the
 * refusal messages asserted here are theirs.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteObjectPrefixIntegrationTest {
    private lateinit var minio: MinIOContainer
    private var process: Process? = null
    private lateinit var client: HogliteClient
    private lateinit var store: HogliteCatalogStore
    private lateinit var alters: HogliteAlters
    private var catalogCounter = 0

    @BeforeAll
    fun startHoglite() {
        val binary = System.getenv("HOGLITE_BINARY") ?: findOnPath("hoglited")
        assumeTrue(binary != null) { "no hoglited binary (set HOGLITE_BINARY); skipping hoglite backend tests" }
        minio = TestImages.minio()
        minio.start()
        val s3Endpoint = "http://127.0.0.1:${minio.firstMappedPort}"
        val bucket = "hoglite-it"
        s3Client().use { s3 ->
            s3.createBucket { it.bucket(bucket) }
            s3.createBucket { it.bucket("hoglake-it") }
        }
        val port = ServerSocket(0).use { it.localPort }
        val dir = Files.createTempDirectory("hoglited-prefix-it").toFile()
        val builder =
            ProcessBuilder(
                binary,
                "-addr", "127.0.0.1:$port",
                "-dir", dir.absolutePath,
                "-bucket", bucket,
                "-id", "it-1",
                "-class", "standard",
            ).redirectErrorStream(true)
        builder.environment().putAll(
            mapOf(
                "AWS_ENDPOINT_URL" to s3Endpoint,
                "AWS_ACCESS_KEY_ID" to minio.userName,
                "AWS_SECRET_ACCESS_KEY" to minio.password,
                "AWS_REGION" to "us-east-1",
            ),
        )
        process = builder.start()
        val http = HttpClient.newHttpClient()
        val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos()
        var up = false
        while (System.nanoTime() < deadline && process?.isAlive != false) {
            try {
                val resp =
                    http.send(
                        HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port/v1/healthz")).build(),
                        HttpResponse.BodyHandlers.discarding(),
                    )
                if (resp.statusCode() == 200) {
                    up = true
                    break
                }
            } catch (_: Exception) {
                // not up yet
            }
            Thread.sleep(100)
        }
        check(
            up,
        ) { "hoglited did not come up; output: ${process?.inputStream?.readBytes()?.decodeToString()?.take(2000)}" }
        client = HogliteClient(standardUrl = "http://127.0.0.1:$port", sleeper = {})
        store = HogliteCatalogStore(client).apply { requireOperationBindingForTests = true }
        alters = HogliteAlters(store)
    }

    @AfterAll
    fun stopHoglite() {
        if (::client.isInitialized) client.close()
        process?.destroy()
        process?.waitFor()
        if (::minio.isInitialized) minio.stop()
    }

    private fun findOnPath(binary: String): String? {
        val path = System.getenv("PATH") ?: return null
        return path.split(':').map { "$it/$binary" }.firstOrNull { java.io.File(it).canExecute() }
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
            database = com.posthog.hoglake.DatabaseBackend.HOGLITE,
            hogliteUrl = "http://127.0.0.1:1",
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

    // ---- fixtures ------------------------------------------------------------

    private val columns = listOf(ColumnDef("team_id", ColType.LONG), ColumnDef("v", ColType.LONG))
    private val teamSpec = listOf(PartitionFieldDef(1, Transform.IDENTITY))

    /** `ingest.events` partitioned by identity(team_id) with the option on it, and `ingest.plain` without. */
    private fun fixture(): String {
        val cat = "prefix${catalogCounter++}"
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

    /** A raw tenant write, for the states the API refuses to produce (a foreign row). */
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

    // ---- set_object_prefix_field (#481) ----------------------------------------

    @Test
    fun `set_object_prefix_field sets the layout, getTable reports it, and the Postgres refusals hold`() {
        val cat = fixture()
        assertThat(store.getTable(cat, "ingest", "events").objectPrefixField).isEqualTo("team_id")
        assertThat(store.getTable(cat, "ingest", "plain").objectPrefixField).isNull()
        // The layout is versioned: before the set, the table had none.
        val setAt = store.getTable(cat, "ingest", "events").readSnapshotId
        assertThat(store.getTable(cat, "ingest", "events", snapshot = setAt - 1).objectPrefixField).isNull()
        assertThat(scalar(cat, "SELECT COUNT(*) FROM table_layouts WHERE end_snapshot IS NULL")).isEqualTo(1L)

        // Same field again: a no-op, not a second row.
        val head = store.getCatalog(cat).headSnapshotId
        alters.alterTable(cat, "ingest", "events", listOf(AlterOp.SetObjectPrefixField("team_id")))
        assertThat(scalar(cat, "SELECT COUNT(*) FROM table_layouts")).isEqualTo(1L)
        assertThat(store.getCatalog(cat).headSnapshotId).isGreaterThanOrEqualTo(head)

        // A different field is refused while one is set.
        assertThatThrownBy {
            alters.alterTable(cat, "ingest", "events", listOf(AlterOp.SetObjectPrefixField("v")))
        }.isInstanceOf(HoglakeException.Validation::class.java)

        // A field without exactly one identity key in the live spec is refused.
        store.createTable(cat, "ingest", "unpartitioned", columns)
        assertThatThrownBy {
            alters.alterTable(cat, "ingest", "unpartitioned", listOf(AlterOp.SetObjectPrefixField("team_id")))
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("identity")

        // A non-empty table is refused (its files were never held to a prefix).
        append(cat, "plain", file(root(cat, "plain") + "a.parquet", "4", null))
        assertThatThrownBy {
            alters.alterTable(cat, "ingest", "plain", listOf(AlterOp.SetObjectPrefixField("team_id")))
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("empty")
        assertThat(store.getTable(cat, "ingest", "plain").objectPrefixField).isNull()
    }

    @Test
    fun `a layout table refuses the DDL that would move its prefix`() {
        val cat = fixture()
        val refusals =
            listOf(
                AlterOp.DropColumn("team_id"),
                AlterOp.RenameColumn("team_id", "team"),
                AlterOp.RenameTable("events2"),
                AlterOp.SetPartitionSpec(emptyList()),
                AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(2, Transform.IDENTITY))),
            )
        for (op in refusals) {
            assertThatThrownBy { alters.alterTable(cat, "ingest", "events", listOf(op)) }
                .describedAs(op.toString())
                .isInstanceOf(HoglakeException.Validation::class.java)
                .hasMessageContaining("object-prefix")
        }
        // The twin without the option accepts the same rename.
        alters.alterTable(cat, "ingest", "plain", listOf(AlterOp.RenameColumn("team_id", "team")))
        // A spec that keeps the single identity key on the field is fine.
        alters.alterTable(
            cat,
            "ingest",
            "events",
            listOf(AlterOp.SetPartitionSpec(teamSpec + PartitionFieldDef(2, Transform.IDENTITY))),
        )
        // An atomic replacement is refused at prepare (TableCreationService's message).
        val events = store.getTable(cat, "ingest", "events")
        assertThatThrownBy {
            HogliteTableCreations(store).prepare(
                cat,
                UUID.randomUUID(),
                TableCreationDefinition(
                    namespace = "ingest",
                    name = "events",
                    columns = columns,
                    replacement =
                        com.posthog.hoglake.service.ReplacementTarget(
                            events.tableUuid,
                            events.readSnapshotId,
                        ),
                    author = "test",
                    message = "replace",
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("cannot replace table 'ingest.events': it has the object-prefix layout")
    }

    // ---- the commit rule (#481/#483) -----------------------------------------

    @Test
    fun `commits on a layout table must sit under the value's prefix and prove it, the twin accepts all`() {
        val cat = fixture()
        // Admitted: under the prefix with proving stats, and deferred stats.
        append(cat, "events", good(cat))
        append(cat, "events", file(under(cat, "events", "4") + "deferred.parquet", "4", null))
        assertThat(scalar(cat, "SELECT COUNT(*) FROM files WHERE stats_state = 'pending'")).isEqualTo(1L)

        val refused =
            listOf(
                // At the table root.
                file(root(cat, "events") + "root.parquet", "4", stats(le8(4), le8(4))),
                // Under another value's prefix.
                file(under(cat, "events", "5") + "other.parquet", "4", stats(le8(4), le8(4))),
                // Non-canonical value.
                file(under(cat, "events", "04") + "pad.parquet", "04", stats(le8(4), le8(4))),
                // Inline stats that do not prove the value.
                file(under(cat, "events", "4") + "wide.parquet", "4", stats(le8(4), le8(5))),
                file(under(cat, "events", "4") + "nulls.parquet", "4", stats(le8(4), le8(4), nulls = 1)),
            )
        for (f in refused) {
            assertThatThrownBy { append(cat, "events", f) }
                .describedAs(f.path)
                .isInstanceOf(HoglakeException.ObjectPrefixViolation::class.java)
                .hasMessageContaining("lives under ${root(cat, "events")}team_id=<value>/")
            // The twin: same shape on the option-off table, accepted.
            append(cat, "plain", f.copy(path = f.path.replace("/events/", "/plain/")))
        }
        // A refusal registers nothing.
        assertThat(
            scalar(
                cat,
                "SELECT COUNT(*) FROM files f JOIN tables t ON t.id = f.table_id " +
                    "JOIN table_versions v ON v.table_id = t.id AND v.end_snapshot IS NULL WHERE v.name = 'events'",
            ),
        )
            .isEqualTo(2L)
    }

    @Test
    fun `a deletion vector must sit under its target's prefix, staged or durable`() {
        val cat = fixture()
        val target = good(cat)
        append(cat, "events", target)
        val id = fileId(cat, target.path)
        val head = store.getCatalog(cat).headSnapshotId

        fun dv(path: String) =
            store.commit(
                cat,
                CommitRequest(
                    readSnapshot = head,
                    idempotencyKey = UUID.randomUUID(),
                    deletes = listOf(TableDeletes("ingest", "events", listOf(DeleteFileRegistration(id, path, 1, 10)))),
                ),
            )
        assertThatThrownBy { dv(root(cat, "events") + "dv.puffin") }
            .isInstanceOf(HoglakeException.ObjectPrefixViolation::class.java)
        assertThatThrownBy { dv(under(cat, "events", "5") + "dv.puffin") }
            .isInstanceOf(HoglakeException.ObjectPrefixViolation::class.java)
        assertThat(scalar(cat, "SELECT COUNT(*) FROM dvs")).isEqualTo(0L)
        dv(under(cat, "events", "4") + "dv.puffin")
        assertThat(scalar(cat, "SELECT COUNT(*) FROM dvs WHERE end_snapshot IS NULL")).isEqualTo(1L)

        // Staged: a DV in the same commit as its target, by path.
        val staged = good(cat, value = 7)
        val expected = store.getTable(cat, "ingest", "events").tableUuid

        fun stagedCommit(dvPath: String) =
            store.commit(
                cat,
                CommitRequest(
                    allowPendingDeletes = true,
                    requireUnchangedTables = true,
                    readSnapshot = store.getCatalog(cat).headSnapshotId,
                    idempotencyKey = UUID.randomUUID(),
                    appends = listOf(TableAppend("ingest", "events", listOf(staged), expectedTableUuid = expected)),
                    deletes =
                        listOf(
                            TableDeletes(
                                "ingest",
                                "events",
                                listOf(DeleteFileRegistration(0, dvPath, 1, 10, dataFilePath = staged.path)),
                                expectedTableUuid = expected,
                            ),
                        ),
                ),
            )
        assertThatThrownBy { stagedCommit(under(cat, "events", "4") + "staged-dv.puffin") }
            .isInstanceOf(HoglakeException.ObjectPrefixViolation::class.java)
        assertThat(scalar(cat, "SELECT COUNT(*) FROM files WHERE path = ?", staged.path)).isEqualTo(0L)
        stagedCommit(under(cat, "events", "7") + "staged-dv.puffin")
        assertThat(scalar(cat, "SELECT COUNT(*) FROM files WHERE path = ?", staged.path)).isEqualTo(1L)
    }

    // ---- retired partition specs survive expiry (#479) ------------------------

    @Test
    fun `retired partition specs survive expiry and still resolve their files`() {
        val cat = fixture()
        val oldFile = file(root(cat, "plain") + "old.parquet", "4", null)
        append(cat, "plain", oldFile)
        // Spec 1 retires; spec 2 (identity on v) goes live.
        val receipt =
            alters.alterTable(
                cat,
                "ingest",
                "plain",
                listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(2, Transform.IDENTITY)))),
            )
        assertThat(receipt.partitionSpec!!.specId).isEqualTo(2L)
        append(cat, "plain", FileRegistration(root(cat, "plain") + "new.parquet", 10, 1_000, 20, null, listOf("9")))
        HogliteOptions(store).patch(cat, PatchField.Set(1L), consumerFloor = null, author = "test", message = "short")
        Thread.sleep(1_100)
        HogliteExpiry(store, purgePage = 100, purgeBudgetMs = 10_000).runOnce(cat, 100, MaintenanceTrigger.MANUAL)
        val floor = store.getCatalog(cat).earliestSnapshotId
        val specEnd =
            scalar(
                cat,
                "SELECT end_snapshot FROM partition_specs WHERE spec_id = 1 AND table_id = " +
                    "(SELECT table_id FROM table_versions WHERE name = 'plain' AND end_snapshot IS NULL)",
            )
        // The floor passed the retired spec's end, and the row is still there.
        assertThat(floor).isGreaterThan(specEnd)
        assertThat(HogliteExpiry.VERSIONED_TABLES_NEVER_CLEARED).containsExactly("partition_specs")
        // The live file written under spec 1 still resolves its arity there.
        val lookup = HoglitePartitionLookup(store).lookup(cat, "ingest", "plain", 1, listOf("4"))
        assertThat(lookup.files.map { it.path }).containsExactly(oldFile.path)
        assertThat(lookup.files.single().specId).isEqualTo(1L)
    }

    @Test
    fun `a spec set after unpartitioning gets the id the allocator gives it, like Postgres`() {
        val cat = fixture()
        // Unpartition (spec 1 ends), then partition again: the allocator is
        // MAX(spec_id)+1 over every row, so the new spec is 2, not 1.
        alters.alterTable(cat, "ingest", "plain", listOf(AlterOp.SetPartitionSpec(emptyList())))
        val again = alters.alterTable(cat, "ingest", "plain", listOf(AlterOp.SetPartitionSpec(teamSpec)))
        assertThat(again.partitionSpec!!.specId).isEqualTo(2L)
        assertThat(store.getTable(cat, "ingest", "plain").partitionSpec!!.specId).isEqualTo(2L)
        // Two spec changes in ONE alter: the first never became visible, so
        // it is deleted and its id reused (AlterService.endOrDeleteSpec).
        val twice =
            alters.alterTable(
                cat,
                "ingest",
                "plain",
                listOf(
                    AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(2, Transform.IDENTITY))),
                    AlterOp.SetPartitionSpec(teamSpec + PartitionFieldDef(2, Transform.IDENTITY)),
                ),
            )
        assertThat(twice.partitionSpec!!.specId).isEqualTo(3L)
        assertThat(store.getTable(cat, "ingest", "plain").partitionSpec!!.specId).isEqualTo(3L)
        val ids =
            q(
                cat,
                "SELECT spec_id FROM partition_specs WHERE table_id = " +
                    "(SELECT table_id FROM table_versions WHERE name = 'plain' AND end_snapshot IS NULL) " +
                    "ORDER BY spec_id",
            )
                .rows.map { it[0].asLong() }
        assertThat(ids).containsExactly(1L, 2L, 3L)
    }

    // ---- exact partition inspection (#482) -----------------------------------

    @Test
    fun `exact partition lookup pages, aggregates and validates like Postgres`() {
        val cat = fixture()
        val fours = (1..3).map { good(cat, value = 4) }
        for (f in fours) append(cat, "events", f)
        append(cat, "events", good(cat, value = 5))
        val lookup = HoglitePartitionLookup(store)

        val all = lookup.lookup(cat, "ingest", "events", 1, listOf("4"))
        assertThat(all.files.map { it.path }).containsExactlyElementsOf(fours.map { it.path })
        assertThat(all.aggregates.status).isEqualTo("exact")
        assertThat(all.aggregates.totals!!.fileCount).isEqualTo(3L)
        assertThat(all.aggregates.totals!!.physicalRows).isEqualTo(30L)
        assertThat(all.aggregates.totals!!.totalBytes).isEqualTo(3_000L)
        assertThat(all.aggregates.totals!!.statsState.provided).isEqualTo(3L)
        assertThat(all.nextCursor).isNull()

        // Paging: two, then the last one with continuation aggregates.
        val first = lookup.lookup(cat, "ingest", "events", 1, listOf("4"), limit = 2)
        assertThat(first.files).hasSize(2)
        assertThat(first.nextCursor).isNotNull()
        val second = lookup.lookup(cat, "ingest", "events", 1, listOf("4"), cursor = first.nextCursor)
        assertThat(second.files.map { it.path }).containsExactly(fours[2].path)
        assertThat(second.aggregates.status).isEqualTo("unavailable")
        assertThat(second.aggregates.reason).isEqualTo("continuation")
        assertThat(second.nextCursor).isNull()
        // A cursor is bound to its lookup.
        assertThatThrownBy { lookup.lookup(cat, "ingest", "events", 1, listOf("5"), cursor = first.nextCursor) }
            .isInstanceOf(PartitionLookupInvalidRequest::class.java)

        // The candidate cap: aggregates refused, the page continues.
        val capped = HoglitePartitionLookup(store, candidateLimit = 2).lookup(cat, "ingest", "events", 1, listOf("4"))
        assertThat(capped.aggregates.reason).isEqualTo("candidate_scan_limit")
        assertThat(capped.nextCursor).isNotNull()

        // At an older snapshot, only what was live there.
        val atTwo = store.getTable(cat, "ingest", "events").readSnapshotId - 2
        assertThat(lookup.lookup(cat, "ingest", "events", 1, listOf("4"), snapshot = atTwo).files).hasSize(2)

        // Validation, Postgres's messages.
        assertThatThrownBy { lookup.lookup(cat, "ingest", "events", 1, listOf("4", "1")) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("values must contain the complete 1-field tuple for spec 1")
        assertThatThrownBy { lookup.lookup(cat, "ingest", "events", 9, listOf("4")) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("partition spec 9 is unavailable")
        assertThatThrownBy { lookup.lookup(cat, "ingest", "events", 1, listOf("4"), snapshot = 1_000_000) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("snapshot must be between 0 and catalog head")
        assertThatThrownBy { lookup.lookup(cat, "ingest", "events", 1, listOf("4"), limit = 0) }
            .isInstanceOf(PartitionLookupInvalidRequest::class.java)
        assertThatThrownBy { lookup.lookup(cat, "ingest", "nope", 1, listOf("4")) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }

    @Test
    fun `the partition lookup route serves the hoglite backend`() {
        val cat = fixture()
        val f = good(cat)
        append(cat, "events", f)
        val app = App.buildHoglite(cfg(), store, HealthProbe.forProbe(1_000) { client.reachable() })
        testApplication {
            application { app.module(this) }
            val resp =
                client.post("/v1/catalogs/$cat/namespaces/ingest/tables/events/partitions/lookup?spec_id=1") {
                    contentType(ContentType.Application.Json)
                    setBody("""["4"]""")
                }
            assertThat(resp.status).isEqualTo(HttpStatusCode.OK)
            assertThat(resp.bodyAsText()).contains(f.path)
        }
    }

    // ---- the hydrator's object-prefix check (#483) ----------------------------

    @Test
    fun `the hydrator quarantines an object-prefix file whose footer contradicts its prefix`() {
        val cat = fixture()
        val honest = putParquet(cat, under(cat, "events", "4") + "honest.parquet", team = 4)
        val liar = putParquet(cat, under(cat, "events", "4") + "liar.parquet", team = 5)
        val plain = putParquet(cat, root(cat, "plain") + "plain.parquet", team = 5)
        append(cat, "events", file(honest.first, "4", null, rows = 2, size = honest.second, footer = null))
        append(cat, "events", file(liar.first, "4", null, rows = 2, size = liar.second, footer = null))
        append(cat, "plain", file(plain.first, "4", null, rows = 2, size = plain.second, footer = null))
        HogliteHydrator(store, ObjectStore(cfg())).runOnce()

        fun state(path: String) =
            q(cat, "SELECT stats_state, prefix_quarantined FROM files WHERE path = ?", path).firstRow()!!.let {
                it[0].asString() to it[1].asLong()
            }
        assertThat(state(honest.first)).isEqualTo("provided" to 0L)
        assertThat(state(liar.first)).isEqualTo("provided" to 1L)
        // The option-off twin is never checked against a prefix.
        assertThat(state(plain.first)).isEqualTo("provided" to 0L)
        val run =
            HogliteMaintenanceStatus(store, 0, 0, 0, 0, 0, 1_000_000).runs(cat, null, null, 50).runs
                .first { it.task == com.posthog.hoglake.model.MaintenanceTask.HYDRATOR }
        assertThat(run.resultJson).contains("\"quarantined\":1")
    }

    // ---- compaction under each group's prefix (#491) --------------------------

    private fun compaction(
        beforeCommit: (String, com.posthog.hoglake.compaction.CompactionGroup) -> Unit = { _, _ -> },
    ) = HogliteCompaction(
        store,
        ObjectStore(cfg()),
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

    /** A real parquet file of two rows of team_id [team], committed with stats claiming [claim]. */
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
        // Pending (deferred stats): not a candidate, so value 6 forms no group.
        appendReal(cat, "c.parquet", 6)
        appendReal(cat, "d.parquet", 6, inlineStats = false)
        // Quarantined: not a candidate either.
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
        // Registered with inline stats claiming team 4; the rows are team 5.
        appendReal(cat, "a.parquet", team = 5, claim = 4)
        appendReal(cat, "b.parquet", team = 5, claim = 4)
        val before = store.getCatalog(cat).headSnapshotId
        val result = compaction().runOnce(cat, null, MaintenanceTrigger.MANUAL)
        assertThat(result.invalidData).isEqualTo(1L)
        assertThat(result.groupsCompacted).isEqualTo(0L)
        assertThat(store.getCatalog(cat).headSnapshotId).isEqualTo(before)
        assertThat(scalar(cat, "SELECT COUNT(*) FROM files WHERE end_snapshot IS NULL")).isEqualTo(2L)
        // The staged output stays queued for the cleanup drain.
        assertThat(
            scalar(cat, "SELECT COUNT(*) FROM removals WHERE reason = 'compaction_staging' AND drained_at IS NULL"),
        ).isEqualTo(1L)
    }

    @Test
    fun `compaction refuses groups whose prefix cannot be named, uncharged, before any IO`() {
        val cat = fixture()
        val a = appendReal(cat, "a.parquet", 4)
        val b = appendReal(cat, "b.parquet", 4)
        // A foreign row: a non-canonical value the commit would have refused.
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
        // The API cannot move a live layout (every route is refused), so the
        // move is a foreign write in the rewrite window: the layout now
        // names `v`, whose prefix the output is not under.
        val moved =
            compaction { c, _ ->
                rawWrite(c, "UPDATE table_layouts SET object_prefix_field_id = 2 WHERE end_snapshot IS NULL")
            }.runOnce(cat, null, MaintenanceTrigger.MANUAL)
        assertThat(moved.skippedConflicts).isEqualTo(1L)
        assertThat(moved.groupsCompacted).isEqualTo(0L)
        assertThat(scalar(cat, "SELECT COUNT(*) FROM files WHERE end_snapshot IS NULL")).isEqualTo(2L)

        // Option OFF at planning, a layout arriving in the window: the
        // root-path output is not under any prefix, so it is skipped too.
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

        // And with nothing moving, the same plain table compacts at its root.
        rawWrite(plain, "DELETE FROM table_layouts")
        val ok = compaction().runOnce(plain, null, MaintenanceTrigger.MANUAL)
        assertThat(ok.groupsCompacted).isEqualTo(1L)
        val out = q(plain, "SELECT path FROM files WHERE end_snapshot IS NULL").rows.single()[0].asString()
        assertThat(out).startsWith(root(plain, "plain")).doesNotContain("team_id=")
    }

    // ---- the tenant-schema migration -----------------------------------------

    @Test
    fun `a tenant created before the layout schema is migrated on its first operation`() {
        val cat = fixture()
        val pre = good(cat)
        append(cat, "events", pre)
        // Roll the tenant back to the pre-merge schema.
        rawWrite(
            cat,
            "DROP TABLE table_layouts",
            "ALTER TABLE files DROP COLUMN prefix_quarantined",
        )
        // A fresh process (store) migrates on the first operation it binds.
        val fresh = HogliteCatalogStore(client).apply { requireOperationBindingForTests = true }
        assertThat(fresh.getTable(cat, "ingest", "events").objectPrefixField).isNull()
        val probe =
            fresh.catalogOperation(cat) {
                HogliteSchema.TENANT_MIGRATIONS.map {
                        m ->
                    fresh.queryCatalog(cat, m.appliedProbe).firstRow()!![0].asLong()
                }
            }
        assertThat(probe).allMatch { it == 1L }
        assertThat(
            fresh.catalogOperation(cat) {
                fresh.queryCatalog(
                    cat,
                    "SELECT prefix_quarantined FROM files WHERE path = ?",
                    pre.path,
                ).firstRow()!![0].asLong()
            },
        ).isEqualTo(0L)
        // And the features work on the migrated tenant.
        HogliteAlters(fresh).alterTable(cat, "ingest", "plain", listOf(AlterOp.SetObjectPrefixField("team_id")))
        assertThat(fresh.getTable(cat, "ingest", "plain").objectPrefixField).isEqualTo("team_id")
    }

    // ---- parquet ---------------------------------------------------------------

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

    /** Two rows of team_id [team] at [path] (an s3://hoglake-it/ URI); returns (path, size). */
    private fun putParquet(
        cat: String,
        path: String,
        team: Long,
    ): Pair<String, Long> {
        check(path.startsWith("s3://hoglake-it/$cat/"))
        val tmp = Files.createTempFile("hoglite-prefix", ".parquet")
        val bytes =
            try {
                Files.delete(tmp)
                val factory = org.apache.parquet.example.data.simple.SimpleGroupFactory(parquetSchema)
                org.apache.parquet.hadoop.example.ExampleParquetWriter
                    .builder(org.apache.parquet.io.LocalOutputFile(tmp))
                    .withType(parquetSchema)
                    .withCompressionCodec(org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED)
                    .build()
                    .use { w ->
                        for (v in listOf(1L, 2L)) {
                            val g = factory.newGroup()
                            g.add("team_id", team)
                            g.add("v", v)
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
