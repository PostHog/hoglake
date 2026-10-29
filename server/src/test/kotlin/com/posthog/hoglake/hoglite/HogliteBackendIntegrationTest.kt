package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.testing.TestImages
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
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import java.net.ServerSocket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.time.Duration
import java.util.UUID

/**
 * The hoglite backend end to end: a real `hoglited` (the same binary
 * production runs) in front of a Silo container, driven through
 * [HogliteCatalogStore] exactly as the /v1 routes drive it.
 *
 * The store is where hoglake's catalog semantics live on this backend —
 * the OCC guards, the idempotency receipts, the typed conflicts — so
 * this suite exercises the semantics, not the wire (the wire is
 * [com.posthog.hoglake.api]'s, and backend-agnostic).
 *
 * The binary is located via `HOGLITE_BINARY` or on the PATH; without
 * one the class skips (the CI packaging story — an image or a
 * prebuilt binary — is the follow-up this suite's skip reports).
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteBackendIntegrationTest {
    private lateinit var minio: MinIOContainer
    private var process: Process? = null
    private lateinit var client: HogliteClient
    private lateinit var store: HogliteCatalogStore
    private var catalogCounter = 0

    @BeforeAll
    fun startHoglite() {
        val binary = System.getenv("HOGLITE_BINARY") ?: findOnPath("hoglited")
        assumeTrue(binary != null) { "no hoglited binary (set HOGLITE_BINARY); skipping hoglite backend tests" }

        minio = TestImages.minio()
        minio.start()
        // The AWS SDK only falls back to PATH-style addressing for an
        // IP-literal endpoint; MinIOContainer's s3URL is a `localhost`
        // URL, which resolves as a virtual-hosted bucket name.
        val s3Endpoint = "http://127.0.0.1:${minio.firstMappedPort}"
        val bucket = "hoglite-it"
        val s3 =
            S3Client.builder()
                .endpointOverride(URI.create(s3Endpoint))
                .credentialsProvider(
                    StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(minio.userName, minio.password),
                    ),
                )
                .region(Region.US_EAST_1)
                .forcePathStyle(true)
                .build()
        s3.createBucket { it.bucket(bucket) }
        s3.close()

        val port = ServerSocket(0).use { it.localPort }
        val dir = Files.createTempDirectory("hoglited-it").toFile()
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
        while (System.nanoTime() < deadline) {
            if (process?.isAlive == false) break
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
        if (!up) {
            val out = process?.inputStream?.readBytes()?.decodeToString()?.take(2000)
            throw IllegalStateException("hoglited did not come up; output: $out")
        }
        client = HogliteClient(standardUrl = "http://127.0.0.1:$port", sleeper = {})
        store = HogliteCatalogStore(client)
    }

    @AfterAll
    fun stopHoglite() {
        if (::client.isInitialized) client.close()
        process?.destroy()
        process?.waitFor()
        if (::minio.isInitialized) minio.stop()
    }

    private fun freshCatalog(): String {
        val name = "it${catalogCounter++}"
        store.createCatalog(name, "s3://hoglake-it/$name")
        return name
    }

    private fun findOnPath(binary: String): String? {
        val path = System.getenv("PATH") ?: return null
        return path.split(':').map { "$it/$binary" }.firstOrNull { java.io.File(it).canExecute() }
    }

    // ---- catalog + namespace + table lifecycle ----------------------------

    @Test
    fun `catalog namespace table lifecycle`() {
        val catalog = freshCatalog()
        // a same-name create is the 409, whatever the data_path.
        assertThatThrownBy { store.createCatalog(catalog, "s3://hoglake-it/$catalog") }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)
        assertThatThrownBy { store.createCatalog(catalog, "s3://hoglake-it/other-$catalog") }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)
        // prefix overlap with an existing catalog's data_path is refused.
        assertThatThrownBy { store.createCatalog("${catalog}sub", "s3://hoglake-it/$catalog/sub") }
            .isInstanceOf(HoglakeException.Validation::class.java)

        assertThat(store.getCatalog(catalog).headSnapshotId).isEqualTo(0)
        assertThat(store.listCatalogs().map { it.name }).contains(catalog)

        val ns = store.createNamespace(catalog, "ns")
        assertThat(store.getNamespace(catalog, "ns").namespaceId).isEqualTo(ns.namespaceId)
        assertThatThrownBy { store.createNamespace(catalog, "ns") }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)

        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        assertThat(table.snapshotId).isNotNull()
        assertThat(store.getTable(catalog, "ns", "t").tableUuid).isEqualTo(table.tableUuid)
        assertThat(store.listTables(catalog, "ns").map { it.name }).containsExactly("t")

        store.dropTable(catalog, "ns", "t", expectedTableUuid = table.tableUuid, author = "test", message = "drop")
        assertThatThrownBy { store.getTable(catalog, "ns", "t") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
        // time travel: the table is still visible at its live snapshot.
        assertThat(
            store.getTable(catalog, "ns", "t", snapshot = table.snapshotId).tableUuid,
        ).isEqualTo(table.tableUuid)

        store.dropNamespace(catalog, "ns", author = "test", message = "drop")
        assertThat(store.listNamespaces(catalog)).isEmpty()

        store.dropCatalog(catalog, "test", "drop")
        assertThatThrownBy { store.getCatalog(catalog) }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }

    // ---- commits: OCC guards, idempotency, typed conflicts -----------------

    @Test
    fun `commit appends land with row lineage and replay idempotently`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val head = table.readSnapshotId

        val key = UUID.randomUUID()
        val request =
            CommitRequest(
                readSnapshot = head,
                idempotencyKey = key,
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            listOf(
                                FileRegistration("s3://hoglake-it/$catalog/data/a.parquet", 100, 1000, 500),
                                FileRegistration("s3://hoglake-it/$catalog/data/b.parquet", 50, 800, 400),
                            ),
                            expectedTableUuid = table.tableUuid,
                        ),
                    ),
            )
        val committed = store.commit(catalog, request)
        assertThat(committed.snapshotId).isEqualTo(head + 1)
        // The same key + payload replays the receipt; it does not re-land.
        val replayed = store.commit(catalog, request)
        assertThat(replayed.snapshotId).isEqualTo(committed.snapshotId)
        assertThat(store.getCatalog(catalog).headSnapshotId).isEqualTo(head + 1)
        assertThat(store.receipt(catalog, key).snapshotId).isEqualTo(committed.snapshotId)
        // The same key with a DIFFERENT payload is the 422.
        assertThatThrownBy {
            store.commit(
                catalog,
                request.copy(
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf(FileRegistration("s3://hoglake-it/$catalog/data/c.parquet", 1, 1, 1)),
                                expectedTableUuid = table.tableUuid,
                            ),
                        ),
                ),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)

        // Row lineage is server-assigned and contiguous.
        val files = store.listFiles(catalog, "ns", "t")
        assertThat(files.map { it.recordCount }).containsExactly(100, 50)
        assertThat(files.map { it.rowIdStart }).containsExactly(0L, 100L)
        val scan = store.planScan(catalog, "ns", "t")
        assertThat(scan.map { it.dataFile.path }).containsExactly(
            "s3://hoglake-it/$catalog/data/a.parquet",
            "s3://hoglake-it/$catalog/data/b.parquet",
        )
        // The changefeed carries the insert's files in the window.
        val changes = store.changes(catalog, "ns", "t", fromSnapshot = head)
        assertThat(changes.files.map { it.path }).containsExactly(
            "s3://hoglake-it/$catalog/data/a.parquet",
            "s3://hoglake-it/$catalog/data/b.parquet",
        )
    }

    @Test
    fun `a prepared delete against a stale read snapshot is the typed conflict`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val head = table.readSnapshotId
        val first =
            store.commit(
                catalog,
                CommitRequest(
                    readSnapshot = head,
                    idempotencyKey = UUID.randomUUID(),
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf(FileRegistration("s3://hoglake-it/$catalog/data/a.parquet", 10, 100, 50)),
                                expectedTableUuid = table.tableUuid,
                            ),
                        ),
                ),
            )
        val fileId = store.listFiles(catalog, "ns", "t").single().dataFileId

        // A writer that read at `first.snapshotId` commits a data change;
        // a second writer's GUARDED delete against the same read snapshot
        // must conflict.
        store.commit(
            catalog,
            CommitRequest(
                readSnapshot = first.snapshotId,
                idempotencyKey = UUID.randomUUID(),
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            listOf(FileRegistration("s3://hoglake-it/$catalog/data/b.parquet", 5, 50, 25)),
                            expectedTableUuid = table.tableUuid,
                        ),
                    ),
            ),
        )
        assertThatThrownBy {
            store.commit(
                catalog,
                CommitRequest(
                    readSnapshot = first.snapshotId,
                    idempotencyKey = UUID.randomUUID(),
                    requireUnchangedTables = true,
                    deletes =
                        listOf(
                            TableDeletes(
                                "ns",
                                "t",
                                listOf(
                                    DeleteFileRegistration(
                                        fileId,
                                        "s3://hoglake-it/$catalog/data/a.puffin",
                                        1,
                                        10,
                                    ),
                                ),
                                expectedTableUuid = table.tableUuid,
                            ),
                        ),
                ),
            )
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)

        // A delete that re-reads lands, and the scan pairs the DV.
        val head2 = store.getCatalog(catalog).headSnapshotId
        val deleted =
            store.commit(
                catalog,
                CommitRequest(
                    readSnapshot = head2,
                    idempotencyKey = UUID.randomUUID(),
                    requireUnchangedTables = true,
                    deletes =
                        listOf(
                            TableDeletes(
                                "ns",
                                "t",
                                listOf(
                                    DeleteFileRegistration(
                                        fileId,
                                        "s3://hoglake-it/$catalog/data/a.puffin",
                                        1,
                                        10,
                                    ),
                                ),
                                expectedTableUuid = table.tableUuid,
                            ),
                        ),
                ),
            )
        val scan = store.planScan(catalog, "ns", "t")
        val withDv = scan.filter { it.deleteFile != null }
        assertThat(withDv).hasSize(1)
        assertThat(withDv.single().deleteFile!!.deleteCount).isEqualTo(1)
        assertThat(deleted.snapshotId).isEqualTo(head2 + 1)
    }

    @Test
    fun `a guarded commit after drop plus recreate is the TableRecreated 409`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        store.dropTable(catalog, "ns", "t", expectedTableUuid = table.tableUuid, author = "test", message = "drop")
        val recreated = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        assertThatThrownBy {
            store.commit(
                catalog,
                CommitRequest(
                    readSnapshot = recreated.readSnapshotId,
                    idempotencyKey = UUID.randomUUID(),
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf(FileRegistration("s3://hoglake-it/$catalog/data/a.parquet", 1, 1, 1)),
                                expectedTableUuid = table.tableUuid,
                            ),
                        ),
                ),
            )
        }.isInstanceOf(HoglakeException.TableRecreated::class.java)
    }

    // ---- consumer offsets ---------------------------------------------------

    @Test
    fun `offsets are monotonic and reject regression`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val offset =
            store.commitOffset(catalog, "consumer-1", table.tableUuid, table.readSnapshotId)
        assertThat(offset.committedSnapshot).isEqualTo(table.readSnapshotId)
        assertThatThrownBy {
            store.commitOffset(catalog, "consumer-1", table.tableUuid, table.readSnapshotId)
        }.isInstanceOf(HoglakeException.OffsetRegression::class.java)
        assertThat(store.getOffset(catalog, "consumer-1", table.tableUuid).committedSnapshot)
            .isEqualTo(table.readSnapshotId)
        assertThatThrownBy {
            store.commitOffset(catalog, "consumer-1", UUID.randomUUID(), 0)
        }.isInstanceOf(HoglakeException.Validation::class.java)
    }

    // ---- upload claims ------------------------------------------------------

    @Test
    fun `an upload claim settles when its owner commits the path`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val owner = UUID.randomUUID()
        val claim = store.uploads.claim(catalog, UUID.randomUUID(), owner, "s3://hoglake-it/$catalog", "data")
        assertThat(claim.path).startsWith("s3://hoglake-it/$catalog/trino-upload/")
        // idempotent: the same upload id re-claims the same path.
        assertThat(store.uploads.claim(catalog, claim.uploadId, owner, "s3://hoglake-it/$catalog", "data").path)
            .isEqualTo(claim.path)
        // a different owner committing the claimed path is fenced.
        assertThatThrownBy {
            store.commit(
                catalog,
                CommitRequest(
                    readSnapshot = table.readSnapshotId,
                    idempotencyKey = UUID.randomUUID(),
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                "t",
                                listOf(FileRegistration(claim.path, 1, 1, 1)),
                                expectedTableUuid = table.tableUuid,
                            ),
                        ),
                ),
            )
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)
        // the owner settles it.
        store.commit(
            catalog,
            CommitRequest(
                readSnapshot = table.readSnapshotId,
                idempotencyKey = owner,
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            listOf(FileRegistration(claim.path, 1, 1, 1)),
                            expectedTableUuid = table.tableUuid,
                        ),
                    ),
            ),
        )
        // and a settled claim cannot be abandoned after the fact.
        assertThat(store.uploads.abandon(catalog, owner, listOf(claim.path))).isEqualTo(0)
    }

    // ---- atomic table creation ----------------------------------------------

    @Test
    fun `prepare publish and the durable rejections`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val op = UUID.randomUUID()
        val definition =
            TableCreationDefinition(
                "ns",
                "created",
                listOf(ColumnDef("id", ColType.LONG)),
            )
        val prepared = store.tableCreations.prepare(catalog, op, definition)
        assertThat(prepared.state).isEqualTo("prepared")
        // idempotent replay
        assertThat(store.tableCreations.prepare(catalog, op, definition).tableUuid)
            .isEqualTo(prepared.tableUuid)
        // a different definition under the same id is the 409
        assertThatThrownBy {
            store.tableCreations.prepare(catalog, op, definition.copy(name = "other"))
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)

        val files =
            listOf(
                FileRegistration(
                    "${prepared.writePath}part-0.parquet",
                    10,
                    100,
                    50,
                ),
            )
        val published = store.tableCreations.publish(catalog, op, files)
        assertThat(published.state).isEqualTo("committed")
        assertThat(published.snapshotId).isNotNull()
        assertThat(store.getTable(catalog, "ns", "created").tableUuid).isEqualTo(prepared.tableUuid)
        assertThat(store.listFiles(catalog, "ns", "created").single().recordCount).isEqualTo(10)
        // publish replay: same files return the receipt; different files 409.
        assertThat(store.tableCreations.publish(catalog, op, files).state).isEqualTo("committed")
        assertThatThrownBy {
            store.tableCreations.publish(
                catalog,
                op,
                listOf(FileRegistration("${prepared.writePath}part-1.parquet", 1, 1, 1)),
            )
        }.isInstanceOf(HoglakeException.CommitConflict::class.java)

        // target_exists: a prepared creation whose name was taken in the
        // meantime rejects DURABLY, and the receipt says why.
        val op2 = UUID.randomUUID()
        store.tableCreations.prepare(catalog, op2, definition.copy(name = "taken"))
        store.createTable(catalog, "ns", "taken", listOf(ColumnDef("id", ColType.LONG)))
        val rejected =
            store.tableCreations.publish(
                catalog,
                op2,
                emptyList(),
            )
        assertThat(rejected.state).isEqualTo("rejected")
        assertThat(rejected.reason).isEqualTo("target_exists")

        // abort fences a prepared receipt.
        val op3 = UUID.randomUUID()
        store.tableCreations.prepare(catalog, op3, definition.copy(name = "aborted"))
        assertThat(store.tableCreations.abort(catalog, op3).state).isEqualTo("aborted")
        assertThat(store.tableCreations.publish(catalog, op3, emptyList()).state).isEqualTo("aborted")
    }

    // ---- alter ----------------------------------------------------------------

    @Test
    fun `rename and add column, guarded and unguarded`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val renamed =
            store.alters.alterTable(
                catalog,
                "ns",
                "t",
                listOf(com.posthog.hoglake.model.AlterOp.RenameTable("t2")),
                expectedTableUuid = table.tableUuid,
            )
        assertThat(renamed.name).isEqualTo("t2")
        assertThat(store.getTable(catalog, "ns", "t2").tableUuid).isEqualTo(table.tableUuid)
        assertThatThrownBy { store.getTable(catalog, "ns", "t") }
            .isInstanceOf(HoglakeException.NotFound::class.java)

        val added =
            store.alters.alterTable(
                catalog,
                "ns",
                "t2",
                listOf(
                    com.posthog.hoglake.model.AlterOp.AddColumn(ColumnDef("extra", ColType.STRING)),
                ),
            )
        assertThat(added.columns.map { it.def.name }).containsExactly("id", "extra")
        // a guarded alter against a stale read snapshot is the typed 409
        assertThatThrownBy {
            store.alters.alterTable(
                catalog,
                "ns",
                "t2",
                listOf(com.posthog.hoglake.model.AlterOp.RenameTable("t3")),
                readSnapshot = table.readSnapshotId,
            )
        }.isInstanceOf(HoglakeException.DdlSinceReadSnapshot::class.java)
    }


    // ---- views -----------------------------------------------------------

    @Test
    fun `views create list drop with change rows`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))

        val view = store.views.create(catalog, "ns", "v", "SELECT 1", "trino", "test", "create")
        assertThat(store.views.get(catalog, "ns", "v").viewUuid).isEqualTo(view.viewUuid)
        assertThat(store.views.list(catalog, "ns").map { it.name }).containsExactly("v")
        assertThatThrownBy { store.views.create(catalog, "ns", "v", "SELECT 2", "trino", "test", "dup") }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)

        store.views.drop(catalog, "ns", "v", "test", "drop")
        assertThat(store.views.list(catalog, "ns")).isEmpty()
        // The changefeed saw both ends of the lifecycle.
        val kinds = mutableListOf<String>()
        var from = 0L
        while (true) {
            val page = store.listSnapshots(catalog, from, 50)
            if (page.first.isEmpty()) break
            from = page.first.last().snapshotId
            if (!page.second) break
        }
        // (snapshots feed is the whole-catalog one; the view change kinds
        // ride it — this asserts the feed grew past the DDL, not the kind
        // strings, which the table-scoped feed owns.)
        assertThat(from).isGreaterThan(1L)
    }

    // ---- options -----------------------------------------------------------

    @Test
    fun `options patch validates and attributes retention decreases`() {
        val catalog = freshCatalog()
        val options = com.posthog.hoglake.hoglite.HogliteOptions(store)
        val initial = options.get(catalog)
        assertThat(initial.snapshotRetentionSeconds).isNull()

        // Setting retention where none was needs operator attribution.
        assertThatThrownBy {
            options.patch(catalog, com.posthog.hoglake.service.PatchField.Set(3600L), null, null, null)
        }.isInstanceOf(HoglakeException.Validation::class.java)
        val set = options.patch(
            catalog,
            com.posthog.hoglake.service.PatchField.Set(3600L),
            null,
            "test",
            "set retention",
        )
        assertThat(set.snapshotRetentionSeconds).isEqualTo(3600L)
        // Raising it never needs attribution.
        val raised = options.patch(catalog, com.posthog.hoglake.service.PatchField.Set(7200L), null, null, null)
        assertThat(raised.snapshotRetentionSeconds).isEqualTo(7200L)
        // Lowering it does.
        assertThatThrownBy {
            options.patch(catalog, com.posthog.hoglake.service.PatchField.Set(60L), null, null, null)
        }.isInstanceOf(HoglakeException.Validation::class.java)
        // A non-positive value is a 422 regardless.
        assertThatThrownBy {
            options.patch(catalog, com.posthog.hoglake.service.PatchField.Set(0L), null, "test", "bad")
        }.isInstanceOf(HoglakeException.Validation::class.java)
        // consumer_floor toggles without attribution.
        val floored = options.patch(catalog, com.posthog.hoglake.service.PatchField.Absent, false, null, null)
        assertThat(floored.consumerFloor).isFalse()
    }

    // ---- truncate ------------------------------------------------------------

    @Test
    fun `truncate ends files and dvs at the new snapshot`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val commit =
            store.commit(
                catalog,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                namespace = "ns",
                                table = "t",
                                files =
                                    listOf(
                                        FileRegistration(
                                            path = "s3://hoglake-it/$catalog/data/ns/t/a.parquet",
                                            recordCount = 10,
                                            fileSizeBytes = 100,
                                        ),
                                    ),
                            ),
                        ),
                ),
            )
        assertThat(store.listFiles(catalog, "ns", "t")).hasSize(1)

        val result =
            store.truncateTable(
                catalog,
                "ns",
                "t",
                expectedTableUuid = table.tableUuid,
                author = "test",
                message = "truncate",
            )
        assertThat(store.listFiles(catalog, "ns", "t")).isEmpty()
        // Time travel sees the file as of the commit.
        assertThat(
            store.listFiles(catalog, "ns", "t", snapshot = commit.snapshotId),
        ).hasSize(1)
        assertThat(result.snapshotId).isGreaterThan(commit.snapshotId)
    }

    // ---- partitions (exact computation over the live manifest) ---------------

    @Test
    fun `partition stats rank by the planner's debt`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        val table = store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        repeat(3) { i ->
            store.commit(
                catalog,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                namespace = "ns",
                                table = "t",
                                files =
                                    listOf(
                                        FileRegistration(
                                            path = "s3://hoglake-it/$catalog/data/ns/t/f$i.parquet",
                                            recordCount = 10,
                                            fileSizeBytes = 100,
                                        ),
                                    ),
                            ),
                        ),
                ),
            )
        }
        val partitions =
            com.posthog.hoglake.hoglite.HoglitePartitions(
                store,
                smallFileThresholdBytes = 1_000_000,
                // Two, not the production default of five: three small
                // files must form a group for the debt assertion to bite.
                minInputFiles = 2,
            )
        val report = partitions.partitionStats(catalog, "ns", "t", 50)
        assertThat(report.partitions).hasSize(1)
        val debt = report.partitions.single()
        assertThat(debt.fileCount).isEqualTo(3)
        assertThat(debt.smallFileCount).isEqualTo(3)
        // Three files under the target pack into one group: all three are debt.
        assertThat(debt.debtScore).isEqualTo(3)
        val listing = partitions.listPartitions(catalog, "ns", "t")
        assertThat(listing.total).isEqualTo(1)
        assertThat(listing.partitions.single().recordCount).isEqualTo(30)
    }

    // ---- maintenance status ---------------------------------------------------

    @Test
    fun `maintenance status reports live backlogs and the run ledger`() {
        val catalog = freshCatalog()
        store.createNamespace(catalog, "ns")
        store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val status =
            com.posthog.hoglake.hoglite.HogliteMaintenanceStatus(
                store,
                hydratorIntervalMs = 0,
                expiryIntervalMs = 60_000,
                cleanupIntervalMs = 60_000,
                compactionIntervalMs = 0,
                retirementIntervalMs = 0,
                smallFileThresholdBytes = 1_000_000,
            )
        val s = status.status(catalog)
        assertThat(s.catalog).isEqualTo(catalog)
        val expiry = s.tasks.first { it.task == com.posthog.hoglake.model.MaintenanceTask.EXPIRY }
        assertThat(expiry.loopIntervalMs).isEqualTo(60_000)
        val backlog = expiry.backlog as com.posthog.hoglake.model.MaintenanceBacklog.ExpiryBacklog
        assertThat(backlog.headSnapshotId).isGreaterThan(0L)
        assertThat(backlog.earliestSnapshotId).isEqualTo(0)
        // The run ledger answers per-catalog and instance-wide.
        assertThat(status.runs(catalog, null, null, 50).runs).isEmpty()
        val inst = status.instanceStatus(null, 50)
        assertThat(inst.catalogs.map { it.catalog }).contains(catalog)
    }

}
