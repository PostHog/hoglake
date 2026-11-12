package com.posthog.hoglake.storage

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.compaction.CompactionConfig
import com.posthog.hoglake.compaction.CompactionService
import com.posthog.hoglake.hydrator.Hydrator
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.CleanupService
import com.posthog.hoglake.service.RemovalStore
import com.posthog.hoglake.stats.IcebergSingleValue
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.TestImages
import com.sun.net.httpserver.HttpServer
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.testcontainers.containers.MinIOContainer
import java.net.InetSocketAddress
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/**
 * The server's three S3-touching jobs under `HOGLAKE_STORAGE_PROVIDER=broker`,
 * against a real MinIO and a fake broker that vends MinIO's keys as the
 * session for whatever catalog is asked for. What this proves is the
 * plumbing: every read and write of hydration, compaction and cleanup
 * goes through a store scoped to the catalog the job is working, the
 * broker is asked for exactly that catalog's prefix at the access the
 * job needs, and the base store — the one with no catalog — does no IO
 * at all.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StorageProviderIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val jdbi get() = db.jdbi
    private val catalogs = CatalogService(jdbi)
    private val commits = CommitService(jdbi)
    private val counter = AtomicInteger(0)

    private class Vend(val catalog: String, val prefix: String, val access: String)

    /** The fake broker: MinIO's root keys, for any catalog, as a session that outlives the test. */
    private class FakeBroker(minio: MinIOContainer) : AutoCloseable {
        private val json = ObjectMapper()
        val vends = CopyOnWriteArrayList<Vend>()
        val tokens = AtomicInteger(0)

        val server: HttpServer =
            HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                createContext("/oauth/token") { ex ->
                    ex.requestBody.readAllBytes()
                    tokens.incrementAndGet()
                    reply(ex, """{"access_token":"tok","token_type":"Bearer","expires_in":3600}""")
                }
                createContext("/storage") { ex ->
                    val body = json.readTree(ex.requestBody.readAllBytes())
                    vends +=
                        Vend(
                            body.get("catalog").asText(),
                            body.get("prefix").asText(),
                            body.get("access").asText(),
                        )
                    reply(
                        ex,
                        json.writeValueAsString(
                            mapOf(
                                "access_key_id" to minio.userName,
                                "secret_access_key" to minio.password,
                                "expiration" to "2099-01-01T00:00:00Z",
                            ),
                        ),
                    )
                }
                start()
            }

        private fun reply(
            ex: com.sun.net.httpserver.HttpExchange,
            body: String,
        ) {
            val bytes = body.toByteArray(StandardCharsets.UTF_8)
            ex.responseHeaders.add("Content-Type", "application/json")
            ex.sendResponseHeaders(200, bytes.size.toLong())
            ex.responseBody.use { it.write(bytes) }
        }

        fun url(path: String): URI = URI.create("http://127.0.0.1:${server.address.port}$path")

        override fun close() = server.stop(0)
    }

    private companion object {
        const val BUCKET = "hoglake-storage-provider"

        val minio: MinIOContainer by lazy { TestImages.minio().also { it.start() } }

        /** Bootstrap and inspection side: static keys, outside the provider under test. */
        val seed: ObjectStore by lazy {
            ObjectStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            ).also { it.createBucket(BUCKET) }
        }
        val seedRemovals: RemovalStore by lazy {
            RemovalStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            )
        }

        val broker: FakeBroker by lazy { FakeBroker(minio) }

        val provider: BrokerProvider by lazy {
            BrokerProvider(
                brokerUrl = broker.url("/storage"),
                tokenUrl = broker.url("/oauth/token"),
                clientId = "hoglake",
                clientSecret = "secret",
                audience = "hoglake",
                refreshMargin = Duration.ofMinutes(5),
            )
        }

        /** The stores under test: built on the broker, with no keys of their own. */
        val objects: ObjectStore by lazy { ObjectStore(minio.s3URL, "us-east-1", true, provider) }
        val removals: RemovalStore by lazy { RemovalStore(minio.s3URL, "us-east-1", true, provider) }

        val schema: MessageType =
            Types.buildMessage()
                .addField(Types.required(PrimitiveTypeName.INT64).id(1).named("id"))
                .addField(
                    Types.optional(PrimitiveTypeName.BINARY)
                        .`as`(LogicalTypeAnnotation.stringType()).id(2).named("name"),
                )
                .named("t")
    }

    @AfterAll
    fun tearDown() {
        objects.close()
        removals.close()
        broker.close()
        db.close()
    }

    private fun parquetBytes(ids: LongRange): ByteArray {
        val tmp = Files.createTempFile("storage-provider", ".parquet")
        try {
            Files.delete(tmp)
            val factory = SimpleGroupFactory(schema)
            ExampleParquetWriter.builder(LocalOutputFile(tmp))
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
                .use { w ->
                    for (id in ids) {
                        w.write(factory.newGroup().append("id", id).append("name", "row-$id"))
                    }
                }
            return Files.readAllBytes(tmp)
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    private fun stats(ids: LongRange): List<ColumnStats> {
        val n = ids.count().toLong()
        return listOf(
            ColumnStats(
                fieldId = 1,
                valueCount = n,
                nullCount = 0,
                nanCount = null,
                sizeBytes = 40,
                lowerBound = IcebergSingleValue.encodeLong(ids.first),
                upperBound = IcebergSingleValue.encodeLong(ids.last),
            ),
            ColumnStats(
                fieldId = 2,
                valueCount = n,
                nullCount = 0,
                nanCount = null,
                sizeBytes = 40,
                lowerBound = IcebergSingleValue.encodeString("row-${ids.first}"),
                upperBound = IcebergSingleValue.encodeString("row-${ids.last}"),
            ),
        )
    }

    /** A catalog with `ns.t` and [files] appended, each written to MinIO with the seed store. */
    private fun catalogWithFiles(
        files: List<LongRange>,
        withStats: Boolean,
    ): Pair<String, List<String>> {
        val cat = "broker-${counter.incrementAndGet()}"
        catalogs.createCatalog(cat, "s3://$BUCKET/$cat")
        catalogs.createNamespace(cat, "ns")
        catalogs.createTable(
            cat,
            "ns",
            "t",
            listOf(ColumnDef("id", ColType.LONG, nullable = false), ColumnDef("name", ColType.STRING)),
        )
        val regs =
            files.mapIndexed { i, ids ->
                val bytes = parquetBytes(ids)
                val path = "s3://$BUCKET/$cat/data/ns/t/f$i.parquet"
                seed.put(path, bytes)
                FileRegistration(
                    path = path,
                    recordCount = ids.count().toLong(),
                    fileSizeBytes = bytes.size.toLong(),
                    columnStats = if (withStats) stats(ids) else null,
                )
            }
        commits.commit(cat, CommitRequest(appends = listOf(TableAppend("ns", "t", regs))))
        return cat to regs.map { it.path }
    }

    private fun vendsFor(cat: String) = broker.vends.filter { it.catalog == cat }

    @Test
    fun `the base store does no IO under broker`() {
        assertThat(objects.scoped).isTrue()
        assertThat(removals.scoped).isTrue()
        assertThatThrownBy { objects.get("s3://$BUCKET/nothing.parquet") }
            .isInstanceOf(StorageCredentialException::class.java)
            .hasMessageContaining("forCatalog")
        assertThatThrownBy { removals.exists("s3://$BUCKET/nothing.parquet") }
            .isInstanceOf(StorageCredentialException::class.java)
            .hasMessageContaining("forCatalog")
    }

    @Test
    fun `hydration reads each catalog's files with a read session for that catalog`() {
        val (cat, paths) = catalogWithFiles(listOf(0L..9L, 10L..19L), withStats = false)
        val before = broker.vends.size

        val hydrated = Hydrator(jdbi, objects).runOnce()

        assertThat(hydrated).isEqualTo(2)
        assertThat(catalogs.listFiles(cat, "ns", "t").map { it.statsState.wire }).containsOnly("provided")
        assertThat(paths).hasSize(2)
        val vends = vendsFor(cat)
        assertThat(vends).describedAs("one read session for the catalog, both files").hasSize(1)
        assertThat(vends.single().access).isEqualTo("read")
        assertThat(vends.single().prefix).isEqualTo("s3://$BUCKET/$cat")
        assertThat(broker.vends.size - before).isEqualTo(1)
    }

    @Test
    fun `compaction rewrites a group with a write session for the catalog`() {
        val (cat, inputs) = catalogWithFiles(listOf(0L..4L, 5L..9L, 10L..14L), withStats = true)
        val cfg = CompactionConfig(targetBytes = 65536, minInputFiles = 2, maxGroupsPerRun = 10)

        val result = CompactionService(jdbi, objects, cfg).runOnce(cat, cfg)

        assertThat(result.groupsCompacted).isEqualTo(1)
        assertThat(result.filesIn).isEqualTo(3)
        val output = catalogs.listFiles(cat, "ns", "t").single()
        assertThat(output.path).isNotIn(inputs)
        assertThat(seed.get(output.path)).describedAs("the output is really there").isNotEmpty()
        val vends = vendsFor(cat)
        assertThat(vends.map { it.access }).containsOnly("write")
        assertThat(vends.map { it.prefix }).containsOnly("s3://$BUCKET/$cat")
        assertThat(vends).describedAs("one write session per catalog, cached across groups").hasSize(1)
    }

    @Test
    fun `cleanup deletes with a write session for the catalog`() {
        val cat = "cleanup-${counter.incrementAndGet()}"
        val catalogId =
            jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path) VALUES (:name, :path) RETURNING catalog_id",
                ).bind("name", cat).bind("path", "s3://$BUCKET/$cat").mapTo(Long::class.java).one()
            }
        val gone = "s3://$BUCKET/$cat/data/ns/t/old.parquet"
        seed.put(gone, byteArrayOf(1, 2, 3))
        jdbi.withHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                VALUES (:catalogId, :path, 'data', 'snapshot_expiry')
                """,
            ).bind("catalogId", catalogId).bind("path", gone).execute()
        }

        val result = CleanupService(jdbi, removals).runOnce(cat, 100, MaintenanceTrigger.MANUAL)

        assertThat(result.removed).isEqualTo(1)
        assertThat(seedRemovals.exists(gone)).isFalse()
        val vends = vendsFor(cat)
        assertThat(vends).hasSize(1)
        assertThat(vends.single().access).isEqualTo("write")
        assertThat(vends.single().prefix).isEqualTo("s3://$BUCKET/$cat")
        // The token was exchanged once for the whole provider, not per vend.
        assertThat(broker.tokens.get()).isEqualTo(1)
    }
}
