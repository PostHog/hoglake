package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.Config
import com.posthog.hoglake.DatabaseBackend
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.testing.TestImages
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
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
import java.net.InetSocketAddress
import java.net.ServerSocket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue

/**
 * The hoglite store against a real `hoglited`, through a relay that can
 * replace the engine's answer to a `/tx` AFTER forwarding it. That is the
 * condition the outcome taxonomy exists for: the engine applied (and here
 * durably committed) the transaction, and the answer the client gets says
 * the outcome is unknown, or is an old engine's unlabelled 422.
 *
 * The engine's own ambiguous paths (fail-stop after a heartbeat gap,
 * lease generation move, a segment that landed but reported an error,
 * checkpoint failure after apply) are driven for real in the engine's
 * outcome_e2e_test.go, which asserts they answer exactly the 504 body
 * this relay serves. This suite proves the hoglake side of the
 * contract: such an answer never becomes a Validation (final) refusal,
 * the client's retries reuse the txid, and the retry lands exactly once.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteOutcomeUnknownIntegrationTest {
    private lateinit var minio: MinIOContainer
    private var process: Process? = null
    private lateinit var relay: HttpServer
    private lateinit var engineUrl: String
    private lateinit var s3Endpoint: String
    private var catalogCounter = 0
    private val json = jacksonObjectMapper()
    private val upstream = HttpClient.newHttpClient()

    /** One forwarded `/tx`: its txid and the status the ENGINE answered. */
    private data class Forwarded(val txid: String, val engineStatus: Int)

    private val forwardedTx = ConcurrentLinkedQueue<Forwarded>()

    /** Every `/tx` attempt the relay saw, forwarded or not, per txid. */
    private val attempts = java.util.concurrent.ConcurrentHashMap<String, Int>()

    /**
     * Decides a `/tx`'s fate: null passes it through untouched; otherwise
     * (forward first?, status, body) for the client.
     */
    @Volatile
    private var txRule: ((txid: String, attempt: Int) -> Triple<Boolean, Int, String>?)? = null

    @BeforeAll
    fun start() {
        val binary = System.getenv("HOGLITE_BINARY") ?: findOnPath("hoglited")
        assumeTrue(binary != null) { "no hoglited binary (set HOGLITE_BINARY); skipping" }
        minio = TestImages.minio()
        minio.start()
        s3Endpoint = "http://127.0.0.1:${minio.firstMappedPort}"
        val bucket = BUCKET
        s3().use { it.createBucket { b -> b.bucket(bucket) } }

        val port = ServerSocket(0).use { it.localPort }
        val dir = Files.createTempDirectory("hoglited-ou").toFile()
        val builder =
            ProcessBuilder(
                binary, "-addr", "127.0.0.1:$port", "-dir", dir.absolutePath,
                "-bucket", bucket, "-id", "ou-1", "-class", "standard",
            ).redirectErrorStream(true).redirectOutput(ProcessBuilder.Redirect.DISCARD)
        builder.environment().putAll(
            mapOf(
                "AWS_ENDPOINT_URL" to s3Endpoint,
                "AWS_ACCESS_KEY_ID" to minio.userName,
                "AWS_SECRET_ACCESS_KEY" to minio.password,
                "AWS_REGION" to "us-east-1",
            ),
        )
        process = builder.start()
        engineUrl = "http://127.0.0.1:$port"
        val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos()
        while (true) {
            val up =
                runCatching {
                    upstream.send(
                        HttpRequest.newBuilder(URI.create("$engineUrl/v1/healthz")).build(),
                        HttpResponse.BodyHandlers.discarding(),
                    ).statusCode() == 200
                }.getOrDefault(false)
            if (up) break
            check(System.nanoTime() < deadline && process!!.isAlive) { "hoglited did not come up" }
            Thread.sleep(100)
        }

        relay = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        relay.createContext("/") { ex ->
            val body = ex.requestBody.readBytes()
            val isTx = ex.requestMethod == "POST" && ex.requestURI.path.endsWith("/tx")
            val rule = if (isTx) txRule else null
            val txid = if (isTx) json.readValue<Map<String, Any?>>(body)["txid"] as String else ""
            val decision = rule?.invoke(txid, attempts.merge(txid, 1, Int::plus)!! - 1)
            var status: Int
            var answer: ByteArray
            if (decision == null || decision.first) {
                val req =
                    HttpRequest.newBuilder(URI.create(engineUrl + ex.requestURI))
                        .method(ex.requestMethod, HttpRequest.BodyPublishers.ofByteArray(body))
                        .header("Content-Type", "application/json")
                        .build()
                val resp = upstream.send(req, HttpResponse.BodyHandlers.ofByteArray())
                status = resp.statusCode()
                answer = resp.body()
                if (isTx) forwardedTx += Forwarded(txid, status)
            } else {
                status = 0
                answer = ByteArray(0)
            }
            if (decision != null) {
                status = decision.second
                answer = decision.third.toByteArray()
            }
            ex.sendResponseHeaders(status, if (answer.isEmpty()) -1 else answer.size.toLong())
            ex.responseBody.use { it.write(answer) }
        }
        relay.start()
    }

    @AfterEach
    fun reset() {
        txRule = null
        forwardedTx.clear()
        attempts.clear()
    }

    @AfterAll
    fun stop() {
        if (::relay.isInitialized) relay.stop(0)
        process?.destroy()
        process?.waitFor()
        if (::minio.isInitialized) minio.stop()
    }

    private fun store(budgetMs: Long = 8_000): HogliteCatalogStore =
        HogliteCatalogStore(
            HogliteClient(
                standardUrl = "http://127.0.0.1:${relay.address.port}",
                retryBudgetMs = budgetMs,
                sleeper = {},
            ),
        ).apply { requireOperationBindingForTests = true }

    private fun freshCatalog(store: HogliteCatalogStore): String {
        val name = "ou${catalogCounter++}"
        store.createCatalog(name, "s3://$BUCKET/$name")
        // the assertions count the test's own transactions, not the setup's
        forwardedTx.clear()
        attempts.clear()
        return name
    }

    private fun namespaces(
        store: HogliteCatalogStore,
        catalog: String,
    ): List<String> = store.listNamespaces(catalog, null, null).map { it.name }

    // The engine's exact answers (outcome.go / server.go).
    private val outcomeUnknown =
        """{"error":"commit outcome unknown (applied locally, durability unconfirmed): """ +
            """pod lease write deadline passed","code":"outcome_unknown","owner":"ou-1"}"""
    private val unlabelled422 = """{"error":"pod lease write deadline passed"}"""
    private val refused422 = """{"error":"UNIQUE constraint failed: namespaces.name","code":"refused"}"""
    private val notApplied503 = """{"error":"lease lost or expiring","code":"not_applied","owner":"ou-1"}"""

    @Test
    fun `a commit applied and answered 504 is retried with the same txid and lands once`() {
        val store = store()
        val catalog = freshCatalog(store)
        // The first attempt reaches the engine and commits; its answer is
        // replaced by outcome_unknown. The retry passes through.
        txRule = { _, attempt -> if (attempt == 0) Triple(true, 504, outcomeUnknown) else null }

        store.createNamespace(catalog, "ns", null, null)

        val attempts = forwardedTx.toList()
        assertThat(attempts).hasSize(2)
        assertThat(attempts.map { it.txid }.distinct()).hasSize(1)
        assertThat(attempts.map { it.engineStatus }).containsExactly(200, 200)
        assertThat(namespaces(store, catalog)).containsExactly("ns")
    }

    @Test
    fun `a commit whose outcome stays unknown surfaces as retryable unknown, never Validation`() {
        val store = store(budgetMs = 1_500)
        val catalog = freshCatalog(store)
        txRule = { _, _ -> Triple(true, 504, outcomeUnknown) }

        val thrown = catchThrowable { store.createNamespace(catalog, "ns", null, null) }

        assertThat(thrown)
            .isInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
            .isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            .isNotInstanceOf(HoglakeException.Validation::class.java)
        // every attempt carried the one txid, and the engine applied it
        // exactly once (the retries were dedup hits)
        assertThat(forwardedTx.map { it.txid }.distinct()).hasSize(1)
        assertThat((thrown as HoglakeException.CommitOutcomeUnknown).txid).isEqualTo(forwardedTx.first().txid)
        txRule = null
        assertThat(namespaces(store, catalog)).containsExactly("ns")
    }

    @Test
    fun `an unlabelled 422 after the engine applied is unknown, not Validation`() {
        val store = store()
        val catalog = freshCatalog(store)
        txRule = { _, _ -> Triple(true, 422, unlabelled422) }

        val thrown = catchThrowable { store.createNamespace(catalog, "ns", null, null) }

        assertThat(thrown)
            .isInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
            .isNotInstanceOf(HoglakeException.Validation::class.java)
        assertThat(forwardedTx).hasSize(1)
        txRule = null
        // it had committed: surfacing it as a refusal would have lied
        assertThat(namespaces(store, catalog)).containsExactly("ns")
    }

    @Test
    fun `a labelled refusal stays a final Validation`() {
        val store = store()
        val catalog = freshCatalog(store)
        txRule = { _, _ -> Triple(false, 422, refused422) }

        val thrown = catchThrowable { store.createNamespace(catalog, "ns", null, null) }

        assertThat(thrown).isInstanceOf(HoglakeException.Validation::class.java)
        txRule = null
        assertThat(namespaces(store, catalog)).isEmpty()
    }

    @Test
    fun `a budget spent only on not_applied answers is unavailable, not unknown`() {
        val store = store(budgetMs = 1_000)
        val catalog = freshCatalog(store)
        txRule = { _, _ -> Triple(false, 503, notApplied503) }

        val thrown = catchThrowable { store.createNamespace(catalog, "ns", null, null) }

        assertThat(thrown)
            .isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            .isNotInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
        txRule = null
        assertThat(namespaces(store, catalog)).isEmpty()
    }

    @Test
    fun `a hydrate whose commit stays unknown leaves the file pending, not failed`() {
        val store = store(budgetMs = 1_000)
        val catalog = freshCatalog(store)
        store.createNamespace(catalog, "ns", null, null)
        store.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val bytes = parquetBytes(listOf(1L, 2L))
        s3().use { s3c ->
            s3c.putObject(
                { it.bucket(BUCKET).key("$catalog/data/f.parquet") },
                software.amazon.awssdk.core.sync.RequestBody.fromBytes(bytes),
            )
        }
        store.commit(
            catalog,
            CommitRequest(
                idempotencyKey = UUID.randomUUID(),
                appends =
                    listOf(
                        TableAppend(
                            "ns",
                            "t",
                            listOf(FileRegistration("s3://$BUCKET/$catalog/data/f.parquet", 2, bytes.size.toLong())),
                        ),
                    ),
            ),
        )

        fun state() =
            store.catalogOperation(catalog) {
                store.queryCatalog(catalog, "SELECT stats_state FROM files").rows.single()[0].asString()
            }
        assertThat(state()).isEqualTo("pending")
        val objects =
            com.posthog.hoglake.hydrator.ObjectStore(
                Config(
                    database = DatabaseBackend.HOGLITE,
                    hogliteUrl = "http://127.0.0.1:1",
                    s3Endpoint = s3Endpoint,
                    s3Region = "us-east-1",
                    s3AccessKey = minio.userName,
                    s3SecretKey = minio.password,
                    s3PathStyle = true,
                ),
            )
        // The hydrate flip's every attempt answers outcome_unknown without
        // reaching the engine; any other tx (markFailed included) passes.
        txRule = { txid, _ -> if (txid.startsWith("hydrate:")) Triple(false, 504, outcomeUnknown) else null }
        HogliteHydrator(store, objects).runOnce()
        assertThat(attempts.keys.filter { it.startsWith("hydrate:") }).isNotEmpty()
        // MUTATION: drop the BackendUnavailable arm and the generic arm
        // marks it 'failed' (the unknown read as a file failure).
        assertThat(state()).isEqualTo("pending")
        // the next sweep, with hoglite answering, hydrates it
        txRule = null
        HogliteHydrator(store, objects).runOnce()
        assertThat(state()).isEqualTo("provided")
    }

    private fun s3(): S3Client =
        S3Client.builder()
            .endpointOverride(URI.create(s3Endpoint))
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create(minio.userName, minio.password)),
            )
            .region(Region.US_EAST_1)
            .forcePathStyle(true)
            .build()

    private val parquetSchema =
        org.apache.parquet.schema.Types
            .buildMessage()
            .addField(
                org.apache.parquet.schema.Types
                    .required(org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64)
                    .id(1)
                    .named("id"),
            ).named("t")

    private fun parquetBytes(ids: List<Long>): ByteArray {
        val tmp = Files.createTempFile("hoglite-ou", ".parquet")
        try {
            Files.delete(tmp)
            val factory = org.apache.parquet.example.data.simple.SimpleGroupFactory(parquetSchema)
            org.apache.parquet.hadoop.example.ExampleParquetWriter
                .builder(org.apache.parquet.io.LocalOutputFile(tmp))
                .withType(parquetSchema)
                .build()
                .use { w -> ids.forEach { id -> w.write(factory.newGroup().append("id", id)) } }
            return Files.readAllBytes(tmp)
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    private fun findOnPath(binary: String): String? {
        val path = System.getenv("PATH") ?: return null
        return path.split(':').map { "$it/$binary" }.firstOrNull { java.io.File(it).canExecute() }
    }

    private companion object {
        const val BUCKET = "hoglite-ou"
    }
}
