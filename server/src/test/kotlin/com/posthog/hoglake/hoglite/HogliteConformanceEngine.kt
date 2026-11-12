package com.posthog.hoglake.hoglite

import com.posthog.hoglake.testing.TestImages
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.testcontainers.containers.MinIOContainer
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import java.io.File
import java.net.ServerSocket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.time.Duration
import java.util.UUID

/**
 * A live hoglited process on a MinIO container, for the hoglite live
 * tests. It never mocks the engine.
 *
 * The binary comes from HOGLITE_BINARY, else from `hoglited` on PATH.
 * Without a binary the tests skip, so a plain `:test` stays green on a
 * machine without the engine. When HOGLITE_REQUIRED=1 (the hoglite-live
 * CI job), a missing binary is a failure, not a skip.
 */
internal class HogliteConformanceEngine(private val authArgs: List<String> = emptyList()) : AutoCloseable {
    private lateinit var minio: MinIOContainer
    private var process: Process? = null
    private lateinit var builder: ProcessBuilder
    private var port: Int = 0
    lateinit var client: HogliteClient
        private set
    lateinit var store: HogliteCatalogStore
        private set

    init {
        val binary = binary()
        minio = TestImages.minio()
        minio.start()
        // The AWS SDK only falls back to path-style addressing for an
        // IP-literal endpoint. MinIOContainer's s3URL is a `localhost`
        // URL, which resolves as a virtual-hosted bucket name.
        s3Client().use { s3 ->
            s3.createBucket { it.bucket(ENGINE_BUCKET) }
            // The catalogs' data_path bucket (s3://hoglake-it/...).
            s3.createBucket { it.bucket(DATA_BUCKET) }
        }
        port = ServerSocket(0).use { it.localPort }
        val dir = Files.createTempDirectory("hoglited-live").toFile()
        val log = File(dir, "engine.log")
        builder =
            ProcessBuilder(
                binary,
                "-addr", "127.0.0.1:$port",
                "-dir", dir.absolutePath,
                "-bucket", ENGINE_BUCKET,
                "-id", "it-1",
                "-class", "standard",
            ).redirectErrorStream(true).redirectOutput(log)
        builder.command().addAll(authArgs)
        builder.environment().putAll(
            mapOf(
                "AWS_ENDPOINT_URL" to s3Endpoint(),
                "AWS_ACCESS_KEY_ID" to minio.userName,
                "AWS_SECRET_ACCESS_KEY" to minio.password,
                "AWS_REGION" to "us-east-1",
            ),
        )
        process = builder.start()
        awaitStarted(log)
        client = HogliteClient(standardUrl = url(), sleeper = {})
        store = HogliteCatalogStore(client, TENANTS)
    }

    /** Abrupt owner loss and a fresh local directory: recovery must read durable S3 state. */
    fun crashAndRecover() {
        process!!.destroyForcibly().waitFor()
        val dir = Files.createTempDirectory("hoglited-live-recovered").toFile()
        val log = File(dir, "engine.log")
        val args = builder.command().toMutableList()
        args[args.indexOf("-dir") + 1] = dir.absolutePath
        args[args.indexOf("-id") + 1] = "recovered-" + UUID.randomUUID()
        builder.command(args).redirectOutput(log)
        process = builder.start()
        awaitStarted(log)
        store = HogliteCatalogStore(client, TENANTS)
    }

    private fun awaitStarted(log: File) {
        val http = HttpClient.newHttpClient()
        val deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos()
        while (System.nanoTime() < deadline) {
            if (process?.isAlive == false) break
            try {
                val resp =
                    http.send(
                        HttpRequest.newBuilder(URI.create("${url()}/v1/healthz")).build(),
                        HttpResponse.BodyHandlers.discarding(),
                    )
                if (resp.statusCode() == 200) return
            } catch (_: Exception) {
                // Not up yet.
            }
            Thread.sleep(100)
        }
        throw IllegalStateException("hoglited did not come up; output: ${log.readText().takeLast(2000)}")
    }

    override fun close() {
        if (::client.isInitialized) client.close()
        process?.destroy()
        process?.waitFor()
        if (::minio.isInitialized) minio.stop()
    }

    fun url(): String = "http://127.0.0.1:$port"

    private fun s3Endpoint(): String = "http://127.0.0.1:${minio.firstMappedPort}"

    fun s3Client(): S3Client =
        S3Client.builder()
            .endpointOverride(URI.create(s3Endpoint()))
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create(minio.userName, minio.password)),
            )
            .region(Region.US_EAST_1)
            .forcePathStyle(true)
            .build()

    companion object {
        const val ENGINE_BUCKET = "hoglite-it"
        const val DATA_BUCKET = "hoglake-it"

        /** One tenant per catalog name. The live tests use unique names. */
        val TENANTS = HogliteTenantResolver { catalog -> "it-$catalog" }

        /** A catalog name that no other test in the run uses. */
        fun uniqueName(prefix: String): String = prefix + UUID.randomUUID().toString().replace("-", "")

        /**
         * The engine binary. A missing binary skips the test, unless
         * HOGLITE_REQUIRED=1 makes it a failure (fail-closed CI).
         */
        fun binary(): String {
            val binary =
                System.getenv("HOGLITE_BINARY")?.takeIf { it.isNotEmpty() }
                    ?: System.getenv("PATH")?.split(':')?.map { "$it/hoglited" }?.firstOrNull { File(it).canExecute() }
            if (System.getenv("HOGLITE_REQUIRED") == "1") {
                check(binary != null && File(binary).canExecute()) {
                    "HOGLITE_REQUIRED=1 but no executable hoglited binary (HOGLITE_BINARY='${System.getenv(
                        "HOGLITE_BINARY",
                    )}')"
                }
            }
            assumeTrue(binary != null) { "no hoglited binary (set HOGLITE_BINARY); skipping hoglite live tests" }
            return binary!!
        }
    }
}
