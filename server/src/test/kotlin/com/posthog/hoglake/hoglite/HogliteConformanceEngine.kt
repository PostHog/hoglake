package com.posthog.hoglake.hoglite

import com.posthog.hoglake.Config
import com.posthog.hoglake.testing.TestImages
import org.junit.jupiter.api.Assumptions.assumeTrue
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

/** Live engine shared by the conformance and schema tests; never mocks /tx. */
internal class HogliteConformanceEngine : AutoCloseable {
    private lateinit var minio: MinIOContainer
    private var process: Process? = null
    private lateinit var builder: ProcessBuilder
    private var port: Int = 0
    lateinit var client: HogliteClient
        private set
    lateinit var store: HogliteCatalogStore
        private set

    init {
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
        // The catalog data_path bucket (freshCatalog's s3://hoglake-it/...):
        // cleanup's drain DELETEs there.
        s3.createBucket { it.bucket("hoglake-it") }
        s3.close()

        port = ServerSocket(0).use { it.localPort }
        val dir = Files.createTempDirectory("hoglited-conformance").toFile()
        val log = java.io.File(dir, "engine.log")
        builder =
            ProcessBuilder(
                binary,
                "-addr", "127.0.0.1:$port",
                "-dir", dir.absolutePath,
                "-bucket", bucket,
                "-id", "it-1",
                "-class", "standard",
            ).redirectErrorStream(true).redirectOutput(log)
        builder.environment().putAll(
            mapOf(
                "AWS_ENDPOINT_URL" to s3Endpoint,
                "AWS_ACCESS_KEY_ID" to minio.userName,
                "AWS_SECRET_ACCESS_KEY" to minio.password,
                "AWS_REGION" to "us-east-1",
            ),
        )
        process = builder.start()
        awaitStarted(log)
        client = HogliteClient(standardUrl = "http://127.0.0.1:$port", sleeper = {})
        store = HogliteCatalogStore(client).apply { requireOperationBindingForTests = true }
    }

    /** Abrupt owner loss and a fresh local directory: recovery must read durable S3 state. */
    fun crashAndRecover() {
        process!!.destroyForcibly().waitFor()
        val dir = Files.createTempDirectory("hoglited-conformance-recovered").toFile()
        val log = java.io.File(dir, "engine.log")
        val args = builder.command().toMutableList()
        args[args.indexOf("-dir") + 1] = dir.absolutePath
        args[args.indexOf("-id") + 1] = "recovered-" + java.util.UUID.randomUUID()
        builder.command(args).redirectOutput(log)
        process = builder.start()
        awaitStarted(log)
        store = HogliteCatalogStore(client).apply { requireOperationBindingForTests = true }
    }

    private fun awaitStarted(log: java.io.File) {
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
            val out = log.readText().takeLast(2000)
            throw IllegalStateException("hoglited did not come up; output: $out")
        }
    }

    override fun close() {
        if (::client.isInitialized) client.close()
        process?.destroy()
        process?.waitFor()
        if (::minio.isInitialized) minio.stop()
    }

    fun config() =
        Config(
            s3Endpoint = "http://127.0.0.1:${minio.firstMappedPort}",
            s3Region = "us-east-1",
            s3AccessKey = minio.userName,
            s3SecretKey = minio.password,
            s3PathStyle = true,
        )

    fun s3Client(): S3Client =
        S3Client.builder()
            .endpointOverride(URI.create(config().s3Endpoint))
            .credentialsProvider(
                StaticCredentialsProvider.create(AwsBasicCredentials.create(minio.userName, minio.password)),
            )
            .region(Region.US_EAST_1)
            .forcePathStyle(true)
            .build()

    private fun findOnPath(binary: String): String? =
        System.getenv("PATH")?.split(':')?.map { "$it/$binary" }?.firstOrNull { java.io.File(it).canExecute() }
}
