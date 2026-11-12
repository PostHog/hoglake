package com.posthog.hoglake.storage

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import java.net.InetSocketAddress
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.Base64
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/**
 * The `broker` provider against a fake broker: the two HTTP shapes it
 * speaks, and the session lifecycle — cached per (catalog, access),
 * refreshed inside the margin, kept through a failed refresh for as long
 * as it is valid, and surfaced as a failure only when nothing valid is
 * held.
 */
class BrokerProviderTest {
    private class Recorded(val headers: Map<String, String>, val body: String)

    private class FakeBroker : AutoCloseable {
        val json = ObjectMapper()
        val tokenRequests = CopyOnWriteArrayList<Recorded>()
        val vendRequests = CopyOnWriteArrayList<Recorded>()
        val tokenSerial = AtomicInteger(0)
        val keySerial = AtomicInteger(0)

        /** What the next vends answer; the default is a fresh session each time. */
        @Volatile
        var vendStatus: Int = 200

        @Volatile
        var vendBody: (Int) -> String = { n -> session("AKIA$n", "secret$n", "session$n", expiration) }

        @Volatile
        var tokenStatus: Int = 200

        @Volatile
        var expiration: String = "2030-01-01T00:00:00Z"

        /** Bearers the vend endpoint accepts; empty = any. */
        val acceptedBearers = CopyOnWriteArrayList<String>()

        fun session(
            id: String,
            secret: String,
            token: String,
            expiration: String,
        ): String =
            json.writeValueAsString(
                mapOf(
                    "access_key_id" to id,
                    "secret_access_key" to secret,
                    "session_token" to token,
                    "expiration" to expiration,
                ),
            )

        val server: HttpServer =
            HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                createContext("/oauth/token") { ex ->
                    val rec = record(ex)
                    tokenRequests += rec
                    if (tokenStatus != 200) {
                        reply(ex, tokenStatus, """{"error":"server_error"}""")
                    } else {
                        val n = tokenSerial.incrementAndGet()
                        acceptedBearers += "tok$n"
                        reply(ex, 200, """{"access_token":"tok$n","token_type":"Bearer","expires_in":600}""")
                    }
                }
                createContext("/storage") { ex ->
                    val rec = record(ex)
                    vendRequests += rec
                    val bearer = rec.headers["authorization"]?.removePrefix("Bearer ")
                    if (acceptedBearers.isNotEmpty() && bearer !in acceptedBearers) {
                        reply(ex, 401, """{"error":"invalid_token"}""")
                    } else if (vendStatus != 200) {
                        reply(ex, vendStatus, """{"error":"refused","reason":"catalog not here"}""")
                    } else {
                        reply(ex, 200, vendBody(keySerial.incrementAndGet()))
                    }
                }
                start()
            }

        val tokenUrl: URI = URI.create("http://127.0.0.1:${server.address.port}/oauth/token")
        val brokerUrl: URI = URI.create("http://127.0.0.1:${server.address.port}/storage")

        private fun record(ex: HttpExchange): Recorded {
            val headers = ex.requestHeaders.entries.associate { (k, v) -> k.lowercase() to v.joinToString(",") }
            val body = ex.requestBody.readAllBytes().toString(StandardCharsets.UTF_8)
            return Recorded(headers, body)
        }

        private fun reply(
            ex: HttpExchange,
            status: Int,
            body: String,
        ) {
            val bytes = body.toByteArray(StandardCharsets.UTF_8)
            ex.responseHeaders.add("Content-Type", "application/json")
            ex.sendResponseHeaders(status, bytes.size.toLong())
            ex.responseBody.use { it.write(bytes) }
        }

        override fun close() = server.stop(0)
    }

    private class MutableClock(
        @Volatile var now: Instant,
    ) : Clock() {
        override fun instant(): Instant = now

        override fun getZone(): ZoneId = ZoneOffset.UTC

        override fun withZone(zone: ZoneId?): Clock = this

        fun advance(by: Duration) {
            now = now.plus(by)
        }
    }

    private val broker = FakeBroker()
    private val clock = MutableClock(Instant.parse("2029-12-31T23:00:00Z"))

    private fun provider(margin: Duration = Duration.ofMinutes(5)) =
        BrokerProvider(
            brokerUrl = broker.brokerUrl,
            tokenUrl = broker.tokenUrl,
            clientId = "hoglake-test",
            clientSecret = "s3cret",
            audience = "hoglake",
            refreshMargin = margin,
            clock = clock,
        )

    @AfterEach
    fun tearDown() = broker.close()

    @Test
    fun `exchanges the client credential and vends a session for the catalog`() {
        provider().use { p ->
            val creds = p.credentialsFor("acme", "s3://bucket/acme/", StorageAccess.WRITE).resolveCredentials()

            assertThat(creds).isInstanceOf(AwsSessionCredentials::class.java)
            creds as AwsSessionCredentials
            assertThat(creds.accessKeyId()).isEqualTo("AKIA1")
            assertThat(creds.secretAccessKey()).isEqualTo("secret1")
            assertThat(creds.sessionToken()).isEqualTo("session1")

            val token = broker.tokenRequests.single()
            val basic = Base64.getEncoder().encodeToString("hoglake-test:s3cret".toByteArray())
            assertThat(token.headers["authorization"]).isEqualTo("Basic $basic")
            assertThat(token.headers["content-type"]).isEqualTo("application/x-www-form-urlencoded")
            assertThat(token.body).isEqualTo("grant_type=client_credentials&audience=hoglake")

            val vend = broker.vendRequests.single()
            assertThat(vend.headers["authorization"]).isEqualTo("Bearer tok1")
            assertThat(vend.headers["content-type"]).isEqualTo("application/json")
            val body = broker.json.readTree(vend.body)
            assertThat(body.get("catalog").asText()).isEqualTo("acme")
            assertThat(body.get("prefix").asText()).isEqualTo("s3://bucket/acme/")
            assertThat(body.get("access").asText()).isEqualTo("write")
        }
    }

    @Test
    fun `one session per catalog and access, one token for all of them`() {
        provider().use { p ->
            val write = p.credentialsFor("acme", "s3://b/acme/", StorageAccess.WRITE)
            val read = p.credentialsFor("acme", "s3://b/acme/", StorageAccess.READ)
            val other = p.credentialsFor("other", "s3://b/other/", StorageAccess.READ)

            repeat(3) { write.resolveCredentials() }
            assertThat(broker.vendRequests).hasSize(1)
            read.resolveCredentials()
            other.resolveCredentials()
            assertThat(broker.vendRequests).hasSize(3)
            assertThat(broker.vendRequests.map { broker.json.readTree(it.body).get("access").asText() })
                .containsExactly("write", "read", "read")
            assertThat(broker.tokenRequests).hasSize(1)
            assertThat(p.held().keys.map { it.catalog to it.access })
                .containsExactlyInAnyOrder(
                    "acme" to StorageAccess.WRITE,
                    "acme" to StorageAccess.READ,
                    "other" to StorageAccess.READ,
                )
        }
    }

    @Test
    fun `refreshes inside the margin, before the session expires`() {
        broker.expiration = "2029-12-31T23:30:00Z" // 30 min from the clock
        provider(margin = Duration.ofMinutes(5)).use { p ->
            val creds = p.credentialsFor("acme", "s3://b/acme/", StorageAccess.READ)
            assertThat((creds.resolveCredentials() as AwsSessionCredentials).accessKeyId()).isEqualTo("AKIA1")

            clock.advance(Duration.ofMinutes(24)) // 6 min left: outside the margin
            assertThat((creds.resolveCredentials() as AwsSessionCredentials).accessKeyId()).isEqualTo("AKIA1")
            assertThat(broker.vendRequests).hasSize(1)

            broker.expiration = "2030-01-01T00:30:00Z"
            clock.advance(Duration.ofMinutes(2)) // 4 min left: inside it
            assertThat((creds.resolveCredentials() as AwsSessionCredentials).accessKeyId()).isEqualTo("AKIA2")
            assertThat(broker.vendRequests).hasSize(2)
        }
    }

    @Test
    fun `a failed refresh keeps the held session while it is valid, then fails`() {
        broker.expiration = "2029-12-31T23:30:00Z"
        provider(margin = Duration.ofMinutes(5)).use { p ->
            val creds = p.credentialsFor("acme", "s3://b/acme/", StorageAccess.WRITE)
            creds.resolveCredentials()

            broker.vendStatus = 503
            clock.advance(Duration.ofMinutes(27)) // 3 min left: refresh due, broker down
            assertThat((creds.resolveCredentials() as AwsSessionCredentials).accessKeyId()).isEqualTo("AKIA1")
            // Every request inside the margin retries the broker; none of
            // them throws while the session is still good.
            creds.resolveCredentials()
            assertThat(broker.vendRequests.size).isGreaterThanOrEqualTo(3)

            clock.advance(Duration.ofMinutes(4)) // past expiry: nothing valid to hand out
            assertThatThrownBy { creds.resolveCredentials() }
                .isInstanceOf(StorageCredentialException::class.java)
                .hasMessageContaining("catalog 'acme'")
                .hasMessageContaining("503")
                .hasMessageNotContaining("secret1")
                .hasMessageNotContaining("session1")

            // The broker comes back: the next request succeeds.
            broker.vendStatus = 200
            broker.expiration = "2030-06-01T00:00:00Z"
            assertThat((creds.resolveCredentials() as AwsSessionCredentials).accessKeyId()).startsWith("AKIA")
        }
    }

    @Test
    fun `a 401 from the broker re-exchanges the token once`() {
        provider().use { p ->
            val creds = p.credentialsFor("acme", "s3://b/acme/", StorageAccess.READ)
            creds.resolveCredentials()
            assertThat(broker.tokenRequests).hasSize(1)

            // The broker forgets tok1 (revoked ahead of its lifetime); the
            // token is still inside its own lifetime here, so only the
            // 401 can prompt a new exchange.
            broker.acceptedBearers.clear()
            broker.acceptedBearers += "revoked"
            p.credentialsFor("acme", "s3://b/acme/", StorageAccess.WRITE).resolveCredentials()

            assertThat(broker.tokenRequests).hasSize(2)
            assertThat(broker.vendRequests.map { it.headers["authorization"] })
                .containsExactly("Bearer tok1", "Bearer tok1", "Bearer tok2")
        }
    }

    @Test
    fun `token and vend failures surface as credential failures without the secret`() {
        broker.tokenStatus = 500
        provider().use { p ->
            assertThatThrownBy { p.credentialsFor("acme", "s3://b/acme/", StorageAccess.READ).resolveCredentials() }
                .isInstanceOf(StorageCredentialException::class.java)
                .hasMessageContaining("token url answered 500")
                .hasMessageNotContaining("s3cret")
        }
        broker.tokenStatus = 200
        broker.vendStatus = 403
        provider().use { p ->
            assertThatThrownBy { p.credentialsFor("acme", "s3://b/acme/", StorageAccess.READ).resolveCredentials() }
                .isInstanceOf(StorageCredentialException::class.java)
                .hasMessageContaining("broker answered 403")
                .hasMessageContaining("catalog not here")
        }
        broker.vendStatus = 200
        broker.vendBody = { """{"access_key_id":"x"}""" }
        provider().use { p ->
            assertThatThrownBy { p.credentialsFor("acme", "s3://b/acme/", StorageAccess.READ).resolveCredentials() }
                .isInstanceOf(StorageCredentialException::class.java)
                .hasMessageContaining("secret_access_key")
        }
        broker.vendBody = { broker.session("a", "b", "c", "tomorrow") }
        provider().use { p ->
            assertThatThrownBy { p.credentialsFor("acme", "s3://b/acme/", StorageAccess.READ).resolveCredentials() }
                .isInstanceOf(StorageCredentialException::class.java)
                .hasMessageContaining("RFC 3339")
        }
        broker.vendBody = { broker.session("a", "b", "c", "2020-01-01T00:00:00Z") }
        provider().use { p ->
            assertThatThrownBy { p.credentialsFor("acme", "s3://b/acme/", StorageAccess.READ).resolveCredentials() }
                .isInstanceOf(StorageCredentialException::class.java)
                .hasMessageContaining("already expired")
        }
    }

    @Test
    fun `an expiration with a zone offset is read as the instant it names`() {
        broker.expiration = "2030-01-01T02:00:00+02:00"
        provider().use { p ->
            p.credentialsFor("acme", "s3://b/acme/", StorageAccess.READ).resolveCredentials()
            assertThat(p.held().values.single()).isEqualTo(Instant.parse("2030-01-01T00:00:00Z"))
        }
    }

    @Test
    fun `the unscoped client is refused, naming the knob`() {
        provider().use { p ->
            assertThat(p.scoped).isTrue()
            assertThatThrownBy { p.unscoped().resolveCredentials() }
                .isInstanceOf(StorageCredentialException::class.java)
                .hasMessageContaining("HOGLAKE_STORAGE_PROVIDER=broker")
                .hasMessageContaining("forCatalog")
            assertThat(broker.vendRequests).isEmpty()
            assertThat(broker.tokenRequests).isEmpty()
        }
    }

    @Test
    fun `concurrent first requests for one key cost one vend`() {
        provider().use { p ->
            val creds = p.credentialsFor("acme", "s3://b/acme/", StorageAccess.READ)
            val threads = (1..8).map { Thread { creds.resolveCredentials() } }
            threads.forEach { it.start() }
            threads.forEach { it.join() }
            assertThat(broker.vendRequests).hasSize(1)
            assertThat(broker.tokenRequests).hasSize(1)
        }
    }
}
