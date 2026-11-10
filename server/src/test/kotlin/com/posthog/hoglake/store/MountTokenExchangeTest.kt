package com.posthog.hoglake.store

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.store.FakeTokenEndpoint.Companion.error
import com.posthog.hoglake.store.FakeTokenEndpoint.Companion.json
import com.posthog.hoglake.store.MountTokenExchange.Failure
import com.posthog.hoglake.store.MountTokenExchange.Outcome
import com.posthog.hoglake.store.MountTokenExchange.Refusal
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.net.ServerSocket
import java.net.URI
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

/**
 * The events mount token exchange (hogtower#181) as a mount Client makes
 * it, against a token endpoint that records every request: the exact
 * request, each answer's meaning, and the cache.
 */
class MountTokenExchangeTest {
    private val pair = TestTokens.keyPair()
    private val orgId = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f0"
    private val otherOrg = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f1"
    private val mount = "$orgId-events"
    private val otherMount = "$otherOrg-events"
    private val hostId = UUID.fromString("3c0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")

    /** The exchange's monotonic clock, moved by hand. */
    private val nanos = AtomicLong(1_000_000_000_000L)

    private fun advance(seconds: Long) = nanos.addAndGet(seconds * 1_000_000_000L)

    @AfterEach
    fun unbindMetrics() = Metrics.clear()

    private fun endpoint() = FakeTokenEndpoint(pair, federated = mapOf(orgId to setOf(7L, 42L), otherOrg to setOf(7L)))

    private fun exchange(
        endpoint: FakeTokenEndpoint,
        maxEntries: Int = MountTokenExchange.MAX_ENTRIES,
        timeoutMs: Long = 5_000,
        config: MountExchange = endpoint.exchangeConfig(timeoutMs),
    ) = MountTokenExchange(hostId, config, maxEntries = maxEntries, nanoTime = nanos::get)

    /** A reader's token for [orgId]'s catalog, with [life] seconds left. */
    private fun reader(
        orgId: String = this.orgId,
        jti: String = "jti-${UUID.randomUUID()}",
        life: Long = 900,
        pad: String = "",
    ): String {
        val now = System.currentTimeMillis() / 1000
        return TestTokens.mint(
            pair,
            """{"iss":"${TestTokens.ISSUER}","aud":"${TestTokens.AUDIENCE}","sub":"hlc_team","jti":"$jti",""" +
                """"ten":"scoped","org":"$orgId","cli":"laptop$pad","usr":"user-1",""" +
                """"iat":${maxOf(now - 5, now + life - 900)},""" +
                """"exp":${now + life},"cat":{"$orgId":["read"]},"prd":{"team_id":{"7":"read","42":"read"}}}""",
        )
    }

    private fun MountTokenExchange.get(
        subject: String,
        mount: String = this@MountTokenExchangeTest.mount,
    ): Outcome = runBlocking { token(subject, mount) }

    private fun Outcome.minted(): String = (this as Outcome.Minted).token

    @Test
    fun `the request is hogtower 181's, field for field`() {
        endpoint().use { endpoint ->
            exchange(endpoint).use { exchange ->
                val subject = reader()
                assertThat(exchange.get(subject).minted()).isEqualTo(endpoint.minted.single())
                val seen = endpoint.seen.single()
                assertThat(seen.method).isEqualTo("POST")
                assertThat(seen.rawPath).isEqualTo("/oauth/token")
                assertThat(seen.rawQuery).isNull()
                // Basic, each half form-encoded before the join (RFC 6749 §2.3.1): the secret holds `+/=&% `.
                assertThat(seen.headers["authorization"]).containsExactly(endpoint.basic)
                assertThat(endpoint.basic).isEqualTo(
                    "Basic " +
                        java.util.Base64.getEncoder().encodeToString(
                            "hls_0123456789abcdef01234567:s3cret-for-the-mount-exchange%2B%2F%3D%26%25+0123456789"
                                .toByteArray(),
                        ),
                )
                assertThat(seen.headers["content-type"]).containsExactly("application/x-www-form-urlencoded")
                assertThat(seen.headers["accept"]).containsExactly("application/json")
                assertThat(seen.headers["user-agent"]!!.single()).startsWith("hoglake-server/")
                // Every field, once, and nothing else: no client_id or secret in the body, no actor_token, no values.
                assertThat(seen.form).containsExactly(
                    "grant_type" to "urn:ietf:params:oauth:grant-type:token-exchange",
                    "subject_token" to subject,
                    "subject_token_type" to "urn:ietf:params:oauth:token-type:access_token",
                    "resource" to "urn:posthog:hoglake:catalog:$mount",
                    "audience" to "hoglake",
                )
                assertThat(seen.body).doesNotContain(endpoint.clientSecret, endpoint.clientId)
            }
        }
    }

    @Test
    fun `each answer of the token endpoint has one meaning`() {
        val minted = TestTokens.mint(pair, TestTokens.scopedClaims())
        val type = "urn:ietf:params:oauth:token-type:access_token"

        fun ok(
            token: String = minted,
            tokenType: String = "Bearer",
            issued: String = type,
            expiresIn: String = "900",
        ) = """{"access_token":"$token","issued_token_type":"$issued",""" +
            """"token_type":"$tokenType","expires_in":$expiresIn}"""
        val cases: List<Triple<String, (com.sun.net.httpserver.HttpExchange) -> Unit, Outcome>> =
            listOf(
                Triple("invalid_target", { it.error(400, "invalid_target") }, Outcome.Refused(Refusal.UNKNOWN_MOUNT)),
                Triple("invalid_grant", { it.error(400, "invalid_grant") }, Outcome.Refused(Refusal.NOT_ENTITLED)),
                Triple(
                    "401",
                    { it.error(401, "invalid_client") },
                    Outcome.Failed(Failure.MISCONFIGURED, "invalid_client"),
                ),
                // 401 is the client's authentication whatever the body is.
                Triple(
                    "401 bare",
                    { it.json(401, "nope", "text/plain") },
                    Outcome.Failed(Failure.MISCONFIGURED, "invalid_client"),
                ),
                Triple(
                    "unauthorized_client",
                    { it.error(400, "unauthorized_client") },
                    Outcome.Failed(Failure.MISCONFIGURED, "unauthorized_client"),
                ),
                Triple(
                    "unsupported_grant_type",
                    { it.error(400, "unsupported_grant_type") },
                    Outcome.Failed(Failure.MISCONFIGURED, "unsupported_grant_type"),
                ),
                Triple(
                    "invalid_request",
                    { it.error(400, "invalid_request") },
                    Outcome.Failed(Failure.MISCONFIGURED, "invalid_request"),
                ),
                Triple("429", { it.error(429, "slow_down") }, Outcome.Failed(Failure.UNAVAILABLE, "rate_limited")),
                Triple(
                    "503",
                    { it.error(503, "temporarily_unavailable") },
                    Outcome.Failed(Failure.UNAVAILABLE, "temporarily_unavailable"),
                ),
                Triple(
                    "invalid_client as a 400",
                    { it.error(400, "invalid_client") },
                    Outcome.Failed(Failure.MISCONFIGURED, "invalid_client"),
                ),
                Triple("an unknown code", { it.error(400, "something_new") }, INVALID),
                // Over the 64 KiB read: not the contract's answer, whatever its status says.
                Triple("a 200 over the size cap", { it.json(200, ok(token = "a.b." + "c".repeat(70_000))) }, INVALID),
                Triple(
                    "a refusal over the size cap",
                    { it.json(400, """{"error":"invalid_target","pad":"${"x".repeat(70_000)}"}""") },
                    INVALID,
                ),
                Triple("a 400 that is not JSON", { it.json(400, "<html>", "text/html") }, INVALID),
                Triple("a 500", { it.error(500, "invalid_grant") }, INVALID),
                Triple("a 403", { it.error(403, "invalid_target") }, INVALID),
                Triple("a 200 that is not JSON", { it.json(200, ok(), "text/plain") }, INVALID),
                Triple("a 200 array", { it.json(200, "[]") }, INVALID),
                Triple("no access_token", { it.json(200, """{"token_type":"Bearer","expires_in":900}""") }, INVALID),
                Triple("a token with a space", { it.json(200, ok(token = "a.b c.d")) }, INVALID),
                Triple("a token with a newline", { it.json(200, ok(token = "a.b.c\\r\\nX-Injected: 1")) }, INVALID),
                Triple("a token of two parts", { it.json(200, ok(token = "a.b")) }, INVALID),
                Triple("an oversized token", { it.json(200, ok(token = "a.b." + "c".repeat(17_000))) }, INVALID),
                Triple("another token type", { it.json(200, ok(tokenType = "MAC")) }, INVALID),
                Triple(
                    "another issued type",
                    { it.json(200, ok(issued = "urn:ietf:params:oauth:token-type:jwt")) },
                    INVALID,
                ),
                Triple("no lifetime", { it.json(200, ok(expiresIn = "0")) }, INVALID),
                Triple("a fractional lifetime", { it.json(200, ok(expiresIn = "900.5")) }, INVALID),
                Triple("a textual lifetime", { it.json(200, ok(expiresIn = "\"900\"")) }, INVALID),
            )
        for ((what, answer, expected) in cases) {
            endpoint().use { endpoint ->
                endpoint.answer = { exchange, _ -> answer(exchange) }
                exchange(endpoint).use { exchange ->
                    assertThat(exchange.get(reader())).describedAs(what).isEqualTo(expected)
                    assertThat(endpoint.seen).describedAs(what).hasSize(1)
                }
            }
        }
        // A usable answer, for contrast, through the same hand-written body.
        endpoint().use { endpoint ->
            endpoint.answer = {
                    exchange,
                    _,
                ->
                exchange.json(200, ok(tokenType = "bearer"), "application/json; charset=utf-8")
            }
            exchange(endpoint).use { assertThat(it.get(reader()).minted()).isEqualTo(minted) }
        }
    }

    @Test
    fun `a redirect is an invalid answer and its target is never contacted`() {
        endpoint().use { elsewhere ->
            endpoint().use { endpoint ->
                endpoint.answer = { exchange, _ ->
                    exchange.responseHeaders.add("Location", elsewhere.tokenUrl.toString())
                    exchange.sendResponseHeaders(307, -1)
                }
                exchange(endpoint).use { assertThat(it.get(reader())).isEqualTo(INVALID) }
                assertThat(elsewhere.seen).isEmpty()
            }
        }
    }

    @Test
    fun `an endpoint that is down or slow is unavailable, inside the deadline`() {
        val closed = ServerSocket(0).use { it.localPort }
        endpoint().use { endpoint ->
            val down =
                MountExchange(
                    URI("http://127.0.0.1:$closed/oauth/token"),
                    endpoint.clientId,
                    ClientSecret(endpoint.clientSecret),
                )
            exchange(endpoint, config = down).use {
                assertThat(it.get(reader())).isEqualTo(Outcome.Failed(Failure.UNAVAILABLE, "unavailable"))
            }
            val release = CountDownLatch(1)
            endpoint.answer = { _, _ -> release.await(20, TimeUnit.SECONDS) }
            try {
                exchange(endpoint, timeoutMs = 300).use {
                    val started = System.nanoTime()
                    assertThat(it.get(reader())).isEqualTo(Outcome.Failed(Failure.UNAVAILABLE, "timeout"))
                    assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(5))
                }
                // Headers in time, then a body that never ends: the same deadline, not the header timeout alone.
                endpoint.answer = { exchange, _ ->
                    exchange.responseHeaders.add("Content-Type", "application/json")
                    exchange.sendResponseHeaders(200, 0)
                    exchange.responseBody.write("{".toByteArray())
                    exchange.responseBody.flush()
                    release.await(20, TimeUnit.SECONDS)
                }
                exchange(endpoint, timeoutMs = 300).use {
                    val started = System.nanoTime()
                    assertThat(it.get(reader())).isEqualTo(Outcome.Failed(Failure.UNAVAILABLE, "timeout"))
                    assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(5))
                }
            } finally {
                release.countDown()
            }
        }
    }

    @Test
    fun `one subject token costs one exchange, and two subject tokens never share a minted token`() {
        endpoint().use { endpoint ->
            exchange(endpoint).use { exchange ->
                val a = reader(jti = "a")
                val b = reader(jti = "b")
                val forA = (1..25).map { exchange.get(a).minted() }.toSet()
                assertThat(forA).hasSize(1)
                assertThat(endpoint.seen).hasSize(1)
                val forB = (1..25).map { exchange.get(b).minted() }.toSet()
                assertThat(forB).hasSize(1)
                assertThat(endpoint.seen).hasSize(2)
                // Each got the token minted from its own subject token.
                assertThat(forA.single()).isNotEqualTo(forB.single())
                assertThat(endpoint.seen.map { it.field("subject_token") }).containsExactly(a, b)
                assertThat(endpoint.minted).containsExactly(forA.single(), forB.single())
                // One token is one entry however its signature is spelled: the same signed part under
                // another signature that verifies (ECDSA signs differently each time) costs no exchange.
                val claims = String(java.util.Base64.getUrlDecoder().decode(a.split('.')[1]))
                val respelled = TestTokens.mint(pair, claims)
                assertThat(respelled).isNotEqualTo(a)
                assertThat(respelled.substringBeforeLast('.')).isEqualTo(a.substringBeforeLast('.'))
                assertThat(exchange.get(respelled).minted()).isEqualTo(forA.single())
                assertThat(endpoint.seen).hasSize(2)
                // Any other signed part, a single claim apart, is another subject token.
                val again = TestTokens.mint(pair, claims.replace("\"jti\":\"a\"", "\"jti\":\"a2\""))
                assertThat(exchange.get(again).minted()).isNotIn(forA.single(), forB.single())
                assertThat(endpoint.seen).hasSize(3)
                // And the key holds the mount: a token for one mount is never the answer for another.
                val other = reader(orgId = otherOrg)
                assertThat(exchange.get(other, otherMount).minted()).isEqualTo(endpoint.minted[3])
                assertThat(exchange.get(other, mount)).isEqualTo(Outcome.Refused(Refusal.UNKNOWN_MOUNT))
                assertThat(endpoint.seen).hasSize(5)
                assertThat(exchange.get(other, otherMount).minted()).isEqualTo(endpoint.minted[3])
            }
        }
    }

    @Test
    fun `concurrent requests for one subject token share one exchange`() {
        endpoint().use { endpoint ->
            val arrived = CountDownLatch(1)
            val release = CountDownLatch(1)
            exchange(endpoint).use { exchange ->
                val subject = reader()
                val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
                runBlocking(Dispatchers.Default) {
                    // The first request's exchange is held open at the issuer.
                    endpoint.answer = { e, request ->
                        arrived.countDown()
                        release.await(20, TimeUnit.SECONDS)
                        endpoint.contract(e, request)
                    }
                    val waiting =
                        (1..20).map { async(start = CoroutineStart.UNDISPATCHED) { exchange.token(subject, mount) } }
                    assertThat(arrived.await(10, TimeUnit.SECONDS)).isTrue()
                    // One of the waiters leaves; the exchange is the others' too and goes on.
                    waiting.first().cancelAndJoin()
                    release.countDown()
                    val answers = waiting.drop(1).awaitAll().map { (it as Outcome.Minted).token }.toSet()
                    assertThat(answers).containsExactly(endpoint.minted.single())
                }
                assertThat(endpoint.seen).hasSize(1)
                assertThat(
                    registry.counter("hoglake_mount_exchange_cache_total", "result", "miss").count(),
                ).isEqualTo(1.0)
                assertThat(
                    registry.counter("hoglake_mount_exchange_cache_total", "result", "shared").count(),
                ).isEqualTo(19.0)
                assertThat(registry.counter("hoglake_mount_exchange_requests_total").count()).isEqualTo(1.0)
            }
        }
    }

    @Test
    fun `an entry is used until thirty seconds before the stated expiry, then exchanged again`() {
        endpoint().use { endpoint ->
            exchange(endpoint).use { exchange ->
                assertThat(MountTokenExchange.MARGIN_SECONDS).isEqualTo(30)
                endpoint.statedLifetime = 600
                val subject = reader()
                val first = exchange.get(subject).minted()
                advance(569)
                assertThat(exchange.get(subject).minted()).isEqualTo(first)
                assertThat(endpoint.seen).hasSize(1)
                // 570 s in, 30 s are left: not more than the margin.
                advance(1)
                val second = exchange.get(subject).minted()
                assertThat(second).isNotEqualTo(first)
                assertThat(endpoint.seen).hasSize(2)
                // Long past its expiry it is not served either.
                advance(100_000)
                assertThat(exchange.get(subject).minted()).isNotIn(first, second)
                assertThat(endpoint.seen).hasSize(3)
                // A stated lifetime over the contract's 15 minutes is not believed.
                endpoint.statedLifetime = 86_400
                val other = reader()
                val long = exchange.get(other).minted()
                advance(869)
                assertThat(exchange.get(other).minted()).isEqualTo(long)
                advance(1)
                assertThat(exchange.get(other).minted()).isNotEqualTo(long)
            }
        }
    }

    @Test
    fun `a token with no usable life is not handed out, and a subject near its expiry is not exchanged`() {
        endpoint().use { endpoint ->
            exchange(endpoint).use { exchange ->
                // The issuer states 30 s: nothing is left over the margin.
                endpoint.statedLifetime = 30
                val subject = reader()
                assertThat(exchange.get(subject)).isEqualTo(Outcome.Refused(Refusal.NEAR_EXPIRY))
                // Remembered until that token would have expired, then asked again.
                assertThat(exchange.get(subject)).isEqualTo(Outcome.Refused(Refusal.NEAR_EXPIRY))
                assertThat(endpoint.seen).hasSize(1)
                advance(31)
                endpoint.statedLifetime = 31
                assertThat(exchange.get(subject).minted()).isEqualTo(endpoint.minted.last())
                assertThat(endpoint.seen).hasSize(2)

                // A subject with thirty seconds or less of its own life left: nothing is sent.
                endpoint.statedLifetime = null
                for (life in listOf(30L, 1L, 0L, -45L)) {
                    assertThat(
                        exchange.get(reader(life = life)),
                    ).describedAs("$life s").isEqualTo(Outcome.Refused(Refusal.NEAR_EXPIRY))
                }
                // Nor for something that is no token at all, or one the issuer would refuse for its length.
                assertThat(exchange.get("not-a-token")).isEqualTo(Outcome.Refused(Refusal.NEAR_EXPIRY))
                assertThat(exchange.get(reader(pad = "x".repeat(12 * 1024))))
                    .isEqualTo(Outcome.Refused(Refusal.SUBJECT_TOO_LARGE))
                assertThat(endpoint.seen).hasSize(2)
                // With a little more than the margin it is exchanged.
                assertThat(exchange.get(reader(life = 40))).isInstanceOf(Outcome.Minted::class.java)
                assertThat(endpoint.seen).hasSize(3)
            }
        }
    }

    @Test
    fun `the cache is bounded and drops the least recently used`() {
        endpoint().use { endpoint ->
            exchange(endpoint, maxEntries = 2).use { exchange ->
                val (a, b, c) = listOf(reader(), reader(), reader())
                exchange.get(a)
                exchange.get(b)
                exchange.get(a)
                exchange.get(c)
                assertThat(exchange.size).isEqualTo(2)
                assertThat(endpoint.seen).hasSize(3)
                // a and c are held; b, the least recently used, was dropped and costs an exchange.
                exchange.get(a)
                exchange.get(c)
                assertThat(endpoint.seen).hasSize(3)
                exchange.get(b)
                assertThat(endpoint.seen).hasSize(4)
                assertThat(exchange.size).isEqualTo(2)
            }
        }
    }

    @Test
    fun `a refusal is believed for thirty seconds and a failure is never kept`() {
        endpoint().use { endpoint ->
            exchange(endpoint).use { exchange ->
                val subject = reader()
                endpoint.answer = { exchange, _ -> exchange.error(400, "invalid_target") }
                repeat(5) { assertThat(exchange.get(subject)).isEqualTo(Outcome.Refused(Refusal.UNKNOWN_MOUNT)) }
                assertThat(endpoint.seen).hasSize(1)
                advance(MountTokenExchange.REFUSAL_SECONDS - 1)
                exchange.get(subject)
                assertThat(endpoint.seen).hasSize(1)
                // The org is federated meanwhile: seen at most thirty seconds late.
                endpoint.answer = null
                advance(1)
                assertThat(exchange.get(subject)).isInstanceOf(Outcome.Minted::class.java)
                assertThat(endpoint.seen).hasSize(2)

                val another = reader()
                endpoint.answer = { exchange, _ -> exchange.error(503, "temporarily_unavailable") }
                repeat(3) { exchange.get(another) }
                assertThat(endpoint.seen).hasSize(5)
                endpoint.answer = null
                assertThat(exchange.get(another)).isInstanceOf(Outcome.Minted::class.java)
                // forget: the host refused the token, so it is not presented again.
                val minted = exchange.get(another).minted()
                exchange.forget(another, mount)
                assertThat(exchange.get(another).minted()).isNotEqualTo(minted)
            }
        }
    }

    @Test
    fun `after the issuer refuses this server's client nothing is sent for ten seconds`() {
        endpoint().use { endpoint ->
            val wrong = MountExchange(endpoint.tokenUrl, endpoint.clientId, ClientSecret("x".repeat(40)))
            exchange(endpoint, config = wrong).use { exchange ->
                val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
                assertThat(exchange.get(reader())).isEqualTo(Outcome.Failed(Failure.MISCONFIGURED, "invalid_client"))
                // Any reader, any mount: answered here, the same failure, nothing sent.
                repeat(
                    50,
                ) { assertThat(exchange.get(reader())).isEqualTo(Outcome.Failed(Failure.MISCONFIGURED, "paused")) }
                assertThat(endpoint.seen).hasSize(1)
                advance(MountTokenExchange.MISCONFIGURED_PAUSE_SECONDS - 1)
                exchange.get(reader())
                assertThat(endpoint.seen).hasSize(1)
                advance(1)
                assertThat(exchange.get(reader())).isEqualTo(Outcome.Failed(Failure.MISCONFIGURED, "invalid_client"))
                assertThat(endpoint.seen).hasSize(2)
                assertThat(registry.counter("hoglake_mount_exchange_errors_total", "reason", "invalid_client").count())
                    .isEqualTo(2.0)
                assertThat(registry.counter("hoglake_mount_exchange_errors_total", "reason", "paused").count())
                    .isEqualTo(51.0)
                assertThat(registry.counter("hoglake_mount_exchange_requests_total").count()).isEqualTo(2.0)
                assertThat(registry.find("hoglake_mount_exchange_requests").timer()!!.count()).isEqualTo(2)
            }
            // A 429 is honoured for its Retry-After, as unavailable.
            exchange(endpoint).use { exchange ->
                endpoint.seen.clear()
                endpoint.answer = { exchange, _ -> exchange.error(429, "slow_down", mapOf("Retry-After" to "7")) }
                assertThat(exchange.get(reader())).isEqualTo(Outcome.Failed(Failure.UNAVAILABLE, "rate_limited"))
                endpoint.answer = null
                advance(6)
                assertThat(exchange.get(reader())).isEqualTo(Outcome.Failed(Failure.UNAVAILABLE, "paused"))
                assertThat(endpoint.seen).hasSize(1)
                advance(1)
                assertThat(exchange.get(reader())).isInstanceOf(Outcome.Minted::class.java)
            }
        }
    }

    @Test
    fun `at most thirty-two exchanges are in flight`() {
        endpoint().use { endpoint ->
            val release = CountDownLatch(1)
            endpoint.answer = { exchange, _ ->
                release.await(20, TimeUnit.SECONDS)
                exchange.error(400, "invalid_target")
            }
            exchange(endpoint).use { exchange ->
                runBlocking(Dispatchers.Default) {
                    val held =
                        (1..MountTokenExchange.MAX_IN_FLIGHT).map {
                            val subject = reader()
                            async(start = CoroutineStart.UNDISPATCHED) { exchange.token(subject, mount) }
                        }
                    assertThat(exchange.token(reader(), mount)).isEqualTo(Outcome.Failed(Failure.UNAVAILABLE, "busy"))
                    release.countDown()
                    assertThat(held.awaitAll().toSet()).containsExactly(Outcome.Refused(Refusal.UNKNOWN_MOUNT))
                }
                await().atMost(Duration.ofSeconds(5)).until { endpoint.seen.size == MountTokenExchange.MAX_IN_FLIGHT }
                endpoint.answer = null
                assertThat(exchange.get(reader())).isInstanceOf(Outcome.Minted::class.java)
            }
        }
    }

    @Test
    fun `each outcome is one typed answer, and none names the endpoint, the client or a token`() {
        val minted = Outcome.Minted("a.b.c")
        assertThat(MountTokenExchange.orThrow(minted, mount)).isEqualTo("a.b.c")
        assertThat(minted.toString()).doesNotContain("a.b.c")
        val thrown =
            mapOf(
                Outcome.Refused(Refusal.UNKNOWN_MOUNT) to HoglakeException.NotFound::class,
                Outcome.Refused(Refusal.NOT_ENTITLED) to HoglakeException.NotFound::class,
                Outcome.Refused(Refusal.SUBJECT_TOO_LARGE) to HoglakeException.NotFound::class,
                Outcome.Refused(Refusal.NEAR_EXPIRY) to HoglakeException.Unauthenticated::class,
                Outcome.Failed(Failure.UNAVAILABLE, "timeout") to HoglakeException.MountExchangeUnavailable::class,
                Outcome.Failed(Failure.MISCONFIGURED, "invalid_client") to
                    HoglakeException.MountExchangeMisconfigured::class,
                Outcome.Failed(Failure.INVALID_RESPONSE, "invalid_response") to
                    HoglakeException.MountExchangeInvalidResponse::class,
            )
        for ((outcome, type) in thrown) {
            assertThatThrownBy { MountTokenExchange.orThrow(outcome, mount) }
                .describedAs("%s", outcome)
                .isInstanceOf(type.java)
                .satisfies({ failure ->
                    assertThat(failure.message).doesNotContain("127.0.0.1", "hls_", "oauth", "invalid_")
                })
        }
        // A mount this reader has not, whatever the issuer's reason, is worded as a catalog that is not there.
        assertThatThrownBy { MountTokenExchange.orThrow(Outcome.Refused(Refusal.NOT_ENTITLED), mount) }
            .hasMessage("catalog '$mount' not found")
        assertThatThrownBy { MountTokenExchange.orThrow(Outcome.Refused(Refusal.NEAR_EXPIRY), mount) }
            .satisfies({ assertThat((it as HoglakeException.Unauthenticated).reason).isEqualTo("token_expired") })
    }

    @Test
    fun `the secret and the tokens are in no log line and no printed form`() {
        val capture = ListAppender<ILoggingEvent>().apply { start() }
        val root = LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME) as Logger
        val level = root.level
        root.level = Level.TRACE
        root.addAppender(capture)
        try {
            endpoint().use { endpoint ->
                val config = endpoint.exchangeConfig()
                val subjects = mutableListOf<String>()

                fun subject() = reader().also { subjects += it }
                exchange(endpoint, config = config).use { exchange ->
                    exchange.get(subject())
                    for (code in listOf("invalid_target", "invalid_grant", "unauthorized_client")) {
                        endpoint.answer = { e, _ -> e.error(400, code) }
                        exchange.get(subject())
                        advance(60)
                    }
                    endpoint.answer = { e, _ -> e.json(200, "garbage ${endpoint.clientSecret}") }
                    exchange.get(subject())
                    endpoint.answer = { e, _ -> e.error(401, "invalid_client") }
                    exchange.get(subject())
                }
                val closed = ServerSocket(0).use { it.localPort }
                val down =
                    MountExchange(
                        URI("http://127.0.0.1:$closed/oauth/token"),
                        endpoint.clientId,
                        ClientSecret(endpoint.clientSecret),
                    )
                exchange(endpoint, config = down).use { it.get(subject()) }

                val host =
                    MountHost(
                        id = hostId,
                        match = MountMatch.Events,
                        kind = MountHost.Kind.HOGLAKE,
                        origin = URI("http://127.0.0.1:1"),
                        auth = MountHost.Auth.EXCHANGE,
                        exchange = config,
                    )
                val printed = "$host $config ${config.clientSecret} ${listOf(config.clientSecret)}"
                assertThat(printed).contains("[redacted]")
                val lines = capture.list.map { it.formattedMessage + " " + (it.throwableProxy?.message ?: "") }
                // The exchange did log its failures, so there is something to search.
                assertThat(lines.filter { "token exchange failed" in it }).hasSizeGreaterThanOrEqualTo(4)
                val secrets =
                    listOf(endpoint.clientSecret, endpoint.basic.removePrefix("Basic "), endpoint.clientId) +
                        subjects + endpoint.minted
                for (secret in secrets) {
                    assertThat(lines).noneMatch { secret in it }
                    assertThat(printed).doesNotContain(secret.takeUnless { it == endpoint.clientId } ?: "\u0000")
                }
                // Nor the endpoint: a host is named by its id.
                assertThat(lines.filter { "token exchange failed" in it }).allMatch { hostId.toString() in it }
                assertThat(
                    lines.filter { "token exchange failed" in it },
                ).noneMatch { "127.0.0.1" in it || "oauth" in it }
            }
        } finally {
            root.detachAppender(capture)
            root.level = level
        }
    }

    @Test
    fun `exchange settings are refused unless they are the contract's`() {
        val secret = ClientSecret("x".repeat(32))
        val url = URI("http://hogtower.internal:8080/oauth/token")
        assertThat(MountExchange(url, "hls_0123456789abcdef01234567", secret).timeoutMs).isEqualTo(5_000)
        for (id in listOf(
            "",
            "hls_",
            "hlc_0123456789abcdef01234567",
            "hls_0123456789ABCDEF01234567",
            "hls_0123456789abcdef012345678",
        )) {
            assertThatThrownBy {
                MountExchange(
                    url,
                    id,
                    secret,
                )
            }.describedAs(id).isInstanceOf(IllegalArgumentException::class.java)
        }
        for (length in listOf(0, 31, 257)) {
            assertThatThrownBy { ClientSecret("x".repeat(length)) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .satisfies({ assertThat(it.message).doesNotContain("xxxx") })
        }
        assertThat(ClientSecret("x".repeat(256))).isEqualTo(ClientSecret("x".repeat(256)))
        val refused =
            listOf(
                "ftp://h/oauth/token",
                "http://h",
                "http://h/",
                "http://user:pw@h/oauth/token",
                "http://h/oauth/token?client_secret=x",
                "http://h/oauth/token#f",
                "http://h/oauth/../token",
                "http://h/oauth/%2e%2e/token",
                "http:///oauth/token",
                "/oauth/token",
                "http://h/oauth/token/",
            )
        for (value in refused) assertThat(MountExchange.tokenUrl(value)).describedAs(value).isNull()
        assertThat(MountExchange.tokenUrl("https://hogtower.internal:8443/oauth/token"))
            .isEqualTo(URI("https://hogtower.internal:8443/oauth/token"))
        for (timeout in listOf(0L, 30_001L)) {
            assertThatThrownBy { MountExchange(url, "hls_0123456789abcdef01234567", secret, timeout) }
                .isInstanceOf(IllegalArgumentException::class.java)
        }
    }

    private companion object {
        val INVALID = Outcome.Failed(Failure.INVALID_RESPONSE, "invalid_response")
    }
}
