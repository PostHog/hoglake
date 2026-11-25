package com.posthog.hoglake.hoglite

import com.posthog.hoglake.auth.JwtViolation
import com.posthog.hoglake.model.HoglakeException
import com.sun.net.httpserver.HttpServer
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.net.InetSocketAddress
import java.util.Base64
import java.util.concurrent.atomic.AtomicInteger

/** Deterministic engine refusals must not invite unchanged retries. */
class HogliteRefusalMappingTest {
    private val store = HogliteCatalogStore(HogliteClient("http://unused.invalid"), { "opaque_1" })

    /** A compact token whose only claim is [exp]; mapRejected reads it, nothing verifies it. */
    private fun tokenExpiring(exp: Long): String = tokenWith("""{"exp":$exp}""")

    /** A compact token whose payload is [claims]; mapRejected reads it, nothing verifies it. */
    private fun tokenWith(claims: String): String {
        val enc = Base64.getUrlEncoder().withoutPadding()
        val header = enc.encodeToString("""{"alg":"ES256"}""".toByteArray())
        val payload = enc.encodeToString(claims.toByteArray())
        return "$header.$payload.${enc.encodeToString("sig".toByteArray())}"
    }

    private fun storeFor(token: String?) =
        HogliteCatalogStore(HogliteClient("http://unused.invalid").forRequest(token), { "opaque_1" })

    /**
     * hoglite's 401 for a token hoglake accepted and that has not expired is
     * one of hoglite's own rules (#681): a 403 `backend_refused_authority`,
     * so a client does not re-mint the same claims in a loop.
     */
    @Test
    fun `a backend 401 for an unexpired token is a backend refusal, not a re-mint`() {
        val now = System.currentTimeMillis() / 1000
        for (token in listOf(tokenExpiring(now + 3600), null, "not-a-jwt")) {
            assertThat(storeFor(token).mapRejected("cat", HogliteRejected(401, """{"code":"refused"}""")))
                .describedAs("token %s", token)
                .isExactlyInstanceOf(HoglakeException.BackendRefusedAuthority::class.java)
        }
    }

    @Test
    fun `a backend 401 for an expired token stays a 401 the client renews`() {
        val now = System.currentTimeMillis() / 1000
        val refused = HogliteRejected(401, """{"code":"refused"}""")
        val failure = storeFor(tokenExpiring(now - 1)).mapRejected("cat", refused)
        assertThat(failure).isExactlyInstanceOf(HoglakeException.Unauthenticated::class.java)
        assertThat((failure as HoglakeException.Unauthenticated).reason).isEqualTo(JwtViolation.EXPIRED)
    }

    /**
     * A token issued more than hoglite's fixed 60 s ahead of hoglake's clock
     * is one hoglite admits once its own clock catches up (hoglake's skew
     * above 60 s): a 503 whose Retry-After is that wait, so a client retries
     * the same token rather than give up on it (#681). A token ahead by less
     * is inside hoglite's leeway, so its refusal is another rule's: a 403.
     */
    @Test
    fun `a backend 401 for a token issued ahead of the clock is retried after hoglite's leeway`() {
        val now = 1_800_000_000L
        val exp = now + 3600

        fun refusal(claims: String) =
            HogliteClient(
                "http://unused.invalid",
            ).forRequest(tokenWith(claims)).authorityRefusal(nowSeconds = now)

        for ((claims, wait) in listOf(
            """{"exp":$exp,"iat":${now + 120},"nbf":${now + 120}}""" to 60L,
            """{"exp":$exp,"iat":$now,"nbf":${now + 150}}""" to 90L,
            """{"exp":$exp,"iat":${now + 170}}""" to 110L,
            """{"exp":$exp,"iat":${now + 61}}""" to 1L,
            """{"exp":$exp,"nbf":${now + 3000}}""" to HogliteClient.MAX_NOT_YET_VALID_RETRY_SECONDS,
        )) {
            val failure = refusal(claims)
            assertThat(failure).describedAs(claims)
                .isExactlyInstanceOf(HoglakeException.BackendTokenNotYetValid::class.java)
            assertThat((failure as HoglakeException.BackendTokenNotYetValid).retryAfterSeconds)
                .describedAs(claims).isEqualTo(wait)
        }
        // Not past hoglite's leeway, or not a time hoglake reads: hoglite's own rule, final.
        for (claims in listOf(
            """{"exp":$exp,"iat":$now,"nbf":$now}""",
            """{"exp":$exp,"iat":${now - 30}}""",
            """{"exp":$exp,"iat":${now + 1}}""",
            """{"exp":$exp,"iat":${now + 30},"nbf":${now + 60}}""",
            """{"exp":$exp,"iat":"${now + 120}"}""",
        )) {
            assertThat(refusal(claims)).describedAs(claims)
                .isExactlyInstanceOf(HoglakeException.BackendRefusedAuthority::class.java)
        }
        // A token past its exp is renewed first.
        assertThat(refusal("""{"exp":${now - 1},"iat":${now + 120}}"""))
            .isExactlyInstanceOf(HoglakeException.Unauthenticated::class.java)
        // The catalog store maps hoglite's 401 through it.
        val wall = System.currentTimeMillis() / 1000
        val ahead = tokenWith("""{"exp":${wall + 3600},"iat":${wall + 600}}""")
        val mapped = storeFor(ahead).mapRejected("cat", HogliteRejected(401, """{"code":"refused"}"""))
        assertThat(mapped).isExactlyInstanceOf(HoglakeException.BackendTokenNotYetValid::class.java)
        assertThat((mapped as HoglakeException.BackendTokenNotYetValid).retryAfterSeconds).isPositive()
    }

    /**
     * hoglite's clock, from its refusal's `Date`, measures a token issued
     * ahead when it trails hoglake's (review of #724). A token 65 s ahead of
     * hoglake's clock on a hoglite 2 s behind it waits 7 s, not 5; the retry
     * hoglake's clock alone would have asked for, still early on hoglite, is
     * a 503 again with the rest of the wait, never the final 403. A `Date`
     * ahead of hoglake's clock may be a relaying pod's, not the verifier's,
     * so hoglake's clock measures the lead then.
     */
    @Test
    fun `hoglite's clock in its refusal measures the wait for a token issued ahead`() {
        val now = 1_800_000_000L
        val base = HogliteClient("http://unused.invalid")
        val ahead = base.forRequest(tokenWith("""{"exp":${now + 3600},"iat":${now + 65}}"""))

        fun waitAt(
            client: HogliteClient,
            hoglite: Long,
            hoglake: Long,
        ): Long? {
            val refusal = client.authorityRefusal(hoglite, hoglake)
            return (refusal as? HoglakeException.BackendTokenNotYetValid)?.retryAfterSeconds
        }

        assertThat(waitAt(ahead, now - 2, now)).isEqualTo(7L)
        assertThat(waitAt(ahead, now + 3, now + 5)).isEqualTo(2L)
        // A relayed Date 10 s ahead reads a 55 s lead; an owner on hoglake's clock saw 65 s.
        assertThat(waitAt(ahead, now + 10, now)).isEqualTo(5L)
        // Within hoglite's leeway on both clocks: another rule refused it.
        val inside = base.forRequest(tokenWith("""{"exp":${now + 3600},"iat":${now + 55}}"""))
        assertThat(inside.authorityRefusal(now + 10, now))
            .isExactlyInstanceOf(HoglakeException.BackendRefusedAuthority::class.java)
        // Within hoglake's 60 s but past them on a hoglite clock 3 s behind.
        val near = base.forRequest(tokenWith("""{"exp":${now + 3600},"iat":${now + 59}}"""))
        assertThat(waitAt(near, now - 3, now)).isEqualTo(2L)
        // The store hands the refusal's clock on; it reads this pod's clock off the wall.
        val wall = System.currentTimeMillis() / 1000
        val wallAhead = base.forRequest(tokenWith("""{"exp":${wall + 3600},"iat":${wall + 65}}"""))
        val mapped =
            HogliteCatalogStore(wallAhead, { "opaque_1" }).mapRejected(
                "cat",
                HogliteRejected(401, """{"code":"refused"}""", backendClockSeconds = wall - 2),
            )
        assertThat((mapped as HoglakeException.BackendTokenNotYetValid).retryAfterSeconds).isEqualTo(7L)
    }

    /**
     * hoglite's exp rule runs on its clock: a token with 20 s left by
     * hoglake's clock is past hoglite's 60 s on a hoglite 90 s ahead, and a
     * re-mint fixes it, so the answer is 401 `token_expired`, not the 403
     * that tells the client not to re-mint.
     */
    @Test
    fun `a token past its exp on hoglite's clock is expired`() {
        val now = 1_800_000_000L
        val client = HogliteClient("http://unused.invalid").forRequest(tokenExpiring(now + 20))
        val failure = client.authorityRefusal(now + 90, now)
        assertThat(failure).isExactlyInstanceOf(HoglakeException.Unauthenticated::class.java)
        assertThat((failure as HoglakeException.Unauthenticated).reason).isEqualTo(JwtViolation.EXPIRED)
        // Within hoglite's 60 s past exp, its exp rule did not refuse it.
        assertThat(client.authorityRefusal(now + 80, now))
            .isExactlyInstanceOf(HoglakeException.BackendRefusedAuthority::class.java)
    }

    /** The call loop reads hoglite's clock from its 401's `Date` (review of #724). */
    @Test
    fun `a backend 401 carries hoglite's Date to the refusal`() {
        val wall = System.currentTimeMillis() / 1000
        val token = tokenWith("""{"exp":${wall + 3600},"iat":${wall + 65}}""")
        HogliteRawAnswer(
            401,
            HogliteRawAnswer.httpDate(wall - 30),
            """{"error":"request not authorized","code":"refused"}""",
        ).use { hoglite ->
            HogliteClient(hoglite.url).use { client ->
                val selected = HogliteCatalogStore(client.forRequest(token), { "opaque_1" })
                val failure =
                    catchThrowable {
                        selected.catalogOperation("cat") { selected.queryCatalog("cat", "SELECT 1") }
                    }
                assertThat(failure).isExactlyInstanceOf(HoglakeException.BackendTokenNotYetValid::class.java)
                // 95 s ahead of hoglite's Date, so 35 s; hoglake's clock alone would say 5.
                assertThat((failure as HoglakeException.BackendTokenNotYetValid).retryAfterSeconds).isEqualTo(35L)
            }
            assertThat(hoglite.calls.get()).isEqualTo(1)
        }
    }

    /**
     * A `/v1/query-batch` item's 401 carries no `Date`: the first such item
     * is re-read on its own, and its 401 brings the `Date` the refusal is
     * judged on. That refusal fails the call, so the other refused items
     * are not re-read one by one (#681).
     */
    @Test
    fun `a batch item 401 is judged on the clock of its own re-read`() {
        val wall = System.currentTimeMillis() / 1000
        val token = tokenWith("""{"exp":${wall + 3600},"iat":${wall + 65}}""")
        val item = """{"status":401,"body":{"code":"refused"}}"""
        HogliteRawAnswer(
            401,
            HogliteRawAnswer.httpDate(wall - 30),
            """{"error":"request not authorized","code":"refused"}""",
            byPath =
                mapOf(
                    "/v1/query-batch" to
                        (200 to """{"results":[$item,$item,$item]}"""),
                ),
        ).use { hoglite ->
            HogliteClient(hoglite.url).use { client ->
                val failure =
                    catchThrowable {
                        HogliteCatalogStore(client.forRequest(token), { "opaque_1" }).listCatalogs(setOf("a", "b", "c"))
                    }
                assertThat(failure).isExactlyInstanceOf(HoglakeException.BackendTokenNotYetValid::class.java)
                // 95 s ahead of the re-read's Date, so 35 s; hoglake's clock alone would say 5.
                assertThat((failure as HoglakeException.BackendTokenNotYetValid).retryAfterSeconds).isEqualTo(35L)
            }
            assertThat(hoglite.calls.get()).isEqualTo(2)
        }
    }

    /**
     * When the first refused batch item's re-read succeeds, the other
     * refused items are re-read too rather than dropped from the list
     * (review of #724). The fake answers every tenant query with the meta
     * row of `a`, so `b` and `c` are skipped on the name check; the call
     * count is what shows they were re-read.
     */
    @Test
    fun `batch item 401s are all re-read when the first re-read succeeds`() {
        val wall = System.currentTimeMillis() / 1000
        val token = tokenWith("""{"exp":${wall + 3600},"iat":${wall - 5}}""")
        val item = """{"status":401,"body":{"code":"refused"}}"""
        val row = """["a","s3://lake/a/",0,1,0,null,null,0]"""
        val meta = """{"cols":["c0","c1","c2","c3","c4","c5","c6","c7"],"rows":[$row],"affected":0}"""
        HogliteRawAnswer(
            401,
            HogliteRawAnswer.httpDate(wall),
            """{"error":"request not authorized","code":"refused"}""",
            byPath =
                mapOf(
                    "/v1/query-batch" to (200 to """{"results":[$item,$item,$item]}"""),
                    "/v1/t/opaque_1/query" to (200 to meta),
                ),
        ).use { hoglite ->
            HogliteClient(hoglite.url).use { client ->
                val store = HogliteCatalogStore(client.forRequest(token), { "opaque_1" })
                val listed = store.listCatalogs(setOf("a", "b", "c"))
                assertThat(listed.map { it.name }).containsExactly("a")
            }
            assertThat(hoglite.calls.get()).isEqualTo(4)
        }
    }

    @Test
    fun `a backend 403 keeps the forbidden code`() {
        assertThat(store.mapRejected("cat", HogliteRejected(403, """{"code":"refused"}""")))
            .isExactlyInstanceOf(HoglakeException.Forbidden::class.java)
    }

    @ParameterizedTest
    @ValueSource(ints = [400, 409, 422])
    fun `SQL refusals and malformed requests map to Validation including native 422`(status: Int) {
        val error = store.mapRejected("cat", HogliteRejected(status, """{"code":"refused","error":"SQL rejected"}"""))
        assertThat(error).isExactlyInstanceOf(HoglakeException.Validation::class.java)
        assertThat(error.message).contains("SQL rejected")
        assertThat(error).isNotInstanceOf(HoglakeException.BackendUnavailable::class.java)
    }

    /**
     * A FOREIGN KEY refusal — an orphan the schema turned from corruption
     * into a refused transaction — is a store bug: a 500 with its stack,
     * never a client error. Both channels: /tx's 422 `refused` and
     * /query's bare 409; immediate and commit-time (deferred) alike.
     */
    @ParameterizedTest
    @ValueSource(ints = [409, 422])
    fun `a foreign key refusal is an integrity violation, not a client error`(status: Int) {
        val error =
            store.mapRejected(
                "cat",
                HogliteRejected(status, """{"code":"refused","error":"FOREIGN KEY constraint failed"}"""),
            )
        assertThat(error).isExactlyInstanceOf(HogliteIntegrityViolation::class.java)
        assertThat(error).isNotInstanceOf(HoglakeException::class.java)
        assertThat(error.message).contains("catalog 'cat'", "store bug", "FOREIGN KEY constraint failed")
        // A CHECK refusal stays a Validation: the row shape, not a dangling reference.
        val check = HogliteRejected(status, """{"code":"refused","error":"CHECK constraint failed: files"}""")
        assertThat(store.mapRejected("cat", check)).isExactlyInstanceOf(HoglakeException.Validation::class.java)
    }

    @ParameterizedTest
    @ValueSource(ints = [400, 409, 422])
    fun `known malformed and SQL refusal statuses stay final without a label`(status: Int) {
        assertThat(store.mapRejected("cat", HogliteRejected(status, "malformed request")))
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `result cap and engine query timeout have separate nonretryable types`() {
        assertThat(store.mapRejected("cat", HogliteRejected(413, """{"code":"result_too_large"}""")))
            .isExactlyInstanceOf(HoglakeException.BackendResultTooLarge::class.java)
        assertThat(store.mapRejected("cat", HogliteRejected(504, """{"code":"query_timeout"}""")))
            .isExactlyInstanceOf(HoglakeException.BackendQueryTimeout::class.java)
    }

    @Test
    fun `only transient engine rejection statuses retain outage semantics`() {
        assertThat(store.mapRejected("cat", HogliteRejected(503, """{"code":"not_applied"}""")))
            .isInstanceOf(HoglakeException.BackendUnavailable::class.java)
        assertThat(store.mapRejected("cat", HogliteRejected(507, """{"code":"out_of_memory"}""")))
            .isExactlyInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `read transaction refused SQL reaches Validation after one transport attempt`() {
        val calls = AtomicInteger()
        val server =
            HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                createContext("/") { ex ->
                    calls.incrementAndGet()
                    val bytes = """{"code":"refused","error":"invalid SQL"}""".toByteArray()
                    ex.sendResponseHeaders(422, bytes.size.toLong())
                    ex.responseBody.use { it.write(bytes) }
                }
                start()
            }
        try {
            HogliteClient("http://127.0.0.1:${server.address.port}").use { client ->
                val selected = HogliteCatalogStore(client, { "opaque_1" })
                assertThatThrownBy {
                    selected.catalogOperation("cat") {
                        selected.readCatalog("cat", listOf(HogliteStatement("SELECT invalid")))
                    }
                }.isInstanceOf(HoglakeException.Validation::class.java)
            }
            assertThat(calls.get()).isEqualTo(1)
        } finally {
            server.stop(0)
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [500, 502])
    fun `unknown server errors remain retryable outages`(status: Int) {
        for (body in listOf("{}", "gateway failure", """{"code":"internal_error","error":"failed"}""")) {
            assertThat(store.mapRejected("cat", HogliteRejected(status, body)))
                .isExactlyInstanceOf(HoglakeException.BackendUnavailable::class.java)
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [500, 502])
    fun `both read channels preserve unknown server failures`(status: Int) {
        val calls = AtomicInteger()
        val server =
            HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                createContext("/") { ex ->
                    ex.requestBody.readBytes()
                    calls.incrementAndGet()
                    val bytes = """{"error":"upstream failed"}""".toByteArray()
                    ex.sendResponseHeaders(status, bytes.size.toLong())
                    ex.responseBody.use { it.write(bytes) }
                }
                start()
            }
        try {
            HogliteClient("http://127.0.0.1:${server.address.port}").use { client ->
                val selected = HogliteCatalogStore(client, { "opaque_1" })
                for (batch in listOf(false, true)) {
                    assertThatThrownBy {
                        selected.catalogOperation("cat") {
                            if (batch) {
                                selected.readCatalog("cat", listOf(HogliteStatement("SELECT 1")))
                            } else {
                                selected.queryCatalog("cat", "SELECT 1")
                            }
                        }
                    }.isExactlyInstanceOf(HoglakeException.BackendUnavailable::class.java)
                }
            }
            assertThat(calls.get()).isEqualTo(2)
        } finally {
            server.stop(0)
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [401, 403])
    fun `create batch and fallback boundaries preserve definite authority refusals`(status: Int) {
        for (mode in listOf("create", "batch", "batch-item", "batch-fallback")) {
            val calls = AtomicInteger()
            val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
            server.createContext("/") { ex ->
                ex.requestBody.readBytes()
                calls.incrementAndGet()
                val batch = ex.requestURI.path == "/v1/query-batch"
                val body =
                    when {
                        batch && mode == "batch-fallback" -> "{}"
                        batch && mode == "batch-item" ->
                            """{"results":[{"status":$status,"body":{"code":"refused"}}]}"""
                        else -> """{"code":"refused"}"""
                    }.toByteArray()
                val responseStatus =
                    when {
                        batch && mode == "batch-fallback" -> 404
                        batch && mode == "batch-item" -> 200
                        else -> status
                    }
                ex.sendResponseHeaders(responseStatus, body.size.toLong())
                ex.responseBody.use { it.write(body) }
            }
            server.start()
            try {
                HogliteClient("http://127.0.0.1:${server.address.port}").use { client ->
                    val selected = HogliteCatalogStore(client, { "opaque_1" })
                    val expected =
                        if (status == 401) {
                            HoglakeException.BackendRefusedAuthority::class.java
                        } else {
                            HoglakeException.Forbidden::class.java
                        }
                    assertThatThrownBy {
                        if (mode == "create") {
                            selected.createCatalog("cat", "s3://bucket/cat", null, null)
                        } else {
                            selected.listCatalogs(setOf("cat"))
                        }
                    }.isInstanceOf(expected)
                    // A batch item's 401 is re-read on its own for its Date.
                    val retried = mode == "batch-fallback" || (mode == "batch-item" && status == 401)
                    assertThat(calls.get()).isEqualTo(if (retried) 2 else 1)
                }
            } finally {
                server.stop(0)
            }
        }
    }

    @ParameterizedTest
    @ValueSource(ints = [404, 409, 413])
    fun `new authority wrappers preserve non-authentication rejection semantics`(status: Int) {
        for (mode in listOf("create", "batch")) {
            val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
            val body = """{"code":"result_too_large","error":"whole request rejected"}"""
            server.createContext("/") { ex ->
                ex.requestBody.readBytes()
                val bytes = body.toByteArray()
                ex.sendResponseHeaders(status, bytes.size.toLong())
                ex.responseBody.use { it.write(bytes) }
            }
            server.start()
            try {
                HogliteClient("http://127.0.0.1:${server.address.port}").use { client ->
                    val selected = HogliteCatalogStore(client, { "opaque_$it" })
                    val failure =
                        catchThrowable {
                            if (mode == "create") {
                                selected.createCatalog("cat", "s3://bucket/cat", null, null)
                            } else {
                                selected.listCatalogs(setOf("first", "second"))
                            }
                        }
                    if (status == 409 && mode == "create") {
                        // Creation's idempotent 409 proceeds to its existing bootstrap guard classification.
                        assertThat(failure).isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
                    } else {
                        assertThat(failure).isExactlyInstanceOf(HogliteRejected::class.java)
                        val refused = failure as HogliteRejected
                        assertThat(refused.status).isEqualTo(status)
                        assertThat(refused.body).isEqualTo(body)
                    }
                }
            } finally {
                server.stop(0)
            }
        }
    }
}
