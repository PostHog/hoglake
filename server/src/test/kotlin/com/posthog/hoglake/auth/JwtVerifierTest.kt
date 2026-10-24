package com.posthog.hoglake.auth

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.security.KeyPair

/**
 * The token verifier, driven with tokens this test signs itself.
 *
 * Every refusal here is a case someone has shipped a bug for somewhere: the
 * `alg: none` downgrade, HS256-with-the-public-key confusion, a float `exp`, a
 * duplicate JSON key, an off-curve JWK. They are grouped in one class because
 * they share a fixture and because a reader deciding whether the verifier is
 * safe should be able to see all of them in one place.
 */
class JwtVerifierTest {
    private val pair: KeyPair = TestTokens.keyPair()
    private val other: KeyPair = TestTokens.keyPair()
    private val now = 2_000_000_000L

    private fun verifier(
        keys: SigningKeys = StaticSigningKeys(TestTokens.KID, pair.public as java.security.interfaces.ECPublicKey),
        issuer: String = TestTokens.ISSUER,
        audience: String = TestTokens.AUDIENCE,
        skew: Long = 60,
    ) = JwtVerifier(issuer, audience, keys, skew) { now }

    private fun mint(
        claims: String = TestTokens.tenantClaims(),
        signWith: KeyPair = pair,
        header: String = """{"alg":"ES256","kid":"${TestTokens.KID}","typ":"JWT"}""",
    ) = TestTokens.mint(signWith, claims, header)

    // ---- the happy path -------------------------------------------------

    @Test
    fun `a tenant token becomes a caller scoped to its catalogs`() {
        val caller =
            verifier().verify(
                mint(
                    TestTokens.tenantClaims(
                        catalogs = mapOf("lake" to listOf("read", "write"), "pond" to listOf("read")),
                    ),
                ),
            )

        assertThat(caller.kind).isEqualTo(Caller.Kind.TENANT)
        assertThat(caller.orgId).isEqualTo("11111111-1111-1111-1111-111111111111")
        assertThat(caller.subject).isEqualTo("client:hedgerow-prod")
        assertThat(caller.actor).isEqualTo("client:hedgerow-prod")
        assertThat(caller.catalogScopes.keys).containsExactlyInAnyOrder("lake", "pond")
        assertThat(caller.visibleCatalogs()).containsExactlyInAnyOrder("lake", "pond")
        assertThat(caller.knowsCatalog("lake")).isTrue()
        assertThat(caller.knowsCatalog("someone-elses-lake")).isFalse()
    }

    @Test
    fun `an operator token carries no catalog list but may see everything`() {
        val caller = verifier().verify(mint(TestTokens.operatorClaims()))
        assertThat(caller.kind).isEqualTo(Caller.Kind.OPERATOR)
        assertThat(caller.orgId).isNull()
        assertThat(caller.visibleCatalogs()).isNull()
        assertThat(caller.knowsCatalog("any-catalog-at-all")).isTrue()
        assertThat(caller.actor).isEqualTo("operator:sa:bulk-loader")
    }

    @Test
    fun `scope strength nests, and admin is the only one that reaches maintenance`() {
        val caller =
            verifier().verify(
                mint(TestTokens.tenantClaims(catalogs = mapOf("lake" to listOf("admin"), "pond" to listOf("write")))),
            )
        assertThat(caller.holds("lake", Scope.READ)).isTrue()
        assertThat(caller.holds("lake", Scope.MAINTENANCE)).isTrue()
        assertThat(caller.holds("lake", Scope.ADMIN)).isTrue()
        assertThat(caller.holds("pond", Scope.WRITE)).isTrue()
        assertThat(caller.holds("pond", Scope.READ)).isTrue()
        // The load-bearing direction: a writer may not compact.
        assertThat(caller.holds("pond", Scope.MAINTENANCE)).isFalse()
        assertThat(caller.holds("pond", Scope.ADMIN)).isFalse()
    }

    @Test
    fun `an audience array containing ours is accepted`() {
        val claims = TestTokens.tenantClaims().replace("\"aud\":\"hoglake\"", "\"aud\":[\"other\",\"hoglake\"]")
        assertThat(verifier().verify(mint(claims)).subject).isEqualTo("client:hedgerow-prod")
    }

    @Test
    fun `an unrecognised scope name is dropped, not fatal`() {
        // A newer issuer minting a scope this build has never heard of must not
        // take every caller down during a rolling upgrade.
        val claims = TestTokens.tenantClaims(catalogs = mapOf("lake" to listOf("read", "quantum")))
        val caller = verifier().verify(mint(claims))
        assertThat(caller.catalogScopes["lake"]).containsExactly(Scope.READ)
    }

    // ---- algorithm attacks ----------------------------------------------

    @Test
    fun `alg none is refused before any key is consulted`() {
        val token =
            TestTokens.mint(
                pair,
                TestTokens.tenantClaims(),
                header = """{"alg":"none","typ":"JWT"}""",
                sign = false,
            )
        assertThatThrownBy { verifier().verify(token) }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(JwtViolation.UNSUPPORTED_ALG)
    }

    @Test
    fun `HS256 is refused, so a public key cannot be smuggled in as an HMAC secret`() {
        val token =
            TestTokens.mint(pair, TestTokens.tenantClaims(), header = """{"alg":"HS256","kid":"${TestTokens.KID}"}""")
        assertThatThrownBy { verifier().verify(token) }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(JwtViolation.UNSUPPORTED_ALG)
    }

    @Test
    fun `a critical header extension is refused rather than ignored`() {
        val token =
            TestTokens.mint(
                pair,
                TestTokens.tenantClaims(),
                header = """{"alg":"ES256","kid":"${TestTokens.KID}","crit":["exp"]}""",
            )
        assertThatThrownBy { verifier().verify(token) }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(JwtViolation.MALFORMED)
    }

    // ---- signature ------------------------------------------------------

    @Test
    fun `a token signed by a key we do not hold is refused`() {
        assertThatThrownBy { verifier().verify(mint(signWith = other)) }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(JwtViolation.BAD_SIGNATURE)
    }

    @Test
    fun `a tampered payload fails the signature, not the claim check`() {
        val good = mint()
        val parts = good.split('.')
        val forgedPayload =
            java.util.Base64.getUrlEncoder().withoutPadding()
                .encodeToString(
                    TestTokens.tenantClaims(catalogs = mapOf("everyones-lake" to listOf("admin"))).toByteArray(),
                )
        val forged = "${parts[0]}.$forgedPayload.${parts[2]}"
        assertThatThrownBy { verifier().verify(forged) }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(JwtViolation.BAD_SIGNATURE)
    }

    @Test
    fun `an unknown kid asks for one background re-read and refuses the request`() {
        var refreshes = 0
        val keys =
            object : SigningKeys {
                override fun byKeyId(keyId: String?) = null

                // The request path must never re-read synchronously: this used to be
                // one outbound fetch per unauthenticated request, on the event loop.
                override fun refresh() = error("refresh() is the boot path; a request called it")

                override fun refreshInBackground() {
                    refreshes += 1
                }
            }
        assertThatThrownBy { verifier(keys = keys).verify(mint()) }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(JwtViolation.UNKNOWN_KEY)
        assertThat(refreshes).isEqualTo(1)
    }

    @Test
    fun `a token with no kid is refused without touching the key source`() {
        // The cheapest token an attacker can send must be the cheapest to refuse.
        // It can name no key, so asking for a re-read could never help.
        var touches = 0
        val keys =
            object : SigningKeys {
                override fun byKeyId(keyId: String?) = null

                override fun refresh() {
                    touches += 1
                }

                override fun refreshInBackground() {
                    touches += 1
                }
            }
        val headerless = mint(TestTokens.tenantClaims(), header = """{"alg":"ES256","typ":"JWT"}""")
        assertThatThrownBy { verifier(keys = keys).verify(headerless) }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(JwtViolation.UNKNOWN_KEY)
        assertThat(touches).isZero()
    }

    @Test
    fun `a background re-read is rate limited to one per interval`() {
        var fetches = 0
        var now = 1_000L
        val keys =
            JwksSigningKeys(
                fetch = {
                    fetches += 1
                    TestTokens.jwks(pair).toByteArray()
                },
                describe = "rate-limit-test",
                minRefreshIntervalMs = 30_000,
                clock = { now },
                runAsync = { it() },
            )
        // Unlike fromFile, the constructor does not fetch: boot asks explicitly.
        assertThat(fetches).isZero()

        repeat(50) { keys.refreshInBackground() }
        assertThat(fetches).isEqualTo(1)

        now += 30_000
        keys.refreshInBackground()
        assertThat(fetches).isEqualTo(2)

        now += 29_999
        repeat(50) { keys.refreshInBackground() }
        assertThat(fetches).isEqualTo(2)
    }

    @Test
    fun `a background re-read hands the work off instead of doing it`() {
        var fetches = 0
        val queued = mutableListOf<() -> Unit>()
        val keys =
            JwksSigningKeys(
                fetch = {
                    fetches += 1
                    TestTokens.jwks(pair).toByteArray()
                },
                describe = "handoff-test",
                minRefreshIntervalMs = 0,
                clock = { 1L },
                runAsync = { queued += it },
            )
        keys.refreshInBackground()
        // The caller returns without the fetch having happened, which is the point:
        // nothing on the request path waits on the JWKS endpoint.
        assertThat(fetches).isZero()
        assertThat(queued).hasSize(1)

        queued.single().invoke()
        assertThat(fetches).isEqualTo(1)
        assertThat(keys.byKeyId(TestTokens.KID)).isNotNull()
    }

    @Test
    fun `a rotation lands on the re-read after the miss, not on the miss`() {
        // The cost of refusing the triggering request, stated as a test. The
        // handoff is deferred here exactly as the real thread pool defers it, so
        // the two halves are observable: the miss refuses, and the next request
        // after the re-read succeeds.
        val rotated = TestTokens.keyPair()
        var served = TestTokens.jwks(pair)
        var nowMs = 1_000L
        val queued = mutableListOf<() -> Unit>()
        val keys =
            JwksSigningKeys(
                fetch = { served.toByteArray() },
                describe = "rotation-test",
                minRefreshIntervalMs = 30_000,
                clock = { nowMs },
                runAsync = { queued += it },
            )
        keys.refresh()
        assertThat(keys.byKeyId(TestTokens.KID)).isNotNull()
        val v = verifier(keys = keys)
        val newKid = """{"alg":"ES256","kid":"test-key-2","typ":"JWT"}"""

        served = TestTokens.jwks(rotated, kid = "test-key-2")
        assertThatThrownBy { v.verify(mint(TestTokens.tenantClaims(), rotated, newKid)) }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(JwtViolation.UNKNOWN_KEY)

        // The re-read the refusal asked for, run off-thread.
        assertThat(queued).hasSize(1)
        queued.single().invoke()

        nowMs += 30_000
        assertThat(v.verify(mint(TestTokens.tenantClaims(), rotated, newKid)).subject)
            .isEqualTo("client:hedgerow-prod")
    }

    @Test
    fun `a re-read that completes before the retry lets the request through`() {
        // The other half of the same behaviour, and worth pinning: the verifier
        // looks once more after asking for a re-read, so a source fast enough to
        // answer inline (the file source on a local disk) serves the request
        // rather than refusing it. An implementation that refused unconditionally
        // would pass the test above and lose this.
        val rotated = TestTokens.keyPair()
        var served = TestTokens.jwks(pair)
        val keys =
            JwksSigningKeys(
                fetch = { served.toByteArray() },
                describe = "inline-refresh-test",
                minRefreshIntervalMs = 30_000,
                clock = { 1_000L },
                runAsync = { it() },
            )
        keys.refresh()
        val v = verifier(keys = keys)

        served = TestTokens.jwks(rotated, kid = "test-key-2")
        assertThat(
            v.verify(mint(TestTokens.tenantClaims(), rotated, """{"alg":"ES256","kid":"test-key-2","typ":"JWT"}"""))
                .subject,
        ).isEqualTo("client:hedgerow-prod")
    }

    // ---- claims ---------------------------------------------------------

    @Test
    fun `an expired token names its reason`() {
        val token = mint(TestTokens.tenantClaims(exp = now - 3600))
        assertThatThrownBy { verifier().verify(token) }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(JwtViolation.EXPIRED)
    }

    @Test
    fun `the clock skew tolerates a slightly stale exp and no more`() {
        assertThat(verifier(skew = 60).verify(mint(TestTokens.tenantClaims(exp = now - 30))).subject)
            .isEqualTo("client:hedgerow-prod")
        assertThatThrownBy { verifier(skew = 60).verify(mint(TestTokens.tenantClaims(exp = now - 120))) }
            .isInstanceOf(JwtViolation::class.java)
    }

    @Test
    fun `a not-yet-valid token is refused`() {
        val token = mint(TestTokens.tenantClaims(nbf = now + 3600))
        assertThatThrownBy { verifier().verify(token) }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(JwtViolation.NOT_YET_VALID)
    }

    @Test
    fun `a token issued in the future is refused, so iat cannot extend a TTL`() {
        val token = mint(TestTokens.tenantClaims(iat = now + 3600))
        assertThatThrownBy { verifier().verify(token) }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(JwtViolation.NOT_YET_VALID)
    }

    @Test
    fun `the issuer must be exactly ours`() {
        val token = mint(TestTokens.tenantClaims(iss = "https://hoglake-other-region.test"))
        assertThatThrownBy { verifier().verify(token) }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(JwtViolation.WRONG_ISSUER)
    }

    @Test
    fun `the audience must include ours`() {
        val token = mint(TestTokens.tenantClaims(aud = "somebody-else"))
        assertThatThrownBy { verifier().verify(token) }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(JwtViolation.WRONG_AUDIENCE)
    }

    @Test
    fun `a tenant token with no catalogs is refused rather than treated as an operator`() {
        val token = mint(TestTokens.tenantClaims(catalogs = emptyMap()))
        assertThatThrownBy { verifier().verify(token) }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(JwtViolation.MISSING_CLAIM)
    }

    @Test
    fun `an unknown ten kind is refused`() {
        val token = mint(TestTokens.tenantClaims().replace("\"ten\":\"tenant\"", "\"ten\":\"root\""))
        assertThatThrownBy { verifier().verify(token) }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(JwtViolation.MALFORMED)
    }

    @Test
    fun `a non-integer exp is refused, so 1e400 cannot become infinity`() {
        val token = mint(TestTokens.tenantClaims().replace("\"exp\":4000000000", "\"exp\":1e400"))
        assertThatThrownBy { verifier().verify(token) }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(JwtViolation.MISSING_CLAIM)
    }

    @Test
    fun `a duplicate claim key is a parse failure, not last-one-wins`() {
        val claims = TestTokens.tenantClaims().replace("\"exp\":4000000000", "\"exp\":1,\"exp\":4000000000")
        assertThatThrownBy { verifier().verify(mint(claims)) }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(JwtViolation.MALFORMED)
    }

    // ---- wire format ----------------------------------------------------

    @Test
    fun `a token without three segments is malformed`() {
        assertThatThrownBy { verifier().verify("only.two") }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(JwtViolation.MALFORMED)
    }

    @Test
    fun `standard base64 characters are refused, because the wire form is url-safe`() {
        // Replace a character we KNOW is there: `replace('-', …)` on a token that
        // happens to contain no '-' is a no-op, and the assertion below would
        // then pass or fail on the luck of the base64 encoding.
        val token = mint()
        val at = token.indexOfFirst { it.isLetterOrDigit() }
        val forStandardBase64 = token.substring(0, at) + "+" + token.substring(at + 1)
        assertThatThrownBy { verifier().verify(forStandardBase64) }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(JwtViolation.MALFORMED)
    }

    // ---- JWK ------------------------------------------------------------

    @Test
    fun `a jwk whose point is off the curve is refused`() {
        val jwks = TestTokens.jwks(pair).replace("\"y\":\"", "\"y\":\"A")
        assertThatThrownBy { JwksSigningKeys.parseJwks(jwks.toByteArray()) }
            .isInstanceOf(JwtViolation::class.java)
    }

    @Test
    fun `a jwk without a kid is refused, so rotation is unambiguous`() {
        val jwks = TestTokens.jwks(pair).replace("\"kid\":\"${TestTokens.KID}\",", "")
        assertThatThrownBy { JwksSigningKeys.parseJwks(jwks.toByteArray()) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `a jwks round-trips through the file key source`(
        @org.junit.jupiter.api.io.TempDir dir: java.nio.file.Path,
    ) {
        val file = dir.resolve("jwks.json")
        java.nio.file.Files.writeString(file, TestTokens.jwks(pair))
        val keys = JwksSigningKeys.fromFile(file)
        assertThat(keys.byKeyId(TestTokens.KID)).isNotNull()
        assertThat(keys.byKeyId("not-a-kid")).isNull()
        assertThat(JwtVerifier(TestTokens.ISSUER, TestTokens.AUDIENCE, keys) { now }.verify(mint()).subject)
            .isEqualTo("client:hedgerow-prod")
    }

    @Test
    fun `a failed refresh keeps the previous key set`(
        @org.junit.jupiter.api.io.TempDir dir: java.nio.file.Path,
    ) {
        val file = dir.resolve("jwks.json")
        java.nio.file.Files.writeString(file, TestTokens.jwks(pair))
        val keys = JwksSigningKeys.fromFile(file)
        // Half-written ConfigMap: the file is now garbage.
        java.nio.file.Files.writeString(file, "{")
        keys.refresh()
        // Keeping a retired key one extra window beats 401-ing the fleet.
        assertThat(keys.byKeyId(TestTokens.KID)).isNotNull()
    }
}
