package com.posthog.hoglake.auth

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.posthog.hoglake.store.BackendKind
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID

class ReadSessionProfileTest {
    @TempDir
    lateinit var directory: Path

    private val mapper = ObjectMapper()
    private val pair = TestTokens.keyPair()
    private val backend = UUID.fromString("11111111-1111-4111-8111-111111111111")
    private val profile = ReadSessionProfile("example.aws-session", setOf("remote:events", "remote:imports"))

    private fun verifier(
        now: Long = 2_000_000_001L,
        experimental: Boolean = false,
        backends: Map<UUID, BackendKind> = mapOf(backend to BackendKind.HOGLITE),
        ignoreUnknownRouting: Boolean = false,
    ): JwtVerifier {
        val jwks = directory.resolve("jwks.json")
        Files.writeString(jwks, """{"keys":[${TestTokens.ecJwk(pair, TestTokens.KID)}]}""")
        return JwtVerifier(
            TestTokens.ISSUER,
            TestTokens.AUDIENCE,
            JwksSigningKeys.fromFile(jwks),
            predicateFields = setOf("team_id"),
            backends = backends,
            sessionProfile = experimental,
            ignoreUnknownScopedRouting = ignoreUnknownRouting,
        ) { now }
    }

    private fun token(mutation: (ObjectNode) -> Unit = {}): String {
        val claims =
            mapper.readTree(
                """
                {
                  "iss": "${TestTokens.ISSUER}",
                  "aud": ["trino-session", "hoglake-session", "hoglite-session"],
                  "sub": "example.alice", "usr": "example.alice", "jti": "token-a",
                  "profile": "example.aws-session", "version": 1, "purpose": "user-session",
                  "ten": "scoped", "org": "example-org", "primary_catalog": "example-catalog",
                  "sid": "session-a", "generation": 1, "session_deadline": 2000000120,
                  "iat": 2000000000, "nbf": 2000000000, "exp": 2000000060,
                  "cat": {"example-catalog": ["read"]}, "prd": {"team_id": {"42": "read"}},
                  "rtg": {"b": "hoglite", "be": "$backend", "t": "fixture_3"},
                  "remote_schemas": {"team": ["read"]},
                  "aws": {"access_key_id":"fictional-key", "secret_access_key":"fictional-secret",
                          "session_token":"fictional-session", "region":"us-east-1", "expires_at":2000000060},
                  "grants": [
                    {"catalog":"example-catalog","target":"example-catalog","bucket":"example-customer",
                     "object_key":"tables/data.parquet","region":"us-east-1","actions":["s3:GetObject"]},
                    {"catalog":"example-catalog","target":"$backend","bucket":"example-owner",
                     "object_key":"root/2c/t/fixture_3/manifest","region":"us-east-1","actions":["s3:GetObject"]},
                    {"catalog":"example-catalog","target":"remote:events","bucket":"example-remote",
                     "prefix":"events/team_id=42/","region":"us-east-1","actions":["s3:GetObject"]},
                    {"catalog":"example-catalog","target":"remote:imports","bucket":"example-remote",
                     "prefix":"imports/team_id=42/","region":"us-east-1","actions":["s3:GetObject"]}
                  ]
                }
                """.trimIndent(),
            ) as ObjectNode
        mutation(claims)
        return TestTokens.mint(pair, claims.toString())
    }

    @Test
    fun `the dedicated verifier accepts literal and prefix grants without exporting credentials`() {
        val caller = verifier().verifyReadSession(token(), profile)
        assertThat(caller.subject).isEqualTo("example.alice")
        assertThat(caller.routing?.backendId).isEqualTo(backend)
        assertThat(caller.remoteSchemaFamilies).containsExactly("team")
        assertThat(caller.toString()).doesNotContain("fictional-key", "fictional-secret", "fictional-session")
        val remote = verifier(backends = emptyMap(), ignoreUnknownRouting = true)
        assertThat(remote.verifyReadSession(token(), profile).routingIgnored).isTrue()
        assertThatThrownBy { verifier(backends = emptyMap()).verifyReadSession(token(), profile) }
            .isInstanceOfSatisfying(JwtViolation::class.java) {
                assertThat(it.reason).isEqualTo(JwtViolation.UNKNOWN_BACKEND)
            }
    }

    @Test
    fun `a rotated key is read from the mounted file after the miss, not on the miss`() {
        val jwks = directory.resolve("rotating-jwks.json")
        Files.writeString(jwks, """{"keys":[${TestTokens.ecJwk(pair, TestTokens.KID)}]}""")
        val queued = mutableListOf<() -> Unit>()
        val keys =
            JwksSigningKeys(
                fetch = { Files.readAllBytes(jwks) },
                describe = "rotation-test",
                mountedFile = true,
                runAsync = { queued += it },
            )
        keys.refresh()
        val v =
            JwtVerifier(
                TestTokens.ISSUER,
                TestTokens.AUDIENCE,
                keys,
                predicateFields = setOf("team_id"),
                backends = mapOf(backend to BackendKind.HOGLITE),
            ) { 2_000_000_001L }
        val rotated = TestTokens.keyPair()
        val header = """{"alg":"ES256","kid":"test-key-2","typ":"JWT"}"""
        val claims = String(java.util.Base64.getUrlDecoder().decode(token().split('.')[1]))
        val next = TestTokens.mint(rotated, claims, header)

        Files.writeString(jwks, """{"keys":[${TestTokens.ecJwk(rotated, "test-key-2")}]}""")
        assertThatThrownBy { v.verifyReadSession(next, profile) }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(JwtViolation.UNKNOWN_KEY)
        assertThat(queued).hasSize(1)
        queued.single().invoke()

        assertThat(v.verifyReadSession(next, profile).subject).isEqualTo("example.alice")
        // The removed key is gone with the same re-read.
        assertThatThrownBy { v.verifyReadSession(token(), profile) }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(JwtViolation.UNKNOWN_KEY)
    }

    @Test
    fun `the request that misses is refused even when the re-read finishes first`() {
        val jwks = directory.resolve("inline-jwks.json")
        Files.writeString(jwks, """{"keys":[${TestTokens.ecJwk(pair, TestTokens.KID)}]}""")
        val keys =
            JwksSigningKeys(
                fetch = { Files.readAllBytes(jwks) },
                describe = "inline-rotation-test",
                mountedFile = true,
                runAsync = { it() },
            )
        keys.refresh()
        val v =
            JwtVerifier(
                TestTokens.ISSUER,
                TestTokens.AUDIENCE,
                keys,
                predicateFields = setOf("team_id"),
                backends = mapOf(backend to BackendKind.HOGLITE),
            ) { 2_000_000_001L }
        val rotated = TestTokens.keyPair()
        val claims = String(java.util.Base64.getUrlDecoder().decode(token().split('.')[1]))
        val next = TestTokens.mint(rotated, claims, """{"alg":"ES256","kid":"test-key-2","typ":"JWT"}""")

        Files.writeString(jwks, """{"keys":[${TestTokens.ecJwk(rotated, "test-key-2")}]}""")
        assertThatThrownBy { v.verifyReadSession(next, profile) }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(JwtViolation.UNKNOWN_KEY)
        // The re-read ran inside that call, so the key was already there.
        assertThat(keys.byKeyId("test-key-2")).isNotNull()
        assertThat(v.verifyReadSession(next, profile).subject).isEqualTo("example.alice")
    }

    @Test
    fun `legacy and dedicated audiences cannot be mixed`() {
        val legacy = verifier(experimental = true)
        val mixed = token { (it["aud"] as ArrayNode).add(TestTokens.AUDIENCE) }
        for (jwt in listOf(token(), mixed)) {
            assertThatThrownBy { legacy.verify(jwt) }.isInstanceOfSatisfying(JwtViolation::class.java) {
                assertThat(it.reason).isEqualTo(JwtViolation.WRONG_AUDIENCE)
            }
        }
        assertThatThrownBy { legacy.verifyReadSession(mixed, profile) }
            .isInstanceOfSatisfying(JwtViolation::class.java) {
                assertThat(it.reason).isEqualTo(JwtViolation.WRONG_AUDIENCE)
            }
    }

    @Test
    fun `credential expiry has no skew allowance`() {
        assertThatThrownBy { verifier(2_000_000_060L).verifyReadSession(token(), profile) }
            .isInstanceOfSatisfying(JwtViolation::class.java) { assertThat(it.reason).isEqualTo(JwtViolation.EXPIRED) }
    }

    @Test
    fun `the dedicated verifier refuses malformed or wider claims`() {
        val mutations: List<(ObjectNode) -> Unit> =
            listOf(
                { it.remove("profile") },
                { it.put("profile", "storage-session") },
                { it.put("nbf", "2000000000") },
                { it.put("generation", 2) },
                { it.put("sub", "example.other") },
                { it.put("session_deadline", 2_000_000_059L) },
                { (it["aws"] as ObjectNode).remove("session_token") },
                { (it["cat"] as ObjectNode).putArray("other").add("read") },
                { (it["rtg"] as ObjectNode).put("t", "fixture_03") },
                { (it["grants"][1] as ObjectNode).put("object_key", "root/2c/t/fixture_2/manifest") },
                { (it["grants"][1] as ObjectNode).put("prefix", "root/2c/t/fixture_3/") },
                { (it["grants"][2] as ObjectNode).put("prefix", "events/*/") },
                { (it["grants"][2] as ObjectNode).put("prefix", "events/part(1)/") },
                { (it["grants"][2] as ObjectNode).putArray("actions").add("s3:PutObject") },
                { (it["grants"][3] as ObjectNode).put("catalog", "other") },
                { (it["grants"] as ArrayNode).remove(3) },
            )
        val verifier = verifier()
        for (mutation in mutations) {
            assertThatThrownBy { verifier.verifyReadSession(token(mutation), profile) }
                .isInstanceOfSatisfying(JwtViolation::class.java) {
                    assertThat(it.reason).isEqualTo(JwtViolation.MALFORMED)
                    assertThat(it.message).isEqualTo("read session refused")
                }
        }
    }

    @Test
    fun `prefix storage grants accept valid S3 object names and reject traversal or control characters`() {
        val jwt =
            token { claims ->
                val grant = claims["grants"][0] as ObjectNode
                grant.remove("object_key")
                grant.put("prefix", "tables/team_id=42/")
            }
        val (_, grants) = verifier().verifyReadSessionWithGrants(jwt, profile)
        assertThat(
            grants.allowsPrimaryCatalogObject(
                "example-catalog",
                "s3://example-customer/tables/team_id=42/part(1).parquet",
            ),
        ).isTrue()
        assertThat(
            grants.allowsPrimaryCatalogObject(
                "example-catalog",
                "s3://example-customer/tables/team_id=42/nested/part(1)+v2.parquet",
            ),
        ).isTrue()
        assertThat(
            grants.allowsPrimaryCatalogObject(
                "example-catalog",
                "s3://example-customer/tables/team_id=42/part\uD83D\uDE00.parquet",
            ),
        ).isTrue()
        assertThat(
            grants.allowsPrimaryCatalogObject(
                "example-catalog",
                "s3://example-customer/tables/team_id=42/" + "a".repeat(1024 - "tables/team_id=42/".length),
            ),
        ).isTrue()
        for (rejected in listOf(
            "s3://example-customer/tables/team_id=42/../team_id=43/part(1).parquet",
            "s3://example-customer/tables/team_id=42/./part(1).parquet",
            "s3://example-customer/tables/team_id=42//part(1).parquet",
            "s3://example-customer/tables/team_id=42/part\u0000.parquet",
            "s3://example-customer/tables/team_id=42/part\n.parquet",
            "s3://example-customer/tables/team_id=42/part\uD800.parquet",
            "s3://example-customer/tables/team_id=42/part\uD800",
            "s3://example-customer/tables/team_id=42/part\uDC00.parquet",
            "s3://example-customer/tables/team_id=42/part\uDC00\uD800.parquet",
            "s3://example-customer/tables/team_id=42/",
            "s3://example-customer/tables/team_id=42/" + "a".repeat(1025 - "tables/team_id=42/".length),
            "s3://example-customer/tables/team_id=42/" + "é".repeat(512),
            "s3://example-customer/tables/team_id=43/part(1).parquet",
            "s3://other-bucket/tables/team_id=42/part(1).parquet",
        )) {
            assertThat(grants.allowsPrimaryCatalogObject("example-catalog", rejected))
                .describedAs(rejected)
                .isFalse()
        }
    }

    @Test
    fun `primary catalog write checks require a writable session and matching prefix`() {
        val verifier = verifier()
        val (_, readOnly) =
            verifier.verifyReadSessionWithGrants(
                token {
                    (it["grants"][0] as ObjectNode).apply {
                        remove("object_key")
                        put("prefix", "data/team_42/")
                    }
                },
                profile,
            )
        assertThat(readOnly.writable).isFalse()
        assertThat(
            readOnly.allowsPrimaryCatalogObject("example-catalog", "s3://example-customer/data/team_42/a.parquet"),
        ).isTrue()
        assertThat(
            readOnly.allowsPrimaryCatalogWrite("example-catalog", "s3://example-customer/data/team_42/a.parquet"),
        ).isFalse()

        val (_, writable) =
            verifier.verifyReadSessionWithGrants(
                token {
                    (it["cat"] as ObjectNode).putArray("example-catalog").add("read").add("write")
                    (it["prd"]["team_id"] as ObjectNode).put("42", "write")
                    (it["grants"][0] as ObjectNode).apply {
                        remove("object_key")
                        put("prefix", "data/team_42/")
                        val actions = putArray("actions")
                        ReadSessionProfile.WRITE_ACTIONS.forEach { action -> actions.add(action) }
                    }
                },
                profile,
            )
        assertThat(writable.writable).isTrue()
        assertThat(
            writable.allowsPrimaryCatalogWrite("example-catalog", "s3://example-customer/data/team_42/a.parquet"),
        ).isTrue()
        assertThat(
            writable.allowsPrimaryCatalogWrite("example-catalog", "s3://example-customer/data/team_43/a.parquet"),
        ).isFalse()
        assertThat(
            writable.allowsPrimaryCatalogWrite("other-catalog", "s3://example-customer/data/team_42/a.parquet"),
        ).isFalse()
    }
}
