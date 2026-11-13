package com.posthog.hoglake.auth

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.store.BackendKind
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import java.util.Base64
import java.util.UUID

/**
 * The local issuer is an issuer like any other: what it mints verifies here,
 * and it conforms to hoglake's own contract vectors — every issuer-input
 * vector, minted from its input, is the vector's claims.
 */
class LocalIssuerTest {
    private val mapper = ObjectMapper()
    private val b64 = Base64.getUrlEncoder().withoutPadding()
    private val root =
        mapper.readTree(javaClass.getResourceAsStream("/oauth/contract-vectors.json") ?: error("vectors missing"))
    private val issuedAt = root["issued_at"].asLong()
    private val verifyAt = root["verify_at"].asLong()
    private val vectorKey =
        LocalSigningKey.decode(
            b64.encodeToString(
                mapper.writeValueAsBytes(
                    mapper.createObjectNode().apply {
                        put("kty", "EC")
                        put("crv", "P-256")
                        put("d", root["test_private_key_d"].asText())
                        put("x", root["jwks"]["keys"][0]["x"].asText())
                        put("y", root["jwks"]["keys"][0]["y"].asText())
                    },
                ),
            ),
        )
    private val backends =
        root["vectors"].mapNotNull { it["claims"]["rtg"]?.get("be")?.asText() }
            .associate { UUID.fromString(it) to BackendKind.HOGLITE }

    private fun verifier(
        key: LocalSigningKey,
        issuer: String,
        now: Long,
        predicateFields: Set<String> = setOf("team_id"),
    ) = JwtVerifier(issuer, "hoglake", key.signingKeys(), 60, predicateFields, backends = backends) { now }

    private val now = 2_000_000_000L
    private val key = LocalSigningKey.generate()
    private val local = LocalIssuer(key, "hoglake", clock = { now })
    private val verify = verifier(key, "hoglake", now)

    @Test
    fun `a tenant token verifies at every scope`() {
        for (scope in Scope.entries) {
            val token =
                local.tenant(
                    "alice",
                    "local",
                    mapOf("demo" to scope, "other" to Scope.READ),
                    "pyhoglake",
                    3_600,
                )
            val caller = verify.verify(token)
            assertThat(caller.kind).isEqualTo(Caller.Kind.TENANT)
            assertThat(caller.subject).isEqualTo("alice")
            assertThat(caller.orgId).isEqualTo("local")
            assertThat(caller.clientName).isEqualTo("pyhoglake")
            assertThat(caller.catalogScopes["demo"]).contains(scope)
            assertThat(caller.catalogScopes["other"]).isEqualTo(setOf(Scope.READ))
            assertThat(caller.delegates).isFalse()
        }
        val delegating = local.tenant("engine", "local", mapOf("demo" to Scope.WRITE), "trino", 60, delegates = true)
        assertThat(verify.verify(delegating).delegates).isTrue()
    }

    @Test
    fun `an operator token verifies fleet-wide, and bound to one catalog`() {
        val fleet = verify.verify(local.operator("ops", "cli", 300))
        assertThat(fleet.kind).isEqualTo(Caller.Kind.OPERATOR)
        assertThat(fleet.catalogScopes).isEmpty()
        val boundToken = local.operator("ops", "cli", 300, org = "local", catalog = "demo")
        val bound = verify.verify(boundToken)
        assertThat(bound.kind).isEqualTo(Caller.Kind.OPERATOR)
        // An operator holds no tenant scope (Caller.catalogScopes is empty for
        // the kind); the binding is the claim itself, which the vectors pin.
        val boundClaims = mapper.readTree(Base64.getUrlDecoder().decode(boundToken.split('.')[1]))
        assertThat(boundClaims["cat"]["demo"][0].asText()).isEqualTo("admin")
        assertThatThrownBy { local.operator("ops", "cli", 300, catalog = "demo") }.hasMessageContaining("org")
    }

    @Test
    fun `a scoped token verifies to a scoped caller, and only with read or write scopes`() {
        val predicate = LocalIssuer.Predicate("team_id", mapOf("42" to "write", "7" to "read"))
        val token =
            local.tenant(
                "laptop",
                "local",
                mapOf("demo" to Scope.WRITE),
                "cli",
                900,
                predicate = predicate,
                user = "u1",
            )
        val caller = verify.verify(token)
        assertThat(caller.kind).isEqualTo(Caller.Kind.SCOPED)
        assertThat(caller.user).isEqualTo("u1")
        assertThat(caller.predicates.keys).containsExactly("team_id")
        assertThatThrownBy {
            local.tenant("laptop", "local", mapOf("demo" to Scope.ADMIN), "cli", 900, predicate = predicate)
        }.hasMessageContaining("read or write")
    }

    @Test
    fun `the token is what the verifier expects on the wire`() {
        val token = local.tenant("alice", "local", mapOf("demo" to Scope.READ), "cli", 60)
        val header = mapper.readTree(Base64.getUrlDecoder().decode(token.substringBefore('.')))
        assertThat(header["alg"].asText()).isEqualTo("ES256")
        assertThat(header["typ"].asText()).isEqualTo("JWT")
        assertThat(header["kid"].asText()).isEqualTo(key.keyId)
        val claims = mapper.readTree(Base64.getUrlDecoder().decode(token.split('.')[1]))
        assertThat(claims.fieldNames().asSequence().toList())
            .containsExactly("iss", "aud", "sub", "jti", "ten", "org", "cat", "cli", "exp", "iat", "nbf")
        assertThat(claims["exp"].asLong() - claims["iat"].asLong()).isEqualTo(60)
        assertThat(UUID.fromString(claims["jti"].asText())).isNotNull()
    }

    /** Mint a vector's `client` or `operator` input the way its issuer would. */
    private fun mintInput(
        vector: JsonNode,
        issuer: LocalIssuer,
        jti: String,
    ): com.fasterxml.jackson.databind.node.ObjectNode {
        val audience = vector["audience"].asText()
        val placement =
            (vector["client"] ?: vector["operator"])["placement"]?.let {
                LocalIssuer.Placement(
                    it["backend_type"].asText(),
                    it["backend_id"].asText(),
                    it["tenant_id"].asText(),
                    it["incarnation"].asLong(),
                )
            }
        vector["operator"]?.let { op ->
            return issuer.operatorClaims(
                subject = op["subject"].asText(),
                clientName = op["client"].asText(),
                ttlSeconds = 5 * 60,
                org = op["org"]?.asText(),
                catalog = op["catalog"]?.asText(),
                placement = placement,
                audience = audience,
                issuedAt = issuedAt,
                jti = jti,
            )
        }
        val c = vector["client"]
        val teams = c["teams"]
        val predicate =
            teams?.let {
                LocalIssuer.Predicate(
                    "team_id",
                    it.properties().associate {
                            f ->
                        f.key to f.value.asText()
                    },
                )
            }
        return issuer.tenantClaims(
            subject = c["credential_id"].asText(),
            org = c["org"].asText(),
            catalogs = mapOf(c["org"].asText() to Scope.fromWire(c["scope"].asText())!!),
            clientName = c["name"].asText(),
            ttlSeconds = 15 * 60,
            delegates = c["delegates"].asBoolean(),
            predicate = predicate,
            user = c["owner_user_uuid"]?.asText(),
            placement = placement,
            audience = audience,
            issuedAt = issuedAt,
            jti = jti,
        )
    }

    @TestFactory
    fun `every issuer-input vector minted here is the vector's claims, and gets its verdict`(): List<DynamicTest> {
        val issuer = LocalIssuer(vectorKey, root["issuer"].asText())
        val vectors = root["vectors"].toList()
        val mintable = vectors.filter { it["mutation"] == null }
        // A vector is one or the other; the file growing a third shape must fail here, not be skipped.
        assertThat(vectors).allSatisfy { assertThat(it.has("client") || it.has("operator")).isTrue() }
        assertThat(mintable).isNotEmpty()
        return mintable.map { vector ->
            DynamicTest.dynamicTest(vector["name"].asText()) {
                val claims = mintInput(vector, issuer, vector["jti"].asText())
                // Re-read through the parser so a long and an int that print the
                // same compare equal: the wire is bytes, not node classes.
                assertThat(mapper.readTree(mapper.writeValueAsBytes(claims))).isEqualTo(vector["claims"])
                val token = issuer.sign(claims)
                val expect = vector["expect"]
                val v = verifier(vectorKey, root["issuer"].asText(), verifyAt)
                when (expect["verdict"].asText()) {
                    "accept" -> assertThat(v.verify(token).kind.name.lowercase()).isEqualTo(expect["kind"].asText())
                    "refuse" ->
                        assertThatThrownBy { v.verify(token) }
                            .isInstanceOfSatisfying(JwtViolation::class.java) {
                                assertThat(it.reason).isEqualTo(expect["reason"].asText())
                            }
                    else -> error("unknown verdict ${expect["verdict"]}")
                }
            }
        }
    }

    @Test
    fun `the verifier-only vectors are the ones this issuer never mints`() {
        val mutated = root["vectors"].filter { it["mutation"] != null }.map { it["name"].asText() }
        assertThat(mutated).containsExactlyInAnyOrder(
            "tenant_without_ten",
            "scoped_without_ten",
            "operator_without_ten",
            "scoped_over_lifetime_cap",
        )
    }
}
