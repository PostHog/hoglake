package com.posthog.hoglake.auth

import com.posthog.hoglake.store.BackendKind
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.security.interfaces.ECPublicKey
import java.util.UUID

class RoutingClaimTest {
    private val pair = TestTokens.keyPair()
    private val id = UUID.fromString("5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")
    private val routing = """{"b":"hoglite","be":"$id","t":"t_3fa8c1d9e2_1"}"""

    private fun verify(
        rtg: String? = routing,
        claims: String = TestTokens.tenantClaims(),
        backends: Map<UUID, BackendKind> = mapOf(id to BackendKind.HOGLITE),
    ): Caller {
        val body = if (rtg == null) claims else claims.dropLast(1) + ",\"rtg\":$rtg}"
        return JwtVerifier(
            TestTokens.ISSUER,
            TestTokens.AUDIENCE,
            StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
            predicateFields = setOf("team_id"),
            backends = backends,
            clock = { 1_700_000_001 },
        ).verify(TestTokens.mint(pair, body))
    }

    @Test
    fun `tenant and scoped placements retain the opaque tenant id`() {
        for (claims in listOf(TestTokens.tenantClaims(), TestTokens.scopedClaims())) {
            val caller = verify(claims = claims)
            assertThat(caller.routing).isEqualTo(Routing("lake", BackendKind.HOGLITE, id, "t_3fa8c1d9e2_1"))
        }
    }

    @Test
    fun `absent placement keeps legacy callers unchanged`() {
        assertThat(verify(rtg = null).routing).isNull()
        val caller = verify(rtg = null, claims = TestTokens.operatorClaims())
        assertThat(caller.routing).isNull()
        assertThat(caller.orgId).isNull()
        assertThat(caller.catalogScopes).isEmpty()
    }

    @Test
    fun `catalog operators retain their operator kind and empty scope map`() {
        val claims = TestTokens.operatorClaims().dropLast(1) + """, "org":"org-1", "cat":{"lake":["admin"]}}"""
        val caller = verify(claims = claims)
        assertThat(caller.kind).isEqualTo(Caller.Kind.OPERATOR)
        assertThat(caller.orgId).isEqualTo("org-1")
        assertThat(caller.catalogScopes).isEmpty()
        assertThat(caller.routing?.catalog).isEqualTo("lake")
    }

    @ParameterizedTest
    @ValueSource(strings = ["null", "[]", "1", "\"hoglite\"", "{}"])
    fun `bad object shapes fail closed`(rtg: String) {
        refused(rtg, JwtViolation.MALFORMED)
    }

    @Test
    fun `every missing field and unknown routing field fails closed`() {
        for (field in listOf("b", "be", "t")) {
            val obj =
                com.fasterxml.jackson.module.kotlin.jacksonObjectMapper().readTree(
                    routing,
                ) as com.fasterxml.jackson.databind.node.ObjectNode
            obj.remove(field)
            refused(obj.toString(), JwtViolation.MALFORMED)
        }
        refused(routing.dropLast(1) + ",\"future\":true}", JwtViolation.MALFORMED)
    }

    @Test
    fun `routing fields require their exact string grammar`() {
        for (value in listOf("1", "null", "[]", "{}", "\"unknown\"")) {
            refused(routing.replace("\"hoglite\"", value), JwtViolation.MALFORMED)
        }
        for (value in listOf("1", "null", "\"1-1-1-1-1\"", "\"${id.toString().uppercase()}\"")) {
            refused(routing.replace("\"$id\"", value), JwtViolation.MALFORMED)
        }
        for (tenant in listOf("", ".start", "two..dots", "slash/id", "space id", "a".repeat(129), "é")) {
            refused(routing.replace("t_3fa8c1d9e2_1", tenant), JwtViolation.MALFORMED)
        }
        for (value in listOf("1", "null", "[]", "{}")) {
            refused(routing.replace("\"t_3fa8c1d9e2_1\"", value), JwtViolation.MALFORMED)
        }
        assertThat(verify(routing.replace("t_3fa8c1d9e2_1", "a".repeat(128))).routing?.tenant).hasSize(128)
        assertThat(verify(routing.replace("t_3fa8c1d9e2_1", "A_1.b-2")).routing?.tenant).isEqualTo("A_1.b-2")
    }

    @Test
    fun `unknown ids and kind mismatches have their own refusal`() {
        refused(routing, JwtViolation.UNKNOWN_BACKEND, backends = emptyMap())
        refused(routing, JwtViolation.UNKNOWN_BACKEND, backends = mapOf(id to BackendKind.POSTGRES))
    }

    @Test
    fun `placement requires exactly one catalog for every caller kind`() {
        for (claims in listOf(
            TestTokens.tenantClaims(catalogs = mapOf("lake" to listOf("read"), "other" to listOf("read"))),
            TestTokens.scopedClaims(catalogs = """{"lake":["read"],"other":["read"]}"""),
            TestTokens.operatorClaims(),
            TestTokens.operatorClaims().dropLast(1) + """, "cat":{"lake":[],"other":[]}}""",
        )) {
            refused(routing, JwtViolation.MALFORMED, claims)
        }
    }

    @Test
    fun `postgres placement has no tenant and needs an explicit allowlist entry`() {
        val postgres = """{"b":"postgres","be":"$id"}"""
        assertThat(verify(postgres, backends = mapOf(id to BackendKind.POSTGRES)).routing?.tenant).isNull()
        refused(postgres, JwtViolation.UNKNOWN_BACKEND)
        refused(
            postgres.dropLast(1) + ",\"t\":\"tenant\"}",
            JwtViolation.MALFORMED,
            backends = mapOf(id to BackendKind.POSTGRES),
        )
    }

    private fun refused(
        rtg: String,
        reason: String,
        claims: String = TestTokens.tenantClaims(),
        backends: Map<UUID, BackendKind> = mapOf(id to BackendKind.HOGLITE),
    ) {
        assertThatThrownBy { verify(rtg, claims, backends) }
            .isInstanceOfSatisfying(JwtViolation::class.java) { assertThat(it.reason).isEqualTo(reason) }
    }
}
