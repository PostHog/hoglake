package com.posthog.hoglake.auth

import io.ktor.http.HttpMethod
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.security.KeyPair
import java.security.interfaces.ECPublicKey

/**
 * Letter case in what auth compares: `cat` keys, scope words, the OAuth
 * `scope` claim, `prd` field names and the request path. Nothing is folded:
 * a catalog grant names the catalog byte for byte, and catalog names are
 * lowercase only (Identifiers.CATALOG_NAME_PATTERN), so an uppercase grant
 * can never match a catalog. Scope words are lowercase wire names.
 */
class IdentifierCaseAuthTest {
    private val pair: KeyPair = TestTokens.keyPair()

    private fun keys() = StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey)

    private fun tenantVerifier() = JwtVerifier(TestTokens.ISSUER, TestTokens.AUDIENCE, keys(), 60) { 2_000_000_000L }

    private fun scopedVerifier() =
        JwtVerifier(TestTokens.ISSUER, TestTokens.AUDIENCE, keys(), predicateFields = setOf("team_id")) {
            1_700_000_100L
        }

    private fun tenant(grants: String): String =
        """{"iss":"${TestTokens.ISSUER}","aud":"${TestTokens.AUDIENCE}","sub":"c","jti":"j1",""" +
            """"ten":"tenant","org":"o1","iat":1700000000,"exp":4000000000,$grants}"""

    private fun verify(
        verifier: JwtVerifier,
        claims: String,
    ): Any =
        try {
            verifier.verify(TestTokens.mint(pair, claims))
        } catch (v: JwtViolation) {
            v.reason
        }

    @Test
    fun `a tenant cat key is compared exactly and is not folded`() {
        val caller = verify(tenantVerifier(), tenant(""""cat":{"Lake":["read"]}""")) as Caller
        assertThat(caller.catalogScopes.keys).containsExactly("Lake")
        assertThat(caller.knowsCatalog("lake")).isFalse()
        assertThat(caller.holds("lake", Scope.READ)).isFalse()
    }

    @Test
    fun `scope words are lowercase only - dropped on a tenant token, refused on a scoped one`() {
        val caller = verify(tenantVerifier(), tenant(""""cat":{"lake":["READ","write"]}""")) as Caller
        assertThat(caller.catalogScopes).isEqualTo(mapOf("lake" to setOf(Scope.WRITE)))
        // The only word unknown: the catalog is dropped, and the token grants nothing.
        assertThat(verify(tenantVerifier(), tenant(""""cat":{"lake":["Read"]}""")))
            .isEqualTo(JwtViolation.MISSING_CLAIM)
        assertThat(verify(scopedVerifier(), TestTokens.scopedClaims(catalogs = """{"lake":["WRITE"]}""")))
            .isEqualTo(JwtViolation.MALFORMED)
        assertThat(Scope.fromWire("READ")).isNull()
    }

    @Test
    fun `the OAuth scope claim requires a lowercase catalog and is matched on a lowercase prefix`() {
        assertThat(verify(tenantVerifier(), tenant(""""scope":"hoglake:Lake:read"""")))
            .isEqualTo(JwtViolation.MALFORMED)
        // `HOGLAKE:` is not our prefix: a foreign value, ignored, so `cat` decides alone.
        val caller =
            verify(tenantVerifier(), tenant(""""cat":{"lake":["read"]},"scope":"HOGLAKE:lake:admin"""")) as Caller
        assertThat(caller.catalogScopes).isEqualTo(mapOf("lake" to setOf(Scope.READ)))
        // An uppercase scope word on a tenant token is dropped like an unknown one.
        assertThat(verify(tenantVerifier(), tenant(""""scope":"hoglake:lake:READ"""")))
            .isEqualTo(JwtViolation.MISSING_CLAIM)
    }

    @Test
    fun `prd field names are compared exactly`() {
        assertThat(verify(scopedVerifier(), TestTokens.scopedClaims(prd = """{"TEAM_ID":{"42":"read"}}""")))
            .isEqualTo(JwtViolation.INVALID_PREDICATE)
        assertThat(verify(scopedVerifier(), TestTokens.scopedClaims(prd = """{"team_id":{"42":"READ"}}""")))
            .isEqualTo(JwtViolation.INVALID_PREDICATE)
    }

    @Test
    fun `path literals are case-sensitive and path parameters reach the policy unfolded`() {
        assertThat(AuthPolicy.match(HttpMethod.Get, "/V1/catalogs/lake")).isNull()
        assertThat(AuthPolicy.match(HttpMethod.Get, "/v1/Catalogs/lake")).isNull()
        val match = AuthPolicy.match(HttpMethod.Get, "/v1/catalogs/LAKE/namespaces/Ns/tables/T")!!
        assertThat(listOf(match.catalog, match.namespace, match.table)).containsExactly("LAKE", "Ns", "T")
        val reader =
            Caller(
                subject = "client:test",
                orgId = "org-1",
                kind = Caller.Kind.TENANT,
                catalogScopes = mapOf("lake" to setOf(Scope.READ)),
                tokenId = "jti",
                clientName = "test",
            )
        assertThat(AuthPolicy.authorize(reader, match)).isEqualTo(AuthDecision.UnknownCatalog("LAKE"))
    }
}
