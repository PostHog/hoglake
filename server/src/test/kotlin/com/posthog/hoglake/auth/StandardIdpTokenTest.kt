package com.posthog.hoglake.auth

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.security.KeyPair

/**
 * Tokens shaped the way off-the-shelf identity providers issue them: signed
 * RS256 against a JWKS that mixes key types and purposes, with grants in the
 * standard OAuth `scope` claim instead of (or alongside) `cat`.
 */
class StandardIdpTokenTest {
    private val ec: KeyPair = TestTokens.keyPair()
    private val rsa: KeyPair = TestTokens.rsaKeyPair()
    private val otherRsa: KeyPair = TestTokens.rsaKeyPair()
    private val now = 2_000_000_000L

    private val mixedJwks =
        """{"keys":[${TestTokens.ecJwk(ec, "ec-1")},${TestTokens.rsaJwk(rsa, "rsa-1")}]}"""

    private fun verifier(
        jwks: String = mixedJwks,
        predicateFields: Set<String> = setOf("team_id"),
    ): JwtVerifier {
        val keys = JwksSigningKeys(fetch = { jwks.toByteArray() }, describe = "test").also { it.load() }
        return JwtVerifier(TestTokens.ISSUER, TestTokens.AUDIENCE, keys, 60, predicateFields) { now }
    }

    private fun rs256(
        claims: String,
        kid: String = "rsa-1",
        signWith: KeyPair = rsa,
    ) = TestTokens.mintRs256(signWith, claims, """{"alg":"RS256","kid":"$kid","typ":"JWT"}""")

    private fun es256(
        claims: String,
        kid: String = "ec-1",
    ) = TestTokens.mint(ec, claims, """{"alg":"ES256","kid":"$kid","typ":"JWT"}""")

    /** A tenant claim set with [grants] spliced in verbatim (`"cat":…`, `"scope":…`, both, or neither). */
    private fun tenant(grants: String): String {
        val tail = if (grants.isEmpty()) "" else ",$grants"
        return """{"iss":"${TestTokens.ISSUER}","aud":"${TestTokens.AUDIENCE}","sub":"idp-client","jti":"j1",""" +
            """"ten":"tenant","org":"o1","iat":1700000000,"exp":4000000000$tail}"""
    }

    private fun refusal(
        reason: String,
        call: () -> Unit,
    ) {
        assertThatThrownBy { call() }
            .isInstanceOf(JwtViolation::class.java)
            .extracting("reason").isEqualTo(reason)
    }

    // ---- RS256 -----------------------------------------------------------

    @Test
    fun `an RS256 token verifies against an RSA key in the JWKS`() {
        val caller = verifier().verify(rs256(TestTokens.tenantClaims()))
        assertThat(caller.kind).isEqualTo(Caller.Kind.TENANT)
        assertThat(caller.catalogScopes["lake"]).containsExactlyInAnyOrder(Scope.READ, Scope.WRITE)
    }

    @Test
    fun `one JWKS holding an EC and an RSA key verifies both algorithms by kid`() {
        val v = verifier()
        assertThat(JwksSigningKeys.parseJwks(mixedJwks.toByteArray()).keys).containsExactly("ec-1", "rsa-1")
        assertThat(v.verify(es256(TestTokens.tenantClaims())).subject).isEqualTo("client:hedgerow-prod")
        assertThat(v.verify(rs256(TestTokens.tenantClaims())).subject).isEqualTo("client:hedgerow-prod")
    }

    @Test
    fun `an RS256 header naming an EC key is refused, and the reverse`() {
        refusal(JwtViolation.BAD_SIGNATURE) { verifier().verify(rs256(TestTokens.tenantClaims(), kid = "ec-1")) }
        refusal(JwtViolation.BAD_SIGNATURE) { verifier().verify(es256(TestTokens.tenantClaims(), kid = "rsa-1")) }
    }

    @Test
    fun `an RS256 token signed by a different RSA key is refused`() {
        refusal(JwtViolation.BAD_SIGNATURE) {
            verifier().verify(rs256(TestTokens.tenantClaims(), signWith = otherRsa))
        }
    }

    @Test
    fun `a tampered RS256 payload is refused`() {
        val token = rs256(TestTokens.tenantClaims())
        val forged = TestTokens.mintRs256(rsa, TestTokens.tenantClaims(catalogs = mapOf("lake" to listOf("admin"))))
        val spliced = token.substringBefore('.') + "." + forged.split('.')[1] + "." + token.substringAfterLast('.')
        refusal(JwtViolation.BAD_SIGNATURE) { verifier().verify(spliced) }
    }

    @Test
    fun `algorithms outside ES256 and RS256 are still refused before any key is used`() {
        for (alg in listOf("none", "HS256", "RS384", "PS256", "ES384")) {
            val token =
                TestTokens.mintRs256(rsa, TestTokens.tenantClaims(), """{"alg":"$alg","kid":"rsa-1"}""")
            refusal(JwtViolation.UNSUPPORTED_ALG) { verifier().verify(token) }
        }
    }

    @Test
    fun `a JWK whose alg does not match its key type is refused at load`() {
        val rsaMarkedEs = """{"keys":[${TestTokens.rsaJwk(rsa, "k", ""","alg":"ES256"""")}]}"""
        assertThatThrownBy { JwksSigningKeys.parseJwks(rsaMarkedEs.toByteArray()) }
            .isInstanceOf(JwtViolation::class.java)
        val ecMarkedRs = """{"keys":[${TestTokens.ecJwk(ec, "k").replace("\"ES256\"", "\"RS256\"")}]}"""
        assertThatThrownBy { JwksSigningKeys.parseJwks(ecMarkedRs.toByteArray()) }
            .isInstanceOf(JwtViolation::class.java)
    }

    @Test
    fun `an RSA key without alg or use is accepted as an RS256 signing key`() {
        val bare = """{"keys":[${TestTokens.rsaJwk(rsa, "rsa-1", extra = "")}]}"""
        assertThat(verifier(jwks = bare).verify(rs256(TestTokens.tenantClaims())).subject)
            .isEqualTo("client:hedgerow-prod")
    }

    @Test
    fun `an RSA modulus under 2048 bits is refused at load`() {
        val weak = """{"keys":[${TestTokens.rsaJwk(TestTokens.rsaKeyPair(1024), "weak")}]}"""
        assertThatThrownBy { JwksSigningKeys.parseJwks(weak.toByteArray()) }
            .isInstanceOf(JwtViolation::class.java)
            .hasMessageContaining("2048")
    }

    @Test
    fun `encryption keys and keys for other algorithms are skipped, as identity providers publish them`() {
        val idp =
            """{"keys":[""" +
                TestTokens.rsaJwk(otherRsa, "enc-1", ""","use":"enc","alg":"RSA-OAEP"""") + "," +
                TestTokens.rsaJwk(otherRsa, "rs384", ""","use":"sig","alg":"RS384"""") + "," +
                TestTokens.rsaJwk(rsa, "rsa-1") +
                "]}"
        assertThat(JwksSigningKeys.parseJwks(idp.toByteArray()).keys).containsExactly("rsa-1")
        val v = verifier(jwks = idp)
        assertThat(v.verify(rs256(TestTokens.tenantClaims())).kind).isEqualTo(Caller.Kind.TENANT)
        refusal(JwtViolation.UNKNOWN_KEY) {
            v.verify(rs256(TestTokens.tenantClaims(), kid = "enc-1", signWith = otherRsa))
        }
    }

    @Test
    fun `a skipped entry need not carry a kid, but a usable signing key must`() {
        // RFC 7517 §4.5 makes kid optional, and identity providers publish
        // encryption keys without one next to their signing keys. The purpose
        // filter runs before the kid requirement, so such a file still loads.
        val encNoKid =
            TestTokens.rsaJwk(
                otherRsa,
                "drop",
                ""","use":"enc","alg":"RSA-OAEP"""",
            ).replace(""""kid":"drop",""", "")
        assertThat(encNoKid).doesNotContain("kid")
        val idp = """{"keys":[""" + encNoKid + "," + TestTokens.rsaJwk(rsa, "rsa-1") + "]}"
        assertThat(JwksSigningKeys.parseJwks(idp.toByteArray()).keys).containsExactly("rsa-1")
        assertThat(verifier(jwks = idp).verify(rs256(TestTokens.tenantClaims())).kind).isEqualTo(Caller.Kind.TENANT)

        // A signing key this verifier would use but that no token can name is a
        // misconfiguration, still refused at load.
        val sigNoKid = TestTokens.rsaJwk(rsa, "drop").replace(""""kid":"drop",""", "")
        assertThatThrownBy { JwksSigningKeys.parseJwks("""{"keys":[$sigNoKid]}""".toByteArray()) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("kid")
    }

    @Test
    fun `a signing key of a type this verifier cannot use is refused at load, not skipped`() {
        val oct = """{"keys":[{"kty":"oct","kid":"h","k":"c2VjcmV0"}]}"""
        assertThatThrownBy { JwksSigningKeys.parseJwks(oct.toByteArray()) }
            .isInstanceOf(JwtViolation::class.java)
    }

    // ---- the scope claim ---------------------------------------------------

    @Test
    fun `hoglake values in scope grant catalogs, and other audiences' values are ignored`() {
        val caller =
            verifier().verify(
                rs256(
                    tenant(
                        """"scope":"openid hoglake:lake:read hoglake:lake:write profile hoglake:pond:admin"""",
                    ),
                ),
            )
        assertThat(caller.catalogScopes).isEqualTo(
            mapOf("lake" to setOf(Scope.READ, Scope.WRITE), "pond" to setOf(Scope.ADMIN)),
        )
        assertThat(caller.holds("pond", Scope.MAINTENANCE)).isTrue()
        assertThat(caller.knowsCatalog("other")).isFalse()
    }

    @Test
    fun `an unknown scope word in scope is dropped like it is in cat`() {
        val caller =
            verifier().verify(
                es256(tenant(""""scope":"hoglake:lake:read hoglake:lake:quantum hoglake:pond:quantum"""")),
            )
        assertThat(caller.catalogScopes).isEqualTo(mapOf("lake" to setOf(Scope.READ)))
    }

    @Test
    fun `cat and scope may both be present when they agree`() {
        val claims = tenant(""""cat":{"lake":["write","read"]},"scope":"hoglake:lake:read hoglake:lake:write"""")
        assertThat(verifier().verify(es256(claims)).catalogScopes)
            .isEqualTo(mapOf("lake" to setOf(Scope.READ, Scope.WRITE)))
    }

    @Test
    fun `cat and scope that disagree refuse the token`() {
        val wider = tenant(""""cat":{"lake":["read"]},"scope":"hoglake:lake:read hoglake:lake:admin"""")
        refusal(JwtViolation.MALFORMED) { verifier().verify(es256(wider)) }
        val otherCatalog = tenant(""""cat":{"lake":["read"]},"scope":"hoglake:pond:read"""")
        refusal(JwtViolation.MALFORMED) { verifier().verify(es256(otherCatalog)) }
    }

    @Test
    fun `a scope claim with no hoglake values leaves cat as the only grant`() {
        val claims = tenant(""""cat":{"lake":["read"]},"scope":"openid email"""")
        assertThat(verifier().verify(es256(claims)).catalogScopes).isEqualTo(mapOf("lake" to setOf(Scope.READ)))
    }

    @Test
    fun `neither cat nor a hoglake scope is the missing-cat refusal`() {
        refusal(JwtViolation.MISSING_CLAIM) { verifier().verify(es256(tenant(""))) }
        refusal(JwtViolation.MISSING_CLAIM) { verifier().verify(es256(tenant(""""scope":"openid profile""""))) }
        refusal(JwtViolation.MISSING_CLAIM) { verifier().verify(es256(tenant(""""scope":"hoglake:lake:quantum""""))) }
    }

    @Test
    fun `a malformed hoglake scope value or a bad catalog name refuses the token`() {
        for (value in listOf(
            "hoglake:lake",
            "hoglake:lake:read:x",
            "hoglake:Lake:read",
            "hoglake::read",
            "hoglake:-x:read",
        )) {
            refusal(JwtViolation.MALFORMED) { verifier().verify(es256(tenant(""""scope":"$value""""))) }
        }
    }

    @Test
    fun `a scope claim that is not a string is refused`() {
        refusal(JwtViolation.MALFORMED) {
            verifier().verify(es256(tenant(""""scope":["hoglake:lake:read"]""")))
        }
    }

    @Test
    fun `an operator token's scope claim is ignored`() {
        val claims =
            TestTokens.operatorClaims().replace(
                "\"ten\":\"operator\"",
                "\"ten\":\"operator\",\"scope\":\"x y\"",
            )
        assertThat(verifier().verify(es256(claims)).kind).isEqualTo(Caller.Kind.OPERATOR)
    }

    // ---- scoped tokens keep their strictness through scope ------------------

    private fun scoped(
        catalogs: String,
        scope: String?,
    ): String {
        val extra = scope?.let { ""","scope":"$it"""" } ?: ""
        val base = TestTokens.scopedClaims(catalogs = catalogs, iat = now - 60, extra = extra)
        return if (catalogs.isEmpty()) base.replace(",\"cat\":", "") else base
    }

    @Test
    fun `a scoped token may carry its grant in scope instead of cat`() {
        val claims = scoped(catalogs = "", scope = "hoglake:lake:write hoglake:lake:read")
        val caller = verifier().verify(rs256(claims))
        assertThat(caller.kind).isEqualTo(Caller.Kind.SCOPED)
        assertThat(caller.catalogScopes).isEqualTo(mapOf("lake" to setOf(Scope.READ, Scope.WRITE)))
    }

    @Test
    fun `a scoped token's scope may hold only read and write`() {
        for (word in listOf("maintenance", "admin", "quantum")) {
            refusal(JwtViolation.MALFORMED) {
                verifier().verify(rs256(scoped(catalogs = "", scope = "hoglake:lake:$word")))
            }
        }
    }

    @Test
    fun `a scoped token whose cat and scope disagree is refused`() {
        refusal(JwtViolation.MALFORMED) {
            verifier().verify(rs256(scoped(catalogs = """{"lake":["write"]}""", scope = "hoglake:lake:read")))
        }
        val agreeing = scoped(catalogs = """{"lake":["write"]}""", scope = "hoglake:lake:write")
        assertThat(verifier().verify(rs256(agreeing)).catalogScopes).isEqualTo(mapOf("lake" to setOf(Scope.WRITE)))
    }

    @Test
    fun `a scoped token with neither cat nor scope is refused as before`() {
        refusal(JwtViolation.MISSING_CLAIM) { verifier().verify(rs256(scoped(catalogs = "", scope = null))) }
    }
}
