package com.posthog.hoglake.auth

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory

/**
 * Tokens hogtower actually mints, checked by the verifier that will receive
 * them.
 *
 * `oauth/contract-vectors.json` is a byte-for-byte copy of hogtower's
 * `tests/oauth/contract-vectors.json` (PostHog/hogtower#116), which hogtower's
 * own suite asserts against its minter. If hogtower renames a claim or changes
 * the scope words, it regenerates that file, the copy here goes stale, and this
 * test is where the disagreement surfaces. Update the copy; do not edit it.
 */
class ContractVectorsTest {
    private val root: JsonNode =
        ObjectMapper().readTree(
            javaClass.getResourceAsStream("/oauth/contract-vectors.json")
                ?: error("oauth/contract-vectors.json is missing from the test resources"),
        )
    private val keys = JwksSigningKeys.parseJwks(ObjectMapper().writeValueAsBytes(root["jwks"]))
    private val verifyAt = root["verify_at"].asLong()

    private fun verifier(
        audience: String = "hoglake",
        now: Long = verifyAt,
    ): JwtVerifier {
        val (kid, key) = keys.entries.single()
        return JwtVerifier(root["issuer"].asText(), audience, StaticSigningKeys(kid, key), 60) { now }
    }

    private fun vectors(): List<JsonNode> = root["vectors"].toList()

    @TestFactory
    fun `every hoglake-audience tenant vector verifies to the caller its claims describe`(): List<DynamicTest> =
        vectors()
            .filter { it["audience"].asText() == "hoglake" && it["claims"]["ten"].asText() == "tenant" }
            .map { vector ->
                DynamicTest.dynamicTest(vector["name"].asText()) {
                    val claims = vector["claims"]
                    val caller = verifier().verify(vector["token"].asText())

                    assertThat(caller.kind).isEqualTo(Caller.Kind.TENANT)
                    assertThat(caller.subject).isEqualTo(claims["sub"].asText())
                    assertThat(caller.subject).startsWith("hlc_")
                    assertThat(caller.orgId).isEqualTo(claims["org"].asText())
                    assertThat(caller.tokenId).isEqualTo(claims["jti"].asText())
                    assertThat(caller.clientName).isEqualTo(claims["cli"].asText())
                    assertThat(caller.delegates).isEqualTo(claims["dlg"]?.asBoolean() ?: false)
                    val expected =
                        claims["cat"].properties().associate { (catalog, scopes) ->
                            catalog to scopes.map { word -> Scope.entries.single { it.wire == word.asText() } }.toSet()
                        }
                    assertThat(caller.catalogScopes).isEqualTo(expected)
                }
            }

    @Test
    fun `the vectors cover a reader, a delegating writer, a vend token and the provisioner`() {
        val byName = vectors().associateBy { it["name"].asText() }
        assertThat(byName.keys).contains(
            "tenant_read",
            "tenant_write_delegating",
            "tenant_vend",
            "operator_provisioner",
        )
        val delegating = byName.getValue("tenant_write_delegating")["token"].asText()
        assertThat(verifier().verify(delegating).delegates).isTrue()
    }

    @Test
    fun `the provisioner's operator token verifies as an operator with no tenant scope`() {
        val vector = vectors().single { it["claims"]["ten"].asText() == "operator" }
        val claims = vector["claims"]
        val caller = verifier().verify(vector["token"].asText())

        assertThat(caller.kind).isEqualTo(Caller.Kind.OPERATOR)
        assertThat(caller.subject).isEqualTo(claims["sub"].asText())
        assertThat(caller.orgId).isNull()
        assertThat(caller.catalogScopes).isEmpty()
        assertThat(caller.delegates).isFalse()
        assertThat(caller.clientName).isEqualTo(claims["cli"].asText())
        assertThat(caller.tokenId).isEqualTo(claims["jti"].asText())
    }

    @Test
    fun `a token minted for the vend audience is refused here`() {
        // One audience per token: a token that can be exchanged for S3
        // credentials must not also open the metadata API, and the reverse.
        val vend = vectors().single { it["audience"].asText() == "hogtower-vend" }
        assertThatThrownBy { verifier().verify(vend["token"].asText()) }
            .isInstanceOfSatisfying(JwtViolation::class.java) {
                assertThat(it.reason).isEqualTo(JwtViolation.WRONG_AUDIENCE)
            }
    }

    @Test
    fun `the vectors expire on the lifetime hogtower states`() {
        val read = vectors().single { it["name"].asText() == "tenant_read" }
        val exp = read["claims"]["exp"].asLong()
        assertThat(exp - read["claims"]["iat"].asLong()).isEqualTo(15 * 60L)
        // Past exp and the skew allowance, the same token is refused.
        assertThatThrownBy { verifier(now = exp + 61).verify(read["token"].asText()) }
            .isInstanceOfSatisfying(JwtViolation::class.java) {
                assertThat(it.reason).isEqualTo(JwtViolation.EXPIRED)
            }
    }
}
