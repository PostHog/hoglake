package com.posthog.hoglake.auth

import com.fasterxml.jackson.databind.ObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.Base64

/**
 * `HOGLAKE_AUTH_SIGNING_KEY`: what decodes, what is refused and how, and that
 * the kid is the contract vectors' rule.
 */
class LocalSigningKeyTest {
    private val mapper = ObjectMapper()
    private val b64 = Base64.getUrlEncoder().withoutPadding()

    private val vectors =
        mapper.readTree(javaClass.getResourceAsStream("/oauth/contract-vectors.json") ?: error("vectors missing"))
    private val vectorJwk = vectors["jwks"]["keys"][0]
    private val vectorD = vectors["test_private_key_d"].asText()

    /** The vectors' key as a private JWK env value, with [edits] applied first. */
    private fun envValue(vararg edits: Pair<String, String?>): String {
        val jwk =
            mapper.createObjectNode().apply {
                put("kty", "EC")
                put("crv", "P-256")
                put("d", vectorD)
                put("x", vectorJwk["x"].asText())
                put("y", vectorJwk["y"].asText())
            }
        edits.forEach { (k, v) -> if (v == null) jwk.remove(k) else jwk.put(k, v) }
        return b64.encodeToString(mapper.writeValueAsBytes(jwk))
    }

    @Test
    fun `a generated key round-trips through its env value`() {
        val key = LocalSigningKey.generate()
        val again = LocalSigningKey.decode(key.encode())
        assertThat(again.keyId).isEqualTo(key.keyId)
        assertThat(again.publicKey).isEqualTo(key.publicKey)
        assertThat(key.encode()).matches("[A-Za-z0-9_-]+")
        assertThat(LocalSigningKey.generate().keyId).isNotEqualTo(key.keyId)
    }

    @Test
    fun `the kid is the vectors' derivation`() {
        val key = LocalSigningKey.decode(envValue())
        assertThat(key.keyId).isEqualTo(vectorJwk["kid"].asText())
        assertThat(
            LocalSigningKey.keyId(vectorJwk["x"].asText(), vectorJwk["y"].asText()),
        ).isEqualTo(vectorJwk["kid"].asText())
    }

    @Test
    fun `an explicit kid wins, and the public JWKS is what the verifier reads`() {
        val key = LocalSigningKey.decode(envValue("kid" to "my-key"))
        assertThat(key.keyId).isEqualTo("my-key")
        val parsed = JwksSigningKeys.parseJwks(key.publicJwks().toByteArray())
        assertThat(parsed.keys).containsExactly("my-key")
        assertThat(parsed["my-key"]).isEqualTo(key.publicKey)
        assertThat(key.publicJwks()).doesNotContain(vectorD)
        assertThat(key.toString()).doesNotContain(vectorD)
    }

    @Test
    fun `a signature made with the key verifies under its public half`() {
        val key = LocalSigningKey.decode(envValue())
        val header = mapper.createObjectNode().put("alg", "ES256").put("kid", key.keyId).put("typ", "JWT")
        val token = key.sign(header, mapper.createObjectNode().put("a", 1))
        val jws = Jwt.parse(token)
        assertThat(jws.keyId).isEqualTo(key.keyId)
        Jwt.verifySignature(jws, key.publicKey)
    }

    @Test
    fun `refusals name the variable and never echo the value`() {
        val cases =
            mapOf(
                "" to "keygen",
                "   " to "keygen",
                "%%%not-base64%%%" to "base64url",
                b64.encodeToString("[1,2]".toByteArray()) to "JSON object",
                b64.encodeToString("nope".toByteArray()) to "JSON",
                envValue("kty" to "RSA") to "kty EC",
                envValue("crv" to "P-384") to "crv P-256",
                envValue("d" to null) to "missing 'd'",
                envValue("d" to b64.encodeToString(ByteArray(31) { 1 })) to "32 bytes",
                envValue("d" to b64.encodeToString(ByteArray(32))) to "scalar",
                envValue("d" to b64.encodeToString(ByteArray(32) { 0x2b })) to "does not match",
                envValue("x" to b64.encodeToString(ByteArray(32) { 7 })) to "public half",
                envValue("kid" to "") to "kid",
            )
        for ((value, expected) in cases) {
            assertThatThrownBy { LocalSigningKey.decode(value) }
                .describedAs("value expecting '$expected'")
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining(LocalSigningKey.ENV)
                .hasMessageContaining(expected)
                .satisfies({ e ->
                    if (value.length > 8) assertThat(e.message).doesNotContain(value)
                    assertThat(e.message).doesNotContain(vectorD)
                })
        }
    }
}
