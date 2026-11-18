package com.posthog.hoglake.fuzz

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.JwtViolation
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.store.BackendKind
import java.security.interfaces.ECPublicKey
import java.util.UUID

/** Signed audience mutations are checked against an independent configured-audience oracle. */
class JwtAudienceFuzzTest {
    private val pair = TestTokens.keyPair()
    private val json = ObjectMapper()
    private val id = UUID.fromString("7c9e2a14-3f51-4b8d-9e60-2d5a1f8c44aa")
    private val verifier =
        JwtVerifier(
            TestTokens.ISSUER,
            "hoglake",
            StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
            backends = mapOf(id to BackendKind.HOGLITE),
        )

    @FuzzTest(maxDuration = "60s")
    fun audienceMembershipRequiresStrings(data: FuzzedDataProvider) {
        val words = listOf<Any?>("hoglake", "hoglite", "hogtower-vend", "other", "", 7, null)
        val values = List(data.consumeInt(0, 5)) { words[data.consumeInt(0, words.lastIndex)] }
        val scalar = data.consumeBoolean()
        val audience: Any? = if (scalar) values.firstOrNull() else values
        val node = json.readTree(TestTokens.tenantClaims()).deepCopy<com.fasterxml.jackson.databind.node.ObjectNode>()
        node.set<com.fasterxml.jackson.databind.JsonNode>("aud", json.valueToTree(audience))
        node.putObject("rtg").put("b", "hoglite").put("be", id.toString()).put("t", "opaque_1")
        val signed = TestTokens.mint(pair, json.writeValueAsString(node))
        val text = if (scalar) listOf(audience) else values
        val expected =
            text.all { it is String } && "hoglake" in text
        val accepted =
            try {
                verifier.verify(signed)
                true
            } catch (_: JwtViolation) {
                false
            }
        check(accepted == expected) { "audience decision disagrees with the configured-audience oracle" }
    }
}
