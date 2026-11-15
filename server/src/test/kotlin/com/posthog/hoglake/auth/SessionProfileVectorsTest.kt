package com.posthog.hoglake.auth

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.store.BackendKind
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import java.util.UUID

/**
 * The session profile's vectors, checked against the verifier with the profile
 * ON — and, for every one of them, against the verifier with it OFF, where the
 * only acceptable verdict is `profile_not_enabled`.
 */
class SessionProfileVectorsTest {
    private val root: JsonNode =
        ObjectMapper().readTree(
            javaClass.getResourceAsStream("/oauth/session-profile-vectors.json")
                ?: error("oauth/session-profile-vectors.json is missing from the test resources"),
        )
    private val keys = JwksSigningKeys.parseJwks(ObjectMapper().writeValueAsBytes(root["jwks"]))
    private val verifyAt = root["verify_at"].asLong()

    private fun verifier(sessionProfile: Boolean): JwtVerifier {
        val (kid, key) = keys.entries.single()
        return JwtVerifier(
            root["issuer"].asText(),
            "hoglake",
            StaticSigningKeys(kid, key),
            60,
            setOf("team_id"),
            backends =
                vectors().mapNotNull { it["claims"]["rtg"]?.get("be")?.asText() }
                    .associate { UUID.fromString(it) to BackendKind.HOGLITE },
            sessionProfile = sessionProfile,
        ) { verifyAt }
    }

    private fun vectors(): List<JsonNode> = root["vectors"].toList()

    @Test
    fun `the checked-in file is what the generator writes, signatures aside`() {
        assertThat(ContractVectors.withoutSignatures(root))
            .describedAs("oauth/session-profile-vectors.json is stale: run `just server contract-vectors`")
            .isEqualTo(
                ContractVectors.withoutSignatures(
                    ObjectMapper().readTree(ContractVectors.render(SessionProfileVectors.document())),
                ),
            )
        assertThat(root["config"]["session_profile"].asText()).isEqualTo("on")
    }

    @Test
    fun `every vector is a mutation until an issuer mints the profile`() {
        vectors().forEach { assertThat(it.has("mutation")).describedAs(it["name"].asText()).isTrue() }
    }

    @TestFactory
    fun `every vector gets the verdict its expect states with the profile on`(): List<DynamicTest> =
        vectors().map { vector ->
            DynamicTest.dynamicTest(vector["name"].asText()) {
                val expect = vector["expect"]
                val token = vector["token"].asText()
                when (expect["verdict"].asText()) {
                    "accept" -> {
                        val caller = verifier(true).verify(token)
                        assertThat(caller.kind.name.lowercase()).isEqualTo(expect["kind"].asText())
                        assertThat(caller.remoteSchemas)
                            .isEqualTo(vector["claims"]["remote_schemas"].fieldNames().asSequence().toSet())
                    }
                    "refuse" ->
                        assertThatThrownBy { verifier(true).verify(token) }
                            .isInstanceOfSatisfying(JwtViolation::class.java) {
                                assertThat(it.reason).isEqualTo(expect["reason"].asText())
                            }
                    else -> error("unknown verdict ${expect["verdict"]}")
                }
            }
        }

    @TestFactory
    fun `every vector is refused as profile_not_enabled with the profile off`(): List<DynamicTest> =
        vectors().filter { it["claims"].has("profile") || it["claims"].has("aws") }.map { vector ->
            DynamicTest.dynamicTest("off: " + vector["name"].asText()) {
                assertThatThrownBy { verifier(false).verify(vector["token"].asText()) }
                    .isInstanceOfSatisfying(JwtViolation::class.java) {
                        assertThat(it.reason).isEqualTo(JwtViolation.PROFILE_NOT_ENABLED)
                    }
            }
        }

    @Test
    fun `no refusal and no caller ever carries a credential value`() {
        val fictional = listOf("FICTIONAL_ACCESS_KEY_ID", "FICTIONAL_SECRET_ACCESS_KEY", "FICTIONAL_SESSION_TOKEN")
        vectors().forEach { vector ->
            val token = vector["token"].asText()
            listOf(true, false).forEach { on ->
                val text =
                    try {
                        verifier(on).verify(token).toString()
                    } catch (v: JwtViolation) {
                        v.reason + " " + v.message
                    }
                fictional.forEach { assertThat(text).describedAs(vector["name"].asText()).doesNotContain(it) }
            }
        }
    }
}
