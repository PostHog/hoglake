package com.posthog.hoglake.auth

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.store.BackendKind
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID

class ReadSessionVectorsTest {
    @TempDir
    lateinit var directory: Path

    private val mapper = ObjectMapper()
    private val root =
        javaClass.getResourceAsStream("/oauth/read-session-vectors.json").use {
            mapper.readTree(requireNotNull(it) { "read session vectors are missing" })
        }

    @Test
    fun `the checked in vectors match the generator`() {
        val generated = mapper.readTree(ContractVectors.render(ReadSessionVectors.document()))
        assertThat(ContractVectors.withoutSignatures(root))
            .isEqualTo(ContractVectors.withoutSignatures(generated))
    }

    @TestFactory
    fun `published tokens match both verifier verdicts`(): List<DynamicTest> {
        val config = root["config"]
        val file = directory.resolve("jwks.json")
        Files.write(file, mapper.writeValueAsBytes(root["jwks"]))
        val keys = JwksSigningKeys.fromFile(file)
        val profile =
            ReadSessionProfile(
                config["profile"].asText(),
                config["remote_targets"].map { it.asText() }.toSet(),
            )
        return root["vectors"].map { vector ->
            DynamicTest.dynamicTest(vector["name"].asText()) {
                val verifier =
                    JwtVerifier(
                        root["issuer"].asText(),
                        ContractVectors.AUDIENCE,
                        keys,
                        predicateFields = setOf(config["predicate_field"].asText()),
                        backends = mapOf(UUID.fromString(config["backend_id"].asText()) to BackendKind.HOGLITE),
                        sessionProfile = true,
                    ) { vector["verify_at"].asLong() }
                val token = vector["token"].asText()
                val expect = vector["expect"]
                if (expect["read"].asText() == "accept") {
                    val caller = verifier.verifyReadSession(token, profile)
                    assertThat(caller.subject).isEqualTo(vector["claims"]["sub"].asText())
                    assertThat(caller.remoteSchemaFamilies).containsExactly("team")
                } else {
                    assertThatThrownBy { verifier.verifyReadSession(token, profile) }
                        .isInstanceOfSatisfying(JwtViolation::class.java) {
                            assertThat(it.reason).isEqualTo(expect["read"].asText())
                        }
                }
                assertThatThrownBy { verifier.verify(token) }.isInstanceOfSatisfying(JwtViolation::class.java) {
                    assertThat(it.reason).isEqualTo(expect["ordinary"].asText())
                }
            }
        }
    }

    /** PROPOSED: the caller rule of a remote schema, for a verified read session. */
    @TestFactory
    fun `a read session relays only the team schema of its own catalog`(): List<DynamicTest> {
        val config = root["config"]
        val file = directory.resolve("relay-jwks.json")
        Files.write(file, mapper.writeValueAsBytes(root["jwks"]))
        val keys = JwksSigningKeys.fromFile(file)
        val profile =
            ReadSessionProfile(
                config["profile"].asText(),
                config["remote_targets"].map { it.asText() }.toSet(),
            )
        return root["relay"].map { vector ->
            DynamicTest.dynamicTest(vector["name"].asText()) {
                val verifier =
                    JwtVerifier(
                        root["issuer"].asText(),
                        ContractVectors.AUDIENCE,
                        keys,
                        predicateFields = setOf(config["predicate_field"].asText()),
                        backends = mapOf(UUID.fromString(config["backend_id"].asText()) to BackendKind.HOGLITE),
                        sessionProfile = true,
                    ) { vector["verify_at"].asLong() }
                val token = vector["token"].asText()
                assertThat(vector["expect"]["read"].asText()).isEqualTo("accept")
                val caller = verifier.verifyReadSession(token, profile)
                val request = vector["request"]
                val relays = caller.readsRemoteSchema(request["catalog"].asText(), request["namespace"].asText())
                assertThat(if (relays) "relay" else "not_found").isEqualTo(vector["expect"]["relay"].asText())
                assertThatThrownBy { verifier.verify(token) }.isInstanceOfSatisfying(JwtViolation::class.java) {
                    assertThat(it.reason).isEqualTo(JwtViolation.WRONG_AUDIENCE)
                }
            }
        }
    }
}
