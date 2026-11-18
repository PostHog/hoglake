package com.posthog.hoglake.auth

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.store.BackendKind
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import java.util.UUID

/**
 * Executes the shared M3-A corpus through the existing verifier and containment API.
 *
 * The corpus supplies synthetic containment after tenant-local placement is established.
 * This class does not implement a metadata handler or assert backend HTTP acceptance.
 * Endpoint tests must cover stored identity, owner forwarding, and cold refusal.
 */
class ScopedMetadataConformanceTest {
    private val mapper = ObjectMapper()
    private val corpus = resource("oauth/scoped-metadata-vectors.json")
    private val source = resource(corpus["token_fixture"]["resource"].asText())
    private val vector =
        source["vectors"].single {
            it["name"].asText() == corpus["token_fixture"]["vector"].asText()
        }
    private val placement = corpus["placement_precondition"]
    private val backendId = UUID.fromString(placement["backend_id"].asText())
    private val backend = BackendKind.entries.single { it.wire == placement["backend"].asText() }

    private fun resource(name: String): JsonNode =
        javaClass.getResourceAsStream("/$name")?.use { mapper.readTree(it) }
            ?: error("$name is missing from the test resources")

    private fun verifiedCaller(): Caller {
        val (kid, key) = JwksSigningKeys.parseJwks(mapper.writeValueAsBytes(source["jwks"])).entries.single()
        val verification = corpus["verification"]
        val caller =
            JwtVerifier(
                source["issuer"].asText(),
                verification["audience"].asText(),
                StaticSigningKeys(kid, key),
                60,
                verification["predicate_fields"].map { it.asText() }.toSet(),
                backends = mapOf(backendId to backend),
            ) { source["verify_at"].asLong() }.verify(vector["token"].asText())

        // The fixture establishes isolation, not the nullable organization mark.
        // Routing assertions bind this fixture to the placement from the verified token.
        assertThat(placement["tenant_local_isolation_established"].asBoolean()).isTrue()
        assertThat(caller.routingIgnored).isFalse()
        assertThat(caller.routing).isEqualTo(
            Routing(
                placement["catalog"].asText(),
                backend,
                backendId,
                placement["tenant"].asText(),
            ),
        )
        return caller
    }

    @Test
    fun `the signed fixture preserves verified caller attribution`() {
        val caller = verifiedCaller()
        val expected = corpus["expected_caller"]

        assertThat(corpus["cases"].size()).isEqualTo(8)
        assertThat(caller.kind.name.lowercase()).isEqualTo(expected["kind"].asText())
        assertThat(caller.subject).isEqualTo(expected["subject"].asText())
        assertThat(caller.actor).isEqualTo(expected["actor"].asText())
        assertThat(caller.orgId).isEqualTo(expected["org"].asText())
        assertThat(caller.user).isEqualTo(expected["user"].asText())
        assertThat(caller.tokenId).isEqualTo(expected["token_id"].asText())
        assertThat(caller.clientName).isEqualTo(expected["client_name"].asText())
        assertThat(caller.delegates).isEqualTo(expected["delegates"].asBoolean())
        assertThat(caller.user).isNotEqualTo(caller.subject)
    }

    @TestFactory
    fun `shared synthetic metadata cases use verified containment authority`(): List<DynamicTest> =
        corpus["cases"].map { case ->
            DynamicTest.dynamicTest(case["name"].asText()) {
                val caller = verifiedCaller()
                val containment = case["containment"]
                val org = containment["org"].takeUnless { it.isNull }?.asText()
                val scope = Scope.entries.single { it.wire == case["scope"].asText() }

                // The namespace name supplies context only. The API uses the supplied containment.
                assertThat(
                    caller.mayUseContained(
                        case["catalog"].asText(),
                        containment["field"].asText(),
                        containment["value"].asLong(),
                        org,
                        scope,
                    ),
                ).isEqualTo(case["expected_may_use_contained"].asBoolean())
            }
        }
}
