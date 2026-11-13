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
 * The token contract, checked against the verifier that enforces it.
 *
 * hoglake owns `oauth/contract-vectors.json`: [ContractVectors] generates it
 * (`just server contract-vectors`) and issuers conform to it, PostHog's
 * included. Their suites mint each vector's input and compare header and
 * claims; this one checks that the verifier gives every checked-in token the
 * verdict its `expect` states, and that the file is still what the generator
 * writes, so a contract change cannot land in one place and not the other.
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
        predicateFields: Set<String> = setOf("team_id"),
    ): JwtVerifier {
        val (kid, key) = keys.entries.single()
        return JwtVerifier(
            root["issuer"].asText(),
            audience,
            StaticSigningKeys(kid, key),
            60,
            predicateFields,
            backends =
                vectors().mapNotNull { it["claims"]["rtg"]?.get("be")?.asText() }
                    .associate { UUID.fromString(it) to BackendKind.HOGLITE },
        ) { now }
    }

    private fun vectors(): List<JsonNode> = root["vectors"].toList()

    private fun accepted(kind: String): List<JsonNode> =
        vectors().filter { it["expect"]["verdict"].asText() == "accept" && it["expect"]["kind"].asText() == kind }

    @Test
    fun `the checked-in file is what the generator writes, signatures aside`() {
        // ES256 signatures are randomised, so the comparison drops them; the
        // signatures themselves are verified by every test below.
        assertThat(ContractVectors.withoutSignatures(root))
            .describedAs("oauth/contract-vectors.json is stale: run `just server contract-vectors`")
            .isEqualTo(
                // Through the file's own text form, so number widths compare as read.
                ContractVectors.withoutSignatures(
                    ObjectMapper().readTree(ContractVectors.render(ContractVectors.document())),
                ),
            )
    }

    @TestFactory
    fun `every vector gets the verdict its expect states`(): List<DynamicTest> =
        vectors().map { vector ->
            DynamicTest.dynamicTest(vector["name"].asText()) {
                val expect = vector["expect"]
                val token = vector["token"].asText()
                when (expect["verdict"].asText()) {
                    "accept" ->
                        assertThat(verifier().verify(token).kind.name.lowercase())
                            .isEqualTo(expect["kind"].asText())
                    "refuse" ->
                        assertThatThrownBy { verifier().verify(token) }
                            .isInstanceOfSatisfying(JwtViolation::class.java) {
                                assertThat(it.reason).isEqualTo(expect["reason"].asText())
                            }
                    else -> error("unknown verdict ${expect["verdict"]}")
                }
            }
        }

    @Test
    fun `the vectors pin an absent ten as tenant, and only tenant`() {
        val byName = vectors().associateBy { it["name"].asText() }
        listOf("tenant_without_ten", "scoped_without_ten", "operator_without_ten").forEach {
            assertThat(byName.getValue(it)["claims"].has("ten")).describedAs(it).isFalse()
        }
        assertThat(verifier().verify(byName.getValue("tenant_without_ten")["token"].asText()).kind)
            .isEqualTo(Caller.Kind.TENANT)
        // A scoped or operator token must say so: without ten, neither verifies.
        listOf("scoped_without_ten", "operator_without_ten").forEach { name ->
            assertThatThrownBy { verifier().verify(byName.getValue(name)["token"].asText()) }
                .describedAs(name)
                .isInstanceOf(JwtViolation::class.java)
        }
    }

    @TestFactory
    fun `every accepted tenant vector verifies to the caller its claims describe`(): List<DynamicTest> =
        accepted("tenant")
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
    fun `the vectors cover a reader, a delegating writer, a foreign-audience token and the provisioner`() {
        val byName = vectors().associateBy { it["name"].asText() }
        assertThat(byName.keys).contains(
            "tenant_read",
            "tenant_write_delegating",
            "tenant_other_audience",
            "operator_provisioner",
        )
        val delegating = byName.getValue("tenant_write_delegating")["token"].asText()
        assertThat(verifier().verify(delegating).delegates).isTrue()
    }

    @Test
    fun `the provisioner's operator token verifies as an operator with no tenant scope`() {
        val vector = vectors().single { it["name"].asText() == "operator_provisioner" }
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
    fun `a team credential's scoped vector verifies to a scoped caller holding its teams`() {
        val vector = vectors().single { it["name"].asText() == "scoped_team" }
        val claims = vector["claims"]
        val caller = verifier().verify(vector["token"].asText())

        assertThat(caller.kind).isEqualTo(Caller.Kind.SCOPED)
        assertThat(caller.subject).isEqualTo(claims["sub"].asText())
        assertThat(caller.orgId).isEqualTo(claims["org"].asText())
        assertThat(caller.user).isEqualTo(claims["usr"].asText())
        assertThat(caller.delegates).isFalse()
        assertThat(caller.catalogScopes).isEqualTo(mapOf(claims["org"].asText() to setOf(Scope.WRITE)))
        val teams =
            claims["prd"]["team_id"].properties().associate { (id, perm) ->
                id.toLong() to Scope.entries.single { it.wire == perm.asText() }
            }
        assertThat(caller.predicates).isEqualTo(mapOf("team_id" to PredicateGrant.Values(teams)))

        // An instance whose auth source declares no predicate field refuses it.
        assertThatThrownBy { verifier(predicateFields = emptySet()).verify(vector["token"].asText()) }
            .isInstanceOfSatisfying(JwtViolation::class.java) {
                assertThat(it.reason).isEqualTo(JwtViolation.INVALID_PREDICATE)
            }
    }

    @Test
    fun `a token minted for another audience is refused here`() {
        // One audience per token: a token minted for another service (an
        // issuer's credential-vending endpoint, say) must not also open the
        // metadata API, and the reverse.
        val other = vectors().single { it["audience"].asText() == "other-service" }
        assertThatThrownBy { verifier().verify(other["token"].asText()) }
            .isInstanceOfSatisfying(JwtViolation::class.java) {
                assertThat(it.reason).isEqualTo(JwtViolation.WRONG_AUDIENCE)
            }
    }

    @TestFactory
    fun `routing vectors pin single catalog placement for every token kind`(): List<DynamicTest> =
        vectors().filter { it["expect"]["verdict"].asText() == "accept" }.map { vector ->
            DynamicTest.dynamicTest("placement: " + vector["name"].asText()) {
                val claims = vector["claims"]
                val caller = verifier().verify(vector["token"].asText())
                val rtg = claims["rtg"]
                if (rtg == null) {
                    assertThat(caller.routing).isNull()
                } else {
                    assertThat(rtg.properties().map { it.key }).containsExactlyInAnyOrder("b", "be", "t")
                    assertThat(caller.routing).isEqualTo(
                        Routing(
                            claims["cat"].fieldNames().next(),
                            BackendKind.HOGLITE,
                            UUID.fromString(rtg["be"].asText()),
                            rtg["t"].asText(),
                        ),
                    )
                    assertThat(caller.orgId).isEqualTo(claims["org"].asText())
                    if (vector["expect"]["kind"].asText() == "operator") {
                        assertThat(caller.kind).isEqualTo(Caller.Kind.OPERATOR)
                        assertThat(caller.catalogScopes).isEmpty()
                    }
                }
            }
        }

    @Test
    fun `the vectors expire on the lifetime the contract states`() {
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
