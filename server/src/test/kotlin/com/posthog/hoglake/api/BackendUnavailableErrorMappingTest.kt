package com.posthog.hoglake.api

import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.WriteReplay
import com.posthog.hoglake.wireObjectMapper
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File

/**
 * The two retryable backend 503s, through the real StatusPages mapping and
 * against the spec. The error codes are the contract, so they are the only
 * literals. Unit test: no database and no engine.
 */
class BackendUnavailableErrorMappingTest {
    private val spec: String = File("src/main/resources/openapi/hoglake.yaml").readText()

    /** The `components/responses` entry, up to the next sibling entry. */
    private fun responseBlock(name: String): String {
        val responses = spec.indexOf("\n  responses:\n")
        assertThat(responses).describedAs("spec has components/responses").isNotEqualTo(-1)
        val header = "\n    $name:\n"
        val start = spec.indexOf(header, responses)
        assertThat(start).describedAs("spec response %s", name).isNotEqualTo(-1)
        val rest = spec.substring(start + header.length)
        val end = Regex("\n    [A-Za-z]|\n  [a-z]").find(rest)?.range?.first ?: rest.length
        return rest.substring(0, end)
    }

    @Test
    fun `both backend codes are 503 with Retry-After and their own error code`() =
        testApplication {
            application {
                install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                install(StatusPages) { installErrorMapping() }
                routing {
                    get("/unavailable") { throw HoglakeException.BackendUnavailable("the database did not answer") }
                    get("/unknown") { throw HoglakeException.CommitOutcomeUnknown("tx-1", "the owner failed") }
                }
            }
            val mapper = wireObjectMapper()
            for ((path, code) in listOf(
                "/unavailable" to "backend_unavailable",
                "/unknown" to "commit_outcome_unknown",
            )) {
                val response = client.get(path)
                assertThat(response.status).describedAs(path).isEqualTo(HttpStatusCode.ServiceUnavailable)
                assertThat(response.headers[HttpHeaders.RetryAfter]).describedAs(path).isEqualTo("1")
                val body = mapper.readTree(response.bodyAsText())
                assertThat(body["error"].asText()).isEqualTo(code)
                assertThat(body["detail"].asText()).isNotBlank()
            }
        }

    /**
     * A replay-unsafe unknown outcome (#682): same status and code, so a
     * client that keys on the code still learns the write may be durable,
     * but NO Retry-After, because a blind replay would truncate twice, be
     * refused as already applied, or register a keyless commit's files a
     * second time. The DDL's detail says so instead of naming an
     * idempotency_key the route does not take; the keyless commit's names
     * the key the caller should have sent.
     */
    @Test
    fun `a replay-unsafe unknown outcome is the same 503 code without a Retry-After header`() =
        testApplication {
            application {
                install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                install(StatusPages) { installErrorMapping() }
                routing {
                    get("/ddl") {
                        throw HoglakeException.CommitOutcomeUnknown(
                            "truncate:tx-2",
                            "the owner failed",
                            WriteReplay.DDL,
                        )
                    }
                    get("/keyless") {
                        throw HoglakeException.CommitOutcomeUnknown(
                            "ch:c:tx-3",
                            "the owner failed",
                            WriteReplay.KEYLESS_COMMIT,
                        )
                    }
                }
            }
            val mapper = wireObjectMapper()
            for (path in listOf("/ddl", "/keyless")) {
                val response = client.get(path)
                assertThat(response.status).describedAs(path).isEqualTo(HttpStatusCode.ServiceUnavailable)
                assertThat(response.headers[HttpHeaders.RetryAfter]).describedAs(path).isNull()
                val body = mapper.readTree(response.bodyAsText())
                assertThat(body["error"].asText()).describedAs(path).isEqualTo("commit_outcome_unknown")
                assertThat(body["detail"].asText())
                    .describedAs(path)
                    .contains("Do not retry it blindly", "the owner failed")
            }
            assertThat(mapper.readTree(client.get("/ddl").bodyAsText())["detail"].asText())
                .contains("already applied", "truncate:tx-2")
                .doesNotContain("idempotency_key")
            assertThat(mapper.readTree(client.get("/keyless").bodyAsText())["detail"].asText())
                .contains("send an idempotency_key", "a second time", "ch:c:tx-3")
        }

    /**
     * The detail reaches API clients. The backend txid is server-generated
     * and random per attempt, so no caller can retry "with the same txid".
     * The detail names what a caller can do.
     */
    @Test
    fun `the commit outcome detail names what a caller can do, not the internal txid retry`() {
        val (_, body) = errorBody(HoglakeException.CommitOutcomeUnknown("tx-1", "the owner failed"))

        assertThat(body.detail).contains("idempotency_key", "the owner failed", "tx-1")
        assertThat(body.detail).doesNotContain("same txid")
    }

    @Test
    fun `the spec documents both backend codes as a 503 with a Retry-After header`() {
        val block = responseBlock("BackendUnavailable")

        assertThat(block).contains("`backend_unavailable`", "`commit_outcome_unknown`")
        assertThat(block).contains("headers:", "Retry-After:")
        // And which writes the header is withheld from (#682).
        assertThat(block).contains("no Retry-After", "truncate")
        assertThat(block).contains("\$ref: \"#/components/schemas/ApiError\"")
    }
}
