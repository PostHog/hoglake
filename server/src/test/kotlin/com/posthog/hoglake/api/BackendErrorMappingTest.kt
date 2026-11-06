package com.posthog.hoglake.api

import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
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

class BackendErrorMappingTest {
    @Test
    fun `deterministic refusals have distinct codes and never invite a retry`() =
        testApplication {
            val errors =
                listOf(
                    HoglakeException.BackendResultTooLarge("narrow the result") to "backend_result_too_large",
                    HoglakeException.BackendQueryTimeout("narrow the query") to "backend_query_timeout",
                    HoglakeException.NotSupported("unsupported operation") to "not_supported_on_backend",
                )
            application {
                install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                install(StatusPages) { installErrorMapping() }
                routing {
                    for ((index, error) in errors.withIndex()) get("/errors/$index") { throw error.first }
                }
            }
            for ((index, error) in errors.withIndex()) {
                val response = client.get("/errors/$index")
                assertThat(
                    response.status,
                ).isEqualTo(if (index == 2) HttpStatusCode.NotImplemented else HttpStatusCode.UnprocessableEntity)
                assertThat(response.headers[HttpHeaders.RetryAfter]).isNull()
                assertThat(response.bodyAsText()).contains("\"error\":\"${error.second}\"")
                assertThat(Metrics.commitFailureResult(error.first)).isEqualTo(error.second)
                assertThat(Audit.failureOutcome(error.first)).isEqualTo(error.second)
            }
            assertThat(
                Metrics.commitFailureResult(HoglakeException.BackendUnavailable("down")),
            ).isEqualTo("backend_unavailable")
            assertThat(
                Audit.failureOutcome(HoglakeException.CommitOutcomeUnknown("lost", "connection closed")),
            ).isEqualTo("commit_outcome_unknown")
        }
}
