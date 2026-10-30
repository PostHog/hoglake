package com.posthog.hoglake.api

import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.model.HoglakeException
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

/**
 * A commit that may have landed reaches the API as a retryable 503 with
 * its own code — never a 4xx a writer would treat as final, and not
 * `backend_unavailable`, which says nothing about the commit.
 */
class CommitOutcomeUnknownMappingTest {
    @Test
    fun `an unknown commit outcome is a retryable 503 with its own code`() =
        testApplication {
            application {
                install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                install(StatusPages) { installErrorMapping() }
                routing {
                    get("/unknown") { throw HoglakeException.CommitOutcomeUnknown("tx-1", "owner lost") }
                    get("/down") { throw HoglakeException.BackendUnavailable("pool down") }
                }
            }
            val mapper = wireObjectMapper()
            val unknown = client.get("/unknown")
            assertThat(unknown.status).isEqualTo(HttpStatusCode.ServiceUnavailable)
            assertThat(unknown.headers[HttpHeaders.RetryAfter]).isEqualTo("1")
            val body = mapper.readTree(unknown.bodyAsText())
            assertThat(body["error"].asText()).isEqualTo("commit_outcome_unknown")
            assertThat(body["detail"].asText()).contains("tx-1", "may or may not have committed")
            val down = client.get("/down")
            assertThat(down.status).isEqualTo(HttpStatusCode.ServiceUnavailable)
            assertThat(mapper.readTree(down.bodyAsText())["error"].asText()).isEqualTo("backend_unavailable")
        }
}
