package com.posthog.hoglake.api

import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.model.PartitionLookupInvalidRequest
import com.posthog.hoglake.model.PartitionLookupUnavailable
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

class PartitionLookupErrorMappingTest {
    @Test
    fun `malformed lookup input is a bad request and capacity failures are retryable`() =
        testApplication {
            application {
                install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                install(StatusPages) { installErrorMapping() }
                routing {
                    get("/invalid") { throw PartitionLookupInvalidRequest("invalid tuple") }
                    get("/busy") { throw PartitionLookupUnavailable("busy", "capacity busy") }
                    get("/timeout") { throw PartitionLookupUnavailable("timeout", "query timed out") }
                }
            }
            val mapper = wireObjectMapper()
            val invalid = client.get("/invalid")
            assertThat(invalid.status).isEqualTo(HttpStatusCode.BadRequest)
            assertThat(mapper.readTree(invalid.bodyAsText())["error"].asText()).isEqualTo("bad_request")
            assertThat(invalid.headers[HttpHeaders.RetryAfter]).isNull()
            for (reason in listOf("busy", "timeout")) {
                val response = client.get("/$reason")
                assertThat(response.status).isEqualTo(HttpStatusCode.ServiceUnavailable)
                assertThat(response.headers[HttpHeaders.RetryAfter]).isEqualTo("1")
                val body = mapper.readTree(response.bodyAsText())
                assertThat(body["error"].asText()).isEqualTo("partition_lookup_unavailable")
                assertThat(body["reason"].asText()).isEqualTo(reason)
                assertThat(body["detail"].asText()).isNotBlank()
            }
        }
}
