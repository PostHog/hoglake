package com.posthog.hoglake.api

import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.model.CompactionResult
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.persistence.PartialResult
import com.posthog.hoglake.service.CompactionBackend
import io.ktor.client.request.post
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class PartialMaintenanceErrorMappingTest {
    private class Failed(cause: Throwable) : RuntimeException(cause), PartialResult {
        override val partial = CompactionResult(1, 2, 1, 20, 10, 0)
    }

    @ParameterizedTest
    @ValueSource(strings = ["unknown", "unavailable", "missing", "partial_fault", "ordinary_wrapper"])
    fun `the real maintenance route preserves typed partial failures but not opaque faults`(kind: String) {
        val (failure, status, code) =
            when (kind) {
                "unknown" ->
                    Triple(
                        Failed(HoglakeException.CommitOutcomeUnknown("tx", "lost answer")),
                        HttpStatusCode.ServiceUnavailable,
                        "commit_outcome_unknown",
                    )
                "unavailable" ->
                    Triple(
                        Failed(HoglakeException.BackendUnavailable("down")),
                        HttpStatusCode.ServiceUnavailable,
                        "backend_unavailable",
                    )
                "missing" ->
                    Triple(
                        Failed(HoglakeException.NotFound("catalog dropped")),
                        HttpStatusCode.NotFound,
                        "not_found",
                    )
                "partial_fault" ->
                    Triple(
                        Failed(IllegalStateException("private details")),
                        HttpStatusCode.InternalServerError,
                        "internal_error",
                    )
                else ->
                    Triple(
                        RuntimeException(HoglakeException.NotFound("private details")),
                        HttpStatusCode.InternalServerError,
                        "internal_error",
                    )
            }
        testApplication {
            application {
                install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                install(StatusPages) { installErrorMapping() }
                installMaintenanceRoutes(
                    optionsProvider = { error("unused options") },
                    expiryProvider = { error("unused expiry") },
                    cleanupProvider = { error("unused cleanup") },
                    hydratorProvider = { error("unused hydrator") },
                    statusProvider = { error("unused status") },
                    databaseHealthProvider = { error("unused health") },
                    compactionProvider = {
                        object : CompactionBackend {
                            override fun runOnce(
                                catalog: String,
                                batchOverride: Int?,
                                trigger: MaintenanceTrigger,
                            ): CompactionResult = throw failure
                        }
                    },
                )
            }
            val response = client.post("/v1/catalogs/lake/maintenance/compact")
            assertThat(response.status).isEqualTo(status)
            assertThat(response.bodyAsText()).contains("\"error\":\"$code\"")
            assertThat(response.headers[HttpHeaders.RetryAfter])
                .isEqualTo(if (status == HttpStatusCode.ServiceUnavailable) "1" else null)
            if (status == HttpStatusCode.InternalServerError) {
                assertThat(response.bodyAsText()).doesNotContain("private details")
            }
        }
        assertThat(Audit.failureOutcome(failure)).isEqualTo(if (code == "internal_error") "error" else code)
    }
}
