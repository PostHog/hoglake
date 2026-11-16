package com.posthog.hoglake.api

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.request.get
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

/** Both default and explicitly routed PostgreSQL retain the fleet endpoints. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FleetMaintenanceRoutesIntegrationTest {
    private val db = PgTestSupport.freshDatabase()

    @AfterAll
    fun close() = db.close()

    @ParameterizedTest
    @ValueSource(strings = ["postgres", "none"])
    fun `configured PostgreSQL serves instance status runs and database health`(defaultBackend: String) {
        val cfg =
            Config(
                defaultBackend = defaultBackend,
                authMode = "off",
                postgresBackendId = "5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e",
                jdbcUrl = db.jdbcUrl,
                dbUser = PgTestSupport.USER,
                dbPassword = PgTestSupport.PASSWORD,
            )
        App.build(cfg, db.jdbi).use { app ->
            testApplication {
                application { app.module(this) }
                for (path in listOf("/v1/maintenance/status", "/v1/maintenance/runs", "/v1/database/health")) {
                    assertThat(client.get(path).status).describedAs(path).isEqualTo(HttpStatusCode.OK)
                }
            }
        }
    }
}
