package com.posthog.hoglake.api

import com.posthog.hoglake.Config
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.OptionsService
import com.posthog.hoglake.store.CatalogStoreSelector
import com.posthog.hoglake.store.PostgresCatalogStore
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.request.patch
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

/** Invalid option bodies must not force the PostgreSQL options provider. */
@Tag("integration")
class MaintenanceProviderIntegrationTest {
    @Test
    fun `options constructs its provider only after parsing with or without a selector`() {
        PgTestSupport.freshDatabase().use { db ->
            val catalog = "provider-order"
            CatalogService(db.jdbi).createCatalog(catalog, "s3://bucket/provider-order")
            val selector = CatalogStoreSelector(PostgresCatalogStore({ db.jdbi }, Config()))
            for (selected in listOf(false, true)) {
                var calls = 0
                testApplication {
                    application {
                        install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                        install(StatusPages) { installErrorMapping() }
                        installMaintenanceRoutes(
                            optionsProvider = {
                                calls++
                                OptionsService(db.jdbi)
                            },
                            expiryProvider = { error("unused expiry") },
                            cleanupProvider = { error("unused cleanup") },
                            compactionProvider = { error("unused compaction") },
                            hydratorProvider = { error("unused hydrator") },
                            statusProvider = { error("unused status") },
                            databaseHealthProvider = { error("unused database health") },
                            selector = selector.takeIf { selected },
                        )
                    }
                    val invalid =
                        client.patch("/v1/catalogs/$catalog/options") {
                            contentType(ContentType.Application.Json)
                            setBody("{")
                        }
                    assertThat(invalid.status).isEqualTo(HttpStatusCode.BadRequest)
                    assertThat(calls).describedAs("provider calls after invalid JSON, selector=$selected").isZero()
                    val valid =
                        client.patch("/v1/catalogs/$catalog/options") {
                            contentType(ContentType.Application.Json)
                            setBody("""{"consumer_floor":false}""")
                        }
                    assertThat(valid.status).isEqualTo(HttpStatusCode.OK)
                    assertThat(calls).isEqualTo(1)
                    assertThat(OptionsService(db.jdbi).get(catalog).consumerFloor).isFalse()
                }
            }
        }
    }
}
