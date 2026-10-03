package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("integration")
class CatalogApiIntegrationTest {
    @Test
    fun `numeric-leading UUID catalog supports provisioning over HTTP`() {
        PgTestSupport.freshDatabase().use { db ->
            val app = App.build(Config(hydratorIntervalMs = 0, metricsIntervalMs = 0), db.jdbi)
            testApplication {
                application { app.module(this) }
                val name = "01234567-89ab-4cde-8fab-0123456789ab"
                val created =
                    client.post("/v1/catalogs") {
                        contentType(ContentType.Application.Json)
                        setBody("""{"name":"$name","data_path":"s3://bucket/uuid/"}""")
                    }
                assertThat(created.status).isEqualTo(HttpStatusCode.Created)
                val fetched = client.get("/v1/catalogs/$name")
                assertThat(fetched.status).isEqualTo(HttpStatusCode.OK)
                assertThat(ObjectMapper().readTree(fetched.bodyAsText())["name"].asText()).isEqualTo(name)
                val namespace =
                    client.post("/v1/catalogs/$name/namespaces") {
                        contentType(ContentType.Application.Json)
                        setBody("""{"name":"main"}""")
                    }
                assertThat(namespace.status).isEqualTo(HttpStatusCode.Created)
                assertThat(client.get("/v1/catalogs/$name/namespaces/main").status).isEqualTo(HttpStatusCode.OK)
            }
        }
    }

    @Test
    fun `packed table creation is disabled by default with typed validation`() {
        PgTestSupport.freshDatabase().use { db ->
            App.build(Config(hydratorIntervalMs = 0, metricsIntervalMs = 0), db.jdbi).use { app ->
                testApplication {
                    application { app.module(this) }
                    assertThat(
                        client.post("/v1/catalogs") {
                            contentType(ContentType.Application.Json)
                            setBody("""{"name":"rollout","data_path":"s3://bucket/rollout/"}""")
                        }.status,
                    ).isEqualTo(HttpStatusCode.Created)
                    assertThat(
                        client.post("/v1/catalogs/rollout/namespaces") {
                            contentType(ContentType.Application.Json)
                            setBody("""{"name":"main"}""")
                        }.status,
                    ).isEqualTo(HttpStatusCode.Created)
                    val refused =
                        client.post("/v1/catalogs/rollout/namespaces/main/tables") {
                            contentType(ContentType.Application.Json)
                            setBody(
                                """
                                {"name":"packed",
                                 "properties":{"write.format.default":"clickhouse-mergetree-packed"},
                                 "columns":[{"name":"id","type":"long"}]}
                                """,
                            )
                        }
                    assertThat(refused.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
                    val error = ObjectMapper().readTree(refused.bodyAsText())
                    assertThat(error["error"].asText()).isEqualTo("validation")
                    assertThat(error["detail"].asText()).contains("HOGLAKE_PACKED_MERGETREE_ENABLED")
                    assertThat(client.get("/v1/catalogs/rollout/namespaces/main/tables").bodyAsText())
                        .isEqualTo("[]")
                }
            }
        }
    }
}
