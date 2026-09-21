package com.posthog.hoglake.api

import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.request.delete
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.UUID

class TableLifecycleRoutesIntegrationTest {
    @Test
    fun `lifecycle routes enforce guards and advertise support`() =
        testApplication {
            PgTestSupport.freshDatabase().use { db ->
                val catalogs = CatalogService(db.jdbi)
                catalogs.createCatalog("wire-lifecycle", "s3://bucket/wire-lifecycle")
                catalogs.createNamespace("wire-lifecycle", "ns")
                val table = catalogs.createTable("wire-lifecycle", "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
                application {
                    install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                    install(StatusPages) { installErrorMapping() }
                    installApiRoutes(catalogs, CommitService(db.jdbi))
                    installAlterRoutes(AlterService(db.jdbi))
                }
                val base = "/v1/catalogs/wire-lifecycle"
                val path = "$base/namespaces/ns/tables/t"
                val expected = "?expected_table_uuid=${table.tableUuid}"
                val stale = "?expected_table_uuid=${UUID.randomUUID()}"
                assertThat(client.get(base).bodyAsText()).contains("guarded-table-lifecycle-v1")
                assertThat(client.post("$path/truncate").status).isEqualTo(HttpStatusCode.BadRequest)
                assertThat(client.post("$path/truncate?expected_table_uuid=invalid").status)
                    .isEqualTo(HttpStatusCode.BadRequest)
                assertThat(client.delete("$path?expected_table_uuid=invalid").status)
                    .isEqualTo(HttpStatusCode.BadRequest)
                assertThat(client.post("$path/truncate$stale").status).isEqualTo(HttpStatusCode.Conflict)
                assertThat(client.delete("$path$stale").status).isEqualTo(HttpStatusCode.Conflict)
                val rename = """{"ops":[{"op":"rename_table","new_name":"renamed"}]}"""
                assertThat(
                    client.post("$path/alter$stale") {
                        contentType(ContentType.Application.Json)
                        setBody(rename)
                    }.status,
                ).isEqualTo(HttpStatusCode.Conflict)
                assertThat(client.post("$path/truncate$expected").status).isEqualTo(HttpStatusCode.OK)
                assertThat(
                    client.post("$path/alter$expected") {
                        contentType(ContentType.Application.Json)
                        setBody(rename)
                    }.status,
                ).isEqualTo(HttpStatusCode.OK)
                assertThat(client.delete("$base/namespaces/ns/tables/renamed$expected").status)
                    .isEqualTo(HttpStatusCode.OK)
            }
        }
}
