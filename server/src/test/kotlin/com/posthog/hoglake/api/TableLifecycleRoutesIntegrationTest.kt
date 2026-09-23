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
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.UUID

@Tag("integration")
class TableLifecycleRoutesIntegrationTest {
    @Test
    fun `schema guards fence reused names and stale column plans`() =
        testApplication {
            PgTestSupport.freshDatabase().use { db ->
                val catalogs = CatalogService(db.jdbi)
                val alter = AlterService(db.jdbi)
                catalogs.createCatalog("schema-guards", "s3://bucket/schema-guards")
                val namespace = catalogs.createNamespace("schema-guards", "ns")
                val table = catalogs.createTable("schema-guards", "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
                application {
                    install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                    install(StatusPages) { installErrorMapping() }
                    installApiRoutes(catalogs, CommitService(db.jdbi))
                    installAlterRoutes(alter)
                }
                val base = "/v1/catalogs/schema-guards"
                val path = "$base/namespaces/ns/tables/t/alter?expected_table_uuid=${table.tableUuid}"
                val snapshot = catalogs.getCatalog("schema-guards").headSnapshotId
                val add = """{"ops":[{"op":"add_column","column":{"name":"extra","type":"long"}}]}"""

                suspend fun alteration(
                    basis: String,
                    body: String,
                ) = client.post("$path&read_snapshot=$basis") {
                    contentType(ContentType.Application.Json)
                    setBody(body)
                }
                assertThat(client.get(base).bodyAsText()).contains("guarded-schema-evolution-v1")
                assertThat(client.get("$base/namespaces/ns").bodyAsText()).contains("namespace_id")
                for (invalid in listOf("invalid", "9223372036854775808")) {
                    assertThat(alteration(invalid, add).status).isEqualTo(HttpStatusCode.BadRequest)
                }
                for (invalid in listOf("-1", "${snapshot + 1}")) {
                    assertThat(alteration(invalid, add).status).isEqualTo(HttpStatusCode.UnprocessableEntity)
                }
                catalogs.createNamespace("schema-guards", "unrelated")
                assertThat(alteration("$snapshot", add).status).isEqualTo(HttpStatusCode.OK)
                val drop = """{"ops":[{"op":"drop_column","name":"id"}]}"""
                assertThat(alteration("$snapshot", drop).status).isEqualTo(HttpStatusCode.Conflict)
                assertThat(catalogs.getTable("schema-guards", "ns", "t").columns.map { it.def.name })
                    .containsExactly("id", "extra")
                val floor = catalogs.getCatalog("schema-guards").headSnapshotId
                db.jdbi.useHandleUnchecked { h ->
                    h.execute("UPDATE hog_catalog SET earliest_snapshot_id = ? WHERE name = ?", floor, "schema-guards")
                }
                assertThat(alteration("$snapshot", drop).status).isEqualTo(HttpStatusCode.Gone)
                catalogs.dropTable("schema-guards", "ns", "t")
                catalogs.dropNamespace("schema-guards", "ns")
                val replacement = catalogs.createNamespace("schema-guards", "ns")
                assertThat(client.delete("$base/namespaces/ns?expected_namespace_id=bad").status)
                    .isEqualTo(HttpStatusCode.BadRequest)
                assertThat(client.delete("$base/namespaces/ns?expected_namespace_id=${namespace.namespaceId}").status)
                    .isEqualTo(HttpStatusCode.Conflict)
                assertThat(catalogs.getNamespace("schema-guards", "ns")).isEqualTo(replacement)
                assertThat(client.delete("$base/namespaces/ns?expected_namespace_id=${replacement.namespaceId}").status)
                    .isEqualTo(HttpStatusCode.OK)
            }
        }

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
                val before = catalogs.getCatalog("wire-lifecycle").headSnapshotId
                assertThat(client.post("$path/truncate$expected").status).isEqualTo(HttpStatusCode.OK)
                val changes = client.get("$path/changes?from_snapshot=$before")
                assertThat(changes.status).isEqualTo(HttpStatusCode.Conflict)
                assertThat(changes.bodyAsText()).contains("reconciliation_required", "Reconcile")
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
