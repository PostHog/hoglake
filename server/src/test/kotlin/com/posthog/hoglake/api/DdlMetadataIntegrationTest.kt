package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.ReplacementTarget
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.service.TableCreationService
import com.posthog.hoglake.service.ViewService
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.request.delete
import io.ktor.client.request.parameter
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.UUID

@Tag("integration")
class DdlMetadataIntegrationTest {
    @Test
    fun `destructive wire calls require each nonblank field and leave state unchanged`() =
        testApplication {
            PgTestSupport.freshDatabase().use { db ->
                val catalogs = CatalogService(db.jdbi)
                val app = App.build(Config(hydratorIntervalMs = 0, metricsIntervalMs = 0), db.jdbi)
                application { app.module(this) }
                val catalog = "metadata-wire"
                catalogs.createCatalog(catalog, "s3://b/metadata")
                val namespace = catalogs.createNamespace(catalog, "ns")
                val table = catalogs.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
                val view = ViewService(db.jdbi).create(catalog, "ns", "v", "SELECT 1")
                val base = "/v1/catalogs/$catalog/namespaces/ns"
                val head = catalogs.getCatalog(catalog).headSnapshotId
                val json = ObjectMapper()
                val missingBody = client.post("$base/tables/t/truncate?expected_table_uuid=${table.tableUuid}")
                assertThat(missingBody.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
                assertThat(missingBody.bodyAsText()).contains("author")
                for (field in listOf("author", "message")) {
                    for (value in listOf(42, true, emptyList<String>(), emptyMap<String, String>())) {
                        val body = mapOf("author" to "qe", "message" to "why") + mapOf(field to value)
                        val response =
                            client.post("$base/tables/t/truncate?expected_table_uuid=${table.tableUuid}") {
                                contentType(ContentType.Application.Json)
                                setBody(json.writeValueAsString(body))
                            }
                        assertThat(response.status).isEqualTo(HttpStatusCode.BadRequest)
                        assertThat(catalogs.getCatalog(catalog).headSnapshotId).isEqualTo(head)
                    }
                }
                for (field in listOf("author", "message")) {
                    for (value in listOf(null, "", " \t\r\n", "\u2003\u00a0", "\u0085")) {
                        val metadata = mutableMapOf<String, String?>("author" to "qe", "message" to "reason")
                        metadata[field] = value
                        for (target in listOf("$base/tables/t", "$base", "$base/views/v")) {
                            val response =
                                client.delete(target) {
                                    metadata.forEach { (key, text) -> if (text != null) parameter(key, text) }
                                }
                            assertThat(response.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
                            assertThat(response.bodyAsText()).contains(field)
                        }
                        val response =
                            client.post("$base/tables/t/truncate?expected_table_uuid=${table.tableUuid}") {
                                contentType(ContentType.Application.Json)
                                setBody(json.writeValueAsString(metadata))
                            }
                        assertThat(response.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
                        assertThat(response.bodyAsText()).contains(field)
                        assertThat(catalogs.getCatalog(catalog).headSnapshotId).isEqualTo(head)
                        assertThat(catalogs.getTable(catalog, "ns", "t").tableUuid).isEqualTo(table.tableUuid)
                        assertThat(ViewService(db.jdbi).get(catalog, "ns", "v").viewUuid).isEqualTo(view.viewUuid)
                        assertThat(catalogs.getNamespace(catalog, "ns").namespaceId).isEqualTo(namespace.namespaceId)
                    }
                }
                val response =
                    client.post("$base/tables/t/truncate?expected_table_uuid=${table.tableUuid}") {
                        contentType(ContentType.Application.Json)
                        setBody("""{"author":" qe/λ ","message":" reclaim & rebuild\nnext "}""")
                    }
                assertThat(response.status).isEqualTo(HttpStatusCode.OK)
                val bomBody =
                    byteArrayOf(0xef.toByte(), 0xbb.toByte(), 0xbf.toByte()) +
                        """{"author":"qe","message":"BOM encoded request"}""".toByteArray()
                assertThat(
                    client.post("$base/tables/t/truncate?expected_table_uuid=${table.tableUuid}") {
                        contentType(ContentType.Application.Json)
                        setBody(bomBody)
                    }.status,
                ).isEqualTo(HttpStatusCode.OK)
                val snapshot = json.readTree(response.bodyAsText())["snapshot_id"].asLong()
                db.jdbi.withHandleUnchecked { h ->
                    val row =
                        h.createQuery("SELECT author, commit_message FROM hog_snapshot WHERE snapshot_id = :s")
                            .bind("s", snapshot).mapToMap().one()
                    assertThat(row["author"]).isEqualTo(" qe/λ ")
                    assertThat(row["commit_message"]).isEqualTo(" reclaim & rebuild\nnext ")
                }
            }
        }

    @Test
    fun `atomic attribution survives replay and legacy destructive receipts become terminal`() {
        PgTestSupport.freshDatabase().use { db ->
            val catalogs = CatalogService(db.jdbi)
            val creations = TableCreationService(db.jdbi, catalogs, CommitService(db.jdbi))
            val catalog = "atomic-metadata"
            catalogs.createCatalog(catalog, "s3://b/atomic")
            catalogs.createNamespace(catalog, "ns")
            val definition =
                TableCreationDefinition(
                    "ns",
                    "t",
                    listOf(ColumnDef("id", ColType.LONG)),
                    author = "qe/λ",
                    message = "first",
                )
            val operation = UUID.randomUUID()
            creations.prepare(catalog, operation, definition)
            assertThatThrownBy { creations.prepare(catalog, operation, definition.copy(author = "different")) }
                .isInstanceOf(HoglakeException.CommitConflict::class.java)
            assertThatThrownBy { creations.prepare(catalog, operation, definition.copy(message = "different")) }
                .isInstanceOf(HoglakeException.CommitConflict::class.java)
            val published = creations.publish(catalog, operation, emptyList())
            assertThat(creations.publish(catalog, operation, emptyList())).isEqualTo(published)
            val target = catalogs.getTable(catalog, "ns", "t")
            val replacement =
                definition.copy(
                    replacement = ReplacementTarget(target.tableUuid, published.snapshotId!!),
                    author = null,
                    message = null,
                )
            val legacy = UUID.randomUUID()
            assertThatThrownBy { creations.prepare(catalog, legacy, replacement) }
                .isInstanceOf(HoglakeException.Validation::class.java).hasMessageContaining("author")
            assertThatThrownBy { creations.status(catalog, legacy) }.isInstanceOf(HoglakeException.NotFound::class.java)
            val prepared = creations.prepare(catalog, legacy, replacement.copy(author = "qe", message = "replace"))
            db.jdbi.withHandleUnchecked { h ->
                h.execute(
                    "UPDATE hog_table_creation SET definition = definition - 'author' - 'message' " +
                        "WHERE operation_id = ?",
                    legacy,
                )
                val row =
                    h.createQuery("SELECT author, commit_message FROM hog_snapshot WHERE snapshot_id = :s")
                        .bind("s", published.snapshotId).mapToMap().one()
                assertThat(row["author"]).isEqualTo(definition.author)
                assertThat(row["commit_message"]).isEqualTo(definition.message)
            }
            assertThat(prepared.state).isEqualTo("prepared")
            assertThat(creations.publish(catalog, legacy, emptyList()).state).isEqualTo("rejected")
            assertThat(creations.status(catalog, legacy).reason).isEqualTo("definition_invalid")
            assertThat(catalogs.getTable(catalog, "ns", "t").tableUuid).isEqualTo(target.tableUuid)
            assertThat(catalogs.getCatalog(catalog).headSnapshotId).isEqualTo(published.snapshotId)
        }
    }
}
