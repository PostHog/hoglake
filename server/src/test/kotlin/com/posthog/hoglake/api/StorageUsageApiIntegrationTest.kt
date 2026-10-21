package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.request.get
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Handle
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("integration")
class StorageUsageApiIntegrationTest {
    private val json = ObjectMapper()

    @Test
    fun `exact footprint includes history dropped tables and deletion vectors but not other catalogs`() {
        PgTestSupport.freshDatabase().use { db ->
            val catalogs = CatalogService(db.jdbi)
            val catalog = catalogs.createCatalog("footprint", "s3://footprint")
            catalogs.createNamespace("footprint", "ns")
            val table = catalogs.createTable("footprint", "ns", "events", listOf(ColumnDef("id", ColType.LONG)))
            val dropped = catalogs.createTable("footprint", "ns", "dropped", listOf(ColumnDef("id", ColType.LONG)))
            val other = catalogs.createCatalog("other", "s3://other")
            catalogs.createNamespace("other", "ns")
            val otherTable = catalogs.createTable("other", "ns", "events", listOf(ColumnDef("id", ColType.LONG)))
            val large = 9_007_199_254_740_993L
            db.jdbi.useHandle<Exception> { h ->
                dataFile(h, catalog.catalogId, table.tableId, 1, large)
                dataFile(h, catalog.catalogId, table.tableId, 2, 31, end = 3)
                dataFile(h, catalog.catalogId, dropped.tableId, 3, 41)
                dataFile(h, other.catalogId, otherTable.tableId, 1, 9999)
                h.execute(
                    """
                    INSERT INTO hog_delete_file
                        (catalog_id, delete_file_id, table_id, data_file_id,
                         begin_snapshot, end_snapshot, path, delete_count, file_size_bytes)
                    VALUES (?, 1, ?, 1, 2, 3, 's3://footprint/old.puffin', 1, 5),
                           (?, 2, ?, 1, 3, NULL, 's3://footprint/new.puffin', 1, 7)
                    """,
                    catalog.catalogId,
                    table.tableId,
                    catalog.catalogId,
                    table.tableId,
                )
                h.execute(
                    """
                    INSERT INTO hog_delete_file
                        (catalog_id, delete_file_id, table_id, data_file_id,
                         begin_snapshot, path, delete_count, file_size_bytes)
                    VALUES (?, 1, ?, 1, 2, 's3://other/delete.puffin', 1, 17)
                    """,
                    other.catalogId,
                    otherTable.tableId,
                )
                // Two undrained rows for the SAME path must both count. Claims/staging
                // also count, while every settled outcome is absent from the queue.
                h.execute(
                    """
                    INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                    VALUES (?, 's3://footprint/queued.parquet', 'data', 'snapshot_expiry'),
                           (?, 's3://footprint/queued.parquet', 'data', 'table_drop_gc'),
                           (?, 's3://other/queued.parquet', 'data', 'snapshot_expiry')
                    """,
                    catalog.catalogId,
                    catalog.catalogId,
                    other.catalogId,
                )
                h.execute(
                    """
                    INSERT INTO hog_file_removal
                        (catalog_id, path, file_kind, reason, claimed_at, claimed_by)
                    VALUES (?, 's3://footprint/staging.parquet', 'data',
                            'compaction_staging', now(), 'test-worker')
                    """,
                    catalog.catalogId,
                )
                for (outcome in listOf("deleted", "absent", "registered")) {
                    h.execute(
                        """
                        INSERT INTO hog_file_removal
                            (catalog_id, path, file_kind, reason, drained_at, drained_outcome)
                        VALUES (?, ?, 'data', 'compaction_staging', now(), ?)
                        """,
                        catalog.catalogId,
                        "s3://footprint/$outcome.parquet",
                        outcome,
                    )
                }
            }
            catalogs.dropTable(
                "footprint",
                "ns",
                "dropped",
                dropped.tableUuid,
                author = "integration-test",
                message = "drop table fixture",
            )
            val app = App.build(Config(hydratorIntervalMs = 0, metricsIntervalMs = 0), db.jdbi)
            testApplication {
                application { app.module(this) }
                val response = client.get("/v1/catalogs/footprint/storage-usage")
                assertThat(response.status).isEqualTo(HttpStatusCode.OK)
                assertUsage(json.readTree(response.bodyAsText()), (large + 84).toString(), "3")
                assertUsage(
                    json.readTree(client.get("/v1/catalogs/other/storage-usage").bodyAsText()),
                    "10016",
                    "1",
                )

                // Manifest removal, not physical object deletion, stops tracking a file.
                db.jdbi.useHandle<Exception> { h ->
                    h.execute("DELETE FROM hog_data_file WHERE catalog_id = ? AND data_file_id = 2", catalog.catalogId)
                    h.execute(
                        "DELETE FROM hog_delete_file WHERE catalog_id = ? AND delete_file_id = 1",
                        catalog.catalogId,
                    )
                    h.execute(
                        """
                        UPDATE hog_file_removal SET drained_at = now(), drained_outcome = 'deleted'
                        WHERE catalog_id = ? AND claimed_by = 'test-worker'
                        """,
                        catalog.catalogId,
                    )
                }
                assertUsage(
                    json.readTree(client.get("/v1/catalogs/footprint/storage-usage").bodyAsText()),
                    (large + 48).toString(),
                    "2",
                )
                for (query in listOf("snapshot=0", "at_timestamp=2026-01-01T00:00:00Z")) {
                    assertThat(client.get("/v1/catalogs/footprint/storage-usage?$query").status)
                        .isEqualTo(HttpStatusCode.BadRequest)
                }
            }
        }
    }

    @Test
    fun `empty is zero while unknown and backend failure are errors`() {
        PgTestSupport.freshDatabase().use { db ->
            CatalogService(db.jdbi).createCatalog("empty", "s3://empty")
            val app = App.build(Config(hydratorIntervalMs = 0, metricsIntervalMs = 0), db.jdbi)
            testApplication {
                application { app.module(this) }
                val empty = client.get("/v1/catalogs/empty/storage-usage")
                assertThat(empty.status).isEqualTo(HttpStatusCode.OK)
                assertUsage(json.readTree(empty.bodyAsText()), "0", "0")
                for (catalog in listOf("unknown", "invalid'name")) {
                    val missing = client.get("/v1/catalogs/$catalog/storage-usage")
                    assertThat(missing.status).isEqualTo(HttpStatusCode.NotFound)
                    assertThat(json.readTree(missing.bodyAsText()).has("tracked_bytes")).isFalse()
                }
                db.dataSource.close()
                val failed = client.get("/v1/catalogs/empty/storage-usage")
                assertThat(failed.status).isEqualTo(HttpStatusCode.InternalServerError)
                assertThat(json.readTree(failed.bodyAsText()).has("tracked_bytes")).isFalse()
            }
        }
    }

    @Test
    fun `int64 overflow fails rather than silently truncating bytes`() {
        PgTestSupport.freshDatabase().use { db ->
            val catalogs = CatalogService(db.jdbi)
            val catalog = catalogs.createCatalog("overflow", "s3://overflow")
            catalogs.createNamespace("overflow", "ns")
            val table = catalogs.createTable("overflow", "ns", "events", listOf(ColumnDef("id", ColType.LONG)))
            db.jdbi.useHandle<Exception> { h ->
                dataFile(h, catalog.catalogId, table.tableId, 1, Long.MAX_VALUE)
                dataFile(h, catalog.catalogId, table.tableId, 2, 1)
            }
            val app = App.build(Config(hydratorIntervalMs = 0, metricsIntervalMs = 0), db.jdbi)
            testApplication {
                application { app.module(this) }
                val failed = client.get("/v1/catalogs/overflow/storage-usage")
                assertThat(failed.status).isEqualTo(HttpStatusCode.InternalServerError)
                assertThat(json.readTree(failed.bodyAsText()).has("tracked_bytes")).isFalse()
            }
        }
    }

    private fun assertUsage(
        body: JsonNode,
        bytes: String,
        pending: String,
    ) {
        assertThat(body.fieldNames().asSequence().toList())
            .containsExactlyInAnyOrder("tracked_bytes", "pending_delete_files")
        assertThat(body["tracked_bytes"].isTextual).isTrue()
        assertThat(body["pending_delete_files"].isTextual).isTrue()
        assertThat(body["tracked_bytes"].asText()).isEqualTo(bytes)
        assertThat(body["pending_delete_files"].asText()).isEqualTo(pending)
    }

    private fun dataFile(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        fileId: Long,
        bytes: Long,
        end: Long? = null,
    ) {
        h.createUpdate(
            """
            INSERT INTO hog_data_file
                (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot,
                 path, record_count, file_size_bytes, row_id_start)
            VALUES (:catalog, :file, :table, 2, :end, :path, 2, :bytes, :row)
            """,
        ).bind("catalog", catalogId).bind("table", tableId).bind("file", fileId)
            .bind("end", end).bind("path", "s3://test/$catalogId/$fileId.parquet")
            .bind("bytes", bytes).bind("row", (fileId - 1) * 2).execute()
    }
}
