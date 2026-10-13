package com.posthog.hoglake.persistence

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.StorageUsage
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test

@Tag("integration")
class StorageUsageRepoIntegrationTest {
    @Test
    fun `exact aggregate on a hundred thousand row catalog has one result and no write lock`() {
        PgTestSupport.freshDatabase().use { db ->
            val catalogs = CatalogService(db.jdbi)
            val ids =
                listOf("measured", "other").map { name ->
                    val catalog = catalogs.createCatalog(name, "s3://$name")
                    catalogs.createNamespace(name, "ns")
                    val table = catalogs.createTable(name, "ns", "events", listOf(ColumnDef("id", ColType.LONG)))
                    db.jdbi.useHandle<Exception> { h ->
                        h.createUpdate(
                            """
                            INSERT INTO hog_data_file
                                (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot,
                                 path, record_count, file_size_bytes, row_id_start)
                            SELECT :catalog, i, :table, 2,
                                   CASE WHEN i % 100 = 0 THEN 3 ELSE NULL END,
                                   's3://test/' || :catalog || '/' || i,
                                   1, 60000, i - 1
                            FROM generate_series(1, 100000) i
                            """,
                        ).bind("catalog", catalog.catalogId).bind("table", table.tableId).execute()
                        h.createUpdate(
                            """
                            INSERT INTO hog_delete_file
                                (catalog_id, delete_file_id, table_id, data_file_id,
                                 begin_snapshot, path, delete_count, file_size_bytes)
                            SELECT :catalog, i, :table, i, 2,
                                   's3://test/delete/' || :catalog || '/' || i, 1, 1200
                            FROM generate_series(1, 10000) i
                            """,
                        ).bind("catalog", catalog.catalogId).bind("table", table.tableId).execute()
                        h.createUpdate(
                            """
                            INSERT INTO hog_file_removal
                                (catalog_id, path, file_kind, reason, drained_at, drained_outcome)
                            SELECT :catalog, 's3://test/removal/' || :catalog || '/' || i,
                                   'data', 'snapshot_expiry',
                                   CASE WHEN i % 2 = 0 THEN now() ELSE NULL END,
                                   CASE WHEN i % 2 = 0 THEN 'deleted' ELSE NULL END
                            FROM generate_series(1, 10000) i
                            """,
                        ).bind("catalog", catalog.catalogId).execute()
                    }
                    catalog.catalogId
                }
            db.jdbi.useHandle<Exception> { h ->
                h.execute("ANALYZE hog_data_file")
                h.execute("ANALYZE hog_delete_file")
                h.execute("ANALYZE hog_file_removal")
                assertThat(StorageUsageRepo.read(h, ids.first())).isEqualTo(StorageUsage(6_012_000_000L, 5000))
                val raw =
                    h.createQuery("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) ${StorageUsageRepo.SQL}")
                        .bind("catalogId", ids.first()).setQueryTimeout(StorageUsageRepo.QUERY_TIMEOUT_SECONDS)
                        .mapTo(String::class.java).one()
                val plan = ObjectMapper().readTree(raw)[0]
                assertThat(plan["Plan"]["Actual Rows"].asLong()).isEqualTo(1)
                // Assert the work, not one preferred index name: scanning a large
                // fraction of a catalog can legitimately favor a sequential scan.
                val leaves =
                    plan.findParents("Relation Name")
                        .filter { it["Relation Name"].asText() == "hog_data_file" }
                assertThat(leaves).isNotEmpty()
                assertThat(leaves.sumOf { it["Actual Rows"].asLong() * it["Actual Loops"].asLong() })
                    // EXPLAIN rounds rows/loop for parallel scan workers.
                    .isBetween(99_998L, 100_002L)
                val examined =
                    leaves.sumOf {
                        (it["Actual Rows"].asLong() + (it["Rows Removed by Filter"]?.asLong() ?: 0)) *
                            it["Actual Loops"].asLong()
                    }
                assertThat(examined).isBetween(99_998L, 200_002L)
                val buffers =
                    leaves.sumOf {
                        (it["Shared Hit Blocks"]?.asLong() ?: 0) + (it["Shared Read Blocks"]?.asLong() ?: 0)
                    }
                assertThat(plan.toString()).doesNotContain("LockRows", "ModifyTable")
                println(
                    "StorageUsage 100000/200000 rows: ${plan["Execution Time"]} ms; " +
                        "examined=$examined scan_buffers=$buffers; plan=${plan["Plan"]}",
                )
            }
        }
    }
}
