package com.posthog.hoglake.observability

import com.fasterxml.jackson.databind.JsonNode
import com.posthog.hoglake.Database
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.wireObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

/** The migration runs against existing history before the production status query uses its index. */
@Tag("integration")
class V33MaintenanceStatusMigrationIntegrationTest {
    @Test
    fun `migration bounds persisted status seeks through 100000 newer failures and repairs invalid retries`() {
        PgTestSupport.freshDatabaseAt("32", productionSession = true).use { db ->
            db.jdbi.useHandleUnchecked { h ->
                h.execute("INSERT INTO hog_catalog(name,data_path) VALUES ('ledger','s3://ledger')")
                h.execute(
                    """
                    INSERT INTO hog_maintenance_run(catalog_id,task,run_trigger,started_at,finished_at,status,error)
                    SELECT c.catalog_id,'expiry','loop',now() - interval '1 second',now(),
                           CASE WHEN n=0 THEN 'ok' ELSE 'failed' END,
                           CASE WHEN n=0 THEN NULL ELSE 'fixture' END
                      FROM hog_catalog c CROSS JOIN generate_series(0,100000) n ORDER BY n
                    """,
                )
                h.execute("VACUUM (ANALYZE) hog_maintenance_run")
            }

            fun explain(): JsonNode =
                db.jdbi.withHandleUnchecked { h ->
                    val query =
                        h.createQuery(
                            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " +
                                PostgresCatalogMetricSource.MIXED_MAINTENANCE_RUN_SQL,
                        ).bind("catalogId", 1L).bind("task", "expiry")
                    wireObjectMapper().readTree(query.mapTo(String::class.java).one())[0]
                }
            val before = explain()
            val began = System.nanoTime()
            Database.migrate(db.dataSource)
            val buildMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - began)
            val after = explain()

            fun nodes(node: JsonNode): List<JsonNode> = listOf(node) + (node["Plans"]?.flatMap(::nodes) ?: emptyList())
            val seeks = nodes(after["Plan"]).filter { it.path("Index Name").asText() == "hog_maintenance_run_status" }
            assertThat(seeks).isNotEmpty()
            val ledgerNodes = nodes(after["Plan"]).filter { it.path("Relation Name").asText() == "hog_maintenance_run" }
            assertThat(ledgerNodes.none { it.path("Node Type").asText() == "Seq Scan" }).isTrue()
            assertThat(ledgerNodes.sumOf { it.path("Rows Removed by Filter").asLong() }).isZero()
            assertThat(seeks.sumOf { it.path("Rows Removed by Filter").asLong() }).isZero()
            assertThat(seeks.maxOf { it.path("Actual Rows").asLong() }).isLessThanOrEqualTo(1)
            val oldFiltered = nodes(before["Plan"]).sumOf { it.path("Rows Removed by Filter").asLong() }
            assertThat(oldFiltered).isGreaterThanOrEqualTo(100000)
            val bytes =
                db.jdbi.withHandleUnchecked { h ->
                    val keys =
                        h.createQuery(
                            """
                    SELECT a.attname FROM pg_index i
                    CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum,n)
                    JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=k.attnum
                    WHERE i.indexrelid='hog_maintenance_run_status'::regclass ORDER BY k.n
                    """,
                        ).mapTo(String::class.java).list()
                    assertThat(keys).containsExactly("catalog_id", "task", "status", "run_id")
                    h.createQuery(
                        "SELECT pg_relation_size('hog_maintenance_run_status')",
                    ).mapTo(Long::class.javaObjectType).one()
                }
            println("V33 rows=100001 build_ms=$buildMs index_bytes=$bytes old_filtered=$oldFiltered seeks=$seeks")
            db.jdbi.useHandleUnchecked { h ->
                h.execute(
                    "UPDATE pg_index SET indisvalid=false WHERE indexrelid='hog_maintenance_run_status'::regclass",
                )
                h.execute("DELETE FROM flyway_schema_history WHERE version='33'")
            }
            Database.migrate(db.dataSource)
            assertThat(
                db.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        "SELECT indisvalid FROM pg_index WHERE indexrelid='hog_maintenance_run_status'::regclass",
                    )
                        .mapTo(Boolean::class.javaObjectType).one()
                },
            ).isTrue()
        }
    }
}
