package com.posthog.hoglake.persistence

import com.fasterxml.jackson.databind.JsonNode
import com.posthog.hoglake.Database
import com.posthog.hoglake.testing.PartitionLookupFixture
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.wireObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.File
import java.util.concurrent.TimeUnit

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class V29DeleteFileSnapshotLookupMigrationIntegrationTest {
    private val db = PgTestSupport.freshDatabaseAt("28", productionSession = true)
    private val f = PartitionLookupFixture(db)
    private val neighbour = PartitionLookupFixture(db)
    private val tuple = listOf("a", "b")
    private lateinit var before: JsonNode
    private lateinit var after: JsonNode
    private val index = "hog_delete_file_snapshot_lookup"

    @AfterAll
    fun close() = db.close()

    @BeforeAll
    fun seedThenMigrate() {
        f.append(tuple)
        neighbour.append(tuple)
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                """
                INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id,
                    begin_snapshot, end_snapshot, path, delete_count, file_size_bytes)
                SELECT f.catalog_id, g, f.table_id, f.data_file_id,
                       g, g + 1,
                       's3://fixture/dv/' || f.catalog_id || '/' || g, 1, 64
                FROM generate_series(1, 100000) g CROSS JOIN hog_data_file f
                ORDER BY g, f.catalog_id
                """,
            )
            h.execute("VACUUM (ANALYZE) hog_delete_file")
        }
        before = explain()
        val started = System.nanoTime()
        Database.migrate(db.dataSource)
        val elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
        after = explain()
        val bytes =
            db.jdbi.withHandleUnchecked {
                    h ->
                h.createQuery("SELECT pg_relation_size('$index')").mapTo(Long::class.javaObjectType).one()
            }
        println("V29 build_ms=$elapsed index_bytes=$bytes bytes_per_version=${bytes / 200000.0}")
        for ((label, plan) in listOf("before" to before, "after" to after)) {
            println(
                "V29 $label buffers=${buffers(plan)} elapsed_ms=${plan["Execution Time"]} dv_nodes=${dvNodes(plan)}",
            )
        }
    }

    private fun explain(snapshot: Long = 50000): JsonNode =
        db.jdbi.withHandleUnchecked { h ->
            h.execute("SET max_parallel_workers_per_gather = 0")
            val query =
                PartitionLookupRepo.bind(
                    query =
                        h.createQuery(
                            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + PartitionLookupRepo.aggregateSql(tuple),
                        ),
                    catalogId = f.catalogId,
                    tableId = f.tableId,
                    specId = f.specId,
                    values = tuple,
                    snapshot = snapshot,
                    upper = 2,
                    after = -1,
                    candidateLimit = 100,
                )
            wireObjectMapper().readTree(query.mapTo(String::class.java).one())[0]
        }

    private fun nodes(node: JsonNode): List<JsonNode> =
        listOf(node) + (node["Plans"]?.flatMap { nodes(it) } ?: emptyList())

    private fun dvNodes(plan: JsonNode) =
        nodes(plan["Plan"]).filter {
            it.path("Relation Name").asText() == "hog_delete_file"
        }

    private fun examined(node: JsonNode): Double =
        (node.path("Actual Rows").asDouble() + node.path("Rows Removed by Filter").asDouble()) *
            node.path("Actual Loops").asDouble()

    private fun buffers(plan: JsonNode): Long =
        plan["Plan"].path("Shared Hit Blocks").asLong() + plan["Plan"].path("Shared Read Blocks").asLong()

    @Test
    fun `migration bounds production aggregate DV seek with interleaved retained history`() {
        assertThat(dvNodes(before).sumOf { examined(it) }).isGreaterThanOrEqualTo(50000.0)
        assertThat(dvNodes(after)).hasSize(1)
        val seek = dvNodes(after).single()
        assertThat(seek["Index Name"].asText()).isEqualTo(index)
        assertThat(examined(seek)).isEqualTo(1.0)
        assertThat(buffers(after)).isLessThan(100)
        assertThat(buffers(before)).isGreaterThan(buffers(after) * 10)
        val totals =
            db.jdbi.withHandleUnchecked { h ->
                PartitionLookupRepo.aggregates(h, f.catalogId, f.tableId, f.specId, tuple, 50000, 2, 100)
            }.totals!!
        assertThat(totals.dvCount).isEqualTo(1)
        assertThat(totals.physicalRows).isEqualTo(10)
        val keys =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT a.attname FROM pg_index i
                    CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY k(attnum, n)
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                    WHERE i.indexrelid = '$index'::regclass ORDER BY k.n
                    """,
                ).mapTo(String::class.java).list()
            }
        assertThat(keys).containsExactly("catalog_id", "data_file_id", "begin_snapshot", "delete_file_id")
    }

    @Test
    fun `ended history gap still examines only the latest version`() {
        val plan = explain(100001)
        assertThat(dvNodes(plan).sumOf { examined(it) }).isEqualTo(1.0)
        assertThat(buffers(plan)).isLessThan(100)
        val result =
            db.jdbi.withHandleUnchecked { h ->
                PartitionLookupRepo.aggregates(h, f.catalogId, f.tableId, f.specId, tuple, 100001, 2, 100)
            }
        assertThat(result.totals!!.dvCount).isZero()
    }

    @Test
    fun `migration repairs invalid concurrent build and preserves session bounds`() {
        val bounds =
            db.jdbi.withHandleUnchecked {
                    h ->
                h.createQuery("SELECT current_setting('statement_timeout'), current_setting('lock_timeout')").map {
                        rs,
                        _,
                    ->
                    rs.getString(1) to rs.getString(2)
                }.one()
            }
        db.jdbi.useHandleUnchecked { h ->
            h.execute("UPDATE pg_index SET indisvalid = false WHERE indexrelid = '$index'::regclass")
            h.execute("DELETE FROM flyway_schema_history WHERE version = '29'")
        }
        Database.migrate(db.dataSource)
        db.jdbi.useHandleUnchecked { h ->
            assertThat(
                h.createQuery(
                    "SELECT indisvalid FROM pg_index WHERE indexrelid = '$index'::regclass",
                ).mapTo(Boolean::class.javaObjectType).one(),
            ).isTrue()
            assertThat(
                h.createQuery("SELECT current_setting('statement_timeout'), current_setting('lock_timeout')").map {
                        rs,
                        _,
                    ->
                    rs.getString(1) to rs.getString(2)
                }.one(),
            ).isEqualTo(bounds)
        }
        assertThat(
            File("src/main/resources/db/migration/V29__delete_file_snapshot_lookup.sql.conf").readText(),
        ).contains("executeInTransaction=false")
    }
}
