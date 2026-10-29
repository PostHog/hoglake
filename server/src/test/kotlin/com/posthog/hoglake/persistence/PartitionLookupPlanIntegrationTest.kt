package com.posthog.hoglake.persistence

import com.fasterxml.jackson.databind.JsonNode
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
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

/** Interleaved catalog/table identities, skewed pivots, retained history, and unused wide metadata. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PartitionLookupPlanIntegrationTest {
    private val db = PgTestSupport.freshDatabase(productionSession = true)
    private val f = PartitionLookupFixture(db)
    private val neighbour = PartitionLookupFixture(db)
    private var otherTable = 0L
    private val json = wireObjectMapper()
    private val explain = "EXPLAIN (ANALYZE, BUFFERS, VERBOSE, FORMAT JSON) "

    @AfterAll
    fun close() = db.close()

    @BeforeAll
    fun seed() {
        f.catalogs.createTable(
            f.name,
            "ns",
            "other",
            listOf(ColumnDef("a", ColType.STRING), ColumnDef("b", ColType.STRING)),
        )
        otherTable =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT table_id FROM hog_table_version WHERE catalog_id = ? AND name = 'other'")
                    .bind(0, f.catalogId).mapTo(Long::class.javaObjectType).one()
            }
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot, end_snapshot,
                    path, record_count, file_size_bytes, row_id_start, spec_id, split_offsets)
                SELECT CASE WHEN g % 3 = 0 THEN :neighbour ELSE :catalog END, g,
                    CASE WHEN g % 3 = 0 THEN :nt WHEN g % 2 = 0 THEN :table ELSE :other END,
                    0, CASE WHEN g % 11 = 0 THEN 2 END,
                    's3://fixture/' || md5(g::text) || '/' || g, 10, 100, g * 10, :spec,
                    CASE WHEN g % 997 = 0 THEN ARRAY(SELECT generate_series(0::bigint, 9999::bigint)) END
                FROM generate_series(1, 120000) g
                """,
            ).bind("neighbour", neighbour.catalogId).bind("catalog", f.catalogId)
                .bind(
                    "nt",
                    neighbour.tableId,
                ).bind("table", f.tableId).bind("other", otherTable).bind("spec", f.specId).execute()
            h.execute(
                """
                INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value)
                SELECT catalog_id, data_file_id, 0,
                    CASE WHEN data_file_id % 7 = 0 THEN NULL WHEN data_file_id % 997 = 0 THEN 'rare' ELSE 'common' END
                FROM hog_data_file
                UNION ALL
                SELECT catalog_id, data_file_id, 1, CASE WHEN data_file_id % 17 = 0 THEN 'wanted' ELSE 'other' END
                FROM hog_data_file
                """,
            )
            h.execute(
                """
                INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id,
                    begin_snapshot, end_snapshot, path, delete_count, file_size_bytes)
                SELECT f.catalog_id, f.data_file_id * 3 + v, f.table_id, f.data_file_id,
                    v - 1, CASE WHEN v < 3 THEN v END, 's3://fixture/dv/' || f.data_file_id || '/' || v, 1, 1
                FROM hog_data_file f CROSS JOIN generate_series(1, 3) v WHERE f.data_file_id % 41 = 0
                """,
            )
            for (relation in listOf(
                "hog_data_file",
                "hog_file_partition_value",
                "hog_delete_file",
            )) h.execute("VACUUM (ANALYZE) $relation")
        }
    }

    private fun nodes(node: JsonNode): List<JsonNode> =
        listOf(node) + (node["Plans"]?.flatMap { nodes(it) } ?: emptyList())

    private fun explain(
        sql: String,
        values: List<String?>,
        cap: Int,
        generic: Boolean,
        page: Boolean,
        after: Long = -1,
    ): JsonNode =
        db.jdbi.withHandleUnchecked { h ->
            h.begin()
            try {
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                val text =
                    if (generic) {
                        h.execute("SET LOCAL plan_cache_mode = force_generic_plan")
                        val params =
                            linkedMapOf(
                                "catalogId" to ("bigint" to f.catalogId.toString()),
                                "tableId" to ("bigint" to f.tableId.toString()),
                                "specId" to ("bigint" to f.specId.toString()),
                                "snapshot" to ("bigint" to db.head(f.name).toString()),
                                "upper" to ("bigint" to "120001"),
                                "after" to ("bigint" to after.toString()),
                                "candidateLimit" to ("integer" to cap.toString()),
                                "candidateProbeLimit" to ("integer" to (cap + 1).toString()),
                                "arity" to ("integer" to values.size.toString()),
                            )
                        if (page) params["pageLimit"] = "integer" to "101"
                        values.forEachIndexed {
                                i,
                                value,
                            ->
                            if (value != null) params["value$i"] = "text" to "'$value'"
                        }
                        val positional =
                            params.keys.foldIndexed(
                                sql,
                            ) { i, text, name -> text.replace(Regex(":$name\\b"), "\\$${i + 1}") }
                        h.execute("PREPARE lookup_plan(${params.values.joinToString(",") { it.first }}) AS $positional")
                        try {
                            h.createQuery(
                                "${explain}EXECUTE lookup_plan(${params.values.joinToString(",") { it.second }})",
                            ).mapTo(String::class.java).one()
                        } finally {
                            h.execute("DEALLOCATE lookup_plan")
                        }
                    } else {
                        val q =
                            PartitionLookupRepo.bind(
                                h.createQuery(explain + sql),
                                f.catalogId,
                                f.tableId,
                                f.specId,
                                values,
                                db.head(f.name),
                                120001,
                                after,
                                cap,
                            )
                        if (page) q.bind("pageLimit", 101)
                        q.mapTo(String::class.java).one()
                    }
                json.readTree(text)[0]
            } finally {
                h.rollback()
            }
        }

    @Test
    fun `custom and generic plans bound index and manifest work before materialization`() {
        for (values in listOf(listOf("common", "wanted"), listOf(null, "wanted"), listOf("rare", "wanted"))) {
            for (cap in listOf(100, 1000)) {
                for (page in listOf(false, true)) {
                    for (generic in listOf(false, true)) {
                        for (after in if (page) listOf(-1L, 100_000L) else listOf(-1L)) {
                            val sql =
                                if (page) {
                                    PartitionLookupRepo.pageSql(
                                        values,
                                    )
                                } else {
                                    PartitionLookupRepo.aggregateSql(values)
                                }
                            val result = explain(sql, values, cap, generic, page, after)
                            val all = nodes(result["Plan"])
                            val candidates = all.single { it.path("Subplan Name").asText() == "CTE candidates" }
                            val pivot =
                                nodes(candidates).single {
                                    it.path("Relation Name").asText() == "hog_file_partition_value"
                                }
                            assertThat(pivot["Node Type"].asText()).isIn("Index Only Scan", "Index Scan")
                            assertThat(pivot.path("Index Name").asText()).isEqualTo("hog_file_partition_value_lookup")
                            assertThat(pivot.path("Index Cond").asText())
                                .contains("key_index = 0", "data_file_id >", "data_file_id <")
                            val pivotExamined =
                                (pivot["Actual Rows"].asDouble() + pivot.path("Rows Removed by Filter").asDouble()) *
                                    pivot["Actual Loops"].asDouble()
                            assertThat(pivotExamined).isLessThanOrEqualTo(cap + 1.0)
                            for (node in all.filter {
                                it["Relation Name"]?.asText() in
                                    listOf("hog_data_file", "hog_file_partition_value", "hog_delete_file")
                            }) {
                                assertThat(node["Node Type"].asText()).doesNotContain("Seq Scan")
                                val examined =
                                    (node["Actual Rows"].asDouble() + node.path("Rows Removed by Filter").asDouble()) *
                                        node["Actual Loops"].asDouble()
                                assertThat(
                                    examined,
                                ).describedAs("bounded relation work %s", node).isLessThanOrEqualTo((cap + 1) * 4.0)
                            }
                            val root = result["Plan"]
                            val buffers =
                                root.path("Shared Hit Blocks").asLong() + root.path("Shared Read Blocks").asLong()
                            assertThat(buffers).isLessThan(40L * cap + 2000)
                            val materialized = all.filter { it.path("Subplan Name").asText() == "CTE matched" }
                            assertThat(materialized).isNotEmpty()
                            for (node in materialized + listOf(root)) {
                                assertThat(
                                    node.path("Output").toString(),
                                ).doesNotContain("split_offsets", "missing_field_ids")
                            }
                            println(
                                "partition lookup plan pivot=${values[0]} cap=$cap page=$page generic=$generic " +
                                    "after=$after buffers=$buffers elapsed_ms=${result["Execution Time"]}",
                            )
                        }
                    }
                }
            }
        }
    }
}
