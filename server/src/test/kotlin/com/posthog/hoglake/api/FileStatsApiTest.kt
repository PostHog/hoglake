package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.stats.IcebergSingleValue
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.math.BigDecimal
import java.math.BigInteger
import java.util.Base64

/**
 * GET .../tables/{table}/files/{fileId}/stats over the wire: per-column
 * statistics for one data file with DECODED bounds, rendered per the
 * JSON conventions the OpenAPI spec documents (BoundWire). Everything
 * asserts raw JSON, because the decode surface exists so callers stop
 * shipping their own bounds codec — the wire form IS the feature.
 *
 * The semantic corners each get a test:
 *  - a file without stats rows (stats_state pending/failed) answers an
 *    explicit empty-with-reason shape, and absent bounds mean the
 *    caller must not prune;
 *  - an all-null column's bounds are stored NULL and render as JSON
 *    null — a real answer, distinct from the no-stats shape;
 *  - variant columns have no stats at any depth and simply do not
 *    appear (nothing fabricated, nothing erroring);
 *  - nested leaves appear by field id under their dotted path,
 *    including the synthetic element/key/value names; containers have
 *    no rows;
 *  - a stored bound the live type cannot decode renders as JSON null
 *    (the "NULL, never guessed" read-side dual), never a 500.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FileStatsApiTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()

    /** Reader that keeps decimal tokens exact instead of routing them through a double. */
    private val bigDecimalJson =
        ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)

    private val catalog = "filestats"
    private val tablesUrl = "/v1/catalogs/$catalog/namespaces/ns/tables"

    @BeforeAll
    fun seed() =
        api { client ->
            client.postJson("/v1/catalogs", """{"name": "$catalog", "data_path": "s3://b/$catalog"}""")
            client.postJson("/v1/catalogs/$catalog/namespaces", """{"name": "ns"}""")
        }

    @AfterAll
    fun tearDown() = db.close()

    private fun api(block: suspend ApplicationTestBuilder.(HttpClient) -> Unit) =
        testApplication {
            application { app.module(this) }
            block(client)
        }

    private suspend fun HttpClient.postJson(
        url: String,
        body: String,
    ): HttpResponse =
        post(url) {
            contentType(ContentType.Application.Json)
            setBody(body)
        }

    private suspend fun body(response: HttpResponse): JsonNode = json.readTree(response.bodyAsText())

    private fun b64(bytes: ByteArray): String = Base64.getEncoder().encodeToString(bytes)

    private fun enc(
        type: ColType,
        value: Any,
    ): String = b64(IcebergSingleValue.encode(type, value))

    /** Create a table, returning name -> field_id over EVERY node (nested included). */
    private suspend fun createTable(
        client: HttpClient,
        name: String,
        columnsJson: String,
    ): Map<String, Long> {
        val response = client.postJson(tablesUrl, """{"name": "$name", "columns": [$columnsJson]}""")
        assertThat(response.status).describedAs(bodyText(response)).isEqualTo(HttpStatusCode.Created)
        val ids = mutableMapOf<String, Long>()

        fun walk(
            node: JsonNode,
            prefix: String,
        ) {
            for (col in node) {
                val path = if (prefix.isEmpty()) col["name"].asText() else "$prefix.${col["name"].asText()}"
                ids[path] = col["field_id"].asLong()
                col["children"]?.let { walk(it, path) }
            }
        }
        walk(body(response)["columns"], "")
        return ids
    }

    private suspend fun bodyText(response: HttpResponse): String = response.bodyAsText()

    /** Commit one file; returns nothing (files are read back over the wire). */
    private suspend fun commitFile(
        client: HttpClient,
        table: String,
        path: String,
        recordCount: Long,
        statsJson: String?,
    ) {
        val stats = statsJson?.let { ""","column_stats": [$it]""" } ?: ""
        val response =
            client.postJson(
                "/v1/catalogs/$catalog/commit",
                """{"appends": [{"namespace": "ns", "table": "$table", "files": [
                   {"path": "$path", "record_count": $recordCount, "file_size_bytes": 1024$stats}]}]}""",
            )
        assertThat(response.status).describedAs(bodyText(response)).isEqualTo(HttpStatusCode.OK)
    }

    private suspend fun fileId(
        client: HttpClient,
        table: String,
        path: String,
    ): Long =
        body(client.get("$tablesUrl/$table/files"))
            .first { it["path"].asText() == path }["data_file_id"].asLong()

    private fun statsRow(
        fieldId: Long,
        valueCount: Long,
        nullCount: Long,
        lower: String?,
        upper: String?,
        extra: String = "",
    ): String {
        val bounds =
            listOfNotNull(
                lower?.let { """"lower_bound": "$it"""" },
                upper?.let { """"upper_bound": "$it"""" },
            ).joinToString(",")
        val tail = listOf(bounds, extra).filter { it.isNotEmpty() }.joinToString(",")
        val suffix = if (tail.isEmpty()) "" else ",$tail"
        return """{"field_id": $fieldId, "value_count": $valueCount, "null_count": $nullCount$suffix}"""
    }

    private fun JsonNode.column(fieldId: Long): JsonNode = this["columns"].first { it["field_id"].asLong() == fieldId }

    // ---- happy paths per type family ---------------------------------------

    @Test
    fun `decoded bounds per type family, exact on the wire`() =
        api { client ->
            val ids =
                createTable(
                    client,
                    "typed",
                    """{"name": "c_long", "type": "long"},
                       {"name": "c_double", "type": "double"},
                       {"name": "c_string", "type": "string"},
                       {"name": "c_decimal", "type": "decimal",
                        "type_params": {"precision": 10, "scale": 2}},
                       {"name": "c_tstz", "type": "timestamptz"},
                       {"name": "c_uint64", "type": "uint64"},
                       {"name": "c_date", "type": "date"},
                       {"name": "c_bool", "type": "boolean"},
                       {"name": "c_bin", "type": "binary"}""",
                )
            val path = "s3://b/$catalog/typed/f1.parquet"
            val rows =
                listOf(
                    // 2^53 + 1: the first long a double silently rounds.
                    statsRow(
                        ids.getValue("c_long"),
                        10,
                        0,
                        enc(ColType.LONG, 9007199254740993L),
                        enc(ColType.LONG, 9223372036854775807L),
                    ),
                    statsRow(
                        ids.getValue("c_double"),
                        10,
                        0,
                        enc(ColType.DOUBLE, -0.0),
                        enc(ColType.DOUBLE, 0.1),
                        extra = """"nan_count": 2, "size_bytes": 80""",
                    ),
                    statsRow(
                        ids.getValue("c_string"),
                        10,
                        1,
                        enc(ColType.STRING, "aardvark"),
                        enc(ColType.STRING, "🦔"),
                    ),
                    statsRow(
                        ids.getValue("c_decimal"),
                        10,
                        0,
                        enc(ColType.DECIMAL, BigInteger("150")),
                        enc(ColType.DECIMAL, BigInteger("99999")),
                    ),
                    statsRow(
                        ids.getValue("c_tstz"),
                        10,
                        0,
                        enc(ColType.TIMESTAMPTZ, 0L),
                        enc(ColType.TIMESTAMPTZ, 1_788_609_600_000_000L),
                    ),
                    statsRow(
                        ids.getValue("c_uint64"),
                        10,
                        0,
                        enc(ColType.UINT64, BigInteger.ZERO),
                        enc(ColType.UINT64, BigInteger("18446744073709551615")),
                    ),
                    statsRow(
                        ids.getValue("c_date"),
                        10,
                        0,
                        enc(ColType.DATE, 0),
                        enc(ColType.DATE, 20701),
                    ),
                    statsRow(
                        ids.getValue("c_bool"),
                        10,
                        0,
                        enc(ColType.BOOLEAN, false),
                        enc(ColType.BOOLEAN, true),
                    ),
                    statsRow(
                        ids.getValue("c_bin"),
                        10,
                        0,
                        enc(ColType.BINARY, byteArrayOf(0, 1, 2)),
                        enc(ColType.BINARY, byteArrayOf(-1)),
                    ),
                ).joinToString(",")
            commitFile(client, "typed", path, 10, rows)
            val id = fileId(client, "typed", path)

            val response = client.get("$tablesUrl/typed/files/$id/stats")
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            val raw = response.bodyAsText()
            val node = bigDecimalJson.readTree(raw)

            assertThat(node["data_file_id"].asLong()).isEqualTo(id)
            assertThat(node["stats_state"].asText()).isEqualTo("provided")
            assertThat(node.has("no_stats_reason")).isFalse()
            assertThat(node["columns"]).hasSize(9)

            val long = node.column(ids.getValue("c_long"))
            assertThat(long["name"].asText()).isEqualTo("c_long")
            assertThat(long["path"].asText()).isEqualTo("c_long")
            assertThat(long["type"].asText()).isEqualTo("long")
            assertThat(long["value_count"].asLong()).isEqualTo(10)
            assertThat(long["lower_bound"].isIntegralNumber).isTrue()
            assertThat(long["lower_bound"].asLong()).isEqualTo(9007199254740993L)
            // The raw token, digit for digit — proof the value never
            // routed through a double on the way out.
            assertThat(raw).contains("9007199254740993")
            assertThat(raw).doesNotContain("9007199254740992")

            val double = node.column(ids.getValue("c_double"))
            // -0.0 is the stored lower-zero (normalization by role);
            // rendered sign intact. Asserted on the RAW token — a
            // BigDecimal-parsing reader erases the sign of zero.
            assertThat(raw).contains("\"lower_bound\":-0.0")
            assertThat(double["upper_bound"].decimalValue()).isEqualByComparingTo(BigDecimal("0.1"))
            assertThat(double["nan_count"].asLong()).isEqualTo(2)
            assertThat(double["size_bytes"].asLong()).isEqualTo(80)

            val string = node.column(ids.getValue("c_string"))
            assertThat(string["lower_bound"].asText()).isEqualTo("aardvark")
            assertThat(string["upper_bound"].asText()).isEqualTo("🦔")

            val decimal = node.column(ids.getValue("c_decimal"))
            assertThat(decimal["type"].asText()).isEqualTo("decimal")
            assertThat(decimal["type_params"]["scale"].asInt()).isEqualTo(2)
            // Value numerically (the JsonNode READER strips trailing
            // BigDecimal zeros); the exact-scale token is pinned on the
            // raw text below.
            assertThat(decimal["lower_bound"].decimalValue()).isEqualByComparingTo(BigDecimal("1.50"))
            assertThat(decimal["upper_bound"].decimalValue()).isEqualByComparingTo(BigDecimal("999.99"))
            assertThat(raw).contains("\"lower_bound\":1.50")

            val tstz = node.column(ids.getValue("c_tstz"))
            assertThat(tstz["lower_bound"].asText()).isEqualTo("1970-01-01T00:00:00Z")
            assertThat(tstz["upper_bound"].asText()).isEqualTo("2026-09-05T12:00:00Z")

            val uint64 = node.column(ids.getValue("c_uint64"))
            assertThat(uint64["upper_bound"].bigIntegerValue())
                .isEqualTo(BigInteger("18446744073709551615"))
            assertThat(raw).contains("18446744073709551615")

            val date = node.column(ids.getValue("c_date"))
            assertThat(date["lower_bound"].asText()).isEqualTo("1970-01-01")
            assertThat(date["upper_bound"].asText()).isEqualTo("2026-09-05")

            val bool = node.column(ids.getValue("c_bool"))
            assertThat(bool["lower_bound"].isBoolean).isTrue()
            assertThat(bool["lower_bound"].asBoolean()).isFalse()
            assertThat(bool["upper_bound"].asBoolean()).isTrue()

            val bin = node.column(ids.getValue("c_bin"))
            assertThat(bin["lower_bound"].asText()).isEqualTo("AAEC")
        }

    // ---- no stats: explicit empty-with-reason ------------------------------

    @Test
    fun `a deferred-stats file answers empty with a reason, not fabricated rows`() =
        api { client ->
            createTable(client, "deferred", """{"name": "id", "type": "long"}""")
            val path = "s3://b/$catalog/deferred/f1.parquet"
            commitFile(client, "deferred", path, 5, statsJson = null)
            val id = fileId(client, "deferred", path)

            val response = client.get("$tablesUrl/deferred/files/$id/stats")
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            val node = body(response)
            assertThat(node["stats_state"].asText()).isEqualTo("pending")
            assertThat(node["columns"]).isEmpty()
            // The reason is part of the contract: absent bounds mean the
            // caller MUST NOT prune, and the shape says so out loud.
            assertThat(node["no_stats_reason"].asText()).contains("must not prune")
            assertThat(node["no_stats_reason"].asText()).contains("pending")
        }

    // ---- all-null column: JSON null bounds, not "no stats" -----------------

    @Test
    fun `an all-null column renders explicit JSON null bounds`() =
        api { client ->
            val ids = createTable(client, "nulls", """{"name": "c", "type": "long"}""")
            val path = "s3://b/$catalog/nulls/f1.parquet"
            commitFile(
                client,
                "nulls",
                path,
                5,
                statsRow(ids.getValue("c"), 5, 5, lower = null, upper = null),
            )
            val id = fileId(client, "nulls", path)

            val response = client.get("$tablesUrl/nulls/files/$id/stats")
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            val raw = response.bodyAsText()
            val node = json.readTree(raw)
            assertThat(node["stats_state"].asText()).isEqualTo("provided")
            val c = node.column(ids.getValue("c"))
            assertThat(c["null_count"].asLong()).isEqualTo(5)
            // EXPLICIT null on the wire — a stats row exists and says
            // "nothing to bound", which is a different answer from the
            // no-stats shape above.
            assertThat(c["lower_bound"].isNull).isTrue()
            assertThat(c["upper_bound"].isNull).isTrue()
            assertThat(raw).contains("\"lower_bound\":null")
        }

    // ---- variant: reflected, never fabricated, never an error --------------

    @Test
    fun `a variant column has no stats at any depth and does not error`() =
        api { client ->
            val ids =
                createTable(
                    client,
                    "varianted",
                    """{"name": "id", "type": "long"}, {"name": "v", "type": "variant"}""",
                )
            val path = "s3://b/$catalog/varianted/f1.parquet"
            commitFile(
                client,
                "varianted",
                path,
                3,
                statsRow(ids.getValue("id"), 3, 0, enc(ColType.LONG, 1L), enc(ColType.LONG, 3L)),
            )
            val id = fileId(client, "varianted", path)

            val response = client.get("$tablesUrl/varianted/files/$id/stats")
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            val node = body(response)
            // Exactly what the catalog has: one row for id, nothing for v.
            assertThat(node["columns"]).hasSize(1)
            assertThat(node["columns"][0]["field_id"].asLong()).isEqualTo(ids.getValue("id"))
        }

    // ---- nested leaves by field id, containers absent ----------------------

    @Test
    fun `nested leaves appear under dotted paths with synthetic names`() =
        api { client ->
            val ids =
                createTable(
                    client,
                    "nested",
                    """{"name": "s", "type": "struct", "children": [{"name": "a", "type": "long"}]},
                       {"name": "l", "type": "list",
                        "children": [{"name": "element", "type": "int"}]},
                       {"name": "m", "type": "map", "children": [
                          {"name": "key", "type": "string", "nullable": false},
                          {"name": "value", "type": "double"}]}""",
                )
            val path = "s3://b/$catalog/nested/f1.parquet"
            val rows =
                listOf(
                    statsRow(ids.getValue("s.a"), 4, 0, enc(ColType.LONG, 1L), enc(ColType.LONG, 9L)),
                    statsRow(ids.getValue("l.element"), 12, 0, enc(ColType.INT, -5), enc(ColType.INT, 5)),
                    statsRow(
                        ids.getValue("m.key"),
                        6,
                        0,
                        enc(ColType.STRING, "a"),
                        enc(ColType.STRING, "z"),
                    ),
                    statsRow(
                        ids.getValue("m.value"),
                        6,
                        2,
                        enc(ColType.DOUBLE, 1.5),
                        enc(ColType.DOUBLE, 2.5),
                    ),
                ).joinToString(",")
            commitFile(client, "nested", path, 4, rows)
            val id = fileId(client, "nested", path)

            val response = client.get("$tablesUrl/nested/files/$id/stats")
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            val node = body(response)
            assertThat(node["columns"]).hasSize(4)

            val leafA = node.column(ids.getValue("s.a"))
            assertThat(leafA["name"].asText()).isEqualTo("a")
            assertThat(leafA["path"].asText()).isEqualTo("s.a")

            val element = node.column(ids.getValue("l.element"))
            assertThat(element["name"].asText()).isEqualTo("element")
            assertThat(element["path"].asText()).isEqualTo("l.element")
            assertThat(element["lower_bound"].asInt()).isEqualTo(-5)

            assertThat(node.column(ids.getValue("m.key"))["path"].asText()).isEqualTo("m.key")
            assertThat(node.column(ids.getValue("m.value"))["path"].asText()).isEqualTo("m.value")

            // The containers themselves never carry rows.
            val returnedIds = node["columns"].map { it["field_id"].asLong() }
            assertThat(returnedIds).doesNotContain(ids.getValue("s"), ids.getValue("l"), ids.getValue("m"))
        }

    // ---- undecodable stored bound: null, never a 500 -----------------------

    @Test
    fun `a stored bound the live type cannot decode renders as null`() =
        api { client ->
            val ids = createTable(client, "poisoned", """{"name": "c", "type": "long"}""")
            val path = "s3://b/$catalog/poisoned/f1.parquet"
            commitFile(
                client,
                "poisoned",
                path,
                3,
                statsRow(ids.getValue("c"), 3, 0, enc(ColType.LONG, 1L), enc(ColType.LONG, 3L)),
            )
            val id = fileId(client, "poisoned", path)
            // Corrupt the stored bound behind the commit door's back: a
            // 4-byte encoding under a long column (the stale-width shape a
            // pre-promote writer could leave).
            db.jdbi.withHandleUnchecked { h ->
                h.createUpdate(
                    "UPDATE hog_file_column_stats SET lower_bound = :b WHERE data_file_id = :f",
                )
                    .bind("b", byteArrayOf(1, 0, 0, 0))
                    .bind("f", id)
                    .execute()
            }

            val response = client.get("$tablesUrl/poisoned/files/$id/stats")
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            val c = body(response).column(ids.getValue("c"))
            // "NULL, never guessed": a bound the reader cannot decode is
            // treated as absent, and absent means do not prune.
            assertThat(c["lower_bound"].isNull).isTrue()
            assertThat(c["upper_bound"].asLong()).isEqualTo(3)
        }

    // ---- resolution and 404s -----------------------------------------------

    @Test
    fun `unknown file, foreign file, and pre-visibility snapshots are 404`() =
        api { client ->
            createTable(client, "vis-a", """{"name": "c", "type": "long"}""")
            createTable(client, "vis-b", """{"name": "c", "type": "long"}""")
            val path = "s3://b/$catalog/vis-a/f1.parquet"
            commitFile(client, "vis-a", path, 1, statsJson = null)
            val id = fileId(client, "vis-a", path)
            val commitSnapshot =
                body(client.get("$tablesUrl/vis-a/files"))
                    .first { it["path"].asText() == path }["begin_snapshot"].asLong()

            assertThat(client.get("$tablesUrl/vis-a/files/999999/stats").status)
                .isEqualTo(HttpStatusCode.NotFound)
            // The file exists, but not under THIS table.
            assertThat(client.get("$tablesUrl/vis-b/files/$id/stats").status)
                .isEqualTo(HttpStatusCode.NotFound)
            // Before its begin_snapshot the file is not visible.
            assertThat(
                client.get("$tablesUrl/vis-a/files/$id/stats?snapshot=${commitSnapshot - 1}").status,
            ).isEqualTo(HttpStatusCode.NotFound)
            // At its begin_snapshot it is.
            assertThat(
                client.get("$tablesUrl/vis-a/files/$id/stats?snapshot=$commitSnapshot").status,
            ).isEqualTo(HttpStatusCode.OK)
        }

    @Test
    fun `a dropped column's stats row is omitted at head and present before the drop`() =
        api { client ->
            val ids =
                createTable(
                    client,
                    "dropped",
                    """{"name": "keep", "type": "long"}, {"name": "gone", "type": "long"}""",
                )
            val path = "s3://b/$catalog/dropped/f1.parquet"
            val rows =
                listOf(
                    statsRow(ids.getValue("keep"), 2, 0, enc(ColType.LONG, 1L), enc(ColType.LONG, 2L)),
                    statsRow(ids.getValue("gone"), 2, 0, enc(ColType.LONG, 5L), enc(ColType.LONG, 6L)),
                ).joinToString(",")
            commitFile(client, "dropped", path, 2, rows)
            val id = fileId(client, "dropped", path)
            val preDrop =
                body(client.get("/v1/catalogs/$catalog"))["head_snapshot_id"].asLong()

            val altered =
                client.postJson(
                    "$tablesUrl/dropped/alter",
                    """{"ops": [{"op": "drop_column", "name": "gone"}]}""",
                )
            assertThat(altered.status).isEqualTo(HttpStatusCode.OK)

            val atHead = body(client.get("$tablesUrl/dropped/files/$id/stats"))
            assertThat(atHead["columns"].map { it["field_id"].asLong() })
                .containsExactly(ids.getValue("keep"))

            val before = body(client.get("$tablesUrl/dropped/files/$id/stats?snapshot=$preDrop"))
            assertThat(before["columns"].map { it["field_id"].asLong() })
                .containsExactlyInAnyOrder(ids.getValue("keep"), ids.getValue("gone"))
        }
}
