package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.Database
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.service.ScanService
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
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.math.BigDecimal
import java.math.BigInteger
import java.util.Base64
import java.util.concurrent.CopyOnWriteArrayList

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
 *
 * The scan plan's `column_stats` (GET .../scan) is the same entries in
 * the same shape, so its tests live here too: they compare the scan's
 * bytes against this endpoint's rather than restating expectations.
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

            // The containers themselves never carry rows, and the leaves
            // arrive in ASCENDING field-id order — the spec's promise on
            // FileStats.columns ("field-id order"), pinned as a sequence
            // so a reversed or shuffled ORDER BY cannot pass.
            val returnedIds = node["columns"].map { it["field_id"].asLong() }
            assertThat(returnedIds).doesNotContain(ids.getValue("s"), ids.getValue("l"), ids.getValue("m"))
            val expectedOrder =
                listOf("s.a", "l.element", "m.key", "m.value").map { ids.getValue(it) }.sorted()
            assertThat(returnedIds).containsExactlyElementsOf(expectedOrder)
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

    // ---- failed hydration: the OTHER no-stats arm --------------------------

    @Test
    fun `a failed-stats file answers the exact failed shape`() =
        api { client ->
            createTable(client, "failedstats", """{"name": "id", "type": "long"}""")
            val path = "s3://b/$catalog/failedstats/f1.parquet"
            commitFile(client, "failedstats", path, 5, statsJson = null)
            val id = fileId(client, "failedstats", path)
            // Stage the hydrator's structural-failure outcome directly
            // (the same behind-the-door technique as the poisoned-bound
            // test): a deferred-stats file whose footer read failed.
            db.jdbi.withHandleUnchecked { h ->
                h.createUpdate(
                    "UPDATE hog_data_file SET stats_state = 'failed' WHERE data_file_id = :f",
                )
                    .bind("f", id)
                    .execute()
            }

            val response = client.get("$tablesUrl/failedstats/files/$id/stats")
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            val node = body(response)
            assertThat(node["stats_state"].asText()).isEqualTo("failed")
            assertThat(node["columns"]).isEmpty()
            // The exact reason string is the contract: it names the
            // state, the recovery door, and the no-pruning rule.
            assertThat(node["no_stats_reason"].asText()).isEqualTo(
                "stats_state is 'failed': stats hydration failed structurally " +
                    "(see POST .../maintenance/rehydrate), so no per-column rows exist; " +
                    "with no bounds, callers must not prune this file",
            )
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

    @Test
    fun `at_timestamp resolves stats to the pre-drop snapshot`() =
        api { client ->
            val ids =
                createTable(
                    client,
                    "tstravel",
                    """{"name": "keep", "type": "long"}, {"name": "gone", "type": "long"}""",
                )
            val path = "s3://b/$catalog/tstravel/f1.parquet"
            val rows =
                listOf(
                    statsRow(ids.getValue("keep"), 2, 0, enc(ColType.LONG, 1L), enc(ColType.LONG, 2L)),
                    statsRow(ids.getValue("gone"), 2, 0, enc(ColType.LONG, 5L), enc(ColType.LONG, 6L)),
                ).joinToString(",")
            commitFile(client, "tstravel", path, 2, rows)
            val id = fileId(client, "tstravel", path)
            val preDrop = body(client.get("/v1/catalogs/$catalog"))["head_snapshot_id"].asLong()

            val altered =
                client.postJson(
                    "$tablesUrl/tstravel/alter",
                    """{"ops": [{"op": "drop_column", "name": "gone"}]}""",
                )
            assertThat(altered.status).isEqualTo(HttpStatusCode.OK)

            // Deterministic snapshot times (base + snapshot_id minutes),
            // rewritten directly like TimestampTravelIntegrationTest:
            // adjacent test commits can share a wall-clock microsecond,
            // and this test is about resolution, not clocks.
            val base = java.time.Instant.parse("2026-03-01T00:00:00Z")
            db.jdbi.withHandleUnchecked { h ->
                h.createUpdate(
                    """
                    UPDATE hog_snapshot
                    SET snapshot_time = :base + make_interval(mins => snapshot_id::int)
                    WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = :cat)
                    """,
                )
                    .bind("base", base.atOffset(java.time.ZoneOffset.UTC))
                    .bind("cat", catalog)
                    .execute()
            }

            // At the pre-drop instant the dropped column's stats row is
            // still visible — a resolution head cannot produce, so a
            // handler that ignores at_timestamp fails here.
            val atPreDrop = base.plusSeconds(preDrop * 60)
            val travelled =
                body(client.get("$tablesUrl/tstravel/files/$id/stats?at_timestamp=$atPreDrop"))
            assertThat(travelled["columns"].map { it["field_id"].asLong() })
                .containsExactlyInAnyOrder(ids.getValue("keep"), ids.getValue("gone"))

            val atHead = body(client.get("$tablesUrl/tstravel/files/$id/stats"))
            assertThat(atHead["columns"].map { it["field_id"].asLong() })
                .containsExactly(ids.getValue("keep"))
        }

    // ---- the scan plan carries the same stats (opt-in, slim) ---------------

    private val statsScan = "scan?include=column_stats"

    /**
     * The `columns` of GET .../stats for one file, projected to the scan
     * plan's slim entry AS RAW TEXT: the per-table identity (name, path,
     * type, type_params) and size_bytes cut out of each entry, every other
     * token left byte for byte. The scan's column_stats must equal this
     * string, so a second decode path — or any re-rendering of a bound —
     * cannot pass.
     */
    private suspend fun slimStatsRaw(
        client: HttpClient,
        table: String,
        fileId: Long,
        query: String = "",
    ): String {
        val raw = client.get("$tablesUrl/$table/files/$fileId/stats$query").bodyAsText()
        // FileStatsDto serializes data_file_id, stats_state, columns — and
        // no_stats_reason only when not provided — so for a provided file
        // the array runs to the closing brace.
        val marker = "\"columns\":"
        assertThat(raw).contains(marker).endsWith("]}")
        return raw.substring(raw.indexOf(marker) + marker.length, raw.length - 1)
            .replace(Regex(""""name":"[^"]*","path":"[^"]*","type":"[^"]*",("type_params":\{[^}]*\},)?"""), "")
            .replace(Regex(""","size_bytes":\d+"""), "")
    }

    private fun JsonNode.scanFile(path: String): JsonNode =
        first { it["data_file"]["path"].asText() == path }["data_file"]

    /** The six-column fixture, one provided + one pending + one failed file. */
    private suspend fun seedScanned(
        client: HttpClient,
        table: String,
    ): Map<String, Long> {
        val ids =
            createTable(
                client,
                table,
                """{"name": "c_long", "type": "long"},
                   {"name": "c_double", "type": "double"},
                   {"name": "c_string", "type": "string"},
                   {"name": "c_decimal", "type": "decimal",
                    "type_params": {"precision": 10, "scale": 2}},
                   {"name": "c_ts", "type": "timestamp"},
                   {"name": "c_allnull", "type": "long"},
                   {"name": "v", "type": "variant"}""",
            )
        commitFile(
            client,
            table,
            "s3://b/$catalog/$table/provided.parquet",
            10,
            listOf(
                // 2^53 + 1, -0.0 and a scale-2 decimal: the tokens a second
                // decode path would most plausibly render differently.
                statsRow(
                    ids.getValue("c_long"),
                    10,
                    0,
                    enc(ColType.LONG, 9007199254740993L),
                    enc(ColType.LONG, Long.MAX_VALUE),
                ),
                statsRow(
                    ids.getValue("c_double"),
                    10,
                    0,
                    enc(ColType.DOUBLE, -0.0),
                    enc(ColType.DOUBLE, 0.1),
                    extra = """"nan_count": 2, "size_bytes": 80""",
                ),
                statsRow(ids.getValue("c_string"), 10, 1, enc(ColType.STRING, "aardvark"), enc(ColType.STRING, "🦔")),
                statsRow(
                    ids.getValue("c_decimal"),
                    10,
                    0,
                    enc(ColType.DECIMAL, BigInteger("150")),
                    enc(ColType.DECIMAL, BigInteger("99999")),
                ),
                statsRow(
                    ids.getValue("c_ts"),
                    10,
                    0,
                    enc(ColType.TIMESTAMP, 1_788_566_400_000_000L),
                    enc(ColType.TIMESTAMP, 1_788_652_799_999_999L),
                ),
                statsRow(ids.getValue("c_allnull"), 10, 10, lower = null, upper = null),
            ).joinToString(","),
        )
        commitFile(client, table, "s3://b/$catalog/$table/pending.parquet", 5, statsJson = null)
        commitFile(client, table, "s3://b/$catalog/$table/failed.parquet", 5, statsJson = null)
        val failedId = fileId(client, table, "s3://b/$catalog/$table/failed.parquet")
        db.jdbi.withHandleUnchecked { h ->
            h.createUpdate("UPDATE hog_data_file SET stats_state = 'failed' WHERE data_file_id = :f")
                .bind("f", failedId)
                .execute()
        }
        return ids
    }

    @Test
    fun `a plain scan carries no column_stats and reads no statistics`() =
        api { client ->
            seedScanned(client, "plainscan")
            val scan = body(client.get("$tablesUrl/plainscan/scan"))
            assertThat(scan).hasSize(3)
            // Opt-in: not even the provided file carries it, so a consumer
            // that cannot prune pays neither the join nor the payload.
            assertThat(scan.map { it["data_file"].has("column_stats") }).containsOnly(false)
            assertThat(scanStatements("plainscan", request = null).none { it.contains("hog_file_column_stats") })
                .isTrue()
        }

    @Test
    fun `include=column_stats carries slim stats for a provided file only, tokens identical to the stats endpoint`() =
        api { client ->
            val ids = seedScanned(client, "scanned")
            val provided = "s3://b/$catalog/scanned/provided.parquet"
            val providedId = fileId(client, "scanned", provided)

            val response = client.get("$tablesUrl/scanned/$statsScan")
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            val raw = response.bodyAsText()
            val scan = json.readTree(raw)

            val providedFile = scan.scanFile(provided)
            assertThat(providedFile["stats_state"].asText()).isEqualTo("provided")
            // Six rows committed, six entries; the variant has none and
            // gets none. Field-id order, as on the stats endpoint.
            val columnIds = providedFile["column_stats"].map { it["field_id"].asLong() }
            assertThat(columnIds).containsExactlyElementsOf(
                listOf(
                    "c_long",
                    "c_double",
                    "c_string",
                    "c_decimal",
                    "c_ts",
                    "c_allnull",
                ).map { ids.getValue(it) }.sorted(),
            )
            // SLIM: the per-table column identity is not repeated per file.
            providedFile["column_stats"].forEach { entry ->
                assertThat(entry.fieldNames().asSequence().toSet())
                    .isSubsetOf("field_id", "value_count", "null_count", "nan_count", "lower_bound", "upper_bound")
                    .contains("field_id", "value_count", "null_count", "lower_bound", "upper_bound")
            }
            // The round trip, byte for byte on every retained token: the
            // scan's array IS the stats endpoint's `columns` minus the
            // identity fields.
            assertThat(raw).contains("\"column_stats\":" + slimStatsRaw(client, "scanned", providedId))
            // And the tokens are the decoded ones, not base64 bytes.
            assertThat(raw).contains("9007199254740993").contains("\"lower_bound\":-0.0")
            assertThat(raw).contains("\"lower_bound\":1.50").contains("\"lower_bound\":\"2026-09-05T00:00\"")
            val allNull = providedFile["column_stats"].first { it["field_id"].asLong() == ids.getValue("c_allnull") }
            // value_count INCLUDES nulls: all-null is null_count == value_count.
            assertThat(allNull["value_count"].asLong()).isEqualTo(allNull["null_count"].asLong())
            assertThat(allNull["lower_bound"].isNull).isTrue()

            // No statistics means NO property — not an empty array, which
            // would read as "provided, but nothing to say".
            val pending = scan.scanFile("s3://b/$catalog/scanned/pending.parquet")
            assertThat(pending["stats_state"].asText()).isEqualTo("pending")
            assertThat(pending.has("column_stats")).isFalse()
            val failed = scan.scanFile("s3://b/$catalog/scanned/failed.parquet")
            assertThat(failed["stats_state"].asText()).isEqualTo("failed")
            assertThat(failed.has("column_stats")).isFalse()

            // The listing and the changefeed embed the same DataFile
            // schema and do not carry it (spec: "Populated by GET
            // .../scan ONLY").
            val listed = body(client.get("$tablesUrl/scanned/files")).first { it["path"].asText() == provided }
            assertThat(listed.has("column_stats")).isFalse()
            val changes = body(client.get("$tablesUrl/scanned/changes?from_snapshot=0"))
            assertThat(changes["files"].first { it["path"].asText() == provided }.has("column_stats")).isFalse()
        }

    @Test
    fun `stats_fields narrows column_stats to the named columns`() =
        api { client ->
            val ids = seedScanned(client, "narrowed")
            val provided = "s3://b/$catalog/narrowed/provided.parquet"
            val ts = ids.getValue("c_ts")
            val long = ids.getValue("c_long")

            val narrowed =
                body(client.get("$tablesUrl/narrowed/$statsScan&stats_fields=$ts,$long,$ts")).scanFile(provided)
            // Only the requested columns, still field-id order, duplicates
            // collapsed.
            assertThat(narrowed["column_stats"].map { it["field_id"].asLong() })
                .containsExactlyElementsOf(listOf(ts, long).sorted())
            // A REPEATED parameter is one list, not its first occurrence —
            // otherwise the engine silently loses the second column's bounds.
            val repeated =
                body(
                    client.get("$tablesUrl/narrowed/$statsScan&stats_fields=$ts&stats_fields=$long"),
                ).scanFile(provided)
            assertThat(repeated["column_stats"].map { it["field_id"].asLong() })
                .containsExactlyElementsOf(listOf(ts, long).sorted())
            val tsEntry = narrowed["column_stats"].first { it["field_id"].asLong() == ts }
            assertThat(tsEntry["upper_bound"].asText()).isEqualTo("2026-09-05T23:59:59.999999")

            // Columns with no stats row (the variant) or no column at all:
            // the file is provided and has nothing for them — an EMPTY
            // array, which is a different answer from an absent property.
            val nothing =
                body(client.get("$tablesUrl/narrowed/$statsScan&stats_fields=${ids.getValue("v")},999999"))
                    .scanFile(provided)
            assertThat(nothing.has("column_stats")).isTrue()
            assertThat(nothing["column_stats"]).isEmpty()
        }

    @Test
    fun `a provided file with zero stats rows answers an empty array, not an absent one`() =
        api { client ->
            createTable(client, "zerorows", """{"name": "a", "type": "long"}""")
            val path = "s3://b/$catalog/zerorows/f1.parquet"
            // Stats were SHIPPED — as an empty list — so the file is
            // provided with nothing recorded for any column.
            commitFile(client, "zerorows", path, 3, statsJson = "")
            val file = body(client.get("$tablesUrl/zerorows/$statsScan")).scanFile(path)
            assertThat(file["stats_state"].asText()).isEqualTo("provided")
            assertThat(file.has("column_stats")).isTrue()
            assertThat(file["column_stats"]).isEmpty()
        }

    @Test
    fun `scan column_stats resolve columns at the scan's snapshot, not at head`() =
        api { client ->
            val ids =
                createTable(
                    client,
                    "scandrop",
                    """{"name": "keep", "type": "long"}, {"name": "gone", "type": "long"}""",
                )
            val path = "s3://b/$catalog/scandrop/f1.parquet"
            commitFile(
                client,
                "scandrop",
                path,
                2,
                listOf(
                    statsRow(ids.getValue("keep"), 2, 0, enc(ColType.LONG, 1L), enc(ColType.LONG, 2L)),
                    statsRow(ids.getValue("gone"), 2, 0, enc(ColType.LONG, 5L), enc(ColType.LONG, 6L)),
                ).joinToString(","),
            )
            val id = fileId(client, "scandrop", path)
            val preDrop = body(client.get("/v1/catalogs/$catalog"))["head_snapshot_id"].asLong()
            val altered =
                client.postJson("$tablesUrl/scandrop/alter", """{"ops": [{"op": "drop_column", "name": "gone"}]}""")
            assertThat(altered.status).isEqualTo(HttpStatusCode.OK)

            fun ids(scan: JsonNode) = scan.scanFile(path)["column_stats"].map { it["field_id"].asLong() }

            // At head the dropped column has no type to decode under and is
            // omitted, exactly as the stats endpoint omits it.
            val atHeadRaw = client.get("$tablesUrl/scandrop/$statsScan").bodyAsText()
            assertThat(ids(json.readTree(atHeadRaw))).containsExactly(ids.getValue("keep"))
            assertThat(atHeadRaw).contains("\"column_stats\":" + slimStatsRaw(client, "scandrop", id))

            // Before the drop it is visible, and present.
            val beforeRaw = client.get("$tablesUrl/scandrop/$statsScan&snapshot=$preDrop").bodyAsText()
            assertThat(ids(json.readTree(beforeRaw)))
                .containsExactly(ids.getValue("keep"), ids.getValue("gone"))
            assertThat(beforeRaw)
                .contains("\"column_stats\":" + slimStatsRaw(client, "scandrop", id, "?snapshot=$preDrop"))
        }

    @Test
    fun `at_timestamp resolves the snapshot that column_stats are read at`() =
        api { client ->
            // The two read-target parameters are not interchangeable in
            // the code — `at_timestamp` goes through TimeTravelRepo and
            // `snapshot` does not — so "snapshot= travels correctly"
            // says nothing about the other one. A handler that resolved
            // the timestamp for the FILE query and then read statistics
            // at head would pass every test above.
            val ids =
                createTable(
                    client,
                    "tsstats",
                    """{"name": "keep", "type": "long"}, {"name": "gone", "type": "long"}""",
                )
            val path = "s3://b/$catalog/tsstats/f1.parquet"
            commitFile(
                client,
                "tsstats",
                path,
                2,
                listOf(
                    statsRow(ids.getValue("keep"), 2, 0, enc(ColType.LONG, 1L), enc(ColType.LONG, 2L)),
                    statsRow(ids.getValue("gone"), 2, 0, enc(ColType.LONG, 5L), enc(ColType.LONG, 6L)),
                ).joinToString(","),
            )
            val preDrop = body(client.get("/v1/catalogs/$catalog"))["head_snapshot_id"].asLong()
            assertThat(
                client.postJson("$tablesUrl/tsstats/alter", """{"ops": [{"op": "drop_column", "name": "gone"}]}""")
                    .status,
            ).isEqualTo(HttpStatusCode.OK)

            // Snapshot times are minute-spaced off a fixed base so a
            // timestamp can name one snapshot unambiguously (the same
            // technique the stats endpoint's time-travel test uses).
            val base = java.time.Instant.parse("2026-02-01T00:00:00Z")
            db.jdbi.withHandleUnchecked { h ->
                h.createUpdate(
                    """
                    UPDATE hog_snapshot
                    SET snapshot_time = :base + make_interval(mins => snapshot_id::int)
                    WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = :cat)
                    """,
                )
                    .bind("base", base.atOffset(java.time.ZoneOffset.UTC))
                    .bind("cat", catalog)
                    .execute()
            }
            val atPreDrop = base.plusSeconds(preDrop * 60)

            val travelled = client.get("$tablesUrl/tsstats/$statsScan&at_timestamp=$atPreDrop")
            assertThat(travelled.status).isEqualTo(HttpStatusCode.OK)
            val travelledFile = json.readTree(travelled.bodyAsText()).scanFile(path)
            assertThat(travelledFile["column_stats"].map { it["field_id"].asLong() })
                .describedAs("at the pre-drop instant the dropped column still resolves")
                .containsExactly(ids.getValue("keep"), ids.getValue("gone"))

            val atHead = body(client.get("$tablesUrl/tsstats/$statsScan")).scanFile(path)
            assertThat(atHead["column_stats"].map { it["field_id"].asLong() })
                .containsExactly(ids.getValue("keep"))

            // And the two targets still refuse to be combined, with the
            // statistics parameters in play.
            assertThat(
                client.get("$tablesUrl/tsstats/$statsScan&at_timestamp=$atPreDrop&snapshot=$preDrop").status,
            ).isEqualTo(HttpStatusCode.UnprocessableEntity)
        }

    @Test
    fun `a plan carries enough for a reader to prune one of two disjoint files`() =
        api { client ->
            // The consumer-facing property, over raw REST rather than
            // through a client. The Trino connector — the one engine in
            // the tree that plans splits from this endpoint — does NOT
            // send `include` (PostHog/trino, HoglakeClient.listScan
            // requests `/scan?snapshot=`, and its own test asserts
            // `column_stats` is absent), so there is no harness case to
            // add there and this stands in for one: the wire really does
            // carry two disjoint ranges for the same column, decoded,
            // under the field id an engine holds from the table schema.
            val ids = createTable(client, "prune", """{"name": "ts", "type": "long"}""")
            val ts = ids.getValue("ts")
            val early = "s3://b/$catalog/prune/early.parquet"
            val late = "s3://b/$catalog/prune/late.parquet"
            commitFile(
                client,
                "prune",
                early,
                100,
                statsRow(ts, 100, 0, enc(ColType.LONG, 1L), enc(ColType.LONG, 10L)),
            )
            commitFile(
                client,
                "prune",
                late,
                100,
                statsRow(ts, 100, 0, enc(ColType.LONG, 100L), enc(ColType.LONG, 110L)),
            )

            val plan = body(client.get("$tablesUrl/prune/$statsScan&stats_fields=$ts"))
            assertThat(plan).hasSize(2)

            fun boundsOf(path: String): Pair<Long, Long> {
                val entry =
                    plan.scanFile(path)["column_stats"].single { it["field_id"].asLong() == ts }
                return entry["lower_bound"].asLong() to entry["upper_bound"].asLong()
            }
            assertThat(boundsOf(early)).isEqualTo(1L to 10L)
            assertThat(boundsOf(late)).isEqualTo(100L to 110L)

            // What an engine would do with them: `ts > 50` keeps exactly
            // the late file. Expressed as the pruning decision rather
            // than as two bound comparisons, because the decision is the
            // feature and a plan that reported both ranges as the same
            // would satisfy the comparisons above only by accident.
            val kept =
                listOf(early, late).filter { path ->
                    val (_, upper) = boundsOf(path)
                    upper > 50L
                }
            assertThat(kept).containsExactly(late)
        }

    @Test
    fun `malformed or unusable stats parameters are refused, never ignored`() =
        api { client ->
            createTable(client, "badparams", """{"name": "a", "type": "long"}""")
            val base = "$tablesUrl/badparams/scan"
            // A misspelt include must not read as "no statistics, so
            // nothing can be pruned".
            val unknown = client.get("$base?include=column_stat")
            assertThat(unknown.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
            assertThat(unknown.bodyAsText()).contains("column_stat").contains("column_stats")
            // ...including when it is the SECOND occurrence of the parameter.
            assertThat(client.get("$base?include=column_stats&include=column_stat").status)
                .isEqualTo(HttpStatusCode.UnprocessableEntity)
            // An EMPTY value is the malformed side of the line, not the
            // unknown-name side, and now answers what an empty
            // `stats_fields` answers. It used to reach the unknown-value
            // arm and answer 422 while its twin answered 400 — one
            // mistake, two verdicts. This assertion is the contract
            // change, not a fixture adjusted to suit a new guard.
            assertThat(client.get("$base?include=").status).isEqualTo(HttpStatusCode.BadRequest)
            assertThat(client.get("$base?include=column_stats,").status)
                .isEqualTo(HttpStatusCode.BadRequest)
            // The echo in the unknown-value refusal is bounded on both
            // axes: Identifiers.cap bounds each token's length, and the
            // DISTINCT value count is capped, so a caller cannot make
            // the server quote a kilobyte of their own input back.
            val tooManyIncludes = (1..17).joinToString(",") { "v$it" }
            assertThat(client.get("$base?include=$tooManyIncludes").status)
                .isEqualTo(HttpStatusCode.BadRequest)
            assertThat(client.get("$base?include=" + (1..16).joinToString(",") { "v$it" }).status)
                .describedAs("sixteen distinct values is a well-formed list; they are simply unknown")
                .isEqualTo(HttpStatusCode.UnprocessableEntity)
            // ...and the caps count DISTINCT values, so the parameters'
            // OWN canonical encoding is not an abuse. `explode: true`
            // makes repetition how a generated client sends a list, and
            // the route joins occurrences before the parse: one legal
            // value seventeen times is a list of one.
            assertThat(client.get("$base?" + (1..17).joinToString("&") { "include=column_stats" }).status)
                .describedAs("one legal include value, repeated past the distinct cap")
                .isEqualTo(HttpStatusCode.OK)
            assertThat(
                client.get(
                    "$base?include=column_stats&" + (1..10_001).joinToString("&") { "stats_fields=1" },
                ).status,
            ).describedAs("one field id, repeated past the distinct cap").isEqualTo(HttpStatusCode.OK)
            // stats_fields alone would silently return no statistics.
            val orphan = client.get("$base?stats_fields=1")
            assertThat(orphan.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
            assertThat(orphan.bodyAsText()).contains("include=column_stats")
            // Malformed ids are 400s.
            assertThat(client.get("$base?include=column_stats&stats_fields=abc").status)
                .isEqualTo(HttpStatusCode.BadRequest)
            assertThat(client.get("$base?include=column_stats&stats_fields=").status)
                .isEqualTo(HttpStatusCode.BadRequest)
            assertThat(client.get("$base?include=column_stats&stats_fields=1,,2").status)
                .isEqualTo(HttpStatusCode.BadRequest)
            val tooMany = (1..10_001).joinToString(",")
            assertThat(client.get("$base?include=column_stats&stats_fields=$tooMany").status)
                .describedAs("10,001 DISTINCT field ids, which no legal table can have")
                .isEqualTo(HttpStatusCode.BadRequest)
            // The well-formed spellings are accepted.
            assertThat(client.get("$base?include=column_stats,column_stats&stats_fields=1").status)
                .isEqualTo(HttpStatusCode.OK)
        }

    /**
     * The statements one planScan issues, captured on an instrumented
     * Jdbi over the same database (the pattern VerifySpecParityTest and
     * CompactionPlanningIntegrationTest use).
     */
    private fun scanStatements(
        table: String,
        request: ScanService.ColumnStatsRequest? = ScanService.ColumnStatsRequest(),
    ): List<String> {
        val issued = CopyOnWriteArrayList<String>()
        val instrumented = Database.jdbi(db.dataSource)
        instrumented.setSqlLogger(
            object : SqlLogger {
                override fun logAfterExecution(context: StatementContext) {
                    issued += context.renderedSql
                }
            },
        )
        ScanService(instrumented).planScan(catalog, "ns", table, columnStats = request)
        return issued.toList()
    }

    private suspend fun commitFiles(
        client: HttpClient,
        table: String,
        paths: List<String>,
        statsJson: String?,
    ) {
        val stats = statsJson?.let { ""","column_stats": [$it]""" } ?: ""
        val files = paths.joinToString(",") { """{"path": "$it", "record_count": 3, "file_size_bytes": 1024$stats}""" }
        val response =
            client.postJson(
                "/v1/catalogs/$catalog/commit",
                """{"appends": [{"namespace": "ns", "table": "$table", "files": [$files]}]}""",
            )
        assertThat(response.status).describedAs(bodyText(response)).isEqualTo(HttpStatusCode.OK)
    }

    @Test
    fun `the scan plan's statement count does not grow with the file count`() =
        api { client ->
            val ids =
                createTable(client, "manyfiles", """{"name": "a", "type": "long"}, {"name": "b", "type": "string"}""")
            val rows =
                listOf(
                    statsRow(ids.getValue("a"), 3, 0, enc(ColType.LONG, 1L), enc(ColType.LONG, 3L)),
                    statsRow(ids.getValue("b"), 3, 0, enc(ColType.STRING, "x"), enc(ColType.STRING, "z")),
                ).joinToString(",")
            val narrow = ScanService.ColumnStatsRequest(setOf(ids.getValue("a")))
            commitFiles(client, "manyfiles", (1..3).map { "s3://b/$catalog/manyfiles/p$it.parquet" }, rows)
            commitFiles(client, "manyfiles", listOf("s3://b/$catalog/manyfiles/pending.parquet"), null)
            val small = scanStatements("manyfiles")
            val smallNarrow = scanStatements("manyfiles", narrow)

            commitFiles(client, "manyfiles", (4..60).map { "s3://b/$catalog/manyfiles/p$it.parquet" }, rows)
            val large = scanStatements("manyfiles")
            val largeNarrow = scanStatements("manyfiles", narrow)

            // Every file of the larger plan did get its stats — the count
            // below is not vacuous because nothing was attached.
            val plan =
                ScanService(
                    db.jdbi,
                ).planScan(catalog, "ns", "manyfiles", columnStats = ScanService.ColumnStatsRequest())
            assertThat(plan).hasSize(61)
            assertThat(plan.filter { it.dataFile.columnStats != null }).hasSize(60)
            assertThat(plan.mapNotNull { it.dataFile.columnStats }).allSatisfy { assertThat(it).hasSize(2) }
            val narrowPlan = ScanService(db.jdbi).planScan(catalog, "ns", "manyfiles", columnStats = narrow)
            assertThat(narrowPlan.mapNotNull { it.dataFile.columnStats }.flatten().map { it.fieldId })
                .hasSize(60)
                .containsOnly(ids.getValue("a"))

            // 4 files -> 61 files, same statements: resolution, one
            // file+DV query, one column query, ONE stats query.
            assertThat(large).hasSameSizeAs(small)
            assertThat(largeNarrow).hasSameSizeAs(smallNarrow).hasSameSizeAs(large)
            for (issued in listOf(large, largeNarrow)) {
                assertThat(issued.count { it.contains("hog_file_column_stats") })
                    .describedAs(issued.joinToString("\n---\n"))
                    .isEqualTo(1)
                // No per-file shape anywhere: no statement binds a file id.
                assertThat(issued.filter { it.contains(":dataFileId") || it.contains("IN (") }).isEmpty()
            }
            // The field filter is ONE array parameter, not one per id.
            assertThat(largeNarrow.single { it.contains("hog_file_column_stats") }).contains("ANY(:fieldIds)")
        }

    @Test
    fun `a scan with no provided file issues no stats query`() =
        api { client ->
            createTable(client, "allpending", """{"name": "a", "type": "long"}""")
            commitFiles(client, "allpending", (1..3).map { "s3://b/$catalog/allpending/f$it.parquet" }, null)
            val issued = scanStatements("allpending")
            assertThat(issued.none { it.contains("hog_file_column_stats") }).isTrue()
            assertThat(
                ScanService(db.jdbi)
                    .planScan(catalog, "ns", "allpending", columnStats = ScanService.ColumnStatsRequest())
                    .map { it.dataFile.columnStats },
            ).containsOnlyNulls()
        }
}
