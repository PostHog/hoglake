package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
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
import java.math.BigInteger
import java.util.Base64

/**
 * The ten scalar-parity types over the HTTP surface, end to end: created,
 * read back, given bounds on a commit, and promoted (or refused) by
 * ALTER.
 *
 * Everything here is proven at the wire with raw JSON rather than through
 * the services, because the wire is where a type name is a STRING and a
 * bound is BASE64. The service-level suites already prove the semantics;
 * what they cannot catch is a type that parses but does not serialize
 * back (a wire name added to the parser but not to `ColType.wire`), or a
 * bound mangled by the byte<->base64 hop. A caller only ever sees this
 * layer, so a round trip that is lossless in Kotlin and lossy in JSON is
 * still a broken type.
 *
 * Each test builds its own table: the class shares one database, and
 * order-dependent fixtures turn a single failure into five.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScalarTypeLifecycleApiTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()

    // Deliberately not "refused-types" (RefusedColumnTypeApiTest's): the
    // two classes share a container, and a name collision would make each
    // depend on whether the other ran first.
    private val catalog = "scalar-lifecycle"
    private val tablesUrl = "/v1/catalogs/$catalog/namespaces/ns/tables"

    /** The parity set, in the order a table declares them. */
    private val newScalars =
        listOf(
            "int8",
            "int16",
            "uint8",
            "uint16",
            "uint32",
            "uint64",
            "timestamp_s",
            "timestamp_ms",
            "timestamp_ns",
            "json",
        )

    @BeforeAll
    fun seed() =
        api { client ->
            client.postJson("/v1/catalogs", """{"name": "$catalog", "data_path": "s3://b/$catalog"}""")
            client.postJson("/v1/catalogs/$catalog/namespaces", """{"name": "ns"}""")
        }

    @AfterAll
    fun tearDown() = db.close()

    // ---- create and read back ---------------------------------------------

    @Test
    fun `all ten new scalars create in one table and echo back verbatim`() =
        api { client ->
            val response = client.postJson(tablesUrl, createBody("created", newScalars))
            assertThat(response.status).isEqualTo(HttpStatusCode.Created)
            val columns = body(response)["columns"]
            // Verbatim matters both ways: the name the caller wrote is the
            // name it must see, so a type quietly widened server-side
            // (uint8 stored as int, say) shows up here rather than in a
            // reader's wrong results months later.
            assertThat(columns.map { it["type"].asText() }).isEqualTo(newScalars)
            assertThat(columns.map { it["name"].asText() }).isEqualTo(newScalars.map { "c_$it" })
        }

    @Test
    fun `GET table returns the ten types with field ids and ordinals`() =
        api { client ->
            client.postJson(tablesUrl, createBody("fetched", newScalars))
            val response = client.get("$tablesUrl/fetched")
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            val columns = body(response)["columns"]
            assertThat(columns.map { it["type"].asText() }).isEqualTo(newScalars)
            assertThat(columns.map { it["ordinal"].asInt() }).isEqualTo(newScalars.indices.toList())
            // Field ids are the durable binding a parquet file carries;
            // ten columns must get ten distinct ones, whatever the
            // allocator's base.
            val fieldIds = columns.map { it["field_id"].asLong() }
            assertThat(fieldIds).doesNotHaveDuplicates()
            assertThat(fieldIds).isSorted()
        }

    // ---- inline bounds on a commit ----------------------------------------

    @Test
    fun `inline base64 bounds for the new types survive a commit byte for byte`() =
        api { client ->
            // All ten, not a sample: the base64 wire and the bytea column
            // are shared, but the ENCODINGS differ per mapped type, and a
            // width mistake only shows on the type that has it.
            val types = newScalars
            val created = client.postJson(tablesUrl, createBody("stats", types))
            assertThat(created.status).isEqualTo(HttpStatusCode.Created)
            val fieldIds = body(created)["columns"].associate { it["name"].asText() to it["field_id"].asLong() }

            val path = "s3://b/$catalog/stats/f1.parquet"
            val statsJson =
                types.joinToString(",") { type ->
                    val (lower, upper) = edgeBounds.getValue(type)
                    """{"field_id": ${fieldIds.getValue("c_$type")},
                        "value_count": 3, "null_count": 0,
                        "lower_bound": "${base64(lower)}",
                        "upper_bound": "${base64(upper)}"}"""
                }
            val commit =
                client.postJson(
                    "/v1/catalogs/$catalog/commit",
                    """{"appends": [{"namespace": "ns", "table": "stats", "files": [
                        {"path": "$path", "record_count": 3, "file_size_bytes": 1024,
                         "footer_size": 256, "column_stats": [$statsJson]}]}]}""",
                )
            assertThat(commit.status).isEqualTo(HttpStatusCode.OK)

            val files = body(client.get("$tablesUrl/stats/files"))
            assertThat(files).hasSize(1)
            // 'provided', not 'pending': inline stats mean the hydrator
            // never opens this file, so these bytes are the only bounds it
            // will ever have.
            assertThat(files[0]["stats_state"].asText()).isEqualTo("provided")
            assertThat(files[0]["path"].asText()).isEqualTo(path)

            // The stored BYTES are asserted at the database (the decoded
            // read surface is .../files/{fileId}/stats, whose wire form
            // FileStatsApiTest pins; this test pins the base64 -> bytea
            // hop itself).
            val stored = storedBounds(path)
            for (type in types) {
                val fieldId = fieldIds.getValue("c_$type")
                val (lower, upper) = edgeBounds.getValue(type)
                assertThat(stored[fieldId]?.first).describedAs("%s lower bound", type).isEqualTo(lower)
                assertThat(stored[fieldId]?.second).describedAs("%s upper bound", type).isEqualTo(upper)
            }
        }

    // ---- ALTER: adding the new scalars to an existing table ----------------

    @Test
    fun `add_column accepts every new scalar on a live table`() =
        api { client ->
            // Only the REFUSAL path was exercised over the wire, which
            // cannot distinguish "the parser rejects bad names" from "the
            // parser rejects everything unfamiliar".
            client.postJson(tablesUrl, """{"name": "grow", "columns": [{"name": "id", "type": "long"}]}""")
            for (type in newScalars) {
                val response =
                    client.postJson(
                        "$tablesUrl/grow/alter",
                        """{"ops": [{"op": "add_column",
                            "column": {"name": "c_$type", "type": "$type", "nullable": true}}]}""",
                    )
                assertThat(response.status).describedAs("add_column %s", type).isEqualTo(HttpStatusCode.OK)
                assertThat(typeOf(body(response), "c_$type"))
                    .describedAs("alter response echoes %s", type)
                    .isEqualTo(type)
            }
            // All ten landed, in the order they were added, alongside the
            // original column.
            val table = body(client.get("$tablesUrl/grow"))
            assertThat(table["columns"].map { it["name"].asText() })
                .isEqualTo(listOf("id") + newScalars.map { "c_$it" })
            assertThat(newScalars.map { typeOf(table, "c_$it") }).isEqualTo(newScalars)
            // Field ids keep ascending across the ten ALTERs — each is its
            // own DDL commit, so a reused id would be a cross-snapshot bug.
            val ids = table["columns"].map { it["field_id"].asLong() }
            assertThat(ids).isSorted()
            assertThat(ids.toSet()).hasSize(ids.size)
        }

    // ---- ALTER: the legal ladders and the illegal steps --------------------

    @Test
    fun `the legal promotion ladders walk over the wire`() =
        api { client ->
            client.postJson(
                tablesUrl,
                """{"name": "ladders", "columns": [
                    {"name": "a", "type": "int8"},
                    {"name": "b", "type": "uint8"},
                    {"name": "u", "type": "uint32"},
                    {"name": "ts", "type": "timestamp_s"}]}""",
            )
            // Each rung is its own request, exactly as a caller evolving a
            // schema over months issues them; a ladder that only works as
            // one batch is not a ladder.
            val rungs =
                listOf(
                    "a" to "int16",
                    "a" to "int",
                    "a" to "long",
                    "b" to "uint16",
                    "b" to "uint32",
                )
            for ((column, target) in rungs) {
                val response =
                    client.postJson(
                        "$tablesUrl/ladders/alter",
                        """{"ops": [{"op": "promote_column", "name": "$column", "to": "$target"}]}""",
                    )
                assertThat(response.status)
                    .describedAs("promote %s to %s", column, target)
                    .isEqualTo(HttpStatusCode.OK)
                assertThat(typeOf(body(response), column))
                    .describedAs("alter response echoes %s as %s", column, target)
                    .isEqualTo(target)
            }
            // u and ts never move: DuckLake's table has no uint32 -> long
            // rung and no timestamp rungs at all, so these are 422 and the
            // columns keep the types they were declared with.
            for ((column, target) in listOf("u" to "long", "ts" to "timestamp_ms")) {
                assertThat(
                    client.postJson(
                        "$tablesUrl/ladders/alter",
                        """{"ops": [{"op": "promote_column", "name": "$column", "to": "$target"}]}""",
                    ).status,
                ).describedAs("promote %s to %s", column, target)
                    .isEqualTo(HttpStatusCode.UnprocessableEntity)
            }
            val table = body(client.get("$tablesUrl/ladders"))
            assertThat(listOf("a", "b", "u", "ts").map { typeOf(table, it) })
                .isEqualTo(listOf("long", "uint32", "uint32", "timestamp_s"))
        }

    @Test
    fun `the Iceberg-illegal promotions are 422 at the wire, naming both types`() =
        api { client ->
            client.postJson(
                tablesUrl,
                """{"name": "illegal", "columns": [
                    {"name": "u", "type": "uint32"},
                    {"name": "ts", "type": "timestamp"},
                    {"name": "j", "type": "json"}]}""",
            )
            // Each of these would change the MAPPED Iceberg type, which
            // reinterprets every bound already stored — the failure a 200
            // here would produce is silent and unrecoverable, so it is
            // refused rather than migrated.
            val refused =
                listOf(
                    Triple("u", "uint32", "uint64"),
                    Triple("ts", "timestamp", "timestamp_ns"),
                    Triple("j", "json", "string"),
                )
            for ((column, from, target) in refused) {
                val response =
                    client.postJson(
                        "$tablesUrl/illegal/alter",
                        """{"ops": [{"op": "promote_column", "name": "$column", "to": "$target"}]}""",
                    )
                assertThat(response.status)
                    .describedAs("promote %s to %s", from, target)
                    .isEqualTo(HttpStatusCode.UnprocessableEntity)
                val node = body(response)
                assertThat(node["error"].asText()).isEqualTo("validation")
                val detail = node["detail"].asText()
                assertThat(detail).describedAs("detail names the source type").contains("'$from'")
                assertThat(detail).describedAs("detail names the target type").contains("'$target'")
            }
            // And nothing moved: a refusal that half-applied would leave
            // the table in a state no client asked for.
            val table = body(client.get("$tablesUrl/illegal"))
            assertThat(listOf("u", "ts", "j").map { typeOf(table, it) })
                .isEqualTo(listOf("uint32", "timestamp", "json"))
        }

    // ---- fixtures ---------------------------------------------------------

    /**
     * Domain-edge bounds per type, encoded by the server's own codec so
     * the test asserts the wire hop, not a second implementation of the
     * encoding (that equivalence is IcebergSingleValueTest's job).
     */
    private val edgeBounds: Map<String, Pair<ByteArray, ByteArray>> =
        mapOf(
            // The int-mapped widths: 4-byte bounds at each domain edge.
            "int8" to (IcebergSingleValue.encodeInt(-128) to IcebergSingleValue.encodeInt(127)),
            "int16" to (IcebergSingleValue.encodeInt(-32_768) to IcebergSingleValue.encodeInt(32_767)),
            "uint8" to (IcebergSingleValue.encodeInt(0) to IcebergSingleValue.encodeInt(255)),
            "uint16" to (IcebergSingleValue.encodeInt(0) to IcebergSingleValue.encodeInt(65_535)),
            // Declared in millis, stored in micros.
            "timestamp_ms" to
                (
                    IcebergSingleValue.encodeTimestampMillis(-62_135_596_800_000L) to
                        IcebergSingleValue.encodeTimestampMillis(253_402_300_799_000L)
                ),
            // uint32 maps to Iceberg long: 8 bytes even at 0.
            "uint32" to (IcebergSingleValue.encodeLong(0L) to IcebergSingleValue.encodeLong(4_294_967_295L)),
            // uint64 maps to decimal(20,0): big-endian, variable length,
            // and 2^64-1 needs its leading sign byte.
            "uint64" to
                (
                    IcebergSingleValue.encodeDecimalUnscaled(BigInteger.ZERO) to
                        IcebergSingleValue.encodeDecimalUnscaled(
                            BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE),
                        )
                ),
            // Declared in seconds, stored in micros — the conversion is
            // the encoder's, and the wire carries the result.
            "timestamp_s" to
                (
                    IcebergSingleValue.encodeTimestampSeconds(-62_135_596_800L) to
                        IcebergSingleValue.encodeTimestampSeconds(253_402_300_799L)
                ),
            // Nanos stay nanos: the one timestamp that is not micros.
            "timestamp_ns" to
                (
                    IcebergSingleValue.encodeTimestampNanos(-1_500L) to
                        IcebergSingleValue.encodeTimestampNanos(4_102_444_800_000_000_000L)
                ),
            // A non-ASCII document: its UTF-8 bytes must arrive unchanged
            // through base64 and Postgres bytea alike.
            "json" to
                (
                    IcebergSingleValue.encodeString("""{"a":1}""") to
                        IcebergSingleValue.encodeString("""{"µ":"ü"}""")
                ),
        )

    private fun createBody(
        name: String,
        types: List<String>,
    ): String {
        val columns = types.joinToString(",") { """{"name": "c_$it", "type": "$it"}""" }
        return """{"name": "$name", "columns": [$columns]}"""
    }

    private fun typeOf(
        table: JsonNode,
        column: String,
    ): String = table["columns"].single { it["name"].asText() == column }["type"].asText()

    /** field_id -> (lower, upper) as the catalog actually stored them. */
    private fun storedBounds(path: String): Map<Long, Pair<ByteArray?, ByteArray?>> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT s.field_id, s.lower_bound, s.upper_bound
                FROM hog_file_column_stats s
                JOIN hog_data_file f
                  ON f.catalog_id = s.catalog_id AND f.data_file_id = s.data_file_id
                WHERE f.path = ?
                """,
            )
                .bind(0, path)
                .map { rs, _ -> rs.getLong(1) to (rs.getBytes(2) to rs.getBytes(3)) }
                .list()
                .toMap()
        }

    private fun base64(bytes: ByteArray): String = Base64.getEncoder().encodeToString(bytes)

    // ---- harness ----------------------------------------------------------

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
}
