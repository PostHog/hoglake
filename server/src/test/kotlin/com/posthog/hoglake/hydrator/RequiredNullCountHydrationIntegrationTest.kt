package com.posthog.hoglake.hydrator

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.stats.IcebergSingleValue
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.TestImages
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
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.format.Util
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.LocalInputFile
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files

/**
 * Hydration of a file whose writer omitted `null_count` from every column
 * chunk's statistics — what ClickHouse does for its non-Nullable
 * columns, which it writes REQUIRED. The file here is a real parquet-java
 * file whose FOOTER has been rewritten with each `null_count` unset (the
 * data pages are untouched and the hydrator never reads them), so
 * parquet-java's own footer decode is part of what is tested.
 *
 * A REQUIRED top-level leaf has max definition level 0 and cannot hold a
 * null, so it gets a row with null_count 0; a leaf that can hold one —
 * OPTIONAL itself, or REQUIRED under an OPTIONAL struct — still gets
 * none. Either way the file becomes 'provided'. The scan plan's
 * `column_stats` then carries the required columns' bounds, which is
 * what planning-time pruning in the Trino connector reads.
 *
 * No real ClickHouse-written fixture exists in the repo (server tests or
 * bench/), and generating one needs a ClickHouse binary; the footer
 * rewrite reproduces the one property that matters, verified below.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RequiredNullCountHydrationIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val hydrator by lazy { Hydrator(db.jdbi, store) }
    private val json = ObjectMapper()

    private val catalog = "chnulls"
    private val tablesUrl = "/v1/catalogs/$catalog/namespaces/ns/tables"

    private companion object {
        const val BUCKET = "hoglake-test"
        const val ROWS = 20
        const val TS0 = 1_788_566_400_000_000L

        val minio by lazy { TestImages.minio().also { it.start() } }

        val store: ObjectStore by lazy {
            ObjectStore(
                endpoint = minio.s3URL,
                region = "us-east-1",
                accessKey = minio.userName,
                secretKey = minio.password,
                pathStyle = true,
            ).also { it.createBucket(BUCKET) }
        }
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

    /** name (dotted for nested) -> field_id, over every node. */
    private suspend fun createTable(
        client: HttpClient,
        name: String,
        columnsJson: String,
    ): Map<String, Int> {
        val response = client.postJson(tablesUrl, """{"name": "$name", "columns": [$columnsJson]}""")
        assertThat(response.status).describedAs(response.bodyAsText()).isEqualTo(HttpStatusCode.Created)
        val ids = mutableMapOf<String, Int>()

        fun walk(
            node: JsonNode,
            prefix: String,
        ) {
            for (col in node) {
                val path = if (prefix.isEmpty()) col["name"].asText() else "$prefix.${col["name"].asText()}"
                ids[path] = col["field_id"].asInt()
                col["children"]?.let { walk(it, path) }
            }
        }
        walk(body(response)["columns"], "")
        return ids
    }

    /**
     * ClickHouse's layout for non-Nullable columns (top-level, REQUIRED) plus
     * the two nullable shapes that must keep today's refusal.
     */
    private fun schema(ids: Map<String, Int>): MessageType =
        MessageType(
            "ch",
            Types.required(PrimitiveType.PrimitiveTypeName.INT64).id(ids.getValue("id")).named("id"),
            Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                .`as`(LogicalTypeAnnotation.stringType()).id(ids.getValue("name")).named("name"),
            Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                .`as`(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MICROS))
                .id(ids.getValue("ts"))
                .named("ts"),
            Types.optional(PrimitiveType.PrimitiveTypeName.INT64).id(ids.getValue("opt")).named("opt"),
            Types.optionalGroup()
                .addField(Types.required(PrimitiveType.PrimitiveTypeName.INT64).id(ids.getValue("g.x")).named("x"))
                .id(ids.getValue("g"))
                .named("g"),
        )

    private fun write(schema: MessageType): ByteArray {
        val tmp = Files.createTempFile("hoglake-chnulls", ".parquet")
        try {
            Files.deleteIfExists(tmp)
            val factory = SimpleGroupFactory(schema)
            ExampleParquetWriter.builder(LocalOutputFile(tmp))
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
                .use { writer ->
                    for (i in 0 until ROWS) {
                        val g = factory.newGroup()
                        g.add("id", 100L + i)
                        g.add("name", "n-%02d".format(i))
                        g.add("ts", TS0 + i * 1_000_000L)
                        g.add("opt", i.toLong()) // never null, but the column COULD be
                        g.addGroup("g").add("x", i.toLong())
                        writer.write(g)
                    }
                }
            return Files.readAllBytes(tmp)
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    /**
     * [bytes] with its thrift footer rewritten so that no column chunk's
     * statistics carry `null_count`. Everything before the footer is
     * copied verbatim, so every offset the footer records stays valid.
     */
    private fun withoutNullCounts(bytes: ByteArray): ByteArray {
        val footerLen = footerSizeOf(bytes).toInt()
        val footerStart = bytes.size - 8 - footerLen
        val meta = Util.readFileMetaData(ByteArrayInputStream(bytes, footerStart, footerLen))
        for (rowGroup in meta.row_groups) {
            for (chunk in rowGroup.columns) chunk.meta_data.statistics?.unsetNull_count()
        }
        val footer = ByteArrayOutputStream().also { Util.writeFileMetaData(meta, it) }.toByteArray()
        return ByteArrayOutputStream().apply {
            write(bytes, 0, footerStart)
            write(footer)
            write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(footer.size).array())
            write("PAR1".toByteArray())
        }.toByteArray()
    }

    private fun footerSizeOf(bytes: ByteArray): Long =
        ByteBuffer.wrap(bytes, bytes.size - 8, 4).order(ByteOrder.LITTLE_ENDIAN).int.toLong()

    private data class StatsRow(
        val valueCount: Long,
        val nullCount: Long,
        val lower: ByteArray?,
        val upper: ByteArray?,
    )

    private fun statsRows(dataFileId: Long): Map<Long, StatsRow> =
        db.jdbi.withHandle<Map<Long, StatsRow>, Exception> { h ->
            h.createQuery(
                """
                SELECT field_id, value_count, null_count, lower_bound, upper_bound
                FROM hog_file_column_stats WHERE data_file_id = ?
                """,
            ).bind(0, dataFileId)
                .map { rs, _ ->
                    rs.getLong("field_id") to
                        StatsRow(
                            rs.getLong("value_count"),
                            rs.getLong("null_count"),
                            rs.getBytes("lower_bound"),
                            rs.getBytes("upper_bound"),
                        )
                }
                .list().toMap()
        }

    @Test
    fun `a writer that omits null_count still yields zone maps for required columns, over the scan plan too`() =
        api { client ->
            client.postJson("/v1/catalogs", """{"name": "$catalog", "data_path": "s3://$BUCKET/$catalog"}""")
            client.postJson("/v1/catalogs/$catalog/namespaces", """{"name": "ns"}""")
            val ids =
                createTable(
                    client,
                    "events",
                    """{"name": "id", "type": "long"},
                       {"name": "name", "type": "string"},
                       {"name": "ts", "type": "timestamp"},
                       {"name": "opt", "type": "long"},
                       {"name": "g", "type": "struct", "children": [{"name": "x", "type": "long"}]}""",
                )
            val bytes = withoutNullCounts(write(schema(ids)))

            // The fixture really is the ClickHouse shape as parquet-java
            // decodes it: min/max present, null count unset, every chunk.
            val tmp = Files.createTempFile("hoglake-chnulls-check", ".parquet")
            try {
                Files.write(tmp, bytes)
                val chunks = FooterParse.parse(LocalInputFile(tmp)).blocks.flatMap { it.columns }
                assertThat(chunks).hasSize(5)
                assertThat(chunks).allSatisfy { chunk ->
                    assertThat(chunk.statistics.isNumNullsSet).isFalse()
                    assertThat(chunk.statistics.hasNonNullValue()).isTrue()
                }
            } finally {
                Files.deleteIfExists(tmp)
            }

            val path = "s3://$BUCKET/$catalog/events/ch.parquet"
            store.put(path, bytes)
            val commit =
                client.postJson(
                    "/v1/catalogs/$catalog/commit",
                    """{"appends": [{"namespace": "ns", "table": "events", "files": [
                       {"path": "$path", "record_count": $ROWS, "file_size_bytes": ${bytes.size},
                        "footer_size": ${footerSizeOf(bytes)}}]}]}""",
                )
            assertThat(commit.status).describedAs(commit.bodyAsText()).isEqualTo(HttpStatusCode.OK)
            val fileId =
                body(client.get("$tablesUrl/events/files")).single()["data_file_id"].asLong()

            assertThat(hydrator.runOnce()).isEqualTo(1)
            val state =
                db.jdbi.withHandle<String, Exception> { h ->
                    h.createQuery("SELECT stats_state FROM hog_data_file WHERE data_file_id = ?")
                        .bind(0, fileId).mapTo(String::class.java).one()
                }
            assertThat(state).isEqualTo("provided")

            // Rows for the three required top-level columns only: `opt`
            // is optional, and `g.x` is required under an optional group,
            // so a null `g` would make it null — both unknowable.
            val stats = statsRows(fileId)
            val id = ids.getValue("id").toLong()
            val name = ids.getValue("name").toLong()
            val ts = ids.getValue("ts").toLong()
            assertThat(stats).containsOnlyKeys(id, name, ts)
            assertThat(stats.values).allSatisfy {
                assertThat(it.valueCount).isEqualTo(ROWS.toLong())
                assertThat(it.nullCount).isEqualTo(0)
            }
            assertThat(stats.getValue(id).lower).isEqualTo(IcebergSingleValue.encodeLong(100))
            assertThat(stats.getValue(id).upper).isEqualTo(IcebergSingleValue.encodeLong(100L + ROWS - 1))
            assertThat(stats.getValue(name).lower).isEqualTo(IcebergSingleValue.encodeString("n-00"))
            assertThat(stats.getValue(name).upper).isEqualTo(IcebergSingleValue.encodeString("n-19"))
            assertThat(stats.getValue(ts).lower).isEqualTo(IcebergSingleValue.encodeTimestampMicros(TS0))
            assertThat(stats.getValue(ts).upper)
                .isEqualTo(IcebergSingleValue.encodeTimestampMicros(TS0 + (ROWS - 1) * 1_000_000L))

            // And the scan plan carries them: this is what planning-time
            // pruning reads, and it used to be an empty array.
            val scan = body(client.get("$tablesUrl/events/scan?include=column_stats"))
            val entries = scan.single()["data_file"]["column_stats"]
            assertThat(entries.map { it["field_id"].asLong() }).containsExactlyInAnyOrder(id, name, ts)
            val byId = entries.associateBy { it["field_id"].asLong() }
            with(byId.getValue(id)) {
                assertThat(this["null_count"].asLong()).isEqualTo(0)
                assertThat(this["value_count"].asLong()).isEqualTo(ROWS.toLong())
                assertThat(this["lower_bound"].asLong()).isEqualTo(100)
                assertThat(this["upper_bound"].asLong()).isEqualTo(100L + ROWS - 1)
            }
            assertThat(byId.getValue(name)["lower_bound"].asText()).isEqualTo("n-00")
            assertThat(byId.getValue(name)["upper_bound"].asText()).isEqualTo("n-19")
            assertThat(byId.getValue(ts)["lower_bound"].asText()).isEqualTo("2026-09-05T00:00")
            assertThat(byId.getValue(ts)["upper_bound"].asText()).isEqualTo("2026-09-05T00:00:19")
        }
}
