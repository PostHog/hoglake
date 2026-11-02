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
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.io.LocalOutputFile
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.file.Files
import java.util.UUID

/**
 * Deferred stats on the PREPARED commit (`POST /commit/prepared`, what
 * pyhoglake's `prepare_append_*(deferred_stats=True)` +
 * `Catalog.commit_prepared` send). A registration without
 * `column_stats` lands `stats_state='pending'`; one with them — or with
 * `[]` — lands `'provided'` in the same commit and is never claimed. One
 * hydrator sweep reads the pending file's footer outside the commit lock:
 * field ids audited, split offsets stored, stats written (or, under
 * [Hydrator.DEFAULT_MIN_FILE_BYTES], none). add_column is NOT held up
 * by the pending file (#445): the guard refuses only over a file the
 * sweep has flagged, so it goes through before the sweep and after.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PreparedCommitDeferredStatsHydrationIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val hydrator by lazy { Hydrator(db.jdbi, store) }
    private val json = ObjectMapper()

    private val catalog = "prepdeferred"
    private val tablesUrl = "/v1/catalogs/$catalog/namespaces/ns/tables"

    private companion object {
        const val BUCKET = "hoglake-test"
        const val ROWS = 20

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

    /** `id` = [base] until [base] + ROWS - 1, field id [idFieldId] — or no id at all when [withId] is false. */
    private fun write(
        idFieldId: Int,
        base: Long,
        withId: Boolean = true,
    ): ByteArray {
        val schema =
            MessageType(
                "prep",
                Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                    .let { if (withId) it.id(idFieldId) else it }
                    .named("id"),
            )
        val tmp = Files.createTempFile("hoglake-prepdeferred", ".parquet")
        try {
            Files.deleteIfExists(tmp)
            val factory = SimpleGroupFactory(schema)
            ExampleParquetWriter.builder(LocalOutputFile(tmp))
                .withType(schema)
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build()
                .use { writer ->
                    for (i in 0 until ROWS) writer.write(factory.newGroup().append("id", base + i))
                }
            return Files.readAllBytes(tmp)
        } finally {
            Files.deleteIfExists(tmp)
        }
    }

    private fun footerSizeOf(bytes: ByteArray): Long =
        ByteBuffer.wrap(bytes, bytes.size - 8, 4).order(ByteOrder.LITTLE_ENDIAN).int.toLong()

    private fun statsStates(): Map<String, String> =
        db.jdbi.withHandle<Map<String, String>, Exception> { h ->
            h.createQuery(
                """
                SELECT f.path, f.stats_state FROM hog_data_file f
                JOIN hog_catalog c USING (catalog_id) WHERE c.name = ?
                """,
            ).bind(0, catalog)
                .map { rs, _ -> rs.getString(1) to rs.getString(2) }
                .list().toMap()
        }

    private fun fileRow(path: String): Map<String, Any?> =
        db.jdbi.withHandle<Map<String, Any?>, Exception> { h ->
            h.createQuery(
                """
                SELECT catalog_id, data_file_id, missing_field_ids,
                       coalesce(cardinality(split_offsets), 0) AS split_offset_count
                FROM hog_data_file WHERE path = ?
                """,
            ).bind(0, path).mapToMap().one()
        }

    /** Stats rows of the file at [path]; by path, so another catalog's same data_file_id is never counted. */
    private fun statsRowCount(path: String): Long =
        db.jdbi.withHandle<Long, Exception> { h ->
            h.createQuery(
                """
                SELECT count(*) FROM hog_file_column_stats s
                JOIN hog_data_file f USING (catalog_id, data_file_id) WHERE f.path = ?
                """,
            ).bind(0, path).mapTo(Long::class.java).one()
        }

    private fun lastSweep(): JsonNode =
        db.jdbi.withHandle<String, Exception> { h ->
            h.createQuery(
                """
                SELECT r.result::text FROM hog_maintenance_run r JOIN hog_catalog c USING (catalog_id)
                WHERE c.name = ? ORDER BY r.run_id DESC LIMIT 1
                """,
            ).bind(0, catalog).mapTo(String::class.java).one()
        }.let { json.readTree(it) }

    /** One file registration; [stats] null omits `column_stats` entirely. */
    private fun registration(
        path: String,
        bytes: ByteArray,
        stats: String?,
    ): String =
        """{"path": "$path", "record_count": $ROWS, "file_size_bytes": ${bytes.size},
            "footer_size": ${footerSizeOf(bytes)}""" +
            (if (stats == null) "" else """, "column_stats": $stats""") + "}"

    private suspend fun HttpClient.commitPrepared(
        tableUuid: String,
        vararg files: String,
        cat: String = catalog,
    ) {
        val head = body(get("/v1/catalogs/$cat"))["head_snapshot_id"].asLong()
        val commit =
            postJson(
                "/v1/catalogs/$cat/commit/prepared",
                """{"idempotency_key": "${UUID.randomUUID()}", "read_snapshot": $head,
                    "appends": [{"namespace": "ns", "table": "events",
                                 "expected_table_uuid": "$tableUuid",
                                 "files": [${files.joinToString(",")}]}]}""",
            )
        assertThat(commit.status).describedAs(commit.bodyAsText()).isEqualTo(HttpStatusCode.OK)
    }

    private suspend fun HttpClient.addColumn(
        name: String,
        cat: String = catalog,
    ): HttpResponse =
        postJson(
            "/v1/catalogs/$cat/namespaces/ns/tables/events/alter",
            """{"ops": [{"op": "add_column", "column": {"name": "$name", "type": "long"}}]}""",
        )

    /** Catalog [cat] with `ns.events(id long)`; returns (table_uuid, id field id). */
    private suspend fun HttpClient.createEvents(cat: String): Pair<String, Int> {
        postJson("/v1/catalogs", """{"name": "$cat", "data_path": "s3://$BUCKET/$cat"}""")
        postJson("/v1/catalogs/$cat/namespaces", """{"name": "ns"}""")
        val created =
            postJson(
                "/v1/catalogs/$cat/namespaces/ns/tables",
                """{"name": "events", "columns": [{"name": "id", "type": "long"}]}""",
            )
        assertThat(created.status).describedAs(created.bodyAsText()).isEqualTo(HttpStatusCode.Created)
        val table = body(created)
        return table["table_uuid"].asText() to table["columns"].single()["field_id"].asInt()
    }

    @Test
    fun `a prepared registration without column_stats lands pending and one hydrator sweep provides it`() =
        api { client ->
            client.postJson("/v1/catalogs", """{"name": "$catalog", "data_path": "s3://$BUCKET/$catalog"}""")
            client.postJson("/v1/catalogs/$catalog/namespaces", """{"name": "ns"}""")
            val created =
                client.postJson(tablesUrl, """{"name": "events", "columns": [{"name": "id", "type": "long"}]}""")
            assertThat(created.status).describedAs(created.bodyAsText()).isEqualTo(HttpStatusCode.Created)
            val table = body(created)
            val tableUuid = table["table_uuid"].asText()
            val idField = table["columns"].single()["field_id"].asInt()

            // Three registrations: stats absent (deferred), stats shipped,
            // and stats shipped EMPTY. Only absent means "hydrate me".
            val deferred = write(idField, base = 100)
            val shipped = write(idField, base = 500)
            val empty = write(idField, base = 700)
            val deferredPath = "s3://$BUCKET/$catalog/data/ns/events/deferred.parquet"
            val shippedPath = "s3://$BUCKET/$catalog/data/ns/events/shipped.parquet"
            val emptyPath = "s3://$BUCKET/$catalog/data/ns/events/empty.parquet"
            store.put(deferredPath, deferred)
            store.put(shippedPath, shipped)
            store.put(emptyPath, empty)
            client.commitPrepared(
                tableUuid,
                registration(deferredPath, deferred, stats = null),
                registration(
                    shippedPath,
                    shipped,
                    stats = """[{"field_id": $idField, "value_count": $ROWS, "null_count": 0}]""",
                ),
                registration(emptyPath, empty, stats = "[]"),
            )
            assertThat(statsStates()).containsExactlyInAnyOrderEntriesOf(
                mapOf(deferredPath to "pending", shippedPath to "provided", emptyPath to "provided"),
            )

            // The deferred file is pending and its footer unread; it is
            // trusted on field ids the way the shipped-stats files are
            // (#445), so add_column goes through now. MUTATION: restore
            // the pending clause in AlterService.requireFieldIds and
            // this is a 409 `idless_files_present`.
            val whilePending = client.addColumn("extra_while_pending")
            assertThat(whilePending.status).describedAs(whilePending.bodyAsText()).isEqualTo(HttpStatusCode.OK)

            // Exactly the absent-stats file is the sweep's work: the `[]`
            // file is provided-with-no-stats and never claimed.
            assertThat(hydrator.runOnce()).isEqualTo(1)
            assertThat(statsStates()).containsExactlyInAnyOrderEntriesOf(
                mapOf(deferredPath to "provided", shippedPath to "provided", emptyPath to "provided"),
            )
            assertThat(statsRowCount(emptyPath)).isZero()

            // What the deferred file's footer read decided: ids verified,
            // row-group offsets stored, and the stats themselves.
            val deferredRow = fileRow(deferredPath)
            assertThat(deferredRow["missing_field_ids"]).isEqualTo(false)
            assertThat(deferredRow["split_offset_count"] as Int).isPositive()
            val deferredId = deferredRow["data_file_id"] as Long
            val row =
                db.jdbi.withHandle<Map<String, Any?>, Exception> { h ->
                    h.createQuery(
                        """
                        SELECT value_count, null_count, lower_bound, upper_bound
                        FROM hog_file_column_stats
                        WHERE catalog_id = ? AND data_file_id = ? AND field_id = ?
                        """,
                    ).bind(0, deferredRow["catalog_id"] as Long).bind(1, deferredId).bind(2, idField.toLong())
                        .mapToMap().one()
                }
            assertThat(row["value_count"]).isEqualTo(ROWS.toLong())
            assertThat(row["null_count"]).isEqualTo(0L)
            assertThat(row["lower_bound"] as ByteArray).isEqualTo(IcebergSingleValue.encodeLong(100))
            assertThat(row["upper_bound"] as ByteArray).isEqualTo(IcebergSingleValue.encodeLong(100L + ROWS - 1))

            // Hydrated with ids found, the file stays unflagged and DDL
            // still goes through.
            val added = client.addColumn("extra")
            assertThat(added.status).describedAs(added.bodyAsText()).isEqualTo(HttpStatusCode.OK)

            // Under the production floor (HOGLAKE_HYDRATOR_MIN_FILE_BYTES,
            // 1 MiB) a deferred file is read and flipped but gets NO stats
            // rows: deferring a small file means no bounds until compaction.
            val tiny = write(idField, base = 900)
            val tinyPath = "s3://$BUCKET/$catalog/data/ns/events/tiny.parquet"
            store.put(tinyPath, tiny)
            client.commitPrepared(tableUuid, registration(tinyPath, tiny, stats = null))
            assertThat(statsStates()[tinyPath]).isEqualTo("pending")
            assertThat(Hydrator(db.jdbi, store, minFileBytes = Hydrator.DEFAULT_MIN_FILE_BYTES).runOnce())
                .isEqualTo(1)
            assertThat(statsStates()[tinyPath]).isEqualTo("provided")
            val tinyRow = fileRow(tinyPath)
            assertThat(tinyRow["missing_field_ids"]).isEqualTo(false)
            assertThat(statsRowCount(tinyPath)).isZero()
            assertThat(lastSweep()["stats_skipped_below_floor"].asLong()).isEqualTo(1)
        }

    @Test
    fun `a flagged file refuses add_column on the wire with idless_files_present`() =
        api { client ->
            // THE 409 ON THE WIRE: a deferred file written WITHOUT field
            // ids, once the sweep has flagged it, refuses add_column with
            // the typed code and a detail naming the id-less file. The
            // refuse half only (the case above is the allow half):
            // MUTATION: negate the missing_field_ids predicate and this
            // is a 200.
            val cat = "prepidless"
            val (tableUuid, idField) = client.createEvents(cat)
            val idless = write(idField, base = 1_100, withId = false)
            val idlessPath = "s3://$BUCKET/$cat/data/ns/events/idless.parquet"
            store.put(idlessPath, idless)
            client.commitPrepared(tableUuid, registration(idlessPath, idless, stats = null), cat = cat)

            assertThat(hydrator.runOnce()).isEqualTo(1)
            assertThat(fileRow(idlessPath)["missing_field_ids"]).isEqualTo(true)
            val refused = client.addColumn("extra_after_flag", cat)
            assertThat(refused.status).describedAs(refused.bodyAsText()).isEqualTo(HttpStatusCode.Conflict)
            val refusal = body(refused)
            assertThat(refusal["error"].asText()).isEqualTo("idless_files_present")
            assertThat(refusal["detail"].asText())
                .contains("1 id-less")
                .doesNotContain("not-yet-hydrated")
        }
}
