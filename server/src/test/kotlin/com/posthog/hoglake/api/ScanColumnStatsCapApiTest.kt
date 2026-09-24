package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.service.ScanService
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
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * The bound on GET .../scan?include=column_stats
 * ([ScanService.SCAN_COLUMN_STATS_MAX_ENTRIES]).
 *
 * Column statistics are the one part of the API whose size is a
 * PRODUCT: provided files x requested columns. Both factors are
 * unremarkable on their own — 20,000 files is a normal table, 30
 * columns is a narrow one — and their product at the measured 106 bytes
 * per entry is 66 MB from a single GET, with nothing in the server
 * refusing it and nothing in the spec bounding it. Every other listing
 * has a ceiling (`/files` 10,000, `/snapshots` 100, verify samples 20).
 *
 * The fixture makes the two factors exactly controllable: a table of
 * [COLUMNS] columns, and files registered directly into the manifest so
 * a hundred thousand of them cost one statement rather than a hundred
 * thousand commits. The files carry NO stats rows, which is what keeps
 * this test fast and costs it nothing: the cap is decided from the file
 * and column metadata BEFORE the statistics query runs, so it is
 * measuring exactly the arithmetic that refuses the request.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScanColumnStatsCapApiTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()

    private val catalog = "scancap"
    private val tablesUrl = "/v1/catalogs/$catalog/namespaces/ns/tables"

    private companion object {
        const val COLUMNS = 100

        /** Files whose product with [COLUMNS] is exactly the cap. */
        val AT_CAP = (ScanService.SCAN_COLUMN_STATS_MAX_ENTRIES / COLUMNS).toInt()
    }

    private var catalogId = 0L
    private var firstFieldId = 0L

    @AfterAll
    fun tearDown() = db.close()

    @BeforeAll
    fun seed() =
        api { client ->
            client.postJson("/v1/catalogs", """{"name": "$catalog", "data_path": "s3://b/$catalog"}""")
            client.postJson("/v1/catalogs/$catalog/namespaces", """{"name": "ns"}""")
            val columns = (1..COLUMNS).joinToString(",") { """{"name": "c$it", "type": "long"}""" }
            // Two tables: one exactly AT the cap, one a single file over
            // it. Separate tables rather than one table grown between
            // tests, so neither depends on the order they run in.
            for (table in listOf("atcap", "overcap")) {
                val created = client.postJson("$tablesUrl", """{"name": "$table", "columns": [$columns]}""")
                assertThat(created.status).isEqualTo(HttpStatusCode.Created)
            }
            db.jdbi.useHandleUnchecked { h ->
                catalogId =
                    h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = :n")
                        .bind("n", catalog).mapTo(Long::class.java).one()
                firstFieldId =
                    h.createQuery(
                        "SELECT min(field_id) FROM hog_column WHERE catalog_id = :c",
                    ).bind("c", catalogId).mapTo(Long::class.java).one()

                fun tableId(name: String) =
                    h.createQuery(
                        "SELECT table_id FROM hog_table_version WHERE catalog_id = :c AND name = :n",
                    ).bind("c", catalogId).bind("n", name).mapTo(Long::class.java).one()

                var nextFileId = 1L
                for ((table, files) in listOf("atcap" to AT_CAP, "overcap" to AT_CAP + 1)) {
                    h.createUpdate(
                        """
                        INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                                   path, record_count, file_size_bytes, row_id_start)
                        SELECT :c, :base + g, :t, 1,
                               's3://b/$catalog/$table/' || g || '.parquet', 1, 1024, (:base + g) * 10
                        FROM generate_series(1, :n) g
                        """,
                    ).bind("c", catalogId).bind("base", nextFileId - 1).bind("t", tableId(table))
                        .bind("n", files).execute()
                    nextFileId += files
                }
            }
        }

    @Test
    fun `a request exactly at the cap is served`() =
        api { client ->
            val response = client.get("$tablesUrl/atcap/scan?include=column_stats")
            assertThat(response.status)
                .describedAs("%d files x %d columns is exactly the cap", AT_CAP, COLUMNS)
                .isEqualTo(HttpStatusCode.OK)
            val plan = json.readTree(response.bodyAsText())
            assertThat(plan).hasSize(AT_CAP)
            // The files are `provided` with no rows, so every entry is an
            // EMPTY array — present, and a different answer from absent.
            assertThat(plan.first()["data_file"]["column_stats"]).isEmpty()
        }

    @Test
    fun `one file past the cap is refused, naming the cap and the way out`() =
        api { client ->
            val refused = client.get("$tablesUrl/overcap/scan?include=column_stats")
            assertThat(refused.status)
                .describedAs("%d files x %d columns is one file past the cap", AT_CAP + 1, COLUMNS)
                .isEqualTo(HttpStatusCode.UnprocessableEntity)
            val body = refused.bodyAsText()

            // EXACT tokens, not substrings: "1000000" matches inside
            // "10000000", so a cap that had silently gained a digit
            // would pass a `contains` check.
            fun containsNumber(n: Long) = Regex("(?<![0-9])$n(?![0-9])").containsMatchIn(body)

            assertThat(containsNumber(ScanService.SCAN_COLUMN_STATS_MAX_ENTRIES))
                .describedAs("the 422 must name the cap exactly: %s", body)
                .isTrue()
            assertThat(containsNumber((AT_CAP + 1).toLong() * COLUMNS))
                .describedAs("...and the product it refused: %s", body)
                .isTrue()
            assertThat(body)
                .describedAs("a refusal that does not say what to do instead is an outage to whoever reads it")
                .contains("stats_fields")
            assertThat(body)
                .describedAs("/scan has no range or page parameter; do not send the caller looking for one")
                .doesNotContain("snapshot ranges")

            // The refusal is the STATISTICS, not the plan: the same scan
            // without them is served, so a caller that cannot narrow can
            // still read the table.
            assertThat(client.get("$tablesUrl/overcap/scan").status).isEqualTo(HttpStatusCode.OK)
        }

    @Test
    fun `stats_fields brings the same request back under the cap`() =
        api { client ->
            // The lever the refusal names, doing what it says: one column
            // instead of a hundred divides the product by a hundred.
            val narrowed =
                client.get("$tablesUrl/overcap/scan?include=column_stats&stats_fields=$firstFieldId")
            assertThat(narrowed.status).isEqualTo(HttpStatusCode.OK)
            assertThat(json.readTree(narrowed.bodyAsText())).hasSize(AT_CAP + 1)

            // The bound is arithmetic, not a waiver: one column below
            // the full width is 990,099 entries and is served; the full
            // width is 1,000,100 and is not. The two differ by one
            // column's worth of the product, which is what the cap
            // counts.
            fun columns(n: Int) = (firstFieldId until firstFieldId + n).joinToString(",")
            assertThat(
                client.get(
                    "$tablesUrl/overcap/scan?include=column_stats&stats_fields=${columns(COLUMNS - 1)}",
                ).status,
            ).isEqualTo(HttpStatusCode.OK)
            assertThat(
                client.get(
                    "$tablesUrl/overcap/scan?include=column_stats&stats_fields=${columns(COLUMNS)}",
                ).status,
            ).isEqualTo(HttpStatusCode.UnprocessableEntity)
        }

    @Test
    fun `field ids that name nothing on the table cost nothing against the cap`() =
        api { client ->
            // The bound tracks the ANSWER, not the question. An engine
            // naming ten thousand field ids of which none exist on this
            // table is asking for no entries at all, and refusing it on
            // the length of its list would refuse a request whose reply
            // is empty.
            val absent = (9_000_000L until 9_010_000L).joinToString(",")
            val response = client.get("$tablesUrl/overcap/scan?include=column_stats&stats_fields=$absent")
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            assertThat(json.readTree(response.bodyAsText()).first()["data_file"]["column_stats"]).isEmpty()
        }

    @Test
    fun `the cap counts files that have statistics, not every file in the plan`() =
        api { client ->
            // `pending` and `failed` files carry no column_stats at all,
            // so they are not part of the product. Flipping the one file
            // that puts `overcap` past the cap to `pending` brings the
            // request back inside it — which is the same arithmetic the
            // response performs, asserted against the response.
            db.jdbi.useHandleUnchecked { h ->
                h.createUpdate(
                    """
                    UPDATE hog_data_file SET stats_state = 'pending'
                    WHERE catalog_id = :c AND data_file_id = (
                        SELECT max(data_file_id) FROM hog_data_file
                        WHERE catalog_id = :c AND path LIKE '%/overcap/%')
                    """,
                ).bind("c", catalogId).execute()
            }
            try {
                val response = client.get("$tablesUrl/overcap/scan?include=column_stats")
                assertThat(response.status).isEqualTo(HttpStatusCode.OK)
                val plan = json.readTree(response.bodyAsText())
                assertThat(plan).hasSize(AT_CAP + 1)
                assertThat(plan.count { it["data_file"].has("column_stats") }).isEqualTo(AT_CAP)
            } finally {
                db.jdbi.useHandleUnchecked { h ->
                    h.createUpdate(
                        """
                        UPDATE hog_data_file SET stats_state = 'provided'
                        WHERE catalog_id = :c AND data_file_id = (
                            SELECT max(data_file_id) FROM hog_data_file
                            WHERE catalog_id = :c AND path LIKE '%/overcap/%')
                        """,
                    ).bind("c", catalogId).execute()
                }
            }
        }

    @Test
    fun `container nodes do not count against the cap, because they never have a row`() =
        api { client ->
            // A struct's own field id gets a hog_column row and a place
            // in the column forest, but never a hog_file_column_stats
            // row (a commit shipping stats for a container's field id
            // is refused by name). Counting it would refuse a request
            // on columns that cannot contribute an entry — and a deeply
            // nested table is mostly containers.
            //
            // TEN structs of ten longs: 100 leaves and 10 container
            // nodes, 110 column nodes in all. On exactly [AT_CAP]
            // files, 100 leaves x AT_CAP is the cap to the entry;
            // counting the containers too would make it 110/100 of the
            // cap, which is a 422. The margin is the containers and
            // nothing else.
            val structs =
                (1..10).joinToString(",") { g ->
                    val leaves = (1..10).joinToString(",") { """{"name": "f$it", "type": "long"}""" }
                    """{"name": "s$g", "type": "struct", "children": [$leaves]}"""
                }
            val created = client.postJson(tablesUrl, """{"name": "nested", "columns": [$structs]}""")
            assertThat(created.status).describedAs(created.bodyAsText()).isEqualTo(HttpStatusCode.Created)
            db.jdbi.useHandleUnchecked { h ->
                val tableId =
                    h.createQuery(
                        "SELECT table_id FROM hog_table_version WHERE catalog_id = :c AND name = 'nested'",
                    ).bind("c", catalogId).mapTo(Long::class.java).one()
                val base =
                    h.createQuery("SELECT max(data_file_id) FROM hog_data_file WHERE catalog_id = :c")
                        .bind("c", catalogId).mapTo(Long::class.java).one()
                h.createUpdate(
                    """
                    INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                               path, record_count, file_size_bytes, row_id_start)
                    SELECT :c, :base + g, :t, 1, 's3://b/$catalog/nested/' || g || '.parquet',
                           1, 1024, (:base + g) * 10
                    FROM generate_series(1, :n) g
                    """,
                ).bind("c", catalogId).bind("base", base).bind("t", tableId).bind("n", AT_CAP).execute()
            }
            // The fixture check: 100 leaves x AT_CAP files IS the cap.
            assertThat(AT_CAP.toLong() * 100).isEqualTo(ScanService.SCAN_COLUMN_STATS_MAX_ENTRIES)
            assertThat(client.get("$tablesUrl/nested/scan?include=column_stats").status)
                .describedAs("the ten struct nodes must not count against the cap")
                .isEqualTo(HttpStatusCode.OK)
        }

    @Test
    fun `the cap is stated in the spec with the number the server enforces`() {
        // A cap the spec does not carry is a cap callers meet as an
        // outage. Read off disk rather than restated, so the two cannot
        // drift (the pattern ScalarTypeParityTest uses).
        val spec =
            javaClass.classLoader.getResourceAsStream("openapi/hoglake.yaml")!!
                .readAllBytes().toString(Charsets.UTF_8)
        assertThat(spec)
            .describedAs("planScan's 422 must name the entry cap the server enforces")
            .contains(ScanService.SCAN_COLUMN_STATS_MAX_ENTRIES.toString())
    }

    // ---- harness -----------------------------------------------------------

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
}
