package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.HttpClient
import io.ktor.client.request.delete
import io.ktor.client.request.get
import io.ktor.client.request.post
import io.ktor.client.request.put
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Wire-level pinning for GET
 * /v1/catalogs/{c}/namespaces/{ns}/tables — the namespace listing the
 * webui renders a row per.
 *
 * Every field is asserted on the RAW JSON, by its snake_case name, with
 * its JSON type: the webui and pyhoglake both build against this shape,
 * and the server suite is the only place that reds when it moves.
 *
 * The listing is HEAD-ONLY (no snapshot / at-timestamp parameters), so
 * the property under test is not time travel but CONSISTENCY: comment,
 * aggregates and history counts all answer for the same snapshot, and
 * an alter that changes the comment is reflected the moment it lands.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TableListingApiTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()

    @AfterAll
    fun tearDown() = db.close()

    private fun api(block: suspend ApplicationTestBuilder.(HttpClient) -> Unit) =
        testApplication {
            application { app.module(this) }
            block(client)
        }

    private suspend fun body(r: HttpResponse): JsonNode = json.readTree(r.bodyAsText())

    private suspend fun HttpClient.postJson(
        url: String,
        body: String,
    ): HttpResponse =
        post(url) {
            contentType(ContentType.Application.Json)
            setBody(body)
        }

    /** The alter surface: POST .../tables/{table}/alter. */
    private suspend fun HttpClient.alter(
        table: String,
        ops: String,
    ): HttpResponse = postJson("$tables/$table/alter", """{"ops":[$ops]}""")

    private val tables = "/v1/catalogs/listing/namespaces/ns/tables"

    /** The listing, by table name. */
    private suspend fun listed(client: HttpClient): Map<String, JsonNode> =
        body(client.get(tables)).associateBy { it["name"].asText() }

    private var built = false

    /**
     * Four tables that differ in exactly the ways the listing reports:
     * `commented` carries a comment and two appends, `bare` carries
     * neither, `churned` is appended to and then TRUNCATED, and `late`
     * exists only for the alter test — which MUTATES its row, so it must
     * not be a table another test states a number for. Test methods in
     * one class have no guaranteed order; sharing `bare` here made the
     * history-count test pass or fail on the order JUnit happened to
     * pick.
     */
    private fun ensureFixture() =
        api { client ->
            if (built) return@api
            client.postJson("/v1/catalogs", """{"name":"listing","data_path":"s3://listing"}""")
            client.postJson("/v1/catalogs/listing/namespaces", """{"name":"ns"}""")
            client.postJson(tables, """{"name":"commented","columns":[{"name":"id","type":"long"}]}""")
            client.postJson(tables, """{"name":"bare","columns":[{"name":"id","type":"long"}]}""")
            client.postJson(tables, """{"name":"late","columns":[{"name":"id","type":"long"}]}""")
            val churned =
                body(
                    client.postJson(
                        tables,
                        """{"name":"churned","columns":[{"name":"id","type":"long"}]}""",
                    ),
                )["table_uuid"].asText()
            client.alter("commented", """{"op":"set_table_comment","comment":"raw pageview events"}""")
            client.postJson(
                "/v1/catalogs/listing/commit",
                """{"appends":[{"namespace":"ns","table":"commented","files":[
                   {"path":"s3://listing/data/a.parquet","record_count":10,"file_size_bytes":1000}]}]}""",
            )
            client.postJson(
                "/v1/catalogs/listing/commit",
                """{"appends":[{"namespace":"ns","table":"commented","files":[
                   {"path":"s3://listing/data/b.parquet","record_count":5,"file_size_bytes":500}]}]}""",
            )
            client.postJson(
                "/v1/catalogs/listing/commit",
                """{"appends":[{"namespace":"ns","table":"churned","files":[
                   {"path":"s3://listing/data/c.parquet","record_count":7,"file_size_bytes":700}]}]}""",
            )
            // Truncate: ONE snapshot carrying TWO table-scoped change
            // rows (table_altered + table_deleted_from), which is the
            // shape `count(DISTINCT snapshot_id)` exists for, and it
            // end-snapshots every live file, which is the shape the
            // visibility predicate exists for. Both come from the real
            // service rather than from seeded rows.
            client.postJson("$tables/churned/truncate?expected_table_uuid=$churned", "")
            built = true
        }

    @Test
    fun `a table with a comment lists it and one without omits it`() {
        ensureFixture()
        api { client ->
            val rows = listed(client)
            // Only the tables this test can name: the two cases below
            // create their own, and `doomed` may already be gone.
            assertThat(rows.keys).contains("bare", "churned", "commented", "late")

            assertThat(rows["commented"]!!["comment"].asText()).isEqualTo("raw pageview events")
            // Absent, not JSON null: the spec says the property is absent
            // when unset, and a strict generated client reads the two
            // differently even though a tolerant one does not.
            assertThat(rows["bare"]!!.has("comment"))
                .describedAs("a table with no comment must omit the property, not null it")
                .isFalse()
        }
    }

    @Test
    fun `the aggregates are the live files at head, per table`() {
        ensureFixture()
        api { client ->
            val rows = listed(client)
            val commented = rows["commented"]!!
            assertThat(commented["record_count"].asLong()).isEqualTo(15)
            assertThat(commented["file_count"].asLong()).isEqualTo(2)
            assertThat(commented["file_size_bytes"].asLong()).isEqualTo(1500)
            // Per TABLE, not per namespace: the appends all landed on
            // `commented`, and a rollup that leaked across the lateral
            // join would give `bare` the same numbers.
            val bare = rows["bare"]!!
            assertThat(bare["record_count"].asLong()).isZero()
            assertThat(bare["file_count"].asLong()).isZero()
            assertThat(bare["file_size_bytes"].asLong()).isZero()

            // Truncate ended every one of `churned`'s files, so its
            // rollup is back to zero while the rows stay in the manifest:
            // a listing that ignored end_snapshot would still report the
            // append.
            val churned = rows["churned"]!!
            assertThat(churned["file_count"].asLong()).isZero()
            assertThat(churned["record_count"].asLong()).isZero()
            assertThat(churned["file_size_bytes"].asLong()).isZero()

            // The same numbers the full Table object reports at head —
            // read from the other endpoint rather than restated, because
            // "agrees with GET table" is the actual contract.
            val table = body(client.get("$tables/commented"))
            for (f in listOf("record_count", "file_count", "file_size_bytes")) {
                assertThat(commented[f].asLong())
                    .describedAs("listing's %s must equal the Table object's", f)
                    .isEqualTo(table[f].asLong())
            }
        }
    }

    @Test
    fun `the history counts are this table's retained change rows`() {
        ensureFixture()
        api { client ->
            val rows = listed(client)
            // `commented`: created, altered, appended, appended = 4
            // snapshots carrying a change row for it. `bare`: created = 1.
            assertThat(rows["commented"]!!["snapshot_count"].asLong()).isEqualTo(4)
            assertThat(rows["bare"]!!["snapshot_count"].asLong()).isEqualTo(1)
            // `churned`: created, appended, truncated. The truncate is
            // ONE snapshot carrying TWO table-scoped change rows, so a
            // count that forgot DISTINCT would say 4 here — snapshots,
            // not change rows, are what this counts.
            assertThat(rows["churned"]!!["snapshot_count"].asLong()).isEqualTo(3)

            // Catalog snapshots run 0 (empty) 1 (namespace) 2 (commented
            // created) 3 (bare) 4 (late) 5 (alter) 6, 7 (appends), so the
            // earliest snapshot each table can be read at is its create —
            // and `bare`'s is strictly the LATER of the two, which a
            // query that ignored object_id would get wrong.
            val commentedEarliest = rows["commented"]!!["earliest_snapshot_id"].asLong()
            val bareEarliest = rows["bare"]!!["earliest_snapshot_id"].asLong()
            assertThat(bareEarliest).isGreaterThan(commentedEarliest)
            // The namespace's own creation snapshot is NOT a table's:
            // `object_id` spans three id spaces and namespace 1 shares an
            // id with a table. Whatever the ids are, no table may claim
            // the snapshot the namespace was made in.
            val namespaceSnapshot = 1L
            assertThat(listOf(commentedEarliest, bareEarliest)).doesNotContain(namespaceSnapshot)
        }
    }

    @Test
    fun `an alter that sets the comment shows up in the listing at head`() {
        ensureFixture()
        api { client ->
            val before = listed(client)["late"]!!
            assertThat(before.has("comment")).isFalse()
            val beforeSnapshots = before["snapshot_count"].asLong()

            val altered =
                client.alter("late", """{"op":"set_table_comment","comment":"filled in later"}""")
            assertThat(altered.status).isEqualTo(HttpStatusCode.OK)

            val after = listed(client)["late"]!!
            assertThat(after["comment"].asText()).isEqualTo("filled in later")
            // The alter is a commit, so it is also a snapshot this table
            // now carries a change row in.
            assertThat(after["snapshot_count"].asLong()).isEqualTo(beforeSnapshots + 1)
            // ...and clearing it takes the property back off the wire.
            client.alter("late", """{"op":"set_table_comment","comment":null}""")
            assertThat(listed(client)["late"]!!.has("comment")).isFalse()
        }
    }

    @Test
    fun `a dropped table leaves the listing entirely`() {
        ensureFixture()
        api { client ->
            // Its own table, for the same reason `late` has one: this
            // case ENDS a row, and test order is not guaranteed.
            client.postJson(tables, """{"name":"doomed","columns":[{"name":"id","type":"long"}]}""")
            assertThat(listed(client)).containsKey("doomed")

            assertThat(client.delete("$tables/doomed").status).isEqualTo(HttpStatusCode.OK)

            val rows = listed(client)
            assertThat(rows)
                .describedAs(
                    "a dropped table's version row is end-snapshotted, not deleted, and its " +
                        "change rows stay in the log — so it disappears from the listing only " +
                        "because the version predicate is evaluated at the snapshot, and would " +
                        "otherwise keep listing with a full history",
                )
                .doesNotContainKey("doomed")
            // The tables around it are untouched.
            assertThat(rows).containsKeys("bare", "churned", "commented", "late")
        }
    }

    @Test
    fun `a replaced table lists the new incarnation's history, not its predecessor's`() {
        ensureFixture()
        api { client ->
            // Build up a history worth losing: created, appended twice.
            client.postJson(tables, """{"name":"phoenix","columns":[{"name":"id","type":"long"}]}""")
            for (f in listOf("p1", "p2")) {
                client.postJson(
                    "/v1/catalogs/listing/commit",
                    """{"appends":[{"namespace":"ns","table":"phoenix","files":[
                       {"path":"s3://listing/data/$f.parquet","record_count":4,"file_size_bytes":400}]}]}""",
                )
            }
            val before = listed(client)["phoenix"]!!
            assertThat(before["snapshot_count"].asLong()).isEqualTo(3)
            val oldUuid = before["table_uuid"].asText()
            val head = body(client.get("/v1/catalogs/listing"))["head_snapshot_id"].asLong()

            // Atomic replacement: a NEW table_uuid at one snapshot.
            val path = "/v1/catalogs/listing/table-creations/${java.util.UUID.randomUUID()}"
            val prepared =
                client.put(path) {
                    contentType(ContentType.Application.Json)
                    setBody(
                        """{"namespace":"ns","name":"phoenix",
                           "columns":[{"name":"id","type":"long"}],
                           "replacement":{"expected_table_uuid":"$oldUuid","read_snapshot":$head}}""",
                    )
                }
            assertThat(prepared.status).describedAs("prepare the replacement").isEqualTo(HttpStatusCode.OK)
            assertThat(client.postJson("$path/commit", """{"files":[]}""").status)
                .isEqualTo(HttpStatusCode.OK)

            val after = listed(client)["phoenix"]!!
            assertThat(after["table_uuid"].asText())
                .describedAs("replacement mints a new incarnation")
                .isNotEqualTo(oldUuid)
            // The predecessor's three snapshots belong to the RETIRED
            // table_id and must not follow the name. Its own snapshot —
            // the replacement — is the only one this row has, even though
            // that one snapshot carries three table-scoped change rows
            // (created + altered + deleted_from), which is also why
            // DISTINCT is not optional.
            assertThat(after["snapshot_count"].asLong()).isEqualTo(1)
            assertThat(after["earliest_snapshot_id"].asLong()).isGreaterThan(head)
            // ...and the predecessor's files went with it.
            assertThat(after["file_count"].asLong()).isZero()
            assertThat(after["record_count"].asLong()).isZero()
        }
    }

    @Test
    fun `every field of every row is present and typed as the spec declares`() {
        ensureFixture()
        api { client ->
            for ((name, row) in listed(client)) {
                assertThat(row["name"].isTextual).describedAs("%s.name", name).isTrue()
                assertThat(row["table_uuid"].isTextual).describedAs("%s.table_uuid", name).isTrue()
                // int64 fields travel as JSON numbers, not strings.
                for (f in listOf("record_count", "file_count", "file_size_bytes", "snapshot_count")) {
                    assertThat(row[f]).describedAs("%s.%s present", name, f).isNotNull()
                    assertThat(row[f]!!.isNumber).describedAs("%s.%s is a JSON number", name, f).isTrue()
                }
                assertThat(row["earliest_snapshot_id"].isNumber)
                    .describedAs("%s.earliest_snapshot_id", name)
                    .isTrue()
                // No camelCase leak, and nothing the spec does not declare.
                assertThat(row.fieldNames().asSequence().toList())
                    .allSatisfy { assertThat(it).matches("[a-z][a-z0-9_]*") }
            }
        }
    }
}
