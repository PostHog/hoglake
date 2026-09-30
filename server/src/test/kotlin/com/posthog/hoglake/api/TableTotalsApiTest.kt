package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.testing.publishMaintenanceSample
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
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * GET .../tables/{table} on the WIRE, against every answer it can give
 * about a table's totals (#232).
 *
 * The endpoint used to aggregate every live `hog_data_file` row of the
 * table on every call — ~10M rows on gigahog-prod-us's busiest table,
 * 9 s under load, charged against a 10-thread request pool, with the
 * writer fleet as the main caller and none of the numbers ever read. It
 * now reads the MAINTENANCE SAMPLER's published generation, which has
 * already walked those files once and stored these three measures per
 * bucket.
 *
 * Each case below is a different CLAIM about the numbers:
 *
 *  - uncovered: absent, meaning "not yet sampled", never 0;
 *  - covered and empty: real zeros — the case that makes absence mean
 *    something;
 *  - covered: the sums plus `totals_snapshot_id` / `totals_as_of`;
 *  - time travel: the exact aggregate, no freshness fields;
 *  - `totals=false`: absent, and no file read behind them at all.
 *
 * And `read_snapshot_id`, which is about the read rather than the
 * totals and is present on all five.
 *
 * Asserted on the RAW JSON by snake_case key, with presence and absence
 * distinguished: pyhoglake, the webui and the duckdb client all build
 * against this shape, and the server suite is the only place that reds
 * when it moves.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TableTotalsApiTest {
    private val db = PgTestSupport.freshDatabase()

    /**
     * No loops run in a test fixture ([App.startBackground] is never
     * called), so the sampler ticks only when a test asks it to
     * ([publishMaintenanceSample]). That is what makes "before the
     * sample" and "after the sample" two states a test can be in rather
     * than a race against a background thread.
     */
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()

    private val tables = "/v1/catalogs/tot/namespaces/ns/tables"
    private val table = "$tables/events"

    /** The five fields that describe the TOTALS, absent or present together. */
    private val totalsFields =
        listOf("record_count", "file_count", "file_size_bytes", "totals_snapshot_id", "totals_as_of")

    @AfterAll
    fun tearDown() = db.close()

    private fun api(
        target: App = app,
        block: suspend ApplicationTestBuilder.(HttpClient) -> Unit,
    ) = testApplication {
        application { target.module(this) }
        block(client)
    }

    private suspend fun body(r: HttpResponse): JsonNode = json.readTree(r.bodyAsText())

    private suspend fun HttpClient.postJson(
        url: String,
        payload: String,
    ): HttpResponse =
        post(url) {
            contentType(ContentType.Application.Json)
            setBody(payload)
        }

    /** Append one file to `events`, returning the snapshot the commit made. */
    private suspend fun HttpClient.append(
        name: String,
        records: Long,
        bytes: Long,
        table: String = "events",
    ): Long =
        body(
            postJson(
                "/v1/catalogs/tot/commit",
                """{"appends":[{"namespace":"ns","table":"$table","files":[
                   {"path":"s3://tot/data/$name.parquet","record_count":$records,
                    "file_size_bytes":$bytes}]}]}""",
            ),
        )["snapshot_id"].asLong()

    private suspend fun HttpClient.head(): Long = body(get("/v1/catalogs/tot"))["head_snapshot_id"].asLong()

    private var built = false

    /**
     * The catalog, the namespace, `events` with one file — and ONE
     * PUBLISHED GENERATION, so every test below this line starts from a
     * COVERED table.
     *
     * The publish is part of the fixture rather than of each test
     * because JUnit gives method order no guarantee: a test asserting
     * "covered" cannot share a database with one asserting "not yet
     * sampled", so the uncovered cases bring their own database (see
     * the lifecycle test and the created-since test).
     */
    private fun ensureFixture() =
        api { client ->
            if (built) return@api
            client.postJson("/v1/catalogs", """{"name":"tot","data_path":"s3://tot"}""")
            client.postJson("/v1/catalogs/tot/namespaces", """{"name":"ns"}""")
            client.postJson(tables, """{"name":"events","columns":[{"name":"id","type":"long"}]}""")
            client.postJson(tables, """{"name":"empty","columns":[{"name":"id","type":"long"}]}""")
            client.append("seed", records = 3, bytes = 300)
            publishMaintenanceSample(db.jdbi)
            built = true
        }

    // ---- the whole lifecycle, on its own database ---------------------------

    @Test
    fun `the whole lifecycle of one table's totals`() {
        // ONE TEST ON ITS OWN DATABASE, and both halves are the test's
        // premise rather than tidiness.
        //
        // ONE TEST: "the GET does not see a file the sampler has not
        // seen" is a statement about the ORDER of a commit, a GET, a
        // sample and another GET. Split across methods it would depend
        // on JUnit's method order, which is not specified.
        //
        // ITS OWN DATABASE: the first assertion is that an UNCOVERED
        // table reports nothing, and "no published generation" is a
        // once-per-database state that any other test's sample destroys.
        PgTestSupport.freshDatabase().use { own ->
            lifecycle(own, App.build(Config(hydratorIntervalMs = 0), own.jdbi))
        }
    }

    private fun lifecycle(
        own: PgTestSupport.TestDb,
        ownApp: App,
    ) {
        api(ownApp) { client ->
            client.postJson("/v1/catalogs", """{"name":"tot","data_path":"s3://tot"}""")
            client.postJson("/v1/catalogs/tot/namespaces", """{"name":"ns"}""")
            client.postJson(tables, """{"name":"events","columns":[{"name":"id","type":"long"}]}""")

            // --- 1. no published generation: absent, and NOT zero -----
            val uncovered = body(client.get(table))
            for (field in totalsFields) {
                assertThat(uncovered.has(field))
                    .describedAs(
                        "the sampler has never published for this catalog, so the response must " +
                            "say nothing rather than claim 0 — an unsampled table and an empty " +
                            "one are different facts ('%s' was present as %s)",
                        field,
                        uncovered[field],
                    )
                    .isFalse()
            }
            // The identity is still there, `read_snapshot_id` included:
            // this is a schema response that happens to carry no numbers.
            assertThat(uncovered["table_uuid"].asText()).isNotBlank()
            assertThat(uncovered["columns"]).hasSize(1)
            assertThat(uncovered["read_snapshot_id"].asLong()).isEqualTo(client.head())

            // --- 2. covered: the sample's sums, with their snapshot ---
            client.append("a", records = 10, bytes = 1_000)
            client.append("b", records = 5, bytes = 500)
            val sampledAtSnapshot = client.head()
            publishMaintenanceSample(own.jdbi)

            val sampled = body(client.get(table))
            assertThat(sampled["record_count"].asLong()).isEqualTo(15)
            assertThat(sampled["file_count"].asLong()).isEqualTo(2)
            assertThat(sampled["file_size_bytes"].asLong()).isEqualTo(1_500)
            assertThat(sampled["totals_snapshot_id"].asLong())
                .describedAs(
                    "the numbers are exact AS OF the snapshot the sampler's scan captured, and " +
                        "saying which one is what lets a caller reconcile them against a " +
                        "time-travel read",
                )
                .isEqualTo(sampledAtSnapshot)
            assertThat(sampled["totals_as_of"].asText())
                .describedAs("and when that snapshot was captured, so a UI can print an age")
                .isNotBlank()

            // --- 3. THE SAMPLE IS THE SOURCE --------------------------
            // A third file lands. The head GET must still report the
            // OLD numbers, because it reads the published generation and
            // nothing has re-sampled. This is the assertion that proves
            // the endpoint stopped touching hog_data_file: revert
            // describeTable to aggregateAt and these numbers move.
            val snapshotWithThree = client.append("c", records = 100, bytes = 10_000)
            val stale = body(client.get(table))
            assertThat(stale["record_count"].asLong())
                .describedAs("a head read answers from the published sample, not from the manifest")
                .isEqualTo(15)
            assertThat(stale["file_count"].asLong()).isEqualTo(2)
            assertThat(stale["totals_snapshot_id"].asLong()).isEqualTo(sampledAtSnapshot)
            assertThat(stale["read_snapshot_id"].asLong())
                .describedAs(
                    "read_snapshot_id is about the READ and moves with head, which is exactly " +
                        "why it is not the totals' freshness field",
                )
                .isEqualTo(snapshotWithThree)

            // --- 4. time travel is EXACT ------------------------------
            val exact = body(client.get("$table?snapshot=$snapshotWithThree"))
            assertThat(exact["record_count"].asLong())
                .describedAs(
                    "a past snapshot has no sampled answer — the sampler measures the files live " +
                        "at its own snapshot — so this path still aggregates the manifest and " +
                        "sees all three files",
                )
                .isEqualTo(115)
            assertThat(exact["file_count"].asLong()).isEqualTo(3)
            assertThat(exact["file_size_bytes"].asLong()).isEqualTo(11_500)
            assertThat(exact.has("totals_snapshot_id"))
                .describedAs("an exact number has no age; a freshness field here would imply it can be stale")
                .isFalse()
            assertThat(exact.has("totals_as_of")).isFalse()
            assertThat(exact["read_snapshot_id"].asLong())
                .describedAs("the read WAS resolved at the snapshot asked for")
                .isEqualTo(snapshotWithThree)

            // --- and a new generation catches the sample up -----------
            publishMaintenanceSample(own.jdbi)
            val fresh = body(client.get(table))
            assertThat(fresh["record_count"].asLong()).isEqualTo(115)
            assertThat(fresh["file_count"].asLong()).isEqualTo(3)
            assertThat(fresh["totals_snapshot_id"].asLong())
                .describedAs("a new generation, so a new snapshot")
                .isEqualTo(snapshotWithThree)
        }
    }

    // ---- covered-but-empty: the case that gives absence its meaning ---------

    @Test
    fun `a covered table with no files reports zeros, not absence`() {
        ensureFixture()
        api { client ->
            // The sampler writes a tier row only for a bucket that HAS
            // files, so `empty` has no tier row at all — the same
            // absence as a table the generation never saw. The read
            // separates them with the table's own created_snapshot, and
            // this is the half that would silently read as "not yet
            // sampled" if that check were dropped.
            val info = body(client.get("$tables/empty"))
            assertThat(info["record_count"].asLong()).isZero()
            assertThat(info["file_count"].asLong()).isZero()
            assertThat(info["file_size_bytes"].asLong()).isZero()
            assertThat(info["totals_snapshot_id"].asLong())
                .describedAs("zeros are a MEASUREMENT, so they are dated like any other sample")
                .isGreaterThan(0)
        }
    }

    @Test
    fun `a table created after the published generation reads as not sampled`() {
        ensureFixture()
        api { client ->
            // The other side of the same check. The generation's scan
            // ran before this table existed, so it has no tier row for
            // the same reason `empty` has none — and here the honest
            // answer is "unknown", not 0.
            client.postJson(tables, """{"name":"latecomer","columns":[{"name":"id","type":"long"}]}""")
            client.append("late", records = 9, bytes = 90, table = "latecomer")
            val info = body(client.get("$tables/latecomer"))
            for (field in totalsFields) {
                assertThat(info.has(field))
                    .describedAs(
                        "created above the published generation's snapshot, so the scan never " +
                            "saw it: '%s' must be absent, not 0",
                        field,
                    )
                    .isFalse()
            }
            // And the next generation covers it.
            publishMaintenanceSample(db.jdbi)
            val after = body(client.get("$tables/latecomer"))
            assertThat(after["record_count"].asLong()).isEqualTo(9)
        }
    }

    @Test
    fun `at_timestamp alone takes the exact path, like snapshot`() {
        ensureFixture()
        api { client ->
            // THE ONE-SIDED HALF OF THE PREDICATE. `timeTravel` is
            // `snapshot != null || atTimestamp != null`, and every other
            // test here names a snapshot — so dropping the SECOND term
            // would leave the suite green while an at_timestamp read
            // returned the live SAMPLE, dated with a
            // `totals_snapshot_id`, beside columns and specs resolved at
            // a past timestamp. Two answers from two different snapshots
            // in one response, and nothing would have said so.
            val future = body(client.get("$table?at_timestamp=2100-01-01T00:00:00Z"))
            assertThat(future.has("totals_snapshot_id"))
                .describedAs("an at_timestamp read aggregates the manifest, so it carries no sample stamp")
                .isFalse()
            assertThat(future.has("totals_as_of")).isFalse()
            assertThat(future["record_count"].asLong())
                .describedAs("and the numbers are exact at the resolved snapshot, not the sample's")
                .isEqualTo(3)
            assertThat(future["read_snapshot_id"].asLong())
                .describedAs("a timestamp past head resolves at head, and the response says which")
                .isEqualTo(client.head())
        }
    }

    // ---- the identity read --------------------------------------------------

    @Test
    fun `totals=false omits the five totals fields and keeps the identity`() {
        ensureFixture()
        api { client ->
            // The read a writer should send: it needs the UUID, the
            // columns, the specs and `read_snapshot_id`, and reads none
            // of the numbers. No client in this tree sends it yet.
            val r = client.get("$table?totals=false")
            assertThat(r.status).isEqualTo(HttpStatusCode.OK)
            val info = body(r)
            for (field in totalsFields) {
                assertThat(info.has(field))
                    .describedAs("'%s' must be absent under totals=false", field)
                    .isFalse()
            }
            assertThat(info["name"].asText()).isEqualTo("events")
            assertThat(info["namespace"].asText()).isEqualTo("ns")
            assertThat(info["table_uuid"].asText()).isNotBlank()
            assertThat(info["columns"]).hasSize(1)
            assertThat(info["read_snapshot_id"].asLong())
                .describedAs(
                    "read_snapshot_id survives totals=false: it is what makes the identity read " +
                        "USEFUL to a writer, which sends it back as a commit's read_snapshot",
                )
                .isEqualTo(client.head())
        }
    }

    @Test
    fun `totals=false suppresses the totals even at a snapshot`() {
        ensureFixture()
        api { client ->
            // The two parameters are independent: `totals=false` means
            // "do no file work", and naming a snapshot does not
            // re-enable it. Without this, `?snapshot=` would be a way to
            // make the identity read pay for a manifest aggregate.
            val info = body(client.get("$table?snapshot=2&totals=false"))
            assertThat(info.has("record_count")).isFalse()
            assertThat(info.has("totals_snapshot_id")).isFalse()
            assertThat(info["table_uuid"].asText()).isNotBlank()
            assertThat(info["read_snapshot_id"].asLong()).isEqualTo(2)
        }
    }

    @Test
    fun `totals=true is the default and is accepted explicitly`() {
        ensureFixture()
        api { client ->
            val explicit = body(client.get("$table?totals=true"))
            val implicit = body(client.get(table))
            assertThat(explicit.has("record_count")).isTrue()
            assertThat(explicit["record_count"]).isEqualTo(implicit["record_count"])
        }
    }

    @Test
    fun `a totals value that is neither true nor false is refused`() {
        ensureFixture()
        api { client ->
            // STRICT, and the reason is that the failure is otherwise
            // silent: `toBoolean` maps "0", "no" and every typo to
            // FALSE, so a caller asking for something misspelt would get
            // a response with no totals and read the absence as "not yet
            // sampled" — believing the server about a number it never
            // looked for.
            for (bad in listOf("0", "no", "yes", "TRUE ", "")) {
                val r = client.get("$table?totals=$bad")
                assertThat(r.status)
                    .describedAs("?totals=%s must be refused, not read as false", bad)
                    .isEqualTo(HttpStatusCode.BadRequest)
                assertThat(body(r)["detail"].asText()).contains("must be 'true' or 'false'")
            }
        }
    }

    // ---- read_snapshot_id ---------------------------------------------------

    @Test
    fun `read_snapshot_id is head on a head read and the named snapshot otherwise`() {
        ensureFixture()
        api { client ->
            val head = client.head()
            assertThat(body(client.get(table))["read_snapshot_id"].asLong())
                .describedAs("a read with no snapshot resolves at head, and says so")
                .isEqualTo(head)
            assertThat(body(client.get("$table?snapshot=2"))["read_snapshot_id"].asLong())
                .describedAs("and a read at a snapshot echoes that snapshot")
                .isEqualTo(2)
            // A NEW commit moves head, so the next read's value moves
            // too — the property that makes it usable as a commit's
            // read_snapshot and useless as the totals' freshness field.
            val after = client.append("rs", records = 1, bytes = 1)
            assertThat(body(client.get(table))["read_snapshot_id"].asLong())
                .isEqualTo(after)
                .isGreaterThan(head)
        }
    }

    @Test
    fun `snapshot_id stays a DDL receipt marker and is absent on a read`() {
        ensureFixture()
        api { client ->
            // `read_snapshot_id` is a SEPARATE field on purpose. Making
            // `snapshot_id` always-present would have been smaller, and
            // it would silently break two released clients: pyhoglake's
            // `Table` treats a non-null `snapshot_id` as a DDL pin and
            // pins later reads to it, and duckdb-client's
            // `PostDDLTravel` branches on `has_snapshot_id`. So a read
            // must keep carrying none.
            val created =
                body(
                    client.postJson(
                        tables,
                        """{"name":"receipt","columns":[{"name":"id","type":"long"}]}""",
                    ),
                )
            assertThat(created["snapshot_id"].asLong())
                .describedAs("a create IS a DDL commit and names the snapshot it made")
                .isGreaterThan(0)
            assertThat(created["read_snapshot_id"].asLong())
                .describedAs("and that is also the snapshot the receipt was resolved at")
                .isEqualTo(created["snapshot_id"].asLong())

            val read = body(client.get("$tables/receipt"))
            assertThat(read.has("snapshot_id"))
                .describedAs("a READ carries no DDL pin — the contract released clients rely on")
                .isFalse()
            assertThat(read.has("read_snapshot_id"))
                .describedAs("but it does say what it was resolved at")
                .isTrue()
        }
    }
}
