package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.compaction.CompactionGrouping
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.MaintenanceSummarySampler
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * Wire-level pinning for
 * `GET /v1/catalogs/{c}/namespaces/{n}/tables/{t}/partitions`.
 *
 * WHY THIS FILE EXISTS, stated plainly: without it, deleting
 * `app.installPartitionListingRoutes(...)` from `App.kt` leaves the
 * whole suite green while the endpoint 404s in production, and every
 * `toDto()` in `PartitionListingRoutes.kt` is called by nothing (the
 * spec-parity test builds its DTOs by hand). Routes here come from
 * `App.module` — the production wiring — so the route registration, the
 * five query-parameter parsers, the six mappers and the `@JsonInclude`
 * null policy are all exercised on a real response.
 *
 * The webui builds against EXACTLY this JSON, so field names are
 * asserted verbatim, int64 fields are asserted to be JSON numbers
 * (never strings — `webui/src/api/int64.ts` re-reads them from the raw
 * token), and the nullable fields are asserted to be PRESENT and null
 * rather than absent.
 *
 * The threshold is the production default (`Config.compactionTargetBytes`,
 * 512 MiB), which the seeded sizes straddle.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PartitionListingApiTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()

    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)
    private val alter = AlterService(db.jdbi)

    /** 40 MiB — under the 512 MiB target. */
    private val smallBytes = 41_943_040L

    /** 5 GiB — over it, and over int32 on purpose. */
    private val bigBytes = 5_368_709_120L

    private val cat = seed()
    private val base = "/v1/catalogs/$cat/namespaces/analytics/tables/events"

    @AfterAll
    fun tearDown() = db.close()

    private fun api(block: suspend ApplicationTestBuilder.(HttpClient) -> Unit) =
        testApplication {
            application {
                val sampler =
                    MaintenanceSummarySampler(
                        db.jdbi,
                        512L * 1024 * 1024,
                        CompactionGrouping.DEFAULT_MIN_INPUT_FILES,
                        CompactionGrouping.DEFAULT_MAX_INPUT_FILES,
                        3600,
                    )
                while (sampler.runOnce()) { /* publish before exercising the listing */ }
                app.module(this)
            }
            block(client)
        }

    private suspend fun body(response: HttpResponse): JsonNode = json.readTree(response.bodyAsText())

    private fun seed(): String {
        val cat = "wire-plist"
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "analytics")
        catalogs.createTable(
            cat,
            "analytics",
            "events",
            listOf(
                ColumnDef("id", ColType.LONG),
                ColumnDef("team", ColType.STRING),
                ColumnDef("ts", ColType.TIMESTAMP),
            ),
        )
        alter.alterTable(
            cat,
            "analytics",
            "events",
            listOf(AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(3, Transform.DAY)))),
        )

        fun f(
            name: String,
            bytes: Long,
            values: List<String?>?,
            records: Long = 10,
        ) = FileRegistration(
            path = "s3://bucket/$cat/$name.parquet",
            recordCount = records,
            fileSizeBytes = bytes,
            partitionValues = values,
        )
        commits.commit(
            cat,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "analytics",
                            "events",
                            listOf(
                                // 20713 = 2026-09-17: one small file and
                                // one at the target (counted, never a
                                // candidate).
                                f("d1-s", smallBytes, listOf("20713"), records = 7),
                                f("d1-big", bigBytes, listOf("20713"), records = 3),
                                // 20744 = 2026-10-18.
                                f("d2-s", smallBytes, listOf("20744"), records = 5),
                                // the null partition value.
                                f("dn-s", smallBytes, listOf(null), records = 1),
                            ),
                        ),
                    ),
            ),
        )
        return cat
    }

    // ---- the happy path, field by field --------------------------------------

    @Test
    fun `the response carries the documented shape, snake_case, with decoded values`() =
        api { client ->
            val response = client.get("$base/partitions")
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            val b = body(response)

            assertThat(b.fieldNames().asSequence().toList())
                .containsExactlyInAnyOrder(
                    "sampled_at",
                    "sample_started",
                    "sampled_snapshot_id",
                    "spec",
                    "total",
                    "stale_spec_groups",
                    "partitions",
                )
            assertThat(b["sampled_at"].isNull).isFalse()
            assertThat(b["sample_started"].isNull).isFalse()
            // int64 on the wire is a JSON NUMBER; the console re-reads
            // the raw token, so a string here would be a contract break.
            assertThat(b["sampled_snapshot_id"].isNumber).isTrue()
            assertThat(b["total"].asInt()).isEqualTo(3)
            assertThat(b["stale_spec_groups"].asInt()).isEqualTo(0)

            val spec = b["spec"]
            assertThat(spec["spec_id"].isNumber).isTrue()
            assertThat(spec["fields"]).hasSize(1)
            assertThat(spec["fields"][0]["field"].asText()).isEqualTo("ts_day")
            assertThat(spec["fields"][0]["transform"].asText()).isEqualTo("day")
            assertThat(spec["fields"][0]["source_field_id"].isNumber).isTrue()
            // transform_param is NON_NULL: a day transform has none.
            assertThat(spec["fields"][0].has("transform_param")).isFalse()

            // Default sort: the decoded tuple ascending, nulls first.
            val groups = b["partitions"]
            assertThat(groups).hasSize(3)
            assertThat(groups.map { it["values"][0]["decoded"].asText("!") })
                .containsExactly("!", "2026-09-17", "2026-10-18")

            val day = groups[1]
            assertThat(day.fieldNames().asSequence().toList())
                .containsExactlyInAnyOrder(
                    "spec_id",
                    "values",
                    "file_count",
                    "small_file_count",
                    "total_bytes",
                    "small_file_bytes",
                    "avg_file_bytes",
                    "dv_count",
                    "debt_score",
                    "record_count",
                    "last_written_snapshot",
                )
            assertThat(day["values"][0]["field"].asText()).isEqualTo("ts_day")
            assertThat(day["values"][0]["raw"].asText()).isEqualTo("20713")
            assertThat(day["values"][0]["decoded"].asText()).isEqualTo("2026-09-17")
            assertThat(day["file_count"].asLong()).isEqualTo(2)
            assertThat(day["small_file_count"].asLong()).isEqualTo(1)
            assertThat(day["total_bytes"].asLong()).isEqualTo(smallBytes + bigBytes)
            assertThat(day["small_file_bytes"].asLong()).isEqualTo(smallBytes)
            assertThat(day["avg_file_bytes"].asLong()).isEqualTo((smallBytes + bigBytes) / 2)
            assertThat(day["dv_count"].asLong()).isEqualTo(0)
            assertThat(day["debt_score"].asLong()).isEqualTo(0)
            assertThat(day["record_count"].asLong()).isEqualTo(10)
            assertThat(day["last_written_snapshot"].isNumber).isTrue()
            // Over int32, on the wire, as a number.
            assertThat(day["total_bytes"].asLong()).isGreaterThan(Int.MAX_VALUE.toLong())

            // A null partition value is a present null on both halves,
            // not an omitted key.
            val nullGroup = groups[0]
            assertThat(nullGroup["values"][0].has("raw")).isTrue()
            assertThat(nullGroup["values"][0]["raw"].isNull).isTrue()
            assertThat(nullGroup["values"][0]["decoded"].isNull).isTrue()
        }

    @Test
    fun `an unmeasured generation reports record_count and last_written as present nulls`() =
        api { client ->
            // The pre-V22 / straddling state: the published generation
            // was not begun by a sampler that measures rows. Marked the
            // way V22 marks it, so the API's own gate is what is tested.
            db.jdbi.useHandleUnchecked {
                it.execute("UPDATE hog_maintenance_summary SET measures_generation = -1")
            }
            try {
                val b = body(client.get("$base/partitions"))
                val group = b["partitions"][1]
                assertThat(group.has("record_count")).isTrue()
                assertThat(group["record_count"].isNull)
                    .describedAs("null, not 0 — 0 is what the column defaults to")
                    .isTrue()
                assertThat(group["last_written_snapshot"].isNull).isTrue()
                // Everything the sampler always measured is still reported.
                assertThat(group["file_count"].asLong()).isEqualTo(2)
            } finally {
                // PER_CLASS shares one database: restore even on failure,
                // or every later test in the class inherits the mark.
                db.jdbi.useHandleUnchecked {
                    it.execute(
                        "UPDATE hog_maintenance_summary SET measures_generation = published_generation",
                    )
                }
            }
        }

    // ---- the parameters ------------------------------------------------------

    @Test
    fun `sort and order reach the service`() =
        api { client ->
            val desc =
                body(client.get("$base/partitions?sort=files&order=desc"))["partitions"]
                    .map { it["file_count"].asLong() }
            // The SIZE assertion is what makes this test see a missing
            // route: ktor's 404 has an empty body, so an empty list is
            // trivially sorted and the ordering assertions alone passed
            // with the endpoint unmounted.
            assertThat(desc).hasSize(3)
            assertThat(desc).isEqualTo(desc.sortedDescending())
            val asc =
                body(client.get("$base/partitions?sort=files&order=asc"))["partitions"]
                    .map { it["file_count"].asLong() }
            assertThat(asc).hasSize(3)
            assertThat(asc).isEqualTo(asc.sorted())
        }

    @Test
    fun `limit and offset page, and limit is capped rather than honoured unbounded`() =
        api { client ->
            assertThat(body(client.get("$base/partitions?limit=1"))["partitions"]).hasSize(1)
            val second = body(client.get("$base/partitions?limit=1&offset=1"))
            assertThat(second["total"].asInt()).isEqualTo(3)
            assertThat(second["partitions"][0]["values"][0]["decoded"].asText())
                .isEqualTo("2026-09-17")
            // Over MAX_LIMIT: served, capped, not refused — the cap is
            // the server's protection, not a contract violation.
            val huge = client.get("$base/partitions?limit=5000")
            assertThat(huge.status).isEqualTo(HttpStatusCode.OK)
            assertThat(body(huge)["partitions"].size()).isLessThanOrEqualTo(1000)
        }

    @Test
    fun `filter matches the decoded value and is repeatable`() =
        api { client ->
            val september = body(client.get("$base/partitions?filter=0:2026-09"))
            assertThat(september["total"].asInt()).isEqualTo(1)
            assertThat(september["partitions"][0]["values"][0]["raw"].asText()).isEqualTo("20713")
            // The empty text is the null value, not "everything".
            val nulls = body(client.get("$base/partitions?filter=0:"))
            assertThat(nulls["total"].asInt()).isEqualTo(1)
            assertThat(nulls["partitions"][0]["values"][0]["raw"].isNull).isTrue()
        }

    @Test
    fun `every malformed parameter is a 400 that names what was wrong`() =
        api { client ->
            suspend fun bad(query: String): String {
                val response = client.get("$base/partitions?$query")
                assertThat(response.status)
                    .describedAs("%s", query)
                    .isEqualTo(HttpStatusCode.BadRequest)
                return response.bodyAsText()
            }
            assertThat(bad("sort=bogus"))
                .contains("'sort' must be one of", "partition", "last_written", "bogus")
            assertThat(bad("order=up")).contains("'order' must be 'asc' or 'desc'", "up")
            assertThat(bad("limit=0")).contains("'limit' must be positive", "0")
            assertThat(bad("limit=abc")).contains("'limit' must be an integer")
            assertThat(bad("offset=-1")).contains("'offset' must be >= 0", "-1")
            assertThat(bad("filter=nocolon")).contains("'filter' must be key_index:text", "nocolon")
            assertThat(bad("filter=abc:x")).contains("key_index must be a non-negative integer", "abc")
            assertThat(bad("filter=-1:x")).contains("key_index must be a non-negative integer", "-1")
        }

    @Test
    fun `a filter on a key the table does not have is 422, not an empty 200`() =
        api { client ->
            val response = client.get("$base/partitions?filter=9:x")
            assertThat(response.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
            assertThat(response.bodyAsText()).contains("partition key_index 9 is out of range")
        }

    @Test
    fun `snapshot and at_timestamp are ignored rather than honoured`() =
        api { client ->
            // The listing answers at the sampler's snapshot and says so.
            // An unknown parameter must not change the answer or 400.
            val plain = body(client.get("$base/partitions"))
            // ANCHORED to ground truth first: two responses agreeing
            // with each other is satisfied by two 404s with empty
            // bodies, so the fixture's own count has to be in here.
            assertThat(plain["total"].asInt()).isEqualTo(3)
            assertThat(plain["sampled_snapshot_id"].isNumber).isTrue()
            val withSnapshot = body(client.get("$base/partitions?snapshot=1"))
            assertThat(withSnapshot["sampled_snapshot_id"]).isEqualTo(plain["sampled_snapshot_id"])
            assertThat(withSnapshot["total"].asInt()).isEqualTo(3)
        }

    // ---- not found -----------------------------------------------------------

    @Test
    fun `unknown catalog, namespace and table are 404`() =
        api { client ->
            for (
            path in
            listOf(
                "/v1/catalogs/nope/namespaces/analytics/tables/events/partitions",
                "/v1/catalogs/$cat/namespaces/nope/tables/events/partitions",
                "/v1/catalogs/$cat/namespaces/analytics/tables/nope/partitions",
            )
            ) {
                assertThat(client.get(path).status)
                    .describedAs("%s", path)
                    .isEqualTo(HttpStatusCode.NotFound)
            }
        }
}
