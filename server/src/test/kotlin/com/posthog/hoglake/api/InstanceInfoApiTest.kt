package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.BuildInfo
import com.posthog.hoglake.Config
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.time.Instant

/**
 * Wire-level pinning for GET /v1/info — the webui header builds against
 * exactly this shape. Semantics pinned: totals come from the metrics
 * sampler's last pass (never computed per request), the fields are
 * ABSENT before the first sample, they sum live data files only, and a
 * dropped table's files leave the totals on the next sample. The version
 * is pinned as ALWAYS present, sampler or no sampler.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class InstanceInfoApiTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0, metricsIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()

    private val catalogs = CatalogService(db.jdbi)
    private val commits = CommitService(db.jdbi)

    @AfterAll
    fun tearDown() = db.close()

    private fun api(block: suspend ApplicationTestBuilder.(HttpClient) -> Unit) =
        testApplication {
            application { app.module(this) }
            block(client)
        }

    private suspend fun body(response: HttpResponse): JsonNode = json.readTree(response.bodyAsText())

    /** The listing is an array; `.single { }` reads better than an index. */
    private fun JsonNode.single(match: (JsonNode) -> Boolean): JsonNode = elements().asSequence().single(match)

    private fun seed(cat: String) {
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "analytics")
        catalogs.createTable(cat, "analytics", "events", listOf(ColumnDef("id", ColType.LONG)))
        commits.commit(
            cat,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend(
                            "analytics",
                            "events",
                            listOf(
                                FileRegistration(
                                    path = "s3://bucket/$cat/e1.parquet",
                                    recordCount = 10,
                                    fileSizeBytes = 1_048_576,
                                ),
                                FileRegistration(
                                    path = "s3://bucket/$cat/e2.parquet",
                                    recordCount = 5,
                                    fileSizeBytes = 2_048,
                                ),
                            ),
                        ),
                    ),
            ),
        )
    }

    @Test
    fun `totals are absent before the first sample and sampled values after`() =
        api { client ->
            // The sampler loop is off (metricsIntervalMs = 0) and nothing
            // has called sampleOnce: the fields must be ABSENT, not zero —
            // zeros would read as "empty warehouse" during boot.
            val before = body(client.get("/v1/info"))
            assertThat(before.has("total_rows")).isFalse()
            assertThat(before.has("total_size_bytes")).isFalse()

            seed("info-totals")
            app.catalogMetrics.sampleOnce()

            val res = client.get("/v1/info")
            assertThat(res.status).isEqualTo(HttpStatusCode.OK)
            val root = body(res)
            assertThat(root["total_rows"].isIntegralNumber).isTrue()
            assertThat(root["total_rows"].asLong()).isEqualTo(15)
            assertThat(root["total_size_bytes"].asLong()).isEqualTo(1_050_624)
        }

    @Test
    fun `version is present from boot, before any sample`() =
        api { client ->
            // Unlike the totals, the version does not depend on the
            // sampler: an operator asking "what is running?" during a boot
            // stall is exactly when the answer matters most.
            val root = body(client.get("/v1/info"))
            assertThat(root.has("version")).isTrue()
            assertThat(root["version"].asText()).isEqualTo(BuildInfo.version)
            assertThat(root["version"].asText()).isNotBlank()
        }

    @Test
    fun `build is present exactly when this build carries a stamp`() =
        api { client ->
            // The invariant is the WIRING — the field mirrors BuildInfo,
            // present iff stamped and never null-or-empty, because a
            // client tells "off the pipeline" from "built locally" by
            // presence alone. Asserted this way rather than hard-coding
            // "absent" so the test still holds if the suite is ever run
            // from a stamped build. That an ORDINARY build is unstamped
            // is pinned in BuildInfoTest, where it belongs.
            val root = body(client.get("/v1/info"))
            assertThat(root.has("build")).isEqualTo(BuildInfo.buildStamp != null)
            BuildInfo.buildStamp?.let { assertThat(root["build"].asText()).isEqualTo(it) }
            // ...while version stays present regardless.
            assertThat(root["version"].asText()).isEqualTo(BuildInfo.version)
        }

    @Test
    fun `catalog listing carries per-catalog totals only once sampled`() =
        api { client ->
            val cat = "info-catalog-totals"
            seed(cat)

            // BEFORE any sample: the three totals must be ABSENT, not
            // zero. Zero would say "this catalog is empty", which is a
            // different — and wrong — claim about a catalog holding 15
            // rows.
            val before = body(client.get("/v1/catalogs")).single { it["name"].asText() == cat }
            assertThat(before.has("table_count")).isFalse()
            assertThat(before.has("live_rows")).isFalse()
            assertThat(before.has("live_size_bytes")).isFalse()

            app.catalogMetrics.sampleOnce()

            val after = body(client.get("/v1/catalogs")).single { it["name"].asText() == cat }
            assertThat(after["table_count"].asLong()).isEqualTo(1)
            assertThat(after["live_rows"].asLong()).isEqualTo(15)
            assertThat(after["live_size_bytes"].asLong()).isEqualTo(1_050_624)
            // The oldest-snapshot time rides the SAME sample: absent
            // before, present after, and an ISO instant the client can
            // turn into an age — never a pre-computed duration.
            assertThat(before.has("oldest_snapshot_time")).isFalse()
            assertThat(after.has("oldest_snapshot_time")).isTrue()
            assertThat(Instant.parse(after["oldest_snapshot_time"].asText()))
                .isBeforeOrEqualTo(Instant.now())
        }

    @Test
    fun `oldest snapshot time is the earliest retained, not the expiry floor`() =
        api { client ->
            // A catalog that has never expired: earliest_snapshot_time
            // (the floor) is still null, but the catalog plainly retains
            // its first snapshot. oldest_snapshot_time must report that
            // one, so the column reads an age rather than a dash exactly
            // where the snapshot is OLDEST.
            val cat = "info-oldest-never-expired"
            seed(cat)
            app.catalogMetrics.sampleOnce()

            val row = body(client.get("/v1/catalogs")).single { it["name"].asText() == cat }
            assertThat(row.has("earliest_snapshot_time"))
                .describedAs("expiry has not run, so the floor time is absent")
                .isFalse()
            assertThat(row.has("oldest_snapshot_time"))
                .describedAs("but the first snapshot is retained and dated")
                .isTrue()
        }

    @Test
    fun `per-catalog totals come from the sample, not from a per-request query`() =
        api { client ->
            val cat = "info-catalog-stale"
            seed(cat)
            app.catalogMetrics.sampleOnce()

            // Drop the table: the catalog is now empty in the database.
            catalogs.dropTable(cat, "analytics", "events")

            // The listing still reports the SAMPLED numbers, because it
            // reads the sampler's map rather than summing the manifest
            // per request. That staleness is the deliberate trade (see
            // CatalogTotals) and this pins it — if someone "fixes" it
            // with a live query, this test says what they gave up.
            val stale = body(client.get("/v1/catalogs")).single { it["name"].asText() == cat }
            assertThat(stale["live_rows"].asLong()).isEqualTo(15)

            app.catalogMetrics.sampleOnce()
            val fresh = body(client.get("/v1/catalogs")).single { it["name"].asText() == cat }
            assertThat(fresh["live_rows"].asLong()).isZero()
            assertThat(fresh["table_count"].asLong()).isZero()
        }

    @Test
    fun `totals hold the last sample until the next one and drop dead files then`() {
        val cat = "info-totals-resample"
        seed(cat)
        app.catalogMetrics.sampleOnce()
        val before = app.catalogMetrics.latestTotals!!

        catalogs.dropTable(cat, "analytics", "events")

        // No resample yet: the drop is invisible.
        assertThat(app.catalogMetrics.latestTotals).isEqualTo(before)

        app.catalogMetrics.sampleOnce()
        assertThat(app.catalogMetrics.latestTotals!!.totalRows).isEqualTo(before.totalRows - 15)
        assertThat(app.catalogMetrics.latestTotals!!.totalSizeBytes)
            .isEqualTo(before.totalSizeBytes - 1_050_624)
    }
}
