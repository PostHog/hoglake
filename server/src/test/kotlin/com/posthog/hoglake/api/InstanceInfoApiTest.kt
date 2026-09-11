package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
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
 * Wire-level pinning for GET /v1/info totals — the webui header builds
 * against exactly this shape. Semantics pinned: totals sum LIVE data
 * files only (a dropped table's files leave the totals), and the cache
 * only recomputes after the TTL (driven via instanceTotals(now)).
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class InstanceInfoApiTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
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
    fun `info carries live totals with verbatim field names`() =
        api { client ->
            seed("info-totals")

            val res = client.get("/v1/info")
            assertThat(res.status).isEqualTo(HttpStatusCode.OK)
            val root = body(res)
            assertThat(root.has("total_rows")).isTrue()
            assertThat(root.has("total_size_bytes")).isTrue()
            assertThat(root["total_rows"].isIntegralNumber).isTrue()
            assertThat(root["total_rows"].asLong()).isEqualTo(15)
            assertThat(root["total_size_bytes"].asLong()).isEqualTo(1_050_624)
        }

    @Test
    fun `totals cache serves stale until the ttl passes and drops dead files after`() {
        val cat = "info-totals-ttl"
        seed(cat)
        val t0 = Instant.now()

        val fresh = catalogs.instanceTotals(t0)
        // seed() above plus the first test's catalog may both be present;
        // assert deltas, not absolutes.
        val rowsBefore = fresh.totalRows

        catalogs.dropTable(cat, "analytics", "events")

        // Within the TTL the cached value still includes the dropped
        // table's files.
        assertThat(catalogs.instanceTotals(t0.plusSeconds(1)).totalRows).isEqualTo(rowsBefore)

        // Past the TTL the recompute sees only live files.
        assertThat(catalogs.instanceTotals(t0.plusSeconds(61)).totalRows)
            .isEqualTo(rowsBefore - 15)
    }
}
