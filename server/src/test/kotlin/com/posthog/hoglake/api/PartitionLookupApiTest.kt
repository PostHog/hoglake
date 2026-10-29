package com.posthog.hoglake.api

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.testing.PartitionLookupFixture
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.wireObjectMapper
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

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PartitionLookupApiTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val f = PartitionLookupFixture(db).also { it.append(listOf(null, ""), listOf(null, "null")) }
    private val json = wireObjectMapper()

    @AfterAll
    fun close() = db.close()

    private fun api(block: suspend ApplicationTestBuilder.(HttpClient) -> Unit) =
        testApplication {
            application { app.module(this) }
            block(client)
        }

    private suspend fun HttpClient.lookup(
        values: String = """[null,""]""",
        vararg extra: Pair<String, String>,
    ): HttpResponse =
        get(f.url) {
            url {
                parameters.append("spec_id", f.specId.toString())
                parameters.append("values", values)
                extra.forEach { (name, value) -> parameters.append(name, value) }
            }
        }

    @Test
    fun `production route returns explicit nullable metadata and exactly one full tuple`() =
        api { client ->
            val response = client.lookup()
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            val body = json.readTree(response.bodyAsText())
            assertThat(body["values"][0].isNull).isTrue()
            assertThat(body["values"][1].textValue()).isEmpty()
            assertThat(body["aggregates"]["status"].textValue()).isEqualTo("exact")
            assertThat(body["aggregates"]["reason"].isNull).isTrue()
            assertThat(body["aggregates"]["totals"]["file_count"].longValue()).isEqualTo(1)
            assertThat(body["files"].size()).isEqualTo(1)
            assertThat(body["next_cursor"].isNull).isTrue()
            assertThat(body["sampled_at"]).isNull()
        }

    @Test
    fun `valid supplementary Unicode and whitespace stay raw through actual route`() =
        api { client ->
            val response = client.lookup("""["  東京🚀  ", "01"]""")
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            assertThat(json.readTree(response.bodyAsText())["values"][0].textValue()).isEqualTo("  東京🚀  ")
        }

    @Test
    fun `malformed missing repeated and unsupported parameters are 400`() =
        api { client ->
            for (query in listOf("", "?spec_id=1", "?values=%5Bnull%5D")) {
                assertThat(client.get(f.url + query).status).isEqualTo(HttpStatusCode.BadRequest)
            }
            val extras =
                listOf(
                    "spec_id" to "1",
                    "values" to "[]",
                    "limit" to "0",
                    "limit" to "501",
                    "limit" to "x",
                    "snapshot" to "x",
                    "cursor" to "garbage",
                    "at_timestamp" to "2026-01-01T00:00:00Z",
                )
            for (extra in extras) assertThat(
                client.lookup(extra = arrayOf(extra)).status,
            ).describedAs("%s", extra).isEqualTo(HttpStatusCode.BadRequest)
            for (name in listOf("limit", "snapshot", "cursor")) {
                assertThat(
                    client.lookup(extra = arrayOf(name to "1", name to "1")).status,
                ).isEqualTo(HttpStatusCode.BadRequest)
            }
            for (raw in listOf(
                "[]",
                "[1]",
                "[true]",
                "[{}]",
                "[null] {}",
                """["\u0000", "x"]""",
                """["\ud800", "x"]""",
                """["\udc00", "x"]""",
            )) {
                val response = client.lookup(raw)
                assertThat(
                    response.status,
                ).describedAs("%s: %s", raw, response.bodyAsText()).isEqualTo(HttpStatusCode.BadRequest)
            }
        }

    @Test
    fun `known incomplete tuple future spec snapshot and unknown resource have distinct errors`() =
        api { client ->
            assertThat(client.lookup("[null]").status).isEqualTo(HttpStatusCode.UnprocessableEntity)
            val future =
                client.get(f.url) {
                    url {
                        parameters.append("spec_id", (f.specId + 100).toString())
                        parameters.append("values", "[null,null]")
                    }
                }
            assertThat(future.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
            assertThat(
                client.lookup(extra = arrayOf("snapshot" to (db.head(f.name) + 1).toString())).status,
            ).isEqualTo(HttpStatusCode.UnprocessableEntity)
            assertThat(
                client.get(f.url.replace("/tables/t/", "/tables/missing/")) {
                    url {
                        parameters.append("spec_id", "1")
                        parameters.append("values", "[null,null]")
                    }
                }.status,
            ).isEqualTo(HttpStatusCode.NotFound)
        }
}
