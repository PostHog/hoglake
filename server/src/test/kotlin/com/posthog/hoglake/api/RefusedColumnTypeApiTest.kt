package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.HttpClient
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
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.util.concurrent.atomic.AtomicInteger

/**
 * The permanently unsupported DuckLake type names, proven refused at the
 * wire with a 422 that NAMES THE TYPE AND THE REASON.
 *
 * The distinction this pins is the whole point: `int128` is not a typo
 * for something hoglake has, it is a type hoglake will never have
 * because Iceberg's widest exact numeric is decimal(38). A caller who
 * gets "unknown column type" goes looking for the right spelling; a
 * caller who gets the reason stops. One case per refused name, on both
 * the CREATE TABLE and the ALTER paths, because they parse the type
 * through different DTOs.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RefusedColumnTypeApiTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()
    private val counter = AtomicInteger(0)

    private val catalog = "refused-types"
    private val tableUrl = "/v1/catalogs/$catalog/namespaces/ns/tables"
    private val alterUrl = "$tableUrl/events/alter"

    @BeforeAll
    fun seed() =
        api { client ->
            client.postJson("/v1/catalogs", """{"name": "$catalog", "data_path": "s3://b/$catalog"}""")
            client.postJson("/v1/catalogs/$catalog/namespaces", """{"name": "ns"}""")
            client.postJson(
                tableUrl,
                """{"name": "events", "columns": [{"name": "id", "type": "long"}]}""",
            )
        }

    @AfterAll
    fun tearDown() = db.close()

    // ---- the refusals ----------------------------------------------------

    @ParameterizedTest(name = "create table with a {0} column")
    @MethodSource("com.posthog.hoglake.api.RefusedColumnTypeApiTest#refusedNames")
    fun `a refused type is 422 with its reason on create table`(name: String) =
        api { client ->
            val response =
                client.postJson(
                    tableUrl,
                    """{"name": "t${counter.incrementAndGet()}",
                        "columns": [{"name": "c", "type": "$name"}]}""",
                )
            assertRefusal(response, name)
        }

    @ParameterizedTest(name = "add_column of type {0}")
    @MethodSource("com.posthog.hoglake.api.RefusedColumnTypeApiTest#refusedNames")
    fun `a refused type is 422 with its reason on alter add_column`(name: String) =
        api { client ->
            val response =
                client.postJson(
                    alterUrl,
                    """{"ops": [{"op": "add_column",
                        "column": {"name": "c", "type": "$name"}}]}""",
                )
            assertRefusal(response, name)
        }

    @ParameterizedTest(name = "promote_column to {0}")
    @MethodSource("com.posthog.hoglake.api.RefusedColumnTypeApiTest#refusedNames")
    fun `a refused type is 422 with its reason on alter promote_column`(name: String) =
        api { client ->
            val response =
                client.postJson(
                    alterUrl,
                    """{"ops": [{"op": "promote_column", "name": "id", "to": "$name"}]}""",
                )
            assertRefusal(response, name)
        }

    @Test
    fun `a genuine typo still gets the unknown-type answer, not a refusal reason`() =
        api { client ->
            // The control case. Without it, a change that turned every
            // unknown name into a refusal would still pass the tests above.
            val response =
                client.postJson(
                    tableUrl,
                    """{"name": "typo", "columns": [{"name": "c", "type": "biging"}]}""",
                )
            assertThat(response.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
            val detail = body(response)["detail"].asText()
            assertThat(detail).contains("unknown column type 'biging'")
            assertThat(detail).doesNotContain("is not supported")
        }

    @Test
    fun `a supported new scalar is accepted on the same path`() =
        api { client ->
            // The other control: the refusals are about specific names, not
            // about the parser having become hostile to anything unfamiliar.
            val response =
                client.postJson(
                    tableUrl,
                    """{"name": "accepted", "columns": [
                        {"name": "a", "type": "uint64"},
                        {"name": "b", "type": "timestamp_ns"},
                        {"name": "c", "type": "json"}]}""",
                )
            assertThat(response.status).isEqualTo(HttpStatusCode.Created)
            assertThat(body(response)["columns"].map { it["type"].asText() })
                .containsExactly("uint64", "timestamp_ns", "json")
        }

    // ---- harness ---------------------------------------------------------

    private suspend fun assertRefusal(
        response: HttpResponse,
        name: String,
    ) {
        assertThat(response.status)
            .describedAs("status for type '%s'", name)
            .isEqualTo(HttpStatusCode.UnprocessableEntity)
        val node = body(response)
        assertThat(node["error"].asText()).isEqualTo("validation")
        val detail = node["detail"].asText()
        assertThat(detail).describedAs("detail names the type").contains("'$name'")
        assertThat(detail).describedAs("detail says it is unsupported").contains("is not supported")
        assertThat(detail)
            .describedAs("detail gives the reason")
            .containsAnyOf("decimal digits", "no Iceberg mapping", "geometry")
        assertThat(detail)
            .describedAs("a permanent refusal is never phrased as a typo")
            .doesNotContain("unknown column type")
    }

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

    companion object {
        @JvmStatic
        fun refusedNames(): List<String> = ColType.REFUSALS.keys.sorted()
    }
}
