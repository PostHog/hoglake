package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.HttpClient
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
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

/**
 * `type_params.shredding` over the HTTP surface: a declaration comes back
 * exactly as it was sent, JSON numbers included, and one that no writer
 * can honour is a named 422 on every path that defines a column.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VariantShreddingApiTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0, metricsIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()
    private val counter = AtomicInteger(0)

    private val catalog = "variant-shredding"
    private val base = "/v1/catalogs/$catalog"
    private val tablesUrl = "$base/namespaces/ns/tables"

    private val declaration =
        """{"type": "object", "fields": [
            {"name": "${'$'}browser", "type": "string"},
            {"name": "price", "type": "decimal8", "precision": 18, "scale": 2},
            {"name": "tags", "type": "array", "element": {"type": "string"}},
            {"name": "${'$'}set", "type": "object", "fields": [{"name": "plan", "type": "string"}]}]}"""

    @BeforeAll
    fun seed() =
        api { client ->
            client.postJson("/v1/catalogs", """{"name": "$catalog", "data_path": "s3://b/$catalog"}""")
            client.postJson("$base/namespaces", """{"name": "ns"}""")
        }

    @AfterAll
    fun tearDown() = db.close()

    @Test
    fun `a declaration is stored and returned as it was sent`() =
        api { client ->
            val name = table()
            val created =
                client.postJson(
                    tablesUrl,
                    """{"name": "$name", "columns": [
                        {"name": "id", "type": "long"},
                        {"name": "properties", "type": "variant", "type_params": {"shredding": $declaration}}]}""",
                )
            assertThat(created.status).describedAs(created.bodyAsText()).isEqualTo(HttpStatusCode.Created)
            val expected = json.readTree("""{"shredding": $declaration}""")
            assertThat(body(created)["columns"][1]["type_params"]).isEqualTo(expected)
            assertThat(body(client.get("$tablesUrl/$name"))["columns"][1]["type_params"]).isEqualTo(expected)

            // Two-phase creation stores the same definition, and publishes it.
            val twoPhase = table()
            val operation = "$base/table-creations/${UUID.randomUUID()}"
            val prepared =
                client.putJson(
                    operation,
                    """{"namespace": "ns", "name": "$twoPhase", "columns": [
                        {"name": "properties", "type": "variant", "type_params": {"shredding": $declaration}}]}""",
                )
            assertThat(prepared.status).describedAs(prepared.bodyAsText()).isEqualTo(HttpStatusCode.OK)
            val published = client.postJson("$operation/commit", """{"files": []}""")
            assertThat(published.status).describedAs(published.bodyAsText()).isEqualTo(HttpStatusCode.OK)
            assertThat(body(client.get("$tablesUrl/$twoPhase"))["columns"][0]["type_params"]).isEqualTo(expected)
        }

    @ParameterizedTest(name = "{0}")
    @MethodSource("com.posthog.hoglake.api.VariantShreddingApiTest#refusals")
    fun `a declaration no writer can honour is a named 422 and creates nothing`(
        label: String,
        column: String,
        detail: String,
    ) = api { client ->
        val name = table()
        assertValidation(client.postJson(tablesUrl, """{"name": "$name", "columns": [$column]}"""), label, detail)
        assertThat(client.get("$tablesUrl/$name").status).isEqualTo(HttpStatusCode.NotFound)
        val prepared =
            client.putJson(
                "$base/table-creations/${UUID.randomUUID()}",
                """{"namespace": "ns", "name": "$name", "columns": [$column]}""",
            )
        assertValidation(prepared, label, detail)
    }

    @Test
    fun `add_column checks a declaration where it is grafted`() =
        api { client ->
            val name = table()
            client.postJson(
                tablesUrl,
                """{"name": "$name", "columns": [{"name": "id", "type": "long"},
                    {"name": "r", "type": "struct", "children": [{"name": "a", "type": "int"}]}]}""",
            )
            val alterUrl = "$tablesUrl/$name/alter"
            val added =
                client.postJson(
                    alterUrl,
                    """{"ops": [{"op": "add_column", "column":
                        {"name": "properties", "type": "variant", "type_params": {"shredding": $declaration}}}]}""",
                )
            assertThat(added.status).describedAs(added.bodyAsText()).isEqualTo(HttpStatusCode.OK)
            assertThat(
                body(added)["columns"][2]["type_params"],
            ).isEqualTo(json.readTree("""{"shredding": $declaration}"""))
            assertValidation(
                client.postJson(
                    alterUrl,
                    """{"ops": [{"op": "add_column", "parent": "r", "column":
                        {"name": "x", "type": "variant", "type_params": {"shredding": $declaration}}}]}""",
                ),
                "grafted into a struct",
                "variant column 'x' is nested, and only a top-level variant column can declare type_params.shredding",
            )
            assertValidation(
                client.postJson(
                    alterUrl,
                    """{"ops": [{"op": "add_column", "column":
                        {"name": "other", "type": "variant", "type_params": {"shredding": {"type": "int128"}}}}]}""",
                ),
                "an unknown type",
                "variant column 'other' has an invalid type_params.shredding: $ has an unknown type 'int128'",
            )
            assertThat(body(client.get("$tablesUrl/$name"))["columns"].map { it["name"].asText() })
                .containsExactly("id", "r", "properties")
        }

    private fun table(): String = "t${counter.incrementAndGet()}"

    private suspend fun assertValidation(
        response: HttpResponse,
        label: String,
        detail: String,
    ) {
        assertThat(
            response.status,
        ).describedAs("%s: %s", label, response.bodyAsText()).isEqualTo(HttpStatusCode.UnprocessableEntity)
        val node = body(response)
        assertThat(node["error"].asText()).isEqualTo("validation")
        assertThat(node["detail"].asText()).describedAs(label).isEqualTo(detail)
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

    private suspend fun HttpClient.putJson(
        url: String,
        body: String,
    ): HttpResponse =
        put(url) {
            contentType(ContentType.Application.Json)
            setBody(body)
        }

    private suspend fun body(response: HttpResponse): JsonNode = json.readTree(response.bodyAsText())

    companion object {
        @JvmStatic
        fun refusals(): List<Arguments> =
            listOf(
                Arguments.of(
                    "an unknown type",
                    """{"name": "properties", "type": "variant", "type_params": {"shredding": {"type": "text"}}}""",
                    "variant column 'properties' has an invalid type_params.shredding: $ has an unknown type 'text'",
                ),
                Arguments.of(
                    "a decimal its width does not hold",
                    """{"name": "properties", "type": "variant", "type_params": {"shredding": {"type": "object",
                        "fields": [{"name": "price", "type": "decimal4", "precision": 18, "scale": 2}]}}}""",
                    "variant column 'properties' has an invalid type_params.shredding: " +
                        "$.price has precision 18 and scale 2, which decimal4 does not hold",
                ),
                Arguments.of(
                    "fields that differ only by case",
                    """{"name": "properties", "type": "variant", "type_params": {"shredding": {"type": "object",
                        "fields": [{"name": "plan", "type": "string"}, {"name": "Plan", "type": "string"}]}}}""",
                    "variant column 'properties' has an invalid type_params.shredding: " +
                        "$ has fields that differ only by case: 'plan' and 'Plan'",
                ),
                Arguments.of(
                    "a misspelt parameter",
                    """{"name": "properties", "type": "variant", "type_params": {"shreding": {"type": "string"}}}""",
                    "variant column 'properties' has an unknown type_params key 'shreding': " +
                        "a variant takes only 'shredding'",
                ),
                Arguments.of(
                    "a variant in a struct",
                    """{"name": "r", "type": "struct", "children": [
                        {"name": "x", "type": "variant", "type_params": {"shredding": {"type": "string"}}}]}""",
                    "variant column 'r.x' is nested, and only a top-level variant column can declare " +
                        "type_params.shredding",
                ),
                Arguments.of(
                    "a json column",
                    """{"name": "properties", "type": "json", "type_params": {"shredding": {"type": "string"}}}""",
                    "column 'properties' is 'json', and only a variant column can declare type_params.shredding",
                ),
            )
    }
}
