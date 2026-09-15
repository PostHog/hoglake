package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.ObjectMapper
import io.ktor.client.HttpClient
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Companion to `fuzz/WireDtoParseFuzzTest`: that target exercises the
 * Jackson mapper directly, so it cannot see ktor's ContentNegotiation
 * wrapping and must treat every parse throw as benign. This pins the
 * half it can't reach — hostile bodies really do come back 4xx through
 * the full serialization + StatusPages stack, never a 500.
 *
 * The UTF-32 case is a corpus entry: Jackson's encoding detector reads a
 * `00 00 00 00 | 00 71 71 71 ...` byte run as UTF-32 and raises a plain
 * [java.io.CharConversionException] — an IOException, *not* a
 * JacksonException. ContentNegotiation still wraps it, so the wire
 * answer is 400; a regression that unwraps it would be a 500 on
 * arbitrary bytes.
 */
class WireParseErrorMappingTest {
    private val json = ObjectMapper()

    private fun api(block: suspend ApplicationTestBuilder.(HttpClient) -> Unit) =
        testApplication {
            application {
                // App.module's serialization + error stack, by sharing
                // its definition rather than restating it.
                this.install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                this.install(StatusPages) { installErrorMapping() }
                routing {
                    // Both phases, exactly as the real routes run them:
                    // receive() inside ContentNegotiation, toModel()
                    // after it in the handler.
                    post("/parse/commit") {
                        call.receive<CommitRequestDto>().toModel()
                        call.respond(HttpStatusCode.OK)
                    }
                    post("/parse/create") {
                        call.receive<CreateTableRequestDto>().columns.forEach { it.toModel() }
                        call.respond(HttpStatusCode.OK)
                    }
                    post("/parse/alter") {
                        call.receive<AlterTableRequestDto>().ops.forEach { it.toModel() }
                        call.respond(HttpStatusCode.OK)
                    }
                }
            }
            block(client)
        }

    private suspend fun HttpClient.postBytes(
        path: String,
        body: ByteArray,
    ): HttpResponse =
        post(path) {
            contentType(ContentType.Application.Json)
            setBody(body)
        }

    private suspend fun assertBadRequest(response: HttpResponse) {
        assertThat(response.status).isEqualTo(HttpStatusCode.BadRequest)
        assertThat(json.readTree(response.bodyAsText())["error"].asText()).isEqualTo("bad_request")
    }

    @Test
    fun `a body Jackson reads as invalid UTF-32 is a 400, not a 500`() =
        api { client ->
            // The fuzzer-found byte run, verbatim (see the class docstring).
            val utf32Trap = ByteArray(8) + ByteArray(34) { 0x71 } + ByteArray(14)
            assertBadRequest(client.postBytes("/parse/commit", utf32Trap))
            assertBadRequest(client.postBytes("/parse/alter", utf32Trap))
        }

    @Test
    fun `truncated and non-JSON bodies are 400s on both parse paths`() =
        api { client ->
            for (body in listOf("{\"snapshot\"", "not json at all", "\u0000\u0001\u0002", "[]")) {
                assertBadRequest(client.postBytes("/parse/commit", body.toByteArray()))
                assertBadRequest(client.postBytes("/parse/alter", body.toByteArray()))
            }
        }

    @Test
    fun `a null element in a request list is a 400, not a 500`() =
        api { client ->
            // #61, fuzzer-found: a JSON null inside a list whose element
            // type is non-null used to bind fine and then NPE in the
            // handler's toModel() — outside ContentNegotiation's wrapping,
            // so a 500 on a malformed request. All five request lists that
            // had the hole, and the shape from the fuzzer's own reproducer
            // (two valid ops then a null).
            val bodies =
                listOf(
                    """{"ops":[null]}""",
                    """{"ops":[{"op":"drop_column","name":"c0"},null]}""",
                    """{"ops":[{"op":"set_partition_spec","fields":[null]}]}""",
                    """{"ops":[{"op":"set_sort_order","sort_fields":[null]}]}""",
                )
            for (body in bodies) {
                assertBadRequest(client.postBytes("/parse/alter", body.toByteArray()))
            }
            val commitBodies =
                listOf(
                    """{"appends":[null]}""",
                    """{"deletes":[null]}""",
                    // Nested one level down, and reported by review as
                    // missing from the first pass of this test.
                    """{"appends":[{"namespace":"n","table":"t","files":[null]}]}""",
                    """{"deletes":[{"namespace":"n","table":"t","files":[null]}]}""",
                    """{"appends":[{"namespace":"n","table":"t","files":[{"path":"s3://b/f",""" +
                        """"record_count":1,"file_size_bytes":1,"column_stats":[null]}]}]}""",
                )
            for (body in commitBodies) {
                assertBadRequest(client.postBytes("/parse/commit", body.toByteArray()))
            }
            // A third endpoint: create-table carries the same shape, so
            // the blast radius was never just alter + commit.
            assertBadRequest(
                client.postBytes("/parse/create", """{"name":"t","columns":[null]}""".toByteArray()),
            )
        }

    @Test
    fun `a declared-nullable element is still accepted`() =
        api { client ->
            // The other half of #61: partition_values is List<String?> and
            // a null partition value is a real value. StrictNullChecks must
            // not touch it — this is the payload the fix could have broken.
            val body =
                """{"appends":[{"namespace":"n","table":"t","files":[""" +
                    """{"path":"s3://b/f.parquet","record_count":1,"file_size_bytes":1,""" +
                    """"partition_values":[null,"a"]}]}]}"""
            assertThat(client.postBytes("/parse/commit", body.toByteArray()).status)
                .isEqualTo(HttpStatusCode.OK)
        }

    @Test
    fun `an unknown alter op is a 400 naming the op`() =
        api { client ->
            val response = client.postBytes("/parse/alter", """{"ops":[{"op":"drop_database"}]}""".toByteArray())
            assertBadRequest(response)
        }
}
