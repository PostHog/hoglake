package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.stats.IcebergSingleValue
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
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.math.BigInteger
import java.util.Base64

/**
 * POST /v1/debug/decode-bound — the stateless incident tool: paste a
 * type name and the base64 of a stored bound, get the decoded JSON
 * scalar back. It parses attacker-shaped input by design, so the whole
 * point of this suite is the refusal ledger: every malformed input is a
 * NAMED 422 through the ordinary error mapping — never a 500, never a
 * guess.
 *
 * DB-less: the route touches no catalog, so the test app is just the
 * serialization + error stack plus the installer App.kt uses.
 */
class DecodeBoundApiTest {
    private val json = ObjectMapper()

    private fun api(block: suspend ApplicationTestBuilder.(HttpClient) -> Unit) =
        testApplication {
            application {
                this.install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                this.install(StatusPages) { installErrorMapping() }
                installDebugRoutes()
            }
            block(client)
        }

    private suspend fun HttpClient.decode(body: String): HttpResponse =
        post("/v1/debug/decode-bound") {
            contentType(ContentType.Application.Json)
            setBody(body)
        }

    private suspend fun body(response: HttpResponse): JsonNode = json.readTree(response.bodyAsText())

    private fun b64(
        type: ColType,
        value: Any,
    ): String = Base64.getEncoder().encodeToString(IcebergSingleValue.encode(type, value))

    private suspend fun assertValidation(
        response: HttpResponse,
        vararg detailContains: String,
    ) {
        assertThat(response.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
        val node = body(response)
        assertThat(node["error"].asText()).isEqualTo("validation")
        for (fragment in detailContains) {
            assertThat(node["detail"].asText()).contains(fragment)
        }
    }

    // ---- happy paths per family --------------------------------------------

    @Test
    fun `decodes a long exactly`() =
        api { client ->
            val response =
                client.decode("""{"type": "long", "value": "${b64(ColType.LONG, 9007199254740993L)}"}""")
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            val node = body(response)
            assertThat(node["type"].asText()).isEqualTo("long")
            assertThat(node["value"].asLong()).isEqualTo(9007199254740993L)
            assertThat(response.bodyAsText()).contains("9007199254740993")
        }

    @Test
    fun `decodes temporals to ISO strings`() =
        api { client ->
            val tstz =
                client.decode(
                    """{"type": "timestamptz", "value": "${b64(ColType.TIMESTAMPTZ, 1_788_609_600_000_000L)}"}""",
                )
            assertThat(body(tstz)["value"].asText()).isEqualTo("2026-09-05T12:00:00Z")
            val date = client.decode("""{"type": "date", "value": "${b64(ColType.DATE, 20701)}"}""")
            assertThat(body(date)["value"].asText()).isEqualTo("2026-09-05")
            // Case-insensitive like every wire type name — and the
            // response echoes the CANONICAL wire spelling, never the
            // caller's casing (the spec's DecodeBoundResponse.type).
            val time = client.decode("""{"type": "TIME", "value": "${b64(ColType.TIME, 49_062_123_456L)}"}""")
            val timeNode = body(time)
            assertThat(timeNode["type"].asText()).isEqualTo("time")
            assertThat(timeNode["value"].asText()).isEqualTo("13:37:42.123456")
        }

    @Test
    fun `decodes decimal at the requested scale`() =
        api { client ->
            val response =
                client.decode(
                    """{"type": "decimal", "type_params": {"scale": 2},
                        "value": "${b64(ColType.DECIMAL, BigInteger("150"))}"}""",
                )
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            assertThat(response.bodyAsText()).contains("1.50")
        }

    @Test
    fun `decodes uint64 and string and uuid and binary`() =
        api { client ->
            val uint64 =
                client.decode(
                    """{"type": "uint64", "value": "${b64(ColType.UINT64, BigInteger("18446744073709551615"))}"}""",
                )
            assertThat(body(uint64)["value"].bigIntegerValue())
                .isEqualTo(BigInteger("18446744073709551615"))
            val string = client.decode("""{"type": "string", "value": "${b64(ColType.STRING, "🦔")}"}""")
            assertThat(body(string)["value"].asText()).isEqualTo("🦔")
            val uuid =
                client.decode(
                    """{"type": "uuid",
                        "value": "${b64(
                        ColType.UUID_T,
                        java.util.UUID.fromString("123e4567-e89b-12d3-a456-426614174000"),
                    )}"}""",
                )
            assertThat(body(uuid)["value"].asText()).isEqualTo("123e4567-e89b-12d3-a456-426614174000")
            val binary = client.decode("""{"type": "binary", "value": "AAEC"}""")
            assertThat(body(binary)["value"].asText()).isEqualTo("AAEC")
        }

    @Test
    fun `renders infinity sentinels and signed zero`() =
        api { client ->
            val inf =
                client.decode(
                    """{"type": "double", "value": "${b64(ColType.DOUBLE, Double.NEGATIVE_INFINITY)}"}""",
                )
            assertThat(body(inf)["value"].asText()).isEqualTo("-Infinity")
            val negZero = client.decode("""{"type": "float", "value": "${b64(ColType.FLOAT, -0.0f)}"}""")
            assertThat(body(negZero)["value"].toString()).isEqualTo("-0.0")
        }

    // ---- empty bytes: a value for the variable-width types only ------------

    @Test
    fun `empty bytes are the empty string for string and a named 422 elsewhere`() =
        api { client ->
            val emptyString = client.decode("""{"type": "string", "value": ""}""")
            assertThat(emptyString.status).isEqualTo(HttpStatusCode.OK)
            assertThat(body(emptyString)["value"].asText()).isEqualTo("")

            assertValidation(client.decode("""{"type": "boolean", "value": ""}"""), "boolean", "1 bytes")
            assertValidation(client.decode("""{"type": "decimal", "value": ""}"""), "decimal")
        }

    // ---- named refusals ----------------------------------------------------

    @Test
    fun `an unknown type name is a named 422`() =
        api { client ->
            assertValidation(
                client.decode("""{"type": "bigint", "value": "AA=="}"""),
                "unknown column type 'bigint'",
            )
        }

    @Test
    fun `a permanently refused type name gets its own reason, never unknown`() =
        api { client ->
            // The type-parity discipline: int128 is refused BY NAME with
            // the reason, so a caller stops trying rather than hunting
            // for a spelling.
            assertValidation(
                client.decode("""{"type": "int128", "value": "AA=="}"""),
                "int128",
                "decimal(38)",
            )
        }

    @Test
    fun `variant and containers are named 422s`() =
        api { client ->
            assertValidation(
                client.decode("""{"type": "variant", "value": "AA=="}"""),
                "variant",
            )
            assertValidation(
                client.decode("""{"type": "list", "value": "AA=="}"""),
                "nested container",
            )
        }

    @Test
    fun `malformed base64 is a named 422, never a 500`() =
        api { client ->
            assertValidation(
                client.decode("""{"type": "long", "value": "!!!not base64!!!"}"""),
                "base64",
            )
        }

    @Test
    fun `truncated bytes are a named 422`() =
        api { client ->
            // 4 bytes under a type whose encoding is 8.
            assertValidation(
                client.decode("""{"type": "long", "value": "AAAAAA=="}"""),
                "long",
                "8 bytes",
            )
            // 15 bytes under uuid's 16.
            assertValidation(
                client.decode("""{"type": "uuid", "value": "AAAAAAAAAAAAAAAAAAAA"}"""),
                "uuid",
                "16 bytes",
            )
        }

    @Test
    fun `NaN bytes are a named 422 - the store never holds a NaN bound`() =
        api { client ->
            val nan = Base64.getEncoder().encodeToString(byteArrayOf(0, 0, -64, 127))
            assertValidation(
                client.decode("""{"type": "float", "value": "$nan"}"""),
                "NaN",
            )
        }

    @Test
    fun `an out-of-day time is a named 422`() =
        api { client ->
            assertValidation(
                client.decode("""{"type": "time", "value": "${b64(ColType.TIME, -1L)}"}"""),
                "time",
            )
        }

    @Test
    fun `a non-UTF-8 string bound is a named 422`() =
        api { client ->
            val bytes = Base64.getEncoder().encodeToString(byteArrayOf(0xFE.toByte(), 0x02))
            assertValidation(
                client.decode("""{"type": "string", "value": "$bytes"}"""),
                "UTF-8",
            )
        }

    @Test
    fun `an out-of-range scale is a named 422`() =
        api { client ->
            assertValidation(
                client.decode(
                    """{"type": "decimal", "type_params": {"scale": 4294967297},
                        "value": "${b64(ColType.DECIMAL, BigInteger.ONE)}"}""",
                ),
                "scale",
            )
        }

    @Test
    fun `a JSON number value is a named 422 - base64 is a string`() =
        api { client ->
            // Jackson happily coerces 1234 into the String "1234", whose
            // characters ARE valid base64 — so a caller who forgot the
            // quotes used to get a 200 decoding bytes they never sent
            // (binary renders any bytes). The value must be a JSON
            // string, refused by name otherwise.
            assertValidation(
                client.decode("""{"type": "binary", "value": 1234}"""),
                "value must be a JSON string",
            )
        }

    @Test
    fun `an oversized value is a named 422 at the documented cap`() =
        api { client ->
            // 1 KiB decoded is the cap (spec DecodeBoundRequest): the
            // largest bound families are unbounded-length string/json/
            // binary, and no diagnostic paste needs more than this.
            val overCap = Base64.getEncoder().encodeToString(ByteArray(1025) { 'a'.code.toByte() })
            assertValidation(
                client.decode("""{"type": "string", "value": "$overCap"}"""),
                "1024",
            )
            // The cap itself still decodes.
            val atCap = Base64.getEncoder().encodeToString(ByteArray(1024) { 'a'.code.toByte() })
            val response = client.decode("""{"type": "string", "value": "$atCap"}""")
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            assertThat(body(response)["value"].asText()).hasSize(1024)
        }

    @Test
    fun `a missing field is a 400 through the ordinary wire mapping`() =
        api { client ->
            val response = client.decode("""{"type": "long"}""")
            assertThat(response.status).isEqualTo(HttpStatusCode.BadRequest)
            assertThat(body(response)["error"].asText()).isEqualTo("bad_request")
        }

    @Test
    fun `a wrong content type is a named 400, never a 500`() =
        api { client ->
            // The bare-curl case: curl -d defaults the Content-Type to
            // x-www-form-urlencoded, and a hand-set text/plain is the
            // next most common slip. Neither has a registered converter,
            // and both must land in the malformed-body 400 like every
            // other body this server cannot read — not the 500 catch-all.
            for (contentType in listOf(ContentType.Text.Plain, ContentType.Application.FormUrlEncoded)) {
                val response =
                    client.post("/v1/debug/decode-bound") {
                        contentType(contentType)
                        setBody("""{"type": "long", "value": "AAAAAAAAAAA="}""")
                    }
                assertThat(response.status)
                    .describedAs(contentType.toString())
                    .isEqualTo(HttpStatusCode.BadRequest)
                assertThat(body(response)["error"].asText()).isEqualTo("bad_request")
            }
        }

    @Test
    fun `a null JSON body is a named 400, never a 500`() =
        api { client ->
            // `null` IS valid JSON, but it is not a request body; binding
            // it to the request DTO must answer the same malformed-body
            // 400 as a missing field, not escape as an unmapped 500.
            val response = client.decode("null")
            assertThat(response.status).isEqualTo(HttpStatusCode.BadRequest)
            assertThat(body(response)["error"].asText()).isEqualTo("bad_request")
        }
}
