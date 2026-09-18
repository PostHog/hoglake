package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.stats.BoundWire
import io.ktor.server.application.Application
import io.ktor.server.request.receive
import io.ktor.server.response.respond
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import java.util.Base64

/**
 * POST /v1/debug/decode-bound — the stateless incident tool (openapi
 * decodeBound): paste a column type name and the base64 of an Iceberg
 * single-value encoding (a `hog_file_column_stats` bound, a manifest
 * bound, a value out of a log line), get the decoded JSON scalar back
 * under the same wire conventions the per-file stats endpoint uses
 * (stats/BoundWire). Reads nothing, writes nothing.
 *
 * This endpoint parses attacker-shaped input BY DESIGN, so every
 * refusal is named and mapped, never a 500:
 *
 *  - an unknown type name is the type-parity 422 (ColType.parseWire) —
 *    a permanently refused DuckLake name (int128, geometry, ...) gets
 *    its own reason, a typo gets "unknown column type";
 *  - a non-string `value` is a named 422: Jackson would silently
 *    coerce a JSON number (1234) into the string "1234", whose digits
 *    are valid base64 — bytes the caller never sent, decoded as a 200.
 *    The field binds as a JsonNode so the check sees the real shape;
 *  - an oversized `value` is a named 422 at the documented 1 KiB
 *    decoded cap ([MAX_VALUE_BYTES]): the commit path stores bounds of
 *    any length, but this is a paste-a-value diagnostic, not a bulk
 *    door, and no single-bound paste needs more;
 *  - malformed base64 is a named 422 (decoded here, deliberately not
 *    by Jackson's lenient byte[] binding, so the caller is told it was
 *    the base64 and not the bytes);
 *  - wrong-length/empty/container/variant/NaN/out-of-domain values are
 *    the codec's and renderer's IllegalArgumentException refusals,
 *    mapped to a 422 carrying the codec's own message.
 */
fun Application.installDebugRoutes() {
    routing {
        post("/v1/debug/decode-bound") {
            val req = call.receive<DecodeBoundRequestDto>()
            val type =
                ColType.parseWire(req.type) { "unknown column type '${req.type}'" }
            if (!req.value.isTextual) {
                throw HoglakeException.Validation(
                    "value must be a JSON string of standard base64, " +
                        "got a JSON ${req.value.nodeType.name.lowercase()}",
                )
            }
            val encoded = req.value.asText()
            // Pre-decode length gate: any base64 of <= MAX_VALUE_BYTES
            // decoded bytes fits MAX_VALUE_BASE64_CHARS characters, so
            // an over-long token is refused before decoding it.
            if (encoded.length > MAX_VALUE_BASE64_CHARS) {
                throw HoglakeException.Validation(
                    "value is ${encoded.length} base64 characters, larger than any " +
                        "$MAX_VALUE_BYTES-byte bound ($MAX_VALUE_BASE64_CHARS characters); " +
                        "this endpoint caps values at $MAX_VALUE_BYTES decoded bytes",
                )
            }
            val bytes =
                try {
                    Base64.getDecoder().decode(encoded)
                } catch (e: IllegalArgumentException) {
                    throw HoglakeException.Validation(
                        "value is not valid base64: ${e.message}",
                    )
                }
            if (bytes.size > MAX_VALUE_BYTES) {
                throw HoglakeException.Validation(
                    "value decodes to ${bytes.size} bytes, larger than this endpoint's " +
                        "$MAX_VALUE_BYTES-byte cap",
                )
            }
            val node =
                try {
                    BoundWire.render(type, BoundWire.scaleOf(req.typeParams), bytes)
                } catch (e: IllegalArgumentException) {
                    // The codec/renderer refusal vocabulary, verbatim: it
                    // already names the type, the length it wanted, or the
                    // domain rule the value broke.
                    throw HoglakeException.Validation(e.message ?: "undecodable value")
                }
            call.respond(DecodeBoundResponseDto(type = type.wire, value = node))
        }
    }
}

/** The debug endpoint's decoded-value cap: 1 KiB. A stored bound can be
 * longer (the commit path caps nothing), but a diagnostic paste never
 * needs to be, and an uncapped echo endpoint is a free amplifier. */
private const val MAX_VALUE_BYTES = 1024

/** Base64 length of [MAX_VALUE_BYTES] decoded bytes: 4 * ceil(1024/3). */
private const val MAX_VALUE_BASE64_CHARS = 1368

/**
 * The request: a wire type name, base64 bytes, and (for decimal) the
 * optional type_params carrying the scale — the same shape a column
 * declares, because the scale lives in the column, not the bytes.
 * `value` binds as a [JsonNode] on purpose: a String field would let
 * Jackson coerce a bare JSON number into digits that pass base64
 * decoding, and the route must be able to refuse that shape by name.
 */
data class DecodeBoundRequestDto(
    val type: String,
    val value: JsonNode,
    val typeParams: Map<String, Any?>? = null,
)

data class DecodeBoundResponseDto(
    val type: String,
    val value: JsonNode,
)
