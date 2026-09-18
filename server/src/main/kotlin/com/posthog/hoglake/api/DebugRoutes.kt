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
            val bytes =
                try {
                    Base64.getDecoder().decode(req.value)
                } catch (e: IllegalArgumentException) {
                    throw HoglakeException.Validation(
                        "value is not valid base64: ${e.message}",
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

/**
 * The request: a wire type name, base64 bytes, and (for decimal) the
 * optional type_params carrying the scale — the same shape a column
 * declares, because the scale lives in the column, not the bytes.
 */
data class DecodeBoundRequestDto(
    val type: String,
    val value: String,
    val typeParams: Map<String, Any?>? = null,
)

data class DecodeBoundResponseDto(
    val type: String,
    val value: JsonNode,
)
