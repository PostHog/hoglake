package com.posthog.hoglake.api

import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.service.CorruptDefinitionException
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.JsonConvertException
import io.ktor.server.plugins.BadRequestException
import io.ktor.server.plugins.ContentTransformationException
import io.ktor.server.plugins.statuspages.StatusPagesConfig
import io.ktor.server.request.httpMethod
import io.ktor.server.request.uri
import io.ktor.server.response.respond

private val log = KotlinLogging.logger("com.posthog.hoglake.api.ErrorMapping")

/**
 * HoglakeException -> HTTP status mapping, plus the malformed-input and
 * catch-all handlers. Every error body is an ApiError {error, detail}
 * (the spec's schema); Ktor's default HTML pages never escape.
 *
 * - NotFound            -> 404
 * - AlreadyExists       -> 409
 * - CommitConflict      -> 409
 * - OffsetRegression    -> 409
 * - IdlessFilesPresent  -> 409
 * - NamespaceNotEmpty   -> 409
 * - TableDropped        -> 409 (a commit to a table that was dropped;
 *                          the detail names the drop snapshot)
 * - Validation          -> 422
 * - Expired             -> 410
 * - CommitQueueTimeout  -> 503 + Retry-After (retryable backpressure,
 *                          never a generic 500)
 * - malformed body / unparseable query or path params -> 400. This
 *   includes the receives ktor refuses BEFORE deserialization even
 *   starts (ContentTransformationException): a body sent under a
 *   Content-Type no converter handles (bare curl -d posts
 *   x-www-form-urlencoded) and a JSON `null` body, both of which
 *   otherwise escape to the Throwable catch-all as 500s.
 * - CorruptDefinitionException -> 500 `corrupt_definition`, NAMING the
 *   unreadable receipt (a stored row nobody can act on without knowing
 *   which one it is)
 * - anything else     -> 500 (logged; generic body, no internals)
 */
fun StatusPagesConfig.installErrorMapping() {
    exception<HoglakeException> { call, cause ->
        val (status, code) =
            when (cause) {
                is HoglakeException.NotFound -> HttpStatusCode.NotFound to "not_found"
                is HoglakeException.AlreadyExists -> HttpStatusCode.Conflict to "already_exists"
                is HoglakeException.ReconciliationRequired -> HttpStatusCode.Conflict to "reconciliation_required"
                is HoglakeException.CommitConflict -> HttpStatusCode.Conflict to "commit_conflict"
                is HoglakeException.OffsetRegression -> HttpStatusCode.Conflict to "offset_regression"
                is HoglakeException.IdlessFilesPresent -> HttpStatusCode.Conflict to "idless_files_present"
                is HoglakeException.NamespaceNotEmpty -> HttpStatusCode.Conflict to "namespace_not_empty"
                is HoglakeException.TableDropped -> HttpStatusCode.Conflict to "table_dropped"
                is HoglakeException.Validation -> HttpStatusCode.UnprocessableEntity to "validation"
                is HoglakeException.Expired -> HttpStatusCode.Gone to "expired"
                is HoglakeException.CommitQueueTimeout -> {
                    // Explicit backpressure: the commit queued too long on
                    // the catalog lock. Clients back off and retry.
                    call.response.headers.append(HttpHeaders.RetryAfter, RETRY_AFTER_SECONDS)
                    HttpStatusCode.ServiceUnavailable to "commit_queue_timeout"
                }
            }
        call.respond(status, ApiErrorDto(error = code, detail = cause.message))
    }
    // A receipt this server WROTE and cannot read back. Still a 500 —
    // the caller did nothing wrong and can do nothing about it — but a
    // named one carrying which receipt, because the catch-all's generic
    // internal_error body left an operator with a row they could not
    // identify.
    //
    // The message is the codec's own prose plus an operation id, and
    // echoes of stored content: a column name, a type spelling, a field
    // location, and the offending JSON node. All of them originated
    // with whoever prepared the receipt. Two earlier versions of this
    // comment got this wrong in turn — first claiming no caller text
    // reached the message at all, then claiming only two echoes did and
    // both were capped, when three more were interpolated raw. They are
    // echoed deliberately (a corrupt receipt is unidentifiable without
    // them), every one of them now goes through the codec's `cap`, and
    // they go back to the same caller who supplied them.
    exception<CorruptDefinitionException> { call, cause ->
        log.error(cause) { "unreadable table creation definition: ${cause.message}" }
        call.respond(
            HttpStatusCode.InternalServerError,
            ApiErrorDto("corrupt_definition", cause.message),
        )
    }
    // Ktor wraps request-body deserialization failures in BadRequestException;
    // route helpers throw it directly for unparseable query/path parameters.
    exception<BadRequestException> { call, cause ->
        call.respond(HttpStatusCode.BadRequest, ApiErrorDto("bad_request", rootMessage(cause)))
    }
    exception<JsonConvertException> { call, cause ->
        call.respond(HttpStatusCode.BadRequest, ApiErrorDto("bad_request", rootMessage(cause)))
    }
    // Receives ktor refuses without a converter ever running: an
    // unsupported Content-Type (UnsupportedMediaTypeException) and a
    // body whose transformation yields nothing to bind — a JSON `null`
    // (CannotTransformContentToTypeException). Both are the caller's
    // malformed request, same 400 as a body Jackson cannot parse.
    exception<ContentTransformationException> { call, cause ->
        call.respond(HttpStatusCode.BadRequest, ApiErrorDto("bad_request", rootMessage(cause)))
    }
    exception<Throwable> { call, cause ->
        log.error(cause) {
            "unhandled exception for ${call.request.httpMethod.value} ${call.request.uri}"
        }
        call.respond(
            HttpStatusCode.InternalServerError,
            ApiErrorDto("internal_error", "internal server error"),
        )
    }
}

/** Retry-After for 503 commit_queue_timeout: back off a beat, then retry. */
private const val RETRY_AFTER_SECONDS = "1"

private fun rootMessage(t: Throwable): String =
    generateSequence(t) { it.cause }.mapNotNull { it.message }.lastOrNull() ?: "malformed request"
