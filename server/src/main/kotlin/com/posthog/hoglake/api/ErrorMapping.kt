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
 * - TableRecreated      -> 409 `table_recreated` (the expected
 *                          table_uuid is not the live one), same
 *                          `retry: re-prepare` family
 * - DdlSinceReadSnapshot -> 409 `ddl_since_read_snapshot`, the one
 *                          error body that is more than {error, detail}:
 *                          it also names `tables`, `read_snapshot` and
 *                          `retry: re-prepare`, because its recovery is
 *                          not a retry and a caller must not have to
 *                          parse prose to learn that
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
 *
 * THIS function is the per-exception half — one `HoglakeException` to its
 * status and its wire body — and it is a named function rather than a
 * `when` inside the StatusPages lambda so the spec-parity test can call
 * the real thing. It used to be inline and the test hand-rebuilt these
 * bodies, which made its own claim ("read off the MAPPING, not a
 * restatement") false: a field dropped from an arm here was supplied by
 * the copy and went unnoticed.
 *
 * `Retry-After` is the one thing that does NOT live here — it is a
 * header, not a body — so [installErrorMapping] adds it, and installs
 * the non-HoglakeException handlers.
 */

internal fun errorBody(cause: HoglakeException): Pair<HttpStatusCode, ApiErrorDto> =
    when (cause) {
        is HoglakeException.NotFound -> HttpStatusCode.NotFound to apiError("not_found", cause)
        is HoglakeException.AlreadyExists -> HttpStatusCode.Conflict to apiError("already_exists", cause)
        is HoglakeException.ReconciliationRequired ->
            HttpStatusCode.Conflict to apiError("reconciliation_required", cause)
        is HoglakeException.OffsetRegression -> HttpStatusCode.Conflict to apiError("offset_regression", cause)
        is HoglakeException.IdlessFilesPresent ->
            HttpStatusCode.Conflict to apiError("idless_files_present", cause)
        is HoglakeException.NamespaceNotEmpty ->
            HttpStatusCode.Conflict to apiError("namespace_not_empty", cause)
        is HoglakeException.TableDropped -> HttpStatusCode.Conflict to apiError("table_dropped", cause)
        // The one body that carries more than {error, detail}: which
        // tables moved, the read_snapshot they moved after, and the fact
        // that replaying is futile. A client decides to re-prepare off
        // those rather than off the prose.
        is HoglakeException.DdlSinceReadSnapshot ->
            HttpStatusCode.Conflict to
                apiError("ddl_since_read_snapshot", cause).copy(
                    tables = cause.tables,
                    readSnapshot = cause.readSnapshot,
                    retry = RETRY_RE_PREPARE,
                )
        // Same family, same `retry`: the incarnation the caller named is
        // gone, so replaying cannot work either.
        is HoglakeException.TableRecreated ->
            HttpStatusCode.Conflict to
                apiError("table_recreated", cause).copy(
                    tables = listOf(cause.table),
                    retry = RETRY_RE_PREPARE,
                )
        // AFTER its subclass above: DdlSinceReadSnapshot IS a
        // CommitConflict (so an isinstance-ladder client keeps working),
        // and a `when` would otherwise answer the base code for it.
        is HoglakeException.CommitConflict -> HttpStatusCode.Conflict to apiError("commit_conflict", cause)
        is HoglakeException.Validation -> HttpStatusCode.UnprocessableEntity to apiError("validation", cause)
        is HoglakeException.Expired -> HttpStatusCode.Gone to apiError("expired", cause)
        // Explicit backpressure: the commit queued too long on the catalog
        // lock. Clients back off and retry; the Retry-After header is
        // added by the caller.
        is HoglakeException.CommitQueueTimeout ->
            HttpStatusCode.ServiceUnavailable to apiError("commit_queue_timeout", cause)
    }

/** Installs [errorBody] plus the malformed-input and catch-all handlers. */
fun StatusPagesConfig.installErrorMapping() {
    exception<HoglakeException> { call, cause ->
        val (status, body) = errorBody(cause)
        if (cause is HoglakeException.CommitQueueTimeout) {
            call.response.headers.append(HttpHeaders.RetryAfter, RETRY_AFTER_SECONDS)
        }
        call.respond(status, body)
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

/**
 * The `retry` hint on `ddl_since_read_snapshot`. Named rather than
 * spelled inline: it is a wire token clients branch on, and the whole
 * point of the field is that replaying the same payload is NOT the
 * recovery — the writer has to re-read the table and build a new
 * request.
 */
private const val RETRY_RE_PREPARE = "re-prepare"

/** The {error, detail} body every mapping starts from. */
private fun apiError(
    code: String,
    cause: HoglakeException,
) = ApiErrorDto(error = code, detail = cause.message)

private fun rootMessage(t: Throwable): String =
    generateSequence(t) { it.cause }.mapNotNull { it.message }.lastOrNull() ?: "malformed request"
