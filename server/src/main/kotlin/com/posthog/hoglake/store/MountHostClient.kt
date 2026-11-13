package com.posthog.hoglake.store

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.BuildInfo
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.observability.REQUEST_ID_HEADER
import com.posthog.hoglake.observability.requestId
import com.posthog.hoglake.service.Identifiers
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.ApplicationCall
import io.ktor.server.netty.NettyApplicationCall
import io.ktor.server.response.respondBytes
import io.ktor.server.response.respondBytesWriter
import io.ktor.server.routing.RoutingCall
import io.ktor.utils.io.cancel
import io.ktor.utils.io.writeFully
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.io.IOException
import java.io.InputStream
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

/**
 * A mount Client's side of the Host/Client split (#291's `RemoteCatalog`,
 * plan row 1.3c): the reader for one remote [host] of kind `hoglake`. It
 * forwards a mount reader's request to that host and relays its answer.
 * Which host serves a name is the resolver's to say ([MountHostResolver]);
 * this class knows only the host it was built for.
 *
 * It decides nothing about isolation. The caller is checked as a mount
 * reader before a binding to this client exists
 * ([CatalogStoreSelector.resolve]); here the caller's `Authorization`
 * header goes to the Host unchanged, and the Host verifies the token again
 * and applies its own `ReadScope`. The Client holds no team values and
 * filters nothing.
 *
 * What it does own:
 * - **Where a request goes.** Only the host's origin. Every path is built here from
 *   literals, the mount name (which must have the reserved shape) and
 *   namespace and table names that pass the identifier grammar, each
 *   percent-encoded as one segment; a name outside the grammar is the 404 a
 *   missing one gets, with nothing sent. Query parameters are re-encoded one
 *   by one. Redirects are not followed and no proxy is used.
 * - **Bounds.** One cap over the host's requests in flight (non-blocking:
 *   503 `mount_proxy_busy`), a connect timeout, one deadline per request
 *   covering the wait for the Host and its whole body, and a byte cap on a
 *   body (the host's `max_concurrent`, `timeout_ms`, `max_response_bytes`). The
 *   handler suspends while the Host works, so no request-dispatcher thread
 *   is held; a body is streamed, never held whole.
 * - **What comes back.** A 200 JSON body, byte for byte. A typed refusal
 *   (an allowed status and an `ApiError` body) as the Host's status, code
 *   and detail. Unreachable or late: 503 `mount_host_unavailable`. Anything
 *   else: 502 `mount_host_invalid_response`. No message built here carries
 *   the Host's address or bytes the Host sent.
 */
class MountHostClient(
    /** The host this reader serves: what it is, where, and how to authenticate to it. */
    val host: MountHost,
    connectTimeoutMs: Long = CONNECT_TIMEOUT_MS,
    /** The most bytes of a 200 body read before this server commits its own response. */
    private val prebufferBytes: Int = PREBUFFER_BYTES,
) : AutoCloseable {
    init {
        // This reader speaks one protocol: another hoglake's mount routes.
        // A host of any other kind needs a reader of its own.
        require(host.kind == MountHost.Kind.HOGLAKE) { "no reader for a mount host of kind '${host.kind.wire}'" }
        require(prebufferBytes >= 1)
    }

    private val timeoutMs = host.timeoutMs
    private val maxResponseBytes = host.maxResponseBytes
    private val log = KotlinLogging.logger {}
    private val base = URI(host.origin.scheme, null, host.origin.host, host.origin.port, null, null, null).toString()
    private val permits = Semaphore(host.maxConcurrent)

    /**
     * Where this host's blocking body reads run: as many threads as it has
     * permits, and its own allowance, so a stalled host cannot use up the
     * shared IO threads the probe and request parsing also run on.
     */
    private val io = Dispatchers.IO.limitedParallelism(host.maxConcurrent)
    private val json = jacksonObjectMapper()

    private val http: HttpClient =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofMillis(connectTimeoutMs))
            // Only the host's origin is ever contacted: a redirect is an invalid answer, not a second address.
            .followRedirects(HttpClient.Redirect.NEVER)
            .proxy(HttpClient.Builder.NO_PROXY)
            .build()

    /** Closes a body still being read at its request's deadline; one daemon thread, no queue growth. */
    private val watchdog =
        ScheduledThreadPoolExecutor(1) { task -> Thread(task, "hoglake-mount-host-deadline").apply { isDaemon = true } }
            .apply { removeOnCancelPolicy = true }

    /** Requests in flight right now; a test holds the cap through [permits]. */
    internal val cap: Semaphore get() = permits

    suspend fun catalog(
        call: ApplicationCall,
        mount: String,
    ) = relay(call, mount)

    suspend fun namespaces(
        call: ApplicationCall,
        mount: String,
    ) = relay(call, mount, "namespaces")

    suspend fun namespace(
        call: ApplicationCall,
        mount: String,
        namespace: String,
    ) = relay(call, mount, "namespaces", namespaceName(namespace))

    suspend fun tables(
        call: ApplicationCall,
        mount: String,
        namespace: String,
    ) = relay(call, mount, "namespaces", namespaceName(namespace), "tables")

    suspend fun table(
        call: ApplicationCall,
        mount: String,
        namespace: String,
        table: String,
    ) = relay(call, mount, "namespaces", namespaceName(namespace), "tables", tableName(namespace, table))

    suspend fun scan(
        call: ApplicationCall,
        mount: String,
        namespace: String,
        table: String,
    ) = relay(call, mount, "namespaces", namespaceName(namespace), "tables", tableName(namespace, table), "scan")

    /**
     * The Host's catalog entries for [mounts], as the Host sent them: one
     * upstream listing whatever [mounts] holds, and only entries it names
     * are kept. The Host decides which of them serve; one it leaves out is
     * not listed here either.
     */
    suspend fun catalogs(
        call: ApplicationCall,
        mounts: Set<String>,
    ): List<JsonNode> {
        if (mounts.isEmpty()) return emptyList()
        return exchange(call, mounts, URI.create("$base/v1/catalogs")) { body, deadline ->
            val bytes = read(body, LISTING_BYTES, mounts, deadline)
            if (bytes.size > LISTING_BYTES) throw tooLarge(mounts)
            val root = runCatching { json.readTree(bytes) }.getOrNull()
            if (root == null || !root.isArray) throw invalid(mounts, "a catalog listing that is not a JSON array")
            root.filter { it.isObject && it.get("name")?.takeIf(JsonNode::isTextual)?.asText() in mounts }
                .distinctBy { it.get("name").asText() }
        }
    }

    /** Forward the caller's GET for `/v1/catalogs/{mount}[/tail...]` and relay the answer to [call]. */
    private suspend fun relay(
        call: ApplicationCall,
        mount: String,
        vararg tail: String,
    ) {
        val mounts = setOf(mount)
        exchange(call, mounts, URI.create(base + path(mount, tail.asList()) + query(call))) { body, deadline ->
            val head = read(body, prebufferBytes, mounts, deadline)
            if (head.size > maxResponseBytes) throw tooLarge(mounts)
            if (head.size <= prebufferBytes) {
                // The whole body is in hand: answered complete, or not at all.
                call.respondBytes(head, ContentType.Application.Json, HttpStatusCode.OK)
            } else {
                call.respondBytesWriter(ContentType.Application.Json, HttpStatusCode.OK) {
                    val out = this
                    try {
                        withContext(io) {
                            // Flushed as it goes, so the caller gets each
                            // piece when the host sends it and the caller's
                            // pace is this loop's pace.
                            out.writeFully(head)
                            out.flush()
                            var total = head.size.toLong()
                            val buffer = ByteArray(COPY_BUFFER_BYTES)
                            while (true) {
                                val n =
                                    try {
                                        body.read(buffer)
                                    } catch (e: IOException) {
                                        throw broken(mounts, deadline, e)
                                    }
                                if (n < 0) break
                                total += n
                                if (total > maxResponseBytes) throw tooLarge(mounts)
                                out.writeFully(buffer, 0, n)
                                out.flush()
                            }
                        }
                    } catch (cause: Throwable) {
                        // The response is already under way, so no status
                        // can say it failed. Drop the caller's connection
                        // before the engine can end the body: the reader
                        // then sees a broken transfer, never a partial
                        // array closed as a complete one (#291 §9).
                        dropConnection(call)
                        out.cancel(cause)
                        throw cause
                    }
                }
            }
        }
    }

    /**
     * One request to the Host under the cap and the deadline. [onOk] gets
     * the body of a 200 JSON answer; every other answer is thrown as the
     * typed error the caller is given. Counted and timed against [mounts],
     * which the caller's verified `cat` names.
     */
    private suspend fun <T> exchange(
        call: ApplicationCall,
        mounts: Set<String>,
        uri: URI,
        onOk: suspend (body: InputStream, deadline: Deadline) -> T,
    ): T {
        check(mounts.isNotEmpty() && mounts.all(MountNames::isReserved)) { "not a mount name" }
        val authorization =
            when (host.auth) {
                // The caller's own credential, exactly as it arrived and as
                // the auth plugin verified it. Right only because this host
                // verifies the same issuer's tokens (MountHost.Auth.FORWARD).
                MountHost.Auth.FORWARD ->
                    call.request.headers[HttpHeaders.Authorization]
                        ?: throw HoglakeException.NotFound("catalog '${mounts.first()}'")
            }
        if (!permits.tryAcquire()) {
            mounts.forEach { Metrics.mountRefused(it, "mount_proxy_busy") }
            throw HoglakeException.MountProxyBusy("too many mount requests are in flight on this server; retry shortly")
        }
        val started = System.nanoTime()
        try {
            val request =
                try {
                    HttpRequest.newBuilder(uri)
                        .timeout(Duration.ofMillis(timeoutMs))
                        .GET()
                        .header(HttpHeaders.Accept, "application/json")
                        .header(HttpHeaders.UserAgent, USER_AGENT)
                        .header(HttpHeaders.Authorization, authorization)
                        .also { builder -> call.requestId?.let { builder.header(REQUEST_ID_HEADER, it) } }
                        .build()
                } catch (_: IllegalArgumentException) {
                    // The JDK names the offending value in its message, and
                    // that value is a credential: it goes no further.
                    throw HoglakeException.Validation("this request's headers cannot be forwarded to the mount host")
                }
            // [timeoutMs] bounds the wait for the host and its body. The
            // grace on top bounds what is left: a caller too slow to take
            // the body. Past it the request ends and its permit is free.
            return withTimeout(timeoutMs + CALLER_GRACE_MS) {
                val response =
                    try {
                        http.sendAsync(request, HttpResponse.BodyHandlers.ofInputStream()).awaitOrAbort()
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        throw unreachable(mounts, e)
                    }
                response.body().use { body ->
                    val deadline = Deadline(body, timeoutMs - (System.nanoTime() - started) / NANOS_PER_MS)
                    try {
                        if (response.statusCode() == HttpStatusCode.OK.value) {
                            if (!isJson(response)) throw invalid(mounts, "a 200 that is not JSON")
                            if (!isFramed(response)) throw invalid(mounts, "a 200 body with no length and no chunking")
                            onOk(body, deadline)
                        } else {
                            throw refusal(mounts, response, read(body, ERROR_BODY_BYTES, mounts, deadline))
                        }
                    } finally {
                        deadline.cancel()
                    }
                }
            }
        } catch (_: TimeoutCancellationException) {
            throw unavailable(mounts, timedOut = true)
        } finally {
            permits.release()
            val nanos = System.nanoTime() - started
            mounts.forEach { Metrics.mountProxied(it, nanos) }
        }
    }

    /**
     * Await the host's answer, and abort the exchange if this request is
     * cancelled meanwhile (`cancel(true)` is what the JDK client acts on).
     * An answer that arrives too late for a cancelled request has its body
     * closed, so its connection is not left open with nobody reading.
     */
    private suspend fun CompletableFuture<HttpResponse<InputStream>>.awaitOrAbort(): HttpResponse<InputStream> =
        suspendCancellableCoroutine { continuation ->
            whenComplete { response, failure ->
                if (failure != null) {
                    continuation.resumeWithException((failure as? CompletionException)?.cause ?: failure)
                } else {
                    continuation.resume(response) { _, late, _ -> runCatching { late.body().close() } }
                }
            }
            continuation.invokeOnCancellation { cancel(true) }
        }

    /** At most [limit] + 1 bytes of [body], read off the request dispatcher. */
    private suspend fun read(
        body: InputStream,
        limit: Int,
        mounts: Set<String>,
        deadline: Deadline,
    ): ByteArray =
        withContext(io) {
            try {
                body.readNBytes(limit + 1)
            } catch (e: IOException) {
                throw broken(mounts, deadline, e)
            }
        }

    /** The Host's own refusal when it is one this server relays, else an invalid answer. */
    private fun refusal(
        mounts: Set<String>,
        response: HttpResponse<*>,
        bytes: ByteArray,
    ): HoglakeException {
        val status = response.statusCode()
        val node =
            bytes.takeIf { status in RELAYED_STATUSES && it.size <= ERROR_BODY_BYTES }
                ?.let { runCatching { json.readTree(it) }.getOrNull() }
                ?.takeIf { it.isObject }
        val code = node?.get("error")?.takeIf(JsonNode::isTextual)?.asText()?.takeIf(ERROR_CODE::matches)
        if (node == null || code == null) return invalid(mounts, "status $status without a typed error")
        val detail = node.get("detail")?.takeIf(JsonNode::isTextual)?.asText()?.take(DETAIL_CHARS)
        val retryAfter =
            response.headers().firstValue(HttpHeaders.RetryAfter).orElse(null)?.trim()?.toLongOrNull()
                ?.takeIf { it in 0..MAX_RETRY_AFTER_SECONDS }
        mounts.forEach { Metrics.mountProxyUpstreamError(it, "http_$status") }
        return HoglakeException.MountHostRefused(
            status,
            code,
            detail ?: "the mount host refused the request",
            retryAfter,
        )
    }

    private fun isJson(response: HttpResponse<*>): Boolean =
        response.headers().firstValue(HttpHeaders.ContentType).orElse(null)
            ?.let { runCatching { ContentType.parse(it) }.getOrNull() }
            ?.match(ContentType.Application.Json) == true

    /**
     * A body whose end the JDK client can tell from a cut connection: it
     * has a length or is chunked, and is not encoded. Without either, a
     * truncated body would read as a complete one.
     */
    private fun isFramed(response: HttpResponse<*>): Boolean {
        val headers = response.headers()
        val encoded = headers.firstValue(HttpHeaders.ContentEncoding).orElse("identity").trim().lowercase()
        val chunked = headers.allValues(HttpHeaders.TransferEncoding).any { "chunked" in it.lowercase() }
        val length = headers.firstValue(HttpHeaders.ContentLength).orElse(null)?.trim()?.toLongOrNull()
        return encoded == "identity" && (chunked || length != null)
    }

    private fun unreachable(
        mounts: Set<String>,
        cause: Exception,
    ): HoglakeException {
        val timedOut = generateSequence<Throwable>(cause) { it.cause }.take(8).any { it is HttpTimeoutException }
        log.warn { "mount host ${host.id} request failed: ${cause.javaClass.simpleName}" }
        return unavailable(mounts, timedOut)
    }

    /** A body that stopped arriving: the deadline closed it, or the connection broke. */
    private fun broken(
        mounts: Set<String>,
        deadline: Deadline,
        cause: IOException,
    ): HoglakeException {
        log.warn { "mount host ${host.id} body failed: ${cause.javaClass.simpleName}, expired=${deadline.expired}" }
        return unavailable(mounts, deadline.expired)
    }

    private fun unavailable(
        mounts: Set<String>,
        timedOut: Boolean,
    ): HoglakeException {
        mounts.forEach { Metrics.mountProxyUpstreamError(it, if (timedOut) "timeout" else "unavailable") }
        return HoglakeException.MountHostUnavailable(
            if (timedOut) "the mount host did not answer in time" else "the mount host could not be reached",
        )
    }

    private fun invalid(
        mounts: Set<String>,
        what: String,
    ): HoglakeException {
        // What was wrong goes to the log; the caller is told only that the Host broke its contract.
        log.warn { "mount host ${host.id} answered with $what" }
        mounts.forEach { Metrics.mountProxyUpstreamError(it, "invalid_response") }
        return HoglakeException.MountHostInvalidResponse("the mount host answered with an unexpected response")
    }

    private fun tooLarge(mounts: Set<String>): HoglakeException {
        log.warn { "mount host ${host.id} body is over the $maxResponseBytes byte cap" }
        mounts.forEach { Metrics.mountProxyUpstreamError(it, "response_too_large") }
        return HoglakeException.MountHostInvalidResponse("the mount host's answer is larger than this server forwards")
    }

    /**
     * Close the caller's connection without finishing the response. The
     * engine would otherwise end a cancelled body as a complete, shorter one
     * (measured on Netty: a 200 with a terminated chunked body). Netty is
     * the one engine this server runs on; on any other (the in-process test
     * host) the cancelled body alone stands, cut short and not valid JSON.
     */
    private fun dropConnection(call: ApplicationCall) {
        val engineCall = (call as? RoutingCall)?.pipelineCall?.engineCall ?: call
        val netty = engineCall as? NettyApplicationCall
        if (netty == null) {
            log.warn { "mount host ${host.id}: a failed body could not be cut at the connection (not a Netty call)" }
        } else {
            netty.context.close()
        }
    }

    /** Closes [body] when the request's deadline passes, which fails the read in progress. */
    private inner class Deadline(
        body: InputStream,
        remainingMs: Long,
    ) {
        @Volatile
        var expired = false
            private set

        private val task =
            watchdog.schedule(
                {
                    expired = true
                    runCatching { body.close() }
                },
                remainingMs.coerceAtLeast(0),
                TimeUnit.MILLISECONDS,
            )

        fun cancel() {
            task.cancel(false)
        }
    }

    override fun close() {
        watchdog.shutdownNow()
        http.shutdownNow()
    }

    internal companion object {
        const val CONNECT_TIMEOUT_MS = 2_000L
        const val PREBUFFER_BYTES = 64 * 1024
        private const val COPY_BUFFER_BYTES = 64 * 1024
        private const val ERROR_BODY_BYTES = 64 * 1024
        private const val LISTING_BYTES = 256 * 1024

        /** Past the host's deadline, how long a caller may still take to receive the body. */
        private const val CALLER_GRACE_MS = 10_000L
        private const val DETAIL_CHARS = 4_096
        private const val MAX_RETRY_AFTER_SECONDS = 3_600L
        private const val NANOS_PER_MS = 1_000_000L

        /** The Host's client class for this hop (`auth/AuthPlugin.kt`). */
        val USER_AGENT = "hoglake-server/${BuildInfo.version} (mount-client)"

        /** The statuses a typed Host refusal is relayed with; any other is an invalid answer. */
        val RELAYED_STATUSES = setOf(400, 401, 403, 404, 409, 410, 422, 429, 500, 501, 503)
        private val ERROR_CODE = Regex("^[a-z][a-z0-9_]{0,63}$")
        private val IDENTIFIER = Regex(Identifiers.PATTERN)

        /** [namespace] as a path segment, or the 404 a missing namespace gets when no catalog could hold it. */
        fun namespaceName(namespace: String): String {
            if (!IDENTIFIER.matches(namespace)) {
                throw HoglakeException.NotFound("namespace '${Identifiers.cap(namespace)}'")
            }
            return namespace
        }

        /** [table] as a path segment, or the 404 a missing table gets when no catalog could hold it. */
        fun tableName(
            namespace: String,
            table: String,
        ): String {
            if (!IDENTIFIER.matches(table)) {
                throw HoglakeException.NotFound("table '${Identifiers.cap(namespace)}.${Identifiers.cap(table)}'")
            }
            return table
        }

        /**
         * [value] as exactly one path segment: every byte outside the URI
         * unreserved set is percent-encoded, so no `/`, `?`, `#` or `%` in
         * a name can end the segment or start another part of the URL. A
         * segment of dots alone is refused: it would be a relative
         * reference.
         */
        fun pathSegment(value: String): String {
            require(value.isNotEmpty() && value.any { it != '.' }) { "not a path segment" }
            return encode(value, "")
        }

        /** The upstream path for [mount] and what follows it, every part encoded as one segment. */
        fun path(
            mount: String,
            tail: List<String>,
        ): String = (listOf("v1", "catalogs", mount) + tail).joinToString("") { "/" + pathSegment(it) }

        /** The caller's query parameters, each name and value re-encoded; empty when there are none. */
        fun query(call: ApplicationCall): String {
            val pairs =
                call.request.queryParameters.entries().flatMap { (name, values) ->
                    values.map { encode(name, QUERY_SAFE) + "=" + encode(it, QUERY_SAFE) }
                }
            return if (pairs.isEmpty()) "" else "?" + pairs.joinToString("&")
        }

        /** `,` and `:` stay literal in a query value: lists and timestamps would triple in length otherwise. */
        private const val QUERY_SAFE = ",:"

        private fun encode(
            value: String,
            alsoSafe: String,
        ): String {
            val out = StringBuilder(value.length)
            for (byte in value.toByteArray(Charsets.UTF_8)) {
                val c = (byte.toInt() and 0xff).toChar()
                if (c in 'A'..'Z' || c in 'a'..'z' || c in '0'..'9' || c in "-._~" || c in alsoSafe) {
                    out.append(c)
                } else {
                    out.append('%').append(HEX[(byte.toInt() shr 4) and 0xf]).append(HEX[byte.toInt() and 0xf])
                }
            }
            return out.toString()
        }

        private const val HEX = "0123456789ABCDEF"
    }
}
