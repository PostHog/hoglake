package com.posthog.hoglake.store

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.BuildInfo
import com.posthog.hoglake.auth.Jwt
import com.posthog.hoglake.auth.JwtViolation
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Metrics
import io.github.oshai.kotlinlogging.KotlinLogging
import kotlinx.coroutines.suspendCancellableCoroutine
import java.io.ByteArrayOutputStream
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpTimeoutException
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.time.Duration
import java.util.Base64
import java.util.HexFormat
import java.util.UUID
import java.util.concurrent.CancellationException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionStage
import java.util.concurrent.Flow
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.coroutines.resume

/**
 * The token a mount host of auth mode `exchange` is called with
 * ([MountHost.Auth.EXCHANGE]): this server trades the reader's own token
 * for it at the issuer's token endpoint, and keeps it for the reader's
 * next requests.
 *
 * **This file is the whole of the wire contract**, PostHog/hogtower#181
 * ("Events mount exchange" in hogtower's
 * `docs/team-scoped-hoglake-credentials.md`), coded against its commit
 * `6b85ff28`: an RFC 8693 token exchange authenticated as one fleet-wide
 * client. The request, the answer, the refusal codes and the bounds on the
 * client id and secret are all here; nothing else in the server names one
 * of them.
 *
 * ```
 * POST <token_url>
 * Authorization: Basic base64(urlencode(client_id) ":" urlencode(client_secret))
 * Content-Type: application/x-www-form-urlencoded
 *
 * grant_type=urn:ietf:params:oauth:grant-type:token-exchange
 * subject_token=<the reader's token, as presented>
 * subject_token_type=urn:ietf:params:oauth:token-type:access_token
 * resource=urn:posthog:hoglake:catalog:<mount>
 * audience=hoglake
 * ```
 *
 * What it owns:
 * - **Who is asked.** Only the configured token endpoint: no redirects, no
 *   proxy, one deadline for the whole call. The mount name in `resource`
 *   has the reserved shape; the caller was checked as that mount's reader
 *   ([MountEntitlement]) before it got here.
 * - **The cache.** Keyed by the SHA-256 of the subject token's signed part
 *   (`header.payload`) and the mount name, so a token minted from one
 *   subject token is never handed to the bearer of another, and no subject
 *   token is kept. The signature's spelling is not in the key: one token
 *   is one entry however its signature is encoded (ECDSA and base64 each
 *   allow more than one spelling that verifies), so a reader cannot turn
 *   one token into many exchanges. At most [maxEntries],
 *   least recently used first out. An entry is used only while more than
 *   [MARGIN_SECONDS] of the lifetime the issuer stated remain. Concurrent
 *   requests for one key share one exchange. A refusal is kept for
 *   [REFUSAL_SECONDS], so a reader with no mount does not cost a call per
 *   listing; a failure is never kept.
 * - **What is said about it.** A log line names the host by id and gives a
 *   closed reason word, never the endpoint, the client id, the secret or a
 *   token; the errors thrown ([orThrow]) carry none of them either.
 */
class MountTokenExchange(
    /** The mount host this exchange serves; what log lines name. */
    private val hostId: UUID,
    private val config: MountExchange,
    connectTimeoutMs: Long = CONNECT_TIMEOUT_MS,
    private val maxEntries: Int = MAX_ENTRIES,
    private val nanoTime: () -> Long = System::nanoTime,
    private val epochSeconds: () -> Long = { System.currentTimeMillis() / MILLIS },
) : AutoCloseable {
    init {
        require(maxEntries >= 1)
    }

    /** What one request for a mount token came to. */
    sealed interface Outcome {
        /** A token to call the host with. Not a data class: its text must never be printed. */
        class Minted(
            val token: String,
        ) : Outcome

        /** No token for this reader: answered to it as [Refusal.orThrow] says, and cached. */
        data class Refused(
            val refusal: Refusal,
        ) : Outcome

        /** The exchange could not be made; nothing is known about the reader. Never cached. */
        data class Failed(
            val failure: Failure,
            /** One of a closed set of words, for the metric and the log. */
            val reason: String,
        ) : Outcome
    }

    enum class Refusal(
        val reason: String,
    ) {
        /** `invalid_target`: no such mount for this reader's org (another org's, or an org with no federated team). */
        UNKNOWN_MOUNT("invalid_target"),

        /** `invalid_grant`: the issuer will not trade this token (revoked, no team in common, not a reader's). */
        NOT_ENTITLED("invalid_grant"),

        /** The reader's token is too close to its expiry to yield a usable mount token. */
        NEAR_EXPIRY("near_expiry"),

        /** The reader's token is longer than the issuer accepts as a subject. */
        SUBJECT_TOO_LARGE("subject_too_large"),
    }

    enum class Failure {
        /** The token endpoint is unreachable, slow, shedding load, or this server's own cap is full. */
        UNAVAILABLE,

        /** The issuer refused this server's client or its request: configuration to fix, not the reader's fault. */
        MISCONFIGURED,

        /** The token endpoint answered with something that is not the contract. */
        INVALID_RESPONSE,
    }

    private data class Key(
        val subjectDigest: String,
        val mount: String,
    )

    private class Entry(
        val outcome: Outcome,
        val untilNanos: Long,
    )

    private val log = KotlinLogging.logger {}
    private val json = jacksonObjectMapper()
    private val lock = Any()

    /** Guarded by [lock]. Access-ordered, and bounded by [maxEntries]. */
    private val cache =
        object : LinkedHashMap<Key, Entry>(INITIAL_CAPACITY, LOAD_FACTOR, true) {
            override fun removeEldestEntry(eldest: MutableMap.MutableEntry<Key, Entry>): Boolean = size > maxEntries
        }

    /** Guarded by [lock]. The exchanges in flight, one per key. */
    private val inFlight = HashMap<Key, CompletableFuture<Outcome>>()

    /** Guarded by [lock]. Until then no exchange is sent and every miss answers [pausedWith]. */
    private var pausedUntilNanos = 0L
    private var pausedWith: Outcome.Failed? = null

    private val http: HttpClient =
        HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofMillis(connectTimeoutMs))
            // Only the configured endpoint is ever contacted: a redirect is an invalid answer.
            .followRedirects(HttpClient.Redirect.NEVER)
            .proxy(HttpClient.Builder.NO_PROXY)
            .build()

    /** Ends an exchange still running at its deadline; one daemon thread, and a finished exchange leaves no task. */
    private val watchdog =
        ScheduledThreadPoolExecutor(1) { task ->
            Thread(task, "hoglake-mount-exchange-deadline").apply { isDaemon = true }
        }.apply { removeOnCancelPolicy = true }

    /** `Basic` credentials per RFC 6749 §2.3.1: each half form-encoded, then joined and base64'd. */
    private val basic: String =
        "Basic " +
            Base64.getEncoder().encodeToString(
                (form(config.clientId) + ":" + form(config.clientSecret.reveal())).toByteArray(Charsets.UTF_8),
            )

    /** Entries held right now, for tests. */
    internal val size: Int get() = synchronized(lock) { cache.size }

    /**
     * The mount token for the bearer of [subjectToken] on [mount], from the
     * cache or from one exchange shared with every concurrent request for
     * the same pair. [subjectToken] is a token this server has verified,
     * and its bearer has passed [MountEntitlement.mayRead] for [mount].
     */
    suspend fun token(
        subjectToken: String,
        mount: String,
    ): Outcome {
        check(MountNames.isReserved(mount)) { "not a mount name" }
        return when (val found = lookup(subjectToken, mount)) {
            is Lookup.Answered -> found.outcome
            is Lookup.Pending -> found.exchange.awaitShared()
        }
    }

    /** Drop what is cached for [subjectToken] on [mount]: the host refused the token this server presented. */
    fun forget(
        subjectToken: String,
        mount: String,
    ) {
        val key = Key(digest(subjectToken), mount)
        synchronized(lock) { cache.remove(key) }
    }

    private sealed interface Lookup {
        class Answered(
            val outcome: Outcome,
        ) : Lookup

        class Pending(
            val exchange: CompletableFuture<Outcome>,
        ) : Lookup
    }

    private fun lookup(
        subjectToken: String,
        mount: String,
    ): Lookup {
        val key = Key(digest(subjectToken), mount)
        synchronized(lock) { held(key) }?.let { return it }
        // Nothing is sent for a subject the issuer cannot use: one over its
        // length bound (which it would charge to the address every
        // in-cluster caller shares), or one with too little life left to
        // yield a mount token this server would use.
        val local =
            when {
                // Measured as it is sent, so the body stays inside the issuer's bound too.
                form(subjectToken).length > MAX_SUBJECT_CHARS -> Refusal.SUBJECT_TOO_LARGE
                !outlivesMargin(subjectToken) -> Refusal.NEAR_EXPIRY
                else -> null
            }
        if (local != null) return Lookup.Answered(counted(Outcome.Refused(local)))
        val pending = CompletableFuture<Outcome>()
        synchronized(lock) {
            held(key)?.let { return it }
            val now = nanoTime()
            pausedWith?.let { paused ->
                if (pausedUntilNanos - now > 0) {
                    return Lookup.Answered(counted(Outcome.Failed(paused.failure, "paused")))
                }
                pausedWith = null
            }
            if (inFlight.size >= MAX_IN_FLIGHT) {
                return Lookup.Answered(counted(Outcome.Failed(Failure.UNAVAILABLE, "busy")))
            }
            inFlight[key] = pending
        }
        Metrics.mountExchangeCache("miss")
        send(key, subjectToken, pending)
        return Lookup.Pending(pending)
    }

    /** Under [lock]: the live cache entry for [key], or the exchange in flight for it, or null. */
    private fun held(key: Key): Lookup? {
        cache[key]?.let { entry ->
            if (entry.untilNanos - nanoTime() > 0) {
                Metrics.mountExchangeCache("hit")
                return Lookup.Answered(entry.outcome)
            }
            cache.remove(key)
        }
        return inFlight[key]?.let {
            Metrics.mountExchangeCache("shared")
            Lookup.Pending(it)
        }
    }

    /** What one answer came to, and what to remember of it. */
    private class Result(
        val outcome: Outcome,
        /** Keep [outcome] for this key until then; null keeps nothing. */
        val cacheUntilNanos: Long? = null,
        /** Send no exchange for this long: the issuer asked, or refused this server itself. */
        val pauseSeconds: Long = 0,
    )

    /**
     * One exchange, on the HTTP client's own threads: it belongs to every
     * request waiting on [pending], so no one of them being cancelled may
     * abort it. [pending] always completes, with an [Outcome].
     */
    private fun send(
        key: Key,
        subjectToken: String,
        pending: CompletableFuture<Outcome>,
    ) {
        val started = nanoTime()
        val sent =
            try {
                val request =
                    HttpRequest.newBuilder(config.tokenUrl)
                        .timeout(Duration.ofMillis(config.timeoutMs))
                        .header("Authorization", basic)
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .header("Accept", "application/json")
                        .header("User-Agent", USER_AGENT)
                        .POST(
                            HttpRequest.BodyPublishers.ofString(
                                listOf(
                                    "grant_type" to GRANT_TYPE,
                                    "subject_token" to subjectToken,
                                    "subject_token_type" to ACCESS_TOKEN_TYPE,
                                    "resource" to RESOURCE_PREFIX + key.mount,
                                    "audience" to AUDIENCE,
                                ).joinToString("&") { (name, value) -> name + "=" + form(value) },
                            ),
                        ).build()
                http.sendAsync(request) { CappedBody(MAX_BODY_BYTES) }
            } catch (_: Exception) {
                // The JDK quotes an offending header value in its message,
                // and these values are credentials: nothing of it is kept.
                finish(key, started, Result(INVALID), pending, sent = false)
                return
            }
        // The request timeout ends at the answer's headers; this ends the
        // whole call, body included, and closes its connection.
        val deadline = watchdog.schedule({ sent.cancel(true) }, config.timeoutMs, TimeUnit.MILLISECONDS)
        sent.whenComplete { response, failure ->
            deadline.cancel(false)
            val result =
                try {
                    if (failure != null) Result(failed(failure)) else answered(response, started)
                } catch (_: Throwable) {
                    Result(INVALID)
                }
            finish(key, started, result, pending, sent = true)
        }
    }

    /**
     * End the exchange for [key]: whatever else happens here, the key
     * leaves [inFlight] and [pending] completes, so no waiter is left
     * hanging and no slot of the in-flight cap is lost.
     */
    private fun finish(
        key: Key,
        started: Long,
        result: Result,
        pending: CompletableFuture<Outcome>,
        sent: Boolean,
    ) {
        val outcome = result.outcome
        try {
            val now = nanoTime()
            synchronized(lock) {
                inFlight.remove(key)
                result.cacheUntilNanos?.let { cache[key] = Entry(outcome, it) }
                if (result.pauseSeconds > 0 && outcome is Outcome.Failed) {
                    pausedWith = outcome
                    pausedUntilNanos = now + result.pauseSeconds * NANOS
                }
            }
            if (sent) Metrics.mountExchangeSent(now - started)
            if (outcome is Outcome.Failed) log.warn { "mount host $hostId token exchange failed: ${outcome.reason}" }
            counted(outcome)
        } finally {
            synchronized(lock) { inFlight.remove(key) }
            pending.complete(outcome)
        }
    }

    private fun counted(outcome: Outcome): Outcome {
        when (outcome) {
            is Outcome.Minted -> Unit
            is Outcome.Refused -> Metrics.mountExchangeError(outcome.refusal.reason)
            is Outcome.Failed -> Metrics.mountExchangeError(outcome.reason)
        }
        return outcome
    }

    private fun failed(failure: Throwable): Outcome {
        val causes = generateSequence(failure) { it.cause }.take(CAUSE_DEPTH).toList()
        // Only the deadline above cancels the call.
        val timedOut = causes.any { it is HttpTimeoutException || it is CancellationException }
        return Outcome.Failed(Failure.UNAVAILABLE, if (timedOut) "timeout" else "unavailable")
    }

    /** The issuer's answer, and what to remember of it. */
    private fun answered(
        response: HttpResponse<ByteArray>,
        started: Long,
    ): Result {
        val status = response.statusCode()
        // No answer of the contract is anywhere near the cap; one over it is not the contract's.
        if (response.body().size > MAX_BODY_BYTES) return Result(INVALID)
        val body = runCatching { json.readTree(response.body()) }.getOrNull()?.takeIf { it.isObject }
        val error = body?.get("error")?.takeIf(JsonNode::isTextual)?.asText()
        val refusedUntil = nanoTime() + REFUSAL_SECONDS * NANOS
        return when {
            status == OK -> minted(response, body, started)
            // A refusal of this server's own client keeps being a refusal:
            // asking again at request rate would only drain the failure
            // budget its address shares with every other in-cluster caller.
            // 401 is the client's authentication failing, whatever the body says.
            status == UNAUTHORIZED ->
                Result(
                    Outcome.Failed(Failure.MISCONFIGURED, "invalid_client"),
                    pauseSeconds = MISCONFIGURED_PAUSE_SECONDS,
                )
            status == BAD_REQUEST && error in CLIENT_REFUSALS ->
                Result(Outcome.Failed(Failure.MISCONFIGURED, error!!), pauseSeconds = MISCONFIGURED_PAUSE_SECONDS)
            status == TOO_MANY_REQUESTS ->
                Result(
                    Outcome.Failed(Failure.UNAVAILABLE, "rate_limited"),
                    pauseSeconds =
                        response.headers().firstValue("Retry-After").orElse(null)?.trim()?.toLongOrNull()
                            ?.coerceIn(1, MAX_RATE_PAUSE_SECONDS) ?: 1,
                )
            status == SERVICE_UNAVAILABLE -> Result(Outcome.Failed(Failure.UNAVAILABLE, "temporarily_unavailable"))
            status == BAD_REQUEST && error == "invalid_target" ->
                Result(Outcome.Refused(Refusal.UNKNOWN_MOUNT), cacheUntilNanos = refusedUntil)
            status == BAD_REQUEST && error == "invalid_grant" ->
                Result(Outcome.Refused(Refusal.NOT_ENTITLED), cacheUntilNanos = refusedUntil)
            else -> Result(INVALID)
        }
    }

    private fun minted(
        response: HttpResponse<ByteArray>,
        body: JsonNode?,
        started: Long,
    ): Result {
        val invalid = Result(INVALID)
        val contentType = response.headers().firstValue("Content-Type").orElse("").lowercase()
        if (body == null || !contentType.startsWith("application/json")) return invalid
        val token = body.get("access_token")?.takeIf(JsonNode::isTextual)?.asText() ?: return invalid
        // The token is opaque here (the host verifies it); it must only be
        // something that can stand in an Authorization header.
        if (token.length > MAX_TOKEN_CHARS || !COMPACT_JWS.matches(token)) return invalid
        if (body.get("token_type")?.asText()?.equals("Bearer", ignoreCase = true) != true) return invalid
        if (body.get("issued_token_type")?.asText() != ACCESS_TOKEN_TYPE) return invalid
        val lifetime =
            body.get("expires_in")?.takeIf { it.isIntegralNumber && it.canConvertToLong() }?.asLong()
                ?.takeIf { it >= 1 }?.coerceAtMost(MAX_LIFETIME_SECONDS) ?: return invalid
        // The lifetime runs from before the request was sent, so this
        // server's copy never outlasts the issuer's `exp`. The token is
        // used only while more than the margin of it remains.
        val expires = started + lifetime * NANOS
        val until = expires - MARGIN_SECONDS * NANOS
        val now = nanoTime()
        return if (until - now > 0) {
            Result(Outcome.Minted(token), cacheUntilNanos = until)
        } else {
            // Minted with no usable life: the subject is about to expire.
            // Kept as that until it has, so the next request does not ask again.
            Result(
                Outcome.Refused(Refusal.NEAR_EXPIRY),
                cacheUntilNanos = if (expires - (now + NANOS) > 0) expires else now + NANOS,
            )
        }
    }

    /** Has [subjectToken] more than the margin of its own life left? A mount token never outlives its subject. */
    private fun outlivesMargin(subjectToken: String): Boolean =
        try {
            Jwt.seconds(Jwt.parse(subjectToken).claims, "exp") - epochSeconds() > MARGIN_SECONDS
        } catch (_: Exception) {
            false
        }

    /**
     * An answer's body, up to [cap] bytes and one more: enough to tell an
     * answer that is over the cap from one that is not, without holding it
     * or waiting for the rest. Past the cap the transfer is cancelled.
     */
    private class CappedBody(
        private val cap: Int,
    ) : HttpResponse.BodySubscriber<ByteArray> {
        private val body = CompletableFuture<ByteArray>()
        private val buffer = ByteArrayOutputStream()
        private var subscription: Flow.Subscription? = null

        override fun getBody(): CompletionStage<ByteArray> = body

        override fun onSubscribe(subscription: Flow.Subscription) {
            this.subscription = subscription
            subscription.request(Long.MAX_VALUE)
        }

        override fun onNext(items: List<ByteBuffer>) {
            for (item in items) {
                val take = minOf(item.remaining(), cap + 1 - buffer.size())
                if (take > 0) buffer.write(ByteArray(take).also { item.get(it) })
            }
            if (buffer.size() > cap && !body.isDone) {
                subscription?.cancel()
                body.complete(buffer.toByteArray())
            }
        }

        override fun onError(throwable: Throwable) {
            body.completeExceptionally(throwable)
        }

        override fun onComplete() {
            body.complete(buffer.toByteArray())
        }
    }

    /** Await the shared exchange. A waiter that is cancelled leaves; the exchange goes on for the others. */
    private suspend fun CompletableFuture<Outcome>.awaitShared(): Outcome =
        suspendCancellableCoroutine { continuation ->
            whenComplete { outcome, failure ->
                // The exchange always completes with an outcome; a failure here would be a bug in this file.
                continuation.resume(if (failure == null && outcome != null) outcome else INVALID)
            }
        }

    override fun close() {
        watchdog.shutdownNow()
        http.shutdownNow()
    }

    companion object {
        // ---- hogtower#181's wire contract -----------------------------------
        private const val GRANT_TYPE = "urn:ietf:params:oauth:grant-type:token-exchange"
        private const val ACCESS_TOKEN_TYPE = "urn:ietf:params:oauth:token-type:access_token"

        /** RFC 8707: the mount is named as this prefix and its catalog name. */
        internal const val RESOURCE_PREFIX = "urn:posthog:hoglake:catalog:"
        private const val AUDIENCE = "hoglake"

        /** The issuer's refusals of this server's client or request, as against the reader's token. */
        private val CLIENT_REFUSALS =
            setOf("unauthorized_client", "unsupported_grant_type", "invalid_request", "invalid_client")

        /** The fleet client's id: neither a store credential's prefix nor a customer's. */
        private val CLIENT_ID = Regex("^hls_[0-9a-f]{24}$")
        const val CLIENT_ID_RULE = "a mount exchange client_id is hls_ followed by 24 lowercase hex characters"

        fun isClientId(value: String): Boolean = CLIENT_ID.matches(value)

        /** The issuer's bounds on the client secret, in bytes. */
        val SECRET_BYTES = 32..256

        /** The issuer refuses a longer subject, and charges the refusal to the caller's address. */
        internal const val MAX_SUBJECT_CHARS = 12 * 1024

        /** A mount token lives 15 minutes at most; a longer stated lifetime is not believed. */
        private const val MAX_LIFETIME_SECONDS = 900L

        // ---- this server's own bounds ---------------------------------------

        /**
         * A minted token is used only while more than this much of its
         * stated lifetime remains, and a subject token with less than this
         * left is not exchanged. It covers the time from the cache check
         * to the host's own verification (a connect of up to 2 s, and
         * queueing) and the clock difference between the issuer and the
         * host, without leaning on the host's skew allowance. It is under
         * the 60 s at which pyhoglake and the DuckDB client renew a token,
         * so their requests never fall inside it.
         */
        const val MARGIN_SECONDS = 30L

        /** How long a refusal is believed. A newly federated org sees its mount at most this late. */
        const val REFUSAL_SECONDS = 30L

        /** No exchange is sent for this long after the issuer refused this server's client. */
        const val MISCONFIGURED_PAUSE_SECONDS = 10L
        private const val MAX_RATE_PAUSE_SECONDS = 30L

        /** A minted token is 1 to 7 KiB, so this is some tens of megabytes at the very most. */
        const val MAX_ENTRIES = 10_000

        /** Exchanges in flight at once; each is a connection to the issuer. */
        const val MAX_IN_FLIGHT = 32
        const val CONNECT_TIMEOUT_MS = 2_000L
        private const val MAX_BODY_BYTES = 64 * 1024
        private const val MAX_TOKEN_CHARS = 16 * 1024
        private val COMPACT_JWS = Regex("^[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+\\.[A-Za-z0-9_-]+$")
        private const val NANOS = 1_000_000_000L
        private const val MILLIS = 1_000L
        private const val CAUSE_DEPTH = 8
        private const val INITIAL_CAPACITY = 64
        private const val LOAD_FACTOR = 0.75f
        private const val OK = 200
        private const val BAD_REQUEST = 400
        private const val UNAUTHORIZED = 401
        private const val TOO_MANY_REQUESTS = 429
        private const val SERVICE_UNAVAILABLE = 503
        private val USER_AGENT = "hoglake-server/${BuildInfo.version} (mount-client)"

        private val INVALID = Outcome.Failed(Failure.INVALID_RESPONSE, "invalid_response")

        private fun form(value: String): String = URLEncoder.encode(value, Charsets.UTF_8)

        /**
         * What names a subject token in the cache: the digest of the part
         * its signature covers. Every caller holds a token this server
         * verified, so two tokens with the same signed part are the same
         * token, and their signatures' spellings do not matter.
         */
        private fun digest(token: String): String =
            HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(token.substringBeforeLast('.').toByteArray(Charsets.UTF_8)),
            )

        /**
         * The answer [outcome] stands for on a request for [mount], or the
         * minted token. No message names the endpoint, the client or a token.
         */
        fun orThrow(
            outcome: Outcome,
            mount: String,
        ): String =
            when (outcome) {
                is Outcome.Minted -> outcome.token
                is Outcome.Refused ->
                    throw when (outcome.refusal) {
                        // The reader renews its token, as it would a moment later anyway.
                        Refusal.NEAR_EXPIRY ->
                            HoglakeException.Unauthenticated(
                                JwtViolation.EXPIRED,
                                "this token is too close to its expiry to read a mount; renew it",
                            )
                        // Not a mount this reader has: the answer, word for
                        // word, that the auth plugin gives for a catalog a
                        // caller is not entitled to (another org's mount).
                        Refusal.UNKNOWN_MOUNT, Refusal.NOT_ENTITLED, Refusal.SUBJECT_TOO_LARGE ->
                            HoglakeException.NotFound("catalog '$mount' not found")
                    }
                is Outcome.Failed ->
                    throw when (outcome.failure) {
                        Failure.UNAVAILABLE ->
                            HoglakeException.MountExchangeUnavailable(
                                "the token service for this mount could not be reached; retry shortly",
                            )
                        Failure.MISCONFIGURED ->
                            HoglakeException.MountExchangeMisconfigured(
                                "this server's credentials for the mount's token service were refused",
                            )
                        Failure.INVALID_RESPONSE ->
                            HoglakeException.MountExchangeInvalidResponse(
                                "the token service for this mount answered with an unexpected response",
                            )
                    }
            }
    }
}
