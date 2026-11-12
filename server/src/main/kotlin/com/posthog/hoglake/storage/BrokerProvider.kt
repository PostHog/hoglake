package com.posthog.hoglake.storage

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.charset.StandardCharsets
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.OffsetDateTime
import java.time.format.DateTimeParseException
import java.util.Base64
import java.util.concurrent.ConcurrentHashMap

/**
 * The `broker` storage provider (docs/auth.md, "Storage credentials"):
 * the server holds no cloud identity of its own and asks an HTTP broker
 * for one short-lived session per (catalog, access), under its own
 * OAuth client credential.
 *
 * Two calls, both the shapes the clients already use:
 *
 * - `POST <tokenUrl>`, `grant_type=client_credentials&audience=<audience>`
 *   with HTTP Basic `clientId:clientSecret`, answered with
 *   `{"access_token", "expires_in"}`. Cached until shortly before it
 *   expires; re-fetched once when the broker answers 401.
 * - `POST <brokerUrl>` with `{"catalog", "prefix", "access"}` as a JSON
 *   body and the token as a bearer, answered with
 *   `{"access_key_id", "secret_access_key", "session_token", "expiration"}`
 *   — STS's shape, `expiration` RFC 3339.
 *
 * One session per key, in memory only. A session is refreshed once it
 * is within [refreshMargin] of expiring; a refresh that FAILS keeps the
 * session already held for as long as it is still valid, and only a key
 * with nothing valid surfaces the failure to the job. Refreshes are
 * single-flight per key, so N threads of one cleanup drain cost one
 * vend. Nothing here is ever logged beyond catalog, access and expiry:
 * not the token, not the keys.
 */
class BrokerProvider(
    private val brokerUrl: URI,
    private val tokenUrl: URI,
    private val clientId: String,
    private val clientSecret: String,
    private val audience: String,
    private val refreshMargin: Duration,
    private val http: HttpClient = defaultHttpClient(),
    private val clock: Clock = Clock.systemUTC(),
    private val requestTimeout: Duration = DEFAULT_REQUEST_TIMEOUT,
) : StorageCredentialProvider {
    init {
        BrokerUrls.require("broker url", brokerUrl.toString())
        BrokerUrls.require("token url", tokenUrl.toString())
        require(clientId.isNotBlank()) { "broker client id must not be blank" }
        require(clientSecret.isNotBlank()) { "broker client secret must not be blank" }
        require(audience.isNotBlank()) { "broker audience must not be blank" }
        require(!refreshMargin.isNegative) { "refresh margin must not be negative, got $refreshMargin" }
    }

    private val log = KotlinLogging.logger {}
    private val json = ObjectMapper()

    override val scoped: Boolean = true

    /** The grant's identity: what the broker was asked for. */
    internal data class Key(val catalog: String, val prefix: String, val access: StorageAccess)

    /**
     * A vended session: when the broker said it ends, and when this
     * provider starts refreshing it — [refreshMargin] before the end, or
     * half way through a lifetime shorter than two margins, so a
     * short-lived session is still cached for half its life instead of
     * being re-vended on every request.
     */
    internal class Session(val credentials: AwsCredentials, val expiration: Instant, val refreshAt: Instant)

    private class Token(val value: String, val expiration: Instant, val refreshAt: Instant)

    /**
     * When a failed refresh may next be retried, per credential. Between
     * attempts every request reuses the held, still-valid credential
     * without calling the broker: without this, a broker outage inside
     * the margin cost every S3 request one broker call — up to the
     * request timeout each, serialized behind one lock. The delay starts
     * at [BACKOFF_INITIAL], doubles to [BACKOFF_MAX], and resets on
     * success. It gates REFRESH only: with nothing valid held there is
     * nothing to serve, so every request tries.
     */
    private class Retry(var notBefore: Instant = Instant.MIN, var delay: Duration = BACKOFF_INITIAL) {
        fun failed(now: Instant) {
            notBefore = now.plus(delay)
            delay = minOf(delay.multipliedBy(2), BACKOFF_MAX)
        }

        fun succeeded() {
            notBefore = Instant.MIN
            delay = BACKOFF_INITIAL
        }
    }

    private val sessions = ConcurrentHashMap<Key, Session>()
    private val locks = ConcurrentHashMap<Key, Any>()
    private val retries = ConcurrentHashMap<Key, Retry>()

    @Volatile
    private var token: Token? = null
    private val tokenLock = Any()
    private val tokenRetry = Retry()

    override fun credentialsFor(
        catalog: String,
        prefix: String,
        access: StorageAccess,
    ): AwsCredentialsProvider {
        require(catalog.isNotBlank()) { "catalog must not be blank" }
        val key = Key(catalog, prefix, access)
        return object : AwsCredentialsProvider {
            override fun resolveCredentials(): AwsCredentials = session(key).credentials

            override fun toString(): String = "broker($catalog, ${access.wire})"
        }
    }

    override fun unscoped(): AwsCredentialsProvider = RefusingCredentials

    /** Sessions currently held, for tests and the health page; never the keys. */
    internal fun held(): Map<Key, Instant> = sessions.mapValues { it.value.expiration }

    internal fun session(key: Key): Session {
        val now = clock.instant()
        sessions[key]?.let { if (now.isBefore(it.refreshAt)) return it }
        synchronized(locks.computeIfAbsent(key) { Any() }) {
            val again = sessions[key]
            val afterLock = clock.instant()
            if (again != null && afterLock.isBefore(again.refreshAt)) return again
            val retry = retries.computeIfAbsent(key) { Retry() }
            val heldValid = again != null && again.expiration.isAfter(afterLock)
            // A refresh that just failed is not retried by the next
            // request in the queue: the session it would refresh is
            // still good, so hand that out until the backoff passes.
            if (heldValid && afterLock.isBefore(retry.notBefore)) return again!!
            return try {
                vend(key).also { fresh ->
                    sessions[key] = fresh
                    retry.succeeded()
                    log.info {
                        "storage broker vended a ${key.access.wire} session for catalog '${key.catalog}' " +
                            "expiring at ${fresh.expiration}"
                    }
                }
            } catch (e: Exception) {
                retry.failed(clock.instant())
                if (heldValid) {
                    // Still valid: keep working on it and try again once
                    // the backoff passes. The margin exists for exactly this.
                    log.warn(e) {
                        "storage broker refresh failed for catalog '${key.catalog}' (${key.access.wire}); " +
                            "keeping the held session until ${again!!.expiration}, next attempt at ${retry.notBefore}"
                    }
                    again!!
                } else {
                    throw StorageCredentialException(
                        "storage broker could not vend a ${key.access.wire} session for catalog '${key.catalog}': " +
                            e.message,
                        e,
                    )
                }
            }
        }
    }

    /**
     * When a credential received at [now] and ending at [expiration] is
     * refreshed: [refreshMargin] before the end, capped at half the
     * lifetime so a lifetime at or below the margin still yields a cache
     * window instead of a credential that is refresh-due on arrival.
     */
    private fun refreshAt(
        now: Instant,
        expiration: Instant,
    ): Instant {
        val lifetime = Duration.between(now, expiration)
        val lead = minOf(refreshMargin, lifetime.dividedBy(2))
        return expiration.minus(lead)
    }

    private fun vend(key: Key): Session {
        var response = post(key, bearer())
        if (response.statusCode() == 401) {
            // The token the broker refused may have been revoked ahead of
            // its lifetime; one fresh token, one retry, then the failure
            // is the failure.
            response = post(key, bearer(force = true))
        }
        if (response.statusCode() != 200) {
            throw IllegalStateException(
                "broker answered ${response.statusCode()} for ${key.access.wire} on '${key.catalog}'" +
                    describe(response),
            )
        }
        val body = parse(response.body())
        val accessKeyId = body.text("access_key_id")
        val secretAccessKey = body.text("secret_access_key")
        // STS sessions carry a token; a broker fronting a secrets engine
        // that hands out per-catalog keys has none to give, and those
        // keys expire all the same.
        val sessionToken = body.get("session_token")?.takeIf { it.isTextual }?.asText().orEmpty()
        val expiration = parseExpiration(body.text("expiration"))
        val now = clock.instant()
        require(!expiration.isBefore(now)) { "broker vended a session that has already expired" }
        val credentials =
            if (sessionToken.isEmpty()) {
                AwsBasicCredentials.create(accessKeyId, secretAccessKey)
            } else {
                AwsSessionCredentials.create(accessKeyId, secretAccessKey, sessionToken)
            }
        return Session(credentials, expiration, refreshAt(now, expiration))
    }

    private fun post(
        key: Key,
        bearer: String,
    ): HttpResponse<ByteArray> {
        val payload =
            json.createObjectNode()
                .put("catalog", key.catalog)
                .put("prefix", key.prefix)
                .put("access", key.access.wire)
        val request =
            HttpRequest.newBuilder(brokerUrl)
                .timeout(requestTimeout)
                .header("Authorization", "Bearer $bearer")
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(json.writeValueAsBytes(payload)))
                .build()
        return http.send(request, HttpResponse.BodyHandlers.ofByteArray())
    }

    /**
     * The bearer for the broker: the cached token until its own refresh
     * moment, else a fresh exchange. [force] (a 401) skips the cache and
     * the backoff: the broker has just said the held token is no good.
     */
    private fun bearer(force: Boolean = false): String {
        val now = clock.instant()
        token?.let { if (!force && now.isBefore(it.refreshAt)) return it.value }
        synchronized(tokenLock) {
            val again = token
            val afterLock = clock.instant()
            if (again != null && !force) {
                if (afterLock.isBefore(again.refreshAt)) return again.value
                if (again.expiration.isAfter(afterLock) && afterLock.isBefore(tokenRetry.notBefore)) return again.value
            }
            return try {
                exchange().also {
                    token = it
                    tokenRetry.succeeded()
                }.value
            } catch (e: Exception) {
                tokenRetry.failed(clock.instant())
                if (!force && again != null && again.expiration.isAfter(afterLock)) {
                    log.warn(
                        e,
                    ) { "storage broker token refresh failed; keeping the held token until ${again.expiration}" }
                    again.value
                } else {
                    throw e
                }
            }
        }
    }

    private fun exchange(): Token {
        val basic =
            Base64.getEncoder().encodeToString("$clientId:$clientSecret".toByteArray(StandardCharsets.UTF_8))
        val form =
            "grant_type=client_credentials&audience=" + URLEncoder.encode(audience, StandardCharsets.UTF_8)
        val request =
            HttpRequest.newBuilder(tokenUrl)
                .timeout(requestTimeout)
                .header("Authorization", "Basic $basic")
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(form))
                .build()
        val response = http.send(request, HttpResponse.BodyHandlers.ofByteArray())
        if (response.statusCode() != 200) {
            throw IllegalStateException("token url answered ${response.statusCode()}" + describe(response))
        }
        val body = parse(response.body())
        val value = body.text("access_token")
        val expiresIn = body.get("expires_in")?.takeIf { it.isNumber }?.asLong() ?: DEFAULT_TOKEN_LIFETIME_SECONDS
        require(expiresIn > 0) { "token response carries a non-positive expires_in" }
        val now = clock.instant()
        val expiration = now.plusSeconds(expiresIn)
        return Token(value, expiration, refreshAt(now, expiration))
    }

    private fun parse(bytes: ByteArray): JsonNode =
        try {
            json.readTree(bytes).also { require(it != null && it.isObject) { "response is not a JSON object" } }
        } catch (e: java.io.IOException) {
            throw IllegalStateException("response is not JSON: ${e.message}", e)
        }

    private fun JsonNode.text(field: String): String {
        val node = get(field)
        require(node != null && node.isTextual && node.asText().isNotEmpty()) { "response lacks '$field'" }
        return node.asText()
    }

    private fun parseExpiration(raw: String): Instant =
        try {
            OffsetDateTime.parse(raw).toInstant()
        } catch (e: DateTimeParseException) {
            throw IllegalStateException("expiration '$raw' is not RFC 3339", e)
        }

    /**
     * A status line's worth of body, for the error message. The broker's
     * error bodies carry a reason, never a credential; a 200 body is
     * never described.
     */
    private fun describe(response: HttpResponse<ByteArray>): String {
        val text = String(response.body(), StandardCharsets.UTF_8).trim()
        if (text.isEmpty()) return ""
        return ": " + text.take(ERROR_BODY_CHARS).replace('\n', ' ')
    }

    override fun close() {
        sessions.clear()
        retries.clear()
        token = null
    }

    override fun toString(): String = "broker($brokerUrl)"

    companion object {
        val DEFAULT_REQUEST_TIMEOUT: Duration = Duration.ofSeconds(10)
        const val DEFAULT_TOKEN_LIFETIME_SECONDS = 300L
        val BACKOFF_INITIAL: Duration = Duration.ofSeconds(5)
        val BACKOFF_MAX: Duration = Duration.ofSeconds(60)
        private const val ERROR_BODY_CHARS = 200

        private fun defaultHttpClient(): HttpClient =
            HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(5))
                .followRedirects(HttpClient.Redirect.NEVER)
                .build()
    }
}
