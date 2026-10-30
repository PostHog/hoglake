package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.model.HoglakeException
import io.github.oshai.kotlinlogging.KotlinLogging
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap

/** One statement of a hoglite transaction (`/v1/t/{tid}/tx`), or the single statement of a `/query`. */
data class HogliteStatement(
    val sql: String,
    val args: List<Any?> = emptyList(),
    /** RowsAffected must equal this, or the whole transaction rolls back (409, `guard_index`). */
    val expectAffected: Long? = null,
    /** First column of the first row must equal this, or the whole transaction rolls back. */
    val expectScalarInt: Long? = null,
)

/** One statement's result: column names, rows, and the affected-row count. */
class HogliteQueryResult(
    val cols: List<String>,
    val rows: List<List<Any?>>,
    val affected: Long,
) {
    /** The one row a scalar query returns, or null. */
    fun firstRow(): List<Any?>? = rows.firstOrNull()
}

/** Long coercion for hoglite's JSON numbers (Jackson hands back Int/Long/Double by size). */
fun Any?.asLong(): Long =
    when (this) {
        null -> throw IllegalStateException("expected a number, got null")
        is Number -> toLong()
        else -> throw IllegalStateException("expected a number, got $this")
    }

/** Nullable variant of [asLong]. */
fun Any?.asLongOrNull(): Long? = (this as? Number)?.toLong()

fun Any?.asString(): String = this?.toString() ?: throw IllegalStateException("expected a string, got null")

/**
 * A transaction guard failed: hoglite answered 409 `expect_failed`.
 * [statementIndex] is 0-based (hoglite's `guard_index` is 1-based; the
 * subtraction happens at the boundary). Deterministic — retrying the
 * same statements can only fail the same guard again.
 */
class HogliteGuardFailure(
    val statementIndex: Int,
    detail: String,
) : RuntimeException(detail)

/**
 * hoglite refused the call deterministically and it is NOT a guard:
 * a 422 (SQL error — constraint violation, malformed statement), a 404
 * (unknown tenant), or a 409 from `/query` (whose error channel has no
 * guard index). Never retried by the client; the store maps it to the
 * typed hoglake refusal it means.
 */
class HogliteRejected(
    val status: Int,
    val body: String,
) : RuntimeException("hoglite $status: $body")

/**
 * The hoglite side of the hoglake backend: tenant lifecycle plus the
 * two-statement verbs (`tx`, `query`), with the routing and retry
 * contract hoglite's ownership model demands.
 *
 * ## Routing
 *
 * Every tenant has exactly one owner pod at a time. Calls go to the
 * standard pool's service and hoglite's owner-proxy forwards them; a
 * 421 answer carries a hint — `class: "whale"` pins the tenant to the
 * whale pool's service, `owner: <pod>` pins it to the pod directly
 * (through [ownerUrlPattern], when configured). A pin is a CACHE, not a
 * fact: a transport error against an owner pin clears it so the next
 * attempt re-resolves through the proxy. A whale class pin wins over an
 * owner pin, because a promoted tenant's owner lives in the whale pool
 * and the old standard-pool name would misroute.
 *
 * ## Retry taxonomy
 *
 * Only three outcomes retry: 421 (re-route), 503 (owner lost
 * mid-flight, lease not yet expired), and transport errors (dead pod
 * under a stale pin). Each retry of a `tx` carries the SAME caller-fixed
 * `txid` and payload fingerprint, so a retry of a commit hoglite
 * already durably applied is answered from its dedup table —
 * exactly-once is hoglite's, the client just has to not change the key.
 * Everything else — 409 guard, 422 SQL, 404, 400 — is deterministic and
 * surfaces on first sight; spending retry budget on an impossible
 * request just delays the caller's refusal.
 *
 * ## Circuit breaking
 *
 * One breaker per POOL (standard, whale) — the pools fail
 * independently. Only TRANSPORT failures count toward tripping: a 503
 * is hoglite's per-tenant owner-loss signal, and folding it into a
 * pool-wide breaker would let one tenant's failover fail every OTHER
 * tenant fast for the open window. Any HTTP answer (2xx, 4xx, 421,
 * 503) proves the pool is up and resets the count. An open breaker
 * fails fast with [HoglakeException.BackendUnavailable] until
 * [circuitOpenMs] has passed, then lets one trial through (half-open:
 * success closes, failure re-opens). This is what converts a down pool
 * into instant typed 503s instead of timeout convoys — hoglake's
 * Postgres path fails fast through Hikari's 5 s connectionTimeout, and
 * this is the hoglite equivalent of that contract.
 */
class HogliteClient(
    private val standardUrl: String,
    private val whaleUrl: String = "",
    private val ownerUrlPattern: String = "",
    private val connectTimeoutMs: Long = 2_000,
    private val readTimeoutMs: Long = 5_000,
    private val commitTimeoutMs: Long = 10_000,
    private val retryBudgetMs: Long = 8_000,
    circuitFailureThreshold: Int = 10,
    circuitOpenMs: Long = 5_000,
    /** Test hook: the retry sleep. Production sleeps the thread. */
    private val sleeper: (Long) -> Unit = { Thread.sleep(it) },
) : AutoCloseable {
    private val log = KotlinLogging.logger {}

    private val http: HttpClient =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(connectTimeoutMs))
            .build()

    private val json: ObjectMapper = jacksonObjectMapper()

    /** Which pool a URL belongs to; breakers and metrics key on it. */
    enum class Pool { STANDARD, WHALE }

    /** A per-tenant routing pin learned from a 421 hint. */
    private sealed class Route {
        /** The tenant is whale-class: its owner lives in the whale pool. */
        data object Whale : Route()

        /** The tenant's owner pod, addressed directly (standard pool only). */
        data class Owner(val url: String) : Route()
    }

    private val tenantRoutes = ConcurrentHashMap<String, Route>()

    private val breakers =
        mapOf(
            Pool.STANDARD to CircuitBreaker(circuitFailureThreshold, circuitOpenMs),
            Pool.WHALE to CircuitBreaker(circuitFailureThreshold, circuitOpenMs),
        )

    // ---- public API -------------------------------------------------------

    /**
     * Run [statements] as one hoglite transaction on [tenant], exactly
     * once under retries: [txid] is caller-fixed and [fingerprint]
     * identifies the logical payload, so a replay is answered from
     * hoglite's dedup table and a key reused for a DIFFERENT payload is
     * a 422 rather than a silent second apply.
     *
     * Throws [HogliteGuardFailure] / [HogliteRejected] for deterministic
     * refusals and [HoglakeException.BackendUnavailable] when the retry
     * budget is exhausted or the pool's breaker is open.
     */
    fun tx(
        tenant: String,
        txid: String,
        fingerprint: String?,
        statements: List<HogliteStatement>,
    ): List<HogliteQueryResult> {
        require(statements.isNotEmpty()) { "a hoglite transaction needs at least one statement" }
        val body =
            json.writeValueAsBytes(
                buildMap {
                    put("client", CLIENT_NAME)
                    put("txid", txid)
                    if (fingerprint != null) put("fp", fingerprint)
                    put("statements", statements.map { it.toWire() })
                },
            )
        return call(tenant, "/v1/t/$tenant/tx", body, commitTimeoutMs) { status, bytes ->
            when (status) {
                200 -> parseTxResults(bytes)
                409 -> throw guardFailure(bytes)
                else -> throw HogliteRejected(status, String(bytes))
            }
        }
    }

    /** A read-only single-statement query against [tenant]. Same routing and retry rules as [tx]. */
    fun query(
        tenant: String,
        sql: String,
        args: List<Any?> = emptyList(),
    ): HogliteQueryResult {
        val body = json.writeValueAsBytes(mapOf("sql" to sql, "args" to args))
        return call(tenant, "/v1/t/$tenant/query", body, readTimeoutMs) { status, bytes ->
            when (status) {
                200 -> parseQueryResult(bytes)
                // /query's error channel is a bare 409 with no guard index:
                // a SQL error, not a conflict a caller can re-prepare for.
                else -> throw HogliteRejected(status, String(bytes))
            }
        }
    }

    /**
     * Create [tenant], idempotently: 200 is "created" and a 409/422
     * from a racing or replayed create on an existing tenant is success
     * too — the tenant exists either way, which is all the caller
     * needs. Anything else is transient and retries within the budget.
     */
    fun createTenant(tenant: String) {
        call(tenant, "/v1/tenants/$tenant", null, readTimeoutMs, method = "PUT") { status, bytes ->
            when (status) {
                200, 409, 422 -> Unit
                else -> throw HogliteRejected(status, String(bytes))
            }
        }
    }

    /**
     * Delete [tenant]'s durable state. hoglite routes the delete to the
     * live owner exactly like `/tx` (proxied, or 421 with the owner
     * hint, which the call loop follows) and the deletion executes AT
     * the owner — fence first ([release]) so nothing re-materializes it.
     * A 404 is success: the tenant is gone either way. (An old engine
     * still answers 409 for a live remote owner; callers tolerate and
     * retry it as transitional during rolling upgrades.)
     */
    fun deleteTenant(tenant: String) {
        call(tenant, "/v1/tenants/$tenant", null, readTimeoutMs, method = "DELETE") { status, bytes ->
            when (status) {
                200, 404 -> Unit
                else -> throw HogliteRejected(status, String(bytes))
            }
        }
    }

    /** Release [tenant]'s ownership (optionally reclassing it, e.g. `whale`); 409 means not owned here. */
    fun release(
        tenant: String,
        newClass: String? = null,
    ) {
        val query = if (newClass != null) "?class=$newClass" else ""
        call(tenant, "/v1/t/$tenant/release$query", null, readTimeoutMs) { status, bytes ->
            when (status) {
                200, 409 -> Unit
                else -> throw HogliteRejected(status, String(bytes))
            }
        }
    }

    /**
     * `/v1/healthz` of the standard pool, one attempt, no retry, and
     * PAST the breaker: the probe's job is to observe the pool, and a
     * tripped breaker over a recovered pool must read healthy. Never
     * throws — the readiness contract (HealthProbe) is a Boolean.
     */
    fun reachable(): Boolean =
        runCatching {
            val resp = send(standardUrl, "/v1/healthz", null, connectTimeoutMs, "GET")
            resp.statusCode() == 200
        }.getOrElse {
            log.warn(it) { "hoglite health probe failed: the standard pool did not answer" }
            false
        }

    // ---- the call loop ------------------------------------------------------

    /**
     * One logical call, retried per the taxonomy in the class KDoc.
     * [parse] turns a final HTTP answer into the result or the
     * deterministic exception it carries; the loop owns everything
     * retryable.
     */
    private fun <T> call(
        tenant: String,
        path: String,
        body: ByteArray?,
        timeoutMs: Long,
        method: String = "POST",
        parse: (Int, ByteArray) -> T,
    ): T {
        val deadline = System.nanoTime() + retryBudgetMs * 1_000_000
        var lastFailure: Throwable? = null
        while (true) {
            val pin = tenantRoutes[tenant]
            val (pool, base) = routeFor(pin)
            val breaker = breakers.getValue(pool)
            if (!breaker.admit()) {
                throw HoglakeException.BackendUnavailable(
                    "hoglite ${pool.name.lowercase()} pool circuit is open " +
                        "(${breaker.consecutiveFailures} consecutive failures); refusing fast for " +
                        "${breaker.openMs}ms rather than queueing behind timeouts",
                )
            }
            // Never START an attempt that cannot fit the budget: cap
            // the attempt's timeout at what is left so the worst case is
            // the budget, not budget + one full timeout.
            val remainingMs = (deadline - System.nanoTime()) / 1_000_000
            if (remainingMs <= 0) {
                throw HoglakeException.BackendUnavailable(
                    "hoglite ${pool.name.lowercase()} pool did not answer within the " +
                        "${retryBudgetMs}ms retry budget (last failure: ${lastFailure?.message})",
                )
            }
            val resp =
                try {
                    send(base, path, body, minOf(timeoutMs, remainingMs), method)
                } catch (e: InterruptedException) {
                    // Shutdown, not a backend failure: restore the flag and
                    // stop retrying.
                    Thread.currentThread().interrupt()
                    throw HoglakeException.BackendUnavailable(
                        "interrupted while calling hoglite (${pool.name.lowercase()} pool)",
                    )
                } catch (e: Exception) {
                    // Transport failure. A stale OWNER pin (dead pod, moved
                    // tenant) must not pin forever: clear it so the next
                    // attempt re-resolves through the service proxy. Only
                    // THIS outcome feeds the breaker — a 503 is per-tenant
                    // owner loss, not a pool outage.
                    if (pin is Route.Owner) tenantRoutes.remove(tenant, pin)
                    breaker.onFailure()
                    lastFailure = e
                    sleeper(RETRY_BACKOFF_MS)
                    continue
                }
            // Any HTTP answer at all proves the pool is up: the breaker
            // only tracks transport silence.
            breaker.onSuccess()
            when (resp.statusCode()) {
                200 -> {
                    return parse(200, resp.body())
                }
                503 -> {
                    // Owner lost mid-flight / transient infra: retryable.
                    lastFailure = HogliteRejected(503, String(resp.body()))
                    sleeper(RETRY_BACKOFF_MS)
                }
                421 -> {
                    // Misdirected: re-pin from the hint and retry. Class wins
                    // over owner — a whale's owner lives in the whale pool.
                    // The hint can name a class or owner that is itself
                    // about to move (lease wait-out), so this arm backs off
                    // like the others instead of spinning hot.
                    repin(tenant, resp.body())
                    lastFailure = HogliteRejected(421, String(resp.body()))
                    sleeper(RETRY_BACKOFF_MS)
                }
                else -> {
                    // Deterministic: the pool answered and the answer is
                    // final.
                    return parse(resp.statusCode(), resp.body())
                }
            }
        }
    }

    /** The pool and base URL one attempt uses, given the tenant's pin. */
    private fun routeFor(pin: Route?): Pair<Pool, String> =
        when (pin) {
            null -> Pool.STANDARD to standardUrl
            is Route.Whale -> Pool.WHALE to (whaleUrl.ifBlank { standardUrl })
            is Route.Owner -> Pool.STANDARD to pin.url
        }

    /** Apply a 421 hint to the tenant's pin. */
    private fun repin(
        tenant: String,
        body: ByteArray,
    ) {
        val hint =
            runCatching { json.readValue<Map<String, Any?>>(body) }.getOrNull()
        val hintClass = hint?.get("class") as? String
        val owner = hint?.get("owner") as? String
        when {
            hintClass == "whale" -> tenantRoutes[tenant] = Route.Whale
            // Owner-direct routing needs the chart's headless-name pattern;
            // without it the service proxy is the route and no pin is kept.
            !owner.isNullOrBlank() && ownerUrlPattern.isNotBlank() -> {
                // Per-boot ids carry a ".nonce" suffix; the stable pod name
                // is what the headless service resolves.
                val stable = owner.substringBefore('.')
                tenantRoutes[tenant] = Route.Owner(ownerUrlPattern.replace("{owner}", stable))
            }
            else -> tenantRoutes.remove(tenant)
        }
    }

    private fun send(
        base: String,
        path: String,
        body: ByteArray?,
        timeoutMs: Long,
        method: String,
    ): HttpResponse<ByteArray> {
        val builder =
            HttpRequest.newBuilder(URI.create(base + path))
                .timeout(Duration.ofMillis(timeoutMs))
                .header("Content-Type", "application/json")
        when (method) {
            "GET" -> builder.GET()
            "DELETE" -> builder.DELETE()
            "PUT" ->
                builder.PUT(
                    body?.let(HttpRequest.BodyPublishers::ofByteArray) ?: HttpRequest.BodyPublishers.noBody(),
                )
            else ->
                builder.POST(
                    body?.let(HttpRequest.BodyPublishers::ofByteArray) ?: HttpRequest.BodyPublishers.noBody(),
                )
        }
        return http.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray())
    }

    // ---- wire codecs --------------------------------------------------------

    private fun HogliteStatement.toWire(): Map<String, Any?> =
        buildMap {
            put("sql", sql)
            put("args", args)
            if (expectAffected != null) put("expect_affected", expectAffected)
            if (expectScalarInt != null) put("expect_scalar_int", expectScalarInt)
        }

    private fun parseTxResults(bytes: ByteArray): List<HogliteQueryResult> {
        val decoded = json.readValue<Map<String, Any?>>(bytes)

        @Suppress("UNCHECKED_CAST")
        val results = decoded["results"] as? List<Map<String, Any?>> ?: emptyList()
        return results.map { it.toQueryResult() }
    }

    private fun parseQueryResult(bytes: ByteArray): HogliteQueryResult =
        json.readValue<Map<String, Any?>>(bytes).toQueryResult()

    private fun Map<String, Any?>.toQueryResult(): HogliteQueryResult {
        @Suppress("UNCHECKED_CAST")
        return HogliteQueryResult(
            cols = (this["cols"] as? List<String>) ?: emptyList(),
            rows = (this["rows"] as? List<List<Any?>>) ?: emptyList(),
            affected = this["affected"].asLongOrNull() ?: 0,
        )
    }

    /**
     * The 409 a failed guard arrives as. hoglite's `guard_index` is
     * 1-based and wins when present; the message parse
     * ("expectation failed at statement N", 0-based) is the fallback
     * for older builds.
     */
    private fun guardFailure(bytes: ByteArray): HogliteGuardFailure {
        val body = String(bytes)
        val decoded = runCatching { json.readValue<Map<String, Any?>>(bytes) }.getOrNull()
        val error = decoded?.get("error") as? String ?: body
        val fromField = (decoded?.get("guard_index") as? Number)?.toInt()
        if (fromField != null && fromField > 0 && error.contains("expect_failed")) {
            return HogliteGuardFailure(fromField - 1, error)
        }
        val m = STMT_INDEX_REGEX.find(body)
        return HogliteGuardFailure(m?.groupValues?.get(1)?.toInt() ?: -1, error)
    }

    override fun close() {
        // java.net.http.HttpClient owns daemon-managed resources; nothing
        // to close explicitly on this JDK.
    }

    /** Per-pool circuit breaker; see the class KDoc for the contract. */
    internal class CircuitBreaker(
        private val threshold: Int,
        val openMs: Long,
    ) {
        private var failures = 0
        private var openSinceNanos = 0L

        val consecutiveFailures: Int get() = failures

        /** True when a call may proceed. A tripped breaker re-admits one trial after [openMs]. */
        @Synchronized
        fun admit(): Boolean {
            if (threshold <= 0 || failures < threshold) return true
            if (openSinceNanos == 0L) openSinceNanos = System.nanoTime()
            if ((System.nanoTime() - openSinceNanos) / 1_000_000 < openMs) return false
            // Half-open: let ONE trial through. A failure re-opens with a
            // fresh clock; a success closes in onSuccess.
            openSinceNanos = System.nanoTime()
            return true
        }

        @Synchronized
        fun onSuccess() {
            failures = 0
            openSinceNanos = 0L
        }

        @Synchronized
        fun onFailure() {
            failures++
            if (failures < threshold) openSinceNanos = 0L
        }
    }

    companion object {
        /** Build the client from the process config's HOGLAKE_HOGLITE_* knobs. */
        fun fromConfig(cfg: com.posthog.hoglake.Config): HogliteClient =
            HogliteClient(
                standardUrl = cfg.hogliteUrl,
                whaleUrl = cfg.hogliteWhaleUrl,
                ownerUrlPattern = cfg.hogliteOwnerUrlPattern,
                connectTimeoutMs = cfg.hogliteConnectTimeoutMs,
                readTimeoutMs = cfg.hogliteReadTimeoutMs,
                commitTimeoutMs = cfg.hogliteCommitTimeoutMs,
                retryBudgetMs = cfg.hogliteRetryBudgetMs,
                circuitFailureThreshold = cfg.hogliteCircuitFailureThreshold,
                circuitOpenMs = cfg.hogliteCircuitOpenMs,
            )

        /** The `client` half of hoglite's (client, txid) dedup key. */
        const val CLIENT_NAME = "hoglake"

        /** Pause between retryable attempts; 421 re-routes skip it. */
        const val RETRY_BACKOFF_MS = 200L

        private val STMT_INDEX_REGEX = Regex("expectation failed at statement (\\d+)")
    }
}
