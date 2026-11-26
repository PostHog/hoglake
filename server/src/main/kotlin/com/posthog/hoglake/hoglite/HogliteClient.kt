package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.RequestAdmission
import com.posthog.hoglake.api.NamespaceDto
import com.posthog.hoglake.api.ScanFileDto
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Metrics
import io.github.oshai.kotlinlogging.KotlinLogging
import java.net.ConnectException
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpConnectTimeoutException
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

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
 * a 422 `refused` (SQL error — constraint violation, malformed
 * statement — rolled back), a 404 (unknown tenant), or a 409 from
 * `/query` (whose error channel has no guard index). Never retried by
 * the client; the store maps it to the typed hoglake refusal it means.
 * A `/tx` answer that is not KNOWN to be non-durable is never this:
 * see [HoglakeException.CommitOutcomeUnknown].
 */
class HogliteRejected(
    val status: Int,
    val body: String,
) : RuntimeException("hoglite $status: $body")

/*
 * A `/tx` whose outcome the client could not learn surfaces as
 * [HoglakeException.CommitOutcomeUnknown]: raised when the retry budget
 * (or an open breaker, or an interrupt) ends a transaction after at least
 * one attempt that may have applied — a 504 `outcome_unknown`, a
 * transport failure after the request may have been sent, a 503 not
 * labelled `not_applied` — or at once for a 422 not labelled `refused`
 * or any other unlabelled error status (an engine that predates the
 * labels answered 422 for durable-but-unreported commits too).
 */

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
 * Only four outcomes retry: 421 (re-route), 503 (`not_applied`: owner
 * lost mid-flight, lease not yet expired, a read cancelled by a
 * checkpoint), 504 (`outcome_unknown`: a commit applied on an owner that
 * then failed before durability was confirmed), and transport errors
 * (dead pod under a stale pin). Each retry of a `tx` carries the SAME caller-fixed
 * `txid` and payload fingerprint, so a retry of a commit hoglite
 * already durably applied is answered from its dedup table —
 * exactly-once is hoglite's, the client just has to not change the key.
 * Everything else — 409 guard, 422 `refused`, 404, 400 — is
 * deterministic and surfaces on first sight; spending retry budget on an
 * impossible request just delays the caller's refusal.
 *
 * A `tx` keeps three outcomes apart end to end: committed (200),
 * definitely not committed (409 guard, 422 `refused`, 404, or a budget
 * spent only on attempts that cannot have applied, which is
 * [HoglakeException.BackendUnavailable]), and unknown
 * ([HoglakeException.CommitOutcomeUnknown], also a BackendUnavailable
 * but typed, with its own API code, so a caller can tell). An attempt may have applied when it got a 504, a 503
 * not labelled `not_applied`, or a transport failure other than a refused
 * or timed-out connect; once one has, the call can only end committed or
 * unknown.
 *
 * ## Request authority
 *
 * A client bound to one request ([forRequest]) forwards the caller's
 * token on every call, and hoglite verifies it on every call, where
 * PostgreSQL checks it once when the request starts. Once a `tx` of the
 * request has applied (or may have), a later 401 cannot mean "nothing
 * ran": it surfaces as [HoglakeException.CommitOutcomeUnknown] naming
 * that `tx` (marked [HoglakeException.CommitOutcomeUnknown.applied] when
 * it answered 200, and always
 * [HoglakeException.CommitOutcomeUnknown.tokenLapsed]), never as the
 * refusal a caller re-mints its token and replays the whole request on.
 * A 401 that ends a `tx` whose own attempt was ambiguous names that `tx`
 * instead, unless an earlier `tx` is known to have applied, which it
 * names, with the ambiguous one in the detail; marked tokenLapsed
 * whenever an earlier `tx` applied or may have. Only 401: hoglite
 * answers it for every token it cannot verify, an expired one included,
 * and 403 only for a verified token whose authority never covers the
 * call (a tenant mismatch, a non-operator on an operator route), which a
 * retry cannot change, so a 403 stays a refusal. This is the fallback: a long-running
 * route refuses up front a token with too little lifetime left
 * (`CatalogStoreSelector.requireTokenLifetime`), before anything is sent.
 *
 * ## Circuit breaking
 *
 * One breaker per POOL (standard, whale) — the pools fail
 * independently. Only TRANSPORT failures AGAINST THE POOL'S SERVICE
 * count toward tripping: a 503 is hoglite's per-tenant owner-loss
 * signal, and folding it into a pool-wide breaker would let one
 * tenant's failover fail every OTHER tenant fast for the open window.
 * A transport failure against an OWNER pin is the same per-tenant
 * signal in another form (one dead pod, or a tenant that moved while
 * the pin said otherwise), so it clears the pin and is retried through
 * the service, and only the service's own silence feeds the breaker
 * (#679) — before that, one dead pod under enough stale pins opened
 * the standard breaker and failed every tenant for [circuitOpenMs].
 * Any HTTP answer from the service (2xx, 4xx, 421, 503) proves the pool
 * is up and resets the count; an owner's answer, like its silence, says
 * nothing to the breaker. An open breaker fails fast with
 * [HoglakeException.BackendUnavailable] until [circuitOpenMs] has
 * passed, then lets one trial through (half-open: success closes,
 * failure re-opens). An owner-pinned attempt is not that trial — its
 * failure cannot re-open the breaker nor its answer close it, so it
 * must not spend the trial the service's own attempt needs. Were an
 * owner's answer to reset the count, live owners of pinned tenants
 * would keep a dead service's breaker from ever tripping, and the
 * unpinned tenants would pay a full timeout per attempt. This is what
 * converts a down pool into instant typed 503s instead of timeout
 * convoys — hoglake's Postgres path fails fast through Hikari's 5 s
 * connectionTimeout, and this is the hoglite equivalent of that
 * contract.
 *
 * ## Bulkhead
 *
 * Every route handler runs on one bounded dispatcher
 * (`api/BlockingDispatch.kt`, `HOGLAKE_REQUEST_THREADS` wide, sized to
 * the PostgreSQL pool), and a call here can hold its thread for the
 * whole [retryBudgetMs] while an operation makes several. Nothing in
 * the dispatcher knows which backend a request is for, so without a
 * bound here one stalled tenant, or an owner failover across many,
 * takes every thread and the PostgreSQL catalogs — whose database is
 * fine — stop being served (#679). [maxConcurrentCalls] is that bound:
 * a semaphore, shared by every per-request clone of this client, on
 * the REQUEST THREADS a backend may hold at once.
 *
 * The permit is PER REQUEST, not per call. A request takes one on its
 * first gated call and every further call it makes while holding it —
 * on its own thread, or on a fan-out worker [RequestAdmission.carrying]
 * its admission — runs under the same permit; the holder releases it
 * when what it was taken for ends. `HogliteCatalogStore` takes it for a
 * whole operation ([underRequestPermit] around `catalogOperation` and
 * the list), so a multi-step operation is refused before its first
 * step or not at all: a `createCatalog` that has committed its boot
 * transaction is never refused the `getCatalog` after it, and a drop
 * that has fenced is never refused its release (#683 is a transport
 * failure's to cause, not the bulkhead's). A call made outside any
 * operation takes and releases its own.
 *
 * Three things follow from "it bounds threads":
 *
 *  - it gates only work done under a dispatched request (an ambient
 *    [RequestAdmission]). A background loop, and a background sweep's
 *    compaction workers, hold no request thread, so there is nothing
 *    for it to protect, and refusing their calls would fail an
 *    iteration for nobody's benefit. A fan-out the request thread
 *    blocks on IS the request's work — the hoglite list reads its
 *    chunks and retries on a worker pool, and a manual sweep rewrites
 *    and publishes its groups on the executor's, while the request
 *    thread waits — so the admission is carried onto those workers,
 *    and they run under the request's permit and budget rather than as
 *    background;
 *  - a request that finds the backend full waits at most
 *    [BULKHEAD_WAIT_MS] — long enough that a burst of fast operations
 *    one over the cap is served rather than refused — and is then
 *    refused with a typed [HoglakeException.BackendUnavailable] (503,
 *    Retry-After), counted as `hoglake_hoglite_bulkhead_refusals_total`.
 *    No attempt was sent, so a refused `tx` is never outcome-unknown.
 *    A waiter holds a request thread too, and the dispatcher is FIFO
 *    and backend-blind: were every arrival let wait, each refusal would
 *    cost a thread 200 ms, and hoglite traffic above (threads left ÷
 *    0.2 s) during a stall would fill the dispatcher's queue and shed
 *    the PostgreSQL requests behind it. So at most
 *    [BULKHEAD_MAX_WAITERS] wait; one that arrives to find the backend
 *    full AND that many already waiting is refused at once. A backend
 *    holds at most [maxConcurrentCalls] + [BULKHEAD_MAX_WAITERS]
 *    request threads, whatever its arrival rate;
 *  - the permit is held for the operation's whole duration, retry
 *    loops and backoffs included, because that is how long the thread
 *    is held.
 *
 * Every path to the engine on a request's behalf goes through the gate:
 * [call] (and so `tx`, `query`, `queryBatch` and the tenant lifecycle)
 * and the typed scoped reads ([readScopedNamespace], [readScopedTable],
 * [readScopedScan], through [scopedRead]). [reachable] does not; it is
 * the readiness probe's, which bypasses the dispatcher.
 *
 * The budget a request's call runs under is also capped at what the
 * request has left of its admission bound
 * ([RequestAdmission.remainingBudgetMs]: `HOGLAKE_COMMIT_LOCK_TIMEOUT_MS`
 * less the QUEUE WAIT, frozen at admission and floored as the lock
 * bound is). The dispatcher sheds a request that queued PAST the bound;
 * one that queued for most of it used to arrive here and spend the
 * full retry budget on top, holding a thread for a caller that had
 * gone. It is not a bound on the request's total time — the handler's
 * own elapsed time is deliberately not charged, for the reason
 * [RequestAdmission.remainingBudgetMs] gives — so an operation of
 * several calls can still spend several retry budgets. The bulkhead is
 * what bounds the threads it holds while doing so. A write `tx` (one
 * with a txid) is not capped: it runs on the full [retryBudgetMs], as
 * before #679. Its attempt cut short after it was sent is
 * outcome-unknown, and refusing it before sending instead would refuse
 * every write the cap engages for (an uncapped attempt already gets no
 * more than the retry budget, below [commitTimeoutMs] at the
 * defaults), where PostgreSQL still tries the same late commit with a
 * shortened lock wait and never cuts the commit itself. The permit
 * still bounds the threads write calls hold, and [retryBudgetMs] how
 * long each holds one.
 */
class HogliteClient(
    private val standardUrl: String,
    private val whaleUrl: String = "",
    private val ownerUrlPattern: String = "",
    private val connectTimeoutMs: Long = 2_000,
    private val readTimeoutMs: Long = 5_000,
    private val commitTimeoutMs: Long = 10_000,
    private val retryBudgetMs: Long = 8_000,
    private val circuitFailureThreshold: Int = 10,
    private val circuitOpenMs: Long = 5_000,
    /** Test hook: the retry sleep. Production sleeps the thread. */
    private val sleeper: (Long) -> Unit = { Thread.sleep(it) },
    private val shared: HogliteClient? = null,
    private val bearerToken: String? = null,
    /** The bulkhead's width (class KDoc); `0` is no bulkhead, for a client built outside `Config`. */
    private val maxConcurrentCalls: Int = 0,
) : AutoCloseable {
    init {
        require(connectTimeoutMs > 0) { "connectTimeoutMs=$connectTimeoutMs must be positive" }
        require(readTimeoutMs > 0) { "readTimeoutMs=$readTimeoutMs must be positive" }
        require(commitTimeoutMs > 0) { "commitTimeoutMs=$commitTimeoutMs must be positive" }
        require(retryBudgetMs > 0) { "retryBudgetMs=$retryBudgetMs must be positive" }
        require(maxConcurrentCalls >= 0) { "maxConcurrentCalls=$maxConcurrentCalls must not be negative" }
    }

    private val log = KotlinLogging.logger {}

    private val http: HttpClient =
        shared?.http ?: HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(connectTimeoutMs))
            .followRedirects(HttpClient.Redirect.NEVER)
            .build()

    private val json: ObjectMapper = shared?.json ?: jacksonObjectMapper()

    /** Which pool a URL belongs to; breakers and metrics key on it. */
    enum class Pool { STANDARD, WHALE }

    /** A per-tenant routing pin learned from a 421 hint. */
    private sealed class Route {
        /** The tenant is whale-class: its owner lives in the whale pool. */
        data object Whale : Route()

        /** The tenant's owner pod, addressed directly (standard pool only). */
        data class Owner(val url: String) : Route()
    }

    private val tenantRoutes: ConcurrentHashMap<String, Route> = shared?.tenantRoutes ?: ConcurrentHashMap()

    private val breakers: Map<Pool, CircuitBreaker> =
        shared?.breakers ?: mapOf(
            Pool.STANDARD to CircuitBreaker(circuitFailureThreshold, circuitOpenMs),
            Pool.WHALE to CircuitBreaker(circuitFailureThreshold, circuitOpenMs),
        )

    /** A `tx` of this request that applied ([certain]) or may have applied. */
    private class RequestTx(
        val txid: String,
        val certain: Boolean,
    )

    /**
     * The last `tx` of this request that applied, else the last that may
     * have applied, null until one has. Not tracked on the shared client, which serves
     * every request. See "Request authority" in the class KDoc.
     */
    private val appliedTx: AtomicReference<RequestTx?>? = if (shared != null) AtomicReference(null) else null

    /**
     * The bulkhead (class KDoc): one per backend, so every per-request
     * clone shares its root's. Fair, so a waiter that arrived first is
     * served first — the dispatcher's own queue is FIFO and this is its
     * continuation. Null when [maxConcurrentCalls] is 0.
     */
    private val bulkhead: Semaphore? =
        shared?.bulkhead ?: maxConcurrentCalls.takeIf { it > 0 }?.let { Semaphore(it, true) }

    /**
     * The requests holding a permit right now, shared with [bulkhead]:
     * what makes the permit per request (class KDoc). A gated call whose
     * admission is already here runs under the permit that admission
     * holds, instead of waiting for a second one it could never get
     * from itself — the fan-out's workers, and every call of an
     * operation after its first.
     */
    private val holders: MutableSet<RequestAdmission> = shared?.holders ?: ConcurrentHashMap.newKeySet()

    /**
     * Requests waiting for a [bulkhead] permit right now, shared with it:
     * exact, unlike `Semaphore.getQueueLength`, because it is what
     * bounds the waiters at [BULKHEAD_MAX_WAITERS] (class KDoc).
     */
    private val waiting: AtomicInteger = shared?.waiting ?: AtomicInteger()

    /** Permits in use: request threads this backend holds right now. For tests and gauges. */
    internal val callsInFlight: Int get() = bulkhead?.let { maxConcurrentCalls - it.availablePermits() } ?: 0

    /** Requests waiting for a permit right now. For tests. */
    internal val permitWaiters: Int get() = waiting.get()

    /** Bind immutable request authority. Share transport, routing pins, breakers and the bulkhead. */
    internal fun forRequest(token: String?): HogliteClient =
        HogliteClient(
            standardUrl, whaleUrl, ownerUrlPattern, connectTimeoutMs, readTimeoutMs,
            commitTimeoutMs, retryBudgetMs, circuitFailureThreshold, circuitOpenMs,
            sleeper, shared = shared ?: this, bearerToken = token, maxConcurrentCalls = maxConcurrentCalls,
        )

    // ---- public API -------------------------------------------------------

    /**
     * Read one namespace through the typed metadata protocol.
     * The caller supplies verified request authority and a trusted local placement.
     * This operation does not use SQL, routing pins, retries, or circuit breakers.
     * It does hold the request thread, so it runs under [scopedRead].
     */
    internal fun readScopedNamespace(
        tenant: String,
        catalog: String,
        namespace: String,
    ): NamespaceDto? {
        val token = bearerToken
        if (shared == null || token == null) throw HogliteScopedNamespaceRead.unavailable()
        return scopedRead("GET scoped namespace read") { budgetMs ->
            HogliteScopedNamespaceRead.read(http, standardUrl, token, tenant, catalog, namespace, budgetMs)
        }
    }

    /**
     * Run one typed scoped read, [read] given its timeout: under a
     * dispatched request it takes a bulkhead permit and its timeout is
     * [readTimeoutMs] capped at the request's remaining budget, like
     * every [call] (class KDoc, "Bulkhead"). The scoped table and scan
     * handlers call these on the request dispatcher with no
     * `NamespaceGate` lookup before them, so this is the only bound on
     * the threads they hold. The refusal names [what], never the target.
     */
    private inline fun <T> scopedRead(
        what: String,
        read: (Long) -> T,
    ): T {
        val admission = RequestAdmission.current()
        val budgetMs = admission?.remainingBudgetMs()?.let { minOf(readTimeoutMs, it) } ?: readTimeoutMs
        val held = takeRequestPermit(admission, System.nanoTime() + budgetMs * 1_000_000, what)
        try {
            return read(budgetMs)
        } finally {
            if (held) releaseRequestPermit(admission)
        }
    }

    /**
     * Run [block] holding one bulkhead permit for the current request,
     * when there is one: the store's operation-level hold (class KDoc).
     * Every gated call inside runs under it, on this thread or on a
     * worker carrying the request's admission, and none is refused once
     * the operation has started. Refused before [block] runs when no
     * permit frees within [BULKHEAD_WAIT_MS]. Outside a dispatched
     * request, or with no bulkhead, it is [block].
     */
    internal inline fun <T> underRequestPermit(
        what: String,
        block: () -> T,
    ): T {
        val admission = RequestAdmission.current()
        val held = takeRequestPermit(admission, operationDeadline(admission), what)
        try {
            return block()
        } finally {
            if (held) releaseRequestPermit(admission)
        }
    }

    /** How long an operation's permit wait may run: the request's capped budget, as [call] computes it. */
    @PublishedApi
    internal fun operationDeadline(admission: RequestAdmission?): Long {
        val budgetMs = admission?.remainingBudgetMs()?.let { minOf(retryBudgetMs, it) } ?: retryBudgetMs
        return System.nanoTime() + budgetMs * 1_000_000
    }

    /**
     * Read one table through the typed metadata protocol.
     * This operation does not use SQL, routing pins, retries, or circuit breakers.
     * It does hold the request thread, so it runs under [scopedRead].
     */
    internal fun readScopedTable(
        tenant: String,
        catalog: String,
        namespace: String,
        table: String,
    ): ScopedReadOutcome<ScopedTableResult> {
        val token = bearerToken
        if (shared == null || token == null) throw HogliteScopedNamespaceRead.tableUnavailable()
        return scopedRead("GET scoped table read") { budgetMs ->
            HogliteScopedNamespaceRead.readTable(
                http,
                standardUrl,
                token,
                tenant,
                catalog,
                namespace,
                table,
                budgetMs,
            )
        }
    }

    /**
     * Read one pinned scan through the typed metadata protocol.
     * This operation does not use SQL, routing pins, retries, or circuit breakers.
     * It does hold the request thread, so it runs under [scopedRead].
     */
    internal fun readScopedScan(
        tenant: String,
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long,
        tableUuid: UUID,
    ): ScopedReadOutcome<List<ScanFileDto>> {
        val token = bearerToken
        if (shared == null || token == null) throw HogliteScopedNamespaceRead.scanUnavailable()
        return scopedRead("GET scoped scan read") { budgetMs ->
            HogliteScopedNamespaceRead.readScan(
                http,
                standardUrl,
                token,
                tenant,
                catalog,
                namespace,
                table,
                snapshot,
                tableUuid,
                budgetMs,
            )
        }
    }

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
        val materialized = statements.map { it.toWire() }
        val statementCount = materialized.size
        val body =
            json.writeValueAsBytes(
                buildMap {
                    put("client", CLIENT_NAME)
                    put("txid", txid)
                    if (fingerprint != null) put("fp", fingerprint)
                    put("statements", materialized)
                },
            )
        try {
            return applyTx(tenant, txid, body, statementCount).also { appliedTx?.set(RequestTx(txid, certain = true)) }
        } catch (e: HoglakeException.CommitOutcomeUnknown) {
            // Only this tx's own unknown outcome: a 401 naming an earlier tx
            // records nothing new. Never downgrades a tx known to have applied.
            if (e.txid == txid && !e.applied) {
                appliedTx?.updateAndGet { earlier ->
                    earlier?.takeIf { it.certain } ?: RequestTx(txid, certain = false)
                }
            }
            throw e
        }
    }

    private fun applyTx(
        tenant: String,
        txid: String,
        body: ByteArray,
        statementCount: Int,
    ): List<HogliteQueryResult> {
        return call(tenant, "/v1/t/${tenantSegment(tenant)}/tx", body, commitTimeoutMs, txid = txid) { status, bytes ->
            when {
                status == 200 ->
                    try {
                        parseWriteTxResults(bytes, statementCount)
                    } catch (e: Exception) {
                        throw HoglakeException.CommitOutcomeUnknown(
                            txid,
                            "invalid hoglite write response: ${e.message}",
                        )
                    }
                status == 409 -> throw guardFailure(bytes)
                // A query guard refusal (504 query_timeout, 413
                // result_too_large, 507 out_of_memory): the statement
                // rolled back and wrote no dedup receipt. Final.
                errorCode(bytes) in GUARD_REFUSAL_CODES -> throw HogliteRejected(status, String(bytes))
                // Never applied: the tenant does not exist, or the request
                // was malformed before anything ran.
                status in setOf(400, 401, 403, 404) -> throw HogliteRejected(status, String(bytes))
                // 422 is a refusal only when the engine labels it one.
                status == 422 ->
                    if (errorCode(bytes) == CODE_REFUSED) {
                        throw HogliteRejected(status, String(bytes))
                    } else {
                        throw HoglakeException.CommitOutcomeUnknown(txid, "unlabelled 422: ${String(bytes)}")
                    }
                // Anything else (a 500 or 502 from the engine or a proxy in
                // front of it) may have followed an apply.
                else -> throw HoglakeException.CommitOutcomeUnknown(txid, "hoglite answered $status: ${String(bytes)}")
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
        return call(tenant, "/v1/t/${tenantSegment(tenant)}/query", body, readTimeoutMs) { status, bytes ->
            when (status) {
                200 -> parseQueryResult(bytes)
                // /query's error channel is a bare 409 with no guard index:
                // a SQL error, not a conflict a caller can re-prepare for.
                else -> throw HogliteRejected(status, String(bytes))
            }
        }
    }

    /**
     * Several reads of [tenant] as ONE read-only hoglite transaction: one
     * reader slot, one pinned snapshot and one durability park for all of
     * them, where the same reads as [query] calls take one of each apiece.
     * hoglite sends a `/tx` whose statements all start with SELECT down
     * its reader path, with no dedup receipt, so this is a read in every
     * respect but the endpoint:
     *
     *  - every statement must be a plain, unguarded SELECT. Anything else
     *    would fail the engine's read-only classifier and go down the
     *    serial write path with a dedup receipt, so it is refused here;
     *  - no attempt can apply anything, so the call never tracks "may have
     *    applied" and never ends [HoglakeException.CommitOutcomeUnknown]:
     *    retry exhaustion is a plain [HoglakeException.BackendUnavailable],
     *    as for [query];
     *  - a statement error is final, as on `/query`, which reports it as a
     *    409 `refused`; a read-only `/tx` reports it as a 422 `refused`.
     *    This reports the `/query` status, so a caller mapping
     *    [HogliteRejected] treats a failed batch exactly like the failed
     *    single read it replaces. Every other final answer (404, 400, a
     *    query guard refusal) passes through with its status, as [query]
     *    passes it.
     *
     * The txid is a fresh `read:` id (the endpoint requires one; the
     * engine records nothing for a read-only transaction) and stays fixed
     * across the call's retries.
     */
    fun readTx(
        tenant: String,
        statements: List<HogliteStatement>,
    ): List<HogliteQueryResult> {
        require(statements.isNotEmpty()) { "a hoglite read transaction needs at least one statement" }
        for (st in statements) {
            require(st.expectAffected == null && st.expectScalarInt == null) {
                "a hoglite read transaction carries no guards: ${st.sql}"
            }
            require(st.sql.trimStart().startsWith("SELECT ", ignoreCase = true)) {
                "a hoglite read transaction holds plain SELECTs only: ${st.sql}"
            }
        }
        val body =
            json.writeValueAsBytes(
                buildMap {
                    put("client", CLIENT_NAME)
                    put("txid", READ_TXID_PREFIX + UUID.randomUUID())
                    put("statements", statements.map { it.toWire() })
                },
            )
        return call(tenant, "/v1/t/${tenantSegment(tenant)}/tx", body, readTimeoutMs) { status, bytes ->
            when {
                status == 200 ->
                    parseTxResults(bytes).also {
                        check(it.size == statements.size) {
                            "hoglite answered ${it.size} results for a ${statements.size}-statement read"
                        }
                    }
                status == 422 && errorCode(bytes) == CODE_REFUSED -> throw HogliteRejected(409, String(bytes))
                else -> throw HogliteRejected(status, String(bytes))
            }
        }
    }

    /** One item of [queryBatch]: a read-only statement against its own tenant. */
    data class BatchRead(
        val tenant: String,
        val sql: String,
        val args: List<Any?> = emptyList(),
    )

    /**
     * One item's answer from [queryBatch]: [result] when the engine
     * answered 200, otherwise the [status] and [body] that `/query`
     * would have answered for that tenant alone.
     */
    class BatchAnswer(
        val status: Int,
        val body: String,
        val result: HogliteQueryResult?,
    )

    /**
     * Many single-statement reads, each against its own tenant, in ONE
     * request: hoglite's `POST /v1/query-batch`. The engine routes each
     * item as `/query` would (local, one sub-batch per owning peer, or a
     * local acquire), and answers every item in request order with the
     * status and body `/query` would have sent. One item's failure (a
     * 404 for a dropped tenant, a 503 for an owner mid-move) is that
     * item's answer, never the call's.
     *
     * Returns null when the engine does not serve the endpoint (a build
     * that predates it answers the route 404 or 405); the caller then
     * sends the reads one by one. That answer is remembered for
     * [BATCH_UNSUPPORTED_RECHECK_MS], so a cluster mid-upgrade is probed
     * again rather than written off.
     *
     * The request is a read, so it retries like [query] (421, 503, 504,
     * transport) and never ends [HoglakeException.CommitOutcomeUnknown].
     */
    fun queryBatch(reads: List<BatchRead>): List<BatchAnswer>? {
        require(reads.size <= MAX_BATCH_READS) { "at most $MAX_BATCH_READS reads per batch, got ${reads.size}" }
        if (reads.isEmpty()) return emptyList()
        if (System.nanoTime() < batchUnsupportedUntilNanos.get()) return null
        val body =
            json.writeValueAsBytes(
                mapOf("queries" to reads.map { mapOf("tenant" to it.tenant, "sql" to it.sql, "args" to it.args) }),
            )
        return call(BATCH_ROUTE_KEY, "/v1/query-batch", body, readTimeoutMs) { status, bytes ->
            when {
                status == 200 -> {
                    val decoded = json.readValue<Map<String, Any?>>(bytes)

                    @Suppress("UNCHECKED_CAST")
                    val results = decoded["results"] as? List<Map<String, Any?>>
                    check(results != null && results.size == reads.size) {
                        "hoglite answered ${results?.size} results for a ${reads.size}-read batch"
                    }
                    results.map { r ->
                        val itemStatus = (r["status"] as Number).toInt()
                        val itemBody = r["body"]

                        @Suppress("UNCHECKED_CAST")
                        val parsed = if (itemStatus == 200) (itemBody as Map<String, Any?>).toQueryResult() else null
                        BatchAnswer(itemStatus, json.writeValueAsString(itemBody), parsed)
                    }
                }
                // The route itself is unknown: an engine without the endpoint.
                // hoglite's own 4xx bodies are JSON with an `error`; Go's
                // mux answers an unknown route with plain text.
                (status == 404 || status == 405) && errorCode(bytes) == null &&
                    runCatching { json.readValue<Map<String, Any?>>(bytes)["error"] }.getOrNull() == null -> {
                    batchUnsupportedUntilNanos.set(System.nanoTime() + BATCH_UNSUPPORTED_RECHECK_MS * 1_000_000)
                    log.info { "hoglite does not serve /v1/query-batch; reading tenants one by one" }
                    null
                }
                else -> throw HogliteRejected(status, String(bytes))
            }
        }
    }

    private val batchUnsupportedUntilNanos: AtomicLong =
        shared?.batchUnsupportedUntilNanos ?: AtomicLong(Long.MIN_VALUE)

    /**
     * Create [tenant], idempotently: 200 is "created" and a 409/422
     * from a racing or replayed create on an existing tenant is success
     * too — the tenant exists either way, which is all the caller
     * needs. Anything else is transient and retries within the budget.
     */
    fun createTenant(tenant: String) {
        call(tenant, "/v1/tenants/${tenantSegment(tenant)}", null, readTimeoutMs, method = "PUT") { status, bytes ->
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
        call(tenant, "/v1/tenants/${tenantSegment(tenant)}", null, readTimeoutMs, method = "DELETE") { status, bytes ->
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
        call(tenant, "/v1/t/${tenantSegment(tenant)}/release$query", null, readTimeoutMs) { status, bytes ->
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
     *
     * Under a dispatched request the call first fits itself to the
     * request (class KDoc, "Bulkhead"): its budget is the retry budget
     * capped at what the request has left of its admission bound, and
     * it runs under the request's bulkhead permit — the one its
     * operation already holds, or one taken here for its own duration —
     * or is refused before any attempt when none frees in time. A write
     * `tx` takes the permit but not the cap (class KDoc).
     */
    private fun <T> call(
        tenant: String,
        path: String,
        body: ByteArray?,
        timeoutMs: Long,
        method: String = "POST",
        /** Set for a `tx`: its attempts can apply, so the call tracks whether one may have. */
        txid: String? = null,
        parse: (Int, ByteArray) -> T,
    ): T {
        val admission = RequestAdmission.current()
        // A write tx is not capped: its attempt cut short after it was
        // sent is outcome-unknown (class KDoc).
        val budgetMs =
            admission?.remainingBudgetMs()?.takeIf { txid == null }?.let { minOf(retryBudgetMs, it) }
                ?: retryBudgetMs
        val deadline = System.nanoTime() + budgetMs * 1_000_000
        val held = takeRequestPermit(admission, deadline, "$method $path")
        try {
            return attempts(tenant, path, body, timeoutMs, method, txid, budgetMs, deadline, parse)
        } finally {
            if (held) releaseRequestPermit(admission)
        }
    }

    /**
     * Take a bulkhead permit for [admission]'s request, or refuse
     * [what]. True when THIS caller took it and owes
     * [releaseRequestPermit]; false when there is nothing to take (no
     * bulkhead, or no dispatched request) or the request already holds
     * one, in which case the caller runs under that.
     *
     * The wait is the smaller of [BULKHEAD_WAIT_MS] and what is left
     * before [deadline], so a request already short of time is not kept
     * waiting for a permit it could not use, and there is no wait at all
     * when [BULKHEAD_MAX_WAITERS] requests are already waiting: the
     * refusal is immediate and the thread goes back to the dispatcher
     * (class KDoc). Nothing has been sent when
     * this throws, so the refusal is a plain
     * [HoglakeException.BackendUnavailable] for a `tx` too — never
     * outcome-unknown.
     */
    @PublishedApi
    internal fun takeRequestPermit(
        admission: RequestAdmission?,
        deadline: Long,
        what: String,
    ): Boolean {
        val gate = bulkhead
        if (admission == null || gate == null) return false
        // The request is registered BEFORE it has the permit, so a
        // worker that starts on its behalf while it is still waiting
        // sees the hold rather than queueing for a second permit; the
        // registration is undone when the wait fails.
        if (!holders.add(admission)) return false
        val waitMs = minOf(BULKHEAD_WAIT_MS, maxOf(0L, (deadline - System.nanoTime()) / 1_000_000))
        var queueFull = false
        val admitted =
            try {
                // A free permit is taken at once; the timed zero wait,
                // unlike `tryAcquire()`, does not barge past a waiter
                // the fair semaphore owes it to.
                gate.tryAcquire(0, TimeUnit.MILLISECONDS) ||
                    if (waiting.incrementAndGet() > BULKHEAD_MAX_WAITERS) {
                        waiting.decrementAndGet()
                        queueFull = true
                        false
                    } else {
                        try {
                            gate.tryAcquire(waitMs, TimeUnit.MILLISECONDS)
                        } finally {
                            waiting.decrementAndGet()
                        }
                    }
            } catch (e: InterruptedException) {
                holders.remove(admission)
                Thread.currentThread().interrupt()
                throw HoglakeException.BackendUnavailable("interrupted while waiting for a hoglite call permit")
            }
        if (!admitted) {
            holders.remove(admission)
            Metrics.hogliteBulkheadRefused()
            throw HoglakeException.BackendUnavailable(
                "hoglite backend is holding its $maxConcurrentCalls permitted request threads " +
                    "(max_concurrent_calls) and " +
                    (
                        if (queueFull) {
                            "$BULKHEAD_MAX_WAITERS more already waiting for one"
                        } else {
                            "none freed within ${waitMs}ms"
                        }
                    ) +
                    "; refusing $what rather than taking a thread from the other backends — retry",
            )
        }
        return true
    }

    /** Give back the permit [takeRequestPermit] returned true for. */
    @PublishedApi
    internal fun releaseRequestPermit(admission: RequestAdmission?) {
        if (admission != null) holders.remove(admission)
        bulkhead?.release()
    }

    /** The retry loop of [call]; [budgetMs] is what [deadline] was set from, for the exhaustion message. */
    private fun <T> attempts(
        tenant: String,
        path: String,
        body: ByteArray?,
        timeoutMs: Long,
        method: String,
        txid: String?,
        budgetMs: Long,
        deadline: Long,
        parse: (Int, ByteArray) -> T,
    ): T {
        var lastFailure: Throwable? = null
        // True once an attempt of a tx may have applied. From then on the
        // call ends committed or unknown, never "not committed".
        var mayHaveApplied = false

        // What the attempts met, so an exhausted budget says WHICH failure
        // spent it: timeouts (a slow or stalled engine) read differently
        // from 421s (a routing problem) or 503s (owner churn).
        val outcomes = AttemptOutcomes()

        fun unavailable(detail: String): HoglakeException.BackendUnavailable =
            if (txid != null && mayHaveApplied) {
                HoglakeException.CommitOutcomeUnknown(txid, detail)
            } else {
                HoglakeException.BackendUnavailable(detail)
            }

        fun backoff() {
            try {
                sleeper(RETRY_BACKOFF_MS)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                // No new attempt was sent: retain the outcome of the
                // attempts already made, including any earlier ambiguity.
                throw unavailable("interrupted during hoglite retry backoff")
            }
        }

        fun finalAnswer(
            status: Int,
            bytes: ByteArray,
        ): T {
            // A later refusal cannot disprove an earlier ambiguous apply.
            // Only a complete successful answer can settle this exact request.
            val earlier = appliedTx?.get()
            if (txid != null && mayHaveApplied && status != 200) {
                // A tx known to have applied outranks this one's unknown outcome.
                if (status == 401 && earlier?.certain == true) {
                    throw HoglakeException.CommitOutcomeUnknown(
                        earlier.txid,
                        "hoglite refused the request's token (401), as it does once the token expires, " +
                            "after an ambiguous attempt of backend transaction '$txid'",
                        applied = true,
                        tokenLapsed = true,
                    )
                }
                throw HoglakeException.CommitOutcomeUnknown(
                    txid,
                    "hoglite answered $status after an ambiguous attempt",
                    // A 401 after an earlier tx of the request still ends a request that partly ran.
                    tokenLapsed = status == 401 && earlier != null,
                )
            }
            // Nor can a token that lapsed mid-request disprove an earlier tx of it.
            if (earlier != null && status == 401) {
                throw HoglakeException.CommitOutcomeUnknown(
                    earlier.txid,
                    "hoglite refused the request's token (401), as it does once the token expires",
                    applied = earlier.certain,
                    tokenLapsed = true,
                )
            }
            return parse(status, bytes)
        }
        while (true) {
            val pin = tenantRoutes[tenant]
            val (pool, base) = routeFor(pin)
            val breaker = breakers.getValue(pool)
            // Never START an attempt that cannot fit the budget: cap
            // the attempt's timeout at what is left so the worst case is
            // the budget, not budget + one full timeout.
            // A RETRY squeezed into the budget's last few milliseconds
            // can only time out, and a tx attempt that timed out after
            // being sent is outcome-unknown: it would turn a clean "not
            // applied" exhaustion into a spurious unknown.
            // Checked BEFORE the breaker is asked: a half-open breaker's
            // admit spends its one trial, and an attempt that is then
            // not sent reports nothing, so the breaker would stay open
            // another full window without the service being tried.
            val remainingMs = (deadline - System.nanoTime()) / 1_000_000
            if (remainingMs <= 0 || (lastFailure != null && remainingMs < MIN_RETRY_ATTEMPT_MS)) {
                val capped =
                    if (budgetMs < retryBudgetMs) {
                        " (the ${retryBudgetMs}ms budget, capped at what the request had left of its admission bound)"
                    } else {
                        ""
                    }
                throw unavailable(
                    "hoglite ${pool.name.lowercase()} pool did not answer $method $path within the " +
                        "${budgetMs}ms retry budget$capped ${outcomes.summary(ownerUrlPattern.isNotBlank())} " +
                        "(last failure: ${lastFailure?.message})",
                )
            }
            if (!breaker.admit(trial = pin !is Route.Owner)) {
                throw unavailable(
                    "hoglite ${pool.name.lowercase()} pool circuit is open " +
                        "(${breaker.consecutiveFailures} consecutive failures); refusing fast for " +
                        "${breaker.openMs}ms rather than queueing behind timeouts" +
                        (
                            lastFailure?.let {
                                "; this call, ${outcomes.summary(ownerUrlPattern.isNotBlank())} " +
                                    "(last failure: ${it.message})"
                            } ?: ""
                        ),
                )
            }
            val resp =
                try {
                    send(base, path, body, minOf(timeoutMs, remainingMs), method)
                } catch (e: InterruptedException) {
                    // Shutdown, not a backend failure: restore the flag and
                    // stop retrying.
                    Thread.currentThread().interrupt()
                    // the interrupted attempt may have been sent
                    if (txid != null) mayHaveApplied = true
                    throw unavailable(
                        "interrupted while calling hoglite (${pool.name.lowercase()} pool)",
                    )
                } catch (e: Exception) {
                    // Transport failure. A stale OWNER pin (dead pod, moved
                    // tenant) must not pin forever: clear it so the next
                    // attempt re-resolves through the service proxy, and
                    // say nothing to the breaker — one pod's silence is
                    // this tenant's news, not the pool's (#679). Only the
                    // SERVICE's silence feeds the breaker, and only
                    // transport silence at that: a 503 is per-tenant owner
                    // loss, not a pool outage.
                    if (pin is Route.Owner) {
                        tenantRoutes.remove(tenant, pin)
                    } else {
                        breaker.onFailure()
                    }
                    // Only a connect that never completed proves the
                    // request was not sent; a timeout or reset after that
                    // can follow a commit.
                    if (txid != null && !neverSent(e)) mayHaveApplied = true
                    outcomes.transport(e, minOf(timeoutMs, remainingMs))
                    lastFailure = e
                    backoff()
                    continue
                }
            // Any HTTP answer from the SERVICE proves the pool is up: the
            // breaker only tracks transport silence. An owner's answer
            // proves that one pod is, and no more than its silence trips
            // the breaker does its answer close it — a half-open breaker
            // waits for the service's own trial (#679).
            if (pin !is Route.Owner) breaker.onSuccess()
            when (resp.statusCode()) {
                200 -> {
                    return finalAnswer(200, resp.body())
                }
                503 -> {
                    // Owner lost mid-flight / transient infra: retryable.
                    // hoglite labels a 503 it KNOWS did not apply; any
                    // other 503 (an older engine, a proxy) might have.
                    if (txid != null && errorCode(resp.body()) != CODE_NOT_APPLIED) mayHaveApplied = true
                    outcomes.status(503, errorCode(resp.body()))
                    lastFailure = HogliteRejected(503, String(resp.body()))
                    backoff()
                }
                504 -> {
                    // A 504 the engine labels a query guard refusal
                    // (query_timeout) is deterministic and rolled back:
                    // final, not retried. Only the label tells it apart
                    // from outcome_unknown or a gateway timeout.
                    if (errorCode(resp.body()) in GUARD_REFUSAL_CODES) return finalAnswer(504, resp.body())
                    // outcome_unknown: applied on an owner that then failed
                    // before durability was confirmed (or a gateway timeout
                    // in front of the engine). The same txid makes the
                    // retry exactly-once: it finds the commit or applies it.
                    if (txid != null) mayHaveApplied = true
                    outcomes.status(504, errorCode(resp.body()))
                    lastFailure = HogliteRejected(504, String(resp.body()))
                    backoff()
                }
                421 -> {
                    // Misdirected: re-pin from the hint and retry. Class wins
                    // over owner — a whale's owner lives in the whale pool.
                    // The hint can name a class or owner that is itself
                    // about to move (lease wait-out), so this arm backs off
                    // like the others instead of spinning hot.
                    outcomes.misdirected(repin(tenant, resp.body()))
                    lastFailure = HogliteRejected(421, "owner handoff")
                    backoff()
                }
                else -> {
                    // Surface other statuses without another transport attempt.
                    // Operation-specific classification distinguishes request
                    // refusals, unknown read failures, and ambiguous writes.
                    return finalAnswer(resp.statusCode(), resp.body())
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

    /** Apply a 421 hint to the tenant's pin; returns the owner the hint named, if any. */
    private fun repin(
        tenant: String,
        body: ByteArray,
    ): String? {
        val hint =
            runCatching { json.readValue<Map<String, Any?>>(body) }.getOrNull()
        val hintClass = hint?.get("class") as? String
        val rawOwner = hint?.get("owner")
        // Accept a DNS label and an optional per-boot nonce, never URL syntax.
        val owner =
            (rawOwner as? String)?.takeIf {
                it.matches(Regex("[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?(?:\\.[a-zA-Z0-9_-]{1,128})?"))
            }?.substringBefore('.')
        if (rawOwner != null && owner == null) {
            if (hintClass == "whale") tenantRoutes[tenant] = Route.Whale else tenantRoutes.remove(tenant)
            return null
        }
        when {
            hintClass == "whale" -> tenantRoutes[tenant] = Route.Whale
            // Owner-direct routing needs the chart's headless-name pattern;
            // without it the service proxy is the route and no pin is kept.
            !owner.isNullOrBlank() && ownerUrlPattern.isNotBlank() -> {
                // Per-boot ids carry a ".nonce" suffix; the stable pod name
                // is what the headless service resolves.
                tenantRoutes[tenant] = Route.Owner(ownerUrlPattern.replace("{owner}", owner))
            }
            else -> tenantRoutes.remove(tenant)
        }
        return owner
    }

    /**
     * The tally of one call's failed attempts, for the exhaustion message:
     * `after 3 attempts: 2 timed out (attempt timeout up to 5000ms), 1
     * misdirected (421); last 421 named owner 'hl-1'`. The counts are what tell a routing problem (421s,
     * or timeouts at a pod that is not the owner) from a slow engine
     * (timeouts everywhere) from owner churn (503s).
     */
    private class AttemptOutcomes {
        private companion object {
            const val TIMED_OUT = "timed out"
        }

        private val counts = LinkedHashMap<String, Int>()
        private var attempts = 0
        private var owner: String? = null
        private var timeoutMs = 0L

        private fun count(label: String) {
            attempts++
            counts.merge(label, 1, Int::plus)
        }

        fun transport(
            e: Throwable,
            attemptTimeoutMs: Long,
        ) = count(
            when {
                generateSequence(e) { it.cause }.take(8).any { it is HttpConnectTimeoutException } ->
                    "connect timed out"
                generateSequence(e) { it.cause }.take(8).any { it is ConnectException } -> "connect refused"
                generateSequence(e) { it.cause }.take(8).any { it is java.net.http.HttpTimeoutException } -> {
                    timeoutMs = maxOf(timeoutMs, attemptTimeoutMs)
                    TIMED_OUT
                }
                else -> "transport error (${e.javaClass.simpleName})"
            },
        )

        fun status(
            status: Int,
            code: String?,
        ) = count(if (code != null) "$status $code" else "$status unlabelled")

        fun misdirected(hintedOwner: String?) {
            if (hintedOwner != null) owner = hintedOwner
            count("misdirected (421)")
        }

        fun summary(ownerRouting: Boolean): String {
            if (attempts == 0) return "before any attempt"
            val parts =
                counts.entries.joinToString(", ") { (label, n) ->
                    if (label == TIMED_OUT) "$n $label (attempt timeout up to ${timeoutMs}ms)" else "$n $label"
                }
            val hint =
                owner?.let {
                    if (ownerRouting) {
                        "; last 421 named owner '$it'"
                    } else {
                        "; last 421 named owner '$it', but owner-direct routing is not configured " +
                            "(ownerUrlPattern), so its retries went back to the service"
                    }
                } ?: ""
            return "after $attempts attempt${if (attempts == 1) "" else "s"}: $parts$hint"
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
            HttpRequest.newBuilder(URI.create(base.trimEnd('/') + path))
                .timeout(Duration.ofMillis(timeoutMs))
                .header("Content-Type", "application/json")
        bearerToken?.let { builder.header("Authorization", "Bearer $it") }
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

    /** A single UTF-8 path component, not a change to the engine's tenant grammar. */
    private fun tenantSegment(tenant: String): String =
        buildString {
            val hex = "0123456789ABCDEF"
            for (byte in tenant.toByteArray(Charsets.UTF_8)) {
                val value = byte.toInt() and 0xff
                if (value in 65..90 || value in 97..122 || value in 48..57 || value.toChar() in "-._~") {
                    append(value.toChar())
                } else {
                    append('%')
                    append(hex[value ushr 4])
                    append(hex[value and 15])
                }
            }
        }

    /** The engine's outcome label (`code`) on an error answer, or null. */
    private fun errorCode(bytes: ByteArray): String? =
        runCatching { json.readValue<Map<String, Any?>>(bytes)["code"] as? String }.getOrNull()

    /** True when [e] proves the request never left: the connection itself failed. */
    private fun neverSent(e: Throwable): Boolean =
        generateSequence(e) { it.cause }.take(8).any {
            it is ConnectException || it is HttpConnectTimeoutException
        }

    private fun HogliteStatement.toWire(): Map<String, Any?> =
        buildMap {
            put("sql", sql)
            put("args", args)
            if (expectAffected != null) put("expect_affected", expectAffected)
            if (expectScalarInt != null) put("expect_scalar_int", expectScalarInt)
        }

    /** Validate every write result before the store removes its outer fence result. */
    private fun parseWriteTxResults(
        bytes: ByteArray,
        statementCount: Int,
    ): List<HogliteQueryResult> {
        val decoded = json.reader().with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).readTree(bytes)
        val results = decoded?.get("results")
        check(results != null && results.isArray && results.size() == statementCount) {
            "expected $statementCount statement results"
        }
        return results.map { result ->
            check(result.isObject && result.has("cols") && result.has("rows")) { "invalid statement result" }
            val cols = result.get("cols")
            val rows = result.get("rows")
            val affected = result.get("affected")
            check(cols.isNull || (cols.isArray && cols.all { it.isTextual })) { "invalid result columns" }
            check(rows.isNull || rows.isArray) { "invalid result rows" }
            check(affected != null && affected.isIntegralNumber && affected.canConvertToLong()) {
                "invalid affected count"
            }
            val names = if (cols.isNull) emptyList() else cols.map { it.textValue() }
            val values =
                if (rows.isNull) {
                    emptyList()
                } else {
                    rows.map { row ->
                        check(row.isArray && row.size() == names.size) { "invalid result row width" }
                        row.map { json.treeToValue(it, Any::class.java) }
                    }
                }
            HogliteQueryResult(names, values, affected.longValue())
        }
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

        /**
         * True when a call may proceed. A tripped breaker re-admits one
         * trial after [openMs]. An attempt that is not a [trial] (an
         * owner pin, whose failure and answer both report nothing) is
         * let through half-open without spending it: a dead pin would
         * otherwise re-open the breaker for a full window without the
         * service ever being tried (#679).
         */
        @Synchronized
        fun admit(trial: Boolean = true): Boolean {
            if (threshold <= 0 || failures < threshold) return true
            if (openSinceNanos == 0L) openSinceNanos = System.nanoTime()
            if ((System.nanoTime() - openSinceNanos) / 1_000_000 < openMs) return false
            // Half-open: let ONE trial through. A failure re-opens with a
            // fresh clock; a success closes in onSuccess.
            if (trial) openSinceNanos = System.nanoTime()
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
        /**
         * How long past its `exp` hoglited still accepts a token: hoglite's
         * fixed leeway, not configurable here. See "Request authority".
         */
        const val TOKEN_LEEWAY_SECONDS = 60L

        fun fromConfig(backend: HogliteBackend): HogliteClient =
            HogliteClient(
                standardUrl = backend.url,
                whaleUrl = backend.whaleUrl,
                ownerUrlPattern = backend.ownerUrlPattern,
                connectTimeoutMs = backend.connectTimeoutMs,
                readTimeoutMs = backend.readTimeoutMs,
                commitTimeoutMs = backend.commitTimeoutMs,
                retryBudgetMs = backend.retryBudgetMs,
                circuitFailureThreshold = backend.circuitFailureThreshold,
                circuitOpenMs = backend.circuitOpenMs,
                maxConcurrentCalls = backend.maxConcurrentCalls,
            )

        /**
         * How long a request waits for a bulkhead permit before it is
         * refused (class KDoc). The same 200 ms `PartitionLookupAdmission`
         * caps its wait at, for the same reason: a full backend answering
         * in milliseconds is a burst to absorb, one still full after this
         * long is a stall to stay out of.
         */
        const val BULKHEAD_WAIT_MS = 200L

        /**
         * How many requests may wait [BULKHEAD_WAIT_MS] for a full
         * backend's permit at once; the next is refused without waiting
         * (class KDoc). One: the burst one over the cap is served, and a
         * stall costs the other backends one thread more than the cap,
         * not one per arrival.
         */
        const val BULKHEAD_MAX_WAITERS = 1

        /** hoglite's label for a deterministic refusal (422). */
        const val CODE_REFUSED = "refused"

        /**
         * hoglite's labels for a query guard refusal: deterministic for
         * the same request and data, rolled back, no dedup receipt.
         */
        val GUARD_REFUSAL_CODES = setOf("query_timeout", "result_too_large", "out_of_memory")

        /** hoglite's label for a failure it knows did not apply (503). */
        const val CODE_NOT_APPLIED = "not_applied"

        /** The `client` half of hoglite's (client, txid) dedup key. */
        const val CLIENT_NAME = "hoglake"

        /** The txid prefix of a [readTx]: the engine keeps no receipt for it. */
        const val READ_TXID_PREFIX = "read:"

        /** Pause between retryable attempts, including 421 re-routing. */
        const val RETRY_BACKOFF_MS = 200L

        /** The least budget a retry (not a first attempt) is started with. */
        const val MIN_RETRY_ATTEMPT_MS = 50L

        /** The engine's bound on one `/v1/query-batch` request. */
        const val MAX_BATCH_READS = 1000

        /** How long an engine that lacks `/v1/query-batch` is believed before it is asked again. */
        const val BATCH_UNSUPPORTED_RECHECK_MS = 60_000L

        /**
         * The routing key of a batch: no tenant pin applies to a request
         * that names many tenants, so it always goes to the standard pool.
         * Not a valid tenant id, so it can never collide with one.
         */
        private const val BATCH_ROUTE_KEY = "\u0000batch"

        private val STMT_INDEX_REGEX = Regex("expectation failed at statement (\\d+)")
    }
}
