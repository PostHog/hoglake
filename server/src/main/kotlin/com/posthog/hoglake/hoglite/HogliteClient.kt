package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.api.NamespaceDto
import com.posthog.hoglake.model.HoglakeException
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
import java.util.concurrent.atomic.AtomicLong

/** One statement of a hoglite transaction (`/v1/t/{tid}/tx`), or the single statement of a `/query`. */
data class HogliteStatement(
    val sql: String,
    val args: List<Any?> = emptyList(),
    /** RowsAffected must equal this, or the whole transaction rolls back (409, `guard_index`). */
    val expectAffected: Long? = null,
    /** First column of the first row must equal this, or the whole transaction rolls back. */
    val expectScalarInt: Long? = null,
)

/**
 * One statement's result: column names, rows, and the affected-row count,
 * plus [dataPath], the tenant's trusted `data_path` as hoglite served it
 * beside this answer (`X-Hoglite-Data-Path`, or a `/query-batch` item's
 * `data_path`): the operator-set copy in the tenant manifest, which only
 * hoglite writes. Null when the tenant's manifest has none (a tenant
 * created before the field existed). It rides on the result rather than
 * in a per-tenant cache so a row and the prefix it is judged against
 * come from the same answer.
 */
class HogliteQueryResult(
    val cols: List<String>,
    val rows: List<List<Any?>>,
    val affected: Long,
    val dataPath: String? = null,
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
 *
 * ## The trusted data_path
 *
 * [createTenant] sends the catalog's `data_path` in the PUT body, and
 * hoglite stores it in the tenant MANIFEST, which only hoglite writes
 * and only an operator can create; it is immutable after creation (a
 * create naming a different one is a 409 `data_path_mismatch`). Every
 * tenant-scoped answer then repeats it in [DATA_PATH_HEADER], served
 * from the owner's in-memory copy, and every [HogliteQueryResult] this
 * client builds carries it as [HogliteQueryResult.dataPath]. That value,
 * never the tenant's own `meta.data_path` row, is what hoglake's S3-acting
 * paths trust (#668): the row is writable by any tenant writer with raw
 * SQL; the manifest is not. An absent header is a tenant whose manifest
 * has no data_path (created before the field existed), never an empty
 * one.
 *
 * WHAT "KNOWN TO HAVE ONE" MEANS. The legacy reading of an absent header
 * is decided per TENANT, not per answer: this client remembers, per
 * tenant, the data_path the engine last served ([knownDataPaths], shared
 * across the per-request clients like the route pins), and a 200 for a
 * tenant that served one before and lacks the header now is an anomaly
 * (a proxy or engine defect), refused rather than read as "the tenant
 * has none" — [HoglakeException.BackendUnavailable] on a read, which
 * the next answer settles, and [HoglakeException.CommitOutcomeUnknown]
 * on a write `tx`, which applied. Two differing header values on one
 * answer are the same anomaly. The memory is a hint about the engine's
 * CONSISTENCY, never a source of the value: a header carrying a
 * different data_path replaces it (the catalog was dropped and created
 * again under another prefix), and a tenant that answers 404 or that
 * this client deletes is forgotten. A process that has not yet seen a
 * tenant's header still reads the first absent one as legacy: there is
 * no cheaper oracle (`/status` carries the manifest, but only for an
 * operator), and the legacy reading is fail-closed for storage anyway
 * (every S3-acting path refuses it).
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
) : AutoCloseable {
    init {
        require(connectTimeoutMs > 0) { "connectTimeoutMs=$connectTimeoutMs must be positive" }
        require(readTimeoutMs > 0) { "readTimeoutMs=$readTimeoutMs must be positive" }
        require(commitTimeoutMs > 0) { "commitTimeoutMs=$commitTimeoutMs must be positive" }
        require(retryBudgetMs > 0) { "retryBudgetMs=$retryBudgetMs must be positive" }
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

    /**
     * The data_path the engine last served per tenant (class KDoc, "what
     * known means"): one entry per tenant this process has talked to,
     * the same population as [tenantRoutes].
     */
    private val knownDataPaths: ConcurrentHashMap<String, String> =
        shared?.knownDataPaths ?: ConcurrentHashMap()

    private val breakers: Map<Pool, CircuitBreaker> =
        shared?.breakers ?: mapOf(
            Pool.STANDARD to CircuitBreaker(circuitFailureThreshold, circuitOpenMs),
            Pool.WHALE to CircuitBreaker(circuitFailureThreshold, circuitOpenMs),
        )

    /** Bind immutable request authority. Share transport, routing pins, and breakers. */
    internal fun forRequest(token: String?): HogliteClient =
        HogliteClient(
            standardUrl, whaleUrl, ownerUrlPattern, connectTimeoutMs, readTimeoutMs,
            commitTimeoutMs, retryBudgetMs, circuitFailureThreshold, circuitOpenMs,
            sleeper, shared = shared ?: this, bearerToken = token,
        )

    // ---- public API -------------------------------------------------------

    /**
     * Read one namespace through the typed metadata protocol.
     * The caller supplies verified request authority and a trusted local placement.
     * This operation does not use SQL, routing pins, retries, or circuit breakers.
     */
    internal fun readScopedNamespace(
        tenant: String,
        catalog: String,
        namespace: String,
    ): NamespaceDto? {
        val token = bearerToken
        if (shared == null || token == null) throw HogliteScopedNamespaceRead.unavailable()
        return HogliteScopedNamespaceRead.read(http, standardUrl, token, tenant, catalog, namespace, readTimeoutMs)
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
        return call(
            tenant,
            "/v1/t/${tenantSegment(tenant)}/tx",
            body,
            commitTimeoutMs,
            txid = txid,
        ) { status, bytes, dataPath ->
            when {
                status == 200 ->
                    try {
                        parseWriteTxResults(bytes, statementCount, dataPath)
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
        return call(tenant, "/v1/t/${tenantSegment(tenant)}/query", body, readTimeoutMs) { status, bytes, dataPath ->
            when (status) {
                200 -> parseQueryResult(bytes, dataPath)
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
        return call(tenant, "/v1/t/${tenantSegment(tenant)}/tx", body, readTimeoutMs) { status, bytes, dataPath ->
            when {
                status == 200 ->
                    parseTxResults(bytes, dataPath).also {
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
     * would have answered for that tenant alone. The item's `data_path`
     * (what `/query` would have sent in [DATA_PATH_HEADER]) rides on
     * [result].
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
        return call(BATCH_ROUTE_KEY, "/v1/query-batch", body, readTimeoutMs) { status, bytes, _ ->
            when {
                status == 200 -> {
                    val decoded = json.readValue<Map<String, Any?>>(bytes)

                    @Suppress("UNCHECKED_CAST")
                    val results = decoded["results"] as? List<Map<String, Any?>>
                    check(results != null && results.size == reads.size) {
                        "hoglite answered ${results?.size} results for a ${reads.size}-read batch"
                    }
                    results.mapIndexed { i, r ->
                        val itemStatus = (r["status"] as Number).toInt()
                        val itemBody = r["body"]
                        // Per item, never the batch header: a batch may name
                        // many tenants, and the header is set only when every
                        // item agrees. The same per-tenant consistency rule as
                        // a header (class KDoc); an anomaly fails the batch,
                        // as a failed single read would fail its item.
                        val itemDataPath =
                            try {
                                servedDataPath(
                                    reads[i].tenant,
                                    tenantScoped = true,
                                    itemStatus,
                                    (r["data_path"] as? String)?.takeIf { it.isNotEmpty() },
                                )
                            } catch (e: DataPathAnomaly) {
                                throw HoglakeException.BackendUnavailable(e.message ?: "data_path anomaly")
                            }

                        @Suppress("UNCHECKED_CAST")
                        val parsed =
                            if (itemStatus == 200) (itemBody as Map<String, Any?>).toQueryResult(itemDataPath) else null
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
     * What [createTenant] learned: [dataPath] is the one hoglite served
     * back ([DATA_PATH_HEADER]) — the stored value, or null from an engine
     * that predates the field, which the caller must treat as "not
     * recorded". NOT whether this call made the tenant: hoglite's body
     * says `existing: true` only when another pod holds it, and a pod
     * that holds the tenant itself, or takes it over from an expired
     * owner, answers a duplicate create exactly like a fresh one. A caller
     * that needs to know reads the tenant before and after (the store's
     * create does); nothing is inferred from the answer's shape.
     */
    class TenantCreate(
        val dataPath: String?,
    )

    /**
     * Create [tenant] with [dataPath] as its trusted prefix, idempotently:
     * 200 is "the tenant exists now" (made by this call, or found holding
     * this data_path), and a 409/422 from a racing or replayed create on
     * an existing tenant is success too — the tenant exists either way.
     * Returns what hoglite served as a [TenantCreate].
     *
     * Three 4xx answers are final, as [HogliteRejected]: a 409
     * `data_path_mismatch` (the tenant exists and holds a DIFFERENT
     * data_path, or none; the field is set at create or never, so the
     * caller cannot proceed), a 409 `data_path_overlap` (the tenant does
     * not exist, and another tenant of the backend owns a prefix that
     * contains, equals or lies under this one; nothing was created), and
     * a 400 `invalid_data_path` (hoglite's validation is CatalogService's,
     * so this is a bug, not a request to retry). A 503 is hoglite's
     * overlap scan not finishing (the tenant listing or a manifest read
     * failed): retried within the budget like any other 503. Anything
     * else is transient and retries within the budget.
     */
    fun createTenant(
        tenant: String,
        dataPath: String,
    ): TenantCreate {
        // Never `"data_path": null`: hoglite refuses a present non-string
        // (400 invalid_data_path), and only the absent field makes no
        // claim. The parameter is non-null, so the field is always a
        // string here; the body is one JSON object, nothing after it, and
        // no key but this one (hoglite answers 400 to a key it does not
        // know, `class` and `data_path` being the only ones).
        val body = json.writeValueAsBytes(mapOf("data_path" to dataPath))
        return call(
            tenant,
            "/v1/tenants/${tenantSegment(tenant)}",
            body,
            readTimeoutMs,
            method = "PUT",
        ) { status, bytes, served ->
            when {
                status == 200 -> TenantCreate(served)
                status == 409 && errorCode(bytes) in setOf(CODE_DATA_PATH_MISMATCH, CODE_DATA_PATH_OVERLAP) ->
                    throw HogliteRejected(status, String(bytes))
                status == 409 || status == 422 -> TenantCreate(served)
                else -> throw HogliteRejected(status, String(bytes))
            }
        }
    }

    /**
     * The monitor the store's catalog create holds (and its drop of a
     * tenant with no catalog in it), one per backend in
     * this process: shared by every client cloned from a backend's root
     * client ([forRequest]) and so by every per-request store built on
     * them, which is what makes it serialize anything (a store lives for
     * one request). It serializes creates within THIS PROCESS only;
     * across pods hoglite's own overlap check is the guard, with its
     * known race between two concurrent creates.
     */
    internal val createLock: Any = shared?.createLock ?: Any()

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
        call(
            tenant,
            "/v1/tenants/${tenantSegment(tenant)}",
            null,
            readTimeoutMs,
            method = "DELETE",
        ) { status, bytes, _ ->
            when (status) {
                // Gone: whatever is created under this id next is a new
                // tenant, with its own data_path.
                200, 404 -> knownDataPaths.remove(tenant)
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
        call(tenant, "/v1/t/${tenantSegment(tenant)}/release$query", null, readTimeoutMs) { status, bytes, _ ->
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
     * [parse] turns a final HTTP answer — status, body and the tenant's
     * served data_path ([DATA_PATH_HEADER], null when absent) — into the
     * result or the deterministic exception it carries; the loop owns
     * everything retryable.
     */
    private fun <T> call(
        tenant: String,
        path: String,
        body: ByteArray?,
        timeoutMs: Long,
        method: String = "POST",
        /** Set for a `tx`: its attempts can apply, so the call tracks whether one may have. */
        txid: String? = null,
        parse: (Int, ByteArray, String?) -> T,
    ): T {
        val deadline = System.nanoTime() + retryBudgetMs * 1_000_000
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

        fun finalAnswer(resp: HttpResponse<ByteArray>): T {
            val status = resp.statusCode()
            // A later refusal cannot disprove an earlier ambiguous apply.
            // Only a complete successful answer can settle this exact request.
            if (txid != null && mayHaveApplied && status != 200) {
                throw HoglakeException.CommitOutcomeUnknown(txid, "hoglite answered $status after an ambiguous attempt")
            }
            val served =
                try {
                    servedDataPath(tenant, path.startsWith(TENANT_ROUTE_PREFIX), status, dataPathOf(resp))
                } catch (e: DataPathAnomaly) {
                    // A 200 on a write tx APPLIED: the answer is the only
                    // thing in doubt, so the caller learns "unknown", never
                    // a 500 over a durable write. A read's next answer
                    // settles it; so does a write's retry with its txid.
                    if (txid != null && status == 200) {
                        throw HoglakeException.CommitOutcomeUnknown(txid, e.message ?: "data_path anomaly")
                    }
                    throw HoglakeException.BackendUnavailable(e.message ?: "data_path anomaly")
                }
            return parse(status, resp.body(), served)
        }
        while (true) {
            val pin = tenantRoutes[tenant]
            val (pool, base) = routeFor(pin)
            val breaker = breakers.getValue(pool)
            if (!breaker.admit()) {
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
            // Never START an attempt that cannot fit the budget: cap
            // the attempt's timeout at what is left so the worst case is
            // the budget, not budget + one full timeout.
            // A RETRY squeezed into the budget's last few milliseconds
            // can only time out, and a tx attempt that timed out after
            // being sent is outcome-unknown: it would turn a clean "not
            // applied" exhaustion into a spurious unknown.
            val remainingMs = (deadline - System.nanoTime()) / 1_000_000
            if (remainingMs <= 0 || (lastFailure != null && remainingMs < MIN_RETRY_ATTEMPT_MS)) {
                throw unavailable(
                    "hoglite ${pool.name.lowercase()} pool did not answer $method $path within the " +
                        "${retryBudgetMs}ms retry budget ${outcomes.summary(ownerUrlPattern.isNotBlank())} " +
                        "(last failure: ${lastFailure?.message})",
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
                    // attempt re-resolves through the service proxy. Only
                    // THIS outcome feeds the breaker — a 503 is per-tenant
                    // owner loss, not a pool outage.
                    if (pin is Route.Owner) tenantRoutes.remove(tenant, pin)
                    breaker.onFailure()
                    // Only a connect that never completed proves the
                    // request was not sent; a timeout or reset after that
                    // can follow a commit.
                    if (txid != null && !neverSent(e)) mayHaveApplied = true
                    outcomes.transport(e, minOf(timeoutMs, remainingMs))
                    lastFailure = e
                    backoff()
                    continue
                }
            // Any HTTP answer at all proves the pool is up: the breaker
            // only tracks transport silence.
            breaker.onSuccess()
            when (resp.statusCode()) {
                200 -> {
                    return finalAnswer(resp)
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
                    if (errorCode(resp.body()) in GUARD_REFUSAL_CODES) return finalAnswer(resp)
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
                    return finalAnswer(resp)
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

    /**
     * A served data_path this client cannot take at its word (class KDoc,
     * "what known means"): two differing header values on one answer, or
     * none on a 200 for a tenant that served one before. Internal: the
     * call loop maps it to the caller's outcome vocabulary.
     */
    private class DataPathAnomaly(message: String) : RuntimeException(message)

    /**
     * The tenant's served data_path on [resp] ([DATA_PATH_HEADER]), or
     * null when absent. hoglite sends it exactly once and never empty;
     * two differing values are a proxy or engine defect, a
     * [DataPathAnomaly], and a prefix hoglake cannot trust is refused
     * rather than guessed.
     */
    private fun dataPathOf(resp: HttpResponse<ByteArray>): String? {
        val values = resp.headers().allValues(DATA_PATH_HEADER).distinct()
        if (values.size > 1) {
            throw DataPathAnomaly("hoglite answered with ${values.size} differing $DATA_PATH_HEADER values")
        }
        return values.singleOrNull()?.takeIf { it.isNotEmpty() }
    }

    /**
     * [served], the data_path an answer of [status] for [tenant] carried
     * (null when none), checked against and recorded in [knownDataPaths].
     * A value is remembered from any route; a 404 forgets the tenant,
     * whose id may be created again. Only a 200 on a TENANT-SCOPED route
     * ([tenantScoped]: `/v1/t/{tid}/...`, or a batch item) is judged for
     * absence: a refusal may legitimately carry no header (a 421, an
     * authority refusal), and so may the answer to the tenant's own
     * deletion (`DELETE /v1/tenants/{tid}`: nothing is left to serve it
     * from). A value replaces a remembered one (a drop and a create
     * under another prefix); an absence after a value is a
     * [DataPathAnomaly]. The batch route key is no tenant and is not
     * judged.
     */
    private fun servedDataPath(
        tenant: String,
        tenantScoped: Boolean,
        status: Int,
        served: String?,
    ): String? {
        if (tenant == BATCH_ROUTE_KEY) return served
        if (status == 404) {
            knownDataPaths.remove(tenant)
            return served
        }
        if (served != null) {
            knownDataPaths[tenant] = served
            return served
        }
        if (status != 200 || !tenantScoped) return null
        val known = knownDataPaths[tenant]
        if (known != null) {
            throw DataPathAnomaly(
                "hoglite answered for tenant '$tenant' without $DATA_PATH_HEADER, which it served as '$known' " +
                    "before; refusing to read the tenant as one without a data_path",
            )
        }
        return null
    }

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
        dataPath: String?,
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
            HogliteQueryResult(names, values, affected.longValue(), dataPath)
        }
    }

    private fun parseTxResults(
        bytes: ByteArray,
        dataPath: String?,
    ): List<HogliteQueryResult> {
        val decoded = json.readValue<Map<String, Any?>>(bytes)

        @Suppress("UNCHECKED_CAST")
        val results = decoded["results"] as? List<Map<String, Any?>> ?: emptyList()
        return results.map { it.toQueryResult(dataPath) }
    }

    private fun parseQueryResult(
        bytes: ByteArray,
        dataPath: String?,
    ): HogliteQueryResult = json.readValue<Map<String, Any?>>(bytes).toQueryResult(dataPath)

    private fun Map<String, Any?>.toQueryResult(dataPath: String?): HogliteQueryResult {
        @Suppress("UNCHECKED_CAST")
        return HogliteQueryResult(
            cols = (this["cols"] as? List<String>) ?: emptyList(),
            rows = (this["rows"] as? List<List<Any?>>) ?: emptyList(),
            affected = this["affected"].asLongOrNull() ?: 0,
            dataPath = dataPath,
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
            )

        /** hoglite's label for a deterministic refusal (422). */
        const val CODE_REFUSED = "refused"

        /**
         * hoglite's labels for a query guard refusal: deterministic for
         * the same request and data, rolled back, no dedup receipt.
         */
        val GUARD_REFUSAL_CODES = setOf("query_timeout", "result_too_large", "out_of_memory")

        /** hoglite's label for a failure it knows did not apply (503). */
        const val CODE_NOT_APPLIED = "not_applied"

        /**
         * The header every tenant-scoped hoglite answer carries when the
         * tenant's manifest holds a data_path: the trusted copy. Absent
         * for a tenant created before the field existed; never empty.
         */
        const val DATA_PATH_HEADER = "X-Hoglite-Data-Path"

        /**
         * hoglite's label on the 409 for a create that names a data_path
         * other than the one the existing tenant holds (or any, when it
         * holds none): final, the field is set at create or never.
         */
        const val CODE_DATA_PATH_MISMATCH = "data_path_mismatch"

        /**
         * hoglite's label on the 409 for a create whose data_path equals,
         * contains or lies under another tenant's on the same backend
         * (hoglite#666, option A): final, nothing was created. The same
         * rule CatalogService applies between PostgreSQL catalogs, so the
         * store maps it to the same 422.
         */
        const val CODE_DATA_PATH_OVERLAP = "data_path_overlap"

        /** hoglite's label on the 400 for a malformed data_path in a create. */
        const val CODE_INVALID_DATA_PATH = "invalid_data_path"

        /** The `client` half of hoglite's (client, txid) dedup key. */
        const val CLIENT_NAME = "hoglake"

        /** The routes a tenant answers for itself (`/v1/t/{tid}/...`), as opposed to its management (`/v1/tenants/{tid}`). */
        private const val TENANT_ROUTE_PREFIX = "/v1/t/"

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
