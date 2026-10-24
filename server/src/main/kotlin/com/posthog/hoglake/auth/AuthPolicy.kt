package com.posthog.hoglake.auth

import io.ktor.http.HttpMethod

/**
 * What a route demands of its caller before the handler runs.
 *
 * The catalog-scoped members ([READ]..[ADMIN]) are the same vocabulary as
 * [Scope] and are deliberately spelled identically: a rule says "this route needs
 * `write`", and a token says "I hold `write` on catalog X". One word, one meaning,
 * so the route table and the token table can be read against each other.
 */
enum class Requirement {
    /** No token needed. Probes, the spec, and instance branding. */
    PUBLIC,

    /**
     * A valid token, with no catalog named by the route. The response must be
     * FILTERED to what the caller can see — this requirement carries no
     * authorization of its own, and a handler that ignores the caller's
     * `visibleCatalogs()` under it would leak the fleet.
     */
    AUTHENTICATED,

    /** PostHog-operated only: cross-tenant reads, instance health, debug surfaces. */
    OPERATOR,

    READ,
    WRITE,
    MAINTENANCE,
    ADMIN,
    ;

    fun scopeOrNull(): Scope? =
        when (this) {
            READ -> Scope.READ
            WRITE -> Scope.WRITE
            MAINTENANCE -> Scope.MAINTENANCE
            ADMIN -> Scope.ADMIN
            else -> null
        }
}

/** What [AuthPolicy] concluded about a request path. */
data class RouteMatch(
    val requirement: Requirement,
    /**
     * The `{catalog}` path segment's VALUE, when the route has one.
     *
     * The value, not the name: the authorization test is on the catalog the
     * request actually names, so a route whose parameter were ever renamed would
     * still be authorized against the right string rather than silently falling
     * back to a claim lookup for the literal word "catalog".
     */
    val catalog: String?,
    /** The rule's own pattern, for logs and for the totality test's failures. */
    val pattern: String,
)

/**
 * The route table: every path this server serves, and what it demands.
 *
 * ONE TABLE, MATCHED BY PATH, RATHER THAN A DECISION MADE INSIDE EACH ROUTE. Two
 * reasons, and they are the reason this file exists at all:
 *
 * 1. **A hand-written check inside a handler can be forgotten.** The routes here
 *    are installed by a dozen `install*Routes` functions; adding a thirteenth is
 *    a two-line change nobody would think of as a security review. A table that
 *    the request pipeline consults *before any handler runs* cannot be forgotten
 *    by the route author, only by the table author — and the table author has
 *    [AuthPolicyTotalityTest] waiting for them.
 * 2. **Deny-by-default is expressible.** [match] returns null for anything
 *    unlisted, and null is DENIED in `enforce`. So a route added without a table
 *    entry is broken loudly rather than served anonymously, which is the failure
 *    mode we want: a 403 during a deploy, not a hole in production.
 *
 * Matching is by segment count and literal comparison, with `{name}` segments
 * wildcarded. That is deliberately not a regex: a path template compiled to a
 * regex invites an unanchored match (`/catalogs` matching `/catalogs/../x`), and
 * segment counting makes every pattern's shape visible in the table itself.
 */
object AuthPolicy {
    private val CATALOG = "{catalog}"

    /**
     * The table. Ordered longest-pattern-first at construction; see [rules].
     *
     * Reading rules:
     * - Every entry names a REQUIREMENT, never a default. There is no "inherit".
     * - A catalog-scoped route always carries a `{catalog}` segment, and that is
     *   how [RouteMatch.catalog] gets its value — so the prefix
     *   `/v1/catalogs/{catalog}` is load-bearing, not cosmetic.
     * - `GET` is not assumed to be `READ` and the mutating methods are not assumed
     *   to be `WRITE`: `POST …/maintenance/compact` is a GET-shaped idempotent
     *   sweep and still needs `MAINTENANCE`, while `POST …/publications` needs it
     *   too because publishing is a change to catalog state other readers see.
     */
    private val table: List<Entry> =
        listOf(
            // ---- root, outside /v1 --------------------------------------
            // Probes must not need a credential: the kubelet has none, and a
            // liveness probe that 401s restarts every pod.
            Entry(HttpMethod.Get, "/livez", Requirement.PUBLIC),
            Entry(HttpMethod.Get, "/healthz", Requirement.PUBLIC),
            // PUBLIC for the same reason the service is: the spec is the
            // contract customers and their agents read before they have a
            // credential. It does publish every route, including the operator
            // ones — deliberate, and the reason this line exists rather than the
            // omission being an accident nobody noticed.
            Entry(HttpMethod.Get, "/openapi.yaml", Requirement.PUBLIC),
            // Gated because it carries per-catalog labels, i.e. the tenant
            // roster: AUTHENTICATED would hand the fleet's catalog names to any
            // tenant that asks. The listener split (docs/auth-tenancy.md §6.7) is
            // the right home for it.
            //
            // FLIPPING TO `enforce` NEEDS THIS FIXED FIRST. Nothing can mint an
            // operator token yet, so the moment the mode changes the Prometheus
            // scrape gets a 403 and the monitoring that would show the flip going
            // wrong goes dark with it. Either the scrape job holds a credential
            // by then, or /metrics moves to its own unauthenticated listener.
            Entry(HttpMethod.Get, "/metrics", Requirement.OPERATOR),
            // ---- instance -------------------------------------------------
            // PUBLIC because instance identity (name, version, build, theme) is
            // not tenant data and the console shows it before anyone logs in;
            // the TENANT TOTALS in this response are omitted for an anonymous
            // caller, which the handler does rather than the table.
            Entry(HttpMethod.Get, "/v1/info", Requirement.PUBLIC),
            Entry(HttpMethod.Get, "/v1/database/health", Requirement.OPERATOR),
            Entry(HttpMethod.Post, "/v1/debug/decode-bound", Requirement.OPERATOR),
            // ---- fleet-wide listings: authenticated, then FILTERED --------
            Entry(HttpMethod.Get, "/v1/catalogs", Requirement.AUTHENTICATED),
            Entry(HttpMethod.Post, "/v1/catalogs", Requirement.OPERATOR),
            // OPERATOR, not "authenticated then filtered". These two are
            // keyset-paginated cross-catalog feeds (`after` + `limit`), and a
            // post-hoc filter would return short pages whose cursor had skipped
            // rows the caller was never shown — a client paging through it would
            // silently miss entries, which is worse than a clean 403. The
            // tenant-facing twins are the per-catalog
            // `…/catalogs/{c}/maintenance/status` and `…/runs`, which are `READ`
            // and need no filtering at all.
            Entry(HttpMethod.Get, "/v1/maintenance/status", Requirement.OPERATOR),
            Entry(HttpMethod.Get, "/v1/maintenance/runs", Requirement.OPERATOR),
            // ---- one catalog ---------------------------------------------
            Entry(HttpMethod.Get, "/v1/catalogs/$CATALOG", Requirement.READ),
            Entry(HttpMethod.Delete, "/v1/catalogs/$CATALOG", Requirement.ADMIN),
            Entry(HttpMethod.Get, "/v1/catalogs/$CATALOG/storage-usage", Requirement.READ),
            Entry(HttpMethod.Get, "/v1/catalogs/$CATALOG/options", Requirement.READ),
            Entry(HttpMethod.Patch, "/v1/catalogs/$CATALOG/options", Requirement.WRITE),
            Entry(HttpMethod.Get, "/v1/catalogs/$CATALOG/snapshots", Requirement.READ),
            Entry(HttpMethod.Get, "/v1/catalogs/$CATALOG/snapshots/resolve", Requirement.READ),
            Entry(HttpMethod.Get, "/v1/catalogs/$CATALOG/stats/partitions", Requirement.READ),
            Entry(HttpMethod.Get, "/v1/catalogs/$CATALOG/export", Requirement.READ),
            // maintenance: a write that also destroys history, so its own scope
            Entry(HttpMethod.Post, "/v1/catalogs/$CATALOG/maintenance/expire", Requirement.MAINTENANCE),
            Entry(HttpMethod.Post, "/v1/catalogs/$CATALOG/maintenance/compact", Requirement.MAINTENANCE),
            Entry(HttpMethod.Post, "/v1/catalogs/$CATALOG/maintenance/rehydrate", Requirement.MAINTENANCE),
            Entry(HttpMethod.Post, "/v1/catalogs/$CATALOG/maintenance/cleanup", Requirement.MAINTENANCE),
            Entry(HttpMethod.Get, "/v1/catalogs/$CATALOG/maintenance/status", Requirement.READ),
            Entry(HttpMethod.Get, "/v1/catalogs/$CATALOG/maintenance/runs", Requirement.READ),
            // ---- namespaces ----------------------------------------------
            Entry(HttpMethod.Get, "/v1/catalogs/$CATALOG/namespaces", Requirement.READ),
            Entry(HttpMethod.Post, "/v1/catalogs/$CATALOG/namespaces", Requirement.WRITE),
            Entry(HttpMethod.Get, "/v1/catalogs/$CATALOG/namespaces/{namespace}", Requirement.READ),
            Entry(HttpMethod.Delete, "/v1/catalogs/$CATALOG/namespaces/{namespace}", Requirement.WRITE),
            // ---- tables ---------------------------------------------------
            Entry(HttpMethod.Get, "/v1/catalogs/$CATALOG/namespaces/{namespace}/tables", Requirement.READ),
            Entry(HttpMethod.Post, "/v1/catalogs/$CATALOG/namespaces/{namespace}/tables", Requirement.WRITE),
            Entry(
                HttpMethod.Get,
                "/v1/catalogs/$CATALOG/namespaces/{namespace}/tables/{table}",
                Requirement.READ,
            ),
            Entry(
                HttpMethod.Delete,
                "/v1/catalogs/$CATALOG/namespaces/{namespace}/tables/{table}",
                Requirement.WRITE,
            ),
            Entry(
                HttpMethod.Post,
                "/v1/catalogs/$CATALOG/namespaces/{namespace}/tables/{table}/truncate",
                Requirement.WRITE,
            ),
            Entry(
                HttpMethod.Post,
                "/v1/catalogs/$CATALOG/namespaces/{namespace}/tables/{table}/alter",
                Requirement.WRITE,
            ),
            Entry(
                HttpMethod.Get,
                "/v1/catalogs/$CATALOG/namespaces/{namespace}/tables/{table}/scan",
                Requirement.READ,
            ),
            Entry(
                HttpMethod.Get,
                "/v1/catalogs/$CATALOG/namespaces/{namespace}/tables/{table}/files",
                Requirement.READ,
            ),
            Entry(
                HttpMethod.Get,
                "/v1/catalogs/$CATALOG/namespaces/{namespace}/tables/{table}/files/{fileId}/stats",
                Requirement.READ,
            ),
            Entry(
                HttpMethod.Get,
                "/v1/catalogs/$CATALOG/namespaces/{namespace}/tables/{table}/partitions",
                Requirement.READ,
            ),
            Entry(
                HttpMethod.Get,
                "/v1/catalogs/$CATALOG/namespaces/{namespace}/tables/{table}/partitions/values",
                Requirement.READ,
            ),
            Entry(
                HttpMethod.Get,
                "/v1/catalogs/$CATALOG/namespaces/{namespace}/tables/{table}/changes",
                Requirement.READ,
            ),
            // ---- views ----------------------------------------------------
            Entry(HttpMethod.Get, "/v1/catalogs/$CATALOG/namespaces/{namespace}/views", Requirement.READ),
            Entry(HttpMethod.Post, "/v1/catalogs/$CATALOG/namespaces/{namespace}/views", Requirement.WRITE),
            Entry(
                HttpMethod.Get,
                "/v1/catalogs/$CATALOG/namespaces/{namespace}/views/{view}",
                Requirement.READ,
            ),
            Entry(
                HttpMethod.Delete,
                "/v1/catalogs/$CATALOG/namespaces/{namespace}/views/{view}",
                Requirement.WRITE,
            ),
            // ---- table creation -------------------------------------------
            Entry(HttpMethod.Put, "/v1/catalogs/$CATALOG/table-creations/{operation}", Requirement.WRITE),
            Entry(HttpMethod.Get, "/v1/catalogs/$CATALOG/table-creations/{operation}", Requirement.READ),
            Entry(
                HttpMethod.Put,
                "/v1/catalogs/$CATALOG/table-creations/{operation}/partitioned",
                Requirement.WRITE,
            ),
            Entry(HttpMethod.Put, "/v1/catalogs/$CATALOG/table-creations/{operation}/sorted", Requirement.WRITE),
            Entry(HttpMethod.Put, "/v1/catalogs/$CATALOG/table-creations/{operation}/metadata", Requirement.WRITE),
            Entry(HttpMethod.Post, "/v1/catalogs/$CATALOG/table-creations/{operation}/commit", Requirement.WRITE),
            Entry(
                HttpMethod.Post,
                "/v1/catalogs/$CATALOG/table-creations/{operation}/commit/uploads",
                Requirement.WRITE,
            ),
            Entry(HttpMethod.Post, "/v1/catalogs/$CATALOG/table-creations/{operation}/abort", Requirement.WRITE),
            // ---- commits ---------------------------------------------------
            Entry(HttpMethod.Post, "/v1/catalogs/$CATALOG/commit", Requirement.WRITE),
            Entry(HttpMethod.Post, "/v1/catalogs/$CATALOG/commit/prepared", Requirement.WRITE),
            Entry(HttpMethod.Post, "/v1/catalogs/$CATALOG/commit/deletes/prepared", Requirement.WRITE),
            Entry(HttpMethod.Post, "/v1/catalogs/$CATALOG/commit/mutations/prepared", Requirement.WRITE),
            Entry(HttpMethod.Post, "/v1/catalogs/$CATALOG/commit/transaction", Requirement.WRITE),
            Entry(HttpMethod.Post, "/v1/catalogs/$CATALOG/commit/uploads", Requirement.WRITE),
            Entry(HttpMethod.Get, "/v1/catalogs/$CATALOG/commit/receipts/{operation}", Requirement.READ),
            // ---- upload claims ---------------------------------------------
            Entry(HttpMethod.Put, "/v1/catalogs/$CATALOG/uploads/{upload}", Requirement.WRITE),
            Entry(HttpMethod.Post, "/v1/catalogs/$CATALOG/uploads/renew", Requirement.WRITE),
            Entry(HttpMethod.Post, "/v1/catalogs/$CATALOG/uploads/abandon", Requirement.WRITE),
            Entry(HttpMethod.Post, "/v1/catalogs/$CATALOG/uploads/schedule-expired", Requirement.WRITE),
            // ---- publications and consumers --------------------------------
            Entry(HttpMethod.Get, "/v1/catalogs/$CATALOG/publications", Requirement.READ),
            Entry(HttpMethod.Post, "/v1/catalogs/$CATALOG/publications", Requirement.MAINTENANCE),
            Entry(HttpMethod.Get, "/v1/catalogs/$CATALOG/publications/{publication}", Requirement.READ),
            Entry(HttpMethod.Delete, "/v1/catalogs/$CATALOG/publications/{publication}", Requirement.MAINTENANCE),
            Entry(HttpMethod.Get, "/v1/catalogs/$CATALOG/consumers", Requirement.READ),
            Entry(HttpMethod.Get, "/v1/catalogs/$CATALOG/consumers/{consumer}/offsets", Requirement.READ),
            Entry(
                HttpMethod.Get,
                "/v1/catalogs/$CATALOG/consumers/{consumer}/offsets/{tableUuid}",
                Requirement.READ,
            ),
            Entry(
                HttpMethod.Put,
                "/v1/catalogs/$CATALOG/consumers/{consumer}/offsets/{tableUuid}",
                Requirement.WRITE,
            ),
        )

    /**
     * The table, longest pattern first, so `…/tables/{t}` cannot shadow `…/alter`.
     *
     * The second key breaks ties by LITERAL COUNT, which is what makes the order
     * a property of the table rather than of where somebody inserted a line. Two
     * rules of equal length can both match one path only when one has a wildcard
     * where the other has a literal, and then the literal one is the more specific
     * — so it must be tried first. With a plain `sortedByDescending` on length
     * alone, source order decided, and a later `POST …/maintenance/{op}` inserted
     * above `…/maintenance/compact` would have answered compaction requests with
     * whatever the wildcard rule demanded.
     *
     * [AuthPolicyTest] fails the build on any remaining pair that can both match
     * a path, so this is a backstop rather than the only defence.
     */
    val rules: List<Entry> =
        table.sortedWith(
            compareByDescending<Entry> { it.segments.size }
                .thenByDescending { it.literalSegments },
        )

    private val segmentsByPattern: Map<Entry, List<Seg>> =
        table.associateWith { e -> e.pattern.split('/').filter { it.isNotEmpty() }.map(::parseSeg) }

    private sealed interface Seg {
        data class Literal(val value: String) : Seg

        data class Param(val name: String) : Seg
    }

    private fun parseSeg(raw: String): Seg =
        if (raw.startsWith('{') && raw.endsWith('}')) Seg.Param(raw.substring(1, raw.length - 1)) else Seg.Literal(raw)

    /**
     * Resolve [method] + [path] to what it demands.
     *
     * Returns null when nothing matches — an unlisted route. Null is a REFUSAL in
     * `enforce` ([AuthPolicy.authorize] says so) and a counted event in `warn`.
     */
    fun match(
        method: HttpMethod,
        path: String,
    ): RouteMatch? {
        val actual = path.split('/').filter { it.isNotEmpty() }
        for (entry in rules) {
            if (entry.method != method) continue
            val pattern = segmentsByPattern.getValue(entry)
            if (pattern.size != actual.size) continue
            var catalog: String? = null
            var ok = true
            for (i in pattern.indices) {
                when (val seg = pattern[i]) {
                    is Seg.Literal ->
                        if (seg.value != actual[i]) {
                            ok = false
                            break
                        }
                    is Seg.Param -> if (seg.name == "catalog") catalog = actual[i]
                }
            }
            if (ok) return RouteMatch(entry.requirement, catalog, entry.pattern)
        }
        return null
    }

    /** One row of the table. */
    data class Entry(
        val method: HttpMethod,
        val pattern: String,
        val requirement: Requirement,
    ) {
        val segments: List<String> get() = pattern.split('/').filter { it.isNotEmpty() }

        /** How many of [segments] are literals rather than `{param}`s. */
        val literalSegments: Int get() = segments.count { !it.startsWith('{') }
    }

    // ---- authorization ---------------------------------------------------

    /**
     * Decide whether [caller] may run the request [match] describes.
     *
     * [match] is nullable because "no rule" is a real answer: an unlisted path is
     * denied (in `enforce`) rather than defaulted. `caller` is nullable because a
     * [Requirement.PUBLIC] route may have no token at all.
     */
    fun authorize(
        caller: Caller?,
        match: RouteMatch?,
    ): AuthDecision {
        // Deny by default. An unlisted route is a bug in the table; it must never
        // be served as though its absence meant "no requirements".
        if (match == null) return AuthDecision.Unauthenticated("no authorization rule for this path")

        if (match.requirement == Requirement.PUBLIC) return AuthDecision.Allow

        if (caller == null) return AuthDecision.Unauthenticated("a bearer token is required")

        // No PUBLIC arm: it returned above. Restating it here would be a second
        // spelling of the same rule, which is how the two drift apart.
        return when (match.requirement) {
            Requirement.AUTHENTICATED -> AuthDecision.Allow
            Requirement.OPERATOR ->
                if (caller.kind.isOperator()) {
                    AuthDecision.Allow
                } else {
                    AuthDecision.InsufficientScope(match.pattern, Scope.ADMIN)
                }
            else -> {
                val catalog =
                    match.catalog
                        ?: return AuthDecision.InsufficientScope(match.pattern, match.requirement.scopeOrNull()!!)
                // Entitlement first, then scope: an unentitled tenant must get the
                // 404 (see AuthDecision), never a 403 that names the catalog.
                if (!caller.knowsCatalog(catalog)) {
                    return AuthDecision.UnknownCatalog(catalog)
                }
                val required = match.requirement.scopeOrNull() ?: return AuthDecision.Allow
                if (caller.holds(catalog, required)) {
                    AuthDecision.Allow
                } else {
                    AuthDecision.InsufficientScope(catalog, required)
                }
            }
        }
    }
}
