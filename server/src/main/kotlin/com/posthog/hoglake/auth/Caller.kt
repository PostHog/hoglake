package com.posthog.hoglake.auth

import com.posthog.hoglake.store.BackendKind
import java.util.UUID

/**
 * The authenticated caller for one request, resolved from a verified token.
 *
 * Threaded through the call attributes by the auth plugin and read back by
 * `ApplicationCall.hoglakeCaller`. It is the ONLY authority the request pipeline
 * consults for "may this request touch this catalog", which is what lets the
 * rule live in one place: if a route handler wanted to make a second
 * authorization decision, it would have to justify it against this type.
 *
 * [catalogScopes] is empty for an operator, whose authority is not enumerated;
 * [kind] is what distinguishes them. Keeping [catalogScopes] empty rather than
 * listing every catalog means an operator token does not go stale when a
 * catalog is created — and it is safe because an operator is a different KIND,
 * never a tenant with a big map.
 */
data class Caller(
    /** `sub`: the client id (`client:…`) or service-account subject (`sa:…`). */
    val subject: String,
    /** The org this caller acts for. Legacy operator tokens have none. */
    val orgId: String?,
    val kind: Kind,
    /** Catalog name -> the scopes held on it. Empty for [Kind.OPERATOR]. */
    val catalogScopes: Map<String, Set<Scope>>,
    /** `jti`, for correlating an audit line with a token and an STS session. */
    val tokenId: String,
    /** `cli`: the human-facing client name, for the audit log. */
    val clientName: String?,
    /**
     * `dlg`: this client acts for end users and may name the one it acts for
     * as `author` (a query engine running a user's statement). The issuer sets
     * it only on service credentials it provisions itself, never on a
     * customer's own; without it a tenant may name nobody but itself.
     */
    val delegates: Boolean = false,
    /**
     * `prd`, on a [Kind.SCOPED] caller only: predicate field -> the values it
     * may see, each with its own permission. Empty for every other kind.
     */
    val predicates: Map<String, PredicateGrant> = emptyMap(),
    /** `usr`: an opaque end-user name for the audit log. Grants nothing. */
    val user: String? = null,
    /** Verified placement for one catalog. Endpoints come only from server configuration. */
    val routing: Routing? = null,
    /**
     * The one mount this server serves the caller through a token exchange
     * (`store/MountEntitlement.kt`), or null. It is NOT a claim: the server
     * derives it from the token's own `org` and `cat` when a mount host of
     * auth mode `exchange` serves that name, and attaches it after the
     * token verified. It opens the mount's routes in [AuthPolicy] at
     * `read` and nothing else; binding dispatch decides again from the
     * claims, and the issuer and the host after it.
     */
    val exchangeMount: String? = null,
) {
    enum class Kind {
        /** A tenant. Every catalog it may touch is enumerated in [catalogScopes]. */
        TENANT,

        /**
         * A tenant narrowed by [predicates]: it may touch only what is
         * attributed to a value it holds, in a catalog in [catalogScopes].
         * Naming a catalog is not enough on its own, so [holds] is false for
         * it: every route a scoped caller may use decides per object.
         */
        SCOPED,

        /** PostHog-operated. Acts across catalogs; every use is audited separately. */
        OPERATOR,
        ;

        fun isOperator(): Boolean = this == OPERATOR
    }

    /**
     * The audit `actor`: stable, attributable, and never client-chosen. A
     * delegated end user is recorded beside it, never instead of it — see
     * `resolveActor`.
     */
    val actor: String
        get() = if (kind.isOperator()) "operator:$subject" else subject

    /**
     * Is this caller entitled to see [catalog] at all?
     *
     * Every request that names a catalog asks this first, and a `false` is
     * reported as **404, not 403** — see [AuthDecision]. A 403 would confirm the
     * catalog exists, which turns an org UUID the caller was never given into an
     * enumeration oracle.
     */
    fun knowsCatalog(catalog: String): Boolean =
        kind.isOperator() || catalogScopes.containsKey(catalog) || catalog == exchangeMount

    /** Holder of [scope] on [catalog], by [Scope.satisfies]. */
    fun holds(
        catalog: String,
        scope: Scope,
    ): Boolean {
        if (kind.isOperator()) return true
        // Fail closed: a scoped caller's authority is per object, never per
        // catalog. A route that asked this would otherwise hand it the catalog.
        if (kind == Kind.SCOPED) return false
        val held = catalogScopes[catalog] ?: return false
        return held.any { it.satisfies(scope) }
    }

    /**
     * May this SCOPED caller use, with [scope], a namespace in [catalog]
     * contained for `field = value`? Its permission on the value is the lower
     * of the predicate's and the catalog's (`cat`), so both must satisfy
     * [scope]. False for every other kind: a tenant's authority is [holds],
     * and nothing here may widen it.
     */
    fun mayUseContained(
        catalog: String,
        field: String,
        value: Long,
        scope: Scope,
    ): Boolean {
        if (kind != Kind.SCOPED) return false
        val held = catalogScopes[catalog] ?: return false
        if (held.none { it.satisfies(scope) }) return false
        val perm =
            when (val grant = predicates[field] ?: return false) {
                is PredicateGrant.All -> grant.scope
                is PredicateGrant.Values -> grant.values[value] ?: return false
            }
        return perm.satisfies(scope)
    }

    /** Does this caller hold [scope] on [catalog] by its `cat` claim alone? Not an authorization. */
    internal fun catalogClaimAllows(
        catalog: String,
        scope: Scope,
    ): Boolean = catalogScopes[catalog]?.any { it.satisfies(scope) } == true

    /**
     * [catalogClaimAllows], or [catalog] is this caller's [exchangeMount]
     * and [scope] is `read`. For the route table only ([AuthPolicy]): it
     * lets the caller reach binding dispatch, which serves the mount or
     * refuses.
     */
    internal fun routeAllows(
        catalog: String,
        scope: Scope,
    ): Boolean = catalogClaimAllows(catalog, scope) || (catalog == exchangeMount && Scope.READ.satisfies(scope))

    /** Every catalog name this caller may read — the filter for fleet-wide listings. */
    fun visibleCatalogs(): Set<String>? = if (kind.isOperator()) null else catalogScopes.keys
}

/**
 * What a scoped token grants on one predicate field.
 *
 * Either every value ([All], `{"*": perm}`) or an enumerated set ([Values]).
 * Permissions are [Scope.READ] or [Scope.WRITE] only; the effective permission
 * on a value is the lower of this and the token's `cat` scope for the catalog.
 */
sealed interface PredicateGrant {
    data class All(
        val scope: Scope,
    ) : PredicateGrant

    data class Values(
        val values: Map<Long, Scope>,
    ) : PredicateGrant
}

/**
 * The outcome of authorizing one request.
 *
 * A sealed type rather than a `Boolean` because the three failures need three
 * different wire answers, and the distinction between the last two is a security
 * property, not a nicety:
 *
 * - [Unauthenticated] -> 401 (no token, or a token that does not verify)
 * - [UnknownCatalog] -> **404** (a tenant asking about a catalog it does not
 *   own; it must be indistinguishable from a catalog that does not exist)
 * - [InsufficientScope] -> 403 (a catalog the caller DOES own, and the request
 *   wanted a scope it does not hold; here the 403 tells the truth, because the
 *   caller already knew the catalog was its own)
 */
sealed interface AuthDecision {
    data object Allow : AuthDecision

    data class Unauthenticated(
        val reason: String,
    ) : AuthDecision

    data class UnknownCatalog(
        val catalog: String,
    ) : AuthDecision

    data class InsufficientScope(
        val catalog: String,
        val required: Scope,
    ) : AuthDecision

    /**
     * A scoped caller on a route that cannot yet narrow by predicate -> 403.
     * Refused in every mode, `warn` included: a verified scoped token is never
     * let through to an unfiltered route.
     */
    data class NotPredicated(
        val catalog: String,
        val pattern: String,
    ) : AuthDecision

    /**
     * A scoped caller on a route that narrows by namespace: allowed only if
     * [namespace] is live, contained, and its value is one the caller holds
     * with [required]. That needs the database, so the plugin defers it to
     * the namespace gate (NamespaceGate.kt), which runs on the request
     * dispatcher before routing and answers 404, exactly as for a missing
     * namespace, when it does not hold.
     */
    data class ByNamespace(
        val catalog: String,
        val namespace: String,
        val required: Scope,
    ) : AuthDecision

    /**
     * A scoped caller on a route whose namespaces are in its BODY (a commit,
     * a table creation, an upload claim). The plugin cannot read the body,
     * so it lets the call through to the handler, which checks every
     * namespace the request names before it acts
     * ([requireScopedNamespaces], ScopedWrites.kt) and answers 404 as the
     * gate does. The handler's check keys on the caller's kind, not on this
     * decision being recorded, so a scoped caller reaching such a handler
     * any other way is refused there too.
     */
    data class ByBody(
        val catalog: String,
        val required: Scope,
    ) : AuthDecision
}

/** A placement verified against the server's backend allowlist. */
data class Routing(
    val catalog: String,
    val backend: BackendKind,
    val backendId: UUID,
    val tenant: String?,
)
