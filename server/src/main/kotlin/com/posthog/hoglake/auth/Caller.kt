package com.posthog.hoglake.auth

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
    /** The org this caller acts for; null for [Kind.OPERATOR]. */
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
    fun knowsCatalog(catalog: String): Boolean = kind.isOperator() || catalogScopes.containsKey(catalog)

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
}
