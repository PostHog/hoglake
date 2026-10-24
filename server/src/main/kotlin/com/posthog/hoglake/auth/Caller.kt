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
) {
    enum class Kind {
        /** A tenant. Every catalog it may touch is enumerated in [catalogScopes]. */
        TENANT,

        /** PostHog-operated. Acts across catalogs; every use is audited separately. */
        OPERATOR,
        ;

        fun isOperator(): Boolean = this == OPERATOR
    }

    /** The audit `actor`: stable, attributable, and never client-chosen. */
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
        val held = catalogScopes[catalog] ?: return false
        return held.any { it.satisfies(scope) }
    }

    /** Every catalog name this caller may read — the filter for fleet-wide listings. */
    fun visibleCatalogs(): Set<String>? = if (kind.isOperator()) null else catalogScopes.keys
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
}
