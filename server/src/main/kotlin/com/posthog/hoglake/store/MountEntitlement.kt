package com.posthog.hoglake.store

import com.posthog.hoglake.auth.Caller
import com.posthog.hoglake.auth.Scope

/**
 * The front edge of exchange mode ([MountHost.Auth.EXCHANGE]): which mount
 * a reader's own token entitles it to, and under what name it asks for it.
 * The rule and the name mapping live here and nowhere else.
 *
 * Today a reader addresses its org's events as the catalog
 * `<orgID>-events`, holding only the token for its org catalog `<orgID>`.
 * The proxy trades that token for the mount's at the issuer
 * ([MountTokenExchange]); this object decides, before anything is sent,
 * whether the caller is one the trade is for. The issuer decides again
 * (it re-reads the reader's teams), and the host a third time.
 *
 * If events are later addressed some other way (a namespace of the org
 * catalog, say), this is the file that changes: the exchange, its cache and
 * the proxy take a mount name and a subject token and know nothing of how
 * the reader spelled its request.
 */
object MountEntitlement {
    private const val SUFFIX = "-events"

    /**
     * The one mount [caller] may read through an exchange host, or null:
     * its own org's, `<org>-events`, for a verified scoped caller whose
     * `cat` names its org's catalog (`<org>`) with read. A tenant, an
     * operator and no caller have none, and so has a scoped token that
     * names only some other catalog, the mount itself included.
     */
    fun mountOf(caller: Caller?): String? {
        if (caller?.kind != Caller.Kind.SCOPED) return null
        val org = caller.orgId ?: return null
        if (!caller.catalogClaimAllows(org, Scope.READ)) return null
        return (org + SUFFIX).takeIf(MountNames::isReserved)
    }

    /** May [caller] read [mount] through an exchange host? Only its own org's mount ([mountOf]). */
    fun mayRead(
        caller: Caller?,
        mount: String,
    ): Boolean = mountOf(caller) == mount
}
