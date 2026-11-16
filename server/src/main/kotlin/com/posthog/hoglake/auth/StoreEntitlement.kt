package com.posthog.hoglake.auth

import com.posthog.hoglake.model.HoglakeException
import io.ktor.server.application.ApplicationCall

/**
 * A store is handed out only for a catalog the caller's token allows.
 *
 * The auth plugin decides on the catalog the request path names
 * ([AuthPolicy.authorize]). `CatalogStoreSelector` asks here again for the
 * name the handler gives it, so a name resolves to a store, a mount or a
 * remote binding only when the token allows that name for what the route
 * asks. The rules and the answers are the plugin's ([storeRefusal]).
 *
 * Deny by default: only a request [storeRefusal] finds nothing against gets
 * a store. The modes follow the plugin. Under `off` (or with no plugin) no
 * caller is resolved and nothing is checked. Under `warn` a refusal is
 * counted, audited and let through, except for a scoped caller, which is
 * refused as in `enforce`.
 *
 * A request adds at most one refusal to the counts and the audit, however
 * many names its handler asks for, and none when the plugin already counted
 * one and let the request through ([RefusalCountedKey]). The audit line
 * names the catalog that was refused, not the path's.
 */
internal fun ApplicationCall.requireCatalogEntitlement(catalog: String) {
    val auth = hoglakeAuth
    if (auth is AuthState.Off) return
    val caller = auth.callerOrNull
    val match = attributes.getOrNull(RouteMatchKey)
    val refusal = storeRefusal(caller, catalog, match) ?: return

    val service = application.attributes[AuthServiceKey]
    val enforced = service.mode.isEnforcing || caller?.kind == Caller.Kind.SCOPED
    if (!attributes.contains(RefusalCountedKey)) {
        attributes.put(RefusalCountedKey, Unit)
        val client = service.clientClass(this)
        countRefusal(service, client, caller, match, refusal, enforced, catalog = catalog, at = "store")
    }
    if (enforced) throw refusal
}

/**
 * What stands against handing [caller] the store of [catalog] on the route
 * [match], as the plugin would answer it; null when nothing does.
 *
 * - No caller: 401.
 * - A catalog the token does not name ([Caller.knowsCatalog]: `cat` for a
 *   tenant and a scoped caller, any catalog for an operator): 404 worded as
 *   a missing catalog.
 * - A catalog the token names without the scope the route asks for (`read`,
 *   `write`, `maintenance` or `admin` in the route table): 403. An operator
 *   holds every scope. A route that asks for no catalog scope, and a path
 *   the table does not list, are held to the name alone.
 */
private fun storeRefusal(
    caller: Caller?,
    catalog: String,
    match: RouteMatch?,
): HoglakeException? {
    if (caller == null) return HoglakeException.Unauthenticated(JwtViolation.NO_TOKEN, "a bearer token is required")
    if (!caller.knowsCatalog(catalog)) return unknownCatalog(catalog)
    val required = match?.requirement?.scopeOrNull() ?: return null
    if (caller.kind.isOperator() || caller.catalogClaimAllows(catalog, required)) return null
    return insufficientScope(catalog, required, match)
}
