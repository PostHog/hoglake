package com.posthog.hoglake.auth

import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.observability.Metrics
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

/**
 * A remote schema (docs/auth-tenancy.md §6.5c) is handed out by the same
 * rule: only to a caller whose token allows it. That is a scoped caller
 * whose `cat` allows `read` on [catalog] and whose `remote_schemas` names
 * [schema]. Deny by default: false for every other caller and for none. A
 * token that places the catalog on hoglite is not refused for it, as a
 * mount's reader is: the catalog is the caller's own.
 */
internal fun Caller?.readsRemoteSchema(
    catalog: String,
    schema: String,
): Boolean =
    this != null && kind == Caller.Kind.SCOPED && catalogClaimAllows(catalog, Scope.READ) && schema in remoteSchemas

/** The names among [schemas] a namespace listing of [catalog] shows this caller: those it [readsRemoteSchema]. */
internal fun Caller?.remoteSchemasAmong(
    catalog: String,
    schemas: Collection<String>,
): List<String> = schemas.filter { readsRemoteSchema(catalog, it) }.sorted()

/**
 * `CatalogStoreSelector` asks here before it hands out the binding of the
 * remote schema [schema] to a request that named [namespace]. The catalog
 * comes first ([requireCatalogEntitlement]), so a caller refused for
 * [catalog] gets that answer and the schema is never looked at. Then a
 * caller that does not [readsRemoteSchema] gets the 404 of a missing
 * namespace, and so does a request for a case variant of the name: the
 * variant is no local namespace and is not the remote schema either.
 *
 * Unlike the store's check this refuses in every mode. Nothing is behind it
 * to let a request through to: the catalog's own namespace of that name is
 * never served, and the Host verifies the token itself. The refusal is
 * counted and audited as the store's is, once per request, and against
 * [schema] on the mount series: a configured name, never request input.
 */
internal fun ApplicationCall.requireRemoteSchemaEntitlement(
    catalog: String,
    schema: String,
    namespace: String,
) {
    requireCatalogEntitlement(catalog)
    if (namespace == schema && hoglakeAuth.callerOrNull.readsRemoteSchema(catalog, schema)) return
    refuseMissingNamespace(catalog, schema, namespace, "remote_schema")
}

/**
 * `CatalogStoreSelector` asks here before it hands out the binding of the
 * served namespace [namespace] (docs/auth-tenancy.md §6.5d) of the caller's
 * own [catalog], which this server does not hold. In order, each failure the
 * 404 of a missing namespace, in every auth mode: the token names [catalog]
 * ([requireCatalogEntitlement]); the caller [readsRemoteSchema]; the stored
 * namespace ([containment], read only for a caller that got this far) is
 * org-marked for the caller's org; and the caller's `prd` holds the mark's
 * field and value with `read`.
 */
internal fun ApplicationCall.requireServedNamespaceEntitlement(
    catalog: String,
    namespace: String,
    containment: () -> NamespaceContainment?,
) {
    requireCatalogEntitlement(catalog)
    val caller = hoglakeAuth.callerOrNull
    if (caller.readsRemoteSchema(catalog, namespace)) {
        val mark = containment()
        // `*` is a grant over every value, and is safe here only because the org
        // comparison has already bound this namespace to the caller's own org.
        if (caller != null && mark?.org != null && mark.org == caller.orgId &&
            caller.holdsPredicate(mark.field, mark.value, Scope.READ)
        ) {
            return
        }
    }
    refuseMissingNamespace(catalog, namespace, namespace, "served_namespace")
}

private fun ApplicationCall.refuseMissingNamespace(
    catalog: String,
    counted: String,
    namespace: String,
    at: String,
): Nothing {
    val auth = hoglakeAuth
    val caller = auth.callerOrNull
    // The gate's and the body check's wording for a missing namespace, to the letter.
    val refusal = HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'")
    Metrics.mountRefused(counted, "not_found")
    if (auth !is AuthState.Off && !attributes.contains(RefusalCountedKey)) {
        attributes.put(RefusalCountedKey, Unit)
        val service = application.attributes[AuthServiceKey]
        val match = attributes.getOrNull(RouteMatchKey)
        countRefusal(service, service.clientClass(this), caller, match, refusal, true, catalog, at = at)
    }
    throw refusal
}
