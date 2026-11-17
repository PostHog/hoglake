package com.posthog.hoglake.auth

import com.posthog.hoglake.model.HoglakeException
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
 * [schema], by the schema's name on its host (a local alias never reaches
 * the claim). Deny by default: false for every other caller and for none. A
 * token that places the catalog on hoglite is not refused for it, as a
 * mount's reader is: the catalog is the caller's own.
 */
internal fun Caller?.readsRemoteSchema(
    catalog: String,
    schema: String,
): Boolean =
    this != null && kind == Caller.Kind.SCOPED && catalogClaimAllows(catalog, Scope.READ) && schema in remoteSchemas

/**
 * The local names a namespace listing of [catalog] shows this caller, among
 * [schemas] (each local name to its name on the host): those whose host name
 * it [readsRemoteSchema].
 */
internal fun Caller?.remoteSchemasAmong(
    catalog: String,
    schemas: Map<String, String>,
): List<String> = schemas.filterValues { readsRemoteSchema(catalog, it) }.keys.sorted()

/**
 * `CatalogStoreSelector` asks here before it hands out the binding of the
 * remote schema [schema] (its local name; [remoteName] on its host) to a
 * request that named [namespace]. The catalog
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
    remoteName: String,
) {
    requireCatalogEntitlement(catalog)
    val auth = hoglakeAuth
    val caller = auth.callerOrNull
    if (namespace == schema && caller.readsRemoteSchema(catalog, remoteName)) return
    // The gate's and the body check's wording for a missing namespace, to the letter.
    val refusal = HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'")
    Metrics.mountRefused(schema, "not_found")
    if (auth !is AuthState.Off && !attributes.contains(RefusalCountedKey)) {
        attributes.put(RefusalCountedKey, Unit)
        val service = application.attributes[AuthServiceKey]
        val match = attributes.getOrNull(RouteMatchKey)
        countRefusal(service, service.clientClass(this), caller, match, refusal, true, catalog, at = "remote_schema")
    }
    throw refusal
}

/**
 * The caller rule of a remote schema this server serves as its Host
 * (docs/auth-tenancy.md §6.5d): may this caller read the served schema
 * [schema] through the path catalog [catalog]? All four must hold:
 *
 * - the caller is scoped;
 * - its `remote_schemas` names [schema] (the verifier admits only `read`);
 * - its `cat` allows `read` on [catalog];
 * - its `org` is [catalog]: a Client's catalog is named by its org's id, so
 *   a token reads only through its own org's catalog, and the org's record
 *   is the one looked up.
 *
 * Deny by default: false for no caller, a tenant and an operator. It reads
 * nothing but the token. The token's `rtg` is not consulted: it places the
 * caller's own catalog on the Client, and a served schema is never read from
 * a backend it could name.
 */
internal fun Caller?.readsServedSchema(
    catalog: String,
    schema: String,
): Boolean =
    this != null && kind == Caller.Kind.SCOPED && schema in remoteSchemas &&
        catalogClaimAllows(catalog, Scope.READ) && orgId == catalog

/**
 * The one answer to a caller [readsServedSchema] refuses, whichever
 * condition failed: the 404 of a missing namespace, in the namespace gate's
 * words. It depends on the path alone. Counted against [schema] on the mount
 * series (a configured name, never request input), so build it only to
 * answer with it.
 */
internal fun servedSchemaRefusal(
    catalog: String,
    schema: String,
): HoglakeException {
    Metrics.mountRefused(schema, "not_found")
    return HoglakeException.NotFound("namespace '$schema' in catalog '$catalog'")
}

/**
 * `CatalogStoreSelector` asks here before it hands out the binding of the
 * served schema [schema] for the path catalog [catalog], and before it reads
 * anything: the org's record, a catalog row, a store. The plugin has asked
 * the same rule for a verified caller; this is the check at the point of
 * use, and the only one under `off` and for a request `warn` let through
 * without a caller. It refuses in every mode: nothing is behind it to let a
 * request through to. Counted and audited once per request.
 */
internal fun ApplicationCall.requireServedSchemaReader(
    catalog: String,
    schema: String,
) {
    val auth = hoglakeAuth
    val caller = auth.callerOrNull
    if (caller.readsServedSchema(catalog, schema)) return
    val refusal = servedSchemaRefusal(catalog, schema)
    if (auth !is AuthState.Off && !attributes.contains(RefusalCountedKey)) {
        attributes.put(RefusalCountedKey, Unit)
        val service = application.attributes[AuthServiceKey]
        val match = attributes.getOrNull(RouteMatchKey)
        countRefusal(service, service.clientClass(this), caller, match, refusal, true, catalog, at = "served_schema")
    }
    throw refusal
}
