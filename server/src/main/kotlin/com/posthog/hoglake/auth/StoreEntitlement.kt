package com.posthog.hoglake.auth

import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.service.TeamNamespaces
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.ServedFamily
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
 * Refuse [refusal], which the store decided against the token itself rather
 * than a catalog name: CatalogStoreSelector's lifetime floor for a
 * long-running hoglite route. Counted and audited as
 * [requireCatalogEntitlement] does, once per request and with [at] naming
 * where; `warn` lets it through, except for a scoped caller.
 */
internal fun ApplicationCall.refuseTokenAtStore(
    refusal: HoglakeException,
    catalog: String,
    at: String,
) {
    val auth = hoglakeAuth
    if (auth is AuthState.Off) return
    val caller = auth.callerOrNull
    val service = application.attributes[AuthServiceKey]
    val enforced = service.mode.isEnforcing || caller?.kind == Caller.Kind.SCOPED
    if (!attributes.contains(RefusalCountedKey)) {
        attributes.put(RefusalCountedKey, Unit)
        val match = attributes.getOrNull(RouteMatchKey)
        countRefusal(service, service.clientClass(this), caller, match, refusal, enforced, catalog = catalog, at = at)
    }
    if (enforced) throw refusal
}

/**
 * A caller whose `rtg` was ignored ([Caller.routingIgnored]) has no placement,
 * so every store but a served namespace is refused as a catalog its token
 * cannot see. Counted and audited as [requireCatalogEntitlement] does: once
 * per request, and always answered, in every auth mode.
 */
internal fun ApplicationCall.refuseIgnoredRouting(catalog: String) {
    val caller = hoglakeAuth.callerOrNull?.takeIf { it.routingIgnored } ?: return
    val refusal = unknownCatalog(catalog)
    if (!attributes.contains(RefusalCountedKey)) {
        attributes.put(RefusalCountedKey, Unit)
        val service = application.attributes[AuthServiceKey]
        countRefusal(
            service,
            service.clientClass(this),
            caller,
            attributes.getOrNull(RouteMatchKey),
            refusal,
            enforced = true,
            catalog = catalog,
            at = "ignored_routing",
        )
    }
    throw refusal
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
 * whose `cat` allows `read` on [catalog] and who holds [schema], by its name
 * on its host (a local alias never reaches the claim): by name in
 * `remote_schemas`, or as a team schema (`team_<id>`, [TeamNamespaces.id])
 * through the `team` family entry, which also needs the caller's `prd` to
 * hold that id. Deny by default: false for every other caller and for none.
 * A token that places the catalog on hoglite is not refused for it, as a
 * mount's reader is: the catalog is the caller's own.
 */
internal fun Caller?.readsRemoteSchema(
    catalog: String,
    schema: String,
): Boolean =
    this != null && kind == Caller.Kind.SCOPED && catalogClaimAllows(catalog, Scope.READ) &&
        (schema in remoteSchemas || holdsTeamSchema(schema))

/** The family entry's rule for [schema]: a canonical `team_<id>` whose id the caller's `prd` holds with `read`. */
private fun Caller.holdsTeamSchema(schema: String): Boolean {
    if (TeamNamespaces.FAMILY !in remoteSchemaFamilies) return false
    val id = TeamNamespaces.id(schema) ?: return false
    return holdsPredicate(TeamNamespaces.PREDICATE_FIELD, id, Scope.READ)
}

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
 * The team schemas a namespace listing shows a caller holding the `team`
 * family: one for each id its `prd.team_id` names, in numeric order. The
 * listing route has already required `read` on the catalog, and the verifier
 * admits only canonical ids, so each name is one the caller holds. A `*`
 * grant names no ids, so it lists none; its schemas are still readable.
 */
internal fun Caller?.remoteTeamSchemas(): List<String> {
    if (this == null || TeamNamespaces.FAMILY !in remoteSchemaFamilies) return emptyList()
    val grant = predicates[TeamNamespaces.PREDICATE_FIELD] as? PredicateGrant.Values ?: return emptyList()
    return grant.values.keys.sorted().map(TeamNamespaces::name)
}

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
 * [counted] on the mount series: a configured name or family label, never
 * request input.
 */
internal fun ApplicationCall.requireRemoteSchemaEntitlement(
    catalog: String,
    schema: String,
    namespace: String,
    remoteName: String,
    counted: String = schema,
) {
    requireCatalogEntitlement(catalog)
    if (namespace == schema && hoglakeAuth.callerOrNull.readsRemoteSchema(catalog, remoteName)) return
    refuseMissingNamespace(catalog, counted, namespace, "remote_schema")
}

/**
 * A team proven to belong to the caller's org [org]: what a served
 * `team_<id>`'s stream rows are read for. The constructor is private and its
 * one caller is [ImportNamespaceMark], the org-bound seam, after its
 * comparison. That seam is private too, and is reached only through
 * [requireServedNamespaceEntitlement]'s rule ([admit]). The rule takes the
 * caller from the request's verified auth state and reads the mark from the
 * store itself. So no code can make a [ProvenTeam] from a caller or a mark it
 * built, or outside the rule.
 */
class ProvenTeam private constructor(
    val org: String,
    val id: Long,
) {
    internal companion object {
        /** The org bound in use: the one place that compares the org. */
        private val orgBound: TeamOrgBound = ImportNamespaceMark

        /**
         * The caller rule for the served team namespace [namespace] (team
         * [id]) of the caller's own [catalog]. In order, each failure the 404
         * of a missing namespace, in every auth mode: the token names
         * [catalog] ([requireCatalogEntitlement]); the caller
         * [readsRemoteSchema]; the namespace of that name in the family's
         * source catalog, read from [store] only for a caller that got this
         * far, proves the team is the caller's org's ([orgBound]); and the
         * caller's `prd` holds that field and id with `read`.
         */
        fun admit(
            call: ApplicationCall,
            catalog: String,
            namespace: String,
            family: ServedFamily,
            id: Long,
            store: CatalogStore,
        ): ProvenTeam {
            call.requireCatalogEntitlement(catalog)
            val caller = call.hoglakeAuth.callerOrNull
            if (caller != null && caller.readsRemoteSchema(catalog, namespace)) {
                val mark = store.liveContainment(family.sourceCatalog, namespace)
                val team = orgBound.prove(caller, family.predicateField, id, mark)
                // `*` is a grant over every value, and is safe here only because the org
                // bound has already proved this team is the caller's own org's.
                if (team != null && caller.holdsPredicate(family.predicateField, id, Scope.READ)) return team
            }
            call.refuseMissingNamespace(catalog, family.label, namespace, "served_namespace")
        }
    }

    /**
     * The org bound today: the org mark Hogtower's provisioner writes on the
     * team's stored import namespace (#640). It proves [id] when the mark has
     * an org, that org is the caller's, and the mark is `predicateField = id`.
     */
    private object ImportNamespaceMark : TeamOrgBound {
        override fun prove(
            caller: Caller,
            predicateField: String,
            id: Long,
            mark: NamespaceContainment?,
        ): ProvenTeam? {
            val org = mark?.org ?: return null
            if (org != caller.orgId) return null
            if (mark.field != predicateField || mark.value != id) return null
            return ProvenTeam(org, id)
        }
    }
}

/**
 * The one place that proves a team belongs to the caller's org, for a served
 * `team_<id>` (docs/auth-tenancy.md §6.5d): null when it cannot. The proof
 * is the org mark on the team's stored import namespace
 * (`ProvenTeam.ImportNamespaceMark`, its one implementation). The stream
 * layout has no `org_id` (PostHog/hogtower#244), so there is no second
 * condition. An implementation elsewhere cannot make a [ProvenTeam]: only
 * the seam inside that class can.
 */
internal fun interface TeamOrgBound {
    fun prove(
        caller: Caller,
        predicateField: String,
        id: Long,
        mark: NamespaceContainment?,
    ): ProvenTeam?
}

/**
 * `CatalogStoreSelector` asks here before it hands out the binding of the
 * served team namespace [namespace] (team [id], docs/auth-tenancy.md §6.5d)
 * of the caller's own [catalog], which this server does not hold. The rule
 * is [ProvenTeam.admit]'s; it returns the team it proved, or refuses with the
 * 404 of a missing namespace.
 */
internal fun ApplicationCall.requireServedNamespaceEntitlement(
    catalog: String,
    namespace: String,
    family: ServedFamily,
    id: Long,
    store: CatalogStore,
): ProvenTeam = ProvenTeam.admit(this, catalog, namespace, family, id, store)

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
