package com.posthog.hoglake.auth

import com.posthog.hoglake.model.Containment
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.observability.Metrics
import io.ktor.server.application.ApplicationCall
import io.ktor.server.request.httpMethod
import io.ktor.server.request.path
import io.ktor.util.AttributeKey

/**
 * What a handler needs to hold a scoped caller to its namespaces: the
 * namespace's live containment (as the gate reads it) and the catalog's
 * data_path, null when the catalog is missing.
 */
internal class ScopedLookups(
    val containment: (call: ApplicationCall, catalog: String, namespace: String) -> NamespaceContainment?,
    val dataPath: (call: ApplicationCall, catalog: String) -> String?,
    /** The namespace contained for `field = value` and its containment, for an org-marked claim's prefix. */
    val containedFor: (call: ApplicationCall, catalog: String, field: String, value: Long) ->
    Pair<String, NamespaceContainment>? = { _, _, _, _ -> null },
    /**
     * Refuses a request that names a remote schema among the namespaces, for
     * every caller kind (`CatalogStoreSelector.refuseRemoteSchemas`); null on
     * a server that configures no remote schema.
     */
    val refuseRemoteSchemas: ((call: ApplicationCall, catalog: String, namespaces: Collection<String>) -> Unit)? = null,
)

/**
 * Refuse a request on [catalog] that names a remote schema among
 * [namespaces], whoever sent it: a remote schema's name is never a local
 * namespace (docs/auth-tenancy.md §6.5c). The first one in the order given
 * is the answer. A no-op on a server that configures none.
 *
 * A scoped caller's body route has no auth outcome yet when this runs (the
 * plugin left it to the handler, [PendingBodyKey]). A caller the token check
 * refuses is counted there. One it passes was authorized, and its read-only
 * answer is counted `ok` here, as the namespace gate counts it on a path.
 */
internal fun ApplicationCall.refuseRemoteSchemas(
    catalog: String,
    namespaces: Collection<String>,
) {
    val refuse = application.attributes.getOrNull(ScopedLookupsKey)?.refuseRemoteSchemas ?: return
    try {
        refuse(this, catalog, namespaces)
    } catch (failure: HoglakeException) {
        val authorized =
            failure is HoglakeException.ReadOnlyCatalog || failure is HoglakeException.MountOperationUnavailable
        if (authorized && attributes.contains(PendingBodyKey)) {
            val service = application.attributes[AuthServiceKey]
            Metrics.authRequest(service.mode.wire, service.listener, "ok", service.clientClass(this))
        }
        throw failure
    }
}

/** Whether a body route must look up the namespaces behind an id for every caller, not only a scoped one. */
internal val ApplicationCall.guardsRemoteSchemas: Boolean
    get() = application.attributes.getOrNull(ScopedLookupsKey)?.refuseRemoteSchemas != null

/** Present on the application once [installNamespaceGate] ran with its lookups. */
internal val ScopedLookupsKey = AttributeKey<ScopedLookups>("HoglakeScopedLookups")

/** The [AuthDecision.ByBody] the plugin left for the handler on this call. */
internal val PendingBodyKey = AttributeKey<AuthDecision.ByBody>("HoglakePendingBody")

/**
 * Refuse a scoped caller unless every one of [namespaces] in [catalog] is
 * live, contained, and contained for a value it holds with the route's scope.
 * For every other caller their authority was decided by the plugin, and only
 * a remote schema among [namespaces] is refused ([refuseRemoteSchemas]).
 * Every route that names its namespaces in a commit body or a table
 * creation calls this, so it is where that rule holds for those routes; an
 * upload claim names a path and is checked by [requireScopedUploadPrefix].
 *
 * A scoped caller's namespaces are checked one by one in the order given,
 * the remote-schema test and the containment test together, so the answer
 * is the first namespace's that fails either: which one that is does not
 * depend on whether a remote schema is configured.
 *
 * Call it BEFORE the handler reads or writes anything, and before a replay
 * can answer: a commit receipt replays its stored result, and a scoped caller
 * must not learn another namespace's commit through it.
 *
 * Refuses 404 worded as a missing namespace, as the gate does. A scoped caller
 * here without the plugin's [AuthDecision.ByBody] (a route not in
 * [AuthPolicy.BODY_SCOPED_ROUTES] calling this) is refused 403: the handler
 * must not be the first to decide.
 *
 * The check runs without the commit lock. What it reads cannot change under a
 * scoped writer: hoglake clears a namespace's containment only when it holds
 * no table or view, drops a contained namespace only for an operator, and
 * never contains a name another namespace has held a table or view under. So a namespace that passed
 * stays the caller's until an operator acts. Re-marking an org-marked namespace
 * (allowed while it has never held a table or view) has the same unlocked-gate
 * window as clearing a plain mark: a request that passed the gate can still
 * see the namespace re-marked before it commits, and a commit is then held to
 * the new mark.
 */
internal fun ApplicationCall.requireScopedNamespaces(
    catalog: String,
    namespaces: Collection<String>,
) {
    val caller = hoglakeAuth.callerOrNull
    if (caller == null || caller.kind != Caller.Kind.SCOPED) return refuseRemoteSchemas(catalog, namespaces)
    val service = application.attributes[AuthServiceKey]
    val match = AuthPolicy.match(request.httpMethod, request.path())
    val pending = attributes.getOrNull(PendingBodyKey)
    val lookups = application.attributes.getOrNull(ScopedLookupsKey)
    if (pending == null || lookups == null || pending.catalog != catalog) {
        val refusal =
            HoglakeException.Forbidden(
                "a scoped credential cannot use route ${match?.pattern ?: "?"} on '$catalog'",
            )
        countRefusal(service, service.clientClass(this), caller, match, refusal, enforced = true)
        throw refusal
    }
    for (namespace in namespaces.distinct()) {
        refuseRemoteSchemas(catalog, listOf(namespace))
        val held = lookups.containment(this, catalog, namespace)
        if (held == null || !caller.mayUseContained(catalog, held.field, held.value, held.org, pending.required)) {
            val refusal = HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'")
            countRefusal(service, service.clientClass(this), caller, match, refusal, enforced = true)
            throw refusal
        }
    }
    Metrics.authRequest(service.mode.wire, service.listener, "ok", service.clientClass(this))
}

/**
 * The namespace whose prefix a scoped caller's claim at [path] spells
 * literally, or null when it spells none: `<data_path>/data/<namespace>/`
 * for a namespace without an org mark, `org_id=<org>/<field>=<value>/` for
 * the namespace [owner] finds for that pair when its org mark is `<org>`.
 * An org-marked namespace's `data/<namespace>/` is not its prefix.
 */
internal fun scopedClaimNamespace(
    dataPath: String,
    path: String,
    containment: (namespace: String) -> NamespaceContainment?,
    owner: (field: String, value: Long) -> Pair<String, NamespaceContainment>?,
): String? {
    Containment.rawSegment(dataPath, path)?.let { namespace ->
        return namespace.takeIf { containment(it)?.org == null }
    }
    val head = Containment.literalHiveHead(dataPath, path) ?: return null
    val (namespace, held) = owner(head.field, head.value) ?: return null
    return namespace.takeIf { held.org == head.org }
}

internal fun scopedClaimRefusal(dataPath: String): String =
    "a scoped credential's upload must be claimed under ${dataPath.trimEnd('/')}/data/<namespace>/, " +
        "or under its namespace's org prefix"

/**
 * The namespace a scoped caller's upload claim under [prefix] is for, checked
 * as [requireScopedNamespaces] checks it. A contained namespace's files must
 * spell `<data_path>/data/<namespace>/` literally (the commit refuses any
 * other spelling), so the claim must too; anything else is refused 403.
 *
 * For every other caller only a claim under a remote schema's directory is
 * refused ([refuseRemoteSchemas]): `<data_path>/data/<schema>/`, with either
 * segment spelled raw or percent-encoded. That costs one read of the
 * catalog's data_path, on a server that configures a remote schema; on any
 * other server this is a no-op for them.
 */
internal fun ApplicationCall.requireScopedUploadPrefix(
    catalog: String,
    prefix: String,
) {
    val caller = hoglakeAuth.callerOrNull
    val scoped = caller?.kind == Caller.Kind.SCOPED
    if (!scoped && !guardsRemoteSchemas) return
    // The claim's path is `<prefix>/trino-upload/<uuid>.<ext>`: its namespace
    // segment is the prefix's second, or the path's when the prefix is `data`.
    val path = prefix.trimEnd('/') + "/trino-upload/x"
    val found = application.attributes.getOrNull(ScopedLookupsKey)?.dataPath?.invoke(this, catalog)
    if (!scoped) {
        // A missing catalog is the upload backend's to answer, as before.
        found?.let { refuseRemoteSchemas(catalog, Containment.segments(it, path)) }
        return
    }
    val dataPath = found ?: throw HoglakeException.NotFound("catalog '$catalog'")
    val lookups = application.attributes.getOrNull(ScopedLookupsKey)
    val namespace =
        scopedClaimNamespace(
            dataPath,
            path,
            containment = { lookups?.containment?.invoke(this, catalog, it) },
            owner = { field, value -> lookups?.containedFor?.invoke(this, catalog, field, value) },
        )
    if (namespace == null) {
        val service = application.attributes[AuthServiceKey]
        val match = AuthPolicy.match(request.httpMethod, request.path())
        val refusal = HoglakeException.Forbidden(scopedClaimRefusal(dataPath))
        countRefusal(service, service.clientClass(this), caller, match, refusal, enforced = true)
        throw refusal
    }
    requireScopedNamespaces(catalog, listOf(namespace))
}
