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
    val containment: (catalog: String, namespace: String) -> NamespaceContainment?,
    val dataPath: (catalog: String) -> String?,
)

/** Present on the application once [installNamespaceGate] ran with its lookups. */
internal val ScopedLookupsKey = AttributeKey<ScopedLookups>("HoglakeScopedLookups")

/** The [AuthDecision.ByBody] the plugin left for the handler on this call. */
internal val PendingBodyKey = AttributeKey<AuthDecision.ByBody>("HoglakePendingBody")

/**
 * Refuse a scoped caller unless every one of [namespaces] in [catalog] is
 * live, contained, and contained for a value it holds with the route's scope.
 * A no-op for every other caller: their authority was decided by the plugin.
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
 * never contains a name another namespace has had. So a namespace that passed
 * stays the caller's until an operator acts.
 */
internal fun ApplicationCall.requireScopedNamespaces(
    catalog: String,
    namespaces: Collection<String>,
) {
    val caller = hoglakeAuth.callerOrNull
    if (caller == null || caller.kind != Caller.Kind.SCOPED) return
    val service = application.attributes[AuthServiceKey]
    val match = AuthPolicy.match(request.httpMethod, request.path())
    val pending = attributes.getOrNull(PendingBodyKey)
    val lookups = application.attributes.getOrNull(ScopedLookupsKey)
    if (pending == null || lookups == null || pending.catalog != catalog) {
        val refusal =
            HoglakeException.Forbidden(
                "a scoped credential cannot use route ${match?.pattern ?: "?"} on '$catalog'",
            )
        countRefusal(service, clientClass(this), caller, match, refusal, enforced = true)
        throw refusal
    }
    for (namespace in namespaces.distinct()) {
        val held = lookups.containment(catalog, namespace)
        if (held == null || !caller.mayUseContained(catalog, held.field, held.value, pending.required)) {
            val refusal = HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'")
            countRefusal(service, clientClass(this), caller, match, refusal, enforced = true)
            throw refusal
        }
    }
    Metrics.authRequest(service.mode.wire, service.listener, "ok", clientClass(this))
}

/**
 * The namespace a scoped caller's upload claim under [prefix] is for, checked
 * as [requireScopedNamespaces] checks it. A contained namespace's files must
 * spell `<data_path>/data/<namespace>/` literally (the commit refuses any
 * other spelling), so the claim must too; anything else is refused 403. A
 * no-op for every other caller.
 */
internal fun ApplicationCall.requireScopedUploadPrefix(
    catalog: String,
    prefix: String,
) {
    val caller = hoglakeAuth.callerOrNull
    if (caller == null || caller.kind != Caller.Kind.SCOPED) return
    val dataPath =
        application.attributes.getOrNull(ScopedLookupsKey)?.dataPath?.invoke(catalog)
            ?: throw HoglakeException.NotFound("catalog '$catalog'")
    // The claim's path is `<prefix>/trino-upload/<uuid>.<ext>`: its namespace
    // segment is the prefix's second, or the path's when the prefix is `data`.
    val namespace = Containment.rawSegment(dataPath, prefix.trimEnd('/') + "/trino-upload/x")
    if (namespace == null) {
        val service = application.attributes[AuthServiceKey]
        val match = AuthPolicy.match(request.httpMethod, request.path())
        val refusal =
            HoglakeException.Forbidden(
                "a scoped credential's upload must be claimed under ${dataPath.trimEnd('/')}/data/<namespace>/",
            )
        countRefusal(service, clientClass(this), caller, match, refusal, enforced = true)
        throw refusal
    }
    requireScopedNamespaces(catalog, listOf(namespace))
}
