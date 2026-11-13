package com.posthog.hoglake.auth

import com.posthog.hoglake.api.BlockingDispatchPhase
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.store.CatalogBinding
import com.posthog.hoglake.store.CatalogStoreSelector
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.ApplicationCallPipeline
import io.ktor.server.application.call
import io.ktor.server.request.httpMethod
import io.ktor.server.request.path
import io.ktor.util.AttributeKey

/** Present on the application once [installNamespaceGate] ran; the plugin defers only then. */
internal val NamespaceGateKey = AttributeKey<Unit>("HoglakeNamespaceGate")

/** The [AuthDecision.ByNamespace] the plugin left for the gate on this call. */
internal val PendingNamespaceKey = AttributeKey<AuthDecision.ByNamespace>("HoglakePendingNamespace")

/**
 * The second half of a scoped caller's authorization: is the namespace its
 * route names one it may use?
 *
 * That needs the namespace's containment, i.e. the database, and the auth
 * plugin runs before the blocking dispatcher on purpose (a refused request
 * never borrows a connection). So the plugin decides everything it can and
 * leaves an [AuthDecision.ByNamespace] on the call; this runs in its own
 * phase AFTER [BlockingDispatchPhase] (so on the request dispatcher, admitted
 * and bounded like any handler) and BEFORE routing, so no handler runs for a
 * namespace the caller may not use.
 *
 * A namespace that is missing, dropped, not contained, or contained for a
 * value the caller does not hold with the route's scope is a 404 worded as
 * the handlers word a missing namespace: a scoped caller must not learn
 * which namespaces exist outside its own.
 *
 * Deny-by-default holds without this installed: the plugin defers only when
 * [NamespaceGateKey] is present, and refuses a [AuthDecision.ByNamespace]
 * otherwise.
 *
 * [containment] is `(catalog, namespace) -> its containment`, null when the
 * namespace is missing, dropped or uncontained. One indexed read
 * (`hog_namespace_live_name`) per scoped request on these routes.
 */
fun Application.installNamespaceGate(
    /** The catalog's data_path, null when it is missing: a scoped upload claim's check (ScopedWrites.kt). */
    dataPath: (catalog: String) -> String? = { null },
    containment: (catalog: String, namespace: String) -> NamespaceContainment?,
) {
    installCallNamespaceGate(
        dataPath = { _, catalog -> dataPath(catalog) },
        containment = { _, catalog, namespace -> containment(catalog, namespace) },
    )
}

/** Resolve both scoped lookups through the current request's verified placement. */
fun Application.installNamespaceGate(selector: CatalogStoreSelector) {
    installCallNamespaceGate(
        dataPath = { call, catalog ->
            try {
                selector.forCatalog(call, catalog).getCatalog(catalog).dataPath
            } catch (_: HoglakeException.NotFound) {
                null
            }
        },
        containment = {
                call,
                catalog,
                namespace,
            ->
            selector.forCatalog(call, catalog).liveContainment(catalog, namespace)
        },
        // A mount (export) resolves here as it does on the handler, cached
        // per request; its isolation is ReadScope in the scan, not
        // containment, so the gate lets every namespace through (plan row
        // 1.3h: "calls resolve and skips containment for an export"). A
        // mount Client's remote binding is the same case one hop early:
        // the Client holds no namespaces, and the Host scopes the read.
        isExport = { call, catalog -> selector.resolve(call, catalog) !is CatalogBinding.Local },
    )
}

private fun Application.installCallNamespaceGate(
    dataPath: (ApplicationCall, String) -> String?,
    containment: (ApplicationCall, String, String) -> NamespaceContainment?,
    isExport: (ApplicationCall, String) -> Boolean = { _, _ -> false },
) {
    // After the blocking dispatcher, so the read runs admitted on it. An
    // application without one (a test harness) gets the gate right after
    // Plugins, which is still before routing.
    insertPhaseAfter(
        if (BlockingDispatchPhase in items) BlockingDispatchPhase else ApplicationCallPipeline.ApplicationPhase.Plugins,
        NamespaceGatePhase,
    )
    attributes.put(NamespaceGateKey, Unit)
    attributes.put(ScopedLookupsKey, ScopedLookups(containment, dataPath))
    intercept(NamespaceGatePhase) {
        val pending = call.attributes.getOrNull(PendingNamespaceKey) ?: return@intercept
        val caller = call.hoglakeAuth.callerOrNull
        val service = call.application.attributes[AuthServiceKey]
        val held =
            try {
                if (isExport(call, pending.catalog)) {
                    // An export serves every namespace to a verified scoped
                    // reader; ReadScope, not the gate, bounds what it sees.
                    Metrics.authRequest(service.mode.wire, service.listener, "ok", service.clientClass(call))
                    return@intercept
                }
                containment(call, pending.catalog, pending.namespace)
            } catch (failure: HoglakeException) {
                // The lookup answered the request itself; the error mapping
                // sends the answer, and this counts the request's one auth
                // outcome. An outage is not an auth outcome.
                when (failure) {
                    // A mount reader on a route the mount does not serve
                    // (store/Mounts.kt): authorized, as the plugin counts it
                    // on the scoped routes it allows outright.
                    is HoglakeException.ReadOnlyCatalog, is HoglakeException.MountOperationUnavailable ->
                        Metrics.authRequest(service.mode.wire, service.listener, "ok", service.clientClass(call))
                    // A mount placed on hoglite, or a catalog no backend holds.
                    is HoglakeException.NotSupported, is HoglakeException.NotFound -> {
                        val match = AuthPolicy.match(call.request.httpMethod, call.request.path())
                        countRefusal(service, service.clientClass(call), caller, match, failure, enforced = true)
                    }
                    else -> Unit
                }
                throw failure
            }
        if (caller != null && held != null &&
            caller.mayUseContained(pending.catalog, held.field, held.value, pending.required)
        ) {
            Metrics.authRequest(service.mode.wire, service.listener, "ok", service.clientClass(call))
            return@intercept
        }
        val refusal =
            HoglakeException.NotFound("namespace '${pending.namespace}' in catalog '${pending.catalog}'")
        val match = AuthPolicy.match(call.request.httpMethod, call.request.path())
        countRefusal(service, service.clientClass(call), caller, match, refusal, enforced = true)
        call.respondRefusal(refusal)
        finish()
    }
}

internal val NamespaceGatePhase = io.ktor.util.pipeline.PipelinePhase("HoglakeNamespaceGate")
