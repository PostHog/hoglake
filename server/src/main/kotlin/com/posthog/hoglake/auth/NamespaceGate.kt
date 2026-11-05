package com.posthog.hoglake.auth

import com.posthog.hoglake.api.BlockingDispatchPhase
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.observability.Metrics
import io.ktor.server.application.Application
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
fun Application.installNamespaceGate(containment: (catalog: String, namespace: String) -> NamespaceContainment?) {
    // After the blocking dispatcher, so the read runs admitted on it. An
    // application without one (a test harness) gets the gate right after
    // Plugins, which is still before routing.
    insertPhaseAfter(
        if (BlockingDispatchPhase in items) BlockingDispatchPhase else ApplicationCallPipeline.ApplicationPhase.Plugins,
        NamespaceGatePhase,
    )
    attributes.put(NamespaceGateKey, Unit)
    intercept(NamespaceGatePhase) {
        val pending = call.attributes.getOrNull(PendingNamespaceKey) ?: return@intercept
        val caller = call.hoglakeAuth.callerOrNull
        val service = call.application.attributes[AuthServiceKey]
        val held = containment(pending.catalog, pending.namespace)
        if (caller != null && held != null &&
            caller.mayUseContained(pending.catalog, held.field, held.value, pending.required)
        ) {
            Metrics.authRequest(service.mode.wire, service.listener, "ok", clientClass(call))
            return@intercept
        }
        val refusal =
            HoglakeException.NotFound("namespace '${pending.namespace}' in catalog '${pending.catalog}'")
        val match = AuthPolicy.match(call.request.httpMethod, call.request.path())
        countRefusal(service, clientClass(call), caller, match, refusal)
        call.respondRefusal(refusal)
        finish()
    }
}

internal val NamespaceGatePhase = io.ktor.util.pipeline.PipelinePhase("HoglakeNamespaceGate")
