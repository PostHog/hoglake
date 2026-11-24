package com.posthog.hoglake.auth

import com.posthog.hoglake.api.BlockingDispatchPhase
import com.posthog.hoglake.api.NamespaceDto
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.store.CatalogBinding
import com.posthog.hoglake.store.CatalogStoreSelector
import io.ktor.http.HttpHeaders
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.ApplicationCallPipeline
import io.ktor.server.application.call
import io.ktor.server.plugins.BadRequestException
import io.ktor.server.request.httpMethod
import io.ktor.server.request.path
import io.ktor.server.request.receiveChannel
import io.ktor.server.request.uri
import io.ktor.server.response.respond
import io.ktor.util.AttributeKey
import io.ktor.utils.io.readAvailable
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withTimeoutOrNull

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
            // Outside the catch: a refusal by the selector is the request's answer, not a missing catalog.
            val store = selector.forCatalog(call, catalog)
            try {
                store.getCatalog(catalog).dataPath
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
        containedFor = { call, catalog, field, value ->
            selector.forCatalog(call, catalog).liveContainedFor(catalog, field, value)
        },
        // A remote schema, one namespace of a local
        // catalog: resolve has checked the token allows it, which is the
        // whole of this server's check, and no containment exists to read.
        isExport = { call, catalog -> selector.resolve(call, catalog) !is CatalogBinding.Local },
        refuseRemoteSchemas = selector::refuseRemoteSchemas.takeIf { selector.hasRemoteSchemas },
        scopedNamespace = { call, pending ->
            val match = call.attributes.getOrNull(RouteMatchKey)
            if (call.verifiedBackendAuthority?.isReadSession == true &&
                ReadSessionProfile.matchesNamespace(call.request.httpMethod, match) &&
                selector.allowsReadSessionRoute(pending.catalog, pending.namespace)
            ) {
                ScopedNamespaceRead {
                    val expected = "/v1/catalogs/${pending.catalog}/namespaces/${pending.namespace}"
                    val lengths = call.request.headers.getAll(HttpHeaders.ContentLength)
                    if (call.request.uri != expected || (lengths != null && lengths != listOf("0")) ||
                        call.request.headers.getAll(HttpHeaders.TransferEncoding) != null
                    ) {
                        throw BadRequestException("invalid scoped namespace request")
                    }
                    // Bound end-of-stream detection, including HTTP/2 without a length.
                    // Parent cancellation still propagates through withTimeoutOrNull.
                    val empty =
                        withTimeoutOrNull(5_000) {
                            call.request.receiveChannel().readAvailable(ByteArray(1), 0, 1) == -1
                        }
                    if (empty != true) throw BadRequestException("invalid scoped namespace request")
                    runInterruptible {
                        selector.readScopedNamespace(call, ReadSessionTarget(pending.catalog, pending.namespace))
                    }
                }
            } else {
                null
            }
        },
        scopedTableRoute = { call, pending ->
            val match = call.attributes.getOrNull(RouteMatchKey)
            call.verifiedBackendAuthority?.isReadSession == true &&
                (
                    ReadSessionProfile.matchesTable(call.request.httpMethod, match) ||
                        ReadSessionProfile.matchesScan(call.request.httpMethod, match)
                ) &&
                selector.allowsReadSessionRoute(pending.catalog, pending.namespace, match?.table)
        },
        relaysReadSession = { selector.relaysReadSession(it) || selector.servesReadSession(it) },
    )
}

private fun Application.installCallNamespaceGate(
    dataPath: (ApplicationCall, String) -> String?,
    containment: (ApplicationCall, String, String) -> NamespaceContainment?,
    containedFor: (
        ApplicationCall,
        String,
        String,
        Long,
    ) -> Pair<String, NamespaceContainment>? = { _, _, _, _ -> null },
    isExport: (ApplicationCall, String) -> Boolean = { _, _ -> false },
    refuseRemoteSchemas: ((ApplicationCall, String, Collection<String>) -> Unit)? = null,
    scopedNamespace: (ApplicationCall, AuthDecision.ByNamespace) -> ScopedNamespaceRead? = { _, _ -> null },
    scopedTableRoute: (ApplicationCall, AuthDecision.ByNamespace) -> Boolean = { _, _ -> false },
    relaysReadSession: (String) -> Boolean = { false },
) {
    // After the blocking dispatcher, so the read runs admitted on it. An
    // application without one (a test harness) gets the gate right after
    // Plugins, which is still before routing.
    insertPhaseAfter(
        if (BlockingDispatchPhase in items) BlockingDispatchPhase else ApplicationCallPipeline.ApplicationPhase.Plugins,
        NamespaceGatePhase,
    )
    attributes.put(NamespaceGateKey, Unit)
    attributes.put(ScopedLookupsKey, ScopedLookups(containment, dataPath, containedFor, refuseRemoteSchemas))
    intercept(NamespaceGatePhase) {
        val pending = call.attributes.getOrNull(PendingNamespaceKey) ?: return@intercept
        val caller = call.hoglakeAuth.callerOrNull
        val service = call.application.attributes[AuthServiceKey]
        // This selection precedes isExport: resolve can issue legacy SQL.
        // A selected read always finishes here, including every refusal.
        val selected = scopedNamespace(call, pending)
        if (selected == null && scopedTableRoute(call, pending)) {
            // The table and scan routes execute terminal typed reads without legacy SQL.
            return@intercept
        }
        // A read session on a remote schema or a served namespace goes on to
        // isExport, which hands it that binding or the 404 of a missing namespace.
        if (selected == null && call.verifiedBackendAuthority?.isReadSession == true &&
            !relaysReadSession(pending.namespace)
        ) {
            val refusal = HoglakeException.NotFound("namespace")
            countRefusal(
                service,
                service.clientClass(call),
                caller,
                call.attributes.getOrNull(RouteMatchKey),
                refusal,
                enforced = true,
            )
            call.respondRefusal(refusal)
            finish()
            return@intercept
        }
        if (selected != null) {
            val result =
                try {
                    selected.read()
                } catch (failure: HoglakeException.NotFound) {
                    countRefusal(
                        service,
                        service.clientClass(call),
                        caller,
                        AuthPolicy.match(call.request.httpMethod, call.request.path()),
                        failure,
                        enforced = true,
                    )
                    call.respondRefusal(failure)
                    finish()
                    return@intercept
                }
            val mark = result?.containment
            if (result != null && mark != null && caller != null &&
                caller.mayUseContained(pending.catalog, mark.field, mark.value, null, pending.required)
            ) {
                Metrics.authRequest(service.mode.wire, service.listener, "ok", service.clientClass(call))
                call.respond(result)
            } else {
                val refusal = HoglakeException.NotFound("namespace")
                countRefusal(
                    service,
                    service.clientClass(call),
                    caller,
                    AuthPolicy.match(call.request.httpMethod, call.request.path()),
                    refusal,
                    enforced = true,
                )
                call.respondRefusal(refusal)
            }
            finish()
            return@intercept
        }
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
                // outcome. An outage is not an auth outcome. A refusal the
                // lookup counted itself (the store's, StoreGrant.kt) is not
                // counted again.
                if (call.attributes.contains(RefusalCountedKey)) throw failure
                when (failure) {
                    // A mount reader on a route the mount does not serve
                    // (store/Mounts.kt): authorized, as the plugin counts it
                    // on the scoped routes it allows outright.
                    is HoglakeException.ReadOnlyCatalog, is HoglakeException.MountOperationUnavailable ->
                        Metrics.authRequest(service.mode.wire, service.listener, "ok", service.clientClass(call))
                    // A mount placed on hoglite, or a catalog no backend holds.
                    // One a remote schema refused was counted where it was decided (StoreEntitlement.kt).
                    is HoglakeException.NotSupported, is HoglakeException.NotFound ->
                        if (!call.attributes.contains(RefusalCountedKey)) {
                            val match = AuthPolicy.match(call.request.httpMethod, call.request.path())
                            countRefusal(service, service.clientClass(call), caller, match, failure, enforced = true)
                        }
                    else -> Unit
                }
                throw failure
            }
        if (caller != null && held != null &&
            caller.mayUseContained(pending.catalog, held.field, held.value, held.org, pending.required)
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

private class ScopedNamespaceRead(val read: suspend () -> NamespaceDto?)
