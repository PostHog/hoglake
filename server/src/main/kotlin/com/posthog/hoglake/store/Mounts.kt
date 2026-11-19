package com.posthog.hoglake.store

import com.posthog.hoglake.auth.AuthPolicy
import com.posthog.hoglake.auth.Requirement
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Metrics
import io.ktor.server.application.ApplicationCall
import io.ktor.server.request.httpMethod
import io.ktor.server.request.path

/**
 * The retired mount names: `<orgID>-events`, a canonical lowercase UUID and a
 * fixed suffix. They no longer resolve to anything, and stay refused at creation
 * (`POST /v1/catalogs`, before placement) because other services still name them.
 */
object MountNames {
    const val PATTERN = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-events$"
    private val regex = Regex(PATTERN)

    fun isReserved(name: String): Boolean = regex.matches(name)
}

/** What a catalog name resolves to for one request (#291 §4). */
sealed interface CatalogBinding {
    /** A catalog in a backend store. Writers exist only here. */
    data class Local(
        val store: CatalogStore,
    ) : CatalogBinding

    /**
     * A remote schema (docs/auth-tenancy.md §6.5c): the request's path names
     * the namespace [schema] of the local catalog [catalog], and [host]
     * serves that namespace. Built only once the caller's token is known to
     * allow it (auth/StoreEntitlement.kt). The catalog's own store is never
     * consulted for this namespace: a route with no relay refuses
     * ([remoteSchemaRefusal]).
     */
    data class RemoteSchema(
        val catalog: String,
        val schema: String,
        val host: MountHostClient,
        /** What this schema's traffic is counted against: its configured name, or its family's label. */
        val label: String = schema,
    ) : CatalogBinding

    /**
     * A namespace this server serves to relayed readers (docs/auth-tenancy.md
     * §6.5d): the request's path names [namespace], and [catalog] is the
     * caller's own, not one this server holds. [reader] reads the source
     * catalog's namespace of that name. Built only once the caller's token
     * and the namespace's org mark are known to allow it.
     */
    data class Served(
        val catalog: String,
        val namespace: String,
        val reader: ServedNamespaceReader,
        /** What a refusal is counted against: the served family's label, never a team id. */
        val label: String,
    ) : CatalogBinding
}

/** #291's `CatalogRegistry.resolve`: a mount name resolves before any backend lookup. */
fun interface CatalogRegistry {
    fun resolve(
        call: ApplicationCall,
        catalog: String,
    ): CatalogBinding
}

/**
 * The refusal for a request that names the remote schema [schema] on a route
 * that is not relayed: a write is never allowed
 * on a remote schema (403), and a read with no relay is unavailable (501).
 * Only for a caller the token check has passed (auth/StoreEntitlement.kt);
 * build it only to throw it.
 */
internal fun remoteSchemaRefusal(
    call: ApplicationCall,
    schema: String,
): HoglakeException =
    if (AuthPolicy.match(call.request.httpMethod, call.request.path())?.requirement == Requirement.READ) {
        refused(
            schema,
            "mount_operation_unavailable",
            HoglakeException.MountOperationUnavailable("this operation is not available on a remote schema"),
        )
    } else {
        refused(schema, "read_only_catalog", HoglakeException.ReadOnlyCatalog("a remote schema is read-only"))
    }

/**
 * The refusal for a request that names a served namespace on a route that is
 * not relayed, with [remoteSchemaRefusal]'s codes, counted against the
 * served family's [label]. Only for a caller the entitlement check has
 * passed (auth/StoreEntitlement.kt); build it only to throw it.
 */
internal fun servedNamespaceRefusal(
    call: ApplicationCall,
    label: String,
): HoglakeException =
    if (AuthPolicy.match(call.request.httpMethod, call.request.path())?.requirement == Requirement.READ) {
        refused(
            label,
            "mount_operation_unavailable",
            HoglakeException.MountOperationUnavailable("this operation is not available on a served namespace"),
        )
    } else {
        refused(label, "read_only_catalog", HoglakeException.ReadOnlyCatalog("a served namespace is read-only"))
    }

/**
 * Count [failure] against [mount], by the error code it is answered with.
 * Only a resolved binding's name reaches here (a record the registry
 * returned or, on a mount Client, a name its reader's verified `cat`
 * holds), so request input cannot add a series. Each refusal ends its
 * request, so a request is counted once.
 */
private fun refused(
    mount: String,
    reason: String,
    failure: HoglakeException,
): HoglakeException = failure.also { Metrics.mountRefused(mount, reason) }
