package com.posthog.hoglake.auth

import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.store.CatalogStore
import io.ktor.server.application.ApplicationCall
import java.util.Optional
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * What one request's caller may do with a store the selector handed out for
 * [catalog]. The caller's view of the store (store/BoundCatalogStore.kt) and
 * of each sub-backend (store/BoundBackends.kt) asks here before every
 * operation, with the catalog name and the namespace its handler passed. The
 * first line of each view method is its rule: [open], [read], [write],
 * [closed], or one of the body and id rules below.
 *
 * This is the second layer. The plugin, the namespace gate and the
 * scoped-write checks (ScopedWrites.kt) decide first, on what the request
 * names; this decides again on what the handler gives the store.
 *
 * - Every caller: the store answers only for the catalog it was handed out
 *   for. Another name gets the plugin's 404 for a catalog the token does not
 *   name.
 * - A scoped caller: a namespace must be live, contained, and contained for a
 *   value the token holds with the scope of the operation: read or write, the
 *   lower of `prd` and `cat` ([Caller.mayUseContained]). The route table is
 *   not asked. Any other namespace gets the gate's 404 for a missing
 *   namespace, and a table creation in one gets the 404 of a creation that
 *   does not exist. An operation that names no namespace is refused 403,
 *   unless its method here says it is open.
 *
 * The modes follow [requireCatalogEntitlement]. Under `off` there is no
 * grant ([of] answers null) and the selector hands out the store itself.
 * Under `warn` a refusal is counted, audited and let through, except for a
 * scoped caller. A request adds at most one refusal from here to the counts
 * and the audit.
 *
 * What a check reads is kept for the request: the gate and the handler's own
 * checks read through the view or the selector, so a namespace's containment,
 * the catalog's data_path and a table creation's namespace are each read once.
 *
 * The views are written out method by method, with no `by` delegation. A
 * method added to an interface without a body does not compile until its
 * view is written; one added with a default body compiles, and BoundStoreTest
 * fails until its rule is listed there.
 */
internal class StoreGrant private constructor(
    private val call: ApplicationCall,
    /** The catalog the selector resolved. Null for the catalog listing, which resolves none. */
    private val catalog: String?,
    /** The store itself, for the lookups only. */
    private val store: CatalogStore,
    private val caller: Caller?,
) {
    private val scoped = caller?.takeIf { it.kind == Caller.Kind.SCOPED }
    private val held = ConcurrentHashMap<String, Optional<NamespaceContainment>>()
    private val idNamespaces = ConcurrentHashMap<UUID, String>()

    @Volatile
    private var dataPath: String? = null

    /** An operation on [name] that is open to every caller kind: held to the catalog only. */
    fun open(name: String) {
        if (name != catalog) refuse(unknownCatalog(name), name)
    }

    /** An operation that reads [namespace] of [name]. */
    fun read(
        name: String,
        namespace: String,
    ) = mustHold(name, namespace, Scope.READ)

    /** An operation that changes [namespace] of [name]. */
    fun write(
        name: String,
        namespace: String,
    ) = mustHold(name, namespace, Scope.WRITE)

    /** An operation on [name] that names no namespace, or every one: not for a scoped caller. */
    fun closed(name: String) {
        open(name)
        if (scoped == null) return
        val pattern = call.attributes.getOrNull(RouteMatchKey)?.pattern ?: "?"
        refuse(HoglakeException.Forbidden("a scoped credential cannot use route $pattern on '$name'"), name)
    }

    /**
     * A listing of the catalogs in [visible], or of every catalog when it is
     * null. It names no single catalog, so the rule is the token's: every
     * name must be one the token names, and "every catalog" is a fleet-wide
     * operator's.
     */
    fun catalogs(visible: Set<String>?) {
        val other =
            if (visible == null) {
                "*".takeIf { caller?.fleetWide != true }
            } else {
                visible.firstOrNull { caller?.knowsCatalog(it) != true }
            }
        if (other != null) refuse(unknownCatalog(other), other)
    }

    /** [items] of [name] as this caller may read them: a scoped caller sees its own namespaces only. */
    fun <T> readable(
        name: String,
        items: List<T>,
        containment: (T) -> NamespaceContainment?,
    ): List<T> {
        val caller = scoped ?: return items
        return items.filter { item ->
            containment(item)?.let { caller.mayUseContained(name, it.field, it.value, it.org, Scope.READ) } == true
        }
    }

    /**
     * The live containment of [namespace], which is what the checks
     * themselves read (auth/NamespaceGate.kt). For a scoped caller it is read
     * once per request, and a namespace the token does not allow for read
     * answers null, as a missing one does.
     */
    fun containment(
        name: String,
        namespace: String,
    ): NamespaceContainment? {
        open(name)
        val caller = scoped ?: return store.liveContainment(name, namespace)
        return held(name, namespace)?.takeIf { caller.mayUseContained(name, it.field, it.value, it.org, Scope.READ) }
    }

    /**
     * The namespace contained for `field = value` and its containment. For a
     * scoped caller, null unless the token may read that value, as
     * [containment] answers for a name.
     */
    fun containedFor(
        name: String,
        field: String,
        value: Long,
    ): Pair<String, NamespaceContainment>? {
        open(name)
        val found = store.liveContainedFor(name, field, value) ?: return null
        val caller = scoped ?: return found
        return found.takeIf { caller.mayUseContained(name, field, value, found.second.org, Scope.READ) }
    }

    /** The catalog's data_path, as an operation that read it found it: kept for [claim]. */
    fun dataPath(path: String) {
        dataPath = path
    }

    /**
     * An upload claim under [prefix]: for a scoped caller, a write to the
     * namespace the prefix spells, found as `requireScopedUploadPrefix`
     * (ScopedWrites.kt) finds it and refused in its words when it spells none.
     */
    fun claim(
        name: String,
        prefix: String,
    ) {
        open(name)
        if (scoped == null) return
        val path = dataPath ?: store.getCatalog(name).dataPath.also { dataPath = it }
        val namespace =
            scopedClaimNamespace(
                path,
                prefix.trimEnd('/') + "/trino-upload/x",
                containment = { held(name, it) },
                owner = { field, value -> store.liveContainedFor(name, field, value) },
            )
        if (namespace == null) {
            refuse(HoglakeException.Forbidden(scopedClaimRefusal(path)), name)
        } else {
            mustHold(name, namespace, Scope.WRITE)
        }
    }

    /** An operation that read table creation [id] and found it in [namespace]. The pair is kept for [write]. */
    fun read(
        name: String,
        id: UUID,
        namespace: String,
    ) {
        mustHold(name, namespace, Scope.READ, id)
        found(id, namespace)
    }

    /**
     * An operation that changes table creation [id], which names no namespace
     * itself. For a scoped caller [find] reads the namespace it is in, unless
     * this request already read it; a missing creation answers there as it
     * does for anyone.
     */
    fun write(
        name: String,
        id: UUID,
        find: () -> String,
    ) {
        open(name)
        if (scoped == null) return
        mustHold(name, idNamespaces.computeIfAbsent(id) { find() }, Scope.WRITE, id)
    }

    /** Table creation [id] is in [namespace], as the handler's own check read it: kept for [write]. */
    fun found(
        id: UUID,
        namespace: String,
    ) {
        if (scoped != null) idNamespaces[id] = namespace
    }

    /**
     * The namespace rule itself; a no-op for every caller that is not scoped.
     * For table [creation] the answer is that of a creation that does not
     * exist, so neither its existence nor its namespace shows; the audit
     * still names the namespace.
     */
    private fun mustHold(
        name: String,
        namespace: String,
        scope: Scope,
        creation: UUID? = null,
    ) {
        open(name)
        val caller = scoped ?: return
        val held = held(name, namespace)
        if (held == null || !caller.mayUseContained(name, held.field, held.value, held.org, scope)) {
            val refusal = HoglakeException.NotFound("namespace '$namespace' in catalog '$name'")
            val missing = creation?.let { HoglakeException.NotFound("table creation operation '$it'") }
            refuse(refusal, name, missing ?: refusal)
        }
    }

    private fun held(
        name: String,
        namespace: String,
    ): NamespaceContainment? =
        held.computeIfAbsent(namespace) { Optional.ofNullable(store.liveContainment(name, it)) }.orElse(null)

    /**
     * Count [refusal] once for the request and answer [answer], as the
     * store's catalog check does ([requireCatalogEntitlement]). It returns
     * only under `warn`, and never for a scoped caller. The audit names
     * [refused], the catalog the store was asked for.
     */
    private fun refuse(
        refusal: HoglakeException,
        refused: String,
        answer: HoglakeException = refusal,
    ) {
        val service = call.application.attributes[AuthServiceKey]
        val enforced = service.mode.isEnforcing || scoped != null
        if (!call.attributes.contains(RefusalCountedKey)) {
            call.attributes.put(RefusalCountedKey, Unit)
            val match = call.attributes.getOrNull(RouteMatchKey)
            countRefusal(service, service.clientClass(call), caller, match, refusal, enforced, refused, at = "store")
        }
        if (enforced) throw answer
    }

    companion object {
        /** The grant for a store handed out for [catalog] on this request; null when no caller is resolved. */
        fun of(
            call: ApplicationCall,
            catalog: String?,
            store: CatalogStore,
        ): StoreGrant? {
            val auth = call.hoglakeAuth
            return if (auth is AuthState.Off) null else StoreGrant(call, catalog, store, auth.callerOrNull)
        }
    }
}
