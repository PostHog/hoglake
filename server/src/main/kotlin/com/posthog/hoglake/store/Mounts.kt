package com.posthog.hoglake.store

import com.posthog.hoglake.auth.AuthPolicy
import com.posthog.hoglake.auth.Caller
import com.posthog.hoglake.auth.Requirement
import com.posthog.hoglake.auth.Scope
import com.posthog.hoglake.model.HoglakeException
import io.ktor.server.application.ApplicationCall
import io.ktor.server.request.httpMethod
import io.ktor.server.request.path
import org.jdbi.v3.core.Jdbi

/**
 * Mount names: `<orgID>-events`, a canonical lowercase UUID and a fixed suffix.
 *
 * Reserved in every backend: `POST /v1/catalogs` refuses the shape before
 * placement selects a backend, so no catalog created through it can take a
 * mount's name. A PostgreSQL name taken before the reservation is the mount
 * write's to refuse; a hoglite one is kept out by the issuer's `cat` grammar.
 * Where both exist, the mount resolves first on every `{catalog}` route,
 * and `GET /v1/catalogs` hides the catalog. Only names of this shape reach
 * mount resolution.
 */
object MountNames {
    const val PATTERN = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-events$"
    private val regex = Regex(PATTERN)

    fun isReserved(name: String): Boolean = regex.matches(name)
}

/** One mount. The read scope (source table, field, values, revision) comes with the mount table. */
data class MountRecord(
    val name: String,
)

/** Mount records by name. Production has none until the mount table and its operator API exist. */
fun interface MountRegistry {
    fun find(name: String): MountRecord?

    companion object {
        val NONE = MountRegistry { null }

        /**
         * [inner]'s mounts while the instance-wide switch is on. The switch is
         * read only for a name [inner] holds, so with no mounts nothing reads it.
         */
        fun behindSwitch(
            switchOn: () -> Boolean,
            inner: MountRegistry,
        ) = MountRegistry { name -> inner.find(name)?.takeIf { switchOn() } }

        /** `hog_mount_switch` in this instance's PostgreSQL, off by default; a missing row reads as off. */
        fun postgresSwitch(jdbi: () -> Jdbi): () -> Boolean =
            {
                jdbi().withHandle<Boolean, Exception> {
                    it.createQuery("SELECT EXISTS (SELECT 1 FROM hog_mount_switch WHERE enabled)")
                        .mapTo(Boolean::class.java)
                        .one()
                }
            }
    }
}

/** What a catalog name resolves to for one request (#291 §4). */
sealed interface CatalogBinding {
    /** A catalog in a backend store. Writers exist only here. */
    data class Local(
        val store: CatalogStore,
    ) : CatalogBinding

    /** A read-only mount over this process's PostgreSQL, never over a token-routed store. */
    data class Mount(
        val mount: MountRecord,
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
 * A mount serves only a verified scoped caller whose `cat` names it, in every
 * auth mode; under `off` no caller is verified, so it serves no one. Anyone
 * else gets a 404 worded as a missing catalog. A token that places the mount
 * on hoglite is refused: a mount is never hoglite-backed. (A routed token
 * names one catalog, which the `cat` check has just shown is the mount.)
 */
internal fun requireMountReader(
    caller: Caller?,
    mount: String,
) {
    if (!mayReadMount(caller, mount)) throw HoglakeException.NotFound("catalog '$mount'")
    if (caller?.routing?.backend == BackendKind.HOGLITE) {
        throw HoglakeException.NotSupported("a mount is never served from a hoglite backend")
    }
}

private fun mayReadMount(
    caller: Caller?,
    mount: String,
): Boolean = caller?.kind == Caller.Kind.SCOPED && caller.catalogClaimAllows(mount, Scope.READ)

/**
 * The names a catalog listing may show [caller] as mounts: those in its own
 * `cat` that [requireMountReader] would let it read. Never null and never
 * "every mount": no caller, a tenant and an operator see none.
 */
internal fun mountVisibility(caller: Caller?): Set<String> =
    if (caller == null || caller.routing?.backend == BackendKind.HOGLITE) {
        emptySet()
    } else {
        caller.catalogScopes.keys.filterTo(sortedSetOf()) { MountNames.isReserved(it) && mayReadMount(caller, it) }
    }

/**
 * The refusal for a route that has no mount implementation, chosen before any
 * service call: a write is never allowed on a mount, and a read waits for the
 * mount reader. The route's class comes from the route table, which covers
 * every served path; an unmatched path is refused as a write.
 */
internal fun mountRefusal(call: ApplicationCall): HoglakeException =
    if (AuthPolicy.match(call.request.httpMethod, call.request.path())?.requirement == Requirement.READ) {
        HoglakeException.MountOperationUnavailable("this operation is not available on a mount")
    } else {
        HoglakeException.ReadOnlyCatalog("a mount is read-only")
    }
