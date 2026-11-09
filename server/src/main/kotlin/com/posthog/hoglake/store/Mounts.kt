package com.posthog.hoglake.store

import com.posthog.hoglake.auth.AuthPolicy
import com.posthog.hoglake.auth.Caller
import com.posthog.hoglake.auth.Requirement
import com.posthog.hoglake.auth.Scope
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Metrics
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
 * Where both exist, the mount resolves first on every `{catalog}` route.
 * Only names of this shape reach mount resolution.
 */
object MountNames {
    const val PATTERN = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-events$"
    private val regex = Regex(PATTERN)

    fun isReserved(name: String): Boolean = regex.matches(name)
}

/**
 * One mount as `hog_mount` holds it (V35). The defaults are a mount never
 * written, which is how `GET /v1/mounts/{name}` answers for one. The table it
 * reads is the instance's, not the record's: every mount reads the same one.
 */
data class MountRecord(
    val name: String,
    /** One more on every write; 0 means never written. */
    val revision: Long = 0,
    val enabled: Boolean = false,
    /** The values it admits: ascending, distinct, non-negative. */
    val values: List<Long> = emptyList(),
)

/** The serving mount for a name: production's is [MountStore]. */
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
 * A refusal is counted against the mount.
 */
internal fun requireMountReader(
    caller: Caller?,
    mount: MountRecord,
) {
    if (caller?.kind != Caller.Kind.SCOPED || !caller.catalogClaimAllows(mount.name, Scope.READ)) {
        throw refused(mount, "not_found", HoglakeException.NotFound("catalog '${mount.name}'"))
    }
    if (caller.routing?.backend == BackendKind.HOGLITE) {
        throw refused(
            mount,
            "not_supported_on_backend",
            HoglakeException.NotSupported("a mount is never served from a hoglite backend"),
        )
    }
}

/**
 * A mount write is the configured writer's alone ([writer], hogtower's events
 * mount loop), verified in every auth mode. Another operator, `platform:admin`, a
 * tenant, and no caller (under `off`, or `warn` without a token) get 403.
 */
internal fun requireMountWriter(
    caller: Caller?,
    writer: String,
) {
    if (writer.isEmpty() || caller?.kind != Caller.Kind.OPERATOR || caller.subject != writer) {
        throw HoglakeException.Forbidden("only the configured mount writer may write a mount")
    }
}

/**
 * The refusal for a route that has no mount implementation, chosen before any
 * service call: a write is never allowed on a mount, and a read waits for the
 * mount reader. The route's class comes from the route table, which covers
 * every served path; an unmatched path is refused as a write. Building it
 * counts it against the mount, so build it only to throw it.
 */
internal fun mountRefusal(
    call: ApplicationCall,
    mount: MountRecord,
): HoglakeException =
    if (AuthPolicy.match(call.request.httpMethod, call.request.path())?.requirement == Requirement.READ) {
        refused(
            mount,
            "mount_operation_unavailable",
            HoglakeException.MountOperationUnavailable("this operation is not available on a mount"),
        )
    } else {
        refused(mount, "read_only_catalog", HoglakeException.ReadOnlyCatalog("a mount is read-only"))
    }

/**
 * Count [failure] against [mount], by the error code it is answered with.
 * Only a record the registry returned reaches here, so request input cannot
 * add a series. Each refusal ends its request, so a request is counted once.
 */
private fun refused(
    mount: MountRecord,
    reason: String,
    failure: HoglakeException,
): HoglakeException = failure.also { Metrics.mountRefused(mount.name, reason) }
