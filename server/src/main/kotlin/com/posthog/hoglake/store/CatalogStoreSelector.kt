package com.posthog.hoglake.store

import com.fasterxml.jackson.databind.JsonNode
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.Routing
import com.posthog.hoglake.auth.hoglakeAuth
import com.posthog.hoglake.auth.verifiedBackendToken
import com.posthog.hoglake.hoglite.HogliteCatalogStore
import com.posthog.hoglake.hoglite.HogliteClient
import com.posthog.hoglake.hoglite.HoglitePartitionLookupAdmission
import com.posthog.hoglake.hoglite.HogliteTenantResolver
import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.service.AlterBackend
import com.posthog.hoglake.service.PartitionListingBackend
import com.posthog.hoglake.service.PartitionLookupBackend
import com.posthog.hoglake.service.PartitionStatsBackend
import com.posthog.hoglake.service.TableCreationBackend
import com.posthog.hoglake.service.UploadBackend
import com.posthog.hoglake.service.ViewBackend
import io.ktor.server.application.ApplicationCall
import io.ktor.util.AttributeKey
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/** Select a catalog backend from verified placement, never from request parameters or claim endpoints. */
class CatalogStoreSelector(
    private val postgres: CatalogStore?,
    private val cfg: Config? = null,
    /** Mount records, consulted only for reserved names. */
    private val mounts: MountRegistry = MountRegistry.NONE,
    /**
     * Client mode (plan row 1.3c): which catalog names are another host's
     * mounts. A name it resolves is that host's and [mounts] is not
     * consulted for it; the default resolves none. A server is a Host or a
     * Client (Config refuses both at boot).
     */
    private val mountHosts: MountHostResolver = MountHostResolver.NONE,
) : CatalogRegistry {
    private val clients = ConcurrentHashMap<UUID, HogliteClient>()
    private val lookupAdmissions = ConcurrentHashMap<UUID, HoglitePartitionLookupAdmission>()
    private val requestStore = AttributeKey<HogliteCatalogStore>("HoglakeCatalogStore")

    /**
     * The binding resolved for this request's `{catalog}`, cached so the
     * namespace gate and the handler resolve a mount once (plan row 1.3h:
     * "cache the binding per request"). A request names one catalog, so a
     * single entry suffices; only a successful resolution is cached.
     */
    private val boundCatalog = AttributeKey<Pair<String, CatalogBinding>>("HoglakeCatalogBinding")

    /**
     * Return one request-scoped hoglite store, or the process's PostgreSQL
     * singleton. Every catalog route calls this before any service or body
     * work, so a mount is refused here on every route it has no reader for.
     */
    fun forCatalog(
        call: ApplicationCall,
        catalog: String,
    ): CatalogStore =
        when (val binding = resolve(call, catalog)) {
            is CatalogBinding.Local -> binding.store
            is CatalogBinding.Mount -> throw mountRefusal(call, binding.mount.name)
            is CatalogBinding.Remote -> throw mountRefusal(call, binding.name)
        }

    /**
     * A mount name resolves first (to the Host's mount on a Client, else to
     * this instance's own), then verified placement, then the default backend.
     */
    override fun resolve(
        call: ApplicationCall,
        catalog: String,
    ): CatalogBinding {
        call.attributes.getOrNull(boundCatalog)?.let { (name, binding) -> if (name == catalog) return binding }
        val binding =
            remote(call, catalog) ?: mount(call, catalog) ?: run {
                val routing = call.hoglakeAuth.callerOrNull?.routing
                CatalogBinding.Local(
                    if (routing != null && routing.catalog == catalog) routed(call, routing) else fallback(catalog),
                )
            }
        call.attributes.put(boundCatalog, catalog to binding)
        return binding
    }

    /**
     * A name a mount host serves ([MountHostResolver]) is that host's
     * whatever backends this server has: the binding is by the name alone,
     * with no record here. The caller is held to the mount reader's rule
     * first, so one the host would refuse that way is refused here and
     * nothing is sent. A refusal is counted against the name only once the
     * caller's verified `cat` is known to hold it; before that the name is
     * raw request input and would mint a series per probe.
     */
    fun remote(
        call: ApplicationCall,
        catalog: String,
    ): CatalogBinding.Remote? {
        val caller = call.hoglakeAuth.callerOrNull
        val host = mountHosts.resolve(caller, catalog) ?: return null
        requireMountReader(caller, catalog) { reason ->
            if (reason != "not_found") Metrics.mountRefused(catalog, reason)
        }
        return CatalogBinding.Remote(catalog, host)
    }

    /** A mount reads this process's PostgreSQL, so it exists only where PostgreSQL is the default. */
    private fun mount(
        call: ApplicationCall,
        catalog: String,
    ): CatalogBinding.Mount? {
        val record = record(catalog) ?: return null
        requireMountReader(call.hoglakeAuth.callerOrNull, record)
        return CatalogBinding.Mount(record)
    }

    /** The mount [catalog] names, whoever asks: the one gate for dispatch and the listing. */
    private fun record(catalog: String): MountRecord? =
        if (MountNames.isReserved(catalog) && defaultPostgres() != null) mounts.find(catalog) else null

    /** A hoglite listing must carry the token's single catalog as an explicit filter. */
    fun forListing(call: ApplicationCall): Listing {
        val caller = call.hoglakeAuth.callerOrNull
        val routing = caller?.routing
        val mounts = mountVisibility(caller)
        return if (routing != null) {
            Listing(
                routed(call, routing),
                setOf(routing.catalog),
                usePostgresTotals = routing.backend == BackendKind.POSTGRES,
                mounts = mounts,
            )
        } else {
            // A mount reader on a Client with no default backend has no local
            // catalogs to list, only the Host's mounts.
            val store = if (mounts.any { mountHosts.resolve(caller, it) != null }) defaultPostgres() else fallback("")
            Listing(store, caller?.visibleCatalogs(), usePostgresTotals = true, mounts = mounts)
        }
    }

    /**
     * Which of [names] are mounts for this request's caller, through the
     * gates [resolve] uses, each name read once: a listing hides catalogs
     * and shows mounts from one answer. A name a mount host serves is one
     * whatever that host would say of it; which of them it serves is its
     * own listing's to say ([remoteCatalogs]).
     */
    fun mountsAmong(
        call: ApplicationCall,
        names: Collection<String>,
    ): Set<String> {
        val caller = call.hoglakeAuth.callerOrNull
        return names.toSet().filterTo(HashSet()) { mountHosts.resolve(caller, it) != null || record(it) != null }
    }

    /**
     * The entries the mount hosts list for [mounts] (names the caller's
     * own `cat` holds, [mountVisibility]), as each host wrote them: one
     * upstream listing per host that serves any of them, so the fan-out is
     * bounded by configuration, never by the token. Null when no host
     * serves any of [mounts], which is a server that is not their Client.
     */
    suspend fun remoteCatalogs(
        call: ApplicationCall,
        mounts: Collection<String>,
    ): List<JsonNode>? {
        val caller = call.hoglakeAuth.callerOrNull
        val byHost = LinkedHashMap<MountHostClient, MutableSet<String>>()
        for (mount in mounts) {
            mountHosts.resolve(caller, mount)?.let { byHost.getOrPut(it) { LinkedHashSet() }.add(mount) }
        }
        if (byHost.isEmpty()) return null
        return byHost.flatMap { (host, names) -> host.catalogs(call, names) }
    }

    /**
     * The row every mount presents under its own name: the instance's
     * source catalog (`HOGLAKE_MOUNT_SOURCE_CATALOG`). Unset or missing, the
     * instance is broken, not the mount absent: a 500, never a 404 that
     * names another catalog.
     */
    fun mountSource(): CatalogInfo {
        val source = cfg?.mountSourceCatalog.orEmpty()
        check(source.isNotEmpty()) { "a mount resolved, but HOGLAKE_MOUNT_SOURCE_CATALOG is not set" }
        return try {
            checkNotNull(defaultPostgres()).getCatalog(source)
        } catch (_: HoglakeException.NotFound) {
            error("HOGLAKE_MOUNT_SOURCE_CATALOG names no catalog")
        }
    }

    /** Resolve sub-backends through the same request-scoped store and default policy. */
    fun uploads(
        call: ApplicationCall,
        catalog: String,
        postgres: UploadBackend,
    ): UploadBackend = uploads(call, catalog) { postgres }

    fun uploads(
        call: ApplicationCall,
        catalog: String,
        postgres: () -> UploadBackend,
    ): UploadBackend = (forCatalog(call, catalog) as? HogliteCatalogStore)?.uploads ?: postgres()

    fun tableCreations(
        call: ApplicationCall,
        catalog: String,
        postgres: TableCreationBackend,
    ): TableCreationBackend = tableCreations(call, catalog) { postgres }

    fun tableCreations(
        call: ApplicationCall,
        catalog: String,
        postgres: () -> TableCreationBackend,
    ): TableCreationBackend = (forCatalog(call, catalog) as? HogliteCatalogStore)?.tableCreations ?: postgres()

    fun alters(
        call: ApplicationCall,
        catalog: String,
        postgres: AlterBackend,
    ): AlterBackend = alters(call, catalog) { postgres }

    fun alters(
        call: ApplicationCall,
        catalog: String,
        postgres: () -> AlterBackend,
    ): AlterBackend = (forCatalog(call, catalog) as? HogliteCatalogStore)?.alters ?: postgres()

    fun views(
        call: ApplicationCall,
        catalog: String,
        postgres: ViewBackend,
    ): ViewBackend = views(call, catalog) { postgres }

    fun views(
        call: ApplicationCall,
        catalog: String,
        postgres: () -> ViewBackend,
    ): ViewBackend = (forCatalog(call, catalog) as? HogliteCatalogStore)?.views ?: postgres()

    fun partitionStats(
        call: ApplicationCall,
        catalog: String,
        postgres: PartitionStatsBackend,
    ): PartitionStatsBackend = partitionStats(call, catalog) { postgres }

    fun partitionStats(
        call: ApplicationCall,
        catalog: String,
        postgres: () -> PartitionStatsBackend,
    ): PartitionStatsBackend = (forCatalog(call, catalog) as? HogliteCatalogStore)?.partitions ?: postgres()

    fun partitionListing(
        call: ApplicationCall,
        catalog: String,
        postgres: PartitionListingBackend,
    ): PartitionListingBackend = partitionListing(call, catalog) { postgres }

    fun partitionListing(
        call: ApplicationCall,
        catalog: String,
        postgres: () -> PartitionListingBackend,
    ): PartitionListingBackend = (forCatalog(call, catalog) as? HogliteCatalogStore)?.partitions ?: postgres()

    fun partitionLookup(
        call: ApplicationCall,
        catalog: String,
        postgres: PartitionLookupBackend,
    ): PartitionLookupBackend = partitionLookup(call, catalog) { postgres }

    fun partitionLookup(
        call: ApplicationCall,
        catalog: String,
        postgres: () -> PartitionLookupBackend,
    ): PartitionLookupBackend = (forCatalog(call, catalog) as? HogliteCatalogStore)?.partitionLookup ?: postgres()

    /** Maintenance is PostgreSQL-only. Resolve placement before any service or body work. */
    fun requirePostgres(
        call: ApplicationCall,
        catalog: String,
    ) {
        if (forCatalog(call, catalog) is HogliteCatalogStore) {
            throw HoglakeException.NotSupported("this operation is not supported on a hoglite backend")
        }
    }

    /** Readiness also checks every configured hoglite backend, including clients not yet used by a request. */
    fun reachable(): Boolean = cfg?.hogliteBackends?.keys?.all { client(it).reachable() } ?: true

    private fun fallback(catalog: String): CatalogStore =
        defaultPostgres() ?: throw HoglakeException.NotFound("catalog '$catalog'")

    private fun defaultPostgres(): CatalogStore? = postgres?.takeIf { cfg == null || cfg.defaultBackend == "postgres" }

    private fun routed(
        call: ApplicationCall,
        routing: Routing,
    ): CatalogStore =
        when (routing.backend) {
            BackendKind.POSTGRES -> postgres ?: throw HoglakeException.NotFound("catalog '${routing.catalog}'")
            BackendKind.HOGLITE ->
                call.attributes.computeIfAbsent(requestStore) {
                    HogliteCatalogStore(
                        client(routing.backendId).forRequest(call.verifiedBackendToken),
                        HogliteTenantResolver { catalog ->
                            if (catalog == routing.catalog) {
                                checkNotNull(routing.tenant)
                            } else {
                                throw HoglakeException.NotFound("catalog '$catalog'")
                            }
                        },
                        refuseBlindPartitionedAppends = checkNotNull(cfg).refuseBlindPartitionedAppends,
                        ledgerRetentionSeconds = cfg.removalLedgerRetentionSeconds,
                        smallFileThresholdBytes = cfg.compactionTargetBytes,
                        minInputFiles = cfg.compactionMinInputFiles,
                        maxInputFiles = cfg.compactionMaxInputFiles,
                        partitionLookupAdmission =
                            lookupAdmissions.computeIfAbsent(routing.backendId) {
                                HoglitePartitionLookupAdmission()
                            },
                        operatorAuthority = call.hoglakeAuth.callerOrNull?.kind?.isOperator() != false,
                    )
                }
        }

    private fun client(id: UUID): HogliteClient =
        clients.computeIfAbsent(id) {
            HogliteClient.fromConfig(checkNotNull(cfg).hogliteBackends.getValue(it))
        }

    data class Listing(
        /** The backend whose catalogs are listed; null only for a mount reader on a Client with no default backend. */
        val store: CatalogStore?,
        val catalogs: Set<String>?,
        /** Only PostgreSQL catalogs can use the PostgreSQL sampler's name-keyed totals. */
        val usePostgresTotals: Boolean,
        /** The names the caller may see as mounts ([mountVisibility]): required, and never "all". */
        val mounts: Set<String>,
    )
}
