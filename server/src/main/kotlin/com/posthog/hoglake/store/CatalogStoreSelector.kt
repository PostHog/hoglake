package com.posthog.hoglake.store

import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.Routing
import com.posthog.hoglake.auth.hoglakeAuth
import com.posthog.hoglake.hoglite.HogliteCatalogStore
import com.posthog.hoglake.hoglite.HogliteClient
import com.posthog.hoglake.hoglite.HoglitePartitionLookupAdmission
import com.posthog.hoglake.hoglite.HogliteTenantResolver
import com.posthog.hoglake.model.HoglakeException
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
) : CatalogRegistry {
    private val clients = ConcurrentHashMap<UUID, HogliteClient>()
    private val lookupAdmissions = ConcurrentHashMap<UUID, HoglitePartitionLookupAdmission>()
    private val requestStore = AttributeKey<HogliteCatalogStore>("HoglakeCatalogStore")

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
            is CatalogBinding.Mount -> throw mountRefusal(call, binding.mount)
        }

    /** A mount name resolves first, then verified placement, then the default backend. */
    override fun resolve(
        call: ApplicationCall,
        catalog: String,
    ): CatalogBinding {
        mount(call, catalog)?.let { return it }
        val routing = call.hoglakeAuth.callerOrNull?.routing
        return CatalogBinding.Local(
            if (routing != null && routing.catalog == catalog) routed(call, routing) else fallback(catalog),
        )
    }

    /** A mount reads this process's PostgreSQL, so it exists only where PostgreSQL is the default. */
    private fun mount(
        call: ApplicationCall,
        catalog: String,
    ): CatalogBinding.Mount? {
        if (!MountNames.isReserved(catalog) || defaultPostgres() == null) return null
        val record = mounts.find(catalog) ?: return null
        requireMountReader(call.hoglakeAuth.callerOrNull, record)
        return CatalogBinding.Mount(record)
    }

    /** A hoglite listing must carry the token's single catalog as an explicit filter. */
    fun forListing(call: ApplicationCall): Listing {
        val caller = call.hoglakeAuth.callerOrNull
        val routing = caller?.routing
        return if (routing != null) {
            Listing(
                routed(call, routing),
                setOf(routing.catalog),
                usePostgresTotals = routing.backend == BackendKind.POSTGRES,
            )
        } else {
            Listing(fallback(""), caller?.visibleCatalogs(), usePostgresTotals = true)
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
                        client(routing.backendId),
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
                    )
                }
        }

    private fun client(id: UUID): HogliteClient =
        clients.computeIfAbsent(id) {
            HogliteClient.fromConfig(checkNotNull(cfg).hogliteBackends.getValue(it))
        }

    data class Listing(
        val store: CatalogStore,
        val catalogs: Set<String>?,
        /** Only PostgreSQL catalogs can use the PostgreSQL sampler's name-keyed totals. */
        val usePostgresTotals: Boolean,
    )
}
