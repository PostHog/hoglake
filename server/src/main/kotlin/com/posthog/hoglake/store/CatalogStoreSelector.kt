package com.posthog.hoglake.store

import com.fasterxml.jackson.databind.JsonNode
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthPolicy
import com.posthog.hoglake.auth.Caller
import com.posthog.hoglake.auth.Routing
import com.posthog.hoglake.auth.StoreGrant
import com.posthog.hoglake.auth.hoglakeAuth
import com.posthog.hoglake.auth.refuseIgnoredRouting
import com.posthog.hoglake.auth.remoteSchemasAmong
import com.posthog.hoglake.auth.remoteTeamSchemas
import com.posthog.hoglake.auth.requireCatalogEntitlement
import com.posthog.hoglake.auth.requireRemoteSchemaEntitlement
import com.posthog.hoglake.auth.requireServedNamespaceEntitlement
import com.posthog.hoglake.auth.verifiedBackendToken
import com.posthog.hoglake.compaction.CompactionConfig
import com.posthog.hoglake.compaction.ParquetRewriter
import com.posthog.hoglake.hoglite.HogliteCatalogStore
import com.posthog.hoglake.hoglite.HogliteCleanup
import com.posthog.hoglake.hoglite.HogliteClient
import com.posthog.hoglake.hoglite.HogliteCompaction
import com.posthog.hoglake.hoglite.HogliteCompactionCursors
import com.posthog.hoglake.hoglite.HogliteMaintenanceStatus
import com.posthog.hoglake.hoglite.HoglitePartitionLookupAdmission
import com.posthog.hoglake.hoglite.HogliteTenantResolver
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.service.AlterBackend
import com.posthog.hoglake.service.CleanupBackend
import com.posthog.hoglake.service.CompactionBackend
import com.posthog.hoglake.service.ExpiryBackend
import com.posthog.hoglake.service.HydratorMaintenanceBackend
import com.posthog.hoglake.service.MaintenanceStatusBackend
import com.posthog.hoglake.service.OptionsBackend
import com.posthog.hoglake.service.PartitionListingBackend
import com.posthog.hoglake.service.PartitionLookupBackend
import com.posthog.hoglake.service.PartitionStatsBackend
import com.posthog.hoglake.service.RemovalStore
import com.posthog.hoglake.service.RetirementBackend
import com.posthog.hoglake.service.TableCreationBackend
import com.posthog.hoglake.service.TeamNamespaces
import com.posthog.hoglake.service.UploadBackend
import com.posthog.hoglake.service.ViewBackend
import io.ktor.server.application.ApplicationCall
import io.ktor.server.request.httpMethod
import io.ktor.server.request.path
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
    /** The server owns this store. Compaction borrows it for one request. */
    private val objectStore: () -> ObjectStore = { error("compaction object store is not configured") },
    /** The server owns this store. Request-scoped cleanup backends only borrow it. */
    private val removalStore: () -> RemovalStore = { error("cleanup object store is not configured") },
    /** Host mode: the families of namespaces served to relayed readers (docs/auth-tenancy.md §6.5d). Empty is none. */
    servedFamilies: List<ServedFamily> = emptyList(),
) : CatalogRegistry {
    private val servedTeams = servedFamilies.singleOrNull { it.family == TeamNamespaces.FAMILY }

    private val clients = ConcurrentHashMap<UUID, HogliteClient>()
    private val lookupAdmissions = ConcurrentHashMap<UUID, HoglitePartitionLookupAdmission>()
    private val compactionCursors = ConcurrentHashMap<UUID, HogliteCompactionCursors>()
    private val requestStore = AttributeKey<HogliteCatalogStore>("HoglakeCatalogStore")

    /**
     * The binding resolved for this request's `{catalog}`, cached so the
     * namespace gate and the handler resolve a mount once (plan row 1.3h:
     * "cache the binding per request"). A request names one catalog, so a
     * single entry suffices; only a successful resolution is cached. It
     * holds what was handed out and, for a local store, the store behind
     * the view ([Handed]).
     */
    private val boundCatalog = AttributeKey<Pair<String, Handed>>("HoglakeCatalogBinding")

    /**
     * Under auth this hands out the caller's view of the store ([asView]),
     * never a [HogliteCatalogStore] itself: `forCatalog(...) as?
     * HogliteCatalogStore` is null there and quietly takes the PostgreSQL
     * path. Choose a backend from the store itself, as [handOut] does.
     *
     * Return one request-scoped hoglite store, or the process's PostgreSQL
     * singleton. Every catalog route calls this before any service or body
     * work, so a mount is refused here on every route it has no reader for,
     * and so is a remote schema the route's path names.
     */
    fun forCatalog(
        call: ApplicationCall,
        catalog: String,
    ): CatalogStore =
        when (val binding = resolve(call, catalog)) {
            is CatalogBinding.Local -> binding.store
            is CatalogBinding.Mount -> throw mountRefusal(call, binding.mount.name)
            is CatalogBinding.Remote -> throw mountRefusal(call, binding.name)
            is CatalogBinding.RemoteSchema -> throw remoteSchemaRefusal(call, binding.label)
            is CatalogBinding.Served -> throw servedNamespaceRefusal(call, binding.label)
        }

    /**
     * A mount name resolves first (to the Host's mount on a Client, else to
     * this instance's own), then verified placement, then the default
     * backend. In a catalog this server holds, a remote schema the request's
     * path names then takes the store's place, so the store is never asked
     * about that namespace.
     * Whatever [catalog] a handler passes, it resolves only when the caller's
     * token allows that name, and a remote schema only when the token allows
     * that too (auth/StoreEntitlement.kt).
     */
    override fun resolve(
        call: ApplicationCall,
        catalog: String,
    ): CatalogBinding {
        call.attributes.getOrNull(boundCatalog)?.let { (name, handed) -> if (name == catalog) return handed.binding }
        // Before any binding: a name resolves only when the caller's token allows it.
        call.requireCatalogEntitlement(catalog)
        // Before every other binding: a served namespace ignores the token's placement (`rtg`) too.
        val binding =
            servedBinding(call, catalog) ?: remoteBinding(call, catalog) ?: mount(call, catalog) ?: run {
                requirePlaced(call, catalog)
                val routing = call.hoglakeAuth.callerOrNull?.routing
                val routed = routing != null && routing.catalog == catalog
                val store = if (routed) routed(call, checkNotNull(routing)) else fallback(catalog)
                pathSchema(call, catalog, store) ?: CatalogBinding.Local(store)
            }
        val handed = asView(call, catalog, binding)
        call.attributes.put(boundCatalog, catalog to handed)
        return handed.binding
    }

    /**
     * A caller whose `rtg` was ignored has no placement: only a served
     * namespace is open to it, so every other binding answers as a catalog
     * its token cannot see, before a mount, a remote or the default backend.
     */
    private fun requirePlaced(
        call: ApplicationCall,
        catalog: String,
    ) {
        call.refuseIgnoredRouting(catalog)
    }

    /**
     * The binding for a path that names a canonical `team_<id>` of a served
     * family, in a catalog this server does not hold: an operator reading the
     * source catalog itself keeps the normal local path. Only the canonical
     * name is served; a case variant or a padded id is not. The caller rule
     * refuses in every auth mode ([requireServedNamespaceEntitlement]), and
     * the source namespace is read once, for a caller that passes its token
     * checks.
     */
    private fun servedBinding(
        call: ApplicationCall,
        catalog: String,
    ): CatalogBinding.Served? {
        val family = servedTeams ?: return null
        if (MountNames.isReserved(catalog)) return null
        // Only a scoped caller enters the served rule. Anyone else is handled as for any
        // catalog this server does not hold, so no refusal can say whether a name is served.
        if (call.hoglakeAuth.callerOrNull?.kind != Caller.Kind.SCOPED) return null
        val match = AuthPolicy.match(call.request.httpMethod, call.request.path())
        val namespace = match?.namespace?.takeIf { match.catalog == catalog } ?: return null
        val id = TeamNamespaces.id(namespace) ?: return null
        val store = defaultPostgres() ?: return null
        try {
            store.getCatalog(catalog)
            return null
        } catch (_: HoglakeException.NotFound) {
            // Not a catalog here: the served namespace is the only reading this path has.
        }
        call.requireServedNamespaceEntitlement(catalog, namespace, family, id) {
            store.liveContainment(family.sourceCatalog, namespace)
        }
        return CatalogBinding.Served(
            catalog,
            namespace,
            ServedNamespaceReader(store, family.sourceCatalog, namespace, catalog),
            family.label,
        )
    }

    /**
     * A name a mount host serves ([MountHostResolver]) is that host's
     * whatever backends this server has: the binding is by the name alone,
     * with no record here. The caller is held to the mount reader's rule
     * first, so one the host would refuse that way is refused here and
     * nothing is sent. A refusal is counted against the name only once the
     * caller's verified `cat` is known to hold it; before that the name is
     * raw request input and would mint a series per probe.
     *
     * The scan route asks here before [resolve], so the token is checked
     * here as it is there (auth/StoreEntitlement.kt).
     */
    fun remote(
        call: ApplicationCall,
        catalog: String,
    ): CatalogBinding.Remote? {
        call.requireCatalogEntitlement(catalog)
        return remoteBinding(call, catalog)
    }

    private fun remoteBinding(
        call: ApplicationCall,
        catalog: String,
    ): CatalogBinding.Remote? {
        val caller = call.hoglakeAuth.callerOrNull
        val host = mountHosts.resolve(caller, catalog) ?: return null
        requirePlaced(call, catalog)
        requireMountReader(caller, catalog) { reason ->
            if (reason != "not_found") Metrics.mountRefused(catalog, reason)
        }
        return CatalogBinding.Remote(catalog, host)
    }

    /**
     * The remote schema this request's path names in [catalog], through
     * [resolve]; null, and nothing resolved, for every other request. A
     * route that relays before it parses anything asks this, so the token
     * is checked here for [catalog] as it is in [remote].
     */
    fun remoteSchema(
        call: ApplicationCall,
        catalog: String,
    ): CatalogBinding.RemoteSchema? {
        call.requireCatalogEntitlement(catalog)
        return if (namedSchema(call, catalog) == null) null else resolve(call, catalog) as? CatalogBinding.RemoteSchema
    }

    /** The configured remote schema the path's `{namespace}` is, or is a case variant of; the namespace as named. */
    private fun namedSchema(
        call: ApplicationCall,
        catalog: String,
    ): Pair<RemoteSchemaHost, String>? {
        val schemas = mountHosts.remoteSchemas()
        if (schemas.isEmpty()) return null
        // The route table splits the path exactly as the router does, and
        // is what mountRefusal reads the route's class from.
        val match = AuthPolicy.match(call.request.httpMethod, call.request.path())
        val namespace = match?.namespace?.takeIf { match.catalog == catalog } ?: return null
        return schemas.firstOrNull { it.match.covers(namespace) }?.let { it to namespace }
    }

    /**
     * The binding for a path that names a remote schema, reached only once
     * the token is known to allow [catalog], [catalog] is not a mount, and
     * a backend of this server ([store]) is the one that would serve it.
     * This is the one place the binding is handed out, and it is only when
     * the token allows the schema too: before the namespace gate lets a
     * scoped caller on and before any handler, so nothing is sent to the
     * Host for anyone else.
     *
     * A remote schema is a namespace of a catalog this server holds, so
     * the catalog must exist in [store]: one read, made only for a caller
     * the token check has passed, once per request (the binding is cached).
     * A catalog nobody holds is the store's own 404, and nothing is sent.
     */
    private fun pathSchema(
        call: ApplicationCall,
        catalog: String,
        store: CatalogStore,
    ): CatalogBinding.RemoteSchema? {
        val (schema, namespace) = namedSchema(call, catalog) ?: return null
        val name = schema.match.schemaFor(namespace)
        call.requireRemoteSchemaEntitlement(catalog, name, namespace, schema.remoteNameFor(name), schema.name)
        store.getCatalog(catalog)
        return CatalogBinding.RemoteSchema(catalog, name, schema.host, schema.name)
    }

    /** Whether this server configures any remote schema. */
    val hasRemoteSchemas: Boolean get() = mountHosts.remoteSchemas().isNotEmpty()

    /** Whether [namespace] is a configured remote schema's name or a case variant: never a local namespace here. */
    fun isRemoteSchema(namespace: String): Boolean = mountHosts.remoteSchemas().any { it.match.covers(namespace) }

    /** The remote schemas a namespace listing of the local [catalog] shows this caller: those its token allows. */
    fun visibleRemoteSchemas(
        call: ApplicationCall,
        catalog: String,
    ): List<String> {
        call.requireCatalogEntitlement(catalog)
        val caller = call.hoglakeAuth.callerOrNull
        val schemas = mountHosts.remoteSchemas()
        val named =
            schemas
                .filter { it.match is MountMatch.Namespace }
                .associate { it.name to it.remoteNameFor(it.name) }
        val hasTeamFamily = schemas.any { it.match is MountMatch.NamespaceFamily }
        val teams = if (hasTeamFamily) caller.remoteTeamSchemas() else emptyList()
        return caller.remoteSchemasAmong(catalog, named) + teams
    }

    /**
     * Refuse a request on the local [catalog] that names a remote schema in
     * its body or query ([namespaces]): a commit's targets, a table
     * creation's namespace, a `namespace` parameter. The answer is the one
     * a path naming it gets: 404 unless the caller's token allows the
     * schema, then 403 for a write and 501 for a read. A no-op when none of
     * [namespaces] is a remote schema, and on a server that configures none.
     */
    fun refuseRemoteSchemas(
        call: ApplicationCall,
        catalog: String,
        namespaces: Collection<String>,
    ) {
        call.requireCatalogEntitlement(catalog)
        val schemas = mountHosts.remoteSchemas()
        if (schemas.isEmpty()) return
        for (namespace in namespaces) {
            val schema = schemas.firstOrNull { it.match.covers(namespace) } ?: continue
            if (resolve(call, catalog) !is CatalogBinding.Local) return
            val name = schema.match.schemaFor(namespace)
            call.requireRemoteSchemaEntitlement(catalog, name, namespace, schema.remoteNameFor(name), schema.name)
            throw remoteSchemaRefusal(call, schema.name)
        }
    }

    /**
     * The live tables of the local [catalog] that sit in a namespace a
     * remote schema hides, by UUID, each with that namespace's name: what a
     * route that addresses a table by UUID alone checks before it serves.
     * Empty, and nothing read, on a server that configures no remote schema.
     */
    fun hiddenTables(
        store: CatalogStore,
        catalog: String,
    ): Map<UUID, String> {
        if (!hasRemoteSchemas) return emptyMap()
        return store.listNamespaces(catalog).map { it.name }.filter(::isRemoteSchema)
            .flatMap { namespace -> store.listTables(catalog, namespace).map { it.tableUuid to namespace } }
            .toMap()
    }

    /** A mount reads this process's PostgreSQL, so it exists only where PostgreSQL is the default. */
    private fun mount(
        call: ApplicationCall,
        catalog: String,
    ): CatalogBinding.Mount? {
        val record = record(catalog) ?: return null
        requirePlaced(call, catalog)
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
        if (caller?.routingIgnored == true) {
            // No placement, so no catalog of this instance to list.
            return Listing(listed(call, fallback("")), emptySet(), usePostgresTotals = true, mounts = emptySet())
        }
        return if (routing != null) {
            Listing(
                listed(call, routed(call, routing)),
                setOf(routing.catalog),
                usePostgresTotals = routing.backend == BackendKind.POSTGRES,
                mounts = mounts,
            )
        } else {
            // A mount reader on a Client with no default backend has no local
            // catalogs to list, only the Host's mounts.
            val store = if (mounts.any { mountHosts.resolve(caller, it) != null }) defaultPostgres() else fallback("")
            Listing(listed(call, store), caller?.visibleCatalogs(), usePostgresTotals = true, mounts = mounts)
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
    ): UploadBackend = handOut(call, catalog, postgres, ::BoundUploads) { it.uploads }

    fun tableCreations(
        call: ApplicationCall,
        catalog: String,
        postgres: TableCreationBackend,
    ): TableCreationBackend = tableCreations(call, catalog) { postgres }

    fun tableCreations(
        call: ApplicationCall,
        catalog: String,
        postgres: () -> TableCreationBackend,
    ): TableCreationBackend = handOut(call, catalog, postgres, ::BoundTableCreations) { it.tableCreations }

    fun alters(
        call: ApplicationCall,
        catalog: String,
        postgres: AlterBackend,
    ): AlterBackend = alters(call, catalog) { postgres }

    fun alters(
        call: ApplicationCall,
        catalog: String,
        postgres: () -> AlterBackend,
    ): AlterBackend = handOut(call, catalog, postgres, ::BoundAlters) { it.alters }

    fun views(
        call: ApplicationCall,
        catalog: String,
        postgres: ViewBackend,
    ): ViewBackend = views(call, catalog) { postgres }

    fun views(
        call: ApplicationCall,
        catalog: String,
        postgres: () -> ViewBackend,
    ): ViewBackend = handOut(call, catalog, postgres, ::BoundViews) { it.views }

    fun partitionStats(
        call: ApplicationCall,
        catalog: String,
        postgres: PartitionStatsBackend,
    ): PartitionStatsBackend = partitionStats(call, catalog) { postgres }

    fun partitionStats(
        call: ApplicationCall,
        catalog: String,
        postgres: () -> PartitionStatsBackend,
    ): PartitionStatsBackend = handOut(call, catalog, postgres, ::BoundPartitionStats) { it.partitions }

    fun partitionListing(
        call: ApplicationCall,
        catalog: String,
        postgres: PartitionListingBackend,
    ): PartitionListingBackend = partitionListing(call, catalog) { postgres }

    fun partitionListing(
        call: ApplicationCall,
        catalog: String,
        postgres: () -> PartitionListingBackend,
    ): PartitionListingBackend = handOut(call, catalog, postgres, ::BoundPartitionListing) { it.partitions }

    fun partitionLookup(
        call: ApplicationCall,
        catalog: String,
        postgres: PartitionLookupBackend,
    ): PartitionLookupBackend = partitionLookup(call, catalog) { postgres }

    fun partitionLookup(
        call: ApplicationCall,
        catalog: String,
        postgres: () -> PartitionLookupBackend,
    ): PartitionLookupBackend = handOut(call, catalog, postgres, ::BoundPartitionLookup) { it.partitionLookup }

    /** Resolve options through the same request-scoped store as catalog operations. */
    fun options(
        call: ApplicationCall,
        catalog: String,
        postgres: OptionsBackend,
    ): OptionsBackend = options(call, catalog) { postgres }

    fun options(
        call: ApplicationCall,
        catalog: String,
        postgres: () -> OptionsBackend,
    ): OptionsBackend = handOut(call, catalog, postgres, ::BoundOptions) { it.options }

    fun expiry(
        call: ApplicationCall,
        catalog: String,
        postgres: ExpiryBackend,
    ): ExpiryBackend = expiry(call, catalog) { postgres }

    fun expiry(
        call: ApplicationCall,
        catalog: String,
        postgres: () -> ExpiryBackend,
    ): ExpiryBackend = handOut(call, catalog, postgres, ::BoundExpiry) { it.expiry }

    fun cleanup(
        call: ApplicationCall,
        catalog: String,
        postgres: CleanupBackend,
    ): CleanupBackend = cleanup(call, catalog) { postgres }

    fun cleanup(
        call: ApplicationCall,
        catalog: String,
        postgres: () -> CleanupBackend,
    ): CleanupBackend =
        handOut(call, catalog, postgres, ::BoundCleanup) {
            HogliteCleanup(checkNotNull(cfg), it, removalStore())
        }

    fun compaction(
        call: ApplicationCall,
        catalog: String,
        postgres: CompactionBackend,
    ): CompactionBackend = compaction(call, catalog) { postgres }

    fun compaction(
        call: ApplicationCall,
        catalog: String,
        postgres: () -> CompactionBackend,
    ): CompactionBackend =
        handOut(call, catalog, postgres, ::BoundCompaction) {
            HogliteCompaction(
                it,
                objectStore(),
                hogliteCompactionConfig(),
                cursors =
                    compactionCursors.computeIfAbsent(checkNotNull(call.hoglakeAuth.callerOrNull?.routing).backendId) {
                        HogliteCompactionCursors()
                    },
            )
        }

    fun retirement(
        call: ApplicationCall,
        catalog: String,
        postgres: RetirementBackend,
    ): RetirementBackend = retirement(call, catalog) { postgres }

    fun retirement(
        call: ApplicationCall,
        catalog: String,
        postgres: () -> RetirementBackend,
    ): RetirementBackend = handOut(call, catalog, postgres, ::BoundRetirement) { it.retirement }

    fun hydratorMaintenance(
        call: ApplicationCall,
        catalog: String,
        postgres: HydratorMaintenanceBackend,
    ): HydratorMaintenanceBackend = hydratorMaintenance(call, catalog) { postgres }

    fun hydratorMaintenance(
        call: ApplicationCall,
        catalog: String,
        postgres: () -> HydratorMaintenanceBackend,
    ): HydratorMaintenanceBackend =
        handOut(call, catalog, postgres, ::BoundHydratorMaintenance) { it.hydratorMaintenance }

    fun maintenanceStatus(
        call: ApplicationCall,
        catalog: String,
        postgres: MaintenanceStatusBackend,
    ): MaintenanceStatusBackend = maintenanceStatus(call, catalog) { postgres }

    fun maintenanceStatus(
        call: ApplicationCall,
        catalog: String,
        postgres: () -> MaintenanceStatusBackend,
    ): MaintenanceStatusBackend =
        handOut(call, catalog, postgres, ::BoundMaintenanceStatus) {
            val config = checkNotNull(cfg)
            HogliteMaintenanceStatus(
                it,
                config.hydratorIntervalMs,
                config.expiryIntervalMs,
                config.cleanupIntervalMs,
                config.compactionIntervalMs,
                config.retirementIntervalMs,
                config.compactionTargetBytes,
            )
        }

    /** Fleet reads require a configured PostgreSQL backend, not a tenant placement. */
    fun requirePostgresInstance() {
        if (cfg != null && cfg.defaultBackend == "none" && cfg.postgresBackendId.isEmpty()) {
            throw HoglakeException.NotSupported("this operation requires a PostgreSQL backend")
        }
    }

    /** Refuse PostgreSQL-only operations before any service or body work. */
    fun requirePostgres(
        call: ApplicationCall,
        catalog: String,
    ) {
        if (local(call, catalog).store is HogliteCatalogStore) throw notOnHoglite()
    }

    private fun notOnHoglite() = HoglakeException.NotSupported("this operation is not supported on a hoglite backend")

    /**
     * The namespace table creation [operationId] of [catalog] was prepared
     * for, read from the backend itself: the input of the handler's own check
     * (api/TableCreationRoutes.kt), which decides before the handler asks the
     * caller's view. The view then finds it kept for the request.
     */
    fun creationNamespace(
        call: ApplicationCall,
        catalog: String,
        operationId: UUID,
        postgres: () -> TableCreationBackend,
    ): String {
        val local = local(call, catalog)
        val backend = (local.store as? HogliteCatalogStore)?.tableCreations ?: postgres()
        return backend.status(catalog, operationId).definition.namespace.also { local.grant?.found(operationId, it) }
    }

    /**
     * What [resolve] handed out for a catalog on this request ([binding]),
     * and for a local store the store itself and the grant its views ask
     * (auth/StoreGrant.kt).
     */
    private class Handed(
        val binding: CatalogBinding,
        val store: CatalogStore? = null,
        val grant: StoreGrant? = null,
    )

    /**
     * A local store leaves this class only as the caller's view of it
     * (BoundCatalogStore.kt), which holds every operation to this catalog
     * and to what the caller's token allows in it. With no caller resolved
     * (auth `off`) there is no grant, and the store itself is handed out.
     */
    private fun asView(
        call: ApplicationCall,
        catalog: String,
        binding: CatalogBinding,
    ): Handed {
        if (binding !is CatalogBinding.Local) return Handed(binding)
        val grant = StoreGrant.of(call, catalog, binding.store) ?: return Handed(binding, binding.store)
        return Handed(CatalogBinding.Local(BoundCatalogStore(grant, binding.store)), binding.store, grant)
    }

    /** What [forCatalog] handed out, after every refusal it makes: for this class's backend choice. */
    private fun local(
        call: ApplicationCall,
        catalog: String,
    ): Handed {
        forCatalog(call, catalog)
        // forCatalog resolved [catalog] last, so the request's one entry is its.
        return call.attributes[boundCatalog].second
    }

    /**
     * The sub-backend of [catalog] for this request: the hoglite store's
     * own ([hoglite]) or the PostgreSQL service, chosen from the store
     * itself and handed out as the caller's view of it ([view]).
     */
    private fun <T : Any> handOut(
        call: ApplicationCall,
        catalog: String,
        postgres: () -> T,
        view: (StoreGrant, T) -> T,
        hoglite: (HogliteCatalogStore) -> T,
    ): T {
        val local = local(call, catalog)
        val backend = (local.store as? HogliteCatalogStore)?.let(hoglite) ?: postgres()
        return local.grant?.let { view(it, backend) } ?: backend
    }

    /** The catalog listing's store, as a view bound to no catalog: it lists the token's catalogs and nothing else. */
    private fun listed(
        call: ApplicationCall,
        store: CatalogStore?,
    ): CatalogStore? = store?.let { StoreGrant.of(call, null, it)?.let { grant -> BoundCatalogStore(grant, it) } ?: it }

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
                    hogliteStore(
                        routing,
                        client(routing.backendId).forRequest(call.verifiedBackendToken),
                        operatorAuthority = call.hoglakeAuth.callerOrNull?.kind?.isOperator() != false,
                    )
                }
        }

    /**
     * Build a store from trusted placement. This method does not discover
     * catalogs or start background work. It checks no token, so it is not
     * part of the public surface: nothing in the server calls it.
     */
    internal fun forPlacement(placement: Routing): CatalogStore =
        when (placement.backend) {
            BackendKind.POSTGRES -> postgres ?: throw HoglakeException.NotFound("catalog '${placement.catalog}'")
            BackendKind.HOGLITE -> hogliteStore(placement, client(placement.backendId), operatorAuthority = true)
        }

    /** Share store construction without sharing request authority. */
    private fun hogliteStore(
        placement: Routing,
        client: HogliteClient,
        operatorAuthority: Boolean,
    ): HogliteCatalogStore =
        HogliteCatalogStore(
            client,
            HogliteTenantResolver { catalog ->
                if (catalog == placement.catalog) {
                    checkNotNull(placement.tenant)
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
                lookupAdmissions.computeIfAbsent(placement.backendId) {
                    HoglitePartitionLookupAdmission()
                },
            operatorAuthority = operatorAuthority,
            expiryPurgePage = cfg.expiryPurgePage,
            expiryPurgeBudgetMs = cfg.expiryPurgeBudgetMs,
        )

    private fun hogliteCompactionConfig(): CompactionConfig {
        val c = checkNotNull(cfg)
        return CompactionConfig(
            targetBytes = c.compactionTargetBytes,
            minInputFiles = c.compactionMinInputFiles,
            maxInputFiles = c.compactionMaxInputFiles,
            maxGroupsPerRun = c.compactionMaxGroupsPerRun,
            candidateHeadroom = c.compactionCandidateHeadroom,
            maxCandidates = c.compactionMaxCandidates,
            maxFanIn = c.compactionMaxFanIn,
            parallelGroups = c.compactionParallelGroups,
            inputOpenParallelism = c.compactionParallelInputOpens,
            claimsEnabled = c.compactionClaimsEnabled,
            claimTtlSeconds = c.compactionClaimTtlSeconds,
            nestedSortExpansion = c.compactionNestedSortExpansion,
            sortedHeapBytes = c.compactionSortedHeapBytes,
            spillBytes = c.compactionSpillBytes,
            spillDir = java.nio.file.Path.of(c.compactionSpillDir),
            verifyMinBytes = c.compactionVerifyMinBytes,
            maxNodesPerRow = c.compactionMaxNodesPerRow,
            codec =
                ParquetRewriter.OutputCodec.parse(
                    c.compactionCodec,
                    c.compactionZstdLevel,
                ),
            variantCompaction = c.compactionVariant,
            variantCompactionCatalogs = c.compactionVariantCatalogs,
        )
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
