package com.posthog.hoglake.store

import com.posthog.hoglake.Config
import com.posthog.hoglake.api.NamespaceDto
import com.posthog.hoglake.api.ScanFileDto
import com.posthog.hoglake.api.TableDto
import com.posthog.hoglake.auth.AuthPolicy
import com.posthog.hoglake.auth.Caller
import com.posthog.hoglake.auth.Jwt
import com.posthog.hoglake.auth.JwtViolation
import com.posthog.hoglake.auth.Routing
import com.posthog.hoglake.auth.Scope
import com.posthog.hoglake.auth.SessionProfile
import com.posthog.hoglake.auth.StoreGrant
import com.posthog.hoglake.auth.hoglakeAuth
import com.posthog.hoglake.auth.refuseIgnoredRouting
import com.posthog.hoglake.auth.remoteSchemasAmong
import com.posthog.hoglake.auth.remoteTeamSchemas
import com.posthog.hoglake.auth.requireCatalogEntitlement
import com.posthog.hoglake.auth.requireRemoteSchemaEntitlement
import com.posthog.hoglake.auth.requireServedNamespaceEntitlement
import com.posthog.hoglake.auth.verifiedBackendAuthority
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
import com.posthog.hoglake.hoglite.ScopedReadOutcome
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.HoglakeException
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
    /**
     * Client mode (plan row 1.3c): the remote schemas another host serves;
     * the default configures none. A server is a Host or a
     * Client (Config refuses both at boot).
     */
    private val mountHosts: MountHostResolver = MountHostResolver.NONE,
    /** The server owns this store. Compaction borrows it for one request. */
    private val objectStore: () -> ObjectStore = { error("compaction object store is not configured") },
    /** The server owns this store. Request-scoped cleanup backends only borrow it. */
    private val removalStore: () -> RemovalStore = { error("cleanup object store is not configured") },
    /** Host mode: the families of namespaces served to relayed readers (docs/auth-tenancy.md §6.5d). Empty is none. */
    servedFamilies: List<ServedFamily> = emptyList(),
    /** The reads of a served family's stream half ([ServedFamily.stream]), asked only once the caller rule passed. */
    private val streamRead: () -> MountReadService? = { null },
) : CatalogRegistry {
    private val servedTeams = servedFamilies.singleOrNull { it.family == TeamNamespaces.FAMILY }

    private val clients = ConcurrentHashMap<UUID, HogliteClient>()

    /** Select only the operator's exact tuple, without resolving a legacy store. */
    internal fun scopedNamespaceBinding(
        catalog: String,
        namespace: String,
    ): ScopedNamespaceBinding? =
        cfg?.scopedNamespaceBinding?.takeIf { it.catalog == catalog && it.namespace == namespace }

    /** Select only the operator's exact table tuple, without resolving a legacy store. */
    internal fun scopedTableBinding(
        catalog: String,
        namespace: String,
        table: String,
    ): ScopedNamespaceBinding? =
        cfg?.scopedNamespaceBinding?.takeIf {
            it.catalog == catalog && it.namespace == namespace && it.table == table && it.readSession != null
        }

    /** Terminal typed table read. No outcome selects another store or credential. */
    internal fun readScopedTable(
        call: ApplicationCall,
        binding: ScopedNamespaceBinding,
    ): TableDto {
        val table = binding.table ?: throw HoglakeException.NotFound("table")
        val caller = call.hoglakeAuth.callerOrNull
        val expected = Routing(binding.catalog, BackendKind.HOGLITE, binding.backendId, binding.tenant)
        if (binding != cfg?.scopedNamespaceBinding || binding.readSession == null ||
            caller?.kind != Caller.Kind.SCOPED || caller.routingIgnored ||
            caller.routing != expected || caller.catalogScopes.keys != setOf(binding.catalog) ||
            !caller.catalogClaimAllows(binding.catalog, Scope.READ)
        ) {
            throw HoglakeException.NotFound("table")
        }
        val authority = call.verifiedBackendAuthority ?: throw HoglakeException.NotFound("table")
        if (authority.readSessionBinding !== binding || authority.storageGrants == null ||
            authority.token.length > 64 * 1024
        ) {
            throw HoglakeException.NotFound("table")
        }
        val outcome =
            client(binding.backendId).forRequest(authority.token)
                .readScopedTable(binding.tenant, binding.catalog, binding.namespace, table)
        return when (outcome) {
            ScopedReadOutcome.NotFound -> throw HoglakeException.NotFound("table")
            ScopedReadOutcome.Forbidden -> throw HoglakeException.Forbidden("access denied")
            is ScopedReadOutcome.Found -> {
                val result = outcome.value
                val fields = checkNotNull(cfg).authPredicateFields.split(',').map { it.trim() }.toSet()
                val mark = result.containment
                if (mark.org != null || mark.field !in fields || mark.value <= 0) {
                    throw HoglakeException.BackendUnavailable("hoglite scoped table read unavailable")
                }
                if (!caller.mayUseContained(binding.catalog, mark.field, mark.value, null, Scope.READ)) {
                    throw HoglakeException.NotFound("table")
                }
                result.table
            }
        }
    }

    /** Terminal typed scan read. Every returned file path must stay inside the verified primary-catalog grant. */
    internal fun readScopedScan(
        call: ApplicationCall,
        binding: ScopedNamespaceBinding,
        snapshot: Long,
        tableUuid: UUID,
    ): List<ScanFileDto> {
        val table = binding.table ?: throw HoglakeException.NotFound("table")
        val caller = call.hoglakeAuth.callerOrNull
        val expected = Routing(binding.catalog, BackendKind.HOGLITE, binding.backendId, binding.tenant)
        if (binding != cfg?.scopedNamespaceBinding || binding.readSession == null ||
            caller?.kind != Caller.Kind.SCOPED || caller.routingIgnored ||
            caller.routing != expected || caller.catalogScopes.keys != setOf(binding.catalog) ||
            !caller.catalogClaimAllows(binding.catalog, Scope.READ)
        ) {
            throw HoglakeException.NotFound("table")
        }
        val authority = call.verifiedBackendAuthority ?: throw HoglakeException.NotFound("table")
        val grants = authority.storageGrants
        if (authority.readSessionBinding !== binding || grants == null || authority.token.length > 64 * 1024) {
            throw HoglakeException.NotFound("table")
        }
        val outcome =
            client(binding.backendId).forRequest(authority.token)
                .readScopedScan(binding.tenant, binding.catalog, binding.namespace, table, snapshot, tableUuid)
        return when (outcome) {
            ScopedReadOutcome.NotFound -> throw HoglakeException.NotFound("table")
            ScopedReadOutcome.Forbidden -> throw HoglakeException.Forbidden("access denied")
            is ScopedReadOutcome.Found -> {
                val files = outcome.value
                for (file in files) {
                    if (!grants.allowsPrimaryCatalogObject(binding.catalog, file.dataFile.path)) {
                        throw HoglakeException.Forbidden("access denied")
                    }
                    val dv = file.deleteFile
                    if (dv != null && !grants.allowsPrimaryCatalogObject(binding.catalog, dv.path)) {
                        throw HoglakeException.Forbidden("access denied")
                    }
                }
                files
            }
        }
    }

    /** Terminal typed read. No outcome can select another store or credential. */
    internal fun readScopedNamespace(
        call: ApplicationCall,
        binding: ScopedNamespaceBinding,
    ): NamespaceDto? {
        val caller = call.hoglakeAuth.callerOrNull
        val expected = Routing(binding.catalog, BackendKind.HOGLITE, binding.backendId, binding.tenant)
        if (binding != cfg?.scopedNamespaceBinding || caller?.kind != Caller.Kind.SCOPED || caller.routingIgnored ||
            caller.routing != expected || caller.catalogScopes.keys != setOf(binding.catalog) ||
            !caller.catalogClaimAllows(binding.catalog, Scope.READ)
        ) {
            throw HoglakeException.NotFound("namespace")
        }
        val authority = call.verifiedBackendAuthority ?: throw HoglakeException.NotFound("namespace")
        val token = authority.token
        if (token.length > 64 * 1024) throw HoglakeException.NotFound("namespace")
        if (authority.readSessionBinding != null) {
            if (authority.readSessionBinding !== binding) {
                throw HoglakeException.NotFound("namespace")
            }
        } else {
            // The ordinary namespace read does not forward embedded credentials.
            val claims =
                try {
                    Jwt.parse(token).claims
                } catch (_: JwtViolation) {
                    throw HoglakeException.NotFound("namespace")
                }
            if (SessionProfile.carried(claims) || claims.has("storage")) {
                throw HoglakeException.NotFound("namespace")
            }
        }
        val result =
            client(binding.backendId).forRequest(token)
                .readScopedNamespace(binding.tenant, binding.catalog, binding.namespace) ?: return null
        val held = result.containment
        val fields = checkNotNull(cfg).authPredicateFields.split(',').map { it.trim() }.toSet()
        if (result.name != binding.namespace || result.namespaceId?.let { it > 0 } != true ||
            result.remote != null || held == null || held.org != null || held.field !in fields || held.value <= 0
        ) {
            throw HoglakeException.BackendUnavailable("scoped namespace unavailable")
        }
        return result
    }

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
            is CatalogBinding.RemoteSchema -> throw remoteSchemaRefusal(call, binding.label)
            is CatalogBinding.Served -> throw servedNamespaceRefusal(call, binding.label)
        }

    /**
     * A served namespace resolves first, then verified placement, then the default
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
        if (call.verifiedBackendAuthority?.readSessionBinding != null) {
            throw HoglakeException.NotFound("namespace")
        }
        call.attributes.getOrNull(boundCatalog)?.let { (name, handed) -> if (name == catalog) return handed.binding }
        // Before any binding: a name resolves only when the caller's token allows it.
        call.requireCatalogEntitlement(catalog)
        // Before every other binding: a served namespace ignores the token's placement (`rtg`) too.
        val binding =
            servedBinding(call, catalog) ?: run {
                requirePlaced(call, catalog)
                val routing = call.hoglakeAuth.callerOrNull?.routing
                val routed = routing != null && routing.catalog == catalog
                val store = if (routed) routed(call, checkNotNull(routing)) else fallback(catalog)
                val hogliteVerified = routed && checkNotNull(routing).backend == BackendKind.HOGLITE
                pathSchema(call, catalog, store, hogliteVerified) ?: CatalogBinding.Local(store)
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
     * checks. The stream half, when the family has one, is built only from
     * the team the rule returned, after it returned.
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
        // The rule reads the namespace's mark from the store itself, only for a caller that passes its token checks.
        val team = call.requireServedNamespaceEntitlement(catalog, namespace, family, id, store)
        val stream =
            family.stream?.let {
                StreamHalf(
                    checkNotNull(streamRead()) { "a served family has a stream, but no stream reader is wired" },
                    it,
                    team,
                    family.predicateField,
                    call.hoglakeAuth.callerOrNull?.predicates?.get(family.predicateField),
                    family.label,
                )
            }
        return CatalogBinding.Served(
            catalog,
            namespace,
            ServedNamespaceReader(store, family.sourceCatalog, namespace, catalog, stream),
            family.label,
        )
    }

    /**
     * The remote schema this request's path names in [catalog], through
     * [resolve]; null, and nothing resolved, for every other request. A
     * route that relays before it parses anything asks this, so the token
     * is checked here for [catalog] as it is in [resolve].
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
        // is what remoteSchemaRefusal reads the route's class from.
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
     * the catalog must exist. The store proves it with one read, made only
     * for a caller the token check has passed, once per request (the binding
     * is cached); for a Hoglite placement ([hogliteVerified]) the verified
     * `rtg` proves it instead, because Hoglite refuses the caller's scoped
     * token on that read. A catalog nobody holds on PostgreSQL is still the
     * store's own 404. The entitlement check runs first, so nothing is sent
     * to the Host for a caller the token check refuses.
     */
    private fun pathSchema(
        call: ApplicationCall,
        catalog: String,
        store: CatalogStore,
        hogliteVerified: Boolean,
    ): CatalogBinding.RemoteSchema? {
        val (schema, namespace) = namedSchema(call, catalog) ?: return null
        val name = schema.match.schemaFor(namespace)
        call.requireRemoteSchemaEntitlement(catalog, name, namespace, schema.remoteNameFor(name), schema.name)
        if (!hogliteVerified) store.getCatalog(catalog)
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

    /** A hoglite listing must carry the token's single catalog as an explicit filter. */
    fun forListing(call: ApplicationCall): Listing {
        val caller = call.hoglakeAuth.callerOrNull
        val routing = caller?.routing
        if (caller?.routingIgnored == true) {
            // No placement, so no catalog of this instance to list.
            return Listing(listed(call, fallback("")), emptySet(), usePostgresTotals = true)
        }
        return if (routing != null) {
            Listing(
                listed(call, routed(call, routing)),
                setOf(routing.catalog),
                usePostgresTotals = routing.backend == BackendKind.POSTGRES,
            )
        } else {
            Listing(listed(call, fallback("")), caller?.visibleCatalogs(), usePostgresTotals = true)
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
        val store: CatalogStore?,
        val catalogs: Set<String>?,
        /** Only PostgreSQL catalogs can use the PostgreSQL sampler's name-keyed totals. */
        val usePostgresTotals: Boolean,
    )
}
