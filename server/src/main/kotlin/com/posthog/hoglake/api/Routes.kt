package com.posthog.hoglake.api

import com.posthog.hoglake.BuildInfo
import com.posthog.hoglake.auth.AuthState
import com.posthog.hoglake.auth.Caller
import com.posthog.hoglake.auth.Scope
import com.posthog.hoglake.auth.actorOrRefuse
import com.posthog.hoglake.auth.hoglakeAuth
import com.posthog.hoglake.auth.requireScopedNamespaces
import com.posthog.hoglake.auth.resolveActor
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.observability.CatalogTotals
import com.posthog.hoglake.observability.InstanceTotals
import com.posthog.hoglake.observability.instanceTotalsFor
import com.posthog.hoglake.persistence.FileRepo
import com.posthog.hoglake.service.ColumnTrees
import com.posthog.hoglake.service.Identifiers
import com.posthog.hoglake.service.ReservedNamespaces
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.store.CatalogBinding
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.CatalogStoreSelector
import com.posthog.hoglake.store.MountNames
import com.posthog.hoglake.store.MountReadService
import io.ktor.http.HttpStatusCode
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.plugins.BadRequestException
import io.ktor.server.request.receive
import io.ktor.server.request.userAgent
import io.ktor.server.response.respond
import io.ktor.server.routing.delete
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.put
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import java.time.Instant
import java.time.format.DateTimeParseException
import java.util.UUID

/** Install the default PostgreSQL-only API without extra backend configuration. */
fun Application.installApiRoutes(
    store: CatalogStore,
    instanceName: String = "",
    instanceTotals: () -> InstanceTotals? = { null },
    catalogTotals: () -> Map<String, CatalogTotals> = { emptyMap() },
    uiTheme: String = "",
) = installApiRoutes(CatalogStoreSelector(store), instanceName, instanceTotals, catalogTotals, uiTheme)

/** Install the default PostgreSQL-only scan route. */
fun Application.installScanRoutes(scan: CatalogStore) = installScanRoutes(CatalogStoreSelector(scan))

/**
 * The /v1 routes, one-to-one with openapi/hoglake.yaml. Handlers only
 * translate wire <-> model and pick status codes; behavior (locking,
 * validation, conflict detection) lives in CatalogService/CommitService,
 * whose HoglakeExceptions are mapped by ErrorMapping.
 */
fun Application.installApiRoutes(
    selector: CatalogStoreSelector,
    instanceName: String = "",
    instanceTotals: () -> InstanceTotals? = { null },
    catalogTotals: () -> Map<String, CatalogTotals> = { emptyMap() },
    uiTheme: String = "",
    /**
     * Serves a mount's five scoped read routes (plan row 1.3h). A provider,
     * not the instance, so a PG-free boot never forces it (it builds a
     * PostgreSQL-backed reader); it resolves to null where no mount can serve.
     */
    mountRead: () -> MountReadService? = { null },
    /** Names `POST .../namespaces` refuses for every caller (HOGLAKE_RESERVED_NAMESPACES). */
    reservedNamespaces: ReservedNamespaces = ReservedNamespaces.NONE,
) {
    // The handlers keep the receiver names `catalogs.` and `commits.`.
    // CommitConflictSpecParityTest and CatalogDropSpecParityTest count call
    // sites by these names. Both names refer to the one CatalogStore.
    routing {
        get("/v1/info") {
            // Totals come from the metrics sampler's last pass — never
            // computed here (a live-manifest sum per request would tax
            // the commit tail's RDS at fleet scale). Null (fields
            // omitted) only in the boot window before the first sample.
            //
            // The instance-wide sums are every tenant's storage, so they are
            // PROJECTED onto what this caller may see. Three cases, and the
            // table in auth/AuthPolicy.kt says why /v1/info is PUBLIC while
            // still caring:
            //   operator   -> the instance totals, unchanged
            //   tenant     -> the same accounting over its own catalogs
            //   anonymous  -> no totals at all (the fields are omitted)
            val totals =
                when (val auth = call.hoglakeAuth) {
                    // Auth off: unauthenticated by configuration, so the
                    // instance totals are what this route has always returned.
                    // The distinction matters — see auth/AuthState.
                    is AuthState.Off -> instanceTotals()
                    is AuthState.Anonymous -> null
                    // A scoped caller sees part of a catalog, so a catalog's
                    // totals would describe data it may not see.
                    is AuthState.Authenticated ->
                        if (auth.caller.kind == Caller.Kind.SCOPED) {
                            null
                        } else {
                            instanceTotalsFor(instanceTotals(), catalogTotals(), auth.caller.visibleCatalogs())
                        }
                }
            call.respond(
                InstanceInfoDto(
                    name = instanceName.ifBlank { null },
                    uiTheme = uiTheme.trim().ifBlank { null },
                    version = BuildInfo.version,
                    build = BuildInfo.buildStamp,
                    totalRows = totals?.totalRows,
                    totalSizeBytes = totals?.totalSizeBytes,
                    unsampledCatalogs = totals?.unsampledCatalogs,
                ),
            )
        }
        route("/v1/catalogs") {
            get {
                // Totals ride the sampler's last pass (catalogTotals), so
                // listing catalogs never sums the manifest per request.
                //
                // The LIST is filtered, not merely gated: without this a tenant
                // token reads the whole fleet's catalog roster off the first
                // request it makes. Under HOGLAKE_AUTH_MODE=off the caller is
                // null and the filter is "everything", i.e. today's behaviour.
                val listing = selector.forListing(call)
                val catalogs = listing.store
                // The sampler reads PostgreSQL only. Catalog names are not unique across backends.
                val totals = if (listing.usePostgresTotals) catalogTotals() else emptyMap()
                val visible =
                    when (val auth = call.hoglakeAuth) {
                        is AuthState.Off -> null
                        // Only reachable in `warn`, which lets a refusal through.
                        is AuthState.Anonymous -> emptySet()
                        is AuthState.Authenticated -> listing.catalogs
                    }
                // Totals are catalog-wide, so a scoped caller gets none.
                val scoped = (call.hoglakeAuth as? AuthState.Authenticated)?.caller?.kind == Caller.Kind.SCOPED
                // A catalog a mount shadows answers as the mount on every
                // route, so it is not listed. The caller's own mounts follow,
                // unsorted: re-sorting could reorder the backend's listing.
                val listed = catalogs?.listCatalogs(visible).orEmpty()
                val mounts = selector.mountsAmong(call, listed.map { it.name } + listing.mounts)
                val local =
                    listed.filterNot { it.name in mounts }.map { it.toDto(if (scoped) null else totals[it.name]) }
                val shown = listing.mounts.filter { it in mounts }
                // A mount Client asks each host once, with the caller's
                // token, and shows the entries the host returns for these
                // names as the host wrote them.
                val remote = if (shown.isEmpty()) null else selector.remoteCatalogs(call, shown)
                if (shown.isEmpty()) {
                    call.respond(local)
                } else if (remote != null) {
                    call.respond<List<Any>>(local + remote)
                } else {
                    val source = selector.mountSource()
                    call.respond(local + shown.map { source.toMountDto(it) })
                }
            }
            post {
                val req = call.receive<CreateCatalogRequestDto>()
                // Before placement picks a backend, so no backend can create
                // a catalog that shares a name with a mount.
                if (MountNames.isReserved(req.name)) {
                    throw HoglakeException.Validation("catalog name '${req.name}' is reserved for mounts")
                }
                val catalogs = selector.forCatalog(call, req.name)
                call.respond(
                    HttpStatusCode.Created,
                    catalogs.createCatalog(req.name, req.dataPath, call.actorOrRefuse(req.author), req.message).toDto(),
                )
            }

            route("/{catalog}") {
                get {
                    // The one route a mount serves so far (AuthPolicy.MOUNT_ROUTES).
                    when (val binding = selector.resolve(call, call.catalog())) {
                        is CatalogBinding.Local -> call.respond(binding.store.getCatalog(call.catalog()).toDto())
                        is CatalogBinding.Mount -> call.respond(selector.mountSource().toMountDto(binding.mount.name))
                        is CatalogBinding.Remote -> binding.host.catalog(call, binding.name)
                    }
                }
                delete {
                    val catalogs = selector.forCatalog(call, call.catalog())
                    call.respond(
                        catalogs.dropCatalog(
                            call.catalog(),
                            call.actorOrRefuse(call.request.queryParameters["author"]),
                            call.request.queryParameters["message"],
                        ).toDto(),
                    )
                }

                get("/storage-usage") {
                    val catalogs = selector.forCatalog(call, call.catalog())
                    // A footprint has no read snapshot: historical/dropped registrations
                    // remain tracked until their manifest rows are physically removed.
                    if (call.request.queryParameters.contains("snapshot") ||
                        call.request.queryParameters.contains("at_timestamp")
                    ) {
                        throw BadRequestException("storage-usage does not accept snapshot or at_timestamp")
                    }
                    call.respond(catalogs.storageUsage(call.catalog()).toDto())
                }

                route("/namespaces") {
                    get {
                        val catalog = call.catalog()
                        when (val binding = selector.resolve(call, catalog)) {
                            // A mount lists only the pinned table's namespace (plan row 1.3h).
                            is CatalogBinding.Mount ->
                                call.respond(
                                    checkNotNull(mountRead()).namespaces(
                                        call.longQuery("snapshot"),
                                        call.instantQuery("at_timestamp"),
                                    ).map { it.toDto() },
                                )
                            is CatalogBinding.Remote -> binding.host.namespaces(call, binding.name)
                            is CatalogBinding.Local -> {
                                val catalogs = binding.store
                                // A scoped caller sees only the contained
                                // namespaces it may use (AuthPolicy.SCOPED_ROUTES);
                                // everyone else, everything.
                                val scoped = call.hoglakeAuth.callerOrNull?.takeIf { it.kind == Caller.Kind.SCOPED }
                                call.respond(
                                    catalogs.listNamespaces(
                                        catalog,
                                        call.longQuery("snapshot"),
                                        call.instantQuery("at_timestamp"),
                                    ).filter { ns ->
                                        scoped == null ||
                                            ns.containment?.let {
                                                scoped.mayUseContained(catalog, it.field, it.value, Scope.READ)
                                            } == true
                                    }.map { it.toDto() },
                                )
                            }
                        }
                    }
                    post {
                        val catalogs = selector.forCatalog(call, call.catalog())
                        val req = call.receive<CreateNamespaceRequestDto>()
                        // Before the store: the only path that creates a namespace, on every backend.
                        reservedNamespaces.refuse(req.name)
                        call.respond(
                            HttpStatusCode.Created,
                            catalogs.createNamespace(
                                call.catalog(),
                                req.name,
                                call.actorOrRefuse(req.author),
                                req.message,
                            ).toDto(),
                        )
                    }
                    get("/{namespace}") {
                        when (val binding = selector.resolve(call, call.catalog())) {
                            is CatalogBinding.Mount ->
                                call.respond(
                                    checkNotNull(mountRead()).namespace(
                                        call.namespace(),
                                        call.longQuery("snapshot"),
                                        call.instantQuery("at_timestamp"),
                                    ).toDto(),
                                )
                            is CatalogBinding.Remote -> binding.host.namespace(call, binding.name, call.namespace())
                            is CatalogBinding.Local ->
                                call.respond(binding.store.getNamespace(call.catalog(), call.namespace()).toDto())
                        }
                    }
                    delete("/{namespace}") {
                        val catalogs = selector.forCatalog(call, call.catalog())
                        call.respond(
                            catalogs.dropNamespace(
                                call.catalog(),
                                call.namespace(),
                                call.longQuery("expected_namespace_id"),
                                call.actorOrRefuse(call.request.queryParameters["author"]),
                                call.request.queryParameters["message"],
                                // Auth off, or an operator: a contained
                                // namespace may be dropped. A tenant may not.
                                mayDropContained = call.hoglakeAuth.callerOrNull?.kind?.isOperator() ?: true,
                            ).toDto(),
                        )
                    }
                    // Containment (V33): operator-only in AuthPolicy.
                    put("/{namespace}/containment") {
                        val catalogs = selector.forCatalog(call, call.catalog())
                        val req = call.receive<NamespaceContainmentDto>()
                        call.respond(
                            catalogs.setNamespaceContainment(
                                call.catalog(),
                                call.namespace(),
                                NamespaceContainment(req.field, req.value),
                                call.actorOrRefuse(null),
                            ).toDto(),
                        )
                    }
                    delete("/{namespace}/containment") {
                        val catalogs = selector.forCatalog(call, call.catalog())
                        call.respond(
                            catalogs.setNamespaceContainment(
                                call.catalog(),
                                call.namespace(),
                                null,
                                call.actorOrRefuse(null),
                            ).toDto(),
                        )
                    }
                    route("/{namespace}/tables") {
                        get {
                            when (val binding = selector.resolve(call, call.catalog())) {
                                // The pinned table only, with SCOPED totals (plan row 1.3h).
                                is CatalogBinding.Mount ->
                                    call.respond(
                                        checkNotNull(mountRead()).tables(
                                            call.hoglakeAuth.callerOrNull,
                                            binding.mount,
                                            call.namespace(),
                                            call.longQuery("snapshot"),
                                            call.instantQuery("at_timestamp"),
                                        ).map { it.toDto() },
                                    )
                                is CatalogBinding.Remote -> binding.host.tables(call, binding.name, call.namespace())
                                is CatalogBinding.Local ->
                                    call.respond(
                                        binding.store.listTables(
                                            call.catalog(),
                                            call.namespace(),
                                            call.longQuery("snapshot"),
                                            call.instantQuery("at_timestamp"),
                                        ).map { it.toDto() },
                                    )
                            }
                        }
                        post {
                            val catalogs = selector.forCatalog(call, call.catalog())
                            val req = call.receive<CreateTableRequestDto>()
                            call.respond(
                                HttpStatusCode.Created,
                                catalogs.createTable(
                                    call.catalog(),
                                    call.namespace(),
                                    req.name,
                                    req.columns.map { it.toModel() },
                                    call.actorOrRefuse(req.author),
                                    req.message,
                                ).toDto(),
                            )
                        }

                        route("/{table}") {
                            get {
                                when (val binding = selector.resolve(call, call.catalog())) {
                                    // A mount serves the pinned table only, totals omitted (plan row 1.3h).
                                    is CatalogBinding.Mount ->
                                        call.respond(
                                            checkNotNull(mountRead()).table(
                                                call.namespace(),
                                                call.table(),
                                                call.longQuery("snapshot"),
                                                call.instantQuery("at_timestamp"),
                                            ).toDto(),
                                        )
                                    is CatalogBinding.Remote ->
                                        binding.host.table(call, binding.name, call.namespace(), call.table())
                                    is CatalogBinding.Local ->
                                        call.respond(
                                            binding.store.getTable(
                                                call.catalog(),
                                                call.namespace(),
                                                call.table(),
                                                call.longQuery("snapshot"),
                                                call.instantQuery("at_timestamp"),
                                                // Defaults TRUE: the field set a
                                                // client already gets does not
                                                // shrink because a parameter was
                                                // added. `totals=false` is the
                                                // identity read (#232), which no
                                                // client in this tree sends yet —
                                                // see CatalogService.getTable.
                                                totals = call.boolQuery("totals") ?: true,
                                            ).toDto(),
                                        )
                                }
                            }
                            delete {
                                val catalogs = selector.forCatalog(call, call.catalog())
                                call.respond(
                                    catalogs.dropTable(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                        call.expectedTableUuid(),
                                        call.actorOrRefuse(call.request.queryParameters["author"]),
                                        call.request.queryParameters["message"],
                                    ).toDto(),
                                )
                            }
                            post("/truncate") {
                                val catalogs = selector.forCatalog(call, call.catalog())
                                val expected =
                                    call.expectedTableUuid()
                                        ?: throw BadRequestException("expected_table_uuid is required")
                                val body = call.receive<ByteArray>()
                                val req =
                                    if (body.isEmpty()) {
                                        OperationMetadataDto()
                                    } else {
                                        try {
                                            com.posthog.hoglake.wireObjectMapper().readValue(
                                                body,
                                                OperationMetadataDto::class.java,
                                            ) ?: throw BadRequestException("request body must be a JSON object")
                                        } catch (e: Exception) {
                                            throw BadRequestException("invalid operation metadata JSON", e)
                                        }
                                    }
                                call.respond(
                                    catalogs.truncateTable(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                        expected,
                                        call.actorOrRefuse(req.author),
                                        req.message,
                                    ).toDto(),
                                )
                            }
                            get("/files") {
                                val catalogs = selector.forCatalog(call, call.catalog())
                                val sort =
                                    call.request.queryParameters["sort"]?.let {
                                        FileRepo.FileSortColumn.fromWire(it)
                                            ?: throw BadRequestException(
                                                "query parameter 'sort' must be one of " +
                                                    FileRepo.FileSortColumn.entries.joinToString(", ") { c -> c.wire } +
                                                    ", got '$it'",
                                            )
                                    }
                                val desc =
                                    when (val o = call.request.queryParameters["order"]) {
                                        null, "asc" -> false
                                        "desc" -> true
                                        else ->
                                            throw BadRequestException(
                                                "query parameter 'order' must be 'asc' or 'desc', got '$o'",
                                            )
                                    }
                                // No 'limit' -> unbounded, the historical
                                // behavior every non-webui caller and the
                                // existing tests rely on. The webui always
                                // sends one.
                                val limit =
                                    call.intQuery("limit")?.also {
                                        if (it < 1) {
                                            throw BadRequestException(
                                                "query parameter 'limit' must be positive, got '$it'",
                                            )
                                        }
                                    }?.coerceAtMost(FILES_MAX_LIMIT)
                                val offset =
                                    call.intQuery("offset")?.also {
                                        if (it < 0) {
                                            throw BadRequestException(
                                                "query parameter 'offset' must be >= 0, got '$it'",
                                            )
                                        }
                                    } ?: 0
                                // partition=key_index:value, repeatable. The
                                // value is the stored, transformed string the
                                // caller got from /partitions/values, so the
                                // match is plain string equality — the server
                                // never encodes. Multiple params AND together.
                                val partitionFilter =
                                    call.request.queryParameters.getAll("partition")
                                        ?.associate { param ->
                                            val (k, v) =
                                                param.split(":", limit = 2).also {
                                                    if (it.size != 2) {
                                                        throw BadRequestException(
                                                            "query parameter 'partition' must be " +
                                                                "key_index:value, got '$param'",
                                                        )
                                                    }
                                                }
                                            val keyIndex =
                                                k.toIntOrNull()
                                                    ?: throw BadRequestException(
                                                        "partition key_index must be a non-negative integer, got '$k'",
                                                    )
                                            if (keyIndex < 0) {
                                                throw BadRequestException(
                                                    "partition key_index must be a non-negative integer, got '$k'",
                                                )
                                            }
                                            keyIndex to v
                                        } ?: emptyMap()
                                call.respond(
                                    catalogs.listFiles(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                        call.longQuery("snapshot"),
                                        call.instantQuery("at_timestamp"),
                                        sort = sort,
                                        desc = desc,
                                        limit = limit,
                                        offset = offset,
                                        partitionFilter = partitionFilter,
                                    ).map { it.toDto() },
                                )
                            }
                            get("/partitions/values") {
                                val catalogs = selector.forCatalog(call, call.catalog())
                                call.respond(
                                    catalogs.partitionValues(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                        call.longQuery("snapshot"),
                                        call.instantQuery("at_timestamp"),
                                    ).toDto(),
                                )
                            }
                            get("/files/{fileId}/stats") {
                                val catalogs = selector.forCatalog(call, call.catalog())
                                call.respond(
                                    catalogs.fileStats(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                        call.longPath("fileId"),
                                        call.longQuery("snapshot"),
                                        call.instantQuery("at_timestamp"),
                                    ).toDto(),
                                )
                            }
                            get("/changes") {
                                val catalogs = selector.forCatalog(call, call.catalog())
                                val from =
                                    call.longQuery("from_snapshot")
                                        ?: throw BadRequestException(
                                            "missing required query parameter 'from_snapshot'",
                                        )
                                call.respond(
                                    catalogs.changes(
                                        call.catalog(),
                                        call.namespace(),
                                        call.table(),
                                        from,
                                        call.longQuery("to_snapshot"),
                                    ).toDto(),
                                )
                            }
                        }
                    }
                }

                get("/snapshots/resolve") {
                    val catalogs = selector.forCatalog(call, call.catalog())
                    val at =
                        call.instantQuery("at_timestamp")
                            ?: throw BadRequestException("query parameter 'at_timestamp' is required")
                    call.respond(catalogs.resolveSnapshot(call.catalog(), at).toDto())
                }

                get("/snapshots") {
                    val catalogs = selector.forCatalog(call, call.catalog())
                    val after = call.longQuery("after") ?: 0L
                    val before = call.longQuery("before")
                    val limit = (call.intQuery("limit") ?: 1000).coerceAtMost(10_000)
                    val (page, hasMore) =
                        catalogs.listSnapshots(call.catalog(), after, limit, before)
                    call.respond(SnapshotPageDto(page.map { it.toDto() }, hasMore))
                }

                // Dedicated endpoint prevents old servers silently ignoring the
                // new request key and degrading retries to at-least-once.
                post("/commit/prepared") {
                    val commits = selector.forCatalog(call, call.catalog())
                    val req = call.receiveCommit()
                    if (req.idempotencyKey == null) {
                        throw com.posthog.hoglake.model.HoglakeException.Validation("idempotency_key is required")
                    }
                    call.respond(
                        commits.commit(call.catalog(), req.toModel(), call.request.userAgent()).toDto(),
                    )
                }

                // Ends whole live files and appends in ONE snapshot. A dedicated
                // endpoint, so an older server answers 404 instead of ignoring
                // `file_ends` and publishing only the appends (a doubled day).
                post("/commit/replacements/prepared") {
                    val commits = selector.forCatalog(call, call.catalog())
                    val req = call.receiveReplacement()
                    if (req.deletes.isNotEmpty() || req.requireUnchangedTables == true) {
                        throw BadRequestException(
                            "a replacement cannot carry deletes or require_unchanged_tables",
                        )
                    }
                    if (req.idempotencyKey == null || req.readSnapshot == null || req.fileEnds.isNullOrEmpty()) {
                        throw com.posthog.hoglake.model.HoglakeException.Validation(
                            "a replacement requires idempotency_key, read_snapshot and file_ends",
                        )
                    }
                    call.respond(
                        commits.commit(call.catalog(), req.toModel(), call.request.userAgent()).toDto(),
                    )
                }

                // Distinct endpoint fences replicas that predate the DELETE contract.
                post("/commit/deletes/prepared") {
                    val commits = selector.forCatalog(call, call.catalog())
                    val req = call.receiveCommit()
                    if (req.idempotencyKey == null || req.readSnapshot == null || req.appends.isNotEmpty() ||
                        req.deletes.isEmpty() || req.deletes.any { it.expectedTableUuid == null }
                    ) {
                        throw com.posthog.hoglake.model.HoglakeException.Validation(
                            "prepared DELETE requires idempotency_key, read_snapshot, guarded deletes and no appends",
                        )
                    }
                    call.respond(
                        commits.commit(
                            call.catalog(),
                            req.toModel(allowEmptyDeletes = true),
                            call.request.userAgent(),
                        ).toDto(),
                    )
                }

                // A separate endpoint fences old replicas and includes the stronger
                // conflict contract in the durable full-payload receipt.
                post("/commit/mutations/prepared") {
                    val commits = selector.forCatalog(call, call.catalog())
                    val req = call.receiveCommit()
                    if (req.idempotencyKey == null || req.readSnapshot == null || req.deletes.isEmpty() ||
                        req.deletes.any { it.expectedTableUuid == null } ||
                        req.appends.any { it.expectedTableUuid == null }
                    ) {
                        throw com.posthog.hoglake.model.HoglakeException.Validation(
                            "prepared mutation requires idempotency_key, read_snapshot and guarded target tables",
                        )
                    }
                    call.respond(
                        commits.commit(
                            call.catalog(),
                            req.toModel(allowEmptyDeletes = true).copy(requireUnchangedTables = true),
                            call.request.userAgent(),
                        ).toDto(),
                    )
                }

                post("/commit/uploads") {
                    val commits = selector.forCatalog(call, call.catalog())
                    val req = call.receiveCommit()
                    if (req.idempotencyKey == null || req.readSnapshot == null ||
                        req.appends.any { it.expectedTableUuid == null } ||
                        req.deletes.any { it.expectedTableUuid == null }
                    ) {
                        throw com.posthog.hoglake.model.HoglakeException.Validation(
                            "claimed uploads require a guarded idempotent commit",
                        )
                    }
                    call.respond(
                        commits.commit(
                            call.catalog(),
                            req.toModel(allowEmptyDeletes = true)
                                .copy(requireUnchangedTables = req.deletes.isNotEmpty()),
                            call.request.userAgent(),
                        ).toDto(),
                    )
                }

                // requireUnchangedTables is unconditional here because the
                // transaction contract demands it, but it binds only the
                // DELETE targets (CommitService.checkConflicts): an
                // append-only transaction keeps the ordinary DDL-only
                // conflict rule and does not 409 on a concurrent INSERT.
                post("/commit/transaction") {
                    val commits = selector.forCatalog(call, call.catalog())
                    val req = call.receiveCommit()
                    if (req.idempotencyKey == null || req.readSnapshot == null ||
                        req.appends.any { it.expectedTableUuid == null } ||
                        req.deletes.any { it.expectedTableUuid == null }
                    ) {
                        throw com.posthog.hoglake.model.HoglakeException.Validation(
                            "transaction requires idempotency_key, read_snapshot and guarded targets",
                        )
                    }
                    call.respond(
                        commits.commit(
                            call.catalog(),
                            req.toModel(allowEmptyDeletes = true)
                                .copy(requireUnchangedTables = true, allowPendingDeletes = true),
                            call.request.userAgent(),
                        ).toDto(),
                    )
                }

                get("/commit/receipts/{operation}") {
                    val commits = selector.forCatalog(call, call.catalog())
                    val operation =
                        try {
                            UUID.fromString(call.parameters["operation"])
                        } catch (_: IllegalArgumentException) {
                            throw BadRequestException("invalid operation UUID")
                        }
                    val receipt = commits.receipt(call.catalog(), operation)
                    call.respond(
                        CommitReceiptDto(operation, receipt.snapshotId, receipt.schemaVersion, receipt.endedFiles),
                    )
                }

                post("/commit") {
                    val commits = selector.forCatalog(call, call.catalog())
                    val req = call.receiveCommit()
                    call.respond(
                        commits.commit(call.catalog(), req.toModel(), call.request.userAgent()).toDto(),
                    )
                }

                get("/consumers") {
                    val catalogs = selector.forCatalog(call, call.catalog())
                    call.respond(
                        catalogs.listConsumers(call.catalog()).toConsumerListDto(),
                    )
                }

                route("/consumers/{consumer}/offsets") {
                    get {
                        val catalogs = selector.forCatalog(call, call.catalog())
                        call.respond(
                            catalogs.listOffsets(call.catalog(), call.consumer())
                                .map { it.toDto() },
                        )
                    }
                    get("/{tableUuid}") {
                        val catalogs = selector.forCatalog(call, call.catalog())
                        call.respond(
                            catalogs.getOffset(
                                call.catalog(),
                                call.consumer(),
                                call.uuidPath("tableUuid"),
                            ).toDto(),
                        )
                    }
                    put("/{tableUuid}") {
                        val catalogs = selector.forCatalog(call, call.catalog())
                        val tableUuid = call.uuidPath("tableUuid")
                        val req = call.receive<CommitOffsetRequestDto>()
                        call.respond(
                            catalogs.commitOffset(
                                call.catalog(),
                                call.consumer(),
                                tableUuid,
                                req.snapshotId,
                            ).toDto(),
                        )
                    }
                }
            }
        }
    }
}

/**
 * GET .../tables/{table}/scan?snapshot= — read planning (openapi
 * planScan): data files paired with their visible deletion vectors, and
 * with `include=column_stats` each provided file's column statistics
 * (narrowed by `stats_fields`), with `include=split_offsets` each file's
 * stored row-group start offsets. Installed separately so App.kt wires
 * it with its own CatalogStore.
 */
fun Application.installScanRoutes(
    selector: CatalogStoreSelector,
    mountRead: () -> MountReadService? = { null },
) {
    routing {
        get("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/scan") {
            // getAll, joined: Parameters[...] is only the FIRST occurrence,
            // so a repeated `stats_fields=3&stats_fields=7` would silently
            // drop field 7's bounds and a second, misspelt `include` would
            // be ignored rather than refused.
            fun joined(name: String) = call.request.queryParameters.getAll(name)?.joinToString(",")
            // A mount Client relays the request as it came (plan row 1.3c):
            // the Host parses it, plans the scoped scan and applies its caps.
            selector.remote(call, call.catalog())?.let { remote ->
                return@get remote.host.scan(call, remote.name, call.namespace(), call.table())
            }
            val request = parseScanRequest(joined("include"), joined("stats_fields"))
            val files =
                when (val binding = selector.resolve(call, call.catalog())) {
                    is CatalogBinding.Remote -> error("a remote binding is relayed before the scan is parsed")
                    is CatalogBinding.Local ->
                        binding.store.planScan(
                            call.catalog(),
                            call.namespace(),
                            call.table(),
                            call.longQuery("snapshot"),
                            call.instantQuery("at_timestamp"),
                            columnStats = request.columnStats,
                            splitOffsets = request.splitOffsets,
                        )
                    // The export's scoped scan (plan row 1.3h): ReadScope in
                    // SQL, each file its team id, under the shared cap, the
                    // file budget and the deadline.
                    is CatalogBinding.Mount ->
                        checkNotNull(mountRead()).scan(
                            call.hoglakeAuth.callerOrNull,
                            binding.mount,
                            call.namespace(),
                            call.table(),
                            call.longQuery("snapshot"),
                            call.instantQuery("at_timestamp"),
                            columnStats = request.columnStats,
                            splitOffsets = request.splitOffsets,
                        )
                }
            call.respond(files.map { it.toDto() })
        }
    }
}

/**
 * The `include` values GET .../scan understands. Each names one optional
 * PART of the plan, and each part is a property that ONLY the scan plan
 * carries (the files listing and the changefeed never do):
 * `column_stats` attaches DataFile.column_stats, `split_offsets`
 * attaches DataFile.split_offsets.
 */
internal val SCAN_INCLUDES = setOf("column_stats", "split_offsets")

/**
 * The optional parts one GET .../scan asked for: column statistics
 * (null when not asked) and row-group split offsets.
 */
internal data class ScanRequest(
    val columnStats: ScanService.ColumnStatsRequest?,
    val splitOffsets: Boolean,
)

/**
 * Most DISTINCT values `include` may name.
 *
 * The parameter names optional PARTS of the plan and there are two of
 * them, so sixteen is already ample headroom. The
 * cap exists because the unknown-value refusal ECHOES what it did not
 * recognise: without a bound, sixteen hundred misspellings produce
 * sixteen hundred quoted tokens in a 422 body. [Identifiers.cap]
 * bounds each token's LENGTH; this bounds their COUNT, and a message
 * needs both.
 *
 * DISTINCT is the whole of it. `explode: true` makes repetition the
 * CANONICAL encoding of this parameter — `include=column_stats&include=column_stats`
 * is what a generated client emits for a two-element list, and the
 * route joins occurrences before the parse — so a cap counting raw
 * tokens refuses a request that names one legal value seventeen times.
 * The count that can hurt a reader is how many DIFFERENT values come
 * back in the message, which is what this bounds. [MAX_RAW_SCAN_TOKENS]
 * is the separate, generous guard on the work done BEFORE the dedupe.
 */
internal const val MAX_SCAN_INCLUDES = 16

/**
 * Most field ids `stats_fields` may name.
 *
 * [ColumnTrees.MAX_COLUMN_NODES] itself, not a copy of its value: a
 * request may legitimately name every column node a table is allowed to
 * have, and nothing beyond that can resolve to anything. Restating the
 * number would let the two drift apart silently, with the parser
 * refusing ids the DDL had just allowed.
 */
internal const val MAX_STATS_FIELDS = ColumnTrees.MAX_COLUMN_NODES

/**
 * The raw-token guard both parameters share, applied BEFORE the
 * dedupe: ten times the larger of the two distinct-value caps.
 *
 * The caps above are on DISTINCT values, which is the number that
 * reaches a message or a query, and a distinct count cannot be taken
 * without building the set first. This bounds that work. Ten times, and
 * not the cap itself, because repetition is the canonical encoding
 * under `explode: true`: a client sending each of ten thousand field
 * ids as its own occurrence is well behaved, and even a client that
 * sends each of them twice is only careless. A hundred thousand tokens
 * is nobody's list.
 */
internal const val MAX_RAW_SCAN_TOKENS = 10 * ColumnTrees.MAX_COLUMN_NODES

/**
 * `include` and `stats_fields` as a scan's stats request, or null when
 * none was asked for.
 *
 * THE LINE BETWEEN 400 AND 422 is whether the VALUE is well formed.
 * Malformed values are 400s (BadRequestException) on both parameters
 * and for the same reasons: an empty token, a non-integer field id, too
 * many of either. Well-formed but unusable ones are 422s (Validation):
 * an `include` value the server does not know — refused rather than
 * ignored, so a caller misspelling it cannot mistake "no stats" for
 * "nothing to prune" — and `stats_fields` without `include=column_stats`,
 * which would otherwise silently return no statistics at all.
 *
 * `include=` and `stats_fields=` therefore answer the SAME status. They
 * did not: an empty `include` used to reach the unknown-value arm and
 * answer 422 while an empty `stats_fields` answered 400, so one caller
 * sending both empty got two different verdicts on one mistake.
 *
 * Both parameters UNION across repeated occurrences (the route joins
 * them with commas before calling this), so `stats_fields=3&stats_fields=7`
 * asks for both columns rather than silently dropping the second.
 */
internal fun parseScanStatsRequest(
    include: String?,
    statsFields: String?,
): ScanService.ColumnStatsRequest? = parseScanRequest(include, statsFields).columnStats

/**
 * `include` and `stats_fields` as the whole of a scan's optional parts:
 * [parseScanStatsRequest]'s rules for the statistics, plus whether
 * `split_offsets` was named. The two values combine freely
 * (`include=column_stats,split_offsets`); `stats_fields` narrows the
 * statistics only, so it still requires `column_stats` and naming only
 * `split_offsets` beside it is the same 422.
 */
internal fun parseScanRequest(
    include: String?,
    statsFields: String?,
): ScanRequest {
    val includes = include?.let { parseScanIncludes(it) } ?: emptySet()
    val fieldIds = statsFields?.let { parseStatsFields(it) }
    val splitOffsets = "split_offsets" in includes
    if ("column_stats" !in includes) {
        if (fieldIds != null) {
            throw HoglakeException.Validation("stats_fields requires include=column_stats")
        }
        return ScanRequest(columnStats = null, splitOffsets = splitOffsets)
    }
    return ScanRequest(ScanService.ColumnStatsRequest(fieldIds), splitOffsets)
}

/**
 * Split one of the two parameters into raw tokens, bounded by
 * [MAX_RAW_SCAN_TOKENS].
 *
 * The guard is on the SPLIT, before anything is trimmed or deduped,
 * because that is the only work whose size the caller controls
 * directly. Everything after it counts distinct values.
 */
private fun scanTokens(
    name: String,
    raw: String,
): List<String> {
    val parts = raw.split(',')
    if (parts.size > MAX_RAW_SCAN_TOKENS) {
        throw BadRequestException(
            "query parameter '$name' carries more than $MAX_RAW_SCAN_TOKENS values",
        )
    }
    return parts
}

/** `include`: a comma-separated list of non-empty known part names. */
internal fun parseScanIncludes(raw: String): Set<String> {
    val values = scanTokens("include", raw).map { it.trim() }.toSet()
    if (values.any { it.isEmpty() }) {
        throw BadRequestException(
            "query parameter 'include' must be a comma-separated list of non-empty values; " +
                "supported: ${SCAN_INCLUDES.sorted().joinToString()}",
        )
    }
    // DISTINCT values, after the dedupe: under `explode: true` a client
    // repeating one legal value is sending a list of one, and refusing
    // it would refuse the parameter's own canonical encoding.
    if (values.size > MAX_SCAN_INCLUDES) {
        throw BadRequestException("query parameter 'include' names more than $MAX_SCAN_INCLUDES distinct values")
    }
    val unknown = values - SCAN_INCLUDES
    if (unknown.isNotEmpty()) {
        throw HoglakeException.Validation(
            "include: unknown value(s) ${unknown.sorted().joinToString { "'${Identifiers.cap(it)}'" }}; " +
                "supported: ${SCAN_INCLUDES.sorted().joinToString()}",
        )
    }
    return values
}

/** `stats_fields`: a non-empty comma-separated list of int64 field ids. */
internal fun parseStatsFields(raw: String): Set<Long> {
    val ids =
        scanTokens("stats_fields", raw).map { part ->
            part.trim().toLongOrNull()
                ?: throw BadRequestException(
                    "query parameter 'stats_fields' must be a comma-separated list of integer field ids",
                )
        }.toSet()
    // Again DISTINCT: the cap is [ColumnTrees.MAX_COLUMN_NODES] because
    // that is how many field ids a legal table can HAVE, and a caller
    // naming one id ten thousand times has named one column.
    if (ids.size > MAX_STATS_FIELDS) {
        throw BadRequestException(
            "query parameter 'stats_fields' names more than $MAX_STATS_FIELDS distinct field ids",
        )
    }
    return ids
}

// ---- parameter helpers ---------------------------------------------------
// internal (not private): ViewRoutes.kt shares them.

internal fun ApplicationCall.pathParam(name: String): String =
    parameters[name] ?: throw BadRequestException("missing path parameter '$name'")

internal fun ApplicationCall.catalog() = pathParam("catalog")

internal fun ApplicationCall.namespace() = pathParam("namespace")

internal fun ApplicationCall.table() = pathParam("table")

private fun ApplicationCall.consumer() = pathParam("consumer")

private fun ApplicationCall.longPath(name: String): Long {
    val raw = pathParam(name)
    return raw.toLongOrNull()
        ?: throw BadRequestException("path parameter '$name' must be an integer, got '$raw'")
}

private fun ApplicationCall.uuidPath(name: String): UUID {
    val raw = pathParam(name)
    return try {
        UUID.fromString(raw)
    } catch (_: IllegalArgumentException) {
        throw BadRequestException("path parameter '$name' must be a UUID, got '$raw'")
    }
}

/** Hard cap on a single file page, matching the snapshots endpoint's cap. */
private const val FILES_MAX_LIMIT = 10_000

internal fun ApplicationCall.longQuery(name: String): Long? =
    request.queryParameters[name]?.let { parseLongQuery(name, it) }

internal fun parseLongQuery(
    name: String,
    raw: String,
): Long = raw.toLongOrNull() ?: throw BadRequestException("query parameter '$name' must be an integer")

/**
 * Boolean query parameter: exactly `true` or `false`, anything else a
 * 400. Null when absent, so a caller can tell "not asked" from "asked
 * for false" and supply its own default.
 *
 * STRICT ON PURPOSE. `toBooleanStrictOrNull` rather than
 * `toBoolean`, which maps every unrecognized string — `"0"`, `"no"`,
 * `"False "`, a typo — to FALSE. On `?totals=` that would silently
 * suppress the totals for a caller that asked for something else, and
 * the caller would read the absent fields as "not yet sampled" and
 * believe the server.
 */
internal fun ApplicationCall.boolQuery(name: String): Boolean? =
    request.queryParameters[name]?.let {
        it.toBooleanStrictOrNull()
            ?: throw BadRequestException("query parameter '$name' must be 'true' or 'false', got '$it'")
    }

internal fun ApplicationCall.intQuery(name: String): Int? =
    request.queryParameters[name]?.let {
        it.toIntOrNull()
            ?: throw BadRequestException("query parameter '$name' must be an integer, got '$it'")
    }

/** ISO-8601 instant query parameter (e.g. 2026-09-04T12:00:00Z); unparseable -> 400. */
private fun ApplicationCall.instantQuery(name: String): Instant? =
    request.queryParameters[name]?.let {
        try {
            Instant.parse(it)
        } catch (_: DateTimeParseException) {
            throw BadRequestException(
                "query parameter '$name' must be an ISO-8601 instant, got '$it'",
            )
        }
    }

/** Optional on legacy DDL endpoints; mandatory for truncate. */
internal fun ApplicationCall.expectedTableUuid(): UUID? {
    val raw = request.queryParameters["expected_table_uuid"] ?: return null
    return parseExpectedTableUuid(raw)
}

internal fun parseExpectedTableUuid(raw: String): UUID =
    try {
        UUID.fromString(raw)
    } catch (_: IllegalArgumentException) {
        throw BadRequestException("expected_table_uuid must be a UUID")
    }

/**
 * Every commit route reads its body through here, so the snapshot's `author`
 * is decided by the same rule as every other write ([resolveActor]). Under
 * auth, a tenant's commit is recorded as its own subject, or `<user> via
 * <subject>` for a delegating client, and naming anyone else is refused. A
 * commit route that read the body directly would take its `author` as given:
 * `CommitAttributionTest` fails if one does.
 *
 * A scoped caller is held here to the namespaces the body names, before the
 * commit runs and before a receipt can replay (ScopedWrites.kt).
 */
internal suspend fun ApplicationCall.receiveCommit(): CommitRequestDto =
    receive<CommitRequestDto>().attributed(hoglakeAuth).also { req ->
        requireScopedNamespaces(catalog(), req.appends.map { it.namespace } + req.deletes.map { it.namespace })
    }

/**
 * A replacement's body, attributed and held to a scoped caller's namespaces:
 * the namespaces of its appends AND of the tables whose files it ends.
 */
internal suspend fun ApplicationCall.receiveReplacement(): ReplacementRequestDto =
    receive<ReplacementRequestDto>().let { it.copy(author = resolveActor(hoglakeAuth, it.author)) }.also { req ->
        requireScopedNamespaces(
            catalog(),
            req.appends.map { it.namespace } + req.deletes.map { it.namespace } +
                req.fileEnds.orEmpty().map { it.namespace },
        )
    }

internal fun CommitRequestDto.attributed(auth: AuthState): CommitRequestDto = copy(author = resolveActor(auth, author))
