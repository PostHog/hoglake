package com.posthog.hoglake

import com.posthog.hoglake.api.RequestDispatcher
import com.posthog.hoglake.api.installAlterRoutes
import com.posthog.hoglake.api.installApiRoutes
import com.posthog.hoglake.api.installBlockingDispatch
import com.posthog.hoglake.api.installDebugRoutes
import com.posthog.hoglake.api.installErrorMapping
import com.posthog.hoglake.api.installMaintenanceRoutes
import com.posthog.hoglake.api.installMountRoutes
import com.posthog.hoglake.api.installPartitionListingRoutes
import com.posthog.hoglake.api.installPartitionLookupRoutes
import com.posthog.hoglake.api.installPartitionStatsRoutes
import com.posthog.hoglake.api.installPublicationRoutes
import com.posthog.hoglake.api.installScanRoutes
import com.posthog.hoglake.api.installTableCreationRoutes
import com.posthog.hoglake.api.installUploadRoutes
import com.posthog.hoglake.api.installViewRoutes
import com.posthog.hoglake.auth.installAuth
import com.posthog.hoglake.auth.installNamespaceGate
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.compaction.CompactionConfig
import com.posthog.hoglake.compaction.CompactionService
import com.posthog.hoglake.compaction.ParquetRewriter
import com.posthog.hoglake.hydrator.Hydrator
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.CatalogMetrics
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.observability.RequestId
import com.posthog.hoglake.observability.requestId
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.CleanupService
import com.posthog.hoglake.service.DatabaseHealthService
import com.posthog.hoglake.service.ExpiryService
import com.posthog.hoglake.service.MaintenanceStatusService
import com.posthog.hoglake.service.MaintenanceSummarySampler
import com.posthog.hoglake.service.OptionsService
import com.posthog.hoglake.service.PartitionListingService
import com.posthog.hoglake.service.PartitionLookupService
import com.posthog.hoglake.service.PartitionStatsService
import com.posthog.hoglake.service.ReindexService
import com.posthog.hoglake.service.RemovalStore
import com.posthog.hoglake.service.RetirementService
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.service.TableCreationService
import com.posthog.hoglake.service.UploadService
import com.posthog.hoglake.service.ViewService
import com.posthog.hoglake.storage.StorageCredentialProviders
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.CatalogStoreSelector
import com.posthog.hoglake.store.MountHostResolver
import com.posthog.hoglake.store.MountReadService
import com.posthog.hoglake.store.MountRegistry
import com.posthog.hoglake.store.MountStore
import com.posthog.hoglake.store.PostgresCatalogStore
import com.posthog.hoglake.store.StaticMountHosts
import com.zaxxer.hikari.HikariDataSource
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.metrics.micrometer.MicrometerMetrics
import io.ktor.server.plugins.calllogging.CallLogging
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.micrometer.core.instrument.config.MeterFilter
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.jdbi.v3.core.Jdbi

/**
 * Application assembly: wires config + Jdbi into services and installs
 * the Ktor module. The api package registers the /v1 routes; this file
 * owns the cross-cutting pieces (serialization, logging, error mapping,
 * health, spec serving) and the background hydrator.
 */
class App private constructor(
    val cfg: Config,
    private val jdbiProvider: () -> Jdbi,
    private val postgresReady: () -> Boolean,
    private val postgresRuntime: PostgresRuntime? = null,
    /**
     * `/healthz`'s own one-connection probe (#218). Injected so a test
     * can point it at a database it is allowed to stop; defaulted from
     * [cfg] otherwise, and LAZY inside, so building an App opens no
     * second connection until something asks.
     */
    val healthProbe: HealthProbe,
    /** Mount records; null is this instance's `hog_mount` ([mountStore]). Tests inject them. */
    private val mountRegistry: MountRegistry? = null,
) : AutoCloseable {
    /** Access from a PostgreSQL request only; probes must use [postgresReady]. */
    val jdbi: Jdbi get() = jdbiProvider()

    /**
     * The bounded pool every route handler's blocking work runs on.
     * Installed once, in [module], by [installBlockingDispatch] — see
     * that file for the mechanism and the rejected alternatives.
     */
    val requestDispatcher = RequestDispatcher(cfg.requestThreads)

    /**
     * This instance's auth, built once so the API listener and the public
     * listener share one verifier and one JWKS cache. LAZY, and that is
     * load-bearing: [build] binds [Metrics] only after the constructor, and
     * building this registers the signing-key gauge, so it must first happen
     * in [module]. A mis-set mode (enforce with no issuer or key source) fails
     * there, at boot, naming the variable — not with a fleet-wide 401.
     */
    val auth: com.posthog.hoglake.auth.AuthService by lazy { com.posthog.hoglake.auth.AuthService.fromConfig(cfg) }

    private val catalogService by lazy {
        CatalogService(jdbi)
    }

    private val commitService by lazy {
        CommitService(
            jdbi,
            commitLockTimeoutMs = cfg.commitLockTimeoutMs,
            refuseBlindPartitionedAppends = cfg.refuseBlindPartitionedAppends,
        )
    }

    private val alterService by lazy {
        AlterService(jdbi)
    }

    private val scanService by lazy {
        ScanService(jdbi)
    }

    /** The default store stays PostgreSQL. Verified placement selects additional backends per request. */
    private val catalogStore: CatalogStore by lazy {
        PostgresCatalogStore({ catalogService }, { commitService }, { scanService })
    }

    private val mountStore by lazy { MountStore({ jdbi }, cfg::mountAdmitted) }

    /**
     * Client mode (plan row 1.3c): the remote hosts this server proxies
     * mounts to (HOGLAKE_MOUNT_HOSTS_FILE). With none configured nothing is
     * built and no name resolves to a host.
     */
    private val mountHosts = lazy { cfg.mountHosts.takeIf { it.isNotEmpty() }?.let(::StaticMountHosts) }

    private val catalogSelector by lazy {
        CatalogStoreSelector(
            catalogStore,
            cfg,
            MountRegistry.behindSwitch(MountRegistry.postgresSwitch { jdbi }, mountRegistry ?: mountStore),
            mountHosts.value ?: MountHostResolver.NONE,
        )
    }

    /** The mount (export) read surface (plan row 1.3h): the five scoped read routes over the pinned source table. */
    private val mountReadService by lazy {
        MountReadService(
            jdbi,
            { catalogStore },
            sourceCatalog = cfg.mountSourceCatalog,
            sourceTableUuid = cfg.mountSourceTableUuidOrNull,
            sourceFieldId = cfg.mountSourceFieldIdOrNull,
            fileBudget = cfg.mountScanFileBudget,
            scanDeadlineMs = cfg.mountScanDeadlineMs,
            maxConcurrentScans = cfg.mountScanMaxConcurrent,
        )
    }

    /** The mount read service, for tests that drive its cap and deadline directly. */
    internal val mountReadServiceForTest get() = mountReadService

    private val partitionLookupService by lazy {
        PartitionLookupService(jdbi)
    }

    private val viewService by lazy {
        ViewService(jdbi)
    }

    private val tableCreationService by lazy {
        TableCreationService(jdbi, catalogService, commitService, cfg.commitLockTimeoutMs)
    }
    private val uploadService by lazy { UploadService(jdbi, cfg.removalLedgerRetentionSeconds) }

    private val optionsService by lazy {
        OptionsService(jdbi)
    }

    private val expiryService by lazy {
        ExpiryService(jdbi, cfg)
    }

    /** Same threshold CompactionService plans with: debt == sweepable files. */
    private val partitionStatsService by lazy {
        PartitionStatsService(
            jdbi,
            smallFileThresholdBytes = cfg.compactionTargetBytes,
            minInputFiles = cfg.compactionMinInputFiles,
            maxInputFiles = cfg.compactionMaxInputFiles,
        )
    }

    /**
     * The per-table partitions tab, over the same published sample and
     * the same policy the debt ranking uses — a sample computed under a
     * different compaction policy is unusable for both.
     */
    private val partitionListingService by lazy {
        PartitionListingService(
            jdbi,
            smallFileThresholdBytes = cfg.compactionTargetBytes,
            minInputFiles = cfg.compactionMinInputFiles,
            maxInputFiles = cfg.compactionMaxInputFiles,
        )
    }

    /**
     * Where the server's own jobs get S3 credentials — ONE provider,
     * shared by the read/put store and the delete store, so under
     * `broker` the token and the per-catalog sessions are fetched once,
     * not once per store.
     */
    private val storageProvider by lazy {
        StorageCredentialProviders.fromConfig(cfg)
    }

    private val removalStore by lazy {
        RemovalStore(cfg, storageProvider)
    }

    /**
     * Every cleanup knob comes from `CleanupService`'s own `Config`
     * constructor rather than being spelled out here, because one of the
     * derivations is load-bearing: the clamp that makes `Config`'s pool
     * refusal true (one worker when the loop is off) has to key on the
     * same predicate the refusal prices on, and stating it at this call
     * site is how the two could quietly diverge.
     */
    private val cleanupService by lazy {
        CleanupService(jdbi, removalStore, cfg)
    }

    /**
     * Paced retirement of dropped tables' file rows — the other half of
     * the O(columns) drop. Off by default; the chart turns it on for the
     * maintenance workload alone, because a batch takes the per-catalog
     * commit lock.
     */
    private val retirementService by lazy {
        RetirementService(
            jdbi,
            batchSize = cfg.retirementBatch,
            pauseMs = cfg.retirementPauseMs,
            runBudgetMs = cfg.retirementRunBudgetMs,
            queueCeiling = cfg.retirementQueueCeiling,
            commitLockTimeoutMs = cfg.commitLockTimeoutMs,
        )
    }

    /**
     * The hourly index-bloat check + REINDEX (#268). Built on every pod:
     * the health page uses its estimate. The loop, and the
     * `hoglake_index_bloat_bytes` gauge, belong to the pod that runs it.
     */
    private val reindexService by lazy {
        ReindexService(
            jdbi,
            publishGauge = cfg.reindexIntervalMs > 0,
            purgeWaitWindows = cfg.reindexPurgeWaitWindows,
        )
    }

    /** Shared read/put store: hydrator footer reads + compaction rewrites. */
    private val objectStore by lazy {
        ObjectStore(cfg, storageProvider)
    }

    /** One hydrator: the background sweep loop AND the rehydrate route. */
    private val hydrator by lazy {
        Hydrator(
            jdbi,
            objectStore,
            maxWholeObjectBytes = cfg.hydratorMaxWholeObjectBytes,
            claimTimeoutMs = cfg.hydratorClaimTimeoutMs,
            minFileBytes = cfg.hydratorMinFileBytes,
        )
    }

    /** This pod's name, for the compaction planner's cursor spread and loop jitter. */
    private val plannerIdentity by lazy {
        CompactionService.defaultPlannerIdentity()
    }

    private val compactionService by lazy {
        CompactionService(
            jdbi,
            objectStore,
            CompactionConfig(
                targetBytes = cfg.compactionTargetBytes,
                minInputFiles = cfg.compactionMinInputFiles,
                maxInputFiles = cfg.compactionMaxInputFiles,
                maxGroupsPerRun = cfg.compactionMaxGroupsPerRun,
                candidateHeadroom = cfg.compactionCandidateHeadroom,
                maxCandidates = cfg.compactionMaxCandidates,
                newBucketWindow = cfg.compactionNewBucketWindow,
                maxFanIn = cfg.compactionMaxFanIn,
                parallelGroups = cfg.compactionParallelGroups,
                inputOpenParallelism = cfg.compactionParallelInputOpens,
                claimsEnabled = cfg.compactionClaimsEnabled,
                claimTtlSeconds = cfg.compactionClaimTtlSeconds,
                committedClaimTtlSeconds = cfg.compactionCommittedClaimTtlSeconds,
                bucketClaimTtlSeconds = cfg.compactionBucketClaimTtlSeconds,
                planStatementTimeoutMs = cfg.compactionPlanStatementTimeoutMs,
                commitLockTimeoutMs = cfg.commitLockTimeoutMs,
                nestedSortExpansion = cfg.compactionNestedSortExpansion,
                sortedHeapBytes = cfg.compactionSortedHeapBytes,
                spillBytes = cfg.compactionSpillBytes,
                spillDir = java.nio.file.Path.of(cfg.compactionSpillDir),
                verifyMinBytes = cfg.compactionVerifyMinBytes,
                maxNodesPerRow = cfg.compactionMaxNodesPerRow,
                codec = ParquetRewriter.OutputCodec.parse(cfg.compactionCodec, cfg.compactionZstdLevel),
                variantCompaction = cfg.compactionVariant,
                variantCompactionCatalogs = cfg.compactionVariantCatalogs,
            ),
            // The pod's identity spreads N pods' bucket cursors (and seeds
            // the compaction loop's jitter below). Passed HERE because the
            // constructor's default is no seeding, for the tests' sake.
            plannerIdentity = plannerIdentity,
        )
    }

    /** The run ledger's read side: status + history routes (webui maintenance page). */
    private val maintenanceStatusService by lazy {
        MaintenanceStatusService(
            jdbi,
            hydratorIntervalMs = cfg.hydratorIntervalMs,
            expiryIntervalMs = cfg.expiryIntervalMs,
            cleanupIntervalMs = cfg.cleanupIntervalMs,
            compactionIntervalMs = cfg.compactionIntervalMs,
            retirementIntervalMs = cfg.retirementIntervalMs,
            reindexIntervalMs = cfg.reindexIntervalMs,
            smallFileThresholdBytes = cfg.compactionTargetBytes,
            minInputFiles = cfg.compactionMinInputFiles,
            maxInputFiles = cfg.compactionMaxInputFiles,
        )
    }

    private val maintenanceSummarySampler by lazy {
        MaintenanceSummarySampler(
            jdbi,
            cfg.compactionTargetBytes,
            cfg.compactionMinInputFiles,
            cfg.compactionMaxInputFiles,
            cfg.maintenanceSummaryRefreshSeconds,
        )
    }

    /**
     * The process meter registry: Ktor http server metrics land here via
     * the MicrometerMetrics plugin, catalog-health gauges via
     * [catalogMetrics], and source-incremented counters via the
     * [Metrics] facade (bound in [build]). GET /metrics scrapes it.
     * /metrics itself is filtered out of the http request metrics.
     */
    val meterRegistry: PrometheusMeterRegistry =
        PrometheusMeterRegistry(PrometheusConfig.DEFAULT).apply {
            config().meterFilter(
                MeterFilter.deny { id ->
                    id.name.startsWith("ktor.http.server.requests") &&
                        id.getTag("route") == "/metrics"
                },
            )
        }

    /** Catalog-health gauge sampler; tests drive [CatalogMetrics.sampleOnce] directly. */
    val catalogMetrics by lazy {
        CatalogMetrics(
            jdbi,
            meterRegistry,
            // The six extended groups run where the maintenance summary
            // is produced (the maintenance Deployment), so API replicas do
            // not each republish the same instance-wide series.
            extendedGroups = cfg.maintenanceSummaryIntervalMs > 0,
        )
    }

    init {
        if (postgresReady()) initializePostgresServices()
    }

    /** Keep eager-mode service construction and gauges at boot, as before. */
    private fun initializePostgresServices() {
        catalogService
        commitService
        alterService
        scanService
        catalogStore
        catalogSelector
        partitionLookupService
        viewService
        optionsService
        expiryService
        partitionStatsService
        partitionListingService
        removalStore
        cleanupService
        retirementService
        reindexService
        objectStore
        hydrator
        plannerIdentity
        compactionService
        maintenanceStatusService
        maintenanceSummarySampler
        catalogMetrics
    }

    companion object {
        /** Production boot: the default keeps the existing eager migration order. */
        fun build(cfg: Config): App {
            val postgres = PostgresRuntime(cfg)
            try {
                if (cfg.defaultBackend == "postgres") postgres.get()
                val app = App(cfg, postgres::get, { postgres.isInitialized }, postgres, HealthProbe.fromConfig(cfg))
                app.bindMetrics(null)
                postgres.onInitialized = { pool ->
                    Metrics.registerDbPoolGauges(app.meterRegistry, pool)
                    app.startBackgroundIfReady()
                }
                // Eager initialization ran before the callback was installed.
                if (postgres.isInitialized) {
                    postgres.notifyInitialized()
                }
                return app
            } catch (e: Exception) {
                postgres.close()
                throw e
            }
        }

        /**
         * @param dataSource the REQUEST pool, when the caller has it —
         *   `Main.kt` does. It is only ever read through its MXBean, to
         *   publish the `hoglake_db_pool_*` saturation gauges; null
         *   (the test default, where Jdbi is all most fixtures hold)
         *   simply publishes no pool gauges.
         * @param healthProbe `/healthz`'s own connection; defaults to
         *   one built from [cfg].
         */
        fun build(
            cfg: Config,
            jdbi: Jdbi,
            dataSource: HikariDataSource? = null,
            healthProbe: HealthProbe = HealthProbe.fromConfig(cfg),
            mountRegistry: MountRegistry? = null,
        ): App =
            App(cfg, { jdbi }, { true }, mountRegistry = mountRegistry, healthProbe = healthProbe).also {
                it.bindMetrics(dataSource)
            }
    }

    private fun bindMetrics(dataSource: HikariDataSource?) {
        Metrics.bind(meterRegistry)
        Metrics.registerRequestPoolGauges(meterRegistry, requestDispatcher)
        Metrics.registerHealthProbeGauge(meterRegistry) { healthProbe.outstandingAttempts }
        // Every pod, not only the loop's: a manual run spills too.
        val spillDir = java.nio.file.Path.of(cfg.compactionSpillDir)
        Metrics.registerSpillDirGauge(meterRegistry) {
            com.posthog.hoglake.compaction.SpillDirectory.measuredBytes(spillDir)
        }
        if (dataSource != null) Metrics.registerDbPoolGauges(meterRegistry, dataSource)
    }

    /**
     * Mounts every route on [app] behind [auth]: this instance's own on the
     * API listener, [com.posthog.hoglake.auth.AuthService.forPublicListener]
     * on the public one. The routes are the same; only who may call them
     * differs.
     */
    fun module(
        app: Application,
        auth: com.posthog.hoglake.auth.AuthService = this.auth,
    ) {
        // RequestId comes before auth, and the order is load-bearing. Auth
        // refuses by responding from inside its own interceptor, so anything
        // installed after it runs on a call whose response is already built: a
        // 401 would lose its X-Request-Id (and, with CallLogging below it, its
        // access-log line) exactly when somebody is trying to correlate a
        // refusal with a server record. Both are cheap and neither touches the
        // database, so both belong on the outside.
        app.install(RequestId)
        app.install(CallLogging) {
            // Coroutine-safe MDC propagation of the request id, so audit
            // lines emitted inside request handling carry request_id.
            mdc(Audit.REQUEST_ID_MDC) { call -> call.requestId }
        }

        // THEN, and before anything that does real work: authenticate and
        // authorize, so a refused request never reaches the blocking
        // dispatcher, never borrows a connection, and never runs a handler that
        // could have been written without a check. The routes below are
        // installed by a dozen `install*Routes` functions; the plugin's rule
        // table (auth/AuthPolicy.kt) is what makes "every route has a policy" a
        // property of the build rather than a property of the reviewer.
        //
        // Inert unless HOGLAKE_AUTH_MODE says otherwise: `off` is the default and
        // resolves no callers, which is precisely today's behaviour. A mis-set
        // `enforce` (no issuer, no key source) fails here, at boot, naming the
        // variable — not with a fleet-wide 401 (see [auth]).
        app.installAuth(auth)

        // Then, and once: every handler installed below this line runs
        // its blocking database/object-store work on requestDispatcher
        // rather than on the Netty event loop. The probe paths are
        // bypassed, the queue wait is stamped and measured, and a
        // request that queued past the commit admission bound is shed
        // with the typed 503 before it borrows a connection
        // (api/BlockingDispatch.kt).
        app.installBlockingDispatch(requestDispatcher, cfg.commitLockTimeoutMs)
        // A scoped caller's namespace check, on the dispatcher and before
        // routing: the half of authorization that needs the database
        // (auth/NamespaceGate.kt).
        app.installNamespaceGate(catalogSelector)
        app.install(ContentNegotiation) {
            // One definition, shared with the tests that pin this
            // behaviour (api/WireJson.kt).
            jackson { configureHoglakeWire() }
        }
        app.install(MicrometerMetrics) {
            registry = meterRegistry
            // The JVM binders belong to the process, not to a listener: bound
            // again by the public listener's module, JvmGcMetrics would add a
            // second GC listener and double every jvm_gc_* counter.
            if (auth.tenantOnly) meterBinders = emptyList()
            // Unmatched request paths must NOT each mint a `route` tag —
            // hostile path scans would otherwise grow series without bound.
            distinctNotRegisteredRoutes = false
        }
        app.install(StatusPages) { installErrorMapping() }
        app.routing {
            // Prometheus scrape endpoint, excluded from the http request
            // metrics + audit log. GATED by auth/AuthPolicy.kt as
            // operator-only: the exposition carries per-catalog labels,
            // i.e. the tenant roster, so it is not a public surface.
            // The scraper uses the infra listener instead
            // (HOGLAKE_METRICS_PORT, observability/MetricsListener.kt,
            // docs/auth-tenancy.md §6.7), which serves the same registry
            // with no auth plugin on a port the Service does not expose.
            get("/metrics") {
                call.respondText(
                    this@App.meterRegistry.scrape(),
                    ContentType.parse("text/plain; version=0.0.4; charset=utf-8"),
                )
            }
            // Process liveness only — never touches the database.
            get("/livez") {
                call.respondText("ok")
            }
            // Readiness: the catalog must be reachable. A dead database
            // must read as unhealthy, not hang (the zombie-server
            // incident: /healthz said ok while every /v1 call hung on a
            // dead PG) — and a BUSY REQUEST POOL must read as healthy,
            // which is the half #218 added. The probe answers on its own
            // connection and its own thread, bounded four ways by
            // HOGLAKE_HEALTH_PROBE_TIMEOUT_MS, so neither a full Hikari
            // pool nor a full request dispatcher can delay it.
            get("/healthz") {
                val ok =
                    (!postgresReady() || healthProbe.reachable()) &&
                        kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.IO) {
                            catalogSelector.reachable()
                        }
                if (ok) {
                    call.respondText("ok")
                } else {
                    call.respondText("db unreachable", status = HttpStatusCode.ServiceUnavailable)
                }
            }
            get("/openapi.yaml") {
                val spec = javaClass.getResource("/openapi/hoglake.yaml")!!.readText()
                call.respondText(spec, ContentType.parse("application/yaml"))
            }
        }
        app.installApiRoutes(
            catalogSelector,
            cfg.instanceName,
            instanceTotals = { if (postgresReady()) catalogMetrics.latestTotals else null },
            catalogTotals = { if (postgresReady()) catalogMetrics.latestByCatalog else emptyMap() },
            uiTheme = cfg.uiTheme,
            // A provider, not the instance: forcing the lazy here would open
            // PG at wiring time and break the PG-free / lazy-boot paths. It is
            // built only when a mount actually resolves (PG is the default then).
            mountRead = { mountReadService },
            reservedNamespaces = cfg.reservedNamespaces,
        )
        app.installTableCreationRoutes(
            { tableCreationService },
            selector = catalogSelector,
        )
        app.installUploadRoutes(
            { uploadService },
            selector = catalogSelector,
        )
        app.installAlterRoutes({ alterService }, selector = catalogSelector)
        app.installScanRoutes(catalogSelector, { mountReadService })
        app.installViewRoutes({ viewService }, selector = catalogSelector)
        app.installMaintenanceRoutes(
            { optionsService },
            { expiryService },
            { cleanupService },
            { compactionService },
            { hydrator },
            { maintenanceStatusService },
            { DatabaseHealthService(jdbi, reindexService) },
            selector = catalogSelector,
        )
        app.installPartitionStatsRoutes({ partitionStatsService }, selector = catalogSelector)
        app.installPartitionListingRoutes({ partitionListingService }, selector = catalogSelector)
        app.installPartitionLookupRoutes({ partitionLookupService }, selector = catalogSelector)
        app.installPublicationRoutes(catalogSelector)
        app.installMountRoutes({ mountStore }, cfg.mountWriterSubject)
        app.installDebugRoutes()
    }

    /**
     * Start the background loops as coroutines under one supervisor
     * scope ([BackgroundLoops]): structured, bounded cancellation on
     * close, per-loop failure isolation, `intervalMs <= 0` = disabled.
     * The returned handle stops every loop and closes the stores.
     */
    private val backgroundLock = Any()
    private var backgroundRequested = false
    private var backgroundClosed = false
    private var background: AutoCloseable? = null

    fun startBackground(): AutoCloseable {
        synchronized(backgroundLock) { backgroundRequested = true }
        startBackgroundIfReady()
        return AutoCloseable { closeBackground() }
    }

    private fun startBackgroundIfReady() =
        synchronized(backgroundLock) {
            if (backgroundRequested && !backgroundClosed && background == null && postgresReady()) {
                background = startPostgresBackground()
            }
        }

    private fun closeBackground() =
        synchronized(backgroundLock) {
            backgroundClosed = true
            background?.close()
            background = null
        }

    private fun startPostgresBackground(): AutoCloseable {
        // BEFORE any loop starts, once, and only where the compaction loop
        // runs: a previous container in this pod may have been killed
        // mid-rewrite, and its `hoglake-compaction-spill-*` directories are on an emptyDir
        // that outlives it and that nothing else would ever empty. Run
        // here, synchronously, so it can never race a group — no loop has
        // started, and Main starts the engine (the manual route) after
        // this returns.
        if (cfg.compactionIntervalMs > 0) {
            com.posthog.hoglake.compaction.SpillDirectory.sweepLeftovers(java.nio.file.Path.of(cfg.compactionSpillDir))
        }
        val loops = BackgroundLoops()
        loops.register("maintenance_summary", cfg.maintenanceSummaryIntervalMs) {
            maintenanceSummarySampler.tick(cfg.maintenanceSummaryBatch)
        }
        loops.register("hydrator", cfg.hydratorIntervalMs) { hydrator.runOnce() }
        loops.register("expiry", cfg.expiryIntervalMs) {
            expiryService.runOnceAllCatalogs(cfg.expiryBatchSize)
        }
        loops.register("cleanup", cfg.cleanupIntervalMs) {
            cleanupService.runOnceAllCatalogs(cfg.cleanupBatchSize)
        }
        // Default interval 0 = off for now; the manual trigger stays live.
        // The ONE jittered loop: N maintenance pods whose sweeps end
        // together must not re-plan together (BackgroundLoops.Jitter has
        // the 2026-10-21 incident). Seeded by the same pod identity that
        // spreads the planners' bucket cursors.
        loops.register(
            "compaction",
            cfg.compactionIntervalMs,
            BackgroundLoops.Jitter(
                BackgroundLoops.COMPACTION_JITTER_FRACTION,
                CompactionService.identitySeed(plannerIdentity),
            ),
        ) {
            compactionService.runOnceAllCatalogs()
        }
        // Default 0 = off, like compaction, and for a sharper reason: a retirement batch TAKES THE PER-CATALOG
        // COMMIT LOCK, hundreds of times per run. On an API replica that
        // is the commit tail taxing itself. The chart turns it on for the
        // maintenance Deployment, where the drain it feeds also runs.
        loops.register("retirement", cfg.retirementIntervalMs) {
            retirementService.runOnceAllCatalogs()
        }
        // Default 0 = off, like retirement: ONE workload rebuilds indexes.
        // The interval is a POLL; the run is due at the top of each UTC hour,
        // gated on the run ledger inside pollOnce (ReindexService).
        loops.register("reindex", cfg.reindexIntervalMs) { reindexService.pollOnce() }
        loops.register("metrics", cfg.metricsIntervalMs) { metricsTick() }
        return AutoCloseable {
            loops.close()
            removalStore.close()
            objectStore.close()
        }
    }

    /**
     * One metrics-loop iteration. The bloat gauge rides it on the reindex
     * pod only (a no-op elsewhere): ~4 ms of catalog reads, so the series
     * follows VACUUM/ANALYZE between the hourly runs. In a `finally`, so a
     * failed sample does not skip it; `refreshGauge` never throws, so its
     * failure neither fails this tick nor counts as a metrics-loop failure
     * (it has `hoglake_reindex_gauge_refresh_failures_total`).
     */
    internal fun metricsTick() {
        try {
            catalogMetrics.sampleOnce()
        } finally {
            reindexService.refreshGauge()
        }
    }

    /**
     * Release what [module] and the probe own: the blocking request
     * dispatcher's threads and `/healthz`'s connection. Separate from
     * [startBackground]'s handle because a process can serve without
     * any loops running (and most test fixtures do); `Main.kt` closes
     * both.
     */
    override fun close() {
        if (mountHosts.isInitialized()) mountHosts.value?.close()
        requestDispatcher.close()
        closeBackground()
        healthProbe.close()
        postgresRuntime?.close()
    }
}
