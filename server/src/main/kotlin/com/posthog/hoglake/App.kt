package com.posthog.hoglake

import com.posthog.hoglake.api.RequestDispatcher
import com.posthog.hoglake.api.installAlterRoutes
import com.posthog.hoglake.api.installApiRoutes
import com.posthog.hoglake.api.installBlockingDispatch
import com.posthog.hoglake.api.installDebugRoutes
import com.posthog.hoglake.api.installErrorMapping
import com.posthog.hoglake.api.installMaintenanceRoutes
import com.posthog.hoglake.api.installPartitionListingRoutes
import com.posthog.hoglake.api.installPartitionLookupRoutes
import com.posthog.hoglake.api.installPartitionStatsRoutes
import com.posthog.hoglake.api.installPublicationRoutes
import com.posthog.hoglake.api.installScanRoutes
import com.posthog.hoglake.api.installTableCreationRoutes
import com.posthog.hoglake.api.installUploadRoutes
import com.posthog.hoglake.api.installViewRoutes
import com.posthog.hoglake.auth.installAuth
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
import com.posthog.hoglake.service.ViewService
import com.posthog.hoglake.store.PostgresCatalogStore
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
    /**
     * The Postgres handle — NULL under `HOGLAKE_DATABASE=hoglite`, where
     * no Postgres exists. Every service below that takes it is `lazy`,
     * so a hoglite-mode App never constructs one; the route installers
     * that need them are guarded on `jdbi != null` in [module], and the
     * background loops likewise in [startBackground].
     */
    val jdbi: Jdbi?,
    /**
     * The catalog backend the /v1 routes serve, when the caller brings
     * one (the hoglite mode); null on the Postgres path, where [store]
     * below builds it from THE SAME lazy service instances the rest of
     * the App uses — one CommitService per process, so state like its
     * warn-dedup set never diverges between the route path and the
     * store path.
     */
    private val storeParam: com.posthog.hoglake.store.CatalogStore?,
    /**
     * `/healthz`'s own one-connection probe (#218). Injected so a test
     * can point it at a database it is allowed to stop; defaulted from
     * [cfg] otherwise, and LAZY inside, so building an App opens no
     * second connection until something asks.
     */
    val healthProbe: HealthProbe,
) : AutoCloseable {
    /**
     * The bounded pool every route handler's blocking work runs on.
     * Installed once, in [module], by [installBlockingDispatch] — see
     * that file for the mechanism and the rejected alternatives.
     */
    val requestDispatcher = RequestDispatcher(cfg.requestThreads)

    /** The routes' catalog backend: the caller's (hoglite) or the Postgres delegate over THIS App's services. */
    val store: com.posthog.hoglake.store.CatalogStore by lazy {
        storeParam ?: PostgresCatalogStore(catalogService, commitService, scanService)
    }

    private val catalogService by lazy { CatalogService(jdbi!!) }
    private val commitService by lazy {
        CommitService(
            jdbi!!,
            commitLockTimeoutMs = cfg.commitLockTimeoutMs,
            refuseBlindPartitionedAppends = cfg.refuseBlindPartitionedAppends,
        )
    }
    private val alterService by lazy { AlterService(jdbi!!) }
    private val scanService by lazy { ScanService(jdbi!!) }
    private val partitionLookupService by lazy { PartitionLookupService(jdbi!!) }
    private val viewService by lazy { ViewService(jdbi!!) }
    private val optionsService by lazy { OptionsService(jdbi!!) }
    private val expiryService by lazy { ExpiryService(jdbi!!, cfg) }

    /** Same threshold CompactionService plans with: debt == sweepable files. */
    private val partitionStatsService by lazy {
        PartitionStatsService(
            jdbi!!,
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
            jdbi!!,
            smallFileThresholdBytes = cfg.compactionTargetBytes,
            minInputFiles = cfg.compactionMinInputFiles,
            maxInputFiles = cfg.compactionMaxInputFiles,
        )
    }
    private val removalStore = RemovalStore(cfg)

    /**
     * Every cleanup knob comes from `CleanupService`'s own `Config`
     * constructor rather than being spelled out here, because one of the
     * derivations is load-bearing: the clamp that makes `Config`'s pool
     * refusal true (one worker when the loop is off) has to key on the
     * same predicate the refusal prices on, and stating it at this call
     * site is how the two could quietly diverge.
     *
     * CleanupConfigTest pins this construction text: the service must be
     * built from Config (the loop predicate derivation lives in its Config
     * constructor, where a test can build it), not knob by knob here.
     */
    private fun newCleanupService(jdbi: Jdbi) = CleanupService(jdbi, removalStore, cfg)

    private val cleanupService by lazy { newCleanupService(requireNotNull(jdbi)) }

    /**
     * Paced retirement of dropped tables' file rows — the other half of
     * the O(columns) drop. Off by default; the chart turns it on for the
     * maintenance workload alone, because a batch takes the per-catalog
     * commit lock.
     */
    private val retirementService by lazy {
        RetirementService(
            jdbi!!,
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
            jdbi!!,
            publishGauge = cfg.reindexIntervalMs > 0,
            purgeWaitWindows = cfg.reindexPurgeWaitWindows,
        )
    }

    /** Shared read/put store: hydrator footer reads + compaction rewrites. */
    private val objectStore = ObjectStore(cfg)

    // ---- the hoglite backend's service ports (constructed only when the
    // backend is hoglite; `store` is the HogliteCatalogStore then) --------

    private fun hogliteStore() = store as com.posthog.hoglake.hoglite.HogliteCatalogStore

    private val hogliteOptions by lazy { com.posthog.hoglake.hoglite.HogliteOptions(hogliteStore()) }
    private val hogliteExpiry by lazy {
        com.posthog.hoglake.hoglite.HogliteExpiry(
            hogliteStore(),
            purgePage = cfg.expiryPurgePage,
            purgeBudgetMs = cfg.expiryPurgeBudgetMs,
        )
    }
    private val hogliteCleanup by lazy { com.posthog.hoglake.hoglite.HogliteCleanup(cfg, hogliteStore()) }
    private val hogliteHydrator by lazy {
        com.posthog.hoglake.hoglite.HogliteHydrator(
            hogliteStore(),
            objectStore,
            minFileBytes = cfg.hydratorMinFileBytes,
        )
    }
    private val hogliteCompaction by lazy {
        com.posthog.hoglake.hoglite.HogliteCompaction(
            hogliteStore(),
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
        )
    }
    private val hogliteRetirement by lazy {
        com.posthog.hoglake.hoglite.HogliteRetirement(
            hogliteStore(),
            batchSize = cfg.retirementBatch,
            queueCeiling = cfg.retirementQueueCeiling,
            runBudgetMs = cfg.retirementRunBudgetMs,
        )
    }
    private val hogliteMaintenanceStatus by lazy {
        com.posthog.hoglake.hoglite.HogliteMaintenanceStatus(
            hogliteStore(),
            hydratorIntervalMs = cfg.hydratorIntervalMs,
            expiryIntervalMs = cfg.expiryIntervalMs,
            cleanupIntervalMs = cfg.cleanupIntervalMs,
            compactionIntervalMs = cfg.compactionIntervalMs,
            retirementIntervalMs = cfg.retirementIntervalMs,
            smallFileThresholdBytes = cfg.compactionTargetBytes,
        )
    }
    private val hoglitePartitions by lazy {
        com.posthog.hoglake.hoglite.HoglitePartitions(
            hogliteStore(),
            smallFileThresholdBytes = cfg.compactionTargetBytes,
            minInputFiles = cfg.compactionMinInputFiles,
            maxInputFiles = cfg.compactionMaxInputFiles,
        )
    }
    private val hoglitePartitionLookup by lazy { com.posthog.hoglake.hoglite.HoglitePartitionLookup(hogliteStore()) }

    /** One hydrator: the background sweep loop AND the rehydrate route. */
    private val hydrator by lazy {
        Hydrator(
            jdbi!!,
            objectStore,
            maxWholeObjectBytes = cfg.hydratorMaxWholeObjectBytes,
            claimTimeoutMs = cfg.hydratorClaimTimeoutMs,
            minFileBytes = cfg.hydratorMinFileBytes,
        )
    }

    /** This pod's name, for the compaction planner's cursor spread and loop jitter. */
    private val plannerIdentity = CompactionService.defaultPlannerIdentity()

    private val compactionService by lazy {
        CompactionService(
            jdbi!!,
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
            jdbi!!,
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
            jdbi!!,
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

    /**
     * Every Postgres-backend service, constructed now. The services are
     * `lazy` only so a hoglite App (jdbi = null) never builds them; on
     * the Postgres path they are built at [build] time, as they were
     * when they were plain vals, so a setting a constructor refuses
     * (a retirement batch of 0, a compaction minInputFiles of 1, a
     * cleanup sub-batch of 0, a summary refresh of 0) refuses BOOT —
     * before Main starts a loop, binds the metrics port or the API —
     * instead of failing every loop tick of a process with no API.
     * CatalogMetrics registers its MultiGauges in its constructor, so
     * this also keeps them present from boot.
     */
    internal fun constructPostgresServices() {
        store
        catalogService
        commitService
        alterService
        scanService
        partitionLookupService
        viewService
        optionsService
        expiryService
        partitionStatsService
        partitionListingService
        cleanupService
        retirementService
        reindexService
        hydrator
        compactionService
        maintenanceStatusService
        maintenanceSummarySampler
        catalogMetrics
    }

    /** The hoglite backend's services, constructed now — the same fail-fast as [constructPostgresServices]. */
    internal fun constructHogliteServices() {
        hogliteStore()
        hogliteOptions
        hogliteExpiry
        hogliteCleanup
        hogliteHydrator
        hogliteCompaction
        hogliteRetirement
        hogliteMaintenanceStatus
        hoglitePartitions
        hoglitePartitionLookup
    }

    /** Catalog-health gauge sampler; tests drive [CatalogMetrics.sampleOnce] directly. */
    val catalogMetrics by lazy {
        CatalogMetrics(
            jdbi!!,
            meterRegistry,
            // The five extended groups run where the maintenance summary
            // is produced (the maintenance Deployment), so API replicas do
            // not each republish the same instance-wide series.
            extendedGroups = cfg.maintenanceSummaryIntervalMs > 0,
        )
    }

    companion object {
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
        ): App =
            App(cfg, jdbi, storeParam = null, healthProbe = healthProbe)
                // Construct first, bind second: a constructor's refusal
                // (an invalid setting Config does not check) must throw
                // here, before Metrics is bound or a loop is started.
                .also { it.constructPostgresServices() }
                .let { buildShared(it, cfg, dataSource) }

        /**
         * The hoglite backend: no Postgres, no Jdbi; the store is a
         * [HogliteCatalogStore] over the client Main built from the
         * `HOGLAKE_HOGLITE_*` knobs, and the probe asks hoglite's own
         * `/v1/healthz`.
         */
        fun buildHoglite(
            cfg: Config,
            store: com.posthog.hoglake.hoglite.HogliteCatalogStore,
            healthProbe: HealthProbe,
        ): App =
            App(cfg, null, store, healthProbe)
                .also { it.constructHogliteServices() }
                .let { buildShared(it, cfg, dataSource = null) }

        private fun buildShared(
            app: App,
            cfg: Config,
            dataSource: HikariDataSource?,
        ): App =
            app.also {
                Metrics.bind(it.meterRegistry)
                Metrics.registerRequestPoolGauges(it.meterRegistry, it.requestDispatcher)
                Metrics.registerHealthProbeGauge(it.meterRegistry) { it.healthProbe.outstandingAttempts }
                // Every pod, not only the loop's: a manual run spills too.
                val spillDir = java.nio.file.Path.of(cfg.compactionSpillDir)
                Metrics.registerSpillDirGauge(it.meterRegistry) {
                    com.posthog.hoglake.compaction.SpillDirectory.measuredBytes(spillDir)
                }
                if (dataSource != null) Metrics.registerDbPoolGauges(it.meterRegistry, dataSource)
            }
    }

    fun module(app: Application) {
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
        // variable — not with a fleet-wide 401.
        app.installAuth(com.posthog.hoglake.auth.AuthService.fromConfig(cfg))

        // Then, and once: every handler installed below this line runs
        // its blocking database/object-store work on requestDispatcher
        // rather than on the Netty event loop. The probe paths are
        // bypassed, the queue wait is stamped and measured, and a
        // request that queued past the commit admission bound is shed
        // with the typed 503 before it borrows a connection
        // (api/BlockingDispatch.kt).
        app.installBlockingDispatch(requestDispatcher, cfg.commitLockTimeoutMs)
        app.install(ContentNegotiation) {
            // One definition, shared with the tests that pin this
            // behaviour (api/WireJson.kt).
            jackson { configureHoglakeWire() }
        }
        app.install(MicrometerMetrics) {
            registry = meterRegistry
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
                val ok = healthProbe.reachable()
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
            store,
            cfg.instanceName,
            // The sampler's last pass — Postgres only. Under hoglite the
            // totals are absent until the metrics sampler is ported
            // (nulls are the documented boot-window shape, so this is
            // honest, not a stub).
            instanceTotals = { if (jdbi != null) catalogMetrics.latestTotals else null },
            catalogTotals = { if (jdbi != null) catalogMetrics.latestByCatalog else emptyMap() },
            uiTheme = cfg.uiTheme,
        )
        app.installScanRoutes(store)
        app.installPublicationRoutes()
        app.installDebugRoutes()
        if (jdbi != null) {
            app.installTableCreationRoutes(
                TableCreationService(jdbi, catalogService, commitService, cfg.commitLockTimeoutMs),
            )
            app.installUploadRoutes(
                com.posthog.hoglake.service.UploadService(jdbi, cfg.removalLedgerRetentionSeconds),
            )
            app.installAlterRoutes(alterService)
            app.installViewRoutes(viewService)
            app.installMaintenanceRoutes(
                optionsService,
                expiryService,
                cleanupService,
                compactionService,
                hydrator,
                maintenanceStatusService,
                DatabaseHealthService(jdbi, reindexService),
            )
            app.installPartitionStatsRoutes(partitionStatsService)
            app.installPartitionListingRoutes(partitionListingService)
            app.installPartitionLookupRoutes(partitionLookupService)
        } else {
            // HOGLAKE_DATABASE=hoglite. These route groups run against
            // the hoglite-backed implementations in the hoglite package
            // instead of the Jdbi services.
            val hogliteStore = store as com.posthog.hoglake.hoglite.HogliteCatalogStore
            app.installUploadRoutes(hogliteStore.uploads)
            app.installTableCreationRoutes(hogliteStore.tableCreations)
            app.installAlterRoutes(hogliteStore.alters)
            app.installViewRoutes(hogliteStore.views)
            app.installMaintenanceRoutes(
                hogliteOptions,
                hogliteExpiry,
                hogliteCleanup,
                hogliteCompaction,
                hogliteHydrator,
                hogliteMaintenanceStatus,
                com.posthog.hoglake.hoglite.HogliteDatabaseHealth(hogliteStore.client),
            )
            app.installPartitionStatsRoutes(hoglitePartitions)
            app.installPartitionListingRoutes(hoglitePartitions)
            app.installPartitionLookupRoutes(hoglitePartitionLookup)
        }
    }

    /**
     * Start the background loops as coroutines under one supervisor
     * scope ([BackgroundLoops]): structured, bounded cancellation on
     * close, per-loop failure isolation, `intervalMs <= 0` = disabled.
     * The returned handle stops every loop and closes the stores.
     */
    fun startBackground(): AutoCloseable {
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
        if (jdbi == null) {
            // HOGLAKE_DATABASE=hoglite. The same loop names, cadences and
            // off-by-default semantics as the Postgres branch below, over
            // the tenant-local ports. What is missing, deliberately:
            //  - maintenance_summary (the sampler exists to spare a
            //    10M-row manifest a per-request scan on a shared
            //    database; a hoglite tenant IS one catalog, so the
            //    status/partition/table-GET reads compute exactly),
            //  - reindex (B-tree bloat is Postgres's problem),
            //  - the metrics tick's catalogTotals sampler (the metrics
            //    loop's gauges ride Jdbi reads).
            // Everything else is the same work on the same cadence.
            loops.register("hydrator", cfg.hydratorIntervalMs) { hogliteHydrator.runOnce() }
            loops.register("expiry", cfg.expiryIntervalMs) {
                hogliteExpiry.runOnceAllCatalogs(cfg.expiryBatchSize)
            }
            loops.register("cleanup", cfg.cleanupIntervalMs) {
                hogliteCleanup.runOnceAllCatalogs(cfg.cleanupBatchSize)
            }
            loops.register(
                "compaction",
                cfg.compactionIntervalMs,
                BackgroundLoops.Jitter(
                    BackgroundLoops.COMPACTION_JITTER_FRACTION,
                    com.posthog.hoglake.compaction.CompactionService.identitySeed(plannerIdentity),
                ),
            ) {
                hogliteCompaction.runOnceAllCatalogs()
            }
            loops.register("retirement", cfg.retirementIntervalMs) {
                hogliteRetirement.runOnceAllCatalogs()
            }
            return AutoCloseable {
                loops.close()
                hogliteCleanup.close()
                removalStore.close()
                objectStore.close()
            }
        }
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
        requestDispatcher.close()
        healthProbe.close()
    }
}
