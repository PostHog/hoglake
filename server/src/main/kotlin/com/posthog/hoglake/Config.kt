package com.posthog.hoglake

/** Process configuration, environment-sourced. No config files in v1. */
data class Config(
    val port: Int = env("HOGLAKE_PORT", "8080").toInt(),
    /**
     * Human-facing name for THIS hoglake instance (e.g. "GigaHog"),
     * shown by the webui so operators can tell deployments apart.
     * Empty = unnamed.
     */
    val instanceName: String = env("HOGLAKE_INSTANCE_NAME", ""),
    /** Color theme for the webui. Empty keeps the default palette. */
    val uiTheme: String = env("HOGLAKE_UI_THEME", ""),
    val jdbcUrl: String = env("HOGLAKE_JDBC_URL", "jdbc:postgresql://localhost:5432/hoglake"),
    val dbUser: String = env("HOGLAKE_DB_USER", "hoglake"),
    val dbPassword: String = env("HOGLAKE_DB_PASSWORD", "hoglake"),
    val dbPoolSize: Int = env("HOGLAKE_DB_POOL_SIZE", "10").toInt(),
    /**
     * How many threads serve blocking route handlers (#218).
     *
     * Every handler's JDBC and object-store work runs on this bounded
     * pool, not on the Netty event loop — see
     * `api/BlockingDispatch.kt` for why that seam exists at all. This
     * knob is the pool's width, and it is therefore the ceiling on
     * CONCURRENT REQUESTS IN A BLOCKING CALL.
     *
     * DEFAULT = [dbPoolSize], and the reason is what happens on the
     * other side of the handler. Nearly every handler's first act is to
     * borrow a Hikari connection, so handler threads above the pool
     * size do not buy concurrency: they queue inside
     * `HikariPool.getConnection`, which gives up after
     * `Database.dataSource`'s 5 s `connectionTimeout` and throws — a
     * 500, not the typed 503 + Retry-After the admission contract
     * promises, and nothing in it says which knob caused it (the same
     * failure `HOGLAKE_COMPACTION_PARALLEL_GROUPS`' boot check exists to
     * prevent). Sizing the dispatcher AT the pool makes the dispatcher's
     * own queue the backpressure instead: excess requests wait in a
     * FIFO queue with no timeout of their own until a thread frees, and
     * whatever bound they hit is the client's or the commit admission
     * bound's, both of which are typed.
     *
     * It is NOT sized at `dbPoolSize` minus the background loops'
     * draw. The loops (hydrator, expiry, cleanup, compaction,
     * retirement, the metrics sampler) share the pool, so under a full
     * dispatcher some foreground requests can still queue on Hikari for
     * up to 5 s and 500. That is the PRE-EXISTING behaviour, unchanged
     * by this knob, and deriving a smaller default from a loop set that
     * is mostly off in the API workload would idle handler threads
     * whenever the loops are quiet. What #218 fixes is that none of it
     * reaches `/healthz` any more (see [healthProbeTimeoutMs]).
     *
     * REFUSED ABOVE THE POOL at boot, for that reason: raising it is a
     * pair of knobs, and forgetting the second one turns dispatcher
     * queueing into Hikari timeouts across the whole fleet. Raise
     * `HOGLAKE_DB_POOL_SIZE` first (a workload dominated by
     * object-store rather than SQL work can genuinely use more handler
     * threads than connections — Ktor parses request bodies on
     * `Dispatchers.IO`, off this pool entirely), then this.
     *
     * THE RESERVE, and what the default does NOT promise. On the API
     * workload the chart turns every background loop off, so the
     * foreground is the pool's only customer and `requestThreads ==
     * dbPoolSize` is exact. On a pod that also runs loops — the
     * maintenance Deployment, a dev stack, anything inheriting the
     * defaults — the foreground alone can take every connection, and a
     * hydrator or expiry sweep then waits Hikari's 5 s and fails its
     * iteration (logged and counted as
     * `hoglake_background_loop_failures_total`, and retried on the next
     * interval, so it is a delay rather than a loss). There is no
     * arithmetic here that prevents it: `Config.FOREGROUND_CONNECTION_RESERVE`
     * bounds COMPACTION's and CLEANUP's draw on the pool, not the
     * foreground's. A
     * workload that runs loops and serves traffic should set
     * `HOGLAKE_REQUEST_THREADS` below `HOGLAKE_DB_POOL_SIZE` by the
     * number of loops it runs.
     */
    val requestThreads: Int = env("HOGLAKE_REQUEST_THREADS", dbPoolSize.toString()).toInt(),
    /**
     * The per-operation bound on the `/healthz` probe's own connection,
     * in milliseconds (#218).
     *
     * The probe does not share the request pool: it owns a
     * one-connection pool of its own (`HealthProbe`), so "the pool is
     * busy serving commits" reads as 200 and only "Postgres does not
     * answer" reads as 503. This value is applied FOUR times over —
     * Hikari `connectionTimeout`, pgjdbc `connectTimeout`, pgjdbc
     * `socketTimeout`, and the session `statement_timeout` — so that
     * no layer can outlast the kubelet's probe timeout. The probe's
     * wall-clock deadline is twice it (connect, then query), which at
     * the default is 4 s inside the kubelet's usual 5 s.
     *
     * pgjdbc's socket bounds have ONE-SECOND granularity, so anything
     * below 1000 still buys a one-second floor on those two; the
     * Hikari and statement bounds honour the millisecond value.
     */
    val healthProbeTimeoutMs: Long = env("HOGLAKE_HEALTH_PROBE_TIMEOUT_MS", "2000").toLong(),
    /**
     * Netty's CALL group: the threads Ktor starts a call on, before
     * `installBlockingDispatch` hands the blocking part off (#218).
     *
     * Ktor's default for this group is `parallelism` exactly — that is,
     * `Runtime.getRuntime().availableProcessors()` — so on the
     * production pod's ONE CPU it is a single thread named
     * `eventLoopGroupProxy-4-1`: the thread every request in the
     * incident log ran on, and the thread a 30 s commit-lock wait held
     * while three `/healthz` probes timed out and liveness killed the
     * pod. The blocking dispatcher is the fix; this is the floor that
     * keeps the fix from having a single thread in front of it, so a
     * probe's dispatch — and a `/metrics` scrape, which shares the same
     * bypass and is real CPU work on this thread — cannot queue behind
     * another call's.
     *
     * [DEFAULT_NETTY_GROUP_SIZE] is a FLOOR, not a replacement: it is a
     * `max` over the SAME `availableProcessors` Ktor reads, so a pod
     * with more CPUs than the floor keeps Ktor's own sizing. Both
     * readings move together if the container's CPU allocation or
     * `-XX:ActiveProcessorCount` changes; what the floor fixes is only
     * the small end.
     *
     * The WORKER group (Netty's IO event loops) is deliberately left at
     * Ktor's own default, which is `parallelism / 2 + 1` — NOT the same
     * formula as this one, so applying this floor there would RAISE the
     * worker count on every pod with more than two CPUs. Those threads
     * do socket IO, they are not where blocking work or the probe
     * handler runs, and #218 produced no evidence about them; changing
     * their number would be an unmeasured change to Netty's own
     * scheduling, smuggled in beside a fix.
     */
    val nettyCallGroupSize: Int =
        env("HOGLAKE_NETTY_CALL_GROUP_SIZE", DEFAULT_NETTY_GROUP_SIZE.toString()).toInt(),
    /** S3/MinIO endpoint for the hydrator; empty = AWS default resolution. */
    val s3Endpoint: String = env("HOGLAKE_S3_ENDPOINT", ""),
    val s3Region: String = env("HOGLAKE_S3_REGION", "us-east-1"),
    val s3AccessKey: String = env("HOGLAKE_S3_ACCESS_KEY", ""),
    val s3SecretKey: String = env("HOGLAKE_S3_SECRET_KEY", ""),
    val s3PathStyle: Boolean = env("HOGLAKE_S3_PATH_STYLE", "true").toBoolean(),
    /**
     * Hydrator poll interval; 0 disables the background loop (tests drive
     * it directly).
     *
     * Fifteen minutes, not seconds: hydration is a backfill for files
     * registered WITHOUT stats, and every writer in the fleet ships its
     * own footer, so the queue is empty in the steady state and a fast
     * poll only records that it was empty. The cost of the interval is
     * the delay before a stats-less file becomes prunable, which is a
     * backfill's latency rather than a reader's.
     */
    val hydratorIntervalMs: Long = env("HOGLAKE_HYDRATOR_INTERVAL_MS", "900000").toLong(),
    /**
     * Cap on the hydrator's whole-object fallback fetch (used when a
     * registration has no usable footer_size): a larger file is marked
     * 'failed' (structural) instead of being buffered on the heap —
     * the OOM guard. Default 256 MiB.
     */
    val hydratorMaxWholeObjectBytes: Long =
        env("HOGLAKE_HYDRATOR_MAX_WHOLE_OBJECT_BYTES", "${256L * 1024 * 1024}").toLong(),
    /**
     * Expiry sweep interval; 0 disables. Sweeps are incremental (bounded
     * per run).
     *
     * Hourly: retention is expressed in hours and days, so nothing
     * becomes expirable on a minute's notice, and a sweep that finds
     * nothing is the only thing a faster cadence buys.
     */
    val expiryIntervalMs: Long = env("HOGLAKE_EXPIRY_INTERVAL_MS", "3600000").toLong(),
    /**
     * Max SNAPSHOTS expired per sweep per catalog (incremental expiry).
     *
     * Unchanged in meaning by the two-phase sweep: it bounds the floor
     * advance, which is what bounds the snapshot range delete and its
     * `hog_snapshot_change` cascade — the one term still inside the
     * commit-lock transaction. It never bounded the FILE rows (those are
     * set by what compaction retired in the window, not by how far the
     * floor moved); `HOGLAKE_EXPIRY_PURGE_PAGE` is that bound.
     */
    val expiryBatchSize: Int = env("HOGLAKE_EXPIRY_BATCH", "10000").toInt(),
    /**
     * File rows one page of expiry's phase-B purge deletes.
     *
     * PHASE B IS THE PART THAT IS NOT UNDER THE COMMIT LOCK: the sweep
     * advances the floor in one short transaction, then deletes the file
     * rows below it in pages, each page its own transaction under its
     * own `statement_timeout` (`ExpiryService.PURGE_STATEMENT_TIMEOUT`).
     * This is the row bound the 2026-10-01 prod-us incident was missing
     * — one statement deleted a whole compaction wave (12,288 rows plus
     * ~26 `hog_file_column_stats` rows each) under the lock, 17-25 s of
     * hold per minute for sixteen hours.
     *
     * 1,000, the figure the drained-ledger purge uses, and the
     * arithmetic that makes it the right order of magnitude: at the
     * 700-870 us per file this statement measured on prod-us a page is
     * ~0.7-0.9 s, two orders inside its 5 s statement bound, and
     * `HOGLAKE_EXPIRY_PURGE_BUDGET_MS` then buys ~10-14 pages per sweep.
     *
     * The per-row cost is NOT a constant of the code — it is the row
     * plus its cascade, so a 200-column table costs several times a
     * 25-column one per row. That is why this is a STARTING size rather
     * than a promise: a page that hits its statement bound is HALVED and
     * retried within the same run (`ExpiryService.walk`), down to a page
     * of one, and the halvings are counted in
     * `hoglake_expiry_halvings_total{phase="purge"}`. Retirement, whose
     * batch walk is otherwise this shape, deliberately does NOT do this
     * (#263) — see Config.retirementBatch. A
     * standing rate on that series means this value is too large for the
     * tables the purge is meeting; `hoglake_expiry_purge_failures_total`
     * means even a page of one could not finish, which is not a page-size
     * problem.
     */
    val expiryPurgePage: Int =
        env(
            "HOGLAKE_EXPIRY_PURGE_PAGE",
            "${com.posthog.hoglake.service.ExpiryService.PURGE_PAGE}",
        ).toInt(),
    /**
     * Wall clock expiry's phase-B purge may spend per sweep, per
     * catalog.
     *
     * TEN SECONDS, and it is a THROUGHPUT knob rather than a safety one:
     * nothing waits on phase B (no lock, one pooled connection), so the
     * budget only decides how fast the backlog drains.
     *
     * THE RATE IS PER CATALOG AND THE LOOP IS FIXED DELAY, which is the
     * arithmetic an earlier version of this comment got wrong.
     * `BackgroundLoops.register` runs `body()` and THEN waits
     * `HOGLAKE_EXPIRY_INTERVAL_MS`, and `runOnceAllCatalogs` sweeps
     * catalogs serially, so the period is `interval + the sum of every
     * catalog's sweep` rather than the interval. At ~1,000 rows a page
     * and ~0.8 s a page a budget-spending sweep is ~10,000-14,000 rows,
     * so with K catalogs behind on one maintenance pod the hot catalog's
     * rate is `12,000 x 60 / (15 + 10K)` rows a minute: ~29k at K=1,
     * ~16k at K=3, ~11k at K=5. Compaction retires ~12,288 files per
     * ~110 s wave on prod-us, i.e. ~6,700 a minute, so the margin is
     * ~4x at K=1 and gone somewhere around K=8 — at which point the
     * symptom is a standing `purge_truncated` and a rising
     * `hoglake_expiry_purge_remaining`, and the lever is fewer catalogs
     * per pod or a bigger budget rather than a bigger page.
     * server/README.md §Retention carries the same derivation for an
     * operator reading it there.
     *
     * The budget is checked BETWEEN pages, so the real bound is one page
     * over it; `ExpiryService.PURGE_STATEMENT_TIMEOUT` is what bounds
     * that page, which is the half a wall budget cannot do. A sweep that
     * spends the whole budget reports `purge_truncated` with the rows it
     * left (`purge_remaining`), so a purge falling behind is visible in
     * the ledger rather than inferred from a growing table.
     *
     * 0 is legal and means "advance the floor, purge nothing": the
     * rows stay eligible for the next sweep. It is the shape a test uses
     * to make the truncation observable without sleeping, and it is not
     * a configuration anyone should deploy — it turns the purge off while
     * leaving every other surface reporting a healthy sweep.
     */
    val expiryPurgeBudgetMs: Long =
        env("HOGLAKE_EXPIRY_PURGE_BUDGET_MS", "${com.posthog.hoglake.service.ExpiryService.PURGE_BUDGET_MS}").toLong(),
    /**
     * Cleanup drain interval; 0 disables.
     *
     * Half-hourly: the queue is fed by expiry and compaction, which are
     * themselves paced, and a queued object costs only storage until it
     * drains. Draining sooner buys nothing a reader can observe.
     */
    val cleanupIntervalMs: Long = env("HOGLAKE_CLEANUP_INTERVAL_MS", "1800000").toLong(),
    /** Queue entries drained per cleanup run, per catalog. */
    val cleanupBatchSize: Int = env("HOGLAKE_CLEANUP_BATCH", "2000").toInt(),
    /**
     * Rows per drain sub-batch, and therefore rows per CLAIM: the drain
     * claims a sub-batch in one short transaction, works it with no
     * transaction open, and settles it in another. It takes no catalog
     * lock at any point.
     *
     * 1,000 is S3's own ceiling on ONE DeleteObjects request, so a
     * default sub-batch whose paths share a bucket is one round trip.
     * It was 25, sized for a drain that issued a HEAD and a DELETE per
     * path UNDER THE COMMIT LOCK: each hold ran ~3.2 s and a 2,000-row
     * run spent ~255 s holding the lock, which took commit latency from
     * 200-400 ms to 12-22 s and got both API pods liveness-killed
     * (gigahog-prod-us, 2026-09-24). The lock is gone — what remains is
     * that keys past 1,000, or spread across buckets, chunk into more
     * calls inside the same sub-batch, so raising this past the ceiling
     * buys nothing.
     *
     * It does not apply to `compaction_staging` rows, which settle in
     * their own sub-batches of 25 because they cost a HEAD and a DELETE
     * each — see CleanupService.STAGING_SUB_BATCH — and a claim of
     * nothing but tickets is the one shape that can approach
     * HOGLAKE_CLEANUP_CLAIM_LEASE_SECONDS (~128 s at the measured 64 ms
     * per round trip, against 900 s).
     */
    val cleanupSubBatchSize: Int = env("HOGLAKE_CLEANUP_SUB_BATCH", "1000").toInt(),
    /**
     * How long a cleanup worker's claim on a queue row is honoured
     * before any worker may reclaim it. Default 900 s.
     *
     * A LEASE, not a lock. The drain claims rows in one short
     * transaction (`claimed_at`/`claimed_by`, V21), commits it before
     * the first object-store call, and settles in another — so a worker
     * that dies in between leaves rows claimed, and this is how long
     * they wait before somebody retries them. Wrong in either direction
     * costs work and never an object: too short and two workers issue
     * the same idempotent delete while the loser's settle is refused by
     * the `claimed_by` fence (counted `settled_elsewhere`); too long and
     * a killed pod's rows idle for a lease.
     *
     * CLEANUP'S ALONE. Compaction does NOT read it: its group commit
     * refuses any staging ticket cleanup has touched at all
     * (`claimed_at IS NULL AND attempts = 0`), because a LAPSED claim
     * does not mean the object survived — it means nobody knows. So there
     * is no number the two services have to agree about, and lengthening
     * the lease cannot make compaction register a path a drain deleted.
     */
    val cleanupClaimLeaseSeconds: Long = env("HOGLAKE_CLEANUP_CLAIM_LEASE_SECONDS", "900").toLong(),
    /**
     * Parallel cleanup workers per run per catalog. Default **1 — the
     * sequential drain this server has always run**, so a deployment
     * that sets nothing changes in no way.
     *
     * Each worker runs its own claim -> work -> settle loop over its own
     * sub-batches until the run's HOGLAKE_CLEANUP_BATCH is consumed or
     * the queue is empty for the catalog. They need no coordination: the
     * claim's `FOR UPDATE SKIP LOCKED` partitions the queue between
     * workers, and between replicas, for free.
     *
     * THE CADENCE IS THE THREE KNOBS TOGETHER, and the arithmetic is
     * worth doing before touching any of them. [cleanupBatchSize] is a
     * budget PER WORKER AND PER REASON — bulk rows and
     * `compaction_staging` tickets are claimed by separate statements and
     * spend separate budgets, so the bulk backlog cannot starve the
     * tickets — which makes the ceiling
     * `HOGLAKE_CLEANUP_WORKERS x 2 x HOGLAKE_CLEANUP_BATCH` rows per run.
     * The rate to size against is the bulk half, since staging tickets
     * arrive at compaction's group rate rather than expiry's:
     *
     *     rows/h = workers x batch x 3600000 / HOGLAKE_CLEANUP_INTERVAL_MS
     *
     * At the compiled defaults (1 x 2,000 per 30 min) that is **4,000
     * rows/h**, which is a deliberately conservative floor and nowhere
     * near a busy catalog's arrivals (~190k/h on gigahog-prod-us, with a
     * 2.6M-row backlog as of 2026-09-29). THE DEFAULTS DO NOT CLEAR A
     * BACKLOG; the values that do live in the chart, not in this repo,
     * and they are a pair — a batch large enough to keep the workers busy
     * and an interval near a run's own duration. A run's duration is set
     * by the reference check: 19 s cold and tens of ms warm per 1,000
     * paths, measured on gigahog-prod-us, so 4 workers x 1,000 rows is
     * ~20 s cold and the matching interval is ~30 s.
     *
     * A worker's rate is one sub-batch per (reference check + one
     * DeleteObjects call), and those are independent per worker, so N
     * workers is close to N times the rate until the database's random
     * reads saturate — sub-linear past two or three on one RDS instance,
     * which is the reason to raise this knob with a measurement rather
     * than by analogy.
     *
     * A RUN'S DURATION IS THE LONGER OF THE TWO ARMS, and at the defaults
     * that is the STAGING one, not bulk: 2,000 tickets at 25 per claim is
     * 80 claims of 50 round trips, ~300 s at the measured 64 ms, against
     * the bulk arm's 2 sub-batches of ~20 s cold. So an interval derived
     * from the bulk arm alone under-counts a run that has tickets to
     * drain; size the interval against whichever arm the catalog's queue
     * actually holds (on a queue of ordinary expiry rows, bulk; while the
     * ~9.4k orphaned tickets clear, staging).
     *
     * A WORKER IS A POOLED CONNECTION FOR THE LENGTH OF ITS REFERENCE
     * CHECK, and that is what sizes the pool: the check is the 19 s
     * statement this whole change exists to get OFF the commit lock, and
     * it is held on a pooled handle for its whole duration. The claim and
     * the settle are milliseconds; the check is not. That is why the boot
     * refusal below is AGGREGATE with compaction's parallel groups — the
     * two together must leave the foreground its reserve, because a
     * writer that cannot get a CONNECTION fails with a Hikari timeout (a
     * 500) instead of the typed, retryable CommitQueueTimeout (503 +
     * Retry-After) the admission contract promises, with nothing in the
     * 500 naming the knob.
     *
     * EACH LOOP IS PRICED WHERE IT RUNS. Both loops are per-workload —
     * `BackgroundLoops.register` returns early at `intervalMs <= 0` — so
     * the refusal prices `compactionParallelGroups` only when compaction's
     * interval is non-zero and this knob only when cleanup's is. A pod
     * that runs neither draws neither, and refusing it would refuse a draw
     * that cannot happen there.
     *
     * THE ONE DELIBERATELY UNPRICED CONNECTION is the manual endpoint's.
     * `POST /v1/maintenance/cleanup` runs this same drain on whichever pod
     * serves it, including one whose loop is off, so the SERVICE CLAMPS
     * that path to ONE worker (see `CleanupService`'s `loopEnabled`) and
     * the arithmetic accepts that one connection rather than pricing four
     * on every pod in the fleet. Per workload, why it is safe:
     * **gigahog-server** pods run neither loop and are the only pods
     * behind the ingress, so a manual POST lands where the priced draw is
     * 0 and one connection is trivially available; the **maintenance**
     * pods run compaction but serve no ingress, so a manual run reaches
     * them only through a port-forward and then costs exactly one
     * connection above the priced draw, which the clamp bounds.
     *
     * WHAT THE ROLLOUT STILL HAS TO DO: at the maintenance shape (pool 10,
     * reserve 4, compaction on at 6 groups) the budget is exactly spent by
     * compaction, so TURNING CLEANUP ON — even at one worker — needs
     * `HOGLAKE_DB_POOL_SIZE` >= 11, and the four-worker drain-down needs
     * >= 14, or `HOGLAKE_COMPACTION_PARALLEL_GROUPS` comes down for the
     * duration. That is a chart change BEFORE the interval is set, and the
     * boot refusal is what makes forgetting it loud.
     */
    val cleanupWorkers: Int = env("HOGLAKE_CLEANUP_WORKERS", "1").toInt(),
    /**
     * How long the drain leaves a fresh `compaction_staging` ticket
     * alone.
     *
     * The ticket is inserted BEFORE the rewrite starts, so with an empty
     * queue a drain can settle it while the group is still uploading;
     * the group then aborts and re-stages, and between the two the
     * object exists with no ticket naming it. One hour is comfortably
     * longer than a group (~8.5 s on gigahog-prod-us), so the case
     * disappears for every group that finishes, while a ticket left by a
     * group that died is still reclaimed — an hour later, by the same
     * drain.
     *
     * 0 does not disable the predicate; it makes every ticket from an
     * already-committed transaction eligible. There is no upper bound on
     * it any more: the ceiling used to be the verify subsystem's 6 h
     * staging-ticket age (this plus HOGLAKE_CLEANUP_INTERVAL_MS plus the
     * backlog had to stay well under it, or `staging_tickets` alerted on
     * tickets the drain was deliberately leaving alone), and verify was
     * removed in #261. A resumable scrubber that re-adds that check has
     * to re-derive the relationship.
     */
    val cleanupStagingGraceSeconds: Long = env("HOGLAKE_CLEANUP_STAGING_GRACE_SECONDS", "3600").toLong(),
    /**
     * How long drained hog_file_removal rows (the soft-deleted cleanup
     * ledger) are kept before the sweep purges them. Default 30 days.
     */
    val removalLedgerRetentionSeconds: Long =
        env("HOGLAKE_REMOVAL_LEDGER_RETENTION_SECONDS", "${30L * 24 * 60 * 60}").toLong(),
    /**
     * How long hog_maintenance_run rows (the maintenance run ledger)
     * are kept before the cleanup sweep purges them. Default 7 days:
     * at the default loop intervals the ledger sees ~2-3 rows per minute
     * per catalog.
     */
    val maintenanceLedgerRetentionSeconds: Long =
        env("HOGLAKE_MAINTENANCE_LEDGER_RETENTION_SECONDS", "${7L * 24 * 60 * 60}").toLong(),
    /**
     * How long a commit receipt (`hog_commit_receipt`) is kept before the
     * cleanup sweep purges it. Default **7 days**; `<= 0` disables the
     * purge, which is V7's behaviour of keeping every receipt forever.
     *
     * AN INSTANCE SETTING, NOT A CATALOG OPTION, unlike snapshot
     * retention. A receipt's cost is a property of the WRITER's commit
     * rate, the operator tunes it against the database's size, and no
     * tenant-visible behaviour depends on the number as long as it
     * exceeds the replay window below. Making it a catalog option would
     * put a correctness-relevant floor in a tenant-editable field.
     *
     * WHY 7 DAYS. Retention has to exceed any plausible REPLAY, because a
     * replay arriving after its receipt is gone is not an error — it is a
     * commit, and it publishes the same files again as a NEW snapshot, so
     * the table silently doubles those rows
     * (`PurgedReceiptReplayIntegrationTest` pins exactly that outcome). The longest a request can
     * legitimately sit before being retried: pyhoglake holds a prepared
     * payload for at least `snapshot_retention_seconds / 2`, and
     * hedgerow's `PendingStore` keeps one across process restarts with no
     * bound of its own. Seven days is comfortably past both, is a number
     * an operator can reason about ("a week of replays are answered"),
     * and at ~100 bytes per receipt costs ~250 MB at 250 commits/min —
     * against the 58 GiB the same table held when it stored bodies
     * (#240).
     *
     * WHAT 7 DAYS DOES NOT COVER, because it is a judgement and not a
     * derivation: hedgerow's `PendingStore` holds a request in its `work`
     * table until `published()` removes it, with NO TTL, and `recover()`
     * replays whatever is left after any outage. That window is not a
     * function of snapshot retention in either direction, so no floor
     * derived from the catalog can bound it — a payload hedgerow replays
     * more than this many seconds after preparing it will re-publish its
     * files as a new snapshot. THAT IS AN ACCEPTED RISK, taken because
     * the alternative is keeping every receipt forever, which is the
     * 58 GiB this change exists to remove.
     *
     * THE PER-CATALOG FLOOR IS APPLIED AT PURGE TIME, NOT HERE.
     * `CleanupService.purgeCommitReceipts` raises the cutoff to
     * `2 x snapshot_retention_seconds` (capped at
     * `CleanupService.RECEIPT_FLOOR_CEILING_SECONDS`) for any catalog
     * whose retention makes this value too short, because that is the
     * term pyhoglake's shelf life is derived from. It cannot be checked
     * at boot: snapshot retention is a per-catalog option an operator
     * PATCHes at runtime, so a boot-time comparison would pass and then
     * become wrong without anything running again. The floor never
     * shortens this value, only lengthens it for the catalog that needs
     * it — and on a catalog with retention DISABLED, where pyhoglake's
     * shelf life is unbounded, there is no derivable floor at all and
     * this value stands with the same accepted risk as hedgerow's.
     *
     * THE FLOOR ON THE KNOB ITSELF is [MIN_RECEIPT_RETENTION_SECONDS]: a
     * positive value under an hour is refused at boot, because it is
     * indistinguishable from a typo (`3600` meant as days) and its effect
     * — duplicate publications from ordinary client retries — is silent
     * and unrecoverable.
     */
    val receiptRetentionSeconds: Long =
        env("HOGLAKE_RECEIPT_RETENTION_SECONDS", "${7L * 24 * 60 * 60}").toLong(),
    /**
     * Retirement sweep interval; <= 0 disables. Default **0 — OFF**, the
     * position compaction takes, and for the same reason: the
     * loop belongs to ONE workload. A retirement batch takes the
     * per-catalog COMMIT lock, so a sweep running on the API replicas
     * would tax the commit tail they exist to serve. The chart turns it
     * on for the maintenance Deployment.
     *
     * What it does: deletes the file rows of tables that were dropped
     * at or below the catalog's expiry floor, in paced batches, queueing
     * every path for the cleanup drain. Drop itself is O(columns) and
     * leaves those rows alone (TableRepo.markDropped), so this loop is
     * where a dropped table's storage actually goes away.
     *
     * A catalog with NO snapshot retention never retires anything,
     * because its floor never advances and every snapshot below the
     * drop is still readable. That is correct, not a gap: retiring
     * there would delete rows a legal time-travel read can still ask
     * for. The verify subsystem's orphans check used to report that
     * population as an informational count rather than a violation;
     * nothing reports it since #261 removed verify.
     */
    val retirementIntervalMs: Long = env("HOGLAKE_RETIREMENT_INTERVAL_MS", "0").toLong(),
    /**
     * Rows per retirement batch — one transaction, one hold of the
     * per-catalog commit lock.
     *
     * 8,000 rows is a ~160 ms hold at the measured 19.6 us per row
     * (`RetirementCostIntegrationTest`, a fixture with the production
     * 9:1 stats ratio and two partition values per file), or ~250 ms on
     * the slower fixture the design was sized against. Either way it is
     * well inside a quarter of the 30 s commit admission bound, and
     * with HOGLAKE_RETIREMENT_PAUSE_MS of 750 it is an 18-25% duty
     * cycle on the commit lock.
     *
     * The cost per row is NOT a constant of the code: it is the row
     * plus its cascade — the per-column stats rows and the partition
     * values — so a 200-column table costs ~20x a narrow one per row.
     * It is also FLAT IN THIS NUMBER (`RetirementCostIntegrationTest`),
     * which is why the loop does not adapt it: a batch that hits its
     * statement bound rolls back, is counted, and is retried at this
     * same size by the next run, because a cancelled batch is a COLD
     * one and halving it would halve the work with it. The case this
     * value is wrong for — one table failing run after run — is
     * reported by
     * `hoglake_retirement_consecutive_timeouts{catalog,table}` and
     * fixed by lowering this knob ON THE INSTANCE THAT RETIRES THAT
     * CATALOG — it is process-wide, so the lever is the maintenance
     * workload's env and it applies to every catalog that pod retires
     * (#263).
     *
     * At 3,008,849 rows (gigahog-prod-us `main.events_raw`) these
     * defaults are ~376 batches and ~8.7 GB of WAL in total (2,902
     * measured bytes per row, a figure that does not move with the
     * machine), of which the commit lock is held for ~60 s ALTOGETHER.
     *
     * THE WALL CLOCK IS SET BY THE LOOP, NOT BY THE WORK. A batch costs
     * ~160 ms of hold plus a 750 ms pause, and the pause is charged
     * against HOGLAKE_RETIREMENT_RUN_BUDGET_MS because the budget is
     * wall clock — so one 60 s run is ~66 batches, and 376 batches is
     * ~6 RUNS. Elapsed time is therefore
     * `runs x (HOGLAKE_RETIREMENT_INTERVAL_MS + run budget)`: about
     * 12 minutes at a one-minute interval, about 1.6 hours at fifteen.
     * That is the knob to move if a retirement has to finish sooner —
     * not the batch size, which is the commit lock's problem.
     */
    val retirementBatch: Int = env("HOGLAKE_RETIREMENT_BATCH", "8000").toInt(),
    /**
     * Pause between retirement batches, in ms. This is the duty cycle,
     * and it is the knob that decides what a retirement run costs the
     * writers: at the default batch the hold is ~250 ms, so 750 ms of
     * pause means the lock is available three quarters of the time and
     * a foreground commit's expected wait is ~62 ms (its p99 tax is
     * about one hold, ~250 ms).
     *
     * 0 makes the run continuous, which is the old drop's behaviour
     * spread over many transactions: correct, much faster, and not
     * something to do while anything is writing.
     */
    val retirementPauseMs: Long = env("HOGLAKE_RETIREMENT_PAUSE_MS", "750").toLong(),
    /**
     * Wall-clock budget for ONE retirement run, per catalog, in ms.
     *
     * A 50M-row table must be paced by the LOOP INTERVAL rather than by
     * one continuously-held run: without this a single run would work
     * for hours, keep a pooled connection and a session advisory lock
     * for all of it, and make every deploy of the maintenance pod
     * throw away however much of that run was in flight. With it, a
     * run does a minute of work and the next interval continues —
     * there is no cursor to lose, because the victim select is just
     * "what is still live on this dropped table".
     */
    val retirementRunBudgetMs: Long = env("HOGLAKE_RETIREMENT_RUN_BUDGET_MS", "60000").toLong(),
    /**
     * Undrained hog_file_removal rows past which a retirement run
     * declines to start for that catalog.
     *
     * RETIREMENT'S OUTPUT IS CLEANUP'S INPUT: every row it deletes
     * queues a path. Left unpaced against the drain, a 3M-row table
     * converts a bounded metadata problem into a 3M-row queue — ~1.9 GB
     * of ledger at ~631 bytes per undrained row — and the
     * drain is the slower of the two by a wide margin.
     *
     * WHAT 500,000 IS, IN BOTH CONFIGURATIONS, because the drain rate
     * is `HOGLAKE_CLEANUP_BATCH` per `HOGLAKE_CLEANUP_INTERVAL_MS` and
     * the two differ by two orders of magnitude:
     *
     *  - at the STANDING defaults (2,000 per 30 min = 4,000/h) the
     *    ceiling is ~125 hours of drain — which is the point. On a
     *    normally-configured instance this ceiling is not a throttle,
     *    it is a CIRCUIT BREAKER: reaching it means the drain has been
     *    failing or turned off, and retiring more would be filling a
     *    bucket with no bottom;
     *  - at the runbook's EVENT settings (10,000 per 60 s = 600,000/h)
     *    it is ~50 minutes of drain, which is the throttle: retirement
     *    runs ahead, hits the ceiling, waits for the drain, resumes.
     *
     * IT IS CHECKED ONCE PER RUN, so the EFFECTIVE CAP IS ABOUT TWICE
     * THIS NUMBER. A run that starts just under the ceiling is not
     * stopped again until the next one, and at the default batch a
     * 60 s run commits ~528,000 more rows — so 500,000 admits a peak
     * queue of ~1,030,000 undrained rows, about **631 MB** of
     * `hog_file_removal` at the measured 631 bytes per undrained row,
     * not the ~315 MB the number on its own suggests. Checking per
     * BATCH would tighten it to ~the ceiling, and would cost that
     * count — ~100k buffers at three million rows — 376 times per run
     * instead of once. Size storage against the doubled figure.
     *
     * An operator running a large retirement raises the cleanup rate
     * FIRST; this number is what stops a forgotten step from becoming a
     * 1.9 GB queue. It costs ONE count per run — about 100k buffers at
     * 3M queued rows — never one per batch.
     */
    val retirementQueueCeiling: Long =
        env(
            "HOGLAKE_RETIREMENT_QUEUE_CEILING",
            "$DEFAULT_RETIREMENT_QUEUE_CEILING",
        ).toLong(),
    /**
     * Catalog-health gauge sample interval; <= 0 disables the sampler
     * loop.
     *
     * STILL 15 s, AND STILL ON EVERY REPLICA — deliberately left alone
     * by #193, which is a decision rather than an omission.
     * `CatalogMetrics.SAMPLE_SQL` is now ONE pass over the manifest
     * instead of five correlated subqueries per catalog (measured
     * 9,467 -> 1,168 execution buffers on a 60,000-row fixture), but
     * one pass is still a FULL SCAN of `hog_data_file`: there is no
     * index that answers "sum the live rows", and at
     * gigahog-prod-us's ~1.5 GiB manifest that is ~1.5 GiB of buffer
     * traffic every 15 seconds on every pod that registers the loop —
     * which `App.startBackground` does unconditionally.
     *
     * Turning the DEFAULT to 0, the way compaction and
     * retirement default off, would be a one-line change here and a
     * fleet-wide observability regression: every `hoglake_*` gauge,
     * `/v1/info`'s instance totals and the catalogs listing's per-
     * catalog totals are served from this sample, and a deployment
     * that did not know to set the variable would simply go dark. So
     * the default stays and the runbook carries the ops step instead —
     * `HOGLAKE_METRICS_INTERVAL_MS=0` on the API replicas, or >= 300000
     * fleet-wide — which is a chart change somebody makes on purpose.
     *
     * The real fix is a sample that does not scan the manifest at all
     * (the asynchronous summary the maintenance sampler already
     * maintains is the shape); that is a separate change.
     */
    val metricsIntervalMs: Long = env("HOGLAKE_METRICS_INTERVAL_MS", "15000").toLong(),
    /**
     * Commit admission bound (B2): lock_timeout on the commit
     * transaction while it queues on the per-catalog advisory commit
     * lock. Expiry -> typed CommitQueueTimeout -> HTTP 503 with
     * Retry-After (retryable backpressure). 0 disables (unbounded wait).
     */
    val commitLockTimeoutMs: Long =
        env(
            "HOGLAKE_COMMIT_LOCK_TIMEOUT_MS",
            com.posthog.hoglake.commit.CommitService.DEFAULT_COMMIT_LOCK_TIMEOUT_MS.toString(),
        ).toLong(),
    /**
     * Whether a blind append (no `read_snapshot`) carrying
     * `partition_values` is REFUSED with 422 or merely logged at WARN.
     *
     * OFF by default because EVERY PRODUCTION FLUSH IS THIS SHAPE TODAY.
     * Two writers send it:
     *
     *  - **millpond**, the primary one. `prepare_append_files` puts a
     *    read_snapshot on the payload and millpond deletes it again
     *    (`millpond/hoglake.py`, `payload.pop("read_snapshot", None)`),
     *    deliberately: a prepared payload's read_snapshot is frozen, so
     *    the 409 it used to earn when another pod added a column was
     *    permanent, and the only way out was re-uploading. Its tables are
     *    partitioned, so every flush on prod-us is a blind partitioned
     *    append.
     *  - **duckdb-client**, whose append-only commits carry no
     *    read_snapshot (storage/hoglake_transaction.cpp) and which sets
     *    partition values whenever the target has a live spec
     *    (storage/hoglake_insert.cpp). Turning this on today breaks
     *    `INSERT INTO <partitioned table>` through the extension.
     *
     * THE FLIP PRECONDITION, both halves:
     *
     *  1. millpond stops popping the field. That is safe now and was not
     *     before: the refusal is `ddl_since_read_snapshot`, a SUBCLASS of
     *     CommitConflictError, which millpond's `is_retryable` ladder
     *     already treats as retryable — so `reset_caches` re-resolves,
     *     `_commit_prepared` drops the refused payload, and the next
     *     attempt re-prepares and succeeds. The re-upload it was avoiding
     *     is now one flush's worth, not a wedge.
     *  2. duckdb-client sends one for a partitioned append.
     *
     * WHEN TO FLIP IT: on
     * `hoglake_blind_partitioned_appends_total{catalog,namespace,table}`
     * reaching zero and staying there. The WARN beside it fires once per
     * (catalog, table) per pod and then goes quiet forever, so it cannot
     * tell a fixed client from a pod that already logged the line; the
     * counter can, and it keeps counting after the flip.
     *
     * The rule itself is invariant 12 and is not optional — partition
     * values are only valid under the spec they were computed with, and a
     * blind commit has no window in which a spec change could be
     * detected — so this knob is a rollout order, not a policy: the WARN
     * names the offending client and the refusal text, and the flag flips
     * once both writers are fixed.
     */
    val refuseBlindPartitionedAppends: Boolean =
        boolEnv("HOGLAKE_REFUSE_BLIND_PARTITIONED_APPENDS", false),
    /**
     * Compaction sweep interval; default 0 = OFF for now (the manual
     * /maintenance/compact trigger still works). Rate-awareness is by
     * construction: tiny bites (see the batch knobs), never a storm.
     */
    val compactionIntervalMs: Long = env("HOGLAKE_COMPACTION_INTERVAL_MS", "0").toLong(),
    /** The size a compaction group packs to, in one rewrite. */
    val compactionTargetBytes: Long = env("HOGLAKE_COMPACTION_TARGET_BYTES", "${512L * 1024 * 1024}").toLong(),
    /**
     * The MOST files a compaction group is asked to hold to be worth
     * rewriting — a ceiling on the requirement, not a fixed one. A group
     * whose files are too large for this many to fit under the target is
     * judged against what does fit, never fewer than 2.
     *
     * A write-amplification knob. Compaction terminates at any value
     * >= 2, because a group turns N >= 2 files into exactly one and the
     * bucket's file count strictly decreases. What a low value costs is
     * rewriting the same bytes repeatedly on the way to the target:
     * compression puts every output back under the target, so it is a
     * candidate again, and at 2 that converges on the target from below
     * one rewrite at a time — roughly 4x the bytes moved, against about
     * 2x at 5. That is the ladder this replaced, by another name; on
     * gigahog-dev it read as `2 -> 1 files, 121 MiB -> 121 MiB` every
     * couple of minutes on a catalog with no ingest at all.
     *
     * 5 matches Iceberg's `min-input-files`. See
     * CompactionGrouping.groups.
     */
    val compactionMinInputFiles: Int =
        env(
            "HOGLAKE_COMPACTION_MIN_INPUT_FILES",
            "${com.posthog.hoglake.compaction.CompactionGrouping.DEFAULT_MIN_INPUT_FILES}",
        ).toInt(),
    /**
     * Fan-in cap for one group. Closes a group whose bytes would never
     * reach the target, which is what lets a partition of tiny files
     * consolidate at all. See CompactionGrouping.groups.
     */
    val compactionMaxInputFiles: Int =
        env(
            "HOGLAKE_COMPACTION_MAX_INPUT_FILES",
            "${com.posthog.hoglake.compaction.CompactionGrouping.DEFAULT_MAX_INPUT_FILES}",
        ).toInt(),
    /** Groups rewritten per run per catalog — the commit-storm guard. */
    val compactionMaxGroupsPerRun: Int = env("HOGLAKE_COMPACTION_MAX_GROUPS_PER_RUN", "1").toInt(),
    /**
     * How many times a run's own capacity in FILES the planner fetches
     * candidate rows for.
     *
     * A run rewrites at most `HOGLAKE_COMPACTION_MAX_GROUPS_PER_RUN`
     * groups of at most `HOGLAKE_COMPACTION_MAX_INPUT_FILES` files, and
     * the planner's candidate read is bounded by that product times
     * this. The multiplier absorbs the candidates that turn out not to
     * be groupable — a bucket's short remainder, a group a sibling
     * replica claims, a group the sorted row ceiling closes short — so
     * a sweep plans as many groups as it can execute. See
     * CompactionConfig.candidateHeadroom.
     */
    val compactionCandidateHeadroom: Int =
        env(
            "HOGLAKE_COMPACTION_CANDIDATE_HEADROOM",
            "${com.posthog.hoglake.compaction.CompactionConfig.DEFAULT_CANDIDATE_HEADROOM}",
        ).toInt(),
    /**
     * Hard cap on any one table plan's candidate read — the binding
     * term of the planner's budget at these defaults, and the bound on
     * the no-published-generation fallback that has no bucket list to
     * scope by.
     *
     * What matters about it is that it exists: the statement it caps
     * used to read every live file of the table under the target. See
     * CompactionConfig.maxCandidates.
     */
    val compactionMaxCandidates: Int =
        env(
            "HOGLAKE_COMPACTION_MAX_CANDIDATES",
            "${com.posthog.hoglake.compaction.CompactionConfig.DEFAULT_MAX_CANDIDATES}",
        ).toInt(),
    /**
     * The CEILING of a group's fan-in, whatever the file sizes say;
     * `HOGLAKE_COMPACTION_MAX_INPUT_FILES` is its floor.
     *
     * The fan-in cap scales with the size of the files it is capping
     * (`CompactionConfig.effectiveMaxInputFiles`), because a fixed 64 is
     * a file count asked to cap a byte target: at 12 KiB per file a
     * 64-file group rewrites 768 KiB against a 512 MiB target and
     * retires 63 files, which cannot outrun a fleet adding ~3,600 files
     * a minute. This is where the scaling stops, and what it bounds is
     * the per-group resources that scale with the INPUT COUNT rather
     * than with the data — one open reader and parsed footer per input,
     * one row lock per input in the commit tail.
     *
     * 2,048, from `CompactionFanInMeasurement`'s measured commit-lock
     * hold: 33.6 ms per group against a stated 50 ms budget, a 0.90%
     * duty cycle on the lock at 64 groups and 15 sweeps an hour.
     */
    val compactionMaxFanIn: Int =
        env(
            "HOGLAKE_COMPACTION_MAX_FAN_IN",
            "${com.posthog.hoglake.compaction.CompactionConfig.DEFAULT_MAX_FAN_IN}",
        ).toInt(),
    /**
     * How many of a sweep's planned groups are rewritten and committed
     * AT ONCE. **1 (the default) is the sequential sweep this server has
     * always run** — an existing deployment that sets nothing changes in
     * no way.
     *
     * A group's cost is object-store LATENCY, not bytes (measured ~8.5 s
     * per group on gigahog-prod-us whatever the group held), and groups
     * share no input file, so they overlap cleanly: the only
     * serialization point is the per-catalog commit lock, which each
     * group takes for its small metadata transaction alone and never
     * across the rewrite or the upload.
     *
     * Raise it together with three things: the JDBI pool (each in-flight
     * group wants a connection at its staging ticket and its commit),
     * the maintenance pod's CPU (the rewrite is CPU-bound on zstd), and
     * an eye on HOGLAKE_COMPACTION_SORTED_HEAP_BYTES — the sorted path's
     * heap budget is DIVIDED by this value so N concurrent sorted groups
     * cannot exceed what one was allowed, which makes every sorted
     * table's groups proportionally smaller. See
     * CompactionConfig.parallelGroups.
     */
    val compactionParallelGroups: Int =
        env(
            "HOGLAKE_COMPACTION_PARALLEL_GROUPS",
            "${com.posthog.hoglake.compaction.CompactionConfig.DEFAULT_PARALLEL_GROUPS}",
        ).toInt(),
    /**
     * How many of ONE group's input files the rewrite may have open at
     * once — the other half of that fixed per-group cost, and the half
     * that is safe to turn on by default (8).
     *
     * Opening a parquet input costs at least one object-store round trip
     * before any row can be read, and a 64-file group used to pay 64 of
     * them end to end. Merge order is preserved (only the `open`
     * overlaps; the rewrite still consumes inputs in order on one
     * thread) and so is the streaming memory bound — an open-but-unread
     * input holds its parsed footer, not a readahead buffer. See
     * ParquetRewriter.forEachOpenedInput.
     */
    val compactionParallelInputOpens: Int = env("HOGLAKE_COMPACTION_PARALLEL_INPUT_OPENS", "8").toInt(),
    /**
     * Whether a maintainer CLAIMS a compaction group before rewriting
     * it, so a second maintainer's planner skips it
     * (`hog_compaction_claim`, V15). Default on.
     *
     * An OPTIMIZATION, never authorization: correctness against a
     * concurrent rewrite is the plan-to-commit re-verification under the
     * catalog commit lock, with or without this. What it removes is
     * WASTE — two replicas planning the same candidate set rewrote the
     * same groups and threw one of the two away at commit (30 of 34
     * committed groups' worth, in one measured 547 s sweep). Turn it off
     * and that behaviour comes back; nothing else changes.
     */
    val compactionClaimsEnabled: Boolean = boolEnv("HOGLAKE_COMPACTION_CLAIMS_ENABLED", true),
    /**
     * How long a group claim is held before ANY maintainer may reclaim
     * it. A lease, not a lock: there is no heartbeat, so this is also
     * how long a killed maintainer's files stay untouched.
     *
     * The default lease is one hour and covers the whole plan, including
     * queue time. Increase it if a full run can exceed one hour.
     * A short lease permits duplicate work; a long lease delays recovery.
     */
    val compactionClaimTtlSeconds: Long =
        env(
            "HOGLAKE_COMPACTION_CLAIM_TTL_SECONDS",
            "${com.posthog.hoglake.compaction.CompactionConfig.DEFAULT_CLAIM_TTL_SECONDS}",
        ).toLong(),
    /**
     * How long a claim survives once its group has COMMITTED — a short
     * lease, not the rewrite lease above.
     *
     * A committed group's claim is kept rather than deleted, because a
     * sibling maintainer's plan formed before the commit still names its
     * (now dead) inputs and the row turns that maintainer's arrival into
     * a counted skip instead of a wasted rewrite.
     *
     * The quantity it has to cover is how OLD that sibling's plan can
     * be, which is one whole SWEEP: a group costs a measured ~8.5 s, so
     * 64 groups is ~544 s. 600 s covers it with margin and stays inside
     * the full rewrite lease. The cost is rows the planner reads the
     * input-id arrays of — `committed groups per sweep x lease / sweep
     * duration`, about 70 per table at those settings.
     */
    val compactionCommittedClaimTtlSeconds: Long =
        env(
            "HOGLAKE_COMPACTION_COMMITTED_CLAIM_TTL_SECONDS",
            "${com.posthog.hoglake.compaction.CompactionConfig.DEFAULT_COMMITTED_CLAIM_TTL_SECONDS}",
        ).toLong(),
    /**
     * Sorted-path heap derate for NESTED tables: the sorted ROW CEILING
     * of a table with both nested columns and a live sort order is
     * divided by this. The sorted path materializes a whole group to
     * sort it, and a nested row's node count is not knowable from the
     * catalog (list lengths are data) — measured at 30-70x its
     * compressed bytes — so the per-node accounting below cannot see it.
     * 1 disables the derate, which is the setting to reach for only with
     * a heap sized for it. See CompactionConfig.nestedSortExpansion.
     */
    val compactionNestedSortExpansion: Int =
        env(
            "HOGLAKE_COMPACTION_NESTED_SORT_EXPANSION",
            "${com.posthog.hoglake.compaction.CompactionConfig.DEFAULT_NESTED_SORT_EXPANSION}",
        ).toInt(),
    /**
     * How much HEAP one group's sorted-path materialization may take,
     * default 1 GiB. This — not compaction_target_bytes — is the
     * sorted path's bound: the planner converts it to a row ceiling
     * using the live schema's node count, and converts THAT back to a
     * group byte budget using the table's own observed bytes-per-row.
     *
     * The default is the largest value that is safe on the maintenance
     * pod AS IT IS TODAY (4 GiB, so ~2.8 GiB of heap at the image's
     * MaxRAMPercentage=70): worst-case peak ~1260 MiB, 44% of that heap.
     * Raise it only together with the pod's memory — on a bigger pod the
     * group bytes it buys scale linearly (server/README.md has the
     * ladder), and raising it WITHOUT the pod turns a counted refusal
     * back into the OOM it replaced.
     *
     * TEMPORARY. The bound exists only because the sorted rewrite sorts
     * a whole group in memory; an external merge sort removes it
     * entirely. See CompactionConfig.sortedHeapBytes.
     */
    val compactionSortedHeapBytes: Long =
        env(
            "HOGLAKE_COMPACTION_SORTED_HEAP_BYTES",
            "${com.posthog.hoglake.compaction.CompactionConfig.DEFAULT_SORTED_HEAP_BYTES}",
        ).toLong(),
    /**
     * Per-ROW node budget for the compaction rewrite. Bounds one row's
     * materialized object graph, which no group-level budget can; a row
     * above it is refused as invalid_data instead of OOM-ing the
     * process. See ParquetRewriter.DEFAULT_MAX_NODES_PER_ROW.
     */
    val compactionMaxNodesPerRow: Int =
        env(
            "HOGLAKE_COMPACTION_MAX_NODES_PER_ROW",
            "${com.posthog.hoglake.compaction.ParquetRewriter.DEFAULT_MAX_NODES_PER_ROW}",
        ).toInt(),
    /**
     * Compression codec for compaction OUTPUT files: zstd (default),
     * snappy, gzip, lz4_raw or uncompressed, case-insensitive. Every
     * name on that list is readable by all four consumers of these
     * files (DuckDB extension, Trino connector, pyarrow, parquet-java)
     * and implemented on the server's runtime classpath; an unknown one
     * is refused at boot.
     *
     * Not a per-file detail: compaction rewrites a table's rows into
     * target-sized files and then leaves them alone, so this is the
     * codec a compacted table is stored and scanned under from that
     * point on.
     * See ParquetRewriter.OutputCodec for the zstd-over-snappy argument.
     */
    val compactionCodec: String =
        env("HOGLAKE_COMPACTION_CODEC", com.posthog.hoglake.compaction.ParquetRewriter.DEFAULT_CODEC.name.lowercase()),
    /**
     * zstd compression level (1-22) when the codec above is zstd; inert
     * otherwise. Pinned rather than inherited from parquet-java so a
     * library bump cannot move the maintenance pod's CPU budget without
     * a diff. See ParquetRewriter.DEFAULT_ZSTD_LEVEL.
     */
    val compactionZstdLevel: Int =
        env(
            "HOGLAKE_COMPACTION_ZSTD_LEVEL",
            "${com.posthog.hoglake.compaction.ParquetRewriter.DEFAULT_ZSTD_LEVEL}",
        ).toInt(),
    /** Dashboard sampling: one bounded metadata page per tick, persisted between ticks/restarts. */
    val maintenanceSummaryIntervalMs: Long = env("HOGLAKE_MAINTENANCE_SUMMARY_INTERVAL_MS", "1000").toLong(),
    val maintenanceSummaryBatch: Int = env("HOGLAKE_MAINTENANCE_SUMMARY_BATCH", "10000").toInt(),
    val maintenanceSummaryRefreshSeconds: Long = env("HOGLAKE_MAINTENANCE_SUMMARY_REFRESH_SECONDS", "60").toLong(),
) {
    init {
        checkRemovedEnv(System::getenv)
        // Concurrent compaction takes connections out of the pool the
        // FOREGROUND shares, and it holds each one across a commit-lock
        // wait. Refuse a configuration where it could take enough of
        // them to starve writers: a writer that cannot get a CONNECTION
        // fails with a Hikari timeout (a 500) rather than the typed,
        // retryable CommitQueueTimeout (503 + Retry-After) the admission
        // contract promises, and nothing in the 500 says which knob
        // caused it.
        //
        // [FOREGROUND_CONNECTION_RESERVE] is a FLOOR, not a model of
        // demand, and the arithmetic below is only the part that can be
        // checked. What it guarantees is that raising
        // HOGLAKE_COMPACTION_PARALLEL_GROUPS (or HOGLAKE_CLEANUP_WORKERS,
        // which the same refusal covers) cannot by itself leave the
        // pool with nothing: four connections stay outside compaction's
        // reach. It does NOT promise four are enough — the other
        // background loops (hydrator, expiry, cleanup, retirement, the
        // metrics sampler) draw on the same pool, and on a busy instance
        // the foreground wants more than four of its own. An operator
        // raising this knob raises HOGLAKE_DB_POOL_SIZE with it; the
        // check exists so that forgetting to is a boot failure naming
        // both knobs rather than a Hikari timeout during the first busy
        // sweep.
        // A CEILING OF ZERO IS NOT "NO PACING", IT IS "NEVER RUN", and
        // it fails in the worst available way: the run skips, and
        // before #193's stamp reordering it skipped before recording
        // eligibility too, so the verify subsystem's orphans arm could
        // not fire either. Retirement would sit at zero forever while
        // every counter said the system was healthy — and since #261
        // removed verify there is no second opinion at all.
        //
        // `count(*) > 0` is true of any catalog with a single undrained
        // row, which a live catalog always has, so there is no reading
        // of 0 that means anything an operator wants. Refuse it at
        // boot, naming BOTH knobs, because the mistake is a pair: a
        // ceiling of 0 is harmless while the loop is off and fatal the
        // moment somebody turns it on.
        require(retirementIntervalMs <= 0 || retirementQueueCeiling > 0) {
            "HOGLAKE_RETIREMENT_QUEUE_CEILING=0 with HOGLAKE_RETIREMENT_INTERVAL_MS=" +
                "$retirementIntervalMs would disable retirement silently: every run would skip " +
                "on the cleanup-queue check and nothing — not the run ledger, not the metrics " +
                "— would say so. Set a positive ceiling (the default " +
                "is $DEFAULT_RETIREMENT_QUEUE_CEILING) or set HOGLAKE_RETIREMENT_INTERVAL_MS=0 " +
                "to turn the loop off on purpose."
        }
        // A dispatcher of zero threads is a server that accepts every
        // request and serves none of them, forever: the Netty call
        // thread hands the call to a pool with nobody in it and the
        // client waits until it gives up. There is no reading of 0 that
        // means "do not dispatch" — that is what the bypass list is for
        // — so refuse it at boot rather than hang at the first request.
        require(requestThreads >= 1) {
            "HOGLAKE_REQUEST_THREADS=$requestThreads: the blocking request dispatcher needs at " +
                "least one thread (the default is HOGLAKE_DB_POOL_SIZE, currently $dbPoolSize). " +
                "A dispatcher with no threads queues every request forever."
        }
        // Each of the four layers this value configures — Hikari's
        // connectionTimeout, pgjdbc's connectTimeout and socketTimeout,
        // the session statement_timeout — has its own floor, and
        // Hikari's is 250 ms. Below that the probe would take a bound
        // Hikari silently replaces with its own, so the number in the
        // values file would stop being the number in force.
        require(healthProbeTimeoutMs >= MIN_HEALTH_PROBE_TIMEOUT_MS) {
            "HOGLAKE_HEALTH_PROBE_TIMEOUT_MS=$healthProbeTimeoutMs is below Hikari's " +
                "$MIN_HEALTH_PROBE_TIMEOUT_MS ms connectionTimeout floor, which Hikari would " +
                "silently replace with its own default — set at least $MIN_HEALTH_PROBE_TIMEOUT_MS"
        }
        // Netty refuses a group of zero with an IllegalArgumentException
        // from deep inside its own constructor, naming neither the knob
        // nor the value — and it does it AFTER migrations have run.
        require(nettyCallGroupSize >= 1) {
            "HOGLAKE_NETTY_CALL_GROUP_SIZE=$nettyCallGroupSize must be >= 1 (the default is " +
                "max(4, availableProcessors), currently $DEFAULT_NETTY_GROUP_SIZE)"
        }
        // Handler threads above pool connections do not buy concurrency
        // for a handler whose first act is to borrow one: they queue
        // inside HikariPool.getConnection and fail with a 500 after its
        // 5 s connectionTimeout, instead of queueing in the dispatcher
        // where the wait is FIFO, measured
        // (hoglake_request_queue_wait_seconds) and shed with a typed
        // 503 once it passes the admission bound. Raising the
        // dispatcher is therefore a PAIR of knobs, and this is what
        // makes forgetting the second one a boot failure that names
        // both rather than a fleet-wide 500 rate nobody can attribute.
        require(requestThreads <= dbPoolSize) {
            "HOGLAKE_REQUEST_THREADS=$requestThreads exceeds HOGLAKE_DB_POOL_SIZE=$dbPoolSize: " +
                "handler threads past the pool queue inside HikariPool.getConnection and 500 " +
                "after its 5s connectionTimeout instead of queueing in the dispatcher, where a " +
                "wait is measured and shed with a typed 503. Raise HOGLAKE_DB_POOL_SIZE with it."
        }
        // A page of 0 is a purge that deletes nothing forever, and a
        // page of a million is the 2026-10-01 statement back: at the
        // measured 700-870 us per file it would be 12-14 MINUTES in one
        // transaction, so every page would die on
        // ExpiryService.PURGE_STATEMENT_TIMEOUT and the purge would be
        // off with a failure counter nobody had a reason to look at yet.
        // The ceiling is where a page stops fitting its own statement
        // bound with room to spare, not a tuning opinion.
        require(expiryPurgePage in 1..MAX_EXPIRY_PURGE_PAGE) {
            "HOGLAKE_EXPIRY_PURGE_PAGE=$expiryPurgePage must be between 1 and " +
                "$MAX_EXPIRY_PURGE_PAGE (the default is " +
                "${com.posthog.hoglake.service.ExpiryService.PURGE_PAGE}): a page past the " +
                "ceiling cannot finish inside the purge's own " +
                "${com.posthog.hoglake.service.ExpiryService.PURGE_STATEMENT_TIMEOUT} statement " +
                "bound at the measured per-row cost, so every page would fail and the purge " +
                "would stop draining while the sweep still reported an advancing floor."
        }
        // A budget of 0 WITH THE LOOP ON is the retirement-ceiling
        // mistake in another costume: every sweep advances the floor,
        // every ledger row reads healthy, and the file rows below the
        // floor accumulate with only `purge_remaining` saying so. Refuse
        // it at boot naming both knobs; the loop being OFF makes it
        // harmless, which is why that case is allowed.
        // THE SIGN CHECK GOES FIRST, and the order is the whole of the
        // fix: the combined check below reads `=0` in its message, so a
        // NEGATIVE value used to be refused by a message telling the
        // operator their value was zero. Each refusal now reports the
        // value it actually saw.
        require(expiryPurgeBudgetMs >= 0) {
            "HOGLAKE_EXPIRY_PURGE_BUDGET_MS=$expiryPurgeBudgetMs must not be negative (the default " +
                "is ${com.posthog.hoglake.service.ExpiryService.PURGE_BUDGET_MS}; 0 means \"advance " +
                "the floor, purge nothing\" and is only legal with the expiry loop off)"
        }
        require(expiryPurgeBudgetMs > 0 || expiryIntervalMs <= 0) {
            "HOGLAKE_EXPIRY_PURGE_BUDGET_MS=0 with HOGLAKE_EXPIRY_INTERVAL_MS=" +
                "$expiryIntervalMs would advance the expiry floor on every sweep and purge none " +
                "of the file rows below it: the rows (and their stats cascade) would accumulate " +
                "while every ledger row reported an advancing floor. Set a positive budget (the " +
                "default is ${com.posthog.hoglake.service.ExpiryService.PURGE_BUDGET_MS}) or set " +
                "HOGLAKE_EXPIRY_INTERVAL_MS=0 to turn expiry off on purpose."
        }
        require(cleanupWorkers >= 1) {
            "HOGLAKE_CLEANUP_WORKERS=$cleanupWorkers must be at least 1 (1 is the sequential " +
                "drain, and 0 would turn the loop into a no-op with nothing saying so — set " +
                "HOGLAKE_CLEANUP_INTERVAL_MS=0 to turn cleanup off on purpose)"
        }
        // A receipt purged while a client can still replay its request
        // turns that replay into a SECOND publication of the same files,
        // and nothing reports it: the commit succeeds, the snapshot is
        // valid, the rows are duplicated. An hour is not a retention an
        // operator would choose on purpose against a client that holds a
        // prepared payload for half the snapshot window, so a positive
        // value under it is read as a unit mistake and refused. 0 and
        // below are left alone: "never purge" is V7's behaviour and a
        // legitimate choice.
        require(receiptRetentionSeconds <= 0 || receiptRetentionSeconds >= MIN_RECEIPT_RETENTION_SECONDS) {
            "HOGLAKE_RECEIPT_RETENTION_SECONDS=$receiptRetentionSeconds is below the " +
                "$MIN_RECEIPT_RETENTION_SECONDS s floor: a receipt purged inside a client's " +
                "replay window makes the replay publish the same files again as a new snapshot, " +
                "silently. Use at least $MIN_RECEIPT_RETENTION_SECONDS (the default is " +
                "${7L * 24 * 60 * 60}), or 0 to keep every receipt forever."
        }
        // ONE CHECK, BOTH DRAWS, EACH PRICED ONLY WHERE ITS LOOP RUNS.
        //
        // One check, because the two knobs spend the same pool and two
        // independent checks each passed while together they took all of
        // it: at the maintenance shape (pool 10, reserve 4, groups 6)
        // `compactionParallelGroups <= 6` and `cleanupWorkers <= 6` are
        // both satisfied by 6 + 4 = 10 of 10 connections, and the first
        // foreground commit gets a Hikari timeout and a 500 instead of the
        // typed, retryable 503 the admission contract promises — the exact
        // failure both checks said they existed to prevent.
        //
        // Per loop, because both are per-workload:
        // `BackgroundLoops.register` returns early at `intervalMs <= 0`,
        // so a pod with an interval of 0 starts no groups and no workers
        // and has no draw to price. Pricing either one unconditionally
        // refuses a draw that cannot happen on that pod, which is how an
        // earlier version of this check would have crash-looped a whole
        // fleet for a drain that was not running.
        //
        // NEITHER DRAW IS BRIEF where it exists: a compaction group holds
        // a connection across its commit-lock wait, a cleanup worker holds
        // one for its reference check (19 s cold per 1,000 paths on
        // gigahog-prod-us — see [cleanupWorkers]). The reserve is a FLOOR,
        // not a model of demand: what this refusal guarantees is only that
        // raising either knob cannot by itself leave the foreground
        // nothing. The consequence is a ROLLOUT CONSTRAINT rather than a
        // default change — at the maintenance shape compaction alone
        // spends the budget, so turning cleanup on needs the pool raised
        // (or compaction's parallelism lowered) in the chart first, and a
        // boot failure naming both knobs is the right place to learn it.
        //
        // The manual endpoint's one clamped worker is the single
        // deliberately unpriced connection in the arithmetic.
        //
        // WHY THE UNPRICED CONNECTION IS SAFE, per workload, because the
        // answer differs: gigahog-server pods run NEITHER loop (both
        // intervals are 0) and are the only pods behind the ingress, so a
        // manual `POST /v1/maintenance/cleanup` lands where the priced
        // draw is 0 and one connection is trivially available. The
        // maintenance pods run compaction but serve no ingress, so a
        // manual run reaches them only through a port-forward, and then
        // costs exactly ONE connection above the priced draw — which is
        // what `CleanupService`'s clamp to a single worker bounds.
        val compactionDraw = if (compactionIntervalMs > 0) compactionParallelGroups else 0
        val cleanupDraw = if (cleanupIntervalMs > 0) cleanupWorkers else 0
        val loopState =
            if (cleanupIntervalMs > 0) {
                "the cleanup loop is ON, HOGLAKE_CLEANUP_INTERVAL_MS=$cleanupIntervalMs"
            } else {
                "the cleanup loop is OFF (HOGLAKE_CLEANUP_INTERVAL_MS=$cleanupIntervalMs), so its " +
                    "draw is not priced here: POST /v1/maintenance/cleanup still runs the drain on " +
                    "whichever pod serves it, clamped to ONE worker, and that single connection is " +
                    "the one deliberately unpriced draw — free on a server pod (no loops, so the " +
                    "priced draw is 0) and one above the priced draw on a maintenance pod, which " +
                    "serves no ingress and is reachable only by port-forward"
            }
        require(compactionDraw + cleanupDraw <= dbPoolSize - FOREGROUND_CONNECTION_RESERVE) {
            "a compaction draw of $compactionDraw " +
                "(HOGLAKE_COMPACTION_PARALLEL_GROUPS=$compactionParallelGroups, " +
                "HOGLAKE_COMPACTION_INTERVAL_MS=$compactionIntervalMs) plus a cleanup draw of " +
                "$cleanupDraw ($loopState; HOGLAKE_CLEANUP_WORKERS=$cleanupWorkers) needs a " +
                "database pool of at least " +
                "${compactionDraw + cleanupDraw + FOREGROUND_CONNECTION_RESERVE} " +
                "(HOGLAKE_DB_POOL_SIZE is $dbPoolSize): a compaction group holds a pooled " +
                "connection across its commit-lock wait and a cleanup worker holds one across its " +
                "reference check (19 s cold per 1,000 paths on gigahog-prod-us), so the two draw " +
                "on the same pool at the same time. Leaving fewer than " +
                "$FOREGROUND_CONNECTION_RESERVE connections for the foreground turns commit " +
                "backpressure from a typed 503 into a connection-pool timeout. Raise " +
                "HOGLAKE_DB_POOL_SIZE, or lower HOGLAKE_COMPACTION_PARALLEL_GROUPS"
        }
        require(cleanupClaimLeaseSeconds > 0) {
            "HOGLAKE_CLEANUP_CLAIM_LEASE_SECONDS=$cleanupClaimLeaseSeconds must be positive: a " +
                "lease of 0 makes every claim immediately reclaimable, so two workers would " +
                "drain the same row by construction (the default is " +
                "${com.posthog.hoglake.service.CleanupService.CLAIM_LEASE_SECONDS})"
        }
    }

    companion object {
        /**
         * Pooled connections the background loops that hold one for a
         * long statement — HOGLAKE_COMPACTION_PARALLEL_GROUPS and
         * HOGLAKE_CLEANUP_WORKERS, checked TOGETHER — must leave for
         * everything else. See the `require` above: a floor, not a model
         * of demand.
         */
        const val FOREGROUND_CONNECTION_RESERVE = 4

        /**
         * The FLOOR under Netty's CALL group (#218), which Ktor
         * otherwise sizes at `availableProcessors` exactly. Four, not a
         * larger number, because with the blocking work dispatched off
         * these threads they do dispatch, the probe handler and a
         * `/metrics` scrape and nothing else: what the floor buys is
         * that a one-CPU pod has more than one of them, so a probe
         * never waits behind another call. A pod with more CPUs than
         * this keeps Ktor's own sizing. It is NOT applied to the worker
         * group — see [nettyCallGroupSize].
         */
        val DEFAULT_NETTY_GROUP_SIZE: Int = maxOf(4, Runtime.getRuntime().availableProcessors())

        /**
         * Hikari refuses a `connectionTimeout` below 250 ms and
         * substitutes its own 30 s default, so this is the floor the
         * health-probe bound is checked against at boot.
         */
        const val MIN_HEALTH_PROBE_TIMEOUT_MS = 250L

        /**
         * Floor on a POSITIVE [receiptRetentionSeconds]; see the boot
         * check for why the failure it prevents is silent.
         */
        const val MIN_RECEIPT_RETENTION_SECONDS = 3_600L

        /**
         * Ceiling on [expiryPurgePage]; see the boot check for the
         * arithmetic. 50,000 rows at the measured 700-870 us per file is
         * 35-43 s, which is seven times the purge's own 5 s statement
         * bound — so anything at or above this is a page that can only
         * fail, and the whole point of a page is that it finishes.
         */
        const val MAX_EXPIRY_PURGE_PAGE = 50_000

        /**
         * The default `HOGLAKE_RETIREMENT_QUEUE_CEILING`, named here so
         * the boot refusal above can quote it rather than restate the
         * literal the property already carries.
         */
        const val DEFAULT_RETIREMENT_QUEUE_CEILING = 500_000L

        /** Env vars that no longer exist, and what replaced them. */
        private val REMOVED_ENV =
            mapOf(
                "HOGLAKE_COMPACTION_TIER_TARGET" to
                    "compaction no longer uses a geometric size-tier ladder. Use " +
                    "HOGLAKE_COMPACTION_MIN_INPUT_FILES (default 5) and " +
                    "HOGLAKE_COMPACTION_MAX_INPUT_FILES (default 64); the old value of 8 " +
                    "maps to neither.",
            )

        /**
         * Interval knobs of a REMOVED subsystem: a POSITIVE value is
         * refused at boot, `0` and absence are accepted in silence.
         *
         * The middle tier between [REMOVED_ENV] (any value refused) and
         * ignoring a knob outright, and it exists because the chart is
         * not in this repository. `HOGLAKE_VERIFY_INTERVAL_MS` is
         * rendered UNCONDITIONALLY by the gigahog chart's hoglake
         * Deployment, from a `verifyIntervalMs` that `values.schema.json`
         * lists as REQUIRED, so every pod in every environment sets it —
         * and sets it to `"0"`, with no override anywhere under
         * `argocd/gigahog/values/`. A [REMOVED_ENV]-style `require` would
         * therefore refuse every pod over a value that is semantically
         * identical to absence, stalling the rollout (the chart sets
         * `maxUnavailable: 0`, so the old ReplicaSet keeps serving and
         * the Application goes Degraded) until the charts PR merged.
         *
         * SPLITTING ON THE VALUE gets both halves right. `0` means "this
         * loop is off", which is now permanently true, so accepting it
         * silently is honest and costs no log line on any pod. Anything
         * POSITIVE is an operator asking for invariant checks that no
         * longer exist, and answering that with silence — or with a WARN
         * nobody reads — would leave them believing a catalog is being
         * scrubbed when nothing is. That is the one failure mode pure
         * ignoring cannot address, and it is the repo's own idiom to
         * answer it with a typed refusal naming the knob.
         *
         * Each entry graduates into [REMOVED_ENV] once the chart has
         * stopped rendering it at all.
         */
        private val REMOVED_INTERVAL_ENV =
            mapOf(
                "HOGLAKE_VERIFY_INTERVAL_MS" to
                    "the verify subsystem was removed in #261: twelve unbounded full-table " +
                    "checks in one REPEATABLE READ transaction, which timed out at the 60 s " +
                    "statement timeout on every production run and was disabled there. A " +
                    "positive interval would schedule a loop that does not exist, so nothing " +
                    "would be checked and nothing would say so. Set it to \"0\" or remove it; " +
                    "the paged, resumable scrubber that replaces it will bring its own knob.",
            )

        /**
         * Both removed-knob tiers, over an injected lookup so the
         * refusals are unit-testable.
         *
         * [lookup] rather than `System.getenv` directly for one reason:
         * these are `require`s in `init`, and a test cannot set a
         * process environment variable on a modern JVM. Deleting either
         * tier's refusal used to leave the whole suite green, which is
         * the condition this seam exists to end — `ConfigTest` drives it
         * with a map.
         *
         * `init` calls it with `System::getenv` and nothing else does.
         */
        internal fun checkRemovedEnv(lookup: (String) -> String?) {
            // A knob that was REMOVED must not be silently ignored.
            // `env()` is getenv-with-a-default and has no notion of an
            // unknown key, so a values file still pinning
            // HOGLAKE_COMPACTION_TIER_TARGET would boot clean and quietly
            // run different defaults — the geometric ladder it configured
            // is gone, and its old value of 8 is now neither the fan-in
            // nor anything else. Fail at boot and name the replacements.
            REMOVED_ENV.forEach { (key, replacement) ->
                require(lookup(key) == null) {
                    "$key was removed: $replacement"
                }
            }
            // The middle tier: a removed subsystem's INTERVAL knob, which
            // a chart this repository does not own still renders. `0` is
            // accepted in silence (it is what every environment sets, and
            // it says exactly what is true); anything positive is an
            // operator asking for checks that no longer run, and is
            // refused naming itself. See [REMOVED_INTERVAL_ENV].
            REMOVED_INTERVAL_ENV.forEach { (key, explanation) ->
                val raw = lookup(key)?.takeIf { it.isNotBlank() }
                // An unparseable value is refused too: it cannot be read
                // as "off", and treating it as 0 would be guessing on an
                // operator's behalf.
                require(raw == null || (raw.toLongOrNull() ?: 1L) <= 0L) {
                    "$key=$raw is not supported: $explanation"
                }
            }
        }

        private fun env(
            name: String,
            default: String,
        ): String = System.getenv(name)?.takeIf { it.isNotBlank() } ?: default

        /**
         * A boolean knob that REFUSES anything but `true`/`false`,
         * naming itself when it does.
         *
         * `String.toBoolean()` maps every other spelling — `1`, `yes`,
         * `on`, a typo — to FALSE, silently, which for a knob whose
         * default is true means a values file can turn a feature off by
         * being wrong about how to turn it on. `toBooleanStrict()` alone
         * throws a message that names neither the variable nor the
         * value, which in a boot crash is the only thing an operator
         * needs.
         */
        private fun boolEnv(
            name: String,
            default: Boolean,
        ): Boolean {
            val raw = System.getenv(name)?.takeIf { it.isNotBlank() } ?: return default
            return raw.lowercase().toBooleanStrictOrNull()
                ?: throw IllegalArgumentException(
                    "$name must be 'true' or 'false', got '$raw'",
                )
        }

        fun fromEnv(): Config = Config()
    }
}
