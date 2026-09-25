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
    val jdbcUrl: String = env("HOGLAKE_JDBC_URL", "jdbc:postgresql://localhost:5432/hoglake"),
    val dbUser: String = env("HOGLAKE_DB_USER", "hoglake"),
    val dbPassword: String = env("HOGLAKE_DB_PASSWORD", "hoglake"),
    val dbPoolSize: Int = env("HOGLAKE_DB_POOL_SIZE", "10").toInt(),
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
    /** Max snapshots expired per sweep per catalog (incremental expiry). */
    val expiryBatchSize: Int = env("HOGLAKE_EXPIRY_BATCH", "10000").toInt(),
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
     * Rows per drain sub-batch — one transaction, which holds the
     * per-catalog commit lock across its reference check, its
     * object-store calls and its settle.
     *
     * 1,000 is S3's own ceiling on ONE DeleteObjects request, so a
     * default sub-batch whose paths share a bucket is one round trip.
     * It was 25, sized for a drain that issued a HEAD and a DELETE per
     * path: each hold ran ~3.2 s and a 2,000-row run spent ~255 s
     * holding the lock, which took commit latency from 200-400 ms to
     * 12-22 s and got both API pods liveness-killed (gigahog-prod-us,
     * 2026-09-24).
     *
     * What scales the hold is the number of CALLS, not this number:
     * keys past 1,000, or spread across buckets, chunk into more calls
     * inside the same hold, so raising this past the ceiling buys
     * nothing. It does not apply to `compaction_staging` rows, which
     * drain in their own sub-batches of 25 because they cost a HEAD and
     * a DELETE each — see CleanupService.STAGING_SUB_BATCH.
     */
    val cleanupSubBatchSize: Int = env("HOGLAKE_CLEANUP_SUB_BATCH", "1000").toInt(),
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
     * already-committed transaction eligible. The UPPER bound is
     * `/verify`: this plus HOGLAKE_CLEANUP_INTERVAL_MS plus the backlog
     * must stay well under VerifyService's 6 h staging-ticket age, or
     * `staging_tickets` alerts on tickets the drain is deliberately
     * leaving alone.
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
     * Verify sweep interval; <= 0 disables. Default **0 — OFF**, the
     * same position compaction takes: the loop belongs to ONE workload,
     * and which one is an ops decision the chart makes, not a default
     * every replica inherits.
     *
     * The reasoning behind the value the chart sets (one hour on the
     * maintenance workload): a run is eleven metadata-only aggregate
     * queries in ONE read-only REPEATABLE READ transaction that takes no
     * catalog lock, measured at 0.3-1.4 s on dev catalogs including a
     * 95k-file table. The cadence is therefore chosen against what it
     * can DETECT rather than what it costs — an invariant violation is a
     * standing state, a broken row does not heal, so an hour's detection
     * latency changes nothing an operator can act on. A default of one
     * hour HERE would have meant every API replica running that
     * aggregate pass over every catalog against the database that serves
     * its own commit tail, which is the one place it must not run.
     */
    val verifyIntervalMs: Long = env("HOGLAKE_VERIFY_INTERVAL_MS", "0").toLong(),
    /** Catalog-health gauge sample interval; <= 0 disables the sampler loop. */
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
     * 900 s covers a worst-case 64-file group by two orders of magnitude.
     * Wrong in either direction costs only work: too short duplicates a
     * rewrite, too long delays one group by one lease.
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
     * the 900 s rewrite lease. The cost is rows the planner reads the
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
        // A knob that was REMOVED must not be silently ignored. `env()`
        // is getenv-with-a-default and has no notion of an unknown key,
        // so a values file still pinning HOGLAKE_COMPACTION_TIER_TARGET
        // would boot clean and quietly run different defaults — the
        // geometric ladder it configured is gone, and its old value of 8
        // is now neither the fan-in nor anything else. Fail at boot and
        // name the replacements instead.
        REMOVED_ENV.forEach { (key, replacement) ->
            require(System.getenv(key) == null) {
                "$key was removed: $replacement"
            }
        }
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
        // HOGLAKE_COMPACTION_PARALLEL_GROUPS cannot by itself leave the
        // pool with nothing: four connections stay outside compaction's
        // reach. It does NOT promise four are enough — the other
        // background loops (hydrator, expiry, cleanup, verify, the
        // metrics sampler) draw on the same pool, and on a busy instance
        // the foreground wants more than four of its own. An operator
        // raising this knob raises HOGLAKE_DB_POOL_SIZE with it; the
        // check exists so that forgetting to is a boot failure naming
        // both knobs rather than a Hikari timeout during the first busy
        // sweep.
        require(compactionParallelGroups <= dbPoolSize - FOREGROUND_CONNECTION_RESERVE) {
            "HOGLAKE_COMPACTION_PARALLEL_GROUPS=$compactionParallelGroups needs a database pool " +
                "of at least ${compactionParallelGroups + FOREGROUND_CONNECTION_RESERVE} " +
                "(HOGLAKE_DB_POOL_SIZE is $dbPoolSize): each concurrent compaction group holds a " +
                "pooled connection across its commit-lock wait, and leaving fewer than " +
                "$FOREGROUND_CONNECTION_RESERVE for the foreground turns commit backpressure " +
                "from a typed 503 into a connection-pool timeout"
        }
    }

    companion object {
        /**
         * Pooled connections HOGLAKE_COMPACTION_PARALLEL_GROUPS must
         * leave for everything else. See the `require` above: a floor,
         * not a model of demand.
         */
        const val FOREGROUND_CONNECTION_RESERVE = 4

        /** Env vars that no longer exist, and what replaced them. */
        private val REMOVED_ENV =
            mapOf(
                "HOGLAKE_COMPACTION_TIER_TARGET" to
                    "compaction no longer uses a geometric size-tier ladder. Use " +
                    "HOGLAKE_COMPACTION_MIN_INPUT_FILES (default 5) and " +
                    "HOGLAKE_COMPACTION_MAX_INPUT_FILES (default 64); the old value of 8 " +
                    "maps to neither.",
            )

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
