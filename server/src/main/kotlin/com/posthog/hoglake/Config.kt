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
    /** Queue entries drained per cleanup run; S3 deletes sub-batch at 500. */
    val cleanupBatchSize: Int = env("HOGLAKE_CLEANUP_BATCH", "2000").toInt(),
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
    /** Catalog-health gauge sample interval; <= 0 disables the sampler loop. */
    val metricsIntervalMs: Long = env("HOGLAKE_METRICS_INTERVAL_MS", "15000").toLong(),
    /**
     * Commit admission bound (B2): lock_timeout on the commit
     * transaction while it queues on the per-catalog advisory commit
     * lock. Expiry -> typed CommitQueueTimeout -> HTTP 503 with
     * Retry-After (retryable backpressure). 0 disables (unbounded wait).
     */
    val commitLockTimeoutMs: Long = env("HOGLAKE_COMMIT_LOCK_TIMEOUT_MS", "30000").toLong(),
    /**
     * Compaction sweep interval; default 0 = OFF for now (the manual
     * /maintenance/compact trigger still works). Rate-awareness is by
     * construction: tiny bites (see the batch knobs), never a storm.
     */
    val compactionIntervalMs: Long = env("HOGLAKE_COMPACTION_INTERVAL_MS", "0").toLong(),
    /** Final compaction size; intermediate tiers divide this repeatedly by the tier target. */
    val compactionTargetBytes: Long = env("HOGLAKE_COMPACTION_TARGET_BYTES", "${512L * 1024 * 1024}").toLong(),
    /**
     * Geometric tier ratio and maximum fan-in, >= 2. Default 8 gives
     * ...128 KiB -> 1 MiB -> 8 MiB -> 64 MiB -> 512 MiB. Merge only
     * the minimal prefix reaching a quota, then repeat on the remainder.
     */
    val compactionTierTarget: Int = env("HOGLAKE_COMPACTION_TIER_TARGET", "8").toInt(),
    /** Groups rewritten per run per catalog — the commit-storm guard. */
    val compactionMaxGroupsPerRun: Int = env("HOGLAKE_COMPACTION_MAX_GROUPS_PER_RUN", "1").toInt(),
    /**
     * Sorted-path heap derate for NESTED tables: the group byte budget a
     * table with both nested columns and a live sort order is planned
     * under is compaction_target_bytes / this. The sorted path
     * materializes a whole group to sort it, and a nested row's object
     * graph measured 30-70x its compressed bytes, so the raw target is
     * not a heap bound for such a table. 1 disables the derate — which
     * is the setting to reach for only with a heap sized for it.
     * See CompactionConfig.nestedSortExpansion.
     */
    val compactionNestedSortExpansion: Int =
        env(
            "HOGLAKE_COMPACTION_NESTED_SORT_EXPANSION",
            "${com.posthog.hoglake.compaction.CompactionConfig.DEFAULT_NESTED_SORT_EXPANSION}",
        ).toInt(),
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
     * Not a per-file detail: the tier ladder rewrites a table's hot rows
     * once per tier, each output feeding the next tier's input, so this
     * is the codec a fully compacted table is stored and scanned under.
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
    companion object {
        private fun env(
            name: String,
            default: String,
        ): String = System.getenv(name)?.takeIf { it.isNotBlank() } ?: default

        fun fromEnv(): Config = Config()
    }
}
