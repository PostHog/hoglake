package com.posthog.hoglake.service

import com.posthog.hoglake.model.CommitLockHolder
import com.posthog.hoglake.model.DatabaseActivity
import com.posthog.hoglake.model.DatabaseFinding
import com.posthog.hoglake.model.DatabaseHealth
import com.posthog.hoglake.model.DatabaseIndex
import com.posthog.hoglake.model.DatabaseIndexHealth
import com.posthog.hoglake.model.DatabaseServer
import com.posthog.hoglake.model.DatabaseTable
import com.posthog.hoglake.model.FindingSeverity
import com.posthog.hoglake.model.ReplicationSlot
import com.posthog.hoglake.persistence.DatabaseHealthRepo
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked

/**
 * Assembles the database health report and interprets it.
 *
 * The reading is the easy half. [findings] is the part worth having: it
 * states what each number means for a catalog whose commit path is a
 * serialized advisory-lock tail over index probes into the manifest
 * tables. A generic dashboard shows a dead-tuple percentage; what an
 * operator needs is that this schema sets `end_snapshot` on drop,
 * compaction and expiry, so those dead tuples land in the very partial
 * indexes every commit scans.
 *
 * Thresholds are deliberately conservative — this page is read when
 * something already feels wrong, and a screen of yellow teaches people
 * to ignore it.
 */
class DatabaseHealthService(private val jdbi: Jdbi) {
    fun report(): DatabaseHealth =
        jdbi.inTransactionUnchecked { handle ->
            // One transaction, so every number is from one MVCC snapshot:
            // a table list that disagrees with its own index list is
            // worse than a slightly stale one.
            handle.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            val server = DatabaseHealthRepo.server(handle)
            val activity = DatabaseHealthRepo.activity(handle)
            val tables = DatabaseHealthRepo.tables(handle)
            val indexes = DatabaseHealthRepo.indexes(handle)
            val commitLocks = DatabaseHealthRepo.commitLocks(handle)
            val slots = DatabaseHealthRepo.replicationSlots(handle)
            val prepared = DatabaseHealthRepo.preparedTransactions(handle)
            val invalid = DatabaseHealthRepo.invalidIndexes(handle)
            DatabaseHealth(
                server = server,
                activity = activity,
                commitLocks = commitLocks,
                replicationSlots = slots,
                tables = tables,
                indexes = indexes,
                findings =
                    findings(server, activity, tables, indexes, commitLocks, slots, prepared, invalid),
                blindSpots = BLIND_SPOTS,
            )
        }

    internal fun findings(
        server: DatabaseServer,
        activity: DatabaseActivity,
        tables: List<DatabaseTable>,
        indexes: List<DatabaseIndex>,
        commitLocks: List<CommitLockHolder> = emptyList(),
        slots: List<ReplicationSlot> = emptyList(),
        preparedTransactions: Pair<Int, Double?> = 0 to null,
        invalidIndexes: List<DatabaseIndexHealth> = emptyList(),
    ): List<DatabaseFinding> {
        val found = mutableListOf<DatabaseFinding>()

        if (invalidIndexes.isNotEmpty()) {
            found +=
                DatabaseFinding(
                    FindingSeverity.WARN,
                    "invalid_indexes",
                    "${invalidIndexes.size} index(es) are invalid and will never be used",
                    invalidIndexes.joinToString("; ") { "${it.table}.${it.name}" } +
                        ". This is what a failed or interrupted CREATE INDEX CONCURRENTLY " +
                        "leaves behind.",
                    "An invalid index is maintained on every registration and read by " +
                        "nothing: pure write amplification on the commit path, and silent " +
                        "forever — it never appears as a slow query. Note the two failure " +
                        "modes chain: a CONCURRENTLY build is blocked by exactly the " +
                        "idle-in-transaction sessions this page also reports, so the corpse " +
                        "is often downstream of a finding above. REINDEX or drop and rebuild.",
                )
        }

        for (slot in slots.filter { !it.active }) {
            val retained = slot.retainedWalBytes ?: 0
            found +=
                DatabaseFinding(
                    if (retained >= SLOT_WAL_CRITICAL) FindingSeverity.CRITICAL else FindingSeverity.WARN,
                    "inactive_replication_slot",
                    "Replication slot ${slot.name} is inactive and pinning ${bytes(retained)} of WAL",
                    "slot_type=${slot.slotType}, active=false.",
                    "An inactive slot retains WAL until the volume fills, and on a managed " +
                        "instance that volume is not visible from SQL — so this row is the " +
                        "warning you get. RDS Blue/Green upgrades work by logical " +
                        "replication and have left slots behind; the CDC WAL tap will " +
                        "create them deliberately. Drop the slot if nothing is consuming it.",
                )
        }

        val (preparedCount, oldestPrepared) = preparedTransactions
        if (preparedCount > 0) {
            found +=
                DatabaseFinding(
                    FindingSeverity.CRITICAL,
                    "prepared_transactions",
                    "$preparedCount orphaned prepared transaction(s)",
                    "Oldest prepared ${oldestPrepared?.let { duration(it) } ?: "unknown"} ago.",
                    "A prepared transaction holds its locks and pins the xid and vacuum " +
                        "horizons exactly as an open transaction does, but survives a " +
                        "restart and appears in none of the session counts above — so it " +
                        "starves vacuum invisibly to every other finding here. hoglake " +
                        "never uses two-phase commit, so any row is an orphan: COMMIT " +
                        "PREPARED or ROLLBACK PREPARED it.",
                )
        }

        commitLocks.filter { it.granted }.forEach { holder ->
            val held = holder.heldSeconds ?: 0.0
            if (held >= COMMIT_LOCK_HELD_WARN || holder.waiters >= COMMIT_LOCK_WAITERS_WARN) {
                found +=
                    DatabaseFinding(
                        if (held >= COMMIT_LOCK_HELD_CRITICAL) FindingSeverity.CRITICAL else FindingSeverity.WARN,
                        "commit_lock_held",
                        "Commit lock for ${holder.catalog ?: "catalog ${holder.catalogId}"} " +
                            "held ${duration(held)} with ${holder.waiters} waiting",
                        "pid ${holder.pid} has held the per-catalog advisory commit lock for " +
                            "${duration(held)}; ${holder.waiters} session(s) are queued behind it.",
                        "This lock IS the serialization point for every writer to that " +
                            "catalog. While it is held nothing else commits there, and " +
                            "clients see admission timeouts (503) rather than anything " +
                            "pointing at the database. A commit tail is milliseconds; this " +
                            "is not one.",
                    )
            }
        }

        if (!server.autovacuumEnabled) {
            found +=
                DatabaseFinding(
                    FindingSeverity.CRITICAL,
                    "autovacuum_disabled",
                    "Autovacuum is off",
                    "The instance has autovacuum = off.",
                    "Nothing reclaims the dead tuples that expiry, compaction and every " +
                        "end_snapshot update produce, and nothing advances the frozen-xid " +
                        "horizon. Both failures are unbounded and neither is visible until " +
                        "the manifest has already bloated.",
                )
        }

        val xidUsed = server.xidAge.toDouble() / server.xidFreezeMaxAge
        if (xidUsed >= XID_WARN) {
            found +=
                DatabaseFinding(
                    if (xidUsed >= XID_CRITICAL) FindingSeverity.CRITICAL else FindingSeverity.WARN,
                    "xid_wraparound",
                    "Transaction-id age is ${percent(xidUsed)} of the freeze limit",
                    "age(datfrozenxid) is ${server.xidAge} against an " +
                        "autovacuum_freeze_max_age of ${server.xidFreezeMaxAge}.",
                    "A catalog commit is a transaction, so id consumption tracks commit " +
                        "rate directly. At the limit Postgres forces an anti-wraparound " +
                        "vacuum that reads every page of the manifest tables, competing " +
                        "with the commit tail exactly when the write rate is highest.",
                )
        }

        activity.longestIdleInTransactionSeconds?.let { seconds ->
            if (seconds >= IDLE_IN_TRANSACTION_WARN) {
                found +=
                    DatabaseFinding(
                        if (seconds >= IDLE_IN_TRANSACTION_CRITICAL) {
                            FindingSeverity.CRITICAL
                        } else {
                            FindingSeverity.WARN
                        },
                        "idle_in_transaction",
                        "A transaction has been idle for ${duration(seconds)}",
                        "${activity.idleInTransaction} session(s) are open in a transaction " +
                            "and running nothing; the oldest has been that way for " +
                            "${duration(seconds)}.",
                        "An open transaction pins the vacuum horizon for the whole database. " +
                            "Every snapshot expiry deletes and every compaction supersession " +
                            "stays unreclaimable until it ends, so the manifest grows while " +
                            "maintenance reports success.",
                    )
            }
        }

        activity.longestTransactionSeconds?.let { seconds ->
            if (seconds >= LONG_TRANSACTION_WARN) {
                found +=
                    DatabaseFinding(
                        FindingSeverity.WARN,
                        "long_transaction",
                        "Longest open transaction is ${duration(seconds)}",
                        "The oldest transaction on this database started ${duration(seconds)} ago.",
                        "If it is a commit, it is holding that catalog's advisory commit " +
                            "lock and every other writer to the catalog is queued behind it " +
                            "— which surfaces to clients as commit-admission 503s rather " +
                            "than as anything pointing here.",
                    )
            }
        }

        activity.longestWaitSeconds?.let { seconds ->
            if (seconds >= LOCK_WAIT_WARN) {
                found +=
                    DatabaseFinding(
                        FindingSeverity.WARN,
                        "lock_wait",
                        "${activity.waiting} session(s) blocked on locks",
                        "The longest lock wait is ${duration(seconds)}.",
                        "Commits to one catalog serialize on a per-catalog advisory lock by " +
                            "design, so brief waits are the system working. Sustained ones " +
                            "mean the tail is not draining as fast as writers arrive.",
                    )
            }
        }

        for (table in tables.filter { it.totalBytes >= SIGNIFICANT_TABLE_BYTES }) {
            val ratio = table.deadRatio ?: continue
            if (ratio >= DEAD_TUPLE_WARN) {
                found +=
                    DatabaseFinding(
                        if (ratio >= DEAD_TUPLE_CRITICAL) FindingSeverity.CRITICAL else FindingSeverity.WARN,
                        "dead_tuples",
                        "${table.name} is ${percent(ratio)} dead tuples",
                        "${table.deadTuples} dead against ${table.liveTuples} live, in " +
                            "${bytes(table.totalBytes)}. Last autovacuum: " +
                            "${table.lastAutovacuum ?: "never"}.",
                        deadTupleImpact(table.name),
                    )
            }
        }

        for (table in tables) {
            if (table.liveTuples >= ANALYZE_ROW_FLOOR &&
                table.lastAnalyze == null &&
                table.lastAutoanalyze == null
            ) {
                found +=
                    DatabaseFinding(
                        FindingSeverity.WARN,
                        "never_analyzed",
                        "${table.name} has never been analyzed",
                        "${table.liveTuples} live rows and no recorded ANALYZE.",
                        "The planner is costing manifest scans from defaults. This is how a " +
                            "commit path that should be index probes turns into sequential " +
                            "scans under load.",
                    )
            }
        }

        for (table in tables.filter { it.totalBytes >= SIGNIFICANT_TABLE_BYTES }) {
            val total = table.seqScans + table.indexScans
            if (total >= SCAN_FLOOR && table.seqScans.toDouble() / total >= SEQ_SCAN_WARN) {
                found +=
                    DatabaseFinding(
                        FindingSeverity.WARN,
                        "sequential_scans",
                        "${table.name} is being scanned sequentially",
                        "${table.seqScans} sequential scans against ${table.indexScans} index " +
                            "scans on ${bytes(table.totalBytes)}.",
                        "Every read on the commit and scan paths is meant to be an index " +
                            "probe scoped to one catalog or table. Sequential scans here are " +
                            "the shape of a cost that grows with the whole catalog.",
                    )
            }
        }

        val unused =
            indexes.filter {
                it.scans == 0L && !it.constraintBacking && it.sizeBytes >= SIGNIFICANT_INDEX_BYTES
            }
        if (unused.isNotEmpty()) {
            found +=
                DatabaseFinding(
                    FindingSeverity.INFO,
                    "unused_indexes",
                    "${unused.size} index(es) have never been scanned",
                    unused.joinToString("; ") { "${it.name} (${bytes(it.sizeBytes)})" } +
                        ". Counts are cumulative since the last statistics reset.",
                    "Every index is maintained on the commit path, so an unread one is " +
                        "write cost on the hottest path in the system. Check the counter " +
                        "covers a representative period before dropping anything — a " +
                        "changefeed or maintenance index may only be read by a job that " +
                        "has not run yet.",
                )
        }

        server.cacheHitRatio?.let { ratio ->
            if (ratio < CACHE_HIT_WARN) {
                found +=
                    DatabaseFinding(
                        FindingSeverity.WARN,
                        "cache_hit_ratio",
                        "Buffer cache hit ratio is ${percent(ratio)}",
                        "Measured over the whole statistics period; check the instance has " +
                            "been up long enough for this to mean anything.",
                        "The manifest indexes are the hot read set of the commit tail. When " +
                            "they stop fitting in shared_buffers, every commit pays disk " +
                            "latency inside the advisory lock, so the serialized section " +
                            "gets longer for every writer at once.",
                    )
            }
        }

        val connectionUse = server.connectionsUsed.toDouble() / server.connectionsMax
        if (connectionUse >= CONNECTION_WARN) {
            found +=
                DatabaseFinding(
                    FindingSeverity.WARN,
                    "connection_saturation",
                    "${percent(connectionUse)} of connections in use",
                    "${server.connectionsUsed} of ${server.connectionsMax}.",
                    "Exhaustion locks out maintenance sweeps and the health of the instance " +
                        "becomes unobservable at the moment it matters.",
                )
        }

        if (server.deadlocks > 0) {
            found +=
                DatabaseFinding(
                    FindingSeverity.WARN,
                    "deadlocks",
                    "${server.deadlocks} deadlock(s) recorded",
                    "Cumulative since the last statistics reset.",
                    "Writers to one catalog are serialized by a single advisory lock taken " +
                        "up front, which is a lock order by construction. A deadlock means " +
                        "something took locks outside that discipline.",
                )
        }

        if (server.checkpointsRequested != null && server.checkpointsTimed != null) {
            val total = server.checkpointsRequested + server.checkpointsTimed
            if (total >= CHECKPOINT_FLOOR &&
                server.checkpointsRequested.toDouble() / total >= CHECKPOINT_REQUESTED_WARN
            ) {
                found +=
                    DatabaseFinding(
                        FindingSeverity.WARN,
                        "checkpoints_requested",
                        "${percent(server.checkpointsRequested.toDouble() / total)} of checkpoints " +
                            "are WAL-driven, not scheduled",
                        "${server.checkpointsRequested} requested against " +
                            "${server.checkpointsTimed} timed, since the last statistics reset.",
                        "Requested checkpoints mean WAL is hitting max_wal_size before the " +
                            "timer expires. A commit-heavy catalog on a default max_wal_size " +
                            "does this, and each one is an I/O burst competing with the " +
                            "commit tail it was caused by.",
                    )
            }
        }

        if (server.tempFiles >= TEMP_FILE_FLOOR) {
            found +=
                DatabaseFinding(
                    FindingSeverity.INFO,
                    "temp_files",
                    "${server.tempFiles} queries have spilled to disk",
                    "${bytes(server.tempBytes)} written to temporary files since the last " +
                        "statistics reset.",
                    "Sorts and hashes exceeding work_mem spill. Maintenance sweeps over " +
                        "large manifests are the usual cause here, and the spill is on the " +
                        "same volume the WAL is on.",
                )
        }

        return found.sortedByDescending { it.severity.ordinal }
    }

    private fun deadTupleImpact(table: String): String =
        when (table) {
            "hog_data_file" ->
                "Every drop, compaction and expiry sets end_snapshot on rows here, and the " +
                    "live-file index is partial ON (...) WHERE end_snapshot IS NULL — so those " +
                    "updates churn precisely the index the scan planner and commit tail probe."
            "hog_file_column_stats" ->
                "The widest table in the schema (per file, per column). Dead tuples here cost " +
                    "the most pages for the least rows, and it is read whenever bounds are."
            "hog_snapshot", "hog_snapshot_change" ->
                "Expiry deletes these in ranges. Until vacuum reclaims them the changefeed and " +
                    "time-travel probes walk dead rows on every read."
            else ->
                "Reads of this table walk dead tuples until vacuum reclaims them, and the " +
                    "indexes stay bloated meanwhile."
        }

    private fun percent(fraction: Double): String = "${Math.round(fraction * 100)}%"

    private fun bytes(value: Long): String =
        when {
            value >= 1L shl 30 -> "%.1f GiB".format(value.toDouble() / (1L shl 30))
            value >= 1L shl 20 -> "%.1f MiB".format(value.toDouble() / (1L shl 20))
            value >= 1L shl 10 -> "%.1f KiB".format(value.toDouble() / (1L shl 10))
            else -> "$value B"
        }

    private fun duration(seconds: Double): String =
        when {
            seconds >= 3600 -> "%.1f h".format(seconds / 3600)
            seconds >= 60 -> "%.1f min".format(seconds / 60)
            else -> "%.0f s".format(seconds)
        }

    private companion object {
        /**
         * What the page cannot see. Stated on the page itself, because a
         * clean report is otherwise read as "the database is fine" — and
         * the single most common way to lose a Postgres is not in any
         * statistics view.
         */
        val BLIND_SPOTS =
            listOf(
                "Disk fullness is invisible from SQL on a managed instance (RDS): a clean " +
                    "page here says nothing about free space on the volume. Watch it from " +
                    "the provider's metrics.",
                "Orphaned catalog rows — stats for dropped tables, the tax that cost the " +
                    "predecessor 30-50x on commit — are deliberately not counted here, " +
                    "because every query on this page avoids reading hog_* tables. " +
                    "Maintenance owns that check: POST /maintenance/verify.",
                "Counters are cumulative since the last statistics reset, so ratios over a " +
                    "freshly restarted instance mean very little.",
            )

        /** Postgres' own default vacuum trigger is 20% of the table. */
        const val DEAD_TUPLE_WARN = 0.20
        const val DEAD_TUPLE_CRITICAL = 0.40

        /** Below this a percentage is noise: a 200 KiB table's ratio means nothing. */
        const val SIGNIFICANT_TABLE_BYTES = 16L * 1024 * 1024
        const val SIGNIFICANT_INDEX_BYTES = 8L * 1024 * 1024

        const val XID_WARN = 0.5
        const val XID_CRITICAL = 0.8

        /**
         * A commit's whole transaction is milliseconds, so a minute of
         * idle-in-transaction is already not hoglake's own work.
         */
        const val IDLE_IN_TRANSACTION_WARN = 60.0
        const val IDLE_IN_TRANSACTION_CRITICAL = 300.0

        /** Maintenance sweeps legitimately run for minutes; this is past that. */
        const val LONG_TRANSACTION_WARN = 300.0

        /** The commit tail holds its lock for milliseconds. */
        const val LOCK_WAIT_WARN = 10.0

        const val SEQ_SCAN_WARN = 0.5
        const val SCAN_FLOOR = 100L
        const val ANALYZE_ROW_FLOOR = 10_000L

        const val CACHE_HIT_WARN = 0.95
        const val CONNECTION_WARN = 0.8

        /**
         * A slot pinning this much WAL is a disk-space problem rather
         * than a slow consumer. Conservative: on a managed instance the
         * volume is invisible here, so the number has to stand alone.
         */
        const val SLOT_WAL_CRITICAL = 8L * 1024 * 1024 * 1024

        /**
         * The commit tail holds its lock for milliseconds, so a second
         * is already anomalous — but the queue matters more than the
         * duration: one waiter on a brief hold is the system working.
         */
        const val COMMIT_LOCK_HELD_WARN = 5.0
        const val COMMIT_LOCK_HELD_CRITICAL = 60.0
        const val COMMIT_LOCK_WAITERS_WARN = 5

        /** WAL-driven checkpoints, once there are enough to mean it. */
        const val CHECKPOINT_REQUESTED_WARN = 0.5
        const val CHECKPOINT_FLOOR = 20L

        /** Informational only, so the floor is about signal, not severity. */
        const val TEMP_FILE_FLOOR = 100L
    }
}
