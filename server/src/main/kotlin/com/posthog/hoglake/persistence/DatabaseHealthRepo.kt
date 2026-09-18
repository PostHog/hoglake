package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.CommitLockHolder
import com.posthog.hoglake.model.DatabaseActivity
import com.posthog.hoglake.model.DatabaseIndex
import com.posthog.hoglake.model.DatabaseIndexHealth
import com.posthog.hoglake.model.DatabaseServer
import com.posthog.hoglake.model.DatabaseTable
import com.posthog.hoglake.model.ReplicationSlot
import com.posthog.hoglake.persistence.Locks.CATALOG_COMMIT_LOCK_CLASS
import org.jdbi.v3.core.Handle
import java.time.Instant

/**
 * Reads Postgres' own statistics views for the health page.
 *
 * Every query here touches catalog and statistics relations only — never
 * a `hog_*` table. That is the rule that keeps this endpoint safe to
 * call on a busy instance: the numbers come from the stats collector and
 * the catalog, so the cost does not scale with the manifest. In
 * particular there is no bloat estimation by scanning
 * (`pgstattuple`-style): the dead-tuple counts below are the same
 * estimates autovacuum itself schedules against, and they are free.
 *
 * Statistics are cumulative since the last `pg_stat_reset` (or instance
 * start), which is why [DatabaseServer.startedAt] rides along — a
 * flattering cache-hit ratio over four minutes of uptime means nothing.
 */
object DatabaseHealthRepo {
    /**
     * `hog_*` only: the catalog's own tables, not whatever else shares
     * the database.
     *
     * The underscore is LIKE's single-character wildcard, so it must be
     * escaped or this matches `hogX...` too. Mind the string form: this
     * is a normal Kotlin string, where `\\` yields the one backslash SQL
     * needs. In a RAW string (`"""`) the same two characters reach SQL
     * unprocessed, and `hog\\_%` matches nothing at all — silently, since
     * an empty result reads exactly like a healthy instance.
     */
    private const val HOG_TABLES = "s.relname LIKE 'hog\\_%'"

    fun server(handle: Handle): DatabaseServer {
        val checkpoints = checkpoints(handle)
        return handle.createQuery(
            """
            SELECT current_setting('server_version') AS version,
                   current_database() AS database,
                   pg_database_size(current_database()) AS size_bytes,
                   pg_postmaster_start_time() AS started_at,
                   (SELECT count(*) FROM pg_stat_activity
                     WHERE datname = current_database()) AS connections_used,
                   current_setting('max_connections')::int AS connections_max,
                   d.blks_hit, d.blks_read, d.deadlocks, d.xact_commit, d.xact_rollback,
                   age(pd.datfrozenxid) AS xid_age,
                   current_setting('autovacuum_freeze_max_age')::bigint AS xid_freeze_max_age,
                   current_setting('autovacuum') = 'on' AS autovacuum_enabled,
                   d.temp_files, d.temp_bytes
              FROM pg_stat_database d
              JOIN pg_database pd ON pd.datname = d.datname
             WHERE d.datname = current_database()
            """.trimIndent(),
        ).map { rs, _ ->
            val hit = rs.getLong("blks_hit")
            val read = rs.getLong("blks_read")
            DatabaseServer(
                // "16.13 (Debian 16.13-1.pgdg13+1)" -> "16.13": the build
                // suffix is noise in a header badge.
                version = rs.getString("version").substringBefore(' '),
                database = rs.getString("database"),
                sizeBytes = rs.getLong("size_bytes"),
                startedAt = rs.getTimestamp("started_at")?.toInstant(),
                connectionsUsed = rs.getInt("connections_used"),
                connectionsMax = rs.getInt("connections_max"),
                // Undefined rather than 100% before anything has been read.
                cacheHitRatio = (hit + read).takeIf { it > 0 }?.let { hit.toDouble() / it },
                deadlocks = rs.getLong("deadlocks"),
                committed = rs.getLong("xact_commit"),
                rolledBack = rs.getLong("xact_rollback"),
                xidAge = rs.getLong("xid_age"),
                xidFreezeMaxAge = rs.getLong("xid_freeze_max_age"),
                autovacuumEnabled = rs.getBoolean("autovacuum_enabled"),
                tempFiles = rs.getLong("temp_files"),
                tempBytes = rs.getLong("temp_bytes"),
                checkpointsTimed = checkpoints?.first,
                checkpointsRequested = checkpoints?.second,
            )
        }.one()
    }

    /**
     * Checkpoint counters, whose home moved: PG 16 and earlier keep them
     * on `pg_stat_bgwriter` as checkpoints_timed/checkpoints_req, PG 17
     * removed those columns and put num_timed/num_requested on
     * `pg_stat_checkpointer`. Querying either blindly breaks the whole
     * page on the other, so the view is chosen at run time and a version
     * this does not recognise reports null rather than failing — a
     * missing tile beats a 500 on the page you opened because something
     * was already wrong.
     */
    private fun checkpoints(handle: Handle): Pair<Long, Long>? {
        val view =
            handle.createQuery(
                "SELECT to_regclass('pg_stat_checkpointer') IS NOT NULL AS modern",
            ).map { rs, _ -> rs.getBoolean("modern") }.one()
        val sql =
            if (view) {
                "SELECT num_timed AS timed, num_requested AS requested FROM pg_stat_checkpointer"
            } else {
                "SELECT checkpoints_timed AS timed, checkpoints_req AS requested FROM pg_stat_bgwriter"
            }
        return runCatching {
            handle.createQuery(sql)
                .map { rs, _ -> rs.getLong("timed") to rs.getLong("requested") }
                .one()
        }.getOrNull()
    }

    /**
     * Session counts and the ages that matter, with NO query text: see
     * the note on [com.posthog.hoglake.model.DatabaseHealth]. Excludes
     * this backend, which is by definition active and would otherwise
     * always report itself as the busiest session on the instance.
     */
    fun activity(handle: Handle): DatabaseActivity =
        handle.createQuery(
            """
            SELECT count(*) FILTER (WHERE state = 'active') AS active,
                   count(*) FILTER (WHERE state = 'idle') AS idle,
                   count(*) FILTER (WHERE state LIKE 'idle in transaction%') AS idle_in_transaction,
                   count(*) FILTER (WHERE wait_event_type = 'Lock') AS waiting,
                   max(extract(epoch FROM now() - xact_start)) AS longest_transaction,
                   max(extract(epoch FROM now() - state_change))
                       FILTER (WHERE state LIKE 'idle in transaction%') AS longest_idle_in_transaction,
                   max(extract(epoch FROM now() - query_start))
                       FILTER (WHERE wait_event_type = 'Lock') AS longest_wait
              FROM pg_stat_activity
             WHERE datname = current_database()
               AND pid <> pg_backend_pid()
            """.trimIndent(),
        ).map { rs, _ ->
            DatabaseActivity(
                active = rs.getInt("active"),
                idle = rs.getInt("idle"),
                idleInTransaction = rs.getInt("idle_in_transaction"),
                waiting = rs.getInt("waiting"),
                longestTransactionSeconds = rs.getDouble("longest_transaction").takeUnless { rs.wasNull() },
                longestIdleInTransactionSeconds =
                    rs.getDouble("longest_idle_in_transaction").takeUnless { rs.wasNull() },
                longestWaitSeconds = rs.getDouble("longest_wait").takeUnless { rs.wasNull() },
            )
        }.one()

    fun tables(handle: Handle): List<DatabaseTable> =
        handle.createQuery(
            """
            SELECT s.relname,
                   s.n_live_tup, s.n_dead_tup,
                   pg_table_size(s.relid)
                       - COALESCE(pg_total_relation_size(c.reltoastrelid), 0) AS table_bytes,
                   pg_indexes_size(s.relid) AS index_bytes,
                   COALESCE(pg_total_relation_size(c.reltoastrelid), 0) AS toast_bytes,
                   s.seq_scan, COALESCE(s.idx_scan, 0) AS idx_scan,
                   s.last_vacuum, s.last_autovacuum, s.last_analyze, s.last_autoanalyze,
                   s.autovacuum_count
              FROM pg_stat_user_tables s
              JOIN pg_class c ON c.oid = s.relid
             WHERE $HOG_TABLES
             ORDER BY pg_total_relation_size(s.relid) DESC, s.relname
            """.trimIndent(),
        ).map { rs, _ ->
            DatabaseTable(
                name = rs.getString("relname"),
                liveTuples = rs.getLong("n_live_tup"),
                deadTuples = rs.getLong("n_dead_tup"),
                tableBytes = rs.getLong("table_bytes"),
                indexBytes = rs.getLong("index_bytes"),
                toastBytes = rs.getLong("toast_bytes"),
                seqScans = rs.getLong("seq_scan"),
                indexScans = rs.getLong("idx_scan"),
                lastVacuum = rs.getTimestamp("last_vacuum")?.toInstant(),
                lastAutovacuum = rs.getTimestamp("last_autovacuum")?.toInstant(),
                lastAnalyze = rs.getTimestamp("last_analyze")?.toInstant(),
                lastAutoanalyze = rs.getTimestamp("last_autoanalyze")?.toInstant(),
                autovacuumCount = rs.getLong("autovacuum_count"),
            )
        }.list()

    /**
     * Index scan counts and sizes. Primary keys and unique constraints
     * are included deliberately even though they can never be dropped —
     * a zero-scan unique index is still enforcing a constraint, and
     * seeing that stated beats an operator inferring it from absence.
     */
    fun indexes(handle: Handle): List<DatabaseIndex> =
        handle.createQuery(
            """
            SELECT s.relname, s.indexrelname, COALESCE(s.idx_scan, 0) AS idx_scan,
                   pg_relation_size(s.indexrelid) AS size_bytes,
                   i.indisunique OR i.indisprimary AS constraint_backing
              FROM pg_stat_user_indexes s
              JOIN pg_index i ON i.indexrelid = s.indexrelid
             WHERE $HOG_TABLES
             ORDER BY pg_relation_size(s.indexrelid) DESC, s.indexrelname
            """.trimIndent(),
        ).map { rs, _ ->
            DatabaseIndex(
                table = rs.getString("relname"),
                name = rs.getString("indexrelname"),
                sizeBytes = rs.getLong("size_bytes"),
                scans = rs.getLong("idx_scan"),
                constraintBacking = rs.getBoolean("constraint_backing"),
            )
        }.list()

    /**
     * Holders of and waiters for hoglake's per-catalog commit locks.
     *
     * Postgres splits the single-bigint advisory key into classid (the
     * lock class) and objid (the low 32 bits — the catalog id), so
     * filtering on the class isolates OUR locks from any other advisory
     * use, and the catalog can be named without touching a hog_* row
     * beyond the one-row catalog lookup that gives it a name.
     */
    fun commitLocks(handle: Handle): List<CommitLockHolder> =
        handle.createQuery(
            """
            SELECT l.objid::bigint AS catalog_id,
                   c.name AS catalog,
                   l.pid,
                   l.granted,
                   extract(epoch FROM now() - a.xact_start) AS held_seconds,
                   (SELECT count(*) FROM pg_locks w
                     WHERE w.locktype = 'advisory'
                       AND w.classid = l.classid AND w.objid = l.objid
                       AND NOT w.granted) AS waiters
              FROM pg_locks l
              LEFT JOIN pg_stat_activity a ON a.pid = l.pid
              LEFT JOIN hog_catalog c ON c.catalog_id = l.objid::bigint
             WHERE l.locktype = 'advisory'
               AND l.classid = $CATALOG_COMMIT_LOCK_CLASS
             ORDER BY l.granted DESC, held_seconds DESC NULLS LAST
            """.trimIndent(),
        ).map { rs, _ ->
            CommitLockHolder(
                catalogId = rs.getLong("catalog_id"),
                catalog = rs.getString("catalog"),
                pid = rs.getInt("pid"),
                granted = rs.getBoolean("granted"),
                heldSeconds = rs.getDouble("held_seconds").takeUnless { rs.wasNull() },
                waiters = rs.getInt("waiters"),
            )
        }.list()

    /**
     * Replication slots and the WAL each pins. Every slot on the
     * instance, not only hoglake's: a slot left behind by someone else's
     * Blue/Green upgrade fills the same volume.
     */
    fun replicationSlots(handle: Handle): List<ReplicationSlot> =
        handle.createQuery(
            """
            SELECT slot_name, slot_type, active,
                   pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint AS retained_bytes
              FROM pg_replication_slots
             ORDER BY active, retained_bytes DESC NULLS LAST
            """.trimIndent(),
        ).map { rs, _ ->
            ReplicationSlot(
                name = rs.getString("slot_name"),
                slotType = rs.getString("slot_type"),
                active = rs.getBoolean("active"),
                // Null for a slot that has never reserved WAL.
                retainedWalBytes = rs.getLong("retained_bytes").takeUnless { rs.wasNull() },
            )
        }.list()

    /** Count and age of orphaned two-phase transactions; they pin the xid horizon. */
    fun preparedTransactions(handle: Handle): Pair<Int, Double?> =
        handle.createQuery(
            """
            SELECT count(*) AS n,
                   max(extract(epoch FROM now() - prepared)) AS oldest_seconds
              FROM pg_prepared_xacts
            """.trimIndent(),
        ).map { rs, _ ->
            rs.getInt("n") to rs.getDouble("oldest_seconds").takeUnless { rs.wasNull() }
        }.one()

    /**
     * Indexes Postgres will not use: `indisvalid = false` is what a
     * failed or interrupted CREATE INDEX CONCURRENTLY leaves behind.
     * Read from pg_index rather than the statistics views, because a
     * never-used invalid index looks identical to a never-used valid one
     * in pg_stat_user_indexes.
     */
    fun invalidIndexes(handle: Handle): List<DatabaseIndexHealth> =
        handle.createQuery(
            """
            SELECT t.relname AS table_name, c.relname AS index_name,
                   i.indisvalid, i.indisready
              FROM pg_index i
              JOIN pg_class c ON c.oid = i.indexrelid
              JOIN pg_class t ON t.oid = i.indrelid
              JOIN pg_namespace n ON n.oid = t.relnamespace
             WHERE t.relname LIKE 'hog\_%'
               AND n.nspname = current_schema()
               AND (NOT i.indisvalid OR NOT i.indisready)
             ORDER BY t.relname, c.relname
            """.trimIndent(),
        ).map { rs, _ ->
            DatabaseIndexHealth(
                table = rs.getString("table_name"),
                name = rs.getString("index_name"),
                valid = rs.getBoolean("indisvalid"),
                ready = rs.getBoolean("indisready"),
            )
        }.list()

    /** Wall clock from the server, so ages are computed against ITS clock, not a pod's. */
    fun now(handle: Handle): Instant =
        handle.createQuery("SELECT now()").map { rs, _ -> rs.getTimestamp(1).toInstant() }.one()
}
