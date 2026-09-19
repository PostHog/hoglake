package com.posthog.hoglake.persistence

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.model.MaintenanceRun
import com.posthog.hoglake.model.MaintenanceRunStatus
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.wireObjectMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import java.sql.Types
import java.time.Instant

/**
 * The hog_maintenance_run ledger: one row per maintenance-task run,
 * loop sweep and manual trigger alike (V2__maintenance.sql).
 *
 * Contracts:
 *  - Recording NEVER rides the task's own transaction (the row lands in
 *    its own transaction after the run resolves) and never fails the
 *    task: a recording failure is logged and swallowed. The ledger
 *    observes; it does not participate.
 *  - [recorded] is the funnel for tasks keyed by catalog name
 *    (expiry/cleanup/compaction/verify, manual hydrator rehydrate).
 *    [recordSweepById] is the hydrator loop's per-catalog fan-out: its
 *    sweep is instance-wide, so it records one row per catalog it
 *    claimed files for.
 *  - `result` is serialized with the wire's snake_case shape — the same
 *    JSON the matching POST maintenance-trigger response body carries —
 *    so the ledger reads exactly like the API.
 *  - Reads (last-run-per-task, history) run on the caller's handle: the
 *    status endpoint's REPEATABLE READ snapshot composes them with its
 *    backlog queries.
 */
class MaintenanceRunStore(private val jdbi: Jdbi) {
    private val log = KotlinLogging.logger {}

    /**
     * Run [body], recording the outcome: an 'ok' row with the result
     * payload when [body] returns, a 'failed' row with the error detail
     * when it throws (a validation rejection included — the run happened,
     * its outcome was a client error). The exception is always rethrown.
     */
    fun <T> recorded(
        catalog: String,
        task: MaintenanceTask,
        trigger: MaintenanceTrigger,
        body: () -> T,
    ): T {
        val startedAt = Instant.now()
        try {
            val result = body()
            record(
                catalog,
                task,
                trigger,
                startedAt,
                Instant.now(),
                MaintenanceRunStatus.OK,
                null,
                result,
            )
            return result
        } catch (e: Throwable) {
            record(
                catalog,
                task,
                trigger,
                startedAt,
                Instant.now(),
                MaintenanceRunStatus.FAILED,
                errorText(e),
                null,
            )
            throw e
        }
    }

    /** Insert one row keyed by catalog NAME (a dropped catalog silently records nothing). */
    fun record(
        catalog: String,
        task: MaintenanceTask,
        trigger: MaintenanceTrigger,
        startedAt: Instant,
        finishedAt: Instant,
        status: MaintenanceRunStatus,
        error: String?,
        result: Any?,
    ) {
        runCatching {
            val resultJson = result?.let { RESULT_JSON.writeValueAsString(it) }
            jdbi.useHandleUnchecked { h ->
                h.createUpdate(
                    """
                    INSERT INTO hog_maintenance_run
                        (catalog_id, task, run_trigger, started_at, finished_at, status, error, result)
                    SELECT catalog_id, :task, :trigger, :startedAt, :finishedAt, :status, :error,
                           CAST(:result AS jsonb)
                      FROM hog_catalog WHERE name = :catalog
                    """,
                )
                    .bind("catalog", catalog)
                    .bindLedgerFields(task, trigger, startedAt, finishedAt, status, error, resultJson)
                    .execute()
            }
        }.onFailure { e ->
            log.warn(e) { "maintenance ledger: failed to record $task run for catalog '$catalog'" }
        }
    }

    /** Insert one row keyed by catalog ID (the hydrator sweep's per-catalog fan-out). */
    fun recordSweepById(
        catalogId: Long,
        task: MaintenanceTask,
        startedAt: Instant,
        finishedAt: Instant,
        result: Any?,
        error: Throwable? = null,
    ) {
        runCatching {
            val resultJson = result?.takeIf { error == null }?.let { RESULT_JSON.writeValueAsString(it) }
            jdbi.useHandleUnchecked { h ->
                h.createUpdate(
                    """
                    INSERT INTO hog_maintenance_run
                        (catalog_id, task, run_trigger, started_at, finished_at, status, error, result)
                    VALUES (:catalogId, :task, 'loop', :startedAt, :finishedAt, :status, :error,
                            CAST(:result AS jsonb))
                    """,
                )
                    .bind("catalogId", catalogId)
                    .bind("task", task.wire)
                    .bind("startedAt", startedAt)
                    .bind("finishedAt", finishedAt)
                    .bind("status", if (error == null) "ok" else "failed")
                    .bind("error", error?.let { errorText(it) })
                    .apply {
                        if (resultJson == null) bindNull("result", Types.VARCHAR) else bind("result", resultJson)
                    }
                    .execute()
            }
        }.onFailure { e ->
            log.warn(e) { "maintenance ledger: failed to record $task sweep for catalog_id $catalogId" }
        }
    }

    /** The most recent run per task for [catalogId], on the caller's handle. */
    fun lastByTask(
        h: Handle,
        catalogId: Long,
    ): Map<MaintenanceTask, MaintenanceRun> = lastByTaskAll(h, listOf(catalogId)).values.associateBy { it.task }

    /** The most recent run per (catalog, task) across the instance — the central page's rollup. */
    fun lastByTaskAll(
        h: Handle,
        catalogIds: List<Long>,
    ): Map<Pair<String, MaintenanceTask>, MaintenanceRun> {
        if (catalogIds.isEmpty()) return emptyMap()
        return h.createQuery(
            """
            SELECT r.run_id, c.name AS catalog_name, r.task,
                   r.run_trigger, r.started_at, r.finished_at, r.status, r.error,
                   CAST(r.result AS text) AS result_json
              FROM hog_catalog c
              CROSS JOIN unnest(:tasks::text[]) AS tasks(task)
              CROSS JOIN LATERAL (
                  SELECT * FROM hog_maintenance_run r
                  WHERE r.catalog_id = c.catalog_id AND r.task = tasks.task
                  ORDER BY run_id DESC LIMIT 1
              ) r
             WHERE c.catalog_id = ANY(:ids)
            """,
        )
            .bindArray("ids", Long::class.javaObjectType, catalogIds)
            .bindArray("tasks", String::class.java, MaintenanceTask.entries.map { it.wire })
            .map { rs, _ -> runMapper(rs) }
            .list()
            .associateBy { it.catalog to it.task }
    }

    /** Recent loop-run start times per task for [catalogId], on the caller's handle. */
    fun recentLoopRuns(
        h: Handle,
        catalogId: Long,
    ): Map<MaintenanceTask, List<Instant>> =
        recentLoopRunsAll(h, listOf(catalogId)).entries.associate { (key, value) -> key.second to value }

    /**
     * The start times of the last [LOOP_SAMPLES] LOOP runs per (catalog,
     * task), newest first — the evidence behind the status page's
     * observed cadence. Unordered pairs are absent, never empty.
     *
     * Only tasks that have a loop are asked for: verify has none, so a
     * `run_trigger = 'loop'` row cannot exist for it, and the LATERAL
     * would read every verify row the catalog has to prove it. For the
     * tasks that do loop, loop rows vastly outnumber manual ones, so the
     * index scan stops within a few rows of the newest.
     */
    fun recentLoopRunsAll(
        h: Handle,
        catalogIds: List<Long>,
    ): Map<Pair<String, MaintenanceTask>, List<Instant>> {
        if (catalogIds.isEmpty()) return emptyMap()
        val looping = MaintenanceTask.entries.filter { it.hasLoop }
        return h.createQuery(
            """
            SELECT c.name AS catalog_name, tasks.task, r.started_at
              FROM hog_catalog c
              CROSS JOIN unnest(:tasks::text[]) AS tasks(task)
              CROSS JOIN LATERAL (
                  SELECT started_at FROM hog_maintenance_run r
                  WHERE r.catalog_id = c.catalog_id AND r.task = tasks.task
                    AND r.run_trigger = 'loop'
                  ORDER BY run_id DESC LIMIT :samples
              ) r
             WHERE c.catalog_id = ANY(:ids)
            """,
        )
            .bindArray("ids", Long::class.javaObjectType, catalogIds)
            .bindArray("tasks", String::class.java, looping.map { it.wire })
            .bind("samples", LOOP_SAMPLES)
            .map { rs, _ ->
                Triple(
                    rs.getString("catalog_name"),
                    MaintenanceTask.fromWire(rs.getString("task"))!!,
                    rs.getTimestamp("started_at").toInstant(),
                )
            }
            .list()
            .groupBy({ it.first to it.second }) { it.third }
            // The outer query imposes no order on the LATERAL's rows.
            .mapValues { (_, times) -> times.sortedDescending() }
    }

    /**
     * Newest-first history: runs with run_id < [before] (null = from the
     * latest), optionally restricted to one [task]. The caller asks for
     * limit+1 rows to resolve has_more.
     */
    fun history(
        h: Handle,
        catalogId: Long,
        task: MaintenanceTask?,
        before: Long?,
        limit: Int,
    ): List<MaintenanceRun> =
        h.createQuery(
            """
            SELECT r.run_id, c.name AS catalog_name, r.task, r.run_trigger,
                   r.started_at, r.finished_at, r.status, r.error,
                   CAST(r.result AS text) AS result_json
              FROM hog_maintenance_run r
              JOIN hog_catalog c ON c.catalog_id = r.catalog_id
             WHERE r.catalog_id = :catalogId
               AND (:task::text IS NULL OR r.task = :task)
               AND (:before::bigint IS NULL OR r.run_id < :before)
             ORDER BY r.run_id DESC
             LIMIT :limit
            """,
        )
            .bind("catalogId", catalogId)
            .bindTaskAndBefore(task, before)
            .bind("limit", limit)
            .map { rs, _ -> runMapper(rs) }
            .list()

    /** Instance-wide history (no catalog scope) — the central page's feed. */
    fun historyAll(
        h: Handle,
        task: MaintenanceTask?,
        before: Long?,
        limit: Int,
    ): List<MaintenanceRun> =
        h.createQuery(
            """
            SELECT r.run_id, c.name AS catalog_name, r.task, r.run_trigger,
                   r.started_at, r.finished_at, r.status, r.error,
                   CAST(r.result AS text) AS result_json
              FROM hog_maintenance_run r
              JOIN hog_catalog c ON c.catalog_id = r.catalog_id
             WHERE (:task::text IS NULL OR r.task = :task)
               AND (:before::bigint IS NULL OR r.run_id < :before)
             ORDER BY r.run_id DESC
             LIMIT :limit
            """,
        )
            .bindTaskAndBefore(task, before)
            .bind("limit", limit)
            .map { rs, _ -> runMapper(rs) }
            .list()

    /** Delete rows older than the retention window; the cleanup sweep calls this per catalog. */
    fun purge(
        h: Handle,
        catalogId: Long,
        retentionSeconds: Long,
    ): Int =
        h.createUpdate(
            """
            DELETE FROM hog_maintenance_run
             WHERE catalog_id = :catalogId
               AND started_at < now() - make_interval(secs => :retention)
            """,
        )
            .bind("catalogId", catalogId)
            .bind("retention", retentionSeconds)
            .execute()

    private fun runMapper(rs: java.sql.ResultSet): MaintenanceRun {
        val status = MaintenanceRunStatus.fromWire(rs.getString("status"))
        return MaintenanceRun(
            runId = rs.getLong("run_id"),
            catalog = rs.getString("catalog_name"),
            task =
                MaintenanceTask.fromWire(rs.getString("task"))
                    ?: error("unknown maintenance task in ledger: ${rs.getString("task")}"),
            trigger = MaintenanceTrigger.fromWire(rs.getString("run_trigger")),
            startedAt = rs.getObject("started_at", java.time.OffsetDateTime::class.java).toInstant(),
            finishedAt = rs.getObject("finished_at", java.time.OffsetDateTime::class.java).toInstant(),
            status = status,
            error = rs.getString("error"),
            resultJson = rs.getString("result_json"),
        )
    }

    private fun org.jdbi.v3.core.statement.Query.bindTaskAndBefore(
        task: MaintenanceTask?,
        before: Long?,
    ): org.jdbi.v3.core.statement.Query =
        apply {
            if (task == null) bindNull("task", Types.VARCHAR) else bind("task", task.wire)
            if (before == null) bindNull("before", Types.BIGINT) else bind("before", before)
        }

    private fun org.jdbi.v3.core.statement.Update.bindLedgerFields(
        task: MaintenanceTask,
        trigger: MaintenanceTrigger,
        startedAt: Instant,
        finishedAt: Instant,
        status: MaintenanceRunStatus,
        error: String?,
        resultJson: String?,
    ): org.jdbi.v3.core.statement.Update =
        bind("task", task.wire)
            .bind("trigger", trigger.wire)
            .bind("startedAt", startedAt)
            .bind("finishedAt", finishedAt)
            .bind("status", status.wire)
            .apply {
                if (error == null) bindNull("error", Types.VARCHAR) else bind("error", error)
                if (resultJson == null) bindNull("result", Types.VARCHAR) else bind("result", resultJson)
            }

    companion object {
        /** Cap on recorded error detail — a stack trace is not a ledger row. */
        const val MAX_ERROR_LENGTH = 2000

        /**
         * Loop runs read per (catalog, task) for the observed cadence.
         * Enough gaps for a median to survive one slow sweep, few enough
         * that the read stays a handful of index rows.
         */
        const val LOOP_SAMPLES = 6

        /**
         * Result payloads serialize exactly like the wire: snake_case,
         * ISO-8601 times, absent-when-null — so an operator reading this
         * ledger in psql sees what the API would have shown.
         *
         * The app-wide config itself, not a copy of it. The copy this
         * replaces asserted the same thing in a comment while having
         * quietly lost WRITE_DATES_AS_TIMESTAMPS=false, so a temporal
         * field would have been written as a numeric epoch under a
         * comment promising ISO-8601. No result payload carries one
         * today, which is the only reason that never showed up.
         */
        private val RESULT_JSON: ObjectMapper = wireObjectMapper()

        private fun errorText(e: Throwable): String {
            val cause = generateSequence(e) { it.cause }.take(16).last()
            return (cause.message ?: cause.javaClass.name).take(MAX_ERROR_LENGTH)
        }
    }
}
