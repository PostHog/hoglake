package com.posthog.hoglake.service

import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.VerifyCheck
import com.posthog.hoglake.model.VerifyReport
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.observability.VerifyGauges
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.MaintenanceRunStore
import com.posthog.hoglake.persistence.OffsetRepo
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import java.util.concurrent.ConcurrentHashMap

/**
 * POST /v1/catalogs/{catalog}/maintenance/verify (gaps.md B3, absorbing
 * B4) and the HOGLAKE_VERIFY_INTERVAL_MS background loop: the QE suite's
 * global-invariant SQL promoted to an on-demand AND periodic,
 * metadata-only check. Read-only, one REPEATABLE READ transaction (a
 * consistent MVCC snapshot across the cross-table checks), no catalog
 * lock — verify never blocks writers.
 *
 * Eleven checks, each one query shape per violation class, each one
 * paragraph of [VerifyCheck.description] naming the invariant it
 * enforces in AGENT.md's own words:
 *
 *  1. row_id_tiling     — invariant 2 (row-id ranges tile, never reused)
 *  2. delete_vectors    — invariant 3 (one live DV, monotone, bounded)
 *  3. orphans           — live child rows of a dropped table
 *  4. removal_queue     — invariant 4 (the queue is never authorization)
 *  5. snapshot_density  — invariant 1 (dense snapshot ids)
 *  6. next_row_id       — invariant 2's allocator half
 *  7. expiry_floor      — invariant 5 (expiry passes neither head nor
 *                         the consumer floor; below-floor versioned rows
 *                         are gone)
 *  8. visibility_bounds — invariant 6 (the versioned-row predicate is
 *                         meaningful for every row)
 *  9. offset_release    — #167 (whatever mints a new identity releases
 *                         the old one's consumers)
 * 10. staging_tickets   — #174 (compaction's claim-ticket lifecycle)
 * 11. upload_claims     — #162/#167 (the upload-claim state machine)
 *
 * The first six names are unchanged: they are the ids on the wire.
 *
 * WORK IS BOUNDED. Every violation query runs ONCE, wrapped in
 * `count(*) OVER ()` and `LIMIT [MAX_SAMPLES]`, so the report carries
 * the TRUE violation count while never materializing more than
 * [MAX_SAMPLES] rendered samples per sub-query — a catalog with a
 * million broken rows costs one aggregate pass, not a million strings.
 * (The previous shape listed every violating row and then took 20.)
 */
class VerifyService(
    private val jdbi: Jdbi,
    /**
     * How long an undrained `compaction_staging` ticket for a path the
     * catalog holds no file row for may sit before `staging_tickets`
     * calls it leaked. See [DEFAULT_STAGING_TICKET_MAX_AGE_SECONDS] for
     * where the number comes from; constructor-tunable for tests.
     */
    private val stagingTicketMaxAgeSeconds: Long = DEFAULT_STAGING_TICKET_MAX_AGE_SECONDS,
    /**
     * `HOGLAKE_COMPACTION_TARGET_BYTES` and `HOGLAKE_CLEANUP_INTERVAL_MS`
     * as this process has them, for the `staging_tickets` description
     * alone. A description that quoted the defaults would be wrong on
     * any deployment that tunes them, and the whole point of the field
     * is that a report explains itself.
     */
    private val compactionTargetBytes: Long = DEFAULT_COMPACTION_TARGET_BYTES,
    private val cleanupIntervalMs: Long = DEFAULT_CLEANUP_INTERVAL_MS,
) {
    private val log = KotlinLogging.logger {}

    /** The run ledger; records after the report resolves, never inside it. */
    private val runStore = MaintenanceRunStore(jdbi)

    /**
     * Last failing-check set per catalog, so the loop logs a STANDING
     * violation when the picture changes rather than once an hour
     * forever — the same shape as CompactionService's heap-refusal
     * warning, and for the same reason (an unchanged permanent condition
     * produced 302 identical lines in 17 hours there). Keyed by catalog
     * name; cleared when the catalog passes again.
     */
    private val lastFailure = ConcurrentHashMap<String, String>()

    /**
     * One verify run for [catalog]. [trigger] is MANUAL from the route
     * and LOOP from [runOnceAllCatalogs]; every run, pass or fail, is
     * recorded in the maintenance run ledger, and every run — including
     * a clean one — republishes `hoglake_verify_violations` so an alert
     * keyed on `> 0` sees the pass as a zero rather than as silence.
     */
    fun runOnce(
        catalog: String,
        trigger: MaintenanceTrigger = MaintenanceTrigger.MANUAL,
    ): VerifyReport {
        // The ledger stores the report WITHOUT the descriptions and the
        // caller gets it with them: `recorded` serializes whatever the
        // body returns, so the body returns the stripped copy and the
        // full one is carried out around it. See VerifyReport.forLedger
        // for why (identical constant prose, per row, forever).
        var full: VerifyReport? = null
        runStore.recorded(catalog, MaintenanceTask.VERIFY, trigger) {
            runChecks(catalog).also { full = it }.forLedger()
        }
        return full!!
    }

    /**
     * One pass across every catalog, for the background loop
     * (BackgroundLoops in App.kt). Catalogs are isolated: one catalog's
     * failure — a thrown scan or a failing CHECK — is logged and the
     * rest proceed, so the loop never throws out of an iteration.
     *
     * A catalog whose report FAILS is warned about once per distinct
     * failing-check set (see [lastFailure]): a broken catalog is a
     * state, not an event, and an hourly line repeating the same five
     * check names is how the one line that changed gets missed.
     */
    fun runOnceAllCatalogs(): List<Pair<String, VerifyReport>> =
        runOnceAllCatalogs(jdbi.withHandleUnchecked { h -> CatalogRepo.listAll(h) }.map { it.name })

    /**
     * Same sweep over a caller-supplied catalog list.
     *
     * The seam exists so per-catalog ISOLATION can be asserted rather
     * than waited for: the only way one catalog's scan throws is for its
     * row to disappear between the listing and its own transaction,
     * which is a race no test can schedule. Handed a name that is not
     * there, the loop takes exactly that path — and the assertion is
     * then a direct one (the other catalogs were still reported), not a
     * timeout.
     */
    internal fun runOnceAllCatalogs(names: List<String>): List<Pair<String, VerifyReport>> {
        val results = mutableListOf<Pair<String, VerifyReport>>()
        for (name in names) {
            try {
                val report = runOnce(name, MaintenanceTrigger.LOOP)
                results += name to report
                warnOnce(name, report)
            } catch (e: Exception) {
                // Counted, not just logged. The catch is what keeps one
                // catalog from stopping the sweep, and it is therefore
                // also what hides the failure from the background-loop
                // failure counter; and the gauge below cannot carry the
                // catalog either, since a sweep with no answer for it
                // must not publish a reassuring zero. See
                // Metrics.verifyError.
                Metrics.verifyError(name)
                log.error(e) { "verify failed for catalog '$name'; continuing" }
            }
        }
        // ONE publish per sweep, with the whole row set: the gauge is a
        // MultiGauge refreshed with overwrite, so this is what retires a
        // deleted catalog's series. It is also why only the LOOP
        // publishes — a manual run on a loop-disabled replica would mint
        // a series nothing refreshes. See VerifyGauges.
        VerifyGauges.publish(results)
        return results
    }

    private fun warnOnce(
        catalog: String,
        report: VerifyReport,
    ) {
        val failing = report.checks.filter { it.status != "pass" }
        if (failing.isEmpty()) {
            lastFailure.remove(catalog)
            return
        }
        val signature = failing.joinToString(",") { it.check }
        if (lastFailure.put(catalog, signature) == signature) return
        log.warn {
            "verify found invariant violations in catalog '$catalog': " +
                failing.joinToString(" ") { "${it.check}=${it.violations}" } +
                ". Samples: " +
                failing.joinToString(" | ") { "${it.check}: ${it.samples.joinToString("; ")}" }
        }
    }

    private fun runChecks(catalog: String): VerifyReport =
        Audit.audited(
            "verify",
            catalog,
            null,
            successOutcome = { r -> if (r.status == "pass") "ok" else "invariant_violation" },
            detail = { r ->
                r.checks.joinToString(" ") { "${it.check}=${it.violations}" }
            },
        ) {
            jdbi.inTransactionUnchecked { h ->
                // First statement of the transaction: a consistent
                // read-only MVCC snapshot for every check below.
                h.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                val cat = CatalogRepo.require(h, catalog)
                val checks =
                    listOf(
                        rowIdTiling(h, cat.catalogId),
                        deleteVectors(h, cat.catalogId),
                        orphans(h, cat.catalogId),
                        removalQueue(h, cat.catalogId),
                        snapshotDensity(h, cat.catalogId, cat.earliestSnapshotId, cat.headSnapshotId),
                        nextRowId(h, cat.catalogId),
                        expiryFloor(h, cat.catalogId),
                        visibilityBounds(h, cat.catalogId),
                        offsetRelease(h, cat.catalogId),
                        stagingTickets(h, cat.catalogId),
                        uploadClaims(h, cat.catalogId),
                    )
                VerifyReport(
                    catalog = catalog,
                    status = if (checks.all { it.status == "pass" }) "pass" else "fail",
                    checks = checks,
                )
            }
        }

    /**
     * One or more violation sub-queries' outcome: the TRUE total count,
     * and each sub-query's samples kept as its OWN list.
     *
     * The grouping is what makes a composite check readable. A check can
     * run up to nine sub-queries ([expiryFloor]) covering different
     * violation classes; concatenating their samples and taking the
     * first 20 means the noisiest class fills the quota and the other
     * eight are invisible in the report — you would read "expiry_floor
     * failed, here are twenty lagging consumers" and never learn that
     * rows also survived below the floor. [check] therefore trims by
     * ROUND-ROBIN across the groups, so every class that fired gets a
     * sample before any class gets a second one.
     */
    private data class Violations(val count: Long, val samples: List<List<String>>) {
        operator fun plus(other: Violations) = Violations(count + other.count, samples + other.samples)

        companion object {
            val NONE = Violations(0, emptyList())

            fun of(
                count: Long,
                samples: List<String>,
            ) = Violations(count, if (samples.isEmpty()) emptyList() else listOf(samples))
        }
    }

    /** Round-robin across the sub-query groups, capped at [MAX_SAMPLES]. */
    private fun fairSamples(groups: List<List<String>>): List<String> {
        val out = mutableListOf<String>()
        var round = 0
        while (out.size < MAX_SAMPLES && groups.any { it.size > round }) {
            for (group in groups) {
                if (out.size == MAX_SAMPLES) break
                group.getOrNull(round)?.let { out += it }
            }
            round++
        }
        return out
    }

    private fun check(
        name: String,
        description: String,
        found: Violations,
    ) = VerifyCheck(
        check = name,
        status = if (found.count == 0L) "pass" else "fail",
        violations = found.count,
        samples = fairSamples(found.samples),
        description = description,
    )

    /**
     * Run one violation query and render at most [MAX_SAMPLES] of its
     * rows, keeping the true count.
     *
     * [sql] is a bare SELECT with NO ordering or limit of its own; this
     * wraps it so a single round trip produces both numbers: the window
     * `count(*) OVER ()` is evaluated over the whole result set BEFORE
     * the LIMIT, so the count is exact no matter how many rows the
     * sample cap drops. [orderBy] and the cap are compile-time literals
     * over [sql]'s own output columns (invariant 9 intact — nothing here
     * is derived from input); every value is a bound parameter.
     *
     * Samples come back as this sub-query's OWN group; [check] trims
     * across groups by round-robin so a composite check's noisiest
     * class cannot crowd its siblings out of the report.
     *
     * `:c` and `:catalogId` are both bound to the catalog id: the checks
     * written here use `:c`, while the SQL fragments borrowed verbatim
     * from OffsetRepo and ExpiryService use `:catalogId`, and restating
     * one of them to match the other would be exactly the drift those
     * fragments are shared to prevent.
     */
    private fun violations(
        h: Handle,
        catalogId: Long,
        query: PathQuery,
        binds: Map<String, Any> = emptyMap(),
        render: (java.sql.ResultSet) -> String,
    ): Violations = violations(h, catalogId, query.sql, query.orderBy, binds, render)

    /** The registered path-equality sub-query, by name. */
    private fun pathQuery(name: String): PathQuery = PATH_EQUALITY_QUERIES.getValue(name)

    private fun violations(
        h: Handle,
        catalogId: Long,
        sql: String,
        orderBy: String,
        binds: Map<String, Any> = emptyMap(),
        render: (java.sql.ResultSet) -> String,
    ): Violations {
        var count = 0L
        val samples =
            h.createQuery(bounded(sql, orderBy))
                .bind("c", catalogId)
                .bind("catalogId", catalogId)
                .also { q -> binds.forEach { (k, value) -> q.bind(k, value) } }
                .map { rs, _ ->
                    count = rs.getLong("hog_violation_count")
                    render(rs)
                }
                .list()
        return Violations.of(count, samples)
    }

    // ---- 1: row-id tiling --------------------------------------------------

    private fun rowIdTiling(
        h: Handle,
        catalogId: Long,
    ): VerifyCheck {
        // A positional file overlapping ANY earlier positional interval of
        // its table (window max of prior range ends; zero-record files
        // occupy no ids and are skipped).
        val overlaps =
            violations(
                h,
                catalogId,
                """
                SELECT table_id, data_file_id, row_id_start, record_count, prev_end FROM (
                    SELECT table_id, data_file_id, row_id_start, record_count,
                           max(row_id_start + record_count) OVER (
                               PARTITION BY table_id
                               ORDER BY row_id_start, data_file_id
                               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prev_end
                    FROM hog_data_file
                    WHERE catalog_id = :c AND NOT explicit_row_ids AND record_count > 0) x
                WHERE prev_end IS NOT NULL AND row_id_start < prev_end
                """,
                orderBy = "table_id, row_id_start",
            ) { rs ->
                "table_id=${rs.getLong("table_id")} data_file_id=${rs.getLong("data_file_id")} " +
                    "range=[${rs.getLong("row_id_start")},+${rs.getLong("record_count")}) " +
                    "overlaps prior end ${rs.getLong("prev_end")}"
            }
        val negative =
            violations(
                h,
                catalogId,
                """
                SELECT table_id, data_file_id, row_id_start FROM hog_data_file
                WHERE catalog_id = :c AND row_id_start < 0
                """,
                orderBy = "table_id, data_file_id",
            ) { rs ->
                "table_id=${rs.getLong("table_id")} data_file_id=${rs.getLong("data_file_id")} " +
                    "negative row_id_start=${rs.getLong("row_id_start")}"
            }
        return check("row_id_tiling", ROW_ID_TILING_DESCRIPTION, overlaps + negative)
    }

    // ---- 2: deletion vectors ----------------------------------------------

    private fun deleteVectors(
        h: Handle,
        catalogId: Long,
    ): VerifyCheck {
        val dupLive =
            violations(
                h,
                catalogId,
                """
                SELECT data_file_id, count(*) AS live FROM hog_delete_file
                WHERE catalog_id = :c AND end_snapshot IS NULL
                GROUP BY data_file_id HAVING count(*) > 1
                """,
                orderBy = "data_file_id",
            ) { rs ->
                "data_file_id=${rs.getLong("data_file_id")} has ${rs.getLong("live")} live DVs"
            }
        val shrinking =
            violations(
                h,
                catalogId,
                """
                SELECT a.data_file_id, a.begin_snapshot AS older, a.delete_count AS older_count,
                       b.begin_snapshot AS newer, b.delete_count AS newer_count
                FROM hog_delete_file a
                JOIN hog_delete_file b
                  ON b.catalog_id = a.catalog_id AND b.data_file_id = a.data_file_id
                 AND b.begin_snapshot > a.begin_snapshot AND b.delete_count < a.delete_count
                WHERE a.catalog_id = :c
                """,
                orderBy = "data_file_id, older",
            ) { rs ->
                "data_file_id=${rs.getLong("data_file_id")} DV shrank: " +
                    "S${rs.getLong("older")}=${rs.getLong("older_count")} -> " +
                    "S${rs.getLong("newer")}=${rs.getLong("newer_count")}"
            }
        val oversize =
            violations(
                h,
                catalogId,
                """
                SELECT dv.delete_file_id, dv.data_file_id, dv.delete_count, df.record_count
                FROM hog_delete_file dv
                JOIN hog_data_file df
                  ON df.catalog_id = dv.catalog_id AND df.data_file_id = dv.data_file_id
                WHERE dv.catalog_id = :c AND dv.delete_count > df.record_count
                """,
                orderBy = "delete_file_id",
            ) { rs ->
                "delete_file_id=${rs.getLong("delete_file_id")} delete_count=" +
                    "${rs.getLong("delete_count")} exceeds record_count=" +
                    "${rs.getLong("record_count")} of data_file_id=${rs.getLong("data_file_id")}"
            }
        return check("delete_vectors", DELETE_VECTORS_DESCRIPTION, dupLive + shrinking + oversize)
    }

    // ---- 3: orphans --------------------------------------------------------

    private fun orphans(
        h: Handle,
        catalogId: Long,
    ): VerifyCheck {
        fun liveChildrenOfDroppedTable(
            table: String,
            idColumn: String,
        ): Violations =
            violations(
                h,
                catalogId,
                """
                SELECT x.table_id, x.$idColumn AS child_id
                FROM $table x
                JOIN hog_table t
                  ON t.catalog_id = x.catalog_id AND t.table_id = x.table_id
                WHERE x.catalog_id = :c AND x.end_snapshot IS NULL
                  AND t.dropped_snapshot IS NOT NULL
                """,
                orderBy = "table_id, child_id",
            ) { rs ->
                "$table $idColumn=${rs.getLong("child_id")} still live on dropped " +
                    "table_id=${rs.getLong("table_id")}"
            }
        // Table names are compile-time literals (invariant 9 intact).
        val found =
            liveChildrenOfDroppedTable("hog_data_file", "data_file_id") +
                liveChildrenOfDroppedTable("hog_column", "field_id") +
                liveChildrenOfDroppedTable("hog_table_version", "begin_snapshot")
        return check("orphans", ORPHANS_DESCRIPTION, found)
    }

    // ---- 4: removal queue --------------------------------------------------

    private fun removalQueue(
        h: Handle,
        catalogId: Long,
    ): VerifyCheck {
        val found =
            violations(h, catalogId, pathQuery("removal_queue.referenced")) { rs ->
                "removal_id=${rs.getLong("removal_id")} path='${rs.getString("path")}' " +
                    "queued but still live-referenced"
            }
        return check("removal_queue", REMOVAL_QUEUE_DESCRIPTION, found)
    }

    // ---- 5: snapshot density -----------------------------------------------

    private fun snapshotDensity(
        h: Handle,
        catalogId: Long,
        earliest: Long,
        head: Long,
    ): VerifyCheck {
        val (count, min, max) =
            h.createQuery(
                "SELECT count(*), min(snapshot_id), max(snapshot_id) FROM hog_snapshot WHERE catalog_id = :c",
            )
                .bind("c", catalogId)
                .map { rs, _ -> Triple(rs.getLong(1), rs.getLong(2), rs.getLong(3)) }
                .one()
        val expected = head - earliest + 1
        val samples = mutableListOf<String>()
        if (count != expected) {
            samples += "count(*)=$count but head($head) - earliest($earliest) + 1 = $expected"
        }
        if (count > 0 && min != earliest) samples += "min(snapshot_id)=$min != earliest_snapshot_id=$earliest"
        if (count > 0 && max != head) samples += "max(snapshot_id)=$max != head=$head"
        return check(
            "snapshot_density",
            SNAPSHOT_DENSITY_DESCRIPTION,
            Violations.of(samples.size.toLong(), samples),
        )
    }

    // ---- 6: next_row_id consistency ----------------------------------------

    private fun nextRowId(
        h: Handle,
        catalogId: Long,
    ): VerifyCheck {
        val found =
            violations(
                h,
                catalogId,
                """
                SELECT s.table_id, s.next_row_id, m.max_end
                FROM hog_table_stats s
                JOIN (SELECT table_id, max(row_id_start + record_count) AS max_end
                      FROM hog_data_file
                      WHERE catalog_id = :c AND NOT explicit_row_ids
                      GROUP BY table_id) m ON m.table_id = s.table_id
                WHERE s.catalog_id = :c AND s.next_row_id < m.max_end
                """,
                orderBy = "table_id",
            ) { rs ->
                "table_id=${rs.getLong("table_id")} next_row_id=${rs.getLong("next_row_id")} " +
                    "below max allocated range end ${rs.getLong("max_end")}"
            }
        return check("next_row_id", NEXT_ROW_ID_DESCRIPTION, found)
    }

    // ---- 7: expiry floor ---------------------------------------------------

    private fun expiryFloor(
        h: Handle,
        catalogId: Long,
    ): VerifyCheck {
        val pastHead =
            violations(
                h,
                catalogId,
                """
                SELECT earliest_snapshot_id, last_snapshot_id
                FROM hog_catalog
                WHERE catalog_id = :c AND earliest_snapshot_id > last_snapshot_id
                """,
                orderBy = "earliest_snapshot_id",
            ) { rs ->
                "earliest_snapshot_id=${rs.getLong("earliest_snapshot_id")} is past " +
                    "head=${rs.getLong("last_snapshot_id")}"
            }
        // ExpiryService.FLOOR_CANDIDATE_OFFSETS, verbatim: it decides
        // WHICH offsets can floor expiry at all, and a copy that lost its
        // hog_table join would let this check pass on exactly the rows
        // the sweep ignores. The #167 release runs FIRST in the sweep, so
        // a row that release would delete is already dead and cannot be a
        // floor — excluded here for the same reason, which is what makes
        // the check agree with the sweep instead of flagging rows the
        // next sweep simply removes (offset_release flags those).
        val belowConsumerFloor =
            violations(
                h,
                catalogId,
                """
                ${OffsetRepo.SUPERSEDED_LINEAGE_CTE}
                SELECT o.consumer_id, o.table_uuid, o.committed_snapshot,
                       (SELECT c.earliest_snapshot_id FROM hog_catalog c
                         WHERE c.catalog_id = o.catalog_id) AS earliest_snapshot_id
                ${ExpiryService.FLOOR_CANDIDATE_OFFSETS}
                  AND EXISTS (SELECT 1 FROM hog_catalog c
                               WHERE c.catalog_id = o.catalog_id
                                 AND c.consumer_floor
                                 AND o.committed_snapshot < c.earliest_snapshot_id)
                  AND NOT ${OffsetRepo.SUPERSEDED_PREDICATE}
                """,
                orderBy = "consumer_id, table_uuid",
            ) { rs ->
                "consumer '${rs.getString("consumer_id")}' on table_uuid=" +
                    "${rs.getString("table_uuid")} is at committed_snapshot=" +
                    "${rs.getLong("committed_snapshot")}, below the expiry floor " +
                    "${rs.getLong("earliest_snapshot_id")}"
            }
        var survivors = Violations.NONE
        for ((table, idColumn, scopeColumn) in BELOW_FLOOR_TABLES) {
            survivors +=
                violations(
                    h,
                    catalogId,
                    """
                    SELECT x.$idColumn AS row_id, x.$scopeColumn AS scope_id,
                           x.begin_snapshot, x.end_snapshot, c.earliest_snapshot_id
                    FROM $table x
                    JOIN hog_catalog c ON c.catalog_id = x.catalog_id
                    WHERE x.catalog_id = :c
                      AND x.end_snapshot IS NOT NULL
                      AND x.end_snapshot <= c.earliest_snapshot_id
                    """,
                    orderBy = VERSIONED_ROW_ORDER,
                ) { rs ->
                    "$table $idColumn=${rs.getLong("row_id")} row " +
                        "[${rs.getLong("begin_snapshot")}, ${rs.getLong("end_snapshot")}) " +
                        "survived the floor advance to ${rs.getLong("earliest_snapshot_id")}"
                }
        }
        return check("expiry_floor", EXPIRY_FLOOR_DESCRIPTION, pastHead + belowConsumerFloor + survivors)
    }

    // ---- 8: versioned-row visibility bounds --------------------------------

    private fun visibilityBounds(
        h: Handle,
        catalogId: Long,
    ): VerifyCheck {
        var found = Violations.NONE
        for ((table, idColumn, scopeColumn) in BELOW_FLOOR_TABLES) {
            found +=
                violations(
                    h,
                    catalogId,
                    // The `end_snapshot <= begin_snapshot` disjunct is
                    // currently unfalsifiable: every one of these tables
                    // carries a CHECK (end_snapshot IS NULL OR
                    // end_snapshot > begin_snapshot), so no INSERT can
                    // produce it. Kept anyway, and stated here rather
                    // than silently: the CHECK is what backs it, a
                    // migration that dropped or relaxed one would take
                    // this invariant with it, and this is the surface
                    // that is supposed to notice.
                    """
                    SELECT x.$idColumn AS row_id, x.$scopeColumn AS scope_id,
                           x.begin_snapshot, x.end_snapshot, c.last_snapshot_id
                    FROM $table x
                    JOIN hog_catalog c ON c.catalog_id = x.catalog_id
                    WHERE x.catalog_id = :c
                      AND (x.begin_snapshot > c.last_snapshot_id
                           OR (x.end_snapshot IS NOT NULL
                               AND (x.end_snapshot <= x.begin_snapshot
                                    OR x.end_snapshot > c.last_snapshot_id)))
                    """,
                    orderBy = VERSIONED_ROW_ORDER,
                ) { rs ->
                    val end = rs.getObject("end_snapshot")?.let { rs.getLong("end_snapshot").toString() } ?: "null"
                    "$table $idColumn=${rs.getLong("row_id")} begin_snapshot=" +
                        "${rs.getLong("begin_snapshot")} end_snapshot=$end is outside " +
                        "[0, head=${rs.getLong("last_snapshot_id")}]"
                }
        }
        // hog_table is the identity row, not a versioned row, but it
        // carries the same shape under other names: created/dropped.
        val identities =
            violations(
                h,
                catalogId,
                """
                SELECT t.table_id, t.created_snapshot, t.dropped_snapshot, c.last_snapshot_id
                FROM hog_table t
                JOIN hog_catalog c ON c.catalog_id = t.catalog_id
                WHERE t.catalog_id = :c
                  AND (t.created_snapshot > c.last_snapshot_id
                       OR (t.dropped_snapshot IS NOT NULL
                           AND (t.dropped_snapshot < t.created_snapshot
                                OR t.dropped_snapshot > c.last_snapshot_id)))
                """,
                orderBy = "table_id",
            ) { rs ->
                val dropped =
                    rs.getObject("dropped_snapshot")?.let { rs.getLong("dropped_snapshot").toString() } ?: "null"
                "hog_table table_id=${rs.getLong("table_id")} created_snapshot=" +
                    "${rs.getLong("created_snapshot")} dropped_snapshot=$dropped " +
                    "is outside [0, head=${rs.getLong("last_snapshot_id")}]"
            }
        return check("visibility_bounds", VISIBILITY_BOUNDS_DESCRIPTION, found + identities)
    }

    // ---- 9: superseded-offset release (#167) -------------------------------

    private fun offsetRelease(
        h: Handle,
        catalogId: Long,
    ): VerifyCheck {
        val found =
            violations(
                h,
                catalogId,
                """
                ${OffsetRepo.SUPERSEDED_LINEAGE_CTE}
                SELECT o.consumer_id, o.table_uuid, o.committed_snapshot
                FROM hog_consumer_offset o
                WHERE o.catalog_id = :catalogId
                  AND ${OffsetRepo.SUPERSEDED_PREDICATE}
                """,
                orderBy = "consumer_id, table_uuid",
            ) { rs ->
                "consumer '${rs.getString("consumer_id")}' still holds an offset on retired " +
                    "table_uuid=${rs.getString("table_uuid")} at committed_snapshot=" +
                    "${rs.getLong("committed_snapshot")}; a reconciled successor exists, so " +
                    "this row should have been released and now pins the expiry floor"
            }
        return check("offset_release", OFFSET_RELEASE_DESCRIPTION, found)
    }

    // ---- 10: compaction staging tickets (#174) -----------------------------

    private fun stagingTickets(
        h: Handle,
        catalogId: Long,
    ): VerifyCheck {
        // (a) settled 'registered', yet the catalog has never heard of the
        // path. "Never heard of" is deliberately stronger than "not live":
        // a compaction output that was itself compacted and then expired
        // legitimately loses its file row, and the expiry sweep always
        // leaves its OWN ledger row for the path behind when it does.
        val registeredNothing =
            violations(h, catalogId, pathQuery("staging_tickets.registered_nothing")) { rs ->
                "removal_id=${rs.getLong("removal_id")} path='${rs.getString("path")}' settled " +
                    "'registered' but no file row and no other ledger row for that path exists"
            }
        // (b) a claim ticket nothing ever settled or drained.
        val leaked =
            violations(
                h,
                catalogId,
                pathQuery("staging_tickets.leaked"),
                binds = mapOf("stagingMaxAge" to stagingTicketMaxAgeSeconds),
            ) { rs ->
                "removal_id=${rs.getLong("removal_id")} path='${rs.getString("path")}' is an " +
                    "undrained compaction_staging ticket ${rs.getLong("age_seconds")}s old with " +
                    "no file row (bound ${stagingTicketMaxAgeSeconds}s)"
            }
        // (c) the #174 race resolved the wrong way.
        val absentButRegistered =
            violations(h, catalogId, pathQuery("staging_tickets.absent_but_registered")) { rs ->
                "removal_id=${rs.getLong("removal_id")} path='${rs.getString("path")}' was drained " +
                    "'absent' but the catalog holds a file row for that path"
            }
        return check(
            "staging_tickets",
            stagingTicketsDescription,
            registeredNothing + leaked + absentButRegistered,
        )
    }

    // ---- 11: upload claims (#162/#167) -------------------------------------

    private fun uploadClaims(
        h: Handle,
        catalogId: Long,
    ): VerifyCheck {
        val registeredButQueued =
            violations(h, catalogId, pathQuery("upload_claims.registered_but_queued")) { rs ->
                "upload_id=${rs.getString("upload_id")} path='${rs.getString("path")}' is " +
                    "'registered' but an undrained trino_upload removal row would reclaim it"
            }
        val unsettled =
            violations(h, catalogId, pathQuery("upload_claims.unsettled")) { rs ->
                val state = rs.getString("state")
                val why =
                    if (state == "active") {
                        "the path was registered without the claim being settled"
                    } else {
                        "the claim fenced a path the catalog has registered"
                    }
                "upload_id=${rs.getString("upload_id")} path='${rs.getString("path")}' is " +
                    "'$state' and the catalog holds a file row for that path: $why"
            }
        return check("upload_claims", UPLOAD_CLAIMS_DESCRIPTION, registeredButQueued + unsettled)
    }

    // ---- descriptions ------------------------------------------------------

    /**
     * The one description that is not a constant: it NAMES the bound
     * and the two config values it is judged against, and all three are
     * per-process, so a report from a service built with different ones
     * must not quote the defaults.
     */
    private val stagingTicketsDescription: String =
        "Invariant 4's compaction half: compaction pre-registers its output path as an undrained " +
            "hog_file_removal row with reason 'compaction_staging' — a claim ticket — before " +
            "uploading, and a successful group commit settles that ticket as drained_outcome " +
            "'registered' in the same transaction that makes the path a live catalog file. Three " +
            "states contradict that. A row settled 'registered' whose path the catalog has never " +
            "heard of (no hog_data_file or hog_delete_file row, and no other ledger row for the " +
            "path either) was settled without anything being registered. An undrained " +
            "'compaction_staging' ticket with no file row for its path that is older than " +
            "$stagingTicketMaxAgeSeconds seconds is a leaked claim. That bound is an OPERATIONAL " +
            "judgement, not a derivation: nothing bounds a rewrite in time — a group is capped in " +
            "bytes (HOGLAKE_COMPACTION_TARGET_BYTES, ${bytes(compactionTargetBytes)} here) and in " +
            "input files, never in seconds, and object-store latency is not the server's to " +
            "promise. What the bound has to clear is the window in which the ticket is still " +
            "somebody's job: the group that owns it, and then the cleanup drain that reclaims it " +
            "(HOGLAKE_CLEANUP_INTERVAL_MS, ${interval(cleanupIntervalMs)} here). Above that, an " +
            "undrained ticket means nobody is coming. And a 'compaction_staging' ticket drained " +
            "'absent' whose path IS a file row is the staged-output race resolved the wrong way: " +
            "the removal ledger, the only thing that knows the path, records that the object " +
            "never existed while the catalog serves reads from it."

    /** MiB where it divides, bytes otherwise — description text only. */
    private fun bytes(value: Long): String =
        if (value >= 1024 * 1024 && value % (1024 * 1024) == 0L) "${value / (1024 * 1024)} MiB" else "$value bytes"

    /** Seconds, or "disabled" for a loop that is off — description text only. */
    private fun interval(ms: Long): String = if (ms <= 0) "disabled" else "every ${ms / 1000} seconds"

    companion object {
        /** Per-check cap on sample details in the report. */
        const val MAX_SAMPLES = 20

        /**
         * A TOTAL order over any [VersionedTable]'s violation rows —
         * scope, then id, then version. Total matters because the
         * samples are the first [MAX_SAMPLES] of it: a partial order
         * leaves which rows a report shows to the plan, and two runs
         * over identical data can disagree.
         */
        private const val VERSIONED_ROW_ORDER = "scope_id, row_id, begin_snapshot"

        /**
         * Default staleness bound for an undrained `compaction_staging`
         * ticket, six hours.
         *
         * It is an OPERATIONAL judgement, and deliberately not dressed
         * up as a derivation. Nothing bounds a compaction rewrite in
         * time: a group is capped in BYTES
         * (`HOGLAKE_COMPACTION_TARGET_BYTES`, derated further for the
         * sorted path) and in input file count, never in seconds, and
         * the object-store latency that would turn bytes into a duration
         * is not the server's to promise. What the bound must clear is
         * the window in which the ticket is still somebody's job — the
         * group that owns it, and then the cleanup drain that reclaims
         * an aborted one (`HOGLAKE_CLEANUP_INTERVAL_MS`, 30 minutes by
         * default, plus however long its backlog takes to reach this
         * row). Six hours is an order of magnitude above that and the
         * asymmetry is the argument: erring large costs a delay in
         * noticing a leak, erring small costs a false alert on every
         * healthy sweep, which is how an alert stops being read.
         */
        const val DEFAULT_STAGING_TICKET_MAX_AGE_SECONDS = 6L * 60 * 60

        /**
         * One path-equality sub-query, with the ordering its samples are
         * taken in. Registered rather than inlined so the plan test can
         * EXPLAIN the exact statement [bounded] builds around it.
         */
        internal data class PathQuery(val sql: String, val orderBy: String)

        /**
         * A versioned table, the column that NAMES one of its rows, and
         * the column that SCOPES that name.
         *
         * Both are needed for a TOTAL ordering, which the sample cap
         * depends on: `hog_column`'s field ids restart per table, so
         * ordering by field id alone leaves the first twenty samples of
         * a broken catalog up to the heap's whim, and two runs of the
         * same scan can disagree about what they show.
         */
        internal data class VersionedTable(
            val table: String,
            val idColumn: String,
            val scopeColumn: String = "table_id",
        )

        /**
         * Every check sub-query whose predicate is a PATH EQUALITY
         * across tables that carry no index on `path`.
         *
         * `hog_data_file`, `hog_delete_file` and `hog_file_removal` are
         * indexed on (catalog, file id) and (catalog, removal id) —
         * their hot predicates — so a path lookup has no access path at
         * all. That is affordable exactly once per query: ONE scan of
         * the manifest, hashed, joined against the candidate set. It is
         * a catastrophe per candidate, and the difference is invisible
         * on any catalog small enough to be a fixture.
         *
         * So every one of these is written in a shape the planner can
         * FLATTEN — a join, or a LEFT JOIN ... IS NULL anti-join — and
         * never as a correlated `EXISTS (... UNION ...)`, which Postgres
         * cannot pull up and which `removal_queue` used to carry on the
         * check most likely to have candidates. `VerifyQueryPlanIntegration
         * Test` EXPLAINs each of them against a 50k-file manifest and
         * fails on anything executed more than once.
         */
        internal val PATH_EQUALITY_QUERIES: Map<String, PathQuery> =
            mapOf(
                // Invariant 4 at rest: an undrained queue entry whose
                // path a file row still claims. Two semi-joins UNIONed
                // rather than one EXISTS over a UNION: each branch is a
                // join the planner can hash, and UNION removes the
                // duplicate a path claimed by both tables would produce.
                "removal_queue.referenced" to
                    PathQuery(
                        """
                        SELECT q.removal_id, q.path
                        FROM hog_file_removal q
                        JOIN hog_data_file f
                          ON f.catalog_id = q.catalog_id AND f.path = q.path
                        WHERE q.catalog_id = :c AND q.drained_at IS NULL
                        UNION
                        SELECT q.removal_id, q.path
                        FROM hog_file_removal q
                        JOIN hog_delete_file d
                          ON d.catalog_id = q.catalog_id AND d.path = q.path
                        WHERE q.catalog_id = :c AND q.drained_at IS NULL
                        """,
                        orderBy = "removal_id",
                    ),
                // Settled 'registered' while the catalog has never heard
                // of the path. Three anti-joins as LEFT JOIN ... IS NULL,
                // which the planner turns into hash anti-joins; the
                // equivalent NOT EXISTS trio picked a nested loop for
                // the smallest of the three.
                "staging_tickets.registered_nothing" to
                    PathQuery(
                        """
                        SELECT q.removal_id, q.path
                        FROM hog_file_removal q
                        LEFT JOIN hog_data_file f
                          ON f.catalog_id = q.catalog_id AND f.path = q.path
                        LEFT JOIN hog_delete_file d
                          ON d.catalog_id = q.catalog_id AND d.path = q.path
                        LEFT JOIN hog_file_removal o
                          ON o.catalog_id = q.catalog_id AND o.path = q.path
                         AND o.removal_id <> q.removal_id
                        WHERE q.catalog_id = :c AND q.drained_outcome = 'registered'
                          AND f.data_file_id IS NULL
                          AND d.delete_file_id IS NULL
                          AND o.removal_id IS NULL
                        """,
                        orderBy = "removal_id",
                    ),
                // A claim ticket nothing ever settled or drained.
                "staging_tickets.leaked" to
                    PathQuery(
                        """
                        SELECT q.removal_id, q.path,
                               (extract(epoch FROM (now() - q.scheduled_at)))::bigint AS age_seconds
                        FROM hog_file_removal q
                        LEFT JOIN hog_data_file f
                          ON f.catalog_id = q.catalog_id AND f.path = q.path
                        LEFT JOIN hog_delete_file d
                          ON d.catalog_id = q.catalog_id AND d.path = q.path
                        WHERE q.catalog_id = :c AND q.drained_at IS NULL
                          AND q.reason = 'compaction_staging'
                          AND q.scheduled_at < now() - make_interval(secs => :stagingMaxAge)
                          AND f.data_file_id IS NULL
                          AND d.delete_file_id IS NULL
                        """,
                        orderBy = "removal_id",
                    ),
                // The staged-output race resolved the wrong way.
                "staging_tickets.absent_but_registered" to
                    PathQuery(
                        """
                        SELECT q.removal_id, q.path
                        FROM hog_file_removal q
                        JOIN hog_data_file f
                          ON f.catalog_id = q.catalog_id AND f.path = q.path
                        WHERE q.catalog_id = :c AND q.reason = 'compaction_staging'
                          AND q.drained_outcome = 'absent'
                        UNION
                        SELECT q.removal_id, q.path
                        FROM hog_file_removal q
                        JOIN hog_delete_file d
                          ON d.catalog_id = q.catalog_id AND d.path = q.path
                        WHERE q.catalog_id = :c AND q.reason = 'compaction_staging'
                          AND q.drained_outcome = 'absent'
                        """,
                        orderBy = "removal_id",
                    ),
                // A fence raced a registration.
                "upload_claims.registered_but_queued" to
                    PathQuery(
                        """
                        SELECT DISTINCT u.upload_id, u.path
                        FROM hog_upload u
                        JOIN hog_file_removal q
                          ON q.catalog_id = u.catalog_id AND q.path = u.path
                         AND q.drained_at IS NULL AND q.reason = 'trino_upload'
                        WHERE u.catalog_id = :c AND u.state = 'registered'
                        """,
                        orderBy = "upload_id",
                    ),
                // Registered without settling, or fenced a live path.
                "upload_claims.unsettled" to
                    PathQuery(
                        """
                        SELECT u.upload_id, u.path, u.state
                        FROM hog_upload u
                        JOIN hog_data_file f
                          ON f.catalog_id = u.catalog_id AND f.path = u.path
                        WHERE u.catalog_id = :c AND u.state IN ('active', 'abandoned')
                        UNION
                        SELECT u.upload_id, u.path, u.state
                        FROM hog_upload u
                        JOIN hog_delete_file d
                          ON d.catalog_id = u.catalog_id AND d.path = u.path
                        WHERE u.catalog_id = :c AND u.state IN ('active', 'abandoned')
                        """,
                        orderBy = "upload_id",
                    ),
            )

        /**
         * The statement [violations] actually runs: the sub-query,
         * wrapped so ONE round trip yields the true count and the
         * samples. `count(*) OVER ()` is evaluated over the whole result
         * set before the LIMIT, so the count is exact however many rows
         * the sample cap drops.
         *
         * `internal` because the plan test must EXPLAIN this, not the
         * bare fragment: the wrapper adds a WindowAgg, a Sort and a
         * Limit, and a Limit in particular can change which plan the
         * planner picks for everything underneath it.
         */
        internal fun bounded(
            sql: String,
            orderBy: String,
        ): String =
            """
            SELECT v.*, count(*) OVER () AS hog_violation_count
            FROM ($sql) v
            ORDER BY $orderBy
            LIMIT $MAX_SAMPLES
            """

        /** Config defaults, used when no wiring supplies the real values. */
        const val DEFAULT_COMPACTION_TARGET_BYTES = 512L * 1024 * 1024

        /** Config default for HOGLAKE_CLEANUP_INTERVAL_MS (30 minutes). */
        const val DEFAULT_CLEANUP_INTERVAL_MS = 1_800_000L

        /**
         * The versioned tables the expiry sweep's fifth step and the
         * file-GC steps clear below the floor, and the ones whose
         * begin/end bounds the visibility rule governs — paired with the
         * column that NAMES a row of each, so a sample points at the row
         * rather than only at its snapshot range (`hog_table_version`
         * and `hog_column` are keyed by (table, ...) so their id is the
         * table; the rest carry their own). A fixed compile-time
         * vocabulary, never derived from input (invariant 9 intact);
         * `internal` so a test can parametrize over the same list rather
         * than restate it.
         */
        internal val BELOW_FLOOR_TABLES =
            listOf(
                VersionedTable("hog_data_file", "data_file_id"),
                VersionedTable("hog_delete_file", "delete_file_id"),
                VersionedTable("hog_table_version", "table_id"),
                VersionedTable("hog_column", "field_id"),
                VersionedTable("hog_partition_spec", "spec_id"),
                VersionedTable("hog_sort_spec", "sort_id"),
                // The one table with no table_id: views have no
                // identity/version split (schema.sql says why).
                VersionedTable("hog_view", "view_id", scopeColumn = "view_id"),
            )

        const val ROW_ID_TILING_DESCRIPTION =
            "Invariant 2: row-id ranges are assigned server-side at commit, contiguous per file, " +
                "tile [0, total) per table, and are never reused — the lineage guarantee. Every " +
                "positional file row of a table, live and historical alike, must therefore be a " +
                "non-overlapping interval with a non-negative start; gaps are legal, because " +
                "expiry deletes rows no retained snapshot can reach, but an overlap never is. " +
                "Compaction outputs are exempt from the interval sweep by design: they legitimately " +
                "CARRY their inputs' row ids as data in the explicit _hog_row_id column under " +
                "reserved parquet field id 2147483646 (flagged by hog_data_file.explicit_row_ids), " +
                "so their row_id_start is only min(input ids) and has no positional meaning."

        const val DELETE_VECTORS_DESCRIPTION =
            "Invariant 3: one live deletion vector per data file (backed by the unique partial " +
                "index), supersessions only grow (delete_count monotone along begin_snapshot), and " +
                "a DV newer than your read_snapshot is a 409 rather than a lost update. This " +
                "asserts the state those rules produce at rest: at most one live DV per data file, " +
                "no supersession chain whose delete_count shrinks, and no vector claiming more " +
                "deletes than its data file has rows."

        const val ORPHANS_DESCRIPTION =
            "The corollary of invariant 6 at rest: dropping a table end-snapshots its data files, " +
                "its columns and its versioned name rows in the drop snapshot, so a live " +
                "(end_snapshot IS NULL) hog_data_file, hog_column or hog_table_version row on a " +
                "table carrying a dropped_snapshot is a write that outlived its parent — a row no " +
                "read path can reach and no sweep will ever collect."

        const val REMOVAL_QUEUE_DESCRIPTION =
            "Invariant 4: physical deletion is never authorized by the queue — cleanup " +
                "liveness-checks every path against live references at drain time, and " +
                "still_referenced > 0 is an invariant violation, alerted, not deleted. This is that " +
                "alert condition observed at rest instead of at drain time: an undrained " +
                "hog_file_removal row whose path a hog_data_file or hog_delete_file row still " +
                "claims, live or historical."

        const val SNAPSHOT_DENSITY_DESCRIPTION =
            "Invariant 1: snapshot ids are dense per catalog and ordered with commit order, " +
                "assigned inside the per-catalog advisory-lock commit tail. The retained snapshots " +
                "must therefore be exactly the dense range [earliest_snapshot_id, head]: count(*) " +
                "equals head - earliest + 1, with min and max sitting on the two ends. The " +
                "catalog-health gauge publishes head - earliest + 1 as an approximation of that " +
                "count; this check asserts it for real."

        const val NEXT_ROW_ID_DESCRIPTION =
            "Invariant 2's allocator half, read through invariant 7's account of hog_table_stats " +
                "as the gross append counter and row-id allocator anchor: next_row_id must be at " +
                "or past every positional file's range end, because the allocator can never have " +
                "handed out a range it does not remember. Compaction outputs are excluded — an " +
                "explicit_row_ids file's row_id_start is min(input ids), not an allocation."

        const val EXPIRY_FLOOR_DESCRIPTION =
            "Invariant 5: expiry never passes head or (when consumer_floor is set) the min " +
                "consumer offset, and a fifth sweep step deletes versioned DDL rows whose " +
                "end_snapshot <= earliest_snapshot_id because they are invisible at every retained " +
                "snapshot. Three things follow at rest. earliest_snapshot_id <= head. With " +
                "consumer_floor on, earliest_snapshot_id <= every committed_snapshot whose table " +
                "row still exists — ExpiryService's floor query mirrored exactly, hog_table join " +
                "included, and with the same release semantics: the sweep runs the superseded-" +
                "offset release BEFORE it measures, so a row that release would delete is already " +
                "dead and is excluded here rather than counted as a floor (offset_release is the " +
                "check that flags those). And no hog_data_file, hog_delete_file, " +
                "hog_table_version, hog_column, hog_partition_spec, hog_sort_spec or hog_view row " +
                "with end_snapshot <= earliest_snapshot_id is still present — there is no lag " +
                "window on that last one, because steps 1, 2 and 5 of the sweep delete those rows " +
                "in the same transaction, under the same per-catalog commit lock, that advances " +
                "the floor."

        const val VISIBILITY_BOUNDS_DESCRIPTION =
            "Invariant 6, versioned-row visibility: a row is visible at S iff begin_snapshot <= S " +
                "AND (end_snapshot IS NULL OR S < end_snapshot), and every read path uses exactly " +
                "this predicate. Bounds outside the catalog's own snapshot range make that " +
                "predicate meaningless, so every versioned row of hog_data_file, hog_delete_file, " +
                "hog_table_version, hog_column, hog_partition_spec, hog_sort_spec and hog_view " +
                "must satisfy begin_snapshot <= head, end_snapshot IS NULL OR end_snapshot > " +
                "begin_snapshot, and end_snapshot <= head. hog_table is the immutable identity row " +
                "rather than a versioned one, but it carries the same shape under other names and " +
                "is held to it: created_snapshot <= head, and created_snapshot <= dropped_snapshot " +
                "whenever the table is dropped."

        const val OFFSET_RELEASE_DESCRIPTION =
            "The working rule that whatever mints a new identity releases the old one's consumers. " +
                "Atomic replacement retires an incarnation and creates its successor with a new " +
                "table_uuid in one snapshot; lineage between them is RECORDED in " +
                "hog_table.replaced_table_id and walked forward from that column, never derived at " +
                "run time from a convention; and a consumer's row on a retired incarnation is " +
                "deleted once that same consumer holds an offset at or past a descendant's " +
                "created_snapshot, which is its own statement that it reconciled across the " +
                "replacement. A row meeting that condition and still present was never released: " +
                "nothing will ever advance it, GET /consumers shows a position the consumer will " +
                "never read again, and on a consumer_floor catalog with retention configured it " +
                "pins the expiry floor with nobody left to lift it. (On a retention-null catalog " +
                "it pins nothing, because expiry never advances there at all — which is exactly " +
                "why the sweep releases these rows ABOVE its retention check, so the stranded " +
                "rows on such a catalog still have a path out.) The check asks " +
                "OffsetRepo.releaseSupersededOffsets' own predicate rather than a restatement of " +
                "it, so the flagged set is the released set by construction."

        const val UPLOAD_CLAIMS_DESCRIPTION =
            "Invariant 4 on the upload-claim ledger: a claim is durable ownership of a path before " +
                "the PUT, UploadService.register settles it as 'registered' inside the same commit " +
                "transaction that registers the path (taking the claim rows FOR UPDATE rather than " +
                "the per-catalog commit lock, which is for commits), and the claim-expiry sweep " +
                "(UploadService.scheduleExpired) fences a claim to 'abandoned' and offers its path " +
                "to cleanup only while no file row claims it. Three states contradict that. A " +
                "'registered' claim with an undrained removal row for its path whose reason is " +
                "'trino_upload' means a fence raced a " +
                "registration and cleanup is queued to delete a published object — rows with " +
                "reason 'snapshot_expiry' are deliberately excluded, since queueing a registered " +
                "path once its file row has expired is the normal lifecycle rather than a " +
                "contradiction. An 'active' claim whose path the catalog holds a file row for was " +
                "registered without being settled. An 'abandoned' claim whose path the catalog " +
                "holds a file row for fenced a live path, and register() refuses to revive it, so " +
                "the claim can never settle."
    }
}
