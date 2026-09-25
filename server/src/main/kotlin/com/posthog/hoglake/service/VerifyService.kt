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
 * Twelve checks, each one query shape per violation class, each one
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
 * 12. compaction_claims — V15 (the group-claim lease's lifecycle; a
 *                         claim is an OPTIMIZATION, never authorization,
 *                         so a violation is redundant work or a leaked
 *                         row, never a wrong commit)
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
    /**
     * How far past its expiry a `hog_compaction_claim` row may sit
     * before `compaction_claims` calls it un-purged. Not the lease
     * length: the lease says when the claim stops protecting a group,
     * this says when nobody having REMOVED it is a defect. Generous by
     * design — the purge runs at the head of a sweep, so the honest
     * bound is a few sweep intervals, and an hour clears any plausible
     * one. Constructor-tunable for tests.
     */
    private val compactionClaimMaxAgeSeconds: Long = DEFAULT_COMPACTION_CLAIM_MAX_AGE_SECONDS,
    /**
     * How long a table may sit RETIREMENT-ELIGIBLE — dropped, and its
     * drop snapshot at or below the expiry floor — with live file rows
     * still on it before `orphans` calls it a leak.
     *
     * Not a statement about how fast retirement runs: it is the bound
     * past which "still going" stops being a credible explanation. The
     * default is a day, which is `N x HOGLAKE_RETIREMENT_INTERVAL_MS`
     * for any N >= 2 at any interval a deployment would plausibly set,
     * and is generous on purpose — retirement is paced against the
     * commit lock and against the cleanup queue, so a big table
     * legitimately takes many runs, and an alert that fires on a system
     * working as designed is an alert nobody reads. Constructor-tunable
     * for tests.
     */
    private val retirementOrphanGraceSeconds: Long = DEFAULT_RETIREMENT_ORPHAN_GRACE_SECONDS,
    /**
     * `HOGLAKE_RETIREMENT_INTERVAL_MS` as this process has it, for the
     * `orphans` description alone — the same treatment
     * [cleanupIntervalMs] gets, and for the same reason: a description
     * that quoted the default would be wrong on any deployment that
     * tunes it.
     *
     * NOT defaulted, unlike its neighbours. Retirement's default is 0
     * (off), so a forgotten wire-up and a correctly-wired API pod
     * produce the SAME description, and nothing would ever say which
     * one a report came from. The compiler asks instead.
     */
    private val retirementIntervalMs: Long,
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
                        compactionClaims(h, cat.catalogId),
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
        /**
         * The DDL arms, VERBATIM from before #193 and now the only
         * assertion that `TableRepo.markDropped` did its job.
         *
         * A drop is three UPDATEs: the identity row's
         * `dropped_snapshot`, the live version row's `end_snapshot`,
         * and the live column rows'. The file rows are deliberately NOT
         * touched any more, so a live one is the normal state of a
         * dropped table awaiting retirement — but a live COLUMN or
         * VERSION row still means one of those three UPDATEs did not
         * run, which is a half-dropped table and unreachable state.
         */
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

        /**
         * A FILE arm: a table whose retirement should have finished and
         * has not.
         *
         * THE OLD ARM CANNOT SURVIVE #193. It called any live
         * `hog_data_file` row on a dropped table a violation, which
         * used to be true because the drop end-snapshotted every one of
         * them in the same transaction. Now the drop leaves them alone
         * and the retirement sweep deletes them later, so that
         * predicate fires on every drop, on every catalog, from the
         * moment the drop commits until the sweep gets there — an alert
         * on the design.
         *
         * What IS still a defect is a table the sweep should have
         * finished, so the arm is qualified twice:
         *
         *  - `dropped_snapshot <= earliest_snapshot_id`: below the
         *    floor, the rows are deletable. ABOVE it they are still
         *    readable by time travel and retirement must not touch
         *    them, so a table waiting for the floor is not a leak. This
         *    is also why a retention-NULL catalog can never trip this
         *    arm, and why the informational count below exists;
         *  - `retirement_eligible_at < now() - grace`: the sweep has
         *    SEEN this table (it stamps that column on first
         *    observation) and has had [retirementOrphanGraceSeconds] to
         *    finish it. Dating from the DROP instead would fire on a
         *    catalog whose floor simply moved slowly, and an alert that
         *    fires on a healthy system stops being read. A NULL stamp
         *    with a passed floor means no sweep has observed the table
         *    at all — retirement is off, or wedged — and it cannot be
         *    distinguished here from "eligible for one second", so it
         *    is deliberately NOT a violation; the run ledger's silence
         *    is what says the loop is not running.
         *
         * `EXISTS`, not a count: this is a yes/no about a table, driven
         * from `hog_data_file_live` / `hog_delete_file_live` — one
         * index probe per dropped table rather than a pass over
         * whatever the table still holds.
         */
        fun unretired(
            relation: String,
            index: String,
        ): Violations =
            violations(
                h,
                catalogId,
                """
                SELECT t.table_id, t.dropped_snapshot,
                       extract(epoch FROM (now() - t.retirement_eligible_at))::bigint AS eligible_age
                FROM hog_table t
                JOIN hog_catalog c ON c.catalog_id = t.catalog_id
                WHERE t.catalog_id = :c
                  AND t.dropped_snapshot IS NOT NULL
                  AND t.dropped_snapshot <= c.earliest_snapshot_id
                  AND t.retirement_eligible_at IS NOT NULL
                  AND t.retirement_eligible_at < now() - make_interval(secs => :grace)
                  AND EXISTS (
                      SELECT 1 FROM $relation x
                      WHERE x.catalog_id = t.catalog_id
                        AND x.table_id = t.table_id
                        AND x.end_snapshot IS NULL)
                """,
                orderBy = "table_id",
                binds = mapOf("grace" to retirementOrphanGraceSeconds.toDouble()),
            ) { rs ->
                "table_id=${rs.getLong("table_id")} was dropped in snapshot " +
                    "${rs.getLong("dropped_snapshot")} and has been retirement-eligible for " +
                    "${rs.getLong("eligible_age")}s (bound ${retirementOrphanGraceSeconds}s) but " +
                    "still holds live $relation rows ($index); retirement is not draining it"
            }

        /**
         * INFORMATIONAL, and never a violation: live file rows on
         * dropped tables, whatever the floor says.
         *
         * DRIVEN FROM `hog_table`, WITH A CAPPED LATERAL, because the
         * obvious shape is a trap. A `GROUP BY table_id` over
         * `hog_data_file` is O(THE LIVE MANIFEST) — it reads every live
         * row in the catalog to find the ones on dropped tables — and
         * `/verify` is an aggregate pass that runs hourly against the
         * database serving the commit tail. This form visits only
         * DROPPED tables (a range scan of `hog_table`'s primary key)
         * and, for each, stops counting at [INFORMATIONAL_ROW_CAP]
         * rows through `hog_data_file_live`. The cost is bounded by
         * `dropped tables x cap` rather than by the manifest, and
         * `VerifyQueryPlanIntegrationTest` holds it to that.
         *
         * THE `ORDER BY` IS WHAT FORCES THE INDEX, and it is free.
         * Without it the planner is entitled to satisfy `LIMIT n` with
         * a SEQUENTIAL SCAN that stops once it has found n matching
         * rows — which is cheap on a dropped table holding a third of
         * the manifest and reads the WHOLE MANIFEST on one holding
         * five rows, so the arm would cost `dropped tables x manifest`
         * on the shape production actually has (many dropped tables,
         * most of them small). Sorting by `hog_data_file_live`'s own
         * key columns makes any other plan pay for a Sort, so the
         * planner takes the index; the index already provides that
         * order, so the clause costs nothing. `VerifyQueryPlan-
         * IntegrationTest` seeds a big dropped table AND a tiny one for
         * exactly this reason — with only the big one, the sequential
         * plan passes.
         *
         * The price is that the count SATURATES, and the sample line
         * says so ("at least N"). That is the right trade for a line
         * whose job is "this dropped table is still holding storage":
         * an operator needs to know WHICH table and roughly how much,
         * and 10,000 is already "a lot". The exact number is a
         * `SELECT count(*)` away for anyone who wants it.
         *
         * The failing arms above are floor-qualified, so on a catalog
         * with no snapshot retention — several production catalogs have
         * none — they can never fire, because the floor never advances
         * and the rows are never deletable. That is correct, and it
         * also means the population would be completely invisible: a
         * dropped 3M-row table would hold its storage forever with
         * nothing reporting it. This reports it, as a sample line on a
         * PASSING check, so an operator can see what configuring
         * retention (or unblocking a consumer's offset) would reclaim.
         *
         * Counted per table in ONE grouped pass over
         * `hog_data_file_live`, which is strictly cheaper than the arm
         * it replaces: that one returned a row per FILE.
         */
        val informational =
            violations(
                h,
                catalogId,
                ORPHANS_INFORMATIONAL_SQL,
                orderBy = "live_rows DESC, table_id",
                binds = mapOf("sampleCap" to INFORMATIONAL_ROW_CAP),
            ) { rs ->
                val why =
                    if (rs.getBoolean("eligible")) {
                        ""
                    } else {
                        " (not yet eligible: the drop snapshot has not sunk to the expiry floor " +
                            "— a catalog with no retention never retires, by design)"
                    }
                val rows = rs.getLong("live_rows")
                val howMany = if (rows >= INFORMATIONAL_ROW_CAP) "at least $rows" else "$rows"
                "note: table_id=${rs.getLong("table_id")} (dropped in snapshot " +
                    "${rs.getLong("dropped_snapshot")}) still holds $howMany " +
                    "live data-file rows awaiting retirement" + why
            }

        // Table names and index names are compile-time literals
        // (invariant 9 intact).
        val found =
            unretired("hog_data_file", "hog_data_file_live") +
                unretired("hog_delete_file", "hog_delete_file_live") +
                liveChildrenOfDroppedTable("hog_column", "field_id") +
                liveChildrenOfDroppedTable("hog_table_version", "begin_snapshot")
        // The informational group contributes SAMPLES and never COUNT,
        // so it can never turn a pass into a fail. `check` decides
        // status from the count alone.
        return check("orphans", orphansDescription, found + Violations(0, informational.samples))
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

    // ---- 12: compaction group claims (V15) ---------------------------------

    /**
     * `hog_compaction_claim` holds only claims some maintainer is
     * WORKING ON.
     *
     * The state a claim may legitimately be in is narrow, and that is
     * what makes it checkable. A claim is taken immediately before a
     * rewrite. What happens next depends on the outcome, and the
     * asymmetry is deliberate: a group that did NOT commit releases its
     * claim at once, so the files stay immediately retryable; a group
     * that DID commit KEEPS the row on a short lease
     * (`HOGLAKE_COMPACTION_COMMITTED_CLAIM_TTL_SECONDS`), because a
     * sibling maintainer's plan formed before the commit still names
     * those now-dead inputs and the row is what turns its arrival into
     * a counted skip rather than a wasted rewrite. Either way the row is
     * gone within a lease, and the bulk purge at the head of the next
     * sweep is what removes it. So a row that is both EXPIRED and OLD
     * means the purge is not running — which it is not if the sweep
     * stops reaching its head for this catalog, and which it also is not
     * if COMPACTION ITSELF was turned off after having run: the purge
     * lives at the head of a sweep, and a loop set back to
     * `HOGLAKE_COMPACTION_INTERVAL_MS=0` runs no sweeps at all. (The
     * purge is deliberately not gated on
     * `HOGLAKE_COMPACTION_CLAIMS_ENABLED`, so turning only the CLAIMS
     * off still clears what they left behind.)
     *
     * Every arm therefore requires the claim to be EXPIRED, and that is
     * load-bearing rather than incidental. A LIVE claim says a
     * maintainer is working, or has just finished working, and the check
     * has nothing to say about it: not about its files being
     * end-snapshotted (that is the ordinary race, and on the kept-claim
     * path it is the NORMAL state of every committed group), and not
     * about its table being dropped inside the lease. Flagging either
     * would fire on correct behaviour — which the first draft of this
     * check did, turning `/verify` red for a full lease after any drop
     * of a table compaction had just touched.
     *
     * And the check asserts nothing about correctness, because a claim
     * carries none: it is an optimization, so a violation here means
     * compaction is doing redundant work or leaking rows, never that
     * anything committed wrongly.
     */
    private fun compactionClaims(
        h: Handle,
        catalogId: Long,
    ): VerifyCheck {
        // (a) expired long ago and still present: nothing purged it.
        val stale =
            violations(
                h,
                catalogId,
                """
                SELECT table_id, group_key, claimant,
                       EXTRACT(EPOCH FROM (now() - expires_at))::bigint AS expired_seconds
                FROM hog_compaction_claim
                WHERE catalog_id = :c
                  AND expires_at <= now() - make_interval(secs => :claimMaxAge)
                """,
                orderBy = "expired_seconds DESC, table_id, group_key",
                binds = mapOf("claimMaxAge" to compactionClaimMaxAgeSeconds.toDouble()),
            ) { rs ->
                "table_id=${rs.getLong("table_id")} group_key='${rs.getString("group_key")}' " +
                    "claimed by ${rs.getString("claimant")} expired " +
                    "${rs.getLong("expired_seconds")}s ago and was never purged (bound " +
                    "${compactionClaimMaxAgeSeconds}s)"
            }
        // (b) an EXPIRED claim on a table that no longer exists, or was
        // dropped. The FK cascades on catalog, not on table, so a
        // dropped table's claims are only removed by the release or the
        // purge.
        //
        // The expiry predicate is not decoration, and it carries arm
        // (a)'s GRACE for the same reason arm (a) does.
        //
        // A COMMITTED group keeps its claim for a lease on purpose, so
        // dropping a table compaction has just touched leaves live
        // claims on a dropped table as the ORDINARY state — without the
        // predicate this check turned red for a whole lease on a catalog
        // where nothing was wrong. And expiry alone is not enough
        // either: a claim expires between sweeps and is removed by the
        // NEXT sweep's purge, so the window in between is a correct
        // system, not a leak. Both arms therefore ask the same question
        // — has this row outlived every mechanism that should have
        // removed it — and $compactionClaimMaxAgeSeconds seconds past
        // expiry is far beyond any sweep interval.
        //
        // Leak detection survives intact: a claim nothing releases or
        // purges stays expired forever, so it crosses the grace and is
        // flagged.
        val orphaned =
            violations(
                h,
                catalogId,
                """
                SELECT c.table_id, c.group_key,
                       (t.table_id IS NULL) AS missing
                FROM hog_compaction_claim c
                LEFT JOIN hog_table t
                  ON t.catalog_id = c.catalog_id AND t.table_id = c.table_id
                WHERE c.catalog_id = :c
                  AND c.expires_at <= now() - make_interval(secs => :claimMaxAge)
                  AND (t.table_id IS NULL OR t.dropped_snapshot IS NOT NULL)
                """,
                orderBy = "table_id, group_key",
                binds = mapOf("claimMaxAge" to compactionClaimMaxAgeSeconds.toDouble()),
            ) { rs ->
                val why = if (rs.getBoolean("missing")) "no longer exists" else "is dropped"
                "table_id=${rs.getLong("table_id")} group_key='${rs.getString("group_key")}' " +
                    "claims a group of a table that $why"
            }
        return check("compaction_claims", compactionClaimsDescription, stale + orphaned)
    }

    // ---- descriptions ------------------------------------------------------

    /**
     * Instance-level, not a constant: it NAMES the bound and the
     * cadence it is judged against, both of which are per-process, so a
     * report from a service built with different ones must not quote
     * the defaults. [stagingTicketsDescription] below is the same shape
     * for the same reason.
     */
    private val orphansDescription: String =
        "A dropped table's rows leave in two stages, and this asserts both of them finished. " +
            "DDL: the drop end-snapshots the table's columns and its versioned name row in the " +
            "drop snapshot, so a live (end_snapshot IS NULL) hog_column or hog_table_version row " +
            "on a table carrying a dropped_snapshot is a HALF-DROPPED table — a row no read path " +
            "can reach and no sweep will ever collect, and these two arms are now the only thing " +
            "asserting that the drop's three UPDATEs all ran. FILES: the drop deliberately does " +
            "NOT touch them. That pass was O(rows) under the per-catalog commit lock and could " +
            "not complete on a large table, so a dropped table's file rows stay live and the " +
            "paced retirement sweep deletes them later — which it may only do once the drop " +
            "snapshot has sunk to or below the catalog's expiry floor, because above the floor " +
            "they are still readable by time travel. Live file rows on a dropped table are " +
            "therefore the NORMAL state, and a violation only when the table is at or below the " +
            "floor AND has been retirement-eligible for longer than " +
            "$retirementOrphanGraceSeconds seconds — meaning retirement has seen it (the sweep " +
            "stamps hog_table.retirement_eligible_at on first observation) and has not drained " +
            "it. That bound is an operational judgement rather than a derivation: retirement is " +
            "paced against the commit lock and against the cleanup queue, so a large table " +
            "legitimately takes many runs (" +
            (
                if (retirementIntervalMs > 0) {
                    "HOGLAKE_RETIREMENT_INTERVAL_MS, every $retirementIntervalMs ms here"
                } else {
                    "HOGLAKE_RETIREMENT_INTERVAL_MS, disabled in this process — the loop belongs " +
                        "to one maintenance workload"
                }
            ) +
            "). A catalog with no snapshot retention never advances its floor and therefore never " +
            "retires anything, which is correct and can never trip this check; the live rows it " +
            "is holding appear instead as informational sample lines on a PASSING check, so the " +
            "storage they are keeping is visible to somebody."

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

    /**
     * The other description that names its own bound: the claim-age
     * grace, which is a constructor field and therefore not a constant.
     */
    private val compactionClaimsDescription: String =
        "hog_compaction_claim holds only groups a maintainer is working on RIGHT NOW. A claim is " +
            "an OPTIMIZATION and never authorization: it lets a second maintenance replica's " +
            "planner skip a group the first one is already rewriting, so the two do not both " +
            "spend a rewrite and an upload for one of them to be discarded at commit. " +
            "Correctness against a concurrent rewrite is, and stays, the plan-to-commit " +
            "re-verification under the per-catalog commit lock, which is why nothing here can " +
            "mean a wrong commit — only redundant work or leaked rows. The lifecycle is narrow, " +
            "and asymmetric on purpose: a claim is taken immediately before the rewrite; a group " +
            "that does NOT commit releases it at once, so its files stay immediately retryable; " +
            "a group that DOES commit KEEPS the row on a short lease " +
            "(HOGLAKE_COMPACTION_COMMITTED_CLAIM_TTL_SECONDS), because a sibling maintainer's " +
            "plan formed before that commit still names the now-dead inputs and the row is what " +
            "turns its arrival into a counted skip instead of a wasted rewrite. Either way the " +
            "row is gone within a lease, and the bulk purge at the head of the next sweep is what " +
            "removes it. Two states contradict that, and BOTH require the claim to be more than " +
            "$compactionClaimMaxAgeSeconds seconds past its expiry — a claim expires between " +
            "sweeps and is removed by the NEXT sweep's purge, so the window in between is a " +
            "correct system rather than a leak. A claim that outlives that grace means nothing " +
            "purged it: the sweep is no longer reaching its head for this catalog, or compaction " +
            "itself was turned off after having run (the purge lives at the head of a sweep, so " +
            "HOGLAKE_COMPACTION_INTERVAL_MS=0 runs none; it is deliberately NOT gated on " +
            "HOGLAKE_COMPACTION_CLAIMS_ENABLED, so turning only the claims off still clears " +
            "them). Such a claim on a table that is dropped or gone outlived the thing " +
            "it was claiming — the foreign key cascades on the CATALOG only, so a table's claims " +
            "are removed by the release or the purge and by nothing else. A LIVE claim is never a " +
            "violation, whatever its files or its table are doing: it says a maintainer is " +
            "working or has just finished, which on the kept-claim path is the normal state of " +
            "every committed group."

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
         * See [compactionClaimMaxAgeSeconds]: an hour past expiry. The
         * purge runs once per catalog per sweep, and a sweep interval is
         * measured in seconds, so an hour is many orders of the honest
         * bound — deliberately, because the consequence of a false
         * positive here is an operator chasing a leak that is not one.
         */
        const val DEFAULT_COMPACTION_CLAIM_MAX_AGE_SECONDS = 60L * 60

        /**
         * A day. See the constructor parameter for why it is generous;
         * the short version is that it bounds "retirement is not
         * running at all", not "retirement is slow".
         */
        const val DEFAULT_RETIREMENT_ORPHAN_GRACE_SECONDS = 24L * 60 * 60

        /**
         * The informational arm, `internal` so the plan test EXPLAINs
         * what production runs. It is the one `orphans` sub-query whose
         * cost is not bounded by the number of dropped tables alone, so
         * it is the one that needs a budget.
         */
        internal const val ORPHANS_INFORMATIONAL_SQL: String =
            """
                SELECT t.table_id, n.live_rows, t.dropped_snapshot,
                       (t.retirement_eligible_at IS NOT NULL) AS eligible
                FROM hog_table t
                CROSS JOIN LATERAL (
                    SELECT count(*) AS live_rows FROM (
                        SELECT 1 FROM hog_data_file f
                        WHERE f.catalog_id = t.catalog_id
                          AND f.table_id = t.table_id
                          AND f.end_snapshot IS NULL
                        ORDER BY f.catalog_id, f.table_id, f.begin_snapshot
                        LIMIT :sampleCap
                    ) capped
                ) n
                WHERE t.catalog_id = :c
                  AND t.dropped_snapshot IS NOT NULL
                  AND n.live_rows > 0
            """

        /**
         * Live rows the `orphans` informational line counts per dropped
         * table before it stops and says "at least N".
         *
         * The cap is what turns an O(live manifest) aggregate into a
         * bounded one: the whole arm costs `dropped tables x this`,
         * and on gigahog-prod-us's `main.events_raw` the uncapped form
         * would count three million rows to print one line. 10,000 is
         * already "a lot" to an operator reading it.
         */
        const val INFORMATIONAL_ROW_CAP = 10_000L

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
         * `hog_data_file` and `hog_delete_file` are indexed on (catalog,
         * file id) — their hot predicate — so a path lookup on either
         * has no access path at all. `hog_file_removal` is the partial
         * exception since V16: `hog_file_removal_undrained_path`
         * (catalog_id, path) WHERE drained_at IS NULL serves a path
         * lookup, but only over the UNDRAINED rows, and several of these
         * checks read the settled ledger as well.
         *
         * A missing access path is affordable exactly once per query:
         * ONE scan of the manifest, hashed, joined against the candidate
         * set. It is a catastrophe per candidate, and the difference is
         * invisible on any catalog small enough to be a fixture.
         *
         * V16 also hands the planner a NEW OPTION on
         * `upload_claims.registered_but_queued` below, whose join to
         * `hog_file_removal` is on `(catalog_id, path)` with
         * `drained_at IS NULL` — exactly the index's key and predicate.
         * A nested loop over `hog_upload` probing that index is now
         * available, and it would NOT be a defect: the loop count is
         * the registered claims, not the manifest, and each probe is a
         * descent rather than a scan. Measured on the 50k fixture it is
         * not what the planner takes — the check still plans as a MERGE
         * JOIN on `path`, driving `hog_upload` from
         * `hog_upload_catalog_id_path_key` and `hog_file_removal` from
         * a Bitmap Index Scan on `hog_file_removal_drain`, 77 buffers,
         * every node at `loops=1`. Which plan it takes is the planner's
         * business; what must not happen is a per-candidate SCAN, and
         * that is what the plan test asserts (it measures `loops=`, so
         * a nested loop that DID appear would still have to justify
         * itself there rather than being waved through).
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
