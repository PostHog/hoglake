package com.posthog.hoglake.service

import com.posthog.hoglake.Config
import com.posthog.hoglake.Database
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.ExpiryResult
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.ExpiryGauges
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.persistence.MaintenanceRunStore
import com.posthog.hoglake.persistence.OffsetRepo
import com.posthog.hoglake.persistence.PartialResult
import com.posthog.hoglake.persistence.Pg
import io.github.oshai.kotlinlogging.KotlinLogging
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
import java.time.Duration

/**
 * Consumer-aware, incremental snapshot expiry (README.md §6). A sweep
 * is TWO PHASES: a short FLOOR ADVANCE under the per-catalog commit
 * lock, and a paged FILE PURGE outside it.
 *
 * # Why it is two phases (the 2026-10-01 prod-us incident)
 *
 * It used to be one transaction, and the file GC rode it: one
 * unbounded `DELETE FROM hog_data_file WHERE end_snapshot <=
 * newEarliest` under the commit lock, plus its `ON DELETE CASCADE` into
 * hog_file_column_stats (26 rows per file, a 66 GiB relation),
 * hog_file_partition_value and hog_delete_file. Compaction 1.3.7
 * commits six groups of up to 2,048 files, so each sweep met one whole
 * compaction wave: 12,288 ended rows, 17-25 s of lock hold per minute,
 * ten commits queued behind it when sampled, the commit-lock wait p99
 * pinned at the 30 s admission bound for sixteen hours from 03:30 UTC,
 * and API p99 of 20-60 s across routes because parked commits held the
 * API pods' pool connections. RDS was idle at 8% CPU the whole time:
 * the cost was never throughput, it was the hold. Shortening the sweep
 * interval to 15 s (charts #16727) shortens each hold and leaves the
 * DUTY CYCLE exactly where it was, and millpond's prod promotion raises
 * the file arrival rate ~5x (plus a 10B-event Kafka backlog to catch
 * up), so the ended-row rate scales with it.
 *
 * ONLY THE FLOOR ADVANCE NEEDS THE LOCK. The file rows do not: a row
 * with `end_snapshot <= earliest_snapshot_id` is below the floor, and a
 * read at a snapshot below the floor is REFUSED with 410
 * (`CatalogService.resolveReadSnapshot` / `TimeTravelRepo.expiryFloor`),
 * so no reader can reach it and no writer updates it. See [sweep] for
 * the full correctness argument and the file:line citations.
 *
 * # Phase A, under the lock, bounded ([advanceBoundMs])
 *
 * Superseded-offset release, the floor math, the snapshot range delete
 * (cascading hog_snapshot_change), the floor advance, and the versioned
 * DDL tables. Still ONE transaction, so expiry serializes with
 * commits/DDL and a sweep can never observe (or leave) a half-advanced
 * floor.
 *
 * # Phase B, outside the lock, paged ([purge])
 *
 * The data-file and delete-file rows below the floor, in pages of
 * [purgePage] under a run budget of [purgeBudgetMs], each page its own
 * transaction under its own `statement_timeout`, each page's delete and
 * its `hog_file_removal` insert in the SAME statement so no path is
 * ever queued without its row going or vice versa.
 *
 * The floor math: with `earliest` the current earliest_snapshot_id,
 * the sweep computes
 *
 *   newEarliest = min( firstFresh ?: +inf,   // oldest snapshot inside the
 *                                            // retention window survives
 *                      head,                 // head NEVER expires
 *                      minConsumerOffset,    // consumer must still read
 *                                            // from its committed offset
 *                                            // (consumer_floor = true only)
 *                      earliest + batchSize) // incremental: bounded work
 *
 * where firstFresh = min snapshot_id with snapshot_time >= now() -
 * retention (so every snapshot below newEarliest is strictly older than
 * the cutoff, even under non-monotone clocks). Snapshots in
 * [earliest, newEarliest) are range-deleted; newEarliest <= earliest is
 * a zero-work sweep. flooredByConsumer names the pinning consumer iff
 * the consumer constraint bound the sweep below what time/head/batch
 * would have allowed — that includes a zero-work sweep the consumer
 * caused (page-worthy: a lagging consumer is pinning retention).
 *
 * File GC is PHASE B and no longer rides that transaction: a file row
 * whose end_snapshot <= the committed floor is invisible at every
 * surviving snapshot, so its path is queued into hog_file_removal (a
 * *suggestion* for CleanupService, never an authorization — README.md
 * §8) and the row deleted (FK cascades take stats and partition
 * values), a page at a time. Delete-vector rows go FIRST, including
 * live DVs whose data file is expiring — deleting the data-file row
 * would cascade them away un-queued, orphaning the object — and the
 * data-file arm does not start until the DV arm has DRAINED, which is
 * what keeps that ordering true across a paged walk.
 *
 * A fifth step applies the same reachability rule to the accumulating
 * versioned DDL tables (hog_table_version, hog_column,
 * hog_partition_spec, hog_sort_spec, hog_view): rows with
 * end_snapshot <= newEarliest are invisible at every retained snapshot
 * and are deleted — DDL churn no longer grows them without bound. The
 * floor advance also captures the new floor snapshot's snapshot_time
 * into hog_catalog.earliest_snapshot_time (the reconciliation anchor
 * that 410s cite once the snapshot rows below the floor are gone).
 */
class ExpiryService(
    private val jdbi: Jdbi,
    /**
     * File rows one phase-B page deletes (HOGLAKE_EXPIRY_PURGE_PAGE).
     * Constructor-tunable so a test can make the WALK observable —
     * several pages, a run the budget stops — without seeding a
     * production-sized manifest.
     */
    private val purgePage: Int = PURGE_PAGE,
    /**
     * Wall clock phase B may spend per sweep per catalog
     * (HOGLAKE_EXPIRY_PURGE_BUDGET_MS). 0 is legal and means "advance
     * the floor, purge nothing this sweep", which is what a test uses to
     * make the budget's stop observable without sleeping.
     */
    private val purgeBudgetMs: Long = PURGE_BUDGET_MS,
    /**
     * The commit admission bound ([Config.commitLockTimeoutMs]), used
     * ONLY to derive [advanceBoundMs]. Expiry's own lock acquisition is
     * unbounded by design (a background sweep has no caller to 503), so
     * this is a number the phase-A bound is measured against rather than
     * one applied to a wait.
     */
    private val commitLockTimeoutMs: Long = CommitService.DEFAULT_COMMIT_LOCK_TIMEOUT_MS,
) {
    /**
     * The production wiring, and the only place the knobs come from
     * [Config] — `CleanupService`'s reason for the same shape: a
     * derivation stated in `App.kt` is a derivation no test can reach.
     */
    constructor(jdbi: Jdbi, config: Config) : this(
        jdbi = jdbi,
        purgePage = config.expiryPurgePage,
        purgeBudgetMs = config.expiryPurgeBudgetMs,
        commitLockTimeoutMs = config.commitLockTimeoutMs,
    )

    private val log = KotlinLogging.logger {}

    /** The run ledger; records after the sweep resolves, never inside it. */
    private val runStore = MaintenanceRunStore(jdbi)

    /**
     * The page size each (catalog, arm) walk SETTLED ON, carried across
     * sweeps for the life of the process.
     *
     * WITHOUT IT THE HALVING LADDER CANNOT CONVERGE ON A TIMEOUT, which
     * is the only failure mode production actually has. A rung that dies
     * on [PURGE_STATEMENT_TIMEOUT] costs five seconds, and
     * [PURGE_BUDGET_MS] is ten — so a sweep affords about two rungs, and
     * a page that needs 1,000 -> 125 would spend every sweep forever
     * burning its budget on 1,000 and 500 and never reach a size that
     * commits. With the hint, sweep one ends at 250, sweep two fails once
     * and then commits pages at 125, sweep three spends its whole budget
     * purging. The stack measurement that showed nine halvings in one
     * sweep came from a trigger that raised INSTANTLY; AGENT.md's own
     * trap, and the reason this had to be measured against `pg_sleep`.
     *
     * IN MEMORY, NOT IN THE DATABASE, and per arm rather than per
     * catalog: it is a throughput hint, not state. Losing it on a restart
     * costs one sweep's rediscovery, which is cheap precisely because
     * the ladder converges in log2(page) rungs — and the two arms meet
     * different tables, so a vector arm's cost says nothing about a data
     * arm's. `RetirementService` carries NO hint of this kind at all
     * (#263): its per-row cost is flat in the batch, so it reads a
     * cancelled batch as a COLD one, counts it, and retries the same
     * size next run. The difference worth testing before this hint is
     * copied anywhere else is whether the unit's cost is flat in its
     * size; phase A's is not (it carries a per-snapshot change-row
     * factor), which is why its ladder is here.
     *
     * It only ever moves DOWN by halving and UP by [PAGE_REGROW_AFTER]
     * clean pages, so a transient slowness cannot pin a catalog at a
     * small page forever, and a genuinely expensive table is not
     * rediscovered from scratch every sweep.
     */
    private val settledPage = java.util.concurrent.ConcurrentHashMap<String, PageHint>()

    /**
     * A walk's carried state: the page size it settled on and how many
     * consecutive FULL pages have committed at that size.
     *
     * THE STREAK HAS TO BE CARRIED TOO, and that is the half an earlier
     * version missed. [PAGE_REGROW_AFTER] is 20 clean pages, and a sweep
     * at the default budget runs ten to fourteen — so with the streak
     * reset per walk a catalog that does fewer than twenty pages a sweep
     * could never re-grow at all, and a page halved during one bad
     * afternoon was permanent for the life of the process. Carrying it
     * makes the re-grow a decision taken ACROSS sweeps, which is what
     * the constant's own KDoc says it is.
     */
    private data class PageHint(val page: Int, val clean: Int)

    init {
        require(purgePage > 0) { "expiry purge page must be positive (got $purgePage)" }
        // 0 is legal — see the knob's KDoc. Negative is not: it would
        // make the deadline already past in a way that reads like a
        // configuration, not like a choice.
        require(purgeBudgetMs >= 0) {
            "expiry purge budget must not be negative (got $purgeBudgetMs)"
        }
    }

    /**
     * The per-statement bound phase A sets transaction-locally, after
     * taking the commit lock.
     *
     * `RetirementService.callBoundMs`'s arithmetic, for its reason: a
     * `statement_timeout` applies to each statement SEPARATELY, so a
     * bound of `admission / 2` across [BOUNDED_STATEMENTS_PER_ADVANCE]
     * statements is a HOLD of eleven times that. Dividing by the count
     * makes the number say what it claims:
     *
     *     per statement = min(session / 4, admission / 2) / statements
     *     whole hold   <= admission / 2
     *
     * At the defaults (session 60 s, admission 30 s, 11 statements) that
     * is 1,363 ms each and at most 15 s of hold — where 11 is the
     * MAXIMUM count rather than the exact one (a catalog with
     * `consumer_floor` off skips the floor read and runs 10), so the
     * derived bound is conservative in the direction that matters — against a phase A
     * whose statements are a PK read, two aggregate reads over
     * `hog_snapshot`, a range delete bounded by `batchSize`, one
     * single-row UPDATE and five deletes over tables whose whole row
     * counts are in the thousands. It is a bound on the pathological
     * case, not a budget anything normal spends.
     *
     * Floored at 1 ms, because `SET statement_timeout = 0` means
     * UNLIMITED and a rounding-down to zero would silently remove the
     * bound. A zero (unbounded) admission bound drops that term of the
     * minimum rather than collapsing the expression.
     */
    internal val advanceBoundMs: Long =
        run {
            val session = Database.SESSION_INIT_SQL_STATEMENT_TIMEOUT.toMillis() / 4
            val whole = if (commitLockTimeoutMs > 0) minOf(session, commitLockTimeoutMs / 2) else session
            maxOf(1, whole / BOUNDED_STATEMENTS_PER_ADVANCE)
        }

    internal companion object {
        /**
         * WHICH OFFSETS CAN FLOOR EXPIRY — the source clause of the
         * consumer-floor query, shared verbatim with
         * the removed verify subsystem's `expiry_floor` check.
         *
         * The join to `hog_table` is the load-bearing part: an offset
         * counts only while its table identity still exists (any
         * incarnation, dropped included — offsets survive drops by
         * design), because an offset naming a uuid the catalog no longer
         * has cannot be advanced by anyone and must not pin retention
         * forever. The check asserts the invariant this query enforces,
         * so it asks THIS clause rather than a restatement of it: a copy
         * that lost the join would let the check pass on exactly the
         * rows the sweep ignores (AGENT.md — a parity test that
         * restates its subject asserts only that the file compiles).
         *
         * Binds `:catalogId`. No interpolated values (invariant 9
         * intact).
         */
        internal const val FLOOR_CANDIDATE_OFFSETS: String =
            """
            FROM hog_consumer_offset o
            JOIN hog_table t
              ON t.catalog_id = o.catalog_id AND t.table_uuid = o.table_uuid
            WHERE o.catalog_id = :catalogId
            """

        /**
         * ONE PAGE of PHASE B's SUPERSEDED-delete-vector arm, `internal`
         * so the plan test EXPLAINs the SQL PRODUCTION runs rather than a
         * lookalike.
         *
         * RENAMED FROM `DELETE_FILE_EXPIRY_SQL`, which is safe where the
         * data-file constant's name is not: `V19__data_file_ended_index
         * .sql`'s header cites [DATA_FILE_EXPIRY_SQL] by name and a
         * migration file's text is part of its Flyway CHECKSUM, so that
         * one keeps its name; no `.sql` file mentions this one.
         *
         * ONE ARM, NOT TWO, and the split is the whole of the 2026-10-01
         * review's B2. The statement used to be an `OR`: superseded DVs
         * (their own `end_snapshot` in range) OR live DVs riding a data
         * file that was about to be purged, the second arm existing
         * because `hog_delete_file`'s FK to `hog_data_file` is
         * `ON DELETE CASCADE` (`V1__init.sql`), so purging a data-file
         * row takes its vectors away WITHOUT queueing their paths — a
         * permanently leaked puffin object that `CleanupService` never
         * hears about, and that the removed `/verify`'s `orphans` arm
         * could not have found either.
         *
         * That ordering was enforced by "the delete-vector arm's page
         * came back SHORT", and a full page comes back short whenever
         * anything else removed one of its rows between the InitPlan and
         * the DELETE — another replica's expiry loop (no leader
         * election; the interval defaults ON on every pod), a manual
         * `POST /maintenance/expire` racing the loop (phase B takes no
         * lock, so these no longer serialize), or
         * `RetirementService.DV_DELETE_SQL`, which deletes exactly these
         * rows holding only the commit lock. One such race and the
         * data-file arm would start with eligible vectors still present.
         *
         * So the riding-DV arm is gone from here and lives INSIDE
         * [DATA_FILE_EXPIRY_SQL]'s page, where it is a fact about that
         * page's own rows in that page's own transaction rather than a
         * claim about a whole table. What is left here is the population
         * that has nothing to ride: a superseded vector whose data file
         * is still live, or not yet purged.
         *
         * PAGED BY `ctid`, with the eligibility predicate repeated on the
         * OUTER delete — see [DATA_FILE_EXPIRY_SQL] for why the ctid
         * array alone is not a qual. Termination: the inner
         * `SELECT ... LIMIT :page` is a scan that stops once it has
         * filled the page, so a page that reports fewer than `:page`
         * EXAMINED rows reached the end of the relation. The walk tests
         * the examined count, not the deleted count, exactly so that a
         * concurrent deleter cannot end it early.
         *
         * NO INDEX SERVES IT YET, and now one could. V19 declined to
         * index the old `OR` because the planner reads an `OR` whose
         * second arm is a correlated `EXISTS` as one pass and would never
         * choose a `(catalog_id, end_snapshot)` index for the first arm
         * — "splitting the statement into its two arms is what would
         * make an index choosable, and that is a change to the sweep's
         * behaviour, ticketed separately". This IS that split, so a
         * partial `hog_delete_file_ended (catalog_id, end_snapshot)
         * WHERE end_snapshot IS NOT NULL` is now the right index and is
         * ticketed with the migration it needs. Until it lands this page
         * is O(the catalog's delete-file rows) when nothing is eligible,
         * which is why [purge] probes before it walks and why the whole
         * of phase B is skipped for a catalog whose floor has never
         * advanced.
         *
         * Binds `:catalogId`, `:newEarliest`, `:page`.
         */
        internal const val SUPERSEDED_DELETE_FILE_PURGE_SQL: String =
            """
            WITH page AS (
                SELECT dv.ctid AS tid FROM hog_delete_file dv
                WHERE dv.catalog_id = :catalogId
                  AND dv.end_snapshot IS NOT NULL
                  AND dv.end_snapshot <= :newEarliest
                LIMIT :page
            ),
            doomed AS (
                DELETE FROM hog_delete_file
                WHERE ctid = ANY (ARRAY(SELECT tid FROM page))
                  AND end_snapshot IS NOT NULL AND end_snapshot <= :newEarliest
                RETURNING path
            ),
            queued AS (
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                SELECT :catalogId, path, 'delete', 'snapshot_expiry' FROM doomed
                RETURNING removal_id
            )
            SELECT (SELECT count(*) FROM page) AS examined,
                   0::bigint AS data_purged,
                   (SELECT count(*) FROM queued) AS delete_purged
            """

        /**
         * The probe that decides whether [SUPERSEDED_DELETE_FILE_PURGE_SQL]
         * is worth a transaction.
         *
         * `EXISTS` with the scan's own predicate: on the catalogs that
         * have no superseded vectors — which is every catalog whose
         * writers only append, i.e. all of millpond's — it is one scan
         * that finds nothing and opens no transaction, and on a catalog
         * that has some it stops at the first. It does NOT make the arm
         * bounded in rows examined (only the index above would); it
         * makes the arm free in the case that is universal today, and it
         * is the honest half of the fix.
         */
        internal const val SUPERSEDED_DELETE_FILE_PROBE_SQL: String =
            """
            SELECT EXISTS (
                SELECT 1 FROM hog_delete_file dv
                WHERE dv.catalog_id = :catalogId
                  AND dv.end_snapshot IS NOT NULL
                  AND dv.end_snapshot <= :newEarliest
            )
            """

        /**
         * ONE PAGE of PHASE B's data-file arm — the statement the
         * 2026-10-01 incident was, bounded, and the whole of the
         * delete-vector ordering rule. `internal` so the plan test
         * EXPLAINs the SQL PRODUCTION runs rather than a lookalike.
         *
         * THE NAME IS KEPT THOUGH THE STATEMENT IS NOW PAGED, and that
         * is not a style choice: `V19__data_file_ended_index.sql`'s
         * header cites this constant BY NAME, and a migration file's
         * text is part of its Flyway CHECKSUM — editing it fails
         * `validate` on every deployed replica until somebody runs
         * `flyway repair`. The KDoc carries the news instead.
         *
         * # Three statements in one, and why they are one
         *
         *  1. `page` — up to `:page` eligible data-file ctids, computed
         *     ONCE (it is referenced twice, so Postgres materializes it
         *     rather than inlining) and reported back as `examined`.
         *  2. `doomed` — the data-file rows themselves, returning both
         *     the path (for the queue) and the ID (for step 3).
         *  3. `vectors` — every `hog_delete_file` row riding a data file
         *     `doomed` ACTUALLY DELETED, deleted and QUEUED. No
         *     `end_snapshot` predicate on purpose: live or superseded, a
         *     vector of a doomed data file is about to be taken by the
         *     FK's `ON DELETE CASCADE` (`V1__init.sql`) whatever its own
         *     state, and a cascaded delete queues nothing — a
         *     permanently leaked puffin object with no row left to find
         *     it. Driven by `hog_delete_file_data_lookup
         *     (catalog_id, data_file_id)`, so it is an index probe per
         *     deleted row rather than a scan.
         *
         * `vectors` KEYS OFF `doomed`, NOT OFF `page`, and that is a
         * correctness fix rather than a tidier dependency. Keyed off
         * `page` the two halves disagreed about which files are actually
         * going: `doomed` repeats the eligibility predicate and SKIPS a
         * row a concurrent writer moved out of eligibility, while
         * `vectors` deleted that surviving file's vectors anyway and
         * queued their puffins for physical removal — a LIVE data file
         * losing its deletion vectors, so rows a vector masked come back
         * and the object is queued for deletion. That is worse than the
         * leak the re-check was added to prevent. Reading `doomed`
         * instead makes the dependency explicit, which also fixes the
         * CTE evaluation order (Postgres runs `doomed` first because
         * `vectors` reads its tuplestore) while leaving the cascade's
         * AFTER-ROW timing intact.
         *
         * ONE STATEMENT, ONE TRANSACTION, ONE SNAPSHOT is what makes the
         * ordering a fact rather than a hope. The previous shape ran the
         * vectors as a separate ARM and started the data-file arm once
         * that arm's page "came back short"; a full page comes back
         * short whenever anything else deleted one of its rows first
         * (another replica's loop, a manual trigger racing it,
         * `RetirementService.DV_DELETE_SQL` under only the commit lock),
         * and one such race leaked objects permanently. Here the
         * vectors of THIS page's files are removed in the same
         * transaction that removes the files, so there is no window and
         * no cross-arm gate to get wrong. The cascade still fires at end
         * of statement and finds nothing, which is the point.
         *
         * THE OUTER DELETES REPEAT THE ELIGIBILITY PREDICATE, and that
         * is a correctness fix rather than belt-and-braces. Under READ
         * COMMITTED a row updated between the InitPlan and the DELETE
         * gives `TM_Updated`; `ExecDelete` follows `t_ctid` into
         * EvalPlanQual and `nodeTidscan.c`'s `TidRecheck` returns true
         * UNCONDITIONALLY, so the new version is deleted even though its
         * ctid is not in the array. With the ctid array as the only
         * qual, the eligibility predicate would never be re-evaluated,
         * and phase B's safety would rest entirely on the convention
         * that no writer moves a row out of `end_snapshot <= floor`.
         * Filter quals ARE re-applied after `recheckMtd` in `ExecScan`,
         * so repeating the predicate makes the statement check for
         * itself what the convention asserts. What that buys is stated
         * carefully, because an earlier draft overstated it: a future
         * writer that broke the convention costs a SKIPPED ROW — the
         * page purges one fewer than it examined, the walk carries on
         * (it tests `examined`), and the row waits for a sweep that
         * finds it eligible. It is only a complete defence because
         * `vectors` keys off `doomed`: a skipped file keeps its vectors,
         * so there is no half-deleted file whose puffin is queued.
         *
         * PAGED BY `ctid`, and the two forms that look more natural are
         * both wrong — `CleanupService.RECEIPT_PURGE_PAGE_SQL`'s KDoc
         * argues this at length and both arguments land here:
         *
         *  - joining the page back on the PRIMARY KEY
         *    (`(catalog_id, data_file_id) IN (page)`) lets the planner
         *    hash-join the page against a SEQUENTIAL SCAN of the
         *    catalog's whole manifest, i.e. a per-page cost of O(ten
         *    million) and an index that bought nothing;
         *  - widening the page into a RANGE (`end_snapshot <= max(page)`)
         *    is worse HERE than it is for receipts: a compaction group
         *    ends every one of its up-to-2,048 input files at ONE
         *    snapshot id, so a range delete rounding up to a whole
         *    `end_snapshot` value takes a whole group, and six groups
         *    share the wave — the unbounded-delete shape this change
         *    exists to remove.
         *
         * NO CURSOR, NO SKIP CAP, unlike `CleanupService`'s
         * drained-ledger walk: the predicate IS the indexed column, so
         * the eligible rows are a dense PREFIX of the catalog's
         * `(catalog_id, end_snapshot)` range (V19's partial index) and
         * `examined < :page` means "nothing eligible is left". The walk
         * tests EXAMINED and not PURGED, deliberately: a concurrent
         * deleter of one of this page's rows lowers the purged count
         * without exhausting the eligible set, and ending the walk on
         * that would simply delay the rest to the next sweep — but
         * reporting it as "drained" is what the old cross-arm gate did
         * with the leak it caused.
         *
         * NO `ORDER BY`, AND THAT IS A MEASUREMENT RATHER THAN A
         * PREFERENCE. An earlier version carried `ORDER BY end_snapshot`
         * to take the oldest corpses first, and
         * `V19DataFileEndedIndexMigrationIntegrationTest` showed what
         * the planner does with it: a Bitmap Heap Scan over the WHOLE
         * eligible set feeding a Sort feeding the Limit — 2,000 heap
         * blocks to produce a page of 1,000 on the 200k-row fixture. A
         * sort cannot be stopped early, so the page bounded the rows
         * DELETED and left the rows EXAMINED at O(the eligible set),
         * which at a backlog of a million rows is the unbounded read
         * this whole change exists to remove. Termination does not need
         * the ordering: every eligible row is equally dead, a page's
         * rows leave the set when it commits, so successive pages are
         * strictly new work whatever order they take them in.
         *
         * Binds `:catalogId`, `:newEarliest`, `:page`. Returns
         * `examined`, `data_purged`, `delete_purged`.
         */
        internal const val DATA_FILE_EXPIRY_SQL: String =
            """
            WITH page AS (
                SELECT df.ctid AS tid
                FROM hog_data_file df
                WHERE df.catalog_id = :catalogId
                  AND df.end_snapshot IS NOT NULL
                  AND df.end_snapshot <= :newEarliest
                LIMIT :page
            ),
            doomed AS (
                DELETE FROM hog_data_file
                WHERE ctid = ANY (ARRAY(SELECT tid FROM page))
                  AND end_snapshot IS NOT NULL AND end_snapshot <= :newEarliest
                RETURNING data_file_id, path
            ),
            vectors AS (
                DELETE FROM hog_delete_file
                WHERE catalog_id = :catalogId
                  AND ctid = ANY (
                    ARRAY(
                        SELECT dv.ctid FROM hog_delete_file dv
                        WHERE dv.catalog_id = :catalogId
                          AND dv.data_file_id IN (SELECT data_file_id FROM doomed)
                    )
                  )
                RETURNING path
            ),
            queued AS (
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                SELECT :catalogId, path, 'delete', 'snapshot_expiry' FROM vectors
                UNION ALL
                SELECT :catalogId, path, 'data', 'snapshot_expiry' FROM doomed
                RETURNING file_kind
            )
            SELECT (SELECT count(*) FROM page) AS examined,
                   (SELECT count(*) FROM queued WHERE file_kind = 'data') AS data_purged,
                   (SELECT count(*) FROM queued WHERE file_kind = 'delete') AS delete_purged
            """

        /**
         * How many file rows phase B left eligible, for the ledger's
         * `purge_remaining`.
         *
         * RUN ONLY WHEN THE PURGE STOPPED EARLY, and capped. An
         * uncapped `count(*)` over the ended index is O(the eligible
         * set), which is exactly the unbounded read the rest of this
         * file is about: after a long outage that set is the whole
         * backlog. The cap makes the number a SATURATING one — "at
         * least this many" — which is all an operator needs from it,
         * since the only decision it feeds is whether the purge is
         * keeping up.
         *
         * BOTH ARMS, summed. An earlier version counted `hog_data_file`
         * only and was reported as the sweep's `purge_remaining`, so a
         * purge stopped inside the superseded-vector arm read
         * `purge_truncated = true, purge_remaining = 0` — "stopped,
         * nothing left", which is the one thing it must never say.
         *
         * The data-file half is index-only over `hog_data_file_ended`
         * (the predicate and the projection are both in the index), so
         * its capped scan touches no heap. The vector half has no index
         * yet (see [SUPERSEDED_DELETE_FILE_PURGE_SQL]) and is a capped
         * scan of the catalog's vectors, which is why this runs only on
         * a truncated sweep.
         *
         * Binds `:catalogId`, `:newEarliest`, `:cap`.
         */
        internal const val PURGE_REMAINING_SQL: String =
            """
            SELECT (SELECT count(*) FROM (
                        SELECT 1 FROM hog_data_file
                        WHERE catalog_id = :catalogId
                          AND end_snapshot IS NOT NULL AND end_snapshot <= :newEarliest
                        LIMIT :cap
                    ) capped_data)
                 + (SELECT count(*) FROM (
                        SELECT 1 FROM hog_delete_file
                        WHERE catalog_id = :catalogId
                          AND end_snapshot IS NOT NULL AND end_snapshot <= :newEarliest
                        LIMIT :cap
                    ) capped_vectors) AS remaining
            """

        /**
         * The end-snapshotted-but-never-deleted versioned tables (DDL
         * churn grows them without bound); sweep step 5 deletes their
         * below-floor corpses. Table names are a fixed compile-time
         * vocabulary, never derived from input (invariant 9 intact).
         */
        val VERSIONED_RETENTION_TABLES =
            listOf(
                "hog_table_version",
                "hog_column",
                "hog_partition_spec",
                "hog_sort_spec",
                "hog_view",
            )

        /** See `Config.expiryPurgePage`. */
        const val PURGE_PAGE = 1_000

        /** See `Config.expiryPurgeBudgetMs`. */
        const val PURGE_BUDGET_MS = 10_000L

        /**
         * Per-statement bound on one phase-B page.
         *
         * THE RUN BUDGET CANNOT BE THIS BOUND. [PURGE_BUDGET_MS] is
         * checked BETWEEN pages, so without a statement bound a single
         * page inherits the session's 60 s (`Database.SESSION_INIT_SQL`)
         * and "one page over the budget" means a minute — which is the
         * 2026-09-28 failure mode exactly: 195 of 200 sweeps died on
         * that 60 s bound, each holding on for the whole of it.
         *
         * Five seconds, the receipt purge's figure, and against the
         * measured per-row cost (see [purge]) it is two orders of
         * magnitude of headroom: a page that cannot finish inside it is
         * a page that is too big for the rows it is meeting, which is a
         * thing to learn in five seconds with a counted failure rather
         * than in sixty.
         */
        const val PURGE_STATEMENT_TIMEOUT = "5s"

        /**
         * Ceiling on the saturating remaining-rows count
         * ([PURGE_REMAINING_SQL]).
         *
         * 100 x [PURGE_PAGE]: one hundred pages is more than any budget
         * will spend, so a value at the cap means "the purge is behind
         * by more than a sweep can catch up" — the only reading that
         * changes what an operator does. Counting further would be
         * paying index pages for a bigger number with the same meaning.
         */
        const val PURGE_REMAINING_CAP = 100_000

        /**
         * Consecutive FULL pages at a reduced size before [walk] doubles
         * it back towards `HOGLAKE_EXPIRY_PURGE_PAGE`.
         *
         * 20, which at the default page is 20,000 rows of evidence that
         * the cost has come down — more than one sweep's budget buys, so
         * a re-grow is a decision taken across sweeps rather than inside
         * one. The cost of guessing wrong is a single failed page (five
         * seconds) and a halving straight back, which is why the number
         * can be this small; the cost of NEVER re-growing is a catalog
         * pinned at a page it discovered during one bad afternoon.
         */
        const val PAGE_REGROW_AFTER = 20

        /**
         * Statements phase A runs under its own `statement_timeout`,
         * between taking the commit lock and committing: the re-read of
         * `hog_catalog`, the superseded-offset release, the
         * first-fresh-snapshot read, the consumer-floor read, the
         * snapshot range delete, the floor advance, and the five
         * [VERSIONED_RETENTION_TABLES] deletes. [advanceBoundMs] divides
         * by this so the whole HOLD, and not each statement, is what the
         * admission bound is compared against.
         *
         * THE MAXIMUM, not the exact count: a catalog with
         * `consumer_floor` off runs ten of these, not eleven. Dividing
         * by the maximum makes the derived per-statement bound slightly
         * tighter than it has to be on such a catalog, which is the
         * direction a bound should err in.
         *
         * The `SET LOCAL statement_timeout` itself is not counted: it is
         * what puts the bound in force rather than something the bound
         * applies to. The pre-lock `CatalogRepo.require` is not counted
         * either — it runs before the lock is held, so it is not part of
         * any hold.
         */
        const val BOUNDED_STATEMENTS_PER_ADVANCE = 11
    }

    /**
     * One expiry sweep for [catalog], expiring at most [batchSize]
     * snapshots. The audit event and the expired-snapshots counter are
     * emitted here, AFTER the sweep transaction has committed — and a
     * ZERO-WORK sweep (nothing expired or queued, no consumer floor in
     * play) emits no audit event at all, only an app-log debug line:
     * every-minute background no-ops must not flood the audit stream.
     * Every run (zero-work included) is recorded in the maintenance run
     * ledger — the row is the per-catalog loop-liveness signal.
     */
    fun runOnce(
        catalog: String,
        batchSize: Int,
        trigger: MaintenanceTrigger = MaintenanceTrigger.MANUAL,
    ): ExpiryResult =
        runStore.recorded(catalog, MaintenanceTask.EXPIRY, trigger) {
            runSweep(catalog, batchSize)
        }

    private fun runSweep(
        catalog: String,
        batchSize: Int,
    ): ExpiryResult {
        // Validation and catalog resolution still audit their failure
        // and rethrow unchanged, which is what the API's error mapping
        // and the ledger's `failed` row have always depended on.
        val pre =
            try {
                if (batchSize <= 0) {
                    throw HoglakeException.Validation("batch size must be positive (got $batchSize)")
                }
                // Resolved BEFORE phase A and outside its transaction,
                // because phase B needs the id whether or not phase A
                // succeeds — and the floor this read saw is what a
                // phase-B that cannot read its own is reported with.
                jdbi.withHandleUnchecked { h -> CatalogRepo.require(h, catalog) }
            } catch (e: Throwable) {
                Audit.event("expiry", catalog, null, Audit.failureOutcome(e), e.message)
                throw e
            }
        val catalogId = pre.catalogId
        val knownFloor = pre.earliestSnapshotId
        // PHASE A: the floor advance, one transaction under the
        // per-catalog commit lock, with the retirement shape's
        // halve-on-timeout.
        var advance: Advance? = null
        var advanceError: Throwable? = null
        var batch = batchSize
        var advanceHalvings = 0L
        while (advance == null) {
            try {
                advance = advanceFloor(catalog, batch)
            } catch (e: Throwable) {
                val halved = batch / 2
                if (isStatementBound(e) && halved >= 1) {
                    // THE RETIREMENT SHAPE, and the reason this loop
                    // exists at all: [advanceBoundMs] bounds each of
                    // phase A's statements, and the one that can exceed
                    // it is the snapshot range delete with its
                    // `hog_snapshot_change` cascade. Its cost is
                    // O(batch x change rows per snapshot), a product the
                    // sweep cannot know in advance — a catch-up sweep on
                    // a catalog with many change rows per snapshot is
                    // exactly the case the bound fires on, and it is
                    // also the case where making no progress is worst.
                    // Halving converges in at most log2(batch) attempts
                    // and the floor is monotone, so every attempt that
                    // commits is progress that the next sweep builds on.
                    advanceHalvings++
                    Metrics.expiryHalvings(catalog, "advance", 1)
                    // NO PAUSE AND NO RUN BUDGET here, unlike
                    // `RetirementService`'s loop, and that is a judgement
                    // rather than an oversight: the ladder is bounded at
                    // log2(batch) rungs, a rolled-back rung held the
                    // commit lock only for its own statement bound
                    // (1,363 ms at the defaults), and the corrected
                    // cascade arithmetic below makes a rung firing at all
                    // near-theoretical on a prod-us-shaped catalog. If this
                    // counter ever runs hot the pause is the next thing
                    // to add, and the duty cycle is what it would protect.
                    log.warn(e) {
                        "expiry: the floor advance for catalog '$catalog' hit its ${advanceBoundMs}ms " +
                            "statement bound at a batch of $batch snapshots and rolled back; halving to " +
                            "$halved and retrying within this run (the snapshot range delete's " +
                            "hog_snapshot_change cascade is what this bound is measuring; " +
                            "HOGLAKE_EXPIRY_BATCH is the knob)"
                    }
                    batch = halved
                    continue
                }
                advanceError = e
                break
            }
        }
        // PHASE B: the file purge, outside the lock, paged.
        //
        // UNCONDITIONAL — not gated on `advance.advanced`, and not gated
        // on phase A having SUCCEEDED. A previous sweep may have left
        // eligible rows (its budget expired, a page failed, the pod died
        // between the phases), and the floor they are below is already
        // committed, so a sweep that advances nothing is still the thing
        // that has to drain them. And phase A now FAILS precisely when
        // the backlog is largest, since that is when its statement bound
        // fires; skipping the purge there would starve it forever while
        // the ledger said only `failed`.
        val purge = purge(catalog, catalogId, knownFloor)
        Metrics.expiryPurgeRows(catalog, purge.dataRows + purge.deleteRows)
        Metrics.expiryPurgeFailures(catalog, purge.failures)
        Metrics.expiryHalvings(catalog, "purge", purge.halvings)
        ExpiryGauges.publish(catalog, purge.remaining)
        if (purge.truncated) Metrics.expiryPurgeTruncated(catalog)
        advanceError?.let { e ->
            // The run FAILED and says so — the floor did not move — but
            // what the purge did is still recorded, both as metrics
            // above and on the ledger row through [PartialResult], so an
            // operator reading `GET /maintenance/runs` is not told the
            // sweep did nothing when it drained ten thousand rows.
            Metrics.expiryAdvanceFailures(catalog)
            Audit.event("expiry", catalog, null, Audit.failureOutcome(e), e.message)
            log.error(e) {
                "expiry: the floor advance failed for catalog '$catalog' after $advanceHalvings " +
                    "halvings; the purge still ran and removed ${purge.dataRows} data-file rows " +
                    "(${purge.remaining} left eligible)"
            }
            throw advanceFailure(
                e,
                Advance(catalogId, 0, purge.floor, null, 0, advanceHalvings).toResult(purge),
            )
        }
        val result = advance!!.copy(halvings = advanceHalvings).toResult(purge)
        // The floored-by page-worthy warn lives HERE, outside the sweep
        // transaction (and outside the advisory lock): the data is already
        // in the result, and log I/O must never ride the commit tail.
        result.flooredByConsumer?.let { consumer ->
            log.warn {
                "expiry for catalog '$catalog' floored by consumer '$consumer' " +
                    "(earliest stays at ${result.newEarliestSnapshotId})"
            }
        }
        // The purge's own metrics were emitted beside the purge (they
        // have to be: that path is also reached when phase A threw and
        // this line is not). Only the advance's counter belongs here.
        Metrics.snapshotsExpired(catalog, result.snapshotsExpired)
        // A sweep that DELETED consumer positions did work, even when it
        // expired nothing — on a retention-null catalog that is the only
        // work it can do, and logging it as "nothing to do" is how the
        // stranded-offset release stayed invisible for as long as it did.
        //
        // The purge counters join the predicate for the same reason, and
        // one of them is NOT a count of work: `purgeFailures` is a count
        // of work that FAILED, and a sweep whose every page timed out
        // reports zero of everything else. Reading that as "nothing to
        // do" is precisely how the receipt purge's 58 GiB would have sat
        // there looking idle (`Metrics.commitReceiptPurgeFailures`), so
        // a failed page is loud here too — and so is a TRUNCATED one,
        // which the console's `isQuietRun` already treated as loud while
        // this predicate did not: a sweep that advanced nothing and
        // stopped with rows left emitted no audit event while the
        // console flagged it, which is the two surfaces disagreeing
        // about the same row.
        val zeroWork =
            result.snapshotsExpired == 0L && result.dataFilesQueued == 0L &&
                result.deleteFilesQueued == 0L && result.flooredByConsumer == null &&
                result.offsetsReleased == 0L && result.purgeFailures == 0L &&
                !result.purgeTruncated && result.advanceHalvings == 0L &&
                result.purgeHalvings == 0L
        if (zeroWork) {
            log.debug { "expiry sweep for catalog '$catalog': nothing to do" }
        } else {
            Audit.event(
                "expiry",
                catalog,
                null,
                outcome = "ok",
                detail =
                    "snapshots_expired=${result.snapshotsExpired} " +
                        "data_files_queued=${result.dataFilesQueued} " +
                        "delete_files_queued=${result.deleteFilesQueued} " +
                        "new_earliest=${result.newEarliestSnapshotId} " +
                        "offsets_released=${result.offsetsReleased} " +
                        "purge_pages=${result.purgePages} " +
                        "purge_failures=${result.purgeFailures} " +
                        "purge_truncated=${result.purgeTruncated} " +
                        "purge_remaining=${result.purgeRemaining} " +
                        "advance_halvings=${result.advanceHalvings} " +
                        "purge_halvings=${result.purgeHalvings}" +
                        (result.flooredByConsumer?.let { " floored_by_consumer=$it" } ?: ""),
            )
        }
        return result
    }

    /**
     * What phase A committed. Carries [catalogId] because phase B runs
     * on its own connection and must not re-resolve the catalog by name
     * (a rename between the phases would send the purge at a different
     * catalog, or at none).
     */
    private data class Advance(
        val catalogId: Long,
        val snapshotsExpired: Long,
        val newEarliest: Long,
        val flooredBy: String?,
        val offsetsReleased: Long,
        val halvings: Long = 0,
    ) {
        fun toResult(purge: Purge) =
            ExpiryResult(
                snapshotsExpired = snapshotsExpired,
                // "Rows queued for cleanup this sweep", which is the
                // rows phase B PURGED: the `hog_file_removal` insert
                // rides the delete in one statement, so the two numbers
                // are equal by construction and cannot drift. See
                // [ExpiryResult.dataFilesPurged].
                dataFilesQueued = purge.dataRows,
                deleteFilesQueued = purge.deleteRows,
                newEarliestSnapshotId = newEarliest,
                flooredByConsumer = flooredBy,
                offsetsReleased = offsetsReleased,
                dataFilesPurged = purge.dataRows,
                purgePages = purge.pages,
                purgeFailures = purge.failures,
                purgeTruncated = purge.truncated,
                purgeRemaining = purge.remaining,
                advanceHalvings = halvings,
                purgeHalvings = purge.halvings,
            )
    }

    /**
     * What phase B did, across both file arms, plus [floor] — the floor
     * it read, so a sweep whose phase A FAILED can still report where
     * the floor actually stands instead of a zero that reads like a
     * catalog that has never expired.
     */
    private data class Purge(
        val dataRows: Long = 0,
        val deleteRows: Long = 0,
        val pages: Long = 0,
        val failures: Long = 0,
        val halvings: Long = 0,
        val truncated: Boolean = false,
        val remaining: Long? = 0,
        val floor: Long = 0,
    )

    /**
     * One arm's walk, counted BY TABLE rather than by whose arm it is.
     *
     * [dataRows] is `hog_data_file` rows and [vectorRows] is
     * `hog_delete_file` rows, whichever arm removed them: the data-file
     * arm's pages produce both (its files, plus the vectors riding them
     * that its own statement takes and queues), and the superseded arm's
     * produce only vectors. Keying the field on the TABLE rather than on
     * "the arm's own population" is not cosmetic — the first draft of
     * this class had `rows` meaning different things in the two arms,
     * which made the sweep under-report `delete_files_queued` by exactly
     * the superseded arm's contribution.
     */
    private data class Walk(
        val dataRows: Long = 0,
        val vectorRows: Long = 0,
        val pages: Long = 0,
        val failures: Long = 0,
        val halvings: Long = 0,
        val drained: Boolean = false,
        val stoppedOn: String = "nothing eligible",
    )

    /**
     * PHASE A as one call: the floor advance for [batch] snapshots, in
     * one transaction holding the per-catalog commit lock under
     * [advanceBoundMs].
     *
     * Extracted from [runSweep] so the halve-on-timeout loop around it is
     * a loop over a function rather than a loop over a transaction
     * literal — and so a test can observe the bound without reaching
     * inside the sweep.
     */
    private fun advanceFloor(
        catalog: String,
        batch: Int,
    ): Advance =
        jdbi.inTransactionUnchecked { h ->
            val pre = CatalogRepo.require(h, catalog)
            Locks.acquireCatalogCommitLock(h, pre.catalogId)
            // THE BOUND GOES ON FIRST, before anything it has to bound —
            // `RetirementService.batch`'s rule, and for its reason: a
            // statement left under the SESSION's 60 s inside a
            // transaction already holding the commit lock can on its own
            // produce a hold twice the admission window. That is what
            // the 2026-09-28 sweep did 195 times in 200 runs.
            h.createQuery("SELECT set_config('statement_timeout', ?, true)")
                .bind(0, advanceBoundMs.toString())
                .mapToMap()
                .one()
            // Re-read under the lock: head/options may have moved while
            // we queued behind a committer.
            val cat = CatalogRepo.require(h, catalog)
            sweep(h, cat, batch)
        }

    /**
     * Did [e] come from a `statement_timeout` (or a cancel) rather than
     * from anything else?
     *
     * `Pg.isQueryCanceled`'s own KDoc states the precondition: a caller
     * that reads `57014` as "my own bound fired" must be one that SET
     * that bound itself, transaction-locally, for the statement it ran.
     * [advanceFloor] does exactly that, which is what makes halving the
     * batch the right response here and would make it the wrong response
     * anywhere else.
     */
    private fun isStatementBound(e: Throwable): Boolean =
        e is UnableToExecuteStatementException && Pg.isQueryCanceled(e)

    /**
     * The phase-A failure, carrying what phase B managed anyway.
     *
     * `MaintenanceRunStore.PartialResult` is the repo's mechanism for
     * exactly this — "a ledger row saying it did nothing is a lie an
     * operator acts on" — and the run still records as `failed` with the
     * original error text. A `HoglakeException` is rethrown UNWRAPPED:
     * its subclass is what the API's error mapping reads, and turning a
     * 422 into a 500 to attach a counter would be a bad trade. Every
     * other throwable already maps to a 500, so wrapping one changes
     * nothing a client can see.
     */
    private fun advanceFailure(
        e: Throwable,
        partial: ExpiryResult,
    ): Throwable = if (e is HoglakeException) e else AdvanceFailed(e, partial)

    private class AdvanceFailed(
        cause: Throwable,
        override val partial: Any?,
    ) : RuntimeException(cause.message, cause), PartialResult

    /**
     * PHASE A: advance the floor. Runs on [h], inside the one
     * transaction that holds the per-catalog commit lock, under
     * [advanceBoundMs].
     *
     * # Why the file rows may be deleted after this commits
     *
     * Four properties, each verified rather than assumed:
     *
     * **1. A row below the floor is UNREADABLE.** Every read resolves
     * its snapshot through `CatalogService.resolveReadSnapshot`, which
     * refuses anything below `hog_catalog.earliest_snapshot_id` with
     * 410 `SnapshotExpired` (`TimeTravelRepo.expiryFloor` is the read of
     * that column, `CatalogService.kt` the refusal). A file row with
     * `end_snapshot <= floor` is visible only at snapshots `S <
     * end_snapshot <= floor` — all of them refused. So after THIS
     * transaction commits, nothing can plan a scan that includes it.
     *
     * **2. A row's ELIGIBILITY is immutable, and the statement checks
     * anyway.** The claim is about `end_snapshot`, not about the whole
     * row. Two kinds of writer exist and neither can move a row across
     * phase B's predicate:
     *
     *  - those that carry `end_snapshot IS NULL` in their own WHERE —
     *    `FileRepo.endLiveFiles` / `endLiveDeleteFiles` and
     *    `AlterService`'s four versioning UPDATEs — which by
     *    construction cannot touch an ended row;
     *  - those that carry NO such clause and are instead fenced by
     *    WHICH IDS THEY TAKE: compaction's two retirement UPDATEs
     *    (`CompactionService.kt`, the data-file and delete-file arms of
     *    `commitGroup`) take only ids its commit-time liveness re-check
     *    just confirmed live, and `CommitService`'s DV supersession
     *    (`CommitService.kt`) keys on a `delete_file_id` it re-read as
     *    live under the commit lock. An earlier draft of this KDoc
     *    listed those three among the `end_snapshot IS NULL` writers;
     *    they are not, and the distinction matters because their
     *    protection is a re-read rather than a predicate.
     *
     * The value a commit tail writes into `end_snapshot` is the NEW
     * HEAD, strictly above the floor by definition of head. So nothing
     * can move a row INTO or OUT OF phase B's predicate while phase B
     * walks, and phase A and phase B never contend on a row.
     *
     * AND THE PAGE DOES NOT RELY ON THAT ARGUMENT. The data-file DELETE
     * repeats `end_snapshot IS NOT NULL AND end_snapshot <= :newEarliest`
     * beside its ctid array, because the array alone is not a qual a
     * concurrent UPDATE is re-checked against (see
     * [DATA_FILE_EXPIRY_SQL] for the EvalPlanQual mechanics), so a future
     * writer that broke the convention above would cost a SKIPPED ROW
     * rather than a live file deleted. The vector DELETE repeats
     * `catalog_id` rather than an eligibility predicate — it has none of
     * its own, its eligibility IS "the data file I ride is going", which
     * it reads from what that delete actually removed. Keyed off the
     * page instead, the two statements could disagree about which files
     * are going, and a skipped file would lose its vectors.
     *
     * NOTHING WRITES TO SUCH A ROW. The hydrator used to: its claim
     * carried no `end_snapshot` filter, so it could claim a below-floor
     * ended row, read its footer, and UPDATE its stats columns — wasted
     * object-store I/O and a wasted WAL record for a row about to go,
     * not a correctness problem, since it never touched `end_snapshot`.
     * Since #269 `CLAIM_PENDING_SQL` claims LIVE pending rows only, and
     * `hog_data_file_pending` is partial on the same term (V26), so an
     * ended row is neither claimed nor walked; `hoglake_stats_pending_files`
     * counts live pending rows too, and agrees with the maintenance
     * sampler's pending count up to a generation's staleness.
     *
     * The one consequence of a row being writable is for the `ctid`
     * page, and it is smaller than an earlier draft of this comment
     * claimed. A concurrently-UPDATEd row MOVES, and under READ
     * COMMITTED the delete follows `t_ctid` into EvalPlanQual and
     * applies the filter quals to the new version — so the row is still
     * deleted, and the page is not short. What a concurrent DELETE of
     * one of the page's rows does produce is a page that PURGED fewer
     * rows than it EXAMINED, which is why [walk] tests the examined
     * count: the walk's termination does not depend on who else deleted
     * what. The superseded-vector arm is not exposed to the hydrator at
     * all (it writes only `hog_data_file`), though `RetirementService`
     * and a second replica's own sweep can delete the same vector rows —
     * which is a reason to test examined rather than purged there too,
     * and was the unsound half of the gate this design replaced.
     *
     * **3. Nothing references a file row by snapshot id.** The only FK
     * onto `hog_snapshot` is `hog_snapshot_change (catalog_id,
     * snapshot_id) ... ON DELETE CASCADE` (`V1__init.sql`). Data-file
     * rows carry `begin_snapshot`/`end_snapshot` as PLAIN COLUMNS, so
     * deleting the snapshot rows in this phase cannot cascade into, or
     * strand, the file rows the next phase deletes.
     *
     * **4. No object is orphaned by a crash between the phases.** Each
     * phase-B page deletes its rows and inserts their paths into
     * `hog_file_removal` in ONE statement (the `RETURNING`-fed CTE), so
     * a path is queued iff its row is gone. A pod that dies between the
     * phases — or a page that fails, or a budget that expires — leaves
     * ELIGIBLE ROWS, which is a state the next sweep's phase B drains
     * (it is not gated on the advance, see [runSweep]), and it leaves no
     * state anything else has to reconcile.
     *
     * # What changes for an observer
     *
     * Ended rows below the floor can now exist for a few sweeps rather
     * than for zero of them. `VerifyService.expiryFloor` was the check
     * that encoded the old timing, and #262 taught its `hog_data_file` /
     * `hog_delete_file` arm to assert only against a sweep whose ledger
     * row reported the purge DRAINED at a floor covering the current
     * one, while the five DDL tables kept the no-lag property because
     * step 5 is still phase A's. #261 then removed the whole check, so
     * NOTHING asserts the invariant half today — a drained purge that
     * leaves rows below the floor is unwatched until the paged scrubber
     * (#261). The backlog half is unaffected and never went through
     * verify: `purge_truncated` / `purge_remaining` / `purge_failures` on
     * the ledger row, and `hoglake_expiry_purge_truncated_total` /
     * `hoglake_expiry_purge_remaining`. The rest of that reasoning stands
     * for whatever replaces the check: `visibility_bounds`
     * uses the same table list and is unaffected — its bounds are
     * HEAD-relative, and a purge-pending row satisfies them exactly as a
     * purged one would; `orphans`, `snapshot_density` and `next_row_id`
     * are unaffected too (dropped-table reachability needs a LIVE row,
     * the snapshot range moves with the floor in phase A, and the
     * allocator is monotone over rows it has already handed out).
     *
     * ONE NEW FALSE-ALARM WINDOW, stated rather than discovered later:
     * `removal_queue` and the cleanup drain's `still_referenced` both
     * ask whether an undrained queue row's path is still claimed by a
     * file row, LIVE OR HISTORICAL — deliberately, because a historical
     * row still claims its object. Two file rows CAN share one path
     * (`V16FileRemovalPathIndexMigrationIntegrationTest` pins it as
     * legitimate state; a replay of a purged receipt is how it arises),
     * and the one statement this used to be deleted both of them
     * together. Paged, the two can land in different pages, and between
     * them the first path is queued while the second row still claims
     * it. The window is one page — unless the budget stops inside it,
     * when it is one sweep — and the consequence is a logged
     * `cleanup_violation` on a path that is in fact still referenced,
     * which is what the detector is for. Closing it would mean taking
     * the page's whole path-closure, which is a per-row subquery against
     * a table with no usable index for it; the window is cheaper.
     *
     * # What is still O(work) under the lock, and why it stays
     *
     * The snapshot range delete cascades `hog_snapshot_change`, and the
     * arithmetic is O(snapshots x CHANGED OBJECTS per snapshot) — NOT
     * O(files). One row per changed object per snapshot: `kind` is a
     * vocabulary (`table_inserted_into`, `table_altered`, ...) with
     * `object_id` the table id (`V1__init.sql`), and every writer inserts
     * ONE row per append (`CommitService.writeAppends`,
     * `SnapshotRepo.recordChange`), so a 270-file commit into one table
     * is ONE change row. A batch of 10,000 snapshots is therefore
     * ~10-30k cascade rows — ~0.2-0.6 s at this repo's measured ~20
     * us/row for the same cascade shape — comfortably inside
     * [advanceBoundMs]. An earlier draft of this comment read "~270
     * change rows per snapshot" and concluded a 2.7M-row cascade, which
     * was wrong by two orders of magnitude and is corrected here because
     * it is the figure a future reader inherits.
     *
     * So the halve-and-retry in [runSweep] is belt-and-braces rather
     * than a repair: it is there for a catalog whose shape differs from
     * prod-us's (many tables altered per snapshot, a replay namespace
     * fanning out), and the REMEDY WHEN IT FIRES IS THE HALVING ITSELF —
     * the run converges without an operator, and
     * `hoglake_expiry_halvings_total{phase="advance"}` is what says the
     * configured `HOGLAKE_EXPIRY_BATCH` is too large for that catalog.
     * Paging the snapshot delete is a separate change and a separate
     * argument, because unlike a file row a snapshot row below the floor
     * is what the floor's own 410 reconciliation reads.
     */
    private fun sweep(
        h: Handle,
        cat: CatalogInfo,
        batchSize: Int,
    ): Advance {
        // FIRST, and deliberately ABOVE the retention check below.
        //
        // An offset left on an incarnation that atomic replacement
        // retired can never be advanced by anyone (see
        // OffsetRepo.releaseSupersededOffsets), so it is dead state
        // whatever the catalog's retention setting is: GET /consumers
        // shows a position the consumer will never read again, and
        // the removed /maintenance/verify's offset_release check used to
        // flag it. It used to sit after the retention early-return,
        // which meant a
        // retention-NULL catalog — expiry configured off, which several
        // production catalogs are — could never release anything: the
        // rows were stranded with no code path left that would ever
        // clear them, and the check would have fired forever with no
        // remedy an operator could apply. The commit path's cheap
        // backward walk still handles the common case; this forward form
        // is the catch-all for rows stranded before that rule existed.
        //
        // The per-catalog commit lock is already held (runSweep takes it
        // before calling here), so this runs under the same
        // serialization as every other write in the sweep.
        //
        // Idempotent, but not free: it joins every offset in the catalog
        // to hog_table. That is affordable because a sweep is once per
        // interval and a commit is not, which is exactly why the commit
        // path uses the cheap backward form instead.
        // It is NOT gated on consumerFloor either: these rows are dead
        // whether or not they would floor anything, and GET /consumers
        // must not keep showing a position the consumer will never read.
        val offsetsReleased = OffsetRepo.releaseSupersededOffsets(h, cat.catalogId).toLong()

        val retention =
            cat.snapshotRetentionSeconds
                ?: return Advance(cat.catalogId, 0, cat.earliestSnapshotId, null, offsetsReleased)

        // Oldest snapshot still inside the retention window; everything
        // below it is expirable time-wise.
        val firstFresh: Long? =
            h.createQuery(
                """
            SELECT min(snapshot_id) FROM hog_snapshot
            WHERE catalog_id = :catalogId
              AND snapshot_id >= :earliest
              AND snapshot_time >= now() - make_interval(secs => :retention)
            """,
            )
                .bind("catalogId", cat.catalogId)
                .bind("earliest", cat.earliestSnapshotId)
                .bind("retention", retention)
                .mapTo(Long::class.javaObjectType)
                .one()

        // [FLOOR_CANDIDATE_OFFSETS] scopes the floor to offsets whose
        // table identity still exists (any incarnation, dropped included
        // — offsets survive drops by design). An offset whose table row
        // is gone entirely (expired away) must not pin retention
        // forever. The removed verify subsystem's expiry_floor check asked the SAME
        // fragment rather than a copy of it.
        val minOffset: Pair<String, Long>? =
            if (cat.consumerFloor) {
                h.createQuery(
                    """
                SELECT o.consumer_id, o.committed_snapshot
                $FLOOR_CANDIDATE_OFFSETS
                ORDER BY o.committed_snapshot, o.consumer_id
                LIMIT 1
                """,
                )
                    .bind("catalogId", cat.catalogId)
                    .map { rs, _ -> rs.getString("consumer_id") to rs.getLong("committed_snapshot") }
                    .findOne()
                    .orElse(null)
            } else {
                null
            }

        val unfloored =
            minOf(
                firstFresh ?: Long.MAX_VALUE,
                cat.headSnapshotId,
                cat.earliestSnapshotId + batchSize,
            )
        val newEarliest = minOf(unfloored, minOffset?.second ?: Long.MAX_VALUE)
        val flooredBy =
            minOffset
                ?.takeIf { it.second < unfloored && unfloored > cat.earliestSnapshotId }
                ?.first
        // NOTE: no logging in here — this runs inside the sweep transaction
        // under the catalog commit lock; runOnce warns AFTER commit.
        if (newEarliest <= cat.earliestSnapshotId) {
            return Advance(cat.catalogId, 0, cat.earliestSnapshotId, flooredBy, offsetsReleased)
        }

        // STEPS 1 AND 2 — the delete-vector and data-file arms — USED TO
        // BE HERE, each one unbounded DELETE under this lock. They are
        // phase B now; see [purge] and this function's KDoc for why
        // their rows may go after this transaction commits.

        // 3) The snapshots themselves (cascades hog_snapshot_change).
        val snapshotsExpired =
            h.createUpdate(
                """
            DELETE FROM hog_snapshot
            WHERE catalog_id = :catalogId
              AND snapshot_id >= :earliest AND snapshot_id < :newEarliest
            """,
            )
                .bind("catalogId", cat.catalogId)
                .bind("earliest", cat.earliestSnapshotId)
                .bind("newEarliest", newEarliest)
                .execute()

        // 4) Advance the floor, capturing the new floor snapshot's time in
        // the SAME update: that snapshot survives this sweep (only ids
        // BELOW newEarliest were deleted) but a later sweep will kill it,
        // and 410 reconciliation needs "the floor was reached at T" after
        // the times below it are gone.
        h.createUpdate(
            """
            UPDATE hog_catalog
               SET earliest_snapshot_id = :newEarliest,
                   earliest_snapshot_time =
                       (SELECT snapshot_time FROM hog_snapshot
                         WHERE catalog_id = :catalogId AND snapshot_id = :newEarliest)
             WHERE catalog_id = :catalogId
            """,
        )
            .bind("newEarliest", newEarliest)
            .bind("catalogId", cat.catalogId)
            .execute()

        // 5) Versioned-row retention for the accumulating DDL tables.
        // Correctness: a versioned row with end_snapshot = E is visible at
        // S iff S < E (visibility rule: begin <= S AND (end IS NULL OR
        // S < end)); every retained snapshot satisfies S >= newEarliest;
        // so E <= newEarliest means NO retained snapshot can see the row —
        // it is unreachable by any valid read (head reads see only
        // end IS NULL rows) and can be deleted outright. Live rows
        // (end IS NULL) and rows ending above the floor are untouched, so
        // time travel at every S >= newEarliest is unchanged. Child tables
        // (hog_partition_field, hog_sort_field) follow their spec headers
        // via FK ON DELETE CASCADE. Incremental like the steps above: the
        // range is bounded by newEarliest, which batchSize caps per sweep.
        for (table in VERSIONED_RETENTION_TABLES) {
            h.createUpdate(
                """
                DELETE FROM $table
                WHERE catalog_id = :catalogId
                  AND end_snapshot IS NOT NULL AND end_snapshot <= :newEarliest
                """,
            )
                .bind("catalogId", cat.catalogId)
                .bind("newEarliest", newEarliest)
                .execute()
        }

        return Advance(
            catalogId = cat.catalogId,
            snapshotsExpired = snapshotsExpired.toLong(),
            newEarliest = newEarliest,
            flooredBy = flooredBy,
            offsetsReleased = offsetsReleased,
        )
    }

    /**
     * PHASE B: purge the file rows below the committed floor, outside
     * every lock, in pages.
     *
     * NEVER THROWS. A failed page is a counted failure and a WARN, and
     * the sweep still reports the floor it advanced: the advance is
     * durable and already committed, and a phase-B failure that turned
     * the run into a `failed` ledger row with no result would hide it.
     * This is `CleanupService.purgeCommitReceipts`' fence for the same
     * reason — "retention is hygiene, and hygiene is not allowed to
     * misreport a sweep".
     *
     * IT RUNS EVEN WHEN PHASE A THREW, which is a fix rather than a
     * flourish. [advanceBoundMs] exists to turn a catch-up sweep into a
     * fast retryable failure, so phase A now fails exactly when the
     * backlog is largest — and a purge skipped on that path would
     * starve forever while the ledger row said only `failed` and, at the
     * time, `/verify`'s file arm stayed dark. All phase B needs is the
     * catalog id, which is read before the lock is taken.
     *
     * THE FLOOR IS READ FRESH, not inherited from phase A. The floor is
     * MONOTONE (nothing in the server lowers `earliest_snapshot_id`), so
     * a value read now is at least phase A's, and on a fleet where a
     * second replica's sweep ran in between it is higher — which means
     * more rows are legitimately eligible, not fewer. It is also the
     * only correct value to read when phase A ROLLED BACK: the floor is
     * then whatever it was before, and the rows below it are still the
     * rows below it.
     *
     * A FLOOR OF 0 SKIPS THE WHOLE PHASE, and the proof is half schema
     * and half allocator — stated that way because only one half is
     * enforced. `hog_data_file` and `hog_delete_file` each carry
     * `CHECK (end_snapshot IS NULL OR end_snapshot > begin_snapshot)`
     * (`V1__init.sql`), which is the schema half; `begin_snapshot >= 0`
     * is NOT a constraint, it is a property of the snapshot allocator
     * (ids start at 0 and are dense per catalog, invariant 1). Together
     * they make every ended row's `end_snapshot >= 1`, so nothing can
     * satisfy `end_snapshot <= 0`. That is what keeps
     * this phase off the catalogs that have never expired anything —
     * several production catalogs run with retention DISABLED, and
     * before this gate they would each have paid the vector arm's
     * unindexed scan every interval for a population that cannot exist.
     * It is a gate on the FLOOR rather than on `retention IS NOT NULL`
     * on purpose: a catalog whose retention is switched off after it has
     * expired keeps its backlog, and that backlog still has to drain.
     *
     * ORDER: the superseded-vector arm, then the data-file pages. The
     * ordering that MATTERS — a data file's own vectors going with it,
     * queued — is inside the data-file page's own statement now (see
     * [DATA_FILE_EXPIRY_SQL]); nothing about this arm order is load
     * bearing, and either arm stopping early simply leaves work for the
     * next sweep.
     *
     * PER-ROW COST, which is what sizes the page (AGENT.md: measure per
     * row, not per statement). The dominant term is the cascade, not the
     * heap delete: `hog_file_column_stats` is ~26 rows per file on
     * production's 25-column tables and lives in a 66 GiB relation, and
     * `hog_file_partition_value` adds its own under V23's index. The
     * 2026-09-28 figure for this statement was 700-870 us per file
     * against retirement's ~20 us/row for the same CTE shape, measured
     * on the same database — the difference being random heap reads in
     * compaction-commit order versus a PK-ordered walk.
     * `ExpiryPurgeCostMeasurement` has the fixture figure over 100,000
     * rows on PG 18, with ALL THREE cascades a production row carries
     * (26 stats rows, one partition value, the vector probe): see that
     * class for the published numbers. The ABSOLUTE figures are a floor
     * and nothing else — the fixture's stats relation is a few hundred
     * megabytes and fits the container's cache, production's is 66 GiB
     * and does not, which is the whole distance between tens of
     * microseconds and 870. The RATIO between the regimes is the
     * transferable part: it says how the page should scale with a
     * table's column count.
     *
     * At 1,000 rows a page and the measured PRODUCTION cost that is a
     * page of ~0.7-0.9 s, two orders inside [PURGE_STATEMENT_TIMEOUT].
     * What [PURGE_BUDGET_MS] then buys per sweep, and how that compares
     * with the arrival rate, is arithmetic over the LOOP's real period
     * rather than its interval — see server/README.md §Retention, which
     * does it properly.
     *
     * A KNOWN FALSE-ALARM WINDOW, stated rather than left to be
     * rediscovered. The cleanup drain's `still_referenced` asks whether
     * an undrained queue row's path is still claimed by a file row, LIVE
     * OR HISTORICAL — deliberately, because a historical row still
     * claims its object — and AGENT.md's invariant 4 calls
     * `still_referenced > 0` an ALERTED violation. (`/verify`'s
     * `removal_queue` asked the same question until #261; the test-side
     * `CatalogInvariants.assertRemovalQueueUnreferenced` still does.)
     * Two file rows can legitimately share one path
     * (`V16FileRemovalPathIndexMigrationIntegrationTest` pins it; a
     * replay of a purged receipt is how it arises). Where one of them is
     * LIVE, purging the other queues a path the live row still claims
     * and the counter fires until the live row dies — that is
     * PRE-EXISTING, not new here: the single-statement sweep did exactly
     * the same. What IS new is that two rows both eligible can now land
     * in different pages, so the counter can also fire for one page's
     * duration (one sweep, if the budget stops between them). Closing
     * either case means taking the page's whole path-closure — a
     * correlated probe per page row against `hog_data_file_path` plus a
     * rule for what to do with the rows it finds — and it is a change to
     * what expiry queues rather than to how it pages, so it is a
     * separate ticket. The counter firing from this shape is a FALSE
     * alarm: nothing is deleted, because the drain's whole purpose is to
     * refuse.
     */
    private fun purge(
        catalog: String,
        catalogId: Long,
        knownFloor: Long,
    ): Purge {
        // A budget of 0 needs no special case: the deadline is already
        // past, so the first arm stops on it before its first page and
        // the sweep reports `purge_truncated` with the rows it left —
        // which is exactly what a test asking for 0 wants to observe.
        val deadline = System.nanoTime() + Duration.ofMillis(purgeBudgetMs).toNanos()
        val floor =
            try {
                jdbi.withHandleUnchecked { h ->
                    h.createQuery("SELECT earliest_snapshot_id FROM hog_catalog WHERE catalog_id = ?")
                        .bind(0, catalogId)
                        .mapTo(Long::class.javaObjectType)
                        .findOne()
                        .orElse(null)
                }
            } catch (e: Exception) {
                // The catalog is there and this is a primary-key read, so
                // a failure here is the database or the pool rather than
                // this sweep. Counted and truncated, NOT silently zero:
                // a catalog whose floor cannot be read is the one state
                // that would otherwise report as a healthy idle sweep.
                log.warn(e) { "expiry: could not read the purge floor for catalog '$catalog'" }
                // [knownFloor] — the value the pre-lock read saw — for
                // REPORTING only, never for purging. Without it a sweep
                // whose phase A also failed reported
                // `new_earliest_snapshot_id = 0`, which is the "zero that
                // reads like a catalog that has never expired" that
                // [Purge.floor] exists to prevent. Purging against it
                // would in fact be SOUND (the floor is monotone, so a
                // stale value is at or below the committed one, which
                // makes the predicate strictly conservative) — but this
                // path has already failed to read the database once, and
                // one counted failure is a better answer than a page.
                return Purge(failures = 1, truncated = true, floor = knownFloor)
            }
                ?: return Purge() // dropped between the phases; nothing to purge
        // See the KDoc: no ended row can sit at or below a floor of 0.
        if (floor <= 0) return Purge(floor = floor)

        // The superseded-vector arm, probed first so a catalog with no
        // such rows — every append-only writer's catalog, which is all
        // of them today — pays one EXISTS and runs no pages.
        //
        // THE PROBE IS A STATEMENT LIKE ANY OTHER, which an earlier
        // version forgot: it ran before any deadline check, on a handle
        // with no transaction, under the session's 60 s bound. On a
        // catalog with a large unindexed `hog_delete_file` that is one
        // scan of the relation, taken after the budget may already be
        // spent, and taken for up to a minute — six times the budget it
        // was supposed to respect. It is now ordered and bounded like a
        // page: the deadline first, then the scan under
        // [PURGE_STATEMENT_TIMEOUT].
        val vectors =
            when (probeSupersededVectors(catalog, catalogId, floor, deadline)) {
                Probe.SOME ->
                    walk(
                        catalog,
                        "superseded delete-file",
                        SUPERSEDED_DELETE_FILE_PURGE_SQL,
                        catalogId,
                        floor,
                        deadline,
                    )
                Probe.NONE -> Walk(drained = true)
                // A probe that could not answer is a COUNTED failure and
                // an undrained arm — never a silent "nothing here" — and
                // the data-file arm still runs on the remaining budget,
                // because the two arms are independent and starving the
                // indexed one for the unindexed one's sake would be the
                // cross-arm coupling this design removed.
                Probe.FAILED -> Walk(failures = 1, drained = false, stoppedOn = "a failed probe")
                Probe.OUT_OF_TIME -> Walk(drained = false, stoppedOn = "the ${purgeBudgetMs}ms run budget")
            }
        val data = walk(catalog, "data-file", DATA_FILE_EXPIRY_SQL, catalogId, floor, deadline)
        val truncated = !(vectors.drained && data.drained)
        val remaining: Long? = if (truncated) remaining(catalog, catalogId, floor) else 0L
        val failures = vectors.failures + data.failures
        val vectorRows = data.vectorRows + vectors.vectorRows
        if (data.dataRows > 0 || vectorRows > 0 || truncated || failures > 0) {
            log.debug {
                "expiry purge for catalog '$catalog' below floor $floor: " +
                    "${data.dataRows} data-file rows and ${data.vectorRows} of their vectors over " +
                    "${data.pages} pages, ${vectors.vectorRows} superseded vectors over " +
                    "${vectors.pages} pages, $failures failed pages, " +
                    "${vectors.halvings + data.halvings} page halvings, stopped on " +
                    "${if (data.drained) vectors.stoppedOn else data.stoppedOn}" +
                    (
                        if (truncated) {
                            " (${remaining?.toString() ?: "an unknown number of"} file rows still eligible)"
                        } else {
                            ""
                        }
                    )
            }
        }
        return Purge(
            dataRows = data.dataRows,
            deleteRows = vectorRows,
            pages = data.pages + vectors.pages,
            failures = failures,
            halvings = data.halvings + vectors.halvings,
            truncated = truncated,
            remaining = remaining,
            floor = floor,
        )
    }

    /** What [probeSupersededVectors] learned. */
    private enum class Probe { SOME, NONE, FAILED, OUT_OF_TIME }

    /**
     * [SUPERSEDED_DELETE_FILE_PROBE_SQL], deadline-checked and bounded.
     *
     * IN A TRANSACTION FOR `SET LOCAL`'S SAKE, as the pages are: the
     * probe's predicate has no index (see
     * [SUPERSEDED_DELETE_FILE_PURGE_SQL]), so on a catalog with a large
     * vector relation it is a scan, and a scan on a pooled connection
     * with no transaction-local bound inherits the session's 60 s. Five
     * seconds is the same bound a page gets, for the same reason: a
     * probe that cannot answer inside it is telling you something, and
     * a minute is not a better way to hear it.
     *
     * [Probe.FAILED] rather than a defaulted answer. "The probe threw"
     * is not evidence that there is nothing to do, and it is not
     * evidence that there is, either — so the caller counts it, leaves
     * the arm undrained, and gets on with the arm that has an index.
     */
    private fun probeSupersededVectors(
        catalog: String,
        catalogId: Long,
        floor: Long,
        deadline: Long,
    ): Probe {
        if (System.nanoTime() >= deadline) return Probe.OUT_OF_TIME
        return try {
            val some =
                jdbi.inTransactionUnchecked { h ->
                    h.execute("SET LOCAL statement_timeout = '$PURGE_STATEMENT_TIMEOUT'")
                    h.createQuery(SUPERSEDED_DELETE_FILE_PROBE_SQL)
                        .bind("catalogId", catalogId)
                        .bind("newEarliest", floor)
                        .mapTo(Boolean::class.javaObjectType)
                        .one()
                }
            if (some) Probe.SOME else Probe.NONE
        } catch (e: Exception) {
            log.warn(e) {
                "expiry: the superseded-vector probe for catalog '$catalog' failed inside its " +
                    "$PURGE_STATEMENT_TIMEOUT bound (floor $floor); counted, and the data-file arm " +
                    "still runs on the remaining budget. A probe that cannot finish means the " +
                    "catalog's hog_delete_file wants the partial (catalog_id, end_snapshot) index"
            }
            Probe.FAILED
        }
    }

    /**
     * One arm's paged walk: [sql] bound to `:catalogId`, `:newEarliest`
     * and `:page`, each page its OWN transaction under
     * [PURGE_STATEMENT_TIMEOUT], stopping on a short page (nothing
     * eligible), on [deadline], or on a page that threw its way down to
     * a page size of one.
     *
     * A PAGE IS ITS OWN TRANSACTION, which is the half that makes a
     * failure survivable: pages already committed stay committed, so a
     * timeout on page 11 does not un-delete pages 1-10 and does not
     * un-queue their paths. The next sweep resumes from the oldest
     * eligible row because the predicate, not a cursor, is what selects
     * the page.
     *
     * A FAILED PAGE IS HALVED AND RETRIED, not abandoned — the shape
     * `RetirementService` uses for the same reason. The page order is
     * stable (a dense index prefix for the data arm, relation order for
     * the vector arm), so a page that cannot finish inside its statement
     * bound is selected FIRST on every subsequent sweep: without
     * halving, one poison page means `purge_failures` increments once a
     * sweep forever and nothing ever drains, with the ledger reporting a
     * failure an operator cannot act on except by guessing at
     * HOGLAKE_EXPIRY_PURGE_PAGE. Halving converges in at most
     * log2(page) steps and the halvings are counted
     * (`hoglake_expiry_halvings_total{phase="purge"}`), so "the
     * configured page is too big for this table's cascade" becomes a
     * number rather than an inference. A page of ONE that still fails is
     * not a page-size problem and the walk gives up.
     *
     * AND THE LADDER SURVIVES THE SWEEP IT STARTED IN. A rung that dies
     * on the statement bound costs five seconds against a ten-second
     * budget, so one sweep affords about two of them; [settledPage]
     * carries the rung the walk reached, which is what makes the descent
     * converge across sweeps instead of restarting from the configured
     * page every time. [PAGE_REGROW_AFTER] clean pages climb back — and
     * the STREAK is carried with the size, because a sweep runs ten to
     * fourteen pages and the threshold is twenty, so a per-walk streak
     * would never reach it on any catalog the budget actually bounds.
     *
     * EVERY FAILED PAGE IS COUNTED, including the rungs. An earlier
     * version counted only the final give-up, so a sweep that spent its
     * whole budget timing out reported `purge_failures = 0` with nothing
     * but `purge_truncated` and a halvings counter to show it — the
     * ambiguity this field exists to remove, reintroduced one level
     * down.
     *
     * A FAILED PAGE IS COUNTED AND NEVER READS AS "NOTHING ELIGIBLE".
     * Without [Walk.failures] a page that times out is indistinguishable
     * on every surface from an idle sweep — both report zero rows — and
     * the failure mode is a purge that attempts the same first page
     * forever while the floor keeps advancing and the console keeps
     * reading healthy. That is the receipt purge's lesson
     * (`Metrics.commitReceiptPurgeFailures`), and [Walk.drained] is
     * `false` after a failure, so the arm is also not credited with
     * having finished.
     *
     * AN EMPTY PAGE IS NOT COUNTED. `purge_pages` is a measure of work
     * done, and an idle sweep that probes once and finds nothing has
     * done none: counting it made every expiry ledger row report
     * `purge_pages = 2`, which defeated the console's hide-quiet filter
     * for every sweep on the fleet.
     */
    private fun walk(
        catalog: String,
        arm: String,
        sql: String,
        catalogId: Long,
        floor: Long,
        deadline: Long,
    ): Walk {
        var dataRows = 0L
        var vectorRows = 0L
        var pages = 0L
        var halvings = 0L
        var failures = 0L
        // THE LADDER STARTS WHERE THE LAST ONE LEFT OFF, which is the
        // difference between converging and not. See [settledPage].
        val key = "$catalogId/$arm"
        val hint = settledPage[key]
        var page = (hint?.page ?: purgePage).coerceIn(1, purgePage)
        var clean = if (hint?.page == page) hint.clean else 0

        fun settle(current: Int): Int {
            settledPage[key] = PageHint(current, clean)
            return current
        }
        while (true) {
            if (System.nanoTime() >= deadline) {
                settle(page)
                return Walk(
                    dataRows = dataRows,
                    vectorRows = vectorRows,
                    pages = pages,
                    failures = failures,
                    halvings = halvings,
                    drained = false,
                    stoppedOn = "the ${purgeBudgetMs}ms run budget",
                )
            }
            val outcome =
                try {
                    jdbi.inTransactionUnchecked { h ->
                        h.execute("SET LOCAL statement_timeout = '$PURGE_STATEMENT_TIMEOUT'")
                        h.createQuery(sql)
                            .bind("catalogId", catalogId)
                            .bind("newEarliest", floor)
                            .bind("page", page)
                            .map { rs, _ ->
                                PageOutcome(
                                    examined = rs.getLong("examined"),
                                    dataRows = rs.getLong("data_purged"),
                                    vectorRows = rs.getLong("delete_purged"),
                                )
                            }
                            .one()
                    }
                } catch (e: Exception) {
                    // One page, not the walk and not the sweep. WARN
                    // because the counter says HOW MANY and only the log
                    // says WHY: a statement timeout here means the page
                    // is too big for the rows it is meeting (a wide
                    // table's stats cascade), which is a knob rather
                    // than a bug.
                    val halved = maxOf(1, page / 2)
                    failures++
                    // A failure ends the streak: the evidence that the
                    // reduced size is comfortable has to be re-earned at
                    // whatever size the ladder lands on.
                    clean = 0
                    log.warn(e) {
                        "expiry: a $arm purge page of $page failed for catalog '$catalog' (floor $floor, " +
                            "bound $PURGE_STATEMENT_TIMEOUT); the ${dataRows + vectorRows} rows already " +
                            "purged this run are committed" +
                            if (page > 1) {
                                ", halving to $halved and retrying within this run (and starting there " +
                                    "next sweep)"
                            } else {
                                " and a page of one cannot be made smaller, so the walk stops"
                            }
                    }
                    if (page <= 1) {
                        settle(1)
                        return Walk(
                            dataRows = dataRows,
                            vectorRows = vectorRows,
                            pages = pages,
                            failures = failures,
                            halvings = halvings,
                            drained = false,
                            stoppedOn = "a failed page of one",
                        )
                    }
                    halvings++
                    page = settle(halved)
                    continue
                }
            // An empty page is not work; see the KDoc.
            if (outcome.examined > 0) pages++
            dataRows += outcome.dataRows
            vectorRows += outcome.vectorRows
            // EXAMINED, not purged: a concurrent deleter of one of this
            // page's rows lowers the purged count without exhausting the
            // eligible set, and ending the walk on that would report a
            // set that is not drained as drained.
            if (outcome.examined < page) {
                settle(page)
                return Walk(
                    dataRows = dataRows,
                    vectorRows = vectorRows,
                    pages = pages,
                    failures = failures,
                    halvings = halvings,
                    drained = true,
                    stoppedOn = "nothing eligible",
                )
            }
            // RE-GROW, so a hint learned during one bad afternoon is not
            // permanent. A table that has absorbed [PAGE_REGROW_AFTER]
            // full pages at a reduced size is a table whose cost has come
            // back down (the stats relation warmed, the wide table was
            // dropped, autovacuum caught up), and doubling costs at most
            // one more failed page to discover otherwise.
            clean++
            if (page < purgePage && clean >= PAGE_REGROW_AFTER) {
                clean = 0
                page = settle(minOf(purgePage, page * 2))
            } else {
                // Carried, so the streak survives a sweep that ends
                // before it is long enough to re-grow on.
                settle(page)
            }
        }
    }

    /**
     * One page's three numbers, the SAME THREE from both arms — the
     * vector arm reports `data_purged` as a literal 0 rather than
     * omitting the column, so [walk] needs no idea which statement it is
     * running. A mapper that branched on the result shape would be a
     * mapper that silently read 0 the day a column was renamed.
     */
    private data class PageOutcome(val examined: Long, val dataRows: Long, val vectorRows: Long)

    /**
     * [PURGE_REMAINING_SQL], best-effort: `null` is UNKNOWN, and
     * unknown is reported rather than guessed.
     *
     * NOT 0 ON FAILURE, which an earlier version returned. Zero means
     * "the purge drained", and a sweep that reports `purge_truncated`
     * beside a remaining of zero is telling an operator the one thing
     * that cannot be true. Null travels all the way out: the field is
     * absent from the ledger row and from the API response, the gauge
     * publishes NaN rather than a fabricated zero, and the console's
     * `positive()` guard reads an absent field as "nothing to show".
     *
     * BOUNDED, because `LIMIT :cap` bounds the rows MATCHED and not the
     * rows EXAMINED. The data-file half is an index-only prefix walk, so
     * for it the two are the same; the vector half has no index yet
     * (see [SUPERSEDED_DELETE_FILE_PURGE_SQL]) and is a scan whose cost
     * is the catalog's whole vector relation when few rows match. On a
     * pooled connection with no transaction-local bound that scan
     * inherits the session's 60 s — six times the purge's entire budget,
     * paid AFTER the budget is spent, and paid per catalog, so one
     * catalog's unindexed count would delay every catalog behind it in
     * the serial fleet sweep. It runs in a transaction for `SET LOCAL`'s
     * sake and gives up at [PURGE_STATEMENT_TIMEOUT] with an unknown.
     *
     * The transaction is also the answer to the aborted-connection
     * question a review raised: it is opened fresh, it carries its own
     * bound, and a failure here is swallowed into `null` rather than
     * surfacing as the sweep's exception.
     */
    private fun remaining(
        catalog: String,
        catalogId: Long,
        floor: Long,
    ): Long? =
        try {
            jdbi.inTransactionUnchecked { h ->
                h.execute("SET LOCAL statement_timeout = '$PURGE_STATEMENT_TIMEOUT'")
                h.createQuery(PURGE_REMAINING_SQL)
                    .bind("catalogId", catalogId)
                    .bind("newEarliest", floor)
                    .bind("cap", PURGE_REMAINING_CAP)
                    .mapTo(Long::class.javaObjectType)
                    .one()
            }
        } catch (e: Exception) {
            log.warn(e) {
                "expiry: could not count the rows left eligible for catalog '$catalog' inside " +
                    "$PURGE_STATEMENT_TIMEOUT; purge_remaining is reported as unknown rather than 0 " +
                    "(the vector half of that count has no index yet)"
            }
            null
        }

    /**
     * One sweep across every catalog, for the background loop
     * (BackgroundLoops in App.kt). Catalogs are isolated: one catalog's
     * failure is logged and the rest proceed.
     */
    fun runOnceAllCatalogs(batchSize: Int): List<Pair<String, ExpiryResult>> {
        val catalogs = jdbi.withHandleUnchecked { h -> CatalogRepo.listAll(h) }
        val names = catalogs.map { it.name }
        val results = mutableListOf<Pair<String, ExpiryResult>>()
        for (name in names) {
            try {
                results += name to runOnce(name, batchSize, MaintenanceTrigger.LOOP)
            } catch (e: Exception) {
                // '$name', not '$': the string interpolation was
                // truncated, so every failure line named no catalog at
                // all — on a fan-out across the fleet that is a log
                // entry an operator cannot act on.
                log.error(e) { "expiry sweep failed for catalog '$name'; continuing" }
            }
        }
        // The fleet sweep is the only caller that knows the whole set, so
        // it is where a DROPPED catalog's remaining-rows gauge is
        // forgotten. Otherwise its last value is republished forever —
        // an alert on a catalog that no longer exists, which nobody can
        // close. Also drop its page hints, for the same reason and
        // because a recreated catalog of the same name should rediscover
        // its own cost.
        ExpiryGauges.retain(names.toSet())
        val live = catalogs.map { it.catalogId }.toSet()
        settledPage.keys.removeIf { key -> key.substringBefore('/').toLongOrNull() !in live }

        return results
    }
}
