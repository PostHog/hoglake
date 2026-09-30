package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
import com.posthog.hoglake.service.CleanupService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.File

/**
 * V21 — `hog_file_removal.claimed_at` / `claimed_by`, the cleanup drain's
 * claim — against a POPULATED queue, which the schema-equivalence gate
 * never sees (it folds the chain onto an empty database).
 *
 * The migration RUNS here: the database arrives at V20, a
 * production-shaped queue is seeded, the columns are observed ABSENT, and
 * then [Database.migrate] applies V21. So "the column was not there
 * before" is a measurement rather than a claim about a file, and the plan
 * assertions below are made against rows that existed before the
 * migration touched the table.
 *
 * WHAT IT HAS TO PROVE, beyond the two columns: that V21 adds NO INDEX
 * and needs none. The claim's candidate select is the old drain select
 * with two more filters, so `hog_file_removal_drain (catalog_id,
 * removal_id) WHERE drained_at IS NULL` (V16) must still supply both the
 * predicate and the ordering, with the claim clause demoted to a filter
 * over rows the index has already narrowed. The statement EXPLAINed is
 * `CleanupService.CLAIM_CANDIDATE_SQL` — the one production issues,
 * exposed `internal` for exactly this — not a hand-written lookalike
 * (V14's first index was proven against a predicate no code path sends).
 *
 * THE FIXTURE IS SPARSE ON PURPOSE. `hog_file_removal_drain` is the
 * planner's choice when a catalog's undrained rows are a small fraction
 * of the table, which is what a 30-day ledger gives a catalog that
 * drains; where they are dense the primary key arrives in the same order
 * and the planner takes that instead, correctly. A second catalog's queue
 * is INTERLEAVED in the id space, because `catalog_id` leads the index
 * and with one catalog "this catalog's undrained rows" and "every
 * undrained row" are the same rows.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class V21CleanupClaimMigrationIntegrationTest {
    private companion object {
        const val DRAIN_INDEX = "hog_file_removal_drain"

        /** Ledger rows across both catalogs; one in [UNDRAINED_EVERY] is queued. */
        const val ROWS = 200_000

        /**
         * One row in twenty is still queued — the shape a 30-day ledger
         * gives a catalog that drains, and what makes
         * `hog_file_removal_drain` the planner's choice rather than the
         * primary key.
         */
        const val UNDRAINED_EVERY = 20

        /**
         * One row in seven goes to the NEIGHBOUR catalog, interleaved —
         * see the class KDoc. COPRIME with [UNDRAINED_EVERY] on purpose:
         * with a shared factor every undrained row lands in one catalog,
         * and the first draft of this fixture did exactly that and left
         * the catalog under test with an EMPTY queue — a plan over no rows
         * that asserts nothing (2 buffers, one search, green).
         */
        const val OTHER_EVERY = 7

        const val MIGRATION = "src/main/resources/db/migration/V21__cleanup_claim.sql"

        /** Put the history back to before V21, so it can be re-applied. */
        const val REAPPLY = "DELETE FROM flyway_schema_history WHERE version::numeric >= 21"
    }

    private val db = PgTestSupport.freshDatabaseAt("20", productionSession = true)
    private var catalogId = 0L
    private var otherCatalogId = 0L
    private lateinit var planAfter: String

    @AfterAll
    fun tearDown() = db.close()

    @BeforeAll
    fun seedThenMigrate() {
        catalogId = createCatalog("v21")
        otherCatalogId = createCatalog("v21-neighbour")
        seedQueue()
        analyze()

        assertThat(columnType("claimed_at")).describedAs("the columns arrive with V21").isNull()
        assertThat(columnType("claimed_by")).isNull()

        Database.migrate(db.dataSource)
        analyze()
        planAfter = explainClaimCandidates()
    }

    private fun createCatalog(name: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "INSERT INTO hog_catalog (name, data_path) VALUES (:n, 's3://v21/' || :n) " +
                    "RETURNING catalog_id",
            ).bind("n", name).mapTo(Long::class.java).one()
        }

    /**
     * Two catalogs' rows INTERLEAVED, one row in [UNDRAINED_EVERY]
     * undrained, and a `compaction_staging` ticket every so often so the
     * claim's grace clause has rows to filter rather than being free.
     */
    private fun seedQueue() =
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason,
                                              scheduled_at, drained_at, drained_outcome)
                SELECT CASE WHEN g % :everyOther = 0 THEN :other ELSE :c END,
                       's3://v21/' || md5(g::text) || '/part-' || g || '.parquet', 'data',
                       CASE WHEN g % 50 = 0 THEN 'compaction_staging' ELSE 'snapshot_expiry' END,
                       now() - make_interval(secs => (g % 7200)::double precision),
                       CASE WHEN g % :every = 0 THEN NULL ELSE now() - interval '1 day' END,
                       CASE WHEN g % :every = 0 THEN NULL ELSE 'deleted' END
                FROM generate_series(1, :n) g
                """,
            )
                .bind("c", catalogId)
                .bind("other", otherCatalogId)
                .bind("every", UNDRAINED_EVERY)
                .bind("everyOther", OTHER_EVERY)
                .bind("n", ROWS)
                .execute()
        }

    private fun analyze() =
        db.jdbi.useHandleUnchecked { h ->
            h.execute("VACUUM (ANALYZE) hog_file_removal")
        }

    /**
     * The CANDIDATE select, not the claim UPDATE that wraps it: EXPLAIN
     * ANALYZE executes its statement, and of the UPDATE that would claim
     * the fixture's rows for real. Serial plans only — a Gather splits
     * the buffer counts across workers and makes every number depend on
     * the machine's core count.
     */
    private fun explainClaimCandidates(): String =
        db.jdbi.inTransactionUnchecked { h ->
            h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
            h.createQuery(
                "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " +
                    CleanupService.CLAIM_CANDIDATE_SQL,
            )
                .bind("catalogId", catalogId)
                .bind("limit", CleanupService.SUB_BATCH)
                .bind("leaseSeconds", CleanupService.CLAIM_LEASE_SECONDS.toDouble())
                .bind("stagingGraceSeconds", CleanupService.STAGING_GRACE_SECONDS.toDouble())
                .mapTo(String::class.java)
                .list()
                .joinToString("\n")
        }

    /**
     * The candidate select, EXECUTED and rolled back: how many rows a
     * claim would actually take. Without it the plan assertions can pass
     * over a scan that found nothing, which is how the first version of
     * this fixture was green with an empty queue.
     */
    private fun claimCandidateCount(): Int =
        db.jdbi.withHandleUnchecked { h ->
            h.begin()
            try {
                h.createQuery(CleanupService.CLAIM_CANDIDATE_SQL)
                    .bind("catalogId", catalogId)
                    .bind("limit", CleanupService.SUB_BATCH)
                    .bind("leaseSeconds", CleanupService.CLAIM_LEASE_SECONDS.toDouble())
                    .bind("stagingGraceSeconds", CleanupService.STAGING_GRACE_SECONDS.toDouble())
                    .mapTo(Long::class.javaObjectType)
                    .list()
                    .size
            } finally {
                h.rollback()
            }
        }

    private fun columnType(column: String): String? =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT data_type FROM information_schema.columns " +
                    "WHERE table_name = 'hog_file_removal' AND column_name = :c",
            ).bind("c", column).mapTo(String::class.java).findOne().orElse(null)
        }

    // ---- the migration -----------------------------------------------------

    @Test
    fun `V21 adds two nullable claim columns, with no default and no backfill`() {
        assertThat(columnType("claimed_at")).isEqualTo("timestamp with time zone")
        assertThat(columnType("claimed_by")).isEqualTo("text")
        val (nullable, default) =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT is_nullable, column_default FROM information_schema.columns
                    WHERE table_name = 'hog_file_removal' AND column_name = 'claimed_at'
                    """,
                ).map { rs, _ -> rs.getString(1) to rs.getString(2) }.one()
            }
        assertThat(nullable).describedAs("an existing row is UNCLAIMED, which is NULL").isEqualTo("YES")
        assertThat(default)
            .describedAs("a default would rewrite the largest ledger in the schema")
            .isNull()
        // Every row that existed before the migration is claimable: a
        // backfill to anything else would have hidden the whole queue from
        // the drain until somebody noticed.
        val claimed =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM hog_file_removal WHERE claimed_at IS NOT NULL " +
                        "OR claimed_by IS NOT NULL",
                ).mapTo(Long::class.java).one()
            }
        assertThat(claimed).isZero()
    }

    @Test
    fun `V21 is re-appliable, so a rolled-back deploy can simply run it again`() {
        db.jdbi.useHandleUnchecked { h -> h.execute(REAPPLY) }
        Database.migrate(db.dataSource)
        assertThat(columnType("claimed_at")).isEqualTo("timestamp with time zone")
        assertThat(columnType("claimed_by")).isEqualTo("text")
    }

    @Test
    fun `the file guards its ALTER with the lock_timeout window`() {
        // MigrationLockWindowTest enforces this rule across the whole
        // chain; asserted here too because it is the one line of V21 that
        // is about the DEPLOY rather than about the schema, and the review
        // history says deleting the pair leaves the suite green. Both
        // ALTERs are ACCESS EXCLUSIVE even though they rewrite nothing.
        val text = File(MIGRATION).readText()
        // '1s', not the chain's usual '5s': the timeout is the width of
        // the window in which a PENDING ACCESS EXCLUSIVE blocks every
        // reader and writer of this table behind it in the lock queue —
        // commits at ~1/s, expiry and retirement inserts under the commit
        // lock, compaction's ticket writes — and a failed attempt costs
        // only a rolled-back migration and a pod retry.
        assertThat(text).contains("SET LOCAL lock_timeout = '1s'")
        assertThat(text.indexOf("SET LOCAL lock_timeout"))
            .describedAs("the window must OPEN before the statement it guards")
            .isLessThan(text.indexOf("ALTER TABLE hog_file_removal"))
        assertThat(File("$MIGRATION.conf"))
            .describedAs(
                "V21 is transactional (V20's shape): a `.conf` would turn SET LOCAL into a " +
                    "setting that applies to nothing and a failure into a history row needing " +
                    "flyway repair",
            )
            .doesNotExist()
    }

    // ---- the statement V21 must not have broken -----------------------------

    @Test
    fun `the drain index still serves the claim's candidate select`() {
        // NON-VACUITY FIRST: a full sub-batch of rows is genuinely
        // claimable for THIS catalog, so the plan below is the plan of a
        // scan that did the work rather than of one that found nothing.
        assertThat(claimCandidateCount())
            .describedAs("the fixture must offer a full claim, or the plan proves nothing")
            .isEqualTo(CleanupService.SUB_BATCH)
        // AN INDEX, AND WHICH ONE IS THE FIXTURE'S BUSINESS. Both
        // `hog_file_removal_drain` (partial, `(catalog_id, removal_id)`)
        // and the primary key arrive in `removal_id` order and both stop
        // at the LIMIT; which the planner takes depends on how dense this
        // catalog's claimable rows are in the key, and at this fixture's
        // ~5% it is marginal enough to flip between runs on statistics
        // alone (observed both ways). V16's own test is where the SPARSE
        // shape's preference for the drain index is pinned, against a
        // fixture built for it. What V21 has to prove is that the claim's
        // two new filters did not cost the statement its index — so the
        // assertion is index-driven, one descent, no sort, no scan of the
        // ledger.
        assertThat(scannedIndex(planAfter))
            .describedAs("the claim must be index-driven:%n%s", planAfter)
            .isIn(DRAIN_INDEX, "hog_file_removal_pkey")
        assertThat(planAfter)
            .describedAs("a sequential scan of the ledger per claim is the shape this excludes:%n%s", planAfter)
            .doesNotContain("Seq Scan on hog_file_removal")
        assertThat(planAfter)
            .describedAs("the LIMIT must stop the scan, not trim a sort of every queued row:%n%s", planAfter)
            .doesNotContain("Sort")
        // One descent, and the claim clause is a FILTER over rows the
        // index already narrowed — which is what "no new index" rests on.
        val node = scanNode(planAfter)
        assertThat(node.indexSearches)
            .describedAs("one descent per claim:%n%s", planAfter)
            .isEqualTo(1)
        // The work sizes with the BATCH, not with the ledger. Derived from
        // the batch rather than written down: an index descent plus a heap
        // buffer per row it emits, with slack for the filtered rows it
        // walks past.
        assertThat(node.buffers)
            .describedAs("a scan that read nothing is not evidence:%n%s", planAfter)
            .isGreaterThan(CleanupService.SUB_BATCH / 10L)
        assertThat(node.buffers)
            .describedAs(
                "%d buffers for a %d-row claim over a %d-row ledger:%n%s",
                node.buffers,
                CleanupService.SUB_BATCH,
                ROWS,
                planAfter,
            )
            .isLessThanOrEqualTo(4L * CleanupService.SUB_BATCH)
        println(
            "[V21] claim candidate select: ${node.buffers} buffers, " +
                "${node.indexSearches} searches, index=${node.index}",
        )
    }

    @Test
    fun `the STAGING arm's candidate select is index-driven too, and needs no index of its own`() {
        // "V21 adds no index" was proven for one arm of two. The staging
        // claim adds `reason = 'compaction_staging'` and `scheduled_at <
        // now() - grace` — two columns nothing indexes — and on prod-us it
        // runs against ~9.4k tickets inside a 2.6M-row queue. The argument
        // is the same as the bulk arm's (the drain index narrows to one
        // catalog's undrained rows and both new terms are filters over
        // that), and this is the measurement of it rather than the
        // restatement: same index, one descent, no Seq Scan, no Sort.
        val plan =
            db.jdbi.inTransactionUnchecked { h ->
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                h.createQuery(
                    "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " +
                        CleanupService.CLAIM_STAGING_CANDIDATE_SQL,
                )
                    .bind("catalogId", catalogId)
                    .bind("limit", CleanupService.STAGING_SUB_BATCH)
                    .bind("leaseSeconds", CleanupService.CLAIM_LEASE_SECONDS.toDouble())
                    .bind("stagingGraceSeconds", 0.0)
                    .mapTo(String::class.java)
                    .list()
                    .joinToString("\n")
            }
        // NON-VACUITY FIRST, as for the bulk arm: a full staging claim is
        // genuinely available, so the plan below is of a scan that did the
        // work. The fixture seeds a `compaction_staging` ticket every 50
        // rows, which at this catalog's share is well over one claim.
        val available =
            db.jdbi.withHandleUnchecked { h ->
                h.begin()
                try {
                    h.createQuery(CleanupService.CLAIM_STAGING_CANDIDATE_SQL)
                        .bind("catalogId", catalogId)
                        .bind("limit", CleanupService.STAGING_SUB_BATCH)
                        .bind("leaseSeconds", CleanupService.CLAIM_LEASE_SECONDS.toDouble())
                        .bind("stagingGraceSeconds", 0.0)
                        .mapTo(Long::class.javaObjectType)
                        .list()
                        .size
                } finally {
                    h.rollback()
                }
            }
        assertThat(available)
            .describedAs("the fixture must offer a full staging claim, or the plan proves nothing")
            .isEqualTo(CleanupService.STAGING_SUB_BATCH)

        val node = scanNode(plan)
        assertThat(node.index)
            .describedAs("the staging arm rides V16's drain index, like the bulk arm:%n%s", plan)
            .isEqualTo(DRAIN_INDEX)
        assertThat(plan)
            .describedAs("a sequential scan per staging claim is the shape this excludes:%n%s", plan)
            .doesNotContain("Seq Scan on hog_file_removal")
        assertThat(plan)
            .describedAs("the LIMIT must stop the scan, not trim a sort:%n%s", plan)
            .doesNotContain("Sort")
        assertThat(node.indexSearches)
            .describedAs("one descent per claim:%n%s", plan)
            .isEqualTo(1)
        // The two unindexed terms are FILTERS over rows the index already
        // narrowed, which is the whole "no new index" argument. What it
        // costs is the rows they discard on the way to 25, and that is
        // reported rather than asserted at a constant: if it ever grows to
        // the size of the catalog's queue, the argument is wrong and the
        // staging arm wants `(catalog_id, reason, removal_id)`.
        println(
            "[V21] staging claim: ${node.buffers} buffers, ${node.indexSearches} searches, " +
                "${node.rowsRemovedByFilter ?: 0} rows filtered for " +
                "${CleanupService.STAGING_SUB_BATCH} tickets, index=${node.index}",
        )
    }

    @Test
    fun `the claim locks its candidates as it reads them`() {
        // The plan has to carry the LockRows the partitioning argument
        // rests on. It does NOT prove `SKIP LOCKED`: the plans of
        // `FOR UPDATE` and `FOR UPDATE SKIP LOCKED` are identical, because
        // the wait policy is not a plan property and EXPLAIN never prints
        // it. The SKIP is driven behaviourally instead, against a row held
        // by a second connection, in
        // `CleanupServiceIntegrationTest.a claim SKIPS the rows another
        // claim holds instead of waiting for them`.
        assertThat(planAfter)
            .describedAs("the candidates are locked as they are read:%n%s", planAfter)
            .contains("LockRows")
    }

    @Test
    fun `the drained-ledger purge pages the primary key instead of scanning the table`() {
        // THE SHAPE THAT CAUSED THE 2026-09-28 OUTAGE, removed and pinned
        // here because this fixture is the only one with a
        // production-shaped ledger. The purge used to be one
        // `DELETE ... WHERE catalog_id = :c AND drained_at < cutoff`, and
        // NO index on this table can serve that predicate — both are
        // partial on `drained_at IS NULL`, the complement of the rows it
        // deletes — so it was a sequential scan with an unbounded row
        // count, once per catalog per run, against a ledger heading for
        // ~137M rows.
        //
        // What replaced it pages the PRIMARY KEY: the property is an index
        // scan whose row count is the page, never a Seq Scan of the
        // ledger.
        val plan =
            db.jdbi.withHandleUnchecked { h ->
                h.begin()
                try {
                    h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                    h.createQuery(
                        "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " +
                            CleanupService.PURGE_PAGE_SQL,
                    )
                        .bind("catalogId", catalogId)
                        .bind("after", 0L)
                        .bind("page", CleanupService.LEDGER_PURGE_PAGE)
                        .bind("retention", 1L)
                        .mapTo(String::class.java)
                        .list()
                        .joinToString("\n")
                } finally {
                    // EXPLAIN ANALYZE EXECUTES the DELETE, and the rest of
                    // this class asserts against these rows.
                    h.rollback()
                }
            }
        assertThat(plan)
            .describedAs("a sequential scan of the ledger per run is the shape this removes:%n%s", plan)
            .doesNotContain("Seq Scan on hog_file_removal")
        assertThat(plan)
            .describedAs("the page is read through the primary key:%n%s", plan)
            .contains("hog_file_removal_pkey")
        // The inner select's LIMIT is what bounds the statement, so the
        // rows it touches size with the page and not with the ledger.
        val limited =
            Regex("""Limit \(actual rows=(\d+)""").find(plan)?.groupValues?.get(1)?.toInt()
        // EQUAL TO THE PAGE, not `<=` it: this fixture holds 200k rows, so
        // a full page is the honest expectation, and `<=` is satisfied by
        // an empty window — the vacuity the claim test guards with
        // `claimCandidateCount()` and this one used not to.
        assertThat(limited)
            .describedAs("the page must be FULL and bounded by the LIMIT:%n%s", plan)
            .isEqualTo(CleanupService.LEDGER_PURGE_PAGE)
        // AND THE PAGE BOUNDS THE ROWS EXAMINED, which is the property the
        // walk's `catalog_id`-free window exists for: a per-catalog page
        // would descend the key and throw most of it away, so the window's
        // scan must discard nothing on its way to a full page.
        //
        // ASSERTED ON THE `page` CTE'S SCAN NODE ALONE, via the same
        // helper the claim plans use. The DELETE's inner scan also carries
        // a `Rows Removed by Filter` line and it CANNOT be read this way:
        // it runs with `loops = :page`, so EXPLAIN prints a per-loop
        // AVERAGE — this fixture filters 50 undrained rows out of a
        // 1,000-row window and the line still reads `0`, which would make
        // a blanket assertion over every node satisfied by arithmetic
        // rather than by the property.
        val window = scanNode(plan)
        assertThat(window.index)
            .describedAs("the window is a primary-key page:%n%s", plan)
            .isEqualTo("hog_file_removal_pkey")
        assertThat(window.indexSearches)
            .describedAs("one descent per page:%n%s", plan)
            .isEqualTo(1)
        assertThat(window.rowsRemovedByFilter ?: 0L)
            .describedAs("the window itself must throw nothing away:%n%s", plan)
            .isZero()
        println("[V21] ledger purge page plan bounded at $limited rows of a $ROWS-row ledger")
    }

    private data class ScanNode(
        val index: String?,
        val buffers: Long,
        val indexSearches: Long?,
        val rowsRemovedByFilter: Long?,
    )

    /**
     * ONE scan node of `hog_file_removal`: how it was driven, how many
     * descents it made and what it cost. Scoped to the node rather than
     * read off the root, because EXPLAIN's `Planning:` section carries a
     * `Buffers:` line of its own that dwarfs a well-indexed scan's.
     */
    private fun scanNode(text: String): ScanNode {
        val lines = text.lines()

        fun indent(l: String) = l.length - l.trimStart().length
        val i = lines.indexOfFirst { Regex("""Scan.* on hog_file_removal\b""").containsMatchIn(it) }
        if (i < 0) throw AssertionError("no scan of hog_file_removal in:\n$text")
        val line = lines[i]
        val detail = lines.drop(i + 1).takeWhile { it.isNotBlank() && indent(it) > indent(line) }
        val buffersLine = detail.firstOrNull { it.trim().startsWith("Buffers:") }
        val hit = buffersLine?.let { Regex("""shared hit=(\d+)""").find(it)?.groupValues?.get(1)?.toLong() } ?: 0L
        val read = buffersLine?.let { Regex("""shared read=(\d+)""").find(it)?.groupValues?.get(1)?.toLong() } ?: 0L

        fun detailLong(prefix: String): Long? =
            detail.firstOrNull { it.trim().startsWith(prefix) }
                ?.let { Regex("""(\d+)""").find(it.substringAfter(prefix))?.value?.toLong() }
        return ScanNode(
            index = Regex("""Scan(?: Backward)? using (\S+) on""").find(line)?.groupValues?.get(1),
            buffers = hit + read,
            indexSearches = detailLong("Index Searches:"),
            rowsRemovedByFilter = detailLong("Rows Removed by Filter:"),
        )
    }

    private fun scannedIndex(text: String): String? = scanNode(text).index
}
