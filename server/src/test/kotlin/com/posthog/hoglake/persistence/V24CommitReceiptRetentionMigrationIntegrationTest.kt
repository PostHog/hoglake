package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
import com.posthog.hoglake.service.CleanupService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * V24 against the two things it has to be true about: that its
 * `created_at` is a METADATA-ONLY fast default on a table that is 58 GiB
 * in production, and that its index is what the purge's page actually
 * descends.
 *
 * THE MIGRATION RUNS HERE, in V22's and V19's shape — the database
 * arrives at V22, [RECEIPTS] receipts are seeded with the pre-V24 shape
 * (a `request` body, no digest, no timestamp), and only then does
 * [Database.migrate] apply V24. So the fast-default assertion is a
 * measurement on rows that existed BEFORE the column, which is the only
 * state in which "no rewrite" means anything.
 *
 * TOLERANT OF THE SIBLING V23. `jakob/compaction-plan-bounded` adds
 * `V23__partition_value_lookup.sql`; this fixture arrives at 22 and then
 * migrates to HEAD, so it applies V23 as well if the branch has landed
 * and nothing if it has not. Nothing here names 23, and nothing asserts
 * the chain's length.
 *
 * THE "BEFORE" PLAN IS THIS MIGRATION MINUS ITS INDEX, not the database
 * at V22. The purge's statement reads `created_at`, which V24 itself
 * adds, so there is no V22 form of it to explain — a rewritten
 * "pre-V24 statement" would be a different statement, and AGENT.md is
 * explicit that a copy asserts only that it compiles. Dropping the index
 * leaves the same rows, the same statistics and the same statement with
 * the one thing under test removed — and the drop happens INSIDE the
 * rolled-back EXPLAIN transaction (DDL is transactional in Postgres), so
 * the schema the other cases read is still the one the migration built.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class V24CommitReceiptRetentionMigrationIntegrationTest {
    private companion object {
        const val INDEX = "hog_commit_receipt_created"

        /**
         * Receipts seeded before the migration.
         *
         * 100,000 is AGENT.md's floor for a plan test on a hot statement,
         * and it is the right order for this one: gigahog-prod-us holds
         * 366,740. Below ~10k rows the planner picks a sequential scan for
         * the purge's page whatever indexes exist, and the test would pass
         * for the wrong reason.
         */
        const val RECEIPTS = 100_000

        /**
         * How much of the `created_at` span sits past the cutoff, which
         * puts ~60% of the rows on the eligible side (measured: 60,199 of
         * 100,000) — the shape a catalog has partway through working off
         * a backlog. A fixture where everything is eligible makes the
         * index's descent free, because the whole range qualifies; one
         * where nothing is makes the page empty.
         */
        const val ELIGIBLE_FRACTION = 0.4

        /** Retention the plan is explained at, in seconds. */
        const val PLAN_RETENTION_SECONDS = 7L * 24 * 60 * 60

        /**
         * Make Flyway run V24 again on a database that already has it.
         * V17's mechanism, for V17's reason: the file is
         * `executeInTransaction=false`, so a partial failure leaves a
         * `success = false` history row that fails validate on every
         * replica until someone repairs it — which means re-entry is a
         * path an operator actually reaches, on a 58 GiB table, after a
         * failed promotion.
         */
        const val REAPPLY_V24 = "DELETE FROM flyway_schema_history WHERE version::numeric >= 24"
    }

    // productionSession: V24 builds CONCURRENTLY with statement_timeout
    // lifted for the build and restored after, and a booting pod's
    // session carries the 60 s bound. The fixture reaches that behaviour
    // rather than running on a session with no bounds at all — V10, V14,
    // V16 and V17's re-entry cases all do the same.
    private val db = PgTestSupport.freshDatabaseAt("22", productionSession = true)
    private var catalogId = 0L

    private lateinit var planWithIndex: String
    private lateinit var planWithoutIndex: String

    @AfterAll
    fun tearDown() = db.close()

    @BeforeAll
    fun seedThenMigrate() {
        catalogId =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "INSERT INTO hog_catalog (name, data_path, last_snapshot_id) " +
                        "VALUES ('v24', 's3://v24', 1000) RETURNING catalog_id",
                ).mapTo(Long::class.java).one()
            }
        db.jdbi.useHandleUnchecked { h ->
            // The pre-V24 shape: `request` is NOT NULL here, so every row
            // carries a body. Deliberately a SMALL one — production's is
            // 566 KiB after TOAST and 100,000 of those is 56 GiB, which is
            // not a fixture. What the plan test needs from the body is that
            // it exists (so the rows are real, so the heap is real); the
            // 58 GiB figure is production's, quoted in V24's header, and
            // measured nowhere in this suite.
            h.createUpdate(
                """
                INSERT INTO hog_commit_receipt
                    (catalog_id, idempotency_key, request, snapshot_id, schema_version)
                SELECT :c,
                       ('00000000-0000-4000-8000-' || lpad(g::text, 12, '0'))::uuid,
                       jsonb_build_object('appends', g),
                       g, 1
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("n", RECEIPTS).execute()
        }
        assertThat(indexDef()).describedAs("%s absent before V24", INDEX).isNull()

        Database.migrate(db.dataSource)

        // Spread `created_at` over a fortnight so the cutoff cuts the
        // catalog's range in two, and SCATTER it: `random()` rather than a
        // function of `g`, because rows ordered by insertion would put
        // every eligible row on the same heap pages and flatter the index
        // exactly the way AGENT.md records for V19.
        db.jdbi.useHandleUnchecked { h ->
            h.createUpdate(
                """
                UPDATE hog_commit_receipt
                   SET created_at = now() - make_interval(secs => (random() * :span)::bigint)
                 WHERE catalog_id = :c
                """,
            ).bind("c", catalogId).bind("span", (PLAN_RETENTION_SECONDS / ELIGIBLE_FRACTION).toLong())
                .execute()
            h.execute("VACUUM (ANALYZE) hog_commit_receipt")
        }
        planWithIndex = explainPurgePage()
        planWithoutIndex = explainPurgePage(dropIndexFirst = true)
        // PRINTED, because the assertions below are structural (an index
        // name, a missing Seq Scan, a zero Rows Removed) and the FIGURES
        // are what V24's header quotes. A reviewer reading the migration's
        // "measured on a 100,000-receipt fixture" paragraph should be able
        // to find the numbers in a test run rather than take them on trust.
        println("V24 purge page, $RECEIPTS receipts, WITH the index:\n$planWithIndex")
        println("V24 purge page, $RECEIPTS receipts, WITHOUT it:\n$planWithoutIndex")
    }

    private fun indexDef(): String? =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT indexdef FROM pg_indexes WHERE indexname = :n")
                .bind("n", INDEX).mapTo(String::class.java).findOne().orElse(null)
        }

    /**
     * EXPLAIN the purge's own page statement, rolled back.
     *
     * `EXPLAIN (ANALYZE)` of a DELETE DELETES, so the whole thing runs in
     * a transaction that is thrown away — which is also what keeps the two
     * plans comparable: both see the same 100,000 rows. [dropIndexFirst]
     * drops V24's index inside that same doomed transaction, which is how
     * the "before" plan is produced without leaving the schema changed.
     *
     * Serial plans only: a Gather splits the buffer counts across workers.
     */
    private fun explainPurgePage(dropIndexFirst: Boolean = false): String =
        db.jdbi.withHandleUnchecked { h ->
            h.begin()
            try {
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                if (dropIndexFirst) h.execute("DROP INDEX $INDEX")
                h.createQuery(
                    "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " +
                        CleanupService.RECEIPT_PURGE_PAGE_SQL,
                )
                    .bind("catalogId", catalogId)
                    .bind("page", CleanupService.RECEIPT_PURGE_PAGE)
                    .bind("retention", PLAN_RETENTION_SECONDS)
                    .mapTo(String::class.java).list().joinToString("\n")
            } finally {
                h.rollback()
            }
        }

    private fun rowsRemoved(plan: String): Long =
        Regex("""Rows Removed by Filter: (\d+)""").findAll(plan)
            .sumOf { it.groupValues[1].toLong() }

    // ---- the columns --------------------------------------------------------

    @Test
    fun `created_at is a fast default, so the 58 GiB table is not rewritten`() {
        val missing =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT atthasmissing, attmissingval::text AS missingval
                    FROM pg_attribute
                    WHERE attrelid = 'hog_commit_receipt'::regclass AND attname = 'created_at'
                    """,
                ).map { rs, _ -> rs.getBoolean("atthasmissing") to rs.getString("missingval") }.one()
            }
        // THE ASSERTION THAT SAYS "NO REWRITE". `atthasmissing` is true
        // only when Postgres stored a value for the rows that predate the
        // column instead of rewriting them into a new heap — i.e. when the
        // default was non-volatile, which `now()` (STABLE) is and
        // `clock_timestamp()` (VOLATILE) would not be.
        //
        // MUTATION: change V24's default to `clock_timestamp()` and this
        // reds, because Postgres rewrites the table instead.
        assertThat(missing.first)
            .describedAs("created_at must be a fast default, not a rewrite: %s", missing.second)
            .isTrue()
        assertThat(missing.second).isNotNull()
    }

    @Test
    fun `every pre-V24 row is dated at the migration, not at its own unknowable age`() {
        // Re-read from a database that has NOT had its timestamps spread
        // (seedThenMigrate does that afterwards for the plan), so this
        // needs its own arrival at V22.
        PgTestSupport.freshDatabaseAt("22").use { fresh ->
            val id =
                fresh.jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        "INSERT INTO hog_catalog (name, data_path) VALUES ('legacy', 's3://l') " +
                            "RETURNING catalog_id",
                    ).mapTo(Long::class.java).one()
                }
            fresh.jdbi.useHandleUnchecked { h ->
                h.createUpdate(
                    """
                    INSERT INTO hog_commit_receipt
                        (catalog_id, idempotency_key, request, snapshot_id, schema_version)
                    SELECT :c, gen_random_uuid(), '{}'::jsonb, g, 1 FROM generate_series(1, 500) g
                    """,
                ).bind("c", id).execute()
            }
            Database.migrate(fresh.dataSource)
            val distinct =
                fresh.jdbi.withHandleUnchecked { h ->
                    h.createQuery("SELECT count(DISTINCT created_at) FROM hog_commit_receipt")
                        .mapTo(Long::class.java).one()
                }
            // ONE value across every legacy row: the migration's own
            // timestamp, which is what makes the whole legacy backlog
            // become purgeable together one retention window after the
            // deploy — and what makes every legacy row look YOUNGER than
            // it is, so nothing is purged early.
            assertThat(distinct)
                .describedAs("a STABLE default is evaluated once for the whole ALTER")
                .isEqualTo(1)
            val nulls =
                fresh.jdbi.withHandleUnchecked { h ->
                    h.createQuery("SELECT count(*) FROM hog_commit_receipt WHERE fingerprint IS NOT NULL")
                        .mapTo(Long::class.java).one()
                }
            // A legacy row has no digest, which is exactly what makes
            // CommitService take the body-comparison arm for it.
            assertThat(nulls).isZero()
        }
    }

    @Test
    fun `the request column becomes nullable so the writer can stop filling it`() {
        val nullable =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT is_nullable FROM information_schema.columns
                    WHERE table_name = 'hog_commit_receipt' AND column_name = 'request'
                    """,
                ).mapTo(String::class.java).one()
            }
        assertThat(nullable).isEqualTo("YES")
    }

    // ---- the index ----------------------------------------------------------

    @Test
    fun `the index keys exactly the columns the purge's page filters and orders on`() {
        val key =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_class c ON c.oid = i.indexrelid
                    JOIN unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = k.attnum
                    WHERE c.relname = :n
                    ORDER BY k.ord
                    """,
                ).bind("n", INDEX).mapTo(String::class.java).list()
            }
        // Resolved through pg_index.indkey, not by grepping indexdef — a
        // `contains("created_at")` over the definition is satisfied by the
        // index's own NAME (AGENT.md).
        assertThat(key).containsExactly("catalog_id", "created_at")
    }

    @Test
    fun `without the index a purge page scans the catalog's whole receipt history`() {
        assertThat(planWithoutIndex)
            .describedAs("the primary key leads on a random UUID, so nothing serves the page:\n%s", planWithoutIndex)
            .contains("hog_commit_receipt")
        // The ineligible 60%, read and discarded to find the oldest 1,000
        // — or a sort of the whole table, depending on how the planner
        // spells it. Either way the work is O(the catalog's receipts) per
        // page, which is the shape the 2026-09-28 expiry outage had.
        assertThat(planWithoutIndex)
            .describedAs("without the index the page cannot be a descent:\n%s", planWithoutIndex)
            .contains("Seq Scan on hog_commit_receipt")
        // The ineligible rows, read and discarded to find the oldest
        // 1,000 — the work that grows with the catalog's whole receipt
        // history on every page.
        assertThat(rowsRemoved(planWithoutIndex))
            .describedAs("the scan reads the catalog's receipts to answer one page:\n%s", planWithoutIndex)
            .isGreaterThan(1_000)
    }

    @Test
    fun `with the index a purge page is a descent and filters nothing`() {
        assertThat(planWithIndex)
            .describedAs("V24's index must serve the purge's page:\n%s", planWithIndex)
            .contains(INDEX)
        assertThat(planWithIndex)
            .describedAs("no sequential scan of the receipt table:\n%s", planWithIndex)
            .doesNotContain("Seq Scan on hog_commit_receipt")
        // NOTHING filtered and NOTHING sorted: the index range IS the
        // eligible set, in `created_at` order, so the page is one descent
        // and a walk. A plan that reads the catalog's receipts and demotes
        // `created_at` to a Filter says so here — the assertion AGENT.md
        // records as the one that separates a driving index from a leading
        // column that merely admits the scan.
        assertThat(rowsRemoved(planWithIndex))
            .describedAs("the index must not leave created_at as a Filter:\n%s", planWithIndex)
            .isEqualTo(0)
        assertThat(planWithIndex)
            .describedAs("the index provides the ORDER BY; a Sort means it did not:\n%s", planWithIndex)
            .doesNotContain("Sort Method")
    }

    // ---- re-entry -------------------------------------------------------------

    @Test
    fun `re-entry is a clean no-op - an existing index is not rebuilt`() {
        // Every statement in V24 is idempotent for this path:
        // `ADD COLUMN IF NOT EXISTS` twice, `DROP NOT NULL` on a column
        // that already allows null, and `CREATE INDEX CONCURRENTLY IF NOT
        // EXISTS` over an index that is already there. What must hold is
        // that the index is FOUND and SKIPPED rather than dropped and
        // rebuilt — the relfilenode is the only thing that can tell those
        // apart, and a rebuild is a second full heap pass budgeted for
        // exactly once.
        PgTestSupport.freshDatabase(productionSession = true).use { fresh ->
            val before = indexNode(fresh, INDEX)
            assertThat(before).isNotZero()
            fresh.jdbi.useHandleUnchecked { h -> h.execute(REAPPLY_V24) }
            Database.migrate(fresh.dataSource)
            assertThat(indexNode(fresh, INDEX))
                .describedAs("an existing index must not be dropped and rebuilt")
                .isEqualTo(before)
            // And the columns survive the second application unchanged —
            // `atthasmissing` is still set, so nothing rewrote the table
            // on the way through.
            assertThat(hasMissing(fresh))
                .describedAs("re-entry must not turn the fast default into a rewrite")
                .isTrue()
        }
    }

    @Test
    fun `an INVALID remnant is cleared rather than skipped forever`() {
        PgTestSupport.freshDatabase(productionSession = true).use { fresh ->
            // Exactly what a cancelled `CREATE INDEX CONCURRENTLY` leaves
            // behind: a killed pod, a statement timeout, an operator's
            // Ctrl-C. Forged, because killing a real concurrent build
            // mid-flight is not reproducible in a test. Without the DO
            // block, `CREATE INDEX CONCURRENTLY IF NOT EXISTS` matches on
            // NAME, sees the remnant, skips, and leaves an index every
            // commit's receipt insert maintains and no query may use.
            fresh.jdbi.useHandleUnchecked { h ->
                h.execute(
                    "UPDATE pg_index SET indisvalid = false WHERE indexrelid = " +
                        "(SELECT oid FROM pg_class WHERE relname = ?)",
                    INDEX,
                )
                h.execute(REAPPLY_V24)
            }
            assertThat(indexValid(fresh, INDEX)).isFalse()

            Database.migrate(fresh.dataSource)

            assertThat(indexValid(fresh, INDEX))
                .describedAs("the remnant must be dropped and rebuilt, not skipped")
                .isTrue()
        }
    }

    private fun indexNode(
        target: PgTestSupport.TestDb,
        name: String,
    ): Long =
        target.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT relfilenode FROM pg_class WHERE relname = :n")
                .bind("n", name).mapTo(Long::class.java).findOne().orElse(0L)
        }

    private fun indexValid(
        target: PgTestSupport.TestDb,
        name: String,
    ): Boolean =
        target.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid " +
                    "WHERE c.relname = :n",
            ).bind("n", name).mapTo(Boolean::class.javaObjectType).findOne().orElse(false)
        }

    private fun hasMissing(target: PgTestSupport.TestDb): Boolean =
        target.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT atthasmissing FROM pg_attribute " +
                    "WHERE attrelid = 'hog_commit_receipt'::regclass AND attname = 'created_at'",
            ).mapTo(Boolean::class.javaObjectType).one()
        }
}
