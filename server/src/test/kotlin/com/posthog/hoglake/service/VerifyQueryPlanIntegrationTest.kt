package com.posthog.hoglake.service

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
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

/**
 * The plans behind the checks' PATH-EQUALITY sub-queries, against a
 * manifest the size of a real one.
 *
 * `hog_data_file`, `hog_delete_file` and `hog_file_removal` carry NO
 * index on `path` — their hot predicates are (catalog, file id) and
 * (catalog, removal id) — so every one of these sub-queries asks a
 * question the schema has no access path for. That is fine exactly once
 * per query: a single scan of the manifest, hashed, joined against the
 * candidate set. It is a catastrophe if any node runs per candidate,
 * which is what a small dev catalog will always look fast doing.
 *
 * Two things this test has to get right to mean anything, both learned
 * the hard way:
 *
 *  - EVERY query must have CANDIDATES. A sub-query whose driving scan
 *    finds no rows plans beautifully and proves nothing: the inner
 *    scans come back `never executed`, carry no `loops=`, and a
 *    detector looking for `loops=` greater than one silently passes.
 *    So the fixture seeds at least one row into every violation class,
 *    and `never executed` on a manifest scan FAILS.
 *  - The statement under EXPLAIN must be the statement production runs
 *    (`VerifyService.bounded`), not the bare fragment: the wrapper adds
 *    a WindowAgg, a Sort and a LIMIT, and a LIMIT can change the plan
 *    underneath it.
 *
 * The verdict, if a plan ever flips: restructure the query (a join or a
 * LEFT JOIN ... IS NULL anti-join the planner can flatten), NOT a
 * migration. A `path` index on `hog_data_file` would be paid for by
 * every commit, on the hottest insert in the system, to serve an
 * hourly read.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VerifyQueryPlanIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private var catalogId = 0L

    @AfterAll
    fun tearDown() = db.close()

    private companion object {
        const val DATA_FILES = 50_000
        const val DELETE_FILES = 1_000
        const val LEDGER_ROWS = 4_000
        const val UPLOAD_CLAIMS = 2_000

        /** Tables with no index on `path`; a rescan of one is the bug. */
        val MANIFEST_TABLES = listOf("hog_data_file", "hog_delete_file", "hog_file_removal")

        /** Violations the fixture seeds ON PURPOSE, so every query has candidates. */
        const val REMOVAL_QUEUE_VIOLATIONS = 2L
        const val STAGING_VIOLATIONS = 5L
        const val UPLOAD_VIOLATIONS = 3L
    }

    @BeforeAll
    fun seed() {
        catalogs.createCatalog("plan", "s3://plan")
        catalogs.createNamespace("plan", "ns")
        catalogs.createTable("plan", "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        catalogId =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = 'plan'")
                    .mapTo(Long::class.java).one()
            }
        val tableId =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT table_id FROM hog_table WHERE catalog_id = :c")
                    .bind("c", catalogId).mapTo(Long::class.java).one()
            }
        // Read the head rather than assuming it: the generated rows must
        // sit inside the catalog's own snapshot range or visibility_bounds
        // flags them, and the number of snapshots createTable/createNamespace
        // allocate is not this test's business to know.
        val head =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT last_snapshot_id FROM hog_catalog WHERE catalog_id = :c")
                    .bind("c", catalogId).mapTo(Long::class.java).one()
            }
        check(head >= 1) { "fixture needs at least one snapshot below head" }
        db.jdbi.useHandleUnchecked { h ->
            // A manifest, generated rather than committed: this test is
            // about the PLAN, and 50k real commits would take minutes.
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start)
                SELECT :c, g, :t, :begin, 's3://plan/data/' || g || '.parquet', 10, 1024, g * 10
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("t", tableId).bind("n", DATA_FILES)
                .bind("begin", head).execute()
            h.createUpdate(
                """
                INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id,
                                             begin_snapshot, end_snapshot, path, delete_count,
                                             file_size_bytes)
                SELECT :c, g, :t, g, :begin, NULL, 's3://plan/dv/' || g || '.puffin', 1, 16
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("t", tableId).bind("n", DELETE_FILES)
                .bind("begin", head).execute()
            // The BULK of the ledger: healthy rows, so the candidate sets
            // are the sizes a real catalog produces. Settled compaction
            // claims point at live manifest paths; drained expiry rows
            // point at paths that are gone; an undrained tail is work the
            // cleanup drain has not reached.
            h.createUpdate(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason, scheduled_at,
                                              drained_at, drained_outcome)
                SELECT :c,
                       CASE WHEN g % 5 <> 0 AND g % 3 = 0
                            THEN 's3://plan/data/' || g || '.parquet'
                            ELSE 's3://plan/gone/' || g || '.parquet' END,
                       'data',
                       CASE WHEN g % 5 <> 0 AND g % 3 = 0
                            THEN 'compaction_staging' ELSE 'snapshot_expiry' END,
                       now() - interval '2 days',
                       CASE WHEN g % 5 = 0 THEN NULL ELSE now() END,
                       CASE WHEN g % 5 = 0 THEN NULL
                            WHEN g % 3 = 0 THEN 'registered' ELSE 'deleted' END
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("n", LEDGER_ROWS).execute()
            h.createUpdate(
                """
                INSERT INTO hog_upload (catalog_id, upload_id, owner, prefix, path, file_kind, state)
                SELECT :c, gen_random_uuid(), gen_random_uuid(), 's3://plan',
                       's3://plan/upload/' || g || '.parquet', 'data',
                       CASE WHEN g % 3 = 0 THEN 'registered'
                            WHEN g % 3 = 1 THEN 'active' ELSE 'abandoned' END
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("n", UPLOAD_CLAIMS).execute()

            // --- and now one row of EVERY violation class, so no query
            // --- plans against an empty candidate set.

            // removal_queue: undrained entries whose path a live file row claims.
            h.createUpdate(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                VALUES (:c, 's3://plan/data/11.parquet', 'data', 'snapshot_expiry'),
                       (:c, 's3://plan/dv/11.puffin', 'delete', 'snapshot_expiry')
                """,
            ).bind("c", catalogId).execute()
            // staging_tickets (a): settled 'registered', path unknown to the catalog.
            h.createUpdate(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason,
                                              drained_at, drained_outcome)
                SELECT :c, 's3://plan/ghost/' || g || '.parquet', 'data', 'compaction_staging',
                       now(), 'registered'
                FROM generate_series(1, 3) g
                """,
            ).bind("c", catalogId).execute()
            // staging_tickets (b): an undrained claim older than any bound.
            h.createUpdate(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason, scheduled_at)
                VALUES (:c, 's3://plan/leaked.parquet', 'data', 'compaction_staging',
                        now() - interval '30 days')
                """,
            ).bind("c", catalogId).execute()
            // staging_tickets (c): drained 'absent' over a path that IS a file row.
            h.createUpdate(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason,
                                              drained_at, drained_outcome)
                VALUES (:c, 's3://plan/data/12.parquet', 'data', 'compaction_staging',
                        now(), 'absent')
                """,
            ).bind("c", catalogId).execute()
            // upload_claims: a registered claim queued for reclamation...
            h.createUpdate(
                """
                INSERT INTO hog_upload (catalog_id, upload_id, owner, prefix, path, file_kind, state)
                VALUES (:c, gen_random_uuid(), gen_random_uuid(), 's3://plan',
                        's3://plan/fenced.parquet', 'data', 'registered')
                """,
            ).bind("c", catalogId).execute()
            h.createUpdate(
                """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                VALUES (:c, 's3://plan/fenced.parquet', 'data', 'trino_upload')
                """,
            ).bind("c", catalogId).execute()
            // ...and two unsettled claims over live paths.
            h.createUpdate(
                """
                INSERT INTO hog_upload (catalog_id, upload_id, owner, prefix, path, file_kind, state)
                VALUES (:c, gen_random_uuid(), gen_random_uuid(), 's3://plan',
                        's3://plan/data/13.parquet', 'data', 'active'),
                       (:c, gen_random_uuid(), gen_random_uuid(), 's3://plan',
                        's3://plan/dv/13.puffin', 'delete', 'abandoned')
                """,
            ).bind("c", catalogId).execute()

            // The allocator remembers what the generated manifest took.
            h.createUpdate(
                "UPDATE hog_table_stats SET next_row_id = :next WHERE catalog_id = :c",
            ).bind("c", catalogId).bind("next", (DATA_FILES + 1L) * 10).execute()
            for (table in MANIFEST_TABLES + "hog_upload") h.execute("ANALYZE $table")
        }
    }

    private fun plan(query: VerifyService.Companion.PathQuery): String =
        db.jdbi.inTransactionUnchecked { h ->
            // Serial plans only, for the duration of this transaction.
            // A Gather node reports its workers' scans with loops= equal
            // to the worker count, so a parallel plan would trip the
            // once-only rule on a query that reads the manifest exactly
            // once — a false positive that depends on the machine's core
            // count and would make this test flap rather than fail.
            h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
            // ANALYZE, so the assertion is on what the executor DID
            // (`loops=`) rather than on plan-node names, which change
            // between Postgres versions and say nothing about how many
            // times the manifest was read. And the BOUNDED statement,
            // which is what production runs.
            h.createQuery(
                "EXPLAIN (ANALYZE, TIMING false, COSTS false, SUMMARY false) " +
                    VerifyService.bounded(query.sql, query.orderBy),
            )
                .bind("c", catalogId)
                .bind("catalogId", catalogId)
                .bind("stagingMaxAge", VerifyService.DEFAULT_STAGING_TICKET_MAX_AGE_SECONDS)
                .mapTo(String::class.java)
                .list()
                .joinToString("\n")
        }

    private val loopCount = Regex("""loops=(\d+)""")

    @Test
    fun `no node of any path-equality plan runs more than once, and none is skipped`() {
        val plans = VerifyService.PATH_EQUALITY_QUERIES.mapValues { (_, q) -> plan(q) }
        for ((name, text) in plans) {
            // (1) Nothing anywhere ran twice. Stated over EVERY node
            // rather than over the scans alone, because a `Materialize`
            // between a Nested Loop and its inner scan carries the
            // `loops=` itself and leaves the scan line reading `loops=1`
            // — the exact shape a scan-only detector waves through.
            val repeated =
                text.lines().map { it.trim() }.filter { line ->
                    loopCount.find(line)?.groupValues?.get(1)?.toInt()?.let { it > 1 } == true
                }
            assertThat(repeated)
                .describedAs("%s executes a node per candidate:\n%s", name, text)
                .isEmpty()

            // (2) No Nested Loop may carry a manifest scan under it at
            // all. Belt and braces over (1): a nested loop whose outer
            // side happens to produce one row today passes the loops
            // check and becomes quadratic the moment the data moves.
            assertNoManifestScanUnderNestedLoop(name, text)

            // (3) The manifest really was read — `never executed` means
            // the driving scan found no candidates, and a query with no
            // candidates plans beautifully and proves nothing.
            val touched =
                text.lines().filter { line -> MANIFEST_TABLES.any { line.contains("Scan on $it") } }
            assertThat(touched)
                .describedAs("%s should touch a manifest table at all:\n%s", name, text)
                .isNotEmpty()
            assertThat(touched.filter { it.contains("never executed") })
                .describedAs("%s has no candidates, so its plan asserts nothing:\n%s", name, text)
                .isEmpty()
        }
    }

    /** Fails if any subtree under a `Nested Loop` scans a manifest table. */
    private fun assertNoManifestScanUnderNestedLoop(
        name: String,
        text: String,
    ) {
        val lines = text.lines()

        fun indent(line: String) = line.length - line.trimStart().length
        for ((i, line) in lines.withIndex()) {
            if (!line.contains("Nested Loop")) continue
            val depth = indent(line)
            for (j in i + 1 until lines.size) {
                if (lines[j].isBlank()) continue
                if (indent(lines[j]) <= depth) break
                val offender = MANIFEST_TABLES.firstOrNull { lines[j].contains("Scan on $it") }
                assertThat(offender)
                    .describedAs(
                        "%s puts %s inside a Nested Loop, which is quadratic the moment the " +
                            "outer side grows:\n%s",
                        name,
                        offender,
                        text,
                    )
                    .isNull()
            }
        }
    }

    @Test
    fun `the scan resolves on a 50k-file catalog and reports exactly the seeded violations`() {
        // Not a wall-clock budget — that is a property of the machine.
        // What is worth pinning is that twelve aggregate queries over a
        // manifest this size RESOLVE, and that the checks see exactly
        // the violations the fixture planted: the same rows that give
        // every plan above its candidates.
        val report = VerifyService(db.jdbi).runOnce("plan")
        assertThat(report.checks).hasSize(12)
        val failing = report.checks.filter { it.status != "pass" }.associate { it.check to it.violations }
        assertThat(failing)
            .containsExactlyInAnyOrderEntriesOf(
                mapOf(
                    "removal_queue" to REMOVAL_QUEUE_VIOLATIONS,
                    "staging_tickets" to STAGING_VIOLATIONS,
                    "upload_claims" to UPLOAD_VIOLATIONS,
                ),
            )
    }
}
