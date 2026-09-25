package com.posthog.hoglake.service

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.testing.ExplainPlan
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
 * These checks are AGGREGATES over whole relations — every undrained
 * queue row against every file row — so the index that serves a
 * thousand-path probe does not serve them: `hog_data_file_path` /
 * `hog_delete_file_path` (V17) and `hog_file_removal_undrained_path`
 * (V16, undrained rows only) each answer one key at a time, and these
 * queries have no small key set to answer. What they need is a single
 * scan of each relation, hashed, joined against the candidate set. It
 * is a catastrophe if any node runs per candidate, which is what a
 * small dev catalog will always look fast doing — and V17 makes that
 * MORE reachable, not less, because a per-candidate index probe is now
 * a plan the planner can pick.
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
 * migration. The `(catalog_id, path)` indexes V17 added exist for the
 * cleanup drain's per-sub-batch probe under the commit lock, and they
 * are paid for on every commit — nothing here may ask for another, and
 * a nested loop that drives one of them per candidate is still the
 * quadratic plan this test exists to exclude.
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

        /** The relations these checks read whole; a RESCAN of one is the bug. */
        val MANIFEST_TABLES = listOf("hog_data_file", "hog_delete_file", "hog_file_removal")

        /**
         * Buffers one candidate's index probe may cost: a B-tree
         * descent, with slack. Measured at 4.0 per loop for the
         * staging-ticket anti-join's inner probe of
         * `hog_data_file_path`.
         */
        const val MAX_BUFFERS_PER_PROBE = 8L

        /** Per-node overhead the budget forgives. */
        const val BUFFER_FLOOR = 64L

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
        // The loops bound (rule 2) says a node may repeat at most once
        // per CANDIDATE. That only EXCLUDES anything while the candidate
        // set is smaller than the manifest: raise LEDGER_ROWS or
        // UPLOAD_CLAIMS past DATA_FILES and a node repeating once per
        // manifest row would satisfy it, the rule would go quiet, and
        // nothing in this class would fail. Asserted here rather than
        // assumed, because it is a property of the FIXTURE and the
        // fixture is edited by people who are not thinking about rule 2.
        val manifestRows =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT count(*) FROM hog_data_file WHERE catalog_id = :c")
                    .bind("c", catalogId).mapTo(Long::class.java).one()
            }
        check(candidateBound() < manifestRows) {
            "the candidate bound (${candidateBound()}) must stay below the manifest " +
                "($manifestRows rows) or the loops-per-candidate rule excludes nothing"
        }
    }

    /**
     * The most candidates any of these checks can drive a loop with:
     * every removal-ledger row and every upload claim this catalog
     * owns. Counted, not written down, so the bound moves with the
     * fixture.
     */
    private fun candidateBound(): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                """
                SELECT (SELECT count(*) FROM hog_file_removal WHERE catalog_id = :c)
                     + (SELECT count(*) FROM hog_upload WHERE catalog_id = :c)
                """,
            ).bind("c", catalogId).mapTo(Long::class.java).one()
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
            // BUFFERS EXPLICITLY. PG 18 turns it on with ANALYZE and 16
            // and 17 do not, and the suite runs against either
            // (`-PpgImage=postgres:16`, PgTestSupport). Without it every
            // buffer number below reads zero and the budget that bounds
            // a per-candidate probe passes on an empty string.
            h.createQuery(
                "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " +
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
    fun `no node of any path-equality plan re-reads a relation per candidate, and none is skipped`() {
        val plans = VerifyService.PATH_EQUALITY_QUERIES.mapValues { (_, q) -> plan(q) }
        for ((name, text) in plans) {
            // (1) A node MAY run per candidate now, and only one shape
            // of it may: an INDEX PROBE.
            //
            // This rule was "nothing anywhere ran twice" and it had to
            // change, because V17 changed what a repeated node costs.
            // With no `(catalog_id, path)` index, the only way to
            // execute a node per candidate was to re-read the relation
            // — quadratic, and invisible on a small catalog, which is
            // why the rule was absolute. With the index,
            // `staging_tickets.registered_nothing` takes a Nested Loop
            // Left Join whose inner side is one B-tree descent per
            // candidate (measured here: 1,069 loops, 4,273 buffers, 4.0
            // per loop), and that is BOUNDED BY THE CANDIDATES rather
            // than by the manifest — on a 5M-row manifest it beats the
            // hash join it replaced until the candidate set reaches tens
            // of thousands.
            //
            // So the rule is now the cost, not the count: a repeated
            // node must be an index probe AND must cost a descent per
            // loop. A repeated SCAN, or an index probe that reads a page
            // range per loop, still fails — and so does anything else
            // that repeats.
            for (node in nodes(text).filter { it.loops > 1 }) {
                assertThat(node.line)
                    .describedAs(
                        "%s runs a node %d times and it is not an index probe; only a B-tree " +
                            "descent may repeat per candidate.\n%s",
                        name,
                        node.loops,
                        text,
                    )
                    .matches(Regex(""".*Index (Only )?Scan using \S+ on \S+.*""").toPattern())
                // THE TOTAL IS BOUNDED BY THE CANDIDATE SET, not by a
                // rate alone. A per-loop budget on its own accepts an
                // unbounded number of loops, which is the quadratic
                // plan wearing a per-loop disguise; the two assertions
                // below multiply to `buffers <= candidates x descent`,
                // which is the bound that transfers to any catalog:
                //
                //  (a) a node may repeat at most once per CANDIDATE,
                //      and every candidate set these checks form is
                //      drawn from the removal ledger and the upload
                //      claims. A node that repeats per MANIFEST row
                //      fails here even if each loop is cheap.
                //  (b) each loop costs a B-tree DESCENT.
                //
                // Why not "the total must beat the single scan it
                // replaced": measured, that rejects the plan production
                // wants. On this fixture the candidate set (1,069) is
                // the same order as the relation's page count (910), so
                // 1,069 descents cost more than one scan — while on
                // gigahog-prod-us the same check has ~16k candidates
                // against a 190,884-page manifest, where the nested
                // loop wins by more than an order of magnitude and the
                // hash it replaced also had to build over 5M rows.
                // Pinning the fixture's ratio would pin the fixture.
                assertThat(node.loops)
                    .describedAs(
                        "%s repeats `%s` %d times; the candidate sets here are drawn from the " +
                            "removal ledger and the upload claims, which hold %d rows for this " +
                            "catalog. More loops than that means the node repeats per manifest " +
                            "row.\n%s",
                        name,
                        node.line.trim(),
                        node.loops,
                        candidateBound(),
                        text,
                    )
                    .isLessThanOrEqualTo(candidateBound())
                assertThat(node.buffers)
                    .describedAs(
                        "%s ran `%s` %d times for %d buffers (%.1f per loop); a B-tree descent " +
                            "is a handful, and more than that is a page RANGE read per " +
                            "candidate.\n%s",
                        name,
                        node.line.trim(),
                        node.loops,
                        node.buffers,
                        node.buffers.toDouble() / node.loops,
                        text,
                    )
                    .isLessThanOrEqualTo(node.loops * MAX_BUFFERS_PER_PROBE + BUFFER_FLOOR)
                // And it must be a probe on the KEY, not a probe on a
                // prefix with `path` demoted. `Index Scan using
                // hog_data_file_changefeed ... Filter: (path = ...)`
                // repeats per candidate AND reads a table_id's whole
                // range each time — V16's lesson, and the one shape the
                // buffer budget alone would let through on a fixture
                // small enough that a range is cheap.
                assertThat(node.filtersPath)
                    .describedAs(
                        "%s repeats `%s` with `path` demoted to a Filter; the probe must be an " +
                            "INDEX CONDITION or it reads a range per candidate.\n%s",
                        name,
                        node.line.trim(),
                        text,
                    )
                    .isFalse()
            }

            // (2) No Nested Loop may carry a manifest SCAN under it —
            // a sequential or bitmap read of the relation, per
            // candidate. Belt and braces over (1): a nested loop whose
            // outer side happens to produce one row today passes the
            // loops check and becomes quadratic the moment the data
            // moves.
            assertNoManifestScanUnderNestedLoop(name, text)

            // (3) The manifest really was read — `never executed` means
            // the driving scan found no candidates, and a query with no
            // candidates plans beautifully and proves nothing.
            val touched =
                text.lines().filter { line -> MANIFEST_TABLES.any { line.contains(" on $it") } }
            assertThat(touched)
                .describedAs("%s should touch a manifest table at all:\n%s", name, text)
                .isNotEmpty()
            assertThat(touched.filter { it.contains("never executed") })
                .describedAs("%s has no candidates, so its plan asserts nothing:\n%s", name, text)
                .isEmpty()
        }
    }

    /**
     * The nodes of a plan, through the shared parser. `ExplainPlanTest`
     * pins its polarity without a container — in particular
     * [ExplainPlan.Node.filtersPath], which no plan in this class
     * carries and which a parser that always answered `false` would
     * leave green forever.
     */
    private fun nodes(text: String) = ExplainPlan.nodes(text)

    /**
     * Fails if the INNER side of a `Nested Loop` SCANS a manifest table
     * — `Seq Scan on ...` or `Bitmap Heap Scan on ...`, a read of the
     * whole relation per outer row.
     *
     * The inner side and not the whole subtree, which is what this
     * asserted before V17 and could, because these plans held no nested
     * loops at all. They do now: the staging-ticket anti-join drives
     * `hog_data_file_path` per candidate, and the hash anti-join that
     * FEEDS it — two sequential scans of `hog_file_removal`, once each —
     * sits on the loop's OUTER side, where a scan is exactly as cheap as
     * it was. `loops=` alone would pass both today and is the rule
     * above; this one is the belt-and-braces, because an inner scan
     * whose outer side happens to produce one row today reads `loops=1`
     * and becomes quadratic the moment the data moves.
     *
     * `Index Scan using <index> on ...` does not match the pattern and
     * is deliberately allowed on either side: since V17 a
     * `(catalog_id, path)` descent per candidate is a bounded plan.
     */
    private fun assertNoManifestScanUnderNestedLoop(
        name: String,
        text: String,
    ) {
        val lines = text.lines()

        fun indent(line: String) = line.length - line.trimStart().length

        fun scanOf(line: String): String? =
            MANIFEST_TABLES.firstOrNull {
                line.contains("Seq Scan on $it") || line.contains("Bitmap Heap Scan on $it")
            }
        for ((i, line) in lines.withIndex()) {
            if (!line.contains("Nested Loop")) continue
            val depth = indent(line)
            // Children sit one level in, in execution order: outer
            // first, inner second. Anything deeper belongs to a child.
            val children =
                (i + 1 until lines.size)
                    .asSequence()
                    .takeWhile { lines[it].isBlank() || indent(lines[it]) > depth }
                    .filter { lines[it].trimStart().startsWith("->") && indent(lines[it]) == depth + 6 }
                    .toList()
            // BOTH children or the parse is wrong, and a parse that
            // silently finds none waves every plan through. A Nested
            // Loop always has two.
            assertThat(children)
                .describedAs(
                    "could not read both children of the Nested Loop at line %d; the rule below " +
                        "asserts nothing without them:\n%s",
                    i + 1,
                    text,
                )
                .hasSize(2)
            val inner = children[1]
            val innerEnd =
                (inner + 1 until lines.size)
                    .firstOrNull { lines[it].isNotBlank() && indent(lines[it]) <= indent(lines[inner]) }
                    ?: lines.size
            for (j in inner until innerEnd) {
                val offender = scanOf(lines[j])
                assertThat(offender)
                    .describedAs(
                        "%s SCANS %s on the INNER side of a Nested Loop, which reads the " +
                            "relation once per outer row — quadratic the moment the outer side " +
                            "grows (an index probe there would be fine):\n%s",
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
