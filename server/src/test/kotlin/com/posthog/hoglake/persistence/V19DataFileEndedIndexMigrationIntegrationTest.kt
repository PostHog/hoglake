package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
import com.posthog.hoglake.service.ExpiryService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * V19 against the statement it exists for: `ExpiryService`'s data-file
 * DELETE, which before this migration matched no index's leading
 * columns at all.
 *
 * THE MIGRATION RUNS HERE — the database arrives at V17, the rows are
 * seeded, and then [Database.migrate] applies V19 — so the BEFORE half
 * is a measurement on the same rows rather than a claim about a file
 * that is not there. That is V16's and V17's shape, and AGENT.md's
 * rule.
 *
 * THE ENDED ROWS ARE SCATTERED, AND THAT IS THE WHOLE FIXTURE.
 * An earlier draft of this file inserted them as one contiguous block
 * and measured a 91x improvement, which was an artifact of the
 * INSERT ORDER: contiguous ended rows share heap pages, so the index
 * path fetched a handful of them. Production's ended rows are whatever
 * expiry and compaction happened to end, spread across the manifest —
 * so the index path costs roughly ONE HEAP BUFFER PER ENDED ROW, and
 * the win is `seq pages / ended rows` rather than a constant. Here they
 * are interleaved one in every [ENDED_EVERY] rows, which puts each on
 * its own page exactly as production does, and the budget is derived
 * from the ended count rather than written down.
 *
 * WHAT THAT MEANS FOR THE TRADE, stated because it is the honest
 * version: the benefit is inversely proportional to the ENDED FRACTION.
 * At gigahog-prod-us's standing shape (30-90k ended of ~5.0M rows,
 * 190,884 heap pages) it is 2-6x. At a high ended fraction — right
 * after a large retirement, before expiry catches up — it approaches a
 * wash, and can be marginally slower. It is still the right index,
 * because the steady state is the low fraction and because a sequential
 * scan's cost grows with the CATALOG while this one grows with the
 * work; but "91x" was never true of any production-shaped manifest.
 *
 * IT DOES NOT BOUND THE ROW WORK EITHER. The scan is not the dominant
 * cost of the sweep's DELETE: the RI triggers for the three cascading
 * children and the CTE's tuplestore are, and neither moves. This index
 * removes a term that grows with the manifest; it does not make the
 * statement cheap.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class V19DataFileEndedIndexMigrationIntegrationTest {
    private companion object {
        const val INDEX = "hog_data_file_ended"

        /**
         * The manifest. Mostly LIVE, as production is — 4.94M of
         * gigahog-prod-us's 5.0M rows are live — because that ratio is
         * the whole argument for the partial predicate: the index holds
         * only what expiry is looking for, and the hottest write in the
         * system never touches it.
         */
        const val TOTAL_FILES = 200_000

        /**
         * One row in every this many is ENDED. 1% is the production
         * shape (30-90k of ~5.0M is 0.6-1.8%) and, more importantly, it
         * is the fraction at which the index has something to win: at a
         * high fraction the index path reads as many heap buffers as
         * the sequential scan reads pages.
         */
        const val ENDED_EVERY = 100

        val ENDED_BELOW_FLOOR = TOTAL_FILES / ENDED_EVERY

        const val FLOOR = 100L

        /**
         * Heap buffers the index path may spend per ENDED row below the
         * floor. Scattered rows mean one heap fetch each; 1.2 covers
         * the index's own pages and leaves no room for a plan that
         * fetches more than the rows it needs.
         */
        const val BUFFERS_PER_ENDED_ROW = 1.2

        /**
         * The page the statement runs with, which is production's.
         *
         * The statement is PAGED since expiry became two-phase: a
         * `ctid` page of this many rows, ordered by `end_snapshot`, run
         * outside the commit lock. That changes what each half of this
         * file measures, and makes the after half's claim stronger.
         *
         * BEFORE V19 THE PAGE BOUNDS NOTHING. With no index on
         * `end_snapshot` the only way to find a page of ended rows is to
         * read the manifest, so the page bounds the rows DELETED and
         * leaves the rows EXAMINED at O(the catalog) — the distinction
         * AGENT.md's bound-every-fetch rule is about, and the reason the
         * index and the page are two fixes rather than one.
         *
         * AFTER V19 THE PAGE IS THE BOUND, and only because the
         * statement carries no `ORDER BY`. It used to: the page was
         * ordered by `end_snapshot` so the walk would take the oldest
         * corpses first, and the plan this file printed was a Bitmap
         * Heap Scan over the whole eligible set feeding a Sort feeding
         * the Limit — 2,000 heap blocks for a page of 1,000, i.e. the
         * examined rows still scaling with the backlog. The ordering is
         * gone; what this test now pins is that a page costs about [PAGE]
         * heap buffers no matter how many rows are eligible.
         */
        val PAGE = ExpiryService.PURGE_PAGE

        /** Put the history back to before V19 (V16's helper, and its reasoning). */
        const val REAPPLY_V19 = "DELETE FROM flyway_schema_history WHERE version::numeric >= 18"
    }

    // productionSession: V19 builds CONCURRENTLY with statement_timeout
    // lifted for the build and restored after, and a booting pod's
    // session carries the 60 s bound. The fixture reaches that
    // behaviour rather than running with no bounds at all.
    private val db = PgTestSupport.freshDatabaseAt("17", productionSession = true)
    private var catalogId = 0L
    private var otherCatalogId = 0L

    private lateinit var expiryPlanBefore: String
    private lateinit var expiryPlanAfter: String
    private var buildMillis = 0L

    @AfterAll
    fun tearDown() = db.close()

    @BeforeAll
    fun seedThenMigrate() {
        catalogId = createCatalog("v18")
        otherCatalogId = createCatalog("v18-neighbour")
        seedManifest()
        analyze()

        assertThat(indexDef(INDEX)).describedAs("%s absent before V19", INDEX).isNull()
        expiryPlanBefore = explainExpiry()

        val start = System.nanoTime()
        Database.migrate(db.dataSource)
        buildMillis = (System.nanoTime() - start) / 1_000_000

        // VACUUM and not merely ANALYZE: a bulk insert leaves the
        // visibility map unset, so an Index Only Scan would pay a heap
        // fetch per row and the budget below would be measuring the
        // absence of autovacuum rather than the plan.
        analyze()
        expiryPlanAfter = explainExpiry()
    }

    private fun createCatalog(name: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "INSERT INTO hog_catalog (name, data_path, earliest_snapshot_id, last_snapshot_id) " +
                    "VALUES (:n, 's3://v18/' || :n, :f, 1000) RETURNING catalog_id",
            ).bind("n", name).bind("f", FLOOR).mapTo(Long::class.java).one()
        }

    /**
     * ONE generate_series, so the ended rows are physically INTERLEAVED
     * with the live ones — the state expiry and compaction actually
     * leave behind, and the one that makes an index probe cost a heap
     * fetch. Two catalogs alternate for the same reason V17's fixture
     * does: `catalog_id` leads the index, and with one catalog "this
     * catalog's rows" and "every row" are the same rows.
     */
    private fun seedManifest() =
        db.jdbi.useHandleUnchecked { h ->
            for (c in listOf(catalogId, otherCatalogId)) {
                h.execute(
                    "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) " +
                        "SELECT ?, g, 0 FROM generate_series(1, 4) g",
                    c,
                )
            }
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           end_snapshot, path, record_count, file_size_bytes,
                                           row_id_start)
                SELECT CASE WHEN g % 10 = 0 THEN :other ELSE :c END, g, 1 + (g % 4), 1,
                       CASE
                           -- below the floor: what the DELETE takes.
                           -- The residue is 7, NOT 0: `g % 10 = 0`
                           -- above sends a row to the OTHER catalog,
                           -- and `g % 100 = 0` implies it — so a
                           -- residue of 0 would have put every ended
                           -- row in the neighbour and left this
                           -- catalog's count at zero. It did, until
                           -- the assertion on the count caught it.
                           WHEN g % :every = 7 THEN 2
                           -- above the floor: rows the predicate must
                           -- exclude, so the index is doing a RANGE and
                           -- not merely "is it ended"
                           WHEN g % :every = 57 THEN :above
                           ELSE NULL
                       END,
                       's3://v18/' || md5(g::text) || '/part-' || g || '.parquet',
                       120000, 268435456, g::bigint * 120000
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("other", otherCatalogId)
                .bind("every", ENDED_EVERY).bind("above", FLOOR + 500)
                .bind("n", TOTAL_FILES).execute()
        }

    private fun analyze() =
        db.jdbi.useHandleUnchecked { h ->
            for (relation in listOf("hog_data_file", "hog_file_removal")) {
                h.execute("VACUUM (ANALYZE) $relation")
            }
        }

    // ---- EXPLAIN -----------------------------------------------------------

    /**
     * Serial plans only. A Gather reports its workers' scans with
     * `loops=` equal to the worker count and splits the buffer counts
     * across them, which would make every number below depend on the
     * machine's core count.
     *
     * The statement MUTATES, so the EXPLAIN ANALYZE runs inside a
     * transaction that is rolled back: the plan is measured, the rows
     * stay.
     */
    private fun <T> explain(block: (Handle) -> T): T =
        db.jdbi.withHandleUnchecked { h ->
            h.begin()
            try {
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                block(h)
            } finally {
                h.rollback()
            }
        }

    private fun explainExpiry(): String =
        explain { h ->
            h.createQuery(
                "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " +
                    ExpiryService.DATA_FILE_EXPIRY_SQL,
            )
                .bind("catalogId", catalogId)
                .bind("newEarliest", FLOOR)
                // The PRODUCTION page, not the whole eligible set: the
                // statement is paged now (#260's successor), and a plan
                // measured at an unbounded page would be a plan nothing
                // runs.
                .bind("page", PAGE)
                .mapTo(String::class.java).list().joinToString("\n")
        }

    private data class ScanNode(
        val line: String,
        val index: String?,
        val buffers: Long,
        val rowsRemovedByFilter: Long?,
        val indexSearches: Long?,
        val seqScan: Boolean,
    )

    /**
     * The node that does the READING, which since the statement became
     * paged is not the first node naming the relation.
     *
     * A `ctid` page is two nodes over `hog_data_file`: an InitPlan that
     * finds the page (the seq scan before V19, the index scan after)
     * and a `Tid Scan` that fetches the named tuples for the DELETE.
     * The Tid Scan is a lookup of a bounded array and has nothing to
     * say about the access path, so every assertion in this file is
     * about the other one — and taking "the first node" would quietly
     * measure the wrong half.
     */
    private fun scanNode(
        text: String,
        relation: String,
    ): ScanNode {
        val lines = text.lines()

        fun indent(l: String) = l.length - l.trimStart().length
        val i =
            lines.indexOfFirst {
                Regex("""Scan.* on $relation\b""").containsMatchIn(it) &&
                    !it.contains("Tid Scan")
            }
        if (i < 0) throw AssertionError("no scan of $relation in:\n$text")
        val line = lines[i]
        val detail = lines.drop(i + 1).takeWhile { it.isNotBlank() && indent(it) > indent(line) }
        val buffersLine = detail.firstOrNull { it.trim().startsWith("Buffers:") }
        val hit = buffersLine?.let { Regex("""\bhit=(\d+)""").find(it)?.groupValues?.get(1)?.toLong() } ?: 0L
        val read = buffersLine?.let { Regex("""\bread=(\d+)""").find(it)?.groupValues?.get(1)?.toLong() } ?: 0L

        fun detailLong(prefix: String): Long? =
            detail.firstOrNull { it.trim().startsWith(prefix) }
                ?.let { Regex("""(\d+)""").find(it.substringAfter(prefix))?.value?.toLong() }
        // THE BITMAP CHILD'S BUFFERS COUNT TOO. A Bitmap Index Scan
        // builds the WHOLE bitmap before its parent's heap scan starts,
        // so the index half is O(the eligible set) — and reading buffers
        // off the Bitmap Heap Scan parent alone would exclude exactly the
        // term a per-page budget is supposed to bound. The parent's
        // `Buffers:` line in `EXPLAIN (BUFFERS)` does NOT include the
        // child's, so they are summed here.
        val childBuffers =
            detail.filter { it.contains("Bitmap Index Scan") }
                .mapNotNull { child ->
                    val i2 = lines.indexOf(child)
                    lines.drop(i2 + 1).takeWhile { indent(it) > indent(child) }
                        .firstOrNull { it.trim().startsWith("Buffers:") }
                }
                .sumOf { b ->
                    (Regex("""\bhit=(\d+)""").find(b)?.groupValues?.get(1)?.toLong() ?: 0L) +
                        (Regex("""\bread=(\d+)""").find(b)?.groupValues?.get(1)?.toLong() ?: 0L)
                }
        return ScanNode(
            line = line.trim(),
            // Two spellings, because the planner has two shapes for the
            // same index. An ordered Index Scan names it on its own
            // line; a BITMAP scan names it on the child Bitmap Index
            // Scan line, and the parent Bitmap Heap Scan — the node
            // whose buffers are the heap cost — names only the
            // relation. Reading only the first form made this file
            // report "no index" for a plan that was using exactly the
            // index it was built to prove.
            index =
                Regex("""Scan(?: Backward)? using (\S+) on""").find(line)?.groupValues?.get(1)
                    ?: detail.firstNotNullOfOrNull {
                        Regex("""Bitmap Index Scan on (\S+)""").find(it)?.groupValues?.get(1)
                    },
            buffers = hit + read + childBuffers,
            rowsRemovedByFilter = detailLong("Rows Removed by Filter:"),
            indexSearches = detailLong("Index Searches:"),
            seqScan = line.contains("Seq Scan on $relation"),
        )
    }

    private fun indexDef(name: String): String? =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT pg_get_indexdef(oid) FROM pg_class WHERE relname = :n")
                .bind("n", name).mapTo(String::class.java).findOne().orElse(null)
        }

    private fun heapPages(relation: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery("SELECT relpages FROM pg_class WHERE relname = :n")
                .bind("n", relation).mapTo(Long::class.java).one()
        }

    /** Ended rows of THIS catalog below the floor — the work, counted. */
    private fun endedBelowFloor(): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT count(*) FROM hog_data_file WHERE catalog_id = :c " +
                    "AND end_snapshot IS NOT NULL AND end_snapshot <= :f",
            ).bind("c", catalogId).bind("f", FLOOR).mapTo(Long::class.java).one()
        }

    // ---- the index, proved against the statement it serves ------------------

    @Test
    fun `the index is the partial shape the migration claims, by catalog rather than by name`() {
        // pg_index, not a substring of pg_get_indexdef: a
        // `contains("catalog_id", "end_snapshot")` over the definition
        // text is satisfied by the index's own NAME (V16's lesson).
        val (keys, predicate) =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    """
                    SELECT (SELECT string_agg(a.attname, ',' ORDER BY k.ord)
                              FROM unnest(i.indkey) WITH ORDINALITY AS k(attnum, ord)
                              JOIN pg_attribute a
                                ON a.attrelid = i.indrelid AND a.attnum = k.attnum) AS keys,
                           pg_get_expr(i.indpred, i.indrelid) AS predicate
                    FROM pg_index i
                    JOIN pg_class c ON c.oid = i.indexrelid
                    WHERE c.relname = :n
                    """,
                ).bind("n", INDEX)
                    .map { rs, _ -> rs.getString("keys") to rs.getString("predicate") }
                    .one()
            }
        assertThat(keys).isEqualTo("catalog_id,end_snapshot")
        assertThat(predicate)
            .describedAs("PARTIAL on the complement of hog_data_file_live, so an append pays nothing")
            .isNotNull()
            .satisfies({ assertThat(it).contains("end_snapshot IS NOT NULL") })

        // V19 TOUCHES NOTHING ELSE, and that is what lets it ship on
        // its own: an operator promoting this file takes no ACCESS
        // EXCLUSIVE lock on any table, so it needs no window in which
        // no expiry sweep is running. The two ALTERs #193 also needs
        // live in V19.
        //
        // Asserted against the FILE rather than against the database,
        // deliberately: `Database.migrate` applies the whole chain, so
        // by the time a schema query runs here V19 has been applied
        // too and the absence would be unobservable. The claim is
        // about what THIS migration contains.
        val v18 =
            java.nio.file.Files.readString(
                java.nio.file.Path.of("src/main/resources/db/migration/V19__data_file_ended_index.sql"),
            )
        val statements =
            v18.lines().map { it.substringBefore("--") }.filter { it.isNotBlank() }.joinToString("\n")
        assertThat(statements.uppercase())
            .describedAs("V19 must take no ACCESS EXCLUSIVE lock; its ALTERs belong to V20:%n%s", statements)
            .doesNotContain("ALTER TABLE")
        assertThat(statements)
            .describedAs("and it must actually build the index")
            .contains("CREATE INDEX CONCURRENTLY IF NOT EXISTS $INDEX")
    }

    @Test
    fun `before V19 a page of expiry's data-file DELETE reads ENDED_EVERY live rows per row it deletes`() {
        // The red half, measured on the same rows the after half runs
        // on. THE ASSERTION IS THE WORK, NOT THE PLAN NODE, and the work
        // is the point the page bound alone cannot make: without an
        // index on `end_snapshot` the scan has to walk the manifest
        // until it has found a page's worth of ended rows, so the rows
        // EXAMINED are the page times the inverse ended fraction —
        // ENDED_EVERY - 1 live rows discarded per row deleted. At
        // production's 0.6-1.8% that is 55-165 live rows per deleted
        // row, over a 1.5 GiB relation, and it is why the page and the
        // index are two fixes rather than one.
        val before = scanNode(expiryPlanBefore, "hog_data_file")
        assertThat(before.seqScan)
            .describedAs("V17's indexes cannot serve `end_snapshot`:%n%s", expiryPlanBefore)
            .isTrue()
        assertThat(before.rowsRemovedByFilter)
            .describedAs(
                "a page of %d must discard ~%d live rows to fill itself:%n%s",
                PAGE,
                PAGE.toLong() * (ENDED_EVERY - 1),
                expiryPlanBefore,
            )
            .isNotNull()
            .satisfies({ assertThat(it).isGreaterThan(PAGE.toLong() * (ENDED_EVERY - 1) * 8 / 10) })
        assertThat(before.buffers)
            .describedAs(
                "and pay the heap for them — a page reads about %d of the manifest's %d pages:%n%s",
                heapPages("hog_data_file") * PAGE / ENDED_BELOW_FLOOR,
                heapPages("hog_data_file"),
                expiryPlanBefore,
            )
            .isGreaterThanOrEqualTo(heapPages("hog_data_file") / 4)
    }

    @Test
    fun `after V19 the work sizes with the ENDED rows, at about one heap buffer each`() {
        val after = scanNode(expiryPlanAfter, "hog_data_file")
        assertThat(after.index)
            .describedAs("the DELETE must be driven by V19's index:%n%s", expiryPlanAfter)
            .isEqualTo(INDEX)
        assertThat(after.seqScan).isFalse()
        // ONE descent: `catalog_id` is an equality and `end_snapshot` a
        // range, so the whole predicate is an index condition.
        assertThat(after.indexSearches)
            .describedAs("one descent, not one per row:%n%s", expiryPlanAfter)
            .isEqualTo(1)
        // And NOTHING is demoted to a Filter. This is the assertion
        // V16's file says separates a real probe from an index the
        // planner picked for its leading column alone.
        assertThat(after.rowsRemovedByFilter)
            .describedAs("no part of the predicate may be a post-scan filter:%n%s", expiryPlanAfter)
            .isNull()

        // THE BUDGET IS ONE HEAP BUFFER PER ROW IN THE PAGE, not per
        // ENDED ROW, and that difference is the property paging bought.
        // Scattered rows cost a heap fetch each — the honest cost of the
        // only population production has — but the scan now stops at
        // the page, so the figure does NOT move when the eligible set
        // does. The fixture holds twice the page in eligible rows for
        // exactly this reason: if the plan still read the whole set,
        // this assertion would fail at 2x rather than pass quietly.
        val ended = endedBelowFloor()
        val before = scanNode(expiryPlanBefore, "hog_data_file")
        assertThat(ended)
            .describedAs("the fixture must hold MORE than one page of eligible rows")
            .isBetween(ENDED_BELOW_FLOOR * 8L / 10, ENDED_BELOW_FLOOR * 12L / 10)
            .satisfies({ assertThat(it).isGreaterThan(PAGE.toLong()) })
        assertThat(after.buffers.toDouble())
            .describedAs(
                "a page costs about its own size in heap buffers whatever is eligible: " +
                    "%d buffers for a page of %d, with %d rows eligible (%d before, over a " +
                    "%d-page manifest):%n%s",
                after.buffers,
                PAGE,
                ended,
                before.buffers,
                heapPages("hog_data_file"),
                expiryPlanAfter,
            )
            .isLessThanOrEqualTo(PAGE * BUFFERS_PER_ENDED_ROW)
        // It must still WIN at a production-shaped ended fraction. The
        // ratio is `seq pages / ended rows`, so this assertion is the
        // one that goes soft as the fraction rises — deliberately, and
        // the class KDoc says so.
        assertThat(after.buffers)
            .describedAs("at a 1%% ended fraction the index must beat the scan:%n%s", expiryPlanAfter)
            .isLessThan(before.buffers)
        println(
            "[#193] V19 (SCATTERED ended rows, the production shape): expiry's data-file DELETE " +
                "scan node went from ${before.buffers} buffers (Seq Scan over a " +
                "${heapPages("hog_data_file")}-page manifest, ${before.rowsRemovedByFilter} rows " +
                "removed by filter) to ${after.buffers} (${after.line.substringBefore(" (actual")}, " +
                "Index Searches ${after.indexSearches}) for a page of $PAGE with $ended eligible — " +
                "${"%.1f".format(before.buffers.toDouble() / after.buffers)}x at an " +
                "${"%.1f".format(100.0 * ended / TOTAL_FILES)}%% ended fraction",
        )
    }

    @Test
    fun `the index build is measured, with its cache state named`() {
        // Re-run the build on the SAME rows, warm, and time it. The
        // migration's own build above ran on a cold-ish cache right
        // after the bulk insert; this is the warm number, and the file
        // states both.
        val warmMillis =
            db.jdbi.withHandleUnchecked { h ->
                h.execute("DROP INDEX $INDEX")
                h.execute("SET statement_timeout = 0")
                val start = System.nanoTime()
                h.execute(
                    "CREATE INDEX CONCURRENTLY $INDEX ON hog_data_file (catalog_id, end_snapshot) " +
                        "WHERE end_snapshot IS NOT NULL",
                )
                (System.nanoTime() - start) / 1_000_000
            }
        // The PLAIN build, for the trade V17's rule says is measured
        // per migration rather than inherited by analogy.
        val plainMillis =
            db.jdbi.withHandleUnchecked { h ->
                h.execute("DROP INDEX $INDEX")
                val start = System.nanoTime()
                h.execute(
                    "CREATE INDEX $INDEX ON hog_data_file (catalog_id, end_snapshot) " +
                        "WHERE end_snapshot IS NOT NULL",
                )
                (System.nanoTime() - start) / 1_000_000
            }
        val sizes =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT pg_relation_size(:i) AS idx, pg_relation_size('hog_data_file') AS heap",
                ).bind("i", INDEX)
                    .map { rs, _ -> rs.getLong("idx") to rs.getLong("heap") }
                    .one()
            }
        val endedRows =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery("SELECT count(*) FROM hog_data_file WHERE end_snapshot IS NOT NULL")
                    .mapTo(Long::class.java).one()
            }
        println(
            "[#193] V19 build over $TOTAL_FILES rows ($endedRows of them ended): whole migration " +
                "${buildMillis}ms cold, CREATE INDEX CONCURRENTLY ${warmMillis}ms warm, plain " +
                "${plainMillis}ms warm; index ${sizes.first} bytes " +
                "(${sizes.first / endedRows} B per ENDED row) against a ${sizes.second}-byte heap",
        )
        // The claim the migration header makes about COST: the index
        // holds only ended rows, so it is a rounding error against the
        // heap even though most of the table is live.
        assertThat(sizes.first)
            .describedAs("a partial index over %d ended rows must not size with the heap", endedRows)
            .isLessThan(sizes.second / 10)
    }

    @Test
    fun `V19 is re-appliable, which executeInTransaction=false makes a requirement`() {
        // A file Flyway runs outside a transaction leaves a
        // `success = false` history row on a partial failure, which
        // fails validate on every replica until an operator runs
        // `flyway repair`. Re-running has to be free, so every
        // statement is idempotent — and this asserts it by running the
        // whole file again over a schema that already has all of it.
        db.jdbi.useHandleUnchecked { h -> h.execute(REAPPLY_V19) }
        Database.migrate(db.dataSource)
        assertThat(indexDef(INDEX)).isNotNull()
    }

    @Test
    fun `findAt's plan is unchanged by V19`() {
        // The index is on hog_data_file and the resolver reads
        // hog_table_version joined to hog_table, so there is nothing for
        // V19 to steal — but "nothing to steal" is a claim, and V16's
        // file is about an index that stole a query it served worse.
        val plan =
            db.jdbi.inTransactionUnchecked { h ->
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                h.createQuery(
                    "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " +
                        "SELECT t.table_id, t.table_uuid, tv.namespace_id, tv.name, tv.comment, tv.properties " +
                        "FROM hog_table_version tv JOIN hog_table t " +
                        "ON t.catalog_id = tv.catalog_id AND t.table_id = tv.table_id " +
                        "WHERE tv.catalog_id = :c AND tv.namespace_id = 1 AND tv.name = 'nope' " +
                        "AND tv.begin_snapshot <= :s AND (tv.end_snapshot IS NULL OR :s < tv.end_snapshot)",
                )
                    .bind("c", catalogId).bind("s", 5L)
                    .mapTo(String::class.java).list().joinToString("\n")
            }
        assertThat(plan)
            .describedAs("the resolver must not touch hog_data_file at all:%n%s", plan)
            .doesNotContain("hog_data_file")
        assertThat(plan)
            .describedAs("and it must stay index-driven:%n%s", plan)
            .doesNotContain("Seq Scan on hog_table_version")
    }
}
