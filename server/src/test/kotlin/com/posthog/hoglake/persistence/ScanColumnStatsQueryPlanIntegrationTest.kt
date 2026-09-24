package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.concurrent.CopyOnWriteArrayList

/**
 * The plan behind [FileRepo.providedColumnStatsAt] — the scan plan's
 * `include=column_stats` statement — on a stats relation the size of a
 * real one, in all three shapes the API can ask for.
 *
 * ## Why `hog_file_column_stats` has no index but its primary key
 *
 * `field_id` is the THIRD column of the key
 * `(catalog_id, data_file_id, field_id)`, so the `stats_fields`
 * narrowing reaches it only once a data file id is known; without
 * that, the narrowing is a Filter over a sequential scan and the
 * payload shrinks while the work does not. The obvious repair is an
 * index leading on `field_id`. Four candidates were built and
 * measured. None of them is here.
 *
 * MEASUREMENT RIG, and it is NOT this test's fixture: a standalone
 * Postgres 18 container, one catalog of **6 tables x 20,000 files x 30
 * columns** — 3.6M stats rows, 44,445 heap pages, 17,229 primary-key
 * pages — heap written file by file and table ids interleaved, serial
 * plans, `EXPLAIN (ANALYZE, BUFFERS)` of the statement itself, one
 * table planned, warm. (This test builds THREE tables at the same
 * per-table shape — see [TABLES] — which is enough for the plan
 * properties it asserts and half the seed time. The numbers below are
 * the rig's, and the two are not interchangeable.)
 *
 * | candidate | stats-relation access | buffers | ms |
 * |---|---|---|---|
 * | none (primary key only) | Seq Scan | 46,650 | 156 |
 * | `(catalog_id, field_id)` | Bitmap, `Heap Blocks: exact=44445` | 44,937 | 125 |
 * | `(catalog_id, field_id, data_file_id)` | Bitmap, same heap blocks | 46,909 | 125 |
 * | BRIN `(catalog_id, field_id)` | Bitmap, `Heap Blocks: lossy=44445` | 46,671 | 236 |
 *
 * Read that as IO-NEUTRAL rather than as "the index does nothing": the
 * two-column B-tree finds the rows in about a hundred index buffers and
 * reads the same 44,445 heap blocks the sequential scan reads, saving
 * the per-row filter and about half the wall clock on the narrowed
 * shape. What it does not do is change the ORDER of the work, and it
 * charges 2,870 index pages of write amplification on the hottest
 * insert in the system, on the largest relation in the catalog, for
 * that. The three-column variant is strictly worse — 17,849 index
 * pages against 3,102, same heap blocks, because the trailing column
 * never enters an index condition (the join is a hash join) and
 * defeats the B-tree's deduplication of the few distinct field ids a
 * table has. BRIN is worse than having no index at all: correlation is
 * with `data_file_id`, not `field_id`, so every range matches and the
 * lossy bitmap adds a recheck over all 3.6M rows.
 *
 * The reason is the PHYSICAL ORDER. A commit writes one file's whole
 * stats set at once, so the heap is clustered by `data_file_id` and one
 * COLUMN's rows are one-per-file, scattered across every page. The
 * bounds live in the heap and cannot ride the index: `INCLUDE
 * (lower_bound, upper_bound)` would turn a long string bound into an
 * "index row size exceeds maximum" failure on the COMMIT that ships it.
 *
 * ## The alternative that does work, and why it is not taken
 *
 * `CLUSTER hog_file_column_stats USING (catalog_id, field_id,
 * data_file_id)` changes the answer, because it changes the order the
 * index is fighting: **4,282 buffers, 17.1 ms**, `Heap Blocks:
 * exact=1483`, against 46,650 and 156 ms. It is rejected on operational
 * grounds, not on merit. CLUSTER takes ACCESS EXCLUSIVE and rewrites
 * the whole relation, it is not a thing the migration chain can express
 * (the order decays back linearly as commits append), and keeping it
 * would mean a recurring maintenance task over the largest table in
 * every catalog. If this read ever becomes hot enough to pay for that,
 * this is the lever — not another B-tree.
 *
 * That whole comparison was nearly missed. The first rig seeded with
 * `generate_series(files), generate_series(columns)`, which Postgres
 * ran columns-outermost, clustering the heap by `field_id` — the one
 * order production never produces, and accidentally the CLUSTER
 * result. Under it the two-column index looked like a 20x win (1,975
 * buffers, 13 ms). An index measured against a heap order the system
 * does not create proves nothing, so both the rig and this test's
 * fixture write their rows file by file, as a commit does.
 *
 * ## What is asserted
 *
 * Not the plan the planner picks — that is its business, and it
 * legitimately differs by table size: the measured crossover is about
 * `relpages / 4.5` files, below which one table's slice is small enough
 * that a primary-key nested loop wins (a 5,000-file table on the rig
 * takes one at 6.4 ms; a 20,000-file table takes a hash join over a
 * sequential scan). What is asserted is the WORK, in all three shapes:
 *
 *  1. the relation is read at most ONCE, filtered-out rows included,
 *     and no node scans it more than [MAX_STATS_SCAN_LOOPS] times.
 *     Both, because a per-file nested loop passes the first on its own;
 *  2. buffers are bounded by reading the relations once, with the
 *     budget taken from `pg_class.relpages` rather than written down,
 *     and with no per-loop slack;
 *  3. the narrowing REACHES THE DATABASE — asserted against the
 *     statement the repo function actually ISSUES, captured with a
 *     SqlLogger, not against one this test rebuilds from a flag;
 *  4. `hog_file_column_stats` carries exactly its primary key. An index
 *     nobody chooses does not exist (AGENT.md), and the table above is
 *     why these four do not.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ScanColumnStatsQueryPlanIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private var catalogId = 0L
    private var head = 0L

    @AfterAll
    fun tearDown() = db.close()

    private companion object {
        const val TABLES = 3
        const val FILES_PER_TABLE = 20_000
        const val COLUMNS = 30
        const val FILES = TABLES * FILES_PER_TABLE
        const val STATS_ROWS = FILES.toLong() * COLUMNS

        /**
         * The table under plan. Its files are ids 1, 1 + [TABLES],
         * 1 + 2 x [TABLES], ... — INTERLEAVED with the other tables'
         * rather than a contiguous block, which is the second way this
         * fixture can flatter a plan. Contiguous ids make one table's
         * rows a single primary-key RANGE, and the planner finds it
         * without help; production interleaves, because tables commit
         * concurrently and file ids come from one per-catalog
         * allocator.
         */
        const val PLANNED_TABLE = 1L

        /** A field id every file has a row for. */
        const val ONE_FIELD = 7L

        /**
         * The documented `stats_fields` maximum. Only [ONE_FIELD] of
         * them names a real column here, which is the realistic shape:
         * an engine names the ids its predicate uses, and a table has
         * far fewer columns than the cap allows.
         */
        const val MANY_FIELDS = 10_000

        /**
         * [MANY_FIELDS] ids of which exactly [ONE_FIELD] names a real
         * column. The other 9,999 sit far above any field id this
         * catalog allocated, so the answer is one row per file however
         * long the list is — which is the property the cap and the plan
         * both have to get right.
         */
        val MANY_FIELD_IDS: List<Long> =
            listOf(ONE_FIELD) + (1_000_000L until 1_000_000L + MANY_FIELDS - 1)

        const val NO_FIELDS = "no stats_fields"
        const val ONE_ID = "stats_fields = 1 id"
        const val MANY_IDS = "stats_fields = 10,000 ids"

        /**
         * Most times a node may scan `hog_file_column_stats` in one
         * plan.
         *
         * ONE is the honest answer for this fixture and every plan the
         * planner picks on it; four is slack for a plan shape that
         * splits the scan (an Append, a BitmapOr) without making it
         * per-file. What this excludes is the per-FILE nested loop —
         * 20,000 loops here — and it has to be excluded EXPLICITLY,
         * because that plan hides from every other assertion in this
         * class: it touches FEWER rows than a sequential scan (one per
         * file, not the whole relation), so the row bound passes, and
         * its buffers are nearly all cache hits. Forced, it runs at
         * 210,796 buffers against 23,393 for the plan the planner
         * actually picks, and `loops` is the only number that says so.
         *
         * A small table legitimately gets that plan — measured
         * crossover is about `relpages / 4.5` files, so a 5,000-file
         * table on this relation takes a primary-key nested loop and a
         * 20,000-file one does not. This fixture is deliberately the
         * far side of it.
         */
        const val MAX_STATS_SCAN_LOOPS = 4L
    }

    @BeforeAll
    fun seed() {
        catalogs.createCatalog("statsplan", "s3://statsplan")
        catalogs.createNamespace("statsplan", "ns")
        catalogs.createTable("statsplan", "ns", "t1", listOf(ColumnDef("id", ColType.LONG)))
        db.jdbi.withHandleUnchecked { h ->
            catalogId =
                h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = 'statsplan'")
                    .mapTo(Long::class.java).one()
            head =
                h.createQuery("SELECT last_snapshot_id FROM hog_catalog WHERE catalog_id = :c")
                    .bind("c", catalogId).mapTo(Long::class.java).one()
        }
        check(head >= 1) { "fixture needs at least one snapshot below head" }

        db.jdbi.useHandleUnchecked { h ->
            // Tables 2..TABLES: identity rows only. The plan statement
            // joins hog_data_file, never hog_table_version, so the
            // version rows the real DDL writes are not what makes these
            // tables matter — their FILES are, because they are the rows
            // the statement must not read.
            h.createUpdate(
                """
                INSERT INTO hog_table (catalog_id, table_id, created_snapshot)
                SELECT :c, g, 1 FROM generate_series(2, :n) g
                """,
            ).bind("c", catalogId).bind("n", TABLES).execute()

            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start)
                SELECT :c, g, ((g - 1) % :tables) + 1, 1,
                       's3://statsplan/data/' || g || '.parquet', 1000, 1048576, g * 1000
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("tables", TABLES).bind("n", FILES).execute()

            // PHYSICAL ORDER IS PART OF THE FIXTURE. A commit writes one
            // file's whole stats set together, so the heap is clustered
            // by data_file_id and one column's rows are scattered one per
            // file across every page. `ORDER BY` is what guarantees that
            // here: without it the planner is free to run the column
            // series outermost and cluster the heap by field_id instead —
            // an order production never produces, under which an index on
            // field_id looks like a 20x win (see the class comment).
            h.createUpdate(
                """
                INSERT INTO hog_file_column_stats (catalog_id, data_file_id, field_id,
                                                   value_count, null_count, lower_bound, upper_bound)
                SELECT :c, f, c, 1000, 0, '\x0000000000000000'::bytea, '\xffffffffffffffff'::bytea
                FROM generate_series(1, :files) f, generate_series(1, :cols) c
                ORDER BY f, c
                """,
            ).bind("c", catalogId).bind("files", FILES).bind("cols", COLUMNS).execute()

            // VACUUM, not just ANALYZE: a bulk insert leaves the
            // visibility map unset, and the buffer budget below would be
            // measuring the absence of autovacuum rather than the plan.
            for (relation in listOf("hog_data_file", "hog_file_column_stats")) {
                h.execute("VACUUM (ANALYZE) $relation")
            }
        }
    }

    /**
     * The statement [FileRepo.providedColumnStatsAt] ACTUALLY ISSUES
     * for a request, captured off an instrumented Jdbi with a
     * [SqlLogger] — the pattern `FileStatsApiTest.scanStatements` and
     * `VerifySpecParityTest` use.
     *
     * Not `providedColumnStatsSql(filtered = ...)` with a flag this
     * test chooses. That version EXPLAINed a statement the test built
     * and merely hoped the repo also built: move the field narrowing
     * out of the SQL and into a Kotlin `filter` after the read and
     * every assertion below stays green, because the test would keep
     * handing EXPLAIN the filtered text. Going through the real call
     * means the captured SQL loses its filter exactly when production
     * does, and `the field narrowing runs in the database` reds.
     *
     * Cached per shape: the unnarrowed call materializes 600,000 rows,
     * and it is the same statement every time.
     */
    private val issuedSql = mutableMapOf<String, String>()

    private fun issuedSqlFor(
        key: String,
        fieldIds: Set<Long>?,
    ): String =
        issuedSql.getOrPut(key) {
            val issued = CopyOnWriteArrayList<String>()
            val instrumented = Database.jdbi(db.dataSource)
            instrumented.setSqlLogger(
                object : SqlLogger {
                    override fun logAfterExecution(context: StatementContext) {
                        issued += context.renderedSql
                    }
                },
            )
            instrumented.useHandleUnchecked { h ->
                FileRepo.providedColumnStatsAt(h, catalogId, PLANNED_TABLE, head, fieldIds)
            }
            // ONE statement for the whole plan, never one per file: the
            // repo function's own contract, and the thing that makes
            // reading a single plan meaningful.
            assertThat(issued.filter { it.contains("hog_file_column_stats") })
                .describedAs("providedColumnStatsAt must issue exactly one statistics statement")
                .hasSize(1)
            issued.single { it.contains("hog_file_column_stats") }
        }

    /** EXPLAIN ANALYZE of that captured statement, serial plans only. */
    private fun plan(
        key: String,
        fieldIds: Set<Long>?,
        force: String = "",
    ): String {
        val sql = issuedSqlFor(key, fieldIds)
        return db.jdbi.inTransactionUnchecked { h ->
            // A Gather reports its workers' scans with loops= equal to the
            // worker count, which would make the arithmetic below depend
            // on the machine's core count.
            h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
            if (force.isNotEmpty()) h.execute(force)
            val q =
                h.createQuery("EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " + sql)
                    .bind("catalogId", catalogId)
                    .bind("tableId", PLANNED_TABLE)
                    .bind("snapshot", head)
            // Bound only when the captured statement declares it, so a
            // narrowing that moved into Kotlin fails on the ASSERTION
            // about rows rather than on a binding error nobody can read.
            if (sql.contains(":fieldIds")) {
                q.bindArray("fieldIds", Long::class.javaObjectType, fieldIds.orEmpty().toList())
            }
            q.mapTo(String::class.java).list().joinToString("\n")
        }
    }

    /** The three shapes the route can produce, by name. */
    private fun shapes(): Map<String, String> =
        mapOf(
            NO_FIELDS to plan(NO_FIELDS, null),
            ONE_ID to plan(ONE_ID, setOf(ONE_FIELD)),
            MANY_IDS to plan(MANY_IDS, MANY_FIELD_IDS.toSet()),
        )

    // ---- reading a plan by WORK, not by output rows ------------------------

    private data class ScanNode(
        val relation: String,
        val index: String?,
        val line: String,
        val rows: Double,
        val loops: Long,
        val removed: Long,
        val buffers: Long,
        val neverExecuted: Boolean,
    ) {
        /** Rows the executor looked at, filtered-out ones included. */
        val rowsTouched: Long get() = ((rows + removed) * loops).toLong()
    }

    private fun indentOf(line: String) = line.length - line.trimStart().length

    /**
     * Every scan of [relation] in [text] with its own detail lines. A
     * scan node is a LEAF, so its detail block runs to the next line at
     * its indentation or shallower and no nested node can steal its
     * `Buffers:` line.
     */
    private fun scanNodes(
        text: String,
        relation: String,
    ): List<ScanNode> {
        val lines = text.lines()
        val onRelation = Regex("""\bon $relation\b""")
        val out = mutableListOf<ScanNode>()
        for ((i, line) in lines.withIndex()) {
            if (!onRelation.containsMatchIn(line)) continue
            val depth = indentOf(line)
            val detail = lines.drop(i + 1).takeWhile { it.isNotBlank() && indentOf(it) > depth }
            val rowsLoops = Regex("""rows=([\d.]+)\s+loops=(\d+)""").find(line)

            fun detailLong(prefix: String): Long =
                detail.firstOrNull { it.trim().startsWith(prefix) }
                    ?.let { Regex("""(\d+)""").find(it.substringAfter(prefix))?.value?.toLong() }
                    ?: 0L
            val buffersLine = detail.firstOrNull { it.trim().startsWith("Buffers:") }
            val hit = buffersLine?.let { Regex("""shared hit=(\d+)""").find(it)?.groupValues?.get(1)?.toLong() } ?: 0L
            val read = buffersLine?.let { Regex("""read=(\d+)""").find(it)?.groupValues?.get(1)?.toLong() } ?: 0L
            out.add(
                ScanNode(
                    relation = relation,
                    index = Regex("""Scan using (\S+) on""").find(line)?.groupValues?.get(1),
                    line = line.trim(),
                    rows = rowsLoops?.groupValues?.get(1)?.toDouble() ?: 0.0,
                    loops = rowsLoops?.groupValues?.get(2)?.toLong() ?: 0L,
                    removed = detailLong("Rows Removed by Filter:"),
                    buffers = hit + read,
                    neverExecuted = line.contains("never executed"),
                ),
            )
        }
        return out
    }

    /** heap + index pages, from the catalog rather than a constant. */
    private fun relationPages(vararg names: String?): Long =
        db.jdbi.withHandleUnchecked { h ->
            names.filterNotNull().sumOf { n ->
                h.createQuery("SELECT relpages FROM pg_class WHERE relname = :n")
                    .bind("n", n).mapTo(Long::class.java).findOne().orElse(0L)
            }
        }

    @Test
    fun `no shape reads the stats relation more than once, and none is skipped`() {
        for ((shape, text) in shapes()) {
            val scans = scanNodes(text, "hog_file_column_stats")
            assertThat(scans)
                .describedAs("%s should touch hog_file_column_stats at all:%n%s", shape, text)
                .isNotEmpty()
            assertThat(scans.filter { it.neverExecuted })
                .describedAs("%s never executed its stats scan, so this plan asserts nothing:%n%s", shape, text)
                .isEmpty()

            val touched = scans.sumOf { it.rowsTouched }
            assertThat(touched)
                .describedAs(
                    "%s LOOKED AT %d rows of hog_file_column_stats across %d loops, and the relation " +
                        "holds %d. More than one pass means the join stopped being a single hash pass " +
                        "and became a per-row re-read.%n%s",
                    shape,
                    touched,
                    scans.sumOf { it.loops },
                    STATS_ROWS,
                    text,
                )
                .isLessThanOrEqualTo(STATS_ROWS)
            assertThat(touched).describedAs("%s read no stats rows at all", shape).isGreaterThan(0)

            // LOOPS, separately and explicitly. The row bound above does
            // NOT catch a per-file nested loop — that plan reads one row
            // per file and so touches fewer rows than the sequential
            // scan it replaced, and passes. `loops` is the degradation
            // signal, so it cannot also be slack in the budget.
            for (scan in scans) {
                assertThat(scan.loops)
                    .describedAs(
                        "%s scans hog_file_column_stats %d times. Once per FILE is the quadratic " +
                            "shape this statement exists to avoid, and it is invisible to every " +
                            "other assertion here: it reads fewer rows than a full pass and its " +
                            "buffers are cache hits.%n%s",
                        shape,
                        scan.loops,
                        text,
                    )
                    .isLessThanOrEqualTo(MAX_STATS_SCAN_LOOPS)
            }
        }
    }

    @Test
    fun `no shape costs more buffers than reading its relations once`() {
        // Rows and buffers fail on different plans, so both are here:
        // rows catch a scan that reads too much, buffers catch one that
        // reads the same pages repeatedly — and buffers are what an
        // operator notices, because they are IO.
        //
        // The budget is read from pg_class rather than written down:
        // both relations' heaps and every index this schema gives them,
        // doubled for slack. There is NO per-loop term — an earlier
        // version added eight pages per loop, which made a 20,000-loop
        // nested loop buy itself a 160,000-page allowance and pass at
        // 210,796 buffers. Loops are the thing being detected; they
        // cannot also fund the detection. The measured plan sits at
        // about a third of this budget.
        val budget =
            relationPages(
                "hog_file_column_stats",
                "hog_file_column_stats_pkey",
                "hog_data_file",
                "hog_data_file_pkey",
                "hog_data_file_live",
                "hog_data_file_changefeed",
            ) * 2
        for ((shape, text) in shapes()) {
            val scans =
                scanNodes(text, "hog_file_column_stats") + scanNodes(text, "hog_data_file")
            val buffers = scans.sumOf { it.buffers }
            assertThat(buffers)
                .describedAs(
                    "%s touched %d shared buffers against a budget of %d (2 x every page both " +
                        "relations and their indexes hold).%n%s",
                    shape,
                    buffers,
                    budget,
                    text,
                )
                .isLessThanOrEqualTo(budget)
            assertThat(buffers).describedAs("%s buffers read", shape).isGreaterThan(0)
        }
    }

    @Test
    fun `the field narrowing runs in the database, not afterwards in Kotlin`() {
        // The property `stats_fields` exists for. A narrowing applied
        // after the read would leave these plans emitting files x 30
        // rows and look identical on every functional test, while
        // shipping the whole wide payload out of Postgres.
        val files = FILES_PER_TABLE.toLong()
        val emitted = { text: String -> Regex("""rows=(\d+)""").find(text)!!.groupValues[1].toLong() }
        val plans = shapes()

        assertThat(emitted(plans.getValue(NO_FIELDS)))
            .describedAs("unnarrowed: every column of every file")
            .isEqualTo(files * COLUMNS)
        assertThat(emitted(plans.getValue(ONE_ID)))
            .describedAs("one field id: one row per file")
            .isEqualTo(files)
        assertThat(emitted(plans.getValue(MANY_IDS)))
            .describedAs("%d field ids of which one exists: one row per file", MANY_FIELDS)
            .isEqualTo(files)

        // ...and the filter is in the STATEMENT, not merely in the row
        // count. A Kotlin-side narrowing would issue the unnarrowed SQL
        // for a narrowed request, which is the same text in both slots.
        assertThat(issuedSqlFor(ONE_ID, setOf(ONE_FIELD)))
            .describedAs("the narrowed request must issue a narrowed statement")
            .contains("field_id = ANY(:fieldIds)")
        assertThat(issuedSqlFor(NO_FIELDS, null))
            .doesNotContain("field_id = ANY(:fieldIds)")
    }

    @Test
    fun `hog_file_column_stats carries its primary key and no other index`() {
        // An index proves itself against the query it serves (AGENT.md).
        // Four candidates this statement invites were built and
        // measured, and all four read the same 44,445 heap blocks the
        // sequential scan reads, because the heap is clustered by
        // data_file_id and one column's rows are one per file. The
        // class comment carries the numbers, the write cost of each,
        // and the one alternative (CLUSTER) that does change the
        // answer. This assertion is what makes the next person read
        // them before adding an index back.
        val indexes =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT indexname FROM pg_indexes " +
                        "WHERE tablename = 'hog_file_column_stats' ORDER BY indexname",
                ).mapTo(String::class.java).list()
            }
        assertThat(indexes).containsExactly("hog_file_column_stats_pkey")
    }

    @Test
    fun `the statement resolves on the big relation and answers only the planned table`() {
        // The plans above are only worth reading if the statement is
        // correct on this fixture: one table's files, every column, and
        // nothing from the other two tables' 1.2M stats rows.
        val rows =
            db.jdbi.withHandleUnchecked { h ->
                FileRepo.providedColumnStatsAt(h, catalogId, PLANNED_TABLE, head, null)
            }
        assertThat(rows).hasSize(FILES_PER_TABLE)
        // Interleaved ids: the planned table owns 1, 1 + TABLES, ...
        assertThat(rows.keys.min()).isEqualTo(1L)
        assertThat(rows.keys.max()).isEqualTo(FILES.toLong() - TABLES + 1)
        assertThat(rows.keys).allSatisfy { assertThat((it - 1) % TABLES).isZero() }
        assertThat(rows.values).allSatisfy { assertThat(it).hasSize(COLUMNS) }

        val narrowed =
            db.jdbi.withHandleUnchecked { h ->
                FileRepo.providedColumnStatsAt(h, catalogId, PLANNED_TABLE, head, setOf(ONE_FIELD))
            }
        assertThat(narrowed).hasSize(FILES_PER_TABLE)
        assertThat(narrowed.values).allSatisfy {
            assertThat(it).singleElement().satisfies({ row -> assertThat(row.fieldId).isEqualTo(ONE_FIELD) })
        }
    }
}
