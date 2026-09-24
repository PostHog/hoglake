package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.service.CatalogService
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
 * The plan behind `TableRepo.listLiveSummaries`, on a manifest and a
 * change log the size of a real one.
 *
 * The listing replaced an N+1 (`listLive` plus one `aggregateAt` per
 * table) with one statement carrying two LATERAL sub-selects. LATERAL
 * is a nested loop BY CONSTRUCTION — one inner execution per table — so
 * the `loops=1` rule `VerifyQueryPlanIntegrationTest` enforces would be
 * the wrong question here. The right one is WORK.
 *
 * The first version of this test measured work as `rows × loops` and was
 * WRONG, in the way that matters: EXPLAIN's `rows=` is the count a node
 * EMITTED, after its Filter. Demote `table_id` out of the index
 * condition — which is all it takes for the planner to pick a
 * `(catalog_id)`-leading index — and the scan emits the same 250 rows
 * per loop while reading 52,000 and discarding 51,750 of them. That plan
 * touched 10.4 million rows and 207,000 shared buffers instead of 1,786,
 * and all five assertions here stayed green. Output is not work.
 *
 * So the four assertions below measure what the executor DID:
 *
 *  1. no SEQUENTIAL scan of `hog_data_file` or `hog_snapshot_change`
 *     anywhere — one seq scan per table is the quadratic shape, and it
 *     is what the planner picks the moment the predicate stops matching
 *     an index;
 *  2. every inner scan is INDEX-DRIVEN. The index NAME is not pinned:
 *     two indexes lead on (catalog_id, table_id, begin_snapshot) and the
 *     planner is free to prefer either — as measured it takes
 *     `hog_data_file_changefeed` over the partial `hog_data_file_live`,
 *     and `hog_snapshot_change_conflict` for the history counts. Which
 *     one is not the property; what it serves is;
 *  3. ROWS LOOKED AT — `(rows + Rows Removed by Filter) × loops` — is
 *     bounded by what the relation holds. This is the assertion the
 *     degraded plan fails, by 200x;
 *  4. SHARED BUFFERS per scan is bounded by reading the relation about
 *     once, with the budget read from `pg_class.relpages` for the heap
 *     and the index the plan chose rather than written down as a
 *     constant. Rows and buffers fail on different plans — rows catch a
 *     scan that reads too much, buffers catch one that reads the same
 *     pages repeatedly — and buffers are what an operator notices,
 *     because they are IO.
 *
 * A fixture that is one namespace of 200 tables over 50k files is not
 * decoration: a small one plans beautifully under every shape, which is
 * precisely how the N+1 survived as long as it did.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TableListingQueryPlanIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private var catalogId = 0L
    private var namespaceId = 0L
    private var head = 0L

    @AfterAll
    fun tearDown() = db.close()

    private companion object {
        const val TABLES = 200
        const val FILES_PER_TABLE = 250
        const val DATA_FILES = TABLES * FILES_PER_TABLE // 50,000

        /** Files end-snapshotted below head: present in the relation, invisible to the listing. */
        const val DEAD_FILES_PER_TABLE = 10
        const val DATA_FILE_ROWS = DATA_FILES + TABLES * DEAD_FILES_PER_TABLE
        const val CHANGES_PER_TABLE = 20
        const val CHANGE_ROWS = TABLES * CHANGES_PER_TABLE

        /**
         * Tables in a SECOND namespace of the same catalog.
         *
         * Without them the listing's driving scan cannot be measured at
         * all: with one namespace, "the tables in this namespace" and
         * "the tables in this catalog" are the same rows, so a plan that
         * reads the whole catalog looks identical to one that reads the
         * namespace. Deliberately larger than [TABLES], so a
         * catalog-wide driving scan costs MORE than the answer.
         */
        const val OTHER_NAMESPACE_TABLES = 600

        /** Scanned per table; a sequential scan of either is the bug. */
        val INNER_RELATIONS = listOf("hog_data_file", "hog_snapshot_change")

        /**
         * The DRIVING relation. Not an inner scan — it runs once — but
         * it is the other half of "the listing sizes with the namespace,
         * not the catalog", and nothing measured it until a second
         * namespace existed to tell the two apart.
         */
        const val DRIVING_RELATION = "hog_table_version"
    }

    @BeforeAll
    fun seed() {
        catalogs.createCatalog("listplan", "s3://listplan")
        catalogs.createNamespace("listplan", "ns")
        // One real table, so the version/stats rows the DDL writes exist
        // in the shape the service makes them; the other 199 are
        // generated, because 200 DDL commits prove nothing about a plan.
        catalogs.createTable("listplan", "ns", "t1", listOf(ColumnDef("id", ColType.LONG)))
        db.jdbi.withHandleUnchecked { h ->
            catalogId =
                h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = 'listplan'")
                    .mapTo(Long::class.java).one()
            namespaceId =
                h.createQuery("SELECT namespace_id FROM hog_namespace WHERE catalog_id = :c")
                    .bind("c", catalogId).mapTo(Long::class.java).one()
            head =
                h.createQuery("SELECT last_snapshot_id FROM hog_catalog WHERE catalog_id = :c")
                    .bind("c", catalogId).mapTo(Long::class.java).one()
        }
        check(head >= 1) { "fixture needs at least one snapshot below head" }

        db.jdbi.useHandleUnchecked { h ->
            // Snapshots for the generated change rows to reference: the FK
            // is real, and a change row pointing at a snapshot that does
            // not exist would not insert.
            h.createUpdate(
                """
                INSERT INTO hog_snapshot (catalog_id, snapshot_id, schema_version)
                SELECT :c, :head + g, 1 FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("head", head).bind("n", CHANGES_PER_TABLE).execute()
            val top = head + CHANGES_PER_TABLE
            h.createUpdate("UPDATE hog_catalog SET last_snapshot_id = :top WHERE catalog_id = :c")
                .bind("c", catalogId).bind("top", top).execute()
            head = top

            // A second namespace, holding more tables than the one under
            // test — see OTHER_NAMESPACE_TABLES.
            h.createUpdate(
                "INSERT INTO hog_namespace (catalog_id, namespace_id, name) " +
                    "VALUES (:c, :ns, 'other')",
            ).bind("c", catalogId).bind("ns", namespaceId + 1).execute()

            // Tables 2..200: identity + live version row, named so the
            // ORDER BY has real work to do.
            h.createUpdate(
                """
                INSERT INTO hog_table (catalog_id, table_id, created_snapshot)
                SELECT :c, g, 1 FROM generate_series(2, :n) g
                """,
            ).bind("c", catalogId).bind("n", TABLES).execute()
            h.createUpdate(
                """
                INSERT INTO hog_table_version (catalog_id, table_id, begin_snapshot, namespace_id, name)
                SELECT :c, g, 1, :ns, 't' || g FROM generate_series(2, :n) g
                """,
            ).bind("c", catalogId).bind("ns", namespaceId).bind("n", TABLES).execute()
            // ...and the other namespace's, which this listing must not
            // pay for. No files and no change rows: the point is the
            // driving scan, and version rows are all it reads.
            h.createUpdate(
                """
                INSERT INTO hog_table (catalog_id, table_id, created_snapshot)
                SELECT :c, :base + g, 1 FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("base", TABLES).bind("n", OTHER_NAMESPACE_TABLES).execute()
            h.createUpdate(
                """
                INSERT INTO hog_table_version (catalog_id, table_id, begin_snapshot, namespace_id, name)
                SELECT :c, :base + g, 1, :ns, 'other' || g FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("base", TABLES).bind("ns", namespaceId + 1)
                .bind("n", OTHER_NAMESPACE_TABLES).execute()

            // The manifest: FILES_PER_TABLE live files on every table, so
            // the rows-times-loops arithmetic below is exact rather than
            // rounded. All live (end_snapshot NULL).
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           path, record_count, file_size_bytes, row_id_start)
                SELECT :c, g, ((g - 1) / :per) + 1, 1,
                       's3://listplan/data/' || g || '.parquet', 10, 1024, g * 10
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("per", FILES_PER_TABLE).bind("n", DATA_FILES).execute()
            // ...and DEAD_FILES_PER_TABLE rows per table that are already
            // end-snapshotted. They exist so invariant 6's visibility
            // predicate is LOAD-BEARING here: drop the end_snapshot half
            // of it and every count below is off by exactly these.
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           end_snapshot, path, record_count, file_size_bytes,
                                           row_id_start)
                SELECT :c, :base + g, ((g - 1) / :per) + 1, 1, 2,
                       's3://listplan/dead/' || g || '.parquet', 99, 9999, (:base + g) * 10
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("base", DATA_FILES).bind("per", DEAD_FILES_PER_TABLE)
                .bind("n", TABLES * DEAD_FILES_PER_TABLE).execute()

            // The change log: CHANGES_PER_TABLE snapshots per table, all
            // table-scoped kinds. Plus namespace rows on the SAME object
            // ids, which is the trap the kind filter exists for — without
            // it every table's count would be inflated by a namespace's.
            h.createUpdate(
                """
                INSERT INTO hog_snapshot_change (catalog_id, snapshot_id, kind, object_id)
                SELECT :c, :head - :per + s, 'table_inserted_into', t
                FROM generate_series(1, :tables) t, generate_series(1, :per) s
                """,
            ).bind("c", catalogId).bind("head", head).bind("per", CHANGES_PER_TABLE)
                .bind("tables", TABLES).execute()
            h.createUpdate(
                """
                INSERT INTO hog_snapshot_change (catalog_id, snapshot_id, kind, object_id)
                SELECT :c, :head, 'namespace_created', t
                FROM generate_series(1, :tables) t
                """,
            ).bind("c", catalogId).bind("head", head).bind("tables", TABLES).execute()

            // VACUUM, not just ANALYZE: a bulk insert leaves the
            // visibility map unset, so an Index Only Scan pays a heap
            // fetch per row and the buffer budget below would be
            // measuring the absence of autovacuum rather than the plan.
            for (r in INNER_RELATIONS + listOf("hog_table", "hog_table_version")) {
                h.execute("VACUUM (ANALYZE) $r")
            }
        }
    }

    /** EXPLAIN ANALYZE of the production statement, serial plan only. */
    private fun plan(): String =
        db.jdbi.inTransactionUnchecked { h ->
            // A Gather reports its workers' scans with loops= equal to the
            // worker count, which would make the arithmetic below depend
            // on the machine's core count.
            h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
            h.createQuery(
                "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " +
                    TableRepo.LIVE_SUMMARIES_SQL,
            )
                .bind("catalogId", catalogId)
                .bind("namespaceId", namespaceId)
                .bindArray("kinds", String::class.java, ChangeKind.TABLE_SCOPED.map { it.wire })
                .mapTo(String::class.java)
                .list()
                .joinToString("\n")
        }

    // ---- reading a plan, by WORK rather than by output rows ---------------

    /**
     * One scan node: what it emitted, what it threw away, and what it
     * cost.
     *
     * [rows] is EXPLAIN's per-loop row count, and it is POST-FILTER —
     * which is the whole reason this type exists. A plan driven by a
     * `(catalog_id)`-only index with `table_id` demoted to a Filter
     * emits the same 250 rows per loop as the correct one while reading
     * 51,750 more and throwing them away, so an assertion on [rows]
     * alone called a plan that touched 10.4 MILLION rows and 207,000
     * buffers "reads each row once". [removed] and [buffers] are the two
     * places that plan could not hide.
     */
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
        /** Rows the executor actually looked at, filtered-out ones included. */
        val rowsTouched: Long get() = ((rows + removed) * loops).toLong()
    }

    private fun indentOf(line: String) = line.length - line.trimStart().length

    /**
     * Every scan of [relation] in [text], with the detail lines that
     * belong to it.
     *
     * A scan node is a LEAF, so its own detail block runs to the next
     * line at its indentation or shallower — no nested node can steal a
     * `Buffers:` line from it.
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
            val detail =
                lines.drop(i + 1).takeWhile { it.isNotBlank() && indentOf(it) > depth }
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

    /** heap + named-index pages, from the catalog rather than a constant. */
    private fun relationPages(vararg names: String?): Long =
        db.jdbi.withHandleUnchecked { h ->
            names.filterNotNull().sumOf { n ->
                h.createQuery("SELECT relpages FROM pg_class WHERE relname = :n")
                    .bind("n", n).mapTo(Long::class.java).findOne().orElse(0L)
            }
        }

    @Test
    fun `neither inner relation is sequentially scanned`() {
        val text = plan()
        for (relation in INNER_RELATIONS) {
            assertThat(text)
                .describedAs("the listing must not sequentially scan %s:\n%s", relation, text)
                .doesNotContain("Seq Scan on $relation")
        }
    }

    @Test
    fun `each inner scan is index-driven, not a scan of the whole relation`() {
        val text = plan()
        for (relation in INNER_RELATIONS) {
            val scans = scanNodes(text, relation)
            assertThat(scans)
                .describedAs("the plan should scan %s at all:\n%s", relation, text)
                .isNotEmpty()
            // The INDEX NAME is deliberately not pinned. Two indexes lead
            // on (catalog_id, table_id, begin_snapshot) — the partial
            // `hog_data_file_live` and the non-partial
            // `hog_data_file_changefeed` — and the planner is free to
            // prefer either; as measured it takes the changefeed one.
            // WHICH index is not the property; that an index supplies the
            // whole predicate is, and the two tests below say so by
            // measuring what the scan read.
            assertThat(scans)
                .describedAs("every scan of %s must be index-driven:\n%s", relation, text)
                .allSatisfy { assertThat(it.index).isNotNull() }
        }
    }

    @Test
    fun `no inner relation has more rows than it holds looked at, filtered ones included`() {
        val text = plan()
        val sizes =
            mapOf(
                "hog_data_file" to DATA_FILE_ROWS.toLong(),
                "hog_snapshot_change" to CHANGE_ROWS.toLong(),
            )
        for ((relation, size) in sizes) {
            val scans = scanNodes(text, relation)
            assertThat(scans).describedAs("scans of %s:\n%s", relation, text).isNotEmpty()
            assertThat(scans.filter { it.neverExecuted })
                .describedAs("%s was never executed, so this plan asserts nothing:\n%s", relation, text)
                .isEmpty()

            val touched = scans.sumOf { it.rowsTouched }
            val emitted = scans.sumOf { (it.rows * it.loops).toLong() }
            val discarded = scans.sumOf { it.removed * it.loops }
            assertThat(touched)
                .describedAs(
                    "the listing LOOKED AT %d rows of %s (%d emitted + %d removed by filter across " +
                        "%d loops), and the relation holds %d. More than once through the relation " +
                        "means the predicate is not being served by the index — the classic shape " +
                        "is a leading-column-only index with the rest demoted to a Filter, which " +
                        "emits exactly the right rows and reads the whole table to find them.\n%s",
                    touched,
                    relation,
                    emitted,
                    discarded,
                    scans.sumOf { it.loops },
                    size,
                    text,
                )
                .isLessThanOrEqualTo(size)
            assertThat(touched).describedAs("%s rows actually read", relation).isGreaterThan(0)
        }
    }

    @Test
    fun `the driving scan reads the catalog's version rows once, and the laterals only this namespace's`() {
        val text = plan()
        val driving = scanNodes(text, DRIVING_RELATION).single()
        assertThat(driving.index)
            .describedAs("the driving scan must be index-driven:\n%s", text)
            .isNotNull()

        // MEASURED, not hoped for. The planner takes
        // `hog_table_version_pkey` (catalog_id, table_id, begin_snapshot)
        // and demotes `namespace_id` to a Filter, so the driving scan
        // reads every version row in the CATALOG — here 200 of ours plus
        // the 600 in the other namespace, discarded. That is a real term
        // and it is asserted rather than wished away: each row once, no
        // more.
        val versionRows = (TABLES + OTHER_NAMESPACE_TABLES).toLong()
        assertThat(driving.rowsTouched)
            .describedAs("driving scan looked at %d version rows:\n%s", driving.rowsTouched, text)
            .isLessThanOrEqualTo(versionRows)

        // What it costs is the point: the driving scan is catalog-sized
        // but CHEAP (tens of buffers), while the laterals are the
        // expensive part and are namespace-sized — they run once per row
        // the driving scan EMITS, which the Filter has already reduced to
        // this namespace. That asymmetry is the whole LATERAL-versus-
        // GROUP-BY argument, so it is asserted rather than asserted in a
        // comment: a grouped rewrite would make the expensive half
        // catalog-sized too.
        for (relation in INNER_RELATIONS) {
            for (scan in scanNodes(text, relation)) {
                assertThat(scan.loops)
                    .describedAs(
                        "%s ran %d times; it must run once per table in THIS namespace (%d), " +
                            "never once per table in the catalog (%d)\n%s",
                        relation,
                        scan.loops,
                        TABLES,
                        versionRows,
                        text,
                    )
                    .isEqualTo(TABLES.toLong())
                assertThat(scan.buffers)
                    .describedAs("the lateral over %s should dominate the driving scan", relation)
                    .isGreaterThan(driving.buffers)
            }
        }
    }

    @Test
    fun `no inner scan costs more buffers than reading its relation once`() {
        // The row bound above and this one fail on different plans, so
        // both are here. Rows catch a scan that reads too much; buffers
        // catch a scan that reads the same pages repeatedly — and buffers
        // are the number an operator would actually notice, because they
        // are IO.
        //
        // The budget is read from pg_class rather than written down: the
        // heap plus the index the plan chose, doubled for slack, plus a
        // B-tree descent's worth of pages per loop. A plan that honours
        // the whole predicate sits far inside it (measured: 1,786 of a
        // ~3,000 budget on hog_data_file); the degraded catalog-only plan
        // spent 207,000.
        val text = plan()
        for (relation in INNER_RELATIONS) {
            for (scan in scanNodes(text, relation)) {
                val budget = relationPages(relation, scan.index) * 2 + scan.loops * 8
                assertThat(scan.buffers)
                    .describedAs(
                        "the %s scan touched %d shared buffers against a budget of %d " +
                            "(2 x (heap + %s pages) + 8 per loop x %d loops).\n%s\n%s",
                        relation,
                        scan.buffers,
                        budget,
                        scan.index ?: "no index — this is a sequential scan",
                        scan.loops,
                        scan.line,
                        text,
                    )
                    .isLessThanOrEqualTo(budget)
                assertThat(scan.buffers).describedAs("%s buffers read", relation).isGreaterThan(0)
            }
        }
    }

    @Test
    fun `the listing resolves on the big catalog and reports per-table numbers`() {
        val rows =
            db.jdbi.withHandleUnchecked { h ->
                TableRepo.listLiveSummaries(h, catalogId, namespaceId)
            }
        assertThat(rows).hasSize(TABLES)
        assertThat(rows.map { it.name }).isSorted()
        assertThat(rows).allSatisfy {
            // The dead rows carry record_count 99 / file_size_bytes 9999,
            // so a visibility predicate that let them through would miss
            // these equalities by a wide, diagnosable margin rather than
            // by a rounding error.
            assertThat(it.fileCount).isEqualTo(FILES_PER_TABLE.toLong())
            assertThat(it.recordCount).isEqualTo(FILES_PER_TABLE * 10L)
            assertThat(it.fileSizeBytes).isEqualTo(FILES_PER_TABLE * 1024L)
        }
        // CHANGES_PER_TABLE table-scoped snapshots each. The
        // 'namespace_created' rows the fixture planted on the SAME object
        // ids must not be counted: object_id spans three id spaces and
        // only the kind filter separates them, so a query missing that
        // filter reports CHANGES_PER_TABLE + 1 here.
        //
        // t1 is the one REAL table, so it carries its own table_created
        // row from the DDL on top of the generated ones — which is also
        // why its earliest is the create snapshot rather than the
        // generated window's floor.
        val (real, generated) = rows.partition { it.name == "t1" }
        assertThat(generated).allSatisfy {
            assertThat(it.snapshotCount).isEqualTo(CHANGES_PER_TABLE.toLong())
        }
        assertThat(real.single().snapshotCount).isEqualTo(CHANGES_PER_TABLE + 1L)
    }

    @Test
    fun `the expiry floor removes snapshots below it from the counts`() {
        // Invariant 5: snapshots below earliest_snapshot_id are gone, so
        // counting them would report history no reader can reach. Half
        // the range, half the count — and the earliest id moves with it.
        //
        // The floor is moved in `hog_catalog`, because the statement
        // reads its own bounds from that row; restored in `finally`, or
        // every other test in this class would inherit it (the fixture
        // is built once for the class).
        val floor = head - CHANGES_PER_TABLE / 2

        fun setFloor(v: Long) =
            db.jdbi.useHandleUnchecked { h ->
                h.execute("UPDATE hog_catalog SET earliest_snapshot_id = ? WHERE catalog_id = ?", v, catalogId)
            }
        val rows =
            try {
                setFloor(floor)
                db.jdbi.withHandleUnchecked { h ->
                    TableRepo.listLiveSummaries(h, catalogId, namespaceId)
                }
            } finally {
                setFloor(0)
            }
        assertThat(rows).allSatisfy {
            assertThat(it.snapshotCount).isEqualTo(CHANGES_PER_TABLE / 2L + 1)
            assertThat(it.earliestSnapshotId).isEqualTo(floor)
        }
    }
}
