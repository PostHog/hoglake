package com.posthog.hoglake.persistence

import com.posthog.hoglake.Database
import com.posthog.hoglake.service.RetirementService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

/**
 * V20 — `hog_table.retirement_eligible_at` and the
 * `hog_maintenance_run.task` CHECK — plus the plans of the three
 * statements a retirement BATCH issues.
 *
 * SPLIT FROM V19 DELIBERATELY. V19 is an index on `hog_data_file` that
 * helps the EXISTING expiry sweep and can ship on its own; this file
 * carries the two `ALTER TABLE`s, which take ACCESS EXCLUSIVE and have
 * to be timed against in-flight sweeps. Keeping them apart is what lets
 * the index go out first.
 *
 * The migration RUNS here: the database arrives at V19, rows are
 * seeded, then [Database.migrate] applies V20 — so "the column was not
 * there before" is observed rather than asserted about a file.
 *
 * Every statement EXPLAINed below comes from `RetirementService`'s own
 * `internal` constants. A plan test that retypes its query asserts the
 * plan of a string only it has ever executed (V14's first index was
 * proven against a predicate no code path issues).
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class V20RetirementMigrationIntegrationTest {
    private companion object {
        const val LIVE_FILES = 200_000

        const val FLOOR = 100L

        /** Rows the retirement batch asks for. */
        const val BATCH = 8_000

        const val REAPPLY_V20 = "DELETE FROM flyway_schema_history WHERE version::numeric >= 20"
    }

    private val db = PgTestSupport.freshDatabaseAt("19", productionSession = true)
    private var catalogId = 0L
    private var otherCatalogId = 0L
    private val droppedTableId = 2L

    private lateinit var victimPlan: String
    private lateinit var dataDeletePlan: String
    private lateinit var dvDeletePlan: String
    private lateinit var candidatePlan: String

    @AfterAll
    fun tearDown() = db.close()

    @BeforeAll
    fun seedThenMigrate() {
        catalogId = createCatalog("v20")
        otherCatalogId = createCatalog("v20-neighbour")
        seedManifest()
        analyze()

        assertThat(column("hog_table", "retirement_eligible_at"))
            .describedAs("the column arrives with V20")
            .isNull()

        Database.migrate(db.dataSource)
        analyze()

        victimPlan = explainVictimSelect()
        dataDeletePlan = explainDelete(RetirementService.DATA_DELETE_SQL)
        dvDeletePlan = explainDelete(RetirementService.DV_DELETE_SQL)
        candidatePlan = explainCandidates()
    }

    private fun createCatalog(name: String): Long =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "INSERT INTO hog_catalog (name, data_path, earliest_snapshot_id, last_snapshot_id) " +
                    "VALUES (:n, 's3://v20/' || :n, :f, 1000) RETURNING catalog_id",
            ).bind("n", name).bind("f", FLOOR).mapTo(Long::class.java).one()
        }

    private fun seedManifest() =
        db.jdbi.useHandleUnchecked { h ->
            for (c in listOf(catalogId, otherCatalogId)) {
                h.execute(
                    "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) " +
                        "SELECT ?, g, 0 FROM generate_series(1, 4) g",
                    c,
                )
            }
            // The dropped table the retirement statements work on, plus
            // a DRAINED one — dropped, eligible, and holding no file
            // row — which the candidate query must exclude.
            h.execute(
                "UPDATE hog_table SET dropped_snapshot = ? WHERE catalog_id = ? AND table_id = ?",
                FLOOR,
                catalogId,
                droppedTableId,
            )
            h.execute(
                "UPDATE hog_table SET dropped_snapshot = ? WHERE catalog_id = ? AND table_id = 3",
                FLOOR,
                catalogId,
            )
            h.createUpdate(
                """
                INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                           end_snapshot, path, record_count, file_size_bytes,
                                           row_id_start)
                SELECT CASE WHEN g % 10 = 0 THEN :other ELSE :c END, g,
                       -- tables 1, 2 and 4 only: table 3 is the DROPPED
                       -- AND DRAINED one, and it holds no file row at
                       -- all. That is the whole point of it.
                       CASE g % 3 WHEN 0 THEN 1 WHEN 1 THEN 2 ELSE 4 END, 1,
                       -- Some of the dropped table's rows are already
                       -- ENDED (a truncate, a compaction, an expiry
                       -- that has not collected them yet), so the
                       -- partial `hog_data_file_live` is genuinely more
                       -- selective than the non-partial changefeed
                       -- index and the planner has a reason to prefer
                       -- it — which is the production shape. With no
                       -- ended rows the two indexes hold the same rows
                       -- and either is a correct choice.
                       CASE WHEN g % 7 = 3 THEN 2 ELSE NULL END,
                       's3://v20/live/' || md5(g::text) || '/part-' || g || '.parquet',
                       120000, 268435456, g::bigint * 120000
                FROM generate_series(1, :n) g
                """,
            ).bind("c", catalogId).bind("other", otherCatalogId).bind("n", LIVE_FILES).execute()
        }

    private fun analyze() =
        db.jdbi.useHandleUnchecked { h ->
            for (relation in listOf("hog_data_file", "hog_delete_file", "hog_file_removal", "hog_table")) {
                h.execute("VACUUM (ANALYZE) $relation")
            }
        }

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

    private fun explainVictimSelect(): String =
        explain { h ->
            h.createQuery(
                "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " +
                    RetirementService.VICTIM_SELECT_SQL,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", droppedTableId)
                .bind("n", BATCH)
                .mapTo(String::class.java).list().joinToString("\n")
        }

    private fun explainCandidates(): String =
        explain { h ->
            h.createQuery(
                "EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) " +
                    RetirementService.CANDIDATE_SQL,
            )
                .bind("catalogId", catalogId)
                .bind("limit", RetirementService.MAX_TABLES_PER_RUN)
                .mapTo(String::class.java).list().joinToString("\n")
        }

    private fun explainDelete(sql: String): String {
        val victims =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(RetirementService.VICTIM_SELECT_SQL)
                    .bind("catalogId", catalogId)
                    .bind("tableId", droppedTableId)
                    .bind("n", BATCH)
                    .mapTo(Long::class.javaObjectType).list()
            }
        check(victims.isNotEmpty()) { "the dropped table must hold live rows for the delete to plan against" }
        return explain { h ->
            h.createQuery("EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) $sql")
                .bind("catalogId", catalogId)
                .bindArray("victims", Long::class.javaObjectType, victims)
                .mapTo(String::class.java).list().joinToString("\n")
        }
    }

    private data class ScanNode(
        val index: String?,
        val buffers: Long,
        val rowsRemovedByFilter: Long?,
        val indexSearches: Long?,
        val seqScan: Boolean,
    )

    private fun scanNode(
        text: String,
        relation: String,
    ): ScanNode {
        val lines = text.lines()

        fun indent(l: String) = l.length - l.trimStart().length
        val i = lines.indexOfFirst { Regex("""Scan.* on $relation\b""").containsMatchIn(it) }
        if (i < 0) throw AssertionError("no scan of $relation in:\n$text")
        val line = lines[i]
        val detail = lines.drop(i + 1).takeWhile { it.isNotBlank() && indent(it) > indent(line) }
        val buffersLine = detail.firstOrNull { it.trim().startsWith("Buffers:") }
        val hit = buffersLine?.let { Regex("""\bhit=(\d+)""").find(it)?.groupValues?.get(1)?.toLong() } ?: 0L
        val read = buffersLine?.let { Regex("""\bread=(\d+)""").find(it)?.groupValues?.get(1)?.toLong() } ?: 0L

        fun detailLong(prefix: String): Long? =
            detail.firstOrNull { it.trim().startsWith(prefix) }
                ?.let { Regex("""(\d+)""").find(it.substringAfter(prefix))?.value?.toLong() }
        return ScanNode(
            index = Regex("""Scan(?: Backward)? using (\S+) on""").find(line)?.groupValues?.get(1),
            buffers = hit + read,
            rowsRemovedByFilter = detailLong("Rows Removed by Filter:"),
            indexSearches = detailLong("Index Searches:"),
            seqScan = line.contains("Seq Scan on $relation"),
        )
    }

    /**
     * The index a plan drove [relation] from, whichever shape the scan
     * takes: an `Index Scan using <index>`, or the `Bitmap Index Scan
     * on <index>` feeding a `Bitmap Heap Scan`.
     */
    private fun driverIndex(
        text: String,
        relation: String,
    ): String? {
        Regex("""Scan(?: Backward)? using (\S+) on $relation\b""").find(text)
            ?.let { return it.groupValues[1] }
        val lines = text.lines()

        fun indent(l: String) = l.length - l.trimStart().length
        val heap = lines.indexOfFirst { it.contains("Bitmap Heap Scan on $relation") }
        if (heap < 0) return null
        return lines.drop(heap + 1)
            .takeWhile { it.isBlank() || indent(it) > indent(lines[heap]) }
            .firstOrNull { it.contains("Bitmap Index Scan on ") }
            ?.let { Regex("""Bitmap Index Scan on (\S+)""").find(it)?.groupValues?.get(1) }
    }

    private fun column(
        table: String,
        name: String,
    ): String? =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(
                "SELECT data_type FROM information_schema.columns " +
                    "WHERE table_name = :t AND column_name = :c",
            ).bind("t", table).bind("c", name).mapTo(String::class.java).findOne().orElse(null)
        }

    // ---- the migration -----------------------------------------------------

    @Test
    fun `V20 adds the column and widens the task vocabulary, and nothing else`() {
        assertThat(column("hog_table", "retirement_eligible_at")).isEqualTo("timestamp with time zone")
        db.jdbi.useHandleUnchecked { h ->
            h.execute(
                "INSERT INTO hog_maintenance_run (catalog_id, task, run_trigger, started_at, " +
                    "finished_at, status) VALUES (?, 'retirement', 'loop', now(), now(), 'ok')",
                catalogId,
            )
        }
        // The CHECK is still exactly one constraint, not two, and it
        // is VALIDATED — a `NOT VALID` left behind would let a future
        // row past it.
        val (count, validated) =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) AS n, bool_and(convalidated) AS ok FROM pg_constraint " +
                        "WHERE conrelid = 'hog_maintenance_run'::regclass " +
                        "AND conname = 'hog_maintenance_run_task_check'",
                ).map { rs, _ -> rs.getLong("n") to rs.getBoolean("ok") }.one()
            }
        assertThat(count).isEqualTo(1)
        assertThat(validated).describedAs("NOT VALID must be followed by VALIDATE").isTrue()
    }

    @Test
    fun `V20 is re-appliable, so a rolled-back deploy can simply run it again`() {
        db.jdbi.useHandleUnchecked { h -> h.execute(REAPPLY_V20) }
        Database.migrate(db.dataSource)
        assertThat(column("hog_table", "retirement_eligible_at")).isEqualTo("timestamp with time zone")
        assertThat(
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(
                    "SELECT count(*) FROM pg_constraint WHERE conrelid = 'hog_maintenance_run'::regclass " +
                        "AND conname = 'hog_maintenance_run_task_check'",
                ).mapTo(Long::class.java).one()
            },
        ).isEqualTo(1)
    }

    // ---- the retirement statements -----------------------------------------

    @Test
    fun `the candidate read is bounded and index-driven, and excludes drained tables`() {
        // `hog_table` is small, so what matters here is not the buffer
        // count but that the query is CORRECT about which tables it
        // proposes: a dropped table with no live file rows is finished,
        // and proposing it again would take the commit lock once per
        // historical drop, every run, forever.
        val proposed =
            db.jdbi.withHandleUnchecked { h ->
                h.createQuery(RetirementService.CANDIDATE_SQL)
                    .bind("catalogId", catalogId)
                    .bind("limit", RetirementService.MAX_TABLES_PER_RUN)
                    .map { rs, _ -> rs.getLong("table_id") }
                    .list()
            }
        assertThat(proposed)
            .describedAs(
                "table 3 is dropped and eligible but holds no file row; it is DONE:%n%s",
                candidatePlan,
            )
            .containsExactly(droppedTableId)
        assertThat(candidatePlan)
            .describedAs("the liveness probe must not scan the manifest:%n%s", candidatePlan)
            .doesNotContain("Seq Scan on hog_data_file")
        assertThat(RetirementService.CANDIDATE_SQL)
            .describedAs("the bound is in the statement, not applied after it")
            .contains("LIMIT :limit")
    }

    @Test
    fun `the victim select is an ordered index scan that stops at the batch`() {
        val node = scanNode(victimPlan, "hog_data_file")
        // AN INDEX, not an index NAME. `hog_data_file_live` and
        // `hog_data_file_changefeed` both lead on
        // (catalog_id, table_id, begin_snapshot) and the planner may
        // take either — V17's own plan test records the same thing and
        // asserts index-driven rather than a name, because a fixture
        // with no ended rows on the table makes the two indexes hold
        // identical rows and the "choice" an artifact.
        assertThat(node.index)
            .describedAs("the batch must be index-driven:%n%s", victimPlan)
            .isIn("hog_data_file_live", "hog_data_file_changefeed")
        assertThat(node.seqScan).isFalse()
        assertThat(victimPlan)
            .describedAs("a Bitmap scan materialises the whole table before the LIMIT:%n%s", victimPlan)
            .doesNotContain("Bitmap")
        // NO SORT, and this is the assertion that is easiest to lose.
        // The statement says `ORDER BY table_id, begin_snapshot` while
        // the index is keyed `(catalog_id, table_id, begin_snapshot)` —
        // the leading column is OMITTED, because it is an equality
        // constant in the WHERE clause. Postgres sees that and reads
        // the index in order; if it did not, it would have to
        // materialise every live row of the table and SORT it before
        // the LIMIT could take eight thousand — which on a 3M-row
        // dropped table is the whole table, inside a transaction
        // holding the commit lock. Nothing but the plan says which of
        // the two happened.
        assertThat(victimPlan)
            .describedAs("a Sort would read every live row before the LIMIT could apply:%n%s", victimPlan)
            .doesNotContain("Sort")
        assertThat(node.indexSearches)
            .describedAs("one descent:%n%s", victimPlan)
            .isEqualTo(1)
        // Whichever index it took, the LIMIT must be reached without
        // throwing away a meaningful number of rows: the partial index
        // carries the predicate, and the non-partial one only has to
        // skip the ended rows it passes on the way.
        assertThat(node.rowsRemovedByFilter ?: 0L)
            .describedAs("the scan must not filter away more than it returns:%n%s", victimPlan)
            .isLessThanOrEqualTo(BATCH.toLong())
        // The work sizes with the BATCH, not with the table. Derived
        // from the batch rather than written down.
        assertThat(node.buffers)
            .describedAs("%d buffers for a %d-row batch:%n%s", node.buffers, BATCH, victimPlan)
            .isLessThanOrEqualTo(BATCH.toLong())
        println("[#193] retirement victim select: ${node.buffers} buffers for a $BATCH-row batch")
    }

    @Test
    fun `both retirement deletes are driven by a primary key, never a sequential scan`() {
        // A Bitmap counts HERE, unlike in the victim select: the
        // statement names 8,000 explicit keys and deletes all of them,
        // so sorting the TIDs and walking the heap in physical order is
        // the BETTER plan. What must not happen is a sequential scan
        // per batch.
        assertThat(driverIndex(dataDeletePlan, "hog_data_file"))
            .describedAs("the data-file delete must be driven by the PK:%n%s", dataDeletePlan)
            .isEqualTo("hog_data_file_pkey")
        assertThat(dataDeletePlan)
            .describedAs("a per-batch sequential scan is the shape this excludes:%n%s", dataDeletePlan)
            .doesNotContain("Seq Scan on hog_data_file")

        // The DV arm has no `end_snapshot` clause on purpose — a
        // superseded vector must go too, or the data-file cascade takes
        // it away un-queued — so the PARTIAL
        // `hog_delete_file_one_live_per_data_file` cannot serve it. It
        // does not have to: `hog_delete_file_data_lookup` (V2) is
        // non-partial on exactly `(catalog_id, data_file_id)`, which is
        // both this statement's key and the FK cascade's.
        // `RetirementCostIntegrationTest` is where that index choice is
        // measured, against a populated relation; this fixture's
        // hog_delete_file is empty, so its plan says nothing.
        assertThat(RetirementService.DV_DELETE_SQL)
            .contains("data_file_id = ANY(:victims)")
            .doesNotContain("end_snapshot")
        println("[#193] retirement data-file delete plan:\n$dataDeletePlan")
        println("[#193] retirement DV delete plan:\n$dvDeletePlan")
    }
}
