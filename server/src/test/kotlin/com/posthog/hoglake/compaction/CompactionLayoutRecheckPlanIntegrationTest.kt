package com.posthog.hoglake.compaction

import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.ExplainPlan
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
 * `CompactionService.LAYOUT_RECHECK_SQL`, the one statement 2.1d adds
 * under the per-catalog commit lock, runs on EVERY group commit, option
 * on or off: it must be a handful of index probes whatever the table's
 * history, never a walk of it.
 *
 * The fixture is a catalog of 1,000 filler namespaces holding 2,000
 * filler tables (10 live columns each, the option on for half of them,
 * so every relation the statement reads has rows to choose an index
 * over), an option-off table `plain`, an option-on table `events` with
 * history, and an option-on table `orphaned` whose prefix column is not
 * live. `events` has 1,000 ended `hog_table_version` rows, 500 ended
 * versions of the prefix column (what renames, promotes and
 * `set_column_comment` leave until expiry clears them), 10 ended layout
 * rows and 200 live columns besides its own two. Every history row is
 * OLDER than the live row it precedes, as the DDL writes them.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CompactionLayoutRecheckPlanIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val alter = AlterService(db.jdbi)
    private val cat = "recheck-plan"
    private var catalogId = 0L

    /** Resolved before the history is rewritten: GET cannot resolve a table whose live rows begin past head. */
    private val tableIds = HashMap<String, Long>()

    @AfterAll
    fun tearDown() = db.close()

    private fun tableId(name: String) = tableIds.getValue(name)

    @BeforeAll
    fun seed() {
        catalogs.createCatalog(cat, "s3://bucket/$cat")
        catalogs.createNamespace(cat, "ns")
        val columns = listOf(ColumnDef("team_id", ColType.LONG), ColumnDef("v", ColType.LONG))
        for (table in listOf("plain", "events", "orphaned")) {
            catalogs.createTable(cat, "ns", table, columns)
            if (table != "plain") {
                alter.alterTable(
                    cat,
                    "ns",
                    table,
                    listOf(
                        AlterOp.SetPartitionSpec(listOf(PartitionFieldDef(1, Transform.IDENTITY))),
                        AlterOp.SetObjectPrefixField("team_id"),
                    ),
                )
            }
            tableIds[table] = catalogs.getTable(cat, "ns", table).tableId
        }
        val events = tableId("events")
        val orphaned = tableId("orphaned")
        db.jdbi.useHandleUnchecked { h ->
            catalogId =
                h.createQuery("SELECT catalog_id FROM hog_catalog WHERE name = :c")
                    .bind("c", cat)
                    .mapTo(Long::class.java)
                    .one()
            seedFillers(h)
            // `events`' history, all of it older than the live rows: the
            // live table version and prefix column move up to snapshot
            // 2,000,000, and their ended predecessors fill the range below.
            update(h, "UPDATE hog_table_version SET begin_snapshot = 2000000 WHERE $LIVE", events)
            update(h, "UPDATE hog_column SET begin_snapshot = 2000000 WHERE field_id = 1 AND $LIVE", events)
            update(
                h,
                "INSERT INTO hog_table_version (catalog_id, table_id, begin_snapshot, end_snapshot, " +
                    "namespace_id, name) SELECT :c, :t, 1000000 + s, 1000001 + s, namespace_id, 'events' " +
                    "FROM hog_namespace, generate_series(0, 999) s WHERE catalog_id = :c AND name = 'ns'",
                events,
            )
            update(
                h,
                "INSERT INTO hog_column (catalog_id, table_id, field_id, begin_snapshot, end_snapshot, name, " +
                    "col_type, ordinal) SELECT :c, :t, 1, 1000000 + s, 1000001 + s, 'team_' || s, 'long', 0 " +
                    "FROM generate_series(0, 499) s",
                events,
            )
            update(
                h,
                "INSERT INTO hog_table_layout (catalog_id, table_id, begin_snapshot, end_snapshot, " +
                    "object_prefix_field_id) SELECT :c, :t, -100 + s, -99 + s, 1 FROM generate_series(0, 9) s",
                events,
            )
            update(
                h,
                "INSERT INTO hog_column (catalog_id, table_id, field_id, begin_snapshot, name, col_type, ordinal) " +
                    "SELECT :c, :t, f, 0, 'c' || f, 'long', f - 1 FROM generate_series(3, 202) f",
                events,
            )
            // `orphaned`: its prefix column ended (a broken invariant the DDL
            // refuses), the row still there.
            update(
                h,
                "UPDATE hog_column SET end_snapshot = (SELECT last_snapshot_id FROM hog_catalog WHERE " +
                    "catalog_id = :c) WHERE field_id = 1 AND $LIVE",
                orphaned,
            )
            for (relation in RELATIONS) h.execute("VACUUM (ANALYZE) $relation")
        }
    }

    private fun update(
        h: Handle,
        sql: String,
        tableId: Long,
    ) {
        h.createUpdate(sql).bind("c", catalogId).bind("t", tableId).execute()
    }

    /** 1,000 namespaces, 2,000 tables of 10 columns, the option on every other one. */
    private fun seedFillers(h: Handle) {
        h.createUpdate(
            "INSERT INTO hog_namespace (catalog_id, namespace_id, name) " +
                "SELECT :c, 100000 + n, 'filler_ns_' || n FROM generate_series(1, 1000) n",
        ).bind("c", catalogId).execute()
        h.createUpdate(
            "INSERT INTO hog_table (catalog_id, table_id, created_snapshot) " +
                "SELECT :c, 100000 + g, 0 FROM generate_series(1, 2000) g",
        ).bind("c", catalogId).execute()
        h.createUpdate(
            "INSERT INTO hog_table_version (catalog_id, table_id, begin_snapshot, namespace_id, name) " +
                "SELECT :c, 100000 + g, 0, 100001 + g % 1000, 'filler_' || g FROM generate_series(1, 2000) g",
        ).bind("c", catalogId).execute()
        h.createUpdate(
            "INSERT INTO hog_column (catalog_id, table_id, field_id, begin_snapshot, name, col_type, ordinal) " +
                "SELECT :c, 100000 + g, f, 0, 'c' || f, 'long', f - 1 " +
                "FROM generate_series(1, 2000) g, generate_series(1, 10) f",
        ).bind("c", catalogId).execute()
        h.createUpdate(
            "INSERT INTO hog_table_layout (catalog_id, table_id, begin_snapshot, object_prefix_field_id) " +
                "SELECT :c, 100000 + g, 0, 1 FROM generate_series(1, 2000, 2) g",
        ).bind("c", catalogId).execute()
    }

    /** EXPLAIN (ANALYZE, BUFFERS) of the shipped statement, on its second (warm) run. */
    private fun explain(tableId: Long): String =
        db.jdbi.withHandleUnchecked { h ->
            h.begin()
            try {
                h.execute("SET LOCAL max_parallel_workers_per_gather = 0")
                val run = {
                    h.createQuery("EXPLAIN (ANALYZE, BUFFERS, TIMING false, COSTS false, SUMMARY false) $RECHECK")
                        .bind("catalogId", catalogId)
                        .bind("tableId", tableId)
                        .mapTo(String::class.java)
                        .list()
                        .joinToString("\n")
                }
                run()
                run()
            } finally {
                h.rollback()
            }
        }

    /** What the statement answers for [tableId]: (field name, table name) per row. */
    private fun answer(tableId: Long): List<Pair<String?, String>> =
        db.jdbi.withHandleUnchecked { h ->
            h.createQuery(RECHECK)
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .map { rs, _ -> rs.getString("field_name") to rs.getString("table_name") }
                .list()
        }

    private fun rows(plan: String): Long =
        Regex("""actual rows=([\d.]+)""").find(plan)!!.groupValues[1].toDouble().toLong()

    private fun buffers(plan: String): Long = ExplainPlan.nodes(plan).first().buffers

    @Test
    fun `an option-off table costs one empty probe of the live layout index`() {
        val plan = explain(tableId("plain"))
        assertThat(plan).describedAs(plan).contains("hog_table_layout_live").doesNotContain("Seq Scan")
        assertThat(rows(plan)).describedAs(plan).isZero()
        assertThat(buffers(plan)).describedAs(plan).isLessThanOrEqualTo(4)
        println("RECORDED layout recheck, option off: buffers ${buffers(plan)}")
    }

    @Test
    fun `an option-on table with history costs a few live and primary-key probes`() {
        // MUTATION: drop the table version's liveness term and the
        // statement returns its 1,000 ended versions; drop the layout's and
        // it returns 11 rows; drop the column's ORDER BY ... LIMIT 1 and it
        // filters the field's 500 ended versions (15 buffers on the probe).
        val plan = explain(tableId("events"))
        assertThat(rows(plan)).describedAs(plan).isEqualTo(1)
        assertThat(plan)
            .describedAs(plan)
            .contains("hog_table_layout_live")
            .contains("hog_table_version_live")
            .contains("hog_namespace_pkey")
            .contains("Index Scan Backward using hog_column_pkey")
            .doesNotContain("Seq Scan")
            .doesNotContain("Rows Removed by Filter")
        assertThat(buffers(plan)).describedAs(plan).isLessThanOrEqualTo(15)
        assertThat(answer(tableId("events"))).containsExactly("team_id" to "events")
        println("RECORDED layout recheck, option on with history: buffers ${buffers(plan)}")
    }

    @Test
    fun `a prefix column that is not live answers with no name`() {
        // MUTATION: drop the column's liveness term and the ended column's
        // name comes back, and the recheck would rebuild a prefix from a
        // column the table no longer has.
        assertThat(answer(tableId("orphaned"))).containsExactly(null to "orphaned")
    }

    private companion object {
        const val LIVE = "catalog_id = :c AND table_id = :t AND end_snapshot IS NULL"
        val RELATIONS = listOf("hog_table", "hog_table_version", "hog_column", "hog_table_layout", "hog_namespace")
        val RECHECK = CompactionService.LAYOUT_RECHECK_SQL
    }
}
