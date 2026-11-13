package com.posthog.hoglake.hoglite

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Schema shape that the read paths depend on. The query plans were
 * checked with SQLite's EXPLAIN QUERY PLAN; these tests keep the shape.
 */
class HogliteSchemaTest {
    private fun normalized(sql: String) = sql.replace(Regex("\\s+"), " ").trim()

    /**
     * tableAt and the live table lookups filter table_versions on
     * (namespace_id, name). Without an index that starts with those
     * columns, SQLite scans every table and its retained versions.
     */
    @Test
    fun `table_versions has a non-partial index that starts with namespace_id and name`() {
        val index =
            HogliteSchema.DDL.map(::normalized).filter {
                it.startsWith("CREATE INDEX") && it.contains(" ON table_versions(namespace_id, name")
            }

        assertThat(index).hasSize(1)
        assertThat(index.single()).doesNotContain("WHERE")
    }

    /**
     * Namespace containment (Postgres V33): two nullable columns, the
     * both-or-neither CHECK, and at most one live namespace per value.
     * The index's WHERE matches the guards' terms, so they can use it.
     */
    @Test
    fun `namespaces carry containment with its shape check and live unique index`() {
        val ddl = normalized(HogliteSchema.DDL.single { it.contains("CREATE TABLE IF NOT EXISTS namespaces(") })
        assertThat(ddl).contains("contained_field TEXT,", "contained_value INTEGER,")
        assertThat(ddl).contains(
            "CHECK ((contained_field IS NULL AND contained_value IS NULL) OR (contained_field IS NOT NULL " +
                "AND contained_value IS NOT NULL AND contained_value > 0))",
        )

        val index = HogliteSchema.DDL.map(::normalized).filter { it.contains("namespaces_live_containment") }
        assertThat(index).containsExactly(
            "CREATE UNIQUE INDEX IF NOT EXISTS namespaces_live_containment ON namespaces(contained_field, " +
                "contained_value) WHERE dropped_snapshot IS NULL AND contained_field IS NOT NULL",
        )
    }

    /**
     * A column after a large value (stats_json is kilobytes on a wide
     * table) sits on that row's overflow pages, and SQLite walks the
     * overflow chain to read it. The visibility predicates read
     * begin_snapshot and end_snapshot on every row a scan touches, so
     * they and the other fixed-width columns come before the JSON values.
     * The order is free to choose before a tenant exists; after, it is a
     * table rewrite.
     */
    @Test
    fun `every fixed-width files column comes before the JSON columns`() {
        val ddl = HogliteSchema.DDL.single { it.contains("CREATE TABLE IF NOT EXISTS files(") }
        val columns =
            ddl.substringAfter("files(").substringBeforeLast(")").split(",").map { it.trim().split(" ") }
        val names = columns.map { it[0] }
        val integers = columns.filter { it[1] == "INTEGER" }.map { names.indexOf(it[0]) }
        val jsons = names.withIndex().filter { it.value.endsWith("_json") }.map { it.index }

        assertThat(names).contains("begin_snapshot", "end_snapshot", "stats_json")
        assertThat(jsons).isNotEmpty()
        assertThat(integers.max()).describedAs("files columns: %s", names).isLessThan(jsons.min())
    }

    @Test
    fun `listing and changefeed keyset indexes include table and every continuation key`() {
        val indexes = HogliteSchema.DDL.map(::normalized)
        assertThat(indexes).contains(
            "CREATE INDEX IF NOT EXISTS files_listing ON files(table_id, begin_snapshot, row_id_start, id)",
            "CREATE INDEX IF NOT EXISTS dvs_changes ON dvs(table_id, begin_snapshot, delete_file_id)",
        )
    }

    @Test
    fun `receipt retention has a non-partial covering index for cutoff and ordering`() {
        val indexes = HogliteSchema.DDL.map(::normalized).filter { it.contains(" ON receipts(") }
        assertThat(indexes).containsExactly(
            "CREATE INDEX IF NOT EXISTS receipts_created_at ON receipts(created_at, idempotency_key)",
        )
    }

    @Test
    fun `ledger retention indexes match the cutoff ordering and cover row ids`() {
        assertThat(HogliteSchema.DDL.map(::normalized)).contains(
            "CREATE INDEX IF NOT EXISTS removals_drained_at ON removals(drained_at, id) WHERE drained_at IS NOT NULL",
            "CREATE INDEX IF NOT EXISTS maintenance_runs_started_at ON maintenance_runs(started_at, id)",
        )
    }

    @Test
    fun `queue arm indexes walk only their own undrained id range`() {
        assertThat(HogliteSchema.DDL.map(::normalized)).contains(
            "CREATE INDEX IF NOT EXISTS removals_staging_queue ON removals(id) " +
                "WHERE drained_at IS NULL AND reason = 'compaction_staging'",
            "CREATE INDEX IF NOT EXISTS removals_bulk_queue ON removals(id) " +
                "WHERE drained_at IS NULL AND (reason IS NULL OR reason <> 'compaction_staging')",
        )
    }
}
