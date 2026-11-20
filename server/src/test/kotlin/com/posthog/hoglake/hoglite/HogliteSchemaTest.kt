package com.posthog.hoglake.hoglite

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Schema shape that the read paths depend on, and the referential
 * integrity the write paths rely on. The query plans were checked with
 * SQLite's EXPLAIN QUERY PLAN; these tests keep the shape.
 */
class HogliteSchemaTest {
    private fun normalized(sql: String) = sql.replace(Regex("\\s+"), " ").trim()

    private fun createTable(name: String): String =
        HogliteSchema.DDL.single { it.contains("CREATE TABLE IF NOT EXISTS $name(") }

    private fun table(name: String): String = normalized(createTable(name))

    /** A CREATE TABLE's column definitions: the body split on top-level commas, table constraints dropped. */
    private fun columnDefinitions(ddl: String): List<String> {
        val body = normalized(ddl).substringAfter("(").substringBeforeLast(")")
        val parts = mutableListOf<String>()
        val current = StringBuilder()
        var depth = 0
        for (ch in body) {
            when {
                ch == '(' -> depth++
                ch == ')' -> depth--
                ch == ',' && depth == 0 -> {
                    parts += current.toString().trim()
                    current.clear()
                    continue
                }
            }
            current.append(ch)
        }
        parts += current.toString().trim()
        return parts.filterNot { part ->
            listOf("CHECK", "CONSTRAINT", "PRIMARY KEY", "UNIQUE", "FOREIGN KEY").any { part.startsWith(it) }
        }
    }

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
     * both-or-neither CHECK with the field regex as GLOB, and at most one
     * live namespace per value. The index's WHERE matches the guards'
     * terms, so they can use it.
     */
    @Test
    fun `namespaces carry containment with its shape check and live unique index`() {
        val ddl = table("namespaces")
        assertThat(ddl).contains("contained_field TEXT,", "contained_value INTEGER,")
        assertThat(ddl).contains(
            "CHECK ((contained_field IS NULL AND contained_value IS NULL) OR (contained_field IS NOT NULL " +
                "AND contained_value IS NOT NULL AND contained_value > 0 AND contained_field GLOB '[a-z]*' " +
                "AND NOT contained_field GLOB '*[^a-z0-9_]*' AND length(contained_field) <= 63))",
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
        val columns = columnDefinitions(createTable("files")).map { it.split(" ") }
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
                "WHERE drained_at IS NULL AND reason <> 'compaction_staging'",
        )
    }

    @Test
    fun `status indexes cover live backlogs and bounded loop reads`() {
        assertThat(HogliteSchema.DDL.map(::normalized)).contains(
            "CREATE INDEX IF NOT EXISTS files_live_stats ON files(stats_state) WHERE end_snapshot IS NULL",
            "CREATE INDEX IF NOT EXISTS files_live_size ON files(file_size_bytes) WHERE end_snapshot IS NULL",
            "CREATE INDEX IF NOT EXISTS removals_queued_at ON removals(queued_at) WHERE drained_at IS NULL",
            "CREATE INDEX IF NOT EXISTS maintenance_runs_loop ON maintenance_runs(task, id, started_at) " +
                "WHERE run_trigger = 'loop'",
        )
    }

    // ---- referential integrity (schema.sql's, for one tenant) -------------

    /**
     * Postgres's `FOREIGN KEY (catalog_id, table_id) REFERENCES hog_table ON
     * DELETE CASCADE` on every table-scoped row, re-keyed for one tenant.
     * A missing edge is an orphan the engine would accept.
     */
    @Test
    fun `every table-scoped row references tables with a cascade`() {
        for (child in TABLE_CHILDREN) {
            assertThat(table(child))
                .describedAs(child)
                .containsPattern("table_id INTEGER (NOT NULL|PRIMARY KEY) REFERENCES tables\\(id\\) ON DELETE CASCADE")
        }
    }

    /** hog_delete_file -> hog_data_file, hog_snapshot_change -> hog_snapshot, and the receipt's ended count. */
    @Test
    fun `vectors, change rows and ended counts die with their parent row`() {
        assertThat(table("dvs")).contains("data_file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE")
        assertThat(table("changes")).contains("snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE")
        assertThat(normalized(HogliteSchema.RECEIPT_ENDED_DDL))
            .contains("idempotency_key TEXT PRIMARY KEY REFERENCES receipts(idempotency_key) ON DELETE CASCADE")
    }

    /**
     * hog_table_version's and hog_view's namespace edges: no action (a
     * namespace with history under it cannot be deleted), deferred so the
     * order of a transaction's statements does not matter. `tables` carries
     * the same edge for its denormalized namespace_id. No other edge is
     * deferred: an immediate check names the failing statement.
     */
    @Test
    fun `namespace edges are deferred no-action references`() {
        for (name in listOf("tables", "table_versions", "views")) {
            assertThat(table(name))
                .describedAs(name)
                .contains("namespace_id INTEGER NOT NULL REFERENCES namespaces(id) DEFERRABLE INITIALLY DEFERRED")
                .doesNotContain("namespaces(id) ON DELETE")
        }
        val deferred = HogliteSchema.DDL.map(::normalized).sumOf { Regex("DEFERRABLE").findAll(it).count() }
        assertThat(deferred).isEqualTo(3)
    }

    /**
     * SQLite checks a cascade by scanning the child table for the parent's
     * key unless an index leads with the child column, so every cascading
     * edge has one (the primary key, or a non-partial index). The deferred
     * namespace edges are exempt: namespace rows are never deleted.
     */
    @Test
    fun `every cascading edge has an index led by its child column`() {
        val edges =
            TABLE_CHILDREN.map { it to "table_id" } +
                listOf("dvs" to "data_file_id", "changes" to "snapshot_id", "receipt_ended" to "idempotency_key")
        val indexes = HogliteSchema.DDL.map(::normalized).filter { it.contains(" INDEX ") && !it.contains(" WHERE ") }
        for ((name, column) in edges) {
            val ddl = if (name == "receipt_ended") normalized(HogliteSchema.RECEIPT_ENDED_DDL) else table(name)
            val primaryKeyLeads =
                Regex("PRIMARY KEY\\($column[,)]").containsMatchIn(ddl) ||
                    ddl.contains("$column INTEGER PRIMARY KEY") ||
                    ddl.contains("$column TEXT PRIMARY KEY")
            val indexLeads = indexes.any { it.contains(" ON $name($column,") || it.contains(" ON $name($column)") }
            assertThat(primaryKeyLeads || indexLeads).describedAs("$name.$column").isTrue()
        }
    }

    /** `[begin_snapshot, end_snapshot)` is never empty, on every versioned table, as on Postgres. */
    @Test
    fun `every versioned table refuses an empty snapshot window`() {
        for (name in TABLE_CHILDREN - "table_totals") {
            assertThat(table(name)).describedAs(name).contains(HogliteSchema.WINDOW_CHECK)
        }
        assertThat(table("views")).contains("CHECK (dropped_snapshot IS NULL OR dropped_snapshot > created_snapshot)")
    }

    /**
     * The identifier regexes SQLite cannot run, as GLOB + length: a first
     * character of the right class, no character outside the class, and
     * the length bound. Applied to every name column Postgres checks.
     */
    @Test
    fun `identifier policy is spelled as the exact GLOB equivalent on every name column`() {
        assertThat(HogliteSchema.identifier("name"))
            .isEqualTo("(name GLOB '[A-Za-z_]*' AND NOT name GLOB '*[^A-Za-z0-9_-]*' AND length(name) <= 128)")
        for (name in listOf("namespaces", "tables", "table_versions", "columns", "views")) {
            assertThat(table(name))
                .describedAs(name)
                .contains("name TEXT NOT NULL CHECK ${HogliteSchema.identifier("name")}")
        }
        assertThat(table("meta")).contains("name TEXT NOT NULL CHECK ${HogliteSchema.CATALOG_NAME_CHECK}")
        assertThat(HogliteSchema.CATALOG_NAME_CHECK)
            .isEqualTo("(name GLOB '[a-z0-9]*' AND NOT name GLOB '*[^a-z0-9_-]*' AND length(name) <= 63)")
    }

    /**
     * A CHECK changes only by rebuilding its table, and rebuilding an FK
     * parent under enforcement cascades into its children, so a vocabulary
     * (which grows) never sits on a parent. hog_data_file's file_format and
     * stats_state lists are the ones deliberately left out.
     */
    @Test
    fun `foreign-key parents carry no vocabulary check`() {
        for (parent in listOf("tables", "files", "snapshots", "namespaces", "receipts")) {
            assertThat(table(parent)).describedAs(parent).doesNotContain(" IN (")
        }
        assertThat(table("files")).contains(
            "stats_state TEXT NOT NULL,",
            "file_format TEXT NOT NULL DEFAULT 'parquet',",
        )
    }

    /** schema.sql's closed vocabularies and sign checks, on the leaf tables that keep those columns. */
    @Test
    fun `closed vocabularies and sign checks match schema sql`() {
        assertThat(table("changes")).contains("kind TEXT NOT NULL CHECK (kind IN ('namespace_created',")
        assertThat(table("columns")).contains(
            "ordinal INTEGER NOT NULL CHECK (ordinal >= 0)",
            "type TEXT NOT NULL CHECK (type IN ('boolean',",
            "'map'))",
            "CHECK (parent_field_id IS NULL OR parent_field_id <> field_id)",
            "CHECK (parent_field_id IS NULL OR parent_field_id < field_id)",
        )
        assertThat(table("tables")).contains(
            "next_row_id INTEGER NOT NULL DEFAULT 0 CHECK (next_row_id >= 0)",
            "replaced_table_id INTEGER CHECK (replaced_table_id <> id)",
        )
        assertThat(table("table_totals"))
            .contains("file_size_bytes INTEGER NOT NULL DEFAULT 0 CHECK (file_size_bytes >= 0)")
        assertThat(table("files")).contains(
            "record_count INTEGER NOT NULL CHECK (record_count >= 0)",
            "file_size_bytes INTEGER NOT NULL CHECK (file_size_bytes >= 0)",
        )
        assertThat(table("dvs")).contains(
            "delete_count INTEGER NOT NULL CHECK (delete_count > 0)",
            "file_size_bytes INTEGER NOT NULL CHECK (file_size_bytes >= 0)",
        )
        assertThat(table("meta"))
            .contains("CHECK (snapshot_retention_seconds IS NULL OR snapshot_retention_seconds > 0)")
        assertThat(table("offsets")).contains("consumer_id TEXT NOT NULL CHECK (length(consumer_id) BETWEEN 1 AND 128)")
        assertThat(table("uploads")).contains(
            "file_kind TEXT NOT NULL CHECK (file_kind IN ('data', 'delete'))",
            "state TEXT NOT NULL CHECK (state IN ('active', 'registered', 'abandoned'))",
        )
        assertThat(table("table_creations")).contains(
            "state TEXT NOT NULL CHECK (state IN ('prepared', 'committed', 'rejected', 'aborted'))",
            "CHECK ((state = 'committed') = (snapshot_id IS NOT NULL AND schema_version IS NOT NULL))",
        )
        assertThat(table("removals")).contains(
            "file_kind TEXT NOT NULL CHECK (file_kind IN ('data', 'delete'))",
            "reason TEXT NOT NULL CHECK (reason IN ('snapshot_expiry', 'table_drop_gc', " +
                "'compaction_staging', 'trino_upload'))",
            "drained_outcome TEXT CHECK (drained_outcome IN ('deleted', 'absent', 'registered'))",
            "CHECK ((drained_at IS NULL) = (drained_outcome IS NULL))",
        )
        assertThat(table("maintenance_runs")).contains(
            "task TEXT NOT NULL CHECK (task IN ('hydrator', 'expiry', 'cleanup', 'compaction', 'verify', " +
                "'retirement', 'reindex'))",
            "run_trigger TEXT NOT NULL CHECK (run_trigger IN ('loop', 'manual'))",
            "status TEXT NOT NULL CHECK (status IN ('ok', 'failed'))",
            "CHECK (finished_at >= started_at)",
            "CHECK ((status = 'failed') = (error IS NOT NULL))",
        )
    }

    companion object {
        /** The tenant tables keyed by a table: hog_table's cascading children. */
        private val TABLE_CHILDREN =
            listOf(
                "table_versions",
                "columns",
                "partition_specs",
                "sort_specs",
                "table_layouts",
                "table_totals",
                "files",
                "dvs",
            )
    }
}
