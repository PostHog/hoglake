package com.posthog.hoglake.hoglite

import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/**
 * The SQLite schema every hoglake catalog carries inside its hoglite
 * tenant, plus the bootstrap that lays it down.
 *
 * This is hoglake's catalog model re-expressed for one tenant, not a
 * new model: one `meta` row standing in for `hog_catalog`, tables and
 * namespaces with created/dropped snapshot windows, files with server-
 * assigned row lineage, history-preserving deletion vectors (a new
 * epoch supersedes; a pinned scan reads the epoch that was live at its
 * snapshot), a typed change feed, and receipts for idempotent
 * publication. The differences are the ones hoglite's shape dictates:
 *
 *  - Allocators (next_snapshot, next_table_id, next_file_id,
 *    next_row_id per table) are counters on rows, advanced by guarded
 *    UPDATEs inside the transaction — hoglite serializes each tenant's
 *    transactions, which is what hoglake's per-catalog advisory lock
 *    gave Postgres.
 *  - Files and deletion vectors carry end_snapshot from day one, so
 *    truncate, expiry, and compaction are writers of a column that
 *    every read predicate already honors — not a schema migration.
 *  - Timestamps are fixed-width UTC text (millis), so lexical order IS
 *    chronological order in SQLite. [fmt]/[parse] are the only codec.
 *
 * All DDL is `IF NOT EXISTS` so the bootstrap transaction is
 * idempotent, and hoglite's (client, txid) dedup makes a replayed
 * bootstrap a no-op read.
 *
 * ## Referential integrity
 *
 * The schema declares what `server/schema.sql` declares, re-keyed for
 * one tenant: every child-of-table row (`table_versions`, `columns`,
 * `partition_specs`, `sort_specs`, `table_layouts`, `table_totals`,
 * `files`, `dvs`) REFERENCES `tables(id)` ON DELETE CASCADE; `dvs`
 * also REFERENCES `files(id)` ON DELETE CASCADE (hog_delete_file ->
 * hog_data_file); `changes` REFERENCES `snapshots(id)` ON DELETE
 * CASCADE (hog_snapshot_change -> hog_snapshot); `table_versions`,
 * `views` and the denormalized `tables.namespace_id` REFERENCE
 * `namespaces(id)` DEFERRABLE INITIALLY DEFERRED with no action, as
 * hog_table_version and hog_view do (a namespace row with versioned
 * history under it cannot be deleted; deferred so a transaction's
 * statement order does not matter). `receipt_ended` REFERENCES
 * `receipts` ON DELETE CASCADE, because on Postgres the ended count is
 * a column of the receipt row and dies with it. Postgres's thirteen
 * `catalog_id REFERENCES hog_catalog ON DELETE CASCADE` edges have no
 * counterpart: the tenant IS the catalog, and dropping it deletes the
 * tenant. The per-file child tables (hog_file_column_stats,
 * hog_file_partition_value, hog_partition_field, hog_sort_field) are
 * JSON columns here and carry no FK of their own.
 *
 * The CHECK constraints are Postgres's, except the four `~` regexes,
 * which SQLite cannot run: the identifier policy (catalog, namespace,
 * table, column and view names, containment field) is spelled as the
 * exact GLOB + length() equivalent instead ([identifier],
 * [CATALOG_NAME_CHECK], [CONTAINMENT_CHECK]). Vocabularies (change
 * kinds, column types, removal reasons, maintenance tasks, upload and
 * creation states) are the literal lists of schema.sql, not the Kotlin
 * enums, so a new member needs the same two-sided change it needs there.
 * They sit on LEAF tables only. An FK parent (`tables`, `files`,
 * `snapshots`, `namespaces`, `receipts`) carries no vocabulary CHECK:
 * SQLite changes a CHECK only by rebuilding the table, and rebuilding a
 * parent under enforcement cascades (DROP TABLE deletes every child row,
 * RENAME rewrites the children's references) while hoglite refuses the
 * PRAGMA that would switch enforcement off for the rebuild. So
 * hog_data_file's `file_format` and `stats_state` lists are not here;
 * the store writes those two columns from closed Kotlin sets.
 *
 * hoglite runs every tenant connection with `PRAGMA foreign_keys = ON`
 * and lets no client turn it off, so the constraints hold on every
 * write: a violating statement is refused (422 `refused`, "FOREIGN KEY
 * constraint failed"), a deferred violation fails its transaction at
 * commit, and ON DELETE CASCADE runs. The store checks that once, at
 * bootstrap: it reads `pragma_foreign_keys` on a new tenant and again as
 * a guard of the bootstrap transaction, and refuses a tenant whose
 * connection does not report 1 ([HogliteCatalogStore.createCatalog]),
 * and a FOREIGN KEY refusal of any later transaction is a store bug,
 * not a client error ([HogliteIntegrityViolation]). Statement order
 * inside a transaction therefore matters: a parent row goes in before
 * its children (the snapshot row before its change rows, the receipt
 * before its `receipt_ended` row, the table before its files), and the
 * maintenance deletes (expiry, retirement) still delete and queue each
 * child explicitly before its parent, so what they queue for removal
 * never depends on a cascade.
 *
 * There are no tenant migrations. [DDL] is the first released schema.
 * The first schema change after release must also upgrade the tenants
 * that exist, before any operation reads the new shape. Until then, a
 * schema change (this one included) means: drop and recreate every
 * development tenant, because `CREATE TABLE IF NOT EXISTS` leaves an
 * existing table as it was, constraints included. After release, adding
 * a vocabulary member (a maintenance task, a change kind, a removal
 * reason, a column type, an upload or creation state) is such a change:
 * the leaf table that holds the CHECK must be rebuilt on every tenant
 * before a server that writes the new member deploys, exactly as
 * Postgres needs its migration first. FK-parent tables must never gain a
 * vocabulary CHECK, for the reason above.
 */
internal object HogliteSchema {
    /**
     * Identifiers.PATTERN (`^[A-Za-z_][A-Za-z0-9_-]{0,127}$`) as a CHECK
     * expression: first character a letter or underscore, every character
     * in `[A-Za-z0-9_-]`, at most 128 of them. GLOB is case-sensitive and
     * `-` last in a class is literal, so the three terms are the regex for
     * any text SQLite stores as a string. The one gap: GLOB reads a value
     * only up to an embedded NUL while length() counts past it, so a name
     * with a NUL after a valid prefix passes here where Postgres (which
     * stores no NUL in text at all) refuses it. The store's Kotlin
     * validation ([com.posthog.hoglake.service.Identifiers]) runs first and
     * refuses it, on both backends.
     */
    internal fun identifier(column: String): String =
        "($column GLOB '[A-Za-z_]*' AND NOT $column GLOB '*[^A-Za-z0-9_-]*' AND length($column) <= 128)"

    /** Identifiers.CATALOG_NAME_PATTERN (`^[a-z0-9][a-z0-9_-]{0,62}$`), the same way. */
    const val CATALOG_NAME_CHECK =
        "(name GLOB '[a-z0-9]*' AND NOT name GLOB '*[^a-z0-9_-]*' AND length(name) <= 63)"

    /**
     * hog_namespace_containment_shape: both or neither, a positive value,
     * and the field's `^[a-z][a-z0-9_]{0,62}$` spelled as GLOB + length.
     */
    const val CONTAINMENT_CHECK =
        "((contained_field IS NULL AND contained_value IS NULL) " +
            "OR (contained_field IS NOT NULL AND contained_value IS NOT NULL AND contained_value > 0 " +
            "AND contained_field GLOB '[a-z]*' AND NOT contained_field GLOB '*[^a-z0-9_]*' " +
            "AND length(contained_field) <= 63))"

    /** A versioned row's window: `[begin_snapshot, end_snapshot)` is never empty. */
    const val WINDOW_CHECK = "CHECK (end_snapshot IS NULL OR end_snapshot > begin_snapshot)"

    /** hog_column.col_type's closed set, in schema.sql's order. */
    const val COLUMN_TYPES =
        "'boolean', 'int8', 'int16', 'int', 'long', 'uint8', 'uint16', 'uint32', 'uint64', 'float', 'double', " +
            "'decimal', 'date', 'time', 'timestamp_s', 'timestamp_ms', 'timestamp', 'timestamp_ns', 'timestamptz', " +
            "'string', 'json', 'uuid', 'binary', 'variant', 'list', 'struct', 'map'"

    /** hog_snapshot_change.kind's closed vocabulary. */
    const val CHANGE_KINDS =
        "'namespace_created', 'namespace_dropped', 'table_created', 'table_dropped', 'table_altered', " +
            "'table_inserted_into', 'table_deleted_from', 'table_compacted', 'table_replaced', " +
            "'view_created', 'view_dropped'"

    /** hog_file_removal.reason's vocabulary. */
    const val REMOVAL_REASONS = "'snapshot_expiry', 'table_drop_gc', 'compaction_staging', 'trino_upload'"

    /** hog_maintenance_run.task's vocabulary. */
    const val MAINTENANCE_TASKS = "'hydrator', 'expiry', 'cleanup', 'compaction', 'verify', 'retirement', 'reindex'"

    /** The `meta` row's fixed id — one catalog, one row. */
    const val META_ID = 1L

    /** Snapshot 0: the empty catalog, as hoglake's createCatalog writes it. */
    const val SNAPSHOT_ZERO = 0L

    /** head = next_snapshot - 1, as an SQL expression over the meta row. */
    const val HEAD_SQL = "(SELECT next_snapshot - 1 FROM meta WHERE id = 1)"

    /** Numeric storage-execution time, using SQLite's timestamp parser. */
    const val CREATION_NOW_SQL = "julianday('now')"

    /**
     * The data files a replacement ended, by its idempotency key: what a replay and
     * GET /commit/receipts answer with. A table of its own because tenants have no
     * migrations, so the column Postgres added to its receipts is not available;
     * every replacement creates it if missing, and a reader checks it exists.
     */
    const val RECEIPT_ENDED_DDL =
        """CREATE TABLE IF NOT EXISTS receipt_ended(
                idempotency_key TEXT PRIMARY KEY REFERENCES receipts(idempotency_key) ON DELETE CASCADE,
                ended_files INTEGER NOT NULL
            )"""

    /*
     * The object-prefix layout (`set_object_prefix_field`), Postgres V29's
     * hog_table_layout: versioned like the spec rows, at most one live row
     * per table (the unique partial index). No unset op exists, so nothing
     * ends a layout row today. Expiry clears ended versioned definitions;
     * retirement drains dropped tables' files and vectors, not their identity
     * or layout rows.
     */
    const val TABLE_LAYOUTS_DDL =
        """CREATE TABLE IF NOT EXISTS table_layouts(
                table_id INTEGER NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
                begin_snapshot INTEGER NOT NULL,
                end_snapshot INTEGER,
                object_prefix_field_id INTEGER NOT NULL,
                PRIMARY KEY(table_id, begin_snapshot),
                $WINDOW_CHECK
            )"""

    const val TABLE_LAYOUTS_LIVE_DDL =
        "CREATE UNIQUE INDEX IF NOT EXISTS table_layouts_live ON table_layouts(table_id) WHERE end_snapshot IS NULL"

    // V16/V17: path probes must scale with the probe, not the retained
    // manifest or 30-day ledger. File paths are deliberately NOT partial:
    // ended files still protect their objects from cleanup. V19: expiry
    // pages visit ended rows, not the live manifest. Tenant == catalog_id.
    val SCALE_INDEXES =
        listOf(
            "CREATE INDEX IF NOT EXISTS files_path ON files(path)",
            "CREATE INDEX IF NOT EXISTS files_compaction_bucket ON files(" +
                "table_id, COALESCE(spec_id, -1), COALESCE(partition_values_json, ''), file_size_bytes, id) " +
                "WHERE end_snapshot IS NULL",
            "CREATE INDEX IF NOT EXISTS files_live_stats ON files(stats_state) WHERE end_snapshot IS NULL",
            "CREATE INDEX IF NOT EXISTS files_live_size ON files(file_size_bytes) WHERE end_snapshot IS NULL",
            "CREATE INDEX IF NOT EXISTS removals_queued_at ON removals(queued_at) WHERE drained_at IS NULL",
            "CREATE INDEX IF NOT EXISTS maintenance_runs_loop ON maintenance_runs(task, id, started_at) " +
                "WHERE run_trigger = 'loop'",
            "CREATE INDEX IF NOT EXISTS maintenance_runs_status ON maintenance_runs(task, status, id)",
            "CREATE INDEX IF NOT EXISTS dvs_path ON dvs(path)",
            "CREATE INDEX IF NOT EXISTS removals_undrained_path ON removals(path) WHERE drained_at IS NULL",
            "CREATE INDEX IF NOT EXISTS files_ended ON files(end_snapshot) WHERE end_snapshot IS NOT NULL",
            "CREATE INDEX IF NOT EXISTS dvs_ended ON dvs(end_snapshot) WHERE end_snapshot IS NOT NULL",
            // Both ordering keys matter: compaction can reuse row_id_start.
            "CREATE INDEX IF NOT EXISTS files_scan ON files(table_id, row_id_start, id)",
            // First-release ordering indexes avoid a later tenant migration.
            "CREATE INDEX IF NOT EXISTS files_listing ON files(table_id, begin_snapshot, row_id_start, id)",
            "CREATE INDEX IF NOT EXISTS dvs_changes ON dvs(table_id, begin_snapshot, delete_file_id)",
        )

    /** At most one live deletion vector per data file. */
    const val DVS_ONE_LIVE_DDL =
        "CREATE UNIQUE INDEX IF NOT EXISTS dvs_one_live_per_data_file ON dvs(data_file_id) WHERE end_snapshot IS NULL"

    val DDL: List<String> =
        listOf(
            """CREATE TABLE IF NOT EXISTS meta(
                id INTEGER PRIMARY KEY CHECK(id = 1),
                name TEXT NOT NULL CHECK $CATALOG_NAME_CHECK,
                data_path TEXT NOT NULL,
                next_snapshot INTEGER NOT NULL DEFAULT 1,
                next_table_id INTEGER NOT NULL DEFAULT 1,
                next_namespace_id INTEGER NOT NULL DEFAULT 1,
                next_file_id INTEGER NOT NULL DEFAULT 1,
                next_view_id INTEGER NOT NULL DEFAULT 1,
                schema_version INTEGER NOT NULL DEFAULT 0,
                earliest_snapshot_id INTEGER NOT NULL DEFAULT 0,
                earliest_snapshot_time TEXT,
                snapshot_retention_seconds INTEGER
                    CHECK (snapshot_retention_seconds IS NULL OR snapshot_retention_seconds > 0),
                consumer_floor INTEGER NOT NULL DEFAULT 1,
                /* Set by dropCatalog's fencing transaction: every
                 * mutating tenant tx guards on it, so no write can land
                 * between the drop's emptiness checks and the tenant's
                 * deletion. */
                dropping INTEGER NOT NULL DEFAULT 0
            )""",
            /*
             * contained_field/contained_value: namespace containment, as
             * Postgres V33 adds it to hog_namespace. Not versioned: a read
             * at a past snapshot gets the current containment, as on
             * Postgres. The CHECK is V33's hog_namespace_containment_shape
             * with the field-name regex as GLOB ([CONTAINMENT_CHECK]); the
             * store still validates the field in Kotlin first, for the
             * message. V37's contained_org has no column here.
             */
            """CREATE TABLE IF NOT EXISTS namespaces(
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL CHECK ${identifier("name")},
                created_snapshot INTEGER NOT NULL,
                dropped_snapshot INTEGER,
                contained_field TEXT,
                contained_value INTEGER,
                CHECK $CONTAINMENT_CHECK
            )""",
            """CREATE UNIQUE INDEX IF NOT EXISTS namespaces_live
                ON namespaces(name) WHERE dropped_snapshot IS NULL""",
            // At most one live namespace per containment (V33's
            // hog_namespace_live_containment). A query uses it only when
            // its WHERE repeats both terms of the index's WHERE.
            """CREATE UNIQUE INDEX IF NOT EXISTS namespaces_live_containment
                ON namespaces(contained_field, contained_value)
                WHERE dropped_snapshot IS NULL AND contained_field IS NOT NULL""",
            /*
             * Identity (hog_table) plus a denormalized copy of the live
             * version's namespace and name, and hog_table_stats's row-id
             * allocator. namespace_id is the deferred no-action edge
             * hog_table_version carries; replaced_table_id is deliberately
             * not a FK (the referenced row is retired, never deleted, and
             * a cascade is the one thing it must not do), so the self-edge
             * CHECK is its only structural defence.
             */
            """CREATE TABLE IF NOT EXISTS tables(
                id INTEGER PRIMARY KEY,
                namespace_id INTEGER NOT NULL REFERENCES namespaces(id) DEFERRABLE INITIALLY DEFERRED,
                namespace TEXT NOT NULL CHECK ${identifier("namespace")},
                name TEXT NOT NULL CHECK ${identifier("name")},
                table_uuid TEXT NOT NULL UNIQUE,
                comment TEXT,
                properties_json TEXT NOT NULL DEFAULT '{}',
                next_row_id INTEGER NOT NULL DEFAULT 0 CHECK (next_row_id >= 0),
                next_field_id INTEGER NOT NULL DEFAULT 1,
                schema_version INTEGER NOT NULL DEFAULT 0,
                replaced_table_id INTEGER CHECK (replaced_table_id <> id),
                created_snapshot INTEGER NOT NULL,
                dropped_snapshot INTEGER
            )""",
            """CREATE UNIQUE INDEX IF NOT EXISTS tables_live
                ON tables(namespace, name) WHERE dropped_snapshot IS NULL""",
            /*
             * The table's (namespace, name, comment, properties) as an
             * interval — hog_table_version's shape. RENAME and metadata
             * sets end the live version and open a new one, so a
             * time-travelled read resolves the name the table HAD at
             * that snapshot. Identity (uuid, allocators, drop window)
             * stays on the tables row.
             */
            """CREATE TABLE IF NOT EXISTS table_versions(
                table_id INTEGER NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
                namespace_id INTEGER NOT NULL REFERENCES namespaces(id) DEFERRABLE INITIALLY DEFERRED,
                namespace TEXT NOT NULL CHECK ${identifier("namespace")},
                name TEXT NOT NULL CHECK ${identifier("name")},
                comment TEXT,
                properties_json TEXT NOT NULL DEFAULT '{}',
                begin_snapshot INTEGER NOT NULL,
                end_snapshot INTEGER,
                PRIMARY KEY(table_id, begin_snapshot),
                $WINDOW_CHECK
            )""",
            """CREATE UNIQUE INDEX IF NOT EXISTS table_versions_live
                ON table_versions(namespace, name) WHERE end_snapshot IS NULL""",
            /*
             * Name lookup: tableAt and the live lookups filter on
             * (namespace_id, name). The live index above is keyed on the
             * namespace TEXT, and the primary key starts with table_id, so
             * without this index SQLite scans every table and its retained
             * versions. end_snapshot last: head reads and commit planning
             * seek the live version, and a historical read stays bounded to
             * the one table's versions.
             */
            "CREATE INDEX IF NOT EXISTS table_versions_name ON table_versions(namespace_id, name, end_snapshot)",
            /*
             * Column VERSIONS, one row per column node (top-level or
             * nested) per interval of visibility — hog_column's shape.
             * A column dropped by an alter keeps its rows with
             * end_snapshot set, so a time-travelled read still resolves
             * the stats of files written under the old shape.
             */
            """CREATE TABLE IF NOT EXISTS columns(
                table_id INTEGER NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
                field_id INTEGER NOT NULL,
                parent_field_id INTEGER,
                ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
                name TEXT NOT NULL CHECK ${identifier("name")},
                type TEXT NOT NULL CHECK (type IN ($COLUMN_TYPES)),
                type_params_json TEXT,
                nullable INTEGER NOT NULL,
                comment TEXT,
                begin_snapshot INTEGER NOT NULL,
                end_snapshot INTEGER,
                PRIMARY KEY(table_id, field_id, begin_snapshot),
                $WINDOW_CHECK,
                CONSTRAINT columns_parent_not_self CHECK (parent_field_id IS NULL OR parent_field_id <> field_id),
                CONSTRAINT columns_parent_precedes_child CHECK (parent_field_id IS NULL OR parent_field_id < field_id)
            )""",
            "CREATE INDEX IF NOT EXISTS columns_table ON columns(table_id, end_snapshot)",
            """CREATE TABLE IF NOT EXISTS partition_specs(
                table_id INTEGER NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
                spec_id INTEGER NOT NULL,
                fields_json TEXT NOT NULL,
                begin_snapshot INTEGER NOT NULL,
                end_snapshot INTEGER,
                PRIMARY KEY(table_id, spec_id),
                $WINDOW_CHECK
            )""",
            """CREATE TABLE IF NOT EXISTS sort_specs(
                table_id INTEGER NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
                spec_id INTEGER NOT NULL,
                fields_json TEXT NOT NULL,
                begin_snapshot INTEGER NOT NULL,
                end_snapshot INTEGER,
                PRIMARY KEY(table_id, spec_id),
                $WINDOW_CHECK
            )""",
            /*
             * The object-prefix layout (`set_object_prefix_field`), Postgres
             * V29's hog_table_layout: versioned like the spec rows, at most
             * one live row per table (the unique partial index). No unset op
             * exists, so nothing ends a row today; expiry clears ended rows
             * like the other versioned definitions, and the dropped-table
             * purge deletes a purged table's rows.
             */
            TABLE_LAYOUTS_DDL,
            TABLE_LAYOUTS_LIVE_DDL,
            """CREATE TABLE IF NOT EXISTS table_totals(
                table_id INTEGER PRIMARY KEY REFERENCES tables(id) ON DELETE CASCADE,
                record_count INTEGER NOT NULL DEFAULT 0,
                file_count INTEGER NOT NULL DEFAULT 0,
                file_size_bytes INTEGER NOT NULL DEFAULT 0 CHECK (file_size_bytes >= 0)
            )""",
            """CREATE TABLE IF NOT EXISTS snapshots(
                id INTEGER PRIMARY KEY,
                ts TEXT NOT NULL,
                author TEXT,
                message TEXT,
                schema_version INTEGER NOT NULL
            )""",
            // hog_snapshot_change: the typed vocabulary is closed, every
            // row names its object, and a row never outlives its snapshot.
            // Immediate, so each DDL and commit transaction inserts its
            // snapshot row before its change rows.
            """CREATE TABLE IF NOT EXISTS changes(
                snapshot_id INTEGER NOT NULL REFERENCES snapshots(id) ON DELETE CASCADE,
                kind TEXT NOT NULL CHECK (kind IN ($CHANGE_KINDS)),
                object_id INTEGER NOT NULL
            )""",
            "CREATE INDEX IF NOT EXISTS changes_obj ON changes(object_id, snapshot_id)",
            "CREATE INDEX IF NOT EXISTS changes_snap ON changes(snapshot_id)",
            /*
             * Files keep an end_snapshot from day one even though the
             * maintenance loops that SET it (expiry, compaction,
             * truncate) are ported later: every visibility predicate is
             * written end-snapshot-aware now, so those ports add writers
             * of the column, not a schema migration plus a rewrite of
             * every read.
             *
             * Column order matters. A row with a large stats_json spills to
             * overflow pages, and a column after it can only be read by a
             * walk of that overflow chain. So the fixed-width columns, which
             * the visibility and eligibility predicates read on every row,
             * come first, and the variable-length text comes last.
             */
            """CREATE TABLE IF NOT EXISTS files(
                id INTEGER PRIMARY KEY,
                table_id INTEGER NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
                begin_snapshot INTEGER NOT NULL,
                end_snapshot INTEGER,
                record_count INTEGER NOT NULL CHECK (record_count >= 0),
                file_size_bytes INTEGER NOT NULL CHECK (file_size_bytes >= 0),
                footer_size INTEGER,
                row_id_start INTEGER NOT NULL,
                explicit_row_ids INTEGER NOT NULL DEFAULT 0,
                spec_id INTEGER,
                missing_field_ids INTEGER NOT NULL DEFAULT 0,
                prefix_quarantined INTEGER NOT NULL DEFAULT 0,
                stats_state TEXT NOT NULL,
                file_format TEXT NOT NULL DEFAULT 'parquet',
                path TEXT NOT NULL,
                partition_values_json TEXT,
                stats_json TEXT,
                split_offsets_json TEXT,
                $WINDOW_CHECK
            )""",
            "CREATE INDEX IF NOT EXISTS files_table ON files(table_id, begin_snapshot)",
            // hog_delete_file: a vector rides its data file (and its table)
            // and goes with it; no file_format column, every row is a
            // puffin deletion vector.
            """CREATE TABLE IF NOT EXISTS dvs(
                data_file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
                table_id INTEGER NOT NULL REFERENCES tables(id) ON DELETE CASCADE,
                delete_file_id INTEGER NOT NULL,
                path TEXT NOT NULL,
                delete_count INTEGER NOT NULL CHECK (delete_count > 0),
                file_size_bytes INTEGER NOT NULL CHECK (file_size_bytes >= 0),
                begin_snapshot INTEGER NOT NULL,
                end_snapshot INTEGER,
                epoch INTEGER NOT NULL,
                PRIMARY KEY(data_file_id, epoch),
                $WINDOW_CHECK
            )""",
            DVS_ONE_LIVE_DDL,
            "CREATE INDEX IF NOT EXISTS dvs_table ON dvs(table_id, begin_snapshot)",
            """CREATE TABLE IF NOT EXISTS receipts(
                idempotency_key TEXT PRIMARY KEY,
                fingerprint TEXT NOT NULL,
                snapshot_id INTEGER NOT NULL,
                schema_version INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )""",
            RECEIPT_ENDED_DDL,
            // First-release DDL. Drop and recreate development tenants instead of adding migration code.
            "CREATE INDEX IF NOT EXISTS receipts_created_at ON receipts(created_at, idempotency_key)",
            """CREATE TABLE IF NOT EXISTS offsets(
                consumer_id TEXT NOT NULL CHECK (length(consumer_id) BETWEEN 1 AND 128),
                table_uuid TEXT NOT NULL,
                committed_snapshot INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(consumer_id, table_uuid)
            )""",
            /*
             * Upload claims: the fenced path reservation a writer without
             * S3 credentials of its own holds between claiming a path and
             * committing the file that landed there. States: 'active',
             * 'registered' (settled by a commit), 'abandoned'.
             */
            """CREATE TABLE IF NOT EXISTS uploads(
                upload_id TEXT PRIMARY KEY,
                owner TEXT NOT NULL,
                prefix TEXT NOT NULL,
                file_kind TEXT NOT NULL CHECK (file_kind IN ('data', 'delete')),
                path TEXT NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('active', 'registered', 'abandoned')),
                claimed_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                scheduled_at TEXT
            )""",
            "CREATE UNIQUE INDEX IF NOT EXISTS uploads_path ON uploads(path)",
            /*
             * Atomic table creations: the durable payload a writer stages
             * with PUT .../table-creations/{op} and publishes with
             * .../commit. The definition is the request verbatim, so a
             * replica reading it back sees what the writer sent.
             */
            """CREATE TABLE IF NOT EXISTS table_creations(
                operation TEXT PRIMARY KEY,
                namespace_id INTEGER NOT NULL,
                definition_json TEXT NOT NULL,
                table_uuid TEXT NOT NULL,
                write_path TEXT NOT NULL,
                state TEXT NOT NULL CHECK (state IN ('prepared', 'committed', 'rejected', 'aborted')),
                expires_at TEXT NOT NULL,
                files_json TEXT,
                snapshot_id INTEGER,
                schema_version INTEGER,
                reason TEXT,
                created_at TEXT NOT NULL,
                CHECK ((state = 'committed') = (snapshot_id IS NOT NULL AND schema_version IS NOT NULL))
            )""",
            /*
             * The removal queue (Postgres's hog_file_removal minus
             * catalog_id): expiry/compaction/retirement/upload-abandon
             * queue an object path here; the cleanup drain claims with a
             * lease, deletes the object, and settles with an outcome.
             * claimed_* is the cleanup worker's lease on the row.
             */
            """CREATE TABLE IF NOT EXISTS removals(
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL,
                file_kind TEXT NOT NULL CHECK (file_kind IN ('data', 'delete')),
                reason TEXT NOT NULL CHECK (reason IN ($REMOVAL_REASONS)),
                queued_snapshot INTEGER NOT NULL,
                queued_at TEXT NOT NULL,
                scheduled_at TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_attempt_at TEXT,
                claimed_at TEXT,
                claimed_by TEXT,
                drained_at TEXT,
                drained_outcome TEXT CHECK (drained_outcome IN ('deleted', 'absent', 'registered')),
                CHECK ((drained_at IS NULL) = (drained_outcome IS NULL))
            )""",
            "CREATE INDEX IF NOT EXISTS removals_unclaimed ON removals(claimed_at)",
            // Each ordered claim walk ignores the opposite arm's backlog.
            "CREATE INDEX IF NOT EXISTS removals_staging_queue ON removals(id) " +
                "WHERE drained_at IS NULL AND reason = 'compaction_staging'",
            "CREATE INDEX IF NOT EXISTS removals_bulk_queue ON removals(id) " +
                "WHERE drained_at IS NULL AND (reason IS NULL OR reason <> 'compaction_staging')",
            "CREATE INDEX IF NOT EXISTS removals_drained_at ON removals(drained_at, id) WHERE drained_at IS NOT NULL",
            """CREATE TABLE IF NOT EXISTS views(
                id INTEGER PRIMARY KEY,
                namespace_id INTEGER NOT NULL REFERENCES namespaces(id) DEFERRABLE INITIALLY DEFERRED,
                namespace TEXT NOT NULL CHECK ${identifier("namespace")},
                name TEXT NOT NULL CHECK ${identifier("name")},
                view_uuid TEXT NOT NULL UNIQUE,
                dialect TEXT NOT NULL,
                sql_text TEXT NOT NULL,
                created_snapshot INTEGER NOT NULL,
                dropped_snapshot INTEGER,
                CHECK (dropped_snapshot IS NULL OR dropped_snapshot > created_snapshot)
            )""",
            """CREATE UNIQUE INDEX IF NOT EXISTS views_live
                ON views(namespace, name) WHERE dropped_snapshot IS NULL""",
            /*
             * Compaction claims (Postgres's hog_compaction_claim): one row
             * per reserved input, keyed by (table, "file:<id>"), with a
             * lease. A group acquires all its inputs in one transaction. A claim is efficiency — the commit's liveness guards
             * are correctness — so a lost claim race wastes one rewrite,
             * never corrupts one.
             */
            """CREATE TABLE IF NOT EXISTS compaction_claims(
                table_id INTEGER NOT NULL,
                group_key TEXT NOT NULL,
                input_file_ids_json TEXT NOT NULL,
                claimant TEXT NOT NULL,
                claimed_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                PRIMARY KEY(table_id, group_key)
            )""",
            /*
             * The maintenance run ledger (the maintenance status/runs
             * routes read it): one row per task run, Postgres's
             * hog_maintenance_run minus catalog_id (the tenant IS the
             * catalog).
             */
            """CREATE TABLE IF NOT EXISTS maintenance_runs(
                id INTEGER PRIMARY KEY,
                task TEXT NOT NULL CHECK (task IN ($MAINTENANCE_TASKS)),
                run_trigger TEXT NOT NULL CHECK (run_trigger IN ('loop', 'manual')),
                started_at TEXT NOT NULL,
                finished_at TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('ok', 'failed')),
                error TEXT,
                result_json TEXT,
                CHECK (finished_at >= started_at),
                CHECK ((status = 'failed') = (error IS NOT NULL))
            )""",
            "CREATE INDEX IF NOT EXISTS maintenance_runs_task ON maintenance_runs(task, id)",
            "CREATE INDEX IF NOT EXISTS maintenance_runs_started_at ON maintenance_runs(started_at, id)",
        ) + SCALE_INDEXES

    private val TS_FORMAT: DateTimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC)

    // The one timestamp encoding: fixed-width UTC millis, lexically sortable.
    fun fmt(instant: Instant): String = TS_FORMAT.format(instant)

    fun parse(ts: String): Instant = Instant.parse(ts)

    fun now(): String = fmt(Instant.now())
}
