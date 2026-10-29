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
 */
internal object HogliteSchema {
    /** The `meta` row's fixed id — one catalog, one row. */
    const val META_ID = 1L

    /** Snapshot 0: the empty catalog, as hoglake's createCatalog writes it. */
    const val SNAPSHOT_ZERO = 0L

    /** head = next_snapshot - 1, as an SQL expression over the meta row. */
    const val HEAD_SQL = "(SELECT next_snapshot - 1 FROM meta WHERE id = 1)"

    val DDL: List<String> =
        listOf(
            """CREATE TABLE IF NOT EXISTS meta(
                id INTEGER PRIMARY KEY CHECK(id = 1),
                name TEXT NOT NULL,
                data_path TEXT NOT NULL,
                next_snapshot INTEGER NOT NULL DEFAULT 1,
                next_table_id INTEGER NOT NULL DEFAULT 1,
                next_namespace_id INTEGER NOT NULL DEFAULT 1,
                next_file_id INTEGER NOT NULL DEFAULT 1,
                next_view_id INTEGER NOT NULL DEFAULT 1,
                schema_version INTEGER NOT NULL DEFAULT 0,
                earliest_snapshot_id INTEGER NOT NULL DEFAULT 0,
                earliest_snapshot_time TEXT,
                snapshot_retention_seconds INTEGER,
                consumer_floor INTEGER NOT NULL DEFAULT 1,
                /* Set by dropCatalog's fencing transaction: every
                 * mutating tenant tx guards on it, so no write can land
                 * between the drop's emptiness checks and the tenant's
                 * deletion. */
                dropping INTEGER NOT NULL DEFAULT 0
            )""",
            """CREATE TABLE IF NOT EXISTS namespaces(
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                created_snapshot INTEGER NOT NULL,
                dropped_snapshot INTEGER
            )""",
            """CREATE UNIQUE INDEX IF NOT EXISTS namespaces_live
                ON namespaces(name) WHERE dropped_snapshot IS NULL""",
            """CREATE TABLE IF NOT EXISTS tables(
                id INTEGER PRIMARY KEY,
                namespace_id INTEGER NOT NULL,
                namespace TEXT NOT NULL,
                name TEXT NOT NULL,
                table_uuid TEXT NOT NULL UNIQUE,
                comment TEXT,
                properties_json TEXT NOT NULL DEFAULT '{}',
                next_row_id INTEGER NOT NULL DEFAULT 0,
                next_field_id INTEGER NOT NULL DEFAULT 1,
                replaced_table_id INTEGER,
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
                table_id INTEGER NOT NULL,
                namespace_id INTEGER NOT NULL,
                namespace TEXT NOT NULL,
                name TEXT NOT NULL,
                comment TEXT,
                properties_json TEXT NOT NULL DEFAULT '{}',
                begin_snapshot INTEGER NOT NULL,
                end_snapshot INTEGER,
                PRIMARY KEY(table_id, begin_snapshot)
            )""",
            """CREATE UNIQUE INDEX IF NOT EXISTS table_versions_live
                ON table_versions(namespace, name) WHERE end_snapshot IS NULL""",
            /*
             * Column VERSIONS, one row per column node (top-level or
             * nested) per interval of visibility — hog_column's shape.
             * A column dropped by an alter keeps its rows with
             * end_snapshot set, so a time-travelled read still resolves
             * the stats of files written under the old shape.
             */
            """CREATE TABLE IF NOT EXISTS columns(
                table_id INTEGER NOT NULL,
                field_id INTEGER NOT NULL,
                parent_field_id INTEGER,
                ordinal INTEGER NOT NULL,
                name TEXT NOT NULL,
                type TEXT NOT NULL,
                type_params_json TEXT,
                nullable INTEGER NOT NULL,
                comment TEXT,
                begin_snapshot INTEGER NOT NULL,
                end_snapshot INTEGER,
                PRIMARY KEY(table_id, field_id, begin_snapshot)
            )""",
            "CREATE INDEX IF NOT EXISTS columns_table ON columns(table_id, end_snapshot)",
            """CREATE TABLE IF NOT EXISTS partition_specs(
                table_id INTEGER NOT NULL,
                spec_id INTEGER NOT NULL,
                fields_json TEXT NOT NULL,
                begin_snapshot INTEGER NOT NULL,
                end_snapshot INTEGER,
                PRIMARY KEY(table_id, spec_id)
            )""",
            """CREATE TABLE IF NOT EXISTS sort_specs(
                table_id INTEGER NOT NULL,
                spec_id INTEGER NOT NULL,
                fields_json TEXT NOT NULL,
                begin_snapshot INTEGER NOT NULL,
                end_snapshot INTEGER,
                PRIMARY KEY(table_id, spec_id)
            )""",
            """CREATE TABLE IF NOT EXISTS table_totals(
                table_id INTEGER PRIMARY KEY,
                record_count INTEGER NOT NULL DEFAULT 0,
                file_count INTEGER NOT NULL DEFAULT 0,
                file_size_bytes INTEGER NOT NULL DEFAULT 0
            )""",
            """CREATE TABLE IF NOT EXISTS snapshots(
                id INTEGER PRIMARY KEY,
                ts TEXT NOT NULL,
                author TEXT,
                message TEXT,
                schema_version INTEGER NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS changes(
                snapshot_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
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
             */
            """CREATE TABLE IF NOT EXISTS files(
                id INTEGER PRIMARY KEY,
                table_id INTEGER NOT NULL,
                path TEXT NOT NULL,
                file_format TEXT NOT NULL DEFAULT 'parquet',
                record_count INTEGER NOT NULL,
                file_size_bytes INTEGER NOT NULL,
                footer_size INTEGER,
                row_id_start INTEGER NOT NULL,
                explicit_row_ids INTEGER NOT NULL DEFAULT 0,
                spec_id INTEGER,
                partition_values_json TEXT,
                stats_state TEXT NOT NULL,
                stats_json TEXT,
                split_offsets_json TEXT,
                missing_field_ids INTEGER NOT NULL DEFAULT 0,
                begin_snapshot INTEGER NOT NULL,
                end_snapshot INTEGER
            )""",
            "CREATE INDEX IF NOT EXISTS files_table ON files(table_id, begin_snapshot)",
            """CREATE TABLE IF NOT EXISTS dvs(
                data_file_id INTEGER NOT NULL,
                table_id INTEGER NOT NULL,
                delete_file_id INTEGER NOT NULL,
                path TEXT NOT NULL,
                delete_count INTEGER NOT NULL,
                file_size_bytes INTEGER NOT NULL,
                begin_snapshot INTEGER NOT NULL,
                end_snapshot INTEGER,
                epoch INTEGER NOT NULL,
                PRIMARY KEY(data_file_id, epoch)
            )""",
            "CREATE INDEX IF NOT EXISTS dvs_table ON dvs(table_id, begin_snapshot)",
            """CREATE TABLE IF NOT EXISTS receipts(
                idempotency_key TEXT PRIMARY KEY,
                fingerprint TEXT NOT NULL,
                snapshot_id INTEGER NOT NULL,
                schema_version INTEGER NOT NULL,
                created_at TEXT NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS offsets(
                consumer_id TEXT NOT NULL,
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
                file_kind TEXT NOT NULL,
                path TEXT NOT NULL,
                state TEXT NOT NULL,
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
                state TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                files_json TEXT,
                snapshot_id INTEGER,
                schema_version INTEGER,
                reason TEXT,
                created_at TEXT NOT NULL
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
                file_kind TEXT NOT NULL,
                reason TEXT,
                queued_snapshot INTEGER NOT NULL,
                queued_at TEXT NOT NULL,
                scheduled_at TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                last_attempt_at TEXT,
                claimed_at TEXT,
                claimed_by TEXT,
                drained_at TEXT,
                drained_outcome TEXT
            )""",
            "CREATE INDEX IF NOT EXISTS removals_unclaimed ON removals(claimed_at)",
            """CREATE TABLE IF NOT EXISTS views(
                id INTEGER PRIMARY KEY,
                namespace_id INTEGER NOT NULL,
                namespace TEXT NOT NULL,
                name TEXT NOT NULL,
                view_uuid TEXT NOT NULL UNIQUE,
                dialect TEXT NOT NULL,
                sql_text TEXT NOT NULL,
                created_snapshot INTEGER NOT NULL,
                dropped_snapshot INTEGER
            )""",
            """CREATE UNIQUE INDEX IF NOT EXISTS views_live
                ON views(namespace, name) WHERE dropped_snapshot IS NULL""",
            /*
             * Compaction claims (Postgres's hog_compaction_claim): one row
             * per claimed group, keyed by (table, sorted input ids), with a
             * lease. A claim is efficiency — the commit's liveness guards
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
                task TEXT NOT NULL,
                run_trigger TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT NOT NULL,
                status TEXT NOT NULL,
                error TEXT,
                result_json TEXT
            )""",
            "CREATE INDEX IF NOT EXISTS maintenance_runs_task ON maintenance_runs(task, id)",
        )

    private val TS_FORMAT: DateTimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC)

    // The one timestamp encoding: fixed-width UTC millis, lexically sortable.
    fun fmt(instant: Instant): String = TS_FORMAT.format(instant)

    fun parse(ts: String): Instant = Instant.parse(ts)

    fun now(): String = fmt(Instant.now())
}
