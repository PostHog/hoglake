-- Frozen pre-layout tenant schema. Append migrations; do not regenerate this baseline.
CREATE TABLE IF NOT EXISTS meta(
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
                
                dropping INTEGER NOT NULL DEFAULT 0
            );

CREATE TABLE IF NOT EXISTS namespaces(
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                created_snapshot INTEGER NOT NULL,
                dropped_snapshot INTEGER
            );

CREATE UNIQUE INDEX IF NOT EXISTS namespaces_live
                ON namespaces(name) WHERE dropped_snapshot IS NULL;

CREATE TABLE IF NOT EXISTS tables(
                id INTEGER PRIMARY KEY,
                namespace_id INTEGER NOT NULL,
                namespace TEXT NOT NULL,
                name TEXT NOT NULL,
                table_uuid TEXT NOT NULL UNIQUE,
                comment TEXT,
                properties_json TEXT NOT NULL DEFAULT '{}',
                next_row_id INTEGER NOT NULL DEFAULT 0,
                next_field_id INTEGER NOT NULL DEFAULT 1,
                schema_version INTEGER NOT NULL DEFAULT 0,
                replaced_table_id INTEGER,
                created_snapshot INTEGER NOT NULL,
                dropped_snapshot INTEGER
            );

CREATE UNIQUE INDEX IF NOT EXISTS tables_live
                ON tables(namespace, name) WHERE dropped_snapshot IS NULL;

CREATE TABLE IF NOT EXISTS table_versions(
                table_id INTEGER NOT NULL,
                namespace_id INTEGER NOT NULL,
                namespace TEXT NOT NULL,
                name TEXT NOT NULL,
                comment TEXT,
                properties_json TEXT NOT NULL DEFAULT '{}',
                begin_snapshot INTEGER NOT NULL,
                end_snapshot INTEGER,
                PRIMARY KEY(table_id, begin_snapshot)
            );

CREATE UNIQUE INDEX IF NOT EXISTS table_versions_live
                ON table_versions(namespace, name) WHERE end_snapshot IS NULL;

CREATE TABLE IF NOT EXISTS columns(
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
            );

CREATE INDEX IF NOT EXISTS columns_table ON columns(table_id, end_snapshot);

CREATE TABLE IF NOT EXISTS partition_specs(
                table_id INTEGER NOT NULL,
                spec_id INTEGER NOT NULL,
                fields_json TEXT NOT NULL,
                begin_snapshot INTEGER NOT NULL,
                end_snapshot INTEGER,
                PRIMARY KEY(table_id, spec_id)
            );

CREATE TABLE IF NOT EXISTS sort_specs(
                table_id INTEGER NOT NULL,
                spec_id INTEGER NOT NULL,
                fields_json TEXT NOT NULL,
                begin_snapshot INTEGER NOT NULL,
                end_snapshot INTEGER,
                PRIMARY KEY(table_id, spec_id)
            );

CREATE TABLE IF NOT EXISTS table_totals(
                table_id INTEGER PRIMARY KEY,
                record_count INTEGER NOT NULL DEFAULT 0,
                file_count INTEGER NOT NULL DEFAULT 0,
                file_size_bytes INTEGER NOT NULL DEFAULT 0
            );

CREATE TABLE IF NOT EXISTS snapshots(
                id INTEGER PRIMARY KEY,
                ts TEXT NOT NULL,
                author TEXT,
                message TEXT,
                schema_version INTEGER NOT NULL
            );

CREATE TABLE IF NOT EXISTS changes(
                snapshot_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                object_id INTEGER NOT NULL
            );

CREATE INDEX IF NOT EXISTS changes_obj ON changes(object_id, snapshot_id);

CREATE INDEX IF NOT EXISTS changes_snap ON changes(snapshot_id);

CREATE TABLE IF NOT EXISTS files(
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
            );

CREATE INDEX IF NOT EXISTS files_table ON files(table_id, begin_snapshot);

CREATE TABLE IF NOT EXISTS dvs(
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
            );

CREATE INDEX IF NOT EXISTS dvs_table ON dvs(table_id, begin_snapshot);

CREATE TABLE IF NOT EXISTS receipts(
                idempotency_key TEXT PRIMARY KEY,
                fingerprint TEXT NOT NULL,
                snapshot_id INTEGER NOT NULL,
                schema_version INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );

CREATE TABLE IF NOT EXISTS offsets(
                consumer_id TEXT NOT NULL,
                table_uuid TEXT NOT NULL,
                committed_snapshot INTEGER NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY(consumer_id, table_uuid)
            );

CREATE TABLE IF NOT EXISTS uploads(
                upload_id TEXT PRIMARY KEY,
                owner TEXT NOT NULL,
                prefix TEXT NOT NULL,
                file_kind TEXT NOT NULL,
                path TEXT NOT NULL,
                state TEXT NOT NULL,
                claimed_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                scheduled_at TEXT
            );

CREATE UNIQUE INDEX IF NOT EXISTS uploads_path ON uploads(path);

CREATE TABLE IF NOT EXISTS table_creations(
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
            );

CREATE TABLE IF NOT EXISTS removals(
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
            );

CREATE INDEX IF NOT EXISTS removals_unclaimed ON removals(claimed_at);

CREATE TABLE IF NOT EXISTS views(
                id INTEGER PRIMARY KEY,
                namespace_id INTEGER NOT NULL,
                namespace TEXT NOT NULL,
                name TEXT NOT NULL,
                view_uuid TEXT NOT NULL UNIQUE,
                dialect TEXT NOT NULL,
                sql_text TEXT NOT NULL,
                created_snapshot INTEGER NOT NULL,
                dropped_snapshot INTEGER
            );

CREATE UNIQUE INDEX IF NOT EXISTS views_live
                ON views(namespace, name) WHERE dropped_snapshot IS NULL;

CREATE TABLE IF NOT EXISTS compaction_claims(
                table_id INTEGER NOT NULL,
                group_key TEXT NOT NULL,
                input_file_ids_json TEXT NOT NULL,
                claimant TEXT NOT NULL,
                claimed_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                PRIMARY KEY(table_id, group_key)
            );

CREATE TABLE IF NOT EXISTS maintenance_runs(
                id INTEGER PRIMARY KEY,
                task TEXT NOT NULL,
                run_trigger TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT NOT NULL,
                status TEXT NOT NULL,
                error TEXT,
                result_json TEXT
            );

CREATE INDEX IF NOT EXISTS maintenance_runs_task ON maintenance_runs(task, id);
