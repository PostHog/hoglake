package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.HoglakeException

/** Shared SQL fragments for the hoglite backend. */
internal object HogliteSql {
    /**
     * The file columns every file listing selects, in the order
     * [HogliteCatalogStore.toFileRow] reads them: 15 columns,
     * indices 0..14. A query that joins DVs puts the five DV columns
     * (delete_file_id, path, delete_count, file_size_bytes,
     * begin_snapshot) at indices 15..19.
     */
    const val FILE_COLUMNS =
        "f.id, f.table_id, f.path, f.file_format, f.record_count, f.file_size_bytes, f.footer_size, " +
            "f.row_id_start, f.explicit_row_ids, f.spec_id, f.partition_values_json, f.stats_state, " +
            "f.stats_json, f.split_offsets_json, f.begin_snapshot"

    /**
     * The readback every DDL transaction ends with: the snapshot just
     * allocated, the schema version it carries, and the two id
     * allocators' latest values (the new table/namespace id, when the
     * operation allocated one). Callers read the columns they need.
     */
    const val READBACK_DDL =
        "SELECT (SELECT next_snapshot - 1 FROM meta WHERE id = 1), " +
            "(SELECT schema_version FROM meta WHERE id = 1), " +
            "(SELECT next_table_id - 1 FROM meta WHERE id = 1), " +
            "(SELECT next_namespace_id - 1 FROM meta WHERE id = 1)"

    /** Commit readback: the snapshot just allocated and the schema version it carries. */
    const val READBACK_COMMIT =
        "SELECT (SELECT next_snapshot - 1 FROM meta WHERE id = 1), " +
            "(SELECT schema_version FROM meta WHERE id = 1)"
}

/** Allocate the DDL snapshot this transaction commits at: bumps BOTH the snapshot and schema allocators. */
internal fun HogliteTx.allocateDdlSnapshot() {
    affectOne(
        "UPDATE meta SET next_snapshot = next_snapshot + 1, schema_version = schema_version + 1 WHERE id = 1",
    ) { HoglakeException.NotFound("catalog (meta row missing)") }
}

/** Allocate the DATA snapshot this transaction commits at: the snapshot allocator only. */
internal fun HogliteTx.allocateCommitSnapshot() {
    affectOne(
        "UPDATE meta SET next_snapshot = next_snapshot + 1 WHERE id = 1",
    ) { HoglakeException.NotFound("catalog (meta row missing)") }
}

/** The snapshot row for the snapshot this transaction just allocated. */
internal fun HogliteTx.insertSnapshotRow(
    author: String?,
    message: String?,
    ts: String = HogliteSchema.now(),
) {
    add(
        "INSERT INTO snapshots(id, ts, author, message, schema_version) " +
            "VALUES (${HogliteSchema.HEAD_SQL}, ?, ?, ?, (SELECT schema_version FROM meta WHERE id = 1))",
        ts,
        author,
        message,
    )
}
