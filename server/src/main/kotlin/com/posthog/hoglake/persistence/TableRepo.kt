package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
import java.util.UUID

/** Resolved table identity + the version row that made it visible. */
data class TableRow(
    val tableId: Long,
    val tableUuid: UUID,
    val namespaceId: Long,
    val name: String,
)

/** Rollup row from hog_table_stats. */
data class TableStatsRow(
    val recordCount: Long,
    val fileSizeBytes: Long,
    val nextRowId: Long,
)

/**
 * hog_table (immutable identity) + hog_table_version (versioned
 * name/namespace) + hog_column + hog_table_stats.
 *
 * Versioned-row visibility everywhere: a row is visible at snapshot S
 * iff begin_snapshot <= S AND (end_snapshot IS NULL OR S < end_snapshot).
 */
object TableRepo {
    private val tableRowMapper =
        RowMapper { rs, _ ->
            TableRow(
                tableId = rs.getLong("table_id"),
                tableUuid = rs.getObject("table_uuid") as UUID,
                namespaceId = rs.getLong("namespace_id"),
                name = rs.getString("name"),
            )
        }

    /** One hog_column row, flat: the tree is assembled from these. */
    private data class ColumnRow(
        val fieldId: Long,
        val parentFieldId: Long?,
        val ordinal: Int,
        val def: ColumnDef,
    )

    private val columnRowMapper =
        RowMapper { rs, _ ->
            ColumnRow(
                fieldId = rs.getLong("field_id"),
                parentFieldId = rs.getObject("parent_field_id", java.lang.Long::class.java)?.toLong(),
                ordinal = rs.getInt("ordinal"),
                def =
                    ColumnDef(
                        name = rs.getString("name"),
                        type = ColType.fromWire(rs.getString("col_type")),
                        typeParams = Pg.fromJson(rs.getString("type_params")),
                        nullable = rs.getBoolean("nullable"),
                    ),
            )
        }

    /**
     * Assemble flat rows into the column FOREST: top-level columns
     * (parent_field_id IS NULL) ordered by ordinal, each container's
     * children likewise.
     *
     * A row whose parent is not in the visible set is DROPPED rather
     * than promoted to top level: the two cannot legitimately disagree
     * (a subtree is end-snapshotted whole), and silently re-rooting an
     * orphan would show a `key` or an `element` as a table column.
     */
    private fun assemble(rows: List<ColumnRow>): List<Column> {
        val byParent = rows.groupBy { it.parentFieldId }

        fun build(parent: Long?): List<Column> =
            (byParent[parent] ?: emptyList())
                .sortedBy { it.ordinal }
                .map { row ->
                    Column(
                        fieldId = row.fieldId,
                        ordinal = row.ordinal,
                        def = row.def,
                        children = if (row.def.type.isNested) build(row.fieldId) else emptyList(),
                    )
                }
        return build(null)
    }

    /** Flatten a forest into insertable rows, parents before children. */
    private fun flatten(
        columns: List<Column>,
        parentFieldId: Long?,
        out: MutableList<Pair<Column, Long?>>,
    ) {
        for (c in columns) {
            out.add(c to parentFieldId)
            flatten(c.children, c.fieldId, out)
        }
    }

    /** Insert the identity row; returns the generated table_uuid. */
    fun insertTable(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        createdSnapshot: Long,
        tableUuid: UUID = UUID.randomUUID(),
    ): UUID =
        handle.createQuery(
            """
            INSERT INTO hog_table (catalog_id, table_id, created_snapshot, table_uuid)
            VALUES (:catalogId, :tableId, :createdSnapshot, :tableUuid)
            RETURNING table_uuid
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("createdSnapshot", createdSnapshot)
            .bind("tableUuid", tableUuid)
            .map { rs, _ -> rs.getObject("table_uuid") as UUID }
            .one()

    /**
     * Allocate [count] consecutive field ids from hog_table.next_field_id
     * (1-based). Returns the first allocated id. Caller holds the
     * catalog commit lock.
     */
    fun allocateFieldIds(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        count: Int,
    ): Long =
        handle.createQuery(
            """
            UPDATE hog_table SET next_field_id = next_field_id + :count
            WHERE catalog_id = :catalogId AND table_id = :tableId
            RETURNING next_field_id - :count AS first_id
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("count", count)
            .mapTo(Long::class.javaObjectType)
            .one()

    /**
     * Insert the initial (or post-rename) version row. The live-name
     * unique index backstops the duplicate check ->
     * [HoglakeException.AlreadyExists].
     */
    fun insertVersion(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        beginSnapshot: Long,
        namespaceId: Long,
        name: String,
    ) {
        try {
            handle.createUpdate(
                """
                INSERT INTO hog_table_version (catalog_id, table_id, begin_snapshot, namespace_id, name)
                VALUES (:catalogId, :tableId, :beginSnapshot, :namespaceId, :name)
                """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("beginSnapshot", beginSnapshot)
                .bind("namespaceId", namespaceId)
                .bind("name", name)
                .execute()
        } catch (e: UnableToExecuteStatementException) {
            if (Pg.isUniqueViolation(e)) {
                throw HoglakeException.AlreadyExists("table '$name' already exists")
            }
            throw e
        }
    }

    /**
     * Insert a column FOREST at [beginSnapshot], parents before their
     * children. [parentFieldId] is the parent of the roots of [columns]
     * — null for top-level columns, the containing struct's field id
     * when an alter grafts a field into one.
     */
    fun insertColumns(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        beginSnapshot: Long,
        columns: List<Column>,
        parentFieldId: Long? = null,
    ) {
        val rows = mutableListOf<Pair<Column, Long?>>()
        flatten(columns, parentFieldId, rows)
        if (rows.isEmpty()) return
        val batch =
            handle.prepareBatch(
                """
            INSERT INTO hog_column
                (catalog_id, table_id, field_id, begin_snapshot, name, col_type,
                 type_params, nullable, ordinal, parent_field_id)
            VALUES (:catalogId, :tableId, :fieldId, :beginSnapshot, :name, :colType,
                    :typeParams::jsonb, :nullable, :ordinal, :parentFieldId)
            """,
            )
        for ((c, parent) in rows) {
            batch
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("fieldId", c.fieldId)
                .bind("beginSnapshot", beginSnapshot)
                .bind("name", c.def.name)
                .bind("colType", c.def.type.wire)
                .bind("typeParams", Pg.toJson(c.def.typeParams))
                .bind("nullable", c.def.nullable)
                .bind("ordinal", c.ordinal)
                // bindByType, NOT bind + bindNull: a prepared BATCH binds
                // one argument factory per name, and a null bound with
                // bindNull registers a NullArgument that the next row's
                // real Long cannot reuse ("No argument factory registered
                // for '1' of qualified type NullArgument"). A nested
                // create-table is exactly a batch with both — a top-level
                // column's NULL parent followed by a child's real one.
                .bindByType("parentFieldId", parent, Long::class.javaObjectType)
                .add()
        }
        batch.execute()
    }

    /** Create the one-per-table rollup/allocator row, all zeros. */
    fun insertStatsRow(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
    ) {
        handle.createUpdate(
            """
            INSERT INTO hog_table_stats (catalog_id, table_id)
            VALUES (:catalogId, :tableId)
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .execute()
    }

    /** Resolve a table by (namespace, name) visible at [snapshot]. */
    fun findAt(
        handle: Handle,
        catalogId: Long,
        namespaceId: Long,
        name: String,
        snapshot: Long,
    ): TableRow? =
        handle.createQuery(
            """
            SELECT t.table_id, t.table_uuid, tv.namespace_id, tv.name
            FROM hog_table_version tv
            JOIN hog_table t
              ON t.catalog_id = tv.catalog_id AND t.table_id = tv.table_id
            WHERE tv.catalog_id = :catalogId
              AND tv.namespace_id = :namespaceId
              AND tv.name = :name
              AND tv.begin_snapshot <= :snapshot
              AND (tv.end_snapshot IS NULL OR :snapshot < tv.end_snapshot)
            """,
        )
            .bind("catalogId", catalogId)
            .bind("namespaceId", namespaceId)
            .bind("name", name)
            .bind("snapshot", snapshot)
            .map(tableRowMapper)
            .findOne()
            .orElse(null)

    /** Resolve a live (head-visible) table by (namespace, name). */
    fun findLive(
        handle: Handle,
        catalogId: Long,
        namespaceId: Long,
        name: String,
    ): TableRow? =
        handle.createQuery(
            """
            SELECT t.table_id, t.table_uuid, tv.namespace_id, tv.name
            FROM hog_table_version tv
            JOIN hog_table t
              ON t.catalog_id = tv.catalog_id AND t.table_id = tv.table_id
            WHERE tv.catalog_id = :catalogId
              AND tv.namespace_id = :namespaceId
              AND tv.name = :name
              AND tv.end_snapshot IS NULL
            """,
        )
            .bind("catalogId", catalogId)
            .bind("namespaceId", namespaceId)
            .bind("name", name)
            .map(tableRowMapper)
            .findOne()
            .orElse(null)

    /** All live tables in a namespace, ordered by name. */
    fun listLive(
        handle: Handle,
        catalogId: Long,
        namespaceId: Long,
    ): List<TableRow> =
        handle.createQuery(
            """
            SELECT t.table_id, t.table_uuid, tv.namespace_id, tv.name
            FROM hog_table_version tv
            JOIN hog_table t
              ON t.catalog_id = tv.catalog_id AND t.table_id = tv.table_id
            WHERE tv.catalog_id = :catalogId
              AND tv.namespace_id = :namespaceId
              AND tv.end_snapshot IS NULL
            ORDER BY tv.name
            """,
        )
            .bind("catalogId", catalogId)
            .bind("namespaceId", namespaceId)
            .map(tableRowMapper)
            .list()

    /**
     * The column FOREST visible at [snapshot]: top-level columns in
     * ordinal order, each container carrying its children (also in
     * ordinal order, which is per-parent since V5).
     */
    fun columnsAt(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
    ): List<Column> =
        assemble(
            handle.createQuery(
                """
                SELECT field_id, parent_field_id, name, col_type, type_params, nullable, ordinal
                FROM hog_column
                WHERE catalog_id = :catalogId AND table_id = :tableId
                  AND begin_snapshot <= :snapshot
                  AND (end_snapshot IS NULL OR :snapshot < end_snapshot)
                """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("snapshot", snapshot)
                .map(columnRowMapper)
                .list(),
        )

    fun stats(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
    ): TableStatsRow =
        handle.createQuery(
            """
            SELECT record_count, file_size_bytes, next_row_id
            FROM hog_table_stats
            WHERE catalog_id = :catalogId AND table_id = :tableId
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .map { rs, _ ->
                TableStatsRow(
                    recordCount = rs.getLong("record_count"),
                    fileSizeBytes = rs.getLong("file_size_bytes"),
                    nextRowId = rs.getLong("next_row_id"),
                )
            }
            .findOne()
            .orElse(TableStatsRow(0, 0, 0))

    /**
     * Drop bookkeeping at [snapshot]: sets hog_table.dropped_snapshot
     * and end-snapshots the live version and column rows. Data files
     * are end-snapshotted by [FileRepo.endLiveFiles].
     */
    fun markDropped(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
    ) {
        handle.createUpdate(
            """
            UPDATE hog_table SET dropped_snapshot = :snapshot
            WHERE catalog_id = :catalogId AND table_id = :tableId
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("snapshot", snapshot)
            .execute()
        handle.createUpdate(
            """
            UPDATE hog_table_version SET end_snapshot = :snapshot
            WHERE catalog_id = :catalogId AND table_id = :tableId AND end_snapshot IS NULL
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("snapshot", snapshot)
            .execute()
        handle.createUpdate(
            """
            UPDATE hog_column SET end_snapshot = :snapshot
            WHERE catalog_id = :catalogId AND table_id = :tableId AND end_snapshot IS NULL
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("snapshot", snapshot)
            .execute()
    }
}
