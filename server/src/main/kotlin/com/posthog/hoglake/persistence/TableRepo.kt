package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableSummaryInfo
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
    val comment: String? = null,
    val properties: Map<String, String> = emptyMap(),
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
                comment = rs.getString("comment"),
                properties = Pg.fromJson(rs.getString("properties"))!!.mapValues { (_, value) -> value as String },
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
                        comment = rs.getString("comment"),
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

    /**
     * Insert the identity row; returns the generated table_uuid.
     *
     * [replacedTableId] is the RECORDED replacement edge (V14): the
     * incarnation this row retires, non-null only on an atomic
     * replacement. It exists so nothing has to infer lineage from
     * `dropped_snapshot = created_snapshot` at runtime — see
     * OffsetRepo.releaseSupersededOffsets for what being wrong about it
     * would cost.
     */
    fun insertTable(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
        createdSnapshot: Long,
        tableUuid: UUID = UUID.randomUUID(),
        replacedTableId: Long? = null,
    ): UUID =
        handle.createQuery(
            """
            INSERT INTO hog_table (catalog_id, table_id, created_snapshot, table_uuid, replaced_table_id)
            VALUES (:catalogId, :tableId, :createdSnapshot, :tableUuid, :replacedTableId)
            RETURNING table_uuid
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("createdSnapshot", createdSnapshot)
            .bind("tableUuid", tableUuid)
            .bind("replacedTableId", replacedTableId)
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
        comment: String? = null,
        properties: Map<String, String> = emptyMap(),
    ) {
        try {
            handle.createUpdate(
                """
                INSERT INTO hog_table_version (catalog_id, table_id, begin_snapshot, namespace_id, name, comment, properties)
                VALUES (:catalogId, :tableId, :beginSnapshot, :namespaceId, :name, :comment, :properties::jsonb)
                """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("beginSnapshot", beginSnapshot)
                .bind("namespaceId", namespaceId)
                .bind("name", name)
                .bind("comment", comment)
                .bind("properties", Pg.toJson(properties))
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
                 type_params, nullable, ordinal, parent_field_id, comment)
            VALUES (:catalogId, :tableId, :fieldId, :beginSnapshot, :name, :colType,
                    :typeParams::jsonb, :nullable, :ordinal, :parentFieldId, :comment)
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
                .bindByType("comment", c.def.comment, String::class.java)
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
            SELECT t.table_id, t.table_uuid, tv.namespace_id, tv.name, tv.comment, tv.properties
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
            SELECT t.table_id, t.table_uuid, tv.namespace_id, tv.name, tv.comment, tv.properties
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
            SELECT t.table_id, t.table_uuid, tv.namespace_id, tv.name, tv.comment, tv.properties
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
     * A namespace's tables at [snapshot], with their rollups, in ONE
     * statement.
     *
     * The listing used to be `listLive` plus one `FileRepo.aggregateAt`
     * per table — an N+1 that a browser page paid on every render and
     * that grows with the namespace. The two LATERAL sub-selects replace
     * it with a single round trip, one index-driven inner scan per
     * table.
     *
     * The catalog's head and expiry floor are read by THIS STATEMENT,
     * in the `bounds` CTE, and every predicate — the version row's
     * included — is evaluated against them. Two things follow, and both
     * were bugs first:
     *
     *  - `end_snapshot IS NULL` would have been "live NOW", a different
     *    question from "live at the snapshot the aggregates were
     *    computed at". A table created between a caller's head read and
     *    the listing would have been LISTED (live now) with
     *    `snapshot_count = 0` and a null `earliest_snapshot_id` — the
     *    shape the spec defines as "every one of its commits has
     *    expired", i.e. the opposite fact, indistinguishable from it on
     *    the wire. So the `tv` predicate is invariant 6's, verbatim and
     *    identically to [findAt].
     *  - and the bounds are read HERE rather than passed in, because one
     *    statement is one MVCC snapshot by definition. The first fix for
     *    the race above wrapped the head read and this query in a
     *    REPEATABLE READ transaction, which broke: JDBI reuses a
     *    thread's open handle, so a caller already inside a transaction
     *    got "nested transaction with isolation level REPEATABLE_READ,
     *    but already running in READ_COMMITTED" — a read path a UI calls
     *    must not care what its caller is doing. A single statement
     *    composes with any caller and needs no isolation level at all.
     *
     * MEASURED plan (200 tables here plus 600 in a sibling namespace,
     * 52k data-file rows, 4.2k change rows —
     * `TableListingQueryPlanIntegrationTest` EXPLAINs this exact string
     * and asserts every number below):
     *
     *  - driving scan: an index scan of `hog_table_version` with
     *    `namespace_id` as a FILTER, not an index condition — the
     *    planner prefers the pkey's (catalog_id, table_id,
     *    begin_snapshot) to `hog_table_version_namespace`. So the
     *    driving scan is catalog-sized. It is also CHEAP (13 buffers
     *    against the laterals' 2,199), and the laterals run only for the
     *    rows it emits, which the filter has already narrowed to this
     *    namespace. That asymmetry is the LATERAL-versus-GROUP-BY
     *    argument, and the test asserts it rather than this comment
     *    claiming it;
     *  - file rollup: `Index Scan using hog_data_file_changefeed on
     *    hog_data_file`, 250 rows/loop, 10 removed by the visibility
     *    filter. NOT `hog_data_file_live`: both lead on
     *    (catalog_id, table_id, begin_snapshot) and the planner prefers
     *    the non-partial one. Either is fine and the test asserts
     *    index-driven rather than an index NAME;
     *  - history counts: `Index Only Scan using
     *    hog_snapshot_change_conflict`, 20 rows/loop, with a per-loop
     *    Sort above it for the `count(DISTINCT)` (see the cost note on
     *    [LIVE_SUMMARIES_SQL]).
     *
     * LATERAL rather than a GROUP BY over the catalog joined back: a
     * grouped scan sizes with the CATALOG's manifest and change log,
     * while these laterals size with this namespace's.
     *
     * `bounds.earliest` is the catalog's expiry floor — snapshots below
     * it are gone (invariant 5), so counting them would report history
     * no reader can reach.
     *
     * `kind = ANY(:kinds)` is not decoration: `object_id` spans three id
     * spaces and only the kind says which (see [ChangeKind.TABLE_SCOPED]).
     */
    fun listLiveSummaries(
        handle: Handle,
        catalogId: Long,
        namespaceId: Long,
    ): List<TableSummaryInfo> =
        handle.createQuery(LIVE_SUMMARIES_SQL)
            .bind("catalogId", catalogId)
            .bind("namespaceId", namespaceId)
            .bindArray("kinds", String::class.java, ChangeKind.TABLE_SCOPED.map { it.wire })
            .map(tableSummaryMapper)
            .list()

    /**
     * [listLiveSummaries]'s statement, `internal` so the plan test
     * EXPLAINs the SQL PRODUCTION runs. A plan test that retypes the
     * query asserts the plan of a string only it has ever executed.
     *
     * KNOWN COST, measured here rather than repeated from anywhere.
     * Fixture: ONE namespace holding 54,000 tables, 270,000 data files
     * (5 per table) and 270,000 change rows (5 per table) over 50
     * snapshots — a Portola-shaped catalog. Postgres 18, warm cache,
     * serial plan:
     *
     *  - 389/398/402 ms over three runs;
     *  - 595,488 shared buffers. The FILE rollup is 432,000 of them
     *    (73%) and the change-log lateral 162,001 (27%) — the file half
     *    dominates, and it is the half that has to read the files;
     *  - the final `ORDER BY tv.name` spills: 595 temp blocks read and
     *    written, at 54,000 rows;
     *  - 9.16 MiB of JSON in one unpaged response.
     *
     * The change-log lateral pays a per-loop Sort, because
     * `count(DISTINCT sc.snapshot_id)` cannot use
     * `hog_snapshot_change_conflict`'s snapshot_id ordering while
     * `kind = ANY(...)` sits between `object_id` and `snapshot_id` in
     * it. A covering index `(catalog_id, object_id, snapshot_id) WHERE
     * kind IN (...)` would remove that Sort — and would add a second
     * index to the table every commit's OCC check writes, to save 27%
     * of a read path's buffers. Not worth it, and measurably not the
     * problem: the file rollup is the bigger term and it is already
     * minimal.
     *
     * The 9 MiB is the real limit, and it is a SHAPE problem rather
     * than a plan problem. PAGING the endpoint is the answer and is a
     * follow-up; the OpenAPI description carries these numbers so the
     * next person starts from measurement.
     */
    internal val LIVE_SUMMARIES_SQL =
        """
            WITH bounds AS (
                SELECT last_snapshot_id AS snapshot, earliest_snapshot_id AS earliest
                FROM hog_catalog WHERE catalog_id = :catalogId
            )
            SELECT t.table_id, t.table_uuid, tv.name, tv.comment,
                   f.file_count, f.record_count, f.file_size_bytes,
                   c.snapshot_count, c.earliest_snapshot_id
            FROM bounds b
            JOIN hog_table_version tv
              ON tv.catalog_id = :catalogId
             AND tv.namespace_id = :namespaceId
             AND tv.begin_snapshot <= b.snapshot
             AND (tv.end_snapshot IS NULL OR b.snapshot < tv.end_snapshot)
            JOIN hog_table t
              ON t.catalog_id = tv.catalog_id AND t.table_id = tv.table_id
            CROSS JOIN LATERAL (
                SELECT count(*) AS file_count,
                       coalesce(sum(d.record_count), 0) AS record_count,
                       coalesce(sum(d.file_size_bytes), 0) AS file_size_bytes
                FROM hog_data_file d
                WHERE d.catalog_id = tv.catalog_id
                  AND d.table_id = tv.table_id
                  AND d.begin_snapshot <= b.snapshot
                  AND (d.end_snapshot IS NULL OR b.snapshot < d.end_snapshot)
            ) f
            CROSS JOIN LATERAL (
                SELECT count(DISTINCT sc.snapshot_id) AS snapshot_count,
                       min(sc.snapshot_id) AS earliest_snapshot_id
                FROM hog_snapshot_change sc
                WHERE sc.catalog_id = tv.catalog_id
                  AND sc.object_id = tv.table_id
                  AND sc.kind = ANY(:kinds)
                  AND sc.snapshot_id >= b.earliest
                  AND sc.snapshot_id <= b.snapshot
            ) c
            ORDER BY tv.name
        """

    private val tableSummaryMapper =
        RowMapper { rs, _ ->
            TableSummaryInfo(
                tableId = rs.getLong("table_id"),
                tableUuid = rs.getObject("table_uuid") as UUID,
                name = rs.getString("name"),
                comment = rs.getString("comment"),
                recordCount = rs.getLong("record_count"),
                fileCount = rs.getLong("file_count"),
                fileSizeBytes = rs.getLong("file_size_bytes"),
                snapshotCount = rs.getLong("snapshot_count"),
                // min() over no rows is SQL NULL, not 0: a table with no
                // retained change row has no earliest snapshot, and
                // getLong would report that as snapshot 0 — a real id.
                earliestSnapshotId = rs.getObject("earliest_snapshot_id", java.lang.Long::class.java)?.toLong(),
            )
        }

    /**
     * The column FOREST visible at [snapshot]: top-level columns in
     * ordinal order, each container carrying its children (also in
     * ordinal order, which is per-parent since V9).
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
                SELECT field_id, parent_field_id, name, col_type, type_params, nullable, ordinal, comment
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
