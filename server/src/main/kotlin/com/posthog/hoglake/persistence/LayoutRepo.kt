package com.posthog.hoglake.persistence

import org.jdbi.v3.core.Handle

/**
 * hog_table_layout reads (V29) — SpecRepo's sibling for the object-prefix
 * option ([com.posthog.hoglake.model.ObjectPrefix]). Visibility follows the
 * standard versioned-row rule (visible at S iff begin_snapshot <= S AND
 * (end_snapshot IS NULL OR S < end_snapshot)), and at most one row is live
 * per table (the unique `hog_table_layout_live`).
 *
 * Both return the FIELD ID, not a name: the name is the column's, resolved
 * by the caller at the same snapshot it reads the columns at. The commit
 * path does not come here; it reads the same rows in `CommitTail.READ_SQL`.
 */
object LayoutRepo {
    /**
     * The table's object-prefix field id visible at [snapshot], or null
     * when the option is off there. A PK-range read: the table's rows are
     * one per layout change, of which there are none or one today.
     */
    fun layoutAt(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        snapshot: Long,
    ): Long? =
        h.createQuery(
            """
            SELECT object_prefix_field_id FROM hog_table_layout
            WHERE catalog_id = :catalogId AND table_id = :tableId
              AND begin_snapshot <= :snapshot
              AND (end_snapshot IS NULL OR :snapshot < end_snapshot)
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("snapshot", snapshot)
            .mapTo(Long::class.javaObjectType)
            .findOne()
            .orElse(null)

    /**
     * `internal` so the V29 migration test EXPLAINs the statement [live]
     * sends: one descent of `hog_table_layout_live` per table id.
     */
    internal const val LIVE_SQL: String =
        """
        SELECT table_id, object_prefix_field_id FROM hog_table_layout
        WHERE catalog_id = :catalogId AND table_id = ANY(:tableIds) AND end_snapshot IS NULL
        """

    /** table_id -> object-prefix field id, for each of [tableIds] whose option is on at head. */
    fun live(
        h: Handle,
        catalogId: Long,
        tableIds: Collection<Long>,
    ): Map<Long, Long> {
        if (tableIds.isEmpty()) return emptyMap()
        return h.createQuery(LIVE_SQL)
            .bind("catalogId", catalogId)
            .bindArray("tableIds", Long::class.javaObjectType, tableIds.distinct())
            .map { rs, _ -> rs.getLong("table_id") to rs.getLong("object_prefix_field_id") }
            .list()
            .toMap()
    }
}
