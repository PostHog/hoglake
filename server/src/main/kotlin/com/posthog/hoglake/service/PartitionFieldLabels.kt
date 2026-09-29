package com.posthog.hoglake.service

import com.posthog.hoglake.model.MAX_COLUMN_NESTING_DEPTH
import org.jdbi.v3.core.Handle

/**
 * The partition FIELDS of every spec vintage a set of tables has ever
 * had, keyed `(table_id, spec_id)` and ordered by `key_index`.
 *
 * Two surfaces need this and they need it at different depths, which is
 * why it is a shared object rather than a private method on either:
 *
 *  - `PartitionStatsService` labels a debt row's values (names only);
 *  - `PartitionListingService` labels AND decodes them, so it needs the
 *    transform and its parameter beside the name.
 *
 * NOT SNAPSHOT-SCOPED, deliberately. A sampled group carries the
 * `spec_id` its files were written under, which may be older than the
 * table's current spec, and the whole point of the stale-spec badge is
 * that those groups are labelled correctly rather than against head's
 * spec. `hog_partition_field` rows are keyed by spec id and survive the
 * spec's retirement (expiry's step 5 removes them only once the whole
 * version is below the floor), so one fetch per table covers every
 * vintage its samples can name.
 *
 * One statement for the fields plus one for the columns, both keyed on
 * `(catalog_id, table_id)` — bounded by the tables asked for, never by
 * the manifest.
 */
internal object PartitionFieldLabels {
    /**
     * One partition field of one spec: the display name the console
     * shows, and the transform that decodes its stored values.
     *
     * [name] follows the Iceberg convention and is the SAME rule the
     * webui applies (`webui/src/lib/partitions.ts#fieldLabel`): the
     * source column's dotted path for an identity transform,
     * `<path>_<transform>` otherwise ("ts_month").
     */
    data class Field(
        val keyIndex: Int,
        val name: String,
        val transform: String,
        val transformParam: Int?,
        val sourceFieldId: Long,
    )

    /**
     * (table_id, spec_id) -> fields in `key_index` order.
     *
     * The column name resolves to the LIVE version when one exists, else
     * the latest version (a dropped column's partitions still need a
     * label); a column with no versions at all (never legal) falls back
     * to "field_<id>".
     */
    fun fields(
        h: Handle,
        catalogId: Long,
        tableIds: List<Long>,
    ): Map<Pair<Long, Long>, List<Field>> {
        if (tableIds.isEmpty()) return emptyMap()
        val paths = columnPaths(h, catalogId, tableIds)
        return h.createQuery(
            """
            SELECT pf.table_id, pf.spec_id, pf.key_index, pf.transform,
                   pf.transform_param, pf.source_field_id
            FROM hog_partition_field pf
            WHERE pf.catalog_id = :catalogId AND pf.table_id IN (<tableIds>)
            ORDER BY pf.table_id, pf.spec_id, pf.key_index
            """,
        )
            .bind("catalogId", catalogId)
            .bindList("tableIds", tableIds)
            .map { rs, _ ->
                val tableId = rs.getLong("table_id")
                val sourceFieldId = rs.getLong("source_field_id")
                val column = paths[tableId to sourceFieldId] ?: "field_$sourceFieldId"
                val transform = rs.getString("transform")
                (tableId to rs.getLong("spec_id")) to
                    Field(
                        keyIndex = rs.getInt("key_index"),
                        name = if (transform == "identity") column else "${column}_$transform",
                        transform = transform,
                        transformParam = rs.getObject("transform_param")?.let { (it as Number).toInt() },
                        sourceFieldId = sourceFieldId,
                    )
            }
            .list()
            .groupBy({ it.first }, { it.second })
            .mapValues { (_, fields) -> fields.sortedBy { it.keyIndex } }
    }

    /** Just the display names, in key order — what a label-only caller needs. */
    fun names(
        h: Handle,
        catalogId: Long,
        tableIds: List<Long>,
    ): Map<Pair<Long, Long>, List<String>> = fields(h, catalogId, tableIds).mapValues { (_, f) -> f.map { it.name } }

    /**
     * The DOTTED PATH of every column of [tableIds], by (table, field id).
     *
     * A bare name is not a label: struct leaves are legal partition
     * sources, so two structs each holding a `zip` would both render
     * "zip" and the console would show one table partitioned twice by
     * the same apparent column. Resolved here rather than in SQL because
     * walking `parent_field_id` is a recursion over versioned rows, and
     * a Kotlin walk over one flat fetch reads better than a recursive
     * CTE with a per-level "prefer the live row" tie-break inside it.
     *
     * DISTINCT ON picks one version per field id — the live one, else the
     * most recent — so a dropped source still labels from its last
     * version.
     */
    private fun columnPaths(
        h: Handle,
        catalogId: Long,
        tableIds: List<Long>,
    ): Map<Pair<Long, Long>, String> {
        data class Node(val name: String, val parent: Long?)

        val nodes = mutableMapOf<Pair<Long, Long>, Node>()
        h.createQuery(
            """
            SELECT DISTINCT ON (table_id, field_id)
                   table_id, field_id, name, parent_field_id
            FROM hog_column
            WHERE catalog_id = :catalogId AND table_id IN (<tableIds>)
            ORDER BY table_id, field_id, (end_snapshot IS NULL) DESC, begin_snapshot DESC
            """,
        )
            .bind("catalogId", catalogId)
            .bindList("tableIds", tableIds)
            .map { rs, _ ->
                nodes[rs.getLong("table_id") to rs.getLong("field_id")] =
                    Node(
                        rs.getString("name"),
                        rs.getObject("parent_field_id", java.lang.Long::class.java)?.toLong(),
                    )
            }
            .list()

        return nodes.mapValues { (key, _) ->
            val (tableId, _) = key
            val segments = mutableListOf<String>()
            var cursor: Pair<Long, Long>? = key
            // Bounded by the depth cap, but guarded anyway: a doctored
            // catalog with a parent cycle must not spin a maintenance
            // read forever.
            var hops = 0
            while (cursor != null && hops <= MAX_COLUMN_NESTING_DEPTH) {
                val node = nodes[cursor] ?: break
                segments += node.name
                cursor = node.parent?.let { tableId to it }
                hops++
            }
            segments.reversed().joinToString(".")
        }
    }
}
