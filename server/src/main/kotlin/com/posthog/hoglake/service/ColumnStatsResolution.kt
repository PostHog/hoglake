package com.posthog.hoglake.service

import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.FileColumnStats

/** Every node of the column forest keyed by field id, with its dotted path. */
internal fun columnsByFieldId(forest: List<Column>): Map<Long, Pair<String, Column>> {
    val out = mutableMapOf<Long, Pair<String, Column>>()

    fun walk(
        columns: List<Column>,
        prefix: String,
    ) {
        for (column in columns) {
            val path = if (prefix.isEmpty()) column.def.name else "$prefix.${column.def.name}"
            out[column.fieldId] = path to column
            walk(column.children, path)
        }
    }
    walk(forest, "")
    return out
}

/**
 * Stored stats rows joined to their column identity — the one join
 * behind both GET .../files/{fileId}/stats and the scan plan's
 * `column_stats`, so the two cannot disagree about which rows appear.
 *
 * Reflective, never generative: one entry per row whose field id
 * resolves in [byFieldId], in the order [rows] came. A row whose field
 * id is not visible (a dropped column) is omitted, because without a
 * column there is no type to decode its bounds under.
 */
internal fun resolveColumnStats(
    rows: List<ColumnStats>,
    byFieldId: Map<Long, Pair<String, Column>>,
): List<FileColumnStats> =
    rows.mapNotNull { row ->
        byFieldId[row.fieldId]?.let { (path, column) ->
            FileColumnStats(
                fieldId = row.fieldId,
                name = column.def.name,
                path = path,
                type = column.def.type,
                typeParams = column.def.typeParams,
                stats = row,
            )
        }
    }
