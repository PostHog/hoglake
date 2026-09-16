package com.posthog.hoglake.model

/**
 * Stable field identities promised before a new table becomes visible.
 *
 * A new table's ids start at 1 and are assigned DEPTH-FIRST — a parent
 * before its children, a subtree before its next sibling — so a
 * container's id is always below its descendants' and a subtree's ids
 * stay contiguous. That is Iceberg's own assignment order, and it is
 * what `hog_column_parent_precedes_child` encodes as a CHECK.
 *
 * Depth-first matters to the atomic-creation receipt as much as to the
 * table: the receipt PROMISES these ids before any row exists, so a
 * flat `index + 1` would have promised one set and created another the
 * moment a column had children.
 */
fun initialColumns(definitions: List<ColumnDef>): List<Column> = assignFieldIds(definitions, 1)

/** How many hog_column rows (and field ids) this forest needs. */
fun nodeCount(definitions: List<ColumnDef>): Int =
    definitions.sumOf { 1 + (it.children?.let { c -> nodeCount(c) } ?: 0) }

/**
 * Assign field ids depth-first from [firstFieldId], and ordinals per
 * sibling group.
 *
 * Lives in `model` rather than beside the validation rules because both
 * the create path and the atomic-creation receipt need it and neither
 * should have to reach into a service to predict an id.
 */
fun assignFieldIds(
    definitions: List<ColumnDef>,
    firstFieldId: Long,
): List<Column> {
    var next = firstFieldId

    fun build(siblings: List<ColumnDef>): List<Column> =
        siblings.mapIndexed { ordinal, def ->
            val fieldId = next++
            Column(
                fieldId = fieldId,
                ordinal = ordinal,
                // children live on Column.children; clearing them on the
                // def keeps one source of truth for the subtree.
                def = def.copy(children = null),
                children = def.children?.let { build(it) } ?: emptyList(),
            )
        }
    return build(definitions)
}
