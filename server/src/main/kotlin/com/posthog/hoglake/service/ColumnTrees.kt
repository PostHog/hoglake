package com.posthog.hoglake.service

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MAX_COLUMN_NESTING_DEPTH
import com.posthog.hoglake.model.columnDefDepth

/**
 * The column-TREE rules: what a nested [ColumnDef] must look like before
 * a field id is allocated for any part of it, and how ids are assigned
 * once it is legal.
 *
 * Every refusal here is a NAMED 422 — "list requires exactly one child
 * (its element)", not "unknown column type" and never a 500 further
 * down. `list`, `struct` and `map` used to fall through [ColType]'s
 * unknown-type error; now that they are real types, a caller who sends
 * a bare `{"type":"list"}` must be told the one thing missing.
 *
 * Shape rules (Iceberg's, not invented here — see
 * iceberg-federation.md §2.8):
 *
 *  - `list` has exactly one child, named `element`. Its nullability is
 *    declarable (Iceberg's `element-required`).
 *  - `map` has exactly two children, `key` then `value`. The key is
 *    REQUIRED: Iceberg's map keys are non-nullable, and a nullable key
 *    has no representation in the parquet MAP shape either.
 *  - `struct` has one or more children, which keep the user's names and
 *    are validated by the ordinary identifier policy.
 *  - a scalar type has no children at all.
 *
 * Depth is capped at [MAX_COLUMN_NESTING_DEPTH], checked BEFORE the
 * per-node walk so a pathological request never recurses to find out it
 * was illegal.
 */
object ColumnTrees {
    /**
     * Validate a forest of requested columns (a create-table column
     * list, or the single def of an add_column). [depthOffset] is the
     * depth already consumed by the parent chain — zero for a top-level
     * request, `n` when adding into a struct that is itself `n` levels
     * deep — so the cap means the same thing wherever a subtree is
     * grafted on.
     */
    fun validate(
        defs: List<ColumnDef>,
        depthOffset: Int = 0,
    ) {
        val depth = depthOffset + columnDefDepth(defs)
        if (depth > MAX_COLUMN_NESTING_DEPTH) {
            throw HoglakeException.Validation(
                "column nesting depth $depth exceeds the maximum $MAX_COLUMN_NESTING_DEPTH " +
                    "(a top-level column is depth 1)",
            )
        }
        validateSiblings(defs, path = emptyList())
    }

    private fun validateSiblings(
        defs: List<ColumnDef>,
        path: List<String>,
    ) {
        val dupes = defs.groupingBy { it.name }.eachCount().filterValues { it > 1 }.keys
        if (dupes.isNotEmpty()) {
            val where = if (path.isEmpty()) "" else " in '${path.joinToString(".")}'"
            throw HoglakeException.Validation("duplicate column names$where: ${dupes.sorted()}")
        }
        for (def in defs) validateNode(def, path)
    }

    private fun validateNode(
        def: ColumnDef,
        path: List<String>,
    ) {
        val here = path + def.name
        val qualified = here.joinToString(".")
        val children = def.children ?: emptyList()

        if (!def.type.isNested) {
            // PRESENT, not non-empty. `"children": []` on a scalar is
            // still a caller stating something about this column that is
            // not true, and normalising it to "omitted" answered 201 to
            // a request the error message promises a 422 for. The two
            // shapes get the same message because they are the same
            // mistake.
            if (def.children != null) {
                throw HoglakeException.Validation(
                    "column '$qualified' is '${def.type.wire}', a scalar type, and cannot have children",
                )
            }
            return
        }

        // type_params describe a SCALAR's parameters (decimal's precision
        // and scale, and nothing else today). A container's shape lives
        // in its children, so a container carrying them means the caller
        // believes something about this column that is not true — and
        // accepting them persists that belief into hog_column.type_params
        // and hands it back on every read. Symmetric with the
        // children-on-a-scalar refusal above: each type takes exactly the
        // one it has a meaning for.
        if (def.typeParams != null && def.typeParams.isNotEmpty()) {
            throw HoglakeException.Validation(
                "column '$qualified' is '${def.type.wire}', a nested container, and cannot have " +
                    "type_params: a container's shape is its children, not its parameters",
            )
        }

        val required = def.type.requiredChildCount
        if (required != null && children.size != required) {
            throw HoglakeException.Validation(nestedArityMessage(def.type, qualified, children.size))
        }
        if (def.type == ColType.STRUCT && children.isEmpty()) {
            throw HoglakeException.Validation(
                "struct column '$qualified' requires at least one child field",
            )
        }

        val synthetic = ColType.syntheticChildNames(def.type)
        if (synthetic != null) {
            children.forEachIndexed { i, child ->
                if (child.name != synthetic[i]) {
                    throw HoglakeException.Validation(
                        "child ${i + 1} of ${def.type.wire} column '$qualified' must be named " +
                            "'${synthetic[i]}' (Iceberg's synthetic name), not '${child.name}'",
                    )
                }
            }
            if (def.type == ColType.MAP && children[0].nullable) {
                throw HoglakeException.Validation(
                    "the key of map column '$qualified' must be required (nullable=false): " +
                        "Iceberg map keys are non-nullable",
                )
            }
        } else {
            // struct children are user-named, so they face the ordinary
            // identifier policy; list/map children are synthetic names
            // this code produced and already match it.
            for (child in children) Identifiers.validate("column", child.name)
        }
        validateSiblings(children, here)
    }

    /** The named arity refusal for a container with the wrong child count. */
    private fun nestedArityMessage(
        type: ColType,
        qualified: String,
        actual: Int,
    ): String =
        when (type) {
            ColType.LIST ->
                "list column '$qualified' requires exactly one child (its 'element'); got $actual"
            ColType.MAP ->
                "map column '$qualified' requires exactly two children ('key' then 'value'); got $actual"
            else -> "column '$qualified' of type '${type.wire}' has the wrong number of children: $actual"
        }

    /** How many hog_column rows (and field ids) this forest needs. */
    fun nodeCount(defs: List<ColumnDef>): Int = defs.sumOf { 1 + (it.children?.let { c -> nodeCount(c) } ?: 0) }

    /**
     * Assign field ids DEPTH-FIRST from [firstFieldId] (a parent before
     * its children, a subtree before its next sibling) and ordinals per
     * sibling group. Depth-first is the order Iceberg's own schema
     * assignment uses, and it keeps a subtree's ids contiguous, which is
     * what makes "drop this struct" a range in the versioned rows rather
     * than a scatter.
     */
    fun assignFieldIds(
        defs: List<ColumnDef>,
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
        return build(defs)
    }
}
