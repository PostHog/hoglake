package com.posthog.hoglake.service

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MAX_COLUMN_NESTING_DEPTH
import com.posthog.hoglake.model.columnDefDepth
import com.posthog.hoglake.model.nodeCount

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
        existingNodes: Int = 0,
    ) {
        // Size FIRST, and by NODES: a forest counted by its roots hides
        // its real cost by its fan-out, and the walks below should never
        // run over a request this cap already refuses.
        val nodes = existingNodes + nodeCount(defs)
        if (nodes > MAX_COLUMN_NODES) {
            throw HoglakeException.Validation(
                "too many columns: $nodes nested column nodes exceeds the maximum $MAX_COLUMN_NODES",
            )
        }
        // Bail one past the cap: every depth beyond it means the same
        // refusal, and walking a twenty-thousand-level request to learn
        // its exact depth is work spent on an answer nobody reads.
        val cap = MAX_COLUMN_NESTING_DEPTH + 2
        val measured = columnDefDepth(defs, cap)
        val depth = depthOffset + measured
        if (depth > MAX_COLUMN_NESTING_DEPTH) {
            val shown = if (measured == cap) "$depth or more" else "$depth"
            throw HoglakeException.Validation(
                "column nesting depth $shown exceeds the maximum $MAX_COLUMN_NESTING_DEPTH " +
                    "(a top-level column is depth 1)",
            )
        }
        validateSiblings(defs, path = emptyList(), syntheticallyNamed = false)
    }

    private fun validateSiblings(
        defs: List<ColumnDef>,
        path: List<String>,
        syntheticallyNamed: Boolean,
    ) {
        val dupes = defs.groupingBy { it.name }.eachCount().filterValues { it > 1 }.keys
        if (dupes.isNotEmpty()) {
            val where = if (path.isEmpty()) "" else " in '${path.joinToString(".")}'"
            throw HoglakeException.Validation("duplicate column names$where: ${dupes.sorted()}")
        }
        for (def in defs) validateNode(def, path, syntheticallyNamed)
    }

    private fun validateNode(
        def: ColumnDef,
        path: List<String>,
        syntheticallyNamed: Boolean,
    ) {
        val here = path + def.name
        val qualified = here.joinToString(".")
        val children = def.children ?: emptyList()

        // Names are policed HERE, at every level, rather than only on
        // the top-level list the callers pass: a struct field carrying a
        // reserved prefix (or any name the identifier policy refuses) is
        // the same defect one level down. Synthetic names — the
        // element/key/value this code mandates — are checked against
        // the literal expected name below instead.
        if (!syntheticallyNamed) Identifiers.validateColumn(def.name, qualified)

        // Decimal's parameters, which nothing validated. Found by the
        // sweep the `version` narrowing prompted, one layer out from the
        // codec: the question is not only "is the value the right kind"
        // but "does it survive the accessor that reads it", and
        // `(typeParams["precision"] as? Number).toInt()` in the
        // compaction rewriter NARROWS — a precision of 4294967297,
        // stored faithfully as a Long, came back as 1 and every value in
        // the column became an over-precision refusal. Out-of-range but
        // in-int values (0, 99) were simply accepted and produced a
        // parquet annotation parquet itself refuses.
        if (def.type == ColType.DECIMAL) {
            val precision = decimalParam(def, "precision", qualified)
            val scale = decimalParam(def, "scale", qualified)
            if (precision != null && precision !in 1..MAX_DECIMAL_PRECISION) {
                throw HoglakeException.Validation(
                    "decimal column '$qualified' has precision $precision, outside " +
                        "1..$MAX_DECIMAL_PRECISION",
                )
            }
            if (scale != null && scale < 0) {
                throw HoglakeException.Validation("decimal column '$qualified' has a negative scale $scale")
            }
            // Scale counts digits to the right of the point, so it
            // cannot exceed the total the precision allows.
            val effective = precision ?: MAX_DECIMAL_PRECISION
            if (scale != null && scale > effective) {
                throw HoglakeException.Validation(
                    "decimal column '$qualified' has scale $scale above its precision $effective",
                )
            }
        }

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
        // PRESENT, not non-empty — the same criterion the scalar
        // children check uses above. `"type_params": {}` on a container
        // is still a caller claiming this column has parameters, and
        // normalising it to "omitted" answered 201 to a request this
        // message promises a 422 for.
        if (def.typeParams != null) {
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
        }
        validateSiblings(children, here, syntheticallyNamed = synthetic != null)
    }

    /**
     * The widest decimal parquet and Iceberg both express. A column
     * declared past it produces an annotation parquet refuses at write
     * time, which is a 500 from inside the writer rather than a 422 here.
     */
    const val MAX_DECIMAL_PRECISION = 38

    /**
     * One decimal parameter as an Int, or null when absent — refusing
     * anything that is not an integer that FITS. `as? Number` followed
     * by `toInt()` is what the rest of the server does with these, and
     * `toInt()` on a Long truncates silently.
     */
    private fun decimalParam(
        def: ColumnDef,
        key: String,
        qualified: String,
    ): Int? {
        val raw = def.typeParams?.get(key) ?: return null
        val asLong =
            when (raw) {
                is Int -> raw.toLong()
                is Long -> raw
                is Short, is Byte -> (raw as Number).toLong()
                else ->
                    // CAPPED, like the codec's echoes. This is a 422
                    // body, `raw` is whatever JSON the caller sent, and
                    // a map value can be arbitrarily large — quoting it
                    // whole would put a caller-sized blob in an error
                    // response and in every log line that records one.
                    throw HoglakeException.Validation(
                        "decimal column '$qualified' has a non-integer '$key' (${capped(raw)})",
                    )
            }
        if (asLong !in Int.MIN_VALUE.toLong()..Int.MAX_VALUE.toLong()) {
            throw HoglakeException.Validation(
                "decimal column '$qualified' has a '$key' outside the int range ($asLong)",
            )
        }
        return asLong.toInt()
    }

    /** A caller-supplied value, capped for an error message. */
    private fun capped(value: Any): String {
        val text = value.toString()
        return if (text.length > 40) text.take(37) + "..." else text
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

    /**
     * Ceiling on a table's total column NODES.
     *
     * Nodes, not top-level columns: each one is a hog_column row and a
     * field id, so a forest counted by its roots hides its real cost by
     * a factor of its fan-out.
     *
     * Enforced in [validate], which every DDL path runs, rather than in
     * one service: it started life capping only the atomic-creation
     * PREPARE path, which left plain createTable and add_column able to
     * build the very forest prepare refused. ALTER passes the table's
     * existing node count as `existingNodes`, so the cap is on the
     * POST-GRAFT total — a cap on the addition alone is no cap at all
     * when the caller can add repeatedly.
     */
    const val MAX_COLUMN_NODES = 10000

    /** How many hog_column rows (and field ids) this forest needs. */
    fun nodeCount(defs: List<ColumnDef>): Int = com.posthog.hoglake.model.nodeCount(defs)

    /**
     * Assign field ids DEPTH-FIRST from [firstFieldId] (a parent before
     * its children, a subtree before its next sibling) and ordinals per
     * sibling group.
     *
     * Delegates to the model, which is where the atomic-creation
     * receipt reaches for the same answer: the ids a table WILL have are
     * promised before any row exists, so the prediction and the
     * assignment have to be one function.
     */
    fun assignFieldIds(
        defs: List<ColumnDef>,
        firstFieldId: Long,
    ): List<Column> = com.posthog.hoglake.model.assignFieldIds(defs, firstFieldId)
}
