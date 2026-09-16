package com.posthog.hoglake.service

import com.posthog.hoglake.model.HoglakeException

/**
 * Identifier policy for namespace, table, view, and column names
 * (decision 2026-09-05, after two reviews flagged that everything but
 * catalog names accepted slashes, markup, and 10KB blobs verbatim):
 * `^[A-Za-z_][A-Za-z0-9_-]{0,127}$`, enforced here (Validation -> 422)
 * and by CHECK constraints on hog_namespace.name, hog_table_version.name,
 * hog_view.name, and hog_column.name (V1__init.sql / schema.sql).
 *
 * Catalog names keep their own stricter CHECK (lowercase, 63 chars);
 * author/message/view SQL stay free-text by design.
 */
object Identifiers {
    const val PATTERN = "^[A-Za-z_][A-Za-z0-9_-]{0,127}$"

    /**
     * The prefix hoglake reserves for its own columns.
     *
     * Compaction outputs carry `_hog_row_id`, a physical column that is
     * not a catalog column and binds by a reserved field id. A user
     * column of that name collides with it head-on: the rewriter builds
     * an output schema holding the name twice, and parquet-java throws
     * an untyped decoding error from deep inside the writer instead of
     * the server refusing the DDL that made it possible. The whole `_hog`
     * prefix is reserved rather than the one name, so the next physical
     * column hoglake needs does not repeat this.
     *
     * Enforced at EVERY nesting level: a struct field named `_hog_row_id`
     * is only harmless by accident today, and "harmless today" is how the
     * top-level one got in.
     */
    const val RESERVED_COLUMN_PREFIX = "_hog"

    private val regex = Regex(PATTERN)

    /** Validate [name] as a [kind] identifier; violation -> Validation (422). */
    fun validate(
        kind: String,
        name: String,
    ) {
        if (!regex.matches(name)) {
            // Hostile names can be huge; keep the error line bounded.
            val shown = if (name.length > 64) name.take(61) + "..." else name
            throw HoglakeException.Validation(
                "invalid $kind name '$shown' (must match $PATTERN)",
            )
        }
    }

    /**
     * Validate a COLUMN name at any nesting level: the identifier policy
     * plus the [RESERVED_COLUMN_PREFIX] refusal. [qualified] is the
     * dotted path for the message (the bare name for a top-level
     * column), so a rejected struct field says where it is.
     */
    fun validateColumn(
        name: String,
        qualified: String = name,
    ) {
        validate("column", name)
        if (name.startsWith(RESERVED_COLUMN_PREFIX)) {
            throw HoglakeException.Validation(
                "column name '$qualified' uses the reserved prefix " +
                    "'$RESERVED_COLUMN_PREFIX': names starting with it belong to hoglake's own " +
                    "physical columns (compaction's _hog_row_id)",
            )
        }
    }
}
