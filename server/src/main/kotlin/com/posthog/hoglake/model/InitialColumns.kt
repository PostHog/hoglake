package com.posthog.hoglake.model

/** Stable field identities promised before a new table becomes visible. */
fun initialColumns(definitions: List<ColumnDef>): List<Column> =
    definitions.mapIndexed { index, definition -> Column(index.toLong() + 1, index, definition) }
