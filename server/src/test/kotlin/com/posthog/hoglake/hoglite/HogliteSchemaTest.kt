package com.posthog.hoglake.hoglite

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Schema shape that the read paths depend on. The query plans were
 * checked with SQLite's EXPLAIN QUERY PLAN; these tests keep the shape.
 */
class HogliteSchemaTest {
    private fun normalized(sql: String) = sql.replace(Regex("\\s+"), " ").trim()

    /**
     * tableAt and the live table lookups filter table_versions on
     * (namespace_id, name). Without an index that starts with those
     * columns, SQLite scans every table and its retained versions.
     */
    @Test
    fun `table_versions has a non-partial index that starts with namespace_id and name`() {
        val index =
            HogliteSchema.DDL.map(::normalized).filter {
                it.startsWith("CREATE INDEX") && it.contains(" ON table_versions(namespace_id, name")
            }

        assertThat(index).hasSize(1)
        assertThat(index.single()).doesNotContain("WHERE")
    }
}
