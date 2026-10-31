package com.posthog.hoglake.hoglite

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.util.UUID

/** Independent historical tenant + real upgrade path must equal a fresh tenant. */
@Tag("integration")
class HogliteSchemaEquivalenceIntegrationTest {
    @Test
    fun `folded tenant migrations match canonical DDL including columns constraints and indexes`() {
        HogliteConformanceEngine().use { engine ->
            val baseline =
                checkNotNull(javaClass.getResource("/hoglite/tenant-v0.sql")).readText()
                    .lineSequence().filterNot { it.trimStart().startsWith("--") }.joinToString("\n")
                    .split(';').map { it.trim() }.filter { it.isNotEmpty() }
            val migrated = create(engine, baseline)
            val canonical = create(engine, HogliteSchema.DDL)
            // The production first-operation upgrade, including its guards/probes.
            engine.store.ensureTenantSchema(migrated)
            assertThat(describe(engine, migrated)).isEqualTo(describe(engine, canonical))
            // A new process must be able to replay the upgrades without changing shape.
            HogliteCatalogStore(engine.client).ensureTenantSchema(migrated)
            assertThat(describe(engine, migrated)).isEqualTo(describe(engine, canonical))
        }
    }

    private fun create(
        engine: HogliteConformanceEngine,
        ddl: List<String>,
    ): String {
        val tenant = "schema-" + UUID.randomUUID()
        engine.client.createTenant(tenant)
        engine.client.tx(tenant, UUID.randomUUID().toString(), null, ddl.map { HogliteStatement(it) })
        return tenant
    }

    /** SQLite's own stored definitions include columns/defaults/checks/FKs and index predicates. */
    private fun describe(
        engine: HogliteConformanceEngine,
        tenant: String,
    ): List<String> =
        engine.client.query(
            tenant,
            "SELECT type, name, tbl_name, sql FROM sqlite_master " +
                "WHERE name NOT LIKE 'sqlite_%' AND name NOT LIKE '_hw_%' ORDER BY type, name",
        ).rows.map { row ->
            row.joinToString("|") { value ->
                value?.toString()?.replace(Regex("/\\*.*?\\*/", RegexOption.DOT_MATCHES_ALL), "")
                    ?.replace(Regex("(?i)IF NOT EXISTS\\s+"), "")
                    ?.replace(Regex("\\s+"), " ")
                    ?.replace(Regex("\\s*([(),])\\s*"), "$1")?.trim() ?: ""
            }
        }.sorted()
}
