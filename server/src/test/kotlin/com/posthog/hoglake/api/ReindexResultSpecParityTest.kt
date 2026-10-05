package com.posthog.hoglake.api

import com.posthog.hoglake.model.ReindexResult
import com.posthog.hoglake.model.ReindexSkip
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File
import kotlin.reflect.full.memberProperties

/**
 * `ReindexResult` on the wire, [RetirementResultSpecParityTest]'s shape
 * and for its reason: no endpoint returns it, the ledger stores the MODEL
 * serialized with the wire mapper, so the spec's schema is referenced by
 * prose alone and only a parse-and-reflect check keeps the two honest.
 *
 * One difference: the nullable fields are ABSENT when null (the wire
 * mapper is NON_NULL), so they are not `required`; the three counters
 * are, because every run carries them.
 */
class ReindexResultSpecParityTest {
    private val spec: String = File("src/main/resources/openapi/hoglake.yaml").readText()

    private fun wire(name: String): String = name.replace(Regex("([a-z0-9])([A-Z])"), "$1_$2").lowercase()

    private fun schemaBlock(header: String): String {
        val start = spec.indexOf(header)
        assertThat(start).describedAs("spec block %s", header).isNotEqualTo(-1)
        val rest = spec.substring(start + header.length)
        val end = Regex("\n    [A-Za-z]").find(rest)?.range?.first ?: rest.length
        return rest.substring(0, end)
    }

    private fun properties(block: String): List<String> =
        Regex("""(?m)^        ([a-z_0-9]+):""").findAll(block).map { it.groupValues[1] }.toList()

    @Test
    fun `the stored model and the spec declare exactly the same fields`() {
        val model = ReindexResult::class.memberProperties.map { wire(it.name) }.sorted()
        assertThat(properties(schemaBlock("    ReindexResult:")).sorted()).isEqualTo(model)
    }

    @Test
    fun `required is exactly the non-nullable fields`() {
        val block = schemaBlock("    ReindexResult:")
        val required =
            Regex("""required: \[([^]]*)]""").find(block)!!.groupValues[1].split(",").map { it.trim() }
        val nonNull =
            ReindexResult::class.memberProperties.filterNot { it.returnType.isMarkedNullable }.map { wire(it.name) }
        assertThat(required).containsExactlyInAnyOrderElementsOf(nonNull)
    }

    @Test
    fun `the skipped_reason vocabulary is the enum's`() {
        val block = schemaBlock("    ReindexResult:")
        val enum =
            Regex("""skipped_reason:[\s\S]*?enum: \[([^]]*)]""").find(block)!!.groupValues[1]
                .split(",").map { it.trim() }
        assertThat(enum).containsExactlyInAnyOrderElementsOf(ReindexSkip.entries.map { it.wire })
    }

    @Test
    fun `the task vocabulary in the spec carries reindex`() {
        val block = schemaBlock("    MaintenanceTask:")
        assertThat(block).contains("reindex")
    }
}
