package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.node.ObjectNode
import com.posthog.hoglake.model.DatabaseFinding
import com.posthog.hoglake.model.FindingResolution
import com.posthog.hoglake.model.FindingSeverity
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.ResolutionKind
import com.posthog.hoglake.wireObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.io.File

/**
 * `FindingResolution` on the wire, [ReindexResultSpecParityTest]'s
 * shape: the spec's `kind` enum is the Kotlin enum's, `task` is ABSENT
 * (not null) off the maintenance kind, and the model refuses a
 * resolution that names a task without the kind that owns it.
 */
class FindingResolutionSpecParityTest {
    private val spec: String = File("src/main/resources/openapi/hoglake.yaml").readText()

    private fun schemaBlock(header: String): String {
        val start = spec.indexOf(header)
        assertThat(start).describedAs("spec block %s", header).isNotEqualTo(-1)
        val rest = spec.substring(start + header.length)
        val end = Regex("\n    [A-Za-z]").find(rest)?.range?.first ?: rest.length
        return rest.substring(0, end)
    }

    private fun properties(block: String): List<String> =
        Regex("""(?m)^        ([a-z_0-9]+):""").findAll(block).map { it.groupValues[1] }.toList()

    private fun finding(resolution: FindingResolution) =
        DatabaseFinding(FindingSeverity.WARN, "x", "t", "d", "i", resolution)

    private fun wire(resolution: FindingResolution): ObjectNode =
        wireObjectMapper().valueToTree<ObjectNode>(finding(resolution).toDto())["resolution"] as ObjectNode

    @Test
    fun `fields, required and the kind enum match the spec`() {
        val block = schemaBlock("    FindingResolution:")
        assertThat(properties(block).sorted()).isEqualTo(listOf("kind", "task", "text"))
        val required =
            Regex("""required: \[([^]]*)]""").find(block)!!.groupValues[1].split(",").map { it.trim() }
        assertThat(required).containsExactlyInAnyOrder("kind", "text")
        val enum =
            Regex("""kind:[\s\S]*?enum: \[([^]]*)]""").find(block)!!.groupValues[1].split(",").map { it.trim() }
        assertThat(enum).containsExactlyInAnyOrderElementsOf(ResolutionKind.entries.map { it.wire })
        assertThat(schemaBlock("    DatabaseFinding:")).contains("resolution:")
    }

    @Test
    fun `task is absent, not null, off the maintenance kind`() {
        val operator = wire(FindingResolution(ResolutionKind.OPERATOR, null, "by hand"))
        assertThat(operator["kind"].asText()).isEqualTo("operator")
        assertThat(operator.has("task")).isFalse()
        val maintenance = wire(FindingResolution(ResolutionKind.MAINTENANCE, MaintenanceTask.REINDEX, "tomorrow"))
        assertThat(maintenance["kind"].asText()).isEqualTo("maintenance")
        assertThat(maintenance["task"].asText()).isEqualTo("reindex")
    }

    @Test
    fun `a task is named exactly when maintenance owns the finding`() {
        assertThatThrownBy { FindingResolution(ResolutionKind.MAINTENANCE, null, "nobody") }
            .isInstanceOf(IllegalArgumentException::class.java)
        assertThatThrownBy { FindingResolution(ResolutionKind.WATCH, MaintenanceTask.REINDEX, "nobody") }
            .isInstanceOf(IllegalArgumentException::class.java)
    }
}
