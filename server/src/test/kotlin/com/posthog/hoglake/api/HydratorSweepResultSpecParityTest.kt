package com.posthog.hoglake.api

import com.posthog.hoglake.model.HydratorSweepResult
import com.posthog.hoglake.wireObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File
import kotlin.reflect.full.memberProperties

/**
 * `HydratorSweepResult` on the WIRE, [RetirementResultSpecParityTest]'s
 * shape: no endpoint returns it, the ledger stores the MODEL's JSON and
 * hands it back, so the spec's schema is checked against the model by
 * REFLECTION here. Added with `stats_skipped_below_floor` (#432), the
 * first hydrator counter that postdates the ledger; `quarantined` (the
 * object-prefix check) is the second.
 *
 * Unit, not integration: one artifact on disk and the class it describes.
 */
class HydratorSweepResultSpecParityTest {
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

    private fun requiredOf(block: String): List<String> =
        Regex("""required: \[([^]]*)]""").find(block)!!.groupValues[1].split(",").map { it.trim() }

    @Test
    fun `the stored model and the spec declare exactly the same counters`() {
        // MUTATION: drop statsSkippedBelowFloor from the model (or the
        // property from the spec) and the two sets differ.
        val model = HydratorSweepResult::class.memberProperties.map { wire(it.name) }.sorted()
        assertThat(properties(schemaBlock("    HydratorSweepResult:")).sorted()).isEqualTo(model)
    }

    @Test
    fun `the counters added after the ledger are optional, the original four are required`() {
        val required = requiredOf(schemaBlock("    HydratorSweepResult:"))
        assertThat(required).containsExactlyInAnyOrder("claimed", "hydrated", "failed", "transient")
        assertThat(required).doesNotContain("stats_skipped_below_floor", "quarantined")
    }

    @Test
    fun `a zero below-floor count is omitted from the stored row, a nonzero one is written`() {
        // NON_DEFAULT, the stored-payload rule: the row an older replica
        // writes and the row this build writes for a floor-free sweep
        // are byte-identical.
        val zero =
            wireObjectMapper().valueToTree<com.fasterxml.jackson.databind.JsonNode>(
                HydratorSweepResult(3, 3, 0, 0),
            )
        assertThat(zero.has("stats_skipped_below_floor")).isFalse()
        val some =
            wireObjectMapper().valueToTree<com.fasterxml.jackson.databind.JsonNode>(
                HydratorSweepResult(3, 3, 0, 0, 2),
            )
        assertThat(some["stats_skipped_below_floor"].asLong()).isEqualTo(2)
    }

    @Test
    fun `a zero quarantined count is omitted from the stored row, a nonzero one is written`() {
        // The object-prefix check's counter, on the same rule.
        val zero =
            wireObjectMapper().valueToTree<com.fasterxml.jackson.databind.JsonNode>(
                HydratorSweepResult(3, 3, 0, 0, 1),
            )
        assertThat(zero.has("quarantined")).isFalse()
        val some =
            wireObjectMapper().valueToTree<com.fasterxml.jackson.databind.JsonNode>(
                HydratorSweepResult(3, 3, 0, 0, 1, quarantined = 2),
            )
        assertThat(some["quarantined"].asLong()).isEqualTo(2)
    }
}
