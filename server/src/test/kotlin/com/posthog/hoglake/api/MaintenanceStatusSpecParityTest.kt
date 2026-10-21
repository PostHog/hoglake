package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.node.ObjectNode
import com.posthog.hoglake.model.MaintenanceStatus
import com.posthog.hoglake.wireObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File
import java.time.Instant

/**
 * The `MaintenanceStatus` catalog entry on the wire against the spec's
 * schema, [FindingResolutionSpecParityTest]'s shape.
 *
 * Added with `sample_reanchored`: the sampler now re-anchors a scan that
 * expiry overtakes instead of restarting it (`MaintenanceSummarySampler`),
 * and a re-anchored sample's file measures are an observation over its
 * window rather than exact at `sampled_snapshot_id`. The status page is
 * where an operator reads which of the two a sample is, so the field and
 * the spec must not drift.
 */
class MaintenanceStatusSpecParityTest {
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

    private fun wire(status: MaintenanceStatus): ObjectNode = wireObjectMapper().valueToTree(status.toDto())

    private val sampled =
        MaintenanceStatus(
            catalog = "c",
            tasks = emptyList(),
            sampledAt = Instant.parse("2026-10-21T00:00:00Z"),
            sampleStartedAt = Instant.parse("2026-10-20T23:00:00Z"),
            sampledSnapshotId = 41,
            sampleReanchored = 3,
        )

    @Test
    fun `every property the server writes is in the spec and the spec has no extra`() {
        val block = schemaBlock("    MaintenanceStatus:")
        // MUTATION: drop `sampleReanchored` from MaintenanceStatusDto and
        // the wire set loses `sample_reanchored` while the spec keeps it.
        assertThat(wire(sampled).fieldNames().asSequence().toList().sorted())
            .isEqualTo(properties(block).sorted())
        assertThat(wire(sampled)["sample_reanchored"].asInt()).isEqualTo(3)
        val required =
            Regex("""required: \[([^]]*)]""").find(block)!!.groupValues[1].split(",").map { it.trim() }
        assertThat(required).containsExactlyInAnyOrder("catalog", "tasks")
    }

    @Test
    fun `the sample fields are absent together during warmup`() {
        val warm = wire(MaintenanceStatus(catalog = "c", tasks = emptyList()))
        for (field in listOf("sampled_at", "sample_started_at", "sampled_snapshot_id", "sample_reanchored")) {
            assertThat(warm.has(field)).describedAs(field).isFalse()
        }
    }

    @Test
    fun `a sample that never re-anchored reports zero, not absent`() {
        assertThat(wire(sampled.copy(sampleReanchored = 0))["sample_reanchored"].asInt()).isZero()
    }
}
