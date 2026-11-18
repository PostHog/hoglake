package com.posthog.hoglake.api

import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.wireObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File
import java.time.Instant
import java.util.UUID

/**
 * `TableDto` against the OpenAPI `Table` schema, as `TableSummarySpecParityTest` checks its sibling: the wire keys
 * of a fully populated table answer are exactly the properties the spec declares, `remote` among them.
 */
class TableSpecParityTest {
    private val spec: String = File("src/main/resources/openapi/hoglake.yaml").readText()
    private val mapper = wireObjectMapper()
    private val block: String =
        spec.substringAfter("\n    Table:\n").let { it.substring(0, Regex("\n    [A-Za-z]").find(it)!!.range.first) }

    private fun keys(dto: TableDto) = mapper.readTree(mapper.writeValueAsString(dto)).fieldNames().asSequence().toList()

    private val minimal = TableDto("events", "ns", UUID.randomUUID(), emptyList(), readSnapshotId = 9)

    @Test
    fun `every property the spec declares is sent by a full table answer, and nothing else is`() {
        val full =
            minimal.copy(
                recordCount = 1, fileCount = 1, fileSizeBytes = 1, totalsSnapshotId = 8, totalsAsOf = Instant.EPOCH,
                partitionSpec = AlterPartitionSpecDto(1, emptyList()), sortSpec = AlterSortSpecDto(1, emptyList()),
                objectPrefixField = "team_id", comment = "c", snapshotId = 9, remote = true,
            )
        val declared =
            block.substringAfter("      properties:\n").lines()
                .mapNotNull { Regex("^        ([a-z_]+):").find(it)?.groupValues?.get(1) }
        assertThat(keys(full)).containsExactlyInAnyOrderElementsOf(declared)
        assertThat(block.substringAfter("        remote:\n")).startsWith("          type: boolean\n")
    }

    @Test
    fun `a table converted for any answer but the served one has no remote key`() {
        val info = TableInfo(1, UUID.randomUUID(), "ns", "events", emptyList(), 9, 1, 1, 1, snapshotId = 9)
        assertThat(keys(info.toDto())).doesNotContain("remote")
    }
}
