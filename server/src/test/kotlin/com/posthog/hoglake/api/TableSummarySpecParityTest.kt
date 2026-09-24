package com.posthog.hoglake.api

import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.wireObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File
import java.util.UUID

/**
 * `TableSummaryDto` against the OpenAPI `TableSummary` schema, both
 * read off the artifacts rather than restated here.
 *
 * The DTO side is a REAL SERIALIZATION through the wire mapper, not a
 * reflection walk: the wire is what a client parses, and the wire is
 * where the naming strategy and the NON_NULL inclusion actually apply.
 * A reflection list would have passed while `snapshot_count` shipped as
 * `snapshotCount`. The spec side is parsed out of the YAML block.
 *
 * Both directions matter, and they are different failures:
 *
 *  - a DTO property the spec never declares is a field no generated
 *    client can see;
 *  - a spec property the DTO never sends is a field a generated client
 *    declares and always finds missing.
 *
 * The `required` list gets its own assertion through the MINIMAL
 * serialization: NON_NULL omits exactly the nullable properties, so
 * "the keys a minimal row carries" is the server's own statement of
 * what is always present, and it must equal the spec's `required`. That
 * is why `comment` and `earliest_snapshot_id` need no literal mention
 * below — the two artifacts settle it between them.
 *
 * Unit test on purpose: it reads a file and serializes an object, so it
 * runs under `-PunitOnly` where a Docker-less machine can still catch
 * the drift.
 */
class TableSummarySpecParityTest {
    private val spec: String = File("src/main/resources/openapi/hoglake.yaml").readText()
    private val mapper = wireObjectMapper()

    /** The `TableSummary:` schema block, up to the next sibling schema. */
    private val block: String by lazy {
        val header = "    TableSummary:\n"
        val start = spec.indexOf(header)
        assertThat(start).describedAs("spec block TableSummary").isNotEqualTo(-1)
        val rest = spec.substring(start + header.length)
        val end = Regex("\n    [A-Za-z]").find(rest)?.range?.first ?: rest.length
        rest.substring(0, end)
    }

    /** Keys under the block's `properties:`, in declaration order. */
    private fun specProperties(): List<String> {
        val start = block.indexOf("      properties:\n")
        assertThat(start).describedAs("TableSummary has a properties block").isNotEqualTo(-1)
        return block.substring(start)
            .lines()
            .drop(1)
            .takeWhile { it.isBlank() || it.startsWith("        ") }
            .mapNotNull { Regex("^        ([a-z_]+):").find(it)?.groupValues?.get(1) }
    }

    /** The block's `required: [...]`, which the spec wraps across lines. */
    private fun specRequired(): List<String> {
        val body =
            Regex("""required: \[([^]]*)]""", RegexOption.DOT_MATCHES_ALL).find(block)!!.groupValues[1]
        return body.split(",").map { it.trim() }.filter { it.isNotEmpty() }
    }

    private fun keysOf(dto: TableSummaryDto): List<String> =
        mapper.readTree(mapper.writeValueAsString(dto)).fieldNames().asSequence().toList()

    private fun full() =
        TableSummaryDto(
            name = "events",
            tableUuid = UUID.fromString("3f2c9c04-8a1b-4c7e-9f10-6d2a5b3e8c71"),
            recordCount = 15,
            fileCount = 2,
            fileSizeBytes = 1500,
            snapshotCount = 3,
            comment = "raw pageview events",
            earliestSnapshotId = 2,
        )

    private fun minimal() = full().copy(comment = null, earliestSnapshotId = null)

    @Test
    fun `every TableSummaryDto property is a declared spec property, and vice versa`() {
        assertThat(keysOf(full()))
            .describedAs("the wire keys of a fully-populated TableSummary must be the spec's properties")
            .containsExactlyInAnyOrderElementsOf(specProperties())
    }

    @Test
    fun `the spec's required list is exactly what a minimal row always carries`() {
        // NON_NULL omission means the keys left on a row with every
        // nullable field unset ARE the always-present set.
        assertThat(keysOf(minimal()))
            .describedAs("a TableSummary with no comment and no retained history")
            .containsExactlyInAnyOrderElementsOf(specRequired())
    }

    @Test
    fun `the nullable properties are omitted rather than sent as JSON null`() {
        // A client that treats absent and null alike is fine either way,
        // but the spec says "absent", so the server must not be sending
        // nulls that a strict generated client would reject.
        val wire = mapper.writeValueAsString(minimal())
        assertThat(wire).doesNotContain("null")
        assertThat(keysOf(minimal())).doesNotContain("comment", "earliest_snapshot_id")
    }

    @Test
    fun `the spec's snapshot_count prose names every table-scoped change kind`() {
        // snapshot_count is DEFINED by which change kinds count, so the
        // definition in the spec is read back against the vocabulary the
        // query actually binds. Adding a table kind without saying so
        // here leaves the documented definition quietly wrong.
        val prose = block.substringAfter("        snapshot_count:")
        for (kind in ChangeKind.TABLE_SCOPED) {
            assertThat(prose)
                .describedAs("the spec's snapshot_count description must name '%s'", kind.wire)
                .contains(kind.wire)
        }
    }
}
