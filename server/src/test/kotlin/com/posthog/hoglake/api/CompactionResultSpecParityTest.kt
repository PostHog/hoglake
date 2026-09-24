package com.posthog.hoglake.api

import com.posthog.hoglake.model.CompactionResult
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File
import kotlin.reflect.full.memberProperties

/**
 * `CompactionResult` on the WIRE, read off both sides rather than
 * restated in either.
 *
 * There are three copies of this counter set and nothing kept them in
 * step: the stored model (`CompactionResult`, which the maintenance run
 * ledger serializes), the response DTO (`CompactionResultDto`, which
 * `POST /maintenance/compaction` returns), and the OpenAPI schema. When
 * `claimed_elsewhere` was added, the model and the spec got it and the
 * DTO did not — so the endpoint declared a field it could never emit,
 * and the only thing that would have noticed was a client generated
 * from the spec.
 *
 * The DTO's property list comes from REFLECTION and the spec's from
 * parsing the file, so neither side is a literal here. A literal would
 * have been a fourth copy.
 *
 * Unit, not integration: no database, no container — just two artifacts
 * on disk and the classes they describe.
 */
class CompactionResultSpecParityTest {
    private val spec: String = File("src/main/resources/openapi/hoglake.yaml").readText()

    /** snake_case, the wire's spelling, for a Kotlin property name. */
    private fun wire(name: String): String = name.replace(Regex("([a-z0-9])([A-Z])"), "$1_$2").lowercase()

    private fun schemaBlock(header: String): String {
        val start = spec.indexOf(header)
        assertThat(start).describedAs("spec block %s", header).isNotEqualTo(-1)
        val rest = spec.substring(start + header.length)
        val end = Regex("\n    [A-Za-z]").find(rest)?.range?.first ?: rest.length
        return rest.substring(0, end)
    }

    /** The schema's own property keys, at its indentation. */
    private fun properties(block: String): List<String> =
        Regex("""(?m)^        ([a-z_0-9]+):""").findAll(block).map { it.groupValues[1] }.toList()

    private fun requiredOf(block: String): List<String> {
        val match = Regex("""required: \[([^]]*)]""", RegexOption.DOT_MATCHES_ALL).find(block)
        assertThat(match).describedAs("CompactionResult has no `required:` list").isNotNull()
        return match!!.groupValues[1].split(",").map { it.trim() }.filter { it.isNotEmpty() }
    }

    @Test
    fun `the response DTO and the spec declare exactly the same counters`() {
        val dto = CompactionResultDto::class.memberProperties.map { wire(it.name) }.sorted()
        val declared = properties(schemaBlock("    CompactionResult:")).sorted()
        assertThat(dto)
            .describedAs(
                "every property the spec declares must exist on the DTO that answers the " +
                    "endpoint, and vice versa — a spec-only field is one no client can ever " +
                    "receive",
            )
            .isEqualTo(declared)
    }

    @Test
    fun `the stored model and the response DTO carry the same counters`() {
        // The ledger's model and the endpoint's DTO are converted one to
        // the other by hand (`CompactionResult.toDto()`), so a counter
        // can be added to the model, stored in the ledger, and never
        // reach the endpoint. That is exactly what happened.
        val model = CompactionResult::class.memberProperties.map { wire(it.name) }.sorted()
        val dto = CompactionResultDto::class.memberProperties.map { wire(it.name) }.sorted()
        assertThat(dto).isEqualTo(model)
    }

    @Test
    fun `toDto copies every counter, with no field left at its default`() {
        // `isEqualTo` on the name lists above cannot see a `toDto` that
        // maps a field to the WRONG source, or forgets one and lets the
        // data class default cover for it. Distinct values per field
        // make either visible.
        val result =
            CompactionResult(
                groupsCompacted = 1,
                filesIn = 2,
                filesOut = 3,
                bytesIn = 4,
                bytesOut = 5,
                skippedConflicts = 6,
                dvSuperseded = 7,
                unconvertibleSchema = 8,
                invalidData = 9,
                heapBudgetExceeded = 10,
                failedGroups = 11,
                claimedElsewhere = 12,
            )
        val dto = result.toDto()
        assertThat(dto.groupsCompacted).isEqualTo(1)
        assertThat(dto.filesIn).isEqualTo(2)
        assertThat(dto.filesOut).isEqualTo(3)
        assertThat(dto.bytesIn).isEqualTo(4)
        assertThat(dto.bytesOut).isEqualTo(5)
        assertThat(dto.skippedConflicts).isEqualTo(6)
        assertThat(dto.dvSuperseded).isEqualTo(7)
        assertThat(dto.unconvertibleSchema).isEqualTo(8)
        assertThat(dto.invalidData).isEqualTo(9)
        assertThat(dto.heapBudgetExceeded).isEqualTo(10)
        assertThat(dto.failedGroups).isEqualTo(11)
        assertThat(dto.claimedElsewhere)
            .describedAs("the counter whose absence made the endpoint unable to report it")
            .isEqualTo(12)
    }

    @Test
    fun `claimed_elsewhere is optional on the wire, like every counter added after the ledger`() {
        // NON_DEFAULT on the stored model means a zero is omitted from a
        // ledger row, and rows older than the counter never had it at
        // all — so `required` would be a promise the ledger's own
        // history breaks. The read path fills 0, which is why absence
        // reads as zero rather than as unknown.
        val block = schemaBlock("    CompactionResult:")
        assertThat(properties(block)).contains("claimed_elsewhere")
        assertThat(requiredOf(block))
            .describedAs("a required counter cannot be one the ledger has rows without")
            .doesNotContain("claimed_elsewhere")
    }
}
