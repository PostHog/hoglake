package com.posthog.hoglake.api

import com.posthog.hoglake.model.CleanupResult
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File
import kotlin.reflect.full.memberProperties

/**
 * `CleanupResult` on the WIRE — the cleanup twin of
 * [CompactionResultSpecParityTest], and for its reason.
 *
 * There are three copies of this counter set and nothing else keeps
 * them in step: the stored model ([CleanupResult], which the
 * maintenance run ledger serializes), the response DTO
 * ([CleanupResultDto], which `POST /maintenance/cleanup` returns), and
 * the OpenAPI schema. On compaction, `claimed_elsewhere` reached the
 * model and the spec but not the DTO — so the endpoint declared a field
 * it could never emit, and only a client generated from the spec would
 * have noticed. `objects_removed` and `settled_elsewhere` are the same
 * shape of addition.
 *
 * The DTO's property list comes from REFLECTION and the spec's from
 * parsing the file, so neither side is a literal here.
 *
 * Unit, not integration: two artifacts on disk and the classes they
 * describe.
 */
class CleanupResultSpecParityTest {
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
        assertThat(match).describedAs("CleanupResult has no `required:` list").isNotNull()
        return match!!.groupValues[1].split(",").map { it.trim() }.filter { it.isNotEmpty() }
    }

    @Test
    fun `the response DTO and the spec declare exactly the same counters`() {
        val dto = CleanupResultDto::class.memberProperties.map { wire(it.name) }.sorted()
        val declared = properties(schemaBlock("    CleanupResult:")).sorted()
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
        val model = CleanupResult::class.memberProperties.map { wire(it.name) }.sorted()
        val dto = CleanupResultDto::class.memberProperties.map { wire(it.name) }.sorted()
        assertThat(dto)
            .describedAs("the ledger's model and the endpoint's DTO are converted by hand, in toDto()")
            .isEqualTo(model)
    }

    @Test
    fun `toDto copies every counter, with no field left at its default`() {
        // Distinct values per field, so a toDto that maps one to the
        // WRONG source — or forgets one and lets the data class default
        // cover for it — is visible.
        val dto =
            CleanupResult(
                removed = 1,
                missing = 2,
                stillReferenced = 3,
                objectsRemoved = 4,
                settledElsewhere = 5,
                deadlineSkipped = 6,
            ).toDto()
        assertThat(dto.removed).isEqualTo(1)
        assertThat(dto.missing).isEqualTo(2)
        assertThat(dto.stillReferenced).isEqualTo(3)
        assertThat(dto.objectsRemoved)
            .describedAs("the physical-delete count the metric reads, distinct from the row count")
            .isEqualTo(4)
        assertThat(dto.settledElsewhere).isEqualTo(5)
        assertThat(dto.deadlineSkipped).isEqualTo(6)
    }

    @Test
    fun `the counters added after the ledger are optional on the wire, like claimed_elsewhere`() {
        // NON_DEFAULT on the stored model means a zero is omitted from a
        // ledger row, and rows older than the counter never had it at
        // all — so `required` would be a promise the ledger's own
        // history breaks. The read path fills 0
        // (CLEANUP_COUNTERS_ADDED_LATER), which is why absence reads as
        // zero rather than as unknown. This is exactly what
        // CompactionResultSpecParityTest pins for claimed_elsewhere.
        val block = schemaBlock("    CleanupResult:")
        val required = requiredOf(block)
        for (counter in listOf("objects_removed", "settled_elsewhere", "deadline_skipped")) {
            assertThat(properties(block)).contains(counter)
            assertThat(required)
                .describedAs("a required counter cannot be one the ledger has rows without")
                .doesNotContain(counter)
        }
        // The ones that predate the ledger stay required: optional is
        // for what was ADDED, not a default for everything.
        assertThat(required).containsExactlyInAnyOrder("removed", "missing", "still_referenced")
    }
}
