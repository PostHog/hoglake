package com.posthog.hoglake.api

import com.posthog.hoglake.model.RetirementResult
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File
import kotlin.reflect.full.memberProperties

/**
 * `RetirementResult` on the WIRE — the retirement twin of
 * [CleanupResultSpecParityTest], with ONE structural difference that is
 * the whole reason this file exists.
 *
 * THERE IS NO DTO AND NO ENDPOINT. Retirement has no
 * `POST /maintenance/retire`: the loop is its only driver, because a
 * trigger reaches every replica and a batch takes the per-catalog
 * commit lock. So the only place this payload appears on the wire is
 * `hog_maintenance_run.result`, which the ledger stores as the raw JSON
 * of the MODEL (serialized with the wire mapper) and hands back
 * verbatim.
 *
 * `MaintenanceRun.result` is `type: object` with prose for EVERY task —
 * expiry, cleanup, compaction and verify included — so a `$ref` there
 * would be a change to every task's contract rather than an addition to
 * this one, and the spec's `RetirementResult` schema is consequently
 * referenced by nothing. An unreferenced component is dead schema
 * unless something checks it, which is what this is: the field set is
 * read from the spec by parsing and from the model by REFLECTION, so
 * neither side is a literal and a counter added to one without the
 * other reds here rather than in a client six weeks later.
 *
 * Unit, not integration: one artifact on disk and the class it
 * describes.
 */
class RetirementResultSpecParityTest {
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
        assertThat(match).describedAs("RetirementResult has no `required:` list").isNotNull()
        return match!!.groupValues[1].split(",").map { it.trim() }.filter { it.isNotEmpty() }
    }

    @Test
    fun `the stored model and the spec declare exactly the same counters`() {
        val model = RetirementResult::class.memberProperties.map { wire(it.name) }.sorted()
        val declared = properties(schemaBlock("    RetirementResult:")).sorted()
        assertThat(declared)
            .describedAs(
                "the ledger serializes the MODEL, so a spec-only field is one no run row can " +
                    "ever carry, and a model-only field is one no client is told about",
            )
            .isEqualTo(model)
    }

    @Test
    fun `every counter is required, because none of them postdates the ledger`() {
        // The opposite of CleanupResult's story, and worth stating
        // rather than inheriting: `objects_removed` and friends are
        // optional there because the ledger holds rows written before
        // those counters existed. RetirementResult arrived WHOLE with
        // the task, so there is no row anywhere missing any of them and
        // no field carries @JsonInclude(NON_DEFAULT). The day one is
        // added, it joins `MaintenanceDto`'s added-later list and
        // LEAVES this required set — and this assertion is what makes
        // that a decision rather than an oversight.
        val block = schemaBlock("    RetirementResult:")
        val required = requiredOf(block)
        assertThat(required).isEqualTo(properties(block))
        assertThat(RetirementResult::class.memberProperties.map { wire(it.name) })
            .describedAs("every model field is on the wire unconditionally")
            .containsExactlyInAnyOrderElementsOf(required)
    }

    @Test
    fun `the spec says retirement has no trigger endpoint, because it does not`() {
        // The absence is a contract: an operator reading the spec must
        // not go looking for a route that was left out by accident.
        assertThat(spec)
            .describedAs("no route may appear for a task whose loop is the only safe driver")
            .doesNotContain("/maintenance/retire")
        assertThat(schemaBlock("    RetirementResult:"))
            .describedAs("and the schema must say so where a reader will find it")
            .contains("no POST trigger for retirement")
    }
}
