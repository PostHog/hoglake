package com.posthog.hoglake.api

import com.posthog.hoglake.model.CatalogNotEmptyReason
import com.posthog.hoglake.model.DroppedCatalog
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.wireObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File

/**
 * `DELETE /v1/catalogs/{catalog}` (#412) against the spec: the response
 * DTO, the refusal's `reason` vocabulary and the operation's documented
 * 409, each read off BOTH sides — the YAML and the production object or
 * mapping — so neither can drift alone.
 *
 * The literals here are only the ones that are the contract: the error
 * code, the schema and operation names. The reason list is never
 * restated; it is the Kotlin enum on one side and the YAML enum on the
 * other.
 *
 * Unit test on purpose, like the other `*SpecParityTest`s: files and
 * serialization only, so `-PunitOnly` still catches the drift.
 */
class CatalogDropSpecParityTest {
    private val spec: String = File("src/main/resources/openapi/hoglake.yaml").readText()
    private val routes: String = File("src/main/kotlin/com/posthog/hoglake/api/Routes.kt").readText()
    private val mapper = wireObjectMapper()

    /** One schema block, up to the next sibling schema. */
    private fun schemaBlock(name: String): String {
        val header = "    $name:\n"
        val start = spec.indexOf(header)
        assertThat(start).describedAs("spec block %s", name).isNotEqualTo(-1)
        val rest = spec.substring(start + header.length)
        val end = Regex("\n    [A-Za-z]").find(rest)?.range?.first ?: rest.length
        return rest.substring(0, end)
    }

    /** One path item, up to the next sibling path. */
    private fun pathBlock(path: String): String {
        val header = "\n  $path:\n"
        val start = spec.indexOf(header)
        assertThat(start).describedAs("spec path %s", path).isNotEqualTo(-1)
        val rest = spec.substring(start + header.length)
        val end = Regex("\n  /").find(rest)?.range?.first ?: rest.length
        return rest.substring(0, end)
    }

    /** The `delete:` operation of a path item, up to the next sibling operation. */
    private fun deleteOperation(path: String): String {
        val item = pathBlock(path)
        val start = item.indexOf("    delete:\n")
        assertThat(start).describedAs("%s declares a delete operation", path).isNotEqualTo(-1)
        val rest = item.substring(start + "    delete:\n".length)
        val end = Regex("\n    [a-z]+:\n").find(rest)?.range?.first ?: rest.length
        return rest.substring(0, end)
    }

    private fun properties(block: String): List<String> =
        Regex("(?m)^        ([a-z_]+):").findAll(block).map { it.groupValues[1] }.toList()

    @Test
    fun `the DroppedCatalog body is exactly the spec's required properties`() {
        val block = schemaBlock("DroppedCatalog")
        val required =
            Regex("""required: \[([^]]*)]""").find(block)!!.groupValues[1].split(",").map { it.trim() }
        val body = mapper.readTree(mapper.writeValueAsString(DroppedCatalog("c", "s3://b/c").toDto()))
        assertThat(body.fieldNames().asSequence().toList())
            .describedAs("the wire keys the route sends")
            .containsExactlyInAnyOrderElementsOf(properties(block))
        assertThat(properties(block)).containsExactlyInAnyOrderElementsOf(required)
        for (field in required) assertThat(body[field].isTextual).isTrue()
    }

    @Test
    fun `the reason enum is exactly the Kotlin vocabulary`() {
        // ApiError.reason's own enum line. MUTATION: a reason added to
        // CatalogNotEmptyReason and not to the spec (or the reverse) reds.
        val reasonProperty = schemaBlock("ApiError").substringAfter("        reason:\n")
        val enum =
            Regex("""enum: \[([^]]*)]""").find(reasonProperty)!!.groupValues[1].split(",").map { it.trim() }
        assertThat(enum).containsExactlyElementsOf(CatalogNotEmptyReason.entries.map { it.wire })
    }

    @Test
    fun `every reason reaches the wire as catalog_not_empty with its own reason`() {
        // Off the PRODUCTION mapping (`errorBody`), for every reason, so an
        // arm that forgot `reason` (or hard-coded one) reds here.
        for (reason in CatalogNotEmptyReason.entries) {
            val (status, dto) = errorBody(HoglakeException.CatalogNotEmpty("d", reason))
            assertThat(status.value).isEqualTo(409)
            assertThat(dto.error).isEqualTo("catalog_not_empty")
            assertThat(dto.reason).isEqualTo(reason.wire)
            assertThat(mapper.readTree(mapper.writeValueAsString(dto)).fieldNames().asSequence().toList())
                .containsExactlyInAnyOrder("error", "detail", "reason")
        }
    }

    @Test
    fun `the drop operation documents its metadata, its body, and every reason in its 409`() {
        val op = deleteOperation("/catalogs/{catalog}")
        assertThat(op).contains("operationId: dropCatalog")
        assertThat(op).contains("#/components/parameters/operationAuthor")
        assertThat(op).contains("#/components/parameters/operationMessage")
        assertThat(op).contains("#/components/schemas/DroppedCatalog")
        assertThat(op).contains("\"404\":").contains("\"422\":")
        val conflict = op.substringAfter("\"409\":").substringBefore("\"422\":")
        assertThat(conflict).contains("catalog_not_empty").contains("#/components/schemas/ApiError")
        for (reason in CatalogNotEmptyReason.entries) {
            assertThat(conflict).describedAs("the 409 must name %s", reason.wire).contains("`${reason.wire}`")
        }
    }

    @Test
    fun `the route the spec documents exists and calls the service`() {
        // The catalog route's `delete {` is the only DELETE without a path
        // argument directly under `route("/{catalog}")`; pin it through the
        // one call site rather than by re-parsing Kotlin.
        assertThat(Regex(Regex.escape("catalogs.dropCatalog(")).findAll(routes).count()).isEqualTo(1)
    }
}
