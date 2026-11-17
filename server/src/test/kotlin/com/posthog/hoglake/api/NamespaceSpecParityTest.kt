package com.posthog.hoglake.api

import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.model.NamespaceInfo
import com.posthog.hoglake.wireObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File

/**
 * `NamespaceDto` against the OpenAPI `Namespace` schema, both read off the
 * artifacts: the DTO through a real serialization with the wire mapper, the
 * schema out of the YAML block.
 *
 * A namespace has two shapes on the wire. One this server holds carries
 * `namespace_id` and never `remote`; a remote schema's listing entry carries
 * `remote: true` and no id. So `required` is what BOTH shapes always carry,
 * and every property the spec declares is sent by at least one of them.
 */
class NamespaceSpecParityTest {
    private val spec: String = File("src/main/resources/openapi/hoglake.yaml").readText()
    private val mapper = wireObjectMapper()

    /** The `Namespace:` schema block, up to the next sibling schema. */
    private val block: String by lazy {
        val header = "    Namespace:\n"
        val start = spec.indexOf(header)
        assertThat(start).describedAs("spec block Namespace").isNotEqualTo(-1)
        val rest = spec.substring(start + header.length)
        rest.substring(0, Regex("\n    [A-Za-z]").find(rest)?.range?.first ?: rest.length)
    }

    private fun specProperties(): List<String> =
        block.substringAfter("      properties:\n").lines()
            .mapNotNull { Regex("^        ([a-z_]+):").find(it)?.groupValues?.get(1) }

    private fun specRequired(): List<String> =
        Regex("""required: \[([^]]*)]""").find(block)!!.groupValues[1].split(",").map { it.trim() }

    private fun keysOf(dto: NamespaceDto): List<String> =
        mapper.readTree(mapper.writeValueAsString(dto)).fieldNames().asSequence().toList()

    private val plain = NamespaceInfo(7, "team_7").toDto()
    private val contained = NamespaceInfo(7, "team_7", NamespaceContainment("team_id", 7)).toDto()
    private val remote = NamespaceDto(name = "posthog", remote = true)

    @Test
    fun `every property the spec declares is sent by some namespace, and none is sent that it does not declare`() {
        val sent = (keysOf(plain) + keysOf(contained) + keysOf(remote)).distinct()
        assertThat(sent).containsExactlyInAnyOrderElementsOf(specProperties())
        assertThat(specProperties()).containsExactly("name", "namespace_id", "containment", "remote")
    }

    @Test
    fun `required is exactly what both a local namespace and a remote schema entry carry`() {
        val always = keysOf(plain).intersect(keysOf(contained).toSet()).intersect(keysOf(remote).toSet())
        assertThat(specRequired()).containsExactlyInAnyOrderElementsOf(always)
    }

    @Test
    fun `a local namespace is what it was on the wire, and a remote entry is a name and the flag`() {
        assertThat(mapper.writeValueAsString(plain)).isEqualTo("""{"name":"team_7","namespace_id":7}""")
        assertThat(mapper.writeValueAsString(contained))
            .isEqualTo("""{"name":"team_7","namespace_id":7,"containment":{"field":"team_id","value":7}}""")
        assertThat(mapper.writeValueAsString(remote)).isEqualTo("""{"name":"posthog","remote":true}""")
        // The spec says which shape has the id, and types the flag.
        assertThat(block).contains("Absent exactly when\n            `remote` is true")
        assertThat(block.substringAfter("        remote:\n")).startsWith("          type: boolean\n")
    }
}
