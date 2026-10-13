package com.posthog.hoglake.api

import com.posthog.hoglake.model.StorageUsage
import com.posthog.hoglake.wireObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.io.File

class StorageUsageSpecParityTest {
    @Test
    fun `footprint wire uses required decimal strings without precision loss`() {
        val spec = File("src/main/resources/openapi/hoglake.yaml").readText()
        val block = spec.substringAfter("    StorageUsage:\n").substringBefore("\n    Catalog:\n")
        val properties =
            Regex("(?m)^        ([a-z_]+):").findAll(block).map { it.groupValues[1] }.toList()
        val required =
            Regex("""required: \[([^]]*)]""").find(block)!!.groupValues[1]
                .split(",").map { it.trim() }
        val body =
            wireObjectMapper().valueToTree<com.fasterxml.jackson.databind.JsonNode>(
                StorageUsage(Long.MAX_VALUE, Long.MAX_VALUE).toDto(),
            )
        assertThat(body.fieldNames().asSequence().toList()).containsExactlyInAnyOrderElementsOf(properties)
        assertThat(properties).containsExactlyInAnyOrderElementsOf(required)
        assertThat(Regex("(?m)^          type: string$").findAll(block).count()).isEqualTo(2)
        for (field in properties) {
            assertThat(body[field].isTextual).isTrue()
            assertThat(body[field].asText()).isEqualTo("9223372036854775807")
        }
        assertThat(spec).contains("  /catalogs/{catalog}/storage-usage:")
    }
}
