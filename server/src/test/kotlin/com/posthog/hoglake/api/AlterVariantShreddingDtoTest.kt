package com.posthog.hoglake.api

import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.wireObjectMapper
import io.ktor.server.plugins.BadRequestException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * `set_variant_shredding` on the wire, through the strict API mapper:
 * what parses, what is a 400 before the service sees it, and that the
 * declaration reaches the service as decoded JSON, untouched. The
 * service-side rules (variant, top-level, grammar) are
 * `AlterVariantShreddingIntegrationTest`.
 */
class AlterVariantShreddingDtoTest {
    private val mapper = wireObjectMapper()

    private fun ops(body: String): List<AlterOp> = mapper.readValue<AlterTableRequestDto>(body).ops.map { it.toModel() }

    @Test
    fun `the op parses to its model with the declaration as decoded JSON`() {
        val parsed =
            ops(
                """{"ops":[{"op":"set_variant_shredding","name":"properties","shredding":
                    {"type":"object","fields":[{"name":"price","type":"decimal8","precision":18,"scale":2}]}}]}""",
            )
        assertThat(parsed).containsExactly(
            AlterOp.SetVariantShredding(
                "properties",
                mapOf(
                    "type" to "object",
                    "fields" to listOf(mapOf("name" to "price", "type" to "decimal8", "precision" to 18, "scale" to 2)),
                ),
            ),
        )
        // A grammar fault is not the mapper's to find: the service refuses
        // it with the column in hand, so the message can name the column.
        assertThat(ops("""{"ops":[{"op":"set_variant_shredding","name":"p","shredding":{"type":"text"}}]}"""))
            .containsExactly(AlterOp.SetVariantShredding("p", mapOf("type" to "text")))
        // Nor is a dotted path: AlterService resolves it, and refuses it as nested.
        assertThat(ops("""{"ops":[{"op":"set_variant_shredding","name":"r.x","shredding":{"type":"string"}}]}"""))
            .containsExactly(AlterOp.SetVariantShredding("r.x", mapOf("type" to "string")))
    }

    @Test
    fun `a null or absent declaration clears, as a null comment removes one`() {
        assertThat(ops("""{"ops":[{"op":"set_variant_shredding","name":"properties","shredding":null}]}"""))
            .containsExactly(AlterOp.SetVariantShredding("properties", null))
        assertThat(ops("""{"ops":[{"op":"set_variant_shredding","name":"properties"}]}"""))
            .containsExactly(AlterOp.SetVariantShredding("properties", null))
    }

    @Test
    fun `name is required`() {
        for (body in listOf(
            """{"ops":[{"op":"set_variant_shredding","shredding":{"type":"string"}}]}""",
            """{"ops":[{"op":"set_variant_shredding","name":null,"shredding":{"type":"string"}}]}""",
        )) {
            assertThatThrownBy { ops(body) }
                .describedAs(body)
                .isInstanceOf(BadRequestException::class.java)
                .hasMessageContaining("requires field 'name'")
        }
    }

    @Test
    fun `parent belongs to add_column alone`() {
        assertThatThrownBy {
            ops("""{"ops":[{"op":"set_variant_shredding","name":"x","parent":"r","shredding":null}]}""")
        }.isInstanceOf(HoglakeException.Validation::class.java).hasMessageContaining("does not take 'parent'")
    }

    @Test
    fun `the capability is advertised`() {
        assertThat(CatalogDto("c", "s3://b/c", 0, 0).capabilities).contains("mutable-variant-shredding-v1")
    }
}
