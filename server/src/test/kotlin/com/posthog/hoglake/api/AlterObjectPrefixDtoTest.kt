package com.posthog.hoglake.api

import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.wireObjectMapper
import io.ktor.server.plugins.BadRequestException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.UUID

/**
 * `set_object_prefix_field` on the wire, through the strict API mapper:
 * what parses, what is a 400 before the service sees it, and what the
 * Table body carries. Unit: the DTOs alone. The service-side rules
 * (top-level, type, spec, emptiness) are `AlterObjectPrefixIntegrationTest`.
 */
class AlterObjectPrefixDtoTest {
    private val mapper = wireObjectMapper()

    private fun ops(body: String): List<AlterOp> = mapper.readValue<AlterTableRequestDto>(body).ops.map { it.toModel() }

    @Test
    fun `the op parses to its model, and a dotted name passes through to the service`() {
        assertThat(ops("""{"ops":[{"op":"set_object_prefix_field","name":"team_id"}]}"""))
            .containsExactly(AlterOp.SetObjectPrefixField("team_id"))
        // Not refused here: the dotted path is resolved, and refused as
        // nested, by AlterService, which is where every column path is.
        assertThat(ops("""{"ops":[{"op":"set_object_prefix_field","name":"addr.zip"}]}"""))
            .containsExactly(AlterOp.SetObjectPrefixField("addr.zip"))
    }

    @Test
    fun `name is required, and there is no null-to-clear`() {
        for (body in listOf(
            """{"ops":[{"op":"set_object_prefix_field"}]}""",
            """{"ops":[{"op":"set_object_prefix_field","name":null}]}""",
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
            ops("""{"ops":[{"op":"set_object_prefix_field","name":"zip","parent":"addr"}]}""")
        }.isInstanceOf(HoglakeException.Validation::class.java).hasMessageContaining("does not take 'parent'")
    }

    @Test
    fun `the Table body carries the field only when it is set`() {
        val info =
            TableInfo(
                tableId = 1,
                tableUuid = UUID.randomUUID(),
                namespace = "ns",
                name = "t",
                columns = emptyList(),
                readSnapshotId = 3,
                recordCount = null,
                fileCount = null,
                fileSizeBytes = null,
            )
        val off = mapper.readTree(mapper.writeValueAsString(info.toDto()))
        assertThat(off.has("object_prefix_field")).describedAs("omitted when off: %s", off).isFalse()
        val on = mapper.readTree(mapper.writeValueAsString(info.copy(objectPrefixField = "team_id").toDto()))
        assertThat(on["object_prefix_field"].asText()).isEqualTo("team_id")
    }

    @Test
    fun `the capability is advertised`() {
        assertThat(CatalogDto("c", "s3://b/c", 0, 0).capabilities).contains("object-prefix-layout-v1")
    }
}
