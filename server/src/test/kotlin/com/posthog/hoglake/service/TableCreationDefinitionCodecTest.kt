package com.posthog.hoglake.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.model.nodeCount
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.UUID

/**
 * The durable format behind an atomic table creation's receipt.
 *
 * It had no tests at all, which is how it came to drop `children`
 * silently: prepare validated a nested definition, stored a gutted one,
 * handed back a receipt promising flat field ids, and publish then
 * failed forever on a definition nobody had sent — with the operation
 * stuck `prepared` and the client's uploaded objects orphaned.
 *
 * Everything here is about ROUND TRIP and about what OLD blobs still
 * mean: this is the one structure in the system that outlives the
 * process that wrote it.
 */
class TableCreationDefinitionCodecTest {
    private val json = ObjectMapper()

    private val nested =
        TableCreationDefinition(
            "ns",
            "events",
            listOf(
                ColumnDef("id", ColType.LONG, nullable = false),
                ColumnDef(
                    "addr",
                    ColType.STRUCT,
                    children =
                        listOf(
                            ColumnDef("zip", ColType.STRING),
                            ColumnDef("city", ColType.STRING),
                        ),
                ),
                ColumnDef("tags", ColType.LIST, children = listOf(ColumnDef("element", ColType.STRING))),
                ColumnDef(
                    "props",
                    ColType.MAP,
                    children =
                        listOf(
                            ColumnDef("key", ColType.STRING, nullable = false),
                            ColumnDef("value", ColType.LONG),
                        ),
                ),
            ),
        )

    private val flat =
        TableCreationDefinition(
            "ns",
            "events",
            listOf(
                ColumnDef("id", ColType.LONG, nullable = false),
                ColumnDef(
                    "amount",
                    ColType.DECIMAL,
                    typeParams = mapOf("precision" to 10, "scale" to 2),
                ),
            ),
        )

    @Test
    fun `sorted definitions compose with partitions and replacement in version five`() {
        val sorted = flat.copy(sortFields = listOf(SortFieldDef(1, SortDirection.DESC, NullOrder.NULLS_FIRST)))
        assertThat(TableCreationDefinitionCodec.encode(sorted)).contains("\"version\":5")
        assertThat(TableCreationDefinitionCodec.decode(TableCreationDefinitionCodec.encode(sorted))).isEqualTo(sorted)
        val combined =
            sorted.copy(
                partitionFields = listOf(PartitionFieldDef(1, Transform.IDENTITY)),
                replacement = ReplacementTarget(UUID.randomUUID(), 42),
            )
        assertThat(
            TableCreationDefinitionCodec.decode(TableCreationDefinitionCodec.encode(combined)),
        ).isEqualTo(combined)
    }

    @Test
    fun `partition fields require version four and preserve replacement guards`() {
        val partitioned = flat.copy(partitionFields = listOf(PartitionFieldDef(1, Transform.BUCKET, 16)))
        val encoded = TableCreationDefinitionCodec.encode(partitioned)
        assertThat(encoded).contains("\"version\":4")
        assertThat(TableCreationDefinitionCodec.decode(encoded)).isEqualTo(partitioned)
        val replaced = partitioned.copy(replacement = ReplacementTarget(UUID.randomUUID(), 42))
        assertThat(
            TableCreationDefinitionCodec.decode(TableCreationDefinitionCodec.encode(replaced)),
        ).isEqualTo(replaced)
        assertThat(TableCreationDefinitionCodec.encode(flat)).contains("\"version\":1")
        assertThatThrownBy {
            TableCreationDefinitionCodec.decode(encoded.replace("\"source_field_id\":1", "\"source_field_id\":0"))
        }.isInstanceOf(CorruptDefinitionException::class.java)
    }

    @Test
    fun `a nested definition survives the round trip whole`() {
        val decoded = TableCreationDefinitionCodec.decode(TableCreationDefinitionCodec.encode(nested))
        assertThat(decoded).isEqualTo(nested)
        // The count is the thing the receipt's field ids depend on: nine
        // nodes, not four columns.
        assertThat(nodeCount(decoded.columns)).isEqualTo(nodeCount(nested.columns)).isEqualTo(9)
    }

    @Test
    fun `a deeply nested definition survives too`() {
        val deep =
            TableCreationDefinition(
                "ns",
                "t",
                listOf(
                    ColumnDef(
                        "d",
                        ColType.STRUCT,
                        children =
                            listOf(
                                ColumnDef(
                                    "runs",
                                    ColType.LIST,
                                    children =
                                        listOf(
                                            ColumnDef(
                                                "element",
                                                ColType.STRUCT,
                                                children = listOf(ColumnDef("score", ColType.DOUBLE)),
                                            ),
                                        ),
                                ),
                            ),
                    ),
                ),
            )
        assertThat(TableCreationDefinitionCodec.decode(TableCreationDefinitionCodec.encode(deep)))
            .isEqualTo(deep)
    }

    @Test
    fun `a flat definition round-trips and keeps its type_params`() {
        assertThat(TableCreationDefinitionCodec.decode(TableCreationDefinitionCodec.encode(flat)))
            .isEqualTo(flat)
    }

    // ---- versions -----------------------------------------------------------

    @Test
    fun `a definition is encoded at the LOWEST version that can express it`() {
        // Not fastidiousness: during a rolling deploy an old binary
        // still reads these rows and rejects versions it does not know.
        // Emitting the new version unconditionally would make every
        // ordinary scalar receipt unreadable to a server that could have
        // handled it perfectly well.
        assertThat(json.readTree(TableCreationDefinitionCodec.encode(flat))["version"].asInt())
            .describedAs("a flat definition stays at version 1")
            .isEqualTo(1)
        assertThat(json.readTree(TableCreationDefinitionCodec.encode(nested))["version"].asInt())
            .describedAs("a nested definition needs version 2")
            .isEqualTo(2)
    }

    @Test
    fun `a scalar column's encoding is byte-identical to version 1's`() {
        // `children` is OMITTED when null rather than written as JSON
        // null, which is what lets an unchanged definition re-encode to
        // an unchanged blob — requireSame compares the normalised stored
        // blob against a freshly encoded request, so a gratuitous key
        // would turn every replay into a false conflict.
        val encoded = json.readTree(TableCreationDefinitionCodec.encode(flat))
        for (column in encoded["columns"]) {
            assertThat(column.has("children")).describedAs("%s", column["name"].asText()).isFalse()
            assertThat(column.fieldNames().asSequence().toList())
                .containsExactlyInAnyOrder("name", "type", "type_params", "nullable")
        }
    }

    @Test
    fun `re-encoding a decoded blob reproduces it exactly, nested included`() {
        // The property requireSame rests on.
        for (definition in listOf(flat, nested)) {
            val once = TableCreationDefinitionCodec.encode(definition)
            val twice = TableCreationDefinitionCodec.encode(TableCreationDefinitionCodec.decode(once))
            assertThat(twice).describedAs(definition.name).isEqualTo(once)
        }
    }

    @Test
    fun `a stored version-1 blob still decodes, with children null`() {
        // Every receipt already in a database is version 0 or 1 and is a
        // scalar definition. Absent children must decode to NULL, not to
        // an empty list: `children: []` on a scalar is a named 422, so
        // reading one back as empty would make old receipts
        // unpublishable — a data-shaped outage from a code change.
        val storedV1 =
            """
            {"version":1,"namespace":"ns","name":"events","columns":[
              {"name":"id","nullable":false,"type":"long","type_params":null},
              {"name":"amount","nullable":true,"type":"decimal",
               "type_params":{"precision":10,"scale":2}}]}
            """.trimIndent()
        val decoded = TableCreationDefinitionCodec.decode(storedV1)
        assertThat(decoded.columns.map { it.children }).containsOnlyNulls()
        assertThat(decoded).isEqualTo(flat)
    }

    @Test
    fun `a stored version-0 blob still decodes, with children null`() {
        val storedV0 =
            """
            {"namespace":"ns","name":"events","columns":[
              {"name":"id","nullable":false,"type":"LONG","typeParams":null},
              {"name":"u","nullable":true,"type":"UUID_T","typeParams":null}]}
            """.trimIndent()
        val decoded = TableCreationDefinitionCodec.decode(storedV0)
        assertThat(decoded.columns.map { it.type }).containsExactly(ColType.LONG, ColType.UUID_T)
        assertThat(decoded.columns.map { it.children }).containsOnlyNulls()
    }

    @Test
    fun `a version this codec does not know is refused, not guessed`() {
        assertThatThrownBy {
            TableCreationDefinitionCodec.decode(
                """{"version":99,"namespace":"ns","name":"t","columns":[]}""",
            )
        }
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessageContaining("unsupported table creation definition version 99")
    }
}
