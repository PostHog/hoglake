package com.posthog.hoglake.service

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.FileFormats
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.Transform
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class TableMetadataTest {
    @Test
    fun `custom metadata enforces bounded strings and reserves configuration names`() {
        TableMetadata.validateComment(null)
        TableMetadata.validateComment("")
        TableMetadata.validateComment("x".repeat(16384))
        TableMetadata.validateProperties(mapOf("owner.team" to "x".repeat(4096)))
        for (key in listOf("", "Owner", "hoglake.location", "trino.foo", "location", "sorted_by", "x".repeat(129))) {
            assertThatThrownBy { TableMetadata.validateProperties(mapOf(key to "value")) }
                .isInstanceOf(HoglakeException.Validation::class.java)
        }
        assertThatThrownBy { TableMetadata.validateProperties((0..100).associate { "k$it" to "value" }) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { TableMetadata.validateProperties(mapOf("key" to "x".repeat(4097))) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { TableMetadata.validateProperties(mapOf("key" to "\u0000")) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { TableMetadata.validateComment("x".repeat(16385)) }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatThrownBy { TableMetadata.validateComment("\u0000") }
            .isInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `packed format property and fixed layout are validated`() {
        val packed = mapOf(FileFormats.TABLE_PROPERTY to FileFormats.CLICKHOUSE_MERGETREE_PACKED)
        TableMetadata.validateProperties(emptyMap())
        TableMetadata.validateProperties(mapOf(FileFormats.TABLE_PROPERTY to FileFormats.PARQUET))
        TableMetadata.validateProperties(packed)
        assertThatThrownBy {
            TableMetadata.validateProperties(mapOf(FileFormats.TABLE_PROPERTY to "orc"))
        }.isInstanceOf(HoglakeException.Validation::class.java)

        TableMetadata.validateDefinitionForFormat(
            packed,
            FileFormats.packedColumnTypes.mapIndexed { index, type -> ColumnDef("c$index", type) },
            emptyList(),
            emptyList(),
        )
        assertThatThrownBy {
            TableMetadata.validateDefinitionForFormat(
                packed,
                listOf(ColumnDef("amount", ColType.DECIMAL)),
                emptyList(),
                emptyList(),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("decimal")
        assertThatThrownBy {
            TableMetadata.validateDefinitionForFormat(
                packed,
                listOf(ColumnDef("id", ColType.LONG)),
                listOf(PartitionFieldDef(1, Transform.IDENTITY)),
                emptyList(),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("partition")
        assertThatThrownBy {
            TableMetadata.validateDefinitionForFormat(
                packed,
                listOf(ColumnDef("id", ColType.LONG)),
                emptyList(),
                listOf(SortFieldDef(1, SortDirection.ASC, NullOrder.NULLS_FIRST)),
            )
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("sort")
        for (name in listOf("_part", "_part_offset", "_anything")) {
            assertThatThrownBy {
                TableMetadata.validateDefinitionForFormat(
                    packed,
                    listOf(ColumnDef("id", ColType.LONG), ColumnDef(name, ColType.LONG)),
                    emptyList(),
                    emptyList(),
                )
            }.isInstanceOf(HoglakeException.Validation::class.java)
                .hasMessageContaining(name)
        }
        // Parquet tables are unaffected: the reservation is the packed reader's.
        TableMetadata.validateDefinitionForFormat(
            emptyMap(),
            listOf(ColumnDef("_part", ColType.LONG)),
            emptyList(),
            emptyList(),
        )
    }

    @Test
    fun `table format is immutable while unrelated properties may change`() {
        val packed = mapOf(FileFormats.TABLE_PROPERTY to FileFormats.CLICKHOUSE_MERGETREE_PACKED)
        TableMetadata.requireFormatUnchanged(
            packed,
            packed + ("owner.team" to "analytics"),
        )
        assertThatThrownBy {
            TableMetadata.requireFormatUnchanged(emptyMap(), packed)
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("immutable")
        assertThatThrownBy {
            TableMetadata.requireFormatUnchanged(packed, emptyMap())
        }.isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("immutable")
    }
}
