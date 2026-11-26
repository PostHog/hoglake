package com.posthog.hoglake.service

import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException
import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NullOrder
import com.posthog.hoglake.model.SortDirection
import com.posthog.hoglake.model.Transform
import com.posthog.hoglake.wireObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.UUID

/**
 * Letter case where a pure function decides: the identifier patterns, the
 * sibling-name check, vocabulary words and JSON field names. Store-level
 * behaviour (uniqueness, lookup, rename) is in
 * HogliteIdentifierCaseParityIntegrationTest; the reserved-namespace and
 * team-namespace folds in ReservedNamespacesTest and TeamNamespacesTest;
 * shredded-field case collisions in VariantShreddingTest.
 */
class IdentifierCaseTest {
    @Test
    fun `namespace, table, view and column names allow mixed case`() {
        for (kind in listOf("namespace", "table", "view", "column")) {
            for (name in listOf("Events", "EVENTS", "eVeNtS_2", "_Private", "A-b")) {
                assertThatCode {
                    Identifiers.validate(
                        kind,
                        name,
                    )
                }.describedAs("$kind $name").doesNotThrowAnyException()
            }
        }
    }

    @Test
    fun `catalog names are lowercase only`() {
        val catalog = Regex(Identifiers.CATALOG_NAME_PATTERN)
        assertThat(catalog.matches("lake-1")).isTrue()
        for (name in listOf("Lake", "LAKE", "lAke", "lake-A")) {
            assertThat(catalog.matches(name)).describedAs(name).isFalse()
        }
    }

    @Test
    fun `sibling columns differing only by case pass the duplicate check`() {
        // Current behaviour; intent unknown (VariantShredding refuses the same
        // shape among shredded fields). An exact duplicate is refused.
        assertThatCode {
            ColumnTrees.validate(listOf(ColumnDef("Id", ColType.LONG), ColumnDef("id", ColType.LONG)))
        }.doesNotThrowAnyException()
        assertThatThrownBy {
            ColumnTrees.validate(listOf(ColumnDef("id", ColType.LONG), ColumnDef("id", ColType.LONG)))
        }.isInstanceOf(HoglakeException.Validation::class.java).hasMessageContaining("duplicate column names")
    }

    @Test
    fun `the reserved column prefix is matched case-sensitively`() {
        // Current behaviour; intent unknown. `_hog_row_id` is refused, `_HOG_row_id` is not.
        assertThatThrownBy { Identifiers.validateColumn("_hog_row_id") }
            .isInstanceOf(HoglakeException.Validation::class.java)
        assertThatCode { Identifiers.validateColumn("_HOG_row_id") }.doesNotThrowAnyException()
        assertThatCode { Identifiers.validateColumn("_Hog") }.doesNotThrowAnyException()
    }

    @Test
    fun `vocabulary words are case-insensitive on input and lowercase on output`() {
        assertThat(ColType.fromWire("LONG")).isEqualTo(ColType.LONG)
        assertThat(ColType.fromWire("Uuid").wire).isEqualTo("uuid")
        assertThat(Transform.fromWire("Bucket")).isEqualTo(Transform.BUCKET)
        assertThat(SortDirection.fromWire("DESC").wire).isEqualTo("desc")
        assertThat(NullOrder.fromWire("Nulls_First")).isEqualTo(NullOrder.NULLS_FIRST)
    }

    private data class Probe(val someName: String? = null)

    @Test
    fun `JSON field names are exact snake_case`() {
        assertThat(wireObjectMapper().readValue<Probe>("""{"some_name":"x"}""").someName).isEqualTo("x")
        for (field in listOf("Some_Name", "SOME_NAME", "someName")) {
            assertThatThrownBy { wireObjectMapper().readValue<Probe>("""{"$field":"x"}""") }
                .describedAs(field)
                .isInstanceOf(UnrecognizedPropertyException::class.java)
        }
    }

    @Test
    fun `an idempotency key is a UUID, so its hex case does not matter`() {
        val lower = "0f0e0d0c-0b0a-4908-8706-a5a4a3a2a1a0"
        assertThat(wireObjectMapper().readValue<UUID>("\"${lower.uppercase()}\""))
            .isEqualTo(UUID.fromString(lower))
        assertThat(UUID.fromString(lower.uppercase()).toString()).isEqualTo(lower)
    }
}
