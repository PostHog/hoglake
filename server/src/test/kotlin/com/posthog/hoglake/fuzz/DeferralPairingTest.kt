package com.posthog.hoglake.fuzz

import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * The oracle's documented-deferral sanction, pinned deterministically.
 *
 * This is the assertion that was missing. The sanction decides whether
 * a rewriter refusal the reader did not share is one of the DOCUMENTED
 * asymmetries (a non-native time unit, a decimal the destination cannot
 * hold) or a real disagreement — so it can fail in two directions, and
 * only one of them is loud:
 *
 *  - too NARROW and the campaign reports a finding that is not one. That
 *    happened, took 1.1M fleet executions to surface, and was fixed by
 *    replacing a set-membership test with structural pairing.
 *  - too BROAD and it suppresses real disagreements SILENTLY. Nothing
 *    would report that, ever.
 *
 * The full suite passed with the structural pairing replaced by a
 * cross-product — which is the set-membership defect again, in the
 * direction that goes quiet. A corpus seed is not enough here: the
 * `fuzz` gradle task is separate from `:test`, so a regression in this
 * logic reaches CI as a green build.
 */
class DeferralPairingTest {
    private fun mapColumn(
        keyType: ColType,
        valueType: ColType,
    ) = Column(
        1,
        0,
        ColumnDef("m", ColType.MAP),
        children =
            listOf(
                Column(2, 0, ColumnDef("key", keyType, nullable = false)),
                Column(3, 1, ColumnDef("value", valueType, nullable = false)),
            ),
    )

    private fun mapSchema(
        keyUnit: LogicalTypeAnnotation.TimeUnit,
        valueUnit: LogicalTypeAnnotation.TimeUnit,
    ): MessageType =
        MessageType(
            "fuzz",
            listOf<Type>(
                Types.optionalGroup()
                    .addField(
                        Types.repeatedGroup()
                            .addField(
                                Types.required(PrimitiveType.PrimitiveTypeName.INT64)
                                    .`as`(LogicalTypeAnnotation.timestampType(false, keyUnit))
                                    .id(2)
                                    .named("key"),
                            )
                            .addField(
                                Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                                    .`as`(LogicalTypeAnnotation.timestampType(false, valueUnit))
                                    .id(3)
                                    .named("value"),
                            )
                            .named("key_value"),
                    )
                    .`as`(LogicalTypeAnnotation.mapType())
                    .id(1)
                    .named("m"),
            ),
        )

    @Test
    fun `a map whose units match POSITIONALLY is not a deferral`() {
        // The direction that fails silently. Catalog key is millis and
        // the file's key is millis; catalog value is micros and the
        // file's value is micros. Nothing is deferred.
        //
        // Under a cross-product pairing the KEY is also compared against
        // the VALUE's micros, finds a mismatch, and claims a deferral
        // that does not exist — suppressing every real disagreement this
        // column could ever produce, with no test and no finding to say
        // so.
        assertThat(
            NestedAgreement.documentedDeferral(
                mapColumn(ColType.TIMESTAMP_MS, ColType.TIMESTAMP),
                mapSchema(LogicalTypeAnnotation.TimeUnit.MILLIS, LogicalTypeAnnotation.TimeUnit.MICROS),
            ),
        ).describedAs("units line up slot for slot").isFalse()
    }

    @Test
    fun `a map whose KEY unit disagrees IS a deferral`() {
        // The seed that took 1.1M executions: catalog key millis, value
        // nanos; file has nanos in both slots. Set membership said
        // "nanos is one of the units this column wants" and reported a
        // finding; pairing sees that the KEY wanted millis.
        assertThat(
            NestedAgreement.documentedDeferral(
                mapColumn(ColType.TIMESTAMP_MS, ColType.TIMESTAMP_NS),
                mapSchema(LogicalTypeAnnotation.TimeUnit.NANOS, LogicalTypeAnnotation.TimeUnit.NANOS),
            ),
        ).describedAs("the key wanted millis and got nanos").isTrue()
    }

    @Test
    fun `a map whose VALUE unit disagrees IS a deferral`() {
        assertThat(
            NestedAgreement.documentedDeferral(
                mapColumn(ColType.TIMESTAMP_MS, ColType.TIMESTAMP),
                mapSchema(LogicalTypeAnnotation.TimeUnit.MILLIS, LogicalTypeAnnotation.TimeUnit.NANOS),
            ),
        ).describedAs("the value wanted micros and got nanos").isTrue()
    }

    @Test
    fun `a list element's unit is paired through the synthetic layer`() {
        // Same pairing question one shape over: a list's element lives
        // under `list`, which is parquet's own layer and binds by
        // POSITION, not by the name or id a catalog lookup would use.
        fun listColumn(elementType: ColType) =
            Column(
                1,
                0,
                ColumnDef("l", ColType.LIST),
                children = listOf(Column(2, 0, ColumnDef("element", elementType))),
            )

        fun listSchema(unit: LogicalTypeAnnotation.TimeUnit): MessageType =
            MessageType(
                "fuzz",
                listOf<Type>(
                    Types.optionalList()
                        .setElementType(
                            Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                                .`as`(LogicalTypeAnnotation.timestampType(false, unit))
                                .id(2)
                                .named("element"),
                        )
                        .id(1)
                        .named("l"),
                ),
            )
        assertThat(
            NestedAgreement.documentedDeferral(
                listColumn(ColType.TIMESTAMP),
                listSchema(LogicalTypeAnnotation.TimeUnit.MICROS),
            ),
        ).isFalse()
        assertThat(
            NestedAgreement.documentedDeferral(
                listColumn(ColType.TIMESTAMP),
                listSchema(LogicalTypeAnnotation.TimeUnit.MILLIS),
            ),
        ).isTrue()
    }

    @Test
    fun `a struct field's unit is paired by the ordinary binding rule`() {
        fun structColumn(fieldType: ColType) =
            Column(
                1,
                0,
                ColumnDef("s", ColType.STRUCT),
                children = listOf(Column(2, 0, ColumnDef("t", fieldType))),
            )

        fun structSchema(unit: LogicalTypeAnnotation.TimeUnit): MessageType =
            MessageType(
                "fuzz",
                listOf<Type>(
                    Types.optionalGroup()
                        .addField(
                            Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                                .`as`(LogicalTypeAnnotation.timestampType(false, unit))
                                .id(2)
                                .named("t"),
                        )
                        .id(1)
                        .named("s"),
                ),
            )
        assertThat(
            NestedAgreement.documentedDeferral(
                structColumn(ColType.TIMESTAMP),
                structSchema(LogicalTypeAnnotation.TimeUnit.MICROS),
            ),
        ).isFalse()
        assertThat(
            NestedAgreement.documentedDeferral(
                structColumn(ColType.TIMESTAMP_NS),
                structSchema(LogicalTypeAnnotation.TimeUnit.MICROS),
            ),
        ).isTrue()
    }

    @Test
    fun `a decimal the destination cannot hold is a deferral, and one it can is not`() {
        fun decimalColumn(
            precision: Int,
            scale: Int,
        ) = Column(1, 0, ColumnDef("d", ColType.DECIMAL, mapOf("precision" to precision, "scale" to scale)))

        fun decimalSchema(
            precision: Int,
            scale: Int,
        ): MessageType =
            MessageType(
                "fuzz",
                listOf<Type>(
                    Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .`as`(LogicalTypeAnnotation.decimalType(scale, precision))
                        .id(1)
                        .named("d"),
                ),
            )
        assertThat(NestedAgreement.documentedDeferral(decimalColumn(10, 2), decimalSchema(9, 2))).isFalse()
        assertThat(NestedAgreement.documentedDeferral(decimalColumn(10, 2), decimalSchema(11, 2)))
            .describedAs("wider than the destination").isTrue()
        assertThat(NestedAgreement.documentedDeferral(decimalColumn(10, 2), decimalSchema(9, 3)))
            .describedAs("a different scale").isTrue()
    }
}
