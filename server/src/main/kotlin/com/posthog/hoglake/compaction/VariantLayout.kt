package com.posthog.hoglake.compaction

import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.service.VariantShredding
import org.apache.parquet.schema.GroupType
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type
import org.apache.parquet.schema.Types
import java.math.BigInteger

/**
 * A group compaction refuses because a VARIANT column's storage in one of
 * its inputs is not the layout the rewrite writes: the column's
 * declaration, laid out as [VariantLayout] lays it out. A subclass of
 * [UnconvertibleSchemaException], so the sweep skips the group exactly as
 * it skips any other shape it cannot rewrite; its own type is what
 * [reason] is counted under (`hoglake_compaction_variant_layout_mismatch_total`).
 */
class VariantLayoutMismatchException(
    message: String,
    /** `layout` (the storage is another shape) or `duckdb` (see the planNode arm). */
    val reason: String,
) : UnconvertibleSchemaException(message)

/**
 * The Parquet layout of a VARIANT column: its group, annotated VARIANT(1)
 * with the catalog field id, and below it the layout of the column's
 * `type_params.shredding` declaration as the Parquet Variant shredding
 * specification lays one out. No declaration, or a root `variant`, is the
 * unshredded REQUIRED `metadata` + REQUIRED `value`, the only unshredded
 * shape the Trino connector reads.
 *
 * A port of pyhoglake's `Plan.expected_elements` (`_variant_codec.py`),
 * which its writer is held to element for element, and which is in turn a
 * port of the Trino connector's `VariantShreddingSchema.toParquetType`: so
 * for a declared column this is the layout both writers that honour
 * declarations produce. `pyhoglake/tests/vectors/variant_layout_vectors.json`
 * pins the two ports to each other (`VariantLayoutTest`).
 *
 * Compaction COPIES a variant (hoglake#70 is in parquet-java's Variant
 * reader, which the rewrite never uses): each input's subtree goes to the
 * output as plain groups, leaf by leaf, and the output's subtree comes from
 * here so the output schema stays a function of the catalog. The copy is
 * positional, so an input whose subtree differs from this one in any way
 * [layoutMismatch] checks would misplace values; such a group is refused instead.
 */
object VariantLayout {
    /**
     * [column]'s group, [repetition] at the top (the rewriter writes every
     * column OPTIONAL), with the catalog field id on it and on nothing
     * below. A declaration the grammar refuses is [UnconvertibleSchemaException]:
     * the catalog checked it at DDL, so one that fails here predates a rule,
     * and no layout can be derived from it.
     */
    fun groupType(
        column: Column,
        repetition: Type.Repetition,
    ): GroupType {
        val declaration = declaration(column)
        val metadata = Types.required(PrimitiveTypeName.BINARY).named("metadata")
        val below =
            if (declaration == null) {
                listOf(Types.required(PrimitiveTypeName.BINARY).named("value"))
            } else {
                nodeFields(declaration)
            }
        return Types.buildGroup(repetition)
            .`as`(LogicalTypeAnnotation.variantType(VARIANT_SPEC_VERSION))
            .addFields(metadata, *below.toTypedArray())
            .id(Math.toIntExact(column.fieldId))
            .named(column.def.name)
    }

    /**
     * How many Parquet nodes [column]'s group has, itself included, and
     * whether any is a list: what one decoded row of it costs the sorted
     * rewrite's chunk ([CompactionConfig.spillChunkRows]). Field groups
     * are REQUIRED, so a row that is an object holds every declared one.
     * Null when no layout can be derived ([groupType] refuses it), which
     * the planner reads as "never plan this table".
     */
    fun shape(column: Column): Pair<Int, Boolean>? {
        val group = runCatching { groupType(column, Type.Repetition.OPTIONAL) }.getOrNull() ?: return null
        var nodes = 0
        var list = false

        fun walk(type: Type) {
            nodes++
            if (type.logicalTypeAnnotation is LogicalTypeAnnotation.ListLogicalTypeAnnotation) list = true
            if (!type.isPrimitive) type.asGroupType().fields.forEach(::walk)
        }
        walk(group)
        return nodes to list
    }

    /**
     * The first way [src], an input file's field bound to a variant
     * column, differs from [expected] ([groupType]'s), or null when it
     * does not: the names, order, repetition and children of every group,
     * every leaf's physical type and length, and every node's logical type.
     *
     * Not `Type.equals`, which also compares field ids and would refuse
     * three differences that are safe. The top group's NAME (a rename
     * changes the catalog name; files bind by id) and REPETITION (a NOT
     * NULL column's REQUIRED group is written OPTIONAL, which holds every
     * value it held) are not compared, and a legacy converted type is
     * not: parquet-java reads a node's logical type from the logical type
     * when there is one, and Trino and parquet-java write `TIME_MICROS`
     * beside a `time` leaf where pyarrow writes none.
     */
    fun layoutMismatch(
        src: Type,
        expected: GroupType,
    ): String? {
        fun diff(
            got: Type,
            want: Type,
            path: String,
            top: Boolean,
        ): String? {
            val same =
                (top || got.name == want.name && got.repetition == want.repetition) &&
                    got.isPrimitive == want.isPrimitive &&
                    got.logicalTypeAnnotation == want.logicalTypeAnnotation &&
                    (
                        !got.isPrimitive ||
                            got.asPrimitiveType().primitiveTypeName == want.asPrimitiveType().primitiveTypeName &&
                            got.asPrimitiveType().typeLength == want.asPrimitiveType().typeLength
                    ) &&
                    (got.isPrimitive || got.asGroupType().fieldCount == want.asGroupType().fieldCount)
            if (!same) return "$path is `${describe(got)}`, where the declaration lays out `${describe(want)}`"
            if (got.isPrimitive) return null
            val children = got.asGroupType().fields.zip(want.asGroupType().fields)
            return children.firstNotNullOfOrNull { (g, w) -> diff(g, w, "$path.${w.name}", top = false) }
        }
        return diff(src, expected, expected.name, top = true)
    }

    /**
     * [type] on one line, without its children: `Type.toString` prints a
     * group's whole subtree, which for a declaration of up to a thousand
     * fields is a hundred kilobytes in a warning logged every sweep.
     */
    fun describe(type: Type): String {
        if (type.isPrimitive) return "$type"
        val logical = type.logicalTypeAnnotation?.let { " ($it)" } ?: ""
        val fields = type.asGroupType().fieldCount
        return "${type.repetition.name.lowercase()} group ${type.name}$logical of $fields fields"
    }

    /** VARIANT(1): version 1 of the Variant encoding, the only one. */
    private const val VARIANT_SPEC_VERSION: Byte = 1

    /** The declaration's root, or null when its layout is unshredded. */
    private fun declaration(column: Column): Map<*, *>? {
        val declaration = column.def.typeParams?.get(VariantShredding.KEY) ?: return null
        try {
            VariantShredding.validate(declaration, column.def.name)
        } catch (e: HoglakeException.Validation) {
            throw UnconvertibleSchemaException("no layout can be derived: ${e.message}")
        }
        // The validator has checked every cast below.
        return (declaration as Map<*, *>).takeUnless { it["type"] == "variant" }
    }

    /** A node's `value`, then its `typed_value` with everything under it. */
    private fun nodeFields(node: Map<*, *>): List<Type> {
        val value = Types.optional(PrimitiveTypeName.BINARY).named("value")
        val typed: Type =
            when (val kind = node["type"] as String) {
                "variant" -> return listOf(value)
                "object" ->
                    Types.optionalGroup()
                        .addFields(*(node["fields"] as List<*>).map { fieldGroup(it as Map<*, *>) }.toTypedArray())
                        .named(TYPED_VALUE)
                // A three-level list of REQUIRED element groups.
                "array" ->
                    Types.optionalGroup()
                        .`as`(LogicalTypeAnnotation.listType())
                        .addField(
                            Types.repeatedGroup()
                                .addField(fieldGroup(node["element"] as Map<*, *>, "element"))
                                .named("list"),
                        )
                        .named(TYPED_VALUE)
                else -> leaf(kind, node)
            }
        return listOf(value, typed)
    }

    /** An object field's or a list element's group: REQUIRED, so a missing field is a group of two nulls. */
    private fun fieldGroup(
        node: Map<*, *>,
        name: String = node["name"] as String,
    ): GroupType = Types.requiredGroup().addFields(*nodeFields(node).toTypedArray()).named(name)

    /** A primitive node's `typed_value`: the spec's physical and logical type for it. */
    private fun leaf(
        kind: String,
        node: Map<*, *>,
    ): PrimitiveType {
        fun of(
            physical: PrimitiveTypeName,
            logical: LogicalTypeAnnotation? = null,
        ) = Types.optional(physical).let { if (logical == null) it else it.`as`(logical) }

        fun timestamp(
            utc: Boolean,
            unit: TimeUnit,
        ) = of(PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(utc, unit))

        val decimal = { LogicalTypeAnnotation.decimalType(int(node, "scale"), int(node, "precision")) }
        val builder =
            when (kind) {
                "boolean" -> of(PrimitiveTypeName.BOOLEAN)
                "int8" -> of(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(8, true))
                "int16" -> of(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(16, true))
                "int32" -> of(PrimitiveTypeName.INT32)
                "int64" -> of(PrimitiveTypeName.INT64)
                "float" -> of(PrimitiveTypeName.FLOAT)
                "double" -> of(PrimitiveTypeName.DOUBLE)
                "date" -> of(PrimitiveTypeName.INT32, LogicalTypeAnnotation.dateType())
                "time" -> of(PrimitiveTypeName.INT64, LogicalTypeAnnotation.timeType(false, TimeUnit.MICROS))
                "timestamp" -> timestamp(false, TimeUnit.MICROS)
                "timestamp_ns" -> timestamp(false, TimeUnit.NANOS)
                "timestamptz" -> timestamp(true, TimeUnit.MICROS)
                "timestamptz_ns" -> timestamp(true, TimeUnit.NANOS)
                "binary" -> of(PrimitiveTypeName.BINARY)
                "string" -> of(PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType())
                "uuid" -> of(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, LogicalTypeAnnotation.uuidType()).length(16)
                "decimal4" -> of(PrimitiveTypeName.INT32, decimal())
                "decimal8" -> of(PrimitiveTypeName.INT64, decimal())
                // The fewest bytes that hold every unscaled value of the
                // precision, sign included: the connector's
                // decimalByteLength, and pyarrow's.
                "decimal16" -> {
                    val largest = BigInteger.TEN.pow(int(node, "precision")).subtract(BigInteger.ONE)
                    of(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, decimal()).length((largest.bitLength() + 1 + 7) / 8)
                }
                else -> error("unreachable: the grammar has no type '$kind'")
            }
        return builder.named(TYPED_VALUE)
    }

    private fun int(
        node: Map<*, *>,
        key: String,
    ): Int = (node[key] as Number).toInt()

    private const val TYPED_VALUE = "typed_value"
}
