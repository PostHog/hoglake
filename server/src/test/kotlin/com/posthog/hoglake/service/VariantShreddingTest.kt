package com.posthog.hoglake.service

import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.wireObjectMapper
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.math.BigInteger

/**
 * The `type_params.shredding` rules ([VariantShredding] and the column
 * checks in [ColumnTrees]). The declarations, valid and invalid, are the
 * cases of the Trino connector's TestHoglakeVariantShredding (PostHog/trino,
 * plugin/trino-hoglake), so that a declaration this server accepts is one
 * the connector can write; the connector's live suite runs them against a
 * server. Declarations are parsed by the production wire mapper, so the
 * numbers have the types a request gives them.
 */
class VariantShreddingTest {
    @Test
    fun `a declaration of every type is accepted`() {
        accept(
            """
            {"type": "object", "fields": [
                {"name": "${'$'}browser", "type": "string"},
                {"name": "payload", "type": "variant"},
                {"name": "flag", "type": "boolean"},
                {"name": "tiny", "type": "int8"},
                {"name": "small", "type": "int16"},
                {"name": "count", "type": "int32"},
                {"name": "id", "type": "int64"},
                {"name": "ratio", "type": "float"},
                {"name": "score", "type": "double"},
                {"name": "price", "type": "decimal4", "precision": 9, "scale": 2},
                {"name": "total", "type": "decimal8", "precision": 18, "scale": 4},
                {"name": "huge", "type": "decimal16", "precision": 38, "scale": 0},
                {"name": "day", "type": "date"},
                {"name": "at", "type": "time"},
                {"name": "local", "type": "timestamp"},
                {"name": "local_ns", "type": "timestamp_ns"},
                {"name": "instant", "type": "timestamptz"},
                {"name": "instant_ns", "type": "timestamptz_ns"},
                {"name": "bytes", "type": "binary"},
                {"name": "uuid", "type": "uuid"},
                {"name": "tags", "type": "array", "element": {"type": "string"}},
                {"name": "${'$'}set", "type": "object", "fields": [{"name": "plan", "type": "string"}]}]}
            """,
        )
        // Any node can be the root, including one with no typed column at all.
        accept("""{"type": "string"}""")
        accept("""{"type": "variant"}""")
        accept("""{"type": "array", "element": {"type": "object", "fields": [{"name": "k", "type": "int64"}]}}""")
        // The edges of the decimal ranges, and a scale equal to the precision.
        accept("""{"type": "decimal4", "precision": 1, "scale": 0}""")
        accept("""{"type": "decimal8", "precision": 18, "scale": 18}""")
        // Names are variant keys, not identifiers: any non-empty string, in its case.
        accept(
            """{"type": "object", "fields": [
                {"name": "a.b[0] c", "type": "string"}, {"name": "Ä", "type": "string"}]}""",
        )
    }

    @Test
    fun `an invalid declaration is refused with its path`() {
        refuse("[]", "$ is not a JSON object")
        refuse("\"string\"", "$ is not a JSON object")
        refuse("""{"fields": []}""", "$ has no type")
        refuse("""{"type": 1}""", "$ has no type")
        refuse("""{"type": null}""", "$ has no type")
        refuse("""{"type": "text"}""", "$ has an unknown type 'text'")
        refuse("""{"type": "String"}""", "$ has an unknown type 'String'")
        refuse("""{"type": "string", "nullable": true}""", "$ has an unknown key 'nullable'")
        refuse("""{"type": "variant", "fields": []}""", "$ has an unknown key 'fields'")
        refuse(
            """{"type": "object", "fields": [{"name": "a", "type": "string"}], "element": {"type": "string"}}""",
            "$ has an unknown key 'element'",
        )
        refuse("""{"type": "array", "element": {"type": "string"}, "fields": []}""", "$ has an unknown key 'fields'")
        // The root is not a field, so it has no name.
        refuse("""{"name": "properties", "type": "string"}""", "$ has an unknown key 'name'")
        refuse("""{"type": "object"}""", "$ has no fields")
        refuse("""{"type": "object", "fields": []}""", "$ has no fields")
        refuse("""{"type": "object", "fields": null}""", "$ has no fields")
        refuse("""{"type": "object", "fields": {"a": {"type": "string"}}}""", "$ has no fields")
        refuse("""{"type": "object", "fields": [{"type": "string"}]}""", "$ has a field without a name")
        refuse("""{"type": "object", "fields": ["a"]}""", "$ has a field without a name")
        refuse("""{"type": "object", "fields": [{"name": 1, "type": "string"}]}""", "$ has a field without a name")
        refuse("""{"type": "object", "fields": [{"name": "a"}]}""", "$.a has no type")
        refuse(
            """{"type": "object", "fields": [{"name": "a", "type": "string", "element": {"type": "string"}}]}""",
            "$.a has an unknown key 'element'",
        )
        refuse("""{"type": "array"}""", "$ has no element")
        refuse("""{"type": "array", "element": null}""", "$ has no element")
        // An element is not a field, so it has no name.
        refuse("""{"type": "array", "element": {"name": "e", "type": "string"}}""", "$[*] has an unknown key 'name'")
        refuse(
            """{"type": "array", "element": {"type": "array", "element": {"type": "decimal"}}}""",
            "$[*][*] has an unknown type 'decimal'",
        )
        refuse(
            """{"type": "object", "fields": [
                {"name": "${'$'}set", "type": "object", "fields": [{"name": "plan", "type": "text"}]}]}""",
            "$.${'$'}set.plan has an unknown type 'text'",
        )
        refuse("""{"type": "decimal8", "scale": 2}""", "$ has no integer precision")
        refuse("""{"type": "decimal8", "precision": 1.5, "scale": 0}""", "$ has no integer precision")
        refuse("""{"type": "decimal8", "precision": 18.0, "scale": 0}""", "$ has no integer precision")
        refuse("""{"type": "decimal8", "precision": "18", "scale": 0}""", "$ has no integer precision")
        refuse("""{"type": "decimal8", "precision": 18}""", "$ has no integer scale")
        refuse("""{"type": "decimal8", "precision": 18, "scale": 2, "width": 8}""", "$ has an unknown key 'width'")
        refuse(
            """{"type": "decimal4", "precision": 10, "scale": 2}""",
            "$ has precision 10 and scale 2, which decimal4 does not hold",
        )
        refuse(
            """{"type": "decimal8", "precision": 19, "scale": 2}""",
            "$ has precision 19 and scale 2, which decimal8 does not hold",
        )
        refuse(
            """{"type": "decimal16", "precision": 39, "scale": 2}""",
            "$ has precision 39 and scale 2, which decimal16 does not hold",
        )
        refuse(
            """{"type": "decimal8", "precision": 5, "scale": 6}""",
            "$ has precision 5 and scale 6, which decimal8 does not hold",
        )
        refuse(
            """{"type": "decimal8", "precision": 0, "scale": 0}""",
            "$ has precision 0 and scale 0, which decimal8 does not hold",
        )
        refuse(
            """{"type": "decimal8", "precision": 5, "scale": -1}""",
            "$ has precision 5 and scale -1, which decimal8 does not hold",
        )
        // A writer finds the columns of a field by its lowercase name.
        refuse(
            """{"type": "object", "fields": [
                {"name": "plan", "type": "string"}, {"name": "Plan", "type": "string"}]}""",
            "$ has fields that differ only by case: 'plan' and 'Plan'",
        )
        refuse(
            """{"type": "object", "fields": [{"name": "a", "type": "string"}, {"name": "a", "type": "int64"}]}""",
            "$ has duplicate field 'a'",
        )
        refuse("""{"type": "object", "fields": [{"name": "", "type": "string"}]}""", "$ has a field with an empty name")
        // The same name in two objects is two fields.
        accept(
            """{"type": "object", "fields": [
                {"name": "a", "type": "object", "fields": [{"name": "a", "type": "string"}]}]}""",
        )
    }

    @Test
    fun `an integer parameter is accepted in every shape the mapper gives one`() {
        for (precision in listOf<Any>(18, 18L, 18.toShort(), 18.toByte(), BigInteger.valueOf(18))) {
            VariantShredding.validate(mapOf("type" to "decimal8", "precision" to precision, "scale" to 2), "v")
        }
        for (precision in listOf<Any>(4_294_967_314L, BigInteger.TWO.pow(64), 18.0, BigDecimal("18"), "18", true)) {
            assertThatThrownBy {
                VariantShredding.validate(mapOf("type" to "decimal8", "precision" to precision, "scale" to 2), "v")
            }.describedAs("precision %s (%s)", precision, precision.javaClass.simpleName)
                .isInstanceOf(HoglakeException.Validation::class.java)
                .hasMessageEndingWith("$ has no integer precision")
        }
        // A long that would narrow to 18 is not 18.
        assertThat(4_294_967_314L.toInt()).isEqualTo(18)
    }

    @Test
    fun `depth and field count are capped`() {
        var deepest = """{"type": "string"}"""
        repeat(VariantShredding.MAX_DEPTH) { deepest = """{"type": "array", "element": $deepest}""" }
        accept(deepest)
        refuse(
            """{"type": "array", "element": $deepest}""",
            "$" + "[*]".repeat(VariantShredding.MAX_DEPTH) + " is nested more than 16 levels deep",
        )
        var deepObject = """{"type": "string"}"""
        repeat(VariantShredding.MAX_DEPTH + 1) {
            val named = deepObject.replaceFirst("{", """{"name": "n",""")
            deepObject = """{"type": "object", "fields": [{"name": "k", "type": "variant"}, $named]}"""
        }
        refuse(deepObject, "$" + ".n".repeat(VariantShredding.MAX_DEPTH) + " is nested more than 16 levels deep")

        accept(objectWithFields(VariantShredding.MAX_FIELDS))
        refuse(objectWithFields(VariantShredding.MAX_FIELDS + 1), "$ has more than 1000 fields and arrays")
        // Over the whole tree, not per object.
        val split = objectWithFields(VariantShredding.MAX_FIELDS / 2)
        refuse(
            """{"type": "object", "fields": [{"name": "x", ${split.drop(1)}, {"name": "y", ${split.drop(1)}]}""",
            "$ has more than 1000 fields and arrays",
        )
        // An array counts as a field does: each gives every data file columns
        // of its own, so nested arrays would otherwise multiply the columns of
        // a field.
        val array = """"type": "array", "element": {"type": "string"}"""
        accept(objectWithFields(VariantShredding.MAX_FIELDS - 1).replaceFirst(""""type": "string"""", array))
        refuse(
            objectWithFields(VariantShredding.MAX_FIELDS).replaceFirst(""""type": "string"""", array),
            "$ has more than 1000 fields and arrays",
        )
    }

    @Test
    fun `a pathological declaration is refused without recursing into it`() {
        // Far deeper than any stack: refused at the depth cap, or at the
        // unknown key, before the walk descends.
        val depth = 100_000
        var chain: Any = mapOf("type" to "string")
        repeat(depth) { chain = mapOf("type" to "array", "element" to chain) }
        assertThatThrownBy { VariantShredding.validate(chain, "v") }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageEndingWith("is nested more than 16 levels deep")
        var blob: Any = emptyMap<String, Any>()
        repeat(depth) { blob = mapOf("x" to blob) }
        assertThatThrownBy { VariantShredding.validate(mapOf("type" to "string", "extra" to blob), "v") }
            .hasMessageEndingWith("$ has an unknown key 'extra'")
    }

    @Test
    fun `a refusal does not echo the caller's blob`() {
        val huge = "z".repeat(5_000)
        // As long as a name can be, so the rules about names reach it
        val long = "z".repeat(VariantShredding.MAX_PATH_NAME_BYTES)
        for (declaration in listOf(
            mapOf("type" to huge),
            mapOf("type" to "string", huge to 1),
            mapOf("type" to "object", "fields" to listOf(mapOf("name" to huge, "type" to "string"))),
            mapOf(
                "type" to "object",
                "fields" to
                    listOf(mapOf("name" to long, "type" to "string"), mapOf("name" to long, "type" to "string")),
            ),
            mapOf(
                "type" to "object",
                "fields" to
                    listOf(
                        mapOf("name" to long, "type" to "string"),
                        mapOf("name" to long.uppercase(), "type" to "string"),
                    ),
            ),
            mapOf("type" to "object", "fields" to listOf(mapOf("name" to long, "type" to "text"))),
            mapOf("type" to "decimal8", "precision" to huge, "scale" to 2),
        )) {
            val thrown = catchThrowable { VariantShredding.validate(declaration, "v") }
            assertThat(thrown).isInstanceOf(HoglakeException.Validation::class.java)
            assertThat(thrown.message!!.length).describedAs(thrown.message!!.take(200)).isLessThan(300)
        }
        val unknownKey =
            catchThrowable { ColumnTrees.validate(listOf(ColumnDef("v", ColType.VARIANT, mapOf(huge to 1)))) }
        assertThat(unknownKey).isInstanceOf(HoglakeException.Validation::class.java)
        assertThat(unknownKey.message!!.length).isLessThan(300)
        // A path is at most MAX_DEPTH names, each capped: here a long one and
        // short ones under it, as many bytes as a path may have.
        var deep: Map<String, Any> = mapOf("name" to "n", "type" to "text")
        repeat(VariantShredding.MAX_DEPTH - 2) {
            deep = mapOf("name" to "n", "type" to "object", "fields" to listOf(deep))
        }
        deep = mapOf("name" to long.drop(VariantShredding.MAX_DEPTH - 1), "type" to "object", "fields" to listOf(deep))
        val thrown =
            catchThrowable { VariantShredding.validate(mapOf("type" to "object", "fields" to listOf(deep)), "v") }
        assertThat(thrown).hasMessageEndingWith("has an unknown type 'text'")
        assertThat(thrown.message!!.length).isLessThan(300)
    }

    @Test
    fun `only a top-level variant column declares a layout`() {
        val declaration =
            mapOf("shredding" to parse("""{"type": "object", "fields": [{"name": "a", "type": "string"}]}"""))
        ColumnTrees.validate(listOf(ColumnDef("v", ColType.VARIANT, declaration)))
        // No declaration, in each of its shapes.
        ColumnTrees.validate(listOf(ColumnDef("v", ColType.VARIANT)))
        ColumnTrees.validate(listOf(ColumnDef("v", ColType.VARIANT, emptyMap())))
        ColumnTrees.validate(listOf(ColumnDef("v", ColType.VARIANT, mapOf("shredding" to null))))
        // A variant nested in a struct, a list or a map, and one grafted into a struct by add_column.
        val nested = ColumnDef("x", ColType.VARIANT, declaration)
        assertRefused(
            listOf(ColumnDef("r", ColType.STRUCT, children = listOf(nested))),
            "variant column 'r.x' is nested",
        )
        assertRefused(
            listOf(ColumnDef("l", ColType.LIST, children = listOf(nested.copy(name = "element")))),
            "variant column 'l.element' is nested",
        )
        assertRefused(
            listOf(
                ColumnDef(
                    "m",
                    ColType.MAP,
                    children = listOf(ColumnDef("key", ColType.STRING, nullable = false), nested.copy(name = "value")),
                ),
            ),
            "variant column 'm.value' is nested",
        )
        assertThatThrownBy { ColumnTrees.validate(listOf(nested), depthOffset = 1) }
            .hasMessage(
                "variant column 'x' is nested, and only a top-level variant column can declare type_params.shredding",
            )
        // Nested variants without a declaration stay legal.
        ColumnTrees.validate(listOf(ColumnDef("r", ColType.STRUCT, children = listOf(nested.copy(typeParams = null)))))
        ColumnTrees.validate(listOf(nested.copy(typeParams = mapOf("shredding" to null))), depthOffset = 1)
        // A column of another type, even with a null declaration.
        assertThatThrownBy { ColumnTrees.validate(listOf(ColumnDef("properties", ColType.JSON, declaration))) }
            .hasMessage("column 'properties' is 'json', and only a variant column can declare type_params.shredding")
        assertRefused(listOf(ColumnDef("s", ColType.STRING, mapOf("shredding" to null))), "column 's' is 'string'")
        assertRefused(
            listOf(ColumnDef("d", ColType.DECIMAL, mapOf("precision" to 10, "scale" to 2, "shredding" to null))),
            "column 'd' is 'decimal'",
        )
        // A variant takes no other parameter.
        assertThatThrownBy {
            ColumnTrees.validate(listOf(ColumnDef("v", ColType.VARIANT, mapOf("shreding" to declaration["shredding"]))))
        }
            .hasMessage(
                "variant column 'v' has an unknown type_params key 'shreding': a variant takes only 'shredding'",
            )
        assertRefused(
            listOf(ColumnDef("v", ColType.VARIANT, mapOf("shredding" to null, "scale" to 2))),
            "unknown type_params key 'scale'",
        )
        // And the declaration itself, named by the column's path.
        assertThatThrownBy {
            ColumnTrees.validate(
                listOf(ColumnDef("v", ColType.VARIANT, mapOf("shredding" to parse("""{"type": "text"}""")))),
            )
        }
            .hasMessage("variant column 'v' has an invalid type_params.shredding: $ has an unknown type 'text'")
    }

    @Test
    fun `a field name is text the catalog stores as declared, and the names on a path are capped in bytes`() {
        // JSON escapes, as a request carries them: JSONB has no NUL character,
        // and UTF-8 has no unpaired surrogates
        refuse(objectWith("""a\u0000b"""), "$ has a field name with a NUL character")
        refuse(objectWith("""\ud800"""), "$ has a field name with an unpaired surrogate")
        refuse(objectWith("""\ud800a"""), "$ has a field name with an unpaired surrogate")
        refuse(objectWith("""a\udc00"""), "$ has a field name with an unpaired surrogate")
        accept(objectWith("""\ud83d\ude00"""))

        val tooLong = "has a field whose name, with the names above it, is longer than 1024 bytes"
        // Bytes of UTF-8, not characters: a name of characters of each width at
        // the cap, and one byte over it
        for (char in listOf("a", "\u00e9", "\u20ac", "\ud83d\ude00")) {
            val width = char.toByteArray(Charsets.UTF_8).size
            val name = char.repeat(1024 / width) + "a".repeat(1024 % width)
            accept(objectWith(name))
            refuse(objectWith(name + "a"), "$ $tooLong")
        }
        val parent = "p".repeat(1000)
        accept(objectWith(parent, "c".repeat(24)))
        refuse(objectWith(parent, "c".repeat(25)), "$.${Identifiers.cap(parent)} $tooLong")
        // An array adds no name, and keeps the names above it
        accept("""{"type": "array", "element": ${objectWith("a".repeat(1024))}}""")
        refuse(
            """{"type": "object", "fields": [{"name": "$parent", "type": "array",
                "element": ${objectWith("c".repeat(25))}}]}""",
            "$.${Identifiers.cap(parent)}[*] $tooLong",
        )
        assertRefused(
            listOf(ColumnDef("v", ColType.VARIANT, mapOf("shredding" to parse(objectWith("a".repeat(2000)))))),
            "variant column 'v' has an invalid type_params.shredding: $ $tooLong",
        )
    }

    /** An object with one field named by the last of [names], under objects with the others. */
    private fun objectWith(vararg names: String): String {
        var node = """{"type": "string"}"""
        for (name in names.reversed()) {
            node = """{"type": "object", "fields": [{"name": "$name", ${node.drop(1)}]}"""
        }
        return node
    }

    @Test
    fun `other scalars keep their parameters unchecked`() {
        // The rule is about variants and the shredding key, not a new
        // policy for every scalar's parameters.
        ColumnTrees.validate(listOf(ColumnDef("s", ColType.STRING, mapOf("anything" to listOf(1, 2)))))
        ColumnTrees.validate(
            listOf(ColumnDef("d", ColType.DECIMAL, mapOf("precision" to 10, "scale" to 2, "extra" to true))),
        )
    }

    private fun assertRefused(
        defs: List<ColumnDef>,
        message: String,
    ) {
        assertThatThrownBy { ColumnTrees.validate(defs) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining(message)
    }

    private fun parse(json: String): Any = wireObjectMapper().readValue<Any>(json)

    private fun accept(json: String) {
        VariantShredding.validate(parse(json), "v")
        ColumnTrees.validate(listOf(ColumnDef("v", ColType.VARIANT, mapOf("shredding" to parse(json)))))
    }

    private fun refuse(
        json: String,
        message: String,
    ) {
        assertThatThrownBy { VariantShredding.validate(parse(json), "v") }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessage("variant column 'v' has an invalid type_params.shredding: $message")
    }

    private fun objectWithFields(count: Int): String =
        (0 until count).joinToString(", ", """{"type": "object", "fields": [""", "]}") {
            """{"name": "k$it", "type": "string"}"""
        }
}
