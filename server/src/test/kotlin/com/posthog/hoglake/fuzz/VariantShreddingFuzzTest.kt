package com.posthog.hoglake.fuzz

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import com.code_intelligence.jazzer.junit.FuzzTest
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.service.ColumnTrees
import com.posthog.hoglake.service.VariantShredding
import com.posthog.hoglake.wireObjectMapper
import java.math.BigInteger

/**
 * Fuzz target (docs/fuzzing.md layer 4): the `type_params.shredding` rules
 * ([VariantShredding]), reached as every DDL path reaches them, through
 * [ColumnTrees.validate] on a top-level variant column.
 *
 * The input picks one of two modes:
 *
 *  - **arbitrary**: the rest of the input is a declaration as a request
 *    carries it, parsed by the production wire mapper. The rules refuse
 *    with [HoglakeException.Validation] and nothing else, a refusal stays
 *    bounded however large the declaration, and the decision is the same
 *    after the declaration is written out and read back, as publication
 *    re-validates a stored definition.
 *  - **generated**: the input drives a generator that builds a declaration
 *    by the rules in [VariantShredding]'s class comment, and then breaks
 *    at most one of them at a node it picks, or takes it to the field or
 *    depth limit without passing it. The generator knows which, so the
 *    oracle is the documented grammar rather than the validator's code: a
 *    sound declaration must be accepted, and a broken one refused.
 */
class VariantShreddingFuzzTest {
    @FuzzTest(maxDuration = "60s")
    fun declarationsFollowTheDocumentedRules(data: FuzzedDataProvider) {
        if (data.consumeBoolean()) arbitrary(data.consumeRemainingAsBytes()) else generated(data)
    }

    private fun arbitrary(bytes: ByteArray) {
        if (bytes.size > MAX_INPUT_BYTES) return
        // An unparsable body is a 400 before any rule runs; JSON null
        // declares nothing.
        val declaration = parseOrNull(bytes) ?: return
        val first = decision(declaration)
        check(decision(declaration) == first) { "unstable decision for ${show(declaration)}" }
        val stored = parseOrNull(mapper.writeValueAsBytes(declaration))
        check(stored != null && (decision(stored) == null) == (first == null)) {
            "a declaration decides differently once stored: ${show(declaration)}"
        }
    }

    private fun generated(data: FuzzedDataProvider) {
        val generator = Generator(data)
        val (declaration, broken) = generator.declaration()
        val refusal = decision(declaration)
        if (broken == null) {
            check(refusal == null) { "refused a sound declaration ($refusal): ${show(declaration)}" }
        } else {
            check(refusal != null) { "accepted a declaration with $broken: ${show(declaration)}" }
        }
    }

    /**
     * null when the declaration is accepted, else the refusal. Any other
     * throwable escapes as a finding. Both entry points must agree, since
     * the column check only adds where the declaration may stand.
     */
    private fun decision(declaration: Any): String? {
        val direct = refusal { VariantShredding.validate(declaration, "v") }
        val column =
            refusal {
                ColumnTrees.validate(
                    listOf(ColumnDef("v", ColType.VARIANT, mapOf(VariantShredding.KEY to declaration))),
                )
            }
        check(direct == column) { "VariantShredding said $direct, ColumnTrees said $column" }
        if (direct != null) {
            check(direct.length <= MAX_MESSAGE) {
                "unbounded refusal (${direct.length} chars): ${direct.take(200)}"
            }
        }
        return direct
    }

    private fun refusal(validate: () -> Unit): String? =
        try {
            validate()
            null
        } catch (e: HoglakeException.Validation) {
            e.message ?: ""
        }

    private fun parseOrNull(bytes: ByteArray): Any? =
        try {
            mapper.readValue<Any?>(bytes)
        } catch (_: Exception) {
            null
        }

    private fun show(declaration: Any): String =
        try {
            mapper.writeValueAsString(declaration).take(2000)
        } catch (e: Exception) {
            "unprintable: $e"
        }

    /**
     * Builds a declaration by the documented rules from the fuzz input,
     * recording each node with its depth and whether it is an object field,
     * so that one rule can then be broken at one of them.
     */
    @Suppress("UNCHECKED_CAST") // the generator's own maps and lists
    private class Generator(private val data: FuzzedDataProvider) {
        private class Node(val map: MutableMap<String, Any?>, val depth: Int, val field: Boolean) {
            /** An object's field names, lowercase. */
            val names = HashSet<String>()
        }

        private val nodes = mutableListOf<Node>()

        /** Fields and arrays, which the limit counts together. */
        private var fields = 0

        /** The declaration, and the rule it breaks, if any. */
        fun declaration(): Pair<Any, String?> {
            val root = node(0, field = false)
            val target = nodes[data.consumeInt(0, nodes.size - 1)]
            val map = target.map
            val type = map["type"]
            return when (data.consumeInt(0, 16)) {
                0 -> root to null
                1 -> {
                    val obj = nodes.firstOrNull { it.map["type"] == "object" } ?: return root to null
                    pad(obj, VariantShredding.MAX_FIELDS - fields)
                    root to null
                }
                2 -> {
                    chain(target, VariantShredding.MAX_DEPTH - target.depth)
                    root to null
                }
                3 -> {
                    chain(target, VariantShredding.MAX_DEPTH + 1 - target.depth)
                    root to "an array nested more than ${VariantShredding.MAX_DEPTH} levels deep"
                }
                4 -> {
                    val obj = nodes.firstOrNull { it.map["type"] == "object" } ?: return root to null
                    pad(obj, VariantShredding.MAX_FIELDS + 1 - fields)
                    root to "more than ${VariantShredding.MAX_FIELDS} fields and arrays"
                }
                5 -> {
                    map["type"] = notAType()
                    root to "an unknown type"
                }
                6 -> {
                    if (data.consumeBoolean()) map.remove("type") else map["type"] = notAString()
                    root to "a node without a type"
                }
                7 -> {
                    map[unknownKey(target)] = notAString()
                    root to "an unknown key"
                }
                8 -> notAnObject() to "a root that is not an object"
                9 ->
                    when (type) {
                        "object" -> {
                            when (data.consumeInt(0, 2)) {
                                0 -> map.remove("fields")
                                1 -> map["fields"] = emptyList<Any>()
                                else -> map["fields"] = notAList()
                            }
                            root to "an object without fields"
                        }
                        "array" -> {
                            if (data.consumeBoolean()) map.remove("element") else map["element"] = notAnObject()
                            root to "an array without an element object"
                        }
                        else -> {
                            decimal(target, broken = true)
                            root to "a decimal its type does not hold"
                        }
                    }
                10 -> {
                    decimal(target, broken = true)
                    root to "a decimal its type does not hold"
                }
                11, 12, 13 -> {
                    val obj = nodes.firstOrNull { it.map["type"] == "object" } ?: return root to null
                    val list = obj.map["fields"] as MutableList<Any?>
                    when (data.consumeInt(11, 13)) {
                        11 -> {
                            // A second field with a name the object has, in
                            // its case or another.
                            val name = (list.first() as Map<*, *>)["name"] as String
                            val variant = name.uppercase().takeIf { it != name && it.lowercase() == name.lowercase() }
                            list += mutableMapOf<String, Any?>("name" to (variant ?: name), "type" to "string")
                            root to "two fields whose names differ at most by case"
                        }
                        12 -> {
                            (list.first() as MutableMap<String, Any?>)["name"] = ""
                            root to "a field with an empty name"
                        }
                        else -> {
                            if (data.consumeBoolean()) {
                                list += notAnObject()
                            } else {
                                val field = list.first() as MutableMap<String, Any?>
                                if (data.consumeBoolean()) field.remove("name") else field["name"] = notAString()
                            }
                            root to "a field without a name"
                        }
                    }
                }
                14 -> {
                    // A field's keys are not an element's: an element, or
                    // the root, with a name.
                    val unnamed = nodes.filter { !it.field }
                    unnamed[data.consumeInt(0, unnamed.size - 1)].map["name"] = "n"
                    root to "a name on a node that is not a field"
                }
                15 -> {
                    // A name the catalog cannot store as declared, or one
                    // that takes the names on its path past the cap.
                    val obj = nodes.firstOrNull { it.map["type"] == "object" } ?: return root to null
                    val field = (obj.map["fields"] as MutableList<Any?>).first() as MutableMap<String, Any?>
                    val name = field["name"] as String
                    when (data.consumeInt(0, 2)) {
                        0 -> {
                            field["name"] = name + "\u0000"
                            root to "a field name with a NUL character"
                        }
                        1 -> {
                            field["name"] = name + if (data.consumeBoolean()) "\ud800" else "\udc00"
                            root to "a field name with an unpaired surrogate"
                        }
                        else -> {
                            field["name"] = name + "x".repeat(VariantShredding.MAX_PATH_NAME_BYTES)
                            root to "names longer than ${VariantShredding.MAX_PATH_NAME_BYTES} bytes on a path"
                        }
                    }
                }
                else -> {
                    decimal(target, broken = false)
                    root to null
                }
            }
        }

        private fun node(
            depth: Int,
            field: Boolean,
        ): MutableMap<String, Any?> {
            val map = linkedMapOf<String, Any?>()
            val node = Node(map, depth, field)
            nodes += node
            val containers = depth < VariantShredding.MAX_DEPTH && nodes.size < MAX_NODES
            when (data.consumeInt(if (containers) 0 else 2, 4)) {
                0 -> {
                    map["type"] = "object"
                    val list = mutableListOf<Any?>()
                    repeat(data.consumeInt(1, 4)) {
                        fields++
                        val child = node(depth + 1, field = true)
                        child["name"] = uniqueName(node)
                        list += child
                    }
                    map["fields"] = list
                }
                1 -> {
                    fields++
                    map["type"] = "array"
                    map["element"] = node(depth + 1, field = false)
                }
                2 -> map["type"] = "variant"
                3 -> map["type"] = PRIMITIVE_TYPES[data.consumeInt(0, PRIMITIVE_TYPES.size - 1)]
                else -> decimal(node, broken = false)
            }
            return map
        }

        /**
         * A name no field of [obj] has, compared case-insensitively. It has
         * no NUL or surrogate characters, and at most 8 characters of 3 bytes
         * and 3 underscores, so 16 of them on a path stay under the byte cap.
         */
        private fun uniqueName(obj: Node): String {
            var name = data.consumeString(8).filter { it != '\u0000' && !it.isSurrogate() }.ifEmpty { "f" }
            while (!obj.names.add(name.lowercase())) name += "_"
            return name
        }

        /** Appends [count] string fields to [obj], with names it does not have. */
        private fun pad(
            obj: Node,
            count: Int,
        ) {
            val list = obj.map["fields"] as MutableList<Any?>
            var i = 0
            repeat(count) {
                while (!obj.names.add("pad$i")) i++
                list += mutableMapOf<String, Any?>("name" to "pad$i", "type" to "string")
                fields++
            }
        }

        /**
         * Makes [node] a chain of [arrays] nested arrays around a string, so
         * that its innermost array is at depth `node.depth + arrays - 1`. Its
         * old subtree, and the fields in it, are gone.
         */
        private fun chain(
            node: Node,
            arrays: Int,
        ) {
            if (arrays <= 0) return
            var inner: MutableMap<String, Any?> = mutableMapOf("type" to "string")
            repeat(arrays - 1) { inner = mutableMapOf("type" to "array", "element" to inner) }
            node.map.keys.retainAll(setOf("name"))
            node.map["type"] = "array"
            node.map["element"] = inner
        }

        /**
         * Makes [node] a decimal, held by its type when [broken] is false, and
         * otherwise wrong in exactly one of its parameters.
         */
        private fun decimal(
            node: Node,
            broken: Boolean,
        ) {
            val (type, max) = DECIMAL_TYPES[data.consumeInt(0, DECIMAL_TYPES.size - 1)]
            var precision: Any? = data.consumeInt(1, max)
            var scale: Any? = data.consumeInt(0, precision as Int)
            if (broken) {
                when (data.consumeInt(0, 5)) {
                    0 -> precision = data.consumeInt(max + 1, max + 100)
                    1 -> precision = data.consumeInt(Int.MIN_VALUE, 0)
                    2 -> scale = data.consumeInt(precision + 1, Int.MAX_VALUE)
                    3 -> scale = data.consumeInt(Int.MIN_VALUE, -1)
                    4 -> precision = notAnInt(precision)
                    else -> scale = notAnInt(scale as Int)
                }
            }
            node.map.keys.retainAll(setOf("name"))
            node.map["type"] = type
            if (precision != MISSING) node.map["precision"] = shape(precision)
            if (scale != MISSING) node.map["scale"] = shape(scale)
        }

        /** An integer in one of the shapes the wire mapper decodes integers to. */
        private fun shape(value: Any?): Any? =
            when {
                value !is Int -> value
                data.consumeInt(0, 2) == 0 -> value.toLong()
                data.consumeInt(0, 1) == 0 -> BigInteger.valueOf(value.toLong())
                else -> value
            }

        /** Anything but an integer that fits an Int, or [MISSING]. */
        private fun notAnInt(value: Int): Any? =
            when (data.consumeInt(0, 6)) {
                0 -> value.toDouble()
                1 -> value.toString()
                2 -> value.toLong() + (1L shl 32)
                3 -> BigInteger.TWO.pow(64).add(BigInteger.valueOf(value.toLong()))
                4 -> null
                5 -> true
                else -> MISSING
            }

        /** A type name that none of the documented types has. */
        private fun notAType(): String {
            val name = data.consumeString(12)
            return if (name in ALL_TYPES) name + "_" else name
        }

        /** A key that [node] may not have. */
        private fun unknownKey(node: Node): String {
            val allowed =
                setOf("type") + (if (node.field) setOf("name") else emptySet()) +
                    when (node.map["type"]) {
                        "object" -> setOf("fields")
                        "array" -> setOf("element")
                        in DECIMAL_TYPES.map { it.first } -> setOf("precision", "scale")
                        else -> emptySet()
                    }
            val candidates = listOf("name", "fields", "element", "precision", "scale", "nullable", "")
            val key = candidates.getOrNull(data.consumeInt(0, candidates.size)) ?: data.consumeString(12)
            return generateSequence(key) { it + "_" }.first { it !in allowed }
        }

        private fun notAString(): Any? =
            when (data.consumeInt(0, 4)) {
                0 -> null
                1 -> data.consumeInt()
                2 -> true
                3 -> listOf("string")
                else -> mapOf("type" to "string")
            }

        private fun notAList(): Any? =
            when (data.consumeInt(0, 2)) {
                0 -> mapOf("a" to mapOf("type" to "string"))
                1 -> "fields"
                else -> null
            }

        private fun notAnObject(): Any =
            when (data.consumeInt(0, 3)) {
                0 -> listOf(mapOf("type" to "string"))
                1 -> "string"
                2 -> data.consumeInt()
                else -> false
            }
    }

    private companion object {
        const val MAX_INPUT_BYTES = 1 shl 20

        /**
         * A refusal names the column, a path of at most MAX_DEPTH + 1 capped
         * names, and at most two capped values.
         */
        const val MAX_MESSAGE = 2048

        /** The nodes a generated declaration has before its one change. */
        const val MAX_NODES = 128

        /** Stands for a parameter left out. */
        val MISSING = Any()

        val PRIMITIVE_TYPES =
            listOf(
                "boolean", "int8", "int16", "int32", "int64", "float", "double", "date", "time",
                "timestamp", "timestamp_ns", "timestamptz", "timestamptz_ns", "binary", "string", "uuid",
            )

        val DECIMAL_TYPES = listOf("decimal4" to 9, "decimal8" to 18, "decimal16" to 38)

        val ALL_TYPES = PRIMITIVE_TYPES.toSet() + DECIMAL_TYPES.map { it.first } + setOf("object", "array", "variant")

        /** The PRODUCTION wire mapper, as WireDtoParseFuzzTest uses it. */
        val mapper: ObjectMapper = wireObjectMapper()
    }
}
