package com.posthog.hoglake.service

import com.posthog.hoglake.model.HoglakeException
import java.math.BigInteger

/**
 * The shredded layout that a top-level `variant` column declares in
 * `type_params.shredding`: the object fields and array elements that a
 * writer stores in Parquet columns of their own, laid out as the Parquet
 * Variant shredding specification lays them out. Readers never need it,
 * because each file's footer carries its own layout, so a declaration
 * steers only the writers that honour it, and changing one rewrites no
 * file.
 *
 * A declaration is a tree of JSON objects, each with a `type`:
 *
 * ```
 * {"type": "object", "fields": [
 *     {"name": "$browser", "type": "string"},
 *     {"name": "price", "type": "decimal8", "precision": 18, "scale": 2},
 *     {"name": "tags", "type": "array", "element": {"type": "string"}},
 *     {"name": "payload", "type": "variant"}]}
 * ```
 *
 *  - `object` lists its `fields`, at least one, each with a `name`: a
 *    variant key in its original case, which no other field of the object
 *    has, compared case-insensitively. A name has no NUL character, which
 *    JSONB cannot store, and no unpaired surrogate, which UTF-8 cannot
 *    encode, so it is stored as it was declared.
 *  - `array` has an `element`.
 *  - `variant` is shredded without a type, into a value column of its own.
 *  - Every other type is a Variant primitive type of the specification:
 *    boolean, int8, int16, int32, int64, float, double, date, time,
 *    timestamp, timestamp_ns, timestamptz, timestamptz_ns, binary,
 *    string and uuid, or decimal4, decimal8 and decimal16, which take an
 *    integer `precision` (at most 9, 18 and 38) and an integer `scale`
 *    (0 to the precision).
 *
 * A node has no other keys, and at most [MAX_DEPTH] objects and arrays
 * nest. Two more limits keep the footer of every data file within the
 * 15 MB the Trino connector reads. A declaration has at most [MAX_FIELDS]
 * fields and arrays, counted together, since each gives every data file
 * columns of its own. And the names on the way to a field are at most
 * [MAX_PATH_NAME_BYTES] bytes of UTF-8, since the footer repeats them
 * once for each column below them.
 *
 * These are the rules of the writer that honours declarations, the Trino
 * connector (PostHog/trino, plugin/trino-hoglake,
 * HoglakeVariantShredding.java), which checks them again on every write.
 * Checked here, a declaration it cannot write is a 422 when it is declared
 * rather than a failure of every later INSERT, and the paths in the
 * messages are that connector's (`$.price`, `$.tags[*]`).
 */
object VariantShredding {
    /** The `type_params` key of the declaration. */
    const val KEY = "shredding"

    /** How deeply objects and arrays may nest, the root being depth 0. */
    const val MAX_DEPTH = 16

    /** How many object fields and arrays a declaration may have, counted together over the whole tree. */
    const val MAX_FIELDS = 1000

    /** The UTF-8 length of the field names on the way to any field. */
    const val MAX_PATH_NAME_BYTES = 1024

    private val PRIMITIVE_TYPES =
        setOf(
            "boolean", "int8", "int16", "int32", "int64", "float", "double", "date", "time",
            "timestamp", "timestamp_ns", "timestamptz", "timestamptz_ns", "binary", "string", "uuid",
        )

    /** The decimal types, with the largest precision each holds. */
    private val DECIMAL_TYPES = mapOf("decimal4" to 9, "decimal8" to 18, "decimal16" to 38)

    /** The key a node may have as an object field, besides those of its type. */
    private val FIELD_KEYS = setOf("name")

    /**
     * Refuses a declaration that does not follow the rules in the class
     * comment. [declaration] is the JSON value of `type_params.shredding`
     * as the wire mapper decodes it (maps, lists, strings and numbers);
     * [column] is the qualified name of the column, which the identifier
     * policy has already bounded.
     *
     * The walk recurses once per object or array, and refuses one past
     * [MAX_DEPTH] before it recurses, so its depth is bounded whatever the
     * request. Every value it quotes from the declaration is capped.
     */
    fun validate(
        declaration: Any,
        column: String,
    ) {
        Walk(column).visit(declaration, "$", 0, 0, emptySet())
    }

    /** The refusal for a declaration on a column whose type has no shredded layout. */
    fun notVariant(
        column: String,
        type: String,
    ) = HoglakeException.Validation(
        "column '$column' is '$type', and only a variant column can declare type_params.shredding",
    )

    /**
     * The refusal for a declaration on a variant nested in a struct, list or
     * map: the connector reads and writes shredded values only in top-level
     * columns, so it could honour none.
     */
    fun nested(column: String) =
        HoglakeException.Validation(
            "variant column '$column' is nested, and only a top-level variant column can declare " +
                "type_params.shredding",
        )

    private class Walk(private val column: String) {
        /** The fields seen so far, over the whole declaration. */
        private var fields = 0

        /**
         * One node at [path]. [nameBytes] is the UTF-8 length of the field
         * names on the way to it, and [context] holds the keys it may have
         * besides those of its type: `name` for an object field.
         */
        fun visit(
            node: Any?,
            path: String,
            depth: Int,
            nameBytes: Int,
            context: Set<String>,
        ) {
            if (node !is Map<*, *>) throw refusal(path, "is not a JSON object")
            val type = node["type"] as? String ?: throw refusal(path, "has no type")
            when (type) {
                "variant" -> checkKeys(node, path, context)
                "object" -> {
                    checkKeys(node, path, context, "fields")
                    checkDepth(path, depth)
                    val list = node["fields"] as? List<*>
                    if (list.isNullOrEmpty()) throw refusal(path, "has no fields")
                    // Lowercase name -> the name in its case. A reader finds
                    // the Parquet column of a field by its lowercase name, so
                    // it cannot tell apart two that differ only by case.
                    val names = HashMap<String, String>()
                    for (field in list) {
                        count()
                        val name =
                            (field as? Map<*, *>)?.get("name") as? String
                                ?: throw refusal(path, "has a field without a name")
                        if (name.isEmpty()) throw refusal(path, "has a field with an empty name")
                        if (name.indexOf('\u0000') >= 0) throw refusal(path, "has a field name with a NUL character")
                        val fieldNameBytes =
                            nameBytes + (
                                utf8Length(name)
                                    ?: throw refusal(path, "has a field name with an unpaired surrogate")
                            )
                        if (fieldNameBytes > MAX_PATH_NAME_BYTES) {
                            throw refusal(
                                path,
                                "has a field whose name, with the names above it, is longer than " +
                                    "$MAX_PATH_NAME_BYTES bytes",
                            )
                        }
                        val previous = names.putIfAbsent(name.lowercase(), name)
                        if (previous == name) throw refusal(path, "has duplicate field '${Identifiers.cap(name)}'")
                        if (previous != null) {
                            throw refusal(
                                path,
                                "has fields that differ only by case: '${Identifiers.cap(previous)}' and " +
                                    "'${Identifiers.cap(name)}'",
                            )
                        }
                        visit(field, "$path.${Identifiers.cap(name)}", depth + 1, fieldNameBytes, FIELD_KEYS)
                    }
                }
                "array" -> {
                    checkKeys(node, path, context, "element")
                    checkDepth(path, depth)
                    count()
                    val element = node["element"] ?: throw refusal(path, "has no element")
                    visit(element, "$path[*]", depth + 1, nameBytes, emptySet())
                }
                in PRIMITIVE_TYPES -> checkKeys(node, path, context)
                in DECIMAL_TYPES -> {
                    checkKeys(node, path, context, "precision", "scale")
                    val precision = intValue(node, path, "precision")
                    val scale = intValue(node, path, "scale")
                    if (precision < 1 || precision > DECIMAL_TYPES.getValue(type) || scale < 0 || scale > precision) {
                        throw refusal(path, "has precision $precision and scale $scale, which $type does not hold")
                    }
                }
                else -> throw refusal(path, "has an unknown type '${Identifiers.cap(type)}'")
            }
        }

        private fun checkKeys(
            node: Map<*, *>,
            path: String,
            context: Set<String>,
            vararg typeKeys: String,
        ) {
            for (key in node.keys) {
                if (key != "type" && key !in context && key !in typeKeys) {
                    throw refusal(path, "has an unknown key '${Identifiers.cap(key)}'")
                }
            }
        }

        /** Counts an object field or an array, each of which gives every data file columns of its own. */
        private fun count() {
            fields++
            if (fields > MAX_FIELDS) throw refusal("$", "has more than $MAX_FIELDS fields and arrays")
        }

        /** The UTF-8 length of [name], or null when it has an unpaired surrogate. */
        private fun utf8Length(name: String): Int? {
            var bytes = 0
            var index = 0
            while (index < name.length) {
                val char = name[index]
                when {
                    char.code < 0x80 -> bytes += 1
                    char.code < 0x800 -> bytes += 2
                    char.isHighSurrogate() -> {
                        if (index + 1 == name.length || !name[index + 1].isLowSurrogate()) return null
                        bytes += 4
                        index++
                    }
                    char.isLowSurrogate() -> return null
                    else -> bytes += 3
                }
                index++
            }
            return bytes
        }

        private fun checkDepth(
            path: String,
            depth: Int,
        ) {
            if (depth >= MAX_DEPTH) throw refusal(path, "is nested more than $MAX_DEPTH levels deep")
        }

        /**
         * An integer that fits an Int, in any of the shapes the wire mapper
         * decodes one to. `18.0` and `"18"` are not integers, as they are
         * not to the connector.
         */
        private fun intValue(
            node: Map<*, *>,
            path: String,
            key: String,
        ): Int =
            when (val value = node[key]) {
                is Int -> value
                is Long -> if (value in Int.MIN_VALUE..Int.MAX_VALUE) value.toInt() else null
                is Short -> value.toInt()
                is Byte -> value.toInt()
                is BigInteger -> if (value.bitLength() < Int.SIZE_BITS) value.toInt() else null
                else -> null
            } ?: throw refusal(path, "has no integer $key")

        private fun refusal(
            path: String,
            problem: String,
        ) = HoglakeException.Validation("variant column '$column' has an invalid type_params.shredding: $path $problem")
    }
}
