package com.posthog.hoglake.service

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.Transform
import java.util.UUID

/**
 * Versioned durable format. Type spellings are API wire names, never JVM
 * enum names.
 *
 * **Versions.** 0 is the pre-versioned shape (JVM enum names,
 * `typeParams`); 1 added the version marker and wire spellings; 2 adds
 * `children`; 3 adds the replacement identity and snapshot guard; 4 adds initial partition fields.
 * Without children a nested definition cannot be represented
 * at all.
 *
 * A definition is encoded at the LOWEST version that can express it —
 * 1 when no column has children, 2 when one does. That is not
 * fastidiousness: during a rolling deploy an old binary still reads
 * these rows, and `decode` there rejects any version it does not know.
 * Emitting 2 unconditionally would make every ordinary scalar receipt
 * unreadable to a server that could have handled it perfectly well,
 * while emitting 1 for a nested definition is the bug this fixes — the
 * old reader would silently DROP the children and hand back a receipt
 * describing a table nobody asked for. Encoding at the lowest capable
 * version keeps old readers working for everything they can represent
 * and fails them loudly on exactly what they cannot.
 *
 * Scalar encodings are byte-identical to version 1's, so nothing
 * already stored changes meaning and `requireSame`'s normalize-then-
 * compare is unaffected.
 */

internal object TableCreationDefinitionCodec {
    private val mapper = jacksonObjectMapper().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
    private val paramsType = object : TypeReference<Map<String, Any?>>() {}

    /** The newest version this codec writes; see the class KDoc. */
    private const val NESTED_VERSION = 2
    private const val FLAT_VERSION = 1

    fun encode(definition: TableCreationDefinition): String =
        mapper.writeValueAsString(
            mapOf(
                "version" to
                    if (definition.partitionFields.isNotEmpty()) {
                        4
                    } else if (definition.replacement != null) {
                        3
                    } else if (anyChildren(definition.columns)) {
                        NESTED_VERSION
                    } else {
                        FLAT_VERSION
                    },
                "namespace" to definition.namespace,
                "name" to definition.name,
                "columns" to definition.columns.map { encodeColumn(it) },
            ) + (
                definition.replacement?.let {
                    mapOf(
                        "replacement" to
                            mapOf(
                                "expected_table_uuid" to it.expectedTableUuid?.toString(),
                                "read_snapshot" to it.readSnapshot,
                            ),
                    )
                } ?: emptyMap()
            ) + (
                if (definition.partitionFields.isEmpty()) {
                    emptyMap()
                } else {
                    mapOf(
                        "partition_fields" to
                            definition.partitionFields.map { field ->
                                mapOf(
                                    "source_field_id" to field.sourceFieldId,
                                    "transform" to field.transform.wire,
                                    "transform_param" to field.transformParam,
                                )
                            },
                    )
                }
            ),
        )

    private fun anyChildren(columns: List<ColumnDef>): Boolean =
        columns.any { it.children != null || anyChildren(it.children ?: emptyList()) }

    /**
     * `children` is OMITTED when null rather than written as JSON null:
     * a scalar column's encoding then matches version 1's byte for byte,
     * which is what lets an unchanged definition re-encode to an
     * unchanged blob.
     */
    private fun encodeColumn(column: ColumnDef): Map<String, Any?> =
        buildMap {
            put("name", column.name)
            put("type", column.type.wire)
            put("type_params", column.typeParams)
            put("nullable", column.nullable)
            column.children?.let { kids -> put("children", kids.map { encodeColumn(it) }) }
        }

    /**
     * Read a stored receipt. Anything unreadable — bad JSON, a missing
     * field, an unknown type spelling, an unknown version — comes back
     * as one [CorruptDefinitionException] naming the defect, never as a
     * raw NPE from a chained `.asText()`.
     */
    fun decode(
        encoded: String,
        receipt: String = "a table creation receipt",
    ): TableCreationDefinition {
        // Every message below names the RECEIPT, which is what the
        // operator needs: one unreadable row among millions is found by
        // its operation id, never by a stack trace pointing at an
        // `.asText()` call. The default exists only so the codec stays
        // callable from a test that has no receipt in hand.
        fun corrupt(
            what: String,
            cause: Throwable? = null,
        ): Nothing = throw CorruptDefinitionException("$receipt: $what", cause)

        // Every echo of stored content goes through this. The messages
        // below quote a column NAME, a TYPE spelling and a JSON node,
        // all of which came from whoever prepared the receipt — so
        // "bounded" has to mean bounded everywhere, not only on the node
        // echoes that happened to get a `take(40)` first. A stored name
        // is `Identifiers`-shaped today, but the whole point of this
        // codec is reading rows that are NOT what they should be.

        val node =
            try {
                mapper.readTree(encoded)
            } catch (e: Exception) {
                corrupt("the stored definition is not valid JSON", e)
            }
        if (node == null || !node.isObject) corrupt("the stored definition is not a JSON object")
        val versionNode = node["version"]
        // TWO questions, not one: is the node the right KIND, and does
        // its VALUE survive the accessor.
        //
        // `asInt()` answers 0 for a STRING, a boolean or an object — the
        // same 0 that means "the pre-versioned shape", so `"2"` decoded
        // as version 0 with version-0 spellings applied to version-2
        // data. And for an integral node that does not fit, `asInt()`
        // NARROWS: 4294967297 becomes 1, a valid-looking version that
        // walks straight past the range check below. Both are the same
        // defect — a reader inventing a value the document did not
        // contain — and a kind check alone catches only the first.
        if (versionNode != null && !versionNode.isNull) {
            if (!versionNode.isIntegralNumber) {
                corrupt(
                    "the stored definition has a non-integer 'version' " +
                        "(${cap(versionNode.toString())})",
                )
            }
            if (!versionNode.canConvertToInt()) {
                corrupt(
                    "the stored definition has a 'version' outside the int range " +
                        "(${cap(versionNode.toString())}); narrowing it would invent a " +
                        "version this codec appears to support",
                )
            }
        }
        val version = versionNode?.takeIf { !it.isNull }?.asInt() ?: 0
        if (version !in 0..4) {
            corrupt("unsupported table creation definition version $version")
        }
        val columns =
            node["columns"]?.takeIf { it.isArray }
                ?: corrupt("the stored definition has no 'columns' array")
        return TableCreationDefinition(
            text(node, "namespace", "the definition", ::corrupt),
            text(node, "name", "the definition", ::corrupt),
            columns.map { decodeColumn(it, version, ::corrupt) },
            if (version == 3 || (version == 4 && node.has("replacement"))) {
                val replacement = node["replacement"]
                if (replacement == null || !replacement.isObject) corrupt("missing replacement guard")
                val snapshot = replacement["read_snapshot"]
                if (snapshot == null || !snapshot.isIntegralNumber ||
                    !snapshot.canConvertToLong() || snapshot.longValue() < 0
                ) {
                    corrupt("invalid replacement read_snapshot")
                }
                val uuid = replacement["expected_table_uuid"]
                ReplacementTarget(
                    if (uuid == null || uuid.isNull) {
                        null
                    } else {
                        try {
                            UUID.fromString(text(replacement, "expected_table_uuid", "replacement", ::corrupt))
                        } catch (e: IllegalArgumentException) {
                            corrupt("invalid replacement UUID", e)
                        }
                    },
                    snapshot.longValue(),
                )
            } else {
                null
            },
            if (version == 4) {
                val fields = node["partition_fields"]
                if (fields == null || !fields.isArray || fields.isEmpty) corrupt("missing partition_fields array")
                fields.map { field ->
                    val source = field["source_field_id"]
                    if (source == null || !source.isIntegralNumber ||
                        !source.canConvertToLong() || source.longValue() <= 0
                    ) {
                        corrupt(
                            "invalid partition source_field_id",
                        )
                    }
                    val param = field["transform_param"]
                    if (param != null && !param.isNull && (!param.isIntegralNumber || !param.canConvertToInt())) {
                        corrupt(
                            "invalid partition transform_param",
                        )
                    }
                    val transform =
                        try {
                            Transform.fromWire(
                                text(field, "transform", "partition field", ::corrupt),
                            )
                        } catch (e: IllegalArgumentException) {
                            corrupt("invalid partition transform", e)
                        }
                    PartitionFieldDef(
                        source.longValue(),
                        transform,
                        param?.takeIf { !it.isNull }?.intValue(),
                    )
                }
            } else {
                emptyList()
            },
        )
    }

    /** A stored fragment, capped for an error message. */
    private fun cap(value: String): String = if (value.length > 40) value.take(37) + "..." else value

    /** A required string field, or the named refusal. */
    private fun text(
        node: JsonNode,
        field: String,
        where: String,
        corrupt: (String, Throwable?) -> Nothing,
    ): String {
        val value = node[field]
        if (value == null || value.isNull || !value.isTextual) {
            corrupt("the stored definition is missing the string field '$field' in ${cap(where)}", null)
        }
        return value.asText()
    }

    /**
     * An optional boolean field, [default] when absent — and a named
     * refusal when PRESENT and not a boolean.
     *
     * `asBoolean()` coerces: a string, an object, a number and an
     * explicit JSON null all answer `false`. `nullable` defaulting to
     * `true`, that turned a corrupt receipt into a valid-looking
     * definition with a REQUIRED column, which then published — a
     * different table from the one the caller prepared, created
     * silently, which is exactly what the named-corruption contract
     * exists to prevent.
     */
    private fun bool(
        node: JsonNode,
        field: String,
        where: String,
        default: Boolean,
        corrupt: (String, Throwable?) -> Nothing,
    ): Boolean {
        val value = node[field] ?: return default
        if (value.isNull) {
            corrupt("the stored definition has a null '$field' in ${cap(where)}; omit it or give a boolean", null)
        }
        if (!value.isBoolean) {
            corrupt(
                "the stored definition has a non-boolean '$field' " +
                    "(${cap(value.toString())}) in ${cap(where)}",
                null,
            )
        }
        return value.asBoolean()
    }

    private fun decodeColumn(
        column: JsonNode,
        version: Int,
        corrupt: (String, Throwable?) -> Nothing,
    ): ColumnDef {
        if (!column.isObject) corrupt("the stored definition has a non-object column", null)
        val name = text(column, "name", "a column", corrupt)
        val storedType = text(column, "type", "column '${cap(name)}'", corrupt)
        val wireType =
            if (version == 0) {
                // Compatibility with receipts written before the versioned format.
                if (storedType == "UUID_T") "uuid" else storedType.lowercase()
            } else {
                storedType
            }
        val params = column[if (version == 0) "typeParams" else "type_params"]
        val children = column["children"]
        val type =
            try {
                ColType.fromWire(wireType)
            } catch (e: Exception) {
                corrupt(
                    "the stored definition gives column '${cap(name)}' the unknown type " +
                        "'${cap(storedType)}'",
                    e,
                )
            }
        // SHAPE first, then the conversion — for the MESSAGE, not for
        // the catch. Measured on this Jackson: every non-object node
        // (array, number, boolean, string, even `[]`) already throws
        // inside convertValue and reaches the same typed refusal, so
        // this guard catches nothing the fallback would miss. What it
        // buys is a diagnostic that names the defect — "non-object
        // 'type_params'" — instead of Jackson's generic conversion
        // failure. That is the only claim it should make, and the test
        // pins the message rather than the refusal.
        if (params != null && !params.isNull && !params.isObject) {
            corrupt(
                "the stored definition has a non-object 'type_params' " +
                    "(${cap(params.toString())}) for column '${cap(name)}'",
                null,
            )
        }
        val decodedParams =
            try {
                if (params == null || params.isNull) null else mapper.convertValue(params, paramsType)
            } catch (e: Exception) {
                corrupt("the stored definition has unreadable type_params for column '${cap(name)}'", e)
            }
        if (children != null && !children.isNull && !children.isArray) {
            corrupt("the stored definition has non-array 'children' for column '${cap(name)}'", null)
        }
        return ColumnDef(
            name,
            type,
            decodedParams,
            bool(column, "nullable", "column '${cap(name)}'", default = true, corrupt = corrupt),
            // ABSENT children decode to null, not to an empty list: every
            // version-0 and version-1 receipt is a scalar definition, and
            // `children: []` on a scalar is a named 422 (ColumnTrees), so
            // reading one back as empty would make old receipts
            // unpublishable.
            if (children == null || children.isNull) null else children.map { decodeColumn(it, version, corrupt) },
        )
    }
}

/**
 * A stored table-creation receipt that cannot be read back.
 *
 * Every field [TableCreationDefinitionCodec.decode] reads was written by
 * [TableCreationDefinitionCodec.encode], so reaching this means the row
 * was truncated, hand-edited, or written by something else — and the
 * blob is caller-supplied only in the sense that a caller's definition
 * went in. What came back out is the server's own storage, so this is
 * not a 422; it is one named 500 that says WHICH receipt is unreadable
 * and why.
 *
 * It exists because the alternative was five different raw
 * NullPointerExceptions (one per missing field), an
 * IllegalArgumentException out of `ColType.fromWire`, and a Jackson
 * parse error, each surfacing as a bare "500 internal error" with a
 * stack trace pointing at a `.asText()` call rather than at the row.
 */
class CorruptDefinitionException(message: String, cause: Throwable? = null) :
    IllegalStateException(message, cause)
