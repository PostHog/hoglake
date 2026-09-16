package com.posthog.hoglake.service

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef

/**
 * Versioned durable format. Type spellings are API wire names, never JVM
 * enum names.
 *
 * **Versions.** 0 is the pre-versioned shape (JVM enum names,
 * `typeParams`); 1 added the version marker and wire spellings; 2 adds
 * `children`, without which a nested definition cannot be represented
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
                "version" to if (anyChildren(definition.columns)) NESTED_VERSION else FLAT_VERSION,
                "namespace" to definition.namespace,
                "name" to definition.name,
                "columns" to definition.columns.map { encodeColumn(it) },
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
    fun decode(encoded: String): TableCreationDefinition {
        val node =
            try {
                mapper.readTree(encoded)
            } catch (e: Exception) {
                throw CorruptDefinitionException("stored table creation definition is not valid JSON", e)
            }
        if (node == null || !node.isObject) {
            throw CorruptDefinitionException("stored table creation definition is not a JSON object")
        }
        val version = node["version"]?.asInt() ?: 0
        if (version !in 0..NESTED_VERSION) {
            throw CorruptDefinitionException("unsupported table creation definition version $version")
        }
        val columns =
            node["columns"]
                ?.takeIf { it.isArray }
                ?: throw CorruptDefinitionException("stored table creation definition has no 'columns' array")
        return TableCreationDefinition(
            text(node, "namespace", "the definition"),
            text(node, "name", "the definition"),
            columns.map { decodeColumn(it, version) },
        )
    }

    /** A required string field, or the named refusal. */
    private fun text(
        node: JsonNode,
        field: String,
        where: String,
    ): String {
        val value = node[field]
        if (value == null || value.isNull || !value.isTextual) {
            throw CorruptDefinitionException(
                "stored table creation definition is missing the string field '$field' in $where",
            )
        }
        return value.asText()
    }

    private fun decodeColumn(
        column: JsonNode,
        version: Int,
    ): ColumnDef {
        if (!column.isObject) {
            throw CorruptDefinitionException("stored table creation definition has a non-object column")
        }
        val name = text(column, "name", "a column")
        val storedType = text(column, "type", "column '$name'")
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
                throw CorruptDefinitionException(
                    "stored table creation definition gives column '$name' the unknown type '$storedType'",
                    e,
                )
            }
        val decodedParams =
            try {
                if (params == null || params.isNull) null else mapper.convertValue(params, paramsType)
            } catch (e: Exception) {
                throw CorruptDefinitionException(
                    "stored table creation definition has unreadable type_params for column '$name'",
                    e,
                )
            }
        if (children != null && !children.isNull && !children.isArray) {
            throw CorruptDefinitionException(
                "stored table creation definition has non-array 'children' for column '$name'",
            )
        }
        return ColumnDef(
            name,
            type,
            decodedParams,
            column["nullable"]?.asBoolean() ?: true,
            // ABSENT children decode to null, not to an empty list: every
            // version-0 and version-1 receipt is a scalar definition, and
            // `children: []` on a scalar is a named 422 (ColumnTrees), so
            // reading one back as empty would make old receipts
            // unpublishable.
            if (children == null || children.isNull) null else children.map { decodeColumn(it, version) },
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
internal class CorruptDefinitionException(message: String, cause: Throwable? = null) :
    IllegalStateException(message, cause)
