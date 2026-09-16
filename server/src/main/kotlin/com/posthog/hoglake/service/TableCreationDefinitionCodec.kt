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

    fun decode(encoded: String): TableCreationDefinition {
        val node = mapper.readTree(encoded)
        val version = node["version"]?.asInt() ?: 0
        check(version in 0..NESTED_VERSION) { "unsupported table creation definition version $version" }
        return TableCreationDefinition(
            node["namespace"].asText(),
            node["name"].asText(),
            node["columns"].map { decodeColumn(it, version) },
        )
    }

    private fun decodeColumn(
        column: JsonNode,
        version: Int,
    ): ColumnDef {
        val storedType = column["type"].asText()
        val wireType =
            if (version == 0) {
                // Compatibility with receipts written before the versioned format.
                if (storedType == "UUID_T") "uuid" else storedType.lowercase()
            } else {
                storedType
            }
        val params = column[if (version == 0) "typeParams" else "type_params"]
        val children = column["children"]
        return ColumnDef(
            column["name"].asText(),
            ColType.fromWire(wireType),
            if (params == null || params.isNull) null else mapper.convertValue(params, paramsType),
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
