package com.posthog.hoglake.service

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef

/** Versioned durable format. Type spellings are API wire names, never JVM enum names. */
internal object TableCreationDefinitionCodec {
    private val mapper = jacksonObjectMapper().enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS)
    private val paramsType = object : TypeReference<Map<String, Any?>>() {}

    fun encode(definition: TableCreationDefinition): String =
        mapper.writeValueAsString(
            mapOf(
                "version" to 1,
                "namespace" to definition.namespace,
                "name" to definition.name,
                "columns" to
                    definition.columns.map { column ->
                        mapOf(
                            "name" to column.name,
                            "type" to column.type.wire,
                            "type_params" to column.typeParams,
                            "nullable" to column.nullable,
                        )
                    },
            ),
        )

    fun decode(encoded: String): TableCreationDefinition {
        val node = mapper.readTree(encoded)
        val version = node["version"]?.asInt() ?: 0
        check(version == 0 || version == 1) { "unsupported table creation definition version $version" }
        return TableCreationDefinition(
            node["namespace"].asText(),
            node["name"].asText(),
            node["columns"].map { column ->
                val storedType = column["type"].asText()
                val wireType =
                    if (version == 0) {
                        // Compatibility with receipts written before the versioned format.
                        if (storedType == "UUID_T") "uuid" else storedType.lowercase()
                    } else {
                        storedType
                    }
                val params = column[if (version == 0) "typeParams" else "type_params"]
                ColumnDef(
                    column["name"].asText(),
                    ColType.fromWire(wireType),
                    if (params == null || params.isNull) null else mapper.convertValue(params, paramsType),
                    column["nullable"]?.asBoolean() ?: true,
                )
            },
        )
    }
}
