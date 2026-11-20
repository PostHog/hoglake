package com.posthog.hoglake.store

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.auth.ReadSessionProfile
import com.posthog.hoglake.auth.RouteMatch
import com.posthog.hoglake.service.Identifiers
import io.ktor.http.HttpMethod
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID

/** One operator attestation of local isolation and typed support across the owners. */
internal data class ScopedNamespaceBinding(
    val catalog: String,
    val backendId: UUID,
    val tenant: String,
    val namespace: String,
    val readSession: ReadSessionProfile? = null,
) {
    fun matches(
        method: HttpMethod,
        route: RouteMatch?,
    ): Boolean =
        method == HttpMethod.Get && route?.pattern == "/v1/catalogs/{catalog}/namespaces/{namespace}" &&
            route.catalog == catalog && route.namespace == namespace

    companion object {
        fun read(file: String): ScopedNamespaceBinding? {
            if (file.isBlank()) return null
            val mapper =
                jacksonObjectMapper()
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            val root = Files.newBufferedReader(Path.of(file)).use { mapper.readTree(it) }
            val keys = setOf("catalog", "backend", "backend_id", "tenant", "namespace")
            val actualKeys = root?.takeIf(JsonNode::isObject)?.properties()?.map { it.key }?.toSet()
            require(root != null && actualKeys?.minus("read_session") == keys) {
                "HOGLAKE_SCOPED_NAMESPACE_FILE requires one complete binding"
            }

            fun text(key: String): String {
                val value = root.get(key)
                require(value.isTextual && value.asText().isNotEmpty()) {
                    "HOGLAKE_SCOPED_NAMESPACE_FILE requires nonempty strings"
                }
                return value.asText()
            }
            require(text("backend") == BackendKind.HOGLITE.wire) {
                "HOGLAKE_SCOPED_NAMESPACE_FILE supports only hoglite"
            }
            val rawId = text("backend_id")
            val id = UUID.fromString(rawId)
            require(id.toString() == rawId) { "scoped namespace backend id must be canonical" }
            val catalog = text("catalog")
            val namespace = text("namespace")
            val tenant = text("tenant")
            require(Regex(Identifiers.CATALOG_NAME_PATTERN).matches(catalog)) {
                "scoped namespace catalog must be canonical"
            }
            require(Regex(Identifiers.PATTERN).matches(namespace)) {
                "scoped namespace name must be canonical"
            }
            require(
                Regex("[A-Za-z0-9._-]{1,128}").matches(tenant) && !tenant.startsWith('.') && ".." !in tenant,
            ) {
                "scoped namespace tenant must be canonical"
            }
            val readSession = root.get("read_session")?.let(::readSession)
            return ScopedNamespaceBinding(catalog, id, tenant, namespace, readSession)
        }

        private fun readSession(node: JsonNode): ReadSessionProfile {
            require(node.isObject && node.properties().map { it.key }.toSet() == setOf("profile", "remote_targets")) {
                "read_session requires profile and remote_targets"
            }
            val profile = node["profile"].textValue()
            val targets = node["remote_targets"]
            require(profile != null && targets.isArray && targets.size() == 2 && targets.all(JsonNode::isTextual)) {
                "read_session requires a profile and two distinct remote targets"
            }
            return ReadSessionProfile(profile, targets.map(JsonNode::textValue).toSet())
        }
    }
}
