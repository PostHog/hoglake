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
    val table: String? = null,
    /**
     * A configured remote schema relays [namespace] by this exact name. Only a
     * request on a [LOCAL_ONLY_PREFIX] path selects this binding. Every request
     * on the ordinary routes keeps the remote schema's relay.
     */
    val localOnly: Boolean = false,
) {
    init {
        require(table == null || readSession != null) {
            "scoped table binding requires read_session"
        }
        require(!localOnly || readSession != null) {
            "a local-only binding requires read_session"
        }
    }

    fun matchesNamespace(
        method: HttpMethod,
        route: RouteMatch?,
    ): Boolean =
        method == HttpMethod.Get &&
            route?.pattern == NAMESPACE_PATTERN &&
            route.catalog == catalog &&
            route.namespace == namespace

    fun matchesTable(
        method: HttpMethod,
        route: RouteMatch?,
    ): Boolean =
        table != null &&
            readSession != null &&
            method == HttpMethod.Get &&
            route?.pattern == TABLE_PATTERN &&
            route.catalog == catalog &&
            route.namespace == namespace &&
            route.table == table

    fun matchesScan(
        method: HttpMethod,
        route: RouteMatch?,
    ): Boolean =
        table != null &&
            readSession != null &&
            method == HttpMethod.Get &&
            route?.pattern == SCAN_PATTERN &&
            route.catalog == catalog &&
            route.namespace == namespace &&
            route.table == table

    fun matches(
        method: HttpMethod,
        route: RouteMatch?,
    ): Boolean = matches(method, route, localOnlyRequest = false)

    /** A [localOnly] binding matches only a local-only request. */
    fun matches(
        method: HttpMethod,
        route: RouteMatch?,
        localOnlyRequest: Boolean,
    ): Boolean =
        (!localOnly || localOnlyRequest) &&
            (matchesNamespace(method, route) || matchesTable(method, route) || matchesScan(method, route))

    companion object {
        /**
         * The path prefix of the local-only read routes. A request on it reaches only
         * this binding's typed reads, or gets an ordinary refusal. It never reaches
         * a remote schema relay or a legacy store. A server that predates the mode
         * has no route under this prefix, so it cannot serve or relay the request.
         */
        const val LOCAL_ONLY_PREFIX = "/v1/local-only"

        /**
         * The response marker of the local-only routes. The server sends it on each
         * answer to a well-formed local-only request on a listener that serves the
         * mode. A client reads a 404 as absence only when the marker is present.
         */
        const val LOCAL_ONLY_MARKER = "X-Hoglake-Local-Only"

        /** The one value of [LOCAL_ONLY_MARKER]. */
        const val LOCAL_ONLY_MARKER_VALUE = "true"

        private const val NAMESPACE_PATTERN = "/v1/catalogs/{catalog}/namespaces/{namespace}"
        private const val TABLE_PATTERN = "/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}"
        private const val SCAN_PATTERN = "/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/scan"

        /** The ordinary route patterns that the local-only routes name, for the policy match only. */
        val LOCAL_ONLY_PATTERNS = setOf(NAMESPACE_PATTERN, TABLE_PATTERN, SCAN_PATTERN)

        /** Whether [path] is under [LOCAL_ONLY_PREFIX]. */
        fun isLocalOnlyPath(path: String): Boolean = path == LOCAL_ONLY_PREFIX || path.startsWith("$LOCAL_ONLY_PREFIX/")

        /**
         * The ordinary path that a local-only [path] names, for the policy match
         * only. Null for every other path. The request URI is never rewritten.
         */
        fun canonicalLocalOnlyPath(path: String): String? =
            if (path.startsWith("$LOCAL_ONLY_PREFIX/catalogs/")) "/v1" + path.removePrefix(LOCAL_ONLY_PREFIX) else null

        fun read(file: String): ScopedNamespaceBinding? {
            if (file.isBlank()) return null
            val mapper =
                jacksonObjectMapper()
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            val root = Files.newBufferedReader(Path.of(file)).use { mapper.readTree(it) }
            val keys = setOf("catalog", "backend", "backend_id", "tenant", "namespace")
            val optional = setOf("read_session", "table")
            val actualKeys = root?.takeIf(JsonNode::isObject)?.properties()?.map { it.key }?.toSet()
            require(root != null && actualKeys?.minus(optional) == keys) {
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
            val table =
                root.get("table")?.let { node ->
                    val name = node.textValue()
                    require(name != null && Regex(Identifiers.PATTERN).matches(name)) {
                        "scoped namespace table must be canonical"
                    }
                    name
                }
            return ScopedNamespaceBinding(catalog, id, tenant, namespace, readSession, table)
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
