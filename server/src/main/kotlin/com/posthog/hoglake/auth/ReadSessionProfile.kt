package com.posthog.hoglake.auth

import com.fasterxml.jackson.databind.JsonNode
import com.posthog.hoglake.service.Identifiers
import com.posthog.hoglake.service.ReservedNamespaces
import com.posthog.hoglake.service.TeamNamespaces
import com.posthog.hoglake.store.MountNames
import io.ktor.http.HttpMethod
import java.nio.ByteBuffer
import java.nio.CharBuffer
import java.nio.charset.CodingErrorAction
import java.nio.charset.StandardCharsets
import java.util.UUID

/**
 * Configuration for a dedicated read verifier. The receiver supplies the profile ID
 * and the two remote storage targets. No ordinary route selects this verifier.
 */
internal class ReadSessionProfile(
    private val profileId: String,
    remoteTargets: Set<String>,
) {
    private val remoteTargets = remoteTargets.toSet()

    init {
        require(profileId.isNotBlank() && profileId != SessionProfile.ID)
        require(this.remoteTargets.size == 2 && this.remoteTargets.all { it.isNotBlank() })
    }

    fun validate(
        claims: JsonNode,
        now: Long,
        skew: Long,
    ): ReadSessionStorageGrants {
        val audiences = claims.get("aud")
        if (audiences?.isArray != true || audiences.size() != AUDIENCES.size ||
            audiences.any { !it.isTextual } || audiences.map { it.asText() }.toSet() != AUDIENCES
        ) {
            throw JwtViolation(JwtViolation.WRONG_AUDIENCE, "invalid read session audience")
        }
        if (text(claims, "profile") != profileId || seconds(claims, "version") != 1L ||
            text(claims, "purpose") != SessionProfile.PURPOSE || text(claims, "ten") != "scoped"
        ) {
            invalid()
        }
        if (text(claims, "sub") != text(claims, "usr") || claims.has("dlg") ||
            claims.has("scope") || claims.has("svc")
        ) {
            invalid()
        }
        text(claims, "org")
        text(claims, "sid")
        text(claims, "jti")
        if (seconds(claims, "generation") != 1L) invalid()
        val primary = text(claims, "primary_catalog")
        val writable = parsePrimaryCatalogAccess(claims.get("cat"), primary)
        requireSingleRead(claims.get("remote_schemas"), TeamNamespaces.FAMILY)
        val predicates = claims.get("prd")
        val teams = predicates?.get("team_id")
        if (predicates?.isObject != true || predicates.size() != 1 || teams?.isObject != true || teams.size() != 1) {
            invalid()
        }
        val team = teams.properties().single()
        val expectedTeamScope = if (writable) "write" else "read"
        if (!POSITIVE_INTEGER.matches(team.key) || team.key.toLongOrNull() == null ||
            team.value.textValue() != expectedTeamScope
        ) {
            invalid()
        }

        val issued = seconds(claims, "iat")
        val notBefore = seconds(claims, "nbf")
        val expiry = seconds(claims, "exp")
        val deadline = seconds(claims, "session_deadline")
        val aws = claims.get("aws") ?: invalid()
        if (!aws.isObject || aws.size() != 5) invalid()
        for (field in listOf("access_key_id", "secret_access_key", "session_token")) text(aws, field)
        val region = text(aws, "region")
        val awsExpiry = seconds(aws, "expires_at")
        if (notBefore > issued || issued >= expiry || expiry > awsExpiry || awsExpiry > deadline) invalid()
        if (now >= awsExpiry || (now > expiry && now - expiry > skew)) {
            throw JwtViolation(JwtViolation.EXPIRED, "read session expired")
        }
        if (issued > now && issued - now > skew) {
            throw JwtViolation(JwtViolation.NOT_YET_VALID, "read session is not valid yet")
        }

        val route = claims.get("rtg") ?: invalid()
        if (!route.isObject || route.size() != 3 || text(route, "b") != "hoglite") invalid()
        val backend = text(route, "be")
        val id = runCatching { UUID.fromString(backend) }.getOrNull() ?: invalid()
        if (id.toString() != backend || id == UUID(0, 0)) invalid()
        val tenant = text(route, "t")
        val incarnation = tenant.substringAfterLast('_', "")
        if (!ROUTED_TENANT.matches(tenant) || ".." in tenant ||
            !POSITIVE_INTEGER.matches(incarnation) || incarnation.toLongOrNull() == null
        ) {
            invalid()
        }
        return validateGrants(claims.get("grants"), primary, backend, tenant, region, writable)
    }

    private fun validateGrants(
        grants: JsonNode?,
        catalog: String,
        backend: String,
        tenant: String,
        region: String,
        writable: Boolean,
    ): ReadSessionStorageGrants {
        val targets = remoteTargets + catalog + backend
        if (targets.size != 4 || grants?.isArray != true || grants.size() < targets.size) invalid()
        var hash = 0x811c9dc5u
        for (character in tenant) hash = (hash xor character.code.toUInt()) * 0x01000193u
        val shard = (hash and 255u).toString(16).padStart(2, '0')
        val manifestSuffix = "/$shard/t/$tenant/manifest"
        val seen = HashSet<String>()
        val parsed = ArrayList<ReadSessionStorageGrant>(grants.size())
        var previousBucket = ""
        var previousKey = ""
        for (grant in grants) {
            if (!grant.isObject || grant.size() != 6 || grant.has("object_key") == grant.has("prefix")) invalid()
            val bucket = text(grant, "bucket")
            val prefix = grant.has("prefix")
            val key = text(grant, if (prefix) "prefix" else "object_key")
            val target = text(grant, "target")
            val actions = grant.get("actions")
            val expectedActions = if (writable && target == catalog) WRITE_ACTIONS else READ_ACTIONS
            if (!BUCKET.matches(bucket) || ".." in bucket || key.length > 1024 ||
                prefix != key.endsWith('/') ||
                key.removeSuffix("/").split('/').any { it == "." || it == ".." || !SEGMENT.matches(it) } ||
                target !in targets || text(grant, "catalog") != catalog || text(grant, "region") != region ||
                !matchesActions(actions, expectedActions)
            ) {
                invalid()
            }
            if (writable && target == catalog && !prefix) invalid()
            if (target == backend && (prefix || !key.endsWith(manifestSuffix) || target in seen)) invalid()
            val overlaps = previousKey.endsWith('/') && key.startsWith(previousKey)
            if (bucket < previousBucket || (bucket == previousBucket && (key <= previousKey || overlaps))) {
                invalid()
            }
            previousBucket = bucket
            previousKey = key
            seen.add(target)
            parsed.add(ReadSessionStorageGrant(catalog, target, bucket, region, key, prefix))
        }
        if (seen != targets) invalid()
        return ReadSessionStorageGrants(region, parsed, remoteTargets, writable)
    }

    /** Return true when [map] grants read+write on [key], false for read-only, or throw on any other shape. */
    private fun parsePrimaryCatalogAccess(
        map: JsonNode?,
        key: String,
    ): Boolean {
        val scopes = map?.get(key)
        if (map?.isObject != true || map.size() != 1 || scopes?.isArray != true) invalid()
        return when (scopes.size()) {
            1 -> {
                if (scopes[0].textValue() != "read") invalid()
                false
            }
            2 -> {
                if (scopes[0].textValue() != "read" || scopes[1].textValue() != "write") invalid()
                true
            }
            else -> invalid()
        }
    }

    private fun matchesActions(
        actions: JsonNode?,
        expected: List<String>,
    ): Boolean {
        if (actions?.isArray != true || actions.size() != expected.size) return false
        for (i in expected.indices) {
            if (actions[i].textValue() != expected[i]) return false
        }
        return true
    }

    private fun requireSingleRead(
        map: JsonNode?,
        key: String,
    ) {
        val scopes = map?.get(key)
        if (map?.isObject != true || map.size() != 1 || scopes?.isArray != true ||
            scopes.size() != 1 || scopes[0].textValue() != "read"
        ) {
            invalid()
        }
    }

    private fun text(
        node: JsonNode,
        field: String,
    ): String = node.get(field)?.textValue()?.takeIf { it.isNotBlank() } ?: invalid()

    private fun seconds(
        node: JsonNode,
        field: String,
    ): Long =
        node.get(field)?.takeIf { it.isIntegralNumber && it.canConvertToLong() }?.longValue()
            ?.takeIf { it > 0L } ?: invalid()

    private fun invalid(): Nothing = throw JwtViolation(JwtViolation.MALFORMED, "invalid read session claims")

    companion object {
        val AUDIENCES = setOf("trino-session", "hoglake-session", "hoglite-session")
        val READ_ACTIONS = listOf("s3:GetObject")
        val WRITE_ACTIONS =
            listOf(
                "s3:AbortMultipartUpload",
                "s3:GetObject",
                "s3:ListMultipartUploadParts",
                "s3:PutObject",
            )
        const val LOCAL_ONLY_PREFIX = "/v1/local-only"
        const val LOCAL_ONLY_MARKER = "X-Hoglake-Local-Only"
        const val LOCAL_ONLY_MARKER_VALUE = "true"
        internal const val NAMESPACES_PATTERN = "/v1/catalogs/{catalog}/namespaces"
        internal const val NAMESPACE_PATTERN = "/v1/catalogs/{catalog}/namespaces/{namespace}"
        internal const val TABLES_PATTERN = "/v1/catalogs/{catalog}/namespaces/{namespace}/tables"
        internal const val TABLE_PATTERN = "/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}"
        internal const val SCAN_PATTERN = "/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/scan"
        internal const val COMMIT_PATTERN = "/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/commit"
        val LOCAL_ONLY_PATTERNS =
            setOf(
                NAMESPACES_PATTERN,
                NAMESPACE_PATTERN,
                TABLES_PATTERN,
                TABLE_PATTERN,
                SCAN_PATTERN,
                COMMIT_PATTERN,
            )
        val LOCAL_ONLY_ALLOWED_METHODS: Map<String, Set<HttpMethod>> =
            mapOf(
                NAMESPACES_PATTERN to setOf(HttpMethod.Get, HttpMethod.Post),
                NAMESPACE_PATTERN to setOf(HttpMethod.Get, HttpMethod.Delete),
                TABLES_PATTERN to setOf(HttpMethod.Get, HttpMethod.Post),
                TABLE_PATTERN to setOf(HttpMethod.Get, HttpMethod.Delete),
                SCAN_PATTERN to setOf(HttpMethod.Get),
                COMMIT_PATTERN to setOf(HttpMethod.Post),
            )

        /** The four routes a remote schema relays (docs/auth-tenancy.md §6.5c). */
        private val RELAY_PATTERNS = setOf(NAMESPACE_PATTERN, TABLES_PATTERN, TABLE_PATTERN, SCAN_PATTERN)
        private const val MAX_PROFILE_LENGTH = 256
        private const val S3_PREFIX = "s3://"
        private const val MAX_KEY_LENGTH = 1024
        private val CATALOG_NAME_REGEX = Regex(Identifiers.CATALOG_NAME_PATTERN)
        private val IDENTIFIER_REGEX = Regex(Identifiers.PATTERN)
        private val POSITIVE_INTEGER = Regex("[1-9][0-9]*")
        private val ROUTED_TENANT = Regex("[A-Za-z0-9][A-Za-z0-9._-]{0,125}_[1-9][0-9]*")
        internal val BUCKET = Regex("[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
        internal val SEGMENT = Regex("[A-Za-z0-9_.=-]+")

        /** Parse HOGLAKE_READ_SESSION_PROFILE and HOGLAKE_READ_SESSION_REMOTE_TARGETS. */
        internal fun fromEnv(
            profile: String,
            remoteTargetsCsv: String,
        ): ReadSessionProfile? {
            if (profile.isBlank() && remoteTargetsCsv.isBlank()) return null
            require(profile.isNotBlank() && remoteTargetsCsv.isNotBlank()) {
                "HOGLAKE_READ_SESSION_PROFILE and HOGLAKE_READ_SESSION_REMOTE_TARGETS must both be set"
            }
            require(profile.trim() == profile && profile.length <= MAX_PROFILE_LENGTH && profile != SessionProfile.ID) {
                "HOGLAKE_READ_SESSION_PROFILE is invalid"
            }
            val rawTargets = remoteTargetsCsv.split(',')
            require(
                rawTargets.size == 2 &&
                    rawTargets.all { it.isNotBlank() && it.trim() == it } &&
                    rawTargets[0] != rawTargets[1],
            ) {
                "HOGLAKE_READ_SESSION_REMOTE_TARGETS requires two distinct non-blank targets"
            }
            return ReadSessionProfile(profile, rawTargets.toSet())
        }

        /** Return true when [path] is under [LOCAL_ONLY_PREFIX]. */
        fun isLocalOnlyPath(path: String): Boolean = path == LOCAL_ONLY_PREFIX || path.startsWith("$LOCAL_ONLY_PREFIX/")

        /** Return the ordinary path that a local-only [path] names, or null. */
        fun canonicalLocalOnlyPath(path: String): String? =
            if (path.startsWith("$LOCAL_ONLY_PREFIX/catalogs/")) "/v1" + path.removePrefix(LOCAL_ONLY_PREFIX) else null

        /** Return true when [method] is a local-only metadata mutation. */
        internal fun isLocalOnlyMutation(method: HttpMethod): Boolean =
            method == HttpMethod.Post || method == HttpMethod.Delete

        /**
         * Match a local-only request path to its canonical route pattern and path segments.
         * Returns null if the path does not match any local-only route shape.
         */
        internal fun matchLocalOnlyRoute(canonicalPath: String): LocalOnlyPathMatch? {
            val parts = canonicalPath.split('/')
            val validPrefix =
                parts.size >= 5 &&
                    parts[0] == "" &&
                    parts[1] == "v1" &&
                    parts[2] == "catalogs" &&
                    parts[4] == "namespaces"
            if (!validPrefix) return null
            val catalog = parts[3]
            return when (parts.size) {
                5 -> LocalOnlyPathMatch(NAMESPACES_PATTERN, catalog, null, null)
                6 -> LocalOnlyPathMatch(NAMESPACE_PATTERN, catalog, parts[5], null)
                7 -> if (parts[6] == "tables") LocalOnlyPathMatch(TABLES_PATTERN, catalog, parts[5], null) else null
                8 ->
                    if (parts[6] == "tables") {
                        LocalOnlyPathMatch(TABLE_PATTERN, catalog, parts[5], parts[7])
                    } else {
                        null
                    }
                9 ->
                    when {
                        parts[6] == "tables" && parts[8] == "scan" ->
                            LocalOnlyPathMatch(SCAN_PATTERN, catalog, parts[5], parts[7])
                        parts[6] == "tables" && parts[8] == "commit" ->
                            LocalOnlyPathMatch(COMMIT_PATTERN, catalog, parts[5], parts[7])
                        else -> null
                    }
                else -> null
            }
        }

        internal fun matchesNamespace(
            method: HttpMethod,
            route: RouteMatch?,
        ): Boolean = method == HttpMethod.Get && route?.pattern == NAMESPACE_PATTERN

        internal fun matchesTable(
            method: HttpMethod,
            route: RouteMatch?,
        ): Boolean = method == HttpMethod.Get && route?.pattern == TABLE_PATTERN

        internal fun matchesScan(
            method: HttpMethod,
            route: RouteMatch?,
        ): Boolean = method == HttpMethod.Get && route?.pattern == SCAN_PATTERN

        internal fun isCanonicalRoute(
            catalog: String,
            namespace: String?,
            table: String? = null,
        ): Boolean =
            CATALOG_NAME_REGEX.matches(catalog) &&
                (namespace == null || IDENTIFIER_REGEX.matches(namespace)) &&
                (table == null || IDENTIFIER_REGEX.matches(table))

        internal fun isEligibleRoute(
            method: HttpMethod,
            route: RouteMatch?,
            reservedNamespaces: ReservedNamespaces,
        ): Boolean {
            if (!matchesNamespace(method, route) && !matchesTable(method, route) && !matchesScan(method, route)) {
                return false
            }
            val catalog = route?.catalog ?: return false
            val namespace = route.namespace ?: return false
            val table = if (route.pattern == NAMESPACE_PATTERN) null else route.table ?: return false
            if (MountNames.isReserved(catalog) || reservedNamespaces.isReserved(namespace)) return false
            return isCanonicalRoute(catalog, namespace, table)
        }

        /**
         * A relayed `GET` route whose namespace a remote schema covers
         * ([relays]), in a catalog that is not a mount. A read session is
         * verified here so that it can be relayed, and for nothing else: the
         * store selector hands it the remote schema or a 404.
         */
        internal fun isRelayRoute(
            method: HttpMethod,
            route: RouteMatch?,
            relays: (String) -> Boolean,
        ): Boolean {
            if (method != HttpMethod.Get || route?.pattern !in RELAY_PATTERNS) return false
            val catalog = route?.catalog ?: return false
            val namespace = route.namespace ?: return false
            return !MountNames.isReserved(catalog) && CATALOG_NAME_REGEX.matches(catalog) && relays(namespace)
        }

        /**
         * A relayed `GET` route of a served namespace ([serves]), as a Host
         * receives it. A read session is verified here so that the served
         * rule can admit it, and for nothing else.
         */
        internal fun isServedRoute(
            method: HttpMethod,
            route: RouteMatch?,
            serves: (String) -> Boolean,
        ): Boolean {
            if (method != HttpMethod.Get || route?.pattern !in RELAY_PATTERNS) return false
            val catalog = route?.catalog ?: return false
            val namespace = route.namespace ?: return false
            return !MountNames.isReserved(catalog) && CATALOG_NAME_REGEX.matches(catalog) && serves(namespace)
        }

        /** Return true when the unverified JWT carries any dedicated read-session audience. */
        internal fun hasDedicatedAudience(token: String): Boolean {
            if (token.length > 65_536 || token.indexOf('.') !in 1..10_923) return false
            val aud =
                try {
                    Jwt.parse(token).claims.get("aud")
                } catch (_: JwtViolation) {
                    return false
                } ?: return false
            return when {
                aud.isTextual -> aud.asText() in AUDIENCES
                aud.isArray -> aud.any { it.isTextual && it.asText() in AUDIENCES }
                else -> false
            }
        }

        internal fun parseS3ObjectUri(path: String): Pair<String, String>? {
            if (!path.startsWith(S3_PREFIX)) return null
            val remainder = path.substring(S3_PREFIX.length)
            val slash = remainder.indexOf('/')
            if (slash <= 0 || slash >= remainder.length - 1) return null
            val bucket = remainder.substring(0, slash)
            val key = remainder.substring(slash + 1)
            if (!BUCKET.matches(bucket) || ".." in bucket) return null
            if (key.isEmpty() || key.endsWith('/') || key.any(Char::isISOControl)) return null
            if (strictUtf8ByteLength(key, MAX_KEY_LENGTH) == null) return null
            if (key.split('/').any { it.isEmpty() || it == "." || it == ".." }) return null
            return bucket to key
        }

        private fun strictUtf8ByteLength(
            value: String,
            maxBytes: Int,
        ): Int? {
            val encoder =
                StandardCharsets.UTF_8
                    .newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
            val output = ByteBuffer.allocate(maxBytes)
            val result = encoder.encode(CharBuffer.wrap(value), output, true)
            if (!result.isUnderflow || !encoder.flush(output).isUnderflow) return null
            return output.position()
        }
    }
}

/** One verified read-only S3 grant from a dedicated read session. */
internal data class ReadSessionStorageGrant(
    val catalog: String,
    val target: String,
    val bucket: String,
    val region: String,
    val path: String,
    val prefix: Boolean,
) {
    fun covers(
        bucket: String,
        objectKey: String,
    ): Boolean = this.bucket == bucket && if (prefix) objectKey.startsWith(path) else objectKey == path

    override fun toString(): String = "ReadSessionStorageGrant(redacted)"
}

/** Verified storage grants from a dedicated session without AWS credentials. */
internal class ReadSessionStorageGrants(
    val region: String,
    private val grants: List<ReadSessionStorageGrant>,
    private val remoteTargets: Set<String> = emptySet(),
    val writable: Boolean = false,
) {
    /** Return true when a signed grant of one of the configured remote targets allows the S3 object URI. */
    fun allowsRemoteTargetObject(path: String): Boolean {
        val (bucket, key) = ReadSessionProfile.parseS3ObjectUri(path) ?: return false
        return grants.any { it.target in remoteTargets && it.region == region && it.covers(bucket, key) }
    }

    /** Return true when a signed primary-catalog grant allows the S3 object URI. */
    fun allowsPrimaryCatalogObject(
        catalog: String,
        path: String,
    ): Boolean {
        val (bucket, key) = ReadSessionProfile.parseS3ObjectUri(path) ?: return false
        return grants.any {
            it.catalog == catalog && it.target == catalog && it.region == region && it.covers(bucket, key)
        }
    }

    /** Return true when this session is writable and a signed primary-catalog grant allows the S3 object URI. */
    fun allowsPrimaryCatalogWrite(
        catalog: String,
        path: String,
    ): Boolean = writable && allowsPrimaryCatalogObject(catalog, path)

    override fun toString(): String = "ReadSessionStorageGrants(redacted)"
}

/** Listener configuration for dedicated read-session JWT verification. */
internal data class ReadSessionConfig(
    val profile: ReadSessionProfile,
    val reservedNamespaces: ReservedNamespaces,
    /** Whether a namespace name is a remote schema of this server: such a route relays a read session. */
    val relays: (String) -> Boolean = { false },
    /** Whether a namespace name is one this server serves as a Host: such a route admits a read session. */
    val serves: (String) -> Boolean = { false },
    /** The public listener: only [serves] selects the verifier. No local read, no relay, no local-only route. */
    val servedOnly: Boolean = false,
)

/** Parsed local-only route shape before method and entitlement checks. */
internal data class LocalOnlyPathMatch(
    val pattern: String,
    val catalog: String,
    val namespace: String?,
    val table: String?,
)

/** Verified route target for a dedicated session call. */
internal data class ReadSessionTarget(
    val catalog: String,
    val namespace: String? = null,
    val table: String? = null,
)
