package com.posthog.hoglake.auth

import com.fasterxml.jackson.databind.JsonNode
import com.posthog.hoglake.service.TeamNamespaces
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
    ) {
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
        requireSingleRead(claims.get("cat"), primary)
        requireSingleRead(claims.get("remote_schemas"), TeamNamespaces.FAMILY)
        val predicates = claims.get("prd")
        val teams = predicates?.get("team_id")
        if (predicates?.isObject != true || predicates.size() != 1 || teams?.isObject != true || teams.size() != 1) {
            invalid()
        }
        val team = teams.properties().single()
        if (!POSITIVE_INTEGER.matches(team.key) || team.key.toLongOrNull() == null ||
            team.value.textValue() != "read"
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
        validateGrants(claims.get("grants"), primary, backend, tenant, region)
    }

    private fun validateGrants(
        grants: JsonNode?,
        catalog: String,
        backend: String,
        tenant: String,
        region: String,
    ) {
        val targets = remoteTargets + catalog + backend
        if (targets.size != 4 || grants?.isArray != true || grants.size() < targets.size) invalid()
        var hash = 0x811c9dc5u
        for (character in tenant) hash = (hash xor character.code.toUInt()) * 0x01000193u
        val shard = (hash and 255u).toString(16).padStart(2, '0')
        val manifestSuffix = "/$shard/t/$tenant/manifest"
        val seen = HashSet<String>()
        var previousBucket = ""
        var previousKey = ""
        for (grant in grants) {
            if (!grant.isObject || grant.size() != 6 || grant.has("object_key") == grant.has("prefix")) invalid()
            val bucket = text(grant, "bucket")
            val prefix = grant.has("prefix")
            val key = text(grant, if (prefix) "prefix" else "object_key")
            val target = text(grant, "target")
            val actions = grant.get("actions")
            if (!BUCKET.matches(bucket) || ".." in bucket || key.length > 1024 ||
                prefix != key.endsWith('/') ||
                key.removeSuffix("/").split('/').any { it == "." || it == ".." || !SEGMENT.matches(it) } ||
                target !in targets || text(grant, "catalog") != catalog || text(grant, "region") != region ||
                actions?.isArray != true || actions.size() != 1 || actions[0].textValue() != "s3:GetObject"
            ) {
                invalid()
            }
            if (target == backend && (prefix || !key.endsWith(manifestSuffix) || target in seen)) invalid()
            val overlaps = previousKey.endsWith('/') && key.startsWith(previousKey)
            if (bucket < previousBucket || (bucket == previousBucket && (key <= previousKey || overlaps))) {
                invalid()
            }
            previousBucket = bucket
            previousKey = key
            seen.add(target)
        }
        if (seen != targets) invalid()
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
        private val POSITIVE_INTEGER = Regex("[1-9][0-9]*")
        private val ROUTED_TENANT = Regex("[A-Za-z0-9][A-Za-z0-9._-]{0,125}_[1-9][0-9]*")
        private val BUCKET = Regex("[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]")
        private val SEGMENT = Regex("[A-Za-z0-9_.=-]+")
    }
}
