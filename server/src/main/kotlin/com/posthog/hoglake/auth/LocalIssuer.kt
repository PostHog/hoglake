package com.posthog.hoglake.auth

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import java.util.UUID

/**
 * Mints hoglake's token contract (docs/auth.md) with a [LocalSigningKey]:
 * the issuer behind `hoglake token mint`, and nothing else.
 *
 * An issuer like any other. It emits exactly the claims the contract vectors
 * pin (`server/src/test/resources/oauth/contract-vectors.json`) and the
 * conformance test runs every issuer-input vector through [claims], so the
 * local issuer cannot drift from what the verifier next door accepts. It is
 * deliberately not an OAuth server: no clients, no endpoint, no request path —
 * a shell command signs a token and an operator hands it to a client as a
 * fixed token.
 */
class LocalIssuer(
    private val key: LocalSigningKey,
    private val issuer: String,
    private val audience: String = DEFAULT_AUDIENCE,
    private val clock: () -> Long = { System.currentTimeMillis() / 1000 },
    private val tokenId: () -> String = { UUID.randomUUID().toString() },
) {
    /** Where a routed catalog lives: the `rtg` claim's input. */
    data class Placement(
        val backendType: String,
        val backendId: String,
        val tenantId: String,
        val incarnation: Long,
    )

    /** A scoped token's narrowing: one predicate field and the values held, each at `read` or `write`. */
    data class Predicate(
        val field: String,
        val values: Map<String, String>,
    )

    /** A tenant token, or a scoped one when [predicate] is given. */
    fun tenant(
        subject: String,
        org: String,
        catalogs: Map<String, Scope>,
        clientName: String,
        ttlSeconds: Long,
        delegates: Boolean = false,
        predicate: Predicate? = null,
        user: String? = null,
        placement: Placement? = null,
        audience: String = this.audience,
    ): String =
        sign(
            tenantClaims(
                subject,
                org,
                catalogs,
                clientName,
                ttlSeconds,
                delegates,
                predicate,
                user,
                placement,
                audience,
                clock(),
                tokenId(),
            ),
        )

    /** An operator token: fleet-wide, or bound to one catalog when [catalog] and [org] are given. */
    fun operator(
        subject: String,
        clientName: String,
        ttlSeconds: Long,
        org: String? = null,
        catalog: String? = null,
        placement: Placement? = null,
        audience: String = this.audience,
    ): String =
        sign(
            operatorClaims(subject, clientName, ttlSeconds, org, catalog, placement, audience, clock(), tokenId()),
        )

    /** The claims a tenant or scoped token carries, in the order the token carries them. */
    fun tenantClaims(
        subject: String,
        org: String,
        catalogs: Map<String, Scope>,
        clientName: String,
        ttlSeconds: Long,
        delegates: Boolean,
        predicate: Predicate?,
        user: String?,
        placement: Placement?,
        audience: String,
        issuedAt: Long,
        jti: String,
    ): ObjectNode {
        require(catalogs.isNotEmpty()) { "a tenant token names at least one catalog" }
        require(ttlSeconds > 0) { "ttl must be positive" }
        if (predicate != null) {
            require(catalogs.values.all { it == Scope.READ || it == Scope.WRITE }) {
                "a scoped token's catalog scopes are read or write only"
            }
            require(predicate.values.isNotEmpty()) { "a scoped token holds at least one predicate value" }
            require(predicate.values.values.all { it == "read" || it == "write" }) {
                "a predicate value is held at read or write"
            }
        }
        val claims = mapper.createObjectNode()
        claims.put("iss", issuer)
        claims.set<JsonNode>("aud", aud(audience, routed = placement?.backendType == "hoglite"))
        claims.put("sub", subject)
        claims.put("jti", jti)
        claims.put("ten", if (predicate == null) "tenant" else "scoped")
        claims.put("org", org)
        claims.set<JsonNode>("cat", cat(catalogs))
        claims.put("cli", clientName)
        if (delegates) claims.put("dlg", true)
        if (predicate != null) {
            claims.putObject("prd").putObject(predicate.field).apply {
                predicate.values.forEach { (value, access) -> put(value, access) }
            }
        }
        if (user != null) claims.put("usr", user)
        if (placement != null) claims.set<JsonNode>("rtg", rtg(placement))
        times(claims, issuedAt, ttlSeconds)
        return claims
    }

    /** The claims an operator token carries, in the order the token carries them. */
    fun operatorClaims(
        subject: String,
        clientName: String,
        ttlSeconds: Long,
        org: String?,
        catalog: String?,
        placement: Placement?,
        audience: String,
        issuedAt: Long,
        jti: String,
    ): ObjectNode {
        require(ttlSeconds > 0) { "ttl must be positive" }
        require(
            (catalog == null) == (org == null),
        ) { "a catalog-bound operator token names its catalog and its org together" }
        require(placement == null || catalog != null) { "a placement needs the catalog it places" }
        val claims = mapper.createObjectNode()
        claims.put("iss", issuer)
        // A fleet operator reaches every backend, so its audience names the
        // hoglite backends too; a catalog-bound one follows its placement.
        claims.set<JsonNode>("aud", aud(audience, routed = placement?.backendType == "hoglite" || placement == null))
        claims.put("sub", subject)
        claims.put("jti", jti)
        claims.put("ten", "operator")
        if (org != null) claims.put("org", org)
        if (catalog != null) claims.set<JsonNode>("cat", cat(mapOf(catalog to Scope.ADMIN)))
        claims.put("cli", clientName)
        if (placement != null) claims.set<JsonNode>("rtg", rtg(placement))
        times(claims, issuedAt, ttlSeconds)
        return claims
    }

    /** Sign [claims] under this key: `alg: ES256`, this key's `kid`, `typ: JWT`. */
    fun sign(claims: ObjectNode): String {
        val header = mapper.createObjectNode().put("alg", Jwt.ES256).put("kid", key.keyId).put("typ", "JWT")
        return key.sign(header, claims)
    }

    private fun aud(
        audience: String,
        routed: Boolean,
    ): JsonNode =
        if (routed && audience == DEFAULT_AUDIENCE) {
            mapper.createArrayNode().add(DEFAULT_AUDIENCE).add(HOGLITE_AUDIENCE)
        } else {
            mapper.valueToTree(audience)
        }

    private fun cat(catalogs: Map<String, Scope>): ObjectNode =
        mapper.createObjectNode().apply {
            catalogs.forEach { (name, scope) -> putArray(name).add(scope.wire) }
        }

    private fun rtg(p: Placement): ObjectNode =
        mapper.createObjectNode().apply {
            put("b", p.backendType)
            put("be", p.backendId)
            put("t", "${p.tenantId}_${p.incarnation}")
        }

    private fun times(
        claims: ObjectNode,
        issuedAt: Long,
        ttlSeconds: Long,
    ) {
        claims.put("exp", issuedAt + ttlSeconds)
        claims.put("iat", issuedAt)
        claims.put("nbf", issuedAt)
    }

    companion object {
        const val DEFAULT_AUDIENCE = "hoglake"

        /** The audience a backend that verifies the token itself requires beside hoglake's (docs/auth.md). */
        const val HOGLITE_AUDIENCE = "hoglite"

        /** `HOGLAKE_AUTH_ISSUER` under `local` when the operator sets none. */
        const val DEFAULT_LOCAL_ISSUER = "hoglake"

        private val mapper = ObjectMapper()
    }
}
