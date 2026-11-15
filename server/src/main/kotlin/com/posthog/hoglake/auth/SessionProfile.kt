package com.posthog.hoglake.auth

import com.fasterxml.jackson.databind.JsonNode
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.service.Identifiers

/**
 * The session profile: a token that carries storage credentials for the
 * caller's own use beside its metadata grants (docs/auth.md, "Session profile
 * with embedded storage credentials").
 *
 * hoglake never uses those credentials. It validates the profile's shape so
 * that a backend verifying the same bytes independently sees the token hoglake
 * saw, records the read-only remote schemas the token authorizes, and forwards
 * the bytes unchanged. The credentials are never read into a [Caller], never
 * logged, and never echoed in a refusal: every violation below names a claim,
 * never a value.
 *
 * Gated by `HOGLAKE_AUTH_SESSION_PROFILE`. OFF, a token that carries the
 * marker or a credential object is REFUSED with [JwtViolation.PROFILE_NOT_ENABLED]
 * rather than read as a plain metadata token: silently accepting it would mean
 * an older hoglake grants on a token whose other half it does not understand,
 * the same widening the strict `prd` rule exists to prevent.
 */
internal object SessionProfile {
    /** The one profile id hoglake verifies; an issuer that needs another changes the contract first. */
    const val ID = "storage-session"
    const val VERSION = 1L
    const val PURPOSE = "user-session"

    /** The most remote schemas one token may name. */
    const val MAX_REMOTE_SCHEMAS = 16

    private val CREDENTIAL_FIELDS = listOf("access_key_id", "secret_access_key", "session_token", "region")
    private val SCHEMA_NAME = Regex(Identifiers.PATTERN)

    /** What a verified profile contributes to the caller: the read-only remote schemas it may use. */
    class Grant(
        val remoteSchemas: Set<String>,
    )

    /** Does [claims] carry any of the profile's claims? Any one of them selects this parser. */
    fun carried(claims: JsonNode): Boolean =
        claims.has("profile") || claims.has("aws") || claims.has("primary_catalog") || claims.has("remote_schemas")

    /**
     * The gate, run BEFORE the ordinary contract's rules: a token carrying the
     * profile where it is off is refused as [JwtViolation.PROFILE_NOT_ENABLED],
     * and no other rule gets to name a different reason first. The reason and
     * the metric are what tell an operator a rollout gap from a bad issuer.
     */
    fun refuseIfDisabled(
        claims: JsonNode,
        enabled: Boolean,
    ) {
        if (enabled || !carried(claims)) return
        Metrics.authSessionProfile("refused_disabled")
        throw JwtViolation(
            JwtViolation.PROFILE_NOT_ENABLED,
            "this instance does not accept the session profile (HOGLAKE_AUTH_SESSION_PROFILE=off)",
        )
    }

    /**
     * Validate the profile claims of [claims], or return null when the token
     * carries none. [enabled] is the gate ([refuseIfDisabled] has normally run
     * already; it is re-checked here so no path can skip it); [kindText] is
     * `ten` as spelled (null when absent).
     */
    fun validate(
        claims: JsonNode,
        enabled: Boolean,
        kindText: String?,
    ): Grant? {
        if (!carried(claims)) return null
        refuseIfDisabled(claims, enabled)
        val marker = claims.get("profile")
        val credentials = claims.get("aws")

        fun malformed(detail: String): Nothing {
            Metrics.authSessionProfile("refused_invalid")
            throw JwtViolation(JwtViolation.MALFORMED, detail)
        }

        // The marker selects the parser. A credential object without it is a
        // token nobody declared the meaning of, and is refused as such.
        if (marker == null) {
            malformed(
                "a token carrying 'aws', 'primary_catalog' or 'remote_schemas' must carry 'profile'",
            )
        }
        if (!marker.isTextual || marker.asText() != ID) malformed("claim 'profile' must be '$ID'")
        val version = claims.get("version")
        if (version == null || !version.isIntegralNumber || version.asLong() != VERSION) {
            malformed("claim 'version' must be $VERSION for profile '$ID'")
        }
        val purpose = claims.get("purpose")
        if (purpose == null || !purpose.isTextual || purpose.asText() != PURPOSE) {
            malformed("claim 'purpose' must be '$PURPOSE' for profile '$ID'")
        }
        if (kindText == "operator") malformed("an operator token may not carry the session profile")

        // One primary catalog, and it is the single key of the RAW `cat`
        // object: not of the parsed grants, which drop a catalog whose scope
        // words the server does not know — a second `cat` key the issuer
        // meant something by must refuse, not vanish. The `scope` spelling
        // alone does not carry the profile.
        val primary = claims.get("primary_catalog")
        if (primary == null || !primary.isTextual) malformed("the session profile requires a string 'primary_catalog'")
        val cat = claims.get("cat")
        if (cat == null || !cat.isObject || cat.size() != 1) {
            malformed("the session profile requires 'cat' to be an object naming exactly one catalog")
        }
        if (cat.fieldNames().next() != primary.asText()) {
            malformed("'primary_catalog' must be the single catalog 'cat' names")
        }

        // Remote schemas are read-only by construction: any other word, or
        // an empty list, refuses the token rather than being dropped.
        val remote = LinkedHashSet<String>()
        claims.get("remote_schemas")?.let { node ->
            if (!node.isObject) malformed("claim 'remote_schemas' is not an object")
            if (node.size() > MAX_REMOTE_SCHEMAS) {
                malformed(
                    "'remote_schemas' names more than $MAX_REMOTE_SCHEMAS schemas",
                )
            }
            node.properties().forEach { (name, scopes) ->
                if (!SCHEMA_NAME.matches(name)) malformed("remote_schemas names an invalid schema")
                // Strict, like a scoped token's cat: a non-array, an empty
                // array, a non-string entry or any word but read refuses the
                // token. Nothing is dropped on the way to a grant.
                if (!scopes.isArray || scopes.isEmpty) malformed("remote_schemas['$name'] must be a non-empty array")
                scopes.forEach { word ->
                    if (!word.isTextual || word.asText() != Scope.READ.wire) {
                        malformed("remote_schemas['$name'] may grant only [\"read\"]")
                    }
                }
                remote.add(name)
            }
        }

        // The credential bundle: complete, and never outlived by the token.
        // Values are checked for presence and type only, and never copied out.
        if (credentials == null || !credentials.isObject) malformed("the session profile requires an object 'aws'")
        CREDENTIAL_FIELDS.forEach { field ->
            val value = credentials.get(field)
            if (value == null || !value.isTextual || value.asText().isEmpty()) {
                malformed("aws.$field must be a non-empty string")
            }
        }
        val expiresAt = credentials.get("expires_at")
        if (expiresAt == null || !expiresAt.isIntegralNumber) malformed("aws.expires_at must be integer seconds")
        val exp = Jwt.seconds(claims, "exp")
        if (expiresAt.asLong() < exp) malformed("'exp' must not be later than aws.expires_at")
        claims.get("session_deadline")?.let { deadline ->
            if (!deadline.isIntegralNumber) malformed("claim 'session_deadline' must be integer seconds")
            if (deadline.asLong() < expiresAt.asLong()) {
                malformed(
                    "aws.expires_at must not be later than 'session_deadline'",
                )
            }
        }
        claims.get("sid")?.let {
            if (!it.isTextual || it.asText().isEmpty()) malformed("claim 'sid' must be a non-empty string")
        }
        claims.get("generation")?.let {
            if (!it.isIntegralNumber || it.asLong() < 1) malformed("claim 'generation' must be a positive integer")
        }
        claims.get("grants")?.let { if (!it.isArray) malformed("claim 'grants' must be an array") }

        Metrics.authSessionProfile("accepted")
        return Grant(remote)
    }
}
