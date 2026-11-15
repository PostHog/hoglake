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

    /**
     * Validate the profile claims of [claims], or return null when the token
     * carries none. [enabled] is the gate; [kindText] is `ten` as spelled (null
     * when absent); [catalogs] is the token's `cat`, already parsed.
     */
    fun validate(
        claims: JsonNode,
        enabled: Boolean,
        kindText: String?,
        catalogs: Map<String, Set<Scope>>,
    ): Grant? {
        val marker = claims.get("profile")
        val credentials = claims.get("aws")
        if (marker == null && credentials == null && !claims.has("primary_catalog") && !claims.has("remote_schemas")) {
            return null
        }
        if (!enabled) {
            Metrics.authSessionProfile("refused_disabled")
            throw JwtViolation(
                JwtViolation.PROFILE_NOT_ENABLED,
                "this instance does not accept the session profile (HOGLAKE_AUTH_SESSION_PROFILE=off)",
            )
        }

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

        // One primary catalog, and it is the one the metadata grants name.
        val primary = claims.get("primary_catalog")
        if (primary == null || !primary.isTextual) malformed("the session profile requires a string 'primary_catalog'")
        if (catalogs.keys != setOf(primary.asText())) {
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
                val words = scopes.takeIf { it.isArray }?.mapNotNull { it.takeIf(JsonNode::isTextual)?.asText() }
                if (words == null || words.isEmpty() || words.any { it != Scope.READ.wire }) {
                    malformed("remote_schemas['$name'] may grant only [\"read\"]")
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
