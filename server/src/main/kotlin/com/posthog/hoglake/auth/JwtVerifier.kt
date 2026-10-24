package com.posthog.hoglake.auth

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.oshai.kotlinlogging.KotlinLogging
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.security.interfaces.ECPublicKey
import java.time.Duration
import java.util.concurrent.atomic.AtomicReference

/**
 * Where signing public keys come from.
 *
 * [refresh] is called AT MOST once per failed lookup, never on the happy path: a
 * key rotation has to be picked up by a running verifier within one request, but
 * a token naming an unknown kid must not turn every 401 into an outbound fetch —
 * that is a trivial way to make an unauthenticated request amplify into load on
 * our own JWKS endpoint or, worse, on an external one.
 */
interface SigningKeys {
    /** The key named [keyId], or null when this source does not have it. */
    fun byKeyId(keyId: String?): ECPublicKey?

    /** Re-read the source. Called after a miss; failures are the caller's problem. */
    fun refresh()
}

/**
 * A fixed key set. One key, usually: the shape tests and single-key deployments
 * need, and the seam the rotation tests drive.
 */
class StaticSigningKeys(
    private val keyId: String,
    private val key: ECPublicKey,
) : SigningKeys {
    override fun byKeyId(keyId: String?): ECPublicKey? = if (keyId == null || keyId == this.keyId) key else null

    override fun refresh() = Unit
}

/**
 * A JWKS document, either a local file or a URL.
 *
 * A FILE source is what the in-cluster deployment uses: a projected secret or a
 * ConfigMap holding the issuer's public JWKS, refreshed by the kubelet, re-read
 * on a miss. A URL source exists for the external-issuer case (a customer or a
 * sister service publishing its own JWKS). Both are read the same way — the
 * bytes go through [parseJwks] — so there is one JWK decoder, not two.
 */
class JwksSigningKeys private constructor(
    private val fetch: () -> ByteArray,
    private val describe: String,
) : SigningKeys {
    private val log = KotlinLogging.logger {}
    private val keys = AtomicReference<Map<String, ECPublicKey>>(emptyMap())

    override fun byKeyId(keyId: String?): ECPublicKey? {
        if (keyId == null) return null
        return keys.get()[keyId]
    }

    override fun refresh() {
        try {
            val parsed = parseJwks(fetch())
            keys.set(parsed)
        } catch (e: Exception) {
            // A failed refresh KEEPS the previous set. Dropping every key because
            // a ConfigMap was mid-write would 401 the whole fleet; keeping a
            // retired key one extra rotation window is the cheaper failure, and
            // it is bounded by the token TTL anyway.
            log.warn(e) { "could not refresh signing keys from $describe; keeping the previous set" }
        }
    }

    companion object {
        fun fromFile(path: Path): JwksSigningKeys =
            JwksSigningKeys(
                fetch = { Files.readAllBytes(path) },
                describe = path.toString(),
            ).also { it.refresh() }

        fun fromUrl(
            url: URI,
            http: HttpClient = defaultHttpClient(),
        ): JwksSigningKeys {
            require(url.scheme == "https" || url.scheme == "http") { "jwks url must be http(s), got $url" }
            return JwksSigningKeys(
                fetch = {
                    val request =
                        HttpRequest.newBuilder(url)
                            .timeout(Duration.ofSeconds(5))
                            .header("Accept", "application/json")
                            .GET()
                            .build()
                    http.send(request, HttpResponse.BodyHandlers.ofByteArray()).body()
                },
                describe = url.toString(),
            ).also { it.refresh() }
        }

        private fun defaultHttpClient(): HttpClient =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()

        /**
         * Parse a JWKS document into kid -> key.
         *
         * A key with no `kid` is REFUSED rather than keyed by position. A
         * positional key set makes rotation ambiguous in the worst possible way:
         * during a two-key overlap the same kid would mean different keys before
         * and after a reload, so a token's validity would depend on when it
         * arrived.
         */
        fun parseJwks(bytes: ByteArray): Map<String, ECPublicKey> {
            val root = ObjectMapper().readTree(bytes)
            val keys =
                root.get("keys")
                    ?: throw IllegalArgumentException("jwks document has no 'keys' array")
            if (!keys.isArray) throw IllegalArgumentException("jwks 'keys' is not an array")
            val out = LinkedHashMap<String, ECPublicKey>()
            keys.forEach { jwk ->
                val kid =
                    jwk.get("kid")?.takeIf { it.isTextual }?.asText()
                        ?: throw IllegalArgumentException("jwks entry has no 'kid'")
                out[kid] = Jwt.ecPublicKey(jwk)
            }
            return out
        }
    }
}

/**
 * Verify a compact JWS into a [Caller].
 *
 * Stateless by construction: it reads the token, the key set, and the clock, and
 * nothing else. That is deliberate — it runs on every request, so a database read
 * here would put the auth check in front of the connection pool on the hot path.
 * The two things that genuinely need server-side state (revocation epochs and
 * client enablement) are checked once per TOKEN by the issuance layer, and the
 * residual window is bounded by the token TTL.
 */
class JwtVerifier(
    private val issuer: String,
    private val audience: String,
    private val keys: SigningKeys,
    private val skewSeconds: Long = 60,
    private val clock: () -> Long = { System.currentTimeMillis() / 1000 },
) {
    init {
        require(issuer.isNotBlank()) { "auth issuer must be configured when auth is not off" }
        require(audience.isNotBlank()) { "auth audience must be configured when auth is not off" }
        require(skewSeconds in 0..600) { "auth clock skew must be 0..600s, got $skewSeconds" }
    }

    /**
     * Verify [token] and return the caller it asserts.
     *
     * Order matters for the error vocabulary a client sees: shape, then
     * signature, then claims. Claims are never trusted before the signature
     * verifies, so a `cat` claim cannot be read out of an unsigned token — which
     * is the whole class of bug this ordering prevents.
     */
    fun verify(token: String): Caller {
        val jws = Jwt.parse(token)

        val key =
            keys.byKeyId(jws.keyId) ?: run {
                // One refresh per miss, then give up: a rotation lands here, a bogus
                // kid does not amplify.
                keys.refresh()
                keys.byKeyId(jws.keyId)
            } ?: throw JwtViolation(
                JwtViolation.UNKNOWN_KEY,
                "no signing key for kid '${jws.keyId}'",
            )

        Jwt.verifySignature(jws, key)

        val claims = jws.claims
        val now = clock()
        validateTimes(claims, now)
        validateIssuerAndAudience(claims)

        return toCaller(claims)
    }

    private fun validateTimes(
        claims: JsonNode,
        now: Long,
    ) {
        val exp = Jwt.seconds(claims, "exp")
        if (now > exp + skewSeconds) {
            throw JwtViolation(JwtViolation.EXPIRED, "token expired ${now - exp}s ago")
        }
        // `nbf` is optional (RFC 7519 §4.1.5); its absence means "valid now".
        val nbf = claims.get("nbf")?.takeIf { it.isIntegralNumber }?.asLong()
        if (nbf != null && now + skewSeconds < nbf) {
            throw JwtViolation(JwtViolation.NOT_YET_VALID, "token is not valid for another ${nbf - now}s")
        }
        // A token issued far in the future is not a clock-skew artifact; it is a
        // forged or mis-provisioned credential, and accepting it would let a
        // 10-minute token be re-dated to outlive its TTL.
        val iat = claims.get("iat")?.takeIf { it.isIntegralNumber }?.asLong()
        if (iat != null && iat > now + skewSeconds) {
            throw JwtViolation(JwtViolation.NOT_YET_VALID, "token was issued ${iat - now}s in the future")
        }
    }

    private fun validateIssuerAndAudience(claims: JsonNode) {
        val iss = Jwt.text(claims, "iss")
        if (iss != issuer) {
            throw JwtViolation(JwtViolation.WRONG_ISSUER, "iss '$iss' is not this instance's issuer")
        }
        // `aud` may be a string or an array of strings (RFC 7519 §4.1.3); either
        // spelling has to be accepted or a standards-conforming issuer we point
        // at would be refused.
        val aud = claims.get("aud") ?: throw JwtViolation(JwtViolation.MISSING_CLAIM, "claim 'aud' is missing")
        val audiences =
            when {
                aud.isTextual -> listOf(aud.asText())
                aud.isArray -> aud.map { it.takeIf(JsonNode::isTextual)?.asText() }.filterNotNull()
                else -> throw JwtViolation(JwtViolation.MALFORMED, "claim 'aud' is neither a string nor an array")
            }
        if (audience !in audiences) {
            throw JwtViolation(JwtViolation.WRONG_AUDIENCE, "aud $audiences does not contain '$audience'")
        }
    }

    private fun toCaller(claims: JsonNode): Caller {
        val subject = Jwt.text(claims, "sub")
        val tokenId = Jwt.text(claims, "jti")
        val kindText = Jwt.text(claims, "ten")
        val clientName = Jwt.textOrNull(claims, "cli")

        return when (kindText) {
            "operator" ->
                Caller(
                    subject = subject,
                    orgId = null,
                    kind = Caller.Kind.OPERATOR,
                    catalogScopes = emptyMap(),
                    tokenId = tokenId,
                    clientName = clientName,
                )

            "tenant" -> {
                val orgId = Jwt.text(claims, "org")
                val cat =
                    claims.get("cat")
                        ?: throw JwtViolation(JwtViolation.MISSING_CLAIM, "a tenant token must carry 'cat'")
                if (!cat.isObject) {
                    throw JwtViolation(JwtViolation.MALFORMED, "claim 'cat' is not an object")
                }
                val scopes = LinkedHashMap<String, Set<Scope>>()
                cat.fields().forEach { (catalog, value) ->
                    if (!value.isArray) {
                        throw JwtViolation(JwtViolation.MALFORMED, "cat['$catalog'] is not an array")
                    }
                    val names = value.mapNotNull { it.takeIf(JsonNode::isTextual)?.asText() }
                    val parsed = Scope.parseAll(names)
                    // A catalog entry naming no scope we recognise is dropped, not
                    // carried as an empty grant: present-but-empty and absent must
                    // not be two spellings of the same thing, because `knowsCatalog`
                    // reads presence.
                    if (parsed.isNotEmpty()) scopes[catalog] = parsed
                }
                if (scopes.isEmpty()) {
                    throw JwtViolation(JwtViolation.MISSING_CLAIM, "a tenant token must grant at least one catalog")
                }
                Caller(
                    subject = subject,
                    orgId = orgId,
                    kind = Caller.Kind.TENANT,
                    catalogScopes = scopes,
                    tokenId = tokenId,
                    clientName = clientName,
                )
            }

            else -> throw JwtViolation(
                JwtViolation.MALFORMED,
                "claim 'ten' must be 'tenant' or 'operator', got '$kindText'",
            )
        }
    }
}
