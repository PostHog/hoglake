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
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * Where signing public keys come from.
 *
 * THREE READ ENTRY POINTS, and the split is the security property.
 *
 * [load] is the BOOT path: it runs once, before the first request, it is
 * allowed to block, and it is STRICT. A source that cannot be read, or that
 * holds no keys, fails boot. The alternative is what the first cut did: boot a
 * healthy-looking server holding zero keys that 401s every request, with one
 * `warn` line as the only evidence.
 *
 * [refresh] is the ROTATION path: a re-read that keeps the previous set on any
 * failure, because a ConfigMap caught mid-write must not 401 the fleet.
 *
 * [refreshInBackground] is the REQUEST path. It must not block, because the
 * caller is an attacker-controlled request and the work is a network round trip
 * to a URL the attacker does not control but can absolutely queue up behind. A
 * token naming an unknown `kid` used to trigger a synchronous re-read, i.e. one
 * outbound fetch per unauthenticated request, on the Netty event loop — the
 * "amplify a 401 into load on our own JWKS endpoint" failure the previous
 * comment described and then permitted.
 *
 * Implementations that need no refresh ([StaticSigningKeys]) inherit a no-op.
 */
interface SigningKeys {
    /** The key named [keyId], or null when this source does not have it. */
    fun byKeyId(keyId: String?): ECPublicKey?

    /** How many keys the source holds now; exported as `hoglake_auth_signing_keys`. */
    val keyCount: Int

    /**
     * Read the source synchronously and THROW if it cannot be read or holds no
     * keys. BOOT ONLY — never call this from a request.
     */
    fun load() = refresh()

    /** Re-read the source synchronously, keeping the previous set on failure. */
    fun refresh()

    /**
     * Ask for a re-read without waiting for it.
     *
     * Returns immediately. A source that cannot refresh asynchronously inherits
     * a no-op, which fails closed: the caller's lookup already missed, so the
     * request is refused with `unknown_key` either way.
     */
    fun refreshInBackground() = Unit
}

/**
 * A fixed key set. One key, usually: the shape tests and single-key deployments
 * need, and the seam the rotation tests drive.
 */
class StaticSigningKeys(
    private val keyId: String,
    private val key: ECPublicKey,
) : SigningKeys {
    // A token with no `kid` names no key, even in a one-key deployment. Saying
    // otherwise here would be the verifier's only path to accepting a token the
    // issuer never labelled.
    override fun byKeyId(keyId: String?): ECPublicKey? = if (keyId == this.keyId) key else null

    override val keyCount: Int = 1

    override fun refresh() = Unit
}

/**
 * A JWKS document, either a local file or a URL.
 *
 * A FILE source is what the in-cluster deployment uses: a projected secret or a
 * ConfigMap holding the issuer's public JWKS, refreshed by the kubelet and re-read
 * here on a miss. A URL source exists for the external-issuer case (a customer or
 * a sister service publishing its own JWKS). Both are read the same way — the
 * bytes go through [parseJwks] — so there is one JWK decoder, not two.
 *
 * RE-READS ARE RATE LIMITED AND ASYNCHRONOUS; see [refreshInBackground]. The miss
 * that triggers one is an unauthenticated request, so the rate limit is a security
 * property rather than a courtesy to the issuing service.
 */
class JwksSigningKeys internal constructor(
    private val fetch: () -> ByteArray,
    private val describe: String,
    private val minRefreshIntervalMs: Long = DEFAULT_MIN_REFRESH_INTERVAL_MS,
    private val clock: () -> Long = System::currentTimeMillis,
    private val runAsync: (() -> Unit) -> Unit = ::runOnRefreshThread,
) : SigningKeys {
    private val log = KotlinLogging.logger {}
    private val keys = AtomicReference<Map<String, ECPublicKey>>(emptyMap())

    /** When the last background re-read was REQUESTED, for [minRefreshIntervalMs]. */
    private val lastRefreshRequestAt = AtomicLong(0)

    override fun byKeyId(keyId: String?): ECPublicKey? {
        if (keyId == null) return null
        return keys.get()[keyId]
    }

    override val keyCount: Int
        get() = keys.get().size

    /**
     * The strict boot read. Any failure — unreadable file, unreachable URL,
     * unparseable document, or `{"keys":[]}` — propagates and fails boot, naming
     * the source. Every one of those would otherwise be a server that refuses
     * every token while its probes report healthy.
     */
    override fun load() {
        val parsed =
            try {
                parseJwks(fetch())
            } catch (e: Exception) {
                throw IllegalStateException("could not load signing keys from $describe: ${e.message}", e)
            }
        check(parsed.isNotEmpty()) {
            "signing-key source $describe holds no keys; every token would be refused"
        }
        keys.set(parsed)
    }

    /**
     * Ask for a re-read, at most once per [minRefreshIntervalMs], off the caller's
     * thread.
     *
     * The rate limit is the whole defence: without it any unauthenticated caller
     * can turn a 401 into an outbound fetch, and one such fetch per request is a
     * load amplifier against our own JWKS endpoint (or a customer's). With it, the
     * maximum rate is one fetch per interval no matter how many requests miss.
     *
     * The compare-and-set is what makes concurrent misses collapse: the first
     * caller to move [lastRefreshRequestAt] forward owns the window and the rest
     * return immediately. The request that triggered it still fails with
     * `unknown_key`, which a client answers by re-minting — the correct response
     * to a key it does not yet know about, and far cheaper than making every
     * caller wait on a network round trip.
     */
    override fun refreshInBackground() {
        val now = clock()
        val last = lastRefreshRequestAt.get()
        if (last != 0L && now - last < minRefreshIntervalMs) return
        if (!lastRefreshRequestAt.compareAndSet(last, now)) return
        runAsync { refresh() }
    }

    override fun refresh() {
        try {
            val parsed = parseJwks(fetch())
            // An EMPTY document is a failed read, not a rotation: no issuer
            // retires its last key on purpose, and taking it at its word would
            // 401 the fleet exactly as a missing file would.
            if (parsed.isEmpty()) {
                log.warn { "signing-key source $describe is empty; keeping the previous set" }
                return
            }
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
        /**
         * One re-read per 30s per instance.
         *
         * Chosen for the failure it bounds rather than for latency: a rotation
         * still lands within a request or two, while an attacker's miss rate stops
         * mattering entirely. Every access token lives 15 minutes, so a 30s blind
         * spot costs at most a couple of refused requests during a rotation — and
         * the client's answer to `unknown_key` is to re-mint anyway.
         */
        const val DEFAULT_MIN_REFRESH_INTERVAL_MS: Long = 30_000

        /**
         * The daemon thread background re-reads run on.
         *
         * Single-threaded and daemon: one re-read at a time (the compare-and-set
         * already collapses callers; this bounds a slow source to one in flight),
         * and no thread preventing JVM exit in a test or a `main` that returns.
         */
        private val refreshThread: ExecutorService by lazy {
            Executors.newSingleThreadExecutor { r ->
                Thread(r, "hoglake-jwks-refresh").apply { isDaemon = true }
            }
        }

        private fun runOnRefreshThread(work: () -> Unit) {
            // A rejected submission (shutdown in progress) is not worth failing a
            // request over: the caller was going to get `unknown_key` regardless.
            runCatching { refreshThread.execute(work) }
        }

        fun fromFile(
            path: Path,
            minRefreshIntervalMs: Long = DEFAULT_MIN_REFRESH_INTERVAL_MS,
        ): JwksSigningKeys =
            JwksSigningKeys(
                fetch = { Files.readAllBytes(path) },
                describe = path.toString(),
                minRefreshIntervalMs = minRefreshIntervalMs,
            ).also { it.load() }

        fun fromUrl(
            url: URI,
            http: HttpClient = defaultHttpClient(),
            minRefreshIntervalMs: Long = DEFAULT_MIN_REFRESH_INTERVAL_MS,
        ): JwksSigningKeys {
            require(url.scheme == "https" || url.scheme == "http") { "jwks url must be http(s), got $url" }
            return JwksSigningKeys(
                minRefreshIntervalMs = minRefreshIntervalMs,
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
            ).also { it.load() }
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

        // No `kid` at all can never resolve, whatever the key set contains, so it
        // is refused WITHOUT asking for a re-read. That matters: a header missing
        // its `kid` is the cheapest thing an attacker can send, and it used to be
        // the cheapest way to trigger one.
        val keyId =
            jws.keyId ?: throw JwtViolation(
                JwtViolation.UNKNOWN_KEY,
                "token header has no kid, so it can name no key",
            )

        // A miss asks for a re-read and refuses THIS request. It does not wait: a
        // rotation costs one refused request, an attacker's miss rate costs one
        // fetch per JwksSigningKeys.DEFAULT_MIN_REFRESH_INTERVAL_MS.
        val key =
            keys.byKeyId(keyId) ?: run {
                keys.refreshInBackground()
                keys.byKeyId(keyId)
            } ?: throw JwtViolation(
                JwtViolation.UNKNOWN_KEY,
                "no signing key for kid '$keyId'",
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
