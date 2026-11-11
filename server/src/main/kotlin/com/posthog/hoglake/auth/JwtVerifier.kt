package com.posthog.hoglake.auth

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.service.Identifiers
import com.posthog.hoglake.store.BackendKind
import io.github.oshai.kotlinlogging.KotlinLogging
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.security.PublicKey
import java.time.Duration
import java.util.UUID
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
    fun byKeyId(keyId: String?): PublicKey?

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
    private val key: PublicKey,
) : SigningKeys {
    // A token with no `kid` names no key, even in a one-key deployment. Saying
    // otherwise here would be the verifier's only path to accepting a token the
    // issuer never labelled.
    override fun byKeyId(keyId: String?): PublicKey? = if (keyId == this.keyId) key else null

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
    private val backends: Map<UUID, BackendKind> = emptyMap(),
    private val clock: () -> Long = System::currentTimeMillis,
    private val runAsync: (() -> Unit) -> Unit = ::runOnRefreshThread,
) : SigningKeys {
    private val log = KotlinLogging.logger {}
    private val keys = AtomicReference<Map<String, PublicKey>>(emptyMap())

    /** When the last background re-read was REQUESTED, for [minRefreshIntervalMs]. */
    private val lastRefreshRequestAt = AtomicLong(0)

    override fun byKeyId(keyId: String?): PublicKey? {
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
         *
         * EC P-256 (ES256) and RSA (RS256) keys may be mixed in one document.
         * An entry that declares a purpose this verifier does not serve — an
         * encryption key, or a signing key for another algorithm — is skipped
         * ([Jwt.isUsableSigningJwk]), because standard identity providers
         * publish such keys alongside their signing keys.
         */
        fun parseJwks(bytes: ByteArray): Map<String, PublicKey> {
            val root = ObjectMapper().readTree(bytes)
            val keys =
                root.get("keys")
                    ?: throw IllegalArgumentException("jwks document has no 'keys' array")
            if (!keys.isArray) throw IllegalArgumentException("jwks 'keys' is not an array")
            val out = LinkedHashMap<String, PublicKey>()
            keys.forEach { jwk ->
                // Purpose first, then `kid`: an entry this verifier skips (an
                // encryption key, another algorithm) need not carry a kid at
                // all (RFC 7517 §4.5 makes it optional), and a standard JWKS
                // with such an entry must still load. A usable signing key
                // without one is unaddressable by any token and refused.
                if (!Jwt.isUsableSigningJwk(jwk)) return@forEach
                val kid =
                    jwk.get("kid")?.takeIf { it.isTextual }?.asText()
                        ?: throw IllegalArgumentException("jwks signing entry has no 'kid'")
                out[kid] = Jwt.publicKey(jwk)
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
    /**
     * The predicate fields this instance's auth source declares
     * (`HOGLAKE_AUTH_PREDICATE_FIELDS`). A scoped token must carry exactly
     * these in `prd`. Empty means scoped tokens are refused outright.
     */
    private val predicateFields: Set<String> = emptySet(),
    private val backends: Map<UUID, BackendKind> = emptyMap(),
    /**
     * The most a scoped token may live, `exp - iat`
     * (`HOGLAKE_AUTH_SCOPED_MAX_LIFETIME_SECONDS`).
     */
    private val scopedMaxLifetimeSeconds: Long = DEFAULT_SCOPED_MAX_LIFETIME_SECONDS,
    private val clock: () -> Long = { System.currentTimeMillis() / 1000 },
) {
    init {
        require(predicateFields.all { FIELD_NAME.matches(it) }) {
            "auth predicate fields must be identifiers, got $predicateFields"
        }
        require(issuer.isNotBlank()) { "auth issuer must be configured when auth is not off" }
        require(audience.isNotBlank()) { "auth audience must be configured when auth is not off" }
        require(skewSeconds in 0..600) { "auth clock skew must be 0..600s, got $skewSeconds" }
        require(scopedMaxLifetimeSeconds > 0) {
            "auth scoped max lifetime must be positive, got $scopedMaxLifetimeSeconds"
        }
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
        // 15-minute token be re-dated to outlive its TTL.
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
                aud.isArray ->
                    aud.map {
                        if (!it.isTextual) {
                            throw JwtViolation(
                                JwtViolation.MALFORMED,
                                "audience entries must be strings",
                            )
                        }
                        it.asText()
                    }
                else -> throw JwtViolation(JwtViolation.MALFORMED, "claim 'aud' is neither a string nor an array")
            }
        if (audience !in audiences) {
            throw JwtViolation(JwtViolation.WRONG_AUDIENCE, "aud $audiences does not contain '$audience'")
        }
    }

    private fun toCaller(claims: JsonNode): Caller {
        val subject = Jwt.text(claims, "sub")
        val tokenId = Jwt.text(claims, "jti")
        // An absent `ten` means `tenant`, so an issuer with one kind of token
        // need not spell it. Absent only: a `ten` that is present but not a
        // string is still refused, and a scoped or operator token must say so.
        val kindText = if (claims.has("ten")) Jwt.text(claims, "ten") else null
        val clientName = Jwt.textOrNull(claims, "cli")
        val routing = routing(claims)

        return when (kindText) {
            "operator" ->
                Caller(
                    subject = subject,
                    orgId = if (claims.has("rtg")) Jwt.textOrNull(claims, "org") else null,
                    kind = Caller.Kind.OPERATOR,
                    catalogScopes = emptyMap(),
                    tokenId = tokenId,
                    clientName = clientName,
                    routing = routing,
                )

            "scoped" -> scopedCaller(claims, subject, tokenId, clientName, routing)

            "tenant", null -> {
                // A token that carries a predicate but no `ten` is a scoped
                // token that forgot to say so. Reading it as a tenant token
                // would drop the predicate, i.e. widen it to the whole catalog.
                if (kindText == null && claims.has("prd")) {
                    throw JwtViolation(JwtViolation.MALFORMED, "a token carrying 'prd' must declare ten 'scoped'")
                }
                val orgId = Jwt.text(claims, "org")
                val scopes =
                    grants(
                        cat = claims.get("cat")?.let(::tenantCat),
                        scope = scopeClaim(claims, strict = false),
                        kind = "tenant",
                    )
                val delegates = delegates(claims)
                Caller(
                    subject = subject,
                    orgId = orgId,
                    kind = Caller.Kind.TENANT,
                    catalogScopes = scopes,
                    tokenId = tokenId,
                    clientName = clientName,
                    delegates = delegates,
                    routing = routing,
                )
            }

            else -> throw JwtViolation(
                JwtViolation.MALFORMED,
                "claim 'ten' must be 'tenant', 'scoped' or 'operator', got '$kindText'",
            )
        }
    }

    /**
     * A tenant token's `cat`, LENIENT about scope words: an unknown word is
     * dropped (see [Scope.parseAll]), and a catalog left with no scope we
     * recognise is dropped with it, not carried as an empty grant —
     * present-but-empty and absent must not be two spellings of the same thing,
     * because `knowsCatalog` reads presence.
     */
    private fun tenantCat(cat: JsonNode): Map<String, Set<Scope>> {
        if (!cat.isObject) {
            throw JwtViolation(JwtViolation.MALFORMED, "claim 'cat' is not an object")
        }
        val scopes = LinkedHashMap<String, Set<Scope>>()
        cat.properties().forEach { (catalog, value) ->
            if (!value.isArray) {
                throw JwtViolation(JwtViolation.MALFORMED, "cat['$catalog'] is not an array")
            }
            val parsed = Scope.parseAll(value.mapNotNull { it.takeIf(JsonNode::isTextual)?.asText() })
            if (parsed.isNotEmpty()) scopes[catalog] = parsed
        }
        return scopes
    }

    /** A scoped token's `cat`, STRICT: only `read` and `write`, never empty. */
    private fun scopedCat(cat: JsonNode): Map<String, Set<Scope>> {
        if (!cat.isObject) {
            throw JwtViolation(JwtViolation.MISSING_CLAIM, "a scoped token must carry an object 'cat'")
        }
        val scopes = LinkedHashMap<String, Set<Scope>>()
        cat.properties().forEach { (catalog, value) ->
            if (!value.isArray || value.isEmpty) {
                throw JwtViolation(JwtViolation.MALFORMED, "cat['$catalog'] is not a non-empty array")
            }
            scopes[catalog] =
                value.map { word ->
                    word.takeIf { it.isTextual }?.asText()?.let(::predicateScope)
                        ?: throw JwtViolation(
                            JwtViolation.MALFORMED,
                            "a scoped token's cat['$catalog'] may hold only 'read' and 'write'",
                        )
                }.toSet()
        }
        return scopes
    }

    /**
     * The catalog grants carried in the standard OAuth `scope` claim
     * (RFC 6749 §3.3, RFC 8693 §4.2): a space-separated string whose
     * `hoglake:<catalog>:<scope>` values are the same grants `cat` expresses.
     * This is the form an off-the-shelf identity provider can emit.
     *
     * Values not starting with `hoglake:` belong to other audiences sharing the
     * claim (`openid`, `profile`, …) and are ignored. Returns null when no
     * value is ours, so a token whose `scope` carries only foreign values is
     * treated as if it had no `scope` at all.
     *
     * A value in our namespace must be exactly three `:`-separated parts with a
     * valid catalog name (which can never contain `:`), or the token is
     * refused. The scope word follows `cat`'s rule for the token kind: dropped
     * when unknown on a tenant token ([strict] false), refused unless `read` or
     * `write` on a scoped one.
     */
    private fun scopeClaim(
        claims: JsonNode,
        strict: Boolean,
    ): Map<String, Set<Scope>>? {
        val claim = claims.get("scope") ?: return null
        if (!claim.isTextual) {
            throw JwtViolation(JwtViolation.MALFORMED, "claim 'scope' is not a space-separated string")
        }
        val grants = LinkedHashMap<String, MutableSet<Scope>>()
        var ours = false
        claim.asText().split(' ').filter { it.startsWith(SCOPE_PREFIX) }.forEach { value ->
            ours = true
            val parts = value.split(':')
            if (parts.size != 3 || !CATALOG_NAME.matches(parts[1])) {
                throw JwtViolation(
                    JwtViolation.MALFORMED,
                    "scope value '$value' is not hoglake:<catalog>:<scope> with a valid catalog name",
                )
            }
            val (_, catalog, word) = parts
            val scope =
                if (strict) {
                    predicateScope(word) ?: throw JwtViolation(
                        JwtViolation.MALFORMED,
                        "a scoped token's scope for '$catalog' may be only 'read' or 'write'",
                    )
                } else {
                    Scope.fromWire(word)
                }
            if (scope != null) grants.getOrPut(catalog) { LinkedHashSet() }.add(scope)
        }
        return if (ours) grants else null
    }

    /**
     * Reconcile the two spellings of a token's catalog grants.
     *
     * Either may be present alone. When BOTH are, they must describe the same
     * grants after each has applied its own parsing rules — a token whose `cat`
     * and `scope` disagree has no single meaning, and picking either one would
     * let an issuer bug widen access, so it is refused. Neither present is the
     * missing-`cat` refusal it has always been.
     */
    private fun grants(
        cat: Map<String, Set<Scope>>?,
        scope: Map<String, Set<Scope>>?,
        kind: String,
    ): Map<String, Set<Scope>> {
        val resolved =
            when {
                cat == null && scope == null ->
                    throw JwtViolation(JwtViolation.MISSING_CLAIM, "a $kind token must carry 'cat' or 'scope'")
                cat != null && scope != null && cat != scope ->
                    throw JwtViolation(
                        JwtViolation.MALFORMED,
                        "claims 'cat' and 'scope' grant different catalogs or scopes",
                    )
                else -> cat ?: scope!!
            }
        if (resolved.isEmpty()) {
            throw JwtViolation(JwtViolation.MISSING_CLAIM, "a $kind token must grant at least one catalog")
        }
        return resolved
    }

    /** A present placement must be understood in full before it can route a request. */
    private fun routing(claims: JsonNode): Routing? {
        val rtg = claims.get("rtg") ?: return null

        fun malformed(detail: String): Nothing = throw JwtViolation(JwtViolation.MALFORMED, detail)
        if (!rtg.isObject) malformed("claim 'rtg' must be an object")
        val backend =
            BackendKind.entries.firstOrNull { rtg.get("b")?.isTextual == true && it.wire == rtg.get("b").asText() }
                ?: malformed("rtg.b must be hoglite or postgres")
        val expected = if (backend == BackendKind.HOGLITE) setOf("b", "be", "t") else setOf("b", "be")
        if (rtg.properties().map { it.key }.toSet() != expected) malformed("rtg has invalid fields")
        val idText = rtg.get("be")?.takeIf { it.isTextual }?.asText() ?: malformed("rtg.be must be a UUID string")
        val id =
            try {
                UUID.fromString(idText)
            } catch (_: IllegalArgumentException) {
                malformed("rtg.be must be a canonical lowercase UUID")
            }
        if (id.toString() != idText) malformed("rtg.be must be a canonical lowercase UUID")
        val cat = claims.get("cat")
        if (cat == null || !cat.isObject || cat.size() != 1) malformed("rtg requires exactly one cat entry")
        val catalog = cat.fieldNames().next()
        val tenant =
            if (backend == BackendKind.HOGLITE) {
                val value = rtg.get("t")?.takeIf { it.isTextual }?.asText() ?: malformed("rtg.t must be a string")
                if (!Regex("[A-Za-z0-9._-]{1,128}").matches(value) || value.startsWith('.') || ".." in value) {
                    malformed("rtg.t is not a valid tenant id")
                }
                value
            } else {
                null
            }
        if (backends[id] != backend) throw JwtViolation(JwtViolation.UNKNOWN_BACKEND, "rtg backend is not configured")
        return Routing(catalog, backend, id, tenant)
    }

    /** Optional and strictly boolean: `"dlg": "false"` is a malformed token, not a truthy string. */
    private fun delegates(claims: JsonNode): Boolean =
        when (val dlg = claims.get("dlg")) {
            null -> false
            else ->
                if (dlg.isBoolean) {
                    dlg.asBoolean()
                } else {
                    throw JwtViolation(JwtViolation.MALFORMED, "claim 'dlg' is not a boolean")
                }
        }

    /**
     * A scoped token, parsed STRICTLY: anything this verifier does not
     * understand refuses the token rather than being ignored, so a narrowing
     * added to the contract later can never be silently dropped by an older
     * hoglake. That is the opposite of `cat`'s rule for unknown scope words,
     * and deliberately so: dropping an unknown scope loses authority, while
     * dropping an unknown predicate would GAIN it.
     */
    private fun scopedCaller(
        claims: JsonNode,
        subject: String,
        tokenId: String,
        clientName: String?,
        routing: Routing?,
    ): Caller {
        if (predicateFields.isEmpty()) {
            throw JwtViolation(JwtViolation.INVALID_PREDICATE, "this instance accepts no scoped tokens")
        }
        // A scoped token is short-lived by contract: its predicate is a
        // snapshot of the holder's access, and exp - iat bounds how stale it gets.
        val iat =
            claims.get("iat")?.takeIf { it.isIntegralNumber }?.asLong()
                ?: throw JwtViolation(JwtViolation.MISSING_CLAIM, "a scoped token must carry an integer 'iat'")
        if (Jwt.seconds(claims, "exp") - iat > scopedMaxLifetimeSeconds) {
            throw JwtViolation(
                JwtViolation.MALFORMED,
                "a scoped token may live at most ${scopedMaxLifetimeSeconds}s",
            )
        }
        val orgId = Jwt.text(claims, "org")
        val scopes =
            grants(
                cat = claims.get("cat")?.let(::scopedCat),
                scope = scopeClaim(claims, strict = true),
                kind = "scoped",
            )
        val user =
            when (val usr = claims.get("usr")) {
                null -> null
                else ->
                    usr.takeIf { it.isTextual && it.asText().isNotBlank() && it.asText().length <= MAX_USER_LENGTH }
                        ?.asText()
                        ?: throw JwtViolation(
                            JwtViolation.MALFORMED,
                            "claim 'usr' is not a non-blank string of at most $MAX_USER_LENGTH characters",
                        )
            }
        return Caller(
            subject = subject,
            orgId = orgId,
            kind = Caller.Kind.SCOPED,
            catalogScopes = scopes,
            tokenId = tokenId,
            clientName = clientName,
            delegates = delegates(claims),
            predicates = predicates(claims.get("prd")),
            user = user,
            routing = routing,
        )
    }

    private fun predicates(prd: JsonNode?): Map<String, PredicateGrant> {
        fun refuse(detail: String): Nothing = throw JwtViolation(JwtViolation.INVALID_PREDICATE, detail)
        if (prd == null || !prd.isObject) refuse("a scoped token must carry an object 'prd'")
        val fields = prd.properties().map { it.key }.toSet()
        if (fields != predicateFields) {
            refuse("prd must name exactly ${predicateFields.sorted()}, got ${fields.sorted()}")
        }
        return predicateFields.associateWith { field ->
            val values = prd.get(field)
            if (!values.isObject || values.isEmpty) refuse("prd.$field is not a non-empty object")
            if (values.size() > MAX_PREDICATE_VALUES) {
                refuse("prd.$field holds ${values.size()} values; at most $MAX_PREDICATE_VALUES")
            }
            val parsed = LinkedHashMap<String, Scope>()
            values.properties().forEach { (value, perm) ->
                parsed[value] = perm.takeIf { it.isTextual }?.asText()?.let(::predicateScope)
                    ?: refuse("prd.$field['$value'] must be 'read' or 'write'")
            }
            if (ALL in parsed) {
                if (parsed.size != 1) refuse("prd.$field mixes '$ALL' with values")
                PredicateGrant.All(parsed.getValue(ALL))
            } else {
                PredicateGrant.Values(
                    parsed.entries.associate { (value, scope) ->
                        val id =
                            value.takeIf { CANONICAL_ID.matches(it) }?.toLongOrNull()
                                ?: refuse("prd.$field key '$value' is not a canonical decimal id")
                        id to scope
                    },
                )
            }
        }
    }

    companion object {
        /** The most values one predicate field may enumerate. */
        const val MAX_PREDICATE_VALUES = 200

        /**
         * Default `exp - iat` ceiling for a scoped token, 15 minutes; an issuer
         * that mints longer-lived scoped tokens needs
         * `HOGLAKE_AUTH_SCOPED_MAX_LIFETIME_SECONDS` raised.
         */
        const val DEFAULT_SCOPED_MAX_LIFETIME_SECONDS = 900L

        private const val MAX_USER_LENGTH = 200
        private const val ALL = "*"

        // No leading zero, no sign, at most 19 digits; toLongOrNull rejects
        // the 19-digit values past Long.MAX_VALUE.
        private val CANONICAL_ID = Regex("^[1-9][0-9]{0,18}$")
        private val FIELD_NAME = Regex("^[a-z][a-z0-9_]{0,62}$")

        /** The namespace of hoglake's values in the OAuth `scope` claim. */
        private const val SCOPE_PREFIX = "hoglake:"
        private val CATALOG_NAME = Regex(Identifiers.CATALOG_NAME_PATTERN)

        /** A predicate permission: `read` or `write`, nothing stronger. */
        private fun predicateScope(word: String): Scope? =
            Scope.fromWire(word)?.takeIf { it == Scope.READ || it == Scope.WRITE }
    }
}
