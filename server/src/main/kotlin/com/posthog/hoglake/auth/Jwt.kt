package com.posthog.hoglake.auth

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import java.math.BigInteger
import java.security.AlgorithmParameters
import java.security.KeyFactory
import java.security.PublicKey
import java.security.Signature
import java.security.interfaces.ECPublicKey
import java.security.interfaces.RSAPublicKey
import java.security.spec.ECGenParameterSpec
import java.security.spec.ECParameterSpec
import java.security.spec.ECPoint
import java.security.spec.ECPublicKeySpec
import java.security.spec.RSAPublicKeySpec
import java.util.Base64

/**
 * A typed refusal from the token layer.
 *
 * [reason] is the machine-readable half that reaches the client (in the 401
 * body's `error` and `WWW-Authenticate`), and it is the closed vocabulary below
 * — never a parser message. The message on [detail] is for the log: a 401 body
 * that echoed "illegal base64 character at index 43" would tell an attacker
 * exactly which byte to change.
 */
class JwtViolation(
    val reason: String,
    detail: String,
) : RuntimeException(detail) {
    companion object {
        /**
         * No credential was presented at all. RFC 6750 §3.1's
         * `invalid_request` for a request the scheme cannot even begin to
         * parse; kept distinct from [MALFORMED] because the two demand
         * different answers — an operator's `warn`-mode checklist cares
         * about callers that send NOTHING, and a burst of malformed tokens
         * is a different (and worse) signal.
         */
        const val NO_TOKEN = "invalid_request"

        const val MALFORMED = "invalid_token"
        const val UNSUPPORTED_ALG = "unsupported_algorithm"
        const val UNKNOWN_BACKEND = "unknown_backend"
        const val UNKNOWN_KEY = "unknown_signing_key"
        const val BAD_SIGNATURE = "invalid_signature"
        const val EXPIRED = "token_expired"
        const val NOT_YET_VALID = "token_not_yet_valid"
        const val WRONG_AUDIENCE = "invalid_audience"
        const val WRONG_ISSUER = "invalid_issuer"
        const val MISSING_CLAIM = "missing_claim"
        const val STALE_EPOCH = "stale_credential_epoch"

        /**
         * A scoped token's `prd` is missing, malformed, or names a field this
         * instance's auth source does not declare; or this instance accepts no
         * scoped tokens at all.
         */
        const val INVALID_PREDICATE = "invalid_predicate"
    }
}

/**
 * A parsed JWS: the pieces before any verification happens.
 *
 * Parsing and verification are separate on purpose. Parsing must be total and
 * cheap (it runs before we know who is calling, so it must not touch a database
 * or the network), and verification is the only thing allowed to return a
 * [Caller]. Nothing outside this file may construct a [Caller] from a
 * [ParsedJws] without going through [JwtVerifier].
 */
internal class ParsedJws(
    val keyId: String?,
    val algorithm: String?,
    val claims: JsonNode,
    /** The exact bytes `header.payload` — what the signature covers. */
    signingInput: ByteArray,
    val signature: ByteArray,
) {
    val signingInput: ByteArray = signingInput.copyOf()
}

/**
 * Compact-JWS parsing, ES256/RS256 verification, and JWK decoding.
 *
 * Hand-rolled rather than pulling `ktor-server-auth-jwt` + a JOSE library, on
 * the same reasoning `observability/RequestId.kt` records for its own plugin:
 * the dependency is not on the classpath, and what we need is two algorithms
 * and their two key types. What that costs is that EVERY line below is
 * load-bearing, so the classes it is easy to get wrong are called out where
 * they appear:
 *
 * - **`alg: none`** — the classic JWT downgrade. The algorithm is read from the
 *   header and checked against an allowlist ([ALGORITHMS]) before any key is
 *   looked up; `none` and every HMAC algorithm are outside it.
 * - **Algorithm confusion** — a verifier that accepts both RSA and EC can be
 *   fed an HMAC token whose "key" is the public key. No HMAC algorithm is
 *   accepted, so a public key is never used as a secret. And the header's `alg`
 *   never *selects* a key: the key is selected by `kid`, and [verifySignature]
 *   then refuses the token unless that key's type is the one the `alg` needs
 *   (ES256 ↔ an EC P-256 key, RS256 ↔ an RSA key), so an RS256 header cannot
 *   steer an EC key into an RSA verifier or the reverse.
 * - **`crit`** — RFC 7515 §4.1.11 requires rejecting a token with a critical
 *   header this implementation does not understand, precisely because ignoring it
 *   is how signature-stripping extensions get accepted.
 * - **Duplicate JSON keys** — `{"exp":1,"exp":1e10}` parses to whichever the
 *   parser saw last, so a lenient parser plus a strict validator disagree about
 *   the expiry. Jackson's strict duplicate detection makes it a parse failure.
 * - **Raw vs DER ECDSA signatures** — JWS ES256 carries the 64-byte `R||S` form,
 *   not ASN.1 DER. `SHA256withECDSA` expects DER and would reject every valid
 *   token; `SHA256withECDSAinP1363Format` takes the JWS form directly.
 * - **Off-curve public keys** — an attacker-supplied JWK whose point is not on
 *   P-256 can leak information through invalid-curve arithmetic. Checked in
 *   [ecPublicKey] rather than trusted to the provider.
 * - **Short RSA moduli** — RFC 7518 §3.3 requires at least 2048 bits for RS256;
 *   a smaller modulus is refused in [rsaPublicKey], so a factorable key in a
 *   JWKS fails at load rather than verifying forgeries.
 */
internal object Jwt {
    const val ES256 = "ES256"
    const val RS256 = "RS256"

    /**
     * Every `alg` this verifier accepts. ES256 is what hoglake's own issuers
     * mint; RS256 is what off-the-shelf identity providers sign with by
     * default. Nothing else — in particular no `none` and no HMAC.
     */
    val ALGORITHMS: Set<String> = setOf(ES256, RS256)

    /** The smallest RSA modulus accepted, in bits (RFC 7518 §3.3). */
    const val MIN_RSA_BITS = 2048

    /** Base64url, no padding, strict alphabet — RFC 7515 §2. */
    private val BASE64URL = Base64.getUrlDecoder()

    /**
     * Strict duplicate detection, and nothing else loosened: an unknown claim is
     * data (forward compatibility), an unknown *header* is refused separately in
     * [parse] via the `crit` check.
     */
    private val json =
        ObjectMapper()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .apply {
                factory.enable(
                    com.fasterxml.jackson.core.StreamReadFeature.STRICT_DUPLICATE_DETECTION.mappedFeature(),
                )
            }

    /** A compact JWS is `header.payload.signature`; nothing else is accepted. */
    fun parse(token: String): ParsedJws {
        val parts = token.trim().split('.')
        if (parts.size != 3) {
            throw JwtViolation(JwtViolation.MALFORMED, "a compact JWS has 3 segments, got ${parts.size}")
        }
        val headerBytes = decodeSegment(parts[0], "header")
        val payloadBytes = decodeSegment(parts[1], "payload")
        val signature = decodeSegment(parts[2], "signature")

        val header = readJson(headerBytes, "header")
        val claims = readJson(payloadBytes, "payload")

        // `crit`: RFC 7515 §4.1.11. We understand no extensions, so ANY
        // critical entry means we must not accept the token.
        val crit = header.get("crit")
        if (crit != null && !(crit.isArray && crit.isEmpty)) {
            throw JwtViolation(
                JwtViolation.MALFORMED,
                "token carries critical header extensions this verifier does not implement: $crit",
            )
        }

        val algorithm = header.get("alg")?.takeIf { it.isTextual }?.asText()
        if (algorithm == null) {
            throw JwtViolation(JwtViolation.MALFORMED, "token header has no alg")
        }
        // Compared before any key is looked up: `none` and `HS256` must not even
        // reach a verifier, so an unknown algorithm can never select one.
        if (algorithm !in ALGORITHMS) {
            throw JwtViolation(
                JwtViolation.UNSUPPORTED_ALG,
                "alg '$algorithm' is not supported; only ${ALGORITHMS.sorted()}",
            )
        }

        return ParsedJws(
            keyId = header.get("kid")?.takeIf { it.isTextual }?.asText(),
            algorithm = algorithm,
            claims = claims,
            signingInput = "${parts[0]}.${parts[1]}".toByteArray(Charsets.US_ASCII),
            signature = signature,
        )
    }

    /**
     * Verify [jws] against [key]. Throws [JwtViolation] on any mismatch.
     *
     * The key's TYPE must be the one the token's `alg` names: an EC key verifies
     * only ES256 and an RSA key only RS256. A mismatch is refused as a bad
     * signature — the key the `kid` names cannot have produced this token —
     * rather than being handed to a verifier for a different algorithm.
     */
    fun verifySignature(
        jws: ParsedJws,
        key: PublicKey,
    ) {
        val verifier =
            when (jws.algorithm) {
                // P1363 is the JWS `R||S` form; see the class comment.
                ES256 ->
                    if (key is ECPublicKey && key.params.order.bitLength() == 256) {
                        Signature.getInstance("SHA256withECDSAinP1363Format")
                    } else {
                        null
                    }
                RS256 ->
                    if (key is RSAPublicKey && key.modulus.bitLength() >= MIN_RSA_BITS) {
                        Signature.getInstance("SHA256withRSA")
                    } else {
                        null
                    }
                else -> null
            } ?: throw JwtViolation(
                JwtViolation.BAD_SIGNATURE,
                "the key named by kid is a ${key.algorithm} key and cannot verify alg '${jws.algorithm}'",
            )
        val ok =
            try {
                verifier.initVerify(key)
                verifier.update(jws.signingInput)
                verifier.verify(jws.signature)
            } catch (e: java.security.SignatureException) {
                // An RSA signature of the wrong length throws rather than
                // returning false; to the caller it is the same refusal.
                false
            }
        if (!ok) {
            throw JwtViolation(JwtViolation.BAD_SIGNATURE, "signature does not verify")
        }
    }

    // ---- claims --------------------------------------------------------

    /** Required textual claim; a missing or non-textual one is a refusal. */
    fun text(
        claims: JsonNode,
        name: String,
    ): String =
        claims.get(name)?.takeIf { it.isTextual }?.asText()
            ?: throw JwtViolation(JwtViolation.MISSING_CLAIM, "claim '$name' is missing or not a string")

    fun textOrNull(
        claims: JsonNode,
        name: String,
    ): String? = claims.get(name)?.takeIf { it.isTextual }?.asText()

    /**
     * A required numeric claim, as seconds since the epoch.
     *
     * `isIntegralNumber` is checked rather than coercing: a float `exp` is
     * legal-looking in some libraries and lets `1e400` become infinity, i.e. a
     * token that never expires.
     */
    fun seconds(
        claims: JsonNode,
        name: String,
    ): Long =
        claims.get(name)?.takeIf { it.isIntegralNumber }?.asLong()
            ?: throw JwtViolation(JwtViolation.MISSING_CLAIM, "claim '$name' is missing or not an integer")

    // ---- JWK -----------------------------------------------------------

    /**
     * Whether a JWKS entry is meant for signatures this verifier can check.
     *
     * Identity providers publish more than signing keys: Keycloak's realm JWKS
     * carries an `RSA-OAEP` encryption key next to its RS256 signing key, and
     * others publish keys for algorithms we do not verify (RS384, PS256,
     * ES384). An entry that DECLARES another purpose — `use` other than `sig`,
     * or an `alg` outside [ALGORITHMS] — is skipped, so pointing hoglake at a
     * standard JWKS works; a token naming that kid then fails as an unknown key.
     * An entry that declares nothing is assumed to be a signing key and must
     * decode as one ([publicKey] refuses it loudly otherwise).
     */
    fun isUsableSigningJwk(jwk: JsonNode): Boolean {
        val use = jwk.get("use")
        if (use != null && !(use.isTextual && use.asText() == "sig")) return false
        val alg = jwk.get("alg")
        if (alg != null && !(alg.isTextual && alg.asText() in ALGORITHMS)) return false
        return true
    }

    /**
     * Decode one JWK object into a verification key: `kty: EC` (P-256, for
     * ES256) or `kty: RSA` (for RS256).
     *
     * A key whose type is neither is refused here rather than at the first
     * signature check, so a JWKS holding a key we cannot use is a loud boot-time
     * or refresh-time error instead of a silent per-request failure. A key whose
     * `alg` member names the other algorithm (an EC key marked RS256, say) is
     * refused for the same reason.
     */
    fun publicKey(jwk: JsonNode): PublicKey =
        when (val kty = jwk.get("kty")?.takeIf { it.isTextual }?.asText()) {
            "EC" -> ecPublicKey(jwk)
            "RSA" -> rsaPublicKey(jwk)
            null -> throw JwtViolation(JwtViolation.MALFORMED, "jwk is missing 'kty'")
            else -> throw JwtViolation(JwtViolation.MALFORMED, "jwk kty must be EC or RSA, got $kty")
        }

    /** A JWK's `alg`, when present, must be [expected]; absent is fine. */
    private fun requireJwkAlg(
        jwk: JsonNode,
        expected: String,
    ) {
        val alg = jwk.get("alg") ?: return
        if (!alg.isTextual || alg.asText() != expected) {
            throw JwtViolation(JwtViolation.MALFORMED, "jwk of kty ${jwk.get("kty")} must have alg $expected, got $alg")
        }
    }

    private fun jwkField(
        jwk: JsonNode,
        name: String,
    ): String =
        jwk.get(name)?.takeIf { it.isTextual }?.asText()
            ?: throw JwtViolation(JwtViolation.MALFORMED, "jwk is missing '$name'")

    /**
     * Decode one EC JWK into an [ECPublicKey].
     *
     * Accepts only `kty: EC`, `crv: P-256`, and 32-byte `x`/`y` — the parameters
     * of [ES256].
     */
    fun ecPublicKey(jwk: JsonNode): ECPublicKey {
        fun field(name: String): String = jwkField(jwk, name)

        if (field("kty") != "EC" || field("crv") != "P-256") {
            throw JwtViolation(
                JwtViolation.MALFORMED,
                "jwk kty/crv must be EC/P-256, got ${field("kty")}/${field("crv")}",
            )
        }
        requireJwkAlg(jwk, ES256)
        val x = decodeSegment(field("x"), "jwk.x")
        val y = decodeSegment(field("y"), "jwk.y")
        if (x.size != 32 || y.size != 32) {
            throw JwtViolation(
                JwtViolation.MALFORMED,
                "P-256 coordinates are 32 bytes, got ${x.size}/${y.size}",
            )
        }

        val params = p256
        val point = ECPoint(BigInteger(1, x), BigInteger(1, y))
        requireOnCurve(point, params)
        return KeyFactory.getInstance("EC").generatePublic(ECPublicKeySpec(point, params)) as ECPublicKey
    }

    /**
     * Decode one RSA JWK (`n`, `e`; RFC 7518 §6.3.1) into an [RSAPublicKey].
     *
     * The modulus must be at least [MIN_RSA_BITS] bits and the exponent an odd
     * integer greater than 1; anything else is not a key RS256 can be safely
     * verified with.
     */
    fun rsaPublicKey(jwk: JsonNode): RSAPublicKey {
        if (jwkField(jwk, "kty") != "RSA") {
            throw JwtViolation(JwtViolation.MALFORMED, "jwk kty must be RSA, got ${jwkField(jwk, "kty")}")
        }
        requireJwkAlg(jwk, RS256)
        val n = BigInteger(1, decodeSegment(jwkField(jwk, "n"), "jwk.n"))
        val e = BigInteger(1, decodeSegment(jwkField(jwk, "e"), "jwk.e"))
        if (n.bitLength() < MIN_RSA_BITS) {
            throw JwtViolation(
                JwtViolation.MALFORMED,
                "RSA modulus is ${n.bitLength()} bits; at least $MIN_RSA_BITS are required",
            )
        }
        if (e <= BigInteger.ONE || !e.testBit(0) || e >= n) {
            throw JwtViolation(JwtViolation.MALFORMED, "RSA public exponent is not a valid odd exponent")
        }
        return KeyFactory.getInstance("RSA").generatePublic(RSAPublicKeySpec(n, e)) as RSAPublicKey
    }

    /** The P-256 domain parameters, resolved once. */
    private val p256: ECParameterSpec by lazy {
        AlgorithmParameters.getInstance("EC").run {
            init(ECGenParameterSpec("secp256r1"))
            getParameterSpec(ECParameterSpec::class.java)
        }
    }

    /**
     * Reject a point that is not on P-256: `y² = x³ - 3x + b (mod p)`.
     *
     * Java's own `ECPublicKeySpec` validation of this has been inconsistent
     * across providers and versions, and this is the one place an attacker
     * supplies key material directly, so it is checked rather than assumed.
     */
    private fun requireOnCurve(
        point: ECPoint,
        params: ECParameterSpec,
    ) {
        val curve = params.curve
        val p = (curve.field as java.security.spec.ECFieldFp).p
        if (point.affineX.signum() < 0 || point.affineX >= p || point.affineY.signum() < 0 || point.affineY >= p) {
            throw JwtViolation(JwtViolation.MALFORMED, "jwk point is outside the field")
        }
        val lhs = point.affineY.modPow(BigInteger.TWO, p)
        val rhs =
            point.affineX
                .modPow(BigInteger.valueOf(3), p)
                .add(curve.a.multiply(point.affineX))
                .add(curve.b)
                .mod(p)
        if (lhs != rhs) {
            throw JwtViolation(JwtViolation.MALFORMED, "jwk point is not on P-256")
        }
    }

    // ---- base64url -----------------------------------------------------

    /**
     * Strict base64url decode.
     *
     * The JDK decoder accepts a `+` and `/` in some paths and tolerates missing
     * padding inconsistently; RFC 7515 mandates the URL-safe alphabet with no
     * padding. So the alphabet is checked here, padding is restored for the
     * decoder, and anything the encoder would not have produced is refused —
     * which keeps a token's wire form canonical and makes two encodings of one
     * signature impossible.
     */
    private fun decodeSegment(
        value: String,
        what: String,
    ): ByteArray {
        if (value.isEmpty()) throw JwtViolation(JwtViolation.MALFORMED, "empty $what segment")
        for (c in value) {
            val ok = c in 'A'..'Z' || c in 'a'..'z' || c in '0'..'9' || c == '-' || c == '_'
            if (!ok) throw JwtViolation(JwtViolation.MALFORMED, "$what segment has a non-base64url character")
        }
        val padding =
            when (value.length % 4) {
                0 -> ""
                2 -> "=="
                3 -> "="
                else -> throw JwtViolation(JwtViolation.MALFORMED, "$what segment has impossible base64 length")
            }
        return try {
            BASE64URL.decode(value + padding)
        } catch (e: IllegalArgumentException) {
            throw JwtViolation(JwtViolation.MALFORMED, "$what segment is not valid base64url: ${e.message}")
        }
    }

    private fun readJson(
        bytes: ByteArray,
        what: String,
    ): JsonNode =
        try {
            json.readTree(bytes)
        } catch (e: Exception) {
            throw JwtViolation(JwtViolation.MALFORMED, "$what is not valid JSON: ${e.message}")
        }
}
