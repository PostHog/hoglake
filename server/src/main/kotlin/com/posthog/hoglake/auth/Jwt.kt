package com.posthog.hoglake.auth

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import java.math.BigInteger
import java.security.AlgorithmParameters
import java.security.KeyFactory
import java.security.Signature
import java.security.interfaces.ECPublicKey
import java.security.spec.ECGenParameterSpec
import java.security.spec.ECParameterSpec
import java.security.spec.ECPoint
import java.security.spec.ECPublicKeySpec
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
        const val UNKNOWN_KEY = "unknown_signing_key"
        const val BAD_SIGNATURE = "invalid_signature"
        const val EXPIRED = "token_expired"
        const val NOT_YET_VALID = "token_not_yet_valid"
        const val WRONG_AUDIENCE = "invalid_audience"
        const val WRONG_ISSUER = "invalid_issuer"
        const val MISSING_CLAIM = "missing_claim"
        const val STALE_EPOCH = "stale_credential_epoch"
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
 * Compact-JWS parsing, ES256 verification, and JWK decoding.
 *
 * Hand-rolled rather than pulling `ktor-server-auth-jwt` + a JOSE library, on
 * the same reasoning `observability/RequestId.kt` records for its own plugin:
 * the dependency is not on the classpath, and what we need is one algorithm and
 * one key type. What that costs is that EVERY line below is load-bearing, so the
 * classes it is easy to get wrong are called out where they appear:
 *
 * - **`alg: none`** — the classic JWT downgrade. The algorithm is read from the
 *   header and checked against an allowlist of exactly one, never used to
 *   *select* a verifier.
 * - **Algorithm confusion** — a verifier that accepts both RSA and EC can be
 *   fed an HMAC token whose "key" is the public key. There is no such mode here:
 *   [ALGORITHM] is the only accepted value and the key is always an [ECPublicKey].
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
 */
internal object Jwt {
    const val ALGORITHM = "ES256"

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
        if (algorithm != ALGORITHM) {
            throw JwtViolation(JwtViolation.UNSUPPORTED_ALG, "alg '$algorithm' is not supported; only $ALGORITHM")
        }

        return ParsedJws(
            keyId = header.get("kid")?.takeIf { it.isTextual }?.asText(),
            algorithm = algorithm,
            claims = claims,
            signingInput = "${parts[0]}.${parts[1]}".toByteArray(Charsets.US_ASCII),
            signature = signature,
        )
    }

    /** Verify [jws] against [key]. Throws [JwtViolation] on any mismatch. */
    fun verifySignature(
        jws: ParsedJws,
        key: ECPublicKey,
    ) {
        // P1363 is the JWS `R||S` form; see the class comment.
        val verifier = Signature.getInstance("SHA256withECDSAinP1363Format")
        verifier.initVerify(key)
        verifier.update(jws.signingInput)
        if (!verifier.verify(jws.signature)) {
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
     * Decode one JWK object into an [ECPublicKey].
     *
     * Accepts only `kty: EC`, `crv: P-256`, and 32-byte `x`/`y` — the parameters
     * of [ALGORITHM]. Anything else is refused here rather than at the first
     * signature check, so a JWKS containing an RSA key is a loud boot-time or
     * refresh-time error instead of a silent per-request failure.
     */
    fun ecPublicKey(jwk: JsonNode): ECPublicKey {
        fun field(name: String): String =
            jwk.get(name)?.takeIf { it.isTextual }?.asText()
                ?: throw JwtViolation(JwtViolation.MALFORMED, "jwk is missing '$name'")

        if (field("kty") != "EC" || field("crv") != "P-256") {
            throw JwtViolation(
                JwtViolation.MALFORMED,
                "jwk kty/crv must be EC/P-256, got ${field("kty")}/${field("crv")}",
            )
        }
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
