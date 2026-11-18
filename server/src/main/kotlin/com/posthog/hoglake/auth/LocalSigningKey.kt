package com.posthog.hoglake.auth

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import java.math.BigInteger
import java.security.AlgorithmParameters
import java.security.KeyFactory
import java.security.KeyPairGenerator
import java.security.MessageDigest
import java.security.SecureRandom
import java.security.Signature
import java.security.interfaces.ECPrivateKey
import java.security.interfaces.ECPublicKey
import java.security.spec.ECGenParameterSpec
import java.security.spec.ECParameterSpec
import java.security.spec.ECPrivateKeySpec
import java.util.Base64

/**
 * The server's own ES256 signing key, for `HOGLAKE_AUTH_MODE=local`
 * (docs/auth.md, "Local issuer").
 *
 * ENV-ONLY, by decision: the key is `HOGLAKE_AUTH_SIGNING_KEY`, a private
 * EC P-256 JWK (`kty`, `crv`, `d`, `x`, `y`, optional `kid`) base64url-encoded
 * so it travels as one shell token. There is no key file and nothing is
 * persisted in the metadata store: an operator generates it once with
 * `hoglake token keygen`, exports it where the server and the minting shell
 * both read it, and that is the whole key management story of a standalone
 * install. The server reads only the PUBLIC half out of it ([signingKeys],
 * [publicJwks]); the private half is used by `hoglake token mint`, offline.
 *
 * `kid` defaults to the derivation the contract vectors use: the RFC 7638
 * thumbprint of the public key, first 8 bytes, hex ([keyId]).
 *
 * Never logged. [toString] names the kid only, and every refusal in [decode]
 * names the variable without echoing its value.
 */
class LocalSigningKey private constructor(
    val keyId: String,
    private val privateKey: ECPrivateKey,
    val publicKey: ECPublicKey,
    private val x: String,
    private val y: String,
    private val d: String,
) {
    /** The public JWK, as a JWKS document would carry it. */
    fun publicJwk(): ObjectNode =
        mapper.createObjectNode().apply {
            put("kty", "EC")
            put("crv", "P-256")
            put("x", x)
            put("y", y)
            put("use", "sig")
            put("alg", Jwt.ES256)
            put("kid", keyId)
        }

    /** `{"keys":[…]}` with the one public key: what `/.well-known/jwks.json` serves under `local`. */
    fun publicJwks(): String =
        mapper.writeValueAsString(
            mapper.createObjectNode().apply {
                putArray("keys").add(publicJwk())
            },
        )

    /** The verifier's view: one fixed key under its kid. */
    fun signingKeys(): SigningKeys = StaticSigningKeys(keyId, publicKey)

    /** The value `HOGLAKE_AUTH_SIGNING_KEY` holds for this key. */
    fun encode(): String {
        val jwk =
            mapper.createObjectNode().apply {
                put("kty", "EC")
                put("crv", "P-256")
                put("kid", keyId)
                put("d", d)
                put("x", x)
                put("y", y)
            }
        return b64.encodeToString(mapper.writeValueAsBytes(jwk))
    }

    /**
     * Sign a compact JWS: `header` must carry `alg: ES256` and this key's
     * `kid`, which [LocalIssuer] guarantees; the signature is P1363 `r||s`, as
     * the verifier expects.
     */
    fun sign(
        header: JsonNode,
        claims: JsonNode,
    ): String {
        val input =
            b64.encodeToString(mapper.writeValueAsBytes(header)) + "." +
                b64.encodeToString(mapper.writeValueAsBytes(claims))
        return input + "." + b64.encodeToString(signBytes(input.toByteArray(Charsets.US_ASCII)))
    }

    private fun signBytes(bytes: ByteArray): ByteArray =
        Signature.getInstance(SIGNATURE).run {
            initSign(privateKey)
            update(bytes)
            sign()
        }

    private fun verifies(
        bytes: ByteArray,
        signature: ByteArray,
    ): Boolean =
        Signature.getInstance(SIGNATURE).run {
            initVerify(publicKey)
            update(bytes)
            verify(signature)
        }

    override fun toString(): String = "LocalSigningKey(kid=$keyId)"

    companion object {
        const val ENV = "HOGLAKE_AUTH_SIGNING_KEY"
        private const val SIGNATURE = "SHA256withECDSAinP1363Format"
        private const val COORDINATE_BYTES = 32

        private val mapper = ObjectMapper()
        private val b64 = Base64.getUrlEncoder().withoutPadding()
        private val b64d = Base64.getUrlDecoder()
        private val p256: ECParameterSpec =
            AlgorithmParameters.getInstance("EC").run {
                init(ECGenParameterSpec("secp256r1"))
                getParameterSpec(ECParameterSpec::class.java)
            }

        /** A fresh P-256 key from the platform's secure random source. */
        fun generate(): LocalSigningKey {
            val pair =
                KeyPairGenerator.getInstance("EC").run {
                    initialize(ECGenParameterSpec("secp256r1"), SecureRandom())
                    generateKeyPair()
                }
            val pub = pair.public as ECPublicKey
            val x = coordinate(pub.w.affineX)
            val y = coordinate(pub.w.affineY)
            return LocalSigningKey(
                keyId = keyId(x, y),
                privateKey = pair.private as ECPrivateKey,
                publicKey = pub,
                x = x,
                y = y,
                d = coordinate((pair.private as ECPrivateKey).s),
            )
        }

        /**
         * Decode `HOGLAKE_AUTH_SIGNING_KEY`. Every refusal is an
         * [IllegalArgumentException] naming the variable and what was wrong with
         * it, never what it held: this runs at boot and in a shell, and both
         * print the message.
         */
        fun decode(value: String): LocalSigningKey {
            val trimmed = value.trim()
            require(trimmed.isNotEmpty()) { "$ENV is empty; run `hoglake token keygen` to make one" }
            val bytes =
                try {
                    b64d.decode(trimmed)
                } catch (e: IllegalArgumentException) {
                    throw IllegalArgumentException("$ENV is not base64url (the output of `hoglake token keygen`)", e)
                }
            val jwk =
                try {
                    mapper.readTree(bytes)
                } catch (e: Exception) {
                    throw IllegalArgumentException("$ENV does not decode to JSON", e)
                }
            require(jwk != null && jwk.isObject) { "$ENV does not decode to a JSON object (a private JWK)" }
            val kty = jwk.get("kty")?.takeIf { it.isTextual }?.asText()
            require(kty == "EC") {
                "$ENV must be an EC P-256 private JWK (kty EC); hoglake's local issuer signs ES256 only, got kty $kty"
            }
            val crv = jwk.get("crv")?.takeIf { it.isTextual }?.asText()
            require(crv == "P-256") { "$ENV must be an EC P-256 private JWK (crv P-256), got crv $crv" }
            val publicKey =
                try {
                    Jwt.ecPublicKey(jwk)
                } catch (e: JwtViolation) {
                    throw IllegalArgumentException("$ENV public half is not a usable P-256 key: ${e.message}", e)
                }
            val dText = jwk.get("d")?.takeIf { it.isTextual }?.asText()
            require(!dText.isNullOrEmpty()) { "$ENV must be a PRIVATE JWK: it is missing 'd'" }
            val d =
                try {
                    b64d.decode(dText)
                } catch (e: IllegalArgumentException) {
                    throw IllegalArgumentException("$ENV 'd' is not base64url", e)
                }
            require(d.size == COORDINATE_BYTES) { "$ENV 'd' must be 32 bytes, got ${d.size}" }
            val scalar = BigInteger(1, d)
            require(scalar.signum() > 0 && scalar < p256.order) { "$ENV 'd' is not a valid P-256 scalar" }
            val privateKey =
                KeyFactory.getInstance(
                    "EC",
                ).generatePrivate(ECPrivateKeySpec(scalar, p256)) as ECPrivateKey
            val x = jwk.get("x").asText()
            val y = jwk.get("y").asText()
            val kid =
                when (val k = jwk.get("kid")) {
                    null -> keyId(x, y)
                    else -> {
                        require(
                            k.isTextual && k.asText().isNotBlank(),
                        ) { "$ENV 'kid', when present, must be a non-empty string" }
                        k.asText()
                    }
                }
            val key = LocalSigningKey(kid, privateKey, publicKey, x, y, dText)
            // The one check the JDK cannot do for us: that d is the private
            // half of x/y. A probe signature proves it, and a key whose halves
            // disagree would otherwise mint tokens the server refuses.
            val probe = "hoglake-local-key-probe".toByteArray(Charsets.US_ASCII)
            require(key.verifies(probe, key.signBytes(probe))) { "$ENV 'd' does not match its 'x'/'y': not one key" }
            return key
        }

        /** RFC 7638 thumbprint of the public P-256 key, first 8 bytes, hex: the contract vectors' rule. */
        fun keyId(
            x: String,
            y: String,
        ): String =
            MessageDigest.getInstance("SHA-256")
                .digest("""{"crv":"P-256","kty":"EC","x":"$x","y":"$y"}""".toByteArray())
                .take(8)
                .joinToString("") { "%02x".format(it) }

        /** A field element as the fixed 32-byte base64url JWK coordinate. */
        private fun coordinate(v: BigInteger): String {
            val raw = v.toByteArray()
            val fixed = ByteArray(COORDINATE_BYTES)
            val from = if (raw.size > COORDINATE_BYTES) raw.size - COORDINATE_BYTES else 0
            val len = raw.size - from
            System.arraycopy(raw, from, fixed, COORDINATE_BYTES - len, len)
            return b64.encodeToString(fixed)
        }
    }
}
