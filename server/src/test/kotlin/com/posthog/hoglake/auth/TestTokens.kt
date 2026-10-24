package com.posthog.hoglake.auth

import java.security.KeyPair
import java.security.KeyPairGenerator
import java.security.Signature
import java.security.spec.ECGenParameterSpec
import java.util.Base64

/**
 * Mints the tokens the auth tests verify.
 *
 * A signing side written here rather than borrowed from the production issuer,
 * because two of the properties under test are about things a shared
 * implementation would get right by accident: `alg: none` has to be *forgeable*
 * by a test (it is written by hand, never by a library), and a token with a
 * tampered payload has to be falsifiable in exactly the way a library would not
 * let us express.
 */
object TestTokens {
    const val KID = "test-key-1"
    const val ISSUER = "https://hoglake.test"
    const val AUDIENCE = "hoglake"

    fun keyPair(): KeyPair =
        KeyPairGenerator.getInstance("EC").run {
            initialize(ECGenParameterSpec("secp256r1"))
            generateKeyPair()
        }

    private val encoder = Base64.getUrlEncoder().withoutPadding()

    /** A compact JWS signed with [pair], or an unsigned one when the alg is forged. */
    fun mint(
        pair: KeyPair,
        claims: String,
        header: String = """{"alg":"ES256","kid":"$KID","typ":"JWT"}""",
        sign: Boolean = true,
    ): String {
        val h = encoder.encodeToString(header.toByteArray())
        val p = encoder.encodeToString(claims.toByteArray())
        val signingInput = "$h.$p".toByteArray(Charsets.US_ASCII)
        if (!sign) {
            // The `alg: none` shape: 64 zero bytes where a signature would be.
            return "$h.$p." + encoder.encodeToString(ByteArray(64))
        }
        val sig =
            Signature.getInstance("SHA256withECDSAinP1363Format").run {
                initSign(pair.private)
                update(signingInput)
                sign()
            }
        return "$h.$p." + encoder.encodeToString(sig)
    }

    /** The standard tenant claim set, overridable per test. */
    fun tenantClaims(
        org: String = "11111111-1111-1111-1111-111111111111",
        catalogs: Map<String, List<String>> = mapOf("lake" to listOf("read", "write")),
        exp: Long = 4_000_000_000L,
        nbf: Long? = null,
        iat: Long = 1_700_000_000L,
        iss: String = ISSUER,
        aud: String = AUDIENCE,
        sub: String = "client:hedgerow-prod",
        jti: String = "jti-1",
    ): String {
        val cat =
            catalogs.entries.joinToString(",") { (name, scopes) ->
                """"$name":[${scopes.joinToString(",") { "\"$it\"" }}]"""
            }
        val nbfClaim = nbf?.let { ""","nbf":$it""" } ?: ""
        return """{"iss":"$iss","aud":"$aud","sub":"$sub","jti":"$jti","ten":"tenant",""" +
            """"org":"$org","cli":"hedgerow-prod","ep":7,"iat":$iat,"exp":$exp$nbfClaim,"cat":{$cat}}"""
    }

    fun operatorClaims(
        exp: Long = 4_000_000_000L,
        sub: String = "sa:bulk-loader",
        kid: String = KID,
    ): String =
        """{"iss":"$ISSUER","aud":"$AUDIENCE","sub":"$sub","jti":"jti-op","ten":"operator",""" +
            """"iat":1700000000,"exp":$exp}"""

    /** The JWKS document for [pair], as the verifier's key source would see it. */
    fun jwks(
        pair: KeyPair,
        kid: String = KID,
    ): String {
        val pub = pair.public as java.security.interfaces.ECPublicKey

        fun coord(v: java.math.BigInteger): String {
            val raw = v.toByteArray()
            // BigInteger drops leading zero bytes; P-256 coordinates are 32.
            val fixed = ByteArray(32)
            val from = if (raw.size > 32) raw.size - 32 else 0
            System.arraycopy(raw, from, fixed, 32 - (raw.size - from), raw.size - from)
            return encoder.encodeToString(fixed)
        }
        return """{"keys":[{"kty":"EC","crv":"P-256","kid":"$kid","use":"sig","alg":"ES256",""" +
            """"x":"${coord(pub.w.affineX)}","y":"${coord(pub.w.affineY)}"}]}"""
    }
}
