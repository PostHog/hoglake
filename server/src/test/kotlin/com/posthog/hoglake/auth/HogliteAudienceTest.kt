package com.posthog.hoglake.auth

import com.posthog.hoglake.store.BackendKind
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.security.interfaces.ECPublicKey
import java.util.UUID

class HogliteAudienceTest {
    @Test
    fun `configured audience membership is issuer neutral and every array entry is textual`() {
        val pair = TestTokens.keyPair()
        val id = UUID.fromString(ContractVectors.BACKEND_ID)
        val verifier =
            JwtVerifier(
                TestTokens.ISSUER,
                "custom-metadata",
                StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
                backends = mapOf(id to BackendKind.HOGLITE),
            )

        fun claims(audience: String): String =
            TestTokens.tenantClaims().replace(
                """"aud":"hoglake"""",
                """"aud":$audience""",
            ).dropLast(1) + ""","rtg":{"b":"hoglite","be":"$id","t":"opaque_1"}}"""
        for (audience in listOf(
            "\"custom-metadata\"",
            """["custom-metadata","hoglite"]""",
            """["hogtower-vend","custom-metadata","other"]""",
        )) {
            val valid = TestTokens.mint(pair, claims(audience))
            assertThat(verifier.verify(valid).routing?.tenant).isEqualTo("opaque_1")
        }
        for (audience in listOf(
            """["hoglake","hoglite"]""",
            """["custom-metadata",7]""",
            """["custom-metadata",null]""",
        )) {
            assertThatThrownBy { verifier.verify(TestTokens.mint(pair, claims(audience))) }
                .isInstanceOf(JwtViolation::class.java)
        }
    }
}
