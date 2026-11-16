package com.posthog.hoglake.auth

import com.posthog.hoglake.Config
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

/** `HOGLAKE_AUTH_SESSION_PROFILE`: off by default, `on` reaches the verifier, anything else fails boot. */
class SessionProfileConfigTest {
    private val pair = TestTokens.keyPair()

    private fun service(
        dir: Path,
        profile: String,
    ): AuthService {
        val jwks = dir.resolve("jwks.json")
        Files.writeString(jwks, TestTokens.jwks(pair))
        return AuthService.fromConfig(
            Config(
                authMode = "enforce",
                authIssuer = TestTokens.ISSUER,
                authJwksFile = jwks.toString(),
                authSessionProfile = profile,
            ),
        )
    }

    private fun profiledToken(): String {
        // The service reads the real clock, so the token lives far into the future.
        val base = TestTokens.tenantClaims(catalogs = mapOf("lake" to listOf("read")), exp = 4_000_000_000L)
        val claims =
            base.removeSuffix("}") +
                ""","profile":"storage-session","version":1,"purpose":"user-session","primary_catalog":"lake",""" +
                """"aws":{"access_key_id":"k","secret_access_key":"s","session_token":"t","region":"r",""" +
                """"expires_at":4000000300}}"""
        return TestTokens.mint(pair, claims)
    }

    @Test
    fun `the profile is off by default`() {
        assertThat(Config().authSessionProfile).isEqualTo("off")
        assertThat(Config().authSessionProfileEnabled).isFalse()
        assertThat(Config(authSessionProfile = " ON ").authSessionProfileEnabled).isTrue()
    }

    @Test
    fun `anything but off or on fails boot, naming the setting`() {
        for (bad in listOf("true", "yes", "enforce", "")) {
            assertThatThrownBy { Config(authSessionProfile = bad) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("HOGLAKE_AUTH_SESSION_PROFILE")
        }
    }

    @Test
    fun `the setting reaches the verifier`(
        @TempDir dir: Path,
    ) {
        val token = profiledToken()
        assertThatThrownBy { service(dir, "off").verifier!!.verify(token) }
            .isInstanceOfSatisfying(JwtViolation::class.java) {
                assertThat(it.reason).isEqualTo(JwtViolation.PROFILE_NOT_ENABLED)
            }
        assertThat(service(dir, "on").verifier!!.verify(token).catalogScopes.keys).containsExactly("lake")
    }
}
