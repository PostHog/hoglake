package com.posthog.hoglake.auth

import com.posthog.hoglake.Config
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

/**
 * The auth settings that are refused at boot, and the ones that reach the
 * verifier: `HOGLAKE_AUTH_SCOPED_MAX_LIFETIME_SECONDS` and
 * `HOGLAKE_AUTH_CLIENT_CLASSES`.
 */
class AuthConfigTest {
    private val pair = TestTokens.keyPair()

    private fun enforcing(
        dir: Path,
        scopedMax: Long? = null,
    ): AuthService {
        val jwks = dir.resolve("jwks.json")
        Files.writeString(jwks, TestTokens.jwks(pair))
        val base =
            Config(
                authMode = "enforce",
                authIssuer = TestTokens.ISSUER,
                authJwksFile = jwks.toString(),
                authPredicateFields = "team_id",
            )
        val cfg = if (scopedMax == null) base else base.copy(authScopedMaxLifetimeSeconds = scopedMax)
        return AuthService.fromConfig(cfg)
    }

    /** A scoped token issued a minute ago and living [lifetime], so `exp` is still ahead of now. */
    private fun scopedToken(lifetime: Long): String {
        val iat = System.currentTimeMillis() / 1000 - 60
        return TestTokens.mint(pair, TestTokens.scopedClaims(iat = iat, exp = iat + lifetime))
    }

    @Test
    fun `the scoped lifetime cap defaults to 900 seconds`() {
        assertThat(Config().authScopedMaxLifetimeSeconds).isEqualTo(900L)
    }

    @Test
    fun `the default cap reaches the verifier`(
        @TempDir dir: Path,
    ) {
        val verifier = enforcing(dir).verifier!!
        assertThat(verifier.verify(scopedToken(900)).kind).isEqualTo(Caller.Kind.SCOPED)
        assertThatThrownBy { verifier.verify(scopedToken(901)) }
            .isInstanceOfSatisfying(JwtViolation::class.java) {
                assertThat(it.reason).isEqualTo(JwtViolation.MALFORMED)
                assertThat(it.message).contains("900s")
            }
    }

    @Test
    fun `an overridden cap reaches the verifier`(
        @TempDir dir: Path,
    ) {
        val verifier = enforcing(dir, scopedMax = 3600).verifier!!
        assertThat(verifier.verify(scopedToken(3600)).kind).isEqualTo(Caller.Kind.SCOPED)
        assertThatThrownBy { verifier.verify(scopedToken(3601)) }
            .isInstanceOfSatisfying(JwtViolation::class.java) {
                assertThat(it.message).contains("3600s")
            }
    }

    @Test
    fun `a cap of zero or less fails boot, naming the setting`() {
        for (bad in listOf(0L, -900L)) {
            assertThatThrownBy { Config(authScopedMaxLifetimeSeconds = bad) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("HOGLAKE_AUTH_SCOPED_MAX_LIFETIME_SECONDS")
        }
    }

    @Test
    fun `the client class comes from a closed set of generic clients`() {
        assertThat(clientClass(null as String?)).isEqualTo("none")
        assertThat(clientClass("  ")).isEqualTo("none")
        assertThat(clientClass("Trino/479")).isEqualTo("trino")
        assertThat(clientClass("python-httpx/0.28")).isEqualTo("pyhoglake")
        assertThat(clientClass("Go-http-client/1.1")).isEqualTo("go")
        assertThat(clientClass("hoglake-server/1.4")).isEqualTo("hoglake")
        assertThat(clientClass("some-issuer/1")).isEqualTo("other")
    }

    @Test
    fun `a configured product token is its own class, matched exactly`() {
        val extra = setOf("some-issuer")
        assertThat(clientClass("some-issuer/1", extra)).isEqualTo("some-issuer")
        assertThat(clientClass("Some-Issuer/2 (provisioner)", extra)).isEqualTo("some-issuer")
        assertThat(clientClass("some-issuer", extra)).isEqualTo("some-issuer")
        // A prefix of the token is not the token: no free text reaches the label.
        assertThat(clientClass("some-issuer-x/1", extra)).isEqualTo("other")
        assertThat(clientClass("Go-http-client/1.1", extra)).isEqualTo("go")
    }

    @Test
    fun `configured client classes are bounded and validated at boot`() {
        assertThat(Config(authClientClasses = setOf("some-issuer")).authClientClasses).containsExactly("some-issuer")
        val tooMany = (1..Config.MAX_AUTH_CLIENT_CLASSES + 1).map { "client$it" }.toSet()
        val refused =
            listOf(setOf("Upper"), setOf("other"), setOf("none"), setOf("a b"), setOf("x".repeat(33)), tooMany)
        for (bad in refused) {
            assertThatThrownBy { Config(authClientClasses = bad) }
                .describedAs(bad.toString())
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("HOGLAKE_AUTH_CLIENT_CLASSES")
        }
    }

    @Test
    fun `the service labels with the configured classes, on both listeners`(
        @TempDir dir: Path,
    ) {
        val jwks = dir.resolve("jwks.json")
        Files.writeString(jwks, TestTokens.jwks(pair))
        val service =
            AuthService.fromConfig(
                Config(
                    authMode = "warn",
                    authIssuer = TestTokens.ISSUER,
                    authJwksFile = jwks.toString(),
                    authClientClasses = setOf("some-issuer"),
                ),
            )
        assertThat(service.clientClasses).containsExactly("some-issuer")
        assertThat(service.forPublicListener().clientClasses).containsExactly("some-issuer")
        assertThat(AuthService.fromConfig(Config(authClientClasses = setOf("some-issuer"))).clientClasses)
            .containsExactly("some-issuer")
    }
}
