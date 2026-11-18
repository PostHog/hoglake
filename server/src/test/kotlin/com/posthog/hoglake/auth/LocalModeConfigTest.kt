package com.posthog.hoglake.auth

import com.posthog.hoglake.Config
import com.posthog.hoglake.SecretString
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

/** The `local` boot matrix: the key goes with the mode and with nothing else. */
class LocalModeConfigTest {
    private val key = LocalSigningKey.generate()
    private val secret = SecretString(key.encode())

    @Test
    fun `local is a mode, and it enforces`() {
        assertThat(AuthMode.parse("HOGLAKE_AUTH_MODE", "local")).isEqualTo(AuthMode.LOCAL)
        assertThat(AuthMode.LOCAL.isEnforcing).isTrue()
        assertThat(AuthMode.LOCAL.resolvesCallers).isTrue()
        assertThatThrownBy { AuthMode.parse("HOGLAKE_AUTH_MODE", "locale") }
            .hasMessageContaining("off, warn, enforce, local")
    }

    @Test
    fun `local without the key fails boot with the keygen hint`() {
        assertThatThrownBy { Config(authMode = "local") }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_AUTH_SIGNING_KEY")
            .hasMessageContaining("hoglake token keygen")
    }

    @Test
    fun `local refuses a JWKS file or URL beside the key`(
        @TempDir dir: Path,
    ) {
        val jwks = dir.resolve("jwks.json")
        Files.writeString(jwks, key.publicJwks())
        assertThatThrownBy { Config(authMode = "local", authSigningKey = secret, authJwksFile = jwks.toString()) }
            .hasMessageContaining("HOGLAKE_AUTH_JWKS_FILE")
        assertThatThrownBy {
            Config(
                authMode = "local",
                authSigningKey = secret,
                authJwksUrl = "https://issuer.example/jwks",
            )
        }
            .hasMessageContaining("HOGLAKE_AUTH_JWKS_URL")
    }

    @Test
    fun `the key is refused under every other mode`() {
        for (mode in listOf("off", "warn", "enforce")) {
            assertThatThrownBy { Config(authMode = mode, authSigningKey = secret) }
                .describedAs(mode)
                .hasMessageContaining("HOGLAKE_AUTH_SIGNING_KEY is read only under HOGLAKE_AUTH_MODE=local")
        }
    }

    @Test
    fun `a bad key fails boot naming the variable, not the value`() {
        assertThatThrownBy {
            AuthService.fromConfig(
                Config(authMode = "local", authSigningKey = SecretString("not-a-key")),
            )
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_AUTH_MODE=local")
            .hasMessageContaining("HOGLAKE_AUTH_SIGNING_KEY")
    }

    @Test
    fun `the config never prints the key`() {
        val cfg = Config(authMode = "local", authSigningKey = secret)
        assertThat(cfg.toString()).doesNotContain(key.encode()).contains("<redacted>")
        assertThat(SecretString("").toString()).isEmpty()
        assertThat(SecretString("a")).isEqualTo(SecretString("a")).isNotEqualTo(SecretString("b"))
    }

    @Test
    fun `local builds an enforcing verifier on the key, with the default issuer, and publishes the JWKS`() {
        val service = AuthService.fromConfig(Config(authMode = "local", authSigningKey = secret))
        assertThat(service.mode).isEqualTo(AuthMode.LOCAL)
        assertThat(service.localJwks).isEqualTo(key.publicJwks())
        assertThat(service.forPublicListener().localJwks).isEqualTo(key.publicJwks())
        val token =
            LocalIssuer(
                key,
                LocalIssuer.DEFAULT_LOCAL_ISSUER,
            ).tenant("me", "local", mapOf("demo" to Scope.WRITE), "cli", 60)
        val verifier = service.verifier!!
        val caller = verifier.verify(token)
        assertThat(caller.kind).isEqualTo(Caller.Kind.TENANT)
        assertThat(caller.catalogScopes).isEqualTo(mapOf("demo" to setOf(Scope.WRITE)))
        // Another key's token is not ours, even with the right kid shape.
        val other = LocalSigningKey.generate()
        val forged =
            LocalIssuer(
                other,
                LocalIssuer.DEFAULT_LOCAL_ISSUER,
            ).tenant("me", "local", mapOf("demo" to Scope.WRITE), "cli", 60)
        assertThatThrownBy { verifier.verify(forged) }.isInstanceOf(JwtViolation::class.java)
    }

    @Test
    fun `an explicit issuer wins over the default`() {
        val service =
            AuthService.fromConfig(
                Config(authMode = "local", authSigningKey = secret, authIssuer = "https://standalone.example"),
            )
        val verifier = service.verifier!!
        val right =
            LocalIssuer(
                key,
                "https://standalone.example",
            ).tenant("me", "local", mapOf("demo" to Scope.READ), "cli", 60)
        assertThat(verifier.verify(right).kind).isEqualTo(Caller.Kind.TENANT)
        val wrong =
            LocalIssuer(
                key,
                LocalIssuer.DEFAULT_LOCAL_ISSUER,
            ).tenant("me", "local", mapOf("demo" to Scope.READ), "cli", 60)
        assertThatThrownBy { verifier.verify(wrong) }
            .isInstanceOfSatisfying(
                JwtViolation::class.java,
            ) { assertThat(it.reason).isEqualTo(JwtViolation.WRONG_ISSUER) }
    }

    @Test
    fun `no other mode publishes a JWKS`() {
        assertThat(AuthService.fromConfig(Config()).localJwks).isNull()
    }
}
