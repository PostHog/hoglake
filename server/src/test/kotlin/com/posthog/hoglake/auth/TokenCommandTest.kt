package com.posthog.hoglake.auth

import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.Config
import com.posthog.hoglake.SecretString
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.Base64

/**
 * `hoglake token keygen` and `hoglake token mint`, driven in-process: what
 * they print, how they fail, and that a minted token verifies under a server
 * booted from the same environment.
 */
class TokenCommandTest {
    // Real time: the minted token is verified by a real AuthService, whose
    // clock is the wall clock.
    private val now = System.currentTimeMillis() / 1000

    private class Run(
        val exit: Int,
        val out: String,
        val err: String,
    )

    private fun run(
        vararg args: String,
        env: Map<String, String> = emptyMap(),
    ): Run {
        val out = StringBuilder()
        val err = StringBuilder()
        val exit = TokenCommand.run(args.toList(), { env[it] }, out, err) { now }
        return Run(exit, out.toString(), err.toString())
    }

    /** keygen's line, as `export $(hoglake token keygen)` would see it. */
    private fun keygen(): Map<String, String> {
        val r = run("keygen")
        assertThat(r.exit).isEqualTo(0)
        assertThat(r.err).isEmpty()
        assertThat(r.out).matches("HOGLAKE_AUTH_SIGNING_KEY=[A-Za-z0-9_-]+\n")
        val (k, v) = r.out.trim().split('=', limit = 2)
        return mapOf(k to v)
    }

    private fun claims(token: String) = ObjectMapper().readTree(Base64.getUrlDecoder().decode(token.split('.')[1]))

    @Test
    fun `keygen prints one env line and nothing else, fresh every time`() {
        val a = keygen()
        val b = keygen()
        assertThat(a.values.single()).isNotEqualTo(b.values.single())
        assertThat(run("keygen", "extra").exit).isEqualTo(2)
    }

    @Test
    fun `a minted token verifies under a server booted from the same environment`() {
        val env = keygen()
        val r = run("mint", "--catalog", "demo", "--scope", "write", env = env)
        assertThat(r.exit).describedAs(r.err).isEqualTo(0)
        assertThat(r.err).isEmpty()
        val token = r.out.trim()
        assertThat(r.out).isEqualTo("$token\n")
        assertThat(token.split('.')).hasSize(3)

        val server =
            AuthService.fromConfig(
                Config(authMode = "local", authSigningKey = SecretString(env.getValue("HOGLAKE_AUTH_SIGNING_KEY"))),
            )
        val caller = server.verifier!!.verify(token)
        assertThat(caller.kind).isEqualTo(Caller.Kind.TENANT)
        assertThat(caller.catalogScopes).isEqualTo(mapOf("demo" to setOf(Scope.WRITE)))
        assertThat(caller.subject).isEqualTo("local")
        assertThat(caller.orgId).isEqualTo("local")
        assertThat(caller.clientName).isEqualTo("hoglake-token")
        val c = claims(token)
        assertThat(c["iss"].asText()).isEqualTo("hoglake")
        assertThat(c["aud"].asText()).isEqualTo("hoglake")
        assertThat(c["exp"].asLong() - c["iat"].asLong()).isEqualTo(24 * 3_600L)
    }

    @Test
    fun `the issuer and audience follow the environment the server reads`() {
        val env =
            keygen() +
                mapOf("HOGLAKE_AUTH_ISSUER" to "https://standalone.example", "HOGLAKE_AUTH_AUDIENCE" to "lake")
        val token = run("mint", "--catalog", "demo", env = env).out.trim()
        assertThat(claims(token)["iss"].asText()).isEqualTo("https://standalone.example")
        assertThat(claims(token)["aud"].asText()).isEqualTo("lake")
        val server =
            AuthService.fromConfig(
                Config(
                    authMode = "local",
                    authSigningKey = SecretString(env.getValue("HOGLAKE_AUTH_SIGNING_KEY")),
                    authIssuer = "https://standalone.example",
                    authAudience = "lake",
                ),
            )
        assertThat(server.verifier!!.verify(token).kind).isEqualTo(Caller.Kind.TENANT)
    }

    @Test
    fun `per-catalog scopes, the subject, org, client and ttl are honoured`() {
        val env = keygen()
        val token =
            run(
                "mint", "--catalog", "a=admin", "--catalog", "b", "--scope", "read",
                "--sub", "alice", "--org", "acme", "--client", "duckdb", "--ttl", "15m",
                env = env,
            ).out.trim()
        val c = claims(token)
        assertThat(c["sub"].asText()).isEqualTo("alice")
        assertThat(c["org"].asText()).isEqualTo("acme")
        assertThat(c["cli"].asText()).isEqualTo("duckdb")
        assertThat(c["cat"]["a"][0].asText()).isEqualTo("admin")
        assertThat(c["cat"]["b"][0].asText()).isEqualTo("read")
        assertThat(c["exp"].asLong() - c["iat"].asLong()).isEqualTo(900)
        assertThat(c["iat"].asLong()).isEqualTo(now)
    }

    @Test
    fun `an operator token, fleet-wide or bound to one catalog`() {
        val env = keygen()
        val server =
            AuthService.fromConfig(
                Config(authMode = "local", authSigningKey = SecretString(env.getValue("HOGLAKE_AUTH_SIGNING_KEY"))),
            )
        val fleet = run("mint", "--operator", env = env).out.trim()
        val verifier = server.verifier!!
        assertThat(verifier.verify(fleet).kind).isEqualTo(Caller.Kind.OPERATOR)
        assertThat(claims(fleet)["cat"]).isNull()
        val bound = run("mint", "--operator", "--catalog", "demo", env = env).out.trim()
        assertThat(verifier.verify(bound).kind).isEqualTo(Caller.Kind.OPERATOR)
        assertThat(claims(bound)["cat"]["demo"][0].asText()).isEqualTo("admin")
        assertThat(claims(bound)["org"].asText()).isEqualTo("local")
        val two = run("mint", "--operator", "--catalog", "a", "--catalog", "b", env = env)
        assertThat(two.exit).isEqualTo(2)
        assertThat(two.err).contains("at most one --catalog")
    }

    @Test
    fun `mint without the key fails naming the variable, with nothing on stdout`() {
        val r = run("mint", "--catalog", "demo")
        assertThat(r.exit).isEqualTo(1)
        assertThat(r.out).isEmpty()
        assertThat(r.err).contains("HOGLAKE_AUTH_SIGNING_KEY").contains("keygen")
        val bad = run("mint", "--catalog", "demo", env = mapOf("HOGLAKE_AUTH_SIGNING_KEY" to "nope"))
        assertThat(bad.exit).isEqualTo(1)
        assertThat(bad.err).contains("HOGLAKE_AUTH_SIGNING_KEY").doesNotContain("nope")
    }

    @Test
    fun `usage errors exit 2 with the usage text, and help exits 0`() {
        val env = keygen()
        for (
        args in
        listOf(
            listOf("mint"),
            listOf("mint", "--catalog"),
            listOf("mint", "--catalog", "demo", "--scope", "root"),
            listOf("mint", "--catalog", "demo", "--ttl", "soon"),
            listOf("mint", "--catalog", "demo", "--ttl", "0"),
            listOf("mint", "--catalog", "demo", "--bogus"),
            listOf("frobnicate"),
            emptyList(),
        )
        ) {
            val r = run(*args.toTypedArray(), env = env)
            assertThat(r.exit).describedAs(args.toString()).isEqualTo(2)
            assertThat(r.out).describedAs(args.toString()).isEmpty()
            assertThat(r.err).describedAs(args.toString()).contains("usage:")
        }
        assertThat(run("--help").exit).isEqualTo(0)
        assertThat(run("help").err).contains("usage:")
    }

    @Test
    fun `durations`() {
        assertThat(TokenCommand.parseDuration("90")).isEqualTo(90)
        assertThat(TokenCommand.parseDuration("90s")).isEqualTo(90)
        assertThat(TokenCommand.parseDuration("15m")).isEqualTo(900)
        assertThat(TokenCommand.parseDuration("24h")).isEqualTo(86_400)
        assertThat(TokenCommand.parseDuration("7d")).isEqualTo(604_800)
        for (bad in listOf("", "0", "-5", "1.5h", "1w", "h")) {
            assertThatThrownBy {
                TokenCommand.parseDuration(bad)
            }.describedAs(bad).isInstanceOf(IllegalArgumentException::class.java)
        }
    }
}
