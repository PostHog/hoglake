package com.posthog.hoglake.auth

import com.posthog.hoglake.model.HoglakeException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * The `author` adjudication: the one rule that decides what the audit trail says
 * about who did something.
 *
 * Three shapes, and only the third is interesting: with no caller the caller's
 * word stands (the pre-auth behaviour, kept byte-identical for `off`), an
 * operator may attribute work elsewhere, and a tenant may not name anybody but
 * itself.
 */
class ResolveActorTest {
    private fun tenant(subject: String = "client:hedgerow-prod") =
        Caller(subject, "org-1", Caller.Kind.TENANT, mapOf("lake" to setOf(Scope.READ)), "jti", "hedgerow")

    private val operator = Caller("sa:bulk", null, Caller.Kind.OPERATOR, emptyMap(), "jti", null)

    private fun authed(caller: Caller) = AuthState.Authenticated(caller)

    @Test
    fun `with auth off the claimed author is taken as-is`() {
        assertThat(resolveActor(AuthState.Off, "etl-bot")).isEqualTo("etl-bot")
        assertThat(resolveActor(AuthState.Off, null)).isNull()
    }

    @Test
    fun `an anonymous request under warn keeps its claim rather than being refused`() {
        // Only reachable in `warn`, which passes refusals through. Refusing here
        // would turn the burn-in mode into an outage for every client it exists
        // to inventory.
        assertThat(resolveActor(AuthState.Anonymous, "legacy-etl")).isEqualTo("legacy-etl")
    }

    @Test
    fun `a tenant's own subject passes through`() {
        assertThat(resolveActor(authed(tenant()), "client:hedgerow-prod")).isEqualTo("client:hedgerow-prod")
    }

    @Test
    fun `an omitted author is filled from the token, not left anonymous`() {
        // The failure this prevents: `actor` defaulting to "anonymous" for every
        // client that never sent one, which is every machine client there is.
        assertThat(resolveActor(authed(tenant()), null)).isEqualTo("client:hedgerow-prod")
    }

    @Test
    fun `a tenant claiming somebody else's name is refused, not silently rewritten`() {
        assertThatThrownBy { resolveActor(authed(tenant()), "someone-else") }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("not permitted")
    }

    @Test
    fun `an operator may attribute work to a component`() {
        assertThat(resolveActor(authed(operator), "backfill-2024")).isEqualTo("backfill-2024")
    }

    @Test
    fun `an operator with no claim still lands under its own identity`() {
        assertThat(resolveActor(authed(operator), null)).isEqualTo("operator:sa:bulk")
    }

    // ---- delegation --------------------------------------------------------

    private fun engine() =
        Caller(
            "client:trino-org1",
            "org-1",
            Caller.Kind.TENANT,
            mapOf("lake" to setOf(Scope.WRITE)),
            "jti",
            "trino",
            true,
        )

    @Test
    fun `a delegating client names its end user, and stays on the record beside it`() {
        assertThat(resolveActor(authed(engine()), "alice")).isEqualTo("alice via client:trino-org1")
    }

    @Test
    fun `a delegating client with no claim is recorded as itself`() {
        assertThat(resolveActor(authed(engine()), null)).isEqualTo("client:trino-org1")
        assertThat(resolveActor(authed(engine()), "client:trino-org1")).isEqualTo("client:trino-org1")
    }

    @Test
    fun `a delegated claim cannot forge a chain or smuggle control characters`() {
        for (claim in listOf("bob via client:other", "alice\nforged", " ", "x".repeat(201))) {
            assertThatThrownBy { resolveActor(authed(engine()), claim) }
                .describedAs(claim)
                .isInstanceOf(HoglakeException.Validation::class.java)
        }
    }
}
