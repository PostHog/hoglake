package com.posthog.hoglake.api

import com.posthog.hoglake.auth.AuthState
import com.posthog.hoglake.auth.Caller
import com.posthog.hoglake.auth.Scope
import com.posthog.hoglake.model.HoglakeException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.io.File

/**
 * A commit's `author` goes through the same rule as every other write.
 *
 * The bug this pins: the six commit routes read `CommitRequestDto` straight
 * off the wire, so under `enforce` a tenant token could record any author on
 * a data commit, and an omitted author stayed null rather than becoming the
 * token's subject. DDL, views and maintenance already went through
 * `actorOrRefuse`; commits did not.
 */
class CommitAttributionTest {
    private fun tenant(delegates: Boolean = false) =
        AuthState.Authenticated(
            Caller(
                "hlc_0123456789abcdef01234567",
                "org-1",
                Caller.Kind.TENANT,
                mapOf("lake" to setOf(Scope.WRITE)),
                "jti",
                "duckdb-client",
                delegates = delegates,
            ),
        )

    private fun commit(author: String?) = CommitRequestDto(author = author)

    @Test
    fun `an unattributed tenant commit is recorded as the token subject`() {
        assertThat(commit(null).attributed(tenant()).author).isEqualTo("hlc_0123456789abcdef01234567")
    }

    @Test
    fun `a tenant cannot record somebody else as a commit's author`() {
        assertThatThrownBy { commit("someone-else").attributed(tenant()) }
            .isInstanceOf(HoglakeException.Validation::class.java)
    }

    @Test
    fun `a delegating client's commit names the end user and who vouched for them`() {
        assertThat(commit("alice").attributed(tenant(delegates = true)).author)
            .isEqualTo("alice via hlc_0123456789abcdef01234567")
    }

    @Test
    fun `with auth off the body's author stands`() {
        assertThat(commit("etl-bot").attributed(AuthState.Off).author).isEqualTo("etl-bot")
    }

    @Test
    fun `no route reads a commit body around the attribution`() {
        // MUTATION: change any commit route back to call.receive<CommitRequestDto>()
        // and this reds.
        val offenders =
            File("src/main/kotlin/com/posthog/hoglake")
                .walkTopDown()
                .filter { it.isFile && it.extension == "kt" }
                .filter { file ->
                    file.readLines().any { line ->
                        val code = line.trim()
                        !code.startsWith("*") && !code.startsWith("//") &&
                            "receive<CommitRequestDto>()" in code && "attributed(" !in code
                    }
                }
                .map { it.name }
                .toList()
        assertThat(offenders).describedAs("files reading CommitRequestDto without attribution").isEmpty()
    }
}
