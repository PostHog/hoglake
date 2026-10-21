package com.posthog.hoglake.observability

import com.posthog.hoglake.model.CatalogNotEmptyReason
import com.posthog.hoglake.model.HoglakeException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * The audit outcome of the precondition refusals (#412 review). A refusal
 * that falls through `Audit.failureOutcome`'s `else` audits as `error`,
 * i.e. as a server fault, and NamespaceNotEmpty did exactly that.
 * MUTATION: dropping any of the three from the `refused` arm reds here.
 *
 * The end-to-end half — the line a real refused namespace or catalog drop
 * emits — is in `CatalogDropApiIntegrationTest`.
 */
class AuditFailureOutcomeTest {
    @Test
    fun `precondition refusals audit as refused, never as error`() {
        for (
        refusal in listOf(
            HoglakeException.CatalogNotEmpty("d", CatalogNotEmptyReason.LIVE_NAMESPACES),
            HoglakeException.NamespaceNotEmpty("d"),
            HoglakeException.IdlessFilesPresent("d"),
        )
        ) {
            assertThat(Audit.failureOutcome(refusal)).describedAs("%s", refusal::class.simpleName).isEqualTo("refused")
        }
    }

    @Test
    fun `a genuine fault is still error`() {
        assertThat(Audit.failureOutcome(IllegalStateException("boom"))).isEqualTo("error")
    }
}
