package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.HoglakeException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test

class HogliteCompactionPurgeTest {
    @Test
    fun `a raw interrupted purge restores the flag and does not become a warning`() {
        var warned = false
        try {
            Thread.interrupted()
            val error =
                catchThrowable {
                    bestEffortCompactionPurge({ throw InterruptedException("cancelled") }) { warned = true }
                }
            assertThat(error).isInstanceOf(InterruptedException::class.java)
            assertThat(Thread.currentThread().isInterrupted).isTrue()
            assertThat(warned).isFalse()
        } finally {
            Thread.interrupted()
        }
    }

    @Test
    fun `a client translated interruption also stops before another read`() {
        try {
            Thread.interrupted()
            val terminal = HoglakeException.BackendUnavailable("interrupted HTTP request")
            val error =
                catchThrowable {
                    bestEffortCompactionPurge({
                        Thread.currentThread().interrupt()
                        throw terminal
                    }) {
                        error("must not log and continue")
                    }
                }
            assertThat(error).isSameAs(terminal)
            assertThat(Thread.currentThread().isInterrupted).isTrue()
        } finally {
            Thread.interrupted()
        }
    }

    @Test
    fun `an ordinary purge failure remains best effort`() {
        val terminal = IllegalStateException("purge failed")
        var warning: Throwable? = null
        bestEffortCompactionPurge({ throw terminal }) { warning = it }
        assertThat(warning).isSameAs(terminal)
        assertThat(Thread.currentThread().isInterrupted).isFalse()
    }
}
