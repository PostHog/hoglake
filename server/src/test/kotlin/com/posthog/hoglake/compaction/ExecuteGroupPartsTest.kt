package com.posthog.hoglake.compaction

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * The one decision `executeGroup` makes between the parts of a split
 * group: when to stop. Pinned here because it is not reachable from the
 * outside — `heapExhausted` is the sweep's own flag, and an interrupt set
 * from a test seam is swallowed by the connection pool or the object store
 * before the group is reached — and because getting it wrong is silent: a
 * check before the first part turned a sibling's heap exhaustion into an
 * ordinary group that did nothing, spent no attempt, and released its
 * claim.
 */
class ExecuteGroupPartsTest {
    @Test
    fun `the first part always runs, whatever the sweep's state`() {
        // A group of a table with no variant is exactly one part, so this is
        // the pre-split behaviour: a group past its claim refresh runs.
        assertThat(CompactionService.yieldsBeforePart(0, heapExhausted = false, interrupted = false)).isFalse()
        assertThat(CompactionService.yieldsBeforePart(0, heapExhausted = true, interrupted = false)).isFalse()
        assertThat(CompactionService.yieldsBeforePart(0, heapExhausted = false, interrupted = true)).isFalse()
        assertThat(CompactionService.yieldsBeforePart(0, heapExhausted = true, interrupted = true)).isFalse()
    }

    @Test
    fun `later parts yield to an exhausted heap or a cancelled sweep, and otherwise run`() {
        for (index in 1..3) {
            assertThat(CompactionService.yieldsBeforePart(index, heapExhausted = false, interrupted = false)).isFalse()
            assertThat(CompactionService.yieldsBeforePart(index, heapExhausted = true, interrupted = false)).isTrue()
            assertThat(CompactionService.yieldsBeforePart(index, heapExhausted = false, interrupted = true)).isTrue()
        }
    }
}
