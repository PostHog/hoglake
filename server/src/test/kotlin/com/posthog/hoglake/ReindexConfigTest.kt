package com.posthog.hoglake

import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * The reindex loop's one long-held connection is priced in the pool
 * budget (Config's background-draw `require`): a rebuild holds it for up
 * to an hour. Unit: a fact about a `require`.
 */
class ReindexConfigTest {
    @Test
    fun `the reindex loop's rebuild connection counts against the pool`() {
        // Exactly at budget without the reindex loop: 6 + 1 + reserve 4 = 11.
        val atBudget =
            { reindex: Long ->
                Config(
                    dbPoolSize = 11,
                    compactionParallelGroups = 6,
                    compactionIntervalMs = 3_600_000,
                    cleanupWorkers = 1,
                    cleanupIntervalMs = 60_000,
                    requestThreads = 11,
                    reindexIntervalMs = reindex,
                )
            }
        atBudget(0)
        // MUTATION: drop reindexDraw from the require and this reds.
        assertThatThrownBy { atBudget(300_000) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("a reindex draw of 1")
            .hasMessageContaining("needs a database pool of at least 12")
    }
}
