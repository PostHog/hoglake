package com.posthog.hoglake

import com.posthog.hoglake.hydrator.Hydrator
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * `HOGLAKE_HYDRATOR_MIN_FILE_BYTES` (#432): 1 MiB by default, 0 legal
 * (stats rows for every file), negative refused at boot. Unit: facts about a
 * default and a `require`.
 */
class HydratorConfigTest {
    @Test
    fun `the floor defaults to 1 MiB`() {
        assertThat(Hydrator.DEFAULT_MIN_FILE_BYTES).isEqualTo(1_048_576L)
        assertThat(Config().hydratorMinFileBytes).isEqualTo(Hydrator.DEFAULT_MIN_FILE_BYTES)
    }

    @Test
    fun `floor 0 is legal and a negative floor is refused`() {
        assertThat(Config(hydratorMinFileBytes = 0).hydratorMinFileBytes).isZero()
        // MUTATION: drop the require and this constructs.
        assertThatThrownBy { Config(hydratorMinFileBytes = -1) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_HYDRATOR_MIN_FILE_BYTES=-1 must not be negative")
    }
}
