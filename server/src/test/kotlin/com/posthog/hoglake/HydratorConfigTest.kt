package com.posthog.hoglake

import com.posthog.hoglake.hoglite.HogliteHydrator
import com.posthog.hoglake.hydrator.FooterFetch
import com.posthog.hoglake.hydrator.Hydrator
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

/**
 * `HOGLAKE_HYDRATOR_MIN_FILE_BYTES` (#432): 1 MiB by default, 0 legal
 * (stats rows for every file), negative refused at boot; and
 * `HOGLAKE_HOGLITE_DATA_PATH_TEMPLATE` (#675), which must name the catalog;
 * and `HOGLAKE_HOGLITE_MAX_FOOTER_BYTES`, the Hoglite sweep's footer tail cap.
 * Unit: facts about defaults and `require`s.
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

    @Test
    fun `the Hoglite footer cap defaults to 64 MiB and holds the tail to one array`() {
        assertThat(Config().hogliteMaxFooterBytes)
            .isEqualTo(HogliteHydrator.DEFAULT_MAX_FOOTER_BYTES)
            .isEqualTo(67_108_864L)
        assertThat(Config(hogliteMaxFooterBytes = FooterFetch.MAX_FOOTER_BYTES).hogliteMaxFooterBytes)
            .isEqualTo(Int.MAX_VALUE - 8L)
        // MUTATION: drop the require and these construct; the tail of an over-Int footer cannot be read.
        for (cap in listOf(0L, FooterFetch.MAX_FOOTER_BYTES + 1)) {
            assertThatThrownBy { Config(hogliteMaxFooterBytes = cap) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("HOGLAKE_HOGLITE_MAX_FOOTER_BYTES")
        }
    }

    @Test
    fun `a Hoglite data path template names the catalog`() {
        assertThat(Config().hogliteDataPathTemplate).isEmpty()
        assertThat(Config(hogliteDataPathTemplate = "s3://warehouse/{catalog}").hogliteDataPathTemplate)
            .isEqualTo("s3://warehouse/{catalog}")
        // MUTATION: drop the require and both construct; one prefix for every catalog binds none.
        for (template in listOf("s3://warehouse/shared", "warehouse/{catalog}")) {
            assertThatThrownBy { Config(hogliteDataPathTemplate = template) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("HOGLAKE_HOGLITE_DATA_PATH_TEMPLATE")
        }
    }
}
