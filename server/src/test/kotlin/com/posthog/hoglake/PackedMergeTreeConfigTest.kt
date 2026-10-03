package com.posthog.hoglake

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

class PackedMergeTreeConfigTest {
    private fun boot(value: String?): Pair<Int, String> {
        val java = System.getProperty("java.home") + "/bin/java"
        val builder =
            ProcessBuilder(java, "-cp", System.getProperty("java.class.path"), ConfigBootProbe::class.java.name)
                .redirectErrorStream(true)
        if (value == null) {
            builder.environment().remove("HOGLAKE_PACKED_MERGETREE_ENABLED")
        } else {
            builder.environment()["HOGLAKE_PACKED_MERGETREE_ENABLED"] = value
        }
        val process = builder.start()
        val output = process.inputStream.bufferedReader().readText()
        assertThat(process.waitFor(120, TimeUnit.SECONDS)).isTrue()
        return process.exitValue() to output
    }

    @Test
    fun `packed table creation rollout gate defaults off and can be enabled`() {
        assertThat(Config(packedMergeTreeEnabled = true).packedMergeTreeEnabled).isTrue()
        assertThat(boot(null)).isEqualTo(0 to "CONSTRUCTED packedMergeTreeEnabled=false\n")
        assertThat(boot("true")).isEqualTo(0 to "CONSTRUCTED packedMergeTreeEnabled=true\n")
    }

    @Test
    fun `packed table creation rollout gate refuses ambiguous boolean spellings`() {
        val (code, output) = boot("yes")
        assertThat(code).isEqualTo(2)
        assertThat(output)
            .contains("HOGLAKE_PACKED_MERGETREE_ENABLED must be 'true' or 'false', got 'yes'")
    }
}
