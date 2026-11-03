package com.posthog.hoglake

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

/**
 * `HOGLAKE_S3_PATH_STYLE` is parsed with `boolEnv`, which refuses anything
 * but `true`/`false`. It used to go through `String.toBoolean()`, which
 * read `1`, `yes` or `on` as FALSE: a deployment that set `=1` on a
 * path-style endpoint (MinIO) silently switched to virtual-hosted
 * addressing and every request failed on DNS.
 *
 * `Config` reads the real process environment and no test can set a
 * variable in its own JVM, so this boots [ConfigBootProbe] in a child JVM
 * that really has it set (the same technique as `RemovedEnvConfigTest`).
 */
class S3PathStyleEnvTest {
    private fun boot(value: String): Pair<Int, String> {
        val java = System.getProperty("java.home") + "/bin/java"
        val builder =
            ProcessBuilder(java, "-cp", System.getProperty("java.class.path"), ConfigBootProbe::class.java.name)
                .redirectErrorStream(true)
        builder.environment()["HOGLAKE_S3_PATH_STYLE"] = value
        val process = builder.start()
        val output = process.inputStream.bufferedReader().readText()
        assertThat(process.waitFor(120, TimeUnit.SECONDS))
            .describedAs("the probe JVM did not exit; output so far:%n%s", output)
            .isTrue()
        return process.exitValue() to output
    }

    @Test
    fun `a spelling that is not true or false is refused, naming the variable`() {
        // MUTATION: put `.toBoolean()` back and `1` boots as false.
        val (code, output) = boot("1")
        assertThat(code).describedAs("probe output:%n%s", output).isEqualTo(2)
        assertThat(output).contains("HOGLAKE_S3_PATH_STYLE must be 'true' or 'false', got '1'")
    }

    @Test
    fun `true and false both boot`() {
        for (value in listOf("true", "false", "TRUE")) {
            val (code, output) = boot(value)
            assertThat(code).describedAs("value %s, probe output:%n%s", value, output).isZero()
            assertThat(output).contains("CONSTRUCTED")
        }
    }
}
