package com.posthog.hoglake

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

/**
 * The one spelling rule for boolean settings ([Config.parseBoolSetting]),
 * which the hoglite engine's `boolSetting` (cmd/hoglited) follows too:
 * `true/1/yes/on` and `false/0/no/off`, case-insensitive; anything else
 * refuses boot, naming the variable and the accepted spellings.
 *
 * The cases are the engine's table, row for row, so the two sides cannot
 * drift without one of the tables being edited on its own.
 */
class BoolSettingConfigTest {
    @Test
    fun `every accepted spelling parses to what it says`() {
        // MUTATION: revert to `toBooleanStrictOrNull` (true/false only) and
        // the `1`, `yes`, `on`, `0`, `no`, `off` rows throw.
        val cases =
            listOf(
                "true" to true, "1" to true, "TRUE" to true, "yes" to true, "on" to true,
                "True" to true, " on " to true, "YES" to true,
                "false" to false, "0" to false, "no" to false, "off" to false,
                "FALSE" to false, "Off" to false, " 0 " to false,
            )
        for ((raw, want) in cases) {
            assertThat(Config.parseBoolSetting("HOGLAKE_X", raw)).describedAs("'%s'", raw).isEqualTo(want)
        }
    }

    @Test
    fun `anything else is refused, naming the variable, the accepted values and the value`() {
        // MUTATION: map unknown spellings to false (`String.toBoolean()`)
        // and nothing throws: the operator believes the flag is on.
        for (raw in listOf("enabled", "2", "-1", "t", "y", "truee", "on1", "nope")) {
            assertThatThrownBy { Config.parseBoolSetting("HOGLAKE_X", raw) }
                .describedAs("'%s'", raw)
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("HOGLAKE_X")
                .hasMessageContaining("true/1/yes/on or false/0/no/off")
                .hasMessageContaining("'$raw'")
        }
    }

    // ---- the wiring, in a process that really has the variable set ---------

    private fun boot(vararg env: Pair<String, String>): Pair<Int, String> {
        val java = System.getProperty("java.home") + "/bin/java"
        val builder =
            ProcessBuilder(
                java,
                "-cp",
                System.getProperty("java.class.path"),
                ConfigBootProbe::class.java.name,
                "hogliteInteriorPlanReads",
            ).redirectErrorStream(true)
        builder.environment().remove("HOGLAKE_HOGLITE_INTERIOR_PLAN_READS")
        builder.environment().putAll(mapOf("HOGLAKE_HOGLITE_URL" to "http://127.0.0.1:1") + env.toMap())
        val process = builder.start()
        val output = process.inputStream.bufferedReader().readText()
        assertThat(process.waitFor(120, TimeUnit.SECONDS))
            .describedAs("the probe JVM did not exit; output so far:%n%s", output)
            .isTrue()
        return process.exitValue() to output
    }

    @Test
    fun `HOGLAKE_HOGLITE_INTERIOR_PLAN_READS=1 enables both hoglite modes but not postgres`() {
        for (backend in listOf("hoglite", "mixed", "postgres")) {
            val (code, output) =
                boot("HOGLAKE_DATABASE" to backend, "HOGLAKE_HOGLITE_INTERIOR_PLAN_READS" to "1")
            assertThat(code).describedAs("%s; probe output:%n%s", backend, output).isZero()
            assertThat(output).contains("hogliteInteriorPlanReads=${backend != "postgres"}")
        }
    }

    @Test
    fun `HOGLAKE_HOGLITE_INTERIOR_PLAN_READS=garbage refuses boot, whatever the backend`() {
        for (backend in listOf("hoglite", "mixed", "postgres")) {
            val (code, output) =
                boot("HOGLAKE_DATABASE" to backend, "HOGLAKE_HOGLITE_INTERIOR_PLAN_READS" to "enabled")
            assertThat(code).describedAs("%s; probe output:%n%s", backend, output).isEqualTo(2)
            assertThat(output)
                .contains("HOGLAKE_HOGLITE_INTERIOR_PLAN_READS must be one of true/1/yes/on or false/0/no/off")
                .contains("got 'enabled'")
        }
    }

    @Test
    fun `unset and explicitly off remain off in every mode`() {
        for (backend in listOf("hoglite", "mixed", "postgres")) {
            for (setting in listOf(null, "0")) {
                val env = mutableListOf("HOGLAKE_DATABASE" to backend)
                if (setting != null) env += "HOGLAKE_HOGLITE_INTERIOR_PLAN_READS" to setting
                val (code, output) = boot(*env.toTypedArray())
                assertThat(code).describedAs("%s/%s; probe output:%n%s", backend, setting, output).isZero()
                assertThat(output).contains("hogliteInteriorPlanReads=false")
            }
        }
    }
}
