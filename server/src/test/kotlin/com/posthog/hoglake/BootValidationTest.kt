package com.posthog.hoglake

import com.posthog.hoglake.hoglite.HogliteCatalogStore
import com.posthog.hoglake.hoglite.HogliteClient
import com.posthog.hoglake.observability.Metrics
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatCode
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.sql.SQLException
import java.util.concurrent.TimeUnit

/**
 * A setting [Config] does not check but a service constructor refuses
 * must refuse BOOT, on both backends: Main builds the App before it
 * starts a loop, binds the metrics port or serves the API, so a refusal
 * thrown from [App.build] / [App.buildHoglite] exits the process. A
 * service built lazily on first use would instead leave a running
 * process failing every loop tick, or (for a service first touched in
 * [App.module]) a JVM with loops and metrics up and no API port.
 */
class BootValidationTest {
    @AfterEach
    fun unbind() = Metrics.clear()

    private val noDb = Jdbi.create { throw SQLException("no database in this test") }

    private fun postgres(cfg: Config): App = App.build(cfg, noDb)

    private fun hoglite(cfg: Config): App =
        App.buildHoglite(
            cfg,
            HogliteCatalogStore(HogliteClient(standardUrl = "http://127.0.0.1:1")),
            HealthProbe.forProbe(1_000) { false },
        )

    private val hogliteBase = Config(database = DatabaseBackend.HOGLITE, hogliteUrl = "http://127.0.0.1:1")

    // ---- Postgres: one bad setting per service that checks its own -------

    @Test
    fun `a retirement batch of 0 refuses a Postgres boot`() {
        assertThatThrownBy { postgres(Config(retirementBatch = 0)) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("retirement batch")
    }

    @Test
    fun `a summary refresh of 0 refuses a Postgres boot`() {
        assertThatThrownBy { postgres(Config(maintenanceSummaryRefreshSeconds = 0)) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `a compaction minInputFiles of 1 refuses a Postgres boot`() {
        assertThatThrownBy { postgres(Config(compactionMinInputFiles = 1)) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_COMPACTION_MIN_INPUT_FILES")
    }

    @Test
    fun `a cleanup sub-batch of 0 refuses a Postgres boot`() {
        assertThatThrownBy { postgres(Config(cleanupSubBatchSize = 0)) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("subBatchSize")
    }

    @Test
    fun `a refused Postgres boot binds no metrics`() {
        // the refusal comes before buildShared binds the registry: nothing
        // of a half-built App is reachable
        assertThatThrownBy { postgres(Config(retirementBatch = 0)) }
        assertThat(Metrics.boundRegistry).isNull()
    }

    // ---- hoglite: the same settings, through its own services -----------

    @Test
    fun `a retirement batch of 0 refuses a hoglite boot`() {
        assertThatThrownBy { hoglite(hogliteBase.copy(retirementBatch = 0)) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("retirement batch")
    }

    @Test
    fun `a compaction minInputFiles of 1 refuses a hoglite boot`() {
        assertThatThrownBy { hoglite(hogliteBase.copy(compactionMinInputFiles = 1)) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_COMPACTION_MIN_INPUT_FILES")
    }

    // ---- every service is built at boot, not on first use ---------------

    private fun lazies(app: App): Map<String, Lazy<*>> =
        App::class.java.declaredFields
            .filter { Lazy::class.java.isAssignableFrom(it.type) }
            .associate {
                it.isAccessible = true
                it.name.removeSuffix("\$delegate") to it.get(app) as Lazy<*>
            }

    @Test
    fun `a Postgres build constructs every Postgres service and no hoglite one`() {
        val all = lazies(postgres(Config()))
        assertThat(all).hasSizeGreaterThan(20)
        val (hogliteOnes, postgresOnes) = all.entries.partition { it.key.startsWith("hoglite") }
        assertThat(postgresOnes.filterNot { it.value.isInitialized() }.map { it.key }).isEmpty()
        assertThat(hogliteOnes.filter { it.value.isInitialized() }.map { it.key }).isEmpty()
    }

    @Test
    fun `a hoglite build constructs every hoglite service and no Postgres one`() {
        val all = lazies(hoglite(hogliteBase))
        val (hogliteOnes, others) = all.entries.partition { it.key.startsWith("hoglite") || it.key == "store" }
        assertThat(hogliteOnes).hasSizeGreaterThan(8)
        assertThat(hogliteOnes.filterNot { it.value.isInitialized() }.map { it.key }).isEmpty()
        // the Postgres services need a Jdbi a hoglite App does not have
        assertThat(others.filter { it.value.isInitialized() }.map { it.key }).isEmpty()
    }

    // ---- HOGLAKE_HOGLITE_* belongs to the hoglite backend alone ----------

    @Test
    fun `invalid hoglite settings do not refuse a Postgres config, and do refuse a hoglite one`() {
        val bad: (Config) -> Config = {
            it.copy(
                hogliteOwnerUrlPattern = "http://no-placeholder",
                hogliteConnectTimeoutMs = 0,
                hogliteCircuitOpenMs = 0,
            )
        }
        assertThatCode { bad(Config()) }.doesNotThrowAnyException()
        assertThatThrownBy { bad(hogliteBase) }.isInstanceOf(IllegalArgumentException::class.java)
    }

    /** [ConfigBootProbe] in a child JVM with [env]; the exit code and stdout. */
    private fun boot(vararg env: Pair<String, String>): Pair<Int, String> {
        val java = System.getProperty("java.home") + "/bin/java"
        val builder =
            ProcessBuilder(java, "-cp", System.getProperty("java.class.path"), ConfigBootProbe::class.java.name)
                .redirectErrorStream(true)
        builder.environment().putAll(env.toMap())
        val process = builder.start()
        val output = process.inputStream.bufferedReader().readText()
        assertThat(process.waitFor(120, TimeUnit.SECONDS)).describedAs(output).isTrue()
        return process.exitValue() to output
    }

    @Test
    fun `a malformed HOGLAKE_HOGLITE value does not refuse a Postgres boot, and refuses a hoglite one`() {
        val malformed =
            arrayOf(
                "HOGLAKE_HOGLITE_RETRY_BUDGET_MS" to "8s",
                "HOGLAKE_HOGLITE_CIRCUIT_FAILURE_THRESHOLD" to "ten",
                "HOGLAKE_HOGLITE_OWNER_URL_PATTERN" to "http://no-placeholder",
            )
        val (pgCode, pgOut) = boot(*malformed)
        assertThat(pgCode).describedAs(pgOut).isEqualTo(0)
        val (hlCode, hlOut) =
            boot(*malformed, "HOGLAKE_DATABASE" to "hoglite", "HOGLAKE_HOGLITE_URL" to "http://127.0.0.1:1")
        assertThat(hlCode).describedAs(hlOut).isEqualTo(2)
    }
}
