package com.posthog.hoglake.store

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.posthog.hoglake.Config
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID

class BackendConfigTest {
    @TempDir
    lateinit var dir: Path

    private val id = "5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e"
    private val entry = """{"id":"$id","kind":"hoglite","url":"http://localhost:8080"}"""

    private fun config(
        entries: String = entry,
        auth: String = "enforce",
    ): Config {
        val file = dir.resolve("backends.json")
        Files.writeString(file, """{"backends":[$entries]}""")
        return Config(backendsFile = file.toString(), authMode = auth)
    }

    @Test
    fun `gigahog keeps postgres as its default and has no extra backends`() {
        val cfg = Config(backendsFile = "", postgresBackendId = "", defaultBackend = "postgres")
        assertThat(cfg.hogliteBackends).isEmpty()
        assertThat(cfg.backendKinds).isEmpty()
    }

    @Test
    fun `backend defaults and ids feed the verifier allowlist`() {
        val cfg = config()
        assertThat(cfg.backendKinds).containsExactlyEntriesOf(mapOf(UUID.fromString(id) to BackendKind.HOGLITE))
        val backend = cfg.hogliteBackends.getValue(UUID.fromString(id))
        assertThat(backend.url).isEqualTo("http://localhost:8080")
        assertThat(backend.connectTimeoutMs).isEqualTo(2000)
        assertThat(backend.readTimeoutMs).isEqualTo(5000)
        assertThat(backend.commitTimeoutMs).isEqualTo(10000)
        assertThat(backend.retryBudgetMs).isEqualTo(8000)
        assertThat(backend.circuitFailureThreshold).isEqualTo(10)
        assertThat(backend.circuitOpenMs).isEqualTo(5000)
        assertThat(backend.whaleUrl).isEmpty()
        assertThat(backend.ownerUrlPattern).isEmpty()
    }

    @Test
    fun `backend file cannot enable hoglite without authentication`() {
        assertThatThrownBy { config(auth = "off") }.hasMessageContaining("hoglite needs auth")
        assertThatThrownBy { Config(defaultBackend = "hoglite") }.hasMessageContaining("cannot be a default")
    }

    @Test
    fun `the hoglite token lifetime minimum defaults to 300 seconds and refuses a negative value`() {
        assertThat(Config().hogliteMinTokenLifetimeSeconds).isEqualTo(300)
        assertThat(Config(hogliteMinTokenLifetimeSeconds = 0).hogliteMinTokenLifetimeSeconds).isZero()
        assertThatThrownBy { Config(hogliteMinTokenLifetimeSeconds = -1) }
            .hasMessageContaining("HOGLAKE_HOGLITE_MIN_TOKEN_LIFETIME_SECONDS=-1")
    }

    @Test
    fun `backend ids are canonical and unique`() {
        for (bad in listOf(id.uppercase(), "1-1-1-1-1", "not-a-uuid")) {
            assertThatThrownBy { config(entry.replace(id, bad)) }.isInstanceOf(IllegalArgumentException::class.java)
        }
        assertThatThrownBy { config("$entry,$entry") }.hasMessageContaining("duplicate")
        assertThatThrownBy { config(entry.replace("hoglite", "postgres")) }.hasMessageContaining("only kind hoglite")
    }

    @Test
    fun `endpoints and owner patterns are server validated`() {
        for (bad in listOf("ftp://localhost", "relative", "http:///path", "http://user:secret@localhost")) {
            assertThatThrownBy { config(entry.replace("http://localhost:8080", bad)) }
                .isInstanceOf(IllegalArgumentException::class.java)
        }
        assertThatThrownBy { config(entry.dropLast(1) + """, "owner_url_pattern":"https://localhost/"}""") }
            .hasMessageContaining("{owner}")
        assertThatThrownBy { config(entry.dropLast(1) + """, "owner_url_pattern":"ftp://{owner}/"}""") }
            .hasMessageContaining("http(s)")
    }

    @Test
    fun `timeouts and circuit settings must be positive integers`() {
        for (key in listOf(
            "connect_timeout_ms",
            "read_timeout_ms",
            "commit_timeout_ms",
            "retry_budget_ms",
            "circuit_failure_threshold",
            "circuit_open_ms",
            "max_concurrent_calls",
        )) {
            for (bad in listOf("0", "-1", "1.5", "null", "\"100\"")) {
                assertThatThrownBy { config(entry.dropLast(1) + ",\"$key\":$bad}") }
                    .isInstanceOf(IllegalArgumentException::class.java)
            }
        }
        assertThatThrownBy { config(entry.dropLast(1) + ",\"circuit_failure_threshold\":2147483648}") }
            .hasMessageContaining("too large")
        assertThatThrownBy { config(entry.dropLast(1) + ",\"max_concurrent_calls\":2147483648}") }
            .hasMessageContaining("too large")
    }

    @Test
    fun `the bulkhead defaults to half the request threads beside PostgreSQL and to all of them alone`() {
        // #679: half, so a hoglite stall leaves the other half to the
        // PostgreSQL catalogs; the whole dispatcher when there is no
        // PostgreSQL to shield; never below one; and the key wins.
        //
        // MUTATION: default to `requestThreads` unconditionally and the
        // first reds at 10; default to half unconditionally and the
        // hoglite-only case reds at 5.
        val file = dir.resolve("backends.json")
        Files.writeString(file, """{"backends":[$entry]}""")

        fun bulkhead(cfg: Config) = cfg.hogliteBackends.getValue(UUID.fromString(id)).maxConcurrentCalls

        assertThat(bulkhead(Config(backendsFile = file.toString(), authMode = "enforce"))).isEqualTo(5)
        assertThat(bulkhead(Config(backendsFile = file.toString(), authMode = "enforce", requestThreads = 1)))
            .isEqualTo(1)
        assertThat(
            bulkhead(
                Config(
                    backendsFile = file.toString(),
                    authMode = "enforce",
                    defaultBackend = "none",
                    requestThreads = 10,
                ),
            ),
        ).isEqualTo(10)
        assertThat(
            bulkhead(
                Config(
                    backendsFile = file.toString(),
                    authMode = "enforce",
                    defaultBackend = "none",
                    postgresBackendId = "6c1d7e1f-2a3b-4c4d-8e5f-6a7b8c9d0e1f",
                    jdbcUrl = "jdbc:postgresql://localhost:5432/hoglake",
                    dbUser = "hoglake",
                    dbPassword = "hoglake",
                ),
            ),
        ).isEqualTo(5)
        assertThat(bulkhead(config(entry.dropLast(1) + ",\"max_concurrent_calls\":3}"))).isEqualTo(3)
    }

    @Test
    fun `a bulkhead as wide as the dispatcher boots with a warning beside PostgreSQL and silently alone`() {
        // #679: the cap is never refused, but a pod that serves
        // PostgreSQL is told when it shields nothing — a key at or above
        // HOGLAKE_REQUEST_THREADS, the one-thread default, or a cap whose
        // one permitted waiter takes the last thread — and a key
        // above the dispatcher's width is named for what it is on any
        // pod. A hoglite-only pod at the whole dispatcher is correct and
        // hears nothing; so is the default beside PostgreSQL.
        //
        // MUTATION: drop the `init` loop and every `hasSize(1)` reds;
        // drop the `servesPostgres &&` clause and the hoglite-only case
        // reds with a warning it should not get.
        val file = dir.resolve("backends.json")
        Files.writeString(file, """{"backends":[$entry]}""")
        val oversized = dir.resolve("oversized.json")
        Files.writeString(oversized, """{"backends":[${entry.dropLast(1) + ",\"max_concurrent_calls\":10}"}]}""")

        fun warnings(
            backends: Path,
            threads: Int = 10,
            default: String = "postgres",
        ) = bootWarnings {
            Config(
                backendsFile = backends.toString(),
                authMode = "enforce",
                defaultBackend = default,
                requestThreads = threads,
                // Silences #675's boot warning, which shares Config's logger.
                hogliteDataPathTemplate = "s3://warehouse/{catalog}",
            )
        }

        assertThat(warnings(file)).describedAs("the default beside PostgreSQL: half, no warning").isEmpty()
        assertThat(warnings(file, threads = 1))
            .describedAs("one request thread: the default IS the dispatcher")
            .hasSize(1)
            .allSatisfy { assertThat(it).contains("the whole of HOGLAKE_REQUEST_THREADS=1").contains("PostgreSQL") }
        // The request the bulkhead lets wait holds a thread too: at two
        // request threads the default cap of one plus its waiter is all
        // of them.
        //
        // MUTATION: drop `+ waiters` from the check and this reds empty.
        assertThat(warnings(file, threads = 2))
            .describedAs("two request threads: the default plus its one waiter IS the dispatcher")
            .hasSize(1)
            .allSatisfy {
                assertThat(it)
                    .contains("max_concurrent_calls=1 plus the 1 request it lets wait for a permit is the whole of")
                    .contains("HOGLAKE_REQUEST_THREADS=2")
            }
        assertThat(warnings(file, threads = 3)).describedAs("three: one left over for PostgreSQL").isEmpty()
        assertThat(warnings(oversized))
            .describedAs("an explicit key at the dispatcher's width beside PostgreSQL")
            .hasSize(1)
            .allSatisfy {
                assertThat(it).contains("max_concurrent_calls=10 is the whole of HOGLAKE_REQUEST_THREADS=10")
            }
        assertThat(warnings(oversized, threads = 4))
            .describedAs("an explicit key above the dispatcher's width")
            .hasSize(1)
            .allSatisfy { assertThat(it).contains("max_concurrent_calls=10 is above HOGLAKE_REQUEST_THREADS=4") }
        assertThat(warnings(file, default = "none"))
            .describedAs("hoglite-only at the whole dispatcher: nothing to shield, nothing to say")
            .isEmpty()
        assertThat(warnings(oversized, threads = 4, default = "none"))
            .describedAs("hoglite-only above the dispatcher: named, without the PostgreSQL clause")
            .hasSize(1)
            .allSatisfy { assertThat(it).contains("is above").doesNotContain("PostgreSQL") }
    }

    /** The warnings `Config` logs while [boot] constructs it, as the server logs them once at boot. */
    private fun bootWarnings(boot: () -> Config): List<String> {
        val capture = ListAppender<ILoggingEvent>().apply { start() }
        val logger = LoggerFactory.getLogger(Config::class.java) as Logger
        logger.addAppender(capture)
        try {
            boot()
        } finally {
            logger.detachAppender(capture)
        }
        return capture.list.filter { it.level == Level.WARN }.map { it.formattedMessage }
    }

    @Test
    fun `postgres routing is opt in and cannot share an id with hoglite`() {
        val cfg = Config(postgresBackendId = id)
        assertThat(cfg.backendKinds[UUID.fromString(id)]).isEqualTo(BackendKind.POSTGRES)
        val file = dir.resolve("backends.json")
        Files.writeString(file, """{"backends":[$entry]}""")
        assertThatThrownBy { Config(backendsFile = file.toString(), authMode = "enforce", postgresBackendId = id) }
            .hasMessageContaining("duplicates")
    }
}
