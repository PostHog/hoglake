package com.posthog.hoglake.store

import com.posthog.hoglake.Config
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
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
        )) {
            for (bad in listOf("0", "-1", "1.5", "null", "\"100\"")) {
                assertThatThrownBy { config(entry.dropLast(1) + ",\"$key\":$bad}") }
                    .isInstanceOf(IllegalArgumentException::class.java)
            }
        }
        assertThatThrownBy { config(entry.dropLast(1) + ",\"circuit_failure_threshold\":2147483648}") }
            .hasMessageContaining("too large")
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
