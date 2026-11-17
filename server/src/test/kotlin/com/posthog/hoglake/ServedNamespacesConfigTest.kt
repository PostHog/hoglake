package com.posthog.hoglake

import com.posthog.hoglake.store.ServedNamespace
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID

/** HOGLAKE_SERVED_NAMESPACES_FILE (docs/auth-tenancy.md §6.5d): what boots as a Host that serves namespaces, and what is refused. */
class ServedNamespacesConfigTest {
    @TempDir
    lateinit var dir: Path
    private val valid = """{"namespaces":[{"name":"team_9400","source_catalog":"imports-dev"}]}"""

    private fun config(
        file: String = valid,
        authMode: String = "enforce",
        sessionProfile: String = "on",
        predicateFields: String = "team_id",
        defaultBackend: String = "postgres",
        mountHostsFile: String = "",
    ): Config {
        val path = Files.writeString(dir.resolve("served-${UUID.randomUUID()}.json"), file)
        return Config(
            servedNamespacesFile = path.toString(),
            authMode = authMode,
            authSessionProfile = sessionProfile,
            authPredicateFields = predicateFields,
            defaultBackend = defaultBackend,
            mountHostsFile = mountHostsFile,
            postgresBackendId = "",
        )
    }

    private fun refused(
        message: String,
        build: () -> Config,
    ) {
        assertThatThrownBy { build() }.isInstanceOf(IllegalArgumentException::class.java).hasMessageContaining(message)
    }

    @Test
    fun `no file serves nothing`() {
        assertThat(Config().servedNamespaces).isEmpty()
    }

    @Test
    fun `an entry is a namespace name and its source catalog`() {
        assertThat(config().servedNamespaces).containsExactly(ServedNamespace("team_9400", "imports-dev"))
        val two = """{"namespaces":[{"name":"a","source_catalog":"x"},{"name":"A","source_catalog":"y"}]}"""
        assertThat(config(two).servedNamespaces.map { it.name }).containsExactly("a", "A")
    }

    @Test
    fun `the file is parsed strictly`() {
        for (bad in listOf(
            """[]""",
            """{}""",
            """{"namespaces":{}}""",
            """{"namespaces":[],"extra":1}""",
            """{"namespaces":[{"name":"a"}]}""",
            """{"namespaces":[{"source_catalog":"x"}]}""",
            """{"namespaces":[{"name":"a","source_catalog":"x","pattern":"b"}]}""",
            """{"namespaces":[{"name":1,"source_catalog":"x"}]}""",
            """{"namespaces":[{"name":"a b","source_catalog":"x"}]}""",
            """{"namespaces":[{"name":"","source_catalog":"x"}]}""",
            """{"namespaces":[{"name":"a","source_catalog":"Upper"}]}""",
            """{"namespaces":[{"name":"a","source_catalog":"x"},{"name":"a","source_catalog":"y"}]}""",
        )) {
            assertThatThrownBy { config(bad) }.describedAs(bad).isInstanceOf(IllegalArgumentException::class.java)
        }
        assertThatThrownBy { config("""{"namespaces":[],"namespaces":[]}""") }
            .isInstanceOf(com.fasterxml.jackson.core.JsonProcessingException::class.java)
    }

    @Test
    fun `boot refuses what a served namespace cannot run without`() {
        refused("HOGLAKE_AUTH_MODE") { config(authMode = "off") }
        refused("HOGLAKE_AUTH_SESSION_PROFILE=on") { config(sessionProfile = "off") }
        refused("HOGLAKE_AUTH_PREDICATE_FIELDS") { config(predicateFields = "") }
        refused("HOGLAKE_DEFAULT_BACKEND=postgres") { config(defaultBackend = "none") }
        val hosts =
            Files.writeString(
                dir.resolve("hosts.json"),
                """{"hosts":[{"id":"3c0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e","match":{"type":"events"},""" +
                    """"kind":"hoglake","url":"http://h:1","auth":"forward"}]}""",
            )
        refused("a server is one or the other") { config(mountHostsFile = hosts.toString()) }
        for (mode in listOf("warn", "enforce")) {
            assertThat(config(authMode = mode).servedNamespaces).describedAs(mode).hasSize(1)
        }
    }
}
