package com.posthog.hoglake

import com.posthog.hoglake.store.ServedFamily
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
    private val valid =
        """{"families":[{"family":"team","source_catalog":"imports-dev","predicate_field":"team_id"}]}"""

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
        assertThat(Config().servedFamilies).isEmpty()
        assertThat(config("""{"families":[]}""").servedFamilies).isEmpty()
    }

    @Test
    fun `an entry is the team family, its source catalog and its predicate field`() {
        assertThat(config().servedFamilies).containsExactly(ServedFamily("team", "imports-dev", "team_id"))
    }

    @Test
    fun `the file is parsed strictly`() {
        val entry = """{"family":"team","source_catalog":"x","predicate_field":"team_id"}"""
        for (bad in listOf(
            """[]""",
            """{}""",
            """{"families":{}}""",
            """{"families":[],"extra":1}""",
            """{"families":[{"family":"team","source_catalog":"x"}]}""",
            """{"families":[{"family":"team","predicate_field":"team_id"}]}""",
            """{"families":[{"source_catalog":"x","predicate_field":"team_id"}]}""",
            """{"families":[{"family":"team","source_catalog":"x","predicate_field":"team_id","name":"b"}]}""",
            """{"families":[{"family":"org","source_catalog":"x","predicate_field":"team_id"}]}""",
            """{"families":[{"family":"Team","source_catalog":"x","predicate_field":"team_id"}]}""",
            """{"families":[{"family":1,"source_catalog":"x","predicate_field":"team_id"}]}""",
            """{"families":[{"family":"team","source_catalog":"Upper","predicate_field":"team_id"}]}""",
            """{"families":[{"family":"team","source_catalog":"x","predicate_field":"a b"}]}""",
            """{"families":[{"family":"team","source_catalog":"x","predicate_field":""}]}""",
            """{"families":[$entry,$entry]}""",
            """{"families":[$entry,{"family":"team","source_catalog":"y","predicate_field":"team_id"}]}""",
            """{"families":[$entry],"namespaces":[]}""",
        )) {
            assertThatThrownBy { config(bad) }.describedAs(bad).isInstanceOf(IllegalArgumentException::class.java)
        }
        assertThatThrownBy { config("""{"families":[],"families":[]}""") }
            .isInstanceOf(com.fasterxml.jackson.core.JsonProcessingException::class.java)
    }

    @Test
    fun `the former per-name list is refused with a message that names the new shape`() {
        refused("no longer takes a namespaces list") {
            config("""{"namespaces":[{"name":"team_9400","source_catalog":"imports-dev"}]}""")
        }
    }

    @Test
    fun `the team family's predicate field must be team_id, and listed among the auth predicate fields`() {
        refused("predicate_field for the 'team' family must be 'team_id'") {
            config(
                """{"families":[{"family":"team","source_catalog":"x","predicate_field":"project_id"}]}""",
                predicateFields = "team_id,project_id",
            )
        }
        refused("predicate_field 'team_id' is not one of HOGLAKE_AUTH_PREDICATE_FIELDS") {
            config(valid, predicateFields = "org_id")
        }
        assertThat(config(valid, predicateFields = "org_id, team_id").servedFamilies).hasSize(1)
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
            assertThat(config(authMode = mode).servedFamilies).describedAs(mode).hasSize(1)
        }
    }
}
