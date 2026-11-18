package com.posthog.hoglake

import com.posthog.hoglake.store.ServedFamily
import com.posthog.hoglake.store.ServedStream
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

    private fun withStream(stream: String): String {
        val entry = """{"family":"team","source_catalog":"imports-dev","predicate_field":"team_id","stream":$stream}"""
        return """{"families":[$entry]}"""
    }

    @Test
    fun `a stream block parses, and an entry without one has none`() {
        val parsed =
            config(withStream("""{"catalog":"millrace-dev","namespace":"ingest","tables":["events","persons"]}"""))
        assertThat(parsed.servedFamilies.single().stream)
            .isEqualTo(ServedStream("millrace-dev", "ingest", listOf("events", "persons")))
        assertThat(config().servedFamilies.single().stream).isNull()
    }

    @Test
    fun `a stream block is parsed strictly, and its names are checked at boot`() {
        val good = """"catalog":"millrace-dev","namespace":"ingest","tables":["events"]"""
        for (bad in listOf(
            // An unknown key in the block, beside it, or a key missing.
            withStream("""{$good,"extra":1}"""),
            """{"families":[{"family":"team","source_catalog":"x","predicate_field":"team_id","streams":{$good}}]}""",
            withStream("""{"catalog":"millrace-dev","namespace":"ingest"}"""),
            withStream("""{"namespace":"ingest","tables":["events"]}"""),
            withStream("""[]"""),
            withStream("""null"""),
            // The tables: empty, not an array, a non-string, an invalid name, two names one fold apart.
            withStream("""{"catalog":"millrace-dev","namespace":"ingest","tables":[]}"""),
            withStream("""{"catalog":"millrace-dev","namespace":"ingest","tables":"events"}"""),
            withStream("""{"catalog":"millrace-dev","namespace":"ingest","tables":[1]}"""),
            withStream("""{"catalog":"millrace-dev","namespace":"ingest","tables":["a b"]}"""),
            withStream("""{"catalog":"millrace-dev","namespace":"ingest","tables":["events","EVENTS"]}"""),
            // The namespace: a team's shape in any case, or invalid. The catalog: invalid, or not a string.
            withStream("""{"catalog":"millrace-dev","namespace":"team_5","tables":["events"]}"""),
            withStream("""{"catalog":"millrace-dev","namespace":"TEAM_007","tables":["events"]}"""),
            withStream("""{"catalog":"millrace-dev","namespace":"a b","tables":["events"]}"""),
            withStream("""{"catalog":"Millrace","namespace":"ingest","tables":["events"]}"""),
            withStream("""{"catalog":1,"namespace":"ingest","tables":["events"]}"""),
        )) {
            assertThatThrownBy { config(bad) }.describedAs(bad).isInstanceOf(IllegalArgumentException::class.java)
        }
        refused("must differ in more than ASCII case") {
            config(withStream("""{"catalog":"c","namespace":"ingest","tables":["events","Events"]}"""))
        }
        refused("that is not a team's") {
            config(withStream("""{"catalog":"c","namespace":"team_1","tables":["e"]}"""))
        }
    }

    @Test
    fun `a stream publishes its exact names and reserves them in any ASCII case`() {
        val stream = ServedStream("millrace-dev", "ingest", listOf("events", "persons"))
        assertThat(stream.publishes("events")).isTrue()
        assertThat(stream.publishes("EVENTS")).isFalse()
        assertThat(stream.publishes("events_raw")).isFalse()
        val reserved = listOf("events", "EVENTS", "Events", "PERSONS")
        assertThat(reserved).allMatch { stream.reserves(it) }
        assertThat(listOf("events_raw", "event", "pageviews")).noneMatch { stream.reserves(it) }
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
