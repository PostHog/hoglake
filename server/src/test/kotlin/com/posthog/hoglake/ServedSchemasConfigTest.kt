package com.posthog.hoglake

import com.posthog.hoglake.store.ServedSchema
import com.posthog.hoglake.store.ServedSchemas
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID

/** HOGLAKE_SERVED_SCHEMAS_FILE (docs/auth-tenancy.md §6.5d): what boots as a remote schema's Host, and what is refused. */
class ServedSchemasConfigTest {
    @TempDir
    lateinit var dir: Path
    private val org = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f0"

    private fun file(content: String): String =
        Files.writeString(dir.resolve("served-${UUID.randomUUID()}.json"), content).toString()

    private fun config(
        schemas: String,
        authMode: String = "enforce",
        sessionProfile: String = "on",
        hostsFile: String = "",
        defaultBackend: String = "postgres",
    ) = Config(
        servedSchemasFile = file(schemas),
        authMode = authMode,
        authPredicateFields = "team_id",
        authSessionProfile = sessionProfile,
        mountHostsFile = hostsFile,
        defaultBackend = defaultBackend,
    )

    private fun refused(
        schemas: String,
        message: String,
    ) {
        assertThatThrownBy { config(schemas) }
            .describedAs(schemas)
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining(message)
    }

    @Test
    fun `no file and an empty list serve nothing and demand nothing`() {
        assertThat(Config().servedSchemas.isEmpty).isTrue()
        val empty = config("""{"schemas":[]}""", sessionProfile = "off", defaultBackend = "none")
        assertThat(empty.servedSchemas.isEmpty).isTrue()
        assertThat(empty.servedSchemas.find(org, "posthog")).isNull()
    }

    @Test
    fun `an entry is its name and source catalog, with the team field defaulted`() {
        val served =
            config(
                """{"schemas":[{"name":"posthog","catalog":"millrace-dev"},""" +
                    """{"name":"imports","catalog":"imports-dev","team_field":"team"}]}""",
            ).servedSchemas
        assertThat(served.schemas).containsExactly(
            ServedSchema("posthog", "millrace-dev", "team_id"),
            ServedSchema("imports", "imports-dev", "team"),
        )
    }

    @Test
    fun `a malformed file or entry is refused`() {
        refused("""[]""", "must contain only a schemas array")
        refused("""{"schemas":[],"hosts":[]}""", "must contain only a schemas array")
        refused("""{"schemas":{}}""", "schemas must be an array")
        refused("""{"schemas":["posthog"]}""", "entry must be an object")
        refused("""{"schemas":[{"name":"posthog","catalog":"c","alias":"x"}]}""", "unknown fields")
        refused("""{"schemas":[{"catalog":"c"}]}""", "requires a string name")
        refused("""{"schemas":[{"name":"posthog"}]}""", "requires a string catalog")
        refused("""{"schemas":[{"name":7,"catalog":"c"}]}""", "name must be a string")
        refused("""{"schemas":[{"name":"posthog","catalog":"c","team_field":7}]}""", "team_field must be a string")
    }

    @Test
    fun `names must be identifiers and unique, and a source catalog is not org shaped`() {
        refused("""{"schemas":[{"name":"post hog","catalog":"c"}]}""", "must be a namespace name")
        refused("""{"schemas":[{"name":"","catalog":"c"}]}""", "must be a namespace name")
        refused("""{"schemas":[{"name":"posthog","catalog":"Not A Catalog"}]}""", "must be a catalog name")
        refused(
            """{"schemas":[{"name":"posthog","catalog":"c","team_field":"a.b"}]}""",
            "team_field must be a column name",
        )
        refused(
            """{"schemas":[{"name":"posthog","catalog":"a"},{"name":"posthog","catalog":"b"}]}""",
            "two served schemas have the same name",
        )
        // No case variant of a served name is a second schema: a Client folds case the same way.
        refused(
            """{"schemas":[{"name":"posthog","catalog":"a"},{"name":"PostHog","catalog":"b"}]}""",
            "two served schemas have the same name",
        )
        refused("""{"schemas":[{"name":"posthog","catalog":"$org"}]}""", "must not have the shape of an org id")
    }

    @Test
    fun `a host needs postgres and the session profile, and is not a client`() {
        val one = """{"schemas":[{"name":"posthog","catalog":"millrace-dev"}]}"""
        assertThatThrownBy { config(one, sessionProfile = "off") }
            .hasMessageContaining("requires HOGLAKE_AUTH_SESSION_PROFILE=on")
        // Off verifies no token, so no profile is asked for: the schema then serves no one.
        assertThat(config(one, authMode = "off", sessionProfile = "off").servedSchemas.schemas).hasSize(1)
        assertThatThrownBy { config(one, defaultBackend = "none") }
            .hasMessageContaining("requires HOGLAKE_DEFAULT_BACKEND=postgres")
        val hosts =
            file(
                """{"hosts":[{"id":"3c0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e","match":{"type":"events"},""" +
                    """"kind":"hoglake","url":"http://gigahog:8080","auth":"forward"}]}""",
            )
        assertThatThrownBy { config(one, hostsFile = hosts) }
            .hasMessageContaining("a server is one or the other")
    }

    @Test
    fun `only an org shaped catalog with exactly a served namespace is a served path`() {
        val served =
            ServedSchemas(listOf(ServedSchema("posthog", "millrace-dev"), ServedSchema("imports", "imports-dev")))
        assertThat(served.find(org, "posthog")?.catalog).isEqualTo("millrace-dev")
        assertThat(served.find(org, "imports")?.catalog).isEqualTo("imports-dev")
        // Exactly the name: no case variant and no other namespace.
        for (namespace in listOf("PostHog", "posthog ", "ingest", "", null)) {
            assertThat(served.find(org, namespace)).describedAs("$namespace").isNull()
        }
        // A catalog of this server's own keeps its namespace of that name; so does a mount's name.
        for (catalog in listOf("millrace-dev", "lake", "$org-events", org.uppercase(), "$org ", "", null)) {
            assertThat(served.find(catalog, "posthog")).describedAs("$catalog").isNull()
        }
        assertThat(ServedSchemas.NONE.find(org, "posthog")).isNull()
        assertThat(ServedSchemas.recordName(org)).isEqualTo("$org-events")
    }
}
