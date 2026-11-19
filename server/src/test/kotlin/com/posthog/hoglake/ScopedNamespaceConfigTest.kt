package com.posthog.hoglake

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID

class ScopedNamespaceConfigTest {
    @TempDir
    lateinit var dir: Path

    private val id = "5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e"
    private val binding =
        """{"catalog":"lake","backend":"hoglite","backend_id":"$id","tenant":"opaque_1","namespace":"reports"}"""

    private fun file(
        name: String,
        text: String,
    ): String = Files.writeString(dir.resolve(name), text).toString()

    private fun config(text: String = binding): Config =
        Config(
            scopedNamespaceFile = file("binding.json", text),
            backendsFile =
                file(
                    "backends.json",
                    """{"backends":[{"id":"$id","kind":"hoglite","url":"http://localhost:1"}]}""",
                ),
            authMode = "enforce",
            authJwksFile = "mounted-public-jwks.json",
            authPredicateFields = "team_id",
        )

    @Test
    fun `an absent binding keeps typed reads disabled`() {
        assertThat(Config(scopedNamespaceFile = "").scopedNamespaceBinding).isNull()
    }

    @Test
    fun `one complete binding carries only trusted placement`() {
        val found = config().scopedNamespaceBinding!!
        assertThat(found.catalog).isEqualTo("lake")
        assertThat(found.backendId).isEqualTo(UUID.fromString(id))
        assertThat(found.tenant).isEqualTo("opaque_1")
        assertThat(found.namespace).isEqualTo("reports")
    }

    @Test
    fun `the parser refuses multiple incomplete ambiguous or noncanonical bindings`() {
        for (bad in listOf(
            "[]",
            "null",
            "{}",
            "[$binding,$binding]",
            "$binding $binding",
            binding.replace(""""namespace":"reports"""", """"namespaces":["reports"]"""),
            binding.dropLast(1) + ""","extra":true}""",
            binding.dropLast(1) + ""","catalog":"other"}""",
            binding.replace("\"hoglite\"", "\"postgres\""),
            binding.replace(id, id.uppercase()),
            binding.replace("\"lake\"", "\"Lake\""),
            binding.replace("\"reports\"", "\"a%2Fb\""),
            binding.replace("\"opaque_1\"", "\"../opaque\""),
            binding.replace("\"opaque_1\"", "\"\""),
        )) {
            assertThatThrownBy { config(bad) }.describedAs(bad).isInstanceOf(Exception::class.java)
        }
    }

    @Test
    fun `activation requires enforced offline verification and a configured backend`() {
        val cfg = config()
        for (change in listOf<(Config) -> Config>(
            { it.copy(authMode = "warn") },
            { it.copy(authMode = "off") },
            { it.copy(authJwksFile = "", authJwksUrl = "https://issuer.invalid/jwks") },
            { it.copy(authJwksUrl = "https://issuer.invalid/jwks") },
            { it.copy(authPredicateFields = "") },
            { it.copy(backendsFile = "") },
            { it.copy(backendsFile = "", postgresBackendId = id) },
        )) {
            assertThatThrownBy { change(cfg) }.isInstanceOf(IllegalArgumentException::class.java)
        }
    }

    @Test
    fun `reserved mount and namespace names cannot activate`() {
        assertThatThrownBy {
            config(binding.replace("\"lake\"", "\"11111111-1111-1111-1111-111111111111-events\""))
        }.hasMessageContaining("reserved mount")
        assertThatThrownBy {
            config().copy(reservedNamespaceNames = setOf("REPORTS"))
        }.hasMessageContaining("reserved namespace")
    }

    @Test
    fun `any remote or served configuration prevents activation`() {
        val cfg = config()
        for (match in listOf(
            """{"type":"namespace","name":"reports"}""",
            """{"type":"namespace","name":"elsewhere"}""",
            """{"type":"namespace_family","family":"team"}""",
        )) {
            val hosts =
                """{"hosts":[{"id":"$id","match":$match,"kind":"hoglake",""" +
                    """"url":"https://host.invalid","auth":"forward"}]}"""
            assertThatThrownBy {
                cfg.copy(mountHostsFile = file("hosts.json", hosts), authSessionProfile = "on")
            }.isInstanceOf(IllegalArgumentException::class.java)
        }
        val served =
            """{"families":[{"family":"team","source_catalog":"imports","predicate_field":"team_id"}]}"""
        assertThatThrownBy {
            cfg.copy(servedNamespacesFile = file("served.json", served), authSessionProfile = "on")
        }.hasMessageContaining("cannot coexist")
    }
}
