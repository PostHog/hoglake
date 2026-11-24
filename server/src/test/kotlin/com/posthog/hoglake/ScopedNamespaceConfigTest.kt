package com.posthog.hoglake

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

class ScopedNamespaceConfigTest {
    @TempDir
    lateinit var dir: Path

    private val id = "5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e"

    private fun file(
        name: String,
        text: String,
    ): String = Files.writeString(dir.resolve(name), text).toString()

    private fun config(
        profile: String = "example.aws-session",
        targets: String = "remote:events,remote:imports",
    ): Config =
        Config(
            readSessionProfile = profile,
            readSessionRemoteTargets = targets,
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
    fun `an absent read session profile keeps dedicated reads disabled`() {
        assertThat(Config(readSessionProfile = "", readSessionRemoteTargets = "").readSession).isNull()
    }

    @Test
    fun `read session requires an explicit complete configuration and exactly the team predicate`() {
        val cfg = config()
        assertThat(cfg.readSession).isNotNull()
        assertThatThrownBy { cfg.copy(authPredicateFields = "team_id,project_id") }
            .hasMessageContaining("only the team_id predicate")
        for ((profile, targets) in listOf(
            "example.aws-session" to "",
            "" to "remote:events,remote:imports",
            " " to "remote:events,remote:imports",
            " example.aws-session" to "remote:events,remote:imports",
            "storage-session" to "remote:events,remote:imports",
            "a".repeat(257) to "remote:events,remote:imports",
            "example.aws-session" to "remote:events",
            "example.aws-session" to "remote:events,remote:events",
            "example.aws-session" to "remote:events, remote:imports",
            "example.aws-session" to "remote:events,remote:imports,remote:extra",
            "example.aws-session" to "remote:events,",
        )) {
            assertThatThrownBy { config(profile = profile, targets = targets) }
                .describedAs("$profile / $targets")
                .isInstanceOf(IllegalArgumentException::class.java)
        }
    }

    @Test
    fun `activation requires enforced offline verification and a configured hoglite backend`() {
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
    fun `mount hosts coexist with read sessions while served namespaces stay blocked`() {
        val cfg = config()
        for (match in listOf(
            """{"type":"namespace","name":"elsewhere"}""",
            """{"type":"namespace_family","family":"team"}""",
        )) {
            val hosts =
                """{"hosts":[{"id":"$id","match":$match,"kind":"hoglake",""" +
                    """"url":"https://host.invalid","auth":"forward"}]}"""
            val combined = cfg.copy(mountHostsFile = file("hosts.json", hosts), authSessionProfile = "on")
            assertThat(combined.readSession).isNotNull()
            assertThat(combined.mountHosts).hasSize(1)
        }
        val served =
            """{"families":[{"family":"team","source_catalog":"imports","predicate_field":"team_id"}]}"""
        assertThatThrownBy {
            cfg.copy(servedNamespacesFile = file("served.json", served), authSessionProfile = "on")
        }.hasMessageContaining("cannot coexist")
    }
}
