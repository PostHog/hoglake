package com.posthog.hoglake

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.name
import kotlin.io.path.readText
import kotlin.streams.asSequence

/**
 * Every Docker-backed test class carries `@Tag("integration")`, because
 * `gradle :test -PunitOnly` excludes that tag and nothing else. A class
 * that opens a container without the tag runs under -PunitOnly and fails
 * on a machine without Docker; a filename pattern does not catch it,
 * which is how ~20 classes slipped through before (#181).
 *
 * "Docker-backed" is read off the source: a reference to the Postgres or
 * MinIO harness. The gate names the harness symbols by concatenation so
 * this file does not match its own predicate.
 */
class IntegrationTagGateTest {
    @Test
    fun `every class that opens a container is tagged integration`() {
        val root = Path.of("src/test/kotlin")
        val harness =
            listOf(
                "PgTest" + "Support",
                "fresh" + "Database(",
                "Minio" + "TestSupport",
                "Testcontainers",
                "GenericContainer(",
                // A class that starts its OWN Postgres rather than
                // going through the shared harness — HealthProbeIntegrationTest
                // does, because its subject is STOPPING one. Without
                // this entry it matched no harness symbol and the gate
                // had nothing to say about it.
                "PostgreSQLContainer(",
            )
        val untagged =
            Files.walk(root).asSequence()
                .filter { it.name.endsWith(".kt") }
                .filter { it.name != "PgTest" + "Support.kt" && it.name != "Minio" + "TestSupport.kt" }
                .map { it to it.readText() }
                .filter { (_, text) -> harness.any { it in text } }
                .filter {
                        (_, text) ->
                    Regex("^\\s*(abstract\\s+|open\\s+)?class\\s", RegexOption.MULTILINE).containsMatchIn(text)
                }
                .filter { (_, text) -> "@Tag(\"integration\")" !in text }
                .map { (path, _) -> root.relativize(path).toString() }
                .toList()
        assertThat(untagged)
            .describedAs(
                "classes that open a container but lack @Tag(\"integration\"); add the tag so -PunitOnly skips them",
            )
            .isEmpty()
    }
}
