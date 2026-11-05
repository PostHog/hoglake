package com.posthog.hoglake.hoglite

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.invariantSeparatorsPathString
import kotlin.io.path.name
import kotlin.io.path.readText
import kotlin.streams.asSequence

/**
 * The hoglite adapter is not verified against a live engine, so no
 * production code may use it yet. This gate fails when a main source file
 * outside the hoglite package refers to that package. Remove the gate in
 * the PR that wires the adapter, together with its conformance tests.
 */
class HogliteReferenceGuardTest {
    @Test
    fun `no main code outside the hoglite package refers to it`() {
        val root = Path.of("src/main")
        val pkg = "com.posthog.hoglake.hoglite"
        val reference = Regex("""\b${Regex.escape(pkg)}\b""")
        val scanned =
            Files.walk(root).asSequence()
                .filter { it.name.endsWith(".kt") || it.name.endsWith(".java") }
                .map { root.relativize(it).invariantSeparatorsPathString }
                .toList()
        val offenders =
            scanned
                .filterNot { it.startsWith("kotlin/com/posthog/hoglake/hoglite/") }
                .filter { reference.containsMatchIn(root.resolve(it).readText()) }

        assertThat(scanned).describedAs("the scan found the main sources").anyMatch { it.endsWith("/App.kt") }
        assertThat(offenders)
            .describedAs("main sources that refer to %s before the adapter is wired", pkg)
            .isEmpty()
    }
}
