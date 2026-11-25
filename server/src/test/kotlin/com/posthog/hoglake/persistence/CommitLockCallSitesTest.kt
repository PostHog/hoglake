package com.posthog.hoglake.persistence

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.lang.reflect.Modifier
import java.nio.file.Files
import java.nio.file.Path
import kotlin.io.path.name
import kotlin.io.path.readText
import kotlin.streams.asSequence

/**
 * EVERY TAKER OF THE COMMIT LOCK IS OBSERVED, AND SAYS WHAT IT IS (#439).
 *
 * The hold histogram lies the moment one taker bypasses
 * [Locks.acquireCatalogCommitLock] or labels itself as another path.
 * Three pins, read off the main sources and the compiled class:
 *
 *  1. the lock SQL exists in one place — [Locks]' private
 *     `lockStatement` — so nothing can take the lock unobserved;
 *  2. the public surface of [Locks] that takes the commit lock is the one
 *     observed function, and it requires a [CommitLockPath];
 *  3. every call site in the main sources, by file and enclosing
 *     function, passes the path written down here. A new call site, a
 *     removed one, or a changed label reds until this table is edited —
 *     which is the moment to decide which path it is.
 *
 * The needles are built by concatenation so this file does not match
 * them itself.
 */
class CommitLockCallSitesTest {
    private val main = Path.of("src/main/kotlin")

    private val lockSql = "pg_advisory_" + "xact_lock"
    private val acquire = "acquireCatalog" + "CommitLock("

    private fun sources(): Sequence<Pair<String, List<String>>> =
        Files.walk(main).asSequence()
            .filter { it.name.endsWith(".kt") }
            .map { main.relativize(it).toString() to it.readText().lines() }

    /** Code, not KDoc or a comment: what the compiler would see. */
    private fun isCode(line: String): Boolean {
        val t = line.trim()
        return !(t.startsWith("*") || t.startsWith("/*") || t.startsWith("//"))
    }

    @Test
    fun `the advisory commit-lock statement exists only inside Locks`() {
        val hits =
            sources().flatMap { (file, lines) ->
                lines.withIndex().filter { (_, l) -> isCode(l) && lockSql in l }.map { (i, _) -> "$file:${i + 1}" }
            }.toList()
        // MUTATION: take the lock in a caller with its own
        // `SELECT pg_advisory_xact_lock(...)` and it is listed here.
        assertThat(hits).allSatisfy { assertThat(it).startsWith("com/posthog/hoglake/persistence/Locks.kt:") }
        val locks = sources().single { it.first.endsWith("persistence/Locks.kt") }.second
        // The one other xact-scoped class, the promote stats fence (#729),
        // has its own two takers; everything else is the commit lock's.
        val byFunction = hits.map { enclosingFunction(locks, it.substringAfterLast(':').toInt() - 1) }
        assertThat(byFunction.filterNot { it in PROMOTE_STATS_FENCE_TAKERS }).containsExactly("lockStatement")
        for ((hit, fn) in hits.zip(byFunction)) {
            if (fn in PROMOTE_STATS_FENCE_TAKERS) {
                assertThat(locks[hit.substringAfterLast(':').toInt() - 1]).contains("PROMOTE_STATS_FENCE_LOCK_CLASS")
            }
        }
    }

    @Test
    fun `the raw statement is private and the public acquire requires a path`() {
        val methods = Locks::class.java.declaredMethods
        // MUTATION: make lockStatement public (or internal, which compiles
        // to public) and a caller can take the lock unobserved.
        assertThat(methods.filter { it.name == "lockStatement" }).singleElement()
            .satisfies({ assertThat(Modifier.isPrivate(it.modifiers)).isTrue() })
        val publicTakers =
            methods.filter {
                Modifier.isPublic(it.modifiers) && !it.isSynthetic && it.name.contains("CatalogCommitLock")
            }
        assertThat(publicTakers.map { it.name }).containsExactly("acquireCatalogCommitLock")
        assertThat(publicTakers.single().parameterTypes.toList()).contains(CommitLockPath::class.java)
    }

    @Test
    fun `every call site passes the path written down for it`() {
        val sites =
            sources()
                .filter { (file, _) -> !file.endsWith("persistence/Locks.kt") }
                .flatMap { (file, lines) ->
                    lines.withIndex()
                        .filter { (_, l) -> isCode(l) && acquire in l }
                        .map { (i, l) ->
                            val call = callText(lines, i)
                            val path =
                                Regex("""CommitLockPath\.(\w+)""").find(call)?.groupValues?.get(1)
                                    ?: "<no path in: $call>"
                            "${file.substringAfterLast('/').removeSuffix(".kt")}.${enclosingFunction(lines, i)}" to path
                        }
                }
                .toList()
                .sortedBy { it.first }
        // MUTATION: label CompactionService's group commit COMMIT (or any
        // row here anything else) and this reds naming the site.
        assertThat(sites).containsExactlyInAnyOrderElementsOf(EXPECTED.toList())
    }

    /** The call starting on [line], to its closing parenthesis (it may wrap). */
    private fun callText(
        lines: List<String>,
        line: Int,
    ): String {
        val text = StringBuilder()
        var depth = 0
        var opened = false
        for (l in lines.drop(line)) {
            val from = if (text.isEmpty()) l.indexOf(acquire) else 0
            for (c in l.substring(from)) {
                text.append(c)
                if (c == '(') {
                    depth++
                    opened = true
                } else if (c == ')') {
                    depth--
                }
                if (opened && depth == 0) return text.toString()
            }
            text.append(' ')
        }
        return text.toString()
    }

    /** The nearest `fun name(` at or above [line]. */
    private fun enclosingFunction(
        lines: List<String>,
        line: Int,
    ): String {
        val fn = Regex("""\bfun\s+(?:<[^>]+>\s+)?(\w+)\s*\(""")
        for (i in line downTo 0) {
            fn.find(lines[i])?.let { return it.groupValues[1] }
        }
        return "<top level>"
    }

    private companion object {
        /** [Locks]' functions taking the promote stats fence, not the commit lock. */
        val PROMOTE_STATS_FENCE_TAKERS = setOf("acquirePromoteStatsFence", "acquirePromoteStatsFenceShared")

        /** `File.function` -> the path it passes. One entry per call site. */
        val EXPECTED: Map<String, String> =
            mapOf(
                "CommitService.doCommit" to "COMMIT",
                "CompactionService.commitGroup" to "COMPACTION",
                "ExpiryService.advanceFloor" to "EXPIRY",
                "RetirementService.batch" to "RETIREMENT",
                "AlterService.alterTable" to "DDL",
                "TableCreationService.publish" to "DDL",
                "CatalogService.createNamespace" to "DDL",
                "CatalogService.createTable" to "DDL",
                "CatalogService.truncateTable" to "DDL",
                "CatalogService.setNamespaceContainment" to "DDL",
                "CatalogService.dropCatalog" to "DROP",
                "CatalogService.dropNamespace" to "DROP",
                "CatalogService.dropTable" to "DROP",
                "ViewService.create" to "DDL",
                "ViewService.drop" to "DROP",
            )
    }
}
