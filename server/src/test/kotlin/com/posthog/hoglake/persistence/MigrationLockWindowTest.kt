package com.posthog.hoglake.persistence

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.nio.file.Files
import java.nio.file.Path

/**
 * AGENT.md's migration rule, enforced against the FILES.
 *
 * > Every heavy-lock statement runs inside the `lock_timeout` window.
 * > `ALTER TABLE` (ADD COLUMN included — it is ACCESS EXCLUSIVE even
 * > when metadata-only), `ADD/DROP CONSTRAINT`, `DROP INDEX` and any
 * > backfill `UPDATE` sit after the save+`SET lock_timeout = '5s'` and
 * > before the restore; only `CREATE INDEX CONCURRENTLY` sits outside
 * > it, because that build waits out older transactions by design.
 *
 * NOTHING PINNED THAT. The rule has been stated in AGENT.md since V9,
 * broken by V10 (ADD COLUMN after the restore), broken by V11 and V12
 * (no guard at all), and broken again by V14's first draft — and it was
 * found by review every time, because deleting the `SET`/restore pair
 * from a migration leaves the whole suite green. A migration test
 * asserts what the schema ENDS UP as; the window is about what the
 * statement does to everyone else while it gets there, and no assertion
 * on the result can see it.
 *
 * This is a TEXTUAL test, in the `ScalarTypeParityTest` mould: it reads
 * the migration files off disk and parses them, because a test that
 * restates its subject asserts only that it compiles. It is a unit test
 * — no Docker, no container — so it reds on any machine, in the fast
 * lane, within a second of the mistake.
 *
 * SCOPE: V14 and up. V9-V13 are grandfathered, and deliberately: V10,
 * V11 and V12 violate the rule TODAY, that is recorded in AGENT.md as a
 * known defect, and retro-fixing frozen migrations is a separate change
 * with its own risk. The gate is for what lands next.
 *
 * Both window SHAPES are accepted, because both are correct and the
 * choice between them is the migration's to make:
 *
 *  - `executeInTransaction=false` (V14, V15): the session outlives each
 *    statement, so the file must SAVE `lock_timeout`, set it, and
 *    RESTORE it. Heavy statements go between the set and the restore.
 *  - transactional (V16, and V13's shape): `SET LOCAL` expires with the
 *    transaction, so there is nothing to restore and every heavy
 *    statement simply follows it.
 */
class MigrationLockWindowTest {
    private companion object {
        const val DIR = "src/main/resources/db/migration"

        /** The first version this gate covers; see the class KDoc. */
        const val FIRST_GUARDED_VERSION = 14

        /**
         * Version numbers that may be MISSING from the chain, and why.
         *
         * 5: never written. V4 is followed by V6, since before the
         * append-only freeze.
         *
         * This set is also a MERGE-COORDINATION REGISTER: a branch that
         * deliberately leaves a number to a sibling adds it here with the
         * owner named, and `no stale entry survives the sibling's merge`
         * below reds the moment that number exists, which is what forces
         * the entry to be pruned rather than left to rot into a hole
         * nobody notices. (23 sat here while #247 and #240 developed
         * V23 and V24 in parallel; #247 merged first and it was pruned.)
         */
        val ALLOWED_GAPS = setOf(5)

        /**
         * Statements that take a lock conflicting with ordinary writes,
         * as a line-level pattern over the migration text.
         *
         * `CREATE INDEX` counts only when it is NOT `CONCURRENTLY` —
         * that is the one build AGENT.md exempts, and the exemption is
         * the whole reason the pattern has to look at the modifier
         * rather than the keyword. `EXECUTE 'DROP INDEX ...'` inside a
         * `DO` block counts as a `DROP INDEX`, because it is one.
         */
        val HEAVY =
            listOf(
                "ALTER TABLE" to Regex("""\bALTER\s+TABLE\b""", RegexOption.IGNORE_CASE),
                "DROP INDEX" to Regex("""\bDROP\s+INDEX\b""", RegexOption.IGNORE_CASE),
                "CREATE INDEX (not CONCURRENTLY)" to
                    Regex("""\bCREATE\s+(UNIQUE\s+)?INDEX\s+(?!CONCURRENTLY)""", RegexOption.IGNORE_CASE),
                "CREATE TABLE ... REFERENCES" to Regex("""\bCREATE\s+TABLE\b""", RegexOption.IGNORE_CASE),
                "backfill UPDATE" to Regex("""^\s*UPDATE\s+""", RegexOption.IGNORE_CASE),
            )

        val SET_TIMEOUT = Regex("""\bSET\s+(LOCAL\s+)?lock_timeout\b""", RegexOption.IGNORE_CASE)

        /** A concurrent build: the one heavy statement that belongs OUTSIDE the window. */
        val CONCURRENT_BUILD =
            Regex("""\bCREATE\s+INDEX\s+CONCURRENTLY\b""", RegexOption.IGNORE_CASE)

        /** Lifting the statement bound for a build that may outlast it. */
        val SET_STATEMENT_TIMEOUT_ZERO =
            Regex("""\bSET\s+statement_timeout\s*=\s*0\b""", RegexOption.IGNORE_CASE)

        /** Putting the session's own statement bound back. */
        val RESTORE_STATEMENT_TIMEOUT =
            Regex(
                """set_config\(\s*'statement_timeout'\s*,\s*""" +
                    """current_setting\(\s*'hoglake\.migration_statement_timeout'""",
                RegexOption.IGNORE_CASE,
            )

        /** The save-and-restore pair's restore half (V9's shape). */
        val RESTORE_TIMEOUT =
            Regex(
                """set_config\(\s*'lock_timeout'\s*,\s*current_setting\(\s*'hoglake\.migration_lock_timeout'""",
                RegexOption.IGNORE_CASE,
            )

        @JvmStatic
        fun guardedMigrations(): List<String> =
            Files.list(Path.of(DIR)).use { stream ->
                stream.map { it.fileName.toString() }
                    .filter { it.endsWith(".sql") }
                    .filter { version(it) >= FIRST_GUARDED_VERSION }
                    .sorted()
                    .toList()
            }

        fun version(name: String): Int =
            Regex("""^V(\d+)__""").find(name)?.groupValues?.get(1)?.toInt()
                ?: error("not a versioned migration: $name")

        /**
         * Lines with their 1-based numbers, COMMENTS STRIPPED. Every one
         * of these files explains itself at length, and `-- ... DROP
         * INDEX ...` in a paragraph of prose is not a statement. The
         * stripper is line-level (`--` to end of line), which is all
         * these files use; a `/* */` block would need more, and there
         * are none.
         */
        fun statementLines(sql: String): List<Pair<Int, String>> =
            sql.lines().mapIndexed { i, line -> (i + 1) to line.substringBefore("--") }
                .filter { it.second.isNotBlank() }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("guardedMigrations")
    fun `every heavy-lock statement sits inside the lock_timeout window`(file: String) {
        val sql = Files.readString(Path.of(DIR, file))
        val lines = statementLines(sql)

        val heavy =
            lines.flatMap { (n, text) ->
                HEAVY.filter { (_, pattern) -> pattern.containsMatchIn(text) }
                    .map { (label, _) -> Triple(n, label, text.trim()) }
            }
        if (heavy.isEmpty()) return // nothing to guard; a data-only migration is fine

        val setAt = lines.firstOrNull { SET_TIMEOUT.containsMatchIn(it.second) }?.first
        assertThat(setAt)
            .describedAs(
                "%s takes heavy locks (%s) and never sets lock_timeout. A migration session has " +
                    "no bound of its own, so an unguarded statement queues behind one in-flight " +
                    "commit for up to the statement timeout, every reader queues behind IT, and " +
                    "every other booting pod waits on the Flyway advisory lock.",
                file,
                heavy.joinToString { "${it.second} at line ${it.first}" },
            )
            .isNotNull()

        // The restore is optional: a transactional migration uses SET
        // LOCAL and has nothing to restore. When it IS present, it
        // closes the window.
        val restoreAt = lines.firstOrNull { RESTORE_TIMEOUT.containsMatchIn(it.second) }?.first
        val closesAt = restoreAt ?: Int.MAX_VALUE

        for ((line, label, text) in heavy) {
            assertThat(line)
                .describedAs(
                    "%s:%d — %s runs BEFORE `SET lock_timeout` (line %d):%n  %s",
                    file,
                    line,
                    label,
                    setAt,
                    text,
                )
                .isGreaterThan(setAt!!)
            assertThat(line)
                .describedAs(
                    "%s:%d — %s runs AFTER the lock_timeout restore (line %d), which is V10's " +
                        "defect exactly: executeInTransaction=false makes the setting " +
                        "per-session, so WHERE it sits relative to the DDL is the whole of its " +
                        "effect.%n  %s",
                    file,
                    line,
                    label,
                    restoreAt,
                    text,
                )
                .isLessThan(closesAt)
        }
    }

    @Test
    fun `a concurrent build sits outside the window, with the statement bound lifted and restored`() {
        // The other half of the rule, and the half V17 made load-bearing.
        // `CREATE INDEX CONCURRENTLY` is exempt from the lock_timeout
        // window because it WAITS OUT older transactions by design — a
        // 5 s bound aborts exactly that wait — and it is exempt from the
        // session's `statement_timeout` for the same reason: V17's build
        // is 26 s at production size against a 60 s session bound
        // (Database.SESSION_INIT_SQL), and a build the bound kills leaves
        // an INVALID index behind and a failed history row that fails
        // Flyway's validate on every replica.
        //
        // So a file that builds concurrently must (a) close the
        // lock_timeout window BEFORE the build, and (b) lift the
        // statement bound around it and put it back — never leave a
        // session with no statement bound at all, because these files
        // run with executeInTransaction=false and the session outlives
        // them.
        //
        // NEITHER RESTORE RUNS IF A STATEMENT BETWEEN THEM FAILS, and
        // this test cannot see that: it reads the file, and the file
        // has no failure path to read. What makes it survivable is the
        // CALLER — `Main.kt` migrates before Netty binds, so a throw
        // takes the JVM and the pooled connection carrying the settings
        // with it, and nothing later can inherit them. A future caller
        // that migrates on a live pool (an admin endpoint, a harness
        // reusing the pool) would leak `lock_timeout = 5s` /
        // `statement_timeout = 0` into every later statement on that
        // connection. V14, V15 and V17 all have this shape.
        for (file in guardedMigrations()) {
            val lines = statementLines(Files.readString(Path.of(DIR, file)))
            val builds = lines.filter { CONCURRENT_BUILD.containsMatchIn(it.second) }
            if (builds.isEmpty()) continue

            val restoreLock = lines.firstOrNull { RESTORE_TIMEOUT.containsMatchIn(it.second) }?.first
            assertThat(restoreLock)
                .describedAs(
                    "%s builds CONCURRENTLY (line %d) and never closes the lock_timeout window; " +
                        "a 5 s bound aborts the build's own wait for older transactions",
                    file,
                    builds.first().first,
                )
                .isNotNull()

            val lifted = lines.firstOrNull { SET_STATEMENT_TIMEOUT_ZERO.containsMatchIn(it.second) }?.first
            val restored = lines.firstOrNull { RESTORE_STATEMENT_TIMEOUT.containsMatchIn(it.second) }?.first
            assertThat(lifted)
                .describedAs(
                    "%s builds CONCURRENTLY (line %d) under the session's own statement_timeout; " +
                        "a build the bound kills leaves an INVALID index and a failed history row",
                    file,
                    builds.first().first,
                )
                .isNotNull()
            assertThat(restored)
                .describedAs(
                    "%s lifts statement_timeout and never restores it; the session outlives the " +
                        "file (executeInTransaction=false), so every later statement runs unbounded",
                    file,
                )
                .isNotNull()

            for ((line, text) in builds) {
                assertThat(line)
                    .describedAs(
                        "%s:%d — a concurrent build must follow the lock_timeout restore (line " +
                            "%d):%n  %s",
                        file,
                        line,
                        restoreLock,
                        text.trim(),
                    )
                    .isGreaterThan(restoreLock!!)
                assertThat(line)
                    .describedAs(
                        "%s:%d — a concurrent build must sit between the statement_timeout lift " +
                            "(line %d) and its restore (line %d):%n  %s",
                        file,
                        line,
                        lifted,
                        restored,
                        text.trim(),
                    )
                    .isBetween(lifted!! + 1, restored!! - 1)
            }
        }
    }

    @Test
    fun `a file that restores lock_timeout also saved it, and vice versa`() {
        // The pair is the whole mechanism: a restore with no save reads
        // an unset GUC, and a save with no restore leaks a 5s bound into
        // every later statement on a session that outlives the file.
        for (file in guardedMigrations()) {
            val sql = Files.readString(Path.of(DIR, file))
            val lines = statementLines(sql)
            val saved =
                lines.any {
                    it.second.contains("set_config('hoglake.migration_lock_timeout'", ignoreCase = true)
                }
            val restored = lines.any { RESTORE_TIMEOUT.containsMatchIn(it.second) }
            assertThat(saved)
                .describedAs("%s restores lock_timeout without saving it first", file)
                .isEqualTo(restored)
        }
    }

    @Test
    fun `a non-transactional migration saves and restores rather than using SET LOCAL`() {
        // SET LOCAL in a file Flyway runs OUTSIDE a transaction is a
        // silent no-op-shaped bug: Postgres warns and the setting
        // applies to nothing, so the DDL below it runs unbounded. Which
        // shape a file must use is decided by its `.conf`, so that is
        // what this reads.
        for (file in guardedMigrations()) {
            val sql = Files.readString(Path.of(DIR, file))
            if (statementLines(sql).none { SET_TIMEOUT.containsMatchIn(it.second) }) continue
            val conf = Path.of(DIR, "$file.conf")
            val nonTransactional =
                Files.exists(conf) &&
                    Files.readString(conf).contains("executeInTransaction=false", ignoreCase = true)
            val usesSetLocal =
                statementLines(sql).any {
                    Regex("""\bSET\s+LOCAL\s+lock_timeout\b""", RegexOption.IGNORE_CASE)
                        .containsMatchIn(it.second)
                }
            if (nonTransactional) {
                assertThat(usesSetLocal)
                    .describedAs(
                        "%s runs with executeInTransaction=false, where SET LOCAL applies to no " +
                            "transaction at all; it must SAVE and RESTORE lock_timeout instead",
                        file,
                    )
                    .isFalse()
            } else {
                assertThat(usesSetLocal)
                    .describedAs(
                        "%s runs in a transaction, so SET LOCAL is the right form — it expires " +
                            "with the transaction and needs no restore",
                        file,
                    )
                    .isTrue()
            }
        }
    }

    /**
     * THE CHAIN IS CONTIGUOUS, which is the guard the V23/V24 collision
     * showed was missing.
     *
     * Flyway runs with `outOfOrder = false` and `validateOnMigrate = true`
     * (`Database.flywayConfig` sets neither, so both are the defaults), so
     * a migration whose version is BELOW the highest one already applied
     * is not run — it is a validate failure, on every replica, at boot:
     * `Detected resolved migration not applied to database: N`. Two
     * branches developing V23 and V24 in parallel therefore have a merge
     * order that matters, and until this assertion existed nothing in the
     * repo said so: the second branch to merge would have taken the whole
     * fleet into a boot crash-loop, with the mitigation living only in a
     * reviewer's notes.
     *
     * A gap is the detectable form of that mistake, and it fails HERE — in
     * the unit lane, in under a second, on the PR that opens second —
     * rather than in production. The rule the failure states is the fix:
     * whichever branch merges second holds the HIGHER number, so the
     * branch that finds a gap renumbers its file, its `.conf` and the
     * `freshDatabaseAt` arrival in its test.
     *
     * Deliberately NOT asserting that the chain starts at 1 or reaches any
     * particular N: V5 never existed (the chain has been 1,2,3,4,6,… since
     * before the freeze, which is why the expected set is built from the
     * MINIMUM present rather than from 1), and the head moves with every
     * change.
     */
    @Test
    fun `the migration chain has no gaps, so a parallel-branch collision fails here`() {
        val versions =
            Files.list(Path.of(DIR)).use { stream ->
                stream.map { it.fileName.toString() }
                    .filter { it.endsWith(".sql") }
                    .toList()
            }.map { version(it) }.sorted()
        assertThat(versions).doesNotHaveDuplicates()
        val expected = (versions.first()..versions.last()).filterNot { it in ALLOWED_GAPS }
        assertThat(versions)
            .describedAs(
                "a gap means some version below the chain's head is missing. Flyway runs " +
                    "outOfOrder=false and validateOnMigrate=true, so a migration that later " +
                    "arrives to fill it is never applied — it is a boot-time validate failure on " +
                    "every replica. Either renumber the new migration above the head, or, if a " +
                    "sibling branch legitimately owns that number, add it to ALLOWED_GAPS with " +
                    "the branch named",
            )
            .isEqualTo(expected)
    }

    /**
     * The register above cannot rot.
     *
     * An [ALLOWED_GAPS] entry for a version that now EXISTS means the
     * sibling merged and nobody pruned the exemption — which leaves a
     * permanent hole in the guard at exactly the number most likely to be
     * reused next. Failing here is cheap; the alternative is the gate
     * quietly not covering a version forever.
     */
    @Test
    fun `no stale entry survives the sibling's merge`() {
        val present =
            Files.list(Path.of(DIR)).use { stream ->
                stream.map { it.fileName.toString() }
                    .filter { it.endsWith(".sql") }
                    .toList()
            }.map { version(it) }.toSet()
        assertThat(ALLOWED_GAPS.filter { it in present })
            .describedAs(
                "these versions are in the chain now, so their ALLOWED_GAPS entries are stale " +
                    "and must be removed — an exemption for a version that exists is a hole in " +
                    "the contiguity guard",
            )
            .isEmpty()
    }
}
