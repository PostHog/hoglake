package com.posthog.hoglake.api

import com.posthog.hoglake.Config
import com.posthog.hoglake.Database
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.VerifyService
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.StatementContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.File
import java.util.concurrent.CopyOnWriteArrayList

/**
 * The verify surface, read off BOTH artifacts rather than restated in
 * either.
 *
 * The check names are the contract — ids clients switch on — and they
 * live in three places nothing else keeps in step: the `VerifyCheck`
 * enum in the spec, the endpoint's own prose, and the server. So the
 * server's list comes from a REAL REPORT (`runOnce().checks`), which is
 * the only thing that knows which checks exist and is exactly what a
 * client receives; the spec's comes from parsing the file. A literal
 * list here would have been a third copy, and renaming a check would
 * have left it green.
 *
 * Equality is ORDERED and exact in both directions. `contains` loops
 * pass on a spec that lists a check the server dropped, which is the
 * failure a client generated from the spec actually meets.
 *
 * The DESCRIPTIONS are deliberately not compared: the spec's are
 * abridged, and pinning 8 KB of duplicated prose whose only reader is
 * this assertion would be worse than the drift it prevents. The spec
 * says as much, and the response is the authority.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class VerifySpecParityTest {
    private val db = PgTestSupport.freshDatabase()
    private val spec: String = File("src/main/resources/openapi/hoglake.yaml").readText()

    @AfterAll
    fun tearDown() = db.close()

    /** The server's own list, from a report rather than a literal. */
    private val serverChecks: List<String> by lazy {
        CatalogService(db.jdbi).createCatalog("spec-parity", "s3://spec-parity")
        VerifyService(db.jdbi, retirementIntervalMs = 0).runOnce("spec-parity").checks.map { it.check }
    }

    private fun block(header: String): String {
        val start = spec.indexOf(header)
        assertThat(start).describedAs("spec block %s", header).isNotEqualTo(-1)
        val rest = spec.substring(start + header.length)
        // Up to the next sibling key at the same indentation.
        val end = Regex("\n    [A-Za-z]").find(rest)?.range?.first ?: rest.length
        return rest.substring(0, end)
    }

    /** The values of a YAML flow-sequence `enum: [...]`, possibly wrapped. */
    private fun enumValues(block: String): List<String> {
        val body = Regex("""enum: \[([^]]*)]""", RegexOption.DOT_MATCHES_ALL).find(block)!!.groupValues[1]
        return body.split(",").map { it.trim() }.filter { it.isNotEmpty() }
    }

    @Test
    fun `the spec's VerifyCheck enum is exactly the checks a report returns, in order`() {
        assertThat(serverChecks).hasSize(12)
        assertThat(enumValues(block("    VerifyCheck:")))
            .describedAs("the spec's check vocabulary must be the server's, with no extras either way")
            .isEqualTo(serverChecks)
    }

    private fun endpointBlock(): String =
        spec.substring(
            spec.indexOf("  /catalogs/{catalog}/maintenance/verify:"),
            spec.indexOf("  /catalogs/{catalog}/maintenance/rehydrate:"),
        )

    @Test
    fun `the endpoint description documents exactly those checks, in order`() {
        val endpoint = endpointBlock()
        val documented = Regex("""\*\*([a-z_]+)\*\*""").findAll(endpoint).map { it.groupValues[1] }.toList()
        assertThat(documented)
            .describedAs("the prose must document every check and invent none")
            .isEqualTo(serverChecks)
    }

    @Test
    fun `description is a documented property, and an optional one`() {
        val schema = block("    VerifyCheck:")
        // The PROPERTY keys, at the schema's own indentation — not a
        // `contains("description:")`, which every property satisfies
        // through its own nested description and which therefore could
        // not fail.
        val properties =
            Regex("""(?m)^        ([a-z_]+):$""").findAll(schema).map { it.groupValues[1] }.toList()
        assertThat(properties)
            .describedAs("the schema must actually declare the field")
            .containsExactly("check", "status", "violations", "samples", "description")
        val required = requiredOf(schema)
        assertThat(required)
            .describedAs("a required description would contradict every stored ledger row")
            .containsExactly("check", "status", "violations", "samples")
        assertThat(required)
            .describedAs("the ledger stores checks without one, so it cannot be required")
            .doesNotContain("description")
    }

    private fun requiredOf(schema: String): List<String> {
        val match = Regex("""required: \[([^]]*)]""").find(schema)
        assertThat(match).describedAs("schema block has no `required:` list").isNotNull()
        return match!!.groupValues[1].split(",").map { it.trim() }
    }

    @Test
    fun `the endpoint names the verify loop's ACTUAL default interval`() {
        // The prose said "hourly by default" while the default was 0.
        // Read the default from Config rather than restating it: the
        // knob is an ops decision (like compaction's), and a spec that
        // advertises a cadence nobody runs is how an operator concludes
        // the sweep is already covering them.
        val default = Config().verifyIntervalMs
        assertThat(default).describedAs("the loop is off unless a workload turns it on").isZero()
        assertThat(endpointBlock().replace(Regex("""\s+"""), " "))
            .describedAs("the endpoint must name the interval the code actually defaults to")
            .contains("HOGLAKE_VERIFY_INTERVAL_MS=$default")
    }

    @Test
    fun `the spec's task vocabulary and its records-every-sweep split match the enum`() {
        assertThat(enumValues(block("    MaintenanceTask:")))
            .isEqualTo(MaintenanceTask.entries.map { it.wire })

        // The prose splits the tasks into two named lists; parse them
        // and check the split against the enum rather than pinning a
        // sentence fragment, which goes stale silently and says nothing
        // about the code.
        // Folded YAML wraps prose across lines, so the block is
        // flattened before the lists are read out of it.
        val loop = block("    LoopObservation:").replace(Regex("""\s+"""), " ")
        val (always, sometimes) =
            Regex("""True \(([a-z, ]+)\)""").find(loop)!!.groupValues[1] to
                Regex("""False \(([a-z, ]+)\)""").find(loop)!!.groupValues[1]

        fun names(s: String) = s.split(",").map { it.trim() }.sorted()
        assertThat(names(always))
            .describedAs("tasks whose every sweep records a run")
            .isEqualTo(MaintenanceTask.entries.filter { it.loopRecordsEverySweep }.map { it.wire }.sorted())
        assertThat(names(sometimes))
            .describedAs("tasks whose silence says nothing about the loop")
            .isEqualTo(MaintenanceTask.entries.filter { !it.loopRecordsEverySweep }.map { it.wire }.sorted())
        assertThat(MaintenanceTask.entries)
            .describedAs("every task has a loop, so loop_interval_ms is never absent")
            .allSatisfy { assertThat(it.hasLoop).isTrue() }
    }

    @Test
    fun `every statement a real run issues across two path-keyed tables is a registered one`() {
        // The honest proof, and the reason it is not a source grep.
        //
        // Counting `.path = ` in VerifyService.kt is evadable in every
        // direction: `USING (path)` and `IN (SELECT path ...)` are the
        // same join with no such text, spacing changes the count, a
        // COUNT is not a SET so two queries can trade an occurrence
        // between them, and the file is not even the whole surface —
        // fragments are spliced in from ExpiryService and OffsetRepo.
        //
        // So this watches what the service ACTUALLY RUNS. Every
        // statement of a real report that touches more than one of the
        // path-keyed tables is a path join by construction, whatever
        // syntax it is written in, and each one must be a registered
        // PathQuery put through the same `bounded` wrapper production
        // uses. The registry stays the single door.
        val issued = CopyOnWriteArrayList<String>()
        val instrumented = Database.jdbi(db.dataSource)
        instrumented.setSqlLogger(
            object : SqlLogger {
                override fun logAfterExecution(context: StatementContext) {
                    issued += context.renderedSql
                }
            },
        )
        CatalogService(instrumented).createCatalog("spec-sql", "s3://spec-sql")
        VerifyService(instrumented, retirementIntervalMs = 0).runOnce("spec-sql")

        // Two of the path-keyed tables AND the word `path` anywhere in
        // the statement. Deliberately not "`.path =` twice": `USING
        // (path)`, `IN (SELECT path ...)` and a correlated subquery all
        // read differently and are the same join. The one thing they
        // cannot do is join on path without mentioning it.
        //
        // Two path-keyed tables WITHOUT the word is a different join
        // entirely — delete_vectors pairs hog_delete_file to
        // hog_data_file on data_file_id, which is a primary key — and
        // is correctly out of scope here.
        val pathToken = Regex("""\bpath\b""")
        val multiTable =
            issued.filter { sql ->
                PATH_KEYED_TABLES.count { sql.contains(it) } > 1 && pathToken.containsMatchIn(sql)
            }
        assertThat(multiTable)
            .describedAs("a report that joins no path-keyed tables would make this vacuous")
            .isNotEmpty()

        val registered =
            VerifyService.PATH_EQUALITY_QUERIES.values
                .map { normalize(VerifyService.bounded(it.sql, it.orderBy)) }
                .toSet()
        val unregistered = multiTable.filter { normalize(it) !in registered }
        assertThat(unregistered)
            .describedAs(
                "a statement joining path-keyed tables that is not a registered PathQuery has " +
                    "no plan test behind it — register it in " +
                    "VerifyService.PATH_EQUALITY_QUERIES (see VerifyQueryPlanIntegrationTest)",
            )
            .isEmpty()

        // ...and every registered query really was one of them, so the
        // map cannot carry a statement the service stopped running.
        assertThat(multiTable.map { normalize(it) }.toSet())
            .describedAs("a registered PathQuery that no run issues is dead weight in the map")
            .containsAll(registered)
    }

    private fun normalize(sql: String): String = sql.replace(Regex("""\s+"""), " ").trim()

    private companion object {
        /**
         * The tables a check can only join BY PATH: none of them carries
         * an index on `path`, so any statement touching two of them is
         * the shape the plan test exists to bound.
         */
        val PATH_KEYED_TABLES =
            listOf("hog_data_file", "hog_delete_file", "hog_file_removal", "hog_upload")
    }
}
