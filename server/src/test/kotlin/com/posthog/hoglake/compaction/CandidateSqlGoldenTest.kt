package com.posthog.hoglake.compaction

import com.posthog.hoglake.hydrator.ObjectStore
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.security.MessageDigest

/**
 * The candidate statement's TEXT for a table WITHOUT the object-prefix
 * layout is byte-identical to what it was before the layout existed, and
 * the layout's `verifiedOnly` form differs from it by exactly one
 * fragment.
 *
 * The plan tests (`CompactionCandidateFetchPlanIntegrationTest`,
 * `CompactionBucketProbePlanIntegrationTest`, `CandidateSqlEquivalenceIntegrationTest`)
 * EXPLAIN the option-off text and measure it; a filter appended to every
 * table's probe would leave most of their assertions green (every fixture
 * file is `provided`), so the text itself is pinned here, by digest, for
 * the four shapes the planner sends. The digests were taken from main
 * before 2.1d. A deliberate change to the statement updates them in the
 * same change and says why.
 *
 * Unit, not integration: the statement is a pure function of its flags.
 * Neither the Jdbi nor the store is ever used.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CandidateSqlGoldenTest {
    private val store = ObjectStore("http://127.0.0.1:9", "us-east-1", "unused", "unused", true)
    private val svc =
        CompactionService(
            Jdbi.create("jdbc:postgresql://127.0.0.1:9/unused"),
            store,
            CompactionConfig(targetBytes = 1024, maxGroupsPerRun = 1),
        )

    @AfterAll
    fun tearDown() = store.close()

    private data class Shape(
        val name: String,
        val arms: List<Int>,
        val nullArms: Set<Int>,
        val scopeToSpec: Boolean,
        val withPartitionValues: Boolean,
    )

    /** What `bucketCandidates` (one key, two keys with a null, no tuple) and `fetchWholeTable` send. */
    private val shapes =
        listOf(
            Shape("one-key bucket", listOf(0), emptySet(), scopeToSpec = true, withPartitionValues = false),
            Shape("two-key bucket, null key", listOf(0, 1), setOf(1), scopeToSpec = true, withPartitionValues = false),
            Shape("tuple-less bucket", emptyList(), emptySet(), scopeToSpec = true, withPartitionValues = false),
            Shape("no-sample fallback", emptyList(), emptySet(), scopeToSpec = false, withPartitionValues = true),
        )

    private fun sha256(s: String): String =
        MessageDigest.getInstance("SHA-256").digest(s.toByteArray()).joinToString("") { "%02x".format(it) }

    private fun sql(
        shape: Shape,
        verifiedOnly: Boolean = false,
    ): String = svc.candidateSql(shape.arms, shape.nullArms, shape.scopeToSpec, shape.withPartitionValues, verifiedOnly)

    @Test
    fun `the option-off statement is byte-identical to the one before the object-prefix layout`() {
        // MUTATION: append the verified-only fragment for every table (or
        // move it, or reword the shared text) and this reds.
        val digests = shapes.associate { it.name to sha256(sql(it)) }
        assertThat(digests)
            .describedAs("statements:\n%s", shapes.joinToString("\n----\n") { sql(it) })
            .isEqualTo(
                mapOf(
                    "one-key bucket" to "d4ee69911fb6efe0429ed0283e21ebb7721c9bab9f1294b48c5ebe0d1aa31d6c",
                    "two-key bucket, null key" to "ec7b6ce21c0256f1b4ae474fa4ca674fe98fdaec8be269a81e6da0613e9d7d44",
                    "tuple-less bucket" to "fcdf4acae3d770b25e46a207d3afb630a1b883be5a290d046c43035adc15c96b",
                    "no-sample fallback" to "874d5251c9bf15408da379ad784f53016ed19ac5ea5f620f6a3a231636169d2a",
                ),
            )
    }

    @Test
    fun `the verified-only statement is the option-off one plus the mount predicate on the inner scan`() {
        // One fragment, inside the scan cap: the LIMIT then counts only
        // files a per-value mount would serve, and nothing else moves, so
        // the option-off plan tests speak for the option-on statement's
        // shape (the plan test for it checks the index too).
        for (shape in shapes) {
            val on = sql(shape, verifiedOnly = true)
            assertThat(on).contains(CompactionService.VERIFIED_ONLY_SQL)
            assertThat(on.indexOf(CompactionService.VERIFIED_ONLY_SQL))
                .describedAs("%s: the predicate is inside the scan cap", shape.name)
                .isLessThan(on.indexOf("LIMIT :scanLimit"))
            assertThat(on.replace(CompactionService.VERIFIED_ONLY_SQL, ""))
                .describedAs(shape.name)
                .isEqualTo(sql(shape))
        }
        assertThat(CompactionService.VERIFIED_ONLY_SQL)
            .contains("f0.stats_state = 'provided'")
            .contains("NOT f0.prefix_quarantined")
    }
}
