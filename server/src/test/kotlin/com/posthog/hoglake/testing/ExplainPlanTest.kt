package com.posthog.hoglake.testing

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * The parser behind `VerifyQueryPlanIntegrationTest`'s rules, and in
 * particular the POLARITY of [ExplainPlan.Node.filtersPath].
 *
 * That flag carries the rule V16 paid for: a repeated probe on
 * `(catalog_id, path)` is a descent per candidate, while a repeated
 * probe on a PREFIX with `path` demoted to a `Filter` reads a range per
 * candidate and looks identical in every other number on a fixture
 * small enough. The integration test cannot show the flag is wired up —
 * its plans do not carry the shape (that is the point), so a parser
 * that always answered `false` would leave the assertion green forever.
 * Both answers are pinned here instead, on synthetic EXPLAIN text, with
 * no container.
 */
class ExplainPlanTest {
    private val probeOnTheKey =
        """
        ->  Nested Loop Left Join (actual rows=3.00 loops=1)
              Buffers: shared hit=4417
              ->  Index Scan using hog_data_file_path on hog_data_file f (actual rows=1.00 loops=1069)
                    Index Cond: ((catalog_id = '1'::bigint) AND (path = q.path))
                    Index Searches: 1069
                    Buffers: shared hit=4273
        """.trimIndent()

    private val probeOnAPrefix =
        """
        ->  Nested Loop Left Join (actual rows=3.00 loops=1)
              Buffers: shared hit=90210
              ->  Index Scan using hog_data_file_changefeed on hog_data_file f (actual rows=1.00 loops=1069)
                    Index Cond: (catalog_id = '1'::bigint)
                    Filter: (path = 's3://bucket/c1/main/events_raw/abc/def/part-0000000001.parquet'::text)
                    Rows Removed by Filter: 49999
                    Buffers: shared hit=90066
        """.trimIndent()

    @Test
    fun `a probe whose index condition carries path filters nothing`() {
        val probe = ExplainPlan.nodes(probeOnTheKey).single { it.loops > 1 }
        assertThat(probe.filtersPath)
            .describedAs("an Index Cond on `path` is not a Filter on `path`")
            .isFalse()
        assertThat(probe.buffers).isEqualTo(4273)
        assertThat(probe.loops).isEqualTo(1069)
    }

    @Test
    fun `a probe on a prefix with path demoted is detected`() {
        val probe = ExplainPlan.nodes(probeOnAPrefix).single { it.loops > 1 }
        assertThat(probe.filtersPath)
            .describedAs(
                "`Filter: (path = ...)` under a repeated index probe is the shape V16 measured: " +
                    "one descent, then a range read and discarded, per candidate",
            )
            .isTrue()
        assertThat(probe.buffers).isEqualTo(90066)
    }

    @Test
    fun `a node's detail stops at its first child, so a parent never inherits one`() {
        // The parse that makes the flag trustworthy: a `Filter:` on a
        // CHILD must not be read as the parent's, or every plan
        // containing the shape anywhere would fail wholesale and the
        // rule would be about the plan rather than about the node.
        val parent = ExplainPlan.nodes(probeOnAPrefix).single { it.loops == 1L }
        assertThat(parent.filtersPath)
            .describedAs("the Nested Loop above the probe carries no Filter of its own")
            .isFalse()
        assertThat(parent.buffers).isEqualTo(90210)
    }

    @Test
    fun `both halves of a shared buffers line are counted`() {
        // `shared hit=N read=M` is one line: a pattern anchored on
        // "shared read=" reads zero for M and a cold scan measures as
        // its cache hits.
        val text =
            """
            ->  Seq Scan on hog_data_file (actual rows=2.00 loops=1)
                  Buffers: shared hit=2608 read=15450 written=10327
            """.trimIndent()
        assertThat(ExplainPlan.nodes(text).single().buffers).isEqualTo(2608 + 15450)
    }
}
