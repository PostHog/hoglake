package com.posthog.hoglake.compaction

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * The two key spaces in `hog_compaction_claim` stay disjoint.
 *
 * Group claims and bucket claims share the table, and every reader tells
 * them apart by the `bucket:` prefix — the overlap read ignores bucket
 * rows because their `input_file_ids` is empty, but the pre-probe read
 * and any count of groups filter on the prefix. An empty or changed
 * prefix would make bucket claims indistinguishable from group claims
 * to those readers, silently.
 *
 * Unit: pure functions, no database.
 */
class CompactionClaimKeyTest {
    @Test
    fun `a bucket key is the literal bucket prefix and a 64-hex digest`() {
        // MUTATION: set `BUCKET_KEY_PREFIX = ""` and this reds (the
        // product `require` in `bucketKey` reds first).
        assertThat(CompactionClaimRepo.BUCKET_KEY_PREFIX).isEqualTo("bucket:")
        val key = CompactionClaimRepo.bucketKey(3, listOf("2026-10-21"))
        assertThat(key).matches("bucket:[0-9a-f]{64}")
    }

    @Test
    fun `bucket keys are stable, and distinguish spec, values, null and empty`() {
        val a = CompactionClaimRepo.bucketKey(3, listOf("2026-10-21"))
        assertThat(CompactionClaimRepo.bucketKey(3, listOf("2026-10-21"))).isEqualTo(a)
        assertThat(
            setOf(
                a,
                CompactionClaimRepo.bucketKey(4, listOf("2026-10-21")),
                CompactionClaimRepo.bucketKey(3, listOf("2026-10-22")),
                CompactionClaimRepo.bucketKey(3, listOf(null)),
                CompactionClaimRepo.bucketKey(3, listOf("")),
                CompactionClaimRepo.bucketKey(null, null),
                CompactionClaimRepo.bucketKey(3, listOf("a", "b")),
                CompactionClaimRepo.bucketKey(3, listOf("ab")),
            ),
        ).hasSize(8)
    }
}
