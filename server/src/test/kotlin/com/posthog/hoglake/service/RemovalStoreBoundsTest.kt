package com.posthog.hoglake.service

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import software.amazon.awssdk.services.s3.model.S3Error
import java.time.Duration

/**
 * The two things about [RemovalStore] that are decisions rather than
 * plumbing, and that no integration test can see: the SDK call bounds,
 * and how one `DeleteObjects` response's error list is read.
 *
 * The overall SDK budget is capped independently of database session
 * timeouts, follows shorter admission bounds, and leaves time for
 * retries. The claim lease must accommodate the resulting call budgets.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RemovalStoreBoundsTest {
    /**
     * A client is built (no call is made), so it owns an HTTP connection
     * pool and a set of threads and has to be closed. No endpoint and no
     * credentials: nothing here reaches a network.
     */
    private val store =
        RemovalStore(endpoint = null, region = "us-east-1", accessKey = null, secretKey = null, pathStyle = true)

    @AfterAll
    fun tearDown() = store.close()

    @Test
    fun `default SDK budgets cap the call and leave time for retries`() {
        assertThat(store.apiCallTimeout).isEqualTo(Duration.ofSeconds(10))
        assertThat(store.apiCallAttemptTimeout).isEqualTo(Duration.ofSeconds(5))
    }

    @Test
    fun `long or unbounded admission waits retain the call cap`() {
        for (admissionMs in listOf(Long.MIN_VALUE, -1L, 0L, 30_000L, 30_001L, 60_000L, Long.MAX_VALUE)) {
            assertThat(RemovalStore.callBoundFor(admissionMs))
                .describedAs("call budget with admission bound %d ms", admissionMs)
                .isEqualTo(Duration.ofSeconds(10))
        }
    }

    @Test
    fun `a whole CLAIM's worst case fits inside the claim lease, for both arms`() {
        // THE INEQUALITY THAT REPLACED THE HOLD BUDGET, and it is about a
        // CLAIM — the unit the lease is stamped on — not about a
        // sub-batch. An earlier version of this test priced
        // STAGING_SUB_BATCH probe pairs and called that a claim; the claim
        // then had no reason predicate and could hold SUB_BATCH tickets,
        // so the real worst case was 2 x 1,000 x 10 s = 20,000 s against a
        // 900 s lease, 22x. The claim is now reason-aware, which is what
        // makes the inequality true rather than the arithmetic convenient.
        val lease = Duration.ofSeconds(CleanupService.CLAIM_LEASE_SECONDS)

        // STAGING: STAGING_SUB_BATCH tickets, two calls each (HEAD then
        // DELETE, because only that pair can report 'absent'), every call
        // at the bound.
        val stagingWorstCase =
            store.apiCallTimeout.multipliedBy(2L * CleanupService.STAGING_SUB_BATCH)
        assertThat(stagingWorstCase)
            .describedAs(
                "a staging CLAIM is %d tickets x 2 calls at the %s call bound",
                CleanupService.STAGING_SUB_BATCH,
                store.apiCallTimeout,
            )
            .isLessThan(lease)

        // BULK: SUB_BATCH paths, but the calls are DeleteObjects requests,
        // one per bucket chunk of MAX_KEYS_PER_DELETE keys — so the call
        // count is the chunk count, not the row count. Priced at the
        // ceiling with a generous allowance for paths spread across
        // several buckets.
        // AN ASSUMPTION, NOT A WORST CASE, and worth saying so: a claim's
        // 1,000 paths are one DeleteObjects request per bucket, and
        // nothing in the schema caps the buckets a catalog's paths span.
        // Eight is generous against today's deployments, where a catalog's
        // `data_path` is one bucket; ninety would exceed the lease.
        val chunksPerBucketSpread = 8
        val chunksPerSubBatch =
            (CleanupService.SUB_BATCH + RemovalStore.MAX_KEYS_PER_DELETE - 1) /
                RemovalStore.MAX_KEYS_PER_DELETE
        val bulkChunks = chunksPerBucketSpread * chunksPerSubBatch
        val bulkWorstCase = store.apiCallTimeout.multipliedBy(bulkChunks.toLong())
        assertThat(bulkWorstCase)
            .describedAs(
                "a bulk CLAIM is %d rows = %d DeleteObjects requests even spread over %d buckets",
                CleanupService.SUB_BATCH,
                bulkChunks,
                chunksPerBucketSpread,
            )
            .isLessThan(lease)

        // And the shape that does NOT fit — the ARITHMETIC THAT MOTIVATES
        // the split, not a guard against undoing it: this is an assertion
        // over constants, and merging the two claim statements back into
        // one would leave it green. What actually reds that mutation is
        // `CleanupServiceIntegrationTest.a claim never mixes the two
        // reasons, and a staging claim is capped at STAGING_SUB_BATCH`,
        // which counts the statements a run issues.
        assertThat(store.apiCallTimeout.multipliedBy(2L * CleanupService.SUB_BATCH))
            .describedAs(
                "one claim of SUB_BATCH staging tickets could not fit inside the lease, which is " +
                    "why no such claim is issued",
            )
            .isGreaterThan(lease)
    }

    @Test
    fun `the bound follows the admission bound this process actually runs with`() {
        assertThat(RemovalStore.callBoundFor(6_000))
            .describedAs("a 6s admission bound is the binding one; the call bound follows it")
            .isEqualTo(Duration.ofSeconds(2))
        assertThat(RemovalStore.callBoundFor(29_999)).isEqualTo(Duration.ofNanos(9_999_666_666))
        assertThat(RemovalStore.callBoundFor(1)).isEqualTo(Duration.ofNanos(333_333))
    }

    @Test
    fun `shorter admission bounds reach both SDK budgets`() {
        RemovalStore(
            endpoint = null,
            region = "us-east-1",
            accessKey = null,
            secretKey = null,
            pathStyle = true,
            commitLockTimeoutMs = 6_000,
        ).use { shorter ->
            assertThat(shorter.apiCallTimeout).isEqualTo(Duration.ofSeconds(2))
            assertThat(shorter.apiCallAttemptTimeout).isEqualTo(Duration.ofSeconds(1))
        }
    }

    @Test
    fun `an attempt bound sits under the call bound, because attempts SHARE the call's budget`() {
        // `apiCallTimeout` is an OVERALL budget for the call: every
        // attempt and all the backoff between them come out of it,
        // rather than each attempt getting its own. So without a
        // per-attempt bound one stalled attempt consumes the whole
        // budget and the call fails having never retried — the opposite
        // of what a retryable object-store fault wants.
        assertThat(store.apiCallAttemptTimeout)
            .isLessThan(store.apiCallTimeout)
            .isGreaterThan(Duration.ZERO)
    }

    // ---- the DeleteObjects response's error list ---------------------------

    private fun error(
        key: String,
        code: String = "AccessDenied",
    ): S3Error = S3Error.builder().key(key).code(code).message("nope").build()

    private val chunk = listOf("a.parquet" to "s3://b/a.parquet", "c.parquet" to "s3://b/c.parquet")

    @Test
    fun `an error for a key we sent fails only that key`() {
        assertThat(store.failuresFor("b", chunk, listOf(error("a.parquet"))))
            .containsOnlyKeys("s3://b/a.parquet")
    }

    @Test
    fun `an error naming a key we did not send fails the WHOLE chunk`() {
        // The response is the only record of what happened to these
        // objects, and an unrecognised key in it says the response
        // cannot be trusted to name the survivors. Settling the rest
        // 'deleted' on the strength of it would leave an object with no
        // ledger row — and the removal queue is the only thing that ever
        // knew the path, so that orphan is invisible and permanent. A
        // retried delete costs one round trip.
        val failures = store.failuresFor("b", chunk, listOf(error("ghost.parquet")))
        assertThat(failures.keys)
            .describedAs("every path in the chunk stays queued, none is settled on a response we cannot read")
            .containsExactlyInAnyOrder("s3://b/a.parquet", "s3://b/c.parquet")
        assertThat(failures.values).allSatisfy { assertThat(it).contains("ghost.parquet") }
    }

    @Test
    fun `no errors is no failures`() {
        assertThat(store.failuresFor("b", chunk, emptyList())).isEmpty()
    }
}
