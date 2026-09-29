package com.posthog.hoglake.service

import com.posthog.hoglake.Database
import com.posthog.hoglake.commit.CommitService
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
 * THE CALL BOUND IS NO LONGER AN INEQUALITY, and these tests say so
 * rather than asserting one that stopped being load-bearing. The drain
 * used to make its object-store calls inside a transaction that held the
 * per-catalog commit lock, so the bound had to sit under both
 * `idle_in_transaction_session_timeout` and the commit admission bound
 * or it could never fire usefully. Cleanup is now a claimed work queue:
 * the claim commits before the first call, the settle opens a new
 * transaction after the last, and no lock is taken at all — so what a
 * hung call costs is one parked worker holding a claim until its lease
 * expires.
 *
 * What is still worth pinning is that the number FOLLOWS the admission
 * knob (an operator who lowers HOGLAKE_COMMIT_LOCK_TIMEOUT_MS gets
 * shorter calls, which is the only reason it is derived rather than
 * written down) and that it stays under both of the bounds it was
 * derived from — not because they constrain it any more, but because a
 * bound above them would be a number chosen by nothing at all.
 *
 * A unit test, deliberately: the failure it guards is a derived bound
 * silently changing meaning when one of its inputs moves, and that is a
 * fact about three constants rather than about a running system. It reds
 * in the fast lane, with no Docker.
 *
 * The idle bound is PARSED out of [Database.SESSION_INIT_SQL] rather
 * than restated, because `SESSION_INIT_SQL` is the single source of
 * truth for what production sets and a helper that copies its values
 * asserts only that it compiles (the rule its own KDoc states).
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

    private val idleFromSql: Duration by lazy {
        val match =
            Regex("""idle_in_transaction_session_timeout\s*=\s*'(\d+)s'""")
                .find(Database.SESSION_INIT_SQL)
        requireNotNull(match) {
            "SESSION_INIT_SQL no longer sets idle_in_transaction_session_timeout in whole seconds: " +
                Database.SESSION_INIT_SQL
        }
        Duration.ofSeconds(match.groupValues[1].toLong())
    }

    @Test
    fun `the named idle bound is the one SESSION_INIT_SQL actually sets`() {
        assertThat(Database.SESSION_INIT_SQL_IDLE_TIMEOUT)
            .describedAs(
                "Database.SESSION_INIT_SQL_IDLE_TIMEOUT drifted from SESSION_INIT_SQL (%s)",
                Database.SESSION_INIT_SQL,
            )
            .isEqualTo(idleFromSql)
    }

    @Test
    fun `the call bound sits under both of the bounds it is derived from`() {
        // NEITHER CONSTRAINS IT ANY MORE — no transaction is open across
        // an object-store call and cleanup takes no lock, so a slow call
        // can neither be killed idle-in-transaction nor convoy a commit.
        // The bound is kept under both because `min(idle, admission) / 3`
        // is where the number came from and a bound above its own inputs
        // would be arbitrary.
        assertThat(store.apiCallTimeout)
            .describedAs("derived from min(idle, admission), so it sits under the idle bound")
            .isLessThan(idleFromSql)
        assertThat(store.apiCallTimeout)
            .describedAs("and under the admission bound, which is the one an operator tunes")
            .isLessThan(Duration.ofMillis(CommitService.DEFAULT_COMMIT_LOCK_TIMEOUT_MS))
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
        // Derived, not written down: an operator who lowers
        // HOGLAKE_COMMIT_LOCK_TIMEOUT_MS to keep a pod responsive gets
        // shorter object-store calls with it, which is the ONLY property
        // the derivation still buys now that no call runs inside a
        // transaction. App.kt passes cfg.commitLockTimeoutMs into
        // RemovalStore for this reason.
        assertThat(RemovalStore.callBoundFor(6_000))
            .describedAs("a 6s admission bound is the binding one; the call bound follows it")
            .isEqualTo(Duration.ofSeconds(2))
        // 0 is an UNBOUNDED wait, so it constrains nothing and the idle
        // bound is left as the only constraint.
        assertThat(RemovalStore.callBoundFor(0))
            .describedAs("an unbounded admission wait leaves the idle bound in charge")
            .isEqualTo(idleFromSql.dividedBy(3))
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
