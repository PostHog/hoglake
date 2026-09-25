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
 * The cleanup drain's object-store calls run INSIDE a transaction that
 * holds the per-catalog commit lock, so the SDK's call bound is not a
 * number somebody liked — it is an INEQUALITY against two bounds that
 * are already in force on that transaction, and it is worth nothing
 * unless it sits under both.
 *
 * A unit test, deliberately: the failure it guards is a bound that
 * silently stops being reachable when one of the other two moves, and
 * that is a fact about three constants rather than about a running
 * system. It reds in the fast lane, with no Docker.
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
    fun `the call bound sits under the idle-in-transaction bound`() {
        // A connection waiting on an S3 response IS idle in transaction.
        // Past this bound Postgres kills the backend, the sub-batch
        // rolls back with its objects already deleted and its ledger
        // rows unsettled, and the SDK's own timeout never fires — so a
        // call bound at or above it cannot do its job.
        assertThat(store.apiCallTimeout)
            .describedAs("the SDK call bound must be able to fire before Postgres kills the backend")
            .isLessThan(idleFromSql)
    }

    @Test
    fun `the call bound sits under the commit admission bound`() {
        // HOGLAKE_COMMIT_LOCK_TIMEOUT_MS is how long a foreground commit
        // waits for this lock before answering a typed 503. A hold
        // longer than that turns every concurrent writer's commit into
        // backpressure, which is the production failure this whole
        // change is about.
        assertThat(store.apiCallTimeout)
            .describedAs("a hold longer than commit admission 503s every concurrent writer")
            .isLessThan(Duration.ofMillis(CommitService.DEFAULT_COMMIT_LOCK_TIMEOUT_MS))
    }

    @Test
    fun `the bound follows the admission bound this process actually runs with`() {
        // Derived, not written down: an operator who lowers
        // HOGLAKE_COMMIT_LOCK_TIMEOUT_MS to keep commits responsive must
        // get a lower call bound with it, or the drain's hold outlives
        // the admission window the operator just chose. App.kt passes
        // cfg.commitLockTimeoutMs into RemovalStore for this reason.
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
    fun `the hold budget plus one call fits inside the idle bound`() {
        // The whole inequality, in one assertion:
        //   hold <= HOLD_BUDGET + one call <= the idle bound.
        // CleanupService checks the budget before every object-store
        // call and requires a whole call bound of headroom, so the last
        // call a sub-batch starts cannot run past the idle bound.
        assertThat(CleanupService.HOLD_BUDGET.plus(store.apiCallTimeout))
            .describedAs("a sub-batch's worst-case hold must fit inside idle_in_transaction_session_timeout")
            .isLessThanOrEqualTo(idleFromSql)
        assertThat(CleanupService.HOLD_BUDGET)
            .describedAs("and the budget must leave room for at least one call, or nothing drains")
            .isGreaterThanOrEqualTo(store.apiCallTimeout)
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
