package com.posthog.hoglake

import com.posthog.hoglake.model.HoglakeException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.concurrent.TimeUnit

/**
 * The admission arithmetic (#218): one budget, charged once.
 *
 * Taking blocking work off the event loop turned starvation into
 * queueing, and a queue nobody accounts for is a second invisible
 * budget. These are the two rules that keep the published
 * `HOGLAKE_COMMIT_LOCK_TIMEOUT_MS` contract meaning what it says — the
 * lock bound is charged the queue wait, and a request that has already
 * spent the whole budget queueing is shed before it borrows anything.
 *
 * A unit test, deliberately: the arithmetic is where the mutations
 * live, and it needs no Docker.
 */
class RequestAdmissionTest {
    /** An admission that queued [ms] and has just been admitted. */
    private fun admissionAgedMs(ms: Long) =
        RequestAdmission(System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(ms)).also { it.admit(System.nanoTime()) }

    @Test
    fun `the queue wait is frozen at admission and does not grow with the handler`() {
        // THE DISTINCTION IS THE WHOLE POINT. Elapsed-since-dispatch
        // grows with the handler's own execution, so a synchronous
        // maintenance sweep — POST .../maintenance/compact runs
        // runOnce on the handler thread — would reach the commit lock
        // claiming to have queued for longer than the admission bound,
        // and every group after the first 30 s of the run would get a
        // 1 s lock bound and a 503 blaming a queue that never happened.
        //
        // MUTATION: define `queueWaitMs()` as
        // `(System.nanoTime() - enqueuedNanos) / 1_000_000` (read live)
        // and both assertions red.
        val admission = RequestAdmission(System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(200))
        admission.admit(System.nanoTime())
        val atAdmission = admission.queueWaitMs()
        assertThat(atAdmission).isBetween(150L, 400L)

        // ... and now the "handler" runs for a while.
        Thread.sleep(600)
        assertThat(admission.queueWaitMs())
            .describedAs("handler execution is not queue wait")
            .isEqualTo(atAdmission)
        assertThat(RequestAdmission.remainingLockTimeoutMs(30_000, admission))
            .describedAs("a long-running handler must still get the whole remaining bound")
            .isBetween(29_600L, 29_850L)
    }

    @Test
    fun `the lock bound is charged the time the request already spent queued`() {
        // MUTATION: return `configured` unchanged (the subtraction
        // dropped) and this reds — which is the 55-s-of-server-time
        // case: 25 s queued plus a full 30 s lock wait.
        val remaining = RequestAdmission.remainingLockTimeoutMs(30_000, admissionAgedMs(25_000))
        assertThat(remaining).isBetween(4_000L, 5_200L)
    }

    @Test
    fun `a request that queued past the bound still gets a usable remainder`() {
        // A commit arriving with 40 ms of budget would 503 on arrival
        // for no useful reason — the lock may well be free.
        //
        // MUTATION: drop the floor (`configured - waited` alone) and
        // this reds with a remainder at or below zero, which
        // set_config would then apply as a lock_timeout of 0 —
        // Postgres's spelling for "wait forever", the exact opposite of
        // the intent.
        val remaining = RequestAdmission.remainingLockTimeoutMs(30_000, admissionAgedMs(40_000))
        assertThat(remaining).isEqualTo(RequestAdmission.MIN_REMAINING_LOCK_TIMEOUT_MS)
    }

    @Test
    fun `the floor never raises a bound an operator set below it`() {
        // `maxOf(MIN, ...)` alone would turn a configured 250 ms into
        // 1 s. A floor exists so a request does not arrive with a
        // useless remainder, never to overrule the operator.
        //
        // MUTATION: remove the `minOf(MIN, configured)` cap and this
        // reds at 1000.
        assertThat(RequestAdmission.remainingLockTimeoutMs(250, admissionAgedMs(0))).isEqualTo(250)
        assertThat(RequestAdmission.remainingLockTimeoutMs(250, admissionAgedMs(5_000))).isEqualTo(250)
    }

    @Test
    fun `no ambient request and an unbounded configuration both pass through untouched`() {
        // Background loops never queued for a handler thread, so their
        // bound is theirs. And 0 is the documented spelling of "wait
        // forever": subtracting from it would invent a bound nobody
        // asked for.
        //
        // The null half is compiler-enforced: `admission` is nullable,
        // so removing that guard does not build.
        //
        // The `configured <= 0` half is NOT pinned by a mutation, and
        // the honest statement is that it cannot be: the floor
        // arithmetic below it already returns 0 for 0
        // (`minOf(MIN, 0)` is 0, and `maxOf(0, 0 - waited)` is 0), so
        // deleting the branch changes no answer. It stays as a stated
        // rule rather than an emergent one — "unbounded means
        // unbounded" is the documented meaning of
        // HOGLAKE_COMMIT_LOCK_TIMEOUT_MS=0 and should not depend on two
        // `min`/`max` calls agreeing — and what this asserts is the
        // BEHAVIOUR, which any future rewrite of the arithmetic must
        // keep.
        assertThat(RequestAdmission.remainingLockTimeoutMs(30_000, null)).isEqualTo(30_000)
        assertThat(RequestAdmission.remainingLockTimeoutMs(0, admissionAgedMs(9_000))).isEqualTo(0)
        assertThat(RequestAdmission.current())
            .describedAs("nothing outside a dispatched request carries an ambient admission")
            .isNull()
    }

    @Test
    fun `the remaining budget is the admission bound less the queue wait, and nothing when unbounded`() {
        // What a backend call made on the request's behalf has to fit
        // inside (#679): the same arithmetic and floor as the lock
        // bound, read off the budget the seam dispatched the request
        // under, so the hoglite client needs no view of the config.
        //
        // MUTATION: return `budgetMs` unchanged and the first reds; drop
        // the `<= 0` guard and the last reds with 0 where "no cap" is
        // the contract.
        val queued25s = RequestAdmission(System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(25_000), 30_000)
        queued25s.admit(System.nanoTime())
        assertThat(queued25s.remainingBudgetMs()).isBetween(4_000L, 5_200L)
        val queuedPast = RequestAdmission(System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(40_000), 30_000)
        queuedPast.admit(System.nanoTime())
        assertThat(queuedPast.remainingBudgetMs()).isEqualTo(RequestAdmission.MIN_REMAINING_LOCK_TIMEOUT_MS)
        assertThat(admissionAgedMs(9_000).remainingBudgetMs())
            .describedAs("the default budget is 0: unbounded, so no cap")
            .isNull()
        // Queue wait ONLY, frozen: a request enqueued 40 s ago that was
        // admitted 10 ms later has spent 40 s in its handler (a hoglite
        // compaction sweep, say) and still has its whole bound, not the
        // floor. Elapsed-since-enqueue would hand its next call 1 s.
        val enqueued = System.nanoTime() - TimeUnit.SECONDS.toNanos(40)
        val longHandler = RequestAdmission(enqueued, 30_000)
        longHandler.admit(enqueued + TimeUnit.MILLISECONDS.toNanos(10))
        assertThat(longHandler.remainingBudgetMs()).isEqualTo(29_990L)
    }

    @Test
    fun `carrying runs a block under an admission on this thread and restores what was there`() {
        // The fan-out exception (class comment): a worker doing a
        // request's work while the request thread waits reads the
        // request's admission, and its pool thread carries nothing onto
        // its next task — or carries back whatever it had.
        //
        // MUTATION: drop the `finally` restore and the post-condition
        // reds with the carried admission still ambient.
        val carried = admissionAgedMs(5)
        assertThat(RequestAdmission.current()).isNull()
        assertThat(RequestAdmission.carrying(carried) { RequestAdmission.current() }).isSameAs(carried)
        assertThat(RequestAdmission.current()).describedAs("nothing before, nothing after").isNull()
        val outer = admissionAgedMs(7)
        RequestAdmission.carrying(outer) {
            assertThat(RequestAdmission.carrying(carried) { RequestAdmission.current() }).isSameAs(carried)
            assertThat(RequestAdmission.current()).describedAs("the outer ambient is back").isSameAs(outer)
            assertThat(RequestAdmission.carrying(null) { RequestAdmission.current() })
                .describedAs("a background fan-out carries none")
                .isNull()
        }
        assertThat(RequestAdmission.current()).isNull()
    }

    @Test
    fun `a queue wait past the budget is shed as the typed retryable 503`() {
        // Typed, so StatusPages answers the documented 503 +
        // Retry-After `commit_queue_timeout` rather than a shape
        // clients have never seen.
        //
        // MUTATION: make the comparison `>` a `>=`-free
        // `queueWaitMs > budgetMs * 2`, or delete the throw, and this
        // reds — the request goes on to borrow a connection and take a
        // lock on behalf of a caller that has gone.
        assertThatThrownBy { RequestAdmission.refuseIfQueueExhausted(budgetMs = 1_000, queueWaitMs = 1_000) }
            .isInstanceOf(HoglakeException.CommitQueueTimeout::class.java)
            .hasMessageContaining("1000ms")
            .hasMessageContaining("HOGLAKE_COMMIT_LOCK_TIMEOUT_MS")

        // Inside the budget, and an unbounded budget, shed nothing.
        RequestAdmission.refuseIfQueueExhausted(budgetMs = 1_000, queueWaitMs = 999)
        RequestAdmission.refuseIfQueueExhausted(budgetMs = 0, queueWaitMs = 600_000)
    }
}
