package com.posthog.hoglake.api

import com.posthog.hoglake.RequestAdmission
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Two properties of the blocking dispatcher that the HTTP-level tests
 * cannot discriminate, exercised directly on the pool (#218). No Docker,
 * no engine: both are facts about the executor and the counter.
 */
class RequestDispatcherTest {
    /**
     * `queued` counts CALLS WAITING FOR THEIR FIRST THREAD, not the
     * executor's queue depth.
     *
     * The distinction is invisible at HTTP level under a saturation
     * fixture — with every handler blocked and nothing resuming, the two
     * numbers agree — so it is pinned here, where a task can be posted
     * to the executor WITHOUT a call having been enqueued. That is
     * exactly what an already-admitted coroutine does every time it
     * resumes (after `call.receive()`, after a suspending write), and
     * counting those as "waiting" would make a busy instance look
     * saturated and a saturation alert fire on throughput.
     *
     * MUTATION: define `queued` as `executor.queue.size` and the first
     * assertion reds at 1.
     */
    @Test
    fun `queued counts calls awaiting a first thread and not executor queue depth`() {
        RequestDispatcher(threads = 1, threadNamePrefix = "test-queued").use { dispatcher ->
            val occupy = CountDownLatch(1)
            val occupied = CountDownLatch(1)
            dispatcher.dispatcher.dispatch(EmptyCoroutineContext) {
                occupied.countDown()
                occupy.await(10, TimeUnit.SECONDS)
            }
            assertTrue(occupied.await(10, TimeUnit.SECONDS))

            // A bare re-dispatch, as a resuming coroutine performs: it
            // sits in the executor's queue and is NOT a waiting call.
            dispatcher.dispatcher.dispatch(EmptyCoroutineContext) { }
            assertThat(dispatcher.queued)
                .describedAs("a resumption of admitted work is not a call waiting for its first thread")
                .isEqualTo(0)

            // What the interceptor counts, on the other hand, is.
            dispatcher.enqueued()
            dispatcher.enqueued()
            assertThat(dispatcher.queued).isEqualTo(2)
            dispatcher.admitted()
            assertThat(dispatcher.queued).isEqualTo(1)
            dispatcher.admitted()
            assertThat(dispatcher.queued).isEqualTo(0)

            occupy.countDown()
        }
    }

    /**
     * The ambient admission does not survive onto the NEXT call that
     * lands on the same pooled thread.
     *
     * This is the property that makes `RequestAdmission` safe to read
     * from deep inside blocking service code, and the one a bare
     * `ThreadLocal.set` would quietly break: handler threads are
     * reused, so a stale value would be charged against a request that
     * never queued — and against BACKGROUND work, whose whole contract
     * is that it carries no ambient at all. `asContextElement` restores
     * the previous value when the coroutine leaves the thread, which is
     * what this asserts on a one-thread pool where "the next call" is
     * guaranteed to be the same thread.
     *
     * MUTATION: replace `holder.asContextElement(admission)` with a
     * raw `holder.set(admission)` inside the block and the second
     * assertion reds with the previous call's admission still ambient.
     */
    @Test
    fun `the ambient admission does not leak onto the next call on the same thread`() {
        RequestDispatcher(threads = 1, threadNamePrefix = "test-ambient").use { dispatcher ->
            var insideThread: String? = null
            var afterThread: String? = null
            var insideAdmission: RequestAdmission? = null
            var afterAdmission: RequestAdmission? = null

            runBlocking {
                val admission = RequestAdmission(System.nanoTime())
                withContext(dispatcher.dispatcher + RequestAdmission.element(admission)) {
                    insideThread = Thread.currentThread().name
                    insideAdmission = RequestAdmission.current()
                }
                withContext(dispatcher.dispatcher) {
                    afterThread = Thread.currentThread().name
                    afterAdmission = RequestAdmission.current()
                }
            }

            assertThat(afterThread)
                .describedAs("a one-thread pool must reuse the same thread, or this proves nothing")
                .isEqualTo(insideThread)
            assertThat(insideAdmission).isNotNull()
            assertThat(afterAdmission)
                .describedAs("the next call on that thread must see no ambient admission")
                .isNull()
        }
    }

    /**
     * Closing the dispatcher lets an in-flight handler FINISH.
     *
     * Interrupting one is the behaviour #218 complains about from the
     * other end: a liveness kill mid-commit discards work the caller had
     * every reason to think was proceeding, and the writers all retry at
     * once. A shutdown that did the same thing deliberately would be the
     * same bug, chosen.
     *
     * MUTATION: use `shutdownNow()` in `close()` and this reds — the
     * sleeping task is interrupted.
     */
    @Test
    fun `close lets an in-flight handler finish instead of interrupting it`() {
        val dispatcher = RequestDispatcher(threads = 1, threadNamePrefix = "test-close")
        val interrupted = AtomicBoolean()
        val finished = CountDownLatch(1)
        val started = CountDownLatch(1)
        dispatcher.dispatcher.dispatch(EmptyCoroutineContext) {
            started.countDown()
            try {
                Thread.sleep(300)
            } catch (e: InterruptedException) {
                interrupted.set(true)
            }
            finished.countDown()
        }
        assertTrue(started.await(10, TimeUnit.SECONDS))

        dispatcher.close()

        assertTrue(finished.await(10, TimeUnit.SECONDS), "the handler must have run to completion")
        assertFalse(interrupted.get(), "close must not interrupt work already in flight")
    }
}
