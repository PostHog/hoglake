package com.posthog.hoglake.commit

import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.persistence.Locks
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.concurrent.thread

/**
 * The pipeline's admission, depth adaptation and delta bookkeeping,
 * without a database: the arithmetic [CommitPipeline] pins for the
 * integration tests (`CommitPipelineIntegrationTest`) that exercise the
 * same paths against Postgres.
 */
class CommitPipelineTest {
    private val pipe = CommitPipeline.CatalogPipe(cap = 8)

    private fun spinUntilTrue(
        timeoutMs: Long = 5_000,
        cond: () -> Boolean,
    ) {
        val deadline = System.nanoTime() + timeoutMs * 1_000_000
        while (!cond()) {
            if (System.nanoTime() > deadline) throw AssertionError("condition not met within ${timeoutMs}ms")
            Thread.sleep(2)
        }
    }

    @Test
    fun `a catalog at rest takes the locked path at depth 1`() {
        assertThat(pipe.depth).isEqualTo(1)
        assertThat(pipe.acquire(1_000)).isTrue()
        assertThat(pipe.pathFor(hasDeletes = false)).isEqualTo(CommitPipeline.Path.LOCKED)
        assertThat(pipe.atRest()).isFalse()
        pipe.release()
        assertThat(pipe.depth).isEqualTo(1)
        assertThat(pipe.atRest()).isTrue()
    }

    @Test
    fun `a commit with deletes takes the drained path even at rest`() {
        assertThat(pipe.acquire(1_000)).isTrue()
        assertThat(pipe.pathFor(hasDeletes = true)).isEqualTo(CommitPipeline.Path.DRAINED)
        pipe.release()
    }

    @Test
    fun `contention doubles the depth up to the cap, draining halves it back`() {
        assertThat(pipe.acquire(1_000)).isTrue() // in-flight 1 at depth 1
        val second =
            thread {
                // Contention: queues behind the first, so the depth doubles,
                // then waits for a slot to free.
                assertThat(pipe.acquire(5_000)).isTrue()
            }
        spinUntilTrue { pipe.depth == 2 }
        // The widened slot admits a third commit while the waiter sleeps.
        assertThat(pipe.acquire(1_000)).isTrue()
        val fourth =
            thread {
                // Full again (in-flight 2 >= depth 2): doubles to 4, waits.
                assertThat(pipe.acquire(5_000)).isTrue()
            }
        spinUntilTrue { pipe.depth == 4 }
        // Freeing a slot wakes both waiters into the widened pipeline.
        pipe.release()
        second.join(5_000)
        fourth.join(5_000)
        assertThat(second.isAlive).isFalse()
        assertThat(fourth.isAlive).isFalse()
        // Three in flight; the draining release halves 4 -> 2.
        repeat(3) { pipe.release() }
        assertThat(pipe.depth).isEqualTo(2)
        // One quiet commit cycle drains to 1 again.
        assertThat(pipe.acquire(1_000)).isTrue()
        pipe.release()
        assertThat(pipe.depth).isEqualTo(1)
        assertThat(pipe.atRest()).isTrue()
    }

    @Test
    fun `the permit wait honours the admission budget`() {
        val single = CommitPipeline.CatalogPipe(cap = 1)
        assertThat(single.acquire(1_000)).isTrue()
        val started = System.nanoTime()
        assertThat(single.acquire(200)).isFalse()
        val waitedMs = (System.nanoTime() - started) / 1_000_000
        assertThat(waitedMs).isGreaterThanOrEqualTo(150)
        single.release()
        assertThat(single.acquire(1_000)).isTrue()
        single.release()
    }

    @Test
    fun `deltas merge by max and union, and prune once durable`() {
        val first = pipe.registerDelta(10, 100, mapOf(1L to 5_000L), setOf("p1" to "data"), null, null)
        val second =
            pipe.registerDelta(11, 105, mapOf(1L to 7_000L, 2L to 40L), setOf("p2" to "delete"), null, null)
        pipe.view().let { v ->
            assertThat(v.headSnapshotId).isEqualTo(11)
            assertThat(v.nextFileId).isEqualTo(105)
            assertThat(v.nextRowIds).isEqualTo(mapOf(1L to 7_000L, 2L to 40L))
            assertThat(v.registeredPaths).containsExactlyInAnyOrder("p1" to "data", "p2" to "delete")
        }
        // The first commit durable: its delta prunes, the second's stays.
        first.result.complete(CommitResult(10, 1))
        pipe.pruneDeltas()
        pipe.view().let { v ->
            assertThat(v.headSnapshotId).isEqualTo(11)
            assertThat(v.registeredPaths).containsExactly("p2" to "delete")
        }
        second.result.complete(CommitResult(11, 1))
        pipe.pruneDeltas()
        pipe.view().let { v ->
            assertThat(v.headSnapshotId).isNull()
            assertThat(v.nextFileId).isNull()
            assertThat(v.nextRowIds).isEmpty()
            assertThat(v.registeredPaths).isEmpty()
        }
    }

    @Test
    fun `sequence numbers are assigned in stage-A order`() {
        val a = pipe.registerDelta(1, 10, emptyMap(), emptySet(), null, null)
        val b = pipe.registerDelta(2, 11, emptyMap(), emptySet(), null, null)
        assertThat(b.seq).isGreaterThan(a.seq)
    }

    @Test
    fun `a duplicate in-flight idempotency key defers the replay`() {
        val key = UUID.randomUUID()
        pipe.registerDelta(3, 20, emptyMap(), emptySet(), key, byteArrayOf(1, 2, 3))
        assertThatThrownBy {
            pipe.registerDelta(4, 21, emptyMap(), emptySet(), key, byteArrayOf(1, 2, 3))
        }.isInstanceOf(CommitPipeline.CatalogPipe.DeferredReplay::class.java)
        assertThat(pipe.inFlightReceipt(key)).isNotNull
        // The original durable: the key leaves the in-flight map.
        pipe.inFlightReceipt(key)!!.result.complete(CommitResult(3, 1))
        pipe.pruneDeltas()
        assertThat(pipe.inFlightReceipt(key)).isNull()
        // And the same key registers cleanly afterwards (the DB receipt
        // answers replays from here on).
        pipe.registerDelta(4, 21, emptyMap(), emptySet(), key, byteArrayOf(1, 2, 3))
    }

    @Test
    fun `a failed delta is forgotten and its waiters are released`() {
        val key = UUID.randomUUID()
        val d = pipe.registerDelta(5, 30, mapOf(7L to 99L), emptySet(), key, null)
        pipe.failDelta(d, RuntimeException("rolled back"))
        assertThat(d.result).isCompletedExceptionally()
        assertThat(pipe.inFlightReceipt(key)).isNull()
        assertThat(pipe.view().headSnapshotId).isNull()
        assertThat(pipe.view().nextRowIds).isEmpty()
    }

    @Test
    fun `the session lock key function computes the acquisition SQL's key`() {
        // The acquisition SQL builds (class << 32) | (catalog_id & 0xFFFFFFFF);
        // the unlock CTE binds this function's value. They must agree or the
        // pipelined path stops serializing (Locks.catalogCommitLockKey's KDoc).
        for (catalogId in listOf(0L, 1L, 42L, Int.MAX_VALUE.toLong(), Int.MAX_VALUE + 5L, 4_000_000_000L)) {
            assertThat(Locks.catalogCommitLockKey(catalogId))
                .isEqualTo((Locks.CATALOG_COMMIT_LOCK_CLASS.toLong() shl 32) or (catalogId and 0xFFFFFFFFL))
        }
    }

    @Test
    fun `the pipeline retires an idle catalog's pipe`() {
        val pipeline = CommitPipeline(maxInFlight = 16)
        val p = pipeline.pipe(7)
        assertThat(p.acquire(1_000)).isTrue()
        p.release()
        pipeline.retireIfIdle(7, p)
        assertThat(pipeline.pipe(7)).isNotSameAs(p)
    }

    @Test
    fun `a busy pipe is not retired`() {
        val pipeline = CommitPipeline(maxInFlight = 16)
        val p = pipeline.pipe(9)
        assertThat(p.acquire(1_000)).isTrue()
        pipeline.retireIfIdle(9, p)
        assertThat(pipeline.pipe(9)).isSameAs(p)
        p.release()
        pipeline.retireIfIdle(9, p)
        assertThat(pipeline.pipe(9)).isNotSameAs(p)
    }

    @Test
    fun `depth adapts within the configured cap`() {
        val pipeline = CommitPipeline(maxInFlight = 3)
        val p = pipeline.pipe(1)
        assertThat(p.acquire(1_000)).isTrue() // in-flight 1, depth 1
        val t =
            thread {
                // Contention doubles 1 -> 2; the waiter sleeps for a slot.
                assertThat(p.acquire(5_000)).isTrue()
            }
        spinUntilTrue { p.depth == 2 }
        assertThat(p.acquire(1_000)).isTrue() // the widened slot: in-flight 2
        val t2 =
            thread {
                // Full again (2 >= 2): doubles 2 -> 3, capped (2 * 2 would exceed it).
                assertThat(p.acquire(5_000)).isTrue()
            }
        spinUntilTrue { p.depth == 3 }
        assertThat(p.acquire(1_000)).isTrue() // in-flight 3, the cap
        // Cap reached and full: a further commit waits out the budget.
        assertThat(p.acquire(200)).isFalse()
        // Freeing two slots wakes both waiters, in either order.
        p.release()
        p.release()
        t.join(5_000)
        t2.join(5_000)
        assertThat(t.isAlive).isFalse()
        assertThat(t2.isAlive).isFalse()
        // Drain: the last release halves 3 -> 1.
        repeat(3) { p.release() }
        assertThat(p.depth).isEqualTo(1)
        assertThat(p.atRest()).isTrue()
    }
}
