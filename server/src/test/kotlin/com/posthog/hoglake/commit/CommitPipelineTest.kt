package com.posthog.hoglake.commit

import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.persistence.Locks
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.UUID

/**
 * The pipeline's admission, depth adaptation and delta bookkeeping,
 * without a database: the arithmetic [CommitPipeline] pins for the
 * integration tests (`CommitPipelineIntegrationTest`) that exercise the
 * same paths against Postgres.
 */
class CommitPipelineTest {
    private val pipe = CommitPipeline.CatalogPipe(cap = 8)

    @Test
    fun `a catalog at rest takes the locked path at depth 1`() {
        assertThat(pipe.depth).isEqualTo(1)
        assertThat(pipe.acquire(1_000)).isEqualTo(CommitPipeline.CatalogPipe.Acquire.ACQUIRED)
        assertThat(pipe.pathFor(hasDeletes = false)).isEqualTo(CommitPipeline.Path.LOCKED)
        assertThat(pipe.depth).isEqualTo(1)
        pipe.release()
        assertThat(pipe.depth).isEqualTo(1)
    }

    @Test
    fun `a commit with deletes takes the drained path even at rest`() {
        assertThat(pipe.acquire(1_000)).isEqualTo(CommitPipeline.CatalogPipe.Acquire.ACQUIRED)
        assertThat(pipe.pathFor(hasDeletes = true)).isEqualTo(CommitPipeline.Path.DRAINED)
        pipe.release()
    }

    @Test
    fun `a full pipeline doubles the depth and admits through the widened slot, up to the cap`() {
        assertThat(pipe.acquire(1_000)).isEqualTo(CommitPipeline.CatalogPipe.Acquire.ACQUIRED) // (inFlight 1, depth 1)
        // Arriving at a full pipeline doubles the depth and enters
        // through the slot the doubling opened — no sleep.
        assertThat(pipe.acquire(1_000)).isEqualTo(CommitPipeline.CatalogPipe.Acquire.ACQUIRED)
        assertThat(pipe.depth).isEqualTo(2)
        assertThat(pipe.acquire(1_000)).isEqualTo(CommitPipeline.CatalogPipe.Acquire.ACQUIRED)
        assertThat(pipe.depth).isEqualTo(4)
        assertThat(pipe.acquire(1_000)).isEqualTo(CommitPipeline.CatalogPipe.Acquire.ACQUIRED) // 3 < 4: room
        assertThat(pipe.depth).isEqualTo(4)
        assertThat(pipe.acquire(1_000)).isEqualTo(CommitPipeline.CatalogPipe.Acquire.ACQUIRED)
        assertThat(pipe.depth).isEqualTo(8)
        // Fill to the cap (8 in flight)...
        repeat(3) { assertThat(pipe.acquire(1_000)).isEqualTo(CommitPipeline.CatalogPipe.Acquire.ACQUIRED) }
        assertThat(pipe.depth).isEqualTo(8)
        // ...beyond the cap the waiter actually waits out its budget.
        val started = System.nanoTime()
        assertThat(pipe.acquire(200)).isEqualTo(CommitPipeline.CatalogPipe.Acquire.TIMED_OUT)
        assertThat((System.nanoTime() - started) / 1_000_000).isGreaterThanOrEqualTo(150)
        // A contended drain halves: the last release of the burst takes 8 -> 4.
        repeat(8) { pipe.release() }
        assertThat(pipe.depth).isEqualTo(4)
    }

    @Test
    fun `an uncontended drain decays the depth straight to 1 from a deep burst`() {
        // The 2->1 case alone cannot distinguish "straight to 1" from
        // unconditional halving (4->2->1 and 4->1 agree at depth 2).
        // Contend to depth 8; the CONTENDED drain halves it to 4.
        repeat(5) { assertThat(pipe.acquire(1_000)).isEqualTo(CommitPipeline.CatalogPipe.Acquire.ACQUIRED) }
        assertThat(pipe.depth).isEqualTo(8)
        repeat(5) { pipe.release() }
        assertThat(pipe.depth).isEqualTo(4)
        // A quiet cycle — no contention — drains 4 straight to 1.
        assertThat(pipe.acquire(1_000)).isEqualTo(CommitPipeline.CatalogPipe.Acquire.ACQUIRED)
        assertThat(pipe.pathFor(hasDeletes = false)).isEqualTo(CommitPipeline.Path.PIPELINED)
        pipe.release()
        assertThat(pipe.depth).isEqualTo(1)
        // And the next quiet commit is the LOCKED path again.
        assertThat(pipe.acquire(1_000)).isEqualTo(CommitPipeline.CatalogPipe.Acquire.ACQUIRED)
        assertThat(pipe.pathFor(hasDeletes = false)).isEqualTo(CommitPipeline.Path.LOCKED)
        pipe.release()
    }

    @Test
    fun `an uncontended drain decays the depth straight to 1`() { // Contend up to depth 4, drain: halves to 2.
        repeat(3) { assertThat(pipe.acquire(1_000)).isEqualTo(CommitPipeline.CatalogPipe.Acquire.ACQUIRED) }
        assertThat(pipe.depth).isEqualTo(4)
        repeat(3) { pipe.release() }
        assertThat(pipe.depth).isEqualTo(2)
        // One quiet cycle — no contention — decays straight to 1, so a
        // catalog gone quiet takes the lock-to-commit path again at once
        // instead of lingering on the session-lock path.
        assertThat(pipe.acquire(1_000)).isEqualTo(CommitPipeline.CatalogPipe.Acquire.ACQUIRED)
        pipe.release()
        assertThat(pipe.depth).isEqualTo(1)
    }

    @Test
    fun `depth adapts within the configured cap`() {
        val pipeline = CommitPipeline(maxInFlight = 3)
        val p = pipeline.pipe(1)
        assertThat(p.acquire(1_000)).isEqualTo(CommitPipeline.CatalogPipe.Acquire.ACQUIRED)
        assertThat(p.acquire(1_000)).isEqualTo(CommitPipeline.CatalogPipe.Acquire.ACQUIRED)
        assertThat(p.depth).isEqualTo(2)
        assertThat(p.acquire(1_000)).isEqualTo(CommitPipeline.CatalogPipe.Acquire.ACQUIRED)
        assertThat(p.depth).isEqualTo(3) // capped: 2 * 2 would exceed it
        assertThat(p.acquire(200)).isEqualTo(CommitPipeline.CatalogPipe.Acquire.TIMED_OUT)
        repeat(3) { p.release() }
        assertThat(p.depth).isEqualTo(1) // 3 / 2, floored
    }

    @Test
    fun `a retired pipe never admits, and the map hands out its fresh replacement`() {
        val pipeline = CommitPipeline(maxInFlight = 16)
        val p = pipeline.pipe(7)
        assertThat(p.acquire(1_000)).isEqualTo(CommitPipeline.CatalogPipe.Acquire.ACQUIRED)
        p.release()
        pipeline.retireIfIdle(7, p)
        val fresh = pipeline.pipe(7)
        assertThat(fresh).isNotSameAs(p)
        // A thread still holding the stale reference is turned away and
        // re-looks-up, so one catalog can never have two live pipes.
        assertThat(p.acquire(1_000)).isEqualTo(CommitPipeline.CatalogPipe.Acquire.RETIRED)
        assertThat(fresh.acquire(1_000)).isEqualTo(CommitPipeline.CatalogPipe.Acquire.ACQUIRED)
        fresh.release()
    }

    @Test
    fun `a busy pipe is not retired`() {
        val pipeline = CommitPipeline(maxInFlight = 16)
        val p = pipeline.pipe(9)
        assertThat(p.acquire(1_000)).isEqualTo(CommitPipeline.CatalogPipe.Acquire.ACQUIRED)
        pipeline.retireIfIdle(9, p)
        assertThat(pipeline.pipe(9)).isSameAs(p)
        p.release()
        pipeline.retireIfIdle(9, p)
        assertThat(pipeline.pipe(9)).isNotSameAs(p)
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
    fun `the lock key function computes the acquisition SQL's key`() {
        // The acquisition SQL binds this function's value; the unlock
        // statement binds it too. They must agree or the pipelined path
        // stops serializing (Locks.catalogCommitLockKey's KDoc).
        for (catalogId in listOf(0L, 1L, 42L, Int.MAX_VALUE.toLong(), Int.MAX_VALUE + 5L, 4_000_000_000L)) {
            assertThat(Locks.catalogCommitLockKey(catalogId))
                .isEqualTo((Locks.CATALOG_COMMIT_LOCK_CLASS.toLong() shl 32) or (catalogId and 0xFFFFFFFFL))
        }
    }
}
