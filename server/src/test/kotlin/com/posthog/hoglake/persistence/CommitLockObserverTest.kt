package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Metrics
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * The in-process commit-lock bookkeeping (#439) around a STUBBED acquire
 * and a stubbed transaction end: the waiting gauge while the acquire
 * blocks, the wait timer, then the held gauge and hold timer from grant
 * to the end. `CommitLockHoldIntegrationTest` is the same contract
 * against real JDBI handles and real callers.
 */
class CommitLockObserverTest {
    private lateinit var registry: SimpleMeterRegistry

    // Holds opened by this test only: the count is process-wide, and an
    // integration class in the same JVM may still have one open.
    private var openBefore = 0

    private fun openHolds() = CommitLockObserver.openHolds - openBefore

    /** A transaction end the test fires by hand, as often as it likes. */
    private class FakeEnd(override val open: Boolean = true) : CommitLockObserver.TransactionEnd {
        override val owner: Any = Any()
        private val settles = mutableListOf<() -> Unit>()

        override fun onEnd(settle: () -> Unit) {
            settles += settle
        }

        fun end() = settles.toList().forEach { it() }
    }

    @BeforeEach
    fun bind() {
        registry = SimpleMeterRegistry()
        Metrics.bind(registry)
        openBefore = CommitLockObserver.openHolds
    }

    @AfterEach
    fun unbind() = Metrics.clear()

    private fun gauge(
        name: String,
        catalog: String,
    ) = registry.find(name).tag("catalog", catalog).gauge()?.value()

    private fun waiting(catalog: String) = gauge("hoglake_commit_lock_waiting", catalog)

    private fun held(catalog: String) = gauge("hoglake_commit_lock_held", catalog)

    private fun waits() = registry.find("hoglake_commit_lock_wait").timer()?.count() ?: 0

    private fun holds(
        catalog: String,
        path: String,
    ) = registry.find("hoglake_commit_lock_hold").tags("catalog", catalog, "path", path).timer()?.count() ?: 0

    @Test
    fun `a granted lock is counted waiting while it blocks, then held until the transaction ends`() {
        val end = FakeEnd()
        var waitingDuringAcquire: Double? = null
        CommitLockObserver.observe(7, "lake", CommitLockPath.COMMIT, end) {
            waitingDuringAcquire = waiting("lake")
        }
        // MUTATION: increment after acquire() instead of before, and this
        // reads 0 — the gauge would never see a session that is queued.
        assertThat(waitingDuringAcquire).isEqualTo(1.0)
        assertThat(waiting("lake")).isEqualTo(0.0)
        assertThat(waits()).isEqualTo(1)
        assertThat(held("lake")).isEqualTo(1.0)
        assertThat(holds("lake", "commit")).isZero()
        assertThat(openHolds()).isEqualTo(1)

        end.end()
        assertThat(held("lake")).isEqualTo(0.0)
        assertThat(holds("lake", "commit")).isEqualTo(1)
        assertThat(openHolds()).isZero()
    }

    @Test
    fun `a lock timeout leaves nobody waiting and opens no hold`() {
        val end = FakeEnd()
        assertThatThrownBy {
            CommitLockObserver.observe(7, "busy", CommitLockPath.COMPACTION, end) {
                assertThat(waiting("busy")).isEqualTo(1.0)
                throw HoglakeException.CommitQueueTimeout("timed out")
            }
        }.isInstanceOf(HoglakeException.CommitQueueTimeout::class.java)
        // MUTATION: decrement only after a successful acquire (outside the
        // finally) and this stays at 1 forever — a queue that never drains,
        // on exactly the catalog that is in trouble.
        assertThat(waiting("busy")).isEqualTo(0.0)
        // The timed-out wait is still a wait: it is the tail of the histogram.
        assertThat(waits()).isEqualTo(1)
        assertThat(held("busy")).isNull()
        assertThat(openHolds()).isZero()
        end.end()
        assertThat(holds("busy", "compaction")).isZero()
    }

    @Test
    fun `any other failure of the lock statement is the same as a timeout`() {
        val end = FakeEnd()
        assertThatThrownBy {
            CommitLockObserver.observe(7, "broken", CommitLockPath.EXPIRY, end) {
                throw IllegalStateException("connection reset")
            }
        }.isInstanceOf(IllegalStateException::class.java)
        assertThat(waiting("broken")).isEqualTo(0.0)
        assertThat(waits()).isEqualTo(1)
        assertThat(held("broken")).isNull()
        assertThat(openHolds()).isZero()
    }

    @Test
    fun `a re-entrant acquire in the same transaction is one hold, under the first path`() {
        val end = FakeEnd()
        CommitLockObserver.observe(7, "lake", CommitLockPath.DDL, end) {}
        CommitLockObserver.observe(7, "lake", CommitLockPath.COMMIT, end) {}
        assertThat(held("lake")).isEqualTo(1.0)
        assertThat(openHolds()).isEqualTo(1)
        end.end()
        assertThat(holds("lake", "ddl")).isEqualTo(1)
        assertThat(holds("lake", "commit")).isZero()
        assertThat(held("lake")).isEqualTo(0.0)
    }

    @Test
    fun `an end reported twice (commit, then close) settles once`() {
        val end = FakeEnd()
        CommitLockObserver.observe(7, "lake", CommitLockPath.RETIREMENT, end) {}
        end.end()
        end.end()
        assertThat(holds("lake", "retirement")).isEqualTo(1)
        // MUTATION: drop the settled guard and the hold count above reads 2.
        assertThat(held("lake")).isEqualTo(0.0)
    }

    @Test
    fun `two transactions of one process on two catalogs are tracked apart`() {
        val a = FakeEnd()
        val b = FakeEnd()
        CommitLockObserver.observe(1, "a", CommitLockPath.DROP, a) {}
        CommitLockObserver.observe(2, "b", CommitLockPath.DROP, b) {}
        a.end()
        assertThat(held("a")).isEqualTo(0.0)
        assertThat(held("b")).isEqualTo(1.0)
        b.end()
        assertThat(holds("a", "drop")).isEqualTo(1)
        assertThat(holds("b", "drop")).isEqualTo(1)
    }

    @Test
    fun `in autocommit the statement released the lock already, so the hold closes at once`() {
        CommitLockObserver.observe(7, "lake", CommitLockPath.DDL, FakeEnd(open = false)) {}
        assertThat(holds("lake", "ddl")).isEqualTo(1)
        assertThat(held("lake")).isNull()
        assertThat(openHolds()).isZero()
    }

    @Test
    fun `with no registry bound the bookkeeping is a no-op that still settles`() {
        Metrics.clear()
        val end = FakeEnd()
        CommitLockObserver.observe(7, "lake", CommitLockPath.COMMIT, end) {}
        assertThat(openHolds()).isEqualTo(1)
        end.end()
        assertThat(openHolds()).isZero()
    }

    @Test
    fun `every path label is the issue's vocabulary`() {
        assertThat(CommitLockPath.entries.map { it.label })
            .containsExactly("commit", "compaction", "expiry", "retirement", "ddl", "drop")
    }
}
