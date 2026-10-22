package com.posthog.hoglake

import com.posthog.hoglake.observability.Metrics
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

/**
 * The coroutine loop runner's contracts: disabled-by-interval, prompt
 * bounded shutdown even mid-body, and per-iteration/per-loop failure
 * isolation (a crashing iteration is logged + counted and the NEXT
 * iteration still runs; sibling loops never notice).
 */
class BackgroundLoopsTest {
    @Test
    fun `non-positive interval registers nothing and close is a no-op`() {
        val ran = AtomicInteger(0)
        BackgroundLoops().use { loops ->
            loops.register("zero", 0) { ran.incrementAndGet() }
            loops.register("negative", -1) { ran.incrementAndGet() }
            Thread.sleep(100)
        }
        assertThat(ran.get()).isEqualTo(0)
    }

    @Test
    fun `close cancels promptly - a loop blocked mid-body is interrupted, not waited out`() {
        val entered = CountDownLatch(1)
        val loops = BackgroundLoops()
        // One loop parked deep inside a blocking body...
        loops.register("blocked", 60_000) {
            entered.countDown()
            try {
                Thread.sleep(60_000)
            } catch (e: InterruptedException) {
                // runInterruptible delivers cancellation as an interrupt.
                throw e
            }
        }
        // ...and one parked in its between-iterations delay.
        loops.register("sleeping", 60_000) { }
        assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue()

        val start = System.nanoTime()
        loops.close()
        val elapsedMs = (System.nanoTime() - start) / 1_000_000
        assertThat(elapsedMs)
            .describedAs("shutdown must interrupt blocked bodies, not wait them out")
            .isLessThan(BackgroundLoops.SHUTDOWN_TIMEOUT_MS)
    }

    @Test
    fun `a crashing iteration does not stop later iterations or sibling loops`() {
        val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
        Metrics.bind(registry)
        try {
            val crashes = AtomicInteger(0)
            val healthy = AtomicInteger(0)
            BackgroundLoops().use { loops ->
                loops.register("crashy", 10) {
                    crashes.incrementAndGet()
                    throw IllegalStateException("boom")
                }
                loops.register("healthy", 10) { healthy.incrementAndGet() }
                await().atMost(Duration.ofSeconds(10)).untilAsserted {
                    // The crashy loop RAN AGAIN after failing, and the
                    // sibling loop never stopped.
                    assertThat(crashes.get()).isGreaterThanOrEqualTo(3)
                    assertThat(healthy.get()).isGreaterThanOrEqualTo(3)
                }
            }
            val counted =
                registry.get("hoglake_background_loop_failures_total")
                    .tag("loop", "crashy")
                    .counter()
                    .count()
            assertThat(counted).isGreaterThanOrEqualTo(3.0)
        } finally {
            Metrics.clear()
        }
    }

    @Test
    fun `a jittered delay stays inside interval x (1 +- fraction) and averages the interval`() {
        val interval = 30_000L
        val f = BackgroundLoops.COMPACTION_JITTER_FRACTION
        // The two ends of the draw, exactly.
        assertThat(BackgroundLoops.jitteredDelayMs(interval, f, 0.0)).isEqualTo(22_500L)
        assertThat(BackgroundLoops.jitteredDelayMs(interval, f, 0.5)).isEqualTo(interval)
        assertThat(BackgroundLoops.jitteredDelayMs(interval, f, Math.nextDown(1.0))).isEqualTo(37_500L)

        val random = java.util.Random(CompactionSeed.of("gigahog-maintenance-compaction-7d9f8-x2k4q"))
        val draws = (0 until 10_000).map { BackgroundLoops.jitteredDelayMs(interval, f, random.nextDouble()) }
        assertThat(draws).allMatch { it in 22_500L..37_500L }
        // MUTATION: drop the `2u - 1` centring (scale by `1 + f x u`) and
        // the mean moves to 1.125 x interval — the loop silently slows —
        // and nothing lands below the interval.
        assertThat(draws.average()).isBetween(interval * 0.99, interval * 1.01)
        assertThat(draws.count { it < interval }).isGreaterThan(4_000)
        assertThat(draws.count { it > interval }).isGreaterThan(4_000)
        // Spread, not a constant: a herd disperses only if pods draw
        // different delays.
        assertThat(draws.toSet().size).isGreaterThan(1_000)
    }

    @Test
    fun `a zero fraction is exact, a tiny interval never sleeps zero, and a fraction of 1 or more is refused`() {
        assertThat(BackgroundLoops.jitteredDelayMs(30_000, 0.0, 0.0)).isEqualTo(30_000L)
        assertThat(BackgroundLoops.jitteredDelayMs(30_000, 0.0, 0.999)).isEqualTo(30_000L)
        assertThat(BackgroundLoops.jitteredDelayMs(1, 0.9, 0.0)).isEqualTo(1L)
        org.assertj.core.api.Assertions.assertThatThrownBy { BackgroundLoops.Jitter(1.0, 0) }
            .isInstanceOf(IllegalArgumentException::class.java)
        org.assertj.core.api.Assertions.assertThatThrownBy { BackgroundLoops.Jitter(-0.1, 0) }
            .isInstanceOf(IllegalArgumentException::class.java)
    }

    @Test
    fun `two pods' jitter sequences differ, and one pod's is reproducible`() {
        fun sequence(pod: String): List<Long> {
            val random = java.util.Random(CompactionSeed.of(pod))
            return (0 until 8).map { BackgroundLoops.jitteredDelayMs(30_000, 0.25, random.nextDouble()) }
        }
        val a = sequence("gigahog-maintenance-compaction-7d9f8-x2k4q")
        val b = sequence("gigahog-maintenance-compaction-7d9f8-x2k4r")
        assertThat(sequence("gigahog-maintenance-compaction-7d9f8-x2k4q")).isEqualTo(a)
        // Two pods whose sweeps end in the same millisecond re-plan in
        // the same millisecond only if their first draws agree.
        assertThat(a.first()).isNotEqualTo(b.first())
        assertThat(a).isNotEqualTo(b)
    }

    @Test
    fun `an unjittered loop sleeps exactly its interval, a jittered one draws around it`() {
        // The KDoc's "only compaction is jittered" rests on this: the
        // sampler's tick cadence and expiry's interval are sized
        // arithmetic, and a register() that jittered by default would
        // move them silently.
        val delays = java.util.concurrent.ConcurrentHashMap<String, MutableList<Long>>()
        BackgroundLoops().use { loops ->
            loops.onDelay = {
                    name,
                    ms,
                ->
                delays.computeIfAbsent(name) { java.util.Collections.synchronizedList(mutableListOf()) }.add(ms)
            }
            loops.register("jittered", 40, BackgroundLoops.Jitter(0.25, 7)) { }
            loops.register("exact", 40) { }
            await().atMost(Duration.ofSeconds(10)).untilAsserted {
                assertThat(delays["jittered"].orEmpty().size).isGreaterThanOrEqualTo(8)
                assertThat(delays["exact"].orEmpty().size).isGreaterThanOrEqualTo(8)
            }
        }
        // MUTATION: default `register`'s `jitter` to a Jitter (jitter
        // every loop) and the exact loop's delays stop being 40.
        assertThat(delays.getValue("exact").toList()).containsOnly(40L)
        val jittered = delays.getValue("jittered").toList()
        assertThat(jittered).allMatch { it in 30L..50L }
        assertThat(jittered.toSet().size).describedAs("drawn, not constant: %s", jittered).isGreaterThan(1)
    }

    /** The production seed derivation, so these tests draw what a pod draws. */
    private object CompactionSeed {
        fun of(pod: String): Long = com.posthog.hoglake.compaction.CompactionService.identitySeed(pod)
    }
}
