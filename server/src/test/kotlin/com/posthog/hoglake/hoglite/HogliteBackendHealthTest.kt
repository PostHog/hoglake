package com.posthog.hoglake.hoglite

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

/**
 * The sampler's control flow against probes that hang, throw and answer
 * (#678). No engine and no Docker: the hang is the fixture, as in
 * `HealthProbeDeadlineTest`, whose single-flight, age-bound and cap
 * this class carries over to the hoglite backends.
 */
class HogliteBackendHealthTest {
    private val a = UUID.fromString("0a0a0a0a-0000-4000-8000-00000000000a")
    private val b = UUID.fromString("0b0b0b0b-0000-4000-8000-00000000000b")

    /**
     * A probe that blocks until released. Every test that hangs a probe
     * releases it before closing the sampler, so no test pays the
     * sampler's shutdown wait for a thread it parked on purpose.
     */
    private class Gate {
        val latch = CountDownLatch(1)
        val entered = AtomicInteger()

        fun hang(): Boolean {
            entered.incrementAndGet()
            latch.await()
            return true
        }
    }

    /**
     * THE PROPERTY THE ISSUE IS ABOUT: the caller of
     * [HogliteBackendHealth.sample] must not wait for a backend. A probe
     * that hangs costs the caller nothing; the answer lands later.
     *
     * MUTATION: make `sample` wait on the task (`task.get()`) and the
     * elapsed assertion reds — the hang reaches the caller.
     *
     * MUTATION: publish 0.0 instead of NaN before the first answer and
     * the first assertion on `reachable` reds: a pod that has not
     * probed yet must not read as a backend outage.
     */
    @Test
    fun `sample returns at once while the probe hangs and the answer lands when it does not`() {
        val gate = Gate()
        HogliteBackendHealth(mapOf(a to 1_000L)) { gate.hang() }.use { health ->
            try {
                val started = System.nanoTime()
                health.sample()
                val elapsedMs = (System.nanoTime() - started) / 1_000_000

                assertThat(elapsedMs).isLessThan(500)
                assertThat(health.reachable(a)).isNaN()
                await().atMost(Duration.ofSeconds(5)).until { gate.entered.get() == 1 }

                gate.latch.countDown()
                await().atMost(Duration.ofSeconds(5)).until { health.reachable(a) == 1.0 }
            } finally {
                gate.latch.countDown()
            }
        }
    }

    /**
     * A probe that throws reads as unreachable, and nothing escapes to
     * the caller.
     *
     * MUTATION: drop the `runCatching` in `probeQuietly` and the task
     * dies with the exception — the series never leaves NaN.
     */
    @Test
    fun `a throwing probe reads unreachable and the sampler keeps answering`() {
        HogliteBackendHealth(mapOf(a to 1_000L)) { throw IllegalStateException("no route") }.use { health ->
            health.sample()
            await().atMost(Duration.ofSeconds(5)).until { health.reachable(a) == 0.0 }
            assertThat(health.outstandingAttempts).isZero()
        }
    }

    /**
     * Single flight: ticks that arrive while an attempt is in flight and
     * still YOUNG start nothing — one attempt for three ticks, which is
     * what keeps a slow backend from accumulating one probe per tick.
     *
     * MUTATION: drop the `!current.task.isDone` join branch (always
     * launch) and the count is 2, the cap, not 1.
     */
    @Test
    fun `ticks arriving while an attempt is young start nothing`() {
        val gate = Gate()
        HogliteBackendHealth(mapOf(a to 5_000L)) { gate.hang() }.use { health ->
            try {
                repeat(3) { health.sample() }
                assertThat(health.launchedProbes(a)).isEqualTo(1L)
                assertThat(health.outstandingAttempts).isEqualTo(1)
            } finally {
                gate.latch.countDown()
            }
        }
    }

    /**
     * The production deadline, which no other test exercises: twice the
     * backend's `connect_timeout_ms`, the bound `HogliteClient.reachable()`
     * applies to the connect and then to the request. Built through
     * [HogliteBackendHealth.deadlines] as `App` does, a probe slower
     * than the tick spacing but inside that deadline is JOINED by every
     * tick until it answers, and the answer lands.
     *
     * MUTATION: make `deadlines` return 0 (or anything below the probe's
     * duration) and every tick abandons a healthy in-flight attempt —
     * `launchedProbes` is 2, the cap, not 1.
     */
    @Test
    fun `deadlines is twice the connect timeout and a probe slower than the tick but inside it is joined`() {
        val backend = HogliteBackend(id = a, url = "http://hoglite.invalid", connectTimeoutMs = 700)
        val deadlines = HogliteBackendHealth.deadlines(mapOf(a to backend))
        assertThat(deadlines).isEqualTo(mapOf(a to 1_400L))

        val slow: (UUID) -> Boolean = {
            Thread.sleep(400)
            true
        }
        HogliteBackendHealth(deadlines, slow).use { health ->
            repeat(3) {
                health.sample()
                Thread.sleep(100)
            }
            assertThat(health.launchedProbes(a)).isEqualTo(1L)
            await().atMost(Duration.ofSeconds(5)).until { health.reachable(a) == 1.0 }
        }
    }

    /**
     * The age bound: an attempt hung past its deadline is abandoned and
     * the next tick launches a fresh one, whose answer lands even though
     * the first never returns. Without it a single hung attempt would
     * pin the series at its last value for the rest of the pod's life.
     *
     * MUTATION: drop the `ageMs <= deadlineMs` check (join on `isDone`
     * alone) and the second tick launches nothing — `reachable` stays
     * NaN and the await times out.
     */
    @Test
    fun `a hung attempt is abandoned past its deadline and the fresh one answers`() {
        val gate = Gate()
        val calls = AtomicInteger()
        val probe: (UUID) -> Boolean = { if (calls.incrementAndGet() == 1) gate.hang() else true }
        HogliteBackendHealth(mapOf(a to 100L), probe).use { health ->
            try {
                health.sample()
                await().atMost(Duration.ofSeconds(5)).until { gate.entered.get() == 1 }
                Thread.sleep(250)

                health.sample()

                assertThat(health.launchedProbes(a)).isEqualTo(2L)
                await().atMost(Duration.ofSeconds(5)).until { health.reachable(a) == 1.0 }
                // The abandoned attempt is still parked; the answer did not wait for it.
                assertThat(health.outstandingAttempts).isEqualTo(1)
            } finally {
                gate.latch.countDown()
            }
        }
    }

    /**
     * A backend whose probes hang READS UNREACHABLE, not whatever its
     * last completed probe said: an attempt past its deadline reads 0,
     * and so does the cap's refusal. The series is the only hoglite health
     * signal (#678), so a backend that answered once and then hung must
     * not show 1 until the pod restarts, which is what `HealthProbe`'s
     * `refuse()` answering false comes to.
     *
     * MUTATION: drop the overdue check in `reachable` and the
     * `backend.reachable = 0.0` in the abandon branch and the first
     * assertion reads 1.0; drop those and the one in the cap branch and
     * the second does.
     */
    @Test
    fun `a backend whose probes hang past their deadline reads unreachable`() {
        val gate = Gate()
        val calls = AtomicInteger()
        val probe: (UUID) -> Boolean = { if (calls.incrementAndGet() == 1) true else gate.hang() }
        HogliteBackendHealth(mapOf(a to 100L), probe).use { health ->
            try {
                health.sample()
                await().atMost(Duration.ofSeconds(5)).until { health.reachable(a) == 1.0 }

                // The second attempt hangs; the third tick abandons it.
                health.sample()
                await().atMost(Duration.ofSeconds(5)).until { gate.entered.get() == 1 }
                Thread.sleep(250)
                health.sample()
                assertThat(health.reachable(a)).isEqualTo(0.0)
                assertThat(health.launchedProbes(a)).isEqualTo(3L)

                // The replacement hangs too; the fourth tick hits the cap.
                await().atMost(Duration.ofSeconds(5)).until { gate.entered.get() == 2 }
                Thread.sleep(250)
                health.sample()
                assertThat(health.reachable(a)).isEqualTo(0.0)
                assertThat(health.launchedProbes(a)).isEqualTo(3L)
                assertThat(health.outstandingAttempts(a)).isEqualTo(HogliteBackendHealth.MAX_CONCURRENT_PROBES)
            } finally {
                gate.latch.countDown()
            }
        }
    }

    /**
     * Only the attempt in flight publishes. An abandoned attempt that
     * finishes late — here with false, the answer a timed-out probe
     * gives — must not overwrite the fresh attempt's true; the series
     * carries the newest attempt's result until the next tick.
     *
     * MUTATION: drop the `backend.inFlight === attempt` check and the
     * stale false lands over the fresh true — the last assertion reads
     * 0.0.
     */
    @Test
    fun `a superseded attempt that finishes late does not publish`() {
        val gate = Gate()
        val calls = AtomicInteger()
        val probe: (UUID) -> Boolean = { if (calls.incrementAndGet() == 1) !gate.hang() else true }
        HogliteBackendHealth(mapOf(a to 100L), probe).use { health ->
            try {
                health.sample()
                await().atMost(Duration.ofSeconds(5)).until { gate.entered.get() == 1 }
                Thread.sleep(250)
                health.sample()
                await().atMost(Duration.ofSeconds(5)).until { health.reachable(a) == 1.0 }

                // The abandoned attempt returns false, after the fresh one answered true.
                gate.latch.countDown()
                await().atMost(Duration.ofSeconds(5)).until { health.outstandingAttempts == 0 }
                assertThat(health.reachable(a)).isEqualTo(1.0)
            } finally {
                gate.latch.countDown()
            }
        }
    }

    /**
     * The deadline holds when no tick checks it. The default sampling
     * interval (10 s) is longer than a default deadline (4 s), so a probe
     * that answers after its deadline but before the next tick is the
     * ordinary late case: it is a failed probe, and publishes 0 even
     * though it said reachable and nothing replaced it.
     *
     * MUTATION: publish `answer` without the `late` check and the series
     * reads 1.0.
     */
    @Test
    fun `an attempt that answers reachable past its deadline publishes unreachable`() {
        val slow: (UUID) -> Boolean = {
            Thread.sleep(300)
            true
        }
        HogliteBackendHealth(mapOf(a to 100L), slow).use { health ->
            health.sample()
            await().atMost(Duration.ofSeconds(5)).until { health.outstandingAttempts == 0 }
            assertThat(health.reachable(a)).isEqualTo(0.0)
            assertThat(health.launchedProbes(a)).isEqualTo(1L)
        }
    }

    /**
     * And the series shows the expiry when the attempt crosses its
     * deadline, not at the next tick: a backend that answered once and
     * whose next probe hangs reads 0 with no further [sample] call.
     *
     * MUTATION: drop the overdue check in `reachable` and it reads the
     * first probe's 1.0 until a tick abandons the attempt.
     */
    @Test
    fun `an attempt past its deadline reads unreachable before the next tick`() {
        val gate = Gate()
        val calls = AtomicInteger()
        val probe: (UUID) -> Boolean = { if (calls.incrementAndGet() == 1) true else gate.hang() }
        HogliteBackendHealth(mapOf(a to 1_000L), probe).use { health ->
            try {
                health.sample()
                await().atMost(Duration.ofSeconds(5)).until { health.reachable(a) == 1.0 }

                health.sample()
                await().atMost(Duration.ofSeconds(5)).until { gate.entered.get() == 1 }
                assertThat(health.reachable(a)).isEqualTo(1.0)
                Thread.sleep(1_200)

                assertThat(health.reachable(a)).isEqualTo(0.0)
                assertThat(health.launchedProbes(a)).isEqualTo(2L)
            } finally {
                gate.latch.countDown()
            }
        }
    }

    /**
     * A stall at the cap is logged once, when the cap is reached, and the
     * recovery once, when a parked attempt has returned and an attempt
     * starts again: a backend hung for an hour must not log the refusal
     * and an "abandoning ... launching" line that launches nothing on
     * every tick of it.
     *
     * MUTATION: drop the `if (backend.capped) return` and the refusal is
     * logged on each of the four capped ticks; move the abandon warning
     * back above the cap branch and it is logged on each of them too.
     */
    @Test
    fun `a stall at the cap logs the refusal and the recovery once each`() {
        val capture = ListAppender<ILoggingEvent>().apply { start() }
        val logger = LoggerFactory.getLogger(HogliteBackendHealth::class.java) as Logger
        logger.addAppender(capture)
        val gate = Gate()
        val calls = AtomicInteger()
        val probe: (UUID) -> Boolean = { if (calls.incrementAndGet() <= 2) gate.hang() else true }
        try {
            HogliteBackendHealth(mapOf(a to 50L), probe).use { health ->
                try {
                    repeat(6) {
                        health.sample()
                        Thread.sleep(120)
                    }
                    assertThat(health.launchedProbes(a)).isEqualTo(HogliteBackendHealth.MAX_CONCURRENT_PROBES.toLong())
                    assertThat(capture.list.count { it.level == Level.ERROR }).isEqualTo(1)
                    assertThat(capture.list.count { it.formattedMessage.startsWith("abandoning ") }).isEqualTo(1)

                    gate.latch.countDown()
                    await().atMost(Duration.ofSeconds(5)).until { health.outstandingAttempts == 0 }
                    health.sample()
                    await().atMost(Duration.ofSeconds(5)).until { health.reachable(a) == 1.0 }
                    assertThat(capture.list.count { it.formattedMessage.contains("sampling resumes") }).isEqualTo(1)
                    assertThat(capture.list.count { it.level == Level.ERROR }).isEqualTo(1)
                } finally {
                    gate.latch.countDown()
                }
            }
        } finally {
            logger.detachAppender(capture)
        }
    }

    /**
     * An attempt that answers is uncounted in the same step as its answer
     * is taken. With one abandoned attempt parked, a tick that found the
     * answer taken but the attempt still counted would read two
     * outstanding, take the cap branch, overwrite the fresh 1 with 0 and
     * log the restart ERROR for a backend that just answered. The window
     * is microseconds wide, so ticks spin against replacements that
     * answer at once: every launch is another chance to land in it. The
     * deadline is long only so a replacement stalled by a GC pause or a
     * busy CI box is not abandoned too, which would legitimately park a
     * second attempt; the window does not depend on it.
     *
     * MUTATION: move `running.decrementAndGet()` out of the
     * `synchronized(backend)` block, after it, and the ERROR is logged.
     */
    @Test
    fun `a tick right after an answer does not take the cap branch while one attempt is parked`() {
        val capture = ListAppender<ILoggingEvent>().apply { start() }
        val logger = LoggerFactory.getLogger(HogliteBackendHealth::class.java) as Logger
        logger.addAppender(capture)
        val gate = Gate()
        val calls = AtomicInteger()
        val probe: (UUID) -> Boolean = { if (calls.incrementAndGet() == 1) gate.hang() else true }
        try {
            HogliteBackendHealth(mapOf(a to 2_000L), probe).use { health ->
                try {
                    health.sample()
                    await().atMost(Duration.ofSeconds(5)).until { gate.entered.get() == 1 }
                    Thread.sleep(2_200)

                    val until = System.nanoTime() + Duration.ofMillis(500).toNanos()
                    while (System.nanoTime() < until) health.sample()

                    assertThat(health.launchedProbes(a)).isGreaterThan(100L)
                    assertThat(capture.list.filter { it.level == Level.ERROR }).isEmpty()
                } finally {
                    gate.latch.countDown()
                }
            }
        } finally {
            logger.detachAppender(capture)
        }
    }

    /**
     * The cap: a permanently hung backend parks
     * [HogliteBackendHealth.MAX_CONCURRENT_PROBES] attempts and no more,
     * however many ticks age them past their deadline.
     *
     * MUTATION: remove the `running.get() >= MAX_CONCURRENT_PROBES`
     * branch and six ticks launch six attempts — a thread per tick for
     * the length of the outage.
     */
    @Test
    fun `a permanent hang parks a bounded number of attempts and no more`() {
        val gate = Gate()
        HogliteBackendHealth(mapOf(a to 50L)) { gate.hang() }.use { health ->
            try {
                repeat(6) {
                    health.sample()
                    Thread.sleep(120)
                }
                assertThat(health.launchedProbes(a)).isEqualTo(HogliteBackendHealth.MAX_CONCURRENT_PROBES.toLong())
                assertThat(health.outstandingAttempts(a)).isEqualTo(HogliteBackendHealth.MAX_CONCURRENT_PROBES)
                assertThat(health.outstandingAttempts).isEqualTo(HogliteBackendHealth.MAX_CONCURRENT_PROBES)
            } finally {
                gate.latch.countDown()
            }
        }
    }

    /**
     * Backends are sampled independently: one that hangs does not delay
     * another's answer. This is the issue's shape at the sampler level —
     * the old serial `all {}` would have reached the second backend only
     * after the first timed out.
     *
     * MUTATION: probe the backends serially on one thread (submit a
     * single task looping over the map) and `b` stays NaN while `a`
     * hangs.
     */
    @Test
    fun `a hung backend does not delay another backend's answer`() {
        val gate = Gate()
        HogliteBackendHealth(mapOf(a to 5_000L, b to 5_000L)) { if (it == a) gate.hang() else true }.use { health ->
            try {
                health.sample()
                await().atMost(Duration.ofSeconds(5)).until { health.reachable(b) == 1.0 }
                assertThat(health.reachable(a)).isNaN()
                assertThat(health.outstandingAttempts(a)).isEqualTo(1)
                assertThat(health.outstandingAttempts(b)).isZero()
            } finally {
                gate.latch.countDown()
            }
        }
    }
}
