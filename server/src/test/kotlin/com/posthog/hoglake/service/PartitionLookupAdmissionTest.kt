package com.posthog.hoglake.service

import org.assertj.core.api.Assertions.assertThat
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

class PartitionLookupAdmissionTest {
    @Test
    fun `global and catalog contention wait for a short release`() {
        for (capacity in listOf(1, 2)) {
            val admission = PartitionLookupAdmission(capacity)
            assertThat(admission.acquire("active", deadline())).isTrue()
            val catalog = if (capacity == 1) "other" else "active"
            val waiter = Waiter(admission, catalog)
            waiter.awaitQueued()
            admission.release("active")
            waiter.assertAcquired()
            admission.release(catalog)
        }
    }

    @Test
    fun `eligible catalogs bypass a busy catalog while eligible waiters keep FIFO order`() {
        val admission = PartitionLookupAdmission(2)
        assertThat(admission.acquire("active", deadline())).isTrue()
        val sameCatalog = Waiter(admission, "active")
        sameCatalog.awaitQueued()
        assertThat(admission.acquire("other", deadline())).isTrue()
        val first = Waiter(admission, "first")
        first.awaitQueued()
        val second = Waiter(admission, "second")
        second.awaitQueued()
        admission.release("other")
        first.assertAcquired()
        assertThat(second.result.get()).isNull()
        admission.release("first")
        second.assertAcquired()
        admission.release("second")
        admission.release("active")
        sameCatalog.assertAcquired()
        admission.release("active")
    }

    @Test
    fun `queue limits bound total waiters and each catalog and interruption frees every slot`() {
        val admission = PartitionLookupAdmission(2)
        assertThat(admission.acquire("active", deadline())).isTrue()
        assertThat(admission.acquire("other", deadline())).isTrue()
        val waiters = mutableListOf<Waiter>()
        try {
            repeat(2) {
                waiters.add(Waiter(admission, "active").also { it.awaitQueued() })
            }
            val perCatalogStart = System.nanoTime()
            assertThat(admission.acquire("active", deadline())).isFalse()
            assertThat(elapsedMillis(perCatalogStart)).isLessThan(100)
            for (catalog in listOf("third", "fourth")) {
                waiters.add(Waiter(admission, catalog).also { it.awaitQueued() })
            }
            val globalStart = System.nanoTime()
            assertThat(admission.acquire("fifth", deadline())).isFalse()
            assertThat(elapsedMillis(globalStart)).isLessThan(100)
        } finally {
            waiters.forEach { it.thread.interrupt() }
            waiters.forEach { it.thread.join(1000) }
            admission.release("active")
            admission.release("other")
        }
        assertThat(waiters.map { it.result.get() }).allMatch { it is InterruptedException }
        for (catalog in listOf("active", "other", "third", "fourth", "fifth")) {
            assertThat(admission.acquire(catalog, deadline())).isTrue()
            admission.release(catalog)
        }
    }

    @Test
    fun `waiting is bounded by both admission allowance and request deadline and timeout frees queue`() {
        val admission = PartitionLookupAdmission(1)
        assertThat(admission.acquire("active", deadline())).isTrue()
        val shortStart = System.nanoTime()
        assertThat(admission.acquire("other", deadline(30))).isFalse()
        assertThat(elapsedMillis(shortStart)).isBetween(15, 150)
        val boundedStart = System.nanoTime()
        assertThat(admission.acquire("other", deadline())).isFalse()
        assertThat(elapsedMillis(boundedStart)).isBetween(150, 800)
        admission.release("active")
        assertThat(admission.acquire("other", deadline())).isTrue()
        admission.release("other")
        assertThat(admission.acquire("active", System.nanoTime() - 1)).isFalse()
    }

    private class Waiter(admission: PartitionLookupAdmission, catalog: String) {
        val result = AtomicReference<Any?>()
        private val finished = CountDownLatch(1)
        val thread =
            Thread {
                try {
                    result.set(admission.acquire(catalog, deadline()))
                } catch (e: InterruptedException) {
                    result.set(e)
                } finally {
                    finished.countDown()
                }
            }.also { it.start() }

        fun awaitQueued() {
            await().pollInterval(Duration.ofMillis(1)).atMost(Duration.ofSeconds(1))
                .until { thread.state == Thread.State.TIMED_WAITING }
        }

        fun assertAcquired() {
            assertThat(finished.await(1, TimeUnit.SECONDS)).isTrue()
            assertThat(result.get()).isEqualTo(true)
        }
    }

    private companion object {
        fun deadline(ms: Long = 3000) = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ms)

        fun elapsedMillis(started: Long) = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)
    }
}
