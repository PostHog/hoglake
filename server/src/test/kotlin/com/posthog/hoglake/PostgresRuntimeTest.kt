package com.posthog.hoglake

import com.posthog.hoglake.model.HoglakeException
import com.zaxxer.hikari.HikariDataSource
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class PostgresRuntimeTest {
    @Test
    fun `initialization is single flight and reuses one Jdbi and pool`() {
        val opens = AtomicInteger()
        val migrations = AtomicInteger()
        val notifications = AtomicInteger()
        val entered = CountDownLatch(1)
        val release = CountDownLatch(1)
        val pool = HikariDataSource()
        val runtime =
            PostgresRuntime(
                Config(defaultBackend = "none"),
                open = {
                    opens.incrementAndGet()
                    pool
                },
                migrate = {
                    migrations.incrementAndGet()
                    entered.countDown()
                    check(release.await(5, TimeUnit.SECONDS))
                },
            )
        runtime.onInitialized = { notifications.incrementAndGet() }
        assertThat(runtime.isInitialized).isFalse()
        assertThat(opens.get()).isZero()
        Executors.newFixedThreadPool(8).use { executor ->
            val results = (1..8).map { executor.submit<org.jdbi.v3.core.Jdbi> { runtime.get() } }
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue()
            assertThat(runtime.isInitialized).isFalse()
            release.countDown()
            val jdbi = results.first().get(5, TimeUnit.SECONDS)
            results.forEach { assertThat(it.get(5, TimeUnit.SECONDS)).isSameAs(jdbi) }
            assertThat(runtime.get()).isSameAs(jdbi)
        }
        assertThat(opens.get()).isEqualTo(1)
        assertThat(migrations.get()).isEqualTo(1)
        assertThat(notifications.get()).isEqualTo(1)
        runtime.close()
        assertThat(pool.isClosed).isTrue()
        assertThatThrownBy { runtime.get() }.isInstanceOf(IllegalStateException::class.java)
    }

    @Test
    fun `a failed migration closes the pool and a later request can retry`() {
        val pools = mutableListOf<HikariDataSource>()
        val runtime =
            PostgresRuntime(
                Config(defaultBackend = "none"),
                open = { HikariDataSource().also(pools::add) },
                migrate = { if (pools.size == 1) error("database unavailable") },
            )
        runtime.use {
            assertThatThrownBy { it.get() }.isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            assertThat(it.isInitialized).isFalse()
            assertThat(pools.single().isClosed).isTrue()
            val jdbi = it.get()
            assertThat(it.isInitialized).isTrue()
            assertThat(it.get()).isSameAs(jdbi)
            assertThat(pools).hasSize(2)
        }
        assertThat(pools).allMatch { it.isClosed }
    }

    @Test
    fun `a post initialization hook failure does not discard the migrated pool`() {
        val pool = HikariDataSource()
        val migrations = AtomicInteger()
        PostgresRuntime(
            Config(defaultBackend = "none"),
            open = { pool },
            migrate = { migrations.incrementAndGet() },
        ).use { runtime ->
            runtime.onInitialized = { error("hook failed") }
            assertThatThrownBy { runtime.get() }.hasMessage("hook failed")
            assertThat(runtime.isInitialized).isTrue()
            assertThat(pool.isClosed).isFalse()
            assertThat(runtime.get()).isSameAs(runtime.get())
            assertThat(migrations.get()).isEqualTo(1)
        }
        assertThat(pool.isClosed).isTrue()
    }
}
