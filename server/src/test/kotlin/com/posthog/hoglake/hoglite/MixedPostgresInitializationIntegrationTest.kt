package com.posthog.hoglake.hoglite

import com.posthog.hoglake.Database
import com.posthog.hoglake.MixedPostgres
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.testing.PgTestSupport
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Proxy
import java.sql.Connection
import java.sql.SQLException
import java.sql.Statement
import java.util.concurrent.CancellationException
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import javax.sql.DataSource

/** No engine dependency. Latches stop the first checkout BEFORE any connection or transaction exists. */
@Tag("integration")
class MixedPostgresInitializationIntegrationTest {
    private class ControlledSource(private val delegate: DataSource) : DataSource by delegate {
        val checkouts = AtomicInteger()
        val migrations = AtomicInteger()
        val next = AtomicReference<(() -> Unit)?>(null)

        override fun getConnection(): Connection = borrow { delegate.connection }

        override fun getConnection(
            user: String?,
            password: String?,
        ): Connection = borrow { delegate.getConnection(user, password) }

        private fun borrow(open: () -> Connection): Connection {
            checkouts.incrementAndGet()
            next.getAndSet(null)?.invoke()
            val connection = open()
            return Proxy.newProxyInstance(javaClass.classLoader, arrayOf(Connection::class.java)) { _, method, args ->
                try {
                    val value = method.invoke(connection, *(args ?: emptyArray()))
                    if (method.name != "createStatement") {
                        value
                    } else {
                        val statement = value as Statement
                        Proxy.newProxyInstance(javaClass.classLoader, arrayOf(Statement::class.java)) { _, m, a ->
                            if (m.name == "executeQuery" &&
                                a?.firstOrNull() == "SELECT pg_try_advisory_lock(${Database.MIGRATION_LOCK_KEY})"
                            ) {
                                migrations.incrementAndGet()
                            }
                            try {
                                m.invoke(statement, *(a ?: emptyArray()))
                            } catch (e: InvocationTargetException) {
                                throw e.targetException
                            }
                        }
                    }
                } catch (e: InvocationTargetException) {
                    throw e.targetException
                }
            } as Connection
        }
    }

    private data class Exit(val error: Throwable?, val interrupted: Boolean)

    private fun exit(body: () -> Unit): Exit =
        try {
            body()
            Exit(null, Thread.currentThread().isInterrupted)
        } catch (e: Throwable) {
            Exit(e, Thread.currentThread().isInterrupted)
        } finally {
            // Capture the flag above before clearing it on this test-owned worker.
            Thread.interrupted()
        }

    private fun <T> progress(future: Future<T>): T =
        try {
            future.get(5, TimeUnit.SECONDS)
        } catch (e: TimeoutException) {
            throw AssertionError("follower must complete while initializer release latch is closed", e)
        }

    private fun concurrent(failure: SQLException?) {
        PgTestSupport.freshDatabase().use { db ->
            val source = ControlledSource(db.dataSource)
            val initializer = MixedPostgres(source)
            val entered = CountDownLatch(1)
            val release = CountDownLatch(1)
            val workers = Executors.newFixedThreadPool(2)
            source.next.set {
                entered.countDown()
                check(release.await(30, TimeUnit.SECONDS)) { "test leader was not released" }
                if (failure != null) throw failure
            }
            val leader = workers.submit<Exit> { exit(initializer::ensureReady) }
            try {
                assertThat(entered.await(10, TimeUnit.SECONDS)).describedAs("leader reached pre-borrow latch").isTrue()
                repeat(3) {
                    val follower = progress(workers.submit<Exit> { exit(initializer::ensureReady) })
                    assertThat(follower.error).isInstanceOf(HoglakeException.BackendUnavailable::class.java)
                    assertThat(source.checkouts.get()).isEqualTo(1)
                    assertThat(source.migrations.get()).isZero()
                }
                assertThat(progress(workers.submit<Int> { 73 })).isEqualTo(73)
                assertThat(release.count).isEqualTo(1)
            } finally {
                release.countDown()
                try {
                    val result = leader.get(30, TimeUnit.SECONDS)
                    if (failure == null) {
                        assertThat(result.error).isNull()
                    } else {
                        assertThat(result.error).isInstanceOf(HoglakeException.BackendUnavailable::class.java)
                        assertThat(result.error!!.cause).isSameAs(failure)
                    }
                } finally {
                    workers.shutdownNow()
                    assertThat(workers.awaitTermination(10, TimeUnit.SECONDS)).isTrue()
                }
            }
            if (failure == null) {
                assertThat(source.migrations.get()).isEqualTo(1)
                val borrowed = source.checkouts.get()
                repeat(5) { initializer.ensureReady() }
                assertThat(source.checkouts.get()).isEqualTo(borrowed)
                assertThat(source.migrations.get()).isEqualTo(1)
            }
        }
    }

    @Test
    fun `blocked cold leader admits no follower migration or business IO`() = concurrent(SQLException("cold offline"))

    @Test
    fun `successful cold leader is the only concurrent initializer`() = concurrent(null)

    @Test
    fun `failed leader retries to success and ready calls never borrow again`() {
        PgTestSupport.freshDatabase().use { db ->
            val source = ControlledSource(db.dataSource)
            val initializer = MixedPostgres(source)
            val original = SQLException("first checkout fails")
            source.next.set { throw original }
            val first = exit(initializer::ensureReady)
            assertThat(first.error).isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            assertThat(first.error!!.cause).isSameAs(original)
            assertThat(source.migrations.get()).isZero()
            assertThat(exit(initializer::ensureReady).error)
                .describedAs("failed leader releases admission for a successful retry").isNull()
            assertThat(source.migrations.get()).isEqualTo(1)
            val borrowed = source.checkouts.get()
            repeat(5) { initializer.ensureReady() }
            assertThat(source.checkouts.get()).isEqualTo(borrowed)
        }
    }

    private fun terminal(error: Throwable) {
        PgTestSupport.freshDatabase().use { db ->
            val source = ControlledSource(db.dataSource)
            val initializer = MixedPostgres(source)
            val worker = Executors.newSingleThreadExecutor()
            source.next.set { throw error }
            try {
                val result = worker.submit<Exit> { exit(initializer::ensureReady) }.get(10, TimeUnit.SECONDS)
                assertThat(result.error).describedAs("direct terminal object must escape unchanged").isSameAs(error)
                assertThat(result.interrupted).isEqualTo(error is InterruptedException)
                assertThat(exit(initializer::ensureReady).error)
                    .describedAs("terminal exit releases admission for a successful retry").isNull()
                assertThat(source.migrations.get()).isEqualTo(1)
            } finally {
                worker.shutdownNow()
                assertThat(worker.awaitTermination(10, TimeUnit.SECONDS)).isTrue()
            }
            // A terminal nested in an ordinary exception is NOT unwrapped.
            val wrapped = IllegalStateException("ordinary outer failure", error)
            val another = MixedPostgres(source)
            source.next.set { throw wrapped }
            val result = exit(another::ensureReady)
            assertThat(result.error).isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            assertThat(result.error!!.cause).isSameAs(wrapped)
            assertThat(result.interrupted).isFalse()
            assertThat(exit(another::ensureReady).error)
                .describedAs("wrapped ordinary failure releases admission for retry").isNull()
            assertThat(source.migrations.get()).isEqualTo(2)
        }
    }

    @Test
    fun `interrupted initializer releases admission and preserves approved terminal semantics`() =
        terminal(InterruptedException("direct test interruption"))

    @Test
    fun `cancelled initializer releases admission and preserves approved terminal semantics`() =
        terminal(CancellationException("direct test cancellation"))

    @Test
    fun `fatal initializer error escapes unchanged and permits retry`() = terminal(AssertionError("direct test fatal"))
}
