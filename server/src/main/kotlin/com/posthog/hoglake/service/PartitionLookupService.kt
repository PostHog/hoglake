package com.posthog.hoglake.service

import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionLookup
import com.posthog.hoglake.model.PartitionLookupInvalidRequest
import com.posthog.hoglake.model.PartitionLookupUnavailable
import com.posthog.hoglake.model.validatePartitionLookupValues
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.NamespaceRepo
import com.posthog.hoglake.persistence.PartitionLookupRepo
import com.posthog.hoglake.persistence.Pg
import com.posthog.hoglake.persistence.TableRepo
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.jdbi.v3.core.statement.SqlLogger
import org.jdbi.v3.core.statement.SqlStatements
import org.jdbi.v3.core.statement.StatementContext
import java.sql.SQLException
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/** Exact lookup is independent of sampler state and bounded before borrowing a database connection. */
class PartitionLookupService(
    private val jdbi: Jdbi,
    private val candidateLimit: Int = 10_000,
    private val statementTimeoutMs: Int = 3_000,
    maxConcurrentRequests: Int = 2,
) : PartitionLookupBackend {
    init {
        require(candidateLimit in 1 until Int.MAX_VALUE)
        require(statementTimeoutMs > 0)
        require(maxConcurrentRequests > 0)
    }

    private val admission = PartitionLookupAdmission(maxConcurrentRequests)

    override fun lookup(
        catalog: String,
        namespace: String,
        table: String,
        specId: Long,
        values: List<String?>,
        snapshot: Long?,
        limit: Int?,
        cursor: String?,
    ): PartitionLookup {
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(statementTimeoutMs.toLong())
        if (specId <= 0) throw PartitionLookupInvalidRequest("spec_id must be positive")
        if (limit != null && limit !in 1..500) throw PartitionLookupInvalidRequest("limit must be between 1 and 500")
        validatePartitionLookupValues(values)
        val continuation = cursor?.let { PartitionLookupCursor.parse(it) }
        val effectiveLimit = limit ?: continuation?.limit ?: 100
        if (continuation != null &&
            ((limit != null && continuation.limit != limit) || (snapshot != null && continuation.snapshot != snapshot))
        ) {
            throw PartitionLookupInvalidRequest("cursor does not match snapshot or limit")
        }
        try {
            if (!admission.acquire(catalog, deadline)) {
                remainingMillis(deadline)
                busy()
            }
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
            busy()
        }
        try {
            val rows =
                jdbi.withHandleUnchecked { h ->
                    remainingMillis(deadline)
                    h.inTransaction<LookupRows, RuntimeException> {
                        h.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                        val remaining = remainingMillis(deadline)
                        h.createQuery(
                            "SELECT set_config('transaction_timeout', :backstop, true) AS transaction_timeout, " +
                                "set_config('statement_timeout', :remaining, true) AS statement_timeout",
                        ).bind("backstop", "${remaining + TRANSACTION_BACKSTOP_MARGIN_MS}ms")
                            .bind("remaining", "${remaining}ms").mapToMap().one()
                        val statements = h.getConfig(SqlStatements::class.java)
                        val delegate = statements.sqlLogger
                        statements.sqlLogger =
                            object : SqlLogger {
                                override fun logBeforeExecution(context: StatementContext) {
                                    delegate.logBeforeExecution(context)
                                    // JDBC avoids re-entering this JDBI hook for the timeout update.
                                    h.connection.prepareStatement(
                                        "SELECT set_config('statement_timeout', ?, true)",
                                    ).use {
                                            stmt ->
                                        stmt.setString(1, "${remainingMillis(deadline)}ms")
                                        stmt.execute()
                                    }
                                }

                                override fun logAfterExecution(context: StatementContext) =
                                    delegate.logAfterExecution(context)

                                override fun logException(
                                    context: StatementContext,
                                    ex: SQLException,
                                ) = delegate.logException(context, ex)
                            }
                        val cat = CatalogRepo.require(h, catalog)
                        val at = snapshot ?: continuation?.snapshot ?: cat.headSnapshotId
                        if (at < 0 || at > cat.headSnapshotId) {
                            throw HoglakeException.Validation(
                                "snapshot must be between 0 and catalog head",
                            )
                        }
                        if (at < cat.earliestSnapshotId) throw HoglakeException.Expired("snapshot $at has expired")
                        val ns =
                            NamespaceRepo.findAt(h, cat.catalogId, namespace, at)
                                ?: throw HoglakeException.NotFound("namespace '$namespace' at snapshot $at")
                        val resolved =
                            TableRepo.findAt(h, cat.catalogId, ns.namespaceId, table, at)
                                ?: throw HoglakeException.NotFound("table '$namespace.$table' at snapshot $at")
                        val upper =
                            continuation?.upper
                                ?: h.createQuery("SELECT next_file_id FROM hog_catalog WHERE catalog_id = :catalogId")
                                    .bind("catalogId", cat.catalogId).mapTo(Long::class.javaObjectType).one()
                        val scope =
                            PartitionLookupCursor.scope(
                                cat.catalogId,
                                resolved.tableId,
                                resolved.tableUuid,
                                specId,
                                values,
                                upper,
                                at,
                                effectiveLimit,
                            )
                        if (continuation != null && continuation.scope != scope) {
                            throw PartitionLookupInvalidRequest(
                                "cursor does not match lookup",
                            )
                        }
                        val arity =
                            PartitionLookupRepo.specArity(h, cat.catalogId, resolved.tableId, specId, at)
                                ?: throw HoglakeException.Validation(
                                    "partition spec $specId is unavailable at snapshot $at " +
                                        "(unknown, future, or expired definition)",
                                )
                        if (arity != values.size) {
                            throw HoglakeException.Validation(
                                "values must contain the complete $arity-field tuple for spec $specId",
                            )
                        }
                        val result =
                            PartitionLookupRepo.lookup(
                                h,
                                cat.catalogId,
                                resolved.tableId,
                                specId,
                                values,
                                at,
                                upper,
                                continuation?.after ?: -1,
                                candidateLimit,
                                effectiveLimit,
                                includeAggregates = continuation == null,
                            )
                        remainingMillis(deadline)
                        LookupRows(at, resolved.tableUuid, upper, scope, result)
                    }
                }
            val page = rows.result.page
            val returned = page.files.take(effectiveLimit)
            val nextAfter =
                when {
                    page.files.size > effectiveLimit -> returned.last().dataFileId
                    page.candidateCount > candidateLimit -> page.lastExamined
                    else -> null
                }
            return PartitionLookup(
                rows.snapshot,
                rows.tableUuid,
                specId,
                values,
                rows.result.aggregates,
                returned,
                nextAfter?.let {
                    PartitionLookupCursor(rows.snapshot, it, rows.upper, effectiveLimit, rows.scope).encode()
                },
            )
        } catch (e: RuntimeException) {
            val state =
                generateSequence<Throwable>(e) { it.cause }.filterIsInstance<SQLException>()
                    .firstOrNull()?.sqlState
            if (state == Pg.QUERY_CANCELED || state == "25P04") timeout()
            throw e
        } finally {
            admission.release(catalog)
        }
    }

    private data class LookupRows(
        val snapshot: Long,
        val tableUuid: UUID,
        val upper: Long,
        val scope: String,
        val result: PartitionLookupRepo.Result,
    )

    private fun remainingMillis(deadline: Long): Long {
        val remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())
        if (remaining <= 0) timeout()
        return remaining
    }

    private companion object {
        const val TRANSACTION_BACKSTOP_MARGIN_MS = 5_000L
    }

    private fun busy(): Nothing =
        throw PartitionLookupUnavailable(
            "busy",
            "partition lookup capacity is busy; retry shortly",
        )

    private fun timeout(): Nothing =
        throw PartitionLookupUnavailable(
            "timeout",
            "partition lookup exceeded its time budget",
        )
}

/** Bounded FIFO among eligible catalogs; a busy catalog cannot park an otherwise free global slot. */
internal class PartitionLookupAdmission(private val capacity: Int) {
    private class Waiter(val catalog: String)

    private val lock = ReentrantLock(true)
    private val changed = lock.newCondition()
    private val activeCatalogs = mutableSetOf<String>()
    private val waiters = ArrayDeque<Waiter>()

    fun acquire(
        catalog: String,
        deadline: Long,
    ): Boolean {
        val waitDeadline = minOf(deadline, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(200))
        lock.lockInterruptibly()
        try {
            if (waiters.size >= capacity * 2 || waiters.count { it.catalog == catalog } >= 2) return false
            val waiter = Waiter(catalog)
            waiters.addLast(waiter)
            try {
                while (true) {
                    if (Thread.currentThread().isInterrupted) throw InterruptedException()
                    val remaining = waitDeadline - System.nanoTime()
                    if (remaining <= 0) return false
                    if (activeCatalogs.size < capacity &&
                        waiters.firstOrNull { it.catalog !in activeCatalogs } === waiter
                    ) {
                        activeCatalogs.add(catalog)
                        return true
                    }
                    changed.awaitNanos(remaining)
                }
            } finally {
                waiters.remove(waiter)
                changed.signalAll()
            }
        } finally {
            lock.unlock()
        }
    }

    fun release(catalog: String) =
        lock.withLock {
            check(activeCatalogs.remove(catalog))
            changed.signalAll()
        }
}
