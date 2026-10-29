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
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
import java.util.concurrent.Semaphore

/** Exact lookup is independent of sampler state and bounded before borrowing a database connection. */
class PartitionLookupService(
    private val jdbi: Jdbi,
    private val candidateLimit: Int = 10_000,
    private val statementTimeoutMs: Int = 3_000,
    maxConcurrentRequests: Int = 2,
) {
    init {
        require(candidateLimit in 1 until Int.MAX_VALUE)
        require(statementTimeoutMs > 0)
        require(maxConcurrentRequests > 0)
    }

    private val admission = Semaphore(maxConcurrentRequests)

    fun lookup(
        catalog: String,
        namespace: String,
        table: String,
        specId: Long,
        values: List<String?>,
        snapshot: Long? = null,
        limit: Int = 100,
        cursor: String? = null,
    ): PartitionLookup {
        if (specId <= 0) throw PartitionLookupInvalidRequest("spec_id must be positive")
        if (limit !in 1..500) throw PartitionLookupInvalidRequest("limit must be between 1 and 500")
        validatePartitionLookupValues(values)
        val continuation = cursor?.let { PartitionLookupCursor.parse(it) }
        if (continuation != null &&
            (continuation.limit != limit || (snapshot != null && continuation.snapshot != snapshot))
        ) {
            throw PartitionLookupInvalidRequest("cursor does not match snapshot or limit")
        }
        if (!admission.tryAcquire()) {
            throw PartitionLookupUnavailable(
                "busy",
                "partition lookup capacity is busy; retry shortly",
            )
        }
        try {
            return jdbi.inTransactionUnchecked { h ->
                h.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                h.execute("SET LOCAL statement_timeout = '$statementTimeoutMs'")
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
                val aggregates =
                    PartitionLookupRepo.aggregates(
                        h,
                        cat.catalogId,
                        resolved.tableId,
                        specId,
                        values,
                        at,
                        upper,
                        candidateLimit,
                    )
                val page =
                    PartitionLookupRepo.page(
                        h,
                        cat.catalogId,
                        resolved.tableId,
                        specId,
                        values,
                        at,
                        upper,
                        continuation?.after ?: -1,
                        candidateLimit,
                        limit,
                    )
                val returned = page.files.take(limit)
                val nextAfter =
                    when {
                        page.files.size > limit -> returned.last().dataFileId
                        page.candidateCount > candidateLimit -> page.lastExamined
                        else -> null
                    }
                PartitionLookup(
                    at,
                    resolved.tableUuid,
                    specId,
                    values,
                    aggregates,
                    returned,
                    nextAfter?.let { PartitionLookupCursor(at, it, upper, limit, scope).encode() },
                )
            }
        } catch (e: UnableToExecuteStatementException) {
            if (Pg.isQueryCanceled(
                    e,
                )
            ) {
                throw PartitionLookupUnavailable("timeout", "partition lookup exceeded its time budget")
            }
            throw e
        } finally {
            admission.release()
        }
    }
}
