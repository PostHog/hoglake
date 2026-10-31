package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.DataFile
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionLookup
import com.posthog.hoglake.model.PartitionLookupAggregates
import com.posthog.hoglake.model.PartitionLookupInvalidRequest
import com.posthog.hoglake.model.PartitionLookupStatsState
import com.posthog.hoglake.model.PartitionLookupTotals
import com.posthog.hoglake.model.PartitionLookupUnavailable
import com.posthog.hoglake.model.StatsState
import com.posthog.hoglake.model.validatePartitionLookupValues
import com.posthog.hoglake.service.PartitionLookupAdmission
import com.posthog.hoglake.service.PartitionLookupBackend
import com.posthog.hoglake.service.PartitionLookupCursor
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.concurrent.TimeUnit

/**
 * Exact partition inspection (`POST .../partitions/lookup`) on hoglite:
 * [com.posthog.hoglake.service.PartitionLookupService]'s contract over
 * the tenant's `files` rows. Same validation and refusal order, same
 * cursor (format, scope, and the snapshot/limit/scope mismatch checks),
 * same candidate bound and continuation rule, same aggregate shapes and
 * `unavailable` reasons, same admission limit and per-request budget.
 *
 * What differs is how "candidate" is read, because the tenant has no
 * `hog_file_partition_value` inverse index to probe. Postgres probes the
 * one tuple key with the fewest entries; here the probe is the table's
 * files in the id window whose WHOLE stored tuple equals the requested
 * one (any spec, live or retained-ended — like Postgres, retained ended
 * files still count), in id order, at most `candidateLimit + 1`. The
 * bounded window, the `candidate_scan_limit` refusal of aggregates, and
 * the `last_examined` continuation all apply to that set unchanged, so a
 * caller pages through the same files with the same cursor semantics;
 * only the exact candidate count past the cap can differ between the
 * backends, which the contract never promised.
 *
 * The budget is cooperative: hoglite has no statement_timeout, so the
 * deadline is checked before and after each query rather than cancelling
 * one mid-flight. Each query is bounded by LIMIT, so none is unbounded.
 *
 * Consistency without a snapshot transaction: every row read is resolved
 * AT a snapshot, and history at a retained snapshot is immutable — only
 * expiry removes rows, and only rows invisible at every snapshot at or
 * above its floor. So the floor is re-read last; if it moved past the
 * lookup's snapshot, the answer is the same `Expired` the first read
 * would have given.
 */
internal class HoglitePartitionLookup(
    private val store: HogliteCatalogStore,
    private val candidateLimit: Int = 10_000,
    private val timeoutMs: Int = 3_000,
    maxConcurrentRequests: Int = 2,
) : PartitionLookupBackend {
    init {
        require(candidateLimit in 1 until Int.MAX_VALUE)
        require(timeoutMs > 0)
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
        val deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs.toLong())
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
            return store.catalogOperation(catalog) {
                lookupBound(catalog, namespace, table, specId, values, snapshot, continuation, effectiveLimit, deadline)
            }
        } finally {
            admission.release(catalog)
        }
    }

    private fun lookupBound(
        catalog: String,
        namespace: String,
        table: String,
        specId: Long,
        values: List<String?>,
        snapshot: Long?,
        continuation: PartitionLookupCursor?,
        effectiveLimit: Int,
        deadline: Long,
    ): PartitionLookup {
        remainingMillis(deadline)
        val m = store.meta(catalog)
        val at = snapshot ?: continuation?.snapshot ?: m.head
        if (at < 0 || at > m.head) {
            throw HoglakeException.Validation("snapshot must be between 0 and catalog head")
        }
        if (at < m.earliestSnapshotId) throw HoglakeException.Expired("snapshot $at has expired")
        val ns =
            store.queryCatalog(
                catalog,
                "SELECT id FROM namespaces WHERE name = ? AND created_snapshot <= ? " +
                    "AND (dropped_snapshot IS NULL OR dropped_snapshot > ?)",
                namespace,
                at,
                at,
            ).firstRow()?.get(0)?.asLong()
                ?: throw HoglakeException.NotFound("namespace '$namespace' at snapshot $at")
        val resolved =
            try {
                store.tableAt(catalog, ns, namespace, table, at)
            } catch (_: HoglakeException.NotFound) {
                throw HoglakeException.NotFound("table '$namespace.$table' at snapshot $at")
            }
        val upper =
            continuation?.upper
                ?: store.queryCatalog(catalog, "SELECT next_file_id FROM meta WHERE id = 1").firstRow()!![0].asLong()
        val scope =
            PartitionLookupCursor.scope(
                incarnationId(store.boundTenant(catalog)),
                resolved.tableId,
                resolved.tableUuid,
                specId,
                values,
                upper,
                at,
                effectiveLimit,
            )
        if (continuation != null && continuation.scope != scope) {
            throw PartitionLookupInvalidRequest("cursor does not match lookup")
        }
        // The spec as DEFINED at `at` (begun by then), retired or not:
        // expiry keeps every spec row (ExpiryService's never-cleared set),
        // so a retired spec is still inspectable while its files are.
        val arity =
            store.queryCatalog(
                catalog,
                "SELECT json_array_length(fields_json) FROM partition_specs " +
                    "WHERE table_id = ? AND spec_id = ? AND begin_snapshot <= ?",
                resolved.tableId,
                specId,
                at,
            ).firstRow()?.get(0)?.asLong()?.toInt()
                ?: throw HoglakeException.Validation(
                    "partition spec $specId is unavailable at snapshot $at " +
                        "(unknown, future, or expired definition)",
                )
        if (arity != values.size) {
            throw HoglakeException.Validation(
                "values must contain the complete $arity-field tuple for spec $specId",
            )
        }
        remainingMillis(deadline)
        val includeAggregates = continuation == null
        val after = continuation?.after ?: -1
        val args = ArrayList<Any?>()
        args += at
        args += at
        args += resolved.tableId
        args += after
        args += upper
        args += values.size
        val predicates =
            values.mapIndexed { i, value ->
                if (value == null) {
                    "json_type(f.partition_values_json, '$[$i]') = 'null'"
                } else {
                    args += value
                    "json_type(f.partition_values_json, '$[$i]') = 'text' " +
                        "AND json_extract(f.partition_values_json, '$[$i]') = ?"
                }
            }.joinToString(" AND ")
        args += candidateLimit + 1
        val candidates =
            store.queryCatalog(
                catalog,
                "SELECT ${HogliteSql.FILE_COLUMNS}, f.end_snapshot, " +
                    "(SELECT CASE WHEN d.end_snapshot IS NULL OR d.end_snapshot > ? THEN 1 ELSE 0 END " +
                    "FROM dvs d WHERE d.data_file_id = f.id AND d.begin_snapshot <= ? " +
                    "ORDER BY d.begin_snapshot DESC, d.delete_file_id DESC LIMIT 1) " +
                    "FROM files f WHERE f.table_id = ? AND f.id > ? AND f.id < ? " +
                    "AND json_array_length(f.partition_values_json) = ? AND $predicates " +
                    "ORDER BY f.id LIMIT ?",
                *args.toTypedArray(),
            ).rows
        remainingMillis(deadline)
        val candidateCount = candidates.size
        val window = candidates.take(candidateLimit)
        val lastExamined = window.lastOrNull()?.get(0)?.asLong()
        val matched =
            window.filter { r ->
                val begin = r[14].asLong()
                val end = r[15].asLongOrNull()
                r[9].asLongOrNull() == specId && begin <= at && (end == null || end > at)
            }
        val aggregates =
            when {
                !includeAggregates -> PartitionLookupAggregates("unavailable", "continuation", null)
                candidateCount > candidateLimit ->
                    PartitionLookupAggregates(
                        "unavailable",
                        "candidate_scan_limit",
                        null,
                    )
                else -> totals(matched)
            }
        val page = matched.take(effectiveLimit + 1).map { store.toDataFile(it) }
        val returned: List<DataFile> = page.take(effectiveLimit)
        val nextAfter =
            when {
                page.size > effectiveLimit -> returned.last().dataFileId
                candidateCount > candidateLimit -> lastExamined
                else -> null
            }
        // The floor, last: see the class KDoc.
        if (store.meta(catalog).earliestSnapshotId > at) throw HoglakeException.Expired("snapshot $at has expired")
        remainingMillis(deadline)
        return PartitionLookup(
            at,
            resolved.tableUuid,
            specId,
            values,
            aggregates,
            returned,
            nextAfter?.let { PartitionLookupCursor(at, it, upper, effectiveLimit, scope).encode() },
        )
    }

    private fun totals(matched: List<List<Any?>>): PartitionLookupAggregates =
        try {
            var rows = 0L
            var bytes = 0L
            var dvs = 0L
            var provided = 0L
            var pending = 0L
            var failed = 0L
            var lastWrite: Long? = null
            for (r in matched) {
                rows = Math.addExact(rows, r[4].asLong())
                bytes = Math.addExact(bytes, r[5].asLong())
                if (r[16].asLongOrNull() == 1L) dvs++
                when (StatsState.fromWire(r[11].asString())) {
                    StatsState.PROVIDED -> provided++
                    StatsState.PENDING -> pending++
                    StatsState.FAILED -> failed++
                }
                val begin = r[14].asLong()
                lastWrite = maxOf(lastWrite ?: begin, begin)
            }
            PartitionLookupAggregates(
                "exact",
                null,
                PartitionLookupTotals(
                    matched.size.toLong(),
                    rows,
                    bytes,
                    dvs,
                    PartitionLookupStatsState(provided, pending, failed),
                    lastWrite,
                ),
            )
        } catch (_: ArithmeticException) {
            PartitionLookupAggregates("unavailable", "overflow", null)
        }

    private fun remainingMillis(deadline: Long): Long {
        val remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime())
        if (remaining <= 0) timeout()
        return remaining
    }

    private fun busy(): Nothing =
        throw PartitionLookupUnavailable("busy", "partition lookup capacity is busy; retry shortly")

    private fun timeout(): Nothing =
        throw PartitionLookupUnavailable("timeout", "partition lookup exceeded its time budget")

    private companion object {
        /**
         * The cursor scope's catalog component: the bound TENANT, hashed,
         * so a continuation minted against one incarnation of a catalog
         * name never matches a lookup against a later one (the table
         * uuid in the scope already differs; this pins the catalog too).
         */
        fun incarnationId(tenant: String): Long =
            ByteBuffer.wrap(MessageDigest.getInstance("SHA-256").digest(tenant.toByteArray(Charsets.UTF_8))).long
    }
}
