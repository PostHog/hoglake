package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.StorageUsage
import org.jdbi.v3.core.Handle

/**
 * Exact, catalog-scoped footprint in one statement/MVCC view, with constant result memory.
 *
 * This deliberately counts ALL manifest rows, including ended files and dropped tables:
 * they occupy tracked storage until expiry/retirement removes their registrations. It is
 * not a reachability query and must not inherit a live-table or snapshot predicate.
 *
 * Exact sums must visit the catalog's data/delete registrations and undrained tickets;
 * the planner may scan broader tables when this catalog is a large fraction of them.
 * There is no O(live tables) shortcut. No commit lock, mutation, object-store call or
 * per-file result allocation.
 * The JDBC query deadline bounds this expensive exact read at 15 seconds; a stricter
 * database statement_timeout still applies. Use low-frequency internal polling, never
 * the console's refresh loop. A timeout is an error, not a partial or sampled answer.
 *
 * Local PG17 warm fixture: 200,000 data files, 20,000 delete files and 20,000 removal
 * tickets split equally between catalogs; one catalog returned in 17.3 ms, about
 * 0.15 microseconds per matching row (115,000). Scan-node buffers: data 609 (the
 * existing maintenance-size covering index), delete 173, removal 175. This is not
 * a production/cold-cache bound: before visibility-map coverage the same data
 * fixture favored a broader sequential scan. No new write-path index is added.
 */
object StorageUsageRepo {
    internal const val QUERY_TIMEOUT_SECONDS = 15

    internal const val SQL = """
        SELECT
            (SELECT COALESCE(SUM(file_size_bytes), 0)
             FROM hog_data_file WHERE catalog_id = :catalogId)
            +
            (SELECT COALESCE(SUM(file_size_bytes), 0)
             FROM hog_delete_file WHERE catalog_id = :catalogId) AS tracked_bytes,
            (SELECT COUNT(*)
             FROM hog_file_removal
             WHERE catalog_id = :catalogId AND drained_at IS NULL) AS pending_delete_files
        """

    fun read(
        handle: Handle,
        catalogId: Long,
    ): StorageUsage =
        handle.createQuery(SQL)
            .bind("catalogId", catalogId)
            .setQueryTimeout(QUERY_TIMEOUT_SECONDS)
            .map { rs, _ ->
                StorageUsage(
                    // PostgreSQL SUM(bigint) is numeric. Refuse overflow rather than
                    // truncating a footprint into a plausible, incorrect int64.
                    trackedBytes = rs.getBigDecimal("tracked_bytes").longValueExact(),
                    pendingDeleteFiles = rs.getLong("pending_delete_files"),
                )
            }
            .one()
}
