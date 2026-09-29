package com.posthog.hoglake.model

import java.time.Instant

/**
 * One partition key/value pair of a listed partition, in key_index
 * order: the label, the value as STORED, and the value as DISPLAYED.
 *
 * Both forms ship because both are load-bearing. [decoded] is what the
 * console shows and what `filter=` matches; [raw] is what
 * `partition=key_index:value` on the files listing takes, so a row can
 * link straight to its own files without the client re-encoding a date
 * back into an ordinal. [decoded] is null exactly when [raw] is — a null
 * partition value, which sorts first and is matched by an EMPTY filter.
 */
data class PartitionListingValue(
    val field: String,
    val raw: String?,
    val decoded: String?,
)

/** One partition field of the table's CURRENT spec, for the console's header and filters. */
data class PartitionSpecField(
    val field: String,
    val transform: String,
    val transformParam: Int?,
    val sourceFieldId: Long,
)

/** The table's partition spec at head; absent when the table is unpartitioned. */
data class PartitionSpecSummary(
    val specId: Long,
    val fields: List<PartitionSpecField>,
)

/**
 * One sampled partition of one table: the live files sharing one
 * `(spec_id, partition value tuple)` as the maintenance sampler last saw
 * them. An unpartitioned table (or the pre-spec vintage of a
 * later-partitioned one) is a single group with empty [values] and a
 * null [specId]; files written under an older spec group under THEIR
 * spec id — vintages never mix.
 */
data class PartitionGroup(
    val specId: Long?,
    val values: List<PartitionListingValue>,
    val fileCount: Long,
    val smallFileCount: Long,
    val totalBytes: Long,
    val smallFileBytes: Long,
    /** totalBytes / fileCount, floored; 0 when the group has no files. */
    val avgFileBytes: Long,
    val dvCount: Long,
    /** Actionable compaction debt, the same measure `/stats/partitions` ranks by. */
    val debtScore: Long,
    /**
     * Rows in the partition, or null when the PUBLISHED GENERATION was
     * not measured by a V22-aware sampler. Null is not zero: a pre-V22
     * generation carries the column's `DEFAULT 0`, and a generation
     * that straddles the deploy carries a partial sum — both would be
     * fabricated numbers. The gate is generation-scoped
     * (`hog_maintenance_summary.measures_generation`), not per row,
     * because the straddling case is invisible in the row.
     */
    val recordCount: Long?,
    /** max(begin_snapshot) over the partition's files; null on a pre-V22 sample. */
    val lastWrittenSnapshot: Long?,
)

/**
 * `GET .../tables/{table}/partitions` — a table's partitions as the
 * maintenance sampler last measured them.
 *
 * THE SAMPLE IS THE POINT. Every number here is at [sampledSnapshotId],
 * not at head, and it is stated on the response rather than implied so a
 * console can date what it shows. That is what buys the listing its
 * cost: no manifest walk, no lock, no per-commit work — the sampler has
 * already walked every file once per generation and this is a read of
 * its output restricted to one table. It is also why the endpoint takes
 * no `snapshot`/`at_timestamp`: there is exactly one snapshot it can
 * answer at, and accepting another would be a time-travel API that
 * silently ignored its argument.
 *
 * [sampledAt] and [sampleStarted] null mean no published sample for the catalog (a fresh
 * instance, or a policy change that invalidated the last one) — with
 * [partitions] empty and [total] 0, and a 200: the table exists, the
 * measurement does not yet.
 */
data class PartitionListing(
    /**
     * When the sample was PUBLISHED. The numbers are older than this by
     * however long the scan ran — see [sampleStarted], which is the one
     * a freshness display must use.
     */
    val sampledAt: Instant?,
    /**
     * When the sample's scan STARTED, which is when
     * [sampledSnapshotId] was captured. A generation takes ~30 minutes
     * on a production catalog, so "published a minute ago" and "measured
     * a minute ago" differ by that much; this is the honest one.
     */
    val sampleStarted: Instant?,
    val sampledSnapshotId: Long?,
    /** The spec at HEAD, which may be newer than some groups' — see [staleSpecGroups]. */
    val spec: PartitionSpecSummary?,
    /** Groups matching the filter, before paging. */
    val total: Int,
    /**
     * Of those, the ones whose vintage is not the current spec —
     * partitions a spec change left behind, which the console badges.
     * Counted over the FILTERED set, so it agrees with [total] rather
     * than with a set the caller cannot see.
     */
    val staleSpecGroups: Int,
    val partitions: List<PartitionGroup>,
)
