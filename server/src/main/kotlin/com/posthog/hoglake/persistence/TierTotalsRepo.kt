package com.posthog.hoglake.persistence

import com.posthog.hoglake.service.MaintenanceSummarySampler
import org.jdbi.v3.core.Handle

/**
 * A table's file totals read off the MAINTENANCE SAMPLER's published
 * generation instead of the manifest (#232).
 *
 * WHY THIS EXISTS RATHER THAN A SECOND SCANNER.
 * `GET .../tables/{table}` used to compute `count(*)`,
 * `sum(record_count)`, `sum(file_size_bytes)` over every live
 * `hog_data_file` row of the table on every call — ~10M rows on
 * gigahog-prod-us's `ingest.events_raw`, 0.7 s quiet and 9 s under
 * load, charged against a 10-thread request pool, with the writer fleet
 * as the main caller and none of the numbers ever read.
 *
 * `MaintenanceSummarySampler` has already walked every live file once
 * per generation and accumulated exactly these three measures per
 * `(table, spec, partition values)` bucket:
 * `MaintenanceSummarySampler.accumulate` does `pool.files++`,
 * `pool.bytes += …`, `pool.rows += …` over `rows.filter { it.visible }`,
 * and `hog_maintenance_summary_tier` stores them as `file_count`,
 * `total_bytes` and `record_count` (the last since V22). So a sum over
 * one table's buckets in the PUBLISHED generation IS that table's live
 * totals as of that generation's snapshot — already computed, already
 * paced in bounded background ticks, already indexed.
 *
 * That is also the rule, not an optimization. AGENT.md, under the
 * maintenance section: "Dashboard and partition-debt requests read
 * persisted asynchronous summaries, NEVER the manifest" — which
 * `MaintenanceStatusService`'s own KDoc states as "NEVER scan the
 * manifest on this path". A dedicated materialized view refreshed by its
 * own loop would have been a second full pass over `hog_data_file` for
 * numbers the first pass already produces.
 *
 * WHAT IS SHARED, AND WHAT IS NOT. [PER_TABLE_SQL] is the ONE per-table
 * sum in the tree; `CatalogService.getTable` is its only caller today
 * and the namespace listing is the intended second (see that statement's
 * KDoc in `TableRepo`). The two OTHER tier readers —
 * `PartitionStatsService.groupRows` and
 * `PartitionListingService.GROUP_ROWS_SQL` — cannot use it: they group
 * per `(table, spec, partition values)` rather than per table, and they
 * select a different measure set (`small_count`, `small_bytes`,
 * `dv_count`, `selected`). What all three DO share is the
 * published-generation predicate, and that is [PUBLISHED_GENERATION_JOIN],
 * spliced into both of them so the join is written once.
 */
object TierTotalsRepo {
    /**
     * What one head table GET needs to answer about totals, read in ONE
     * statement: the published generation's identity and freshness, the
     * table's own creation snapshot, and the sums.
     *
     * ONE STATEMENT ON PURPOSE. The obvious composition —
     * `MaintenanceSummarySampler.read` for the freshness, a second query
     * for `measures_generation`, a third for the sums, which is what
     * `PartitionListingService` does — costs three round trips on the
     * endpoint this change exists to make cheap. The summary table holds
     * exactly one row per catalog, so folding it in is a primary-key
     * join, not a scan.
     */
    data class TierTotals(
        val fileCount: Long,
        val recordCount: Long,
        val fileSizeBytes: Long,
        /**
         * The table's `created_snapshot`, so the caller can decide
         * whether the published generation COVERS this table at all —
         * see [PER_TABLE_SQL].
         */
        val createdSnapshot: Long,
        /**
         * Whether the published generation was accumulated by a sampler
         * that knows the V22 measures — `measures_generation =
         * published_generation`. False makes `record_count` a possibly
         * UNDERCOUNTED number rather than an absent one, which is worse
         * than saying nothing; see `CatalogService.getTable`.
         */
        val measured: Boolean,
        /** The published sample, or null when it is absent or unparseable. */
        val sample: MaintenanceSummarySampler.Sample?,
    )

    /**
     * The predicate that selects the PUBLISHED generation's tier rows,
     * written once and spliced into the three statements that need it
     * (`PartitionStatsService.groupRows`,
     * `PartitionListingService.GROUP_ROWS_SQL`, and any future reader
     * that has no generation in hand).
     *
     * `published_generation`, never `generation`: the sampler
     * accumulates the generation it is currently scanning and only
     * PUBLISHES a complete one, so joining on the live `generation`
     * would serve a half-accumulated scan as fact. Alias `p` for the
     * tier rows is the convention all three callers already use.
     *
     * [PER_TABLE_SQL] does not splice it either: it joins the summary
     * row itself, because it also needs that row's `sample` and its
     * `measures_generation`, and a value already on the row costs
     * nothing to carry.
     */
    const val PUBLISHED_GENERATION_JOIN: String =
        "JOIN hog_maintenance_summary s ON s.catalog_id = p.catalog_id " +
            "AND s.published_generation = p.generation"

    /**
     * One table's totals over the catalog's PUBLISHED generation, with
     * that generation's own identity and freshness alongside.
     *
     * `internal` so that `TierTotalsQueryPlanIntegrationTest` can EXPLAIN
     * THIS string rather than a retyped copy of it — a plan test that
     * restates its query asserts the plan of something no code path runs
     * (AGENT.md: an index proves itself against the query it serves).
     *
     * LEADS ON `hog_table`, WITH THE TIER ROWS ON A LEFT JOIN, and both
     * halves are load-bearing.
     *
     * Leading on `hog_table` is invariant 11's shape — the table is the
     * authority on whether its file rows are reachable data — and it is
     * what puts `created_snapshot` on the same row as the sums, so the
     * caller can tell a covered table from one the scan never saw
     * without a second read.
     *
     * The summary join is INNER: a catalog the sampler has no row for at
     * all returns nothing, which the caller reads as "not sampled" —
     * the same answer as a row whose `sample` is null, and one less
     * branch than making it outer and then testing the generation for
     * null. `published_generation` and `measures_generation` are NOT
     * NULL columns, so once a row comes back they are values.
     *
     * The tier join is LEFT, and that half IS load-bearing: it is what
     * lets an EMPTY table report zeros. The
     * sampler writes a tier row per bucket that HAS files, so a live
     * table with none has no tier row at all — the same absence as a
     * table the generation never saw. An inner join would collapse
     * those two into one answer and the API could not then report the
     * second as "not yet sampled". With the left join the sums are
     * `coalesce(…, 0)` for a covered-but-empty table, and the caller
     * separates the cases with `created_snapshot` against the
     * generation's own snapshot.
     *
     * ACCESS PATH: V22's `hog_maintenance_summary_tier_table
     * (catalog_id, generation, table_id)`, and the reason this read needs
     * no index of its own — the primary key's `bucket_key` is a SHA-256
     * and carries NO table locality, so without it this would be a scan
     * of the whole catalog's tier rows to answer about one table.
     *
     * HOW MUCH OF THAT INDEX THE PLANNER USES IS NOT FIXED, and the
     * difference is worth knowing before anyone changes retention. Two
     * shapes have been measured on PG 18 against the real `schema.sql`:
     *
     *  - as a NESTED-LOOP inner scan, with the summary row already
     *    resolved on the outer side, `published_generation` is a
     *    parameterized equality and all three columns land in the index
     *    condition. Measured by `TierTotalsQueryPlanIntegrationTest`'s
     *    fixture (3 catalogs x 1,000 tables x 18 buckets x 2 generations,
     *    3,177 heap pages): `Index Cond: ((catalog_id = ?) AND
     *    (generation = s.published_generation) AND (table_id = ?))`,
     *    `Index Searches: 1`, **18 rows and 4 buffers on the scan node**,
     *    nothing filtered.
     *  - under a merge/hash shape the generation equality is applied
     *    AFTER the scan, so the index condition is `(catalog_id,
     *    table_id)` — the middle column skip-scanned — and the read
     *    touches EVERY RETAINED GENERATION's buckets for the table
     *    before filtering. Measured on a wider fixture (5 catalogs x
     *    3,000 tables, 540,000 tier rows / 181 MB, 2 generations):
     *    0.19 ms / 20 buffers for a typical 18-bucket table, and
     *    1.6 ms / 161 buffers / 5,018 rows for the 5,000-bucket worst
     *    case `PartitionListingService` documents as its own stress
     *    shape.
     *
     * So the cost is bounded by `buckets x retained generations` in the
     * worse of the two shapes, and by `buckets` in the better one.
     * Against the 0.7 s quiet / 9 s loaded manifest aggregate this
     * replaces, both are three to four orders of magnitude better.
     *
     * WHAT WOULD CHANGE THAT: keeping more generations. The sampler keeps
     * the published one plus at most one in flight, so the multiplier is
     * 2 and nothing varies it today.
     * `TierTotalsQueryPlanIntegrationTest` pins the invariant part —
     * the index is used, BOTH `catalog_id` and `table_id` are in the
     * index condition (a plan that demoted `table_id` to a filter is
     * V16's recorded failure), rows looked at are bounded by
     * `buckets x generations`, and buffers by a fraction of the relation
     * — rather than pinning whichever of the two plans the planner picks
     * on a given fixture.
     */
    internal const val PER_TABLE_SQL: String =
        """
        SELECT t.created_snapshot,
               s.published_generation,
               s.measures_generation,
               -- min(), not a GROUP BY term: this aggregate has exactly
               -- ONE group by construction (hog_table filtered by its
               -- primary key, hog_maintenance_summary keyed on
               -- catalog_id), so min() over one row is that row — and
               -- grouping by a jsonb blob instead would hash the whole
               -- sample payload to prove a property the keys already
               -- guarantee.
               min(s.sample::text) AS sample,
               coalesce(sum(p.file_count), 0)   AS file_count,
               coalesce(sum(p.record_count), 0) AS record_count,
               coalesce(sum(p.total_bytes), 0)  AS file_size_bytes
          FROM hog_table t
          JOIN hog_maintenance_summary s
            ON s.catalog_id = t.catalog_id
          LEFT JOIN hog_maintenance_summary_tier p
                 ON p.catalog_id = t.catalog_id
                AND p.table_id = t.table_id
                AND p.generation = s.published_generation
         WHERE t.catalog_id = :catalogId AND t.table_id = :tableId
         GROUP BY t.created_snapshot, s.published_generation, s.measures_generation
        """

    /**
     * [tableId]'s totals summed over the catalog's PUBLISHED generation,
     * or null when the table row is gone (a concurrent retirement: the
     * caller resolved the table at a snapshot and this statement reads
     * `hog_table` at head).
     *
     * Null ALSO when the catalog has no summary row at all (the inner
     * join above), which is the sampler's warm-up state.
     *
     * A non-null result is NOT yet an answer. The caller must still
     * check that a sample was published, that the generation is
     * [TierTotals.measured], and that [TierTotals.createdSnapshot] is at
     * or below the generation's own snapshot — because a table created
     * after the scan began is indistinguishable here from one with no
     * files. `CatalogService.getTable` is where those three checks live
     * and where each one's answer is stated.
     *
     * NO `dropped_snapshot IS NULL` HERE, deliberately, and a second
     * caller must know why. `getTable` resolves the table through
     * `TableRepo.findAt` before reaching this, so a dropped table 404s
     * and never gets here — but a caller that skipped that resolution
     * would be handed a dropped-but-unretired table's last published
     * sums, because a drop is O(columns) and leaves its tier rows in
     * place until retirement. Any new caller either resolves the table
     * first (as the namespace listing already does through its version
     * row) or adds the predicate.
     */
    fun totalsFor(
        handle: Handle,
        catalogId: Long,
        tableId: Long,
    ): TierTotals? =
        handle.createQuery(PER_TABLE_SQL)
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .map { rs, _ ->
                TierTotals(
                    fileCount = rs.getLong("file_count"),
                    recordCount = rs.getLong("record_count"),
                    fileSizeBytes = rs.getLong("file_size_bytes"),
                    createdSnapshot = rs.getLong("created_snapshot"),
                    // getLong is safe on both: the summary join is INNER
                    // and both columns are NOT NULL, so a row that comes
                    // back has values. V22 defaults measures_generation
                    // to -1 rather than 0 precisely so a
                    // discovered-but-never-scanned catalog (generation 0)
                    // does not read as measured.
                    measured = rs.getLong("measures_generation") == rs.getLong("published_generation"),
                    sample = MaintenanceSummarySampler.sampleFrom(rs.getString("sample")),
                )
            }
            .findOne()
            .orElse(null)
}
