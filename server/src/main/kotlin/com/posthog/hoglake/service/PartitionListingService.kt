package com.posthog.hoglake.service

import com.posthog.hoglake.compaction.CompactionGrouping
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionGroup
import com.posthog.hoglake.model.PartitionListing
import com.posthog.hoglake.model.PartitionListingValue
import com.posthog.hoglake.model.PartitionSpecField
import com.posthog.hoglake.model.PartitionSpecSummary
import com.posthog.hoglake.model.PartitionValueDecoding
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.NamespaceRepo
import com.posthog.hoglake.persistence.SpecRepo
import com.posthog.hoglake.persistence.TableRepo
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked

/**
 * `GET /v1/catalogs/{c}/namespaces/{n}/tables/{t}/partitions` — the
 * console's partitions tab.
 *
 * WHAT IT COSTS, and why it is allowed to exist: nothing on the write
 * path and nothing on the manifest. The measures come from
 * `hog_maintenance_summary_tier`, which `MaintenanceSummarySampler`
 * fills one bounded page at a time in the background, so a request is
 * ONE indexed read of the published generation's rows for one table
 * (the V22 index `hog_maintenance_summary_tier_table` serves
 * `catalog_id, generation, table_id`; the primary key's `bucket_key` is
 * a SHA-256 and carries no table locality), plus two small lookups for
 * partition-field labels. Read-only, one REPEATABLE READ MVCC snapshot,
 * no locks — it never blocks a writer, and it takes no per-catalog lock
 * of any kind.
 *
 * THE BOUND ON THE MATERIALISED SET is the reason sorting, filtering and
 * paging happen in Kotlin rather than in SQL. A table's group count is
 * `partitions x spec versions` — not files, not rows — because the
 * sampler has already collapsed the manifest into one row per
 * `(table, spec, values)`. The ops shape hoglake is built for (coarse
 * months-by-team partitioning) puts that in the hundreds; a table that
 * partitions by day and has run for a decade with three spec revisions
 * is still under 11,000. So the service reads the whole set for the one
 * table and finishes the job in memory, which is what lets the
 * `partition` sort order by DECODED values (`2026-09-17`, not `20713`)
 * and the filter match the string the operator is looking at. Neither is
 * expressible as an ORDER BY or a LIKE over the stored `text[]`.
 *
 * MEASURED: the 5,000-group fixture in
 * `PartitionListingServiceIntegrationTest` — 5,000 buckets for the
 * table inside a published generation of 25,000 (five such tables) —
 * lists a 100-row page in 12.5 ms, decode, sort and paging included,
 * against the 200 ms budget that test gates on (PG 18 in
 * Testcontainers, warm, serial). A table that outgrows the bound
 * degrades in latency, not in correctness; the fix then is a keyset
 * over the stored columns with decoded sorting refused, not a bigger
 * page.
 *
 * SEMANTICS:
 *  - The sample is at the sampler's snapshot, which the response states.
 *    No `snapshot`/`at_timestamp`: see [PartitionListing].
 *  - The table must be LIVE (invariant 11) — a dropped table 404s, and
 *    the sampler skips its rows by key range anyway.
 *  - A catalog with no usable published sample answers 200 with
 *    `sampled_at: null` and no partitions. "Not measured yet" is not
 *    "no partitions", and it is not a 404 either: the table is there.
 *  - A sample computed under a different compaction policy than the one
 *    running now is treated as absent, exactly as `/stats/partitions`
 *    does — its `debt_score` would answer a question nobody asked.
 */
class PartitionListingService(
    private val jdbi: Jdbi,
    /** Compaction target size, which is also the small-file threshold (strict <). */
    private val smallFileThresholdBytes: Long,
    private val minInputFiles: Int = CompactionGrouping.DEFAULT_MIN_INPUT_FILES,
    private val maxInputFiles: Int = CompactionGrouping.DEFAULT_MAX_INPUT_FILES,
) {
    /**
     * Sortable columns. `PARTITION` sorts on the decoded value tuple
     * (nulls first, raw as the tiebreak) and defaults to ascending; every
     * measure defaults to DESCENDING, because "which partition has the
     * most debt" is the question a measure column is clicked to answer.
     */
    enum class SortColumn(val wire: String, val descendingByDefault: Boolean) {
        PARTITION("partition", false),
        FILES("files", true),
        SMALL_FILES("small_files", true),
        DEBT("debt", true),
        TOTAL_SIZE("total_size", true),
        AVG_SIZE("avg_size", true),
        DVS("dvs", true),
        ROWS("rows", true),
        LAST_WRITTEN("last_written", true),
        ;

        companion object {
            fun fromWire(s: String): SortColumn? = entries.firstOrNull { it.wire == s }
        }
    }

    /**
     * One `filter=<key_index>:<text>` clause. [text] is matched
     * case-insensitively as a PREFIX of the decoded value, so
     * `0:2026-09` selects every day in September under a day transform;
     * an EMPTY [text] means "the null value", which no prefix could
     * otherwise address.
     */
    data class Filter(val keyIndex: Int, val text: String)

    fun listPartitions(
        catalog: String,
        namespace: String,
        table: String,
        sort: SortColumn = SortColumn.PARTITION,
        desc: Boolean? = null,
        limit: Int = DEFAULT_LIMIT,
        offset: Int = 0,
        filters: List<Filter> = emptyList(),
    ): PartitionListing {
        if (limit <= 0) throw HoglakeException.Validation("limit must be positive (got $limit)")
        if (offset < 0) throw HoglakeException.Validation("offset must be >= 0 (got $offset)")
        val cappedLimit = limit.coerceAtMost(MAX_LIMIT)
        val descending = desc ?: sort.descendingByDefault

        // ONLY THE READ IS IN THE TRANSACTION. Decoding, filtering,
        // sorting and paging are CPU over a materialised list, and a
        // pool connection held across them is a connection no commit
        // can have — at the cap below that is tens of milliseconds of
        // pinned pool per request for work the database is not doing.
        val read = jdbi.inTransactionUnchecked { h -> read(h, catalog, namespace, table, filters) }

        val groups = read.rows.map { it.decode(read.tableId, read.labels, read.measured) }
        val matching = groups.filter { it.matches(filters) }
        val ordered = matching.sortedWith(comparator(sort, descending))
        return PartitionListing(
            sampledAt = read.sampledAt,
            sampleStarted = read.sampleStarted,
            sampledSnapshotId = read.sampledSnapshotId,
            spec = read.spec,
            total = matching.size,
            staleSpecGroups = matching.count { it.specId != read.spec?.specId },
            partitions = ordered.drop(offset).take(cappedLimit).map { it.group },
        )
    }

    /**
     * Everything one request reads, and nothing it computes. With no
     * usable sample the three sample fields are null and [rows] is
     * empty — the warm-up state, which still carries the SPEC, because
     * that comes from the catalog rather than from the sample and the
     * console builds its filter boxes from it.
     */
    private class Read(
        val tableId: Long,
        val spec: PartitionSpecSummary?,
        val labels: Map<Pair<Long, Long>, List<PartitionFieldLabels.Field>>,
        val sampledAt: java.time.Instant?,
        val sampleStarted: java.time.Instant?,
        val sampledSnapshotId: Long?,
        /** Whether the published generation's V22 measures can be reported. */
        val measured: Boolean,
        val rows: List<Row>,
    )

    /**
     * Resolve the names, the published sample and the table's buckets in
     * one REPEATABLE READ READ ONLY snapshot.
     */
    private fun read(
        h: Handle,
        catalog: String,
        namespace: String,
        table: String,
        filters: List<Filter>,
    ): Read {
        // First statement of the transaction: one consistent read-only
        // MVCC snapshot for every query below.
        h.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        val cat = CatalogRepo.require(h, catalog)
        val ns =
            NamespaceRepo.findLiveByName(h, cat.catalogId, namespace)
                ?: throw HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'")
        val t =
            TableRepo.findLive(h, cat.catalogId, ns.namespaceId, table)
                ?: throw HoglakeException.NotFound("table '$namespace.$table' in catalog '$catalog'")

        val headSpec = SpecRepo.specAt(h, cat.catalogId, t.tableId, cat.headSnapshotId)
        val arity = headSpec?.fields?.size ?: 0
        // Same rule as the files listing: a filter on a key the table
        // does not partition by at head can only be a client bug, so
        // refuse it rather than return an empty page the caller reads
        // as "no such partitions".
        filters.firstOrNull { it.keyIndex < 0 || it.keyIndex >= arity }?.let {
            throw HoglakeException.Validation(
                "partition key_index ${it.keyIndex} is out of range for table " +
                    "'$namespace.$table' ($arity partition keys)",
            )
        }

        val published =
            MaintenanceSummarySampler.read(h, listOf(cat.catalogId))[cat.catalogId]
                ?.takeIf {
                    // Only a sample computed under the policy running
                    // NOW is usable — see MaintenanceStatusService.
                    it.sample.targetBytes == smallFileThresholdBytes &&
                        it.sample.minInputFiles == minInputFiles &&
                        it.sample.maxInputFiles == maxInputFiles
                }
        val labels = PartitionFieldLabels.fields(h, cat.catalogId, listOf(t.tableId))
        val spec = headSpec?.let { summarize(it, t.tableId, labels) }
        if (published == null) {
            return Read(
                tableId = t.tableId,
                spec = spec,
                labels = labels,
                sampledAt = null,
                sampleStarted = null,
                sampledSnapshotId = null,
                measured = false,
                rows = emptyList(),
            )
        }
        // THE CAP IS THE READ'S OWN LIMIT, one past it. A pre-count
        // would be the SAME scan and the SAME aggregation run twice
        // (identical FROM/JOIN/WHERE/GROUP BY), not the extra index
        // descent an earlier draft of this comment claimed; bounding
        // the read costs nothing and makes the guarantee structural —
        // at most MAX_GROUPS + 1 rows ever reach the JVM.
        val rows = groupRows(h, cat.catalogId, t.tableId)
        if (rows.size > MAX_GROUPS) {
            // NO ADVICE TO FILTER. `filter` is applied in Kotlin, after
            // this read, so it cannot lower the row count and a caller
            // who followed that advice would get the identical refusal
            // forever. Point at the endpoint that CAN answer instead.
            throw HoglakeException.Validation(
                "table '$namespace.$table' has more than $MAX_GROUPS sampled partition groups; " +
                    "this listing decodes, sorts and pages them in memory and refuses above that. " +
                    "A 'filter' does not help (it is applied after the read). Use " +
                    "GET /v1/catalogs/{catalog}/stats/partitions for the catalog-wide ranking.",
            )
        }
        return Read(
            tableId = t.tableId,
            spec = spec,
            labels = labels,
            sampledAt = published.sampledAt,
            sampleStarted = published.sample.startedAt,
            sampledSnapshotId = published.sample.snapshotId,
            measured = measuresPublished(h, cat.catalogId),
            rows = rows,
        )
    }

    private fun summarize(
        spec: com.posthog.hoglake.model.PartitionSpec,
        tableId: Long,
        labels: Map<Pair<Long, Long>, List<PartitionFieldLabels.Field>>,
    ): PartitionSpecSummary {
        val fields = labels[tableId to spec.specId].orEmpty()
        return PartitionSpecSummary(
            specId = spec.specId,
            fields =
                spec.fields.mapIndexed { i, f ->
                    PartitionSpecField(
                        field = fields.getOrNull(i)?.name ?: "key_$i",
                        transform = f.transform.wire,
                        transformParam = f.transformParam,
                        sourceFieldId = f.sourceFieldId,
                    )
                },
        )
    }

    /**
     * Whether the PUBLISHED generation was measured by a sampler that
     * knows about `record_count` / `newest_begin_snapshot`.
     *
     * `measures_generation` IS STAMPED BY THE PUBLISH, from a flag the
     * scan carries in `scan_state` (`Scan.measures`, false by default
     * so a checkpoint an older build wrote resumes unmeasured). It
     * therefore names the last generation PUBLISHED with the measures,
     * and the equality below is true from the moment such a generation
     * publishes until a later one publishes without them.
     *
     * NOT STAMPED WHEN A GENERATION BEGINS, and that is the whole
     * distinction: a marker naming the generation being SCANNED
     * disagrees with `published_generation` from 60 s after a publish
     * until the next one lands — a ~30-minute generation on a
     * production catalog, so ~97% of wall-clock time with both
     * measures blanked. That was this change's round-2 defect, and it
     * lived in this comment before it lived in the code.
     *
     * Generation-scoped rather than row-scoped, also on purpose. A row
     * test ("is newest null?") catches a bucket a pre-V22 sampler wrote
     * and never revisited, and misses the one that matters: a
     * generation begun before the deploy and finished after it leaves
     * buckets whose accumulation restarted from the column defaults
     * partway through, so they carry a non-null newest beside an
     * UNDERCOUNTED record_count and look measured. See V22's header for
     * the direction this does not cover.
     */
    private fun measuresPublished(
        h: Handle,
        catalogId: Long,
    ): Boolean =
        h.createQuery(
            """
            SELECT measures_generation = published_generation
            FROM hog_maintenance_summary WHERE catalog_id = :catalogId
            """,
        ).bind("catalogId", catalogId).mapTo(Boolean::class.javaObjectType).findOne().orElse(false)

    // ---- the one statement -------------------------------------------------

    private data class Row(
        val specId: Long?,
        val values: List<String?>,
        val fileCount: Long,
        val smallFileCount: Long,
        val totalBytes: Long,
        val smallFileBytes: Long,
        val dvCount: Long,
        val debtScore: Long,
        val recordCount: Long,
        val newestBeginSnapshot: Long?,
    ) {
        /**
         * Decode the stored tuple against the spec the group's files were
         * WRITTEN under, not head's — which is the whole reason the
         * transforms are fetched per spec id. A group whose spec has no
         * field rows at all (never legal) falls back to identity
         * labelling rather than dropping the value.
         */
        fun decode(
            tableId: Long,
            labels: Map<Pair<Long, Long>, List<PartitionFieldLabels.Field>>,
            measured: Boolean,
        ): Decoded {
            val fields = specId?.let { labels[tableId to it] }.orEmpty()
            val decoded =
                values.mapIndexed { i, raw ->
                    val f = fields.getOrNull(i)
                    PartitionListingValue(
                        field = f?.name ?: "key_$i",
                        raw = raw,
                        decoded =
                            PartitionValueDecoding.decode(
                                f?.transform ?: "identity",
                                raw,
                                f?.transformParam,
                            ),
                    )
                }
            return Decoded(
                specId = specId,
                group =
                    PartitionGroup(
                        specId = specId,
                        values = decoded,
                        fileCount = fileCount,
                        smallFileCount = smallFileCount,
                        totalBytes = totalBytes,
                        smallFileBytes = smallFileBytes,
                        avgFileBytes = if (fileCount > 0) totalBytes / fileCount else 0,
                        dvCount = dvCount,
                        debtScore = debtScore,
                        // THE V22 MEASURES ARE GENERATION-SCOPED, not
                        // row-scoped. A per-row test ("is newest null?")
                        // blesses the straddling case — a generation
                        // begun before the deploy and finished after it,
                        // whose buckets restarted their accumulation
                        // from the column defaults partway through and
                        // so carry a non-null newest beside an
                        // UNDERCOUNTED record_count. `measured` is
                        // `measures_generation = published_generation`;
                        // see measuresPublished and V22's header.
                        recordCount = if (measured) recordCount else null,
                        lastWrittenSnapshot = if (measured) newestBeginSnapshot else null,
                    ),
            )
        }
    }

    /** A group with its decoded values, ready to filter, sort and page. */
    private class Decoded(val specId: Long?, val group: PartitionGroup) {
        /**
         * The decoded tuple, nulls first, raw as the tiebreak.
         *
         * A `val`, not a function: the comparator reads it on both
         * operands of every comparison, so computing it per call would
         * allocate ~2·n·log n lists per request — ~120k for the
         * 5,000-group fixture.
         */
        val sortKey: List<Pair<String?, String?>> = group.values.map { it.decoded to it.raw }

        fun matches(filters: List<Filter>): Boolean =
            filters.all { f ->
                // A group that does not HAVE key f.keyIndex — a stale
                // vintage of lower arity, or the unpartitioned group —
                // matches nothing, including the empty filter. Absent
                // is not null: `0:` asks for the partitions whose key 0
                // IS null, and a tuple with no key 0 has no such value
                // to be null.
                val value = group.values.getOrNull(f.keyIndex) ?: return@all false
                if (f.text.isEmpty()) {
                    // `<key_index>:` addresses the NULL value. No prefix
                    // can: "" is a prefix of every string, so an empty
                    // filter that meant "match everything" would be a
                    // clause the caller could never have wanted to send.
                    value.decoded == null
                } else {
                    value.decoded != null && value.decoded.startsWith(f.text, ignoreCase = true)
                }
            }
    }

    /**
     * Ordering over the materialised groups. Every column falls back to
     * the decoded tuple so the page is STABLE: two partitions with the
     * same file count must not swap places between requests, or paging
     * shows one twice and another never.
     */
    private fun comparator(
        sort: SortColumn,
        desc: Boolean,
    ): Comparator<Decoded> {
        val tuple = Comparator<Decoded> { a, b -> compareTuples(a.sortKey, b.sortKey) }
        if (sort == SortColumn.PARTITION) return if (desc) tuple.reversed() else tuple
        // NULL IS ALL-OR-NOTHING HERE, so there is no nulls-last arm to
        // write. The only nullable measures are `rows` and
        // `last_written`, and the gate that nulls them is
        // GENERATION-scoped: within one response either every group has
        // them or none does. An all-null column compares equal
        // throughout and falls through to the tuple, which is the
        // stable order paging needs. (A per-row nullable measure would
        // need nulls-last handling; adding one means adding it back,
        // with a test that can red.)
        val measure =
            Comparator<Decoded> { a, b ->
                val x = measureOf(sort, a) ?: 0L
                val y = measureOf(sort, b) ?: 0L
                if (desc) y.compareTo(x) else x.compareTo(y)
            }
        return measure.thenComparing(tuple)
    }

    private fun measureOf(
        sort: SortColumn,
        d: Decoded,
    ): Long? =
        when (sort) {
            SortColumn.PARTITION -> null
            SortColumn.FILES -> d.group.fileCount
            SortColumn.SMALL_FILES -> d.group.smallFileCount
            SortColumn.DEBT -> d.group.debtScore
            SortColumn.TOTAL_SIZE -> d.group.totalBytes
            SortColumn.AVG_SIZE -> d.group.avgFileBytes
            SortColumn.DVS -> d.group.dvCount
            SortColumn.ROWS -> d.group.recordCount
            SortColumn.LAST_WRITTEN -> d.group.lastWrittenSnapshot
        }

    /** Element-wise, nulls first; the shorter tuple sorts first on a tie. */
    private fun compareTuples(
        a: List<Pair<String?, String?>>,
        b: List<Pair<String?, String?>>,
    ): Int {
        for (i in 0 until minOf(a.size, b.size)) {
            val c = compareValue(a[i].first, b[i].first)
            if (c != 0) return c
            val r = compareValue(a[i].second, b[i].second)
            if (r != 0) return r
        }
        return a.size.compareTo(b.size)
    }

    private fun compareValue(
        a: String?,
        b: String?,
    ): Int =
        when {
            a == null && b == null -> 0
            a == null -> -1
            b == null -> 1
            else -> a.compareTo(b)
        }

    /**
     * The published generation's buckets for ONE table, collapsed to one
     * row per `(spec_id, partition_values)` — the same grouping and the
     * same measures as `PartitionStatsService.groupRows`, narrowed from
     * the catalog to a table and widened by the two V22 columns.
     *
     * `max(newest_begin_snapshot)` skips nulls, so a generation half
     * written by a pre-V22 sampler reports the newest snapshot it DID
     * measure rather than dropping the group's whole measurement.
     */
    private fun groupRows(
        h: Handle,
        catalogId: Long,
        tableId: Long,
    ): List<Row> =
        h.createQuery(GROUP_ROWS_SQL)
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .map { rs, _ ->
                Row(
                    specId = rs.getObject("spec_id")?.let { (it as Number).toLong() },
                    values =
                        (rs.getArray("partition_values")?.array as? Array<*>)
                            ?.map { it as String? } ?: emptyList(),
                    fileCount = rs.getLong("file_count"),
                    smallFileCount = rs.getLong("small_file_count"),
                    totalBytes = rs.getLong("total_bytes"),
                    smallFileBytes = rs.getLong("small_file_bytes"),
                    dvCount = rs.getLong("dv_count"),
                    debtScore = rs.getLong("debt_score"),
                    recordCount = rs.getLong("record_count"),
                    newestBeginSnapshot = rs.getObject("newest_begin_snapshot")?.let { (it as Number).toLong() },
                )
            }
            .list()

    companion object {
        /** Default page size when no `limit` is supplied. */
        const val DEFAULT_LIMIT = 100

        /** Hard cap on `limit`. */
        const val MAX_LIMIT = 1000

        /**
         * The most groups one request will materialise, over ALL pages.
         *
         * The design reads the table's whole group set and decodes,
         * filters, sorts and pages it in memory (see the class KDoc), so
         * the memory a request costs is set by the TABLE, not by
         * `limit`. At roughly 400-700 bytes of live objects per group,
         * 50,000 is ~20-35 MB for one request — already generous against
         * gigahog-prod-us's ~3.5k live tier rows for a whole catalog,
         * and still an order of magnitude under what a pool's worth of
         * concurrent requests could survive. It is enforced as the
         * READ's own `LIMIT MAX_GROUPS + 1`, so the bound is structural
         * — no request can materialise more than that, whatever the
         * table holds — and past it the listing refuses with a 422
         * rather than paging a heap dump. The refusal does NOT suggest
         * a filter: filters run in Kotlin after this read.
         */
        const val MAX_GROUPS = 50_000L

        /**
         * The read's bound, one past the cap so "over it" is
         * distinguishable from "exactly at it". Interpolated rather
         * than bound because it is a compile-time constant of this
         * class and `GROUP_ROWS_SQL` is `const` — no value from a
         * request reaches the SQL (invariant 9).
         */
        private const val MAX_GROUPS_PLUS_ONE = MAX_GROUPS + 1

        /**
         * The listing's one aggregation, `internal` so the V22 migration
         * test can EXPLAIN the statement the service actually issues
         * rather than a restatement of it (AGENT.md: an index proves
         * itself against the query it serves).
         */
        internal const val GROUP_ROWS_SQL = """
            SELECT p.spec_id, p.partition_values,
                   sum(p.file_count) AS file_count,
                   sum(p.small_count) AS small_file_count,
                   sum(p.total_bytes) AS total_bytes,
                   sum(p.small_bytes) AS small_file_bytes,
                   sum(p.dv_count) AS dv_count,
                   sum(p.selected) AS debt_score,
                   sum(p.record_count) AS record_count,
                   max(p.newest_begin_snapshot) AS newest_begin_snapshot
            FROM hog_maintenance_summary_tier p
            JOIN hog_maintenance_summary s ON s.catalog_id = p.catalog_id
              AND s.published_generation = p.generation
            WHERE p.catalog_id = :catalogId AND p.table_id = :tableId
            GROUP BY p.spec_id, p.partition_values
            LIMIT $MAX_GROUPS_PLUS_ONE
            """
    }
}
