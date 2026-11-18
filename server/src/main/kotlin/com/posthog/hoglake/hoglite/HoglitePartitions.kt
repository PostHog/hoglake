package com.posthog.hoglake.hoglite

import com.posthog.hoglake.compaction.CompactionGrouping
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MAX_COLUMN_NESTING_DEPTH
import com.posthog.hoglake.model.PartitionDebt
import com.posthog.hoglake.model.PartitionGroup
import com.posthog.hoglake.model.PartitionListing
import com.posthog.hoglake.model.PartitionListingValue
import com.posthog.hoglake.model.PartitionSpecField
import com.posthog.hoglake.model.PartitionSpecSummary
import com.posthog.hoglake.model.PartitionStatsReport
import com.posthog.hoglake.model.PartitionValue
import com.posthog.hoglake.model.PartitionValueDecoding
import com.posthog.hoglake.service.PartitionListingBackend
import com.posthog.hoglake.service.PartitionListingService
import com.posthog.hoglake.service.PartitionStatsBackend
import com.posthog.hoglake.service.PartitionStatsService

/**
 * The partition surfaces on hoglite (PartitionStatsService and
 * PartitionListingService's port): the same rankings, computed EXACTLY
 * over the tenant's live files at request time.
 *
 * The Postgres services read the MaintenanceSummarySampler's published
 * generation because the debt arithmetic over a 10M-row manifest is
 * what a request path cannot pay on a shared database — the sample is
 * the affordance, the staleness its price. A hoglite tenant is one
 * catalog's own file: this implementation reads its whole live manifest
 * and computes exact results, still subject to the engine's read limits.
 * The API caps below do not make that scan bounded or repair capacity.
 * The wire shape keeps the sampler's fields
 * (`sampledAt` null — "no sample" — with every group present).
 *
 * The DEBT definition ports the planner's, not the sampler's pool
 * arithmetic: a file counts toward debt when the SAME packing the
 * compaction sweep runs ([CompactionGrouping.groups], same min/max,
 * same target) would select it. On Postgres the sampler simulates the
 * pack with a quota ladder; here the pack itself runs — it is the same
 * arithmetic the hoglite planner will act on next sweep, which the
 * sampled figure can only approximate.
 */
internal class HoglitePartitions(
    private val store: HogliteCatalogStore,
    private val smallFileThresholdBytes: Long,
    private val minInputFiles: Int = CompactionGrouping.DEFAULT_MIN_INPUT_FILES,
    private val maxInputFiles: Int = CompactionGrouping.DEFAULT_MAX_INPUT_FILES,
) : PartitionStatsBackend, PartitionListingBackend {
    // ---- the shared read: live files grouped by (table, spec, values) ----

    private data class FileEntry(
        val sizeBytes: Long,
        val recordCount: Long,
        val beginSnapshot: Long,
    )

    private data class Group(
        val tableId: Long,
        val namespace: String,
        val table: String,
        val tableUuid: String,
        val specId: Long?,
        val values: List<String?>?,
        val thresholdBytes: Long,
        val files: MutableList<FileEntry> = mutableListOf(),
        var dvCount: Long = 0,
    ) {
        val fileCount: Long get() = files.size.toLong()
        val smallFiles: List<FileEntry> get() = files.filter { it.sizeBytes < thresholdBytes }
        val smallFileCount: Long get() = smallFiles.size.toLong()
        val totalBytes: Long get() = files.sumOf { it.sizeBytes }
        val smallFileBytes: Long get() = smallFiles.sumOf { it.sizeBytes }
        val recordCount: Long get() = files.sumOf { it.recordCount }
        val newestBeginSnapshot: Long? get() = files.maxOfOrNull { it.beginSnapshot }

        /**
         * Actionable debt: files the planner's pack would select. Runs the
         * same grouping arithmetic HogliteCompaction plans with.
         */
        fun debt(
            threshold: Long,
            minFiles: Int,
            maxFiles: Int,
        ): Long {
            val small = files.filter { it.sizeBytes < threshold }
            if (small.size < minFiles) return 0
            val packed =
                CompactionGrouping.of(threshold).groups(
                    small,
                    minFiles,
                    maxFiles,
                ) { it.sizeBytes }
            return packed.sumOf { it.size }.toLong()
        }
    }

    /** The live-manifest group read for one catalog, optionally one table. */
    private fun groups(
        catalog: String,
        namespace: String?,
        table: String?,
    ): List<Group> {
        val m = store.meta(catalog)
        val nsFilter = if (namespace != null) "AND v.namespace = ?" else ""
        val tFilter = if (table != null) "AND v.name = ?" else ""
        val args =
            buildList {
                if (namespace != null) add(namespace)
                if (table != null) add(table)
            }
        val rows =
            store.queryCatalog(
                catalog,
                "SELECT t.id, v.namespace, v.name, t.table_uuid, f.spec_id, f.partition_values_json, " +
                    "f.file_size_bytes, f.record_count, f.begin_snapshot, " +
                    "(SELECT COUNT(*) FROM dvs d WHERE d.data_file_id = f.id AND d.end_snapshot IS NULL) " +
                    "FROM files f " +
                    "JOIN tables t ON t.id = f.table_id AND t.dropped_snapshot IS NULL " +
                    "JOIN table_versions v ON v.table_id = t.id AND v.end_snapshot IS NULL " +
                    "WHERE f.end_snapshot IS NULL $nsFilter $tFilter",
                *args.toTypedArray(),
            ).rows
        val byKey = linkedMapOf<List<Any?>, Group>()
        for (r in rows) {
            val values =
                (r[5] as? String)?.let {
                    store.json.readValue(it, Array<String?>::class.java).toList()
                }
            val key = listOf(r[0].asLong(), r[4].asLongOrNull(), values)
            val g =
                byKey.getOrPut(key) {
                    Group(
                        tableId = r[0].asLong(),
                        namespace = r[1].asString(),
                        table = r[2].asString(),
                        tableUuid = r[3].asString(),
                        specId = r[4].asLongOrNull(),
                        values = values,
                        thresholdBytes = smallFileThresholdBytes,
                    )
                }
            g.files += FileEntry(r[6].asLong(), r[7].asLong(), r[8].asLong())
            g.dvCount += r[9].asLong()
        }
        return byKey.values.toList()
    }

    /** Every spec vintage, using the live source-column version or its latest retained version. */
    private fun fieldNames(
        catalog: String,
        tableIds: Collection<Long>,
    ): Map<Pair<Long, Long>, List<String>> =
        tableIds.flatMap { tableId ->
            fieldDefs(catalog, tableId).map { (specId, fields) -> (tableId to specId) to fields.map { it.first } }
        }.toMap()

    /** PartitionFieldLabels' live-or-latest rule, including retained parents of dropped nested sources. */
    private fun columnPaths(
        catalog: String,
        tableId: Long,
    ): Map<Long, String> {
        data class Node(val name: String, val parent: Long?)
        val nodes =
            store.queryCatalog(
                catalog,
                "SELECT field_id, name, parent_field_id FROM (" +
                    "SELECT field_id, name, parent_field_id, ROW_NUMBER() OVER (PARTITION BY field_id " +
                    "ORDER BY (end_snapshot IS NULL) DESC, begin_snapshot DESC) AS rank " +
                    "FROM columns WHERE table_id = ?) WHERE rank = 1",
                tableId,
            ).rows.associate { it[0].asLong() to Node(it[1].asString(), it[2].asLongOrNull()) }
        return nodes.mapValues { (fieldId, _) ->
            val segments = mutableListOf<String>()
            var cursor: Long? = fieldId
            var hops = 0
            while (cursor != null && hops <= MAX_COLUMN_NESTING_DEPTH) {
                val node = nodes[cursor] ?: break
                segments += node.name
                cursor = node.parent
                hops++
            }
            segments.reversed().joinToString(".")
        }
    }

    /** The live head spec per table (for stale-spec counting and the listing's summary). */
    private fun headSpec(
        catalog: String,
        tableId: Long,
    ): com.posthog.hoglake.model.PartitionSpec? {
        val head = store.meta(catalog).head
        return store.specAt(catalog, tableId, head)
    }

    // ---- stats: the catalog-wide debt ranking -------------------------------

    override fun partitionStats(
        catalog: String,
        namespace: String?,
        table: String?,
        limit: Int,
    ): PartitionStatsReport =
        store.catalogOperation(catalog) {
            if (table != null && namespace == null) {
                throw HoglakeException.Validation("'table' filter requires 'namespace'")
            }
            if (limit <= 0) {
                throw HoglakeException.Validation("limit must be positive (got $limit)")
            }
            val cappedLimit = limit.coerceAtMost(PartitionStatsService.MAX_LIMIT)
            if (namespace != null) {
                store.liveNamespace(catalog, namespace)
                if (table != null) store.liveTable(catalog, namespace, table)
            }
            val groups = groups(catalog, namespace, table)
            val names = fieldNames(catalog, groups.map { it.tableId }.distinct())
            val headSpecs = groups.map { it.tableId }.distinct().associateWith { headSpec(catalog, it)?.specId }
            val debts =
                groups.map { g ->
                    val fieldNames = g.specId?.let { names[g.tableId to it] } ?: emptyList()
                    PartitionDebt(
                        namespace = g.namespace,
                        table = g.table,
                        tableUuid = java.util.UUID.fromString(g.tableUuid),
                        partitionValues =
                            g.values.orEmpty().mapIndexed { i, value ->
                                PartitionValue(fieldNames.getOrElse(i) { "key_$i" }, value)
                            },
                        specId = g.specId,
                        fileCount = g.fileCount,
                        smallFileCount = g.smallFileCount,
                        totalBytes = g.totalBytes,
                        smallFileBytes = g.smallFileBytes,
                        avgFileBytes = if (g.fileCount > 0) g.totalBytes / g.fileCount else 0,
                        dvCount = g.dvCount,
                        debtScore = g.debt(smallFileThresholdBytes, minInputFiles, maxInputFiles),
                    ) to g.values
                }
            val ordered =
                debts.sortedWith(
                    compareByDescending<Pair<PartitionDebt, List<String?>?>> { it.first.debtScore }
                        .thenByDescending { it.first.smallFileBytes }
                        .thenBy { it.first.namespace }
                        .thenBy { it.first.table }
                        .thenBy { it.first.specId }
                        .thenComparator { a, b -> compareStatsValues(a.second, b.second) },
                )
            return PartitionStatsReport(
                partitions = ordered.take(cappedLimit).map { it.first },
                truncated = ordered.size > cappedLimit,
                staleSpecGroups =
                    groups.count { it.specId != headSpecs[it.tableId] }.toLong(),
                smallFileThresholdBytes = smallFileThresholdBytes,
                sampledAt = null,
            )
        }

    // ---- listing: one table's groups, decoded, filtered, paged --------------

    override fun listPartitions(
        catalog: String,
        namespace: String,
        table: String,
        sort: PartitionListingService.SortColumn,
        desc: Boolean?,
        limit: Int,
        offset: Int,
        filters: List<PartitionListingService.Filter>,
    ): PartitionListing =
        store.catalogOperation(catalog) {
            if (limit <= 0) throw HoglakeException.Validation("limit must be positive (got $limit)")
            if (offset < 0) throw HoglakeException.Validation("offset must be >= 0 (got $offset)")
            val cappedLimit = limit.coerceAtMost(PartitionListingService.MAX_LIMIT)
            val descending = desc ?: sort.descendingByDefault

            val headSpec =
                run {
                    val t = store.liveTable(catalog, namespace, table)
                    headSpec(catalog, t.tableId)
                }
            val arity = headSpec?.fields?.size ?: 0
            filters.firstOrNull { it.keyIndex < 0 || it.keyIndex >= arity }?.let {
                throw HoglakeException.Validation(
                    "partition key_index ${it.keyIndex} is out of range for table " +
                        "'$namespace.$table' ($arity partition keys)",
                )
            }
            val t = store.liveTable(catalog, namespace, table)
            val groups = groups(catalog, namespace, table)
            if (groups.size > PartitionListingService.MAX_GROUPS) {
                throw HoglakeException.Validation(
                    "table '$namespace.$table' has more than ${PartitionListingService.MAX_GROUPS} partition groups; " +
                        "this listing decodes, sorts and pages them in memory and refuses above that. " +
                        "A 'filter' does not help (it is applied after the read). Use " +
                        "GET /v1/catalogs/{catalog}/stats/partitions for the catalog-wide ranking.",
                )
            }
            val names = fieldNames(catalog, listOf(t.tableId))
            // Field labels WITH transforms for decoding (PartitionFieldLabels'
            // Field carries transform + param; rebuild that here — the names
            // map above flattens them away).
            val fieldsBySpec = fieldDefs(catalog, t.tableId)
            val spec =
                headSpec?.let { s ->
                    val fields = fieldsBySpec[s.specId] ?: emptyList()
                    PartitionSpecSummary(
                        specId = s.specId,
                        fields =
                            s.fields.mapIndexed { i, f ->
                                PartitionSpecField(
                                    field = fields.getOrNull(i)?.first ?: "key_$i",
                                    transform = f.transform.wire,
                                    transformParam = f.transformParam,
                                    sourceFieldId = f.sourceFieldId,
                                )
                            },
                    )
                }
            val decoded =
                groups.map { g ->
                    val fields = g.specId?.let { fieldsBySpec[it] } ?: emptyList()
                    val values =
                        g.values.orEmpty().mapIndexed { i, raw ->
                            val f = fields.getOrNull(i)
                            PartitionListingValue(
                                field = f?.first ?: "key_$i",
                                raw = raw,
                                decoded =
                                    PartitionValueDecoding.decode(
                                        f?.second ?: "identity",
                                        raw,
                                        f?.third,
                                    ),
                            )
                        }
                    DecodedGroup(
                        specId = g.specId,
                        group =
                            PartitionGroup(
                                specId = g.specId,
                                values = values,
                                fileCount = g.fileCount,
                                smallFileCount = g.smallFileCount,
                                totalBytes = g.totalBytes,
                                smallFileBytes = g.smallFileBytes,
                                avgFileBytes = if (g.fileCount > 0) g.totalBytes / g.fileCount else 0,
                                dvCount = g.dvCount,
                                debtScore = g.debt(smallFileThresholdBytes, minInputFiles, maxInputFiles),
                                // Always measured here — there is no sample to
                                // be unpublished.
                                recordCount = g.recordCount,
                                lastWrittenSnapshot = g.newestBeginSnapshot,
                            ),
                    )
                }
            val matching =
                decoded.filter { d ->
                    filters.all { f ->
                        val value = d.group.values.getOrNull(f.keyIndex) ?: return@all false
                        if (f.text.isEmpty()) {
                            value.decoded == null
                        } else {
                            value.decoded != null && value.decoded.startsWith(f.text, ignoreCase = true)
                        }
                    }
                }
            val ordered = matching.sortedWith(comparator(sort, descending))
            return PartitionListing(
                sampledAt = null,
                sampleStarted = null,
                sampledSnapshotId = null,
                spec = spec,
                total = matching.size,
                staleSpecGroups = matching.count { it.specId != headSpec?.specId },
                partitions = ordered.drop(offset).take(cappedLimit).map { it.group },
            )
        }

    /** (spec_id -> [(displayName, transform, transformParam)]) for one table — PartitionFieldLabels over the tenant. */
    private fun fieldDefs(
        catalog: String,
        tableId: Long,
    ): Map<Long, List<Triple<String, String, Int?>>> {
        val paths = columnPaths(catalog, tableId)
        val rows =
            store.queryCatalog(
                catalog,
                "SELECT spec_id, fields_json FROM partition_specs WHERE table_id = ?",
                tableId,
            ).rows
        return rows.associate { row ->
            val fields =
                store.json.readValue(
                    row[1].asString(),
                    Array<com.posthog.hoglake.model.PartitionFieldDef>::class.java,
                )
            row[0].asLong() to
                fields.map { f ->
                    val column = paths[f.sourceFieldId] ?: "field_${f.sourceFieldId}"
                    Triple(
                        if (f.transform.wire == "identity") column else "${column}_${f.transform.wire}",
                        f.transform.wire,
                        f.transformParam,
                    )
                }
        }
    }

    private class DecodedGroup(val specId: Long?, val group: PartitionGroup) {
        /** (decoded, raw) per value — the partition sort's key, nulls first. */
        val sortKey: List<Pair<String?, String?>> = group.values.map { it.decoded to it.raw }
    }

    /** The ordering, PartitionListingService.comparator verbatim. */
    private fun comparator(
        sort: PartitionListingService.SortColumn,
        descending: Boolean,
    ): Comparator<DecodedGroup> {
        val tuple = Comparator<DecodedGroup> { a, b -> compareTuples(a.sortKey, b.sortKey) }
        if (sort == PartitionListingService.SortColumn.PARTITION) {
            return if (descending) tuple.reversed() else tuple
        }
        val measure =
            Comparator<DecodedGroup> { a, b ->
                val x = measureOf(sort, a) ?: 0L
                val y = measureOf(sort, b) ?: 0L
                if (descending) y.compareTo(x) else x.compareTo(y)
            }
        return measure.thenComparing(tuple)
    }

    private fun measureOf(
        sort: PartitionListingService.SortColumn,
        d: DecodedGroup,
    ): Long? =
        when (sort) {
            PartitionListingService.SortColumn.PARTITION -> null
            PartitionListingService.SortColumn.FILES -> d.group.fileCount
            PartitionListingService.SortColumn.SMALL_FILES -> d.group.smallFileCount
            PartitionListingService.SortColumn.DEBT -> d.group.debtScore
            PartitionListingService.SortColumn.TOTAL_SIZE -> d.group.totalBytes
            PartitionListingService.SortColumn.AVG_SIZE -> d.group.avgFileBytes
            PartitionListingService.SortColumn.DVS -> d.group.dvCount
            PartitionListingService.SortColumn.ROWS -> d.group.recordCount
            PartitionListingService.SortColumn.LAST_WRITTEN -> d.group.lastWrittenSnapshot
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
        if (a == null && b == null) {
            0
        } else if (a == null) {
            -1
        } else if (b == null) {
            1
        } else {
            a.compareTo(b)
        }

    /**
     * PG's one-dimensional text[] order: whole NULL and null elements LAST,
     * shorter equal prefixes FIRST. Keep HG's deterministic text comparison;
     * PG's configured collation may differ outside the characterized fixtures.
     * This is separate from LIST's decoded/raw, nulls-FIRST comparator above.
     */
    private fun compareStatsValues(
        a: List<String?>?,
        b: List<String?>?,
    ): Int {
        if (a === b) return 0
        if (a == null) return 1
        if (b == null) return -1
        for (i in 0 until minOf(a.size, b.size)) {
            val x = a[i]
            val y = b[i]
            val c =
                when {
                    x == null && y == null -> 0
                    x == null -> 1
                    y == null -> -1
                    else -> x.compareTo(y)
                }
            if (c != 0) return c
        }
        return a.size.compareTo(b.size)
    }
}
