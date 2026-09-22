package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.LongNode
import com.fasterxml.jackson.databind.node.NullNode
import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.ChangesPlan
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.ConsumerOffset
import com.posthog.hoglake.model.DataFile
import com.posthog.hoglake.model.DeleteFile
import com.posthog.hoglake.model.DeleteFileRegistration
import com.posthog.hoglake.model.FileColumnStats
import com.posthog.hoglake.model.FileOrderingBounds
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.FileStats
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceInfo
import com.posthog.hoglake.model.ScanFile
import com.posthog.hoglake.model.Snapshot
import com.posthog.hoglake.model.StatsState
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.model.TableDeletes
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.model.ViewInfo
import com.posthog.hoglake.observability.CatalogTotals
import com.posthog.hoglake.stats.BoundWire
import java.time.Instant
import java.util.UUID

/*
 * Wire DTOs mirroring the OpenAPI schemas (openapi/hoglake.yaml).
 *
 * Naming: Kotlin camelCase properties; the ObjectMapper installed in
 * App.module uses PropertyNamingStrategies.SNAKE_CASE, so the wire is
 * snake_case exactly as the spec spells it (head_snapshot_id, ...).
 *
 * Enums travel as their wire strings (ColType.wire / StatsState.wire /
 * ChangeKind.wire), never as Kotlin enum names. lower_bound/upper_bound
 * are `format: byte` — Jackson's native ByteArray <-> base64 mapping.
 */

// ---- errors --------------------------------------------------------------

data class ApiErrorDto(val error: String, val detail: String? = null)

// ---- catalogs ------------------------------------------------------------

data class CatalogDto(
    val name: String,
    val dataPath: String,
    val headSnapshotId: Long,
    val schemaVersion: Long,
    /** Expiry-floor snapshot's time; NON_NULL omits it until expiry first advances the floor. */
    val earliestSnapshotTime: Instant? = null,
    val capabilities: List<String> =
        listOf(
            "atomic-table-creation-v1",
            "idempotent-append-v1",
            "idempotent-delete-v1",
            "idempotent-mutation-v1",
            "guarded-table-lifecycle-v1",
            "atomic-table-replacement-v1",
            "guarded-schema-evolution-v1",
            "recursive-write-schema-v1",
            "atomic-partitioned-table-creation-v1",
            "atomic-sorted-table-creation-v1",
            "versioned-table-metadata-v1",
        ),
    /**
     * Live totals from the metrics sampler's last pass — display
     * numbers, not a consistency primitive, and the same provenance as
     * the instance totals on GET /v1/info.
     *
     * All three are OMITTED rather than zeroed when the catalog has not
     * been sampled yet (the boot window, or a catalog created since the
     * last pass): an unsampled catalog and an empty one are different
     * facts, and zero would assert the wrong one. table_count counts
     * live tables; live_rows is gross of deletion-vector masking, as
     * /v1/info's totals are.
     */
    val tableCount: Long? = null,
    val liveRows: Long? = null,
    val liveSizeBytes: Long? = null,
    /**
     * Commit time of the oldest RETAINED snapshot, from the same sample
     * as the totals above (so OMITTED, not zeroed, until first sampled).
     * Distinct from [earliestSnapshotTime]: that is the expiry floor and
     * stays null until expiry advances it, which would read a never-
     * expired catalog — the one that retains its FIRST snapshot — as
     * having none. This is MIN(snapshot_time) and is right either way.
     * An instant, not an age: the client renders "3 days ago" live.
     */
    val oldestSnapshotTime: Instant? = null,
)

fun CatalogInfo.toDto(totals: CatalogTotals? = null) =
    CatalogDto(
        name = name,
        dataPath = dataPath,
        headSnapshotId = headSnapshotId,
        schemaVersion = schemaVersion,
        earliestSnapshotTime = earliestSnapshotTime,
        tableCount = totals?.tableCount,
        liveRows = totals?.liveRows,
        liveSizeBytes = totals?.liveBytes,
        oldestSnapshotTime = totals?.oldestSnapshotTime,
    )

data class CreateCatalogRequestDto(val name: String, val dataPath: String)

// ---- namespaces ----------------------------------------------------------

data class NamespaceDto(val name: String, val namespaceId: Long)

fun NamespaceInfo.toDto() = NamespaceDto(name, namespaceId)

data class CreateNamespaceRequestDto(val name: String)

// ---- tables --------------------------------------------------------------

data class ColumnDefDto(
    val name: String,
    val type: String,
    val typeParams: Map<String, Any?>? = null,
    val nullable: Boolean = true,
    /**
     * Children of a container type (list/struct/map). Recursive, and
     * bounded by the depth cap the service enforces
     * ([com.posthog.hoglake.model.MAX_COLUMN_NESTING_DEPTH]) — Jackson
     * itself will refuse a pathologically deep body first
     * (StreamReadConstraints), which is a 400 rather than a stack
     * overflow.
     */
    val children: List<ColumnDefDto>? = null,
    val comment: String? = null,
) {
    fun toModel(): ColumnDef =
        ColumnDef(
            name = name,
            // parseWire, not fromWire: a permanently unsupported DuckLake
            // type name gets a 422 that names the type and says WHY, so a
            // client stops trying instead of hunting for a spelling.
            type = ColType.parseWire(type) { "unknown column type '$type' for column '$name'" },
            typeParams = typeParams,
            nullable = nullable,
            children = children?.map { it.toModel() },
            comment = comment,
        )
}

data class CreateTableRequestDto(val name: String, val columns: List<ColumnDefDto>)

data class ColumnDto(
    val name: String,
    val type: String,
    val typeParams: Map<String, Any?>?,
    val nullable: Boolean,
    val fieldId: Long,
    val ordinal: Int,
    /**
     * Children of a container type, with their assigned field ids;
     * NON_NULL omits it entirely for a scalar column, so a pre-phase-2
     * client sees the shape it always saw.
     */
    val children: List<ColumnDto>? = null,
    val comment: String? = null,
)

fun Column.toDto(): ColumnDto =
    ColumnDto(
        name = def.name,
        comment = def.comment,
        type = def.type.wire,
        typeParams = def.typeParams,
        nullable = def.nullable,
        fieldId = fieldId,
        ordinal = ordinal,
        children = if (def.type.isNested) children.map { it.toDto() } else null,
    )

data class TableSummaryDto(val name: String, val tableUuid: UUID)

fun TableInfo.toSummaryDto() = TableSummaryDto(name, tableUuid)

data class TableDto(
    val name: String,
    val namespace: String,
    val tableUuid: UUID,
    val columns: List<ColumnDto>,
    val recordCount: Long,
    val fileCount: Long,
    val fileSizeBytes: Long,
    /**
     * The spec visible at the requested snapshot (AlterDto's shape);
     * NON_NULL omits it for an unpartitioned table.
     */
    val partitionSpec: AlterPartitionSpecDto? = null,
    /** Sort order at the requested snapshot; NON_NULL omits when unsorted. */
    val sortSpec: AlterSortSpecDto? = null,
    val comment: String? = null,
    val properties: Map<String, String> = emptyMap(),
)

fun TableInfo.toDto() =
    TableDto(
        name = name,
        namespace = namespace,
        tableUuid = tableUuid,
        columns = columns.map { it.toDto() },
        recordCount = recordCount,
        fileCount = fileCount,
        fileSizeBytes = fileSizeBytes,
        partitionSpec = partitionSpec?.toAlterDto(),
        sortSpec = sortSpec?.toAlterDto(),
        comment = comment,
        properties = properties,
    )

// ---- commits -------------------------------------------------------------

data class ColumnStatsDto(
    val fieldId: Long,
    val valueCount: Long,
    val nullCount: Long,
    val nanCount: Long? = null,
    val sizeBytes: Long? = null,
    val lowerBound: ByteArray? = null,
    val upperBound: ByteArray? = null,
) {
    fun toModel() =
        ColumnStats(
            fieldId = fieldId,
            valueCount = valueCount,
            nullCount = nullCount,
            nanCount = nanCount,
            sizeBytes = sizeBytes,
            lowerBound = lowerBound,
            upperBound = upperBound,
        )

    // ByteArray members: identity equals/hashCode are fine — DTOs are
    // one-way carriers, never compared.
}

data class FileRegistrationDto(
    val path: String,
    val recordCount: Long,
    val fileSizeBytes: Long,
    val footerSize: Long? = null,
    val columnStats: List<ColumnStatsDto>? = null,
    val partitionValues: List<String?>? = null,
) {
    fun toModel() =
        FileRegistration(
            path = path,
            recordCount = recordCount,
            fileSizeBytes = fileSizeBytes,
            footerSize = footerSize,
            columnStats = columnStats?.map { it.toModel() },
            partitionValues = partitionValues,
        )
}

data class TableAppendDto(
    val namespace: String,
    val table: String,
    val files: List<FileRegistrationDto>,
    val expectedTableUuid: UUID? = null,
) {
    fun toModel(): TableAppend {
        if (files.isEmpty()) {
            throw HoglakeException.Validation("append to $namespace.$table has no files")
        }
        return TableAppend(namespace, table, files.map { it.toModel() }, expectedTableUuid)
    }
}

data class DeleteFileRegistrationDto(
    val dataFileId: Long,
    val path: String,
    val deleteCount: Long,
    val fileSizeBytes: Long,
) {
    fun toModel() =
        DeleteFileRegistration(
            dataFileId = dataFileId,
            path = path,
            deleteCount = deleteCount,
            fileSizeBytes = fileSizeBytes,
        )
}

data class TableDeletesDto(
    val namespace: String,
    val table: String,
    val files: List<DeleteFileRegistrationDto>,
    val expectedTableUuid: UUID? = null,
) {
    fun toModel(allowEmpty: Boolean = false): TableDeletes {
        if (files.isEmpty() && !allowEmpty) {
            throw HoglakeException.Validation("deletes for $namespace.$table have no files")
        }
        return TableDeletes(namespace, table, files.map { it.toModel() }, expectedTableUuid)
    }
}

data class CommitRequestDto(
    val readSnapshot: Long? = null,
    // Both default empty per the spec: at least one must be non-empty,
    // which CommitService enforces (422 validation, not a 400).
    val appends: List<TableAppendDto> = emptyList(),
    val deletes: List<TableDeletesDto> = emptyList(),
    val author: String? = null,
    val message: String? = null,
    val idempotencyKey: UUID? = null,
) {
    fun toModel(allowEmptyDeletes: Boolean = false) =
        CommitRequest(
            readSnapshot = readSnapshot,
            appends = appends.map { it.toModel() },
            deletes = deletes.map { it.toModel(allowEmptyDeletes) },
            author = author,
            message = message,
            idempotencyKey = idempotencyKey,
        )
}

data class CommitReceiptDto(val operationId: UUID, val snapshotId: Long, val schemaVersion: Long)

data class CommitResultDto(val snapshotId: Long, val schemaVersion: Long)

fun CommitResult.toDto() = CommitResultDto(snapshotId, schemaVersion)

// ---- files + changefeed --------------------------------------------------

data class DataFileDto(
    val dataFileId: Long,
    val path: String,
    val fileFormat: String,
    val recordCount: Long,
    val fileSizeBytes: Long,
    val footerSize: Long?,
    val rowIdStart: Long,
    val statsState: String,
    val beginSnapshot: Long,
    // Partitioning binding; NON_NULL inclusion drops both when absent
    // (unpartitioned file, or a read path that does not load them).
    val specId: Long? = null,
    val partitionValues: List<String?>? = null,
    /** True for compaction outputs: row ids ride the physical _hog_row_id column. */
    val explicitRowIds: Boolean = false,
    /**
     * The file's ordering-key range; the files LISTING fills it, the
     * changefeed and the scan plan leave it absent (NON_NULL omits it).
     */
    val orderingBounds: FileOrderingBoundsDto? = null,
)

/**
 * A file's range along the key its table is ordered by (GET
 * .../files). [fieldId] names the sort-spec field the bounds belong to;
 * ABSENT means the table is unsorted and the bounds are the file's
 * row-id span, the implicit ordering key — the row id is not a catalog
 * column and has no field id to give.
 *
 * `lower_bound`/`upper_bound` are typed [JsonNode] for the same reason
 * [FileColumnStatsDto]'s are: a JSON null is an ANSWER here, not an
 * absence. On a sort key it is "no bound stored, so do not prune"; on a
 * row-id span it is "unknown", which a compaction output's maximum
 * genuinely is. NON_NULL inclusion would silently drop either, and the
 * reader would see a range with one end missing rather than a stated
 * one.
 */
data class FileOrderingBoundsDto(
    val fieldId: Long? = null,
    val lowerBound: JsonNode,
    val upperBound: JsonNode,
)

fun FileOrderingBounds.toDto(): FileOrderingBoundsDto =
    when (this) {
        is FileOrderingBounds.SortKey ->
            FileOrderingBoundsDto(
                fieldId = column.fieldId,
                lowerBound = renderBoundOrNull(column, column.stats.lowerBound),
                upperBound = renderBoundOrNull(column, column.stats.upperBound),
            )
        is FileOrderingBounds.RowIds ->
            FileOrderingBoundsDto(
                lowerBound = LongNode.valueOf(lower),
                upperBound = upper?.let { LongNode.valueOf(it) } ?: NullNode.instance,
            )
    }

fun DataFile.toDto() =
    DataFileDto(
        dataFileId = dataFileId,
        path = path,
        fileFormat = fileFormat,
        recordCount = recordCount,
        fileSizeBytes = fileSizeBytes,
        footerSize = footerSize,
        rowIdStart = rowIdStart,
        statsState = statsState.wire,
        beginSnapshot = beginSnapshot,
        specId = specId,
        partitionValues = partitionValues,
        explicitRowIds = explicitRowIds,
        orderingBounds = orderingBounds?.toDto(),
    )

// ---- per-file column statistics (decoded bounds) ---------------------------

/**
 * GET .../files/{fileId}/stats — one stats row with its bounds DECODED
 * to the JSON wire conventions (stats/BoundWire; documented on the
 * spec's FileColumnStats schema). `lower`/`upper` are typed [JsonNode]
 * rather than Kotlin nullables so an all-null column's stored NULL
 * bound serializes as an EXPLICIT JSON null (the global NON_NULL
 * inclusion would silently omit a null property — and "null bound" is
 * an answer, not an absence).
 */
data class FileColumnStatsDto(
    val fieldId: Long,
    val name: String,
    val path: String,
    val type: String,
    val typeParams: Map<String, Any?>? = null,
    val valueCount: Long,
    val nullCount: Long,
    val nanCount: Long? = null,
    val sizeBytes: Long? = null,
    val lowerBound: JsonNode,
    val upperBound: JsonNode,
)

data class FileStatsDto(
    val dataFileId: Long,
    val statsState: String,
    val columns: List<FileColumnStatsDto>,
    /** Present iff the file has no stats rows (stats_state != provided). */
    val noStatsReason: String? = null,
)

fun FileStats.toDto(): FileStatsDto =
    FileStatsDto(
        dataFileId = dataFileId,
        statsState = statsState.wire,
        columns = columns.map { it.toDto() },
        noStatsReason =
            when (statsState) {
                StatsState.PROVIDED -> null
                StatsState.PENDING ->
                    "stats_state is 'pending': column statistics have not been hydrated yet, " +
                        "so no per-column rows exist; with no bounds, callers must not prune " +
                        "this file"
                StatsState.FAILED ->
                    "stats_state is 'failed': stats hydration failed structurally " +
                        "(see POST .../maintenance/rehydrate), so no per-column rows exist; " +
                        "with no bounds, callers must not prune this file"
            },
    )

fun FileColumnStats.toDto(): FileColumnStatsDto =
    FileColumnStatsDto(
        fieldId = fieldId,
        name = name,
        path = path,
        type = type.wire,
        typeParams = typeParams,
        valueCount = stats.valueCount,
        nullCount = stats.nullCount,
        nanCount = stats.nanCount,
        sizeBytes = stats.sizeBytes,
        lowerBound = renderBoundOrNull(this, stats.lowerBound),
        upperBound = renderBoundOrNull(this, stats.upperBound),
    )

/**
 * A stored bound as wire JSON, or JSON null when there is none — and
 * ALSO null when the stored bytes cannot be decoded/rendered under the
 * column's live type (a stale-width bound from a pre-promote writer,
 * an out-of-domain value): the "NULL, never guessed" read-side dual. A
 * bound the reader cannot decode is treated as absent, and the spec
 * says absent bounds mean "do not prune". Never a 500: this endpoint
 * reports the store, it does not vouch for it.
 */
private fun renderBoundOrNull(
    column: FileColumnStats,
    bytes: ByteArray?,
): JsonNode {
    if (bytes == null) return NullNode.instance
    return try {
        BoundWire.render(
            column.type,
            BoundWire.scaleOf(column.typeParams),
            bytes,
        )
    } catch (_: IllegalArgumentException) {
        NullNode.instance
    }
}

// ---- scan planning -------------------------------------------------------

data class DeleteFileDto(
    val deleteFileId: Long,
    val dataFileId: Long,
    val path: String,
    val fileFormat: String,
    val deleteCount: Long,
    val fileSizeBytes: Long,
    val beginSnapshot: Long,
)

fun DeleteFile.toDto() =
    DeleteFileDto(
        deleteFileId = deleteFileId,
        dataFileId = dataFileId,
        path = path,
        fileFormat = fileFormat,
        deleteCount = deleteCount,
        fileSizeBytes = fileSizeBytes,
        beginSnapshot = beginSnapshot,
    )

data class ScanFileDto(
    val dataFile: DataFileDto,
    val deleteFile: DeleteFileDto? = null,
)

fun ScanFile.toDto() =
    ScanFileDto(
        dataFile = dataFile.toDto(),
        deleteFile = deleteFile?.toDto(),
    )

data class ChangePlanDto(
    val tableUuid: UUID,
    val fromSnapshot: Long,
    val toSnapshot: Long,
    val files: List<DataFileDto>,
    /** DVs registered in (from, to] — the deletions feed. */
    val deleteFiles: List<DeleteFileDto>,
)

fun ChangesPlan.toDto() =
    ChangePlanDto(
        tableUuid = tableUuid,
        fromSnapshot = fromSnapshot,
        toSnapshot = toSnapshot,
        files = files.map { it.toDto() },
        deleteFiles = deleteFiles.map { it.toDto() },
    )

// ---- views ---------------------------------------------------------------

data class ViewDto(
    val name: String,
    val namespace: String,
    val viewUuid: UUID,
    val dialect: String,
    val sql: String,
)

fun ViewInfo.toDto() =
    ViewDto(
        name = name,
        namespace = namespace,
        viewUuid = viewUuid,
        dialect = dialect,
        sql = sql,
    )

data class CreateViewRequestDto(
    val name: String,
    /** Stored verbatim — the server never parses view SQL. */
    val sql: String,
    val dialect: String = "trino",
)

// ---- snapshots -----------------------------------------------------------

data class SnapshotChangeDto(val kind: String, val objectId: Long)

data class SnapshotDto(
    val snapshotId: Long,
    val snapshotTime: Instant,
    val schemaVersion: Long,
    val author: String?,
    val message: String?,
    val changes: List<SnapshotChangeDto>,
)

fun Snapshot.toDto() =
    SnapshotDto(
        snapshotId = snapshotId,
        snapshotTime = snapshotTime,
        schemaVersion = schemaVersion,
        author = author,
        message = message,
        changes = changes.map { SnapshotChangeDto(it.kind.wire, it.objectId) },
    )

data class SnapshotPageDto(val snapshots: List<SnapshotDto>, val hasMore: Boolean)

// ---- consumer offsets ----------------------------------------------------

data class ConsumerOffsetDto(
    val consumerId: String,
    val tableUuid: UUID,
    val committedSnapshot: Long,
    val updatedAt: Instant,
)

fun ConsumerOffset.toDto() = ConsumerOffsetDto(consumerId, tableUuid, committedSnapshot, updatedAt)

data class CommitOffsetRequestDto(val snapshotId: Long)

/**
 * GET /v1/info — instance identity plus live-data totals for the webui
 * header. Name omitted when unset; totals come from the metrics
 * sampler's last pass and are omitted in the boot window before it.
 * Version is the running server's own, always present (BuildInfo falls
 * back to "unknown" rather than omitting it — "which version is this?"
 * having no answer is itself the answer an operator needs).
 *
 * Build is the packaging stamp and is omitted on any build nobody
 * stamped — every local build, every PR image. Absent means "not off
 * the pipeline", which is why it is a separate optional field and not
 * folded into `version`: `version` is the contract version the spec is
 * gated against, and it must keep meaning exactly that.
 */
data class InstanceInfoDto(
    val name: String?,
    val version: String,
    val build: String?,
    val totalRows: Long?,
    val totalSizeBytes: Long?,
)

/** One row of the catalog-wide consumer listing (GET /consumers). */
data class ConsumerTableOffsetDto(
    val tableUuid: UUID,
    val committedSnapshot: Long,
    val updatedAt: Instant,
    val namespace: String?,
    val tableName: String?,
    val tableDropped: Boolean,
)

data class ConsumerSummaryDto(
    val consumerId: String,
    val offsets: List<ConsumerTableOffsetDto>,
)

data class ConsumerListDto(val consumers: List<ConsumerSummaryDto>)

/** Group flat repo rows by consumer, preserving the repo's ordering. */
fun List<com.posthog.hoglake.model.ConsumerTableOffset>.toConsumerListDto() =
    ConsumerListDto(
        groupBy { it.consumerId }
            .map { (consumer, rows) ->
                ConsumerSummaryDto(
                    consumerId = consumer,
                    offsets =
                        rows.map {
                            ConsumerTableOffsetDto(
                                tableUuid = it.tableUuid,
                                committedSnapshot = it.committedSnapshot,
                                updatedAt = it.updatedAt,
                                namespace = it.namespace,
                                tableName = it.tableName,
                                tableDropped = it.tableDropped,
                            )
                        },
                )
            },
    )
