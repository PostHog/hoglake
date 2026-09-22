package com.posthog.hoglake.service

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.initialColumns
import com.posthog.hoglake.model.validateFooterSize
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.Locks
import com.posthog.hoglake.persistence.NamespaceRepo
import com.posthog.hoglake.persistence.Pg
import com.posthog.hoglake.persistence.TableRepo
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
import java.time.Instant
import java.util.UUID

data class ReplacementTarget(val expectedTableUuid: UUID?, val readSnapshot: Long)

data class TableCreationDefinition(
    val namespace: String,
    val name: String,
    val columns: List<ColumnDef>,
    val replacement: ReplacementTarget? = null,
    val partitionFields: List<PartitionFieldDef> = emptyList(),
    val sortFields: List<SortFieldDef> = emptyList(),
    val comment: String? = null,
    val properties: Map<String, String> = emptyMap(),
)

data class TableCreation(
    val operationId: UUID,
    val tableUuid: UUID,
    val definition: TableCreationDefinition,
    val columns: List<Column>,
    val writePath: String,
    val state: String,
    val expiresAt: Instant,
    val snapshotId: Long?,
    val schemaVersion: Long?,
    val reason: String?,
)

/**
 * Unpublished definitions plus permanent publication receipts. Every state transition takes
 * an operation row lock; publication takes the catalog lock first. A prepared status is never a cleanup permit:
 * only a terminal rejected/aborted receipt fences an in-flight publication.
 */
class TableCreationService(
    private val jdbi: Jdbi,
    private val catalogs: CatalogService,
    private val commits: CommitService,
    private val lockTimeoutMs: Long = CommitService.DEFAULT_COMMIT_LOCK_TIMEOUT_MS,
) {
    private val log = io.github.oshai.kotlinlogging.KotlinLogging.logger {}

    private val mapper =
        jacksonObjectMapper().findAndRegisterModules().enable(
            SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS,
        )

    fun prepare(
        catalog: String,
        operationId: UUID,
        definition: TableCreationDefinition,
    ): TableCreation {
        var replay = false
        return observed("prepare", catalog, operationId, { if (replay) "replayed" else it.state }) {
            operationTransaction { h ->
                val cat = CatalogRepo.require(h, catalog)
                val encoded = TableCreationDefinitionCodec.encode(definition)
                if (exists(h, cat.catalogId, operationId)) {
                    replay = true
                    requireSame(h, cat.catalogId, operationId, "definition", encoded)
                    return@operationTransaction load(h, cat.catalogId, operationId)
                }
                // The node cap lives in ColumnTrees.validate, which
                // this reaches through validateTableDefinition — so
                // prepare, plain createTable and add_column all enforce
                // the same one. It used to live here alone, which capped
                // the ONE path that had it and left the others building
                // the forest prepare refused.
                TableMetadata.validateComment(definition.comment)
                TableMetadata.validateProperties(definition.properties)
                catalogs.validateTableDefinition(definition.name, definition.columns)
                AlterService(
                    jdbi,
                ).validatePartitionFields(initialColumns(definition.columns), definition.partitionFields)
                AlterService(jdbi).validateSortFields(initialColumns(definition.columns), definition.sortFields)
                Identifiers.validate("namespace", definition.namespace)
                val ns =
                    NamespaceRepo.findLiveByName(h, cat.catalogId, definition.namespace)
                        ?: throw HoglakeException.NotFound("namespace '${definition.namespace}'")
                definition.replacement?.let {
                    if (it.readSnapshot < cat.earliestSnapshotId || it.readSnapshot > cat.headSnapshotId) {
                        throw HoglakeException.CommitConflict("replacement read snapshot is not retained")
                    }
                    val target = TableRepo.findAt(h, cat.catalogId, ns.namespaceId, definition.name, it.readSnapshot)
                    if (target?.tableUuid != it.expectedTableUuid) {
                        throw HoglakeException.CommitConflict("replacement target does not match read snapshot")
                    }
                }
                // Prefix includes a server-generated identity: even a future catalog recreation
                // and reused client operation id cannot reuse old object paths.
                val uuid = UUID.randomUUID()
                val inserted =
                    h.createUpdate(
                        """
                INSERT INTO hog_table_creation (catalog_id, operation_id, namespace_id, definition, table_uuid, write_path)
                VALUES (:catalog, :operation, :namespace, CAST(:definition AS jsonb), :uuid, :path)
                ON CONFLICT (catalog_id, operation_id) DO NOTHING
                """,
                    ).bind("catalog", cat.catalogId).bind("operation", operationId)
                        .bind("namespace", ns.namespaceId).bind("definition", encoded).bind("uuid", uuid)
                        .bind("path", cat.dataPath.trimEnd('/') + "/data/$uuid/").execute()
                replay = inserted == 0
                requireSame(h, cat.catalogId, operationId, "definition", encoded)
                load(h, cat.catalogId, operationId)
            }
        }
    }

    fun status(
        catalog: String,
        operationId: UUID,
    ): TableCreation =
        operationTransaction { h ->
            val cat = CatalogRepo.require(h, catalog)
            load(h, cat.catalogId, operationId)
        }

    fun abort(
        catalog: String,
        operationId: UUID,
    ): TableCreation {
        var replay = false
        return observed("abort", catalog, operationId, { if (replay) "replayed" else it.state }) {
            operationTransaction { h ->
                val cat = CatalogRepo.require(h, catalog)
                val operation = load(h, cat.catalogId, operationId)
                replay = operation.state != "prepared"
                if (operation.state == "prepared") transition(h, cat.catalogId, operationId, "aborted", "client_abort")
                load(h, cat.catalogId, operationId)
            }
        }
    }

    fun publish(
        catalog: String,
        operationId: UUID,
        files: List<FileRegistration>,
    ): TableCreation {
        var replay = false
        return observed("publish", catalog, operationId, { if (replay) "replayed" else it.state }) {
            operationTransaction { h ->
                val catalogId = CatalogRepo.require(h, catalog).catalogId
                Locks.acquireCatalogCommitLock(h, catalogId, lockTimeoutMs)
                val cat = CatalogRepo.require(h, catalog)
                val operation = load(h, cat.catalogId, operationId)
                replay = operation.state != "prepared"
                val encoded = mapper.writeValueAsString(files)
                if (operation.state == "committed" || operation.state == "rejected") {
                    requireSame(h, cat.catalogId, operationId, "files", encoded)
                    return@operationTransaction operation
                }
                if (operation.state == "aborted") return@operationTransaction operation
                if (files.size > 10000) throw HoglakeException.Validation("too many files")
                if (files.map { it.path }.toSet().size != files.size) {
                    throw HoglakeException.Validation(
                        "duplicate file paths",
                    )
                }
                files.forEach {
                    if (!it.path.startsWith(
                            operation.writePath,
                        )
                    ) {
                        throw HoglakeException.Validation("file outside operation write_path")
                    }
                    it.validateFooterSize(required = true)
                }
                h.createUpdate(
                    """
                UPDATE hog_table_creation SET files = CAST(:files AS jsonb) WHERE catalog_id = :catalog AND
                operation_id = :operation
                """,
                )
                    .bind("files", encoded).bind("catalog", cat.catalogId).bind("operation", operationId).execute()
                val definition = operation.definition
                val ns = NamespaceRepo.findLiveByName(h, cat.catalogId, definition.namespace)
                val originalNamespace =
                    h.createQuery(
                        """
                    SELECT namespace_id FROM hog_table_creation
                    WHERE catalog_id = :catalog AND operation_id = :operation
                    """,
                    )
                        .bind("catalog", cat.catalogId).bind("operation", operationId).mapTo(Long::class.java).one()
                val target = ns?.let { TableRepo.findLive(h, cat.catalogId, it.namespaceId, definition.name) }
                val replacement = definition.replacement
                val targetModified =
                    replacement != null && target != null &&
                        h.createQuery(
                            """
                        SELECT EXISTS (SELECT 1 FROM hog_snapshot_change
                        WHERE catalog_id = :catalog AND object_id = :table
                          AND snapshot_id > :snapshot AND kind LIKE 'table_%')
                        """,
                        ).bind("catalog", cat.catalogId).bind("table", target.tableId)
                            .bind("snapshot", replacement.readSnapshot).mapTo(Boolean::class.java).one()
                val targetChanged =
                    replacement != null && (
                        replacement.readSnapshot < cat.earliestSnapshotId ||
                            target?.tableUuid != replacement.expectedTableUuid || targetModified
                    )
                if (ns == null || ns.namespaceId != originalNamespace) {
                    transition(h, cat.catalogId, operationId, "rejected", "namespace_changed")
                } else if (targetChanged) {
                    transition(h, cat.catalogId, operationId, "rejected", "target_changed")
                } else if (replacement == null && target != null) {
                    transition(h, cat.catalogId, operationId, "rejected", "target_exists")
                } else {
                    // A receipt PREPARED under an older, laxer rule set
                    // can hold a definition this server now refuses — a
                    // reserved `_hog` name, a forest past the node cap, a
                    // shape a later check tightened. Letting the
                    // Validation escape rolls the transaction back and
                    // leaves the operation `prepared` FOREVER: every
                    // retry re-validates and re-fails, and the only exit
                    // is an explicit abort the client has no reason to
                    // send, since from its side publish is just 422-ing.
                    //
                    // `rejected` is the terminal state this belongs in.
                    // It is the same shape as target_exists and
                    // namespace_changed: the receipt was honestly
                    // prepared and is no longer publishable, through no
                    // fault of THIS call.
                    //
                    // Scoped to createTable deliberately. The file checks
                    // below are about the arguments of this call, and a
                    // client that posts a blank path deserves a 422 it
                    // can fix by posting again — not a dead receipt.
                    val table =
                        try {
                            catalogs.createTable(
                                h,
                                catalog,
                                definition.namespace,
                                definition.name,
                                definition.columns,
                                operation.tableUuid,
                                replacementTableId = target?.tableId,
                                partitionFields = definition.partitionFields,
                                sortFields = definition.sortFields,
                                comment = definition.comment,
                                properties = definition.properties,
                            )
                        } catch (e: HoglakeException.Validation) {
                            log.warn {
                                "table creation $operationId in $catalog was prepared with a " +
                                    "definition this server refuses (${e.message}); rejecting the " +
                                    "receipt rather than leaving it prepared forever"
                            }
                            transition(h, cat.catalogId, operationId, "rejected", "definition_invalid")
                            return@operationTransaction load(h, cat.catalogId, operationId)
                        }
                    check(table.columns == operation.columns)
                    val published = CatalogRepo.require(h, catalog)
                    commits.registerInitialFiles(
                        h,
                        cat.catalogId,
                        cat.dataPath,
                        definition.namespace,
                        definition.name,
                        table.tableId,
                        published.headSnapshotId,
                        files,
                        operationId,
                    )
                    h.createUpdate(
                        """
                    UPDATE hog_table_creation SET state = 'committed', snapshot_id = :snapshot, schema_version = :version
                    WHERE catalog_id = :catalog AND operation_id = :operation
                    """,
                    ).bind("snapshot", published.headSnapshotId).bind("version", published.schemaVersion)
                        .bind("catalog", cat.catalogId).bind("operation", operationId).execute()
                }
                load(h, cat.catalogId, operationId)
            }
        }
    }

    private fun observed(
        action: String,
        catalog: String,
        operation: UUID,
        outcome: (TableCreation) -> String,
        block: () -> TableCreation,
    ): TableCreation =
        Audit.audited(
            "table_creation_$action",
            catalog,
            operation.toString(),
            successOutcome = outcome,
            detail = { "operation=$operation state=${it.state} snapshot=${it.snapshotId}" },
        ) {
            // The block returns only after its database transaction commits or rolls back.
            val result =
                try {
                    block()
                } catch (e: Throwable) {
                    Metrics.tableCreationRecorded(catalog, action, Audit.failureOutcome(e))
                    throw e
                }
            val status = outcome(result)
            Metrics.tableCreationRecorded(catalog, action, status)
            if (action == "publish" && status == "committed") Metrics.commitRecorded(catalog, "committed")
            result
        }

    private fun <T> operationTransaction(block: (Handle) -> T): T =
        try {
            jdbi.inTransactionUnchecked { h ->
                if (lockTimeoutMs > 0) {
                    h.createQuery("SELECT set_config('lock_timeout', :timeout, true)")
                        .bind("timeout", lockTimeoutMs.toString()).mapTo(String::class.java).one()
                }
                block(h)
            }
        } catch (e: UnableToExecuteStatementException) {
            if (Pg.isLockTimeout(e)) {
                throw HoglakeException.CommitQueueTimeout("table creation lock timed out after ${lockTimeoutMs}ms")
            }
            throw e
        }

    private fun exists(
        h: Handle,
        catalog: Long,
        operation: UUID,
    ): Boolean =
        h.createQuery(
            """
            SELECT count(*) FROM hog_table_creation
            WHERE catalog_id = :catalog AND operation_id = :operation
            """,
        )
            .bind("catalog", catalog).bind("operation", operation).mapTo(Int::class.java).one() == 1

    private fun requireSame(
        h: Handle,
        catalog: Long,
        operation: UUID,
        column: String,
        encoded: String,
    ) {
        check(column == "definition" || column == "files")
        if (column == "definition") {
            val stored =
                h.createQuery(
                    """
                SELECT definition::text FROM hog_table_creation
                WHERE catalog_id = :catalog AND operation_id = :operation
                """,
                ).bind("catalog", catalog).bind("operation", operation).mapTo(String::class.java).one()
            val normalized =
                TableCreationDefinitionCodec.encode(
                    TableCreationDefinitionCodec.decode(stored, "table creation operation $operation"),
                )
            val same =
                h.createQuery("SELECT CAST(:stored AS jsonb) = CAST(:requested AS jsonb)")
                    .bind("stored", normalized).bind("requested", encoded).mapTo(Boolean::class.java).one()
            if (!same) {
                throw HoglakeException.CommitConflict("operation id reused with a different definition")
            }
            return
        }
        val same =
            h.createQuery(
                """
                SELECT $column = CAST(:encoded AS jsonb) FROM hog_table_creation WHERE catalog_id = :catalog AND
                operation_id = :operation
                """,
            )
                .bind(
                    "encoded",
                    encoded,
                ).bind("catalog", catalog).bind("operation", operation).mapTo(Boolean::class.java).one()
        if (!same) throw HoglakeException.CommitConflict("operation id reused with a different $column")
    }

    private fun transition(
        h: Handle,
        catalog: Long,
        operation: UUID,
        state: String,
        reason: String,
    ) {
        h.createUpdate(
            """
            UPDATE hog_table_creation SET state = :state, reason = :reason WHERE catalog_id = :catalog AND
            operation_id = :operation AND state = 'prepared'
            """,
        )
            .bind("state", state).bind("reason", reason).bind("catalog", catalog).bind("operation", operation).execute()
    }

    private fun load(
        h: Handle,
        catalog: Long,
        operation: UUID,
    ): TableCreation {
        // Always lock before inspecting state. Publish acquires catalog -> operation;
        // other endpoints never acquire the catalog lock while holding this row.
        h.createQuery(
            """
            SELECT operation_id FROM hog_table_creation
            WHERE catalog_id = :catalog AND operation_id = :operation FOR UPDATE
            """,
        ).bind("catalog", catalog).bind("operation", operation).mapTo(UUID::class.java)
            .findOne().orElseThrow { HoglakeException.NotFound("table creation operation '$operation'") }
        // Lazy expiry is sufficient to fence late publication; no object deletion is performed.
        h.createUpdate(
            """
            UPDATE hog_table_creation SET state = 'aborted', reason = 'expired' WHERE catalog_id = :catalog AND
            operation_id = :operation AND state = 'prepared' AND expires_at <= clock_timestamp()
            """,
        )
            .bind("catalog", catalog).bind("operation", operation).execute()
        return h.createQuery(
            "SELECT * FROM hog_table_creation WHERE catalog_id = :catalog AND operation_id = :operation",
        )
            .bind("catalog", catalog).bind("operation", operation)
            .map { rs, _ ->
                val definition =
                    TableCreationDefinitionCodec.decode(
                        rs.getString("definition"),
                        "table creation operation $operation",
                    )
                TableCreation(
                    operation, rs.getObject("table_uuid", UUID::class.java), definition,
                    initialColumns(definition.columns),
                    rs.getString(
                        "write_path",
                    ),
                    rs.getString(
                        "state",
                    ),
                    rs.getObject("expires_at", java.time.OffsetDateTime::class.java).toInstant(),
                    rs.getObject("snapshot_id")?.let { (it as Number).toLong() },
                    rs.getObject("schema_version")?.let { (it as Number).toLong() }, rs.getString("reason"),
                )
            }.findOne().orElseThrow { HoglakeException.NotFound("table creation operation '$operation'") }
    }
}
