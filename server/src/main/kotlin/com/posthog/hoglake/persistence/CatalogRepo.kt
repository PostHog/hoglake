package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.CatalogNotEmptyReason
import com.posthog.hoglake.model.HoglakeException
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.statement.UnableToExecuteStatementException

/** (new snapshot id, new schema version) minted by [CatalogRepo.allocateSnapshot]. */
data class SnapshotAlloc(val snapshotId: Long, val schemaVersion: Long)

/**
 * hog_catalog: identity, head pointers, and the per-catalog id
 * allocators. Allocator advances are only legal inside a transaction
 * that holds the catalog commit lock ([Locks.acquireCatalogCommitLock]);
 * callers own that discipline.
 */
object CatalogRepo {
    /**
     * THE hog_catalog row mapping — options/expiry/cleanup read the
     * lifecycle slice through the same mapper (the drifted
     * OptionsService.LifecycleCatalog duplicate is gone). The column
     * list lives in [HogSchemaColumns]; the mapper-coverage gate keeps
     * it equal to the live schema.
     */
    private const val CATALOG_COLUMNS =
        "catalog_id, name, data_path, last_snapshot_id, schema_version, " +
            "earliest_snapshot_time, snapshot_retention_seconds, consumer_floor, " +
            "earliest_snapshot_id"

    private val catalogMapper =
        RowMapper { rs, _ ->
            CatalogInfo(
                catalogId = rs.getLong("catalog_id"),
                name = rs.getString("name"),
                dataPath = rs.getString("data_path"),
                headSnapshotId = rs.getLong("last_snapshot_id"),
                schemaVersion = rs.getLong("schema_version"),
                earliestSnapshotTime =
                    rs.getObject("earliest_snapshot_time", java.time.OffsetDateTime::class.java)
                        ?.toInstant(),
                snapshotRetentionSeconds =
                    rs.getLong("snapshot_retention_seconds")
                        .let { if (rs.wasNull()) null else it },
                consumerFloor = rs.getBoolean("consumer_floor"),
                earliestSnapshotId = rs.getLong("earliest_snapshot_id"),
            )
        }

    /**
     * Insert a new catalog with all allocators at their defaults.
     * Duplicate name -> [HoglakeException.AlreadyExists]; a name failing
     * the schema CHECK -> [HoglakeException.Validation].
     */
    fun insert(
        handle: Handle,
        name: String,
        dataPath: String,
    ): CatalogInfo =
        try {
            handle.createQuery(
                """
                INSERT INTO hog_catalog (name, data_path)
                VALUES (:name, :dataPath)
                RETURNING $CATALOG_COLUMNS
                """,
            )
                .bind("name", name)
                .bind("dataPath", dataPath)
                .map(catalogMapper)
                .one()
        } catch (e: UnableToExecuteStatementException) {
            when {
                Pg.isUniqueViolation(e) ->
                    throw HoglakeException.AlreadyExists("catalog '$name' already exists")
                Pg.isCheckViolation(e) ->
                    throw HoglakeException.Validation(
                        "invalid catalog name '$name' (must match ^[a-z0-9][a-z0-9_-]{0,62}$)",
                    )
                else -> throw e
            }
        }

    fun findByName(
        handle: Handle,
        name: String,
    ): CatalogInfo? =
        handle.createQuery("SELECT $CATALOG_COLUMNS FROM hog_catalog WHERE name = :name")
            .bind("name", name)
            .map(catalogMapper)
            .findOne()
            .orElse(null)

    /** Serialize option validation and updates on the single catalog row. */
    fun requireForUpdate(
        handle: Handle,
        name: String,
    ): CatalogInfo =
        handle.createQuery("SELECT $CATALOG_COLUMNS FROM hog_catalog WHERE name = :name FOR UPDATE")
            .bind("name", name)
            .map(catalogMapper)
            .findOne()
            .orElseThrow { HoglakeException.NotFound("catalog '$name'") }

    /** [findByName] or [HoglakeException.NotFound]. */
    fun require(
        handle: Handle,
        name: String,
    ): CatalogInfo = findByName(handle, name) ?: throw HoglakeException.NotFound("catalog '$name'")

    fun listAll(handle: Handle): List<CatalogInfo> =
        handle.createQuery("SELECT $CATALOG_COLUMNS FROM hog_catalog ORDER BY name")
            .map(catalogMapper)
            .list()

    fun page(
        handle: Handle,
        after: String?,
        limit: Int,
    ): List<CatalogInfo> =
        handle.createQuery(
            """
            SELECT $CATALOG_COLUMNS FROM hog_catalog
            WHERE (:after::text IS NULL OR name > :after) ORDER BY name LIMIT :limit
            """,
        )
            .bind("after", after).bind("limit", limit).map(catalogMapper).list()

    /**
     * Mint the next snapshot id (and schema version) for a DDL/commit
     * tail. Caller must hold the catalog commit lock.
     *
     * NO ROW IS A 404, NOT A 500, since catalogs can be dropped
     * (`CatalogService.dropCatalog`). Every DDL tail resolves the catalog
     * BEFORE it queues on the commit lock, so one that queued behind a
     * drop wakes up holding a `catalog_id` whose row is gone — and this
     * UPDATE is the first statement after the lock that notices. `.one()`
     * answered that with an IllegalStateException and the caller got an
     * internal_error for an ordinary lost race. Costs nothing: it is the
     * same statement, read with `findOne`.
     */
    fun allocateSnapshot(
        handle: Handle,
        catalogId: Long,
    ): SnapshotAlloc =
        handle.createQuery(
            """
            UPDATE hog_catalog
            SET last_snapshot_id = last_snapshot_id + 1,
                schema_version   = schema_version + 1
            WHERE catalog_id = :catalogId
            RETURNING last_snapshot_id, schema_version
            """,
        )
            .bind("catalogId", catalogId)
            .map { rs, _ -> SnapshotAlloc(rs.getLong("last_snapshot_id"), rs.getLong("schema_version")) }
            .findOne()
            .orElseThrow { droppedWhileQueued(catalogId) }

    /**
     * The 404 for a transaction whose catalog was dropped after it
     * resolved it — a request queued on the commit lock, or an expiry,
     * retirement or compaction tail that reaches [allocateSnapshot]. By
     * id, because the name is not in scope at the allocators or in the
     * commit tail, and the name may already belong to a NEW catalog by
     * the time anyone reads this.
     */
    fun droppedWhileQueued(catalogId: Long): HoglakeException.NotFound =
        HoglakeException.NotFound(
            "catalog (catalog_id=$catalogId) was dropped after this operation resolved it",
        )

    /**
     * Row-lock the catalog's `hog_maintenance_summary` row (if it has one)
     * for `CatalogService.dropCatalog`, BEFORE [lockForDrop].
     *
     * Lock order, not a precondition. `MaintenanceSummarySampler` claims
     * this row `FOR UPDATE SKIP LOCKED` and then inserts tier rows, whose
     * foreign key takes `FOR KEY SHARE` on the catalog row: summary, then
     * catalog. The drop, without this, would take the catalog row and
     * then the summary row through its cascade — the reverse order, and a
     * deadlock whenever a tick lands on the catalog being dropped. Taken
     * first, the drop waits out a tick in flight, and a tick that starts
     * after it SKIPs the catalog. Absent row: nothing to order against
     * (the sampler's discovery INSERT takes only the catalog row).
     */
    fun lockSummaryForDrop(
        handle: Handle,
        catalogId: Long,
    ) {
        handle.createQuery("SELECT 1 FROM hog_maintenance_summary WHERE catalog_id = :catalogId FOR UPDATE")
            .bind("catalogId", catalogId)
            .mapTo(Int::class.java)
            .findOne()
    }

    /**
     * Row-lock the catalog for `CatalogService.dropCatalog` and return
     * its `data_path`; null when the row is gone (a concurrent drop won).
     *
     * THE ROW LOCK IS WHAT THE COMMIT LOCK CANNOT GIVE. Several writers
     * insert catalog-scoped rows WITHOUT the commit lock, on purpose:
     * upload claims (UploadService's class comment says why), consumer
     * offset commits, table-creation prepares, compaction claims and
     * staging tickets, maintenance ledger rows. Each such INSERT takes
     * `FOR KEY SHARE` on this row through its foreign key, and `FOR
     * UPDATE` conflicts with `FOR KEY SHARE`, so taking it BEFORE the
     * precondition read does two things:
     *
     *  1. an insert that is in flight (uncommitted) when the drop arrives
     *     holds KEY SHARE, so this waits for it to commit — and the
     *     precondition statement after it, under READ COMMITTED, takes a
     *     new snapshot and SEES the row. Without the lock, the
     *     precondition read misses the uncommitted claim, the DELETE
     *     waits on the same KEY SHARE, and the cascade then deletes the
     *     claim that committed in between: an `active` upload refused by
     *     nothing and erased by the drop.
     *  2. an insert that arrives AFTER this blocks on it until the drop
     *     commits, and then fails its foreign key. It never lands in a
     *     catalog that has passed its preconditions.
     *
     * The DELETE would take this lock anyway; taking it here moves it
     * ahead of the read it has to fence. A commit-path writer never
     * contends on it — it needs the commit lock, which the drop already
     * holds — and readers do not lock the row at all.
     */
    fun lockForDrop(
        handle: Handle,
        catalogId: Long,
    ): String? =
        handle.createQuery("SELECT data_path FROM hog_catalog WHERE catalog_id = :catalogId FOR UPDATE")
            .bind("catalogId", catalogId)
            .mapTo(String::class.java)
            .findOne()
            .orElse(null)

    /**
     * What [DROP_BLOCKERS_SQL] found: one field per precondition, plus a
     * few names for the detail. [failing] empty means the catalog may be
     * deleted.
     */
    data class DropBlockers(
        val liveNamespaces: Long,
        val liveNamespaceSample: List<String>,
        val liveFiles: Boolean,
        val endedFiles: Boolean,
        val undrainedRemovals: Boolean,
        val activeCompactionClaims: Boolean,
        val consumerOffsets: Long,
        val consumerSample: List<String>,
        val activeUploads: Long,
        val abandonedUploadsUnsettled: Long,
    ) {
        /**
         * The failing preconditions in [CatalogNotEmptyReason] order —
         * the resolution order, so the first is the one to act on — each
         * with the prose the refusal's detail carries.
         */
        fun failing(): List<Pair<CatalogNotEmptyReason, String>> =
            buildList {
                if (liveNamespaces > 0) {
                    add(
                        CatalogNotEmptyReason.LIVE_NAMESPACES to
                            "$liveNamespaces live namespace(s) (${sample(liveNamespaceSample, liveNamespaces)}); " +
                            "drop their tables and views, then the namespaces",
                    )
                }
                if (liveFiles) {
                    add(
                        CatalogNotEmptyReason.TABLES_PENDING_RETIREMENT to
                            "live file rows remain on dropped tables; retirement deletes them and queues " +
                            "their objects once the expiry floor passes the drop (a catalog with no " +
                            "retention never gets there)",
                    )
                }
                if (endedFiles) {
                    add(
                        CatalogNotEmptyReason.FILES_PENDING_EXPIRY to
                            "ended file rows remain (with any deletion vectors on them); expiry purges " +
                            "them and queues their objects once the floor passes their end snapshot",
                    )
                }
                if (undrainedRemovals) {
                    add(
                        CatalogNotEmptyReason.REMOVALS_PENDING to
                            "the removal queue is not drained (storage-usage pending_delete_files " +
                            "counts it); let cleanup finish",
                    )
                }
                if (activeCompactionClaims) {
                    add(
                        CatalogNotEmptyReason.ACTIVE_COMPACTION_CLAIMS to
                            "unexpired compaction claims remain; a compactor may still be writing an output " +
                            "under the data path (claims expire within an hour)",
                    )
                }
                if (consumerOffsets > 0) {
                    add(
                        CatalogNotEmptyReason.CONSUMER_OFFSETS to
                            "$consumerOffsets consumer offset(s) (consumers ${sample(consumerSample, null)}); " +
                            "there is no offset-delete API",
                    )
                }
                if (activeUploads > 0) {
                    add(
                        CatalogNotEmptyReason.ACTIVE_UPLOADS to
                            "$activeUploads active upload claim(s); let them register or abandon them, " +
                            "or let them expire and run uploads/schedule-expired",
                    )
                }
                if (abandonedUploadsUnsettled > 0) {
                    add(
                        CatalogNotEmptyReason.ABANDONED_UPLOADS_UNSETTLED to
                            "$abandonedUploadsUnsettled abandoned upload(s) unscheduled or still inside " +
                            "their claim's expiry window (a late PUT can land until then); run " +
                            "uploads/schedule-expired, let cleanup drain, and wait out the expiry",
                    )
                }
            }

        /**
         * A capped name list. [names] holds up to [DROP_BLOCKER_SAMPLE] + 1
         * (the SQL fetches one more than it prints), so "there are more"
         * is a fact about the rows rather than a guess at exactly the cap;
         * [total] is the counted population when there is one (namespaces),
         * null when the count is of something else (offset rows, not
         * consumers).
         */
        internal fun sample(
            names: List<String>,
            total: Long?,
        ): String {
            val shown = names.take(DROP_BLOCKER_SAMPLE)
            val truncated = names.size > DROP_BLOCKER_SAMPLE || (total != null && total > shown.size)
            return shown.joinToString(", ") + if (truncated) ", ..." else ""
        }
    }

    /** How many namespace names / consumer ids a refusal names. */
    const val DROP_BLOCKER_SAMPLE = 10

    /**
     * Every catalog-drop precondition in ONE statement, `internal` so a
     * test can run the SQL production runs.
     *
     * WHY "ANY FILE ROW", NOT `RetirementService.ELIGIBLE_PREDICATE`.
     * The cascade deletes file rows WITHOUT queueing their paths, and
     * every file row still in the table names an object nobody has
     * queued yet (retirement and expiry's purge delete a row in the same
     * statement that queues its path). So the precondition that keeps
     * the drop from orphaning objects is that NO file row remains, live
     * or ended. ELIGIBLE_PREDICATE is narrower on purpose and would be
     * wrong here twice: its floor clause misses a table dropped ABOVE the
     * floor (not yet eligible, every row still present), and its live-row
     * `EXISTS` misses a dropped table whose rows are all ENDED (truncated
     * or compacted before the drop), which retirement never touches and
     * expiry purges instead. The two arms below split "any file row" by
     * which loop removes it, so the reason says what to wait for: a live
     * data-file row is retirement's, an ended one is expiry's.
     *
     * NO ARM FOR `hog_delete_file`, because the schema makes one
     * unreachable: a vector's `data_file_id` is NOT NULL and a foreign key
     * into `hog_data_file`, so every vector row has a data-file row under
     * it, live or ended, and one of the two arms above already fires.
     * Both loops take the vectors with their data files (retirement
     * deletes them FIRST, expiry's vectors arm purges those riding a
     * purged data file), so waiting on the data files is waiting on the
     * vectors. An `OR EXISTS (… hog_delete_file …)` arm was here and no
     * state could make it the deciding term — a review's mutation of it
     * survived for exactly that reason. `CatalogDropApiIntegrationTest`
     * pins the foreign key this argument rests on, so dropping it reds
     * there rather than silently reopening the Puffin-orphan hole.
     *
     * EXISTS, NOT count(*), for the file and queue arms. On the success
     * path all of them are empty and each is one index probe
     * (`hog_data_file_live`, `hog_data_file_ended`,
     * `hog_file_removal_drain`, `hog_compaction_claim_expiry`); on the refusal path EXISTS
     * stops at the first row, where a count would walk millions of rows
     * under the commit lock to put a number in a message. The exact
     * backlog is GET .../storage-usage's job. The counted arms are bounded
     * by what they count (namespaces, consumers, uploads in flight).
     *
     * Binds `:catalogId` and `:sample` (the name cap plus one). No
     * interpolated values (invariant 9).
     */
    internal const val DROP_BLOCKERS_SQL: String =
        """
        SELECT
          (SELECT count(*) FROM hog_namespace
            WHERE catalog_id = :catalogId AND NOT dropped) AS live_namespaces,
          ARRAY(SELECT name FROM hog_namespace
                 WHERE catalog_id = :catalogId AND NOT dropped
                 ORDER BY name LIMIT :sample) AS live_namespace_sample,
          EXISTS (SELECT 1 FROM hog_data_file
                   WHERE catalog_id = :catalogId AND end_snapshot IS NULL) AS live_files,
          EXISTS (SELECT 1 FROM hog_data_file
                   WHERE catalog_id = :catalogId AND end_snapshot IS NOT NULL) AS ended_files,
          EXISTS (SELECT 1 FROM hog_file_removal
                   WHERE catalog_id = :catalogId AND drained_at IS NULL) AS undrained_removals,
          EXISTS (SELECT 1 FROM hog_compaction_claim
                   WHERE catalog_id = :catalogId AND expires_at > now()) AS active_compaction_claims,
          (SELECT count(*) FROM hog_consumer_offset
            WHERE catalog_id = :catalogId) AS consumer_offsets,
          ARRAY(SELECT DISTINCT consumer_id FROM hog_consumer_offset
                 WHERE catalog_id = :catalogId
                 ORDER BY consumer_id LIMIT :sample) AS consumer_sample,
          (SELECT count(*) FROM hog_upload
            WHERE catalog_id = :catalogId AND state = 'active') AS active_uploads,
          (SELECT count(*) FROM hog_upload
            WHERE catalog_id = :catalogId AND state = 'abandoned'
              AND (last_scheduled_at IS NULL OR expires_at > now())) AS abandoned_uploads_unsettled
        """

    fun dropBlockers(
        handle: Handle,
        catalogId: Long,
    ): DropBlockers =
        handle.createQuery(DROP_BLOCKERS_SQL)
            .bind("catalogId", catalogId)
            // One past the cap, so sample() can tell "exactly ten" from "more".
            .bind("sample", DROP_BLOCKER_SAMPLE + 1)
            .map { rs, _ ->
                fun names(column: String): List<String> = (rs.getArray(column).array as Array<*>).map { it as String }
                DropBlockers(
                    liveNamespaces = rs.getLong("live_namespaces"),
                    liveNamespaceSample = names("live_namespace_sample"),
                    liveFiles = rs.getBoolean("live_files"),
                    endedFiles = rs.getBoolean("ended_files"),
                    undrainedRemovals = rs.getBoolean("undrained_removals"),
                    activeCompactionClaims = rs.getBoolean("active_compaction_claims"),
                    consumerOffsets = rs.getLong("consumer_offsets"),
                    consumerSample = names("consumer_sample"),
                    activeUploads = rs.getLong("active_uploads"),
                    abandonedUploadsUnsettled = rs.getLong("abandoned_uploads_unsettled"),
                )
            }
            .one()

    /**
     * Delete the catalog row; the `ON DELETE CASCADE` foreign keys take
     * everything under it. Only `CatalogService.dropCatalog` calls this,
     * after [lockForDrop] and [dropBlockers].
     */
    fun delete(
        handle: Handle,
        catalogId: Long,
    ): Int =
        handle.createUpdate("DELETE FROM hog_catalog WHERE catalog_id = :catalogId")
            .bind("catalogId", catalogId)
            .execute()

    /** Allocate the next namespace id. Caller holds the commit lock. */
    fun allocateNamespaceId(
        handle: Handle,
        catalogId: Long,
    ): Long =
        handle.createQuery(
            """
            UPDATE hog_catalog SET next_namespace_id = next_namespace_id + 1
            WHERE catalog_id = :catalogId
            RETURNING next_namespace_id - 1 AS allocated
            """,
        )
            .bind("catalogId", catalogId)
            .mapTo(Long::class.javaObjectType)
            .one()

    /** Allocate the next table id. Caller holds the commit lock. */
    fun allocateTableId(
        handle: Handle,
        catalogId: Long,
    ): Long =
        handle.createQuery(
            """
            UPDATE hog_catalog SET next_table_id = next_table_id + 1
            WHERE catalog_id = :catalogId
            RETURNING next_table_id - 1 AS allocated
            """,
        )
            .bind("catalogId", catalogId)
            .mapTo(Long::class.javaObjectType)
            .one()
}
