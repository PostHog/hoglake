package com.posthog.hoglake.commit

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.PropertyNamingStrategies
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.service.UploadClaim
import com.posthog.hoglake.service.UploadService
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.argument.Argument
import org.jdbi.v3.core.statement.Query
import org.jdbi.v3.core.statement.SqlStatement
import java.time.Instant
import java.time.OffsetDateTime
import java.util.UUID

/**
 * The serialized commit tail as TWO statements (#393): [READ_SQL], which
 * reads everything [CommitService]'s checks need, and [WRITE_SQL], which
 * writes everything the commit publishes. Between them the checks run in
 * Kotlin exactly as they did against the statements these replace, in
 * the same order, on the same rows, with the same refusals.
 *
 * WHY. A commit's hold on the per-catalog lock is a FIXED cost per
 * commit plus a per-file one, and the fixed part is round trips: a
 * one-file prepared append sent 19 statements under the lock (a
 * two-table append 24, a three-DV delete 20), each a network round trip
 * paid while every other writer of the catalog queues. On 2026-10-14
 * millrace-prod-us held gigahog-prod-us at ~67 commits/s of small
 * commits (~43 files/s in all) and commit p50 went to ~30 s, with the
 * lock's per-file duty at ~16 ms/s. Two statements is the floor for this
 * design, whatever the commit's shape; `CommitLockRoundTripsIntegrationTest`
 * pins it.
 *
 * WHY NOT ONE. Two things stand in the way, and neither is a matter of
 * taste:
 *
 *  - The lock is its own statement ([com.posthog.hoglake.persistence.Locks],
 *    unchanged). Under READ COMMITTED a statement's snapshot is taken
 *    when it STARTS, so a statement that took the lock itself would read
 *    the catalog as it was BEFORE its wait — without the previous
 *    holder's receipt (a retried key published twice), its change rows
 *    (a missed conflict) or its allocator advance.
 *  - The checks between the read and the write are Kotlin: StatsSanity's
 *    bound decoding, whose output IS the column-stats rows written, and
 *    the typed refusals in their documented precedence (the cause before
 *    the symptom, see `doCommit`). Folding them into the database means a
 *    second implementation of both in PL/pgSQL, plus a migration,
 *    schema.sql, and a versioned function name for a rolling deploy.
 *
 * WHY THE READ CAN BE ONE STATEMENT. Everything it reads is one of:
 * written only under the commit lock (head, receipts, table resolution,
 * specs, columns, the row-id allocator, change rows, delete targets and
 * their live DVs), so one snapshot taken right after the lock sees what
 * the separate statements saw — the purges that DELETE receipts and
 * ended file rows outside the lock raced those statements at an
 * instant no less arbitrary; or ROW-LOCKED by the read itself (the
 * upload claims — `FOR UPDATE` re-fetches the newest version after a
 * wait, so a sweep that fenced a claim while the read waited is seen,
 * exactly as before); or the path-reuse guard
 * ([CommitService.REMOVAL_QUEUE_COLLISION_SQL]), whose unlocked
 * inserters it reads at the read's snapshot rather than a few
 * statements later. That snapshot is earlier than the old statement's,
 * but the time it leaves to the COMMIT is shorter, because fewer round
 * trips follow it.
 *
 * WHY THE WRITE CAN BE ONE STATEMENT. Every value it writes is computed
 * in Kotlin from the read, so the write has no decisions left in it.
 * Data-modifying CTEs all hang off the allocator UPDATE (`alloc`), which
 * is GUARDED by the head and the next file id the read returned: the
 * snapshot and file ids are now computed from the read rather than
 * RETURNED by the UPDATE, and the guard keeps the two the same (the old
 * `UPDATE ... RETURNING` was atomic and could not disagree with itself).
 * The table-stats UPDATE is guarded by each next_row_id, which closes the
 * read-then-write the old per-table SELECT and UPDATE already had. The
 * DV-ending UPDATE touches only rows still live (`end_snapshot IS NULL`),
 * the state the read selected them in. Under the lock nothing can move
 * any of these; a writer that ever broke that discipline now fails the
 * commit (the counts [write] checks) where it used to hand out
 * overlapping row-id ranges or end a DV twice. The claim settlement
 * carries no such guard: the read holds those rows `FOR UPDATE` in this
 * transaction, so no state can change under it, and a `state = 'active'`
 * predicate moved the generic plan off `hog_upload_pkey` onto the
 * cleanup index for nothing.
 *
 * Foreign keys are checked at the end of the statement, so column
 * stats, partition values and a transaction's DV on its own staged file
 * may reference rows a sibling CTE inserts; `delete_files` reads
 * `ended_dvs` first, so the one-live-DV-per-data-file slot is free before
 * the superseding row arrives. snapshot_time is still clock_timestamp(),
 * evaluated by this statement, under the lock.
 *
 * Plan shape: per-key probes are `LATERAL ... LIMIT 2` (a LIMIT keeps the
 * planner from flattening the subquery into a join it could hash over
 * `hog_table_version`; 2 rather than 1 so a broken unique invariant is
 * seen, as `findOne` saw it), and sets computed inside the statement
 * drive index scans as `= ANY(ARRAY(SELECT ...))`, the shape the
 * separate statements had with a bound array. `CommitStatementPlanIntegrationTest`
 * EXPLAINs both statements, custom and generic.
 */
internal object CommitTail {
    private val LONG = Long::class.javaObjectType

    /**
     * Rows of [READ_SQL]'s JSON sections. Snake-case keys are the CTEs'
     * own column names; a `RowMapper` per section would be one query per
     * section, which is the cost this file exists to remove.
     *
     * A `timestamptz` arrives as `json_agg` renders it: ISO 8601 in the
     * SESSION's time zone with that zone's offset, `+00:00` on a UTC
     * session and `+02:00` on one that is not. The `Instant`
     * deserializer normalizes any offset, so the session zone does not
     * reach a value; `CommitTailJsonTest` pins that with a non-UTC offset.
     */
    internal val readSetMapper: ObjectMapper =
        jacksonObjectMapper()
            .registerModule(JavaTimeModule())
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)

    /**
     * Everything the commit's checks read, after the lock, in one
     * statement. See the object KDoc for why one snapshot is enough.
     *
     * Binds `:catalogId` (and `:catalog`, the name the embedded claims
     * statement uses), `:idempotencyKey`, the requested tables as four
     * parallel arrays `:namespaces` / `:names` / `:appending` /
     * `:unchanged` (one entry per distinct (namespace, table), in the
     * order the refusals are decided), `:readSnapshot`, every registered
     * path as `:paths`, and the numeric DV targets as `:targetIds`. No
     * interpolated values (invariant 9 intact): the two embedded
     * statements are compile-time constants.
     *
     * A receipt hit short-circuits everything but the removal-queue probe
     * (an index probe, no lock): a replay answers from the receipt, as it
     * did, and locks no claim — the claims statement sits under a one-time
     * filter on the receipt, so its `FOR UPDATE` never runs for one.
     */
    internal const val READ_SQL: String =
        """
        WITH receipt AS MATERIALIZED (
            SELECT snapshot_id, schema_version, fingerprint, request::text AS request
              FROM hog_commit_receipt
             WHERE catalog_id = :catalogId AND idempotency_key = CAST(:idempotencyKey AS uuid)
        ),
        head AS MATERIALIZED (
            SELECT last_snapshot_id, schema_version, earliest_snapshot_id, earliest_snapshot_time, next_file_id
              FROM hog_catalog
             WHERE catalog_id = :catalogId
        ),
        requested AS MATERIALIZED (
            SELECT q.ord, q.namespace, q.name, q.appending, q.unchanged
              FROM unnest(CAST(:namespaces AS text[]), CAST(:names AS text[]),
                          CAST(:appending AS boolean[]), CAST(:unchanged AS boolean[]))
                       WITH ORDINALITY AS q(namespace, name, appending, unchanged, ord)
             WHERE NOT EXISTS (SELECT 1 FROM receipt)
        ),
        resolved AS MATERIALIZED (
            SELECT q.ord, q.appending, q.unchanged, live.table_id, live.table_uuid
              FROM requested q
              CROSS JOIN LATERAL (
                  SELECT tv.table_id, t.table_uuid
                    FROM hog_table_version tv
                    JOIN hog_namespace ns
                      ON ns.catalog_id = tv.catalog_id AND ns.namespace_id = tv.namespace_id
                    JOIN hog_table t
                      ON t.catalog_id = tv.catalog_id AND t.table_id = tv.table_id
                   WHERE tv.catalog_id = :catalogId
                     AND tv.end_snapshot IS NULL
                     AND tv.name = q.name
                     AND ns.name = q.namespace
                     AND NOT ns.dropped
                     AND t.dropped_snapshot IS NULL
                   LIMIT 2
              ) live
        ),
        tables AS (
            SELECT r.ord, r.table_id, r.table_uuid, spec.spec_id, spec.field_count,
                   CASE WHEN r.appending THEN
                       (SELECT ts.next_row_id FROM hog_table_stats ts
                         WHERE ts.catalog_id = :catalogId AND ts.table_id = r.table_id)
                   END AS next_row_id
              FROM resolved r
              LEFT JOIN LATERAL (
                  SELECT ps.spec_id,
                         (SELECT count(*) FROM hog_partition_field pf
                           WHERE pf.catalog_id = ps.catalog_id AND pf.table_id = ps.table_id
                             AND pf.spec_id = ps.spec_id) AS field_count
                    FROM hog_partition_spec ps
                   WHERE ps.catalog_id = :catalogId AND ps.table_id = r.table_id
                     AND ps.end_snapshot IS NULL
                     AND r.appending
                   LIMIT 2
              ) spec ON true
        ),
        columns AS (
            SELECT c.table_id, c.field_id, c.col_type
              FROM hog_column c
             WHERE c.catalog_id = :catalogId
               AND c.table_id = ANY(ARRAY(SELECT table_id FROM resolved WHERE appending))
               AND c.end_snapshot IS NULL
        ),
        conflicts AS (
            SELECT DISTINCT c.object_id, c.kind IN ('table_dropped', 'table_altered') AS ddl
              FROM hog_snapshot_change c
             WHERE c.catalog_id = :catalogId
               AND CAST(:readSnapshot AS bigint) >= (SELECT earliest_snapshot_id FROM head)
               AND CAST(:readSnapshot AS bigint) <= (SELECT last_snapshot_id FROM head)
               AND c.snapshot_id > CAST(:readSnapshot AS bigint)
               AND c.object_id = ANY(ARRAY(SELECT table_id FROM resolved))
               AND (c.kind IN ('table_dropped', 'table_altered') OR
                    (c.kind IN ('table_created', 'table_inserted_into', 'table_deleted_from')
                     AND c.object_id = ANY(ARRAY(SELECT table_id FROM resolved WHERE unchanged))))
        ),
        claims AS MATERIALIZED (
            SELECT * FROM (${UploadService.REGISTER_CLAIMS_SQL}) locked WHERE NOT EXISTS (SELECT 1 FROM receipt)
        ),
        retained AS (
            SELECT path, 'data' AS file_kind FROM hog_data_file
             WHERE catalog_id = :catalogId
               AND path = ANY(ARRAY(SELECT path FROM claims WHERE state = 'registered'))
            UNION
            SELECT path, 'delete' AS file_kind FROM hog_delete_file
             WHERE catalog_id = :catalogId
               AND path = ANY(ARRAY(SELECT path FROM claims WHERE state = 'registered'))
        ),
        queued AS (${CommitService.REMOVAL_QUEUE_COLLISION_SQL}),
        targets AS (
            SELECT data_file_id, table_id, record_count, begin_snapshot, end_snapshot
              FROM hog_data_file
             WHERE catalog_id = :catalogId AND data_file_id = ANY(CAST(:targetIds AS bigint[]))
               AND NOT EXISTS (SELECT 1 FROM receipt)
        ),
        live_dvs AS (
            SELECT data_file_id, delete_file_id, delete_count, begin_snapshot
              FROM hog_delete_file
             WHERE catalog_id = :catalogId AND data_file_id = ANY(CAST(:targetIds AS bigint[]))
               AND end_snapshot IS NULL
               AND NOT EXISTS (SELECT 1 FROM receipt)
        )
        SELECT h.last_snapshot_id, h.schema_version, h.earliest_snapshot_id, h.earliest_snapshot_time,
               h.next_file_id,
               rc.snapshot_id AS receipt_snapshot_id, rc.schema_version AS receipt_schema_version,
               rc.fingerprint AS receipt_fingerprint, rc.request AS receipt_request,
               (SELECT json_agg(t ORDER BY t.ord) FROM tables t) AS tables,
               (SELECT json_agg(c) FROM columns c) AS columns,
               (SELECT json_agg(c) FROM conflicts c) AS conflicts,
               (SELECT json_agg(c ORDER BY c.upload_id) FROM claims c) AS claims,
               (SELECT json_agg(rt) FROM retained rt) AS retained,
               (SELECT json_agg(q.path) FROM queued q) AS queued,
               (SELECT json_agg(t) FROM targets t) AS targets,
               (SELECT json_agg(d) FROM live_dvs d) AS live_dvs
          FROM head h
          LEFT JOIN receipt rc ON true
        """

    /**
     * The append rows: the row-id allocator advance and gross rollup per
     * table, then the files, their partition values and their column
     * stats. One fragment for [WRITE_SQL] and [APPEND_SQL], so a commit
     * and an atomic table creation write file rows through the same text.
     * Every CTE reads `snapshot`, so nothing is written unless it has a
     * row. `table_id = ANY(...)` beside the join keeps the stats UPDATE on
     * the primary key whatever the planner guesses for the unnest.
     */
    private const val APPEND_ROWS_CTES: String =
        """
        table_stats AS (
            UPDATE hog_table_stats ts
               SET next_row_id = u.to_row_id,
                   record_count = ts.record_count + u.records,
                   file_size_bytes = ts.file_size_bytes + u.bytes
              FROM snapshot s,
                   unnest(CAST(:statsTables AS bigint[]), CAST(:statsFrom AS bigint[]),
                          CAST(:statsTo AS bigint[]), CAST(:statsRecords AS bigint[]),
                          CAST(:statsBytes AS bigint[]))
                       AS u(table_id, from_row_id, to_row_id, records, bytes)
             WHERE ts.catalog_id = :catalogId
               AND ts.table_id = ANY(CAST(:statsTables AS bigint[]))
               AND ts.table_id = u.table_id
               AND ts.next_row_id = u.from_row_id
            RETURNING 1
        ),
        data_files AS (
            INSERT INTO hog_data_file (catalog_id, data_file_id, table_id, begin_snapshot,
                                       path, record_count, file_size_bytes, footer_size,
                                       row_id_start, stats_state, spec_id, split_offsets)
            SELECT s.catalog_id, f.data_file_id, f.table_id, s.snapshot_id,
                   f.path, f.record_count, f.file_size_bytes, f.footer_size,
                   f.row_id_start, f.stats_state, f.spec_id, CAST(f.split_offsets AS bigint[])
              FROM snapshot s,
                   unnest(CAST(:fileIds AS bigint[]), CAST(:fileTables AS bigint[]),
                          CAST(:filePaths AS text[]), CAST(:fileRecords AS bigint[]),
                          CAST(:fileBytes AS bigint[]), CAST(:fileFooters AS bigint[]),
                          CAST(:fileRowIds AS bigint[]), CAST(:fileStates AS text[]),
                          CAST(:fileSpecs AS bigint[]), CAST(:fileSplits AS text[]))
                       AS f(data_file_id, table_id, path, record_count, file_size_bytes, footer_size,
                            row_id_start, stats_state, spec_id, split_offsets)
            RETURNING 1
        ),
        partition_values AS (
            INSERT INTO hog_file_partition_value (catalog_id, data_file_id, key_index, value)
            SELECT s.catalog_id, p.data_file_id, p.key_index, p.value
              FROM snapshot s,
                   unnest(CAST(:partFiles AS bigint[]), CAST(:partKeys AS int[]), CAST(:partValues AS text[]))
                       AS p(data_file_id, key_index, value)
            RETURNING 1
        ),
        column_stats AS (
            INSERT INTO hog_file_column_stats (catalog_id, data_file_id, field_id, value_count,
                                               null_count, nan_count, size_bytes,
                                               lower_bound, upper_bound)
            SELECT s.catalog_id, c.data_file_id, c.field_id, c.value_count,
                   c.null_count, c.nan_count, c.size_bytes,
                   c.lower_bound, c.upper_bound
              FROM snapshot s,
                   unnest(CAST(:statFiles AS bigint[]), CAST(:statFields AS bigint[]),
                          CAST(:statValues AS bigint[]), CAST(:statNulls AS bigint[]),
                          CAST(:statNans AS bigint[]), CAST(:statSizes AS bigint[]),
                          CAST(:statLowers AS bytea[]), CAST(:statUppers AS bytea[]))
                       AS c(data_file_id, field_id, value_count, null_count, nan_count, size_bytes,
                            lower_bound, upper_bound)
            RETURNING 1
        )"""

    private const val APPEND_ROWS_COUNTS: String =
        """
               (SELECT count(*) FROM table_stats) AS table_stats,
               (SELECT count(*) FROM data_files) AS data_files,
               (SELECT count(*) FROM partition_values) AS partition_values,
               (SELECT count(*) FROM column_stats) AS column_stats"""

    /**
     * Everything a commit publishes, in one statement. See the object
     * KDoc for the guards and the CTE ordering.
     *
     * `delete_files`' `WHERE (SELECT count(*) FROM ended_dvs) >= 0` is the
     * ordering, not decoration: it makes the INSERT drain `ended_dvs`
     * before its first row. The final SELECT happens to count `ended_dvs`
     * first as well, and that is NOT relied on — with the gate removed and
     * the two counts swapped, `hog_delete_file_one_live_per_data_file`
     * refuses every supersession (`CommitDeletesAndPartitionsTest`); with
     * the gate kept, either order passes.
     *
     * Binds `:catalogId`, the allocator's expected `:head` /
     * `:firstFileId` and its advance `:fileCount`, the snapshot's
     * `:schemaVersion` / `:author` / `:message`, the change rows as
     * `:changeKinds` / `:changeTables`, the append rows ([APPEND_ROWS_CTES]),
     * the DVs to end (`:endedDvs`) and to insert (`:dv*`), the claims to
     * settle (`:uploadIds`), and the receipt's `:idempotencyKey` /
     * `:fingerprint` (both null without a key: no receipt row).
     */
    internal const val WRITE_SQL: String =
        """
        WITH alloc AS (
            UPDATE hog_catalog
               SET last_snapshot_id = last_snapshot_id + 1,
                   next_file_id = next_file_id + :fileCount
             WHERE catalog_id = :catalogId AND last_snapshot_id = :head AND next_file_id = :firstFileId
            RETURNING catalog_id, last_snapshot_id
        ),
        snapshot AS (
            INSERT INTO hog_snapshot (catalog_id, snapshot_id, snapshot_time, schema_version, author, commit_message)
            SELECT catalog_id, last_snapshot_id, clock_timestamp(), CAST(:schemaVersion AS bigint),
                   CAST(:author AS text), CAST(:message AS text)
              FROM alloc
            RETURNING catalog_id, snapshot_id, schema_version
        ),
        changes AS (
            INSERT INTO hog_snapshot_change (catalog_id, snapshot_id, kind, object_id)
            SELECT s.catalog_id, s.snapshot_id, c.kind, c.object_id
              FROM snapshot s,
                   unnest(CAST(:changeKinds AS text[]), CAST(:changeTables AS bigint[])) AS c(kind, object_id)
            RETURNING 1
        ),
        $APPEND_ROWS_CTES,
        ended_dvs AS (
            UPDATE hog_delete_file d
               SET end_snapshot = s.snapshot_id
              FROM snapshot s
             WHERE d.catalog_id = :catalogId AND d.delete_file_id = ANY(CAST(:endedDvs AS bigint[]))
               AND d.end_snapshot IS NULL
            RETURNING 1
        ),
        delete_files AS (
            INSERT INTO hog_delete_file (catalog_id, delete_file_id, table_id, data_file_id,
                                         begin_snapshot, path, delete_count, file_size_bytes)
            SELECT s.catalog_id, d.delete_file_id, d.table_id, d.data_file_id,
                   s.snapshot_id, d.path, d.delete_count, d.file_size_bytes
              FROM snapshot s,
                   unnest(CAST(:dvIds AS bigint[]), CAST(:dvTables AS bigint[]), CAST(:dvTargets AS bigint[]),
                          CAST(:dvPaths AS text[]), CAST(:dvCounts AS bigint[]), CAST(:dvBytes AS bigint[]))
                       AS d(delete_file_id, table_id, data_file_id, path, delete_count, file_size_bytes)
             WHERE (SELECT count(*) FROM ended_dvs) >= 0
            RETURNING 1
        ),
        uploads AS (
            UPDATE hog_upload u
               SET state = 'registered'
              FROM snapshot s
             WHERE u.catalog_id = :catalogId AND u.upload_id = ANY(CAST(:uploadIds AS uuid[]))
            RETURNING 1
        ),
        receipt AS (
            INSERT INTO hog_commit_receipt (catalog_id, idempotency_key, fingerprint, snapshot_id, schema_version)
            SELECT s.catalog_id, CAST(:idempotencyKey AS uuid), CAST(:fingerprint AS bytea), s.snapshot_id,
                   s.schema_version
              FROM snapshot s
             WHERE CAST(:idempotencyKey AS uuid) IS NOT NULL
            RETURNING 1
        )
        SELECT (SELECT snapshot_id FROM snapshot) AS snapshot_id,
               (SELECT count(*) FROM changes) AS changes,
               $APPEND_ROWS_COUNTS,
               (SELECT count(*) FROM ended_dvs) AS ended_dvs,
               (SELECT count(*) FROM delete_files) AS delete_files,
               (SELECT count(*) FROM uploads) AS uploads,
               (SELECT count(*) FROM receipt) AS receipts
        """

    /**
     * The append rows alone, into a snapshot the caller already
     * published: `CommitService.registerInitialFiles`, whose DDL tail
     * allocates its own snapshot and file ids. Binds `:snapshotId`,
     * `:catalogId` and the append rows.
     */
    internal const val APPEND_SQL: String =
        """
        WITH snapshot AS (SELECT CAST(:catalogId AS bigint) AS catalog_id, CAST(:snapshotId AS bigint) AS snapshot_id),
        $APPEND_ROWS_CTES
        SELECT $APPEND_ROWS_COUNTS
        """

    // ---- the read -----------------------------------------------------------

    /** One distinct (namespace, table) a commit names, in the order its refusals are decided. */
    data class TableKey(
        val namespace: String,
        val table: String,
        /** Appended to: its spec, live columns and next_row_id are read. */
        val appending: Boolean,
        /** A delete target of a guarded request: row-content changes conflict too. */
        val unchanged: Boolean,
    )

    /** What [READ_SQL] binds: the request's shape, nothing from the database. */
    class ReadInputs(
        val catalogId: Long,
        val idempotencyKey: UUID?,
        val tables: List<TableKey>,
        val readSnapshot: Long?,
        /** Every registered path, data and DV, distinct. */
        val paths: List<String>,
        /** The DV registrations' numeric targets (a transaction's staged targets are not read). */
        val targetIds: List<Long>,
    )

    /** hog_catalog's head-of-line state. */
    data class Head(
        val lastSnapshotId: Long,
        val schemaVersion: Long,
        val earliestSnapshotId: Long,
        val earliestSnapshotTime: Instant?,
        val nextFileId: Long,
    )

    /** A stored publication receipt; [fingerprint] null = a pre-V24 row compared through [request]. */
    class Receipt(
        val snapshotId: Long,
        val schemaVersion: Long,
        val fingerprint: ByteArray?,
        val request: String?,
    )

    /** A requested table that resolved live, with its live spec and allocator when it is appended to. */
    data class ResolvedTable(
        /** 1-based position in [ReadInputs.tables]. */
        val ord: Int,
        val tableId: Long,
        val tableUuid: UUID,
        val specId: Long?,
        val fieldCount: Int?,
        val nextRowId: Long?,
    )

    data class LiveColumn(val tableId: Long, val fieldId: Long, val colType: String)

    data class Conflict(val objectId: Long, val ddl: Boolean)

    data class RetainedPath(val path: String, val fileKind: String)

    data class Target(
        val dataFileId: Long,
        val tableId: Long,
        val recordCount: Long,
        val beginSnapshot: Long,
        val endSnapshot: Long?,
    )

    data class LiveDv(val dataFileId: Long, val deleteFileId: Long, val deleteCount: Long, val beginSnapshot: Long)

    /** Everything [READ_SQL] returned. Sections keyed by table are empty on a receipt hit. */
    class ReadSet(
        val head: Head,
        val receipt: Receipt?,
        /** By [ResolvedTable.ord]; a key with no entry did not resolve. More than one row is a broken invariant. */
        val tables: Map<Int, List<ResolvedTable>>,
        /** field_id -> col_type of each appended table's live columns. */
        val columns: Map<Long, Map<Long, String>>,
        val conflicts: List<Conflict>,
        /** The claims of [ReadInputs.paths], locked in upload_id order. */
        val claims: List<UploadClaim>,
        val retained: Set<Pair<String, String>>,
        /** Paths with an undrained hog_file_removal row. */
        val queued: List<String>,
        val targets: Map<Long, Target>,
        val liveDvs: Map<Long, LiveDv>,
    )

    /**
     * Binds [inputs] to a statement carrying [READ_SQL]'s parameters —
     * [READ_SQL] itself, or the EXPLAIN / EXECUTE of it a plan test runs.
     */
    internal fun bindRead(
        query: Query,
        inputs: ReadInputs,
    ): Query =
        query
            .bind("catalogId", inputs.catalogId)
            .bind("catalog", inputs.catalogId)
            .bindByType("idempotencyKey", inputs.idempotencyKey, UUID::class.java)
            .bindArray("namespaces", String::class.java, inputs.tables.map { it.namespace })
            .bindArray("names", String::class.java, inputs.tables.map { it.table })
            .bindArray("appending", Boolean::class.javaObjectType, inputs.tables.map { it.appending })
            .bindArray("unchanged", Boolean::class.javaObjectType, inputs.tables.map { it.unchanged })
            .bind("readSnapshot", inputs.readSnapshot)
            .bindArray("paths", String::class.java, inputs.paths)
            .bindArray("targetIds", LONG, inputs.targetIds)

    fun read(
        h: Handle,
        inputs: ReadInputs,
    ): ReadSet =
        bindRead(h.createQuery(READ_SQL), inputs)
            .map { rs, _ ->
                val receiptSnapshot = rs.getObject("receipt_snapshot_id") as Long?
                ReadSet(
                    head =
                        Head(
                            lastSnapshotId = rs.getLong("last_snapshot_id"),
                            schemaVersion = rs.getLong("schema_version"),
                            earliestSnapshotId = rs.getLong("earliest_snapshot_id"),
                            earliestSnapshotTime =
                                rs.getObject("earliest_snapshot_time", OffsetDateTime::class.java)?.toInstant(),
                            nextFileId = rs.getLong("next_file_id"),
                        ),
                    receipt =
                        receiptSnapshot?.let {
                            Receipt(
                                snapshotId = it,
                                schemaVersion = rs.getLong("receipt_schema_version"),
                                fingerprint = rs.getBytes("receipt_fingerprint"),
                                request = rs.getString("receipt_request"),
                            )
                        },
                    tables = section<ResolvedTable>(rs.getString("tables")).groupBy { it.ord },
                    columns =
                        section<LiveColumn>(rs.getString("columns"))
                            .groupBy({ it.tableId }) { it.fieldId to it.colType }
                            .mapValues { (_, types) -> types.toMap() },
                    conflicts = section(rs.getString("conflicts")),
                    claims = section(rs.getString("claims")),
                    retained =
                        section<RetainedPath>(rs.getString("retained")).mapTo(HashSet()) { it.path to it.fileKind },
                    queued = section(rs.getString("queued")),
                    targets = section<Target>(rs.getString("targets")).associateBy { it.dataFileId },
                    // One live DV per data file is a unique index; a second
                    // row is refused as the per-target `findOne` refused it,
                    // never resolved by whichever row came last.
                    liveDvs =
                        section<LiveDv>(rs.getString("live_dvs")).let { rows ->
                            rows.associateBy { it.dataFileId }.also {
                                check(it.size == rows.size) {
                                    "more than one live deletion vector for one data file in " +
                                        "catalog_id=${inputs.catalogId}"
                                }
                            }
                        },
                )
            }
            .findOne()
            .orElseThrow { IllegalStateException("no hog_catalog row for catalog_id=${inputs.catalogId}") }

    /** One JSON section of the read; json_agg over no rows is SQL NULL. */
    private inline fun <reified T> section(json: String?): List<T> =
        json?.let { readSetMapper.readValue<List<T>>(it) } ?: emptyList()

    // ---- the write ----------------------------------------------------------

    /** One append table's row-id allocator advance and gross rollup (hog_table_stats). */
    data class StatsAdvance(
        val tableId: Long,
        /** next_row_id as the read returned it: the UPDATE's guard. */
        val fromRowId: Long,
        val toRowId: Long,
        val records: Long,
        val bytes: Long,
    )

    data class DataFileRow(
        val dataFileId: Long,
        val tableId: Long,
        val path: String,
        val recordCount: Long,
        val fileSizeBytes: Long,
        val footerSize: Long?,
        val rowIdStart: Long,
        val statsState: String,
        val specId: Long?,
        val splitOffsets: List<Long>?,
    )

    data class PartitionValueRow(val dataFileId: Long, val keyIndex: Int, val value: String?)

    class ColumnStatsRow(val dataFileId: Long, val stats: ColumnStats)

    /** Every row an append writes, in the order the batches it replaced wrote them. */
    class AppendRows(
        val stats: List<StatsAdvance>,
        val files: List<DataFileRow>,
        val partitionValues: List<PartitionValueRow>,
        val columnStats: List<ColumnStatsRow>,
    )

    data class DeleteFileRow(
        val deleteFileId: Long,
        val tableId: Long,
        val dataFileId: Long,
        val path: String,
        val deleteCount: Long,
        val fileSizeBytes: Long,
    )

    /** The DVs a commit supersedes (by delete_file_id) and the ones it registers. */
    class DeleteRows(val ended: List<Long>, val files: List<DeleteFileRow>)

    /** What [WRITE_SQL] binds: every value computed, nothing left to decide. */
    class WriteSet(
        val catalogId: Long,
        /** last_snapshot_id as read: the snapshot published is head + 1. */
        val head: Long,
        /** next_file_id as read: appended files, then DVs, take ids from it. */
        val firstFileId: Long,
        val fileCount: Int,
        val schemaVersion: Long,
        val author: String?,
        val message: String?,
        /** (kind, table_id) change rows. */
        val changes: List<Pair<String, Long>>,
        val appends: AppendRows,
        val deletes: DeleteRows,
        val settleUploads: List<UUID>,
        val idempotencyKey: UUID?,
        val fingerprint: ByteArray?,
    )

    /** The counts [APPEND_SQL] selects, one per append CTE. */
    private val APPEND_COUNTS = listOf("table_stats", "data_files", "partition_values", "column_stats")

    /** The counts [WRITE_SQL] selects: the allocator's snapshot id (null when its guard matched no row), then one per CTE. */
    private val WRITE_COUNTS =
        listOf("snapshot_id", "changes") + APPEND_COUNTS + listOf("ended_dvs", "delete_files", "uploads", "receipts")

    /** What each CTE wrote, by name, off the statement's own counts. */
    private fun written(
        rs: java.sql.ResultSet,
        names: List<String>,
    ): Map<String, Long?> = names.associateWith { rs.getObject(it) as Long? }

    /** What each append CTE should write, by the names of [APPEND_COUNTS]. */
    private fun AppendRows.expected(): Map<String, Long?> =
        mapOf(
            "table_stats" to stats.size.toLong(),
            "data_files" to files.size.toLong(),
            "partition_values" to partitionValues.size.toLong(),
            "column_stats" to columnStats.size.toLong(),
        )

    /**
     * The failure for a write whose CTEs did not write what was asked:
     * each count that differs, and the cause only where the counts show
     * it — a guarded UPDATE that matched no row is a value the read
     * returned moving under the commit lock; any other short count is
     * reported as exactly that.
     */
    private fun shortWrite(
        what: String,
        catalogId: Long,
        written: Map<String, Long?>,
        expected: Map<String, Long?>,
        allocator: String? = null,
    ): String {
        val diff =
            expected.keys.filter { written[it] != expected[it] }
                .joinToString(", ") { "$it ${written[it]} (expected ${expected[it]})" }
        val cause =
            when {
                allocator != null && written["snapshot_id"] == null ->
                    "$allocator moved under the commit lock: its guarded UPDATE matched no row"
                written["table_stats"] != expected["table_stats"] ->
                    "a next_row_id moved under the commit lock: its guarded UPDATE matched no row"
                written["ended_dvs"] != expected["ended_dvs"] ->
                    "a DV it read as live was ended under the commit lock: its guarded UPDATE skipped it"
                else -> "its row counts disagree with the rows it was given"
            }
        return "$what for catalog_id=$catalogId wrote $diff; $cause"
    }

    /** Binds [set] to a statement carrying [WRITE_SQL]'s parameters, as [bindRead] does for the read. */
    internal fun bindWrite(
        query: Query,
        set: WriteSet,
    ): Query =
        query
            .bind("catalogId", set.catalogId)
            .bind("head", set.head)
            .bind("firstFileId", set.firstFileId)
            .bind("fileCount", set.fileCount)
            .bind("schemaVersion", set.schemaVersion)
            .bind("author", set.author)
            .bind("message", set.message)
            .bindArray("changeKinds", String::class.java, set.changes.map { it.first })
            .bindArray("changeTables", LONG, set.changes.map { it.second })
            .bindAppendRows(set.appends)
            .bindArray("endedDvs", LONG, set.deletes.ended)
            .bindArray("dvIds", LONG, set.deletes.files.map { it.deleteFileId })
            .bindArray("dvTables", LONG, set.deletes.files.map { it.tableId })
            .bindArray("dvTargets", LONG, set.deletes.files.map { it.dataFileId })
            .bindArray("dvPaths", String::class.java, set.deletes.files.map { it.path })
            .bindArray("dvCounts", LONG, set.deletes.files.map { it.deleteCount })
            .bindArray("dvBytes", LONG, set.deletes.files.map { it.fileSizeBytes })
            .bindArray("uploadIds", UUID::class.java, set.settleUploads)
            .bindByType("idempotencyKey", set.idempotencyKey, UUID::class.java)
            .bind("fingerprint", set.fingerprint)

    /**
     * Publish [set]: returns the snapshot id. Every CTE's row count is
     * checked against what was asked, and a short one is an
     * IllegalStateException that rolls the commit back — under the lock
     * the guards cannot miss, so a guard that misses is a writer outside
     * it ([shortWrite] says which).
     */
    fun write(
        h: Handle,
        set: WriteSet,
    ): Long {
        val written = bindWrite(h.createQuery(WRITE_SQL), set).map { rs, _ -> written(rs, WRITE_COUNTS) }.one()
        val expected =
            mapOf("snapshot_id" to set.head + 1, "changes" to set.changes.size.toLong()) +
                set.appends.expected() +
                mapOf(
                    "ended_dvs" to set.deletes.ended.size.toLong(),
                    "delete_files" to set.deletes.files.size.toLong(),
                    "uploads" to set.settleUploads.size.toLong(),
                    "receipts" to if (set.idempotencyKey != null) 1L else 0L,
                )
        check(written == expected) {
            shortWrite(
                "commit write",
                set.catalogId,
                written,
                expected,
                allocator = "the allocator (last_snapshot_id ${set.head}, next_file_id ${set.firstFileId} as read)",
            )
        }
        return set.head + 1
    }

    /** The append rows alone into the caller's [snapshotId] ([APPEND_SQL]), checked like [write]. */
    fun writeAppendRows(
        h: Handle,
        catalogId: Long,
        snapshotId: Long,
        rows: AppendRows,
    ) {
        val written =
            h.createQuery(APPEND_SQL)
                .bind("catalogId", catalogId)
                .bind("snapshotId", snapshotId)
                .bindAppendRows(rows)
                .map { rs, _ -> written(rs, APPEND_COUNTS) }
                .one()
        check(written == rows.expected()) { shortWrite("append rows", catalogId, written, rows.expected()) }
    }

    private fun <T : SqlStatement<T>> T.bindAppendRows(rows: AppendRows): T =
        bindArray("statsTables", LONG, rows.stats.map { it.tableId })
            .bindArray("statsFrom", LONG, rows.stats.map { it.fromRowId })
            .bindArray("statsTo", LONG, rows.stats.map { it.toRowId })
            .bindArray("statsRecords", LONG, rows.stats.map { it.records })
            .bindArray("statsBytes", LONG, rows.stats.map { it.bytes })
            .bindArray("fileIds", LONG, rows.files.map { it.dataFileId })
            .bindArray("fileTables", LONG, rows.files.map { it.tableId })
            .bindArray("filePaths", String::class.java, rows.files.map { it.path })
            .bindArray("fileRecords", LONG, rows.files.map { it.recordCount })
            .bindArray("fileBytes", LONG, rows.files.map { it.fileSizeBytes })
            .bindArray("fileFooters", LONG, rows.files.map { it.footerSize })
            .bindArray("fileRowIds", LONG, rows.files.map { it.rowIdStart })
            .bindArray("fileStates", String::class.java, rows.files.map { it.statsState })
            .bindArray("fileSpecs", LONG, rows.files.map { it.specId })
            .bindArray("fileSplits", String::class.java, rows.files.map { it.splitOffsetsLiteral() })
            .bindArray("partFiles", LONG, rows.partitionValues.map { it.dataFileId })
            .bindArray("partKeys", Int::class.javaObjectType, rows.partitionValues.map { it.keyIndex })
            .bindArray("partValues", String::class.java, rows.partitionValues.map { it.value })
            .bindArray("statFiles", LONG, rows.columnStats.map { it.dataFileId })
            .bindArray("statFields", LONG, rows.columnStats.map { it.stats.fieldId })
            .bindArray("statValues", LONG, rows.columnStats.map { it.stats.valueCount })
            .bindArray("statNulls", LONG, rows.columnStats.map { it.stats.nullCount })
            .bindArray("statNans", LONG, rows.columnStats.map { it.stats.nanCount })
            .bindArray("statSizes", LONG, rows.columnStats.map { it.stats.sizeBytes })
            .bindByteaArray("statLowers", rows.columnStats.map { it.stats.lowerBound })
            .bindByteaArray("statUppers", rows.columnStats.map { it.stats.upperBound })

    /**
     * [DataFileRow.splitOffsets] as a `bigint[]` literal, or null. A file's
     * offsets are an array and Postgres arrays are rectangular, so each
     * list rides in a `text[]` and the statement casts it back. Numbers
     * only, and a bound value either way.
     */
    private fun DataFileRow.splitOffsetsLiteral(): String? = splitOffsets?.joinToString(",", "{", "}")

    /** A `bytea[]` with null elements: JDBI has no array type for byte[], pgjdbc encodes byte[][] itself. */
    private fun <T : SqlStatement<T>> T.bindByteaArray(
        name: String,
        values: List<ByteArray?>,
    ): T =
        bind(
            name,
            Argument { position, statement, _ ->
                statement.setArray(position, statement.connection.createArrayOf("bytea", values.toTypedArray()))
            },
        )
}
