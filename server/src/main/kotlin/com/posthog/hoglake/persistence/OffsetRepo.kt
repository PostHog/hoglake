package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.ConsumerOffset
import com.posthog.hoglake.model.ConsumerTableOffset
import com.posthog.hoglake.model.HoglakeException
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.mapper.RowMapper
import org.jdbi.v3.core.statement.UnableToExecuteStatementException
import java.time.OffsetDateTime
import java.util.UUID

/** hog_consumer_offset: the per-consumer committed-offset log primitive. */
object OffsetRepo {
    private val offsetMapper =
        RowMapper { rs, _ ->
            ConsumerOffset(
                consumerId = rs.getString("consumer_id"),
                tableUuid = rs.getObject("table_uuid") as UUID,
                committedSnapshot = rs.getLong("committed_snapshot"),
                updatedAt = rs.getObject("updated_at", OffsetDateTime::class.java).toInstant(),
            )
        }

    /**
     * Monotonic upsert: the update only fires when the stored offset is
     * <= the new one, so a regression returns no row -> null. Equal
     * re-commits succeed (idempotent). A consumer_id failing the length
     * CHECK -> [HoglakeException.Validation].
     */
    fun upsert(
        handle: Handle,
        catalogId: Long,
        consumerId: String,
        tableUuid: UUID,
        snapshotId: Long,
    ): ConsumerOffset? =
        try {
            handle.createQuery(
                """
                INSERT INTO hog_consumer_offset
                    (catalog_id, consumer_id, table_uuid, committed_snapshot)
                VALUES (:catalogId, :consumerId, :tableUuid, :snapshotId)
                ON CONFLICT (catalog_id, consumer_id, table_uuid) DO UPDATE
                SET committed_snapshot = EXCLUDED.committed_snapshot,
                    updated_at = now()
                WHERE hog_consumer_offset.committed_snapshot <= EXCLUDED.committed_snapshot
                RETURNING consumer_id, table_uuid, committed_snapshot, updated_at
                """,
            )
                .bind("catalogId", catalogId)
                .bind("consumerId", consumerId)
                .bind("tableUuid", tableUuid)
                .bind("snapshotId", snapshotId)
                .map(offsetMapper)
                .findOne()
                .orElse(null)
        } catch (e: UnableToExecuteStatementException) {
            if (Pg.isCheckViolation(e)) {
                throw HoglakeException.Validation("invalid consumer_id (length must be 1..128)")
            }
            throw e
        }

    /**
     * Release-on-reconcile, the COMMIT path: the consumer has just
     * committed [snapshotId] on [tableUuid], so drop its rows on the
     * incarnations that one replaced.
     *
     * Atomic replacement (`CREATE OR REPLACE`, prepared replacement CTAS)
     * retires one incarnation and creates its successor with a NEW
     * table_uuid in one snapshot. The consumer notices the incarnation
     * change, reconciles, and from then on commits against the new uuid —
     * and its row on the retired uuid sits at whatever snapshot it last
     * reached, forever. Offsets deliberately survive a DROP so a consumer
     * can finish reading a dropped table, but nothing will ever advance
     * THIS row: the consumer has no reason to look at that uuid again. On
     * a `consumer_floor` catalog it is an eternal expiry floor.
     *
     * So the rows are DELETED, not advanced. Advancing would leave a row
     * naming an incarnation the consumer will never read again — the
     * misleading state `GET /consumers` exists to surface — and would
     * need advancing again after every sweep. Idempotent: a second call
     * deletes nothing.
     *
     * BACKWARD, from the row in hand. Walking forward here — seeding from
     * every offset the consumer holds and asking which have a reconciled
     * descendant — costs O(that consumer's offset count) on EVERY offset
     * commit, including on catalogs that have never had a replacement,
     * which is most of them. The committed table is known, so the walk
     * instead follows `replaced_table_id` back from it: a primary-key
     * point lookup on `(catalog_id, table_id)` per hop, O(chain length),
     * and typically zero hops because the column is NULL. No index beyond
     * the primary key is involved. [releaseSupersededOffsets] keeps the
     * forward all-consumer form for the expiry sweep, which has no row in
     * hand to start from.
     *
     * The gate is `snapshotId >= the committed incarnation's
     * created_snapshot`: reaching the incarnation at or past the snapshot
     * that created it is the consumer's own statement that it reconciled
     * across the replacement. An offset below that means it has not
     * finished, and the ancestors keep pinning.
     *
     * LINEAGE IS READ, NOT INFERRED. `hog_table.replaced_table_id` (V14)
     * is written by CatalogService.createTable. There is no
     * snapshot-equality fallback anywhere at runtime: `old.dropped_snapshot
     * = new.created_snapshot` is sound today, but nothing enforces it and
     * the cost of it being wrong is deleting a consumer's position on a
     * table it is still draining. V14's backfill uses that derivation
     * exactly once, over rows whose provenance it argues structurally.
     *
     * TERMINATION is `a.created_snapshot < l.created_snapshot` (strictly
     * DECREASING going backwards) rather than a hop cap. Each replacement
     * allocates a later snapshot than the incarnation it retires, so the
     * condition can never stop a legitimate walk — while a cycle, which
     * would need it to increase, cannot survive one hop. A fixed cap did
     * stop legitimate walks: an origin more than N replacements behind
     * became permanently unreleasable, which is the bug this exists to
     * fix. `hog_table_no_self_replacement` (V14) rules out the 1-cycle.
     */
    fun releaseAncestorsOf(
        handle: Handle,
        catalogId: Long,
        consumerId: String,
        tableUuid: UUID,
        snapshotId: Long,
    ): Int =
        handle.createUpdate(
            """
            WITH RECURSIVE ancestors AS (
                -- Seed: the incarnation just committed on, and only if the
                -- commit actually reached it.
                SELECT t.table_id, t.table_uuid, t.created_snapshot, t.replaced_table_id
                FROM hog_table t
                WHERE t.catalog_id = :catalogId
                  AND t.table_uuid = :tableUuid
                  AND :snapshotId >= t.created_snapshot
                UNION ALL
                -- One hop back along the RECORDED edge; primary-key lookup.
                SELECT a.table_id, a.table_uuid, a.created_snapshot, a.replaced_table_id
                FROM ancestors l
                JOIN hog_table a
                  ON a.catalog_id = :catalogId
                 AND a.table_id = l.replaced_table_id
                 -- Strictly decreasing going backwards; a cycle would have
                 -- to increase and so cannot survive one hop.
                 AND a.created_snapshot < l.created_snapshot
            )
            DELETE FROM hog_consumer_offset o
            WHERE o.catalog_id = :catalogId
              AND o.consumer_id = :consumerId
              AND o.table_uuid <> :tableUuid
              AND o.table_uuid IN (SELECT table_uuid FROM ancestors)
            """,
        )
            .bind("catalogId", catalogId)
            .bind("consumerId", consumerId)
            .bind("tableUuid", tableUuid)
            .bind("snapshotId", snapshotId)
            .execute()

    /**
     * Release-on-reconcile, the SWEEP path: the same rule applied to
     * every consumer in the catalog at once.
     *
     * The expiry sweep has no committed row to walk back from, so this is
     * the FORWARD form: seed from every offset naming a dropped
     * incarnation, follow `replaced_table_id` forward, and delete an
     * origin whose descendant carries an offset for the SAME consumer at
     * or past that descendant's `created_snapshot`. Same-consumer is
     * load-bearing, not incidental: one consumer's reconciliation says
     * nothing about another's, and dropping that predicate would delete a
     * lagging consumer's live position.
     *
     * The walk is recursive because a consumer that was down across two
     * replacements reconciles straight to the newest incarnation:
     * A -> B -> C with an offset only on C must release both A and B, or
     * the intermediate row pins the floor with nobody left to advance it.
     *
     * This is the ONLY caller of the forward seed, and it runs once per
     * sweep rather than once per commit — see [releaseAncestorsOf] for
     * why the commit path does not use it. The forward hop is what
     * `hog_table_replacement_lineage` (V14) indexes; termination and the
     * no-inference rule are as documented there.
     *
     * It exists alongside the commit-path release to clear rows stranded
     * by replicas that predate release-on-reconcile, so the sweep's floor
     * is honest rather than merely filtered — the row is GONE, so a later
     * sweep cannot rediscover it.
     */
    fun releaseSupersededOffsets(
        handle: Handle,
        catalogId: Long,
    ): Int = handle.createUpdate(RELEASE_ALL_CONSUMERS).bind("catalogId", catalogId).execute()

    /** Exposed for the V14 migration test, which EXPLAINs the real walk. */
    internal val RELEASE_ALL_CONSUMERS: String =
        """
        WITH RECURSIVE lineage AS (
            -- Seed: every offset row that names a DROPPED incarnation. A
            -- live incarnation is never superseded.
            SELECT o.consumer_id,
                   o.table_uuid       AS origin_uuid,
                   t.table_id,
                   t.table_uuid,
                   t.created_snapshot
            FROM hog_consumer_offset o
            JOIN hog_table t
              ON t.catalog_id = o.catalog_id AND t.table_uuid = o.table_uuid
            WHERE o.catalog_id = :catalogId
              AND t.dropped_snapshot IS NOT NULL
            UNION ALL
            -- One hop forward along the RECORDED edge.
            SELECT l.consumer_id, l.origin_uuid,
                   n.table_id, n.table_uuid, n.created_snapshot
            FROM lineage l
            JOIN hog_table n
              ON n.catalog_id = :catalogId
             AND n.replaced_table_id = l.table_id
             -- Strictly increasing along any real chain; a cycle would
             -- have to decrease and so cannot survive one hop.
             AND n.created_snapshot > l.created_snapshot
        )
        DELETE FROM hog_consumer_offset o
        WHERE o.catalog_id = :catalogId
          AND EXISTS (
              SELECT 1
              FROM lineage l
              JOIN hog_consumer_offset reconciled
                ON reconciled.catalog_id = :catalogId
               AND reconciled.consumer_id = l.consumer_id
               AND reconciled.table_uuid = l.table_uuid
              WHERE l.consumer_id = o.consumer_id
                AND l.origin_uuid = o.table_uuid
                AND l.table_uuid <> o.table_uuid
                AND reconciled.committed_snapshot >= l.created_snapshot
          )
        """

    fun find(
        handle: Handle,
        catalogId: Long,
        consumerId: String,
        tableUuid: UUID,
    ): ConsumerOffset? =
        handle.createQuery(
            """
            SELECT consumer_id, table_uuid, committed_snapshot, updated_at
            FROM hog_consumer_offset
            WHERE catalog_id = :catalogId AND consumer_id = :consumerId
              AND table_uuid = :tableUuid
            """,
        )
            .bind("catalogId", catalogId)
            .bind("consumerId", consumerId)
            .bind("tableUuid", tableUuid)
            .map(offsetMapper)
            .findOne()
            .orElse(null)

    /**
     * Every offset row in the catalog, enriched with the table's most
     * recent name (live version, or the last version before a drop —
     * offsets survive drops by design). Ordered for stable grouping:
     * consumer, then namespace.table.
     */
    fun listAll(
        handle: Handle,
        catalogId: Long,
    ): List<ConsumerTableOffset> =
        handle.createQuery(
            """
            SELECT o.consumer_id, o.table_uuid, o.committed_snapshot,
                   o.updated_at, ns.name AS namespace, tv.name AS table_name,
                   (t.dropped_snapshot IS NOT NULL) AS table_dropped
            FROM hog_consumer_offset o
            LEFT JOIN hog_table t
              ON t.catalog_id = o.catalog_id AND t.table_uuid = o.table_uuid
            LEFT JOIN LATERAL (
                SELECT v.name, v.namespace_id
                FROM hog_table_version v
                WHERE v.catalog_id = t.catalog_id AND v.table_id = t.table_id
                ORDER BY v.begin_snapshot DESC
                LIMIT 1
            ) tv ON true
            LEFT JOIN hog_namespace ns
              ON ns.catalog_id = o.catalog_id
             AND ns.namespace_id = tv.namespace_id
            WHERE o.catalog_id = :catalogId
            ORDER BY o.consumer_id, ns.name NULLS LAST, tv.name NULLS LAST,
                     o.table_uuid
            """,
        )
            .bind("catalogId", catalogId)
            .map { rs, _ ->
                ConsumerTableOffset(
                    consumerId = rs.getString("consumer_id"),
                    tableUuid = rs.getObject("table_uuid") as UUID,
                    committedSnapshot = rs.getLong("committed_snapshot"),
                    updatedAt =
                        rs.getObject("updated_at", OffsetDateTime::class.java).toInstant(),
                    namespace = rs.getString("namespace"),
                    tableName = rs.getString("table_name"),
                    tableDropped = rs.getBoolean("table_dropped"),
                )
            }
            .list()

    fun list(
        handle: Handle,
        catalogId: Long,
        consumerId: String,
    ): List<ConsumerOffset> =
        handle.createQuery(
            """
            SELECT consumer_id, table_uuid, committed_snapshot, updated_at
            FROM hog_consumer_offset
            WHERE catalog_id = :catalogId AND consumer_id = :consumerId
            ORDER BY table_uuid
            """,
        )
            .bind("catalogId", catalogId)
            .bind("consumerId", consumerId)
            .map(offsetMapper)
            .list()
}
