package com.posthog.hoglake.compaction

import org.jdbi.v3.core.Handle
import java.security.MessageDigest
import java.util.UUID

/**
 * `hog_compaction_claim` (V15): the lease one maintainer takes on a
 * compaction group so a second maintainer's planner can skip it.
 *
 * # A claim is an optimization, never authorization
 *
 * Compaction's correctness against a concurrent rewrite is, and stays,
 * [CompactionService]'s plan-to-commit re-verification under the
 * per-catalog commit lock: every input still live, carrying exactly its
 * planned DV, and the staging ticket still unsettled. Nothing in this
 * file is consulted there, and nothing here may be read as permission
 * to commit. Truncate the table at any moment and the only consequence
 * is that two replicas waste object-store work on the same group again
 * — which is precisely the production shape it exists to remove (a
 * 547 s sweep on gigahog-prod-us committed 34 groups and lost 30 to the
 * other replica's commits).
 *
 * That ordering matters for reviews as much as for runtime: a change
 * that makes the commit path trust a claim turns an advisory lease with
 * a wall-clock expiry into a correctness dependency on two clocks.
 *
 * # Identity is the WORK, not the worker
 *
 * [groupKey] is a hex SHA-256 over the group's spec id, its partition
 * values and its sorted input `data_file_id`s. Two replicas planning
 * from the same catalog metadata form the same group and compute the
 * same key with no coordination, which is what lets a plain
 * `INSERT ... ON CONFLICT` be the whole protocol.
 *
 * The planner's skip is nevertheless by **overlap**, not by key
 * equality ([liveClaimedFileIds]). Bin packing is a function of the
 * candidate set, so a replica that plans a moment later — after one
 * more ingest file lands — packs groups with different keys over mostly
 * the same files, and an exact-match skip would let exactly the
 * original race back in. `input_file_ids` is stored for that read.
 *
 * # Expiry is the only liveness protocol
 *
 * There is no heartbeat, deliberately. A claim is a lease
 * (`HOGLAKE_COMPACTION_CLAIM_TTL_SECONDS`) and a maintainer that dies
 * mid-rewrite costs one TTL of delay on those files and nothing else:
 * [acquire]'s `ON CONFLICT ... WHERE expires_at <= now()` arm reclaims
 * an expired row in the same statement that fails to insert over a live
 * one, and [purgeExpired] clears the rest at the head of every sweep.
 * A heartbeat would buy a shorter TTL at the price of a second failure
 * mode (a live maintainer whose renewals are starved losing a claim it
 * is working), for a lease nothing depends on for correctness.
 */
internal object CompactionClaimRepo {
    /**
     * The group's identity: spec id, partition values and sorted input
     * file ids, hashed.
     *
     * SORTED file ids, because the two replicas' planners may emit a
     * group's files in different orders if the candidate query's tie
     * break ever moves; the SET is the identity. Partition values are
     * length-prefixed rather than joined with a separator so a value
     * containing the separator cannot collide with two values that do
     * not, and a null value is distinguishable from an empty one.
     */
    fun groupKey(group: CompactionGroup): String {
        val digest = MessageDigest.getInstance("SHA-256")

        fun feed(s: String) {
            val bytes = s.toByteArray(Charsets.UTF_8)
            digest.update(bytes.size.toString().toByteArray(Charsets.UTF_8))
            digest.update(':'.code.toByte())
            digest.update(bytes)
        }
        feed("spec=${group.specId ?: "none"}")
        feed("values=${group.partitionValues?.size ?: -1}")
        group.partitionValues?.forEach { feed(if (it == null) "null:" else "v:$it") }
        group.files.map { it.dataFileId }.sorted().forEach { feed("f$it") }
        return digest.digest().joinToString("") { "%02x".format(it) }
    }

    /**
     * The planner's read, as a named constant so the migration test can
     * EXPLAIN the statement this code ISSUES rather than a hand-written
     * lookalike. V14's first index was on the wrong column and its test
     * was green because it explained a predicate nothing sends.
     */
    internal const val LIVE_CLAIMS_SQL: String =
        """
        SELECT input_file_ids
        FROM hog_compaction_claim
        WHERE catalog_id = :catalogId AND table_id = :tableId
          AND expires_at > now()
        """

    /** The per-sweep bulk purge, exposed for the same reason. */
    internal const val PURGE_EXPIRED_SQL: String =
        "DELETE FROM hog_compaction_claim WHERE catalog_id = :catalogId AND expires_at <= now()"

    /**
     * Every file id under a LIVE claim for this table — the planner's
     * skip set.
     *
     * Read on the planner's own REPEATABLE READ snapshot, so a claim
     * taken after that snapshot is invisible here and the group is
     * planned anyway. That is not a hole to close: the two maintainers
     * then race exactly as they do today, and the commit-time
     * re-verification resolves it. Closing it would mean taking the
     * claim inside planning, which is a write inside a read-only
     * transaction that exists to give the plan a consistent view.
     */
    fun liveClaimedFileIds(
        h: Handle,
        catalogId: Long,
        tableId: Long,
    ): Set<Long> {
        val rows =
            h.createQuery(LIVE_CLAIMS_SQL)
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .map { rs, _ -> (rs.getArray("input_file_ids").array as Array<*>).map { (it as Number).toLong() } }
                .list()
        return rows.flatten().toSet()
    }

    /**
     * Take the claim, or report that someone else holds a live one.
     *
     * One statement: the insert lands when no row exists, the `DO
     * UPDATE ... WHERE expires_at <= now()` arm takes over an EXPIRED
     * row, and a live row matches neither, updates nothing and returns
     * no row. `RETURNING` is therefore the answer — true means the
     * claim is ours, false means skip the group. No lock, no read
     * before the write, so two replicas racing on the same key resolve
     * on the primary key's own arbitration.
     */
    fun acquire(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        groupKey: String,
        inputFileIds: List<Long>,
        claimant: UUID,
        ttlSeconds: Long,
    ): Boolean =
        h.createQuery(
            """
            INSERT INTO hog_compaction_claim
                   (catalog_id, table_id, group_key, input_file_ids, claimant, claimed_at, expires_at)
            VALUES (:catalogId, :tableId, :groupKey, :inputFileIds, :claimant, now(),
                    now() + make_interval(secs => :ttlSeconds))
            ON CONFLICT (catalog_id, table_id, group_key) DO UPDATE
               SET claimant = EXCLUDED.claimant,
                   claimed_at = now(),
                   input_file_ids = EXCLUDED.input_file_ids,
                   expires_at = EXCLUDED.expires_at
             WHERE hog_compaction_claim.expires_at <= now()
            RETURNING 1
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("groupKey", groupKey)
            .bindArray("inputFileIds", Long::class.javaObjectType, inputFileIds)
            .bind("claimant", claimant)
            .bind("ttlSeconds", ttlSeconds.toDouble())
            .mapTo(Int::class.javaObjectType)
            .findOne()
            .isPresent

    /**
     * Drop our own claim, whatever the group's outcome was.
     *
     * `claimant` is in the predicate so a release can never delete a
     * claim some other process reclaimed after ours expired mid-rewrite
     * — that process is working those files now, and removing its lease
     * would reintroduce the duplicate rewrite one TTL late.
     */
    fun release(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        groupKey: String,
        claimant: UUID,
    ): Int =
        h.createUpdate(
            """
            DELETE FROM hog_compaction_claim
            WHERE catalog_id = :catalogId AND table_id = :tableId
              AND group_key = :groupKey AND claimant = :claimant
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("groupKey", groupKey)
            .bind("claimant", claimant)
            .execute()

    /**
     * Cut our own claim's lease down to [ttlSeconds] because its group
     * COMMITTED.
     *
     * The row is deliberately not deleted — see
     * `CompactionService.executeGroup`'s release: a sibling maintainer's
     * plan formed before the commit still names these (now dead) inputs,
     * and the row is what turns its arrival into a counted skip instead
     * of a wasted rewrite. But that reader is a plan at most one sweep
     * old, so the row's useful life is sweep intervals rather than the
     * full rewrite lease. Holding the full lease instead leaves roughly
     * `committed groups per sweep x lease / interval` rows per table for
     * the planner to read the `input_file_ids` of on every pass.
     *
     * `claimant` is in the predicate for the same reason [release] has
     * it: after our own lease expired, another maintainer may hold this
     * key, and shortening ITS lease would be the one thing a release
     * must never do.
     */
    fun shortenToCommitted(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        groupKey: String,
        claimant: UUID,
        ttlSeconds: Long,
    ): Int =
        h.createUpdate(
            """
            UPDATE hog_compaction_claim
               SET expires_at = now() + make_interval(secs => :ttlSeconds)
             WHERE catalog_id = :catalogId AND table_id = :tableId
               AND group_key = :groupKey AND claimant = :claimant
            """,
        )
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("groupKey", groupKey)
            .bind("claimant", claimant)
            .bind("ttlSeconds", ttlSeconds.toDouble())
            .execute()

    /**
     * Clear every expired claim for one catalog — the head of each
     * sweep.
     *
     * [acquire]'s reclaim arm already handles the row a re-planned group
     * lands on; this covers the rest, which nothing would ever look at
     * again: a group whose files were compacted by the other replica is
     * never re-planned, so its abandoned claim would sit in the table
     * forever. `compaction_claims` in `/verify` is what reds if this
     * stops running.
     */
    fun purgeExpired(
        h: Handle,
        catalogId: Long,
    ): Int =
        h.createUpdate(PURGE_EXPIRED_SQL)
            .bind("catalogId", catalogId)
            .execute()
}
