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
 * same key. `INSERT ... ON CONFLICT` arbitrates each key. A separate
 * catalog planning lock prevents overlapping plans from reserving
 * different keys for the same input files.
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
 *
 * # Bucket claims: the same table, a second kind of row
 *
 * A group claim is found by OVERLAP with a plan's packed groups, so a
 * sibling can only honour it AFTER it has probed the bucket and read
 * every candidate row — which is exactly the work that multiplied on
 * gigahog-prod-us on 2026-10-21. Nine maintenance pods planned
 * millpond-prod-us one after another under the catalog planning lock,
 * each probing the same 123 buckets (`candidates_fetched` 50,029 every
 * time) and discarding its predecessors' groups (`claimed_elsewhere`
 * 0, 64, 128, ... 320), and the probes were millpond's day buckets —
 * one-key arms over catalog-wide day values, the expensive regime —
 * which then crossed the 15 s plan timeout on all nine at once.
 *
 * The planner knows a bucket's TUPLE before it probes, not its files,
 * so the skip it needs is keyed by the tuple. Three ways to get one:
 *
 *  - a check after the probe and before packing saves nothing: the
 *    probe IS the cost;
 *  - an `EXISTS` over the bucket's sampled file ids would be cheap, but
 *    `hog_maintenance_summary_tier` carries per-bucket measures only,
 *    no file ids, and adding them is a per-file column on the sampler's
 *    hottest table;
 *  - a per-bucket marker row. A new table would need a migration and a
 *    second purge for no new semantics, because THIS table already is
 *    "a lease on some work, keyed by its identity, scoped to (catalog,
 *    table), expired by wall clock, released by its claimant". So the
 *    marker is a row here: [bucketKey] (`bucket:` + a hash over spec id
 *    and partition values, which can never equal a 64-hex [groupKey])
 *    and an EMPTY `input_file_ids` — admissible under V15's
 *    `NOT NULL`, and invisible to [liveClaimedFileIds]' overlap set.
 *
 * Reuse buys [acquire]'s arbitration and expired-row takeover,
 * [release]'s claimant fence and [purgeExpired] for nothing, and adds
 * no migration. A marker is taken only for a bucket whose every packed
 * group this plan claimed (so a bucket with work left over for a
 * sibling is not hidden from it) and released when the last of those
 * groups settles; see `CompactionService.planAndClaim`. Its lease is
 * SHORT (`CompactionConfig.bucketClaimTtlSeconds`, 600 s, against the
 * group claims' hour) and renewed by [refresh] before each of its
 * groups' IO, because a pod killed mid-sweep releases nothing and a
 * leaked bucket claim hides a whole bucket, new files included. It is the same
 * OPTIMIZATION a group claim is: a sibling that ignored it would probe
 * the bucket, find its groups by overlap and drop them as
 * `claimed_elsewhere`, which is the pre-marker behaviour.
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
    fun groupKey(group: CompactionGroup): String =
        tupleDigest(group.specId, group.partitionValues) { feed ->
            group.files.map { it.dataFileId }.sorted().forEach { feed("f$it") }
        }

    /** The prefix that marks a bucket claim; see the class KDoc. */
    const val BUCKET_KEY_PREFIX: String = "bucket:"

    /**
     * A bucket claim's key: [BUCKET_KEY_PREFIX] and a hex SHA-256 over
     * the bucket's spec id and partition values, fed exactly as
     * [groupKey] feeds them (length-prefixed, null distinct from empty)
     * and with no file ids. The prefix is what keeps the two key spaces
     * disjoint — a group key is 64 hex characters and nothing else.
     */
    fun bucketKey(
        specId: Long?,
        partitionValues: List<String?>?,
    ): String {
        // The prefix IS the type tag that keeps the two kinds of row apart
        // (the overlap read, the pre-probe read, anything that counts
        // groups); an empty one would make every bucket claim look like a
        // group claim to a reader that filters on it.
        require(BUCKET_KEY_PREFIX.isNotEmpty()) { "BUCKET_KEY_PREFIX must be non-empty" }
        val key = BUCKET_KEY_PREFIX + tupleDigest(specId, partitionValues) { }
        check(key.startsWith(BUCKET_KEY_PREFIX) && key.length == BUCKET_KEY_PREFIX.length + 64)
        return key
    }

    private fun tupleDigest(
        specId: Long?,
        partitionValues: List<String?>?,
        rest: ((String) -> Unit) -> Unit,
    ): String {
        val digest = MessageDigest.getInstance("SHA-256")

        fun feed(s: String) {
            val bytes = s.toByteArray(Charsets.UTF_8)
            digest.update(bytes.size.toString().toByteArray(Charsets.UTF_8))
            digest.update(':'.code.toByte())
            digest.update(bytes)
        }
        feed("spec=${specId ?: "none"}")
        feed("values=${partitionValues?.size ?: -1}")
        partitionValues?.forEach { feed(if (it == null) "null:" else "v:$it") }
        rest(::feed)
        return digest.digest().joinToString("") { "%02x".format(it) }
    }

    /**
     * The pre-probe read: every LIVE bucket claim's key for one table.
     *
     * Rides the primary key's `(catalog_id, table_id)` prefix, as
     * [LIVE_CLAIMS_SQL] does, with the prefix and the expiry as filters
     * on the rows it reaches; the rows are this table's live claims,
     * which the queue bound (`maxGroupsPerRun` group claims plus at most
     * one marker per claimed bucket, per concurrent run) keeps small.
     */
    internal const val LIVE_BUCKET_CLAIMS_SQL: String =
        """
        SELECT group_key
        FROM hog_compaction_claim
        WHERE catalog_id = :catalogId AND table_id = :tableId
          AND expires_at > now()
          AND starts_with(group_key, :prefix)
        """

    /** The keys of every live bucket claim on this table ([bucketKey]s). */
    fun liveBucketKeys(
        h: Handle,
        catalogId: Long,
        tableId: Long,
    ): Set<String> =
        h.createQuery(LIVE_BUCKET_CLAIMS_SQL)
            .bind("catalogId", catalogId)
            .bind("tableId", tableId)
            .bind("prefix", BUCKET_KEY_PREFIX)
            .mapTo(String::class.java)
            .set()

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
     * Read after packing. Execution plans hold the catalog planning lock
     * until they claim all selected groups. Metadata-only previews do not
     * take the lock or create claims.
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
     * Refresh only a live claim that this run owns. An expired or replaced claim must stop execution.
     *
     * With [bucketKey], the group's BUCKET claim is renewed in the same
     * statement on its own [bucketTtlSeconds] lease — the renewal that
     * keeps a short-leased bucket claim alive exactly as long as its
     * queue is being worked (`CompactionConfig.bucketClaimTtlSeconds`).
     * Same fences for both rows: ours, and still live. An expired bucket
     * claim is NOT revived, because a sibling may already have read the
     * bucket as free; reviving it would hide a bucket that sibling is now
     * planning. The answer is the GROUP row's alone.
     */
    fun refresh(
        h: Handle,
        catalogId: Long,
        tableId: Long,
        groupKey: String,
        claimant: UUID,
        ttlSeconds: Long,
        bucketKey: String? = null,
        bucketTtlSeconds: Long = ttlSeconds,
    ): Boolean =
        groupKey in
            h.createQuery(
                """
                UPDATE hog_compaction_claim
                   SET expires_at = now() + make_interval(
                           secs => CASE WHEN group_key = :groupKey THEN :ttlSeconds ELSE :bucketTtlSeconds END)
                 WHERE catalog_id = :catalogId AND table_id = :tableId
                   AND group_key = ANY(:keys) AND claimant = :claimant
                   AND expires_at > now()
                RETURNING group_key
                """,
            )
                .bind("catalogId", catalogId)
                .bind("tableId", tableId)
                .bind("groupKey", groupKey)
                .bindArray("keys", String::class.java, listOfNotNull(groupKey, bucketKey))
                .bind("claimant", claimant)
                .bind("ttlSeconds", ttlSeconds.toDouble())
                .bind("bucketTtlSeconds", bucketTtlSeconds.toDouble())
                .mapTo(String::class.java)
                .list()

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
     * forever. The verify subsystem's `compaction_claims` check was what redded if this
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
