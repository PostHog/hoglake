package com.posthog.hoglake.service

import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.persistence.CatalogRepo
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import java.time.Instant
import java.util.UUID

data class UploadClaim(
    val uploadId: UUID,
    val owner: UUID,
    val prefix: String,
    val path: String,
    val fileKind: String,
    val state: String,
    val expiresAt: Instant,
)

/**
 * Durable ownership before PUT. Every transition serializes with publication and physical cleanup.
 *
 * None of the claim transitions take the per-catalog commit lock, and
 * they must not: a writer claims one upload per OUTPUT FILE, so a wide
 * INSERT took the lock that serializes the whole catalog's commit tail
 * once per file, with an unbounded wait (no lock_timeout, so no
 * backpressure either) — the one lock whose hold time is the catalog's
 * write throughput. It bought nothing. Claim paths are server-generated
 * random UUIDs, the claim INSERT is ON CONFLICT DO NOTHING, and the one
 * transition that must not race — [register], settling a claim as part
 * of a publication — runs inside the commit transaction and takes the
 * claim rows FOR UPDATE.
 *
 * What replaces the lock is row-level semantics: every UPDATE here
 * re-checks `state` in its WHERE clause, so under READ COMMITTED an
 * update that waited on [register]'s row lock re-evaluates the predicate
 * against the committed row and declines to clobber a settled claim.
 */
class UploadService(
    private val jdbi: Jdbi,
    /**
     * How long an abandoned tombstone rests between [scheduleExpired]
     * sweeps. Deliberately the DRAINED-LEDGER retention: a tombstone is
     * re-offered only once the removal ledger row from the previous
     * offer could have been purged, so the sweep cannot spin on the same
     * rows (CleanupService, HOGLAKE_REMOVAL_LEDGER_RETENTION_SECONDS).
     */
    private val ledgerRetentionSeconds: Long = CleanupService.LEDGER_RETENTION_SECONDS,
) {
    fun claim(
        catalog: String,
        id: UUID,
        owner: UUID,
        prefix: String,
        kind: String,
    ): UploadClaim =
        inCatalog(catalog, "upload_claim") { h, catalogId ->
            if (kind !in setOf("data", "delete")) throw HoglakeException.Validation("invalid upload file_kind")
            val normalized = prefix.trimEnd('/')
            val cat = CatalogRepo.require(h, catalog)
            val path = "$normalized/trino-upload/${UUID.randomUUID()}.${if (kind == "data") "parquet" else "puffin"}"
            val root = cat.dataPath.trimEnd('/') + "/"
            if (!path.startsWith(root) || path.any { it.isWhitespace() || it.isISOControl() } ||
                path.removePrefix(root).split('/').any { it.isEmpty() || it == "." || it == ".." }
            ) {
                throw HoglakeException.Validation("upload prefix must stay under the catalog data_path")
            }
            // The path is a fresh random UUID, so this INSERT races nothing;
            // ON CONFLICT DO NOTHING makes a retry of the SAME upload_id
            // idempotent, and the identity check below rejects a reuse that
            // changed the definition.
            h.createUpdate(
                """
                INSERT INTO hog_upload (catalog_id, upload_id, owner, prefix, path, file_kind)
                VALUES (:catalog, :id, :owner, :prefix, :path, :kind)
                ON CONFLICT (catalog_id, upload_id) DO NOTHING
            """,
            ).bind("catalog", catalogId).bind("id", id).bind("owner", owner)
                .bind("prefix", normalized).bind("path", path).bind("kind", kind).execute()
            val claim = load(h, catalogId, id)
            if (claim.owner != owner || claim.prefix != normalized || claim.fileKind != kind) {
                throw HoglakeException.CommitConflict("upload identity was reused with a different definition")
            }
            claim
        }

    /** Renewal may win against expiry, but can never revive a fenced claim. */
    fun renew(
        catalog: String,
        owner: UUID,
    ): Int =
        inCatalog(catalog, "upload_renew") { h, catalogId ->
            // state = 'active' is the fence, and it is re-evaluated after any
            // row-lock wait (READ COMMITTED), so a claim that a concurrent
            // publication settled as 'registered' — or a sweep fenced as
            // 'abandoned' — is never revived by a renewal that read it first.
            h.createUpdate(
                """
            UPDATE hog_upload SET expires_at = now() + interval '24 hours'
            WHERE catalog_id = :catalog AND owner = :owner AND state = 'active'
        """,
            ).bind("catalog", catalogId).bind("owner", owner).execute()
        }

    fun abandon(
        catalog: String,
        owner: UUID,
        paths: List<String>,
    ): Int =
        inCatalog(catalog, "upload_abandon") { h, catalogId ->
            if (paths.size > 10000) throw HoglakeException.Validation("too many upload paths")
            // Registered claims are immutable: abort cannot retract a publication with an unknown response.
            // The state predicate is what enforces that without the commit lock (see [renew]).
            h.createUpdate(
                """
            UPDATE hog_upload SET state = 'abandoned'
            WHERE catalog_id = :catalog AND owner = :owner AND path = ANY(:paths) AND state = 'active'
        """,
            ).bind("catalog", catalogId).bind("owner", owner).bindArray("paths", String::class.java, paths).execute()
        }

    /** Explicit operator action only. No new background sweep is enabled by this feature. */
    fun scheduleExpired(
        catalog: String,
        limit: Int = 1000,
    ): Int =
        com.posthog.hoglake.observability.Audit.audited("upload_schedule_expired", catalog, null) {
            if (limit !in 1..10000) throw HoglakeException.Validation("limit must be between 1 and 10000")
            val catalogId =
                jdbi.withHandleUnchecked { h -> CatalogRepo.require(h, catalog).catalogId }
            // Candidates: expired active claims, plus abandoned tombstones
            // that have RESTED. Re-offering every tombstone on every sweep
            // made the sweep's cost grow with the catalog's whole upload
            // history and re-queued paths whose previous removal row was
            // still in the ledger; resting them for the ledger retention
            // means a tombstone is re-offered only once the earlier row
            // could have been purged. (A PUT already in flight when the
            // claim was fenced can still land after an earlier DELETE, so
            // tombstones stay re-offerable — just not every sweep.)
            val claims =
                jdbi.withHandleUnchecked { h ->
                    h.createQuery(
                        """
                SELECT upload_id FROM hog_upload
                WHERE catalog_id = :catalog AND $CANDIDATE_PREDICATE
                ORDER BY last_scheduled_at NULLS FIRST, upload_id LIMIT :limit
            """,
                    ).bind("catalog", catalogId).bind("limit", limit)
                        .bind("retention", ledgerRetentionSeconds).mapTo(UUID::class.java).list()
                }
            // ONE SHORT TRANSACTION PER CANDIDATE, deliberately not one
            // transaction over all of them. Each fence takes that claim's
            // row lock, and a publication registering that very path waits
            // on it WHILE HOLDING the catalog commit lock — so a single
            // transaction spanning up to 10,000 rows would convoy the whole
            // catalog behind this sweep, which is the opposite of the point
            // of taking the commit lock off this path. Selection above is
            // still one query; a candidate that stops qualifying between
            // the two is rejected by the fence, not by the snapshot.
            claims.sumOf { id -> jdbi.inTransactionUnchecked { h -> fenceAndQueue(h, catalogId, id) } }
        }

    /**
     * Fence one claim and queue its path. Returns 1 if the claim was
     * fenced, 0 if it stopped qualifying.
     */
    private fun fenceAndQueue(
        h: Handle,
        catalogId: Long,
        id: UUID,
    ): Int {
        // Fence and read in ONE statement that RE-CHECKS the candidate
        // predicate — not merely `state <> 'registered'`. Under READ
        // COMMITTED this UPDATE waits on any concurrent transaction's row
        // lock and then re-evaluates its WHERE against the committed row,
        // which is what the catalog commit lock used to buy. Two things
        // can have happened since the candidate query: a publication
        // settled the claim ('registered'), or a RENEW pushed expires_at
        // out by 24 hours. Both must win — renewUploads promises a
        // renewal beats expiry, and abandoning a just-renewed claim would
        // queue a path whose writer is still going to register it.
        val fenced =
            h.createQuery(
                """
            UPDATE hog_upload SET state = 'abandoned', last_scheduled_at = now()
            WHERE catalog_id = :catalog AND upload_id = :id AND $CANDIDATE_PREDICATE
            RETURNING path, file_kind
        """,
            ).bind("catalog", catalogId).bind("id", id).bind("retention", ledgerRetentionSeconds)
                .map { rs, _ -> rs.getString("path") to rs.getString("file_kind") }
                .findOne().orElse(null) ?: return 0
        val (path, kind) = fenced
        // Queue membership is never authorization (CleanupService
        // liveness-checks every path at drain time), but a path a file row
        // already claims must not even be OFFERED: it would show up in the
        // drain as a still_referenced invariant violation, which is an
        // alert, every run, forever.
        h.createUpdate(QUEUE_ABANDONED_UPLOAD_SQL)
            .bind("catalog", catalogId).bind("path", path).bind("kind", kind).execute()
        return 1
    }

    private fun <T> inCatalog(
        catalog: String,
        action: String,
        block: (Handle, Long) -> T,
    ): T =
        com.posthog.hoglake.observability.Audit.audited(action, catalog, null) {
            jdbi.inTransactionUnchecked { h ->
                val catalogId = CatalogRepo.require(h, catalog).catalogId
                block(h, catalogId)
            }
        }

    companion object {
        /**
         * What makes a claim sweepable, used VERBATIM by both the
         * candidate query and the fence that re-checks it. One constant,
         * because the two drifting apart is the bug: the fence used to
         * check only `state <> 'registered'`, so a renewal that landed
         * between them was silently abandoned. A compile-time fragment
         * with no interpolated values (invariant 9 intact); :retention is
         * a bound parameter.
         */
        private const val CANDIDATE_PREDICATE =
            """
            ((state = 'active' AND expires_at <= now())
             OR (state = 'abandoned'
                 AND (last_scheduled_at IS NULL
                      OR last_scheduled_at <= now() - make_interval(secs => :retention))))
            """

        /**
         * The reclaim insert for a fenced upload claim: queue the
         * abandoned object's path, unless the queue already owns it or a
         * file row still claims it. `internal` so
         * `V16FileRemovalPathIndexMigrationIntegrationTest` can EXPLAIN
         * what production runs rather than a restatement of it.
         *
         * The FIRST `NOT EXISTS` is the one V16 indexes — it shares the
         * commit guard's `(catalog_id, path) WHERE drained_at IS NULL`
         * predicate, and rode the same sequential scan of the catalog's
         * whole queue (#199). It is a GUARD and never a constraint: two
         * concurrent sweeps can both pass it under READ COMMITTED, which
         * is one of the reasons `hog_file_removal_undrained_path` is not
         * unique.
         *
         * The other two probe `hog_data_file` / `hog_delete_file` by
         * path, and V17 indexes those as well —
         * `hog_data_file_path` / `hog_delete_file_path` (catalog_id,
         * path). They were deliberately sequential until then ("a path
         * index there would be paid for by every commit, on the hottest
         * insert in the system, to serve an hourly read"), and what
         * re-decided it was finding the SAME scan on the cleanup
         * drain's commit-lock hold, once per sub-batch, rather than
         * hourly.
         *
         * Binds `:catalog`, `:path`, `:kind`. No interpolated values
         * (invariant 9 intact).
         */
        internal const val QUEUE_ABANDONED_UPLOAD_SQL: String =
            """
            INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
            SELECT :catalog, :path, :kind, 'trino_upload'
            WHERE NOT EXISTS (SELECT 1 FROM hog_file_removal
                    WHERE catalog_id = :catalog AND path = :path AND drained_at IS NULL)
              AND NOT EXISTS (SELECT 1 FROM hog_data_file
                    WHERE catalog_id = :catalog AND path = :path)
              AND NOT EXISTS (SELECT 1 FROM hog_delete_file
                    WHERE catalog_id = :catalog AND path = :path)
            """

        private val claimMapper =
            org.jdbi.v3.core.mapper.RowMapper { rs, _ ->
                UploadClaim(
                    rs.getObject("upload_id", UUID::class.java),
                    rs.getObject("owner", UUID::class.java),
                    rs.getString("prefix"),
                    rs.getString("path"),
                    rs.getString("file_kind"),
                    rs.getString("state"),
                    rs.getTimestamp("expires_at").toInstant(),
                )
            }

        private fun load(
            h: Handle,
            catalogId: Long,
            id: UUID,
        ): UploadClaim =
            h.createQuery(
                """
            SELECT upload_id, owner, prefix, path, file_kind, state, expires_at FROM hog_upload
            WHERE catalog_id = :catalog AND upload_id = :id
        """,
            ).bind("catalog", catalogId).bind("id", id).map(claimMapper).one()

        /** Called in the same transaction/lock as file registration and durable publication receipts. */
        internal fun register(
            h: Handle,
            catalogId: Long,
            owner: UUID?,
            files: List<Pair<String, String>>,
        ) {
            if (files.isEmpty()) return
            val kinds = files.toMap()
            if (files.any { (path, kind) -> kinds[path] != kind }) {
                throw HoglakeException.Validation("one upload path cannot hold both data and deletion vectors")
            }
            // FOR UPDATE: the claim transitions no longer serialize on the
            // catalog commit lock, so this read-then-settle pair holds the
            // rows itself. Without it, abort/expiry could flip a row to
            // 'abandoned' between the state check below and the UPDATE at
            // the end, and the UPDATE — keyed on upload_id alone — would
            // silently revive it.
            val claims =
                h.createQuery(
                    """
                SELECT upload_id, owner, prefix, path, file_kind, state, expires_at
                FROM hog_upload WHERE catalog_id = :catalog AND path = ANY(:paths)
                FOR UPDATE
            """,
                ).bind("catalog", catalogId).bindArray("paths", String::class.java, kinds.keys)
                    .map(claimMapper).list()
            // Preserve the existing catalog contract: immutable objects still referenced
            // at a retained snapshot may be referenced again. Ownership never permits
            // rewriting them, and an expired object with no retained reference stays fenced.
            val retained =
                h.createQuery(
                    """
                SELECT path, 'data' AS file_kind FROM hog_data_file
                WHERE catalog_id = :catalog AND path = ANY(:paths)
                UNION
                SELECT path, 'delete' AS file_kind FROM hog_delete_file
                WHERE catalog_id = :catalog AND path = ANY(:paths)
            """,
                ).bind("catalog", catalogId).bindArray(
                    "paths",
                    String::class.java,
                    claims.filter { it.state == "registered" }.map { it.path },
                )
                    .map { rs, _ -> rs.getString("path") to rs.getString("file_kind") }.list().toSet()
            for (claim in claims) {
                if (claim.state == "registered" && claim.fileKind == kinds[claim.path] &&
                    (claim.path to claim.fileKind) in retained
                ) {
                    continue
                }
                if (claim.state != "active" || claim.owner != owner || claim.fileKind != kinds[claim.path]) {
                    throw HoglakeException.CommitConflict(
                        "upload is fenced, already registered, or belongs to another operation",
                    )
                }
            }
            h.createUpdate(
                """
                UPDATE hog_upload SET state = 'registered'
                WHERE catalog_id = :catalog AND upload_id = ANY(:ids)
            """,
            ).bind("catalog", catalogId).bindArray(
                "ids",
                UUID::class.java,
                claims.filter {
                    it.state == "active"
                }.map { it.uploadId },
            ).execute()
        }
    }
}
