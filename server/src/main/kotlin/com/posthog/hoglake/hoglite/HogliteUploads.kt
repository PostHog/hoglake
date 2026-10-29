package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.service.UploadBackend
import com.posthog.hoglake.service.UploadClaim
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.UUID

/**
 * [UploadBackend] on hoglite: the claim state machine of
 * [com.posthog.hoglake.service.UploadService] over the tenant's
 * `uploads` table.
 *
 * The Postgres original relies on row locks and READ COMMITTED
 * re-checks; a hoglite tenant executes one transaction at a time, so
 * the same fences are plain WHERE clauses inside a serial transaction
 * and the lock-ordering commentary does not port (there is nothing to
 * deadlock with). What ports unchanged: claim identity is pinned to
 * the upload id (reuse with a different owner/prefix/kind is a 409),
 * a claim lives 24 hours between renewals, renewal only touches
 * 'active' rows (never revives a fenced one), abandonment cannot
 * retract a settled claim, and the expiry sweep fences a claim and
 * queues its path in one transaction per candidate.
 *
 * The claim/commit rendezvous (a publication settling the claims its
 * paths carry) lives in [HogliteCommit]'s `decideClaims` — the claim
 * owner is the commit's own idempotency key on both backends.
 */
internal class HogliteUploads(
    private val store: HogliteCatalogStore,
    private val ledgerRetentionSeconds: Long,
) : UploadBackend {
    override fun claim(
        catalog: String,
        id: UUID,
        owner: UUID,
        prefix: String,
        kind: String,
    ): UploadClaim {
        if (kind !in setOf("data", "delete")) throw HoglakeException.Validation("invalid upload file_kind")
        val normalized = prefix.trimEnd('/')
        // meta read: the catalog 404 and the data_path the prefix must stay under.
        val dataPath = store.meta(catalog).dataPath
        val path = "$normalized/trino-upload/${UUID.randomUUID()}.${if (kind == "data") "parquet" else "puffin"}"
        val root = dataPath.trimEnd('/') + "/"
        if (!path.startsWith(root) || path.any { it.isWhitespace() || it.isISOControl() } ||
            path.removePrefix(root).split('/').any { it.isEmpty() || it == "." || it == ".." }
        ) {
            throw HoglakeException.Validation("upload prefix must stay under the catalog data_path")
        }
        val now = Instant.now()
        val expires = now.plus(24, ChronoUnit.HOURS)
        // The path is a fresh random UUID, so this races nothing;
        // INSERT OR IGNORE makes a retry of the SAME upload_id
        // idempotent, and the identity check below rejects a reuse
        // that changed the definition.
        store.runTx(
            catalog,
            HogliteCatalogStore.freshTxid("upload-claim"),
            null,
            HogliteTx().apply {
                add(
                    "INSERT OR IGNORE INTO uploads(upload_id, owner, prefix, file_kind, path, state, " +
                        "claimed_at, expires_at) VALUES (?, ?, ?, ?, ?, 'active', ?, ?)",
                    id.toString(),
                    owner.toString(),
                    normalized,
                    kind,
                    path,
                    HogliteSchema.fmt(now),
                    HogliteSchema.fmt(expires),
                )
            },
        )
        val claim = load(catalog, id)
        if (claim.owner != owner || claim.prefix != normalized || claim.fileKind != kind) {
            throw HoglakeException.CommitConflict("upload identity was reused with a different definition")
        }
        return claim
    }

    /** Renewal may win against expiry, but can never revive a fenced claim. */
    override fun renew(
        catalog: String,
        owner: UUID,
    ): Int {
        store.meta(catalog)
        val expires = HogliteSchema.fmt(Instant.now().plus(24, ChronoUnit.HOURS))
        val tx = HogliteTx()
        val idx =
            tx.add(
                "UPDATE uploads SET expires_at = ? WHERE owner = ? AND state = 'active'",
                expires,
                owner.toString(),
            )
        return store.runTx(catalog, HogliteCatalogStore.freshTxid("upload-renew"), null, tx)[idx]
            .affected.toInt()
    }

    override fun abandon(
        catalog: String,
        owner: UUID,
        paths: List<String>,
    ): Int {
        store.meta(catalog)
        if (paths.size > 10000) throw HoglakeException.Validation("too many upload paths")
        if (paths.isEmpty()) return 0
        // Registered claims are immutable: abort cannot retract a
        // publication with an unknown response. The state predicate
        // enforces that.
        val tx = HogliteTx()
        val idx =
            tx.add(
                "UPDATE uploads SET state = 'abandoned' " +
                    "WHERE owner = ? AND state = 'active' AND path IN (${paths.joinToString(",") { "?" }})",
                owner.toString(),
                *paths.toTypedArray(),
            )
        return store.runTx(catalog, HogliteCatalogStore.freshTxid("upload-abandon"), null, tx)[idx]
            .affected.toInt()
    }

    /**
     * Fence expired active claims and rested abandoned tombstones, one
     * transaction per candidate (the Postgres path's reason — not
     * convoying the catalog behind a 10,000-row sweep — is moot under
     * a serial tenant, but the per-candidate re-check still is not: a
     * renewal or publication between the candidate query and the fence
     * must win).
     */
    override fun scheduleExpired(
        catalog: String,
        limit: Int,
    ): Int {
        if (limit !in 1..10000) throw HoglakeException.Validation("limit must be between 1 and 10000")
        store.meta(catalog)
        val now = HogliteSchema.now()
        val restBefore = HogliteSchema.fmt(Instant.now().minusSeconds(ledgerRetentionSeconds))
        val candidates =
            store.queryCatalog(
                catalog,
                "SELECT upload_id FROM uploads " +
                    "WHERE (state = 'active' AND expires_at <= ?) " +
                    "OR (state = 'abandoned' AND (scheduled_at IS NULL OR scheduled_at <= ?)) " +
                    "ORDER BY upload_id LIMIT ?",
                now,
                restBefore,
                limit.toLong(),
            ).rows.map { it[0].asString() }
        return candidates.sumOf { id -> fenceAndQueue(catalog, id, now, restBefore) }
    }

    /** Fence one claim and queue its path; 1 if fenced, 0 if it stopped qualifying. */
    private fun fenceAndQueue(
        catalog: String,
        id: String,
        now: String,
        restBefore: String,
    ): Int {
        val tx = HogliteTx()
        val fence =
            tx.add(
                "UPDATE uploads SET state = 'abandoned', scheduled_at = ? " +
                    "WHERE upload_id = ? AND " +
                    "((state = 'active' AND expires_at <= ?) " +
                    "OR (state = 'abandoned' AND (scheduled_at IS NULL OR scheduled_at <= ?)))",
                now,
                id,
                now,
                restBefore,
            )
        // Queue membership is never authorization (cleanup liveness-
        // checks every path at drain time), but a path a file row
        // already claims must not even be OFFERED.
        // The queue insert is gated on THIS transaction's fence
        // (scheduled_at = now is the fence's own write): a renewal that
        // won the race leaves the claim active with its old scheduled_at,
        // the fence UPDATE affects nothing, and no removal row may be
        // queued for a live claim.
        tx.add(
            """INSERT INTO removals(path, file_kind, reason, queued_snapshot, queued_at)
               SELECT u.path, u.file_kind, 'trino_upload',
                      (SELECT next_snapshot - 1 FROM meta WHERE id = 1), ?
                 FROM uploads u
                WHERE u.upload_id = ? AND u.scheduled_at = ?
                  AND NOT EXISTS (SELECT 1 FROM removals r WHERE r.path = u.path AND r.drained_at IS NULL)
                  AND NOT EXISTS (SELECT 1 FROM files f WHERE f.path = u.path)
                  AND NOT EXISTS (SELECT 1 FROM dvs d WHERE d.path = u.path)""",
            HogliteSchema.now(),
            id,
            now,
        )
        val results = store.runTx(catalog, HogliteCatalogStore.freshTxid("upload-sweep"), null, tx)
        return results[fence].affected.toInt()
    }

    private fun load(
        catalog: String,
        id: UUID,
    ): UploadClaim =
        store.queryCatalog(
            catalog,
            "SELECT upload_id, owner, prefix, path, file_kind, state, expires_at FROM uploads WHERE upload_id = ?",
            id.toString(),
        ).firstRow()?.let { r ->
            UploadClaim(
                UUID.fromString(r[0].asString()),
                UUID.fromString(r[1].asString()),
                r[2].asString(),
                r[3].asString(),
                r[4].asString(),
                r[5].asString(),
                HogliteSchema.parse(r[6].asString()),
            )
        } ?: throw HoglakeException.NotFound("upload '$id'")
}
