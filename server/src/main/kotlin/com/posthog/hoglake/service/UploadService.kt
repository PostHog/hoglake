package com.posthog.hoglake.service

import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.persistence.Locks
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.inTransactionUnchecked
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

/** Durable ownership before PUT. Every transition serializes with publication and physical cleanup. */
class UploadService(private val jdbi: Jdbi) {
    fun claim(
        catalog: String,
        id: UUID,
        owner: UUID,
        prefix: String,
        kind: String,
    ): UploadClaim =
        locked(catalog, "upload_claim") { h, catalogId ->
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
        locked(catalog, "upload_renew") { h, catalogId ->
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
        locked(catalog, "upload_abandon") { h, catalogId ->
            if (paths.size > 10000) throw HoglakeException.Validation("too many upload paths")
            // Registered claims are immutable: abort cannot retract a publication with an unknown response.
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
        locked(catalog, "upload_schedule_expired") { h, catalogId ->
            if (limit !in 1..10000) throw HoglakeException.Validation("limit must be between 1 and 10000")
            val claims =
                h.createQuery(
                    """
            SELECT upload_id FROM hog_upload
            WHERE catalog_id = :catalog AND (state = 'abandoned' OR (state = 'active' AND expires_at <= now()))
            ORDER BY last_scheduled_at NULLS FIRST, upload_id LIMIT :limit
        """,
                ).bind("catalog", catalogId).bind("limit", limit).mapTo(UUID::class.java).list()
            for (id in claims) {
                val claim = load(h, catalogId, id)
                // Fence first, under the publication lock; queue membership alone is never permission.
                h.createUpdate(
                    """
                UPDATE hog_upload SET state = 'abandoned', last_scheduled_at = now()
                WHERE catalog_id = :catalog AND upload_id = :id
            """,
                ).bind("catalog", catalogId).bind("id", id).execute()
                h.createUpdate(
                    """
                INSERT INTO hog_file_removal (catalog_id, path, file_kind, reason)
                SELECT :catalog, :path, :kind, 'trino_upload'
                WHERE NOT EXISTS (SELECT 1 FROM hog_file_removal
                    WHERE catalog_id = :catalog AND path = :path AND drained_at IS NULL)
            """,
                ).bind("catalog", catalogId).bind("path", claim.path).bind("kind", claim.fileKind).execute()
            }
            // Revisit abandoned tombstones on later calls: a PUT already in flight when fenced
            // can finish after an earlier DELETE. It can never publish, and remains reclaimable.
            claims.size
        }

    private fun <T> locked(
        catalog: String,
        action: String,
        block: (Handle, Long) -> T,
    ): T =
        com.posthog.hoglake.observability.Audit.audited(action, catalog, null) {
            jdbi.inTransactionUnchecked { h ->
                val catalogId = CatalogRepo.require(h, catalog).catalogId
                Locks.acquireCatalogCommitLock(h, catalogId)
                block(h, catalogId)
            }
        }

    companion object {
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
            val claims =
                h.createQuery(
                    """
                SELECT upload_id, owner, prefix, path, file_kind, state, expires_at
                FROM hog_upload WHERE catalog_id = :catalog AND path = ANY(:paths)
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
