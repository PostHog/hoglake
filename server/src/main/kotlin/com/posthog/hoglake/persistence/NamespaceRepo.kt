package com.posthog.hoglake.persistence

import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceInfo
import org.jdbi.v3.core.Handle
import org.jdbi.v3.core.statement.UnableToExecuteStatementException

/**
 * hog_namespace. Not snapshot-versioned in v1 (rename disallowed, drop
 * requires emptiness); liveness is the `dropped` flag.
 */
object NamespaceRepo {
    /**
     * Insert a namespace row. The live-name unique index backstops the
     * service-level duplicate check -> [HoglakeException.AlreadyExists].
     */
    fun insert(
        handle: Handle,
        catalogId: Long,
        namespaceId: Long,
        name: String,
    ): NamespaceInfo =
        try {
            handle.createUpdate(
                """
                INSERT INTO hog_namespace (catalog_id, namespace_id, name)
                VALUES (:catalogId, :namespaceId, :name)
                """,
            )
                .bind("catalogId", catalogId)
                .bind("namespaceId", namespaceId)
                .bind("name", name)
                .execute()
                .let { NamespaceInfo(namespaceId, name) }
        } catch (e: UnableToExecuteStatementException) {
            if (Pg.isUniqueViolation(e)) {
                throw HoglakeException.AlreadyExists("namespace '$name' already exists")
            }
            throw e
        }

    /**
     * The namespaces visible at [snapshot], derived from the change log:
     * hog_namespace carries only a liveness flag, but every create and
     * drop records a namespace_created / namespace_dropped change row.
     *
     * Visible at S = no create after S, and either not dropped or
     * dropped after S. Expiry cascades change rows away only BELOW the
     * floor, and a read at S is refused below the floor, so a missing
     * row always means "before every readable S": a namespace whose
     * create expired was created before S, and a dropped namespace whose
     * drop expired was gone before S. Names are unique at any one S (a
     * same-named namespace can only be created after the drop).
     */
    fun listAt(
        handle: Handle,
        catalogId: Long,
        snapshot: Long,
    ): List<NamespaceInfo> =
        handle.createQuery(
            """
            SELECT namespace_id, name FROM hog_namespace n
            WHERE catalog_id = :catalogId AND $VISIBLE_AT
            ORDER BY name
            """,
        )
            .bind("catalogId", catalogId)
            .bind("snapshot", snapshot)
            .map { rs, _ -> NamespaceInfo(rs.getLong("namespace_id"), rs.getString("name")) }
            .list()

    /** [listAt]'s rule for one name. */
    fun findAt(
        handle: Handle,
        catalogId: Long,
        name: String,
        snapshot: Long,
    ): NamespaceInfo? =
        handle.createQuery(
            """
            SELECT namespace_id, name FROM hog_namespace n
            WHERE catalog_id = :catalogId AND name = :name AND $VISIBLE_AT
            """,
        )
            .bind("catalogId", catalogId)
            .bind("name", name)
            .bind("snapshot", snapshot)
            .map { rs, _ -> NamespaceInfo(rs.getLong("namespace_id"), rs.getString("name")) }
            .findOne()
            .orElse(null)

    private const val VISIBLE_AT = """NOT EXISTS (
                SELECT 1 FROM hog_snapshot_change sc
                WHERE sc.catalog_id = n.catalog_id AND sc.object_id = n.namespace_id
                  AND sc.kind = 'namespace_created' AND sc.snapshot_id > :snapshot)
              AND (NOT n.dropped OR EXISTS (
                SELECT 1 FROM hog_snapshot_change sc
                WHERE sc.catalog_id = n.catalog_id AND sc.object_id = n.namespace_id
                  AND sc.kind = 'namespace_dropped' AND sc.snapshot_id > :snapshot))"""

    fun findLiveByName(
        handle: Handle,
        catalogId: Long,
        name: String,
    ): NamespaceInfo? =
        handle.createQuery(
            """
            SELECT namespace_id, name FROM hog_namespace
            WHERE catalog_id = :catalogId AND name = :name AND NOT dropped
            """,
        )
            .bind("catalogId", catalogId)
            .bind("name", name)
            .map { rs, _ -> NamespaceInfo(rs.getLong("namespace_id"), rs.getString("name")) }
            .findOne()
            .orElse(null)

    fun listLive(
        handle: Handle,
        catalogId: Long,
    ): List<NamespaceInfo> =
        handle.createQuery(
            """
            SELECT namespace_id, name FROM hog_namespace
            WHERE catalog_id = :catalogId AND NOT dropped
            ORDER BY name
            """,
        )
            .bind("catalogId", catalogId)
            .map { rs, _ -> NamespaceInfo(rs.getLong("namespace_id"), rs.getString("name")) }
            .list()

    /**
     * Drop tail: set the liveness flag. The row itself stays — namespace
     * ids are not reused, so a dropped namespace keeps its history and a
     * fresh namespace of the same name is a new id, never a resurrection.
     */
    fun markDropped(
        handle: Handle,
        catalogId: Long,
        namespaceId: Long,
    ) {
        handle.createUpdate(
            """
            UPDATE hog_namespace SET dropped = true
            WHERE catalog_id = :catalogId AND namespace_id = :namespaceId AND NOT dropped
            """,
        )
            .bind("catalogId", catalogId)
            .bind("namespaceId", namespaceId)
            .execute()
    }
}
