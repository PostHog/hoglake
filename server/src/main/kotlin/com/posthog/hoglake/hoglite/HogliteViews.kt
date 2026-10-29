package com.posthog.hoglake.hoglite

import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.ViewInfo
import com.posthog.hoglake.service.Identifiers
import com.posthog.hoglake.service.ViewBackend
import com.posthog.hoglake.service.requireOperationMetadata
import java.util.UUID

/**
 * [ViewBackend] on hoglite: [com.posthog.hoglake.service.ViewService]'s
 * semantics over the tenant's `views` table. Views are pure catalog
 * metadata: a create/drop is a DDL snapshot, a change row, and a
 * version row, exactly like the Postgres path (ViewRepo), so each
 * operation is one guarded hoglite transaction.
 */
internal class HogliteViews(
    private val store: HogliteCatalogStore,
) : ViewBackend {
    override fun create(
        catalog: String,
        namespace: String,
        name: String,
        sql: String,
        dialect: String,
        author: String?,
        message: String?,
    ): ViewInfo {
        Identifiers.validate("view", name)
        if (sql.isBlank()) throw HoglakeException.Validation("view sql must not be blank")
        if (dialect.isBlank()) throw HoglakeException.Validation("view dialect must not be blank")
        val ns = store.liveNamespace(catalog, namespace)

        val tx = HogliteTx()
        tx.expectZero(
            "SELECT COUNT(*) FROM views WHERE namespace_id = ? AND name = ? AND dropped_snapshot IS NULL",
            ns.namespaceId,
            name,
        ) { HoglakeException.AlreadyExists("view '$name' already exists in namespace '$namespace'") }
        tx.allocateDdlSnapshot()
        tx.insertSnapshotRow(author, message)
        tx.affectOne(
            "UPDATE meta SET next_view_id = next_view_id + 1 WHERE id = 1",
        ) { HoglakeException.NotFound("catalog (meta row missing)") }
        // The readback answers the snapshot AND the view id this tx
        // allocated (the insert below binds the same subselect).
        tx.add(
            "INSERT INTO changes(snapshot_id, kind, object_id) " +
                "VALUES (${HogliteSchema.HEAD_SQL}, 'view_created', (SELECT next_view_id - 1 FROM meta WHERE id = 1))",
        )
        val viewUuid = UUID.randomUUID()
        tx.add(
            "INSERT INTO views(id, namespace_id, namespace, name, view_uuid, dialect, sql_text, created_snapshot) " +
                "VALUES ((SELECT next_view_id - 1 FROM meta WHERE id = 1), ?, ?, ?, ?, ?, ?, " +
                "${HogliteSchema.HEAD_SQL})",
            ns.namespaceId,
            namespace,
            name,
            viewUuid.toString(),
            dialect,
            sql,
        )
        val readback =
            tx.add(
                "SELECT (SELECT next_snapshot - 1 FROM meta WHERE id = 1), " +
                    "(SELECT next_view_id - 1 FROM meta WHERE id = 1)",
            )
        val results = store.runTx(catalog, HogliteCatalogStore.freshTxid("view-create"), null, tx)
        val viewId = results[readback].firstRow()!![1].asLong()
        return ViewInfo(
            viewId = viewId,
            viewUuid = viewUuid,
            namespace = namespace,
            name = name,
            dialect = dialect,
            sql = sql,
        )
    }

    override fun get(
        catalog: String,
        namespace: String,
        name: String,
    ): ViewInfo {
        val ns = store.liveNamespace(catalog, namespace)
        return viewRow(catalog, ns.namespaceId, namespace, name)
            ?: throw HoglakeException.NotFound("view '$namespace.$name' in catalog '$catalog'")
    }

    override fun list(
        catalog: String,
        namespace: String,
    ): List<ViewInfo> {
        val ns = store.liveNamespace(catalog, namespace)
        return store.queryCatalog(
            catalog,
            "SELECT id, view_uuid, name, dialect, sql_text FROM views " +
                "WHERE namespace_id = ? AND dropped_snapshot IS NULL ORDER BY name",
            ns.namespaceId,
        ).rows.map { row ->
            ViewInfo(
                viewId = row[0].asLong(),
                viewUuid = UUID.fromString(row[1].asString()),
                namespace = namespace,
                name = row[2].asString(),
                dialect = row[3].asString(),
                sql = row[4].asString(),
            )
        }
    }

    override fun drop(
        catalog: String,
        namespace: String,
        name: String,
        author: String?,
        message: String?,
    ): CommitResult {
        requireOperationMetadata(author, message)
        val ns = store.liveNamespace(catalog, namespace)
        val view =
            viewRow(catalog, ns.namespaceId, namespace, name)
                ?: throw HoglakeException.NotFound("view '$namespace.$name' in catalog '$catalog'")

        val tx = HogliteTx()
        tx.allocateDdlSnapshot()
        tx.insertSnapshotRow(author, message)
        tx.add(
            "INSERT INTO changes(snapshot_id, kind, object_id) " +
                "VALUES (${HogliteSchema.HEAD_SQL}, 'view_dropped', ?)",
            view.viewId,
        )
        tx.affectOne(
            "UPDATE views SET dropped_snapshot = ${HogliteSchema.HEAD_SQL} " +
                "WHERE id = ? AND dropped_snapshot IS NULL",
            view.viewId,
        ) { HoglakeException.NotFound("view '$namespace.$name' in catalog '$catalog'") }
        val readback = tx.add(HogliteSql.READBACK_DDL)
        val results = store.runTx(catalog, HogliteCatalogStore.freshTxid("view-drop"), null, tx)
        val row = results[readback].firstRow()!!
        return CommitResult(snapshotId = row[0].asLong(), schemaVersion = row[1].asLong())
    }

    private fun viewRow(
        catalog: String,
        namespaceId: Long,
        namespace: String,
        name: String,
    ): ViewInfo? =
        store.queryCatalog(
            catalog,
            "SELECT id, view_uuid, dialect, sql_text FROM views " +
                "WHERE namespace_id = ? AND name = ? AND dropped_snapshot IS NULL",
            namespaceId,
            name,
        ).firstRow()?.let { row ->
            ViewInfo(
                viewId = row[0].asLong(),
                viewUuid = UUID.fromString(row[1].asString()),
                namespace = namespace,
                name = name,
                dialect = row[2].asString(),
                sql = row[3].asString(),
            )
        }
}
