package com.posthog.hoglake.store

import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceInfo
import com.posthog.hoglake.model.ScanFile
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.model.TableSummaryInfo
import com.posthog.hoglake.service.Identifiers
import com.posthog.hoglake.service.ScanService
import java.time.Instant

/**
 * One namespace this server serves to relayed readers (docs/auth-tenancy.md
 * §6.5d), as `HOGLAKE_SERVED_NAMESPACES_FILE` states it: requests naming
 * [name] under a catalog this server does not hold are answered from the
 * namespace of the same name in [sourceCatalog].
 */
data class ServedNamespace(
    val name: String,
    val sourceCatalog: String,
) {
    init {
        require(Regex(Identifiers.PATTERN).matches(name)) {
            "a served namespace is named by a namespace name (${Identifiers.PATTERN}), got '$name'"
        }
        require(Regex(Identifiers.CATALOG_NAME_PATTERN).matches(sourceCatalog)) {
            "a served namespace's source_catalog is a catalog name (${Identifiers.CATALOG_NAME_PATTERN}), " +
                "got '$sourceCatalog'"
        }
    }
}

/**
 * The read surface of a [CatalogBinding.Served]: bound to one namespace of
 * one source catalog, over this process's PostgreSQL store itself and not the
 * caller's grant view. The caller's token names its own catalog, which is not
 * the source's, so that view would refuse every read; the narrowness of this
 * object stands in for it. No method takes a catalog or a namespace, so a
 * route cannot point it elsewhere. Snapshots are the source catalog's own.
 */
class ServedNamespaceReader(
    private val store: CatalogStore,
    private val sourceCatalog: String,
    private val namespace: String,
    /** The caller's own catalog: the only catalog name an error may carry. */
    private val pathCatalog: String,
) {
    /**
     * The stored namespace without its containment. A head read is the one
     * single-namespace lookup; only a pinned read lists the namespaces of that
     * snapshot.
     */
    fun namespace(
        snapshot: Long?,
        atTimestamp: Instant?,
    ): NamespaceInfo =
        translated("namespace '$namespace' in catalog '$pathCatalog'") {
            if (snapshot == null && atTimestamp == null) {
                store.getNamespace(sourceCatalog, namespace)
            } else {
                store.listNamespaces(sourceCatalog, snapshot, atTimestamp).firstOrNull { it.name == namespace }
                    ?: throw HoglakeException.NotFound("namespace")
            }.copy(containment = null)
        }

    fun tables(
        snapshot: Long?,
        atTimestamp: Instant?,
    ): List<TableSummaryInfo> =
        translated("namespace '$namespace' in catalog '$pathCatalog'") {
            store.listTables(sourceCatalog, namespace, snapshot, atTimestamp)
        }

    fun table(
        table: String,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): TableInfo =
        translated(tableMissing(table)) { store.getTable(sourceCatalog, namespace, table, snapshot, atTimestamp) }

    fun scan(
        table: String,
        snapshot: Long?,
        atTimestamp: Instant?,
        columnStats: ScanService.ColumnStatsRequest?,
        splitOffsets: Boolean,
    ): List<ScanFile> =
        translated(tableMissing(table)) {
            store.planScan(sourceCatalog, namespace, table, snapshot, atTimestamp, columnStats, splitOffsets)
        }

    private fun tableMissing(table: String) = "table '$namespace.$table' in catalog '$pathCatalog'"

    /**
     * The store words its refusals in the source catalog's terms: its name, the
     * head range, the expiry floor. An admitted caller gets the same statuses
     * in the path catalog's terms, with none of those.
     */
    private fun <T> translated(
        missing: String,
        read: () -> T,
    ): T =
        try {
            read()
        } catch (_: HoglakeException.NotFound) {
            throw HoglakeException.NotFound(missing)
        } catch (_: HoglakeException.Expired) {
            throw HoglakeException.Expired("the requested snapshot is below the expiry floor of catalog '$pathCatalog'")
        } catch (e: HoglakeException.Validation) {
            val detail = e.message.orEmpty()
            if (sourceCatalog in detail || "out of range" in detail) {
                throw HoglakeException.Validation("the requested snapshot is out of range for catalog '$pathCatalog'")
            }
            throw e
        }
}
