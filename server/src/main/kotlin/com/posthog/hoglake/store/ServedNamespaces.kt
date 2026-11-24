package com.posthog.hoglake.store

import com.posthog.hoglake.auth.PredicateGrant
import com.posthog.hoglake.auth.ProvenTeam
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceInfo
import com.posthog.hoglake.model.ScanFile
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.model.TableSummaryInfo
import com.posthog.hoglake.service.Identifiers
import com.posthog.hoglake.service.ReservedNamespaces
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.service.TeamNamespaces
import java.time.Instant

/**
 * One family of namespaces this server serves to relayed readers
 * (docs/auth-tenancy.md §6.5d), as `HOGLAKE_SERVED_NAMESPACES_FILE` states
 * it: a request for a canonical `team_<id>` ([TeamNamespaces.id]) under a
 * catalog this server does not hold is answered from the namespace of the
 * same name in [sourceCatalog], when that namespace is marked
 * `[predicateField] = <id>` for the caller's org. With [stream], the same
 * `team_<id>` also shows the published stream tables, with team `<id>`'s rows
 * only.
 */
data class ServedFamily(
    val family: String,
    val sourceCatalog: String,
    val predicateField: String,
    val stream: ServedStream? = null,
) {
    init {
        require(family == TeamNamespaces.FAMILY) {
            "the only served family is '${TeamNamespaces.FAMILY}', got '$family'"
        }
        require(Regex(Identifiers.CATALOG_NAME_PATTERN).matches(sourceCatalog)) {
            "a served family's source_catalog is a catalog name (${Identifiers.CATALOG_NAME_PATTERN}), " +
                "got '$sourceCatalog'"
        }
        require(Regex(Identifiers.PATTERN).matches(predicateField)) {
            "a served family's predicate_field is a field name (${Identifiers.PATTERN}), got '$predicateField'"
        }
    }

    /** What a refusal is counted against: the family, never a team id. */
    val label: String get() = TeamNamespaces.LABEL
}

/**
 * The stream tables a served `team_<id>` shows beside its imports
 * (docs/auth-tenancy.md §6.5d): [tables] of the namespace [namespace] in
 * [catalog], shared by every team and shown with one team's rows. [publishes]
 * and [reserves] are the only place that says which names are stream names.
 */
data class ServedStream(
    val catalog: String,
    val namespace: String,
    val tables: List<String>,
) {
    private val folded: Set<String> = tables.map(ReservedNamespaces::fold).toSet()

    init {
        require(Regex(Identifiers.CATALOG_NAME_PATTERN).matches(catalog)) {
            "a served stream's catalog is a catalog name (${Identifiers.CATALOG_NAME_PATTERN}), got '$catalog'"
        }
        require(Regex(Identifiers.PATTERN).matches(namespace) && !TeamNamespaces.hasShape(namespace)) {
            "a served stream's namespace is a namespace name (${Identifiers.PATTERN}) that is not a team's, " +
                "got '$namespace'"
        }
        require(tables.isNotEmpty()) { "a served stream's tables list at least one table" }
        val invalid = tables.firstOrNull { !Regex(Identifiers.PATTERN).matches(it) }
        require(invalid == null) { "a served stream's tables are table names (${Identifiers.PATTERN}), got '$invalid'" }
        require(folded.size == tables.size) {
            "a served stream's tables must differ in more than ASCII case, got $tables"
        }
    }

    /** Whether [name] is a published stream table: exactly one of [tables]. */
    fun publishes(name: String): Boolean = name in tables

    /** Whether [name] is a published table's name in any ASCII case: no import of that name is shown. */
    fun reserves(name: String): Boolean = ReservedNamespaces.fold(name) in folded
}

/**
 * The stream half of one served `team_<id>`: [stream]'s tables, read by
 * [service] with team [team]'s rows only. Built only by `CatalogStoreSelector`
 * from the [ProvenTeam] the caller rule returned, so it never exists for a
 * caller the rule refused.
 */
class StreamHalf(
    val service: MountReadService,
    val stream: ServedStream,
    val team: ProvenTeam,
    /** The family's predicate field: the top-level column a stream table's object-prefix field must be. */
    val field: String,
    /** The caller's `prd` grant on [field]. A scan reads [team]'s files only while it holds that team. */
    val grant: PredicateGrant?,
    /** What the stream half's series are counted against: the family's label, never a team id. */
    val label: String,
)

/**
 * The read surface of a [CatalogBinding.Served]: bound to one namespace of
 * one source catalog, over this process's PostgreSQL store itself and not the
 * caller's grant view. The caller's token names its own catalog, which is not
 * the source's, so that view would refuse every read; the narrowness of this
 * object stands in for it. No method takes a catalog or a namespace, so a
 * route cannot point it elsewhere. Snapshots are the source catalog's own.
 *
 * With [stream], a name the stream publishes is the stream's table and is
 * read there, at the stream catalog's snapshots; an import of a name it
 * reserves is never shown. Without it, every answer is the import
 * namespace's alone.
 */
class ServedNamespaceReader(
    private val store: CatalogStore,
    private val sourceCatalog: String,
    private val namespace: String,
    /** The caller's own catalog: the only catalog name an error may carry. */
    private val pathCatalog: String,
    private val stream: StreamHalf? = null,
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

    /**
     * The import tables, and with [stream] its published tables in their
     * place, by name. [snapshot] is the import catalog's, so the stream
     * tables are then listed at their own catalog's head; [atTimestamp]
     * applies to both.
     */
    fun tables(
        snapshot: Long?,
        atTimestamp: Instant?,
    ): List<TableSummaryInfo> =
        translated("namespace '$namespace' in catalog '$pathCatalog'") {
            val imports = store.listTables(sourceCatalog, namespace, snapshot, atTimestamp)
            val half = stream ?: return@translated imports
            (imports.filterNot { half.stream.reserves(it.name) } + half.service.streamTables(half, atTimestamp))
                .sortedBy { it.name }
        }

    fun table(
        table: String,
        snapshot: Long?,
        atTimestamp: Instant?,
        /** False skips an import table's file aggregate: for a caller that is not given the totals. */
        totals: Boolean = true,
    ): TableInfo =
        translated(tableMissing(table)) {
            val half = streamFor(table)
            if (half == null) {
                store.getTable(sourceCatalog, namespace, table, snapshot, atTimestamp, totals)
            } else {
                // The served name, never the stream's own namespace.
                half.service.streamTable(half, table, snapshot, atTimestamp).copy(namespace = namespace)
            }
        }

    fun scan(
        table: String,
        snapshot: Long?,
        atTimestamp: Instant?,
        columnStats: ScanService.ColumnStatsRequest?,
        splitOffsets: Boolean,
    ): List<ScanFile> =
        translated(tableMissing(table)) {
            val half = streamFor(table)
            if (half == null) {
                store.planScan(sourceCatalog, namespace, table, snapshot, atTimestamp, columnStats, splitOffsets)
            } else {
                half.service.streamScan(half, table, snapshot, atTimestamp, columnStats, splitOffsets)
            }
        }

    /**
     * The stream half when it publishes [table]; null for an import. A case
     * variant of a published name is neither: an engine that folds names
     * would show it as the stream table, so it is missing.
     */
    private fun streamFor(table: String): StreamHalf? {
        val half = stream ?: return null
        if (half.stream.publishes(table)) return half
        if (half.stream.reserves(table)) throw HoglakeException.NotFound("table")
        return null
    }

    private fun tableMissing(table: String) = "table '$namespace.$table' in catalog '$pathCatalog'"

    /**
     * The store words its refusals in the source catalog's terms: its name, the
     * head range, the expiry floor. An admitted caller gets the same statuses
     * in the path catalog's terms, with none of those, and none of the stream
     * catalog's or the stream namespace's names either.
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
            val names = listOfNotNull(sourceCatalog, stream?.stream?.catalog, stream?.stream?.namespace)
            if (names.any { it in detail } || "out of range" in detail) {
                throw HoglakeException.Validation("the requested snapshot is out of range for catalog '$pathCatalog'")
            }
            throw e
        }
}
