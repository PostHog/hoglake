package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.CatalogNotEmptyReason
import com.posthog.hoglake.model.ChangeKind
import com.posthog.hoglake.model.ChangesPlan
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.ColumnStats
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.CommitResult
import com.posthog.hoglake.model.ConsumerOffset
import com.posthog.hoglake.model.ConsumerTableOffset
import com.posthog.hoglake.model.Containment
import com.posthog.hoglake.model.DataFile
import com.posthog.hoglake.model.DeleteFile
import com.posthog.hoglake.model.DroppedCatalog
import com.posthog.hoglake.model.FileColumnStats
import com.posthog.hoglake.model.FileOrderingBounds
import com.posthog.hoglake.model.FileStats
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.model.NamespaceInfo
import com.posthog.hoglake.model.PartitionFieldDef
import com.posthog.hoglake.model.PartitionFieldValues
import com.posthog.hoglake.model.PartitionSpec
import com.posthog.hoglake.model.PartitionValues
import com.posthog.hoglake.model.ScanFile
import com.posthog.hoglake.model.Snapshot
import com.posthog.hoglake.model.SnapshotChange
import com.posthog.hoglake.model.SnapshotResolution
import com.posthog.hoglake.model.SortFieldDef
import com.posthog.hoglake.model.SortSpec
import com.posthog.hoglake.model.StatsState
import com.posthog.hoglake.model.StorageUsage
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.model.TableSummaryInfo
import com.posthog.hoglake.model.assignFieldIds
import com.posthog.hoglake.persistence.FileRepo
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.ColumnTrees
import com.posthog.hoglake.service.Identifiers
import com.posthog.hoglake.service.ScanService
import com.posthog.hoglake.service.columnsByFieldId
import com.posthog.hoglake.service.requireOperationMetadata
import com.posthog.hoglake.store.CatalogStore
import java.time.Instant
import java.util.UUID

/**
 * Maps a catalog name to the hoglite tenant id that holds the catalog.
 *
 * The caller owns this mapping. [HogliteCatalogStore] does not keep a
 * registry of catalogs, and it does not mint tenant ids. A routing layer
 * supplies the mapping (for example from the request token or from a
 * routing service). Throw [HoglakeException.NotFound] for a catalog that
 * has no tenant.
 *
 * Give each incarnation of a catalog name its own tenant id (for example
 * a suffix that changes on each create). Then a drop that resumes late
 * cannot delete a catalog that was created again with the same name.
 */
fun interface HogliteTenantResolver {
    fun tenantFor(catalog: String): String
}

/**
 * A hoglake catalog store on hoglite: every catalog is one hoglite
 * TENANT, and every operation is one hoglite transaction (writes) or one
 * read-only query (reads). The semantics are hoglake's own — the OCC
 * guards, snapshot allocation, history-preserving deletion vectors and
 * idempotent publication that CommitService/CatalogService implement
 * against Postgres — re-expressed against hoglite's single-round-trip
 * transaction API, where the per-tenant serialization hoglite already
 * provides replaces the per-catalog advisory lock.
 *
 * This class keeps no cross-catalog state. [tenants] tells it which
 * tenant holds a catalog. Cross-catalog rules (unique names, disjoint
 * data paths) belong to the component that owns the catalogs.
 *
 * It implements [CatalogStore], the seam used by the request-scoped backend selector.
 */
class HogliteCatalogStore(
    internal val client: HogliteClient,
    /** Catalog name -> tenant id. See [HogliteTenantResolver]. */
    private val tenants: HogliteTenantResolver,
    /** CommitService's blind-partitioned-append enforcement flag, for the commit port. */
    internal val refuseBlindPartitionedAppends: Boolean = false,
    internal val ledgerRetentionSeconds: Long = com.posthog.hoglake.service.CleanupService.LEDGER_RETENTION_SECONDS,
    internal val smallFileThresholdBytes: Long = 512L * 1024 * 1024,
    internal val minInputFiles: Int = com.posthog.hoglake.compaction.CompactionGrouping.DEFAULT_MIN_INPUT_FILES,
    internal val maxInputFiles: Int = com.posthog.hoglake.compaction.CompactionGrouping.DEFAULT_MAX_INPUT_FILES,
    internal val partitionLookupAdmission: HoglitePartitionLookupAdmission = HoglitePartitionLookupAdmission(),
    private val operatorAuthority: Boolean = true,
) : CatalogStore {
    internal val json: ObjectMapper = jacksonObjectMapper()

    private val log = io.github.oshai.kotlinlogging.KotlinLogging.logger {}

    /**
     * Catalog APIs are synchronous: this re-entrant scope
     * follows their call stack, not a request-global mutable name cache. Nested
     * helpers reuse the SAME incarnation; concurrent requests have separate scopes.
     * If catalog work ever moves to another thread, its binding must be carried
     * explicitly rather than starting another operation there.
     */
    private class CatalogOperation(val tenant: String) {
        var missing: HoglakeException.NotFound? = null
    }

    private val operationBindings = ThreadLocal<MutableMap<String, CatalogOperation>>()

    /**
     * Bind [catalog] to ONE tenant id for the whole operation. The id is
     * resolved once, here, through [tenants]. Nested helpers reuse the
     * binding. If the catalog is dropped (and perhaps created again)
     * during the operation, the bound tenant answers 404 and the
     * operation fails NotFound; it never reaches another tenant.
     */
    @PublishedApi
    internal fun enterCatalogOperation(
        catalog: String,
        creating: Boolean = false,
    ): Boolean {
        if (operationBindings.get()?.containsKey(catalog) == true) {
            check(!creating) { "cannot bootstrap inside an existing catalog operation" }
            return false
        }
        val tenant = tenants.tenantFor(catalog)
        val bindings =
            operationBindings.get()
                ?: mutableMapOf<String, CatalogOperation>().also { operationBindings.set(it) }
        bindings[catalog] = CatalogOperation(tenant)
        return true
    }

    @PublishedApi
    internal fun finishCatalogOperation(
        catalog: String,
        entered: Boolean,
    ) {
        val bindings = checkNotNull(operationBindings.get())
        val binding = checkNotNull(bindings[catalog])
        if (entered) {
            bindings.remove(catalog)
            if (bindings.isEmpty()) operationBindings.remove()
        }
        // A broad catch must not acknowledge success after a bound 404,
        // nor allow a later write to run in another incarnation.
        binding.missing?.let { throw it }
    }

    /** Run blocking work only. Never suspend or change threads inside [body]. */
    internal inline fun <T> catalogOperation(
        catalog: String,
        creating: Boolean = false,
        body: () -> T,
    ): T {
        val entered = enterCatalogOperation(catalog, creating)
        var failure: Throwable? = null
        try {
            return body()
        } catch (e: Throwable) {
            failure = e
            throw e
        } finally {
            try {
                finishCatalogOperation(catalog, entered)
            } catch (secondary: Throwable) {
                val original = failure
                if (original == null) throw secondary
                // Nested scopes may observe the same poison. Preserve the body's
                // outcome (especially an ambiguous txid), without self/duplicate suppression.
                if (original !== secondary && original.suppressed.none { it === secondary }) {
                    original.addSuppressed(secondary)
                }
            }
        }
    }

    private fun missingCatalog(
        catalog: String,
        message: String = "catalog '$catalog'",
    ): HoglakeException.NotFound {
        val binding = operationBindings.get()?.get(catalog)
        binding?.missing?.let { return it }
        return HoglakeException.NotFound(message).also { binding?.missing = it }
    }

    // ==================== catalogs =========================================

    /**
     * Create the catalog's tenant and its schema. The tenant id comes from
     * [tenants]. The caller checks cross-catalog rules (unique names,
     * disjoint data paths) before it calls this function.
     *
     * A create on a tenant that already holds a catalog is AlreadyExists.
     * A fresh txid per CALL: a same-name create is a 409, so the guard,
     * not hoglite's dedup, answers a second create. The client's own
     * retries reuse the txid, so one call applies at most once.
     */
    override fun createCatalog(
        name: String,
        dataPath: String,
        author: String?,
        message: String?,
    ): CatalogInfo =
        catalogOperation(name, creating = true) {
            validateCatalogName(name)
            CatalogService.validateDataPath(dataPath)
            val tenantId = boundTenant(name)
            backendCall(name) { client.createTenant(tenantId) }
            val boot = HogliteTx()
            // The schema is bootstrapped in ONE transaction, so the meta
            // table exists exactly when an earlier create committed.
            boot.expectZero("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'meta'") {
                HoglakeException.AlreadyExists("catalog '$name' already exists")
            }
            HogliteSchema.DDL.forEach { boot.add(it) }
            boot.add(
                "INSERT INTO meta(id, name, data_path) VALUES (1, ?, ?)",
                name,
                dataPath,
            )
            boot.add(
                "INSERT INTO snapshots(id, ts, author, message, schema_version) VALUES (0, ?, ?, ?, 0)",
                HogliteSchema.now(),
                author,
                message,
            )
            try {
                client.tx(tenantId, freshTxid("create-catalog"), null, boot.statements())
            } catch (g: HogliteGuardFailure) {
                throw boot.classify(g)
            } catch (r: HogliteRejected) {
                throw mapRejected(name, r)
            }
            return getCatalog(name)
        }

    override fun getCatalog(name: String): CatalogInfo =
        catalogOperation(name) {
            meta(name).toCatalogInfo()
        }

    /**
     * The named catalogs, in a constant number of hoglite round trips
     * rather than one or more per catalog: the catalogs' meta rows, each
     * read from the tenant [tenants] names, through hoglite's
     * `/v1/query-batch` ([HogliteClient.queryBatch]): one request per
     * [listBatchSize] catalogs. An engine without the endpoint gets the
     * same reads one by one, [LIST_FALLBACK_PARALLELISM] at a time.
     *
     * This class keeps no list of catalogs, so the caller names them in
     * [visible]. A null [visible] ("all catalogs") is refused with
     * Validation, because there is no list to enumerate.
     *
     * A name that [tenants] does not know (NotFound), with no readable
     * tenant, or whose tenant holds another catalog, is skipped. A drop
     * that runs during the list must not fail the list. Any other
     * resolver failure fails the call.
     */
    override fun listCatalogs(visible: Set<String>?): List<CatalogInfo> {
        // TODO(wiring): a null visible set ("all catalogs") comes with auth off and from operator callers.
        // This adapter keeps no catalog list, so it refuses that request. A later change must design
        // the answer, for example a catalog list from the routing source.
        val names =
            visible ?: throw HoglakeException.Validation(
                "the hoglite backend keeps no list of catalogs; name the catalogs to list",
            )
        val bound =
            names.mapNotNull { name ->
                try {
                    name to tenants.tenantFor(name)
                } catch (_: HoglakeException.NotFound) {
                    log.warn { "catalog '$name' has no tenant; skipping" }
                    null
                }
            }
        val metas = readMetas(bound)
        return bound.indices.mapNotNull { i ->
            val name = bound[i].first
            val row = metas[i]
            when {
                row == null -> {
                    log.warn { "catalog '$name' has no readable tenant; skipping" }
                    null
                }
                row[0] != name -> {
                    log.warn { "catalog '$name' resolved to a tenant whose meta names '${row[0]}'; skipping" }
                    null
                }
                else -> metaRow(row).toCatalogInfo()
            }
        }
    }

    /** Catalogs per `/v1/query-batch` request; a test hook, so a test can force several. */
    internal var listBatchSize: Int = LIST_BATCH_SIZE

    /**
     * The meta row of each (name, tenant) in [bound], in order: the row,
     * or null when that tenant does not exist (404) or has no meta row.
     * Any other answer for an item is retried on its own through
     * [HogliteClient.query] (which owns re-routing and the retry budget),
     * and an item that still fails fails the call, as a single
     * [getCatalog] would. That covers a whale-class tenant: the batch goes
     * to the standard pool, which answers that item 421 with the class,
     * and the retry follows the hint to the whale pool.
     */
    private fun readMetas(bound: List<Pair<String, String>>): List<List<Any?>?> {
        val out = arrayOfNulls<List<Any?>>(bound.size)
        val retry = mutableListOf<Int>()
        val sql = metaSql
        val chunks = bound.indices.chunked(listBatchSize.coerceIn(1, HogliteClient.MAX_BATCH_READS))
        // The chunks are independent requests: sent together, a list costs
        // the slowest chunk's latency, not their sum.
        val chunkAnswers =
            parallelReads(chunks.indices.toList()) { c ->
                backendCall(bound[chunks[c].first()].first) {
                    client.queryBatch(chunks[c].map { HogliteClient.BatchRead(bound[it].second, sql) })
                }
            }
        for ((chunk, answers) in chunks.zip(chunkAnswers)) {
            if (answers == null) {
                retry += chunk
                continue
            }
            for ((k, i) in chunk.withIndex()) {
                val a = answers[k]
                when {
                    a.status == 200 -> out[i] = a.result!!.firstRow()
                    a.status == 404 -> out[i] = null
                    a.status == 401 || a.status == 403 ->
                        throw mapRejected(bound[i].first, HogliteRejected(a.status, a.body))
                    else -> retry += i
                }
            }
        }
        if (retry.isNotEmpty()) {
            val rows = parallelReads(retry) { i -> readMetaOne(bound[i].first, bound[i].second, sql) }
            for ((k, i) in retry.withIndex()) out[i] = rows[k]
        }
        return out.toList()
    }

    private fun readMetaOne(
        catalog: String,
        tenant: String,
        sql: String,
    ): List<Any?>? =
        try {
            client.query(tenant, sql, emptyList()).firstRow()
        } catch (r: HogliteRejected) {
            if (r.status == 404) null else throw mapRejected(catalog, r)
        }

    /**
     * [read] for each of [items], [LIST_FALLBACK_PARALLELISM] at a time, on
     * [fanoutPool]. The reads address explicit tenant ids, so they need no
     * operation binding on the pool's threads. The first failure is
     * rethrown once every read has finished.
     */
    private fun <T> parallelReads(
        items: List<Int>,
        read: (Int) -> T,
    ): List<T> {
        if (items.size == 1) return listOf(read(items[0]))
        val futures = items.map { i -> fanoutPool.submit<T> { read(i) } }
        var failure: Throwable? = null
        val results =
            futures.map { f ->
                try {
                    f.get()
                } catch (e: java.util.concurrent.ExecutionException) {
                    failure = failure ?: e.cause
                    null
                }
            }
        failure?.let { throw it }
        @Suppress("UNCHECKED_CAST")
        return results as List<T>
    }

    private val fanoutPool: java.util.concurrent.ExecutorService by lazy {
        val n = java.util.concurrent.atomic.AtomicInteger()
        java.util.concurrent.Executors.newFixedThreadPool(LIST_FALLBACK_PARALLELISM) { r ->
            Thread(r, "hoglite-list-${n.incrementAndGet()}").apply { isDaemon = true }
        }
    }

    /**
     * Drop an EMPTY catalog: prove it empty and fence its writes in one
     * transaction, then release and delete its tenant. A retry after a
     * partial failure proceeds: the fence transaction itself is not
     * fenced.
     */
    override fun dropCatalog(
        catalog: String,
        author: String?,
        message: String?,
    ): DroppedCatalog {
        if (!operatorAuthority) {
            throw HoglakeException.Forbidden("hoglite catalog deletion requires operator authority")
        }
        val binding = operationBindings.get()?.get(catalog)
        binding?.missing?.let { throw it }
        val tenantId = binding?.tenant ?: tenants.tenantFor(catalog)
        val info =
            when (val probe = dropMetadata(catalog, tenantId)) {
                DropMetadata.Absent -> throw missingCatalog(catalog)
                is DropMetadata.Present -> probe.meta
            }

        // The emptiness preconditions AND the write fence in ONE
        // transaction: the guards run in the resolution ORDER the
        // Postgres side documents (CatalogNotEmptyReason's KDoc is the
        // contract), and the same tx sets meta.dropping, which every
        // mutating tenant tx guards on (runTx statement 0) — so no
        // namespace or commit can land between these
        // checks and the tenant deletion below. The fence tx itself
        // opts out of the fence (a retried drop after a partial failure
        // must be able to proceed).
        fun countNow(sql: String): Long =
            try {
                client.query(tenantId, sql, emptyList()).firstRow()!!.get(0).asLong()
            } catch (r: HogliteRejected) {
                throw mapRejected(catalog, r)
            }

        val fenceNow = HogliteSchema.now()
        val fence = HogliteTx()

        fun emptyGuard(
            sql: String,
            reason: CatalogNotEmptyReason,
            describe: (Long) -> String,
        ) {
            fence.expectZero(sql) {
                val n = countNow(sql)
                HoglakeException.CatalogNotEmpty(
                    "catalog '$catalog' is not empty: ${describe(n)}",
                    reason,
                )
            }
        }
        emptyGuard(
            "SELECT COUNT(*) FROM namespaces WHERE dropped_snapshot IS NULL",
            CatalogNotEmptyReason.LIVE_NAMESPACES,
        ) { "$it live namespace(s) remain" }
        emptyGuard(
            "SELECT (SELECT COUNT(*) FROM files WHERE end_snapshot IS NULL) + " +
                "(SELECT COUNT(*) FROM dvs WHERE end_snapshot IS NULL)",
            CatalogNotEmptyReason.TABLES_PENDING_RETIREMENT,
        ) { "$it live file row(s) await retirement" }
        emptyGuard(
            "SELECT (SELECT COUNT(*) FROM files) + (SELECT COUNT(*) FROM dvs)",
            CatalogNotEmptyReason.FILES_PENDING_EXPIRY,
        ) { "$it ended file row(s) await expiry" }
        emptyGuard(
            "SELECT COUNT(*) FROM removals WHERE drained_at IS NULL",
            CatalogNotEmptyReason.REMOVALS_PENDING,
        ) { "$it removal(s) are not drained" }
        emptyGuard(
            "SELECT COUNT(*) FROM compaction_claims WHERE expires_at > '$fenceNow'",
            CatalogNotEmptyReason.ACTIVE_COMPACTION_CLAIMS,
        ) { "$it live compaction claim(s); a compactor may be mid-rewrite" }
        emptyGuard(
            "SELECT COUNT(*) FROM offsets",
            CatalogNotEmptyReason.CONSUMER_OFFSETS,
        ) { "$it consumer offset(s) remain" }
        emptyGuard(
            "SELECT COUNT(*) FROM uploads WHERE state = 'active'",
            CatalogNotEmptyReason.ACTIVE_UPLOADS,
        ) { "$it active upload claim(s) remain" }
        emptyGuard(
            "SELECT COUNT(*) FROM uploads WHERE state = 'abandoned' " +
                "AND (scheduled_at IS NULL OR expires_at > '$fenceNow')",
            CatalogNotEmptyReason.ABANDONED_UPLOADS_UNSETTLED,
        ) {
            "$it abandoned upload(s) are not settled (schedule them with POST .../uploads/schedule-expired, " +
                "or wait out their expiry window)"
        }
        fence.add("UPDATE meta SET dropping = 1 WHERE id = 1")
        try {
            client.tx(tenantId, freshTxid("drop-fence"), null, fence.statements())
        } catch (g: HogliteGuardFailure) {
            throw fence.classify(g)
        } catch (r: HogliteRejected) {
            throw mapRejected(catalog, r)
        }

        // Order matters: the fence stops new writes, then the owner is
        // released so nothing in flight can re-materialize the tenant,
        // THEN its durable state. hoglite routes
        // release AND delete to the owning pod (a 421/503 rides the
        // client's retry taxonomy) and answers release idempotently once
        // no live owner remains. The 409 catch is transitional: an old
        // engine (pre delete-routing) refuses a delete under a live
        // remote owner — release again and retry with a pause.
        var fencingFailure: HogliteRejected? = null
        var deleted = false
        for (attempt in 0 until 3) {
            if (attempt > 0) Thread.sleep(200)
            backendCall(catalog) { client.release(tenantId) }
            try {
                client.deleteTenant(tenantId)
                deleted = true
                break
            } catch (e: HogliteRejected) {
                if (e.status == 409) fencingFailure = e else throw mapRejected(catalog, e)
            }
        }
        if (!deleted) {
            throw HoglakeException.BackendUnavailable(
                "hoglite refused to delete tenant '$catalog' three times in a row (a live owner " +
                    "kept re-claiming it): ${fencingFailure?.body}",
            )
        }
        return DroppedCatalog(catalog, info.dataPath)
    }

    /**
     * The catalog's footprint, with the StorageUsageRepo contract.
     * Tracked bytes count EVERY file and DV row, ended rows included,
     * because those objects stay in storage until expiry or retirement.
     * Pending deletes count only removals that are not drained.
     * Overflow is an error, not a wrapped total.
     */
    override fun storageUsage(catalog: String): StorageUsage =
        catalogOperation(catalog) {
            meta(catalog)
            val row = queryCatalog(catalog, STORAGE_USAGE_SQL).firstRow()!!
            return StorageUsage(
                trackedBytes = Math.addExact(row[0].asLong(), row[1].asLong()),
                pendingDeleteFiles = row[2].asLong(),
            )
        }

    // ==================== namespaces =======================================

    override fun createNamespace(
        catalog: String,
        name: String,
        author: String?,
        message: String?,
    ): NamespaceInfo =
        catalogOperation(catalog) {
            Identifiers.validate("namespace", name)
            meta(catalog) // 404 first, as on the Postgres path
            val tx = HogliteTx()
            tx.expectZero(
                "SELECT COUNT(*) FROM namespaces WHERE name = ? AND dropped_snapshot IS NULL",
                name,
            ) { HoglakeException.AlreadyExists("namespace '$name' already exists") }
            tx.allocateDdlSnapshot()
            tx.affectOne(
                "UPDATE meta SET next_namespace_id = next_namespace_id + 1 WHERE id = 1",
            ) { HoglakeException.NotFound("catalog '$catalog'") }
            tx.add(
                "INSERT INTO namespaces(id, name, created_snapshot) " +
                    "VALUES ((SELECT next_namespace_id - 1 FROM meta WHERE id = 1), ?, ${HogliteSchema.HEAD_SQL})",
                name,
            )
            tx.add(
                "INSERT INTO changes(snapshot_id, kind, object_id) " +
                    "VALUES (${HogliteSchema.HEAD_SQL}, 'namespace_created', " +
                    "(SELECT next_namespace_id - 1 FROM meta WHERE id = 1))",
            )
            tx.insertSnapshotRow(author, message)
            val readback = tx.add(HogliteSql.READBACK_DDL)
            val results = runTx(catalog, freshTxid("create-ns"), null, tx)
            val row = results[readback].firstRow()!!
            return NamespaceInfo(namespaceId = row[3].asLong(), name = name)
        }

    override fun listNamespaces(
        catalog: String,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): List<NamespaceInfo> =
        catalogOperation(catalog) {
            val m = meta(catalog)
            val rows =
                if (snapshot == null && atTimestamp == null) {
                    queryCatalog(
                        catalog,
                        "SELECT $NAMESPACE_COLUMNS FROM namespaces WHERE dropped_snapshot IS NULL ORDER BY name",
                    )
                } else {
                    val at = resolveRead(catalog, m, snapshot, atTimestamp)
                    // The containment columns are not versioned: a pinned
                    // read gets the current containment, as on Postgres.
                    queryCatalog(
                        catalog,
                        "SELECT $NAMESPACE_COLUMNS FROM namespaces WHERE created_snapshot <= ? " +
                            "AND (dropped_snapshot IS NULL OR dropped_snapshot > ?) ORDER BY name",
                        at,
                        at,
                    )
                }
            return rows.rows.map { namespaceInfo(it) }
        }

    override fun getNamespace(
        catalog: String,
        namespace: String,
    ): NamespaceInfo =
        catalogOperation(catalog) {
            meta(catalog)
            return liveNamespace(catalog, namespace)
        }

    /**
     * A contained namespace needs [mayDropContained]. The pre-flight row
     * refuses first. Postgres reads containment under the commit lock that
     * marking also takes. Here a guard in the drop transaction stands in
     * for that lock, so a mark that lands after the pre-flight read still
     * refuses the drop.
     */
    override fun dropNamespace(
        catalog: String,
        namespace: String,
        expectedNamespaceId: Long?,
        author: String?,
        message: String?,
        mayDropContained: Boolean,
    ): CommitResult =
        catalogOperation(catalog) {
            requireOperationMetadata(author, message)
            val ns = liveNamespace(catalog, namespace)
            if (expectedNamespaceId != null && ns.namespaceId != expectedNamespaceId) {
                throw HoglakeException.CommitConflict(
                    "namespace '$namespace' no longer has the expected identity",
                )
            }
            val containedRefusal = {
                HoglakeException.Forbidden("namespace '$namespace' is contained; only an operator may drop it")
            }
            if (ns.containment != null && !mayDropContained) throw containedRefusal()
            val tx = HogliteTx()
            if (!mayDropContained) {
                tx.expectZero(
                    "SELECT COUNT(*) FROM namespaces WHERE id = ? AND contained_field IS NOT NULL",
                    ns.namespaceId,
                    failure = containedRefusal,
                )
            }
            // Emptiness under hoglite's per-tenant serialization: the guard
            // runs IN the transaction that flips the flag, so the live set
            // it counts is the live set the drop commits against.
            tx.expectZero(
                "SELECT COUNT(*) FROM tables WHERE namespace_id = ? AND dropped_snapshot IS NULL",
                ns.namespaceId,
            ) {
                HoglakeException.NamespaceNotEmpty(
                    "namespace '$namespace' in catalog '$catalog' is not empty: live table(s) remain",
                )
            }
            tx.expectZero(
                "SELECT COUNT(*) FROM views WHERE namespace_id = ? AND dropped_snapshot IS NULL",
                ns.namespaceId,
            ) {
                HoglakeException.NamespaceNotEmpty(
                    "namespace '$namespace' in catalog '$catalog' is not empty: live view(s) remain",
                )
            }
            tx.expectOne(
                "SELECT COUNT(*) FROM namespaces WHERE id = ? AND dropped_snapshot IS NULL",
                ns.namespaceId,
            ) { HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'") }
            tx.allocateDdlSnapshot()
            tx.affectOne(
                "UPDATE namespaces SET dropped_snapshot = ${HogliteSchema.HEAD_SQL} WHERE id = ?",
                ns.namespaceId,
            ) { HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'") }
            tx.add(
                "INSERT INTO changes(snapshot_id, kind, object_id) " +
                    "VALUES (${HogliteSchema.HEAD_SQL}, 'namespace_dropped', ?)",
                ns.namespaceId,
            )
            tx.insertSnapshotRow(author, message)
            val readback = tx.add(HogliteSql.READBACK_DDL)
            val results = runTx(catalog, freshTxid("drop-ns"), null, tx)
            val row = results[readback].firstRow()!!
            return CommitResult(snapshotId = row[0].asLong(), schemaVersion = row[1].asLong())
        }

    // ==================== namespace containment ============================

    /**
     * CatalogService.setNamespaceContainment on hoglite: the same
     * refusals, with the same messages, in the same order.
     *
     * Postgres runs an unlocked pre-flight transaction with the prefix
     * scan, then re-checks under the commit lock. Here the pre-flight is
     * reads: one [readCatalog] snapshot, then the paged scan
     * ([foreignPathUnderPrefix]). The re-check is the guards of ONE
     * transaction. Its statements, after the drop fence:
     *
     *  - 0: the namespace is live, else NotFound.
     *  - A mark, 1: the namespace is still uncontained, else CommitConflict
     *    ("clear it first").
     *  - A mark, 2-4: the namespace never held a table or a view, and no
     *    other namespace has had its name.
     *  - A mark, 5: no file, DV or undrained removal registered after the
     *    scan sits under the prefix ([prefixRaceArms]).
     *  - A mark, 6: no other live namespace holds the same containment,
     *    else AlreadyExists. A unique-index violation would surface as a
     *    422 Validation, so this guard names the refusal. The
     *    `namespaces_live_containment` index stays as the backstop.
     *  - A clear, 1-2: no live table or view.
     *  - Last: the UPDATE.
     *
     * A clear has no state guard. Under the lock, Postgres clears the
     * containment the namespace has at that time, and an UPDATE to NULL
     * gives the same result. A mark that finds the namespace contained
     * with the same value by a concurrent mark refuses with the
     * "clear it first" conflict. Postgres returns the namespace for that
     * race.
     *
     * A mark or a clear allocates no snapshot and writes no change row:
     * on Postgres it is a plain UPDATE too. This store audits no
     * operation, so the namespace_containment_set and _clear events stay
     * with CatalogService.
     */
    override fun setNamespaceContainment(
        catalog: String,
        namespace: String,
        containment: NamespaceContainment?,
        author: String?,
    ): NamespaceInfo =
        catalogOperation(catalog) {
            containment?.let {
                if (!CONTAINMENT_FIELD.matches(it.field)) {
                    throw HoglakeException.Validation("containment field must match ${CONTAINMENT_FIELD.pattern}")
                }
                if (it.value <= 0) throw HoglakeException.Validation("containment value must be a positive id")
            }
            val liveId = "(SELECT id FROM namespaces WHERE name = ? AND dropped_snapshot IS NULL)"
            val pre =
                readCatalog(
                    catalog,
                    listOf(
                        HogliteStatement(metaSql),
                        HogliteStatement(
                            "SELECT $NAMESPACE_COLUMNS FROM namespaces WHERE name = ? AND dropped_snapshot IS NULL",
                            listOf(namespace),
                        ),
                        // The scan watermark, read BEFORE the scan, so every
                        // row the scan cannot see sits above it.
                        HogliteStatement(
                            "SELECT (SELECT next_file_id FROM meta WHERE id = 1), " +
                                "(SELECT COALESCE(MAX(rowid), 0) FROM dvs), " +
                                "(SELECT COALESCE(MAX(id), 0) FROM removals)",
                        ),
                        HogliteStatement(
                            "SELECT (SELECT COUNT(*) FROM table_versions WHERE namespace_id = $liveId) + " +
                                "(SELECT COUNT(*) FROM views WHERE namespace_id = $liveId)",
                            listOf(namespace, namespace),
                        ),
                        HogliteStatement(
                            "SELECT COUNT(*) FROM namespaces WHERE name = ? AND id <> $liveId",
                            listOf(namespace, namespace),
                        ),
                    ),
                )
            val meta = metaRow(pre[0].firstRow() ?: throw HoglakeException.NotFound("catalog '$catalog'"))
            val ns =
                pre[1].firstRow()?.let { namespaceInfo(it) }
                    ?: throw HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'")
            if (ns.containment == containment) return ns
            if (containment != null && ns.containment != null) throw alreadyContained(namespace, ns.containment)
            val tx = HogliteTx()
            tx.expectOne(
                "SELECT COUNT(*) FROM namespaces WHERE id = ? AND dropped_snapshot IS NULL",
                ns.namespaceId,
            ) { HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'") }
            if (containment != null) {
                // The cheap refusals first, as on Postgres: a namespace that
                // held tables is told so, not that its own files sit under
                // its prefix.
                if (pre[3].firstRow()!![0].asLong() > 0) throw heldRefusal(namespace)
                if (pre[4].firstRow()!![0].asLong() > 0) throw nameReusedRefusal(namespace)
                foreignPathUnderPrefix(catalog, meta.dataPath, namespace)?.let { throw prefixTaken(namespace, it) }

                tx.expectZero(
                    "SELECT COUNT(*) FROM namespaces WHERE id = ? AND contained_field IS NOT NULL",
                    ns.namespaceId,
                ) {
                    // Runs after the rollback: this read sees the containment
                    // that refused the mark. An if, not an Elvis: see
                    // HogliteTx.classify.
                    val now =
                        queryCatalog(
                            catalog,
                            "SELECT contained_field, contained_value FROM namespaces WHERE id = ?",
                            ns.namespaceId,
                        ).firstRow()?.takeIf { it[0] != null }
                    if (now != null) {
                        alreadyContained(namespace, NamespaceContainment(now[0].asString(), now[1].asLong()))
                    } else {
                        HoglakeException.CommitConflict(
                            "namespace '$namespace' changed containment while it was being marked; retry",
                        )
                    }
                }
                tx.expectZero("SELECT COUNT(*) FROM table_versions WHERE namespace_id = ?", ns.namespaceId) {
                    heldRefusal(namespace)
                }
                tx.expectZero("SELECT COUNT(*) FROM views WHERE namespace_id = ?", ns.namespaceId) {
                    heldRefusal(namespace)
                }
                tx.expectZero("SELECT COUNT(*) FROM namespaces WHERE name = ? AND id <> ?", namespace, ns.namespaceId) {
                    nameReusedRefusal(namespace)
                }
                val watermark = pre[2].firstRow()!!
                val arms =
                    prefixRaceArms(
                        meta.dataPath,
                        namespace,
                        fromFileId = watermark[0].asLong(),
                        fromDvRowid = watermark[1].asLong(),
                        fromRemovalId = watermark[2].asLong(),
                    )
                tx.expectZero(
                    "SELECT " + arms.joinToString(" + ") { "(SELECT COUNT(*) FROM ${it.first})" },
                    *arms.flatMap { it.second }.toTypedArray(),
                ) {
                    val hit =
                        queryCatalog(
                            catalog,
                            arms.joinToString(" UNION ALL ") { "SELECT path FROM ${it.first}" } + " LIMIT 1",
                            *arms.flatMap { it.second }.toTypedArray(),
                        ).firstRow()?.get(0)?.asString()
                    if (hit != null) {
                        prefixTaken(namespace, hit)
                    } else {
                        HoglakeException.CommitConflict(
                            "cannot mark namespace '$namespace' contained: a file was registered under its prefix " +
                                "while it was being marked",
                        )
                    }
                }
                tx.expectZero(
                    "SELECT COUNT(*) FROM namespaces WHERE contained_field = ? AND contained_value = ? " +
                        "AND dropped_snapshot IS NULL AND id <> ?",
                    containment.field,
                    containment.value,
                    ns.namespaceId,
                ) {
                    HoglakeException.AlreadyExists(
                        "another namespace is already contained for ${containment.field} = ${containment.value}",
                    )
                }
            } else {
                val notEmpty = {
                    HoglakeException.NamespaceNotEmpty(
                        "namespace '$namespace' still holds tables or views; containment can only be " +
                            "cleared from an empty namespace",
                    )
                }
                tx.expectZero(
                    "SELECT COUNT(*) FROM tables WHERE namespace_id = ? AND dropped_snapshot IS NULL",
                    ns.namespaceId,
                    failure = notEmpty,
                )
                tx.expectZero(
                    "SELECT COUNT(*) FROM views WHERE namespace_id = ? AND dropped_snapshot IS NULL",
                    ns.namespaceId,
                    failure = notEmpty,
                )
            }
            tx.affectOne(
                "UPDATE namespaces SET contained_field = ?, contained_value = ? " +
                    "WHERE id = ? AND dropped_snapshot IS NULL",
                containment?.field,
                containment?.value,
                ns.namespaceId,
            ) { HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'") }
            runTx(catalog, freshTxid("ns-containment"), null, tx)
            return ns.copy(containment = containment)
        }

    /**
     * The namespace gate's read (auth/NamespaceGate.kt): one probe of the
     * `namespaces_live` index. Null when the catalog or the namespace is
     * missing, as CatalogService.liveContainment answers. The catch is
     * outside the operation: a bound tenant's 404 poisons the operation,
     * and its end raises that NotFound again.
     */
    override fun liveContainment(
        catalog: String,
        namespace: String,
    ): NamespaceContainment? =
        try {
            catalogOperation(catalog) {
                queryCatalog(
                    catalog,
                    "SELECT contained_field, contained_value FROM namespaces " +
                        "WHERE name = ? AND dropped_snapshot IS NULL",
                    namespace,
                ).firstRow()?.takeIf { it[0] != null }?.let { NamespaceContainment(it[0].asString(), it[1].asLong()) }
            }
        } catch (_: HoglakeException.NotFound) {
            null
        }

    /**
     * Another table's file, DV, live upload claim or undrained removal
     * under [namespace]'s prefix, in any spelling that
     * [Containment.segments] resolves to it, or null. The unlocked pass of
     * CatalogService's foreignFileUnderPrefix.
     *
     * The literal prefix is one range read per table: SQLite's BINARY
     * collation keeps byte order, so `path >= p AND path < p'` walks the
     * path index. SQLite has no regex, so the escaped spellings are a
     * paged walk in path order over the paths under the root that hold a
     * `%`. Kotlin then keeps the paths that [Containment.segments]
     * resolves to [namespace]. The commit check uses the same decoder.
     * Pages resume at the last path, not at an id: the path index gives
     * the order, and a path repeated past a page edge has one verdict.
     *
     * hoglite has no per-statement timeout. The bound is a deadline of
     * [CatalogService.CONTAINMENT_SCAN_TIMEOUT_MS], checked before each
     * read, so one read can run past it by the client's retry budget. An
     * expired local deadline refuses the mark with ContainmentUnverified
     * (503 `containment_unverified`). Transport uncertainty has the same result.
     * Deterministic engine limits (`query_timeout`, `result_too_large`,
     * `out_of_memory`) remain final 422 refusals, including during this scan.
     * No failed scan marks the namespace.
     */
    private fun foreignPathUnderPrefix(
        catalog: String,
        dataPath: String,
        namespace: String,
    ): String? {
        val root = dataPath.trimEnd('/') + "/"
        val prefix = root + "data/" + namespace + "/"
        val deadline = System.nanoTime() + CatalogService.CONTAINMENT_SCAN_TIMEOUT_MS * 1_000_000

        fun unverified(cause: Throwable?) =
            HoglakeException.ContainmentUnverified(
                "could not check $prefix for other tables' files within its time bound; " +
                    "the namespace was not marked",
            ).also { e -> cause?.let { e.initCause(it) } }

        fun <T> bounded(read: () -> T): T {
            if (System.nanoTime() - deadline > 0) throw unverified(null)
            return try {
                read()
            } catch (e: HoglakeException.BackendUnavailable) {
                throw unverified(e)
            }
        }
        // (table, the predicate that keeps its rows in scope). Claims and
        // removals count only while their object can still be there.
        val sources =
            listOf(
                "files" to "",
                "dvs" to "",
                "uploads" to " AND state = 'active'",
                "removals" to " AND drained_at IS NULL",
            )
        val literal =
            bounded {
                readCatalog(
                    catalog,
                    sources.map { (table, live) ->
                        HogliteStatement(
                            "SELECT path FROM $table WHERE path >= ? AND path < ?$live LIMIT 1",
                            listOf(prefix, pastPrefix(prefix)),
                        )
                    },
                )
            }
        literal.firstNotNullOfOrNull { it.firstRow()?.get(0)?.asString() }?.let { return it }
        for ((table, live) in sources) {
            var after: String? = null
            while (true) {
                val page =
                    bounded {
                        queryCatalog(
                            catalog,
                            "SELECT path FROM $table WHERE path ${if (after == null) ">=" else ">"} ? " +
                                "AND path < ?$live AND instr(path, '%') > 0 ORDER BY path LIMIT $CONTAINMENT_SCAN_PAGE",
                            after ?: root,
                            pastPrefix(root),
                        ).rows.map { it[0].asString() }
                    }
                page.firstOrNull { namespace in Containment.segments(dataPath, it) }?.let { return it }
                if (page.size < CONTAINMENT_SCAN_PAGE) break
                after = page.last()
            }
        }
        return null
    }

    /**
     * The mark transaction's prefix guard: (FROM clause, its arguments)
     * per arm. It closes the window between the scan and the transaction.
     * Postgres closes it with the locked pass of foreignFileUnderPrefix.
     *
     * The literal arms take every row under the prefix. The scan found
     * none, so each one there now is new. They walk the path indexes.
     *
     * No Kotlin runs between the statements of a hoglite transaction, so
     * the escaped arms are a SQL superset: any path under the root that
     * holds a `%`, on a row above the watermark. A hit refuses the mark,
     * and a retry scans again with a new watermark. The unary `+` keeps
     * SQLite off the path indexes: the id range is the narrow one. DVs
     * have no index on delete_file_id, so their arm uses rowid. A
     * rowid only grows while nothing deletes the DV with the largest
     * rowid, and no hoglite code deletes DV rows. Claims are not
     * re-read, as on Postgres: a claim cannot register a file into the
     * prefix, because the commit check refuses it.
     */
    private fun prefixRaceArms(
        dataPath: String,
        namespace: String,
        fromFileId: Long,
        fromDvRowid: Long,
        fromRemovalId: Long,
    ): List<Pair<String, List<Any?>>> {
        val root = dataPath.trimEnd('/') + "/"
        val prefix = root + "data/" + namespace + "/"
        val literal = listOf(prefix, pastPrefix(prefix))
        val escaped = "+path >= ? AND +path < ? AND instr(path, '%') > 0"
        return listOf(
            "files WHERE path >= ? AND path < ?" to literal,
            "dvs WHERE path >= ? AND path < ?" to literal,
            "removals WHERE drained_at IS NULL AND path >= ? AND path < ?" to literal,
            "files WHERE id >= ? AND $escaped" to listOf(fromFileId, root, pastPrefix(root)),
            "dvs WHERE rowid > ? AND $escaped" to listOf(fromDvRowid, root, pastPrefix(root)),
            "removals WHERE drained_at IS NULL AND id > ? AND $escaped" to
                listOf(fromRemovalId, root, pastPrefix(root)),
        )
    }

    /** The least string above every string that starts with [prefix], which ends in `/`. */
    private fun pastPrefix(prefix: String): String {
        check(prefix.endsWith('/')) { "prefix '$prefix' must end with '/'" }
        // '0' follows '/' in byte order.
        return prefix.dropLast(1) + '0'
    }

    private fun alreadyContained(
        namespace: String,
        current: NamespaceContainment,
    ) = HoglakeException.CommitConflict(
        "namespace '$namespace' is already contained for ${current.field} = ${current.value}; clear it first",
    )

    private fun heldRefusal(namespace: String) =
        HoglakeException.NamespaceNotEmpty(
            "namespace '$namespace' has held a table or view; only a namespace that never has can be " +
                "marked contained",
        )

    private fun nameReusedRefusal(namespace: String) =
        HoglakeException.CommitConflict(
            "namespace name '$namespace' has belonged to another, dropped namespace in this catalog; " +
                "only a name no other namespace has had can be marked contained",
        )

    private fun prefixTaken(
        namespace: String,
        path: String,
    ) = HoglakeException.CommitConflict(
        "cannot mark namespace '$namespace' contained: '$path' is already registered or claimed under its prefix",
    )

    // ==================== tables ===========================================

    override fun createTable(
        catalog: String,
        namespace: String,
        name: String,
        columns: List<ColumnDef>,
        author: String?,
        message: String?,
    ): TableInfo =
        catalogOperation(catalog) {
            Identifiers.validate("table", name)
            if (columns.isEmpty()) {
                throw HoglakeException.Validation("table '$name' must have at least one column")
            }
            // The column-tree rules (nesting depth, synthetic child names,
            // decimal params, duplicates) are hoglake's own object — same
            // refusals, same messages, as the Postgres path.
            ColumnTrees.validate(columns)
            val ns = liveNamespace(catalog, namespace)
            val assigned = assignFieldIds(columns, firstFieldId = 1)
            val tableUuid = UUID.randomUUID()
            val maxFieldId = assigned.flatMap { it.selfAndDescendants() }.maxOf { it.fieldId }

            val tx = HogliteTx()
            tx.expectOne(
                "SELECT COUNT(*) FROM namespaces WHERE id = ? AND dropped_snapshot IS NULL",
                ns.namespaceId,
            ) { HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'") }
            tx.expectZero(
                "SELECT COUNT(*) FROM tables WHERE namespace = ? AND name = ? AND dropped_snapshot IS NULL",
                namespace,
                name,
            ) { HoglakeException.AlreadyExists("table '$namespace.$name' already exists in catalog '$catalog'") }
            tx.allocateDdlSnapshot()
            tx.affectOne(
                "UPDATE meta SET next_table_id = next_table_id + 1 WHERE id = 1",
            ) { HoglakeException.NotFound("catalog '$catalog'") }
            tx.add(
                "INSERT INTO tables(id, namespace_id, namespace, name, table_uuid, next_field_id, created_snapshot) " +
                    "VALUES ((SELECT next_table_id - 1 FROM meta WHERE id = 1), " +
                    "?, ?, ?, ?, ?, ${HogliteSchema.HEAD_SQL})",
                ns.namespaceId,
                namespace,
                name,
                tableUuid.toString(),
                maxFieldId + 1,
            )
            insertColumnRows(tx, "(SELECT next_table_id - 1 FROM meta WHERE id = 1)", assigned)
            tx.add(
                "INSERT INTO table_versions(table_id, namespace_id, namespace, name, begin_snapshot) " +
                    "VALUES ((SELECT next_table_id - 1 FROM meta WHERE id = 1), ?, ?, ?, ${HogliteSchema.HEAD_SQL})",
                ns.namespaceId,
                namespace,
                name,
            )
            tx.add(
                "INSERT INTO table_totals(table_id) VALUES ((SELECT next_table_id - 1 FROM meta WHERE id = 1))",
            )
            tx.add(
                "INSERT INTO changes(snapshot_id, kind, object_id) " +
                    "VALUES (${HogliteSchema.HEAD_SQL}, 'table_created', " +
                    "(SELECT next_table_id - 1 FROM meta WHERE id = 1))",
            )
            tx.insertSnapshotRow(author, message)
            val readback = tx.add(HogliteSql.READBACK_DDL)
            val results = runTx(catalog, freshTxid("create-table"), null, tx)
            val row = results[readback].firstRow()!!
            val snapshotId = row[0].asLong()
            return TableInfo(
                tableId = row[2].asLong(),
                tableUuid = tableUuid,
                namespace = namespace,
                name = name,
                columns = assigned,
                readSnapshotId = snapshotId,
                recordCount = 0,
                fileCount = 0,
                fileSizeBytes = 0,
                // snapshotId set ONLY here and in alter/creation receipts —
                // reads leave it null (the TableInfo KDoc's pyhoglake
                // contract).
                snapshotId = snapshotId,
            )
        }

    override fun getTable(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long?,
        atTimestamp: Instant?,
        totals: Boolean,
    ): TableInfo =
        catalogOperation(catalog) {
            if (snapshot == null && atTimestamp == null) return headTable(catalog, namespace, table, totals)
            val m = meta(catalog)
            val at = resolveRead(catalog, m, snapshot, atTimestamp)
            // A pinned read resolves the namespace incarnation at its snapshot, as listTables does.
            val ns = namespaceAt(catalog, namespace, at)
            val t = tableAt(catalog, ns.namespaceId, namespace, table, at)
            // table_totals is exact at head only, so a pinned read
            // aggregates the files visible at `at`.
            val counts =
                if (totals) {
                    val row =
                        queryCatalog(
                            catalog,
                            "SELECT COALESCE(SUM(record_count),0), COUNT(*), COALESCE(SUM(file_size_bytes),0) " +
                                "FROM files WHERE table_id = ? AND begin_snapshot <= ? " +
                                "AND (end_snapshot IS NULL OR end_snapshot > ?)",
                            t.tableId,
                            at,
                            at,
                        ).firstRow()!!
                    listOf(row[0].asLong(), row[1].asLong(), row[2].asLong())
                } else {
                    null
                }
            return tableInfo(catalog, namespace, table, t, at, counts)
        }

    /**
     * The alter path's view of a table: meta (the head), the live namespace
     * and the table's version row AT that head, in ONE read-only
     * transaction, so from one engine snapshot — [headTable]'s read without
     * the totals. Read apart (a live-row read, then a meta read), a rename
     * and a declaration change committing between the two gave a no-op
     * receipt the table's OLD name and properties beside the NEW head's
     * columns: a state that existed at no snapshot. The identity, the
     * head, and every versioned row then read at that head agree.
     */
    internal fun headTableRow(
        catalog: String,
        namespace: String,
        table: String,
    ): Triple<MetaRow, NamespaceInfo, TableRow> {
        val head = HogliteSchema.HEAD_SQL
        val (metaResult, nsResult, tableResult) =
            readCatalog(
                catalog,
                listOf(
                    HogliteStatement(metaSql),
                    HogliteStatement(LIVE_NAMESPACE_SQL, listOf(namespace)),
                    HogliteStatement(
                        "SELECT t.id, t.table_uuid, v.namespace_id, v.namespace, v.name, v.comment, " +
                            "v.properties_json, t.next_row_id, t.schema_version, $head" +
                            " FROM table_versions v JOIN tables t ON t.id = v.table_id " +
                            "WHERE v.namespace_id = $LIVE_NAMESPACE_ID_SQL AND v.name = ? " +
                            "AND v.begin_snapshot <= $head AND (v.end_snapshot IS NULL OR v.end_snapshot > $head) " +
                            "AND t.created_snapshot <= $head " +
                            "AND (t.dropped_snapshot IS NULL OR t.dropped_snapshot > $head)",
                        listOf(namespace, table),
                    ),
                ),
            )
        val m = metaRow(metaResult.firstRow() ?: throw HoglakeException.NotFound("catalog '$catalog'"))
        val ns =
            nsResult.firstRow()?.let { namespaceInfo(it) }
                ?: throw HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'")
        val row =
            tableResult.firstRow()
                ?: throw HoglakeException.NotFound("table '$namespace.$table' in catalog '$catalog'")
        check(
            row[9].asLong() == m.head,
        ) { "head read saw head ${row[9]} beside meta head ${m.head} in one transaction" }
        return Triple(m, ns, tableRowOf(row.subList(0, 9)))
    }

    /**
     * A head getTable: meta, the live namespace and the table row (with its
     * table_totals) in ONE read-only transaction, so from one engine
     * snapshot (#537). The head (meta.next_snapshot) and table_totals are
     * the only reads that no begin/end snapshot pins, so only the snapshot
     * that read the head can serve totals that describe it. Every `at` in
     * the table statement is the head inlined, and the namespace id is a
     * subselect, so no argument carries a value from another snapshot.
     * The reads after it are pinned at the returned head, so a later
     * /query of each returns what this snapshot would have.
     */
    private fun headTable(
        catalog: String,
        namespace: String,
        table: String,
        totals: Boolean,
    ): TableInfo {
        val head = HogliteSchema.HEAD_SQL
        val (metaResult, nsResult, tableResult) =
            readCatalog(
                catalog,
                listOf(
                    HogliteStatement(metaSql),
                    HogliteStatement(LIVE_NAMESPACE_SQL, listOf(namespace)),
                    // Column 9 is the head the statement saw; the totals follow it.
                    HogliteStatement(
                        "SELECT t.id, t.table_uuid, v.namespace_id, v.namespace, v.name, v.comment, " +
                            "v.properties_json, t.next_row_id, t.schema_version, $head" +
                            (if (totals) ", tt.record_count, tt.file_count, tt.file_size_bytes" else "") +
                            " FROM table_versions v JOIN tables t ON t.id = v.table_id " +
                            (if (totals) "LEFT JOIN table_totals tt ON tt.table_id = t.id " else "") +
                            "WHERE v.namespace_id = $LIVE_NAMESPACE_ID_SQL AND v.name = ? " +
                            "AND v.begin_snapshot <= $head AND (v.end_snapshot IS NULL OR v.end_snapshot > $head) " +
                            "AND t.created_snapshot <= $head " +
                            "AND (t.dropped_snapshot IS NULL OR t.dropped_snapshot > $head)",
                        listOf(namespace, table),
                    ),
                ),
            )
        val m = metaRow(metaResult.firstRow() ?: throw HoglakeException.NotFound("catalog '$catalog'"))
        val at = resolveRead(catalog, m, null, null)
        nsResult.firstRow() ?: throw HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'")
        val row =
            tableResult.firstRow()
                ?: throw HoglakeException.NotFound("table '$namespace.$table' in catalog '$catalog' at snapshot $at")
        // One transaction is one engine snapshot, so a statement head that
        // differs from the meta row is an engine defect, never a race.
        check(row[9].asLong() == at) { "head read saw head ${row[9]} beside meta head $at in one transaction" }
        // Exact, maintained transactionally by every commit — the hoglite
        // backend has no sampler lag, so head totals are not a sample and
        // carry no freshness fields (null, per the TableInfo KDoc:
        // freshness fields are for samples). A table with no files has no
        // table_totals row.
        val counts = if (totals) (10..12).map { row[it].asLongOrNull() ?: 0L } else null
        return tableInfo(catalog, namespace, table, tableRowOf(row.subList(0, 9)), at, counts)
    }

    /** The rest of a getTable at [at]: each read is pinned at [at]. [counts] is record, file and byte totals. */
    private fun tableInfo(
        catalog: String,
        namespace: String,
        table: String,
        t: TableRow,
        at: Long,
        counts: List<Long>?,
    ): TableInfo {
        val columns = columnsAt(catalog, t.tableId, at)
        return TableInfo(
            tableId = t.tableId,
            tableUuid = t.tableUuid,
            namespace = namespace,
            name = table,
            columns = columns,
            readSnapshotId = at,
            recordCount = counts?.get(0),
            fileCount = counts?.get(1),
            fileSizeBytes = counts?.get(2),
            partitionSpec = specAt(catalog, t.tableId, at),
            sortSpec = sortSpecAt(catalog, t.tableId, at),
            comment = t.comment,
            properties = t.properties,
            // The option as it was at `at`, named by the column at `at`
            // (CatalogService.getTable): the field is top-level and can
            // be neither dropped nor renamed while the option is on.
            objectPrefixField =
                layoutAt(catalog, t.tableId, at)?.let { id ->
                    columns.single { it.fieldId == id }.def.name
                },
        )
    }

    override fun listTables(
        catalog: String,
        namespace: String,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): List<TableSummaryInfo> =
        catalogOperation(catalog) {
            if (snapshot == null && atTimestamp == null) return headListing(catalog, namespace)
            val m = meta(catalog)
            val at = resolveRead(catalog, m, snapshot, atTimestamp)
            // A pinned listing resolves the namespace incarnation at its snapshot.
            val ns = namespaceAt(catalog, namespace, at)
            // Totals are the files visible at `at`, as Postgres's listing
            // rolls them up at its snapshot: a PINNED listing aggregates the
            // manifest at `at` per table, never the head counters
            // (table_totals is exact at head only, see headListing).
            val visible =
                "FROM files f WHERE f.table_id = t.id AND f.begin_snapshot <= ? " +
                    "AND (f.end_snapshot IS NULL OR f.end_snapshot > ?)"
            val totalsSql =
                "(SELECT COALESCE(SUM(f.record_count),0) $visible), (SELECT COUNT(*) $visible), " +
                    "(SELECT COALESCE(SUM(f.file_size_bytes),0) $visible)"
            // snapshotCount/earliestSnapshotId count RETAINED change rows, so
            // the window opens at the expiry floor — read from meta so the
            // day expiry lands the read is already right.
            // Object ids overlap across tables, namespaces, and views.
            val tableKinds = ChangeKind.TABLE_SCOPED.map { it.wire }.toTypedArray()
            val kindParams = tableKinds.joinToString(",") { "?" }
            // Column 9 (the head) and the table_totals join are unused here.
            // They keep the pinned statement as it was before the head
            // listing moved to its own transaction (#537).
            val rows =
                queryCatalog(
                    catalog,
                    """SELECT t.id, t.table_uuid, v.name, v.comment, $totalsSql,
                              (SELECT COUNT(DISTINCT c.snapshot_id) FROM changes c
                                WHERE c.object_id = t.id AND c.kind IN ($kindParams)
                                  AND c.snapshot_id >= ? AND c.snapshot_id <= ?),
                              (SELECT MIN(c.snapshot_id) FROM changes c
                                WHERE c.object_id = t.id AND c.kind IN ($kindParams)
                                  AND c.snapshot_id >= ? AND c.snapshot_id <= ?),
                              ${HogliteSchema.HEAD_SQL}
                         FROM tables t
                         JOIN table_versions v ON v.table_id = t.id
                          AND v.begin_snapshot <= ? AND (v.end_snapshot IS NULL OR v.end_snapshot > ?)
                         LEFT JOIN table_totals tt ON tt.table_id = t.id
                        WHERE v.namespace_id = ?
                          AND t.created_snapshot <= ? AND (t.dropped_snapshot IS NULL OR t.dropped_snapshot > ?)
                        ORDER BY v.name""",
                    at,
                    at,
                    at,
                    at,
                    at,
                    at,
                    *tableKinds,
                    m.earliestSnapshotId,
                    at,
                    *tableKinds,
                    m.earliestSnapshotId,
                    at,
                    at,
                    at,
                    ns.namespaceId,
                    at,
                    at,
                )
            return rows.rows.map { tableSummaryOf(it) }
        }

    /**
     * A head listTables: meta, the live namespace and the listing in ONE
     * read-only transaction, so from one engine snapshot (#537). As in
     * [headTable], the head and table_totals are the only reads that no
     * begin/end snapshot pins, so they must come from the same snapshot.
     * Every `at`, the namespace id and the expiry floor are subselects in
     * the listing, so no argument carries a value from another snapshot.
     * The namespace statement tells a missing namespace (NotFound) from
     * an empty one (no rows).
     */
    private fun headListing(
        catalog: String,
        namespace: String,
    ): List<TableSummaryInfo> {
        val head = HogliteSchema.HEAD_SQL
        // snapshotCount/earliestSnapshotId count RETAINED change rows, so the
        // window opens at the expiry floor. Object ids overlap across tables,
        // namespaces, and views.
        val floor = "(SELECT earliest_snapshot_id FROM meta WHERE id = 1)"
        val tableKinds = ChangeKind.TABLE_SCOPED.map { it.wire }
        val kindParams = tableKinds.joinToString(",") { "?" }
        val (metaResult, nsResult, listing) =
            readCatalog(
                catalog,
                listOf(
                    HogliteStatement(metaSql),
                    HogliteStatement(LIVE_NAMESPACE_SQL, listOf(namespace)),
                    // Column 9 is the head the statement saw.
                    HogliteStatement(
                        """SELECT t.id, t.table_uuid, v.name, v.comment,
                                  COALESCE(tt.record_count,0), COALESCE(tt.file_count,0), COALESCE(tt.file_size_bytes,0),
                                  (SELECT COUNT(DISTINCT c.snapshot_id) FROM changes c
                                    WHERE c.object_id = t.id AND c.kind IN ($kindParams)
                                      AND c.snapshot_id >= $floor AND c.snapshot_id <= $head),
                                  (SELECT MIN(c.snapshot_id) FROM changes c
                                    WHERE c.object_id = t.id AND c.kind IN ($kindParams)
                                      AND c.snapshot_id >= $floor AND c.snapshot_id <= $head),
                                  $head
                             FROM tables t
                             JOIN table_versions v ON v.table_id = t.id
                              AND v.begin_snapshot <= $head AND (v.end_snapshot IS NULL OR v.end_snapshot > $head)
                             LEFT JOIN table_totals tt ON tt.table_id = t.id
                            WHERE v.namespace_id = $LIVE_NAMESPACE_ID_SQL
                              AND t.created_snapshot <= $head AND (t.dropped_snapshot IS NULL OR t.dropped_snapshot > $head)
                            ORDER BY v.name""",
                        tableKinds + tableKinds + namespace,
                    ),
                ),
            )
        val m = metaRow(metaResult.firstRow() ?: throw HoglakeException.NotFound("catalog '$catalog'"))
        nsResult.firstRow() ?: throw HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'")
        // One transaction is one engine snapshot, so a statement head that
        // differs from the meta row is an engine defect, never a race.
        check(listing.rows.all { it[9].asLong() == m.head }) {
            "head listing saw a head other than meta head ${m.head} in one transaction"
        }
        return listing.rows.map { tableSummaryOf(it) }
    }

    /** One listing row: columns 0-8 of the head and the pinned listing alike. */
    private fun tableSummaryOf(r: List<Any?>): TableSummaryInfo =
        TableSummaryInfo(
            tableId = r[0].asLong(),
            tableUuid = UUID.fromString(r[1].asString()),
            name = r[2].asString(),
            comment = r[3] as? String,
            recordCount = r[4].asLong(),
            fileCount = r[5].asLong(),
            fileSizeBytes = r[6].asLong(),
            snapshotCount = r[7].asLong(),
            earliestSnapshotId = r[8].asLongOrNull(),
        )

    override fun dropTable(
        catalog: String,
        namespace: String,
        table: String,
        expectedTableUuid: UUID?,
        author: String?,
        message: String?,
    ): CommitResult =
        catalogOperation(catalog) {
            requireOperationMetadata(author, message)
            val t = liveTable(catalog, namespace, table)
            if (expectedTableUuid != null && t.tableUuid != expectedTableUuid) {
                throw HoglakeException.TableRecreated(
                    "table '$namespace.$table' was dropped and recreated: expected incarnation " +
                        "$expectedTableUuid, live incarnation is ${t.tableUuid}",
                    table = "$namespace.$table",
                    expectedTableUuid = expectedTableUuid,
                    currentTableUuid = t.tableUuid,
                )
            }
            // THE WHOLE DROP is the mark: file rows stay live on the dropped
            // table and RetirementService ends + queues them in paced batches
            // once the drop sinks under the expiry floor (hoglake #193:
            // ending 3M file rows under the commit lock was an outage).
            val tx = HogliteTx()
            tx.expectOne(
                "SELECT COUNT(*) FROM tables WHERE id = ? AND dropped_snapshot IS NULL",
                t.tableId,
            ) { HoglakeException.NotFound("table '$namespace.$table' in catalog '$catalog'") }
            tx.allocateDdlSnapshot()
            tx.affectOne(
                "UPDATE tables SET dropped_snapshot = ${HogliteSchema.HEAD_SQL} WHERE id = ?",
                t.tableId,
            ) { HoglakeException.NotFound("table '$namespace.$table' in catalog '$catalog'") }
            tx.affectOne(
                "UPDATE table_versions SET end_snapshot = ${HogliteSchema.HEAD_SQL} " +
                    "WHERE table_id = ? AND end_snapshot IS NULL",
                t.tableId,
            ) { HoglakeException.NotFound("table '$namespace.$table' in catalog '$catalog'") }
            tx.add(
                "UPDATE columns SET end_snapshot = ${HogliteSchema.HEAD_SQL} " +
                    "WHERE table_id = ? AND end_snapshot IS NULL",
                t.tableId,
            )
            tx.add(
                "INSERT INTO changes(snapshot_id, kind, object_id) " +
                    "VALUES (${HogliteSchema.HEAD_SQL}, 'table_dropped', ?)",
                t.tableId,
            )
            tx.insertSnapshotRow(author, message)
            val readback = tx.add(HogliteSql.READBACK_DDL)
            val results = runTx(catalog, freshTxid("drop-table"), null, tx)
            val row = results[readback].firstRow()!!
            return CommitResult(snapshotId = row[0].asLong(), schemaVersion = row[1].asLong())
        }

    override fun truncateTable(
        catalog: String,
        namespace: String,
        table: String,
        expectedTableUuid: UUID,
        author: String?,
        message: String?,
    ): CommitResult =
        catalogOperation(catalog) {
            requireOperationMetadata(author, message)
            val t = liveTable(catalog, namespace, table)
            if (t.tableUuid != expectedTableUuid) {
                throw HoglakeException.TableRecreated(
                    "table '$namespace.$table' was dropped and recreated: expected incarnation " +
                        "$expectedTableUuid, live incarnation is ${t.tableUuid}",
                    table = "$namespace.$table",
                    expectedTableUuid = expectedTableUuid,
                    currentTableUuid = t.tableUuid,
                )
            }
            val tx = HogliteTx()
            tx.expectOne(
                "SELECT COUNT(*) FROM tables WHERE id = ? AND dropped_snapshot IS NULL AND table_uuid = ?",
                t.tableId,
                expectedTableUuid.toString(),
            ) {
                HoglakeException.TableRecreated(
                    "table '$namespace.$table' was recreated under the truncate",
                    table = "$namespace.$table",
                    expectedTableUuid = expectedTableUuid,
                    currentTableUuid = t.tableUuid,
                )
            }
            tx.allocateDdlSnapshot()
            // End every live file and DV at the new snapshot and zero the
            // totals. The OBJECTS are not queued for removal here — a
            // retained pre-truncate snapshot still references them; the
            // expiry loop queues removals once the floor passes the end
            // snapshot (same as the Postgres path, where truncate only
            // calls endLiveFiles/endLiveDeleteFiles). The row-id allocator
            // stays: truncation must never reuse historical row ids.
            tx.add(
                "UPDATE dvs SET end_snapshot = ${HogliteSchema.HEAD_SQL} WHERE table_id = ? AND end_snapshot IS NULL",
                t.tableId,
            )
            tx.add(
                "UPDATE files SET end_snapshot = ${HogliteSchema.HEAD_SQL} WHERE table_id = ? AND end_snapshot IS NULL",
                t.tableId,
            )
            tx.add(
                "UPDATE table_totals SET record_count = 0, file_count = 0, file_size_bytes = 0 WHERE table_id = ?",
                t.tableId,
            )
            // The changefeed barrier pair (hoglake writes the same two rows):
            // 'table_altered' + 'table_deleted_from' in one snapshot is what
            // changes() refuses to represent — a consumer must reconcile
            // from a full scan.
            tx.add(
                "INSERT INTO changes(snapshot_id, kind, object_id) " +
                    "VALUES (${HogliteSchema.HEAD_SQL}, 'table_altered', ?)",
                t.tableId,
            )
            tx.add(
                "INSERT INTO changes(snapshot_id, kind, object_id) " +
                    "VALUES (${HogliteSchema.HEAD_SQL}, 'table_deleted_from', ?)",
                t.tableId,
            )
            tx.insertSnapshotRow(author, message)
            val readback = tx.add(HogliteSql.READBACK_DDL)
            val results = runTx(catalog, freshTxid("truncate"), null, tx)
            val row = results[readback].firstRow()!!
            return CommitResult(snapshotId = row[0].asLong(), schemaVersion = row[1].asLong())
        }

    // ==================== files + changefeed ===============================

    override fun listFiles(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long?,
        atTimestamp: Instant?,
        sort: FileRepo.FileSortColumn?,
        desc: Boolean,
        limit: Int?,
        offset: Int,
        partitionFilter: Map<Int, String>,
    ): List<DataFile> =
        catalogOperation(catalog) {
            val m = meta(catalog)
            val at = resolveRead(catalog, m, snapshot, atTimestamp)
            val ns = namespaceForRead(catalog, namespace, at, headRead = snapshot == null && atTimestamp == null)
            val t = tableAt(catalog, ns.namespaceId, namespace, table, at)
            if (partitionFilter.isNotEmpty()) {
                // A filter on a key the table does not partition by at `at`
                // can only be a client bug (or a stale spec), so refuse it
                // rather than return an empty page that reads as "no files".
                val arity = specAt(catalog, t.tableId, at)?.fields?.size ?: 0
                val bad = partitionFilter.keys.firstOrNull { it < 0 || it >= arity }
                if (bad != null) {
                    throw HoglakeException.Validation(
                        "partition key_index $bad is out of range for table '$namespace.$table' " +
                            "($arity partition keys)",
                    )
                }
            }
            val dir = if (desc) "DESC" else "ASC"
            val orderBy =
                if (sort == null) {
                    "begin_snapshot, row_id_start, id"
                } else {
                    // Fixed ascending id tiebreak: a stable total order under
                    // paging regardless of the primary direction.
                    "${sortColumnSql(sort)} $dir, id ASC"
                }
            val partitionWhere =
                partitionFilter.entries.sortedBy { it.key }.joinToString("") { (keyIndex, _) ->
                    " AND EXISTS (SELECT 1 FROM json_each(f.partition_values_json) pv " +
                        "WHERE pv.key = $keyIndex AND pv.value = ?)"
                }
            val prefix =
                "SELECT ${HogliteSql.FILE_COLUMNS} FROM files f WHERE f.table_id = ? " +
                    "AND f.begin_snapshot <= ? AND (f.end_snapshot IS NULL OR f.end_snapshot > ?)" + partitionWhere
            val baseArgs =
                buildList<Any?> {
                    add(t.tableId)
                    add(at)
                    add(at)
                    partitionFilter.entries.sortedBy { it.key }.forEach { add(it.value) }
                }
            val page = mutableListOf<FileRow>()
            var last: List<Any?>? = null
            var remaining = limit ?: Int.MAX_VALUE
            while (remaining > 0) {
                val cursor = last
                val cursorArgs = mutableListOf<Any?>()
                val after =
                    if (cursor == null) {
                        ""
                    } else if (sort == null) {
                        cursorArgs.addAll(listOf(cursor[14], cursor[7], cursor[0]))
                        " AND (f.begin_snapshot, f.row_id_start, f.id) > (?, ?, ?)"
                    } else {
                        val key =
                            cursor[
                                when (sort) {
                                    FileRepo.FileSortColumn.ID -> 0
                                    FileRepo.FileSortColumn.PATH -> 2
                                    FileRepo.FileSortColumn.RECORD_COUNT -> 4
                                    FileRepo.FileSortColumn.SIZE -> 5
                                    FileRepo.FileSortColumn.STATS -> 11
                                    FileRepo.FileSortColumn.BEGIN_SNAPSHOT -> 14
                                },
                            ]
                        cursorArgs.addAll(listOf(key, key, cursor[0]))
                        val comparison = if (desc) "<" else ">"
                        " AND (${sortColumnSql(sort)} $comparison ? OR (${sortColumnSql(sort)} = ? AND f.id > ?))"
                    }
                val count = minOf(remaining, SCAN_PAGE_SIZE)
                val initialOffset = if (last == null && limit != null) offset else 0
                val rows =
                    queryCatalog(
                        catalog,
                        prefix + after + " ORDER BY $orderBy LIMIT ? OFFSET ?",
                        *(baseArgs + cursorArgs + listOf(count, initialOffset)).toTypedArray(),
                    ).rows
                page.addAll(rows.map { toFileRow(it) })
                remaining -= rows.size
                if (rows.size < count) break
                last = rows.last()
            }
            if (meta(catalog).earliestSnapshotId > at) {
                throw HoglakeException.Expired("snapshot $at expired while listing files")
            }
            return withOrderingBounds(catalog, t.tableId, at, page)
        }

    override fun partitionValues(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): PartitionValues =
        catalogOperation(catalog) {
            val m = meta(catalog)
            val at = resolveRead(catalog, m, snapshot, atTimestamp)
            val ns = namespaceForRead(catalog, namespace, at, headRead = snapshot == null && atTimestamp == null)
            val t = tableAt(catalog, ns.namespaceId, namespace, table, at)
            val spec = specAt(catalog, t.tableId, at) ?: return PartitionValues(-1, emptyList())
            val fields =
                spec.fields.mapIndexed { keyIndex, field ->
                    val cap = PartitionValues.PARTITION_VALUES_CAP
                    // One past the cap, so "there are more" is
                    // distinguishable from "exactly cap values".
                    val rows =
                        queryCatalog(
                            catalog,
                            "SELECT pv.value, COUNT(*) AS n FROM files f, json_each(f.partition_values_json) pv " +
                                "WHERE f.table_id = ? AND f.begin_snapshot <= ? " +
                                "AND (f.end_snapshot IS NULL OR f.end_snapshot > ?) " +
                                "AND pv.key = ? GROUP BY pv.value ORDER BY n DESC, pv.value LIMIT ?",
                            t.tableId,
                            at,
                            at,
                            keyIndex,
                            cap + 1,
                        )
                    PartitionFieldValues(
                        sourceFieldId = field.sourceFieldId,
                        transform = field.transform,
                        transformParam = field.transformParam,
                        values = rows.rows.take(cap).map { it[0] as? String },
                        truncated = rows.rows.size > cap,
                    )
                }
            return PartitionValues(spec.specId, fields)
        }

    override fun fileStats(
        catalog: String,
        namespace: String,
        table: String,
        fileId: Long,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): FileStats =
        catalogOperation(catalog) {
            val m = meta(catalog)
            val at = resolveRead(catalog, m, snapshot, atTimestamp)
            val ns = namespaceForRead(catalog, namespace, at, headRead = snapshot == null && atTimestamp == null)
            val t = tableAt(catalog, ns.namespaceId, namespace, table, at)
            val row =
                queryCatalog(
                    catalog,
                    "SELECT ${HogliteSql.FILE_COLUMNS} FROM files f " +
                        "WHERE f.id = ? AND f.table_id = ? AND f.begin_snapshot <= ? " +
                        "AND (f.end_snapshot IS NULL OR f.end_snapshot > ?)",
                    fileId,
                    t.tableId,
                    at,
                    at,
                ).firstRow()
                    ?: throw HoglakeException.NotFound(
                        "file $fileId in table '$namespace.$table' in catalog '$catalog' at snapshot $at",
                    )
            val file = toFileRow(row)
            val columns = columnsAt(catalog, t.tableId, at)
            return FileStats(
                dataFileId = file.file.dataFileId,
                statsState = file.file.statsState,
                columns = resolveStoredStats(file.statsJson, columnsByFieldId(columns), null),
            )
        }

    override fun changes(
        catalog: String,
        namespace: String,
        table: String,
        fromSnapshot: Long,
        toSnapshot: Long?,
    ): ChangesPlan =
        catalogOperation(catalog) {
            val m = meta(catalog)
            val to =
                when {
                    toSnapshot == null -> m.head
                    toSnapshot > m.head ->
                        throw HoglakeException.Validation(
                            "to_snapshot $toSnapshot is beyond head ${m.head}",
                        )
                    else -> toSnapshot
                }
            if (fromSnapshot < 0) throw HoglakeException.Validation("from_snapshot must be >= 0, got $fromSnapshot")
            if (fromSnapshot > to) {
                throw HoglakeException.Validation("from_snapshot $fromSnapshot is beyond to_snapshot $to")
            }

            fun requireRetained(floor: Long) {
                if (fromSnapshot < floor - 1) {
                    throw HoglakeException.Expired(
                        "from_snapshot $fromSnapshot reaches below the expiry floor " +
                            "(earliest retained snapshot is $floor); part of the range is " +
                            "gone; reconcile by re-reading from a full scan at a retained snapshot",
                    )
                }
            }
            requireRetained(m.earliestSnapshotId)
            // An explicit to_snapshot pins the feed: its table and namespace
            // are the ones alive at `to`, as for every other pinned read.
            val ns = namespaceForRead(catalog, namespace, to, headRead = toSnapshot == null)
            val t = tableAt(catalog, ns.namespaceId, namespace, table, to)
            // TRUNCATE / replacement barrier: a window that crosses a
            // table_altered + table_deleted_from pair cannot be represented
            // by an append-oriented feed.
            val truncated =
                queryCatalog(
                    catalog,
                    "SELECT c.snapshot_id FROM changes c WHERE c.object_id = ? AND c.kind = 'table_deleted_from' " +
                        "AND c.snapshot_id > ? AND c.snapshot_id <= ? AND EXISTS (" +
                        "SELECT 1 FROM changes d WHERE d.object_id = c.object_id " +
                        "AND d.snapshot_id = c.snapshot_id AND d.kind = 'table_altered') " +
                        "ORDER BY c.snapshot_id LIMIT 1",
                    t.tableId,
                    fromSnapshot,
                    to,
                ).firstRow()
            if (truncated != null) {
                throw HoglakeException.ReconciliationRequired(
                    "table '$namespace.$table' was truncated or replaced at snapshot ${truncated[0].asLong()}; " +
                        "changefeed window ($fromSnapshot, $to] cannot represent this deletion. " +
                        "Reconcile the destination from a full snapshot before advancing its checkpoint.",
                )
            }
            // Compaction outputs rewrite rows that consumers already receive through the original files.
            val files = mutableListOf<DataFile>()
            var fileCursor: Pair<Long, Long>? = null
            while (true) {
                val cursor = fileCursor
                val lower =
                    if (cursor == null) {
                        "f.begin_snapshot > ?"
                    } else {
                        "(f.begin_snapshot, f.id) > (?, ?)"
                    }
                val args =
                    buildList<Any?> {
                        add(t.tableId)
                        if (cursor == null) {
                            add(fromSnapshot)
                        } else {
                            add(cursor.first)
                            add(cursor.second)
                        }
                        add(to)
                        add(SCAN_PAGE_SIZE)
                    }
                val rows =
                    queryCatalog(
                        catalog,
                        "SELECT ${HogliteSql.CHANGEFEED_FILE_COLUMNS} FROM files f " +
                            "WHERE f.table_id = ? AND $lower AND f.begin_snapshot <= ? " +
                            "AND NOT EXISTS (SELECT 1 FROM changes c WHERE c.snapshot_id = f.begin_snapshot " +
                            "AND c.kind = 'table_compacted' AND c.object_id = f.table_id)" +
                            " ORDER BY f.begin_snapshot, f.id LIMIT ?",
                        *args.toTypedArray(),
                    ).rows
                files.addAll(rows.map { toDataFile(it) })
                if (rows.size < SCAN_PAGE_SIZE) break
                fileCursor = rows.last().let { it[14].asLong() to it[0].asLong() }
            }
            val dvs = mutableListOf<DeleteFile>()
            var dvCursor: Pair<Long, Long>? = null
            while (true) {
                val cursor = dvCursor
                val lower =
                    if (cursor == null) {
                        "begin_snapshot > ?"
                    } else {
                        "(begin_snapshot, delete_file_id) > (?, ?)"
                    }
                val args =
                    buildList<Any?> {
                        add(t.tableId)
                        if (cursor == null) {
                            add(fromSnapshot)
                        } else {
                            add(cursor.first)
                            add(cursor.second)
                        }
                        add(to)
                        add(SCAN_PAGE_SIZE)
                    }
                val rows =
                    queryCatalog(
                        catalog,
                        "SELECT delete_file_id, data_file_id, path, delete_count, file_size_bytes, begin_snapshot " +
                            "FROM dvs WHERE table_id = ? AND $lower AND begin_snapshot <= ?" +
                            " ORDER BY begin_snapshot, delete_file_id LIMIT ?",
                        *args.toTypedArray(),
                    ).rows
                dvs.addAll(
                    rows.map { r ->
                        DeleteFile(
                            deleteFileId = r[0].asLong(),
                            dataFileId = r[1].asLong(),
                            path = r[2].asString(),
                            fileFormat = "puffin",
                            deleteCount = r[3].asLong(),
                            fileSizeBytes = r[4].asLong(),
                            beginSnapshot = r[5].asLong(),
                        )
                    },
                )
                if (rows.size < SCAN_PAGE_SIZE) break
                dvCursor = rows.last().let { it[5].asLong() to it[0].asLong() }
            }
            // Expiry can purge barriers or ended files/DVs between these reads.
            // Their snapshots precede the floor. Recheck the exclusive lower bound before returning.
            requireRetained(meta(catalog).earliestSnapshotId)
            return ChangesPlan(t.tableUuid, fromSnapshot, to, files, dvs)
        }

    // ==================== snapshots ========================================

    override fun resolveSnapshot(
        catalog: String,
        atTimestamp: Instant,
    ): SnapshotResolution =
        catalogOperation(catalog) {
            val m = meta(catalog)
            val at = resolveTimestamp(catalog, m, atTimestamp)
            val ts =
                queryCatalog(catalog, "SELECT ts FROM snapshots WHERE id = ?", at)
                    .firstRow()!![0].asString()
            return SnapshotResolution(at, HogliteSchema.parse(ts))
        }

    override fun listSnapshots(
        catalog: String,
        after: Long,
        limit: Int,
        before: Long?,
    ): Pair<List<Snapshot>, Boolean> =
        catalogOperation(catalog) {
            if (limit < 1) throw HoglakeException.Validation("limit must be >= 1, got $limit")
            if (before != null && after != 0L) {
                throw HoglakeException.Validation(
                    "'before' and a non-zero 'after' are mutually exclusive; supply at most one cursor",
                )
            }
            meta(catalog)
            // limit + 1 rows: the extra one answers "are there more".
            val rows =
                if (before != null) {
                    queryCatalog(
                        catalog,
                        "SELECT id, ts, schema_version, author, message FROM snapshots " +
                            "WHERE id < ? ORDER BY id DESC LIMIT ?",
                        before,
                        limit + 1,
                    )
                } else {
                    queryCatalog(
                        catalog,
                        "SELECT id, ts, schema_version, author, message FROM snapshots " +
                            "WHERE id > ? ORDER BY id ASC LIMIT ?",
                        after,
                        limit + 1,
                    )
                }
            val hasMore = rows.rows.size > limit
            val page = rows.rows.take(limit)
            if (page.isEmpty()) return Pair(emptyList(), false)
            val low = minOf(page.first()[0].asLong(), page.last()[0].asLong())
            val high = maxOf(page.first()[0].asLong(), page.last()[0].asLong())
            val changes =
                queryCatalog(
                    catalog,
                    "SELECT snapshot_id, kind, object_id FROM changes WHERE snapshot_id >= ? AND snapshot_id <= ? " +
                        "ORDER BY snapshot_id, kind, object_id",
                    low,
                    high,
                ).rows.groupBy(
                    { it[0].asLong() },
                ) { SnapshotChange(ChangeKind.fromWire(it[1].asString()), it[2].asLong()) }
            return Pair(
                page.map { r ->
                    Snapshot(
                        snapshotId = r[0].asLong(),
                        snapshotTime = HogliteSchema.parse(r[1].asString()),
                        schemaVersion = r[2].asLong(),
                        author = r[3] as? String,
                        message = r[4] as? String,
                        changes = changes[r[0].asLong()] ?: emptyList(),
                    )
                },
                hasMore,
            )
        }

    // ==================== read planning ====================================

    override fun planScan(
        catalog: String,
        namespace: String,
        table: String,
        snapshot: Long?,
        atTimestamp: Instant?,
        columnStats: ScanService.ColumnStatsRequest?,
        splitOffsets: Boolean,
    ): List<ScanFile> =
        catalogOperation(catalog) {
            val m = meta(catalog)
            val at = resolveRead(catalog, m, snapshot, atTimestamp)
            val ns = namespaceForRead(catalog, namespace, at, headRead = snapshot == null && atTimestamp == null)
            val t = tableAt(catalog, ns.namespaceId, namespace, table, at)
            // The DV join picks the newest epoch VISIBLE AT the pin: history-
            // preserving supersession means a pinned scan sees the vector
            // that was live then.
            // Only requested payload travels over the wire. Keyset pages share
            // one logical snapshot AND the operation's immutable tenant binding.
            val statsBytes = if (columnStats != null) "length(CAST(f.stats_json AS BLOB))" else "0"
            val offsetBytes = if (splitOffsets) "length(CAST(f.split_offsets_json AS BLOB))" else "0"

            fun boundedJson(
                column: String,
                bytes: String,
            ): String =
                if (bytes == "0") "NULL" else "CASE WHEN $bytes <= $SCAN_MAX_ROW_JSON_BYTES THEN $column ELSE NULL END"
            val projection =
                HogliteSql.FILE_COLUMNS
                    .replace("f.stats_json", boundedJson("f.stats_json", statsBytes))
                    .replace("f.split_offsets_json", boundedJson("f.split_offsets_json", offsetBytes))
            var afterRow = -1L
            var afterId = -1L
            val budget = HogliteScanBudget()
            var files: List<Pair<ScanFile, FileRow>>
            val collected = mutableListOf<Pair<ScanFile, FileRow>>()
            while (true) {
                val rows =
                    queryCatalog(
                        catalog,
                        """SELECT $projection,
                          d.delete_file_id, d.path, d.delete_count, d.file_size_bytes, d.begin_snapshot,
                          $statsBytes, $offsetBytes
                     FROM files f
                     LEFT JOIN dvs d ON d.data_file_id = f.id
                       AND d.begin_snapshot <= ? AND (d.end_snapshot IS NULL OR d.end_snapshot > ?)
                       AND d.epoch = (SELECT MAX(d2.epoch) FROM dvs d2
                                       WHERE d2.data_file_id = f.id AND d2.begin_snapshot <= ?
                                         AND (d2.end_snapshot IS NULL OR d2.end_snapshot > ?))
                    WHERE f.table_id = ? AND f.begin_snapshot <= ?
                      AND (f.end_snapshot IS NULL OR f.end_snapshot > ?)
                      AND (f.row_id_start, f.id) > (?, ?)
                    ORDER BY f.row_id_start, f.id LIMIT ?""",
                        at,
                        at,
                        at,
                        at,
                        t.tableId,
                        at,
                        at,
                        afterRow,
                        afterId,
                        SCAN_PAGE_SIZE,
                    )
                for (r in rows.rows) {
                    if ((r[20].asLongOrNull() ?: 0L) > SCAN_MAX_ROW_JSON_BYTES ||
                        (r[21].asLongOrNull() ?: 0L) > SCAN_MAX_ROW_JSON_BYTES
                    ) {
                        throw HoglakeException.Validation(
                            "scan plan file metadata exceeds maximum $SCAN_MAX_ROW_JSON_BYTES bytes",
                        )
                    }
                    // Conservative UTF-8 + JSON envelope budget, independent of
                    // HTTP timeouts. Refuse, never return a truncated scan.
                    budget.add(
                        512L + json.writeValueAsBytes(r).size,
                        (r[20].asLongOrNull() ?: 0L) + (r[21].asLongOrNull() ?: 0L),
                    )
                }
                collected.addAll(
                    rows.rows.map { r ->
                        val fr = toFileRow(r)
                        val dv =
                            if (r[15] == null) {
                                null
                            } else {
                                DeleteFile(
                                    deleteFileId = r[15].asLong(),
                                    dataFileId = fr.file.dataFileId,
                                    path = r[16].asString(),
                                    fileFormat = "puffin",
                                    deleteCount = r[17].asLong(),
                                    fileSizeBytes = r[18].asLong(),
                                    beginSnapshot = r[19].asLong(),
                                )
                            }
                        ScanFile(fr.file, dv) to fr
                    },
                )
                if (rows.rows.size < SCAN_PAGE_SIZE) break
                val last = rows.rows.last()
                afterRow = last[7].asLong()
                afterId = last[0].asLong()
            }
            files = collected
            // Expiry may advance between pages. Never return an incomplete
            // plan if the pin was purged while the pages were being read.
            resolveRead(catalog, meta(catalog), at, null)
            val providedFiles =
                if (columnStats == null) {
                    0L
                } else {
                    files.count { it.first.dataFile.statsState == StatsState.PROVIDED }.toLong()
                }
            if (columnStats != null && providedFiles > 0) {
                val columns = columnsAt(catalog, t.tableId, at)
                // Match PostgreSQL's column-tree contract, including childless
                // VARIANTs in the conservative cap. This does not generate rows
                // for columns with no stored statistics.
                val leaves = columns.flatMap { it.selfAndDescendants() }.filter { it.children.isEmpty() }
                val leafIds = leaves.map { it.fieldId }.toSet()
                com.posthog.hoglake.service.enforceScanColumnStatsEntryCap(providedFiles, leafIds, columnStats)
                val wanted = columnStats.fieldIds?.intersect(leafIds) ?: leafIds
                val byId = columnsByFieldId(columns)
                files =
                    files.map { (sf, fr) ->
                        // Eligibility is the state captured with the selected file,
                        // never a later hydration result. Payloads across queries
                        // are not a new frozen statistics snapshot.
                        if (sf.dataFile.statsState != StatsState.PROVIDED) {
                            sf to fr
                        } else {
                            val stats = resolveStoredStats(fr.statsJson, byId, wanted)
                            sf.copy(dataFile = sf.dataFile.copy(columnStats = stats)) to fr
                        }
                    }
            }
            if (splitOffsets) {
                files =
                    files.map { (sf, fr) ->
                        val offsets = fr.splitOffsetsJson?.let { json.readValue<List<Long>>(it) }
                        sf.copy(dataFile = sf.dataFile.copy(splitOffsets = offsets)) to fr
                    }
            }
            return files.map { it.first }
        }

    // ==================== consumer offsets =================================

    override fun listConsumers(catalog: String): List<ConsumerTableOffset> =
        catalogOperation(catalog) {
            meta(catalog)
            return queryCatalog(
                catalog,
                """SELECT o.consumer_id, o.table_uuid, o.committed_snapshot, o.updated_at,
                      t.namespace, t.name, CASE WHEN t.dropped_snapshot IS NULL THEN 0 ELSE 1 END
                 FROM offsets o LEFT JOIN tables t ON t.table_uuid = o.table_uuid
                ORDER BY o.consumer_id, t.namespace NULLS LAST, t.name NULLS LAST, o.table_uuid""",
            ).rows.map { r ->
                ConsumerTableOffset(
                    consumerId = r[0].asString(),
                    tableUuid = UUID.fromString(r[1].asString()),
                    committedSnapshot = r[2].asLong(),
                    updatedAt = HogliteSchema.parse(r[3].asString()),
                    namespace = r[4] as? String,
                    tableName = r[5] as? String,
                    tableDropped = r[6].asLongOrNull() == 1L,
                )
            }
        }

    override fun listOffsets(
        catalog: String,
        consumerId: String,
    ): List<ConsumerOffset> =
        catalogOperation(catalog) {
            meta(catalog)
            return queryCatalog(
                catalog,
                "SELECT consumer_id, table_uuid, committed_snapshot, updated_at FROM offsets " +
                    "WHERE consumer_id = ? ORDER BY table_uuid",
                consumerId,
            ).rows.map { r ->
                ConsumerOffset(
                    consumerId = r[0].asString(),
                    tableUuid = UUID.fromString(r[1].asString()),
                    committedSnapshot = r[2].asLong(),
                    updatedAt = HogliteSchema.parse(r[3].asString()),
                )
            }
        }

    override fun getOffset(
        catalog: String,
        consumerId: String,
        tableUuid: UUID,
    ): ConsumerOffset =
        catalogOperation(catalog) {
            meta(catalog)
            val row =
                queryCatalog(
                    catalog,
                    "SELECT consumer_id, table_uuid, committed_snapshot, updated_at FROM offsets " +
                        "WHERE consumer_id = ? AND table_uuid = ?",
                    consumerId,
                    tableUuid.toString(),
                ).firstRow()
                    ?: throw HoglakeException.NotFound(
                        "no offset for consumer '$consumerId' on table $tableUuid in catalog '$catalog'",
                    )
            return ConsumerOffset(
                consumerId = row[0].asString(),
                tableUuid = UUID.fromString(row[1].asString()),
                committedSnapshot = row[2].asLong(),
                updatedAt = HogliteSchema.parse(row[3].asString()),
            )
        }

    override fun commitOffset(
        catalog: String,
        consumerId: String,
        tableUuid: UUID,
        snapshotId: Long,
    ): ConsumerOffset =
        catalogOperation(catalog) {
            // Postgres enforces the consumer id's 1..128 length as a CHECK;
            // here it is a typed refusal instead of a SQL error.
            if (consumerId.isEmpty() || consumerId.length > 128) {
                throw HoglakeException.Validation("consumer_id must be 1..128 characters, got ${consumerId.length}")
            }
            val m = meta(catalog)
            if (snapshotId < 0 || snapshotId > m.head) {
                throw HoglakeException.Validation("snapshot $snapshotId out of range [0, ${m.head}]")
            }
            // The floor guard (mirrors every read path's 410 contract): an
            // offset below earliest_snapshot_id is a position the consumer
            // can never read from.
            if (snapshotId < m.earliestSnapshotId) {
                throw HoglakeException.Expired(
                    "cannot commit offset at snapshot $snapshotId: it is below the expiry floor " +
                        "(earliest retained snapshot is ${m.earliestSnapshotId}); reconcile from a " +
                        "full scan at a retained snapshot",
                )
            }
            // The uuid must name a table this catalog has EVER had — any
            // incarnation, dropped included (offsets deliberately survive
            // drops so consumers SEE incarnation changes).
            val known =
                queryCatalog(catalog, "SELECT COUNT(*) FROM tables WHERE table_uuid = ?", tableUuid.toString())
                    .firstRow()!![0].asLong() > 0
            if (!known) {
                throw HoglakeException.Validation("unknown table_uuid $tableUuid in catalog '$catalog'")
            }
            val now = HogliteSchema.now()
            val tx = HogliteTx()
            // Monotonic: never move BACKWARDS. An EQUAL re-commit is the
            // Postgres upsert's `<= EXCLUDED` arm — accepted, a no-op — which
            // is what makes a client retry of a timed-out-but-applied call
            // safe (each call carries a fresh txid).
            // The expiry floor, re-asserted IN the transaction: the preflight
            // read is a separate request, and a sweep that advanced the floor
            // in between must not let this offset land below it.
            tx.expectZero(
                "SELECT COUNT(*) FROM meta WHERE id = 1 AND earliest_snapshot_id > ?",
                snapshotId,
            ) {
                HoglakeException.Expired(
                    "cannot commit offset at snapshot $snapshotId: the expiry floor advanced past it; " +
                        "reconcile from a full scan at a retained snapshot",
                )
            }
            tx.expectZero(
                "SELECT COUNT(*) FROM offsets WHERE consumer_id = ? AND table_uuid = ? AND committed_snapshot > ?",
                consumerId,
                tableUuid.toString(),
                snapshotId,
            ) {
                HoglakeException.OffsetRegression(
                    "consumer '$consumerId' already committed past snapshot $snapshotId for table $tableUuid",
                )
            }
            tx.add(
                "INSERT INTO offsets(consumer_id, table_uuid, committed_snapshot, updated_at) VALUES (?, ?, ?, ?) " +
                    "ON CONFLICT(consumer_id, table_uuid) DO UPDATE SET " +
                    "committed_snapshot = excluded.committed_snapshot, updated_at = excluded.updated_at",
                consumerId,
                tableUuid.toString(),
                snapshotId,
                now,
            )
            // Reaching an incarnation at or past the snapshot that created it
            // is the consumer's statement that it has reconciled across the
            // replacement: release its offsets on the incarnations this one
            // replaced (they would otherwise pin the consumer_floor forever).
            tx.add(
                """WITH RECURSIVE ancestors(id, uuid, created, replaced) AS (
                 SELECT t.id, t.table_uuid, t.created_snapshot, t.replaced_table_id FROM tables t
                  WHERE t.table_uuid = ? AND ? >= t.created_snapshot
                 UNION ALL
                 SELECT a.id, a.table_uuid, a.created_snapshot, a.replaced_table_id
                   FROM ancestors l JOIN tables a
                     ON a.id = l.replaced AND a.created_snapshot < l.created
               )
               DELETE FROM offsets WHERE consumer_id = ? AND table_uuid <> ?
                 AND table_uuid IN (SELECT uuid FROM ancestors)""",
                tableUuid.toString(),
                snapshotId,
                consumerId,
                tableUuid.toString(),
            )
            runTx(catalog, freshTxid("offset"), null, tx)
            return ConsumerOffset(consumerId, tableUuid, snapshotId, HogliteSchema.parse(now))
        }

    // ==================== commits ==========================================

    override fun commit(
        catalog: String,
        request: CommitRequest,
        userAgent: String?,
    ): CommitResult =
        catalogOperation(catalog) {
            HogliteCommit.commit(this, catalog, request)
        }

    override fun receipt(
        catalog: String,
        operation: UUID,
    ): CommitResult =
        catalogOperation(catalog) {
            HogliteCommit.receipt(this, catalog, operation)
        }

    // ==================== shared internals =================================

    /** The one meta row of a catalog tenant. */
    internal class MetaRow(
        val name: String,
        val dataPath: String,
        val head: Long,
        val schemaVersion: Long,
        val earliestSnapshotId: Long,
        val earliestSnapshotTime: Instant?,
        val retentionSeconds: Long?,
        val consumerFloor: Boolean,
    ) {
        fun toCatalogInfo(): CatalogInfo =
            CatalogInfo(
                // hoglite tenants have no numeric id; the field is
                // internal-only, so a stable name hash keeps logs that
                // print it distinguishable without inventing a sequence.
                catalogId = name.hashCode().toLong() and 0x7fffffff,
                name = name,
                dataPath = dataPath,
                headSnapshotId = head,
                schemaVersion = schemaVersion,
                earliestSnapshotTime = earliestSnapshotTime,
                snapshotRetentionSeconds = retentionSeconds,
                consumerFloor = consumerFloor,
                earliestSnapshotId = earliestSnapshotId,
            )
    }

    internal val metaSql =
        "SELECT name, data_path, next_snapshot - 1, schema_version, earliest_snapshot_id, " +
            "earliest_snapshot_time, snapshot_retention_seconds, consumer_floor FROM meta WHERE id = 1"

    internal fun meta(catalog: String): MetaRow {
        val row =
            queryCatalog(catalog, metaSql).firstRow()
                ?: throw HoglakeException.NotFound("catalog '$catalog'")
        return metaRow(row)
    }

    private sealed interface DropMetadata {
        data class Present(val meta: MetaRow) : DropMetadata

        data object Absent : DropMetadata
    }

    /** Only this drop-private probe permits proven absence without poisoning an operation. */
    private fun dropMetadata(
        catalog: String,
        tenant: String,
    ): DropMetadata {
        val row =
            try {
                client.query(tenant, metaSql, emptyList()).firstRow()
            } catch (r: HogliteRejected) {
                if (tenantAbsent(r)) return DropMetadata.Absent
                throw mapRejected(catalog, r)
            } ?: throw HoglakeException.BackendUnavailable("catalog '$catalog' has no metadata row")
        return DropMetadata.Present(metaRow(row))
    }

    internal fun metaRow(row: List<Any?>): MetaRow =
        MetaRow(
            name = row[0].asString(),
            dataPath = row[1].asString(),
            head = row[2].asLong(),
            schemaVersion = row[3].asLong(),
            earliestSnapshotId = row[4].asLong(),
            earliestSnapshotTime = (row[5] as? String)?.let { HogliteSchema.parse(it) },
            retentionSeconds = row[6].asLongOrNull(),
            consumerFloor = row[7].asLong() != 0L,
        )

    /** Resolve the read snapshot for a read path, with hoglake's guard order and typed refusals. */
    internal fun resolveRead(
        catalog: String,
        m: MetaRow,
        snapshot: Long?,
        atTimestamp: Instant?,
    ): Long {
        if (snapshot != null && atTimestamp != null) {
            throw HoglakeException.Validation("snapshot and at_timestamp are mutually exclusive")
        }
        if (snapshot != null) {
            if (snapshot < 0) throw HoglakeException.Validation("snapshot must be >= 0, got $snapshot")
            if (snapshot > m.head) {
                throw HoglakeException.Validation("snapshot $snapshot is ahead of head ${m.head}")
            }
            if (snapshot < m.earliestSnapshotId) {
                throw HoglakeException.Expired(
                    "snapshot $snapshot is below the expiry floor " +
                        "(earliest retained snapshot is ${m.earliestSnapshotId})",
                )
            }
            return snapshot
        }
        if (atTimestamp != null) return resolveTimestamp(catalog, m, atTimestamp)
        return m.head
    }

    /**
     * Largest retained snapshot with snapshot_time <= [atTimestamp]; before the floor -> Expired.
     *
     * ts never decreases as the id grows ([insertSnapshotRow]), so the newest
     * id with ts <= T is the answer. ORDER BY id walks the primary key
     * backwards and stops at the first match, as PostgreSQL's
     * RESOLVE_SNAPSHOT_SQL does. ts has no index: ORDER BY ts sorts every
     * retained snapshot.
     */
    internal fun resolveTimestamp(
        catalog: String,
        m: MetaRow,
        atTimestamp: Instant,
    ): Long =
        queryCatalog(
            catalog,
            "SELECT id FROM snapshots WHERE ts <= ? ORDER BY id DESC LIMIT 1",
            HogliteSchema.fmt(atTimestamp),
        ).firstRow()?.get(0)?.asLong()
            ?: throw HoglakeException.Expired(
                "no retained snapshot at or before $atTimestamp " +
                    "(earliest retained snapshot is ${m.earliestSnapshotId})",
            )

    internal fun liveNamespace(
        catalog: String,
        namespace: String,
    ): NamespaceInfo =
        queryCatalog(
            catalog,
            "SELECT $NAMESPACE_COLUMNS FROM namespaces WHERE name = ? AND dropped_snapshot IS NULL",
            namespace,
        ).firstRow()?.let { namespaceInfo(it) }
            ?: throw HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog'")

    /** The namespace incarnation alive at [at]: created at or before it, not dropped by it. */
    private fun namespaceAt(
        catalog: String,
        namespace: String,
        at: Long,
    ): NamespaceInfo =
        queryCatalog(
            catalog,
            "SELECT $NAMESPACE_COLUMNS FROM namespaces WHERE name = ? AND created_snapshot <= ? " +
                "AND (dropped_snapshot IS NULL OR dropped_snapshot > ?)",
            namespace,
            at,
            at,
        ).firstRow()?.let { namespaceInfo(it) }
            ?: throw HoglakeException.NotFound("namespace '$namespace' in catalog '$catalog' at snapshot $at")

    /**
     * The namespace a READ resolves [namespace] to: the live one for a
     * head read, else the incarnation alive at [at] ([namespaceAt]). A
     * pinned reader must still see a namespace dropped after its pin, and
     * must not see one recreated under the same name after it. The reads
     * that serve head and pinned requests from one body (listFiles,
     * partitionValues, fileStats, changes, planScan) resolve through here;
     * getTable and listTables, whose head requests take their own path
     * (headTable, headListing), call [namespaceAt] directly. So every
     * pinned read agrees on the incarnation. [at] alone cannot tell a head
     * read from a pinned read that names head, hence the flag.
     */
    private fun namespaceForRead(
        catalog: String,
        namespace: String,
        at: Long,
        headRead: Boolean,
    ): NamespaceInfo = if (headRead) liveNamespace(catalog, namespace) else namespaceAt(catalog, namespace, at)

    /** A namespace row read with [NAMESPACE_COLUMNS]. */
    private fun namespaceInfo(row: List<Any?>): NamespaceInfo =
        NamespaceInfo(
            row[0].asLong(),
            row[1].asString(),
            row[2]?.let { NamespaceContainment(it.asString(), row[3].asLong()) },
        )

    /** A live table row as the store uses one. */
    internal class TableRow(
        val tableId: Long,
        val tableUuid: UUID,
        val namespaceId: Long,
        val namespace: String,
        val name: String,
        val comment: String?,
        val properties: Map<String, String>,
        val nextRowId: Long,
        val schemaVersion: Long,
    )

    /** Resolve (namespace, name) at [at]: the VERSION visible then, of an incarnation alive then. */
    internal fun tableAt(
        catalog: String,
        namespaceId: Long,
        namespace: String,
        table: String,
        at: Long,
    ): TableRow {
        val row =
            queryCatalog(
                catalog,
                "SELECT t.id, t.table_uuid, v.namespace_id, v.namespace, v.name, v.comment, " +
                    "v.properties_json, t.next_row_id, t.schema_version FROM table_versions v " +
                    "JOIN tables t ON t.id = v.table_id " +
                    "WHERE v.namespace_id = ? AND v.name = ? " +
                    "AND v.begin_snapshot <= ? AND (v.end_snapshot IS NULL OR v.end_snapshot > ?) " +
                    "AND t.created_snapshot <= ? AND (t.dropped_snapshot IS NULL OR t.dropped_snapshot > ?)",
                namespaceId,
                table,
                at,
                at,
                at,
                at,
            ).firstRow()
                ?: throw HoglakeException.NotFound(
                    "table '$namespace.$table' in catalog '$catalog' at snapshot $at",
                )
        return tableRowOf(row)
    }

    internal fun liveTable(
        catalog: String,
        namespace: String,
        table: String,
    ): TableRow {
        val ns = liveNamespace(catalog, namespace)
        val row =
            queryCatalog(
                catalog,
                "SELECT t.id, t.table_uuid, v.namespace_id, v.namespace, v.name, v.comment, " +
                    "v.properties_json, t.next_row_id, t.schema_version FROM table_versions v " +
                    "JOIN tables t ON t.id = v.table_id " +
                    "WHERE v.namespace_id = ? AND v.name = ? AND v.end_snapshot IS NULL " +
                    "AND t.dropped_snapshot IS NULL",
                ns.namespaceId,
                table,
            ).firstRow()
                ?: throw HoglakeException.NotFound("table '$namespace.$table' in catalog '$catalog'")
        return tableRowOf(row)
    }

    @Suppress("UNCHECKED_CAST")
    internal fun tableRowOf(row: List<Any?>): TableRow =
        TableRow(
            tableId = row[0].asLong(),
            tableUuid = UUID.fromString(row[1].asString()),
            namespaceId = row[2].asLong(),
            namespace = row[3].asString(),
            name = row[4].asString(),
            comment = row[5] as? String,
            properties = json.readValue<Map<String, String>>(row[6].asString()),
            nextRowId = row[7].asLong(),
            schemaVersion = row[8].asLong(),
        )

    /** One column row as read back, before the tree is rebuilt. */
    private class ColumnRow(
        val fieldId: Long,
        val parentFieldId: Long?,
        val ordinal: Int,
        val name: String,
        val type: ColType,
        val typeParams: Map<String, Any?>?,
        val nullable: Boolean,
        val comment: String?,
    )

    /** The column forest visible at [at], parents before children. */
    internal fun columnsAt(
        catalog: String,
        tableId: Long,
        at: Long,
    ): List<Column> {
        val rows =
            queryCatalog(
                catalog,
                "SELECT field_id, parent_field_id, ordinal, name, type, type_params_json, nullable, comment " +
                    "FROM columns WHERE table_id = ? AND begin_snapshot <= ? " +
                    "AND (end_snapshot IS NULL OR end_snapshot > ?)",
                tableId,
                at,
                at,
            ).rows.map { r ->
                @Suppress("UNCHECKED_CAST")
                ColumnRow(
                    fieldId = r[0].asLong(),
                    parentFieldId = r[1].asLongOrNull(),
                    ordinal = r[2].asLong().toInt(),
                    name = r[3].asString(),
                    type = ColType.fromWire(r[4].asString()),
                    typeParams = (r[5] as? String)?.let { json.readValue<Map<String, Any?>>(it) },
                    nullable = r[6].asLong() != 0L,
                    comment = r[7] as? String,
                )
            }

        // Group once, as TableRepo.assemble does. A filter per node is
        // quadratic: 10,000 flat columns cost 100 million comparisons.
        val byParent = rows.groupBy { it.parentFieldId }

        fun build(parent: Long?): List<Column> =
            byParent[parent].orEmpty().sortedBy { it.ordinal }.map { r ->
                Column(
                    fieldId = r.fieldId,
                    ordinal = r.ordinal,
                    def =
                        ColumnDef(
                            name = r.name,
                            type = r.type,
                            typeParams = r.typeParams,
                            nullable = r.nullable,
                            children = null,
                            comment = r.comment,
                        ),
                    children = build(r.fieldId),
                )
            }
        return build(null)
    }

    internal fun specAt(
        catalog: String,
        tableId: Long,
        at: Long,
    ): PartitionSpec? =
        queryCatalog(
            catalog,
            "SELECT spec_id, fields_json FROM partition_specs WHERE table_id = ? " +
                "AND begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?) " +
                "ORDER BY spec_id DESC LIMIT 1",
            tableId,
            at,
            at,
        ).firstRow()?.let { r ->
            PartitionSpec(r[0].asLong(), json.readValue<List<PartitionFieldDef>>(r[1].asString()))
        }

    internal fun sortSpecAt(
        catalog: String,
        tableId: Long,
        at: Long,
    ): SortSpec? =
        queryCatalog(
            catalog,
            "SELECT spec_id, fields_json, begin_snapshot FROM sort_specs WHERE table_id = ? " +
                "AND begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?) " +
                "ORDER BY spec_id DESC LIMIT 1",
            tableId,
            at,
            at,
        ).firstRow()?.let { r ->
            SortSpec(r[0].asLong(), json.readValue<List<SortFieldDef>>(r[1].asString()), r[2].asLong())
        }

    /**
     * The table's object-prefix field id visible at [at] (`table_layouts`,
     * Postgres's LayoutRepo.layoutAt), or null when the option is off there.
     */
    internal fun layoutAt(
        catalog: String,
        tableId: Long,
        at: Long,
    ): Long? =
        queryCatalog(
            catalog,
            "SELECT object_prefix_field_id FROM table_layouts WHERE table_id = ? " +
                "AND begin_snapshot <= ? AND (end_snapshot IS NULL OR end_snapshot > ?)",
            tableId,
            at,
            at,
        ).firstRow()?.get(0)?.asLong()

    /** table_id -> live object-prefix field id, for every table whose option is on at head (LayoutRepo.live). */
    internal fun liveLayouts(catalog: String): Map<Long, Long> =
        queryCatalog(
            catalog,
            "SELECT table_id, object_prefix_field_id FROM table_layouts WHERE end_snapshot IS NULL",
        ).rows.associate { it[0].asLong() to it[1].asLong() }

    /**
     * The tenant the CURRENT operation on [catalog] is bound to. Only
     * valid inside [catalogOperation]; a value derived from it (a cursor
     * scope) is therefore scoped to that one incarnation of the name.
     */
    internal fun boundTenant(catalog: String): String {
        val binding =
            checkNotNull(operationBindings.get()?.get(catalog)) {
                "catalog '$catalog' has no operation binding"
            }
        binding.missing?.let { throw it }
        return binding.tenant
    }

    /** A file row plus its stored payloads, before per-endpoint projection. */
    internal class FileRow(
        val file: DataFile,
        val statsJson: String?,
        val splitOffsetsJson: String?,
    )

    internal fun toFileRow(r: List<Any?>): FileRow {
        val file =
            DataFile(
                dataFileId = r[0].asLong(),
                tableId = r[1].asLong(),
                path = r[2].asString(),
                fileFormat = r[3].asString(),
                recordCount = r[4].asLong(),
                fileSizeBytes = r[5].asLong(),
                footerSize = r[6].asLongOrNull(),
                rowIdStart = r[7].asLong(),
                explicitRowIds = r[8].asLong() != 0L,
                specId = r[9].asLongOrNull(),
                partitionValues = (r[10] as? String)?.let { json.readValue<List<String?>>(it) },
                statsState = StatsState.fromWire(r[11].asString()),
                beginSnapshot = r[14].asLong(),
            )
        return FileRow(file, r[12] as? String, r[13] as? String)
    }

    internal fun toDataFile(r: List<Any?>): DataFile = toFileRow(r).file

    /**
     * A file's stored stats rows ([statsJson]) resolved against the columns
     * visible at the read snapshot — one [FileColumnStats] per stored row
     * whose field id is a visible LEAF (and in [wanted], when given); a row
     * whose field id is not visible (a dropped column) is omitted.
     *
     * [byId] is [columnsByFieldId] of those columns. The caller builds it
     * ONCE per read, as ScanService does, not once per file.
     */
    internal fun resolveStoredStats(
        statsJson: String?,
        byId: Map<Long, Pair<String, Column>>,
        wanted: Set<Long>?,
    ): List<FileColumnStats> {
        val stored = statsJson?.let { json.readValue<List<ColumnStats>>(it) } ?: return emptyList()
        return stored.mapNotNull { stat ->
            if (wanted != null && stat.fieldId !in wanted) return@mapNotNull null
            val (path, col) = byId[stat.fieldId] ?: return@mapNotNull null
            if (col.def.type == ColType.VARIANT || col.def.type.isNested) return@mapNotNull null
            FileColumnStats(
                fieldId = stat.fieldId,
                name = col.def.name,
                path = path,
                type = col.def.type,
                typeParams = col.def.typeParams,
                stats = stat,
            )
        }
    }

    /** Attach each listed file's ordering-key range (RowIds for an unsorted table, SortKey for a sorted one). */
    private fun withOrderingBounds(
        catalog: String,
        tableId: Long,
        at: Long,
        page: List<FileRow>,
    ): List<DataFile> {
        if (page.isEmpty()) return emptyList()
        val sort = sortSpecAt(catalog, tableId, at)
        if (sort == null || sort.fields.isEmpty()) {
            return page.map { fr ->
                val f = fr.file
                if (f.recordCount <= 0) return@map f.copy(orderingBounds = null)
                f.copy(
                    orderingBounds =
                        FileOrderingBounds.RowIds(
                            lower = f.rowIdStart,
                            // Explicit row ids: row_id_start is only
                            // min(input row ids); the upper bound is
                            // arithmetic on a meaning the file lacks.
                            upper = if (f.explicitRowIds) null else f.rowIdStart + f.recordCount - 1,
                        ),
                )
            }
        }
        val leadingFieldId = sort.fields.first().sourceFieldId
        val byId = columnsByFieldId(columnsAt(catalog, tableId, at))
        val wanted = setOf(leadingFieldId)
        return page.map { fr ->
            // A file with no stored stats for the sort key carries no
            // bound — null is "unknown", not a computed lie.
            val bound = resolveStoredStats(fr.statsJson, byId, wanted).firstOrNull()
            fr.file.copy(orderingBounds = bound?.let { FileOrderingBounds.SortKey(it) })
        }
    }

    /** Record each outcome after the run. Ledger failures never replace the task outcome. */
    internal fun <T> recorded(
        catalog: String,
        task: com.posthog.hoglake.model.MaintenanceTask,
        trigger: com.posthog.hoglake.model.MaintenanceTrigger,
        body: () -> T,
    ): T =
        catalogOperation(catalog) {
            val startedAt = java.time.Instant.now()
            try {
                val result = body()
                recordRun(
                    catalog,
                    task,
                    trigger,
                    startedAt,
                    java.time.Instant.now(),
                    com.posthog.hoglake.model.MaintenanceRunStatus.OK,
                    null,
                    result,
                )
                return result
            } catch (e: Throwable) {
                recordRun(
                    catalog,
                    task,
                    trigger,
                    startedAt,
                    java.time.Instant.now(),
                    com.posthog.hoglake.model.MaintenanceRunStatus.FAILED,
                    e.message,
                    (e as? com.posthog.hoglake.persistence.PartialResult)?.partial,
                )
                throw e
            }
        }

    private val ledgerJson = com.posthog.hoglake.wireObjectMapper()

    private fun recordRun(
        catalog: String,
        task: com.posthog.hoglake.model.MaintenanceTask,
        trigger: com.posthog.hoglake.model.MaintenanceTrigger,
        startedAt: java.time.Instant,
        finishedAt: java.time.Instant,
        status: com.posthog.hoglake.model.MaintenanceRunStatus,
        error: String?,
        result: Any?,
    ) {
        val binding = operationBindings.get()?.get(catalog)
        val taskMissing = binding?.missing
        try {
            runCatching {
                val tx = HogliteTx()
                tx.add(
                    "INSERT INTO maintenance_runs(task, run_trigger, started_at, finished_at, status, error, " +
                        "result_json) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    task.wire,
                    trigger.wire,
                    HogliteSchema.fmt(startedAt),
                    HogliteSchema.fmt(finishedAt),
                    status.wire,
                    error,
                    result?.let { ledgerJson.writeValueAsString(it) },
                )
                runTx(catalog, freshTxid("ledger"), null, tx)
            }.onFailure { e ->
                io.github.oshai.kotlinlogging.KotlinLogging.logger {}
                    .warn(e) { "maintenance ledger: failed to record $task run for catalog '$catalog'" }
            }
        } finally {
            // Keep the tenant binding and any task-level absence. Do not retain absence found only by the ledger.
            binding?.missing = taskMissing
        }
    }

    // ---- backends over the same tenant -------------------------------------

    /** Upload claims (claim/renew/abandon/expiry sweep) on this store's tenant tables. */
    internal val uploads: HogliteUploads by lazy { HogliteUploads(this, ledgerRetentionSeconds) }

    /** Atomic table creations (prepare/publish/abort receipts) on this store's tenant tables. */
    internal val tableCreations: HogliteTableCreations by lazy { HogliteTableCreations(this) }

    /** ALTER TABLE on this store's tenant tables. */
    internal val alters: HogliteAlters by lazy { HogliteAlters(this) }
    internal val views: HogliteViews by lazy { HogliteViews(this) }
    internal val options: HogliteOptions by lazy { HogliteOptions(this) }

    /**
     * Load a receipt with logical expiry.
     * Write-side callers also persist lazy expiry.
     * Status reads never require write authority.
     */
    internal fun loadCreationOrThrow(
        catalog: String,
        operationId: UUID,
        persistExpiry: Boolean = true,
    ): com.posthog.hoglake.service.TableCreation {
        if (persistExpiry) {
            runTx(
                catalog,
                freshTxid("creation-expire"),
                null,
                HogliteTx().apply {
                    add(
                        "UPDATE table_creations SET state = 'aborted', reason = 'expired' " +
                            "WHERE operation = ? AND state = 'prepared' " +
                            "AND julianday(expires_at) <= ${HogliteSchema.CREATION_NOW_SQL}",
                        operationId.toString(),
                    )
                },
            )
        }
        // Complete the clock read before the receipt's snapshot can be pinned.
        // A later clock over an older prepared snapshot could falsely report terminal expiry.
        val expiryCut =
            if (persistExpiry) {
                null
            } else {
                (queryCatalog(catalog, "SELECT ${HogliteSchema.CREATION_NOW_SQL}").firstRow()!![0] as Number).toDouble()
            }
        val expiryView =
            if (expiryCut == null) {
                "0"
            } else {
                "CASE WHEN state = 'prepared' AND julianday(expires_at) <= ? THEN 1 ELSE 0 END"
            }
        val row =
            queryCatalog(
                catalog,
                "SELECT table_uuid, definition_json, write_path, state, expires_at, snapshot_id, " +
                    "schema_version, reason, $expiryView FROM table_creations WHERE operation = ?",
                *if (expiryCut == null) {
                    arrayOf<Any?>(operationId.toString())
                } else {
                    arrayOf<Any?>(expiryCut, operationId.toString())
                },
            ).firstRow()
                ?: throw HoglakeException.NotFound("table creation operation '$operationId'")
        val definition =
            com.posthog.hoglake.service.TableCreationDefinitionCodec.decode(
                row[1].asString(),
                "table creation operation $operationId",
            )
        val expires = HogliteSchema.parse(row[4].asString())
        val expired = row[8].asLong() == 1L
        return com.posthog.hoglake.service.TableCreation(
            operationId,
            UUID.fromString(row[0].asString()),
            definition,
            com.posthog.hoglake.model.initialColumns(definition.columns),
            row[2].asString(),
            if (expired) "aborted" else row[3].asString(),
            expires,
            row[5].asLongOrNull(),
            row[6].asLongOrNull(),
            if (expired) "expired" else row[7] as? String,
        )
    }

    internal val partitions: HoglitePartitions by lazy {
        HoglitePartitions(this, smallFileThresholdBytes, minInputFiles, maxInputFiles)
    }
    internal val partitionLookup: HoglitePartitionLookup by lazy {
        HoglitePartitionLookup(this, admission = partitionLookupAdmission)
    }

    // ---- transports --------------------------------------------------------

    /**
     * Run [tx] against [catalog]'s tenant.
     *
     * Every mutating tenant transaction carries the DROP FENCE as
     * statement 0: dropCatalog's fencing phase sets `meta.dropping`
     * under the same guard set that proves the catalog empty, and this
     * statement is what stops any later write (namespace create, commit)
     * from landing between those checks and the tenant's
     * deletion. Bootstrap opts out (it is what CREATES the meta row).
     */
    internal fun runTx(
        catalog: String,
        txid: String,
        fp: String?,
        tx: HogliteTx,
        fenced: Boolean = true,
    ): List<HogliteQueryResult> =
        try {
            val statements =
                if (fenced) {
                    listOf(
                        HogliteStatement(
                            "SELECT COUNT(*) FROM meta WHERE id = 1 AND dropping = 0",
                            emptyList(),
                            expectScalarInt = 1,
                        ),
                    ) + tx.statements()
                } else {
                    tx.statements()
                }
            val results =
                withBoundTenant(catalog) { tenant ->
                    client.tx(tenant, txid, fp, statements)
                }
            if (fenced) results.subList(1, results.size) else results
        } catch (g: HogliteGuardFailure) {
            if (fenced) {
                if (g.statementIndex == 0) {
                    throw missingCatalog(catalog, "catalog '$catalog' (it is being dropped)")
                }
                throw tx.classify(HogliteGuardFailure(g.statementIndex - 1, g.message ?: "guard failed"))
            }
            throw tx.classify(g)
        } catch (r: HogliteRejected) {
            throw mapRejected(catalog, r)
        }

    internal fun queryCatalog(
        catalog: String,
        sql: String,
        vararg args: Any?,
    ): HogliteQueryResult =
        try {
            withBoundTenant(catalog) { tenant -> client.query(tenant, sql, args.toList()) }
        } catch (r: HogliteRejected) {
            throw mapRejected(catalog, r)
        }

    /**
     * Several reads of [catalog] as ONE read-only transaction on the bound
     * incarnation ([HogliteClient.readTx]): one snapshot, one reader slot,
     * one durability park. Same binding and error mapping as
     * [queryCatalog], so a failed batch raises what the failed single read
     * would have.
     */
    internal fun readCatalog(
        catalog: String,
        statements: List<HogliteStatement>,
    ): List<HogliteQueryResult> {
        return try {
            withBoundTenant(catalog) { tenant -> client.readTx(tenant, statements) }
        } catch (r: HogliteRejected) {
            throw mapRejected(catalog, r)
        }
    }

    /** No per-call refresh: a bound 404 aborts the entire plan. */
    private fun <T> withBoundTenant(
        catalog: String,
        call: (String) -> T,
    ): T {
        // Every tenant query/read/tx belongs to one already-entered operation.
        val tenant = boundTenant(catalog)
        try {
            return call(tenant)
        } catch (r: HogliteRejected) {
            if (r.status == 404) throw missingCatalog(catalog)
            throw r
        }
    }

    private inline fun <T> backendCall(
        catalog: String,
        call: () -> T,
    ): T =
        try {
            call()
        } catch (r: HogliteRejected) {
            if (r.status == 401 || r.status == 403) throw mapRejected(catalog, r)
            throw r
        }

    internal fun mapRejected(
        catalog: String,
        r: HogliteRejected,
    ): HoglakeException {
        val body = runCatching { json.readTree(r.body) }.getOrNull()
        val code = body?.get("code")?.asText()
        val detail = body?.get("error")?.asText() ?: r.body
        return when {
            r.status == 401 -> HoglakeException.Unauthenticated("invalid_token", "backend refused request authority")
            r.status == 403 -> HoglakeException.Forbidden("backend refused request authority")
            r.status == 404 -> missingCatalog(catalog)
            code == "result_too_large" ->
                HoglakeException.BackendResultTooLarge(
                    "hoglite result exceeds the engine limit; narrow the request: $detail",
                )
            code == "query_timeout" ->
                HoglakeException.BackendQueryTimeout(
                    "hoglite query exceeded the engine deadline; narrow the request: $detail",
                )
            // /query's bare 409 is SQL refusal; readTx also normalizes refused 422 to 409.
            code in setOf("refused", "out_of_memory") || r.status in setOf(400, 409, 422) ->
                HoglakeException.Validation("hoglite refused the statement: $detail")
            // An unclassified engine/proxy server failure is not a deterministic request refusal.
            else -> HoglakeException.BackendUnavailable("hoglite answered ${r.status}: $detail")
        }
    }

    /**
     * hoglite's 404 for a tenant with no manifest. Matched on the engine's
     * message too: a 404 from anything else (a wrong URL, a proxy) must not
     * read as "no catalogs".
     */
    private fun tenantAbsent(r: HogliteRejected): Boolean = r.status == 404 && "tenant does not exist" in r.body

    // ---- small shared pieces ------------------------------------------------

    private fun sortColumnSql(sort: FileRepo.FileSortColumn): String =
        when (sort) {
            FileRepo.FileSortColumn.ID -> "id"
            FileRepo.FileSortColumn.PATH -> "path"
            FileRepo.FileSortColumn.RECORD_COUNT -> "record_count"
            FileRepo.FileSortColumn.SIZE -> "file_size_bytes"
            FileRepo.FileSortColumn.STATS -> "stats_state"
            FileRepo.FileSortColumn.BEGIN_SNAPSHOT -> "begin_snapshot"
        }

    internal fun insertColumnRows(
        tx: HogliteTx,
        tableIdSql: String,
        columns: List<Column>,
    ) {
        fun walk(
            cols: List<Column>,
            parent: Long?,
        ) {
            for (c in cols) {
                tx.add(
                    "INSERT INTO columns(table_id, field_id, parent_field_id, ordinal, name, type, " +
                        "type_params_json, nullable, comment, begin_snapshot) " +
                        "VALUES ($tableIdSql, ?, ?, ?, ?, ?, ?, ?, ?, ${HogliteSchema.HEAD_SQL})",
                    c.fieldId,
                    parent,
                    c.ordinal,
                    c.def.name,
                    c.def.type.wire,
                    c.def.typeParams?.let { json.writeValueAsString(it) },
                    if (c.def.nullable) 1L else 0L,
                    c.def.comment,
                )
                walk(c.children, c.fieldId)
            }
        }
        walk(columns, null)
    }

    internal fun validateCatalogName(name: String) {
        if (!CATALOG_NAME_REGEX.matches(name)) {
            throw HoglakeException.Validation(
                "invalid catalog name '${Identifiers.cap(name)}' (must match ${Identifiers.CATALOG_NAME_PATTERN})",
            )
        }
    }

    companion object {
        internal const val SCAN_PAGE_SIZE = 256
        internal const val SCAN_MAX_FILES = 1_000_000
        internal const val SCAN_MAX_BYTES = 512L * 1024 * 1024
        internal const val SCAN_MAX_ROW_JSON_BYTES = 64 * 1024
        internal const val SCAN_MAX_METADATA_BYTES = 64L * 1024 * 1024

        private val CATALOG_NAME_REGEX = Regex(Identifiers.CATALOG_NAME_PATTERN)

        /** The namespace columns [namespaceInfo] reads, in its order. */
        private const val NAMESPACE_COLUMNS = "id, name, contained_field, contained_value"
        private const val LIVE_NAMESPACE_SQL =
            "SELECT $NAMESPACE_COLUMNS FROM namespaces WHERE name = ? AND dropped_snapshot IS NULL"

        /** The live namespace's id as a subselect of one `?` (the name), for a statement in the same snapshot. */
        private const val LIVE_NAMESPACE_ID_SQL =
            "(SELECT id FROM namespaces WHERE name = ? AND dropped_snapshot IS NULL)"

        /**
         * CatalogService's containment field rule (its CONTAINMENT_FIELD is
         * private). Keep the two the same: the hoglite CHECK has no regex,
         * so this is the only check of the field on this backend.
         */
        private val CONTAINMENT_FIELD = Regex("^[a-z][a-z0-9_]{0,62}$")

        /**
         * Rows per page of the escaped-spelling pass of the containment
         * scan. hoglite refuses a large result (413), and a path can be
         * about 1 KiB.
         */
        internal const val CONTAINMENT_SCAN_PAGE = 256

        /** [storageUsage]'s read: no live-row predicate on files or DVs. */
        internal const val STORAGE_USAGE_SQL =
            "SELECT (SELECT COALESCE(SUM(file_size_bytes),0) FROM files), " +
                "(SELECT COALESCE(SUM(file_size_bytes),0) FROM dvs), " +
                "(SELECT COUNT(*) FROM removals WHERE drained_at IS NULL)"

        /** Catalogs per `/v1/query-batch` request in [listCatalogs]. */
        const val LIST_BATCH_SIZE = 250

        /** Concurrent single reads when the engine has no `/v1/query-batch`. */
        const val LIST_FALLBACK_PARALLELISM = 16

        internal fun freshTxid(prefix: String): String = "$prefix:${UUID.randomUUID()}"
    }
}
