package com.posthog.hoglake.hoglite

import com.posthog.hoglake.DatabaseBackend
import com.posthog.hoglake.model.DatabaseTable
import com.posthog.hoglake.observability.CatalogMetricSource
import com.posthog.hoglake.observability.CatalogMetrics
import com.posthog.hoglake.observability.CatalogRow
import com.posthog.hoglake.observability.LastRuns
import com.posthog.hoglake.observability.Lifecycle
import com.posthog.hoglake.observability.PostgresCatalogMetricSource
import com.posthog.hoglake.observability.TableRow
import org.jdbi.v3.core.Jdbi

/** One serial roster walk per group. Cached observations belong to immutable catalog incarnations. */
internal class RoutedCatalogMetricSource(
    private val store: HogliteCatalogStore,
    private val smallFileThresholdBytes: Long,
    private val router: CatalogRouter? = null,
    private val jdbi: Jdbi? = null,
    private val ensurePostgres: () -> Unit = {},
) : CatalogMetricSource {
    private var entries = emptyList<CatalogDirectory.Entry>()
    private val caches = mutableMapOf<String, MutableMap<CatalogDirectory.Entry, Any>>()
    private val failures = mutableMapOf<String, Throwable>()
    override val identities: Map<String, String>
        get() = entries.associate { it.name to "${it.backend}:${it.connection}:${it.identity}" }

    override fun startSample() {
        // Do not change the previous roster or caches if enumeration fails.
        val roster = router?.directory?.all(readyOnly = true) ?: hogliteRoster()
        entries = roster
        failures.clear()
        caches.values.forEach { cache -> cache.keys.retainAll(roster.toSet()) }
    }

    private fun hogliteRoster(): List<CatalogDirectory.Entry> {
        val out = mutableListOf<CatalogDirectory.Entry>()
        var after = ""
        while (true) {
            val page =
                store.queryRegistry(
                    "SELECT name, data_path, tenant FROM catalogs WHERE name > ? ORDER BY name LIMIT 250",
                    after,
                ).rows
            out +=
                page.map { r ->
                    val name = r[0].asString()
                    CatalogDirectory.Entry(
                        name, r[1].asString(), DatabaseBackend.HOGLITE, "hoglite",
                        (r[2] as? String) ?: store.tenantOf(name), "ready", "",
                    )
                }
            if (page.size < 250) return out
            after = page.last()[0].asString()
        }
    }

    override fun failure(group: String): Throwable? = failures[group]

    private fun <T : Any> collect(
        group: String,
        read: (CatalogMetricSource) -> T,
    ): List<T> {
        val cache = caches.getOrPut(group) { mutableMapOf() }
        for (entry in entries) {
            if (Thread.currentThread().isInterrupted) throw InterruptedException("metrics collection interrupted")
            try {
                val source =
                    if (entry.backend == DatabaseBackend.POSTGRES) {
                        PostgresCatalogMetricSource(requireNotNull(jdbi), entry.identity.toLong())
                    } else {
                        HogliteCatalogMetrics(store, entry.name, smallFileThresholdBytes)
                    }
                val observation =
                    if (router != null) {
                        router.bound(entry) { read(source) }
                    } else {
                        store.catalogOperation(entry.name, tenant = entry.identity) { read(source) }
                    }
                cache[entry] = observation
            } catch (e: Exception) {
                if (e is InterruptedException || e is java.util.concurrent.CancellationException) throw e
                val previous = failures.putIfAbsent(group, e)
                if (previous != null && previous !== e) previous.addSuppressed(e)
            }
        }
        @Suppress("UNCHECKED_CAST")
        return entries.mapNotNull { cache[it] as T? }
    }

    override fun catalogs(): Pair<List<CatalogRow>, List<Triple<String, String, Long>>> {
        val observations = collect(CatalogMetrics.GROUP_CATALOGS) { it.catalogs() }
        return observations.flatMap { it.first } to observations.flatMap { it.second }
    }

    override fun tables(cap: Int): Pair<List<String>, List<TableRow>> {
        val observations = collect(CatalogMetrics.GROUP_TABLES) { it.tables(cap) }
        return observations.flatMap { it.first } to observations.flatMap { it.second }
    }

    override fun deleteFiles(): List<Triple<String, Long, Long>> =
        collect(CatalogMetrics.GROUP_DELETE_FILES) {
            it.deleteFiles()
        }.flatten()

    override fun lifecycle(probeCap: Int): List<Lifecycle> =
        collect(CatalogMetrics.GROUP_LIFECYCLE) {
            it.lifecycle(probeCap)
        }.flatten()

    override fun maintenanceRuns(lookback: Int): Pair<Set<String>, List<LastRuns>> {
        val observations = collect(CatalogMetrics.GROUP_MAINTENANCE_RUNS) { it.maintenanceRuns(lookback) }
        return observations.flatMap { it.first }.toSet() to observations.flatMap { it.second }
    }

    override fun relations(): List<DatabaseTable> {
        if (jdbi == null) return emptyList()
        ensurePostgres()
        return PostgresCatalogMetricSource(jdbi).relations()
    }
}
