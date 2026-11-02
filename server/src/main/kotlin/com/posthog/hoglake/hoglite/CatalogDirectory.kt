package com.posthog.hoglake.hoglite

import com.posthog.hoglake.DatabaseBackend
import com.posthog.hoglake.model.HoglakeException
import java.util.UUID

/**
 * The authoritative directory in mixed mode. Connection references name configured pools, not endpoints.
 * Entries never change their backend. PostgreSQL reservations retain their identity until a pinned drop completes.
 */
internal class CatalogDirectory(private val store: HogliteCatalogStore) {
    data class Entry(
        val name: String,
        val dataPath: String,
        val backend: DatabaseBackend,
        val connection: String,
        val identity: String,
        val state: String,
        val operation: String,
    )

    @Volatile
    private var ready = false

    internal var beforePublish: (Entry) -> Unit = {}

    @Synchronized
    fun initialize() {
        if (ready) return
        store.ensureRegistry()
        for ((column, declaration) in listOf(
            "backend" to "TEXT NOT NULL DEFAULT 'hoglite'",
            "connection_ref" to "TEXT NOT NULL DEFAULT 'hoglite'",
            "state" to "TEXT NOT NULL DEFAULT 'ready'",
            "operation" to "TEXT NOT NULL DEFAULT ''",
        )) {
            val probe = "SELECT COUNT(*) FROM sqlite_master WHERE name = 'catalogs' AND instr(sql, '$column ') > 0"
            if (query(probe).firstRow()!![0].asLong() != 0L) continue
            val tx = HogliteTx()
            tx.expectZero(probe) { HoglakeException.AlreadyExists("directory schema already initialized") }
            tx.add("ALTER TABLE catalogs ADD COLUMN $column $declaration")
            try {
                write(tx)
            } catch (_: HoglakeException.AlreadyExists) {
                // Another replica added this column.
            }
        }
        store.directoryEnabled = true
        ready = true
    }

    fun find(name: String): Entry? {
        initialize()
        return query("SELECT $COLUMNS FROM catalogs WHERE name = ?", name).rows.firstOrNull()?.let(::entry)
    }

    fun entries(
        after: String? = null,
        limit: Int = PAGE_SIZE,
        readyOnly: Boolean = false,
    ): List<Entry> {
        initialize()
        return query(
            "SELECT $COLUMNS FROM catalogs WHERE (? IS NULL OR name > ?) " +
                "AND (? = 0 OR state = 'ready') ORDER BY name LIMIT ?",
            after,
            after,
            if (readyOnly) 1 else 0,
            limit,
        ).rows.map(::entry)
    }

    fun all(readyOnly: Boolean = false): List<Entry> {
        val out = mutableListOf<Entry>()
        while (true) {
            val page = entries(out.lastOrNull()?.name, readyOnly = readyOnly)
            out += page
            if (page.size < PAGE_SIZE) return out
        }
    }

    fun reserve(
        name: String,
        path: String,
        id: Long,
    ): Entry {
        initialize()
        val e =
            Entry(
                name,
                path,
                DatabaseBackend.POSTGRES,
                "postgres",
                id.toString(),
                "creating",
                UUID.randomUUID().toString(),
            )
        val tx = HogliteTx()
        tx.expectZero("SELECT COUNT(*) FROM catalogs WHERE name = ?", name) {
            HoglakeException.AlreadyExists("catalog '$name' already exists")
        }
        // The serial registry transaction makes the prefix check authoritative across both backends.
        tx.expectZero(
            "SELECT COUNT(*) FROM catalogs WHERE " +
                "substr(rtrim(data_path, '/') || '/', 1, length(?)) = ? " +
                "OR substr(?, 1, length(rtrim(data_path, '/') || '/')) = rtrim(data_path, '/') || '/'",
            path.trimEnd('/') + "/",
            path.trimEnd('/') + "/",
            path.trimEnd('/') + "/",
        ) { HoglakeException.Validation("data_path overlaps another catalog") }
        tx.add(
            "INSERT INTO catalogs(name, data_path, tenant, backend, connection_ref, state, operation) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
            e.name,
            e.dataPath,
            e.identity,
            e.backend.wire,
            e.connection,
            e.state,
            e.operation,
        )
        write(tx)
        return e
    }

    fun publish(e: Entry) {
        beforePublish(e)
        // Defense in depth: the name lock already excludes replacement during publication.
        // Removing this predicate alone cannot produce a live interleaving under that lock.
        write(
            HogliteTx().apply {
                affectOne(
                    "UPDATE catalogs SET state = 'ready' WHERE name = ? AND tenant = ? " +
                        "AND operation = ? AND state = 'creating'",
                    e.name,
                    e.identity,
                    e.operation,
                ) { HoglakeException.NotFound("catalog '${e.name}' reservation") }
            },
        )
    }

    fun remove(e: Entry) {
        // Defense in depth beneath the name lock and drop's fresh entry comparison.
        write(
            HogliteTx().apply {
                add(
                    "DELETE FROM catalogs WHERE name = ? AND tenant = ? AND operation = ? AND backend = ?",
                    e.name,
                    e.identity,
                    e.operation,
                    e.backend.wire,
                )
            },
        )
    }

    fun validateSelection(
        backend: String?,
        connection: String?,
    ): DatabaseBackend {
        val selected = backend ?: connection ?: "hoglite"
        val parsed = DatabaseBackend.fromWire(selected)
        if (parsed == null || parsed == DatabaseBackend.MIXED || (connection != null && connection != selected)) {
            throw HoglakeException.Validation(
                "backend and connection_ref must name the configured postgres or hoglite connection",
            )
        }
        return parsed
    }

    private fun entry(row: List<Any?>): Entry {
        val name = row[0].asString()
        val backend =
            try {
                validateSelection(row[3].asString(), row[4].asString())
            } catch (_: HoglakeException.Validation) {
                throw HoglakeException.BackendUnavailable("catalog '$name' has an unsupported connection reference")
            }
        val identity = row[2] as? String ?: store.tenantOf(name)
        return Entry(
            name,
            row[1].asString(),
            backend,
            row[4].asString(),
            identity,
            row[5].asString(),
            row[6].asString(),
        )
    }

    /**
     * A missing directory fails closed after initialization in this process.
     * A restarted process cannot distinguish directory loss from first boot; initialize can bootstrap an empty registry.
     */
    private fun query(
        sql: String,
        vararg args: Any?,
    ): HogliteQueryResult =
        try {
            store.client.query(store.registryTenantId, sql, args.toList())
        } catch (e: Exception) {
            throw HoglakeException.BackendUnavailable("catalog directory unavailable").apply { initCause(e) }
        }

    private fun write(tx: HogliteTx) {
        try {
            store.client.tx(store.registryTenantId, "directory:${UUID.randomUUID()}", null, tx.statements())
        } catch (e: HogliteGuardFailure) {
            throw tx.classify(e)
        } catch (e: Exception) {
            throw HoglakeException.BackendUnavailable("catalog directory unavailable").apply { initCause(e) }
        }
    }

    companion object {
        private const val COLUMNS = "name, data_path, tenant, backend, connection_ref, state, operation"
        private const val PAGE_SIZE = 1000
    }
}
