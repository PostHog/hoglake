package com.posthog.hoglake.hoglite

import com.posthog.hoglake.DatabaseBackend
import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.DroppedCatalog
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.persistence.CatalogBinding
import com.posthog.hoglake.persistence.CatalogRepo
import com.posthog.hoglake.store.CatalogStore
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Proxy

/** All catalog service ports use the same selection and synchronous incarnation scope. */
internal class CatalogRouter(
    val directory: CatalogDirectory,
    private val postgres: CatalogStore,
    private val hoglite: HogliteCatalogStore,
    private val jdbi: Jdbi,
    private val ensurePostgres: () -> Unit = {},
) {
    private val bindings = ThreadLocal<CatalogDirectory.Entry>()

    /** Tests pause here after selection, before any backend-local lookup. */
    internal var selected: (CatalogDirectory.Entry) -> Unit = {}
    internal var beforePostgresInsert: (CatalogDirectory.Entry) -> Unit = {}
    internal var afterPostgresDrop: (CatalogDirectory.Entry) -> Unit = {}

    val store: CatalogStore =
        port(CatalogStore::class.java, postgres, hoglite) { method, args ->
            when (method.name) {
                "createCatalog" -> create(args)
                "dropCatalog" -> drop(args)
                "listCatalogs" -> {
                    @Suppress("UNCHECKED_CAST")
                    val visible = args[0] as Set<String>?
                    list(visible)
                }
                else -> null
            }
        }

    /**
     * Port methods take the catalog as their first argument.
     * Instance methods need an explicit handler; they cannot infer a catalog.
     */
    fun <T : Any> port(
        type: Class<T>,
        pg: T,
        hg: T,
        instance: ((Method, Array<out Any?>) -> Any?)? = null,
    ): T {
        val proxy =
            Proxy.newProxyInstance(type.classLoader, arrayOf(type)) { proxy, method, argsOrNull ->
                val args = argsOrNull ?: emptyArray()
                when (method.name) {
                    "toString" -> "CatalogRouter(${type.simpleName})"
                    "hashCode" -> System.identityHashCode(proxy)
                    "equals" -> proxy === args.firstOrNull()
                    else -> {
                        val special = instance?.invoke(method, args)
                        if (special != null) {
                            special
                        } else {
                            val name =
                                args.firstOrNull() as? String
                                    ?: error("${type.simpleName}.${method.name} needs an instance handler")
                            operation(
                                name,
                            ) { e -> invoke(method, if (e.backend == DatabaseBackend.POSTGRES) pg else hg, args) }
                        }
                    }
                }
            }
        return type.cast(proxy)
    }

    fun <T> operation(
        name: String,
        body: (CatalogDirectory.Entry) -> T,
    ): T {
        val existing = bindings.get()
        if (existing != null) {
            check(existing.name == name) { "nested operation addresses another catalog" }
            return body(existing)
        }
        val e = directory.find(name) ?: throw HoglakeException.NotFound("catalog '$name'")
        return bound(e) { body(e) }
    }

    fun <T> bound(
        e: CatalogDirectory.Entry,
        body: () -> T,
    ): T {
        if (e.state != "ready") {
            throw HoglakeException.BackendUnavailable(
                "catalog '${e.name}' has a ${e.state} reservation; retry DELETE to cancel",
            )
        }
        val previous = bindings.get()
        check(previous == null || previous == e) { "conflicting catalog selection" }
        bindings.set(e)
        try {
            selected(e)
            return if (e.backend == DatabaseBackend.POSTGRES) {
                ensurePostgres()
                postgresCall { CatalogBinding.withIdentity(e.name, e.identity.toLong(), body) }
            } else {
                hoglite.catalogOperation(e.name, tenant = e.identity, body = body)
            }
        } finally {
            if (previous == null) bindings.remove() else bindings.set(previous)
        }
    }

    private fun list(visible: Set<String>?): List<CatalogInfo> {
        val entries = directory.all(readyOnly = true).filter { visible == null || it.name in visible }
        val infos = mutableMapOf<String, CatalogInfo>()
        val pg = entries.filter { it.backend == DatabaseBackend.POSTGRES }
        if (pg.isNotEmpty()) {
            ensurePostgres()
            postgresCall {
                jdbi.withHandleUnchecked { h ->
                    for (page in pg.chunked(250)) {
                        val rows =
                            CatalogRepo.listByIds(
                                h,
                                page.map { it.identity.toLong() },
                            ).associateBy { it.catalogId }
                        for (entry in page) {
                            rows[entry.identity.toLong()]?.takeIf { it.name == entry.name }?.let { infos[it.name] = it }
                        }
                    }
                }
            }
        }
        val hg = entries.filter { it.backend == DatabaseBackend.HOGLITE }
        infos.putAll(hoglite.listBoundCatalogs(hg.map { it.name to it.identity }).associateBy { it.name })
        // A concurrent drop can remove a selected identity. Never look up its replacement by name.
        return entries.mapNotNull { infos[it.name] }
    }

    private fun create(args: Array<out Any?>): CatalogInfo {
        val name = args[0] as String
        val path = args[1] as String
        val author = args[2] as String?
        val message = args[3] as String?
        val backend = directory.validateSelection(args[4] as String?, args[5] as String?)
        if (backend == DatabaseBackend.POSTGRES) {
            if (!Regex("^[a-z0-9][a-z0-9_-]{0,62}$").matches(name)) {
                throw HoglakeException.Validation("invalid catalog name")
            }
        } else {
            hoglite.validateCatalogName(name)
        }
        hoglite.validateDataPathShape(path)
        // Do not adopt a pre-existing PostgreSQL catalog, including an unregistered one.
        return nameLock(name) {
            if (directory.find(name) != null) throw HoglakeException.AlreadyExists("catalog '$name' already exists")
            val existing = jdbi.withHandleUnchecked { h -> CatalogRepo.listAll(h) }
            for (catalog in existing) {
                if (catalog.name == name) {
                    throw HoglakeException.AlreadyExists(
                        "catalog '$name' already exists in PostgreSQL",
                    )
                }
                val prefix = path.trimEnd('/') + "/"
                val other = catalog.dataPath.trimEnd('/') + "/"
                if (prefix.startsWith(other) || other.startsWith(prefix)) {
                    throw HoglakeException.Validation("data_path overlaps a PostgreSQL catalog")
                }
            }
            if (backend == DatabaseBackend.HOGLITE) {
                hoglite.createCatalog(name, path, author, message)
            } else {
                val id =
                    jdbi.withHandleUnchecked { h ->
                        h.createQuery("SELECT nextval(pg_get_serial_sequence('hog_catalog', 'catalog_id'))")
                            .mapTo(Long::class.java).one()
                    }
                val entry = directory.reserve(name, path, id)
                beforePostgresInsert(entry)
                val info = CatalogBinding.withIdentity(name, id) { postgres.createCatalog(name, path, author, message) }
                // A failed publication retains the reservation and its catalog_id. Never report success.
                directory.publish(entry)
                info
            }
        }
    }

    private fun drop(args: Array<out Any?>): DroppedCatalog {
        val name = args[0] as String
        com.posthog.hoglake.service.requireOperationMetadata(args[1] as String?, args[2] as String?)
        val entry = directory.find(name) ?: throw HoglakeException.NotFound("catalog '$name'")
        selected(entry)
        if (entry.backend == DatabaseBackend.HOGLITE) {
            return bound(entry) { hoglite.dropCatalog(entry) }
        }
        return nameLock(name) {
            // A delayed drop never acts on a replacement reservation.
            if (directory.find(name) != entry) throw HoglakeException.NotFound("catalog '$name' incarnation")
            val id = entry.identity.toLong()
            val result =
                CatalogBinding.withIdentity(name, id) {
                    val present =
                        jdbi.withHandleUnchecked { h ->
                            h.createQuery(
                                "SELECT 1 FROM hog_catalog WHERE catalog_id = ?",
                            ).bind(0, id).mapTo(Int::class.java).findOne().isPresent
                        }
                    if (present) {
                        postgres.dropCatalog(name, args[1] as String?, args[2] as String?)
                    } else {
                        DroppedCatalog(name, entry.dataPath)
                    }
                }
            afterPostgresDrop(entry)
            directory.remove(entry)
            result
        }
    }

    /**
     * One nonblocking acquisition. The same scoped Jdbi handle performs every PostgreSQL mutation.
     * Registry calls have the configured retry deadline. No transaction spans those calls.
     */
    private fun <T> nameLock(
        name: String,
        body: () -> T,
    ): T {
        ensurePostgres()
        return postgresCall {
            jdbi.withHandleUnchecked { h ->
                // Jdbi's HandleScope makes nested service callbacks reuse h, including their transactions.
                // The single-connection-pool and terminated-session interleaving tests pin this guarantee.
                val key =
                    h.createQuery(
                        "SELECT hashtextextended(?, 7249831)",
                    ).bind(0, name).mapTo(Long::class.java).one()
                val acquired =
                    h.createQuery(
                        "SELECT pg_try_advisory_lock(?)",
                    ).bind(0, key).mapTo(Boolean::class.java).one()
                if (!acquired) throw HoglakeException.BackendUnavailable("catalog '$name' lifecycle operation is busy")
                try {
                    body()
                } finally {
                    try {
                        h.createQuery("SELECT pg_advisory_unlock(?)").bind(0, key).mapTo(Boolean::class.java).one()
                    } catch (e: Exception) {
                        // Never return a session with an uncertain lock state to the pool.
                        h.connection.abort(java.util.concurrent.Executor { it.run() })
                        throw e
                    }
                }
            }
        }
    }

    private fun <T> postgresCall(body: () -> T): T =
        try {
            body()
        } catch (e: Exception) {
            if (e is HoglakeException) throw e
            if (generateSequence<Throwable>(e) { it.cause }.any { it is java.sql.SQLException }) {
                throw HoglakeException.BackendUnavailable("configured postgres connection is unavailable")
            }
            throw e
        }

    private fun invoke(
        method: Method,
        target: Any,
        args: Array<out Any?>,
    ): Any? =
        try {
            method.invoke(target, *args)
        } catch (e: InvocationTargetException) {
            throw e.targetException
        }
}
