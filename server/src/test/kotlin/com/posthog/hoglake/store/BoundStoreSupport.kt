package com.posthog.hoglake.store

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.model.NamespaceInfo
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.service.AlterBackend
import com.posthog.hoglake.service.CleanupBackend
import com.posthog.hoglake.service.CompactionBackend
import com.posthog.hoglake.service.ExpiryBackend
import com.posthog.hoglake.service.HydratorMaintenanceBackend
import com.posthog.hoglake.service.MaintenanceStatusBackend
import com.posthog.hoglake.service.OptionsBackend
import com.posthog.hoglake.service.PartitionListingBackend
import com.posthog.hoglake.service.PartitionLookupBackend
import com.posthog.hoglake.service.PartitionStatsBackend
import com.posthog.hoglake.service.PatchField
import com.posthog.hoglake.service.RetirementBackend
import com.posthog.hoglake.service.TableCreation
import com.posthog.hoglake.service.TableCreationBackend
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.service.UploadBackend
import com.posthog.hoglake.service.ViewBackend
import io.ktor.server.application.ApplicationCall
import org.slf4j.LoggerFactory
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Proxy
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.KParameter
import kotlin.reflect.full.declaredMemberFunctions
import kotlin.reflect.full.valueParameters

/**
 * The store or backend behind a request's view of it (BoundCatalogStore.kt,
 * BoundBackends.kt), or the object itself when it is no view.
 *
 * The views give production code no way back to what they wrap. A test reads
 * the field, to assert which store or backend the selector chose.
 */
@Suppress("UNCHECKED_CAST")
internal fun <T : Any> T.plain(): T {
    val views = BoundCatalogStore::class.java.`package`
    if (!javaClass.simpleName.startsWith("Bound") || javaClass.`package` != views) return this
    return javaClass.getDeclaredField("inner").apply { isAccessible = true }.get(this) as T
}

/** The audit's refusal lines (`auth_refused`, then its values) while [block] runs, beside what it returned. */
internal inline fun <T> auditRefusals(block: () -> T): Pair<T, List<String>> {
    val capture = ListAppender<ILoggingEvent>().apply { start() }
    val audit = LoggerFactory.getLogger(Audit.LOGGER_NAME) as ch.qos.logback.classic.Logger
    audit.addAppender(capture)
    val result =
        try {
            block()
        } finally {
            audit.detachAppender(capture)
        }
    val lines =
        capture.list.filter { it.formattedMessage.startsWith("auth_refused ") }
            .map { it.formattedMessage + " " + it.argumentArray.joinToString(" ") }
    return result to lines
}

/** What a scoped caller's view does with one method (auth/StoreGrant.kt). Every caller is held to the catalog first. */
internal enum class Rule {
    /** The catalog only. */
    OPEN,

    /** The `namespace` argument, for read. */
    READ,

    /** The `namespace` argument, for write. */
    WRITE,

    /** The namespace the body names (a commit, a table creation, an upload claim's prefix), for write. */
    BODY,

    /** The namespace of the object an id names, known once it is read. */
    ID_READ,

    /** The same, for write: checked before the operation. */
    ID_WRITE,

    /** Not for a scoped caller. */
    CLOSED,

    /** [READ] with a namespace, [CLOSED] with none. */
    READ_OR_CLOSED,

    /** The result is narrowed to the caller's namespaces. */
    FILTER,

    /** The containment lookup the checks themselves read: null for a namespace the token does not allow. */
    LOOKUP,

    /** The catalog listing: the names must be the token's. */
    CATALOGS,
}

/**
 * Every method of the store and of each sub-backend, with its rule, and the
 * means to call each one through a view with chosen values.
 *
 * The list is written by hand on purpose. BoundStoreTest compares it with
 * the methods the interfaces declare, so a method added to an interface
 * fails there until it has a rule here (and a view that compiles).
 */
internal object BoundStoreCalls {
    const val DATA_PATH = "s3://bucket/alpha"

    val rules: Map<KClass<*>, Map<String, Rule>> =
        mapOf(
            CatalogStore::class to
                mapOf(
                    "createCatalog" to Rule.CLOSED,
                    "getCatalog" to Rule.OPEN,
                    "listCatalogs" to Rule.CATALOGS,
                    "dropCatalog" to Rule.CLOSED,
                    "storageUsage" to Rule.CLOSED,
                    "createNamespace" to Rule.CLOSED,
                    "listNamespaces" to Rule.FILTER,
                    "getNamespace" to Rule.READ,
                    "dropNamespace" to Rule.CLOSED,
                    "setNamespaceContainment" to Rule.CLOSED,
                    "liveContainment" to Rule.LOOKUP,
                    "createTable" to Rule.WRITE,
                    "getTable" to Rule.READ,
                    "listTables" to Rule.READ,
                    "dropTable" to Rule.WRITE,
                    "truncateTable" to Rule.WRITE,
                    "listFiles" to Rule.READ,
                    "partitionValues" to Rule.READ,
                    "fileStats" to Rule.READ,
                    "changes" to Rule.READ,
                    "resolveSnapshot" to Rule.CLOSED,
                    "listSnapshots" to Rule.CLOSED,
                    "commit" to Rule.BODY,
                    "receipt" to Rule.CLOSED,
                    "planScan" to Rule.READ,
                    "listConsumers" to Rule.CLOSED,
                    "listOffsets" to Rule.CLOSED,
                    "getOffset" to Rule.CLOSED,
                    "commitOffset" to Rule.CLOSED,
                ),
            UploadBackend::class to
                mapOf(
                    "claim" to Rule.BODY,
                    "renew" to Rule.OPEN,
                    "abandon" to Rule.OPEN,
                    "scheduleExpired" to Rule.CLOSED,
                ),
            TableCreationBackend::class to
                mapOf(
                    "prepare" to Rule.BODY,
                    "status" to Rule.ID_READ,
                    "abort" to Rule.ID_WRITE,
                    "publish" to Rule.ID_WRITE,
                ),
            AlterBackend::class to mapOf("alterTable" to Rule.WRITE),
            ViewBackend::class to
                mapOf("create" to Rule.WRITE, "get" to Rule.READ, "list" to Rule.READ, "drop" to Rule.WRITE),
            PartitionStatsBackend::class to mapOf("partitionStats" to Rule.READ_OR_CLOSED),
            PartitionListingBackend::class to mapOf("listPartitions" to Rule.READ),
            PartitionLookupBackend::class to mapOf("lookup" to Rule.READ),
            OptionsBackend::class to mapOf("get" to Rule.CLOSED, "patch" to Rule.CLOSED),
            ExpiryBackend::class to mapOf("runOnce" to Rule.CLOSED),
            CleanupBackend::class to mapOf("runOnce" to Rule.CLOSED),
            CompactionBackend::class to mapOf("runOnce" to Rule.CLOSED),
            RetirementBackend::class to mapOf("runOnce" to Rule.CLOSED),
            HydratorMaintenanceBackend::class to mapOf("rehydrateFailed" to Rule.CLOSED),
            MaintenanceStatusBackend::class to mapOf("status" to Rule.CLOSED, "runs" to Rule.CLOSED),
        )

    data class Op(
        val type: KClass<*>,
        val fn: KFunction<*>,
        val rule: Rule,
    ) {
        val name: String get() = "${type.simpleName}.${fn.name}"

        override fun toString(): String = name
    }

    /** Every method the interfaces declare that has a rule above. */
    val ops: List<Op> =
        rules.flatMap { (type, byName) ->
            type.declaredMemberFunctions.mapNotNull { fn -> byName[fn.name]?.let { Op(type, fn, it) } }
        }

    /** The id of the table creation the recording backend holds in [namespace]. */
    fun creationId(namespace: String): UUID = UUID.nameUUIDFromBytes(namespace.toByteArray())

    /**
     * How a scoped caller is told it may not have [op] in [namespace]: as a
     * missing namespace, or for an operation on a table creation by id, as a
     * creation that does not exist.
     */
    fun missing(
        op: Op,
        namespace: String,
    ): String =
        if (op.rule == Rule.ID_READ || op.rule == Rule.ID_WRITE) {
            "table creation operation '${creationId(namespace)}'"
        } else {
            "namespace '$namespace' in catalog 'alpha'"
        }

    /**
     * Values for every parameter of [op]: [catalog] and [namespace] wherever
     * the method names one (in a parameter, a body or behind an id), and a
     * value that differs by position everywhere else.
     */
    fun arguments(
        op: Op,
        catalog: String,
        namespace: String,
    ): List<Any?> = op.fn.valueParameters.mapIndexed { i, p -> argument(op, p, i, catalog, namespace) }

    private fun argument(
        op: Op,
        p: KParameter,
        i: Int,
        catalog: String,
        namespace: String,
    ): Any? {
        val type = p.type.classifier as KClass<*>
        return when {
            p.name == "catalog" -> catalog
            p.name == "name" && op.fn.name in setOf("createCatalog", "getCatalog") -> catalog
            p.name == "namespace" -> namespace
            p.name == "prefix" -> "$DATA_PATH/data/$namespace"
            type == CommitRequest::class -> CommitRequest(appends = listOf(TableAppend(namespace, "t", emptyList())))
            type == TableCreationDefinition::class -> TableCreationDefinition(namespace, "t", emptyList())
            type == UUID::class ->
                if (op.rule == Rule.ID_READ || op.rule == Rule.ID_WRITE) creationId(namespace) else UUID(0, i.toLong())
            type == String::class -> "s$i"
            type == Long::class -> i.toLong()
            type == Int::class -> i
            type == Boolean::class -> true
            type == List::class -> emptyList<Any>()
            type == Map::class -> emptyMap<Any, Any>()
            type == Set::class -> setOf(catalog)
            type == Instant::class -> Instant.ofEpochSecond(i.toLong())
            type == PatchField::class -> PatchField.Absent
            type.java.isEnum -> type.java.enumConstants.first()
            p.type.isMarkedNullable -> null
            else -> error("no test value for $op.${p.name}: $type")
        }
    }

    /**
     * Whether a scoped caller that may read the namespaces in [readable] and
     * write those in [writable] gets [op] in [namespace] from the store.
     */
    fun reaches(
        op: Op,
        namespace: String,
        readable: Set<String>,
        writable: Set<String>,
    ): Boolean =
        when (op.rule) {
            Rule.OPEN, Rule.FILTER, Rule.LOOKUP, Rule.CATALOGS -> true
            Rule.READ, Rule.ID_READ, Rule.READ_OR_CLOSED -> namespace in readable
            Rule.WRITE, Rule.BODY, Rule.ID_WRITE -> namespace in writable
            Rule.CLOSED -> false
        }

    /** Call [op] on [view]; the refusal it answered with, or null when it returned. */
    fun call(
        view: Any,
        op: Op,
        arguments: List<Any?>,
    ): HoglakeException? =
        try {
            op.fn.call(view, *arguments.toTypedArray())
            null
        } catch (e: InvocationTargetException) {
            e.targetException as? HoglakeException ?: throw e.targetException
        }

    /**
     * What [selector] hands out for [catalog] on this request, by interface:
     * the store, and each sub-backend through the selector function that
     * returns it, over [stores]' recording backend as the PostgreSQL service.
     */
    fun views(
        selector: CatalogStoreSelector,
        call: ApplicationCall,
        catalog: String,
        stores: RecordingStores,
    ): Map<KClass<*>, Any> =
        rules.keys.associateWith { type ->
            if (type == CatalogStore::class) {
                selector.forCatalog(call, catalog)
            } else {
                val provider: () -> Any = { stores.backend(type) }
                try {
                    accessor(type).invoke(selector, call, catalog, provider)
                } catch (e: InvocationTargetException) {
                    throw e.targetException
                }
            }
        }

    /** The selector function that hands out the sub-backend [type], given a provider of the PostgreSQL service. */
    fun accessor(type: KClass<*>) =
        CatalogStoreSelector::class.java.methods.single { m ->
            m.returnType == type.java && m.parameterTypes.size == 3 && m.parameterTypes[2] == Function0::class.java
        }
}

/**
 * A store and one of each sub-backend that record what they are asked and
 * answer just enough for a view to go on: the containment of the namespaces
 * in [contained], a catalog whose data_path is [BoundStoreCalls.DATA_PATH],
 * and a table creation in every namespace of [namespaces] under
 * [BoundStoreCalls.creationId].
 */
internal class RecordingStores(
    private val contained: Map<String, NamespaceContainment>,
    private val namespaces: List<String> = contained.keys.toList() + "plain",
) {
    data class Call(
        val name: String,
        val arguments: List<Any?>,
    )

    val calls = CopyOnWriteArrayList<Call>()

    fun count(name: String): Int = calls.count { it.name == name }

    val store: CatalogStore = backend(CatalogStore::class) as CatalogStore

    private fun creation(
        id: UUID,
        definition: TableCreationDefinition? = null,
    ): TableCreation {
        val found =
            definition ?: namespaces.firstOrNull { BoundStoreCalls.creationId(it) == id }
                ?.let { TableCreationDefinition(it, "t", emptyList()) }
                ?: throw HoglakeException.NotFound("table creation operation '$id'")
        return TableCreation(id, id, found, emptyList(), "", "prepared", Instant.EPOCH, null, null, null)
    }

    fun backend(type: KClass<*>): Any =
        Proxy.newProxyInstance(type.java.classLoader, arrayOf(type.java)) { _, method, args ->
            val given = args?.toList().orEmpty()
            calls += Call("${type.simpleName}.${method.name}", given)
            when {
                method.name == "liveContainment" -> contained[given[1]]
                method.name == "getCatalog" -> CatalogInfo(1, given[0] as String, BoundStoreCalls.DATA_PATH, 0, 0)
                method.name == "listNamespaces" ->
                    namespaces.mapIndexed { i, name -> NamespaceInfo(i.toLong(), name, contained[name]) }
                method.name == "prepare" -> creation(given[1] as UUID, given[2] as TableCreationDefinition)
                method.returnType == TableCreation::class.java -> creation(given[1] as UUID)
                method.returnType == List::class.java -> emptyList<Any>()
                method.returnType == Pair::class.java -> emptyList<Any>() to false
                method.returnType == Int::class.javaPrimitiveType -> 0
                else -> null
            }
        }
}
