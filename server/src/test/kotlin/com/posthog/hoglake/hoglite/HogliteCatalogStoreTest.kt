package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.api.toDto
import com.posthog.hoglake.compaction.CompactionConfig
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.CatalogNotEmptyReason
import com.posthog.hoglake.model.DroppedCatalog
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.service.RemovalStore
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.assertj.core.api.Assertions.entry
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.net.InetSocketAddress
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/**
 * The catalog lifecycle of [HogliteCatalogStore] (create, list, drop and
 * the drop fence) and namespace containment, against an in-process fake
 * of hoglite's HTTP API. The fake keeps only the state these paths read:
 * which tenants exist, each tenant's meta row, its drop fence, and the
 * namespace and path rows the containment paths read. It evaluates the
 * guards these paths send by their SQL text. No hoglite binary and no
 * Docker.
 */
class HogliteCatalogStoreTest {
    private val json = jacksonObjectMapper()

    private class Ns(
        val id: Long,
        val name: String,
        var dropped: Boolean = false,
        var field: String? = null,
        var value: Long? = null,
    )

    private class Tenant {
        var name: String? = null
        var dataPath: String? = null

        /** The manifest's data_path, served in X-Hoglite-Data-Path; null is a legacy tenant. */
        var manifestDataPath: String? = null

        /** The `meta` table exists but its row was deleted with raw SQL (with [name] null). */
        var metaRowDeleted = false
        var dropping = false
        val namespaces = mutableListOf<Ns>()

        /** Namespace ids with a table version or a view row. */
        val held = mutableSetOf<Long>()

        /** Namespace ids with a live table. */
        val liveTables = mutableSetOf<Long>()

        /** Paths per table: files, dvs, uploads, removals. */
        val paths = mutableMapOf<String, MutableList<String>>()

        /** Rows the mark transaction's prefix guard counts. */
        var raceHits = 0L

        fun live(name: String) = namespaces.firstOrNull { it.name == name && !it.dropped }

        fun byId(id: Long) = namespaces.first { it.id == id }
    }

    private val tenants = ConcurrentHashMap<String, Tenant>()

    /** One entry per request: "METHOD path", plus the first SQL statement of a /tx. */
    private val requests = CopyOnWriteArrayList<String>()

    /** A fence guard whose SQL contains this text counts this many rows (the catalog is not empty). */
    private var nonEmpty: Pair<String, Long>? = null

    /** What the bootstrap's `pragma_foreign_keys` probe reads: 1 on every real hoglite connection. */
    private var foreignKeys = 1L

    /** When set, the /query probe is refused with this (status, body) instead of answered. */
    private var probeRefusal: Pair<Int, Map<String, Any?>>? = null

    /** The storage-usage read's row: file bytes, DV bytes, undrained removals. */
    private var usage: List<Long> = listOf(0, 0, 0)

    /** The SQL of every statement of every write /tx, in order. */
    private val txStatements = CopyOnWriteArrayList<List<String>>()

    /** Runs once, before the next write /tx: a write that lands after the pre-flight reads. */
    private var beforeWrite: ((Tenant) -> Unit)? = null

    /** When set, a containment scan read answers with this (status, body). */
    private var scanAnswer: Pair<Int, Map<String, Any?>>? = null
    private var authorityRefusal: Pair<String, Int>? = null

    private val server: HttpServer =
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
            createContext("/") { ex -> handle(ex) }
            start()
        }

    private val client = HogliteClient("http://127.0.0.1:${server.address.port}", sleeper = {})

    private val resolved = AtomicInteger()

    /** Incarnation-style ids: the store must use exactly what the resolver names. */
    private val store =
        HogliteCatalogStore(client, { catalog ->
            resolved.incrementAndGet()
            "t-$catalog-1"
        })

    @AfterEach
    fun stop() {
        server.stop(0)
    }

    private fun handle(ex: HttpExchange) {
        val path = ex.requestURI.path
        val body = ex.requestBody.readBytes().takeIf { it.isNotEmpty() }?.let { json.readTree(it) }
        val tenantId = path.removePrefix("/v1/t/").removePrefix("/v1/tenants/").substringBefore('/')
        val first = body?.get("statements")?.get(0)?.get("sql")?.asText()
        requests += "${ex.requestMethod} $path" + (first?.let { " :: $it" } ?: "")
        val (status, answer) =
            when {
                path == authorityRefusal?.first ->
                    authorityRefusal!!.second to mapOf("code" to "refused", "error" to "authority refused")
                ex.requestMethod == "PUT" -> {
                    // hoglite's create: the body's data_path is stored in the
                    // manifest on a fresh tenant, must equal the stored one on
                    // an existing tenant (a legacy tenant holds none, so any
                    // claim mismatches), and is omitted when echoHeader is off
                    // (an engine that predates the field). The answer never
                    // says `existing`: hoglite omits it when the receiving
                    // pod holds the tenant itself, so a duplicate create of
                    // a live catalog looks exactly like a fresh one, and the
                    // store must not read anything into its absence.
                    beforePut?.let {
                        beforePut = null
                        it()
                    }
                    val claimed = body?.get("data_path")?.asText()
                    val existing = tenants[tenantId]
                    if (claimed != null && claimed == invalidDataPath) {
                        400 to
                            mapOf(
                                "code" to "invalid_data_path",
                                "error" to "data_path \"$claimed\" lies inside hoglite's own storage root",
                            )
                    } else if (echoHeader && existing != null && claimed != null &&
                        existing.manifestDataPath != claimed
                    ) {
                        409 to mapOf("code" to "data_path_mismatch", "error" to "tenant holds another data_path")
                    } else if (existing == null && claimed != null && overlapWith != null) {
                        409 to
                            mapOf(
                                "code" to "data_path_overlap",
                                "error" to
                                    "tenant $tenantId cannot take data_path \"$claimed\": it overlaps tenant " +
                                    "t-other-1 (data_path \"$overlapWith\"); tenant data_paths must be disjoint",
                            )
                    } else {
                        val t = tenants.computeIfAbsent(tenantId) { Tenant() }
                        if (existing == null && echoHeader) t.manifestDataPath = claimed
                        200 to mapOf("tenant" to tenantId, "owner" to "pod-1")
                    }
                }
                ex.requestMethod == "DELETE" -> {
                    tenants.remove(tenantId)
                    200 to mapOf("ok" to true)
                }
                path == "/v1/query-batch" -> 200 to mapOf("results" to body!!["queries"].map { batchItem(it) })
                path.endsWith("/release") -> 200 to mapOf("ok" to true)
                tenants[tenantId] == null -> 404 to mapOf("error" to "tenant does not exist")
                path.endsWith("/query") -> {
                    val sql = body!!["sql"].asText()
                    if (scanAnswer != null && sql.contains("instr(path, '%') > 0 ORDER BY path")) {
                        scanAnswer!!
                    } else if (probeRefusal != null && sql == HogliteCatalogStore.FOREIGN_KEYS_PROBE_SQL) {
                        probeRefusal!!
                    } else {
                        200 to query(tenants[tenantId]!!, sql, body["args"])
                    }
                }
                path.endsWith("/tx") && body!!["txid"].asText().startsWith(HogliteClient.READ_TXID_PREFIX) -> {
                    val t = tenants[tenantId]!!
                    200 to mapOf("results" to body["statements"].map { query(t, it["sql"].asText(), it["args"]) })
                }
                path.endsWith("/tx") -> tx(tenants[tenantId]!!, body!!["statements"])
                else -> 400 to mapOf("error" to "unexpected $path")
            }
        val bytes = json.writeValueAsBytes(answer)
        // Every tenant-scoped answer repeats the manifest's data_path, as hoglite does.
        if (path != "/v1/query-batch") {
            tenants[tenantId]?.manifestDataPath?.let { ex.responseHeaders.add(HogliteClient.DATA_PATH_HEADER, it) }
        }
        ex.sendResponseHeaders(status, bytes.size.toLong())
        ex.responseBody.use { it.write(bytes) }
    }

    /** False fakes an engine that predates the manifest data_path: no header, nothing stored. */
    private var echoHeader = true

    /** When set, a create of a NEW tenant with a data_path is refused as overlapping tenant `t-other-1`'s. */
    private var overlapWith: String? = null

    /** A data_path hoglite's own validation refuses (400 `invalid_data_path`). */
    private var invalidDataPath: String? = null

    /** Runs once, before the next tenant PUT is handled: a racing create that lands between the probe and the PUT. */
    private var beforePut: (() -> Unit)? = null

    private fun result(vararg rows: List<Any?>): Map<String, Any?> =
        mapOf("cols" to List(rows.firstOrNull()?.size ?: 0) { "c$it" }, "rows" to rows.toList(), "affected" to 0)

    private fun metaRow(t: Tenant): List<Any?> = listOf(t.name, t.dataPath, 0, 0, 0, null, null, 0)

    private fun nsRow(n: Ns): List<Any?> = listOf(n.id, n.name, n.field, n.value)

    /** The rows of [table] in [lo, hi), sorted as an index walk returns them. */
    private fun pathsIn(
        t: Tenant,
        table: String,
        lo: String,
        hi: String,
    ) = t.paths[table].orEmpty().filter { it >= lo && it < hi }.sorted()

    private fun query(
        t: Tenant,
        sql: String,
        args: JsonNode? = null,
    ): Map<String, Any?> =
        when {
            sql == HogliteCatalogStore.FOREIGN_KEYS_PROBE_SQL -> result(listOf(foreignKeys))
            sql == HogliteCatalogStore.META_TABLE_EXISTS_SQL ->
                result(listOf(if (t.name == null && !t.metaRowDeleted) 0 else 1))
            sql.startsWith("SELECT name, data_path,") -> if (t.name == null) result() else result(metaRow(t))
            sql == HogliteCatalogStore.STORAGE_USAGE_SQL -> result(usage)
            sql.startsWith("SELECT id, name, contained_field, contained_value FROM namespaces WHERE name = ?") ->
                t.live(args!![0].asText())?.let { result(nsRow(it)) } ?: result()
            sql.startsWith("SELECT id, name, contained_field, contained_value FROM namespaces WHERE dropped") ->
                result(*t.namespaces.filter { !it.dropped }.sortedBy { it.name }.map { nsRow(it) }.toTypedArray())
            sql.startsWith("SELECT contained_field, contained_value FROM namespaces WHERE name = ?") ->
                t.live(args!![0].asText())?.let { result(listOf(it.field, it.value)) } ?: result()
            sql.startsWith("SELECT contained_field, contained_value FROM namespaces WHERE id = ?") ->
                t.byId(args!![0].asLong()).let { result(listOf(it.field, it.value)) }
            sql.startsWith("SELECT (SELECT next_file_id") -> result(listOf(1, 0, 0))
            sql.startsWith("SELECT (SELECT COUNT(*) FROM table_versions") ->
                result(listOf(if (t.live(args!![0].asText())?.id in t.held) 1 else 0))
            sql.startsWith("SELECT COUNT(*) FROM namespaces WHERE name = ? AND id <> (") -> {
                val live = t.live(args!![0].asText())
                result(listOf(t.namespaces.count { it.name == args[0].asText() && it !== live }))
            }
            sql.contains("instr(path, '%') > 0 ORDER BY path") -> {
                val table = sql.substringAfter("FROM ").substringBefore(" ")
                val after = args!![0].asText()
                val inclusive = sql.contains("path >= ?")
                val rows =
                    pathsIn(t, table, after, args[1].asText())
                        .filter { '%' in it && (inclusive || it > after) }
                        .take(HogliteCatalogStore.CONTAINMENT_SCAN_PAGE)
                result(*rows.map { listOf(it) }.toTypedArray())
            }
            sql.startsWith("SELECT path FROM ") && sql.endsWith("LIMIT 1") -> {
                val table = sql.substringAfter("FROM ").substringBefore(" ")
                pathsIn(t, table, args!![0].asText(), args[1].asText()).firstOrNull()?.let { result(listOf(it)) }
                    ?: result()
            }
            // The count a CatalogNotEmpty message reports.
            else -> result(listOf(nonEmpty?.second ?: 0))
        }

    private fun batchItem(q: JsonNode): Map<String, Any?> {
        val t =
            tenants[q["tenant"].asText()]
                ?: return mapOf("status" to 404, "body" to mapOf("error" to "tenant does not exist"))
        return buildMap {
            put("status", 200)
            put("body", query(t, q["sql"].asText()))
            t.manifestDataPath?.let { put("data_path", it) }
        }
    }

    /** The value a guard's COUNT(*) returns on [t]. */
    private fun count(
        t: Tenant,
        sql: String,
        args: JsonNode?,
    ): Long {
        fun nsArg() = t.byId(args!![0].asLong())
        return when {
            sql.contains("sqlite_master") && sql.contains("'meta'") -> if (t.name == null) 0 else 1
            sql == HogliteCatalogStore.FOREIGN_KEYS_PROBE_SQL -> foreignKeys
            sql.contains("dropping = 0") -> if (t.dropping) 0 else 1
            nonEmpty != null && sql.contains(nonEmpty!!.first) -> nonEmpty!!.second
            sql == "SELECT COUNT(*) FROM namespaces WHERE id = ? AND dropped_snapshot IS NULL" ->
                if (nsArg().dropped) 0 else 1
            sql == "SELECT COUNT(*) FROM namespaces WHERE id = ? AND contained_field IS NOT NULL" ->
                if (nsArg().field != null) 1 else 0
            sql == "SELECT COUNT(*) FROM table_versions WHERE namespace_id = ?" ||
                sql == "SELECT COUNT(*) FROM views WHERE namespace_id = ?" ->
                if (args!![0].asLong() in t.held) 1 else 0
            sql.startsWith("SELECT COUNT(*) FROM tables WHERE namespace_id = ?") ->
                if (args!![0].asLong() in t.liveTables) 1 else 0
            sql == "SELECT COUNT(*) FROM namespaces WHERE name = ? AND id <> ?" ->
                t.namespaces.count { it.name == args!![0].asText() && it.id != args[1].asLong() }.toLong()
            sql.startsWith("SELECT (SELECT COUNT(*) FROM files WHERE path >= ?") -> t.raceHits
            sql.startsWith("SELECT COUNT(*) FROM namespaces WHERE contained_field = ?") ->
                t.namespaces.count {
                    !it.dropped && it.field == args!![0].asText() && it.value == args[1].asLong() &&
                        it.id != args[2].asLong()
                }.toLong()
            else -> -1 // "passes": the statement's own expectation
        }
    }

    private fun tx(
        t: Tenant,
        statements: JsonNode,
    ): Pair<Int, Map<String, Any?>> {
        beforeWrite?.let {
            beforeWrite = null
            it(t)
        }
        txStatements += statements.map { it["sql"].asText() }
        for ((i, st) in statements.withIndex()) {
            val expected = st["expect_scalar_int"]?.asLong() ?: continue
            val actual = count(t, st["sql"].asText(), st["args"]).takeIf { it >= 0 } ?: expected
            if (actual != expected) {
                val error = "expect_failed: expectation failed at statement $i"
                return 409 to mapOf("error" to error, "guard_index" to i + 1)
            }
        }
        for (st in statements) {
            val sql = st["sql"].asText()
            if (sql.startsWith("INSERT INTO meta")) {
                t.name = st["args"][0].asText()
                t.dataPath = st["args"][1].asText()
            }
            if (sql == "UPDATE meta SET dropping = 1 WHERE id = 1") t.dropping = true
            if (sql.startsWith("UPDATE namespaces SET contained_field")) {
                val args = st["args"]
                t.byId(args[2].asLong()).apply {
                    field = args[0].takeUnless { it.isNull }?.asText()
                    value = args[1].takeUnless { it.isNull }?.asLong()
                }
            }
        }
        return 200 to mapOf("results" to statements.map { result() })
    }

    private fun seed(
        tenantId: String,
        name: String,
        dataPath: String = "s3://bucket/$name",
        /** The manifest's value; defaults to the row's. Null seeds a legacy tenant. */
        manifestDataPath: String? = dataPath,
    ) {
        tenants[tenantId] =
            Tenant().apply {
                this.name = name
                this.dataPath = dataPath
                this.manifestDataPath = manifestDataPath
            }
    }

    @Test
    fun `create bootstraps the tenant the resolver names in one guarded transaction`() {
        val info = store.createCatalog("wh", "s3://bucket/wh", "me", "hello")

        assertThat(info.name).isEqualTo("wh")
        assertThat(info.dataPath).isEqualTo("s3://bucket/wh")
        assertThat(info.headSnapshotId).isZero()
        assertThat(tenants.keys).containsExactly("t-wh-1")
        // The tenant is probed (absent: this call's PUT makes it) before the create.
        assertThat(requests[0]).isEqualTo("POST /v1/t/t-wh-1/query")
        assertThat(requests[1]).isEqualTo("PUT /v1/tenants/t-wh-1")
        // The tenant create carries the data_path, so hoglite holds the trusted copy.
        assertThat(tenants["t-wh-1"]!!.manifestDataPath).isEqualTo("s3://bucket/wh")
        // The enforcement probe reads the new tenant before anything is written to it.
        assertThat(requests[2]).isEqualTo("POST /v1/t/t-wh-1/query")
        // The duplicate guard is statement 0, before any DDL.
        assertThat(requests[3]).startsWith("POST /v1/t/t-wh-1/tx :: SELECT COUNT(*) FROM sqlite_master")
        // The enforcement probe is statement 1, still before any DDL.
        assertThat(txStatements.single()[1]).isEqualTo(HogliteCatalogStore.FOREIGN_KEYS_PROBE_SQL)
        // The read-back of the new catalog reuses the operation's binding.
        assertThat(resolved.get()).isEqualTo(1)
    }

    /**
     * The schema's FOREIGN KEYs hold only with `PRAGMA foreign_keys` on,
     * which hoglite sets on every tenant connection. A connection that
     * reads 0 is a misconfigured engine: the bootstrap refuses (503, so
     * the caller retries once it is fixed) and lays no schema down.
     */
    @Test
    fun `create refuses a tenant whose connection does not enforce foreign keys`() {
        foreignKeys = 0

        val failure = catchThrowable { store.createCatalog("wh", "s3://bucket/wh", null, null) }

        assertThat(failure)
            .isExactlyInstanceOf(HoglakeException.BackendUnavailable::class.java)
            .hasMessageContaining("does not enforce foreign keys")
            .hasMessageContaining("t-wh-1")
        assertThat(tenants["t-wh-1"]!!.name).isNull()
        assertThat(catchThrowable { store.getCatalog("wh") }).isInstanceOf(HoglakeException.NotFound::class.java)
    }

    /**
     * An engine that refuses to report the flag at all (a build denying the
     * pragma) is the same misconfiguration: 503, not the Validation the
     * generic refusal mapping would give the caller, and no bootstrap sent.
     */
    @ParameterizedTest
    @ValueSource(ints = [409, 422])
    fun `create refuses a tenant whose engine will not report foreign-key enforcement`(status: Int) {
        probeRefusal = status to mapOf("code" to "refused", "error" to "not authorized")

        val failure = catchThrowable { store.createCatalog("wh", "s3://bucket/wh", null, null) }

        assertThat(failure)
            .isExactlyInstanceOf(HoglakeException.BackendUnavailable::class.java)
            .hasMessageContaining("refused to report PRAGMA foreign_keys")
            .hasMessageContaining("not authorized")
        assertThat(txStatements).isEmpty()
        assertThat(tenants["t-wh-1"]!!.name).isNull()
    }

    /**
     * An engine that cannot answer the probe (503) is unavailable now, not
     * a refused bootstrap later: the in-transaction guard would map a 409
     * or 422 there to Validation.
     */
    @Test
    fun `create answers 503 when the engine cannot answer the foreign-key probe`() {
        probeRefusal = 503 to mapOf("error" to "lease lost")

        val failure = catchThrowable { store.createCatalog("wh", "s3://bucket/wh", null, null) }

        assertThat(failure)
            .isExactlyInstanceOf(HoglakeException.BackendUnavailable::class.java)
            .hasMessageContaining("lease lost")
        assertThat(txStatements).isEmpty()
        assertThat(tenants["t-wh-1"]!!.name).isNull()
    }

    @Test
    fun `a create on a tenant that already holds a catalog is AlreadyExists and changes nothing`() {
        seed("t-wh-1", "wh", "s3://bucket/old")

        val failure = catchThrowable { store.createCatalog("wh", "s3://bucket/new", null, null) }

        assertThat(failure)
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)
            .hasMessageContaining("'wh'")
            .hasMessageContaining("set at create")
        assertThat(tenants["t-wh-1"]!!.dataPath).isEqualTo("s3://bucket/old")
        // Decided from the tenant's own schema: no PUT, no bootstrap statement.
        assertThat(requests).containsExactly("POST /v1/t/t-wh-1/query")

        // The same path is the plain duplicate.
        requests.clear()
        assertThatThrownBy { store.createCatalog("wh", "s3://bucket/old", null, null) }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)
            .hasMessage("catalog 'wh' already exists")
        assertThat(requests).containsExactly("POST /v1/t/t-wh-1/query")
    }

    @Test
    fun `a duplicate create of a live catalog leaves it intact whatever shape hoglite's answer has`() {
        // The tenant is absent at the probe and a racing create bootstraps
        // it before this call's PUT, which hoglite then answers exactly as
        // it answers a fresh create (no `existing`: the pod holds the
        // tenant). The bootstrap guard, not the answer, says AlreadyExists,
        // and the live catalog is never deleted.
        beforePut = { seed("t-wh-1", "wh") }

        assertThatThrownBy { store.createCatalog("wh", "s3://bucket/wh", null, null) }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)
        assertThat(requests.map { it.substringBefore(" ::") })
            .containsExactly(
                "POST /v1/t/t-wh-1/query",
                "PUT /v1/tenants/t-wh-1",
                "POST /v1/t/t-wh-1/query",
                "POST /v1/t/t-wh-1/tx",
            )
        assertThat(tenants["t-wh-1"]!!.name).isEqualTo("wh")
        assertThat(tenants["t-wh-1"]!!.dataPath).isEqualTo("s3://bucket/wh")
    }

    @Test
    fun `a create on an engine that does not record the data_path fails before bootstrapping and keeps its tenant`() {
        echoHeader = false

        val failure = catchThrowable { store.createCatalog("wh", "s3://bucket/wh", null, null) }

        assertThat(failure)
            .isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            .hasMessageContaining(HogliteClient.DATA_PATH_HEADER)
        // Nothing is bootstrapped and nothing is deleted: hoglite's answer
        // cannot prove this call made the tenant, so an operator removes it.
        assertThat(
            requests,
        ).containsExactly("POST /v1/t/t-wh-1/query", "PUT /v1/tenants/t-wh-1", "POST /v1/t/t-wh-1/query")
        assertThat(tenants).containsKey("t-wh-1")
        assertThat(tenants["t-wh-1"]!!.name).isNull()

        // A tenant the call FOUND (the probe saw it) is not its to delete,
        // whatever hoglite's answer says.
        tenants["t-wh-1"] = Tenant()
        requests.clear()
        assertThatThrownBy { store.createCatalog("wh", "s3://bucket/wh", null, null) }
            .isInstanceOf(HoglakeException.BackendUnavailable::class.java)
        assertThat(
            requests,
        ).containsExactly("POST /v1/t/t-wh-1/query", "PUT /v1/tenants/t-wh-1", "POST /v1/t/t-wh-1/query")
        assertThat(tenants).containsKey("t-wh-1")
    }

    @Test
    fun `a create whose bootstrap fails never deletes the tenant, and a retry completes it`() {
        authorityRefusal = "/v1/t/t-wh-1/tx" to 403

        assertThatThrownBy { store.createCatalog("wh", "s3://bucket/wh", null, null) }
            .isInstanceOf(HoglakeException.Forbidden::class.java)
        // MUTATION: delete the tenant after a failed bootstrap and this
        // request list grows a DELETE and the tenant is gone.
        assertThat(requests.map { it.substringBefore(" ::") })
            .containsExactly(
                "POST /v1/t/t-wh-1/query",
                "PUT /v1/tenants/t-wh-1",
                "POST /v1/t/t-wh-1/query",
                "POST /v1/t/t-wh-1/tx",
            )
        assertThat(tenants).containsKey("t-wh-1")
        assertThat(tenants["t-wh-1"]!!.name).isNull()

        // The same create again completes it: the probe finds no catalog,
        // the PUT is idempotent, and the bootstrap runs.
        authorityRefusal = null
        requests.clear()
        assertThat(store.createCatalog("wh", "s3://bucket/wh", null, null).name).isEqualTo("wh")
        assertThat(tenants["t-wh-1"]!!.name).isEqualTo("wh")

        // AlreadyExists from the bootstrap guard: a racing create's catalog
        // landed after the PUT, and it stays.
        tenants.clear()
        beforeWrite = { t ->
            t.name = "wh"
            t.dataPath = "s3://bucket/wh"
        }
        requests.clear()
        assertThatThrownBy { store.createCatalog("wh", "s3://bucket/wh", null, null) }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)
        assertThat(requests.map { it.substringBefore(" ::") })
            .containsExactly(
                "POST /v1/t/t-wh-1/query",
                "PUT /v1/tenants/t-wh-1",
                "POST /v1/t/t-wh-1/query",
                "POST /v1/t/t-wh-1/tx",
            )
        assertThat(tenants["t-wh-1"]!!.name).isEqualTo("wh")
    }

    @Test
    fun `a failed bootstrap on a tenant that holds a catalog by then leaves the catalog`() {
        // Absent at the probe; a racing create bootstraps it before the PUT
        // (hoglite's 200 says nothing of it); this call's bootstrap then
        // fails for a reason other than the guard. Nothing is deleted and
        // the failure surfaces as it is.
        beforePut = { seed("t-wh-1", "wh") }
        authorityRefusal = "/v1/t/t-wh-1/tx" to 403

        assertThatThrownBy { store.createCatalog("wh", "s3://bucket/wh", null, null) }
            .isInstanceOf(HoglakeException.Forbidden::class.java)
        assertThat(requests.map { it.substringBefore(" ::") })
            .containsExactly(
                "POST /v1/t/t-wh-1/query",
                "PUT /v1/tenants/t-wh-1",
                "POST /v1/t/t-wh-1/query",
                "POST /v1/t/t-wh-1/tx",
            )
        assertThat(tenants["t-wh-1"]!!.name).isEqualTo("wh")
    }

    @Test
    fun `hoglite's overlap refusal is the 422 PostgreSQL answers for the same condition`() {
        overlapWith = "s3://bucket"

        val failure = catchThrowable { store.createCatalog("wh", "s3://bucket/wh", null, null) }

        assertThat(failure)
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("overlaps another catalog's")
            .hasMessageContaining("catalog data_paths must be disjoint")
            .hasMessageContaining("t-other-1")
        // Nothing was created, so nothing is deleted.
        assertThat(requests).containsExactly("POST /v1/t/t-wh-1/query", "PUT /v1/tenants/t-wh-1")
        assertThat(tenants).isEmpty()
    }

    @Test
    fun `hoglite's own data_path refusal is a 422 in its words, not a 500`() {
        invalidDataPath = "s3://bucket/wh"

        val failure = catchThrowable { store.createCatalog("wh", "s3://bucket/wh", null, null) }

        assertThat(failure)
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("'wh'")
            .hasMessageContaining("lies inside hoglite's own storage root")
        assertThat(requests).containsExactly("POST /v1/t/t-wh-1/query", "PUT /v1/tenants/t-wh-1")
        assertThat(tenants).isEmpty()
    }

    @Test
    fun `a drop removes a tenant whose catalog never bootstrapped`() {
        // The create failed between the tenant PUT and its one transaction
        // (and its clean-up failed too): a tenant with the manifest's
        // data_path and no schema, which locks the name and the prefix.
        tenants["t-wh-1"] = Tenant().apply { manifestDataPath = "s3://bucket/wh" }

        val dropped = store.dropCatalog("wh", null, null)

        assertThat(dropped.dataPath).isEqualTo("s3://bucket/wh")
        // No fence transaction: there is no catalog to prove empty.
        assertThat(requests.map { it.substringBefore(" ::") })
            .containsExactly("POST /v1/t/t-wh-1/query", "POST /v1/t/t-wh-1/release", "DELETE /v1/tenants/t-wh-1")
        assertThat(tenants).isEmpty()
        // The name is free: a create now bootstraps a fresh catalog.
        assertThat(store.createCatalog("wh", "s3://bucket/elsewhere", null, null).dataPath)
            .isEqualTo("s3://bucket/elsewhere")

        // A tenant with no data_path at all (an older engine's) drops too,
        // and reports no prefix.
        tenants["t-x-1"] = Tenant()
        assertThat(store.dropCatalog("x", null, null).dataPath).isEmpty()
        assertThat(tenants).doesNotContainKey("t-x-1")

        // A `meta` table WITHOUT its row is not an unbootstrapped tenant: the
        // bootstrap writes both at once, so only raw SQL leaves this shape.
        // Refused as damage; the tenant stays, with no fence and no delete.
        tenants["t-y-1"] = Tenant().apply { metaRowDeleted = true }
        requests.clear()
        assertThatThrownBy { store.dropCatalog("y", null, null) }
            .isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            .hasMessageContaining("no metadata row")
        assertThat(requests.map { it.substringBefore(" ::") })
            .containsExactly("POST /v1/t/t-y-1/query", "POST /v1/t/t-y-1/query")
        assertThat(tenants).containsKey("t-y-1")
    }

    @Test
    fun `a drop that finds a tenant with no catalog waits for the create that is bootstrapping it`() {
        // The tenant is absent at the create's probe. Between its PUT and
        // its bootstrap a drop starts on another thread: it would find a
        // tenant with no catalog in it and delete it, after the bootstrap
        // landed, with no fence and no emptiness proof, while the create
        // answered success. The drop's probe-and-delete takes the create
        // lock, so it blocks on it until the create (read-back included)
        // has left, then finds a bootstrapped catalog and drops THAT, the
        // fenced way.
        val lock = client.createLock
        var drop: Thread? = null
        var dropped: DroppedCatalog? = null
        beforePut = {
            drop =
                Thread { dropped = store.dropCatalog("wh", null, null) }.apply {
                    isDaemon = true
                    start()
                }
            awaitBlockedOn(drop!!, lock)
        }

        val created = store.createCatalog("wh", "s3://bucket/wh", null, null)
        drop!!.join(10_000)

        assertThat(created.dataPath).isEqualTo("s3://bucket/wh")
        assertThat(dropped?.dataPath).isEqualTo("s3://bucket/wh")
        assertThat(requests.map { it.substringBefore(" ::") })
            .containsExactly(
                // The create, whole, under the lock.
                "POST /v1/t/t-wh-1/query",
                "PUT /v1/tenants/t-wh-1",
                "POST /v1/t/t-wh-1/query",
                "POST /v1/t/t-wh-1/tx",
                "POST /v1/t/t-wh-1/query",
                // Then the drop: a bootstrapped catalog, so fenced before deleted.
                "POST /v1/t/t-wh-1/query",
                "POST /v1/t/t-wh-1/query",
                "POST /v1/t/t-wh-1/tx",
                "POST /v1/t/t-wh-1/release",
                "DELETE /v1/tenants/t-wh-1",
            )
        assertThat(txStatements.last()).contains("UPDATE meta SET dropping = 1 WHERE id = 1")
        assertThat(tenants).isEmpty()
    }

    /**
     * Spin until [thread] is blocked entering [monitor] (not merely some
     * monitor), or has ended (it never blocked: the caller's assertions
     * say so), or fail after ten seconds.
     */
    private fun awaitBlockedOn(
        thread: Thread,
        monitor: Any,
    ) {
        val threads = java.lang.management.ManagementFactory.getThreadMXBean()
        val deadline = System.nanoTime() + 10_000_000_000L
        while (thread.isAlive && System.nanoTime() < deadline) {
            val info = threads.getThreadInfo(thread.threadId())
            if (info?.threadState == Thread.State.BLOCKED &&
                info.lockInfo?.identityHashCode == System.identityHashCode(monitor)
            ) {
                return
            }
            Thread.sleep(1)
        }
        throw AssertionError("the drop never blocked on the create lock")
    }

    @Test
    fun `a drop refused for undrained removals says when a legacy catalog can never drain them`() {
        seed("t-wh-1", "wh", manifestDataPath = null)
        nonEmpty = "FROM removals" to 2L

        val failure = catchThrowable { store.dropCatalog("wh", null, null) }

        assertThat(failure).isInstanceOf(HoglakeException.CatalogNotEmpty::class.java)
        assertThat((failure as HoglakeException.CatalogNotEmpty).reason)
            .isEqualTo(CatalogNotEmptyReason.REMOVALS_PENDING)
        assertThat(failure)
            .hasMessageContaining("2 removal(s) are not drained and never will be")
            .hasMessageContaining("predates the trusted data_path")
            .hasMessageContaining("create the catalog again")
        assertThat(tenants["t-wh-1"]!!.dropping).isFalse()
        assertThat(requests.filter { it.startsWith("DELETE") || it.endsWith("/release") }).isEmpty()

        // A catalog with a trusted path gets the plain reason: cleanup drains it.
        seed("t-ok-1", "ok")
        assertThat(catchThrowable { store.dropCatalog("ok", null, null) })
            .hasMessageContaining("2 removal(s) are not drained")
            .hasMessageNotContaining("never will be")
    }

    @Test
    fun `an answer drops and counts each file outside the trusted prefix, and a legacy tenant drops none`() {
        val trusted =
            HogliteCatalogStore.MetaRow("wh", "s3://bucket/wh", 1, 1, 0, null, null, false, dataPathTrusted = true)
        val legacy =
            HogliteCatalogStore.MetaRow("wh", "s3://bucket/wh", 1, 1, 0, null, null, false, dataPathTrusted = false)
        val inside = listOf("s3://bucket/wh/data/a.parquet", "s3://bucket/wh/data/b.parquet")
        val outside = listOf("s3://victim/x.parquet", "s3://bucket/wh2/y.parquet", "s3://bucket/wh/../z.parquet")
        val meters = SimpleMeterRegistry()
        Metrics.bind(meters)
        try {
            for (surface in listOf("scan", "files", "changes")) {
                assertThat(store.underTrustedPrefix("wh", trusted, surface, inside + outside) { listOf(it) })
                    .describedAs(surface)
                    .containsExactlyElementsOf(inside)
                assertThat(
                    meters.find("hoglake_path_outside_data_path_total")
                        .tags("catalog", "wh", "surface", surface).counter()?.count(),
                ).describedAs(surface).isEqualTo(3.0)
            }
            // An item is judged on EVERY path it carries (a file and its DV):
            // one outside takes the item with it.
            val pairs = listOf(inside[0] to inside[1], inside[0] to outside[0])
            assertThat(
                store.underTrustedPrefix("wh", trusted.dataPath, "compaction", pairs) { listOf(it.first, it.second) },
            ).containsExactly(pairs[0])
            assertThat(
                meters.find("hoglake_path_outside_data_path_total")
                    .tags("catalog", "wh", "surface", "compaction").counter()?.count(),
            ).isEqualTo(1.0)
            // A legacy tenant's only prefix is the row's own: nothing is judged.
            assertThat(store.underTrustedPrefix("wh", legacy, "scan", inside + outside) { listOf(it) })
                .containsExactlyElementsOf(inside + outside)
            assertThat(
                meters.find("hoglake_path_outside_data_path_total").tags("catalog", "wh", "surface", "scan")
                    .counter()?.count(),
            ).isEqualTo(3.0)
        } finally {
            Metrics.clear()
            meters.close()
        }
    }

    @Test
    fun `reads answer with the manifest data_path and audit a meta row that differs`() {
        seed("t-wh-1", "wh", dataPath = "s3://victim/x", manifestDataPath = "s3://bucket/wh")
        val meters = SimpleMeterRegistry()
        Metrics.bind(meters)
        try {
            assertThat(store.getCatalog("wh").dataPath).isEqualTo("s3://bucket/wh")
            assertThat(store.listCatalogs(setOf("wh")).single().dataPath).isEqualTo("s3://bucket/wh")
            assertThat(store.dropCatalog("wh", null, null).dataPath).isEqualTo("s3://bucket/wh")
            assertThat(
                meters.find("hoglake_hoglite_data_path_mismatch_total").tag("catalog", "wh").counter()?.count(),
            ).isEqualTo(3.0)
        } finally {
            Metrics.clear()
            meters.close()
        }
    }

    @Test
    fun `a legacy tenant is readable and refuses every storage-acting path`() {
        seed("t-wh-1", "wh", manifestDataPath = null)
        val meters = SimpleMeterRegistry()
        Metrics.bind(meters)
        try {
            assertThat(store.getCatalog("wh").dataPath).isEqualTo("s3://bucket/wh")
            assertThat(meters.find("hoglake_hoglite_data_path_mismatch_total").counter()).isNull()
            val refused =
                catchThrowable {
                    store.uploads.claim("wh", UUID.randomUUID(), UUID.randomUUID(), "s3://bucket/wh/up", "data")
                }
            assertThat(refused)
                .isInstanceOf(HoglakeException.DataPathUnverified::class.java)
                .hasMessageContaining("no trusted data_path")
            // Refused before the claim could be written.
            assertThat(requests.filter { it.startsWith("POST /v1/t/t-wh-1/tx") }).isEmpty()

            // Every other storage-acting job refuses at its gate, before any
            // row it would queue, claim or rewrite. The stores point at a
            // closed port: a job that got past the gate would fail with a
            // connect error, not the typed refusal.
            val cfg =
                com.posthog.hoglake.Config(
                    s3Endpoint = "http://127.0.0.1:9",
                    s3Region = "us-east-1",
                    s3AccessKey = "k",
                    s3SecretKey = "s",
                    s3PathStyle = true,
                )
            val gates =
                mapOf<String, () -> Any>(
                    "cleanup" to {
                        RemovalStore(cfg).use {
                            HogliteCleanup(cfg, store, it).runOnce("wh", 10, MaintenanceTrigger.MANUAL)
                        }
                    },
                    "compaction" to {
                        ObjectStore(cfg).use {
                            HogliteCompaction(
                                store,
                                it,
                                CompactionConfig(
                                    targetBytes = 1L shl 30,
                                    minInputFiles = 2,
                                    maxInputFiles = 2,
                                    maxFanIn = 2,
                                    maxGroupsPerRun = 10,
                                ),
                            ).runOnce("wh", null, MaintenanceTrigger.MANUAL)
                        }
                    },
                )
            for ((name, run) in gates) {
                assertThat(catchThrowable { run() })
                    .describedAs(name)
                    .isInstanceOf(HoglakeException.DataPathUnverified::class.java)
                    .hasMessageContaining("no trusted data_path")
            }
            assertThat(txStatements.flatten())
                .describedAs("no job wrote what it would have acted on")
                .noneMatch {
                    it.startsWith("INSERT INTO removals") || it.startsWith("INSERT INTO compaction_claims") ||
                        it.startsWith("UPDATE uploads") || it.startsWith("UPDATE files")
                }

            // Expiry, retirement and the expired-upload sweep only QUEUE
            // removal rows (cleanup, gated above, is what deletes), so they
            // are not gated: a legacy catalog must still be able to satisfy
            // its drop guards. Each gets past the meta read to the tenant
            // (whatever this fake then makes of its SQL); the live class
            // runs them to completion.
            val ungated =
                mapOf<String, () -> Any>(
                    "expiry" to { store.expiry.runOnce("wh", 10, MaintenanceTrigger.MANUAL) },
                    "retirement" to { store.retirement.runOnce("wh", MaintenanceTrigger.MANUAL) },
                    "upload sweep" to { store.uploads.scheduleExpired("wh", 10) },
                )
            for ((name, run) in ungated) {
                requests.clear()
                val outcome = catchThrowable { run() }
                if (outcome != null) {
                    assertThat(outcome)
                        .describedAs(name)
                        .isNotInstanceOf(HoglakeException.DataPathUnverified::class.java)
                }
                assertThat(requests.size).describedAs("$name reached the tenant past the meta read").isGreaterThan(1)
            }
        } finally {
            Metrics.clear()
            meters.close()
        }
    }

    @Test
    fun `create refuses an invalid name or data path before any request`() {
        for ((name, path) in listOf("Bad" to "s3://bucket/x", "ok" to "s3://", "ok" to "s3://b/../x", "ok" to "/tmp")) {
            assertThat(catchThrowable { store.createCatalog(name, path, null, null) })
                .`as`("$name $path")
                .isInstanceOf(HoglakeException.Validation::class.java)
        }
        assertThat(requests).isEmpty()
    }

    @Test
    fun `create accepts a numeric-leading name as Postgres does since V13`() {
        val name = "0e9b1c52-7f3a-4d6e-9a41-2b8c5d7e1f60"

        assertThat(store.createCatalog(name, "s3://bucket/u", null, null).name).isEqualTo(name)
    }

    @Test
    fun `storage usage counts every file and DV row and only undrained removals`() {
        seed("t-wh-1", "wh")
        usage = listOf(100, 20, 3)

        val footprint = store.storageUsage("wh")

        assertThat(footprint.trackedBytes).isEqualTo(120)
        assertThat(footprint.pendingDeleteFiles).isEqualTo(3)
        // StorageUsageRepo's contract: ended rows still occupy storage.
        assertThat(HogliteCatalogStore.STORAGE_USAGE_SQL)
            .doesNotContain("end_snapshot")
            .contains("FROM removals WHERE drained_at IS NULL")
    }

    @Test
    fun `storage usage refuses a byte total that overflows`() {
        seed("t-wh-1", "wh")
        usage = listOf(Long.MAX_VALUE, 1, 0)

        assertThat(catchThrowable { store.storageUsage("wh") }).isInstanceOf(ArithmeticException::class.java)
    }

    @Test
    fun `get of a catalog whose tenant does not exist is NotFound`() {
        assertThat(catchThrowable { store.getCatalog("gone") })
            .isInstanceOf(HoglakeException.NotFound::class.java)
            .hasMessageContaining("'gone'")
    }

    @Test
    fun `list reads the named catalogs in one batch and skips missing or foreign tenants`() {
        seed("t-a-1", "a")
        seed("t-b-1", "b")
        seed("t-d-1", "a") // a tenant whose meta names another catalog

        val listed = store.listCatalogs(setOf("b", "a", "c", "d", "a"))

        assertThat(listed.map { it.name }).containsExactly("b", "a")
        assertThat(listed.map { it.dataPath }).containsExactly("s3://bucket/b", "s3://bucket/a")
        assertThat(requests).containsExactly("POST /v1/query-batch")
    }

    @Test
    fun `list skips a name the resolver does not know and keeps the other names`() {
        seed("t-a-1", "a")
        // A drop can remove "gone" from the mapping while a list is running.
        val partial =
            HogliteCatalogStore(client, { catalog ->
                if (catalog == "gone") throw HoglakeException.NotFound("catalog '$catalog'")
                "t-$catalog-1"
            })

        assertThat(partial.listCatalogs(setOf("gone", "a")).map { it.name }).containsExactly("a")
    }

    @Test
    fun `list refuses a request for all catalogs because there is no list to enumerate`() {
        assertThat(catchThrowable { store.listCatalogs(null) })
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("name the catalogs to list")
        assertThat(requests).isEmpty()
    }

    @Test
    fun `list fails when the resolver fails for a reason other than NotFound`() {
        val broken = HogliteCatalogStore(client, { throw IllegalStateException("routing is down") })

        assertThat(catchThrowable { broken.listCatalogs(setOf("a")) })
            .isInstanceOf(IllegalStateException::class.java)
            .hasMessage("routing is down")
    }

    @Test
    fun `drop fences the catalog, then releases and deletes its tenant`() {
        seed("t-wh-1", "wh")

        val dropped = store.dropCatalog("wh", null, null)

        assertThat(dropped.name).isEqualTo("wh")
        assertThat(dropped.dataPath).isEqualTo("s3://bucket/wh")
        assertThat(tenants).isEmpty()
        val writes = requests.filterNot { it.contains("/query") }
        assertThat(writes).hasSize(3)
        assertThat(writes[0]).startsWith("POST /v1/t/t-wh-1/tx :: SELECT COUNT(*) FROM namespaces")
        assertThat(writes[1]).isEqualTo("POST /v1/t/t-wh-1/release")
        assertThat(writes[2]).isEqualTo("DELETE /v1/tenants/t-wh-1")
    }

    @Test
    fun `drop of a catalog whose tenant does not exist is NotFound and deletes nothing`() {
        assertThat(catchThrowable { store.dropCatalog("gone", null, null) })
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(requests.filter { it.startsWith("DELETE") || it.endsWith("/release") }).isEmpty()
    }

    @Test
    fun `drop of a catalog that is not empty names the reason and keeps the tenant`() {
        seed("t-wh-1", "wh")
        nonEmpty = "FROM offsets" to 2L

        val failure = catchThrowable { store.dropCatalog("wh", null, null) }

        assertThat(failure).isInstanceOf(HoglakeException.CatalogNotEmpty::class.java)
        assertThat((failure as HoglakeException.CatalogNotEmpty).reason)
            .isEqualTo(CatalogNotEmptyReason.CONSUMER_OFFSETS)
        assertThat(failure).hasMessageContaining("2 consumer offset(s) remain")
        assertThat(tenants["t-wh-1"]!!.dropping).isFalse()
        assertThat(requests.filter { it.startsWith("DELETE") || it.endsWith("/release") }).isEmpty()
    }

    // ---- namespace containment -------------------------------------------

    @ParameterizedTest
    @ValueSource(ints = [401, 403])
    fun `release authority refusal retains its definite API type`(status: Int) {
        seed("t-wh-1", "wh")
        authorityRefusal = "/v1/t/t-wh-1/release" to status
        val expected =
            if (status == 401) HoglakeException.Unauthenticated::class.java else HoglakeException.Forbidden::class.java
        assertThat(catchThrowable { store.dropCatalog("wh", null, null) }).isInstanceOf(expected)
        assertThat(requests.none { it.startsWith("DELETE") }).isTrue()
    }

    @ParameterizedTest
    @ValueSource(ints = [404, 409, 413])
    fun `release preserves existing non-authentication refusal semantics`(status: Int) {
        seed("t-wh-1", "wh")
        authorityRefusal = "/v1/t/t-wh-1/release" to status
        val failure = catchThrowable { store.dropCatalog("wh", null, null) }
        if (status == 409) {
            assertThat(failure).isNull()
            assertThat(requests.any { it.startsWith("DELETE") }).isTrue()
        } else {
            assertThat(failure).isExactlyInstanceOf(HogliteRejected::class.java)
            assertThat((failure as HogliteRejected).status).isEqualTo(status)
            assertThat(requests.none { it.startsWith("DELETE") }).isTrue()
        }
    }

    @Test
    fun `tenant lifecycle refusal precedes every storage call and fence`() {
        seed("t-wh-1", "wh")
        val tenantStore = HogliteCatalogStore(client, { "t-wh-1" }, operatorAuthority = false)
        assertThat(catchThrowable { tenantStore.dropCatalog("wh", null, null) })
            .isInstanceOf(HoglakeException.Forbidden::class.java)
        assertThat(requests).isEmpty()
        assertThat(tenants["t-wh-1"]!!.dropping).isFalse()
    }

    private val team42 = NamespaceContainment("team_id", 42)

    /** Catalog "wh" with live namespaces "team_42" (id 1) and "other" (id 2). */
    private fun seedNamespaces(): Tenant {
        seed("t-wh-1", "wh")
        return tenants["t-wh-1"]!!.apply {
            namespaces += Ns(1, "team_42")
            namespaces += Ns(2, "other")
        }
    }

    private fun writes() = txStatements.toList()

    @Test
    fun `liveContainment reads the live row and is null for an uncontained, dropped or missing namespace`() {
        val t = seedNamespaces()
        t.byId(1).apply {
            field = "team_id"
            value = 42
        }
        t.namespaces += Ns(3, "gone", dropped = true, field = "team_id", value = 7)

        assertThat(store.liveContainment("wh", "team_42")).isEqualTo(team42)
        assertThat(store.liveContainment("wh", "other")).isNull()
        assertThat(store.liveContainment("wh", "gone")).isNull()
        assertThat(store.liveContainment("wh", "absent")).isNull()
    }

    @Test
    fun `liveContainment of a missing catalog is null, as the namespace gate expects`() {
        assertThat(store.liveContainment("gone", "team_42")).isNull()
        val unknown =
            HogliteCatalogStore(client, { catalog -> throw HoglakeException.NotFound("catalog '$catalog'") })
        assertThat(unknown.liveContainment("gone", "team_42")).isNull()
    }

    @Test
    fun `namespace reads carry the containment`() {
        val t = seedNamespaces()
        t.byId(1).apply {
            field = "team_id"
            value = 42
        }

        assertThat(store.getNamespace("wh", "team_42").containment).isEqualTo(team42)
        assertThat(store.getNamespace("wh", "other").containment).isNull()
        assertThat(store.listNamespaces("wh").associate { it.name to it.containment })
            .containsExactly(entry("other", null), entry("team_42", team42))
    }

    @Test
    fun `a mark is one guarded transaction with no snapshot and no change row`() {
        val t = seedNamespaces()

        val marked = store.setNamespaceContainment("wh", "team_42", team42, "op")

        assertThat(marked.containment).isEqualTo(team42)
        assertThat(t.byId(1).field).isEqualTo("team_id")
        assertThat(t.byId(1).value).isEqualTo(42)
        val tx = writes().single()
        // The drop fence, then the guards in CatalogService's order, then the UPDATE.
        assertThat(tx.map { it.substringBefore(" WHERE") }).containsExactly(
            "SELECT COUNT(*) FROM meta",
            "SELECT COUNT(*) FROM namespaces",
            "SELECT COUNT(*) FROM namespaces",
            "SELECT COUNT(*) FROM table_versions",
            "SELECT COUNT(*) FROM views",
            "SELECT COUNT(*) FROM namespaces",
            "SELECT (SELECT COUNT(*) FROM files",
            "SELECT COUNT(*) FROM namespaces",
            "UPDATE namespaces SET contained_field = ?, contained_value = ?",
        )
        assertThat(tx[2]).contains("contained_field IS NOT NULL")
        assertThat(tx[7]).contains("contained_field = ? AND contained_value = ?")
    }

    @Test
    fun `an org mark is refused before any write, and the catalog does not advertise it`() {
        seedNamespaces()

        assertThat(catchThrowable { store.setNamespaceContainment("wh", "team_42", team42.copy(org = "acme"), "op") })
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("needs the Postgres backend")
        assertThat(writes()).isEmpty()
        assertThat(store.getCatalog("wh").toDto().capabilities)
            .contains("object-prefix-layout-v1")
            .doesNotContain("org-marked-containment-v1")
    }

    @Test
    fun `creating a namespace with a containment mark is refused before any write`() {
        seedNamespaces()

        assertThat(catchThrowable { store.createNamespace("wh", "team_43", "op", null, team42) })
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("needs the Postgres backend")
        assertThat(writes()).isEmpty()
    }

    @Test
    fun `marking the same containment again and clearing an uncontained namespace send no write`() {
        val t = seedNamespaces()
        t.byId(1).apply {
            field = "team_id"
            value = 42
        }

        assertThat(store.setNamespaceContainment("wh", "team_42", team42, null).containment).isEqualTo(team42)
        assertThat(store.setNamespaceContainment("wh", "other", null, null).containment).isNull()
        assertThat(writes()).isEmpty()
    }

    @Test
    fun `a different containment must be cleared first`() {
        val t = seedNamespaces()
        t.byId(1).apply {
            field = "team_id"
            value = 41
        }

        assertThat(catchThrowable { store.setNamespaceContainment("wh", "team_42", team42, null) })
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessageContaining("already contained for team_id = 41; clear it first")
        assertThat(writes()).isEmpty()
    }

    @Test
    fun `a bad field or value is refused before any request`() {
        for (bad in listOf(NamespaceContainment("Team", 1), NamespaceContainment("team_id", 0))) {
            assertThat(catchThrowable { store.setNamespaceContainment("wh", "team_42", bad, null) })
                .`as`("$bad")
                .isInstanceOf(HoglakeException.Validation::class.java)
        }
        assertThat(requests).isEmpty()
    }

    @Test
    fun `a missing catalog or namespace is NotFound`() {
        seedNamespaces()

        assertThat(catchThrowable { store.setNamespaceContainment("gone", "team_42", team42, null) })
            .isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(catchThrowable { store.setNamespaceContainment("wh", "absent", team42, null) })
            .isInstanceOf(HoglakeException.NotFound::class.java)
            .hasMessageContaining("namespace 'absent'")
    }

    @Test
    fun `a namespace that held a table, or whose name was used before, is refused before the scan`() {
        val t = seedNamespaces()
        t.held += 1
        t.namespaces += Ns(3, "other", dropped = true)

        assertThat(catchThrowable { store.setNamespaceContainment("wh", "team_42", team42, null) })
            .isInstanceOf(HoglakeException.NamespaceNotEmpty::class.java)
            .hasMessageContaining("has held a table or view")
        assertThat(catchThrowable { store.setNamespaceContainment("wh", "other", team42, null) })
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessageContaining("has belonged to another, dropped namespace")
        assertThat(writes()).isEmpty()
        assertThat(requests.filter { it.contains("/query") }).isEmpty()
    }

    @Test
    fun `a foreign path under the prefix refuses the mark, in a literal or an escaped spelling`() {
        val t = seedNamespaces()
        val root = "s3://bucket/wh/"
        for ((table, path) in listOf(
            "files" to "${root}data/team_42/a.parquet",
            "removals" to "${root}data/team_42/b.parquet",
            "dvs" to "${root}d%61ta/team_42/c.puffin",
            "uploads" to "${root}data/team%5F42/d.parquet",
        )) {
            t.paths.clear()
            t.paths[table] = mutableListOf(path)

            assertThat(catchThrowable { store.setNamespaceContainment("wh", "team_42", team42, null) })
                .`as`(path)
                .isInstanceOf(HoglakeException.CommitConflict::class.java)
                .hasMessageContaining("'$path' is already registered or claimed under its prefix")
        }
        assertThat(writes()).isEmpty()
    }

    @Test
    fun `escaped paths that name another namespace do not block the mark`() {
        val t = seedNamespaces()
        val root = "s3://bucket/wh/"
        t.paths["files"] =
            (1..HogliteCatalogStore.CONTAINMENT_SCAN_PAGE + 5)
                .map { "${root}data/ot%68er/$it.parquet" }.toMutableList()
        t.paths["dvs"] = mutableListOf("${root}data/team_420/x.puffin", "s3://bucket/wh2/data/team_42/y.puffin")

        assertThat(store.setNamespaceContainment("wh", "team_42", team42, null).containment).isEqualTo(team42)
        // Two pages of escaped file paths.
        assertThat(requests.count { it.contains("/query") }).isEqualTo(5)
    }

    @Test
    fun `deterministic engine limits during containment scans stay nonretryable and do not mark the namespace`() {
        seedNamespaces()
        val limits =
            listOf(
                Triple(504, "query_timeout", HoglakeException.BackendQueryTimeout::class.java),
                Triple(413, "result_too_large", HoglakeException.BackendResultTooLarge::class.java),
                Triple(507, "out_of_memory", HoglakeException.Validation::class.java),
            )
        for ((status, code, type) in limits) {
            scanAnswer = status to mapOf("error" to "engine refused the scan", "code" to code)
            assertThat(catchThrowable { store.setNamespaceContainment("wh", "team_42", team42, null) })
                .isExactlyInstanceOf(type)
            assertThat(writes()).isEmpty()
        }
    }

    @Test
    fun `a write between the pre-flight and the mark is refused by the guards`() {
        val t = seedNamespaces()
        beforeWrite = {
            it.byId(1).apply {
                field = "team_id"
                value = 41
            }
        }
        assertThat(catchThrowable { store.setNamespaceContainment("wh", "team_42", team42, null) })
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessageContaining("already contained for team_id = 41; clear it first")

        t.byId(1).apply {
            field = null
            value = null
        }
        beforeWrite = { it.held += 1 }
        assertThat(catchThrowable { store.setNamespaceContainment("wh", "team_42", team42, null) })
            .isInstanceOf(HoglakeException.NamespaceNotEmpty::class.java)

        t.held.clear()
        beforeWrite = { it.raceHits = 1 }
        assertThat(catchThrowable { store.setNamespaceContainment("wh", "team_42", team42, null) })
            .isInstanceOf(HoglakeException.CommitConflict::class.java)
            .hasMessageContaining("cannot mark namespace 'team_42' contained")

        t.raceHits = 0
        beforeWrite = {
            it.byId(2).apply {
                field = "team_id"
                value = 42
            }
        }
        assertThat(catchThrowable { store.setNamespaceContainment("wh", "team_42", team42, null) })
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)
            .hasMessage("another namespace is already contained for team_id = 42")
        assertThat(t.byId(1).field).isNull()
    }

    @Test
    fun `a clear refuses a namespace with live tables and otherwise sets the columns to null`() {
        val t = seedNamespaces()
        t.byId(1).apply {
            field = "team_id"
            value = 42
        }
        t.liveTables += 1

        assertThat(catchThrowable { store.setNamespaceContainment("wh", "team_42", null, null) })
            .isInstanceOf(HoglakeException.NamespaceNotEmpty::class.java)
            .hasMessageContaining("containment can only be cleared from an empty namespace")
        assertThat(t.byId(1).field).isEqualTo("team_id")

        t.liveTables.clear()
        assertThat(store.setNamespaceContainment("wh", "team_42", null, null).containment).isNull()
        assertThat(t.byId(1).field).isNull()
        assertThat(t.byId(1).value).isNull()
        // No state guard on a clear: Postgres clears what is there under the lock.
        assertThat(writes().last().none { it.contains("contained_field IS NOT NULL") }).isTrue()
    }

    @Test
    fun `a non-operator drop of a contained namespace is Forbidden, before and inside the transaction`() {
        val t = seedNamespaces()
        t.byId(1).apply {
            field = "team_id"
            value = 42
        }

        assertThat(catchThrowable { store.dropNamespace("wh", "team_42", null, "me", "drop", false) })
            .isInstanceOf(HoglakeException.Forbidden::class.java)
            .hasMessage("namespace 'team_42' is contained; only an operator may drop it")
        assertThat(writes()).isEmpty()

        beforeWrite = {
            it.byId(2).apply {
                field = "team_id"
                value = 7
            }
        }
        assertThat(catchThrowable { store.dropNamespace("wh", "other", null, "me", "drop", false) })
            .isInstanceOf(HoglakeException.Forbidden::class.java)
            .hasMessage("namespace 'other' is contained; only an operator may drop it")
        assertThat(t.byId(2).dropped).isFalse()
    }

    @Test
    fun `a write after the drop fence is NotFound because the catalog is being dropped`() {
        seed("t-wh-1", "wh")
        tenants["t-wh-1"]!!.dropping = true

        val failure = catchThrowable { store.createNamespace("wh", "ns", null, null) }

        assertThat(failure).isInstanceOf(HoglakeException.NotFound::class.java).hasMessageContaining("being dropped")
    }
}
