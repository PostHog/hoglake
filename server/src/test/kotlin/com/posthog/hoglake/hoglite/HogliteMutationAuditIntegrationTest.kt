package com.posthog.hoglake.hoglite

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.service.AlterBackend
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.TableCreationBackend
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.service.TableCreationService
import com.posthog.hoglake.service.ViewBackend
import com.posthog.hoglake.service.ViewService
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.PostgresCatalogStore
import com.posthog.hoglake.testing.PgTestSupport
import com.sun.net.httpserver.HttpServer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import net.logstash.logback.encoder.LogstashEncoder
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.ValueSource
import org.slf4j.LoggerFactory
import org.slf4j.MDC
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/** Compare the emitted JSON contract, not merely logger invocation counts. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteMutationAuditIntegrationTest {
    enum class Backend { POSTGRES, HOGLITE }

    private var started = false
    private val engine by lazy { HogliteConformanceEngine().also { started = true } }
    private val db by lazy { PgTestSupport.freshDatabase() }
    private val json = ObjectMapper()
    private val author = "operator:review"
    private val message = "verify audit parity"

    @AfterAll
    fun close() {
        if (started) engine.close()
    }

    private data class Fixture(
        val catalog: String,
        val store: CatalogStore,
        val alter: AlterBackend,
        val views: ViewBackend,
        val creations: TableCreationBackend,
    )

    private fun fixture(backend: Backend): Fixture {
        val cat = "audit" + UUID.randomUUID().toString().replace("-", "")
        val result =
            if (backend == Backend.HOGLITE) {
                Fixture(cat, engine.store, engine.store.alters, engine.store.views, engine.store.tableCreations)
            } else {
                val catalogs = CatalogService(db.jdbi)
                Fixture(
                    cat,
                    PostgresCatalogStore(
                        catalogs,
                        CommitService(db.jdbi),
                        com.posthog.hoglake.service.ScanService(db.jdbi),
                    ),
                    AlterService(db.jdbi),
                    ViewService(db.jdbi),
                    TableCreationService(db.jdbi, catalogs, CommitService(db.jdbi)),
                )
            }
        result.store.createCatalog(cat, "s3://hoglake-it/$cat")
        result.store.createNamespace(cat, "ns")
        result.store.createTable(cat, "ns", "t", listOf(ColumnDef("a", ColType.LONG)))
        return result
    }

    private class Capture(private val encoder: LogstashEncoder) : AppenderBase<ILoggingEvent>() {
        val lines = CopyOnWriteArrayList<String>()

        override fun append(event: ILoggingEvent) {
            lines += String(encoder.encode(event))
        }
    }

    private fun captured(
        batch: Int = 1,
        run: (MutableList<JsonNode>, () -> Unit) -> Unit,
    ) {
        val context = LoggerFactory.getILoggerFactory() as LoggerContext
        val encoder =
            LogstashEncoder().apply {
                this.context = context
                setIncludeMdc(false)
                start()
            }
        val capture =
            Capture(encoder).apply {
                this.context = context
                start()
            }
        val logger = LoggerFactory.getLogger(Audit.LOGGER_NAME) as Logger
        logger.addAppender(capture)
        val previous = MDC.get(Audit.REQUEST_ID_MDC)
        MDC.put(Audit.REQUEST_ID_MDC, "review-request")
        try {
            val events = mutableListOf<JsonNode>()
            var seen = 0
            run(events) {
                val fresh = capture.lines.drop(seen)
                assertThat(fresh).hasSize(batch)
                fresh.forEach { events.add(json.readTree(it)) }
                seen = capture.lines.size
            }
            assertThat(capture.lines).hasSize(events.size)
        } finally {
            if (previous == null) MDC.remove(Audit.REQUEST_ID_MDC) else MDC.put(Audit.REQUEST_ID_MDC, previous)
            logger.detachAppender(capture)
            capture.stop()
            encoder.stop()
        }
    }

    private fun event(
        events: List<JsonNode>,
        action: String,
        catalog: String,
        obj: String,
        outcome: String,
        actor: String? = author,
        msg: String? = message,
    ): JsonNode {
        val node = events.last()
        assertThat(node["action"].asText()).isEqualTo(action)
        assertThat(node["catalog"].asText()).isEqualTo(catalog)
        assertThat(node["object"].asText()).isEqualTo(obj)
        assertThat(node["outcome"].asText()).isEqualTo(outcome)
        assertThat(node["actor"].asText()).isEqualTo(actor ?: "anonymous")
        assertThat(node["request_id"].asText()).isEqualTo("review-request")
        if (msg == null) {
            assertThat(node.has("operation_message")).isFalse()
        } else {
            assertThat(node["operation_message"].asText()).isEqualTo(msg)
        }
        return node
    }

    @ParameterizedTest
    @EnumSource(Backend::class)
    fun `alter and view success and failures emit one attributed audit`(backend: Backend) {
        val f = fixture(backend)
        captured { events, next ->
            f.alter.alterTable(
                f.catalog,
                "ns",
                "t",
                listOf(
                    AlterOp.AddColumn(ColumnDef("b", ColType.LONG)),
                    AlterOp.AddColumn(ColumnDef("c", ColType.LONG)),
                    AlterOp.SetTableComment("audit"),
                ),
                author = author,
                message = message,
            )
            next()
            assertThat(event(events, "table_alter", f.catalog, "ns.t", "ok")["detail"].asText())
                .isEqualTo("ops=AddColumnx2,SetTableComment")
            assertThatThrownBy {
                f.alter.alterTable(f.catalog, "ns", "t", emptyList(), author = author, message = message)
            }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
            next()
            event(events, "table_alter", f.catalog, "ns.t", "validation")
            f.views.create(f.catalog, "ns", "v", "SELECT 1", "generic", author, message)
            next()
            assertThat(event(events, "view_create", f.catalog, "ns.v", "ok")["detail"].asText())
                .isEqualTo("dialect=generic")
            assertThatThrownBy {
                f.views.create(f.catalog, "ns", "v", "SELECT 1", "generic", author, message)
            }.isExactlyInstanceOf(HoglakeException.AlreadyExists::class.java)
            next()
            event(events, "view_create", f.catalog, "ns.v", "conflict")
            assertThatThrownBy { f.views.drop(f.catalog, "ns", "v", "", message) }
                .isExactlyInstanceOf(HoglakeException.Validation::class.java)
            next()
            event(events, "view_drop", f.catalog, "ns.v", "validation", "")
            val dropped = f.views.drop(f.catalog, "ns", "v", author, message)
            next()
            assertThat(event(events, "view_drop", f.catalog, "ns.v", "ok")["detail"].asText())
                .isEqualTo("snapshot=${dropped.snapshotId}")
            assertThatThrownBy { f.views.drop(f.catalog, "ns", "v", author, message) }
                .isExactlyInstanceOf(HoglakeException.NotFound::class.java)
            next()
            event(events, "view_drop", f.catalog, "ns.v", "not_found")
        }
    }

    @ParameterizedTest
    @EnumSource(Backend::class)
    fun `creation audits include replay stored attribution and operation failures`(backend: Backend) {
        val f = fixture(backend)
        val op = UUID.randomUUID()
        val definition =
            TableCreationDefinition(
                "ns",
                "new_table",
                listOf(ColumnDef("a", ColType.LONG)),
                author = author,
                message = message,
            )
        captured { events, next ->
            f.creations.prepare(f.catalog, op, definition)
            next()
            event(events, "table_creation_prepare", f.catalog, op.toString(), "prepared")
            f.creations.prepare(f.catalog, op, definition)
            next()
            event(events, "table_creation_prepare", f.catalog, op.toString(), "replayed")
            assertThatThrownBy { f.creations.prepare(f.catalog, op, definition.copy(name = "other")) }
                .isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
            next()
            event(events, "table_creation_prepare", f.catalog, op.toString(), "conflict")
            val published = f.creations.publish(f.catalog, op, emptyList())
            next()
            assertThat(
                event(events, "table_creation_publish", f.catalog, op.toString(), "committed")["detail"].asText(),
            )
                .isEqualTo("operation=$op state=committed snapshot=${published.snapshotId}")
            f.creations.status(f.catalog, op) // PG does not audit status reads.
            f.creations.publish(f.catalog, op, emptyList())
            next()
            event(events, "table_creation_publish", f.catalog, op.toString(), "replayed")
            assertThatThrownBy { f.creations.publish(f.catalog, op, listOf(FileRegistration("bad", 1, 100, 20))) }
                .isExactlyInstanceOf(HoglakeException.CommitConflict::class.java)
            next()
            event(events, "table_creation_publish", f.catalog, op.toString(), "conflict")
            f.creations.abort(f.catalog, op)
            next()
            event(events, "table_creation_abort", f.catalog, op.toString(), "replayed")
            val blocked = UUID.randomUUID()
            f.creations.prepare(f.catalog, blocked, definition.copy(name = "t"))
            next()
            event(events, "table_creation_prepare", f.catalog, blocked.toString(), "prepared")
            f.creations.publish(f.catalog, blocked, emptyList())
            next()
            event(events, "table_creation_publish", f.catalog, blocked.toString(), "rejected")
            f.creations.publish(f.catalog, blocked, emptyList())
            next()
            event(events, "table_creation_publish", f.catalog, blocked.toString(), "replayed")
            val other = UUID.randomUUID()
            f.creations.prepare(f.catalog, other, definition.copy(name = "aborted"))
            next()
            event(events, "table_creation_prepare", f.catalog, other.toString(), "prepared")
            f.creations.abort(f.catalog, other)
            next()
            event(events, "table_creation_abort", f.catalog, other.toString(), "aborted")
            f.creations.abort(f.catalog, other)
            next()
            event(events, "table_creation_abort", f.catalog, other.toString(), "replayed")
            val missing = UUID.randomUUID()
            assertThatThrownBy { f.creations.abort(f.catalog, missing) }
                .isExactlyInstanceOf(HoglakeException.NotFound::class.java)
            next()
            event(events, "table_creation_abort", f.catalog, missing.toString(), "not_found", null, null)
            assertThatThrownBy { f.creations.publish(f.catalog, missing, emptyList()) }
                .isExactlyInstanceOf(HoglakeException.NotFound::class.java)
            next()
            event(events, "table_creation_publish", f.catalog, missing.toString(), "not_found", null, null)
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["prepare", "publish", "reject", "abort", "guard_reject"])
    fun `receipt race winners are observed as replays`(mode: String) {
        val f = fixture(Backend.HOGLITE)
        val namespace = if (mode == "guard_reject") "guarded" else "ns"
        if (namespace != "ns") f.store.createNamespace(f.catalog, namespace)
        val op = UUID.randomUUID()
        val definition =
            TableCreationDefinition(
                namespace,
                if (mode == "reject") "t" else "raced",
                listOf(ColumnDef("a", ColType.LONG)),
                author = author,
                message = message,
            )
        if (mode != "prepare") f.creations.prepare(f.catalog, op, definition)
        val hook = AtomicReference<(() -> Unit)?>(null)
        val hookFailure = AtomicReference<Throwable?>(null)
        val hookTransitions = CopyOnWriteArrayList<List<String>>()
        val lazyExpiryBeforeHook = AtomicInteger()
        val win: () -> Unit = {
            when (mode) {
                "prepare" -> f.creations.prepare(f.catalog, op, definition)
                "abort" -> f.creations.abort(f.catalog, op)
                else -> f.creations.publish(f.catalog, op, emptyList())
            }
        }
        hook.set(
            if (mode != "guard_reject") {
                win
            } else {
                {
                    engine.store.catalogOperation(f.catalog) {
                        engine.store.runTx(
                            f.catalog,
                            HogliteCatalogStore.freshTxid("namespace-race"),
                            null,
                            HogliteTx().apply {
                                add(
                                    "UPDATE namespaces SET id = id + 10000 WHERE name = ?",
                                    namespace,
                                )
                            },
                        )
                    }
                    hook.set(win)
                }
            },
        )
        val http = HttpClient.newHttpClient()
        val proxy = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        proxy.createContext("/") { ex ->
            try {
                val bytes = ex.requestBody.readBytes()
                val statements = json.readTree(bytes)["statements"]?.map { it["sql"].asText() }.orEmpty()
                val transitions =
                    statements.filter { sql ->
                        sql.startsWith("INSERT INTO table_creations") ||
                            sql.startsWith("UPDATE table_creations SET files_json =") ||
                            (
                                sql.startsWith("UPDATE table_creations SET state = 'aborted'") &&
                                    "'client_abort'" in sql
                            )
                    }
                if (ex.requestURI.path.endsWith("/tx") && hook.get() != null &&
                    statements.any { sql ->
                        sql.startsWith("UPDATE table_creations SET state = 'aborted'") &&
                            "'client_abort'" !in sql
                    }
                ) {
                    lazyExpiryBeforeHook.incrementAndGet()
                }
                if (ex.requestURI.path.endsWith("/tx") && transitions.isNotEmpty()) {
                    try {
                        hook.getAndSet(null)?.let { pending ->
                            hookTransitions.add(transitions)
                            pending()
                        }
                    } catch (failure: Throwable) {
                        hookFailure.set(failure)
                        throw failure
                    }
                }
                val response =
                    http.send(
                        HttpRequest.newBuilder(URI.create(engine.url() + ex.requestURI))
                            .header("Content-Type", "application/json")
                            .method(ex.requestMethod, HttpRequest.BodyPublishers.ofByteArray(bytes)).build(),
                        HttpResponse.BodyHandlers.ofByteArray(),
                    )
                ex.sendResponseHeaders(response.statusCode(), response.body().size.toLong())
                ex.responseBody.use { it.write(response.body()) }
            } finally {
                ex.close()
            }
        }
        proxy.start()
        val registry = SimpleMeterRegistry()
        Metrics.bind(registry)
        try {
            HogliteClient("http://127.0.0.1:${proxy.address.port}").use { client ->
                val selected = HogliteCatalogStore(client, HogliteConformanceEngine.TENANTS)
                captured(batch = 2) { events, next ->
                    when (mode) {
                        "prepare" -> selected.tableCreations.prepare(f.catalog, op, definition)
                        "abort" -> selected.tableCreations.abort(f.catalog, op)
                        else -> selected.tableCreations.publish(f.catalog, op, emptyList())
                    }
                    next()
                    assertThat(hook.get() == null).isTrue()
                    assertThat(hookFailure.get()).isNull()
                    assertThat(hookTransitions).hasSize(if (mode == "guard_reject") 2 else 1)
                    if (mode == "abort") {
                        assertThat(lazyExpiryBeforeHook.get()).isPositive()
                        assertThat(hookTransitions.single()).hasSize(1)
                        val sql = hookTransitions.single().single()
                        assertThat(sql).startsWith("UPDATE table_creations SET state = 'aborted'")
                        assertThat(sql).contains("CASE", "'client_abort'")
                    }
                    val winner =
                        when (mode) {
                            "prepare" -> "prepared"
                            "abort" -> "aborted"
                            "reject", "guard_reject" -> "rejected"
                            else -> "committed"
                        }
                    assertThat(events.first()["outcome"].asText()).isEqualTo(winner)
                    val action = if (mode in setOf("reject", "guard_reject")) "publish" else mode
                    event(events, "table_creation_$action", f.catalog, op.toString(), "replayed")
                    assertThat(
                        registry.find("hoglake_table_creation_total").tags(
                            "catalog",
                            f.catalog,
                            "action",
                            action,
                            "outcome",
                            "replayed",
                        ).counter()?.count(),
                    ).isEqualTo(1.0)
                    assertThat(
                        registry.find("hoglake_commits_total").tags(
                            "catalog", f.catalog, "result", "committed",
                        ).counter()?.count() ?: 0.0,
                    ).isEqualTo(if (mode == "publish") 1.0 else 0.0)
                }
            }
        } finally {
            Metrics.clear()
            registry.close()
            proxy.stop(0)
        }
    }
}
