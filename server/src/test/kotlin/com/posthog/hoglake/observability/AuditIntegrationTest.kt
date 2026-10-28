package com.posthog.hoglake.observability

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.commit.CommitService
import com.posthog.hoglake.model.AlterOp
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import com.posthog.hoglake.service.AlterService
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.service.OptionsService
import com.posthog.hoglake.service.PatchField
import com.posthog.hoglake.service.TableCreationDefinition
import com.posthog.hoglake.service.TableCreationService
import com.posthog.hoglake.service.ViewService
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.client.HttpClient
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import net.logstash.logback.encoder.LogstashEncoder
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.kotlin.useHandleUnchecked
import org.jdbi.v3.core.kotlin.withHandleUnchecked
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList

/**
 * The audit log end to end: events on the dedicated "hoglake.audit"
 * logger are captured through the real LogstashEncoder (so the
 * assertion is on actual JSON lines, exactly what stdout would carry)
 * and checked for the structured fields — plus the transaction-boundary
 * rule: a failed commit emits its failure outcome and never a
 * 'committed' line.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AuditIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val app = App.build(Config(hydratorIntervalMs = 0, metricsIntervalMs = 0), db.jdbi)
    private val json = ObjectMapper()

    /** Encodes each event to a JSON line at append time (MDC still live). */
    private class CapturingAppender(private val encoder: LogstashEncoder) :
        AppenderBase<ILoggingEvent>() {
        val lines = CopyOnWriteArrayList<String>()

        override fun append(event: ILoggingEvent) {
            lines += String(encoder.encode(event))
        }
    }

    private lateinit var capture: CapturingAppender

    @BeforeEach
    fun attachAppender() {
        val ctx = LoggerFactory.getILoggerFactory() as LoggerContext
        val encoder =
            LogstashEncoder().apply {
                context = ctx
                // Mirror logback.xml: request_id comes from the structured
                // argument, not MDC (which would duplicate the JSON key).
                setIncludeMdc(false)
                start()
            }
        capture =
            CapturingAppender(encoder).apply {
                context = ctx
                start()
            }
        (LoggerFactory.getLogger(Audit.LOGGER_NAME) as Logger).addAppender(capture)
    }

    @AfterEach
    fun detachAppender() {
        (LoggerFactory.getLogger(Audit.LOGGER_NAME) as Logger).detachAppender(capture)
        capture.stop()
    }

    @Test
    fun `atomic publication audits outcomes after transactions and counts publication once`() {
        val catalogs = com.posthog.hoglake.service.CatalogService(db.jdbi)
        val creations =
            com.posthog.hoglake.service.TableCreationService(
                db.jdbi,
                catalogs,
                com.posthog.hoglake.commit.CommitService(db.jdbi),
            )
        val catalog = "atomic-audit"
        catalogs.createCatalog(catalog, "s3://bucket/audit")
        catalogs.createNamespace(catalog, "test")
        val definition =
            com.posthog.hoglake.service.TableCreationDefinition(
                "test",
                "target",
                listOf(com.posthog.hoglake.model.ColumnDef("id", com.posthog.hoglake.model.ColType.LONG)),
            )
        val registry = io.micrometer.core.instrument.simple.SimpleMeterRegistry()
        Metrics.bind(registry)
        try {
            val id = java.util.UUID.randomUUID()
            creations.prepare(catalog, id, definition)
            creations.publish(catalog, id, emptyList())
            creations.publish(catalog, id, emptyList())
            creations.abort(catalog, id)
            val rejected = java.util.UUID.randomUUID()
            creations.prepare(catalog, rejected, definition)
            creations.publish(catalog, rejected, emptyList())
            val aborted = java.util.UUID.randomUUID()
            creations.prepare(catalog, aborted, definition)
            creations.abort(catalog, aborted)
            org.assertj.core.api.Assertions.assertThatThrownBy {
                creations.publish(catalog, id, listOf(com.posthog.hoglake.model.FileRegistration("bad", 1, 100, 20)))
            }.isInstanceOf(com.posthog.hoglake.model.HoglakeException.CommitConflict::class.java)
            assertThat(eventsFor("table_creation_publish").map { it["outcome"].asText() })
                .containsExactly("committed", "replayed", "rejected", "conflict")
            assertThat(eventsFor("table_creation_abort").map { it["outcome"].asText() })
                .containsExactly("replayed", "aborted")
            assertThat(eventsFor("table_creation_publish").first()["detail"].asText())
                .contains("operation=$id", "snapshot=")
            assertThat(registry.get("hoglake_commits_total").tag("catalog", catalog).counter().count()).isEqualTo(1.0)
            assertThat(catalogs.listTables(catalog, "test")).hasSize(1)
        } finally {
            Metrics.clear()
            registry.close()
        }
    }

    private fun events(): List<JsonNode> = capture.lines.map { json.readTree(it) }

    @Test
    fun `an idempotent no-op create audits as exists, not ok`() {
        val catalogs = CatalogService(db.jdbi)
        val catalog = "audit-idem"
        catalogs.createCatalog(catalog, "s3://bucket/$catalog")
        catalogs.createNamespace(catalog, "ns")
        catalogs.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        // The no-op retries: identical definitions, nothing created.
        catalogs.createCatalogOutcome(catalog, "s3://bucket/$catalog")
        catalogs.createNamespaceOutcome(catalog, "ns")
        catalogs.createTableOutcome(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        // One "ok" (the real create) and one "exists" (the no-op) each —
        // the trail must not read as two creations of the same object.
        for (action in listOf("catalog_create", "namespace_create", "table_create")) {
            assertThat(
                eventsFor(action)
                    .filter { it["catalog"].asText() == catalog }
                    .map { it["outcome"].asText() },
            ).containsExactly("ok", "exists")
        }
    }

    private fun eventsFor(action: String): List<JsonNode> = events().filter { it["action"]?.asText() == action }

    private fun assertAttribution(
        line: JsonNode,
        author: String?,
        message: String?,
        outcome: String,
    ) {
        assertThat(line["actor"].asText()).isEqualTo(author ?: "anonymous")
        assertThat(line["operation_message"]?.asText()).isEqualTo(message)
        assertThat(line["outcome"].asText()).isEqualTo(outcome)
        assertThat(line["message"].asText()).isEqualTo("${line["action"].asText()} $outcome")
    }

    private fun assertSnapshotAttribution(
        catalog: String,
        snapshot: Long,
        author: String,
        message: String,
    ) {
        db.jdbi.withHandleUnchecked { h ->
            val row =
                h.createQuery(
                    """
                    SELECT s.author, s.commit_message FROM hog_snapshot s
                    JOIN hog_catalog c ON c.catalog_id = s.catalog_id
                    WHERE c.name = :catalog AND s.snapshot_id = :snapshot
                    """,
                ).bind("catalog", catalog).bind("snapshot", snapshot).mapToMap().one()
            assertThat(row["author"]).isEqualTo(author)
            assertThat(row["commit_message"]).isEqualTo(message)
        }
    }

    @Test
    fun `each DDL snapshot and audit preserve exact caller attribution`() {
        val catalog = "ddl-audit-metadata"
        val catalogs = CatalogService(db.jdbi)
        val views = ViewService(db.jdbi)
        val author = " engineer/λ \t "
        val message = " explicit reason\n\"quoted\" & preserved "

        fun verify(
            action: String,
            snapshot: Long,
        ) {
            assertSnapshotAttribution(catalog, snapshot, author, message)
            val line = eventsFor(action).single { it["catalog"].asText() == catalog }
            assertAttribution(line, author, message, "ok")
            assertThat(catalogs.getCatalog(catalog).headSnapshotId).isEqualTo(snapshot)
        }

        catalogs.createCatalog(catalog, "s3://bucket/$catalog", author, message)
        verify("catalog_create", 0)
        catalogs.createNamespace(catalog, "ns", author, message)
        verify("namespace_create", 1)
        val table = catalogs.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)), author, message)
        verify("table_create", 2)
        AlterService(db.jdbi).alterTable(
            catalog,
            "ns",
            "t",
            listOf(AlterOp.SetTableComment("updated")),
            author = author,
            message = message,
        )
        verify("table_alter", 3)
        views.create(catalog, "ns", "v", "SELECT id FROM t", author = author, message = message)
        verify("view_create", 4)
        catalogs.truncateTable(catalog, "ns", "t", table.tableUuid, author, message)
        verify("table_truncate", 5)
        views.drop(catalog, "ns", "v", author, message)
        verify("view_drop", 6)
        catalogs.dropTable(catalog, "ns", "t", author = author, message = message)
        verify("table_drop", 7)
        catalogs.dropNamespace(catalog, "ns", author = author, message = message)
        verify("namespace_drop", 8)
        val options = OptionsService(db.jdbi).patch(catalog, PatchField.Set(60L), false, author, message)
        assertThat(options.snapshotRetentionSeconds).isEqualTo(60L)
        assertThat(options.consumerFloor).isFalse()
        assertThat(catalogs.getCatalog(catalog).headSnapshotId).isEqualTo(8)
        assertAttribution(eventsFor("options_patch").single(), author, message, "ok")
    }

    @Test
    fun `blank destructive attribution emits one validation audit and leaves state unchanged`() {
        val catalog = "ddl-audit-refusal"
        val catalogs = CatalogService(db.jdbi)
        val views = ViewService(db.jdbi)
        catalogs.createCatalog(catalog, "s3://bucket/$catalog")
        val namespace = catalogs.createNamespace(catalog, "ns")
        val table = catalogs.createTable(catalog, "ns", "t", listOf(ColumnDef("id", ColType.LONG)))
        val view = views.create(catalog, "ns", "v", "SELECT id FROM t")
        CommitService(db.jdbi).commit(
            catalog,
            CommitRequest(
                appends =
                    listOf(
                        TableAppend("ns", "t", listOf(FileRegistration("s3://bucket/$catalog/a.parquet", 1, 100))),
                    ),
            ),
        )
        val head = catalogs.getCatalog(catalog).headSnapshotId
        val files = catalogs.listFiles(catalog, "ns", "t")
        for (field in listOf("author", "message")) {
            for (blank in listOf(null, "", " \t\n", "\u2003\u00a0")) {
                val author = if (field == "author") blank else "engineer"
                val message = if (field == "message") blank else "reason"
                val attempts =
                    listOf<Pair<String, () -> Unit>>(
                        "namespace_drop" to {
                            catalogs.dropNamespace(catalog, "ns", author = author, message = message)
                        },
                        "table_drop" to { catalogs.dropTable(catalog, "ns", "t", author = author, message = message) },
                        "table_truncate" to {
                            catalogs.truncateTable(catalog, "ns", "t", table.tableUuid, author, message)
                        },
                        "view_drop" to { views.drop(catalog, "ns", "v", author, message) },
                    )
                for ((action, attempt) in attempts) {
                    capture.lines.clear()
                    assertThatThrownBy { attempt() }
                        .isInstanceOf(HoglakeException.Validation::class.java).hasMessageContaining(field)
                    assertThat(events()).hasSize(1)
                    assertAttribution(eventsFor(action).single(), author, message, "validation")
                    assertThat(catalogs.getCatalog(catalog).headSnapshotId).isEqualTo(head)
                    assertThat(catalogs.getNamespace(catalog, "ns")).isEqualTo(namespace)
                    assertThat(catalogs.getTable(catalog, "ns", "t").tableUuid).isEqualTo(table.tableUuid)
                    assertThat(catalogs.listFiles(catalog, "ns", "t")).isEqualTo(files)
                    assertThat(views.get(catalog, "ns", "v")).isEqualTo(view)
                }
            }
        }
    }

    @Test
    fun `atomic prepare failure commit and replay audit the stored attribution`() {
        val catalog = "atomic-audit-attribution"
        val catalogs = CatalogService(db.jdbi)
        catalogs.createCatalog(catalog, "s3://bucket/$catalog")
        catalogs.createNamespace(catalog, "ns")
        val definition =
            TableCreationDefinition(
                "ns",
                "t",
                listOf(ColumnDef("id", ColType.LONG)),
                author = " engineer/λ \t ",
                message = " exact reason\n\"quoted\" ",
            )
        val operation = UUID.randomUUID()
        TableCreationService(db.jdbi, catalogs, CommitService(db.jdbi)).prepare(catalog, operation, definition)
        assertAttribution(
            eventsFor("table_creation_prepare").single(),
            definition.author,
            definition.message,
            "prepared",
        )
        val creations = TableCreationService(db.jdbi, CatalogService(db.jdbi), CommitService(db.jdbi))
        val head = catalogs.getCatalog(catalog).headSnapshotId
        assertThatThrownBy {
            creations.publish(catalog, operation, listOf(FileRegistration("s3://outside/a.parquet", 1, 100, 20)))
        }.isInstanceOf(HoglakeException.Validation::class.java)
        assertAttribution(
            eventsFor("table_creation_publish").single(),
            definition.author,
            definition.message,
            "validation",
        )
        assertThat(creations.status(catalog, operation).state).isEqualTo("prepared")
        assertThat(catalogs.getCatalog(catalog).headSnapshotId).isEqualTo(head)
        assertThat(catalogs.listTables(catalog, "ns")).isEmpty()
        val committed = creations.publish(catalog, operation, emptyList())
        assertThat(creations.publish(catalog, operation, emptyList())).isEqualTo(committed)
        val lines = eventsFor("table_creation_publish")
        assertThat(lines.map { it["outcome"].asText() }).containsExactly("validation", "committed", "replayed")
        lines.forEach { assertAttribution(it, definition.author, definition.message, it["outcome"].asText()) }
        assertThat(committed.snapshotId).isEqualTo(head + 1)
        assertThat(catalogs.getCatalog(catalog).headSnapshotId).isEqualTo(committed.snapshotId)
        assertSnapshotAttribution(catalog, committed.snapshotId!!, definition.author!!, definition.message!!)
    }

    private fun api(block: suspend ApplicationTestBuilder.(HttpClient) -> Unit) =
        testApplication {
            application { app.module(this) }
            block(client)
        }

    private suspend fun HttpClient.postJson(
        url: String,
        body: String,
        requestId: String? = null,
    ): HttpResponse =
        post(url) {
            contentType(ContentType.Application.Json)
            requestId?.let { header("X-Request-Id", it) }
            setBody(body)
        }

    private suspend fun seed(
        client: HttpClient,
        catalog: String,
    ) {
        assertThat(
            client.postJson(
                "/v1/catalogs",
                """{"name": "$catalog", "data_path": "s3://hog/$catalog"}""",
            ).status,
        ).isEqualTo(HttpStatusCode.Created)
        assertThat(
            client.postJson("/v1/catalogs/$catalog/namespaces", """{"name": "ns"}""").status,
        ).isEqualTo(HttpStatusCode.Created)
    }

    @Test
    fun `create-table, commit, conflict, and expiry all emit structured audit lines`() =
        api { client ->
            val catalog = "audited"
            seed(client, catalog)

            // -- create table (with a caller-supplied request id) --------------
            val create =
                client.postJson(
                    "/v1/catalogs/$catalog/namespaces/ns/tables",
                    """{"name": "events", "columns": [{"name": "id", "type": "long"}]}""",
                    requestId = "req-42",
                )
            assertThat(create.status).isEqualTo(HttpStatusCode.Created)
            val tableCreate = eventsFor("table_create").single { it["catalog"].asText() == catalog }
            assertThat(tableCreate["outcome"].asText()).isEqualTo("ok")
            assertThat(tableCreate["object"].asText()).isEqualTo("ns.events")
            assertThat(tableCreate["actor"].asText()).isEqualTo("anonymous")
            assertThat(tableCreate["request_id"].asText()).isEqualTo("req-42")

            // -- successful commit --------------------------------------------
            val commit =
                client.postJson(
                    "/v1/catalogs/$catalog/commit",
                    """
            {"appends": [{"namespace": "ns", "table": "events",
                          "files": [{"path": "s3://hog/$catalog/a.parquet",
                                     "record_count": 5, "file_size_bytes": 100}]}]}
            """,
                )
            assertThat(commit.status).isEqualTo(HttpStatusCode.OK)
            val committed =
                eventsFor("commit").single {
                    it["catalog"].asText() == catalog && it["outcome"].asText() == "committed"
                }
            assertThat(committed["detail"].asText()).contains("snapshot=3", "files=1", "deletes=0")
            // Generated request id (no header sent) still lands on the line.
            assertThat(committed["request_id"].asText()).isNotBlank()

            // -- conflicting commit (DDL after its read snapshot) --------------
            val alter =
                client.postJson(
                    "/v1/catalogs/$catalog/namespaces/ns/tables/events/alter",
                    """{"ops": [{"op": "set_sort_order", "sort_fields": []}]}""",
                )
            assertThat(alter.status).isEqualTo(HttpStatusCode.OK)
            val conflicted =
                client.postJson(
                    "/v1/catalogs/$catalog/commit",
                    """
            {"read_snapshot": 3,
             "appends": [{"namespace": "ns", "table": "events",
                          "files": [{"path": "s3://hog/$catalog/b.parquet",
                                     "record_count": 5, "file_size_bytes": 100}]}]}
            """,
                )
            assertThat(conflicted.status).isEqualTo(HttpStatusCode.Conflict)
            // The outcome names the CODE, not a generic `conflict`: an
            // audit line saying `conflict` means "retry", and this one
            // means "re-prepare". Whoever reads the trail after a stuck
            // writer needs to see which.
            val conflictLine =
                eventsFor("commit").single {
                    it["catalog"].asText() == catalog &&
                        it["outcome"].asText() == "ddl_since_read_snapshot"
                }
            assertThat(conflictLine["detail"].asText()).contains("concurrent DDL")

            // -- expiry runs ---------------------------------------------------
            // Zero-work sweep: NO audit event (app-log debug only) — the
            // every-minute background no-op must not flood the stream.
            val idleExpire = client.post("/v1/catalogs/$catalog/maintenance/expire")
            assertThat(idleExpire.status).isEqualTo(HttpStatusCode.OK)
            assertThat(eventsFor("expiry").filter { it["catalog"].asText() == catalog }).isEmpty()

            // A sweep that actually expires something still audits.
            db.jdbi.useHandleUnchecked { h ->
                h.execute(
                    "UPDATE hog_snapshot SET snapshot_time = now() - make_interval(secs => 3600) " +
                        "WHERE catalog_id = (SELECT catalog_id FROM hog_catalog WHERE name = ?)",
                    catalog,
                )
                h.execute(
                    "UPDATE hog_catalog SET snapshot_retention_seconds = 60 WHERE name = ?",
                    catalog,
                )
            }
            val expire = client.post("/v1/catalogs/$catalog/maintenance/expire")
            assertThat(expire.status).isEqualTo(HttpStatusCode.OK)
            val expiry = eventsFor("expiry").single { it["catalog"].asText() == catalog }
            assertThat(expiry["outcome"].asText()).isEqualTo("ok")
            assertThat(expiry["detail"].asText()).doesNotContain("snapshots_expired=0")

            // Every captured line already proved JSON-parseable via events();
            // spot-check the audit lines all carry the core fields.
            events().forEach { line ->
                assertThat(line["action"].asText()).isNotBlank()
                assertThat(line["outcome"].asText()).isNotBlank()
                assertThat(line["actor"].asText()).isEqualTo("anonymous")
            }
        }

    @Test
    fun `an unexpected mid-commit failure emits outcome=error and maps to 500`() =
        api { client ->
            val catalog = "haywire"
            seed(client, catalog)
            assertThat(
                client.postJson(
                    "/v1/catalogs/$catalog/namespaces/ns/tables",
                    """{"name": "events", "columns": [{"name": "id", "type": "long"}]}""",
                ).status,
            ).isEqualTo(HttpStatusCode.Created)
            // Sabotage: delete the hog_table_stats row so the commit tail
            // hits its IllegalStateException — a genuine unexpected
            // (non-Hoglake) failure mid-transaction.
            db.jdbi.useHandleUnchecked { h ->
                h.execute(
                    "DELETE FROM hog_table_stats WHERE catalog_id = " +
                        "(SELECT catalog_id FROM hog_catalog WHERE name = ?)",
                    catalog,
                )
            }
            val boom =
                client.postJson(
                    "/v1/catalogs/$catalog/commit",
                    """
                {"appends": [{"namespace": "ns", "table": "events",
                              "files": [{"path": "s3://hog/$catalog/x.parquet",
                                         "record_count": 1, "file_size_bytes": 1}]}]}
                """,
                )
            assertThat(boom.status).isEqualTo(HttpStatusCode.InternalServerError)
            val line =
                eventsFor("commit").single { it["catalog"].asText() == catalog }
            assertThat(line["outcome"].asText()).isEqualTo("error")
            assertThat(line["detail"].asText()).contains("hog_table_stats")
        }

    @Test
    fun `a request id outside the allowlist is regenerated, a tame one passes through`() =
        api { client ->
            // Tab-bearing header: hostile for structured logs; the plugin
            // regenerates instead of echoing it.
            val hostile =
                client.postJson(
                    "/v1/catalogs",
                    """{"name":"rid","data_path":"s3://x"}""",
                    requestId = "evil\tid",
                )
            val echoed = hostile.headers["X-Request-Id"]
            assertThat(echoed).isNotNull().isNotEqualTo("evil\tid")
            assertThat(echoed).matches("[0-9a-f-]{36}") // generated UUID shape
            // Tame id: passthrough (already covered above with req-42, but
            // pin dots/underscores/hyphens explicitly).
            val tame =
                client.postJson(
                    "/v1/catalogs",
                    """{"name":"rid2","data_path":"s3://x"}""",
                    requestId = "job-1.retry_2",
                )
            assertThat(tame.headers["X-Request-Id"]).isEqualTo("job-1.retry_2")
        }

    @Test
    fun `a validation-failed commit emits outcome=validation and never a committed line`() =
        api { client ->
            val catalog = "rollback"
            seed(client, catalog)
            assertThat(
                client.postJson(
                    "/v1/catalogs/$catalog/namespaces/ns/tables",
                    """{"name": "events", "columns": [{"name": "id", "type": "long"}]}""",
                ).status,
            ).isEqualTo(HttpStatusCode.Created)

            val headBefore =
                json.readTree(
                    client.get("/v1/catalogs/$catalog").bodyAsText(),
                )["head_snapshot_id"].asLong()

            // Commit that fails validation INSIDE the transaction (unknown
            // table): the rollback must leave exactly one audit line, the
            // validation outcome — no 'committed' line ever appears because
            // the event is only emitted after the transaction concludes.
            val invalid =
                client.postJson(
                    "/v1/catalogs/$catalog/commit",
                    """
                {"appends": [{"namespace": "ns", "table": "nope",
                              "files": [{"path": "s3://hog/rollback/x.parquet", "record_count": 1,
                                         "file_size_bytes": 1}]}]}
                """,
                )
            assertThat(invalid.status).isEqualTo(HttpStatusCode.UnprocessableEntity)

            val commitLines = eventsFor("commit").filter { it["catalog"].asText() == catalog }
            assertThat(commitLines).hasSize(1)
            assertThat(commitLines.single()["outcome"].asText()).isEqualTo("validation")
            assertThat(commitLines.single()["detail"].asText()).contains("unknown table")

            // And the catalog head never moved: nothing was committed for
            // the line to describe.
            val headAfter =
                json.readTree(
                    client.get("/v1/catalogs/$catalog").bodyAsText(),
                )["head_snapshot_id"].asLong()
            assertThat(headAfter).isEqualTo(headBefore)
        }
}
