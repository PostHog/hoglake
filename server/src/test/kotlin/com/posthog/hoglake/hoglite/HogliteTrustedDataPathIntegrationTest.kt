package com.posthog.hoglake.hoglite

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.AppenderBase
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.hydrator.ObjectStore
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.service.RemovalStore
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import net.logstash.logback.encoder.LogstashEncoder
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.sync.RequestBody
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList

/**
 * The trusted data_path on the live engine (#668): the tenant create
 * carries it, every tenant answer repeats it, and hoglake acts on THAT
 * copy — a `meta.data_path` row rewritten with raw SQL steers nothing,
 * a removal row naming another prefix deletes nothing, and a tenant
 * created without one (a legacy tenant) is readable but refuses every
 * storage-acting path.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteTrustedDataPathIntegrationTest {
    private lateinit var engine: HogliteConformanceEngine
    private val json = jacksonObjectMapper()
    private val http = HttpClient.newHttpClient()
    private val meters = SimpleMeterRegistry()

    private val store get() = engine.store
    private val client get() = engine.client

    @BeforeAll
    fun start() {
        engine = HogliteConformanceEngine()
    }

    @AfterAll
    fun stop() {
        if (::engine.isInitialized) engine.close()
    }

    @BeforeEach
    fun bind() = Metrics.bind(meters)

    @AfterEach
    fun unbind() = Metrics.clear()

    private fun tenantOf(cat: String) = HogliteConformanceEngine.TENANTS.tenantFor(cat)

    private fun dataPathOf(cat: String) = "s3://${HogliteConformanceEngine.DATA_BUCKET}/$cat"

    /** A raw tenant write, as a tenant writer with raw SQL would make it. */
    private fun rawWrite(
        cat: String,
        sql: String,
        vararg args: Any?,
    ) = client.tx(tenantOf(cat), "raw:${UUID.randomUUID()}", null, listOf(HogliteStatement(sql, args.toList())))

    private fun rawRead(
        cat: String,
        sql: String,
    ) = client.query(tenantOf(cat), sql)

    private fun status(cat: String): HttpResponse<String> =
        http.send(
            HttpRequest.newBuilder(URI.create("${engine.url()}/v1/t/${tenantOf(cat)}/status")).GET().build(),
            HttpResponse.BodyHandlers.ofString(),
        )

    private fun counter(
        name: String,
        vararg tags: String,
    ): Double = meters.find(name).tags(*tags).counter()?.count() ?: 0.0

    private class Capture(private val encoder: LogstashEncoder) : AppenderBase<ILoggingEvent>() {
        val lines = CopyOnWriteArrayList<String>()

        override fun append(event: ILoggingEvent) {
            lines += String(encoder.encode(event))
        }
    }

    /** Run [body] with the audit stream captured; returns the events it emitted. */
    private fun audited(body: () -> Unit): List<JsonNode> {
        val context = LoggerFactory.getILoggerFactory() as LoggerContext
        val encoder =
            LogstashEncoder().apply {
                this.context = context
                start()
            }
        val capture =
            Capture(encoder).apply {
                this.context = context
                start()
            }
        val logger = LoggerFactory.getLogger(Audit.LOGGER_NAME) as Logger
        logger.addAppender(capture)
        try {
            body()
        } finally {
            logger.detachAppender(capture)
            capture.stop()
            encoder.stop()
        }
        return capture.lines.map { json.readTree(it) }
    }

    @Test
    fun `the create stores the data_path in the manifest and every tenant answer repeats it`() {
        val cat = HogliteConformanceEngine.uniqueName("trusted")
        val created = store.createCatalog(cat, dataPathOf(cat), "test", "create")
        assertThat(created.dataPath).isEqualTo(dataPathOf(cat))

        // The engine's own view of the tenant: the manifest holds it.
        val status = status(cat)
        assertThat(status.statusCode()).isEqualTo(200)
        assertThat(status.headers().allValues(HogliteClient.DATA_PATH_HEADER)).containsExactly(dataPathOf(cat))
        assertThat(json.readTree(status.body())["manifest"]["data_path"].asText()).isEqualTo(dataPathOf(cat))

        // And every read and write answer carries it.
        assertThat(rawRead(cat, "SELECT 1").dataPath).isEqualTo(dataPathOf(cat))
        assertThat(rawWrite(cat, "UPDATE meta SET snapshot_retention_seconds = NULL WHERE id = 1").single().dataPath)
            .isEqualTo(dataPathOf(cat))
        assertThat(store.listCatalogs(setOf(cat)).single().dataPath).isEqualTo(dataPathOf(cat))
    }

    @Test
    fun `a create naming another data_path for an existing tenant is AlreadyExists and changes nothing`() {
        val cat = HogliteConformanceEngine.uniqueName("immutable")
        store.createCatalog(cat, dataPathOf(cat), null, null)

        store.createNamespace(cat, "kept", null, null)

        assertThatThrownBy { store.createCatalog(cat, "${dataPathOf(cat)}-moved", null, null) }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)
            .hasMessageContaining("set at create")
        // The same path is the ordinary duplicate. On one pod hoglite's
        // answer to it is byte-for-byte a fresh create's (no `existing`:
        // the pod holds the tenant), so "already exists" comes from the
        // tenant's own schema, and the live catalog is left exactly as it was.
        assertThatThrownBy { store.createCatalog(cat, dataPathOf(cat), null, null) }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)
            .hasMessage("catalog '$cat' already exists")
        assertThat(store.getCatalog(cat).dataPath).isEqualTo(dataPathOf(cat))
        assertThat(store.listNamespaces(cat, null, null).map { it.name }).containsExactly("kept")
        store.dropNamespace(cat, "kept", author = "test", message = "drop")
        assertThat(status(cat).headers().firstValue(HogliteClient.DATA_PATH_HEADER).get()).isEqualTo(dataPathOf(cat))

        // Dropped, the name and its prefix are free again, with a new path.
        store.dropCatalog(cat, null, null)
        assertThat(store.createCatalog(cat, "${dataPathOf(cat)}-moved", null, null).dataPath)
            .isEqualTo("${dataPathOf(cat)}-moved")
    }

    @Test
    fun `a rewritten meta row steers nothing because reads, the drop and cleanup act on the manifest's path`() {
        val cat = HogliteConformanceEngine.uniqueName("tamper")
        val victim = HogliteConformanceEngine.uniqueName("victim")
        store.createCatalog(cat, dataPathOf(cat), null, null)
        // A tenant writer with raw SQL points the catalog at another prefix
        // and queues a removal under it.
        rawWrite(cat, "UPDATE meta SET data_path = ? WHERE id = 1", dataPathOf(victim))
        val victimObject = "${dataPathOf(victim)}/data/keep.parquet"
        val ownObject = "${dataPathOf(cat)}/data/gone.parquet"
        engine.s3Client().use { s3 ->
            for (path in listOf(victimObject, ownObject)) {
                val loc = ObjectStore.parse(path)
                s3.putObject({ it.bucket(loc.bucket).key(loc.key) }, RequestBody.fromString("x"))
            }
        }
        for (path in listOf(victimObject, ownObject)) {
            rawWrite(
                cat,
                "INSERT INTO removals(path, file_kind, reason, queued_snapshot, queued_at, scheduled_at) " +
                    "VALUES (?, 'data', 'snapshot_expiry', 0, ?, ?)",
                path,
                HogliteSchema.now(),
                HogliteSchema.fmt(Instant.now().minusSeconds(86400)),
            )
        }

        val events =
            audited {
                // Reads answer with the manifest's path and audit the row.
                assertThat(store.getCatalog(cat).dataPath).isEqualTo(dataPathOf(cat))
            }
        assertThat(events.map { it["action"].asText() to it["outcome"].asText() })
            .containsExactly("data_path_mismatch" to "invariant_violation")
        assertThat(events.single()["detail"].asText()).contains(dataPathOf(victim)).contains(dataPathOf(cat))
        assertThat(counter("hoglake_hoglite_data_path_mismatch_total", "catalog", cat)).isEqualTo(1.0)

        // Cleanup deletes the catalog's own object and leaves the victim's
        // row queued, counted, with its object untouched.
        val result =
            RemovalStore(engine.config()).use { objects ->
                HogliteCleanup(engine.config(), store, objects).runOnce(cat, 10, MaintenanceTrigger.MANUAL)
            }
        assertThat(result.removed).isEqualTo(1)
        assertThat(result.objectsRemoved).isEqualTo(1)
        engine.s3Client().use { s3 ->
            assertThat(
                s3.listObjectsV2 { it.bucket(HogliteConformanceEngine.DATA_BUCKET).prefix("$victim/") }.contents(),
            )
                .hasSize(1)
            assertThat(s3.listObjectsV2 { it.bucket(HogliteConformanceEngine.DATA_BUCKET).prefix("$cat/") }.contents())
                .isEmpty()
        }
        assertThat(rawRead(cat, "SELECT path, attempts FROM removals WHERE drained_at IS NULL").rows)
            .containsExactly(listOf(victimObject, 1))
        assertThat(counter("hoglake_path_outside_data_path_total", "catalog", cat, "surface", "removal_store"))
            .isEqualTo(1.0)

        // The drop reports the manifest's prefix, not the row's.
        rawWrite(cat, "DELETE FROM removals")
        assertThat(store.dropCatalog(cat, null, null).dataPath).isEqualTo(dataPathOf(cat))
    }

    @Test
    fun `a file row outside the prefix is dropped from scan, listing and changefeed answers`() {
        val cat = HogliteConformanceEngine.uniqueName("scanfilter")
        store.createCatalog(cat, dataPathOf(cat), null, null)
        store.createNamespace(cat, "ns", null, null)
        store.createTable(
            cat,
            "ns",
            "t",
            listOf(com.posthog.hoglake.model.ColumnDef("id", com.posthog.hoglake.model.ColType.LONG)),
        )
        // The table landed at snapshot 1, the head; rows begun there are visible.
        val tableId = rawRead(cat, "SELECT id FROM tables").firstRow()!![0].asLong()
        val inside = "${dataPathOf(cat)}/data/ns/t/in.parquet"
        val outside = "s3://${HogliteConformanceEngine.DATA_BUCKET}/elsewhere/out.parquet"
        for ((i, path) in listOf(inside, outside).withIndex()) {
            rawWrite(
                cat,
                "INSERT INTO files(table_id, path, file_format, record_count, file_size_bytes, row_id_start, " +
                    "explicit_row_ids, stats_state, begin_snapshot) " +
                    "VALUES (?, ?, 'parquet', 1, 1, ?, 0, 'provided', 1)",
                tableId,
                path,
                i.toLong(),
            )
        }

        val plan = store.planScan(cat, "ns", "t", null, null, null, false)
        assertThat(plan.map { it.dataFile.path }).containsExactly(inside)
        assertThat(store.listFiles(cat, "ns", "t", null, null, null, false, null, 0, emptyMap()).map { it.path })
            .containsExactly(inside)
        assertThat(store.changes(cat, "ns", "t", 0, null).files.map { it.path }).containsExactly(inside)
        assertThat(counter("hoglake_path_outside_data_path_total", "catalog", cat, "surface", "scan")).isEqualTo(1.0)
        assertThat(counter("hoglake_path_outside_data_path_total", "catalog", cat, "surface", "files")).isEqualTo(1.0)
        assertThat(counter("hoglake_path_outside_data_path_total", "catalog", cat, "surface", "changes")).isEqualTo(1.0)
    }

    @Test
    fun `compaction planning skips a file outside the prefix instead of failing its group every run`() {
        val cat = HogliteConformanceEngine.uniqueName("compactfilter")
        store.createCatalog(cat, dataPathOf(cat), null, null)
        store.createNamespace(cat, "ns", null, null)
        store.createTable(
            cat,
            "ns",
            "t",
            listOf(com.posthog.hoglake.model.ColumnDef("id", com.posthog.hoglake.model.ColType.LONG)),
        )
        val tableId = rawRead(cat, "SELECT id FROM tables").firstRow()!![0].asLong()
        // One small file inside the prefix and one outside: with the outside
        // one skipped no group reaches the minimum of two, so the sweep
        // plans nothing and opens nothing. Without the skip the pair would
        // be a group whose open the bounded store refuses, a failed group
        // on every run. Neither object exists; nothing here reads one.
        val inside = "${dataPathOf(cat)}/data/ns/t/in.parquet"
        val outside = "s3://${HogliteConformanceEngine.DATA_BUCKET}/elsewhere/out.parquet"
        for ((i, path) in listOf(inside, outside).withIndex()) {
            rawWrite(
                cat,
                "INSERT INTO files(table_id, path, file_format, record_count, file_size_bytes, row_id_start, " +
                    "explicit_row_ids, stats_state, begin_snapshot) " +
                    "VALUES (?, ?, 'parquet', 1, 1, ?, 0, 'provided', 1)",
                tableId,
                path,
                i.toLong(),
            )
        }

        val result =
            ObjectStore(engine.config()).use { objects ->
                HogliteCompaction(
                    store,
                    objects,
                    com.posthog.hoglake.compaction.CompactionConfig(
                        targetBytes = 1L shl 30,
                        minInputFiles = 2,
                        maxInputFiles = 2,
                        maxFanIn = 2,
                        maxGroupsPerRun = 10,
                    ),
                ).runOnce(cat, null, MaintenanceTrigger.MANUAL)
            }
        assertThat(result.groupsCompacted).isZero()
        assertThat(result.failedGroups).describedAs("the outside file never formed a group").isZero()
        assertThat(counter("hoglake_path_outside_data_path_total", "catalog", cat, "surface", "compaction"))
            .isEqualTo(1.0)
    }

    @Test
    fun `an overlapping data_path is the 422 PostgreSQL answers, and a half-created tenant can be dropped`() {
        val owner = HogliteConformanceEngine.uniqueName("owner")
        store.createCatalog(owner, dataPathOf(owner), null, null)
        // hoglite's own check, over every tenant of the backend: the same
        // 422 CatalogService.createCatalog answers between PostgreSQL
        // catalogs, and nothing is left behind.
        val under = HogliteConformanceEngine.uniqueName("under")
        assertThatThrownBy { store.createCatalog(under, "${dataPathOf(owner)}/sub", null, null) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("catalog data_paths must be disjoint")
            .hasMessageContaining(tenantOf(owner))
        assertThat(status(under).body()).contains("\"manifest\":null")
        // A data_path is printable ASCII: refused before any request.
        assertThatThrownBy { store.createCatalog(under, "${dataPathOf(under)}/café", null, null) }
            .isInstanceOf(HoglakeException.Validation::class.java)
            .hasMessageContaining("printable ASCII")

        // A tenant created with a data_path whose bootstrap never landed: the
        // drop removes it and reports the manifest's prefix, and the name
        // and prefix are free again.
        val half = HogliteConformanceEngine.uniqueName("half")
        val put =
            http.send(
                HttpRequest.newBuilder(URI.create("${engine.url()}/v1/tenants/${tenantOf(half)}"))
                    .header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.ofString("{\"data_path\":\"${dataPathOf(half)}\"}")).build(),
                HttpResponse.BodyHandlers.ofString(),
            )
        assertThat(put.statusCode()).isEqualTo(200)
        assertThatThrownBy { store.getCatalog(half) }.isInstanceOf(HoglakeException.Validation::class.java)
        assertThat(store.dropCatalog(half, null, null).dataPath).isEqualTo(dataPathOf(half))
        assertThat(status(half).body()).contains("\"manifest\":null")
        assertThat(store.createCatalog(half, dataPathOf(half), null, null).dataPath).isEqualTo(dataPathOf(half))

        // A `meta` table whose row was deleted with raw SQL is NOT a tenant
        // that never bootstrapped: the drop refuses it and deletes nothing.
        rawWrite(half, "DELETE FROM meta")
        assertThatThrownBy { store.dropCatalog(half, null, null) }
            .isInstanceOf(HoglakeException.BackendUnavailable::class.java)
            .hasMessageContaining("no metadata row")
        assertThat(json.readTree(status(half).body())["manifest"]["data_path"].asText()).isEqualTo(dataPathOf(half))
        // Repaired, it drops as any empty catalog does.
        rawWrite(half, "INSERT INTO meta(id, name, data_path) VALUES (1, ?, ?)", half, dataPathOf(half))
        assertThat(store.dropCatalog(half, null, null).dataPath).isEqualTo(dataPathOf(half))
    }

    @Test
    fun `a legacy tenant is readable and refuses every storage-acting path`() {
        val cat = HogliteConformanceEngine.uniqueName("legacy")
        val tenant = tenantOf(cat)
        // A tenant created before the field existed: no data_path in the
        // manifest, hoglake's schema bootstrapped by hand.
        val put =
            http.send(
                HttpRequest.newBuilder(URI.create("${engine.url()}/v1/tenants/$tenant"))
                    .PUT(HttpRequest.BodyPublishers.noBody()).build(),
                HttpResponse.BodyHandlers.ofString(),
            )
        assertThat(put.statusCode()).isEqualTo(200)
        assertThat(put.headers().firstValue(HogliteClient.DATA_PATH_HEADER)).isEmpty()
        client.tx(
            tenant,
            "boot:${UUID.randomUUID()}",
            null,
            HogliteSchema.DDL.map { HogliteStatement(it) } +
                HogliteStatement(
                    "INSERT INTO meta(id, name, data_path) VALUES (1, ?, ?)",
                    listOf(cat, dataPathOf(cat)),
                ) +
                HogliteStatement(
                    "INSERT INTO snapshots(id, ts, author, message, schema_version) VALUES (0, ?, NULL, NULL, 0)",
                    listOf(HogliteSchema.now()),
                ),
        )

        // Reads and writes work, on the row's own path, with no mismatch to audit.
        assertThat(store.getCatalog(cat).dataPath).isEqualTo(dataPathOf(cat))
        store.createNamespace(cat, "ns", null, null)
        assertThat(store.listNamespaces(cat, null, null).map { it.name }).containsExactly("ns")
        assertThat(counter("hoglake_hoglite_data_path_mismatch_total", "catalog", cat)).isZero()

        // Every storage-acting path is refused with the typed 409, before it acts.
        val refusals =
            mapOf(
                "cleanup" to {
                    RemovalStore(engine.config()).use { objects ->
                        HogliteCleanup(engine.config(), store, objects).runOnce(cat, 10, MaintenanceTrigger.MANUAL)
                    }
                },
                "compaction" to {
                    ObjectStore(engine.config()).use { objects ->
                        HogliteCompaction(
                            store, objects,
                            com.posthog.hoglake.compaction.CompactionConfig(
                                targetBytes = 1L shl 30,
                                minInputFiles = 2,
                                maxInputFiles = 2,
                                maxFanIn = 2,
                                maxGroupsPerRun = 10,
                            ),
                        ).runOnce(cat, null, MaintenanceTrigger.MANUAL)
                    }
                },
                "upload claim" to {
                    store.uploads.claim(cat, UUID.randomUUID(), UUID.randomUUID(), "${dataPathOf(cat)}/up", "data")
                },
            )
        for ((name, run) in refusals) {
            assertThat(catchThrowable { run() })
                .describedAs(name)
                .isInstanceOf(HoglakeException.DataPathUnverified::class.java)
                .hasMessageContaining("no trusted data_path")
        }
        assertThat(rawRead(cat, "SELECT COUNT(*) FROM uploads").firstRow()!![0].asLong()).isZero()

        // Expiry, retirement and the expired-upload sweep only queue removal
        // rows (cleanup, refused above, is what deletes), so they run: a
        // legacy catalog must still be able to satisfy its drop guards.
        assertThat(store.expiry.runOnce(cat, 10, MaintenanceTrigger.MANUAL).snapshotsExpired).isZero()
        assertThat(store.retirement.runOnce(cat, MaintenanceTrigger.MANUAL).tables).isZero()
        assertThat(store.uploads.scheduleExpired(cat, 10)).isZero()

        // A create that now names the path is refused: the field is set at
        // create or never, so the tenant has to go — and it can: the drop
        // (which deletes nothing in S3) takes a legacy catalog.
        assertThatThrownBy { store.createCatalog(cat, dataPathOf(cat), null, null) }
            .isInstanceOf(HoglakeException.AlreadyExists::class.java)
            .hasMessageContaining("set at create")
        store.dropNamespace(cat, "ns", author = "test", message = "drop")
        assertThat(store.dropCatalog(cat, null, null).dataPath).isEqualTo(dataPathOf(cat))
        assertThat(store.createCatalog(cat, dataPathOf(cat), null, null).dataPath).isEqualTo(dataPathOf(cat))
    }
}
