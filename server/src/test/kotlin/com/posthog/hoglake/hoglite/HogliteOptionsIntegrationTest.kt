package com.posthog.hoglake.hoglite

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTask
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.persistence.PartialResult
import com.posthog.hoglake.service.PatchField
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.patch
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.nio.file.Files
import java.security.interfaces.ECPublicKey
import java.time.Instant
import java.util.UUID

/** Options and run records use the live engine, without a registry or a loop. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteOptionsIntegrationTest {
    private lateinit var engine: HogliteConformanceEngine
    private val store get() = engine.store
    private val options get() = store.options

    @BeforeAll
    fun start() {
        engine = HogliteConformanceEngine()
    }

    @AfterAll
    fun stop() {
        if (::engine.isInitialized) engine.close()
    }

    private fun freshCatalog(): String =
        HogliteConformanceEngine.uniqueName("options").also {
            store.createCatalog(it, "s3://hoglake-it/$it")
        }

    private fun rows(c: String) =
        engine.client.query(
            HogliteConformanceEngine.TENANTS.tenantFor(c),
            "SELECT task, run_trigger, status, error, result_json, started_at, finished_at " +
                "FROM maintenance_runs ORDER BY id",
        ).rows

    @Test
    fun `manual options routes use token placement without PostgreSQL and status stays unsupported`() {
        val c = freshCatalog()
        val id = UUID.randomUUID()
        val file = Files.createTempFile("options-backends", ".json")
        Files.writeString(file, """{"backends":[{"id":"$id","kind":"hoglite","url":"${engine.url()}"}]}""")
        val cfg =
            Config(
                defaultBackend = "none",
                backendsFile = file.toString(),
                authMode = "enforce",
                jdbcUrl = "jdbc:no-such-driver:must-not-open",
            )
        val pair = TestTokens.keyPair()
        val auth =
            AuthService(
                AuthMode.ENFORCE,
                JwtVerifier(
                    TestTokens.ISSUER,
                    TestTokens.AUDIENCE,
                    StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
                    backends = cfg.backendKinds,
                ),
            )
        val tenant = HogliteConformanceEngine.TENANTS.tenantFor(c)
        val token =
            TestTokens.mint(
                pair,
                TestTokens.operatorClaims().dropLast(1) +
                    """, "cat":{"$c":["admin"]}, "rtg":{"b":"hoglite","be":"$id","t":"$tenant"}}""",
            )
        try {
            App.build(cfg).use { app ->
                testApplication {
                    application { app.module(this, auth) }
                    val initial =
                        client.get("/v1/catalogs/$c/options") {
                            header(HttpHeaders.Authorization, "Bearer $token")
                        }
                    assertThat(initial.status).isEqualTo(HttpStatusCode.OK)
                    val patched =
                        client.patch("/v1/catalogs/$c/options") {
                            header(HttpHeaders.Authorization, "Bearer $token")
                            contentType(ContentType.Application.Json)
                            setBody(
                                """{"snapshot_retention_seconds":3600,"consumer_floor":false,""" +
                                    """"author":"test","message":"retain"}""",
                            )
                        }
                    assertThat(patched.status).isEqualTo(HttpStatusCode.OK)
                    val body = store.json.readTree(patched.bodyAsText())
                    assertThat(body["snapshot_retention_seconds"].asLong()).isEqualTo(3600)
                    assertThat(body["consumer_floor"].asBoolean()).isFalse()
                    assertThat(options.get(c).snapshotRetentionSeconds).isEqualTo(3600)
                    val status =
                        client.get("/v1/catalogs/$c/maintenance/status") {
                            header(HttpHeaders.Authorization, "Bearer $token")
                        }
                    assertThat(status.status).isEqualTo(HttpStatusCode.NotImplemented)
                    assertThat(status.bodyAsText()).contains("not_supported_on_backend")
                    assertThat(rows(c)).isEmpty()
                }
            }
        } finally {
            Files.delete(file)
        }
    }

    data class LedgerResult(val earliestSnapshotId: Long, val at: Instant)

    @Test
    fun `run ledger records success failure and partial results without changing outcomes`() {
        val c = freshCatalog()
        val started = Instant.now()
        val result = LedgerResult(7L, started)
        assertThat(store.recorded(c, MaintenanceTask.EXPIRY, MaintenanceTrigger.MANUAL) { result }).isSameAs(result)
        val failure = IllegalStateException("task failed")
        assertThat(
            runCatching {
                store.recorded(c, MaintenanceTask.CLEANUP, MaintenanceTrigger.LOOP) { throw failure }
            }.exceptionOrNull(),
        ).isSameAs(failure)
        val partial =
            object : RuntimeException("stopped"), PartialResult {
                override val partial: Any = mapOf("files_removed" to 3)
            }
        assertThat(
            runCatching {
                store.recorded(c, MaintenanceTask.COMPACTION, MaintenanceTrigger.MANUAL) { throw partial }
            }.exceptionOrNull(),
        ).isSameAs(partial)
        val runs = rows(c)
        assertThat(runs).hasSize(3)
        assertThat(runs[0].take(3).map { it.asString() }).containsExactly("expiry", "manual", "ok")
        assertThat(runs[0][3]).isNull()
        assertThat(store.json.readTree(runs[0][4].asString())["earliest_snapshot_id"].asLong()).isEqualTo(7)
        assertThat(store.json.readTree(runs[0][4].asString())["at"].asText()).isEqualTo(started.toString())
        assertThat(runs[1].take(4).map { it.asString() }).containsExactly("cleanup", "loop", "failed", "task failed")
        assertThat(runs[1][4]).isNull()
        assertThat(runs[2][2].asString()).isEqualTo("failed")
        assertThat(store.json.readTree(runs[2][4].asString())["files_removed"].asInt()).isEqualTo(3)
        for (run in runs) {
            val start = Instant.parse(run[5].asString())
            val end = Instant.parse(run[6].asString())
            assertThat(start).isAfterOrEqualTo(started.truncatedTo(java.time.temporal.ChronoUnit.MILLIS))
            assertThat(end).isAfterOrEqualTo(start)
        }
    }

    @Test
    fun `ledger insert failure never replaces the task result or failure`() {
        val c = freshCatalog()
        engine.client.tx(
            HogliteConformanceEngine.TENANTS.tenantFor(c),
            UUID.randomUUID().toString(),
            null,
            listOf(HogliteStatement("DROP TABLE maintenance_runs")),
        )
        assertThat(store.recorded(c, MaintenanceTask.EXPIRY, MaintenanceTrigger.MANUAL) { 42 }).isEqualTo(42)
        val failure = IllegalStateException("original")
        assertThat(
            runCatching {
                store.recorded(c, MaintenanceTask.CLEANUP, MaintenanceTrigger.MANUAL) { throw failure }
            }.exceptionOrNull(),
        ).isSameAs(failure)
    }

    @Test
    fun `an options patch cannot cross incarnations after reading its policy`() {
        val c = freshCatalog()
        val old = HogliteConformanceEngine.TENANTS.tenantFor(c)
        var tenant = old
        HogliteInterposer(engine.url()).use { proxy ->
            val routed = HogliteCatalogStore(proxy.client, HogliteTenantResolver { tenant })
            val hook =
                proxy.hook({ m, p, b ->
                    HogliteInterposer.isWriteTx(m, p, b) && "UPDATE meta SET consumer_floor" in b
                }) {
                    store.dropCatalog(c, "test", "replace")
                    tenant = "replacement-${UUID.randomUUID()}"
                    HogliteCatalogStore(engine.client, HogliteTenantResolver { tenant })
                        .createCatalog(c, "s3://hoglake-it/$c")
                }
            assertThatThrownBy {
                routed.options.patch(c, PatchField.Absent, false, "test", "policy")
            }.isInstanceOf(HoglakeException.NotFound::class.java)
            assertThat(hook.fired).isTrue()
            assertThat(engine.client.query(tenant, "SELECT consumer_floor FROM meta").firstRow()!![0].asLong())
                .isEqualTo(1L)
        }
    }

    @Test
    fun `a swallowed bound 404 cannot record a run in a replacement tenant`() {
        val c = freshCatalog()
        var tenant = HogliteConformanceEngine.TENANTS.tenantFor(c)
        val routed = HogliteCatalogStore(engine.client, HogliteTenantResolver { tenant })
        assertThatThrownBy {
            routed.recorded(c, MaintenanceTask.EXPIRY, MaintenanceTrigger.MANUAL) {
                routed.meta(c)
                store.dropCatalog(c, "test", "replace")
                assertThat(runCatching { routed.queryCatalog(c, "SELECT 1") }.exceptionOrNull())
                    .isInstanceOf(HoglakeException.NotFound::class.java)
                tenant = "replacement-${UUID.randomUUID()}"
                HogliteCatalogStore(engine.client, HogliteTenantResolver { tenant })
                    .createCatalog(c, "s3://hoglake-it/$c")
                42
            }
        }.isInstanceOf(HoglakeException.NotFound::class.java)
        assertThat(engine.client.query(tenant, "SELECT COUNT(*) FROM maintenance_runs").firstRow()!![0].asLong())
            .isZero()
        assertThat(routed.getCatalog(c).name).isEqualTo(c)
    }

    @Test
    fun `options patch validates and attributes retention decreases`() {
        val catalog = freshCatalog()
        val options = options
        val initial = options.get(catalog)
        assertThat(initial.snapshotRetentionSeconds).isNull()

        // Setting retention where none was needs operator attribution.
        assertThatThrownBy {
            options.patch(catalog, com.posthog.hoglake.service.PatchField.Set(3600L), null, null, null)
        }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
        val set =
            options.patch(
                catalog,
                com.posthog.hoglake.service.PatchField.Set(3600L),
                null,
                "test",
                "set retention",
            )
        assertThat(set.snapshotRetentionSeconds).isEqualTo(3600L)
        // Raising it never needs attribution.
        val raised = options.patch(catalog, com.posthog.hoglake.service.PatchField.Set(7200L), null, null, null)
        assertThat(raised.snapshotRetentionSeconds).isEqualTo(7200L)
        // Lowering it does.
        assertThatThrownBy {
            options.patch(catalog, com.posthog.hoglake.service.PatchField.Set(60L), null, null, null)
        }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
        // A non-positive value is a 422 regardless.
        assertThatThrownBy {
            options.patch(catalog, com.posthog.hoglake.service.PatchField.Set(0L), null, "test", "bad")
        }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
        // consumer_floor toggles without attribution.
        val floored = options.patch(catalog, com.posthog.hoglake.service.PatchField.Absent, false, null, null)
        assertThat(floored.consumerFloor).isFalse()
    }

    @Test
    fun `options metadata matrix preserves the existing attribution policy`() {
        val c = freshCatalog()
        val set = com.posthog.hoglake.service.PatchField.Set(20L)
        val absent = com.posthog.hoglake.service.PatchField.Absent
        for (initial in listOf(null, 100L)) {
            for (blank in listOf(null, "", " ", "\u2003", "\u0085")) {
                options.patch(c, com.posthog.hoglake.service.PatchField.Set(initial), true, "test", "reset")
                for ((author, message, required) in listOf(
                    Triple(blank, "valid", "author"),
                    Triple("valid", blank, "message"),
                    Triple(blank, blank, "author"),
                )) {
                    assertThatThrownBy { options.patch(c, set, false, author, message) }
                        .isExactlyInstanceOf(HoglakeException.Validation::class.java)
                        .hasMessage("$required is required and must not be blank")
                    assertThat(options.get(c).snapshotRetentionSeconds).isEqualTo(initial)
                    assertThat(options.get(c).consumerFloor).isTrue()
                }
            }
        }
        options.patch(c, set, true, "test", "valid reduction")
        for (v in listOf(20L, 30L)) {
            assertThat(
                options.patch(c, com.posthog.hoglake.service.PatchField.Set(v), null, "\u0085", " ")
                    .snapshotRetentionSeconds,
            ).isEqualTo(v)
        }
        for (v in listOf(0L, -1L)) {
            assertThatThrownBy {
                options.patch(c, com.posthog.hoglake.service.PatchField.Set(v), false, "test", "invalid")
            }.isExactlyInstanceOf(HoglakeException.Validation::class.java)
                .hasMessage("snapshot_retention_seconds must be positive (got $v); null disables expiry")
            assertThat(options.get(c).consumerFloor).isTrue()
        }
        assertThat(options.patch(c, absent, false, null, null).consumerFloor).isFalse()
        val disabled = options.patch(c, com.posthog.hoglake.service.PatchField.Set(null), null, null, null)
        assertThat(disabled.snapshotRetentionSeconds).isNull()
        assertThat(options.patch(c, absent, null, null, null)).isEqualTo(disabled)
    }
}
