package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.App
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.service.PatchField
import com.posthog.hoglake.service.RemovalStore
import com.sun.net.httpserver.HttpServer
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource
import java.net.InetSocketAddress
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.security.interfaces.ECPublicKey
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CancellationException
import java.util.concurrent.CopyOnWriteArrayList

/** Real purge SQL, foreign-key refusals and response-loss schedules on the published pinned engine. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteCleanupLifecycleIntegrationTest {
    private val engine by lazy { HogliteConformanceEngine() }
    private var started = false
    private val json = jacksonObjectMapper()

    private data class Attempt(val txid: String, val tenant: String)

    private val attempts = CopyOnWriteArrayList<Attempt>()
    private val tenantTraffic = CopyOnWriteArrayList<String>()

    @AfterAll
    fun close() {
        if (started) engine.close()
    }

    private fun fixture(
        rule: (String) -> Pair<Int, String>? = { null },
        sleeper: (Long) -> Unit = {},
        afterApply: (String) -> Unit = {},
        beforeApply: (String) -> Unit = {},
        body: (HogliteCatalogStore, String, String) -> Unit,
    ) {
        val running = engine
        started = true
        val upstream = HttpClient.newHttpClient()
        val relay = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        relay.createContext("/") { ex ->
            val raw = ex.requestBody.readBytes()
            if (ex.requestURI.path.startsWith("/v1/t/")) tenantTraffic += ex.requestURI.path.split('/')[3]
            val txid = if (raw.isEmpty()) "" else json.readTree(raw)["txid"]?.asText().orEmpty()
            if (txid.isNotEmpty()) attempts += Attempt(txid, ex.requestURI.path.split('/')[3])
            val decision = rule(txid)
            beforeApply(txid)
            // Ambiguity is injected AFTER successful apply. Other schedules refuse before forwarding.
            val answer =
                if (decision == null || decision.first == 422) {
                    upstream.send(
                        HttpRequest.newBuilder(URI.create(running.url() + ex.requestURI))
                            .header("Content-Type", "application/json")
                            .method(ex.requestMethod, HttpRequest.BodyPublishers.ofByteArray(raw)).build(),
                        HttpResponse.BodyHandlers.ofByteArray(),
                    )
                } else {
                    null
                }
            if (decision?.first == 422) check(answer!!.statusCode() == 200)
            if (answer?.statusCode() == 200) afterApply(txid)
            val bytes = decision?.second?.toByteArray() ?: answer!!.body()
            ex.sendResponseHeaders(
                decision?.first ?: answer!!.statusCode(),
                if (bytes.isEmpty()) -1 else bytes.size.toLong(),
            )
            ex.responseBody.use { it.write(bytes) }
        }
        relay.start()
        val client = HogliteClient("http://127.0.0.1:${relay.address.port}", sleeper = sleeper)
        val store = HogliteCatalogStore(client, HogliteConformanceEngine.TENANTS)
        val meters = SimpleMeterRegistry()
        Metrics.bind(meters)
        try {
            val cat = "hygiene" + UUID.randomUUID().toString().replace("-", "")
            store.createCatalog(cat, "s3://hoglake-it/$cat")
            val tenant = store.catalogOperation(cat) { store.boundTenant(cat) }
            attempts.clear()
            body(store, cat, tenant)
        } finally {
            Metrics.clear()
            meters.close()
            client.close()
            relay.stop(0)
        }
    }

    private fun write(
        store: HogliteCatalogStore,
        cat: String,
        sql: String,
        vararg args: Any?,
    ) = store.catalogOperation(cat) {
        store.runTx(cat, HogliteCatalogStore.freshTxid("fixture"), null, HogliteTx().apply { add(sql, *args) })
    }

    private fun rows(
        store: HogliteCatalogStore,
        cat: String,
        sql: String,
    ): List<List<Any?>> =
        store.catalogOperation(cat) {
            store.queryCatalog(cat, sql).rows
        }

    private fun seed(
        store: HogliteCatalogStore,
        cat: String,
    ) {
        val old = HogliteSchema.fmt(Instant.now().minusSeconds(40 * 86400))
        val now = HogliteSchema.now()
        write(
            store,
            cat,
            "INSERT INTO maintenance_runs(task, run_trigger, started_at, finished_at, status) " +
                "VALUES ('cleanup','manual',?,?, 'ok')",
            old,
            old,
        )
        for ((key, time) in listOf("old" to old, "keep" to now)) {
            write(store, cat, "INSERT INTO receipts VALUES (?, 'fp', 0, 0, ?)", key, time)
            write(
                store,
                cat,
                "INSERT INTO removals(path,file_kind,queued_snapshot,queued_at,drained_at,drained_outcome) " +
                    "VALUES (?, 'data',0,?,?, 'missing')",
                key,
                time,
                time,
            )
        }
    }

    private fun cleanup(
        store: HogliteCatalogStore,
        cat: String,
    ) = RemovalStore(engine.config()).use { objects ->
        HogliteCleanup(engine.config().copy(receiptRetentionSeconds = 3600), store, objects)
            .runOnce(cat, 10, MaintenanceTrigger.MANUAL)
    }

    private fun receiptFailures(cat: String): Double =
        Metrics.boundRegistry!!
            .find("hoglake_commit_receipt_purge_failures_total").tag("catalog", cat).counter()?.count() ?: 0.0

    private fun phaseNames(): List<String> =
        attempts.map {
            it.txid.substringBefore(':')
        }.filter { it.startsWith("cleanup-") }

    private fun queue(
        store: HogliteCatalogStore,
        cat: String,
        path: String,
        reason: String? = null,
    ) {
        write(
            store,
            cat,
            "INSERT INTO removals(path,file_kind,reason,queued_snapshot,queued_at,scheduled_at) " +
                "VALUES (?, 'data', ?, 0, ?, ?)",
            path,
            reason,
            HogliteSchema.now(),
            HogliteSchema.fmt(Instant.now().minusSeconds(86400)),
        )
    }

    @Test
    fun `manual cleanup route uses placement and server S3 configuration without PostgreSQL`() =
        fixture { store, cat, tenant ->
            val id = UUID.randomUUID()
            val file = Files.createTempFile("cleanup-backends", ".json")
            Files.writeString(file, """{"backends":[{"id":"$id","kind":"hoglite","url":"${engine.url()}"}]}""")
            val cfg =
                engine.config().copy(
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
            val token =
                TestTokens.mint(
                    pair,
                    TestTokens.operatorClaims().dropLast(1) +
                        """, "cat":{"$cat":["admin"]}, "rtg":{"b":"hoglite","be":"$id","t":"$tenant"}}""",
                )
            val path = "s3://hoglake-it/$cat/manual"
            queue(store, cat, path)
            engine.s3Client().use { s3 ->
                s3.putObject(
                    { it.bucket("hoglake-it").key("$cat/manual") },
                    software.amazon.awssdk.core.sync.RequestBody.fromString("delete"),
                )
            }
            try {
                App.build(cfg).use { app ->
                    testApplication {
                        application { app.module(this, auth) }
                        val response =
                            client.post("/v1/catalogs/$cat/maintenance/cleanup?batch=1") {
                                header(HttpHeaders.Authorization, "Bearer $token")
                            }
                        assertThat(response.status).isEqualTo(HttpStatusCode.OK)
                        assertThat(json.readTree(response.bodyAsText())["removed"].asLong()).isEqualTo(1)
                        val replay =
                            client.post("/v1/catalogs/$cat/maintenance/cleanup") {
                                header(HttpHeaders.Authorization, "Bearer $token")
                            }
                        assertThat(replay.status).isEqualTo(HttpStatusCode.OK)
                        assertThat(json.readTree(replay.bodyAsText())["removed"].asLong()).isZero()
                    }
                }
                assertThat(rows(store, cat, "SELECT task,run_trigger,status FROM maintenance_runs").map { it.take(3) })
                    .containsExactly(listOf("cleanup", "manual", "ok"), listOf("cleanup", "manual", "ok"))
                engine.s3Client().use { s3 ->
                    assertThat(s3.listObjectsV2 { it.bucket("hoglake-it").prefix("$cat/") }.contents()).isEmpty()
                }
            } finally {
                Files.delete(file)
            }
        }

    @Test
    fun `bulk and staging have separate budgets and staging distinguishes absent objects`() =
        fixture { store, cat, _ ->
            val paths = (0..3).map { "s3://hoglake-it/$cat/object$it" }
            engine.s3Client().use { s3 ->
                for (path in paths.take(3)) {
                    s3.putObject(
                        { it.bucket("hoglake-it").key(path.substringAfter("s3://hoglake-it/")) },
                        software.amazon.awssdk.core.sync.RequestBody.fromString("delete"),
                    )
                }
            }
            paths.forEachIndexed { i, path -> queue(store, cat, path, if (i < 2) null else "compaction_staging") }
            RemovalStore(engine.config()).use { objects ->
                val cleanup = HogliteCleanup(engine.config(), store, objects)
                val first = cleanup.runOnce(cat, 1)
                assertThat(first.removed).isEqualTo(2)
                assertThat(first.objectsRemoved).isEqualTo(2)
                assertThat(
                    rows(store, cat, "SELECT COUNT(*) FROM removals WHERE drained_at IS NULL").single()[0].asLong(),
                )
                    .isEqualTo(2)
                val second = cleanup.runOnce(cat, 10)
                assertThat(second.removed).isEqualTo(1)
                assertThat(second.missing).isEqualTo(1)
                assertThat(second.objectsRemoved).isEqualTo(1)
            }
            engine.s3Client().use { s3 ->
                assertThat(s3.listObjectsV2 { it.bucket("hoglake-it").prefix("$cat/") }.contents()).isEmpty()
            }
            assertThat(rows(store, cat, "SELECT drained_outcome FROM removals ORDER BY id").flatten())
                .containsExactly("deleted", "deleted", "deleted", "absent")
        }

    @Test
    fun `cleanup only deletes objects for removal rows it actually acquired`() {
        var steal: () -> Unit = {}
        fixture(beforeApply = { if (it.startsWith("cleanup-claim:")) steal() }) { store, cat, _ ->
            val path = "s3://hoglake-it/$cat/unclaimed"
            engine.s3Client().use { s3 ->
                s3.putObject(
                    { it.bucket("hoglake-it").key("$cat/unclaimed") },
                    software.amazon.awssdk.core.sync.RequestBody.fromString("keep"),
                )
            }
            queue(store, cat, path)
            var stolen = false
            steal = {
                if (!stolen) {
                    stolen = true
                    write(
                        engine.store, cat,
                        "UPDATE removals SET claimed_at = ?, claimed_by = 'other-worker' WHERE path = ?",
                        HogliteSchema.now(), path,
                    )
                }
            }
            val result = cleanup(store, cat)
            assertThat(stolen).isTrue()
            assertThat(result.objectsRemoved).isZero()
            assertThat(result.removed + result.missing).isZero()
            assertThat(rows(store, cat, "SELECT claimed_by, drained_at FROM removals").single())
                .containsExactly("other-worker", null)
            engine.s3Client().use { s3 ->
                assertThat(s3.headObject { it.bucket("hoglake-it").key("$cat/unclaimed") }.contentLength()).isEqualTo(4)
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["upload", "live-file", "ended-file", "live-dv", "ended-dv"])
    fun `live and historical references keep objects and retry only once per run`(kind: String) =
        fixture { store, cat, _ ->
            val path = "s3://hoglake-it/$cat/active"
            engine.s3Client().use { s3 ->
                s3.putObject(
                    { it.bucket("hoglake-it").key("$cat/active") },
                    software.amazon.awssdk.core.sync.RequestBody.fromString("keep"),
                )
            }
            when (kind) {
                "upload" ->
                    write(
                        store,
                        cat,
                        "INSERT INTO uploads(upload_id,owner,prefix,file_kind,path,state,claimed_at,expires_at) " +
                            "VALUES ('upload','owner','prefix','data',?,'active',?,?)",
                        path,
                        HogliteSchema.now(),
                        HogliteSchema.now(),
                    )
                "live-file", "ended-file" ->
                    write(
                        store,
                        cat,
                        "INSERT INTO files(table_id,begin_snapshot,end_snapshot,record_count,file_size_bytes," +
                            "row_id_start,stats_state,path) VALUES (1,0,?,1,4,0,'pending',?)",
                        if (kind == "ended-file") 1 else null,
                        path,
                    )
                else ->
                    write(
                        store,
                        cat,
                        "INSERT INTO dvs(data_file_id,table_id,delete_file_id,path,delete_count,file_size_bytes," +
                            "begin_snapshot,end_snapshot,epoch) VALUES (1,1,2,?,1,4,0,?,1)",
                        path,
                        if (kind == "ended-dv") 1 else null,
                    )
            }
            queue(store, cat, path)
            val result = cleanup(store, cat)
            assertThat(result.stillReferenced).isEqualTo(1)
            assertThat(result.objectsRemoved).isZero()
            assertThat(rows(store, cat, "SELECT attempts, claimed_by, drained_at FROM removals").single())
                .containsExactly(1, null, null)
            engine.s3Client().use { s3 ->
                assertThat(s3.headObject { it.bucket("hoglake-it").key("$cat/active") }.contentLength()).isEqualTo(4)
            }
        }

    @Test
    fun `referenced first sub-batch does not starve later removal rows`() =
        fixture { store, cat, _ ->
            for (i in 0..3) {
                val path = "s3://hoglake-it/$cat/row$i"
                queue(store, cat, path)
                if (i < 2) {
                    write(
                        store,
                        cat,
                        "INSERT INTO uploads(upload_id,owner,prefix,file_kind,path,state,claimed_at,expires_at) " +
                            "VALUES (?,'owner','prefix','data',?,'active',?,?)",
                        "upload$i",
                        path,
                        HogliteSchema.now(),
                        HogliteSchema.now(),
                    )
                }
            }
            RemovalStore(engine.config()).use { objects ->
                val cleanup = HogliteCleanup(engine.config().copy(cleanupSubBatchSize = 2), store, objects)
                val result = cleanup.runOnce(cat, 10)
                assertThat(result.stillReferenced).isEqualTo(2)
                assertThat(result.removed).isEqualTo(2)
                assertThat(
                    rows(store, cat, "SELECT COUNT(*) FROM removals WHERE drained_at IS NULL").single()[0].asLong(),
                )
                    .isEqualTo(2)
                assertThat(rows(store, cat, "SELECT attempts FROM removals WHERE drained_at IS NULL").flatten())
                    .containsExactly(1, 1)
            }
        }

    @Test
    fun `per-key deletion failures release claims and count attempts without counting objects`() =
        fixture { store, cat, _ ->
            val good = "s3://hoglake-it/$cat/good"
            val failed = "s3://hoglake-it/$cat/failed"
            queue(store, cat, good)
            queue(store, cat, failed)
            val objects =
                object : RemovalStore(engine.config()) {
                    override fun deleteBatch(paths: Collection<String>): Map<String, String> = mapOf(failed to "denied")
                }
            objects.use {
                val result = HogliteCleanup(engine.config(), store, objects).runOnce(cat, 10)
                assertThat(result.removed).isEqualTo(1)
                assertThat(result.objectsRemoved).isEqualTo(1)
                assertThat(
                    rows(store, cat, "SELECT attempts,claimed_by,drained_at FROM removals WHERE path = '$failed'")
                        .single(),
                ).containsExactly(1, null, null)
            }
        }

    @ParameterizedTest
    @ValueSource(strings = ["claim", "registered"])
    fun `settlement cannot overwrite another claimant or a drained row after object deletion`(race: String) {
        var steal: () -> Unit = {}
        fixture(beforeApply = { if (it.startsWith("cleanup-settle:")) steal() }) { store, cat, _ ->
            val path = "s3://hoglake-it/$cat/settlement"
            queue(store, cat, path)
            steal = {
                if (race == "claim") {
                    write(engine.store, cat, "UPDATE removals SET claimed_by = 'other-worker' WHERE path = ?", path)
                } else {
                    write(
                        engine.store, cat,
                        "UPDATE removals SET drained_at = ?, drained_outcome = 'registered' WHERE path = ?",
                        HogliteSchema.now(), path,
                    )
                }
            }
            val result = cleanup(store, cat)
            assertThat(result.removed).isZero()
            assertThat(result.objectsRemoved).isEqualTo(1)
            assertThat(result.settledElsewhere).isEqualTo(1)
            if (race == "claim") {
                assertThat(rows(store, cat, "SELECT claimed_by,drained_at FROM removals").single())
                    .containsExactly("other-worker", null)
                assertThat(result.stillReferenced).isZero()
            } else {
                assertThat(
                    rows(store, cat, "SELECT drained_outcome FROM removals").flatten(),
                ).containsExactly("registered")
                assertThat(result.stillReferenced).isEqualTo(1)
            }
        }
    }

    @Test
    fun `untyped hygiene failure records completed drain as a partial result`() {
        val original = IllegalStateException("ledger unavailable")
        fixture(
            rule = {
                if (it.startsWith(
                        "cleanup-ledger:",
                    )
                ) {
                    503 to """{"code":"not_applied","error":"injected"}"""
                } else {
                    null
                }
            },
            sleeper = { throw original },
        ) { store, cat, _ ->
            queue(store, cat, "s3://hoglake-it/$cat/absent")
            val failure = catchThrowable { cleanup(store, cat) }
            assertThat(failure).isInstanceOf(com.posthog.hoglake.persistence.PartialResult::class.java)
            assertThat(failure.cause).isSameAs(original)
            val partial =
                (failure as com.posthog.hoglake.persistence.PartialResult).partial as
                    com.posthog.hoglake.model.CleanupResult
            assertThat(partial.removed).isEqualTo(1)
            assertThat(phaseNames()).endsWith("cleanup-ledger", "cleanup-receipts", "cleanup-drained")
            val run =
                rows(
                    store,
                    cat,
                    "SELECT status,result_json FROM maintenance_runs ORDER BY id DESC LIMIT 1",
                ).single()
            assertThat(run[0]).isEqualTo("failed")
            assertThat(json.readTree(run[1].asString())["removed"].asLong()).isEqualTo(1)
        }
    }

    @Test
    fun `hygiene walks multiple bounded pages and counts actual receipt deletions`() =
        fixture { store, cat, _ ->
            val old = "2000-01-01T00:00:00.000Z"
            val numbers = "WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<1001) "
            write(
                store,
                cat,
                numbers +
                    "INSERT INTO maintenance_runs(task,run_trigger,started_at,finished_at,status) " +
                    "SELECT 'cleanup','manual',?,?,'ok' FROM n",
                old,
                old,
            )
            write(store, cat, numbers + "INSERT INTO receipts SELECT 'old-'||i,'fp',0,0,? FROM n", old)
            write(
                store,
                cat,
                numbers +
                    "INSERT INTO removals(path,file_kind,queued_snapshot,queued_at,drained_at,drained_outcome) " +
                    "SELECT 'old-'||i,'data',0,?,?,'missing' FROM n",
                old,
                old,
            )
            attempts.clear()
            val result = cleanup(store, cat)
            assertThat(result.receiptsPurged).isEqualTo(1001)
            assertThat(phaseNames().count { it == "cleanup-ledger" }).isEqualTo(2)
            assertThat(phaseNames().count { it == "cleanup-receipts" }).isEqualTo(6)
            assertThat(phaseNames().count { it == "cleanup-drained" }).isEqualTo(2)
            assertThat(rows(store, cat, "SELECT COUNT(*) FROM maintenance_runs").single()[0].asLong()).isEqualTo(1)
            assertThat(rows(store, cat, "SELECT COUNT(*) FROM receipts").single()[0].asLong()).isZero()
            assertThat(rows(store, cat, "SELECT COUNT(*) FROM removals").single()[0].asLong()).isZero()
        }

    @Test
    fun `configured staging grace and claim leases protect ineligible tickets`() =
        fixture { store, cat, _ ->
            queue(store, cat, "s3://hoglake-it/$cat/staging", "compaction_staging")
            queue(store, cat, "s3://hoglake-it/$cat/claimed")
            write(
                store,
                cat,
                "UPDATE removals SET claimed_by = 'other-worker', claimed_at = ? WHERE reason IS NULL",
                HogliteSchema.now(),
            )
            RemovalStore(engine.config()).use { objects ->
                val cleanup =
                    HogliteCleanup(
                        engine.config().copy(cleanupStagingGraceSeconds = 172800, cleanupClaimLeaseSeconds = 60),
                        store,
                        objects,
                    )
                assertThat(cleanup.runOnce(cat, 10).objectsRemoved).isZero()
                write(store, cat, "UPDATE removals SET claimed_at = '2000-01-01T00:00:00.000Z' WHERE reason IS NULL")
                val expiredLease = cleanup.runOnce(cat, 10)
                assertThat(expiredLease.removed).isEqualTo(1)
                assertThat(expiredLease.missing).isZero()
                write(store, cat, "UPDATE removals SET scheduled_at = '2000-01-01T00:00:00.000Z'")
                assertThat(cleanup.runOnce(cat, 10).missing).isEqualTo(1)
            }
        }

    @Test
    fun `ordinary maintenance purge failure still attempts receipts and drained ledger then fails original`() =
        fixture { store, cat, _ ->
            seed(store, cat)
            write(
                store,
                cat,
                "CREATE TABLE hold_ledger(run_id INTEGER REFERENCES maintenance_runs(id) ON DELETE RESTRICT)",
            )
            write(store, cat, "INSERT INTO hold_ledger SELECT id FROM maintenance_runs")
            attempts.clear()
            val failure = catchThrowable { cleanup(store, cat) }
            assertThat(
                failure,
            ).isInstanceOf(
                HoglakeException.Validation::class.java,
            ).hasMessageContaining("FOREIGN KEY constraint failed")
            assertThat(phaseNames()).containsExactly("cleanup-ledger", "cleanup-receipts", "cleanup-drained")
            assertThat(
                rows(store, cat, "SELECT idempotency_key FROM receipts ORDER BY idempotency_key").flatten(),
            ).containsExactly("keep")
            assertThat(rows(store, cat, "SELECT path FROM removals ORDER BY path").flatten()).containsExactly("keep")
            assertThat(
                rows(store, cat, "SELECT status, error FROM maintenance_runs ORDER BY id DESC LIMIT 1").single(),
            ).containsExactly("failed", failure.message)
        }

    @Test
    fun `ordinary receipt refusal counts once while all independent phases retain their contracts`() =
        fixture { store, cat, _ ->
            seed(store, cat)
            write(
                store,
                cat,
                "CREATE TABLE hold_receipt(key TEXT REFERENCES receipts(idempotency_key) ON DELETE RESTRICT)",
            )
            write(store, cat, "INSERT INTO hold_receipt VALUES ('old')")
            attempts.clear()
            val result = cleanup(store, cat)
            assertThat(result.receiptsPurged).isZero()
            assertThat(result.receiptsPurgeFailures).isEqualTo(1)
            assertThat(receiptFailures(cat)).isEqualTo(1.0)
            assertThat(phaseNames()).containsExactly("cleanup-ledger", "cleanup-receipts", "cleanup-drained")
            assertThat(rows(store, cat, "SELECT path FROM removals").flatten()).containsExactly("keep")
            assertThat(
                rows(store, cat, "SELECT status FROM maintenance_runs ORDER BY id DESC LIMIT 1").flatten(),
            ).containsExactly("ok")
        }

    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun `drained ordinary refusal stays best effort alone and secondary to ledger failure`(failLedger: Boolean) =
        fixture { store, cat, _ ->
            seed(store, cat)
            write(
                store,
                cat,
                "CREATE TABLE hold_drained(removal_id INTEGER REFERENCES removals(id) ON DELETE RESTRICT)",
            )
            write(store, cat, "INSERT INTO hold_drained SELECT id FROM removals WHERE path = 'old'")
            if (failLedger) {
                write(
                    store,
                    cat,
                    "CREATE TABLE hold_ledger(run_id INTEGER REFERENCES maintenance_runs(id) ON DELETE RESTRICT)",
                )
                write(store, cat, "INSERT INTO hold_ledger SELECT id FROM maintenance_runs")
            }
            attempts.clear()
            val failure =
                catchThrowable {
                    val result = cleanup(store, cat)
                    assertThat(result.receiptsPurged).isEqualTo(1)
                    assertThat(result.receiptsPurgeFailures).isZero()
                }
            if (failLedger) {
                assertThat(
                    failure,
                ).isInstanceOf(
                    HoglakeException.Validation::class.java,
                ).hasMessageContaining("FOREIGN KEY constraint failed")
                assertThat(failure.suppressed).hasSize(1)
                assertThat(
                    failure.suppressed.single(),
                ).isNotSameAs(failure).isInstanceOf(HoglakeException.Validation::class.java)
            } else {
                assertThat(failure).isNull()
            }
            assertThat(phaseNames()).containsExactly("cleanup-ledger", "cleanup-receipts", "cleanup-drained")
            assertThat(rows(store, cat, "SELECT idempotency_key FROM receipts").flatten()).containsExactly("keep")
            assertThat(
                rows(store, cat, "SELECT path FROM removals ORDER BY path").flatten(),
            ).containsExactly("keep", "old")
        }

    @Test
    fun `independent receipt purge retains snapshot retention floor and reports actual count`() =
        fixture { store, cat, _ ->
            seed(store, cat)
            HogliteOptions(store).patch(cat, PatchField.Set(7200L), null, "test", "receipt floor")
            write(
                store,
                cat,
                "INSERT INTO receipts VALUES ('floor-held','fp',0,0,?)",
                HogliteSchema.fmt(Instant.now().minusSeconds(7200)),
            )
            val result = cleanup(store, cat)
            assertThat(result.receiptsPurged).isEqualTo(1)
            assertThat(result.receiptsPurgeFailures).isZero()
            assertThat(
                rows(store, cat, "SELECT idempotency_key FROM receipts ORDER BY idempotency_key").flatten(),
            ).containsExactly("floor-held", "keep")
        }

    @Test
    fun `unknown ledger outcome keeps original txid plus finalizer poison and never reaches next incarnation`() {
        var replace: () -> Unit = {}
        var replacementTenant = ""
        fixture(rule = { txid ->
            when {
                txid.startsWith("cleanup-ledger:") -> 422 to "{\"error\":\"lost applied response\"}"
                else -> null
            }
        }, afterApply = { if (it.startsWith("cleanup-ledger:")) replace() }) { store, cat, tenant ->
            seed(store, cat)
            replace = {
                engine.store.dropCatalog(cat, "test", "replace during hygiene")
                replacementTenant = "replacement-$cat"
                val replacement = HogliteCatalogStore(engine.client, HogliteTenantResolver { replacementTenant })
                replacement.createCatalog(cat, "s3://hoglake-it/$cat")
                write(
                    replacement, cat,
                    "INSERT INTO receipts VALUES ('replacement-keep','fp',0,0,'2000-01-01T00:00:00.000Z')",
                )
            }
            attempts.clear()
            val failure = catchThrowable { cleanup(store, cat) }
            val ledger = attempts.single { it.txid.startsWith("cleanup-ledger:") }
            assertThat(failure).isInstanceOf(HoglakeException.CommitOutcomeUnknown::class.java)
            assertThat((failure as HoglakeException.CommitOutcomeUnknown).txid).isEqualTo(ledger.txid)
            assertThat(failure.suppressed).hasSize(1)
            assertThat(failure.suppressed.single()).isInstanceOf(HoglakeException.NotFound::class.java)
            assertThat(phaseNames()).containsExactly("cleanup-ledger", "cleanup-receipts")
            assertThat(receiptFailures(cat)).isZero()
            assertThat(attempts.map { it.tenant }.distinct()).containsExactly(tenant)
            assertThat(replacementTenant).isNotEqualTo(tenant)
            // Explicit captured-tenant diagnostic: never bind old work to the replacement.
            assertThat(
                engine.client.query(replacementTenant, "SELECT idempotency_key FROM receipts").rows.flatten(),
            ).containsExactly("replacement-keep")
            assertThat(catchThrowable { store.boundTenant(cat) }).isInstanceOf(IllegalStateException::class.java)
            assertThat(catchThrowable { store.getCatalog(cat) }).isInstanceOf(HoglakeException.NotFound::class.java)
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["cancel", "raw-interrupt", "restored-interrupt", "fatal", "ordinary", "poison"])
    fun `manual drain failures preserve terminal signals and stop hygiene`(kind: String) =
        fixture { store, cat, tenant ->
            val path = "s3://hoglake-it/$cat/drain-ticket"
            write(
                store,
                cat,
                "INSERT INTO removals(path,file_kind,queued_snapshot,queued_at) VALUES (?, 'data',0,?)",
                path,
                HogliteSchema.now(),
            )
            val original: Throwable =
                when (kind) {
                    "cancel" -> CancellationException("drain cancelled")
                    "raw-interrupt" -> InterruptedException("raw drain interruption")
                    "restored-interrupt" -> HoglakeException.BackendUnavailable("interrupted work")
                    "fatal" -> AssertionError("fatal drain")
                    else -> IllegalStateException("ordinary drain failure")
                }
            val removals =
                object : RemovalStore(engine.config()) {
                    override fun deleteBatch(paths: Collection<String>): Map<String, String> {
                        if (kind == "poison") {
                            engine.client.release(tenant)
                            engine.client.deleteTenant(tenant)
                            store.queryCatalog(cat, "SELECT 1")
                            error("missing tenant answered successfully")
                        }
                        if (kind == "restored-interrupt") Thread.currentThread().interrupt()
                        throw original
                    }
                }
            try {
                attempts.clear()
                val failure = catchThrowable { HogliteCleanup(engine.config(), store, removals).runOnce(cat, 10) }
                if (kind == "poison") {
                    assertThat(failure).isInstanceOf(HoglakeException.NotFound::class.java)
                } else {
                    assertThat(failure).isSameAs(original)
                }
                assertThat(phaseNames()).containsExactly("cleanup-claim")
                assertThat(Thread.currentThread().isInterrupted).isEqualTo(kind == "restored-interrupt")
                assertThat(catchThrowable { store.boundTenant(cat) }).isInstanceOf(IllegalStateException::class.java)
            } finally {
                Thread.interrupted()
                removals.close()
            }
        }

    @ParameterizedTest
    @CsvSource(
        "cleanup-ledger,cancel",
        "cleanup-ledger,fatal",
        "cleanup-receipts,cancel",
        "cleanup-receipts,fatal",
        "cleanup-drained,cancel",
        "cleanup-drained,fatal",
    )
    fun `terminal cancellation and Error in each purge stop siblings without swallowing`(
        phase: String,
        kind: String,
    ) {
        val original = if (kind == "cancel") CancellationException("cancel $phase") else AssertionError("fatal $phase")
        run {
            run {
                fixture(rule = {
                    if (it.startsWith("$phase:")) 503 to "{\"code\":\"not_applied\",\"error\":\"injected\"}" else null
                }, sleeper = { throw original }) { store, cat, _ ->
                    attempts.clear()
                    assertThat(catchThrowable { cleanup(store, cat) }).isSameAs(original)
                    assertThat(receiptFailures(cat)).isZero()
                    assertThat(
                        phaseNames(),
                    ).isEqualTo(
                        listOf("cleanup-ledger", "cleanup-receipts", "cleanup-drained").takeWhile {
                            it != phase
                        } + phase,
                    )
                    assertThat(
                        catchThrowable { store.boundTenant(cat) },
                    ).isInstanceOf(IllegalStateException::class.java)
                    assertThat(store.getCatalog(cat).name).isEqualTo(cat)
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["cleanup-ledger", "cleanup-receipts", "cleanup-drained"])
    fun `interrupt during each purge preserves interrupt and stops siblings`(phase: String) {
        run {
            fixture(rule = {
                if (it.startsWith("$phase:")) 503 to "{\"code\":\"not_applied\",\"error\":\"injected\"}" else null
            }, sleeper = { throw InterruptedException("shutdown") }) { store, cat, _ ->
                try {
                    attempts.clear()
                    assertThat(
                        catchThrowable { cleanup(store, cat) },
                    ).isInstanceOf(HoglakeException.BackendUnavailable::class.java)
                    assertThat(Thread.currentThread().isInterrupted).isTrue()
                    assertThat(receiptFailures(cat)).isZero()
                    assertThat(
                        phaseNames(),
                    ).isEqualTo(
                        listOf("cleanup-ledger", "cleanup-receipts", "cleanup-drained").takeWhile {
                            it != phase
                        } + phase,
                    )
                    assertThat(
                        catchThrowable { store.boundTenant(cat) },
                    ).isInstanceOf(IllegalStateException::class.java)
                } finally {
                    Thread.interrupted()
                }
                assertThat(store.getCatalog(cat).name).isEqualTo(cat)
            }
        }
    }
}
