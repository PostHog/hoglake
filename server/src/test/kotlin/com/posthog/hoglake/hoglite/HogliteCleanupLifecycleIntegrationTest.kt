package com.posthog.hoglake.hoglite

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.MaintenanceTrigger
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.service.CleanupService
import com.posthog.hoglake.service.PatchField
import com.posthog.hoglake.service.RemovalStore
import com.sun.net.httpserver.HttpServer
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
        tenantPrefix: String = "hoglake-",
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
        val store = HogliteCatalogStore(client, tenantPrefix = tenantPrefix)
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
    ) = HogliteCleanup(engine.config().copy(receiptRetentionSeconds = 3600), store).use {
        it.runOnce(cat, 10, MaintenanceTrigger.MANUAL)
    }

    private fun receiptFailures(cat: String): Double =
        Metrics.boundRegistry!!
            .find("hoglake_commit_receipt_purge_failures_total").tag("catalog", cat).counter()?.count() ?: 0.0

    private fun phaseNames(): List<String> =
        attempts.map {
            it.txid.substringBefore(':')
        }.filter { it.startsWith("cleanup-") }

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
                engine.store.createCatalog(cat, "s3://hoglake-it/$cat")
                replacementTenant = engine.store.catalogOperation(cat) { engine.store.boundTenant(cat) }
                write(
                    engine.store, cat,
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
            assertThat(store.getCatalog(cat).name).isEqualTo(cat)
        }
    }

    @ParameterizedTest
    @ValueSource(strings = ["cancel", "raw-interrupt", "restored-interrupt", "fatal", "ordinary", "poison"])
    fun `sweep stops terminal signals before second catalog IO but ordinary and poison continue`(kind: String) {
        // A private registry contains exactly two real catalogs. Shuffling stays enabled:
        // the fault is assigned to whichever catalog is actually visited first.
        fixture(tenantPrefix = "sweep-${UUID.randomUUID()}-") { store, firstName, firstTenant ->
            val secondName = "second" + UUID.randomUUID().toString().replace("-", "")
            store.createCatalog(secondName, "s3://hoglake-it/$secondName")
            val tenants =
                mapOf(
                    firstName to firstTenant,
                    secondName to store.catalogOperation(secondName) { store.boundTenant(secondName) },
                )
            val paths = tenants.keys.associateBy { "s3://hoglake-it/$it/sweep-ticket" }
            for ((path, cat) in paths) {
                write(
                    store,
                    cat,
                    "INSERT INTO removals(path,file_kind,queued_snapshot,queued_at) VALUES (?, 'data',0,?)",
                    path,
                    HogliteSchema.now(),
                )
            }
            var failedName: String? = null
            val original: Throwable =
                when (kind) {
                    "cancel" -> CancellationException("sweep cancelled")
                    "raw-interrupt" -> InterruptedException("raw sweep interruption")
                    "restored-interrupt" -> HoglakeException.BackendUnavailable("interrupted work")
                    "fatal" -> AssertionError("fatal sweep")
                    else -> IllegalStateException("ordinary sweep failure")
                }
            // Test seam only: throw BEFORE object I/O. Claims, binding, ledger writes
            // and the complete sweep use the real engine; this is not an S3-delete claim.
            val removals =
                object : RemovalStore(engine.config()) {
                    override fun deleteBatch(pathsToDelete: Collection<String>): Map<String, String> {
                        if (failedName != null) return emptyMap()
                        val cat = paths.getValue(pathsToDelete.single())
                        failedName = cat
                        if (kind == "poison") {
                            engine.client.release(tenants.getValue(cat))
                            engine.client.deleteTenant(tenants.getValue(cat))
                            // Proven bound absence, not a synthetic NotFound.
                            store.queryCatalog(cat, "SELECT 1")
                            error("missing tenant answered successfully")
                        }
                        if (kind == "restored-interrupt") Thread.currentThread().interrupt()
                        throw original
                    }
                }
            val cfg = engine.config()
            val cleanup =
                HogliteCleanup(
                    store, removals, cfg.removalLedgerRetentionSeconds, cfg.maintenanceLedgerRetentionSeconds,
                    cfg.receiptRetentionSeconds, CleanupService.RECEIPT_PURGE_PAGE,
                    CleanupService.RECEIPT_PURGE_BUDGET_MS,
                    CleanupService.CLAIM_LEASE_SECONDS, CleanupService.STAGING_GRACE_SECONDS,
                    CleanupService.SUB_BATCH, CleanupService.STAGING_SUB_BATCH, "sweep-test",
                )
            var result = emptyList<Pair<String, com.posthog.hoglake.model.CleanupResult>>()
            val failure: Throwable?
            val observed: List<String>
            val interrupted: Boolean
            try {
                // Only transport DURING this sweep is observed. Seed and later diagnostic
                // reads cannot make the second catalog appear visited.
                tenantTraffic.clear()
                failure = catchThrowable { result = cleanup.runOnceAllCatalogs(10) }
                observed = tenantTraffic.filter { it in tenants.values }.distinct()
                interrupted = Thread.currentThread().isInterrupted
            } finally {
                Thread.interrupted()
                cleanup.close()
            }
            val failed = checkNotNull(failedName)
            val other = tenants.keys.single { it != failed }
            if (kind == "ordinary" || kind == "poison") {
                assertThat(failure).isNull()
                assertThat(observed).containsExactly(tenants.getValue(failed), tenants.getValue(other))
                assertThat(result.map { it.first }).containsExactly(other)
            } else {
                assertThat(failure).isSameAs(original)
                assertThat(observed).containsExactly(tenants.getValue(failed))
                assertThat(result).isEmpty()
            }
            assertThat(interrupted).isEqualTo(kind == "restored-interrupt")
            for (cat in tenants.keys) assertThat(
                catchThrowable {
                    store.boundTenant(cat)
                },
            ).isInstanceOf(IllegalStateException::class.java)
            assertThat(store.getCatalog(other).name).isEqualTo(other)
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
