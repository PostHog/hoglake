package com.posthog.hoglake.hoglite

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.CommitRequest
import com.posthog.hoglake.model.FileRegistration
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.TableAppend
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.security.interfaces.ECPublicKey
import java.util.UUID

/** Requeue failed files through the live engine. This operation does not read object data. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteHydratorMaintenanceIntegrationTest {
    private val engine by lazy { HogliteConformanceEngine() }
    private val store get() = engine.store

    @AfterAll
    fun close() = engine.close()

    private fun catalog(): String {
        val c = HogliteConformanceEngine.uniqueName("rehydrate")
        store.createCatalog(c, "s3://hoglake-it/$c")
        store.createNamespace(c, "ns")
        for (name in listOf("a", "b", "dropped")) {
            store.createTable(c, "ns", name, listOf(ColumnDef("id", ColType.LONG)))
            store.commit(
                c,
                CommitRequest(
                    appends =
                        listOf(
                            TableAppend(
                                "ns",
                                name,
                                (1..3).map { FileRegistration("s3://hoglake-it/$c/$name$it", 1, 100) },
                            ),
                        ),
                ),
            )
        }
        write(c, "UPDATE files SET stats_state = 'failed'")
        write(c, "UPDATE files SET end_snapshot = begin_snapshot WHERE id = 2")
        write(c, "UPDATE files SET stats_state = 'provided' WHERE id = 3")
        val dropped = store.getTable(c, "ns", "dropped")
        store.dropTable(c, "ns", "dropped", dropped.tableUuid, "test", "drop")
        return c
    }

    private fun write(
        c: String,
        sql: String,
    ) = store.catalogOperation(c) {
        store.runTx(c, HogliteCatalogStore.freshTxid("fixture"), null, HogliteTx().apply { add(sql) })
    }

    private fun states(c: String) =
        store.catalogOperation(c) {
            store.queryCatalog(c, "SELECT stats_state FROM files ORDER BY id").rows.map { it[0].asString() }
        }

    @Test
    fun `table and catalog requeue exclude ended provided and dropped files and count real changes`() {
        val c = catalog()
        assertThat(store.hydratorMaintenance.rehydrateFailed(c, "ns", "a").requeued).isEqualTo(1)
        assertThat(
            states(c),
        ).containsExactly("pending", "failed", "provided", "failed", "failed", "failed", "failed", "failed", "failed")
        assertThat(store.hydratorMaintenance.rehydrateFailed(c).requeued).isEqualTo(3)
        assertThat(store.hydratorMaintenance.rehydrateFailed(c).requeued).isZero()
        assertThat(
            states(c),
        ).containsExactly(
            "pending", "failed", "provided", "pending", "pending", "pending", "failed", "failed", "failed",
        )
        val runs =
            store.catalogOperation(c) {
                store.queryCatalog(
                    c,
                    "SELECT task, run_trigger, status, result_json FROM maintenance_runs ORDER BY id",
                ).rows
            }
        assertThat(runs).hasSize(3)
        assertThat(runs.map { it.take(3) }).allSatisfy { assertThat(it).containsExactly("hydrator", "manual", "ok") }
        assertThat(runs.map { store.json.readTree(it[3].asString())["requeued"].asLong() }).containsExactly(1L, 3L, 0L)
    }

    @Test
    fun `requeue validates scope and requires a live table and catalog`() {
        val c = catalog()
        for ((ns, table) in listOf("ns" to null, null to "a")) {
            assertThatThrownBy { store.hydratorMaintenance.rehydrateFailed(c, ns, table) }
                .isInstanceOf(HoglakeException.Validation::class.java)
        }
        for ((ns, table) in listOf("ns" to "dropped", "missing" to "a", "ns" to "missing")) {
            assertThatThrownBy { store.hydratorMaintenance.rehydrateFailed(c, ns, table) }
                .isInstanceOf(HoglakeException.NotFound::class.java)
        }
        assertThatThrownBy { store.hydratorMaintenance.rehydrateFailed("missing") }
            .isInstanceOf(HoglakeException.NotFound::class.java)
    }

    @Test
    fun `table dropped between lookup and requeue stays failed`() {
        val c = catalog()
        HogliteInterposer(engine.url()).use { proxy ->
            val hook =
                proxy.hook({ m, p, b -> HogliteInterposer.isWriteTx(m, p, b) && "UPDATE files SET stats_state" in b }) {
                    val t = store.getTable(c, "ns", "a")
                    store.dropTable(c, "ns", "a", t.tableUuid, "test", "race")
                }
            assertThat(proxy.store.hydratorMaintenance.rehydrateFailed(c, "ns", "a").requeued).isZero()
            assertThat(hook.fired).isTrue()
            assertThat(states(c).first()).isEqualTo("failed")
        }
    }

    @TempDir
    lateinit var dir: Path

    @Test
    fun `routed requeue needs no PostgreSQL and the retirement route stays absent`() {
        val c = catalog()
        val id = UUID.randomUUID()
        val file = dir.resolve("backends.json")
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
        App.build(cfg).use { app ->
            testApplication {
                application { app.module(this, auth) }
                val result =
                    client.post("/v1/catalogs/$c/maintenance/rehydrate?namespace=ns&table=a") {
                        header(HttpHeaders.Authorization, "Bearer $token")
                    }
                assertThat(result.status).isEqualTo(HttpStatusCode.OK)
                assertThat(store.json.readTree(result.bodyAsText())["requeued"].asLong()).isEqualTo(1)
                val invalid =
                    client.post("/v1/catalogs/$c/maintenance/rehydrate?namespace=ns") {
                        header(HttpHeaders.Authorization, "Bearer $token")
                    }
                assertThat(invalid.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
                val other =
                    client.post("/v1/catalogs/other/maintenance/rehydrate") {
                        header(HttpHeaders.Authorization, "Bearer $token")
                    }
                assertThat(other.status).isEqualTo(HttpStatusCode.NotFound)
                val absent =
                    client.post("/v1/catalogs/$c/maintenance/retire") {
                        header(HttpHeaders.Authorization, "Bearer $token")
                    }
                assertThat(absent.status).isEqualTo(HttpStatusCode.Unauthorized)
            }
        }
    }
}
