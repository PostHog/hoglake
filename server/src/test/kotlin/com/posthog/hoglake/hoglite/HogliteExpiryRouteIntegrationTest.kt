package com.posthog.hoglake.hoglite

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.service.PatchField
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.patch
import io.ktor.client.request.post
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.nio.file.Files
import java.security.interfaces.ECPublicKey
import java.util.UUID

/** Options and run records use the live engine, without a registry or a loop. */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class HogliteExpiryRouteIntegrationTest {
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
    fun `manual expiry uses token placement and configured purge budget without PostgreSQL`() {
        val c = freshCatalog()
        store.createNamespace(c, "ns")
        val table =
            store.createTable(
                c,
                "ns",
                "t",
                listOf(
                    com.posthog.hoglake.model.ColumnDef("id", com.posthog.hoglake.model.ColType.LONG),
                ),
            )
        store.commit(
            c,
            com.posthog.hoglake.model.CommitRequest(
                appends =
                    listOf(
                        com.posthog.hoglake.model.TableAppend(
                            "ns",
                            "t",
                            listOf(
                                com.posthog.hoglake.model.FileRegistration(
                                    "s3://hoglake-it/$c/data/ended.parquet",
                                    1,
                                    100,
                                ),
                            ),
                        ),
                    ),
            ),
        )
        store.truncateTable(c, "ns", "t", table.tableUuid, "test", "end history")
        store.options.patch(c, PatchField.Set(1L), null, "test", "short retention")
        store.catalogOperation(c) {
            store.runTx(
                c,
                HogliteCatalogStore.freshTxid("age"),
                null,
                HogliteTx().apply {
                    add("UPDATE snapshots SET ts = '2001-01-01T00:00:00.000Z'")
                },
            )
        }
        val id = UUID.randomUUID()
        val file = Files.createTempFile("options-backends", ".json")
        Files.writeString(file, """{"backends":[{"id":"$id","kind":"hoglite","url":"${engine.url()}"}]}""")
        val cfg =
            Config(
                defaultBackend = "none",
                backendsFile = file.toString(),
                authMode = "enforce",
                jdbcUrl = "jdbc:no-such-driver:must-not-open",
                expiryIntervalMs = 0,
                expiryPurgeBudgetMs = 0,
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
                    val invalid =
                        client.post("/v1/catalogs/$c/maintenance/expire?batch=0") {
                            header(HttpHeaders.Authorization, "Bearer $token")
                        }
                    assertThat(invalid.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
                    val response =
                        client.post("/v1/catalogs/$c/maintenance/expire?batch=100") {
                            header(HttpHeaders.Authorization, "Bearer $token")
                        }
                    assertThat(response.status).isEqualTo(HttpStatusCode.OK)
                    val body = store.json.readTree(response.bodyAsText())
                    assertThat(body["new_earliest_snapshot_id"].asLong()).isEqualTo(store.getCatalog(c).headSnapshotId)
                    assertThat(body["purge_truncated"].asBoolean()).isTrue()
                    assertThat(body["purge_remaining"].asLong()).isEqualTo(1)
                    assertThat(body["data_files_purged"].asLong()).isZero()
                    val status =
                        client.get("/v1/catalogs/$c/maintenance/status") {
                            header(HttpHeaders.Authorization, "Bearer $token")
                        }
                    assertThat(status.status).isEqualTo(HttpStatusCode.OK)
                    assertThat(store.json.readTree(status.bodyAsText())["catalog"].asText()).isEqualTo(c)
                    val runs =
                        client.get("/v1/catalogs/$c/maintenance/runs?task=expiry&limit=1") {
                            header(HttpHeaders.Authorization, "Bearer $token")
                        }
                    assertThat(runs.status).isEqualTo(HttpStatusCode.OK)
                    val page = store.json.readTree(runs.bodyAsText())
                    assertThat(page["has_more"].asBoolean()).isTrue()
                    assertThat(page["runs"].single()["result"]["purge_remaining"].asLong()).isEqualTo(1)
                    val before = page["runs"].single()["run_id"].asLong()
                    val previous =
                        client.get("/v1/catalogs/$c/maintenance/runs?before=$before") {
                            header(HttpHeaders.Authorization, "Bearer $token")
                        }
                    val older = store.json.readTree(previous.bodyAsText())
                    assertThat(older["has_more"].asBoolean()).isFalse()
                    assertThat(older["runs"].single()["status"].asText()).isEqualTo("failed")
                    assertThat(older["runs"].single()["result"].isNull).isTrue()
                    for ((suffix, expected) in listOf(
                        "?limit=0" to HttpStatusCode.UnprocessableEntity,
                        "?task=unknown" to HttpStatusCode.UnprocessableEntity,
                        "?before=bad" to HttpStatusCode.BadRequest,
                    )) {
                        val invalidRun =
                            client.get("/v1/catalogs/$c/maintenance/runs$suffix") {
                                header(HttpHeaders.Authorization, "Bearer $token")
                            }
                        assertThat(invalidRun.status).describedAs(suffix).isEqualTo(expected)
                    }
                    for (surface in listOf("status", "runs")) {
                        assertThat(client.get("/v1/catalogs/$c/maintenance/$surface").status)
                            .isEqualTo(HttpStatusCode.Unauthorized)
                        val other =
                            client.get("/v1/catalogs/other/maintenance/$surface") {
                                header(HttpHeaders.Authorization, "Bearer $token")
                            }
                        assertThat(other.status).isEqualTo(HttpStatusCode.NotFound)
                    }
                    assertThat(rows(c)).hasSize(2)
                    assertThat(rows(c)[0].take(3).map { it.asString() }).containsExactly("expiry", "manual", "failed")
                    assertThat(rows(c)[1].take(3).map { it.asString() }).containsExactly("expiry", "manual", "ok")
                }
            }
        } finally {
            Files.delete(file)
        }
    }
}
