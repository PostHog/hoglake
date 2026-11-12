package com.posthog.hoglake.store

import com.posthog.hoglake.Config
import com.posthog.hoglake.api.installErrorMapping
import com.posthog.hoglake.api.installPublicationRoutes
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.Routing
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.auth.installAuth
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.hoglite.HogliteCatalogStore
import com.posthog.hoglake.hoglite.HogliteClient
import com.sun.net.httpserver.HttpServer
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.lang.reflect.Proxy
import java.net.InetSocketAddress
import java.nio.file.Files
import java.nio.file.Path
import java.security.interfaces.ECPublicKey
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList

class CatalogStoreSelectorTest {
    @TempDir
    lateinit var dir: Path
    private val id = UUID.fromString("5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")
    private val otherId = UUID.fromString("6b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")
    private val pair = TestTokens.keyPair()
    private var backend: HttpServer? = null

    @AfterEach
    fun stopBackend() {
        backend?.stop(0)
    }

    private fun clientState(
        client: HogliteClient,
        field: String,
    ): Any? = HogliteClient::class.java.getDeclaredField(field).apply { isAccessible = true }.get(client)

    private val postgres =
        Proxy.newProxyInstance(
            CatalogStore::class.java.classLoader,
            arrayOf(CatalogStore::class.java),
        ) { _, method, _ ->
            error("selection must not call PostgreSQL: ${method.name}")
        } as CatalogStore

    private fun config(
        default: String,
        backendIds: List<UUID> = listOf(id),
        backendUrl: String = "http://localhost:1",
    ): Config {
        val file = dir.resolve("backends.json")
        val entries = backendIds.joinToString(",") { """{"id":"$it","kind":"hoglite","url":"$backendUrl"}""" }
        Files.writeString(file, """{"backends":[$entries]}""")
        return Config(backendsFile = file.toString(), defaultBackend = default, authMode = "enforce")
    }

    private fun token(
        routed: Boolean = true,
        backendId: UUID = id,
        subject: String = "sa:selection",
    ): String {
        val placement = if (routed) """, "rtg":{"b":"hoglite","be":"$backendId","t":"opaque_1"}""" else ""
        val claims = TestTokens.operatorClaims(sub = subject).dropLast(1) + """, "cat":{"lake":["admin"]}$placement}"""
        return TestTokens.mint(pair, claims)
    }

    @Test
    fun `explicit placement uses the routed constructor without a request or fallback`() {
        val cfg = config("none").copy(removalLedgerRetentionSeconds = 12345)
        val selector = CatalogStoreSelector(postgres, cfg)
        val routing = Routing("lake", BackendKind.HOGLITE, id, "opaque_1")
        val first = selector.forPlacement(routing) as HogliteCatalogStore
        val second = selector.forPlacement(routing) as HogliteCatalogStore
        assertThat(first).isNotSameAs(second)
        assertThat(first.client).isSameAs(second.client)
        assertThat(first.partitionLookupAdmission).isSameAs(second.partitionLookupAdmission)
        assertThat(first.ledgerRetentionSeconds).isEqualTo(12345)
        assertThat(first.catalogOperation("lake") { first.boundTenant("lake") }).isEqualTo("opaque_1")
        assertThatThrownBy { first.catalogOperation("other") { first.boundTenant("other") } }
            .isInstanceOf(com.posthog.hoglake.model.HoglakeException.NotFound::class.java)
        val pg = Routing("lake", BackendKind.POSTGRES, id, null)
        assertThat(selector.forPlacement(pg)).isSameAs(postgres)
        assertThatThrownBy { CatalogStoreSelector(null, cfg).forPlacement(pg) }
            .isInstanceOf(com.posthog.hoglake.model.HoglakeException.NotFound::class.java)
    }

    @Test
    fun `verified placements share backend admission without sharing request scoped stores`() =
        testApplication {
            val observed = CopyOnWriteArrayList<String?>()
            val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
            backend = server
            server.createContext("/") { exchange ->
                exchange.requestBody.readBytes()
                observed.add(exchange.requestHeaders.getFirst("Authorization"))
                val body = """{"cols":["x"],"rows":[[1]],"affected":0}""".toByteArray()
                exchange.sendResponseHeaders(200, body.size.toLong())
                exchange.responseBody.use { it.write(body) }
            }
            server.start()
            val cfg =
                config("none", listOf(id, otherId), "http://127.0.0.1:${server.address.port}").copy(
                    removalLedgerRetentionSeconds = 12_345,
                    compactionTargetBytes = 64L * 1024 * 1024,
                    compactionMinInputFiles = 3,
                    compactionMaxInputFiles = 17,
                )
            val selector = CatalogStoreSelector(postgres, cfg)
            val stores = mutableListOf<CatalogStore>()
            application {
                installAuth(
                    AuthService(
                        AuthMode.ENFORCE,
                        JwtVerifier(
                            TestTokens.ISSUER,
                            TestTokens.AUDIENCE,
                            StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
                            backends = cfg.backendKinds,
                            clock = { 1_700_000_001 },
                        ),
                    ),
                )
                install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                install(StatusPages) { installErrorMapping() }
                installPublicationRoutes(selector)
                routing {
                    get("/v1/catalogs") {
                        val listing = selector.forListing(call)
                        assertThat(listing.store).isInstanceOf(HogliteCatalogStore::class.java)
                        assertThat(selector.forCatalog(call, "lake")).isSameAs(listing.store)
                        val hoglite = listing.store as HogliteCatalogStore
                        assertThat(hoglite.ledgerRetentionSeconds).isEqualTo(cfg.removalLedgerRetentionSeconds)
                        assertThat(hoglite.smallFileThresholdBytes).isEqualTo(cfg.compactionTargetBytes)
                        assertThat(hoglite.minInputFiles).isEqualTo(cfg.compactionMinInputFiles)
                        assertThat(hoglite.maxInputFiles).isEqualTo(cfg.compactionMaxInputFiles)
                        assertThat(selector.uploads(call, "lake", hoglite.uploads)).isSameAs(hoglite.uploads)
                        assertThat(selector.tableCreations(call, "lake", hoglite.tableCreations))
                            .isSameAs(hoglite.tableCreations)
                        assertThat(selector.alters(call, "lake", hoglite.alters)).isSameAs(hoglite.alters)
                        assertThat(selector.views(call, "lake", hoglite.views)).isSameAs(hoglite.views)
                        assertThat(
                            selector.partitionStats(call, "lake", hoglite.partitions),
                        ).isSameAs(hoglite.partitions)
                        assertThat(
                            selector.partitionListing(call, "lake", hoglite.partitions),
                        ).isSameAs(hoglite.partitions)
                        assertThat(selector.partitionLookup(call, "lake", hoglite.partitionLookup))
                            .isSameAs(hoglite.partitionLookup)
                        assertThat(selector.options(call, "lake") { error("must not construct PostgreSQL options") })
                            .isSameAs(hoglite.options)
                        stores.add(listing.store)
                        call.respondText(listing.catalogs!!.single())
                    }
                    get("/v1/catalogs/{catalog}") {
                        val selected = selector.forCatalog(call, call.parameters["catalog"]!!)
                        call.respondText(if (selected === postgres) "postgres" else "hoglite")
                    }
                }
            }
            val credentials = listOf(token(subject = "sa:first"), token(subject = "sa:second"))
            for (credential in credentials) {
                val listing = client.get("/v1/catalogs") { header(HttpHeaders.Authorization, "Bearer $credential") }
                assertThat(listing.status).isEqualTo(HttpStatusCode.OK)
                assertThat(listing.bodyAsText()).isEqualTo("lake")
            }
            assertThat(stores[0]).isNotSameAs(stores[1])
            val first = stores[0] as HogliteCatalogStore
            val second = stores[1] as HogliteCatalogStore
            assertThat(first.client).isNotSameAs(second.client)
            val sharedFields = listOf("http", "tenantRoutes", "breakers", "batchUnsupportedUntilNanos")
            for (field in sharedFields) {
                assertThat(clientState(first.client, field)).isSameAs(clientState(second.client, field))
            }
            val shared = clientState(first.client, "shared") as HogliteClient
            assertThat(shared).isSameAs(clientState(second.client, "shared"))
            first.client.query("opaque_1", "SELECT 1")
            second.client.query("opaque_1", "SELECT 1")
            first.client.query("opaque_1", "SELECT 1")
            shared.query("opaque_1", "SELECT 1")
            val expected =
                listOf("Bearer ${credentials[0]}", "Bearer ${credentials[1]}", "Bearer ${credentials[0]}", null)
            assertThat(observed).hasSize(4)
            assertThat(observed.mapIndexed { index, value -> value == expected[index] })
                .containsExactly(true, true, true, true)
            assertThat(first.partitionLookupAdmission).isSameAs(second.partitionLookupAdmission)
            val otherListing =
                client.get("/v1/catalogs") {
                    header(HttpHeaders.Authorization, "Bearer ${token(backendId = otherId)}")
                }
            assertThat(otherListing.status).isEqualTo(HttpStatusCode.OK)
            val other = stores[2] as HogliteCatalogStore
            assertThat(other).isNotSameAs(first)
            for (field in sharedFields) {
                assertThat(clientState(other.client, field)).isNotSameAs(clientState(first.client, field))
            }
            assertThat(other.partitionLookupAdmission).isNotSameAs(first.partitionLookupAdmission)
            val publication =
                client.get("/v1/catalogs/lake/publications") {
                    header(HttpHeaders.Authorization, "Bearer ${token()}")
                }
            assertThat(publication.status).isEqualTo(HttpStatusCode.NotImplemented)
            assertThat(publication.bodyAsText()).contains("not_supported_on_backend")
            assertThat(publication.headers[HttpHeaders.RetryAfter]).isNull()
            assertThat(
                client.get("/v1/catalogs/other") { header(HttpHeaders.Authorization, "Bearer ${token()}") }.status,
            )
                .isEqualTo(HttpStatusCode.NotFound)
            assertThat(
                client.get("/v1/catalogs/lake") { header(HttpHeaders.Authorization, "Bearer ${token(false)}") }.status,
            )
                .isEqualTo(HttpStatusCode.NotFound)
            assertThat(
                client.get("/v1/catalogs/lake") {
                    header(HttpHeaders.Authorization, "Bearer ${token(backendId = UUID.randomUUID())}")
                }.status,
            )
                .isEqualTo(HttpStatusCode.Unauthorized)
        }

    @Test
    fun `unrouted and anonymous requests keep the configured PostgreSQL default`() =
        testApplication {
            val selector = CatalogStoreSelector(postgres, config("postgres"))
            application {
                routing {
                    get("/v1/catalogs/{catalog}") {
                        assertThat(selector.forCatalog(call, "lake")).isSameAs(postgres)
                        assertThat(selector.forListing(call).store).isSameAs(postgres)
                        call.respondText("postgres")
                    }
                }
            }
            assertThat(client.get("/v1/catalogs/lake").bodyAsText()).isEqualTo("postgres")
        }
}
