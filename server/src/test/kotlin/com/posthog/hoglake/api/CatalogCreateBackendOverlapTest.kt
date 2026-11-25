package com.posthog.hoglake.api

import com.posthog.hoglake.Config
import com.posthog.hoglake.PostgresRuntime
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.auth.installAuth
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.CatalogStoreSelector
import com.posthog.hoglake.store.PostgresCatalogStore
import com.sun.net.httpserver.HttpServer
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.Jdbi
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.lang.reflect.Proxy
import java.net.InetSocketAddress
import java.nio.file.Files
import java.nio.file.Path
import java.security.interfaces.ECPublicKey
import java.sql.Connection
import java.sql.SQLException
import java.sql.SQLTransientConnectionException
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

/**
 * A create routed to hoglite checks its data_path against the PostgreSQL
 * catalogs of the same server, through the selector's wiring (#666): the
 * refusal is PostgreSQL's 422, and it lands before the engine sees a
 * request. The list is the configured PostgreSQL backend's, default or
 * lazily opened; a server with no PostgreSQL backend has none to check,
 * and one whose lazy open fails, or whose PostgreSQL stops answering after
 * it opened, answers the retryable 503 rather than create unchecked.
 */
class CatalogCreateBackendOverlapTest {
    @TempDir
    lateinit var dir: Path

    private val id = UUID.fromString("5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")
    private val pair = TestTokens.keyPair()
    private val postgresCatalog = CatalogInfo(1, "pg-lake", "s3://bucket/shared", 11, 3)

    /** The PostgreSQL store: lists its one catalog, and nothing else may be asked of it. */
    private val postgres: CatalogStore =
        Proxy.newProxyInstance(
            CatalogStore::class.java.classLoader,
            arrayOf(CatalogStore::class.java),
        ) { _, method, _ ->
            check(method.name == "listCatalogs") { "unexpected PostgreSQL operation: ${method.name}" }
            listOf(postgresCatalog)
        } as CatalogStore

    /**
     * A create the check lets through: the tenant PUT (the probe's 422 reads
     * as "exists"), then the PRAGMA foreign_keys probe, whose refusal is a 503
     * before any bootstrap (#696).
     */
    private val engineCallsPerCreate = 2

    @Test
    fun `a hoglite create overlapping a PostgreSQL catalog is refused before any engine request`() =
        withServer("postgres") { engineCalls, create ->
            // Equal, trailing-slash, nested-under and parent-of (bucket root).
            val overlapping = listOf("s3://bucket/shared", "s3://bucket/shared/", "s3://bucket/shared/x", "s3://bucket")
            for (bad in overlapping) {
                val response = create(bad)
                assertThat(response.status).describedAs(bad).isEqualTo(HttpStatusCode.UnprocessableEntity)
                assertThat(response.bodyAsText()).describedAs(bad).contains("overlaps catalog 'pg-lake'")
            }
            assertThat(engineCalls.get()).describedAs("no tenant PUT for a refused create").isZero()

            // A disjoint data_path goes on to the engine (whose answer here is the probe's).
            val disjoint = create("s3://bucket/shared2")
            assertThat(disjoint.status).isEqualTo(HttpStatusCode.ServiceUnavailable)
            assertThat(disjoint.bodyAsText()).contains("engine-dispatch-probe")
            assertThat(engineCalls.get()).isEqualTo(engineCallsPerCreate)
        }

    @Test
    fun `a lazily opened PostgreSQL backend is read for the check`() =
        withServer("none", postgresBackendId = "6b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e") { engineCalls, create ->
            // HOGLAKE_DEFAULT_BACKEND=none with HOGLAKE_POSTGRES_BACKEND_ID: the
            // PostgreSQL catalogs exist on this server whether or not a request
            // has opened the backend, so the create opens it and reads them.
            val response = create("s3://bucket/shared/deeper")
            assertThat(response.status).isEqualTo(HttpStatusCode.UnprocessableEntity)
            assertThat(response.bodyAsText()).contains("overlaps catalog 'pg-lake'")
            assertThat(engineCalls.get()).describedAs("no tenant PUT for a refused create").isZero()
        }

    @Test
    fun `a lazily opened PostgreSQL backend that fails to open fails the create closed`() {
        // The real lazy path: PostgresCatalogStore -> CatalogService(jdbi) ->
        // PostgresRuntime.get, whose failed open under HOGLAKE_DEFAULT_BACKEND=none
        // is BackendUnavailable (PostgresRuntime.kt). The pool never opens.
        val opens = AtomicInteger()
        val unavailable = { cfg: Config ->
            val runtime =
                PostgresRuntime(cfg, open = {
                    opens.incrementAndGet()
                    error("connection refused")
                })
            PostgresCatalogStore(
                { CatalogService(runtime.get()) },
                { error("no commit on a failed open") },
                { error("no scan on a failed open") },
            )
        }
        val postgresId = "6b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e"
        withServer("none", postgresBackendId = postgresId, postgres = unavailable) { engineCalls, create ->
            // The PostgreSQL catalogs cannot be read, so nothing is known to be
            // disjoint: the request gets the open's 503, and no tenant is made.
            val response = create("s3://bucket/anything")
            assertThat(response.status).isEqualTo(HttpStatusCode.ServiceUnavailable)
            assertThat(response.bodyAsText()).contains("backend_unavailable")
            assertThat(opens.get()).describedAs("the create opened the backend").isOne()
            assertThat(engineCalls.get()).describedAs("no tenant PUT for an unchecked create").isZero()
        }
    }

    @Test
    fun `a PostgreSQL backend lost after it opened fails the create closed and retryable`() {
        // The pool is open (PostgresRuntime.get returned a Jdbi); PostgreSQL then
        // stops answering. The real read path: PostgresCatalogStore ->
        // CatalogService.listCatalogs -> Jdbi, over connections that fail as
        // Hikari (no connection from the pool) and PgJDBC (a connection that
        // dies under the statement) do.
        val poolTimeout = SQLTransientConnectionException("HikariPool-1 - Connection is not available")
        val dropped = SQLException("An I/O error occurred while sending to the backend.", "08006")
        val shutdown = SQLException("terminating connection due to administrator command", "57P01")
        val lost =
            listOf(
                "pool timeout" to failingStore(open = poolTimeout),
                "dropped connection" to failingStore(statement = dropped),
                "server shutdown" to failingStore(statement = shutdown),
            )
        for ((what, store) in lost) {
            withServer("postgres", postgres = { store }) { engineCalls, create ->
                val response = create("s3://bucket/anything")
                assertThat(response.status).describedAs(what).isEqualTo(HttpStatusCode.ServiceUnavailable)
                assertThat(response.bodyAsText()).describedAs(what).contains("backend_unavailable")
                assertThat(response.headers[HttpHeaders.RetryAfter]).describedAs(what).isNotNull()
                assertThat(engineCalls.get()).describedAs("$what: no tenant PUT for an unchecked create").isZero()
            }
        }
        // A statement PostgreSQL refuses is not an outage: the catch-all 500, and still no tenant.
        val refused = failingStore(statement = SQLException("relation \"hog_catalog\" does not exist", "42P01"))
        withServer("postgres", postgres = { refused }) { engineCalls, create ->
            val response = create("s3://bucket/anything")
            assertThat(response.status).isEqualTo(HttpStatusCode.InternalServerError)
            assertThat(engineCalls.get()).describedAs("no tenant PUT for an unchecked create").isZero()
        }
    }

    /**
     * A PostgreSQL store over a Jdbi whose connection source throws [open],
     * or whose connections open and then throw [statement] from every
     * statement they are asked to prepare.
     */
    private fun failingStore(
        open: SQLException? = null,
        statement: SQLException? = null,
    ): CatalogStore {
        val connection =
            Proxy.newProxyInstance(
                Connection::class.java.classLoader,
                arrayOf(Connection::class.java),
            ) { _, method, _ ->
                when (method.name) {
                    "prepareStatement", "createStatement", "prepareCall" -> throw checkNotNull(statement)
                    "getAutoCommit", "isValid" -> true
                    "isClosed", "isReadOnly" -> false
                    "getTransactionIsolation" -> Connection.TRANSACTION_READ_COMMITTED
                    else -> null
                }
            } as Connection
        val jdbi = Jdbi.create { open?.let { throw it } ?: connection }
        return PostgresCatalogStore(
            { CatalogService(jdbi) },
            { error("no commit after a failed read") },
            { error("no scan after a failed read") },
        )
    }

    @Test
    fun `a server with no PostgreSQL backend reads no catalogs for the check`() =
        withServer("none") { engineCalls, create ->
            // The same overlapping path reaches the engine: there is no list to check it against.
            val response = create("s3://bucket/shared/deeper")
            assertThat(response.status).isEqualTo(HttpStatusCode.ServiceUnavailable)
            assertThat(response.bodyAsText()).contains("engine-dispatch-probe")
            assertThat(engineCalls.get()).isEqualTo(engineCallsPerCreate)
        }

    /**
     * The API over a probe engine, on a server whose default backend is
     * [default], whose PostgreSQL backend, if not the default, is
     * [postgresBackendId] (none when empty), and whose PostgreSQL store is
     * [postgres] (the one-catalog store by default); [body] gets the engine's
     * request count and a routed create of catalog `lake` at a data_path.
     */
    private fun withServer(
        default: String,
        postgresBackendId: String = "",
        postgres: (Config) -> CatalogStore = { this.postgres },
        body: suspend (AtomicInteger, suspend (String) -> HttpResponse) -> Unit,
    ) {
        val engineCalls = AtomicInteger()
        val server =
            HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
                createContext("/") { exchange ->
                    engineCalls.incrementAndGet()
                    exchange.requestBody.close()
                    val bytes = """{"code":"refused","error":"engine-dispatch-probe"}""".toByteArray()
                    exchange.sendResponseHeaders(422, bytes.size.toLong())
                    exchange.responseBody.use { it.write(bytes) }
                }
                start()
            }
        try {
            testApplication {
                val file = dir.resolve("backends.json")
                Files.writeString(
                    file,
                    """{"backends":[{"id":"$id","kind":"hoglite","url":"http://127.0.0.1:${server.address.port}"}]}""",
                )
                // A non-default PostgreSQL backend needs its connection settings at boot (Config).
                val cfg =
                    Config(
                        backendsFile = file.toString(),
                        defaultBackend = default,
                        authMode = "enforce",
                        postgresBackendId = postgresBackendId,
                        jdbcUrl = "jdbc:postgresql://localhost/lake",
                        dbUser = "hoglake",
                        dbPassword = "hoglake",
                    )
                val claims =
                    TestTokens.operatorClaims().dropLast(1) +
                        """, "cat":{"lake":["admin"]}, "rtg":{"b":"hoglite","be":"$id","t":"opaque_1"}}"""
                val routed = TestTokens.mint(pair, claims)
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
                    installApiRoutes(CatalogStoreSelector(postgres(cfg), cfg))
                }
                body(engineCalls) { dataPath ->
                    client.post("/v1/catalogs") {
                        header(HttpHeaders.Authorization, "Bearer $routed")
                        contentType(ContentType.Application.Json)
                        setBody("""{"name":"lake","data_path":"$dataPath"}""")
                    }
                }
            }
        } finally {
            server.stop(0)
        }
    }
}
