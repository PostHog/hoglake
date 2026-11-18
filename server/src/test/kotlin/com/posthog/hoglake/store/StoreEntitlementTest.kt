package com.posthog.hoglake.store

import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.posthog.hoglake.api.installErrorMapping
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.ProvenTeam
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.auth.installAuth
import com.posthog.hoglake.auth.installNamespaceGate
import com.posthog.hoglake.auth.requireScopedUploadPrefix
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import io.ktor.client.request.header
import io.ktor.client.request.request
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.response.respondText
import io.ktor.server.routing.RoutingContext
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.put
import io.ktor.server.routing.routing
import io.ktor.server.testing.ApplicationTestBuilder
import io.ktor.server.testing.testApplication
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.lang.reflect.Proxy
import java.security.interfaces.ECPublicKey
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.reflect.KVisibility

/**
 * A store is handed out only for a catalog the caller's token allows
 * (auth/StoreEntitlement.kt), for every caller kind and auth mode.
 *
 * The handlers here ask the selector for the catalogs in `X-Handler-Catalog`
 * when the header is present, and for the path's catalog otherwise: a
 * handler that passes a name other than the one the auth plugin decided on.
 */
class StoreEntitlementTest {
    private val pair = TestTokens.keyPair()

    private val store =
        Proxy.newProxyInstance(
            CatalogStore::class.java.classLoader,
            arrayOf(CatalogStore::class.java),
        ) { _, method, _ ->
            error("selection must not call the store: ${method.name}")
        } as CatalogStore
    private val stores = CatalogStoreSelector(store)

    /** Every catalog name the selector handed a store out for. */
    private val handed = CopyOnWriteArrayList<String>()

    @AfterEach
    fun unbindMetrics() = Metrics.clear()

    private fun service(mode: AuthMode) =
        if (mode == AuthMode.OFF) {
            AuthService(mode, null)
        } else {
            AuthService(
                mode,
                JwtVerifier(
                    TestTokens.ISSUER,
                    TestTokens.AUDIENCE,
                    StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
                    predicateFields = setOf("team_id"),
                ) { 1_700_000_100L },
            )
        }

    /** [mode] null installs no auth plugin at all. */
    private fun ApplicationTestBuilder.server(mode: AuthMode?) {
        application {
            mode?.let { installAuth(service(it)) }
            install(ContentNegotiation) { jackson { configureHoglakeWire() } }
            install(StatusPages) { installErrorMapping() }
            routing {
                suspend fun RoutingContext.hand() {
                    val header = call.request.headers["X-Handler-Catalog"]
                    for (name in header?.split(',') ?: listOf(call.parameters["catalog"]!!)) {
                        // The store itself with auth off; under auth, the caller's view of it.
                        val handedOut = stores.forCatalog(call, name)
                        if (mode == null || mode == AuthMode.OFF) {
                            assertThat(handedOut).isSameAs(store)
                        } else {
                            assertThat(handedOut).isNotSameAs(store)
                            assertThat(handedOut.plain()).isSameAs(store)
                        }
                        handed += name
                    }
                    call.respondText("store")
                }
                // READ in the route table.
                get("/v1/catalogs/{catalog}") { hand() }
                // READ, and open to a scoped caller by `cat` alone (AuthPolicy.SCOPED_ROUTES).
                get("/v1/catalogs/{catalog}/namespaces") { hand() }
                // WRITE.
                post("/v1/catalogs/{catalog}/namespaces") { hand() }
                // WRITE, and open to a scoped caller by `cat` alone.
                post("/v1/catalogs/{catalog}/uploads/renew") { hand() }
                // A PUBLIC route: the only way a request with no caller reaches a handler under `enforce`.
                get("/livez") { hand() }
            }
        }
    }

    private suspend fun ApplicationTestBuilder.ask(
        path: String,
        token: String?,
        handler: String? = null,
        method: HttpMethod = HttpMethod.Get,
    ): HttpResponse =
        client.request(path) {
            this.method = method
            token?.let { header(HttpHeaders.Authorization, "Bearer $it") }
            handler?.let { header("X-Handler-Catalog", it) }
        }

    private fun tenant(catalogs: Map<String, String> = mapOf("alpha" to "read")) =
        TestTokens.mint(pair, TestTokens.tenantClaims(catalogs = catalogs.mapValues { listOf(it.value) }))

    private fun scoped(catalogs: String = """{"alpha":["read"]}""") =
        TestTokens.mint(pair, TestTokens.scopedClaims(catalogs = catalogs))

    private fun operator() = TestTokens.mint(pair, TestTokens.operatorClaims())

    private val readAndWrite = mapOf("alpha" to "write", "beta" to "read")

    private fun SimpleMeterRegistry.outcomes(): Map<String, Double> =
        find("hoglake_auth_requests_total").counters()
            .groupBy { "${it.id.getTag("mode")}/${it.id.getTag("outcome")}" }
            .mapValues { (_, c) -> c.sumOf { it.count() } }

    private fun auditLines(block: () -> Unit): List<String> {
        val capture = ListAppender<ILoggingEvent>().apply { start() }
        val audit = org.slf4j.LoggerFactory.getLogger(Audit.LOGGER_NAME) as ch.qos.logback.classic.Logger
        audit.addAppender(capture)
        try {
            block()
        } finally {
            audit.detachAppender(capture)
        }
        return capture.list
            .filter { it.formattedMessage.startsWith("auth_refused ") }
            .map { it.formattedMessage + " " + it.argumentArray.joinToString(" ") }
    }

    @Test
    fun `with auth off, or with no plugin, every name gets the store as before`() {
        for (mode in listOf(null, AuthMode.OFF)) {
            handed.clear()
            testApplication {
                server(mode)
                assertThat(ask("/v1/catalogs/alpha", null).status).isEqualTo(HttpStatusCode.OK)
                assertThat(ask("/v1/catalogs/alpha", null, handler = "beta").status).isEqualTo(HttpStatusCode.OK)
                assertThat(ask("/livez", null, handler = "gamma").status).isEqualTo(HttpStatusCode.OK)
            }
            assertThat(handed).describedAs("mode $mode").containsExactly("alpha", "beta", "gamma")
        }
    }

    @Test
    fun `a tenant gets the store of a catalog its token names, and the plugin's 404 for any other`() {
        for (mode in listOf(AuthMode.ENFORCE, AuthMode.LOCAL)) {
            handed.clear()
            testApplication {
                server(mode)
                assertThat(ask("/v1/catalogs/alpha", tenant()).status).isEqualTo(HttpStatusCode.OK)
                // MUTATION: remove the check from `resolve`, or let a tenant
                // through it, and this hands out the store of `beta`.
                val refused = ask("/v1/catalogs/alpha", tenant(), handler = "beta")
                // The answer the plugin gives the same token on the path of `beta`.
                val atTheDoor = ask("/v1/catalogs/beta", tenant())
                assertThat(refused.status).describedAs("mode $mode").isEqualTo(HttpStatusCode.NotFound)
                assertThat(refused.status).isEqualTo(atTheDoor.status)
                assertThat(refused.bodyAsText()).isEqualTo(atTheDoor.bodyAsText()).contains("catalog 'beta' not found")
                // Another catalog of the same token, with the scope the route asks for, is the token's too.
                val both = tenant(mapOf("alpha" to "read", "beta" to "read"))
                assertThat(ask("/v1/catalogs/alpha", both, handler = "beta").status).isEqualTo(HttpStatusCode.OK)
            }
            assertThat(handed).describedAs("mode $mode").containsExactly("alpha", "beta")
        }
    }

    @Test
    fun `a scoped caller gets the store of a catalog its token names, and a 404 for any other`() {
        // `warn` too: a verified scoped token is refused there as in `enforce`.
        for (mode in listOf(AuthMode.ENFORCE, AuthMode.LOCAL, AuthMode.WARN)) {
            handed.clear()
            testApplication {
                server(mode)
                assertThat(ask("/v1/catalogs/alpha/namespaces", scoped()).status).isEqualTo(HttpStatusCode.OK)
                // MUTATION: let a scoped caller through the check and this hands out the store of `beta`.
                val refused = ask("/v1/catalogs/alpha/namespaces", scoped(), handler = "beta")
                assertThat(refused.status).describedAs("mode $mode").isEqualTo(HttpStatusCode.NotFound)
                assertThat(refused.bodyAsText()).contains("catalog 'beta' not found")
            }
            assertThat(handed).describedAs("mode $mode").containsExactly("alpha")
        }
    }

    @Test
    fun `an operator gets the store of any catalog, on a read route and on a write route`() {
        for (mode in listOf(AuthMode.ENFORCE, AuthMode.LOCAL, AuthMode.WARN)) {
            handed.clear()
            testApplication {
                server(mode)
                assertThat(ask("/v1/catalogs/alpha", operator()).status).isEqualTo(HttpStatusCode.OK)
                assertThat(ask("/v1/catalogs/alpha", operator(), handler = "beta").status).isEqualTo(HttpStatusCode.OK)
                val write = ask("/v1/catalogs/alpha/namespaces", operator(), "gamma", HttpMethod.Post)
                assertThat(write.status).isEqualTo(HttpStatusCode.OK)
            }
            assertThat(handed).describedAs("mode $mode").containsExactly("alpha", "beta", "gamma")
        }
    }

    @Test
    fun `a write route gets no store for a catalog the token holds only for read`() {
        val namespaces = "/v1/catalogs/alpha/namespaces"
        val renew = "/v1/catalogs/alpha/uploads/renew"
        val scopedBoth = scoped("""{"alpha":["write"],"beta":["read"]}""")
        val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
        for (mode in listOf(AuthMode.ENFORCE, AuthMode.LOCAL)) {
            handed.clear()
            testApplication {
                server(mode)
                // The path's catalog, held for write, and the other one on a read route: both served.
                assertThat(ask(namespaces, tenant(readAndWrite), method = HttpMethod.Post).status)
                    .isEqualTo(HttpStatusCode.OK)
                assertThat(ask(namespaces, tenant(readAndWrite), handler = "beta").status).isEqualTo(HttpStatusCode.OK)
                // MUTATION: drop the scope from the store's check and this hands out `beta` for a write.
                val refused = ask(namespaces, tenant(readAndWrite), handler = "beta", method = HttpMethod.Post)
                // The answer the plugin gives the same token for the same write on the path of `beta`.
                val atTheDoor = ask("/v1/catalogs/beta/namespaces", tenant(readAndWrite), method = HttpMethod.Post)
                assertThat(refused.status).describedAs("mode $mode").isEqualTo(HttpStatusCode.Forbidden)
                assertThat(refused.bodyAsText()).isEqualTo(atTheDoor.bodyAsText())
                    .contains("does not hold 'write' on 'beta'")
                // A scoped caller's `cat` carries the scope the same way.
                assertThat(ask(renew, scopedBoth, method = HttpMethod.Post).status).isEqualTo(HttpStatusCode.OK)
                val scopedRefused = ask(renew, scopedBoth, handler = "beta", method = HttpMethod.Post)
                assertThat(scopedRefused.status).describedAs("mode $mode").isEqualTo(HttpStatusCode.Forbidden)
                assertThat(scopedRefused.bodyAsText()).contains("does not hold 'write' on 'beta'")
                // A route that asks for no catalog scope holds the token to the name alone.
                assertThat(ask("/livez", tenant(readAndWrite), handler = "beta").status).isEqualTo(HttpStatusCode.OK)
            }
            assertThat(handed).describedAs("mode $mode").containsExactly("alpha", "beta", "alpha", "beta")
        }
        // The denial is counted against the catalog that was refused: a name from the token's own `cat`.
        val denials =
            registry.find("hoglake_auth_denials_total").counters().filter { it.id.getTag("catalog") == "beta" }
        assertThat(denials.map { "${it.id.getTag("scope")}/${it.id.getTag("reason")}" }).containsOnly("write/forbidden")
        // Per mode: the tenant's and the scoped caller's at the store, and the plugin's own on the path of `beta`.
        assertThat(denials.sumOf { it.count() }).isEqualTo(6.0)
    }

    @Test
    fun `a request with no caller gets no store under enforce`() {
        for (mode in listOf(AuthMode.ENFORCE, AuthMode.LOCAL)) {
            testApplication {
                server(mode)
                // MUTATION: allow by default (a missing caller passes) and this hands out a store.
                val refused = ask("/livez", null, handler = "alpha")
                assertThat(refused.status).describedAs("mode $mode").isEqualTo(HttpStatusCode.Unauthorized)
                assertThat(refused.headers[HttpHeaders.WWWAuthenticate]).isEqualTo("Bearer error=\"invalid_request\"")
            }
        }
        assertThat(handed).isEmpty()
    }

    @Test
    fun `every name a handler asks for is checked, not only the first`() =
        testApplication {
            server(AuthMode.ENFORCE)
            assertThat(ask("/v1/catalogs/alpha", tenant(), handler = "alpha,beta").status)
                .isEqualTo(HttpStatusCode.NotFound)
            assertThat(ask("/v1/catalogs/alpha", tenant(), handler = "alpha,alpha").status).isEqualTo(HttpStatusCode.OK)
            assertThat(handed).containsExactly("alpha", "alpha", "alpha")
        }

    @Test
    fun `a refusal by the selector reaches the caller through the scoped lookups`() =
        testApplication {
            application {
                installAuth(service(AuthMode.ENFORCE))
                installNamespaceGate(stores)
                install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                install(StatusPages) { installErrorMapping() }
                routing {
                    // An upload claim: the handler asks the gate's lookup for the catalog's data_path.
                    put("/v1/catalogs/{catalog}/uploads/{upload}") {
                        call.requireScopedUploadPrefix(call.request.headers["X-Handler-Catalog"]!!, "s3://b/data/ns")
                        call.respondText("claimed")
                    }
                }
            }
            val upload = "uploads/00000000-0000-0000-0000-00000000000c"
            val writer = scoped("""{"alpha":["write"]}""")
            // MUTATION: catch the selector's refusal in the lookup and the answer is a 404 worded otherwise.
            val refused = ask("/v1/catalogs/alpha/$upload", writer, handler = "beta", method = HttpMethod.Put)
            val atTheDoor = ask("/v1/catalogs/beta/$upload", writer, method = HttpMethod.Put)
            assertThat(refused.status).isEqualTo(HttpStatusCode.NotFound)
            assertThat(refused.bodyAsText()).isEqualTo(atTheDoor.bodyAsText()).contains("catalog 'beta' not found")
        }

    @Test
    fun `a refusal is counted and audited once, against the catalog that was refused`() {
        val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
        val lines =
            auditLines {
                testApplication {
                    server(AuthMode.ENFORCE)
                    ask("/v1/catalogs/alpha", tenant(), handler = "beta")
                }
            }
        // The plugin allowed the path's catalog and counted `ok`; the store refused the handler's.
        assertThat(registry.outcomes()).isEqualTo(mapOf("enforce/ok" to 1.0, "enforce/not_entitled" to 1.0))
        // MUTATION: audit the path's catalog and this line says `catalog=alpha`.
        assertThat(lines).singleElement().asString()
            .startsWith("auth_refused refused")
            .contains("catalog=beta", "catalog 'beta' not found", "pattern=/v1/catalogs/{catalog}", "at=store")
            .doesNotContain("catalog=alpha")
        // The series has the plugin's labels and no catalog name.
        assertThat(registry.find("hoglake_auth_requests_total").counters().flatMap { c -> c.id.tags.map { it.key } })
            .containsOnly("mode", "listener", "outcome", "client")
    }

    @Test
    fun `under warn a refusal is counted and audited once for the request, and let through`() {
        val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
        val lines =
            auditLines {
                testApplication {
                    server(AuthMode.WARN)
                    // MUTATION: refuse under `warn` and this is a 404.
                    // Three names the token does not hold, none of them cached by the one before.
                    assertThat(ask("/v1/catalogs/alpha", tenant(), handler = "beta,gamma,beta").status)
                        .isEqualTo(HttpStatusCode.OK)
                }
            }
        assertThat(handed).containsExactly("beta", "gamma", "beta")
        // MUTATION: skip the count under `warn` and `not_entitled` is missing;
        // drop the store's once-per-request mark and it is 3.
        assertThat(registry.outcomes()).isEqualTo(mapOf("warn/ok" to 1.0, "warn/not_entitled" to 1.0))
        assertThat(lines).singleElement().asString()
            .startsWith("auth_refused warn_allowed").contains("catalog=beta", "at=store")
    }

    @Test
    fun `under warn a write for a catalog held only for read is counted and let through`() {
        val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
        testApplication {
            server(AuthMode.WARN)
            val write = ask("/v1/catalogs/alpha/namespaces", tenant(readAndWrite), "beta", HttpMethod.Post)
            assertThat(write.status).isEqualTo(HttpStatusCode.OK)
        }
        assertThat(handed).containsExactly("beta")
        assertThat(registry.outcomes()).isEqualTo(mapOf("warn/ok" to 1.0, "warn/forbidden" to 1.0))
    }

    @Test
    fun `under warn a request with no caller on a public route is counted once and gets the store`() {
        val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
        val lines =
            auditLines {
                testApplication {
                    server(AuthMode.WARN)
                    // The plugin allows a public route for anyone, so it left no mark: the store counts.
                    assertThat(ask("/livez", null, handler = "alpha").status).isEqualTo(HttpStatusCode.OK)
                }
            }
        assertThat(handed).containsExactly("alpha")
        assertThat(registry.outcomes()).isEqualTo(mapOf("warn/ok" to 1.0, "warn/missing" to 1.0))
        assertThat(lines).singleElement().asString()
            .startsWith("auth_refused warn_allowed").contains("catalog=alpha", "at=store")
    }

    @Test
    fun `under warn a request the plugin already counted as refused is not counted again`() {
        val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
        val lines =
            auditLines {
                testApplication {
                    server(AuthMode.WARN)
                    // No token: the plugin counts `missing` and lets it through.
                    assertThat(ask("/v1/catalogs/alpha", null).status).isEqualTo(HttpStatusCode.OK)
                    // A catalog the token does not name: the plugin counts `not_entitled`.
                    assertThat(ask("/v1/catalogs/beta", tenant()).status).isEqualTo(HttpStatusCode.OK)
                    // The same, with a handler that asks for a third catalog.
                    assertThat(ask("/v1/catalogs/beta", tenant(), handler = "gamma").status)
                        .isEqualTo(HttpStatusCode.OK)
                }
            }
        assertThat(handed).containsExactly("alpha", "beta", "gamma")
        // MUTATION: drop the plugin's mark and each of these counts twice.
        assertThat(registry.outcomes()).isEqualTo(mapOf("warn/missing" to 1.0, "warn/not_entitled" to 2.0))
        assertThat(lines).hasSize(3).noneMatch { "at=store" in it }
    }

    @Test
    fun `a proven team is made only by the org-bound seam`() {
        // Its constructor and the seam that calls it are private; the rule is the only way in.
        val proven = ProvenTeam::class
        assertThat(proven.constructors.map { it.visibility }).containsOnly(KVisibility.PRIVATE)
        assertThat(proven.nestedClasses.single { it.simpleName == "ImportNamespaceMark" }.visibility)
            .isEqualTo(KVisibility.PRIVATE)
    }
}
