package com.posthog.hoglake.auth

import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.observability.REQUEST_ID_HEADER
import com.posthog.hoglake.observability.RequestId
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.post
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.security.KeyPair
import java.security.interfaces.ECPublicKey

/**
 * The plugin end to end, through Ktor's test host.
 *
 * The routes registered here are the REAL paths the policy table names, wired to
 * the thinnest possible handlers: the point is to observe what the pipeline does
 * BEFORE a handler runs, which is why several assertions are about a handler that
 * would have answered 200 having never been reached.
 */
class AuthPluginTest {
    private val pair: KeyPair = TestTokens.keyPair()
    private val now = 2_000_000_000L

    @AfterEach
    fun unbindMetrics() = Metrics.clear()

    private fun service(mode: AuthMode): AuthService =
        if (mode == AuthMode.OFF) {
            AuthService(mode, null)
        } else {
            AuthService(
                mode,
                JwtVerifier(
                    TestTokens.ISSUER,
                    TestTokens.AUDIENCE,
                    StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
                ) { now },
            )
        }

    /**
     * A miniature server: the auth plugin, the wire JSON, and one handler per
     * interesting shape of route. `whoami` echoes the resolved actor so a test
     * can see whether a caller was attached at all.
     */
    private fun Application.harness(mode: AuthMode) {
        installAuth(service(mode))
        install(ContentNegotiation) { jackson { configureHoglakeWire() } }
        routing {
            get("/livez") { call.respondText("ok") }
            get("/v1/catalogs") { call.respondText(call.hoglakeAuth.actorOrNull ?: "anonymous") }
            // PUBLIC in the table, so it is reachable in every mode: echoes which
            // of the three states the plugin landed on.
            get("/v1/info") { call.respondText(call.hoglakeAuth::class.simpleName ?: "?") }
            get("/v1/catalogs/{catalog}") { call.respondText("catalog") }
            post("/v1/catalogs/{catalog}/commit") { call.respondText("committed") }
            post("/v1/catalogs/{catalog}/maintenance/compact") { call.respondText("compacted") }
            post("/v1/catalogs") { call.respondText("created") }
            get("/v1/database/health") { call.respondText("health") }
        }
    }

    private fun token(claims: String = TestTokens.tenantClaims()) = TestTokens.mint(pair, claims)

    // ---- off -------------------------------------------------------------

    @Test
    fun `off mode resolves nothing, so every surface answers as it does today`() =
        testApplication {
            application { harness(AuthMode.OFF) }
            assertThat(client.get("/v1/catalogs").bodyAsText()).isEqualTo("anonymous")
            assertThat(client.get("/v1/database/health").status).isEqualTo(HttpStatusCode.OK)
            assertThat(client.post("/v1/catalogs").bodyAsText()).isEqualTo("created")
        }

    @Test
    fun `off is a state of its own, not an anonymous one`() =
        testApplication {
            application { harness(AuthMode.OFF) }
            // The distinction the /v1/info totals regression turned on: under
            // `off` the request is unauthenticated by configuration and every
            // surface must behave as it did before auth existed.
            assertThat(client.get("/v1/info").bodyAsText()).isEqualTo("Off")
        }

    // ---- enforce ---------------------------------------------------------

    @Test
    fun `a request with no token gets a 401 and a bearer challenge`() =
        testApplication {
            application { harness(AuthMode.ENFORCE) }
            val response = client.get("/v1/catalogs")
            assertThat(response.status).isEqualTo(HttpStatusCode.Unauthorized)
            assertThat(response.headers[HttpHeaders.WWWAuthenticate]).isEqualTo("Bearer error=\"invalid_request\"")
            assertThat(response.bodyAsText()).contains("invalid_request")
        }

    @Test
    fun `a refusal carries the request id that the server logged it against`() =
        testApplication {
            // RequestId is installed BEFORE auth in App.module, and this is why:
            // auth refuses by responding from inside its own interceptor, so a
            // plugin installed after it runs on a call whose response is already
            // built. Without the ordering, a customer reporting "I got a 401 at
            // 14:07" cannot be joined to the server's record of it.
            application {
                install(RequestId)
                harness(AuthMode.ENFORCE)
            }
            val refused = client.get("/v1/catalogs")
            assertThat(refused.status).isEqualTo(HttpStatusCode.Unauthorized)
            assertThat(refused.headers[REQUEST_ID_HEADER]).isNotNull()

            // And a request that never reaches a handler is still attributable.
            val allowed =
                client.get("/v1/catalogs") {
                    header(HttpHeaders.Authorization, "Bearer ${token()}")
                }
            assertThat(allowed.status).isEqualTo(HttpStatusCode.OK)
            assertThat(allowed.headers[REQUEST_ID_HEADER]).isNotNull()
            assertThat(allowed.headers[REQUEST_ID_HEADER]).isNotEqualTo(refused.headers[REQUEST_ID_HEADER])
        }

    @Test
    fun `a token that does not verify gets a 401 naming the reason`() =
        testApplication {
            application { harness(AuthMode.ENFORCE) }
            val response =
                client.get("/v1/catalogs") {
                    header(
                        HttpHeaders.Authorization,
                        "Bearer ${TestTokens.mint(TestTokens.keyPair(), TestTokens.tenantClaims())}",
                    )
                }
            assertThat(response.status).isEqualTo(HttpStatusCode.Unauthorized)
            assertThat(response.bodyAsText()).contains("invalid_signature")
        }

    @Test
    fun `a tenant reaches its own catalog and the actor is attached`() =
        testApplication {
            application { harness(AuthMode.ENFORCE) }
            val response = client.get("/v1/catalogs") { header(HttpHeaders.Authorization, "Bearer ${token()}") }
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            assertThat(response.bodyAsText()).isEqualTo("client:hedgerow-prod")
        }

    @Test
    fun `a tenant asking about another catalog gets 404, not 403`() =
        testApplication {
            application { harness(AuthMode.ENFORCE) }
            val response =
                client.get("/v1/catalogs/their-lake") { header(HttpHeaders.Authorization, "Bearer ${token()}") }
            // Indistinguishable from a catalog that does not exist — that is the
            // property, not an accident of the fixture.
            assertThat(response.status).isEqualTo(HttpStatusCode.NotFound)
        }

    @Test
    fun `a reader cannot commit, and the 403 is a scope shortfall not a denial`() =
        testApplication {
            application { harness(AuthMode.ENFORCE) }
            val auth = "Bearer ${token(TestTokens.tenantClaims(catalogs = mapOf("lake" to listOf("read"))))}"
            val response = client.post("/v1/catalogs/lake/commit") { header(HttpHeaders.Authorization, auth) }
            assertThat(response.status).isEqualTo(HttpStatusCode.Forbidden)
            assertThat(response.bodyAsText()).contains("forbidden")
        }

    @Test
    fun `a writer commits but cannot compact`() =
        testApplication {
            application { harness(AuthMode.ENFORCE) }
            val auth = "Bearer ${token(TestTokens.tenantClaims(catalogs = mapOf("lake" to listOf("write"))))}"
            assertThat(client.post("/v1/catalogs/lake/commit") { header(HttpHeaders.Authorization, auth) }.status)
                .isEqualTo(HttpStatusCode.OK)
            assertThat(
                client.post("/v1/catalogs/lake/maintenance/compact") { header(HttpHeaders.Authorization, auth) }.status,
            )
                .isEqualTo(HttpStatusCode.Forbidden)
        }

    @Test
    fun `a tenant cannot reach an operator-only surface even as admin`() =
        testApplication {
            application { harness(AuthMode.ENFORCE) }
            val auth = "Bearer ${token(TestTokens.tenantClaims(catalogs = mapOf("lake" to listOf("admin"))))}"
            assertThat(client.get("/v1/database/health") { header(HttpHeaders.Authorization, auth) }.status)
                .isEqualTo(HttpStatusCode.Forbidden)
            assertThat(client.post("/v1/catalogs") { header(HttpHeaders.Authorization, auth) }.status)
                .isEqualTo(HttpStatusCode.Forbidden)
        }

    @Test
    fun `an operator reaches everything`() =
        testApplication {
            application { harness(AuthMode.ENFORCE) }
            val auth = "Bearer ${token(TestTokens.operatorClaims())}"
            assertThat(client.get("/v1/database/health") { header(HttpHeaders.Authorization, auth) }.status)
                .isEqualTo(HttpStatusCode.OK)
            assertThat(client.get("/v1/catalogs/any-catalog") { header(HttpHeaders.Authorization, auth) }.status)
                .isEqualTo(HttpStatusCode.OK)
        }

    @Test
    fun `public routes stay public, even with a stale credential attached`() =
        testApplication {
            application { harness(AuthMode.ENFORCE) }
            assertThat(client.get("/livez").status).isEqualTo(HttpStatusCode.OK)
            // A probe behind a browser cookie must not start failing.
            assertThat(client.get("/livez") { header(HttpHeaders.Authorization, "Bearer not-a-token") }.status)
                .isEqualTo(HttpStatusCode.OK)
        }

    @Test
    fun `a path with no rule is refused rather than served`() =
        testApplication {
            application { harness(AuthMode.ENFORCE) }
            // Nothing registered this path, so under `enforce` the plugin's
            // deny-by-default is what the caller meets — and that is the point:
            // adding a route without a table entry must fail closed.
            assertThat(client.get("/v1/invented").status).isEqualTo(HttpStatusCode.Unauthorized)
        }

    // ---- warn ------------------------------------------------------------

    @Test
    fun `warn mode refuses nothing but counts what enforce would have refused`() =
        testApplication {
            val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
            Metrics.bind(registry)
            application { harness(AuthMode.WARN) }

            assertThat(client.get("/v1/catalogs").status).isEqualTo(HttpStatusCode.OK)
            assertThat(client.get("/v1/catalogs").bodyAsText()).isEqualTo("anonymous")
            assertThat(client.get("/v1/info").bodyAsText()).isEqualTo("Anonymous")
            assertThat(client.post("/v1/catalogs/lake/maintenance/compact").status).isEqualTo(HttpStatusCode.OK)
            // A token that does not even parse — the other half of the burn-in
            // checklist, and a different counter value on purpose.
            assertThat(client.get("/v1/catalogs") { header(HttpHeaders.Authorization, "Bearer garbage") }.status)
                .isEqualTo(HttpStatusCode.OK)

            val counters = registry.scrape()
            assertThat(counters).contains("hoglake_auth_requests_total")
            assertThat(counters).contains("outcome=\"missing\"")
            assertThat(counters).contains("outcome=\"invalid\"")
        }

    @Test
    fun `warn mode still attaches a caller whose token verified`() =
        testApplication {
            application { harness(AuthMode.WARN) }
            val response = client.get("/v1/catalogs") { header(HttpHeaders.Authorization, "Bearer ${token()}") }
            // The burn-in period has to produce the attribution `enforce` will
            // rely on, so a good token is honoured even while refusals are not.
            assertThat(response.bodyAsText()).isEqualTo("client:hedgerow-prod")
        }
}
