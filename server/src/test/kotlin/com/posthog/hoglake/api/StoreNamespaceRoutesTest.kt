package com.posthog.hoglake.api

import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthPolicy
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.RefusalCountedKey
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.auth.installAuth
import com.posthog.hoglake.auth.installNamespaceGate
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.service.TableCreationBackend
import com.posthog.hoglake.store.BoundStoreCalls
import com.posthog.hoglake.store.BoundStoreCalls.missing
import com.posthog.hoglake.store.CatalogStoreSelector
import com.posthog.hoglake.store.RecordingStores
import com.posthog.hoglake.store.Rule
import com.posthog.hoglake.store.auditRefusals
import io.ktor.client.request.header
import io.ktor.client.request.request
import io.ktor.client.request.setBody
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.http.contentType
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.ApplicationCallPipeline
import io.ktor.server.application.call
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.response.respondText
import io.ktor.server.routing.get
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.security.interfaces.ECPublicKey
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList

/**
 * A scoped caller's view of the store is bound to the namespaces its token
 * allows (auth/StoreGrant.kt), behind every route of the route table a scoped
 * caller may reach.
 *
 * This drives the views, not the server's own handlers, which pass the
 * values of their route (RoutePathValuesTest). Each route is served here,
 * behind the real auth plugin and the real namespace gate, by a test handler
 * that asks the store and every sub-backend the selector hands out for every
 * operation that names a namespace: the namespace of the path, or the one in
 * `X-Handler-Namespace`. The path's namespace is always `mine`, which the
 * token holds for write.
 */
class StoreNamespaceRoutesTest {
    private val pair = TestTokens.keyPair()

    /** The scoped token holds team 42 for write and team 7 for read. */
    private val contained =
        mapOf(
            "mine" to NamespaceContainment("team_id", 42),
            "shared" to NamespaceContainment("team_id", 7),
            "theirs" to NamespaceContainment("team_id", 99),
        )
    private val readable = setOf("mine", "shared")
    private val writable = setOf("mine")

    /** The operations that name a namespace: in a parameter, in a body, or behind an id. */
    private val named =
        BoundStoreCalls.ops.filter {
            it.rule in setOf(Rule.READ, Rule.WRITE, Rule.BODY, Rule.ID_READ, Rule.ID_WRITE, Rule.READ_OR_CLOSED)
        }

    /** `<operation>` for every call of the current request that the view answered with a refusal, and with which. */
    private val refused = CopyOnWriteArrayList<Pair<BoundStoreCalls.Op, HoglakeException>>()

    private val scopedRoutes = AuthPolicy.SCOPED_ROUTES + AuthPolicy.BODY_SCOPED_ROUTES

    private fun routes(): List<AuthPolicy.Entry> = AuthPolicy.rules.filter { (it.method to it.pattern) in scopedRoutes }

    private fun url(rule: AuthPolicy.Entry): String =
        Regex("\\{([a-zA-Z]+)}").replace(rule.pattern) {
            when (it.groupValues[1]) {
                "catalog" -> "alpha"
                "namespace" -> "mine"
                else -> "x"
            }
        }

    private fun scoped() = TestTokens.mint(pair, TestTokens.scopedClaims(catalogs = """{"alpha":["write"]}"""))

    private fun tenant() = TestTokens.mint(pair, TestTokens.tenantClaims(catalogs = mapOf("alpha" to listOf("admin"))))

    private fun verifier() =
        JwtVerifier(
            TestTokens.ISSUER,
            TestTokens.AUDIENCE,
            StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
            predicateFields = setOf("team_id"),
        ) { 1_700_000_100L }

    @AfterEach
    fun unbindMetrics() = Metrics.clear()

    @Test
    fun `on every route a scoped caller reaches, the store refuses a namespace other than the token's`() =
        drive(AuthMode.ENFORCE)

    @Test
    fun `under warn the same holds`() = drive(AuthMode.WARN)

    private fun drive(mode: AuthMode) {
        val stores = RecordingStores(contained)
        testApplication {
            val selector = CatalogStoreSelector(stores.store)
            application {
                installAuth(AuthService(mode, verifier()))
                // The gate reads containment through the selector, as the server wires it (App.kt).
                installNamespaceGate(selector)
                install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                install(StatusPages) { installErrorMapping() }
                routing {
                    for (rule in routes()) {
                        route(rule.pattern, rule.method) {
                            handle {
                                val namespace = call.request.headers["X-Handler-Namespace"] ?: "mine"
                                val views = BoundStoreCalls.views(selector, call, "alpha", stores)
                                for (op in named) {
                                    val given = BoundStoreCalls.arguments(op, "alpha", namespace)
                                    BoundStoreCalls.call(
                                        views.getValue(op.type),
                                        op,
                                        given,
                                    )?.let { refused += op to it }
                                }
                                // All refused: answer as the store did. Otherwise say how many were.
                                if (refused.size == named.size) throw refused.first().second
                                call.respondText("refused ${refused.size}")
                            }
                        }
                    }
                }
            }
            val wrong = mutableListOf<String>()
            var refusedAtTheStore = 0
            for (rule in routes()) {
                val what = "${rule.method.value} ${rule.pattern}"

                suspend fun ask(
                    token: String,
                    namespace: String?,
                ): Pair<HttpStatusCode, String> {
                    stores.calls.clear()
                    refused.clear()
                    val response =
                        client.request(url(rule)) {
                            method = rule.method
                            header(HttpHeaders.Authorization, "Bearer $token")
                            namespace?.let { header("X-Handler-Namespace", it) }
                        }
                    return response.status to response.bodyAsText()
                }

                // The path's own namespace: the handler is reached and every operation is served.
                val own = ask(scoped(), null)
                if (own != HttpStatusCode.OK to "refused 0") wrong += "$what: its own namespace: $own $refused"
                if (stores.calls.count { it.name in named.map { op -> op.name } } < named.size) {
                    wrong += "$what: not every operation reached the store"
                }
                // The gate (or the first check) read the containment; no later check read it again.
                val lookups = stores.calls.filter { it.name == "CatalogStore.liveContainment" }.map { it.arguments }
                if (lookups != listOf(listOf("alpha", "mine"))) wrong += "$what: containment was read as $lookups"

                // A namespace of another team, and one that is not contained: nothing is served.
                for (other in listOf("theirs", "plain")) {
                    val answer = ask(scoped(), other)
                    // MUTATION: hand a scoped caller the store itself, or drop the
                    // namespace rule from a view's method, and that method is served.
                    if (answer.first != HttpStatusCode.NotFound || missing(named.first(), other) !in answer.second) {
                        wrong += "$what with '$other': answered $answer"
                    }
                    refused.filterNot { (op, refusal) ->
                        refusal is HoglakeException.NotFound && refusal.message == missing(op, other)
                    }.forEach { wrong += "$what with '$other': ${it.first} answered ${it.second}" }
                    refusedAtTheStore += refused.size
                    val served =
                        stores.calls.map { it.name }.filter { name ->
                            // A creation's own read comes before its namespace is known; it is not shown.
                            name != "TableCreationBackend.status" && named.any { it.name == name }
                        }
                    if (served.isNotEmpty()) wrong += "$what with '$other' reached the store: $served"
                }

                // A namespace the token holds for read only: read from the token, whatever the route's method.
                val shared = ask(scoped(), "shared")
                val expectedRefused = named.filterNot { BoundStoreCalls.reaches(it, "shared", readable, writable) }
                if (shared.first != HttpStatusCode.OK || refused.map { it.first } != expectedRefused) {
                    wrong += "$what with 'shared': $shared, refused ${refused.map { it.first }}"
                }

                // A tenant is not held to namespaces: unchanged.
                val asTenant = ask(tenant(), "theirs")
                if (asTenant != HttpStatusCode.OK to "refused 0") wrong += "$what as a tenant: $asTenant $refused"
                if (stores.count("CatalogStore.liveContainment") != 0) wrong += "$what as a tenant read containment"
            }
            assertThat(wrong).isEmpty()
            // Not vacuous: every route open to a scoped caller, every operation that names a namespace.
            assertThat(routes()).hasSameSizeAs(scopedRoutes).hasSizeGreaterThan(30)
            assertThat(named).hasSizeGreaterThan(20)
            assertThat(refusedAtTheStore).isEqualTo(routes().size * 2 * named.size)
        }
    }

    @Test
    fun `a table creation in another namespace is refused by the route's own check, as one that does not exist`() {
        val stores = RecordingStores(contained)
        val wrong = mutableListOf<String>()
        testApplication {
            val selector = CatalogStoreSelector(stores.store)
            application {
                installAuth(AuthService(AuthMode.ENFORCE, verifier()))
                installNamespaceGate(selector)
                install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                install(StatusPages) { installErrorMapping() }
                installTableCreationRoutes(
                    { stores.backend(TableCreationBackend::class) as TableCreationBackend },
                    selector,
                )
            }

            suspend fun answer(
                method: HttpMethod,
                path: String,
            ): Pair<HttpResponse, List<String>> =
                auditRefusals {
                    client.request(path) {
                        this.method = method
                        header(HttpHeaders.Authorization, "Bearer ${scoped()}")
                        contentType(ContentType.Application.Json)
                        if (method == HttpMethod.Post) setBody("""{"files":[]}""")
                    }
                }
            for ((method, suffix) in listOf(
                HttpMethod.Get to "",
                HttpMethod.Post to "/commit",
                HttpMethod.Post to "/commit/uploads",
                HttpMethod.Post to "/abort",
            )) {
                for (other in listOf("theirs", "plain")) {
                    val what = "${method.value} $suffix in '$other'"
                    stores.calls.clear()
                    val foreign = BoundStoreCalls.creationId(other)
                    val (refused, refusal) = answer(method, "/v1/catalogs/alpha/table-creations/$foreign$suffix")
                    val asked = stores.calls.map { it.name }.filter { it.startsWith("TableCreationBackend.") }
                    val unknown = UUID.randomUUID()
                    val (absent, none) = answer(method, "/v1/catalogs/alpha/table-creations/$unknown$suffix")
                    // MUTATION: answer with the namespace's 404 at the route's check and the bodies differ.
                    val bodies =
                        listOf(refused to foreign, absent to unknown).map { (response, id) ->
                            response.status to response.bodyAsText().replace(id.toString(), "<id>")
                        }
                    if (bodies[0].first != HttpStatusCode.NotFound || bodies[0] != bodies[1]) wrong += "$what: $bodies"
                    // MUTATION: read the creation through the handler's view in the route's
                    // check and the view refuses first, audited `at=store`.
                    if (refusal.size != 1 || "at=store" in refusal.single()) wrong += "$what: audited $refusal"
                    if ("namespace '$other' in catalog 'alpha'" !in refusal.singleOrNull().orEmpty()) {
                        wrong += "$what: the audit does not name the namespace: $refusal"
                    }
                    if (none.isNotEmpty()) wrong += "$what: a missing creation was audited: $none"
                    // Read once, by the route's check, and nothing done.
                    if (asked != listOf("TableCreationBackend.status")) wrong += "$what: the backend was asked $asked"
                }
            }
        }
        assertThat(wrong).isEmpty()
    }

    @Test
    fun `the namespace gate does not count again a refusal its lookup already counted`() {
        val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
        testApplication {
            application {
                installAuth(AuthService(AuthMode.ENFORCE, verifier()))
                // Stands for a lookup that counts its own refusal, as the store's view does (auth/StoreGrant.kt);
                // the server's own wiring does not reach this today.
                intercept(ApplicationCallPipeline.Plugins) { call.attributes.put(RefusalCountedKey, Unit) }
                installNamespaceGate(containment = { _, _ -> throw HoglakeException.NotFound("catalog 'alpha'") })
                install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                install(StatusPages) { installErrorMapping() }
                routing { get("/v1/catalogs/{catalog}/namespaces/{namespace}") { call.respondText("served") } }
            }
            val response =
                client.request("/v1/catalogs/alpha/namespaces/mine") {
                    header(HttpHeaders.Authorization, "Bearer ${scoped()}")
                }
            assertThat(response.status).isEqualTo(HttpStatusCode.NotFound)
        }
        // MUTATION: drop the gate's guard and it counts a second refusal for the request.
        assertThat(registry.find("hoglake_auth_requests_total").counters().sumOf { it.count() }).isZero()
    }
}
