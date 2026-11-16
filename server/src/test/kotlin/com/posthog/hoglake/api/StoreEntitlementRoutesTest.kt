package com.posthog.hoglake.api

import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthPolicy
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.Requirement
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.auth.installAuth
import com.posthog.hoglake.auth.installNamespaceGate
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.CatalogStoreSelector
import io.ktor.client.request.header
import io.ktor.client.request.request
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.response.respondText
import io.ktor.server.routing.route
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Method
import java.lang.reflect.Modifier
import java.lang.reflect.Proxy
import java.security.interfaces.ECPublicKey
import java.util.concurrent.CopyOnWriteArrayList

/**
 * A store is handed out only for a catalog the caller's token allows
 * (auth/StoreEntitlement.kt), behind the auth decision of every catalog
 * route of the route table and through every way the selector hands one out.
 *
 * This drives the selector, not the server's own handlers. Each catalog
 * route of [AuthPolicy.rules] is served here, behind the real auth plugin
 * and namespace gate, by a test handler that asks the selector for a catalog
 * through every one of its per-catalog functions: the catalog of the path,
 * or the one in `X-Handler-Catalog`, which the token does not name.
 * RoutePathValuesTest covers the server's own handlers: they ask for the
 * path's catalog. This covers what the selector answers to one that does not.
 */
class StoreEntitlementRoutesTest {
    private val pair = TestTokens.keyPair()

    private inline fun <reified T : Any> unused(): T = unused(T::class.java) as T

    private fun unused(type: Class<*>): Any =
        Proxy.newProxyInstance(type.classLoader, arrayOf(type)) { _, method, _ ->
            error("handing out a store must not call it: ${method.name}")
        }

    private val selector = CatalogStoreSelector(unused<CatalogStore>())

    /**
     * Every public selector function that takes a call and a catalog name,
     * found by reflection so that a function added later is driven too.
     * `remote` is one of them: with no mount host it answers null for a name
     * the token allows, which counts here as an answer.
     */
    private val perCatalog: List<Method> =
        CatalogStoreSelector::class.java.methods.filter { m ->
            !m.isSynthetic && !Modifier.isStatic(m.modifiers) &&
                m.parameterTypes.size in 2..3 &&
                m.parameterTypes[0] == ApplicationCall::class.java && m.parameterTypes[1] == String::class.java
        }

    /** `<function>(<catalog>)` for every call the selector answered instead of refusing. */
    private val handed = CopyOnWriteArrayList<String>()

    /** Ask for [catalog] through every function; the refusals come back, the rest is recorded in [handed]. */
    private fun askEvery(
        call: ApplicationCall,
        catalog: String,
    ): List<HoglakeException> =
        perCatalog.mapNotNull { m ->
            val backend =
                m.parameterTypes.getOrNull(2)?.let { type ->
                    // The PostgreSQL service, given as an instance or as a provider of one.
                    if (type == Function0::class.java) ({ unused(m.returnType) }) else unused(type)
                }
            try {
                if (backend == null) m.invoke(selector, call, catalog) else m.invoke(selector, call, catalog, backend)
                handed += "${m.name}($catalog)"
                null
            } catch (e: InvocationTargetException) {
                e.targetException as? HoglakeException ?: throw e.targetException
            }
        }

    private fun routes(): List<AuthPolicy.Entry> = AuthPolicy.rules.filter { "{catalog}" in it.pattern }

    private fun url(rule: AuthPolicy.Entry): String =
        Regex("\\{([a-zA-Z]+)}").replace(rule.pattern) { if (it.groupValues[1] == "catalog") "alpha" else "x" }

    private fun tenant() = TestTokens.mint(pair, TestTokens.tenantClaims(catalogs = mapOf("alpha" to listOf("admin"))))

    private fun scoped() = TestTokens.mint(pair, TestTokens.scopedClaims(catalogs = """{"alpha":["write"]}"""))

    private fun operator() = TestTokens.mint(pair, TestTokens.operatorClaims())

    @Test
    fun `no catalog route hands out a store for a catalog the token does not name`() = drive(AuthMode.ENFORCE)

    @Test
    fun `under warn the same holds for a scoped caller, and a tenant is let through`() = drive(AuthMode.WARN)

    private fun drive(mode: AuthMode) {
        testApplication {
            application {
                val verifier =
                    JwtVerifier(
                        TestTokens.ISSUER,
                        TestTokens.AUDIENCE,
                        StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
                        predicateFields = setOf("team_id"),
                    ) { 1_700_000_100L }
                installAuth(AuthService(mode, verifier))
                // Every namespace is the scoped caller's own (team 42), so it reaches the scoped routes.
                installNamespaceGate(dataPath = { "s3://bucket/alpha" }) { _, _ -> NamespaceContainment("team_id", 42) }
                install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                install(StatusPages) { installErrorMapping() }
                routing {
                    for (rule in routes()) {
                        route(rule.pattern, rule.method) {
                            handle {
                                val asked = call.request.headers["X-Handler-Catalog"] ?: "alpha"
                                val refusals = askEvery(call, asked)
                                // All refused: answer as the selector did. All handed out: say so.
                                refusals.firstOrNull()?.let { if (refusals.size == perCatalog.size) throw it }
                                call.respondText("reached ${refusals.size}")
                            }
                        }
                    }
                }
            }
            val everyFunction = perCatalog.map { it.name }.sorted()
            val wrong = mutableListOf<String>()
            var refusedAtTheStore = 0
            for (rule in routes()) {
                val what = "${rule.method.value} ${rule.pattern}"
                for ((kind, token) in listOf("tenant" to tenant(), "scoped" to scoped(), "operator" to operator())) {
                    // The path's catalog first: which callers reach this route's handler at all.
                    handed.clear()
                    val own =
                        client.request(url(rule)) {
                            method = rule.method
                            header(HttpHeaders.Authorization, "Bearer $token")
                        }
                    val reaches = own.status == HttpStatusCode.OK
                    if (reaches && (own.bodyAsText() != "reached 0" || handed.size != perCatalog.size)) {
                        wrong += "$what as $kind: its own catalog was refused: ${own.bodyAsText()} $handed"
                    }
                    val expected =
                        when (kind) {
                            "operator" -> true
                            // `warn` lets a tenant through the plugin on every route.
                            "tenant" -> !mode.isEnforcing || rule.requirement != Requirement.OPERATOR
                            else ->
                                (rule.method to rule.pattern).let {
                                    it in AuthPolicy.SCOPED_ROUTES || it in AuthPolicy.BODY_SCOPED_ROUTES
                                }
                        }
                    if (reaches != expected) wrong += "$what as $kind: handler reached=$reaches, expected $expected"

                    // The same request, with a handler that asks for a catalog the token does not name.
                    handed.clear()
                    val other =
                        client.request(url(rule)) {
                            method = rule.method
                            header(HttpHeaders.Authorization, "Bearer $token")
                            header("X-Handler-Catalog", "beta")
                        }
                    if (kind == "operator" || (kind == "tenant" && !mode.isEnforcing)) {
                        // An operator's authority is every catalog, as at the door;
                        // a tenant's refusal under `warn` is counted and let through.
                        if (handed.map { it.substringBefore('(') }.sorted() != everyFunction) {
                            wrong += "$what as $kind: beta was not handed out by every function: $handed"
                        }
                    } else {
                        // MUTATION: remove the check from `resolve` and every route a
                        // tenant or a scoped caller reaches hands out `beta`; remove
                        // it from `remote` and that one function answers for `beta`.
                        if (handed.isNotEmpty()) wrong += "$what as $kind was handed $handed"
                        if (reaches) {
                            refusedAtTheStore++
                            if (other.status != HttpStatusCode.NotFound ||
                                "catalog 'beta' not found" !in other.bodyAsText()
                            ) {
                                wrong += "$what as $kind: answered ${other.status} ${other.bodyAsText()}"
                            }
                        }
                    }
                }
            }
            assertThat(wrong).isEmpty()
            // Not vacuous: the routes, the functions, and the refusals that came from the store.
            assertThat(routes()).hasSizeGreaterThan(60)
            assertThat(everyFunction)
                .contains("forCatalog", "resolve", "remote", "requirePostgres", "uploads", "compaction")
                .hasSizeGreaterThan(25)
            // Under `warn` only the scoped caller is refused.
            assertThat(refusedAtTheStore).isGreaterThan(if (mode.isEnforcing) routes().size else 30)
        }
    }
}
