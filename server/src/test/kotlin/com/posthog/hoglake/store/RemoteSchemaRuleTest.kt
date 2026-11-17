package com.posthog.hoglake.store

import com.posthog.hoglake.api.installErrorMapping
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.Caller
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.Scope
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.auth.installAuth
import com.posthog.hoglake.auth.readsRemoteSchema
import com.posthog.hoglake.auth.remoteSchemasAmong
import com.posthog.hoglake.auth.requireRemoteSchemaEntitlement
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.observability.Metrics
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
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.security.interfaces.ECPublicKey

/**
 * The rule that hands out a remote schema on its own
 * (auth/StoreEntitlement.kt), with callers built by hand: each clause is
 * checked apart from the verifier and the route table, which make some of
 * them unreachable through a request.
 */
class RemoteSchemaRuleTest {
    private fun caller(
        kind: Caller.Kind = Caller.Kind.SCOPED,
        cat: Map<String, Set<Scope>> = mapOf("lake" to setOf(Scope.READ)),
        schemas: Set<String> = setOf("posthog"),
    ) = Caller("sub", "org", kind, cat, "jti", "cli", remoteSchemas = schemas)

    @Test
    fun `the reader is a scoped caller whose cat reads the catalog and whose claim names the schema`() {
        assertThat(caller().readsRemoteSchema("lake", "posthog")).isTrue()
        // A write scope on the catalog reads it too.
        assertThat(caller(cat = mapOf("lake" to setOf(Scope.WRITE))).readsRemoteSchema("lake", "posthog")).isTrue()
    }

    @Test
    fun `every clause is needed, and nothing is allowed by default`() {
        val none: Caller? = null
        assertThat(none.readsRemoteSchema("lake", "posthog")).isFalse()
        // The kind, for a caller that carries the claim: a tenant token may, and is still not served.
        assertThat(caller(kind = Caller.Kind.TENANT).readsRemoteSchema("lake", "posthog")).isFalse()
        assertThat(caller(kind = Caller.Kind.OPERATOR).readsRemoteSchema("lake", "posthog")).isFalse()
        // The catalog: another one, or this one with no scope.
        assertThat(caller(cat = mapOf("pond" to setOf(Scope.READ))).readsRemoteSchema("lake", "posthog")).isFalse()
        assertThat(caller(cat = mapOf("lake" to emptySet())).readsRemoteSchema("lake", "posthog")).isFalse()
        assertThat(caller().readsRemoteSchema("pond", "posthog")).isFalse()
        // The claim: absent, another schema, or a case variant.
        assertThat(caller(schemas = emptySet()).readsRemoteSchema("lake", "posthog")).isFalse()
        assertThat(caller(schemas = setOf("imports")).readsRemoteSchema("lake", "posthog")).isFalse()
        assertThat(caller(schemas = setOf("PostHog")).readsRemoteSchema("lake", "posthog")).isFalse()
    }

    @AfterEach
    fun unbindMetrics() = Metrics.clear()

    @Test
    fun `a caller refused for the catalog gets that answer, and the schema is never looked at`() {
        val pair = TestTokens.keyPair()
        val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
        testApplication {
            application {
                val keys = StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey)
                val verifier =
                    JwtVerifier(
                        TestTokens.ISSUER,
                        TestTokens.AUDIENCE,
                        keys,
                        predicateFields = setOf("team_id"),
                        sessionProfile = true,
                    ) { 1_700_000_100L }
                installAuth(AuthService(AuthMode.ENFORCE, verifier))
                install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                install(StatusPages) { installErrorMapping() }
                routing {
                    // A handler that asks for the schema in the catalog its header names, not the path's:
                    // the auth plugin decided on `lake` only.
                    get("/v1/catalogs/{catalog}/namespaces") {
                        val asked = call.request.headers["X-Handler-Catalog"]!!
                        call.requireRemoteSchemaEntitlement(asked, "posthog", "posthog", "posthog")
                        call.respondText("entitled")
                    }
                }
            }
            val claims =
                TestTokens.scopedClaims(
                    catalogs = """{"lake":["read"]}""",
                    extra = TestTokens.sessionProfile("lake", """{"posthog":["read"]}"""),
                )
            val token = TestTokens.mint(pair, claims)

            suspend fun ask(catalog: String) =
                client.get("/v1/catalogs/lake/namespaces") {
                    header(HttpHeaders.Authorization, "Bearer $token")
                    header("X-Handler-Catalog", catalog)
                }
            assertThat(ask("lake").bodyAsText()).isEqualTo("entitled")
            val other = ask("pond")
            assertThat(other.status).isEqualTo(HttpStatusCode.NotFound)
            assertThat(other.bodyAsText()).contains("catalog 'pond' not found").doesNotContain("namespace")
        }
        // Nothing was counted against the schema: its rule did not run for `pond`.
        assertThat(registry.find("hoglake_mount_refusals_total").counters()).isEmpty()
    }

    @Test
    fun `a listing shows a caller exactly the schemas the rule would let it read`() {
        val configured = mapOf("posthog" to "posthog", "imports" to "imports")
        val both = setOf("posthog", "imports", "other")
        assertThat(caller(schemas = both).remoteSchemasAmong("lake", configured)).containsExactly("imports", "posthog")
        assertThat(caller().remoteSchemasAmong("lake", configured)).containsExactly("posthog")
        assertThat(caller(schemas = emptySet()).remoteSchemasAmong("lake", configured)).isEmpty()
        assertThat(caller().remoteSchemasAmong("pond", configured)).isEmpty()
        assertThat(caller(kind = Caller.Kind.TENANT).remoteSchemasAmong("lake", configured)).isEmpty()
        assertThat(caller(kind = Caller.Kind.OPERATOR).remoteSchemasAmong("lake", configured)).isEmpty()
        val none: Caller? = null
        assertThat(none.remoteSchemasAmong("lake", configured)).isEmpty()
        assertThat(caller().remoteSchemasAmong("lake", emptyMap())).isEmpty()
    }

    @Test
    fun `an alias is checked by the host's name and listed by the local one`() {
        // Local `analytics` is the host's `shared`; local `posthog` is the host's `analytics`.
        val configured = mapOf("analytics" to "shared", "posthog" to "analytics")
        assertThat(caller(schemas = setOf("shared")).remoteSchemasAmong("lake", configured))
            .containsExactly("analytics")
        assertThat(caller(schemas = setOf("analytics")).remoteSchemasAmong("lake", configured))
            .containsExactly("posthog")
        assertThat(caller(schemas = setOf("posthog")).remoteSchemasAmong("lake", configured)).isEmpty()
    }
}
