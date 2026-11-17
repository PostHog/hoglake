package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.auth.installAuth
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.model.CatalogInfo
import com.posthog.hoglake.observability.CatalogTotals
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.CatalogStoreSelector
import com.posthog.hoglake.store.auditRefusals
import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.testing.testApplication
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.lang.reflect.Proxy
import java.security.interfaces.ECPublicKey
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList

/**
 * What `GET /v1/catalogs` lists, and with which totals, for each auth state
 * the real handler (api/Routes.kt) can meet: `off`, an anonymous caller let
 * through under `warn`, a tenant, a scoped caller, and an anonymous caller
 * refused under `enforce`.
 */
class CatalogListingAuthModesTest {
    private val json = jacksonObjectMapper()
    private val pair = TestTokens.keyPair()
    private val lake = CatalogInfo(1, "lake", "s3://bucket/lake", 11, 3)
    private val theirs = CatalogInfo(2, "theirs", "s3://bucket/theirs", 5, 1)
    private val totals =
        mapOf(
            lake.name to CatalogTotals(13, 1234, 5678, Instant.parse("2026-10-01T00:00:00Z")),
            theirs.name to CatalogTotals(2, 10, 20, Instant.parse("2026-10-02T00:00:00Z")),
        )
    private val totalFields = listOf("table_count", "live_rows", "live_size_bytes", "oldest_snapshot_time")

    /** The `visible` argument of every listing the store was asked for. */
    private val listings = CopyOnWriteArrayList<Set<String>?>()

    private val store =
        Proxy.newProxyInstance(
            CatalogStore::class.java.classLoader,
            arrayOf(CatalogStore::class.java),
        ) { _, method, args ->
            check(method.name == "listCatalogs") { "the listing reached the store for ${method.name}" }
            @Suppress("UNCHECKED_CAST")
            val visible = args[0] as Set<String>?
            listings += visible
            listOf(lake, theirs).filter { visible == null || it.name in visible }
        } as CatalogStore

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

    private fun tenant() = TestTokens.mint(pair, TestTokens.tenantClaims())

    private fun scoped() = TestTokens.mint(pair, TestTokens.scopedClaims())

    private fun list(
        mode: AuthMode,
        token: String?,
    ): HttpResponse {
        var answer: HttpResponse? = null
        testApplication {
            application {
                installAuth(service(mode))
                install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                install(StatusPages) { installErrorMapping() }
                installApiRoutes(CatalogStoreSelector(store), catalogTotals = { totals })
            }
            answer = client.get("/v1/catalogs") { token?.let { header(HttpHeaders.Authorization, "Bearer $it") } }
        }
        return answer!!
    }

    private fun HttpResponse.entries(): List<JsonNode> {
        assertThat(status).isEqualTo(HttpStatusCode.OK)
        return json.readTree(kotlinx.coroutines.runBlocking { bodyAsText() }).toList()
    }

    private fun List<JsonNode>.names() = map { it["name"].asText() }

    /** The entries that carry every totals field, by name; one with only some of them fails. */
    private fun List<JsonNode>.withTotals(): List<String> {
        for (entry in this) {
            val present = totalFields.filter { entry.has(it) }
            assertThat(present).describedAs(entry["name"].asText()).satisfiesAnyOf(
                { assertThat(it).isEmpty() },
                { assertThat(it).isEqualTo(totalFields) },
            )
        }
        return filter { entry -> totalFields.all { entry.has(it) } }.names()
    }

    private fun SimpleMeterRegistry.outcomes(): Map<String, Double> =
        find("hoglake_auth_requests_total").counters()
            .groupBy { "${it.id.getTag("mode")}/${it.id.getTag("outcome")}" }
            .mapValues { (_, c) -> c.sumOf { it.count() } }

    @Test
    fun `under warn an anonymous caller lists every catalog as off does, with no totals, counted once`() {
        val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
        val (entries, lines) = auditRefusals { list(AuthMode.WARN, null).entries() }
        // MUTATION: list `emptySet()` for an anonymous caller and the web UI,
        // which sends no token under `warn`, shows no catalogs.
        assertThat(entries.names()).isEqualTo(listOf("lake", "theirs"))
        // The store filters nothing: the whole roster, as under `off`.
        assertThat(listings).containsExactly(null)
        // MUTATION: strip totals for a scoped caller only and the fleet's
        // sizes go to a caller with no token.
        assertThat(entries.withTotals()).isEmpty()
        // The plugin counted the missing token and let the request through;
        // the store's refusal of "*" is the same request's and adds nothing.
        assertThat(registry.outcomes()).isEqualTo(mapOf("warn/missing" to 1.0))
        assertThat(lines).singleElement().asString().startsWith("auth_refused warn_allowed")
    }

    @Test
    fun `under warn a tenant lists its own catalogs, with their totals`() {
        val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
        val entries = list(AuthMode.WARN, tenant()).entries()
        assertThat(entries.names()).isEqualTo(listOf("lake"))
        assertThat(listings).containsExactly(setOf("lake"))
        assertThat(entries.withTotals()).isEqualTo(listOf("lake"))
        assertThat(registry.outcomes()).isEqualTo(mapOf("warn/ok" to 1.0))
    }

    @Test
    fun `under warn a scoped caller lists its catalog, with no totals`() {
        val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
        val entries = list(AuthMode.WARN, scoped()).entries()
        assertThat(entries.names()).isEqualTo(listOf("lake"))
        assertThat(listings).containsExactly(setOf("lake"))
        assertThat(entries.withTotals()).isEmpty()
        assertThat(registry.outcomes()).isEqualTo(mapOf("warn/ok" to 1.0))
    }

    @Test
    fun `with auth off everything is listed, with totals`() {
        val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
        val entries = list(AuthMode.OFF, null).entries()
        assertThat(entries.names()).isEqualTo(listOf("lake", "theirs"))
        assertThat(listings).containsExactly(null)
        assertThat(entries.withTotals()).isEqualTo(listOf("lake", "theirs"))
        assertThat(registry.outcomes()).isEmpty()
    }

    @Test
    fun `under enforce an anonymous caller is refused before the handler runs`() {
        val registry = SimpleMeterRegistry().also { Metrics.bind(it) }
        val response = list(AuthMode.ENFORCE, null)
        assertThat(response.status).isEqualTo(HttpStatusCode.Unauthorized)
        assertThat(listings).isEmpty()
        assertThat(registry.outcomes()).isEqualTo(mapOf("enforce/missing" to 1.0))
    }
}
