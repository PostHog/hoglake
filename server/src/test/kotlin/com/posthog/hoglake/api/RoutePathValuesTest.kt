package com.posthog.hoglake.api

import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthPolicy
import com.posthog.hoglake.auth.AuthService
import com.posthog.hoglake.auth.JwtVerifier
import com.posthog.hoglake.auth.StaticSigningKeys
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.auth.installAuth
import com.posthog.hoglake.auth.installNamespaceGate
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.CatalogStoreSelector
import io.ktor.client.request.header
import io.ktor.client.request.request
import io.ktor.client.request.setBody
import io.ktor.http.ContentType
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.contentType
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.testing.testApplication
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import java.lang.reflect.Proxy
import java.nio.file.Files
import java.nio.file.Path
import java.security.interfaces.ECPublicKey
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.io.path.extension
import kotlin.io.path.readText
import kotlin.io.path.walk

/**
 * Route values come from the path only. A query parameter that has the name
 * of a path segment (`?catalog=`, `?namespace=`, `?table=`, ...) is not a
 * route value: a handler reads `{catalog}`, `{namespace}` and the rest from
 * the path, as the route table does (auth/AuthPolicy.kt).
 *
 * Every catalog route is driven through the real auth plugin and routes with
 * such a query parameter beside each path value. The stores and backends
 * record what they are asked, and the catalog resolver records every catalog
 * name a handler resolves.
 */
class RoutePathValuesTest {
    private val pair = TestTokens.keyPair()

    /** The values of the path, and a different value under the same name in the query. */
    private val path =
        mapOf(
            "catalog" to "alpha",
            "namespace" to "ns_path",
            "table" to "t_path",
            "view" to "v_path",
            "consumer" to "c_path",
            "publication" to "p_path",
            "fileId" to "11",
            "tableUuid" to "00000000-0000-0000-0000-00000000000a",
            "operation" to "00000000-0000-0000-0000-00000000000b",
            "upload" to "00000000-0000-0000-0000-00000000000c",
        )
    private val query =
        mapOf(
            "catalog" to "beta",
            "namespace" to "ns_query",
            "table" to "t_query",
            "view" to "v_query",
            "consumer" to "c_query",
            "publication" to "p_query",
            "fileId" to "987654321",
            "tableUuid" to "10000000-0000-0000-0000-00000000000a",
            "operation" to "10000000-0000-0000-0000-00000000000b",
            "upload" to "10000000-0000-0000-0000-00000000000c",
        )

    /** Routes where `namespace` and `table` are documented query parameters: only their path values are checked. */
    private val namespaceInQuery =
        setOf("/v1/catalogs/{catalog}/stats/partitions", "/v1/catalogs/{catalog}/maintenance/rehydrate")

    /** Every argument a store or backend received, as text. */
    private val received = CopyOnWriteArrayList<String>()

    /** A backend that records its arguments and then fails: what it was asked is all this test reads. */
    private inline fun <reified T : Any> recording(): T =
        Proxy.newProxyInstance(T::class.java.classLoader, arrayOf(T::class.java)) { _, method, args ->
            args?.forEach { received += it.toString() }
            throw IllegalStateException("recording backend: ${method.name}")
        } as T

    private fun operator() = TestTokens.mint(pair, TestTokens.operatorClaims())

    private fun tenant() = TestTokens.mint(pair, TestTokens.tenantClaims(catalogs = mapOf("alpha" to listOf("admin"))))

    private fun routes(): List<AuthPolicy.Entry> = AuthPolicy.rules.filter { "{catalog}" in it.pattern }

    private fun url(rule: AuthPolicy.Entry): String {
        val names = Regex("\\{([a-zA-Z]+)}").findAll(rule.pattern).map { it.groupValues[1] }.toList()
        val filled = names.fold(rule.pattern) { acc, name -> acc.replace("{$name}", path.getValue(name)) }
        val alsoInQuery = names.filterNot { rule.pattern in namespaceInQuery && it != "catalog" } + "catalog"
        return filled + "?" + alsoInQuery.distinct().joinToString("&") { "$it=${query.getValue(it)}" }
    }

    @Test
    fun `a query parameter named like a path segment never reaches a store or a backend as the route's value`() {
        testApplication {
            val selector = CatalogStoreSelector(recording<CatalogStore>())
            application {
                val verifier =
                    JwtVerifier(
                        TestTokens.ISSUER,
                        TestTokens.AUDIENCE,
                        StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey),
                        clock = { 1_700_000_001 },
                    )
                installAuth(AuthService(AuthMode.ENFORCE, verifier))
                installNamespaceGate(selector)
                install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                install(StatusPages) { installErrorMapping() }
                installApiRoutes(selector)
                installScanRoutes(selector)
                installViewRoutes({ recording() }, selector)
                installAlterRoutes({ recording() }, selector)
                installPartitionListingRoutes({ recording() }, selector)
                installPartitionLookupRoutes({ recording() }, selector)
                installPartitionStatsRoutes({ recording() }, selector)
                installTableCreationRoutes({ recording() }, selector)
                installUploadRoutes({ recording() }, selector)
                installPublicationRoutes(selector)
                installMaintenanceRoutes(
                    { recording() },
                    { recording() },
                    { recording() },
                    { recording() },
                    { recording() },
                    { error("recording backend: status") },
                    { error("recording backend: database health") },
                    selector,
                )
            }
            val wrong = mutableListOf<String>()
            for (rule in routes()) {
                for (token in listOf(operator(), tenant())) {
                    received.clear()
                    client.request(url(rule)) {
                        method = rule.method
                        header(HttpHeaders.Authorization, "Bearer $token")
                        if (rule.method != HttpMethod.Get && rule.method != HttpMethod.Delete) {
                            contentType(ContentType.Application.Json)
                            setBody("{}")
                        }
                    }
                    val what = "${rule.method.value} ${rule.pattern}"
                    val fromQuery = received.filter { it in query.values }
                    if (fromQuery.isNotEmpty()) wrong += "$what was handed $fromQuery"
                }
            }
            assertThat(routes()).hasSizeGreaterThan(60)
            assertThat(wrong).isEmpty()
        }
    }

    @Test
    fun `a documented query parameter is still read from the query`() {
        testApplication {
            val selector = CatalogStoreSelector(recording<CatalogStore>())
            application {
                install(ContentNegotiation) { jackson { configureHoglakeWire() } }
                install(StatusPages) { installErrorMapping() }
                installPartitionStatsRoutes({ recording() }, selector)
            }
            client.request("/v1/catalogs/alpha/stats/partitions?namespace=ns_query&table=t_query&catalog=beta")
            assertThat(received).contains("alpha", "ns_query", "t_query").doesNotContain("beta")
        }
    }

    @Test
    fun `no handler reads a route value from the merged parameters`() {
        // `call.parameters` holds the query parameters as well as the path's,
        // so only `pathParam` (Routes.kt), which reads the path's alone, may
        // name a route value. Query values are read from `request.queryParameters`.
        val api = Path.of("src/main/kotlin/com/posthog/hoglake/api")
        check(Files.isDirectory(api)) { "run from the server project directory" }
        val merged = Regex("""(?<![A-Za-z.])parameters\s*(\[|\.get)|call\.parameters|this\.parameters""")
        val offenders =
            api.walk().filter { it.extension == "kt" }.flatMap { file ->
                file.readText().lines().mapIndexedNotNull { i, line ->
                    val code =
                        if (line.trim().startsWith("*") || line.trim().startsWith("/*")) {
                            ""
                        } else {
                            line.substringBefore(
                                "//",
                            )
                        }
                    if (merged.containsMatchIn(code)) "${file.fileName}:${i + 1}: ${line.trim()}" else null
                }
            }.toList()
        assertThat(offenders).isEmpty()
    }
}
