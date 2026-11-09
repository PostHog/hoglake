package com.posthog.hoglake.store

import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.AuthPolicy
import com.posthog.hoglake.auth.Requirement
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.configureApiListener
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import io.ktor.http.HttpMethod
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files

/**
 * Mount dispatch through the real application and its migrated database.
 *
 * The mount's name is also a PostgreSQL catalog here, created below the API
 * (a name taken before the reservation existed). Any route that reached a
 * service instead of the binding would answer as that catalog, so "every
 * route answers the binding's refusal" is a totality check.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MountDispatchIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val pair = TestTokens.keyPair()
    private val keyFile =
        Files.createTempFile("hoglake-mounts-", ".json").also { Files.writeString(it, TestTokens.jwks(pair)) }
    private val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    private val mount = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f0-events"
    private val registry = MountRegistry { if (it == mount) MountRecord(it) else null }

    /** As migrated, before any test writes it. */
    private val migratedSwitch: Boolean =
        db.jdbi.withHandle<Boolean, Exception> {
            it.createQuery("SELECT enabled FROM hog_mount_switch").mapTo(Boolean::class.java).one()
        }

    init {
        val catalogs = CatalogService(db.jdbi)
        catalogs.createCatalog(mount, "s3://bucket/$mount")
        catalogs.createNamespace(mount, "ingest")
        catalogs.createTable(mount, "ingest", "events", listOf(ColumnDef("team_id", ColType.LONG)))
    }

    @AfterAll
    fun close() {
        http.close()
        db.close()
        Files.deleteIfExists(keyFile)
    }

    private fun switch(on: Boolean) =
        db.jdbi.useHandle<Exception> { it.execute("UPDATE hog_mount_switch SET enabled = ?", on) }

    private fun <T> withServer(
        mounts: MountRegistry = registry,
        mode: AuthMode = AuthMode.ENFORCE,
        block: (port: Int) -> T,
    ): T {
        val cfg =
            Config(port = 0, hydratorIntervalMs = 0).copy(
                authMode = mode.wire,
                authIssuer = TestTokens.ISSUER,
                authJwksFile = keyFile.toString(),
                authPredicateFields = "team_id",
            )
        val app = App.build(cfg, db.jdbi, mountRegistry = mounts)
        val server = embeddedServer(Netty, configure = { configureApiListener(cfg) }) { app.module(this) }
        try {
            server.start(wait = false)
            return block(runBlocking { server.engine.resolvedConnectors().first().port })
        } finally {
            server.stop(0, 1000)
            app.close()
        }
    }

    private fun operator() = TestTokens.mint(pair, TestTokens.operatorClaims())

    private fun scoped(): String {
        val iat = System.currentTimeMillis() / 1000 - 5
        return TestTokens.mint(pair, TestTokens.scopedClaims(catalogs = """{"$mount":["write"]}""", iat = iat))
    }

    private fun send(
        port: Int,
        method: HttpMethod,
        path: String,
        token: String?,
    ): HttpResponse<String> {
        val body = if (method == HttpMethod.Get || method == HttpMethod.Delete) "" else "{}"
        val request =
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port$path"))
                .header("Content-Type", "application/json")
                .method(method.value, HttpRequest.BodyPublishers.ofString(body))
        token?.let { request.header("Authorization", "Bearer $it") }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString())
    }

    /** Every catalog route in the route table, with its parameters filled. */
    private fun catalogRoutes(): List<Pair<AuthPolicy.Entry, String>> =
        AuthPolicy.rules.filter { "{catalog}" in it.pattern }.map { rule ->
            rule to
                rule.pattern
                    .replace("{catalog}", mount)
                    .replace("{namespace}", "ingest")
                    .replace("{table}", "events")
                    .replace("{tableUuid}", "00000000-0000-0000-0000-000000000001")
                    .replace("{operation}", "00000000-0000-0000-0000-000000000002")
                    .replace("{upload}", "00000000-0000-0000-0000-000000000003")
                    .replace(Regex("\\{[a-zA-Z]+}"), "x")
        }

    /** A content hash of every `hog_` table: a refused request must leave all of them unchanged. */
    private fun fingerprint(): Map<String, String> =
        db.jdbi.withHandle<Map<String, String>, Exception> { h ->
            h.createQuery("SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'hog_%'")
                .mapTo(String::class.java)
                .list()
                .associateWith {
                    h.createQuery("SELECT md5(coalesce(string_agg(t::text, '|' ORDER BY t::text), '')) FROM $it t")
                        .mapTo(String::class.java)
                        .one()
                }
        }

    @Test
    fun `the switch is migrated off and is read per request, uncached`() {
        assertThat(migratedSwitch).isFalse()
        try {
            withServer { port ->
                val path = "/v1/catalogs/$mount"
                assertThat(send(port, HttpMethod.Get, path, operator()).statusCode()).isEqualTo(200)
                switch(true)
                val refused = send(port, HttpMethod.Get, path, operator())
                assertThat(refused.statusCode()).isEqualTo(404)
                assertThat(refused.body()).contains("catalog '$mount'")
                switch(false)
                assertThat(send(port, HttpMethod.Get, path, operator()).statusCode()).isEqualTo(200)
                // A missing row reads as off.
                db.jdbi.useHandle<Exception> { it.execute("DELETE FROM hog_mount_switch") }
                assertThat(send(port, HttpMethod.Get, path, operator()).statusCode()).isEqualTo(200)
            }
        } finally {
            db.jdbi.useHandle<Exception> {
                it.execute("INSERT INTO hog_mount_switch DEFAULT VALUES ON CONFLICT DO NOTHING")
            }
            switch(false)
        }
    }

    @Test
    fun `every catalog route on a mount is refused before any service call`() {
        switch(true)
        try {
            withServer { port ->
                val before = fingerprint()
                for ((rule, path) in catalogRoutes()) {
                    // An operator passes the plugin everywhere; only the binding stops it.
                    val operator = send(port, rule.method, path, operator())
                    assertThat(operator.statusCode()).describedAs("%s %s: %s", rule.method.value, path, operator.body())
                        .isEqualTo(404)
                    assertThat(operator.body()).describedAs(path)
                        .isEqualTo("""{"error":"not_found","detail":"catalog '$mount'"}""")
                    // A scoped caller naming the mount: the plugin lets it reach
                    // the binding on its own routes, and refuses it elsewhere.
                    val scoped = send(port, rule.method, path, scoped())
                    val route = rule.method to rule.pattern
                    val scopedRoute = route in AuthPolicy.SCOPED_ROUTES || route in AuthPolicy.BODY_SCOPED_ROUTES
                    val expected =
                        when {
                            !scopedRoute -> 403 to "forbidden"
                            rule.requirement == Requirement.READ -> 501 to "mount_operation_unavailable"
                            else -> 403 to "read_only_catalog"
                        }
                    assertThat(scoped.statusCode() to scoped.body().substringAfter("\"error\":\"").substringBefore('"'))
                        .describedAs("%s %s: %s", rule.method.value, path, scoped.body())
                        .isEqualTo(expected)
                }
                assertThat(fingerprint()).isEqualTo(before)
            }
        } finally {
            switch(false)
        }
    }

    @Test
    fun `with the switch off a mount record changes no read`() {
        switch(false)
        val withRecord = withServer { port -> reads(port) }
        val without = withServer(MountRegistry.NONE) { port -> reads(port) }
        assertThat(withRecord).isEqualTo(without)
        assertThat(withRecord.values.map { it.first }).contains(200)
    }

    private fun reads(port: Int): Map<String, Pair<Int, String>> =
        catalogRoutes()
            .filter { (rule, _) -> rule.requirement == Requirement.READ }
            .flatMap { (rule, path) ->
                listOf("operator" to operator(), "scoped" to scoped()).map { (who, token) ->
                    val response = send(port, rule.method, path, token)
                    "$who ${rule.method.value} $path" to (response.statusCode() to response.body())
                }
            }.toMap()

    @Test
    fun `no backend can create a catalog with a mount's name, whatever the switch says`() {
        // On, the refusal must still be the 422: a check placed after
        // dispatch would answer the binding's 404 for the mount instead.
        for (on in listOf(false, true)) {
            switch(on)
            try {
                createRefused()
            } finally {
                switch(false)
            }
        }
    }

    private fun createRefused() {
        withServer(mode = AuthMode.OFF) { port ->
            fun create(name: String): HttpResponse<String> =
                http.send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port/v1/catalogs"))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString("""{"name":"$name","data_path":"s3://b/$name"}"""))
                        .build(),
                    HttpResponse.BodyHandlers.ofString(),
                )
            for (name in listOf(mount, "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f1-events")) {
                val response = create(name)
                assertThat(response.statusCode()).describedAs(response.body()).isEqualTo(422)
                assertThat(response.body()).contains("reserved for mounts")
            }
            val near = create("0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f1-events2")
            assertThat(near.statusCode()).describedAs(near.body()).isIn(201, 409)
        }
    }
}
