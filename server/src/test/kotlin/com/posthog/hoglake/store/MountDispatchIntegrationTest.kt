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
    private val source = "millrace-dev"
    private val postgresId = "7b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e"
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
        // The mount's source, a step ahead of the shadowed catalog's clock.
        catalogs.createCatalog(source, "s3://bucket/millrace/$source")
        catalogs.createNamespace(source, "ingest")
        catalogs.createNamespace(source, "other")
        catalogs.createCatalog("lake", "s3://bucket/lake")
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
        sourceCatalog: String = source,
        block: (port: Int) -> T,
    ): T {
        val cfg =
            Config(port = 0, hydratorIntervalMs = 0).copy(
                mountSourceCatalog = sourceCatalog,
                postgresBackendId = postgresId,
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

    private fun scoped(
        catalogs: String = """{"$mount":["write"]}""",
        extra: String = "",
    ): String {
        val iat = System.currentTimeMillis() / 1000 - 5
        return TestTokens.mint(pair, TestTokens.scopedClaims(catalogs = catalogs, iat = iat, extra = extra))
    }

    private fun tenant(): String {
        val cat = mapOf(mount to listOf("admin"), "lake" to listOf("read"))
        return TestTokens.mint(pair, TestTokens.tenantClaims(catalogs = cat))
    }

    /** The mount as its scoped reader sees it: the source's row under the mount's name. */
    private fun mountBody(): String {
        val row = CatalogService(db.jdbi).getCatalog(source)
        return """{"name":"$mount","data_path":"s3://bucket/millrace/$source",""" +
            """"head_snapshot_id":${row.headSnapshotId},"schema_version":${row.schemaVersion},""" +
            """"capabilities":[],"read_only":true}"""
    }

    private fun names(body: String): List<String> =
        Regex(""""name":"([^"]+)"""").findAll(body).map { it.groupValues[1] }.toList()

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
    fun `every catalog route on a mount answers as its mount column says, before any service call`() {
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
                    // A scoped caller naming the mount: served on the mount
                    // column's routes; elsewhere the plugin lets it reach the
                    // binding on its own routes, and refuses it on the rest.
                    val scoped = send(port, rule.method, path, scoped())
                    if ((rule.method to rule.pattern) in AuthPolicy.MOUNT_ROUTES) {
                        assertThat(scoped.statusCode()).describedAs("%s %s", rule.method.value, path).isEqualTo(200)
                        assertThat(scoped.body()).isEqualTo(mountBody())
                        continue
                    }
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
        // The one change for a reserved name that is no mount: its scoped
        // reader gets the catalog as its listing shows it (main: 403).
        val (status, body) = withRecord.getValue("scoped GET /v1/catalogs/$mount")
        assertThat(status).isEqualTo(200)
        assertThat(body)
            .contains(""""data_path":"s3://bucket/$mount"""", "atomic-table-creation-v1")
            .doesNotContain("read_only")
    }

    private fun reads(port: Int): Map<String, Pair<Int, String>> =
        catalogRoutes()
            .filter { (rule, _) -> rule.requirement == Requirement.READ }
            .map { (rule, path) -> rule.method to path }
            .plus(HttpMethod.Get to "/v1/catalogs")
            .flatMap { (method, path) ->
                listOf("operator" to operator(), "scoped" to scoped(), "tenant" to tenant()).map { (who, token) ->
                    val response = send(port, method, path, token)
                    "$who ${method.value} $path" to (response.statusCode() to response.body())
                }
            }.toMap()

    @Test
    fun `the listing shows a mount only to its scoped reader, and never the catalog it shadows`() {
        switch(true)
        try {
            withServer { port ->
                val reader = scoped("""{"$mount":["read"],"lake":["read"]}""")
                val listed = send(port, HttpMethod.Get, "/v1/catalogs", reader)
                assertThat(listed.statusCode()).isEqualTo(200)
                assertThat(names(listed.body())).containsExactly("lake", mount)
                // The mount entry is exactly what GET answers for it, after the local catalogs.
                assertThat(listed.body()).endsWith(",${mountBody()}]")
                assertThat(send(port, HttpMethod.Get, "/v1/catalogs/$mount", reader).body()).isEqualTo(mountBody())
                // A token that places the mount on this PostgreSQL: the routed listing shows it too.
                val placed = scoped("""{"$mount":["read"]}""", ""","rtg":{"b":"postgres","be":"$postgresId"}""")
                assertThat(send(port, HttpMethod.Get, "/v1/catalogs", placed).body()).isEqualTo("[${mountBody()}]")
                // Everyone else: no mount, and not the catalog it shadows.
                val all = send(port, HttpMethod.Get, "/v1/catalogs", operator()).body()
                assertThat(names(all)).contains("lake", source).doesNotContain(mount)
                assertThat(all).doesNotContain("read_only")
                assertThat(names(send(port, HttpMethod.Get, "/v1/catalogs", tenant()).body())).containsExactly("lake")
            }
            withServer(mode = AuthMode.OFF) { port ->
                val body = send(port, HttpMethod.Get, "/v1/catalogs", null).body()
                assertThat(names(body)).contains("lake", source).doesNotContain(mount)
                assertThat(body).doesNotContain("read_only")
            }
            withServer(mode = AuthMode.WARN) { port ->
                val reader = scoped("""{"$mount":["read"]}""")
                assertThat(send(port, HttpMethod.Get, "/v1/catalogs", reader).body()).isEqualTo("[${mountBody()}]")
                assertThat(send(port, HttpMethod.Get, "/v1/catalogs/$mount", reader).body()).isEqualTo(mountBody())
                assertThat(send(port, HttpMethod.Get, "/v1/catalogs/$mount", null).statusCode()).isEqualTo(404)
                // A scoped caller whose `cat` does not name the mount.
                val other = scoped("""{"lake":["read"]}""")
                assertThat(send(port, HttpMethod.Get, "/v1/catalogs/$mount", other).statusCode()).isEqualTo(404)
                assertThat(names(send(port, HttpMethod.Get, "/v1/catalogs", other).body())).containsExactly("lake")
            }
        } finally {
            switch(false)
        }
    }

    @Test
    fun `a mount with no source catalog answers 500, never a 404 naming another catalog`() {
        switch(true)
        try {
            for (configured in listOf("gone", "")) {
                withServer(sourceCatalog = configured) { port ->
                    for (path in listOf("/v1/catalogs/$mount", "/v1/catalogs")) {
                        val response = send(port, HttpMethod.Get, path, scoped())
                        assertThat(response.statusCode()).describedAs("%s %s", configured, path).isEqualTo(500)
                        assertThat(response.body())
                            .isEqualTo("""{"error":"internal_error","detail":"internal server error"}""")
                    }
                    // A caller who sees no mount never needs the source.
                    assertThat(send(port, HttpMethod.Get, "/v1/catalogs", operator()).statusCode()).isEqualTo(200)
                }
            }
        } finally {
            switch(false)
        }
    }

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
