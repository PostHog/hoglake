package com.posthog.hoglake.store

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.fasterxml.jackson.databind.JsonNode
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.configureApiListener
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.wireObjectMapper
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.jdbi.v3.core.Handle
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * The mount API (plan 1.4) through the real application and its migrated
 * database, driven with the request shapes hogtower's events mount loop
 * (#138) sends: GET, then PUT `{revision, enabled, values}` at the revision
 * it read, and a list on repair passes.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MountApiIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val pair = TestTokens.keyPair()
    private val keyFile =
        Files.createTempFile("hoglake-mount-api-", ".json").also { Files.writeString(it, TestTokens.jwks(pair)) }
    private val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    private val json = wireObjectMapper()
    private val store = MountStore({ db.jdbi }, { true })

    private fun org(n: Int) = "0190f3c4-1b2a-4c3d-8e4f-%012x".format(n)

    private fun mount(n: Int) = "${org(n)}-events"

    @AfterAll
    fun close() {
        http.close()
        db.close()
        Files.deleteIfExists(keyFile)
    }

    private fun <T> withServer(
        mode: AuthMode = AuthMode.ENFORCE,
        allowlist: Set<String> = setOf("*"),
        writer: String = WRITER,
        block: (port: Int) -> T,
    ): T {
        val cfg =
            Config(port = 0, hydratorIntervalMs = 0).copy(
                authMode = mode.wire,
                authIssuer = TestTokens.ISSUER,
                authJwksFile = keyFile.toString(),
                authPredicateFields = "team_id",
                mountAllowlist = allowlist,
                mountWriterSubject = writer,
            )
        val app = App.build(cfg, db.jdbi)
        val server = embeddedServer(Netty, configure = { configureApiListener(cfg) }) { app.module(this) }
        try {
            server.start(wait = false)
            return block(runBlocking { server.engine.resolvedConnectors().first().port })
        } finally {
            server.stop(0, 1000)
            app.close()
        }
    }

    private fun operator(sub: String) = TestTokens.mint(pair, TestTokens.operatorClaims(sub = sub))

    private fun scoped(
        name: String,
        scope: String,
    ) = TestTokens.mint(
        pair,
        TestTokens.scopedClaims(catalogs = """{"$name":["$scope"]}""", iat = System.currentTimeMillis() / 1000 - 5),
    )

    private fun send(
        port: Int,
        method: String,
        path: String,
        token: String?,
        body: String? = null,
    ): HttpResponse<String> {
        val request =
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port$path"))
                .header("Content-Type", "application/json")
                .method(method, body?.let(HttpRequest.BodyPublishers::ofString) ?: HttpRequest.BodyPublishers.noBody())
        token?.let { request.header("Authorization", "Bearer $it") }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString())
    }

    /** #138's PUT: the whole mount, at the revision its GET read; `retention_months` null. */
    private fun putBody(
        revision: Long,
        enabled: Boolean,
        values: List<Long>,
    ) = """{"revision":$revision,"enabled":$enabled,"values":[${values.joinToString(",") {
        """{"value":$it,"retention_months":null}"""
    }}]}"""

    private fun put(
        port: Int,
        name: String,
        revision: Long,
        enabled: Boolean,
        values: List<Long>,
        token: String? = operator(WRITER),
    ) = send(port, "PUT", "/v1/mounts/$name", token, putBody(revision, enabled, values))

    private fun HttpResponse<String>.tree(): JsonNode = json.readTree(body())

    private fun JsonNode.mountValues(): List<Long> = get("values").map { it.get("value").asLong() }

    /** The row as stored, or null when it was never written. */
    private fun row(name: String): MountRecord? = store.get(name).takeIf { it.revision > 0 }

    @Test
    fun `a never-written mount reads as revision 0, and each CAS write moves it one revision`() {
        val name = mount(1)
        withServer { port ->
            val fresh = send(port, "GET", "/v1/mounts/$name", operator(WRITER))
            assertThat(fresh.statusCode()).describedAs(fresh.body()).isEqualTo(200)
            assertThat(fresh.tree().get("revision").asLong()).isZero()
            assertThat(fresh.tree().get("enabled").asBoolean()).isFalse()
            assertThat(fresh.tree().mountValues()).isEmpty()

            // A revision it was never at is stale, written or not.
            assertThat(put(port, name, 3, true, listOf(4)).statusCode()).isEqualTo(409)
            val created = put(port, name, 0, true, listOf(9, 4, 42))
            assertThat(created.statusCode()).describedAs(created.body()).isEqualTo(200)
            assertThat(created.tree().get("revision").asLong()).isEqualTo(1)
            // Ascending, so #138's slices.Equal against its sorted owners holds.
            assertThat(created.tree().mountValues()).containsExactly(4, 9, 42)
            // The wire drops nulls: no `retention_months` key.
            assertThat(created.tree().get("values").first().has("retention_months")).isFalse()

            for (stale in listOf(0L, 2L, 7L)) {
                val refused = put(port, name, stale, true, listOf(4))
                assertThat(refused.statusCode()).describedAs("revision $stale").isEqualTo(409)
                assertThat(refused.tree().get("error").asText()).isEqualTo("mount_revision_conflict")
            }
            assertThat(row(name)).isEqualTo(MountRecord(name, 1, true, listOf(4, 9, 42)))

            // #138's repair pass on an org no longer federated: empty and disabled.
            val disabled = put(port, name, 1, false, emptyList())
            assertThat(disabled.statusCode()).describedAs(disabled.body()).isEqualTo(200)
            val read = send(port, "GET", "/v1/mounts/$name", operator("hogtower:provisioner")).tree()
            assertThat(read.get("revision").asLong()).isEqualTo(2)
            assertThat(read.get("enabled").asBoolean()).isFalse()

            val listed = send(port, "GET", "/v1/mounts", operator(WRITER))
            assertThat(listed.statusCode()).isEqualTo(200)
            assertThat(listed.tree().map { it.get("name").asText() }).contains(name)
        }
    }

    @Test
    fun `only the configured writer's operator token may write, in every auth mode`() {
        val name = mount(2)
        withServer { port ->
            assertThat(put(port, name, 0, true, listOf(1)).statusCode()).isEqualTo(200)
            val refused =
                mapOf(
                    "the provisioner" to operator("hogtower:provisioner"),
                    "platform:admin" to operator("platform:admin"),
                    "the webui" to operator("webui"),
                    "a tenant naming the mount" to
                        TestTokens.mint(pair, TestTokens.tenantClaims(catalogs = mapOf(name to listOf("admin")))),
                    "a scoped reader of the mount" to
                        scoped(name, "write"),
                )
            for ((who, token) in refused) {
                assertThat(put(port, name, 1, true, listOf(666), token).statusCode()).describedAs(who).isEqualTo(403)
            }
            assertThat(put(port, name, 1, true, listOf(666), token = null).statusCode()).isEqualTo(401)
        }
        // warn lets the route table's refusals through; the handler's holds.
        withServer(mode = AuthMode.WARN) { port ->
            assertThat(put(port, name, 1, true, listOf(666), token = null).statusCode()).isEqualTo(403)
            assertThat(put(port, name, 1, true, listOf(666), operator("hogtower:provisioner")).statusCode())
                .isEqualTo(403)
            // The writer's subject is not enough: it must be on an operator token.
            val tenant = TestTokens.tenantClaims(sub = WRITER, catalogs = mapOf(name to listOf("admin")))
            assertThat(put(port, name, 1, true, listOf(666), TestTokens.mint(pair, tenant)).statusCode())
                .isEqualTo(403)
        }
        // off verifies no one, so no one may write; reads are served as on every route.
        withServer(mode = AuthMode.OFF) { port ->
            assertThat(put(port, name, 1, true, listOf(666)).statusCode()).isEqualTo(403)
            assertThat(send(port, "GET", "/v1/mounts/$name", null).statusCode()).isEqualTo(200)
        }
        // No subject configured: not even the loop's token.
        withServer(writer = "") { port ->
            assertThat(put(port, name, 1, true, listOf(666)).statusCode()).isEqualTo(403)
            assertThat(put(port, name, 1, true, listOf(666), operator("")).statusCode()).isEqualTo(403)
        }
        assertThat(row(name)).isEqualTo(MountRecord(name, 1, true, listOf(1)))
    }

    @Test
    fun `a write refuses a name any catalog row holds, until the catalog is dropped`() {
        val name = mount(3)
        val catalogs = CatalogService(db.jdbi)
        // Taken below the API, as a name created before the reservation was.
        catalogs.createCatalog(name, "s3://bucket/$name")
        catalogs.createNamespace(name, "ingest")
        catalogs.dropNamespace(name, "ingest", author = "test", message = "emptied")
        withServer { port ->
            val refused = put(port, name, 0, true, listOf(5))
            assertThat(refused.statusCode()).describedAs(refused.body()).isEqualTo(422)
            assertThat(refused.body()).contains("is a catalog's name")
            assertThat(row(name)).isNull()

            catalogs.dropCatalog(name, "test", "free the name")
            assertThat(put(port, name, 0, true, listOf(5)).statusCode()).isEqualTo(200)
        }
    }

    @Test
    fun `values are bounded, non-negative and distinct, retention months are null, and names are mount names`() {
        val name = mount(4)
        withServer { port ->
            val max = (0L until MountStore.MAX_VALUES.toLong()).toList()
            assertThat(put(port, name, 0, true, max + MountStore.MAX_VALUES.toLong()).statusCode()).isEqualTo(422)
            assertThat(put(port, name, 0, true, listOf(3, -1)).statusCode()).isEqualTo(422)
            assertThat(put(port, name, 0, true, listOf(3, 8, 3)).statusCode()).isEqualTo(422)
            for (body in listOf(
                """{"revision":0,"enabled":true,"values":[{"value":3,"retention_months":12}]}""",
                """{"enabled":true,"values":[]}""",
                """{"revision":0,"values":[]}""",
                """{"revision":0,"enabled":true,"values":[{}]}""",
            )) {
                assertThat(send(port, "PUT", "/v1/mounts/$name", operator(WRITER), body).statusCode())
                    .describedAs(body)
                    .isEqualTo(422)
            }
            for (bad in listOf("lake", "${org(4).uppercase()}-events", "${org(4)}-events2")) {
                assertThat(put(port, bad, 0, true, listOf(3)).statusCode()).describedAs(bad).isEqualTo(422)
                assertThat(send(port, "GET", "/v1/mounts/$bad", operator(WRITER)).statusCode()).isEqualTo(422)
            }
            assertThat(row(name)).isNull()
            val full = put(port, name, 0, true, max.reversed())
            assertThat(full.statusCode()).describedAs(full.body()).isEqualTo(200)
            assertThat(full.tree().mountValues()).isEqualTo(max)
            // A Go nil slice marshals as null: #138's repair pass sends one to disable.
            val nil = """{"revision":1,"enabled":false,"values":null}"""
            assertThat(send(port, "PUT", "/v1/mounts/$name", operator(WRITER), nil).statusCode()).isEqualTo(200)
            assertThat(row(name)).isEqualTo(MountRecord(name, 2, false, emptyList()))
        }
    }

    @Test
    fun `of writers racing from one revision, exactly one wins`() {
        val name = mount(5)
        for (revision in 0L..2L) {
            val pool = Executors.newFixedThreadPool(8)
            try {
                val outcomes =
                    pool.invokeAll(
                        (1..8).map { i ->
                            Callable {
                                runCatching { store.put(name, revision, true, listOf(revision * 10 + i)) }
                            }
                        },
                    ).map { it.get() }
                assertThat(outcomes.count { it.isSuccess }).describedAs("from revision $revision").isEqualTo(1)
                assertThat(outcomes.filter { it.isFailure }.map { it.exceptionOrNull() })
                    .allMatch { it is HoglakeException.MountRevisionConflict }
            } finally {
                pool.shutdown()
            }
            assertThat(store.get(name).revision).isEqualTo(revision + 1)
        }
    }

    @Test
    fun `a writer racing from a revision another writer is leaving writes nothing`() {
        val name = mount(10)
        // A first write: the loser reads no row, then waits on the winner's unique entry.
        raced(name, 0) { it.execute("INSERT INTO hog_mount VALUES (?, 1, true, '{1}')", name) }
        assertThat(store.get(name)).isEqualTo(MountRecord(name, 1, true, listOf(1)))
        // A later write: the loser reads revision 1, then waits on the winner's row.
        raced(name, 1) { it.execute("UPDATE hog_mount SET revision = 2, mount_values = '{2}' WHERE name = ?", name) }
        assertThat(store.get(name)).isEqualTo(MountRecord(name, 2, true, listOf(2)))
    }

    /** [winner] runs uncommitted until a put from [revision] is blocked on it; that put must then lose. */
    private fun raced(
        name: String,
        revision: Long,
        winner: (Handle) -> Unit,
    ) = db.jdbi.open().use { h ->
        h.begin()
        winner(h)
        val pool = Executors.newSingleThreadExecutor()
        try {
            val loser = pool.submit(Callable { runCatching { store.put(name, revision, true, listOf(99)) } })
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30)
            while (lockWaiters() == 0) {
                check(System.nanoTime() < deadline) { "the racing write never waited on the winner" }
                Thread.sleep(20)
            }
            h.commit()
            assertThat(loser.get(30, TimeUnit.SECONDS).exceptionOrNull())
                .isInstanceOf(HoglakeException.MountRevisionConflict::class.java)
        } finally {
            pool.shutdownNow()
        }
    }

    private fun lockWaiters(): Int =
        db.jdbi.withHandle<Int, Exception> {
            it.createQuery(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'",
            ).mapTo(Int::class.java).one()
        }

    @Test
    fun `a written mount serves only while enabled, allowlisted and switched on`() {
        val name = mount(6)
        val reader = scoped(name, "read")

        fun dispatch(port: Int) = send(port, "GET", "/v1/catalogs/$name/namespaces", reader).statusCode()

        fun switch(on: Boolean) =
            db.jdbi.useHandle<Exception> { it.execute("UPDATE hog_mount_switch SET enabled = ?", on) }
        switch(true)
        try {
            withServer(allowlist = setOf(org(6))) { port ->
                assertThat(dispatch(port)).describedAs("never written").isEqualTo(404)
                assertThat(put(port, name, 0, true, listOf(42)).statusCode()).isEqualTo(200)
                // 1.1a's answer for a mount read it has no reader for yet.
                assertThat(dispatch(port)).describedAs("enabled").isEqualTo(501)
                assertThat(put(port, name, 1, false, emptyList()).statusCode()).isEqualTo(200)
                assertThat(dispatch(port)).describedAs("disabled").isEqualTo(404)
                assertThat(put(port, name, 2, true, listOf(42)).statusCode()).isEqualTo(200)
            }
            withServer(allowlist = setOf(org(7))) { port ->
                assertThat(dispatch(port)).describedAs("another org allowlisted").isEqualTo(404)
            }
            withServer(allowlist = emptySet()) { port ->
                assertThat(dispatch(port)).describedAs("empty allowlist").isEqualTo(404)
                // Writes are not gated by the allowlist: a disable always lands.
                assertThat(put(port, name, 3, false, emptyList()).statusCode()).isEqualTo(200)
            }
        } finally {
            switch(false)
        }
        withServer(allowlist = setOf(org(6))) { port ->
            assertThat(put(port, name, 4, true, listOf(42)).statusCode()).isEqualTo(200)
            assertThat(dispatch(port)).describedAs("switch off").isEqualTo(404)
        }
    }

    @Test
    fun `a name outside the allowlist never reaches the database`() {
        val store = MountStore({ error("read the database") }, { false })
        assertThat(store.find(mount(8))).isNull()
    }

    @Test
    fun `every write attempt is one audit line, with the writer and the values that came and went`() {
        val name = mount(9)
        val capture = ListAppender<ILoggingEvent>().apply { start() }
        val audit = LoggerFactory.getLogger(Audit.LOGGER_NAME) as Logger
        audit.addAppender(capture)
        try {
            withServer { port ->
                assertThat(put(port, name, 0, true, listOf(4, 9)).statusCode()).isEqualTo(200)
                assertThat(put(port, name, 1, true, listOf(9, 12)).statusCode()).isEqualTo(200)
                assertThat(put(port, name, 2, true, listOf(1), operator("platform:admin")).statusCode()).isEqualTo(403)
                assertThat(put(port, name, 0, true, listOf(1)).statusCode()).isEqualTo(409)
            }
            val lines = capture.list.filter { it.formattedMessage.startsWith("mount_write ") }
            assertThat(lines.map { it.formattedMessage })
                .containsExactly("mount_write ok", "mount_write ok", "mount_write forbidden", "mount_write conflict")
            val second = lines[1].argumentArray.map { it.toString() }
            assertThat(second).contains("actor=operator:$WRITER", "catalog=$name", "operation_message=jti=jti-op")
            assertThat(second.single { it.startsWith("detail=") })
                .isEqualTo("detail=revision=1->2 enabled=true values=2 added=[12] removed=[4]")
            // A refusal carries the jti too: it is what ties the attempt to its token.
            assertThat(lines[2].argumentArray.map { it.toString() })
                .contains("actor=operator:platform:admin", "operation_message=jti=jti-op")
        } finally {
            audit.detachAppender(capture)
        }
    }

    @Test
    fun `the allowlist takes org ids or a star, and admits only their mounts`() {
        val cfg = Config(port = 0, hydratorIntervalMs = 0).copy(mountAllowlist = setOf(org(1)))
        assertThat(cfg.mountAdmitted(mount(1))).isTrue()
        assertThat(cfg.mountAdmitted(mount(2))).isFalse()
        assertThat(cfg.mountAdmitted(org(1))).isFalse()
        assertThat(cfg.copy(mountAllowlist = setOf("*")).mountAdmitted(mount(2))).isTrue()
        assertThat(cfg.copy(mountAllowlist = emptySet()).mountAdmitted(mount(1))).isFalse()
        for (bad in listOf(org(1).uppercase(), "${org(1)}-events", "acme")) {
            assertThatThrownBy { cfg.copy(mountAllowlist = setOf(bad)) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("HOGLAKE_MOUNT_ALLOWLIST")
        }
    }

    private companion object {
        const val WRITER = "hogtower:events-mounts"
    }
}
