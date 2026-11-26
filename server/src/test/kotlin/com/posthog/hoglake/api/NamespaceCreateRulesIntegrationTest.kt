package com.posthog.hoglake.api

import com.fasterxml.jackson.databind.JsonNode
import com.posthog.hoglake.App
import com.posthog.hoglake.Config
import com.posthog.hoglake.auth.AuthMode
import com.posthog.hoglake.auth.TestTokens
import com.posthog.hoglake.configureApiListener
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.service.CatalogService
import com.posthog.hoglake.testing.PgTestSupport
import com.posthog.hoglake.wireObjectMapper
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.jdbi.v3.core.kotlin.withHandleUnchecked
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
 * `POST /namespaces` through the real application on PostgreSQL: the
 * operator-only `team_<digits>` names (`HOGLAKE_OPERATOR_TEAM_NAMESPACE_CATALOGS`)
 * and the operator's one-call create-and-mark. A refusal writes nothing.
 */
@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NamespaceCreateRulesIntegrationTest {
    private val db = PgTestSupport.freshDatabase()
    private val catalogs = CatalogService(db.jdbi)
    private val pair = TestTokens.keyPair()
    private val keyFile =
        Files.createTempFile("hoglake-ns-create-", ".json").also { Files.writeString(it, TestTokens.jwks(pair)) }
    private val json = wireObjectMapper()
    private val http = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    private val listed = "imports-dev"
    private val unlisted = "imports-prod"
    private val org = "01a1e5de-1ac5-0000-e53e-d2f9a77deae4"
    private val otherOrg = "02b2f6ef-2bd6-0000-f64f-e3a0b88efbf5"

    init {
        for (cat in listOf(listed, unlisted)) catalogs.createCatalog(cat, "s3://bucket/$cat")
    }

    @AfterAll
    fun close() {
        http.close()
        db.close()
        Files.deleteIfExists(keyFile)
    }

    private fun <T> withServer(
        mode: AuthMode = AuthMode.ENFORCE,
        block: (port: Int) -> T,
    ): T {
        val cfg =
            Config(port = 0, hydratorIntervalMs = 0).copy(
                authMode = mode.wire,
                authIssuer = TestTokens.ISSUER,
                authJwksFile = keyFile.toString(),
                authPredicateFields = "team_id",
                operatorTeamNamespaceCatalogs = setOf(listed),
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

    private fun operator() = TestTokens.mint(pair, TestTokens.operatorClaims())

    private fun tenant(cat: String) =
        TestTokens.mint(pair, TestTokens.tenantClaims(catalogs = mapOf(cat to listOf("write"))))

    private fun scoped(cat: String): String {
        val iat = System.currentTimeMillis() / 1000 - 5
        return TestTokens.mint(pair, TestTokens.scopedClaims(catalogs = """{"$cat":["write"]}""", iat = iat))
    }

    private fun send(
        port: Int,
        token: String?,
        cat: String,
        method: String,
        path: String,
        body: String? = null,
    ): HttpResponse<String> =
        http.send(
            HttpRequest.newBuilder(URI.create("http://127.0.0.1:$port/v1/catalogs/$cat$path"))
                .apply { token?.let { header("Authorization", "Bearer $it") } }
                .header("Content-Type", "application/json")
                .method(
                    method,
                    body?.let { HttpRequest.BodyPublishers.ofString(it) } ?: HttpRequest.BodyPublishers.noBody(),
                )
                .build(),
            HttpResponse.BodyHandlers.ofString(),
        )

    private fun create(
        port: Int,
        token: String?,
        cat: String,
        name: String,
        containment: String? = null,
    ) = send(
        port,
        token,
        cat,
        "POST",
        "/namespaces",
        """{"name":"$name"${containment?.let { ""","containment":$it""" } ?: ""}}""",
    )

    private fun mark(
        value: Long,
        markOrg: String? = org,
    ) = """{"field":"team_id","value":$value${markOrg?.let { ""","org":"$it"""" } ?: ""}}"""

    private fun tree(response: HttpResponse<String>): JsonNode = json.readTree(response.body())

    /** Every row a namespace create writes, catalog-wide. */
    private fun footprint(cat: String): List<Long> =
        db.jdbi.withHandleUnchecked { h ->
            listOf("hog_snapshot", "hog_namespace", "hog_snapshot_change").map {
                h.createQuery("SELECT count(*) FROM $it").mapTo(Long::class.java).one()
            } + catalogs.getCatalog(cat).headSnapshotId
        }

    private fun assertRefused(
        response: HttpResponse<String>,
        status: Int,
        error: String,
        what: String,
    ) {
        assertThat(response.statusCode()).describedAs("%s: %s", what, response.body()).isEqualTo(status)
        assertThat(tree(response).path("error").asText()).describedAs(what).isEqualTo(error)
    }

    @Test
    fun `a non-operator may not create a team name in a listed catalog, in any case or with leading zeros`() {
        withServer { port ->
            val before = footprint(listed)
            for ((who, token) in listOf("tenant" to tenant(listed), "scoped" to scoped(listed))) {
                for (name in listOf("team_7", "TEAM_7", "Team_007", "team_007", "tEAM_70000")) {
                    assertRefused(create(port, token, listed, name), 403, "forbidden", "$who $name")
                }
            }
            assertThat(footprint(listed)).isEqualTo(before)
            // Names that are not team_<digits> stay open to a tenant.
            for (name in listOf("team_", "team_x", "team_7a", "myteam_7", "team-7", "teams_7")) {
                assertThat(create(port, tenant(listed), listed, name).statusCode()).describedAs(name).isEqualTo(201)
            }
        }
    }

    @Test
    fun `a team name is open in a catalog that is not listed, and to an operator in one that is`() {
        withServer { port ->
            for (name in listOf("team_8", "TEAM_9", "team_010")) {
                assertThat(create(port, tenant(unlisted), unlisted, name).statusCode()).describedAs(name).isEqualTo(201)
            }
            for (name in listOf("team_18", "TEAM_19", "team_0110")) {
                val made = create(port, operator(), listed, name)
                assertThat(made.statusCode()).describedAs(made.body()).isEqualTo(201)
            }
            assertThat(catalogs.listNamespaces(listed).map { it.name }).contains("team_18", "TEAM_19", "team_0110")
        }
        // Auth off has no callers to tell apart.
        withServer(AuthMode.OFF) { port ->
            assertThat(create(port, null, listed, "team_28").statusCode()).isEqualTo(201)
        }
    }

    @Test
    fun `warn mode requires a verified operator for team names in listed catalogs`() {
        withServer(AuthMode.WARN) { port ->
            val before = footprint(listed)
            assertRefused(create(port, null, listed, "team_41"), 403, "forbidden", "missing token")
            assertRefused(create(port, "invalid-token", listed, "team_41"), 403, "forbidden", "invalid token")
            assertRefused(create(port, tenant(listed), listed, "team_41"), 403, "forbidden", "tenant token")
            assertThat(footprint(listed)).isEqualTo(before)
            assertThat(create(port, operator(), listed, "team_41", mark(41)).statusCode()).isEqualTo(201)
            assertThat(create(port, null, unlisted, "team_42").statusCode()).isEqualTo(201)
            assertThat(create(port, null, listed, "analytics").statusCode()).isEqualTo(201)
        }
    }

    @Test
    fun `an operator creates and marks a namespace in one call, and the read shows the mark`() {
        withServer { port ->
            val made = create(port, operator(), listed, "team_31", mark(31))
            assertThat(made.statusCode()).describedAs(made.body()).isEqualTo(201)
            val containment = tree(made).path("containment")
            assertThat(containment.path("field").asText()).isEqualTo("team_id")
            assertThat(containment.path("value").asLong()).isEqualTo(31)
            assertThat(containment.path("org").asText()).isEqualTo(org)
            val read = tree(send(port, operator(), listed, "GET", "/namespaces/team_31")).path("containment")
            assertThat(read).isEqualTo(containment)
            assertThat(catalogs.getNamespace(listed, "team_31").containment)
                .isEqualTo(NamespaceContainment("team_id", 31, org))
            // Without an org the mark is the plain one, and a body without `containment` marks nothing.
            val plain = create(port, operator(), unlisted, "plain", mark(32, markOrg = null))
            assertThat(tree(plain).path("containment").has("org")).isFalse()
            assertThat(tree(create(port, operator(), unlisted, "unmarked")).has("containment")).isFalse()
        }
    }

    @Test
    fun `repeating a one-call create answers already_exists even once files sit under the prefix`() {
        withServer { port ->
            val token = operator()
            assertThat(create(port, token, unlisted, "loaded", mark(61)).statusCode()).isEqualTo(201)
            catalogs.createTable(unlisted, "loaded", "t", listOf(ColumnDef("id", ColType.LONG)))
            val path = "s3://bucket/$unlisted/org_id=$org/team_id=61/f.parquet"
            val file = """{"path":"$path","record_count":1,"file_size_bytes":10}"""
            val body = """{"appends":[{"namespace":"loaded","table":"t","files":[$file]}]}"""
            val registered = send(port, token, unlisted, "POST", "/commit", body)
            assertThat(registered.statusCode()).describedAs(registered.body()).isEqualTo(200)
            assertRefused(create(port, token, unlisted, "loaded", mark(61)), 409, "already_exists", "repeat")
        }
    }

    @Test
    fun `a non-operator may not create with a containment mark`() {
        withServer { port ->
            val before = footprint(unlisted)
            // An unlisted catalog and a name that is not a team name, so the mark is what is refused.
            for ((who, token) in listOf("tenant" to tenant(unlisted), "scoped" to scoped(unlisted))) {
                assertRefused(create(port, token, unlisted, "marked_by_$who", mark(41)), 403, "forbidden", who)
            }
            assertThat(footprint(unlisted)).isEqualTo(before)
            assertThat(catalogs.listNamespaces(unlisted).map { it.name }).noneMatch { it.startsWith("marked_by_") }
        }
    }

    @Test
    fun `a refused mark creates nothing`() {
        catalogs.createNamespace(unlisted, "holder")
        catalogs.createTable(unlisted, "holder", "t", listOf(ColumnDef("id", ColType.LONG)))
        catalogs.createNamespace(unlisted, "ghost")
        catalogs.createTable(unlisted, "ghost", "t", listOf(ColumnDef("id", ColType.LONG)))
        catalogs.dropTable(unlisted, "ghost", "t", author = "op", message = "drop")
        catalogs.dropNamespace(unlisted, "ghost", author = "op", message = "drop")
        catalogs.createNamespace(unlisted, "taken")
        catalogs.setNamespaceContainment(unlisted, "taken", NamespaceContainment("team_id", 54, org))
        withServer { port ->
            val token = operator()
            val path = "s3://bucket/$unlisted/org_id=$org/team_id=52/f.parquet"
            val files = """{"path":"$path","record_count":1,"file_size_bytes":10}"""
            val registered =
                send(
                    port,
                    token,
                    unlisted,
                    "POST",
                    "/commit",
                    """{"appends":[{"namespace":"holder","table":"t","files":[$files]}]}""",
                )
            assertThat(registered.statusCode()).describedAs(registered.body()).isEqualTo(200)

            val before = footprint(unlisted)

            fun refused(
                name: String,
                containment: String,
                status: Int,
                error: String,
            ) {
                assertRefused(create(port, token, unlisted, name, containment), status, error, name)
                assertThat(footprint(unlisted)).describedAs(name).isEqualTo(before)
                assertThat(catalogs.listNamespaces(unlisted).map { it.name }).describedAs(name).doesNotContain(name)
            }
            // A bad org (and a bad field or value): 422, and no namespace.
            refused("bad_org", mark(51, markOrg = "ACME"), 422, "validation")
            refused("upper_org", mark(51, markOrg = org.uppercase()), 422, "validation")
            refused("bad_field", """{"field":"Team","value":51}""", 422, "validation")
            refused("bad_value", mark(0), 422, "validation")
            // Another table's file is already under the prefix: 409.
            refused("prefix_taken", mark(52), 409, "commit_conflict")
            // The value is another namespace's mark: 409.
            refused("value_taken", mark(54), 409, "already_exists")
            // The name belonged to a dropped namespace that held a table: 409.
            refused("ghost", mark(53), 409, "commit_conflict")
            // A refusal leaves the name free for a valid create.
            assertThat(create(port, token, unlisted, "bad_org", mark(55)).statusCode()).isEqualTo(201)
        }
    }
}
