package com.posthog.hoglake.auth

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.posthog.hoglake.Config
import com.posthog.hoglake.api.installApiRoutes
import com.posthog.hoglake.api.installErrorMapping
import com.posthog.hoglake.api.installScanRoutes
import com.posthog.hoglake.auth.ReadSessionProfile.Companion.LOCAL_ONLY_MARKER
import com.posthog.hoglake.auth.ReadSessionProfile.Companion.LOCAL_ONLY_PREFIX
import com.posthog.hoglake.configureHoglakeWire
import com.posthog.hoglake.model.ColType
import com.posthog.hoglake.model.Column
import com.posthog.hoglake.model.ColumnDef
import com.posthog.hoglake.model.DataFile
import com.posthog.hoglake.model.NamespaceContainment
import com.posthog.hoglake.model.NamespaceInfo
import com.posthog.hoglake.model.ScanFile
import com.posthog.hoglake.model.StatsState
import com.posthog.hoglake.model.TableInfo
import com.posthog.hoglake.observability.Metrics
import com.posthog.hoglake.store.CatalogStore
import com.posthog.hoglake.store.CatalogStoreSelector
import com.posthog.hoglake.store.MountHostResolver
import com.posthog.hoglake.store.StaticMountHosts
import com.sun.net.httpserver.HttpServer
import io.ktor.client.request.delete
import io.ktor.client.request.get
import io.ktor.client.request.head
import io.ktor.client.request.header
import io.ktor.client.request.post
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpMethod
import io.ktor.http.HttpStatusCode
import io.ktor.serialization.jackson.jackson
import io.ktor.server.application.Application
import io.ktor.server.application.install
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.StatusPages
import io.ktor.server.testing.testApplication
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.lang.reflect.Proxy
import java.net.InetSocketAddress
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.security.interfaces.ECPublicKey
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList

/** Owned HTTP fixtures exercise the real verifier, gate, selector, and existing API route. */
class ScopedNamespaceDispatchTest {
    @TempDir
    lateinit var dir: Path

    private val id = UUID.fromString("5b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")
    private val otherId = UUID.fromString("6b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")
    private val postgresId = UUID.fromString("7b0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")
    private val pair = TestTokens.keyPair()
    private val path = "/v1/catalogs/lake/namespaces/reports"
    private val success = """{"namespace_id":17,"name":"reports","containment":{"field":"team_id","value":42}}"""
    private val requests = CopyOnWriteArrayList<Pair<String, String?>>()
    private val sessionDigests = CopyOnWriteArrayList<String>()
    private val legacy = CopyOnWriteArrayList<String>()
    private var server: HttpServer? = null
    private var remoteServer: HttpServer? = null
    private var mountHosts: StaticMountHosts? = null
    private val remoteAuthorizations = CopyOnWriteArrayList<String>()

    @Volatile
    private var status = 200

    @Volatile
    private var body = success

    @AfterEach
    fun stop() {
        mountHosts?.close()
        remoteServer?.stop(0)
        server?.stop(0)
        Metrics.clear()
    }

    private val mapper = ObjectMapper()
    private val vectors =
        javaClass.getResourceAsStream("/oauth/read-session-vectors.json").use { mapper.readTree(requireNotNull(it)) }
    private val readVector = vectors["vectors"].single { it["name"].asText() == "complete read session" }
    private val readPath = "/v1/catalogs/example-catalog/namespaces/reports"
    private val readTablePath = "$readPath/tables/metrics"
    private val readScanPath = "$readTablePath/scan"
    private val readBackend = UUID.fromString(vectors["config"]["backend_id"].asText())
    private val tableUuid = "11111111-1111-4111-8111-1111111111aa"
    private val tableSuccess =
        """
        {
          "namespace_id": 17,
          "namespace": "reports",
          "containment": {"field": "team_id", "value": 42},
          "table_id": 101,
          "table_uuid": "$tableUuid",
          "name": "metrics",
          "read_snapshot_id": 5,
          "columns": [
            {"field_id": 1, "ordinal": 0, "name": "id", "type": "long", "nullable": false},
            {
              "field_id": 2,
              "ordinal": 1,
              "name": "amount",
              "type": "decimal",
              "type_params": {"precision": 10, "scale": 2},
              "nullable": true,
              "comment": "total amount"
            }
          ]
        }
        """.trimIndent()
    private val scanSuccess =
        """
        {
          "namespace_id": 17,
          "namespace": "reports",
          "table_uuid": "$tableUuid",
          "read_snapshot_id": 5,
          "files": [
            {
              "data_file": {
                "data_file_id": 1,
                "path": "s3://example-customer/tables/data.parquet",
                "file_format": "parquet",
                "record_count": 3,
                "file_size_bytes": 1024,
                "footer_size": 256,
                "row_id_start": 0,
                "stats_state": "none",
                "begin_snapshot": 1,
                "explicit_row_ids": false
              },
              "delete_file": null
            }
          ]
        }
        """.trimIndent()

    private fun digest(text: String): String =
        MessageDigest.getInstance("SHA-256").digest(text.toByteArray()).joinToString("") { "%02x".format(it) }

    private fun config(
        withReadSession: Boolean = true,
        readFixture: Boolean = true,
        mountHostsJson: String? = null,
        reservedNamespaceNames: Set<String> = emptySet(),
        relay: Boolean = false,
    ): Config {
        val peer = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        server = peer
        peer.createContext("/") { exchange ->
            exchange.requestBody.readBytes()
            val request = "${exchange.requestMethod} ${exchange.requestURI}"
            val authorization = exchange.requestHeaders.getFirst("Authorization")
            if (readFixture) {
                sessionDigests.add(digest(authorization.orEmpty()))
                requests.add(request to null)
            } else {
                requests.add(request to authorization)
            }
            exchange.responseHeaders.set("Content-Type", "application/json")
            val bytes = body.toByteArray()
            exchange.sendResponseHeaders(status, bytes.size.toLong())
            exchange.responseBody.use { it.write(bytes) }
        }
        peer.start()
        val backends =
            listOf(id, otherId, readBackend).joinToString(",") {
                """{"id":"$it","kind":"hoglite","url":"http://127.0.0.1:${peer.address.port}"}"""
            }
        val backendsFile = Files.writeString(dir.resolve("backends.json"), """{"backends":[$backends]}""")
        val hostsFile = mountHostsJson?.let { Files.writeString(dir.resolve("hosts.json"), it).toString() } ?: ""
        val targets = vectors["config"]["remote_targets"].map { it.asText() }.joinToString(",")
        return Config(
            backendsFile = backendsFile.toString(),
            readSessionProfile = if (withReadSession) vectors["config"]["profile"].asText() else "",
            readSessionRemoteTargets = if (withReadSession) targets else "",
            readSessionRelay = if (relay) "on" else "off",
            mountHostsFile = hostsFile,
            reservedNamespaceNames = reservedNamespaceNames,
            postgresBackendId = postgresId.toString(),
            authMode = "enforce",
            authJwksFile = "mounted-public-jwks.json",
            authPredicateFields = "team_id",
            authSessionProfile = if (mountHostsJson != null) "on" else "off",
        )
    }

    private fun Application.harness(
        cfg: Config,
        guardLegacy: Boolean = true,
        audience: String = TestTokens.AUDIENCE,
        readFixture: Boolean = false,
        publicListener: Boolean = false,
    ) {
        val postgres =
            Proxy.newProxyInstance(
                CatalogStore::class.java.classLoader,
                arrayOf(CatalogStore::class.java),
            ) { _, method, args ->
                legacy.add(method.name)
                check(!guardLegacy) { "legacy store must not run: ${method.name}" }
                when (method.name) {
                    "liveContainment" -> NamespaceContainment("team_id", 42)
                    "getNamespace" -> NamespaceInfo(99, args!![1] as String, NamespaceContainment("team_id", 42))
                    "getTable" ->
                        TableInfo(
                            tableId = 101,
                            tableUuid = UUID.fromString(tableUuid),
                            namespace = args!![1] as String,
                            name = args[2] as String,
                            columns = listOf(Column(1, 0, ColumnDef("id", ColType.LONG, nullable = false))),
                            readSnapshotId = 5,
                            recordCount = 3,
                            fileCount = 1,
                            fileSizeBytes = 1024,
                        )
                    "planScan" ->
                        listOf(
                            ScanFile(
                                dataFile =
                                    DataFile(
                                        dataFileId = 1,
                                        tableId = 101,
                                        path = "s3://example-customer/tables/data.parquet",
                                        fileFormat = "parquet",
                                        recordCount = 3,
                                        fileSizeBytes = 1024,
                                        footerSize = 256,
                                        rowIdStart = 0,
                                        statsState = StatsState.PENDING,
                                        beginSnapshot = 1,
                                    ),
                                deleteFile = null,
                            ),
                        )
                    else -> error("unexpected legacy method: ${method.name}")
                }
            } as CatalogStore
        val resolver =
            if (cfg.mountHosts.isNotEmpty()) {
                StaticMountHosts(cfg.mountHosts).also { mountHosts = it }
            } else {
                MountHostResolver.NONE
            }
        val selector = CatalogStoreSelector(postgres, cfg, resolver)
        val service =
            AuthService(
                AuthMode.ENFORCE,
                JwtVerifier(
                    if (readFixture) vectors["issuer"].asText() else TestTokens.ISSUER,
                    audience,
                    if (readFixture) {
                        val jwks = Files.writeString(dir.resolve("test-jwks.json"), vectors["jwks"].toString())
                        JwksSigningKeys.fromFile(jwks)
                    } else {
                        StaticSigningKeys(TestTokens.KID, pair.public as ECPublicKey)
                    },
                    predicateFields = setOf("team_id"),
                    backends = cfg.backendKinds,
                    sessionProfile = cfg.authSessionProfileEnabled,
                ) { if (readFixture) readVector["verify_at"].asLong() else 1_700_000_100L },
            )
        if (publicListener) {
            installAuth(service.forPublicListener())
        } else {
            installAuth(
                service,
                cfg.readSession?.let {
                    ReadSessionConfig(
                        it,
                        cfg.reservedNamespaces,
                        relays = selector::relaysReadSession,
                    )
                },
            )
        }
        installNamespaceGate(selector)
        install(ContentNegotiation) { jackson { configureHoglakeWire() } }
        install(StatusPages) { installErrorMapping() }
        installApiRoutes(selector, reservedNamespaces = cfg.reservedNamespaces)
        installScanRoutes(selector)
    }

    private fun claims(
        backend: UUID = id,
        tenant: String = "opaque_1",
        catalog: String = "lake",
        prd: String = """{"team_id":{"42":"read"}}""",
    ): String =
        TestTokens.scopedClaims(
            catalogs = """{"$catalog":["read"]}""",
            prd = prd,
            extra = ""","rtg":{"b":"hoglite","be":"$backend","t":"$tenant"}""",
        )

    private fun token(claims: String = claims()): String = TestTokens.mint(pair, claims)

    private fun signedRead(change: (ObjectNode) -> Unit): String {
        val claims = (readVector["claims"] as ObjectNode).deepCopy().also(change)
        val header = mapper.createObjectNode().put("alg", "ES256").put("kid", ContractVectors.KID).put("typ", "JWT")
        return ContractVectors.sign(header, claims)
    }

    @Test
    fun `the opted in table GET forwards exact tokens and returns TableDto without totals`() =
        testApplication {
            val cfg = config(withReadSession = true)
            body = tableSuccess
            application { harness(cfg, readFixture = true) }
            val first = readVector["token"].asText()
            val second = signedRead { it.put("usr", "example.bob").put("sub", "example.bob").put("jti", "token-b") }
            for ((target, jwt) in listOf(
                readTablePath to first,
                "$readTablePath?totals=false" to second,
            )) {
                val response = client.get(target) { header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(response.status).isEqualTo(HttpStatusCode.OK)
                assertThat(response.headers[HttpHeaders.CacheControl]).isEqualTo("no-store")
                val tree = mapper.readTree(response.bodyAsText())
                assertThat(tree["name"].asText()).isEqualTo("metrics")
                assertThat(tree["namespace"].asText()).isEqualTo("reports")
                assertThat(tree["table_uuid"].asText()).isEqualTo(tableUuid)
                assertThat(tree["read_snapshot_id"].asLong()).isEqualTo(5L)
                assertThat(tree["remote"].asBoolean()).isFalse()
                assertThat(tree.has("record_count")).isFalse()
                assertThat(tree.has("file_count")).isFalse()
                assertThat(tree.has("file_size_bytes")).isFalse()
                assertThat(tree["columns"].size()).isEqualTo(2)
                assertThat(tree["columns"][1]["type_params"]["precision"].asInt()).isEqualTo(10)
            }
            val peer = "GET /v1/t/fixture_3/metadata/v1/catalogs/example-catalog/namespaces/reports/tables/metrics"
            assertThat(requests).containsExactly(peer to null, peer to null)
            assertThat(sessionDigests).containsExactly(digest("Bearer $first"), digest("Bearer $second"))
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `table GET rejects invalid query parameters noncanonical paths and bodies`() =
        testApplication {
            val cfg = config(withReadSession = true)
            body = tableSuccess
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()
            for (target in listOf(
                "$readTablePath?",
                "$readTablePath?totals=true",
                "$readTablePath?totals=FALSE",
                "$readTablePath?totals=false&totals=false",
                "$readTablePath?snapshot=5",
                "$readTablePath?at_timestamp=2026-01-01T00:00:00Z",
                "$readTablePath?totals=false&snapshot=5",
                "$readTablePath?extra=1",
                readTablePath.replace("metrics", "%6detrics"),
            )) {
                val response = client.get(target) { header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(response.status).describedAs(target).isEqualTo(HttpStatusCode.BadRequest)
            }
            val withBody =
                client.get(readTablePath) {
                    header(HttpHeaders.Authorization, "Bearer $jwt")
                    setBody("x")
                }
            assertThat(withBody.status).isEqualTo(HttpStatusCode.BadRequest)
            assertThat(requests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `table GET translates Hoglite refusals and malformed bodies without SQL fallback`() =
        testApplication {
            val cfg = config(withReadSession = true)
            val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
            Metrics.bind(registry)
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()

            status = 404
            body = """{"code":"not_found"}"""
            val missing = client.get(readTablePath) { header(HttpHeaders.Authorization, "Bearer $jwt") }
            assertThat(missing.status).isEqualTo(HttpStatusCode.NotFound)
            assertThat(missing.headers[HttpHeaders.CacheControl]).isEqualTo("no-store")

            status = 403
            body = """{"code":"forbidden"}"""
            val forbidden = client.get(readTablePath) { header(HttpHeaders.Authorization, "Bearer $jwt") }
            assertThat(forbidden.status).isEqualTo(HttpStatusCode.Forbidden)
            assertThat(forbidden.headers[HttpHeaders.CacheControl]).isEqualTo("no-store")
            assertThat(forbidden.bodyAsText()).doesNotContain("fictional-key", "example-customer")

            status = 200
            body = tableSuccess.replace("\"value\": 42", "\"value\": 43")
            val wrongTeam = client.get(readTablePath) { header(HttpHeaders.Authorization, "Bearer $jwt") }
            assertThat(wrongTeam.status).isEqualTo(HttpStatusCode.NotFound)
            assertThat(wrongTeam.headers[HttpHeaders.CacheControl]).isEqualTo("no-store")
            assertThat(wrongTeam.bodyAsText()).doesNotContain("43")

            for ((code, peerBody) in listOf(
                503 to """{"code":"unavailable","detail":"peer-secret"}""",
                200 to tableSuccess.replace("\"metrics\"", "\"other\""),
                200 to tableSuccess.replace("\"reports\"", "\"other\""),
                200 to tableSuccess.replace(tableUuid, tableUuid.uppercase()),
                200 to tableSuccess.replace("\"read_snapshot_id\": 5", "\"read_snapshot_id\": -1"),
                200 to tableSuccess.replace("\"columns\": [", "\"record_count\": 1, \"columns\": ["),
                200 to """{"namespace_id":17,"namespace":"reports","containment":{"field":"team_id","value":42},""" +
                    """"table_id":101,"table_uuid":"$tableUuid","name":"metrics","read_snapshot_id":5,"columns":[]}""",
            )) {
                status = code
                body = peerBody
                val response = client.get(readTablePath) { header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(response.status).isEqualTo(HttpStatusCode.ServiceUnavailable)
                assertThat(response.headers[HttpHeaders.CacheControl]).isEqualTo("no-store")
                assertThat(response.bodyAsText()).doesNotContain("peer-secret", "other")
            }
            assertThat(requests).hasSize(10)
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `wrong catalogs reserved names wrong methods and unknown backends stay closed`() =
        testApplication {
            val cfg = config(withReadSession = true, reservedNamespaceNames = setOf("posthog"))
            body = tableSuccess
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()
            val query = "?snapshot=5&table_uuid=$tableUuid"
            val wrongCatalogTargets =
                listOf(
                    readPath.replace("example-catalog", "other-catalog"),
                    readTablePath.replace("example-catalog", "other-catalog"),
                    readScanPath.replace("example-catalog", "other-catalog") + query,
                )
            for (target in wrongCatalogTargets) {
                val response = client.get(target) { header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(response.status).describedAs(target).isEqualTo(HttpStatusCode.NotFound)
            }
            for (target in listOf(
                "/v1/catalogs/example-catalog/namespaces/posthog",
                "/v1/catalogs/example-catalog/namespaces/posthog/tables/events",
                "/v1/catalogs/example-catalog/namespaces/posthog/tables/events/scan$query",
                "/v1/catalogs/11111111-1111-1111-1111-111111111111-events/namespaces/reports",
                "$readPath/tables",
            )) {
                val response = client.get(target) { header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(response.status).describedAs(target).isEqualTo(HttpStatusCode.Unauthorized)
            }
            assertThat(client.head(readPath) { header(HttpHeaders.Authorization, "Bearer $jwt") }.status)
                .isEqualTo(HttpStatusCode.Unauthorized)
            assertThat(client.head(readTablePath) { header(HttpHeaders.Authorization, "Bearer $jwt") }.status)
                .isEqualTo(HttpStatusCode.Unauthorized)
            assertThat(client.head("$readScanPath$query") { header(HttpHeaders.Authorization, "Bearer $jwt") }.status)
                .isEqualTo(HttpStatusCode.Unauthorized)
            val unknownBackend =
                signedRead {
                    (it["rtg"] as ObjectNode).put("be", "99999999-9999-4999-8999-999999999999")
                }
            assertThat(client.get(readPath) { header(HttpHeaders.Authorization, "Bearer $unknownBackend") }.status)
                .isEqualTo(HttpStatusCode.Unauthorized)
            assertThat(requests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `dynamic JWT routing serves any catalog non-reserved namespace and table`() =
        testApplication {
            val cfg = config(withReadSession = true)
            application { harness(cfg, readFixture = true) }
            val query = "?snapshot=5&table_uuid=$tableUuid"
            val otherCatalogJwt =
                signedRead {
                    it.put("primary_catalog", "customer-cat")
                    it.putObject("cat").putArray("customer-cat").add("read")
                    (it["rtg"] as ObjectNode).put("be", otherId.toString()).put("t", "fixture_2")
                    (it["grants"][0] as ObjectNode)
                        .put("catalog", "customer-cat")
                        .put("target", "customer-cat")
                    (it["grants"][1] as ObjectNode)
                        .put("catalog", "customer-cat")
                        .put("target", otherId.toString())
                        .put("object_key", "root/bf/t/fixture_2/manifest")
                    (it["grants"][2] as ObjectNode).put("catalog", "customer-cat")
                    (it["grants"][3] as ObjectNode).put("catalog", "customer-cat")
                }

            body = success.replace("reports", "analytics")
            val nsResp =
                client.get("/v1/catalogs/customer-cat/namespaces/analytics") {
                    header(HttpHeaders.Authorization, "Bearer $otherCatalogJwt")
                }
            assertThat(nsResp.status).isEqualTo(HttpStatusCode.OK)
            assertThat(nsResp.bodyAsText()).contains("\"name\":\"analytics\"")

            body = tableSuccess.replace("reports", "analytics").replace("metrics", "events_daily")
            val tblResp =
                client.get("/v1/catalogs/customer-cat/namespaces/analytics/tables/events_daily") {
                    header(HttpHeaders.Authorization, "Bearer $otherCatalogJwt")
                }
            assertThat(tblResp.status).isEqualTo(HttpStatusCode.OK)
            assertThat(mapper.readTree(tblResp.bodyAsText())["name"].asText()).isEqualTo("events_daily")

            body = scanSuccess.replace("reports", "analytics")
            val scanResp =
                client.get("/v1/catalogs/customer-cat/namespaces/analytics/tables/events_daily/scan$query") {
                    header(HttpHeaders.Authorization, "Bearer $otherCatalogJwt")
                }
            assertThat(scanResp.status).isEqualTo(HttpStatusCode.OK)
            assertThat(mapper.readTree(scanResp.bodyAsText()).size()).isEqualTo(1)

            assertThat(requests.map { it.first }).containsExactly(
                "GET /v1/t/fixture_2/metadata/v1/catalogs/customer-cat/namespaces/analytics",
                "GET /v1/t/fixture_2/metadata/v1/catalogs/customer-cat/namespaces/analytics/tables/events_daily",
                "GET /v1/t/fixture_2/metadata/v1/catalogs/customer-cat/namespaces/analytics/tables/" +
                    "events_daily/scan$query",
            )
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `the public listener refuses dedicated tokens on bound table and scan routes`() =
        testApplication {
            val cfg = config(withReadSession = true)
            application { harness(cfg, readFixture = true, publicListener = true) }
            val jwt = readVector["token"].asText()
            assertThat(client.get(readTablePath) { header(HttpHeaders.Authorization, "Bearer $jwt") }.status)
                .isEqualTo(HttpStatusCode.Unauthorized)
            assertThat(
                client.get("$readScanPath?snapshot=5&table_uuid=$tableUuid") {
                    header(HttpHeaders.Authorization, "Bearer $jwt")
                }.status,
            ).isEqualTo(HttpStatusCode.Unauthorized)
            assertThat(requests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `the API and public listeners keep ordinary tenant and scoped reads working`() =
        testApplication {
            val cfg = config(withReadSession = true)
            application { harness(cfg, guardLegacy = false, readFixture = true, publicListener = false) }
            val readFields =
                listOf("profile", "version", "purpose", "primary_catalog", "remote_schemas", "aws", "grants", "rtg")
            val tenantJwt =
                signedRead {
                    it.put("aud", TestTokens.AUDIENCE)
                    it.put("ten", "tenant")
                    it.remove(readFields + "prd")
                }
            val scopedJwt =
                signedRead {
                    it.put("aud", TestTokens.AUDIENCE)
                    it.put("ten", "scoped")
                    it.remove(readFields)
                }
            for (jwt in listOf(tenantJwt, scopedJwt)) {
                val nsResp = client.get(readPath) { header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(nsResp.status).isEqualTo(HttpStatusCode.OK)
                assertThat(nsResp.bodyAsText()).contains("\"namespace_id\":99")

                val tableResp = client.get(readTablePath) { header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(tableResp.status).isEqualTo(HttpStatusCode.OK)
                val tableTree = mapper.readTree(tableResp.bodyAsText())
                assertThat(tableTree["name"].asText()).isEqualTo("metrics")
                assertThat(tableTree["record_count"].asLong()).isEqualTo(3L)

                val scanResp = client.get(readScanPath) { header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(scanResp.status).isEqualTo(HttpStatusCode.OK)
                val scanTree = mapper.readTree(scanResp.bodyAsText())
                assertThat(scanTree.size()).isEqualTo(1)
                assertThat(scanTree[0]["data_file"]["path"].asText())
                    .isEqualTo("s3://example-customer/tables/data.parquet")
            }
            assertThat(legacy).contains("getNamespace", "getTable", "planScan")
            assertThat(requests).isEmpty()
        }

    @Test
    fun `the opted in scan GET forwards exact tokens and returns ScanFileDto with optional DV`() =
        testApplication {
            val cfg = config(withReadSession = true)
            application { harness(cfg, readFixture = true) }
            val exactJwt = readVector["token"].asText()
            body = scanSuccess
            val first =
                client.get("$readScanPath?snapshot=5&table_uuid=$tableUuid") {
                    header(HttpHeaders.Authorization, "Bearer $exactJwt")
                }
            assertThat(first.status).isEqualTo(HttpStatusCode.OK)
            assertThat(first.headers[HttpHeaders.CacheControl]).isEqualTo("no-store")
            val firstTree = mapper.readTree(first.bodyAsText())
            assertThat(firstTree.isArray).isTrue()
            assertThat(firstTree.size()).isEqualTo(1)
            assertThat(firstTree[0]["data_file"]["path"].asText())
                .isEqualTo("s3://example-customer/tables/data.parquet")
            assertThat(firstTree[0]["data_file"]["explicit_row_ids"].asBoolean()).isFalse()
            assertThat(firstTree[0].has("delete_file")).isFalse()

            val prefixJwt =
                signedRead { claims ->
                    val grant = claims["grants"][0] as ObjectNode
                    grant.remove("object_key")
                    grant.put("prefix", "tables/team_id=42/")
                }
            body =
                """
                {
                  "namespace_id": 17,
                  "namespace": "reports",
                  "table_uuid": "$tableUuid",
                  "read_snapshot_id": 5,
                  "files": [
                    {
                      "data_file": {
                        "data_file_id": 7,
                        "path": "s3://example-customer/tables/team_id=42/part(1).parquet",
                        "file_format": "parquet",
                        "record_count": 10,
                        "file_size_bytes": 2048,
                        "footer_size": 128,
                        "row_id_start": 100,
                        "stats_state": "none",
                        "begin_snapshot": 2,
                        "explicit_row_ids": true
                      },
                      "delete_file": {
                        "delete_file_id": 9,
                        "path": "s3://example-customer/tables/team_id=42/dv(1).puffin",
                        "delete_count": 1,
                        "file_size_bytes": 64,
                        "begin_snapshot": 3
                      }
                    }
                  ]
                }
                """.trimIndent()
            val second =
                client.get("$readScanPath?table_uuid=$tableUuid&snapshot=5") {
                    header(HttpHeaders.Authorization, "Bearer $prefixJwt")
                }
            assertThat(second.status).isEqualTo(HttpStatusCode.OK)
            assertThat(second.headers[HttpHeaders.CacheControl]).isEqualTo("no-store")
            val secondTree = mapper.readTree(second.bodyAsText())
            assertThat(secondTree.size()).isEqualTo(1)
            assertThat(secondTree[0]["data_file"]["data_file_id"].asLong()).isEqualTo(7L)
            assertThat(secondTree[0]["data_file"]["path"].asText())
                .isEqualTo("s3://example-customer/tables/team_id=42/part(1).parquet")
            assertThat(secondTree[0]["data_file"]["explicit_row_ids"].asBoolean()).isTrue()
            assertThat(secondTree[0]["delete_file"]["delete_file_id"].asLong()).isEqualTo(9L)
            assertThat(secondTree[0]["delete_file"]["data_file_id"].asLong()).isEqualTo(7L)
            assertThat(secondTree[0]["delete_file"]["file_format"].asText()).isEqualTo("puffin")
            assertThat(secondTree[0]["delete_file"]["path"].asText())
                .isEqualTo("s3://example-customer/tables/team_id=42/dv(1).puffin")

            val peer =
                "GET /v1/t/fixture_3/metadata/v1/catalogs/example-catalog/namespaces/reports/tables/metrics/scan" +
                    "?snapshot=5&table_uuid=$tableUuid"
            assertThat(requests).containsExactly(peer to null, peer to null)
            assertThat(sessionDigests).containsExactly(digest("Bearer $exactJwt"), digest("Bearer $prefixJwt"))
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `scan GET rejects missing duplicate invalid or extra query parameters`() =
        testApplication {
            val cfg = config(withReadSession = true)
            body = scanSuccess
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()
            for (target in listOf(
                readScanPath,
                "$readScanPath?",
                "$readScanPath?snapshot=5",
                "$readScanPath?table_uuid=$tableUuid",
                "$readScanPath?snapshot=-1&table_uuid=$tableUuid",
                "$readScanPath?snapshot=05&table_uuid=$tableUuid",
                "$readScanPath?snapshot=+5&table_uuid=$tableUuid",
                "$readScanPath?snapshot=abc&table_uuid=$tableUuid",
                "$readScanPath?snapshot=5&table_uuid=${tableUuid.uppercase()}",
                "$readScanPath?snapshot=5&table_uuid=not-a-uuid",
                "$readScanPath?snapshot=5&snapshot=5&table_uuid=$tableUuid",
                "$readScanPath?snapshot=5&table_uuid=$tableUuid&table_uuid=$tableUuid",
                "$readScanPath?snapshot=5&table_uuid=$tableUuid&at_timestamp=2026-01-01T00:00:00Z",
                "$readScanPath?snapshot=5&table_uuid=$tableUuid&include=column_stats",
                "$readScanPath?snapshot=5&table_uuid=$tableUuid&stats_fields=1",
                "$readScanPath?snapshot=5&table_uuid=$tableUuid&extra=1",
                readScanPath.replace("metrics", "%6detrics") + "?snapshot=5&table_uuid=$tableUuid",
            )) {
                val response = client.get(target) { header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(response.status).describedAs(target).isEqualTo(HttpStatusCode.BadRequest)
            }
            val withBody =
                client.get("$readScanPath?snapshot=5&table_uuid=$tableUuid") {
                    header(HttpHeaders.Authorization, "Bearer $jwt")
                    setBody("x")
                }
            assertThat(withBody.status).isEqualTo(HttpStatusCode.BadRequest)
            assertThat(requests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `scan GET refuses out of grant data files and deletion vectors with redacted 403`() =
        testApplication {
            val cfg = config(withReadSession = true)
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()
            val outOfGrantPaths =
                listOf(
                    "s3://example-customer/tables/other.parquet",
                    "s3://other-bucket/tables/data.parquet",
                    "s3://example-remote/events/team_id=42/part-0.parquet",
                    "s3://example-owner/root/2c/t/fixture_3/manifest",
                    "s3://example-customer/tables/../tables/data.parquet",
                    "https://example-customer/tables/data.parquet",
                    "s3://example-customer/tables/data.parquet/",
                    "",
                )
            for (badPath in outOfGrantPaths) {
                body = scanSuccess.replace("s3://example-customer/tables/data.parquet", badPath)
                val response =
                    client.get("$readScanPath?snapshot=5&table_uuid=$tableUuid") {
                        header(HttpHeaders.Authorization, "Bearer $jwt")
                    }
                val text = response.bodyAsText()
                assertThat(response.status).describedAs(badPath).isEqualTo(HttpStatusCode.Forbidden)
                assertThat(response.headers[HttpHeaders.CacheControl]).isEqualTo("no-store")
                assertThat(text).doesNotContain(
                    "example-customer",
                    "other-bucket",
                    "example-remote",
                    "example-owner",
                    "data.parquet",
                    "other.parquet",
                    "manifest",
                    "fictional-key",
                    "fictional-secret",
                    "fictional-session",
                )
            }

            // Prefix grant rejects traversal, empty segments, control characters, and out-of-prefix keys.
            val prefixJwt =
                signedRead { claims ->
                    val grant = claims["grants"][0] as ObjectNode
                    grant.remove("object_key")
                    grant.put("prefix", "tables/team_id=42/")
                }
            for (badPrefixPath in listOf(
                "s3://example-customer/tables/team_id=42/../team_id=43/part(1).parquet",
                "s3://example-customer/tables/team_id=42/./part(1).parquet",
                "s3://example-customer/tables/team_id=42//part(1).parquet",
                "s3://example-customer/tables/team_id=42/part\\u0000.parquet",
                "s3://example-customer/tables/team_id=43/part(1).parquet",
            )) {
                body = scanSuccess.replace("s3://example-customer/tables/data.parquet", badPrefixPath)
                val response =
                    client.get("$readScanPath?snapshot=5&table_uuid=$tableUuid") {
                        header(HttpHeaders.Authorization, "Bearer $prefixJwt")
                    }
                assertThat(response.status).describedAs(badPrefixPath).isEqualTo(HttpStatusCode.Forbidden)
                assertThat(response.headers[HttpHeaders.CacheControl]).isEqualTo("no-store")
                assertThat(response.bodyAsText()).doesNotContain("example-customer", "part(1)", "team_id=43")
            }

            // Data file matches grant, but deletion vector lies outside grant.
            body =
                scanSuccess.replace(
                    """"delete_file": null""",
                    """"delete_file": {"delete_file_id": 9, "path": "s3://example-customer/tables/outside.puffin", """ +
                        """"delete_count": 1, "file_size_bytes": 64, "begin_snapshot": 3}""",
                )
            val dvRefusal =
                client.get("$readScanPath?snapshot=5&table_uuid=$tableUuid") {
                    header(HttpHeaders.Authorization, "Bearer $jwt")
                }
            assertThat(dvRefusal.status).isEqualTo(HttpStatusCode.Forbidden)
            assertThat(dvRefusal.headers[HttpHeaders.CacheControl]).isEqualTo("no-store")
            assertThat(dvRefusal.bodyAsText()).doesNotContain("outside.puffin", "example-customer", "fictional-key")
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `scan GET refuses mismatched identity and Hoglite errors without SQL fallback`() =
        testApplication {
            val cfg = config(withReadSession = true)
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()

            status = 404
            body = """{"code":"not_found"}"""
            val missing =
                client.get("$readScanPath?snapshot=5&table_uuid=$tableUuid") {
                    header(HttpHeaders.Authorization, "Bearer $jwt")
                }
            assertThat(missing.status).isEqualTo(HttpStatusCode.NotFound)
            assertThat(missing.headers[HttpHeaders.CacheControl]).isEqualTo("no-store")

            status = 403
            body = """{"code":"forbidden"}"""
            val forbidden =
                client.get("$readScanPath?snapshot=5&table_uuid=$tableUuid") {
                    header(HttpHeaders.Authorization, "Bearer $jwt")
                }
            assertThat(forbidden.status).isEqualTo(HttpStatusCode.Forbidden)
            assertThat(forbidden.headers[HttpHeaders.CacheControl]).isEqualTo("no-store")
            assertThat(forbidden.bodyAsText()).doesNotContain("example-customer", "fictional-key")

            for ((code, peerBody) in listOf(
                503 to """{"code":"unavailable","detail":"peer-secret"}""",
                200 to scanSuccess.replace(tableUuid, "22222222-2222-4222-8222-222222222222"),
                200 to scanSuccess.replace("\"read_snapshot_id\": 5", "\"read_snapshot_id\": 6"),
                200 to scanSuccess.replace("\"reports\"", "\"other\""),
                200 to scanSuccess.replace(Regex(",\\s*\"explicit_row_ids\":\\s*false"), ""),
                200 to scanSuccess.replace("\"explicit_row_ids\": false", "\"explicit_row_ids\": \"false\""),
                200 to "{not-json",
            )) {
                status = code
                body = peerBody
                val response =
                    client.get("$readScanPath?snapshot=5&table_uuid=$tableUuid") {
                        header(HttpHeaders.Authorization, "Bearer $jwt")
                    }
                assertThat(response.status).isEqualTo(HttpStatusCode.ServiceUnavailable)
                assertThat(response.headers[HttpHeaders.CacheControl]).isEqualTo("no-store")
                assertThat(response.bodyAsText()).doesNotContain("peer-secret", "other")
            }
            assertThat(requests).hasSize(9)
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `only the opted in namespace GET forwards two exact dedicated tokens`() =
        testApplication {
            val cfg = config(withReadSession = true)
            application { harness(cfg, readFixture = true) }
            val first = readVector["token"].asText()
            val second = signedRead { it.put("usr", "example.bob").put("sub", "example.bob").put("jti", "token-b") }
            for (jwt in listOf(first, second, first)) {
                val response = client.get(readPath) { header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(response.status).isEqualTo(HttpStatusCode.OK)
                assertThat(response.bodyAsText()).contains("\"namespace_id\":17")
            }
            val peer = "GET /v1/t/fixture_3/metadata/v1/catalogs/example-catalog/namespaces/reports"
            assertThat(requests).containsExactly(peer to null, peer to null, peer to null)
            assertThat(sessionDigests).containsExactly(
                digest("Bearer $first"),
                digest("Bearer $second"),
                digest("Bearer $first"),
            )
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `namespace GET rejects noncanonical requests and bodies`() =
        testApplication {
            val cfg = config(withReadSession = true)
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()
            for (target in listOf("$readPath?snapshot=1", "$readPath?", readPath.replace("reports", "%72eports"))) {
                assertThat(client.get(target) { header(HttpHeaders.Authorization, "Bearer $jwt") }.status)
                    .isEqualTo(HttpStatusCode.BadRequest)
            }
            val withBody =
                client.get(readPath) {
                    header(HttpHeaders.Authorization, "Bearer $jwt")
                    setBody("x")
                }
            assertThat(withBody.status).isEqualTo(HttpStatusCode.BadRequest)
            assertThat(requests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `an expired session and repeated bearer headers never reach the backend`() =
        testApplication {
            val cfg = config(withReadSession = true)
            application { harness(cfg, readFixture = true) }
            val now = readVector["verify_at"].asLong()
            val jwt =
                signedRead {
                    it.put("exp", now)
                    (it["aws"] as ObjectNode).put("expires_at", now)
                }
            assertThat(client.get(readPath) { header(HttpHeaders.Authorization, "Bearer $jwt") }.status)
                .isEqualTo(HttpStatusCode.Unauthorized)
            val valid = readVector["token"].asText()
            val repeated =
                client.get(readPath) {
                    header(HttpHeaders.Authorization, "Bearer $valid")
                    header(HttpHeaders.Authorization, "Bearer $valid")
                }
            assertThat(repeated.status).isEqualTo(HttpStatusCode.Unauthorized)
            assertThat(requests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `an owner refusal never triggers a second request`() =
        testApplication {
            val cfg = config(withReadSession = true)
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()
            status = 503
            body = """{"code":"unavailable"}"""
            val response = client.get(readPath) { header(HttpHeaders.Authorization, "Bearer $jwt") }
            assertThat(response.status).isEqualTo(HttpStatusCode.ServiceUnavailable)
            assertThat(requests).hasSize(1)
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `a disabled read session configuration refuses the dedicated credential`() =
        testApplication {
            val cfg = config(withReadSession = false, readFixture = true)
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()
            assertThat(client.get(readPath) { header(HttpHeaders.Authorization, "Bearer $jwt") }.status)
                .isEqualTo(HttpStatusCode.Unauthorized)
            assertThat(requests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `the public listener refuses the dedicated credential even when the API listener opts in`() =
        testApplication {
            val cfg = config(withReadSession = true)
            application { harness(cfg, readFixture = true, publicListener = true) }
            val jwt = readVector["token"].asText()
            assertThat(client.get(readPath) { header(HttpHeaders.Authorization, "Bearer $jwt") }.status)
                .isEqualTo(HttpStatusCode.Unauthorized)
            assertThat(requests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `a supported miss or rejected mark returns one refusal without legacy reads`() =
        testApplication {
            val cfg = config(withReadSession = true)
            val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
            Metrics.bind(registry)
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()
            status = 404
            body = """{"code":"not_found"}"""
            val missing = client.get(readPath) { header(HttpHeaders.Authorization, "Bearer $jwt") }
            status = 200
            body = success.replace("\"value\":42", "\"value\":43")
            val rejected = client.get(readPath) { header(HttpHeaders.Authorization, "Bearer $jwt") }
            assertThat(missing.status).isEqualTo(HttpStatusCode.NotFound)
            assertThat(rejected.status).isEqualTo(HttpStatusCode.NotFound)
            assertThat(rejected.bodyAsText()).isEqualTo(missing.bodyAsText()).doesNotContain("43")
            val count = registry.find("hoglake_auth_requests_total").tag("outcome", "not_entitled").counter()
            assertThat(count?.count()).isEqualTo(2.0)
            assertThat(requests).hasSize(2)
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `unsupported outcomes cannot trigger a legacy retry or disclose peer bodies`() =
        testApplication {
            val cfg = config(withReadSession = true)
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()
            for ((code, peerBody) in listOf(
                404 to """{"code":"different","secret":"peer-secret"}""",
                501 to """{"code":"unsupported","secret":"peer-secret"}""",
                503 to """{"code":"unavailable","secret":"peer-secret"}""",
                200 to success.replace("\"reports\"", "\"wrong_name\""),
                200 to success.replace("\"team_id\"", "\"unknown_field\""),
                200 to success.replace("\"value\":42", "\"value\":0"),
                200 to success.replace("\"namespace_id\":17", "\"namespace_id\":0"),
                200 to """{"namespace_id":17,"name":"reports"}""",
            )) {
                status = code
                body = peerBody
                val response = client.get(readPath) { header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(response.status).isEqualTo(HttpStatusCode.ServiceUnavailable)
                assertThat(response.bodyAsText()).doesNotContain("peer-secret", "wrong_name", "unknown_field")
            }
            assertThat(requests).hasSize(8)
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `mount hosts for the team family coexist with typed scoped reads without SQL or mount fallback`() =
        testApplication {
            val remoteRequests = CopyOnWriteArrayList<String>()
            val remote = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
            remoteServer = remote
            remote.createContext("/") { exchange ->
                exchange.requestBody.readBytes()
                remoteRequests.add("${exchange.requestMethod} ${exchange.requestURI}")
                exchange.sendResponseHeaders(500, -1)
                exchange.close()
            }
            remote.start()
            val hostsJson =
                """{"hosts":[{"id":"$id","match":{"type":"namespace_family","family":"team"},""" +
                    """"kind":"hoglake","url":"http://127.0.0.1:${remote.address.port}","auth":"forward",""" +
                    """"allow_insecure_http":true}]}"""
            val cfg = config(withReadSession = true, mountHostsJson = hostsJson)
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()

            body = success
            val nsResponse = client.get(readPath) { header(HttpHeaders.Authorization, "Bearer $jwt") }
            assertThat(nsResponse.status).isEqualTo(HttpStatusCode.OK)
            assertThat(nsResponse.bodyAsText()).contains("\"namespace_id\":17", "\"name\":\"reports\"")

            body = tableSuccess
            val tableResponse = client.get(readTablePath) { header(HttpHeaders.Authorization, "Bearer $jwt") }
            assertThat(tableResponse.status).isEqualTo(HttpStatusCode.OK)
            assertThat(tableResponse.headers[HttpHeaders.CacheControl]).isEqualTo("no-store")
            assertThat(mapper.readTree(tableResponse.bodyAsText())["table_uuid"].asText()).isEqualTo(tableUuid)

            body = scanSuccess
            val query = "?snapshot=5&table_uuid=$tableUuid"
            val scanResponse = client.get("$readScanPath$query") { header(HttpHeaders.Authorization, "Bearer $jwt") }
            assertThat(scanResponse.status).isEqualTo(HttpStatusCode.OK)
            assertThat(scanResponse.headers[HttpHeaders.CacheControl]).isEqualTo("no-store")
            assertThat(mapper.readTree(scanResponse.bodyAsText()).size()).isEqualTo(1)

            for (remoteTarget in listOf(
                "/v1/catalogs/example-catalog/namespaces/team_42",
                "/v1/catalogs/example-catalog/namespaces/team_42/tables/events",
                "/v1/catalogs/example-catalog/namespaces/team_42/tables/events/scan$query",
            )) {
                // The relay is off by default: the ordinary verifier refuses the dedicated audiences.
                val response = client.get(remoteTarget) { header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(response.status).describedAs(remoteTarget).isEqualTo(HttpStatusCode.Unauthorized)
            }

            assertThat(requests.map { it.first }).containsExactly(
                "GET /v1/t/fixture_3/metadata/v1/catalogs/example-catalog/namespaces/reports",
                "GET /v1/t/fixture_3/metadata/v1/catalogs/example-catalog/namespaces/reports/tables/metrics",
                "GET /v1/t/fixture_3/metadata/v1/catalogs/example-catalog/namespaces/reports/tables/metrics/scan$query",
            )
            assertThat(remoteRequests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    /** The body that the team-family relay host answers. Its namespace id is not the local one. */
    private val relayed = """{"namespace_id":900,"name":"team_42","remote":true}"""

    /** A team-family relay host that records every request it gets and answers each one with [relayed]. */
    private fun teamRelay(remoteRequests: MutableList<String>): String {
        val remote = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0)
        remoteServer = remote
        remote.createContext("/") { exchange ->
            exchange.requestBody.readBytes()
            remoteRequests.add("${exchange.requestMethod} ${exchange.requestURI}")
            remoteAuthorizations.addAll(exchange.requestHeaders["Authorization"].orEmpty())
            exchange.responseHeaders.set("Content-Type", "application/json")
            val bytes = relayed.toByteArray()
            exchange.sendResponseHeaders(200, bytes.size.toLong())
            exchange.responseBody.use { it.write(bytes) }
        }
        remote.start()
        return """{"hosts":[{"id":"$id","match":{"type":"namespace_family","family":"team"},""" +
            """"kind":"hoglake","url":"http://127.0.0.1:${remote.address.port}","auth":"forward",""" +
            """"allow_insecure_http":true}]}"""
    }

    /** A shared-catalog session token for the same principal: the ordinary audience and profile. */
    private fun sharedSession(): String =
        signedRead {
            it.put("aud", TestTokens.AUDIENCE)
            it.put("profile", "storage-session")
        }

    /** The local-only route of an ordinary `/v1/catalogs/...` target. The query stays as it is. */
    private fun localOnly(target: String): String {
        check(target.startsWith("/v1/catalogs/"))
        return LOCAL_ONLY_PREFIX + target.removePrefix("/v1")
    }

    private val teamPath = "/v1/catalogs/example-catalog/namespaces/team_42"
    private val teamTablePath = "$teamPath/tables/metrics"
    private val teamScanPath = "$teamTablePath/scan?snapshot=5&table_uuid=$tableUuid"

    @Test
    fun `local-only routes refuse reserved team and posthog namespaces while shared path still relays team`() =
        testApplication {
            val remoteRequests = CopyOnWriteArrayList<String>()
            val cfg =
                config(
                    withReadSession = true,
                    mountHostsJson = teamRelay(remoteRequests),
                    reservedNamespaceNames = setOf("posthog"),
                )
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()
            val query = "?snapshot=5&table_uuid=$tableUuid"

            for (target in listOf(
                teamPath,
                teamTablePath,
                teamScanPath,
                "/v1/catalogs/example-catalog/namespaces/posthog",
                "/v1/catalogs/example-catalog/namespaces/posthog/tables/events",
                "/v1/catalogs/example-catalog/namespaces/posthog/tables/events/scan$query",
                "/v1/catalogs/other-catalog/namespaces/reports",
            )) {
                val response = client.get(localOnly(target)) { header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(response.status).describedAs(target).isEqualTo(HttpStatusCode.NotFound)
                assertThat(response.headers[LOCAL_ONLY_MARKER]).describedAs(target).isEqualTo("true")
            }
            assertThat(requests).isEmpty()
            assertThat(remoteRequests).isEmpty()

            // The unmodified shared path on team_42 keeps the team relay and its answer.
            val shared = client.get(teamPath) { header(HttpHeaders.Authorization, "Bearer ${sharedSession()}") }
            assertThat(shared.status).isEqualTo(HttpStatusCode.OK)
            assertThat(shared.headers[LOCAL_ONLY_MARKER]).isNull()
            assertThat(mapper.readTree(shared.bodyAsText())["namespace_id"].asLong()).isEqualTo(900L)
            for (target in listOf(teamTablePath, teamScanPath)) {
                val response = client.get(target) { header(HttpHeaders.Authorization, "Bearer ${sharedSession()}") }
                assertThat(response.headers[LOCAL_ONLY_MARKER]).describedAs(target).isNull()
            }
            assertThat(remoteRequests).hasSize(3)
            for (relay in remoteRequests) {
                assertThat(relay).startsWith("GET ").contains("/namespaces/team_42")
            }
            assertThat(remoteRequests[2]).endsWith("/tables/metrics/scan?snapshot=5&table_uuid=$tableUuid")
            assertThat(requests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `a read session relays the four routes of its own team schema with its exact token`() =
        testApplication {
            val remoteRequests = CopyOnWriteArrayList<String>()
            val cfg = config(withReadSession = true, mountHostsJson = teamRelay(remoteRequests), relay = true)
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()

            for (target in listOf(teamPath, "$teamPath/tables", teamTablePath, teamScanPath)) {
                val response = client.get(target) { header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(response.status).describedAs(target).isEqualTo(HttpStatusCode.OK)
                assertThat(response.headers[LOCAL_ONLY_MARKER]).describedAs(target).isNull()
                assertThat(response.headers[HttpHeaders.CacheControl]).describedAs(target).isEqualTo("no-store")
            }
            assertThat(remoteRequests).containsExactly(
                "GET /v1/catalogs/example-catalog/namespaces/team_42",
                "GET /v1/catalogs/example-catalog/namespaces/team_42/tables",
                "GET /v1/catalogs/example-catalog/namespaces/team_42/tables/metrics",
                "GET $teamScanPath",
            )
            assertThat(remoteAuthorizations).containsOnly("Bearer $jwt")
            // Nothing local ran: no Hoglite owner request and no legacy store read.
            assertThat(requests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `a read session reaches the relay only for its own catalog and team, on GET`() =
        testApplication {
            val remoteRequests = CopyOnWriteArrayList<String>()
            val cfg = config(withReadSession = true, mountHostsJson = teamRelay(remoteRequests), relay = true)
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()

            // The relay vectors: another team, another catalog with the same
            // organization and team, and the non-canonical spellings.
            for (vector in vectors["relay"].filter { it["expect"]["relay"].asText() == "not_found" }) {
                val request = vector["request"]
                val namespace = request["namespace"].asText()
                if (!namespace.lowercase().startsWith("team_")) continue
                val base = "/v1/catalogs/${request["catalog"].asText()}/namespaces/$namespace"
                for (target in listOf(base, "$base/tables", "$base/tables/metrics", "$base/tables/metrics/scan")) {
                    val response =
                        client.get(target) { header(HttpHeaders.Authorization, "Bearer ${vector["token"].asText()}") }
                    assertThat(response.status).describedAs(target).isEqualTo(HttpStatusCode.NotFound)
                }
            }
            // A write on the team schema is not a relayed route: the ordinary verifier refuses the audiences.
            val write = client.delete(teamTablePath) { header(HttpHeaders.Authorization, "Bearer $jwt") }
            assertThat(write.status).isEqualTo(HttpStatusCode.Unauthorized)
            // A second bearer header and a changed signature never reach the Host.
            val twice =
                client.get(teamPath) {
                    header(HttpHeaders.Authorization, "Bearer $jwt")
                    header(HttpHeaders.Authorization, "Bearer $jwt")
                }
            assertThat(twice.status).isEqualTo(HttpStatusCode.Unauthorized)
            val tampered = client.get(teamPath) { header(HttpHeaders.Authorization, "Bearer ${jwt.dropLast(2)}xx") }
            assertThat(tampered.status).isEqualTo(HttpStatusCode.Unauthorized)

            assertThat(remoteRequests).isEmpty()
            assertThat(requests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `with the relay on the local-only routes still refuse the session's own team schema`() =
        testApplication {
            val remoteRequests = CopyOnWriteArrayList<String>()
            val cfg = config(withReadSession = true, mountHostsJson = teamRelay(remoteRequests), relay = true)
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()

            for (target in listOf(teamPath, teamTablePath, teamScanPath)) {
                val response = client.get(localOnly(target)) { header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(response.status).describedAs(target).isEqualTo(HttpStatusCode.NotFound)
                assertThat(response.headers[LOCAL_ONLY_MARKER]).describedAs(target).isEqualTo("true")
            }
            assertThat(remoteRequests).isEmpty()
            assertThat(requests).isEmpty()
            assertThat(legacy).isEmpty()

            // The same token relays on the shared route, so the 404 above is the route's and not the token's.
            val shared = client.get(teamPath) { header(HttpHeaders.Authorization, "Bearer $jwt") }
            assertThat(shared.status).isEqualTo(HttpStatusCode.OK)
            assertThat(remoteRequests).hasSize(1)
        }

    @Test
    fun `without a read session profile the relay still refuses the dedicated audiences`() =
        testApplication {
            val remoteRequests = CopyOnWriteArrayList<String>()
            val cfg = config(withReadSession = false, readFixture = true, mountHostsJson = teamRelay(remoteRequests))
            application { harness(cfg, readFixture = true) }
            val response =
                client.get(teamPath) { header(HttpHeaders.Authorization, "Bearer ${readVector["token"].asText()}") }
            assertThat(response.status).isEqualTo(HttpStatusCode.Unauthorized)
            assertThat(remoteRequests).isEmpty()
        }

    @Test
    fun `an authorized local-only lookup of an absent local name is ordinary absence with zero relay calls`() =
        testApplication {
            val remoteRequests = CopyOnWriteArrayList<String>()
            val cfg = config(withReadSession = true, mountHostsJson = teamRelay(remoteRequests))
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()
            val query = "?snapshot=5&table_uuid=$tableUuid"

            status = 404
            body = """{"code":"not_found"}"""
            for (target in listOf(readPath, readTablePath, "$readScanPath$query")) {
                val response = client.get(localOnly(target)) { header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(response.status).describedAs(target).isEqualTo(HttpStatusCode.NotFound)
                assertThat(response.headers[LOCAL_ONLY_MARKER]).describedAs(target).isEqualTo("true")
            }
            assertThat(requests).hasSize(3)
            assertThat(remoteRequests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `the local-only routes keep token failures and principal checks and cannot bypass the relay policy`() =
        testApplication {
            val remoteRequests = CopyOnWriteArrayList<String>()
            val cfg = config(withReadSession = true, mountHostsJson = teamRelay(remoteRequests))
            application { harness(cfg, readFixture = true) }
            val query = "?snapshot=5&table_uuid=$tableUuid"

            for (target in listOf(readPath, readTablePath, "$readScanPath$query", teamPath)) {
                val anonymous = client.get(localOnly(target))
                assertThat(anonymous.status).describedAs(target).isEqualTo(HttpStatusCode.Unauthorized)
                assertThat(anonymous.headers[LOCAL_ONLY_MARKER]).describedAs(target).isEqualTo("true")
                for (bad in listOf("not.a.jwt", sharedSession())) {
                    val response = client.get(localOnly(target)) { header(HttpHeaders.Authorization, "Bearer $bad") }
                    assertThat(response.status).describedAs(target).isEqualTo(HttpStatusCode.Unauthorized)
                }
            }
            val doubled =
                client.get(localOnly(readPath)) {
                    header(HttpHeaders.Authorization, "Bearer ${readVector["token"].asText()}")
                    header(HttpHeaders.Authorization, "Bearer ${readVector["token"].asText()}")
                }
            assertThat(doubled.status).isEqualTo(HttpStatusCode.Unauthorized)
            assertThat(requests).isEmpty()
            assertThat(remoteRequests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `malformed local-only routes, methods and queries fail closed before any read`() =
        testApplication {
            val remoteRequests = CopyOnWriteArrayList<String>()
            val cfg = config(withReadSession = true, mountHostsJson = teamRelay(remoteRequests))
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()

            for (target in listOf(
                LOCAL_ONLY_PREFIX,
                "$LOCAL_ONLY_PREFIX/",
                "$LOCAL_ONLY_PREFIX/catalogs/example-catalog",
                "$LOCAL_ONLY_PREFIX/catalogs/example-catalog/namespaces",
                "$LOCAL_ONLY_PREFIX//catalogs/example-catalog/namespaces/reports",
                localOnly("$readPath/tables"),
                localOnly("$readTablePath/files"),
                "$LOCAL_ONLY_PREFIX/namespaces/reports",
            )) {
                val response = client.get(target) { header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(response.status).describedAs(target).isEqualTo(HttpStatusCode.UnprocessableEntity)
                assertThat(response.headers[LOCAL_ONLY_MARKER]).describedAs(target).isNull()
            }

            for (target in listOf(readPath, readTablePath)) {
                val posted =
                    client.post(localOnly(target)) {
                        header(HttpHeaders.Authorization, "Bearer $jwt")
                        setBody("{}")
                    }
                assertThat(posted.status).describedAs(target).isEqualTo(HttpStatusCode.MethodNotAllowed)
                assertThat(posted.headers[HttpHeaders.Allow]).isEqualTo("GET")
                assertThat(posted.headers[LOCAL_ONLY_MARKER]).isNull()
                val head = client.head(localOnly(target)) { header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(head.status).describedAs(target).isEqualTo(HttpStatusCode.MethodNotAllowed)
            }

            for (target in listOf(
                localOnly("$readPath?snapshot=5"),
                localOnly("$readTablePath?totals=true"),
                localOnly("$readTablePath?totals=false&totals=false"),
                localOnly("$readTablePath/scan"),
                localOnly("$readTablePath/scan?snapshot=5"),
                localOnly("$readTablePath/scan?snapshot=5&snapshot=5&table_uuid=$tableUuid"),
                localOnly("$readTablePath/scan?snapshot=05&table_uuid=$tableUuid"),
                localOnly("$readTablePath/scan?snapshot=5&table_uuid=${tableUuid.uppercase()}"),
                localOnly("$readTablePath/scan?snapshot=5&table_uuid=$tableUuid&include=column_stats"),
            )) {
                val response = client.get(target) { header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(response.status).describedAs(target).isEqualTo(HttpStatusCode.BadRequest)
                assertThat(response.headers[LOCAL_ONLY_MARKER]).describedAs(target).isEqualTo("true")
            }

            assertThat(requests).isEmpty()
            assertThat(remoteRequests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `a server without the routes cannot match a local-only path to an ordinary route`() {
        for (target in listOf(teamPath, teamTablePath, "$teamTablePath/scan")) {
            assertThat(AuthPolicy.match(HttpMethod.Get, localOnly(target))).describedAs(target).isNull()
            assertThat(ReadSessionProfile.canonicalLocalOnlyPath(localOnly(target))).isEqualTo(target)
        }
        assertThat(ReadSessionProfile.canonicalLocalOnlyPath(teamPath)).isNull()
        assertThat(ReadSessionProfile.isLocalOnlyPath("/v1/local-onlyx/catalogs/example-catalog")).isFalse()
    }

    @Test
    fun `a listener without a read session refuses the local-only routes without the marker`() =
        testApplication {
            val remoteRequests = CopyOnWriteArrayList<String>()
            val cfg = config(withReadSession = false, readFixture = true, mountHostsJson = teamRelay(remoteRequests))
            application { harness(cfg, readFixture = true) }
            for (jwt in listOf(readVector["token"].asText(), sharedSession())) {
                for (target in listOf(teamPath, teamTablePath, teamScanPath)) {
                    val response = client.get(localOnly(target)) { header(HttpHeaders.Authorization, "Bearer $jwt") }
                    assertThat(response.status).describedAs(target).isEqualTo(HttpStatusCode.NotImplemented)
                    assertThat(response.headers[LOCAL_ONLY_MARKER]).describedAs(target).isNull()
                }
            }
            assertThat(requests).isEmpty()
            assertThat(remoteRequests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `the public listener refuses the local-only routes`() =
        testApplication {
            val cfg = config(withReadSession = true)
            application { harness(cfg, readFixture = true, publicListener = true) }
            for (target in listOf(readPath, readTablePath, "$readScanPath?snapshot=5&table_uuid=$tableUuid")) {
                val response =
                    client.get(localOnly(target)) {
                        header(HttpHeaders.Authorization, "Bearer ${readVector["token"].asText()}")
                    }
                assertThat(response.status).describedAs(target).isEqualTo(HttpStatusCode.NotImplemented)
                assertThat(response.headers[LOCAL_ONLY_MARKER]).describedAs(target).isNull()
            }
            assertThat(requests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `the local-only routes read a non-reserved local namespace and table as the plain routes do`() =
        testApplication {
            val cfg = config(withReadSession = true)
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()
            body = success
            val namespace = client.get(localOnly(readPath)) { header(HttpHeaders.Authorization, "Bearer $jwt") }
            assertThat(namespace.status).isEqualTo(HttpStatusCode.OK)
            assertThat(namespace.headers[LOCAL_ONLY_MARKER]).isEqualTo("true")
            body = tableSuccess
            val table = client.get(localOnly(readTablePath)) { header(HttpHeaders.Authorization, "Bearer $jwt") }
            assertThat(table.status).isEqualTo(HttpStatusCode.OK)
            assertThat(table.headers[LOCAL_ONLY_MARKER]).isEqualTo("true")
            val plain = client.get(readTablePath) { header(HttpHeaders.Authorization, "Bearer $jwt") }
            assertThat(plain.status).isEqualTo(HttpStatusCode.OK)
            assertThat(plain.headers[LOCAL_ONLY_MARKER]).isNull()
            assertThat(requests.map { it.first }).containsExactly(
                "GET /v1/t/fixture_3/metadata/v1/catalogs/example-catalog/namespaces/reports",
                "GET /v1/t/fixture_3/metadata/v1/catalogs/example-catalog/namespaces/reports/tables/metrics",
                "GET /v1/t/fixture_3/metadata/v1/catalogs/example-catalog/namespaces/reports/tables/metrics",
            )
            assertThat(legacy).isEmpty()
        }
}
