package com.posthog.hoglake.auth

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.posthog.hoglake.Config
import com.posthog.hoglake.api.installApiRoutes
import com.posthog.hoglake.api.installErrorMapping
import com.posthog.hoglake.api.installScanRoutes
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
import com.sun.net.httpserver.HttpServer
import io.ktor.client.request.get
import io.ktor.client.request.head
import io.ktor.client.request.header
import io.ktor.client.request.setBody
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
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

    @Volatile
    private var status = 200

    @Volatile
    private var body = success

    @AfterEach
    fun stop() {
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
        enabled: Boolean = true,
        withReadSession: Boolean = false,
        readFixture: Boolean = withReadSession,
        table: String? = null,
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
        val binding =
            mapper.createObjectNode()
                .put("catalog", if (readFixture) "example-catalog" else "lake")
                .put("backend", "hoglite")
                .put("backend_id", (if (readFixture) readBackend else id).toString())
                .put("tenant", if (readFixture) "fixture_3" else "opaque_1")
                .put("namespace", "reports")
        if (withReadSession) {
            binding.putObject("read_session").put("profile", vectors["config"]["profile"].asText())
                .set<ObjectNode>("remote_targets", vectors["config"]["remote_targets"])
        }
        if (table != null) {
            binding.put("table", table)
        }
        val bindingFile = Files.writeString(dir.resolve("binding.json"), binding.toString())
        return Config(
            backendsFile = backendsFile.toString(),
            scopedNamespaceFile = if (enabled) bindingFile.toString() else "",
            postgresBackendId = postgresId.toString(),
            authMode = "enforce",
            authJwksFile = "mounted-public-jwks.json",
            authPredicateFields = "team_id",
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
        val selector = CatalogStoreSelector(postgres, cfg)
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
                ) { if (readFixture) readVector["verify_at"].asLong() else 1_700_000_100L },
            )
        if (publicListener) {
            installAuth(service.forPublicListener())
        } else {
            installAuth(service, cfg.scopedNamespaceBinding)
        }
        installNamespaceGate(selector)
        install(ContentNegotiation) { jackson { configureHoglakeWire() } }
        install(StatusPages) { installErrorMapping() }
        installApiRoutes(selector)
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
            val cfg = config(withReadSession = true, table = "metrics")
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
            val cfg = config(withReadSession = true, table = "metrics")
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
            val cfg = config(withReadSession = true, table = "metrics")
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
    fun `unbound tables wrong methods legacy tokens and public listener stay closed`() =
        testApplication {
            val cfg = config(withReadSession = true, table = "metrics")
            body = tableSuccess
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()
            val query = "?snapshot=5&table_uuid=$tableUuid"
            for (target in listOf(
                "$readPath/tables/other",
                "$readPath/tables/other/scan$query",
                readTablePath.replace("reports", "other"),
                readScanPath.replace("example-catalog", "other-catalog") + query,
            )) {
                val response = client.get(target) { header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(response.status).describedAs(target).isEqualTo(HttpStatusCode.Unauthorized)
            }
            assertThat(client.head(readTablePath) { header(HttpHeaders.Authorization, "Bearer $jwt") }.status)
                .isEqualTo(HttpStatusCode.Unauthorized)
            assertThat(client.head("$readScanPath$query") { header(HttpHeaders.Authorization, "Bearer $jwt") }.status)
                .isEqualTo(HttpStatusCode.Unauthorized)
            val readFields =
                listOf("profile", "version", "purpose", "primary_catalog", "remote_schemas", "aws", "grants")
            val metadata =
                signedRead {
                    it.putArray("aud").add("hoglake").add("hoglite")
                    it.remove(readFields)
                }
            assertThat(client.get(readTablePath) { header(HttpHeaders.Authorization, "Bearer $metadata") }.status)
                .isEqualTo(HttpStatusCode.Unauthorized)
            assertThat(
                client.get("$readScanPath$query") { header(HttpHeaders.Authorization, "Bearer $metadata") }.status,
            ).isEqualTo(HttpStatusCode.Unauthorized)
            val wrongTenant =
                signedRead {
                    (it["rtg"] as ObjectNode).put("t", "fixture_2")
                    (it["grants"][1] as ObjectNode).put("object_key", "root/bf/t/fixture_2/manifest")
                }
            val wrongTenantTable =
                client.get(readTablePath) { header(HttpHeaders.Authorization, "Bearer $wrongTenant") }
            assertThat(wrongTenantTable.status).isEqualTo(HttpStatusCode.NotFound)
            assertThat(wrongTenantTable.headers[HttpHeaders.CacheControl]).isEqualTo("no-store")
            val wrongTenantScan =
                client.get("$readScanPath$query") { header(HttpHeaders.Authorization, "Bearer $wrongTenant") }
            assertThat(wrongTenantScan.status).isEqualTo(HttpStatusCode.NotFound)
            assertThat(wrongTenantScan.headers[HttpHeaders.CacheControl]).isEqualTo("no-store")
            assertThat(requests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `namespace only read session does not opt in table or scan routes`() =
        testApplication {
            val cfg = config(withReadSession = true, table = null)
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()
            assertThat(client.get(readPath) { header(HttpHeaders.Authorization, "Bearer $jwt") }.status)
                .isEqualTo(HttpStatusCode.OK)
            assertThat(client.get(readTablePath) { header(HttpHeaders.Authorization, "Bearer $jwt") }.status)
                .isEqualTo(HttpStatusCode.Unauthorized)
            assertThat(
                client.get("$readScanPath?snapshot=5&table_uuid=$tableUuid") {
                    header(HttpHeaders.Authorization, "Bearer $jwt")
                }.status,
            ).isEqualTo(HttpStatusCode.Unauthorized)
            assertThat(requests).hasSize(1)
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `the public listener refuses dedicated tokens on bound table and scan routes`() =
        testApplication {
            val cfg = config(withReadSession = true, table = "metrics")
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
    fun `the public listener keeps ordinary tenant and scoped table and scan reads working`() =
        testApplication {
            val cfg = config(withReadSession = true, table = "metrics")
            application { harness(cfg, guardLegacy = false, readFixture = true, publicListener = true) }
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
            assertThat(legacy).contains("getTable", "planScan")
            assertThat(requests).isEmpty()
        }

    @Test
    fun `the opted in scan GET forwards exact tokens and returns ScanFileDto with optional DV`() =
        testApplication {
            val cfg = config(withReadSession = true, table = "metrics")
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
            val cfg = config(withReadSession = true, table = "metrics")
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
            val cfg = config(withReadSession = true, table = "metrics")
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
            val cfg = config(withReadSession = true, table = "metrics")
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
    fun `the dedicated credential cannot reach other routes or use a different placement`() =
        testApplication {
            val cfg = config(withReadSession = true)
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()
            for (target in listOf(
                readPath.replace("reports", "other"),
                readPath.replace("example-catalog", "other-catalog"),
                "$readPath/tables",
            )) {
                val response = client.get(target) { header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(response.status).isEqualTo(HttpStatusCode.Unauthorized)
            }
            assertThat(client.head(readPath) { header(HttpHeaders.Authorization, "Bearer $jwt") }.status)
                .isEqualTo(HttpStatusCode.Unauthorized)
            val wrongTenant =
                signedRead {
                    (it["rtg"] as ObjectNode).put("t", "fixture_2")
                    (it["grants"][1] as ObjectNode).put("object_key", "root/bf/t/fixture_2/manifest")
                }
            assertThat(client.get(readPath) { header(HttpHeaders.Authorization, "Bearer $wrongTenant") }.status)
                .isEqualTo(HttpStatusCode.NotFound)
            assertThat(requests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `the opted in namespace refuses legacy credentials and noncanonical requests`() =
        testApplication {
            val cfg = config(withReadSession = true)
            application { harness(cfg, readFixture = true) }
            val jwt = readVector["token"].asText()
            val readFields =
                listOf("profile", "version", "purpose", "primary_catalog", "remote_schemas", "aws", "grants")
            val metadata =
                signedRead {
                    it.putArray("aud").add("hoglake").add("hoglite")
                    it.remove(readFields)
                }
            assertThat(client.get(readPath) { header(HttpHeaders.Authorization, "Bearer $metadata") }.status)
                .isEqualTo(HttpStatusCode.Unauthorized)
            for (target in listOf("$readPath?snapshot=1", readPath.replace("reports", "%72eports"))) {
                assertThat(client.get(target) { header(HttpHeaders.Authorization, "Bearer $jwt") }.status)
                    .isEqualTo(HttpStatusCode.BadRequest)
            }
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
    fun `the metadata configuration refuses the dedicated credential`() =
        testApplication {
            val cfg = config(readFixture = true)
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
    fun `the exact route forwards the exact verified JWT without legacy reads`() =
        testApplication {
            val cfg = config()
            val jwt = token(claims().replace(""""aud":"hoglake"""", """"aud":["hoglake","hoglite"]"""))
            application { harness(cfg) }
            val response = client.get(path) { header(HttpHeaders.Authorization, "Bearer $jwt") }
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            assertThat(response.bodyAsText()).contains("\"namespace_id\":17", "\"name\":\"reports\"")
            assertThat(requests).containsExactly(
                "GET /v1/t/opaque_1/metadata/v1/catalogs/lake/namespaces/reports" to "Bearer $jwt",
            )
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `the selected route rejects other authority without fallback`() =
        testApplication {
            val cfg = config()
            application { harness(cfg) }
            val noPlacement = TestTokens.scopedClaims(catalogs = """{"lake":["read"]}""")
            val postgresPlacement = noPlacement.dropLast(1) + ""","rtg":{"b":"postgres","be":"$postgresId"}}"""
            for (claims in listOf(
                claims(backend = otherId),
                claims(tenant = "other_tenant"),
                claims(catalog = "other"),
                claims().dropLast(1) + """, "storage": {"fixture": true}}""",
                noPlacement,
                postgresPlacement,
            )) {
                val response = client.get(path) { header(HttpHeaders.Authorization, "Bearer ${token(claims)}") }
                assertThat(response.status).isEqualTo(HttpStatusCode.NotFound)
            }
            assertThat(requests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `JWT failures stop before typed or legacy dispatch`() =
        testApplication {
            val cfg = config()
            application { harness(cfg) }
            val invalid = TestTokens.mint(TestTokens.keyPair(), claims())
            val expired = token(claims().replace("\"exp\":1700000900", "\"exp\":1700000001"))
            val wrongAudience = token(claims().replace(""""aud":"hoglake"""", """"aud":"elsewhere""""))
            for (jwt in listOf("", invalid, expired, wrongAudience)) {
                val response =
                    client.get(
                        path,
                    ) { if (jwt.isNotEmpty()) header(HttpHeaders.Authorization, "Bearer $jwt") }
                assertThat(response.status).isEqualTo(HttpStatusCode.Unauthorized)
            }
            assertThat(requests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `a supported miss or rejected mark returns one refusal without legacy reads`() =
        testApplication {
            val cfg = config()
            val registry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
            Metrics.bind(registry)
            application { harness(cfg) }
            status = 404
            body = """{"code":"not_found"}"""
            val missing = client.get(path) { header(HttpHeaders.Authorization, "Bearer ${token()}") }
            status = 200
            body = success.replace("\"value\":42", "\"value\":43")
            val rejected = client.get(path) { header(HttpHeaders.Authorization, "Bearer ${token()}") }
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
            val cfg = config()
            application { harness(cfg) }
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
                val response = client.get(path) { header(HttpHeaders.Authorization, "Bearer ${token()}") }
                assertThat(response.status).isEqualTo(HttpStatusCode.ServiceUnavailable)
                assertThat(response.bodyAsText()).doesNotContain("peer-secret", "wrong_name", "unknown_field")
            }
            assertThat(requests).hasSize(8)
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `the typed route requires a canonical path without a body or query`() =
        testApplication {
            val cfg = config()
            application { harness(cfg) }
            for (target in listOf("$path?snapshot=1", "$path?", path.replace("reports", "%72eports"))) {
                val response = client.get(target) { header(HttpHeaders.Authorization, "Bearer ${token()}") }
                assertThat(response.status).isEqualTo(HttpStatusCode.BadRequest)
            }
            val withBody =
                client.get(path) {
                    header(HttpHeaders.Authorization, "Bearer ${token()}")
                    setBody("x")
                }
            assertThat(withBody.status).isEqualTo(HttpStatusCode.BadRequest)
            assertThat(requests).isEmpty()
            assertThat(legacy).isEmpty()
        }

    @Test
    fun `a different namespace keeps scoped legacy behavior`() =
        testApplication {
            val cfg = config()
            application { harness(cfg, guardLegacy = false) }
            val jwt = token(TestTokens.scopedClaims(catalogs = """{"lake":["read"]}"""))
            val response =
                client.get(
                    "/v1/catalogs/lake/namespaces/other",
                ) { header(HttpHeaders.Authorization, "Bearer $jwt") }
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            assertThat(response.bodyAsText()).contains("\"namespace_id\":99")
            assertThat(legacy).contains("liveContainment", "getNamespace")
            assertThat(requests).isEmpty()
        }

    @Test
    fun `an absent binding leaves the exact legacy route intact`() =
        testApplication {
            val cfg = config(enabled = false)
            application { harness(cfg, guardLegacy = false) }
            val jwt = token(TestTokens.scopedClaims(catalogs = """{"lake":["read"]}"""))
            val response = client.get(path) { header(HttpHeaders.Authorization, "Bearer $jwt") }
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            assertThat(response.bodyAsText()).contains("\"namespace_id\":99")
            assertThat(legacy).contains("liveContainment", "getNamespace")
            assertThat(requests).isEmpty()
        }

    @Test
    fun `tenant and operator callers retain the legacy namespace handler`() =
        testApplication {
            val cfg = config()
            application { harness(cfg, guardLegacy = false) }
            for (claims in listOf(
                TestTokens.tenantClaims(),
                TestTokens.operatorClaims(),
            )) {
                val response = client.get(path) { header(HttpHeaders.Authorization, "Bearer ${token(claims)}") }
                assertThat(response.status).isEqualTo(HttpStatusCode.OK)
                assertThat(response.bodyAsText()).contains("\"namespace_id\":99")
            }
            assertThat(legacy).containsExactly("getNamespace", "getNamespace")
            assertThat(requests).isEmpty()
        }

    @Test
    fun `typed routing preserves the configured audience membership rule`() =
        testApplication {
            val cfg = config()
            application { harness(cfg, audience = "custom-metadata") }
            val jwt =
                token(claims().replace(""""aud":"hoglake"""", """"aud":["other","custom-metadata","hoglite"]"""))
            val response = client.get(path) { header(HttpHeaders.Authorization, "Bearer $jwt") }
            assertThat(response.status).isEqualTo(HttpStatusCode.OK)
            assertThat(requests.single().second).isEqualTo("Bearer $jwt")
            assertThat(legacy).isEmpty()
        }
}
