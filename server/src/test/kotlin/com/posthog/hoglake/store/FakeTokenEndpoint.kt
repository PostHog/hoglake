package com.posthog.hoglake.store

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.posthog.hoglake.auth.TestTokens
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.net.URI
import java.net.URLDecoder
import java.security.KeyPair
import java.util.Base64
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors

/**
 * hogtower's token endpoint for the events mount exchange (hogtower#181),
 * as far as a mount Client can see it: it records every request it gets,
 * checks the client's Basic credentials and the request's shape, and mints
 * a real token signed with [pair], which a Host verifying [pair]'s JWKS
 * accepts. The mount token is the contract's: `ten: scoped`, `cat` only the
 * mount at read, `prd.team_id` the subject's values that are federated,
 * the subject's `sub`, `usr`, `cli` and `org`, no `rtg`, and an `exp` no
 * later than the subject's.
 */
class FakeTokenEndpoint(
    private val pair: KeyPair,
    val clientId: String = "hls_0123456789abcdef01234567",
    val clientSecret: String = "s3cret-for-the-mount-exchange+/=&% 0123456789",
    /** Org id -> its federated teams. An org missing here has no mount. */
    private val federated: Map<String, Set<Long>> = emptyMap(),
) : AutoCloseable {
    /** One request as it arrived. */
    data class Seen(
        val method: String,
        val rawPath: String,
        val rawQuery: String?,
        val headers: Map<String, List<String>>,
        val body: String,
    ) {
        /** The form body's fields, in order, decoded. */
        val form: List<Pair<String, String>>
            get() =
                body.split('&').filter { it.isNotEmpty() }.map {
                    val (name, value) = it.split('=', limit = 2)
                    URLDecoder.decode(name, Charsets.UTF_8) to URLDecoder.decode(value, Charsets.UTF_8)
                }

        fun field(name: String): String? = form.singleOrNull { it.first == name }?.second
    }

    val seen = CopyOnWriteArrayList<Seen>()

    /** Every token minted, newest last. */
    val minted = CopyOnWriteArrayList<String>()

    /** Replaces the contract's answer when set: for refusals, garbage and delays. */
    @Volatile
    var answer: ((HttpExchange, Seen) -> Unit)? = null

    /** The `expires_in` to state instead of the token's real lifetime, when set. */
    @Volatile
    var statedLifetime: Long? = null

    private val mapper = jacksonObjectMapper()
    private val workers = Executors.newCachedThreadPool()
    private val server =
        HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).also { server ->
            server.executor = workers
            server.createContext("/") { exchange ->
                val uri = exchange.requestURI
                val request =
                    Seen(
                        exchange.requestMethod,
                        uri.rawPath,
                        uri.rawQuery,
                        exchange.requestHeaders.mapKeys { it.key.lowercase() },
                        exchange.requestBody.readAllBytes().toString(Charsets.UTF_8),
                    )
                seen += request
                try {
                    (answer ?: ::contract)(exchange, request)
                } finally {
                    exchange.close()
                }
            }
            server.start()
        }

    val port: Int get() = server.address.port
    val tokenUrl: URI get() = URI("http://127.0.0.1:$port/oauth/token")

    /** The `Authorization` header the configured client must send. */
    val basic: String
        get() =
            "Basic " +
                Base64.getEncoder().encodeToString(
                    (
                        java.net.URLEncoder.encode(clientId, Charsets.UTF_8) + ":" +
                            java.net.URLEncoder.encode(clientSecret, Charsets.UTF_8)
                    ).toByteArray(),
                )

    fun exchangeConfig(timeoutMs: Long = 5_000) =
        MountExchange(tokenUrl, clientId, ClientSecret(clientSecret), timeoutMs)

    /** hogtower#181's answer to [request]. */
    fun contract(
        exchange: HttpExchange,
        request: Seen,
    ) {
        if (request.method != "POST" || request.rawPath != "/oauth/token" || request.rawQuery != null) {
            return exchange.error(400, "invalid_request")
        }
        if (credentials(request) != clientId to clientSecret) return exchange.error(401, "invalid_client")
        val expected = setOf("grant_type", "subject_token", "subject_token_type", "resource", "audience")
        if (request.form.map { it.first }.toSet() != expected || request.form.size != expected.size) {
            return exchange.error(400, "invalid_request")
        }
        if (request.field("grant_type") != "urn:ietf:params:oauth:grant-type:token-exchange") {
            return exchange.error(400, "unsupported_grant_type")
        }
        if (request.field("subject_token_type") != "urn:ietf:params:oauth:token-type:access_token") {
            return exchange.error(400, "invalid_request")
        }
        if (request.field("audience") != "hoglake") return exchange.error(400, "invalid_target")
        val subject = claims(request.field("subject_token")!!) ?: return exchange.error(400, "invalid_grant")
        val org = subject["org"]?.asText().orEmpty()
        val cat = subject["cat"]
        val now = System.currentTimeMillis() / 1000
        if (subject["ten"]?.asText() != "scoped" || cat == null || cat.size() != 1 || cat[org] == null ||
            subject["exp"].asLong() - now < 1
        ) {
            return exchange.error(400, "invalid_grant")
        }
        val mount = "$org-events"
        val teams = federated[org]
        if (request.field("resource") != "urn:posthog:hoglake:catalog:$mount" || teams.isNullOrEmpty()) {
            return exchange.error(400, "invalid_target")
        }
        val held = subject["prd"]["team_id"]
        val values =
            if (held.has("*")) teams else teams.filter { held.has(it.toString()) }
        if (values.isEmpty()) return exchange.error(400, "invalid_grant")
        val exp = minOf(now + 900, subject["exp"].asLong())
        val prd = values.joinToString(",") { "\"$it\":\"read\"" }
        val token =
            TestTokens.mint(
                pair,
                """{"iss":"${TestTokens.ISSUER}","aud":"${TestTokens.AUDIENCE}","sub":"${subject["sub"].asText()}",""" +
                    """"jti":"minted-${minted.size + 1}","ten":"scoped","org":"$org",""" +
                    """"cat":{"$mount":["read"]},"prd":{"team_id":{$prd}},"cli":"${subject["cli"].asText()}",""" +
                    """"usr":"${subject["usr"].asText()}","exp":$exp,"iat":$now,"nbf":$now}""",
            )
        minted += token
        exchange.json(
            200,
            """{"access_token":"$token","issued_token_type":"urn:ietf:params:oauth:token-type:access_token",""" +
                """"token_type":"Bearer","expires_in":${statedLifetime ?: (exp - now)}}""",
        )
    }

    /**
     * The client's id and secret as hogtower reads them (`token.go`,
     * `clientCredentials`): HTTP Basic, split at the first colon, each
     * half form-decoded (RFC 6749 §2.3.1). Null when the header is not that.
     */
    private fun credentials(request: Seen): Pair<String, String>? =
        runCatching {
            val header = request.headers.getValue("authorization").single()
            require(header.startsWith("Basic "))
            val (id, secret) = String(Base64.getDecoder().decode(header.removePrefix("Basic "))).split(':', limit = 2)
            URLDecoder.decode(id, Charsets.UTF_8) to URLDecoder.decode(secret, Charsets.UTF_8)
        }.getOrNull()

    private fun claims(token: String): JsonNode? =
        runCatching { mapper.readTree(Base64.getUrlDecoder().decode(token.split('.')[1])) }.getOrNull()

    private val closed = java.util.concurrent.atomic.AtomicBoolean()

    /** Stops answering; a later request finds nothing listening. Closing twice is closing once. */
    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        server.stop(0)
        workers.shutdownNow()
    }

    companion object {
        fun HttpExchange.json(
            status: Int,
            body: String,
            contentType: String = "application/json",
            headers: Map<String, String> = emptyMap(),
        ) {
            val bytes = body.toByteArray()
            responseHeaders.add("Content-Type", contentType)
            headers.forEach { (name, value) -> responseHeaders.add(name, value) }
            sendResponseHeaders(status, if (bytes.isEmpty()) -1 else bytes.size.toLong())
            if (bytes.isNotEmpty()) responseBody.use { it.write(bytes) }
        }

        fun HttpExchange.error(
            status: Int,
            code: String,
            headers: Map<String, String> = emptyMap(),
        ) = json(status, """{"error":"$code","error_description":"refused"}""", headers = headers)
    }
}
