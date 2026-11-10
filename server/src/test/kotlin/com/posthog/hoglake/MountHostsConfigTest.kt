package com.posthog.hoglake

import com.posthog.hoglake.store.ClientSecret
import com.posthog.hoglake.store.MountExchange
import com.posthog.hoglake.store.MountHost
import com.posthog.hoglake.store.MountMatch
import com.posthog.hoglake.store.SecretSource
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.net.URI
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID

/** HOGLAKE_MOUNT_HOSTS_FILE (plan row 1.3c): what boots as a mount Client, and what is refused. */
class MountHostsConfigTest {
    @TempDir
    lateinit var dir: Path
    private val id = "3c0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e"
    private val otherId = "4c0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e"
    private val mount = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f0-events"
    private val other = "0190f3c4-1b2a-4c3d-8e4f-a5b6c7d8e9f1-events"

    private fun entry(
        id: String = this.id,
        match: String = """{"type":"events"}""",
        kind: String = "hoglake",
        url: String = "http://gigahog-server.gigahog:8080",
        auth: String = "forward",
        extra: String = "",
    ) = """{"id":"$id","match":$match,"kind":"$kind","url":"$url","auth":"$auth"$extra}"""

    private val secretVariable = "HOGLAKE_MOUNT_EXCHANGE_CLIENT_SECRET"
    private val secret = "a-client-secret-of-forty-bytes-0123456789"
    private val clientId = "hls_0123456789abcdef01234567"

    private fun config(
        hosts: String,
        authMode: String = "enforce",
        predicateFields: String = "team_id",
        allowlist: Set<String> = emptySet(),
        secrets: Map<String, String> = mapOf(secretVariable to secret),
    ): Config {
        val file = Files.writeString(dir.resolve("hosts-${UUID.randomUUID()}.json"), hosts)
        return Config(
            mountHostsFile = file.toString(),
            authMode = authMode,
            authPredicateFields = predicateFields,
            mountAllowlist = allowlist,
            mountSecrets = SecretSource { secrets[it] },
        )
    }

    /** An entry of auth mode `exchange`, with [exchange] as its settings object (null leaves it out). */
    private fun exchanging(
        exchange: String? =
            """{"token_url":"http://hogtower.internal:8080/oauth/token","client_id":"$clientId",""" +
                """"client_secret_env":"$secretVariable"}""",
        auth: String = "exchange",
    ) = entry(auth = auth, extra = exchange?.let { ""","exchange":$it""" } ?: "")

    private fun refused(
        hosts: String,
        message: String,
    ) {
        assertThatThrownBy { config(hosts) }
            .describedAs(hosts)
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining(message)
    }

    @Test
    fun `no file and an empty list are a server that is not a mount client`() {
        assertThat(Config().mountHosts).isEmpty()
        // Nothing a Client needs is demanded of it: auth off, no predicate fields, and it may be a Host.
        val empty = config("""{"hosts":[]}""", authMode = "off", predicateFields = "", allowlist = setOf("*"))
        assertThat(empty.mountHosts).isEmpty()
    }

    @Test
    fun `an entry is its id, matcher, kind, origin and auth, with bounded defaults`() {
        val host = config("""{"hosts":[${entry()}]}""").mountHosts.single()
        assertThat(host).isEqualTo(
            MountHost(
                id = UUID.fromString(id),
                match = MountMatch.Events,
                kind = MountHost.Kind.HOGLAKE,
                origin = URI("http://gigahog-server.gigahog:8080"),
                auth = MountHost.Auth.FORWARD,
                timeoutMs = 75_000,
                maxResponseBytes = 1L shl 30,
                maxConcurrent = 16,
            ),
        )
        val tuned =
            config(
                """{"hosts":[${entry(
                    match = """{"type":"catalog","name":"$mount"}""",
                    url = "https://host.example/",
                    extra = ""","timeout_ms":1000,"max_response_bytes":2048,"max_concurrent":3""",
                )}]}""",
            ).mountHosts.single()
        assertThat(tuned.match).isEqualTo(MountMatch.Catalog(mount))
        assertThat(tuned.origin).isEqualTo(URI("https://host.example"))
        assertThat(listOf(tuned.timeoutMs, tuned.maxResponseBytes, tuned.maxConcurrent.toLong()))
            .containsExactly(1000L, 2048L, 3L)
    }

    @Test
    fun `two hosts may serve two different mounts`() {
        val first = entry(match = """{"type":"catalog","name":"$mount"}""")
        val second = entry(id = otherId, match = """{"type":"catalog","name":"$other"}""", url = "http://b:1")
        val hosts = config("""{"hosts":[$first,$second]}""").mountHosts
        assertThat(
            hosts.map { it.origin.toString() },
        ).containsExactly("http://gigahog-server.gigahog:8080", "http://b:1")
    }

    @Test
    fun `entries that match the same name, or share an id, are refused`() {
        val exact = """{"type":"catalog","name":"$mount"}"""
        for (pair in listOf(
            entry() to entry(id = otherId),
            entry() to entry(id = otherId, match = exact),
            entry(match = exact) to entry(id = otherId),
            entry(match = exact) to entry(id = otherId, match = exact, url = "http://b:1"),
        )) {
            refused("""{"hosts":[${pair.first},${pair.second}]}""", "match the same catalog name")
        }
        refused(
            """{"hosts":[${entry(match = exact)},${entry(match = """{"type":"catalog","name":"$other"}""")}]}""",
            "duplicate mount host id",
        )
    }

    @Test
    fun `a kind, auth mode or matcher this server does not implement is refused`() {
        for (kind in listOf("iceberg", "iceberg-rest", "HOGLAKE", "")) {
            refused("""{"hosts":[${entry(kind = kind)}]}""", "kind '$kind' is not supported")
        }
        for (auth in listOf("client-credentials", "none", "static", "Forward", "")) {
            refused("""{"hosts":[${entry(auth = auth)}]}""", "auth '$auth' is not supported")
        }
        refused(
            """{"hosts":[${entry(match = """{"type":"prefix","name":"x"}""")}]}""",
            "match type must be events or catalog",
        )
        refused("""{"hosts":[${entry(match = """{"type":"events","name":"$mount"}""")}]}""", "takes no other field")
        refused("""{"hosts":[${entry(match = """{"type":"catalog"}""")}]}""", "takes exactly a name")
        refused("""{"hosts":[${entry(match = "\"events\"")}]}""", "requires a match object")
        // An exact name that is not a mount's could be another tenant's catalog.
        for (name in listOf("lake", "millrace-dev", "${mount}2")) {
            refused(
                """{"hosts":[${entry(match = """{"type":"catalog","name":"$name"}""")}]}""",
                "only when the name is a mount's",
            )
        }
    }

    @Test
    fun `a url that is more than an http origin is refused`() {
        for (url in listOf(
            "http://host:8080/v1",
            "http://host:8080/..",
            "http://host:8080?x=1",
            "http://host:8080#f",
            "http://user:pw@host:8080",
            "http://user@host:8080",
            "ftp://host",
            "file:///etc/passwd",
            "//host:8080",
            "host:8080",
            "http://",
            "http:///path",
            "http://host:8080/%2e%2e",
            "http://ho st",
            "",
        )) {
            refused("""{"hosts":[${entry(url = url)}]}""", "url must be http(s)://host[:port]")
        }
    }

    @Test
    fun `a malformed file is refused`() {
        refused("""{"hosts":[${entry(id = id.uppercase())}]}""", "canonical lowercase UUID")
        refused("""{"hosts":[${entry(id = "1-1-1-1-1")}]}""", "canonical lowercase UUID")
        refused("""{"hosts":[${entry(extra = ""","token":"secret"""")}]}""", "unknown fields")
        refused("""{"hosts":[${entry(extra = ""","timeout_ms":0""")}]}""", "timeout_ms must be positive")
        refused("""{"hosts":[${entry(extra = ""","max_concurrent":-1""")}]}""", "max_concurrent must be positive")
        refused(
            """{"hosts":[${entry(extra = ""","max_response_bytes":"1"""")}]}""",
            "max_response_bytes must be positive",
        )
        refused("""{"hosts":[${entry(extra = ""","timeout_ms":600001""")}]}""", "timeout_ms must be at most")
        refused("""{"hosts":[${entry(extra = ""","max_concurrent":257""")}]}""", "max_concurrent must be at most")
        refused("""{"hosts":[],"other":1}""", "only a hosts array")
        refused("""{"hosts":{}}""", "hosts must be an array")
        refused("""{"hosts":["x"]}""", "entry must be an object")
        refused("""{"hosts":[{"id":"$id"}]}""", "requires a match object")
    }

    @Test
    fun `an exchange entry is its token endpoint, its client id and a secret read from the named variable`() {
        val cfg = config("""{"hosts":[${exchanging()}]}""")
        val host = cfg.mountHosts.single()
        assertThat(host.auth).isEqualTo(MountHost.Auth.EXCHANGE)
        assertThat(host.exchange).isEqualTo(
            MountExchange(
                tokenUrl = URI("http://hogtower.internal:8080/oauth/token"),
                clientId = clientId,
                clientSecret = ClientSecret(secret),
                timeoutMs = 5_000,
            ),
        )
        // Nothing that prints the configuration prints the secret.
        assertThat("$cfg $host ${host.exchange} ${cfg.mountHosts}").doesNotContain(secret).contains("[redacted]")
        val tuned =
            exchanging(
                """{"token_url":"https://hogtower.internal/oauth/token","client_id":"$clientId",""" +
                    """"client_secret_env":"HOGLAKE_MOUNT_EXCHANGE_OTHER","timeout_ms":1500}""",
            )
        val otherSecret = "another-client-secret-of-enough-bytes-000"
        val read = config("""{"hosts":[$tuned]}""", secrets = mapOf("HOGLAKE_MOUNT_EXCHANGE_OTHER" to otherSecret))
        assertThat(read.mountHosts.single().exchange!!.timeoutMs).isEqualTo(1500)
        assertThat(read.mountHosts.single().exchange!!.clientSecret).isEqualTo(ClientSecret(otherSecret))
        // A forwarding entry beside it is as it was.
        val forward = entry(id = otherId, match = """{"type":"catalog","name":"$other"}""")
        val both =
            config(
                """{"hosts":[${exchanging().replace(
                    """{"type":"events"}""",
                    """{"type":"catalog","name":"$mount"}""",
                )},$forward]}""",
            )
        assertThat(both.mountHosts.map { it.auth }).containsExactly(MountHost.Auth.EXCHANGE, MountHost.Auth.FORWARD)
        assertThat(both.mountHosts.last().exchange).isNull()
        // The process environment is where a secret is read by default, and it holds none of these names.
        assertThatThrownBy {
            Config(mountHostsFile = cfg.mountHostsFile, authMode = "enforce", authPredicateFields = "team_id")
        }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("$secretVariable is not set")
    }

    @Test
    fun `exchange settings that are not the contract's are refused, and no refusal shows the secret`() {
        fun refusedExchange(
            exchange: String?,
            message: String,
            auth: String = "exchange",
            secrets: Map<String, String> = mapOf(secretVariable to secret),
        ) {
            val hosts = """{"hosts":[${exchanging(exchange, auth)}]}"""
            assertThatThrownBy { config(hosts, secrets = secrets) }
                .describedAs(hosts)
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining(message)
                .satisfies({ failure ->
                    secrets.values.filter { it.isNotEmpty() }.forEach {
                        assertThat(
                            failure.toString(),
                        ).doesNotContain(it)
                    }
                })
        }

        fun settings(
            url: String = "http://hogtower.internal:8080/oauth/token",
            id: String = clientId,
            variable: String = secretVariable,
            extra: String = "",
        ) = """{"token_url":"$url","client_id":"$id","client_secret_env":"$variable"$extra}"""
        // The object goes with the mode, and only with it.
        refusedExchange(null, "takes an exchange object exactly when its auth is 'exchange'")
        refusedExchange(settings(), "takes an exchange object exactly when its auth is 'exchange'", auth = "forward")
        refusedExchange("\"x\"", "exchange must be an object")
        refusedExchange(
            """{"token_url":"http://h/oauth/token","client_id":"$clientId"}""",
            "requires a string client_secret_env",
        )
        refusedExchange(settings(extra = ""","client_secret":"$secret""""), "unknown fields")
        refusedExchange(settings(extra = ""","audience":"x""""), "unknown fields")
        for (url in listOf(
            "http://hogtower.internal:8080",
            "http://hogtower.internal:8080/",
            "http://user:pw@hogtower.internal/oauth/token",
            "http://hogtower.internal/oauth/token?x=1",
            "http://hogtower.internal/oauth/token#f",
            "http://hogtower.internal/oauth/../admin",
            "ftp://hogtower.internal/oauth/token",
            "/oauth/token",
            "",
        )) {
            refusedExchange(settings(url = url), "token_url must be http(s)://host[:port]/path")
        }
        for (id in listOf(
            "",
            "trino-pool",
            "svc_0123456789abcdef01234567",
            "hlc_0123456789abcdef01234567",
            "hls_XYZ",
        )) {
            refusedExchange(settings(id = id), "client_id is hls_ followed by 24 lowercase hex")
        }
        // The file may name only this feature's own variables: never another secret of the process.
        for (variable in listOf(
            "HOGLAKE_DB_PASSWORD",
            "AWS_SECRET_ACCESS_KEY",
            "hoglake_mount_exchange_x",
            "HOGLAKE_MOUNT_EXCHANGE_",
            "",
        )) {
            refusedExchange(
                settings(variable = variable),
                "client_secret_env must name a variable matching",
                secrets = mapOf(variable to secret, secretVariable to secret),
            )
        }
        refusedExchange(settings(), "$secretVariable is not set", secrets = emptyMap())
        refusedExchange(settings(), "$secretVariable is not set", secrets = mapOf(secretVariable to ""))
        for (bad in listOf("too-short-a-secret-0123456789ab", "x".repeat(257))) {
            refusedExchange(
                settings(),
                "$secretVariable must be 32 to 256 bytes",
                secrets = mapOf(secretVariable to bad),
            )
        }
        for (timeout in listOf("0", "-1", "30001", "\"5\"", "1.5")) {
            refusedExchange(settings(extra = ""","timeout_ms":$timeout"""), "exchange timeout_ms must be 1..30000")
        }
    }

    @Test
    fun `a mount client must verify scoped tokens and cannot also be a mount host`() {
        val hosts = """{"hosts":[${entry()}]}"""
        assertThatThrownBy { config(hosts, allowlist = setOf("*")) }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_MOUNT_ALLOWLIST")
        assertThatThrownBy { config(hosts, authMode = "off") }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("HOGLAKE_AUTH_MODE")
        for (fields in listOf("", " , ")) {
            assertThatThrownBy { config(hosts, predicateFields = fields) }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("HOGLAKE_AUTH_PREDICATE_FIELDS")
        }
        assertThat(config(hosts, authMode = "warn").mountHosts).hasSize(1)
    }
}
