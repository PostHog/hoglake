package com.posthog.hoglake

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.posthog.hoglake.store.MountHost
import com.posthog.hoglake.store.MountMatch
import com.posthog.hoglake.store.StaticMountHosts
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.slf4j.LoggerFactory
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

    private fun entry(
        id: String = this.id,
        match: String = """{"type":"namespace_family","family":"team"}""",
        kind: String = "hoglake",
        url: String = "https://gigahog-server.gigahog:8443",
        auth: String = "forward",
        extra: String = "",
    ) = """{"id":"$id","match":$match,"kind":"$kind","url":"$url","auth":"$auth"$extra}"""

    private fun config(
        hosts: String,
        authMode: String = "enforce",
        predicateFields: String = "team_id",
        /** `HOGLAKE_AUTH_SESSION_PROFILE`; the server's default is off, and every entry needs it on. */
        sessionProfile: String = "on",
    ): Config {
        val file = Files.writeString(dir.resolve("hosts-${UUID.randomUUID()}.json"), hosts)
        return Config(
            mountHostsFile = file.toString(),
            authMode = authMode,
            authPredicateFields = predicateFields,
            authSessionProfile = sessionProfile,
        )
    }

    /** A Client with remote schemas: the session profile is on, as a namespace entry requires. */
    private fun schemaConfig(hosts: String) = config(hosts, sessionProfile = "on")

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
        // Nothing a Client needs is demanded of it: auth off, no predicate fields.
        val empty = config("""{"hosts":[]}""", authMode = "off", predicateFields = "")
        assertThat(empty.mountHosts).isEmpty()
    }

    @Test
    fun `an entry is its id, matcher, kind, origin and auth, with bounded defaults`() {
        val host = config("""{"hosts":[${entry()}]}""").mountHosts.single()
        assertThat(host).isEqualTo(
            MountHost(
                id = UUID.fromString(id),
                match = MountMatch.NamespaceFamily("team"),
                kind = MountHost.Kind.HOGLAKE,
                origin = URI("https://gigahog-server.gigahog:8443"),
                auth = MountHost.Auth.FORWARD,
                timeoutMs = 75_000,
                maxResponseBytes = 1L shl 30,
                maxConcurrent = 16,
            ),
        )
        val tuned =
            config(
                """{"hosts":[${entry(
                    match = """{"type":"namespace","name":"posthog"}""",
                    url = "https://host.example/",
                    extra = ""","timeout_ms":1000,"max_response_bytes":2048,"max_concurrent":3""",
                )}]}""",
            ).mountHosts.single()
        assertThat(tuned.match).isEqualTo(MountMatch.Namespace("posthog"))
        assertThat(tuned.origin).isEqualTo(URI("https://host.example"))
        assertThat(listOf(tuned.timeoutMs, tuned.maxResponseBytes, tuned.maxConcurrent.toLong()))
            .containsExactly(1000L, 2048L, 3L)
    }

    @Test
    fun `entries that match the same name, or share an id, are refused`() {
        refused("""{"hosts":[${entry()},${entry(id = otherId)}]}""", "match the same namespace name")
        refused("""{"hosts":[${schema("posthog")},${schema("billing")}]}""", "duplicate mount host id")
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
            "match type must be namespace or namespace_family",
        )
        refused("""{"hosts":[${entry(match = "\"team\"")}]}""", "requires a match object")
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
    fun `a mount client must verify scoped tokens`() {
        val hosts = """{"hosts":[${entry()}]}"""
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

    // ---- remote schemas: {"type":"namespace","name":...} ---------------------

    private fun schema(
        name: String,
        id: String = this.id,
        url: String = "https://host-hoglake.example",
        extra: String = "",
    ) = entry(id = id, match = """{"type":"namespace","name":"$name"}""", url = url, extra = extra)

    private fun family(
        id: String = this.id,
        family: String = "team",
        url: String = "https://host-hoglake.example",
        extra: String = "",
    ) = entry(id = id, match = """{"type":"namespace_family","family":"$family"}""", url = url, extra = extra)

    /** The warnings logged when [cfg]'s hosts are taken up, as the server does once at boot. */
    private fun bootWarnings(cfg: Config): List<String> {
        val capture = ListAppender<ILoggingEvent>().apply { start() }
        val logger = LoggerFactory.getLogger(StaticMountHosts::class.java) as Logger
        logger.addAppender(capture)
        try {
            StaticMountHosts(cfg.mountHosts).close()
        } finally {
            logger.detachAppender(capture)
        }
        return capture.list.filter { it.level == Level.WARN }.map { it.formattedMessage }
    }

    @Test
    fun `a namespace entry over http is refused unless the entry opts in, and then says so once`() {
        val http = "http://host-hoglake.example:8080"
        val opted = ""","allow_insecure_http":true"""
        // Refused, with the reason and the way out.
        for (extra in listOf("", ""","allow_insecure_http":false""")) {
            assertThatThrownBy { schemaConfig("""{"hosts":[${schema("posthog", url = http, extra = extra)}]}""") }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("must be an https origin")
                .hasMessageContaining("storage credentials")
                .hasMessageContaining("allow_insecure_http")
        }
        // Opted in: it boots, and one warning names the host by its id, never by its URL.
        val cfg = schemaConfig("""{"hosts":[${entry()},${schema("posthog", otherId, http, opted)}]}""")
        assertThat(cfg.mountHosts.map { it.allowInsecureHttp }).containsExactly(false, true)
        assertThat(bootWarnings(cfg)).singleElement().asString()
            .contains(otherId, "http", "without transport encryption")
            .doesNotContain("host-hoglake.example")
        // https is as it was, with the key or without it, and warns of nothing.
        for (extra in listOf("", opted)) {
            val secure = schemaConfig("""{"hosts":[${schema("posthog", extra = extra)}]}""")
            assertThat(secure.mountHosts.single().origin.scheme).isEqualTo("https")
            assertThat(bootWarnings(secure)).isEmpty()
        }
        refused(
            """{"hosts":[${schema("posthog", url = http, extra = ""","allow_insecure_http":"yes"""")}]}""",
            "must be true or false",
        )
    }

    @Test
    fun `an events or a catalog entry refuses to boot, and a file of a namespace_family entry alone loads`() {
        val removed = "the events and catalog match types were removed with the mount path; delete the entry"
        for (match in listOf("""{"type":"events"}""", """{"type":"catalog","name":"$mount"}""")) {
            refused("""{"hosts":[${entry(match = match)}]}""", removed)
            // Beside a valid entry too, in either order: no file boots half-understood.
            refused("""{"hosts":[${schema("posthog", otherId)},${entry(match = match)}]}""", removed)
            refused("""{"hosts":[${entry(match = match)},${schema("posthog", otherId)}]}""", removed)
        }
        assertThat(config("""{"hosts":[${family()}]}""").mountHosts.map { it.match })
            .containsExactly(MountMatch.NamespaceFamily("team"))
        // Every entry relays a token now, so http is refused on each type unless the entry opts in.
        for (http in listOf(schema("posthog", url = "http://h:1"), family(url = "http://h:1"))) {
            refused("""{"hosts":[$http]}""", "must be an https origin")
        }
    }

    @Test
    fun `a namespace entry is a remote schema that matches no catalog name and reserves its own`() {
        val cfg = schemaConfig("""{"hosts":[${schema("posthog")}]}""")
        val host = cfg.mountHosts.single()
        assertThat(host.match).isEqualTo(MountMatch.Namespace("posthog"))
        val match = host.match as MountMatch.Namespace
        assertThat(match.covers("posthog")).isTrue()
        assertThat(match.covers("PostHog")).isTrue()
        assertThat(match.covers("posthog_raw")).isFalse()
        assertThat(match.covers("postho")).isFalse()
        // The name is reserved without being listed, compared as a reserved name is.
        assertThat(cfg.reservedNamespaces.names).containsExactly("posthog")
        assertThat(cfg.reservedNamespaces.isReserved("POSTHOG")).isTrue()
        assertThat(cfg.reservedNamespaces.isReserved("imports")).isFalse()
    }

    @Test
    fun `remote schema names join the configured reserved names`() {
        val file =
            Files.writeString(
                dir.resolve("hosts-reserved.json"),
                """{"hosts":[${schema("posthog")},${schema("Imports", otherId)}]}""",
            )
        val cfg =
            Config(
                mountHostsFile = file.toString(),
                authMode = "enforce",
                authPredicateFields = "team_id",
                authSessionProfile = "on",
                reservedNamespaceNames = setOf("billing", "posthog"),
            )
        assertThat(cfg.reservedNamespaces.names).containsExactly("billing", "imports", "posthog")
        assertThat(
            Config(reservedNamespaceNames = setOf("posthog")).reservedNamespaces.names,
        ).containsExactly("posthog")
    }

    @Test
    fun `a namespace entry with a bad name, a duplicate name or another field is refused`() {
        for (name in listOf("", "post/hog", "9lives", "post hog", "a".repeat(129), "é")) {
            refused("""{"hosts":[${schema(name)}]}""", "matches a namespace by a namespace name")
        }
        // Two entries for one name, and for two spellings the reserved-name fold makes one.
        for (second in listOf("posthog", "PostHog")) {
            refused(
                """{"hosts":[${schema("posthog")},${schema(second, otherId)}]}""",
                "match the same namespace name",
            )
        }
        refused(
            """{"hosts":[${entry(match = """{"type":"namespace"}""")}]}""",
            "a namespace match takes exactly a name",
        )
        refused(
            """{"hosts":[${entry(match = """{"type":"namespace","name":"posthog","host_name":"ingest"}""")}]}""",
            "a namespace match takes exactly a name",
        )
        refused("""{"hosts":[${entry(match = """{"type":"namespace","name":7}""")}]}""", "requires a string name")
    }

    @Test
    fun `a namespace entry is held to the mount client's boot checks`() {
        val hosts = """{"hosts":[${schema("posthog")}]}"""
        val on = "on"
        assertThatThrownBy { config(hosts, authMode = "off", sessionProfile = on) }
            .hasMessageContaining("HOGLAKE_AUTH_MODE")
        assertThatThrownBy { config(hosts, predicateFields = "", sessionProfile = on) }
            .hasMessageContaining("HOGLAKE_AUTH_PREDICATE_FIELDS")
    }

    @Test
    fun `a namespace entry refuses to boot while the session profile is off`() {
        // Off, no token can carry `remote_schemas`: the name would be reserved and hidden, and never readable.
        for (hosts in listOf(
            """{"hosts":[${schema("posthog")}]}""",
            """{"hosts":[${entry()},${schema("posthog", otherId)}]}""",
        )) {
            assertThatThrownBy { config(hosts, sessionProfile = "off") }
                .isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("HOGLAKE_AUTH_SESSION_PROFILE=on")
            assertThat(schemaConfig(hosts).mountHosts.map { it.match }).contains(MountMatch.Namespace("posthog"))
        }
    }

    // ---- an alias: "remote_name" on a namespace entry -------------------------

    @Test
    fun `a namespace entry may name the schema on its host, and only the local name is reserved`() {
        val cfg = schemaConfig("""{"hosts":[${schema("analytics", extra = ""","remote_name":"shared"""")}]}""")
        val host = cfg.mountHosts.single()
        assertThat(host.match).isEqualTo(MountMatch.Namespace("analytics"))
        assertThat(host.remoteName).isEqualTo("shared")
        StaticMountHosts(cfg.mountHosts).use { hosts ->
            val schema = hosts.remoteSchemas().single()
            assertThat(schema.name to schema.remoteNameFor(schema.name)).isEqualTo("analytics" to "shared")
        }
        // Only the local name is reserved; the host's name is no local business.
        assertThat(cfg.reservedNamespaces.names).containsExactly("analytics")
        assertThat(cfg.reservedNamespaces.isReserved("shared")).isFalse()
        // Without the key, or with the local name itself, the two names are one.
        for (extra in listOf("", ""","remote_name":"analytics"""")) {
            val plain = schemaConfig("""{"hosts":[${schema("analytics", extra = extra)}]}""").mountHosts
            StaticMountHosts(plain).use {
                assertThat(
                    it.remoteSchemas().single().let {
                            s ->
                        s.remoteNameFor(s.name)
                    },
                ).isEqualTo("analytics")
            }
        }
    }

    @Test
    fun `remote_name is refused on a family entry, outside the grammar, and for a name on the hosts taken twice`() {
        fun refusedSchema(
            hosts: String,
            message: String,
        ) = assertThatThrownBy { schemaConfig(hosts) }
            .describedAs(hosts)
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining(message)
        val alias = ""","remote_name":"shared""""
        for (name in listOf("", "sha/red", "9lives", "sha red", "a".repeat(129), "é")) {
            refusedSchema(
                """{"hosts":[${schema("analytics", extra = ""","remote_name":"$name"""")}]}""",
                "remote_name must be a namespace name",
            )
        }
        for (value in listOf("7", "null", "true", """["shared"]""")) {
            refusedSchema(
                """{"hosts":[${schema("analytics", extra = ""","remote_name":$value""")}]}""",
                "requires a string remote_name",
            )
        }
        // Not a family entry's key, built directly too.
        assertThatThrownBy {
            MountHost(
                UUID.randomUUID(),
                MountMatch.NamespaceFamily("team"),
                MountHost.Kind.HOGLAKE,
                URI("https://h"),
                MountHost.Auth.FORWARD,
                remoteName = "shared",
            )
        }.hasMessageContaining("remote_name is only for")
        // One entry per name on a host, whatever the hosts, since a claim names a schema by that name alone:
        // an alias of another entry's local name and two aliases of one name, on one host or on two.
        val aliased = schema("analytics", extra = alias)
        val elsewhere = "https://other-hoglake.example"
        val toAnalytics = ""","remote_name":"analytics""""
        for (second in listOf(
            schema("events_v2", otherId, extra = toAnalytics),
            schema("events_v2", otherId, url = elsewhere, extra = toAnalytics),
        )) {
            refusedSchema("""{"hosts":[${schema("analytics")},$second]}""", "relay to the same schema name 'analytics'")
        }
        for (second in listOf(
            schema("events_v2", otherId, extra = alias),
            schema("events_v2", otherId, url = elsewhere, extra = alias),
            schema("shared", otherId),
            schema("shared", otherId, url = elsewhere),
        )) {
            refusedSchema("""{"hosts":[$aliased,$second]}""", "relay to the same schema name 'shared'")
        }
        // Another name on the host is another schema, names compare exactly (as the claim does), and a
        // local name may be another entry's host name (the claim names a host's schema, the reservation a local one).
        for (second in listOf(
            schema("events_v2", otherId, extra = ""","remote_name":"published""""),
            schema("events_v2", otherId, url = elsewhere, extra = ""","remote_name":"Shared""""),
            schema("shared", otherId, extra = ""","remote_name":"published""""),
        )) {
            assertThat(schemaConfig("""{"hosts":[$aliased,$second]}""").mountHosts).hasSize(2)
        }
    }

    @Test
    fun `a namespace_family entry matches no catalog and reserves every team_digits name`() {
        val cfg = schemaConfig("""{"hosts":[${family()}]}""")
        val match = cfg.mountHosts.single().match
        assertThat(match).isEqualTo(MountMatch.NamespaceFamily("team"))
        match as MountMatch.NamespaceFamily
        // Every spelling of the shape is kept off the local catalogs; only the canonical one is relayed.
        for (name in listOf("team_5", "TEAM_5", "Team_5", "team_007", "team_0", "team_99999999999999999999")) {
            assertThat(match.covers(name)).describedAs(name).isTrue()
            assertThat(cfg.reservedNamespaces.isReserved(name)).describedAs(name).isTrue()
        }
        for (name in listOf("team_x", "teams", "team_", "team5", "xteam_5", "team_5x", "team-5", "posthog")) {
            assertThat(match.covers(name)).describedAs(name).isFalse()
            assertThat(cfg.reservedNamespaces.isReserved(name)).describedAs(name).isFalse()
        }
        assertThat(match.relays("team_5")).isTrue()
        for (name in listOf("TEAM_5", "team_007", "team_99999999999999999999", "team_x")) {
            assertThat(match.relays(name)).describedAs(name).isFalse()
        }
        assertThat(cfg.reservedNamespaces.names).isEmpty()
        // The reservation refuses a create the way a reserved name does.
        assertThatThrownBy { cfg.reservedNamespaces.refuse("team_5") }.hasMessageContaining("is reserved")
        cfg.reservedNamespaces.refuse("team_x")
        // Beside the names a deployment reserves, and with no family entry nothing of the shape is reserved.
        assertThat(config("""{"hosts":[${schema("posthog")}]}""").reservedNamespaces.isReserved("team_5")).isFalse()
        assertThat(Config().reservedNamespaces.isReserved("team_5")).isFalse()
    }

    @Test
    fun `a namespace_family entry is a remote schema host and is held to the same boot checks`() {
        val http = "http://host-hoglake.example:8080"
        val opted = ""","allow_insecure_http":true"""
        assertThatThrownBy { schemaConfig("""{"hosts":[${family(url = http)}]}""") }
            .hasMessageContaining("must be an https origin")
        assertThat(
            schemaConfig("""{"hosts":[${family(url = http, extra = opted)}]}""").mountHosts.single().allowInsecureHttp,
        )
            .isTrue()
        assertThatThrownBy {
            config(
                """{"hosts":[${family()}]}""",
                sessionProfile = "off",
            )
        }.hasMessageContaining("HOGLAKE_AUTH_SESSION_PROFILE=on")
        assertThatThrownBy { config("""{"hosts":[${family()}]}""", authMode = "off", sessionProfile = "on") }
            .hasMessageContaining("HOGLAKE_AUTH_MODE")
        assertThatThrownBy { config("""{"hosts":[${family()}]}""", predicateFields = "", sessionProfile = "on") }
            .hasMessageContaining("HOGLAKE_AUTH_PREDICATE_FIELDS")
    }

    @Test
    fun `only the team family exists, and the entry takes exactly a family`() {
        for (name in listOf("org", "Team", "teams", "")) {
            refused("""{"hosts":[${family(family = name)}]}""", "the only namespace family is 'team'")
        }
        refused("""{"hosts":[${entry(match = """{"type":"namespace_family"}""")}]}""", "takes exactly a family")
        refused(
            """{"hosts":[${entry(match = """{"type":"namespace_family","family":"team","name":"x"}""")}]}""",
            "takes exactly a family",
        )
        refused(
            """{"hosts":[${entry(match = """{"type":"namespace_family","family":7}""")}]}""",
            "requires a string family",
        )
    }

    @Test
    fun `a namespace_family entry has no alias, so remote_name is refused on it`() {
        refused(
            """{"hosts":[${family(extra = ""","remote_name":"shared"""")}]}""",
            "remote_name is only for a mount host that matches a namespace",
        )
    }

    @Test
    fun `a family entry and a namespace entry of a team name are an overlap, in either order`() {
        for (name in listOf("team_5", "TEAM_5", "team_007", "Team_0")) {
            val named = schema(name, otherId)
            for (hosts in listOf(listOf(family(), named), listOf(named, family()))) {
                refused("""{"hosts":[${hosts.joinToString(",")}]}""", "two mount hosts match the same namespace name")
            }
        }
        // A name the family does not cover is not an overlap.
        val cfg =
            schemaConfig(
                """{"hosts":[${family()},${schema(
                    "team_x",
                    otherId,
                )},${schema("teams", "5c0c6d0e-1f2a-4b3c-9d4e-5f6a7b8c9d0e")}]}""",
            )
        assertThat(cfg.mountHosts).hasSize(3)
        // The family is one entry: a second is the same names matched twice.
        refused(
            """{"hosts":[${family()},${family(otherId)}]}""",
            "two mount hosts match the same namespace name",
        )
    }

    @Test
    fun `an alias into a team name is refused beside a family entry`() {
        val alias =
            entry(
                id = otherId,
                match = """{"type":"namespace","name":"legacy"}""",
                url = "https://other-hoglake.example",
                extra = ""","remote_name":"team_7"""",
            )
        refused("""{"hosts":[${family()},$alias]}""", "has the team_<digits> shape")
        refused("""{"hosts":[$alias,${family()}]}""", "has the team_<digits> shape")
        // Without a family entry it is an ordinary alias, and a non-team alias is fine beside one.
        assertThat(schemaConfig("""{"hosts":[$alias]}""").mountHosts).hasSize(1)
        val plain = alias.replace("team_7", "legacy_remote")
        assertThat(schemaConfig("""{"hosts":[${family()},$plain]}""").mountHosts).hasSize(2)
    }

    @Test
    fun `a namespace_family entry needs team_id among the auth predicate fields`() {
        assertThatThrownBy { config("""{"hosts":[${family()}]}""", predicateFields = "org_id", sessionProfile = "on") }
            .hasMessageContaining("'team_id' in HOGLAKE_AUTH_PREDICATE_FIELDS")
        assertThat(
            config(
                """{"hosts":[${family()}]}""",
                predicateFields = "org_id, team_id",
                sessionProfile = "on",
            ).mountHosts,
        ).hasSize(1)
        // A namespace entry asks nothing of the field.
        assertThat(
            config(
                """{"hosts":[${schema("posthog")}]}""",
                predicateFields = "org_id",
                sessionProfile = "on",
            ).mountHosts,
        )
            .hasSize(1)
    }

    @Test
    fun `a namespace entry named team, by name or remote_name, is refused`() {
        refused("""{"hosts":[${schema("team")}]}""", "may not be named 'team'")
        val aliased =
            entry(
                match = """{"type":"namespace","name":"legacy"}""",
                url = "https://h.example",
                extra = ""","remote_name":"team"""",
            )
        refused("""{"hosts":[$aliased]}""", "may not be named 'team'")
        // Only the exact key is the family entry.
        assertThat(schemaConfig("""{"hosts":[${schema("teams")}]}""").mountHosts).hasSize(1)
    }
}
