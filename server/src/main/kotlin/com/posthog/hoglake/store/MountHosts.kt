package com.posthog.hoglake.store

import com.posthog.hoglake.auth.Caller
import java.net.URI
import java.security.MessageDigest
import java.util.UUID

/**
 * Which catalog names one mount host serves. A name matches at most one
 * configured host ([MountHost.requireDisjoint], checked at boot).
 */
sealed interface MountMatch {
    fun matches(catalog: String): Boolean

    /** Every mount name, `<orgID>-events` ([MountNames]). Wire: `{"type": "events"}`. */
    data object Events : MountMatch {
        override fun matches(catalog: String): Boolean = MountNames.isReserved(catalog)
    }

    /**
     * One catalog, by its exact name. Wire: `{"type": "catalog", "name": "..."}`.
     *
     * Only a mount-shaped name is accepted today. Everything that keeps a
     * mount apart from a catalog is keyed on that shape: `POST /v1/catalogs`
     * refuses it, the issuer's `cat` grammar keeps it off hoglite,
     * `AuthPolicy.MOUNT_ROUTES` opens by it, and the listing shows by it. A
     * name outside it could be another tenant's catalog on a token-routed
     * backend, which this binding would then shadow.
     */
    data class Catalog(
        val name: String,
    ) : MountMatch {
        init {
            require(MountNames.isReserved(name)) {
                "a mount host can match a catalog by name only when the name is a mount's (<orgID>-events), got '$name'"
            }
        }

        override fun matches(catalog: String): Boolean = catalog == name
    }
}

/**
 * One remote host a mount Client proxies to (plan row 1.3c), as server
 * configuration states it (HOGLAKE_MOUNT_HOSTS_FILE): which names it serves,
 * what it is, where it is and how the Client authenticates to it. Endpoints
 * only, never credentials: an exchange host's client secret is named here
 * by the environment variable that holds it ([MountExchange]).
 */
data class MountHost(
    /**
     * This host's stable id: a canonical lowercase UUID, unique among the
     * configured hosts, as a hoglite backend's is. It is what names the
     * host in logs (never its URL), and what a verified routing claim
     * could name later, as `rtg.be` names a backend today.
     */
    val id: UUID,
    val match: MountMatch,
    val kind: Kind,
    /** Scheme, host and optional port; the Client builds every path itself. */
    val origin: URI,
    val auth: Auth,
    /** The deadline for one proxied request, send to the last byte of the answer. */
    val timeoutMs: Long = DEFAULT_TIMEOUT_MS,
    /** The most bytes of one answer the Client forwards; a body is streamed, never held whole. */
    val maxResponseBytes: Long = DEFAULT_MAX_RESPONSE_BYTES,
    /** The cap over this host's proxied requests in flight. */
    val maxConcurrent: Int = DEFAULT_MAX_CONCURRENT,
    /** Where and as whom this server exchanges tokens for the host; set exactly when [auth] is [Auth.EXCHANGE]. */
    val exchange: MountExchange? = null,
) {
    /** What the host speaks. Each kind needs its own reader; only a hoglake's exists. */
    enum class Kind(val wire: String) {
        /** Another hoglake serving mounts (exports): the same routes and bodies as this server's. */
        HOGLAKE("hoglake"),
    }

    /** How the Client authenticates to the host. */
    enum class Auth(val wire: String) {
        /**
         * The caller's own bearer token, unchanged. Sound only for a host
         * that verifies tokens of the SAME issuer and audience as this
         * server: the Client has verified the token, the host verifies it
         * again and scopes by its claims, and the Client holds no
         * credential of its own. A host under another authority would
         * refuse the token, or worse, be handed a credential it has no
         * business seeing; it needs an auth mode of its own.
         */
        FORWARD("forward"),

        /**
         * A token this server gets for the caller from the issuer's token
         * endpoint (hogtower#181), in exchange for the caller's own, and
         * presents in its place ([MountExchange], [MountTokenExchange]).
         * For a reader whose token names its org catalog, which the host
         * would not accept: the reader keeps one token, and the mount
         * token never leaves this server. Who may be served this way is
         * [MountEntitlement]'s to say.
         */
        EXCHANGE("exchange"),
    }

    init {
        require(origin.scheme in setOf("http", "https") && !origin.host.isNullOrEmpty()) {
            "a mount host must be an http(s) origin"
        }
        require((auth == Auth.EXCHANGE) == (exchange != null)) {
            "a mount host has exchange settings exactly when its auth is '${Auth.EXCHANGE.wire}'"
        }
        require(timeoutMs >= 1) { "timeout_ms must be >= 1, got $timeoutMs" }
        require(maxResponseBytes >= 1) { "max_response_bytes must be >= 1, got $maxResponseBytes" }
        require(maxConcurrent >= 1) { "max_concurrent must be >= 1, got $maxConcurrent" }
    }

    companion object {
        /** Above a Host's default 60 s scan deadline, so its typed `mount_scan_timeout` arrives first. */
        const val DEFAULT_TIMEOUT_MS = 75_000L
        const val DEFAULT_MAX_RESPONSE_BYTES = 1L shl 30
        const val DEFAULT_MAX_CONCURRENT = 16

        /** A request holds a permit, a socket and at times a thread for this long at most: kept to minutes. */
        const val TIMEOUT_CEILING_MS = 600_000L

        /** Each permit can hold one blocked IO thread; this keeps a host's share of them modest. */
        const val MAX_CONCURRENT_CEILING = 256

        /**
         * A base URL as a bare origin, or null when [value] is anything
         * else. Scheme, host and optional port only: a path, query,
         * fragment or user-info could only redirect what the Client
         * contacts. Rebuilt from its three parts, so nothing else of
         * [value] survives.
         */
        fun origin(value: String): URI? {
            val uri =
                try {
                    URI(value)
                } catch (_: java.net.URISyntaxException) {
                    return null
                }
            val bare =
                uri.scheme in setOf("http", "https") && !uri.host.isNullOrEmpty() && uri.rawUserInfo == null &&
                    (uri.rawPath.isNullOrEmpty() || uri.rawPath == "/") && uri.rawQuery == null &&
                    uri.rawFragment == null
            return if (bare) URI(uri.scheme, null, uri.host, uri.port, null, null, null) else null
        }

        /**
         * No id may name two of [hosts], and no catalog name may match two:
         * which host answers must never depend on their order.
         */
        fun requireDisjoint(hosts: List<MountHost>) {
            require(hosts.map { it.id }.toSet().size == hosts.size) { "duplicate mount host id" }
            for ((i, a) in hosts.withIndex()) {
                for (b in hosts.drop(i + 1)) {
                    require(!overlap(a.match, b.match)) {
                        "two mount hosts match the same catalog name (${describe(a.match)} and ${describe(b.match)})"
                    }
                }
            }
        }

        private fun overlap(
            a: MountMatch,
            b: MountMatch,
        ): Boolean =
            when (a) {
                is MountMatch.Events -> b is MountMatch.Events || (b is MountMatch.Catalog && a.matches(b.name))
                is MountMatch.Catalog -> b.matches(a.name)
            }

        private fun describe(match: MountMatch): String =
            when (match) {
                is MountMatch.Events -> "events"
                is MountMatch.Catalog -> "catalog '${match.name}'"
            }
    }
}

/**
 * How this server exchanges a caller's token for one a host accepts
 * ([MountHost.Auth.EXCHANGE]): the issuer's token endpoint, and the client
 * this server authenticates as. The wire contract is [MountTokenExchange]'s.
 */
data class MountExchange(
    /** The token endpoint, whole: scheme, host, optional port and path. The only address an exchange contacts. */
    val tokenUrl: URI,
    val clientId: String,
    val clientSecret: ClientSecret,
    /** The deadline for one exchange, send to the last byte of the answer. */
    val timeoutMs: Long = DEFAULT_TIMEOUT_MS,
) {
    init {
        require(tokenUrl(tokenUrl.toString()) == tokenUrl) {
            "a token endpoint must be an http(s) URL with a path and no user-info, query or fragment"
        }
        require(MountTokenExchange.isClientId(clientId)) { MountTokenExchange.CLIENT_ID_RULE }
        require(timeoutMs in 1..TIMEOUT_CEILING_MS) { "exchange timeout_ms must be 1..$TIMEOUT_CEILING_MS" }
    }

    companion object {
        const val DEFAULT_TIMEOUT_MS = 5_000L

        /** A reader waits on the exchange before its request is even sent: kept to seconds. */
        const val TIMEOUT_CEILING_MS = 30_000L

        /**
         * A token endpoint URL, or null when [value] is anything else: an
         * http(s) URL with a host and a plain path (unreserved characters,
         * no dot segments), and no user-info, query or fragment
         * (parameters, the secret above all, go in the body). Rebuilt from
         * its parts, so nothing else of [value] survives.
         */
        fun tokenUrl(value: String): URI? {
            val uri =
                try {
                    URI(value)
                } catch (_: java.net.URISyntaxException) {
                    return null
                }
            val path = uri.rawPath.orEmpty()
            val plain =
                uri.scheme in setOf("http", "https") && !uri.host.isNullOrEmpty() && uri.rawUserInfo == null &&
                    PATH.matches(path) && path.split('/').none { it == "." || it == ".." } &&
                    uri.rawQuery == null && uri.rawFragment == null
            return if (plain) URI(uri.scheme, null, uri.host, uri.port, path, null, null) else null
        }

        private val PATH = Regex("^(/[A-Za-z0-9._~-]+)+$")
    }
}

/**
 * A client secret held in memory. It is not a data class and prints as
 * `[redacted]`, so no log line, error message or `toString()` of a
 * configuration object can carry it; its one reader is the exchange, which
 * builds the request's credentials from it ([reveal]).
 */
class ClientSecret(
    private val value: String,
) {
    init {
        require(value.toByteArray(Charsets.UTF_8).size in MountTokenExchange.SECRET_BYTES) {
            "a mount exchange client secret must be ${MountTokenExchange.SECRET_BYTES.first} to " +
                "${MountTokenExchange.SECRET_BYTES.last} bytes"
        }
    }

    /** The secret itself, for the request's credentials and nothing else. */
    internal fun reveal(): String = value

    override fun toString(): String = "[redacted]"

    override fun equals(other: Any?): Boolean =
        other is ClientSecret && MessageDigest.isEqual(other.value.toByteArray(), value.toByteArray())

    override fun hashCode(): Int = value.hashCode()
}

/**
 * Where a secret named in configuration is read from: the process
 * environment, or a fixed map in a test. Prints nothing of what it holds.
 */
class SecretSource(
    private val lookup: (name: String) -> String?,
) {
    operator fun get(name: String): String? = lookup(name)

    override fun toString(): String = "SecretSource"

    companion object {
        val ENVIRONMENT = SecretSource(System::getenv)

        /** The names a hosts file may point at: this feature's own variables, and no other secret of the process. */
        val NAME = Regex("^HOGLAKE_MOUNT_EXCHANGE_[A-Z0-9_]{1,64}$")
    }
}

/**
 * Where a catalog name is mounted from: the one seam between "this name is
 * remote" and the proxy. `resolve` answers the reader for the host that
 * serves [catalog] to [caller], or null when no host does and the name is
 * this server's own business. The proxy knows only what it is handed here:
 * a host's kind, origin and auth mode, never a process-wide address.
 *
 * [caller] is the request's verified caller (null when there is none). The
 * static resolver does not read it; it is in the signature so that a later
 * one can take the host from a verified claim, by id ([StaticMountHosts.host]),
 * before falling back to the configured matchers, with no call site changed.
 */
fun interface MountHostResolver {
    fun resolve(
        caller: Caller?,
        catalog: String,
    ): MountHostClient?

    companion object {
        /** No hosts: every name is local, which is a server that is not a mount Client. */
        val NONE = MountHostResolver { _, _ -> null }
    }
}

/** The resolver over static server configuration: each configured host, by id and by the names it matches. */
class StaticMountHosts(
    hosts: List<MountHost>,
) : MountHostResolver, AutoCloseable {
    private val readers: List<MountHostClient>

    init {
        MountHost.requireDisjoint(hosts)
        readers = hosts.map(::MountHostClient)
    }

    /** The reader for the configured host [id], or null: an id outside the configuration names nothing. */
    fun host(id: UUID): MountHostClient? = readers.firstOrNull { it.host.id == id }

    override fun resolve(
        caller: Caller?,
        catalog: String,
    ): MountHostClient? = readers.firstOrNull { it.host.match.matches(catalog) }

    override fun close() = readers.forEach { it.close() }
}
