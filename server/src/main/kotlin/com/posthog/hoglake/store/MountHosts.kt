package com.posthog.hoglake.store

import com.posthog.hoglake.auth.Caller
import com.posthog.hoglake.service.Identifiers
import com.posthog.hoglake.service.ReservedNamespaces
import com.posthog.hoglake.service.TeamNamespaces
import io.github.oshai.kotlinlogging.KotlinLogging
import java.net.URI
import java.util.UUID

/**
 * Which names one mount host serves: catalog names ([Events], [Catalog]) or
 * namespace names in every local catalog ([Namespace], [NamespaceFamily]). A
 * name matches at most one configured host ([MountHost.requireDisjoint],
 * checked at boot).
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

    /**
     * What a match that serves namespaces, not catalogs, answers: the names
     * it keeps off this server's own catalogs ([covers]), the ones it relays
     * ([schemaFor]) and what its traffic is counted against ([label]).
     */
    sealed interface RemoteSchemaMatch : MountMatch {
        /** What a relay or a refusal for this match is counted against: configuration, never request input. */
        val label: String

        /** Whether [namespace] is never a local namespace here: a relayed name, or a variant of one. */
        fun covers(namespace: String): Boolean

        /**
         * The schema a request that named [namespace] asks for: the name the
         * caller must hold. A covered name that is not relayed (a case variant)
         * maps to a name that is not [namespace], so no caller holds it.
         */
        fun schemaFor(namespace: String): String

        /** Whether exactly [namespace] is relayed to the host. */
        fun relays(namespace: String): Boolean = schemaFor(namespace) == namespace
    }

    /**
     * A remote schema: the namespace [name] of every catalog this server
     * holds itself is served by the host. Wire:
     * `{"type": "namespace", "name": "..."}`.
     *
     * It matches no catalog name: the catalog stays local and only requests
     * for this namespace go to the host ([MountHostResolver.remoteSchemas]).
     * [covers] folds ASCII case as the reserved-name check does
     * ([ReservedNamespaces]), so no case variant of the name is a local
     * namespace; only the exact [name] is relayed. [name] is the local
     * name; the host may know the schema by another ([MountHost.remoteName]).
     */
    data class Namespace(
        val name: String,
    ) : RemoteSchemaMatch {
        init {
            require(Regex(Identifiers.PATTERN).matches(name)) {
                "a mount host matches a namespace by a namespace name (${Identifiers.PATTERN}), got '$name'"
            }
        }

        override fun matches(catalog: String): Boolean = false

        override val label: String get() = name

        /** Whether [namespace] is this remote schema's name or a case variant of it. */
        override fun covers(namespace: String): Boolean =
            ReservedNamespaces.fold(namespace) == ReservedNamespaces.fold(name)

        override fun schemaFor(namespace: String): String = name
    }

    /**
     * Every team schema: each `team_<id>` of every catalog this server holds
     * itself is served by the host. Wire:
     * `{"type": "namespace_family", "family": "team"}`.
     *
     * A name is relayed only in its canonical form ([TeamNamespaces.id]); the
     * names kept off this server's catalogs are every spelling of the shape
     * ([TeamNamespaces.hasShape]), so a variant is neither a local namespace
     * nor relayed.
     */
    data class NamespaceFamily(
        val family: String,
    ) : RemoteSchemaMatch {
        init {
            require(family == TeamNamespaces.FAMILY) {
                "the only namespace family is '${TeamNamespaces.FAMILY}', got '$family'"
            }
        }

        override fun matches(catalog: String): Boolean = false

        override val label: String get() = TeamNamespaces.LABEL

        override fun covers(namespace: String): Boolean = TeamNamespaces.hasShape(namespace)

        override fun schemaFor(namespace: String): String = if (TeamNamespaces.id(namespace) != null) namespace else ""
    }
}

/**
 * One remote host a mount Client proxies to (plan row 1.3c), as server
 * configuration states it (HOGLAKE_MOUNT_HOSTS_FILE): which names it serves,
 * what it is, where it is and how the Client authenticates to it. Endpoints
 * only, never credentials.
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
    /**
     * `allow_insecure_http`, for a namespace or namespace family entry only: this
     * remote schema's host may be an `http` origin. Without it such an
     * entry must be `https`: the token relayed for a remote schema can
     * carry storage credentials (the session profile), and `http` sends
     * it without transport encryption.
     */
    val allowInsecureHttp: Boolean = false,
    /**
     * `remote_name`, for a [MountMatch.Namespace] entry only: the schema's
     * name on the host, when it is not the local name (an alias). The
     * relayed path and the token check use it; the reserved name, the
     * hidden local namespace and every answer to the caller use the local
     * name. Null: the same name on both sides.
     */
    val remoteName: String? = null,
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
    }

    init {
        require(origin.scheme in setOf("http", "https") && !origin.host.isNullOrEmpty()) {
            "a mount host must be an http(s) origin"
        }
        require(!allowInsecureHttp || match is MountMatch.RemoteSchemaMatch) {
            "allow_insecure_http is only for a mount host that matches a namespace"
        }
        require(match !is MountMatch.RemoteSchemaMatch || origin.scheme == "https" || allowInsecureHttp) {
            "a mount host that matches a namespace must be an https origin: the token it is sent can carry " +
                "storage credentials. Set \"allow_insecure_http\": true on the entry to send it over http"
        }
        require(match !is MountMatch.Namespace || TeamNamespaces.FAMILY !in listOfNotNull(match.name, remoteName)) {
            "a namespace entry may not be named '${TeamNamespaces.FAMILY}', by name or remote_name: " +
                "the remote_schemas key '${TeamNamespaces.FAMILY}' is the team family entry"
        }
        require(remoteName == null || match is MountMatch.Namespace) {
            "remote_name is only for a mount host that matches a namespace"
        }
        require(remoteName == null || Regex(Identifiers.PATTERN).matches(remoteName)) {
            "a mount host's remote_name must be a namespace name (${Identifiers.PATTERN}), got '$remoteName'"
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
         * No id may name two of [hosts], and no catalog or namespace name
         * may match two: which host answers must never depend on their order.
         * Nor may two namespace entries have one name on their hosts, on one
         * host or two: a `remote_schemas` claim names a schema by that name
         * alone, exactly, so one key would open both.
         */
        fun requireDisjoint(hosts: List<MountHost>) {
            require(hosts.map { it.id }.toSet().size == hosts.size) { "duplicate mount host id" }
            for ((i, a) in hosts.withIndex()) {
                for (b in hosts.drop(i + 1)) {
                    require(!overlap(a.match, b.match)) {
                        val what = if (a.match is MountMatch.RemoteSchemaMatch) "namespace" else "catalog"
                        "two mount hosts match the same $what name (${describe(a.match)} and ${describe(b.match)})"
                    }
                    require(a.remoteSchema == null || a.remoteSchema != b.remoteSchema) {
                        "two mount hosts relay to the same schema name '${a.remoteSchema}' on their hosts " +
                            "(${describe(a.match)} and ${describe(b.match)})"
                    }
                }
            }
            if (hosts.any { it.match is MountMatch.NamespaceFamily }) {
                // A family claim is checked against the name the host knows, so an alias into a team name
                // would be reachable by that claim as well as by its own.
                hosts.firstOrNull { it.remoteSchema?.let(TeamNamespaces::hasShape) == true }?.let {
                    throw IllegalArgumentException(
                        "a namespace entry's host name '${it.remoteSchema}' has the team_<digits> shape, which the " +
                            "namespace_family entry serves",
                    )
                }
            }
        }

        /** A namespace entry's schema name on its host; null for a mount entry. */
        private val MountHost.remoteSchema: String?
            get() = (match as? MountMatch.Namespace)?.let { remoteName ?: it.name }

        private fun overlap(
            a: MountMatch,
            b: MountMatch,
        ): Boolean =
            when (a) {
                is MountMatch.Events -> b is MountMatch.Events || (b is MountMatch.Catalog && a.matches(b.name))
                is MountMatch.Catalog -> b.matches(a.name)
                // A namespace name and a catalog name never meet.
                is MountMatch.RemoteSchemaMatch -> b is MountMatch.RemoteSchemaMatch && overlapping(a, b)
            }

        // A name a family covers may not also be a namespace entry's: one name, two hosts.
        private fun overlapping(
            a: MountMatch.RemoteSchemaMatch,
            b: MountMatch.RemoteSchemaMatch,
        ): Boolean =
            when {
                a is MountMatch.Namespace && b is MountMatch.Namespace -> a.covers(b.name)
                a is MountMatch.Namespace && b is MountMatch.NamespaceFamily -> b.covers(a.name)
                a is MountMatch.NamespaceFamily && b is MountMatch.Namespace -> a.covers(b.name)
                else -> true
            }

        private fun describe(match: MountMatch): String =
            when (match) {
                is MountMatch.Events -> "events"
                is MountMatch.Catalog -> "catalog '${match.name}'"
                is MountMatch.Namespace -> "namespace '${match.name}'"
                is MountMatch.NamespaceFamily -> "namespace family '${match.family}'"
            }
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

    /**
     * The remote schemas this server configures: empty unless a host matches
     * a namespace or a namespace family ([MountMatch.RemoteSchemaMatch]). Static configuration, the same
     * for every caller and catalog; who may read one is the token's to say
     * (auth/StoreEntitlement.kt).
     */
    fun remoteSchemas(): List<RemoteSchemaHost> = emptyList()

    companion object {
        /** No hosts: every name is local, which is a server that is not a mount Client. */
        val NONE = MountHostResolver { _, _ -> null }
    }
}

/** One configured remote schema: its [match] and the reader of the host that serves it. */
class RemoteSchemaHost(
    val match: MountMatch.RemoteSchemaMatch,
    val host: MountHostClient,
) {
    /** The schema's name as configured, or the family's label: its label in metrics, never request input. */
    val name: String get() = match.label

    /**
     * The name on the host of the schema this server calls [local]: what is
     * relayed and what the token's `remote_schemas` names. Only a namespace
     * entry has an alias; a family's schemas have the same name on both sides.
     */
    fun remoteNameFor(local: String): String =
        (match as? MountMatch.Namespace)?.let { host.host.remoteName ?: it.name } ?: local
}

/** The resolver over static server configuration: each configured host, by id and by the names it matches. */
class StaticMountHosts(
    hosts: List<MountHost>,
) : MountHostResolver, AutoCloseable {
    private val log = KotlinLogging.logger {}
    private val readers: List<MountHostClient>
    private val schemas: List<RemoteSchemaHost>

    init {
        MountHost.requireDisjoint(hosts)
        // Once, where the configuration is taken up: an entry that opted in is named by its id.
        hosts.filter { it.allowInsecureHttp && it.origin.scheme == "http" }.forEach {
            log.warn {
                "mount host ${it.id} is a remote schema's host over http (allow_insecure_http): the caller's " +
                    "token, which can carry storage credentials, is sent to it without transport encryption"
            }
        }
        readers = hosts.map(::MountHostClient)
        schemas =
            readers.mapNotNull { reader ->
                (reader.host.match as? MountMatch.RemoteSchemaMatch)?.let { RemoteSchemaHost(it, reader) }
            }
    }

    /** The reader for the configured host [id], or null: an id outside the configuration names nothing. */
    fun host(id: UUID): MountHostClient? = readers.firstOrNull { it.host.id == id }

    override fun resolve(
        caller: Caller?,
        catalog: String,
    ): MountHostClient? = readers.firstOrNull { it.host.match.matches(catalog) }

    override fun remoteSchemas(): List<RemoteSchemaHost> = schemas

    override fun close() = readers.forEach { it.close() }
}
