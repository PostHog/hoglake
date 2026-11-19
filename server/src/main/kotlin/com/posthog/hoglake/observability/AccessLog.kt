package com.posthog.hoglake.observability

import com.posthog.hoglake.api.ApiErrorDto
import com.posthog.hoglake.api.PROBE_PATHS
import com.posthog.hoglake.api.normalisedPath
import com.posthog.hoglake.api.queueWaitMs
import com.posthog.hoglake.auth.AuthPolicy
import com.posthog.hoglake.auth.AuthServiceKey
import com.posthog.hoglake.auth.AuthState
import com.posthog.hoglake.auth.Caller
import com.posthog.hoglake.auth.PredicateGrant
import com.posthog.hoglake.auth.RouteMatchKey
import com.posthog.hoglake.auth.clientClass
import com.posthog.hoglake.auth.hoglakeAuth
import com.posthog.hoglake.service.TeamNamespaces
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.createApplicationPlugin
import io.ktor.server.application.hooks.CallSetup
import io.ktor.server.application.hooks.ResponseSent
import io.ktor.server.request.httpMethod
import io.ktor.server.request.path
import io.ktor.util.AttributeKey
import net.logstash.logback.argument.StructuredArgument
import net.logstash.logback.argument.StructuredArguments.kv
import org.slf4j.LoggerFactory

/** The access log's logger; logback.xml routes it to stdout as JSON, additivity off. */
const val ACCESS_LOGGER_NAME = "hoglake.access"

private val accessLog = LoggerFactory.getLogger(ACCESS_LOGGER_NAME)

/** Application logger for emission failures (never the access stream itself). */
private val appLog = LoggerFactory.getLogger("com.posthog.hoglake.observability.AccessLog")

private val StartKey = AttributeKey<Long>("HoglakeAccessStart")
private val OutcomeKey = AttributeKey<String>("HoglakeAccessOutcome")

/**
 * The access log: one JSON line per request on [ACCESS_LOGGER_NAME], naming
 * the caller (org, team, token `jti`, `usr`), the route's pattern, how long the
 * request queued for a thread and took, and how it ended.
 *
 * It exists to measure per-tenant load. It is a log and not metric labels on
 * purpose: an org, team or token label is unbounded cardinality, which is why
 * the metrics stop at pod, catalog and family. Read in Loki with `| json`.
 *
 * Installed ahead of auth (App.module), and every hook here is independent of
 * the Plugins-phase order: the line is written on the send pipeline
 * ([ResponseSent]), so a refusal the auth plugin answers from inside its own
 * interceptor, a request shed by admission, and a relay error StatusPages maps
 * all get one. The probes ([PROBE_PATHS]) get none.
 *
 * Never logged: the raw path (the rule's pattern stands in for it), the query
 * string, any header (the token above all) or body.
 */
val AccessLog =
    createApplicationPlugin("HoglakeAccessLog") {
        on(CallSetup) { call -> call.attributes.put(StartKey, System.nanoTime()) }
        // The typed error code, read before ContentNegotiation (installed after
        // this plugin, at the same phase) turns the body into bytes.
        onCallRespond { call, body -> if (body is ApiErrorDto) call.attributes.put(OutcomeKey, body.error) }
        on(ResponseSent) { call -> if (normalisedPath(call) !in PROBE_PATHS) emitAccessLine(call) }
    }

/**
 * Write [call]'s line. NEVER propagates, as `Audit.event`: the response is
 * already sent, and a broken appender must not turn it into a failure.
 */
internal fun emitAccessLine(call: ApplicationCall) {
    try {
        val method = call.request.httpMethod
        // The plugin's own match when it made one; `off` makes none.
        val match = call.attributes.getOrNull(RouteMatchKey) ?: AuthPolicy.match(method, call.request.path())
        val route = match?.pattern ?: "unmatched"
        val status = call.response.status()?.value
        val auth = call.hoglakeAuth
        val caller = auth.callerOrNull
        val service = call.application.attributes.getOrNull(AuthServiceKey)
        val args = mutableListOf<StructuredArgument>()

        fun put(
            key: String,
            value: Any?,
        ) {
            if (value != null) args += kv(key, value)
        }
        put("request_id", call.requestId)
        put("listener", service?.listener)
        put("method", method.value)
        put("status", status)
        put("route", route)
        put("catalog", match?.catalog)
        put("namespace", match?.namespace)
        put("duration_ms", call.attributes.getOrNull(StartKey)?.let { (System.nanoTime() - it) / 1_000_000 })
        put("queue_wait_ms", call.queueWaitMs)
        put(
            "auth",
            when (auth) {
                AuthState.Off -> "off"
                AuthState.Anonymous -> "anonymous"
                is AuthState.Authenticated -> "authenticated"
            },
        )
        put("kind", caller?.kind?.name?.lowercase())
        put("org", caller?.orgId)
        put("jti", caller?.tokenId)
        put("usr", caller?.user)
        put("client", caller?.clientName)
        put("client_class", service?.clientClass(call) ?: clientClass(call))
        put("team", team(match?.namespace, caller))
        if (status !in 200..299) put("outcome", call.attributes.getOrNull(OutcomeKey))
        accessLog.info("${method.value} $route ${status ?: "-"}", *args.toTypedArray())
    } catch (t: Throwable) {
        appLog.error("access log emission failed", t)
    }
}

/**
 * The team a request is about: the `team_<id>` namespace it names, else the
 * one team a scoped caller's `team_id` grant holds. Null when neither says.
 */
private fun team(
    namespace: String?,
    caller: Caller?,
): Long? =
    namespace?.let(TeamNamespaces::id)
        ?: (caller?.predicates?.get(TeamNamespaces.PREDICATE_FIELD) as? PredicateGrant.Values)
            ?.values
            ?.keys
            ?.singleOrNull()
