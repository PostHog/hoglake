package com.posthog.hoglake.auth

import com.posthog.hoglake.api.errorBody
import com.posthog.hoglake.model.HoglakeException
import com.posthog.hoglake.observability.Audit
import com.posthog.hoglake.observability.Metrics
import io.github.oshai.kotlinlogging.KotlinLogging
import io.ktor.http.HttpHeaders
import io.ktor.server.application.Application
import io.ktor.server.application.ApplicationCall
import io.ktor.server.application.createApplicationPlugin
import io.ktor.server.application.install
import io.ktor.server.request.httpMethod
import io.ktor.server.request.path
import io.ktor.server.response.header
import io.ktor.server.response.respond
import io.ktor.util.AttributeKey

/**
 * Everything the plugin needs, resolved once at boot.
 *
 * [verifier] is null exactly when [mode] is [AuthMode.OFF]: the two are decided
 * together in [fromConfig] so a half-configured instance (enforce with no key
 * source) cannot be constructed — it fails at boot, naming the variable, rather
 * than 401-ing every request in production.
 */
class AuthService(
    val mode: AuthMode,
    val verifier: JwtVerifier?,
) {
    init {
        require(mode == AuthMode.OFF || verifier != null) {
            "auth mode '${mode.wire}' needs a verifier; HOGLAKE_AUTH_ISSUER plus a JWKS source are required"
        }
    }

    companion object {
        /**
         * Build from config. Every refusal here names the environment variable an
         * operator has to set — this is a boot path, and the message is the only
         * thing they will see.
         */
        fun fromConfig(cfg: com.posthog.hoglake.Config): AuthService {
            val mode = AuthMode.parse("HOGLAKE_AUTH_MODE", cfg.authMode)
            if (mode == AuthMode.OFF) return AuthService(mode, null)

            val issuer = cfg.authIssuer.trim()
            require(issuer.isNotEmpty()) {
                "HOGLAKE_AUTH_MODE=${mode.wire} requires HOGLAKE_AUTH_ISSUER (this instance's issuer URL, " +
                    "e.g. https://hoglake-prod-us.internal)"
            }
            val audience = cfg.authAudience.trim()
            require(audience.isNotEmpty()) { "HOGLAKE_AUTH_AUDIENCE must not be blank" }

            val keys = signingKeys(cfg)
            return AuthService(
                mode = mode,
                verifier =
                    JwtVerifier(
                        issuer = issuer,
                        audience = audience,
                        keys = keys,
                        skewSeconds = cfg.authClockSkewSeconds,
                    ),
            )
        }

        private fun signingKeys(cfg: com.posthog.hoglake.Config): SigningKeys {
            val file = cfg.authJwksFile.trim()
            val url = cfg.authJwksUrl.trim()
            require(file.isNotEmpty() || url.isNotEmpty()) {
                "HOGLAKE_AUTH_MODE=${cfg.authMode} requires a signing-key source: set " +
                    "HOGLAKE_AUTH_JWKS_FILE (a JWKS document on disk, re-read on a key miss) or " +
                    "HOGLAKE_AUTH_JWKS_URL"
            }
            // Both is a misconfiguration worth refusing, not a preference: the
            // file is what an in-cluster deployment rotates and the URL is what a
            // remote issuer publishes, and an operator who set both believes one
            // of them is in use that is not.
            require(!(file.isNotEmpty() && url.isNotEmpty())) {
                "set exactly one of HOGLAKE_AUTH_JWKS_FILE and HOGLAKE_AUTH_JWKS_URL, not both"
            }
            return if (file.isNotEmpty()) {
                JwksSigningKeys.fromFile(java.nio.file.Path.of(file))
            } else {
                JwksSigningKeys.fromUrl(java.net.URI.create(url))
            }
        }
    }
}

private val AuthStateKey = AttributeKey<AuthState>("HoglakeAuthState")

/**
 * What the auth plugin concluded about this request.
 *
 * THREE STATES, NOT A NULLABLE CALLER, and the third one is the reason. "No
 * caller" is ambiguous between two situations that must behave differently:
 *
 * - [Off] — the plugin is not resolving callers ([AuthMode.OFF]). There is no
 *   notion of an anonymous caller here because there is no notion of a caller:
 *   the request is unauthenticated BY CONFIGURATION, and every surface must
 *   behave exactly as it did before auth existed.
 * - [Anonymous] — the plugin IS resolving callers, and this request presented no
 *   valid credential. It is entitled to nothing.
 *
 * Collapsing the two into `null` is a live bug rather than a hypothetical: the
 * first draft of `GET /v1/info` omitted its totals whenever the caller was null,
 * which under the DEFAULT `off` mode stripped the instance totals from every
 * response in production. The type exists so that every read site has to say
 * which of the two it means.
 */
sealed interface AuthState {
    /** The plugin is not resolving callers: `HOGLAKE_AUTH_MODE=off`. */
    data object Off : AuthState

    /** Resolved, and no valid credential was presented (or it did not verify). */
    data object Anonymous : AuthState

    /** Resolved, with a verified token. */
    data class Authenticated(
        val caller: Caller,
    ) : AuthState

    val callerOrNull: Caller?
        get() = (this as? Authenticated)?.caller

    /** The audit `actor`, when there is one to name. */
    val actorOrNull: String?
        get() = callerOrNull?.actor
}

/**
 * The request's auth state.
 *
 * [AuthState.Off] when the plugin is not installed at all, which is what a test
 * or a sub-application that only mounts a subset of the routes sees — and the
 * honest reading: no plugin means no authentication, so behave as though the
 * mode were `off` rather than inventing an anonymous caller.
 */
val ApplicationCall.hoglakeAuth: AuthState
    get() = attributes.getOrNull(AuthStateKey) ?: AuthState.Off

/**
 * The audit `actor` for a request, or a refusal.
 *
 * This is the ONE place a caller-supplied author is adjudicated, and the choice
 * of `422` over a silent override is deliberate: a client that sends
 * `author: "etl-bot"` and is quietly recorded as `client:…` will keep believing
 * its attribution landed, and the discrepancy only surfaces during the incident
 * the audit trail exists for. Under [AuthMode.OFF] the caller's word is still
 * taken, so the pre-auth behaviour is unchanged.
 */
fun ApplicationCall.actorOrRefuse(claimed: String?): String? = resolveActor(hoglakeAuth, claimed)

/**
 * The adjudication itself, split out from the call so it is testable without a
 * Ktor request — the rule is small but it is the one that decides what the audit
 * trail says about who did something.
 */
internal fun resolveActor(
    auth: AuthState,
    claimed: String?,
): String? {
    // Auth off, or no credential on a route that got this far (only reachable in
    // `warn`, which passes everything through): the caller's word is what the
    // pre-auth server took, and taking it again keeps `off` byte-identical.
    val caller = auth.callerOrNull ?: return claimed
    // An operator may attribute an action to something else — that is what bulk
    // and backfill work by an operator IS. Their own identity is the default so
    // an unattributed operator action still lands as `operator:…` rather than
    // `anonymous`.
    if (caller.kind.isOperator()) return claimed ?: caller.actor
    if (claimed != null && claimed != caller.actor) {
        throw HoglakeException.Validation(
            "author '$claimed' is not permitted: a tenant token's actor is its own subject; " +
                "omit `author` to have it filled in",
        )
    }
    return caller.actor
}

/**
 * The auth plugin.
 *
 * Installed on the Application, before any `install*Routes`, so it sees every
 * request including the ones whose handlers know nothing about auth. It runs at
 * the `Plugins` phase, i.e. before routing and before any handler body, which is
 * what makes the deny-by-default in [AuthPolicy] real: there is no route whose
 * first line could have been forgotten.
 */
val HoglakeAuth =
    createApplicationPlugin("HoglakeAuth") {
        // Set by installAuth; the plugin object itself is stateless so a test can
        // install two applications with different services.
        // (Ktor's plugin config cannot hold a nullable field cleanly, so the
        // service arrives as a non-null attribute before install.)
        onCall { call ->
            val service =
                call.application.attributes.getOrNull(AuthServiceKey)
                    ?: throw IllegalStateException("HoglakeAuth installed without an AuthService: this is a wiring bug")

            if (!service.mode.resolvesCallers) return@onCall

            val match = AuthPolicy.match(call.request.httpMethod, call.request.path())
            val token = bearerToken(call)

            var caller: Caller? = null
            var failure: HoglakeException? = null

            if (token != null) {
                try {
                    caller = service.verifier!!.verify(token)
                } catch (v: JwtViolation) {
                    // Detail to the log, reason to the client; see JwtViolation.
                    AuthLog.debug(v) { "token refused for ${call.request.path()}" }
                    failure = HoglakeException.Unauthenticated(v.reason, "token refused")
                }
            }

            // Recorded for BOTH outcomes: an anonymous request under `warn` or
            // `enforce` must be distinguishable from a request in `off`.
            call.attributes.put(
                AuthStateKey,
                caller?.let { AuthState.Authenticated(it) } ?: AuthState.Anonymous,
            )

            // One decision, for every mode and every route shape. A PUBLIC route
            // answers Allow even for an anonymous caller, which is also what makes
            // it survive a stale credential: the parse above may have set
            // `failure`, and an Allow discards it. That is deliberate — a
            // liveness probe behind a session cookie or an expired console token
            // fetching /v1/info must not start 401-ing.
            val decision = AuthPolicy.authorize(caller, match)
            if (decision is AuthDecision.Allow) {
                // The token was still parsed, so a valid one still yields its
                // caller and its per-tenant totals.
                Metrics.authRequest(service.mode.wire, "ok")
                return@onCall
            }
            // A failure already recorded against the token wins over the decision:
            // it names the actual violation (`expired`, a bad signature) where the
            // decision can only say that a token was required.
            failure = failure ?: failureFor(decision, match)
            val refusal = failure ?: return@onCall

            if (service.mode.isEnforcing) {
                call.respondRefusal(refusal)
                return@onCall
            }

            // WARN: count it, name it, and let it through. The caller (when the
            // token verified) is already attached, so the burn-in period produces
            // the attribution `enforce` will rely on.
            countWarn(caller, match, refusal)
        }
    }

private suspend fun ApplicationCall.respondRefusal(failure: HoglakeException) {
    val (status, body) = errorBody(failure)
    if (failure is HoglakeException.Unauthenticated) {
        response.header(HttpHeaders.WWWAuthenticate, "Bearer error=\"${failure.reason}\"")
    }
    // respond() from a plugin's Plugins phase: the handler never runs, so the
    // refusal is the whole response. The detail stays in the log — the client
    // gets the reason code and nothing that describes the parser.
    AuthLog.info { "refusing ${request.httpMethod.value} ${request.path()}: ${failure.message}" }
    respond(status, body)
}

private fun countWarn(
    caller: Caller?,
    match: RouteMatch?,
    failure: HoglakeException,
) {
    val outcome =
        when (failure) {
            is HoglakeException.Unauthenticated ->
                if (failure.reason == JwtViolation.NO_TOKEN) "missing" else "invalid"
            is HoglakeException.Forbidden -> "forbidden"
            is HoglakeException.NotFound -> "not_entitled"
            else -> "other"
        }
    Metrics.authRequest(AuthMode.WARN.wire, outcome)
    val catalog = match?.catalog ?: ""
    if (failure is HoglakeException.Forbidden) {
        Metrics.authDenial(catalog, match?.requirement?.wire() ?: "", outcome)
    }
    Audit.event(
        action = "auth_refused",
        catalog = match?.catalog,
        obj = null,
        outcome = "warn_allowed",
        detail = "${failure::class.simpleName}: ${failure.message} pattern=${match?.pattern}",
        actor = caller?.actor,
        tenant = caller?.orgId,
    )
}

/** Turn a denial into the exception whose status the wire contract documents. */
private fun failureFor(
    decision: AuthDecision,
    match: RouteMatch?,
): HoglakeException? =
    when (decision) {
        is AuthDecision.Allow -> null
        is AuthDecision.Unauthenticated -> HoglakeException.Unauthenticated(JwtViolation.NO_TOKEN, decision.reason)
        // 404, not 403: an unentitled tenant must not learn that the catalog exists.
        // The wording matches a genuinely missing catalog on purpose.
        is AuthDecision.UnknownCatalog -> HoglakeException.NotFound("catalog '${decision.catalog}' not found")
        is AuthDecision.InsufficientScope ->
            HoglakeException.Forbidden(
                "this credential does not hold '${decision.required.wire}' on '${decision.catalog}' " +
                    "(route ${match?.pattern ?: "?"})",
            )
    }

private fun Requirement.wire(): String = scopeOrNull()?.wire ?: name.lowercase()

/**
 * The bearer credential, per RFC 6749 §2.1: `Authorization: Bearer <token>`.
 *
 * The scheme is matched case-insensitively because the RFC says it is; the token
 * itself must contain no whitespace, which for a compact JWS is a free structural
 * check that rejects a header like `Bearer a b` before the parser sees it.
 */
private fun bearerToken(call: ApplicationCall): String? {
    val header = call.request.headers[HttpHeaders.Authorization] ?: return null
    val trimmed = header.trim()
    if (trimmed.length < 8) return null
    if (!trimmed.regionMatches(0, "bearer ", 0, 7, ignoreCase = true)) return null
    val token = trimmed.substring(7).trim()
    if (token.isEmpty() || token.any(Char::isWhitespace)) return null
    return token
}

private val AuthServiceKey = AttributeKey<AuthService>("HoglakeAuthService")

/**
 * Install the plugin with [service] bound to the application.
 *
 * The service rides an application attribute rather than the plugin's own config
 * so the plugin stays a `val` — a single object, installed the same way twice,
 * holding no per-application state that a second test application could inherit.
 */
fun Application.installAuth(service: AuthService) {
    attributes.put(AuthServiceKey, service)
    install(HoglakeAuth)
}

private val AuthLog = KotlinLogging.logger("com.posthog.hoglake.auth")
