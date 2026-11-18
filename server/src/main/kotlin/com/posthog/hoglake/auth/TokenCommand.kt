package com.posthog.hoglake.auth

/**
 * `hoglake token keygen` and `hoglake token mint`: the local issuer's shell
 * surface (docs/auth.md, "Local issuer"). Reached through the server binary
 * (`bin/hoglake-server token …`, or `just server token …`), which runs this
 * and exits instead of listening.
 *
 * Offline by construction: it reads `HOGLAKE_AUTH_SIGNING_KEY` from the
 * environment, prints to stdout, and opens no socket. `keygen` prints one
 * `HOGLAKE_AUTH_SIGNING_KEY=…` line and nothing else, so `export $(…)` and
 * `>> .env` both work; `mint` prints the compact JWS and nothing else, so
 * `TOKEN=$(…)` works. Everything a human reads goes to stderr.
 *
 * [run] takes its environment and streams as parameters so a test drives it
 * in-process; [main] wires the real ones.
 */
object TokenCommand {
    const val DEFAULT_TTL = "24h"
    private const val USAGE_EXIT = 2
    private const val FAILURE_EXIT = 1

    private val usage =
        """
        usage: hoglake token keygen
               hoglake token mint --catalog <name>[=<scope>] [--catalog …] [--scope <scope>]
                                  [--org <id>] [--sub <name>] [--client <name>] [--ttl <duration>]
               hoglake token mint --operator [--catalog <name> --org <id>] [--sub <name>] [--client <name>] [--ttl <duration>]

        keygen  prints a fresh HOGLAKE_AUTH_SIGNING_KEY=<value> line: export it where the server
                (HOGLAKE_AUTH_MODE=local) and this command both read it.
        mint    prints a token signed with HOGLAKE_AUTH_SIGNING_KEY, for a client's fixed-token setting.
                --catalog   a catalog the token may use, repeatable; <name>=<scope> sets that catalog's scope
                --scope     the scope for catalogs given without one: read (default), write, maintenance, admin
                --operator  an operator token instead of a tenant one: fleet-wide, or bound to one --catalog
                --org       the org recorded in the audit log (default: local)
                --sub       the subject recorded in the audit log (default: local)
                --client    the client name recorded in the audit log (default: hoglake-token)
                --ttl       lifetime, e.g. 15m, 24h (default), 7d, or seconds
                HOGLAKE_AUTH_ISSUER and HOGLAKE_AUTH_AUDIENCE are honoured when set (defaults: hoglake, hoglake).
        """.trimIndent()

    /** Run with [args] (after `token`), returning the process exit code. */
    fun run(
        args: List<String>,
        env: (String) -> String?,
        out: Appendable,
        err: Appendable,
        now: () -> Long = { System.currentTimeMillis() / 1000 },
    ): Int {
        if (args.isEmpty() || args[0] in setOf("-h", "--help", "help")) {
            err.appendLine(usage)
            return if (args.isEmpty()) USAGE_EXIT else 0
        }
        return when (args[0]) {
            "keygen" -> keygen(args.drop(1), out, err)
            "mint" -> mint(args.drop(1), env, out, err, now)
            else -> {
                err.appendLine("hoglake token: unknown subcommand '${args[0]}'")
                err.appendLine(usage)
                USAGE_EXIT
            }
        }
    }

    private fun keygen(
        args: List<String>,
        out: Appendable,
        err: Appendable,
    ): Int {
        if (args.isNotEmpty()) {
            err.appendLine("hoglake token keygen takes no arguments")
            return USAGE_EXIT
        }
        out.appendLine("${LocalSigningKey.ENV}=${LocalSigningKey.generate().encode()}")
        return 0
    }

    private class MintArgs(
        val catalogs: LinkedHashMap<String, Scope?> = LinkedHashMap(),
        var scope: Scope = Scope.READ,
        var operator: Boolean = false,
        var org: String = "local",
        var subject: String = "local",
        var client: String = "hoglake-token",
        var ttlSeconds: Long = parseDuration(DEFAULT_TTL),
    )

    private fun mint(
        args: List<String>,
        env: (String) -> String?,
        out: Appendable,
        err: Appendable,
        now: () -> Long,
    ): Int {
        val parsed =
            try {
                parseMintArgs(args)
            } catch (e: IllegalArgumentException) {
                err.appendLine("hoglake token mint: ${e.message}")
                err.appendLine(usage)
                return USAGE_EXIT
            }
        val key =
            try {
                LocalSigningKey.decode(env(LocalSigningKey.ENV) ?: "")
            } catch (e: IllegalArgumentException) {
                err.appendLine("hoglake token mint: ${e.message}")
                return FAILURE_EXIT
            }
        val issuer = env("HOGLAKE_AUTH_ISSUER")?.trim()?.takeIf { it.isNotEmpty() } ?: LocalIssuer.DEFAULT_LOCAL_ISSUER
        val audience = env("HOGLAKE_AUTH_AUDIENCE")?.trim()?.takeIf { it.isNotEmpty() } ?: LocalIssuer.DEFAULT_AUDIENCE
        val local = LocalIssuer(key, issuer, audience, clock = now)
        val token =
            try {
                if (parsed.operator) {
                    val catalog = parsed.catalogs.keys.singleOrNull()
                    require(parsed.catalogs.size <= 1) { "an operator token is bound to at most one --catalog" }
                    local.operator(
                        subject = parsed.subject,
                        clientName = parsed.client,
                        ttlSeconds = parsed.ttlSeconds,
                        org = if (catalog == null) null else parsed.org,
                        catalog = catalog,
                    )
                } else {
                    require(parsed.catalogs.isNotEmpty()) { "--catalog is required for a tenant token" }
                    local.tenant(
                        subject = parsed.subject,
                        org = parsed.org,
                        catalogs = parsed.catalogs.mapValues { (_, s) -> s ?: parsed.scope },
                        clientName = parsed.client,
                        ttlSeconds = parsed.ttlSeconds,
                    )
                }
            } catch (e: IllegalArgumentException) {
                err.appendLine("hoglake token mint: ${e.message}")
                err.appendLine(usage)
                return USAGE_EXIT
            }
        out.appendLine(token)
        return 0
    }

    private fun parseMintArgs(args: List<String>): MintArgs {
        val m = MintArgs()
        var i = 0

        fun value(flag: String): String {
            require(i + 1 < args.size) { "$flag needs a value" }
            i += 1
            return args[i]
        }
        while (i < args.size) {
            when (val a = args[i]) {
                "--catalog" -> {
                    val v = value(a)
                    val name = v.substringBefore('=')
                    require(name.isNotBlank()) { "--catalog needs a name" }
                    val scope = if ('=' in v) parseScope(v.substringAfter('=')) else null
                    m.catalogs[name] = scope
                }
                "--scope" -> m.scope = parseScope(value(a))
                "--operator" -> m.operator = true
                "--org" -> m.org = value(a).also { require(it.isNotBlank()) { "--org needs a value" } }
                "--sub" -> m.subject = value(a).also { require(it.isNotBlank()) { "--sub needs a value" } }
                "--client" -> m.client = value(a).also { require(it.isNotBlank()) { "--client needs a value" } }
                "--ttl" -> m.ttlSeconds = parseDuration(value(a))
                else -> throw IllegalArgumentException("unknown argument '$a'")
            }
            i += 1
        }
        return m
    }

    private fun parseScope(raw: String): Scope =
        Scope.fromWire(raw.trim().lowercase())
            ?: throw IllegalArgumentException(
                "scope must be one of ${Scope.entries.joinToString(", ") { it.wire }}, got '$raw'",
            )

    /** `15m`, `24h`, `7d`, `90s`, or bare seconds. */
    fun parseDuration(raw: String): Long {
        val m =
            Regex("^(\\d+)([smhd]?)$").matchEntire(raw.trim())
                ?: throw IllegalArgumentException("duration must be <n>[s|m|h|d], got '$raw'")
        val n = m.groupValues[1].toLongOrNull() ?: throw IllegalArgumentException("duration out of range: '$raw'")
        val unit =
            when (m.groupValues[2]) {
                "", "s" -> 1L
                "m" -> 60L
                "h" -> 3_600L
                "d" -> 86_400L
                else -> throw IllegalArgumentException("duration must be <n>[s|m|h|d], got '$raw'")
            }
        require(n > 0) { "duration must be positive, got '$raw'" }
        return Math.multiplyExact(n, unit)
    }

    /** The real entry point: process environment, stdout, stderr. */
    fun main(args: List<String>): Int = run(args, System::getenv, System.out, System.err)
}
