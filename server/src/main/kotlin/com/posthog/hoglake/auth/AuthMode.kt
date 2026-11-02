package com.posthog.hoglake.auth

/**
 * How hard the auth plugin tries to stop you.
 *
 * Three values rather than a boolean, because the middle one is the whole
 * rollout strategy and it has to be a state an operator can put a cluster in and
 * observe (docs/auth-tenancy.md §11). A boolean would force every deployment
 * through an instant flip, which for a fleet of already-running machine clients
 * means a same-minute outage whose blast radius is "every writer at once".
 */
enum class AuthMode(
    val wire: String,
) {
    /**
     * Today's behaviour. The plugin is installed but resolves nothing, so the
     * audit actor is whatever the caller claimed — the pre-auth baseline.
     *
     * KEPT IN THE SHIPPED BINARY INDEFINITELY rather than deleted after the
     * migration: it is the rollback for a bad `enforce` flip, and a rollback that
     * requires a build is not a rollback.
     */
    OFF("off"),

    /**
     * Resolve and decide, then ALLOW regardless, counting the verdict.
     *
     * The point is the `hoglake_auth_requests_total{outcome="missing"}` series: it
     * enumerates, per catalog, every caller that would break under `enforce`. An
     * operator flips only when that reaches zero, which turns "did we find all
     * the clients" from a guess into a scrape.
     */
    WARN("warn"),

    /** The plugin is authoritative. */
    ENFORCE("enforce"),
    ;

    val isEnforcing: Boolean get() = this == ENFORCE

    val resolvesCallers: Boolean get() = this != OFF

    companion object {
        /**
         * Parse the knob, REFUSING anything else by name.
         *
         * `enforce` must never be reachable by a typo in the other direction: a
         * value like `enforced` or `on` silently falling back to `off` is a
         * deployment that believes it is protected and is not. That is the same
         * failure [com.posthog.hoglake.Config] guards against with `boolEnv`, and
         * the answer is the same — fail at boot, naming the variable.
         */
        fun parse(
            name: String,
            raw: String,
        ): AuthMode =
            entries.firstOrNull { it.wire == raw.trim().lowercase() }
                ?: throw IllegalArgumentException(
                    "$name must be one of ${entries.joinToString(", ") { it.wire }}, got '$raw'",
                )
    }
}
