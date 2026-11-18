package com.posthog.hoglake.service

import com.posthog.hoglake.model.HoglakeException

/**
 * Namespace names no caller may create in any catalog on this server
 * (`HOGLAKE_RESERVED_NAMESPACES`, plus every remote schema the hosts file
 * configures, and every `team_<digits>` name when it configures the `team`
 * family). Empty reserves nothing.
 *
 * A deployment reserves a name that something else serves under it, for
 * example a query engine that shows a mount as a schema of that name: user
 * data must never sit under it. hoglake itself reserves no name.
 *
 * Matching folds ASCII `A`-`Z` only, so `PostHog` is `posthog` and nothing
 * else is folded, trimmed or prefix-matched. Both backends compare
 * namespace names case-sensitively, so without the fold a case variant
 * could be created, and an engine that folds identifiers (Trino) would
 * then hide it behind the reserved name. Non-ASCII letters are not folded:
 * Kotlin's case folding maps some of them to ASCII (`ſ` to `s`), and such
 * names fail the identifier pattern anyway.
 *
 * Refused in `POST /v1/catalogs/{catalog}/namespaces`, the only path that
 * creates a namespace (there is no rename and no implicit creation), for
 * every caller and every backend, before the store is called. A namespace
 * created before its name was reserved is left alone: readable, writable
 * and droppable, but once dropped it cannot be created again. (One that has
 * a remote schema's name is hidden instead, by the catalog binding, not
 * here.)
 */
class ReservedNamespaces(
    names: Collection<String>,
    /**
     * Also reserve every `team_<digits>` name, in any case and with any
     * leading zeros ([TeamNamespaces.hasShape]): the names a remote host's
     * `team` family serves.
     */
    private val teamFamily: Boolean = false,
) {
    private val folded: Set<String> =
        names.map { name ->
            require(Regex(Identifiers.PATTERN).matches(name)) {
                "HOGLAKE_RESERVED_NAMESPACES takes namespace names (${Identifiers.PATTERN}), got '$name'"
            }
            fold(name)
        }.toSortedSet()

    /** The reserved names, lowercase, apart from the `team_<digits>` family when [teamFamily] reserves it. */
    val names: Set<String> get() = folded

    fun isReserved(name: String): Boolean = fold(name) in folded || (teamFamily && TeamNamespaces.hasShape(name))

    /** 422 `validation` for a reserved [name], as `POST /v1/catalogs` refuses a mount's name. */
    fun refuse(name: String) {
        if (isReserved(name)) {
            throw HoglakeException.Validation("namespace name '$name' is reserved on this server")
        }
    }

    companion object {
        val NONE = ReservedNamespaces(emptyList())

        /** [name] with ASCII `A`-`Z` lowered and nothing else changed: how a reserved name is compared. */
        internal fun fold(name: String): String =
            buildString(name.length) {
                for (c in name) append(if (c in 'A'..'Z') c + ('a' - 'A') else c)
            }
    }
}
