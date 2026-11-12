package com.posthog.hoglake.service

import com.posthog.hoglake.model.HoglakeException

/**
 * Namespace names no caller may create in any catalog on this server
 * (`HOGLAKE_RESERVED_NAMESPACES`). Empty reserves nothing.
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
 * and droppable, but once dropped it cannot be created again.
 */
class ReservedNamespaces(names: Collection<String>) {
    private val folded: Set<String> =
        names.map { name ->
            require(Regex(Identifiers.PATTERN).matches(name)) {
                "HOGLAKE_RESERVED_NAMESPACES takes namespace names (${Identifiers.PATTERN}), got '$name'"
            }
            fold(name)
        }.toSortedSet()

    /** The reserved names, lowercase. */
    val names: Set<String> get() = folded

    fun isReserved(name: String): Boolean = fold(name) in folded

    /** 422 `validation` for a reserved [name], as `POST /v1/catalogs` refuses a mount's name. */
    fun refuse(name: String) {
        if (isReserved(name)) {
            throw HoglakeException.Validation("namespace name '$name' is reserved on this server")
        }
    }

    companion object {
        val NONE = ReservedNamespaces(emptyList())

        private fun fold(name: String): String =
            buildString(name.length) {
                for (c in name) append(if (c in 'A'..'Z') c + ('a' - 'A') else c)
            }
    }
}
