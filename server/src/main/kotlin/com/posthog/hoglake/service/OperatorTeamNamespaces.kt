package com.posthog.hoglake.service

import com.posthog.hoglake.model.HoglakeException

/**
 * Catalogs (`HOGLAKE_OPERATOR_TEAM_NAMESPACE_CATALOGS`) in which only an
 * operator creates a namespace named `team_<digits>`. Empty, the default,
 * lists none.
 *
 * A per-team scoped writer reaches `team_<id>` through an operator's org
 * mark, so a tenant that could create such a name itself would own the
 * namespace a later mark or token resolves to. The name is matched on its
 * shape alone, ASCII-case-insensitively and whatever the digits' leading
 * zeros: `TEAM_5` and `team_007` are the same kind of name as `team_5`,
 * and an engine that folds identifiers shows them as one.
 *
 * Refused in `POST /v1/catalogs/{catalog}/namespaces`, the only path that
 * creates a namespace (there is no rename and no implicit creation), for
 * every non-operator caller. Auth `off` has no callers to tell apart and
 * refuses nothing.
 */
class OperatorTeamNamespaces(catalogs: Collection<String>) {
    private val listed: Set<String> =
        catalogs.onEach { name ->
            require(Regex(Identifiers.CATALOG_NAME_PATTERN).matches(name)) {
                "HOGLAKE_OPERATOR_TEAM_NAMESPACE_CATALOGS takes catalog names " +
                    "(${Identifiers.CATALOG_NAME_PATTERN}), got '$name'"
            }
        }.toSortedSet()

    /** The listed catalogs. */
    val catalogs: Set<String> get() = listed

    fun isOperatorOnly(
        catalog: String,
        name: String,
    ): Boolean = catalog in listed && TeamNamespaces.hasShape(name)

    /** 403 `forbidden` for a non-operator creating an operator-only name; [operator] false is such a caller. */
    fun refuse(
        catalog: String,
        name: String,
        operator: Boolean,
    ) {
        if (!operator && isOperatorOnly(catalog, name)) {
            throw HoglakeException.Forbidden("only an operator may create namespace '$name' in catalog '$catalog'")
        }
    }

    companion object {
        val NONE = OperatorTeamNamespaces(emptyList())
    }
}
