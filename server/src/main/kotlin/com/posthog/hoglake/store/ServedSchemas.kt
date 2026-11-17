package com.posthog.hoglake.store

import com.posthog.hoglake.service.Identifiers
import com.posthog.hoglake.service.ReservedNamespaces

/**
 * One namespace this server serves to remote readers as a remote schema's
 * Host (docs/auth-tenancy.md §6.5d), as HOGLAKE_SERVED_SCHEMAS_FILE states
 * it. The namespace is served under its real name, and so is every table in
 * it: there is no name mapping.
 */
data class ServedSchema(
    /**
     * The namespace's name in [catalog], on the wire, and in a token's
     * `remote_schemas`. A Client that shows the schema under an alias relays
     * this name (its entry's `remote_name`).
     */
    val name: String,
    /** The source catalog, in this process's PostgreSQL. Never named to a reader. */
    val catalog: String,
    /**
     * The column a table's rows are filtered on. A table is served only when
     * its object-prefix field is a column of this name; the caller's `prd`
     * grant on that name is its read scope.
     */
    val teamField: String = DEFAULT_TEAM_FIELD,
) {
    init {
        require(Regex(Identifiers.PATTERN).matches(name)) {
            "a served schema's name must be a namespace name (${Identifiers.PATTERN}), got '$name'"
        }
        require(Regex(Identifiers.CATALOG_NAME_PATTERN).matches(catalog)) {
            "a served schema's catalog must be a catalog name (${Identifiers.CATALOG_NAME_PATTERN}), got '$catalog'"
        }
        require(!ServedSchemas.isOrgId(catalog)) {
            "a served schema's catalog must not have the shape of an org id (a UUID), got '$catalog': " +
                "a path with that catalog and a served namespace is a remote reader's"
        }
        require(Regex(Identifiers.PATTERN).matches(teamField)) {
            "a served schema's team_field must be a column name (${Identifiers.PATTERN}), got '$teamField'"
        }
    }

    companion object {
        const val DEFAULT_TEAM_FIELD = "team_id"
    }
}

/**
 * The namespaces this server serves as a remote schema's Host. Static
 * configuration, the same for every caller. Who may read one is the token's
 * to say (auth/StoreEntitlement.kt), and which rows is the org's record and
 * [ReadScope].
 */
class ServedSchemas(
    val schemas: List<ServedSchema>,
) {
    init {
        val folded = schemas.map { ReservedNamespaces.fold(it.name) }
        require(folded.toSet().size == folded.size) {
            "two served schemas have the same name (ASCII case ignored): ${schemas.map { it.name }}"
        }
    }

    val isEmpty: Boolean get() = schemas.isEmpty()

    /**
     * The served schema a request path names, or null: the path's
     * `{namespace}` is exactly a served name, and its `{catalog}` has the
     * shape of an org id. Decided from the path alone, so it is the same
     * answer before a token is verified and after, and for every caller.
     *
     * Only an org-shaped catalog is captured. A catalog of this server's own
     * keeps all its routes, its namespace of a served name included.
     */
    fun find(
        catalog: String?,
        namespace: String?,
    ): ServedSchema? {
        if (catalog == null || namespace == null || schemas.isEmpty() || !isOrgId(catalog)) return null
        return schemas.firstOrNull { it.name == namespace }
    }

    companion object {
        val NONE = ServedSchemas(emptyList())

        /** The shape [find] captures, as a pattern PostgreSQL's `~` reads the same way. */
        const val ORG_ID_PATTERN = "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
        private val ORG_ID = Regex(ORG_ID_PATTERN)

        /** Whether [catalog] is a canonical lowercase UUID: an org id, as in a mount's name ([MountNames]). */
        fun isOrgId(catalog: String): Boolean = ORG_ID.matches(catalog)

        /** The name of [org]'s record in `hog_mount`: the legacy mount's, until the layout carries the org. */
        fun recordName(org: String): String = "$org-events"
    }
}
