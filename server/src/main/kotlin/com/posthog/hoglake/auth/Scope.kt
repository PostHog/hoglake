package com.posthog.hoglake.auth

/**
 * The catalog-scoped authority a token carries, one per catalog it may touch.
 *
 * SCOPES ARE PER-CATALOG, not global. A flat `scope: "read write"` string on a
 * token minted for an org's `-dev` catalog would read production, and the
 * server would have no way to notice: the token is valid, the caller is
 * entitled to *a* catalog, and only the pairing is wrong. Carrying the map
 * instead makes authorization a set-membership test on one key, so a tenant is
 * never trusted to have picked the right value.
 *
 * ORDERED BY STRENGTH, and [implies] uses that order. [MAINTENANCE] sits above
 * [WRITE] because a compaction or expiry is a write that also destroys history
 * a plain writer cannot reach, and [ADMIN] above that because dropping a catalog
 * is the one operation with no undo. [READ] is the floor.
 *
 * Deliberately NOT an enum with a `bitmask`: the wire form is a JSON array of
 * these lowercase names (see [wire]), the same vocabulary the OpenAPI
 * `TokenScope` schema publishes, and a client needs to read it without
 * importing us.
 */
enum class Scope(
    /** The name on the wire and in the token's `cat` claim. */
    val wire: String,
) {
    /** Plan, list, read metadata, fetch statistics. Never mutates. */
    READ("read"),

    /** Commit, upload, DDL, truncate, consumer offsets. */
    WRITE("write"),

    /** Expire, compact, rehydrate, cleanup, publications. */
    MAINTENANCE("maintenance"),

    /** Drop the catalog; manage this tenant's own credentials. */
    ADMIN("admin"),
    ;

    /**
     * Whether holding this scope satisfies a requirement for [other].
     *
     * `ADMIN` satisfies everything below it and `MAINTENANCE` satisfies `WRITE`
     * and `READ`, on the same reasoning a filesystem's owner bit does: the
     * operations are nested, and a token asked to compact a table is already
     * trusted to write it. The converse never holds, which is the whole point —
     * `WRITE` does not imply `MAINTENANCE`, so a customer's writer cannot trigger
     * a compaction that rewrites files under a snapshot their readers pinned.
     */
    fun satisfies(other: Scope): Boolean = ordinal >= other.ordinal

    companion object {
        private val byWire = entries.associateBy { it.wire }

        /** Parse one wire name; null when it is not ours. */
        fun fromWire(value: String): Scope? = byWire[value]

        /**
         * Parse a claim's scope list. Unknown names are DROPPED, not refused.
         *
         * A token is signed by us, so an unknown name means the minting side is
         * newer than this verifier. Dropping it fails closed (the caller loses
         * an authority, and a 403 names the scope it wanted); refusing the whole
         * token would take down every caller during a rolling upgrade for a
         * capability most of them do not use.
         */
        fun parseAll(values: Iterable<String>): Set<Scope> = values.mapNotNull(::fromWire).toSet()
    }
}
