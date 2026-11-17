package com.posthog.hoglake.service

/**
 * The namespaces named for a team, `team_<id>`, and the one place that says
 * which names those are.
 *
 * Two readings of the same shape, for two jobs. [id] is the canonical name
 * of a team's schema: `team_` and the decimal digits of a Long with no
 * leading zero, lowercase only. It decides which schema a token's `team`
 * family entry grants and which one a relay or a served read ever names.
 * [hasShape] is wider on purpose: any case and any leading zeros, because
 * an engine that folds identifiers shows `TEAM_5` and `team_007` as the
 * same kind of name, so a rule that keeps a name for operators or for a
 * remote host must catch every spelling.
 */
object TeamNamespaces {
    /** The `remote_schemas` key and hosts-file family that stand for every `team_<id>` schema. */
    const val FAMILY = "team"

    /** What a refusal or a relay of any team schema is counted against: the family, never a team id. */
    const val LABEL = "team_*"

    /** The `prd` field a team schema's id is held under. */
    const val PREDICATE_FIELD = "team_id"

    private val CANONICAL = Regex("team_(0|[1-9][0-9]*)")
    private val SHAPE = Regex("team_[0-9]+", RegexOption.IGNORE_CASE)

    /** The team id [name] canonically names, or null for every other name, `team_007` and `TEAM_7` included. */
    fun id(name: String): Long? = if (CANONICAL.matches(name)) name.removePrefix("team_").toLongOrNull() else null

    /** The canonical schema name of team [id]. */
    fun name(id: Long): String = "team_$id"

    /** Whether [name] is `team_<digits>` in any ASCII case and with any leading zeros. */
    fun hasShape(name: String): Boolean = SHAPE.matches(name)
}
