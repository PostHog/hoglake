---
hoglake: minor (Server)
---

Stream tables in team namespaces (#651): the `team` entry of `HOGLAKE_SERVED_NAMESPACES_FILE` takes an optional `stream` block (catalog, namespace, published tables), and `team_<id>` then serves those tables filtered to that one team, beside the team's stored imports. A caller gets a team's stream rows only when the org mark on the team's stored import namespace equals the token's `org`. A stream table wins a name collision with an import. Off unless a `stream` block is set. Operators: in an environment that serves customers, run `enforce` and audit every `team_<digits>` org mark before setting one.
