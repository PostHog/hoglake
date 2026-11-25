# Identifier case — case-preserving, case-insensitive names

Status: **proposed** (2026-11-25), not implemented. Today the server
compares namespace, table, view and column names exactly. This document
is the change, the reasons for it, and the order to ship it in.

Companions: [README.md](../README.md) (architecture),
[trino-integration.md](trino-integration.md) (the connector whose
lowercasing drives this), [iceberg-federation.md](iceberg-federation.md)
(the facade that will export these names),
[../duckdb-client/DESIGN.md](../duckdb-client/DESIGN.md) §Identifier
case (the client-side bridge this replaces),
[auth-tenancy.md](auth-tenancy.md) §6.2c and §6.2d (the two places that
already fold case).

## The decision

hoglake identifiers become **case-preserving and case-insensitive**
(CPCI), and the server enforces it:

1. **Stored as written.** `Events` stays `Events`.
2. **Unique after folding.** No two live siblings are equal once ASCII
   `A`-`Z` is folded to `a`-`z`: namespaces in a catalog, tables and
   views in a namespace (one scope for both), columns under one parent
   at every nesting level.
3. **Resolved by the server.** A lookup takes the exact match if there
   is one, otherwise the one fold-equal name. Two fold-equal names and
   no exact match is a typed refusal, never a guess.
4. **Returned as stored.** A response carries the stored spelling,
   whatever the request spelled.
5. **The fold is ASCII only.** It is the same function in Kotlin,
   Python, C++, Java, PostgreSQL and SQLite, and it stays ASCII-only if
   the identifier alphabet is ever widened.
6. **Files bind by field id**, as now. Names in a Parquet footer decide
   nothing except in files that carry no ids.

## Why: every engine hoglake cares about folds case

hoglake has three stated goals, and each one meets an engine that
changes the case of names before it compares them.

| Goal | What folds | What hoglake has to guarantee |
|---|---|---|
| Served by truffle | Trino lowercases every schema, table and column name, quoted or not | A lowercased name finds the object, and no two objects share a lowercased name |
| An open table format | Spark, DuckDB and Databricks resolve columns case-insensitively; Hive, Glue and Unity Catalog store names lowercased; Snowflake uppercases | The format states one rule, so two readers cannot disagree about which column a name means |
| Iceberg interoperability | The Iceberg spec has no case rule; its engines each apply their own | An exported table is readable by engines that fold, not only by engines that do not |

One property serves all three: **names stay distinct under any engine's
fold**. Uniqueness after folding is that property. Preserving the
stored case costs nothing on top of it, and lowercasing on store would
discard spelling that DuckDB, Spark and Python users see.

## Where hoglake is today

Read from this repository and from PostHog/truffle at `7fb1767`
(2026-11-25). The Trino behaviour is read from the connector's source,
not run.

- **Names are case-sensitive and case-preserving.** They match
  `^[A-Za-z_][A-Za-z0-9_-]{0,127}$` (`Identifiers.PATTERN`). The live-name
  indexes are on the exact text (`hog_namespace_live_name`,
  `hog_table_version_live_name`, `hog_view_live_name`), so `Events` and
  `events` coexist. Sibling column names are checked for exact
  duplicates in the service only (`ColumnTrees`, `AlterService`); no
  index covers them.
- **A table and a view can share a name, even spelled the same.** On
  both backends `createTable` looks only at tables and `createView`
  only at views. Read from the code, not run.
- **Trino cannot address a mixed-case name.** The SPI lowercases in
  `SchemaTableName`, `ColumnMetadata` and `ColumnSchema`. The connector
  sends that lowercased name (`HoglakeMetadata.getTableHandle`), and
  the server's `tv.name = :name` does not match `Events`. The table is
  listed and cannot be queried.
- **Case-colliding columns are unreadable in Trino.** Trino's Parquet
  footer reader lowercases field names, so the connector refuses a file
  whose bound column has a case twin: "Names that differ only by case
  are not supported" (`HoglakePageSourceProvider.checkNoCaseCollision`).
  Field-id binding does not save such a file.
- **The connector builds write paths from lowercased names.**
  `HoglakeMetadata.tableWritePath` joins Trino's schema and table names
  into `data/<namespace>/<table>/`, while the server spells the table
  root with the stored names (`ObjectPrefix.tableRoot`).
- **Four places already patched this on their own, each differently.**

  | Where | What it does |
  |---|---|
  | duckdb-client | Resolves every name case-insensitively against a listing and sends the server's spelling; reports case-only pairs as ambiguous |
  | `ReservedNamespaces` | Folds ASCII `A`-`Z` before comparing |
  | `TeamNamespaces.hasShape` | Matches `team_<digits>` in any ASCII case |
  | `VariantShredding` | Refuses fields that differ only by case, using the JDK's `lowercase()`; pyhoglake carries a `Σ` carve-out because Python and the JDK fold differently |

- **truffle's other lake connectors chose list-and-match.** Its DuckLake
  connector lowercases and fails on "multiple tables differ only in
  case"; Trino's Iceberg REST catalog has
  `iceberg.rest-catalog.case-insensitive-name-matching`, which lists
  remote names and caches a lowercase-to-remote map.

Not known: how many mixed-case names and case-only pairs production
holds. The [audit queries](#appendix-audit-queries) answer it, and the
answer sets how much of the [rollout](#rollout) is needed.

## Prior art

Read on 2026-11-25 from each project's default branch or documentation.

| System | Stored | Resolution | Case-only duplicates |
|---|---|---|---|
| Iceberg spec, Iceberg REST spec | As written | Not stated; data binds by field id | Not addressed |
| Iceberg Java library | As written | Case-sensitive by default | Accepted; the first case-insensitive lookup throws |
| Delta protocol | As written | Case-insensitive | Refused |
| DuckLake spec | As written | Not stated | Not addressed |
| DuckDB and its DuckLake extension | As written | Case-insensitive, quoted or not; ASCII fold | Refused |
| Hive Metastore, AWS Glue, Athena | Lowercased | Case-insensitive | Cannot exist |
| Unity Catalog | Object names lowercased; column case kept | Case-insensitive | Cannot exist for objects |
| Lakekeeper (namespaces, tables, views) | As written | Case-insensitive, ICU collation | Refused |
| Apache Polaris | As written | Case-sensitive | Accepted |
| Paimon | As written | Per catalog: `case-sensitive` | Depends on the option |
| Trino | Lowercased by the engine | Case-insensitive | `AMBIGUOUS_COLUMN_NAME` |
| Spark (default) | As written | Case-insensitive | Refused |
| Snowflake | Unquoted names uppercased | Exact after folding | Accepted through quoting |
| Flink, ClickHouse | As written | Case-sensitive | Accepted |
| Parquet | As written | Exact string | Not rejected by parquet-java |

What the survey shows:

- **The specs that say nothing are where the interop bugs are.**
  Iceberg's Java library builds a schema holding `data` and `DATA`,
  then fails with "[Cannot build lower case index: data and DATA
  collide](https://github.com/apache/iceberg/blob/2e12b22aea8e43af1d3bc1559decc6bbe2e92ac0/api/src/main/java/org/apache/iceberg/types/TypeUtil.java#L240-L259)"
  on the first case-insensitive lookup. A request for case-insensitive
  matching in the REST client was closed as not planned
  ([apache/iceberg#14386](https://github.com/apache/iceberg/issues/14386)).
- **Delta wrote the column rule down.** "[All column names must be
  unique regardless of casing](https://github.com/delta-io/delta/blob/ba9194122531fb576e1b6404038cc596bfce1825/PROTOCOL.md?plain=1#L2760-L2761)";
  its documentation calls the schema "case-preserving but insensitive".
- **Lakekeeper applied the same rule to namespaces and tables**, in the
  catalog, so that it is "[transparent to all query engines — no
  client-side configuration is needed](https://github.com/lakekeeper/lakekeeper/blob/4054ae96775158fc47ab61845b9e9f0852d71e4e/docs/docs/concepts.md?plain=1#L26-L35)".
  Polaris, which is case-sensitive, has an open request for a
  case-insensitive mode because users arriving from Hive "hit 'not
  found' errors or create duplicate tables"
  ([apache/polaris#5842](https://github.com/apache/polaris/issues/5842)).
- **Trino is not about to change.** [trinodb/trino#17](https://github.com/trinodb/trino/issues/17),
  "Add support for case sensitive identifiers", has been open since
  2019.
- **Tables and views share one name scope in Iceberg's catalogs.**
  Creating a view where a table has the name fails with "[Table with
  same name already exists](https://github.com/apache/iceberg/blob/2e12b22aea8e43af1d3bc1559decc6bbe2e92ac0/core/src/main/java/org/apache/iceberg/view/BaseMetastoreViewCatalog.java#L224)",
  and the reverse with "View with same name already exists". SQL
  engines have one relation namespace for the same reason: `FROM ns.x`
  cannot say which it means.
- **A Unicode-aware fold is a second interop problem.** Lakekeeper
  documents that Trino's Java case rules disagree with its ICU collation
  on letters such as the dotless `ı`. DuckDB folds ASCII only, and so
  does the Unity Catalog OSS Delta API ("[equal under ASCII lowercase
  comparison](https://github.com/unitycatalog/unitycatalog/blob/2bc62bd5ce0d4de4721714e9975e91fcde216e91/api/delta.yaml#L1340-L1347)").
- **Lowercase-only catalogs exist downstream.** Iceberg's Glue client
  rejects, by default, any table name outside `^[a-z0-9_]{1,255}$`. A
  catalog whose names are unique after folding can be lowercased for
  such a target without a collision.

The Iceberg, Delta, Lakekeeper, Glue, Trino, DuckDB, DuckLake, Hive,
Polaris, Paimon, parquet-java, Unity Catalog and Athena rows were
checked against the source file or documentation page. The Spark,
Snowflake, Flink and ClickHouse rows come from one survey pass over
their documentation and were not re-checked.

## The rule

### Scope

| Covered | Not covered, and why |
|---|---|
| Namespace names | Catalog names: lowercase-only already (`CATALOG_NAME_PATTERN`) |
| Table names | Keys inside VARIANT, JSON and MAP values: they are data, and `status` and `Status` stay two keys |
| View names | Property keys, consumer names, mount names: no engine folds them |
| Column names, including struct fields at every depth | Object paths and partition values: bytes, compared exactly |

List and map children keep their fixed names (`element`, `key`,
`value`).

VARIANT shredding declarations keep the rule they have. A declaration
already refuses two fields that differ only by case, because each
shredded field becomes a file column and truffle's reader lowercases
column names. That check folds as the JDK does, to match the reader,
and it limits only what may be shredded: the values keep both keys.

### The fold

`fold(name)` replaces each of ASCII `A`-`Z` with `a`-`z` and changes
nothing else. Two names are **fold-equal** when their folds are equal.

- The identifier alphabet is ASCII, so on a valid name every
  implementation below agrees. The fold is still defined on any string,
  because a lookup folds a name before anything has validated it.
- A general lowercase is wrong here. Kotlin's `lowercase()` maps the
  Kelvin sign `K` (U+212A) to `k`, so a request for `K` would resolve
  to table `k`. `ReservedNamespacesTest` already records this.
- If the alphabet is ever widened, non-ASCII letters are **not** folded.
  That is DuckDB's rule, and it is the only fold every language agrees
  on.

| Where | The fold |
|---|---|
| Kotlin | `Identifiers.fold`, moved from `ReservedNamespaces.fold` |
| PostgreSQL | `lower(name COLLATE "C")` |
| SQLite (Hoglite) | `COLLATE NOCASE` on the comparison |
| Python, C++, Java | An ASCII loop; never `str.lower()` or `toLowerCase()` |

### Uniqueness

Among live siblings, no two names are fold-equal.

| Kind | Siblings | Live means |
|---|---|---|
| Namespace | Same catalog | `NOT dropped` |
| Table or view | Same namespace, tables and views together | Table: live version row, table not dropped. View: `end_snapshot IS NULL` |
| Column | Same table and same parent | `end_snapshot IS NULL` |

- **Tables and views are one scope.** A table cannot take a name
  fold-equal to a live view's, nor a view a table's, including the same
  spelling. This is new on its own account: today the two are checked
  separately. Lookups do not change, because a table route reads tables
  and a view route reads views.
- The set visible at any snapshot was once the live set, so the
  property holds at every snapshot taken after the rule ships.
- A dropped name can be reused in another case: drop `Events`, create
  `events`.
- A rename that changes only the case of the object itself is legal.
  The check excludes the object by id, not by name.
- Pairs that exist before the rule ships are left as they are. Case-only
  pairs of one kind are the one case the resolution rule below has to
  handle. A table and a view that already share a name each stay
  addressable through their own routes.

### Resolution

Every lookup by name, at head or at a snapshot, runs one rule:

1. Read the visible siblings fold-equal to the requested name, the one
   spelled exactly as requested first, and stop after two.
2. No row: not found, as today.
3. One row: that is the object.
4. Two rows, the first spelled exactly as requested: the first.
5. Two rows, neither exact: refuse as ambiguous.

Step 4 keeps a legacy pair addressable by clients that spell names
exactly (pyhoglake, the console, DuckDB). Step 5 is all Trino can get
for such a pair, and it is what truffle's DuckLake connector answers.

A destructive request through another spelling reaches the same object:
`DELETE .../tables/events` drops `Events`. That is what every SQL engine
in the table above does. `expected_table_uuid` remains the guard against
acting on the wrong incarnation.

### What a response carries

- **Bodies carry the stored name.** A `GET .../tables/events` for a
  table stored as `Events` answers `"name": "Events"`. Lakekeeper echoes
  the requested spelling instead; hoglake cannot, because clients build
  object paths from the names in the response.
- **Paths are spelled with the stored name.** `data/<namespace>/<table>/`
  and `<field>=<value>/` are compared as bytes. A client that builds
  them from its own spelling writes to the wrong prefix.
- **Labels and dedupe keys use resolved ids or stored names.** Once a
  lookup resolves any spelling, a metric labelled with the requested
  name gets one series per spelling a client cares to send. The
  blind-append WARN already dedupes on `tableId` rather than on a
  client-supplied name (AGENT.md, invariant 12).
- **Listings list stored names**, both members of a legacy pair
  included.

### Names hoglake reserves

- The `_hog` column prefix is matched after folding, so `_HOG_ROW_ID`
  is refused. Today `Identifiers.validateColumn` uses a case-sensitive
  `startsWith`, and a file holding both `_HOG_ROW_ID` and compaction's
  `_hog_row_id` would meet the connector's case-collision refusal. That
  consequence is read from the two sources, not reproduced.
- Reserved namespace names and the `team_<digits>` shape already fold
  ASCII. They move to `Identifiers.fold` and do not change behaviour.

### Files whose footers hold case-colliding names

The rule is about catalog names. A data file can still carry two
sibling columns that differ only by case: one written by a foreign
writer, or one written under a table that already held such a pair.

- **The catalog cannot see it.** Registration never opens the Parquet,
  so nothing here is a server check.
- **truffle cannot read such a file**, whatever the catalog says: its
  reader lowercases footer names before it binds. The connector refuses
  the read, and its `add_files` procedure runs the same refusal before
  it registers a file.
- **Importers fail by default.** A writer that meets colliding source
  fields refuses the batch. Renaming one with a suffix, or moving both
  into a VARIANT column, is a choice the writer's operator makes. The
  catalog never renames a column on a writer's behalf.
- **truffle binds files without field ids by name**, exact first and
  then ignoring case. With the catalog's names unique after folding,
  that fallback names at most one catalog column per file column.

### Errors

| Request | Answer |
|---|---|
| Create or rename a namespace, table or view to a name fold-equal to another live sibling, a table and a view being siblings | 409 `already_exists`, naming the stored spelling and its kind |
| Add or rename a column to a name fold-equal to another sibling | 422 `validation`, as an exact duplicate answers today |
| Create a table whose definition holds two fold-equal sibling columns | 422 `validation` |
| A column name starting with `_hog` in any case | 422 `validation` |
| A lookup with two candidates and neither exact | 422 `ambiguous_name`, naming the two it read |
| The same lookup by a scoped caller on a namespace-gated route | 404, as a missing namespace answers: a scoped caller must not learn what exists outside its own |

`ambiguous_name` is a new typed `HoglakeException`. The first four rows
are new refusals on paths deployed writers use, so they are staged
([Rollout](#rollout)).

## Implementation

### PostgreSQL

Three partial expression indexes serve both the lookup and the
collision check:

```sql
CREATE INDEX hog_namespace_live_name_folded
    ON hog_namespace (catalog_id, lower(name COLLATE "C"))
    WHERE NOT dropped;
CREATE INDEX hog_table_version_live_name_folded
    ON hog_table_version (catalog_id, namespace_id, lower(name COLLATE "C"))
    WHERE end_snapshot IS NULL;
CREATE INDEX hog_view_live_name_folded
    ON hog_view (catalog_id, namespace_id, lower(name COLLATE "C"))
    WHERE end_snapshot IS NULL;
```

- **`COLLATE "C"` is required.** In a database created with the ICU
  locale `tr-TR`, `lower('TEAM_I')` returns `team_ı`, and
  `lower('TEAM_I' COLLATE "C")` returns `team_i`. Under `"C"` the Kelvin
  sign and `İ` come back unchanged.
- **The lookup needs the index.** Measured on PostgreSQL 18.7 with
  `server/schema.sql`, one namespace of 54,000 live tables and 54,000
  ended version rows, warm:

  | Lookup of one table by name | Plan | Shared buffers |
  |---|---|---|
  | Exact, as today | `hog_table_version_live_name` | 3 |
  | Folded, no new index | Seq Scan, 108,003 rows filtered | 1,495 |
  | Folded, with the index | `hog_table_version_live_name_folded`, 1 search, nothing filtered | 7 |
  | Folded, generic plan with a bound parameter | The same | 7 |

  The table GET is on the writer fleet's flush path and the commit read
  resolves names under the commit lock, so a scan per lookup is not an
  option. This fixture is a design-time check, not the plan test the
  migration's PR owes.
- **Reads at a snapshot do not get slower.** `TableRepo.findAt` has no
  index on a name at a snapshot today: the same fixture reads 1,492
  buffers for an exact name. Folding changes its filter, not its plan.
- **The indexes start out not unique.** A unique build fails on the
  first legacy pair, and a migration that fails leaves every pod unable
  to boot. Until the audit is empty, uniqueness is enforced where exact
  uniqueness of columns already is, in the service, and DDL already
  takes the per-catalog commit lock before its exists check
  (`createNamespace`, `createTable`, `createView`). Unique indexes are
  the end state, as the last [rollout](#rollout) step. They must stay
  partial: `hog_table_version` keeps ended rows, and a comment change
  ends the live row and inserts a new one with the same name. A
  case-only rename in the service's write order (end the live row,
  insert the new one) passes a unique folded index; checked.
- **Tables against views is a service check, permanently.** No index
  spans two tables. `createTable` and a table rename read
  `hog_view_live_name_folded`; `createView` reads
  `hog_table_version_live_name_folded`. Both already hold the commit
  lock.
- **Cost.** 4.1 MB for the 54,003 live names of the fixture, built in
  58 ms on an idle scratch database. `hog_table_version` rows are
  written by DDL, not by commits, so the commit path writes none of
  these indexes. The migration follows the lock-window rules in
  AGENT.md and states its own measurement; a plain build on
  `hog_table_version` waits for an expiry sweep's row lock like any
  other.
- **Columns need no index.** A table's live columns are loaded whole
  and resolved in Kotlin (`AlterService.requireColumn`); the comparison
  changes from `==` to fold-equal.

One resolver per kind replaces the name predicates. The repo functions
(`NamespaceRepo.findLiveByName` and `findAt`, `TableRepo.findLive` and
`findAt`, `ViewRepo.findLiveByName`) read through the folded predicate
with `ORDER BY (name = :name) DESC LIMIT 2` and apply the
[resolution](#resolution) rule in Kotlin. That form uses the folded
index and returns the exact member of a seeded pair first; checked on
the fixture above. The other places that compare a name directly go
through the resolver or use the same predicate: `CommitTail.READ_SQL`,
two reads in `CommitService`, `MountReadService`,
`PartitionStatsService`'s filters, `ServedNamespaces`, and
`CatalogService.requireNeverHeld`. A grep finds about fifteen such
statements outside the Hoglite store.

### Hoglite

- **The comparison is `COLLATE NOCASE`**, which SQLite defines as
  folding the 26 ASCII capitals and nothing else. `lower()` is avoided
  because SQLite's ICU extension replaces it with a Unicode one.
- **Uniqueness is a transaction guard**, in the store's existing
  `expectZero` style: zero live siblings equal under `NOCASE` other
  than the object itself. For a table or a view the guard reads both
  `tables` and `views`. hoglite serializes a tenant's transactions,
  which is what the commit lock gives PostgreSQL.
- **No tenant index in the first cut.** Tenants have no migrations
  (`HogliteSchema` KDoc), so a new index reaches only new tenants. The
  fold lookup reads a tenant's live names, which the design keeps small;
  the PR measures it against hoglited's per-query limits.
- **About forty statements compare a name** across
  `HogliteCatalogStore`, `HogliteCommit`, `HogliteTableCreations`,
  `HogliteAlters`, `HogliteViews`, `HoglitePartitions`,
  `HoglitePartitionLookup` and `HogliteCompaction`. A grep count, not
  an audit. Resolving the namespace and table to ids once and passing
  ids down is the smaller change.
- **Same status, code and body on both backends**, in one test class
  parameterized over them (AGENT.md, Hoglite considerations).

### Authorization

- **Authorize the resolved object, never the spelling.** The namespace
  gate reads a namespace's containment by name (`NamespaceGate`). It
  uses the same resolver as the handler, so `TEAM_5` is authorized as
  `team_5` is, on the mark that namespace carries.
- **The reused-name refusal must fold.** `requireNeverHeld` refuses to
  mark a namespace contained when another namespace has ever had its
  name, because a read at a past snapshot resolves the namespace by
  name and "a dropped namespace that once had this name would be served
  to whoever may read this one". With folded resolution, a dropped
  `Team_5` that held tables is such a namespace for a live `team_5`.
  The check compares folds, or a scoped caller's time-travel read
  crosses into it.
- **Ambiguity is a 404 to a scoped caller** on gated routes, and to a
  scoped write that names its namespaces in the body.
- **Path prefixes stay literal.** Containment compares prefixes as
  bytes, and `data/Team_5/` and `data/team_5/` are different prefixes
  on S3, whose keys are case-sensitive.

### The commit path

`CommitTail.READ_SQL` joins each requested `(namespace, name)` to its
live table with `tv.name = q.name AND ns.name = q.namespace LIMIT 2`.
Both predicates become folded, the lateral orders the exact match
first, and the service applies the resolution rule to the two rows.
This is a change to the commit path, so its PR answers the six scale
questions in AGENT.md and carries a plan test on a production-shaped
fixture.

### Wire contract

- **OpenAPI.** The name schemas keep their pattern and gain the rule:
  compared after folding ASCII case, returned as stored. The
  `name` and `namespace` fields of a response are documented as the
  stored spelling.
- **A capability**, `case-insensitive-names-v1`, in the catalog's
  `capabilities`, so a client can tell a server that resolves case from
  one that does not.
- **A new error code**, `ambiguous_name`.
- **A shared vector file**, `pyhoglake/tests/vectors/identifier_case_vectors.json`,
  in the style of the bounds and shredding vectors: names, their folds,
  fold-equal and not-equal pairs, with the count pinned on both sides.

## Consumers

| Consumer | Today | Change |
|---|---|---|
| truffle (`trino-hoglake`) | Sends Trino's lowercased names; builds write paths from them; exposes no hoglake views (`HoglakeMetadata` has no view methods) | Nothing for reads. Build write paths, and the names in commits, from the `namespace` and `name` of the table response. `checkNoCaseCollision` stays for legacy files |
| Trino harness (`server/trino/`) | No mixed-case case | A namespace, table and column created in mixed case through the REST API, read through Trino in lowercase |
| pyhoglake | Exact names. `Catalog.namespace()` keeps the caller's spelling on its fallback path; table paths come from `TableInfo` | Take the namespace's name from the response. Arrow columns keep matching exactly (an [open question](#open-questions)) |
| duckdb-client | Resolves through listings; conflict-checks DDL itself | Keeps working unchanged. With the capability it can send the typed name and drop the listing round trip |
| hedgerow | Matches source and destination columns by exact name | None |
| millrace | Looks configured columns up by exact name | None |
| webui | Routes by names from listings | None required |
| Iceberg facade (not built) | | Resolve REST namespace, table and view identifiers with the same resolver; export stored names. Trino's Iceberg connector then needs no `case-insensitive-name-matching`. A view named like a table answers `AlreadyExists`, as Iceberg's own catalogs do |

## Rollout

The repository's rule for a new refusal on a deployed writer's path is
a flag that defaults off, a counter, and a flip when the counter is
quiet (AGENT.md, Scale doctrine).

1. **Audit.** Run the [queries](#appendix-audit-queries) in each
   environment. Three numbers matter: case-only pairs, tables and views
   that share a name, and live names that are not already lowercase.
   The last is what truffle cannot address today.
2. **Resolve and count.** Ship the fold, the indexes, the resolver on
   both backends, `ambiguous_name` and the capability. A request that
   would create a case-only pair, or a table and a view sharing a name,
   is still accepted, logged at WARN once per resolved parent per pod,
   and counted in `hoglake_colliding_names_total{catalog,kind}`.
   `HOGLAKE_REFUSE_COLLIDING_NAMES` defaults to `false`.
3. **Refuse.** Set the flag per environment once the counter has stayed
   at zero, then make `true` the default in a later release. From here
   the uniqueness rule holds for everything new.
4. **Clients.** truffle builds paths from stored names before any
   mixed-case table with an object-prefix layout or a contained
   namespace is written through it. The DuckDB extension may drop its
   listing round trip.
5. **Enforce in the database.** Once the case-only audit is empty in
   every environment and the flag is on, a later migration replaces the
   three folded indexes with unique ones. A pair found before then is
   renamed by its owner first. The table-against-view check stays in
   the service.

Step 2 changes one thing a client could notice besides the count: a
request that answered 404 for `events` now finds `Events`. A client
that creates on 404 stops creating case twins, which is the point.

## Tests

- **The fold, cross-language.** The shared vector file, plus a property
  test that `Identifiers.fold` equals PostgreSQL's `lower(x COLLATE "C")`,
  SQLite's `NOCASE` and Java's `toLowerCase(ENGLISH)` on every string
  the identifier pattern admits, and differs from Kotlin's `lowercase()`
  on the Kelvin sign.
- **Parity.** One class over both backends: create, rename, drop and
  lookup in each kind, at head and at a snapshot, asserting status, code
  and body. It includes a table against a view in both orders, same
  spelling and another case.
- **Legacy pairs.** Seeded by SQL, since the API will refuse to make
  them: exact wins, a fold-only lookup is `ambiguous_name`, listings
  show both.
- **Authorization.** A scoped caller reaches its namespace through
  another spelling; the reused-name refusal fires on a fold-equal
  dropped namespace; ambiguity answers 404.
- **The plan.** The folded lookup and the commit read on a
  production-shaped fixture, asserting the index, one search per probe
  and no filtered rows.
- **Mutation.** Flip each fold-equal comparison back to exact and each
  self-exclusion off; the test written for it must red.
- **Fuzzing.** The wire corpus gains case variants of names.

## Alternatives rejected

- **Lowercase on store** (Hive, Glue, Unity Catalog objects). The
  simplest rule and the best fit for Glue. It discards the spelling a
  source system chose, and existing mixed-case names would need
  renaming, which the object-prefix layout forbids because the name is
  in every file's path. It stays available later as a per-catalog
  tightening: CPCI guarantees lowercasing loses nothing.
- **Case-sensitive, with a shim in each client** (today). The DuckDB
  extension has written one, and the server has patched three places of
  its own. The shim Trino's Iceberg REST catalog uses lists remote names
  and caches the mapping; against hoglake that is the table listing,
  9 MiB unpaged on a 54,000-table namespace (AGENT.md, Working
  conventions).
- **A Unicode-aware collation** (Lakekeeper's ICU `und-u-ks-level2`).
  It needs every client to fold as ICU does, and Trino does not.
- **Echo the requested spelling** (Lakekeeper). Clients build object
  paths from response names.
- **A stored folded column** (`name_folded` beside `name`). It is a
  second copy of a fact the name already holds, and it has to be kept
  equal on every write. PostgreSQL indexes the expression instead.
  Hoglite cannot add a column to tenants that exist, and `NOCASE` needs
  none.
- **The catalog disambiguates colliding fields** (a suffix, or a spill
  into VARIANT). A writer may; the catalog would be inventing column
  names nobody asked for.

## Open questions

1. **What does a deployment holding a legacy pair do at the unique
   migration?** The build fails and rolls back, and the server cannot
   boot until the pair is renamed. Proposed: the release that carries
   the migration names the audit as its prerequisite, and it does not
   ship until PostHog's own environments are empty.
2. **`ambiguous_name` as 422 or 409?** Proposed 422: the request names
   nothing the server can act on, and retrying does not change that.
3. **Should pyhoglake match Arrow column names by fold?** Today a
   column the table does not spell exactly is refused. Exact matching
   is safe and stricter; a fold match is unambiguous once the rule
   holds. Proposed: keep exact, and name the fold-equal column in the
   refusal.
4. **A lowercase-only catalog option** for catalogs that will be
   registered in Glue. Not now.

## Appendix: audit queries

Read-only. The column query reads every live `hog_column` row, so run
it on a replica or with a statement timeout. Both sets were run against
seeded fixtures: `server/schema.sql` on PostgreSQL 18.7, and tables with
the tenant schema's columns on SQLite 3.46.1 (not a hoglite tenant).

PostgreSQL, case-only pairs:

```sql
-- namespaces
SELECT c.name AS catalog, array_agg(n.name ORDER BY n.name) AS names
  FROM hog_namespace n JOIN hog_catalog c USING (catalog_id)
 WHERE NOT n.dropped
 GROUP BY c.name, lower(n.name COLLATE "C")
HAVING count(*) > 1;

-- tables
SELECT c.name AS catalog, n.name AS namespace,
       array_agg(tv.name ORDER BY tv.name) AS names
  FROM hog_table_version tv
  JOIN hog_table t USING (catalog_id, table_id)
  JOIN hog_namespace n USING (catalog_id, namespace_id)
  JOIN hog_catalog c USING (catalog_id)
 WHERE tv.end_snapshot IS NULL AND t.dropped_snapshot IS NULL AND NOT n.dropped
 GROUP BY c.name, n.name, lower(tv.name COLLATE "C")
HAVING count(*) > 1;

-- views
SELECT c.name AS catalog, n.name AS namespace,
       array_agg(v.name ORDER BY v.name) AS names
  FROM hog_view v
  JOIN hog_namespace n USING (catalog_id, namespace_id)
  JOIN hog_catalog c USING (catalog_id)
 WHERE v.end_snapshot IS NULL AND NOT n.dropped
 GROUP BY c.name, n.name, lower(v.name COLLATE "C")
HAVING count(*) > 1;

-- sibling columns; parent_field_id is NULL for top-level columns
SELECT c.name AS catalog, n.name AS namespace, tv.name AS "table",
       col.parent_field_id, array_agg(col.name ORDER BY col.name) AS names
  FROM hog_column col
  JOIN hog_table t USING (catalog_id, table_id)
  JOIN hog_table_version tv
    ON tv.catalog_id = col.catalog_id AND tv.table_id = col.table_id
   AND tv.end_snapshot IS NULL
  JOIN hog_namespace n
    ON n.catalog_id = tv.catalog_id AND n.namespace_id = tv.namespace_id
  JOIN hog_catalog c ON c.catalog_id = col.catalog_id
 WHERE col.end_snapshot IS NULL AND t.dropped_snapshot IS NULL
 GROUP BY c.name, n.name, tv.name, col.table_id, col.parent_field_id,
          lower(col.name COLLATE "C")
HAVING count(*) > 1;
```

PostgreSQL, a table and a view sharing a name, in the same spelling or
another case:

```sql
SELECT c.name AS catalog, n.name AS namespace,
       tv.name AS "table", v.name AS view
  FROM hog_table_version tv
  JOIN hog_table t USING (catalog_id, table_id)
  JOIN hog_view v
    ON v.catalog_id = tv.catalog_id AND v.namespace_id = tv.namespace_id
   AND lower(v.name COLLATE "C") = lower(tv.name COLLATE "C")
  JOIN hog_namespace n
    ON n.catalog_id = tv.catalog_id AND n.namespace_id = tv.namespace_id
  JOIN hog_catalog c ON c.catalog_id = tv.catalog_id
 WHERE tv.end_snapshot IS NULL AND t.dropped_snapshot IS NULL
   AND v.end_snapshot IS NULL AND NOT n.dropped;
```

PostgreSQL, live names truffle cannot address today:

```sql
SELECT 'namespace' AS kind, count(*) FROM hog_namespace
 WHERE NOT dropped AND name <> lower(name COLLATE "C")
UNION ALL
SELECT 'table', count(*)
  FROM hog_table_version tv JOIN hog_table t USING (catalog_id, table_id)
 WHERE tv.end_snapshot IS NULL AND t.dropped_snapshot IS NULL
   AND tv.name <> lower(tv.name COLLATE "C")
UNION ALL
SELECT 'view', count(*) FROM hog_view
 WHERE end_snapshot IS NULL AND name <> lower(name COLLATE "C");
```

Hoglite, per tenant, case-only pairs:

```sql
SELECT group_concat(name, ', ') FROM namespaces
 WHERE dropped_snapshot IS NULL
 GROUP BY name COLLATE NOCASE HAVING count(*) > 1;

SELECT namespace, group_concat(name, ', ') FROM tables
 WHERE dropped_snapshot IS NULL
 GROUP BY namespace, name COLLATE NOCASE HAVING count(*) > 1;

SELECT namespace, group_concat(name, ', ') FROM views
 WHERE dropped_snapshot IS NULL
 GROUP BY namespace, name COLLATE NOCASE HAVING count(*) > 1;

SELECT table_id, parent_field_id, group_concat(name, ', ') FROM columns
 WHERE end_snapshot IS NULL
 GROUP BY table_id, parent_field_id, name COLLATE NOCASE HAVING count(*) > 1;
```

Hoglite, per tenant, a table and a view sharing a name:

```sql
SELECT t.namespace, t.name AS "table", v.name AS view
  FROM tables t
  JOIN views v
    ON v.namespace = t.namespace AND v.name = t.name COLLATE NOCASE
 WHERE t.dropped_snapshot IS NULL AND v.dropped_snapshot IS NULL;
```
