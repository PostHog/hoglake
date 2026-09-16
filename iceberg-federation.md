# Iceberg federation — design obligations for v1

What the initial hoglake design must get right so the read-only Iceberg
REST facade ([README.md](README.md), Decisions) works — and keeps
working — as a federation participant. The theme: *mappability is a
property of the data model and the files, not of the facade code*, and
several pieces are unretrofittable once a lake exists.

Companions: [README.md](README.md) (architecture + decisions),
[trino-integration.md](trino-integration.md) (the first consumer of
this facade), [metadata-schema.md](metadata-schema.md) (what today's
model looks like).

## 1. Field IDs in the parquet files (unretrofittable)

Iceberg resolves projection by **field ID embedded in the parquet
schema** (`PARQUET:field_id` on each schema element), not by column
name. The hoglake writer contract must mandate embedding the catalog's
column IDs in every file from the first byte written; a lake without
them can only be served by name-mapping shims, and fixing it later
means rewriting every file.

This also formally kills the pyducklake field-ID fiction (sequential
IDs synthesized client-side from `ordinal_position` — see
[pyducklake-api-map.md](pyducklake-api-map.md)): the catalog's column
IDs are the one identity, in the metadata and in the files.

Registration-time enforcement is cheap: the footer the writer ships
already contains the schema; the commit endpoint verifies field IDs are
present and match the table schema. The async hydrator re-verifies for
deferred-stats registrations.

## 2. Type discipline

The rule: **a type may not be used in a hoglake table unless its facade
story is defined** — the decision happens at DDL time, not at read time
in someone else's engine. There is no `unmappable` marker and there
never will be: a name either has a row in the table below, or it is
refused by the API with a reason.

That rule is enforced in three places that must agree — the `ColType`
enum, the `hog_column.col_type` CHECK, and the OpenAPI `ColumnDef.type`
enum — and `ScalarTypeParityTest` asserts the three against each other
from their actual files.

### 2.1 The mapping table

`col_type` is the hoglake wire name; *Iceberg* is what the facade
presents; *parquet* is what files hold; *bounds* is what
`hog_file_column_stats.lower_bound` / `upper_bound` contain.

**The bounds invariant**: bounds are stored in the Iceberg
single-value serialization of the **mapped Iceberg type**, never of the
hoglake type. That is what keeps manifest generation a mechanical copy
(§5), and it is why several hoglake types share one encoding.

| col_type | Iceberg | parquet physical | bounds |
|---|---|---|---|
| `boolean` | boolean | BOOLEAN | 1 byte |
| `int8` | int | INT32 + INT(8, signed) | 4-byte LE int |
| `int16` | int | INT32 + INT(16, signed) | 4-byte LE int |
| `int` | int | INT32 | 4-byte LE int |
| `long` | long | INT64 | 8-byte LE long |
| `uint8` | int | INT32 + INT(8, unsigned) | 4-byte LE int |
| `uint16` | int | INT32 + INT(16, unsigned) | 4-byte LE int |
| `uint32` | long | INT64 (see 2.2) | 8-byte LE long |
| `uint64` | decimal(20,0) | INT64 + INT(64, unsigned) | minimal two's-complement BE unscaled |
| `float` | float | FLOAT | 4-byte LE IEEE-754 |
| `double` | double | DOUBLE | 8-byte LE IEEE-754 |
| `decimal` | decimal(p,s) | BINARY + DECIMAL(p,s) | minimal two's-complement BE unscaled |
| `date` | date | INT32 + DATE | 4-byte LE int (epoch days) |
| `time` | time | INT64 + TIME(MICROS) | 8-byte LE long (micros since midnight) |
| `timestamp_s` | timestamp | INT64 + TIMESTAMP(MILLIS) (see 2.3) | 8-byte LE long, **micros** |
| `timestamp_ms` | timestamp | INT64 + TIMESTAMP(MILLIS) | 8-byte LE long, **micros** |
| `timestamp` | timestamp | INT64 + TIMESTAMP(MICROS) | 8-byte LE long, micros |
| `timestamp_ns` | timestamp_ns (V3) | INT64 + TIMESTAMP(NANOS) | 8-byte LE long, **nanos** |
| `timestamptz` | timestamptz | INT64 + TIMESTAMP(MICROS, UTC) | 8-byte LE long, micros |
| `string` | string | BYTE_ARRAY + STRING | UTF-8 bytes |
| `json` | string | BYTE_ARRAY + JSON | UTF-8 bytes |
| `uuid` | uuid | FIXED_LEN_BYTE_ARRAY(16) + UUID | 16 bytes BE |
| `binary` | binary | BYTE_ARRAY | the bytes |

Three rows carry consequences worth stating out loud.

### 2.2 `uint32` is written as INT64, deliberately

pyarrow and DuckDB both emit `uint32` as parquet INT32 +
INT(32, unsigned). An Iceberg reader resolving that column as a `long`
reads the INT32 as **signed**, so every value above 2^31 comes back
negative. hoglake's writer contract is therefore parquet **INT64**: the
pyhoglake writer casts, and compaction rewrites converge existing files
onto it. The read paths (footer stats, compaction input) still accept
the INT32 + unsigned form from foreign writers and zero-extend it.

The INT32 form is only accepted **with** its unsigned annotation.
Without it, parquet computed the chunk's min/max in signed order, so
reinterpreting those bounds as unsigned would invert the range — the
bound is dropped instead (the "NULL, never guessed" rule). Same
argument, at 2^63, for `uint64`.

### 2.3 `timestamp_s` files are physically millis

Parquet has no seconds timestamp unit. pyarrow coerces `timestamp[s]`
to TIMESTAMP(MILLIS) on write (verified against pyarrow 25), and
DuckDB does the same. So a `timestamp_s` column's files hold millis,
and hoglake never infers a file's unit from the catalog type: **the
parquet annotation is authoritative for what a file's int64s mean**,
and the catalog type records what the column was declared as. The
footer-stats path is unit-driven for exactly this reason.

### 2.4 `uint64` is the one type whose FILES are not facade-readable

Its bounds are already facade-shaped (decimal(20,0) unscaled bytes),
but parquet cannot express an INT64 column as an Iceberg
decimal(20,0) — that needs FIXED_LEN_BYTE_ARRAY. A facade serving a
`uint64` column must therefore rewrite the files, not just the
metadata. Recorded here rather than solved: the facade does not exist
yet, and `uint64` columns are rare enough that pinning the honest
mapping now beats inventing a physical format no writer emits.

### 2.5 Promotions are an intersection, not a judgement call

A promotion is offered iff it appears in **both**:

1. **DuckLake's documented promotion table**
   ([schema evolution](https://ducklake.select/docs/stable/duckdb/usage/schema_evolution)
   — "Only type promotions are supported. Type promotions must be
   lossless"): `int8 → int16/int32/int64`, `int16 → int32/int64`,
   `int32 → int64`, `uint8 → uint16/uint32/uint64`,
   `uint16 → uint32/uint64`, `uint32 → uint64`, `float32 → float64`.
2. **Iceberg schema-evolution legality** of the induced facade change:
   same mapped type, or `int → long`, `float → double`, decimal
   precision widening.

The result:

| from | to |
|---|---|
| `int8` | `int16`, `int`, `long` |
| `int16` | `int`, `long` |
| `int` | `long` |
| `uint8` | `uint16`, `uint32` |
| `uint16` | `uint32` |
| `float` | `double` |

Being an intersection cuts both ways, and both directions have bitten:

- **Value-preserving is not sufficient.** `uint8 → int`,
  `uint16 → long` and the whole
  `timestamp_s → timestamp_ms → timestamp` ladder are lossless — the
  timestamp one is pure metadata, since all three store micros bounds —
  but DuckLake offers none of them. A hoglake catalog that accepts DDL
  a DuckLake client rejects is a catalog the two disagree about, so
  they are refused.
- **DuckLake-legal is not sufficient either.** Every `→ uint64` rung is
  in DuckLake's table, but `uint64` maps to `decimal(20,0)`, and
  `int → decimal` / `long → decimal` are not Iceberg evolutions. Taking
  them would silently make the lake unservable through the facade.

Because bounds are keyed to the *mapped* type (§2.1), a promotion needs
its stats re-encoded only when that mapped type changes. The
`int8 → int16 → int` steps and `uint8 → uint16` are metadata-only;
`* → long` and `uint8/uint16 → uint32` widen 4-byte bounds to 8.

`json` and `string` share a mapped type but neither promotes to the
other — json carries a validity claim string does not — and nothing
promotes into `timestamp_ns`, which is its own Iceberg type with its
own bound unit.

### 2.6 Partition transforms over the new types

`year`/`month`/`day`/`hour` accept every timestamp precision (the
transforms are epoch-relative integers; declared precision does not
change them).

`bucket(n)` is the narrow one, and it has **three** exclusion reasons:

- `boolean`/`float`/`double` — outside the Iceberg spec's Appendix-B
  hash domain outright.
- `json` — bucketing hashes BYTES, and two documents equal as JSON (key
  order, whitespace, number spelling) have different bytes, so a json
  bucket spec would scatter equal values across partitions and prune
  wrong. The same reasoning excludes `truncate` when the server grows
  it.
- `uint32`, `uint64`, `timestamp_s`, `timestamp_ms`, `timestamp_ns` —
  the **hash-domain mismatch**. Appendix B hashes the *mapped* type's
  representation: timestamps as micros, `uint64`-as-`decimal(20,0)` as
  minimal two's-complement bytes, `uint32`-as-`long` as the
  zero-extended value. The server never computes a bucket value — it
  stores the partition strings clients send — so accepting a bucket
  spec on these would be accepting values nobody has verified. They are
  identity/truncate-only until there is a client contract for hashing
  on the mapped value AND cross-language bucket vectors proving both
  sides agree. Re-admitting them is a deliberate change with those
  vectors attached, not a default.

The server's `AlterService.BUCKETABLE_TYPES` and pyhoglake's
`_BUCKETABLE` must stay exactly equal (there is a test): the client is
where bucket values are actually computed, so a divergence would mean
the server accepting a spec the writer cannot honour. The 422 for the
five hash-domain types names that reason specifically rather than
saying "not bucketable", because the two are different problems.

### 2.7 Permanently refused names

These are refused at the API with a 422 that **names the type and the
reason** — never a generic unknown-type error, because "stop trying" and
"check your spelling" are different answers:

| name(s) | why |
|---|---|
| `int128`, `uint128` | 39 decimal digits; Iceberg's widest exact numeric is decimal(38) |
| `timetz` | no Iceberg time-with-timezone type |
| `interval` | no Iceberg interval type |
| `point`, `linestring`, `polygon`, `multipoint`, `multilinestring`, `multipolygon`, `linestring_z`, `geometrycollection` | DuckLake geometry; out of scope |

Still open from the DuckLake type system: VARIANT (Iceberg V3 has
`variant` — tracking V3 for `timestamp_ns` does not commit us to it).

### 2.8 Nested types: `list`, `struct`, `map`

| col_type | Iceberg | parquet | bounds |
|---|---|---|---|
| `list` | list | `optional group x (LIST) { repeated group list { <t> element } }` | **none** |
| `struct` | struct | `optional group x { <fields> }` | **none** |
| `map` | map | `optional group x (MAP) { repeated group key_value { required <k> key; <v> value } }` | **none** |

The mapping is native and one for one, element/key/value **field ids
included**: an Iceberg facade presents these columns without converting
anything or synthesizing an id.

**The column model is a tree.** `hog_column` gained
`parent_field_id` (V7) — a same-table reference by
`(catalog_id, table_id, field_id)` identity, deliberately **not** a
foreign key, because these rows are versioned and an FK would have to
name one *version* of a parent that a rename or a promote immediately
retires. Field ids are stable across versions; versions are not.
`ordinal` orders **siblings**, so the live-ordinal unique index is
per-parent (`NULLS NOT DISTINCT`, so top-level columns — whose parent is
NULL — keep the guarantee they always had). Ids are assigned
**depth-first**, parent before children, which keeps a subtree's ids
contiguous.

**Shape rules are Iceberg's, and each has its own named 422.** A `list`
has exactly one child, named `element`, whose nullability is declarable
(Iceberg's `element-required`). A `map` has exactly two, `key` then
`value`, and the key is **required** — Iceberg map keys are
non-nullable and the parquet MAP shape says so too. A `struct` has one
or more children, which keep the user's names. A scalar has none.
Nesting depth is capped at **8**, counting a top-level column as 1 — not
a physical limit (parquet and Iceberg have none) but a blast-radius one:
every level multiplies field ids, definition/repetition levels, and the
recursion every surface performs per row.

**Bounds are per LEAF.** A container carries no values, so it gets no
`hog_file_column_stats` row at all, and a commit shipping
`column_stats` for a container's field id is refused by name (the server
never sees the file, so this is the only place it can be caught). A
list's `element` and a map's `key`/`value` DO get counts and bounds —
that is Iceberg's own rule for nested fields — and a struct leaf behaves
exactly like a top-level scalar, same encoding, same width. `value_count`
for a repeated leaf is the number of VALUES, not of rows.

**Partition and sort sources must be leaves with no repeated ancestor.**
Iceberg's `source-id` may point at a struct's leaf field, so `addr.zip`
is a legal source and a struct leaf of a bucketable scalar type is
bucketable — that is a property of the leaf, and §2.6's allowlist is
untouched. A container itself is refused (no single value per row), and
so is anything under a `list` or a `map` (many values per row). Both
refusals name which of the two applies.

**Promotion: containers never, struct leaves by the ordinary matrix.**
Promotion is keyed on field id, so a struct leaf promotes and re-encodes
its bounds exactly like a top-level column; §2.5's intersection applies
unchanged. A container has no promotion at all, in either direction, and
says so rather than reporting that this particular target was wrong.

**The alter matrix for nested columns**, by dotted path (`addr.zip`;
names cannot contain `.`, so the path is unambiguous):

| op | struct interior | list / map interior |
|---|---|---|
| `add_column` (with `parent`) | ✅ new field id, appended ordinal | ❌ 422 |
| `drop_column` | ✅ (not the last field; takes its subtree) | ❌ 422 |
| `rename_column` | ✅ | ❌ 422 |
| `promote_column` | ✅ scalar matrix on the leaf | ❌ 422 |
| anything on the container itself | drop/rename ✅, promote ❌ | drop/rename ✅, promote ❌ |

There is no op for a list's element or a map's key/value because Iceberg
has none: the container's shape is part of its type, and changing it
would be a type change, not a column op.

**Compaction rewrites nested columns; it does not refuse them.**
parquet-java's Group API is already a tree, so the existing
plan-and-copy pipeline extends one level at a time (the plan becomes a
tree of steps; the copy recurses through `addGroup`/`getGroup`). The
alternative — making a nested schema `unconvertible_schema` — was
cheaper and permanently wrong: a table with one `map` column could then
never be compacted, and its small-file debt would grow forever with no
operator lever. The costs are worth stating precisely, because one of them is much
larger than it looks. The **unsorted** path holds exactly one record at
a time, so its heap is one row's object graph — bounded by the widest
row, and nothing bounds a row (a million-element list is a million
`SimpleGroup`s at once). The **sorted** path materializes the whole
group to sort it, and a nested group's object graph is **not** its byte
size: a measured `list<long>` table with five elements per row peaked at
343 MiB of heap from a 4.6 MiB compressed input — 70x — because every
element carries an object header, a field array and a boxed value, none
of which compression touches. `compaction_target_bytes` is therefore not
a heap bound for such a table, and the planner derates instead: a table
with BOTH nested columns and a live sort order is planned under
`target / HOGLAKE_COMPACTION_NESTED_SORT_EXPANSION` (default 64, near
the top of the measured 30-70x range), which puts the materialized graph
back under roughly the target. That is a bounded mitigation per GROUP,
not spilling, and not a per-ROW bound.
Inputs are matched by SHAPE, not by the synthetic group names (the
parquet spec says those are insignificant), but a shape that disagrees
with the live column — a struct over a primitive, a 2-level legacy list,
an optional map key — is `unconvertible_schema`, never a guess.

Still open: VARIANT, and `list`/`map` internals as partition sources
(Iceberg does not define them either).

## 3. Iceberg-identical partition transforms only

Facade pruning works only if partition specs translate exactly.

- `bucket(n)`: our `murmur3_32` is already bit-compatible with
  Iceberg's hash by design ([ducklake-api-map.md](ducklake-api-map.md)
  §1.2) — keep it that way, and fix the nested-type
  string-representation fallback rather than inheriting it.
- Date/time transforms: Iceberg defines `year/month/day/hour` as
  epoch-relative integers. DuckLake grew both calendar-flavored and
  `epoch_*` variants; hoglake supports **only the Iceberg-semantics
  set**. One definition, one name, no aliases.
- `identity`, `truncate(w)`: direct mappings; adopt Iceberg's
  truncation semantics verbatim if/when we add truncate.

## 4. One delete encoding, and it's one Iceberg understands

A table with live deletes is only servable through the facade if the
deletes are expressible as Iceberg V2 position-delete files or V3
deletion vectors (puffin). The fork already half-supports puffin DVs.
Position: **deletion vectors become THE hoglake delete representation**
— internal readers and the facade share one encoding, and we skip
carrying DuckLake's bespoke positional-parquet delete format into the
new world. This also pins the facade's Iceberg format-version target
(V3, with V2 position-delete generation as a fallback only if a
consumer forces it).

## 5. Stats bounds stored typed, not as text

Manifest entries carry `lower_bounds`/`upper_bounds` in Iceberg's
single-value binary serialization. DuckLake stores min/max as VARCHAR
text, which makes bound conversion a per-type parsing adventure with
ambiguity (float formatting, timestamp zones, binary values). Hoglake
stores bounds in a typed/binary form chosen so manifest generation is a
mechanical re-encode. Same rule for `value_counts`/`null_value_counts`
/`nan_value_counts` — they map 1:1 from the footer-shipped stats, so
the registration API's stats shape should be Iceberg-`Metrics`-shaped
from day one (see [trino-integration.md](trino-integration.md) §2 for
why this pays twice).

## 6. Metadata-artifact generation and bucket layout

The REST facade ultimately hands readers real objects: a
`metadata.json`, manifest lists, and manifest files (avro) in object
storage. Two v1 obligations:

- **Bucket layout reserves the space now**: a `metadata/` prefix per
  table alongside `data/`, so facade artifacts have a home that
  credential vending can scope to.
- **The manifest generator is a service component**: lazy per-snapshot
  generation with caching (a snapshot's manifests are immutable once
  written — generate once, serve forever, GC with snapshot expiry).
  Incremental generation falls out of the snapshot model: a new
  snapshot's manifest list reuses the previous manifests for untouched
  files, exactly like Iceberg's own writers do.

## 7. Identity and snapshot mapping (mostly free, keep it that way)

- `table_uuid` ↔ Iceberg `table-uuid`: already in the model; the
  changefeed carries it (README key moves) — the facade uses the same
  one.
- hoglake snapshot ↔ Iceberg snapshot is 1:1; the snapshot's
  change-summary must be mappable to Iceberg's snapshot `operation`
  (`append` / `overwrite` / `delete` / `replace`) — a constraint on
  how we encode the typed `snapshot_change` rows, cheap if considered
  now.
- Namespaces: hoglake schemas map to single-level Iceberg REST
  namespaces; don't invent nesting we'd have to flatten.

## 8. What stays out of scope (and why that's safe)

Write-path Iceberg compatibility remains out (Decisions) — but see
[trino-integration.md](trino-integration.md) for the commit-shape
choice that keeps an append-only write adapter a *translation* rather
than a redesign. Facade freshness bounds (relevant only if
Arrow-inline data files ever land) are covered in the README's
inlining decision.

## Checklist form (for the phase-2 spec)

| Obligation | Cost if skipped | When it must land |
|---|---|---|
| Field IDs in files | Lake rewrite | First file written |
| Per-type facade story | Read-time errors in foreign engines | DDL validation, v1 |
| Iceberg-exact transforms | No pruning through facade | Partition spec DDL, v1 |
| DV-only deletes | Delete-bearing tables unservable | First delete support |
| Typed stats bounds | Lossy/ambiguous manifests | Registration API, v1 |
| `metadata/` prefix + generator | Facade rework + layout migration | Bucket layout, v1 |
| Operation-mappable snapshot changes | Facade guesses `operation` | snapshot_change encoding, v1 |
