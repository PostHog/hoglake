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

### 2.5 Promotions are constrained twice

A promotion is legal iff it is DuckLake-legal **and** the induced
Iceberg schema evolution is legal (same mapped type, or Iceberg's own
int→long / float→double / decimal-precision widenings). Two places
where the second half makes hoglake stricter than DuckLake:

- `uint32 → uint64` is refused: long → decimal(20,0) is not an Iceberg
  evolution, so the promotion would make the table unservable.
- `timestamp → timestamp_ns` is refused: the mapped type changes, and
  the stored bound unit would change from micros to nanos underneath
  every existing stats row.

`json` and `string` share a mapped type but neither promotes to the
other — json carries a validity claim string does not.

Because bounds are keyed to the *mapped* type, a promotion only needs
its stats re-encoded when the mapped type changes. The whole
int8→int16→int ladder, `uint32 → long`, and
`timestamp_s → timestamp_ms → timestamp` are all metadata-only.

### 2.6 Partition transforms over the new types

`year`/`month`/`day`/`hour` accept every timestamp precision (the
transforms are epoch-relative integers; declared precision does not
change them). `bucket(n)` accepts everything except the Iceberg-excluded
`boolean`/`float`/`double` — **and except `json`**. That last one is a
deliberate choice, not an oversight: bucketing hashes bytes, and two
documents that are equal as JSON (key order, whitespace, number
spelling) have different bytes, so a json bucket spec would scatter
equal values across partitions and prune wrong. `json` supports
`identity` only, which is at least honestly opaque byte equality. The
same reasoning would exclude `truncate` when the server grows it.

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
`variant` — tracking V3 for `timestamp_ns` does not commit us to it),
and nested types generally, which the flat model does not admit yet.

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
