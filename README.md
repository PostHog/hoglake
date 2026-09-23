# Hoglake — a lakehouse catalog as a service

[![Server](https://img.shields.io/github/actions/workflow/status/PostHog/hoglake/server.yml?branch=main&label=server)](https://github.com/PostHog/hoglake/actions/workflows/server.yml)
[![Webui](https://img.shields.io/github/actions/workflow/status/PostHog/hoglake/webui.yml?branch=main&label=webui)](https://github.com/PostHog/hoglake/actions/workflows/webui.yml)
[![CI python](https://img.shields.io/github/actions/workflow/status/PostHog/hoglake/ci-python.yml?branch=main&label=python)](https://github.com/PostHog/hoglake/actions/workflows/ci-python.yml)
[![Fuzz nightly](https://img.shields.io/github/actions/workflow/status/PostHog/hoglake/fuzz.yml?branch=main&label=fuzz)](https://github.com/PostHog/hoglake/actions/workflows/fuzz.yml)
[![Semgrep](https://img.shields.io/github/actions/workflow/status/PostHog/hoglake/semgrep.yml?branch=main&label=semgrep)](https://github.com/PostHog/hoglake/actions/workflows/semgrep.yml)
[![PyPI](https://img.shields.io/pypi/v/pyhoglake?label=pyhoglake)](https://pypi.org/project/pyhoglake/)
[![Python](https://img.shields.io/pypi/pyversions/pyhoglake)](https://pypi.org/project/pyhoglake/)
[![Downloads](https://static.pepy.tech/badge/pyhoglake)](https://pepy.tech/project/pyhoglake)
[![Kotlin](https://img.shields.io/badge/kotlin-2.4-7F52FF?logo=kotlin&logoColor=white)](server/build.gradle.kts)
[![JDK](https://img.shields.io/badge/JDK-21-orange?logo=openjdk&logoColor=white)](server/build.gradle.kts)
[![Postgres](https://img.shields.io/badge/Postgres-18-4169E1?logo=postgresql&logoColor=white)](server/schema.sql)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![PRs welcome](https://img.shields.io/badge/PRs-welcome-brightgreen)](https://github.com/PostHog/hoglake/pulls)
[![Last commit](https://img.shields.io/github/last-commit/PostHog/hoglake)](https://github.com/PostHog/hoglake/commits/main)

Petabyte-scale open table format. Snapshots, schema evolution, data files and
statistics live as rows in Postgres; data lives as parquet in object storage.
The catalog is a **service** — clients speak a REST contract and never see the
metadata database.

**Contents:**
[What](#what) | [Quick start](#quick-start) | [Components](#components) | [How a commit works](#how-a-commit-works) | [Compared to DuckLake and Iceberg](#compared-to-ducklake-and-iceberg) | [Performance](#performance) | [Status](#status) | [Development](#development) | [Docs](#docs)

## What

- **Catalog as a service.** All catalog SQL lives server-side behind a REST API. One implementation, one version, one deployment — not a library that every client embeds.
- **Postgres as the only backend.** Foreign keys with `ON DELETE CASCADE`, advisory locks, partial indexes and range deletes are load-bearing design material, not portability casualties.
- **Commits scoped to the write set.** One commit is one snapshot, and its cost does not grow with the size of the catalog.
- **Footer-shipping registration.** Writers upload parquet and send the footer statistics; the server never opens a data file on the commit path. A deferred-stats mode hydrates them asynchronously instead.
- **Server-assigned row lineage.** Contiguous row-id ranges, never reused, under a `table_uuid` incarnation contract. Compaction preserves row ids across a sorted rewrite.
- **Row-level deletes as deletion vectors.** One live vector per data file, growth-monotonic; a stale write is refused with a typed 409 rather than a generic conflict.
- **Maintenance owned by the server.** Compaction, snapshot expiry, physical cleanup and an on-demand invariant scan run as background jobs under advisory locks — not as client procedures racing each other.
- **A changefeed with consumer offsets.** Range-scoped plans of files and deletion vectors. Offsets are catalog state, and expiry respects them instead of stranding readers.
- **Time travel** by snapshot id or timestamp, answering `410` below the retention floor rather than something quietly wrong.
- **Observability built in.** Prometheus `/metrics`, a structured audit log, `GET /v1/info` for instance identity and build, and a web console.
- **Several ways in.** A native Trino connector, a Python client, a DuckDB extension, and an Iceberg REST facade on the design board.

## Quick start

The whole stack in containers — Postgres, MinIO, the server and the console:

```bash
just up
```

The console comes up on <http://localhost:5173> and the API on
<http://localhost:8080>. `just down` stops it; `just down-volumes` also
discards the data.

Then write some rows through the Python client:

```bash
pip install pyhoglake        # or: uv add pyhoglake
```

```python
import pyarrow as pa
from pyhoglake import HoglakeClient, S3Config

client = HoglakeClient(
    "http://localhost:8080",
    s3=S3Config(
        access_key="hoglake",
        secret_key="hoglake123",
        endpoint_override="http://localhost:9000",  # MinIO; omit for AWS
        region="us-east-1",
    ),
)

catalog = client.create_catalog("demo", "s3://my-bucket/demo/")
ns = catalog.create_namespace("analytics")
table = ns.create_table(
    "events",
    pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("name", pa.string())]),
)

result = table.append(pa.table({"id": [1, 2], "name": ["a", None]}))
print(result.snapshot_id)
```

The client writes the parquet itself — field ids embedded in the parquet
schema — extracts the footer statistics, and registers the file in one commit.
Reads are metadata-only planning: `table.files()` returns paths and statistics
and you fetch the data yourself. See [pyhoglake/](pyhoglake/README.md) for
schema evolution, time travel, the changefeed and maintenance calls.

## Components

| Path | What it is |
|---|---|
| [server/](server/README.md) | The control plane: Kotlin, Ktor, JDBI, Flyway |
| [server/trino/](server/trino/README.md) | Integration harness for the native Trino connector kept in [PostHog/trino](https://github.com/PostHog/trino/tree/master/plugin/trino-hoglake) |
| [pyhoglake/](pyhoglake/README.md) | Python client; owns the parquet writer path |
| [webui/](webui/README.md) | Management console: React, Vite, TypeScript |
| [hedgerow/](hedgerow/README.md) | Replication daemon; append-only, single destination |
| [duckdb-client/](duckdb-client/README.md) | DuckDB extension — reads, writes, DML, DDL and time travel over the REST contract |
| [bench/](bench/README.md) | Benchmark harness behind the numbers below |

## How a commit works

```
writer                          hoglake                    Postgres
  │                                │                          │
  ├─ write parquet ──▶ object store│                          │
  │                                │                          │
  ├─ POST /commit ────────────────▶│                          │
  │   paths, row counts,           ├─ validate ──────────────▶│
  │   footer stats, DVs            ├─ take per-catalog lock ─▶│
  │                                ├─ conflict scan ─────────▶│
  │                                ├─ allocate snapshot ─────▶│
  │◀─ 200 {snapshot_id} ───────────┤   write rows             │
  │   or a typed 409               │                          │
```

Appends never conflict with appends. A commit carrying deletes declares the
snapshot it read, and the server refuses it if a conflicting change landed in
between — a typed refusal naming the reason, not a message to pattern-match.
The serialization point is a per-catalog advisory lock held for milliseconds.

## Types

The column vocabulary is closed: 27 wire names, enforced identically by the `ColType` enum, the `hog_column.col_type` CHECK and the OpenAPI `ColumnDef.type` enum, which `ScalarTypeParityTest` asserts against each other from their actual files. Bounds are stored in the Iceberg single-value serialization of the **mapped** Iceberg type, never of the hoglake type — that is what keeps manifest generation a mechanical copy. Depth in [docs/iceberg-federation.md](docs/iceberg-federation.md) §2; this is the card, in `ColType` order, with adjacent names sharing a row when they share a mapping.

| `col_type` | Iceberg | bounds |
|---|---|---|
| `boolean` | boolean | 1 byte |
| `int8` `int16` `int` | int | 4-byte LE int |
| `long` | long | 8-byte LE long |
| `uint8` `uint16` | int | 4-byte LE int |
| `uint32` | long | 8-byte LE long |
| `uint64` | decimal(20,0) | minimal two's-complement BE unscaled |
| `float` | float | 4-byte LE IEEE-754 |
| `double` | double | 8-byte LE IEEE-754 |
| `decimal` | decimal(p,s) | minimal two's-complement BE unscaled |
| `date` | date | 4-byte LE epoch days |
| `time` | time | 8-byte LE micros since midnight |
| `timestamp_s` `timestamp_ms` `timestamp` | timestamp | 8-byte LE micros |
| `timestamp_ns` | timestamp_ns (V3) | 8-byte LE nanos |
| `timestamptz` | timestamptz | 8-byte LE micros |
| `string` `json` | string | UTF-8 bytes |
| `uuid` | uuid | 16 bytes BE |
| `binary` | binary | the bytes |
| `variant` | variant (V3) | none |
| `list` `struct` `map` | list / struct / map | none — per leaf |

- **`uint64` maps to decimal(20,0)**, the narrowest Iceberg type holding [0, 2^64). Its bounds are facade-shaped already; its *files* are the one kind a facade cannot serve, because parquet cannot express an INT64 column as an Iceberg decimal(20,0).
- **`timestamp_s` and `timestamp_ms` are declarations, not file units.** Parquet has no seconds unit, so those files hold millis; the parquet annotation is authoritative for what a file's int64s mean, and bounds are micros for all three precisions.
- **`json` is string bytes** — same encoding, same comparison — and takes identity partitioning only.
- **Signed zeros are canonical.** A `float`/`double` lower bound stores `-0.0` and an upper bound `+0.0`. The two are IEEE-equal, but Iceberg's evaluators compare in natural order, where `-0.0 < 0.0`, so the pair (lower `+0.0`, upper `-0.0`) would read as an empty range and prune away a file that holds `0.0`. Every door that stores a bound canonicalizes; the cases are pinned cross-language in `pyhoglake/tests/vectors/bounds_vectors.json`.

**`variant`** is a catalog scalar — one node, one field id, no children — whose parquet storage is nonetheless a group: `metadata` (required) plus `value` and/or `typed_value`, addressed by name, as the variant specification mandates. The field id sits on the group and nothing below it needs one. It has no single-value encoding, so no scalar bounds, no partition transform, no sort-key contract and no promotion in either direction; shredded child statistics are not whole-column statistics and are omitted. Compaction skips a table holding one at any depth, and the rewriter refuses it explicitly. A variant inside a `list`, `struct` or `map` is expressible and means exactly what a top-level one means.

**Containers** are native and one-for-one with Iceberg, element/key/value field ids included. A `list` has exactly one child named `element`; a `map` has exactly two, `key` then `value`, and the key is required; a `struct` has one or more children keeping the user's names. Nesting depth is capped at 8 with a top-level column counting as 1, measured from the graft point — adding a 3-deep struct into a 6-deep one is refused exactly like declaring a 9-deep column — and every DDL path also caps a table at 10,000 column nodes. Statistics are per leaf: a container carries no values and gets no stats row, and a commit shipping one for a container field id is refused by name, while a list's `element`, a map's `key`/`value` and a struct leaf all get counts and bounds like any scalar. A struct leaf is a legal partition or sort source; the container itself and anything beneath a `list` or `map` are refused, each naming which of the two applies.

**Permanent refusals**, each a named 422 rather than "unknown type": `int128` and `uint128` need 39 decimal digits and exceed Iceberg's widest exact numeric, decimal(38); `timetz` and `interval` have no Iceberg mapping; the DuckLake geometry family (`point`, `linestring`, `polygon`, `multipoint`, `multilinestring`, `multipolygon`, `linestring_z`, `geometrycollection`) is out of scope. None of them is a "not yet" — there is no facade story to write.

**Promotion** is the intersection of two sets hoglake does not own: DuckLake's documented promotion table, and Iceberg schema-evolution legality of the induced facade change. That leaves the signed ladder (`int8`→`int16`/`int`/`long`, `int16`→`int`/`long`, `int`→`long`), the unsigned ladder up to `uint32`, and `float`→`double`. Two exclusions are deliberate and cut in opposite directions: anything→`uint64` is DuckLake-legal, but `uint64` maps to decimal(20,0) and int/long→decimal is not an Iceberg evolution; while `uint8`→`int` and `timestamp_s`→`timestamp_ms` lose nothing at all and are absent from DuckLake's table, and a hoglake catalog must never accept DDL a DuckLake client would reject. Containers never promote, in either direction; a struct leaf promotes by the ordinary matrix, since promotion is keyed on field id.

**Transforms** are `identity`, `bucket(n)` (Murmur3, bit-compatible with Iceberg) and `year`/`month`/`day`/`hour`. Bucket sources are a positive allowlist — `int8`, `int16`, `int`, `long`, `uint8`, `uint16`, `decimal`, `date`, `time`, `timestamp`, `timestamptz`, `string`, `uuid`, `binary` — so a new column type is not bucketable until someone decides how it hashes. `boolean`, `float` and `double` are outside the specification's Appendix-B hash domain; `json` has no canonical byte form, so equal documents with different bytes would scatter across partitions; and `uint32`, `uint64`, `timestamp_s`, `timestamp_ms` and `timestamp_ns` are a hash-domain mismatch, because Appendix B hashes the mapped type's representation while the client computes bucket values and the server only accepts the strings it is sent — admitting these needs a client hashing contract and cross-language vectors first. The temporal transforms take `date` and every timestamp precision. pyhoglake's allowlist is pinned set-equal to the server's by a test that parses the Kotlin.

## Compared to DuckLake and Iceberg

All three share a shape — immutable data files plus versioned metadata — and
differ in where the metadata lives and who is allowed to change it.

| | Hoglake | DuckLake | Iceberg |
|---|---|---|---|
| **Metadata lives in** | Postgres, behind a service | An RDBMS, read directly by clients | Files in object storage, plus a catalog pointer |
| **Who commits** | The server | Every client, through an embedded extension | Every client, through a catalog pointer swap |
| **Metadata read cost** | Indexed SQL, scoped to the write set | SQL, but the commit path loads catalog-wide statistics | Fetch `metadata.json`, the manifest list, then manifests |
| **Engine support** | Trino connector, DuckDB extension, Python; Iceberg facade designed | DuckDB | Nearly universal — the reason to choose it |
| **Extra infrastructure** | A service and a Postgres | An RDBMS | None beyond the object store |
| **Row-level deletes** | Deletion vectors, one live per file | Positional delete files | Positional and equality deletes; vectors in v3 |
| **Row lineage** | Server-assigned, never reused, preserved by compaction | Row ids reused on recreate; sorted compaction remaps them | `_row_id` in v3 |
| **Maintenance** | Server background jobs under advisory locks | Client-invoked procedures, unlocked | Engine-specific actions you schedule |
| **Consumer offsets** | Catalog state; expiry respects them | Not a concept | Not a concept |
| **Integrity** | Foreign keys, CASCADE, CHECK vocabularies, partial unique indexes | Five primary keys; no foreign keys or indexes | Whatever the catalog implementation enforces |

The trade is plain. Iceberg buys ubiquity: every engine already reads it and
there is no service to operate. Hoglake buys metadata operations that are
indexed SQL rather than object-store round trips, and one place where commit,
compaction and expiry are implemented. DuckLake shares hoglake's
RDBMS-metadata idea but keeps the catalog inside a client library, which is
the part hoglake changes.

Hoglake is not DuckLake-compatible on the wire, in SQL, or in metadata, and
does not aim to be. For the empirical record behind these choices see
[docs/ducklake-defect-ledger.md](docs/ducklake-defect-ledger.md); for what the
Iceberg facade requires, [docs/iceberg-federation.md](docs/iceberg-federation.md).

## Performance

Benchmarked rather than asserted — `bench/` runs these as regression guards
(`just bench quick`, results journalled to JSONL). Figures come from
`all --quick` on a development laptop against the local compose stack, so read
them as shape rather than capacity planning.

| Guard | Measured |
|---|---|
| Commit latency vs catalog size | Flat — p50 ratio ~1.0 with 10K preseeded snapshots, and with 150–500 live tables |
| Snapshot expiry | 14,462 snapshots/s; 14,885 files queued/s |
| Concurrent appends | 0 conflicts at 1–8 writers; aggregate plateau ~380 commits/s at the serialized tail |
| Delete races | 0 lost updates under deliberate hot-file contention; retry to success p50 5.6 ms |
| Changefeed reads | Latency tracks rows returned (r=0.998), not catalog size |
| DDL churn | Post-churn commit p50 ratio 0.64 — no residual tax |
| Writer path, real parquet | 202K rows/s through `table.append` |
| Physical cleanup | Fully liveness-checked, ~900 paths/s drained |

Every scenario asserts catalog invariants after load — dense snapshot ids, row
range tiling, deletion-vector accounting, `still_referenced == 0` — and fails
loudly rather than reporting numbers it cannot stand behind.

## Status

A feature-complete control plane, with a Python client, a replication daemon, a
management console, a native Trino connector, a DuckDB extension and a
benchmark harness. Everything is tested; nothing is deployed, authenticated, or
has touched production data. That last sentence is the roadmap.

Known gaps are tracked as [issues](https://github.com/PostHog/hoglake/issues).
The larger ones: authentication and tenancy, the Iceberg REST facade (designed,
unbuilt), a converter from the predecessor's catalogs, and a TLA model check of
the commit protocol.

## Development

```bash
just test-all      # the server suite, then the client suites
just lint-all      # ktlint, ruff check and format, mypy
just dev           # the dev stack plus the server in the foreground
just --list        # everything, including the per-component recipes
```

Requires JDK 21, Docker (the server suite runs Postgres and MinIO through
Testcontainers), [uv](https://docs.astral.sh/uv/) for the Python trees, Node
for the console, and [just](https://github.com/casey/just). Each component
carries its own recipes — `just server test`, `just pyhoglake unit`,
`just webui test`.

[AGENT.md](AGENT.md) holds the pre-push checklist, the repository invariants
and the working conventions.

The wire contract is
[`openapi/hoglake.yaml`](server/src/main/resources/openapi/hoglake.yaml), and
it is the single source of truth: server routes, the Python client, the console
fixtures and hedgerow all conform to it. Change the specification and the
implementations together.

## Docs

| Doc | What |
|---|---|
| [docs/iceberg-federation.md](docs/iceberg-federation.md) | What the Iceberg REST facade requires of v1: field ids, type mapping, transforms, delete encoding, bounds |
| [docs/metadata-schema.md](docs/metadata-schema.md) | The `hog_*` schema inventory and its invariants, mirroring [server/schema.sql](server/schema.sql) |
| [docs/trino-integration.md](docs/trino-integration.md) | The native connector, and the commit shapes that keep Trino writes a translation away |
| [docs/operational-notes.md](docs/operational-notes.md) | What hoglake changes operationally and what it does not; sizing, and the ceilings to watch |
| [docs/fuzzing.md](docs/fuzzing.md) | Property testing and fuzzing: hypothesis, kotest-property, cross-language codec vectors |
| [docs/ducklake-defect-ledger.md](docs/ducklake-defect-ledger.md) | The predecessor's production defects, and where hoglake answers each |
| [duckdb-client/DESIGN.md](duckdb-client/DESIGN.md) | The DuckDB extension as built, including its findings against the wire contract |

## License

[MIT](LICENSE)
