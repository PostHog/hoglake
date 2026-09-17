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
[![Postgres](https://img.shields.io/badge/Postgres-16-4169E1?logo=postgresql&logoColor=white)](server/schema.sql)
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
