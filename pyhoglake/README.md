# pyhoglake

Python client for [hoglake](https://github.com/PostHog/hoglake#readme), the Postgres-native
lakehouse-catalog control plane. A **thin API wrapper**: no direct
catalog-database access. The standard writer path writes Parquet to
object storage and registers it with the control plane via footer-shipping
commits. The optional packed MergeTree adapter invokes a configured
`clickhouse local` executable; ClickHouse remains outside the library.

## Install

```sh
pip install pyhoglake                  # or: uv add pyhoglake
pip install 'pyhoglake[fast-upload]'   # + boto3, for single-request uploads
```

Dependencies: `httpx`, `pyarrow`, `thrift`. The `fast-upload` extra
adds `boto3`, which the writer path uses to put a small parquet object
in ONE request (see [Prepared
appends](#prepared-appends--buffered-encode-one-request-uploads-one-commit));
without it every object takes pyarrow's three-request multipart write,
and pyhoglake logs one warning saying so. Development uses the flox env
in this directory:

```sh
flox activate -- uv sync
flox activate -- uv run pytest                      # unit + integration
flox activate -- uv run pytest -m "not integration" # unit only
```

Integration tests need a live server (`HOGLAKE_URL`, default
`http://localhost:8080`) and S3 credentials (`HOGLAKE_S3_ENDPOINT`,
`HOGLAKE_S3_ACCESS_KEY`, `HOGLAKE_S3_SECRET_KEY`); they skip cleanly
when the server is unreachable.

## Quickstart — the append path end to end

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
    pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string()),
            pa.field("amount", pa.decimal128(10, 2)),
        ]
    ),
)

# THE writer path: writes one parquet file (with catalog field ids
# embedded in the parquet schema) to
#   s3://my-bucket/demo/data/analytics/events/<uuid>.parquet
# extracts per-column footer stats (value/null counts, Iceberg
# single-value binary min/max bounds), and registers the file in one
# commit. The server never opens the file.
result = table.append(
    pa.table({"id": [1, 2], "name": ["a", None], "amount": [None, None]}),
    author="me",
    message="first batch",
)
print(result.snapshot_id)

# reads are metadata-only planning; you fetch the parquet yourself
for f in table.files():
    print(f.path, f.record_count, f.stats_state, f.row_id_start)

# changefeed + consumer offsets
plan = table.changes(from_snapshot=0)
catalog.commit_offset("my-consumer", table.table_uuid, plan.to_snapshot)
catalog.offset("my-consumer", table.table_uuid)  # one offset; None if unset
```

More surface:

```python
from pyhoglake import ops

table.alter([ops.add_column("score", pa.float64())])  # schema evolution
table.info(snapshot=5)  # time travel
table.files(at_timestamp=some_datetime)  # by timestamp
table.append(big_table, deferred_stats=True)  # register as pending
catalog.set_retention(7 * 86400, consumer_floor=True)  # retention policy
catalog.expire()
catalog.cleanup()  # maintenance sweeps
ns.create_view("v", "SELECT 1", dialect="trino")
for s in catalog.snapshots(limit=1000):
    ...  # auto-paginated
for s in catalog.snapshots(before=head + 1):
    ...  # descending walk
    # (mutually exclusive
    # with non-zero after)
```

## Packed MergeTree adapter

`ClickHousePackedAdapter` is the explicit writer and reader for tables created with
`write.format.default=clickhouse-mergetree-packed`. The server must first complete the rollout
sequence in `server/README.md` and enable `HOGLAKE_PACKED_MERGETREE_ENABLED=true`:

```python
import pyarrow as pa
from pyhoglake import ClickHousePackedAdapter

packed = ns.create_table(
    "packed_events",
    pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string()),
        ]
    ),
    properties={"write.format.default": "clickhouse-mergetree-packed"},
)
adapter = ClickHousePackedAdapter("/usr/local/bin/clickhouse")
adapter.append(packed, pa.table({"id": [1, 2], "name": ["a", "b"]}))
rows = adapter.read(packed, snapshot=catalog.refresh().head_snapshot_id)
```

For durable publication, call `prepare_append`, persist the returned JSON payload, then call
`commit_prepared` with that exact payload. Do not call `prepare_append` again with the same
idempotency key; replay `commit_prepared` with the persisted payload. Preparation creates one
MergeTree part in an isolated local directory, refuses any layout except one `data.packed` file,
claims a fresh `.packed` object
path from Hoglake, uploads it, and includes Arrow-derived Iceberg bounds in the registration.
`abandon_prepared` fences a payload that is known not to have committed. Do not abandon after an
unknown commit outcome; replay the exact payload instead.

Reads request one exact Hoglake scan plan, download only those registered objects, attach them to an
isolated local table, enable ClickHouse `table_readonly`, and return an Arrow table. They never list
or scan an object-storage prefix. The adapter currently supports boolean, signed and unsigned integer,
float, double, string, binary, date, and second/millisecond/microsecond/nanosecond timestamp columns.
Date and timestamp values must also fit the configured ClickHouse version's `Date32`/`DateTime64`
ranges; the adapter does not widen those engine domains. Schemas are fixed. Partition specs, sort
orders, deletion vectors, explicit row IDs, packed-part
compaction, and mixed-format tables are refused. The writer and reader should use the same ClickHouse
version; no cross-version compatibility or remote-read performance claim is made.

The adapter also bounds each part, part count and total registered bytes in a read, ClickHouse
memory, result bytes, worker threads, and process time. Constructor arguments can lower or raise those limits for a
known workload. The ordinary `Table.append` and `prepare_append_*` methods remain Parquet-only and
reject packed tables before writing an object.

## Configuration

| What | How |
|---|---|
| Server | `HoglakeClient(base_url, timeout=30.0)` — `/v1` is appended |
| Object store | `S3Config(access_key, secret_key, endpoint_override, region, allow_bucket_creation, single_request_uploads)`; the write path uses `pyarrow.fs.S3FileSystem` (path-style with an endpoint override), and `boto3` built from the same settings for single-request uploads (`single_request_uploads=False` sends everything through the streaming writer) |
| Upload fan-out | `concurrency=` on the prepared-append calls, else `PYHOGLAKE_UPLOAD_CONCURRENCY`, else 64 — capped at the number of objects. Arrow's process-global IO thread pool is raised to match only when the flush actually routes an object through Arrow |
| `User-Agent` | `pyhoglake/<version>` on every request, from the installed package metadata (`pyhoglake/unknown` from a source checkout). The server names the client in its transition warnings by this header — notably the blind-partitioned-append WARN — so it identifies which writer has to change |
| Errors | Typed, all under `HoglakeError`, and every one of them answers two questions — `retryable` (replay the SAME request) and `re_prepare` (replay cannot work; re-read the table and build a NEW one). `CommitConflictError` (`retryable=True` — refresh the read snapshot and retry) · `DdlSinceReadSnapshotError` (`re_prepare=True` — DDL landed on a touched table after the request's `read_snapshot`, so replaying is a livelock) · `IncarnationChangedError` (`re_prepare=True` — the table was dropped and recreated; the append incarnation guard, enforced server-side at commit, see below) · `ReadSnapshotExpiredError` (`re_prepare=True`, a subclass of `ExpiredError` — a commit whose `read_snapshot` sank below the expiry floor) · `NotFoundError` · `AlreadyExistsError` · `ValidationError` · `OffsetRegressionError` · `ExpiredError` (410 on a changefeed window — reconcile from a full scan) · `MalformedResponseError` (every wire-parse failure — a structurally defective response body, an unexpected redirect (3xx is never success), a field of the wrong shape — one exception type naming the model and field) |

## Type mapping

The Arrow writer path maps 26 of the catalog's 27 wire names, both
directions. `variant` is the exception: Arrow cannot construct it, so a
`variant` column arrives only through `Table.prepare_append_files` (see
"Native VARIANT files" below). An Arrow type outside the table is
rejected with an error listing the supported set.

| pyarrow | hoglake |
|---|---|
| `bool_` | `boolean` |
| `int8` / `int16` / `int32` / `int64` | `int8` / `int16` / `int` / `long` |
| `uint8` / `uint16` / `uint64` | `uint8` / `uint16` / `uint64` |
| `uint32` | `uint32` — asymmetric: the writer contract is parquet INT64, so `coltype_to_arrow("uint32")` returns `int64` |
| `float32` / `float64` | `float` / `double` |
| `string` / `large_string` | `string` |
| `pa.json_()` (pyarrow >= 19) | `json` — the extension type only; a bare string stays `string` |
| `binary` / `large_binary` | `binary` |
| `date32` | `date` |
| `time64("us")` | `time` |
| `timestamp("s")` / `("ms")` / `("us")` / `("ns")` | `timestamp_s` / `timestamp_ms` / `timestamp` / `timestamp_ns` |
| `timestamp("us", tz)` | `timestamptz` — micros only; the other units exist tz-naive |
| `decimal128(p, s)` | `decimal` (`type_params: {precision, scale}`) |
| `pa.uuid()` or `binary(16)` (fixed) | `uuid` — 16 big-endian bytes, i.e. `uuid.UUID(...).bytes` |
| `list` family / `struct` / `map` | `list` / `struct` / `map` — children validated recursively; an empty struct is refused |

### uuid columns: the wire form, and the two spellings

A hoglake `uuid` column is parquet `FIXED_LEN_BYTE_ARRAY(16)` carrying
the `UUID` logical annotation (docs/iceberg-federation.md) — what
server-side compaction writes on every file it rewrites, and the form an
Iceberg-conformant reader needs to see a UUID rather than opaque bytes.
pyarrow stamps that annotation only for its canonical uuid extension
type, so `coltype_to_arrow("uuid")` returns `pa.uuid()` and every file
this client writes is annotated. It needs **pyarrow >= 21** (the package
floor): `pa.uuid()` exists from 18, but 18 through 20 write the bytes
with no annotation.

Both spellings are accepted everywhere on the way in:

- `Table.append` takes `pa.uuid()` or plain `pa.binary(16)` — the batch
  is cast to the catalog's schema, and the 16 bytes are the same either
  way.
- `Table.prepare_append_files` accepts a file whose uuid column is
  annotated **or** bare. A hoglake table holds both: compaction outputs
  have always been annotated, while files this client registered before
  the contract — and any writer below the pyarrow floor, or one that
  simply does not annotate — carry the bare form. Nothing else about the
  schema check is loosened, so a file with the right type and the wrong
  `PARQUET:field_id` is refused exactly as before.
- Reading, `arrow_type_to_coltype` maps both to `uuid`, and footer
  bounds are read from either (the bytes are unsigned-lexicographic in
  both).

Identifiers (namespace/table/view/column names) must match
`^[A-Za-z_][A-Za-z0-9_-]{0,127}$` — the server 422s anything else, and
the constraint is also a CHECK in its schema; catalog names are
stricter (lower-case start, max 63).

Column names starting with `_hog` are additionally reserved for hoglake
internals (`_hog_row_id` is compaction's row-id carrier) — the server
422s them at create/add/rename, and the client fast-fails them at
table-create and append time before any request or upload;
namespace/table/view names are not affected. Deletion-vector positions
are per-file **physical row ordinals** (0-based position within that
parquet file), not row ids.

## Partitioned writes — client-side transforms, fanout appends

When a table has a live partition spec, `Table.append` computes each
row's partition tuple client-side (the server never opens data files),
splits the batch by tuple, writes **one parquet file per partition**,
and registers them all in **one atomic commit** — each file carrying its
`partition_values` (transformed values as strings, by key_index). The
coarse-grained layout the near-term consumers use is months per team:

```python
from pyhoglake import ops

table = ns.create_table(
    "events",
    pa.schema(
        [
            pa.field("team_id", pa.int64(), nullable=False),
            pa.field("ts", pa.timestamp("us")),
            pa.field("payload", pa.string()),
        ]
    ),
)
fid = {c.name: c.field_id for c in table.columns}
table.alter(
    [
        ops.set_partition_spec(
            [
                ops.partition_field(fid["team_id"], "identity"),
                ops.partition_field(fid["ts"], "month"),
            ]
        )
    ]
)

# a batch spanning 3 months x 2 teams -> 6 files, ONE commit
res = table.append(batch)
for f in res.files:  # AppendResult exposes the computed tuples
    print(f.path, f.record_count, f.partition_values)
    # ('101', '672') = identity(team_id)=101, month(ts)=2026-01
    # (months are epoch-relative ints per Iceberg: 672 = (2026-1970)*12)
```

Transforms (`pyhoglake.transforms`) follow the Iceberg spec exactly and
are pinned against its published test vectors:

| Transform | Semantics |
|---|---|
| `identity` | the source value |
| `year` / `month` / `day` / `hour` | epoch-relative ints (years/months since 1970, days/hours since the epoch, floored — pre-1970 is negative) |
| `bucket` (param N) | `(murmur3_x86_32(iceberg_encode(v)) & Integer.MAX_VALUE) % N` — bit-compatible with Iceberg/server bucketing |
| `truncate` (param W) | ints floored to a multiple of W; strings to W codepoints (client-side, ahead of server vocabulary support) |

A null source value yields a null partition value forming its own
partition group (Iceberg semantics). The tuple is computed under the
spec the client resolved (`Table.append` re-resolves pre-flight;
`prepare_append_files` uses its cached read). If the spec changes before
the commit lands, the server refuses the commit — 409
`ddl_since_read_snapshot`, atomically and with zero writes — and the
client never silently recomputes under a different spec. That refusal is
also why an append whose files carry `partition_values` must send a
`read_snapshot`: without one the commit has no conflict window, so the
spec change could not be detected at all. The server will answer 422
once `HOGLAKE_REFUSE_BLIND_PARTITIONED_APPENDS` is flipped; today it
accepts the shape, WARNs once per resolved table and counts
`hoglake_blind_partitioned_appends_total`.
pyhoglake supplies the basis itself, so a caller never has to. Grouping runs arrow-native where possible and per
*unique* value (never per row) otherwise. Compaction groups only within
`(spec_id, partition_values)` server-side, so partition-local file
layout is preserved end to end.

## Prepared appends — buffered encode, one-request uploads, one commit

A writer that owns its own partitioning and sort order (the events
writer flushes ~105K rows fanned out over ~271 partition tuples) prepares
its files itself and publishes them in a separate, idempotent step:

```python
key = str(uuid.uuid4())  # this prepare's idempotency key
# ... or prepare_append_files([(path, partition_tuple), ...])
request = table.prepare_append_tables(
    [(arrow_table, partition_tuple), ...],
    idempotency_key=key,
    expected_table_uuid=table.table_uuid,
)
persist(request)  # durably, BEFORE publishing
catalog.commit_prepared(request)  # idempotent: safe to retry
```

`prepare_append_tables` encodes each group to a parquet buffer in memory
— nothing to write, fsync, re-read for the footer and unlink — and
produces the same registrations `prepare_append_files` produces for the
same rows. Use `prepare_append_files` for files another writer produced
(native VARIANT, `allow_optional_fields`); Arrow tables are refused for
a VARIANT destination, because an Arrow rewrite loses the annotation.

| | |
|---|---|
| Objects ≤ 8 MiB | ONE `PutObject`. pyarrow's S3 output stream always opens a multipart upload — three round trips for a 12 KiB file, ~320 ms — so this path uses `boto3` (the `fast-upload` extra), built from the same `S3Config`. Without the extra, small objects take the streaming path and pay the three requests. |
| Larger objects | pyarrow's streaming multipart write, 8 MiB at a time. |
| Fan-out | every object of the flush at once, `min(len(groups), 64)` in flight; `concurrency=` per call, `PYHOGLAKE_UPLOAD_CONCURRENCY` per process. Arrow's IO thread pool is **process-global** and every Arrow S3 request runs on it, so it is raised to match the fan-out — but only when this flush has an object that Arrow will carry (one above the threshold, or no single-request client at all). A flush where every object goes up as one `PutObject` leaves the pool alone. Never lowered either way. |
| A fault mid-flush | the first error is re-raised as the object store's own `OSError` (botocore's fault classes are translated to it, so both upload paths fail alike); uploads that have not started are cancelled, in-flight ones are waited for, and the exception carries `uploaded_files` / `uploaded_uris` — exactly the objects that landed, in input order. That list is **sparse**, not a prefix: uploads run in parallel, so file 7 can be in it while file 6 is not, and neither the count nor a position tells you anything about a particular file. Sweep the uris themselves, never the `{idempotency_key}/` prefix: a retry under the same key writes new names beside the old. |
| Memory | `prepare_append_tables` holds the caller's Arrow input AND every encoded buffer until it returns; `prepare_append_files` reads each file inside its own upload, so it holds at most `concurrency` of them. |

**Deploy prerequisite.** The single-request path is only active if
`boto3` is importable, so a writer that wants it must install
`pyhoglake[fast-upload]` — a plain `pyhoglake` install keeps the
three-request streaming path and pays ~3x the S3 requests per flush.
When the extra is missing, or when botocore refuses the ambient AWS
configuration, pyhoglake logs one warning per client on the `pyhoglake`
logger naming the condition — so a process that configures logging hears
about it, while the library itself only attaches a `NullHandler` and
never writes to stderr uninvited. `single_request_uploads=False` is how
to choose the streaming path deliberately, and it logs nothing.

## The append incarnation guard — atomic at commit

Commit payloads are addressed by **(namespace, table) name**, so an
append lands on whatever table currently holds that name. If the table
is dropped and recreated (a new `table_uuid`) between your resolve and
your append, a naive client would write into the new incarnation's
history without noticing.

`Table.append(..., expected_table_uuid=...)` guards against this
**atomically, at commit time**. Every commit carries an
`expected_table_uuid` field (default: the `table_uuid` the `Table`
object was resolved as; pass one explicitly to pin a specific
incarnation), and the server rejects the whole commit with 409
`table_recreated` — zero writes — when the live table's uuid differs.
The client maps that CODE to `IncarnationChangedError`
(`re_prepare=True`); ordinary commit conflicts remain
`CommitConflictError` (retryable). There is no window in which a
recreated table can accept rows from a guarded append.

`Table.append` also keeps **one** cheap pre-flight re-resolve before the
parquet upload. That is purely an optimization — it fast-fails an
already-dead incarnation before paying for the S3 write — not the
safety mechanism. `prepare_append_files` makes no such read when its
cache is warm (see "The writer path's reads" below); the server-side
guard is the same either way. A commit-time refusal orphans the
uploaded parquet (cleanup's problem, never the catalog's). A refused
append never rebases the `Table` object's pinned identity, so a blind
retry trips the guard again rather than silently adopting the new
incarnation.

To opt out entirely (name-only resolution), pass
`expected_table_uuid=pyhoglake.UNGUARDED`; the commit then carries no
`expected_table_uuid` field and no pre-flight check runs.

## The writer path's reads

A flush used to cost two GETs before its commit: one on the catalog for
a `read_snapshot`, and one on the table for its uuid, columns and spec.
The second is the expensive one — the server's table response computes
the table's live totals, a `count(*)` and two sums over every live file
row of the table (hoglake#232) — and the writer never reads a total.

Both are gone from the steady state, and no new wire field was needed
for it. `Table` caches the last `TableInfo` it read together with the
snapshot that read resolved at (`TableInfo.read_snapshot_id`), and
`prepare_append_files` prepares against the cache and sends that same
snapshot as `read_snapshot`. The server's existing OCC then does exactly
the validation the re-read was for: the commit is accepted iff nothing
has altered, dropped or recreated the table since. Anything that has is
one of the three `re_prepare=True` refusals, which drops the cache so
the next `prepare_*` re-reads once.

The zero-GET steady state needs a server that returns
`TableInfo.read_snapshot_id`, and a **current server always does** — it is
required and non-null in the spec. An OLDER one sends nothing to use as a
`read_snapshot`, so the cache is never used and the writer path makes the
same two reads it always did (one of them cheaper, since it asks for
identity only). Every row below is measured in tests on both sides of
that, and `test_metadata_parity.py` pins the three cross-tree facts the
fast path rests on: the server declares the field, declares it required,
and documents the `totals` parameter.

| Per flush | Requests |
|---|---|
| steady state (server returns `read_snapshot_id`) | 1 POST commit, 0 GET |
| every half of `snapshot_retention_seconds` | + 1 identity GET (`totals=false`) + 1 catalog GET |
| after DDL / a recreate / a below-floor snapshot | 1 refused POST, then one re-prepare's 2 GETs; that flush's uploads are orphaned |
| server too old for `TableInfo.read_snapshot_id` | 1 catalog GET + 1 identity GET, as before (minus the totals scan) |

The age refresh is the one thing the cache needs beyond invalidation: a
`read_snapshot` older than the catalog's retention is below the expiry
floor, and the resulting 410 arrives *after* the flush's parquet is
uploaded — a re-encode and a set of orphaned objects each time. The
cache therefore refreshes at half the retention (read once per `Catalog`
from `snapshot_retention_seconds`; 30 minutes assumed when the catalog
will not say, and never when retention is disabled). An old
`read_snapshot` costs the server nothing otherwise: the conflict scan is
an index range over one table's change rows.

`Catalog.commit_prepared(payload, table=...)` is what drops the cache on
a `re_prepare` refusal, and **`Table.commit_prepared(payload)` is the
form to prefer** — it passes the Table for you, so the invalidation
cannot be forgotten. The Catalog form stays for the cross-process case
the contract is built around (persist the payload, publish from anywhere,
possibly after a restart); **a caller on that form without `table=` owns
invalidation itself**, via `Table.invalidate()` or by dropping the
`Table`. Not doing so turns a one-flush refusal into a loop until the
cache ages out.

Reads a *caller* makes are untouched — `table.info()` still returns the
totals. Only the writer path asks for identity alone, and
`expected_table_info=` opts out of the cache entirely, because its
contract is to compare the caller's layout against a fresh *server*
read.

## Not in 0.1

Deletion-vector writes and Iceberg-facade reads.

## Comparison with pyiceberg

pyhoglake follows pyiceberg's ergonomics where they fit, but the
architecture differs on purpose: the catalog is a *service* — pyhoglake
is a thin REST client that owns only the writer path (parquet + footer
stats), and everything transactional happens server-side.

| Feature | pyhoglake | pyiceberg |
|---------|-----------|-----------|
| **Metadata storage** | Postgres, behind a REST control plane (never touched by clients) | Files (JSON, Avro manifests) via catalog |
| **Catalog backends** | 1 (the hoglake service) | 7 (REST, Hive, Glue, DynamoDB, SQL, BigQuery, In-memory) |
| **Commit protocol** | Footer-shipping: client writes parquet, ships stats, server registers + OCC | Client writes manifests + metadata, catalog swaps pointer |
| **Deferred statistics** | Yes (`deferred_stats=True`; server hydrates async) | No |
| **Field IDs in files** | Written (`PARQUET:field_id`), verified round-trip | Written |
| **Append** | Yes (`Table.append(arrow_table)`) | Yes |
| **Streaming/batch inputs** | Arrow Table (batching via `row_group_size`) | Arrow only |
| **Row-level deletes** | Deletion vectors (server-registered, superseding, conflict-checked); DV *writing* not yet in the client | Position/equality delete files (v0.7+, partial) |
| **Upsert / merge / overwrite** | No (append + DV only, by design v1) | Overwrite yes; upsert yes (v0.7+) |
| **Schema evolution** | Typed ops: add, drop, rename, promote (int→long, float→double), rename table, set partition spec | Add, drop, rename, widen, reorder, union-by-name |
| **Partitioning** | identity, bucket (Murmur3, Iceberg-compatible), year/month/day/hour — spec DDL + partitioned appends (client-side transforms, one file per partition, one commit) | identity, bucket, truncate, year/month/day/hour |
| **Time travel** | Snapshot id or timestamp, on tables/files/scan | Snapshot id, ref name, or timestamp |
| **Snapshot branches/tags** | No | Yes |
| **Change data capture** | First-class: `changes()` plan (files + DVs per snapshot range), 410 on expired ranges | Not implemented |
| **Consumer offsets** | First-class catalog state, monotonic, retention-aware | Not implemented |
| **Row lineage** | Server-assigned contiguous row-id ranges, never reused | Row lineage (v3 spec, partial) |
| **Retention / expiry** | Catalog property; consumer-offset floor; client can trigger + tune | Expire snapshots (limited) |
| **Table maintenance** | Server-side (expiry, cleanup, compaction — DV-aware, row-id-preserving) — client just triggers | Client-side, limited |
| **Views** | Full CRUD (SQL stored verbatim + dialect) | Not implemented |
| **Multi-table transactions** | Yes — one commit may span tables (atomic) | Single-table only |
| **Concurrency** | Server-side OCC; typed 409s with `retryable=True`; appends never conflict with appends | Optimistic, client-side, no retry |
| **Metrics/observability** | Server `/metrics` + audit log; client stays thin | N/A |
| **Zero-infrastructure quickstart** | No — requires the service (docker compose up) | Yes with SQL/memory catalogs |
| **Package size** | 3 deps (httpx, pyarrow, thrift), +boto3 with the `fast-upload` extra | ~200MB with PyArrow + optional deps |

### Native VARIANT files

Catalog columns may use `variant` (Iceberg v3). Publish native Parquet VARIANT(1)
files with `Table.prepare_append_files` and the existing prepared-commit API.
The client validates top-level field IDs, native annotations, scalar types and
required-column null counts, then uploads the original bytes. Arrow `Table.append`
does not construct VARIANT; an Arrow rewrite loses the annotation.

VARIANT columns cannot be partition or sort keys, and have no whole-column
statistics. Shredded child statistics are not published as catalog bounds.
The server skips VARIANT tables during scalar compaction. Reader interoperability
and the buffered-ingestion coordinator are separate work.
