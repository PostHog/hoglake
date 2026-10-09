# pyhoglake

Python client for [hoglake](https://github.com/PostHog/hoglake#readme), the Postgres-native
lakehouse-catalog control plane. A **thin API wrapper**: no embedded
engine, no SQL, no direct catalog-database access — ever. The client
writes parquet to object storage itself and registers it with the
control plane via footer-shipping commits.

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
when the server is unreachable. For a complete check, run `just live-python`
from the repository root. It starts an isolated stack, enables the hydrator
at a one-second interval for the deferred-stats test, and rejects skipped tests.

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
for f in table.files(snapshot=result.snapshot_id):
    print(f.path, f.record_count, f.stats_state, f.row_id_start)

# changefeed + consumer offsets
plan = table.changes(from_snapshot=0)
catalog.commit_offset("my-consumer", table.table_uuid, plan.to_snapshot)
catalog.offset("my-consumer", table.table_uuid)  # one offset; None if unset
```

`create_table()` and `alter()` keep their DDL snapshot on the table handle.
Without an explicit snapshot, `files()` and `scan_plan()` read that snapshot.
After an append, pass the returned `snapshot_id`, as above. To inspect later
writes from another process, pass `catalog.refresh().head_snapshot_id`.
Calling `info()` or passing `snapshot=None` does not clear the DDL snapshot.
A new handle from `ns.table(name)` has no DDL snapshot and reads the current head.

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
directions. `variant` is the exception: Arrow has no VARIANT type, so a
schema DECLARES one with `variant.variant_field` (see "variant columns:
declaring a shredded layout" below), and the data arrives only through
`Table.prepare_append_files` (see "Native VARIANT files" below). An
Arrow type outside the table is rejected with an error listing the
supported set.

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
| `list` family / `struct` / `map` | `list` / `struct` / `map` — children validated recursively; an empty struct is refused, and so is a struct shaped like VARIANT storage (below) |
| `variant.variant_field(name[, shredding])` — a `pa.json_()` field with a marker | `variant`, with `type_params.shredding` when declared — DDL only |

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

### variant columns: declaring a shredded layout

A top-level `variant` column may declare in `type_params.shredding` the
layout that writers which shred VARIANT values (the Trino connector)
give it: which object fields and array elements get Parquet columns of
their own, and the Variant type of each. Readers use each file's own
layout, so a declaration changes no read, and it is fixed once the
column exists — no alter op changes it, so a different layout is a new
column. pyhoglake declares one in either DDL call:

```python
from pyhoglake import ops, variant

decl = {
    "type": "object",
    "fields": [
        {"name": "$browser", "type": "string"},
        {"name": "price", "type": "decimal8", "precision": 18, "scale": 2},
        {
            "name": "$active_feature_flags",
            "type": "array",
            "element": {"type": "string"},
        },
    ],
}

events = ns.create_table(
    "events",
    pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            variant.variant_field("properties", decl),  # declared
            variant.variant_field("props_raw"),  # undeclared: no type_params
        ]
    ),
)
events.alter([ops.add_column("extra", "variant", shredding={"type": "string"})])
```

`variant_field` is a `pa.json_()` field whose metadata carries
`variant.VARIANT_FIELD_KEY`; `create_table` turns it into
`{"type": "variant", "type_params": {"shredding": ...}}`, and leaves
`type_params` out when nothing is declared. The marker, not the JSON
type, makes it a variant: a `pa.json_()` field without it is still a
`json` column.

The grammar is the server's (the OpenAPI `ColumnDef.type_params` states
it): `object` with a non-empty `fields` list of named fields, `array`
with an `element`, `variant`, or a Variant primitive — `boolean`,
`int8`/`16`/`32`/`64`, `float`, `double`, `date`, `time`, `timestamp`,
`timestamp_ns`, `timestamptz`, `timestamptz_ns`, `binary`, `string`,
`uuid`, or `decimal4`/`8`/`16` with an integer `precision` (at most
9/18/38) and `scale` (0 to the precision). Field names are variant keys
in their original case, unique case-insensitively, with no NUL and no
unpaired surrogate; objects and arrays nest at most 16 deep; at most
1000 fields and arrays, counted together; and the names on the way to a
field are at most 1024 bytes of UTF-8. `variant.validate_shredding`
checks all of it locally, so a declaration the server would refuse
raises `ValidationError` before the request, with the server's own 422
message and the path of the node at fault (`$.price`, `$.tags[*]`). The
server stays authoritative; `tests/vectors/variant_shredding_vectors.json`
is shared with its test suite so the two cannot drift silently. One rule
is partly left to it: whether two names differ only by case follows the
JDK's lowercase mapping, which Python's matches except for GREEK CAPITAL
SIGMA (the JDK picks `ς` or `σ` by word boundaries, Python by Unicode's
Final_Sigma rule) and for case pairs newer than one side's Unicode data.
The client leaves a name with a `Σ`, and such a pair, out of its case
check (names without one are still checked against each other), and
the server answers with a 422 if it finds a collision; the client never
refuses a pair the server accepts.

The client also refuses, locally and in the server's words, a
declaration on a variant nested in a struct, list or map (an
undeclared nested `variant_field` is fine) and `shredding=` on a column
of any other type the server knows (`add_column` reads the type name in
any case, as the server does, so `"VARIANT"` is a variant; a name the
server does not know is left to its 422). And an Arrow struct shaped exactly like VARIANT
storage — `metadata` (binary, not null), `value` (binary), optionally
`typed_value`, which is how pyarrow reads a VARIANT group out of a
Parquet file — is refused with a pointer to `variant_field`, where it
used to become a plain `struct` column without a word.

Choose a declaration with the data's source in mind.
`variant.json_unshreddable_paths(decl)` lists the leaves that values
pyhoglake's writer will encode from JSON text (from 1.4) fill rarely or
never: JSON has no temporal, binary, uuid or float values; an integer
within the int64 range is an integer and never fills a `double` or
`decimal` leaf, and a larger one fills only `decimal16` with scale 0
and a precision of at least 19; a fraction is a double and never fills
a `decimal`. Those columns would stay empty for the life of the table.
Other writers type JSON their own way (DuckDB, for one, types a
non-negative integer as unsigned), so the list speaks for pyhoglake's
writer only.

### variant columns: encoding values

`pyhoglake.variant` also holds a Variant codec at the Arrow level. **It is
not wired into the write paths yet:** `Table.append` and
`prepare_append_tables` still refuse a VARIANT column, and nothing here
writes Parquet. What it gives is the storage a VARIANT column's Parquet
group is written from, shredded as its declaration lays it out, and the
reader of any such storage:

```python
from decimal import Decimal

import pyarrow as pa
from pyhoglake import variant

enc = variant.encode_json(
    pa.array(['{"$browser":"Chrome","price":9.99}', None, "null"], pa.json_()),
    shredding=decl,  # or a catalog Column in its place:
    # encode_json(values, events.columns[1])
    on_invalid="null",  # an invalid row becomes SQL NULL, counted
)
enc.array  # a StructArray of variant.storage_type(decl)
enc.report  # rows, invalid_by_reason, typed / fallback / variant_null per path
variant.to_python(enc.array, shredding=decl)
# [{'$browser': 'Chrome', 'price': 9.99}, None, VARIANT_NULL]

enc = variant.encode_python([{"price": Decimal("12.34")}], shredding=decl)
variant.verify(enc.array, shredding=decl)  # opt-in, byte by byte
```

`storage_type(decl, form="write" | "read")` is the struct: required
`metadata`; with no declaration (or a root `variant`) a required `value`
and nothing else, the only unshredded shape the Trino connector reads;
with one, an optional `value` and an optional `typed_value` in each
group, objects as structs of required field groups (declared order and
case), arrays as lists of required `element` groups. That is the
connector's `VariantShreddingSchema.toParquetType` layout, field for
field. The forms differ only at `decimal4`/`decimal8` leaves, which are
written as `decimal32(p, s)` and `decimal64(p, s)` and read back as
`decimal128(p, s)` from a file with no Arrow schema stored in it (a file
pyarrow wrote with its default `store_schema=True` reads back as the
write form; both forms read). The file holds their unscaled INT32/INT64
with `DECIMAL(p, s)`, the connector's layout. pyarrow 21 cannot write a
decimal32 or decimal64 at all, and later versions write one as a
fixed-length array by default. Their `store_decimal_as_integer=True`
(23 and later) is file-wide and ruled out (D15): it also turns a
`decimal16` leaf and a top-level decimal column of precision 18 or less
into INT32/INT64, where the connector's layout has a fixed-length array.
The writer instead views these leaves as int32/int64 and stamps
`DECIMAL(p, s)` from the plan. The group's VARIANT annotation, which
Arrow cannot carry, is the writer's to add too. Because
each leaf's type names its precision and scale, the storage of `int64`
or of `decimal8(18,6)` is never `==` that of `decimal8(18,2)`, and Arrow
will not put the two in one column, a concat, a take or a sort. The one
overlap is `decimal4`, `decimal8` and `decimal16` of one (p, s): the
first two read back as `decimal128(p, s)`, the third's write form, and
all three hold the same values. A declaration this version cannot honour
raises `UnsupportedShreddingError` (upgrade pyhoglake) rather than write
another layout, or check storage against one it does not know: a node
type it does not know, or any fault in a catalog column's declaration,
which the server accepted. A catalog column's refusal names it.

`to_python` reads any conformant storage as pyarrow reads it from
Parquet, with any of its `binary_type` and `list_type` options:
pyhoglake's, the Trino connector's, or DuckDB's, whose field and element
groups are optional (a null one reads as a missing field), and whose
1.5.5 writer flags every dictionary sorted, sorted or not, and lists an
object's fields as they were inserted, which the connector reads too: an
object reads in key order, however its bytes list it, but one that holds
a name twice is refused. That writer also spells a string of exactly 64
bytes, wherever it is not shredded into a typed leaf, as an empty short
string followed by the bytes, which DuckDB itself reads as `""`: the
bytes do not end where the spec says, so the row is refused, at the top,
in an array or in an object alike (hedgerow's DuckDB writer is exposed,
see its DUCKDB_WRITER.md). The same goes for any object or array whose
values leave bytes no reader sees: the first value starts at offset 0,
each ends where the next one up begins, and an empty one's last offset
is 0. SQL NULL is `None` and a Variant null `VARIANT_NULL`; inside a
value a Variant null is `None`. DuckDB writes a SQL NULL as a top-level
Variant null, which the Trino connector reads back as SQL NULL in a file
whose `created_by` names DuckDB; storage carries no footer, so
`to_python` gives `VARIANT_NULL`, and a caller reading DuckDB's files
maps a top-level `VARIANT_NULL` to `None`. A date or timestamp outside
the years 1 to 9999 that Python holds is valid, and reads as the pyarrow
scalar of its type. It raises `VariantEncodingError` on what the spec
calls invalid (a typed decimal of more digits than its precision among
it), and on a valid value nested deeper than 128 levels
(`nesting_too_deep`), in bytes or shredded: the spec sets no bound, but
this reader keeps the encoder's (D7).

A table's files need no declaration to be read, since the storage says
how to read itself, and those of a writer that honours none (DuckDB's,
and so hedgerow's) are no form of their column's declaration: read them
without one. A catalog column (`verify(enc.array, events.columns[1])`)
or `shredding=` checks storage that pyhoglake or the Trino connector
built from that declaration: storage that is not a form of it is
`invalid_shredding`, and a column that declares nothing is checked
against the unshredded form, which as `shredding=` is `{"type":
"variant"}`. With neither, the form is not checked: `shredding=None`
means no declaration to the readers, where to the encoders it means the
unshredded one.

`verify` adds the spec's writer rules (no null group, no missing
element, every key in the row's dictionary, a sorted flag only on a
sorted dictionary, fields in key order) and, by default, the canonical
encoding below (D8, D9), placement (D3, D10) included: a value left in
`value` that its group's typed leaf takes is a second spelling of the
row, and refused. Given a NOT NULL column it refuses a SQL NULL row too,
as the encoders do (D5), where `to_python` reads it as `None`.
`to_python` is checked against apache/parquet-testing's `variant/` and
`shredded_variant/` vectors, vendored in `tests/data/parquet-testing`.

The policies, each a decision with its reason (the spec is
[VariantEncoding.md](https://github.com/apache/parquet-format/blob/master/VariantEncoding.md)
and
[VariantShredding.md](https://github.com/apache/parquet-format/blob/master/VariantShredding.md)):

| | Policy | Why |
|---|---|---|
| D1 | JSON numbers: an integer is the smallest of int8/16/32/64 that holds it; beyond int64 and under 39 digits, decimal16 with scale 0 (both signs); 39 digits or more (or past CPython's 4300-digit parse limit) is invalid, `integer_out_of_range`. A fraction or exponent is a double, `-0.0` kept; `NaN`, `Infinity` and a literal beyond the double range are invalid, `non_finite_number`. | Exact or nothing: a lossy double is never written, and one JSON value never encodes differently per table. |
| D2 | Python objects: `bool`, `int` (as D1), `float` (NaN and infinities too), `Decimal` (decimal4/8/16 by precision, which counts the scale; non-finite or over 38 digits is `decimal_out_of_range`), `str`, bytes-like, `date`, naive `time`, `datetime` (aware ones normalised to UTC; one whose UTC time is outside the years 1 to 9999 is `timestamp_out_of_range`, since no Python reader could give it back), `uuid.UUID`, `dict` with str keys, `list`/`tuple`. Types are matched exactly, containers too: a numpy scalar, a str subclass, an OrderedDict, a namedtuple or an aware `time` is invalid. | Strings are never sniffed into other types. No float, and no nanosecond timestamp, is ever written. |
| D3 | A value goes to `typed_value` only in its own type class: an integer of any width into an integer leaf that holds it, a decimal into a decimal leaf of its scale whose precision holds it, booleans, doubles, dates, times, timestamps, binary, strings and uuids into their own type. Never an integer into a double or a decimal. A value that does not match keeps its bytes in `value`. | The Trino connector's rule (`VariantShredder.isShreddedPrimitive`), ported. |
| D4 | SQL NULL (an Arrow null, a top-level `None`) is a null group. A Variant null (JSON `null`, `VARIANT_NULL`, a nested `None`) is `value` = `00`. A missing field has both columns null, which only a field group may. | The spec's truth table; Trino keeps the two apart. |
| D5 | A NOT NULL target refuses SQL NULL, even under `on_invalid="null"`, which is itself refused for it, and `verify` refuses it in storage given a NOT NULL column. A Variant null is a value and allowed. | pyarrow writes a null row of a required group as a present group of empty bytes. |
| D6 | Keys are case-sensitive: only the exact declared name shreds, every other spelling stays in the residual. A duplicate key is invalid (`duplicate_key`), never last-wins. An unpaired surrogate is invalid (`invalid_unicode`), found where the string is encoded, so one bad row never fails the batch. | DuckDB drops the other spellings; a Variant object cannot hold a key twice. |
| D7 | A value nested deeper than 128 levels (the root is 0) is invalid, `nesting_too_deep`, as is anything the JSON parser cannot recurse into. `to_python` and `verify` refuse deeper storage with the same reason. | A deterministic bound inside CPython's recursion limit; not motivated by any reader (DuckDB reads 400), so deeper bytes are not called malformed. |
| D8 | One dictionary per row: every key at every depth, shredded keys too, once, sorted by UTF-8 bytes, `sorted_strings` set when not empty, the smallest offset width; empty is `01 00 00`. Every value column of a row uses it. | The spec requires every key; parquet-java and Iceberg fail without the shredded ones. DuckDB rejects the two-byte empty form. |
| D9 | Canonical bytes: object fields in key order with values laid out in that order, the smallest id and offset widths, `is_large` only above 255 elements, short strings up to 63 bytes, `02 00 00` and `03 00 00` for empty containers. | Encoding is a function of the row alone, so a retry writes the same bytes. |
| D10 | An object under an object declaration always has a `typed_value`; its residual holds only undeclared keys and is null when there are none. An array has `value` null and every element exactly one side set. A primitive has exactly one side. | The spec's writer rules. |
| D11 | A column that declares nothing (`type_params` absent, `{}` or `{"shredding": null}`) or declares a root `variant` is unshredded: required `metadata` and required `value`. pyhoglake never shreds by itself. | The layout never depends on the data, and it is the only unshredded shape Trino reads. |
| D12 | The writer uses exactly the declaration it is given. An unknown node type, or any fault in a catalog column's declaration, raises `UnsupportedShreddingError`, from the encoders and from a reader given the column; a `shredding=` the caller passes is checked as the server would, with its `ValidationError`. Storage built for another declaration is no form of this one (D15). | Fail closed on a newer grammar. |
| D13 | No catalog statistics for a variant column or its leaves. | The server refuses them (applies from the write paths, 1.4). |
| D14 | Field ids belong to the group only; nothing inside the storage struct carries one. | The server's footer reader expects them there. |
| D15 | decimal4/decimal8 leaves are `decimal32(p, s)`/`decimal64(p, s)` in Arrow, and in the file their unscaled int32/int64 with DECIMAL(p, s) in the footer; decimal16 is `decimal128(p, s)`. | Spec- and Trino-exact without touching other decimal columns. Each leaf's type names its precision and scale, so the storage says how to read itself, and declarations of different types never share storage; decimal4/8/16 of one (p, s) read back alike, holding the same values. |
| D16 | uuid leaves are 16 big-endian bytes, as the Variant value is; strings that look like uuids stay strings. | |
| D17 | Only a top-level variant column is encoded. | No writer shreds a nested variant. |
| D18 | `on_invalid="raise"` (the default) raises `VariantEncodingError` with the column, row, path (`$.tags[3]`, or none where the JSON parser found the fault) and reason; `"null"` writes SQL NULL, counts the row by reason and keeps the first. A key is `.name`, or `["a.b"]` when it holds `.`, `[`, `]`, `"` or `\` or is empty, and `["\ud800"]`, escaped, when it has a control character, a surrogate, U+2028 or U+2029 (a fixed set, so a path does not change with Python's Unicode version). `json_unshreddable_paths` and the report spell paths the same way. A row's path, in an error or in `first_invalid`, is cut to 1024 characters, and a key a message quotes to 64. | A path is one line of text any UTF-8 log can write, and a key as long as the row does not make one as long. |
| D19 | The output for a given input and declaration is pinned by `tests/data/variant/golden_storage.arrow`, which every change to the encoder must reproduce. | Determinism is the contract a faster encoder is held to. |

Large input is encoded in chunks (`EncodedVariant.array` is then a
ChunkedArray): a chunk ends at 8192 rows, or at the row that brings its
encoded bytes to 512 MiB, so that no binary child nears int32 offsets
and the Python objects of one chunk are gone before the next. On
PostHog-like `properties` (about 1 KB and 26 keys a row) with a 12-field
declaration, `encode_json` costs about 16 µs of CPU a row on CPython
3.12 and an Apple M4 Pro (`bench/variant_bench.py`, which is not part of
CI).

### Identifiers and reserved names

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
