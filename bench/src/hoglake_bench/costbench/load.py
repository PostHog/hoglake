"""Load the public ClickBench seed, then apply the original CostBench inflation.

Schema source: ClickHouse/ClickBench b348345e, trino-datalake-partitioned/create.sql.
Inflation source: ClickHouse/CostBench 9455db6, large/inflate_until.sh.
The final LIMIT deliberately has no ORDER BY, matching the upstream workload.
Loading is separate from query timing. Checkpoints are private operational files,
not publication artifacts: they carry the destination bucket and table identity.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import time
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyhoglake import AlreadyExistsError, HoglakeClient, S3Config

SEED_URL = "https://datasets.clickhouse.com/hits_compatible/hits.parquet"
SEED_BYTES = 14_779_976_446
SEED_ROWS = 99_997_497
SEED_SHA256 = "a390f6cb782f6aaef278c72fc1dd86c4f30bc843ebab3c159e9bd4d45ddb079f"
SCHEMA_REVISION = "b348345eb251fb7ebe611d403d6089bcdcc700d4"
COSTBENCH_REVISION = "9455db619f29f5141b35f9efd93025867af1f56c"

# The signed widths are intentional: these are the upstream Trino/Hive types.
# In particular uint64 IDs are represented by their signed BIGINT bit pattern,
# rather than changing the benchmark's grouping keys to DECIMAL(20, 0).
_COLUMN_TYPES = """
WatchID bigint
JavaEnable smallint
Title varchar
GoodEvent smallint
EventTime timestamp
EventDate date
CounterID integer
ClientIP integer
RegionID integer
UserID bigint
CounterClass smallint
OS smallint
UserAgent smallint
URL varchar
Referer varchar
IsRefresh smallint
RefererCategoryID smallint
RefererRegionID integer
URLCategoryID smallint
URLRegionID integer
ResolutionWidth smallint
ResolutionHeight smallint
ResolutionDepth smallint
FlashMajor smallint
FlashMinor smallint
FlashMinor2 varchar
NetMajor smallint
NetMinor smallint
UserAgentMajor smallint
UserAgentMinor varchar
CookieEnable smallint
JavascriptEnable smallint
IsMobile smallint
MobilePhone smallint
MobilePhoneModel varchar
Params varchar
IPNetworkID integer
TraficSourceID smallint
SearchEngineID smallint
SearchPhrase varchar
AdvEngineID smallint
IsArtifical smallint
WindowClientWidth smallint
WindowClientHeight smallint
ClientTimeZone smallint
ClientEventTime timestamp
SilverlightVersion1 smallint
SilverlightVersion2 smallint
SilverlightVersion3 integer
SilverlightVersion4 smallint
PageCharset varchar
CodeVersion integer
IsLink smallint
IsDownload smallint
IsNotBounce smallint
FUniqID bigint
OriginalURL varchar
HID integer
IsOldCounter smallint
IsEvent smallint
IsParameter smallint
DontCountHits smallint
WithHash smallint
HitColor varchar
LocalEventTime timestamp
Age smallint
Sex smallint
Income smallint
Interests smallint
Robotness smallint
RemoteIP integer
WindowName integer
OpenerName integer
HistoryLength smallint
BrowserLanguage varchar
BrowserCountry varchar
SocialNetwork varchar
SocialAction varchar
HTTPError smallint
SendTiming integer
DNSTiming integer
ConnectTiming integer
ResponseStartTiming integer
ResponseEndTiming integer
FetchTiming integer
SocialSourceNetworkID smallint
SocialSourcePage varchar
ParamPrice bigint
ParamOrderID varchar
ParamCurrency varchar
ParamCurrencyID smallint
OpenstatServiceName varchar
OpenstatCampaignID varchar
OpenstatAdID varchar
OpenstatSourceID varchar
UTMSource varchar
UTMMedium varchar
UTMCampaign varchar
UTMContent varchar
UTMTerm varchar
FromTag varchar
HasGCLID smallint
RefererHash bigint
URLHash bigint
CLID integer
"""


def seed_schema() -> pa.Schema:
    types = {
        "bigint": pa.int64(),
        "integer": pa.int32(),
        "smallint": pa.int16(),
        "varchar": pa.string(),
        "date": pa.date32(),
        "timestamp": pa.timestamp("us", tz="UTC"),
    }
    return pa.schema(
        [
            pa.field(name, types[kind])
            for name, kind in (
                line.split() for line in _COLUMN_TYPES.splitlines() if line
            )
        ]
    )


def convert_batch(batch: pa.RecordBatch | pa.Table) -> pa.Table:
    """Use upstream Trino signed integers, dates, and UTC epoch timestamps."""
    source = (
        pa.Table.from_batches([batch]) if isinstance(batch, pa.RecordBatch) else batch
    )
    names = {name.lower(): name for name in source.column_names}
    schema = seed_schema()
    if set(names) != {field.name.lower() for field in schema}:
        raise ValueError("seed columns differ from the pinned ClickBench schema")
    arrays = []
    for field in schema:
        column = source[names[field.name.lower()]]
        if pa.types.is_timestamp(field.type) and pa.types.is_integer(column.type):
            # Numeric seed values are seconds, not the microseconds in our output.
            column = pc.cast(column, pa.int64(), safe=False)
            column = pc.cast(column, pa.timestamp("s", tz="UTC"))
        elif pa.types.is_date32(field.type) and pa.types.is_integer(column.type):
            # The public seed stores epoch days as uint16. Arrow's date32 cast
            # supports int32 input, but not uint16/int64 directly.
            column = pc.cast(column, pa.int32(), safe=False)
        arrays.append(pc.cast(column, field.type, safe=False))
    return pa.Table.from_arrays(arrays, schema=schema)


def seed_batches(
    path: Path, *, start: int, stop: int, batch_rows: int
) -> Iterator[pa.Table]:
    """Read bounded batches, skipping whole row groups before a checkpoint."""
    if batch_rows <= 0 or start < 0 or stop < start:
        raise ValueError("invalid batch size or seed row interval")
    parquet = pq.ParquetFile(path)
    if stop > parquet.metadata.num_rows:
        raise ValueError("requested seed rows exceed the source file")
    offset = 0
    for group in range(parquet.num_row_groups):
        group_rows = parquet.metadata.row_group(group).num_rows
        if offset >= stop:
            break
        if offset + group_rows <= start:
            offset += group_rows
            continue
        for batch in parquet.iter_batches(batch_size=batch_rows, row_groups=[group]):
            lower = max(0, start - offset)
            upper = min(batch.num_rows, stop - offset)
            if upper > lower:
                yield convert_batch(batch.slice(lower, upper - lower))
            offset += batch.num_rows
            if offset >= stop:
                return


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, indent=2, default=str)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def obtain_seed(path: Path) -> dict[str, Any]:
    """Download once; verify the pinned bytes without loading them into RAM."""
    digest = hashlib.sha256()
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(path.name + ".download")
        with httpx.stream(
            "GET", SEED_URL, timeout=120, follow_redirects=True
        ) as response:
            response.raise_for_status()
            with temporary.open("wb") as stream:
                for chunk in response.iter_bytes(chunk_size=8 * 1024 * 1024):
                    stream.write(chunk)
                    digest.update(chunk)
        if temporary.stat().st_size != SEED_BYTES:
            raise ValueError("downloaded seed size differs from the pinned source")
        if digest.hexdigest() != SEED_SHA256:
            raise ValueError("seed checksum differs from the pinned source")
        temporary.replace(path)
    else:
        if path.stat().st_size != SEED_BYTES:
            raise ValueError("existing seed size differs from the pinned source")
        with path.open("rb") as stream:
            for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
                digest.update(chunk)
    if digest.hexdigest() != SEED_SHA256:
        raise ValueError("seed checksum differs from the pinned source")
    metadata = pq.ParquetFile(path).metadata
    if metadata.num_rows != SEED_ROWS:
        raise ValueError("seed row count differs from the pinned ClickBench dataset")
    return {
        "url": SEED_URL,
        "bytes": SEED_BYTES,
        "rows": SEED_ROWS,
        "sha256": digest.hexdigest(),
    }


def inflation_steps(rows: int, target_rows: int) -> Iterator[int]:
    """Number of rows to insert at each upstream doubling/final-LIMIT step."""
    if rows <= 0 or target_rows < rows:
        raise ValueError("inflation requires a positive seed no larger than the target")
    while rows < target_rows:
        added = min(rows, target_rows - rows)
        yield added
        rows += added


def validate_checkpoint(
    state: dict[str, Any], config: dict[str, Any], table_uuid: str, rows: int
) -> None:
    if state.get("version") != 1 or state.get("config") != config:
        raise ValueError(
            "checkpoint configuration differs; use a new empty table and state file"
        )
    if state.get("table_uuid") != table_uuid:
        raise ValueError("checkpoint table identity changed")
    if state.get("pending"):
        raise ValueError(
            "checkpoint contains an operation with an unknown outcome; "
            "do not retry it blindly: inspect the table and query before reconciling"
        )
    if state.get("rows") != rows:
        raise ValueError(
            "checkpoint row count differs from the server; refusing to duplicate rows"
        )


def exact_table_info(table: Any, *, snapshot_id: int | None = None) -> Any:
    """Count this isolated load's files at a fixed snapshot, never a head sample.

    Current head responses use maintenance-sampled totals which can be stale or
    absent. A time-travel response aggregates the manifest at the requested pin.
    The load creates only hundreds/thousands of files; this explicit bounded
    benchmark check is intentionally outside the measured query execution.
    """
    if snapshot_id is None:
        snapshot_id = table.info(totals=False).read_snapshot_id
    if snapshot_id is None:
        raise ValueError(
            "loader requires a server exposing the resolved read_snapshot_id"
        )
    info = table.info(snapshot=snapshot_id)
    if any(
        value is None
        for value in (info.record_count, info.file_count, info.file_size_bytes)
    ):
        raise ValueError("snapshot-pinned table totals must be exact and present")
    return info


def wait_for_stats(
    base_url: str,
    catalog: str,
    namespace: str,
    table: str,
    *,
    snapshot: int,
    timeout_s: float = 1800,
    poll_interval: float = 5,
    http_client: httpx.Client | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Wait outside measurement until every loaded file has usable stats.

    Trino's footer-only registrations start pending. Starting queries before
    the hydrator catches up would benchmark a changing pruning state. Requests
    pin the completed dataset's snapshot; errors name counts, never S3 paths.
    """
    if (
        timeout_s <= 0
        or not math.isfinite(timeout_s)
        or poll_interval <= 0
        or not math.isfinite(poll_interval)
    ):
        raise ValueError("hydration timeout and poll interval must be positive")
    catalog_path, namespace_path, table_path = (
        quote(value, safe="") for value in (catalog, namespace, table)
    )
    url = (
        f"{base_url.rstrip('/')}/v1/catalogs/{catalog_path}"
        f"/namespaces/{namespace_path}/tables/{table_path}/files"
    )
    started = clock()
    deadline = started + timeout_s
    client = http_client or httpx.Client(follow_redirects=False)
    counts = {"provided": 0, "pending": 0, "failed": 0}
    try:
        while True:
            remaining = deadline - clock()
            if remaining <= 0:
                raise TimeoutError(
                    f"stats hydration timed out: {counts['pending']} pending files"
                )
            try:
                response = client.get(
                    url, params={"snapshot": snapshot}, timeout=min(30, remaining)
                )
                response.raise_for_status()
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code < 500 and exc.response.status_code != 429:
                    raise
                print("stats hydration API temporarily unavailable", flush=True)
            except httpx.TransportError:
                print("stats hydration API temporarily unavailable", flush=True)
            else:
                files = response.json()
                if not isinstance(files, list) or not files:
                    raise ValueError("loaded table has no file list to validate")
                counts = {"provided": 0, "pending": 0, "failed": 0}
                for file in files:
                    state = file.get("stats_state") if isinstance(file, dict) else None
                    if state not in counts:
                        raise ValueError("loaded file has an unknown stats_state")
                    counts[state] += 1
                if counts["failed"]:
                    raise ValueError(
                        f"stats hydration failed for {counts['failed']} loaded files"
                    )
                if not counts["pending"]:
                    return {
                        "provided_files": counts["provided"],
                        "elapsed_s": clock() - started,
                    }
                print(
                    f"waiting for stats: pending_files={counts['pending']}", flush=True
                )
            sleep(min(poll_interval, max(0, deadline - clock())))
    finally:
        if http_client is None:
            client.close()


def _begin_operation(path: Path, state: dict[str, Any], kind: str, added: int) -> None:
    state["pending"] = {
        "kind": kind,
        "before": state["rows"],
        "after": state["rows"] + added,
    }
    _atomic_json(path, state)


def _finish_operation(path: Path, state: dict[str, Any], observed_rows: int) -> None:
    if observed_rows != state["pending"]["after"]:
        raise ValueError("load operation produced an unexpected server row count")
    state["rows"] = observed_rows
    state["pending"] = None
    _atomic_json(path, state)


def add_load_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--trino-url", required=True)
    parser.add_argument("--user", default="benchmark")
    parser.add_argument(
        "--password-env", help="environment variable containing the Trino password"
    )
    parser.add_argument("--ca-file", type=Path, help="CA PEM file for Trino HTTPS")
    parser.add_argument("--catalog", default="hoglake", help="Trino catalog name")
    parser.add_argument(
        "--hoglake-catalog", default="costbench", help="Hoglake API catalog name"
    )
    parser.add_argument("--schema", default="costbench")
    parser.add_argument("--table", default="hits")
    parser.add_argument("--hoglake-url", required=True)
    parser.add_argument(
        "--data-path", required=True, help="new catalog's s3://bucket/prefix/"
    )
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--seed-file", type=Path, default=Path("hits.parquet"))
    parser.add_argument("--target-rows", type=int, default=1_000_000_000)
    parser.add_argument("--batch-rows", type=int, default=250_000)
    parser.add_argument(
        "--state",
        type=Path,
        default=Path("costbench-load-state.json"),
        help="private checkpoint",
    )
    parser.add_argument("--manifest", type=Path, default=Path("costbench-load.json"))
    parser.add_argument("--query-timeout", type=float, default=7200)
    parser.add_argument(
        "--hydration-timeout",
        type=float,
        default=1800,
        help="seconds to wait for all file stats before validation",
    )
    parser.add_argument("--resume", action="store_true")


def load_main(args: argparse.Namespace) -> int:
    from .client import TrinoClient, table_name

    if args.target_rows <= 0 or args.batch_rows <= 0:
        raise ValueError("target rows and batch rows must be positive")
    if args.hydration_timeout <= 0 or not math.isfinite(args.hydration_timeout):
        raise ValueError("hydration timeout must be finite and positive")
    if not args.data_path.startswith("s3://"):
        raise ValueError("data-path must use S3")
    password = os.environ.get(args.password_env) if args.password_env else None
    if args.password_env and not password:
        raise ValueError("Trino password environment variable is unset or empty")
    trino_options = {
        "user": args.user,
        "catalog": args.catalog,
        "schema": args.schema,
        "timeout_s": args.query_timeout,
        "password": password,
        "ca_file": args.ca_file,
    }
    # Verify endpoint authentication/TLS before the large download or any writes.
    with TrinoClient(args.trino_url, **trino_options) as preflight:
        preflight.execute("SELECT 1", collect_rows=True)
    started = time.monotonic()
    source = obtain_seed(args.seed_file)
    config = {
        "catalog": args.hoglake_catalog,
        "trino_catalog": args.catalog,
        "schema": args.schema,
        "table": args.table,
        "data_path": args.data_path.rstrip("/"),
        "target_rows": args.target_rows,
        "batch_rows": args.batch_rows,
        "seed_sha256": source["sha256"],
    }
    existing_state = json.loads(args.state.read_text()) if args.state.exists() else None
    if bool(existing_state) != args.resume:
        raise ValueError(
            "use --resume with an existing checkpoint, or a new state path for a new table"
        )
    with HoglakeClient(
        args.hoglake_url, s3=S3Config(region=args.region), timeout=120
    ) as hoglake:
        try:
            catalog = hoglake.create_catalog(args.hoglake_catalog, args.data_path)
        except AlreadyExistsError:
            catalog = hoglake.catalog(args.hoglake_catalog)
        if catalog.data_path.rstrip("/") != args.data_path.rstrip("/"):
            raise ValueError("existing catalog points to a different data path")
        try:
            namespace = catalog.create_namespace(args.schema)
        except AlreadyExistsError:
            namespace = catalog.namespace(args.schema)
        try:
            table = namespace.create_table(args.table, seed_schema())
        except AlreadyExistsError:
            if not existing_state:
                raise ValueError(
                    "target table already exists; use its checkpoint or choose a new table"
                ) from None
            table = namespace.table(args.table)
        info = exact_table_info(table)
        state = existing_state or {
            "version": 1,
            "config": config,
            "table_uuid": str(table.table_uuid),
            "rows": 0,
            "pending": None,
        }
        validate_checkpoint(state, config, str(table.table_uuid), info.record_count)
        _atomic_json(args.state, state)
        seed_target = min(SEED_ROWS, args.target_rows)
        for batch in seed_batches(
            args.seed_file,
            start=min(state["rows"], seed_target),
            stop=seed_target,
            batch_rows=args.batch_rows,
        ):
            _begin_operation(args.state, state, "seed_append", batch.num_rows)
            receipt = table.append(
                batch,
                compression="snappy",
                author="costbench",
                message="public ClickBench seed",
            )
            _finish_operation(
                args.state,
                state,
                exact_table_info(table, snapshot_id=receipt.snapshot_id).record_count,
            )
            print(f"seed rows={state['rows']:,}/{seed_target:,}", flush=True)
        qualified = table_name(args.catalog, args.schema, args.table)
        with TrinoClient(args.trino_url, **trino_options) as trino:
            for added in inflation_steps(state["rows"], args.target_rows):
                _begin_operation(args.state, state, "trino_insert", added)
                limit = "" if added == state["rows"] else f" LIMIT {added}"
                trino.execute(
                    f"INSERT INTO {qualified} SELECT * FROM {qualified}{limit}"
                )
                _finish_operation(
                    args.state, state, exact_table_info(table).record_count
                )
                print(
                    f"inflation rows={state['rows']:,}/{args.target_rows:,}", flush=True
                )
            completed = exact_table_info(table)
            hydration = wait_for_stats(
                args.hoglake_url,
                args.hoglake_catalog,
                args.schema,
                args.table,
                snapshot=completed.read_snapshot_id,
                timeout_s=args.hydration_timeout,
            )
            validation = trino.execute(
                f"SELECT count(*), min(EventDate), max(EventDate), min(EventTime), max(EventTime), "
                f"sum(CAST(JavaEnable AS bigint)) FROM {qualified}",
                collect_rows=True,
            )
        if not validation.rows or int(validation.rows[0][0]) != args.target_rows:
            raise ValueError("Trino row-count validation failed")
        info = exact_table_info(table)
        manifest = {
            "profile": "costbench-query-2025",
            "costbench_revision": COSTBENCH_REVISION,
            "schema_revision": SCHEMA_REVISION,
            "source": source,
            "target_rows": args.target_rows,
            "rows": info.record_count,
            "files": info.file_count,
            "bytes": info.file_size_bytes,
            "layout": {
                "partitioning": [],
                "sorted_by": [],
                "seed_codec": "snappy",
                "inflation_writer": "Trino defaults",
            },
            "integer_conversion": "upstream Trino signed widths; unsigned bit patterns preserved",
            "timestamp_conversion": "epoch seconds to UTC timestamp(6) with time zone",
            "expansion": "INSERT SELECT self-doubling; final unordered LIMIT, as upstream (subset can vary)",
            "validation_columns": [
                column.get("name", str(column))
                if isinstance(column, dict)
                else str(column)
                for column in validation.columns
            ],
            "validation": validation.rows,
            "stats_hydration": hydration,
            "load_elapsed_s": time.monotonic() - started,
        }
        _atomic_json(args.manifest, manifest)
        print(
            f"validated rows={info.record_count:,}; manifest={args.manifest}",
            flush=True,
        )
    return 0
