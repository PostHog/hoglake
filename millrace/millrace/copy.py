"""millrace-copy: the one-time copy of ``ingest.events`` into
``ingest.events_v2`` (federation plan 2.6; design-2.6.md).

- ``plan``: the settle gate, the census at head, the run lock.
- ``copy``: one shard of (team, partition) units; each unit is ONE commit
  under a key fixed for the unit's version, and its record in S3 (``sent``,
  then ``done``) holds the payload, so a restart resends it.
- ``verify``: what landed, against the census, the records and both tables.

State lives under ``COPY_SCRATCH_URL``: the lock ``RUN``, then
``<run>/census.json.gz``, ``<run>/units/<unit>.json``, ``<run>/verify.json``.
Exit codes: 0 done, 1 verify failed, 2 bad config or shapes, 3 refused or a
unit halted (a human decides: runbook-2.6-copy.md).
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import resource
import secrets
import sys
import time
import uuid
from collections import Counter
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pyhoglake as hl
from pyhoglake.models import DataFile, ScanFile
from pyhoglake.types import column_to_arrow_field
from pyhoglake.upload import Upload, perform_upload, s3_key

from . import flush as fl
from .coerce import UnsupportedColumnError
from .config import _parse_variant_sources
from .main import _s3_kwargs

log = logging.getLogger("millrace.copy")

TEAM, TS = "team_id", "timestamp"
_PAGE = 10_000
_COMMIT_TRIES = 8
_STEPS_PER_RUN = 12
_RETRIES = 5  # transient errors per unit and run, then the pod fails
_MAX_REPAIRS = 3
_CPU_MS_PER_ROW = 0.6  # the fixture's upper bound (design §7)
_EXISTS = ("PreconditionFailed", "412", "ConditionalRequestConflict")
_FINAL = ("done", "unresolved", "halted")
_now = time.time
_sleep = time.sleep

Unit = dict[str, Any]
Rec = dict[str, Any]
Key = tuple[str | None, ...]


class Refused(Exception):
    """Stops the tool with ``code`` (2: config or shapes, 3: refused)."""

    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code


class Halt(Exception):
    """Stops one unit: its record becomes ``halted``, with the reason."""


@dataclass(frozen=True)
class CopyConfig:
    url: str
    catalog: str
    source: tuple[str, str]
    source_uuid: str
    dest: tuple[str, str]
    dest_uuid: str
    variant_sources: dict[str, str]
    run: str
    scratch_url: str
    chunk_bytes: int
    shards: int
    trust_s: float
    retention_s: float
    s_time: float


def load_config(env: Mapping[str, str]) -> CopyConfig:
    def need(key: str) -> str:
        if not (value := env.get(key, "").strip()):
            raise Refused(2, f"{key} is required")
        return value

    def table(key: str) -> tuple[str, str]:
        ns, dot, name = need(key).partition(".")
        if not (dot and ns and name):
            raise Refused(2, f"{key} must be <namespace>.<table>")
        return ns, name

    problems: list[str] = []
    raw = env.get("COPY_VARIANT_SOURCES", "properties_sv=properties")
    sources = _parse_variant_sources(raw, problems) if raw.strip() else ()
    run = need("COPY_RUN")
    if not (run.isalnum() and run.islower() and len(run) <= 24):
        problems.append("COPY_RUN must be 1-24 lowercase letters and digits")
    trust = float(env.get("COPY_RECEIPT_TRUST_S", 6 * 86400))
    # The server's HOGLAKE_RECEIPT_RETENTION_SECONDS (runbook: read it first).
    retention = float(env.get("COPY_RECEIPT_RETENTION_S", 7 * 86400))
    if trust >= retention:
        problems.append("COPY_RECEIPT_TRUST_S must be below COPY_RECEIPT_RETENTION_S")
    if problems or sources is None:
        raise Refused(2, "; ".join(problems))
    return CopyConfig(
        url=need("HOGLAKE_URL"),
        catalog=need("COPY_CATALOG"),
        source=table("COPY_SOURCE"),
        source_uuid=need("COPY_SOURCE_UUID"),
        dest=table("COPY_DEST"),
        dest_uuid=need("COPY_DEST_UUID"),
        variant_sources=dict(sources),
        run=run,
        scratch_url=need("COPY_SCRATCH_URL").rstrip("/") + "/",
        chunk_bytes=int(env.get("COPY_CHUNK_BYTES", 256 * 1024 * 1024)),
        shards=int(env.get("COPY_SHARDS", 1)),
        trust_s=trust,
        retention_s=retention,
        s_time=_iso(need("COPY_S_TIME")),
    )


def _iso(value: str) -> float:
    return datetime.fromisoformat(value).timestamp()


class Scratch:
    """Objects under the scratch prefix, through a boto3 S3 client;
    ``put(create=True)`` is a conditional create (``If-None-Match: *``)."""

    def __init__(self, s3: Any, url: str) -> None:
        self.s3 = s3
        self.bucket, _, self.prefix = s3_key(url).partition("/")

    def get(self, name: str) -> bytes | None:
        try:
            body = self.s3.get_object(Bucket=self.bucket, Key=self.prefix + name)
        except Exception as e:
            if _s3_code(e) in ("NoSuchKey", "404"):
                return None
            raise
        return bytes(body["Body"].read())

    def put(self, name: str, body: bytes, *, create: bool = False) -> bool:
        extra = {"IfNoneMatch": "*"} if create else {}
        key = self.prefix + name
        try:
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=body, **extra)
        except Exception as e:
            if create and _s3_code(e) in _EXISTS:
                return False
            raise
        return True


def _s3_code(e: Exception) -> str | None:
    code = getattr(e, "response", {}).get("Error", {}).get("Code")
    return None if code is None else str(code)


class Ctx:
    """The run: its config, the lake (HTTP and objects) and its records."""

    def __init__(self, cfg: CopyConfig, client: Any, fs: Any, put: Any) -> None:
        self.cfg, self.client, self.fs, self.put = cfg, client, fs, put
        self.scratch = Scratch(put, cfg.scratch_url)
        self.catalog = client.catalog(cfg.catalog)
        self.commit = True

    def req(self, method: str, suffix: str, **kw: Any) -> Any:
        path = f"/catalogs/{fl._seg(self.cfg.catalog)}{suffix}"
        return self.client._request(method, path, **kw)

    def _table(self, which: str, suffix: str) -> str:
        ns, name = getattr(self.cfg, which)
        return f"/namespaces/{fl._seg(ns)}/tables/{fl._seg(name)}{suffix}"

    def info(self, which: str) -> hl.TableInfo:
        """``source`` or ``dest`` at head, refused unless it is the
        configured incarnation."""
        ns, name = getattr(self.cfg, which)
        info = fl.read_table_identity(self.client, self.cfg.catalog, ns, name)
        if info.table_uuid != (want := getattr(self.cfg, f"{which}_uuid")):
            raise Refused(3, f"{ns}.{name} is table {info.table_uuid}, not {want}")
        return info

    def scan(self, which: str, snapshot: int | None) -> list[ScanFile]:
        params = {"snapshot": snapshot}
        body = self.req("GET", self._table(which, "/scan"), params=params)
        return [ScanFile.from_wire(f) for f in body]

    def files(
        self, which: str, snapshot: int | None, filters: Mapping[int, str | None]
    ) -> list[DataFile]:
        """Every file at ``snapshot`` (head is pinned first, so the pages
        agree) matching ``filters``, partition key index → value."""
        parts = [f"{i}:{v}" for i, v in filters.items() if v is not None]
        snapshot = snapshot or self.info(which).read_snapshot_id
        out: list[DataFile] = []
        while True:
            params = {"snapshot": snapshot, "partition": parts, "limit": _PAGE}
            params["offset"] = len(out)
            page = self.req("GET", self._table(which, "/files"), params=params)
            out += [DataFile.from_wire(f) for f in page]
            if len(page) < _PAGE:
                return out

    def load(self, name: str) -> Any:
        raw = self.scratch.get(f"{self.cfg.run}/{name}")
        if raw is None:
            return None
        return json.loads(gzip.decompress(raw) if name.endswith(".gz") else raw)

    def save(self, name: str, obj: Any, *, create: bool = False) -> bool:
        body = json.dumps(obj, separators=(",", ":"), default=str).encode()
        body = gzip.compress(body) if name.endswith(".gz") else body
        return self.scratch.put(f"{self.cfg.run}/{name}", body, create=create)

    def save_unit(self, rec: Rec) -> None:
        if self.commit:  # a dry run writes no unit record
            self.save(f"units/{rec['unit']}.json", rec)

    def check_lock(self) -> None:
        held = self.scratch.get("RUN")
        if held != self.cfg.run.encode():
            raise Refused(3, f"the lock RUN holds {held!r}, not {self.cfg.run!r}")


# -- plan: settle gate, census, run lock -------------------------------------------


def _col(info: hl.TableInfo, name: str) -> hl.Column:
    for column in info.columns:
        if column.name == name:
            return column
    raise Refused(2, f"{info.namespace}.{info.name} has no column {name!r}")


def _team_index(info: hl.TableInfo) -> int:
    spec, fid = info.partition_spec, _col(info, TEAM).field_id
    index = None if spec is None else hl.object_prefix.key_index(spec.fields, fid)
    if index is None:
        raise Refused(3, f"{info.name}'s live spec has no identity({TEAM})")
    return index


def _file_rows(files: list[DataFile]) -> list[list[Any]]:
    files = sorted(files, key=lambda f: f.data_file_id)
    return [[f.data_file_id, f.path, f.record_count, f.file_size_bytes] for f in files]


def take_census(ctx: Ctx) -> dict[str, Any]:
    """The source's live files at head as units, one per partition tuple,
    bin-packed by bytes across the shards, largest first."""
    started = _now()
    src = ctx.info("source")
    spec, team_i = src.partition_spec, _team_index(src)
    assert spec is not None
    units: dict[Key, list[DataFile]] = {}
    for sf in ctx.scan("source", src.read_snapshot_id):
        f, why = sf.data_file, None
        if sf.delete_file is not None:
            why = "has a live deletion vector"
        elif f.partition_values is None:
            why = "has no partition tuple"
        elif f.spec_id != spec.spec_id:
            why = f"is under spec {f.spec_id}, not the live {spec.spec_id}"
        if why or f.partition_values is None:
            raise Refused(3, f"census: {f.path} {why}")
        units.setdefault(f.partition_values, []).append(f)
    out: list[Unit] = []
    for values, files in sorted(units.items(), key=lambda kv: json.dumps(kv[0])):
        rows = _file_rows(files)
        uid = uuid.uuid5(uuid.NAMESPACE_URL, json.dumps(values)).hex[:16]
        out.append(
            {"id": uid, "values": list(values), "team": values[team_i], "files": rows}
        )
        out[-1].update(rows=sum(r[2] for r in rows), bytes=sum(r[3] for r in rows))
    loads = [0] * ctx.cfg.shards
    for unit in sorted(out, key=lambda u: (-u["bytes"], u["id"])):
        unit["shard"] = loads.index(min(loads))
        loads[unit["shard"]] += unit["bytes"]
    return {"snapshot": src.read_snapshot_id, "time": started, "units": out}


def _file_ids(census: dict[str, Any]) -> set[int]:
    return {f[0] for unit in census["units"] for f in unit["files"]}


def plan(ctx: Ctx, *, settle_s: float, accept: set[str]) -> int:
    check_shapes(ctx.info("source"), ctx.info("dest"), ctx.cfg.variant_sources)
    first = take_census(ctx)
    _sleep(settle_s)
    census = take_census(ctx)
    if _file_ids(first) != _file_ids(census):
        raise Refused(3, f"settle: the census changed within {settle_s:g} s")
    ns, name = ctx.cfg.source
    params = {"namespace": ns, "table": name, "limit": len(accept) + 1}
    debt = ctx.req("GET", "/stats/partitions", params=params)
    sampled = debt.get("sampled_at")
    if sampled is None or _iso(sampled) <= first["time"]:
        raise Refused(3, f"settle: no debt sample after the census ({sampled})")
    for p in debt["partitions"]:
        key = ",".join(str(v["value"]) for v in p["partition_values"])
        if p["debt_score"] > 0 and key not in accept:
            raise Refused(3, f"settle: partition {key} has debt {p['debt_score']}")
    census["committing"] = ctx.commit
    if ctx.commit:
        if any("/c26-" in sf.data_file.path for sf in ctx.scan("dest", None)):
            raise Refused(3, "lock: the destination already holds /c26- files")
        if not ctx.scratch.put("RUN", ctx.cfg.run.encode(), create=True):
            ctx.check_lock()
    if not ctx.save("census.json.gz", census, create=True):
        raise Refused(3, f"run {ctx.cfg.run} already has a census")
    units = census["units"]
    empty: Unit = {"values": None, "rows": 0}
    big = max(units, key=lambda u: u["rows"], default=empty)
    log.info(
        "census at snapshot %s: %d units, %d rows; the largest, %s, has %d rows "
        "(at most %.1f pod-hours)",
        *(census["snapshot"], len(units), sum(u["rows"] for u in units)),
        *(big["values"], big["rows"], big["rows"] * _CPU_MS_PER_ROW / 3.6e6),
    )
    return 0


# -- copy: read by field id, map, write, tally the stored objects ------------------


def _sig(c: hl.Column) -> Any:
    kids = tuple((k.name, _sig(k)) for k in c.children or ())
    return c.type, json.dumps(c.type_params, sort_keys=True), kids


def check_shapes(
    src: hl.TableInfo, dest: hl.TableInfo, sources: Mapping[str, str]
) -> None:
    """Every destination column has a source column of its type (a mapped
    variant, a text column), and every source column a destination one."""
    try:
        fl._variant_plans(dest.columns, sources)
        fl.arrow_schema(dest.columns)
        if fl._placement(dest) is None:
            raise Refused(2, "the destination has no object_prefix_field")
    except (UnsupportedColumnError, fl.FlushError) as e:
        raise Refused(2, str(e)) from e
    by_name = {c.name: c for c in src.columns}
    for c in dest.columns:
        s = by_name.get(sources.get(c.name, c.name))
        text = c.name in sources and s is not None and s.type == "string"
        if not text and (s is None or c.name in sources or _sig(s) != _sig(c)):
            raise Refused(2, f"no source column of the type of {c.name!r}")
    if extra := set(by_name) - {c.name for c in dest.columns}:
        raise Refused(2, f"source columns {sorted(extra)} are not in the destination")


def _read(ctx: Ctx, path: str, src: hl.TableInfo, wanted: list[hl.Column]) -> pa.Table:
    """``wanted`` from one object, matched by field id: a renamed column
    reads its old name, a column added after the file reads as null."""
    with pq.ParquetFile(ctx.fs.open_input_file(s3_key(path))) as pf:
        ids: dict[int, str] = {}
        for field in pf.schema_arrow:
            if (fid := (field.metadata or {}).get(b"PARQUET:field_id")) is None:
                raise Halt(f"{path}: column {field.name!r} has no field id")
            ids[int(fid)] = field.name
        if missing := [n for n in (TEAM, TS) if _col(src, n).field_id not in ids]:
            raise Halt(f"{path} lacks the field ids of {missing}")
        data = pf.read(columns=[ids[c.field_id] for c in wanted if c.field_id in ids])
    arrays = [
        data.column(ids[c.field_id])
        if c.field_id in ids
        else pa.nulls(data.num_rows, column_to_arrow_field(c, variant="json").type)
        for c in wanted
    ]
    return pa.table(arrays, names=[c.name for c in wanted])


def _to_dest(data: pa.Table, schema: pa.Schema, sources: Mapping[str, str]) -> pa.Table:
    arrays = []
    for field in schema:
        if field.name in sources:  # "" and null are SQL NULL, as variant_text
            text = data.column(sources[field.name]).cast(pa.string())
            text = pc.if_else(pc.equal(text, ""), pa.scalar(None, pa.string()), text)
            arrays.append(text.cast(field.type))
        else:
            arrays.append(data.column(field.name).cast(field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


def _tally(data: pa.Table) -> Counter[str]:
    """Rows per ``<team_id>|<day of timestamp>``."""
    days = pc.cast(data.column(TS), pa.date32())
    keyed = pa.table({"t": data.column(TEAM), "d": days})
    rows = keyed.group_by(["t", "d"]).aggregate([([], "count_all")]).to_pylist()
    return Counter({f"{r['t']}|{r['d']}": r["count_all"] for r in rows})


def _placeable(tally: Counter[str]) -> Counter[str]:
    """The tally less the rows no team prefix holds (null or negative)."""
    return Counter({k: n for k, n in tally.items() if k.split("|")[0].isdigit()})


def _chunks(files: list[list[Any]], limit: int) -> Iterator[list[list[Any]]]:
    """Files in id order, packed into chunks of at most ``limit`` bytes."""
    chunk: list[list[Any]] = []
    for f in files:
        if chunk and sum(c[3] for c in chunk) + f[3] > limit:
            yield chunk
            chunk = []
        chunk.append(f)
    if chunk:
        yield chunk


def _build(
    ctx: Ctx, unit: Unit, rec: Rec, src: hl.TableInfo, dest: hl.TableInfo
) -> list[dict[str, Any]]:
    """Each chunk read, mapped, split, written, uploaded and read back from
    the store: the registrations; the tallies and measures go in ``rec``."""
    sources, cfg = ctx.cfg.variant_sources, ctx.cfg
    schema = fl._encode_schema(fl.arrow_schema(dest.columns))
    names = {c.name for c in dest.columns} - set(sources) | set(sources.values())
    wanted = [c for c in src.columns if c.name in names]
    placement, sort_keys = fl._placement(dest), fl._spec_sort_keys(dest)
    spec = dest.partition_spec
    assert placement is not None and spec is not None  # check_shapes
    version = f"v{rec['version']}a{rec['attempt']}-{rec['nonce']}"
    build = f"c26-{cfg.run}-{unit['id']}-{version}"
    src_tally: Counter[str] = Counter()
    dest_tally: Counter[str] = Counter()
    regs: list[dict[str, Any]] = []
    report, out_bytes, cpu = hl.VariantReport(), 0, time.process_time()
    files = rec["files"] if "files" in rec else unit["files"]
    for chunk in _chunks(files, cfg.chunk_bytes):
        read = [_to_dest(_read(ctx, f[1], src, wanted), schema, sources) for f in chunk]
        data = pa.concat_tables(read)
        src_tally += _tally(data)
        data = data.filter(pc.fill_null(pc.greater_equal(data[TEAM], 0), False))
        if sort_keys:
            data = fl._sort_batch_by_spec(data, sort_keys)
        for values, part in fl._partition_groups(data, dest, spec):
            written = hl.write_part(
                part,
                dest.columns,
                write_options=fl._WRITE_OPTIONS,
                variant_on_invalid="null",
                variant_report=report,
            )
            uri = fl._placed_uri(
                written,
                dest,
                placement,
                values,
                data_path=ctx.catalog.data_path,
                build=build,
                index=len(regs),
            )
            reg = written.registration(uri, dest.columns, stats=True)
            regs.append({**reg, "partition_values": list(values)})
            if ctx.commit:  # the stored object, not the buffer, is tallied
                upload = Upload(uri=uri, size=written.size, body=written.body)
                perform_upload(ctx.fs, ctx.put, upload)
                stored = ctx.fs.open_input_file(s3_key(uri))
            else:
                stored = pa.BufferReader(written.body)
            dest_tally += _tally(pq.read_table(stored, columns=[TEAM, TS]))
            out_bytes += written.size
    if dest_tally != _placeable(src_tally):
        raise Halt("the stored objects' tally is not the source's less its drops")
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    rec.update(src_tally=src_tally, dest_tally=dest_tally, max_rss=rss)
    rec.update(drops=src_tally.total() - dest_tally.total())
    rec.update(invalid=dict(report.invalid_by_reason))
    rec.update(cpu_s=time.process_time() - cpu, bytes_out=out_bytes)
    rec.update(bytes_in=sum(f[3] for f in files))
    return regs


# -- copy: attempts, commit, trust window, registration, repair, resolve -----------


def _shape(info: hl.TableInfo) -> Any:
    return info.columns, info.partition_spec, info.sort_spec, info.object_prefix_field


def _new_attempt(ctx: Ctx, unit: Unit, rec: Rec, src: hl.TableInfo) -> None:
    """Build and upload a new attempt (``building``, then ``sent``), again
    when the destination's shape changed under the build."""
    cfg = ctx.cfg
    for _ in range(3):
        dest = ctx.info("dest")
        rec.update(state="building", attempt=rec["attempt"] + 1)
        rec["nonce"] = secrets.token_hex(4)  # recorded before any upload
        ctx.save_unit(rec)
        regs = _build(ctx, unit, rec, src, dest)
        again = ctx.info("dest")
        if _shape(again) != _shape(dest):
            continue
        if not regs:  # every row dropped: nothing to commit
            rec.update(state="done", registered=0, done_at=_now())
            return ctx.save_unit(rec)
        run_ns = uuid.uuid5(
            uuid.NAMESPACE_URL, f"millrace-copy/{cfg.catalog}/{cfg.run}"
        )
        key = uuid.uuid5(run_ns, f"{unit['id']}/v{rec['version']}")
        ns, name = cfg.dest
        append: dict[str, Any] = {"namespace": ns, "table": name, "files": regs}
        append["expected_table_uuid"] = cfg.dest_uuid
        records = sum(r["record_count"] for r in regs)
        message = f"millrace-copy run={cfg.run} unit={unit['id']} records={records}"
        payload: dict[str, Any] = {
            "idempotency_key": str(key),
            "read_snapshot": again.read_snapshot_id,
        }
        payload.update(appends=[append], author=f"millrace-copy/{cfg.run}")
        payload["message"] = f"{message} v={rec['version']} a={rec['attempt']}"
        rec.update(state="sent", sent_at=_now(), sent_attempt=rec["attempt"])
        rec.update(dead=False, payload=payload)
        return ctx.save_unit(rec)
    raise Halt("the destination's shape changed under 3 builds")


def _commit(ctx: Ctx, payload: dict[str, Any]) -> int:
    """The commit's snapshot. A 5xx, 408, 429, a plain conflict or a lost
    connection resends the identical bytes under the same key."""
    for n in range(_COMMIT_TRIES):
        try:
            return int(ctx.catalog.commit_prepared(payload).snapshot_id)
        except hl.DdlSinceReadSnapshotError:
            raise
        except (hl.CommitConflictError, httpx.TransportError):
            pass
        except hl.HoglakeError as e:
            status = e.status_code or 0
            if status < 500 and status not in (408, 429):
                raise
        _sleep(min(30.0, 0.5 * 2**n))
    # Still ambiguous: the record stays ``sent``, and the next run resends.
    raise RuntimeError(f"commit {payload['idempotency_key']} is still ambiguous")


def _dead(rec: Rec, proof: str) -> None:
    """Records that the last sent attempt can never land, and the proof."""
    proof_rec = {"attempt": rec["sent_attempt"], "proof": proof, "at": _now()}
    rec.setdefault("proofs", []).append(proof_rec)
    rec.update(state="dead", dead=True)


def _send(ctx: Ctx, rec: Rec) -> None:
    ctx.check_lock()
    try:
        snapshot = _commit(ctx, rec["payload"])
    except (hl.DdlSinceReadSnapshotError, hl.ExpiredError) as e:
        # The server answers a used key from its receipt before it checks
        # the read basis, and the receipt outlives the trust window: inside
        # it, this refusal proves the attempt never landed (design §4).
        if _now() - rec["sent_at"] >= ctx.cfg.trust_s:
            rec.update(state="unresolved", why=f"{e.message} past the trust window")
            log.error("unit %s is unresolved: copy --resolve %s", *[rec["unit"]] * 2)
        else:
            _dead(rec, f"{e.status_code} {e.message}")
        return ctx.save_unit(rec)
    except hl.HoglakeError as e:
        raise Halt(f"commit refused: {e}") from e
    _finish(ctx, rec, snapshot)


def _finish(ctx: Ctx, rec: Rec, snapshot: int) -> None:
    """Checks what the server registered at the commit snapshot, or at head
    once it expired, where a merged path is ``registration_unchecked``."""
    regs = rec["payload"]["appends"][0]["files"]
    want = {r["path"]: r["record_count"] for r in regs}
    tuples = {tuple(r["partition_values"]) for r in regs}

    def listed(at: int | None) -> dict[str, int]:
        found = [f for t in tuples for f in ctx.files("dest", at, dict(enumerate(t)))]
        return {f.path: f.record_count for f in found if f.path in want}

    try:
        got, unchecked = listed(snapshot), []
        if got != want:
            raise Halt(f"snapshot {snapshot} does not register the files as sent")
    except hl.ExpiredError:
        got = listed(None)
        unchecked = sorted(set(want) - set(got))
        if any(want[p] != n for p, n in got.items()):
            raise Halt(
                f"head registers snapshot {snapshot}'s files otherwise"
            ) from None
    registered = sum(got.values()) + sum(want[p] for p in unchecked)
    rec.update(state="done", snapshot=snapshot, registered=registered)
    rec.update(registration_unchecked=unchecked, done_at=_now())
    ctx.save_unit(rec)


def _repair(ctx: Ctx, unit: Unit, rec: Rec, why: str) -> None:
    """A census object is gone: the unit is re-listed at head as version
    v+1, allowed once no attempt of version v can land."""
    if rec.get("sent_attempt") is not None and not rec.get("dead"):
        raise Halt(f"{why}, and attempt {rec['sent_attempt']} may still land")
    if rec["repairs"] >= _MAX_REPAIRS:
        raise Halt(f"{why}, after {_MAX_REPAIRS} repairs")
    values = unit["values"]
    found = ctx.files("source", None, dict(enumerate(values)))
    files = _file_rows([f for f in found if list(f.partition_values or ()) == values])
    log.warning("unit %s %s: %s; repaired at head", unit["id"], values, why)
    rec.update(version=rec["version"] + 1, attempt=0, repairs=rec["repairs"] + 1)
    rec.update(sent_attempt=None, dead=False, files=files)
    rec["rows"] = sum(f[2] for f in files)
    ctx.save_unit(rec)


def copy_unit(ctx: Ctx, unit: Unit, src: hl.TableInfo) -> str:
    rec = ctx.load(f"units/{unit['id']}.json")
    rec = rec or {"unit": unit["id"], "values": unit["values"], "state": "new"}
    rec.update({k: rec.get(k, 0) for k in ("version", "attempt", "repairs")})
    retries = 0
    for _ in range(_STEPS_PER_RUN):
        if rec["state"] in _FINAL:
            break
        try:
            if rec["state"] == "sent":
                _send(ctx, rec)
                continue
            try:
                _new_attempt(ctx, unit, rec, src)
            except FileNotFoundError as e:
                _repair(ctx, unit, rec, f"source object gone ({e})")
        except (Halt, fl.FlushError, hl.ValidationError) as e:
            rec.update(state="halted", why=str(e))
            ctx.save_unit(rec)
            log.error("unit %s %s halted: %s", unit["id"], unit["values"], e)
        except (httpx.TransportError, OSError, hl.HoglakeError) as e:
            # Every step is safe to repeat: the record says where it stands.
            status = getattr(e, "status_code", None)
            other = status is not None and status < 500 and status not in (408, 429)
            if other or retries >= _RETRIES:
                raise
            retries += 1
            log.warning("unit %s: %s; retry %d", unit["id"], e, retries)
            _sleep(min(60.0, 5.0 * 2**retries))
    if rec["state"] not in (*_FINAL, "sent"):  # resending a sent unit is safe
        rec.update(state="halted", why=f"not done after {_STEPS_PER_RUN} steps")
        ctx.save_unit(rec)
    return str(rec["state"])


def copy(ctx: Ctx, *, shard: int | None, limit: int | None, teams: set[str]) -> int:
    census = ctx.load("census.json.gz")
    if census is None:
        raise Refused(3, f"run {ctx.cfg.run} has no census: run plan first")
    if ctx.commit and not census["committing"]:
        raise Refused(3, "a dry run's census: only copy --no-commit may read it")
    if ctx.commit:
        ctx.check_lock()
    src = ctx.info("source")
    check_shapes(src, ctx.info("dest"), ctx.cfg.variant_sources)
    units = [u for u in census["units"] if shard is None or u["shard"] == shard]
    units.sort(key=lambda u: u["team"] not in teams)
    states: Counter[str] = Counter()
    for unit in units[:limit]:
        states[copy_unit(ctx, unit, src) if ctx.commit else _dry(ctx, unit, src)] += 1
    log.info("shard %s: %s", shard, dict(states))
    return 3 if states["halted"] or states["unresolved"] else int(states["sent"] > 0)


def _dry(ctx: Ctx, unit: Unit, src: hl.TableInfo) -> str:
    """Builds one unit without uploading, and logs what it cost."""
    rec: Rec = {"unit": unit["id"], "version": 0, "attempt": 0, "nonce": "dry"}
    try:
        _build(ctx, unit, rec, src, ctx.info("dest"))
    except (Halt, fl.FlushError, hl.ValidationError, FileNotFoundError) as e:
        log.error("dry unit %s %s: %s", unit["id"], unit["values"], e)
        return "halted"
    rows = max(1, rec["src_tally"].total())
    log.info(
        "dry unit %s %s: %d rows, %.3f cpu-ms/row, %.2f bytes out per byte in, "
        "max rss %d, %d drops, invalid variants %s",
        *(unit["id"], unit["values"], rows, rec["cpu_s"] * 1e3 / rows),
        *(rec["bytes_out"] / max(1, rec["bytes_in"]), rec["max_rss"]),
        *(rec["drops"], rec["invalid"]),
    )
    return "dry"


def resolve(ctx: Ctx, unit_id: str) -> int:
    """An ``unresolved`` unit, by its receipt while it is surely kept."""
    rec = ctx.load(f"units/{unit_id}.json")
    if rec is None or rec["state"] != "unresolved":
        raise Refused(2, f"unit {unit_id} is not unresolved")
    if _now() >= rec["sent_at"] + ctx.cfg.retention_s:
        raise Refused(3, f"unit {unit_id}'s receipt may be purged: see the runbook")
    try:
        receipt = ctx.req(
            "GET", f"/commit/receipts/{rec['payload']['idempotency_key']}"
        )
    except hl.NotFoundError:
        _dead(rec, "receipt 404 (copy --resolve)")  # the next copy builds anew
        ctx.save_unit(rec)
        return 0
    _finish(ctx, rec, receipt["snapshot_id"])
    return 0


def rearm(ctx: Ctx, unit_id: str) -> int:
    """A ``halted`` unit back to a new attempt, once nothing sent can land."""
    rec = ctx.load(f"units/{unit_id}.json")
    if rec is None or rec["state"] != "halted":
        raise Refused(2, f"unit {unit_id} is not halted")
    if rec.get("sent_attempt") is not None and not rec.get("dead"):
        raise Refused(3, f"unit {unit_id}: attempt {rec['sent_attempt']} may land")
    rec.update(state="rearmed", rearmed_from=rec.get("why"), repairs=0)
    ctx.save_unit(rec)
    return 0


# -- verify ------------------------------------------------------------------------


def _month(month: int) -> float:
    return datetime(1970 + month // 12, month % 12 + 1, 1, tzinfo=UTC).timestamp()


def _span(info: hl.TableInfo, values: Key) -> tuple[float, float] | None:
    """A partition's time range [start, end) in epoch seconds, from its
    temporal field; None when it has none."""
    assert info.partition_spec is not None
    for field, v in zip(info.partition_spec.fields, values, strict=True):
        if v is None or field.transform not in ("hour", "day", "month", "year"):
            continue
        n, t = int(v), field.transform
        if t in ("hour", "day"):
            step = 3600 if t == "hour" else 86400
            return n * step, (n + 1) * step
        m, months = (n, 1) if t == "month" else (12 * n, 12)
        return _month(m), _month(m + months)
    return None


def verify(ctx: Ctx, *, fallback_at: float | None) -> int:
    census = ctx.load("census.json.gz")
    if census is None:
        raise Refused(3, f"run {ctx.cfg.run} has no census")
    units = census["units"]
    recs = {u["id"]: _slim(ctx.load(f"units/{u['id']}.json") or {}) for u in units}
    states = Counter(r.get("state", "new") for r in recs.values())
    fails: list[str] = []
    flags: list[str] = []
    rows: dict[str, list[int]] = {}
    if states["done"] != len(units):
        fails.append(f"incomplete: {dict(states)}")
    else:
        _check_landed(units, recs, rows, fails)
        _check_dest(ctx, units, recs, fallback_at, fails, flags)
        _check_source(ctx, units, recs, fails)
    invalid: Counter[str] = Counter()
    for r in recs.values():
        invalid.update(r.get("invalid", {}))
    repaired = [
        {"values": u["values"], "census_rows": u["rows"], **recs[u["id"]]}
        for u in units
        if "files" in recs[u["id"]]
    ]
    keep = ("unit", "values", "version", "census_rows", "rows", "files")
    repaired = [{k: r[k] for k in keep} for r in repaired]
    report = {"run": ctx.cfg.run, "ok": not fails, "fails": fails[:500], "flags": flags}
    report.update(fail_count=len(fails), states=dict(states), variant_invalid=invalid)
    report.update(census_snapshot=census["snapshot"], census_time=census["time"])
    report.update(end_time=_now(), repaired=repaired)
    unchecked = [p for r in recs.values() for p in r.get("registration_unchecked", ())]
    report["registration_unchecked"] = len(unchecked)
    ctx.save("verify.json", report)
    lines = [f"{k.replace('|', ',')},{s},{d}\n" for k, (s, d) in sorted(rows.items())]
    csv = "team_id,day,source_rows,copied_rows\n" + "".join(lines)
    ctx.scratch.put(f"{ctx.cfg.run}/tally.csv", csv.encode())
    for line in fails[:50] + flags[:50]:
        log.warning("%s", line)
    verdict = "FAILED" if fails else "passed"
    log.info("verify %s: %d failures, %d flagged", verdict, len(fails), len(flags))
    return 1 if fails else 0


def _slim(rec: Rec) -> Rec:
    """A record without its files' stats: verify holds every record."""
    if "payload" in rec:
        keep = ("path", "record_count", "partition_values")
        files = [{k: f[k] for k in keep} for f in rec["payload"]["appends"][0]["files"]]
        rec["payload"] = {"appends": [{"files": files}]}
    return rec


def _check_landed(
    units: list[Unit],
    recs: dict[str, Rec],
    rows: dict[str, list[int]],
    fails: list[str],
) -> None:
    """Per unit, the stored objects' tally and the server's registered rows
    account for the census rows, less drops that a null team explains."""
    for u in units:
        r, where = recs[u["id"]], f"unit {u['id']} {u['values']}"
        src: Counter[str] = Counter(r["src_tally"])
        dest: Counter[str] = Counter(r["dest_tally"])
        expected = r.get("rows", u["rows"])
        counts = (expected, src.total(), dest.total(), r["registered"])
        if counts[0] != counts[1] or counts[2] != counts[3] or dest != _placeable(src):
            fails.append(f"{where}: census, read, copied, registered = {counts}")
        if r["drops"] and u["team"] is not None:
            fails.append(f"{where}: {r['drops']} rows dropped (object_prefix_value)")
        for k in src.keys() | dest.keys():
            pair = rows.setdefault(k, [0, 0])
            pair[0] += src[k]
            pair[1] += dest[k]


def _check_dest(
    ctx: Ctx,
    units: list[Unit],
    recs: dict[str, Rec],
    fallback_at: float | None,
    fails: list[str],
    flags: list[str],
) -> None:
    """No foreign or unrecorded copy file at head, and every partition holds
    at least what was copied into it, and less than twice that."""
    dest = ctx.info("dest")
    team_i = _team_index(dest)
    paths: set[str] = set()
    expect: Counter[Key] = Counter()
    committed: dict[Key, list[float]] = {}
    for r in recs.values():
        for f in r["payload"]["appends"][0]["files"] if "payload" in r else ():
            key = tuple(f["partition_values"])
            paths.add(f["path"])
            expect[key] += f["record_count"]
            committed.setdefault(key, []).append(r["done_at"])
    have: Counter[Key] = Counter()
    for team in sorted({u["team"] for u in units if u["team"] is not None}):
        for f in ctx.files("dest", None, {team_i: team}):
            if "/c26-" in f.path and f.path not in paths:
                fails.append(f"a copy file of another run, or unrecorded: {f.path}")
            have[tuple(f.partition_values or ())] += f.record_count
    start, end = ctx.cfg.s_time - 7 * 86400, _now()
    for key, want in sorted(expect.items(), key=str):
        got, where = have[key], f"partition {list(key)}"
        if got < want:
            early = fallback_at is not None and min(committed[key]) < fallback_at
            (flags if early else fails).append(f"{where}: {got} rows < {want} copied")
        if got >= 2 * want:
            span = _span(dest, key)
            late = span is None or (span[1] > start and span[0] <= end)
            (flags if late else fails).append(f"{where}: {got} rows >= 2x {want}")


def _check_source(
    ctx: Ctx, units: list[Unit], recs: dict[str, Rec], fails: list[str]
) -> None:
    """The source at head: no DV, no file outside the census, and no unit
    holding more rows than were copied from it."""
    by_values = {tuple(u["values"]): u for u in units}
    rows: Counter[Key] = Counter()
    for sf in ctx.scan("source", None):
        f, key = sf.data_file, tuple(sf.data_file.partition_values or ())
        if sf.delete_file is not None:
            fails.append(f"source file {f.path} has a deletion vector")
        if key not in by_values:
            fails.append(f"source file {f.path} is in no census unit")
        rows[key] += f.record_count
    for key, u in by_values.items():
        r = recs[u["id"]]
        if rows[key] > (copied := r["rows"] if "rows" in r else u["rows"]):
            fails.append(f"source {list(key)} holds {rows[key]} > {copied} rows")


# -- CLI ---------------------------------------------------------------------------


def build_ctx(env: Mapping[str, str]) -> Ctx:
    cfg = load_config(env)
    s3 = hl.S3Config(**_s3_kwargs(env))
    if (put := s3.put_client(8)) is None:
        raise Refused(2, "boto3 is required: the records are conditional S3 writes")
    fs = s3.filesystem()  # type: ignore[no-untyped-call]
    # /scan of a settled prod table is one long, unpaged answer.
    return Ctx(cfg, hl.HoglakeClient(cfg.url, s3=s3, timeout=600.0), fs, put)


def main(argv: list[str] | None = None, *, ctx: Ctx | None = None) -> int:
    parser = argparse.ArgumentParser(prog="millrace-copy")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--no-commit", action="store_true")
    p.add_argument("--settle-window", type=float, default=3600.0)
    p.add_argument("--accept-debt", action="append", default=[], metavar="VALUES")
    c = sub.add_parser("copy")
    c.add_argument("--no-commit", action="store_true")
    c.add_argument("--shard", type=int, default=os.environ.get("JOB_COMPLETION_INDEX"))
    c.add_argument("--resolve", metavar="UNIT")
    c.add_argument("--rearm", metavar="UNIT")
    c.add_argument("--limit", type=int)
    c.add_argument("--teams", default="")
    sub.add_parser("verify").add_argument("--fallback-run-at", type=_iso)
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)  # a line per request
    try:
        ctx = ctx or build_ctx(os.environ)
        ctx.commit = not getattr(args, "no_commit", False)
        if args.cmd == "plan":
            return plan(ctx, settle_s=args.settle_window, accept=set(args.accept_debt))
        if args.cmd == "verify":
            return verify(ctx, fallback_at=args.fallback_run_at)
        if args.resolve:
            return resolve(ctx, args.resolve)
        if args.rearm:
            return rearm(ctx, args.rearm)
        teams = {t.strip() for t in args.teams.split(",") if t.strip()}
        return copy(ctx, shard=args.shard, limit=args.limit, teams=teams)
    except Refused as e:
        log.error("%s", e)
        return e.code


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
