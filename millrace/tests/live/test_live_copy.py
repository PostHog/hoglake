"""2.6: millrace-copy against the real server and MinIO.

The source is shaped like ``ingest.events``: option off,
``identity(team_id), day(timestamp)``, three teams over three days, and a
``properties`` text column holding JSON, ``""``, null and a lone surrogate
escape. One column is renamed after the first files were written. The
destination is shaped like ``ingest.events_v2``: the option on, and
``properties_sv`` with the 16-key declaration.

``plan``; then server compaction merges one partition's census files (its
output carries ``_hog_row_id``) and one input object is removed, as
cleanup would, so the copy repairs that unit from head; ``copy`` killed
once between a commit and its ``done`` record, ``copy`` again, a rerun,
server compaction of the copy's files, and ``verify``. Each row lands
exactly once, under its team's prefix, with ``properties_sv`` NULL for the
invalid value; the rerun commits nothing; and a second run is refused by
the lock. The harness runs no maintenance
summary, so the settle gate's debt read is stubbed (test_copy.py covers
it).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import livekit
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyhoglake import object_prefix, ops
from pyhoglake.variant import to_python
from test_live_variant import DECLARATION

from millrace import copy as cp

pytestmark = pytest.mark.live

TEAMS = (4, 42, 7)
DAY0 = 20454  # 2026-01-01
DAY_US = 86_400_000_000
PROPS = ['{"$browser":"Chrome"}', "", None, '{"$os":"\\ud800"}']


class Crash(BaseException):
    pass


def _rows(seqs: range, team: int, day: int, event: str) -> pa.Table:
    return pa.table(
        {
            "team_id": pa.array([team] * len(seqs), pa.int64()),
            "timestamp": pa.array(
                [(DAY0 + day) * DAY_US + s for s in seqs], pa.timestamp("us", tz="UTC")
            ),
            "seq": pa.array(list(seqs), pa.int64()),
            event: [f"e{s}" for s in seqs],
            "properties": [PROPS[s % len(PROPS)] for s in seqs],
        }
    )


def _table(lake_home: Any, name: str, event: str) -> Any:
    schema = pa.schema(
        [
            pa.field("team_id", pa.int64(), nullable=False),
            pa.field("timestamp", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("seq", pa.int64()),
            pa.field(event, pa.string()),
            pa.field("properties", pa.string()),
        ]
    )
    table = lake_home.namespace.create_table(name, schema)
    lake_home.tables.append(table)
    ids = {c.name: c.field_id for c in table.columns}
    spec = [ops.partition_field(ids["team_id"], "identity")]
    spec.append(ops.partition_field(ids["timestamp"], "day"))
    table.alter([ops.set_partition_spec(spec)])
    return table


def _compact(lake_home: Any) -> dict[str, Any]:
    path = f"/catalogs/{lake_home.catalog.name}/maintenance/compact"
    return dict(lake_home.client._request("POST", path))


def test_the_copy_lands_every_row_once_and_resumes_exactly(
    lake_home: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    src = _table(lake_home, "events", "ev")
    want: dict[int, tuple[int, int, str | None]] = {}  # seq -> team, day, props
    seq = 0

    def append(team: int, day: int, n: int, event: str = "event") -> None:
        nonlocal seq
        seqs = range(seq, seq + n)
        src.append(_rows(seqs, team, day, event), compression="zstd")
        want.update({s: (team, DAY0 + day, PROPS[s % len(PROPS)]) for s in seqs})
        seq += n

    for team in TEAMS:
        for day in range(3):
            append(team, day, 4, "ev")
    src.alter([ops.rename_column("ev", "event")])
    for _ in range(5):  # six small files each in (4, day 0) and (42, day 1)
        append(4, 0, 2)
        append(42, 1, 2)

    dest = _table(lake_home, "events_v2", "event")
    dest.alter([ops.AlterOp("set_object_prefix_field", {"name": "team_id"})])
    dest.alter([ops.add_column("properties_sv", "variant", shredding=DECLARATION)])

    env = {
        "HOGLAKE_URL": livekit.HOGLAKE_URL,
        "HOGLAKE_S3_ENDPOINT": livekit.S3_ENDPOINT,
        "HOGLAKE_S3_ACCESS_KEY": livekit.S3_ACCESS_KEY,
        "HOGLAKE_S3_SECRET_KEY": livekit.S3_SECRET_KEY,
        "HOGLAKE_S3_REGION": livekit.S3_REGION,
        "COPY_CATALOG": lake_home.catalog.name,
        "COPY_SOURCE": "ns1.events",
        "COPY_SOURCE_UUID": src.table_uuid,
        "COPY_DEST": "ns1.events_v2",
        "COPY_DEST_UUID": dest.table_uuid,
        "COPY_RUN": "live1",
        "COPY_SCRATCH_URL": f"s3://{livekit.BUCKET}/{livekit.RUN_ID}/copy-2.6/"
        f"{lake_home.catalog.name}",
        "COPY_CHUNK_BYTES": "1",  # a chunk per source file: several copy files
        "COPY_S_TIME": datetime.now(UTC).isoformat(),
    }
    request = cp.Ctx.req

    def stub_debt(self: cp.Ctx, method: str, suffix: str, **kw: Any) -> Any:
        if suffix == "/stats/partitions":
            return {"sampled_at": datetime.now(UTC).isoformat(), "partitions": []}
        return request(self, method, suffix, **kw)

    monkeypatch.setattr(cp.Ctx, "req", stub_debt)
    ctx = cp.build_ctx(env)
    assert cp.main(["plan", "--settle-window", "1"], ctx=ctx) == 0
    assert not ctx.scratch.put("RUN", b"other", create=True)  # MinIO's If-None-Match

    def files_of(table: Any, team: int, day: int) -> list[Any]:
        files = livekit.lake_files(table, lake_home.head())
        return [f for f in files if f.partition_values == (str(team), str(DAY0 + day))]

    census_files = files_of(src, 4, 0)
    assert len(census_files) == 6
    for _ in range(4):  # one group per sweep; (42, day 1)'s may go first
        if len(files_of(src, 4, 0)) == 1:
            break
        _compact(lake_home)
    assert len(files_of(src, 4, 0)) == 1  # merged after the census
    lake_home.fs.delete_file(census_files[0].path[len("s3://") :])  # as cleanup

    finish, calls = cp._finish, []

    def die_once(*args: Any) -> None:  # a kill between a commit and `done`
        if not calls:
            calls.append(1)
            raise Crash
        finish(*args)

    monkeypatch.setattr(cp, "_finish", die_once)
    with pytest.raises(Crash):
        cp.main(["copy"], ctx=ctx)
    assert cp.main(["copy"], ctx=ctx) == 0
    head = lake_home.head()
    assert cp.main(["copy"], ctx=ctx) == 0
    assert lake_home.head() == head  # the rerun commits nothing

    info = dest.info(totals=False)
    (sv,) = [c for c in info.columns if c.name == "properties_sv"]

    census = ctx.load("census.json.gz")
    (unit,) = [u for u in census["units"] if u["values"] == ["4", str(DAY0)]]
    repaired = ctx.load(f"units/{unit['id']}.json")
    assert repaired["version"] == 1 and repaired["rows"] == unit["rows"] == 14

    assert len(files_of(dest, 42, 1)) == 6
    for _ in range(8):  # one group per sweep; the source's (42, 1) may go first
        if len(files_of(dest, 42, 1)) == 1:
            break
        _compact(lake_home)
    assert len(files_of(dest, 42, 1)) == 1  # the copy files are merged away
    assert cp.main(["verify"], ctx=ctx) == 0

    got: dict[int, list[tuple[int, int, Any]]] = {}
    for f in livekit.lake_files(dest, lake_home.head()):
        team = int(f.partition_values[0])
        assert f.path.startswith(
            object_prefix.prefix(ctx.catalog.data_path, info, team)
        )
        data = pq.read_table(lake_home.fs.open_input_file(f.path[len("s3://") :]))
        days = data["timestamp"].cast(pa.date32()).cast(pa.int32()).to_pylist()
        values = to_python(data["properties_sv"], sv)
        for row in zip(
            data["seq"].to_pylist(), data["team_id"].to_pylist(), days, values
        ):
            got.setdefault(row[0], []).append(row[1:])
    assert set(got) == set(want) and all(len(v) == 1 for v in got.values())
    for s, ((team, day, value),) in got.items():
        props = want[s][2]
        assert (team, day) == want[s][:2]
        assert value == (
            None if props in (None, "", PROPS[3]) else {"$browser": "Chrome"}
        )

    other = cp.build_ctx({**env, "COPY_RUN": "live2"})
    assert cp.main(["plan", "--settle-window", "0"], ctx=other) == 3  # the lock
