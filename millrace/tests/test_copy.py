"""millrace-copy: config, the census and the settle gate, the run lock,
reads by field id, the properties_sv mapping, a new attempt's nonce,
repair, and the time span behind verify's cutover-window flags."""

from __future__ import annotations

import gzip
import json
from typing import Any

import copykit as ck
import pyarrow as pa
import pyhoglake as hl
import pytest
from copykit import DAY, Lake, seed

from millrace import copy as cp
from millrace import flush as fl


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cp, "_sleep", lambda s: None)


def run(ctx: cp.Ctx, *argv: str) -> int:
    return cp.main(list(argv), ctx=ctx)


def plan(lake: Lake, *extra: str, **env: str) -> int:
    return run(lake.ctx(**env), "plan", "--settle-window", "0", *extra)


def census(lake: Lake, run_id: str = "r1") -> dict[str, Any]:
    raw = lake.store.objects[f"scratch/copy/cat/{run_id}/census.json.gz"]
    return dict(json.loads(gzip.decompress(raw)))


@pytest.mark.parametrize(
    "env",
    [
        {"COPY_RECEIPT_TRUST_S": "604800"},  # not below the 7-day retention
        {"COPY_RECEIPT_RETENTION_S": "86400"},
        {"COPY_RUN": "R1"},
        {"COPY_RUN": "a-b"},
        {"COPY_SOURCE": "events"},
    ],
)
def test_config_refusals_exit_2(env: dict[str, str]) -> None:
    with pytest.raises(cp.Refused) as refused:
        Lake().ctx(**env)
    assert refused.value.code == 2


def test_the_census_is_deterministic_and_packs_the_largest_unit_first() -> None:
    lake = Lake()
    seed(lake)
    ctx = lake.ctx(COPY_SHARDS="2")
    first, second = cp.take_census(ctx), cp.take_census(ctx)
    assert first["units"] == second["units"]
    by_values = {tuple(u["values"]): u for u in first["units"]}
    big = by_values[("2", str(DAY))]
    assert big["rows"] == 4 and len(big["files"]) == 2 and big["shard"] == 0
    assert by_values[("3", str(DAY))]["shard"] == 1  # the least-loaded shard
    assert big["id"] == cp.uuid.uuid5(cp.uuid.NAMESPACE_URL, '["2", "20454"]').hex[:16]


@pytest.mark.parametrize("fault", ["dv", "no tuple", "old spec", "no identity"])
def test_the_census_refuses_what_it_cannot_copy(
    fault: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    lake = Lake()
    f = lake.write(2, [("a", None)])
    if fault == "dv":
        f["_dv"] = {"delete_file_id": 1, "data_file_id": f["data_file_id"]}
        f["_dv"].update(path="s3://x", file_format="puffin", delete_count=1)
        f["_dv"].update(file_size_bytes=1, begin_snapshot=1)
    elif fault == "no tuple":
        f["partition_values"] = None
    elif fault == "old spec":
        f["spec_id"] = 0
    else:
        spec = {"spec_id": 1, "fields": [{"source_field_id": 2, "transform": "day"}]}
        monkeypatch.setattr(ck, "SPEC", spec)
    with pytest.raises(cp.Refused) as refused:
        cp.take_census(lake.ctx())
    assert refused.value.code == 3


def test_the_settle_gate_refuses_a_census_that_changed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake = Lake()
    seed(lake)
    monkeypatch.setattr(cp, "_sleep", lambda s: lake.write(4, [("late", None)]))
    assert plan(lake) == 3
    assert "scratch/copy/cat/RUN" not in lake.store.objects


def test_the_settle_gate_reads_debt_and_accepts_listed_partitions() -> None:
    lake = Lake()
    seed(lake)
    values = [{"field": "team_id", "value": "2"}, {"field": "ts", "value": str(DAY)}]
    lake.debt["partitions"] = [{"partition_values": values, "debt_score": 3}]
    assert plan(lake) == 3
    assert plan(lake, "--accept-debt", f"2,{DAY}") == 0
    lake.debt["partitions"].append({"partition_values": values[:1], "debt_score": 1})
    assert plan(lake, "--accept-debt", f"2,{DAY}", COPY_RUN="r2") == 3


@pytest.mark.parametrize("sampled", [None, "2020-01-01T00:00:00+00:00"])
def test_the_settle_gate_refuses_a_debt_sample_older_than_the_census(
    sampled: str | None,
) -> None:
    lake = Lake()
    seed(lake)
    lake.debt["sampled_at"] = sampled
    assert plan(lake) == 3


def test_the_run_lock_refuses_another_run_and_a_live_copy_file() -> None:
    lake = Lake()
    seed(lake)
    assert plan(lake) == 0
    assert lake.store.objects["scratch/copy/cat/RUN"] == b"r1"
    assert plan(lake, COPY_RUN="r2") == 3  # RUN names r1
    assert run(lake.ctx(), "copy") == 0
    del lake.store.objects["scratch/copy/cat/RUN"]  # a human released it
    assert plan(lake, COPY_RUN="r2") == 3  # r1's /c26- files are live
    assert "scratch/copy/cat/RUN" not in lake.store.objects


def test_a_dry_run_census_cannot_commit_and_a_dry_copy_writes_nothing() -> None:
    lake = Lake()
    seed(lake)
    assert plan(lake, "--no-commit") == 0
    assert census(lake)["committing"] is False
    assert "scratch/copy/cat/RUN" not in lake.store.objects
    before = set(lake.store.objects)
    assert run(lake.ctx(), "copy") == 3
    assert run(lake.ctx(), "copy", "--no-commit", "--teams", "3", "--limit", "1") == 0
    assert set(lake.store.objects) == before and lake.commits == 0


def test_a_lock_changed_before_a_commit_stops_the_copy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake = Lake()
    seed(lake)
    assert plan(lake) == 0
    build = cp._build

    def steal(*args: Any) -> Any:
        lake.store.objects["scratch/copy/cat/RUN"] = b"r9"
        return build(*args)

    monkeypatch.setattr(cp, "_build", steal)
    assert run(lake.ctx(), "copy") == 3
    assert lake.commits == 0


def test_reads_match_columns_by_field_id() -> None:
    lake = Lake()
    renamed = lake.write(2, [("a", '{"k":1}')], rename={"event": "ev"})
    older = lake.write(2, [("b", None)], drop=("properties",))
    src = lake.info("events")
    ctx = lake.ctx()
    read = cp._read(ctx, renamed["path"], src, list(src.columns))
    assert read.column_names == ["team_id", "timestamp", "event", "properties"]
    assert read["event"].to_pylist() == ["a"]
    assert (
        cp._read(ctx, older["path"], src, list(src.columns))["properties"].null_count
        == 1
    )


@pytest.mark.parametrize("kwargs", [{"field_ids": False}, {"drop": ("team_id",)}])
def test_a_file_without_field_ids_halts_its_unit(kwargs: dict[str, Any]) -> None:
    lake = Lake()
    lake.write(2, [("a", None)], **kwargs)
    assert plan(lake) == 0
    assert run(lake.ctx(), "copy") == 3
    (unit,) = census(lake)["units"]
    assert lake.store.record(unit["id"])["state"] == "halted"


def test_a_destination_column_of_another_type_exits_2() -> None:
    lake = Lake()
    seed(lake)
    lake.tables["events_v2"]["columns"][2] = {**ck.COLS[2], "type": "int"}
    assert plan(lake) == 2


def test_properties_sv_is_null_for_empty_and_null_text() -> None:
    dest = Lake().info("events_v2")
    schema = fl._encode_schema(fl.arrow_schema(dest.columns))
    data = pa.table(
        {
            "team_id": pa.array([1, 1, 1], pa.int64()),
            "timestamp": pa.array([0, 1, 2], pa.timestamp("us", tz="UTC")),
            "event": ["a", "b", "c"],
            "properties": ['{"k":1}', "", None],
        }
    )
    out = cp._to_dest(data, schema, {"properties_sv": "properties"})
    assert out["properties_sv"].to_pylist() == ['{"k":1}', None, None]
    assert out["properties"].to_pylist() == ['{"k":1}', "", None]


def test_each_attempt_writes_under_its_own_nonce(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake = Lake()
    lake.write(2, [("a", None)])
    assert plan(lake) == 0
    build, calls = cp._build, []

    def ddl_once(*args: Any) -> Any:
        if calls:
            lake.snap += 1  # the DDL's own snapshot
        else:
            lake.ddl["events_v2"] = lake.snap + 1  # lands after the build's read
        calls.append(1)
        return build(*args)

    monkeypatch.setattr(cp, "_build", ddl_once)
    assert run(lake.ctx(), "copy") == 0
    (unit,) = census(lake)["units"]
    rec = lake.store.record(unit["id"])
    assert rec["attempt"] == 2 and rec["proofs"][0]["attempt"] == 1
    assert rec["proofs"][0]["proof"].startswith("409")
    assert lake.commits == 1
    names = {k.split("/")[-2] for k in lake.store.objects if "/c26-" in k}
    assert len(names) == 2 and {n.split("-")[3][:4] for n in names} == {"v0a1", "v0a2"}
    assert len({n.split("-")[4] for n in names}) == 2  # two nonces


def test_a_unit_that_cannot_land_halts_and_rearm_restarts_it() -> None:
    lake = Lake()
    lake.write(2, [("a", None)])
    assert plan(lake) == 0
    lake.ddl["events_v2"] = 10**9  # every commit: 409 inside the trust window
    assert run(lake.ctx(), "copy") == 3
    (unit,) = census(lake)["units"]
    rec = lake.store.record(unit["id"])
    assert rec["state"] == "halted" and "12 steps" in rec["why"] and rec["dead"]
    assert lake.commits == 0 and len(rec["proofs"]) == 6
    del lake.ddl["events_v2"]
    assert run(lake.ctx(), "copy", "--rearm", unit["id"]) == 0
    assert run(lake.ctx(), "copy") == 0 and lake.commits == 1
    assert run(lake.ctx(), "copy", "--rearm", unit["id"]) == 2  # not halted


def test_rearm_resets_the_repairs_of_a_unit_halted_after_three() -> None:
    lake = Lake()
    f = lake.write(2, [("a", None)])
    assert plan(lake) == 0
    body = lake.store.objects.pop(f["path"][5:])  # gone, and still listed
    assert run(lake.ctx(), "copy") == 3
    (unit,) = census(lake)["units"]
    assert "after 3 repairs" in lake.store.record(unit["id"])["why"]
    lake.store.objects[f["path"][5:]] = body
    assert run(lake.ctx(), "copy", "--rearm", unit["id"]) == 0
    assert lake.store.record(unit["id"])["repairs"] == 0
    assert run(lake.ctx(), "copy") == 0 and lake.commits == 1


def test_the_step_cap_leaves_a_sent_unit_to_resend() -> None:
    lake = Lake()
    gone = lake.write(2, [("a", None)])
    lake.write(2, [("b", None)])
    assert plan(lake) == 0
    del lake.store.objects[gone["path"][5:]]
    lake.end(gone)  # one repair, so the 12th step is a build that sends
    lake.ddl["events_v2"] = 10**9
    assert run(lake.ctx(), "copy") == 1  # not halted: the pod retries
    (unit,) = census(lake)["units"]
    assert lake.store.record(unit["id"])["state"] == "sent"
    del lake.ddl["events_v2"]
    assert run(lake.ctx(), "copy") == 0 and lake.commits == 1


def test_rearm_refuses_a_unit_whose_sent_attempt_may_land() -> None:
    lake = Lake()
    lake.write(2, [("a", None)])
    assert plan(lake) == 0
    lake.fail = [422]
    assert run(lake.ctx(), "copy") == 3
    (unit,) = census(lake)["units"]
    assert run(lake.ctx(), "copy", "--rearm", unit["id"]) == 3


def test_a_shape_that_changes_under_every_build_halts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake = Lake()
    lake.write(2, [("a", None)])
    assert plan(lake) == 0
    build, dest = cp._build, lake.tables["events_v2"]

    def alter(*args: Any) -> Any:
        dest["columns"] = [
            {**c, "comment": f"{c.get('comment')}+"} for c in dest["columns"]
        ]
        return build(*args)

    monkeypatch.setattr(cp, "_build", alter)
    assert run(lake.ctx(), "copy") == 3
    (unit,) = census(lake)["units"]
    rec = lake.store.record(unit["id"])
    assert rec["state"] == "halted" and "3 builds" in rec["why"]
    assert rec["attempt"] == 3 and lake.commits == 0


def test_a_gone_object_is_repaired_only_when_nothing_sent_can_land() -> None:
    lake = Lake()
    lake.write(2, [("a", None)])
    ctx = lake.ctx()
    unit = cp.take_census(ctx)["units"][0]
    rec = {"unit": unit["id"], "version": 0, "attempt": 1, "repairs": 0}
    cp._repair(ctx, unit, {**rec, "sent_attempt": 1, "dead": True}, "gone")
    for bad in ({"sent_attempt": 1, "dead": False}, {"repairs": 3}):
        with pytest.raises(cp.Halt):
            cp._repair(ctx, unit, {**rec, **bad}, "gone")


@pytest.mark.parametrize(
    ("transform", "value", "start", "end"),
    [
        ("day", "1", "1970-01-02T00:00", "1970-01-03T00:00"),
        ("hour", "25", "1970-01-02T01:00", "1970-01-02T02:00"),
        ("month", "11", "1970-12-01T00:00", "1971-01-01T00:00"),
        ("year", "56", "2026-01-01T00:00", "2027-01-01T00:00"),
    ],
)
def test_a_partitions_span(transform: str, value: str, start: str, end: str) -> None:
    spec = {"spec_id": 1, "fields": [{"source_field_id": 1, "transform": "identity"}]}
    spec["fields"].append({"source_field_id": 2, "transform": transform})
    wire = {"name": "t", "namespace": "n", "table_uuid": ck.SRC_UUID}
    info = hl.TableInfo.from_wire({**wire, "columns": ck.COLS, "partition_spec": spec})
    utc = "+00:00"
    assert cp._span(info, ("1", value)) == (cp._iso(start + utc), cp._iso(end + utc))
    assert cp._span(info, ("1", None)) is None
