"""millrace-copy end to end against copykit's fake gigahog: every row lands
once under its team's prefix, whatever step a crash interrupts; the
receipt trust window and ``copy --resolve``; and each refusal of
``verify``, with the cutover-window and fallback flags."""

from __future__ import annotations

import gzip
import io
import json
import time
from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pyhoglake as hl
import pytest
from copykit import DATA, DAY, Lake, seed
from pyhoglake.variant import to_python

from millrace import copy as cp

#: seed()'s rows per (team, day).
ROWS = {("2", str(DAY)): 4, ("2", str(DAY + 1)): 1, ("3", str(DAY)): 3}


class Crash(BaseException):
    """A pod killed mid-step: nothing in the tool catches it."""


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cp, "_sleep", lambda s: None)


def run(ctx: cp.Ctx, *argv: str) -> int:
    return cp.main(list(argv), ctx=ctx)


def planned(**env: str) -> tuple[Lake, cp.Ctx]:
    lake = Lake()
    seed(lake)
    ctx = lake.ctx(**env)
    assert run(ctx, "plan", "--settle-window", "0") == 0
    return lake, ctx


def units(lake: Lake) -> list[dict[str, Any]]:
    raw = lake.store.objects["scratch/copy/cat/r1/census.json.gz"]
    return list(json.loads(gzip.decompress(raw))["units"])


def in_state(lake: Lake, state: str) -> list[str]:
    """The ids of the units whose record is in ``state``."""
    found = []
    for unit in units(lake):
        key = f"scratch/copy/cat/r1/units/{unit['id']}.json"
        if (
            key in lake.store.objects
            and lake.store.record(unit["id"])["state"] == state
        ):
            found.append(unit["id"])
    return found


def report(lake: Lake) -> dict[str, Any]:
    return dict(json.loads(lake.store.objects["scratch/copy/cat/r1/verify.json"]))


def crash_once(monkeypatch: pytest.MonkeyPatch, name: str, after: bool) -> None:
    """The first call of ``cp.<name>`` dies, before or after it ran."""
    real, calls = getattr(cp, name), []

    def step(*args: Any, **kwargs: Any) -> Any:
        if calls:
            return real(*args, **kwargs)
        calls.append(1)
        if after:
            real(*args, **kwargs)
        raise Crash(name)

    monkeypatch.setattr(cp, name, step)


def test_every_row_lands_once_under_its_team_prefix_and_a_rerun_adds_nothing() -> None:
    lake, ctx = planned()
    assert run(ctx, "copy") == 0
    assert run(ctx, "verify") == 0
    assert lake.dest_rows() == ROWS and lake.commits == 3
    dest = lake.info("events_v2")
    (sv,) = [c for c in dest.columns if c.name == "properties_sv"]
    values = []
    for f in lake.live("events_v2"):
        team = f["partition_values"][0]
        assert f["path"].startswith(
            f"{DATA}/data/ingest/events_v2/team_id={team}/c26-r1-"
        )
        data = pq.read_table(lake.store.open_input_file(f["path"][5:]))
        values += to_python(data["properties_sv"], sv)
    # "", null and the lone surrogate escape are SQL NULL; the rows are kept.
    assert sorted(values, key=str) == [None] * 5 + [{"$browser": "Chrome"}] * 3
    assert report(lake)["variant_invalid"] == {"invalid_unicode": 2}
    csv = lake.store.objects["scratch/copy/cat/r1/tally.csv"].decode()
    assert "2,2026-01-01,4,4\n" in csv
    head = lake.snap
    assert run(ctx, "copy") == 0 and run(ctx, "verify") == 0
    assert lake.snap == head and lake.dest_rows() == ROWS


@pytest.mark.parametrize(
    ("step", "after"),
    [("perform_upload", True), ("_send", False), ("_finish", False), ("_finish", True)],
    ids=["after-upload", "after-sent", "after-commit", "after-registration"],
)
def test_a_crash_at_any_step_lands_each_unit_once(
    monkeypatch: pytest.MonkeyPatch, step: str, after: bool
) -> None:
    lake, ctx = planned()
    crash_once(monkeypatch, step, after)
    with pytest.raises(Crash):
        run(ctx, "copy")
    assert run(ctx, "copy") == 0
    assert run(ctx, "verify") == 0
    assert lake.dest_rows() == ROWS and lake.commits == 3


def test_ambiguous_answers_resend_the_same_bytes_and_a_refusal_halts() -> None:
    lake, ctx = planned()
    lake.fail = [503, 429, 500]
    assert run(ctx, "copy") == 0 and lake.dest_rows() == ROWS
    lake, ctx = planned()
    lake.fail = [422]
    assert run(ctx, "copy") == 3
    states = [lake.store.record(u["id"])["state"] for u in units(lake)]
    assert states.count("halted") == 1 and states.count("done") == 2
    assert run(ctx, "verify") == 1


def test_a_refusal_inside_the_trust_window_starts_a_new_attempt() -> None:
    lake, ctx = planned()
    lake.floor = lake.snap + 1  # every read basis so far has expired
    lake.snap += 1
    assert run(ctx, "copy") == 0
    assert run(ctx, "verify") == 0
    assert lake.dest_rows() == ROWS


def late(monkeypatch: pytest.MonkeyPatch, days: float) -> None:
    now = time.time() + days * 86400
    monkeypatch.setattr(cp, "_now", lambda: now)


def test_a_late_resume_halts_unresolved_until_resolve_proves_it_dead(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake, ctx = planned()
    crash_once(monkeypatch, "_send", after=False)
    with pytest.raises(Crash):
        run(ctx, "copy")
    lake.floor = lake.snap = lake.snap + 1  # the sent read basis expired
    late(monkeypatch, 6.5)  # past the 6-day trust window
    assert run(ctx, "copy") == 3
    (unit,) = in_state(lake, "unresolved")
    assert lake.commits == 2
    assert run(ctx, "copy") == 3  # it stays halted, never re-attempted
    assert run(ctx, "copy", "--resolve", unit) == 0  # receipt 404
    rec = lake.store.record(unit)
    assert rec["state"] == "dead" and rec["proofs"][-1]["proof"].startswith(
        "receipt 404"
    )
    lake.snap += 1
    assert run(ctx, "copy") == 0  # the proof stands: a new attempt, no halt
    assert run(ctx, "verify") == 0 and lake.dest_rows() == ROWS
    late(monkeypatch, 7.5)
    lake.store.put_record({**rec, "state": "unresolved"})
    assert run(ctx, "copy", "--resolve", unit) == 3  # the receipt may be gone


def test_resolve_finds_a_landed_unit_by_its_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake, ctx = planned()
    crash_once(monkeypatch, "_finish", after=False)
    with pytest.raises(Crash):
        run(ctx, "copy")
    (unit,) = in_state(lake, "sent")  # landed, then the pod died
    lake.store.put_record({**lake.store.record(unit), "state": "unresolved"})
    lake.floor = lake.snap = lake.snap + 1  # the commit snapshot expired
    late(monkeypatch, 6.5)
    assert run(ctx, "copy", "--resolve", unit) == 0
    assert lake.store.record(unit)["state"] == "done"
    assert run(ctx, "copy") == 0 and run(ctx, "verify") == 0
    assert lake.dest_rows() == ROWS and lake.commits == 3


def test_a_wrong_proof_of_death_still_cannot_publish_a_unit_twice(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The key is the unit version's, not the attempt's: a new attempt
    after a landed one is refused by its receipt (422), never committed."""
    lake, ctx = planned()
    crash_once(monkeypatch, "_finish", after=False)
    with pytest.raises(Crash):
        run(ctx, "copy")
    (unit,) = in_state(lake, "sent")
    lake.store.put_record({**lake.store.record(unit), "state": "dead", "dead": True})
    assert run(ctx, "copy") == 3
    assert lake.store.record(unit)["state"] == "halted" and lake.commits == 3


def test_a_merged_path_of_an_expired_commit_is_registration_unchecked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lake, ctx = planned()
    crash_once(monkeypatch, "_finish", after=False)
    with pytest.raises(Crash):
        run(ctx, "copy")
    (unit,) = in_state(lake, "sent")
    (path,) = [
        f["path"] for f in lake.store.record(unit)["payload"]["appends"][0]["files"]
    ]
    (landed,) = [f for f in lake.live("events_v2") if f["path"] == path]
    lake.merge("events_v2", [landed], "s3://lake/compacted.parquet")
    lake.floor = lake.snap = lake.snap + 1  # and the commit snapshot expired
    assert run(ctx, "copy") == 0 and run(ctx, "verify") == 0
    assert lake.store.record(unit)["registration_unchecked"] == [path]
    assert report(lake)["registration_unchecked"] == 1 and lake.commits == 3


def test_a_transient_error_repeats_the_step_and_a_lasting_one_fails_the_pod() -> None:
    lake, ctx = planned()
    lake.flaky = [503, 502, 429]
    assert run(ctx, "copy") == 0 and run(ctx, "verify") == 0
    assert lake.dest_rows() == ROWS and lake.commits == 3
    lake, ctx = planned()
    lake.flaky = [503] * 10
    with pytest.raises(hl.HoglakeError):
        run(ctx, "copy")
    assert in_state(lake, "sent") and not in_state(lake, "halted")


def test_a_gone_census_object_is_repaired_from_head() -> None:
    lake, ctx = planned()
    first, second = lake.tables["events"]["files"][:2]
    read = [
        pq.read_table(lake.store.open_input_file(f["path"][5:]))
        for f in (first, second)
    ]
    sink = io.BytesIO()
    pq.write_table(pa.concat_tables(read), sink)  # compaction's output, then
    lake.store.objects["lake/root/merged.parquet"] = sink.getvalue()
    lake.merge("events", [first, second], f"{DATA}/merged.parquet")
    del lake.store.objects[first["path"][5:]]  # cleanup removes an input
    assert run(ctx, "copy") == 0 and run(ctx, "verify") == 0
    assert lake.dest_rows() == ROWS
    (repaired,) = report(lake)["repaired"]
    assert repaired["version"] == 1 and repaired["rows"] == 4


def _foreign(lake: Lake) -> None:
    path = f"{DATA}/data/ingest/events_v2/team_id=2/c26-r0-x-v0a1-0/0.parquet"
    lake.add("events_v2", path, 1, ["2", str(DAY)])


def _unrecorded(lake: Lake) -> None:
    path = f"{DATA}/data/ingest/events_v2/team_id=3/c26-r1-x-v0a9-0/0.parquet"
    lake.add("events_v2", path, 3, ["3", str(DAY)])


def _doubled_then_compacted(lake: Lake) -> None:
    files = [f for f in lake.live("events_v2") if f["partition_values"][0] == "3"]
    twice = lake.add("events_v2", "s3://lake/again.parquet", 3, ["3", str(DAY)])
    lake.merge("events_v2", [*files, twice], "s3://lake/compacted.parquet")


def _source_write(lake: Lake) -> None:
    lake.write(2, [("late", None)])


def _short(lake: Lake) -> None:
    lake.end(lake.live("events_v2")[0])


@pytest.mark.parametrize(
    "damage",
    [_foreign, _unrecorded, _doubled_then_compacted, _source_write, _short],
)
def test_verify_fails_on(damage: Callable[[Lake], None]) -> None:
    lake, ctx = planned()
    assert run(ctx, "copy") == 0
    damage(lake)
    assert run(ctx, "verify") == 1
    assert report(lake)["fail_count"] >= 1


def test_verify_fails_an_incomplete_run_and_reports_progress() -> None:
    lake, ctx = planned()
    assert run(ctx, "verify") == 1
    assert report(lake)["fails"] == ["incomplete: {'new': 3}"]


@pytest.mark.parametrize(("team", "verdict"), [(-1, 1), (None, 0)])
def test_only_a_null_census_team_explains_a_drop(
    team: int | None, verdict: int
) -> None:
    lake = Lake()
    seed(lake)
    lake.write(team, [("x", None), ("y", None)])
    ctx = lake.ctx()
    assert run(ctx, "plan", "--settle-window", "0") == 0
    assert run(ctx, "copy") == 0  # the unit is done without a commit
    assert run(ctx, "verify") == verdict and lake.commits == 3


def test_a_cutover_window_partition_over_twice_is_flagged_not_failed() -> None:
    lake, ctx = planned(COPY_S_TIME="2026-01-05T00:00:00+00:00")
    assert run(ctx, "copy") == 0
    _doubled_then_compacted(lake)
    assert run(ctx, "verify") == 0
    (flag,) = report(lake)["flags"]
    assert ">= 2x" in flag


def test_a_fallback_run_flags_the_lower_bound_of_earlier_units() -> None:
    lake, ctx = planned()
    assert run(ctx, "copy") == 0
    _short(lake)
    assert run(ctx, "verify", "--fallback-run-at", "2999-01-01T00:00:00+00:00") == 0
    assert len(report(lake)["flags"]) == 1
