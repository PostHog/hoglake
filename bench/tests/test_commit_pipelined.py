"""commit-pipelined against fake catalogs: the invariants must fire.

A checker that has never seen a seeded fault is itself unverified, so
each invariant gets a fake backend that violates exactly it (same
convention as tests/test_adversarial.py).
"""

from __future__ import annotations

import threading
from types import SimpleNamespace

import httpx
import pytest

from hoglake_bench.cli import _namespace_for, build_parser
from hoglake_bench.runner import BenchAbort, InvariantViolation
from hoglake_bench.scenarios import commit_pipelined
from hoglake_bench.scenarios.common import BENCH_SCHEMA


class _FakeTable:
    def __init__(self, catalog, namespace, name):
        self._catalog = catalog
        self.namespace = namespace
        self.name = name
        # fabricated_files() looks up field ids by column name
        self.columns = [
            SimpleNamespace(name="id", field_id=1),
            SimpleNamespace(name="v", field_id=2),
        ]

    def files(self):
        with self._catalog.lock:
            return list(self._catalog._files)


class _FakeNamespace:
    def __init__(self, catalog, name):
        self._catalog = catalog
        self.name = name

    def create_table(self, name, schema):
        assert schema is BENCH_SCHEMA
        with self._catalog.lock:
            self._catalog.head += 1  # DDL mints snapshots too
            t = _FakeTable(self._catalog, self.name, name)
            self._catalog._tables[(self.name, name)] = t
            return t

    def table(self, name):
        # re-resolution returns a fresh handle over the same state
        return self._catalog._tables[(self.name, name)]


class _FakeCatalog:
    """In-memory commit surface with seeded-fault switches.

    fault=None          correct: dense ids, tiling row ranges
    fault="overlap"     every file gets row_id_start=0
    fault="lost_dup"    the 3rd payload is dropped and the 4th applied
                        twice: same row count, wrong path multiset
    fault="unanchored"  acks snapshot ids offset by +100: dense among
                        themselves, but not the anchored run
    fault="no_history"  commits ack but snapshots() never grows
    fault="dead"        every commit raises a transport error
    fault="harness_bug" every commit raises ValueError (not a server
                        failure: must propagate, not be counted)
    """

    def __init__(self, name, fault=None):
        self.name = name
        self.fault = fault
        self.data_path = f"s3://fake/{name}/"
        self.lock = threading.Lock()
        self.head = 0
        self.attempts = 0
        self._files = []
        self._tables = {}
        self._next_row = 0

    def create_namespace(self, name):
        with self.lock:
            self.head += 1
        return _FakeNamespace(self, name)

    def namespace(self, name):
        return _FakeNamespace(self, name)

    def refresh(self):
        with self.lock:
            return SimpleNamespace(head_snapshot_id=self.head)

    def snapshots(self, after=0):
        if self.fault == "no_history":
            return
        with self.lock:
            ids = list(range(1, self.head + 1))
        for i in ids:
            if i > after:
                yield SimpleNamespace(snapshot_id=i)

    def _commit(self, payload):
        with self.lock:
            self.attempts += 1
            if self.fault == "dead":
                raise httpx.ConnectError("connection refused")
            if self.fault == "harness_bug":
                raise ValueError("4xx-shaped harness bug")
            self.head += 1
            snap = self.head
            drop = self.fault == "lost_dup" and self.attempts == 3
            twice = self.fault == "lost_dup" and self.attempts == 4
            for appended in payload.get("appends", []):
                reps = 0 if drop else (2 if twice else 1)
                for _ in range(reps):
                    for f in appended["files"]:
                        start = 0 if self.fault == "overlap" else self._next_row
                        self._files.append(
                            SimpleNamespace(
                                path=f["path"],
                                record_count=f["record_count"],
                                row_id_start=start,
                            )
                        )
                        if self.fault != "overlap":
                            self._next_row += f["record_count"]
        if self.fault == "unanchored":
            snap += 100
        return SimpleNamespace(snapshot_id=snap)


class _FakeBench:
    def __init__(self, fault=None):
        self._fault = fault
        self.catalogs = []

    def new_catalog(self, slug):
        cat = _FakeCatalog(f"bench-{slug}", fault=self._fault)
        self.catalogs.append(cat)
        return cat


def _args(**overrides):
    return _namespace_for(
        "commit-pipelined",
        build_parser().parse_args(["commit-pipelined"]),
        {"depths": [2], "ops": 10, "warmup": 1, **overrides},
    )


class TestHappyPath:
    def test_green_run_metrics_and_accounting(self):
        bench = _FakeBench()
        report = commit_pipelined.run(bench, _args(depths=[1, 3], ops=8, warmup=2))
        names = [m.name for m in report.metrics]
        assert names == ["pipelined.d1.fpc1", "pipelined.d3.fpc1"]
        assert all(m.ops == 8 for m in report.metrics)
        # 2 catalogs x (2 warm + 8 measured) single-file commits
        assert [c.attempts for c in bench.catalogs] == [10, 10]

    def test_multi_fpc_stage_accounting(self):
        # ids and rows accumulate across stages, anchored once at DDL
        report = commit_pipelined.run(
            _FakeBench(), _args(depths=[2], ops=5, warmup=1, files_per_commit=[1, 3])
        )
        assert [m.name for m in report.metrics] == [
            "pipelined.d2.fpc1",
            "pipelined.d2.fpc3",
        ]

    def test_duration_cut_with_zero_measured_emits_no_metric(self):
        # the measured phase is cut instantly; warmup commits still
        # happened and must be covered by the anchored invariants
        report = commit_pipelined.run(
            _FakeBench(), _args(depths=[1], ops=50, warmup=2, duration=1e-9)
        )
        assert report.metrics == []


class TestInvariantsFire:
    def test_overlapping_row_ranges_fail(self):
        with pytest.raises(InvariantViolation, match="tiling"):
            commit_pipelined.run(_FakeBench("overlap"), _args())

    def test_lost_payload_plus_duplicate_fails(self):
        # same row total as a clean run — only the path multiset catches it
        with pytest.raises(InvariantViolation, match="submitted payloads"):
            commit_pipelined.run(_FakeBench("lost_dup"), _args())

    def test_unanchored_ids_fail(self):
        # dense among themselves, but not the anchored run
        with pytest.raises(InvariantViolation, match="anchored"):
            commit_pipelined.run(_FakeBench("unanchored"), _args())

    def test_missing_persisted_history_fails(self):
        with pytest.raises(InvariantViolation, match="dense"):
            commit_pipelined.run(_FakeBench("no_history"), _args())


class TestFailurePropagation:
    def test_dead_server_aborts_and_stops_producing(self):
        bench = _FakeBench("dead")
        with pytest.raises(BenchAbort, match="unresponsive"):
            commit_pipelined.run(bench, _args(depths=[2], ops=200, warmup=0))
        # the guard trips at 10 consecutive failures; the producer must
        # stop instead of hammering through all 200 ops (in-flight
        # attempts at trip time account for a few extra)
        attempts = bench.catalogs[0].attempts
        assert 10 <= attempts < 200

    def test_non_server_error_propagates_uncounted(self):
        # a harness bug (ValueError) must surface as itself, never as a
        # miscounted InvariantViolation
        with pytest.raises(ValueError, match="harness bug"):
            commit_pipelined.run(_FakeBench("harness_bug"), _args())
