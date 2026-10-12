"""Writer invariants must survive compaction without hiding lost rows or stats."""

import os
from types import SimpleNamespace

import pytest
from pyhoglake import Table

from hoglake_bench.cli import build_parser
from hoglake_bench.context import Bench, BenchConfig
from hoglake_bench.runner import InvariantViolation
from hoglake_bench.scenarios import end_to_end


class FakeTable:
    name = "events"
    snapshot_id = 2

    def __init__(self, *, compact=False):
        self.head = self.snapshot_id
        self.compact = compact
        self.written = []
        self.snapshots = {self.head: []}

    def append(self, data, **kwargs):
        self.head += 1
        f = SimpleNamespace(
            path=f"append-{self.head}",
            row_id_start=sum(f.record_count for f in self.written),
            record_count=data.num_rows,
            stats_state="provided",
        )
        self.written.append(f)
        self.snapshots[self.head] = list(self.written)
        return SimpleNamespace(snapshot_id=self.head, files=(f,))

    def refresh(self):
        if self.compact:
            self.compact = False
            self.head += 1
            # Merge non-adjacent ranges: row_id_start is now only their minimum.
            merged = SimpleNamespace(
                path="compacted",
                row_id_start=0,
                record_count=sum(f.record_count for f in self.written[::2]),
                stats_state="pending",
            )
            self.snapshots[self.head] = [merged, *self.written[1::2]]
        return SimpleNamespace(head_snapshot_id=self.head)

    def info(self, snapshot=None):
        assert snapshot is not None, "head totals may be absent or sampled"
        return SimpleNamespace(
            record_count=sum(f.record_count for f in self.written),
            file_count=len(self.snapshots[snapshot]),
        )

    def files(self, snapshot=None):
        assert snapshot is not None, "implicit files() reads use the DDL pin"
        return list(self.snapshots[snapshot])

    def changes(self, from_snapshot, to_snapshot):
        assert from_snapshot == self.snapshot_id
        assert to_snapshot in self.snapshots
        return SimpleNamespace(files=tuple(self.written))


def run(table, *, schema="simple"):
    catalog = SimpleNamespace(
        refresh=table.refresh,
        create_namespace=lambda name: SimpleNamespace(
            create_table=lambda name, schema: table
        ),
    )
    bench = SimpleNamespace(
        ensure_bucket=lambda: None, new_catalog=lambda slug: catalog
    )
    args = build_parser().parse_args(
        [
            "end-to-end-writer",
            "--rows",
            "30",
            "--batch-rows",
            "10",
            "--table-schema",
            schema,
        ]
    )
    return end_to_end.run(bench, args)


@pytest.mark.parametrize("schema", ["simple", "typed"])
@pytest.mark.parametrize("compact", [False, True])
def test_writer_accepts_compaction(schema, compact):
    table = FakeTable(compact=compact)
    report = run(table, schema=schema)
    assert report.metrics[0].ops == 3
    assert report.metrics[0].extra["rows"] == 30
    assert len(table.written) == 4  # Includes the warmup append.


@pytest.mark.parametrize(
    ("fault", "message"),
    [
        ("extra_file", "exceeds 4 appended files"),
        ("lost_live_rows", "visible files do not account"),
        ("failed_live_stats", "visible file stats failed"),
        ("missing_append", "changefeed files do not match"),
        ("foreign_append", "changefeed files do not match"),
        ("pending_append_stats", "did not register as provided"),
        ("failed_append_stats", "did not register as provided"),
        ("row_gap", "row-range tiling broken"),
    ],
)
def test_writer_rejects_inconsistent_state(fault, message):
    table = FakeTable(compact=fault != "extra_file")
    refresh = table.refresh

    def inject():
        result = refresh()
        live = table.snapshots[table.head]
        if fault == "extra_file":
            live.append(SimpleNamespace(path="extra", record_count=0))
        elif fault == "lost_live_rows":
            live[0].record_count -= 1
        elif fault == "failed_live_stats":
            live[0].stats_state = "failed"
        elif fault == "missing_append":
            table.changes = lambda **kwargs: SimpleNamespace(
                files=tuple(table.written[:-1])
            )
        elif fault == "foreign_append":
            foreign = SimpleNamespace(**vars(table.written[0]))
            foreign.path = "unregistered"
            table.changes = lambda **kwargs: SimpleNamespace(
                files=(foreign, *table.written[1:])
            )
        elif fault == "pending_append_stats":
            table.written[0].stats_state = "pending"
        elif fault == "failed_append_stats":
            table.written[0].stats_state = "failed"
        elif fault == "row_gap":
            table.written[0].row_id_start = 1
        return result

    table.refresh = inject
    with pytest.raises(InvariantViolation, match=message):
        run(table)


def test_validation_uses_one_snapshot_across_a_compaction_race():
    table = FakeTable()
    info = table.info

    def compact_after_info(snapshot):
        result = info(snapshot)
        table.compact = True
        table.refresh()
        return result

    table.info = compact_after_info
    report = run(table)
    assert report.metrics[0].ops == 3


@pytest.mark.integration
@pytest.mark.parametrize("schema", ["simple", "typed"])
def test_writer_survives_live_compaction(monkeypatch, live_server_url, schema):
    bench = Bench(
        BenchConfig(
            url=live_server_url,
            s3_endpoint=os.environ.get("HOGLAKE_S3_ENDPOINT", "http://localhost:9000"),
        )
    )
    append = Table.append
    appended = 0

    def append_and_compact(table, data, **kwargs):
        nonlocal appended
        result = append(table, data, **kwargs)
        appended += 1
        if appended == 9:
            catalog = table._namespace._catalog
            catalog._client._request(
                "POST", catalog._path("/maintenance/compact"), params={"batch": 10}
            )
            files = table.files(snapshot=catalog.refresh().head_snapshot_id)
            assert len(files) < appended, "the live test must actually compact files"
        return result

    monkeypatch.setattr(Table, "append", append_and_compact)
    args = build_parser().parse_args(
        [
            "end-to-end-writer",
            "--rows",
            "80",
            "--batch-rows",
            "10",
            "--table-schema",
            schema,
        ]
    )
    try:
        report = end_to_end.run(bench, args)
        assert report.metrics[0].extra["rows"] == 80
        assert appended == 9
    finally:
        bench.close()
