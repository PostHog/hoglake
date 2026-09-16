import time
from datetime import UTC, datetime
from types import SimpleNamespace

import pyarrow.parquet as pq
from pyhoglake.models import ChangesPlan, DataFile
from test_events_discovery import layout, raw

from hedgerow.buffering import BufferPolicy
from hedgerow.ingestion import BufferedIngestion


class Catalog:
    def __init__(self):
        self.head_snapshot_id = 1
        self.committed = None
        self.receipts = {}
        self.calls = []

    def refresh(self):
        return self

    def options(self):
        return SimpleNamespace(consumer_floor=True)

    def offset(self, *args):
        return (
            None
            if self.committed is None
            else SimpleNamespace(committed_snapshot=self.committed)
        )

    def commit_offset(self, consumer, uuid, snapshot):
        assert self.committed is None or snapshot >= self.committed
        self.committed = snapshot

    def snapshots(self, after):
        return iter(
            [
                SimpleNamespace(
                    snapshot_id=i, snapshot_time=datetime(2026, 9, 1, tzinfo=UTC)
                )
                for i in range(after + 1, self.head_snapshot_id + 1)
            ]
        )

    def commit_prepared(self, request):
        self.calls.append(request)
        key = request["idempotency_key"]
        if key not in self.receipts:
            self.receipts[key] = request
            raise TimeoutError("response lost after publication")
        assert self.receipts[key] == request


def test_coordinator_accumulates_windows_and_recovers_ambiguous_publication(tmp_path):
    from dataclasses import replace

    transform = layout()
    path = tmp_path / "raw.parquet"
    pq.write_table(raw(), path, row_group_size=2)
    paths = [tmp_path / f"raw-{i}.parquet" for i in (1, 2)]
    for output in paths:
        output.write_bytes(path.read_bytes())
    files = [
        DataFile(i, str(output), "parquet", 3, output.stat().st_size, 0, "ready", i)
        for i, output in enumerate(paths, 1)
    ]
    source_info = replace(
        transform.destination,
        name="raw_events",
        table_uuid="source",
        columns=transform.source_columns,
        partition_spec=None,
        sort_spec=None,
    )
    source = SimpleNamespace(
        info=lambda: source_info,
        changes=lambda start, end: ChangesPlan(
            "source",
            start,
            end,
            tuple(f for f in files if start < f.begin_snapshot <= end),
        ),
    )
    prepared_rows = []

    def prepare(files, **kwargs):
        rows = sum(pq.ParquetFile(path).metadata.num_rows for path, partition in files)
        prepared_rows.append(rows)
        return {
            "idempotency_key": kwargs["idempotency_key"],
            "rows": rows,
            "appends": [{"expected_table_uuid": kwargs["expected_table_uuid"]}],
        }

    dest = SimpleNamespace(
        info=lambda: transform.destination, prepare_append_files=prepare
    )
    source_cat, dest_cat = Catalog(), Catalog()

    def coordinator():
        daemon = BufferedIngestion(
            source,
            dest,
            source_catalog=source_cat,
            destination_catalog=dest_cat,
            consumer_id="job",
            state_path=str(tmp_path / "state.sqlite"),
            filesystem=None,
            spill_directory=str(tmp_path),
            policy=BufferPolicy(workers=2),
        )
        daemon._open_parquet = pq.ParquetFile
        return daemon

    now = datetime(2026, 9, 1, tzinfo=UTC).timestamp()
    daemon = coordinator()
    assert daemon.run_once(now).files == 1
    source_cat.head_snapshot_id = 2
    assert daemon.run_once(now + 10).files == 1
    assert source_cat.committed == 0  # discovery is not publication
    daemon.close()
    daemon = coordinator()
    try:
        assert daemon.store.discovered == 2
        deadline = time.monotonic() + 5
        while source_cat.committed != 2:
            assert time.monotonic() < deadline
            try:
                daemon.run_once(now + 86400)
            except TimeoutError:
                pass
            time.sleep(0.001)
        assert prepared_rows == [
            2,
            2,
            2,
        ]  # consolidated across windows, separate team/month
        assert len(dest_cat.receipts) == 3
        assert len(dest_cat.calls) == 6
        assert len(daemon.store.cleanup_candidates()) == 2
    finally:
        daemon.close()
