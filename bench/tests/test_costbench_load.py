"""Loader correctness: source types, bounded slices, inflation, and safe resume."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from contextlib import nullcontext
from datetime import UTC, date, datetime
from types import SimpleNamespace

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from hoglake_bench.costbench import load


def source_table(rows: int = 7) -> pa.Table:
    arrays = {}
    for field in load.seed_schema():
        if pa.types.is_timestamp(field.type):
            arrays[field.name] = pa.array(
                [1_370_000_000 + index for index in range(rows)], type=pa.int64()
            )
        elif pa.types.is_date32(field.type):
            arrays[field.name] = pa.array([16_000] * rows, type=pa.uint16())
        elif pa.types.is_string(field.type):
            arrays[field.name] = pa.array([b"example"] * rows, type=pa.binary())
        else:
            arrays[field.name] = pa.array(range(rows), type=field.type)
    return pa.table(arrays)


def test_schema_has_the_pinned_columns_and_widths():
    schema = load.seed_schema()
    assert len(schema) == 105
    assert schema.field("WatchID").type == pa.int64()
    assert schema.field("ClientIP").type == pa.int32()
    assert schema.field("JavaEnable").type == pa.int16()
    assert schema.field("EventTime").type == pa.timestamp("us", tz="UTC")
    assert schema.field("EventDate").type == pa.date32()
    assert schema.names[-9:] == [
        "UTMMedium",
        "UTMCampaign",
        "UTMContent",
        "UTMTerm",
        "FromTag",
        "HasGCLID",
        "RefererHash",
        "URLHash",
        "CLID",
    ]


def test_conversion_preserves_upstream_signed_bit_patterns_and_epoch_units():
    source = source_table(2)
    source = source.set_column(
        source.schema.get_field_index("WatchID"),
        "WatchID",
        pa.array([2**64 - 1, None], type=pa.uint64()),
    )
    source = source.set_column(
        source.schema.get_field_index("UserID"),
        "UserID",
        pa.array([2**63, 7], type=pa.uint64()),
    )
    source = source.set_column(
        source.schema.get_field_index("ClientIP"),
        "ClientIP",
        pa.array([2**32 - 1, 1], type=pa.uint32()),
    )
    # Columns may arrive in any Parquet order; names, not offsets, bind them.
    converted = load.convert_batch(source.select(list(reversed(source.column_names))))
    assert converted.schema == load.seed_schema()
    assert converted["WatchID"].to_pylist() == [-1, None]
    assert converted["UserID"].to_pylist() == [-(2**63), 7]
    assert converted["ClientIP"].to_pylist() == [-1, 1]
    assert converted["EventDate"].to_pylist() == [date(2013, 10, 22)] * 2
    assert converted["EventTime"][0].as_py() == datetime(
        2013, 5, 31, 11, 33, 20, tzinfo=UTC
    )
    assert converted["Title"].to_pylist() == ["example", "example"]


def test_conversion_refuses_schema_drift():
    with pytest.raises(ValueError, match="seed columns differ"):
        load.convert_batch(source_table().drop(["Title"]))


@pytest.mark.parametrize("source_type", [pa.uint16(), pa.int32(), pa.int64()])
def test_integer_epoch_days_convert_through_int32(source_type):
    source = source_table(2)
    source = source.set_column(
        source.schema.get_field_index("EventDate"),
        "EventDate",
        pa.array([15_888, None], type=source_type),
    )
    assert load.convert_batch(source)["EventDate"].to_pylist() == [
        date(2013, 7, 2),
        None,
    ]


def test_seed_reader_bounds_memory_and_resumes_at_exact_row(tmp_path):
    path = tmp_path / "seed.parquet"
    pq.write_table(source_table(11), path, row_group_size=4)
    batches = list(load.seed_batches(path, start=3, stop=10, batch_rows=2))
    assert all(batch.num_rows <= 2 for batch in batches)
    assert pa.concat_tables(batches)["WatchID"].to_pylist() == list(range(3, 10))
    assert list(load.seed_batches(path, start=11, stop=11, batch_rows=2)) == []


@pytest.mark.parametrize(
    "start,stop,batch_rows", [(-1, 2, 2), (2, 1, 2), (0, 2, 0), (0, 20, 2)]
)
def test_seed_reader_refuses_invalid_interval(tmp_path, start, stop, batch_rows):
    path = tmp_path / "seed.parquet"
    pq.write_table(source_table(), path)
    with pytest.raises(ValueError):
        list(load.seed_batches(path, start=start, stop=stop, batch_rows=batch_rows))


def test_inflation_matches_one_billion_exactly():
    additions = list(load.inflation_steps(load.SEED_ROWS, 1_000_000_000))
    assert additions == [99_997_497, 199_994_994, 399_989_988, 200_020_024]
    assert load.SEED_ROWS + sum(additions) == 1_000_000_000
    assert list(load.inflation_steps(1, 1)) == []


@pytest.mark.parametrize("rows,target", [(0, 10), (-1, 10), (10, 9)])
def test_inflation_refuses_bad_counts(rows, target):
    with pytest.raises(ValueError):
        next(load.inflation_steps(rows, target))


def checkpoint():
    return {
        "version": 1,
        "config": {"target_rows": 100},
        "table_uuid": "example-table",
        "rows": 20,
        "pending": None,
    }


def test_checkpoint_refuses_ambiguous_commit_even_when_row_count_matches():
    state = checkpoint()
    state["pending"] = {"kind": "trino_insert", "before": 20, "after": 40}
    with pytest.raises(ValueError, match="unknown outcome"):
        load.validate_checkpoint(state, state["config"], state["table_uuid"], 20)


def test_checkpoint_refuses_changed_destination_or_rows():
    state = checkpoint()
    load.validate_checkpoint(state, state["config"], state["table_uuid"], 20)
    for config, uuid, rows in [
        ({"target_rows": 101}, "example-table", 20),
        (state["config"], "recreated-table", 20),
        (state["config"], "example-table", 40),
    ]:
        with pytest.raises(ValueError):
            load.validate_checkpoint(state, config, uuid, rows)


def test_checkpoint_records_pending_before_mutation_and_requires_exact_growth(tmp_path):
    path = tmp_path / "state.json"
    state = checkpoint()
    load._begin_operation(path, state, "seed_append", 5)
    assert json.loads(path.read_text())["pending"] == {
        "kind": "seed_append",
        "before": 20,
        "after": 25,
    }
    with pytest.raises(ValueError, match="unexpected server row count"):
        load._finish_operation(path, state, 24)
    assert json.loads(path.read_text())["pending"] is not None
    load._finish_operation(path, state, 25)
    assert json.loads(path.read_text())["rows"] == 25
    assert json.loads(path.read_text())["pending"] is None


@pytest.mark.parametrize("sampled_rows", [None, 0, 999])
def test_exact_totals_ignore_absent_or_stale_head_sample(sampled_rows):
    calls = []

    class SampledTable:
        def info(self, *, snapshot=None, totals=True):
            calls.append((snapshot, totals))
            if snapshot is None:
                return SimpleNamespace(
                    read_snapshot_id=42,
                    record_count=sampled_rows,
                    file_count=None,
                    file_size_bytes=None,
                )
            assert snapshot == 42
            return SimpleNamespace(record_count=12, file_count=2, file_size_bytes=100)

    assert load.exact_table_info(SampledTable()).record_count == 12
    assert calls == [(None, False), (42, True)]
    calls.clear()
    assert load.exact_table_info(SampledTable(), snapshot_id=42).record_count == 12
    assert calls == [(42, True)]


def test_exact_totals_refuse_unpinnable_server():
    class OldTable:
        def info(self, *, totals=True):
            return SimpleNamespace(read_snapshot_id=None)

    with pytest.raises(ValueError, match="resolved read_snapshot_id"):
        load.exact_table_info(OldTable())


def test_existing_seed_gets_digest_and_metadata_validation(tmp_path, monkeypatch):
    path = tmp_path / "seed.parquet"
    pq.write_table(source_table(), path)
    monkeypatch.setattr(load, "SEED_BYTES", path.stat().st_size)
    monkeypatch.setattr(load, "SEED_ROWS", 7)
    monkeypatch.setattr(
        load, "SEED_SHA256", hashlib.sha256(path.read_bytes()).hexdigest()
    )
    metadata = load.obtain_seed(path)
    assert metadata["rows"] == 7
    assert metadata["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    monkeypatch.setattr(load, "SEED_ROWS", 8)
    with pytest.raises(ValueError, match="seed row count differs"):
        load.obtain_seed(path)


def test_existing_seed_refuses_partial_download(tmp_path):
    path = tmp_path / "seed.parquet"
    path.write_bytes(b"partial")
    with pytest.raises(ValueError, match="seed size differs"):
        load.obtain_seed(path)


def test_existing_seed_refuses_same_size_different_content(tmp_path, monkeypatch):
    path = tmp_path / "seed.parquet"
    pq.write_table(source_table(), path)
    monkeypatch.setattr(load, "SEED_BYTES", path.stat().st_size)
    monkeypatch.setattr(load, "SEED_ROWS", 7)
    monkeypatch.setattr(load, "SEED_SHA256", "0" * 64)
    with pytest.raises(ValueError, match="checksum differs"):
        load.obtain_seed(path)


def test_corrupt_download_is_not_promoted_to_the_seed_cache(tmp_path, monkeypatch):
    path = tmp_path / "seed.parquet"
    pq.write_table(source_table(), path)
    payload = path.read_bytes()
    path.unlink()
    monkeypatch.setattr(load, "SEED_BYTES", len(payload))
    monkeypatch.setattr(load, "SEED_ROWS", 7)
    monkeypatch.setattr(load, "SEED_SHA256", "0" * 64)
    response = httpx.Response(
        200, content=payload, request=httpx.Request("GET", load.SEED_URL)
    )
    monkeypatch.setattr(load.httpx, "stream", lambda *a, **kw: nullcontext(response))
    with pytest.raises(ValueError, match="checksum differs"):
        load.obtain_seed(path)
    assert not path.exists()


def test_password_environment_is_checked_before_download(tmp_path, monkeypatch):
    parser = argparse.ArgumentParser()
    load.add_load_arguments(parser)
    monkeypatch.delenv("COSTBENCH_TEST_PASSWORD", raising=False)
    monkeypatch.setattr(
        load,
        "obtain_seed",
        lambda _: pytest.fail("download started before credential validation"),
    )
    args = parser.parse_args(
        [
            "--trino-url",
            "https://localhost:8443",
            "--hoglake-url",
            "http://localhost:8081",
            "--data-path",
            "s3://benchmark-example/data/",
            "--password-env",
            "COSTBENCH_TEST_PASSWORD",
        ]
    )
    with pytest.raises(ValueError, match="unset or empty"):
        load.load_main(args)
    monkeypatch.setenv("COSTBENCH_TEST_PASSWORD", "")
    with pytest.raises(ValueError, match="unset or empty"):
        load.load_main(args)


def test_load_passes_basic_password_and_ca_without_persisting_the_secret(
    tmp_path, monkeypatch
):
    from hoglake_bench.costbench import client as trino_module

    parser = argparse.ArgumentParser()
    load.add_load_arguments(parser)
    calls = []

    class Preflight:
        def __init__(self, url, **kwargs):
            calls.append(kwargs)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql, **kwargs):
            assert sql == "SELECT 1"

    def stop_after_preflight(_):
        raise RuntimeError("preflight done")

    ca_file = tmp_path / "ca.pem"
    monkeypatch.setenv("COSTBENCH_TEST_PASSWORD", "example-test-password")
    monkeypatch.setattr(trino_module, "TrinoClient", Preflight)
    monkeypatch.setattr(load, "obtain_seed", stop_after_preflight)
    args = parser.parse_args(
        [
            "--trino-url",
            "https://localhost:8443",
            "--hoglake-url",
            "http://localhost:8081",
            "--data-path",
            "s3://benchmark-example/data/",
            "--password-env",
            "COSTBENCH_TEST_PASSWORD",
            "--ca-file",
            str(ca_file),
        ]
    )
    with pytest.raises(RuntimeError, match="preflight done"):
        load.load_main(args)
    assert calls[0]["password"] == "example-test-password"
    assert calls[0]["ca_file"] == ca_file


def test_stats_wait_pins_snapshot_and_waits_for_all_files():
    polls = []
    now = [0.0]

    def handle(request):
        polls.append(request)
        states = (
            ["provided", "pending"] if len(polls) == 1 else ["provided", "provided"]
        )
        return httpx.Response(200, json=[{"stats_state": state} for state in states])

    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        result = load.wait_for_stats(
            "http://localhost:8081",
            "example",
            "costbench",
            "hits",
            snapshot=42,
            http_client=http,
            clock=lambda: now[0],
            sleep=lambda seconds: now.__setitem__(0, now[0] + seconds),
        )
    assert result == {"provided_files": 2, "elapsed_s": 5}
    assert all(request.url.params["snapshot"] == "42" for request in polls)


@pytest.mark.parametrize(
    "files", [[], [{"stats_state": "failed"}], [{"stats_state": "unexpected"}]]
)
def test_stats_wait_refuses_missing_failed_or_unknown_file_state(files):
    with (
        httpx.Client(
            transport=httpx.MockTransport(lambda _: httpx.Response(200, json=files))
        ) as http,
        pytest.raises(ValueError),
    ):
        load.wait_for_stats(
            "http://localhost:8081",
            "example",
            "costbench",
            "hits",
            snapshot=42,
            http_client=http,
        )


def test_stats_wait_retries_transient_errors_only_until_deadline():
    now = [0.0]
    calls = []

    def handle(request):
        assert now[0] < 3, "polled after the hydration deadline"
        calls.append(request)
        return (
            httpx.Response(503)
            if len(calls) == 1
            else httpx.Response(200, json=[{"stats_state": "pending"}])
        )

    with (
        httpx.Client(transport=httpx.MockTransport(handle)) as http,
        pytest.raises(TimeoutError, match="hydration timed out"),
    ):
        load.wait_for_stats(
            "http://localhost:8081",
            "example",
            "costbench",
            "hits",
            snapshot=42,
            timeout_s=3,
            poll_interval=2,
            http_client=http,
            clock=lambda: now[0],
            sleep=lambda seconds: now.__setitem__(0, now[0] + seconds),
        )
    assert now[0] == 3
    assert len(calls) == 2
    assert [call.extensions["timeout"]["read"] for call in calls] == [3, 1]


def test_stats_wait_does_not_retry_permission_errors():
    requests = []

    def handle(request):
        requests.append(request)
        assert len(requests) == 1, "retried a permission error"
        return httpx.Response(403)

    with (
        httpx.Client(transport=httpx.MockTransport(handle)) as http,
        pytest.raises(httpx.HTTPStatusError),
    ):
        load.wait_for_stats(
            "http://localhost:8081",
            "example",
            "costbench",
            "hits",
            snapshot=42,
            http_client=http,
            sleep=lambda _: None,
        )
    assert len(requests) == 1


@pytest.mark.parametrize(
    "failure",
    [None, "unexpected_state", "missing_state", "path", "existing_table", "validation"],
)
def test_load_inflates_exactly_and_completed_resume_writes_nothing(
    tmp_path, monkeypatch, failure
):
    from hoglake_bench.costbench import client as trino_module

    seed = tmp_path / "seed.parquet"
    pq.write_table(source_table(), seed, row_group_size=4)
    monkeypatch.setattr(load, "SEED_BYTES", seed.stat().st_size)
    monkeypatch.setattr(load, "SEED_ROWS", 7)
    monkeypatch.setattr(
        load, "SEED_SHA256", hashlib.sha256(seed.read_bytes()).hexdigest()
    )
    rows = 0
    appends = []
    inserts = []
    created = False

    class FakeTable:
        table_uuid = "example-table"

        def info(self, *, snapshot=None, totals=True):
            if snapshot is None:
                # Real current head totals are absent before sampling, then
                # lag behind appends; checkpoints must never depend on them.
                return SimpleNamespace(
                    read_snapshot_id=rows + 1,
                    record_count=None if rows == 0 else 0,
                    file_count=None,
                    file_size_bytes=None,
                )
            assert snapshot == rows + 1
            return SimpleNamespace(
                record_count=rows,
                file_count=len(appends) + len(inserts),
                file_size_bytes=100,
                read_snapshot_id=rows + 1,
            )

        def append(self, batch, **kwargs):
            nonlocal rows
            assert batch.schema == load.seed_schema()
            appends.append(batch.num_rows)
            rows += batch.num_rows
            return SimpleNamespace(snapshot_id=rows + 1)

    table = FakeTable()

    class FakeCatalog:
        data_path = (
            "s3://other-example/data/"
            if failure == "path"
            else "s3://benchmark-example/data/"
        )

        def create_namespace(self, name):
            return self

        def create_table(self, name, schema):
            nonlocal created
            if created or failure == "existing_table":
                raise load.AlreadyExistsError("exists")
            created = True
            return table

        def table(self, name):
            return table

    catalog = FakeCatalog()

    class FakeHoglake:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def create_catalog(self, name, path):
            return catalog

    class FakeTrino(FakeHoglake):
        def execute(self, sql, *, collect_rows=False):
            nonlocal rows
            if sql == "SELECT 1":
                return SimpleNamespace(rows=[[1]], columns=[])
            if sql.startswith("INSERT"):
                limit = re.search(r" LIMIT (\d+)$", sql)
                added = int(limit[1]) if limit else rows
                inserts.append(added)
                rows += added
                return SimpleNamespace(rows=[], columns=[])
            assert collect_rows
            return SimpleNamespace(
                rows=[
                    [
                        rows + (1 if failure == "validation" else 0),
                        "2013-10-22",
                        "2013-10-22",
                        "2013-05-31",
                        "2013-05-31",
                        0,
                    ]
                ],
                columns=[],
            )

    monkeypatch.setattr(load, "HoglakeClient", FakeHoglake)
    monkeypatch.setattr(trino_module, "TrinoClient", FakeTrino)
    monkeypatch.setattr(
        load,
        "wait_for_stats",
        lambda *args, **kwargs: {"provided_files": 5, "elapsed_s": 0},
    )
    parser = argparse.ArgumentParser()
    load.add_load_arguments(parser)
    arguments = [
        "--trino-url",
        "http://localhost:8080",
        "--hoglake-url",
        "http://localhost:8081",
        "--data-path",
        "s3://benchmark-example/data/",
        "--seed-file",
        str(seed),
        "--target-rows",
        "20",
        "--batch-rows",
        "3",
        "--state",
        str(tmp_path / "state.json"),
        "--manifest",
        str(tmp_path / "manifest.json"),
    ]
    if failure == "unexpected_state":
        (tmp_path / "state.json").write_text(
            json.dumps(
                {
                    "version": 1,
                    "rows": 0,
                    "pending": None,
                    "table_uuid": table.table_uuid,
                    "config": {
                        "catalog": "costbench",
                        "trino_catalog": "hoglake",
                        "schema": "costbench",
                        "table": "hits",
                        "data_path": "s3://benchmark-example/data",
                        "target_rows": 20,
                        "batch_rows": 3,
                        "seed_sha256": load.SEED_SHA256,
                    },
                }
            )
        )
    if failure:
        message = {
            "unexpected_state": "use --resume",
            "missing_state": "use --resume",
            "path": "different data path",
            "existing_table": "target table already exists",
            "validation": "row-count validation",
        }[failure]
        with pytest.raises(ValueError, match=message):
            load.load_main(
                parser.parse_args(
                    [*arguments, *(["--resume"] if failure == "missing_state" else [])]
                )
            )
        assert not (tmp_path / "manifest.json").exists()
        return
    assert load.load_main(parser.parse_args(arguments)) == 0
    assert rows == 20
    assert appends == [3, 1, 3]
    assert inserts == [7, 6]
    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["rows"] == 20
    assert "benchmark-example" not in json.dumps(manifest)
    assert load.load_main(parser.parse_args([*arguments, "--resume"])) == 0
    assert appends == [3, 1, 3]
    assert inserts == [7, 6]


def test_download_size_is_checked_before_promoting_the_seed(tmp_path, monkeypatch):
    path = tmp_path / "seed.parquet"
    pq.write_table(source_table(), path)
    payload = path.read_bytes()
    path.unlink()
    monkeypatch.setattr(load, "SEED_BYTES", len(payload) + 1)
    monkeypatch.setattr(load, "SEED_SHA256", hashlib.sha256(payload).hexdigest())
    monkeypatch.setattr(load, "SEED_ROWS", 7)
    response = httpx.Response(
        200, content=payload, request=httpx.Request("GET", load.SEED_URL)
    )
    monkeypatch.setattr(load.httpx, "stream", lambda *a, **kw: nullcontext(response))
    with pytest.raises(ValueError, match="downloaded seed size"):
        load.obtain_seed(path)
    assert not path.exists()


def test_snapshot_totals_must_all_be_present():
    table = SimpleNamespace(
        info=lambda **kw: SimpleNamespace(
            record_count=None, file_count=1, file_size_bytes=1
        )
    )
    with pytest.raises(ValueError, match="exact and present"):
        load.exact_table_info(table, snapshot_id=1)


@pytest.mark.parametrize(
    "options",
    [
        {"timeout_s": 0},
        {"timeout_s": float("inf")},
        {"poll_interval": 0},
        {"poll_interval": float("nan")},
    ],
)
def test_invalid_hydration_limits_are_refused_before_io(options):
    with (
        httpx.Client(
            transport=httpx.MockTransport(
                lambda _: pytest.fail("performed I/O with invalid timeout")
            )
        ) as http,
        pytest.raises(ValueError, match="must be positive"),
    ):
        load.wait_for_stats(
            "http://example.test",
            "example",
            "costbench",
            "hits",
            snapshot=1,
            http_client=http,
            **options,
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"target_rows": 0},
        {"batch_rows": 0},
        {"hydration_timeout": 0},
        {"data_path": "file:///tmp/data"},
    ],
)
def test_load_arguments_are_checked_before_remote_work(tmp_path, monkeypatch, changes):
    from hoglake_bench.costbench import client as trino_module

    parser = argparse.ArgumentParser()
    load.add_load_arguments(parser)
    args = parser.parse_args(
        [
            "--trino-url",
            "http://example.test",
            "--hoglake-url",
            "http://example.test",
            "--data-path",
            "s3://example-bucket/prefix/",
        ]
    )
    for key, value in changes.items():
        setattr(args, key, value)
    monkeypatch.setattr(
        trino_module,
        "TrinoClient",
        lambda *a, **kw: pytest.fail(
            "performed remote work with invalid load arguments"
        ),
    )
    with pytest.raises(ValueError):
        load.load_main(args)
