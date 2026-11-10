"""The wire payloads pyhoglake's three writers produce on a table WITHOUT
the object-prefix option, recorded as a fixture from an older pyhoglake
and held to by ``test_write_golden.py``.

Not a test module (pytest collects ``test_*.py`` and ``qe_*.py``): the
same code records the fixture and replays it, so the two cannot drift.
``python tests/golden_writes.py --record tests/vectors/write_golden.json
<commit>`` writes the fixture from whichever pyhoglake the interpreter imports;
record it from a checkout of the version the working tree must still
match (``git archive origin/main`` into a scratch directory, then
``PYTHONPATH=<that>/pyhoglake/src``), never from the working tree
itself, or the golden compares the tree with itself.

What is pinned, per writer (``append`` / ``prepare_append_tables`` /
``prepare_append_files``) and ``deferred_stats`` setting: the commit
request's body (``append`` POSTs it; the prepared calls return it) with
``uuid4`` made deterministic, and the sha256 of every object uploaded,
by uri. Parquet bytes are pinned on purpose: the registration's
``file_size_bytes`` and ``footer_size`` are measured on them, so the
fixture is specific to the locked pyarrow, and a pyarrow bump re-records
it (with the old pyhoglake, as above).
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest import mock

import pyarrow as pa
import pyarrow.parquet as pq

IDEMPOTENCY_KEY = "00000000-0000-4000-8000-00000000abcd"
CATALOG_WIRE = {
    "name": "cat",
    "data_path": "s3://bkt/lake",
    "head_snapshot_id": 90,
    "schema_version": 2,
}
TABLE_WIRE: dict[str, Any] = {
    "name": "events",
    "namespace": "ns1",
    "table_uuid": "0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f012",
    "columns": [
        {
            "name": "team_id",
            "type": "long",
            "field_id": 1,
            "ordinal": 0,
            "nullable": True,
        },
        {
            "name": "ts",
            "type": "timestamp",
            "field_id": 2,
            "ordinal": 1,
            "nullable": True,
        },
        {
            "name": "val",
            "type": "string",
            "field_id": 3,
            "ordinal": 2,
            "nullable": True,
        },
    ],
    "partition_spec": {
        "spec_id": 3,
        "fields": [
            {"source_field_id": 1, "transform": "identity"},
            {"source_field_id": 2, "transform": "month"},
        ],
    },
    "record_count": 0,
    "file_count": 0,
    "file_size_bytes": 0,
    "read_snapshot_id": 88,
}
OPTIONS_WIRE = {
    "consumer_floor": False,
    "earliest_snapshot_id": 40,
    "snapshot_retention_seconds": 3600,
}
COMMIT_WIRE = {"snapshot_id": 91, "schema_version": 2}
M_JAN = "672"  # months since 1970-01 for 2026-01
GROUPS = [([7, 7], ("7", M_JAN)), ([9], ("9", M_JAN))]
WRITERS = ("append", "prepare_append_tables", "prepare_append_files")


class _Store:
    """``S3Config`` + ``pyarrow.fs.S3FileSystem`` stand-in that keeps bytes."""

    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}

    def filesystem(self):
        return self

    def open_output_stream(self, key: str):
        import io

        store = self

        class Stream(io.BytesIO):
            def close(self) -> None:
                if not self.closed:
                    store.files[key] = self.getvalue()
                super().close()

        return Stream()


def _rows(pyhoglake, columns, team_ids: list[int]) -> pa.Table:
    from pyhoglake.types import columns_to_arrow_schema

    schema = columns_to_arrow_schema(columns)
    return pa.Table.from_pylist(
        [
            {"team_id": t, "ts": datetime(2026, 1, 1 + i, 12, 0, 0), "val": f"v-{i}"}
            for i, t in enumerate(team_ids)
        ],
        schema=schema,
    )


def _deterministic_uuid4():
    counter = iter(range(1, 1 << 16))
    return lambda: uuid.UUID(int=next(counter), version=4)


def _server(posted: list[Any]):
    """``HoglakeClient._request`` answering the writers' reads from the
    wires above and keeping each commit body in ``posted``."""

    def request(self, method, path, *, json=None, params=None, conflict=None):
        if method == "GET" and path == "/catalogs/cat":
            return CATALOG_WIRE
        if method == "GET" and path == "/catalogs/cat/namespaces/ns1/tables/events":
            return TABLE_WIRE
        if method == "GET" and path == "/catalogs/cat/options":
            return OPTIONS_WIRE
        if method == "POST" and path == "/catalogs/cat/commit":
            posted.append(json)
            return COMMIT_WIRE
        raise AssertionError(f"unexpected request {method} {path}")

    return request


def record(pyhoglake) -> dict[str, Any]:
    """Every case's payload and uploaded objects, from the given
    ``pyhoglake`` package (the one the interpreter imported)."""
    from pyhoglake.client import HoglakeClient, Namespace

    cases: dict[str, Any] = {}
    for writer in WRITERS:
        for deferred in (False, True):
            posted: list[Any] = []
            store = _Store()
            with (
                mock.patch.object(HoglakeClient, "_request", _server(posted)),
                mock.patch("uuid.uuid4", _deterministic_uuid4()),
                tempfile.TemporaryDirectory() as tmp,
            ):
                client = HoglakeClient("http://hog.test")
                client.s3 = store
                table = Namespace(client.catalog("cat"), "ns1").table("events")
                groups = [
                    (_rows(pyhoglake, table.columns, ids), partition)
                    for ids, partition in GROUPS
                ]
                if writer == "append":
                    table.append(
                        pa.concat_tables([rows for rows, _ in groups]),
                        deferred_stats=deferred,
                    )
                    (payload,) = posted
                elif writer == "prepare_append_tables":
                    payload = table.prepare_append_tables(
                        groups, idempotency_key=IDEMPOTENCY_KEY, deferred_stats=deferred
                    )
                else:
                    files = []
                    for index, (rows, partition) in enumerate(groups):
                        path = os.path.join(tmp, f"g{index}.parquet")
                        pq.write_table(rows, path)
                        files.append((path, partition))
                    payload = table.prepare_append_files(
                        files, idempotency_key=IDEMPOTENCY_KEY, deferred_stats=deferred
                    )
                client.close()
            cases[f"{writer}:deferred={deferred}"] = {
                "payload": payload,
                "uploaded": {
                    uri: hashlib.sha256(body).hexdigest()
                    for uri, body in sorted(store.files.items())
                },
            }
    return cases


def main(argv: list[str]) -> int:
    if len(argv) not in (2, 3) or argv[0] != "--record":
        print(
            "usage: golden_writes.py --record <fixture.json> [<commit recorded from>]",
            file=sys.stderr,
        )
        return 2
    import pyhoglake

    fixture = {
        "description": __doc__.strip().splitlines()[0],
        "recorded_from": {
            "commit": argv[2] if len(argv) == 3 else None,
            # The tail of the path only: where it was unpacked is noise.
            "pyhoglake_file": "/".join(Path(pyhoglake.__file__).parts[-4:]),
            "pyhoglake_version": pyhoglake.__version__,
            "pyarrow": pa.__version__,
            "python": sys.version.split()[0],
        },
        "cases": record(pyhoglake),
    }
    Path(argv[1]).write_text(json.dumps(fixture, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
