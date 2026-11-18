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
fixture is specific to the locked pyarrow — all but one string of it.
The footer's ``created_by`` (``parquet-cpp-arrow version X.Y.Z``) is
parquet-cpp's stamp, not pyhoglake's output, and the replay rewrites it
to the recorded one (``recorded_from.parquet_created_by``) before
hashing (``restamp``), so a pyarrow release that changes nothing else
passes: 26.0.0 -> 26.0.1 moved exactly one byte per object, in that
stamp. A bump that changes any other byte, or the stamp's length (which
moves ``footer_size`` and ``file_size_bytes`` in the payload), still
re-records the fixture (with the old pyhoglake, as above).
"""

from __future__ import annotations

import hashlib
import io
import json
import os
import struct
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


PARQUET_MAGIC = b"PAR1"


def created_by_of(body: bytes) -> str:
    """The ``created_by`` stamp in the footer of a parquet object's bytes."""
    return pq.read_metadata(io.BytesIO(body)).created_by


def _varint(n: int) -> bytes:
    """Thrift compact protocol's unsigned LEB128 — a string's length prefix."""
    out = bytearray()
    while n >= 0x80:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)
    return bytes(out)


def restamp(body: bytes, created_by: str) -> bytes:
    """``body`` with the footer's ``created_by`` rewritten to ``created_by``
    and every other byte as it was.

    The footer is the thrift-compact ``FileMetaData``, its length (u32 LE)
    and the magic; a string field in it is a varint length and the UTF-8
    bytes. The stamp pyarrow wrote (``created_by_of``) is located by that
    encoding, which must occur exactly once in the footer, and the length
    trailer is recomputed, so a stamp of another length is rewritten
    correctly too (the payload's ``footer_size`` and ``file_size_bytes``
    then differ from the recorded ones, and that test fails as it
    should). Nothing before the footer — the column chunks and their
    pages — is touched, so a changed data byte still fails the
    comparison; the test module pins both.
    """
    if body[:4] != PARQUET_MAGIC or body[-4:] != PARQUET_MAGIC:
        raise ValueError("not a parquet file")
    current = created_by_of(body)
    if current == created_by:
        return body
    (footer_len,) = struct.unpack("<I", body[-8:-4])
    footer_start = len(body) - 8 - footer_len
    footer = body[footer_start:-8]
    old = _varint(len(current.encode())) + current.encode()
    if footer.count(old) != 1:
        raise ValueError(
            f"created_by {current!r} occurs {footer.count(old)} times in the footer"
        )
    new = _varint(len(created_by.encode())) + created_by.encode()
    footer = footer.replace(old, new)
    out = body[:footer_start] + footer + struct.pack("<I", len(footer)) + PARQUET_MAGIC
    if created_by_of(out) != created_by:
        raise ValueError("the restamped footer does not read back as requested")
    return out


def record(pyhoglake, *, created_by: str | None = None) -> dict[str, Any]:
    """``digest(replay(pyhoglake))``: the fixture's ``cases``, from the
    given ``pyhoglake`` package (the one the interpreter imported)."""
    return digest(replay(pyhoglake), created_by=created_by)


def digest(cases: dict[str, Any], *, created_by: str | None = None) -> dict[str, Any]:
    """``replay``'s cases in the fixture's shape: the payload, and the
    sha256 of every uploaded object by uri — each object first restamped
    to ``created_by`` when one is given. The replay passes the fixture's
    ``recorded_from.parquet_created_by``; the recorder hashes the bytes
    as written."""
    return {
        case: {
            "payload": c["payload"],
            "uploaded": {
                uri: hashlib.sha256(
                    body if created_by is None else restamp(body, created_by)
                ).hexdigest()
                for uri, body in c["objects"].items()
            },
        }
        for case, c in cases.items()
    }


def replay(pyhoglake) -> dict[str, Any]:
    """Every case's payload and the bytes of every object it uploaded, by
    uri, from the given ``pyhoglake`` package."""
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
                "objects": dict(sorted(store.files.items())),
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

    cases = replay(pyhoglake)
    stamps = {
        created_by_of(body)
        for case in cases.values()
        for body in case["objects"].values()
    }
    if len(stamps) != 1:
        print(f"one created_by expected across the objects, got {sorted(stamps)}")
        return 1
    (created_by,) = stamps
    fixture = {
        "description": __doc__.strip().splitlines()[0],
        "recorded_from": {
            "commit": argv[2] if len(argv) == 3 else None,
            # The tail of the path only: where it was unpacked is noise.
            "pyhoglake_file": "/".join(Path(pyhoglake.__file__).parts[-4:]),
            "pyhoglake_version": pyhoglake.__version__,
            "pyarrow": pa.__version__,
            # The footer stamp the hashes under ``cases`` carry; the replay
            # restamps its objects to it before hashing (``restamp``).
            "parquet_created_by": created_by,
            "python": sys.version.split()[0],
        },
        "cases": digest(cases),
    }
    Path(argv[1]).write_text(json.dumps(fixture, indent=2, sort_keys=True) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
