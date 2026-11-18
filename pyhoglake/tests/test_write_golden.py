"""Golden: on a table without the object-prefix option, the three writers
produce exactly the payloads and objects an older pyhoglake produced.

The fixture ``vectors/write_golden.json`` was recorded by
``golden_writes.py`` from a checkout of the version the working tree
must still match (its ``recorded_from`` names the commit), NOT from the
working tree: the previous golden compared the tree with itself. The
same module replays the cases here, so a difference is a change in what
goes over the wire — the commit body of ``append``, the request the
prepared calls return, and the bytes uploaded under each uri, all but
the footer's ``created_by``: parquet-cpp's version stamp, which the
replay rewrites to the recorded one before hashing (``restamp``), so a
pyarrow release that changes nothing else is not a difference, and a
changed data byte still is (both pinned below).
"""

import io
import json
import struct
import subprocess
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from golden_writes import WRITERS, created_by_of, digest, replay, restamp

import pyhoglake

FIXTURE = Path(__file__).parent / "vectors" / "write_golden.json"


@pytest.fixture(scope="module")
def golden():
    return json.loads(FIXTURE.read_text())


@pytest.fixture(scope="module")
def replayed_cases():
    return replay(pyhoglake)


@pytest.fixture(scope="module")
def replayed(golden, replayed_cases):
    return digest(
        replayed_cases, created_by=golden["recorded_from"]["parquet_created_by"]
    )


def test_the_fixture_was_recorded_from_another_checkout(golden):
    source = golden["recorded_from"]
    assert source["commit"], "re-record with the source commit named"
    assert source["pyhoglake_file"] == "pyhoglake/src/pyhoglake/__init__.py"
    assert source["parquet_created_by"], "re-record: the stamp the hashes carry"
    assert set(golden["cases"]) == {
        f"{writer}:deferred={deferred}"
        for writer in WRITERS
        for deferred in (False, True)
    }


def _git(*args: str) -> subprocess.CompletedProcess[str] | None:
    """``git`` in the tests' checkout, or None when git itself is not to
    be had (``--is-ancestor`` and ``cat-file -e`` answer by exit code, so
    a non-zero return is an answer, not an absence)."""
    try:
        return subprocess.run(
            ["git", *args],
            check=False,
            cwd=Path(__file__).parent,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None


def test_the_fixture_was_recorded_from_an_ancestor_of_head(golden):
    """The recorded commit is in HEAD's history: an older pyhoglake this
    tree descends from, so the comparison is older-vs-newer and never the
    tree with itself. (Not the merge-base with origin/main: that moves
    with every merge and rebase.) Skipped where git, or the commit object
    (a shallow CI checkout), is not to be had."""
    commit = golden["recorded_from"]["commit"]
    present = _git("cat-file", "-e", f"{commit}^{{commit}}")
    if present is None:
        pytest.skip("git is not available")
    if present.returncode != 0:
        pytest.skip(
            f"commit {commit} is not in this checkout: {present.stderr.strip()}"
        )
    ancestor = _git("merge-base", "--is-ancestor", commit, "HEAD")
    assert ancestor is not None
    assert ancestor.returncode == 0, (
        f"{commit} is not an ancestor of HEAD ({ancestor.stderr.strip()}); "
        "re-record the fixture from a commit this tree descends from "
        "(see golden_writes.py)"
    )


@pytest.mark.parametrize("deferred", [False, True])
@pytest.mark.parametrize("writer", WRITERS)
def test_without_the_option_the_wire_payload_is_the_recorded_one(
    golden, replayed, writer, deferred
):
    case = f"{writer}:deferred={deferred}"
    assert replayed[case]["payload"] == golden["cases"][case]["payload"]


@pytest.mark.parametrize("deferred", [False, True])
@pytest.mark.parametrize("writer", WRITERS)
def test_without_the_option_the_uploaded_objects_are_the_recorded_ones(
    golden, replayed, writer, deferred
):
    case = f"{writer}:deferred={deferred}"
    assert replayed[case]["uploaded"] == golden["cases"][case]["uploaded"]


CASE = "append:deferred=False"


def _first_object(cases) -> tuple[str, bytes]:
    uri, body = next(iter(cases[CASE]["objects"].items()))
    return uri, body


def test_restamp_rewrites_the_created_by_stamp_and_nothing_else(replayed_cases):
    """The normalisation is exactly the footer's ``created_by``: a stamp of
    another length round-trips to the original bytes, and the column
    chunks (everything before the footer) are never touched."""
    _, body = _first_object(replayed_cases)
    original = created_by_of(body)
    longer = "parquet-cpp-arrow version 100.0.0-SNAPSHOT"
    assert len(longer) != len(original)

    restamped = restamp(body, longer)
    assert created_by_of(restamped) == longer
    assert len(restamped) - len(body) == len(longer) - len(original)
    assert restamp(restamped, original) == body
    assert restamp(body, original) is body

    (footer_len,) = struct.unpack("<I", body[-8:-4])
    data_end = len(body) - 8 - footer_len
    assert restamped[:data_end] == body[:data_end]
    # The footer parses to the same metadata, longer by the stamp's delta.
    assert pq.read_metadata(io.BytesIO(restamped)).to_dict() == {
        **pq.read_metadata(io.BytesIO(body)).to_dict(),
        "created_by": longer,
        "serialized_size": footer_len + len(longer) - len(original),
    }


def test_a_changed_byte_in_a_data_page_still_fails_the_comparison(
    golden, replayed_cases
):
    """What the stamp rewrite must not mask: the last byte of the first
    column chunk's pages — the values, not metadata — flipped, the object
    no longer matches the fixture under the same normalisation, and the
    footer (with its stamp) is intact, so it was not the footer that
    changed."""
    uri, body = _first_object(replayed_cases)
    column = pq.read_metadata(io.BytesIO(body)).row_group(0).column(0)
    first_page = min(
        offset
        for offset in (column.dictionary_page_offset, column.data_page_offset)
        if offset is not None
    )
    position = first_page + column.total_compressed_size - 1
    (footer_len,) = struct.unpack("<I", body[-8:-4])
    assert position < len(body) - 8 - footer_len, "inside the pages, not the footer"

    mutated = body[:position] + bytes([body[position] ^ 0x01]) + body[position + 1 :]
    assert created_by_of(mutated) == created_by_of(body)
    cases = {CASE: {**replayed_cases[CASE], "objects": {uri: mutated}}}
    stamp = golden["recorded_from"]["parquet_created_by"]
    assert (
        digest(cases, created_by=stamp)[CASE]["uploaded"][uri]
        != golden["cases"][CASE]["uploaded"][uri]
    )
