"""Golden: on a table without the object-prefix option, the three writers
produce exactly the payloads and objects an older pyhoglake produced.

The fixture ``vectors/write_golden.json`` was recorded by
``golden_writes.py`` from a checkout of the version the working tree
must still match (its ``recorded_from`` names the commit), NOT from the
working tree: the previous golden compared the tree with itself. The
same module replays the cases here, so a difference is a change in what
goes over the wire — the commit body of ``append``, the request the
prepared calls return, and the bytes uploaded under each uri.
"""

import json
import subprocess
from pathlib import Path

import pytest
from golden_writes import WRITERS, record

import pyhoglake

FIXTURE = Path(__file__).parent / "vectors" / "write_golden.json"


@pytest.fixture(scope="module")
def golden():
    return json.loads(FIXTURE.read_text())


@pytest.fixture(scope="module")
def replayed():
    return record(pyhoglake)


def test_the_fixture_was_recorded_from_another_checkout(golden):
    source = golden["recorded_from"]
    assert source["commit"], "re-record with the source commit named"
    assert source["pyhoglake_file"] == "pyhoglake/src/pyhoglake/__init__.py"
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
