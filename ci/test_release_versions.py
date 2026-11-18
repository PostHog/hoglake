"""ci/release_versions.py keeps the ten version strings and the codec pin in lockstep.

Run from the repository root:

    uv run --project pyhoglake pytest ci/test_release_versions.py

The synthetic tree mirrors the shape of each real file (other `version`
lines nearby, dependency versions in the JSON, a nested `version:` in the
spec) so a pattern that over-matches fails here. The last test runs the
script over a copy of the repository's real files.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import release_versions as rv

REPO_ROOT = Path(__file__).resolve().parent.parent

REAL_FILES = sorted(
    {
        str(rv.TRAIN_MANIFEST),
        str(rv.CODEC_MANIFEST),
        str(rv.UV_LOCK),
        *(str(f.path) for f in rv.FOLLOWERS),
    }
)


def write(root: Path, rel: str, text: str) -> None:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def make_tree(
    root: Path,
    train: str,
    codec: str,
    pin: str | None = None,
    lock_train: str | None = None,
) -> None:
    pin = pin or codec
    lock_train = lock_train or train
    write(
        root,
        "pyproject.toml",
        f'[tool.uv.workspace]\nmembers = ["pyhoglake"]\n\n[project]\nname = "hoglake"\nversion = "{train}"\n',
    )
    write(
        root,
        "pyhoglake-variant-codec/pyproject.toml",
        f'[project]\nname = "pyhoglake-variant-codec"\nversion = "{codec}"\n\n[tool.maturin]\nfeatures = ["python"]\n',
    )
    gradle = f'plugins {{\n    kotlin("jvm") version "2.4.21"\n}}\n\ngroup = "com.posthog.hoglake"\nversion = "{train}"\n\ntasks {{\n    val version = "9.9.9"\n}}\n'
    write(root, "server/build.gradle.kts", gradle)
    write(root, "server/trino/build.gradle.kts", gradle)
    write(
        root,
        "server/src/main/resources/openapi/hoglake.yaml",
        f"openapi: 3.0.3\ninfo:\n  title: hoglake\n  version: {train}\n  description: |\n    version: not this one\ncomponents:\n  schemas:\n    Info:\n      properties:\n        version:\n          type: string\n",
    )
    write(
        root,
        "webui/package.json",
        f'{{\n  "name": "hoglake-webui",\n  "private": true,\n  "version": "{train}",\n  "dependencies": {{\n    "react": "^19.3.1"\n  }}\n}}\n',
    )
    write(
        root,
        "webui/package-lock.json",
        f'{{\n  "name": "hoglake-webui",\n  "version": "{train}",\n  "lockfileVersion": 3,\n  "packages": {{\n    "": {{\n      "name": "hoglake-webui",\n      "version": "{train}",\n      "dependencies": {{\n        "react": "^19.3.1"\n      }}\n    }},\n    "node_modules/react": {{\n      "version": "19.3.1"\n    }}\n  }}\n}}\n',
    )
    write(
        root,
        "pyhoglake/pyproject.toml",
        f'[project]\nname = "pyhoglake"\nversion = "{train}"\ndependencies = ["httpx>=0.27", "pyarrow>=26.0.1"]\n\n[project.optional-dependencies]\nvariant-codec = ["""pyhoglake-variant-codec=={pin}; \\\n    platform_python_implementation == \'CPython\'"""]\n',
    )
    write(
        root,
        "hedgerow/pyproject.toml",
        f'[project]\nname = "hedgerow"\nversion = "{train}"\ndependencies = ["pyhoglake"]\n',
    )
    write(
        root,
        "uv.lock",
        "version = 1\n\n"
        f'[[package]]\nname = "hoglake"\nversion = "{lock_train}"\nsource = {{ virtual = "." }}\n\n'
        f'[[package]]\nname = "hedgerow"\nversion = "{lock_train}"\nsource = {{ editable = "hedgerow" }}\n\n'
        f'[[package]]\nname = "pyhoglake"\nversion = "{lock_train}"\nsource = {{ editable = "pyhoglake" }}\n\n'
        f'[[package]]\nname = "pyhoglake-variant-codec"\nversion = "{codec}"\nsource = {{ editable = "pyhoglake-variant-codec" }}\n\n'
        '[[package]]\nname = "react-is-not-python"\nversion = "1.4.11"\n',
    )


def all_strings(root: Path) -> dict[str, list[str]]:
    return {f"{f.path}#{f.source}": f.read(root) for f in rv.FOLLOWERS}


def test_sync_moves_every_follower_and_the_pin(tmp_path: Path) -> None:
    make_tree(tmp_path, train="1.4.11", codec="0.1.1")
    # Sampo bumped the two leaders; nothing else moved yet.
    rv.VersionString(rv.TRAIN_MANIFEST, rv.PYPROJECT_VERSION, "train").write(
        tmp_path, "1.5.0"
    )
    rv.VersionString(rv.CODEC_MANIFEST, rv.PYPROJECT_VERSION, "codec").write(
        tmp_path, "0.1.2"
    )
    assert rv.check(tmp_path), "the followers are stale before sync"

    changed = rv.sync(tmp_path, run_uv_lock=False)

    assert changed == sorted(
        {
            "server/build.gradle.kts",
            "server/trino/build.gradle.kts",
            "server/src/main/resources/openapi/hoglake.yaml",
            "webui/package.json",
            "webui/package-lock.json",
            "pyhoglake/pyproject.toml",
            "hedgerow/pyproject.toml",
        }
    )
    strings = all_strings(tmp_path)
    train = [
        v for key, values in strings.items() if key.endswith("#train") for v in values
    ]
    assert train == ["1.5.0"] * 8, strings  # 7 files, package-lock twice
    assert strings["pyhoglake/pyproject.toml#codec"] == ["0.1.2"]
    # uv.lock is not written without `uv lock`, so check() still names it and nothing else.
    problems = rv.check(tmp_path)
    assert problems and all(p.startswith("uv.lock:") for p in problems), problems


def test_sync_is_idempotent_and_touches_only_version_lines(tmp_path: Path) -> None:
    make_tree(tmp_path, train="1.4.11", codec="0.1.1")
    before = {f.path: (tmp_path / f.path).read_text() for f in rv.FOLLOWERS}
    rv.VersionString(rv.TRAIN_MANIFEST, rv.PYPROJECT_VERSION, "train").write(
        tmp_path, "1.4.12"
    )
    rv.sync(tmp_path, run_uv_lock=False)
    for follower in rv.FOLLOWERS:
        if follower.source != "train":
            continue
        old_lines = before[follower.path].splitlines()
        new_lines = (tmp_path / follower.path).read_text().splitlines()
        assert len(old_lines) == len(new_lines)
        differing = [i for i, (a, b) in enumerate(zip(old_lines, new_lines)) if a != b]
        assert len(differing) == follower.count, (follower.path, differing)
        for i in differing:
            assert old_lines[i].replace("1.4.11", "1.4.12") == new_lines[i]
    assert rv.sync(tmp_path, run_uv_lock=False) == []


def test_other_version_like_lines_are_left_alone(tmp_path: Path) -> None:
    make_tree(tmp_path, train="1.4.11", codec="0.1.1")
    rv.VersionString(rv.TRAIN_MANIFEST, rv.PYPROJECT_VERSION, "train").write(
        tmp_path, "2.0.0"
    )
    rv.sync(tmp_path, run_uv_lock=False)
    gradle = (tmp_path / "server/build.gradle.kts").read_text()
    assert (
        'kotlin("jvm") version "2.4.21"' in gradle and 'val version = "9.9.9"' in gradle
    )
    spec = (tmp_path / "server/src/main/resources/openapi/hoglake.yaml").read_text()
    assert (
        "version: not this one" in spec
        and "        version:\n          type: string" in spec
    )
    lock = (tmp_path / "webui/package-lock.json").read_text()
    assert '"version": "19.3.1"' in lock
    assert '"pyarrow>=26.0.1"' in (tmp_path / "pyhoglake/pyproject.toml").read_text()


def test_check_names_each_stray_string(tmp_path: Path) -> None:
    make_tree(tmp_path, train="1.4.12", codec="0.1.2", pin="0.1.1", lock_train="1.4.11")
    (tmp_path / "server/trino/build.gradle.kts").write_text(
        (tmp_path / "server/trino/build.gradle.kts")
        .read_text()
        .replace("1.4.12", "1.4.12-dev")
    )
    problems = rv.check(tmp_path)
    assert any(
        p.startswith("server/trino/build.gradle.kts: 1.4.12-dev") for p in problems
    )
    assert any(
        p.startswith("pyhoglake/pyproject.toml: 0.1.1") and "codec version" in p
        for p in problems
    )
    assert {p.split(":")[0] for p in problems if p.startswith("uv.lock")} == {"uv.lock"}
    assert (
        sum(p.startswith("uv.lock") for p in problems) == 3
    )  # hoglake, hedgerow, pyhoglake
    assert len(problems) == 5


def test_missing_string_is_an_error(tmp_path: Path) -> None:
    make_tree(tmp_path, train="1.4.11", codec="0.1.1")
    (tmp_path / "webui/package-lock.json").write_text(
        '{"name": "x", "version": "1.4.11", "packages": {"": {}}}\n'
    )
    with pytest.raises(SystemExit, match="package-lock.json: expected 2"):
        rv.check(tmp_path)


def test_cli_check_exit_codes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    make_tree(tmp_path, train="1.4.11", codec="0.1.1")
    assert rv.main(["check", "--root", str(tmp_path)]) == 0
    assert "all version strings in lockstep" in capsys.readouterr().out
    rv.VersionString(rv.TRAIN_MANIFEST, rv.PYPROJECT_VERSION, "train").write(
        tmp_path, "1.4.12"
    )
    assert rv.main(["check", "--root", str(tmp_path)]) == 1
    assert "out of lockstep" in capsys.readouterr().err
    assert (
        rv.main(["sync", "--no-lock", "--root", str(tmp_path)]) == 1
    )  # uv.lock still stale without `uv lock`


def test_repository_is_in_lockstep() -> None:
    assert rv.check(REPO_ROOT) == []


def test_sync_on_a_copy_of_the_real_files(tmp_path: Path) -> None:
    """All ten strings plus the codec pin move on the repository's own files."""
    for rel in REAL_FILES:
        (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(REPO_ROOT / rel, tmp_path / rel)
    current = rv.leaders(tmp_path)
    new_train, new_codec = "99.1.0", "0.99.0"
    rv.VersionString(rv.TRAIN_MANIFEST, rv.PYPROJECT_VERSION, "train").write(
        tmp_path, new_train
    )
    rv.VersionString(rv.CODEC_MANIFEST, rv.PYPROJECT_VERSION, "codec").write(
        tmp_path, new_codec
    )

    changed = rv.sync(tmp_path, run_uv_lock=False)

    assert len(changed) == 7
    strings = all_strings(tmp_path)
    assert [
        v for key, values in strings.items() if key.endswith("#train") for v in values
    ] == [new_train] * 8
    assert strings["pyhoglake/pyproject.toml#codec"] == [new_codec]
    for rel in changed:
        old, new = (
            (REPO_ROOT / rel).read_text().splitlines(),
            (tmp_path / rel).read_text().splitlines(),
        )
        assert len(old) == len(new), rel
        for a, b in zip(old, new):
            if a != b:
                assert (
                    a.replace(current["train"], new_train).replace(
                        current["codec"], new_codec
                    )
                    == b
                ), (rel, a, b)
    # The only thing left for `uv lock` is the lock itself.
    assert all(p.startswith("uv.lock:") for p in rv.check(tmp_path))


def test_verify_json_rejects_disagreeing_lock_records(tmp_path: Path) -> None:
    make_tree(tmp_path, train="1.4.11", codec="0.1.1")
    lock = tmp_path / "webui/package-lock.json"
    lock.write_text(
        lock.read_text().replace(
            '"version": "1.4.11",\n      "dependencies"',
            '"version": "1.4.10",\n      "dependencies"',
        )
    )
    with pytest.raises(
        SystemExit,
        match="package-lock.json: root version 1.4.11 != packages\\[''\\] 1.4.10",
    ):
        rv.verify_json(tmp_path, Path("webui/package-lock.json"))
    # Through sync: a lock whose second "version" is a dependency's, not the
    # root record's, is rewritten wrongly and must be caught, not committed.
    lock.write_text(
        '{\n  "name": "hoglake-webui",\n  "version": "1.4.11",\n  "packages": {\n    "": {\n      "name": "hoglake-webui"\n    },\n'
        '    "node_modules/react": {\n      "version": "19.3.1"\n    }\n  }\n}\n'
    )
    rv.VersionString(rv.TRAIN_MANIFEST, rv.PYPROJECT_VERSION, "train").write(
        tmp_path, "1.5.0"
    )
    with pytest.raises(
        SystemExit,
        match="package-lock.json: root version 1.5.0 != packages\\[''\\] None",
    ):
        rv.sync(tmp_path, run_uv_lock=False)


def test_check_reports_a_stale_codec_lock_entry(tmp_path: Path) -> None:
    make_tree(tmp_path, train="1.4.11", codec="0.1.2")
    lock = tmp_path / "uv.lock"
    lock.write_text(
        lock.read_text().replace(
            'name = "pyhoglake-variant-codec"\nversion = "0.1.2"',
            'name = "pyhoglake-variant-codec"\nversion = "0.1.1"',
        )
    )
    assert rv.check(tmp_path) == [
        "uv.lock: pyhoglake-variant-codec is 0.1.1 (expected 0.1.2)"
    ]


def test_check_reports_a_missing_lock_entry(tmp_path: Path) -> None:
    make_tree(tmp_path, train="1.4.11", codec="0.1.1")
    lock = tmp_path / "uv.lock"
    lock.write_text(
        lock.read_text().replace(
            '[[package]]\nname = "hedgerow"\nversion = "1.4.11"\nsource = { editable = "hedgerow" }\n\n',
            "",
        )
    )
    assert rv.check(tmp_path) == ["uv.lock: no entry for hedgerow"]


def test_sync_reads_back_the_pyproject_it_wrote(tmp_path: Path) -> None:
    make_tree(tmp_path, train="1.4.11", codec="0.1.1")
    # A `version =` line before [project] takes the regex's first match; the
    # TOML read-back notices that [project].version did not move.
    hedgerow = tmp_path / "hedgerow/pyproject.toml"
    hedgerow.write_text(
        '[tool.something]\nversion = "1.4.11"\n\n' + hedgerow.read_text()
    )
    rv.VersionString(rv.TRAIN_MANIFEST, rv.PYPROJECT_VERSION, "train").write(
        tmp_path, "1.5.0"
    )
    with pytest.raises(
        SystemExit, match="hedgerow/pyproject.toml: wrote 1.5.0 but reads back 1.4.11"
    ):
        rv.sync(tmp_path, run_uv_lock=False)


def test_sync_runs_uv_lock_and_checks_its_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    make_tree(tmp_path, train="1.4.11", codec="0.1.1")
    rv.VersionString(rv.TRAIN_MANIFEST, rv.PYPROJECT_VERSION, "train").write(
        tmp_path, "1.5.0"
    )
    rv.VersionString(rv.CODEC_MANIFEST, rv.PYPROJECT_VERSION, "codec").write(
        tmp_path, "0.1.2"
    )
    calls: list[list[str]] = []

    def uv_lock_that_does_nothing(args: list[str], **kwargs: object) -> None:
        calls.append(args)

    monkeypatch.setattr(rv.subprocess, "run", uv_lock_that_does_nothing)
    with pytest.raises(
        SystemExit,
        match="uv lock did not pick up the new version of hoglake, pyhoglake, hedgerow, pyhoglake-variant-codec",
    ):
        rv.sync(tmp_path)
    assert calls == [["uv", "lock"]]

    def uv_lock_that_rewrites_the_lock(args: list[str], **kwargs: object) -> None:
        lock = tmp_path / "uv.lock"
        lock.write_text(
            lock.read_text()
            .replace('version = "1.4.11"\nsource', 'version = "1.5.0"\nsource')
            .replace('version = "0.1.1"', 'version = "0.1.2"')
        )

    monkeypatch.setattr(rv.subprocess, "run", uv_lock_that_rewrites_the_lock)
    changed = rv.sync(tmp_path)
    assert "uv.lock" in changed and rv.check(tmp_path) == []
