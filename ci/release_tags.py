#!/usr/bin/env python3
"""The tag side of a release (release.yml), and the codec-changeset test.

    python3 ci/release_tags.py plan --train-before 1.4.11 --codec-before 0.1.1
        After `sampo release` + `release_versions.py sync`: prints
        GITHUB_OUTPUT lines (train-version, train-release, codec-version,
        codec-release) and fails when nothing is to be released or when a tag
        a released package needs already exists. A package is released when
        its version moved in this run, or, for the codec, when its current
        version has no tag yet (a hand bump, or a run that died before
        tagging): pyhoglake pins that version from PyPI, so an untagged codec
        must be published before the next pyhoglake.

    python3 ci/release_tags.py tag-state --tag v1.5.0 --commit <sha>
        Prints `absent`, or `present` when the tag already points at the
        commit (an idempotent re-run), and fails when it points elsewhere.

    python3 ci/release_tags.py codec-changeset FILE...
        Exit 0 when one of the changeset files names pyhoglake-variant-codec
        in its front matter (ci-python.yml's codec-version).

Stdlib only; git is called in the current directory.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tomllib
from pathlib import Path

CODEC = "pyhoglake-variant-codec"
CODEC_ENTRY = re.compile(
    r"^\s*(?:pypi/)?pyhoglake-variant-codec\s*:\s*(patch|minor|major)\b"
)


def git(*args: str, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True, check=False
    )


def tag_target(tag: str, cwd: Path | None = None) -> str | None:
    """The commit a tag points at (annotated tags dereferenced), or None."""
    result = git("rev-parse", "-q", "--verify", f"refs/tags/{tag}^{{commit}}", cwd=cwd)
    return result.stdout.strip() if result.returncode == 0 else None


def tag_state(tag: str, commit: str, cwd: Path | None = None) -> str:
    """'absent' or 'present'; raises when the tag points at another commit."""
    target = tag_target(tag, cwd)
    if target is None:
        return "absent"
    full = git("rev-parse", "--verify", f"{commit}^{{commit}}", cwd=cwd).stdout.strip()
    if target == full:
        return "present"
    raise SystemExit(f"tag {tag} exists and points at {target}, not at {commit}")


def project_version(manifest: Path) -> str:
    with manifest.open("rb") as f:
        return tomllib.load(f)["project"]["version"]


def tags_for(train: str | None, codec: str | None) -> list[str]:
    tags: list[str] = []
    if train is not None:
        tags += [f"v{train}", f"pyhoglake-v{train}"]
    if codec is not None:
        tags.append(f"{CODEC}-v{codec}")
    return tags


def plan(root: Path, train_before: str, codec_before: str) -> dict[str, str]:
    """What this run releases, as GITHUB_OUTPUT key/values. Raises on nothing-to-do or a clash."""
    train = project_version(root / "pyproject.toml")
    codec = project_version(root / CODEC / "pyproject.toml")
    train_release = train != train_before
    codec_moved = codec != codec_before
    codec_untagged = tag_target(f"{CODEC}-v{codec}", root) is None
    codec_release = codec_moved or codec_untagged
    if not train_release and not codec_release:
        raise SystemExit(
            f"nothing to release: hoglake is still {train} and {CODEC} {codec} is already tagged; "
            "the changesets name no releasable package"
        )
    clashes = [
        t
        for t in tags_for(
            train if train_release else None, codec if codec_moved else None
        )
        if tag_target(t, root)
    ]
    if clashes:
        raise SystemExit(
            "tag(s) already exist for a version this run would release: "
            + ", ".join(clashes)
        )
    if codec_release and not codec_moved:
        print(
            f"{CODEC} {codec} has no tag yet; releasing it with this run",
            file=sys.stderr,
        )
    return {
        "train-version": train,
        "train-release": "true" if train_release else "false",
        "codec-version": codec,
        "codec-release": "true" if codec_release else "false",
    }


def names_codec(changeset_text: str) -> bool:
    """Whether a changeset's front matter bumps the codec."""
    lines = changeset_text.splitlines()
    if not lines or lines[0].strip() != "---":
        return False
    for line in lines[1:]:
        if line.strip() == "---":
            return False
        if CODEC_ENTRY.match(line):
            return True
    return False


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("plan")
    p.add_argument("--train-before", required=True)
    p.add_argument("--codec-before", required=True)
    p.add_argument("--root", type=Path, default=Path.cwd())
    t = sub.add_parser("tag-state")
    t.add_argument("--tag", required=True)
    t.add_argument("--commit", required=True)
    c = sub.add_parser("codec-changeset")
    c.add_argument("files", nargs="*", type=Path)
    args = parser.parse_args(argv)

    if args.command == "plan":
        for key, value in plan(args.root, args.train_before, args.codec_before).items():
            print(f"{key}={value}")
        return 0
    if args.command == "tag-state":
        print(tag_state(args.tag, args.commit))
        return 0
    for file in args.files:
        if file.exists() and names_codec(file.read_text(encoding="utf-8")):
            print(f"{file} releases the codec")
            return 0
    print(f"no changeset names {CODEC}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
