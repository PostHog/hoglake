#!/usr/bin/env python3
"""Keep hoglake's version strings in lockstep with the two Sampo packages.

Sampo (.sampo/config.toml) owns two versions:

- `hoglake`, the release train, in the root pyproject.toml. It is the
  version of the server, the trino harness, the webui, the OpenAPI spec,
  pyhoglake and hedgerow, which release together under one `vX.Y.Z` tag.
- `pyhoglake-variant-codec`, in pyhoglake-variant-codec/pyproject.toml,
  released on its own.

Sampo bumps only those two files and refreshes uv.lock. This script copies
the train version into every other file that carries it, and the codec
version into the exact pin of pyhoglake's `variant-codec` extra, so the
strings `:checkOpenapiVersion` and the publish workflows compare still
agree. `release.yml` runs `sync` right after `sampo release`; `check`
verifies the lockstep and exits 1 when a string is out of line.

    python3 ci/release_versions.py sync [--no-lock] [--root DIR]
    python3 ci/release_versions.py check [--root DIR]

Stdlib only: the release job runs it with the runner's python3.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path

TRAIN_MANIFEST = Path("pyproject.toml")
CODEC_MANIFEST = Path("pyhoglake-variant-codec/pyproject.toml")
PYHOGLAKE_MANIFEST = Path("pyhoglake/pyproject.toml")
UV_LOCK = Path("uv.lock")

# The same pattern :checkOpenapiVersion applies (server/build.gradle.kts).
OPENAPI_VERSION = re.compile(r"(?m)^\s{2}version:\s*(\S+)\s*$")
GRADLE_VERSION = re.compile(r'(?m)^version = "([^"]+)"$')
PYPROJECT_VERSION = re.compile(r'(?m)^version = "([^"]+)"$')
JSON_VERSION = re.compile(r'"version": "([^"]+)"')
CODEC_PIN = re.compile(r"pyhoglake-variant-codec==([0-9A-Za-z.+!-]+)")


@dataclass(frozen=True)
class VersionString:
    """One version string: where it lives, how to find it, what it follows."""

    path: Path
    pattern: re.Pattern[str]
    source: str  # "train" or "codec"
    # How many leading matches carry the version. package-lock.json has
    # two (the root record and the "" package); every other file has one.
    count: int = 1

    def read(self, root: Path) -> list[str]:
        text = (root / self.path).read_text(encoding="utf-8")
        found = [m.group(1) for m in self.pattern.finditer(text)]
        if len(found) < self.count:
            raise SystemExit(
                f"{self.path}: expected {self.count} version string(s) matching "
                f"{self.pattern.pattern!r}, found {len(found)}"
            )
        return found[: self.count]

    def write(self, root: Path, version: str) -> bool:
        """Rewrite the leading matches to `version`; True when the file changed."""
        file = root / self.path
        text = file.read_text(encoding="utf-8")
        seen = 0

        def replace(match: re.Match[str]) -> str:
            nonlocal seen
            seen += 1
            if seen > self.count:
                return match.group(0)
            start, end = match.span(1)
            offset = match.start()
            return (
                match.group(0)[: start - offset]
                + version
                + match.group(0)[end - offset :]
            )

        new_text = self.pattern.sub(replace, text)
        if seen < self.count:
            raise SystemExit(
                f"{self.path}: expected {self.count} version string(s) matching "
                f"{self.pattern.pattern!r}, found {seen}"
            )
        if new_text == text:
            return False
        file.write_text(new_text, encoding="utf-8")
        return True


# Every version string that is not Sampo's, in the order the release PRs
# used to list them. The two Sampo-owned manifests are read, never written.
FOLLOWERS: tuple[VersionString, ...] = (
    VersionString(Path("server/build.gradle.kts"), GRADLE_VERSION, "train"),
    VersionString(Path("server/trino/build.gradle.kts"), GRADLE_VERSION, "train"),
    VersionString(
        Path("server/src/main/resources/openapi/hoglake.yaml"), OPENAPI_VERSION, "train"
    ),
    VersionString(Path("webui/package.json"), JSON_VERSION, "train"),
    VersionString(Path("webui/package-lock.json"), JSON_VERSION, "train", count=2),
    VersionString(PYHOGLAKE_MANIFEST, PYPROJECT_VERSION, "train"),
    VersionString(Path("hedgerow/pyproject.toml"), PYPROJECT_VERSION, "train"),
    VersionString(PYHOGLAKE_MANIFEST, CODEC_PIN, "codec"),
)

# uv.lock entries that must agree with their pyproject.toml; `uv lock`
# writes them, this script only checks them.
LOCKED_PACKAGES = {
    "hoglake": "train",
    "pyhoglake": "train",
    "hedgerow": "train",
    "pyhoglake-variant-codec": "codec",
}


def project_version(root: Path, manifest: Path) -> str:
    with (root / manifest).open("rb") as f:
        data = tomllib.load(f)
    try:
        return data["project"]["version"]
    except KeyError as e:
        raise SystemExit(f"{manifest}: no [project].version") from e


def leaders(root: Path) -> dict[str, str]:
    return {
        "train": project_version(root, TRAIN_MANIFEST),
        "codec": project_version(root, CODEC_MANIFEST),
    }


def lock_versions(root: Path) -> dict[str, str]:
    with (root / UV_LOCK).open("rb") as f:
        lock = tomllib.load(f)
    return {
        p["name"]: p["version"]
        for p in lock.get("package", [])
        if p["name"] in LOCKED_PACKAGES
    }


def verify_json(root: Path, path: Path) -> None:
    """A regex edit of a JSON file must still parse (and keep the lock's two records equal)."""
    data = json.loads((root / path).read_text(encoding="utf-8"))
    if "packages" in data:
        root_record = data["packages"].get("", {}).get("version")
        if data.get("version") != root_record:
            raise SystemExit(
                f"{path}: root version {data.get('version')} != packages[''] {root_record}"
            )


def sync(root: Path, run_uv_lock: bool = True) -> list[str]:
    """Copy the Sampo-owned versions into every follower. Returns the files changed."""
    versions = leaders(root)
    changed: list[str] = []
    for follower in FOLLOWERS:
        if follower.write(root, versions[follower.source]):
            changed.append(str(follower.path))
        if follower.path.suffix == ".json":
            verify_json(root, follower.path)
    # pyproject.toml edits must still be TOML with the version we meant.
    for manifest, expected in (
        (PYHOGLAKE_MANIFEST, versions["train"]),
        (Path("hedgerow/pyproject.toml"), versions["train"]),
    ):
        if project_version(root, manifest) != expected:
            raise SystemExit(
                f"{manifest}: wrote {expected} but reads back {project_version(root, manifest)}"
            )
    if run_uv_lock:
        # pyhoglake's and hedgerow's lock entries follow their pyproject.
        # `uv lock` re-resolves nothing when only versions moved, so it is
        # quick and needs no network for a lock that is otherwise current.
        subprocess.run(["uv", "lock"], cwd=root, check=True)
        locked = lock_versions(root)
        stale = [n for n, s in LOCKED_PACKAGES.items() if locked.get(n) != versions[s]]
        if stale:
            raise SystemExit(
                f"uv lock did not pick up the new version of {', '.join(stale)}"
            )
        changed.append(str(UV_LOCK))
    return sorted(set(changed))


def check(root: Path) -> list[str]:
    """Every follower and lock entry agrees with its leader. Returns the problems."""
    versions = leaders(root)
    problems: list[str] = []
    for follower in FOLLOWERS:
        for found in follower.read(root):
            if found != versions[follower.source]:
                problems.append(
                    f"{follower.path}: {found} (expected {versions[follower.source]}, the {follower.source} version)"
                )
    locked = lock_versions(root)
    for name, source in LOCKED_PACKAGES.items():
        if name not in locked:
            problems.append(f"uv.lock: no entry for {name}")
        elif locked[name] != versions[source]:
            problems.append(
                f"uv.lock: {name} is {locked[name]} (expected {versions[source]})"
            )
    return problems


def report(root: Path) -> str:
    versions = leaders(root)
    lines = [
        f"hoglake (train): {versions['train']}",
        f"pyhoglake-variant-codec: {versions['codec']}",
    ]
    for follower in FOLLOWERS:
        for found in follower.read(root):
            lines.append(f"  {follower.path} [{follower.pattern.pattern}]: {found}")
    for name, version in sorted(lock_versions(root).items()):
        lines.append(f"  uv.lock {name}: {version}")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("command", choices=["sync", "check"])
    parser.add_argument(
        "--root", type=Path, default=Path(__file__).resolve().parent.parent
    )
    parser.add_argument(
        "--no-lock", action="store_true", help="sync: do not run `uv lock` afterwards"
    )
    args = parser.parse_args(argv)
    root = args.root.resolve()

    if args.command == "sync":
        changed = sync(root, run_uv_lock=not args.no_lock)
        print("changed: " + (", ".join(changed) if changed else "nothing"))
        print(report(root))
        problems = check(root)
    else:
        print(report(root))
        problems = check(root)
    if problems:
        print("\nout of lockstep:", file=sys.stderr)
        for p in problems:
            print(f"  {p}", file=sys.stderr)
        return 1
    print("\nall version strings in lockstep")
    return 0


if __name__ == "__main__":
    sys.exit(main())
