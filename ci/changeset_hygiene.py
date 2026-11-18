#!/usr/bin/env python3
"""Informational changeset check for a pull request (changeset-hygiene.yml).

Compares the Sampo packages a PR touches with the packages its changesets
name, and prints a sticky-comment body for the workflow to post (or an
empty `body=` so it deletes a stale one). It never fails the PR: a refactor
or a CI change legitimately ships without a changeset. The one hard rule,
a codec source change needs a codec changeset, is ci-python.yml's
`codec-version`.

    BASE_REF=main python3 ci/changeset_hygiene.py >> "$GITHUB_OUTPUT"

What counts as a package's source is the list below, not the whole tree:
tests, docs and CI never need a changeset.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Repository-relative prefixes of what ships in each Sampo package. A file
# outside every prefix (tests, docs, CI, millrace, bench) needs no changeset.
PACKAGE_PATHS: dict[str, tuple[str, ...]] = {
    "hoglake": (
        "server/src/main/",
        "server/build.gradle.kts",
        "server/settings.gradle.kts",
        "server/gradle.properties",
        "server/gradle/",
        "server/trino/src/main/",
        "server/trino/build.gradle.kts",
        "server/Dockerfile",
        "webui/src/",
        "webui/public/",
        "webui/index.html",
        "webui/vite.config.ts",
        "webui/tsconfig.json",
        "webui/package.json",
        "webui/package-lock.json",
        "webui/nginx.conf.template",
        "webui/10-resolver.envsh",
        "webui/Dockerfile",
        "pyhoglake/src/",
        "pyhoglake/pyproject.toml",
        "hedgerow/src/",
        "hedgerow/pyproject.toml",
    ),
    "pyhoglake-variant-codec": (
        "pyhoglake-variant-codec/src/",
        "pyhoglake-variant-codec/Cargo.toml",
        "pyhoglake-variant-codec/Cargo.lock",
        "pyhoglake-variant-codec/pyproject.toml",
        "pyhoglake-variant-codec/rust-toolchain.toml",
    ),
}
# Files inside a package prefix that are written by the release, not by a change.
RELEASE_WRITTEN = ("CHANGELOG.md",)
BUMPS = ("patch", "minor", "major")
MARKER = "<!-- changeset-hygiene -->"

FRONT_MATTER_LINE = re.compile(
    r"^\s*(?:pypi/)?([A-Za-z0-9_.-]+)\s*:\s*(\S+)(?:\s*\(([^)]*)\))?\s*$"
)


def sh(*args: str) -> str:
    return subprocess.run(
        args, check=True, capture_output=True, text=True, cwd=ROOT
    ).stdout.strip()


def configured_tags(root: Path = ROOT) -> set[str]:
    with (root / ".sampo/config.toml").open("rb") as f:
        return set(tomllib.load(f).get("changesets", {}).get("tags", []))


def affected_packages(changed_files: list[str]) -> set[str]:
    affected: set[str] = set()
    for file in changed_files:
        if file.endswith(RELEASE_WRITTEN):
            continue
        for package, prefixes in PACKAGE_PATHS.items():
            if any(file == p.rstrip("/") or file.startswith(p) for p in prefixes):
                affected.add(package)
    return affected


def parse_changeset(text: str) -> tuple[dict[str, tuple[str, str | None]], list[str]]:
    """Front-matter entries as {package: (bump, tag)} plus the lines that parse as nothing."""
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        return {}, ["no front matter (the file must start with a `---` line)"]
    entries: dict[str, tuple[str, str | None]] = {}
    bad: list[str] = []
    for line in lines[1:]:
        if line.strip() == "---":
            break
        if not line.strip():
            continue
        m = FRONT_MATTER_LINE.match(line)
        if not m:
            bad.append(line.strip())
            continue
        entries[m.group(1)] = (m.group(2), m.group(3))
    else:
        bad.append("front matter is not closed by a second `---` line")
    return entries, bad


def analyse(
    changed_files: list[str], changesets: dict[str, str], tags: set[str]
) -> str:
    """The comment body, or '' when nothing is worth saying."""
    affected = affected_packages(changed_files)
    declared: dict[str, str] = {}
    problems: list[str] = []
    for name, text in sorted(changesets.items()):
        entries, bad = parse_changeset(text)
        for line in bad:
            problems.append(f"`{name}`: cannot read `{line}`")
        if not entries and not bad:
            problems.append(f"`{name}`: names no package")
        for package, (bump, tag) in entries.items():
            if package not in PACKAGE_PATHS:
                problems.append(
                    f"`{name}`: `{package}` is not a releasable package (one of {', '.join(f'`{p}`' for p in PACKAGE_PATHS)})"
                )
            if bump not in BUMPS:
                problems.append(
                    f"`{name}`: `{package}: {bump}` is not a bump level (patch, minor or major)"
                )
            if tag is not None and tag not in tags:
                problems.append(
                    f"`{name}`: tag `({tag})` is not in `.sampo/config.toml` `changesets.tags`"
                )
            declared[package] = bump
    missing = sorted(affected - set(declared))
    extra = sorted(p for p in declared if p in PACKAGE_PATHS and p not in affected)

    if not problems and not missing and not extra:
        return ""

    if problems:
        summary = "A changeset in this PR does not parse"
    elif missing and not changesets:
        summary = f"{', '.join(f'`{p}`' for p in missing)} changed but this PR has no changeset"
    elif missing:
        summary = (
            f"{', '.join(f'`{p}`' for p in missing)} changed but no changeset names it"
        )
    else:
        summary = f"Changeset names {', '.join(f'`{p}`' for p in extra)} but none of its source changed"

    body = [
        MARKER,
        "<details open>",
        f"<summary>⚠️ {summary}</summary>",
        "",
        "This is informational — the PR is not blocked. Push a fix and this comment deletes itself.",
        "",
    ]
    if problems:
        body.append("**Changeset problems:**")
        body += [f"- {p}" for p in problems]
        body.append("")
    if missing:
        body.append("**Changed in this PR, named by no changeset:**")
        body += [f"- `{p}`" for p in missing]
        body.append("")
        body.append(
            "If users should hear about it, add a changeset: `sampo add -p "
            + " -p ".join(missing)
            + "` (CONTRIBUTING.md § Adding a changeset). A refactor, a test or a CI change needs none."
        )
        body.append("")
    if extra:
        body.append("**Named by a changeset, but none of its source changed:**")
        body += [f"- `{p}`" for p in extra]
        body.append("")
        body.append(
            "Fine if this releases something merged earlier, or documents a change outside the source paths."
        )
        body.append("")
    if declared:
        body.append("**Changesets in this PR:**")
        body += [f"- `{p}` — {b}" for p, b in sorted(declared.items())]
        body.append("")
    body.append("</details>")
    return "\n".join(body)


def main() -> int:
    base_ref = os.environ.get("BASE_REF", "main")
    merge_base = sh("git", "merge-base", f"origin/{base_ref}", "HEAD")
    changed = sh("git", "diff", "--name-only", f"{merge_base}...HEAD").split("\n")
    changed = [f for f in changed if f]
    added = sh(
        "git",
        "diff",
        "--name-only",
        "--diff-filter=AM",
        f"{merge_base}...HEAD",
        "--",
        ".sampo/changesets/",
    ).split("\n")
    changesets = {
        f: (ROOT / f).read_text(encoding="utf-8")
        for f in added
        if f.endswith(".md") and (ROOT / f).exists()
    }

    body = analyse(changed, changesets, configured_tags())
    if body:
        sys.stdout.write(
            f"body<<CHANGESET_HYGIENE_EOF\n{body}\nCHANGESET_HYGIENE_EOF\n"
        )
    else:
        sys.stdout.write("body=\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
