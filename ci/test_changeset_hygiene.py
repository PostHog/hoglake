"""ci/changeset_hygiene.py: the PR comment about changesets, or none.

uv run --project pyhoglake pytest ci/test_changeset_hygiene.py
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import changeset_hygiene as ch

TAGS = {"Server", "pyhoglake"}


def changeset(*entries: str, body: str = "Something changed.") -> str:
    return "---\n" + "\n".join(entries) + "\n---\n\n" + body + "\n"


def test_quiet_when_nothing_shipped_changed() -> None:
    files = [
        "server/src/test/kotlin/Foo.kt",
        "docs/auth.md",
        ".github/workflows/server.yml",
        "millrace/src/x.py",
        "AGENT.md",
    ]
    assert ch.analyse(files, {}, TAGS) == ""


def test_quiet_when_every_changed_package_is_named() -> None:
    files = [
        "server/src/main/kotlin/Foo.kt",
        "pyhoglake-variant-codec/src/lib.rs",
        "pyhoglake-variant-codec/CHANGELOG.md",
    ]
    sets = {
        ".sampo/changesets/a.md": changeset(
            "hoglake: minor (Server)", "pypi/pyhoglake-variant-codec: patch"
        )
    }
    assert ch.analyse(files, sets, TAGS) == ""


def test_missing_changeset_names_the_package_and_the_command() -> None:
    body = ch.analyse(["webui/src/App.tsx"], {}, TAGS)
    assert body.startswith(ch.MARKER)
    assert "`hoglake` changed but this PR has no changeset" in body
    assert "`sampo add -p hoglake`" in body
    assert "not blocked" in body


def test_missing_package_in_an_existing_changeset() -> None:
    files = ["pyhoglake/src/pyhoglake/x.py", "pyhoglake-variant-codec/Cargo.toml"]
    body = ch.analyse(files, {"c.md": changeset("hoglake: patch (pyhoglake)")}, TAGS)
    assert "`pyhoglake-variant-codec` changed but no changeset names it" in body
    assert "- `hoglake` — patch" in body


def test_extra_package_is_a_soft_note() -> None:
    body = ch.analyse(["docs/x.md"], {"c.md": changeset("hoglake: patch")}, TAGS)
    assert "Changeset names `hoglake` but none of its source changed" in body
    assert "merged earlier" in body


def test_unknown_package_bad_bump_and_bad_tag() -> None:
    sets = {"c.md": changeset("pyhoglake: patch", "hoglake: big (Webui)")}
    body = ch.analyse(["pyhoglake/src/a.py"], sets, TAGS)
    assert "`pyhoglake` is not a releasable package" in body
    assert "`hoglake: big` is not a bump level" in body
    assert "tag `(Webui)` is not in" in body
    assert "does not parse" in body


def test_malformed_front_matter() -> None:
    assert "no front matter" in ch.analyse([], {"c.md": "hoglake: patch\n"}, TAGS)
    assert "not closed" in ch.analyse([], {"c.md": "---\nhoglake: patch\n"}, TAGS)
    assert "cannot read `hoglake patch`" in ch.analyse(
        [], {"c.md": "---\nhoglake patch\n---\n"}, TAGS
    )
    assert "names no package" in ch.analyse([], {"c.md": "---\n---\nwords\n"}, TAGS)


def test_parse_changeset_accepts_the_forms_sampo_writes() -> None:
    entries, bad = ch.parse_changeset(
        "---\nhoglake: minor (Server)\npypi/pyhoglake-variant-codec: patch\n---\n\nText.\n"
    )
    assert bad == []
    assert entries == {
        "hoglake": ("minor", "Server"),
        "pyhoglake-variant-codec": ("patch", None),
    }


def test_repository_config_tags_are_read() -> None:
    assert {"Server", "pyhoglake", "Operators"} <= ch.configured_tags()


def test_shipped_webui_and_gradle_files_belong_to_the_train() -> None:
    files = [
        "webui/index.html",
        "webui/public/favicon.svg",
        "webui/vite.config.ts",
        "webui/nginx.conf.template",
        "webui/10-resolver.envsh",
        "webui/package-lock.json",
        "server/settings.gradle.kts",
        "server/gradle/wrapper/gradle-wrapper.properties",
        "server/trino/build.gradle.kts",
    ]
    for file in files:
        assert ch.affected_packages([file]) == {"hoglake"}, file
    assert (
        ch.affected_packages(
            [
                "webui/test/App.test.tsx",
                "server/schema.sql",
                "server/docker-compose.yml",
            ]
        )
        == set()
    )
