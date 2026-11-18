"""ci/changeset_hygiene.py: the PR comment about changesets, or none.

python -m unittest discover -s ci -p 'test_changeset_hygiene.py'
"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import changeset_hygiene as ch

TAGS = {"Server", "pyhoglake"}


def changeset(*entries: str, body: str = "Something changed.") -> str:
    return "---\n" + "\n".join(entries) + "\n---\n\n" + body + "\n"


class ChangesetHygieneTest(unittest.TestCase):
    def test_quiet_when_nothing_shipped_changed(self) -> None:
        files = [
            "server/src/test/kotlin/Foo.kt",
            "docs/auth.md",
            ".github/workflows/server.yml",
            "millrace/src/x.py",
            "AGENT.md",
        ]
        self.assertEqual(ch.analyse(files, {}, TAGS), "")

    def test_quiet_when_every_changed_package_is_named(self) -> None:
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
        self.assertEqual(ch.analyse(files, sets, TAGS), "")

    def test_missing_changeset_names_the_package_and_the_command(self) -> None:
        body = ch.analyse(["webui/src/App.tsx"], {}, TAGS)
        self.assertTrue(body.startswith(ch.MARKER))
        self.assertIn("`hoglake` changed but this PR has no changeset", body)
        self.assertIn("`sampo add -p hoglake`", body)
        self.assertIn("not blocked", body)

    def test_missing_package_in_an_existing_changeset(self) -> None:
        files = ["pyhoglake/src/pyhoglake/x.py", "pyhoglake-variant-codec/Cargo.toml"]
        body = ch.analyse(
            files, {"c.md": changeset("hoglake: patch (pyhoglake)")}, TAGS
        )
        self.assertIn(
            "`pyhoglake-variant-codec` changed but no changeset names it", body
        )
        self.assertIn("- `hoglake` — patch", body)

    def test_extra_package_is_a_soft_note(self) -> None:
        body = ch.analyse(["docs/x.md"], {"c.md": changeset("hoglake: patch")}, TAGS)
        self.assertIn("Changeset names `hoglake` but none of its source changed", body)
        self.assertIn("merged earlier", body)

    def test_unknown_package_bad_bump_and_bad_tag(self) -> None:
        sets = {"c.md": changeset("pyhoglake: patch", "hoglake: big (Webui)")}
        body = ch.analyse(["pyhoglake/src/a.py"], sets, TAGS)
        self.assertIn("`pyhoglake` is not a releasable package", body)
        self.assertIn("`hoglake: big` is not a bump level", body)
        self.assertIn("tag `(Webui)` is not in", body)
        self.assertIn("does not parse", body)

    def test_malformed_front_matter(self) -> None:
        self.assertIn(
            "no front matter", ch.analyse([], {"c.md": "hoglake: patch\n"}, TAGS)
        )
        self.assertIn(
            "not closed", ch.analyse([], {"c.md": "---\nhoglake: patch\n"}, TAGS)
        )
        self.assertIn(
            "cannot read `hoglake patch`",
            ch.analyse([], {"c.md": "---\nhoglake patch\n---\n"}, TAGS),
        )
        self.assertIn(
            "names no package", ch.analyse([], {"c.md": "---\n---\nwords\n"}, TAGS)
        )

    def test_parse_changeset_accepts_the_forms_sampo_writes(self) -> None:
        entries, bad = ch.parse_changeset(
            "---\nhoglake: minor (Server)\npypi/pyhoglake-variant-codec: patch\n---\n\nText.\n"
        )
        self.assertEqual(bad, [])
        self.assertEqual(
            entries,
            {
                "hoglake": ("minor", "Server"),
                "pyhoglake-variant-codec": ("patch", None),
            },
        )

    def test_repository_config_tags_are_read(self) -> None:
        self.assertLessEqual({"Server", "pyhoglake", "Operators"}, ch.configured_tags())

    def test_shipped_webui_and_gradle_files_belong_to_the_train(self) -> None:
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
            self.assertEqual(ch.affected_packages([file]), {"hoglake"}, file)
        self.assertEqual(
            ch.affected_packages(
                [
                    "webui/test/App.test.tsx",
                    "server/schema.sql",
                    "server/docker-compose.yml",
                ]
            ),
            set(),
        )


if __name__ == "__main__":
    unittest.main()
