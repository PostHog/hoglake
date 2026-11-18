"""ci/release_tags.py: which tags a release makes, and whether a re-run may skip one.

python -m unittest discover -s ci -p 'test_release_tags.py'
"""

from __future__ import annotations

import contextlib
import io
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import release_tags as rt


def git(root: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-c", "user.name=t", "-c", "user.email=t@t", *args],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def repo(tmp_path: Path, train: str, codec: str, tags: tuple[str, ...] = ()) -> Path:
    """A git repository holding the two Sampo manifests at the given versions, with the given tags."""
    (tmp_path / rt.CODEC).mkdir(parents=True)
    (tmp_path / "pyproject.toml").write_text(
        f'[project]\nname = "hoglake"\nversion = "{train}"\n'
    )
    (tmp_path / rt.CODEC / "pyproject.toml").write_text(
        f'[project]\nname = "{rt.CODEC}"\nversion = "{codec}"\n'
    )
    git(tmp_path, "init", "-q")
    git(tmp_path, "add", "-A")
    git(tmp_path, "commit", "-qm", "versions")
    for tag in tags:
        git(tmp_path, "tag", "-a", tag, "-m", tag)
    return tmp_path


def set_versions(root: Path, train: str, codec: str) -> None:
    (root / "pyproject.toml").write_text(
        f'[project]\nname = "hoglake"\nversion = "{train}"\n'
    )
    (root / rt.CODEC / "pyproject.toml").write_text(
        f'[project]\nname = "{rt.CODEC}"\nversion = "{codec}"\n'
    )


RELEASED = ("v1.4.11", "pyhoglake-v1.4.11", "pyhoglake-variant-codec-v0.1.1")


class ReleaseTagsTest(unittest.TestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.tmp_path = Path(tmp.name)

    def test_train_only_release_ignores_the_codec_tag(self) -> None:
        root = repo(self.tmp_path, "1.4.11", "0.1.1", RELEASED)
        set_versions(root, "1.5.0", "0.1.1")  # sampo moved the train only
        self.assertEqual(
            rt.plan(root, "1.4.11", "0.1.1"),
            {
                "train-version": "1.5.0",
                "train-release": "true",
                "codec-version": "0.1.1",
                "codec-release": "false",
            },
        )

    def test_codec_only_release_ignores_the_train_tags(self) -> None:
        root = repo(self.tmp_path, "1.4.11", "0.1.1", RELEASED)
        set_versions(root, "1.4.11", "0.1.2")
        self.assertEqual(
            rt.plan(root, "1.4.11", "0.1.1"),
            {
                "train-version": "1.4.11",
                "train-release": "false",
                "codec-version": "0.1.2",
                "codec-release": "true",
            },
        )

    def test_both_moved(self) -> None:
        root = repo(self.tmp_path, "1.4.11", "0.1.1", RELEASED)
        set_versions(root, "1.4.12", "0.2.0")
        out = rt.plan(root, "1.4.11", "0.1.1")
        self.assertEqual((out["train-release"], out["codec-release"]), ("true", "true"))

    def test_nothing_moved_is_an_error(self) -> None:
        root = repo(self.tmp_path, "1.4.11", "0.1.1", RELEASED)
        with self.assertRaisesRegex(SystemExit, "nothing to release"):
            rt.plan(root, "1.4.11", "0.1.1")

    def test_a_moved_version_whose_tag_exists_is_an_error(self) -> None:
        root = repo(self.tmp_path, "1.4.11", "0.1.1", RELEASED + ("v1.5.0",))
        set_versions(root, "1.5.0", "0.1.1")
        with self.assertRaisesRegex(SystemExit, "already exist.*v1.5.0"):
            rt.plan(root, "1.4.11", "0.1.1")
        root2 = repo(
            self.tmp_path / "two",
            "1.4.11",
            "0.1.1",
            RELEASED + ("pyhoglake-variant-codec-v0.1.2",),
        )
        set_versions(root2, "1.4.11", "0.1.2")
        with self.assertRaisesRegex(SystemExit, "pyhoglake-variant-codec-v0.1.2"):
            rt.plan(root2, "1.4.11", "0.1.1")

    def test_an_untagged_codec_is_released_even_when_it_did_not_move(self) -> None:
        # A hand bump merged earlier (0.1.2, never tagged), then a train-only changeset.
        root = repo(
            self.tmp_path,
            "1.4.11",
            "0.1.2",
            ("v1.4.11", "pyhoglake-v1.4.11", "pyhoglake-variant-codec-v0.1.1"),
        )
        set_versions(root, "1.5.0", "0.1.2")
        err = io.StringIO()
        with contextlib.redirect_stderr(err):
            out = rt.plan(root, "1.4.11", "0.1.2")
        self.assertTrue(
            out["codec-release"] == "true" and out["train-release"] == "true"
        )
        self.assertIn("has no tag yet", err.getvalue())
        # And on its own, with no train change, it is still a release (not "nothing to do").
        root2 = repo(
            self.tmp_path / "two",
            "1.4.11",
            "0.1.2",
            ("v1.4.11", "pyhoglake-variant-codec-v0.1.1"),
        )
        with contextlib.redirect_stderr(io.StringIO()):
            out2 = rt.plan(root2, "1.4.11", "0.1.2")
        self.assertEqual(
            (out2["train-release"], out2["codec-release"]), ("false", "true")
        )

    def test_tag_state(self) -> None:
        root = repo(self.tmp_path, "1.4.11", "0.1.1", ("v1.4.11",))
        head = git(root, "rev-parse", "HEAD")
        self.assertEqual(rt.tag_state("v1.4.11", head, root), "present")
        self.assertEqual(
            rt.tag_state("v1.4.11", head[:10], root), "present"
        )  # an abbreviated commit
        self.assertEqual(rt.tag_state("v1.5.0", head, root), "absent")
        set_versions(root, "1.5.0", "0.1.1")
        git(root, "commit", "-qam", "next")
        with self.assertRaisesRegex(SystemExit, "points at"):
            rt.tag_state("v1.4.11", git(root, "rev-parse", "HEAD"), root)

    def test_cli_plan_and_tag_state(self) -> None:
        root = repo(self.tmp_path, "1.4.11", "0.1.1", RELEASED)
        set_versions(root, "1.5.0", "0.1.1")
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(root)
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            self.assertEqual(
                rt.main(
                    ["plan", "--train-before", "1.4.11", "--codec-before", "0.1.1"]
                ),
                0,
            )
        out = stdout.getvalue().splitlines()
        self.assertTrue(
            "train-release=true" in out
            and "codec-release=false" in out
            and "train-version=1.5.0" in out
        )
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            self.assertEqual(
                rt.main(
                    [
                        "tag-state",
                        "--tag",
                        "v1.4.11",
                        "--commit",
                        git(root, "rev-parse", "HEAD"),
                    ]
                ),
                0,
            )
        self.assertEqual(stdout.getvalue().strip(), "present")

    def test_names_codec(self) -> None:
        for text, expected in (
            ("---\npyhoglake-variant-codec: patch\n---\n\nFix.\n", True),
            (
                "---\nhoglake: minor (Server)\npypi/pyhoglake-variant-codec: minor\n---\n",
                True,
            ),
            (
                "---\nhoglake: patch (pyhoglake)\n---\n\npyhoglake-variant-codec: patch is mentioned in the body only.\n",
                False,
            ),
            ("---\npyhoglake-variant-codec-v2: patch\n---\n", False),
            ("pyhoglake-variant-codec: patch\n", False),  # no front matter
            (
                "Title\npyhoglake-variant-codec: patch\n",
                False,
            ),  # no front matter, entry on line 2
            (
                "---\nmy-pyhoglake-variant-codec: patch\n---\n",
                False,
            ),  # another package
            ("---\npyhoglake-variant-codec: big\n---\n", False),
            ("", False),
        ):
            with self.subTest(text=text):
                self.assertIs(rt.names_codec(text), expected)

    def test_cli_codec_changeset(self) -> None:
        tmp_path = self.tmp_path
        a = tmp_path / "a.md"
        a.write_text("---\nhoglake: patch\n---\n")
        b = tmp_path / "b.md"
        b.write_text("---\npyhoglake-variant-codec: patch\n---\n")
        with (
            contextlib.redirect_stdout(io.StringIO()),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            self.assertEqual(rt.main(["codec-changeset", str(a)]), 1)
            self.assertEqual(rt.main(["codec-changeset", str(a), str(b)]), 0)
            self.assertEqual(
                rt.main(["codec-changeset", str(tmp_path / "missing.md")]), 1
            )
            self.assertEqual(rt.main(["codec-changeset"]), 1)


if __name__ == "__main__":
    unittest.main()
