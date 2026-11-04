"""Command-level workflow, launcher, inventory and XML regression fixtures."""

import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml
from check_hoglite_results import ROOT, required_reports


def workflow(name):
    return yaml.load(
        (ROOT / f".github/workflows/{name}.yml").read_text(), Loader=yaml.BaseLoader
    )


class WorkflowTest(unittest.TestCase):
    def test_required_postgres_job_is_credential_free(self):
        self.assertEqual(
            workflow("server")["jobs"]["server-test"]["permissions"],
            {"contents": "read"},
        )

    def test_external_graph_and_credentials(self):
        server = workflow("server")
        live = workflow("hoglite-live")
        self.assertEqual(server["name"], "Server")
        self.assertEqual(set(live["on"]), {"push"})
        self.assertEqual(live["on"]["push"]["branches"], ["main"])
        self.assertIn("ci/**", live["on"]["push"]["paths"])
        self.assertIn(".github/workflows/**", live["on"]["push"]["paths"])
        self.assertEqual(set(live["jobs"]), {"hoglite-live"})
        self.assertEqual(
            server["jobs"]["server-checks"]["needs"],
            ["changes", "server-test", "python-live"],
        )
        self.assertEqual(server["jobs"]["deploy"]["needs"], ["server-checks"])
        self.assertEqual(
            server["jobs"]["python-live"]["uses"], "./.github/workflows/python-live.yml"
        )
        self.assertEqual(
            server["jobs"]["server-test"]["permissions"], {"contents": "read"}
        )
        self.assertEqual(
            live["jobs"]["hoglite-live"]["permissions"],
            {"contents": "read", "id-token": "write"},
        )
        self.assertNotIn("env", server["jobs"]["server-test"])
        for job in server["jobs"].values():
            for step in job.get("steps", []):
                self.assertNotIn("aws-actions/", step.get("uses", ""))
                self.assertNotIn("fetch-hoglited", step.get("run", ""))
        for step in live["jobs"]["hoglite-live"]["steps"]:
            self.assertNotIn("continue-on-error", step)
            self.assertNotIn("|| true", step.get("run", ""))
        pins = live["jobs"]["hoglite-live"]["env"]
        self.assertRegex(pins["HOGLITE_COMMIT"], r"^[0-9a-f]{40}$")
        self.assertRegex(pins["HOGLITED_IMAGE_DIGEST"], r"^sha256:[0-9a-f]{64}$")
        self.assertEqual(
            workflow("ecr-release")["on"]["workflow_run"]["workflows"],
            ["Server", "Webui"],
        )

    def test_main_only_event_matrix(self):
        condition = workflow("hoglite-live")["jobs"]["hoglite-live"]["if"]

        def accepts(event, ref, repo):
            values = {
                "github.event_name": event,
                "github.ref": ref,
                "github.repository": repo,
            }
            result = []
            for clause in condition.split(" && "):
                match = re.fullmatch(r"(github\.[\w_]+) == '([^']+)'", clause)
                self.assertIsNotNone(match, "unreviewed credential condition")
                result.append(values[match[1]] == match[2])
            return all(result)

        for event, ref, repo, expected in [
            ("push", "refs/heads/main", "PostHog/hoglake", True),
            ("pull_request", "refs/pull/1/merge", "PostHog/hoglake", False),
            ("pull_request", "refs/pull/1/merge", "fork/hoglake", False),
            ("push", "refs/heads/other", "PostHog/hoglake", False),
            ("push", "refs/heads/main", "fork/hoglake", False),
            ("workflow_dispatch", "refs/heads/main", "PostHog/hoglake", False),
        ]:
            self.assertEqual(accepts(event, ref, repo), expected)

    def test_required_gate_command_fails_closed(self):
        job = workflow("server")["jobs"]["server-checks"]
        self.assertEqual(
            job["if"],
            "always() && (needs.changes.result != 'success' || needs.changes.outputs.server == 'true')",
        )
        command = job["steps"][0]["run"]
        for changes, pg, live in [
            ("success", "success", "success"),
            ("failure", "success", "success"),
            ("success", "failure", "success"),
            ("success", "success", "failure"),
            ("success", "skipped", "success"),
        ]:
            result = subprocess.run(
                ["bash", "-euc", command],
                env={
                    **os.environ,
                    "CHANGES_RESULT": changes,
                    "SERVER_RESULT": pg,
                    "LIVE_RESULT": live,
                },
                check=False,
                capture_output=True,
            )
            self.assertEqual(result.returncode == 0, changes == pg == live == "success")


class CommandTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "ci").mkdir()
        (self.root / "server").mkdir()
        (self.root / "bin").mkdir()
        for name in ["test-postgres.sh", "hoglite-live.sh", "fetch-hoglited.sh"]:
            shutil.copy2(ROOT / "ci" / name, self.root / "ci" / name)
        self.env = {
            **os.environ,
            "PATH": f"{self.root}/bin:/usr/bin:/bin",
            "RUNNER_TEMP": str(self.root),
            "GITHUB_ENV": str(self.root / "env"),
            "GITHUB_STEP_SUMMARY": str(self.root / "summary"),
        }
        self.env.pop("HOGLITE_BINARY", None)
        self.write(
            "server/gradlew",
            '#!/bin/bash\n[ "${HOGLITE_BINARY+x}" != x ] || [ "${LIVE:-}" = yes ] || exit 97\nprintf "%s\\n" "$@" > "$(dirname "$0")/args"\nexit "${GRADLE_STATUS:-0}"\n',
        )

    def write(self, name, text):
        path = self.root / name
        path.write_text(text)
        path.chmod(0o755)

    def run_script(self, script, *args, **env):
        return subprocess.run(
            ["bash", str(self.root / "ci" / script), *args],
            env={**self.env, **env},
            check=False,
            capture_output=True,
            text=True,
        )

    def test_pg_absence_not_empty_and_no_path_fallback(self):
        result = self.run_script("test-postgres.sh", HOGLITE_BINARY="inherited")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(
            (self.root / "server/args").read_text().splitlines(),
            [
                "-p",
                "server",
                "--no-daemon",
                ":test",
                ":ktlintCheck",
                ":checkOpenapiVersion",
            ],
        )
        self.write("bin/hoglited", "#!/bin/sh\nexit 0\n")
        result = self.run_script("test-postgres.sh")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("PATH fallback", result.stderr)

    def test_live_test_failure_not_masked_and_full_suite(self):
        self.write("engine", "#!/bin/sh\nexit 0\n")
        result = self.run_script(
            "hoglite-live.sh",
            "test",
            HOGLITE_BINARY=str(self.root / "engine"),
            LIVE="yes",
            GRADLE_STATUS="23",
        )
        self.assertEqual(result.returncode, 23, result.stderr)
        self.assertEqual(
            (self.root / "server/args").read_text().splitlines(),
            ["-p", "server", "--no-daemon", ":test"],
        )
        self.assertNotEqual(self.run_script("hoglite-live.sh", "test").returncode, 0)

    def test_role_pull_and_revision_failure_not_masked(self):
        self.assertNotEqual(
            self.run_script("hoglite-live.sh", "role", HAS_ROLE="false").returncode, 0
        )
        self.assertEqual(
            self.run_script("hoglite-live.sh", "role", HAS_ROLE="true").returncode, 0
        )
        pins = workflow("hoglite-live")["jobs"]["hoglite-live"]["env"]
        self.write("bin/docker", '#!/bin/sh\n[ "$1" != pull ] || exit 31\nexit 0\n')
        self.assertNotEqual(
            self.run_script(
                "hoglite-live.sh", "pull", REGISTRY="fixture.invalid", **pins
            ).returncode,
            0,
        )
        self.write(
            "bin/docker",
            '#!/bin/sh\n[ "$1" != image ] || echo wrong-revision\nexit 0\n',
        )
        result = self.run_script(
            "hoglite-live.sh", "pull", REGISTRY="fixture.invalid", **pins
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("revision", result.stderr)

    def test_ecr_gate_ignores_external_workflow_state_but_not_pg_failure(self):
        shutil.copy2(ROOT / ".github/scripts/ecr-release.sh", self.root / "gate.sh")
        self.write(
            "bin/gh",
            '#!/bin/sh\ncase "$2" in *"/1/jobs?"*) cat "$FIXTURE/server.json";; *"/2/jobs?"*) cat "$FIXTURE/webui.json";; *) cat "$FIXTURE/runs.json";; esac\n',
        )
        for state in ["failure", None, "in_progress"]:
            runs = [
                {"path": ".github/workflows/server.yml", "id": 1},
                {"path": ".github/workflows/webui.yml", "id": 2},
            ]
            if state is not None:
                runs.append(
                    {
                        "path": ".github/workflows/hoglite-live.yml",
                        "id": 3,
                        "conclusion": state,
                    }
                )
            (self.root / "runs.json").write_text(json.dumps({"workflow_runs": runs}))
            for pg in ["success", "failure"]:
                for name, gate, result in [
                    ("server", "server-checks", pg),
                    ("webui", "webui-test", "skipped"),
                ]:
                    jobs = [
                        {
                            "name": "changes",
                            "status": "completed",
                            "conclusion": "success",
                        },
                        {"name": gate, "status": "completed", "conclusion": result},
                    ]
                    (self.root / f"{name}.json").write_text(json.dumps({"jobs": jobs}))
                result = subprocess.run(
                    [
                        "bash",
                        str(self.root / "gate.sh"),
                        "gate",
                        "fixture/repo",
                        "a" * 40,
                    ],
                    env={**self.env, "FIXTURE": str(self.root)},
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(
                    result.stdout.strip(),
                    f"publish={'true' if pg == 'success' else 'false'}",
                )


class InventoryTest(unittest.TestCase):
    def test_current_roster_matches_source(self):
        try:
            reports = required_reports(ROOT)
        except (OSError, ValueError) as error:
            self.fail(str(error))
        self.assertGreaterEqual(len(reports), 16)

    def test_outside_package_non_hoglite_consumers_and_report_failures(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "server/src/test/kotlin"
            source.mkdir(parents=True)
            (root / "ci").mkdir()
            reports = root / "server/build/test-results/test"
            reports.mkdir(parents=True)
            (source / "HogliteConformanceEngine.kt").write_text(
                'package com.posthog.hoglake.hoglite\nclass HogliteConformanceEngine { val b = System.getenv("HOGLITE_BINARY") }'
            )
            fqcn = "example.api.BackendAvailabilityIntegrationTest"
            (source / "BackendAvailabilityIntegrationTest.kt").write_text(
                "package example.api\nclass BackendAvailabilityIntegrationTest { val engine = HogliteConformanceEngine()\n@Test fun live() {} }"
            )
            roster = root / "ci/hoglite-live-classes.txt"
            roster.write_text(fqcn + "\n")
            required = reports / f"TEST-{fqcn}.xml"
            self.assertIn(required, required_reports(root))
            for content, good in [
                ('<testsuite><testcase name="live"/></testsuite>', True),
                (None, False),
                ("<testsuite/>", False),
                ("not XML", False),
                ("<!DOCTYPE testsuite><testsuite/>", False),
                *[
                    (
                        f'<testsuite><testcase name="live"><{kind}/></testcase></testsuite>',
                        False,
                    )
                    for kind in ("failure", "error", "skipped")
                ],
            ]:
                if content is None:
                    required.unlink(missing_ok=True)
                else:
                    required.write_text(content)
                result = subprocess.run(
                    [
                        sys.executable,
                        str(ROOT / "ci/check_hoglite_results.py"),
                        "--root",
                        str(root),
                    ],
                    check=False,
                    capture_output=True,
                )
                self.assertEqual(result.returncode == 0, good, result.stderr)
            # Ordinary URL strings must not make a subsequent direct launcher
            # disappear as if it were a line comment. Indirect new helpers fail
            # for review instead of silently hiding their consumers.
            direct = source / "OutsideEngineTest.kt"
            direct.write_text(
                'package example.api\nclass OutsideEngineTest { val url = "http://fixture.invalid"; val b = System.getenv("HOGLITE_BINARY"); @Test fun live() {} }'
            )
            with self.assertRaisesRegex(ValueError, "roster drift"):
                required_reports(root)
            direct.unlink()
            helper = source / "SharedEngineFixture.kt"
            helper.write_text(
                "package example\nclass SharedEngineFixture { val e = HogliteConformanceEngine() }"
            )
            with self.assertRaisesRegex(ValueError, "unreviewed engine helper"):
                required_reports(root)
            helper.unlink()
            roster.write_text("com.posthog.hoglake.hoglite.HogliteOtherTest\n")
            with self.assertRaisesRegex(ValueError, "roster drift"):
                required_reports(root)
            (source / "UnreviewedHelper.kt").write_text(
                'package example\nclass UnreviewedHelper { val p = ProcessBuilder("something") }'
            )
            with self.assertRaisesRegex(ValueError, "unreviewed process launcher"):
                required_reports(root)


if __name__ == "__main__":
    unittest.main()
