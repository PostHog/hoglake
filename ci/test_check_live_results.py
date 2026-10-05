"""Exercise the command that decides whether required live tests passed."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

CHECKER = Path(__file__).with_name("check_live_results.py")


class LiveResultsTest(unittest.TestCase):
    def run_checker(self, *reports):
        with tempfile.TemporaryDirectory() as directory:
            paths = []
            for index, content in enumerate(reports):
                path = Path(directory) / f"report-{index}.xml"
                if content is not None:
                    path.write_text(content)
                paths.append(str(path))
            return subprocess.run(
                [sys.executable, str(CHECKER), *paths],
                check=False,
                capture_output=True,
                text=True,
            )

    def test_passed_suite_is_accepted(self):
        result = self.run_checker(
            '<testsuites><testsuite><testcase name="live"/></testsuite></testsuites>'
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_incomplete_or_failed_suites_are_rejected(self):
        for report in (
            None,
            "invalid XML",
            "<testsuites/>",
            '<testsuite><testcase name="live"><skipped/></testcase></testsuite>',
            '<testsuite><testcase name="live"><failure/></testcase></testsuite>',
            '<testsuite><testcase name="live"><error/></testcase></testsuite>',
        ):
            with self.subTest(report=report):
                result = self.run_checker(report)
                self.assertNotEqual(result.returncode, 0, result.stdout)

    def test_a_passing_component_cannot_hide_a_skipped_component(self):
        result = self.run_checker(
            '<testsuite><testcase name="passed"/></testsuite>',
            '<testsuite><testcase name="skipped"><skipped/></testcase></testsuite>',
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)

    def test_a_passing_test_cannot_hide_a_skipped_test(self):
        result = self.run_checker(
            '<testsuite><testcase name="passed"/>'
            '<testcase name="skipped"><skipped/></testcase></testsuite>'
        )
        self.assertNotEqual(result.returncode, 0, result.stdout)


if __name__ == "__main__":
    unittest.main()
