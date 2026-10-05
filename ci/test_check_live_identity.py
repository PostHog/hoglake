"""Reject another server on the test port and incomplete startup responses."""

import subprocess
import sys
import unittest
from pathlib import Path

CHECKER = Path(__file__).with_name("check_live_identity.py")


class LiveIdentityTest(unittest.TestCase):
    def test_only_the_expected_server_is_ready(self):
        for response, expected in (
            ('{"name": "this-run"}', 0),
            ('{"name": "other-run"}', 1),
            ("{}", 1),
            ("[]", 1),
            ("null", 1),
            ("", 1),
            ("not JSON", 1),
        ):
            with self.subTest(response=response):
                result = subprocess.run(
                    [sys.executable, str(CHECKER), "this-run"],
                    input=response,
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode, expected, result.stderr)
                self.assertEqual(result.stderr, "")


if __name__ == "__main__":
    unittest.main()
