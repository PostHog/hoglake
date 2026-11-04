"""Bounded inventory negatives: a successful command cannot hide skipped PG."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from xml.sax.saxutils import quoteattr

from check_postgres_results import PACKAGE, PG_INVENTORY

CHECKER = Path(__file__).with_name("check_postgres_results.py")


class PostgresResultsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.reports = self.root / "server/build/test-results/test"
        self.reports.mkdir(parents=True)
        for name, (expected, other) in PG_INVENTORY.items():
            names = [
                label
                for label, count in (expected + other).items()
                for _ in range(count)
            ]
            cases = "".join(
                f"<testcase classname={quoteattr(PACKAGE + name)} name={quoteattr(n)}/>"
                for n in names
            )
            self.report(name).write_text(f"<testsuite>{cases}</testsuite>")

    def report(self, name):
        return self.reports / f"TEST-{PACKAGE}{name}.xml"

    def run_checker(self):
        return subprocess.run(
            [sys.executable, str(CHECKER), "--root", str(self.root)],
            check=False,
            capture_output=True,
            text=True,
        )

    def test_partition_and_configured_create_inventory_is_explicit_and_preserves_legacy_counts(
        self,
    ):
        added = {
            "HoglitePartitionParityIntegrationTest",
            "HogliteCatalogCreateParityIntegrationTest",
        }
        runtime = {
            "HogliteChangesProjectionIntegrationTest",
            "MixedPostgresInitializationIntegrationTest",
        }
        self.assertEqual(
            sum(
                sum(pg.values())
                for name, (pg, _) in PG_INVENTORY.items()
                if name not in added | runtime
            ),
            78,
        )
        self.assertEqual(
            sum(
                sum(other.values())
                for name, (_, other) in PG_INVENTORY.items()
                if name not in added | runtime
            ),
            75,
        )
        self.assertEqual(
            sum(PG_INVENTORY["HoglitePartitionParityIntegrationTest"][0].values()), 7
        )
        self.assertEqual(
            sum(PG_INVENTORY["HogliteCatalogCreateParityIntegrationTest"][0].values()),
            3,
        )
        self.assertEqual(
            sum(PG_INVENTORY["HoglitePartitionParityIntegrationTest"][1].values()), 6
        )
        self.assertEqual(
            sum(PG_INVENTORY["HogliteCatalogCreateParityIntegrationTest"][1].values()),
            6,
        )
        for name in added:
            pg, other = PG_INVENTORY[name]
            self.assertTrue(all(count == 1 for count in (pg + other).values()))
            self.assertTrue(
                all(
                    "POSTGRES" in label or label.startswith("PostgreSQL fixture")
                    for label in pg
                )
            )
            self.assertTrue(
                all("HOGLITE" in label or "MIXED" in label for label in other)
            )

    def test_new_parity_cases_cannot_hide_missing_skipped_duplicated_or_misclassified(
        self,
    ):
        for name in (
            "HoglitePartitionParityIntegrationTest",
            "HogliteCatalogCreateParityIntegrationTest",
            "HogliteChangesProjectionIntegrationTest",
            "MixedPostgresInitializationIntegrationTest",
        ):
            path = self.report(name)
            original = path.read_text()
            for label in PG_INVENTORY[name][0]:
                case = f"<testcase classname={quoteattr(PACKAGE + name)} name={quoteattr(label)}/>"
                for changed in (
                    "",
                    case + case,
                    case.replace("/>", "><skipped/></testcase>"),
                    case.replace("/>", "><error/></testcase>"),
                    case.replace("/>", "><failure/></testcase>"),
                    case.replace(
                        quoteattr(PACKAGE + name),
                        quoteattr(PACKAGE + "MixedCatalogIntegrationTest"),
                    ),
                ):
                    with self.subTest(name=name, label=label, changed=changed):
                        path.write_text(original.replace(case, changed, 1))
                        self.assertNotEqual(self.run_checker().returncode, 0)
                        path.write_text(original)

    def test_runtime_inventory_preserves_every_previous_record_and_legacy_subsets(self):
        runtime = {
            "HogliteChangesProjectionIntegrationTest",
            "MixedPostgresInitializationIntegrationTest",
        }
        self.assertEqual(
            sum(
                sum(pg.values())
                for name, (pg, _) in PG_INVENTORY.items()
                if name not in runtime
            ),
            88,
        )
        self.assertEqual(
            sum(
                sum(other.values())
                for name, (_, other) in PG_INVENTORY.items()
                if name not in runtime
            ),
            87,
        )
        self.assertEqual(sum(sum(pg.values()) for pg, _ in PG_INVENTORY.values()), 96)
        self.assertEqual(
            sum(sum(other.values()) for _, other in PG_INVENTORY.values()), 91
        )
        self.assertEqual(
            sum(PG_INVENTORY["MixedPostgresInitializationIntegrationTest"][0].values()),
            6,
        )
        self.assertFalse(PG_INVENTORY["MixedPostgresInitializationIntegrationTest"][1])
        self.assertEqual(
            sum(PG_INVENTORY["HogliteChangesProjectionIntegrationTest"][0].values()), 2
        )
        self.assertEqual(
            sum(PG_INVENTORY["HogliteChangesProjectionIntegrationTest"][1].values()), 4
        )
        self.assertNotIn("MixedPostgresColdStartIntegrationTest", PG_INVENTORY)

    def test_complete_inventory_accepts_unrelated_engine_skips(self):
        # Unselected HG arms and real mixed-directory engine cases are distinct.
        for name, (_, other) in PG_INVENTORY.items():
            path = self.report(name)
            text = path.read_text()
            for label in other:
                text = text.replace(
                    f"name={quoteattr(label)}/>",
                    f"name={quoteattr(label)}><skipped/></testcase>",
                )
            path.write_text(text)
        self.report("MixedCatalogIntegrationTest").write_text(
            '<testsuite><testcase name="PostgreSQL via mixed directory"><skipped/></testcase></testsuite>'
        )
        result = self.run_checker()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("PostgreSQL 45/45 cases", result.stdout)

    def test_every_required_report_fails_closed(self):
        for name in PG_INVENTORY:
            path = self.report(name)
            original = path.read_text()
            for content in (
                None,
                "not XML",
                "<!DOCTYPE testsuite><testsuite/>",
                "<testsuite/>",
                original.replace("<testcase", "<testcase-wrong", 1),
                original.replace("classname=", "wrongclass=", 1),
                original.replace(
                    f"name={quoteattr(next(iter(PG_INVENTORY[name][0])))}",
                    'name="unexpected relevant label"',
                    1,
                ),
                original.replace("</testsuite>", original[11:-12] + "</testsuite>"),
                *[
                    original.replace("/>", f"><{kind}/></testcase>", 1)
                    for kind in ("skipped", "error", "failure")
                ],
            ):
                with self.subTest(name=name, content=content):
                    if content is None:
                        path.unlink()
                    else:
                        path.write_text(content)
                    result = self.run_checker()
                    self.assertNotEqual(result.returncode, 0, result.stdout)
                    self.assertNotIn("Traceback", result.stderr)
                    path.write_text(original)

    def test_duplicate_report_or_duplicate_replacing_named_case_is_rejected(self):
        duplicate = self.reports / "TEST-duplicate.xml"
        for name in PG_INVENTORY:
            with self.subTest(name=name):
                duplicate.write_text(self.report(name).read_text())
                self.assertNotEqual(self.run_checker().returncode, 0)
                duplicate.unlink()
                duplicate.write_text(f'<testsuite name="{PACKAGE}{name}"/>')
                self.assertNotEqual(self.run_checker().returncode, 0)
                duplicate.unlink()
                malformed = self.reports / f"TEST-{PACKAGE}{name}-copy.xml"
                malformed.write_text("not XML")
                self.assertNotEqual(self.run_checker().returncode, 0)
                malformed.unlink()
        name = "HogliteReadParityIntegrationTest"
        path = self.report(name)
        labels = list(PG_INVENTORY[name][0])
        path.write_text(path.read_text().replace(labels[1], labels[0]))
        result = self.run_checker()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("extra/duplicate", result.stderr)

    def test_pg_abort_is_rejected_even_when_engine_arms_are_skipped(self):
        name = "HogliteBackendConformanceIntegrationTest"
        path = self.report(name)
        path.write_text(
            path.read_text().replace(
                "/>",
                '><skipped type="org.opentest4j.TestAbortedException"/></testcase>',
                1,
            )
        )
        result = self.run_checker()
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertIn("'skipped': 1", result.stdout)


if __name__ == "__main__":
    unittest.main()
