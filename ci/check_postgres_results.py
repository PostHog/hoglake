"""Fail closed on the known independent PostgreSQL test-arm inventory.

This is a fixed inventory, not Kotlin discovery or a general report framework.
Mixed-directory cases still need their directory engine and are not legacy-PG
arms. Update these explicit XML-name multiplicities deliberately when adding cases.
Legacy conformance/commit enum labels repeat POSTGRES; this check pins their
collected multiplicities, not arbitrary Kotlin method discovery.
"""

import argparse
import sys
from collections import Counter
from pathlib import Path

from defusedxml import ElementTree as ET
from defusedxml.common import DefusedXmlException

ROOT = Path(__file__).resolve().parent.parent
PACKAGE = "com.posthog.hoglake.hoglite."
# PG records first, explicitly known non-PG/mixed records second.
# 39 enum PG records + six ordered-delete permutations in conformance;
# creation/sampler has five independent PG cases and one directory-engine mixed case.
PG_INVENTORY = {
    "HogliteBackendConformanceIntegrationTest": (
        Counter(
            {
                "POSTGRES": 39,
                'POSTGRES "output" durable-first=false': 1,
                'POSTGRES "output" durable-first=true': 1,
                'POSTGRES "overcount" durable-first=false': 1,
                'POSTGRES "overcount" durable-first=true': 1,
                'POSTGRES "retired" durable-first=false': 1,
                'POSTGRES "retired" durable-first=true': 1,
            }
        ),
        Counter(
            {
                "HOGLITE": 39,
                'HOGLITE "output" durable-first=false': 1,
                'HOGLITE "output" durable-first=true': 1,
                'HOGLITE "overcount" durable-first=false': 1,
                'HOGLITE "overcount" durable-first=true': 1,
                'HOGLITE "retired" durable-first=false': 1,
                'HOGLITE "retired" durable-first=true': 1,
            }
        ),
    ),
    "HogliteCommitParityIntegrationTest": (
        Counter({"POSTGRES": 17}),
        Counter({"HOGLITE": 17}),
    ),
    "HogliteScanStatsParityIntegrationTest": (
        Counter(
            {
                "PG leaf contract counts VARIANT but not populated containers(Backend) [1] POSTGRES": 1,
                "no provided files skip metadata and captured pending never acquires hydrated stats(Backend) [1] POSTGRES": 1,
                "route cap charges provided files and conservatively counts sparse visible leaves(Backend) [1] POSTGRES": 1,
                "route rejects exactly one million and one possible sparse entries(Backend) [1] POSTGRES": 1,
                "statistics resolve against selected read metadata not later DDL(Backend) [1] POSTGRES": 1,
            }
        ),
        Counter(
            {
                "PG leaf contract counts VARIANT but not populated containers(Backend) [2] HOGLITE": 1,
                "hoglite captured scan refuses an overtaking floor without rebinding()": 1,
                "no provided files skip metadata and captured pending never acquires hydrated stats(Backend) [2] HOGLITE": 1,
                "route cap charges provided files and conservatively counts sparse visible leaves(Backend) [2] HOGLITE": 1,
                "route rejects exactly one million and one possible sparse entries(Backend) [2] HOGLITE": 1,
                "statistics resolve against selected read metadata not later DDL(Backend) [2] HOGLITE": 1,
            }
        ),
    ),
    "HogliteAlterRaceIntegrationTest": (
        Counter(
            {
                "conflicting add revalidates and consumes no rejected snapshot(Backend) / POSTGRES": 1,
                "fixed read snapshot refuses DDL without another publication(Backend) / POSTGRES": 1,
                "independent adds serialize with fresh field ids and own receipts(Backend, boolean) / POSTGRES UUID=false": 1,
                "independent adds serialize with fresh field ids and own receipts(Backend, boolean) / POSTGRES UUID=true": 1,
            }
        ),
        Counter(
            {
                "conflicting add revalidates and consumes no rejected snapshot(Backend) / HOGLITE": 1,
                "fixed read snapshot refuses DDL without another publication(Backend) / HOGLITE": 1,
                "independent adds serialize with fresh field ids and own receipts(Backend, boolean) / HOGLITE UUID=false": 1,
                "independent adds serialize with fresh field ids and own receipts(Backend, boolean) / HOGLITE UUID=true": 1,
            }
        ),
    ),
    "HogliteReadParityIntegrationTest": (
        Counter(
            {
                "PostgreSQL consumer names nulls and UUID ties determine grouped wire order but not offset order()": 1,
                "PostgreSQL empty unsorted row ids omit bounds and sorted stored bounds survive()": 1,
            }
        ),
        Counter(
            {
                "hoglite consumer names nulls and UUID ties determine grouped wire order but not offset order()": 1,
                "hoglite empty unsorted row ids omit bounds and sorted stored bounds survive()": 1,
            }
        ),
    ),
    "PostgresCreationAndSamplerParityIntegrationTest": (
        Counter(
            {
                "bound creation preserves id sequence typed errors and fencing after rename()": 1,
                "filtered sampler binds arrays for one several and empty ids and isolates both seed and due work()": 1,
                "null sampler filter executes the historical seed and due statements without filter bindings()": 1,
                "ordinary creation uses identity default under a real role without sequence rights before and after rename()": 1,
                "sampler filters independently isolate seed due empty and tick work()": 1,
            }
        ),
        Counter(
            {
                "real mixed reservation uses exactly its allocated identity after sequence rename()": 1
            }
        ),
    ),
    "HoglitePartitionParityIntegrationTest": (
        Counter(
            {
                "API writes use actual PG sampler while hoglite stays exact and legacy escapes coalesce(Backend) [1] POSTGRES": 1,
                "PostgreSQL fixture records actual database and text array collation without a new setting()": 1,
                "compact listing group boundary accepts50000 refuses50001 before filters(Backend) [1] POSTGRES": 1,
                "listing decoded raw null prefix filters and every measure direction stay unchanged(Backend) [1] POSTGRES": 1,
                "partition route caps distinguish stats500 from listing1000(Backend) [1] POSTGRES": 1,
                "stats rank precedence keeps debt smallbytes namespace and table ahead of tuple(Backend) [1] POSTGRES": 1,
                "stats typed tuples retain absent empty null elements prefixes and numeric specs(Backend) [1] POSTGRES": 1,
            }
        ),
        Counter(
            {
                "API writes use actual PG sampler while hoglite stays exact and legacy escapes coalesce(Backend) [2] HOGLITE": 1,
                "compact listing group boundary accepts50000 refuses50001 before filters(Backend) [2] HOGLITE": 1,
                "listing decoded raw null prefix filters and every measure direction stay unchanged(Backend) [2] HOGLITE": 1,
                "partition route caps distinguish stats500 from listing1000(Backend) [2] HOGLITE": 1,
                "stats rank precedence keeps debt smallbytes namespace and table ahead of tuple(Backend) [2] HOGLITE": 1,
                "stats typed tuples retain absent empty null elements prefixes and numeric specs(Backend) [2] HOGLITE": 1,
            }
        ),
    ),
    "HogliteCatalogCreateParityIntegrationTest": (
        Counter(
            {
                "configured create authenticates before decoding and keeps operator attribution(Mode) [1] POSTGRES": 1,
                "configured create malformed unknown strict attribution and content type do no backend IO(Mode) [1] POSTGRES": 1,
                "configured create selection presence null scalar and conflict matrix(Mode) [1] POSTGRES": 1,
            }
        ),
        Counter(
            {
                "configured create authenticates before decoding and keeps operator attribution(Mode) [2] HOGLITE": 1,
                "configured create authenticates before decoding and keeps operator attribution(Mode) [3] MIXED": 1,
                "configured create malformed unknown strict attribution and content type do no backend IO(Mode) [2] HOGLITE": 1,
                "configured create malformed unknown strict attribution and content type do no backend IO(Mode) [3] MIXED": 1,
                "configured create selection presence null scalar and conflict matrix(Mode) [2] HOGLITE": 1,
                "configured create selection presence null scalar and conflict matrix(Mode) [3] MIXED": 1,
            }
        ),
    ),
}


def check_postgres_report(
    path: Path, name: str, expected: Counter, other: Counter
) -> bool:
    try:
        cases = list(ET.parse(path, forbid_dtd=True).getroot().iter("testcase"))
    except (OSError, ET.ParseError, DefusedXmlException) as error:
        print(f"{path}: cannot read PostgreSQL results: {error}", file=sys.stderr)
        return False
    if any(case.get("classname") != PACKAGE + name for case in cases):
        print(f"{path}: unexpected testcase class", file=sys.stderr)
        return False
    selected = [case for case in cases if case.get("name") in expected]
    all_names = Counter(case.get("name") for case in cases)
    allowed_names = expected + other
    names_match = all_names == allowed_names
    if not names_match:
        print(
            f"{path}: missing {allowed_names - all_names}; extra/duplicate {all_names - allowed_names}",
            file=sys.stderr,
        )
    failures = {
        kind: sum(case.find(kind) is not None for case in selected)
        for kind in ("failure", "error", "skipped")
    }
    actual = Counter(case.get("name") for case in selected)
    print(
        f"{path}: PostgreSQL {len(selected)}/{sum(expected.values())} cases; {failures}"
    )
    return names_match and actual == expected and not any(failures.values())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()
    directory = args.root / "server/build/test-results/test"
    results = [
        check_postgres_report(
            directory / f"TEST-{PACKAGE}{name}.xml", name, expected, other
        )
        for name, (expected, other) in PG_INVENTORY.items()
    ]
    # A second report for a listed class must not hide outside the canonical path.
    canonical = {f"TEST-{PACKAGE}{name}.xml" for name in PG_INVENTORY}
    known = {PACKAGE + name for name in PG_INVENTORY}
    for path in directory.glob("TEST-*.xml"):
        if path.name in canonical:
            continue
        relevant = any(name in path.name for name in known)
        try:
            document = ET.parse(path, forbid_dtd=True).getroot()
            relevant |= document.get("name") in known or any(
                case.get("classname") in known for case in document.iter("testcase")
            )
        except (OSError, ET.ParseError, DefusedXmlException):
            pass  # Unlisted reports are outside this fixed inventory.
        if relevant:
            print(f"{path}: duplicate/noncanonical PostgreSQL report", file=sys.stderr)
            results.append(False)
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
