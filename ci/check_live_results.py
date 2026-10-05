"""Reject missing, empty, skipped, or failed required live-test reports."""

import argparse
import sys
from pathlib import Path

from defusedxml import ElementTree as ET
from defusedxml.common import DefusedXmlException


def check_report(path: Path) -> bool:
    try:
        cases = list(ET.parse(path, forbid_dtd=True).getroot().iter("testcase"))
    except (OSError, ET.ParseError, DefusedXmlException) as error:
        print(f"{path}: cannot read live-test results: {error}", file=sys.stderr)
        return False
    if not cases:
        print(f"{path}: no live tests ran", file=sys.stderr)
        return False
    counts = {
        kind: sum(case.find(kind) is not None for case in cases)
        for kind in ("failure", "error", "skipped")
    }
    print(f"{path}: {len(cases)} tests; {counts}")
    return not any(counts.values())


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reports", nargs="+", type=Path)
    args = parser.parse_args()
    results = [check_report(path) for path in args.reports]
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
