"""Reject missing, empty, skipped, or failed required live-test reports."""

import argparse
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def check_report(path: Path) -> bool:
    try:
        cases = list(ET.parse(path).getroot().iter("testcase"))
    except (OSError, ET.ParseError) as error:
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
