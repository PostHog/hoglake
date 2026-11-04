"""Bounded engine-consumer inventory and fail-closed live report gate.

This recognizes today's direct launchers/shared fixture, not arbitrary Kotlin
syntax or future dependency injection. New fixture patterns require review here.
It never selects/excludes tests: both CI jobs run full root :test.
"""

import argparse
import re
import sys
from pathlib import Path

from check_live_results import check_report

ROOT = Path(__file__).resolve().parent.parent
HELPERS = {"com.posthog.hoglake.hoglite.HogliteConformanceEngine"}
NON_ENGINE_LAUNCHERS = {
    "com.posthog.hoglake.BootValidationTest",
    "com.posthog.hoglake.RemovedEnvConfigTest",
}


def inventory(source: Path) -> set[str]:
    consumers = set()
    helpers = set()
    for path in source.rglob("*.kt"):
        # Deliberately bounded structural conventions: one top-level class
        # named after the file. Ignore comments so unit-test prose is not a dependency.
        tokens = r'""".*?"""|"(?:\\.|[^"\\])*"|/\*.*?\*/|//[^\n]*'
        text = re.sub(
            tokens,
            lambda match: "" if match[0].startswith(("//", "/*")) else match[0],
            path.read_text(),
            flags=re.DOTALL,
        )
        package = re.search(r"^package ([\w.]+)", text, re.MULTILINE)
        if not package:
            raise ValueError(f"missing package: {path}")
        name = f"{package[1]}.{path.stem}"
        direct = bool(re.search(r'System\s*\.\s*getenv\s*\(\s*"HOGLITE_BINARY"', text))
        shared = bool(re.search(r"\bHogliteConformanceEngine\b", text))
        launch = bool(re.search(r"\b(?:ProcessBuilder|findOnPath)\s*\(", text))
        if launch and not direct and name not in NON_ENGINE_LAUNCHERS:
            raise ValueError(f"unreviewed process launcher: {name}")
        if name in HELPERS:
            if not direct:
                raise ValueError(f"shared fixture launcher changed: {name}")
            helpers.add(name)
        elif direct or shared:
            if not re.search(r"@(?:Test|ParameterizedTest)\b", text):
                raise ValueError(
                    f"unreviewed engine helper (declare its consumers): {name}"
                )
            if not re.search(rf"\bclass\s+{re.escape(path.stem)}\b", text):
                raise ValueError(f"class/file naming contract changed: {path}")
            consumers.add(name)
    if helpers != HELPERS:
        raise ValueError(f"missing shared fixtures: {HELPERS - helpers}")
    return consumers


def required_reports(root: Path) -> list[Path]:
    roster = root / "ci/hoglite-live-classes.txt"
    names = [
        line.strip()
        for line in roster.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    ]
    if not names or len(names) != len(set(names)):
        raise ValueError("empty/duplicate live-engine roster")
    discovered = inventory(root / "server/src/test/kotlin")
    if discovered != set(names):
        raise ValueError(
            f"live-engine roster drift: missing={discovered - set(names)}, stale={set(names) - discovered}"
        )
    directory = root / "server/build/test-results/test"
    reports = {directory / f"TEST-{name}.xml" for name in names}
    # Also check additional produced Hoglite/package reports, preserving the old
    # safety net. Missing consumers are caught by the inventory/explicit roster.
    reports.update(directory.glob("TEST-com.posthog.hoglake.hoglite.*.xml"))
    reports.update(directory.glob("TEST-*.Hoglite*.xml"))
    return sorted(reports)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args()
    try:
        reports = required_reports(args.root)
    except (OSError, ValueError) as error:
        print(error, file=sys.stderr)
        return 1
    results = [check_report(path) for path in reports]
    result = 0 if all(results) else 1
    import os

    if summary := os.getenv("GITHUB_STEP_SUMMARY"):
        with Path(summary).open("a") as out:
            out.write(
                f"\nLive engine: {len(reports)} required/discovered reports; zero-skip gate {'passed' if result == 0 else 'FAILED'}.\n"
            )
    return result


if __name__ == "__main__":
    sys.exit(main())
