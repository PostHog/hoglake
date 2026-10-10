"""The mutmut survivors gate (M8): a clean `mutmut run`'s outcome must
reconcile with the survivor ledger in tests/MUTATION_TESTING.md.

Run AFTER `uv run mutmut run`, from anywhere:

    uv run python tests/check_mutants.py

The gate fails (exit 1) when:

- `mutmut results` reports any mutant in a state other than "survived"
  (a timeout, a suspicious result, a "no tests" mutant or an unchecked
  one is a broken run, not a triaged survivor — mutmut prints killed
  mutants only with --all, so every listed line is non-killed), or
- a SURVIVOR is not named in the ledger's "Surviving mutants" section
  (a genuinely new survivor: kill it with a test, or argue it into the
  ledger's equivalence classes — in the same change).

The other direction (a ledger entry that no longer survives) is NOT
gated here: the ledger is reconciled exactly whenever the mutated
modules change (see the ledger's header), and this gate is the
deterministic CI half of that discipline.

The ledger side is parsed out of MUTATION_TESTING.md: backtick-quoted
mutant names (``x…__mutmut_N``, the `mutmut results` name minus its
``millrace.<module>.`` prefix) between the "## Surviving mutants"
heading and the next "## " heading.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
LEDGER = Path(__file__).resolve().with_name("MUTATION_TESTING.md")

_RESULT_LINE = re.compile(r"^\s+(\S+): (\S(?:.*\S)?)\s*$", re.MULTILINE)
_MUTANT_NAME = re.compile(r"`([^`]*__mutmut_\d+)`")


def _ledger_survivors() -> set[str]:
    text = LEDGER.read_text()
    start = text.find("## Surviving mutants")
    if start == -1:
        sys.exit(f"error: {LEDGER} has no '## Surviving mutants' section")
    end = text.find("\n## ", start + 1)
    section = text[start : end if end != -1 else len(text)]
    names = set(_MUTANT_NAME.findall(section))
    if not names:
        sys.exit(f"error: no survivor names parsed from {LEDGER}")
    return names


def main() -> int:
    proc = subprocess.run(
        [sys.executable, "-m", "mutmut", "results"],
        cwd=PACKAGE_ROOT,
        capture_output=True,
        text=True,
        check=False,  # the exit code is inspected, not raised
    )
    if proc.returncode != 0:
        print(proc.stdout)
        print(proc.stderr, file=sys.stderr)
        print(f"error: `mutmut results` exited {proc.returncode}", file=sys.stderr)
        return 1

    ledger = _ledger_survivors()
    not_killed: list[tuple[str, str]] = _RESULT_LINE.findall(proc.stdout)
    survived = [name for name, status in not_killed if status == "survived"]
    broken = [(name, status) for name, status in not_killed if status != "survived"]
    unledgered = [name for name in survived if name.split(".", 2)[-1] not in ledger]

    print(
        f"mutants not killed: {len(not_killed)} "
        f"(survived {len(survived)}, ledgered {len(ledger)})"
    )
    if broken:
        print("\nNON-SURVIVOR, NON-KILLED mutants (a broken run, not a triage):")
        for name, status in broken:
            print(f"  {name}: {status}")
    if unledgered:
        print("\nSURVIVORS NOT IN THE LEDGER (kill them or ledger them):")
        for name in unledgered:
            print(f"  {name}")
    if broken or unledgered:
        return 1
    print("survivors gate: OK — every survivor is in the ledger")
    return 0


if __name__ == "__main__":
    sys.exit(main())
