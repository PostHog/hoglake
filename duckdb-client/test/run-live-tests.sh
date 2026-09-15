#!/usr/bin/env bash
# The full live-stack verification: fixtures -> sqllogictests ->
# cross-client partition-wire check. Requires the dev stack (hoglake
# server + MinIO) and a pyhoglake checkout.
#
#   HOGLAKE_URL          default http://localhost:8080
#   DUCKEXT_S3_ENDPOINT  default localhost:9000 (host:port, no scheme)
#   PYHOGLAKE_DIR        default ~/src/hoglake/pyhoglake
set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
export HOGLAKE_URL="${HOGLAKE_URL:-http://localhost:8080}"
export DUCKEXT_S3_ENDPOINT="${DUCKEXT_S3_ENDPOINT:-localhost:9000}"
PYHOGLAKE_DIR="${PYHOGLAKE_DIR:-$HOME/src/hoglake/pyhoglake}"

echo "== fixtures (pyhoglake) =="
(cd "$PYHOGLAKE_DIR" && HOGLAKE_S3_ENDPOINT="http://$DUCKEXT_S3_ENDPOINT" \
    uv run python "$HERE/test/fixtures/read_fixture.py")

# time-travel snapshot ids / timestamps written by the fixture
source "$HERE/test/fixtures/live-env.sh"

# Every gate the suite's files declare. `require-env` is a WHOLE-FILE
# skip disposition in sqllogictest, so a missing gate does not weaken a
# file — it DELETES it from the run, silently, while the runner still
# exits 0. Green must mean green: an unset gate is a failure here, and
# each gated file keeps its gate alone (one conditional gate per file)
# so this list is also the file-level inventory.
REQUIRED_GATES=(
    DUCKEXT_POINTS_SNAP_V1 DUCKEXT_POINTS_SNAP_V2
    DUCKEXT_POINTS_T1 DUCKEXT_POINTS_T1_PLUS2
    DUCKEXT_EVO_SNAP_V1
    DUCKEXT_POINTS_COMPACTED   # hoglake_compacted_read.test
    DUCKEXT_SORTED_COMPACTED   # hoglake_sorted_compacted.test
    DUCKEXT_FID_MISSING        # hoglake_fieldid_missing.test
)
missing=()
for gate in "${REQUIRED_GATES[@]}"; do
    [[ -n "${!gate:-}" ]] || missing+=("$gate")
done
if (( ${#missing[@]} )); then
    echo "FAIL: fixture gate(s) not exported: ${missing[*]}" >&2
    echo "      The fixture warns and skips a gate when the dev stack could not" >&2
    echo "      produce it (e.g. compaction did not run). Re-run the fixture." >&2
    exit 1
fi

echo "== sqllogictests =="
# expected = one test case per .test file; anything less means a file
# was skipped, which unittest reports WITHOUT a non-zero exit
expected_files=$(find "$HERE/test/sql" -name '*.test' | wc -l | tr -d ' ')
test_log=$(mktemp)
trap 'rm -f "$test_log"' EXIT
(cd "$HERE" && make test) 2>&1 | tee "$test_log"
test_status=${PIPESTATUS[0]}
if (( test_status != 0 )); then
    echo "FAIL: sqllogictests exited $test_status" >&2
    exit "$test_status"
fi
if grep -q "Skipped tests for the following reasons" "$test_log"; then
    echo "FAIL: test files were SKIPPED (a skip is not a pass):" >&2
    sed -n '/Skipped tests for the following reasons/,$p' "$test_log" >&2
    exit 1
fi
ran_cases=$(grep -o '[0-9]* test cases' "$test_log" | tail -1 | grep -o '[0-9]*' || true)
ran_assertions=$(grep -o '[0-9]* assertions' "$test_log" | tail -1 | grep -o '[0-9]*' || true)
if [[ "$ran_cases" != "$expected_files" ]]; then
    echo "FAIL: ran ${ran_cases:-0} test cases, expected $expected_files (one per .test file)" >&2
    exit 1
fi
# floor, not an equality: adding assertions must not need a script edit,
# but losing a chunk of them silently must not pass either
MIN_ASSERTIONS=640
if [[ -z "$ran_assertions" || "$ran_assertions" -lt "$MIN_ASSERTIONS" ]]; then
    echo "FAIL: ${ran_assertions:-0} assertions ran, below the $MIN_ASSERTIONS floor" >&2
    exit 1
fi
echo "   verified: $ran_cases/$expected_files test files ran, $ran_assertions assertions, no skips"

echo "== cross-client partition-wire check =="
(cd "$PYHOGLAKE_DIR" && HOGLAKE_S3_ENDPOINT="http://$DUCKEXT_S3_ENDPOINT" \
    uv run python "$HERE/test/fixtures/verify_partition_wire.py")

echo "ALL LIVE TESTS PASSED ($ran_cases test files, $ran_assertions assertions, 0 skipped)"
