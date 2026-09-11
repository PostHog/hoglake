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

echo "== sqllogictests =="
(cd "$HERE" && make test)

echo "== cross-client partition-wire check =="
(cd "$PYHOGLAKE_DIR" && HOGLAKE_S3_ENDPOINT="http://$DUCKEXT_S3_ENDPOINT" \
    uv run python "$HERE/test/fixtures/verify_partition_wire.py")

echo "ALL LIVE TESTS PASSED"
