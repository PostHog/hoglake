#!/usr/bin/env bash
# Live millrace tests: the Kafka ingestion path against this checkout and
# isolated services.
#
# Isolated Postgres + hoglake server built from THIS checkout + MinIO +
# apache/kafka (pinned BY DIGEST in ci/docker-compose.live-millrace.yml —
# a tag is a pointer, and a live suite green against a broker nobody
# chose proves nothing), on high ports that collide with neither the dev
# stack (5432/8080/9000/9001) nor ci/live-python.sh
# (15432/18080/19000/19001). Logs and JUnit reports land in
# .artifacts/live-millrace/; all services are torn down on exit.
#
# PHASE 5 STATE: the suite under millrace/tests/live/ (marker `live`) is
# the live docker suite of docs/kafka-ingestion-plan.md — end-to-end
# truth, kill -9 / lost-commit-response recovery, reassignment, junk
# event_time, the whale/freshness scenarios, the DDL race, drop+recreate
# and a compaction observation. The pytest run below selects exactly
# those (-m live; tests/conftest.py deselects the marker otherwise), and
# ci/check_live_results.py rejects a missing, empty, skipped or failed
# report.
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
report_dir=${HOGLAKE_LIVE_MILLRACE_REPORT_DIR:-"$repo_dir/.artifacts/live-millrace"}
mkdir -p "$report_dir"
report_dir=$(cd "$report_dir" && pwd)
project="hoglake-live-millrace-$(date +%s)-$$"
export HOGLAKE_PG_PORT=${HOGLAKE_PG_PORT:-25432}
export HOGLAKE_MINIO_PORT=${HOGLAKE_MINIO_PORT:-29000}
export HOGLAKE_MINIO_CONSOLE_PORT=${HOGLAKE_MINIO_CONSOLE_PORT:-29001}
export HOGLAKE_PORT=${HOGLAKE_PORT:-28080}
export HOGLAKE_KAFKA_PORT=${HOGLAKE_KAFKA_PORT:-29092}
export HOGLAKE_URL="http://localhost:$HOGLAKE_PORT"
export KAFKA_BOOTSTRAP_SERVERS="127.0.0.1:$HOGLAKE_KAFKA_PORT"
export HOGLAKE_S3_ENDPOINT="http://localhost:$HOGLAKE_MINIO_PORT"
export HOGLAKE_S3_ACCESS_KEY=hoglake
export HOGLAKE_S3_SECRET_KEY=hoglake123
compose=(docker compose -p "$project"
    -f "$repo_dir/server/docker-compose.yml"
    -f "$repo_dir/ci/docker-compose.live-millrace.yml")
server_pid=

# shellcheck disable=SC2329 # Called by the EXIT trap.
cleanup() {
    result=$?
    trap - EXIT
    if [[ -n "$server_pid" ]]; then
        kill "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
    "${compose[@]}" logs --no-color > "$report_dir/services.log" 2>&1 || true
    "${compose[@]}" down --volumes --remove-orphans >> "$report_dir/services.log" 2>&1 || true
    exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Delete old reports before setup, so a failed build cannot leave stale test evidence.
rm -f "$report_dir/millrace.xml" "$report_dir/millrace.log"

# Keep the report checker dependency out of the client environments.
ci_python=(uv run --no-project --with defusedxml==0.7.1 python)
"${ci_python[@]}" -m unittest discover -s "$repo_dir/ci" -p 'test_*.py'
(
    cd "$repo_dir/server"
    ./gradlew --no-daemon :installDist -x test -x ktlintCheck --console=plain
) 2>&1 | tee "$report_dir/build.log"
"${compose[@]}" up -d --wait --wait-timeout 120 postgres minio kafka

# The maintenance loops the suite drives by hand stay off; the hydrator
# runs because the deferred-stats flush path (age-triggered flushes ship
# stats_mode=deferred) needs bounds backfilled — same rationale as
# ci/live-python.sh.
env HOGLAKE_JDBC_URL="jdbc:postgresql://localhost:$HOGLAKE_PG_PORT/hoglake" \
    HOGLAKE_DB_USER=hoglake HOGLAKE_DB_PASSWORD=hoglake \
    HOGLAKE_INSTANCE_NAME="$project" \
    HOGLAKE_HYDRATOR_INTERVAL_MS=1000 HOGLAKE_EXPIRY_INTERVAL_MS=0 \
    HOGLAKE_CLEANUP_INTERVAL_MS=0 HOGLAKE_COMPACTION_INTERVAL_MS=0 \
    HOGLAKE_RETIREMENT_INTERVAL_MS=0 HOGLAKE_REINDEX_INTERVAL_MS=0 \
    HOGLAKE_METRICS_INTERVAL_MS=0 HOGLAKE_MAINTENANCE_SUMMARY_INTERVAL_MS=0 \
    "$repo_dir/server/build/install/hoglake-server/bin/hoglake-server" \
    > "$report_dir/server.log" 2>&1 &
server_pid=$!

ready=false
for ((attempt = 0; attempt < 60; attempt++)); do
    if ! kill -0 "$server_pid" 2>/dev/null; then
        cat "$report_dir/server.log"
        exit 1
    fi
    if curl --fail --silent --max-time 2 "$HOGLAKE_URL/v1/info" \
        | python3 "$repo_dir/ci/check_live_identity.py" "$project" \
        && curl --fail --silent --max-time 2 "$HOGLAKE_S3_ENDPOINT/minio/health/ready" > /dev/null; then
        ready=true
        break
    fi
    sleep 1
done
if [[ "$ready" != true ]]; then
    echo 'The live millrace test stack did not become ready.' >&2
    exit 1
fi

result=0
if ! (
    cd "$repo_dir/millrace"
    uv run --locked pytest -m live -q --junitxml="$report_dir/millrace.xml"
) 2>&1 | tee "$report_dir/millrace.log"; then
    result=1
fi
# Missing, empty, skipped or failed reports are all a hard failure.
if ! "${ci_python[@]}" "$repo_dir/ci/check_live_results.py" \
    "$report_dir/millrace.xml"; then
    result=1
fi
if ! kill -0 "$server_pid" 2>/dev/null; then
    echo 'The test server stopped before the live checks completed.' >&2
    result=1
fi
exit "$result"
