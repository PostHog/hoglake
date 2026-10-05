#!/usr/bin/env bash
# Run required client integration tests against this checkout and isolated services.
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
report_dir=${HOGLAKE_LIVE_REPORT_DIR:-"$repo_dir/.artifacts/live-python"}
mkdir -p "$report_dir"
report_dir=$(cd "$report_dir" && pwd)
project="hoglake-live-$(date +%s)-$$"
export HOGLAKE_PG_PORT=${HOGLAKE_PG_PORT:-15432}
export HOGLAKE_MINIO_PORT=${HOGLAKE_MINIO_PORT:-19000}
export HOGLAKE_MINIO_CONSOLE_PORT=${HOGLAKE_MINIO_CONSOLE_PORT:-19001}
export HOGLAKE_PORT=${HOGLAKE_PORT:-18080}
export HOGLAKE_URL="http://localhost:$HOGLAKE_PORT"
export HOGLAKE_S3_ENDPOINT="http://localhost:$HOGLAKE_MINIO_PORT"
export HOGLAKE_S3_ACCESS_KEY=hoglake
export HOGLAKE_S3_SECRET_KEY=hoglake123
compose=(docker compose -p "$project" -f "$repo_dir/server/docker-compose.yml")
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
for component in pyhoglake hedgerow; do
    rm -f "$report_dir/$component.xml" "$report_dir/$component.log"
done

python3 -m unittest discover -s "$repo_dir/ci" -p 'test_*.py'
(
    cd "$repo_dir/server"
    ./gradlew --no-daemon :installDist -x test -x ktlintCheck --console=plain
) 2>&1 | tee "$report_dir/build.log"
"${compose[@]}" up -d --wait --wait-timeout 90 postgres minio

# The deferred-stats integration test requires the hydrator loop.
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
    echo 'The live test stack did not become ready.' >&2
    exit 1
fi

result=0
for component in pyhoglake hedgerow; do
    # Run both components even if the first fails. Preserve both exit codes
    # and reports; a later success must not hide an earlier failure.
    if ! (
        cd "$repo_dir/$component"
        uv run --locked pytest -m integration -q --junitxml="$report_dir/$component.xml"
    ) 2>&1 | tee "$report_dir/$component.log"; then
        result=1
    fi
done
if ! python3 "$repo_dir/ci/check_live_results.py" \
    "$report_dir/pyhoglake.xml" "$report_dir/hedgerow.xml"; then
    result=1
fi
if ! kill -0 "$server_pid" 2>/dev/null; then
    echo 'The test server stopped before the live checks completed.' >&2
    result=1
fi
exit "$result"
