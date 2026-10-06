#!/usr/bin/env bash
# The pinned ClickHouse that pyhoglake's packed adapter is tested against,
# exposed as a `clickhouse` executable (PYHOGLAKE_CLICKHOUSE). Packed parts
# are version-coupled bytes, so writer and reader are pinned by digest.
# The adapter only uses temporary directories, which the container sees at
# the same path through the TMPDIR bind mount.
set -euo pipefail

image=clickhouse/clickhouse-server:26.9.8.3@sha256:230b973a00b5bac5b8925bd2898fae95a039aa6f25063fa8c507ffe9e2c7d770
tmp=${TMPDIR:-/tmp}
exec docker run --rm -i --network none \
    --user "$(id -u):$(id -g)" \
    --volume "$tmp:$tmp" \
    --env TMPDIR="$tmp" \
    --entrypoint clickhouse \
    "$image" "$@"
