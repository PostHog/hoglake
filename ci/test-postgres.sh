#!/usr/bin/env bash
# Full required suite, with engine absence (not an empty binary path).
set -euo pipefail
cd "$(dirname "$0")/.."
unset HOGLITE_BINARY
if command -v hoglited >/dev/null 2>&1; then
    echo 'Required PostgreSQL suite refuses a hoglited PATH fallback' >&2
    exit 1
fi
./server/gradlew -p server --no-daemon :test :ktlintCheck :checkOpenapiVersion
