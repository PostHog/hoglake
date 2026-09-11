#!/usr/bin/env bash
# Fetch the pinned duckdb + extension-ci-tools trees into duckdb-client/.
#
# duckdb-client/ lives inside the hoglake monorepo, so these cannot be
# git submodules yet (that would touch the repo root); they are plain
# gitignored clones at pinned SHAs. On extraction to a standalone
# community-extension repo they become ordinary submodules.
#
# Clones prefer a local ducklake-fork checkout (fast, offline) and fall
# back to GitHub.
set -euo pipefail

DUCKDB_SHA=ac5c6d11c3b0915fcc0fa43080d31893269d6448
CI_TOOLS_SHA=795096d04b009c0d087468439ebb526a5460dfac

HERE="$(cd "$(dirname "$0")/.." && pwd)"
LOCAL_FORK="${DUCKLAKE_FORK_DIR:-$HOME/src/ducklake-fork}"

fetch() { # name, sha, local_path, url
    local name=$1 sha=$2 local_path=$3 url=$4
    local dst="$HERE/$name"
    if [ ! -d "$dst/.git" ] && [ ! -f "$dst/.git" ]; then
        if [ -e "$local_path" ]; then
            git clone --no-checkout "$local_path" "$dst"
            git -C "$dst" remote set-url origin "$url"
        else
            git clone --no-checkout "$url" "$dst"
        fi
    fi
    if ! git -C "$dst" cat-file -e "$sha^{commit}" 2>/dev/null; then
        git -C "$dst" fetch origin "$sha"
    fi
    git -C "$dst" checkout --detach -q "$sha"
    echo "$name @ $(git -C "$dst" rev-parse HEAD)"
}

fetch duckdb "$DUCKDB_SHA" "$LOCAL_FORK/duckdb" \
    https://github.com/duckdb/duckdb.git
fetch extension-ci-tools "$CI_TOOLS_SHA" "$LOCAL_FORK/extension-ci-tools" \
    https://github.com/duckdb/extension-ci-tools.git
