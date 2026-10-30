#!/usr/bin/env bash
# Build hoglited from a PostHog/hoglite checkout and refuse any bytes
# that differ from the pinned digest.
#
# Usage: ci/build-hoglited.sh <hoglite-checkout> <commit> <sha256> <out>
#
# CI and a reviewer run this same script. The build is bit-for-bit
# reproducible: pure Go (CGO off), -trimpath, no VCS stamp, one fixed
# Go toolchain (GOTOOLCHAIN=local, so go.mod cannot swap it). The same
# commit and Go version give the same sha256 on any linux/amd64 host.
# The script exits non-zero when the checkout is not at <commit>, the
# Go version is not HOGLITE_GO_VERSION, the build fails, or the digest
# differs from <sha256>.
set -euo pipefail

src=${1:?usage: build-hoglited.sh <hoglite-checkout> <commit> <sha256> <out>}
commit=${2:?missing commit}
want=${3:?missing sha256}
out=$(realpath -m "${4:?missing output path}")
go_version=${HOGLITE_GO_VERSION:?set HOGLITE_GO_VERSION, e.g. 1.27.1}

have_commit=$(git -C "$src" rev-parse HEAD)
if [[ $have_commit != "$commit" ]]; then
    echo "::error title=hoglited build::checkout is at ${have_commit}, the pin is ${commit} (expected sha256 ${want})" >&2
    exit 1
fi
have_go=$(GOTOOLCHAIN=local go env GOVERSION)
if [[ $have_go != "go${go_version}" ]]; then
    echo "::error title=hoglited build::Go is ${have_go}, the pin is go${go_version}" >&2
    exit 1
fi

mkdir -p "$(dirname "$out")"
(
    cd "$src"
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 GOAMD64=v1 GOTOOLCHAIN=local \
        go build -trimpath -buildvcs=false -o "$out" ./cmd/hoglited
)

have=$(sha256sum "$out" | awk '{print $1}')
echo "hoglited ${commit} go${go_version} sha256 ${have}"
if [[ $have != "$want" ]]; then
    echo "::error title=hoglited build::hoglited built from ${commit} with go${go_version} has sha256 ${have}; the pin expects ${want}. Re-pin the commit and digest together, or fix the toolchain." >&2
    rm -f "$out"
    exit 1
fi
