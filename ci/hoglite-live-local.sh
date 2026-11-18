#!/usr/bin/env bash
# Run the hoglite live tests locally, in a Linux container.
#
# hoglite ships hoglited for Linux only (hoglited-linux-amd64/-arm64), so
# on macOS the live classes always skip. This runs them the way CI and
# production see the engine: the newest tested Linux build, under a
# Linux JDK. The container shares the host's Docker daemon (the socket
# is mounted), so Testcontainers inside it starts MinIO as a sibling
# container, and runs on the host network because
# HogliteConformanceEngine reaches MinIO at 127.0.0.1:<mapped port>.
# It ends with ci/check-hoglite-live.sh, the same fail-closed check as
# the hoglite-live CI job: the classes ran, none skipped.
#
# Usage: ci/hoglite-live-local.sh [test-pattern ...]
# Each pattern is a Gradle --tests filter; the default is every
# com.posthog.hoglake.hoglite.*IntegrationTest. `just test-hoglite`.
# A pattern must match only *IntegrationTest classes of that package:
# the check reads those reports alone, so a run of anything else ends
# with "no report" even when its tests passed.
#
# Needs docker, gh (authenticated), jq, curl, sha256sum or shasum on
# the host.
#
# Environment:
#   HOGLITE_LIVE_CACHE   persistent state, default
#                        ${XDG_CACHE_HOME:-~/.cache}/hoglake/hoglite-live:
#                        bin/<arch>/<build tag>/ (the engines; old tags
#                        are not pruned, 25 MB each), gradle/ (a Linux
#                        GRADLE_USER_HOME: never the host's ~/.gradle),
#                        project-cache/<checkout>/ (--project-cache-dir,
#                        in place of server/.gradle, per worktree)
#   HOGLITE_LIVE_ARCH    amd64 or arm64, default: the Docker daemon's
#   HOGLITE_LIVE_BINARY  a hoglited for HOGLITE_LIVE_ARCH to run instead
#                        of fetching the newest build (a local hoglite
#                        checkout's, say); unverified
#   HOGLITE_LIVE_IMAGE   the JDK image, default eclipse-temurin:25
#   DOCKER_HOST          honoured; otherwise the active docker context's
#                        endpoint, then the usual socket paths (Docker
#                        Desktop, OrbStack, colima, rootless Linux)
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cache=${HOGLITE_LIVE_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/hoglake/hoglite-live}
image=${HOGLITE_LIVE_IMAGE:-eclipse-temurin:25}
patterns=("$@")
if [[ ${#patterns[@]} -eq 0 ]]; then
    patterns=('com.posthog.hoglake.hoglite.*IntegrationTest')
fi
case "$repo_dir$cache" in
    *[[:space:]]*) echo "the checkout and cache paths must not contain whitespace: '$repo_dir', '$cache'" >&2; exit 1 ;;
esac
for tool in docker gh jq curl; do
    command -v "$tool" >/dev/null || { echo "$tool is required on the host" >&2; exit 1; }
done
command -v sha256sum >/dev/null || command -v shasum >/dev/null || { echo "sha256sum or shasum is required on the host" >&2; exit 1; }

# ---- the Docker daemon ----------------------------------------------------
# Inside the container, Testcontainers wants /var/run/docker.sock. A unix
# DOCKER_HOST (or context endpoint) is mounted there; a tcp one is passed
# through, reachable as-is on the host network.
endpoint=${DOCKER_HOST:-$(docker context inspect --format '{{(index .Endpoints "docker").Host}}' 2>/dev/null || true)}
socket=
docker_env=()
case "$endpoint" in
    unix://*) socket=${endpoint#unix://} ;;
    tcp://*) docker_env=(-e "DOCKER_HOST=$endpoint") ;;
esac
if [[ -z "$socket" && ${#docker_env[@]} -eq 0 ]]; then
    for candidate in /var/run/docker.sock "$HOME/.orbstack/run/docker.sock" "$HOME/.docker/run/docker.sock" \
        "$HOME/.colima/default/docker.sock" "$HOME/.colima/docker.sock" "${XDG_RUNTIME_DIR:-/nonexistent}/docker.sock"; do
        if [[ -S "$candidate" ]]; then socket=$candidate; break; fi
    done
fi
if [[ -n "$socket" ]]; then
    if [[ ! -S "$socket" ]]; then
        echo "Docker socket $socket does not exist; is the daemon running?" >&2
        exit 1
    fi
    # The daemon resolves a bind source on ITS filesystem. On macOS that
    # is the VM, where the host-side socket (Docker Desktop's
    # ~/.docker/run, colima's ssh-forwarded one) may not exist or bind,
    # but /var/run/docker.sock is the daemon's own; on Linux the paths
    # are one filesystem.
    # Ryuk is a sibling container, so its socket bind is resolved on the
    # daemon's filesystem too: tell Testcontainers the daemon-side path,
    # not the /var/run/docker.sock it sees in here (absent, or another
    # daemon's, under rootless Docker at /run/user/<uid>/docker.sock).
    source=$socket
    if [[ $(uname -s) == Darwin ]]; then source=/var/run/docker.sock; fi
    docker_env=(-v "$source:/var/run/docker.sock" -e "TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=$source")
elif [[ ${#docker_env[@]} -eq 0 ]]; then
    echo "no Docker daemon found: set DOCKER_HOST" >&2
    exit 1
fi
if ! daemon_arch=$(docker version --format '{{.Server.Arch}}' 2>/dev/null) || [[ -z "$daemon_arch" ]]; then
    echo "cannot reach the Docker daemon (${endpoint:-no endpoint})" >&2
    exit 1
fi
arch=${HOGLITE_LIVE_ARCH:-$daemon_arch}
case "$arch" in
    amd64 | arm64) ;;
    *) echo "unsupported container architecture $arch" >&2; exit 1 ;;
esac
platform=linux/$arch

# On Linux a rootful daemon would otherwise write the build tree and the
# cache as root; run as the caller, with the daemon's group for the
# socket. A rootless daemon (socket under XDG_RUNTIME_DIR, or the
# caller's) already maps container root to the caller, and the caller's
# uid inside it would be an unprivileged subordinate id. macOS daemons
# map ownership to the host user already.
user_args=()
if [[ $(uname -s) == Linux ]]; then
    rootless=false
    if [[ -n "$socket" ]]; then
        case "$socket" in "${XDG_RUNTIME_DIR:-/nonexistent}"/*) rootless=true ;; esac
        if [[ $(stat -c %u "$socket") == "$(id -u)" ]]; then rootless=true; fi
    fi
    if [[ $rootless == false ]]; then
        user_args=(--user "$(id -u):$(id -g)" -e "HOME=$cache/home")
        mkdir -p "$cache/home"
        if [[ -n "$socket" ]]; then user_args+=(--group-add "$(stat -c %g "$socket")"); fi
    fi
fi

# ---- the engine -----------------------------------------------------------
bin_dir=$cache/bin/$arch
mkdir -p "$bin_dir"
if [[ -n "${HOGLITE_LIVE_BINARY:-}" ]]; then
    binary=$(cd "$(dirname "$HOGLITE_LIVE_BINARY")" && pwd)/$(basename "$HOGLITE_LIVE_BINARY")
    test -x "$binary" || { echo "HOGLITE_LIVE_BINARY=$HOGLITE_LIVE_BINARY is not executable" >&2; exit 1; }
    mounts=(-v "$(dirname "$binary"):$(dirname "$binary"):ro")
    export HOGLITE_BUILD="$binary (HOGLITE_LIVE_BINARY, unverified)"
else
    # The host cannot run a Linux binary, so the fetch's provenance check
    # (`hoglited -version`) runs in the container, bin_dir at its own path.
    # The fetch files each build under bin_dir/<tag>/; only that build's
    # directory is mounted, so the engine run IS the one named, however
    # many runs overlap.
    binary=$(HOGLITE_EXEC="docker run --rm --platform $platform -v $bin_dir:$bin_dir:ro $image" \
        "$repo_dir/ci/fetch-hoglited.sh" "$bin_dir" "$arch")
    build_dir=$(dirname "$binary")
    mounts=(-v "$build_dir:$build_dir:ro")
    HOGLITE_BUILD=$(basename "$build_dir")
    export HOGLITE_BUILD
fi

# ---- the run --------------------------------------------------------------
# The checkout and the cache are mounted at their host paths, so the
# Gradle caches key on the same paths every run, and the reports name
# files where the host finds them. One project cache per checkout: the
# worktrees must not share one.
project_cache=$cache/project-cache/$(cksum <<<"$repo_dir" | cut -d' ' -f1)
mkdir -p "$cache/gradle" "$project_cache"
results=$repo_dir/server/build/test-results/test
# Only this run's reports count: a host `:test` leaves reports of the
# same classes, skipped, which the check must not read.
rm -f "$results"/TEST-com.posthog.hoglake.hoglite.*.xml
tests_args=()
for pattern in "${patterns[@]}"; do tests_args+=(--tests "$pattern"); done
tty_args=()
if [[ -t 0 && -t 1 ]]; then tty_args=(-t); fi

echo "hoglite live: $platform in $image, engine $HOGLITE_BUILD, filters: ${patterns[*]}" >&2
status=0
docker run --rm -i "${tty_args[@]+"${tty_args[@]}"}" --init \
    --platform "$platform" --network host \
    "${docker_env[@]}" "${user_args[@]+"${user_args[@]}"}" \
    -v "$repo_dir:$repo_dir" -v "$cache:$cache" "${mounts[@]}" \
    -w "$repo_dir/server" \
    -e "GRADLE_USER_HOME=$cache/gradle" \
    -e "HOGLITE_BINARY=$binary" -e HOGLITE_REQUIRED=1 \
    "$image" ./gradlew --no-daemon --console=plain --project-cache-dir "$project_cache" \
    :test --rerun "${tests_args[@]}" || status=$?
if [[ $status -ne 0 ]]; then
    echo "hoglite live: gradle exited $status; report: $repo_dir/server/build/reports/tests/test/index.html" >&2
fi
"$repo_dir/ci/check-hoglite-live.sh" "$results" || status=$?
exit "$status"
