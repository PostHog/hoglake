#!/usr/bin/env bash
# Fetch the newest tested hoglited build for the hoglite live tests.
#
# hoglite publishes one GitHub pre-release per tested main commit, tagged
# build-<40-hex commit>. None is marked "latest", so this lists the
# releases and takes the newest published build-* that carries the
# binary and checksums.txt. It verifies the binary against that
# checksums.txt and checks that `hoglited -version` names the commit of
# the tag. Any failure stops the script: the caller must not run on an
# unverified engine.
#
# Usage: ci/fetch-hoglited.sh <output-dir> [arch]
# arch is amd64 or arm64; default: the host's. A binary already in
# <output-dir> that matches the newest build's checksum is kept, not
# downloaded again (the local runner calls this on every run).
# Prints the binary path and writes the tag to <output-dir>/BUILD. In
# GitHub Actions it also sets HOGLITE_BINARY and HOGLITE_BUILD in
# $GITHUB_ENV. Needs gh (GH_TOKEN in Actions), curl, jq and sha256sum
# (or shasum).
#
# HOGLITE_EXEC: a command prefix that runs the binary for the -version
# check, for a host that cannot execute it (ci/hoglite-live-local.sh
# fetches a Linux binary on macOS and runs the check in the container,
# with <output-dir> mounted at the same path). Default: run directly.
set -euo pipefail

repo=PostHog/hoglite
out=${1:?usage: fetch-hoglited.sh <output-dir> [arch]}
case ${2:-$(uname -m)} in
    x86_64 | amd64) arch=amd64 ;;
    aarch64 | arm64) arch=arm64 ;;
    *) echo "unsupported architecture ${2:-$(uname -m)}" >&2; exit 1 ;;
esac
asset=hoglited-linux-$arch

# Newest first by publication time; drafts are not published builds.
release=$(gh api "repos/$repo/releases?per_page=100" | jq -c --arg asset "$asset" '
    [ .[]
      | select(.draft | not)
      | select(.tag_name | test("^build-[0-9a-f]{40}$"))
      | select(any(.assets[]; .name == $asset) and any(.assets[]; .name == "checksums.txt")) ]
    | sort_by(.published_at) | last // empty')
if [[ -z "$release" ]]; then
    echo "no published build-* release of $repo carries $asset and checksums.txt" >&2
    exit 1
fi
tag=$(jq -r .tag_name <<<"$release")
commit=${tag#build-}
echo "hoglite build: $tag (published $(jq -r .published_at <<<"$release"), immutable: $(jq -r '.immutable // false' <<<"$release"))" >&2

mkdir -p "$out"
out=$(cd "$out" && pwd)
download() {
    url=$(jq -r --arg name "$1" '.assets[] | select(.name == $name) | .browser_download_url' <<<"$release")
    curl -fsSL --retry 3 -o "$out/$1" "$url"
}
download checksums.txt

# Exactly one checksum line for the asset, verified with the binary's own name.
line=$(grep -E "^[0-9a-f]{64}  $asset\$" "$out/checksums.txt" || true)
if [[ $(grep -c . <<<"$line") -ne 1 ]]; then
    echo "checksums.txt of $tag has no single entry for $asset" >&2
    exit 1
fi
expected_sha=${line%% *}
# GNU and BSD sha256sum agree on the digest, not on --check.
sha256() {
    if command -v sha256sum >/dev/null; then sha256sum "$1"; else shasum -a 256 "$1"; fi | cut -d' ' -f1
}
# The download is skipped only when the binary on disk already IS this
# build's; a different or damaged one is fetched again and verified.
if [[ -f "$out/$asset" && $(sha256 "$out/$asset") == "$expected_sha" ]]; then
    echo "$asset of $tag is already in $out" >&2
else
    download "$asset"
    actual_sha=$(sha256 "$out/$asset")
    if [[ "$actual_sha" != "$expected_sha" ]]; then
        echo "checksum mismatch for $asset of $tag: $actual_sha, checksums.txt says $expected_sha" >&2
        rm -f "$out/$asset"
        exit 1
    fi
    echo "$asset: sha256 $actual_sha verified" >&2
fi
chmod +x "$out/$asset"

read -ra exec_prefix <<<"${HOGLITE_EXEC:-}"
version=$("${exec_prefix[@]+"${exec_prefix[@]}"}" "$out/$asset" -version)
expected="hoglited $tag (commit $commit)"
if [[ "$version" != "$expected" ]]; then
    echo "provenance mismatch: binary reports '$version', expected '$expected'" >&2
    exit 1
fi
echo "$tag" >"$out/BUILD"

if [[ -n "${GITHUB_ENV:-}" ]]; then
    echo "HOGLITE_BINARY=$out/$asset" >>"$GITHUB_ENV"
    echo "HOGLITE_BUILD=$tag" >>"$GITHUB_ENV"
fi
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    echo "hoglite live tests ran against [$tag](https://github.com/$repo/releases/tag/$tag)." >>"$GITHUB_STEP_SUMMARY"
fi
echo "$out/$asset"
