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
# Usage: ci/fetch-hoglited.sh <output-dir>
# Prints the binary path. In GitHub Actions it also sets HOGLITE_BINARY
# and HOGLITE_BUILD in $GITHUB_ENV. Needs gh (GH_TOKEN in Actions),
# curl, jq and sha256sum.
set -euo pipefail

repo=PostHog/hoglite
out=${1:?usage: fetch-hoglited.sh <output-dir>}
case $(uname -m) in
    x86_64 | amd64) arch=amd64 ;;
    aarch64 | arm64) arch=arm64 ;;
    *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;;
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
for name in "$asset" checksums.txt; do
    url=$(jq -r --arg name "$name" '.assets[] | select(.name == $name) | .browser_download_url' <<<"$release")
    curl -fsSL --retry 3 -o "$out/$name" "$url"
done

# Exactly one checksum line for the asset, verified with the binary's own name.
line=$(grep -E "^[0-9a-f]{64}  $asset\$" "$out/checksums.txt" || true)
if [[ $(grep -c . <<<"$line") -ne 1 ]]; then
    echo "checksums.txt of $tag has no single entry for $asset" >&2
    exit 1
fi
(cd "$out" && sha256sum --check --strict <<<"$line") >&2
chmod +x "$out/$asset"

version=$("$out/$asset" -version)
expected="hoglited $tag (commit $commit)"
if [[ "$version" != "$expected" ]]; then
    echo "provenance mismatch: binary reports '$version', expected '$expected'" >&2
    exit 1
fi

if [[ -n "${GITHUB_ENV:-}" ]]; then
    echo "HOGLITE_BINARY=$out/$asset" >>"$GITHUB_ENV"
    echo "HOGLITE_BUILD=$tag" >>"$GITHUB_ENV"
fi
if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    echo "hoglite live tests ran against [$tag](https://github.com/$repo/releases/tag/$tag)." >>"$GITHUB_STEP_SUMMARY"
fi
echo "$out/$asset"
