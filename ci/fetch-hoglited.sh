#!/usr/bin/env bash
# Take hoglited out of a hoglite release image pinned by digest, and
# refuse it unless the image is the pinned one, built from the pinned
# commit.
#
# Usage: ci/fetch-hoglited.sh <registry> <image-digest> <commit> <out>
#
#   <registry>      the registry host; the repository is "hoglite"
#   <image-digest>  sha256:... of the image (index) in that repository
#   <commit>        the hoglite commit the image was built from; checked
#                   against the image's org.opencontainers.image.revision
#                   label, which hoglite's release workflow sets
#   <out>           where to write the binary
#
# The digest is the integrity check. A digest reference is
# content-addressed: docker verifies every pulled manifest and layer
# against it and refuses a mismatch, so the pinned digest fixes every
# byte of the engine, the binary included. A second hash of the binary
# would add nothing, and would break whenever the release is rebuilt on
# a newer Go patch. The label check catches what a digest cannot: a
# wrong or mislabelled release. The Go toolchain and build settings
# are printed (go version -m) so the build can be reproduced from
# source.
#
# Exits non-zero, naming the image digest and the commit, if the pin is
# malformed, the pull fails, the label names another commit, or the
# binary is missing.
set -euo pipefail

usage='usage: fetch-hoglited.sh <registry> <image-digest> <commit> <out>'
registry=${1:?$usage}
digest=${2:?$usage}
commit=${3:?$usage}
out=$(realpath -m "${4:?$usage}")
ref="${registry}/hoglite@${digest}"
what="hoglite image ${digest} (expected hoglite ${commit})"

fail() {
    echo "::error title=hoglite engine unavailable::$*" >&2
    exit 1
}

[[ $digest =~ ^sha256:[0-9a-f]{64}$ ]] || fail "the image pin '${digest}' is not a sha256:<64 hex> digest"
[[ $commit =~ ^[0-9a-f]{40}$ ]] || fail "the commit pin '${commit}' is not a 40-hex commit"

# By digest: docker refuses content that does not match it.
docker pull --quiet --platform linux/amd64 "$ref" >/dev/null ||
    fail "cannot pull ${what}. The pull role lost access, the digest is not in the repository, or the registry served content that does not match the digest (docker's error above says which); re-pin HOGLITE_COMMIT and HOGLITED_IMAGE_DIGEST together."

label=$(docker image inspect --format '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$ref")
[[ $label == "$commit" ]] ||
    fail "${what}: the image's org.opencontainers.image.revision label is '${label}', not ${commit}. Re-pin HOGLITE_COMMIT and HOGLITED_IMAGE_DIGEST together."

id=$(docker create --platform linux/amd64 "$ref")
trap 'docker rm -f "$id" >/dev/null 2>&1 || true' EXIT
mkdir -p "$(dirname "$out")"
rm -f "$out"
docker cp "${id}:/usr/local/bin/hoglited" "$out" >/dev/null 2>&1 ||
    fail "${what}: /usr/local/bin/hoglited is not in the image"
chmod 0755 "$out"

echo "hoglited from hoglite image ${digest}, revision ${label}"
echo "binary sha256 $(sha256sum "$out" | cut -d' ' -f1) (informational; the image digest is the pin)"
if command -v go >/dev/null; then
    # Toolchain and build settings, without the dependency list.
    go version -m "$out" | grep -v -P '^\t(dep|=>)\t' || echo "go version -m could not read the build info"
else
    echo "go is not on PATH; the toolchain is not recorded"
fi
