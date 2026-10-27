#!/usr/bin/env bash
# This script gives the functions for .github/workflows/ecr-release.yml.
#
# The workflow publishes hoglake-server and hoglake-webui to ECR for
# a Kargo Warehouse. The Warehouse makes Freight only when the newest
# release tag is the same in both repositories. The Warehouse also
# checks the tag format and the OCI annotations on each image index.
# These functions make and check that contract.
#
# Subcommands:
#   gate <owner/repo> <sha>
#   select <registry> <sha> <repository>...
#   verify <registry>/<repository>:<tag> <sha>
#   check-release <registry> <tag> <sha> <repository>...
#
# gate and select write key=value lines to stdout for GITHUB_OUTPUT.
# All other text goes to stderr.

set -Eeuo pipefail
# A command substitution does not inherit errexit by default. This
# option makes a failure inside $(...) stop the script too.
shopt -s inherit_errexit

SOURCE_URL=https://github.com/PostHog/hoglake
TAG_RE='^r[0-9]{12}-[0-9a-f]{6}$'
SHA_RE='^[0-9a-f]{40}$'

log() { printf '%s\n' "$*" >&2; }
die() {
  printf '::error::%s\n' "$*" >&2
  exit 1
}

require_sha() {
  [[ $1 =~ $SHA_RE ]] || die "not a 40-hex commit: '$1'"
}

# gate: read the Server and Webui runs for one main commit.
# The test gates are the server-checks and webui-test jobs. A gate job
# reports skipped when its path filter did not select the commit.
# The function publishes when no gate failed and one gate passed.
# It does not read the deploy jobs. A gigahog failure must not stop it.
gate() {
  local repo=$1 sha=$2
  require_sha "$sha"
  local runs
  runs=$(gh api "repos/${repo}/actions/runs?head_sha=${sha}&event=push&per_page=100")

  local passed=0 workflow job run result changes
  for pair in server.yml:server-checks webui.yml:webui-test; do
    workflow=${pair%%:*}
    job=${pair#*:}
    run=$(jq -r --arg path ".github/workflows/${workflow}" \
      '[.workflow_runs[] | select(.path == $path)] | max_by(.id) | .id // empty' <<<"$runs")
    if [[ -z $run ]]; then
      log "${workflow}: no run for ${sha} yet. The next completed run decides."
      echo "publish=false"
      return 0
    fi
    local jobs
    jobs=$(gh api "repos/${repo}/actions/runs/${run}/jobs?filter=latest&per_page=100")
    changes=$(job_result "$jobs" changes)
    result=$(job_result "$jobs" "$job")
    log "${workflow} run ${run}: changes=${changes} ${job}=${result}"
    case "$changes" in
      completed/success) ;;
      completed/*)
        log "${workflow}: the changes job did not pass. Do not publish."
        echo "publish=false"
        return 0
        ;;
      *)
        log "${workflow}: the changes job is not complete. The next completed run decides."
        echo "publish=false"
        return 0
        ;;
    esac
    case "$result" in
      completed/success) passed=$((passed + 1)) ;;
      completed/skipped) ;;
      completed/*)
        log "${workflow}: ${job} did not pass. Do not publish."
        echo "publish=false"
        return 0
        ;;
      *)
        log "${workflow}: ${job} is not complete. The next completed run decides."
        echo "publish=false"
        return 0
        ;;
    esac
  done

  if ((passed == 0)); then
    log "No server or webui change in ${sha}. Nothing to publish."
    echo "publish=false"
    return 0
  fi
  log "The test gates passed for ${sha}. Publish both images."
  echo "publish=true"
}

# job_result <jobs-json> <name>: print status/conclusion, or missing.
job_result() {
  jq -r --arg name "$2" '
    [.jobs[] | select(.name == $name)]
    | if length == 1 then .[0] | "\(.status)/\(.conclusion)" else "missing" end' <<<"$1"
}

# list_tags <image>: print all tags of one repository.
# A repository that does not exist yet gives an empty list. All other
# errors stop the script.
list_tags() {
  local image=$1 out
  if ! out=$(crane ls "$image" 2>&1); then
    if grep -q NAME_UNKNOWN <<<"$out"; then
      return 0
    fi
    die "cannot list tags of ${image}: ${out}"
  fi
  printf '%s\n' "$out"
}

# list_release_tags <image> <sha6>: print the release tags of one commit.
list_release_tags() {
  local tags
  tags=$(list_tags "$1") || return 1
  grep -E "^r[0-9]{12}-${2}$" <<<"$tags" || true
}

# select: choose ONE release tag for all repositories.
#
# This adapts the trino-gateway rerun guard to two repositories. The
# ECR tags are immutable. A rerun must not make a second tag for a
# commit that has a good release. Thus the function does this:
#   1. It finds the newest release tag of this commit in any repository.
#   2. If each copy of that tag verifies, it uses the tag again. It
#      pushes only to the repositories that do not have the tag. Thus
#      a rerun repairs a run that pushed one image and then failed.
#   3. If there is no tag, or a copy fails verification, it makes a
#      new tag from the current UTC minute. Then it pushes all images.
select_tag() {
  local registry=$1 sha=$2
  shift 2
  require_sha "$sha"
  (($# > 0)) || die "select needs at least one repository"
  local short=${sha:0:6}

  declare -A tags_of
  local all="" name
  for name in "$@"; do
    tags_of[$name]=$(list_release_tags "${registry}/${name}" "$short") ||
      die "cannot read the release tags of ${name}"
    all+="${tags_of[$name]}"$'\n'
  done
  local newest
  newest=$(grep -E "$TAG_RE" <<<"$all" | sort -u | tail -n 1 || true)

  if [[ -n $newest ]]; then
    local good=true
    for name in "$@"; do
      if grep -qx "$newest" <<<"${tags_of[$name]}"; then
        if ! verify "${registry}/${name}:${newest}" "$sha"; then
          log "::warning::${name}:${newest} fails verification. Make a new release tag."
          good=false
        fi
      fi
    done
    if [[ $good == true ]]; then
      log "Use the existing release tag ${newest} for ${sha}."
      echo "tag=${newest}"
      for name in "$@"; do
        if grep -qx "$newest" <<<"${tags_of[$name]}"; then
          echo "push-${name}=false"
        else
          echo "push-${name}=true"
        fi
      done
      return 0
    fi
  fi

  # The new tag must not exist in any repository. A failed release in
  # the same minute can have that tag. Then the loop waits for the
  # next minute.
  local tag attempt
  for attempt in $(seq 1 10); do
    tag="r$(date -u +%Y%m%d%H%M)-${short}"
    if ! grep -qx "$tag" <<<"$all"; then
      break
    fi
    log "${tag} exists already (attempt ${attempt}). Wait for the next minute."
    sleep 15
    tag=""
  done
  [[ -n $tag ]] || die "cannot make a new release tag for ${sha}"
  log "Make the new release tag ${tag} for ${sha}."
  echo "tag=${tag}"
  for name in "$@"; do
    echo "push-${name}=true"
  done
}

# verify: check one pushed image against the Warehouse criteria.
# The Warehouse reads the image index, so the index must have the
# annotations. The build also writes them to each per-arch manifest
# and config. This function checks those copies too.
verify() {
  local ref=$1 sha=$2
  require_sha "$sha"
  local tag=${ref##*:}
  if ! [[ $tag =~ $TAG_RE ]]; then
    log "${ref}: the tag does not match ${TAG_RE}"
    return 1
  fi
  if [[ ${tag:14:6} != "${sha:0:6}" ]]; then
    log "${ref}: the tag does not end with the commit prefix ${sha:0:6}"
    return 1
  fi

  local index
  index=$(crane manifest "$ref") || {
    log "${ref}: cannot read the index"
    return 1
  }
  if ! jq -e --arg rev "$sha" --arg src "$SOURCE_URL" '
    .mediaType == "application/vnd.oci.image.index.v1+json"
    and ([.manifests[]
          | select(.annotations["vnd.docker.reference.type"] != "attestation-manifest")
          | .platform | .os + "/" + .architecture] | sort == ["linux/amd64", "linux/arm64"])
    and .annotations["org.opencontainers.image.revision"] == $rev
    and .annotations["org.opencontainers.image.source"] == $src' >/dev/null <<<"$index"; then
    log "${ref}: the index does not have the two platforms and the revision and source annotations"
    return 1
  fi

  local arch
  for arch in amd64 arm64; do
    if ! crane manifest --platform "linux/${arch}" "$ref" | jq -e --arg rev "$sha" --arg src "$SOURCE_URL" '
      .annotations["org.opencontainers.image.revision"] == $rev
      and .annotations["org.opencontainers.image.source"] == $src' >/dev/null; then
      log "${ref}: the linux/${arch} manifest does not have the revision and source annotations"
      return 1
    fi
    if ! crane config --platform "linux/${arch}" "$ref" | jq -e --arg rev "$sha" --arg src "$SOURCE_URL" '
      .config.Labels["org.opencontainers.image.revision"] == $rev
      and .config.Labels["org.opencontainers.image.source"] == $src' >/dev/null; then
      log "${ref}: the linux/${arch} config does not have the revision and source labels"
      return 1
    fi
  done
  log "${ref}: verified"
}

# check-release: this is the last check before Kargo. Each repository
# must have the same tag, and each copy must verify. Then the Warehouse
# can make Freight from this commit.
check_release() {
  local registry=$1 tag=$2 sha=$3
  shift 3
  require_sha "$sha"
  (($# > 0)) || die "check-release needs at least one repository"
  local name failed=0 newest newest_seen=""
  for name in "$@"; do
    verify "${registry}/${name}:${tag}" "$sha" || failed=1
  done
  ((failed == 0)) || die "the release ${tag} is not complete. The Warehouse cannot make Freight from it."

  # Another commit can publish at the same time. Then the newest tags
  # can differ for a short time. Thus this is a warning, not a failure.
  # If the warning stays, a newer release is not complete. Then run the
  # ECR release again for the commit in the newest tag.
  local tags
  for name in "$@"; do
    tags=$(list_tags "${registry}/${name}") || die "cannot read the tags of ${name}"
    newest=$(grep -E "$TAG_RE" <<<"$tags" | sort | tail -n 1 || true)
    if [[ -n $newest_seen && $newest != "$newest_seen" ]]; then
      log "::warning::The newest release tags differ between repositories (${newest_seen} and ${newest}). Kargo makes no Freight until they agree. If this stays, run the ECR release again for the commit of the newer tag."
    fi
    newest_seen=$newest
  done
  log "The release ${tag} is complete in: $*"
}

cmd=${1:-}
shift || true
case "$cmd" in
  gate) gate "$@" ;;
  select) select_tag "$@" ;;
  verify) verify "$@" ;;
  check-release) check_release "$@" ;;
  *) die "usage: $0 gate|select|verify|check-release ..." ;;
esac
