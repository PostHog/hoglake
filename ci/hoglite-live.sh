#!/usr/bin/env bash
# External main-only workflow entrypoints. Failures remain failures.
set -euo pipefail
cd "$(dirname "$0")/.."
case "${1:?expected role, pull or test}" in
    role)
        [[ ${HAS_ROLE:-false} == true ]] || {
            echo 'Live engine requires the configured ECR pull role' >&2
            exit 1
        }
        ;;
    pull)
        ci/fetch-hoglited.sh "${REGISTRY:?}" "${HOGLITED_IMAGE_DIGEST:?}" "${HOGLITE_COMMIT:?}" "${RUNNER_TEMP:?}/hoglited" \
            > "$RUNNER_TEMP/hoglite-engine.txt"
        cat "$RUNNER_TEMP/hoglite-engine.txt"
        { echo '### hoglite engine'; echo '```'; cat "$RUNNER_TEMP/hoglite-engine.txt"; echo '```'; } >> "${GITHUB_STEP_SUMMARY:?}"
        echo "HOGLITE_BINARY=$RUNNER_TEMP/hoglited" >> "${GITHUB_ENV:?}"
        ;;
    test)
        [[ -n ${HOGLITE_BINARY:-} && -x $HOGLITE_BINARY ]] || { echo 'Verified engine is missing' >&2; exit 1; }
        ./server/gradlew -p server --no-daemon :test
        ;;
    *) echo 'expected role, pull or test' >&2; exit 1 ;;
esac
