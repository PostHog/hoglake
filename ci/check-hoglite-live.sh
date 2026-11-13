#!/usr/bin/env bash
# The fail-closed proof for the hoglite live tests: the live classes
# ran, and none skipped. Shared by the hoglite-live CI job and
# ci/hoglite-live-local.sh, so the two cannot drift.
#
# Usage: ci/check-hoglite-live.sh [results-dir]
# results-dir defaults to server/build/test-results/test. Reads the
# JUnit XML of every com.posthog.hoglake.hoglite.*IntegrationTest class
# there; fails when there is none, when they hold no test, or when any
# test skipped. HOGLITE_BUILD, when set, names the engine in the summary.
set -euo pipefail

repo_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
results=${1:-"$repo_dir/server/build/test-results/test"}

shopt -s nullglob
reports=("$results"/TEST-com.posthog.hoglake.hoglite.*IntegrationTest.xml)
if [[ ${#reports[@]} -eq 0 ]]; then
    echo "hoglite live: no report of a hoglite *IntegrationTest class in $results" >&2
    exit 1
fi
sum() { cat "${reports[@]}" | grep -o "<testsuite [^>]*" | grep -o " $1=\"[0-9]*\"" | tr -dc '0-9\n' | awk '{ n += $1 } END { print n + 0 }'; }
tests=$(sum tests)
skipped=$(sum skipped)
echo "hoglite live: $tests tests in ${#reports[@]} classes, $skipped skipped against ${HOGLITE_BUILD:-an unnamed build}"
if [[ "$tests" -eq 0 ]]; then
    echo "hoglite live: the live classes ran no test" >&2
    exit 1
fi
if [[ "$skipped" -ne 0 ]]; then
    echo "hoglite live: $skipped live tests skipped; the engine did not run them" >&2
    exit 1
fi
