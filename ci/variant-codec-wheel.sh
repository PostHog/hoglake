#!/bin/sh
# Tests one built pyhoglake-variant-codec wheel, or the sdist (built into a
# wheel here, as pip would), as it ships: pyhoglake's locked env from the
# repo root with exactly that file installed, on the file's own platform.
# pyhoglake must take it and encode as the Python codec does
# (ci/variant_codec_smoke.py); then, unless --smoke-only, pyhoglake's
# variant suites run through it, and conftest fails any fallback to Python.
#
#   sh ci/variant-codec-wheel.sh <wheel or sdist> [--smoke-only]
set -eu
uv sync --locked --package pyhoglake --no-install-package pyhoglake-variant-codec
uv pip install --no-deps "$1"
uv run --no-sync python ci/variant_codec_smoke.py
if [ "${2:-}" != --smoke-only ]; then
  cd pyhoglake
  HOGLAKE_URL=http://127.0.0.1:9 HOGLAKE_S3_ENDPOINT=http://127.0.0.1:9 \
    uv run --no-sync pytest -m "not integration" -q -k variant
fi
