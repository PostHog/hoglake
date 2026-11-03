"""Checks an installed pyhoglake-variant-codec against pyhoglake's Python
codec: pyhoglake takes it (its protocol and version), and it encodes JSON
text exactly as Python does, valid rows and invalid, unshredded and
shredded, in both on_invalid modes. CI runs it on each wheel and the sdist
(ci/variant-codec-wheel.sh) and in the millrace image, on its platform.
"""

from __future__ import annotations

import sys
from dataclasses import replace
from importlib import metadata

from pyhoglake import VariantEncodingError, _variant_backend
from pyhoglake import _variant_codec as codec
from pyhoglake.variant import VariantReport, _storage_slots

ROWS = [
    '{"$browser": "Firefox", "n": -0.0, "big": 123456789012345678901234567890}',
    '{"t": [1, 2.5, "x", null, true], "$set": {"plan": "free", "n": 1e400}}',
    '{"$set": {"plan": 7}, "\\u00e9": "\\ud83d\\ude00", "": {}, "t": []}',
    None,
    "null",
    '{"a": 1, "a": 2}',
    '["\\ud800"]',
    "[" * 200 + "]" * 200,
    '{"t": [1.5, ',
]
SHREDDED = {"type": "object", "fields": [
    {"name": "$browser", "type": "string"},
    {"name": "t", "type": "array", "element": {"type": "double"}},
    {"name": "$set", "type": "object", "fields": [{"name": "plan", "type": "string"}]},
]}  # fmt: skip


def encode(backend: str, plan: codec.Plan, on_invalid: str) -> object:
    """The storage slots and report, or the error, of one backend."""
    report = VariantReport()
    try:
        out = _variant_backend.encode(
            ROWS, plan, report, json_text=True, column="c", nullable=True,
            on_invalid=on_invalid, backend=backend,
        )  # fmt: skip
    except VariantEncodingError as exc:
        return str(exc), exc.reason, exc.row, exc.path
    # Only Rust counts the rows a parser's recursion bound left to Python.
    report = replace(report, encode_seconds=0.0, recursion_bound_rows=0)
    return [_storage_slots(chunk) for chunk in out], report


def main() -> None:
    module, why = _variant_backend.native()
    if module is None:
        sys.exit(f"pyhoglake does not take the codec: {why}")
    for declaration in (None, SHREDDED):
        plan = codec.compile_plan(declaration)
        for on_invalid in ("null", "raise"):
            native = encode("native", plan, on_invalid)
            if native != encode("python", plan, on_invalid):
                sys.exit(f"the codecs differ under {declaration}, {on_invalid}")
    version = metadata.version("pyhoglake-variant-codec")
    print(f"pyhoglake-variant-codec {version} encodes as the Python codec does")


if __name__ == "__main__":
    main()
