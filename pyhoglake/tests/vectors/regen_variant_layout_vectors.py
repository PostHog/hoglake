"""Writes variant_layout_vectors.json: each declaration's Parquet layout,
as :meth:`Plan.expected_elements` derives it, for the server's port of it
(compaction's ``VariantLayout.kt``) to be held to.

    cd pyhoglake && flox activate -- uv run python tests/vectors/regen_variant_layout_vectors.py

``test_variant_layout_vectors.py`` regenerates the file in memory and
fails when it differs, so a change to the layout either side derives
reds until the file and the other side move with it.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pyhoglake._variant_codec import compile_plan
from pyhoglake.models import Column

OUT = Path(__file__).parent / "variant_layout_vectors.json"
FIELD_ID = 7
PRIMITIVES = [
    *("boolean", "int8", "int16", "int32", "int64", "float", "double", "date", "time"),
    *("timestamp", "timestamp_ns", "timestamptz", "timestamptz_ns"),
    *("binary", "string", "uuid"),
]
#: The declaration millrace's ``properties_sv`` takes: every field a string.
PROPERTIES_SV = [
    *("$session_id", "$window_id"),
    *(f"$group_{i}" for i in range(5)),
    *("$current_url", "$pathname", "$host", "$browser", "$os", "$device_type"),
    *("$lib", "$referring_domain", "$device_id"),
]


def _obj(*fields: tuple[str, dict[str, Any]]) -> dict[str, Any]:
    return {"type": "object", "fields": [{"name": n, **d} for n, d in fields]}


def _dec(kind: str, precision: int, scale: int) -> dict[str, Any]:
    return {"type": kind, "precision": precision, "scale": scale}


def _arr(element: dict[str, Any]) -> dict[str, Any]:
    return {"type": "array", "element": element}


def _nest(depth: int, leaf: dict[str, Any], array: bool) -> dict[str, Any]:
    # ``depth`` objects (or alternating objects and arrays) around ``leaf``,
    # the deepest the grammar allows at 16.
    node = leaf
    for level in range(depth):
        node = _arr(node) if array and level % 2 else _obj(("f", node))
    return node


def cases() -> list[tuple[str, bool, Any]]:
    """(id, nullable, declaration) for every vector, in file order."""
    out: list[tuple[str, bool, Any]] = [
        ("undeclared", True, None),
        ("undeclared_not_null", False, None),
        ("root_variant", True, {"type": "variant"}),
    ]
    out += [(f"root_{t}", True, {"type": t}) for t in PRIMITIVES]
    decimals = [("decimal4", p, s) for p, s in ((1, 0), (9, 2), (9, 9))]
    decimals += [("decimal8", p, s) for p, s in ((10, 0), (18, 2), (18, 18))]
    decimals += [("decimal16", p, 2 if p > 2 else 0) for p in (1, 2, 3, 9, 10, 18, 19)]
    decimals += [("decimal16", p, 4) for p in (20, 27, 28, 37, 38)]
    out += [(f"root_{k}_{p}_{s}", True, _dec(k, p, s)) for k, p, s in decimals]
    every = [(t, {"type": t}) for t in PRIMITIVES] + [
        (f"{k}_{p}", _dec(k, p, s)) for k, p, s in decimals[::3]
    ]
    out += [
        ("object_of_every_leaf", True, _obj(*every)),
        ("object_of_every_leaf_not_null", False, _obj(*every)),
        ("object_with_variant_field", True, _obj(("any", {"type": "variant"}))),
        ("array_of_strings", True, _arr({"type": "string"})),
        ("array_of_variants", True, _arr({"type": "variant"})),
        ("array_of_objects", True, _arr(_obj(*every[:4]))),
        ("array_of_arrays", True, _arr(_arr({"type": "int64"}))),
        ("depth_16_objects", True, _nest(16, {"type": "string"}, array=False)),
        ("depth_16_objects_and_arrays", True, _nest(16, {"type": "uuid"}, array=True)),
        (
            "names_spec_and_unicode",
            True,
            _obj(
                ("a.b", {"type": "int8"}),
                ("value", {"type": "string"}),
                ("typed_value", {"type": "variant"}),
                ("ключ ☃", {"type": "date"}),
            ),
        ),
        (
            "mixed_object",
            True,
            _obj(
                ("$browser", {"type": "string"}),
                ("price", _dec("decimal8", 18, 2)),
                ("tags", _arr({"type": "string"})),
                ("objs", _arr(_obj(("k", {"type": "string"})))),
                ("nested", _obj(("a", {"type": "int64"}), ("z", {"type": "int32"}))),
                ("payload", {"type": "variant"}),
            ),
        ),
        (
            "properties_sv",
            True,
            _obj(*((n, {"type": "string"}) for n in PROPERTIES_SV)),
        ),
    ]
    return out


def build() -> dict[str, Any]:
    vectors = []
    for case_id, nullable, declaration in cases():
        params = None if declaration is None else {"shredding": declaration}
        column = Column("v", "variant", FIELD_ID, 0, nullable, type_params=params)
        elements = compile_plan(declaration).expected_elements(column)
        vectors.append(
            {
                "id": case_id,
                "nullable": nullable,
                "declaration": declaration,
                "elements": [{"path": list(p), "element": e} for p, e in elements],
            }
        )
    return {
        "format": "hoglake-variant-layout-vectors",
        "version": 1,
        "description": (
            f"Plan.expected_elements of a variant column 'v' (field id {FIELD_ID}) "
            "per declaration: parquet.thrift SchemaElements by Thrift field id, in "
            "schema order. Converted types (6, with 7 and 8) are legacy spellings a "
            "writer may add or omit. Written by regen_variant_layout_vectors.py."
        ),
        "consumers": [
            "pyhoglake/tests/test_variant_layout_vectors.py",
            "server/src/test/kotlin/com/posthog/hoglake/compaction/VariantLayoutTest.kt",
        ],
        "vectors": vectors,
    }


def main() -> None:
    doc = build()
    OUT.write_text(
        json.dumps(doc, indent=1, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(f"wrote {len(doc['vectors'])} vectors to {OUT.name}")


if __name__ == "__main__":
    main()
