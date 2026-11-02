"""The shared VARIANT layout vectors: each declaration's Parquet elements.

``tests/vectors/variant_layout_vectors.json`` is what the server's
compaction is held to when it derives a variant column's output layout
(``VariantLayout.kt``, checked by ``VariantLayoutTest``), so it must be
exactly what :meth:`Plan.expected_elements` derives today, which
``test_variant_footer.py`` holds pyhoglake's own writer to. The file is
regenerated here and compared whole; the count is pinned on both sides.
"""

import importlib.util
import json
from pathlib import Path

from pyhoglake import variant

VECTORS = Path(__file__).parent / "vectors"
PATH = VECTORS / "variant_layout_vectors.json"

#: Pinned exactly, so a vector lost to a bad merge or a shrunken case list
#: fails instead of shrinking coverage in silence. Keep in step with
#: VariantLayoutVectorFile.EXPECTED_COUNT on the Kotlin side.
EXPECTED_VECTOR_COUNT = 49


def _generator():
    spec = importlib.util.spec_from_file_location(
        "regen_variant_layout_vectors", VECTORS / "regen_variant_layout_vectors.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_vector_file_is_what_the_generator_derives_today():
    on_disk = json.loads(PATH.read_text(encoding="utf-8"))
    # Through JSON, which is what the file holds: the elements' Thrift ids
    # become string keys and tuples lists.
    assert on_disk == json.loads(json.dumps(_generator().build()))


def test_vector_count_and_coverage():
    doc = json.loads(PATH.read_text(encoding="utf-8"))
    assert doc["format"] == "hoglake-variant-layout-vectors"
    assert doc["version"] == 1
    vectors = doc["vectors"]
    assert len(vectors) == EXPECTED_VECTOR_COUNT
    assert len({v["id"] for v in vectors}) == len(vectors)
    by_id = {v["id"]: v for v in vectors}
    # The cases the server leans on: no declaration, both nullabilities,
    # millrace's properties_sv, and decimal16's minimal FIXED widths across
    # the byte boundaries at precision 2/3, 9/10 and 18/19, up to 38's 16.
    assert by_id["undeclared"]["declaration"] is None
    assert {v["nullable"] for v in vectors} == {True, False}
    assert len(by_id["properties_sv"]["declaration"]["fields"]) == 16
    widths = {
        e["element"]["2"]
        for v in vectors
        if v["id"].startswith("root_decimal16")
        for e in v["elements"]
        if "2" in e["element"]
    }
    assert widths == {1, 2, 4, 5, 8, 9, 12, 16}
    # Every type of the grammar, read from it rather than restated: a
    # type added to it without a vector would leave the server's port
    # unchecked, and its tables silently never planned.
    roots = {v["declaration"]["type"] for v in vectors if v["id"].startswith("root_")}
    assert roots >= variant.PRIMITIVE_TYPES | variant.DECIMAL_TYPES.keys()
