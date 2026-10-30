"""The shared object-prefix layout vectors.

``tests/vectors/object_prefix_vectors.json`` is what the server's path
helper is held to (``ObjectPrefix.kt``, checked by
``ObjectPrefixVectorsTest`` and ``ObjectPrefixTest``). No pyhoglake code
builds these paths yet; the writer that does must be held to the same
file. Until then this pins the file itself: it is exactly what the
generator writes, and the count is pinned on both sides.
"""

import importlib.util
import json
from pathlib import Path

VECTORS = Path(__file__).parent / "vectors"
PATH = VECTORS / "object_prefix_vectors.json"

#: Pinned exactly, so a vector lost to a bad merge fails instead of
#: shrinking coverage in silence. Keep in step with
#: ObjectPrefixVectorFile.EXPECTED_COUNT on the Kotlin side.
EXPECTED_VECTOR_COUNT = 69


def _generator():
    spec = importlib.util.spec_from_file_location(
        "regen_object_prefix_vectors", VECTORS / "regen_object_prefix_vectors.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_vector_file_is_what_the_generator_writes():
    on_disk = json.loads(PATH.read_text(encoding="utf-8"))
    assert on_disk == json.loads(json.dumps(_generator().build()))


def test_vector_file_header_and_count():
    doc = json.loads(PATH.read_text(encoding="utf-8"))
    assert doc["format"] == "hoglake-object-prefix-vectors"
    assert doc["version"] == 1
    assert _generator().vector_count(doc) == EXPECTED_VECTOR_COUNT
