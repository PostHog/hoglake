"""The determinism pin (D19): the encoder reproduces golden_storage.arrow.

The file holds the encoder's output over the fixed corpus of
variant_corpus.py (tests/data/variant/regen_golden_storage.py writes it).
Arrow arrays are compared, not Parquet bytes, so a pyarrow upgrade cannot
move it; a change to the encoder that moves one Variant byte, one typed
value or one null fails here, and a faster encoder must pass it unchanged.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pytest
from variant_corpus import DECLARATIONS, PYTHON_DECLARATIONS, golden_table

from pyhoglake import variant

GOLDEN = Path(__file__).parents[1] / "data" / "variant" / "golden_storage.arrow"


def bits(array: pa.Array) -> pa.Array:
    """``array`` with every float leaf viewed as integers, so that equality
    is of bits: Arrow's own says NaN != NaN, and -0.0 == 0.0."""
    kind = array.type
    mask = array.is_null() if array.null_count else None
    if pa.types.is_struct(kind):
        return pa.StructArray.from_arrays(
            [bits(array.field(i)) for i in range(kind.num_fields)],
            names=[field.name for field in kind],
            mask=mask,
        )
    if pa.types.is_list(kind):
        return pa.ListArray.from_arrays(array.offsets, bits(array.values), mask=mask)
    if kind == pa.float64():
        return array.view(pa.int64())
    if kind == pa.float32():
        return array.view(pa.int32())
    return array


@pytest.fixture(scope="module")
def golden() -> pa.Table:
    with pa.memory_map(str(GOLDEN)) as source:
        return pa.ipc.open_file(source).read_all()


@pytest.fixture(scope="module")
def current() -> pa.Table:
    return golden_table()


def test_the_corpus_is_the_one_pinned(golden):
    assert json.loads(golden.schema.metadata[b"declarations"]) == DECLARATIONS
    assert golden.column_names == [f"json/{n}" for n in DECLARATIONS] + [
        f"python/{n}" for n in PYTHON_DECLARATIONS
    ]


@pytest.mark.parametrize(
    "name",
    [f"json/{n}" for n in DECLARATIONS] + [f"python/{n}" for n in PYTHON_DECLARATIONS],
)
def test_the_encoder_reproduces_the_golden_storage(golden, current, name):
    pinned = golden.column(name).combine_chunks()
    now = current.column(name).combine_chunks()
    # With the field metadata, of which there is none to differ.
    assert now.type.equals(pinned.type, check_metadata=True)
    assert bits(now).equals(bits(pinned)), name
    decl = DECLARATIONS[name.split("/", 1)[1]]
    assert pinned.type.equals(variant.storage_type(decl), check_metadata=True)
    variant.verify(pinned, shredding=decl)
