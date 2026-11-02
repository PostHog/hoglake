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
from variant_helpers import storage_slots

from pyhoglake import variant

GOLDEN = Path(__file__).parents[1] / "data" / "variant" / "golden_storage.arrow"


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
    # Every slot, floats as bits and the placeholders under null parents
    # too, which Arrow's equality skips.
    assert storage_slots(now) == storage_slots(pinned), name
    decl = DECLARATIONS[name.split("/", 1)[1]]
    assert pinned.type.equals(variant.storage_type(decl), check_metadata=True)
    variant.verify(pinned, shredding=decl)
