"""Writes corpus_stamped.parquet, the footer splice's fixed expected output.

    cd pyhoglake && flox activate -- uv run python tests/data/variant/regen_footer_fixtures.py

The file is the encoder's output over the fixed corpus
(tests/variant_conformance/variant_corpus.py), one variant column per
declaration with field ids 1 to 20, its decimal4/decimal8 leaves viewed as
int32/int64, as pyarrow writes it with ``store_schema=False`` (zstd, to keep
it small), and then stamped by the test oracle (tests/footer_oracle.py),
not by the code under test: VARIANT(1) on each group, DECIMAL(p, s) on each
decimal leaf. The pyarrow-written input is not stored: the oracle's
``unstamp`` gives it back exactly, since its round trip is byte for byte.
test_variant_footer.py requires the splice to turn that input into this
file byte for byte, and every pyarrow of the CI matrix to read this file
as the plans' read forms.

Regenerate when the corpus changes, and say why in the commit.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).parent
TESTS = HERE.parents[1]
sys.path.insert(0, str(TESTS / "variant_conformance"))
sys.path.insert(0, str(TESTS))

import footer_oracle
from variant_corpus import corpus_columns, golden_table, integer_view, oracle_stamps


def main() -> None:
    table = golden_table()
    columns = corpus_columns(table)
    fields, data = [], []
    for column in columns:
        kind = integer_view(table.schema.field(column.name).type)
        fields.append(
            pa.field(
                column.name,
                kind,
                metadata={b"PARQUET:field_id": str(column.field_id).encode()},
            )
        )
        chunks = table.column(column.name).chunks
        data.append(pa.chunked_array([chunk.view(kind) for chunk in chunks], kind))
    sink = pa.BufferOutputStream()
    pq.write_table(
        pa.table(data, schema=pa.schema(fields)),
        sink,
        store_schema=False,
        compression="zstd",
    )
    raw = sink.getvalue().to_pybytes()
    stamps = oracle_stamps(raw, columns)
    stamped = footer_oracle.stamp(raw, stamps)
    if footer_oracle.unstamp(stamped, stamps) != raw:
        raise SystemExit("the oracle does not give the written file back")
    (HERE / "corpus_stamped.parquet").write_bytes(stamped)
    print(f"wrote {len(columns)} columns, {len(stamped)} bytes")


if __name__ == "__main__":
    main()
