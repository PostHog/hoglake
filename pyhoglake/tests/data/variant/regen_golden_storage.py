"""Writes golden_storage.arrow, the encoder's output over the fixed corpus
of tests/variant_conformance/variant_corpus.py, as an Arrow IPC file.

    cd pyhoglake && flox activate -- uv run python tests/data/variant/regen_golden_storage.py

Regenerating means the encoder's output changed on purpose (D19): say what
changed and why in the commit, because every file written before it holds
the old bytes.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE.parents[1] / "variant_conformance"))

from variant_corpus import golden_table


def main() -> None:
    table = golden_table()
    with (
        pa.OSFile(str(HERE / "golden_storage.arrow"), "wb") as sink,
        pa.ipc.new_file(sink, table.schema) as writer,
    ):
        writer.write_table(table)
    print(f"wrote {table.num_columns} columns of {table.num_rows} rows")


if __name__ == "__main__":
    main()
