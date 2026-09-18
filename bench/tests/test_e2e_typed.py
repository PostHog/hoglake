"""end-to-end-writer's typed mode: the real-parquet write path can carry
the full 1.1.0 type matrix, deterministically."""

import io

import pyarrow.parquet as pq
import pytest
from pyhoglake.types import schema_to_column_defs

from hoglake_bench.cli import build_parser
from hoglake_bench.datagen import WRITABLE_SCALARS
from hoglake_bench.scenarios import end_to_end


class TestArgs:
    def test_defaults_to_the_simple_schema(self):
        args = build_parser().parse_args(["end-to-end-writer"])
        assert args.table_schema == "simple"

    def test_typed_is_accepted(self):
        args = build_parser().parse_args(
            ["end-to-end-writer", "--table-schema", "typed"]
        )
        assert args.table_schema == "typed"

    def test_unknown_schema_is_refused(self):
        with pytest.raises(SystemExit):
            build_parser().parse_args(["end-to-end-writer", "--table-schema", "wat"])


class TestTypedBatches:
    def test_typed_batch_covers_the_matrix(self):
        batch = end_to_end.typed_batch(0, 128)
        types = set()

        def walk(defs):
            for d in defs:
                types.add(d["type"])
                walk(d.get("children") or [])

        walk(schema_to_column_defs(batch.schema))
        assert set(WRITABLE_SCALARS) <= types
        assert {"list", "struct", "map"} <= types

    def test_typed_batches_are_deterministic_per_index(self):
        def bytes_of(i):
            sink = io.BytesIO()
            pq.write_table(end_to_end.typed_batch(i, 64), sink)
            return sink.getvalue()

        assert bytes_of(3) == bytes_of(3)
        assert bytes_of(3) != bytes_of(4)
