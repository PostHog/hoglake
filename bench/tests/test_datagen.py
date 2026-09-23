"""The full-1.1.0-type synthetic data generator: coverage, determinism,
server-cap respect, and honest exclusions — all without a server."""

import io

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyhoglake.types import (
    MAX_COLUMN_NESTING_DEPTH,
    schema_to_column_defs,
)

from hoglake_bench import datagen
from hoglake_bench.datagen import (
    MAX_COLUMN_NODES,
    VARIANT_EXCLUSION_REASON,
    WRITABLE_SCALARS,
    deep_list_chain,
    full_coverage_columns,
    make_table,
    node_count,
    random_columns,
    to_arrow_schema,
)
from hoglake_bench.datagen.arrays import make_array

#: The server's closed scalar vocabulary (openapi/hoglake.yaml ColumnDef
#: `type` enum, containers and variant excluded). Restated here so a
#: silently shrunken generator matrix fails loudly.
SERVER_SCALARS = frozenset(
    {
        "boolean",
        "int8",
        "int16",
        "int",
        "long",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float",
        "double",
        "decimal",
        "date",
        "time",
        "timestamp_s",
        "timestamp_ms",
        "timestamp",
        "timestamp_ns",
        "timestamptz",
        "string",
        "json",
        "uuid",
        "binary",
    }
)


def _coltypes(defs):
    out = set()
    for d in defs:
        out.add(d["type"])
        out.update(_coltypes(d.get("children") or []))
    return out


class TestCoverageMatrix:
    def test_writable_scalars_are_every_server_scalar_but_variant(self):
        assert set(WRITABLE_SCALARS) == SERVER_SCALARS

    def test_variant_exclusion_is_stated_not_silent(self):
        assert "variant" not in WRITABLE_SCALARS
        assert "pyarrow" in VARIANT_EXCLUSION_REASON.lower() or (
            "parquet" in VARIANT_EXCLUSION_REASON.lower()
        )

    def test_full_coverage_spans_every_writable_scalar_and_container(self):
        defs = schema_to_column_defs(to_arrow_schema(full_coverage_columns()))
        types = _coltypes(defs)
        assert SERVER_SCALARS <= types
        assert {"list", "struct", "map"} <= types

    def test_nested_showcase_has_the_required_shapes(self):
        defs = schema_to_column_defs(to_arrow_schema(full_coverage_columns()))
        by_name = {d["name"]: d for d in defs}
        # a struct with many fields
        wide = next(
            d for d in defs if d["type"] == "struct" and len(d["children"]) >= 12
        )
        assert wide is not None
        # a list of structs
        assert any(
            d["type"] == "list" and d["children"][0]["type"] == "struct" for d in defs
        )
        # a struct holding a list
        assert any(
            d["type"] == "struct" and any(c["type"] == "list" for c in d["children"])
            for d in defs
        )
        # a map with a NON-string key
        assert any(
            d["type"] == "map" and d["children"][0]["type"] != "string" for d in defs
        ), f"no non-string map key in {sorted(by_name)}"


class TestServerCaps:
    def test_deep_chain_reaches_but_never_exceeds_the_cap(self):
        spec = deep_list_chain(MAX_COLUMN_NESTING_DEPTH)
        defs = schema_to_column_defs(to_arrow_schema([spec]))

        def depth(d):
            kids = d.get("children") or []
            return 1 + max(map(depth, kids), default=0)

        assert depth(defs[0]) == MAX_COLUMN_NESTING_DEPTH

    def test_deeper_than_the_cap_is_refused_locally(self):
        with pytest.raises(ValueError, match="depth"):
            deep_list_chain(MAX_COLUMN_NESTING_DEPTH + 1)

    def test_node_count_matches_the_server_counting_rule(self):
        # column + every descendant, synthetic children included:
        # map<string, struct<a,b>> = map(1) + key(1) + value struct(1)
        # + a(1) + b(1) = 5
        t = pa.map_(
            pa.string(),
            pa.struct([pa.field("a", pa.int64()), pa.field("b", pa.string())]),
        )
        assert node_count(t) == 5
        assert node_count(pa.int64()) == 1
        assert node_count(pa.list_(pa.int32())) == 2

    def test_random_columns_respect_depth_and_node_budget(self):
        rng = np.random.default_rng(7)
        specs = random_columns(rng, count=40, max_depth=5, max_nodes=200)
        schema = to_arrow_schema(specs)
        defs = schema_to_column_defs(schema)  # raises past the depth cap

        def depth(d):
            kids = d.get("children") or []
            return 1 + max(map(depth, kids), default=0)

        assert max(map(depth, defs)) <= 5
        assert sum(node_count(f.type) for f in schema) <= 200

    def test_default_budget_is_the_server_node_cap(self):
        assert MAX_COLUMN_NODES == 10_000


class TestDeterminism:
    @staticmethod
    def _parquet_bytes(seed):
        # Byte-level comparison (Table.equals is IEEE: the planted NaN
        # would make a table unequal to itself).
        t = make_table(np.random.default_rng(seed), full_coverage_columns(), 500)
        sink = io.BytesIO()
        pq.write_table(t, sink)
        return sink.getvalue()

    def test_same_seed_same_parquet_bytes(self):
        assert self._parquet_bytes(42) == self._parquet_bytes(42)

    def test_different_seed_different_table(self):
        assert self._parquet_bytes(42) != self._parquet_bytes(43)

    def test_random_columns_are_a_pure_function_of_the_seed(self):
        a = random_columns(np.random.default_rng(3), count=20)
        b = random_columns(np.random.default_rng(3), count=20)
        assert to_arrow_schema(a).equals(to_arrow_schema(b))


class TestDataShape:
    def test_generated_table_matches_its_schema_and_row_count(self):
        specs = full_coverage_columns()
        t = make_table(np.random.default_rng(1), specs, 256)
        t.validate(full=True)
        assert t.num_rows == 256
        assert t.schema.equals(to_arrow_schema(specs))

    def test_nullable_columns_actually_carry_nulls(self):
        specs = full_coverage_columns()
        t = make_table(np.random.default_rng(1), specs, 256)
        for spec in specs:
            col = t.column(spec.name)
            if spec.nullable and spec.null_rate > 0:
                assert col.null_count > 0, f"{spec.name} never null"
            if not spec.nullable:
                assert col.null_count == 0, f"{spec.name} has nulls"

    def test_uuid_generates_for_the_catalog_writer_type(self):
        """The lifecycle scenario fabricates values for the type
        coltype_to_arrow returns, which for uuid is the pa.uuid()
        extension (the only spelling pyarrow annotates UUID in parquet).
        Every pa.types.is_* predicate answers no to an extension type, so
        without its own arm the fabricator raised NotImplementedError on
        the first uuid column a scenario generated."""
        from pyhoglake import coltype_to_arrow

        dtype = coltype_to_arrow("uuid")
        values = make_array(np.random.default_rng(1), dtype, 8)
        assert values.type == dtype
        assert len(values) == 8
        # Same 16 bytes underneath either spelling.
        assert values.cast(pa.binary(16)).to_pylist() == [
            v.bytes for v in values.to_pylist()
        ]

    def test_uint64_exercises_the_range_beyond_int64(self):
        specs = full_coverage_columns()
        t = make_table(np.random.default_rng(1), specs, 64)
        uint64_cols = [s.name for s in specs if s.coltype == "uint64"]
        assert uint64_cols
        top = max(
            v
            for name in uint64_cols
            for v in t.column(name).to_pylist()
            if v is not None
        )
        assert top > 2**63 - 1

    def test_strings_stress_bounds(self):
        specs = full_coverage_columns()
        t = make_table(np.random.default_rng(1), specs, 256)
        string_cols = [s.name for s in specs if s.coltype == "string"]
        values = [
            v
            for name in string_cols
            for v in t.column(name).to_pylist()
            if v is not None
        ]
        # long values (multi-KB) and near-identical long-prefix pairs
        assert max(len(v) for v in values) >= 2000
        prefixed = [v for v in values if v.startswith(datagen.BOUND_PREFIX)]
        assert len(set(prefixed)) >= 2

    def test_the_whole_matrix_round_trips_through_parquet(self):
        specs = full_coverage_columns()
        t = make_table(np.random.default_rng(1), specs, 128)
        sink = io.BytesIO()
        pq.write_table(t, sink)
        back = pq.read_table(io.BytesIO(sink.getvalue()))
        assert back.num_rows == 128
