"""The seeded warehouse's typed tables: the telemetry pair extends the
star schema so a seeded catalog exercises the FULL 1.1.0 type system —
every writable scalar on device_metrics, nested list/struct/map (with a
non-string-key map) on app_events."""

import numpy as np
import pytest
from pyhoglake.types import schema_to_column_defs

from hoglake_bench.datagen import WRITABLE_SCALARS
from hoglake_bench.seed import tables as T
from hoglake_bench.seed import typed_tables as TT
from hoglake_bench.seed.budget import WAREHOUSE_SHAPE, plan_budget


def _coltypes(defs):
    out = set()
    for d in defs:
        out.add(d["type"])
        out.update(_coltypes(d.get("children") or []))
    return out


class TestWarehouseShape:
    def test_telemetry_tables_are_in_the_shape(self):
        keys = {s.key for s in WAREHOUSE_SHAPE}
        assert "telemetry.device_metrics" in keys
        assert "telemetry.app_events" in keys

    def test_shares_still_sum_to_one(self):
        assert sum(s.share for s in WAREHOUSE_SHAPE) == pytest.approx(1.0)

    def test_telemetry_gets_a_real_budget(self):
        budgets = plan_budget(1.0).budgets
        assert budgets["telemetry.device_metrics"] > 0
        assert budgets["telemetry.app_events"] > 0


class TestDeviceMetrics:
    def test_covers_every_writable_scalar(self):
        types = _coltypes(schema_to_column_defs(TT.DEVICE_METRICS_SCHEMA))
        assert set(WRITABLE_SCALARS) <= types

    def test_fabricator_matches_schema_and_ids_continue(self):
        rng = np.random.default_rng(9)
        batch = TT.device_metrics(rng, 100, id_offset=40)
        assert batch.schema.equals(TT.DEVICE_METRICS_SCHEMA)
        assert batch.num_rows == 100
        assert batch.column("reading_id").to_pylist() == list(range(41, 141))
        # uint64 upper-range stress reaches the seeded warehouse too
        top = max(v for v in batch.column("total_bytes").to_pylist() if v is not None)
        assert top > 2**63 - 1

    def test_deterministic_under_seed(self):
        a = TT.device_metrics(np.random.default_rng(4), 64, id_offset=0)
        b = TT.device_metrics(np.random.default_rng(4), 64, id_offset=0)
        # Byte-level: datagen plants NaN in float columns, and arrow's
        # Table.equals is IEEE (NaN != NaN), so compare serialized bytes.
        import io

        import pyarrow.parquet as pq

        sa, sb = io.BytesIO(), io.BytesIO()
        pq.write_table(a, sa)
        pq.write_table(b, sb)
        assert sa.getvalue() == sb.getvalue()


class TestAppEvents:
    def test_covers_the_container_kinds(self):
        defs = schema_to_column_defs(TT.APP_EVENTS_SCHEMA)
        types = _coltypes(defs)
        assert {"list", "struct", "map"} <= types
        # at least one map keyed by a non-string type
        assert any(
            d["type"] == "map" and d["children"][0]["type"] != "string" for d in defs
        )

    def test_fabricator_matches_schema(self):
        rng = np.random.default_rng(9)
        lo, hi = T.history_window(TT.ANCHOR_FOR_TESTS)
        batch = TT.app_events(rng, 200, team_ids=T.TEAM_IDS, lo_us=lo, hi_us=hi)
        assert batch.schema.equals(TT.APP_EVENTS_SCHEMA)
        assert batch.num_rows == 200
        assert batch.column("team_id").null_count == 0
        assert batch.column("ts").null_count == 0
        # nested nullable columns actually carry nulls
        assert batch.column("properties").null_count > 0
