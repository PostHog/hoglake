"""The real-parquet registration helper: scenarios that register many
files per commit (too many for one Table.append per file) still get REAL
parquet bytes and stats that come from the written footer, never invented
numbers."""

import io
import struct

import pyarrow as pa
import pyarrow.parquet as pq
from pyhoglake.bounds import encode_bound
from pyhoglake.models import Column

from hoglake_bench.realfiles import build_real_registration

COLUMNS = (
    Column(name="id", type="long", field_id=1, ordinal=0, nullable=False),
    Column(name="v", type="double", field_id=2, ordinal=1),
)


def _data(ids, vs):
    return pa.table(
        {
            "id": pa.array(ids, pa.int64()),
            "v": pa.array(vs, pa.float64()),
        }
    )


class TestBuildRealRegistration:
    def test_bytes_are_real_parquet_holding_the_rows(self):
        reg, payload = build_real_registration(
            _data([1, 2, 3], [0.5, 1.5, 2.5]),
            COLUMNS,
            data_path="s3://bucket/run/cat/",
            namespace="bench",
            table="real",
        )
        table = pq.read_table(io.BytesIO(payload))
        assert table.num_rows == 3
        assert table.column("id").to_pylist() == [1, 2, 3]
        assert reg["record_count"] == 3
        assert reg["file_size_bytes"] == len(payload)

    def test_footer_size_matches_the_trailer(self):
        reg, payload = build_real_registration(
            _data([1], [1.0]),
            COLUMNS,
            data_path="s3://bucket/run/cat/",
            namespace="bench",
            table="real",
        )
        assert payload[-4:] == b"PAR1"
        assert reg["footer_size"] == struct.unpack("<I", payload[-8:-4])[0]

    def test_stats_come_from_the_footer_not_from_thin_air(self):
        reg, _payload = build_real_registration(
            _data([7, 42, 13], [2.25, -1.5, 0.0]),
            COLUMNS,
            data_path="s3://bucket/run/cat/",
            namespace="bench",
            table="real",
        )
        stats = {s["field_id"]: s for s in reg["column_stats"]}
        assert set(stats) == {1, 2}
        assert stats[1]["value_count"] == 3
        assert stats[1]["null_count"] == 0
        # Bounds are the Iceberg single-value serialization of the REAL
        # min/max of the written data (base64 on the wire).
        import base64

        assert base64.b64decode(stats[1]["lower_bound"]) == encode_bound("long", 7)
        assert base64.b64decode(stats[1]["upper_bound"]) == encode_bound("long", 42)
        assert base64.b64decode(stats[2]["lower_bound"]) == encode_bound("double", -1.5)
        assert base64.b64decode(stats[2]["upper_bound"]) == encode_bound("double", 2.25)

    def test_field_ids_ride_the_parquet_schema(self):
        _, payload = build_real_registration(
            _data([1], [1.0]),
            COLUMNS,
            data_path="s3://bucket/run/cat/",
            namespace="bench",
            table="real",
        )
        schema = pq.ParquetFile(io.BytesIO(payload)).schema_arrow
        assert schema.field("id").metadata[b"PARQUET:field_id"] == b"1"
        assert schema.field("v").metadata[b"PARQUET:field_id"] == b"2"

    def test_paths_are_unique_and_under_the_table_prefix(self):
        regs = [
            build_real_registration(
                _data([1], [1.0]),
                COLUMNS,
                data_path="s3://bucket/run/cat/",
                namespace="bench",
                table="real",
            )[0]
            for _ in range(3)
        ]
        paths = {r["path"] for r in regs}
        assert len(paths) == 3
        for p in paths:
            assert p.startswith("s3://bucket/run/cat/data/bench/real/")
            assert p.endswith(".parquet")
