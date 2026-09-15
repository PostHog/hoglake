"""Deterministic read-path fixture for the duckdb-client sqllogictests.

Creates (idempotently) catalog `duckext-read` with namespace ns1 and
table `points`, then drops and recreates the table and appends two
batches (two parquet files). Row ids restart at 0 for the new table
incarnation, so the sqllogictests can assert exact values.

Run with pyhoglake's environment against the live dev stack:

    cd ~/src/hoglake/pyhoglake && uv run python \
        <path-to>/duckdb-client/test/fixtures/read_fixture.py

Environment (pyhoglake conventions): HOGLAKE_URL (default
http://localhost:8080), HOGLAKE_S3_ENDPOINT (default
http://localhost:19000), HOGLAKE_S3_ACCESS_KEY / _SECRET_KEY
(default hoglake / hoglake123).
"""

import os
import sys
from datetime import UTC, datetime, timedelta

import pyarrow as pa

from pyhoglake import AlreadyExistsError, HoglakeClient, NotFoundError, S3Config, ops

HOGLAKE_URL = os.environ.get("HOGLAKE_URL", "http://localhost:8080")
S3_ENDPOINT = os.environ.get("HOGLAKE_S3_ENDPOINT", "http://localhost:19000")
S3_ACCESS_KEY = os.environ.get("HOGLAKE_S3_ACCESS_KEY", "hoglake")
S3_SECRET_KEY = os.environ.get("HOGLAKE_S3_SECRET_KEY", "hoglake123")

CATALOG = "duckext-read"
BUCKET = "duckext-itest"
DATA_PATH = f"s3://{BUCKET}/read/"

SCHEMA = pa.schema(
    [
        pa.field("id", pa.int64(), nullable=False),
        pa.field("name", pa.string()),
        pa.field("val", pa.float64()),
        pa.field("ts", pa.timestamp("us")),
    ]
)

BATCH1 = pa.table(
    {
        "id": pa.array([1, 2, 3], pa.int64()),
        "name": ["alpha", "beta", None],
        "val": [1.5, -2.25, 42.0],
        "ts": [
            datetime(2026, 1, 1, 0, 0, 0),
            datetime(2026, 1, 2, 12, 30, 0),
            None,
        ],
    },
    schema=SCHEMA,
)

BATCH2 = pa.table(
    {
        "id": pa.array([4, 5], pa.int64()),
        "name": ["delta", "epsilon"],
        "val": [None, 0.125],
        "ts": [datetime(2026, 2, 1), datetime(2026, 2, 2)],
    },
    schema=SCHEMA,
)


def main() -> None:
    s3 = S3Config(
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY,
        endpoint_override=S3_ENDPOINT,
        region="us-east-1",
        allow_bucket_creation=True,
    )
    env_lines: list[str] = []
    with HoglakeClient(HOGLAKE_URL, s3=s3) as client:
        fs = s3.filesystem()
        fs.create_dir(BUCKET)
        try:
            catalog = client.catalog(CATALOG)
        except NotFoundError:
            catalog = client.create_catalog(CATALOG, DATA_PATH)
        try:
            ns = catalog.create_namespace("ns1")
        except AlreadyExistsError:
            ns = catalog.namespace("ns1")
        try:
            ns.table("points").drop()
        except NotFoundError:
            pass
        table = ns.create_table("points", SCHEMA)
        r1 = table.append(BATCH1)
        r2 = table.append(BATCH2)
        print(f"fixture ready: {CATALOG}/ns1.points "
              f"snapshots {r1.snapshot_id},{r2.snapshot_id}")

        # partitioned table: identity(team) -> one file per team value
        try:
            ns.table("part_points").drop()
        except NotFoundError:
            pass
        part_schema = pa.schema(
            [
                pa.field("team", pa.string()),
                pa.field("n", pa.int64(), nullable=False),
            ]
        )
        part = ns.create_table("part_points", part_schema)
        team_field = next(c for c in part.columns if c.name == "team")
        part.alter([
            ops.set_partition_spec([ops.partition_field(team_field.field_id, "identity")])
        ])
        rp = part.append(
            pa.table(
                {
                    "team": ["a", "a", "b", "b", "c", None],
                    "n": pa.array([1, 2, 3, 4, 5, 6], pa.int64()),
                },
                schema=part_schema,
            )
        )
        print(f"fixture ready: {CATALOG}/ns1.part_points snapshot {rp.snapshot_id} "
              f"files {len(rp.files)}")

        # schema-evolution history for time-travel tests: batch1 under
        # the 1-column schema (snapshot E1), then ADD COLUMN, then
        # batch2 under the 2-column schema
        try:
            ns.table("points_evo").drop()
        except NotFoundError:
            pass
        evo = ns.create_table(
            "points_evo", pa.schema([pa.field("id", pa.int64(), nullable=False)])
        )
        e1 = evo.append(pa.table({"id": pa.array([1, 2, 3], pa.int64())}))
        evo.alter([ops.add_column("v", pa.string())])
        e2 = evo.append(
            pa.table(
                {"id": pa.array([4], pa.int64()), "v": ["four"]},
                schema=pa.schema(
                    [pa.field("id", pa.int64(), nullable=False), pa.field("v", pa.string())]
                ),
            )
        )
        print(f"fixture ready: {CATALOG}/ns1.points_evo snapshots {e1.snapshot_id},{e2.snapshot_id}")

        # cross-client partition-grouping fixtures: pyhoglake writes one
        # file per key value; the extension's sqllogictests insert the
        # SAME logical values, and verify_partition_wire.py asserts both
        # clients' files land in identical partition string groups
        xclient = {
            "xclient_int": (pa.int64(), pa.array([7, 42], pa.int64())),
            "xclient_date": (
                pa.date32(),
                pa.array([datetime(2026, 3, 1).date(), datetime(2026, 3, 2).date()], pa.date32()),
            ),
            "xclient_ts": (
                pa.timestamp("us"),
                pa.array(
                    [datetime(2026, 1, 2, 3, 4, 5, 900000), datetime(2026, 1, 2, 3, 4, 5)],
                    pa.timestamp("us"),
                ),
            ),
            "xclient_bool": (pa.bool_(), pa.array([True, False], pa.bool_())),
        }
        for tname, (ktype, kvals) in xclient.items():
            try:
                ns.table(tname).drop()
            except NotFoundError:
                pass
            t = ns.create_table(
                tname,
                pa.schema([pa.field("k", ktype), pa.field("src", pa.string())]),
            )
            k_field = next(c for c in t.columns if c.name == "k")
            t.alter([ops.set_partition_spec([ops.partition_field(k_field.field_id, "identity")])])
            t.append(
                pa.table(
                    {"k": kvals, "src": ["py"] * len(kvals)},
                    schema=pa.schema([pa.field("k", ktype), pa.field("src", pa.string())]),
                )
            )
            print(f"fixture ready: {CATALOG}/ns1.{tname}")

        # binary identity partition (pyhoglake-only: the extension
        # refuses binary partition WRITES; its pruning must still read
        # these correctly — the wire value is base64)
        try:
            ns.table("part_bin").drop()
        except NotFoundError:
            pass
        pb = ns.create_table(
            "part_bin", pa.schema([pa.field("k", pa.binary()), pa.field("n", pa.int64())])
        )
        k_field = next(c for c in pb.columns if c.name == "k")
        pb.alter([ops.set_partition_spec([ops.partition_field(k_field.field_id, "identity")])])
        pb.append(
            pa.table(
                {"k": pa.array([b"hello", b"\x00\x01"], pa.binary()), "n": pa.array([1, 2], pa.int64())},
                schema=pa.schema([pa.field("k", pa.binary()), pa.field("n", pa.int64())]),
            )
        )
        print(f"fixture ready: {CATALOG}/ns1.part_bin")

        # case-colliding pairs the EXTENSION refuses to create but other
        # clients legally can: a table pair (ambiguity errors + listings
        # must survive) and a namespace pair. Idempotent; namespaces
        # cannot be dropped on the wire, so the pair persists.
        for tname in ("ambig_t", "Ambig_T"):
            try:
                ns.table(tname).drop()
            except NotFoundError:
                pass
            ns.create_table(tname, pa.schema([pa.field("x", pa.int64())]))
        print(f"fixture ready: {CATALOG}/ns1.ambig_t + Ambig_T (CI-colliding pair)")
        for nsname in ("ambigns", "AmbigNs"):
            try:
                catalog.create_namespace(nsname)
            except AlreadyExistsError:
                pass
        print(f"fixture ready: {CATALOG} namespaces ambigns + AmbigNs (CI-colliding pair)")

        # ---- poison tables (round-3 hardening): metadata OTHER clients
        # can legally register but DuckDB cannot represent. Listings
        # must skip them with targeted errors; the instance must NEVER
        # be invalidated. Created via raw REST because pyhoglake/pyarrow
        # refuse some of these shapes.
        import httpx

        def rest(method, path, **kw):
            r = httpx.request(method, HOGLAKE_URL.rstrip("/") + "/v1" + path, **kw)
            if r.status_code not in (200, 201, 404, 409):
                raise RuntimeError(f"{method} {path}: {r.status_code} {r.text[:200]}")
            return r

        # decimal precision far beyond DuckDB's 38
        rest("DELETE", f"/catalogs/{CATALOG}/namespaces/ns1/tables/bad_dec100")
        rest("POST", f"/catalogs/{CATALOG}/namespaces/ns1/tables", json={
            "name": "bad_dec100",
            "columns": [{"name": "d", "type": "decimal", "type_params": {"precision": 100, "scale": 2}}],
        })
        # decimal with no type_params at all
        rest("DELETE", f"/catalogs/{CATALOG}/namespaces/ns1/tables/bad_dec_nop")
        rest("POST", f"/catalogs/{CATALOG}/namespaces/ns1/tables", json={
            "name": "bad_dec_nop",
            "columns": [{"name": "d", "type": "decimal"}],
        })
        # case-colliding COLUMN names (server dedupe is exact-match)
        rest("DELETE", f"/catalogs/{CATALOG}/namespaces/ns1/tables/bad_cols")
        rest("POST", f"/catalogs/{CATALOG}/namespaces/ns1/tables", json={
            "name": "bad_cols",
            "columns": [{"name": "team", "type": "string"}, {"name": "Team", "type": "int"}],
        })
        # decimal params OUTSIDE the parse bound range (negative): this
        # throws from the parse layer, not the named-table belt — the
        # containment must hold for it too (R4-5/R4-9)
        rest("DELETE", f"/catalogs/{CATALOG}/namespaces/ns1/tables/bad_dec_neg")
        rest("POST", f"/catalogs/{CATALOG}/namespaces/ns1/tables", json={
            "name": "bad_dec_neg",
            "columns": [{"name": "d", "type": "decimal", "type_params": {"precision": -1, "scale": 0}}],
        })
        print(f"fixture ready: {CATALOG}/ns1 poison tables bad_dec100, bad_dec_nop, bad_dec_neg, bad_cols")

        # ---- duplicate-path registration (in duckext-sqltest, where
        # the DML tests run): one physical parquet registered as TWO
        # live logical files — legal on the wire (writer retries).
        SQLTEST = "duckext-sqltest"
        r = rest("GET", f"/catalogs/{SQLTEST}")
        if r.status_code == 404:
            rest("POST", "/catalogs", json={"name": SQLTEST, "data_path": "s3://duckext-itest/sqltest/"})
            rest("POST", f"/catalogs/{SQLTEST}/namespaces", json={"name": "ns1"})
        sq = client.catalog(SQLTEST)
        try:
            sq_ns = sq.create_namespace("ns1")
        except AlreadyExistsError:
            sq_ns = sq.namespace("ns1")
        try:
            sq_ns.table("dup_path").drop()
        except NotFoundError:
            pass
        dup = sq_ns.create_table("dup_path", pa.schema([pa.field("a", pa.int64())]))
        dup.append(pa.table({"a": pa.array([1, 2, 3], pa.int64())}))
        f = dup.files()[0]
        rest("POST", f"/catalogs/{SQLTEST}/commit", json={
            "appends": [{
                "namespace": "ns1", "table": "dup_path",
                "files": [{"path": f.path, "record_count": f.record_count,
                            "file_size_bytes": f.file_size_bytes}],
            }],
        })
        print(f"fixture ready: {SQLTEST}/ns1.dup_path (one path, two live registrations)")

        # duplicate registration with an INCONSISTENT record_count (the
        # server validates only >= 0, never against the parquet): DML
        # touching the physical rows beyond the under-declared count
        # must refuse with a typed error, never invalidate the
        # instance (R4-1)
        try:
            sq_ns.table("dup_incon").drop()
        except NotFoundError:
            pass
        dupi = sq_ns.create_table("dup_incon", pa.schema([pa.field("a", pa.int64())]))
        dupi.append(pa.table({"a": pa.array([1, 2, 3], pa.int64())}))
        fi = dupi.files()[0]
        rest("POST", f"/catalogs/{SQLTEST}/commit", json={
            "appends": [{
                "namespace": "ns1", "table": "dup_incon",
                "files": [{"path": fi.path, "record_count": 2,
                            "file_size_bytes": fi.file_size_bytes}],
            }],
        })
        print(f"fixture ready: {SQLTEST}/ns1.dup_incon (duplicate registration declaring 2 of 3 rows)")

        # ---- reserved-field-id poison (R5-2): an ordinary in-contract
        # registration whose parquet carries the RESERVED field id
        # 2147483646 on a normal column. The server never opens
        # registered parquet (and /verify excludes the field-id
        # contract), so explicit_row_ids stays false — the client must
        # refuse the file rather than read its contents as row ids. The
        # column also holds a NULL, the R5-1 trigger.
        import io as _io
        import time as _time
        import uuid as _uuid
        import pyarrow.parquet as _pq

        try:
            sq_ns.table("bad_fieldid").drop()
        except NotFoundError:
            pass
        fid_tbl = sq_ns.create_table(
            "bad_fieldid",
            pa.schema([pa.field("a", pa.int64()), pa.field("junk", pa.int64())]),
        )
        fid_cols = {c.name: c for c in fid_tbl.columns}
        poison_schema = pa.schema([
            pa.field("a", pa.int64(),
                     metadata={b"PARQUET:field_id": str(fid_cols["a"].field_id).encode()}),
            # the reserved id, on an ordinary column
            pa.field("junk", pa.int64(), metadata={b"PARQUET:field_id": b"2147483646"}),
        ])
        sink = _io.BytesIO()
        _pq.write_table(
            pa.table({"a": pa.array([1, 2, 3], pa.int64()),
                      "junk": pa.array([10, None, 30], pa.int64())}, schema=poison_schema),
            sink,
        )
        raw = sink.getvalue()
        key = f"duckext-itest/sqltest/data/ns1/bad_fieldid/{_uuid.uuid4()}.parquet"
        with s3.filesystem().open_output_stream(key) as out:
            out.write(raw)
        rest("POST", f"/catalogs/{SQLTEST}/commit", json={
            "appends": [{
                "namespace": "ns1", "table": "bad_fieldid",
                "files": [{"path": f"s3://{key}", "record_count": 3,
                            "file_size_bytes": len(raw)}],
            }],
        })
        print(f"fixture ready: {SQLTEST}/ns1.bad_fieldid (reserved field id on a positional file)")

        # ---- flag-true / column-missing (R6-2): the complementary
        # direction of the reserved-field-id contract. Only compaction
        # sets explicit_row_ids, so the fixture lets the server compact
        # a table and then replaces the OUTPUT OBJECT in the bucket
        # with a parquet that carries field ids but no _hog_row_id —
        # what an object-store-level corruption or a compaction bug
        # looks like. Gated: exports DUCKEXT_FID_MISSING only if the
        # swap succeeded.
        try:
            sq_ns.table("fid_missing").drop()
        except NotFoundError:
            pass
        fm_schema = pa.schema([pa.field("id", pa.int64(), nullable=False)])
        fm = sq_ns.create_table("fid_missing", fm_schema)
        for i in range(8):
            fm.append(pa.table({"id": pa.array([i + 1], pa.int64())}, schema=fm_schema))
        fm_deadline = _time.time() + 120
        fm_out = None
        while _time.time() < fm_deadline:
            files = rest("GET", f"/catalogs/{SQLTEST}/namespaces/ns1/tables/fid_missing/scan").json()
            explicit = [f["data_file"] for f in files if f["data_file"].get("explicit_row_ids")]
            if explicit:
                fm_out = explicit[0]
                break
            if all(f["data_file"]["stats_state"] == "provided" for f in files):
                rest("POST", f"/catalogs/{SQLTEST}/maintenance/compact?batch=10")
            _time.sleep(2)
        if fm_out is None:
            print("WARNING: fid_missing did not compact; flag-true/column-missing test will skip")
        else:
            # Byte-patch the compaction output's FOOTER so the reserved
            # field id 2147483646 becomes 2147483645: thrift compact
            # encodes both as 5-byte zigzag varints, so file size and
            # footer size are unchanged (the reader validates both
            # against the registration) while no column carries the
            # reserved id any more — exactly flag-true/column-missing.
            swap_key = fm_out["path"][len("s3://"):]
            fs_handle = s3.filesystem()
            with fs_handle.open_input_stream(swap_key) as src:
                original = src.read()
            footer_len = int.from_bytes(original[-8:-4], "little")
            footer_start = len(original) - footer_len - 8
            reserved = (2147483646 << 1).to_bytes(5, "little")  # placeholder, replaced below

            def zigzag_varint(value: int) -> bytes:
                zz = (value << 1) ^ (value >> 31)
                out = bytearray()
                while True:
                    byte = zz & 0x7F
                    zz >>= 7
                    if zz:
                        out.append(byte | 0x80)
                    else:
                        out.append(byte)
                        break
                return bytes(out)

            reserved = zigzag_varint(2147483646)
            replacement = zigzag_varint(2147483645)
            footer = original[footer_start:]
            if len(reserved) != len(replacement) or reserved not in footer:
                print("WARNING: could not patch the reserved field id in the compaction footer; "
                      "flag-true/column-missing test will skip")
            else:
                patched = original[:footer_start] + footer.replace(reserved, replacement, 1)
                assert len(patched) == len(original)
                with fs_handle.open_output_stream(swap_key) as out:
                    out.write(patched)
                env_lines.append("export DUCKEXT_FID_MISSING=1")
                print(f"fixture ready: {SQLTEST}/ns1.fid_missing "
                      f"(explicit_row_ids output whose parquet lacks the reserved field id)")

        # sorted table for the DISCRIMINATING compacted-rowid test
        # (R4-10): compaction sorts the merge by the live sort order, so
        # the compacted file's _hog_row_id column is PERMUTED relative
        # to physical position — a positional-fallback regression reads
        # different rowids
        try:
            ns.table("points_sorted").drop()
        except NotFoundError:
            pass
        srt = ns.create_table(
            "points_sorted",
            pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("v", pa.int64())]),
        )
        v_field = next(c for c in srt.columns if c.name == "v")
        rest("POST", f"/catalogs/{CATALOG}/namespaces/ns1/tables/points_sorted/alter", json={
            "ops": [{"op": "set_sort_order",
                      "sort_fields": [{"source_field_id": v_field.field_id,
                                        "direction": "asc", "null_order": "nulls_last"}]}],
        })
        # 8 single-row batches (8 files: the tiered planner promotes a
        # group only when its aggregate reaches the next tier floor —
        # tier ratio 8 — and caps groups at 8 inputs) with a DESCENDING
        # sort key, so the sorted merge fully REVERSES row order:
        # explicit _hog_row_id = [7,6,...,0] by physical position,
        # maximally discriminating against positional fallback
        srt_schema = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("v", pa.int64())])
        for i in range(8):
            srt.append(pa.table({"id": pa.array([i + 1], pa.int64()),
                                 "v": pa.array([80 - 10 * i], pa.int64())}, schema=srt_schema))
        print(f"fixture ready: {CATALOG}/ns1.points_sorted (8 unsorted batches, sort order v asc)")

        # time-travel env for the sqllogictests: genuinely historical
        # snapshot ids and a timestamp BETWEEN the two points batches
        # (proves timestamp resolution picks the earlier snapshot).
        # points batch2 (r2) has a strictly later snapshot_time than
        # batch1 (r1); a timestamp equal to r1's snapshot_time resolves
        # to r1 per the wire contract (largest snapshot_time <= t).
        snap_times = {}
        for snap in catalog.snapshots(after=max(0, r1.snapshot_id - 1)):
            snap_times[snap.snapshot_id] = snap.snapshot_time
            if snap.snapshot_id >= r2.snapshot_id:
                break
        t1 = snap_times[r1.snapshot_id]
        # DuckDB-natural local format (no zone, space separator): the
        # attach path must normalize it to an ISO instant itself
        t1_natural = t1.strftime("%Y-%m-%d %H:%M:%S.%f")
        # the same instant as T1 expressed at +02:00 (offset handling:
        # a client that drops the offset reads head instead of batch1)
        t1_plus2 = (t1 + timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S.%f") + "+02:00"
        env_lines += [
            f"export DUCKEXT_POINTS_SNAP_V1={r1.snapshot_id}",
            f"export DUCKEXT_POINTS_SNAP_V2={r2.snapshot_id}",
            f"export DUCKEXT_POINTS_T1='{t1_natural}'",
            f"export DUCKEXT_POINTS_T1_PLUS2='{t1_plus2}'",
            f"export DUCKEXT_EVO_SNAP_V1={e1.snapshot_id}",
        ]
        # force-compact points so the suite can deterministically read a
        # compaction output (explicit _hog_row_id file). Needs the
        # stack's hydrator (stats must be provided before files become
        # compaction candidates); on timeout the flag is left unset and
        # the gated test file skips.
        def table_files(tname):
            return rest("GET", f"/catalogs/{CATALOG}/namespaces/ns1/tables/{tname}/scan").json()

        def is_compacted(tname, files):
            if tname == "points_sorted":
                # the tiered planner may stall above one file; ANY
                # explicit (sorted-merge) output already carries the
                # position-permuted _hog_row_id the discriminating test
                # needs
                return any(f["data_file"].get("explicit_row_ids") for f in files)
            return len(files) == 1 and files[0]["data_file"].get("explicit_row_ids")

        compact_targets = ["points", "points_sorted"]
        compacted = {t: False for t in compact_targets}
        deadline = _time.time() + 120
        while _time.time() < deadline and not all(compacted.values()):
            pending = False
            for t in compact_targets:
                files = table_files(t)
                if is_compacted(t, files):
                    compacted[t] = True
                elif not all(f["data_file"]["stats_state"] == "provided" for f in files):
                    pending = True
            if all(compacted.values()):
                break
            if not pending:
                rest("POST", f"/catalogs/{CATALOG}/maintenance/compact?batch=10")
            _time.sleep(2)
        if compacted["points"]:
            env_lines.append("export DUCKEXT_POINTS_COMPACTED=1")
            print("points compacted (explicit _hog_row_id file); gated test enabled")
        else:
            print("WARNING: points did not compact within 120s (hydrator down?); compacted-read test will skip")
        if compacted["points_sorted"]:
            env_lines.append("export DUCKEXT_SORTED_COMPACTED=1")
            print("points_sorted compacted (sorted merge => permuted _hog_row_id); discriminating test enabled")
        else:
            print("WARNING: points_sorted did not compact within 120s; discriminating rowid test will skip")

        env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "live-env.sh")
        with open(env_path, "w") as f:
            f.write("\n".join(env_lines) + "\n")
        print(f"wrote {env_path}")
        for line in env_lines:
            print("  " + line)


if __name__ == "__main__":
    main()
