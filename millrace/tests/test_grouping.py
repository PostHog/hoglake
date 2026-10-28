"""The partition-tuple keyer (millrace/grouping.py), pure.

Pinned here:

- hoglake's field display names (the server's ``PartitionFieldLabels``
  rule): the source's dotted path for identity, ``<path>_<transform>``
  otherwise, ``field_<id>`` for an unresolvable source;
- construction refuses a spec it cannot key (unknown source, a
  container source, a source under a list or map, an unsupported
  column type, a transform the type cannot take);
- PARITY with the flush: for every scalar column type and transform the
  keyer's tuple is string-for-string the tuple the flush's fanout
  (``flush._partition_groups`` over the decoded Arrow batch) files the
  same payload under — the property that makes a steady-state flush
  exactly one file — and the same for struct-leaf sources (read by path
  on both sides; a null or absent struct is a null component).
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from pyhoglake import TableInfo, ValidationError

from millrace import flush
from millrace.coerce import DecodeError, load_json_object
from millrace.grouping import GroupKeyer, GroupKeyError, field_names

UUID = "0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f012"

#: One column per scalar type the JSON coercers support (STRUCT_LEAVES
#: below repeats the table one level down).
SCALARS: list[dict[str, Any]] = [
    {"name": "a", "type": "long", "field_id": 1, "ordinal": 0},
    {"name": "ts", "type": "timestamp", "field_id": 2, "ordinal": 1},
    {"name": "tz", "type": "timestamptz", "field_id": 3, "ordinal": 2},
    {"name": "d", "type": "date", "field_id": 4, "ordinal": 3},
    {"name": "f", "type": "float", "field_id": 5, "ordinal": 4},
    {
        "name": "dec",
        "type": "decimal",
        "field_id": 6,
        "ordinal": 5,
        "type_params": {"precision": 10, "scale": 2},
    },
    {"name": "s", "type": "string", "field_id": 7, "ordinal": 6},
    {"name": "ns", "type": "timestamp_ns", "field_id": 8, "ordinal": 7},
    {"name": "tm", "type": "time", "field_id": 9, "ordinal": 8},
    {"name": "u", "type": "uuid", "field_id": 10, "ordinal": 9},
    {"name": "tms", "type": "timestamp_s", "field_id": 11, "ordinal": 10},
    {"name": "b", "type": "boolean", "field_id": 12, "ordinal": 11},
    {"name": "i", "type": "int", "field_id": 13, "ordinal": 12},
    {"name": "dbl", "type": "double", "field_id": 14, "ordinal": 13},
    {"name": "bin", "type": "binary", "field_id": 15, "ordinal": 14},
    {"name": "tmsec", "type": "timestamp_ms", "field_id": 16, "ordinal": 15},
]


def info(
    spec: list[tuple[int, str, int | None]] | None,
    columns: list[dict[str, Any]] | None = None,
) -> TableInfo:
    wire: dict[str, Any] = {
        "name": "t",
        "namespace": "n",
        "table_uuid": UUID,
        "columns": SCALARS if columns is None else columns,
        "read_snapshot_id": 3,
    }
    if spec is not None:
        wire["partition_spec"] = {
            "spec_id": 1,
            "fields": [
                {
                    "source_field_id": fid,
                    "transform": transform,
                    **({"transform_param": param} if param is not None else {}),
                }
                for fid, transform, param in spec
            ],
        }
    return TableInfo.from_wire(wire)


NESTED: list[dict[str, Any]] = [
    {"name": "team_id", "type": "long", "field_id": 1, "ordinal": 0},
    {
        "name": "properties",
        "type": "struct",
        "field_id": 2,
        "ordinal": 1,
        "children": [
            {"name": "team_id", "type": "long", "field_id": 3, "ordinal": 0},
            {
                "name": "geo",
                "type": "struct",
                "field_id": 4,
                "ordinal": 1,
                "children": [
                    {"name": "zip", "type": "string", "field_id": 5, "ordinal": 0}
                ],
            },
        ],
    },
    {
        "name": "tags",
        "type": "list",
        "field_id": 6,
        "ordinal": 2,
        "children": [
            {"name": "element", "type": "string", "field_id": 7, "ordinal": 0}
        ],
    },
]


def test_field_names_follow_hoglakes_display_rule():
    assert field_names(info(None)) == ()
    assert field_names(
        info([(1, "identity", None), (2, "month", None), (7, "bucket", 16)])
    ) == ("a", "ts_month", "s_bucket")
    assert field_names(info([(5, "identity", None)], NESTED)) == ("properties.geo.zip",)
    assert field_names(info([(3, "truncate", 4)], NESTED)) == (
        "properties.team_id_truncate",
    )
    assert field_names(info([(99, "day", None)])) == ("field_99_day",)


def test_a_deeply_nested_struct_leaf_is_keyed_by_its_path():
    keyer = GroupKeyer(info([(5, "identity", None)], NESTED))
    assert keyer.group_for({"properties": {"geo": {"zip": "94110"}}}) == ("94110",)
    assert keyer.group_for({"properties": {"geo": None}}) == (None,)
    assert keyer.group_for({}) == (None,)


@pytest.mark.parametrize(
    ("spec", "columns", "match"),
    [
        ([(99, "identity", None)], None, "field_id 99"),
        ([(2, "identity", None)], NESTED, "container"),
        ([(7, "identity", None)], NESTED, "sits under 'tags'"),
        ([(12, "month", None)], None, "cannot be keyed"),  # month(boolean)
        ([(1, "bucket", None)], None, "cannot be keyed"),  # bucket without N
        ([(1, "frobnicate", None)], None, "cannot be keyed"),
        # bucket/truncate type gates: refused at BUILD, never per record
        (
            [(8, "bucket", 7)],
            None,
            "bucket cannot be applied to column type 'timestamp_ns'",
        ),
        ([(11, "bucket", 7)], None, "bucket cannot be applied"),  # timestamp_s
        ([(12, "bucket", 7)], None, "bucket cannot be applied"),  # boolean
        ([(5, "bucket", 7)], None, "bucket cannot be applied"),  # float
        (
            [(2, "truncate", 3)],
            None,
            "truncate cannot be applied to column type 'timestamp'",
        ),
        ([(10, "truncate", 3)], None, "truncate cannot be applied"),  # uuid
        ([(4, "truncate", 3)], None, "truncate cannot be applied"),  # date
        ([(7, "truncate", None)], None, "cannot be keyed"),  # truncate without W
    ],
)
def test_construction_refuses_a_spec_it_cannot_key(spec, columns, match):
    with pytest.raises(GroupKeyError, match=match):
        GroupKeyer(info(spec, columns))


@pytest.mark.parametrize("col_type", ["json", "variant"])
def test_a_json_or_variant_source_is_refused_as_unkeyable(col_type):
    """Both by type, with the same refusal. The decoder takes a
    top-level variant column now; its coercer's refusal (a different
    message, about the payload-key mapping) is no keying rule."""
    columns = [{"name": "j", "type": col_type, "field_id": 1, "ordinal": 0}]
    with pytest.raises(
        GroupKeyError,
        match=f"'j' cannot be keyed: a '{col_type}' column is not a partition source",
    ):
        GroupKeyer(info([(1, "identity", None)], columns))


PARITY_SPECS: list[list[tuple[int, str, int | None]]] = [
    [(1, "identity", None), (2, "month", None)],
    [(2, "identity", None), (3, "identity", None), (4, "identity", None)],
    [(3, "day", None), (3, "hour", None), (4, "year", None), (2, "year", None)],
    [(5, "identity", None), (6, "identity", None), (7, "truncate", 3)],
    [(8, "identity", None), (8, "day", None), (9, "identity", None)],
    [(10, "identity", None), (10, "bucket", 7), (6, "bucket", 5), (6, "truncate", 50)],
    [(1, "bucket", 16), (7, "bucket", 4), (11, "identity", None), (11, "month", None)],
    [(12, "identity", None), (13, "truncate", 10), (14, "identity", None)],
    [
        (15, "identity", None),
        (15, "bucket", 3),
        (16, "identity", None),
        (16, "hour", None),
    ],
]

PARITY_DOCS: list[dict[str, Any]] = [
    {
        "a": 5,
        "ts": 1_700_000_000_123_456,
        "tz": "2026-10-12T03:04:05.5+02:00",
        "d": "2026-01-31",
        "f": 0.1,
        "dec": "1.5",
        "s": "héllo/=x",
        "ns": 1_700_000_000_123_456_789,
        "tm": "12:34:56.25",
        "u": "12345678-1234-5678-1234-567812345678",
        "tms": 1_700_000_000,
        "b": True,
        "i": -17,
        "dbl": 2.5e-7,
        "bin": "AAEC/w==",
        "tmsec": 1_700_000_000_123,
    },
    {
        "a": -3,
        "ts": "1969-12-31T23:59:59",
        "tz": 1,
        "d": -1,
        "f": 3.0e38,
        "dec": 7,
        "s": "ab",
        "ns": -1,
        "tm": 0,
        "u": "00000000-0000-0000-0000-000000000001",
        "tms": -5,
        "b": False,
        "i": 0,
        "dbl": -0.0,
        "bin": "",
        "tmsec": -1,
    },
    {"a": None},  # every source null (or absent)
    {"dec": "-0"},  # negative zero: Arrow's unscaled 0 renders 0.00
    {"dec": "-0.0", "f": -0.0, "i": -0},
    # Outside years 1-9999: clamped identically on both sides, never an
    # OverflowError in the keyer's transforms.
    {
        "ts": 10**20,
        "tz": "10000-01-01T00:00:00",
        "tms": -(10**15),
        "tmsec": 10**20,
        "ns": 10**25,
    },
    {"ts": "0000-06-01T00:00:00", "tz": -(10**20), "tms": 10**15, "ns": -(10**25)},
]


@pytest.mark.parametrize("spec", PARITY_SPECS)
@pytest.mark.parametrize("doc", PARITY_DOCS)
def test_the_keyers_tuple_is_the_flush_fanouts_tuple(spec, doc):
    table = info(spec)
    keyer = GroupKeyer(table)
    raw = json.dumps(doc).encode()
    group = keyer.group_for(load_json_object(raw))

    record = flush.JsonObjectDecoder(table.columns).decode_record(raw)
    _, encoded = flush._build_batch([record], table.columns)
    assert table.partition_spec is not None
    ((fanout_tuple, _),) = flush._partition_groups(encoded, table, table.partition_spec)
    assert group == fanout_tuple


#: Every transform the matrix tries on every scalar column.
TRANSFORMS: list[tuple[str, int | None]] = [
    ("identity", None),
    ("bucket", 7),
    ("truncate", 3),
    ("year", None),
    ("month", None),
    ("day", None),
    ("hour", None),
]
MATRIX = [
    (column["field_id"], column["type"], transform, param)
    for column in SCALARS
    for transform, param in TRANSFORMS
]


@pytest.mark.parametrize(("field_id", "col_type", "transform", "param"), MATRIX)
def test_the_full_type_transform_matrix_keys_like_the_fanout_or_refuses_at_build(
    field_id, col_type, transform, param
):
    """The whole scalar type × transform matrix (ns year/month/hour, tz
    year/month, bucket/truncate on every type …): where the keyer
    builds, its tuple equals the fanout's for every parity payload;
    where it refuses at build, the flush's own array path refuses the
    same pair on a real value too — the gate is exactly pyhoglake's,
    never stricter."""
    table = info([(field_id, transform, param)])
    try:
        keyer = GroupKeyer(table)
    except GroupKeyError:
        record = flush.JsonObjectDecoder(table.columns).decode_record(
            json.dumps(PARITY_DOCS[0]).encode()
        )
        _, encoded = flush._build_batch([record], table.columns)
        assert table.partition_spec is not None
        with pytest.raises(ValidationError):
            flush._partition_groups(encoded, table, table.partition_spec)
        return
    for doc in PARITY_DOCS:
        raw = json.dumps(doc).encode()
        record = flush.JsonObjectDecoder(table.columns).decode_record(raw)
        _, encoded = flush._build_batch([record], table.columns)
        assert table.partition_spec is not None
        ((fanout_tuple, _),) = flush._partition_groups(
            encoded, table, table.partition_spec
        )
        assert keyer.group_for(load_json_object(raw)) == fanout_tuple, (col_type, doc)


def test_a_null_non_nullable_source_is_a_null_component_and_the_flush_drops():
    """The keyer keys a missing value of a NON-nullable source as a null
    component (grouping is not validation); the flush's decoder then
    refuses the record, so no file is ever written under that tuple."""
    columns = [
        {"name": "a", "type": "long", "field_id": 1, "ordinal": 0, "nullable": False}
    ]
    table = info([(1, "identity", None)], columns)
    raw = b'{"other": 1}'
    assert GroupKeyer(table).group_for(load_json_object(raw)) == (None,)
    with pytest.raises(DecodeError, match="not nullable"):
        flush.JsonObjectDecoder(table.columns).decode_record(raw)


#: The scalar table again, every column moved one level down into a
#: ``props`` struct (field ids shifted by 100), beside the original
#: top-level ``a``.
STRUCT_LEAVES: list[dict[str, Any]] = [
    {"name": "a", "type": "long", "field_id": 1, "ordinal": 0},
    {
        "name": "props",
        "type": "struct",
        "field_id": 100,
        "ordinal": 1,
        "children": [{**c, "field_id": c["field_id"] + 100} for c in SCALARS],
    },
]

STRUCT_PARITY_SPECS: list[list[tuple[int, str, int | None]]] = [
    [(101, "identity", None)],
    [(1, "identity", None), (102, "month", None), (103, "day", None)],
    [(104, "year", None), (106, "truncate", 3), (107, "bucket", 4)],
    [(108, "hour", None), (110, "bucket", 7), (111, "identity", None)],
    [(105, "identity", None), (113, "truncate", 10), (115, "identity", None)],
]


@pytest.mark.parametrize("spec", STRUCT_PARITY_SPECS)
@pytest.mark.parametrize(
    "doc",
    [{"a": 1, "props": d} for d in PARITY_DOCS]
    + [{"a": 2, "props": None}, {"a": 3}, {"props": {}}],
)
def test_a_struct_leaf_sources_tuple_is_the_flush_fanouts_tuple(spec, doc):
    """The parity property for struct-leaf sources: the keyer reads the
    leaf by path from the payload, the fanout by ``struct_field`` from the
    decoded Arrow batch; a null or absent struct is a null component on
    both sides."""
    table = info(spec, STRUCT_LEAVES)
    keyer = GroupKeyer(table)
    raw = json.dumps(doc).encode()
    group = keyer.group_for(load_json_object(raw))

    record = flush.JsonObjectDecoder(table.columns).decode_record(raw)
    _, encoded = flush._build_batch([record], table.columns)
    assert table.partition_spec is not None
    ((fanout_tuple, _),) = flush._partition_groups(encoded, table, table.partition_spec)
    assert group == fanout_tuple


@pytest.mark.parametrize(
    ("ts", "month", "day"),
    [
        # Year 10000 as epoch micros: clamped to 9999-12-31 (month index
        # (9999-1970)*12+11, day index of 9999-12-31).
        (253_402_300_800_000_000, "96359", "2932896"),
        # Year 0 as epoch micros: clamped to 0001-01-01.
        (-62_167_219_200_000_000, "-23628", "-719162"),
    ],
)
def test_an_out_of_range_partition_source_is_staged_under_the_clamped_tuple(
    ts: int, month: str, day: str
) -> None:
    from millrace.consumer import classify_message
    from millrace.stage import StagedRecord

    cols = [{"name": "ts", "type": "timestamptz", "field_id": 1, "ordinal": 0}]
    table = info([(1, "month", None), (1, "day", None)], cols)
    payload = json.dumps({"ts": ts}).encode()
    outcome = classify_message(GroupKeyer(table), offset=3, value=payload)
    assert isinstance(outcome, StagedRecord)
    assert outcome.group == (month, day)
    # The flush files it under the same tuple.
    record = flush.JsonObjectDecoder(table.columns).decode_record(payload)
    _, encoded = flush._build_batch([record], table.columns)
    assert table.partition_spec is not None
    ((fanout_tuple, _),) = flush._partition_groups(encoded, table, table.partition_spec)
    assert fanout_tuple == (month, day)
