"""The shared object-prefix layout vectors.

``tests/vectors/object_prefix_vectors.json`` is what the server's path
helper is held to (``ObjectPrefix.kt``, checked by
``ObjectPrefixVectorsTest`` and ``ObjectPrefixTest``), and what
``pyhoglake.object_prefix``, the writer's twin of it, is held to here:
every prefix case, every stats case and every IAM value (containment
and the IAM foreign keys are the server's alone: a writer never tests
whether a path is under a prefix). The file itself is pinned too: it is
exactly what the generator writes, and the count is pinned on both
sides.
"""

import enum
import importlib.util
import json
import re
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pytest
from test_variant_ddl import _IDENTIFIERS_REL, _kotlin

from pyhoglake import TableInfo, object_prefix
from pyhoglake.models import Column, ColumnStats, PartitionField

VECTORS = Path(__file__).parent / "vectors"
PATH = VECTORS / "object_prefix_vectors.json"
DOC = json.loads(PATH.read_text(encoding="utf-8"))

#: Pinned exactly, so a vector lost to a bad merge fails instead of
#: shrinking coverage in silence. Keep in step with
#: ObjectPrefixVectorFile.EXPECTED_COUNT on the Kotlin side.
EXPECTED_VECTOR_COUNT = 69


def _generator():
    spec = importlib.util.spec_from_file_location(
        "regen_object_prefix_vectors", VECTORS / "regen_object_prefix_vectors.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_vector_file_is_what_the_generator_writes():
    on_disk = json.loads(PATH.read_text(encoding="utf-8"))
    assert on_disk == json.loads(json.dumps(_generator().build()))


def test_vector_file_header_and_count():
    doc = json.loads(PATH.read_text(encoding="utf-8"))
    assert doc["format"] == "hoglake-object-prefix-vectors"
    assert doc["version"] == 1
    assert _generator().vector_count(doc) == EXPECTED_VECTOR_COUNT


def _info(
    col_type: str = "long",
    *,
    namespace: str = "ns",
    table: str = "t",
    field: str = "f",
    field_id: int = 7,
) -> TableInfo:
    """A table whose object-prefix field is its one column."""
    return TableInfo(
        name=table,
        namespace=namespace,
        table_uuid="u",
        columns=(Column(field, col_type, field_id, 0),),
        record_count=None,
        file_count=None,
        file_size_bytes=None,
        object_prefix_field=field,
    )


def _case_info(case) -> TableInfo:
    return _info(
        case["col_type"],
        namespace=case["namespace"],
        table=case["table"],
        field=case["field"],
    )


@pytest.mark.parametrize("case", DOC["prefix_cases"])
def test_every_prefix_case_builds_its_prefix_or_refuses_the_value(case):
    """A partition value is read with parse_value, and the prefix is built
    from the int it names: the vector's string comes back canonical."""
    parsed = object_prefix.parse_value(case["value"], case["col_type"])
    if "error" in case:
        assert parsed is None
    else:
        assert str(parsed) == case["value"]
        built = object_prefix.prefix(case["data_path"], _case_info(case), parsed)
        assert built == case["expected"]


@pytest.mark.parametrize("value", ["4\n", "-0", "00"])
def test_a_value_the_vectors_leave_out_is_refused_too(value):
    assert object_prefix.parse_value(value, "long") is None


def test_every_iam_value_is_its_prefix_as_a_resource():
    iam = DOC["iam"]
    tag = _generator().TAG
    for value in iam["values"]:
        number = object_prefix.parse_value(value, iam["col_type"])
        assert number is not None
        built = object_prefix.prefix(iam["data_path"], _case_info(iam), number)
        resource = "arn:aws:s3:::" + built.removeprefix("s3://") + "*"
        assert resource == iam["resource_pattern"].replace(tag, value)


def _stats(lower: bytes | None, upper: bytes | None, nulls: int = 0, field_id=7):
    return ColumnStats(field_id, 1, nulls, lower_bound=lower, upper_bound=upper)


def _bound(hex_or_none: str | None) -> bytes | None:
    # An empty hex string is an empty bound, b"", never None or "".
    return None if hex_or_none is None else bytes.fromhex(hex_or_none)


def test_an_empty_hex_bound_is_empty_bytes():
    assert _bound("") == b""
    assert _bound(None) is None


@pytest.mark.parametrize("case", DOC["stats_cases"])
def test_every_stats_case_gets_its_verdict(case):
    s = case["stats"]
    stats = s and _stats(
        _bound(s["lower_hex"]), _bound(s["upper_hex"]), s["null_count"]
    )
    verdict = object_prefix.check_field_stats(
        _info(case["col_type"]), case["value"], stats
    )
    assert verdict == case["verdict"]


def test_stats_prove_nothing_for_another_field_or_a_value_out_of_range():
    int8 = _info("int8")
    four = object_prefix.expected_bound(int8, 4)
    assert four == b"\x04\x00\x00\x00"
    check = object_prefix.check_field_stats
    assert check(int8, 4, _stats(four, four, field_id=8)) == "MISSING_STATS"
    wrapped = (128).to_bytes(4, "little")
    assert check(int8, 128, _stats(wrapped, wrapped)) == "BOUND_MISMATCH"
    assert check(int8, -1, _stats(four, four)) == "BOUND_MISMATCH"
    for value in (128, -1):
        with pytest.raises(ValueError, match="outside the prefix range of int8"):
            object_prefix.expected_bound(int8, value)
        with pytest.raises(ValueError, match="outside the prefix range of int8"):
            object_prefix.prefix("s3://b/lake", int8, value)


@pytest.mark.parametrize(
    "col_type", ["int8", "int16", "int", "uint8", "uint16", "uint32"]
)
def test_every_type_refuses_one_past_its_largest_value(col_type):
    info, top = _info(col_type), object_prefix.MAX_VALUE[col_type]
    assert object_prefix.prefix("s3://b/lake", info, top).endswith(f"/f={top}/")
    with pytest.raises(ValueError, match="outside"):
        object_prefix.prefix("s3://b/lake", info, top + 1)


def test_the_field_and_its_type_come_from_the_table_info():
    """The server checks against the live column: an int field promoted to
    long wants 8-byte bounds, and a writer gets them from the read its
    read_snapshot comes from, not from a type it holds apart."""
    before, after = _info("int"), _info("long")
    small = object_prefix.expected_bound(before, 4)
    wide = object_prefix.expected_bound(after, 4)
    assert (len(small), len(wide)) == (4, 8)
    check = object_prefix.check_field_stats
    assert check(before, 4, _stats(small, small)) == "OK"
    assert check(after, 4, _stats(small, small)) == "BOUND_MISMATCH"
    assert check(after, 4, _stats(wide, wide)) == "OK"
    # The field id is the column's too.
    moved = _info(field_id=9)
    assert check(moved, 4, _stats(wide, wide)) == "MISSING_STATS"
    assert check(moved, 4, _stats(wide, wide, field_id=9)) == "OK"


@pytest.mark.parametrize(
    "info,match",
    [
        (replace(_info(), object_prefix_field=None), "has no object-prefix field"),
        (replace(_info(), object_prefix_field="g"), "'g' is no column"),
        (_info("uint64"), "'uint64' cannot be an object-prefix field"),
        (_info("string"), "'string' cannot be an object-prefix field"),
    ],
    ids=["option-off", "no-such-column", "uint64", "string"],
)
def test_a_table_no_prefix_can_be_built_for_is_refused_before_the_value(info, match):
    """The type is checked first: uint64 with a good value names the type,
    not the value, and so does it with a bad one."""
    for value in (4, 2**64, "4"):
        with pytest.raises(ValueError, match=match):
            object_prefix.prefix("s3://b/lake", info, value)
        with pytest.raises(ValueError, match=match):
            object_prefix.expected_bound(info, value)
        with pytest.raises(ValueError, match=match):
            object_prefix.check_field_stats(info, value, None)


@pytest.mark.parametrize("value", [4.0, True, False, "4", None])
def test_a_value_that_is_not_an_int_is_refused(value):
    """The server's value is a Long: neither 4.0 nor True is one."""
    with pytest.raises(TypeError, match="is an int"):
        object_prefix.prefix("s3://b/lake", _info(), value)
    with pytest.raises(TypeError, match="is an int"):
        object_prefix.expected_bound(_info(), value)
    with pytest.raises(TypeError, match="is an int"):
        object_prefix.check_field_stats(_info(), value, None)


class _NumpyLikeInt:
    """What numpy.int64 offers (numpy is not a dependency): __index__,
    and no int base class."""

    def __index__(self) -> int:
        return 4


class _Team(enum.IntEnum):
    FOUR = 4


@pytest.mark.parametrize(
    "value",
    [_NumpyLikeInt(), pa.scalar(4, pa.int64()), pa.scalar(4, pa.uint8()), _Team.FOUR],
    ids=["numpy-like", "arrow-int64", "arrow-uint8", "int-enum"],
)
def test_any_integer_is_a_value_and_its_path_and_stats_agree(value):
    """An integer from a pandas or Arrow column, or an IntEnum, is the int
    it holds: the path writes it as 4, never as `_Team.FOUR`."""
    info = _info()
    assert object_prefix.prefix("s3://b/lake", info, value).endswith("/f=4/")
    bound = object_prefix.expected_bound(info, value)
    assert bound == object_prefix.expected_bound(info, 4)
    assert object_prefix.check_field_stats(info, value, _stats(bound, bound)) == "OK"


def test_the_allowed_types_cannot_be_widened():
    with pytest.raises(TypeError):
        object_prefix.MAX_VALUE["uint64"] = 2**64 - 1


def test_verdicts_come_in_the_servers_order():
    """An all-null column: no bounds before nulls. Nulls before bounds."""
    check, info = object_prefix.check_field_stats, _info()
    assert check(info, 4, _stats(None, None, nulls=1)) == "NULL_BOUNDS"
    wrong = object_prefix.expected_bound(info, 42)
    assert check(info, 4, _stats(wrong, wrong, nulls=1)) == "NULLS_PRESENT"


@pytest.mark.parametrize("name", ["", "9ns", "a/b", "a=b", "ns\n", "x" * 129])
def test_a_prefix_refuses_a_name_outside_the_catalog_grammar(name):
    for info in (
        _info(namespace=name),
        _info(table=name),
        _info(field=name),
    ):
        with pytest.raises(ValueError, match="cannot be a name"):
            object_prefix.prefix("s3://b/lake", info, 4)


def test_the_name_grammar_is_the_servers():
    found = re.search(r'const val PATTERN = "\^(.*)\$"', _kotlin(_IDENTIFIERS_REL))
    assert found, "Identifiers.PATTERN not found"
    assert object_prefix._NAME.pattern == found.group(1)


def test_key_index_is_the_one_identity_on_the_field():
    team, day = PartitionField(1, "identity"), PartitionField(2, "day")
    assert object_prefix.key_index((day, team), 1) == 1
    assert object_prefix.key_index((day,), 1) is None
    assert object_prefix.key_index((PartitionField(1, "bucket", 8),), 1) is None
    assert object_prefix.key_index((team, team), 1) is None


def test_table_info_reads_the_object_prefix_field_and_its_absence():
    wire = {"name": "events", "namespace": "ingest", "table_uuid": "u"}
    assert TableInfo.from_wire(wire).object_prefix_field is None
    on = TableInfo.from_wire({**wire, "object_prefix_field": "team_id"})
    assert on.object_prefix_field == "team_id"
