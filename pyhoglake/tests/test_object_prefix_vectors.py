"""The shared object-prefix layout vectors.

``tests/vectors/object_prefix_vectors.json`` is what the server's path
helper is held to (``ObjectPrefix.kt``, checked by
``ObjectPrefixVectorsTest`` and ``ObjectPrefixTest``), and what
``pyhoglake.object_prefix``, the writer's twin of it, is held to here:
every prefix case and every stats case (containment is the server's
alone). The file itself is pinned too: it is exactly what the generator
writes, and the count is pinned on both sides.
"""

import importlib.util
import json
import re
from pathlib import Path

import pytest
from test_variant_ddl import _IDENTIFIERS_REL, _kotlin

from pyhoglake import TableInfo, object_prefix
from pyhoglake.models import ColumnStats, PartitionField

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


@pytest.mark.parametrize("case", DOC["prefix_cases"])
def test_every_prefix_case_builds_its_prefix_or_refuses_the_value(case):
    value, col_type = case["value"], case["col_type"]
    names = (case["data_path"], case["namespace"], case["table"], case["field"])
    parsed = object_prefix.parse_value(value, col_type)
    if "error" in case:
        assert parsed is None
        with pytest.raises(ValueError, match="canonical"):
            object_prefix.prefix(*names, col_type, value)
    else:
        assert str(parsed) == value
        assert object_prefix.prefix(*names, col_type, value) == case["expected"]


@pytest.mark.parametrize("value", ["4\n", "-0", "00"])
def test_a_value_the_vectors_leave_out_is_refused_too(value):
    assert object_prefix.parse_value(value, "long") is None
    with pytest.raises(ValueError, match="canonical"):
        object_prefix.prefix("s3://b/lake", "ns", "t", "f", "long", value)


def test_every_iam_value_is_its_prefix_as_a_resource():
    iam = DOC["iam"]
    names = (iam["data_path"], iam["namespace"], iam["table"], iam["field"])
    tag = _generator().TAG
    for value in iam["values"]:
        built = object_prefix.prefix(*names, iam["col_type"], value)
        resource = "arn:aws:s3:::" + built.removeprefix("s3://") + "*"
        assert resource == iam["resource_pattern"].replace(tag, value)


def _stats(lower: bytes | None, upper: bytes | None, nulls: int = 0, field_id=7):
    return ColumnStats(field_id, 1, nulls, lower_bound=lower, upper_bound=upper)


@pytest.mark.parametrize("case", DOC["stats_cases"])
def test_every_stats_case_gets_its_verdict(case):
    s = case["stats"]
    stats = s and _stats(
        *(s[k] and bytes.fromhex(s[k]) for k in ("lower_hex", "upper_hex")),
        s["null_count"],
    )
    verdict = object_prefix.check_field_stats(7, case["col_type"], case["value"], stats)
    assert verdict == case["verdict"]


def test_stats_prove_nothing_for_another_field_or_a_value_out_of_range():
    four = object_prefix.expected_bound("int8", 4)
    assert four == b"\x04\x00\x00\x00"
    check = object_prefix.check_field_stats
    assert check(7, "int8", 4, _stats(four, four, field_id=8)) == "MISSING_STATS"
    wrapped = (128).to_bytes(4, "little")
    assert check(7, "int8", 128, _stats(wrapped, wrapped)) == "BOUND_MISMATCH"
    assert check(7, "int8", -1, _stats(four, four)) == "BOUND_MISMATCH"
    for col_type, value in (("int8", 128), ("uint64", 4)):
        with pytest.raises(ValueError):
            object_prefix.expected_bound(col_type, value)


@pytest.mark.parametrize("value", [4.0, True, "4"])
def test_a_value_that_is_not_an_int_is_refused(value):
    """The server's value is a Long: neither 4.0 nor True is one."""
    with pytest.raises(TypeError, match="is an int"):
        object_prefix.expected_bound("long", value)
    with pytest.raises(TypeError, match="is an int"):
        object_prefix.check_field_stats(7, "long", value, None)


def test_the_allowed_types_cannot_be_widened():
    with pytest.raises(TypeError):
        object_prefix.MAX_VALUE["uint64"] = 2**64 - 1


def test_verdicts_come_in_the_servers_order():
    """An all-null column: no bounds before nulls. Nulls before bounds."""
    check = object_prefix.check_field_stats
    assert check(7, "long", 4, _stats(None, None, nulls=1)) == "NULL_BOUNDS"
    wrong = object_prefix.expected_bound("long", 42)
    assert check(7, "long", 4, _stats(wrong, wrong, nulls=1)) == "NULLS_PRESENT"


@pytest.mark.parametrize("name", ["", "9ns", "a/b", "a=b", "ns\n", "x" * 129])
def test_a_prefix_refuses_a_name_outside_the_catalog_grammar(name):
    for names in ((name, "t", "f"), ("ns", name, "f"), ("ns", "t", name)):
        with pytest.raises(ValueError, match="cannot be a name"):
            object_prefix.prefix("s3://b/lake", *names, "long", "4")


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
