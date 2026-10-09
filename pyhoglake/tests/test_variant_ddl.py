"""Declaring VARIANT columns: the shredding grammar, the marker, the DDL wire.

Three things are pinned here.

- The grammar parity. ``validate_shredding`` mirrors the server's
  ``VariantShredding.kt``, and both read
  ``tests/vectors/variant_shredding_vectors.json``: every declaration in
  it is accepted or refused as the file says, with the server's message
  word for word (a case collision marked ``may_accept``, which the client
  leaves to the server, may pass), and the Kotlin
  ``VariantShreddingVectorFileTest`` checks the same file against the real
  validator. The count is pinned on both sides. The limits and
  vocabularies are PARSED out of the Kotlin source rather than restated
  (AGENT.md: a test that restates the constant it mirrors asserts only
  that the file compiles).
- The DDL wire: the exact JSON ``create_table`` sends for a schema with
  declared and undeclared ``variant_field`` columns, and ``add_column``
  with ``shredding=``.
- The local refusals: a declaration on a non-variant column or below the
  top level, a marker this module did not write, and an Arrow struct
  shaped like VARIANT storage, which would otherwise become a plain
  ``struct`` column.
"""

import json
import re
from collections import OrderedDict
from pathlib import Path
from types import MappingProxyType

import pyarrow as pa
import pytest

from pyhoglake import (
    HoglakeClient,
    UnsupportedTypeError,
    ValidationError,
    ops,
    variant,
    variant_field,
)
from pyhoglake.client import Namespace
from pyhoglake.types import arrow_type_to_coltype, schema_to_column_defs
from pyhoglake.variant import (
    VARIANT_FIELD_KEY,
    json_unshreddable_paths,
    validate_shredding,
)

BASE = "http://hog.test"

VECTOR_PATH = Path(__file__).parent / "vectors" / "variant_shredding_vectors.json"

with VECTOR_PATH.open(encoding="utf-8") as f:
    DOC = json.load(f)

VECTORS = DOC["vectors"]

#: Pinned exactly, so a vector lost to a bad merge fails instead of
#: shrinking coverage in silence. Keep in step with
#: VariantShreddingVectorFile.EXPECTED_COUNT on the Kotlin side.
EXPECTED_VECTOR_COUNT = 205

#: The problem of a case collision, the one refusal the client may leave
#: to the server (validate_shredding).
_CASE_PROBLEM = "has fields that differ only by case: "

DECLARATION = {
    "type": "object",
    "fields": [
        {"name": "$browser", "type": "string"},
        {"name": "price", "type": "decimal8", "precision": 18, "scale": 2},
        {"name": "tags", "type": "array", "element": {"type": "string"}},
    ],
}


# -- the shared vector file ---------------------------------------------------


def test_vector_file_header_contract():
    assert DOC["format"] == "hoglake-variant-shredding-vectors"
    assert DOC["version"] == 1
    assert DOC["column"] == "v"
    assert len(VECTORS) == EXPECTED_VECTOR_COUNT
    ids = [v["id"] for v in VECTORS]
    assert len(ids) == len(set(ids))
    for v in VECTORS:
        assert {"id", "source", "declaration", "expect"} <= set(v)
        assert v["expect"] in ("accept", "refuse")
        if v["expect"] == "refuse":
            assert v["path"].startswith("$")
            assert v["problem"]
        # The one client-side marking: a refusal the client may leave to
        # the server (test_vector). Only a case collision qualifies, and
        # only one with a name that is not ASCII, whose case every Python
        # decides as the server does: the marking switches off the
        # client's half of the check, so it is no general escape hatch.
        assert v.get("client", "may_accept") == "may_accept"
        if "client" in v:
            assert v["expect"] == "refuse", v["id"]
            assert v["problem"].startswith(_CASE_PROBLEM), v["id"]
            assert not v["problem"].isascii(), v["id"]


def test_vector_file_seeds_every_request_seed_of_the_fuzz_corpus():
    """The request_* seeds are in the file verbatim, so a seed #323 or a
    later change adds is a vector both languages decide, not a fuzz input
    only the server replays. VariantShreddingVectorFileTest checks the
    same on the server's side, so a server change that adds a seed reds
    its own CI rather than this suite's next run."""
    seeds = _server_file(
        Path(
            "server/src/test/resources/com/posthog/hoglake/fuzz/"
            "VariantShreddingFuzzTestInputs/declarationsFollowTheDocumentedRules"
        )
    )
    by_id = {v["id"]: v for v in VECTORS}
    names = sorted(p.name for p in seeds.iterdir() if p.name.startswith("request_"))
    assert (
        "request_nul_name" in names and "request_fields_and_arrays_over_limit" in names
    )
    for name in names:
        vector = by_id.get(f"fuzz_{name}")
        assert vector is not None, f"seed {name} has no vector fuzz_{name}"
        # The seed is the declaration and one trailing byte that picks the
        # target's arbitrary mode.
        raw = (seeds / name).read_bytes()
        assert vector["declaration"] == json.loads(raw[:-1]), name


def _left_to_the_server(vector) -> bool:
    """Whether the client may accept a refusal of the file and leave it to
    the server: a case collision marked may_accept, or, on a Python whose
    Unicode data is newer than the server's (CPython 3.15 and Unicode 17),
    any case collision that is not ASCII. Read off the vector and the
    versions, not off validate_shredding's own predicate, which this is
    the check of."""
    if vector.get("client") == "may_accept":
        return True
    return (
        variant._LOCAL_UNICODE > variant._SERVER_UNICODE
        and vector["problem"].startswith(_CASE_PROBLEM)
        and not vector["problem"].isascii()
    )


@pytest.mark.parametrize("unicode", ["this-python", "newer-than-the-server"])
@pytest.mark.parametrize("vector", VECTORS, ids=[v["id"] for v in VECTORS])
def test_vector(vector, unicode, monkeypatch):
    """Every vector on this Python's Unicode data and, so that the suite
    holds on the interpreter that first moves past the server's, on data
    newer than the server's too."""
    if unicode != "this-python":
        newer = (variant._SERVER_UNICODE[0] + 1, 0)
        monkeypatch.setattr(variant, "_LOCAL_UNICODE", newer)
    declaration = vector["declaration"]
    if vector["expect"] == "accept":
        validate_shredding(declaration, column=DOC["column"])
        validate_shredding(declaration)
        return
    tail = f"invalid type_params.shredding: {vector['path']} {vector['problem']}"
    if _left_to_the_server(vector):
        # A case pair the client leaves to the server (validate_shredding):
        # it may pass here, but a refusal here is still the server's.
        try:
            validate_shredding(declaration, column=DOC["column"])
        except ValidationError as e:
            assert e.message == f"variant column '{DOC['column']}' has an {tail}"
        return
    with pytest.raises(ValidationError) as ei:
        validate_shredding(declaration, column=DOC["column"])
    assert ei.value.message == f"variant column '{DOC['column']}' has an {tail}"
    assert ei.value.status_code is None
    # Without a column the message is the same refusal, unattributed.
    with pytest.raises(ValidationError) as ei:
        validate_shredding(declaration)
    assert ei.value.message == tail


# -- parity with the server's source -------------------------------------------

_SHREDDING_REL = Path(
    "server/src/main/kotlin/com/posthog/hoglake/service/VariantShredding.kt"
)
_IDENTIFIERS_REL = Path(
    "server/src/main/kotlin/com/posthog/hoglake/service/Identifiers.kt"
)


def _server_file(relative: Path) -> Path:
    """Locate a server file by walking up from this test. Failing, not
    skipping, when missing: a skip reads as green."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / relative
        if candidate.exists():
            return candidate
    raise AssertionError(
        f"cannot find {relative} above {here}; this test compares the client's "
        "shredding rules against the server's"
    )


def _kotlin(relative: Path = _SHREDDING_REL) -> str:
    return _server_file(relative).read_text()


@pytest.mark.parametrize(
    "constant,mirror",
    [
        ("MAX_DEPTH", variant.MAX_SHREDDING_DEPTH),
        ("MAX_FIELDS", variant.MAX_SHREDDING_FIELDS),
        ("MAX_PATH_NAME_BYTES", variant.MAX_PATH_NAME_BYTES),
    ],
)
def test_limits_match_the_server(constant, mirror):
    m = re.search(rf"const val {constant} = (\d+)", _kotlin())
    assert m, f"{constant} not found in VariantShredding.kt"
    assert mirror == int(m.group(1))


def test_type_vocabulary_matches_the_server():
    src = _kotlin()
    block = re.search(r"PRIMITIVE_TYPES =\s*setOf\((.*?)\)", src, re.DOTALL)
    assert block, "PRIMITIVE_TYPES not found in VariantShredding.kt"
    assert set(re.findall(r'"(\w+)"', block.group(1))) == variant.PRIMITIVE_TYPES
    decimals = re.search(r"DECIMAL_TYPES = mapOf\((.*?)\)\n", src, re.DOTALL)
    assert decimals, "DECIMAL_TYPES not found in VariantShredding.kt"
    pairs = re.findall(r'"(\w+)" to (\d+)', decimals.group(1))
    assert {k: int(v) for k, v in pairs} == variant.DECIMAL_TYPES
    assert re.search(r'const val KEY = "(\w+)"', src).group(1) == variant.SHREDDING_KEY


def test_message_cap_matches_the_server():
    m = re.search(
        r"text\.length > (\d+)\) text\.take\((\d+)\) \+ \"\.\.\.\"",
        _kotlin(_IDENTIFIERS_REL),
    )
    assert m, "Identifiers.cap not found"
    assert (int(m.group(1)), int(m.group(2))) == (
        variant._CAP_UNITS,
        variant._CAP_UNITS - 3,
    )


_COLUMN_TREES_REL = Path(
    "server/src/main/kotlin/com/posthog/hoglake/service/ColumnTrees.kt"
)


def _joined(relative: Path) -> str:
    """A server file's source with its split string literals joined:
    Kotlin splits long ones with `" +` and a line break."""
    return re.sub(r'"\s*\+\s*\n\s*"', "", _kotlin(relative))


def _container_refusal(column: str, wire: str) -> str:
    """ColumnTrees' refusal of type_params on a container, parsed out of
    its source and filled in, so the expectation is the server's text
    and not a copy of it."""
    m = re.search(
        r"\"(column '\$qualified' is '\$\{def\.type\.wire\}', a nested container[^\"]*)\"",
        _joined(_COLUMN_TREES_REL),
    )
    assert m, "the container type_params refusal not found in ColumnTrees.kt"
    return m.group(1).replace("$qualified", column).replace("${def.type.wire}", wire)


def test_column_refusals_are_the_servers_words():
    """The three column-level refusals the client raises locally are the
    server's templates, so the local error and the 422 read the same:
    VariantShredding's nested and notVariant, which variant.py and ops.py
    raise, and ColumnTrees' container refusal, which ops.py raises."""
    src = _joined(_SHREDDING_REL)
    assert (
        "variant column '$column' is nested, and only a top-level variant column "
        "can declare type_params.shredding"
    ) in src
    assert (
        "column '$column' is '$type', and only a variant column can declare "
        "type_params.shredding"
    ) in src
    assert _container_refusal("p", "list") == (
        "column 'p' is 'list', a nested container, and cannot have type_params: "
        "a container's shape is its children, not its parameters"
    )


#: The Unicode version of each JDK's character data, from its release
#: notes. Unicode only grows with the JDK, so a JDK newer than the last
#: entry has at least that entry's.
_JDK_UNICODE = {21: (15, 0), 22: (15, 1), 23: (15, 1), 24: (16, 0), 25: (16, 0)}


def test_the_servers_unicode_is_the_one_the_client_assumes():
    """variant._SERVER_UNICODE must be no newer than the Unicode of the JDK
    the server builds with and runs on. Were it newer, a case pair this
    Python knows and that JDK does not would be refused here and accepted
    there. It is also no older than the toolchain's: an older one would
    leave every pair that is not ASCII to the server on a Python that
    shares the server's data, and test_vector reads it for its
    expectations, so only this test would notice."""
    build = re.search(
        r"jvmToolchain\((\d+)\)", _kotlin(Path("server/build.gradle.kts"))
    )
    assert build, "jvmToolchain not found in server/build.gradle.kts"
    images = re.findall(r"eclipse-temurin:(\d+)-", _kotlin(Path("server/Dockerfile")))
    assert images, "no eclipse-temurin image in server/Dockerfile"
    newest = max(_JDK_UNICODE)

    def unicode(jdk: int) -> tuple[int, int]:
        assert jdk >= min(_JDK_UNICODE), f"JDK {jdk} is older than any known here"
        return _JDK_UNICODE.get(jdk, _JDK_UNICODE[newest])

    for jdk in {int(build.group(1)), *map(int, images)}:
        assert variant._SERVER_UNICODE <= unicode(jdk), jdk
    assert variant._SERVER_UNICODE == unicode(int(build.group(1)))


def _two_fields(a: str, b: str) -> dict:
    return {
        "type": "object",
        "fields": [{"name": a, "type": "string"}, {"name": b, "type": "string"}],
    }


def test_a_case_pair_is_left_to_a_server_on_older_unicode(monkeypatch):
    """When this Python's Unicode data is newer than the server's, only an
    ASCII pair is certain to collide there too. An exact duplicate is one
    whatever the data."""
    # The same data (Python 3.14 against JDK 25's 16.0) decides as here.
    monkeypatch.setattr(variant, "_LOCAL_UNICODE", variant._SERVER_UNICODE)
    with pytest.raises(ValidationError, match="differ only by case"):
        validate_shredding(_two_fields("\u00c4", "\u00e4"))
    monkeypatch.setattr(variant, "_LOCAL_UNICODE", (variant._SERVER_UNICODE[0] + 1, 0))
    validate_shredding(_two_fields("\u00c4", "\u00e4"))
    with pytest.raises(ValidationError) as ei:
        validate_shredding(_two_fields("A", "a"))
    assert ei.value.message.endswith("differ only by case: 'A' and 'a'")
    with pytest.raises(ValidationError) as ei:
        validate_shredding(_two_fields("\u00c4", "\u00c4"))
    assert ei.value.message.endswith("has duplicate field '\u00c4'")
    # A duplicate behind a pair left to the server is not the name the
    # lowercase map holds for it (the capital is), so only the names as
    # written catch it: a server without the pair says this, one with it
    # refuses at the first small letter already.
    behind = {
        "type": "object",
        "fields": [
            {"name": n, "type": "string"} for n in ("\u00c4", "\u00e4", "\u00e4")
        ],
    }
    with pytest.raises(ValidationError) as ei:
        validate_shredding(behind)
    assert ei.value.message.endswith("has duplicate field '\u00e4'")
    # The first name of a lowercase is the one a later name meets, as in
    # the server's map: KELVIN SIGN after k is left to the server, and a K
    # after both still meets k, an ASCII pair, which the server refuses
    # too (at the KELVIN SIGN already).
    kelvin = {
        "type": "object",
        "fields": [{"name": n, "type": "string"} for n in ("k", "\u212a", "K")],
    }
    with pytest.raises(ValidationError) as ei:
        validate_shredding(kelvin)
    assert ei.value.message.endswith("differ only by case: 'k' and 'K'")


# -- the declaration as the client takes it ------------------------------------


def test_none_declares_nothing():
    validate_shredding(None)
    assert json_unshreddable_paths(None) == []


def test_python_containers_are_json_shapes():
    """A Mapping is a JSON object and a tuple a JSON array: the request
    encoder writes them as such, so the validator takes them as such."""
    decl = MappingProxyType(
        {
            "type": "object",
            "fields": (
                OrderedDict(name="a", type="string"),
                {
                    "name": "b",
                    "type": "array",
                    "element": MappingProxyType({"type": "int64"}),
                },
            ),
        }
    )
    validate_shredding(decl)
    field = variant_field("v", decl)
    assert json.loads(field.metadata[VARIANT_FIELD_KEY]) == {
        "shredding": {
            "type": "object",
            "fields": [
                {"name": "a", "type": "string"},
                {"name": "b", "type": "array", "element": {"type": "int64"}},
            ],
        }
    }
    op = ops.add_column("v", "variant", shredding=decl)
    assert op.to_wire()["column"]["type_params"] == {
        "shredding": json.loads(field.metadata[VARIANT_FIELD_KEY])["shredding"]
    }


def test_a_key_that_is_not_a_string_is_an_unknown_key():
    """The request encoder would send the key 1 as "1", which the server
    refuses as an unknown key: refused here alike, not skipped."""
    with pytest.raises(ValidationError) as ei:
        validate_shredding({"type": "string", 1: "x"})
    assert ei.value.message == "invalid type_params.shredding: $ has an unknown key '1'"


@pytest.mark.parametrize("name", ["\ud83d\ude00", "a\udfff"])
def test_surrogate_code_points_are_refused_even_paired(name):
    """A Python str cannot hold a UTF-16 pair: two surrogate code points
    are two unpaired surrogates, which the request could not encode."""
    with pytest.raises(ValidationError, match="unpaired surrogate"):
        validate_shredding(
            {"type": "object", "fields": [{"name": name, "type": "string"}]}
        )


@pytest.mark.parametrize(
    "value",
    [True, False, 18.0, "18", None, 2**31, -(2**31) - 1, [18], {"v": 18}],
    ids=repr,
)
def test_precision_must_be_an_int_the_server_reads(value):
    with pytest.raises(ValidationError, match=r"\$ has no integer precision$"):
        validate_shredding({"type": "decimal8", "precision": value, "scale": 0})


def test_a_pathological_declaration_is_refused_without_recursing_into_it():
    """Far deeper than Python's recursion limit: refused at the depth cap,
    or at the unknown key, before the walk descends."""
    chain: dict = {"type": "string"}
    for _ in range(100_000):
        chain = {"type": "array", "element": chain}
    with pytest.raises(ValidationError, match="is nested more than 16 levels deep"):
        validate_shredding(chain)
    blob: dict = {}
    for _ in range(100_000):
        blob = {"x": blob}
    with pytest.raises(ValidationError, match=r"\$ has an unknown key 'extra'"):
        validate_shredding({"type": "string", "extra": blob})
    # A cycle is just a very deep declaration.
    loop: dict = {"type": "array"}
    loop["element"] = loop
    with pytest.raises(ValidationError, match="is nested more than 16 levels deep"):
        validate_shredding(loop)


def test_json_unshreddable_paths():
    decl = {
        "type": "object",
        "fields": [
            {"name": "s", "type": "string"},
            {"name": "b", "type": "boolean"},
            {"name": "i", "type": "int32"},
            {"name": "v", "type": "variant"},
            {"name": "d", "type": "double"},
            {"name": "f", "type": "float"},
            {"name": "when", "type": "timestamptz"},
            {"name": "raw", "type": "binary"},
            {"name": "id", "type": "uuid"},
            {"name": "p4", "type": "decimal4", "precision": 9, "scale": 0},
            {"name": "p8", "type": "decimal8", "precision": 18, "scale": 2},
            {"name": "big", "type": "decimal16", "precision": 38, "scale": 0},
            {"name": "small16", "type": "decimal16", "precision": 18, "scale": 0},
            {"name": "frac16", "type": "decimal16", "precision": 38, "scale": 2},
            # Scale 1 and a precision that integers beyond int64 fit: still
            # out of their reach, since they have scale 0 (Trino's
            # fitsDecimal takes the same scale only).
            {"name": "frac16_1", "type": "decimal16", "precision": 20, "scale": 1},
            {
                "name": "a.b",
                "type": "array",
                "element": {
                    "type": "object",
                    "fields": [{"name": "day", "type": "date"}],
                },
            },
        ],
    }
    paths = json_unshreddable_paths(decl)
    assert [p for p, _ in paths] == [
        "$.d", "$.f", "$.when", "$.raw", "$.id", "$.p4", "$.p8", "$.big",
        "$.small16", "$.frac16", "$.frac16_1", '$["a.b"][*].day',
    ]  # fmt: skip
    reasons = dict(paths)
    assert reasons["$.d"].startswith("double: only numbers written with a fraction")
    assert reasons["$.big"] == "decimal16(38,0): only integers beyond the int64 range"
    assert "nothing wider fits" in reasons["$.small16"]
    assert "never decimals" in reasons["$.frac16"]
    assert reasons["$.frac16_1"] == (
        "decimal16(20,1): JSON fractions become double values, never decimals"
    )
    assert reasons["$.p4"].startswith("decimal4: ")
    # Reachable leaves are not reported, and a primitive root is a leaf.
    assert json_unshreddable_paths({"type": "int64"}) == []
    assert json_unshreddable_paths({"type": "uuid"})[0][0] == "$"
    # Unbounded at decimal16(19,0): the first precision a value beyond
    # int64 fits.
    assert json_unshreddable_paths(
        {"type": "decimal16", "precision": 19, "scale": 0}
    ) == [("$", "decimal16(19,0): only integers beyond the int64 range")]
    with pytest.raises(ValidationError, match="unknown type"):
        json_unshreddable_paths({"type": "text"})


#: The leaf types JSON text fills as such (json_unshreddable_paths).
_FILLED_FROM_JSON = {"boolean", "int8", "int16", "int32", "int64", "string", "variant"}


@pytest.mark.parametrize(
    "type_",
    sorted(variant.PRIMITIVE_TYPES | set(variant.DECIMAL_TYPES) | {"variant"}),
)
def test_json_unshreddable_paths_reports_every_other_leaf_type(type_):
    """Every leaf type is reported or not, as the docstring says; and the
    path is not capped, unlike a refusal's, since a caller reads it."""
    largest = variant.DECIMAL_TYPES.get(type_)
    leaf = (
        {"type": type_}
        if largest is None
        else {"type": type_, "precision": largest, "scale": 0}
    )
    name = "x" * 80
    reported = json_unshreddable_paths(
        {"type": "object", "fields": [{"name": name, **leaf}]}
    )
    if type_ in _FILLED_FROM_JSON:
        assert reported == []
    else:
        [(path, reason)] = reported
        assert path == f"$.{name}"
        assert reason.startswith(type_)


# -- variant_field -------------------------------------------------------------


def test_variant_field_is_json_text_with_the_marker():
    field = variant_field("properties", DECLARATION, nullable=False)
    assert field.name == "properties"
    assert field.type == pa.json_()
    assert field.nullable is False
    # Canonical: keys sorted, arrays in declared order, no whitespace.
    assert field.metadata == {
        VARIANT_FIELD_KEY: (
            b'{"shredding":{"fields":[{"name":"$browser","type":"string"},'
            b'{"name":"price","precision":18,"scale":2,"type":"decimal8"},'
            b'{"element":{"type":"string"},"name":"tags","type":"array"}],'
            b'"type":"object"}}'
        )
    }
    assert variant_field("raw").metadata == {VARIANT_FIELD_KEY: b"{}"}
    assert variant_field("raw").nullable is True


def test_variant_field_copies_and_validates_the_declaration():
    decl = json.loads(json.dumps(DECLARATION))
    field = variant_field("p", decl)
    decl["fields"].append({"name": "Price", "type": "string"})
    assert b"Price" not in field.metadata[VARIANT_FIELD_KEY]
    with pytest.raises(ValidationError) as ei:
        variant_field("p", decl)
    assert ei.value.message == (
        "variant column 'p' has an invalid type_params.shredding: "
        "$ has fields that differ only by case: 'price' and 'Price'"
    )


def test_non_ascii_names_are_escaped_in_the_marker():
    field = variant_field(
        "p",
        {"type": "object", "fields": [{"name": "\u00c4\U0001f600", "type": "string"}]},
    )
    raw = field.metadata[VARIANT_FIELD_KEY]
    assert raw.isascii() and b"\\u00c4\\ud83d\\ude00" in raw
    assert (
        schema_to_column_defs(pa.schema([field]))[0]["type_params"]["shredding"][
            "fields"
        ][0]["name"]
        == "\u00c4\U0001f600"
    )


# -- schema_to_column_defs: the marker ------------------------------------------


def test_marker_becomes_a_variant_column_def():
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            variant_field("properties", DECLARATION),
            variant_field("raw", nullable=False),
            pa.field("doc", pa.json_()),  # no marker: still json
        ]
    )
    assert schema_to_column_defs(schema) == [
        {"name": "id", "type": "long", "nullable": False},
        {
            "name": "properties",
            "type": "variant",
            "nullable": True,
            "type_params": {"shredding": DECLARATION},
        },
        {"name": "raw", "type": "variant", "nullable": False},
        {"name": "doc", "type": "json", "nullable": True},
    ]


def test_an_undeclared_variant_may_be_nested():
    schema = pa.schema(
        [
            pa.field("r", pa.struct([variant_field("x")])),
            pa.field("l", pa.list_(variant_field("item"))),
            pa.field("m", pa.map_(pa.string(), variant_field("anything"))),
        ]
    )
    defs = schema_to_column_defs(schema)
    assert defs[0]["children"] == [{"name": "x", "type": "variant", "nullable": True}]
    assert defs[1]["children"] == [
        {"name": "element", "type": "variant", "nullable": True}
    ]
    assert defs[2]["children"][1] == {
        "name": "value",
        "type": "variant",
        "nullable": True,
    }


@pytest.mark.parametrize(
    "field,qualified",
    [
        (pa.field("r", pa.struct([variant_field("x", {"type": "string"})])), "r.x"),
        (
            pa.field("l", pa.list_(variant_field("item", {"type": "string"}))),
            "l.element",
        ),
        (
            pa.field("m", pa.map_(pa.string(), variant_field("v", {"type": "string"}))),
            "m.value",
        ),
        (
            pa.field(
                "a",
                pa.struct(
                    [pa.field("b", pa.list_(variant_field("c", {"type": "string"})))]
                ),
            ),
            "a.b.element",
        ),
        (
            pa.field(
                "r",
                pa.struct(
                    [pa.field("s", pa.struct([variant_field("x", {"type": "string"})]))]
                ),
            ),
            "r.s.x",
        ),
    ],
    ids=["struct", "list", "map", "deep", "struct-in-struct"],
)
def test_a_nested_declaration_is_refused_as_the_server_refuses_it(field, qualified):
    with pytest.raises(ValidationError) as ei:
        schema_to_column_defs(pa.schema([field]))
    assert ei.value.message == (
        f"variant column '{qualified}' is nested, and only a top-level variant "
        "column can declare type_params.shredding"
    )


def test_a_nested_declaration_is_refused_before_its_own_faults():
    """The server's ColumnTrees refuses the nesting before it reads the
    declaration, so a hand-built marker with a bad declaration below the
    top level gets the nested refusal, as the 422 would."""
    bad = pa.field(
        "x", pa.json_(), metadata={VARIANT_FIELD_KEY: b'{"shredding":{"type":"text"}}'}
    )
    with pytest.raises(ValidationError) as ei:
        schema_to_column_defs(pa.schema([pa.field("r", pa.struct([bad]))]))
    assert ei.value.message == (
        "variant column 'r.x' is nested, and only a top-level variant column "
        "can declare type_params.shredding"
    )


def test_synthetic_children_take_the_catalogs_names():
    """A map's key and value and a list's element are named by the
    catalog, whatever Arrow calls them."""
    schema = pa.schema(
        [
            pa.field(
                "m",
                pa.map_(
                    pa.field("k", pa.string(), nullable=False),
                    pa.field("v", pa.int64()),
                ),
            ),
            pa.field("l", pa.list_(pa.field("item0", pa.int64()))),
        ]
    )
    m, l = schema_to_column_defs(schema)
    assert [c["name"] for c in m["children"]] == ["key", "value"]
    assert [c["name"] for c in l["children"]] == ["element"]


@pytest.mark.parametrize(
    "metadata,match",
    [
        (b"not json", "malformed"),
        # Deeper than json.loads recurses: RecursionError, not ValueError.
        (b"[" * 100_000, "malformed"),
        (b"[]", "malformed"),
        (b'{"shredding": null, "extra": 1}', "malformed"),
        (b'{"shreding": {"type": "string"}}', "malformed"),
        # A repeated key, which json.loads would settle by keeping the
        # last: here an undeclared column, the other way round a declared
        # one, and inside the declaration a different leaf.
        (b'{"shredding": {"type": "text"}, "shredding": null}', "malformed"),
        (b'{"shredding": null, "shredding": {"type": "string"}}', "malformed"),
        (b'{"shredding": {"type": "string", "type": "int64"}}', "malformed"),
        # Declarations that are falsy but not null declare something, and
        # are refused as the server refuses them: never an undeclared
        # column, which would be permanent.
        (
            b'{"shredding": {}}',
            r"^variant column 'v' has an invalid type_params.shredding: \$ has no type$",
        ),
        (
            b'{"shredding": false}',
            r"^variant column 'v' has an invalid type_params.shredding: \$ is not a JSON object$",
        ),
        (
            b'{"shredding": []}',
            r"^variant column 'v' has an invalid type_params.shredding: \$ is not a JSON object$",
        ),
        (
            b'{"shredding": {"type": "text"}}',
            r"variant column 'v' has an invalid type_params.shredding: \$ has an unknown type 'text'",
        ),
    ],
)
def test_a_marker_this_client_did_not_write_is_refused(metadata, match):
    field = pa.field("v", pa.json_(), metadata={VARIANT_FIELD_KEY: metadata})
    with pytest.raises(ValidationError, match=match):
        schema_to_column_defs(pa.schema([field]))


def test_a_marker_on_another_type_is_refused():
    field = variant_field("v").with_type(pa.string())
    with pytest.raises(
        ValidationError, match=r"marker on string; a variant field is pa\.json_\(\)"
    ):
        schema_to_column_defs(pa.schema([field]))
    # json_ over large_string is JSON text all the same.
    large = variant_field("v").with_type(pa.json_(pa.large_string()))
    assert schema_to_column_defs(pa.schema([large]))[0]["type"] == "variant"


def test_a_null_declaration_in_the_marker_declares_nothing():
    field = pa.field(
        "v", pa.json_(), metadata={VARIANT_FIELD_KEY: b'{"shredding":null}'}
    )
    assert schema_to_column_defs(pa.schema([field])) == [
        {"name": "v", "type": "variant", "nullable": True}
    ]
    nested = pa.field("r", pa.struct([field]))
    assert (
        schema_to_column_defs(pa.schema([nested]))[0]["children"][0]["type"]
        == "variant"
    )


# -- the VARIANT storage shape -------------------------------------------------

_META = pa.field("metadata", pa.binary(), nullable=False)
_VALUE = pa.field("value", pa.binary())


@pytest.mark.parametrize(
    "t",
    [
        pa.struct([_META, _VALUE]),
        pa.struct([_META, _VALUE.with_nullable(False)]),
        pa.struct([_META, _VALUE, pa.field("typed_value", pa.int64())]),
        pa.struct(
            [
                _META,
                _VALUE,
                pa.field(
                    "typed_value",
                    pa.struct(
                        [
                            pa.field(
                                "a",
                                pa.struct(
                                    [_VALUE, pa.field("typed_value", pa.string())]
                                ),
                                False,
                            )
                        ]
                    ),
                ),
            ]
        ),
        pa.struct(
            [_META.with_type(pa.large_binary()), _VALUE.with_type(pa.large_binary())]
        ),
    ],
    ids=["unshredded", "required-value", "shredded", "shredded-object", "large-binary"],
)
def test_variant_storage_is_refused_with_a_hint(t):
    with pytest.raises(
        UnsupportedTypeError, match=r"shaped like VARIANT storage.*variant_field"
    ):
        arrow_type_to_coltype(t)
    # Wherever it stands: nested, through DDL, through add_column.
    with pytest.raises(UnsupportedTypeError, match="VARIANT storage"):
        arrow_type_to_coltype(pa.list_(t))
    with pytest.raises(UnsupportedTypeError, match="VARIANT storage"):
        schema_to_column_defs(pa.schema([pa.field("r", pa.struct([pa.field("v", t)]))]))
    with pytest.raises(UnsupportedTypeError, match="VARIANT storage"):
        ops.add_column("v", t)


@pytest.mark.parametrize(
    "t",
    [
        pa.struct([_META.with_nullable(True), _VALUE]),
        pa.struct([_VALUE, _META]),
        pa.struct([_VALUE.with_nullable(False), _META]),
        pa.struct([_META, _VALUE, pa.field("extra", pa.int64())]),
        pa.struct([_META, _VALUE, pa.field("typed_value", pa.int64()), pa.field("x", pa.int8())]),
        pa.struct([_META, _VALUE.with_type(pa.string())]),
        pa.struct([_META.with_type(pa.string()), _VALUE]),
        pa.struct([_META]),
        pa.struct([pa.field("Metadata", pa.binary(), False), _VALUE]),
    ],
    ids=["nullable-metadata", "reordered", "reordered-required", "extra", "typed-then-extra", "string-value",
         "string-metadata", "metadata-only", "case"],
)  # fmt: skip
def test_a_struct_merely_like_variant_storage_is_a_struct(t):
    assert arrow_type_to_coltype(t) == ("struct", None)


# -- the DDL wire --------------------------------------------------------------

TABLE_WIRE = {
    "name": "events",
    "namespace": "ns1",
    "table_uuid": "0b8ee9ba-79a1-4f3e-b7e5-6a0b6ab6f012",
    "columns": [
        {"name": "id", "type": "long", "field_id": 1, "ordinal": 0, "nullable": False},
        {
            "name": "properties",
            "type": "variant",
            "field_id": 2,
            "ordinal": 1,
            "nullable": True,
            "type_params": {"shredding": DECLARATION},
        },
    ],
}


@pytest.fixture
def namespace(httpx_mock):
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat",
        json={
            "name": "cat",
            "data_path": "s3://b/",
            "head_snapshot_id": 1,
            "schema_version": 1,
        },
    )
    with HoglakeClient(BASE) as client:
        yield Namespace(client.catalog("cat"), "ns1")


def test_create_table_sends_the_declaration(namespace, httpx_mock):
    """The exact body, as a literal: it is a contract with the server's
    ColumnDefDto. A declared variant carries type_params.shredding; an
    undeclared one carries no type_params at all."""
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables",
        json=TABLE_WIRE,
        status_code=201,
    )
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            variant_field("properties", DECLARATION),
            variant_field("props_raw"),
            variant_field("strict", {"type": "variant"}, nullable=False),
        ]
    )
    table = namespace.create_table("events", schema)
    assert json.loads(httpx_mock.get_requests()[-1].content) == {
        "name": "events",
        "columns": [
            {"name": "id", "type": "long", "nullable": False},
            {
                "name": "properties",
                "type": "variant",
                "nullable": True,
                "type_params": {
                    "shredding": {
                        "type": "object",
                        "fields": [
                            {"name": "$browser", "type": "string"},
                            {
                                "name": "price",
                                "type": "decimal8",
                                "precision": 18,
                                "scale": 2,
                            },
                            {
                                "name": "tags",
                                "type": "array",
                                "element": {"type": "string"},
                            },
                        ],
                    }
                },
            },
            {"name": "props_raw", "type": "variant", "nullable": True},
            {
                "name": "strict",
                "type": "variant",
                "nullable": False,
                "type_params": {"shredding": {"type": "variant"}},
            },
        ],
    }
    # The declaration reads back as an ordinary type_params map.
    assert table.columns[1].type_params == {"shredding": DECLARATION}


def test_create_table_refuses_locally_before_any_request(namespace, httpx_mock):
    nested = pa.schema(
        [pa.field("r", pa.struct([variant_field("x", {"type": "string"})]))]
    )
    with pytest.raises(ValidationError, match="is nested"):
        namespace.create_table("t", nested)
    storage = pa.schema([pa.field("v", pa.struct([_META, _VALUE]))])
    with pytest.raises(UnsupportedTypeError, match="VARIANT storage"):
        namespace.create_table("t", storage)
    assert [r.method for r in httpx_mock.get_requests()] == ["GET"]


def test_add_column_sends_the_declaration(httpx_mock):
    assert ops.add_column("props", "variant", shredding=DECLARATION).to_wire() == {
        "op": "add_column",
        "column": {
            "name": "props",
            "type": "variant",
            "nullable": True,
            "type_params": {"shredding": DECLARATION},
        },
    }
    # Without one: no type_params, as before (bench's step_add_variant).
    assert ops.add_column("raw", "variant", False).to_wire() == {
        "op": "add_column",
        "column": {"name": "raw", "type": "variant", "nullable": False},
    }
    # Through Table.alter, on the wire.
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat",
        json={
            "name": "cat",
            "data_path": "s3://b/",
            "head_snapshot_id": 1,
            "schema_version": 1,
        },
    )
    httpx_mock.add_response(
        method="GET",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events",
        json=TABLE_WIRE,
    )
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs/cat/namespaces/ns1/tables/events/alter",
        json=TABLE_WIRE,
    )
    with HoglakeClient(BASE) as client:
        table = Namespace(client.catalog("cat"), "ns1").table("events")
        table.alter([ops.add_column("p", "variant", shredding={"type": "string"})])
    assert json.loads(httpx_mock.get_requests()[-1].content) == {
        "ops": [
            {
                "op": "add_column",
                "column": {
                    "name": "p",
                    "type": "variant",
                    "nullable": True,
                    "type_params": {"shredding": {"type": "string"}},
                },
            }
        ]
    }


def test_add_column_copies_the_declaration():
    decl = {"type": "object", "fields": [{"name": "a", "type": "string"}]}
    op = ops.add_column("p", "variant", shredding=decl)
    decl["fields"].append({"name": "b", "type": "string"})
    assert op.to_wire()["column"]["type_params"]["shredding"]["fields"] == [
        {"name": "a", "type": "string"}
    ]


@pytest.mark.parametrize("type_", ["VARIANT", "Variant"])
def test_add_column_reads_the_type_name_in_any_case(type_):
    """The server parses a type name case-insensitively (ColType.fromWire),
    so "VARIANT" is a variant there and takes a declaration; the name goes
    as the caller spelt it, as it does without one."""
    op = ops.add_column("p", type_, shredding={"type": "string"})
    assert op.to_wire()["column"] == {
        "name": "p",
        "type": type_,
        "nullable": True,
        "type_params": {"shredding": {"type": "string"}},
    }
    with pytest.raises(ValidationError, match=r"\$ has no type$"):
        ops.add_column("p", type_, shredding={})


@pytest.mark.parametrize(
    "type_,shown",
    [
        ("json", "json"),
        ("string", "string"),
        ("STRING", "string"),
        ("Uuid", "uuid"),
        (pa.float64(), "double"),
        (pa.json_(), "json"),
        (pa.decimal128(10, 2), "decimal"),
    ],
    ids=["json", "string", "upper-string", "mixed-uuid", "arrow-double", "arrow-json",
         "arrow-decimal"],
)  # fmt: skip
def test_add_column_refuses_a_declaration_on_another_type(type_, shown):
    """In the server's words, which quote the type's wire name, not the
    caller's spelling of it."""
    with pytest.raises(ValidationError) as ei:
        ops.add_column("p", type_, shredding={"type": "string"})
    assert ei.value.message == (
        f"column 'p' is '{shown}', and only a variant column can declare "
        "type_params.shredding"
    )


@pytest.mark.parametrize(
    "type_,shown",
    [
        ("list", "list"),
        ("Struct", "struct"),
        ("map", "map"),
        (pa.list_(pa.int64()), "list"),
        (pa.struct([pa.field("a", pa.int64())]), "struct"),
    ],
    ids=["list", "mixed-struct", "map", "arrow-list", "arrow-struct"],
)
def test_add_column_refuses_a_declaration_on_a_container(type_, shown):
    """ColumnTrees refuses any type_params on a container before it asks
    whether the column is a variant, so that is the 422, and the local
    refusal."""
    with pytest.raises(ValidationError) as ei:
        ops.add_column("p", type_, shredding={"type": "string"})
    assert ei.value.message == _container_refusal("p", shown)


@pytest.mark.parametrize("type_", ["foo", "int128", "uuid_t"])
def test_add_column_leaves_an_unknown_type_name_to_the_server(type_):
    """A name outside the vocabulary is refused by the server with a
    message of its own per name ("unknown column type", or int128's
    permanent reason), so it goes as it would without a declaration, and
    the declaration with it, unchecked: the type's refusal comes first."""
    op = ops.add_column("p", type_, shredding={"type": "text"})
    assert op.to_wire()["column"] == {
        "name": "p",
        "type": type_,
        "nullable": True,
        "type_params": {"shredding": {"type": "text"}},
    }


def test_the_column_type_vocabulary_matches_the_server():
    """ops reads the vocabulary to tell a type the server knows (refused
    here in its words) from one it does not (left to it). Parsed out of
    the ColType enum, so a type added there reds here."""
    body = re.search(
        r"enum class ColType \{(.*?)\n\s*;",
        _kotlin(Path("server/src/main/kotlin/com/posthog/hoglake/model/Model.kt")),
        re.DOTALL,
    )
    assert body, "ColType enum not found in Model.kt"
    entries = re.findall(r"^\s+([A-Z][A-Z0-9_]*),\s*$", body.group(1), re.MULTILINE)
    # Its wire names: lowercase, and UUID_T is "uuid" (ColType.wire).
    wire = {"uuid" if e == "UUID_T" else e.lower() for e in entries}
    assert len(wire) >= 20, entries
    assert ops._COLUMN_TYPES == wire


def test_add_column_refuses_an_invalid_declaration():
    with pytest.raises(ValidationError) as ei:
        ops.add_column(
            "props",
            "variant",
            shredding={
                "type": "object",
                "fields": [
                    {"name": "price", "type": "decimal4", "precision": 18, "scale": 2}
                ],
            },
        )
    # VariantShreddingApiTest's refusal, word for word.
    assert ei.value.message == (
        "variant column 'props' has an invalid type_params.shredding: "
        "$.price has precision 18 and scale 2, which decimal4 does not hold"
    )
    # {} is a declaration without a type, not "no declaration".
    with pytest.raises(ValidationError, match=r"\$ has no type"):
        ops.add_column("props", "variant", shredding={})
