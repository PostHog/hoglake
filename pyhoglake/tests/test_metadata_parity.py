"""Client/server bounds parity for table metadata (#169).

The alter ops in ``pyhoglake.ops`` mirror the server's validation bounds so
a bad comment or property set fails before the round trip. Those bounds are
the server's to own — so this test PARSES them out of
``server/src/main/kotlin/com/posthog/hoglake/service/TableMetadata.kt``
rather than restating them. A bound that drifts on the server fails here;
a test that restated the constant would only assert the file compiles
(AGENT.md).
"""

import dataclasses
import re
from pathlib import Path

import pytest

from pyhoglake import ops

_TABLE_METADATA_REL = Path(
    "server/src/main/kotlin/com/posthog/hoglake/service/TableMetadata.kt"
)


def _server_file(relative: Path) -> Path:
    """Locate a server source file by walking up from this test.

    Failing, not skipping, when missing: a skip reads as green, so a moved
    directory would silently retire the only check that the client and
    server bounds agree.
    """
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / relative
        if candidate.exists():
            return candidate
    raise AssertionError(
        f"cannot find {relative} above {here}. This test compares the "
        f"client's metadata bounds against the server's; if the trees really "
        f"are separate now, delete it deliberately rather than letting it skip."
    )


def _source() -> str:
    return _server_file(_TABLE_METADATA_REL).read_text()


def test_comment_max_chars_matches_server():
    m = re.search(r"comment\.length > (\d+)", _source())
    assert m, "comment length bound not found in TableMetadata.kt"
    assert ops._COMMENT_MAX_CHARS == int(m.group(1))


def test_comment_nul_rule_matches_server():
    # The server rejects a NUL in a comment; the client's ops must too.
    assert "\\u0000' in comment" in _source()
    with pytest.raises(ValueError, match="no NUL"):
        ops.set_table_comment("a\x00b")


def test_properties_max_matches_server():
    m = re.search(r"properties\.size > (\d+)", _source())
    assert m, "properties count bound not found in TableMetadata.kt"
    assert ops._PROPERTIES_MAX == int(m.group(1))


def test_property_value_max_chars_matches_server():
    m = re.search(r"value\.length > (\d+)", _source())
    assert m, "property value length bound not found in TableMetadata.kt"
    assert ops._PROPERTY_VALUE_MAX_CHARS == int(m.group(1))


def test_property_value_nul_rule_matches_server():
    assert "\\u0000' in value" in _source()
    with pytest.raises(ValueError, match="no NUL"):
        ops.set_properties({"k": "a\x00b"})


def test_property_key_pattern_matches_server():
    m = re.search(r'Regex\("(\[a-z\]\[a-z0-9_.\-\]\{0,127\})"\)', _source())
    assert m, "property key regex not found in TableMetadata.kt"
    # The Kotlin regex and the Python one describe the same language.
    assert ops._PROPERTY_KEY_PATTERN.pattern == m.group(1)


def test_reserved_keys_match_server():
    m = re.search(r"key in setOf\(([^)]*)\)", _source())
    assert m, "reserved property keys not found in TableMetadata.kt"
    server_reserved = frozenset(re.findall(r'"([^"]+)"', m.group(1)))
    assert ops._PROPERTY_KEY_RESERVED == server_reserved


def test_reserved_prefixes_match_server():
    prefixes = re.findall(r'key\.startsWith\("([^"]+)"\)', _source())
    assert prefixes, "reserved property key prefixes not found in TableMetadata.kt"
    assert set(ops._PROPERTY_KEY_RESERVED_PREFIXES) == set(prefixes)


# --- the writer path's zero-read contract (#232/#233) ---------------------
#
# `Table.prepare_append_files` sends no table GET at all while its cache is
# warm: it prepares against the cached `TableInfo` and sends that read's own
# `read_snapshot_id` as the commit's `read_snapshot`, which the server's OCC
# then validates. That works only if THREE things agree across the two
# trees, and none of them is checked by either suite alone:
#
#   * the server really sends the field the cache reads, under that name;
#   * it sends it on every response, so the cache is always usable (a
#     nullable field would silently fall back to two reads per flush);
#   * `totals=false` — the parameter the writer path adds to every read it
#     does make — is a documented parameter and not an undocumented one the
#     server happens to ignore.
#
# Parsed out of the spec and the DTO rather than restated, so a rename on
# either side reds here instead of degrading the writer path in silence.

_SPEC_REL = Path("server/src/main/resources/openapi/hoglake.yaml")
_TABLE_DTO_REL = Path("server/src/main/kotlin/com/posthog/hoglake/api/Dto.kt")

# The wire key pyhoglake's writer cache is keyed on; see
# `pyhoglake.client._cache_entry`.
_CACHE_KEY = "read_snapshot_id"


def _spec_text() -> str:
    return _server_file(_SPEC_REL).read_text()


def _table_schema_block() -> str:
    """The spec's `Table:` schema block, up to the next sibling schema.

    Text, not parsed YAML: this tree has no YAML dependency and this file
    already reads the server by regex. The block boundary is the indent,
    which is what makes that safe.
    """
    text = _spec_text()
    marker = "\n    Table:\n"
    start = text.index(marker) + len(marker)
    rest = text[start:]
    end = re.search(r"\n    [A-Za-z]", rest)
    return rest[: end.start()] if end else rest


def test_the_cache_key_is_a_field_the_server_declares():
    assert re.search(rf"^        {_CACHE_KEY}:", _table_schema_block(), re.MULTILINE), (
        f"the writer cache reads Table.{_CACHE_KEY}; the spec does not declare it"
    )


def test_the_cache_key_is_required_so_the_cache_is_always_usable():
    # Not decoration: `_cache_entry` returns None without it, and the
    # writer path then falls back to a catalog GET plus an identity GET
    # per flush. If this ever becomes optional the fallback is correct but
    # the zero-read claim is not, and pyhoglake's README says otherwise.
    required = re.search(
        r"required: \[([^]]*)]", _table_schema_block(), re.DOTALL
    ).group(1)
    names = {n.strip() for n in required.split(",")}
    assert _CACHE_KEY in names, (
        f"Table.{_CACHE_KEY} is no longer required; the writer cache degrades "
        "to two reads per flush and pyhoglake's README claims otherwise"
    )


def test_the_client_reads_exactly_the_key_the_server_serializes():
    # The Kotlin DTO property is camelCase and the wire is snake_case
    # (PropertyNamingStrategies.SNAKE_CASE), so the comparison is on the
    # derived wire name — a rename on either side breaks the pairing.
    dto = _server_file(_TABLE_DTO_REL).read_text()
    camel = re.sub(r"_(\w)", lambda m: m.group(1).upper(), _CACHE_KEY)
    assert re.search(rf"\bval {camel}\b", dto), (
        f"TableDto has no `{camel}` property, so the server does not serialize "
        f"`{_CACHE_KEY}` and the writer cache is dead"
    )
    from pyhoglake.models import TableInfo

    assert _CACHE_KEY in {f.name for f in dataclasses.fields(TableInfo)}


def test_totals_false_is_a_documented_parameter():
    # Every read the writer path makes carries it. It was safe to send
    # before the server honoured it (unknown query parameters are
    # ignored), but "safe" is not "documented", and the client must not
    # depend on an undocumented parameter.
    text = _spec_text()
    marker = "\n  /catalogs/{catalog}/namespaces/{namespace}/tables/{table}:\n"
    start = text.index(marker) + len(marker)
    rest = text[start:]
    end = re.search(r"\n  /", rest)
    path_item = rest[: end.start()] if end else rest
    assert re.search(r"^\s+name: totals$", path_item, re.MULTILINE), (
        "the writer path sends ?totals=false on every table read; the spec "
        "does not document the parameter on GET /tables/{table}"
    )
