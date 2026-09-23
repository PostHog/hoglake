"""Client/server bounds parity for table metadata (#169).

The alter ops in ``pyhoglake.ops`` mirror the server's validation bounds so
a bad comment or property set fails before the round trip. Those bounds are
the server's to own — so this test PARSES them out of
``server/src/main/kotlin/com/posthog/hoglake/service/TableMetadata.kt``
rather than restating them. A bound that drifts on the server fails here;
a test that restated the constant would only assert the file compiles
(AGENT.md).
"""

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
