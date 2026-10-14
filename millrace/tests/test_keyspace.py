"""Keyspace codecs: round-trips, byte order = logical order, golden vectors.

The golden vectors pin the byte-level contract; each is written out byte
by byte. The group encoding is prefix-free, so a range built from
``prefix + group`` selects exactly one tuple's rows: the property the
"a tuple that is a byte prefix of another" tests pin.
"""

from typing import Any

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from millrace import keyspace
from millrace.keyspace import (
    FENCE_PROBE_KEY,
    META_PREFIX,
    ROWS_PREFIX,
    TABLE_UUID_KEY,
    PartitionTuple,
    RowKey,
    decode_group,
    decode_row_key,
    encode_fence_probe_value,
    encode_group,
    encode_row_key,
    group_label,
    group_rows_prefix,
    group_rows_range,
    prefix_successor,
    rows_range,
)

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1

#: Partition tuple components: any UTF-8-encodable string or a null.
components = st.one_of(
    st.none(),
    st.text(alphabet=st.characters(exclude_categories=("Cs",)), max_size=12),
)
groups = st.lists(components, max_size=4).map(tuple)
kafka_offsets = st.integers(min_value=INT64_MIN, max_value=INT64_MAX)


def _fields(group: PartitionTuple) -> tuple[str, ...]:
    return tuple(f"f{i}" for i in range(len(group)))


def _logical(group: PartitionTuple) -> tuple[Any, ...]:
    """A tuple's logical sort key, independent of the codec: arity first,
    then per component null-before-present, then the UTF-8 length, then
    the bytes."""
    out: list[Any] = [len(group)]
    for c in group:
        if c is None:
            out.append((0,))
        else:
            raw = c.encode()
            out.append((1, len(raw), raw))
    return tuple(out)


row_keys = st.builds(RowKey, group=groups, offset=kafka_offsets)


# -- the partition tuple (group) ----------------------------------------------


def test_group_golden_vectors():
    # arity(u8), then per component 0x00 (null) or 0x01 len(u16) utf8.
    assert encode_group(()) == b"\x00"
    assert encode_group((None,)) == b"\x01\x00"
    assert encode_group(("42", "681")) == (
        b"\x02" + b"\x01\x00\x02" + b"42" + b"\x01\x00\x03" + b"681"
    )
    assert encode_group(("é", None)) == b"\x02" + b"\x01\x00\x02\xc3\xa9" + b"\x00"


@given(group=groups)
def test_group_round_trip(group: PartitionTuple):
    assert decode_group(encode_group(group)) == group


def test_group_round_trip_edge_shapes():
    for group in [(), (None,), (None, None), ("",), ("", None), ("日本", "🦔")]:
        assert decode_group(encode_group(group)) == group


@given(a=groups, b=groups)
def test_no_group_encoding_is_a_byte_prefix_of_another(
    a: PartitionTuple, b: PartitionTuple
):
    # Prefix-freeness of the WHOLE tuple — including across arities, which
    # the per-component tags alone would not give: (x,) and (x, y) would
    # share a prefix without the arity byte.
    assume(a != b)
    assert not encode_group(b).startswith(encode_group(a))


@given(a=groups, b=groups)
def test_group_byte_order_is_the_documented_logical_order(
    a: PartitionTuple, b: PartitionTuple
):
    assert (encode_group(a) < encode_group(b)) == (_logical(a) < _logical(b))


def test_decode_group_is_total_and_refuses_corruption():
    good = encode_group(("42", None))
    for cut in range(len(good)):
        with pytest.raises(ValueError, match="corrupt partition tuple"):
            decode_group(good[:cut])
    with pytest.raises(ValueError, match="trailing"):
        decode_group(good + b"\x00")
    # The count is the bytes past the tuple, not the buffer length.
    with pytest.raises(ValueError, match=r": 2 trailing byte\(s\)$"):
        decode_group(b"\x00\x00\x00")
    with pytest.raises(ValueError, match="no arity byte"):
        decode_group(b"")
    with pytest.raises(ValueError, match="tag 0x02"):
        decode_group(b"\x01\x02")
    with pytest.raises(ValueError, match="not UTF-8"):
        decode_group(b"\x01\x01\x00\x01\xff")


@given(data=st.binary(max_size=24))
def test_decode_group_never_raises_anything_but_value_error(data: bytes):
    try:
        group = decode_group(data)
    except ValueError:
        return
    assert encode_group(group) == data


def test_encode_group_validation():
    with pytest.raises(ValueError, match="at most 255"):
        encode_group(("x",) * 256)
    encode_group(("x",) * 255)
    with pytest.raises(ValueError, match="^partition value does not fit in a u16"):
        encode_group(("x" * 2**16,))
    encode_group(("x" * (2**16 - 1),))
    with pytest.raises(TypeError, match="wire strings or None, got int$"):
        encode_group((42,))  # type: ignore[arg-type]


def test_group_label_is_the_hoglake_human_form():
    assert (
        group_label(("team_id", "timestamp_month"), ("42", "681"))
        == "team_id=42/timestamp_month=681"
    )
    assert group_label((), ()) == ""
    # A null component is its bare field name; values percent-encode, so
    # the form is ASCII and injective (a value "/", "=" or ":" cannot
    # forge another tuple's form; a string "null" is not a null).
    assert group_label(("a", "b"), (None, "x")) == "a/b=x"
    assert group_label(("a",), ("null",)) == "a=null"
    assert group_label(("s",), ("a/b=c:d é",)) == "s=a%2Fb%3Dc%3Ad%20%C3%A9"
    assert group_label(("properties.team_id",), ("7",)) == "properties.team_id=7"
    # FIELD NAMES percent-encode too: a "/" or "=" in a name cannot forge
    # another tuple's form (a name "a=b" with a null value would read as
    # field "a" holding "b"; a name "a/b" as two fields).
    assert group_label(("a=b",), (None,)) == "a%3Db"
    assert group_label(("a",), ("b",)) == "a=b"
    assert group_label(("a/b",), ("x",)) == "a%2Fb=x"
    assert group_label(("a", "b"), (None, "x")) == "a/b=x"
    assert group_label(("a:b é",), ("1",)) == "a%3Ab%20%C3%A9=1"
    # "._-" stay literal in names (dotted struct paths read as written).
    assert group_label(("p.q_r-s",), ("1",)) == "p.q_r-s=1"
    with pytest.raises(ValueError, match="1 field name"):
        group_label(("a",), ("x", "y"))


@given(a=groups, b=groups)
def test_group_label_is_injective_and_ascii(a: PartitionTuple, b: PartitionTuple):
    assume(len(a) == len(b) and a != b)
    la, lb = group_label(_fields(a), a), group_label(_fields(b), b)
    assert la.isascii() and lb.isascii()
    assert la != lb


# -- row keys: golden vectors -------------------------------------------------

# -- row keys -----------------------------------------------------------------


def test_row_key_golden_vectors():
    # group ("1",), offset 0: zero maps to 0x8000... (the offset-binary
    # midpoint), so byte order can be numeric order.
    assert encode_row_key(RowKey(group=("1",), offset=0)) == (
        b"rows/" + b"\x01\x01\x00\x01" + b"1" + b"\x80" + b"\x00" * 7
    )
    # The unpartitioned group and INT64_MIN: arity 0, then zero bytes —
    # the smallest encodable offset sorts before everything.
    assert encode_row_key(RowKey(group=(), offset=INT64_MIN)) == (
        b"rows/" + b"\x00" + b"\x00" * 8
    )
    # A null component and the all-ones corner.
    assert (
        encode_row_key(RowKey(group=(None,), offset=INT64_MAX))
        == b"rows/" + b"\x01\x00" + b"\xff" * 8
    )
    # Offset -1 sorts just below 0, and its bytes show it; 41 is 0x29.
    assert encode_row_key(RowKey(group=("ab",), offset=-1)) == (
        b"rows/" + b"\x01\x01\x00\x02ab" + b"\x7f\xff\xff\xff\xff\xff\xff\xff"
    )
    assert encode_row_key(RowKey(group=("ab",), offset=41)) == (
        b"rows/" + b"\x01\x01\x00\x02ab" + b"\x80\x00\x00\x00\x00\x00\x00\x29"
    )


@given(key=row_keys)
def test_row_key_round_trip(key: RowKey):
    assert decode_row_key(encode_row_key(key)) == key


@given(keys=st.lists(row_keys, max_size=60))
def test_row_key_byte_order_is_logical_order(keys: list[RowKey]):
    # Group, then offset: within a group rows sort by offset, so a capped
    # scan of the group's slice is an exact offset prefix.
    by_bytes = sorted(keys, key=encode_row_key)
    by_value = sorted(keys, key=lambda k: (_logical(k.group), k.offset))
    assert [encode_row_key(k) for k in by_bytes] == [
        encode_row_key(k) for k in by_value
    ]


@given(keys=st.lists(row_keys, min_size=2, max_size=60))
def test_row_key_encoding_is_injective(keys: list[RowKey]):
    encoded = [encode_row_key(k) for k in keys]
    assert len(set(encoded)) == len(set(keys))


def test_rows_within_a_group_sort_by_offset():
    offsets = [INT64_MIN, -2, -1, 0, 1, 2, 255, 256, 2**32, INT64_MAX]
    keys = [RowKey(group=("7",), offset=o) for o in reversed(offsets)]
    by_bytes = sorted(keys, key=encode_row_key)
    assert [k.offset for k in by_bytes] == offsets


@given(key=row_keys, group=groups)
def test_group_rows_range_membership(key: RowKey, group: PartitionTuple):
    start, end = group_rows_range(group)
    encoded = encode_row_key(key)
    assert (start <= encoded < end) == (key.group == group)


@given(keys=st.lists(row_keys, max_size=60))
def test_one_groups_rows_are_contiguous_in_byte_order(keys: list[RowKey]):
    ordered = [decode_row_key(b) for b in sorted(encode_row_key(k) for k in keys)]
    present = {k.group for k in ordered}
    for group in present:
        positions = [i for i, k in enumerate(ordered) if k.group == group]
        assert positions == list(range(positions[0], positions[-1] + 1))


@pytest.mark.parametrize(
    ("shorter", "longer"),
    [
        ((), ("a",)),  # the unpartitioned tuple vs a one-field spec
        (("a",), ("a", "b")),  # an older one-field spec vs a two-field one
        (("a",), ("a", None)),
        ((None,), (None, None)),
        (("a",), ("ab",)),
        (("",), ("", "")),
    ],
)
def test_a_tuple_whose_components_prefix_another_never_leaks_into_its_range(
    shorter: PartitionTuple, longer: PartitionTuple
):
    # Without the arity byte the shorter tuple's component bytes ARE a
    # byte prefix of the longer one's — and its prefix-successor range
    # would swallow the longer tuple's rows and prepared entries.
    for off in [INT64_MIN, 0, INT64_MAX]:
        row_long = encode_row_key(RowKey(longer, off))
        row_short = encode_row_key(RowKey(shorter, off))
        start, end = group_rows_range(shorter)
        assert not start <= row_long < end
        assert start <= row_short < end
        start, end = group_rows_range(longer)
        assert not start <= row_short < end
        assert start <= row_long < end


def test_prefix_successor_strips_only_trailing_ff():
    assert prefix_successor(b"rows/") == b"rows0"
    assert prefix_successor(b"a\xff\xff") == b"b"
    assert prefix_successor(b"aX") == b"aY"
    assert prefix_successor(b"\x00") == b"\x01"
    for bad in (b"\xff", b"\xff\xff"):
        with pytest.raises(ValueError, match="no successor"):
            prefix_successor(bad)


def test_group_rows_range_end_is_the_exactprefix_successor():
    start, end = group_rows_range(("X",))
    assert (start, end) == (b"rows/\x01\x01\x00\x01X", b"rows/\x01\x01\x00\x01Y")
    # The successor increments the last byte of the full prefix.
    start, end = group_rows_range(("\x7f",))
    assert end == b"rows/\x01\x01\x00\x01\x80"
    top = encode_row_key(RowKey(("X",), INT64_MAX))
    assert group_rows_range(("X",))[0] <= top < group_rows_range(("X",))[1]
    assert group_rows_prefix(()) == b"rows/\x00"


def test_row_key_field_validation():
    with pytest.raises(ValueError, match="offset"):
        RowKey(group=(), offset=INT64_MAX + 1)
    with pytest.raises(ValueError, match="offset"):
        RowKey(group=(), offset=INT64_MIN - 1)


@given(key=row_keys)
def test_decode_row_key_rejects_strict_prefixes_and_wrong_prefix(key: RowKey):
    encoded = encode_row_key(key)
    for cut in range(len(encoded)):
        with pytest.raises(ValueError):
            decode_row_key(encoded[:cut])
    with pytest.raises(ValueError, match="does not start with"):
        decode_row_key(b"rowx/" + encoded[len(ROWS_PREFIX) :])
    # A corrupted group (an arity naming one more field) is refused.
    corrupt = bytearray(encoded)
    corrupt[len(ROWS_PREFIX)] += 1
    with pytest.raises(ValueError):
        decode_row_key(bytes(corrupt))


def test_decode_row_key_reports_the_minimum_length():
    # rows/ (5) + the smallest group (1) + the offset (8).
    with pytest.raises(ValueError, match="at least 14 bytes, got 5"):
        decode_row_key(encode_row_key(RowKey(("1",), 0))[:5])
    with pytest.raises(ValueError, match="at least 14 bytes, got 13"):
        decode_row_key(b"rows/" + b"\x00" + b"\x00" * 7)
    assert decode_row_key(b"rows/" + b"\x00" + b"\x80" + b"\x00" * 7) == RowKey((), 0)


def test_codec_refusals_name_exactly_what_failed():
    with pytest.raises(
        ValueError, match="^a prefix of only 0xFF bytes has no successor$"
    ):
        prefix_successor(b"\xff")
    with pytest.raises(ValueError, match="^corrupt partition tuple: no arity byte$"):
        decode_group(b"")
    # A present component cut inside its u16 length.
    with pytest.raises(
        ValueError, match="^corrupt partition tuple component 0: length$"
    ):
        decode_group(b"\x01\x01\x00")
    with pytest.raises(
        ValueError, match="^corrupt partition tuple component 0: length$"
    ):
        decode_group(b"\x01\x01")
    # A length that runs past the end.
    with pytest.raises(
        ValueError, match="^corrupt partition tuple component 0: 5 bytes declared$"
    ):
        decode_group(b"\x01\x01\x00\x05ab")
    with pytest.raises(ValueError, match="^offset must fit in a signed 64-bit field"):
        RowKey((), INT64_MAX + 1)


def test_rows_range_covers_every_row_and_nothing_else():
    start, end = rows_range()
    assert (start, end) == (b"rows/", b"rows0")
    for key in [RowKey((), INT64_MIN), RowKey(("\U0010ffff",) * 3, INT64_MAX)]:
        assert start <= encode_row_key(key) < end
    assert not start <= FENCE_PROBE_KEY < end
    assert FENCE_PROBE_KEY == META_PREFIX + b"fence" == b"meta/fence"
    assert TABLE_UUID_KEY == META_PREFIX + b"table_uuid" == b"meta/table_uuid"
    assert not start <= TABLE_UUID_KEY < end
    assert ROWS_PREFIX == b"rows/"


@given(key=row_keys)
def test_rows_range_membership(key: RowKey):
    start, end = rows_range()
    assert start <= encode_row_key(key) < end


def test_fence_probe_value_golden_vectors():
    assert encode_fence_probe_value(0) == b"\x00" * 8
    assert encode_fence_probe_value(41) == b"\x00" * 7 + b"\x29"
    assert encode_fence_probe_value(-1) == b"\xff" * 8
    assert encode_fence_probe_value(INT64_MAX) == b"\x7f" + b"\xff" * 7
    assert encode_fence_probe_value(INT64_MIN) == b"\x80" + b"\x00" * 7
    with pytest.raises(ValueError, match="^offset must fit"):
        encode_fence_probe_value(INT64_MAX + 1)
    with pytest.raises(ValueError, match="^offset must fit"):
        encode_fence_probe_value(INT64_MIN - 1)


@pytest.mark.parametrize("pos", [1, 3, 7])
def test_decode_group_at_reads_from_its_offset(pos: int):
    """Pins the decoder's cursor arithmetic off position 0 (a mutant
    that assigns instead of advancing the cursor survives at pos 0)."""
    group: PartitionTuple = ("ab", None, "c")
    data = b"\xee" * pos + encode_group(group) + b"tail"
    decoded, end = keyspace._decode_group_at(data, pos)
    assert decoded == group
    assert data[end:] == b"tail"
