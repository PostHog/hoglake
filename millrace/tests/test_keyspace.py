"""Keyspace codecs: round-trips, byte order = logical order, golden vectors.

The golden vectors pin the byte-level contract: a future encoding change
fails loudly here instead of silently reshuffling every staged key in an
existing SlateDB. Each golden is written out byte by byte — no
implementation function builds an expectation.

The staging group is the destination's partition tuple (wire strings,
None for a null component); its encoding is prefix-free, so a range
built from ``prefix + group`` selects exactly one tuple's keys — the
property the "a tuple that is a byte prefix of another" tests pin.
"""

import hashlib
import uuid
from typing import Any

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from millrace.keyspace import (
    FENCE_PROBE_KEY,
    FLUSHED_PREFIX,
    IDEMPOTENCY_NAMESPACE,
    IDEMPOTENCY_NAMESPACE_DERIVATION,
    META_PREFIX,
    PREPARED_PREFIX,
    ROWS_PREFIX,
    SCHED_AGE_PREFIX,
    STATS_PREFIX,
    FlushedKey,
    PartitionTuple,
    PreparedKey,
    PreparedRequest,
    RowKey,
    SchedAgeKey,
    StatsValue,
    _prefix_successor,
    content_digest,
    decode_fence_probe_value,
    decode_group,
    decode_prepared_key,
    decode_prepared_value,
    decode_row_key,
    decode_sched_age_key,
    decode_stats_key,
    decode_stats_value,
    encode_fence_probe_value,
    encode_group,
    encode_prepared_value,
    encode_row_key,
    encode_sched_age_key,
    encode_stats_value,
    flushed_range,
    group_key_form,
    group_label,
    group_prepared_range,
    group_rows_prefix,
    group_rows_range,
    idempotency_key,
    prepared_key,
    row_in_flushed_range,
    sched_age_range_through,
    stats_key,
    window_rows_range,
)

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
UINT64_MAX = 2**64 - 1
#: A placeholder content digest where the test is not about content.
D0 = "0" * 64

#: Partition tuple components: any UTF-8-encodable string (unicode
#: included) or a null.
components = st.one_of(
    st.none(),
    st.text(alphabet=st.characters(exclude_categories=("Cs",)), max_size=12),
)
groups = st.lists(components, max_size=4).map(tuple)
timestamps = st.integers(min_value=INT64_MIN, max_value=INT64_MAX)
kafka_offsets = st.integers(min_value=INT64_MIN, max_value=INT64_MAX)
#: Content digests as the name line carries them (64 lowercase hex).
digests = st.binary(min_size=32, max_size=32).map(bytes.hex)
#: Kafka partitions (u32).
partitions = st.integers(min_value=0, max_value=2**32 - 1)
# UTF-8-encodable topics (no surrogates: the codecs refuse those loudly).
topics = st.text(
    alphabet=st.characters(exclude_categories=("Cs",)), min_size=1, max_size=32
)


def _fields(group: PartitionTuple) -> tuple[str, ...]:
    return tuple(f"f{i}" for i in range(len(group)))


def _flushed_key(group: PartitionTuple, a: int, b: int) -> FlushedKey:
    return FlushedKey(group, min(a, b), max(a, b), group_fields=_fields(group))


def _stats_value(staged: int, first: int, last: int, rows: int) -> StatsValue:
    return StatsValue(staged, min(first, last), max(first, last), rows)


def _logical(group: PartitionTuple) -> tuple[Any, ...]:
    """A tuple's logical sort key, independent of the codec: arity first,
    then per component null-before-present, then the UTF-8 length, then
    the bytes (the encoding's documented order)."""
    out: list[Any] = [len(group)]
    for c in group:
        if c is None:
            out.append((0,))
        else:
            raw = c.encode()
            out.append((1, len(raw), raw))
    return tuple(out)


def _request(**overrides: Any) -> PreparedRequest:
    base: dict[str, Any] = {
        "idempotency_key": "k",
        "body": b"{}",
        "persisted_at": 7,
        "topic": "events",
        "partition": 0,
        "group": ("42", None),
        "group_fields": ("team_id", "timestamp_month"),
        "row_count": 3,
        "digest": GOLDEN_DIGEST,
    }
    return PreparedRequest(**{**base, **overrides})


row_keys = st.builds(RowKey, group=groups, offset=kafka_offsets)
stats_values = st.builds(
    _stats_value,
    staged=st.integers(min_value=0, max_value=UINT64_MAX),
    first=timestamps,
    last=timestamps,
    rows=st.integers(min_value=0, max_value=UINT64_MAX),
)
sched_age_keys = st.builds(SchedAgeKey, first_staged_ts=timestamps, group=groups)
flushed_keys = st.builds(_flushed_key, group=groups, a=kafka_offsets, b=kafka_offsets)


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


def test_stats_golden_vectors():
    assert stats_key(("1",)) == b"stats/" + b"\x01\x01\x00\x01" + b"1"
    assert stats_key(()) == b"stats/\x00"
    assert encode_stats_value(
        StatsValue(staged_bytes=1, first_staged_ts=0, last_staged_ts=0, row_count=1)
    ) == (
        b"\x01"  # version
        + b"\x00" * 7
        + b"\x01"  # staged_bytes
        + b"\x00" * 8  # first_staged_ts (plain two's complement: values don't sort)
        + b"\x00" * 8  # last_staged_ts
        + b"\x00" * 7
        + b"\x01"  # row_count
    )
    # Negative timestamps in a value are plain two's complement.
    assert (
        encode_stats_value(
            StatsValue(
                staged_bytes=0, first_staged_ts=-1, last_staged_ts=-1, row_count=0
            )
        )
        == b"\x01" + b"\x00" * 8 + b"\xff" * 16 + b"\x00" * 8
    )


def test_sched_age_golden_vector():
    assert encode_sched_age_key(SchedAgeKey(first_staged_ts=0, group=("1",))) == (
        b"sched_age/" + b"\x80" + b"\x00" * 7 + b"\x01\x01\x00\x01" + b"1"
    )


def test_prepared_golden_vectors():
    assert prepared_key(("1",), 0) == (
        b"prepared/" + b"\x01\x01\x00\x01" + b"1" + b"\x80" + b"\x00" * 7
    )
    # The v6 value envelope, written out byte by byte — the on-disk
    # contract a future codec change must not shift. The tuple travels as
    # hoglake partition values: each field's name beside its wire string.
    assert encode_prepared_value(
        PreparedRequest(
            "key-abc",
            b"{}",
            persisted_at=42,
            topic="events",
            partition=0,
            group=("42", None),
            group_fields=("team_id", "ts_month"),
            row_count=258,
            digest="ab" * 32,
        )
    ) == (
        b"\x06"  # version
        + b"\x00\x07"
        + b"key-abc"
        + b"\x00\x06"
        + b"events"
        + b"\x00\x00\x00\x00"  # partition
        + b"\x02"  # arity
        + b"\x00\x07team_id"
        + b"\x01\x00\x0242"
        + b"\x00\x08ts_month"
        + b"\x00"  # null
        + b"\x00\x00\x00\x02"
        + b"{}"
        + b"\x00" * 7
        + b"\x2a"  # persisted_at = 42
        + b"\x00" * 6
        + b"\x01\x02"  # row_count = 258
        + b"\xab" * 32  # digest, raw
    )


#: The content digest of the golden window: rows (0, b"{}") and (41, b"x").
GOLDEN_DIGEST = "c1d2da607246399fad6b6c879cf0afc07aee5dd1183c8306c54442e106c7bfb5"


def test_content_digest_golden_vectors():
    # SHA-256 over offset (i64 BE) || length (u64 BE) || payload per row,
    # offset order — written out byte by byte for the golden.
    assert content_digest([]) == (
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )
    raw = (
        b"\x00" * 8
        + b"\x00" * 7
        + b"\x02"
        + b"{}"
        + b"\x00" * 7
        + b"\x29"
        + b"\x00" * 7
        + b"\x01"
        + b"x"
    )
    assert content_digest([(0, b"{}"), (41, b"x")]) == hashlib.sha256(raw).hexdigest()
    assert content_digest([(0, b"{}"), (41, b"x")]) == GOLDEN_DIGEST
    # a negative offset is two's complement (not offset binary)
    assert (
        content_digest([(-1, b"")])
        == hashlib.sha256(b"\xff" * 8 + b"\x00" * 8).hexdigest()
    )


def test_content_digest_is_unchanged_across_its_1_mib_update_chunks():
    # Framed rows are hashed in 1 MiB buffered updates (so hashlib runs
    # with the GIL released); rows straddling, filling and overshooting a
    # chunk boundary hash exactly like the one-shot framing.
    mib = 1 << 20
    for sizes in ([mib - 16], [mib - 15], [600_000, 600_000, 10], [3 * mib], [0, mib]):
        rows = [(i, bytes([i % 251]) * n) for i, n in enumerate(sizes)]
        framed = b"".join(
            o.to_bytes(8, "big", signed=True) + len(p).to_bytes(8, "big") + p
            for o, p in rows
        )
        assert content_digest(rows) == hashlib.sha256(framed).hexdigest()


def test_content_digest_framing_is_injective():
    # The length field keeps a payload's bytes from posing as the next
    # row's offset: these two sequences concatenate to the same
    # offset||payload bytes without it.
    one = [(0, b"ab" + (1).to_bytes(8, "big") + b"cd")]
    two = [(0, b"ab"), (1, b"cd")]
    assert content_digest(one) != content_digest(two)
    # Order, offsets and payloads all matter.
    assert content_digest([(0, b"a"), (1, b"b")]) != content_digest(
        [(1, b"b"), (0, b"a")]
    )
    assert content_digest([(0, b"a")]) != content_digest([(1, b"a")])
    assert content_digest([(0, b"a")]) != content_digest([(0, b"b")])
    assert content_digest([(0, b"")]) != content_digest([])
    with pytest.raises(ValueError, match="offset"):
        content_digest([(INT64_MAX + 1, b"")])
    with pytest.raises(ValueError, match="offset"):
        content_digest([(INT64_MIN - 1, b"")])
    assert (
        content_digest([(INT64_MIN, b""), (INT64_MAX, b"")])
        == hashlib.sha256(
            b"\x80" + b"\x00" * 15 + b"\x7f" + b"\xff" * 7 + b"\x00" * 8
        ).hexdigest()
    )


@given(rows=st.lists(st.tuples(kafka_offsets, st.binary(max_size=40)), max_size=8))
def test_content_digest_matches_its_documented_framing(
    rows: list[tuple[int, bytes]],
):
    framed = b"".join(
        o.to_bytes(8, "big", signed=True) + len(p).to_bytes(8, "big") + p
        for o, p in rows
    )
    assert content_digest(rows) == hashlib.sha256(framed).hexdigest()


def test_idempotency_key_golden_vector():
    # The derivation: uuid5(IDEMPOTENCY_NAMESPACE, name) over the
    # canonical v3 name line, the group under POSITIONAL names and the
    # window's content digest ("…:group=k0=42/k1=681:offsets=0-41:
    # digest=<hex>"). Pinned so a derivation change fails loudly.
    assert (
        idempotency_key(
            "12345678-1234-5678-1234-567812345678",
            "events",
            0,
            ("42", "681"),
            0,
            41,
            GOLDEN_DIGEST,
        )
        == "3d7cce29-5e52-5c1a-9569-236d242a3b02"
    )
    # The unpartitioned table's empty tuple renders as an empty group.
    assert (
        idempotency_key(
            "12345678-1234-5678-1234-567812345678",
            "events",
            0,
            (),
            0,
            41,
            GOLDEN_DIGEST,
        )
        == "cd679d76-97f5-529a-a84d-2724a00a59ee"
    )


def test_idempotency_namespace_matches_its_documented_derivation():
    assert IDEMPOTENCY_NAMESPACE == uuid.uuid5(
        uuid.NAMESPACE_URL, IDEMPOTENCY_NAMESPACE_DERIVATION
    )


# -- row keys: properties -----------------------------------------------------


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
        start, end = group_prepared_range(shorter)
        assert not start <= prepared_key(longer, off) < end
        assert start <= prepared_key(shorter, off) < end
    # The range the settle deletes is exact too, at both offset extremes.
    for first, last in [(0, 10), (INT64_MIN, INT64_MAX)]:
        window = FlushedKey(shorter, first, last, group_fields=_fields(shorter))
        start, end = window_rows_range(window)
        for off in [first, 5, last]:
            assert not start <= encode_row_key(RowKey(longer, off)) < end


def test_key_functions_validate_offsets():
    with pytest.raises(ValueError, match="first_offset"):
        prepared_key(("1",), INT64_MAX + 1)
    with pytest.raises(ValueError, match="first_offset"):
        idempotency_key(TABLE, "events", 0, (), INT64_MAX + 1, INT64_MAX + 1, D0)
    with pytest.raises(ValueError, match="last_offset"):
        idempotency_key(TABLE, "events", 0, (), 0, INT64_MAX + 1, D0)


def test_prefix_successor_strips_only_trailing_ff():
    assert _prefix_successor(b"rows/") == b"rows0"
    assert _prefix_successor(b"a\xff\xff") == b"b"
    assert _prefix_successor(b"aX") == b"aY"
    assert _prefix_successor(b"\x00") == b"\x01"
    for bad in (b"\xff", b"\xff\xff"):
        with pytest.raises(ValueError, match="no successor"):
            _prefix_successor(bad)


def test_group_rows_range_end_is_the_exact_prefix_successor():
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


# -- stats/ -------------------------------------------------------------------


@given(value=stats_values)
def test_stats_value_round_trip(value: StatsValue):
    assert decode_stats_value(encode_stats_value(value)) == value


@given(group=groups)
def test_stats_key_round_trip(group: PartitionTuple):
    assert decode_stats_key(stats_key(group)) == group


def test_stats_value_rejects_bad_version_and_length():
    good = encode_stats_value(StatsValue(0, 0, 0, 0))
    assert len(good) == 33  # 1 version byte + 4 fixed-width 8-byte fields
    for cut in range(len(good)):
        with pytest.raises(ValueError, match="expected 33 bytes"):
            decode_stats_value(good[:cut])
    with pytest.raises(ValueError, match="expected 33 bytes"):
        decode_stats_value(good + b"\x00")
    with pytest.raises(ValueError, match="unknown stats value version 2"):
        decode_stats_value(b"\x02" + good[1:])


def test_stats_value_validation():
    with pytest.raises(ValueError, match="staged_bytes"):
        StatsValue(staged_bytes=-1, first_staged_ts=0, last_staged_ts=0, row_count=0)
    with pytest.raises(ValueError, match="first_staged_ts"):
        StatsValue(staged_bytes=0, first_staged_ts=1, last_staged_ts=0, row_count=0)
    with pytest.raises(ValueError, match="row_count"):
        StatsValue(staged_bytes=0, first_staged_ts=0, last_staged_ts=0, row_count=-1)


def test_stats_value_timestamp_field_bounds():
    # The int64 range checks on the timestamp fields themselves, both
    # directions: a corrupted stats/ value must fail loudly, naming the
    # field. (first > last is covered by test_stats_value_validation.)
    for bad in (INT64_MAX + 1, INT64_MIN - 1):
        with pytest.raises(ValueError, match="first_staged_ts"):
            StatsValue(
                staged_bytes=0, first_staged_ts=bad, last_staged_ts=0, row_count=0
            )
        with pytest.raises(ValueError, match="last_staged_ts"):
            StatsValue(
                staged_bytes=0, first_staged_ts=0, last_staged_ts=bad, row_count=0
            )


# Directed field-boundary round-trips: a 7-byte slice or a signedness slip
# in the decoder is invisible to values whose top byte is mere sign
# extension, and hypothesis need not draw an extreme for every field.
UINT_FIELD_EXTREMES = [0, 1, 2**56, 2**63, UINT64_MAX]
TS_FIELD_EXTREMES = [INT64_MIN, -(2**55) - 1, -1, 0, 1, 2**55, INT64_MAX]


@pytest.mark.parametrize("staged", UINT_FIELD_EXTREMES)
@pytest.mark.parametrize("rows", UINT_FIELD_EXTREMES)
def test_stats_value_round_trip_at_int_field_boundaries(staged: int, rows: int):
    value = StatsValue(
        staged_bytes=staged, first_staged_ts=0, last_staged_ts=0, row_count=rows
    )
    assert decode_stats_value(encode_stats_value(value)) == value


@pytest.mark.parametrize("ts", TS_FIELD_EXTREMES)
def test_stats_value_round_trip_at_ts_field_boundaries(ts: int):
    value = StatsValue(0, min(ts, 0), max(ts, 0), 0)
    assert decode_stats_value(encode_stats_value(value)) == value


# -- sched_age/ ---------------------------------------------------------------


@given(key=sched_age_keys)
def test_sched_age_key_round_trip(key: SchedAgeKey):
    assert decode_sched_age_key(encode_sched_age_key(key)) == key


def test_sched_age_key_validation():
    with pytest.raises(ValueError, match="first_staged_ts"):
        SchedAgeKey(first_staged_ts=INT64_MAX + 1, group=())


def test_sched_age_range_through_validates_cutoff():
    with pytest.raises(ValueError, match="cutoff_ts"):
        sched_age_range_through(INT64_MAX + 1)


@given(keys=st.lists(sched_age_keys, max_size=60))
def test_sched_age_byte_order_is_age_order(keys: list[SchedAgeKey]):
    by_bytes = sorted(keys, key=encode_sched_age_key)
    by_value = sorted(keys, key=lambda k: (k.first_staged_ts, _logical(k.group)))
    assert [encode_sched_age_key(k) for k in by_bytes] == [
        encode_sched_age_key(k) for k in by_value
    ]


@given(key=sched_age_keys, cutoff=timestamps)
def test_sched_age_range_through_membership(key: SchedAgeKey, cutoff: int):
    start, end = sched_age_range_through(cutoff)
    encoded = encode_sched_age_key(key)
    assert (start <= encoded < end) == (key.first_staged_ts <= cutoff)


@pytest.mark.parametrize("cutoff", [INT64_MIN, -1, 0, 41, INT64_MAX - 1])
def test_sched_age_range_through_boundary(cutoff: int):
    # Directed boundaries (the membership property's stream need not draw
    # them): an entry AT the cutoff is in, one past it is out, and the end
    # is exactly the next instant's prefix — at INT64_MAX - 1 too, the
    # last cutoff that takes the arithmetic arm.
    start, end = sched_age_range_through(cutoff)
    assert start == SCHED_AGE_PREFIX
    assert end == SCHED_AGE_PREFIX + (cutoff + 1 + 2**63).to_bytes(8, "big")
    at = encode_sched_age_key(SchedAgeKey(first_staged_ts=cutoff, group=()))
    at_widest = encode_sched_age_key(
        SchedAgeKey(first_staged_ts=cutoff, group=("\U0010ffff",) * 4)
    )
    past = encode_sched_age_key(SchedAgeKey(first_staged_ts=cutoff + 1, group=()))
    assert start <= at < end
    assert start <= at_widest < end
    assert not start <= past < end


def test_sched_age_range_through_covers_everything_at_int64_max():
    start, end = sched_age_range_through(INT64_MAX)
    assert start == SCHED_AGE_PREFIX
    assert end == b"sched_age0"  # prefix successor: last byte incremented
    oldest = encode_sched_age_key(SchedAgeKey(first_staged_ts=INT64_MIN, group=()))
    newest = encode_sched_age_key(
        SchedAgeKey(first_staged_ts=INT64_MAX, group=("\U0010ffff",) * 4)
    )
    assert start <= oldest < end
    assert start <= newest < end


# -- group-key decodes -------------------------------------------------------


def test_key_decodes_name_their_minimum_length():
    # Each key decoder refuses a key too short to hold its fixed fields
    # AND a group's arity byte, by length — before parsing a group.
    with pytest.raises(ValueError, match="at least 7 bytes, got 6"):
        decode_stats_key(STATS_PREFIX)
    with pytest.raises(ValueError, match="at least 19 bytes, got 18"):
        decode_sched_age_key(SCHED_AGE_PREFIX + b"\x80" + b"\x00" * 7)
    with pytest.raises(ValueError, match="at least 18 bytes, got 17"):
        decode_prepared_key(PREPARED_PREFIX + b"\x80" + b"\x00" * 7)
    with pytest.raises(ValueError, match="at least 14 bytes, got 13"):
        decode_row_key(ROWS_PREFIX + b"\x80" + b"\x00" * 7)


def test_group_key_decodes_refuse_a_corrupt_group():
    with pytest.raises(ValueError, match="corrupt partition tuple"):
        decode_stats_key(STATS_PREFIX + b"\x02\x00")  # arity 2, one component
    with pytest.raises(ValueError):
        decode_stats_key(STATS_PREFIX)  # no group at all
    with pytest.raises(ValueError, match="corrupt partition tuple"):
        decode_sched_age_key(SCHED_AGE_PREFIX + b"\x80" + b"\x00" * 7 + b"\x01\x09")
    with pytest.raises(ValueError, match="corrupt partition tuple"):
        decode_prepared_key(PREPARED_PREFIX + b"\x01" + b"\x80" + b"\x00" * 7)


def test_varlen_fields_accept_exactly_the_u16_maximum():
    # 65535 fits the length field; 65536 does not. The width is
    # load-bearing: a `>` flipped to `>=` must red here.
    _request(idempotency_key="k" * (2**16 - 1))
    with pytest.raises(ValueError, match="idempotency key too long"):
        _request(idempotency_key="k" * (2**16))


class _FakeLength:
    """A ``bytes`` stand-in whose ``len`` is faked: the u32 body-length
    boundary sits at 4 GiB, which no unit test allocates. The check under
    test reads only ``len(body)``, so duck-typing pins it exactly."""

    def __init__(self, n: int):
        self._n = n

    def __len__(self) -> int:
        return self._n


def test_prepared_body_accepts_exactly_the_u32_maximum():
    # 2**32 - 1 fits the body_len(u32) field; 2**32 does not. The width
    # is load-bearing: a `>` flipped to `>=` must red here.
    _request(body=_FakeLength(2**32 - 1))
    with pytest.raises(ValueError, match="prepared body too long"):
        _request(body=_FakeLength(2**32))


# -- prepared/ ----------------------------------------------------------------


@given(group=groups, first=kafka_offsets)
def test_prepared_key_round_trip(group: PartitionTuple, first: int):
    assert decode_prepared_key(prepared_key(group, first)) == PreparedKey(group, first)


def test_group_prepared_range_selects_exactly_the_group():
    # The flush's outstanding-entry gate scans this range: it must cover
    # every prepared/ key of the tuple and no other tuple's.
    start, end = group_prepared_range(("7",))
    assert start == PREPARED_PREFIX + b"\x01\x01\x00\x017"
    assert end == PREPARED_PREFIX + b"\x01\x01\x00\x018"
    assert start <= prepared_key(("7",), 0) < end
    assert start <= prepared_key(("7",), INT64_MAX) < end
    # first_offset is offset-binary: negative offsets sort below 0 and
    # still belong to the tuple's range.
    assert start <= prepared_key(("7",), INT64_MIN) < end
    assert not start <= prepared_key(("6",), 0) < end
    assert not start <= prepared_key(("8",), 0) < end
    assert not start <= prepared_key(("7", None), 0) < end
    assert not start <= prepared_key((), 0) < end
    assert not start <= b"prepared" < end  # one byte short of the prefix


@given(
    keys=st.lists(
        st.builds(PreparedKey, group=groups, first_offset=kafka_offsets), max_size=60
    )
)
def test_prepared_keys_sort_by_group_then_first_offset(keys: list[PreparedKey]):
    by_bytes = sorted(keys, key=lambda k: prepared_key(k.group, k.first_offset))
    by_value = sorted(keys, key=lambda k: (_logical(k.group), k.first_offset))
    assert [prepared_key(k.group, k.first_offset) for k in by_bytes] == [
        prepared_key(k.group, k.first_offset) for k in by_value
    ]


@given(
    key=st.text(alphabet=st.characters(exclude_categories=("Cs",)), max_size=64),
    body=st.binary(max_size=256),
    persisted_at=st.integers(min_value=0, max_value=UINT64_MAX),
    topic=topics,
    partition=partitions,
    group=groups,
    row_count=st.integers(min_value=0, max_value=UINT64_MAX),
    digest=digests,
)
def test_prepared_value_round_trip(
    key: str,
    body: bytes,
    persisted_at: int,
    topic: str,
    partition: int,
    group: PartitionTuple,
    row_count: int,
    digest: str,
):
    request = PreparedRequest(
        idempotency_key=key,
        body=body,
        persisted_at=persisted_at,
        topic=topic,
        partition=partition,
        group=group,
        group_fields=_fields(group),
        row_count=row_count,
        digest=digest,
    )
    assert decode_prepared_value(encode_prepared_value(request)) == request


@pytest.mark.parametrize("at", [0, 1, 2**63 - 1, 2**63, UINT64_MAX])
def test_prepared_persisted_at_round_trips_across_the_sign_bit(at: int):
    # persisted_at and row_count are u64: values at and above 2**63 must
    # decode as the same unsigned value (a signed decode reads them
    # negative and the dataclass refuses them). Each is pinned against a
    # different value of the other, so a swapped tail reds too.
    request = _request(persisted_at=at, row_count=UINT64_MAX - at)
    encoded = encode_prepared_value(request)
    assert encoded[-48:-40] == at.to_bytes(8, "big")
    assert encoded[-40:-32] == (UINT64_MAX - at).to_bytes(8, "big")
    assert encoded[-32:] == bytes.fromhex(GOLDEN_DIGEST)
    decoded = decode_prepared_value(encoded)
    assert (decoded.persisted_at, decoded.row_count) == (at, UINT64_MAX - at)


def test_prepared_request_validation():
    with pytest.raises(ValueError, match="persisted_at"):
        _request(persisted_at=2**64)  # u64 overflow
    _request(persisted_at=2**64 - 1)  # the boundary fits
    with pytest.raises(ValueError, match="row_count"):
        _request(row_count=2**64)
    with pytest.raises(ValueError, match="row_count"):
        _request(row_count=-1)
    _request(row_count=2**64 - 1)
    for bad in ("", "0" * 63, "0" * 65, "A" * 64, "g" * 64):
        with pytest.raises(ValueError, match="digest must be 64 lowercase hex"):
            _request(digest=bad)
    with pytest.raises(ValueError, match="topic"):
        _request(topic="")
    with pytest.raises(ValueError, match="partition"):
        _request(partition=-1)
    with pytest.raises(ValueError, match="partition"):
        _request(partition=2**32)
    with pytest.raises(ValueError, match="1 field name"):
        _request(group=("1", "2"), group_fields=("a",))
    with pytest.raises(ValueError, match="non-empty"):
        _request(group=("1",), group_fields=("",))


@given(
    body=st.binary(max_size=128),
    at=st.integers(min_value=0, max_value=UINT64_MAX),
    topic=topics,
    partition=partitions,
    group=groups,
)
def test_prepared_value_rejects_strict_prefixes(
    body: bytes, at: int, topic: str, partition: int, group: PartitionTuple
):
    encoded = encode_prepared_value(
        _request(
            body=body,
            persisted_at=at,
            topic=topic,
            partition=partition,
            group=group,
            group_fields=_fields(group),
        )
    )
    for cut in range(len(encoded)):
        with pytest.raises(ValueError):
            decode_prepared_value(encoded[:cut])


def test_prepared_value_truncation_errors_name_the_failed_parse_stage():
    # v6 layout for key "some-key", topic "events", group ("42",) named
    # ("t",): version(1) key_len(2) key(8) topic_len(2) topic(6)
    # partition(4) arity(1) name_len(2) name(1) tag(1) value_len(2)
    # value(2) body_len(4) body(2) persisted_at(8) row_count(8)
    # digest(32) — 86 bytes total; the tail's cuts report the
    # body-length mismatch (a cut digest included).
    encoded = encode_prepared_value(
        _request(idempotency_key="some-key", group=("42",), group_fields=("t",))
    )
    expect = (
        ["empty"]
        + ["key: length"] * 2
        + ["key: 8 bytes declared"] * 8
        + ["topic: length"] * 2
        + ["topic: 6 bytes declared"] * 6
        + ["partition / arity"] * 5
        + ["field 0 name: length"] * 2
        + ["field 0 name: 1 bytes declared"]
        + ["field 0 tag"]
        + ["field 0 value: length"] * 2
        + ["field 0 value: 2 bytes declared"] * 2
        + ["body length$"] * 4
        + ["length mismatch"] * 50  # body (2) + persisted_at + row_count + digest
    )
    assert len(encoded) == 86
    assert len(encoded) == len(expect)
    for cut, stage in enumerate(expect):
        with pytest.raises(ValueError, match=stage):
            decode_prepared_value(encoded[:cut])


def test_prepared_value_encode_names_an_oversized_partition_field():
    # Field names and values are bounded by their u16 length fields at
    # encode (the dataclass checks only arity and non-emptiness), and the
    # refusal names which one overflowed.
    with pytest.raises(ValueError, match="^partition field name does not fit"):
        encode_prepared_value(_request(group=("1",), group_fields=("n" * 2**16,)))
    with pytest.raises(ValueError, match="^partition value does not fit"):
        encode_prepared_value(_request(group=("v" * 2**16,), group_fields=("n",)))
    request = _request(group=("v" * (2**16 - 1),), group_fields=("n" * (2**16 - 1),))
    assert decode_prepared_value(encode_prepared_value(request)) == request


def test_prepared_value_round_trip_with_multi_byte_lengths():
    # 300-byte key, topic, field name and value: every u16 length's high
    # byte is nonzero, so a decoder reading only its low byte misaligns
    # and must fail loudly.
    request = _request(
        idempotency_key="k" * 300,
        topic="t" * 300,
        group=("v" * 300,),
        group_fields=("n" * 300,),
    )
    assert decode_prepared_value(encode_prepared_value(request)) == request


def test_prepared_value_refuses_every_other_version_and_trailing_bytes():
    good = encode_prepared_value(_request(body=b""))
    for version in (0, 1, 2, 3, 4, 5, 7):
        with pytest.raises(
            ValueError, match=f"unknown prepared value version {version}"
        ):
            decode_prepared_value(bytes([version]) + good[1:])
    with pytest.raises(ValueError, match="field says 0, value holds 1$"):
        decode_prepared_value(good + b"\x00")
    corrupt = bytearray(good)
    # The null tag of the second field (after "\x00\x0ftimestamp_month").
    tag_at = good.index(b"timestamp_month") + len("timestamp_month")
    corrupt[tag_at] = 0x07
    with pytest.raises(ValueError, match="field 1 has tag 0x07"):
        decode_prepared_value(bytes(corrupt))


# -- flushed/ (LEGACY prefix + the settlement window) ----------------------------


def test_flushed_range_covers_the_whole_legacy_prefix():
    # Nothing writes flushed/ any more; recover()'s collector scans
    # exactly this range. Any key under the prefix — whatever its bytes —
    # is inside it.
    start, end = flushed_range()
    assert start == FLUSHED_PREFIX
    assert start <= FLUSHED_PREFIX < end
    assert start <= FLUSHED_PREFIX + b"\x00" * 24 < end
    assert start <= FLUSHED_PREFIX + b"\xff" * 24 < end
    assert not (start <= b"flushee/" < end)


def test_flushed_key_validation():
    with pytest.raises(ValueError, match="first_offset 42 > last_offset 41"):
        FlushedKey(("1",), 42, 41, group_fields=("a",))
    with pytest.raises(ValueError, match="first_offset"):
        FlushedKey((), INT64_MAX + 1, INT64_MAX + 1, group_fields=())
    with pytest.raises(ValueError, match="last_offset"):
        FlushedKey((), 0, INT64_MAX + 1, group_fields=())
    with pytest.raises(ValueError, match="0 field name"):
        FlushedKey(("1",), 0, 1, group_fields=())


def test_flushed_key_carries_the_tuple_as_hoglake_partition_values():
    window = FlushedKey(
        ("42", "681"), 0, 9, group_fields=("team_id", "timestamp_month")
    )
    assert window.label == "team_id=42/timestamp_month=681"


@given(flushed=flushed_keys, row=row_keys)
def test_flushed_window_selects_exactly_its_own_rows(flushed: FlushedKey, row: RowKey):
    # The settlement deletes every key in window_rows_range: exactly the
    # tuple's rows with an offset inside the window — row keys sort by
    # (group, offset), so the window is one contiguous byte range.
    start, end = window_rows_range(flushed)
    in_range = start <= encode_row_key(row) < end
    assert in_range == (
        row.group == flushed.group
        and flushed.first_offset <= row.offset <= flushed.last_offset
    )
    assert row_in_flushed_range(row, flushed) == (
        flushed.first_offset <= row.offset <= flushed.last_offset
    )


@given(flushed=flushed_keys)
def test_a_settlement_spares_younger_and_older_rows_of_the_same_group(
    flushed: FlushedKey,
):
    start, end = window_rows_range(flushed)
    if flushed.last_offset < INT64_MAX:
        younger = RowKey(flushed.group, flushed.last_offset + 1)
        assert not start <= encode_row_key(younger) < end
        assert not row_in_flushed_range(younger, flushed)
    if flushed.first_offset > INT64_MIN:
        older = RowKey(flushed.group, flushed.first_offset - 1)
        assert not start <= encode_row_key(older) < end
        assert not row_in_flushed_range(older, flushed)


def test_window_rows_range_golden_and_boundaries():
    f = FlushedKey(("7",), 10, 20, group_fields=("a",))
    prefix = b"rows/\x01\x01\x00\x017"
    assert window_rows_range(f) == (
        prefix + b"\x80" + b"\x00" * 6 + b"\x0a",
        prefix + b"\x80" + b"\x00" * 6 + b"\x15",  # last + 1 = 21
    )
    start, end = window_rows_range(f)
    for offset, inside in [(9, False), (10, True), (20, True), (21, False)]:
        assert (start <= encode_row_key(RowKey(("7",), offset)) < end) == inside
        assert row_in_flushed_range(RowKey(("7",), offset), f) == inside
    # Another tuple's row with an in-window offset is outside the range.
    assert not start <= encode_row_key(RowKey(("8",), 15)) < end
    assert not start <= encode_row_key(RowKey(("7", None), 15)) < end
    # last_offset == INT64_MAX has no successor offset: the end is the
    # group's prefix successor, and the top row is inside.
    top = FlushedKey(("7",), INT64_MAX - 1, INT64_MAX, group_fields=("a",))
    start, end = window_rows_range(top)
    assert end == b"rows/\x01\x01\x00\x018"
    assert start <= encode_row_key(RowKey(("7",), INT64_MAX)) < end
    assert start <= encode_row_key(RowKey(("7",), INT64_MAX - 1)) < end
    assert not start <= encode_row_key(RowKey(("7",), INT64_MAX - 2)) < end
    whole = FlushedKey((), INT64_MIN, INT64_MAX, group_fields=())
    assert window_rows_range(whole) == (
        b"rows/\x00" + b"\x00" * 8,
        group_rows_range(())[1],
    )


# -- meta/ (the consumer's fence probe) -------------------------------------------


def test_fence_probe_golden_vector_and_round_trip():
    assert FENCE_PROBE_KEY == b"meta/fence"
    assert encode_fence_probe_value(41) == b"\x01" + b"\x00" * 7 + b"\x29"
    assert encode_fence_probe_value(-1) == b"\x01" + b"\xff" * 8
    for offset in [INT64_MIN, -1, 0, 41, 2**32, INT64_MAX]:
        assert decode_fence_probe_value(encode_fence_probe_value(offset)) == offset


def test_fence_probe_value_validation():
    with pytest.raises(ValueError, match="offset"):
        encode_fence_probe_value(INT64_MAX + 1)
    with pytest.raises(ValueError, match="offset"):
        encode_fence_probe_value(INT64_MIN - 1)
    good = encode_fence_probe_value(7)
    with pytest.raises(ValueError, match="expected 9 bytes, got 8"):
        decode_fence_probe_value(good[:-1])
    with pytest.raises(ValueError, match="expected 9 bytes, got 10"):
        decode_fence_probe_value(good + b"\x00")
    with pytest.raises(ValueError, match="unknown fence probe value version 2"):
        decode_fence_probe_value(b"\x02" + good[1:])


def test_the_fence_probe_key_is_outside_every_row_and_index_range():
    # Not a row: no scan the flush, the planner or the gauges run can see
    # it.
    for start, end in [
        group_rows_range(()),
        group_rows_range(("\U0010ffff",) * 4),
        sched_age_range_through(INT64_MAX),
        flushed_range(),
        group_prepared_range(()),
    ]:
        assert not start <= FENCE_PROBE_KEY < end
    assert not FENCE_PROBE_KEY.startswith(STATS_PREFIX)
    assert not FENCE_PROBE_KEY.startswith(ROWS_PREFIX)


# -- idempotency key ----------------------------------------------------------

TABLE = "12345678-1234-5678-1234-567812345678"
uuid_strings = st.uuids().map(str)


@st.composite
def _first_last(draw: Any) -> tuple[int, int]:
    a = draw(kafka_offsets)
    b = draw(kafka_offsets)
    return min(a, b), max(a, b)


@given(
    table=uuid_strings,
    topic=topics,
    partition=partitions,
    group=groups,
    window=_first_last(),
    digest=digests,
)
def test_idempotency_key_matches_the_documented_derivation(
    table: str,
    topic: str,
    partition: int,
    group: PartitionTuple,
    window: tuple[int, int],
    digest: str,
):
    # The derivation FORMULA, pinned across arbitrary inputs (the golden
    # vector pins one concrete output): uuid5 over the millrace namespace
    # and the canonical "millrace:v3:" name line, table canonicalized,
    # the group under positional names, percent-encoded, the content
    # digest last.
    first, last = window
    fields = tuple(f"k{i}" for i in range(len(group)))
    name = (
        f"millrace:v3:table={uuid.UUID(table)}:topic={topic}"
        f":partition={partition}:group={group_label(fields, group)}"
        f":offsets={first}-{last}:digest={digest}"
    )
    assert idempotency_key(table, topic, partition, group, first, last, digest) == str(
        uuid.uuid5(IDEMPOTENCY_NAMESPACE, name)
    )


@given(
    table=uuid_strings,
    topic=topics,
    partition=partitions,
    group=groups,
    window=_first_last(),
)
def test_idempotency_key_is_sensitive_to_every_input_field(
    table: str,
    topic: str,
    partition: int,
    group: PartitionTuple,
    window: tuple[int, int],
):
    first, last = window
    assume(first < last)  # a degenerate single-offset range is uninteresting
    base = idempotency_key(table, topic, partition, group, first, last, D0)
    uuid.UUID(base)  # a well-formed UUID string
    assert base == idempotency_key(table, topic, partition, group, first, last, D0)
    other_table = "00000000-0000-0000-0000-000000000000"
    if table == other_table:
        other_table = "00000000-0000-0000-0000-000000000001"
    assert (
        idempotency_key(other_table, topic, partition, group, first, last, D0) != base
    )
    # TOPIC and PARTITION are derivation inputs: offsets are per partition,
    # so two partitions flushing one tuple over the same range must never
    # share a key (nor the object URIs derived from it).
    other_topic = f"{topic}-other"
    assert (
        idempotency_key(table, other_topic, partition, group, first, last, D0) != base
    )
    other_partition = partition + 1 if partition < 2**32 - 1 else partition - 1
    assert (
        idempotency_key(table, topic, other_partition, group, first, last, D0) != base
    )
    other_group = (*group, "x")
    assert (
        idempotency_key(table, topic, partition, other_group, first, last, D0) != base
    )
    assert idempotency_key(table, topic, partition, group, first + 1, last, D0) != base
    assert idempotency_key(table, topic, partition, group, first, last - 1, D0) != base
    # The CONTENT is an input: different rows under the same bounds never
    # share a key (so never one receipt).
    assert (
        idempotency_key(table, topic, partition, group, first, last, "1" + D0[1:])
        != base
    )


@given(
    table=uuid_strings,
    topic_a=topics,
    topic_b=topics,
    partition_a=partitions,
    partition_b=partitions,
    group=groups,
    window=_first_last(),
)
def test_identical_window_on_two_partitions_yields_different_keys(
    table: str,
    topic_a: str,
    topic_b: str,
    partition_a: int,
    partition_b: int,
    group: PartitionTuple,
    window: tuple[int, int],
):
    """Identical (table, tuple, first, last) — but different (topic,
    partition) — MUST yield different keys: two Kafka partitions flushing
    one tuple over the same offset range would otherwise share the key
    and the object URIs derived from it."""
    assume((topic_a, partition_a) != (topic_b, partition_b))
    first, last = window
    one = idempotency_key(table, topic_a, partition_a, group, first, last, D0)
    two = idempotency_key(table, topic_b, partition_b, group, first, last, D0)
    assert one != two


def test_idempotency_key_new_form_names_the_group_positionally():
    # The v3 name line carries ``group=k0=…/k1=…``: the tuple's wire
    # strings under POSITIONAL names, percent-encoded, nulls bare — a
    # function of the staged tuple alone, so a spec change of the same
    # arity (a renamed source, month → day) between an upload and its
    # rebuild cannot move the key or the object names derived from it.
    for group, form in [
        (("42", "681"), "k0=42/k1=681"),
        ((None, "681"), "k0/k1=681"),
        (("a/b", None), "k0=a%2Fb/k1"),
        ((), ""),
    ]:
        assert group_key_form(group) == form
        name = (
            f"millrace:v3:table={TABLE}:topic=events:partition=3"
            f":group={form}:offsets=5-9:digest={D0}"
        )
        assert idempotency_key(TABLE, "events", 3, group, 5, 9, D0) == str(
            uuid.uuid5(IDEMPOTENCY_NAMESPACE, name)
        )


def test_prepared_value_varlen_fields_accept_exactly_the_u16_maximum():
    # 2**16 - 1 utf-8 bytes fits topic_len(u16); 2**16 does not. The
    # width is load-bearing (a `>` flipped to `>=` must red here), and
    # it is BYTES, not characters (a non-ASCII topic is longer encoded).
    topic = "t" * (2**16 - 1)
    _request(topic=topic)
    with pytest.raises(ValueError, match="u16"):
        _request(topic=topic + "t")
    # The idempotency derivation validates the same width (the envelope
    # must be able to carry what the key was derived from).
    idempotency_key(TABLE, topic, 0, (), 0, 41, D0)
    with pytest.raises(ValueError, match="u16"):
        idempotency_key(TABLE, topic + "t", 0, (), 0, 41, D0)


def test_partition_accepts_exactly_the_u32_maximum():
    # 2**32 - 1 fits partition(u32); 2**32 does not (a `<=` flipped to
    # `<` must red here — it refuses exactly the boundary value).
    idempotency_key(TABLE, "events", 2**32 - 1, (), 0, 41, D0)
    _request(partition=2**32 - 1)
    with pytest.raises(ValueError, match="partition"):
        idempotency_key(TABLE, "events", 2**32, (), 0, 41, D0)
    with pytest.raises(ValueError, match="partition"):
        _request(partition=2**32)


def test_idempotency_key_canonicalizes_the_table_uuid():
    lower = idempotency_key(
        "12345678-1234-5678-1234-567812345678", "events", 0, ("1",), 0, 41, D0
    )
    upper = idempotency_key(
        "12345678-1234-5678-1234-567812345678".upper(),
        "events",
        0,
        ("1",),
        0,
        41,
        D0,
    )
    assert lower == upper


def test_idempotency_key_does_not_canonicalize_the_topic():
    # Kafka topic names are case-sensitive; the derivation must be too.
    assert idempotency_key(TABLE, "Events", 0, (), 0, 41, D0) != idempotency_key(
        TABLE, "events", 0, (), 0, 41, D0
    )


def test_idempotency_key_refuses_a_malformed_digest():
    for bad in ("", "0" * 63, "0" * 65, "G" * 64, "A" * 64, " " + "0" * 63):
        with pytest.raises(ValueError, match="digest must be 64 lowercase hex"):
            idempotency_key(TABLE, "events", 0, (), 0, 41, bad)
    idempotency_key(TABLE, "events", 0, (), 0, 41, "a" * 64)


def test_idempotency_key_validation():
    with pytest.raises(ValueError):
        idempotency_key("not-a-uuid", "events", 0, (), 0, 41, D0)
    with pytest.raises(ValueError, match="topic"):
        idempotency_key(TABLE, "", 0, (), 0, 41, D0)  # empty topic
    with pytest.raises(ValueError, match="partition"):
        idempotency_key(TABLE, "events", -1, (), 0, 41, D0)  # negative partition
    with pytest.raises(ValueError, match="partition"):
        idempotency_key(TABLE, "events", 2**32, (), 0, 41, D0)  # u32 overflow
    with pytest.raises(ValueError, match="first_offset"):
        idempotency_key(TABLE, "events", 0, (), 42, 41, D0)  # inverted offset range


def test_prefixes_are_distinct():
    prefixes = [
        ROWS_PREFIX,
        STATS_PREFIX,
        SCHED_AGE_PREFIX,
        PREPARED_PREFIX,
        FLUSHED_PREFIX,
        META_PREFIX,
    ]
    assert len(set(prefixes)) == len(prefixes)
    # No prefix is a strict prefix of another: one family's keys never
    # fall inside another family's range scan.
    for a in prefixes:
        for b in prefixes:
            if a is not b:
                assert not a.startswith(b), (a, b)
    assert group_rows_prefix(()).startswith(ROWS_PREFIX)
