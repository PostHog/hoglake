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

import uuid
from typing import Any

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from millrace.keyspace import (
    FLUSHED_PREFIX,
    IDEMPOTENCY_NAMESPACE,
    IDEMPOTENCY_NAMESPACE_DERIVATION,
    OFFSETS_PREFIX,
    PREPARED_PREFIX,
    ROWS_PREFIX,
    SCHED_AGE_PREFIX,
    STATS_PREFIX,
    FlushedKey,
    OffsetRange,
    PartitionTuple,
    PreparedKey,
    PreparedRequest,
    RowKey,
    SchedAgeKey,
    StatsValue,
    _prefix_successor,
    decode_group,
    decode_offsets_key,
    decode_offsets_value,
    decode_prepared_key,
    decode_prepared_value,
    decode_row_key,
    decode_sched_age_key,
    decode_stats_key,
    decode_stats_value,
    encode_group,
    encode_offsets_value,
    encode_prepared_value,
    encode_row_key,
    encode_sched_age_key,
    encode_stats_value,
    flushed_range,
    flushed_rows_scan_range,
    group_key_form,
    group_label,
    group_prepared_range,
    group_rows_prefix,
    group_rows_range,
    idempotency_key,
    offsets_key,
    prepared_key,
    row_in_flushed_range,
    sched_age_range_through,
    stats_key,
)

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
UINT64_MAX = 2**64 - 1

#: Partition tuple components: any UTF-8-encodable string (unicode
#: included) or a null.
components = st.one_of(
    st.none(),
    st.text(alphabet=st.characters(exclude_categories=("Cs",)), max_size=12),
)
groups = st.lists(components, max_size=4).map(tuple)
timestamps = st.integers(min_value=INT64_MIN, max_value=INT64_MAX)
kafka_offsets = st.integers(min_value=INT64_MIN, max_value=INT64_MAX)
#: Kafka partitions (u32).
partitions = st.integers(min_value=0, max_value=2**32 - 1)
# UTF-8-encodable topics (no surrogates: OffsetRange refuses those loudly).
topics = st.text(
    alphabet=st.characters(exclude_categories=("Cs",)), min_size=1, max_size=32
)


def _fields(group: PartitionTuple) -> tuple[str, ...]:
    return tuple(f"f{i}" for i in range(len(group)))


def _offset_range(topic: str, partition: int, a: int, b: int) -> OffsetRange:
    return OffsetRange(topic, partition, min(a, b), max(a, b))


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
    }
    return PreparedRequest(**{**base, **overrides})


row_keys = st.builds(
    RowKey, group=groups, timestamp_us=timestamps, offset=kafka_offsets
)
offset_ranges = st.builds(
    _offset_range,
    topic=topics,
    partition=st.integers(min_value=0, max_value=2**32 - 1),
    a=kafka_offsets,
    b=kafka_offsets,
)
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
    with pytest.raises(ValueError, match="u16 length"):
        encode_group(("x" * 2**16,))
    encode_group(("x" * (2**16 - 1),))
    with pytest.raises(TypeError, match="wire strings"):
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
    # group ("1",), epoch 0, offset 0: zero timestamps map to 0x8000...
    # (the offset-binary midpoint), so byte order can be numeric order.
    assert encode_row_key(RowKey(group=("1",), timestamp_us=0, offset=0)) == (
        b"rows/"
        + b"\x01\x01\x00\x01"
        + b"1"
        + b"\x80"
        + b"\x00" * 7
        + b"\x80"
        + b"\x00" * 7
    )
    # The unpartitioned group and INT64_MIN: arity 0, then zero bytes —
    # the smallest encodable instant sorts before everything.
    assert encode_row_key(
        RowKey(group=(), timestamp_us=INT64_MIN, offset=INT64_MIN)
    ) == (b"rows/" + b"\x00" + b"\x00" * 16)
    # A null component and the all-ones corner.
    assert (
        encode_row_key(RowKey(group=(None,), timestamp_us=INT64_MAX, offset=INT64_MAX))
        == b"rows/" + b"\x01\x00" + b"\xff" * 16
    )
    # A negative instant: -1 sorts just below 0, and its bytes show it.
    assert encode_row_key(RowKey(group=("ab",), timestamp_us=-1, offset=1)) == (
        b"rows/"
        + b"\x01\x01\x00\x02ab"
        + b"\x7f\xff\xff\xff\xff\xff\xff\xff"
        + b"\x80\x00\x00\x00\x00\x00\x00\x01"
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


def test_offsets_golden_vector():
    assert offsets_key(("1",)) == b"offsets/" + b"\x01\x01\x00\x01" + b"1"
    assert encode_offsets_value([OffsetRange("events", 7, 0, 41)]) == (
        b"\x01"  # version
        + b"\x00\x01"  # count
        + b"\x00\x06"
        + b"events"
        + b"\x00\x00\x00\x07"  # partition
        + b"\x00" * 8  # first_offset
        + b"\x00" * 7
        + b"\x29"  # last_offset = 41
    )


def test_prepared_golden_vectors():
    assert prepared_key(("1",), 0) == (
        b"prepared/" + b"\x01\x01\x00\x01" + b"1" + b"\x80" + b"\x00" * 7
    )
    # The v4 value envelope, written out byte by byte — the on-disk
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
        )
    ) == (
        b"\x04"  # version
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
    )


def test_idempotency_key_golden_vector():
    # The derivation: uuid5(IDEMPOTENCY_NAMESPACE, name) over the
    # canonical name line, the group under POSITIONAL names
    # ("…:group=k0=42/k1=681:…"). Pinned so a derivation change fails
    # loudly.
    assert (
        idempotency_key(
            "12345678-1234-5678-1234-567812345678",
            "events",
            0,
            ("42", "681"),
            0,
            41,
        )
        == "8125f3fe-2e21-58fa-8146-44a1dd17fc3c"
    )
    # The unpartitioned table's empty tuple renders as an empty group.
    assert (
        idempotency_key("12345678-1234-5678-1234-567812345678", "events", 0, (), 0, 41)
        == "b4a9deaa-07f5-5aa2-80aa-02fee899326e"
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
    # Group, then timestamp, then offset.
    by_bytes = sorted(keys, key=encode_row_key)
    by_value = sorted(keys, key=lambda k: (_logical(k.group), k.timestamp_us, k.offset))
    assert [encode_row_key(k) for k in by_bytes] == [
        encode_row_key(k) for k in by_value
    ]


@given(keys=st.lists(row_keys, min_size=2, max_size=60))
def test_row_key_encoding_is_injective(keys: list[RowKey]):
    encoded = [encode_row_key(k) for k in keys]
    assert len(set(encoded)) == len(set(keys))


def test_row_key_negative_timestamps_sort_before_positive():
    instants = [INT64_MIN, -2, -1, 0, 1, 2, INT64_MAX]
    keys = [RowKey(group=("7",), timestamp_us=ts, offset=0) for ts in instants]
    by_bytes = sorted(keys, key=encode_row_key)
    assert [k.timestamp_us for k in by_bytes] == instants


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
    for ts, off in [(INT64_MIN, INT64_MIN), (0, 0), (INT64_MAX, INT64_MAX)]:
        row_long = encode_row_key(RowKey(longer, ts, off))
        row_short = encode_row_key(RowKey(shorter, ts, off))
        start, end = group_rows_range(shorter)
        assert not start <= row_long < end
        assert start <= row_short < end
        start, end = group_rows_range(longer)
        assert not start <= row_short < end
        assert start <= row_long < end
        start, end = group_prepared_range(shorter)
        assert not start <= prepared_key(longer, off) < end
        assert start <= prepared_key(shorter, off) < end
    # The selection the settle runs is exact too.
    window = FlushedKey(shorter, 0, 10, group_fields=_fields(shorter))
    start, end = flushed_rows_scan_range(window)
    assert not start <= encode_row_key(RowKey(longer, 0, 5)) < end


def test_key_functions_validate_offsets():
    with pytest.raises(ValueError, match="first_offset"):
        prepared_key(("1",), INT64_MAX + 1)
    with pytest.raises(ValueError, match="first_offset"):
        idempotency_key(TABLE, "events", 0, (), INT64_MAX + 1, INT64_MAX + 1)
    with pytest.raises(ValueError, match="last_offset"):
        idempotency_key(TABLE, "events", 0, (), 0, INT64_MAX + 1)


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
    top = encode_row_key(RowKey(("X",), INT64_MAX, INT64_MAX))
    assert group_rows_range(("X",))[0] <= top < group_rows_range(("X",))[1]
    assert group_rows_prefix(()) == b"rows/\x00"


def test_row_key_field_validation():
    with pytest.raises(ValueError, match="timestamp_us"):
        RowKey(group=(), timestamp_us=INT64_MIN - 1, offset=0)
    with pytest.raises(ValueError, match="offset"):
        RowKey(group=(), timestamp_us=0, offset=INT64_MAX + 1)


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
    with pytest.raises(ValueError, match="at least 22 bytes, got 5"):
        decode_row_key(encode_row_key(RowKey(("1",), 0, 0))[:5])


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


# -- offsets/ -----------------------------------------------------------------


@given(ranges=st.lists(offset_ranges, max_size=8))
def test_offsets_value_round_trip(ranges: list[OffsetRange]):
    decoded = decode_offsets_value(encode_offsets_value(ranges))
    expected = sorted(
        ranges, key=lambda r: (r.topic, r.partition, r.first_offset, r.last_offset)
    )
    assert decoded == tuple(expected)


@given(ranges=st.lists(offset_ranges, min_size=1, max_size=6))
def test_offsets_value_encoding_is_canonical(ranges: list[OffsetRange]):
    assert encode_offsets_value(ranges) == encode_offsets_value(list(reversed(ranges)))


@given(group=groups)
def test_offsets_key_round_trip(group: PartitionTuple):
    assert decode_offsets_key(offsets_key(group)) == group


def test_group_key_decodes_refuse_a_corrupt_group():
    for decode, prefix in (
        (decode_stats_key, STATS_PREFIX),
        (decode_offsets_key, OFFSETS_PREFIX),
    ):
        with pytest.raises(ValueError, match="corrupt partition tuple"):
            decode(prefix + b"\x02\x00")  # arity 2, one component
        with pytest.raises(ValueError):
            decode(prefix)  # no group at all
    with pytest.raises(ValueError, match="corrupt partition tuple"):
        decode_sched_age_key(SCHED_AGE_PREFIX + b"\x80" + b"\x00" * 7 + b"\x01\x09")
    with pytest.raises(ValueError, match="corrupt partition tuple"):
        decode_prepared_key(PREPARED_PREFIX + b"\x01" + b"\x80" + b"\x00" * 7)


def test_offset_range_validation():
    with pytest.raises(ValueError, match="first_offset"):
        OffsetRange("events", 0, 42, 41)  # inverted
    with pytest.raises(ValueError, match="partition"):
        OffsetRange("events", 2**32, 0, 1)  # partition field width
    with pytest.raises(ValueError, match="u16 length"):
        OffsetRange("x" * (2**16), 0, 0, 1)  # topic length field is u16
    with pytest.raises(ValueError):
        OffsetRange("\ud800", 0, 0, 1)  # not UTF-8 encodable


def test_offset_range_field_bounds():
    # The int64 range checks on the offset fields themselves, both
    # directions, and the partition field's lower edge. (Inverted ranges
    # and the upper partition edge are test_offset_range_validation.)
    for bad in (INT64_MAX + 1, INT64_MIN - 1):
        with pytest.raises(ValueError, match="first_offset"):
            OffsetRange("events", 0, bad, 0)
        with pytest.raises(ValueError, match="last_offset"):
            OffsetRange("events", 0, 0, bad)
    with pytest.raises(ValueError, match="partition"):
        OffsetRange("events", -1, 0, 1)


def test_varlen_fields_accept_exactly_the_u16_maximum():
    # 65535 fits the length field; 65536 does not. The width is
    # load-bearing: a `>` flipped to `>=` must red here.
    OffsetRange("t" * (2**16 - 1), 0, 0, 1)
    _request(idempotency_key="k" * (2**16 - 1))
    with pytest.raises(ValueError, match="u16 length"):
        OffsetRange("t" * (2**16), 0, 0, 1)
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


def test_offsets_value_round_trip_with_a_multi_byte_count():
    # 300 ranges: the count's high byte is nonzero, so a decoder reading
    # only its low byte stops early and trips the trailing-bytes guard.
    ranges = [OffsetRange("events", p, 0, 1) for p in range(300)]
    assert decode_offsets_value(encode_offsets_value(ranges)) == tuple(ranges)


def test_offsets_value_accepts_exactly_u16_max_ranges():
    ranges = [OffsetRange("events", 0, 0, 1)] * (2**16 - 1)
    assert len(decode_offsets_value(encode_offsets_value(ranges))) == 2**16 - 1
    with pytest.raises(ValueError, match="too many offset ranges"):
        encode_offsets_value(ranges + [OffsetRange("events", 0, 0, 1)])


def test_offsets_value_round_trip_with_negative_offsets():
    # Kafka offsets are non-negative in practice; the codec is int64-wide
    # (values use plain two's complement) and must round-trip the range.
    ranges = [OffsetRange("events", 0, INT64_MIN, -1), OffsetRange("events", 1, -5, 5)]
    decoded = decode_offsets_value(encode_offsets_value(ranges))
    assert decoded == tuple(
        sorted(
            ranges, key=lambda r: (r.topic, r.partition, r.first_offset, r.last_offset)
        )
    )


def test_offsets_value_truncation_errors_name_the_failed_parse_stage():
    # The message says WHICH guard fired — the forensic handle when a
    # corrupted SlateDB value is in front of you. One 31-byte value:
    # version(1) count(2), then topic_len(2) topic(6) partition(4)
    # first(8) last(8).
    encoded = encode_offsets_value([OffsetRange("events", 7, 0, 41)])
    expect = (
        ["too short"] * 3 + ["topic length"] * 2 + ["range record"] * (len(encoded) - 5)
    )
    for cut, stage in enumerate(expect):
        with pytest.raises(ValueError, match=stage):
            decode_offsets_value(encoded[:cut])


@given(ranges=st.lists(offset_ranges, max_size=6))
def test_offsets_value_rejects_strict_prefixes(ranges: list[OffsetRange]):
    encoded = encode_offsets_value(ranges)
    for cut in range(len(encoded)):
        with pytest.raises(ValueError):
            decode_offsets_value(encoded[:cut])


def test_offsets_value_rejects_bad_version_and_trailing_bytes():
    good = encode_offsets_value([])
    assert good == b"\x01\x00\x00"
    assert decode_offsets_value(good) == ()
    with pytest.raises(ValueError, match="unknown offsets value version 2"):
        decode_offsets_value(b"\x02\x00\x00")
    with pytest.raises(ValueError, match="trailing bytes after offsets value: 1$"):
        decode_offsets_value(good + b"\x00")


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
)
def test_prepared_value_round_trip(
    key: str,
    body: bytes,
    persisted_at: int,
    topic: str,
    partition: int,
    group: PartitionTuple,
):
    request = PreparedRequest(
        idempotency_key=key,
        body=body,
        persisted_at=persisted_at,
        topic=topic,
        partition=partition,
        group=group,
        group_fields=_fields(group),
    )
    assert decode_prepared_value(encode_prepared_value(request)) == request


def test_prepared_request_validation():
    with pytest.raises(ValueError, match="persisted_at"):
        _request(persisted_at=2**64)  # u64 overflow
    _request(persisted_at=2**64 - 1)  # the boundary fits
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
    # v4 layout for key "some-key", topic "events", group ("42",) named
    # ("t",): version(1) key_len(2) key(8) topic_len(2) topic(6)
    # partition(4) arity(1) name_len(2) name(1) tag(1) value_len(2)
    # value(2) body_len(4) body(2) persisted_at(8) — 46 bytes total; the
    # tail's cuts report the body-length mismatch.
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
        + ["length mismatch"] * 10  # body (2) + persisted_at (8) tail
    )
    assert len(encoded) == 46
    assert len(encoded) == len(expect)
    for cut, stage in enumerate(expect):
        with pytest.raises(ValueError, match=stage):
            decode_prepared_value(encoded[:cut])


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
    for version in (0, 1, 2, 3, 5):
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
    # The settlement delete's selection: tuple match and offset inside
    # the window — nothing else, however it sorts in byte order.
    selected = row_in_flushed_range(row, flushed)
    assert selected == (
        row.group == flushed.group
        and flushed.first_offset <= row.offset <= flushed.last_offset
    )
    start, end = flushed_rows_scan_range(flushed)
    if selected:
        assert start <= encode_row_key(row) < end
    elif row.group != flushed.group:
        # another tuple's rows are never even inside the scan range
        assert not (start <= encode_row_key(row) < end)


@given(flushed=flushed_keys, ts=timestamps)
def test_pending_delete_spares_younger_ranges_of_the_same_group(
    flushed: FlushedKey, ts: int
):
    assume(flushed.last_offset < INT64_MAX)
    # A row of the same tuple staged AFTER the flushed range: it sits
    # inside the byte scan range, so a blind range delete would take it —
    # the window's selection must not.
    younger = RowKey(flushed.group, ts, flushed.last_offset + 1)
    start, end = flushed_rows_scan_range(flushed)
    assert start <= encode_row_key(younger) < end
    assert not row_in_flushed_range(younger, flushed)


@given(flushed=flushed_keys, ts=timestamps)
def test_pending_delete_spares_older_ranges_of_the_same_group(
    flushed: FlushedKey, ts: int
):
    assume(flushed.first_offset > INT64_MIN)
    older = RowKey(flushed.group, ts, flushed.first_offset - 1)
    assert not row_in_flushed_range(older, flushed)


def test_flushed_selection_boundaries_are_inclusive():
    f = FlushedKey(("7",), 10, 20, group_fields=("a",))
    assert row_in_flushed_range(RowKey(("7",), 0, 10), f)
    assert row_in_flushed_range(RowKey(("7",), 0, 20), f)
    assert not row_in_flushed_range(RowKey(("7",), 0, 9), f)
    assert not row_in_flushed_range(RowKey(("7",), 0, 21), f)
    # Another tuple's row with an in-window offset is not selected.
    assert not row_in_flushed_range(RowKey(("8",), 0, 15), f)
    assert not row_in_flushed_range(RowKey(("7", None), 0, 15), f)


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
)
def test_idempotency_key_matches_the_documented_derivation(
    table: str,
    topic: str,
    partition: int,
    group: PartitionTuple,
    window: tuple[int, int],
):
    # The derivation FORMULA, pinned across arbitrary inputs (the golden
    # vector pins one concrete output): uuid5 over the millrace namespace
    # and the canonical "millrace:v2:" name line, table canonicalized,
    # the group under positional names, percent-encoded.
    first, last = window
    fields = tuple(f"k{i}" for i in range(len(group)))
    name = (
        f"millrace:v2:table={uuid.UUID(table)}:topic={topic}"
        f":partition={partition}:group={group_label(fields, group)}"
        f":offsets={first}-{last}"
    )
    assert idempotency_key(table, topic, partition, group, first, last) == str(
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
    base = idempotency_key(table, topic, partition, group, first, last)
    uuid.UUID(base)  # a well-formed UUID string
    assert base == idempotency_key(table, topic, partition, group, first, last)
    other_table = "00000000-0000-0000-0000-000000000000"
    if table == other_table:
        other_table = "00000000-0000-0000-0000-000000000001"
    assert idempotency_key(other_table, topic, partition, group, first, last) != base
    # TOPIC and PARTITION are derivation inputs: offsets are per partition,
    # so two partitions flushing one tuple over the same range must never
    # share a key (nor the object URIs derived from it).
    other_topic = f"{topic}-other"
    assert idempotency_key(table, other_topic, partition, group, first, last) != base
    other_partition = partition + 1 if partition < 2**32 - 1 else partition - 1
    assert idempotency_key(table, topic, other_partition, group, first, last) != base
    other_group = (*group, "x")
    assert idempotency_key(table, topic, partition, other_group, first, last) != base
    assert idempotency_key(table, topic, partition, group, first + 1, last) != base
    assert idempotency_key(table, topic, partition, group, first, last - 1) != base


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
    one = idempotency_key(table, topic_a, partition_a, group, first, last)
    two = idempotency_key(table, topic_b, partition_b, group, first, last)
    assert one != two


def test_idempotency_key_new_form_names_the_group_positionally():
    # The v2 name line carries ``group=k0=…/k1=…``: the tuple's wire
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
            f"millrace:v2:table={TABLE}:topic=events:partition=3"
            f":group={form}:offsets=5-9"
        )
        assert idempotency_key(TABLE, "events", 3, group, 5, 9) == str(
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
    idempotency_key(TABLE, topic, 0, (), 0, 41)
    with pytest.raises(ValueError, match="u16"):
        idempotency_key(TABLE, topic + "t", 0, (), 0, 41)


def test_partition_accepts_exactly_the_u32_maximum():
    # 2**32 - 1 fits partition(u32); 2**32 does not (a `<=` flipped to
    # `<` must red here — it refuses exactly the boundary value).
    idempotency_key(TABLE, "events", 2**32 - 1, (), 0, 41)
    _request(partition=2**32 - 1)
    with pytest.raises(ValueError, match="partition"):
        idempotency_key(TABLE, "events", 2**32, (), 0, 41)
    with pytest.raises(ValueError, match="partition"):
        _request(partition=2**32)


def test_idempotency_key_canonicalizes_the_table_uuid():
    lower = idempotency_key(
        "12345678-1234-5678-1234-567812345678", "events", 0, ("1",), 0, 41
    )
    upper = idempotency_key(
        "12345678-1234-5678-1234-567812345678".upper(),
        "events",
        0,
        ("1",),
        0,
        41,
    )
    assert lower == upper


def test_idempotency_key_does_not_canonicalize_the_topic():
    # Kafka topic names are case-sensitive; the derivation must be too.
    assert idempotency_key(TABLE, "Events", 0, (), 0, 41) != idempotency_key(
        TABLE, "events", 0, (), 0, 41
    )


def test_idempotency_key_validation():
    with pytest.raises(ValueError):
        idempotency_key("not-a-uuid", "events", 0, (), 0, 41)
    with pytest.raises(ValueError, match="topic"):
        idempotency_key(TABLE, "", 0, (), 0, 41)  # empty topic
    with pytest.raises(ValueError, match="partition"):
        idempotency_key(TABLE, "events", -1, (), 0, 41)  # negative partition
    with pytest.raises(ValueError, match="partition"):
        idempotency_key(TABLE, "events", 2**32, (), 0, 41)  # u32 overflow
    with pytest.raises(ValueError, match="first_offset"):
        idempotency_key(TABLE, "events", 0, (), 42, 41)  # inverted offset range


def test_prefixes_are_distinct():
    prefixes = [
        ROWS_PREFIX,
        STATS_PREFIX,
        SCHED_AGE_PREFIX,
        OFFSETS_PREFIX,
        PREPARED_PREFIX,
        FLUSHED_PREFIX,
    ]
    assert len(set(prefixes)) == len(prefixes)
    # No prefix is a strict prefix of another: one family's keys never
    # fall inside another family's range scan.
    for a in prefixes:
        for b in prefixes:
            if a is not b:
                assert not a.startswith(b), (a, b)
    assert group_rows_prefix(()).startswith(ROWS_PREFIX)
