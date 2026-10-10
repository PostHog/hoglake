"""Keyspace codecs: round-trips, byte order = logical order, golden vectors.

The golden vectors pin the byte-level contract: a future encoding change
fails loudly here instead of silently reshuffling every staged key in an
existing SlateDB. Each golden is written out byte by byte — no
implementation function builds an expectation.
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
    POISON_PREFIX,
    PREPARED_PREFIX,
    ROWS_PREFIX,
    SCHED_AGE_PREFIX,
    STATS_PREFIX,
    FlushedKey,
    OffsetRange,
    PoisonValue,
    PreparedKey,
    PreparedRequest,
    RowKey,
    SchedAgeKey,
    StatsValue,
    _prefix_successor,
    decode_offsets_key,
    decode_offsets_value,
    decode_poison_key,
    decode_poison_value,
    decode_prepared_key,
    decode_prepared_value,
    decode_row_key,
    decode_sched_age_key,
    decode_stats_key,
    decode_stats_value,
    encode_offsets_value,
    encode_poison_value,
    encode_prepared_value,
    encode_row_key,
    encode_sched_age_key,
    encode_stats_value,
    flushed_range,
    flushed_rows_scan_range,
    idempotency_key,
    offsets_key,
    poison_key,
    poison_range,
    prepared_key,
    row_in_flushed_range,
    sched_age_range_through,
    staged_rows_range,
    stats_key,
    team_prepared_range,
    team_rows_prefix,
    team_rows_range,
)

INT64_MIN = -(2**63)
INT64_MAX = 2**63 - 1
UINT64_MAX = 2**64 - 1

teams = st.integers(min_value=0, max_value=UINT64_MAX)
timestamps = st.integers(min_value=INT64_MIN, max_value=INT64_MAX)
kafka_offsets = st.integers(min_value=INT64_MIN, max_value=INT64_MAX)
#: Kafka partitions (u32).
partitions = st.integers(min_value=0, max_value=2**32 - 1)
# UTF-8-encodable topics (no surrogates: OffsetRange refuses those loudly).
topics = st.text(
    alphabet=st.characters(exclude_categories=("Cs",)), min_size=1, max_size=32
)


def _offset_range(topic: str, partition: int, a: int, b: int) -> OffsetRange:
    return OffsetRange(topic, partition, min(a, b), max(a, b))


def _flushed_key(team: int, a: int, b: int) -> FlushedKey:
    return FlushedKey(team, min(a, b), max(a, b))


def _stats_value(staged: int, first: int, last: int, rows: int) -> StatsValue:
    return StatsValue(staged, min(first, last), max(first, last), rows)


row_keys = st.builds(
    RowKey, team_id=teams, timestamp_us=timestamps, offset=kafka_offsets
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
sched_age_keys = st.builds(SchedAgeKey, first_staged_ts=timestamps, team_id=teams)
flushed_keys = st.builds(_flushed_key, team=teams, a=kafka_offsets, b=kafka_offsets)


# -- row keys: golden vectors -------------------------------------------------


def test_row_key_golden_vectors():
    # team 1, epoch 0, offset 0: zero timestamps map to 0x8000... (the
    # offset-binary midpoint), so byte order can be numeric order.
    assert encode_row_key(RowKey(team_id=1, timestamp_us=0, offset=0)) == (
        b"rows/" + b"\x00" * 7 + b"\x01" + b"\x80" + b"\x00" * 7 + b"\x80" + b"\x00" * 7
    )
    # INT64_MIN maps to zero bytes — the smallest encodable instant sorts
    # before everything, exactly where negative instants belong.
    assert encode_row_key(
        RowKey(team_id=0, timestamp_us=INT64_MIN, offset=INT64_MIN)
    ) == (b"rows/" + b"\x00" * 24)
    # The all-ones corner.
    assert (
        encode_row_key(
            RowKey(team_id=UINT64_MAX, timestamp_us=INT64_MAX, offset=INT64_MAX)
        )
        == b"rows/" + b"\xff" * 24
    )
    # A negative instant: -1 sorts just below 0, and its bytes show it.
    assert encode_row_key(
        RowKey(team_id=0x0102030405060708, timestamp_us=-1, offset=1)
    ) == (
        b"rows/"
        + b"\x01\x02\x03\x04\x05\x06\x07\x08"
        + b"\x7f\xff\xff\xff\xff\xff\xff\xff"
        + b"\x80\x00\x00\x00\x00\x00\x00\x01"
    )


def test_stats_golden_vectors():
    assert stats_key(1) == b"stats/" + b"\x00" * 7 + b"\x01"
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
    assert encode_sched_age_key(SchedAgeKey(first_staged_ts=0, team_id=1)) == (
        b"sched_age/" + b"\x80" + b"\x00" * 7 + b"\x00" * 7 + b"\x01"
    )


def test_offsets_golden_vector():
    assert offsets_key(1) == b"offsets/" + b"\x00" * 7 + b"\x01"
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
    assert prepared_key(1, 0) == (
        b"prepared/" + b"\x00" * 7 + b"\x01" + b"\x80" + b"\x00" * 7
    )
    # The v3 value envelope: version(3) key_len(2) key("key-abc")
    # topic_len(2) topic("events") partition(u32 = 0) body_len(4)
    # body("{}") persisted_at(u64 = 42). Written out byte by byte — the
    # on-disk contract a future codec change must not shift. The v3
    # fields are the v2 idempotency-key derivation inputs (topic,
    # partition): the envelope is self-describing, and recovery
    # cross-checks them against the stage's identity.
    assert encode_prepared_value(
        PreparedRequest("key-abc", b"{}", persisted_at=42, topic="events", partition=0)
    ) == (
        b"\x03"  # version
        + b"\x00\x07"
        + b"key-abc"
        + b"\x00\x06"
        + b"events"
        + b"\x00\x00\x00\x00"  # partition
        + b"\x00\x00\x00\x02"
        + b"{}"
        + b"\x00" * 7
        + b"\x2a"  # persisted_at = 42
    )


def test_idempotency_key_golden_vector():
    # The derivation: uuid5(IDEMPOTENCY_NAMESPACE, name) over the
    # canonical name line. Pinned so a derivation change fails loudly.
    # (millrace-named: namespace = uuid5(NAMESPACE_URL, "github.com/
    # PostHog/hoglake millrace idempotency v1"), name = "millrace:v2:…" —
    # the v2 name carries topic+partition because offsets are per
    # partition: two partitions flushing one team over the same range
    # must not share a key (PR #331 review).)
    assert (
        idempotency_key("12345678-1234-5678-1234-567812345678", "events", 0, 1, 0, 41)
        == "5ccd24a8-f651-5fbc-a8db-4fe1ae7f2dcb"
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
    by_bytes = sorted(keys, key=encode_row_key)
    by_value = sorted(keys, key=lambda k: (k.team_id, k.timestamp_us, k.offset))
    assert by_bytes == by_value


@given(keys=st.lists(row_keys, min_size=2, max_size=60))
def test_row_key_encoding_is_injective(keys: list[RowKey]):
    encoded = [encode_row_key(k) for k in keys]
    assert len(set(encoded)) == len(set(keys))


def test_row_key_negative_timestamps_sort_before_positive():
    instants = [INT64_MIN, -2, -1, 0, 1, 2, INT64_MAX]
    keys = [RowKey(team_id=7, timestamp_us=ts, offset=0) for ts in instants]
    by_bytes = sorted(keys, key=encode_row_key)
    assert [k.timestamp_us for k in by_bytes] == instants


@given(key=row_keys, team=teams)
def test_team_rows_range_membership(key: RowKey, team: int):
    start, end = team_rows_range(team)
    encoded = encode_row_key(key)
    assert (start <= encoded < end) == (key.team_id == team)


@given(keys=st.lists(row_keys, max_size=60))
def test_one_teams_rows_are_contiguous_in_byte_order(keys: list[RowKey]):
    ordered = [decode_row_key(b) for b in sorted(encode_row_key(k) for k in keys)]
    present = {k.team_id for k in ordered}
    for team in present:
        positions = [i for i, k in enumerate(ordered) if k.team_id == team]
        assert positions == list(range(positions[0], positions[-1] + 1))


@given(team=teams, first=kafka_offsets, last=kafka_offsets)
def test_staged_rows_range_is_the_teams_range(team: int, first: int, last: int):
    if first > last:
        first, last = last, first
    assert staged_rows_range(team, first, last) == team_rows_range(team)


def test_staged_rows_range_rejects_inverted_offsets():
    with pytest.raises(ValueError, match="first_offset 42 > last_offset 41"):
        staged_rows_range(1, 42, 41)


def test_key_functions_validate_team_id():
    for fn in (stats_key, offsets_key, team_rows_prefix):
        with pytest.raises(ValueError, match="team_id"):
            fn(-1)
    with pytest.raises(ValueError, match="team_id"):
        prepared_key(-1, 0)
    with pytest.raises(ValueError, match="team_id"):
        FlushedKey(-1, 0, 1)


def test_offset_arguments_are_validated():
    with pytest.raises(ValueError, match="first_offset"):
        staged_rows_range(1, INT64_MAX + 1, INT64_MAX + 1)
    with pytest.raises(ValueError, match="last_offset"):
        staged_rows_range(1, 0, INT64_MAX + 1)
    with pytest.raises(ValueError, match="first_offset"):
        prepared_key(1, INT64_MAX + 1)
    with pytest.raises(ValueError, match="first_offset"):
        idempotency_key(TABLE, "events", 0, 0, INT64_MAX + 1, INT64_MAX + 1)
    with pytest.raises(ValueError, match="last_offset"):
        idempotency_key(TABLE, "events", 0, 0, 0, INT64_MAX + 1)


def test_prefix_successor_strips_only_trailing_ff():
    assert _prefix_successor(b"rows/") == b"rows0"
    assert _prefix_successor(b"a\xff\xff") == b"b"
    assert _prefix_successor(b"aX") == b"aY"
    assert _prefix_successor(b"\x00") == b"\x01"
    for bad in (b"\xff", b"\xff\xff"):
        with pytest.raises(ValueError, match="no successor"):
            _prefix_successor(bad)


def test_team_rows_range_end_is_the_exact_prefix_successor():
    # team 0x58's low key byte is 'X': a successor rstripping anything but
    # 0xFF would eat it and collapse the range.
    start, end = team_rows_range(0x58)
    assert (start, end) == (
        b"rows/" + b"\x00" * 7 + b"\x58",
        b"rows/" + b"\x00" * 7 + b"\x59",
    )
    # UINT64_MAX rolls over to the prefix's own successor.
    start, end = team_rows_range(UINT64_MAX)
    assert end == b"rows0"
    top = encode_row_key(RowKey(UINT64_MAX, INT64_MAX, INT64_MAX))
    assert start <= top < end


def test_row_key_field_validation():
    with pytest.raises(ValueError, match="team_id"):
        RowKey(team_id=-1, timestamp_us=0, offset=0)
    with pytest.raises(ValueError, match="team_id"):
        RowKey(team_id=UINT64_MAX + 1, timestamp_us=0, offset=0)
    with pytest.raises(ValueError, match="timestamp_us"):
        RowKey(team_id=0, timestamp_us=INT64_MIN - 1, offset=0)
    with pytest.raises(ValueError, match="offset"):
        RowKey(team_id=0, timestamp_us=0, offset=INT64_MAX + 1)


@given(key=row_keys)
def test_decode_row_key_rejects_strict_prefixes_and_wrong_prefix(key: RowKey):
    encoded = encode_row_key(key)
    for cut in range(len(encoded)):
        with pytest.raises(ValueError):
            decode_row_key(encoded[:cut])
    with pytest.raises(ValueError, match="does not start with"):
        decode_row_key(b"rowx/" + encoded[len(ROWS_PREFIX) :])


def test_decode_row_key_reports_the_expected_length():
    with pytest.raises(ValueError, match="expected 29 bytes, got 5"):
        decode_row_key(encode_row_key(RowKey(1, 0, 0))[:5])


# -- stats/ -------------------------------------------------------------------


@given(value=stats_values)
def test_stats_value_round_trip(value: StatsValue):
    assert decode_stats_value(encode_stats_value(value)) == value


@given(team=teams)
def test_stats_key_round_trip(team: int):
    assert decode_stats_key(stats_key(team)) == team


@given(a=teams, b=teams)
def test_stats_keys_sort_by_team(a: int, b: int):
    assert (stats_key(a) < stats_key(b)) == (a < b)


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
        SchedAgeKey(first_staged_ts=INT64_MAX + 1, team_id=0)
    with pytest.raises(ValueError, match="team_id"):
        SchedAgeKey(first_staged_ts=0, team_id=-1)


def test_sched_age_range_through_validates_cutoff():
    with pytest.raises(ValueError, match="cutoff_ts"):
        sched_age_range_through(INT64_MAX + 1)


@given(keys=st.lists(sched_age_keys, max_size=60))
def test_sched_age_byte_order_is_age_order(keys: list[SchedAgeKey]):
    by_bytes = sorted(keys, key=encode_sched_age_key)
    by_value = sorted(keys, key=lambda k: (k.first_staged_ts, k.team_id))
    assert by_bytes == by_value


@given(key=sched_age_keys, cutoff=timestamps)
def test_sched_age_range_through_membership(key: SchedAgeKey, cutoff: int):
    start, end = sched_age_range_through(cutoff)
    encoded = encode_sched_age_key(key)
    assert (start <= encoded < end) == (key.first_staged_ts <= cutoff)


def test_sched_age_range_through_covers_everything_at_int64_max():
    start, end = sched_age_range_through(INT64_MAX)
    assert start == SCHED_AGE_PREFIX
    assert end == b"sched_age0"  # prefix successor: last byte incremented
    oldest = encode_sched_age_key(SchedAgeKey(first_staged_ts=INT64_MIN, team_id=0))
    newest = encode_sched_age_key(
        SchedAgeKey(first_staged_ts=INT64_MAX, team_id=UINT64_MAX)
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


@given(team=teams)
def test_offsets_key_round_trip(team: int):
    assert decode_offsets_key(offsets_key(team)) == team


def test_team_id_key_decodes_are_unsigned_at_the_signed_bit():
    # Every u64 team_id key decode's signedness is load-bearing: a
    # signed read maps team_ids >= 2**63 to negatives. The round-trip
    # properties cannot be trusted with this — hypothesis's derandomized
    # example stream is per-test-name, and e.g. test_offsets_key_round_trip's
    # stream draws no team_id >= 2**63 (mutmut's signed=False→signed=True
    # flip on decode_offsets_key survived a full clean run under it) — so
    # the boundary is pinned here by hand, deterministically, for every
    # team-id codec at once.
    for team in (2**63 - 1, 2**63, UINT64_MAX):
        assert decode_stats_key(stats_key(team)) == team
        assert decode_offsets_key(offsets_key(team)) == team
        assert decode_prepared_key(prepared_key(team, 0)).team_id == team
        assert (
            decode_sched_age_key(encode_sched_age_key(SchedAgeKey(0, team))).team_id
            == team
        )
        assert decode_row_key(encode_row_key(RowKey(team, 0, 0))).team_id == team


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
    PreparedRequest(idempotency_key="k" * (2**16 - 1), body=b"")
    with pytest.raises(ValueError, match="u16 length"):
        OffsetRange("t" * (2**16), 0, 0, 1)
    with pytest.raises(ValueError, match="idempotency key too long"):
        PreparedRequest(idempotency_key="k" * (2**16), body=b"")


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
    PreparedRequest(idempotency_key="k", body=_FakeLength(2**32 - 1))
    with pytest.raises(ValueError, match="prepared body too long"):
        PreparedRequest(idempotency_key="k", body=_FakeLength(2**32))


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


@given(team=teams, first=kafka_offsets)
def test_prepared_key_round_trip(team: int, first: int):
    assert decode_prepared_key(prepared_key(team, first)) == PreparedKey(team, first)


def test_team_prepared_range_selects_exactly_the_team():
    # The flush's outstanding-entry gate scans this range: it must cover
    # every prepared/ key of the team and no other team's.
    start, end = team_prepared_range(7)
    assert start == PREPARED_PREFIX + (7).to_bytes(8, "big")
    assert end == PREPARED_PREFIX + (8).to_bytes(8, "big")
    assert start <= prepared_key(7, 0) < end
    assert start <= prepared_key(7, INT64_MAX) < end
    # first_offset is offset-binary: negative offsets sort below 0 and
    # still belong to the team's range.
    assert start <= prepared_key(7, INT64_MIN) < end
    assert not start <= prepared_key(6, 0) < end
    assert not start <= prepared_key(8, 0) < end
    assert not start <= b"prepared" < end  # one byte short of the prefix


def test_team_prepared_range_validates_team_id():
    with pytest.raises(ValueError, match="team_id"):
        team_prepared_range(-1)
    with pytest.raises(ValueError, match="team_id"):
        team_prepared_range(UINT64_MAX + 1)


@given(
    keys=st.lists(
        st.builds(PreparedKey, team_id=teams, first_offset=kafka_offsets), max_size=60
    )
)
def test_prepared_keys_sort_by_team_then_first_offset(keys: list[PreparedKey]):
    by_bytes = sorted(keys, key=lambda k: prepared_key(k.team_id, k.first_offset))
    by_value = sorted(keys, key=lambda k: (k.team_id, k.first_offset))
    assert by_bytes == by_value


@given(
    key=st.text(alphabet=st.characters(exclude_categories=("Cs",)), max_size=64),
    body=st.binary(max_size=256),
    persisted_at=st.integers(min_value=0, max_value=UINT64_MAX),
    topic=topics,
    partition=partitions,
)
def test_prepared_value_round_trip(
    key: str, body: bytes, persisted_at: int, topic: str, partition: int
):
    request = PreparedRequest(
        idempotency_key=key,
        body=body,
        persisted_at=persisted_at,
        topic=topic,
        partition=partition,
    )
    assert decode_prepared_value(encode_prepared_value(request)) == request


#: The legacy v1 envelope for key "k", body b"{}": version(1) key_len key
#: body_len body — and NO persisted_at. Golden, decode-only.
PREPARED_V1_GOLDEN: bytes = b"\x01" + b"\x00\x01" + b"k" + b"\x00\x00\x00\x02" + b"{}"

#: The v2 envelope this branch wrote before the v2 key derivation:
#: version(2) key_len key body_len body persisted_at(u64=42) — and NO
#: topic/partition. Golden, decode-only.
PREPARED_V2_GOLDEN: bytes = (
    b"\x02" + b"\x00\x01" + b"k" + b"\x00\x00\x00\x02" + b"{}" + b"\x00" * 7 + b"\x2a"
)


def test_prepared_value_v1_decodes_with_unknown_age():
    # A pre-v2 build's envelope: decodes, and persisted_at is None —
    # "unknown age", which the flusher's receipt-horizon guard treats as
    # halt-safe (never a blind replay). No derivation inputs either.
    assert decode_prepared_value(PREPARED_V1_GOLDEN) == PreparedRequest(
        "k", b"{}", persisted_at=None
    )
    # ...and a trailing byte on a v1 envelope is still corruption.
    with pytest.raises(ValueError, match="field says 2, value holds 3$"):
        decode_prepared_value(PREPARED_V1_GOLDEN + b"\x00")


def test_prepared_value_v2_decodes_without_the_derivation_inputs():
    # The intermediate shape: persisted_at is known (the horizon guard
    # works), topic/partition are None (recovery's self-description
    # cross-check does not apply).
    assert decode_prepared_value(PREPARED_V2_GOLDEN) == PreparedRequest(
        "k", b"{}", persisted_at=42, topic=None, partition=None
    )
    with pytest.raises(ValueError, match="field says 2, value holds 3$"):
        decode_prepared_value(PREPARED_V2_GOLDEN + b"\x00")


def test_prepared_value_encode_requires_persisted_at():
    # An entry whose age is unknown can never be checked against the
    # receipt horizon, so WRITING one is refused outright (decoding a
    # legacy v1 envelope with no timestamp stays legal).
    with pytest.raises(ValueError, match="without persisted_at"):
        encode_prepared_value(PreparedRequest("k", b"{}", topic="events", partition=0))
    with pytest.raises(ValueError, match="persisted_at"):
        PreparedRequest("k", b"{}", persisted_at=2**64)  # u64 overflow
    PreparedRequest("k", b"{}", persisted_at=2**64 - 1)  # the boundary fits


def test_prepared_value_encode_requires_the_derivation_inputs():
    # The v3 envelope carries the v2 idempotency-key derivation inputs
    # (topic, partition): a write that cannot name them is refused, and
    # the dataclass refuses a half-specified pair.
    with pytest.raises(ValueError, match="without topic/partition"):
        encode_prepared_value(PreparedRequest("k", b"{}", persisted_at=7))
    with pytest.raises(ValueError, match="travel together"):
        PreparedRequest("k", b"{}", persisted_at=7, topic="events")
    with pytest.raises(ValueError, match="travel together"):
        PreparedRequest("k", b"{}", persisted_at=7, partition=0)
    with pytest.raises(ValueError, match="topic"):
        PreparedRequest("k", b"{}", persisted_at=7, topic="", partition=0)
    with pytest.raises(ValueError, match="partition"):
        PreparedRequest("k", b"{}", persisted_at=7, topic="events", partition=-1)
    with pytest.raises(ValueError, match="partition"):
        PreparedRequest("k", b"{}", persisted_at=7, topic="events", partition=2**32)


@given(
    body=st.binary(max_size=128),
    at=st.integers(min_value=0, max_value=UINT64_MAX),
    topic=topics,
    partition=partitions,
)
def test_prepared_value_rejects_strict_prefixes(
    body: bytes, at: int, topic: str, partition: int
):
    encoded = encode_prepared_value(
        PreparedRequest(
            "some-key", body, persisted_at=at, topic=topic, partition=partition
        )
    )
    for cut in range(len(encoded)):
        with pytest.raises(ValueError):
            decode_prepared_value(encoded[:cut])


def test_prepared_value_truncation_errors_name_the_failed_parse_stage():
    # v3 layout: version(1) key_len(2) key(8) topic_len(2) topic(6)
    # partition(4) body_len(4) body(2) persisted_at(8) — 37 bytes total;
    # the tail's cuts report the body-length mismatch.
    encoded = encode_prepared_value(
        PreparedRequest("some-key", b"{}", 12345, topic="events", partition=0)
    )
    expect = (
        ["too short"] * 3  # the version+key_len header
        + ["key$"] * 8  # cuts inside the 8-byte key
        + ["topic length$"] * 2
        + ["topic$"] * 10  # topic bytes (6) + partition (4)
        + ["body length$"] * 4
        + ["length mismatch"] * 10  # body (2) + persisted_at (8) tail
    )
    assert len(encoded) == 37
    assert len(encoded) == len(expect)
    for cut, stage in enumerate(expect):
        with pytest.raises(ValueError, match=stage):
            decode_prepared_value(encoded[:cut])


def test_prepared_value_round_trip_with_a_multi_byte_key_length():
    # A 300-byte key: the key_len high byte is nonzero, so a decoder
    # reading only its low byte misaligns the body and must fail loudly.
    request = PreparedRequest(
        idempotency_key="k" * 300,
        body=b"{}",
        persisted_at=7,
        topic="events",
        partition=0,
    )
    assert decode_prepared_value(encode_prepared_value(request)) == request


def test_prepared_value_round_trip_with_a_multi_byte_topic_length():
    # Same for topic_len: a 300-byte topic misaligns partition onward.
    request = PreparedRequest(
        idempotency_key="k", body=b"{}", persisted_at=7, topic="t" * 300, partition=7
    )
    assert decode_prepared_value(encode_prepared_value(request)) == request


def test_prepared_value_rejects_bad_version_and_trailing_bytes():
    good = encode_prepared_value(
        PreparedRequest("k", b"", persisted_at=0, topic="events", partition=0)
    )
    with pytest.raises(ValueError, match="unknown prepared value version 4"):
        decode_prepared_value(b"\x04" + good[1:])
    with pytest.raises(ValueError, match="unknown prepared value version 0"):
        decode_prepared_value(b"\x00" + good[1:])
    with pytest.raises(ValueError, match="field says 0, value holds 1$"):
        decode_prepared_value(good + b"\x00")


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


def test_flushed_key_rejects_inverted_offsets():
    with pytest.raises(ValueError, match="first_offset 42 > last_offset 41"):
        FlushedKey(1, 42, 41)


def test_flushed_key_field_validation():
    with pytest.raises(ValueError, match="team_id"):
        FlushedKey(-1, 0, 1)
    with pytest.raises(ValueError, match="first_offset"):
        FlushedKey(0, INT64_MAX + 1, INT64_MAX + 1)
    with pytest.raises(ValueError, match="last_offset"):
        FlushedKey(0, 0, INT64_MAX + 1)


@given(flushed=flushed_keys, row=row_keys)
def test_flushed_marker_selects_exactly_its_own_rows(flushed: FlushedKey, row: RowKey):
    # The pending delete's selection: team match and offset inside the
    # marker's window — nothing else, however it sorts in byte order.
    selected = row_in_flushed_range(row, flushed)
    assert selected == (
        row.team_id == flushed.team_id
        and flushed.first_offset <= row.offset <= flushed.last_offset
    )
    start, end = flushed_rows_scan_range(flushed)
    if selected:
        assert start <= encode_row_key(row) < end
    elif row.team_id != flushed.team_id:
        # another team's rows are never even inside the scan range
        assert not (start <= encode_row_key(row) < end)


@given(flushed=flushed_keys, ts=timestamps)
def test_pending_delete_spares_younger_ranges_of_the_same_team(
    flushed: FlushedKey, ts: int
):
    assume(flushed.last_offset < INT64_MAX)
    # A row of the same team staged AFTER the flushed range: it sits
    # inside the byte scan range, so a blind range delete would take it —
    # the marker's selection must not.
    younger = RowKey(flushed.team_id, ts, flushed.last_offset + 1)
    start, end = flushed_rows_scan_range(flushed)
    assert start <= encode_row_key(younger) < end
    assert not row_in_flushed_range(younger, flushed)


@given(flushed=flushed_keys, ts=timestamps)
def test_pending_delete_spares_older_ranges_of_the_same_team(
    flushed: FlushedKey, ts: int
):
    assume(flushed.first_offset > INT64_MIN)
    older = RowKey(flushed.team_id, ts, flushed.first_offset - 1)
    assert not row_in_flushed_range(older, flushed)


def test_flushed_selection_boundaries_are_inclusive():
    f = FlushedKey(7, 10, 20)
    assert row_in_flushed_range(RowKey(7, 0, 10), f)
    assert row_in_flushed_range(RowKey(7, 0, 20), f)
    assert not row_in_flushed_range(RowKey(7, 0, 9), f)
    assert not row_in_flushed_range(RowKey(7, 0, 21), f)
    # Another team's row with an in-window offset is not selected.
    assert not row_in_flushed_range(RowKey(8, 0, 15), f)


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
    team=teams,
    window=_first_last(),
)
def test_idempotency_key_matches_the_documented_derivation(
    table: str, topic: str, partition: int, team: int, window: tuple[int, int]
):
    # The derivation FORMULA, pinned across arbitrary inputs (the golden
    # vector pins one concrete output): uuid5 over the millrace namespace
    # and the canonical "millrace:v2:" name line, table canonicalized.
    first, last = window
    name = (
        f"millrace:v2:table={uuid.UUID(table)}:topic={topic}"
        f":partition={partition}:team={team}:offsets={first}-{last}"
    )
    assert idempotency_key(table, topic, partition, team, first, last) == str(
        uuid.uuid5(IDEMPOTENCY_NAMESPACE, name)
    )


@given(
    team=teams,
    window=_first_last(),
    table=uuid_strings,
    topic=topics,
    partition=partitions,
)
def test_idempotency_key_is_a_stable_pure_function(
    team: int, window: tuple[int, int], table: str, topic: str, partition: int
):
    first, last = window
    one = idempotency_key(table, topic, partition, team, first, last)
    two = idempotency_key(table, topic, partition, team, first, last)
    assert one == two
    uuid.UUID(one)  # a well-formed UUID string


@given(
    table=uuid_strings,
    topic=topics,
    partition=partitions,
    team=teams,
    window=_first_last(),
)
def test_idempotency_key_is_sensitive_to_every_input_field(
    table: str, topic: str, partition: int, team: int, window: tuple[int, int]
):
    first, last = window
    assume(first < last)  # a degenerate single-offset range is uninteresting
    base = idempotency_key(table, topic, partition, team, first, last)
    other_table = "00000000-0000-0000-0000-000000000000"
    if table == other_table:
        other_table = "00000000-0000-0000-0000-000000000001"
    assert idempotency_key(other_table, topic, partition, team, first, last) != base
    # TOPIC and PARTITION are derivation inputs (v2): offsets are per
    # partition, so two partitions flushing one team over the same range
    # must never share a key (nor the object URIs derived from it). The
    # pre-fix test passed only because partition was not an input at all.
    other_topic = f"{topic}-other"
    assert idempotency_key(table, other_topic, partition, team, first, last) != base
    other_partition = partition + 1 if partition < 2**32 - 1 else partition - 1
    assert idempotency_key(table, topic, other_partition, team, first, last) != base
    other_team = team + 1 if team < UINT64_MAX else team - 1
    assert idempotency_key(table, topic, partition, other_team, first, last) != base
    assert idempotency_key(table, topic, partition, team, first + 1, last) != base
    assert idempotency_key(table, topic, partition, team, first, last - 1) != base


@given(
    table=uuid_strings,
    topic_a=topics,
    topic_b=topics,
    partition_a=partitions,
    partition_b=partitions,
    team=teams,
    window=_first_last(),
)
def test_identical_window_on_two_partitions_yields_different_keys(
    table: str,
    topic_a: str,
    topic_b: str,
    partition_a: int,
    partition_b: int,
    team: int,
    window: tuple[int, int],
):
    """The B1 property: identical (table, team, first, last) — but
    different (topic, partition) — MUST yield different keys. Two
    partitions of one topic flushing one team over the same offset range
    are the case that overwrote committed objects under the v1
    derivation."""
    assume((topic_a, partition_a) != (topic_b, partition_b))
    first, last = window
    one = idempotency_key(table, topic_a, partition_a, team, first, last)
    two = idempotency_key(table, topic_b, partition_b, team, first, last)
    assert one != two


def test_prepared_value_varlen_fields_accept_exactly_the_u16_maximum():
    # 2**16 - 1 utf-8 bytes fits topic_len(u16); 2**16 does not. The
    # width is load-bearing (a `>` flipped to `>=` must red here), and
    # it is BYTES, not characters (a non-ASCII topic is longer encoded).
    topic = "t" * (2**16 - 1)
    PreparedRequest("k", b"{}", persisted_at=1, topic=topic, partition=0)
    with pytest.raises(ValueError, match="u16"):
        PreparedRequest("k", b"{}", persisted_at=1, topic=topic + "t", partition=0)
    # The idempotency derivation validates the same width (the envelope
    # must be able to carry what the key was derived from).
    idempotency_key(TABLE, topic, 0, 1, 0, 41)
    with pytest.raises(ValueError, match="u16"):
        idempotency_key(TABLE, topic + "t", 0, 1, 0, 41)


def test_partition_accepts_exactly_the_u32_maximum():
    # 2**32 - 1 fits partition(u32); 2**32 does not (a `<=` flipped to
    # `<` must red here — it refuses exactly the boundary value).
    idempotency_key(TABLE, "events", 2**32 - 1, 1, 0, 41)
    PreparedRequest("k", b"{}", persisted_at=1, topic="events", partition=2**32 - 1)
    with pytest.raises(ValueError, match="partition"):
        idempotency_key(TABLE, "events", 2**32, 1, 0, 41)
    with pytest.raises(ValueError, match="partition"):
        PreparedRequest("k", b"{}", persisted_at=1, topic="events", partition=2**32)


def test_idempotency_key_canonicalizes_the_table_uuid():
    lower = idempotency_key(
        "12345678-1234-5678-1234-567812345678", "events", 0, 1, 0, 41
    )
    upper = idempotency_key(
        "12345678-1234-5678-1234-567812345678".upper(), "events", 0, 1, 0, 41
    )
    assert lower == upper


def test_idempotency_key_does_not_canonicalize_the_topic():
    # Kafka topic names are case-sensitive; the derivation must be too.
    assert idempotency_key(TABLE, "Events", 0, 1, 0, 41) != idempotency_key(
        TABLE, "events", 0, 1, 0, 41
    )


def test_idempotency_key_validation():
    with pytest.raises(ValueError):
        idempotency_key("not-a-uuid", "events", 0, 1, 0, 41)
    with pytest.raises(ValueError, match="topic"):
        idempotency_key(TABLE, "", 0, 1, 0, 41)  # empty topic
    with pytest.raises(ValueError, match="partition"):
        idempotency_key(TABLE, "events", -1, 1, 0, 41)  # negative partition
    with pytest.raises(ValueError, match="partition"):
        idempotency_key(TABLE, "events", 2**32, 1, 0, 41)  # u32 overflow
    with pytest.raises(ValueError, match="first_offset"):
        idempotency_key(TABLE, "events", 0, 1, 42, 41)  # inverted offset range
    with pytest.raises(ValueError, match="team_id"):
        idempotency_key(TABLE, "events", 0, -1, 0, 41)  # team_id is non-negative


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
    assert team_rows_prefix(0).startswith(ROWS_PREFIX)


# -- poison/ (the consume-time quarantine, Phase 3) -------------------------------


def test_poison_key_round_trip_and_order():
    for offset in (0, 1, 42, INT64_MAX):
        assert decode_poison_key(poison_key(offset)) == offset
    # Byte order = numeric order across the whole int64 range (offset
    # binary), so the quarantine scans in arrival order.
    keys = [poison_key(o) for o in (INT64_MIN, -1, 0, 1, INT64_MAX)]
    assert keys == sorted(keys)
    with pytest.raises(ValueError, match="offset"):
        poison_key(INT64_MAX + 1)


def test_poison_prefix_is_a_family_of_its_own():
    assert poison_key(0).startswith(POISON_PREFIX)
    for other in (
        ROWS_PREFIX,
        STATS_PREFIX,
        SCHED_AGE_PREFIX,
        OFFSETS_PREFIX,
        PREPARED_PREFIX,
        FLUSHED_PREFIX,
    ):
        assert not POISON_PREFIX.startswith(other)
        assert not other.startswith(POISON_PREFIX)
    start, end = poison_range()
    assert start == POISON_PREFIX and end == _prefix_successor(POISON_PREFIX)


def test_poison_value_golden_vector():
    """Byte-level contract, written out by hand: a change to the poison
    encoding fails loudly here rather than silently reshaping the
    quarantine of an existing SlateDB."""
    encoded = encode_poison_value(
        PoisonValue(
            reason="missing_key", key=None, value=b"junk", value_bytes_original=4
        )
    )
    expected = (
        b"\x01"  # version
        + (11).to_bytes(2, "big")
        + b"missing_key"
        + b"\x00"  # has_key = 0
        + b"\x01"  # has_value = 1
        + (4).to_bytes(4, "big")
        + b"junk"
        + (4).to_bytes(4, "big")  # value_bytes_original
    )
    assert encoded == expected
    assert decode_poison_value(expected) == PoisonValue(
        reason="missing_key", key=None, value=b"junk", value_bytes_original=4
    )


def test_poison_value_v2_golden_vector_and_legacy_decode():
    """Version 2 appends ``quarantined_at_us`` (u64) to the v1 body — the
    retention purge's age basis. Written out byte by byte; a v1 envelope
    (the golden above) still decodes, as "unknown age"."""
    encoded = encode_poison_value(
        PoisonValue(
            reason="missing_key",
            key=None,
            value=b"junk",
            value_bytes_original=4,
            quarantined_at_us=1_800_000_000_000_000,
        )
    )
    expected = (
        b"\x02"  # version
        + (11).to_bytes(2, "big")
        + b"missing_key"
        + b"\x00"  # has_key = 0
        + b"\x01"  # has_value = 1
        + (4).to_bytes(4, "big")
        + b"junk"
        + (4).to_bytes(4, "big")  # value_bytes_original
        + (1_800_000_000_000_000).to_bytes(8, "big")  # quarantined_at_us
    )
    assert encoded == expected
    assert decode_poison_value(expected) == PoisonValue(
        reason="missing_key",
        key=None,
        value=b"junk",
        value_bytes_original=4,
        quarantined_at_us=1_800_000_000_000_000,
    )
    # The v1 golden decodes as unknown-age (None) — and encoding THAT
    # value reproduces the exact legacy bytes (stamping is the only
    # wire change).
    legacy = (
        b"\x01"
        + (11).to_bytes(2, "big")
        + b"missing_key"
        + b"\x00"
        + b"\x01"
        + (4).to_bytes(4, "big")
        + b"junk"
        + (4).to_bytes(4, "big")
    )
    decoded = decode_poison_value(legacy)
    assert decoded.quarantined_at_us is None
    assert encode_poison_value(decoded) == legacy


def test_poison_value_v2_truncated_timestamp_tail():
    """A v2 envelope cut anywhere inside the 8-byte stamp tail fails
    naming the stage — never a silent None (a purge would then keep an
    entry whose age was simply lost)."""
    value = PoisonValue(
        reason="rk",
        key=b"K",
        value=b"V",
        value_bytes_original=1,
        quarantined_at_us=123,
    )
    encoded = encode_poison_value(value)
    for cut in range(1, 8):
        with pytest.raises(ValueError, match="quarantined_at_us"):
            decode_poison_value(encoded[: len(encoded) - cut])
    assert decode_poison_value(encoded) == value


def test_poison_value_round_trip_shapes():
    cases = [
        PoisonValue(reason="missing_key", key=None, value=b"v", value_bytes_original=1),
        PoisonValue(
            reason="missing_value", key=b"12345", value=None, value_bytes_original=0
        ),
        PoisonValue(
            reason="malformed_key",
            key=b"\xff\xfe",
            value=b"x" * 8,
            value_bytes_original=500,  # truncated store: original > stored
        ),
        PoisonValue(
            reason="missing_timestamp", key=b"0", value=b"", value_bytes_original=0
        ),
    ]
    for value in cases:
        assert decode_poison_value(encode_poison_value(value)) == value


def test_poison_value_validation():
    with pytest.raises(ValueError, match="reason must be non-empty"):
        PoisonValue(reason="", key=None, value=None, value_bytes_original=0)
    with pytest.raises(ValueError, match="value is absent"):
        PoisonValue(reason="r", key=None, value=None, value_bytes_original=5)
    with pytest.raises(ValueError, match="value_bytes_original"):
        PoisonValue(reason="r", key=None, value=b"stored", value_bytes_original=2)
    with pytest.raises(ValueError, match="quarantined_at_us"):
        PoisonValue(
            reason="r",
            key=None,
            value=None,
            value_bytes_original=0,
            quarantined_at_us=-1,
        )
    with pytest.raises(ValueError, match="quarantined_at_us"):
        PoisonValue(
            reason="r",
            key=None,
            value=None,
            value_bytes_original=0,
            quarantined_at_us=2**64,
        )


def test_poison_value_decode_refusals():
    good = encode_poison_value(
        PoisonValue(reason="missing_key", key=b"k", value=b"v", value_bytes_original=1)
    )
    with pytest.raises(ValueError, match="too short"):
        decode_poison_value(b"\x01\x00")
    with pytest.raises(ValueError, match="version"):
        # 3 is the version nobody writes (1 and 2 are both known).
        decode_poison_value(b"\x03" + good[1:])
    # corrupt has_key flag (byte after the reason text)
    bad = bytearray(good)
    bad[3 + len("missing_key")] = 7
    with pytest.raises(ValueError, match="has_key"):
        decode_poison_value(bytes(bad))
    # corrupt has_value flag
    bad2 = bytearray(good)
    has_value_at = 3 + len("missing_key") + 1 + 4 + 1
    bad2[has_value_at] = 9
    with pytest.raises(ValueError, match="has_value"):
        decode_poison_value(bytes(bad2))
    # trailing garbage
    with pytest.raises(ValueError, match="trailing|length mismatch"):
        decode_poison_value(good + b"\x00")


def _expect_truncation_stage(value: PoisonValue, expected: dict[int, str]) -> None:
    """Cut the encoded envelope at each byte boundary: every truncation
    must raise a ValueError naming the exact parse stage that ran out of
    bytes (never an IndexError from an unguarded read), and the full
    envelope must round-trip. Anchored matches: "key" and "key length"
    are different stages."""
    encoded = encode_poison_value(value)
    assert max(expected) == len(encoded) - 1 and min(expected) == 1
    for cut, stage in expected.items():
        with pytest.raises(ValueError, match=stage):
            decode_poison_value(encoded[:cut])
    assert decode_poison_value(encoded) == value


def test_poison_value_truncation_stages_full_envelope():
    # layout: 0 version | 1-2 reason_len | 3-4 "rk" | 5 has_key |
    # 6-9 key_len | 10-12 "KEY" | 13 has_value | 14-17 value_len |
    # 18-22 "VALUE" | 23-26 value_bytes_original — 27 bytes total.
    value = PoisonValue(reason="rk", key=b"KEY", value=b"VALUE", value_bytes_original=9)
    expected: dict[int, str] = {}
    for cut in range(1, 4):
        expected[cut] = "too short"
    for cut in range(4, 6):
        expected[cut] = "reason$"
    for cut in range(6, 10):
        expected[cut] = "key length$"
    for cut in range(10, 14):
        expected[cut] = "key$"
    for cut in range(14, 18):
        expected[cut] = "value length$"
    for cut in range(18, 27):
        expected[cut] = "value$"
    _expect_truncation_stage(value, expected)


def test_poison_value_truncation_stages_without_a_key():
    # The has_key=0 path: nothing before the flags guard has guaranteed
    # the has_value byte exists — a buffer cut exactly there must raise
    # the named stage, not IndexError.
    value = PoisonValue(reason="rk", key=None, value=b"VALUE", value_bytes_original=9)
    expected: dict[int, str] = {}
    for cut in range(1, 4):
        expected[cut] = "too short"
    for cut in range(4, 6):
        expected[cut] = "reason$"
    expected[6] = "flags$"
    for cut in range(7, 11):
        expected[cut] = "value length$"
    for cut in range(11, 20):
        expected[cut] = "value$"
    _expect_truncation_stage(value, expected)


def test_poison_value_truncation_stages_without_a_value():
    # The has_value=0 path: only the u32 original may follow.
    value = PoisonValue(reason="rk", key=b"K", value=None, value_bytes_original=0)
    expected: dict[int, str] = {}
    for cut in range(1, 4):
        expected[cut] = "too short"
    for cut in range(4, 6):
        expected[cut] = "reason$"
    for cut in range(6, 10):
        expected[cut] = "key length$"
    for cut in range(10, 12):
        expected[cut] = "key$"
    for cut in range(12, 16):
        expected[cut] = "length mismatch"
    _expect_truncation_stage(value, expected)


def test_poison_value_decode_rejects_an_impossible_original():
    # A stored original SMALLER than the stored value is corruption.
    encoded = bytearray(
        encode_poison_value(
            PoisonValue(reason="r", key=None, value=b"abcd", value_bytes_original=4)
        )
    )
    encoded[-4:] = (2).to_bytes(4, "big")
    with pytest.raises(ValueError, match="value_bytes_original"):
        decode_poison_value(bytes(encoded))


def test_poison_reason_accepts_exactly_the_u16_maximum():
    # 2**16 - 1 fits reason_len(u16); 2**16 does not. The width is
    # load-bearing: a `>` flipped to `>=` must red here.
    PoisonValue(reason="r" * (2**16 - 1), key=None, value=None, value_bytes_original=0)
    with pytest.raises(ValueError, match="u16"):
        PoisonValue(reason="r" * (2**16), key=None, value=None, value_bytes_original=0)


def test_poison_value_round_trip_with_a_multi_byte_reason_length():
    # A 300-byte reason: the reason_len high byte is nonzero, so a
    # decoder reading only the low byte misreads the split.
    value = PoisonValue(reason="r" * 300, key=b"k", value=b"v", value_bytes_original=1)
    assert decode_poison_value(encode_poison_value(value)) == value


def test_poison_key_and_value_accept_exactly_the_u32_maximum():
    # 2**32 - 1 fits the u32 length fields; 2**32 does not. _FakeLength:
    # the boundary sits at 4 GiB, which no unit test allocates — the
    # checks read only len().
    PoisonValue(
        reason="r", key=_FakeLength(2**32 - 1), value=None, value_bytes_original=0
    )
    with pytest.raises(ValueError, match="poison key too long"):
        PoisonValue(
            reason="r", key=_FakeLength(2**32), value=None, value_bytes_original=0
        )
    PoisonValue(
        reason="r",
        key=None,
        value=_FakeLength(2**32 - 1),
        value_bytes_original=2**32 - 1,
    )
    with pytest.raises(ValueError, match="poison value too long"):
        PoisonValue(
            reason="r",
            key=None,
            value=_FakeLength(2**32),
            value_bytes_original=2**32,
        )


def test_poison_value_bytes_original_accepts_exactly_the_u32_maximum():
    PoisonValue(reason="r", key=None, value=b"", value_bytes_original=2**32 - 1)
    with pytest.raises(ValueError, match="u32"):
        PoisonValue(reason="r", key=None, value=b"", value_bytes_original=2**32)


@given(
    offset=kafka_offsets,
    reason=st.text(
        alphabet=st.characters(categories=("L", "N", "P"), blacklist_characters='"'),
        min_size=1,
        max_size=40,
    ),
    key=st.one_of(st.none(), st.binary(max_size=64)),
    value=st.one_of(st.none(), st.binary(max_size=256)),
    extra=st.integers(min_value=0, max_value=1000),
    quarantined_at=st.one_of(st.none(), st.integers(min_value=0, max_value=2**64 - 1)),
)
def test_poison_round_trip_property(offset, reason, key, value, extra, quarantined_at):
    assert decode_poison_key(poison_key(offset)) == offset
    original = 0 if value is None else len(value) + extra
    envelope = PoisonValue(
        reason=reason,
        key=key,
        value=value,
        value_bytes_original=original,
        quarantined_at_us=quarantined_at,
    )
    assert decode_poison_value(encode_poison_value(envelope)) == envelope


@given(offsets=st.lists(kafka_offsets, min_size=1, max_size=64))
def test_poison_key_order_matches_offset_order(offsets):
    keyed = sorted(offsets)
    assert [
        decode_poison_key(k) for k in sorted(poison_key(o) for o in offsets)
    ] == keyed
