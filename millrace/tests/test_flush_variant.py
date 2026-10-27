"""A top-level VARIANT column filled from a payload key's JSON text
(``MILLRACE_VARIANT_SOURCES``), end to end through the decoder, the
build and the flusher.

Pinned here:

- the decoder takes the raw string at the mapped key, unparsed; an
  unmapped (nullable) variant column is SQL NULL; the mapped key is not
  an unknown payload key; a value that is not text counts as invalid
  only on a row that is kept;
- the startup refusals: a mapping naming no top-level variant column, a
  NOT NULL variant column (mapped or not), a declaration this pyhoglake
  cannot write;
- the build encodes each mapped column per file with
  ``on_invalid="null"``: an invalid value is SQL NULL and its row kept
  (no drop path), the file carries ``VARIANT(1)`` in the declared
  layout, the variant registers no stats, and the values follow their
  rows through the sort and the fanout;
- a variant refusal that still reaches the build is a FlushError naming
  the column and reason, never payload text;
- the encode is its own build stage, and the flusher counts rows,
  reasons and declared paths per column;
- a column added under a running flusher (rollout step 3) is written
  SQL NULL by the rebuild on the re-read shape.
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from flushkit import (
    COLUMNS_WIRE,
    DATA_PATH,
    SORT_WIRE,
    TABLE_UUID,
    CommitCapture,
    FakeObjectStore,
    commit_ok,
    commit_refusal,
    event_payload,
    flush_group,
    make_config,
    month_wire,
    parquet_rows,
    script_options,
    script_table_refresh,
    table_wire,
    team_group,
)
from pyhoglake import Column, TableInfo, VariantEncodingError
from pyhoglake.variant import VARIANT_NULL, to_python
from stagekit import NOW

import millrace.flush as flush_mod
from millrace import keyspace
from millrace.coerce import (
    REASON_UNCASTABLE_COLUMN,
    VARIANT_INVALID_UNICODE,
    VARIANT_NOT_TEXT,
    DecodeError,
    UnsupportedColumnError,
)
from millrace.flush import FlushStartupError, JsonObjectDecoder, build_flush_plan

component = pytest.mark.component

DAY_US = 86_400_000_000

#: properties_sv's declaration on ingest.events: every field a string.
DECLARATION: dict[str, Any] = {
    "type": "object",
    "fields": [
        {"name": name, "type": "string"}
        for name in (
            "$session_id",
            "$window_id",
            *(f"$group_{i}" for i in range(5)),
            "$current_url",
            "$pathname",
            "$host",
            "$browser",
            "$os",
            "$device_type",
            "$lib",
            "$referring_domain",
            "$device_id",
        )
    ],
}

PROPERTIES = {"name": "properties", "type": "string", "field_id": 4, "ordinal": 3}
PROPERTIES_SV = {
    "name": "properties_sv",
    "type": "variant",
    "field_id": 5,
    "ordinal": 4,
    "type_params": {"shredding": DECLARATION},
}
#: flushkit's events table plus the text column and its variant companion.
VARIANT_WIRE = [*COLUMNS_WIRE, PROPERTIES, PROPERTIES_SV]
SOURCES = {"properties_sv": "properties"}


def columns(wire: list[dict[str, Any]] | None = None) -> tuple[Column, ...]:
    return tuple(Column.from_wire(c) for c in (wire or VARIANT_WIRE))


def info_for(**kwargs: Any) -> TableInfo:
    return TableInfo.from_wire(table_wire(columns=VARIANT_WIRE, **kwargs))


def plan_for(
    payloads: list[bytes],
    info: TableInfo,
    sources: dict[str, str] | None = SOURCES,
    **kwargs: Any,
) -> flush_mod.FlushPlan:
    return build_flush_plan(
        [(keyspace.RowKey(team_group(7), i), p) for i, p in enumerate(payloads)],
        info,
        ctx=flush_mod.FlushContext(
            topic="events",
            slot=0,
            group=team_group(7),
            group_fields=("team_id", "timestamp_month"),
            trigger="size",
            read_snapshot=88,
            table_uuid=TABLE_UUID,
            build="b0",
        ),
        data_path=DATA_PATH,
        max_files_per_commit=512,
        footer_stats_min_bytes=1,
        version="test",
        variant_sources=sources,
        **kwargs,
    )


def files(plan: flush_mod.FlushPlan) -> list[pq.ParquetFile]:
    return [
        pq.ParquetFile(pa.BufferReader(u.body)) for c in plan.commits for u in c.uploads
    ]


def sv(table: pa.Table) -> list[Any]:
    (column,) = [c for c in columns() if c.name == "properties_sv"]
    return to_python(table.column("properties_sv"), column)


#: (properties as sent, properties_sv as read back): the semantics table.
CASES: list[tuple[Any, Any]] = [
    (
        json.dumps({"$browser": "Chrome", "$os": "Mac"}),
        {"$browser": "Chrome", "$os": "Mac"},
    ),
    (
        json.dumps({"$browser": 5, "extra": [1]}),
        {"$browser": 5, "extra": [1]},
    ),  # type miss
    (json.dumps({}), {}),
    ("null", VARIANT_NULL),  # the text "null" is a Variant null
    (None, None),  # absent key: SQL NULL
    ("", None),  # empty string: SQL NULL
    ('{"$browser":"a","$browser":"b"}', None),  # duplicate key: invalid
    ('{"k":"\\ud800"}', None),  # lone surrogate escape: invalid
    ('{"n":1e400}', None),  # non-finite number: invalid
    ("[" * 200 + "]" * 200, None),  # nested past 128 levels: invalid
    ('{"n":' + "9" * 39 + "}", None),  # an integer of 39 digits: invalid
    ("{not json", None),  # invalid JSON
    ({"$browser": "x"}, None),  # not text: invalid, never encoded
]


#: The encoder's reasons for CASES mapped from "properties".
INVALID_BY_REASON = {
    "duplicate_key": 1,
    "invalid_unicode": 1,
    "non_finite_number": 1,
    "nesting_too_deep": 1,
    "integer_out_of_range": 1,
    "invalid_json": 1,
}


def case_payloads() -> list[bytes]:
    out = []
    for i, (sent, _) in enumerate(CASES):
        extra = {} if sent is None else {"properties": sent}
        if not isinstance(sent, str) and sent is not None:
            # The string column cannot hold an object: map from a key that
            # only the variant reads.
            extra = {"raw": sent}
        out.append(event_payload(7, NOW + i, f"e{i}", **extra))
    return out


# -- the decoder -----------------------------------------------------------------


class TestDecoder:
    def test_a_mapped_column_takes_the_raw_text_and_an_unmapped_one_is_null(
        self,
    ) -> None:
        text = '{"$browser":"a","$browser":"b"}'  # json.loads would keep "b"
        payload = event_payload(7, NOW, properties=text, properties_sv="{}")
        mapped = JsonObjectDecoder(columns(), SOURCES).decode_record(payload)
        assert mapped.values["properties_sv"] == text
        assert mapped.values["properties"] == text
        # Only the mapped key is read: one of the column's own name is an
        # unknown key, reported like any other.
        assert mapped.dropped_keys == ("properties_sv",)
        # Unmapped: SQL NULL, even with a payload key of the column's name.
        unmapped = JsonObjectDecoder(columns()).decode_record(payload)
        assert unmapped.values["properties_sv"] is None
        assert unmapped.dropped_keys == ("properties_sv",)

    def test_the_mapped_key_is_consumed_and_other_unknown_keys_still_drop(
        self,
    ) -> None:
        decoder = JsonObjectDecoder(columns(), {"properties_sv": "props_text"})
        record = decoder.decode_record(event_payload(7, NOW, props_text="{}", stray=1))
        assert record.values["properties_sv"] == "{}"
        assert record.dropped_keys == ("stray",)

    def test_invalid_values_count_only_on_kept_rows(self) -> None:
        decoder = JsonObjectDecoder(columns(), {"properties_sv": "raw"})
        decoder.decode_record(event_payload(7, NOW, raw={"a": 1}))
        decoder.decode_record(event_payload(7, NOW, raw="\ud800"))
        decoder.decode_record(event_payload(7, NOW, raw=""))
        # Dropped by team_id (NOT NULL): its variant value is never counted.
        with pytest.raises(DecodeError):
            decoder.decode_record(json.dumps({"timestamp": NOW, "raw": 5}).encode())
        assert decoder.variant_invalid == {
            ("properties_sv", VARIANT_NOT_TEXT): 1,
            ("properties_sv", VARIANT_INVALID_UNICODE): 1,
        }

    @pytest.mark.parametrize("sent", ["\ud800", 5, {"a": 1}])
    def test_a_string_columns_refusal_drops_first_the_variant_never(
        self, sent: Any
    ) -> None:
        """A key the properties string column also reads keeps that
        column's rules (a lone surrogate Arrow cannot build, a value that
        is not a string); the variant alone never drops the record."""
        payload = event_payload(7, NOW, properties=sent)
        with pytest.raises(DecodeError) as e:
            JsonObjectDecoder(columns(), SOURCES).decode_record(payload)
        assert (e.value.reason, e.value.path) == (
            REASON_UNCASTABLE_COLUMN,
            "properties",
        )
        alone = JsonObjectDecoder(columns(), {"properties_sv": "raw"})
        record = alone.decode_record(event_payload(7, NOW, raw=sent))
        assert record.values["properties_sv"] is None


# -- startup refusals ------------------------------------------------------------


def _nested_variant() -> dict[str, Any]:
    return {
        "name": "s",
        "type": "struct",
        "field_id": 6,
        "ordinal": 5,
        "children": [{"name": "v", "type": "variant", "field_id": 7, "ordinal": 0}],
    }


@pytest.mark.parametrize(
    ("wire", "sources", "needle"),
    [
        (
            VARIANT_WIRE,
            {"missing": "p"},
            "maps column 'missing', which is no top-level",
        ),
        (VARIANT_WIRE, {"event": "p"}, "maps column 'event', which is a string"),
        (
            [*VARIANT_WIRE, _nested_variant()],
            {"v": "p"},
            "maps column 'v', which is no top-level column",
        ),
        (
            [*COLUMNS_WIRE, {**PROPERTIES_SV, "nullable": False}],
            {},
            "variant column 'properties_sv' is NOT NULL",
        ),
        (
            [*COLUMNS_WIRE, {**PROPERTIES_SV, "nullable": False}],
            SOURCES,
            "variant column 'properties_sv' is NOT NULL",
        ),
        (
            [
                *COLUMNS_WIRE,
                {**PROPERTIES_SV, "type_params": {"shredding": {"type": "x"}}},
            ],
            {},
            "column 'properties_sv' has no Arrow form this writer can build",
        ),
        ([*VARIANT_WIRE, _nested_variant()], {}, "'s.v' is a variant below the top"),
    ],
    ids=[
        "missing",
        "not-a-variant",
        "nested",
        "not-null-unmapped",
        "not-null-mapped",
        "unknown-declaration",
        "nested-unmapped",
    ],
)
def test_the_decoder_refuses(
    wire: list[dict[str, Any]], sources: dict[str, str], needle: str
) -> None:
    with pytest.raises(UnsupportedColumnError) as e:
        JsonObjectDecoder(columns(wire), sources)
    assert needle in str(e.value)


@component
async def test_a_mapping_the_live_table_cannot_honor_refuses_at_resolve(
    httpx_mock: Any, flusher_factory: Any, store: FakeObjectStore
) -> None:
    cfg = make_config(variant_sources=(("properties_sv", "properties"),))
    with pytest.raises(FlushStartupError, match="which is no top-level column"):
        flusher_factory(store=store, cfg=cfg, table=table_wire())


# -- the build -------------------------------------------------------------------


class TestBuild:
    def test_every_row_is_kept_and_the_file_carries_the_declared_variant(
        self,
    ) -> None:
        plan = plan_for(case_payloads(), info_for(), {"properties_sv": "properties"})
        # Rows mapped through "raw" read the absent "properties": SQL NULL.
        assert plan.dropped_records == ()
        assert plan.records == len(CASES)
        (parquet,) = files(plan)
        assert "properties_sv (Variant(1))" in str(parquet.schema)
        assert "$referring_domain" in str(parquet.schema)  # the declared layout
        table = parquet.read()
        want = [None if not isinstance(s, str) else w for s, w in CASES]
        assert sv(table) == want
        # The text column is unchanged by its companion, invalid rows too.
        assert table.column("properties").to_pylist() == [
            s if isinstance(s, str) else None for s, _ in CASES
        ]
        (registration,) = plan.commits[0].payload["appends"][0]["files"]
        assert {s["field_id"] for s in registration["column_stats"]} == {1, 2, 3, 4}

    def test_the_report_counts_rows_reasons_and_declared_paths(self) -> None:
        plan = plan_for(case_payloads(), info_for(), {"properties_sv": "raw"})
        report = plan.variant["properties_sv"]
        assert (report.rows, report.sql_null_rows, report.invalid_rows) == (13, 12, 1)
        assert report.invalid_by_reason == {VARIANT_NOT_TEXT: 1}
        plan = plan_for(case_payloads(), info_for())
        report = plan.variant["properties_sv"]
        # 4 encoded, 3 SQL NULL (absent, "", the raw-mapped object), 6 invalid.
        assert (report.rows, report.sql_null_rows, report.invalid_rows) == (13, 3, 6)
        assert report.invalid_by_reason == INVALID_BY_REASON
        assert report.typed[("properties_sv", "$.$browser")] == 1
        assert report.fallback[("properties_sv", "$.$browser")] == 1
        assert report.variant_null[("properties_sv", "$")] == 1

    def test_the_report_sums_every_file_and_both_sides_reasons(self) -> None:
        """Three files, each reason more than once across them: the
        report is every file's, and a reason the decoder found
        (invalid_unicode on the outer string) adds to the encoder's
        (on an escape inside the text)."""
        old = NOW - 40 * DAY_US
        rows: list[tuple[int, int, dict[str, Any]]] = [
            (7, NOW, {"raw": {"a": 1}}),  # not_text
            (7, NOW, {"raw": "\ud800"}),  # invalid_unicode, the decoder's
            (7, NOW, {"raw": '{"k":"a","k":"b"}'}),  # duplicate_key
            (7, NOW, {"raw": '{"$os":"x"}'}),
            (7, old, {"raw": 5}),  # not_text
            (7, old, {"raw": '{"k":"\\ud800"}'}),  # invalid_unicode, the encoder's
            (7, old, {"raw": '{"n":1e400}'}),  # non_finite_number
            (7, old, {}),  # absent: SQL NULL
            (7, old, {"raw": '{"$os":"y"}'}),
            (8, NOW, {"raw": '{"$os":"z"}'}),
            (8, NOW, {"raw": "null"}),
        ]
        payloads = [
            event_payload(team, ts, f"e{i}", **extra)
            for i, (team, ts, extra) in enumerate(rows)
        ]
        plan = plan_for(payloads, info_for(), {"properties_sv": "raw"})
        assert plan.files == 3 and plan.dropped_records == ()
        report = plan.variant["properties_sv"]
        assert (report.rows, report.sql_null_rows, report.invalid_rows) == (11, 1, 6)
        assert report.invalid_by_reason == {
            VARIANT_NOT_TEXT: 2,
            VARIANT_INVALID_UNICODE: 2,
            "duplicate_key": 1,
            "non_finite_number": 1,
        }
        assert report.typed[("properties_sv", "$.$os")] == 3

    def test_text_nested_past_any_stack_is_invalid_on_a_pool_thread(self) -> None:
        """The build runs on a pool thread: depth is the encoder's
        nesting_too_deep there, the row kept, never a RecursionError.
        129 arrays put the innermost at depth 128 (the root is 0), the
        deepest kept."""
        deep = [
            event_payload(7, NOW, f"e{n}", properties="[" * n + "]" * n)
            for n in (129, 130, 200, 100_000)
        ]
        with ThreadPoolExecutor(1) as pool:
            plan = pool.submit(plan_for, deep, info_for()).result()
        assert plan.dropped_records == () and plan.records == 4
        report = plan.variant["properties_sv"]
        assert report.invalid_by_reason == {"nesting_too_deep": 3}

    def test_an_unmapped_column_is_written_all_sql_null(self) -> None:
        plan = plan_for(case_payloads(), info_for(), sources=None)
        assert plan.dropped_records == () and plan.variant == {}
        (parquet,) = files(plan)
        assert "properties_sv (Variant(1))" in str(parquet.schema)
        assert sv(parquet.read()) == [None] * len(CASES)

    def test_values_follow_their_rows_through_the_sort_and_the_fanout(self) -> None:
        """Two teams, two months, arrival order against event time: each
        file holds its own tuple's rows, sorted, each with its own value."""
        payloads = [
            event_payload(
                team,
                ts,
                f"e{i}",
                properties=json.dumps({"$session_id": f"s{i}", "$pathname": f"/{i}"}),
            )
            for i, (team, ts) in enumerate(
                [(7, NOW + 3), (8, NOW + 2), (7, NOW - 40 * DAY_US), (7, NOW + 1)]
            )
        ]
        plan = plan_for(payloads, info_for(sort=SORT_WIRE))
        assert plan.files == 3 and plan.records == 4
        seen = set()
        for parquet in files(plan):
            table = parquet.read()
            stamps = table.column("timestamp").to_pylist()
            assert stamps == sorted(stamps)
            for event, value in zip(table.column("event").to_pylist(), sv(table)):
                i = event.removeprefix("e")
                assert value == {"$session_id": f"s{i}", "$pathname": f"/{i}"}
                seen.add(event)
        assert seen == {"e0", "e1", "e2", "e3"}

    def test_a_table_without_a_variant_writes_no_report(self) -> None:
        info = TableInfo.from_wire(table_wire())
        plan = plan_for([event_payload(7, NOW)], info, sources=None)
        assert plan.variant == {}

    def test_a_variant_refusal_names_the_column_and_reason_never_payload_text(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def refuse(*args: Any, **kwargs: Any) -> Any:
            raise VariantEncodingError(
                "row 0 at $.secret-key: the encoder's own output fails",
                reason="invalid_shredding",
                column="properties_sv",
                row=0,
                path="$.secret-key",
            )

        monkeypatch.setattr(flush_mod, "encode_json", refuse)
        with pytest.raises(flush_mod.FlushError) as e:
            plan_for(case_payloads(), info_for())
        message = str(e.value)
        assert "'properties_sv'" in message and "invalid_shredding" in message
        assert "secret" not in message
        assert e.value.__cause__ is None and e.value.__suppress_context__

    def test_the_encode_is_its_own_stage(self, monkeypatch: pytest.MonkeyPatch) -> None:
        now = [0.0]
        real = flush_mod.encode_json

        def timed(*args: Any, **kwargs: Any) -> Any:
            now[0] += 7.0
            return real(*args, **kwargs)

        monkeypatch.setattr(flush_mod, "encode_json", timed)
        payloads = [event_payload(7, NOW, properties="{}")] * 2 + [
            event_payload(7, NOW - 40 * DAY_US, properties="{}")
        ]
        plan = plan_for(payloads, info_for(), clock=lambda: now[0])
        assert plan.files == 2
        assert plan.stage_seconds["variant_encode"] == 14.0
        assert plan.stage_seconds["write"] == 0.0

    @pytest.mark.parametrize(
        "wire", [COLUMNS_WIRE, VARIANT_WIRE], ids=["none", "unmapped"]
    )
    def test_no_mapped_column_spends_nothing_in_the_encode_stage(
        self, wire: list[dict[str, Any]]
    ) -> None:
        """A clock that ticks on every read: the per-file row-index work
        is the write's, so a table that encodes nothing reports 0."""
        ticks = iter(range(1_000_000))
        payloads = [event_payload(7, NOW), event_payload(7, NOW - 40 * DAY_US)]
        info = TableInfo.from_wire(table_wire(columns=wire))
        plan = plan_for(payloads, info, sources=None, clock=lambda: next(ticks))
        assert plan.files == 2
        assert plan.stage_seconds["variant_encode"] == 0.0
        assert plan.stage_seconds["write"] > 0.0


# -- the flusher -----------------------------------------------------------------


@component
async def test_a_flush_lands_every_row_and_counts_per_column(
    httpx_mock: Any,
    stage_factory: Any,
    fake_clock: Any,
    flusher_factory: Any,
    store: FakeObjectStore,
) -> None:
    from millrace.stage import StagedRecord

    stage = await stage_factory()
    await stage.stage_batch(
        [
            StagedRecord(group=team_group(7), offset=i, payload=p)
            for i, p in enumerate(case_payloads())
        ],
        now_us=fake_clock(),
    )
    script_options(httpx_mock)
    capture = CommitCapture(httpx_mock, [commit_ok(50)])
    flusher = flusher_factory(
        store=store,
        cfg=make_config(variant_sources=(("properties_sv", "properties"),)),
        table=table_wire(columns=VARIANT_WIRE),
    )

    report = await flush_group(flusher, stage, team_group(7), "size")

    assert (report.outcome, report.records, report.dropped) == ("committed", 13, 0)
    (registration,) = capture.payloads[0]["appends"][0]["files"]
    assert registration["partition_values"] == ["7", month_wire(NOW)]
    assert sv(parquet_rows(store, registration["path"]))[3] is VARIANT_NULL
    stats = flusher.stats()
    assert stats.records_dropped == {}
    assert stats.variant_rows == {
        ("properties_sv", "encoded"): 4,
        ("properties_sv", "sql_null"): 3,
        ("properties_sv", "invalid"): 6,
    }
    assert stats.variant_invalid == {
        ("properties_sv", reason): count for reason, count in INVALID_BY_REASON.items()
    }
    # Declared paths only: "extra" (a payload key) is never a label.
    paths = {path for _, path, _ in stats.variant_values}
    assert paths == {"$", *(f"$.{f['name']}" for f in DECLARATION["fields"])}
    values = stats.variant_values
    assert values[("properties_sv", "$.$browser", "typed")] == 1
    assert values[("properties_sv", "$.$browser", "fallback")] == 1
    # $os is typed once and never a fallback: the outcomes are not swapped.
    assert values[("properties_sv", "$.$os", "typed")] == 1
    assert values.get(("properties_sv", "$.$os", "fallback"), 0) == 0
    assert values[("properties_sv", "$", "variant_null")] == 1
    assert stats.build_seconds["variant_encode"] > 0.0

    # A second build adds to every counter (they are Prometheus counters).
    await stage.stage_batch(
        [
            StagedRecord(group=team_group(7), offset=100 + i, payload=p)
            for i, p in enumerate(case_payloads())
        ],
        now_us=fake_clock(),
    )
    capture.extend([commit_ok(51)])
    report = await flush_group(flusher, stage, team_group(7), "size")
    assert (report.outcome, report.records) == ("committed", 13)
    again = flusher.stats()
    assert again.variant_rows == {k: 2 * v for k, v in stats.variant_rows.items()}
    assert again.variant_invalid == {k: 2 * v for k, v in stats.variant_invalid.items()}
    assert again.variant_values == {k: 2 * v for k, v in stats.variant_values.items()}


@component
async def test_a_column_added_under_a_running_flusher_is_written_sql_null(
    httpx_mock: Any,
    stage_factory: Any,
    fake_clock: Any,
    flusher_factory: Any,
    store: FakeObjectStore,
) -> None:
    """Rollout step 3: the operator adds the nullable column while
    millrace runs, before any mapping. The commit built on the old shape
    is refused, the rebuild on the re-read shape writes the new column
    as SQL NULL, and nothing drops or sticks."""
    from millrace.stage import StagedRecord

    stage = await stage_factory()
    await stage.stage_batch(
        [
            StagedRecord(group=team_group(7), offset=i, payload=event_payload(7, NOW))
            for i in range(3)
        ],
        now_us=fake_clock(),
    )
    script_options(httpx_mock)
    ddl = commit_refusal(409, "ddl_since_read_snapshot", "add_column")
    capture = CommitCapture(httpx_mock, [ddl, commit_ok(96)])
    flusher = flusher_factory(store=store, table=table_wire())
    script_table_refresh(
        httpx_mock, table=table_wire(columns=VARIANT_WIRE, read_snapshot_id=95)
    )

    report = await flush_group(flusher, stage, team_group(7), "size")

    assert (report.outcome, report.records, report.dropped) == ("committed", 3, 0)
    assert [p["read_snapshot"] for p in capture.payloads] == [88, 95]
    (registration,) = capture.payloads[1]["appends"][0]["files"]
    rows = parquet_rows(store, registration["path"])
    assert sv(rows) == [None, None, None]
    assert flusher.stats().variant_rows == {}
