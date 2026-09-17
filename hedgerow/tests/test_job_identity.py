"""Durable job identity must track the JOB, not pyhoglake's dataclass shape.

The identity string is compared byte-for-byte when a coordinator opens its
pending store: a mismatch refuses the store and the claimed work has to be
reconciled by hand. That makes these assertions operational, not cosmetic —
a change that alters the identity without changing the job halts the fleet
at startup, and one that fails to alter it when the job HAS changed lets a
coordinator resume against a table it no longer matches.
"""

import dataclasses

import pytest
from pyhoglake.models import (
    Column,
    PartitionField,
    PartitionSpec,
    SortField,
    SortSpec,
    TableInfo,
)

from hedgerow.ingestion import _table_identity


def info(**overrides):
    base = {
        "name": "events",
        "namespace": "ns",
        "table_uuid": "a3f1c2d4-0000-4000-8000-000000000001",
        "columns": (
            Column("team_id", "long", 1, 0, False),
            Column("timestamp", "timestamptz", 2, 1, False),
            Column("props", "string", 3, 2, True, {"variant": "json"}),
        ),
        "record_count": 0,
        "file_count": 0,
        "file_size_bytes": 0,
        "partition_spec": PartitionSpec(
            1, (PartitionField(1, "identity"), PartitionField(2, "month"))
        ),
        "sort_spec": SortSpec(1, (SortField(2, "asc", "nulls_first"),)),
    }
    base.update(overrides)
    return TableInfo(**base)


def test_a_new_dataclass_field_does_not_change_identity():
    """The regression this file exists for (#90).

    A field added to any of pyhoglake's model dataclasses — or adopted
    from a new server field — used to re-write the identity of every
    running job, because identity was `asdict`. A pyhoglake bump then
    halted the fleet with no change in hedgerow at all.
    """
    before = _table_identity(info())

    # A synthetic field on each dataclass the projection walks, exactly
    # as a library bump would add one. Real subclasses, so the projection
    # sees a genuinely wider object rather than a mock; frozen=True
    # because the pyhoglake models are frozen and Python refuses a
    # non-frozen subclass of a frozen dataclass.
    wider_column = dataclasses.make_dataclass(
        "WiderColumn",
        [("added_by_a_library_bump", str, dataclasses.field(default="x"))],
        bases=(Column,),
        frozen=True,
    )
    wider_partition_field = dataclasses.make_dataclass(
        "WiderPartitionField",
        [("also_new", int, dataclasses.field(default=7))],
        bases=(PartitionField,),
        frozen=True,
    )
    wider_sort_field = dataclasses.make_dataclass(
        "WiderSortField",
        [("also_new", int, dataclasses.field(default=7))],
        bases=(SortField,),
        frozen=True,
    )
    wider_table = dataclasses.make_dataclass(
        "WiderTableInfo",
        [("brand_new_server_field", str, dataclasses.field(default="whatever"))],
        bases=(TableInfo,),
        frozen=True,
    )

    widened = wider_table(
        name="events",
        namespace="ns",
        table_uuid="a3f1c2d4-0000-4000-8000-000000000001",
        columns=(
            wider_column("team_id", "long", 1, 0, False),
            wider_column("timestamp", "timestamptz", 2, 1, False),
            wider_column("props", "string", 3, 2, True, {"variant": "json"}),
        ),
        record_count=0,
        file_count=0,
        file_size_bytes=0,
        partition_spec=PartitionSpec(
            1, (wider_partition_field(1, "identity"), wider_partition_field(2, "month"))
        ),
        sort_spec=SortSpec(1, (wider_sort_field(2, "asc", "nulls_first"),)),
    )

    assert _table_identity(widened) == before


def test_aggregate_counts_are_not_identity():
    """These move on every append; they never defined the job."""
    assert _table_identity(
        info(record_count=10**9, file_count=42, file_size_bytes=7)
    ) == _table_identity(info())


def test_name_and_namespace_are_not_identity():
    """A rename keeps table_uuid, and table_uuid is the incarnation contract."""
    assert _table_identity(
        info(name="renamed", namespace="elsewhere")
    ) == _table_identity(info())


def test_column_order_on_the_wire_is_not_identity():
    """Sorted by field id, so a reordered listing is not a schema change."""
    reordered = info(columns=tuple(reversed(info().columns)))
    assert _table_identity(reordered) == _table_identity(info())


@pytest.mark.parametrize(
    "overrides",
    [
        pytest.param(
            {"table_uuid": "a3f1c2d4-0000-4000-8000-000000000002"}, id="recreated table"
        ),
        pytest.param(
            {"columns": info().columns + (Column("extra", "long", 4, 3, True),)},
            id="column added",
        ),
        pytest.param(
            {
                "columns": (Column("team_id", "string", 1, 0, False),)
                + info().columns[1:]
            },
            id="column type changed",
        ),
        pytest.param(
            {"columns": (Column("team_id", "long", 1, 0, True),) + info().columns[1:]},
            id="nullability changed",
        ),
        pytest.param(
            {"columns": (Column("renamed", "long", 1, 0, False),) + info().columns[1:]},
            id="column renamed",
        ),
        pytest.param(
            {
                "columns": info().columns[:2]
                + (Column("props", "string", 3, 2, True, {"variant": "none"}),)
            },
            id="variant mapping changed",
        ),
        pytest.param(
            {"partition_spec": PartitionSpec(2, (PartitionField(1, "identity"),))},
            id="partition spec",
        ),
        pytest.param({"partition_spec": None}, id="partition spec dropped"),
        pytest.param(
            {
                "partition_spec": PartitionSpec(
                    1, (PartitionField(2, "month"), PartitionField(1, "identity"))
                )
            },
            id="partition field order (semantic: directory nesting)",
        ),
        pytest.param(
            {"sort_spec": SortSpec(1, (SortField(2, "desc", "nulls_first"),))},
            id="sort direction",
        ),
        pytest.param({"sort_spec": None}, id="sort spec dropped"),
    ],
)
def test_a_real_job_change_does_change_identity(overrides):
    """The other half: identity must not be so narrow it misses a real change.

    Every case here is one `_guard` would refuse at runtime, so the two
    must agree — otherwise a coordinator either halts without the guard
    tripping, or resumes against a table the guard would reject.
    """
    assert _table_identity(info(**overrides)) != _table_identity(info())
