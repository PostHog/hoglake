"""destination.py: the destination is created from configuration at
startup (catalog, namespace, table — each if missing, the replica race
tolerated), never altered when it exists, and a missing object the
config cannot create is a ConfigError (exit 2) naming the knob.

Scripted hoglake server (pytest-httpx, test_flush.py's pattern): every
test pins exactly which requests go out — an existing destination sends
only GETs, a created table is ONE atomic table-creations operation with
columns + partition_fields + sort_fields in the prepare body, and a
disagreeing live table produces one WARNING and no alter.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

import httpx
import pytest
from flushkit import (
    BASE,
    CATALOG,
    CATALOG_URL,
    COLUMNS_WIRE,
    NAMESPACE,
    NAMESPACES_URL,
    SORT_WIRE,
    TABLE,
    TABLE_URL,
    TABLE_UUID,
    catalog_wire,
    make_config,
    table_wire,
)
from pyhoglake import HoglakeClient

from millrace.config import (
    ColumnSpec,
    ConfigError,
    PartitionFieldSpec,
    SortFieldSpec,
)
from millrace.destination import (
    DestinationConfigError,
    DestinationError,
    ensure_destination,
)

DATA_PATH = "s3://lake-bkt/lake/"
CREATIONS = re.compile(re.escape(f"{CATALOG_URL}/table-creations/") + r"[0-9a-f-]{36}/")

#: The configured shape matching flushkit's default live table (team_id,
#: timestamp, event; identity(team_id), month(timestamp)).
COLUMNS = (
    ColumnSpec("team_id", "long"),
    ColumnSpec("timestamp", "timestamp"),
    ColumnSpec("event", "string"),
)
PARTITION = (
    PartitionFieldSpec("team_id", "identity"),
    PartitionFieldSpec("timestamp", "month"),
)
#: flushkit's columns with every column nullable — what millrace itself
#: creates, so a live table of this shape AGREES with COLUMNS (the
#: default COLUMNS_WIRE has NOT NULL team_id/timestamp, which the
#: agreement check reports).
NULLABLE_COLUMNS_WIRE = [dict(c, nullable=True) for c in COLUMNS_WIRE]


def atomic_catalog_wire() -> dict[str, Any]:
    return dict(
        catalog_wire(),
        data_path=DATA_PATH,
        capabilities=[
            "atomic-table-creation-v1",
            "atomic-partitioned-table-creation-v1",
            "atomic-sorted-table-creation-v1",
        ],
    )


def receipt(state: str, reason: str | None = None) -> dict[str, Any]:
    out: dict[str, Any] = {
        "operation_id": "11111111-2222-3333-4444-555555555555",
        "table_uuid": TABLE_UUID,
        "namespace": NAMESPACE,
        "name": TABLE,
        "columns": COLUMNS_WIRE,
        "write_path": f"{DATA_PATH}data/{NAMESPACE}/{TABLE}/",
        "state": state,
        "expires_at": "2026-10-14T00:00:00Z",
    }
    if reason is not None:
        out["reason"] = reason
    return out


def not_found(httpx_mock: Any, method: str, url: Any) -> None:
    httpx_mock.add_response(
        method=method, url=url, status_code=404, json={"error": "not_found"}
    )


def conflict(httpx_mock: Any, method: str, url: Any) -> None:
    httpx_mock.add_response(
        method=method, url=url, status_code=409, json={"error": "already_exists"}
    )


def script_catalog(httpx_mock: Any, wire: dict[str, Any] | None = None) -> None:
    httpx_mock.add_response(
        method="GET",
        url=CATALOG_URL,
        json=atomic_catalog_wire() if wire is None else wire,
    )


def script_namespaces(httpx_mock: Any, names: list[str] | None = None) -> None:
    httpx_mock.add_response(
        method="GET",
        url=NAMESPACES_URL,
        json=[{"name": n} for n in ([NAMESPACE] if names is None else names)],
    )


def script_table(httpx_mock: Any, wire: dict[str, Any] | None = None) -> None:
    httpx_mock.add_response(
        method="GET",
        url=f"{TABLE_URL}?totals=false",
        json=table_wire() if wire is None else wire,
    )


def requests(httpx_mock: Any) -> list[tuple[str, str]]:
    return [(r.method, r.url.path) for r in httpx_mock.get_requests()]


def mutations(httpx_mock: Any) -> list[tuple[str, str]]:
    return [(m, p) for m, p in requests(httpx_mock) if m != "GET"]


@pytest.fixture
def client() -> Any:
    with HoglakeClient(BASE) as c:
        yield c


# -- everything exists ------------------------------------------------------------


def test_existing_destination_sends_only_reads(client: Any, httpx_mock: Any):
    script_catalog(httpx_mock)
    script_namespaces(httpx_mock)
    script_table(httpx_mock)
    ensure_destination(
        client,
        make_config(
            catalog_data_path=DATA_PATH,
            table_columns=COLUMNS,
            partition_by=PARTITION,
        ),
    )
    assert mutations(httpx_mock) == []
    # The table read is the identity read, like every other table read.
    assert requests(httpx_mock)[-1] == ("GET", f"/v1{TABLE_URL[len(BASE) + 3 :]}")
    assert str(httpx_mock.get_requests()[-1].url).endswith("?totals=false")


def test_existing_destination_with_nothing_configured_is_fine(
    client: Any, httpx_mock: Any, caplog: pytest.LogCaptureFixture
):
    """No creation knobs at all (the pre-existing deployment shape): the
    destination resolves as before, and nothing is compared."""
    script_catalog(httpx_mock, catalog_wire())
    script_namespaces(httpx_mock)
    script_table(httpx_mock)
    with caplog.at_level(logging.WARNING, logger="millrace.destination"):
        ensure_destination(client, make_config())
    assert mutations(httpx_mock) == []
    assert caplog.records == []


# -- the catalog ----------------------------------------------------------------------


def test_missing_catalog_is_created_with_the_configured_data_path(
    client: Any, httpx_mock: Any, caplog: pytest.LogCaptureFixture
):
    not_found(httpx_mock, "GET", CATALOG_URL)
    httpx_mock.add_response(
        method="POST",
        url=f"{BASE}/v1/catalogs",
        json=atomic_catalog_wire(),
        status_code=201,
    )
    script_namespaces(httpx_mock)
    script_table(httpx_mock)
    with caplog.at_level(logging.INFO, logger="millrace.destination"):
        ensure_destination(client, make_config(catalog_data_path=DATA_PATH))
    (post,) = [r for r in httpx_mock.get_requests() if r.method == "POST"]
    assert json.loads(post.content) == {"name": CATALOG, "data_path": DATA_PATH}
    assert [r.message for r in caplog.records if r.levelno == logging.INFO] == [
        f"millrace created catalog {CATALOG} (data_path={DATA_PATH})"
    ]


def test_missing_catalog_without_a_data_path_is_a_config_refusal(
    client: Any, httpx_mock: Any
):
    not_found(httpx_mock, "GET", CATALOG_URL)
    with pytest.raises(DestinationConfigError) as excinfo:
        ensure_destination(client, make_config())
    assert isinstance(excinfo.value, ConfigError)
    (problem,) = excinfo.value.problems
    assert "MILLRACE_CATALOG_DATA_PATH" in problem and CATALOG in problem
    assert mutations(httpx_mock) == []


def test_catalog_create_race_re_reads_the_winner(client: Any, httpx_mock: Any):
    not_found(httpx_mock, "GET", CATALOG_URL)
    conflict(httpx_mock, "POST", f"{BASE}/v1/catalogs")
    script_catalog(httpx_mock)
    script_namespaces(httpx_mock)
    script_table(httpx_mock)
    ensure_destination(client, make_config(catalog_data_path=DATA_PATH))
    assert mutations(httpx_mock) == [("POST", "/v1/catalogs")]


def test_existing_catalog_with_another_data_path_warns_and_continues(
    client: Any, httpx_mock: Any, caplog: pytest.LogCaptureFixture
):
    script_catalog(httpx_mock)
    script_namespaces(httpx_mock)
    script_table(httpx_mock)
    with caplog.at_level(logging.WARNING, logger="millrace.destination"):
        ensure_destination(
            client, make_config(catalog_data_path="s3://other-bkt/elsewhere/")
        )
    (warning,) = caplog.records
    assert "immutable" in warning.message
    assert (
        DATA_PATH in warning.message and "s3://other-bkt/elsewhere/" in warning.message
    )
    assert mutations(httpx_mock) == []


def test_a_trailing_slash_is_not_a_different_data_path(
    client: Any, httpx_mock: Any, caplog: pytest.LogCaptureFixture
):
    script_catalog(httpx_mock)
    script_namespaces(httpx_mock)
    script_table(httpx_mock)
    with caplog.at_level(logging.WARNING, logger="millrace.destination"):
        ensure_destination(client, make_config(catalog_data_path=DATA_PATH.rstrip("/")))
    assert caplog.records == []


# -- the namespace --------------------------------------------------------------------


def test_missing_namespace_is_created(
    client: Any, httpx_mock: Any, caplog: pytest.LogCaptureFixture
):
    script_catalog(httpx_mock)
    script_namespaces(httpx_mock, ["other"])
    httpx_mock.add_response(
        method="POST", url=NAMESPACES_URL, json={"name": NAMESPACE}, status_code=201
    )
    script_table(httpx_mock)
    with caplog.at_level(logging.INFO, logger="millrace.destination"):
        ensure_destination(client, make_config())
    (post,) = [r for r in httpx_mock.get_requests() if r.method == "POST"]
    assert json.loads(post.content) == {"name": NAMESPACE}
    assert [r.message for r in caplog.records if r.levelno == logging.INFO] == [
        f"millrace created namespace {CATALOG}.{NAMESPACE}"
    ]


def test_namespace_create_race_is_tolerated(client: Any, httpx_mock: Any):
    script_catalog(httpx_mock)
    script_namespaces(httpx_mock, [])
    conflict(httpx_mock, "POST", NAMESPACES_URL)
    script_table(httpx_mock)
    ensure_destination(client, make_config())
    assert mutations(httpx_mock) == [("POST", f"/v1/catalogs/{CATALOG}/namespaces")]


# -- the table ---------------------------------------------------------------------------


def test_missing_table_is_created_atomically_with_spec_and_sort(
    client: Any, httpx_mock: Any, caplog: pytest.LogCaptureFixture
):
    """The create is ONE table-creations operation: the prepare body
    carries the configured columns (all nullable), the partition fields
    and the sort fields by the depth-first ids the server assigns, the
    publish carries no files, and the only other request is the
    read-back. No POST /tables, no /alter."""
    script_catalog(httpx_mock)
    script_namespaces(httpx_mock)
    not_found(httpx_mock, "GET", f"{TABLE_URL}?totals=false")
    httpx_mock.add_response(
        method="PUT",
        url=re.compile(CREATIONS.pattern + "sorted$"),
        json=receipt("prepared"),
    )
    httpx_mock.add_response(
        method="POST",
        url=re.compile(CREATIONS.pattern + "commit$"),
        json=receipt("committed"),
    )
    httpx_mock.add_response(
        method="GET", url=TABLE_URL, json=table_wire(sort=SORT_WIRE)
    )
    with caplog.at_level(logging.INFO, logger="millrace.destination"):
        ensure_destination(
            client,
            make_config(
                table_columns=(
                    *COLUMNS,
                    ColumnSpec("amount", "decimal", (("precision", 10), ("scale", 2))),
                ),
                partition_by=PARTITION,
                sort_by=(SortFieldSpec("timestamp"),),
            ),
        )
    put, post = [r for r in httpx_mock.get_requests() if r.method in ("PUT", "POST")]
    assert put.url.path.rsplit("/", 1)[0] == post.url.path.rsplit("/", 1)[0]
    assert json.loads(put.content) == {
        "namespace": NAMESPACE,
        "name": TABLE,
        "columns": [
            {"name": "team_id", "type": "long", "nullable": True},
            {"name": "timestamp", "type": "timestamp", "nullable": True},
            {"name": "event", "type": "string", "nullable": True},
            {
                "name": "amount",
                "type": "decimal",
                "nullable": True,
                "type_params": {"precision": 10, "scale": 2},
            },
        ],
        "partition_fields": [
            {"source_field_id": 1, "transform": "identity"},
            {"source_field_id": 2, "transform": "month"},
        ],
        "sort_fields": [
            {"source_field_id": 2, "direction": "asc", "null_order": "nulls_last"}
        ],
    }
    assert json.loads(post.content) == {"files": []}
    assert mutations(httpx_mock) == [
        ("PUT", put.url.path),
        ("POST", post.url.path),
    ]
    (created,) = [r.message for r in caplog.records if r.levelno == logging.INFO]
    assert created.startswith(f"millrace created table {CATALOG}/{NAMESPACE}/{TABLE}")
    assert "identity(team_id), month(timestamp)" in created
    assert "sort by [timestamp asc nulls_last]" in created


def test_missing_table_with_columns_only_is_the_plain_create(
    client: Any, httpx_mock: Any
):
    """No partition or sort configured: an unpartitioned, unsorted
    table through the plain create."""
    script_catalog(httpx_mock)
    script_namespaces(httpx_mock)
    not_found(httpx_mock, "GET", f"{TABLE_URL}?totals=false")
    httpx_mock.add_response(
        method="POST",
        url=f"{CATALOG_URL}/namespaces/{NAMESPACE}/tables",
        json=table_wire(spec={"spec_id": 0, "fields": []}),
        status_code=201,
    )
    ensure_destination(client, make_config(table_columns=COLUMNS))
    (post,) = [r for r in httpx_mock.get_requests() if r.method == "POST"]
    assert json.loads(post.content) == {
        "name": TABLE,
        "columns": [
            {"name": "team_id", "type": "long", "nullable": True},
            {"name": "timestamp", "type": "timestamp", "nullable": True},
            {"name": "event", "type": "string", "nullable": True},
        ],
    }


def test_missing_table_without_columns_is_a_config_refusal(
    client: Any, httpx_mock: Any
):
    script_catalog(httpx_mock)
    script_namespaces(httpx_mock)
    not_found(httpx_mock, "GET", f"{TABLE_URL}?totals=false")
    with pytest.raises(DestinationConfigError) as excinfo:
        ensure_destination(client, make_config(partition_by=PARTITION))
    (problem,) = excinfo.value.problems
    assert "MILLRACE_TABLE_COLUMNS" in problem
    assert f"{CATALOG}/{NAMESPACE}/{TABLE}" in problem
    assert mutations(httpx_mock) == []


def test_table_create_race_re_reads_the_winner_and_compares(
    client: Any, httpx_mock: Any, caplog: pytest.LogCaptureFixture
):
    """The loser's publish is refused target_exists (HTTP 200, the
    receipt says so) → re-read, no failure; the winner's table is then
    compared like any existing one (here: it agrees, no warning)."""
    script_catalog(httpx_mock)
    script_namespaces(httpx_mock)
    not_found(httpx_mock, "GET", f"{TABLE_URL}?totals=false")
    httpx_mock.add_response(
        method="PUT",
        url=re.compile(CREATIONS.pattern + "partitioned$"),
        json=receipt("prepared"),
    )
    httpx_mock.add_response(
        method="POST",
        url=re.compile(CREATIONS.pattern + "commit$"),
        json=receipt("rejected", "target_exists"),
    )
    script_table(httpx_mock, table_wire(columns=NULLABLE_COLUMNS_WIRE))
    with caplog.at_level(logging.INFO, logger="millrace.destination"):
        ensure_destination(
            client, make_config(table_columns=COLUMNS, partition_by=PARTITION)
        )
    assert any("created by another replica" in r.message for r in caplog.records)
    assert not [r for r in caplog.records if r.levelno == logging.WARNING]
    # One prepare and one publish, then the re-read: no second attempt.
    assert [m for m, _ in mutations(httpx_mock)] == ["PUT", "POST"]


def test_table_create_race_through_the_plain_create_409(
    client: Any, httpx_mock: Any, caplog: pytest.LogCaptureFixture
):
    script_catalog(httpx_mock)
    script_namespaces(httpx_mock)
    not_found(httpx_mock, "GET", f"{TABLE_URL}?totals=false")
    conflict(httpx_mock, "POST", f"{CATALOG_URL}/namespaces/{NAMESPACE}/tables")
    script_table(httpx_mock, table_wire(columns=NULLABLE_COLUMNS_WIRE))
    with caplog.at_level(logging.INFO, logger="millrace.destination"):
        ensure_destination(client, make_config(table_columns=COLUMNS))
    assert mutations(httpx_mock) == [
        ("POST", f"/v1/catalogs/{CATALOG}/namespaces/{NAMESPACE}/tables")
    ]
    assert any("created by another replica" in r.message for r in caplog.records)
    assert not [r for r in caplog.records if r.levelno == logging.WARNING]


def test_existing_table_that_differs_warns_once_and_never_alters(
    client: Any, httpx_mock: Any, caplog: pytest.LogCaptureFixture
):
    """Columns, spec and sort all differ: ONE warning carrying every
    difference with both sides in the knobs' grammar; no alter."""
    script_catalog(httpx_mock)
    script_namespaces(httpx_mock)
    script_table(httpx_mock, table_wire(sort=SORT_WIRE))
    with caplog.at_level(logging.WARNING, logger="millrace.destination"):
        ensure_destination(
            client,
            make_config(
                table_columns=(
                    ColumnSpec("team_id", "long"),
                    ColumnSpec("timestamp", "timestamptz"),
                    ColumnSpec("distinct_id", "string"),
                ),
                partition_by=(
                    PartitionFieldSpec("team_id", "identity"),
                    PartitionFieldSpec("timestamp", "day"),
                ),
                sort_by=(SortFieldSpec("team_id"), SortFieldSpec("timestamp")),
            ),
        )
    (warning,) = caplog.records
    msg = warning.message
    assert "never alters" in msg
    assert "column timestamp is timestamp in the live table, timestamptz" in msg
    assert "names distinct_id:string, which the live table lacks" in msg
    assert "carries column(s) MILLRACE_TABLE_COLUMNS does not: event:string" in msg
    assert (
        "live partition spec [identity(team_id), month(timestamp)] != "
        "MILLRACE_PARTITION_BY [identity(team_id), day(timestamp)]"
    ) in msg
    assert (
        "live sort order [timestamp asc nulls_first] != "
        "MILLRACE_SORT_BY [team_id asc nulls_last, timestamp asc nulls_last]"
    ) in msg
    assert mutations(httpx_mock) == []


def test_existing_table_that_agrees_is_silent(
    client: Any, httpx_mock: Any, caplog: pytest.LogCaptureFixture
):
    script_catalog(httpx_mock)
    script_namespaces(httpx_mock)
    script_table(
        httpx_mock,
        table_wire(
            columns=NULLABLE_COLUMNS_WIRE,
            sort={
                "sort_id": 1,
                "fields": [
                    {
                        "source_field_id": 2,
                        "direction": "asc",
                        "null_order": "nulls_last",
                    }
                ],
            },
        ),
    )
    with caplog.at_level(logging.WARNING, logger="millrace.destination"):
        ensure_destination(
            client,
            make_config(
                catalog_data_path=DATA_PATH,
                table_columns=COLUMNS,
                partition_by=PARTITION,
                sort_by=(SortFieldSpec("timestamp"),),
            ),
        )
    assert caplog.records == []


def test_unpartitioned_live_table_against_a_configured_spec_warns(
    client: Any, httpx_mock: Any, caplog: pytest.LogCaptureFixture
):
    script_catalog(httpx_mock)
    script_namespaces(httpx_mock)
    script_table(httpx_mock, table_wire(spec={"spec_id": 0, "fields": []}))
    with caplog.at_level(logging.WARNING, logger="millrace.destination"):
        ensure_destination(client, make_config(partition_by=PARTITION))
    (warning,) = caplog.records
    assert "live partition spec [] != MILLRACE_PARTITION_BY" in warning.message


def test_a_server_failure_is_a_startup_error_not_a_config_one(
    client: Any, httpx_mock: Any
):
    httpx_mock.add_response(
        method="GET", url=CATALOG_URL, status_code=503, json={"error": "busy"}
    )
    with pytest.raises(DestinationError, match="cannot ensure"):
        ensure_destination(client, make_config(catalog_data_path=DATA_PATH))


# -- review additions -------------------------------------------------------------


def test_sort_only_uses_the_sorted_endpoint_without_partition_fields(
    client: Any, httpx_mock: Any
):
    script_catalog(httpx_mock)
    script_namespaces(httpx_mock)
    not_found(httpx_mock, "GET", f"{TABLE_URL}?totals=false")
    httpx_mock.add_response(
        method="PUT",
        url=re.compile(CREATIONS.pattern + "sorted$"),
        json=receipt("prepared"),
    )
    httpx_mock.add_response(
        method="POST",
        url=re.compile(CREATIONS.pattern + "commit$"),
        json=receipt("committed"),
    )
    httpx_mock.add_response(
        method="GET", url=TABLE_URL, json=table_wire(sort=SORT_WIRE)
    )
    ensure_destination(
        client,
        make_config(
            table_columns=COLUMNS,
            sort_by=(SortFieldSpec("timestamp", "desc", "nulls_first"),),
        ),
    )
    (put,) = [r for r in httpx_mock.get_requests() if r.method == "PUT"]
    body = json.loads(put.content)
    assert "partition_fields" not in body
    assert body["sort_fields"] == [
        {"source_field_id": 2, "direction": "desc", "null_order": "nulls_first"}
    ]


def test_catalog_without_the_atomic_capability_is_refused_before_any_write(
    client: Any, httpx_mock: Any
):
    """Millrace never takes pyhoglake's create + alter fallback (a
    sibling replica could stage under the empty spec between the two):
    a catalog that does not advertise the capability is a startup
    failure naming it, with no POST or PUT sent."""
    script_catalog(httpx_mock, catalog_wire())
    script_namespaces(httpx_mock)
    not_found(httpx_mock, "GET", f"{TABLE_URL}?totals=false")
    with pytest.raises(DestinationError) as excinfo:
        ensure_destination(
            client, make_config(table_columns=COLUMNS, partition_by=PARTITION)
        )
    assert "atomic-partitioned-table-creation-v1" in str(excinfo.value)
    assert mutations(httpx_mock) == []


def test_sort_only_creation_needs_only_the_sorted_capability(
    client: Any, httpx_mock: Any, caplog: Any
):
    """A sort order with no partition spec requires the sorted
    capability alone: the partitioned one is for a partition spec."""
    script_catalog(
        httpx_mock,
        dict(atomic_catalog_wire(), capabilities=["atomic-sorted-table-creation-v1"]),
    )
    script_namespaces(httpx_mock)
    not_found(httpx_mock, "GET", f"{TABLE_URL}?totals=false")
    httpx_mock.add_response(
        method="PUT",
        url=re.compile(CREATIONS.pattern + "sorted$"),
        json=receipt("prepared"),
    )
    httpx_mock.add_response(
        method="POST",
        url=re.compile(CREATIONS.pattern + "commit$"),
        json=receipt("committed"),
    )
    httpx_mock.add_response(
        method="GET",
        url=TABLE_URL,
        json=table_wire(columns=NULLABLE_COLUMNS_WIRE, sort=SORT_WIRE),
    )
    with caplog.at_level(logging.INFO, logger="millrace.destination"):
        ensure_destination(
            client,
            make_config(table_columns=COLUMNS, sort_by=(SortFieldSpec("timestamp"),)),
        )
    assert any("millrace created table" in r.message for r in caplog.records)
    assert [m for m, _ in mutations(httpx_mock)] == ["PUT", "POST"]


def test_sorted_creation_also_requires_the_sorted_capability(
    client: Any, httpx_mock: Any
):
    script_catalog(
        httpx_mock,
        dict(
            atomic_catalog_wire(), capabilities=["atomic-partitioned-table-creation-v1"]
        ),
    )
    script_namespaces(httpx_mock)
    not_found(httpx_mock, "GET", f"{TABLE_URL}?totals=false")
    with pytest.raises(DestinationError, match="atomic-sorted-table-creation-v1"):
        ensure_destination(
            client,
            make_config(
                table_columns=COLUMNS,
                partition_by=PARTITION,
                sort_by=(SortFieldSpec("timestamp"),),
            ),
        )
    assert mutations(httpx_mock) == []


def test_a_rejection_other_than_target_exists_is_a_startup_error(
    client: Any, httpx_mock: Any
):
    script_catalog(httpx_mock)
    script_namespaces(httpx_mock)
    not_found(httpx_mock, "GET", f"{TABLE_URL}?totals=false")
    httpx_mock.add_response(
        method="PUT",
        url=re.compile(CREATIONS.pattern + "partitioned$"),
        json=receipt("prepared"),
    )
    httpx_mock.add_response(
        method="POST",
        url=re.compile(CREATIONS.pattern + "commit$"),
        json=receipt("rejected", "namespace_changed"),
    )
    with pytest.raises(DestinationError, match="namespace_changed"):
        ensure_destination(
            client, make_config(table_columns=COLUMNS, partition_by=PARTITION)
        )


def test_already_exists_then_not_found_is_a_startup_error(client: Any, httpx_mock: Any):
    script_catalog(httpx_mock)
    script_namespaces(httpx_mock)
    not_found(httpx_mock, "GET", f"{TABLE_URL}?totals=false")
    conflict(httpx_mock, "POST", f"{CATALOG_URL}/namespaces/{NAMESPACE}/tables")
    not_found(httpx_mock, "GET", f"{TABLE_URL}?totals=false")
    with pytest.raises(DestinationError, match="already-exists"):
        ensure_destination(client, make_config(table_columns=COLUMNS))


def test_a_connection_error_is_a_startup_error(client: Any, httpx_mock: Any):
    httpx_mock.add_exception(
        httpx.ConnectError("refused"), method="GET", url=CATALOG_URL
    )
    with pytest.raises(DestinationError, match="refused"):
        ensure_destination(client, make_config())


def test_a_403_is_a_startup_error_naming_the_status(client: Any, httpx_mock: Any):
    httpx_mock.add_response(
        method="GET", url=CATALOG_URL, status_code=403, json={"error": "forbidden"}
    )
    with pytest.raises(DestinationError, match="HTTP 403"):
        ensure_destination(client, make_config())


def _warning(client: Any, httpx_mock: Any, caplog: Any, cfg: Any, wire: Any) -> str:
    script_catalog(httpx_mock)
    script_namespaces(httpx_mock)
    script_table(httpx_mock, wire)
    with caplog.at_level(logging.WARNING, logger="millrace.destination"):
        ensure_destination(client, cfg)
    (warning,) = caplog.records
    assert mutations(httpx_mock) == []
    return str(warning.message)


def test_decimal_params_take_part_in_the_comparison(
    client: Any, httpx_mock: Any, caplog: pytest.LogCaptureFixture
):
    live = [
        *COLUMNS_WIRE,
        {
            "name": "amount",
            "type": "decimal",
            "field_id": 4,
            "ordinal": 3,
            "nullable": True,
            "type_params": {"precision": 10, "scale": 2},
        },
    ]
    msg = _warning(
        client,
        httpx_mock,
        caplog,
        make_config(
            table_columns=(
                *COLUMNS,
                ColumnSpec("amount", "decimal", (("precision", 12), ("scale", 2))),
            )
        ),
        table_wire(columns=live),
    )
    assert (
        "column amount is decimal(10,2) in the live table, decimal(12,2) in "
        "MILLRACE_TABLE_COLUMNS"
    ) in msg


def test_a_not_null_live_column_is_reported(
    client: Any, httpx_mock: Any, caplog: pytest.LogCaptureFixture
):
    # flushkit's team_id and timestamp are NOT NULL.
    msg = _warning(
        client, httpx_mock, caplog, make_config(table_columns=COLUMNS), table_wire()
    )
    assert (
        "column team_id is NOT NULL in the live table; millrace creates nullable "
        "columns and cannot fill it"
    ) in msg
    assert "column timestamp is NOT NULL" in msg
    assert "column event is NOT NULL" not in msg


def test_an_operator_added_variant_column_is_no_difference(
    client: Any, httpx_mock: Any, caplog: pytest.LogCaptureFixture
):
    """MILLRACE_TABLE_COLUMNS cannot declare a variant, so a live one is
    not reported as a column the knob lacks (millrace fills it through
    MILLRACE_VARIANT_SOURCES); another extra column still is."""
    live = [
        *COLUMNS_WIRE,
        {"name": "properties_sv", "type": "variant", "field_id": 4, "ordinal": 3},
        {"name": "extra", "type": "string", "field_id": 5, "ordinal": 4},
    ]
    msg = _warning(
        client,
        httpx_mock,
        caplog,
        make_config(table_columns=COLUMNS),
        table_wire(columns=live),
    )
    assert "does not: extra:string (millrace never writes them)" in msg
    assert "properties_sv" not in msg


def test_a_nested_live_partition_source_is_rendered_by_path(
    client: Any, httpx_mock: Any, caplog: pytest.LogCaptureFixture
):
    live = [
        {
            "name": "properties",
            "type": "struct",
            "field_id": 1,
            "ordinal": 0,
            "nullable": True,
            "children": [
                {
                    "name": "team_id",
                    "type": "long",
                    "field_id": 2,
                    "ordinal": 0,
                    "nullable": True,
                }
            ],
        },
    ]
    msg = _warning(
        client,
        httpx_mock,
        caplog,
        make_config(partition_by=(PartitionFieldSpec("team_id", "identity"),)),
        table_wire(
            columns=live,
            spec={
                "spec_id": 1,
                "fields": [{"source_field_id": 2, "transform": "identity"}],
            },
        ),
    )
    assert (
        "live partition spec [identity(properties.team_id)] != "
        "MILLRACE_PARTITION_BY [identity(team_id)]"
    ) in msg


def test_a_transform_param_difference_is_a_difference(
    client: Any, httpx_mock: Any, caplog: pytest.LogCaptureFixture
):
    msg = _warning(
        client,
        httpx_mock,
        caplog,
        make_config(partition_by=(PartitionFieldSpec("team_id", "bucket", 32),)),
        table_wire(
            spec={
                "spec_id": 1,
                "fields": [
                    {"source_field_id": 1, "transform": "bucket", "transform_param": 16}
                ],
            }
        ),
    )
    assert (
        "live partition spec [bucket(team_id, 16)] != "
        "MILLRACE_PARTITION_BY [bucket(team_id, 32)]"
    ) in msg
