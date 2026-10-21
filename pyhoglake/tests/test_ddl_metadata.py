import json
import re
from pathlib import Path

import httpx
import pyarrow as pa
import pytest
from test_client import CATALOG_WIRE, TABLE_WIRE, _receipt

from pyhoglake import (
    CatalogFilesPendingExpiryError,
    CatalogHasAbandonedUploadsError,
    CatalogHasActiveCompactionClaimsError,
    CatalogHasActiveUploadsError,
    CatalogHasConsumerOffsetsError,
    CatalogHasLiveNamespacesError,
    CatalogNotEmptyError,
    CatalogRemovalsPendingError,
    CatalogTablesPendingRetirementError,
    DroppedCatalog,
    HoglakeClient,
    NotFoundError,
    ValidationError,
    ops,
)
from pyhoglake.client import Catalog, Namespace, Table, View
from pyhoglake.models import CatalogInfo, TableInfo, ViewInfo

BASE = "http://hog.test/v1"
METADATA = {"author": " qa &+=?#/ é ", "message": "drop 50% & reason=one+two\n✓"}
VIEW_WIRE = {
    "name": "v",
    "namespace": "ns1",
    "view_uuid": "view-id",
    "sql": "SELECT 1",
    "dialect": "trino",
}


@pytest.fixture
def objects():
    with HoglakeClient("http://hog.test") as client:
        catalog = Catalog(client, CatalogInfo.from_wire(CATALOG_WIRE))
        namespace = Namespace(catalog, "ns1")
        table = Table(
            namespace,
            TableInfo.from_wire(
                {**TABLE_WIRE, "snapshot_id": 12, "read_snapshot_id": 12}
            ),
        )
        view = View(namespace, ViewInfo.from_wire(VIEW_WIRE))
        yield client, catalog, namespace, table, view


@pytest.mark.parametrize(
    "metadata",
    [{}, {"author": METADATA["author"]}, {"message": METADATA["message"]}, METADATA],
)
@pytest.mark.parametrize(
    "operation", ["catalog", "namespace", "table", "view", "alter", "retention"]
)
def test_optional_metadata_is_forwarded_exactly(
    objects, httpx_mock, operation, metadata
):
    client, catalog, namespace, table, _ = objects
    options = {"consumer_floor": True, "earliest_snapshot_id": 0}
    cases = {
        "catalog": (
            "POST",
            "/catalogs",
            {"name": "cat", "data_path": "s3://bucket/lake"},
            CATALOG_WIRE,
            lambda: client.create_catalog("cat", "s3://bucket/lake", **metadata),
        ),
        "namespace": (
            "POST",
            "/catalogs/cat/namespaces",
            {"name": "ns2"},
            {},
            lambda: catalog.create_namespace("ns2", **metadata),
        ),
        "table": (
            "POST",
            "/catalogs/cat/namespaces/ns1/tables",
            {
                "name": "events",
                "columns": [{"name": "id", "type": "long", "nullable": False}],
            },
            TABLE_WIRE,
            lambda: namespace.create_table(
                "events",
                pa.schema([pa.field("id", pa.int64(), nullable=False)]),
                **metadata,
            ),
        ),
        "view": (
            "POST",
            "/catalogs/cat/namespaces/ns1/views",
            {"name": "v", "sql": "SELECT 1", "dialect": "trino"},
            VIEW_WIRE,
            lambda: namespace.create_view("v", "SELECT 1", **metadata),
        ),
        "alter": (
            "POST",
            "/catalogs/cat/namespaces/ns1/tables/events/alter",
            {"ops": [{"op": "rename_table", "new_name": "new"}]},
            TABLE_WIRE,
            lambda: table.alter([ops.rename_table("new")], **metadata),
        ),
        "retention": (
            "PATCH",
            "/catalogs/cat/options",
            {"snapshot_retention_seconds": None},
            options,
            lambda: catalog.set_retention(None, **metadata),
        ),
    }
    method, path, payload, response, call = cases[operation]
    httpx_mock.add_response(method=method, url=BASE + path, json=response)
    call()
    request = httpx_mock.get_requests()[-1]
    assert json.loads(request.content) == {**payload, **metadata}
    assert not request.url.query


@pytest.mark.parametrize(
    "metadata",
    [{}, {"author": METADATA["author"]}, {"message": METADATA["message"]}, METADATA],
)
@pytest.mark.parametrize("shape", ["partitioned", "sorted", "both"])
@pytest.mark.parametrize("atomic", [True, False])
def test_create_with_initial_specs_preserves_metadata(
    objects, httpx_mock, metadata, shape, atomic
):
    client, _, _, _, _ = objects
    capabilities = (
        ["atomic-partitioned-table-creation-v1", "atomic-sorted-table-creation-v1"]
        if atomic
        else []
    )
    catalog = Catalog(
        client, CatalogInfo.from_wire({**CATALOG_WIRE, "capabilities": capabilities})
    )
    namespace = Namespace(catalog, "ns1")
    path = BASE + "/catalogs/cat/namespaces/ns1/tables"
    if atomic:
        endpoint = "partitioned" if shape == "partitioned" else "sorted"
        operation = re.escape(BASE + "/catalogs/cat/table-creations/")
        httpx_mock.add_response(
            method="PUT",
            url=re.compile(operation + "[0-9a-f-]{36}/" + endpoint),
            json=_receipt("prepared"),
        )
        httpx_mock.add_response(
            method="POST",
            url=re.compile(operation + "[0-9a-f-]{36}/commit"),
            json=_receipt("committed"),
        )
        httpx_mock.add_response(method="GET", url=path + "/events", json=TABLE_WIRE)
    else:
        httpx_mock.add_response(method="POST", url=path, json=TABLE_WIRE)
        httpx_mock.add_response(
            method="POST", url=path + "/events/alter", json=TABLE_WIRE
        )
    namespace.create_table(
        "events",
        pa.schema([pa.field("id", pa.int64(), nullable=False)]),
        partition_fields=[ops.NamedPartitionField("id")]
        if shape in ("partitioned", "both")
        else None,
        sort_fields=[ops.NamedSortField("id")] if shape in ("sorted", "both") else None,
        **metadata,
    )
    requests = httpx_mock.get_requests()
    for request in requests[:1] if atomic else requests:
        payload = json.loads(request.content)
        assert {
            k: payload[k] for k in ("author", "message") if k in payload
        } == metadata
    if atomic:
        assert json.loads(requests[1].content) == {"files": []}
    assert len(requests) == (3 if atomic else 2)


@pytest.mark.parametrize("operation", ["table", "view", "namespace"])
def test_drop_metadata_survives_query_encoding(objects, httpx_mock, operation):
    _, _, namespace, table, view = objects
    cases = {
        "table": (
            "/catalogs/cat/namespaces/ns1/tables/events",
            METADATA,
            lambda: table.drop(**METADATA),
        ),
        "view": (
            "/catalogs/cat/namespaces/ns1/views/v",
            METADATA,
            lambda: view.drop(**METADATA),
        ),
        "namespace": (
            "/catalogs/cat/namespaces/ns1",
            {**METADATA, "expected_namespace_id": "42"},
            lambda: namespace.drop(expected_namespace_id=42, **METADATA),
        ),
    }
    path, params, call = cases[operation]
    httpx_mock.add_response(
        method="DELETE",
        url=httpx.URL(BASE + path, params=params),
        json={"snapshot_id": 13},
    )
    assert call().snapshot_id == 13
    request = httpx_mock.get_requests()[-1]
    assert request.url.path == "/v1" + path
    assert dict(request.url.params) == params
    assert request.content == b""
    assert b"%25" in request.url.query


def test_namespace_drop_reads_identity_before_delete(objects, httpx_mock):
    _, _, namespace, _, _ = objects
    path = BASE + "/catalogs/cat/namespaces/ns1"
    httpx_mock.add_response(
        method="GET", url=path, json={"namespace_id": 42, "name": "ns1"}
    )
    httpx_mock.add_response(
        method="DELETE",
        url=httpx.URL(path, params={**METADATA, "expected_namespace_id": "42"}),
        json={"snapshot_id": 13},
    )
    namespace.drop(**METADATA)
    assert [r.method for r in httpx_mock.get_requests()] == ["GET", "DELETE"]


@pytest.mark.parametrize("operation", ["table", "view", "namespace", "truncate"])
@pytest.mark.parametrize("field", ["author", "message"])
@pytest.mark.parametrize("blank", [None, "", "\t\n ", "\u00a0", "\u0085"])
def test_destructive_metadata_is_validated_before_network(
    objects, httpx_mock, operation, field, blank
):
    _, _, namespace, table, view = objects
    calls = {
        "table": table.drop,
        "view": view.drop,
        "namespace": namespace.drop,
        "truncate": table.truncate,
    }
    metadata = {**METADATA, field: blank}
    with pytest.raises(ValidationError, match=field) as error:
        calls[operation](**metadata)
    assert error.value.status_code is None
    assert httpx_mock.get_requests() == []


@pytest.mark.parametrize("operation", ["table", "view", "namespace", "truncate"])
@pytest.mark.parametrize("field", ["author", "message"])
def test_destructive_metadata_keywords_are_required(
    objects, httpx_mock, operation, field
):
    _, _, namespace, table, view = objects
    calls = {
        "table": table.drop,
        "view": view.drop,
        "namespace": namespace.drop,
        "truncate": table.truncate,
    }
    with pytest.raises(TypeError, match=field):
        calls[operation](**{k: v for k, v in METADATA.items() if k != field})
    assert httpx_mock.get_requests() == []


@pytest.mark.parametrize("success", [True, False])
def test_truncate_sends_identity_guard_and_updates_cache_only_on_success(
    objects, httpx_mock, success
):
    _, _, _, table, _ = objects
    old_cache = table._cache
    assert old_cache is not None
    path = BASE + "/catalogs/cat/namespaces/ns1/tables/events/truncate"
    httpx_mock.add_response(
        method="POST",
        url=httpx.URL(path, params={"expected_table_uuid": TABLE_WIRE["table_uuid"]}),
        status_code=200 if success else 422,
        json={"snapshot_id": 13}
        if success
        else {"error": "validation", "detail": "refused"},
    )
    if success:
        assert table.truncate(**METADATA).snapshot_id == 13
        assert table.snapshot_id == 13
        assert table._cache is None
    else:
        with pytest.raises(ValidationError):
            table.truncate(**METADATA)
        assert table.snapshot_id == 12
        assert table._cache is old_cache
    assert table.table_uuid == TABLE_WIRE["table_uuid"]
    request = httpx_mock.get_requests()[-1]
    assert json.loads(request.content) == METADATA
    assert dict(request.url.params) == {"expected_table_uuid": TABLE_WIRE["table_uuid"]}


# ---- Catalog.drop (#412) -----------------------------------------------------

_SPEC = Path(__file__).resolve().parents[2] / (
    "server/src/main/resources/openapi/hoglake.yaml"
)


def test_catalog_drop_sends_delete_with_metadata_as_query(objects, httpx_mock):
    _, catalog, _, _, _ = objects
    path = "/catalogs/cat"
    httpx_mock.add_response(
        method="DELETE",
        url=httpx.URL(BASE + path, params=METADATA),
        json={"name": "cat", "data_path": "s3://bucket/lake"},
    )
    dropped = catalog.drop(**METADATA)
    assert dropped == DroppedCatalog(name="cat", data_path="s3://bucket/lake")
    request = httpx_mock.get_requests()[-1]
    assert request.method == "DELETE"
    assert request.url.path == "/v1" + path
    assert dict(request.url.params) == METADATA
    assert request.content == b""
    assert b"%25" in request.url.query
    # Exactly one request: unlike Namespace.drop there is no identity read.
    assert len(httpx_mock.get_requests()) == 1


def test_catalog_drop_clears_the_retention_cache(objects, httpx_mock):
    # A reused Catalog after a same-name recreate must not apply the
    # dropped catalog's retention to the new one's writer path.
    _, catalog, _, _, _ = objects
    catalog._retention_cache = 3600.0
    catalog._retention_failed_at = 1.0
    httpx_mock.add_response(
        method="DELETE",
        url=httpx.URL(BASE + "/catalogs/cat", params=METADATA),
        json={"name": "cat", "data_path": "s3://bucket/lake"},
    )
    catalog.drop(**METADATA)
    assert catalog._retention_cache is None
    assert catalog._retention_failed_at is None


def test_a_refused_catalog_drop_keeps_the_retention_cache(objects, httpx_mock):
    _, catalog, _, _, _ = objects
    catalog._retention_cache = 3600.0
    httpx_mock.add_response(
        method="DELETE",
        url=httpx.URL(BASE + "/catalogs/cat", params=METADATA),
        status_code=409,
        json={"error": "catalog_not_empty", "reason": "live_namespaces"},
    )
    with pytest.raises(CatalogHasLiveNamespacesError):
        catalog.drop(**METADATA)
    assert catalog._retention_cache == 3600.0


@pytest.mark.parametrize("field", ["author", "message"])
@pytest.mark.parametrize("blank", [None, "", "\t\n ", "\u2003\u00a0", "\u0085"])
def test_catalog_drop_metadata_is_validated_before_network(
    objects, httpx_mock, field, blank
):
    _, catalog, _, _, _ = objects
    with pytest.raises(ValidationError, match=field) as error:
        catalog.drop(**{**METADATA, field: blank})
    assert error.value.status_code is None
    assert httpx_mock.get_requests() == []


@pytest.mark.parametrize("field", ["author", "message"])
def test_catalog_drop_metadata_keywords_are_required(objects, httpx_mock, field):
    _, catalog, _, _, _ = objects
    with pytest.raises(TypeError, match=field):
        catalog.drop(**{k: v for k, v in METADATA.items() if k != field})
    assert httpx_mock.get_requests() == []


_REFUSALS = {
    "live_namespaces": CatalogHasLiveNamespacesError,
    "tables_pending_retirement": CatalogTablesPendingRetirementError,
    "files_pending_expiry": CatalogFilesPendingExpiryError,
    "removals_pending": CatalogRemovalsPendingError,
    "active_compaction_claims": CatalogHasActiveCompactionClaimsError,
    "consumer_offsets": CatalogHasConsumerOffsetsError,
    "active_uploads": CatalogHasActiveUploadsError,
    "abandoned_uploads_unsettled": CatalogHasAbandonedUploadsError,
}


@pytest.mark.parametrize("reason", sorted(_REFUSALS))
def test_catalog_drop_refusal_maps_to_its_typed_error(objects, httpx_mock, reason):
    _, catalog, _, _, _ = objects
    httpx_mock.add_response(
        method="DELETE",
        url=httpx.URL(BASE + "/catalogs/cat", params=METADATA),
        status_code=409,
        json={"error": "catalog_not_empty", "detail": "why", "reason": reason},
    )
    with pytest.raises(_REFUSALS[reason]) as error:
        catalog.drop(**METADATA)
    # A base-class catch keeps working for every reason, and none of them
    # tells a retry loop to replay.
    assert isinstance(error.value, CatalogNotEmptyError)
    assert error.value.reason == reason
    assert error.value.status_code == 409
    assert error.value.detail == "why"
    assert not error.value.retryable
    assert not error.value.re_prepare


@pytest.mark.parametrize(
    "body",
    [
        {"error": "catalog_not_empty", "detail": "why", "reason": "from_the_future"},
        {"error": "catalog_not_empty", "detail": "why"},
    ],
)
def test_catalog_drop_unknown_reason_is_still_typed(objects, httpx_mock, body):
    _, catalog, _, _, _ = objects
    httpx_mock.add_response(
        method="DELETE",
        url=httpx.URL(BASE + "/catalogs/cat", params=METADATA),
        status_code=409,
        json=body,
    )
    with pytest.raises(CatalogNotEmptyError) as error:
        catalog.drop(**METADATA)
    assert type(error.value) is CatalogNotEmptyError
    assert error.value.reason == body.get("reason")


@pytest.mark.parametrize(
    ("status", "error"), [(404, NotFoundError), (422, ValidationError)]
)
def test_catalog_drop_other_statuses_map_as_documented(
    objects, httpx_mock, status, error
):
    _, catalog, _, _, _ = objects
    httpx_mock.add_response(
        method="DELETE",
        url=httpx.URL(BASE + "/catalogs/cat", params=METADATA),
        status_code=status,
        json={"error": "not_found" if status == 404 else "validation"},
    )
    with pytest.raises(error):
        catalog.drop(**METADATA)


def test_catalog_drop_reasons_match_the_spec_enum():
    # Read off the spec rather than restated: a reason added on the server
    # without a typed error here fails this, not a caller's except ladder.
    text = _SPEC.read_text()
    block = text[text.index("\n    ApiError:\n") :]
    enum = re.search(
        r"\n        reason:\n          type: string\n          enum: \[([^]]*)]", block
    )
    assert enum, "ApiError.reason has no enum in the spec"
    assert {r.strip() for r in enum.group(1).split(",")} == set(_REFUSALS)
