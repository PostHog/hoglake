from __future__ import annotations

import hashlib
import re
import ssl
from contextlib import nullcontext
from pathlib import Path

import certifi
import httpx
import pytest

from hoglake_bench.costbench import PROFILE
from hoglake_bench.costbench import __main__ as cli_module
from hoglake_bench.costbench import client as client_module
from hoglake_bench.costbench import queries as query_module
from hoglake_bench.costbench.client import (
    QueryError,
    QueryResult,
    TrinoClient,
    table_name,
)
from hoglake_bench.costbench.runner import (
    BenchmarkAborted,
    read_records,
    run_queries,
    summarize,
)


def test_cli_run_returns_failure_when_one_attempt_fails(tmp_path, monkeypatch):
    class Client(FakeTrino):
        def __init__(self, *args, **kwargs):
            super().__init__()

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql):
            if not self.calls:
                self.calls.append(sql)
                raise QueryError("synthetic error", name="TEST_ERROR")
            return super().execute(sql)

    monkeypatch.setattr(cli_module, "TrinoClient", Client)
    monkeypatch.setattr(cli_module, "load_queries", lambda *args: ["SELECT 1"] * 43)
    path = tmp_path / "run.jsonl"
    status = cli_module.main(
        [
            "run",
            "--trino-url",
            "http://example.test",
            "--hourly-usd",
            "1",
            "--queries",
            "1",
            "--output",
            str(path),
        ]
    )
    assert status == 2
    assert [record["status"] for record in read_records(path)[1:]] == [
        "error",
        "ok",
        "ok",
    ]


def test_client_consumes_all_pages_and_preserves_both_clocks():
    seen = []

    def handle(request):
        seen.append(request)
        if request.method == "POST":
            return httpx.Response(
                200,
                json={
                    "id": "query-1",
                    "nextUri": "http://coordinator.invalid:8080/v1/statement/queued/query-1/1",
                    "stats": {"state": "QUEUED"},
                },
            )
        if request.url.path.endswith("/1"):
            return httpx.Response(
                200,
                json={
                    "id": "query-1",
                    "data": [[1], [2]],
                    "columns": [{"name": "n", "type": "bigint"}],
                    "nextUri": "http://coordinator.invalid:8080/v1/statement/executing/query-1/2",
                    "stats": {"state": "RUNNING"},
                },
            )
        return httpx.Response(
            200,
            json={
                "id": "query-1",
                "data": [[3]],
                "stats": {
                    "state": "FINISHED",
                    "elapsedTimeMillis": 1200,
                    "cpuTimeMillis": 500,
                    "rootStage": {"host": "private-host"},
                },
            },
        )

    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        result = TrinoClient("http://localhost:18080", http_client=http).execute(
            "SELECT n", collect_rows=True
        )
    assert result.rows == [[1], [2], [3]]
    assert result.row_count == 3
    assert result.columns == [{"name": "n", "type": "bigint"}]
    assert result.query_id == "query-1"
    assert result.server_elapsed_ms == 1200
    assert result.elapsed_s >= 0
    assert result.stats == {
        "state": "FINISHED",
        "elapsedTimeMillis": 1200,
        "cpuTimeMillis": 500,
    }
    assert len(result.result_sha256) == 64
    assert [r.method for r in seen] == ["POST", "GET", "GET"]
    assert all(r.url.host == "localhost" for r in seen)
    assert seen[0].headers["X-Trino-Time-Zone"] == "UTC"
    assert seen[0].headers["X-Trino-Session"] == "query_max_run_time=1800s"


def test_client_can_drain_without_retaining_result_rows():
    with httpx.Client(
        transport=httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                json={
                    "id": "q",
                    "data": [[1], [2]],
                    "stats": {"state": "FINISHED", "wallTimeMillis": 12},
                },
            )
        )
    ) as http:
        result = TrinoClient("http://localhost:8080", http_client=http).execute(
            "SELECT n"
        )
    assert result.rows == []
    assert result.row_count == 2
    # Scheduled task wall time can exceed elapsed latency on parallel workers.
    assert result.server_elapsed_ms is None


def test_timeout_cancels_the_active_query_without_retry(monkeypatch):
    seen = []

    def handle(request):
        seen.append(request.method)
        if request.method == "DELETE":
            return httpx.Response(204)
        return httpx.Response(
            200,
            json={
                "id": "q",
                "nextUri": "http://localhost:8080/v1/statement/executing/q/1",
                "stats": {"state": "RUNNING"},
            },
        )

    ticks = iter([0, 0, 2])
    monkeypatch.setattr(client_module.time, "monotonic", lambda: next(ticks))
    with (
        httpx.Client(transport=httpx.MockTransport(handle)) as http,
        pytest.raises(QueryError) as raised,
    ):
        TrinoClient("http://localhost:8080", timeout_s=1, http_client=http).execute(
            "SELECT n"
        )
    assert raised.value.safe_error["name"] == "CLIENT_TIMEOUT"
    assert raised.value.query_id == "q"
    assert raised.value.safe_to_continue
    assert seen == ["POST", "DELETE"]


def test_server_failure_retains_codes_but_not_private_message_in_artifacts(tmp_path):
    def handle(_):
        return httpx.Response(
            200,
            json={
                "id": "failed-query",
                "stats": {"state": "FAILED"},
                "error": {
                    "message": "cannot read s3://private-bucket/x using private-host",
                    "errorName": "PERMISSION_DENIED",
                    "errorType": "USER_ERROR",
                    "errorCode": 4,
                },
            },
        )

    path = tmp_path / "run.jsonl"
    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        run_queries(
            TrinoClient("http://localhost:8080", http_client=http),
            ["SELECT 1"] * 43,
            path,
            selected_queries=[1],
            hourly_usd=1,
            metadata={
                "worker_count": 1,
                "trino_url": "http://private-host",
                "aws_access_key_id": "secret",
            },
            progress=None,
        )
    text = path.read_text()
    assert "private-bucket" not in text
    assert "private-host" not in text
    assert "secret" not in text
    records = read_records(path)
    assert records[1]["error"] == {
        "name": "PERMISSION_DENIED",
        "type": "USER_ERROR",
        "code": 4,
    }
    report = summarize(records)
    assert report["failed_attempts"] == 3
    assert not report["complete"]
    assert report["total_best_s"] is None
    assert report["query_compute_usd"] is None
    assert report["missing_queries"] == list(range(1, 44))


class FakeTrino:
    def __init__(self):
        self.calls = []

    def execute(self, sql):
        self.calls.append(sql)
        attempt = (len(self.calls) - 1) % 3
        return QueryResult(
            "query", [3, 1, 2][attempt], [2500, 1500, 3500][attempt], {}, 1
        )


def full_run(path: Path):
    client = FakeTrino()
    run_queries(
        client, [f"SELECT {q}" for q in range(43)], path, hourly_usd=3.6, progress=None
    )
    return client, read_records(path)


def test_legacy_minimum_includes_first_attempt_and_keeps_sequential_order(tmp_path):
    client, records = full_run(tmp_path / "run.jsonl")
    assert len(client.calls) == 129
    assert client.calls[:6] == ["SELECT 0"] * 3 + ["SELECT 1"] * 3
    # Make the first attempt the fastest: legacy CostBench must include it.
    records[1]["client_elapsed_s"] = 0.25
    report = summarize(records)
    assert report["profile"] == PROFILE
    assert report["complete"]
    assert report["total_best_s"] == 42.25
    assert report["query_compute_usd"] == pytest.approx(0.04225)
    assert report["runtime_times_cost"] == pytest.approx(42.25 * 0.04225)
    assert report["queries"][0]["client_times_s"] == [0.25, 1, 2]
    assert summarize(records, timing="server")["total_best_s"] == 43 * 1.5


def test_incomplete_query_coverage_is_not_presented_as_a_full_score(tmp_path):
    _, records = full_run(tmp_path / "run.jsonl")
    for record in records:
        if record.get("query") == 5:
            record["status"] = "error"
    report = summarize(records)
    assert report["successful_queries"] == 42
    assert report["missing_queries"] == [5]
    assert report["partial_best_sum_s"] == 42
    assert report["total_best_s"] is None
    assert report["runtime_times_cost"] is None


def test_all_129_execution_cost_is_separate_from_the_43_query_headline(tmp_path):
    _, records = full_run(tmp_path / "run.jsonl")
    report = summarize(records)
    assert report["total_best_s"] == 43
    assert report["query_compute_usd"] == pytest.approx(0.043)
    assert report["all_attempts_runtime_s"] == 43 * 6
    assert report["all_attempts_compute_usd"] == pytest.approx(0.258)
    assert report["mean_43_query_compute_usd"] == pytest.approx(0.086)
    assert summarize(records, timing="server")["all_attempts_runtime_s"] == 43 * 7.5
    assert summarize(records[:-1])["all_attempts_compute_usd"] is None


def test_one_failed_attempt_prevents_a_complete_score_but_keeps_recorded_query_cost(
    tmp_path,
):
    _, records = full_run(tmp_path / "run.jsonl")
    records[1]["status"] = "error"
    report = summarize(records)
    assert not report["complete"]
    assert report["failed_attempts"] == 1
    assert report["successful_queries"] == 43
    assert report["query_compute_usd"] is None
    assert report["all_attempts_compute_usd"] == pytest.approx(0.258)


def test_interrupted_run_does_not_get_full_score_even_if_all_queries_have_a_success(
    tmp_path,
):
    _, records = full_run(tmp_path / "run.jsonl")
    report = summarize(records[:-1])
    assert report["successful_queries"] == 43
    assert report["attempts_recorded"] == 128
    assert not report["complete"]
    assert report["total_best_s"] is None


def test_mixed_or_duplicate_attempts_are_rejected(tmp_path):
    _, records = full_run(tmp_path / "run.jsonl")
    with pytest.raises(ValueError, match="duplicate"):
        summarize(records + [records[1]])
    records[1]["run_id"] = "another-run"
    with pytest.raises(ValueError, match="different run"):
        summarize(records)


def test_existing_result_file_is_not_overwritten(tmp_path):
    path = tmp_path / "run.jsonl"
    path.write_text("preserve me")
    with pytest.raises(FileExistsError):
        full_run(path)
    assert path.read_text() == "preserve me"


def test_queries_require_exact_pinned_bytes_and_quote_target_identifiers(
    tmp_path, monkeypatch
):
    payload = ("SELECT COUNT(*) FROM hits;\n" * 43).encode()
    monkeypatch.setattr(
        query_module, "QUERY_SHA256", hashlib.sha256(payload).hexdigest()
    )
    cache = tmp_path / "queries.sql"
    cache.write_bytes(payload)
    queries = query_module.load_queries(cache, "hoglake", "costbench", 'a"b')
    assert len(queries) == 43
    assert queries[0] == 'SELECT COUNT(*) FROM "hoglake"."costbench"."a""b"'
    cache.write_bytes(payload + b"SELECT bad")
    with pytest.raises(ValueError, match="checksum"):
        query_module.load_queries(cache, "hoglake", "costbench", "hits")


def test_q29_retains_trino_regex_escaping_without_changing_the_upstream_input(
    tmp_path, monkeypatch
):
    lines = ["SELECT COUNT(*) FROM hits;"] * 43
    lines[28] = (
        r"SELECT regexp_replace(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '$1') FROM hits;"
    )
    payload = ("\n".join(lines) + "\n").encode()
    monkeypatch.setattr(
        query_module, "QUERY_SHA256", hashlib.sha256(payload).hexdigest()
    )
    cache = tmp_path / "queries.sql"
    cache.write_bytes(payload)
    queries = query_module.load_queries(cache, "hoglake", "costbench", "hits")
    assert r"www\." in queries[28]
    assert r"www\\." not in queries[28]
    pattern = re.search(r"regexp_replace\(Referer, '([^']+)'", queries[28]).group(1)
    assert re.match(pattern, "https://www.example.com/x").group(1) == "example.com"
    assert re.match(pattern, "https://wwwXexample.com/x").group(1) == "wwwXexample.com"
    assert cache.read_bytes() == payload


def test_identifier_and_endpoint_validation():
    assert table_name("cat", "schema", 'a"b') == '"cat"."schema"."a""b"'
    with pytest.raises(ValueError):
        table_name("cat", "", "table")
    with pytest.raises(ValueError, match="credentials"):
        TrinoClient("https://user:secret@example.invalid")


@pytest.mark.parametrize(
    "url,timeout",
    [
        ("ftp://example.test", 1),
        ("http:///missing-host", 1),
        ("http://example.test", 0),
        ("http://example.test", float("inf")),
    ],
)
def test_client_refuses_invalid_endpoint_or_deadline(url, timeout):
    with pytest.raises(ValueError, match="Trino URL|timeout must"):
        TrinoClient(url, timeout_s=timeout)


def test_supplied_http_client_owns_its_ca_configuration(tmp_path):
    with (
        httpx.Client() as http,
        pytest.raises(ValueError, match="CA on a supplied HTTP client"),
    ):
        TrinoClient(
            "https://example.test", http_client=http, ca_file=tmp_path / "ca.pem"
        )


@pytest.mark.parametrize(
    "problem",
    [
        "unsafe-path",
        "changed-id",
        "nonterminal-error",
        "invalid-error",
        "empty-next-uri",
    ],
)
def test_malformed_page_is_cancelled_without_following_an_unsafe_path(problem):
    seen = []

    def handle(request):
        seen.append((request.method, request.url.path))
        if request.method == "DELETE":
            return httpx.Response(204)
        if problem == "changed-id" and request.method == "GET":
            return httpx.Response(
                200, json={"id": "other-query", "stats": {"state": "FINISHED"}}
            )
        assert request.method == "POST", "followed an invalid result URI"
        page = {
            "id": "query",
            "stats": {"state": "RUNNING"},
            "nextUri": "http://example.test/v1/statement/query/1",
        }
        if problem == "unsafe-path":
            page["nextUri"] = "http://example.test/unrelated-path"
        elif problem == "nonterminal-error":
            page["error"] = {"message": "synthetic error"}
        elif problem == "invalid-error":
            page["stats"]["state"] = "FAILED"
            page["error"] = "invalid error object"
        elif problem == "empty-next-uri":
            page["nextUri"] = ""
        return httpx.Response(200, json=page)

    with (
        httpx.Client(transport=httpx.MockTransport(handle)) as http,
        pytest.raises(QueryError) as raised,
    ):
        TrinoClient("http://localhost:8080", http_client=http).execute("SELECT 1")
    assert raised.value.safe_error["name"] == "PROTOCOL_ERROR"
    if problem == "unsafe-path":
        assert not raised.value.safe_to_continue
        assert seen == [("POST", "/v1/statement")]
    else:
        assert raised.value.safe_to_continue
        assert seen[-1][0] == "DELETE"


@pytest.mark.parametrize(
    "queries,price,selected",
    [
        (["SELECT 1"] * 42, 1, [1]),
        (["SELECT 1"] * 43, -1, [1]),
        (["SELECT 1"] * 43, float("nan"), [1]),
        (["SELECT 1"] * 43, 1, []),
        (["SELECT 1"] * 43, 1, [1, 1]),
        (["SELECT 1"] * 43, 1, [0]),
        (["SELECT 1"] * 43, 1, [44]),
    ],
)
def test_invalid_workload_or_price_is_refused_before_creating_output(
    tmp_path, queries, price, selected
):
    client = FakeTrino()
    path = tmp_path / "run.jsonl"
    with pytest.raises(ValueError):
        run_queries(client, queries, path, hourly_usd=price, selected_queries=selected)
    assert not path.exists() and client.calls == []


@pytest.mark.parametrize(
    "problem",
    [
        "timing",
        "no-manifest",
        "two-manifests",
        "profile",
        "query-count",
        "price",
        "query-id",
        "attempt-id",
        "client-time",
        "server-time",
    ],
)
def test_report_refuses_malformed_or_mixed_workload_data(tmp_path, problem):
    _, records = full_run(tmp_path / "run.jsonl")
    options = {}
    if problem == "timing":
        options["timing"] = "cpu"
    elif problem == "no-manifest":
        records = records[1:]
    elif problem == "two-manifests":
        records.append(records[0])
    elif problem == "profile":
        records[0]["profile"] = "unrelated-workload"
    elif problem == "query-count":
        records[0]["expected_queries"] = 42
    elif problem == "price":
        records[0]["hourly_compute_usd"] = -1
    elif problem == "query-id":
        records[1]["query"] = 0
    elif problem == "attempt-id":
        records[1]["attempt"] = 0
    elif problem == "client-time":
        records[1]["client_elapsed_s"] = -1
    else:
        records[1]["server_elapsed_s"] = float("nan")
    with pytest.raises(ValueError):
        summarize(records, **options)


def test_checksummed_sql_with_wrong_query_count_is_refused(tmp_path, monkeypatch):
    payload = b"SELECT count(*) FROM hits;\n"
    path = tmp_path / "queries.sql"
    path.write_bytes(payload)
    monkeypatch.setattr(
        query_module, "QUERY_SHA256", hashlib.sha256(payload).hexdigest()
    )
    with pytest.raises(ValueError, match="43 upstream queries"):
        query_module.load_queries(path, "benchmark", "costbench", "hits")


def test_report_drops_private_metadata_even_when_given_an_external_manifest(tmp_path):
    _, records = full_run(tmp_path / "run.jsonl")
    records[0]["metadata"] = {"worker_count": 4, "trino_url": "http://private-host"}
    assert summarize(records)["metadata"] == {"worker_count": 4}


def test_transport_failure_is_one_failed_attempt_without_an_implicit_retry():
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(503, text="unavailable")

    with (
        httpx.Client(transport=httpx.MockTransport(handle)) as http,
        pytest.raises(QueryError) as raised,
    ):
        TrinoClient("http://localhost:8080", http_client=http).execute("SELECT 1")
    assert raised.value.safe_error["name"] == "HTTP_ERROR"
    assert not raised.value.safe_to_continue
    assert len(calls) == 1


def test_unconfirmed_cancellation_journals_failure_and_stops_before_the_next_attempt(
    tmp_path,
):
    seen = []

    def handle(request):
        seen.append(request.method)
        if request.method == "POST":
            return httpx.Response(
                200,
                json={
                    "id": "q",
                    "stats": {"state": "RUNNING"},
                    "nextUri": "http://localhost:8080/v1/statement/executing/q/1",
                },
            )
        if request.method == "GET":
            raise httpx.ReadTimeout("connection lost", request=request)
        return httpx.Response(500)

    path = tmp_path / "run.jsonl"
    with (
        httpx.Client(transport=httpx.MockTransport(handle)) as http,
        pytest.raises(BenchmarkAborted, match="unconfirmed"),
    ):
        run_queries(
            TrinoClient("http://localhost:8080", http_client=http),
            ["SELECT 1"] * 43,
            path,
            selected_queries=[1],
            hourly_usd=1,
            progress=None,
        )
    assert seen == ["POST", "GET", "DELETE"]
    records = read_records(path)
    assert len(records) == 2
    assert records[1]["status"] == "error"
    assert not records[1]["cancellation_confirmed"]
    assert not summarize(records)["complete"]


@pytest.mark.parametrize(
    "page", [{}, {"id": "q"}, {"id": "q", "stats": {"state": "RUNNING"}}]
)
def test_malformed_or_unfinished_protocol_response_is_not_a_success(page):
    def handle(request):
        return (
            httpx.Response(204)
            if request.method == "DELETE"
            else httpx.Response(200, json=page)
        )

    with (
        httpx.Client(transport=httpx.MockTransport(handle)) as http,
        pytest.raises(QueryError) as raised,
    ):
        TrinoClient("http://localhost:8080", http_client=http).execute("SELECT 1")
    assert raised.value.safe_error["name"] == "PROTOCOL_ERROR"


def test_unknown_post_outcome_stops_without_sending_a_second_query(tmp_path):
    calls = []

    def handle(request):
        calls.append(request.method)
        raise httpx.ReadTimeout("response lost", request=request)

    with (
        httpx.Client(transport=httpx.MockTransport(handle)) as http,
        pytest.raises(BenchmarkAborted),
    ):
        run_queries(
            TrinoClient("http://localhost:8080", http_client=http),
            ["SELECT 1"] * 43,
            tmp_path / "run.jsonl",
            selected_queries=[1],
            hourly_usd=1,
            progress=None,
        )
    assert calls == ["POST"]


def test_run_cli_returns_failure_when_a_selected_query_has_no_success(
    tmp_path, monkeypatch
):
    class FailedClient:
        def execute(self, sql):
            raise QueryError("failed", name="TEST_FAILURE")

    monkeypatch.setattr(cli_module, "load_queries", lambda *args: ["SELECT 1"] * 43)
    monkeypatch.setattr(
        cli_module, "TrinoClient", lambda *args, **kwargs: nullcontext(FailedClient())
    )
    path = tmp_path / "run.jsonl"
    assert (
        cli_module.main(
            [
                "run",
                "--trino-url",
                "http://localhost:8080",
                "--hourly-usd",
                "1",
                "--output",
                str(path),
                "--queries",
                "1",
            ]
        )
        == 2
    )
    assert len(read_records(path)) == 4


def test_https_basic_auth_is_sent_on_each_page_and_not_written_to_results(tmp_path):
    authorizations = []

    def handle(request):
        authorizations.append(request.headers.get("Authorization"))
        if request.method == "POST":
            return httpx.Response(
                200,
                json={
                    "id": "q",
                    "stats": {"state": "RUNNING"},
                    "nextUri": "https://advertised.invalid:8443/v1/statement/executing/q/1",
                },
            )
        return httpx.Response(
            200,
            json={
                "id": "q",
                "data": [[1]],
                "stats": {"state": "FINISHED", "elapsedTimeMillis": 1},
            },
        )

    path = tmp_path / "run.jsonl"
    with httpx.Client(transport=httpx.MockTransport(handle)) as http:
        client = TrinoClient(
            "https://localhost:8443",
            user="benchmark",
            password="synthetic-password",
            http_client=http,
        )
        run_queries(
            client,
            ["SELECT 1"] * 43,
            path,
            selected_queries=[1],
            hourly_usd=1,
            progress=None,
        )
    assert len(authorizations) == 6
    expected = httpx.BasicAuth("benchmark", "synthetic-password").auth_flow(
        httpx.Request("GET", "https://localhost")
    )
    authorization = next(expected).headers["Authorization"]
    assert all(value == authorization for value in authorizations)
    assert "synthetic-password" not in path.read_text()
    assert authorization not in path.read_text()


def test_password_authentication_refuses_plain_http():
    with pytest.raises(ValueError, match="requires HTTPS"):
        TrinoClient("http://localhost:8080", password="synthetic-password")


def test_custom_ca_configures_verified_tls_context(monkeypatch):
    factory = httpx.Client
    configured = {}

    def make_client(**kwargs):
        configured.update(kwargs)
        return factory(
            transport=httpx.MockTransport(
                lambda _: httpx.Response(
                    200, json={"id": "q", "stats": {"state": "FINISHED"}}
                )
            ),
            **kwargs,
        )

    monkeypatch.setattr(client_module.httpx, "Client", make_client)
    with TrinoClient("https://localhost:8443", ca_file=Path(certifi.where())) as client:
        client.execute("SELECT 1")
    context = configured["verify"]
    assert isinstance(context, ssl.SSLContext)
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname
    assert context.cert_store_stats()["x509_ca"] > 0


def test_missing_named_password_environment_fails_before_fetch_or_connect(
    tmp_path, monkeypatch
):
    monkeypatch.delenv("COSTBENCH_TEST_PASSWORD", raising=False)

    def forbidden(*args, **kwargs):
        pytest.fail("missing credentials must fail before network access")

    monkeypatch.setattr(cli_module, "load_queries", forbidden)
    monkeypatch.setattr(cli_module, "TrinoClient", forbidden)
    with pytest.raises(SystemExit) as raised:
        cli_module.main(
            [
                "run",
                "--trino-url",
                "https://localhost:8443",
                "--password-env",
                "COSTBENCH_TEST_PASSWORD",
                "--hourly-usd",
                "1",
                "--output",
                str(tmp_path / "run.jsonl"),
            ]
        )
    assert raised.value.code == 2
    assert not (tmp_path / "run.jsonl").exists()
