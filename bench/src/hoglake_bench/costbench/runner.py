"""Three consecutive attempts per query; preserve failures and both clocks."""

from __future__ import annotations

import hashlib
import json
import math
import uuid
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TextIO

from . import CLICKBENCH_REVISION, COSTBENCH_REVISION, PROFILE
from .client import QueryError, TrinoClient
from .queries import (
    QUERY_COUNT,
    QUERY_LICENSE,
    QUERY_SHA256,
    QUERY_URL,
)

ATTEMPTS = 3


class BenchmarkAborted(RuntimeError):
    """A previous query may still be active; another timed query is unsafe."""


def _write(stream: TextIO, record: dict[str, Any]) -> None:
    stream.write(json.dumps(record, separators=(",", ":"), allow_nan=False) + "\n")
    stream.flush()


def public_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    """Allow explicit reproducibility fields, never runtime endpoints or keys."""
    allowed = {
        "trino_version",
        "trino_image",
        "hoglake_version",
        "hoglake_image",
        "runner_commit",
        "runner_image",
        "worker_count",
        "worker_instance_type",
        "coordinator_instance_type",
        "control_instance_type",
        "region",
        "architecture",
        "layout",
        "compression",
        "target_file_bytes",
        "dataset_rows",
        "dataset_sha256",
        "loader_manifest_sha256",
        "runtime_bundle_sha256",
        "worker_cpu_limit",
        "worker_memory_limit",
        "coordinator_cpu_limit",
        "coordinator_memory_limit",
        "filesystem_cache",
        "hardware_profile",
        "worker_heap",
        "worker_query_memory_per_node",
        "coordinator_heap",
        "coordinator_query_memory_per_node",
        "cluster_query_memory",
        "filesystem_cache_capacity",
        "filesystem_cache_page_size",
        "cache_storage",
        "availability_zone",
        "initial_cache_state",
    }
    return {
        key: value
        for key, value in metadata.items()
        if key in allowed and isinstance(value, (str, int, float, bool))
    }


def run_queries(
    client: TrinoClient,
    queries: list[str],
    output: Path,
    *,
    selected_queries: list[int] | None = None,
    hourly_usd: float,
    metadata: dict[str, Any] | None = None,
    progress: Callable[[str], None] | None = print,
) -> None:
    if len(queries) != QUERY_COUNT:
        raise ValueError(f"the benchmark requires exactly {QUERY_COUNT} queries")
    if hourly_usd < 0 or not math.isfinite(hourly_usd):
        raise ValueError("hourly compute price must be finite and nonnegative")
    selected = (
        selected_queries
        if selected_queries is not None
        else list(range(1, QUERY_COUNT + 1))
    )
    if (
        not selected
        or len(set(selected)) != len(selected)
        or any(q not in range(1, QUERY_COUNT + 1) for q in selected)
    ):
        raise ValueError("selected query numbers must be unique and between 1 and 43")
    run_id = uuid.uuid4().hex
    output.parent.mkdir(parents=True, exist_ok=True)
    # Never append a second run to an existing artifact: that could mix clusters.
    with output.open("x", encoding="utf-8") as stream:
        _write(
            stream,
            {
                "record_type": "manifest",
                "run_id": run_id,
                "profile": PROFILE,
                "started_at": datetime.now(UTC).isoformat(),
                "costbench_revision": COSTBENCH_REVISION,
                "clickbench_revision": CLICKBENCH_REVISION,
                "query_source": QUERY_URL,
                "query_source_sha256": QUERY_SHA256,
                "query_source_license": QUERY_LICENSE,
                "expected_queries": QUERY_COUNT,
                "selected_queries": selected,
                "attempts_per_query": ATTEMPTS,
                "timing": "persistent-http-client-fully-drained-results",
                "server_timing": "terminal elapsedTimeMillis only; task-scheduled wallTimeMillis is not query latency",
                "result_cache": "Trino has no query-result cache enabled by this runner",
                "data_cache": "normal data caching retained; no cache reset between attempts",
                "hourly_compute_usd": hourly_usd,
                "metadata": public_metadata(metadata or {}),
            },
        )
        for query_number in selected:
            query = queries[query_number - 1]
            for attempt in range(1, ATTEMPTS + 1):
                abort = False
                record: dict[str, Any] = {
                    "record_type": "attempt",
                    "run_id": run_id,
                    "query": query_number,
                    "attempt": attempt,
                    "sql_sha256": hashlib.sha256(query.encode()).hexdigest(),
                }
                try:
                    result = client.execute(query)
                    record.update(
                        {
                            "status": "ok",
                            "query_id": result.query_id,
                            "client_elapsed_s": result.elapsed_s,
                            "server_elapsed_s": result.server_elapsed_ms / 1000
                            if result.server_elapsed_ms is not None
                            else None,
                            "stats": result.stats,
                            "returned_rows": result.row_count,
                            "result_sha256": result.result_sha256,
                            "error": None,
                        }
                    )
                except QueryError as exc:
                    # Error messages can contain S3 paths, hosts, SQL or secrets.
                    # Keep only codes/names; details remain in the local exception.
                    record.update(
                        {
                            "status": "error",
                            "query_id": exc.query_id,
                            "client_elapsed_s": exc.elapsed_s,
                            "server_elapsed_s": None,
                            "stats": {},
                            "returned_rows": None,
                            "result_sha256": None,
                            "error": exc.safe_error,
                            "cancellation_confirmed": exc.safe_to_continue,
                        }
                    )
                    abort = not exc.safe_to_continue
                _write(stream, record)
                if progress:
                    progress(
                        f"q{query_number:02d} attempt={attempt} status={record['status']} client_s={record['client_elapsed_s']:.3f}"
                    )
                if abort:
                    raise BenchmarkAborted(
                        "query outcome or cancellation is unconfirmed; stopped before the next attempt to avoid overlapping queries"
                    )


def summarize(
    records: Iterable[dict[str, Any]],
    *,
    timing: str = "client",
    hourly_usd: float | None = None,
) -> dict[str, Any]:
    if timing not in {"client", "server"}:
        raise ValueError("timing must be client or server")
    records = list(records)
    manifests = [r for r in records if r.get("record_type") == "manifest"]
    if len(manifests) != 1:
        raise ValueError("results must contain exactly one run manifest")
    manifest = manifests[0]
    if (
        manifest.get("profile") != PROFILE
        or manifest.get("expected_queries") != QUERY_COUNT
    ):
        raise ValueError(
            "results do not describe the pinned 43-query CostBench profile"
        )
    price = manifest["hourly_compute_usd"] if hourly_usd is None else hourly_usd
    if not isinstance(price, (int, float)) or not math.isfinite(price) or price < 0:
        raise ValueError("hourly compute price must be finite and nonnegative")
    attempts: dict[tuple[int, int], dict[str, Any]] = {}
    for record in records:
        if record.get("record_type") != "attempt":
            continue
        query = record.get("query")
        attempt = record.get("attempt")
        if record.get("run_id") != manifest["run_id"]:
            raise ValueError("results contain attempts from a different run")
        if query not in range(1, QUERY_COUNT + 1) or attempt not in range(
            1, ATTEMPTS + 1
        ):
            raise ValueError("invalid query or attempt number")
        if (query, attempt) in attempts:
            raise ValueError("duplicate query attempt; refusing to mix repeated runs")
        for field in ("client_elapsed_s", "server_elapsed_s"):
            value = record.get(field)
            if value is not None and (
                not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value < 0
            ):
                raise ValueError("invalid execution time")
        attempts[query, attempt] = record
    per_query = []
    failed_attempts = sum(r["status"] != "ok" for r in attempts.values())
    for query in range(1, QUERY_COUNT + 1):
        row: dict[str, Any] = {"query": query}
        for clock in ("client", "server"):
            times = [
                r.get(f"{clock}_elapsed_s")
                if (r := attempts.get((query, attempt))) and r["status"] == "ok"
                else None
                for attempt in range(1, ATTEMPTS + 1)
            ]
            valid = [v for v in times if v is not None]
            row[f"{clock}_times_s"] = times
            row[f"{clock}_best_s"] = min(valid) if valid else None
        per_query.append(row)
    missing = [r["query"] for r in per_query if r[f"{timing}_best_s"] is None]
    complete = (
        not missing and len(attempts) == QUERY_COUNT * ATTEMPTS and failed_attempts == 0
    )
    partial_total = sum(r[f"{timing}_best_s"] or 0 for r in per_query)
    total = partial_total if complete else None
    cost = total / 3600 * price if total is not None else None
    recorded_times = [r.get(f"{timing}_elapsed_s") for r in attempts.values()]
    all_attempts_total = (
        sum(recorded_times)
        if len(attempts) == QUERY_COUNT * ATTEMPTS
        and all(value is not None for value in recorded_times)
        else None
    )
    all_attempts_cost = (
        all_attempts_total / 3600 * price if all_attempts_total is not None else None
    )
    return {
        "profile": PROFILE,
        "run_id": manifest["run_id"],
        "costbench_revision": manifest.get("costbench_revision"),
        "query_source": manifest.get("query_source"),
        "query_source_sha256": manifest.get("query_source_sha256"),
        "query_source_license": manifest.get("query_source_license"),
        "timing": timing,
        "complete": complete,
        "expected_queries": QUERY_COUNT,
        "successful_queries": QUERY_COUNT - len(missing),
        "attempts_recorded": len(attempts),
        "failed_attempts": failed_attempts,
        "missing_queries": missing,
        "total_best_s": total,
        "partial_best_sum_s": partial_total,
        "hourly_compute_usd": price,
        "query_compute_usd": cost,
        "headline_basis": "43 queries, sum of per-query minima over three consecutive attempts",
        "all_attempts_runtime_s": all_attempts_total,
        "all_attempts_compute_usd": all_attempts_cost,
        "all_attempts_basis": "all 129 recorded executions, including failed attempts when their elapsed time is known",
        "mean_43_query_compute_usd": all_attempts_cost / ATTEMPTS
        if all_attempts_cost is not None
        else None,
        "runtime_times_cost": total * cost
        if total is not None and cost is not None
        else None,
        "cost_boundary": "estimated fixed-stack query compute; excludes loading, idle, storage, requests and taxes",
        "metadata": public_metadata(manifest.get("metadata", {})),
        "queries": per_query,
    }


def read_records(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as stream:
        return [json.loads(line) for line in stream if line.strip()]
