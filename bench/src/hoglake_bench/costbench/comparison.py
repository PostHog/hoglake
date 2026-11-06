"""Compare full runs with explicitly selected, immutable 2025 public records."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from . import COSTBENCH_REVISION, PROFILE
from .queries import QUERY_COUNT


@dataclass(frozen=True)
class Reference:
    key: str
    label: str
    path: str
    sha256: str
    tier: str
    compute_model: str | None = None

    @property
    def url(self) -> str:
        return (
            "https://raw.githubusercontent.com/ClickHouse/CostBench/"
            f"{COSTBENCH_REVISION}/{self.path}"
        )

    @property
    def source(self) -> str:
        return f"https://github.com/ClickHouse/CostBench/blob/{COSTBENCH_REVISION}/{self.path}"


REFERENCES = (
    Reference(
        "clickhouse-enterprise",
        "ClickHouse Cloud Enterprise (9 × 236 GiB)",
        "clickhouse-cloud/results_1B/aws.9.236.parallel_replicas.json",
        "75345459739a6030228de60101b77fed0f191e51d57502e8e6adbc7f19cfc135",
        "Enterprise",
    ),
    Reference(
        "snowflake-xs",
        "Snowflake Enterprise X-Small",
        "snowflake/results_1B/xs_enriched.json",
        "3c45319acb8d08fe0b2081ee82ce215b0fc88688f00bc841e44397bfec67acb8",
        "enterprise",
    ),
    Reference(
        "snowflake-large",
        "Snowflake Enterprise Large",
        "snowflake/results_1B/large_enriched.json",
        "a5f0ea6c1d6a764c40a2c3b50432e7eb1c8e4de303d82083270a69bf2f1ba81e",
        "enterprise",
    ),
    Reference(
        "snowflake-4xl",
        "Snowflake Enterprise 4X-Large",
        "snowflake/results_1B/4xl_enriched.json",
        "801cae5d89a3b2408ca3ec750438bdef2c690aaefe01ee9be42472344fee8dd9",
        "enterprise",
    ),
    Reference(
        "databricks-large",
        "Databricks Premium Large",
        "databricks/results_1B/clickbench_Large_enriched.json",
        "149cd4565e07e41fc7a33640410495585b7d11bdaeb8ef9bad9c9494592bab28",
        "premium",
    ),
    Reference(
        "bigquery-capacity",
        "BigQuery Enterprise capacity",
        "bigquery/results_1B/result_enriched.json",
        "ad75f13c424ac18de7492e891c700e43ab5c63d30ffae74481fad3abc276d89c",
        "Enterprise",
        "capacity",
    ),
    Reference(
        "bigquery-on-demand",
        "BigQuery On-Demand",
        "bigquery/results_1B/result_enriched.json",
        "ad75f13c424ac18de7492e891c700e43ab5c63d30ffae74481fad3abc276d89c",
        "OnDemand",
        "on_demand",
    ),
    Reference(
        "redshift-serverless",
        "Redshift Serverless Standard (128 RPU)",
        "redshift-serverless/results_1B/enriched_1b.json",
        "1cad602223742306664828045c557e92d97157c7fea90f8e310f9095d8d59d60",
        "Standard",
        "capacity",
    ),
)


def load_reference(reference: Reference, cache: Path) -> dict[str, Any]:
    """Fetch a separate public input, verifying downloaded and cached bytes alike."""
    path = cache / (hashlib.sha256(reference.path.encode()).hexdigest() + ".json")
    payload = (
        path.read_bytes()
        if path.exists()
        else httpx.get(reference.url, timeout=60).raise_for_status().content
    )
    if hashlib.sha256(payload).hexdigest() != reference.sha256:
        raise ValueError("reference checksum differs from the pinned public record")
    record = json.loads(payload)
    if not isinstance(record, dict):
        raise TypeError("reference must be a JSON object")
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        temporary.write_bytes(payload)
        temporary.replace(path)
    return record


def _valid_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(value)
        and value >= 0
    )


def _minima(matrix: Any) -> tuple[list[float | None], bool]:
    if not isinstance(matrix, list) or len(matrix) > QUERY_COUNT:
        raise ValueError("reference matrix must contain at most 43 query entries")
    complete = len(matrix) == QUERY_COUNT
    minima = []
    for query in range(QUERY_COUNT):
        attempts = matrix[query] if query < len(matrix) else []
        if not isinstance(attempts, list) or len(attempts) > 3:
            raise ValueError("reference query must have at most three attempts")
        if any(value is not None and not _valid_number(value) for value in attempts):
            raise ValueError("reference contains an invalid attempt value")
        complete = complete and len(attempts) == 3
        successful = [float(value) for value in attempts if value is not None]
        minima.append(min(successful) if successful else None)
    return minima, complete


def _all_attempts(matrix: list[list[float | None]]) -> float | None:
    if len(matrix) != QUERY_COUNT or any(
        len(row) != 3 or any(value is None for value in row) for row in matrix
    ):
        return None
    return sum(sum(row) for row in matrix)


def reference_row(reference: Reference, record: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(record.get("costs"), list) or not all(
        isinstance(variant, dict) for variant in record["costs"]
    ):
        raise ValueError("reference billing variants must be a list of objects")
    variants = [
        variant
        for variant in record.get("costs", [])
        if variant.get("tier") == reference.tier
        and variant.get("compute_model") == reference.compute_model
    ]
    if len(variants) != 1:
        raise ValueError("reference billing variant is missing or ambiguous")
    timings, timing_shape = _minima(record.get("result"))
    costs, cost_shape = _minima(variants[0].get("compute_costs"))
    missing_times = [query for query, value in enumerate(timings, 1) if value is None]
    missing_costs = [query for query, value in enumerate(costs, 1) if value is None]
    covered = sum(t is not None and c is not None for t, c in zip(timings, costs))
    complete = timing_shape and cost_shape and covered == QUERY_COUNT
    total = sum(value for value in timings if value is not None) if complete else None
    cost = sum(value for value in costs if value is not None) if complete else None
    return {
        "key": reference.key,
        "label": reference.label,
        "origin": "published-reference",
        "cost_basis": "historical_customer_compute_price",
        "date": record.get("date"),
        "source": reference.source,
        "source_sha256": reference.sha256,
        "tier": reference.tier,
        "compute_model": reference.compute_model,
        "complete": complete,
        "successful_queries": covered,
        "missing_runtime_queries": missing_times,
        "missing_cost_queries": missing_costs,
        "runtime_s": total,
        "compute_usd": cost,
        "all_attempts_runtime_s": _all_attempts(record["result"]),
        "all_attempts_compute_usd": _all_attempts(variants[0]["compute_costs"]),
        "runtime_times_cost": total * cost
        if total is not None and cost is not None
        else None,
        "status": "complete" if complete else "incomplete",
    }


def report_row(report: dict[str, Any], index: int = 1) -> dict[str, Any]:
    """A partial workload never receives a full-workload rank."""
    if report.get("profile") != PROFILE:
        raise ValueError("our report does not use the pinned CostBench profile")
    timings, shape = _minima(
        [
            query.get(f"{report.get('timing')}_times_s", [])
            for query in report.get("queries", [])
        ]
    )
    queries = report.get("queries", [])
    ids = [query.get("query") for query in queries]
    complete = (
        report.get("complete") is True
        and report.get("expected_queries") == QUERY_COUNT
        and report.get("successful_queries") == QUERY_COUNT
        and report.get("attempts_recorded") == 3 * QUERY_COUNT
        and report.get("failed_attempts") == 0
        and not report.get("missing_queries")
        and shape
        and ids == list(range(1, QUERY_COUNT + 1))
        and all(value is not None for value in timings)
        and report.get("metadata", {}).get("dataset_rows") == 1_000_000_000
    )
    total = report.get("total_best_s")
    cost = report.get("query_compute_usd")
    if complete:
        if (
            not _valid_number(total)
            or not _valid_number(cost)
            or not math.isclose(total, sum(timings), rel_tol=1e-9, abs_tol=1e-9)
        ):
            raise ValueError("our report totals disagree with full per-query timings")
    else:
        total = cost = None
    workers = report.get("metadata", {}).get("worker_count")
    all_runtime = _all_attempts(
        [query.get(f"{report.get('timing')}_times_s", []) for query in queries]
    )
    all_cost = (
        all_runtime * cost / total
        if complete and total and all_runtime is not None
        else None
    )
    label = "Trino + Hoglake" + (
        f" ({workers} workers)" if isinstance(workers, int) else f" (run {index})"
    )
    return {
        "key": f"ours-{index}",
        "label": label,
        "origin": "our-run",
        "cost_basis": "self_hosted_infrastructure_compute_estimate",
        "timing": report.get("timing"),
        "complete": complete,
        "successful_queries": sum(value is not None for value in timings),
        "failed_attempts": report.get("failed_attempts"),
        "runtime_s": total,
        "compute_usd": cost,
        "all_attempts_runtime_s": all_runtime if complete else None,
        "all_attempts_compute_usd": all_cost,
        "runtime_times_cost": total * cost
        if total is not None and cost is not None
        else None,
        "status": "complete" if complete else "incomplete",
    }


def build_comparison(
    reports: list[dict[str, Any]], references: list[dict[str, Any]]
) -> dict[str, Any]:
    if not reports:
        raise ValueError("at least one of our reports is required")
    rows = [report_row(report, i) for i, report in enumerate(reports, 1)] + references
    eligible = [
        row
        for row in rows
        if row["complete"]
        and _valid_number(row.get("runtime_times_cost"))
        and row["runtime_times_cost"] > 0
    ]
    best = min((row["runtime_times_cost"] for row in eligible), default=None)
    for row in rows:
        row["relative_score"] = (
            row["runtime_times_cost"] / best
            if best is not None and row in eligible
            else None
        )
    return {
        "profile": PROFILE,
        "costbench_revision": COSTBENCH_REVISION,
        "dataset_rows": 1_000_000_000,
        "query_count": QUERY_COUNT,
        "complete": all(row["complete"] for row in rows),
        "score": "T=sum(min(runtime attempts)); C=sum(min(compute cost attempts)); score=T*C; relative score normalized to the best eligible row",
        "all_attempts_basis": "separate totals over all 129 executions; these are not the 43-query best-of-three headline",
        "caveats": [
            "Published records use historical 2025 customer compute prices; our rows use stated self-hosted infrastructure compute estimates.",
            "These are dated published configurations, not a contemporaneous controlled comparison or equal physical hardware.",
            "Runtime and compute cost minima are selected independently, as in the pinned upstream analysis.",
            "All 43 queries are required for ranking. Unlike upstream's common-query exclusion mask, incomplete entries are retained without a rank.",
            "Loading, idle time, storage, requests, minimum billing periods and taxes are outside the query-compute comparison.",
        ],
        "rows": rows,
    }


def compare_files(paths: list[Path], cache: Path) -> dict[str, Any]:
    reports = [json.loads(path.read_text()) for path in paths]
    references = []
    for reference in REFERENCES:
        try:
            references.append(
                reference_row(reference, load_reference(reference, cache))
            )
        except (httpx.HTTPError, TypeError, ValueError, OSError):
            # Retain an explicit unavailable row instead of silently shrinking
            # the comparator set. The fixed public source identifies the input.
            references.append(
                {
                    "key": reference.key,
                    "label": reference.label,
                    "origin": "published-reference",
                    "cost_basis": "historical_customer_compute_price",
                    "source": reference.source,
                    "source_sha256": reference.sha256,
                    "complete": False,
                    "successful_queries": 0,
                    "runtime_s": None,
                    "compute_usd": None,
                    "runtime_times_cost": None,
                    "status": "reference_unavailable_or_invalid",
                }
            )
    return build_comparison(reports, references)


def markdown_table(comparison: dict[str, Any]) -> str:
    def number(value: Any, digits: int) -> str:
        return f"{value:.{digits}f}" if value is not None else "—"

    lines = [
        "| Configuration | Runtime (s) | Query compute (USD) | Runtime × cost | Relative | Coverage | Cost basis |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for row in comparison["rows"]:
        lines.append(
            f"| {row['label']} | {number(row.get('runtime_s'), 3)} | {number(row.get('compute_usd'), 4)} | {number(row.get('runtime_times_cost'), 4)} | {number(row.get('relative_score'), 2)} | {row['successful_queries']}/43 | {row['cost_basis']} |"
        )
    return (
        "\n".join(lines)
        + "\n\nAll 129 executions (43 queries × 3), at the same stated compute prices:\n\n"
        + "| Configuration | All-execution runtime (s) | All-execution compute (USD) |\n"
        + "|---|---:|---:|\n"
        + "\n".join(
            f"| {row['label']} | {number(row.get('all_attempts_runtime_s'), 3)} | {number(row.get('all_attempts_compute_usd'), 4)} |"
            for row in comparison["rows"]
        )
        + "\n\n"
        + "\n".join(f"- {caveat}" for caveat in comparison["caveats"])
        + "\n"
    )
