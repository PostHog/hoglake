from __future__ import annotations

import hashlib
import json

import httpx
import pytest

from hoglake_bench.costbench import PROFILE
from hoglake_bench.costbench import comparison as comparison_module
from hoglake_bench.costbench.comparison import (
    REFERENCES,
    Reference,
    build_comparison,
    compare_files,
    load_reference,
    markdown_table,
    reference_row,
    report_row,
)


def reference():
    return Reference(
        "synthetic",
        "Synthetic warehouse",
        "synthetic/result.json",
        "0" * 64,
        "enterprise",
    )


def record():
    return {
        "date": "2025-11-01",
        "result": [[1, 9, 5] for _ in range(43)],
        "costs": [
            {
                "tier": "enterprise",
                "compute_costs": [[0.4, 0.1, 0.2] for _ in range(43)],
            }
        ],
        "comment": "private details must not appear in comparison artifacts",
    }


def report():
    return {
        "profile": PROFILE,
        "timing": "client",
        "complete": True,
        "expected_queries": 43,
        "successful_queries": 43,
        "attempts_recorded": 129,
        "missing_queries": [],
        "failed_attempts": 0,
        "total_best_s": 43,
        "query_compute_usd": 0.43,
        "metadata": {"worker_count": 1, "dataset_rows": 1_000_000_000},
        "queries": [{"query": q, "client_times_s": [1, 2, 3]} for q in range(1, 44)],
    }


def test_reference_uses_independent_minimum_runtime_and_cost_including_first_attempt():
    row = reference_row(reference(), record())
    assert row["complete"]
    assert row["successful_queries"] == 43
    assert row["runtime_s"] == 43
    # Cheapest attempt is the slowest one; choosing cost of the fastest
    # attempt would incorrectly yield 17.2 instead of 4.3.
    assert row["compute_usd"] == pytest.approx(4.3)
    assert row["all_attempts_runtime_s"] == 43 * 15
    assert row["all_attempts_compute_usd"] == pytest.approx(43 * 0.7)
    assert row["runtime_times_cost"] == pytest.approx(43 * 4.3)
    assert "private details" not in json.dumps(row)
    assert row["cost_basis"] == "historical_customer_compute_price"


def test_bigquery_billing_variant_is_selected_by_both_tier_and_model():
    entry = record()
    entry["costs"] = [
        {
            "tier": "Enterprise",
            "compute_model": "capacity",
            "compute_costs": [[0.2] * 3 for _ in range(43)],
        },
        {
            "tier": "Enterprise",
            "compute_model": "on_demand",
            "compute_costs": [[0.3] * 3 for _ in range(43)],
        },
    ]
    chosen = Reference(
        "synthetic", "Synthetic", "synthetic.json", "0" * 64, "Enterprise", "on_demand"
    )
    assert reference_row(chosen, entry)["compute_usd"] == pytest.approx(12.9)
    entry["costs"].append(entry["costs"][1])
    with pytest.raises(ValueError, match="ambiguous"):
        reference_row(chosen, entry)


def test_incomplete_reference_retains_query_coverage_without_receiving_a_rank():
    entry = record()
    entry["result"][4] = [None, None, None]
    row = reference_row(reference(), entry)
    comparison = build_comparison([report()], [row])
    assert not row["complete"]
    assert row["successful_queries"] == 42
    assert row["missing_runtime_queries"] == [5]
    assert row["missing_cost_queries"] == []
    assert row["runtime_times_cost"] is None
    assert row["relative_score"] is None
    assert not comparison["complete"]
    assert len(comparison["rows"]) == 2


def test_truncated_attempt_matrix_does_not_get_a_complete_score():
    entry = record()
    entry["result"][0] = [1]
    row = reference_row(reference(), entry)
    assert row["successful_queries"] == 43
    assert not row["complete"]
    assert row["runtime_s"] is None


def test_relative_scores_include_our_run_but_explain_the_cost_boundaries():
    comparison = build_comparison([report()], [reference_row(reference(), record())])
    ours, published = comparison["rows"]
    assert comparison["complete"]
    assert ours["relative_score"] == 1
    assert published["relative_score"] == pytest.approx(10)
    assert ours["cost_basis"] == "self_hosted_infrastructure_compute_estimate"
    text = markdown_table(comparison)
    assert "2025 customer compute prices" in text
    assert "43/43" in text
    assert "Trino + Hoglake (1 workers)" in text
    assert "All 129 executions" in text
    assert ours["all_attempts_runtime_s"] == 43 * 6
    assert ours["all_attempts_compute_usd"] == pytest.approx(2.58)


@pytest.mark.parametrize(
    "alteration", ["incomplete", "missing_attempt", "smoke_dataset", "unknown_dataset"]
)
def test_our_incomplete_or_smoke_report_cannot_be_ranked(alteration):
    ours = report()
    if alteration == "incomplete":
        ours["complete"] = False
    elif alteration == "missing_attempt":
        ours["attempts_recorded"] = 128
    elif alteration == "smoke_dataset":
        ours["metadata"]["dataset_rows"] = 1000
    else:
        del ours["metadata"]["dataset_rows"]
    row = report_row(ours)
    assert not row["complete"]
    assert row["runtime_times_cost"] is None


def test_our_report_totals_must_agree_with_all_per_query_timings():
    ours = report()
    ours["total_best_s"] = 42
    with pytest.raises(ValueError, match="disagree"):
        report_row(ours)


def test_download_and_cache_are_verified_against_pinned_checksum(tmp_path, monkeypatch):
    payload = json.dumps(record()).encode()
    chosen = Reference(
        "synthetic",
        "Synthetic",
        "synthetic.json",
        hashlib.sha256(payload).hexdigest(),
        "enterprise",
    )
    seen = []

    def get(url, **kwargs):
        seen.append(url)
        return httpx.Response(200, content=payload, request=httpx.Request("GET", url))

    monkeypatch.setattr(comparison_module.httpx, "get", get)
    assert load_reference(chosen, tmp_path) == record()
    assert load_reference(chosen, tmp_path) == record()
    assert len(seen) == 1
    next(tmp_path.glob("*.json")).write_text("{}")
    with pytest.raises(ValueError, match="checksum"):
        load_reference(chosen, tmp_path)


def test_unavailable_references_remain_visible_instead_of_shrinking_comparison(
    tmp_path, monkeypatch
):
    path = tmp_path / "our-report.json"
    path.write_text(json.dumps(report()))

    def unavailable(*args):
        raise httpx.ConnectError("unavailable")

    monkeypatch.setattr(comparison_module, "load_reference", unavailable)
    comparison = compare_files([path], tmp_path / "cache")
    assert len(comparison["rows"]) == 9
    assert not comparison["complete"]
    assert all(
        row["status"] == "reference_unavailable_or_invalid"
        for row in comparison["rows"][1:]
    )
    assert all(row["relative_score"] is None for row in comparison["rows"][1:])


def test_explicit_reference_set_has_eight_variants_and_seven_immutable_sources():
    assert len(REFERENCES) == 8
    assert len({reference.path for reference in REFERENCES}) == 7
    assert all(len(reference.sha256) == 64 for reference in REFERENCES)


@pytest.mark.parametrize(
    "problem",
    [
        "matrix-type",
        "too-many-queries",
        "attempt-type",
        "too-many-attempts",
        "negative",
        "nan",
        "boolean",
        "costs-type",
        "costs-entry",
    ],
)
def test_malformed_reference_cannot_receive_a_score(problem):
    entry = record()
    if problem == "matrix-type":
        entry["result"] = {}
    elif problem == "too-many-queries":
        entry["result"].append([1, 2, 3])
    elif problem == "attempt-type":
        entry["result"][0] = "123"
    elif problem == "too-many-attempts":
        entry["result"][0].append(4)
    elif problem in {"negative", "nan", "boolean"}:
        entry["result"][0][0] = {"negative": -1, "nan": float("nan"), "boolean": True}[
            problem
        ]
    elif problem == "costs-type":
        entry["costs"] = {}
    else:
        entry["costs"] = [None]
    with pytest.raises(ValueError):
        reference_row(reference(), entry)


def test_checksummed_non_object_reference_is_refused(tmp_path, monkeypatch):
    payload = b"[]"
    chosen = Reference(
        "synthetic",
        "Synthetic",
        "synthetic.json",
        hashlib.sha256(payload).hexdigest(),
        "enterprise",
    )
    monkeypatch.setattr(
        comparison_module.httpx,
        "get",
        lambda *a, **kw: httpx.Response(
            200, content=payload, request=httpx.Request("GET", chosen.url)
        ),
    )
    with pytest.raises(TypeError, match="JSON object"):
        load_reference(chosen, tmp_path)


def test_other_profile_and_reference_only_comparisons_are_refused():
    ours = report()
    ours["profile"] = "other-workload"
    with pytest.raises(ValueError, match="pinned CostBench profile"):
        report_row(ours)
    with pytest.raises(ValueError, match="at least one"):
        build_comparison([], [reference_row(reference(), record())])
