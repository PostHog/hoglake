"""Pin millrace's wiring into the ci-python.yml gate.

Phase 0's meta-test (docs/kafka-ingestion-plan.md): a workflow that
path-filters itself out of a PR leaves its required check "expected"
forever (AGENT.md, #299), and a gate that does not need a leaf job
cannot see that job fail (AGENT.md, #304). Both failure modes are
pinned here.
"""

from pathlib import Path

import yaml

WORKFLOW = (
    Path(__file__).resolve().parents[2] / ".github" / "workflows" / "ci-python.yml"
)


def _jobs():
    return yaml.safe_load(WORKFLOW.read_text())["jobs"]


def test_millrace_tree_triggers_the_workflow():
    steps = _jobs()["changes"]["steps"]
    filter_step = next(step for step in steps if step.get("id") == "filter")
    filters = yaml.safe_load(filter_step["with"]["filters"])
    assert "millrace/**" in filters["python"]


def test_python_checks_gate_needs_the_millrace_job():
    jobs = _jobs()
    assert "millrace" in jobs
    assert "millrace" in jobs["python-checks"]["needs"]
