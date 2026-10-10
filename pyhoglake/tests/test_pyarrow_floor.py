"""The pyarrow floor pyproject.toml declares is a claim only CI's pyarrow
matrix tests (pyhoglake-checks.yml): one the matrix does not install is
untested. pyproject.toml says why the floor is where it is."""

import json
import re
from pathlib import Path

import pytest


def test_the_ci_matrix_runs_the_declared_pyarrow_floor_and_millponds_pin():
    """Raising the floor without the matrix, or dropping millpond's pin
    from it, reds here."""
    root = Path(__file__).parents[2]
    workflow = root / ".github" / "workflows" / "pyhoglake-checks.yml"
    pyproject = Path(__file__).parents[1] / "pyproject.toml"
    if not workflow.exists():
        pytest.skip("not a repository checkout")
    (floor,) = re.findall(r'"pyarrow>=([0-9.]+)"', pyproject.read_text())
    matrix = re.search(r"pyarrow: \[([^\]]*)\]", workflow.read_text())
    assert matrix is not None
    versions = json.loads(f"[{matrix.group(1)}]")
    assert f"{floor}.0" in versions or floor in versions
    assert {"23.0.1", "locked", "latest"} <= set(versions)
    # "latest" resolves at run time, so it may not gate a required check.
    assert (
        "continue-on-error: ${{ matrix.pyarrow == 'latest' }}" in workflow.read_text()
    )


def test_the_latest_pyarrow_canary_keeps_the_dependency_cooldown():
    """The canary runs the newest pyarrow's native code across the suite,
    in the publish run too; dependabot.yml's uv cooldown holds every uv
    dependency back for its days (7) before a PR carries it in, and the
    canary must hold pyarrow back as long, or a release published today
    runs here today."""
    root = Path(__file__).parents[2]
    workflow = root / ".github" / "workflows" / "pyhoglake-checks.yml"
    if not workflow.exists():
        pytest.skip("not a repository checkout")
    (install,) = re.findall(
        r"uv pip install --upgrade\b[^\n]*\n[^\n]*", workflow.read_text()
    )
    (days,) = re.findall(r"--exclude-newer \"\$\(date -u -d '(\d+) days ago'", install)
    assert install.rstrip().endswith("pyarrow")
    # The uv ecosystem's own cooldown: every ecosystem's block carries a
    # default-days of its own.
    dependabot = (root / ".github" / "dependabot.yml").read_text()
    (uv,) = re.findall(
        r"- package-ecosystem: uv\n(.*?)(?=\n  - package-ecosystem:|\Z)",
        dependabot,
        re.DOTALL,
    )
    (cooldown,) = re.findall(r"default-days: (\d+)", uv)
    assert days == cooldown
