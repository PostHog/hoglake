"""The pyarrow floor pyproject.toml declares, and the CI matrix that tests
it (pyhoglake-checks.yml). pyproject.toml says why the floor is where it
is; the VARIANT writer is what holds it at 23 or above."""

import re
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "pyhoglake-checks.yml"

#: The lowest pyarrow the VARIANT writer works on (measured on 21.0.0,
#: 22.0.0, 23.0.0, 23.0.1, 24.0.0, 25.0.1 and 26.0.0): 21 cannot open a
#: file with a VARIANT group at all, and 21 and 22 cannot read the
#: statistics of an INT32/INT64 DECIMAL column, which is how a decimal4 or
#: decimal8 leaf is written.
VARIANT_FLOOR = (23, 0)


def _floor() -> str:
    pyproject = tomllib.loads(
        (Path(__file__).parents[1] / "pyproject.toml").read_text()
    )
    (floor,) = [
        dependency.removeprefix("pyarrow>=")
        for dependency in pyproject["project"]["dependencies"]
        if dependency.startswith("pyarrow")
    ]
    return floor


def test_the_declared_floor_is_one_the_variant_writer_runs_on():
    """A floor lowered below 23 would let an install resolve a pyarrow that
    cannot read back what write_variant_parquet writes."""
    assert re.fullmatch(r"\d+(\.\d+){1,2}", _floor())
    assert tuple(int(part) for part in _floor().split(".")[:2]) >= VARIANT_FLOOR


def _matrix() -> str:
    if not WORKFLOW.exists():
        pytest.skip("not a repository checkout")
    return WORKFLOW.read_text()


def test_the_ci_matrix_runs_the_variant_suites_on_the_declared_floor():
    """The floor is read from pyproject.toml when the job runs, so it can
    move (Dependabot raises it with the lock) without the matrix; "latest"
    resolves at run time, so it may not gate a required check."""
    workflow = _matrix()
    (matrix,) = re.findall(r"pyarrow: \[([^\]]*)\]", workflow)
    assert [v.strip().strip('"') for v in matrix.split(",")] == ["floor", "latest"]
    assert "continue-on-error: ${{ matrix.pyarrow == 'latest' }}" in workflow
    assert 'tomllib.load(open("pyproject.toml", "rb"))' in workflow


def test_the_latest_pyarrow_canary_keeps_the_dependency_cooldown():
    """The canary runs the newest pyarrow's native code across the suites,
    in the publish run too; dependabot.yml's uv cooldown holds every uv
    dependency back for its days before a PR carries it in, and the canary
    must hold pyarrow back as long, or a release published today runs here
    today."""
    workflow = _matrix()
    (days,) = re.findall(r"--exclude-newer \"\$\(date -u -d '(\d+) days ago'", workflow)
    dependabot = (ROOT / ".github" / "dependabot.yml").read_text()
    (uv,) = re.findall(
        r"- package-ecosystem: uv\n(.*?)(?=\n  - package-ecosystem:|\Z)",
        dependabot,
        re.DOTALL,
    )
    (cooldown,) = re.findall(r"default-days: (\d+)", uv)
    assert days == cooldown
