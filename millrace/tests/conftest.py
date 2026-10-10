"""Suite-wide fixtures and hooks for the millrace tests.

- One process-global hypothesis profile: ``derandomize`` (every run
  explores the same examples — in CI, and under mutmut, where a mutant
  must face exactly the inputs that killed or missed it before) and
  ``deadline=None`` (CI/laptop timing variance must not flake tests;
  pyhoglake's conftest pattern).
- Async test support: ``async def`` tests run on a fresh asyncio loop
  via ``pytest_pyfunc_call`` — no plugin dependency (the slatedb
  binding's UniFFI futures bridge into asyncio; nothing else is needed).
- ``--bench``: the rate benches (``tests/test_stage_bench.py``,
  marker ``bench``) are DESELECTED from a default run — deselected, not
  skipped, so the no-skips rule stays intact. Run them with
  ``uv run pytest --bench -m bench -s``.
- ``live`` (``tests/live/``, the docker stack: hoglake server + kafka +
  MinIO) is DESELECTED the same way unless the ``-m`` expression
  mentions ``live``: a bare ``pytest`` run needs no services, and the
  live harness (``ci/live-millrace.sh``) runs exactly these via
  ``-m live``. Deselected, not skipped — the live harness fails on ANY
  skip in the report.
"""

import asyncio
import inspect
import re
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from hypothesis import HealthCheck, settings
from stagekit import NOW, fast_flush_settings

from millrace.stage import AckHook, PartitionStage

settings.register_profile(
    "millrace",
    deadline=None,
    derandomize=True,
    suppress_health_check=[HealthCheck.too_slow],
)
settings.load_profile("millrace")


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--bench",
        action="store_true",
        default=False,
        help="run the non-gating SlateDB rate benches (tests/test_stage_bench.py)",
    )


# A -m expression selects live tests iff the marker name appears as a
# word ("live", "live or ...", "not live" — the mark plugin then applies
# the polarity). The live suite lives only in tests/live/.
_LIVE_MARKER = re.compile(r"\blive\b")


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    if not config.getoption("--bench"):
        items[:] = [item for item in items if "bench" not in item.keywords]
    markexpr = getattr(config.option, "markexpr", "") or ""
    if not _LIVE_MARKER.search(markexpr):
        items[:] = [item for item in items if "live" not in item.keywords]


def pytest_pyfunc_call(pyfuncitem: pytest.Function) -> bool | None:
    """Run coroutine test functions on a fresh event loop.

    Returns None for sync tests so pytest's default call runs — this is
    a firstresult hook and returning False would SILENTLY skip the test
    body (pinned by the integration suite actually driving a
    subprocess)."""
    testfunction = pyfuncitem.obj
    if not inspect.iscoroutinefunction(testfunction):
        return None
    argnames = pyfuncitem._fixtureinfo.argnames
    testargs = {name: pyfuncitem.funcargs[name] for name in argnames}
    asyncio.run(testfunction(**testargs))
    return True


@pytest.fixture
def memory_store() -> Any:
    """One isolated in-memory object store per test.

    Each ``ObjectStore.resolve("memory:///")`` is a SEPARATE store
    (pinned by the parity suite), so a stage closed and reopened through
    this fixture's store sees the same data — that is what makes
    recovery testable without ``file:///``.
    """
    from slatedb.uniffi import ObjectStore

    return ObjectStore.resolve("memory:///")


StageFactory = Callable[..., Awaitable[PartitionStage]]


@pytest.fixture
def stage_factory(memory_store: Any) -> StageFactory:
    """Open PartitionStages on the test's shared memory store.

    One (topic, partition) may be open only once at a time (SlateDB is
    single-writer per path and the binding fences the older handle);
    close before reopening.
    """

    async def open_stage(
        *,
        topic: str = "events",
        partition: int = 0,
        settings: Any = None,
        ack_hook: AckHook | None = None,
    ) -> PartitionStage:
        return await PartitionStage.open_store(
            memory_store,
            f"millrace/{topic}/{partition}",
            topic=topic,
            partition=partition,
            settings=settings if settings is not None else fast_flush_settings(),
            ack_hook=ack_hook,
        )

    return open_stage


class FakeClock:
    """A manually advanced microsecond clock — the injected ``now_us``
    for anything time-based (staging timestamps, the age gauge). Tests
    cross deadlines by advancing it, never by sleeping."""

    def __init__(self, now_us: int = NOW) -> None:
        self.now_us = now_us

    def __call__(self) -> int:
        return self.now_us

    def advance(self, delta_us: int) -> None:
        self.now_us += delta_us


@pytest.fixture
def fake_clock() -> FakeClock:
    return FakeClock()
