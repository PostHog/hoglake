"""Fixtures for the live suite (marker ``live``, run by ci/live-millrace.sh).

NO SKIPS, by the harness's rule (ci/check_live_results.py fails the run
on a single skip): if the stack is not there, the ``stack`` fixture
FAILS — the suite only ever runs with the stack up (a bare ``pytest``
deselects the marker, see tests/conftest.py).

Scope layout: one session client/admin/fs over the harness's stack; one
catalog PER TEST (``lake_home`` — cheap: two POSTs; and snapshot-count
assertions stay catalog-clean); one kafka topic + consumer group per
test, deleted in teardown (the suite leaves nothing behind; catalogs
have no delete API — AGENT.md's known gap — and die with the stack).
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

import livekit
import pytest

# Everything in this directory is a live test.
pytestmark = pytest.mark.live


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    # NB: collection hooks from EVERY collected conftest see the WHOLE
    # session's items — scope the auto-marker to this directory or a
    # `pytest tests/` run would mark (and then deselect) the world.
    here = os.path.dirname(os.path.abspath(__file__)) + os.sep
    for item in items:
        if str(item.path).startswith(here):
            item.add_marker(pytest.mark.live)


# SlateDB's s3 store resolves from the standard AWS_* environment — map
# the harness's MinIO settings onto it BEFORE any stage opens.
for _name, _value in livekit.apply_slatedb_aws_env({}).items():
    os.environ.setdefault(_name, _value)


@pytest.fixture(scope="session")
def stack() -> str:
    """The harness's stack, verified — LOUDLY. Never a skip."""
    import httpx

    try:
        r = httpx.get(f"{livekit.HOGLAKE_URL}/v1/catalogs", timeout=5)
        assert r.status_code == 200, f"hoglake answered {r.status_code}"
    except Exception as e:
        raise AssertionError(
            f"no live hoglake server at {livekit.HOGLAKE_URL} ({e}) — the "
            "live suite runs through ci/live-millrace.sh, which starts and "
            "tears down the stack"
            # NB: raising, not pytest.skip — the harness fails on skips.
        ) from e
    fs = livekit.s3_config().filesystem()
    fs.create_dir(livekit.BUCKET)  # idempotent; allow_bucket_creation=True
    admin = livekit.kafka_admin()
    admin.list_topics(timeout=30)
    return livekit.HOGLAKE_URL


@pytest.fixture(scope="session")
def s3config(stack: str) -> Any:
    return livekit.s3_config()


@pytest.fixture(scope="session")
def fs(s3config: Any) -> Any:
    return s3config.filesystem()


@pytest.fixture(scope="session")
def client(stack: str, s3config: Any) -> Any:
    with livekit.HoglakeClient(livekit.HOGLAKE_URL, s3=s3config) as c:
        yield c


@pytest.fixture(scope="session")
def admin(stack: str) -> Any:
    return livekit.kafka_admin()


@dataclass
class LakeHome:
    """One test's hoglake home: its own catalog + namespace, events-like
    tables created with the live partition spec, all dropped at teardown."""

    client: Any
    catalog: Any
    namespace: Any
    fs: Any
    slug: str
    tables: list[Any] = field(default_factory=list)

    def create_events_table(self, name: str = "events") -> Any:
        table = livekit.create_events_table(self.namespace, name)
        self.tables.append(table)
        return table

    def head(self) -> int:
        return livekit.head_snapshot(self.catalog)

    def drop_all(self) -> None:
        for table in self.tables:
            try:
                table.drop()
            except Exception as e:  # noqa: BLE001 - teardown is best-effort
                print(f"teardown: dropping {table.name} failed: {e}")


@pytest.fixture
def lake_home(client: Any, fs: Any, request: pytest.FixtureRequest) -> Any:
    slug = request.node.name.removeprefix("test_")[:40]
    slug = "".join(c if c.isalnum() or c in "-_" else "-" for c in slug)
    catalog = client.create_catalog(
        f"mr-{livekit.RUN_ID}-{slug}".lower()[:60],
        f"s3://{livekit.BUCKET}/{livekit.RUN_ID}/lake/{slug}/",
    )
    home = LakeHome(
        client=client,
        catalog=catalog,
        namespace=catalog.create_namespace("ns1"),
        fs=fs,
        slug=slug,
    )
    yield home
    home.drop_all()


@dataclass
class Topic:
    """One test's topic (deleted at teardown) plus its consumer group."""

    admin: Any
    name: str
    group: str
    partitions: int


@pytest.fixture
def topic_factory(admin: Any, request: pytest.FixtureRequest) -> Any:
    created: list[str] = []
    slug = "".join(
        c if c.isalnum() or c in "-._" else "-"
        for c in request.node.name.removeprefix("test_")[:40]
    )

    def make(*, partitions: int = 4, name: str | None = None) -> Topic:
        topic_name = name or f"ev-{livekit.RUN_ID}-{slug}-{len(created)}"
        livekit.create_topic(admin, topic_name, partitions=partitions)
        created.append(topic_name)
        return Topic(
            admin=admin,
            name=topic_name,
            group=f"millrace-{livekit.RUN_ID}-{slug}-{len(created)}",
            partitions=partitions,
        )

    yield make
    for topic_name in created:
        try:
            livekit.delete_topic(admin, topic_name)
        except Exception as e:  # noqa: BLE001 - teardown is best-effort
            print(f"teardown: deleting topic {topic_name} failed: {e}")


@pytest.fixture
def producer_factory() -> Any:
    producers: list[livekit.EventProducer] = []

    def make(topic: str) -> livekit.EventProducer:
        producer = livekit.EventProducer(topic)
        producers.append(producer)
        return producer

    yield make
    for producer in producers:
        producer.close()
