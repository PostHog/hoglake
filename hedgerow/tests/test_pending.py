import json
import time

import pytest

from hedgerow.buffering import BufferPolicy
from hedgerow.config import ConfigError
from hedgerow.halts import DataIntegrityError, SplitBrainError
from hedgerow.pending import Fragment, PendingStore, SourceFile
from hedgerow.scheduler import FlushScheduler


@pytest.fixture
def store(tmp_path):
    state = PendingStore(
        str(tmp_path / "state.sqlite"), {"source_uuid": "raw", "dest_uuid": "events"}
    )
    yield state
    state.close()


def add(store, file_id, snapshot, committed_at, entries):
    store.discover(
        store.discovered,
        snapshot,
        [
            SourceFile(
                file_id, snapshot, committed_at, f"s3://b/{file_id}", len(entries)
            )
        ],
        [
            Fragment(file_id, team, (str(team), month), (0,), 1, size)
            for team, month, size in entries
        ],
    )


def publish(store, work):
    store.prepare(work.work_id, {"idempotency_key": work.work_id, "appends": []})
    store.published(work.work_id)


def test_defaults_overrides_and_strict_validation():
    policy = BufferPolicy.parse({})
    assert policy.max_age_s == 86400
    assert policy.target_file_bytes == 256 * 1024 * 1024
    assert BufferPolicy.parse({"team_max_age_s": {"42": 60}}).team_max_age_s == {42: 60}
    for value in (
        {"workers": 0},
        {"max_age_s": True},
        {"team_max_age_s": {1: -1}},
        {"max_age_s": float("nan")},
        {"unknown": 1},
        {"team_max_age_s": {1.5: 5}},
    ):
        with pytest.raises(ConfigError):
            BufferPolicy.parse(value)


def test_accumulates_across_windows_but_not_months(store):
    add(store, 1, 2, 100, [(1, "680", 40), (1, "681", 40), (2, "680", 80)])
    assert store.ready(101, 100) == []
    add(store, 2, 5, 200, [(1, "680", 60), (2, "680", 20)])
    ready = store.ready(201, 100)
    assert [(team, json.loads(key)) for team, key in ready] == [
        (1, ["1", "680"]),
        (2, ["2", "680"]),
    ]
    work = store.claim(*ready[0])
    assert [f["file_id"] for f in work.fragments] == [1, 2]
    assert store.discovered == 5
    assert store.published_through == 1
    publish(store, work)
    assert store.published_through == 1
    assert store.cleanup_candidates() == []


def test_age_anchored_to_oldest_commit_and_overrides(store):
    add(store, 1, 2, 100, [(1, "old-event-month", 1), (2, "new-event-month", 1)])
    add(store, 2, 3, 1000, [(1, "old-event-month", 1)])
    assert store.ready(159, 100, team_max_age_s={2: 60}) == []
    assert [r[0] for r in store.ready(160, 100, team_max_age_s={2: 60})] == [2]
    assert [r[0] for r in store.ready(86500, 100)] == [1, 2]


def test_new_arrival_does_not_join_frozen_work_or_reset_oldest(store):
    add(store, 1, 1, 10, [(1, "680", 1)])
    work = store.claim(*store.ready(86410, 100)[0])
    add(store, 2, 2, 20, [(1, "680", 1)])
    assert store.ready(999999, 1) == []
    assert len(store.work(work.work_id).fragments) == 1
    publish(store, work)
    assert store.ready(86420, 100)
    assert store.published_through == 1
    assert [f["file_id"] for f in store.cleanup_candidates()] == [1]


def test_restart_preserves_age_identity_and_request(tmp_path):
    path = str(tmp_path / "state.sqlite")
    store = PendingStore(path, {"source": "uuid"})
    add(store, 1, 3, 100, [(7, "1", 1)])
    work = store.claim(*store.ready(86500, 100)[0])
    request = {
        "idempotency_key": work.work_id,
        "appends": [{"files": ["immutable-path"]}],
    }
    store.prepare(work.work_id, request)
    with pytest.raises(SplitBrainError):
        PendingStore(path, {"source": "uuid"})
    store.close()
    store = PendingStore(path, {"source": "uuid"})
    assert store.recover()[0].request == request
    assert store.recover()[0].fragments[0]["committed_at"] == 100
    assert store.published_through == 2
    with pytest.raises(DataIntegrityError):
        store.prepare(work.work_id, {**request, "message": "changed"})
    store.published(work.work_id)
    assert store.published_through == store.discovered == 3
    store.close()
    with pytest.raises(DataIntegrityError, match="different job"):
        PendingStore(path, {"source": "recreated"})


def test_incomplete_discovery_never_advances(store):
    with pytest.raises(DataIntegrityError):
        store.discover(
            0,
            2,
            [SourceFile(1, 2, 100, "s3://b/a", 2)],
            [Fragment(1, 1, ("1",), (0,), 1, 1)],
        )
    assert store.discovered == 0
    assert store.cleanup_candidates() == []


def test_shared_file_reclaimable_only_after_all_teams_publish(store):
    add(store, 1, 1, 100, [(1, "1", 1), (2, "1", 1)])
    ready = store.ready(86500, 100)
    publish(store, store.claim(*ready[0]))
    assert store.cleanup_candidates() == []
    publish(store, store.claim(*ready[1]))
    assert len(store.cleanup_candidates()) == 1
    assert store.published_through == 1


def test_scheduler_ambiguous_commit_replays_exact_request(store):
    add(store, 1, 1, 100, [(1, "1", 1), (2, "1", 1)])
    prepared, calls, published = [], [], set()

    def prepare(work):
        prepared.append(work.work_id)
        return {"idempotency_key": work.work_id, "files": [f"output/{work.work_id}"]}

    def commit(request):
        calls.append(request)
        key = request["idempotency_key"]
        if key not in published:
            published.add(key)
            raise TimeoutError("server committed, response lost")

    scheduler = FlushScheduler(store, BufferPolicy(workers=2), prepare, commit)
    try:
        deadline = time.monotonic() + 5
        while store.published_through < 1:
            assert time.monotonic() < deadline
            try:
                scheduler.tick(86500)
            except TimeoutError:
                work_id, stage = scheduler.failed_work
                assert stage == "publish"
                assert work_id in published
                assert work_id in {w.work_id for w in store.recover()}
            time.sleep(0.001)
        assert scheduler.failed_work is None
        assert len(prepared) == len(published) == 2
        assert len(calls) == 4
        assert all(calls.count(request) == 2 for request in calls)
    finally:
        scheduler.close()


def test_distinct_committed_identities_may_reference_the_same_path(store):
    # Hoglake permits registering the same object more than once. This is not
    # event-level deduplication: both committed row ranges must be delivered.
    for snapshot in (1, 2):
        store.discover(
            snapshot - 1,
            snapshot,
            [SourceFile(snapshot, snapshot, 100, "s3://b/shared", 1)],
            [Fragment(snapshot, 1, ("1",), (0,), 1, 10)],
        )
    work = store.claim(*store.ready(100, 20)[0])
    assert len(work.fragments) == 2
