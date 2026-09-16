"""Bounded flush execution, separate from source discovery and retention offsets.

All SQLite operations run on the coordinator thread. Workers read immutable work
and create sorted output files; the coordinator fsyncs their exact commit request
before scheduling publication. Ambiguous commits retain that request unchanged.
The store's process lock must remain held until every worker has terminated.
"""

from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

from .buffering import BufferPolicy
from .pending import PendingStore, Work


class FlushScheduler:
    def __init__(
        self,
        store: PendingStore,
        policy: BufferPolicy,
        prepare: Callable[[Work], dict[str, Any]],
        publish: Callable[[dict[str, Any]], None],
    ):
        self.store = store
        self.policy = policy
        self.prepare = prepare
        self.publish = publish
        self.pool = ThreadPoolExecutor(
            max_workers=policy.workers, thread_name_prefix="flush"
        )
        self.active: dict[str, tuple[str, Future]] = {}

    def tick(self, now: float) -> None:
        # Harvest completions first. Any exception leaves its durable work for
        # retry; never release ownership or advance publication on an error.
        for key, (stage, future) in list(self.active.items()):
            if not future.done():
                continue
            del self.active[key]
            result = future.result()
            if stage == "prepare":
                work = self.store.prepare(key, result)
                self.active[key] = (
                    "publish",
                    self.pool.submit(self.publish, work.request),
                )
            else:
                self.store.published(key)
        for work in self.store.recover():
            if len(self.active) >= self.policy.workers:
                break
            if work.work_id not in self.active:
                self._submit(work)
        available = self.policy.workers - len(self.active)
        if available:
            for team, partition in self.store.ready(
                now,
                self.policy.target_file_bytes,
                self.policy.max_age_s,
                self.policy.team_max_age_s,
                available,
            ):
                self._submit(self.store.claim(team, partition))

    def _submit(self, work: Work) -> None:
        if work.request is None:
            self.active[work.work_id] = (
                "prepare",
                self.pool.submit(self.prepare, work),
            )
        else:
            self.active[work.work_id] = (
                "publish",
                self.pool.submit(self.publish, work.request),
            )

    def close(self) -> None:
        # Waiting is essential: releasing the store lock while an old publish
        # runs would allow a replacement coordinator to acquire the same job.
        self.pool.shutdown(wait=True, cancel_futures=True)
