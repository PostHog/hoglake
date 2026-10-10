"""Scripted fake for the confluent-kafka Consumer surface millrace uses
(hedgerow ``tests/fakes.py`` pattern: one journal, and call ORDER is the
assertion surface). No broker anywhere: messages are fed, rebalances are
scripted, commit failures are injected.

Fidelity rules the fake keeps — the consume loop's correctness rests on
them:

- rebalance callbacks (``on_assign`` / ``on_revoke`` / ``on_lost``) fire
  INSIDE ``consume()``, the way librdkafka fires them inside
  poll/consume: ``script_rebalance`` queues the event and the next
  ``consume()`` fires the callbacks (revokes and losts before assigns)
  before delivering messages;
- a paused or unassigned partition's queued messages are NOT delivered
  (librdkafka stops fetching them) and stay queued for a later
  resume/reassignment; error events deliver whenever their partition is
  assigned (pause gates fetches, not error events);
- ``commit(asynchronous=False)` returns the committed TopicPartitions
  on success and raises ``KafkaException`` on a scripted failure;
- ``pause``/``resume`` of an unassigned partition is a test bug and
  raises (AssertionError).

The journal interleaves with the StageManager's ack hook when tests hand
both the same list — which is how the rows-then-offset call-order
invariant is asserted (every ``("commit", …)`` entry is preceded by the
durable ``("ack", …)`` it covers).
"""

from __future__ import annotations

from collections import deque
from collections.abc import Iterable
from typing import Any

from confluent_kafka import (
    OFFSET_INVALID,
    TIMESTAMP_CREATE_TIME,
    TIMESTAMP_NOT_AVAILABLE,
    KafkaError,
    KafkaException,
    TopicPartition,
)

TS_MS = 1_800_000_000_000  # default message timestamp, ms (≈ 2027-01-15)


class FakeMessage:
    """The ``MessageView`` shape: error/topic/partition/offset/key/value/
    timestamp, constructible without a broker."""

    def __init__(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        key: bytes | None,
        value: bytes | None,
        timestamp: tuple[int, int],
        error: KafkaError | None = None,
    ) -> None:
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key
        self._value = value
        self._timestamp = timestamp
        self._error = error

    def error(self) -> KafkaError | None:
        return self._error

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes | None:
        return self._key

    def value(self) -> bytes | None:
        return self._value

    def timestamp(self) -> tuple[int, int]:
        return self._timestamp


def msg(
    partition: int,
    offset: int,
    *,
    key: bytes | None,
    value: bytes | None = b"{}",
    topic: str = "events",
    ts_ms: int | None = TS_MS,
    error: KafkaError | None = None,
) -> FakeMessage:
    """One scripted message; ``ts_ms=None`` models TIMESTAMP_NOT_AVAILABLE."""
    ts = (
        (TIMESTAMP_NOT_AVAILABLE, 0)
        if ts_ms is None
        else (TIMESTAMP_CREATE_TIME, ts_ms)
    )
    return FakeMessage(
        topic=topic,
        partition=partition,
        offset=offset,
        key=key,
        value=value,
        timestamp=ts,
        error=error,
    )


def keyed_msg(
    partition: int,
    offset: int,
    *,
    team: int,
    value: bytes | None = None,
    ts_ms: int | None = TS_MS,
    topic: str = "events",
) -> FakeMessage:
    """A well-formed utf8-decimal-keyed message."""
    return msg(
        partition,
        offset,
        key=str(team).encode(),
        value=value if value is not None else f"v-{offset}".encode(),
        ts_ms=ts_ms,
        topic=topic,
    )


def error_event(
    partition: int,
    offset: int,
    code: int,
    *,
    topic: str = "events",
) -> FakeMessage:
    """A broker/client error event (no stageable record)."""
    return msg(
        partition,
        offset,
        key=None,
        value=None,
        topic=topic,
        error=KafkaError(code, f"scripted error {code}"),
    )


class FakeConsumer:
    """The confluent-kafka Consumer surface millrace drives: ``consume``,
    ``commit``, ``assign``, ``subscribe``, ``assignment``, ``pause``,
    ``resume``, ``close``."""

    def __init__(self, *, journal: list[tuple] | None = None) -> None:
        self.journal: list[tuple] = journal if journal is not None else []
        self._queue: deque[FakeMessage] = deque()
        self._pending_rebalance: list[tuple[str, list[TopicPartition]]] = []
        self._pending_rebalance_after: list[tuple[str, list[TopicPartition]]] = []
        self._cbs: dict[str, Any] = {}
        self._assigned: list[TopicPartition] = []
        self.paused: set[tuple[str, int]] = set()
        self.committed_store: dict[tuple[str, int], int] = {}
        self._fail_commits: deque[KafkaException] = deque()
        self._fail_committed: deque[KafkaException] = deque()
        self.consume_calls = 0
        #: ``num_messages`` of every ``consume()`` call — the batch-size
        #: request surface (the byte-cap clamp's observable).
        self.requested_batch_sizes: list[int] = []
        self.closed = False
        self.subscribed_topics: list[str] | None = None

    # -- scripting -------------------------------------------------------------

    def feed(self, *messages: FakeMessage) -> None:
        """Append messages to the fetch queue (as if produced)."""
        self._queue.extend(messages)

    def script_rebalance(
        self,
        *,
        assign: Iterable[int] = (),
        revoke: Iterable[int] = (),
        lost: Iterable[int] = (),
        topic: str = "events",
        after_delivery: bool = False,
    ) -> None:
        """Queue a rebalance; the NEXT ``consume()`` fires the callbacks
        (librdkafka fires them inside poll/consume, so that is when
        millrace sees them). Revokes and losts fire before assigns.

        ``after_delivery=False`` (the common shape): the callbacks fire
        BEFORE this consume delivers, so a revoked partition's queued
        messages are held back (librdkafka drops the fetch buffer of a
        revoked partition; the new owner refetches from the last
        commit). ``after_delivery=True`` models the other real ordering:
        already-queued messages ARE returned by the call whose callbacks
        revoke the partition, and the consumer-side guard must skip
        them."""
        target = (
            self._pending_rebalance_after if after_delivery else self._pending_rebalance
        )
        for kind, partitions in (
            ("revoke", revoke),
            ("lost", lost),
            ("assign", assign),
        ):
            tps = [TopicPartition(topic, p) for p in partitions]
            if tps:
                target.append((kind, tps))

    def fail_next_commits(self, n: int, code: int = KafkaError._TRANSPORT) -> None:
        """The next ``n`` commit calls raise ``KafkaException``."""
        for _ in range(n):
            self._fail_commits.append(
                KafkaException(KafkaError(code, "scripted commit failure"))
            )

    def fail_next_committed(
        self, n: int, code: int = KafkaError.NOT_COORDINATOR
    ) -> None:
        """The next ``n`` committed-offset probes raise ``KafkaException``
        (a fresh broker's coordinator warmup, ``NOT_COORDINATOR``)."""
        for _ in range(n):
            self._fail_committed.append(
                KafkaException(KafkaError(code, "scripted committed failure"))
            )

    def deliverable(self) -> int:
        """Messages a ``consume()`` could return right now (assigned and,
        for data messages, unpaused)."""
        assigned = {(tp.topic, tp.partition) for tp in self._assigned}
        return sum(
            1
            for m in self._queue
            if (m.topic(), m.partition()) in assigned
            and (m.error() is not None or (m.topic(), m.partition()) not in self.paused)
        )

    def queued(self) -> int:
        """Every message still sitting in the fetch queue."""
        return len(self._queue)

    def has_pending_rebalance(self) -> bool:
        return bool(self._pending_rebalance or self._pending_rebalance_after)

    # -- the consumer surface ----------------------------------------------------

    def assign(self, partitions: list[TopicPartition]) -> None:
        self._assert_open()
        self.journal.append(
            ("assign", [(tp.topic, tp.partition, tp.offset) for tp in partitions])
        )
        self._assigned = list(partitions)

    def subscribe(
        self,
        topics: list[str],
        on_assign: Any = None,
        on_revoke: Any = None,
        on_lost: Any = None,
    ) -> None:
        self._assert_open()
        self.journal.append(("subscribe", list(topics)))
        self.subscribed_topics = list(topics)
        self._cbs = {"on_assign": on_assign, "on_revoke": on_revoke, "on_lost": on_lost}

    def assignment(self) -> list[TopicPartition]:
        self._assert_open()
        return list(self._assigned)

    def consume(self, num_messages: int = 1, timeout: float = -1) -> list[FakeMessage]:
        self._assert_open()
        self.consume_calls += 1
        self.requested_batch_sizes.append(num_messages)
        self._fire_pending_rebalance(self._pending_rebalance)
        self._pending_rebalance = []
        assigned = {(tp.topic, tp.partition) for tp in self._assigned}
        taken: list[FakeMessage] = []
        held: deque[FakeMessage] = deque()
        for m in self._queue:
            key = (m.topic(), m.partition())
            if len(taken) >= num_messages:
                held.append(m)
            elif m.error() is not None:
                # Error events deliver whenever the partition is assigned;
                # pause gates fetches, not errors.
                if key in assigned:
                    taken.append(m)
                else:
                    held.append(m)
            elif key in assigned and key not in self.paused:
                taken.append(m)
            else:
                held.append(m)
        self._queue = held
        self._fire_pending_rebalance(self._pending_rebalance_after)
        self._pending_rebalance_after = []
        if taken:
            self.journal.append(
                ("consume", [(m.topic(), m.partition(), m.offset()) for m in taken])
            )
        return taken

    def pause(self, partitions: list[TopicPartition]) -> None:
        self._assert_open()
        assigned = {(tp.topic, tp.partition) for tp in self._assigned}
        for tp in partitions:
            if (tp.topic, tp.partition) not in assigned:
                raise AssertionError(
                    f"pause of unassigned partition {tp.topic}[{tp.partition}]"
                )
        self.journal.append(("pause", [(tp.topic, tp.partition) for tp in partitions]))
        self.paused |= {(tp.topic, tp.partition) for tp in partitions}

    def resume(self, partitions: list[TopicPartition]) -> None:
        self._assert_open()
        assigned = {(tp.topic, tp.partition) for tp in self._assigned}
        for tp in partitions:
            if (tp.topic, tp.partition) not in assigned:
                raise AssertionError(
                    f"resume of unassigned partition {tp.topic}[{tp.partition}]"
                )
        self.journal.append(("resume", [(tp.topic, tp.partition) for tp in partitions]))
        self.paused -= {(tp.topic, tp.partition) for tp in partitions}

    def commit(
        self, offsets: list[TopicPartition] | None = None, asynchronous: bool = True
    ) -> list[TopicPartition]:
        self._assert_open()
        if self._fail_commits:
            raise self._fail_commits.popleft()
        offsets = offsets or []
        self.journal.append(
            ("commit", [(tp.topic, tp.partition, tp.offset) for tp in offsets])
        )
        for tp in offsets:
            self.committed_store[(tp.topic, tp.partition)] = tp.offset
        return offsets

    def committed(
        self, partitions: list[TopicPartition], timeout: float = -1
    ) -> list[TopicPartition]:
        """The committed-offset probe (the static start's coordinator
        warmup). Answers from the store, ``OFFSET_INVALID`` where no
        offset exists — the broker's answer for a fresh group."""
        self._assert_open()
        self.journal.append(
            ("committed", [(tp.topic, tp.partition) for tp in partitions])
        )
        if self._fail_committed:
            raise self._fail_committed.popleft()
        return [
            TopicPartition(
                tp.topic,
                tp.partition,
                self.committed_store.get((tp.topic, tp.partition), OFFSET_INVALID),
            )
            for tp in partitions
        ]

    def close(self) -> None:
        self._assert_open()
        self.journal.append(("close",))
        self.closed = True

    # -- internals ---------------------------------------------------------------

    def _assert_open(self) -> None:
        if self.closed:
            raise RuntimeError("consumer is closed")

    def _fire_pending_rebalance(
        self, pending: list[tuple[str, list[TopicPartition]]]
    ) -> None:
        for kind, tps in pending:
            cb = self._cbs.get(f"on_{kind}")
            if cb is not None:
                self.journal.append(
                    ("rebalance_cb", kind, [(tp.topic, tp.partition) for tp in tps])
                )
                cb(self, tps)
            # librdkafka's automatic incremental_unassign/incremental_assign
            # when the app does not call them (millrace does not).
            tpset = {(tp.topic, tp.partition) for tp in tps}
            if kind in ("revoke", "lost"):
                self._assigned = [
                    tp for tp in self._assigned if (tp.topic, tp.partition) not in tpset
                ]
                self.paused -= tpset
            else:
                have = {(tp.topic, tp.partition) for tp in self._assigned}
                self._assigned.extend(
                    tp for tp in tps if (tp.topic, tp.partition) not in have
                )
