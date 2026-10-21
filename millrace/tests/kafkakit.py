"""Scripted fake for the confluent-kafka Consumer surface millrace uses
(hedgerow ``tests/fakes.py`` pattern: one journal, and call ORDER is the
assertion surface). No broker anywhere: messages are fed, the topic's
partition count is a constructor argument, commit failures are injected.

Fidelity rules the fake keeps — the consume loop's correctness rests on
them:

- ``list_topics(topic)`` answers a ``ClusterMetadata`` the way the
  broker does: the requested topic with one ``PartitionMetadata`` per
  partition, or — for a topic the fake was not given — the topic
  present with ``UNKNOWN_TOPIC_OR_PART`` as its error and no
  partitions (``strict_metadata=True`` omits it instead, the other
  shape a metadata answer can take); ``fail_next_list_topics`` scripts
  a raised ``KafkaException`` or a topic-level error for the next calls;
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

import json
from collections import deque
from collections.abc import Mapping

from confluent_kafka import (
    OFFSET_INVALID,
    TIMESTAMP_CREATE_TIME,
    TIMESTAMP_NOT_AVAILABLE,
    KafkaError,
    KafkaException,
    TopicPartition,
)
from confluent_kafka.admin import ClusterMetadata, PartitionMetadata, TopicMetadata

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


def team_msg(
    partition: int,
    offset: int,
    *,
    team: int,
    tag: str | None = None,
    ts_ms: int | None = TS_MS,
    topic: str = "events",
) -> FakeMessage:
    """A well-formed message for ``team``: the JSON object payload
    ``{"team_id": <team>, "v": <tag>}`` (``tag`` defaults to
    ``v-<offset>``). The key is the team's decimal id, as a team-keyed
    producer would send it — millrace never reads it."""
    return msg(
        partition,
        offset,
        key=str(team).encode(),
        value=team_payload(team, tag if tag is not None else f"v-{offset}"),
        ts_ms=ts_ms,
        topic=topic,
    )


def team_payload(team: int, tag: str) -> bytes:
    """The payload :func:`team_msg` sends."""
    return json.dumps({"team_id": team, "v": tag}, separators=(",", ":")).encode()


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
    """The confluent-kafka Consumer surface millrace drives:
    ``list_topics``, ``committed``, ``assign``, ``consume``, ``commit``,
    ``close`` (plus ``pause``/``resume``). ``partitions`` is the
    partition count of the one topic (``topic``) the cluster knows;
    ``topics`` maps several. ``strict_metadata`` omits unknown topics
    from a metadata answer instead of reporting them with an error."""

    def __init__(
        self,
        *,
        journal: list[tuple] | None = None,
        partitions: int = 1,
        topic: str = "events",
        topics: Mapping[str, int] | None = None,
        strict_metadata: bool = False,
    ) -> None:
        self.journal: list[tuple] = journal if journal is not None else []
        self._queue: deque[FakeMessage] = deque()
        self._topics: dict[str, int] = (
            dict(topics) if topics is not None else {topic: partitions}
        )
        self._strict_metadata = strict_metadata
        self._fail_list_topics: deque[KafkaException | KafkaError] = deque()
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

    # -- scripting -------------------------------------------------------------

    def feed(self, *messages: FakeMessage) -> None:
        """Append messages to the fetch queue (as if produced)."""
        self._queue.extend(messages)

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

    def fail_next_list_topics(
        self,
        n: int,
        code: int = KafkaError._TRANSPORT,
        *,
        topic_level: bool = False,
    ) -> None:
        """The next ``n`` ``list_topics`` calls fail: raise
        ``KafkaException`` (a transport error, a timeout), or with
        ``topic_level=True`` answer with ``code`` as the requested
        topic's error (``LEADER_NOT_AVAILABLE`` on a fresh broker)."""
        for _ in range(n):
            err = KafkaError(code, f"scripted metadata failure {code}")
            self._fail_list_topics.append(err if topic_level else KafkaException(err))

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

    # -- the consumer surface ----------------------------------------------------

    def list_topics(
        self, topic: str | None = None, timeout: float = -1
    ) -> ClusterMetadata:
        """The cluster metadata (the startup partition-count read). A
        topic the fake was not given comes back the way a broker answers
        a metadata request for a nonexistent topic: present, with
        ``UNKNOWN_TOPIC_OR_PART`` as its error and no partitions."""
        self._assert_open()
        self.journal.append(("list_topics", topic, timeout))
        scripted = self._fail_list_topics.popleft() if self._fail_list_topics else None
        if isinstance(scripted, KafkaException):
            raise scripted
        cluster = ClusterMetadata()
        names = [topic] if topic is not None else list(self._topics)
        for name in names:
            meta = TopicMetadata()
            meta.topic = name
            count = self._topics.get(name)
            if scripted is not None:
                meta.error = scripted
            elif count is None:
                if self._strict_metadata:
                    continue
                meta.error = KafkaError(
                    KafkaError.UNKNOWN_TOPIC_OR_PART, f"Unknown topic {name!r}"
                )
            else:
                for i in range(count):
                    part = PartitionMetadata()
                    part.id = i
                    part.leader = 1
                    meta.partitions[i] = part
            cluster.topics[name] = meta
        return cluster

    def assign(self, partitions: list[TopicPartition]) -> None:
        self._assert_open()
        self.journal.append(
            ("assign", [(tp.topic, tp.partition, tp.offset) for tp in partitions])
        )
        self._assigned = list(partitions)

    def consume(self, num_messages: int = 1, timeout: float = -1) -> list[FakeMessage]:
        self._assert_open()
        self.consume_calls += 1
        self.requested_batch_sizes.append(num_messages)
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
        """The committed-offset probe (the start's coordinator
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
