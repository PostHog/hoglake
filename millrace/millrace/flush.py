"""The flush path: staged range → Arrow → parquet → prepared commit.

One key's flush (docs/kafka-ingestion.md §The flush planner and §The
wire contract) — a key is one hoglake partition tuple's staged rows
(the consumer staged each record under the tuple the destination's spec
gave it at consume time): scan the staged range → decode payloads (v1:
JSON values) → clamp/drop junk ``event_time`` → one parquet file
per partition tuple under the table's LIVE partition spec (exactly one
file in the steady state, where the staged tuple and the live spec
agree; more when the group was keyed under an older spec or the junk
policy clamped a temporal source — the fanout is the correctness
backstop) → upload → persist the complete prepared request under
``prepared/`` → ``commit/prepared`` → on receipt, ONE staging
transaction deletes the staged range and the sched entries and drops
the persisted request. No ``flushed/`` marker: the settle is atomic and
the receipt is already in hand when it runs, so a marker could only
restate both (stage.py's docstring carries the argument); recovery's
ambiguity set is exactly the surviving ``prepared/`` entries.

The wire contract, from day one (AGENT.md invariant 12 — this writer is
born correct; it does not repeat millpond's blind-append shape):

- Every commit carries a ``read_snapshot`` — the snapshot the table
  shape was resolved at (``TableInfo.read_snapshot_id``), and the shape
  read ALWAYS happens before the staged range is scanned — and an
  ``expected_table_uuid`` pinned at startup resolution. Identity reads
  use ``totals=false`` exclusively: this pipeline never issues a totals
  read, not even at startup (the ``Table`` handle is constructed from
  the ``totals=false`` response directly).
- The table-shape cache refreshes at a QUARTER of the catalog's snapshot
  retention (millpond's ``_live_info_ttl_s`` rule: this cache is never
  staler than pyhoglake's own ``Table._cache``, which refreshes at
  HALF the retention — so a spec change the commit's conflict window
  cannot see is a spec no flush is still computing under). Retention is
  read through the same pyhoglake accessor pyhoglake's own threshold
  uses, so the two cannot disagree about the period.
- The COMPLETE prepared request is persisted (``prepared/``) AFTER the
  upload and BEFORE publication, and a lost or ambiguous response
  replays the persisted bytes — never a regenerated request. An
  ANSWERED refusal is the opposite rule: the server judged the payload
  and wrote zero rows, so the payload is dead and the flush is REBUILT
  from the still-staged rows against a fresh read. The two rules are
  kept deliberately distinct in the code below (``_publish`` retries
  identical bytes; the refusal arms drop the persisted entry).
- Regeneration is deterministic apart from one token: same staged
  rows, same table basis, same clock → the same request except for the
  per-build object-name token. The idempotency key is a pure function
  of the window's bounds and content (a receipt still dedupes a
  verbatim replay); object
  names are ``…/{key}/{build}/{index}.parquet`` with ``build`` a fresh
  uuid4 hex minted per build (millpond's rule: a retry under the SAME
  key writes fresh objects). A verbatim replay sends the persisted
  bytes and so names the persisted objects; only a REBUILD mints new
  names, so a rebuild never overwrites an object a landed first attempt
  may have registered. The cost: a crash between upload and persist
  leaves objects nothing names or counts. The payload is assembled here
  over pyhoglake's primitives
  (:func:`pyhoglake.types.column_to_arrow_field`,
  :func:`pyhoglake.parquet_schema.prepared_schema_matches`,
  :func:`pyhoglake.stats.extract_column_stats`,
  :mod:`pyhoglake.transforms`, :mod:`pyhoglake.upload`) instead of via
  ``Table.prepare_append_tables`` so the builder stays pure: the token
  arrives in :class:`FlushContext`.

Refusal taxonomy (each recovery pinned by its own test):

- 409 ``commit_conflict`` (retryable): drop the cached shape, back off
  (injected sleep), resend the IDENTICAL request — bounded attempts.
- 409 ``ddl_since_read_snapshot``: NEVER replay the refused payload.
  The refusal is definitive (one commit is one transaction: zero writes
  happened), so the persisted entry is dropped, the uploaded objects are
  orphan-accounted, the shape cache is dropped, and the still-staged
  rows are re-decided on a later sweep and RE-PREPARED against a fresh
  read.
- 409 ``table_recreated``: halt the pipeline loudly (incarnation guard;
  viaduck lesson 3). The pinned uuid never changes hands mid-run.
- 410 (``ReadSnapshotExpiredError``) on a fresh publish: halt, carrying
  the server's reconcile instructions (viaduck lesson 4; "410 below the
  retention floor is a stop sign, never a skip"). On a RECOVERED entry's
  replay (``reconcile_prepared``): re-prepare like
  ``ddl_since_read_snapshot`` — drop the entry, rebuild from the
  still-staged rows on a later sweep — but count its objects UNRESOLVED
  rather than orphaned (a landed first attempt may own them).
- 503 ``commit_queue_timeout`` (and other 5xx / transport failures):
  ambiguous — back off per ``Retry-After`` (captured through an httpx
  hook, millpond's pattern) floored against an exponential curve, retry
  the IDENTICAL persisted request, bounded attempts. Exhaustion leaves
  the persisted entry in place; the next sweep replays it (receipt
  lookup first).
- The idempotency key names the window's CONTENT (a SHA-256 over its
  rows is a key input), and every build mints a fresh object token, so
  a rebuild of content that already landed would differ in body from
  the landed request. A fresh build therefore looks its key's receipt
  up BEFORE it builds: a 200 means this exact content already landed
  (an identical rewind) and the window settles locally
  (``millrace_flush_deduped_total``). A 422 saying the key was reused
  with a DIFFERENT request halts (``idempotency_key_reused``) on both
  paths: on a fresh publish it means another writer committed this
  content after the lookup, on a persisted entry's verbatim replay a
  foreign writer or broken determinism — never settle rows this
  process did not publish.
- EVERY OTHER answered 4xx (a 401/403 from a proxy, a 413 from an
  ingress, a plain 400, any 422 — request-shape ``validation`` or
  otherwise): HALT (``commit_refused``) with the rows staged, exactly
  like ``idempotency_key_reused`` and ``destination_missing``. No
  answered refusal deletes staged rows: the persisted ``prepared/``
  entry plus the staged rows are the operator's reconciliation
  material. (The pre-fix behavior discarded
  the whole window on any answered 4xx, which is why this paragraph
  exists.)

The staged scan order is Kafka OFFSET order (row keys are ``(group,
offset)``); the catalog's sort spec is PAYLOAD-EVENT time
(``TableInfo.sort_spec``), which late and skewed events invert
constantly. :func:`build_prepared_plan` reconciles the two:
after the Arrow build and BEFORE the partition fanout it sorts the batch
by the live spec's fields, directions and null placement (native Arrow
compute), so every registered file is key-nondecreasing under the spec
compaction's sortedness checks enforce — "the ingest sort and the
catalog sort spec must match" (docs/kafka-ingestion.md §What the server
owns) is a property of the file, not of the arrival order.

Stats policy (m10 — decided by REALIZED size, not by trigger): a file
whose realized parquet size reaches half of ``target_output_bytes``
ships full footer stats (large files are where pruning pays — a
post-outage catch-up flush on the age/slow lane is exactly such a
file); below the threshold the file registers ``stats_mode=deferred``
and the hydrator backfills off the commit path.

Bounded unit of work (M4): a decision's scan is capped in staged bytes
AND rows, so a post-outage catch-up slice is flushed in bounded windows
over consecutive sweeps instead of one unbounded scan → Arrow build →
settlement. The byte budget derives from the size lane:
``ceil(target_output_bytes ÷ estimated_compression_ratio ×
SCAN_BUDGET_HEADROOM)``, floored at ``flush_scan_min_bytes``. The
arithmetic: the size trigger fires at ``staged × ratio ≥ target``, so
``target ÷ ratio`` staged bytes estimate to one target-sized output;
the headroom (×1.25) absorbs ratio-estimate error in the
under-compression direction and row-boundary quantization — an
over-sized file is a sizing miss compaction absorbs, an under-sized one
is the small-file regime the design exists to kill. Row keys sort by
(group, offset), so a scan — capped or not — is an exact offset PREFIX
of the group's staged rows, and the window is ``[first_scanned_offset,
last_scanned_offset]``: every staged row of the group inside it was
scanned. The window identity and the settlement guard both come from
the SCANNED rows, never the stats snapshot; rows past a capped scan
re-decide on the next sweep, so a group of N rows drains in
``⌈N ÷ cap⌉`` windows however it interleaves with other groups in the
Kafka partition. A single row larger than the byte budget still
flushes (one record is always a bounded unit), probed with a one-row
scan. Memory shape per decision: ≤ ``max(scan_budget, largest row)``
payload bytes materialized plus ``≤ flush_scan_max_rows`` decoded
records and the Arrow build over them; the settlement streams exactly
the window's key range within its transaction.

Compression-ratio feedback (the planner's size lane is fed an OBSERVED
ratio, docs/kafka-ingestion.md §The flush planner):
:class:`CompressionRatioEwma` keeps an EWMA of ``parquet bytes ÷
staged bytes`` over this table's completed flushes (halflife
``MILLRACE_COMPRESSION_RATIO_HALFLIFE``, default 20), resetting on a
table shape/incarnation change (a schema change makes the old
observations meaningless). In-memory only: a restart falls back to
:data:`~millrace.planner.DEFAULT_COMPRESSION_RATIO`, which is
deliberately small, so the fallback errs toward OVER-staging (bigger
files early, never a small-file regime) — no persisted estimate is
worth the drift hazard.

The commit message is deterministic (millpond's rules): a fixed
``key=value`` sequence, no wall-clock/random/per-attempt state, the
flushed offset window, and ``table=`` carries the incarnation uuid. It
is formatted inside the pure request builder — BEFORE any upload
(pinned by a source-order test) — and its ``offsets`` line is the
recovery material: a persisted request's flush window is parsed back
out of it (``parse_message_offsets``).

Recovery handoff: :meth:`FlushRunner.run_once` reconciles every newly
claimed partition before planning it: ``PartitionStage.recover()``
collects legacy ``flushed/`` markers (bounded pages) and returns each
surviving ``prepared/`` entry, which is reconciled by receipt lookup —
a 200 receipt settles the range locally (zero republication), and a
404 republishes the persisted bytes verbatim whatever the entry's age
(same idempotency key, so the server's receipt dedupes a landing replay
into one logical commit), logging the entry's age (``persisted_at`` on
the ``prepared/`` envelope). The server purges receipts after
``min(max(HOGLAKE_RECEIPT_RETENTION_SECONDS, 2 × snapshot retention),
30 d)``; by then a request's ``read_snapshot`` is below the catalog's
expiry floor (snapshot retention ≤ 30 d), so a replay that old is
answered 410 and the window re-prepares from the staged rows — under
the same key when the window is unchanged, always under fresh object
names (a new build token), so the first attempt's objects are never
touched. If the first attempt landed, the window lands a second time
(two file-row sets over two object sets): accepted
at-least-once behaviour, counted
(``millrace_prepared_reprepared_total{reason="read_snapshot_expired"}``;
the possibly-live objects in ``millrace_flush_unresolved_uploads_total``).
Without a snapshot floor the verbatim replay itself executes (a second
set of file rows over the same objects; ``millrace_prepared_replays_total``).
The envelope (v5) also carries the idempotency key's derivation inputs
— ``topic``, ``partition`` and the partition tuple as hoglake partition
values (field names beside wire strings) — cross-checked at recovery
against the stage's own identity and the ``prepared/`` key: an entry
naming another partition or tuple is foreign state, refused as
corruption rather than replayed under it.

Junk ``event_time`` policy: the check applies to the decoded values of
the live partition spec's TEMPORAL source columns (a ``month(timestamp)``
spec makes ``timestamp`` the checked column). Out-of-window values are
dropped (logged and counted under ``event_time_out_of_window``; the
row is deleted by its window's ordinary settlement) or clamped to the
window edge, per config; staged rows are NEVER mutated. A spec without a temporal
source leaves the policy with nothing to check (a documented no-op).

Decoder: v1 is JSON objects behind the :class:`PayloadDecoder`
protocol (a registry/Avro decoder slots in later). Column values are
coerced per record with per-record failure routing: a record that
cannot decode or cast is dropped (logged and counted) and never crashes
the flush. Strict JSON: the NaN/Infinity literals are refused at parse
time, and magnitude-overflow literals (``1e400`` parses to ``inf``
WITHOUT tripping the parse hook) are refused at coercion — nothing
non-finite reaches a float column or its stats, and integer values
outside a column's int64 (per-unit) range are refused the same way
rather than detonating the batch's Arrow build. Two more coercions
guard what Arrow would otherwise do SILENTLY at the batch build: a
catalog ``float`` (float32) column refuses magnitudes past the float32
range (Arrow's narrowing cast stores infinity without raising), and a
string carrying an unpaired surrogate (legal JSON: ``\\ud800`` parses
to a lone surrogate) is refused per record because Arrow's UTF-8
conversion raises a plain ``UnicodeEncodeError`` — outside the build
isolation's taxonomy, so unhandled it would wedge the whole window.
Supported column types
are the scalars (boolean, the int widths, float/double, string,
binary-as-base64, uuid, decimal, date, time, and the timestamp family
from epoch ints in the column's own unit or strict ISO-8601 strings),
``json`` (any JSON value, stored as its canonical text), ``struct``/
``list``/``map`` at any depth up to pyhoglake's nesting cap (a JSON
object, array, and object respectively, each child coerced by its own
catalog type; :mod:`millrace.coerce` has the exact rules). A column the
decoder cannot fill (a ``variant`` — pyarrow cannot write the native
Parquet VARIANT annotation hoglake requires — or a malformed container)
is a STARTUP refusal naming the column, not a runtime surprise. Should
a value still defeat the batch build (encoder drift), the build
isolates the guilty record and drops it (``uncastable_column``) — one
malformed record never loses the batch.

Sync/async discipline: pyhoglake is synchronous, so every client call
(table reads, uploads, commits, receipt lookups) runs on the flusher's
own single-worker executor via ``run_in_executor``; the asyncio loop
the flusher shares with the consumer is never blocked by hoglake I/O.
Clocks (``now_us``, ``monotonic``) and ``sleep`` are injected
throughout.
"""

from __future__ import annotations

import asyncio
import importlib.metadata
import json
import logging
import math
import re
import struct
import time
import uuid
from collections import deque
from collections.abc import Awaitable, Callable, Mapping, Sequence
from concurrent.futures import Executor, ThreadPoolExecutor
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import timedelta
from functools import partial
from typing import Any, Final, Literal, Protocol, TypeVar
from urllib.parse import quote

import httpx
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyhoglake import (
    Catalog,
    Column,
    CommitConflictError,
    CommitResult,
    DdlSinceReadSnapshotError,
    HoglakeClient,
    HoglakeError,
    IncarnationChangedError,
    Namespace,
    NotFoundError,
    ReadSnapshotExpiredError,
    Table,
    TableInfo,
    UnsupportedTypeError,
    ValidationError,
)
from pyhoglake.models import PartitionSpec
from pyhoglake.parquet_schema import prepared_schema_matches
from pyhoglake.stats import extract_column_stats
from pyhoglake.transforms import (
    partition_source_array,
    transform_strings,
    transform_value,
)
from pyhoglake.types import column_to_arrow_field
from pyhoglake.upload import (
    Upload,
    perform_upload,
    resolve_concurrency,
    run_uploads,
    widen_io_threads_for,
)
from slatedb.uniffi import Error as _SlateError

from . import keyspace
from .coerce import (
    EPOCH_DATE,
    MICROS_PER_DAY,
    REASON_UNCASTABLE_COLUMN,
    TIMESTAMP_UNITS,
    DecodeError,
    UnsupportedColumnError,
    coercer_for,
    load_json_object,
)
from .config import Config, EventTimePolicy
from .grouping import column_chains, field_names
from .keyspace import FlushedKey, PreparedRequest
from .planner import (
    DEFAULT_COMPRESSION_RATIO,
    FlushDecision,
    FlushTrigger,
    KeyStats,
    PlannerKnobs,
    plan_flush,
)
from .stage import (
    DroppedRecord,
    FlushCommitReport,
    PartitionStage,
    RecoveredPrepared,
    SettlementMismatchError,
    StageClosedError,
    StageCorruptionError,
    StageGauges,
    StageManager,
    gauges_from_stats,
)

log = logging.getLogger(__name__)

_R = TypeVar("_R")

_MICROS_PER_SECOND: Final = 1_000_000

# -- the flush's bounded drop vocabulary (the
# ``millrace_records_dropped_total`` reason label; the decode half —
# undecodable_payload, uncastable_column — lives in coerce.py, and the
# consumer's REASON_* constants cover the consume-time half) --
REASON_EVENT_TIME_OUT_OF_WINDOW: Final = "event_time_out_of_window"

# The publish retry ladders. 503 commit_queue_timeout / 5xx / transport
# failures are AMBIGUOUS and retry the identical persisted request; a plain
# 409 commit_conflict is RETRYABLE per the server and also resends the
# identical bytes (a fresh read_snapshot cannot help a payload that must be
# replayed verbatim, and for an append-only commit the row-content conflict
# kinds cannot fire — the arm exists for the mixed-refusal case).
_PUBLISH_MAX_ATTEMPTS: Final = 8
_CONFLICT_MAX_ATTEMPTS: Final = 5
_BASE_DELAY_S: Final = 0.5
_MAX_DELAY_S: Final = 30.0

# Statuses whose Retry-After header is honored (millpond's rule: only
# statuses that mean "not now" leave a hint, so a stray header on a 200 can
# never slow the pipeline down).
_RETRY_AFTER_STATUS: Final = frozenset({429, 503})

# Mirrored from pyhoglake.client (a private module constant): the retention
# assumed when the catalog will not say. The VALUE matters less than the two
# sides agreeing; the table-shape TTL derives from it as retention/4.
_ASSUMED_RETENTION_S: Final = 1800.0

# The server's 422 when a key is replayed with a payload that is not the one
# the receipt was written for, matched as a substring of the lower-cased
# error AND detail (millpond's `_error_text` rule: hoglake's body is
# {error, detail} and for a 422 the sentence is in the detail).
_REUSED_KEY_MARKER: Final = "idempotency_key reused"
#: ``millrace_prepared_reprepared_total`` reason labels.
_REPREPARE_DDL: Final = "ddl_since_read_snapshot"
_REPREPARE_EXPIRED: Final = "read_snapshot_expired"
REPREPARE_REASONS: Final = (_REPREPARE_DDL, _REPREPARE_EXPIRED)

# Transforms whose source values the junk event_time policy checks.
_TEMPORAL_TRANSFORMS: Final = frozenset({"year", "month", "day", "hour"})

_ORPHAN_URIS_LOGGED: Final = 20
_DROPPED_OFFSETS_LOGGED: Final = 20

_VERSION: str | None = None


def _millrace_version() -> str:
    """The running millrace version, named in every commit message — read
    ONCE (millpond's rule: a metadata lookup per flush for a value that
    cannot change while the process lives is waste)."""
    global _VERSION
    if _VERSION is None:
        try:
            _VERSION = importlib.metadata.version("millrace")
        except importlib.metadata.PackageNotFoundError:  # pragma: no cover
            _VERSION = "unknown"
    return _VERSION


# -- errors --------------------------------------------------------------------


class FlushError(Exception):
    """Base for flush-path failures."""


class FlushStartupError(FlushError):
    """Startup resolution/validation failed: the catalog, namespace or
    table, the server's capability set or the destination's shape —
    fail-fast at boot, never a runtime surprise (docs/kafka-ingestion.md
    §Deployment)."""


class FlushHalted(FlushError):
    """The pipeline must stop loudly (viaduck lesson 3/4 posture):
    ``table_recreated``, a 410 below the expiry floor on a fresh publish
    (a recovered entry's 410 re-prepares instead), an idempotency-key
    identity anomaly, the destination gone, any other answered 4xx
    (``commit_refused`` — the rows stay staged and the persisted entry
    stays for an operator; no answered refusal deletes rows), a
    settlement that disagrees with the scan, a fanout the
    commit bound cannot hold. ``reason`` is a bounded vocabulary, named in the exception
    text, the halt log line and the ``/healthz`` 503 body (no metric
    exports it); ``detail`` carries the operator-facing text (for a 410,
    the server's reconcile instructions)."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


class FlushTransient(FlushError):
    """A bounded retry ladder ran out on transient server or transport
    failures. Nothing was settled or deleted; the next sweep retries.
    The runner counts it as a failed decision and logs ONE warning line
    (no stack trace)."""


class FlushPublishExhausted(FlushTransient):
    """The publish ladder ran out on an ambiguous or retryable failure.
    The persisted ``prepared/`` entry stays: the next sweep replays it
    (receipt lookup first). Contained per decision by the runner — the
    sweep is never wedged by one key."""


class ReceiptLookupExhausted(FlushTransient):
    """The receipt-lookup ladder ran out on 5xx/408/429 or transport
    failures. Nothing was built, published or settled."""


# -- counters ------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FlushStats:
    """Cumulative flush counters (passive observability — nothing here
    feeds control flow; the operational surface reads a snapshot).
    ``commit_attempts`` counts every commit RPC including retried ones;
    ``commit_retries`` counts only the retried ones, split by cause.
    ``records_dropped`` maps a flush-time drop reason to its row count;
    ``decisions_dropped`` counts decisions that dropped EVERY row (settled
    without a commit). ``flush_deduped`` counts fresh windows whose
    content (same bounds, same rows) already had a receipt at the
    pre-build lookup, settled with nothing new committed.
    ``reprepared`` maps a re-prepare reason
    (``ddl_since_read_snapshot``, ``read_snapshot_expired``) to the
    ``prepared/`` entries dropped for it; ``unresolved_uploads`` counts
    objects named by a dropped entry that may be live table files or
    orphans (never counted as orphans)."""

    flushes_committed: int = 0
    prepared_replays: int = 0
    receipt_settlements: int = 0
    flush_deduped: int = 0
    reprepared: Mapping[str, int] = dataclass_field(default_factory=dict)
    decisions_dropped: int = 0
    records_dropped: Mapping[str, int] = dataclass_field(default_factory=dict)
    rows_published: int = 0
    files_written: int = 0
    bytes_written: int = 0
    commit_attempts: int = 0
    commit_retries: int = 0
    conflict_retries: int = 0
    queue_retries: int = 0
    receipt_lookups: int = 0
    orphaned_uploads: int = 0
    unresolved_uploads: int = 0
    dropped_payload_keys: int = 0
    stats_repaired: int = 0


# -- the observed compression ratio (EWMA) -------------------------------------

#: The scan budget's headroom over ``target_output_bytes ÷ ratio``
#: (module docstring §Bounded unit of work).
SCAN_BUDGET_HEADROOM: Final = 1.25

#: A realized file at or above this fraction of ``target_output_bytes``
#: ships full footer stats; below it the file registers deferred (m10 —
#: the rule keys on the REALIZED size, not the deciding trigger, so a
#: big catch-up flush on the age/slow lane still ships stats).
FOOTER_STATS_SIZE_FRACTION: Final = 0.5


class CompressionRatioEwma:
    """EWMA of observed ``parquet bytes ÷ staged bytes`` for one table —
    the planner's ``estimated_compression_ratio`` input (the ratio is
    observed, not guessed; the planner stays pure and validates the
    estimate it is handed).

    ``estimate`` is :data:`DEFAULT_COMPRESSION_RATIO` until the first
    observation lands. The first observation REPLACES the fallback
    (blending a made-up constant into real data would slow convergence
    for no robustness: the fallback's only job is covering "no data
    yet"). Broken observations never poison the estimate: a flush with
    zero staged bytes read (or zero parquet bytes out) is skipped, and
    a ratio above 1 (parquet larger than the staged bytes — pathological
    for this pipeline's payloads) is clamped to 1.0, the largest value
    the planner's ``0 < r <= 1`` domain admits. :meth:`reset` forgets
    everything (a table shape/incarnation change makes old observations
    meaningless). In-memory only, by design (module docstring).
    """

    def __init__(self, halflife: float) -> None:
        if halflife < 1:
            raise ValueError(f"halflife must be >= 1, got {halflife}")
        # Per-observation decay factor: after `halflife` observations a
        # value's weight has halved.
        self._decay = 0.5 ** (1.0 / halflife)
        self._ewma: float | None = None
        self._count = 0

    @property
    def estimate(self) -> float:
        """The current planner input (the fallback until the first
        observation)."""
        return self._ewma if self._ewma is not None else DEFAULT_COMPRESSION_RATIO

    @property
    def observations(self) -> int:
        return 0 if self._ewma is None else self._count

    def observe(self, parquet_bytes: int, staged_bytes: int) -> float | None:
        """Fold one completed flush's realized ratio into the estimate.
        Returns the observation used (None when skipped as broken)."""
        if staged_bytes <= 0 or parquet_bytes <= 0:
            return None
        ratio = min(parquet_bytes / staged_bytes, 1.0)
        if self._ewma is None:
            self._ewma = ratio
            self._count = 1
        else:
            self._ewma = self._decay * self._ewma + (1 - self._decay) * ratio
            self._count += 1
        return ratio

    def reset(self) -> None:
        """Forget every observation (the next estimate is the fallback
        again). Called when the table's shape or incarnation changes."""
        self._ewma = None
        self._count = 0


# -- the payload decoder --------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DecodedRecord:
    """One staged payload decoded to column-aligned values. ``values``
    holds every destination column (null where the payload lacked a
    nullable key); ``dropped_keys`` are payload keys the table does not
    have (dropped, never silently: the flusher counts and logs them)."""

    values: dict[str, Any]
    dropped_keys: tuple[str, ...]


class PayloadDecoder(Protocol):
    """Staged payload bytes → one row's column values, aligned to the
    destination's columns. v1 is JSON objects; a schema-registry
    (Avro/Proto) decoder slots in behind this protocol (design doc §Open
    questions)."""

    def decode_record(self, payload: bytes) -> DecodedRecord:
        """Decode one staged payload. Raises :class:`DecodeError` —
        per-record, never the flush's death."""
        ...


class JsonObjectDecoder:
    """v1 decoder: staged payloads are JSON objects; keys are column
    names. Defensive per record (the flush drops failures, never
    crashes): strict JSON (no NaN/Infinity literals — and no
    magnitude-overflow literals: ``1e400`` parses to ``inf`` without
    tripping the parse hook, and the float coercion refuses anything
    non-finite), per-column coercion with int64-per-unit bounds,
    missing nullable keys → null, missing/explicit-null non-nullable
    keys → failure, unknown keys dropped (counted and logged by the
    flusher)."""

    def __init__(self, columns: tuple[Column, ...] | Sequence[Column]) -> None:
        self._columns = tuple(columns)
        self._plans = tuple((c, coercer_for(c)) for c in self._columns)
        self._names = frozenset(c.name for c in self._columns)
        # The Arrow schema is built once here too, so a column the batch
        # build could not describe (a malformed container) is a startup
        # refusal like any other.
        arrow_schema(self._columns)

    @property
    def columns(self) -> tuple[Column, ...]:
        return self._columns

    def decode_record(self, payload: bytes) -> DecodedRecord:
        doc = load_json_object(payload)
        values: dict[str, Any] = {}
        for column, coerce in self._plans:
            raw = doc.get(column.name)
            if raw is None:
                if not column.nullable:
                    raise DecodeError(
                        f"column {column.name!r} is not nullable and the "
                        "payload has no usable value",
                        reason=REASON_UNCASTABLE_COLUMN,
                        path=column.name,
                    )
                values[column.name] = None
                continue
            try:
                values[column.name] = coerce(raw)
            except DecodeError as e:
                # A nested failure already names its child's path; a
                # top-level scalar's is the column itself.
                if e.path is None:
                    e.path = column.name
                raise
        dropped = tuple(sorted(k for k in doc if k not in self._names))
        return DecodedRecord(values=values, dropped_keys=dropped)


# -- junk event_time policy -------------------------------------------------------


@dataclass(frozen=True, slots=True)
class JunkWindow:
    """The event_time validity window in micros: [min_us, max_us] —
    ``now − max_past`` … ``now + max_future``, computed once per flush
    from the injected clock (so the window is a pure input, never a
    wall-clock read). Comparisons happen in micros; clamping converts
    back to the column's own unit (floored — a clamped value is the
    representable edge, junk by definition either way)."""

    min_us: int
    max_us: int

    def contains(self, value_us: int) -> bool:
        return self.min_us <= value_us <= self.max_us


def _value_to_micros(col_type: str, value: Any) -> int:
    """A coerced temporal value → epoch micros for the window check."""
    if col_type == "date":
        days: int = (value - EPOCH_DATE).days
        return days * MICROS_PER_DAY
    v = int(value)
    if col_type == "timestamp_ns":
        return v // 1000
    return v * TIMESTAMP_UNITS[col_type]


def _micros_to_value(col_type: str, micros: int) -> Any:
    if col_type == "date":
        return EPOCH_DATE + timedelta(days=micros // MICROS_PER_DAY)
    if col_type == "timestamp_ns":
        return micros * 1000
    return micros // TIMESTAMP_UNITS[col_type]


def _temporal_sources(info: TableInfo) -> tuple[tuple[Any, tuple[str, ...], str], ...]:
    """The live spec's temporal partition sources as ``(field, path,
    column type)`` — the decoded values the junk event_time policy
    checks, by path (a struct leaf such as ``properties.ts`` included).
    Startup validation refuses sources under a list or a map and unknown
    sources; one that appears under a warm shape cache is skipped here
    and refused by :func:`_partition_groups`."""
    spec = info.partition_spec
    if spec is None or not spec.fields:
        return ()
    chains = column_chains(info.columns)
    return tuple(
        (
            field,
            tuple(c.name for c in chains[field.source_field_id]),
            chains[field.source_field_id][-1].type,
        )
        for field in spec.fields
        if field.transform in _TEMPORAL_TRANSFORMS
        and field.source_field_id in chains
        # A shape that changed under a warm cache to a source this writer
        # cannot read is refused loudly by the fanout, not crashed on here.
        and _source_fault(chains[field.source_field_id]) is None
    )


def _leaf_value(values: Mapping[str, Any], path: tuple[str, ...]) -> Any:
    """A decoded record's value at a struct path (None when any struct on
    the path is null)."""
    value: Any = values
    for step in path:
        if value is None:
            return None
        value = value[step]
    return value


def _set_leaf(values: dict[str, Any], path: tuple[str, ...], value: Any) -> None:
    """Rewrite a decoded value at a struct path (the clamp policy). The
    decoded dicts are this record's own, built by the coercers; the
    staged payload is never touched."""
    parent: Any = values
    for step in path[:-1]:
        parent = parent[step]
    parent[path[-1]] = value


# -- Arrow building -------------------------------------------------------------


def arrow_schema(columns: Sequence[Column]) -> pa.Schema:
    """The batch's Arrow schema: pyhoglake's
    (:func:`~pyhoglake.types.column_to_arrow_field` — field ids on every
    level). Raises :class:`UnsupportedColumnError` naming a column it
    cannot describe."""
    fields = []
    for column in sorted(columns, key=lambda c: c.ordinal):
        try:
            fields.append(column_to_arrow_field(column))
        except (UnsupportedTypeError, ValidationError) as e:
            raise UnsupportedColumnError(
                f"column {column.name!r} has no Arrow form this writer can build: {e}"
            ) from e
    return pa.schema(fields)


def _build_type(t: pa.DataType) -> pa.DataType:
    """The type ``pa.array`` builds a column's coerced Python values at,
    before ONE cast to the catalog type — the single place the leaf
    conversions live, at any depth: ``uuid`` is built as its 16-byte
    storage (pyarrow cannot build an extension value from Python inside
    a container), ``json`` as its string storage, ``timestamptz`` as the
    naive micros it is coerced to (no int → tz kernel). Field names,
    nullability and field-id metadata are kept, so the cast lands on the
    catalog type exactly. A type that needs none of this is returned as
    is (equal, and the cast is then skipped)."""
    if isinstance(t, pa.BaseExtensionType):
        return t.storage_type
    if pa.types.is_timestamp(t) and t.tz is not None:
        return pa.timestamp(t.unit)
    if pa.types.is_struct(t):
        fields = [_build_field(t.field(i)) for i in range(t.num_fields)]
        if all(f.type == t.field(i).type for i, f in enumerate(fields)):
            return t
        return pa.struct(fields)
    if pa.types.is_map(t):
        key, item = _build_field(t.key_field), _build_field(t.item_field)
        if key.type == t.key_type and item.type == t.item_type:
            return t
        return pa.map_(key, item, keys_sorted=t.keys_sorted)
    if pa.types.is_list(t):
        value = _build_field(t.value_field)
        if value.type == t.value_type:
            return t
        return pa.list_(value)
    return t


def _build_field(f: pa.Field) -> pa.Field:
    built = _build_type(f.type)
    return f if built == f.type else f.with_type(built)


def _array_for(column: Column, arrow_type: pa.DataType, values: list[Any]) -> pa.Array:
    """One column's Arrow array from coerced values, at exactly the type
    :func:`arrow_schema` gives the column (so ``Table.from_arrays``
    against that schema needs no cast): built at :func:`_build_type` and
    cast once."""
    build = _build_type(arrow_type)
    if build == arrow_type:
        return pa.array(values, type=arrow_type)
    return pa.array(values, type=build).cast(arrow_type)


def _encode_type(t: pa.DataType) -> pa.DataType:
    """``t`` with every ``timestamp[s]`` — at any depth — stored as
    milliseconds (Parquet has no seconds unit)."""
    if t == pa.timestamp("s"):
        return pa.timestamp("ms")
    if pa.types.is_struct(t):
        return pa.struct([_encode_field(t.field(i)) for i in range(t.num_fields)])
    if pa.types.is_map(t):
        return pa.map_(
            _encode_field(t.key_field),
            _encode_field(t.item_field),
            keys_sorted=t.keys_sorted,
        )
    if pa.types.is_list(t):
        return pa.list_(_encode_field(t.value_field))
    return t


def _encode_field(f: pa.Field) -> pa.Field:
    return f.with_type(_encode_type(f.type))


def _map_entries_type(t: pa.DataType) -> pa.DataType:
    """``t`` with every map's entries struct under Arrow's default name.
    A parquet round trip names a map's entries after the COLUMN (pyarrow
    reads ``map<k, v ('m')>`` back for a column ``m`` it wrote as
    ``entries``), and Arrow type equality compares that name; nothing
    else about the type, field ids included, is touched."""
    if pa.types.is_map(t):
        return pa.map_(
            t.key_field.with_type(_map_entries_type(t.key_type)),
            t.item_field.with_type(_map_entries_type(t.item_type)),
            keys_sorted=t.keys_sorted,
        )
    if pa.types.is_struct(t):
        return pa.struct(
            [
                t.field(i).with_type(_map_entries_type(t.field(i).type))
                for i in range(t.num_fields)
            ]
        )
    if pa.types.is_list(t):
        return pa.list_(t.value_field.with_type(_map_entries_type(t.value_type)))
    return t


def file_schema(schema: pa.Schema) -> pa.Schema:
    """A written file's Arrow schema, comparable with the encoding schema
    (:func:`_map_entries_type`)."""
    return pa.schema(
        [f.with_type(_map_entries_type(f.type)) for f in schema],
        metadata=schema.metadata,
    )


def _encode_schema(schema: pa.Schema) -> pa.Schema:
    """The parquet-encoding schema: the catalog schema with field ids,
    plus the one storage conversion pyhoglake's writer path makes —
    Parquet has no seconds timestamp unit, so ``timestamp_s`` is stored
    as milliseconds (mirrors ``Table._prepare_append``, which does it for
    top-level columns; this does it at every depth)."""
    return pa.schema(
        [_encode_field(field) for field in schema], metadata=schema.metadata
    )


def _build_arrow(
    records: Sequence[DecodedRecord], columns: tuple[Column, ...]
) -> pa.Table:
    """Column-aligned records → the Arrow table at the catalog's schema
    (field ids embedded). Rows keep SCAN order here — Kafka offset order
    within the group; :func:`build_prepared_plan` then
    sorts the batch by the live catalog sort spec (payload event time)
    before the partition fanout — the sort, not the scan order, is what
    the ingest-sort / catalog-sort-spec convergence rests on
    (docs/kafka-ingestion.md §What the server owns)."""
    schema = arrow_schema(columns)
    arrays = [
        _array_for(
            column,
            schema.field(column.name).type,
            [r.values[column.name] for r in records],
        )
        for column in columns
    ]
    return pa.Table.from_arrays(arrays, schema=schema)


#: The catalog sort spec's wire vocabulary → Arrow's (pyhoglake
#: ``SortField.direction`` / ``null_order``, spec ``SortField`` schema).
#: Null placement is INDEPENDENT of direction (Iceberg's rule, and the
#: server comparator's — ParquetRewriter), and Arrow takes it per key.
_SORT_DIRECTIONS: Final = {"asc": "ascending", "desc": "descending"}
_SORT_NULL_ORDERS: Final = {"nulls_first": "at_start", "nulls_last": "at_end"}


#: Column types that are no partition or sort source: containers have no
#: single value per row, and a json value has no order or transform this
#: writer (or the server) defines.
_UNORDERED_TYPES: Final = frozenset({"struct", "list", "map", "json"})
_REPEATED_TYPES: Final = frozenset({"list", "map"})


def _source_fault(chain: tuple[Column, ...]) -> str | None:
    """Why a partition/sort source chain cannot be read as one scalar per
    row, or None: a container or json leaf, or a leaf under a
    list or a map (many values per row). A struct leaf is fine — it is
    read by path."""
    leaf = chain[-1]
    path = ".".join(c.name for c in chain)
    if leaf.type in _UNORDERED_TYPES:
        return f"{path!r} is a {leaf.type!r} column"
    repeated = next((c for c in chain[:-1] if c.type in _REPEATED_TYPES), None)
    if repeated is not None:
        return (
            f"{path!r} sits under {repeated.name!r}, a {repeated.type!r}: a row "
            "has many such values"
        )
    return None


def _spec_sort_keys(info: TableInfo) -> list[tuple[tuple[str, ...], str, str]]:
    """The table's LIVE sort spec as Arrow sort keys — ``(column path,
    direction, null placement)`` per field, in spec order (a struct leaf
    is its root-to-leaf name path); ``[]`` for an unsorted table.

    A field that does not resolve to a live column, resolves to one that
    is not one scalar per row (a container, json, or a leaf
    under a list or a map), or carries a direction/null_order outside the
    wire vocabulary, is a shape this writer cannot honor — refused
    LOUDLY (the same posture as the partition-spec guard below), never
    silently unsorted: an unsorted file under a sorted spec is exactly
    what compaction's sortedness check demotes, so "looked odd, skipped
    it" would tax every file forever. Startup validation
    (:meth:`HoglakeFlusher.resolve`) refuses the same shapes fail-fast;
    this guard covers a spec that changed under a warm shape cache.
    """
    spec = info.sort_spec
    if spec is None or not spec.fields:
        return []
    chains = column_chains(info.columns)
    keys: list[tuple[tuple[str, ...], str, str]] = []
    for field in spec.fields:
        chain = chains.get(field.source_field_id)
        if chain is None:
            raise FlushError(
                f"sort spec (sort_id={spec.sort_id}) references field_id "
                f"{field.source_field_id}, which is not a live column of "
                f"{info.namespace}.{info.name}"
            )
        fault = _source_fault(chain)
        if fault is not None:
            raise FlushError(
                f"sort spec (sort_id={spec.sort_id}) field "
                f"{field.source_field_id} cannot be a sort source: {fault}"
            )
        direction = _SORT_DIRECTIONS.get(field.direction)
        null_order = _SORT_NULL_ORDERS.get(field.null_order)
        if direction is None or null_order is None:
            raise FlushError(
                f"sort spec (sort_id={spec.sort_id}) field "
                f"{field.source_field_id} carries direction="
                f"{field.direction!r} null_order={field.null_order!r}, "
                "outside the asc/desc × nulls_first/nulls_last vocabulary"
            )
        keys.append((tuple(c.name for c in chain), direction, null_order))
    return keys


def _sort_batch_by_spec(
    data: pa.Table, keys: list[tuple[tuple[str, ...], str, str]]
) -> pa.Table:
    """Sort the encoded batch by the catalog's sort keys — native Arrow
    compute (``pc.sort_indices`` + ``take``; ``take`` preserves the
    schema's field-id metadata).

    The keys are computed into a SHADOW table, one temporary column per
    key, and only the permutation is applied to ``data`` (so nothing is
    added to or dropped from the batch). A struct-leaf key is the leaf
    read by path (``pc.struct_field``; a null struct on the path is a
    null key). Extension-typed keys (``uuid``) have no Arrow sort
    kernel, so they sort on their STORAGE — fixed-size binary compares
    unsigned-lexicographic in Arrow exactly as the server comparator
    reads the same physical bytes (ParquetRewriter's
    BINARY/FIXED_LEN_BYTE_ARRAY arm), so the orders agree. NaN cannot
    appear (the decoder refuses non-finite floats), so Arrow's NaN
    placement never diverges from the server's NaN-greatest rule here.
    """
    names: list[str] = []
    arrays: list[pa.ChunkedArray] = []
    sort_keys: list[tuple[str, str, str]] = []
    for i, (path, direction, null_order) in enumerate(keys):
        column: pa.ChunkedArray = data.column(path[0])
        for step in path[1:]:
            column = pc.struct_field(column, step)
        if isinstance(column.type, pa.BaseExtensionType):
            column = pa.chunked_array(
                [chunk.storage for chunk in column.chunks],
                type=column.type.storage_type,
            )
        name = f"__millrace_sort_{i}"
        names.append(name)
        arrays.append(column)
        sort_keys.append((name, direction, null_order))
    shadow = pa.table(dict(zip(names, arrays, strict=True)))
    return data.take(pc.sort_indices(shadow, sort_keys=sort_keys))


#: What the Arrow batch build (array construction plus the parquet-encoding
#: cast) can raise on a single bad VALUE. Coercion is total — every bad
#: value is a per-record :class:`DecodeError` BEFORE the build — so a
#: failure here is encoder drift (a coercion that under-refuses), and the
#: build isolates the guilty record rather than losing the batch.
_BUILD_FAILURES: Final = (pa.ArrowException, OverflowError)


def _build_batch(
    records: Sequence[DecodedRecord], columns: tuple[Column, ...]
) -> tuple[pa.Table, pa.Table]:
    """The batch's Arrow table and its parquet-encoding cast (the two
    build steps a value can still fail)."""
    table = _build_arrow(records, columns)
    return table, table.cast(_encode_schema(table.schema))


def _first_unbuildable(
    records: Sequence[DecodedRecord], columns: tuple[Column, ...]
) -> int | None:
    """The index of the first record that fails the batch build ON ITS
    OWN, or None. Arrow array builds convert per value, so a record that
    fails alone is exactly the record that fails the batch. This probe
    only ever runs on the failure path — never in the steady state."""
    for index, record in enumerate(records):
        try:
            _build_batch([record], columns)
        except _BUILD_FAILURES:
            return index
    return None


def _build_batch_isolating(
    good: list[tuple[keyspace.RowKey, bytes, DecodedRecord]],
    columns: tuple[Column, ...],
) -> tuple[pa.Table, pa.Table, list[tuple[keyspace.RowKey, bytes, DecodedRecord]]]:
    """The batch's ``(table, encoded)`` pair — with per-record failure
    routing even at the Arrow boundary: a record whose values defeat the
    build is popped from ``good`` and returned in the guilty list (scan
    order), never taking the batch down with it.

    Coercion is total, so this loop is the belt for encoder drift, not
    the hot path: it costs nothing when the build succeeds. A batch
    failure no single record reproduces is a genuine encoder bug and
    raises (loud, not wedged).
    """
    guilty: list[tuple[keyspace.RowKey, bytes, DecodedRecord]] = []
    while True:
        try:
            table, encoded = _build_batch([r for _, _, r in good], columns)
            return table, encoded, guilty
        except _BUILD_FAILURES as e:
            index = _first_unbuildable([r for _, _, r in good], columns)
            if index is None:
                raise FlushError(
                    "the Arrow batch build failed but every record builds "
                    "alone — a millrace encoder bug"
                ) from e
            guilty.append(good.pop(index))


def _partition_groups(
    data: pa.Table, info: TableInfo, spec: PartitionSpec
) -> list[tuple[tuple[str | None, ...], pa.Table]]:
    """Split the batch by partition tuple under the table's live spec:
    one (wire-string tuple, sub-table) per distinct tuple, ordered by
    first occurrence (so file registration — and the server's
    rows-then-offset row-id assignment — follows scan order).

    The transform math is pyhoglake's (Iceberg semantics have exactly one
    implementation); the grouping is millrace's — pyhoglake's own
    ``_partition_groups`` and millpond's are the same shape, kept local
    because the prepared path puts row-to-partition correctness on the
    caller. A null source value forms its own group, per Iceberg; a
    struct-leaf source under a null struct is a null source value.
    """
    chains = column_chains(info.columns)
    key_names = [f"__millrace_pk_{i}" for i in range(len(spec.fields))]
    key_arrays: list[pa.Array] = []
    for field in spec.fields:
        chain = chains.get(field.source_field_id)
        if chain is None:  # startup validation refuses this; re-refuse loudly
            raise FlushError(
                f"partition spec (spec_id={spec.spec_id}) references field_id "
                f"{field.source_field_id}, which is not a live column of "
                f"{info.namespace}.{info.name}"
            )
        leaf = chain[-1]
        # A struct leaf is read by path (pyhoglake's partition_source_array
        # walks it with struct_field and refuses a container or a source
        # under a list/map itself).
        key_arrays.append(
            transform_strings(
                field.transform,
                field.transform_param,
                partition_source_array(data, list(chain)),
                leaf.type,
                leaf.type_params,
            )
        )
    keyed = pa.table(
        {
            **dict(zip(key_names, key_arrays, strict=True)),
            "__millrace_row": pa.array(range(data.num_rows), pa.int64()),
        }
    )
    combos = (
        keyed.group_by(key_names)
        .aggregate([("__millrace_row", "min")])
        .sort_by("__millrace_row_min")
    )
    out: list[tuple[tuple[str | None, ...], pa.Table]] = []
    for i in range(combos.num_rows):
        values = tuple(combos.column(k)[i].as_py() for k in key_names)
        mask = None
        for name, value in zip(key_names, values, strict=True):
            key_col = keyed.column(name)
            field_mask = (
                pc.is_null(key_col)
                if value is None
                else pc.fill_null(pc.equal(key_col, value), False)
            )
            mask = field_mask if mask is None else pc.and_(mask, field_mask)
        out.append((values, data.filter(mask)))
    return out


# -- deterministic commit message -------------------------------------------------


def format_commit_message(
    *,
    table_uuid: str,
    group: str,
    trigger: FlushTrigger,
    records: int,
    dropped: int,
    files: int,
    partitions: int,
    staged_bytes: int,
    arrow_bytes: int,
    topic: str,
    partition: int,
    first_offset: int,
    last_offset: int,
    version: str,
) -> str:
    """The snapshot's ``message``: what the flush was, then where it came
    from in Kafka.

    DETERMINISTIC for a given flush: fixed ``key=value`` sequence in a
    fixed order, no wall-clock, no randomness, no per-attempt state — the
    same staged range flushed in any process produces the same text.
    ``table=`` carries the destination INCARNATION for the same reason it
    is in the idempotency key: a drop+recreate under one name makes two
    tables that receipts do not span.

    Line 2 is the flush's identity material in machine-readable form:
    the offset window ``first-last``. Recovery parses it back out of a
    persisted request's message (``parse_message_offsets``) — the
    ``prepared/`` value envelope holds only the request body, so the
    message is where the window lives.
    """
    summary = (
        f"millrace=v1 table={table_uuid} group={group} trigger={trigger} "
        f"records={records} dropped={dropped} files={files} "
        f"partitions={partitions} staged_bytes={staged_bytes} "
        f"arrow_bytes={arrow_bytes} millrace={_message_token(version)}"
    )
    return (
        f"{summary}\noffsets topic={_message_token(topic)} "
        f"partition={partition} window={first_offset}-{last_offset}"
    )


def _message_token(value: Any) -> str:
    """One ``key=value`` value, guaranteed to hold no space (millpond's
    rule: a space inside a value moves every pair after it for anything
    reading the line by splitting on whitespace)."""
    text = "".join("_" if c.isspace() else c for c in str(value))
    return text or "unknown"


_OFFSETS_LINE: Final = re.compile(
    r"^offsets topic=(?P<topic>\S+) partition=(?P<partition>\d+) "
    r"window=(?P<first>\d+)-(?P<last>\d+)$"
)


def parse_message_offsets(
    message: str, *, topic: str, partition: int
) -> tuple[int, int]:
    """The flush window ``(first_offset, last_offset)`` back out of a
    persisted request's commit message.

    Strict by construction: the format above is millrace's own, and a
    persisted body whose message does not parse is corruption (foreign
    writes or a bug), refused loudly rather than guessed at. Staged
    offsets are non-negative (the consumer drops anything else), which
    is what makes the ``first-last`` spelling unambiguous."""
    for line in message.splitlines():
        match = _OFFSETS_LINE.match(line)
        if match is None:
            continue
        if (
            match.group("topic") != _message_token(topic)
            or int(match.group("partition")) != partition
        ):
            raise StageCorruptionError(
                f"prepared request's offsets line names "
                f"{match.group('topic')}[{match.group('partition')}], "
                f"not this stage's {topic}[{partition}]"
            )
        first, last = int(match.group("first")), int(match.group("last"))
        if first > last:
            raise StageCorruptionError(
                f"persisted commit message names an inverted window {first}-{last}"
            )
        return first, last
    raise StageCorruptionError(
        "persisted prepared request has no offsets line in its commit message"
    )


# -- the pure request builder -----------------------------------------------------

#: What one flush becomes on the wire.
FlushOutcome = Literal[
    "committed",
    "replayed",
    "receipt_settled",
    "deduped",
    "reprepare",
    "dropped",
    "empty",
]


@dataclass(frozen=True, slots=True)
class FlushContext:
    """The per-decision constants one prepared request is built from.

    ``first_offset`` / ``last_offset`` are the flushed window (the
    message's identity material). Nothing here reads a clock: the junk window arrives precomputed in
    :func:`build_prepared_plan`'s arguments, so the request's bytes are a
    pure function of the staged rows, the table basis and the window.
    """

    topic: str
    partition: int
    group: keyspace.PartitionTuple
    group_fields: tuple[str, ...]
    trigger: FlushTrigger
    first_offset: int
    last_offset: int
    idempotency_key: str
    read_snapshot: int
    table_uuid: str
    #: The per-build object-name token (``…/{key}/{build}/{index}.parquet``),
    #: minted fresh by the flusher for every build so a rebuild of the same
    #: window never reuses an earlier build's object names.
    build: str

    def __post_init__(self) -> None:
        if not self.build or "/" in self.build:
            raise ValueError(
                f"build token must be a non-empty path segment: {self.build!r}"
            )


@dataclass(frozen=True, slots=True)
class PreparedPlan:
    """The pure half of a flush: the complete commit request and the
    parquet it registers, built without a single I/O.

    ``payload`` is None exactly when every scanned row was dropped
    (nothing may be committed; the decision settles as dropped).
    ``uploads`` are the buffers to write; ``uris`` names every one of
    them in input order for orphan accounting. ``dropped_records`` are
    the rows left out of the payload (scan order) — counted and logged by
    the caller, deleted by the window's ordinary settlement."""

    payload: dict[str, Any] | None
    uploads: tuple[Upload, ...]
    uris: tuple[str, ...]
    dropped_records: tuple[DroppedRecord, ...]
    records: int
    files: int
    partition_tuples: int
    staged_bytes: int
    arrow_bytes: int
    dropped_keys: tuple[str, ...]


def _footer_size(raw: bytes) -> int:
    """The thrift footer-metadata length for the commit's ``footer_size``.

    Wire convention (pyhoglake's ``_footer_size`` / bugs.md #7): EXACTLY
    the 4-byte LE length stored in the parquet trailer — the serialized
    thrift FileMetaData size, EXCLUDING the trailing 8-byte suffix
    (4-byte length + ``PAR1`` magic). The server's hydrator tail-reads
    ``[file_size - footer_size - 8, file_size)``.
    """
    (meta_len,) = struct.unpack("<I", raw[-8:-4])
    return int(meta_len)


def build_prepared_plan(
    records: Sequence[tuple[keyspace.RowKey, bytes]],
    info: TableInfo,
    *,
    ctx: FlushContext,
    event_time_policy: EventTimePolicy,
    event_time_window_us: tuple[int, int],
    data_path: str,
    max_files_per_commit: int,
    footer_stats_min_bytes: int,
    version: str,
) -> PreparedPlan:
    """Decode → junk policy → Arrow → sort by the live sort spec →
    fanout → parquet → the complete commit request. PURE: no I/O, no
    clocks, no randomness — the same inputs (``ctx.build`` included)
    produce byte-identical output in any process.

    Object names: ``{data_path}/data/{ns}/{table}/{idempotency_key}/
    {build}/{index}.parquet``. The key derives from ``(table_uuid, topic,
    partition, partition tuple, first_offset, last_offset)`` — two
    partitions flushing one tuple over the same offset range name
    DIFFERENT objects (offsets are per partition; PR #331 review) — and
    ``ctx.build`` is the caller's per-build token, so a rebuild of the
    same window never reuses an earlier build's names (it may be live
    data of a landed first attempt).

    The sort: scan order is Kafka offset order; the catalog's sort spec
    (``info.sort_spec``) is payload-event time. The batch is sorted by
    the spec — fields, directions and null placement — right before
    :func:`_partition_groups`, so every registered file is
    key-nondecreasing under the spec compaction enforces.

    Stats policy is per FILE on the realized parquet size
    (``footer_stats_min_bytes``): big enough that stats pruning pays →
    footer stats ride the commit; smaller → ``stats_mode=deferred`` and
    the hydrator backfills. Realized size is a deterministic function of
    the rows, so the decision replays byte-identically (m10: keying on
    the trigger instead inverts exactly on the big catch-up flushes).
    """
    decoder = JsonObjectDecoder(info.columns)
    window = JunkWindow(*event_time_window_us)
    temporal = _temporal_sources(info)

    staged_bytes = sum(len(payload) for _, payload in records)
    good: list[tuple[keyspace.RowKey, bytes, DecodedRecord]] = []
    dropped_records: list[DroppedRecord] = []
    dropped_keys: set[str] = set()

    def drop(row_key: keyspace.RowKey, reason: str, path: str | None = None) -> None:
        dropped_records.append(DroppedRecord(row_key.offset, reason, path))

    for row_key, payload_bytes in records:
        try:
            record = decoder.decode_record(payload_bytes)
        except DecodeError as e:
            drop(row_key, e.reason, e.path)
            continue
        dropped_keys.update(record.dropped_keys)
        junked = False
        for _field, path, col_type in temporal:
            value = _leaf_value(record.values, path)
            if value is None:
                continue
            value_us = _value_to_micros(col_type, value)
            if window.contains(value_us):
                continue
            if event_time_policy is EventTimePolicy.DROP:
                drop(row_key, REASON_EVENT_TIME_OUT_OF_WINDOW)
                junked = True
                break
            # clamp mode: rewrite the DECODED value to the window edge —
            # the staged row is never mutated.
            clamped = min(max(value_us, window.min_us), window.max_us)
            _set_leaf(record.values, path, _micros_to_value(col_type, clamped))
        if junked:
            continue
        good.append((row_key, payload_bytes, record))

    # The batch build. Coercion is total — every per-record failure above
    # is a DecodeError and was dropped — so a failure HERE is encoder
    # drift (a coercion that under-refuses). Isolate the guilty record and
    # drop it rather than losing the whole batch to one value (one
    # malformed record must never wedge the flush).
    table, encoded, build_guilty = _build_batch_isolating(good, decoder.columns)
    for bad_key, _bad_payload, _bad_record in build_guilty:
        drop(bad_key, REASON_UNCASTABLE_COLUMN)

    if not good:
        return PreparedPlan(
            payload=None,
            uploads=(),
            uris=(),
            dropped_records=tuple(dropped_records),
            records=0,
            files=0,
            partition_tuples=0,
            staged_bytes=staged_bytes,
            arrow_bytes=0,
            dropped_keys=tuple(sorted(dropped_keys)),
        )

    # The catalog's sort spec is the file's contract (BINDING for
    # compaction; SortednessCheck demotes a file at the first inversion).
    # Scan order is Kafka offset order and the spec is payload-event time,
    # which late/skewed events invert constantly — so the batch is sorted
    # by the live spec HERE, before the fanout (a group filter preserves
    # row order, so every output file is then key-nondecreasing).
    sort_keys = _spec_sort_keys(info)
    if sort_keys:
        encoded = _sort_batch_by_spec(encoded, sort_keys)

    encode_schema = encoded.schema

    spec = info.partition_spec
    groups: list[tuple[tuple[str | None, ...] | None, pa.Table]]
    if spec is not None and spec.fields:
        groups = list(_partition_groups(encoded, info, spec))
    else:
        groups = [(None, encoded)]
    if len(groups) > max_files_per_commit:
        # A deployment bug, not data junk: the destination's spec fans one
        # decision out past the commit bound (the event-time window bounds
        # temporal fanout; identity on a high-cardinality column is the
        # way past it). Halt loudly rather than emit an unbounded commit.
        raise FlushHalted(
            "fanout_exceeds_commit_bound",
            f"one flush decision fans out to {len(groups)} files, above "
            f"max_files_per_commit={max_files_per_commit}: the destination "
            "spec and the commit bound cannot both be honored",
        )

    # The message is formatted HERE, inside the pure builder, before any
    # upload exists — a formatting failure can never leave an uncounted
    # orphan (millpond's rule, pinned by a source-order test).
    message = format_commit_message(
        table_uuid=ctx.table_uuid,
        group=keyspace.group_label(ctx.group_fields, ctx.group),
        trigger=ctx.trigger,
        records=sum(part.num_rows for _, part in groups),
        dropped=len(dropped_records),
        files=len(groups),
        partitions=sum(1 for values, _ in groups if values is not None),
        staged_bytes=staged_bytes,
        arrow_bytes=table.nbytes,
        topic=ctx.topic,
        partition=ctx.partition,
        first_offset=ctx.first_offset,
        last_offset=ctx.last_offset,
        version=version,
    )

    # Stats asymmetry, per file on its REALIZED size (m10): at or above
    # the footer-stats threshold the file ships full footer stats (big
    # files are where pruning pays — a catch-up flush on the age/slow
    # lane included); below it the file registers deferred (no
    # ``column_stats`` key — the file registers as `pending` and the
    # hydrator backfills off the commit path).
    registrations: list[dict[str, Any]] = []
    uploads: list[Upload] = []
    uris: list[str] = []
    base = f"{data_path.rstrip('/')}/data/{info.namespace}/{info.name}"
    for index, (partition_values, part) in enumerate(groups):
        sink = pa.BufferOutputStream()
        pq.write_table(part, sink)
        raw = sink.getvalue()
        with pq.ParquetFile(pa.BufferReader(raw)) as parquet:
            if not prepared_schema_matches(
                file_schema(parquet.schema_arrow), encode_schema
            ):
                # Unreachable while the decoder and the schema builder
                # agree; a failure here is a bug in THIS process, so the
                # decision raises (contained, loud) rather than drops.
                raise FlushError(
                    "encoded parquet schema/field ids differ from the "
                    "destination schema — a millrace encoder bug"
                )
            metadata = parquet.metadata
        uri = f"{base}/{ctx.idempotency_key}/{ctx.build}/{index}.parquet"
        registration: dict[str, Any] = {
            "path": uri,
            "record_count": metadata.num_rows,
            "file_size_bytes": raw.size,
            "footer_size": _footer_size(bytes(raw[-8:])),
        }
        if raw.size >= footer_stats_min_bytes:
            registration["column_stats"] = [
                stat.to_wire() for stat in extract_column_stats(metadata, info.columns)
            ]
        if partition_values is not None:
            registration["partition_values"] = list(partition_values)
        registrations.append(registration)
        uploads.append(Upload(uri=uri, size=raw.size, body=raw))
        uris.append(uri)

    payload: dict[str, Any] = {
        "idempotency_key": ctx.idempotency_key,
        "read_snapshot": ctx.read_snapshot,
        "appends": [
            {
                "namespace": info.namespace,
                "table": info.name,
                "expected_table_uuid": ctx.table_uuid,
                "files": registrations,
            }
        ],
        "author": f"millrace/{ctx.topic}/{ctx.partition}",
        "message": message,
    }
    return PreparedPlan(
        payload=payload,
        uploads=tuple(uploads),
        uris=tuple(uris),
        dropped_records=tuple(dropped_records),
        records=sum(part.num_rows for _, part in groups),
        files=len(groups),
        partition_tuples=sum(1 for values, _ in groups if values is not None),
        staged_bytes=staged_bytes,
        arrow_bytes=table.nbytes,
        dropped_keys=tuple(sorted(dropped_keys)),
    )


def canonical_body(payload: Mapping[str, Any]) -> bytes:
    """The persisted form of a prepared request: strict JSON in the
    payload's construction order, with httpx's exact encoding
    (``ensure_ascii=False``, compact separators, ``allow_nan=False``), so
    the bytes on the wire ARE the persisted bytes — first publish and
    replay alike."""
    return json.dumps(
        payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


@dataclass(frozen=True, slots=True)
class FlushReport:
    """The outcome of one decision's flush (or replay). ``records`` are
    the rows THIS process published — 0 for a receipt settlement (some
    earlier process published them) and for drops. ``dropped`` counts
    the window's rows left out of the publication."""

    topic: str
    partition: int
    group: keyspace.PartitionTuple
    trigger: FlushTrigger | Literal["recovery"]
    outcome: FlushOutcome
    first_offset: int
    last_offset: int
    records: int = 0
    files: int = 0
    dropped: int = 0
    snapshot_id: int | None = None
    idempotency_key: str | None = None


# -- the hoglake-facing flusher ---------------------------------------------------


def _seg(name: object) -> str:
    """Percent-encode one URL path segment (pyhoglake's ``_seg`` rule:
    identifiers are user data and must stay inside their segment)."""
    return quote(str(name), safe="")


def _error_text(exc: HoglakeError) -> str:
    """Everything the server said, lower-cased — BOTH halves (millpond's
    rule: hoglake's body is ``{error, detail}`` and for a 422 the
    sentence is in the detail; a matcher that reads only ``message``
    matches nothing on a real response)."""
    return f"{exc.message} {exc.detail or ''}".lower()


def _payload_uris(payload: Mapping[str, Any]) -> tuple[str, ...]:
    """Every registered file's path in a persisted payload (orphan
    accounting on the replay path, where no plan survives)."""
    try:
        files = payload["appends"][0]["files"]
        return tuple(str(f["path"]) for f in files)
    except (KeyError, IndexError, TypeError):
        return ()


def _payload_records(payload: Mapping[str, Any]) -> int:
    try:
        files = payload["appends"][0]["files"]
        return sum(int(f["record_count"]) for f in files)
    except (KeyError, IndexError, TypeError, ValueError):
        return 0


def _shape_fingerprint(info: TableInfo) -> tuple[Any, ...]:
    """What the compression-ratio observations are conditioned on: the
    incarnation plus the column/type/spec shape — the PARTITION spec and
    the SORT spec (a sort-order change rearranges every row's neighbors,
    so realized compression under the old order is a different quantity).
    A change (schema evolution, a spec change, a recreate) resets the
    EWMA on a fingerprint change (:meth:`HoglakeFlusher._shape_sync`)."""
    spec = info.partition_spec
    sort = info.sort_spec
    return (
        info.table_uuid,
        tuple(
            (
                c.name,
                c.type,
                c.nullable,
                tuple(sorted((c.type_params or {}).items())),
            )
            for c in info.columns
        ),
        None if spec is None else (spec.spec_id, len(spec.fields)),
        None
        if sort is None
        else (
            sort.sort_id,
            tuple((f.source_field_id, f.direction, f.null_order) for f in sort.fields),
        ),
    )


def read_destination(client: HoglakeClient, cfg: Config) -> tuple[Catalog, TableInfo]:
    """The startup identity read: catalog → namespace → table, the table
    read ``totals=false`` (the ``Table`` handle is built straight from it
    rather than through ``Namespace.table()``, which issues a plain GET).
    A server that does not report ``read_snapshot_id`` is refused: without
    it there is no conflict basis worth sending. Raises
    :class:`FlushStartupError`."""
    try:
        catalog = client.catalog(cfg.catalog)
        # Existence-check the namespace (the API has no per-namespace
        # GET; the listing is the check).
        catalog.namespace(cfg.namespace)
        request = getattr(client, "_request", None)
        if request is None:
            raise FlushStartupError(
                "pyhoglake no longer exposes HoglakeClient._request; "
                "millrace's identity reads and receipt lookups cannot run"
            )
        body = request(
            "GET",
            f"/catalogs/{_seg(cfg.catalog)}/namespaces/{_seg(cfg.namespace)}"
            f"/tables/{_seg(cfg.table)}",
            params={"totals": "false"},
        )
        info = TableInfo.from_wire(body)
    except FlushStartupError:
        raise
    except (HoglakeError, httpx.HTTPError, OSError) as e:
        raise FlushStartupError(
            f"cannot resolve the hoglake destination "
            f"{cfg.catalog}/{cfg.namespace}/{cfg.table}: {e}"
        ) from e
    if info.read_snapshot_id is None:
        raise FlushStartupError(
            f"the hoglake server did not report read_snapshot_id for "
            f"{cfg.namespace}.{cfg.table}: millrace requires a server "
            "whose table reads carry the snapshot they resolved at "
            "(the commit's conflict basis)"
        )
    return catalog, info


def retention_ttl_s(catalog: Catalog) -> float:
    """A QUARTER of the catalog's snapshot retention — the destination
    shape's re-read cadence (:meth:`HoglakeFlusher._retention_ttl_s`
    carries the argument), with pyhoglake's assumed retention (so 450 s)
    when the catalog will not say; ``inf`` when retention is disabled."""
    read = getattr(catalog, "_retention_seconds", None)
    if read is None:
        return _ASSUMED_RETENTION_S / 4
    try:
        retention = read()
    except Exception:  # noqa: BLE001 - a retention read must never fail a flush
        return _ASSUMED_RETENTION_S / 4
    if (
        not isinstance(retention, (int, float))
        or isinstance(retention, bool)
        or retention <= 0
    ):
        return _ASSUMED_RETENTION_S / 4
    return float(retention) / 4


def _group_fields(info: TableInfo, group: keyspace.PartitionTuple) -> tuple[str, ...]:
    """hoglake's display names for a staged tuple's fields, from the live
    spec (:func:`millrace.grouping.field_names`). A tuple keyed under a
    spec of another arity (a spec change while it was staged) gets
    positional names ``key_<i>`` — the label is identity material, not a
    claim about the live spec."""
    names = field_names(info)
    if len(names) == len(group):
        return names
    return tuple(f"key_{i}" for i in range(len(group)))


class ShapeReader:
    """The destination's shape for the CONSUMER's keyer refresh
    (``consumer.ShapeSource``): its own ``Table`` handle over the
    flusher's resolved catalog, pinned to the flusher's incarnation.
    :meth:`read` is a ``totals=false`` identity read that refuses a
    different ``table_uuid`` (``table_recreated``) or a missing
    ``read_snapshot_id`` (``server_capability``) the way the flusher's
    shape cache does; :meth:`ttl_s` is the flusher's cadence
    (:func:`retention_ttl_s`). Both block (pyhoglake I/O)."""

    def __init__(self, catalog: Catalog, namespace: str, info: TableInfo) -> None:
        self._catalog = catalog
        self._table = Table(Namespace(catalog, namespace), info)
        self._table_uuid = info.table_uuid

    def read(self) -> TableInfo:
        info = self._table.info(totals=False)
        if info.table_uuid != self._table_uuid:
            raise FlushHalted(
                "table_recreated",
                f"table {info.namespace}.{info.name} now resolves to "
                f"{info.table_uuid}, not the pinned incarnation {self._table_uuid}",
            )
        if info.read_snapshot_id is None:
            raise FlushHalted(
                "server_capability",
                f"the server stopped reporting read_snapshot_id for "
                f"{info.namespace}.{info.name}",
            )
        return info

    def ttl_s(self) -> float:
        return retention_ttl_s(self._catalog)


class HoglakeFlusher:
    """Owns the hoglake side of the flush path for one pipeline: the
    pyhoglake client, the resolved destination and its pinned
    incarnation, the table-shape cache (TTL = catalog retention / 4), the
    retry ladders, orphan accounting and the flush counters.

    Built by :meth:`resolve` (blocking startup I/O — call it before the
    loop starts, or through an executor). Driven by :class:`FlushRunner`
    (or directly): :meth:`flush_key` for one planner decision,
    :meth:`reconcile_prepared` for a recovery survivor. The lifecycle is
    caller-owned: the runner does not close the flusher, the way the
    consumer does not close the StageManager.
    """

    @classmethod
    def resolve(
        cls,
        client: HoglakeClient,
        cfg: Config,
        *,
        now_us: Callable[[], int],
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        executor: Executor | None = None,
        io_workers: int = 1,
        build_token: Callable[[], str] = lambda: uuid.uuid4().hex,
    ) -> HoglakeFlusher:
        """Resolve the catalog → namespace → table and validate the
        destination, fail-fast (docs/kafka-ingestion.md §Deployment), and
        PIN the incarnation the read returned: there is no configured
        ``table_uuid`` to refuse against at boot — the pin is what every
        commit sends as ``expected_table_uuid``, and a later read naming
        another uuid halts ``table_recreated``.

        EVERY table read this pipeline makes is ``totals=false``,
        starting here: the ``Table`` handle is constructed straight from
        the identity read rather than through ``Namespace.table()``
        (which issues a plain GET). A server that does not report
        ``read_snapshot_id`` on the read is refused: without it there is
        no conflict basis worth sending, and falling back to a head read
        is the loose shape pyhoglake keeps for legacy servers, not one a
        born-correct writer takes up.
        """
        flusher = cls(
            client,
            cfg,
            now_us=now_us,
            sleep=sleep,
            monotonic=monotonic,
            executor=executor,
            io_workers=io_workers,
            build_token=build_token,
        )
        catalog, info = read_destination(client, cfg)
        flusher._catalog = catalog
        flusher._table = Table(Namespace(catalog, cfg.namespace), info)
        flusher._table_uuid = info.table_uuid
        flusher._shape_info = info
        flusher._shape_read_at = flusher._monotonic()
        flusher._shape_fingerprint = _shape_fingerprint(info)
        flusher._resolved_info = info
        # Startup validation: the decoder supports every column, and the
        # live partition spec is one this writer can honor — each source
        # one scalar per row (a top-level column or a struct leaf; never a
        # container, a json column, or a leaf under a list or a
        # map) with a transform pyhoglake accepts for the source type.
        # `transform_value(..., None)` is the validity dry-run: it checks
        # the vocabulary, the param and the temporal-type gate without
        # fabricating data.
        try:
            JsonObjectDecoder(info.columns)
        except UnsupportedColumnError as e:
            raise FlushStartupError(str(e)) from e
        spec = info.partition_spec
        if spec is not None:
            chains = column_chains(info.columns)
            for field in spec.fields:
                chain = chains.get(field.source_field_id)
                if chain is None:
                    raise FlushStartupError(
                        f"partition spec (spec_id={spec.spec_id}) references "
                        f"field_id {field.source_field_id}, which is not a "
                        f"live column of {info.namespace}.{info.name}"
                    )
                fault = _source_fault(chain)
                if fault is not None:
                    raise FlushStartupError(
                        f"partition spec (spec_id={spec.spec_id}) field "
                        f"{field.source_field_id} cannot be a partition "
                        f"source: {fault}"
                    )
                leaf = chain[-1]
                try:
                    transform_value(
                        field.transform,
                        field.transform_param,
                        leaf.type,
                        None,
                        leaf.type_params,
                    )
                except ValidationError as e:
                    raise FlushStartupError(
                        f"partition spec (spec_id={spec.spec_id}) cannot be "
                        f"honored by this writer: {e}"
                    ) from e
        # The live SORT spec must be one this writer can honor too —
        # build_prepared_plan sorts every batch by it (the ingest file's
        # contract with compaction's sortedness checks). Fail fast here
        # rather than per decision at flush time; _spec_sort_keys
        # re-checks on a warm-cache shape change.
        try:
            _spec_sort_keys(info)
        except FlushError as e:
            raise FlushStartupError(str(e)) from e
        # Retry-After, captured off raw responses (pyhoglake maps statuses
        # to exception classes and drops headers). Best-effort, millpond's
        # posture: if pyhoglake restructures its transport the exponential
        # curve is the fallback, never a construction failure.
        try:
            flusher._client._http.event_hooks["response"].append(flusher._note_response)
        except Exception:  # noqa: BLE001 - optional enhancement, never fatal
            log.debug(
                "could not install the Retry-After hook; 503 backoff falls "
                "back to the exponential curve"
            )
        log.info(
            "millrace destination resolved: %s/%s/%s table_uuid=%s "
            "read_snapshot=%d (%d columns, spec %s)",
            cfg.catalog,
            cfg.namespace,
            cfg.table,
            flusher._table_uuid,
            info.read_snapshot_id,
            len(info.columns),
            "none"
            if spec is None
            else f"id={spec.spec_id} ({len(spec.fields)} fields)",
        )
        return flusher

    def __init__(
        self,
        client: HoglakeClient,
        cfg: Config,
        *,
        now_us: Callable[[], int],
        sleep: Callable[[float], Awaitable[None]],
        monotonic: Callable[[], float],
        executor: Executor | None,
        io_workers: int = 1,
        build_token: Callable[[], str] = lambda: uuid.uuid4().hex,
    ) -> None:
        if io_workers < 1:
            raise ValueError(f"io_workers must be >= 1, got {io_workers}")
        self._client = client
        self._cfg = cfg
        self._now_us = now_us
        self._sleep = sleep
        self._monotonic = monotonic
        self._build_token = build_token
        self._owns_executor = executor is None
        # The blocking pyhoglake I/O pool. Sized to the sweep's
        # process-wide in-flight bound when the runner flushes in
        # parallel (main.py passes 4 × MILLRACE_FLUSH_CONCURRENCY);
        # a smaller pool just queues (no deadlock — _io calls never
        # nest), a larger one idles.
        self._executor = (
            ThreadPoolExecutor(
                max_workers=io_workers, thread_name_prefix="millrace-hoglake"
            )
            if executor is None
            else executor
        )
        self._closed = False
        self._catalog: Catalog  # set by resolve()
        self._table: Table  # set by resolve()
        self._table_uuid: str  # set by resolve()
        self._resolved_info: TableInfo  # set by resolve()
        self._shape_info: TableInfo | None = None
        self._shape_read_at: float | None = None
        self._retry_after: float | None = None
        self._warned_dropped: set[str] = set()
        # Counter attributes, incremented in place (consumer.py's
        # ConsumerStats pattern: plain ints, frozen snapshot on stats()).
        self._flushes_committed = 0
        self._prepared_replays = 0
        self._receipt_settlements = 0
        self._flush_deduped = 0
        self._reprepared: dict[str, int] = {}
        self._decisions_dropped = 0
        self._records_dropped: dict[str, int] = {}
        self._rows_published = 0
        self._files_written = 0
        self._bytes_written = 0
        self._commit_attempts = 0
        self._commit_retries = 0
        self._conflict_retries = 0
        self._queue_retries = 0
        self._receipt_lookups = 0
        self._orphaned_uploads = 0
        self._unresolved_uploads = 0
        self._dropped_payload_keys = 0
        self._stats_repaired = 0
        # The observed compression ratio (module docstring): EWMA fed
        # back into the planner's size lane; resets when the table's
        # shape fingerprint changes.
        self._ratio = CompressionRatioEwma(cfg.compression_ratio_halflife)
        self._shape_fingerprint: tuple[Any, ...] | None = None

    # -- lifecycle -----------------------------------------------------------

    def close(self) -> None:
        """Close the HTTP client and (if owned) the executor. Idempotent."""
        if self._closed:
            return
        self._closed = True
        self._client.close()
        if self._owns_executor:
            self._executor.shutdown(wait=True)

    @property
    def table_uuid(self) -> str:
        """The incarnation pinned at startup resolution."""
        return self._table_uuid

    @property
    def resolved_info(self) -> TableInfo:
        """The destination shape read at startup resolution (the
        consumer's first keyer is built from it)."""
        return self._resolved_info

    def shape_reader(self) -> ShapeReader:
        """A :class:`ShapeReader` over this flusher's resolved catalog and
        pinned incarnation, for the consumer's keyer refresh."""
        return ShapeReader(self._catalog, self._cfg.namespace, self._resolved_info)

    def stats(self) -> FlushStats:
        """A snapshot of the run's counters."""
        return FlushStats(
            flushes_committed=self._flushes_committed,
            prepared_replays=self._prepared_replays,
            receipt_settlements=self._receipt_settlements,
            flush_deduped=self._flush_deduped,
            reprepared=dict(self._reprepared),
            decisions_dropped=self._decisions_dropped,
            records_dropped=dict(self._records_dropped),
            rows_published=self._rows_published,
            files_written=self._files_written,
            bytes_written=self._bytes_written,
            commit_attempts=self._commit_attempts,
            commit_retries=self._commit_retries,
            conflict_retries=self._conflict_retries,
            queue_retries=self._queue_retries,
            receipt_lookups=self._receipt_lookups,
            orphaned_uploads=self._orphaned_uploads,
            unresolved_uploads=self._unresolved_uploads,
            dropped_payload_keys=self._dropped_payload_keys,
            stats_repaired=self._stats_repaired,
        )

    # -- internals -------------------------------------------------------------

    async def _io(self, fn: Callable[[], _R]) -> _R:
        """Run a blocking pyhoglake call on the flusher's executor — the
        asyncio loop this shares with the consumer never waits on hoglake
        I/O."""
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, fn)

    def _note_response(self, response: httpx.Response) -> None:
        """httpx response hook: remember a Retry-After from a response
        that is telling us to back off (429/503 only — a stray header on
        a 200 must not slow the pipeline)."""
        if response.status_code not in _RETRY_AFTER_STATUS:
            return
        raw = response.headers.get("retry-after")
        if raw is None:
            return
        try:
            # Delta-seconds only: the HTTP-date form is legal but hoglake
            # never sends it.
            self._retry_after = float(raw.strip())
        except ValueError:
            log.debug("ignoring non-numeric Retry-After %r", raw)

    def _retry_after_hint(self) -> float | None:
        """The server's own backoff advice for the most recent refusal,
        consumed once — a hint left over from an earlier failure must not
        govern an unrelated retry."""
        hint, self._retry_after = self._retry_after, None
        return hint

    def _count_orphans(self, uris: Sequence[str], why: str, *, reason: str) -> None:
        """Record parquet objects uploaded to the lake that no commit
        references (millpond's ``_count_orphans``, restated).

        ``reason`` is the counter's bounded vocabulary; ``why`` is the
        operator-facing sentence. The uris are NAMED, and naming them is
        the point: sweeping the ``{idempotency_key}/`` prefix is never
        safe (a retry under the same key writes new names beside the old
        ones — pyhoglake's shape, and millrace's too when a rebuild
        changes the fanout). Nothing server-side reclaims a client's
        uploads, so this counter is the whole observability story for
        them. Every build mints fresh object names, so a later rebuild
        never reclaims these: the count is of objects that stay orphaned
        until an operator deletes them."""
        count = len(uris)
        if count <= 0:
            return
        listed = list(uris[:_ORPHAN_URIS_LOGGED])
        omitted = count - len(listed)
        log.warning(
            "orphaned %d uploaded parquet file(s) in the lake: %s. Delete "
            "these objects by name, never the prefix they share: %s%s",
            count,
            why,
            ", ".join(listed),
            f" — and {omitted} more not listed here" if omitted else "",
        )
        self._orphaned_uploads += count

    def _count_reprepare(self, reason: str) -> None:
        """One ``prepared/`` entry dropped for a rebuild from the staged
        rows (``millrace_prepared_reprepared_total{reason}``)."""
        self._reprepared[reason] = self._reprepared.get(reason, 0) + 1

    def _count_unresolved(self, uris: Sequence[str], why: str) -> None:
        """Record objects named by a dropped ``prepared/`` entry whose
        commit may have landed (its receipt may have been purged): they
        may be live table files or orphans, and only the catalog can say
        which. Never orphan-accounted — naming them for deletion could
        delete live data."""
        count = len(uris)
        if count <= 0:
            return
        listed = list(uris[:_ORPHAN_URIS_LOGGED])
        omitted = count - len(listed)
        log.warning(
            "%d uploaded parquet file(s) may be live (a landed commit whose "
            "receipt was purged) or orphaned; reconcile against the catalog "
            "before deleting: %s. Objects: %s%s",
            count,
            why,
            ", ".join(listed),
            f" — and {omitted} more not listed here" if omitted else "",
        )
        self._unresolved_uploads += count

    # -- table shape cache ----------------------------------------------------

    def _retention_ttl_s(self) -> float:
        """The shape cache's TTL: a QUARTER of the catalog's snapshot
        retention (millpond's ``_live_info_ttl_s``, verbatim).

        The number is half of pyhoglake's own writer-cache threshold, and
        the factor of two is the only thing standing between this writer
        and silently mis-partitioned files: pyhoglake's ``Table._cache``
        refreshes at retention/2 and the ``read_snapshot`` moves forward
        with it, so a spec change older than that is outside the commit's
        conflict window with nothing on the file to contradict it.
        Re-reading at retention/4 keeps this cache at least as fresh as
        pyhoglake's — and because every refresh here goes through
        ``Table.info(totals=False)``, which re-seeds pyhoglake's cache in
        the same call, pyhoglake's cache is always sink-seeded and never
        independently refreshed. THE INVARIANT: THIS CACHE IS NEVER
        STALER THAN ``Table._cache``.

        Retention comes from the same pyhoglake accessor pyhoglake's own
        threshold uses, so the two cannot disagree about the period even
        when an operator shortens retention live (guarded private
        accessor, millpond's posture; the fallback is pyhoglake's own
        assumed retention, keeping the factor of two intact). ``inf``
        (retention disabled) correctly means never re-read for staleness.
        """
        return retention_ttl_s(self._catalog)

    def _drop_shape(self) -> None:
        """Forget the destination's cached shape so the next prepare
        re-reads. Called on ANY HoglakeError out of the commit call —
        millpond's rule: any event that can drop pyhoglake's cache drops
        ours. pyhoglake invalidates its own cache on the ``re_prepare``
        family and on 410 (we pass ``table=``); over-dropping on a plain
        ``commit_conflict`` costs one table read on the rebuild that
        refusal already forces."""
        self._shape_info = None
        self._shape_read_at = None

    def _shape_sync(self) -> TableInfo:
        """The destination's live shape: cached, re-read
        (``totals=false``) only on a cold cache or past the TTL. ZERO
        table reads per steady-state flush is the point; the commit's
        conflict window plus the TTL is what makes it safe (see
        ``_retention_ttl_s``)."""
        if (
            self._shape_info is not None
            and self._shape_read_at is not None
            and self._monotonic() - self._shape_read_at < self._retention_ttl_s()
        ):
            return self._shape_info
        info = self._table.info(totals=False)
        if info.table_uuid != self._table_uuid:
            raise FlushHalted(
                "table_recreated",
                f"table {self._cfg.namespace}.{self._cfg.table} was "
                f"recreated: pinned incarnation {self._table_uuid}, the name "
                f"now resolves to {info.table_uuid}. The pipeline halts "
                "rather than writing across incarnations.",
            )
        if info.read_snapshot_id is None:
            raise FlushHalted(
                "server_capability",
                f"the server stopped reporting read_snapshot_id for "
                f"{self._cfg.namespace}.{self._cfg.table}: without it there "
                "is no conflict basis worth sending",
            )
        self._shape_info = info
        self._shape_read_at = self._monotonic()
        fingerprint = _shape_fingerprint(info)
        if (
            self._shape_fingerprint is not None
            and fingerprint != self._shape_fingerprint
        ):
            # A schema/spec evolution changes what a staged byte
            # compresses to — the old observations are a different
            # quantity. (An INCARNATION change never reaches here: the
            # uuid check above halted first.)
            self._ratio.reset()
        self._shape_fingerprint = fingerprint
        return info

    @property
    def compression_ratio(self) -> float:
        """The planner's ``estimated_compression_ratio`` input: the EWMA
        of observed parquet-bytes ÷ staged-bytes over this table's
        completed flushes (the fallback ratio until the first
        observation). Read by the sweep's planning pass."""
        return self._ratio.estimate

    # -- the publish ladder -----------------------------------------------------

    async def _publish(self, payload: dict[str, Any]) -> CommitResult:
        """Publish the (persisted) request with the retry ladders.

        Retries send the IDENTICAL bytes — the persisted-request
        discipline: a lost or ambiguous response replays the payload
        verbatim. ANSWERED refusals escape to the caller's taxonomy
        (typed re-prepare family, 422, 404): the payload is dead the
        moment the server judges it, and no ladder arm may resend one of
        those.
        """
        attempts = 0
        conflicts = 0
        while True:
            attempts += 1
            self._commit_attempts += 1
            try:
                return await self._io(partial(self._table.commit_prepared, payload))
            # The re-prepare family FIRST (DdlSinceReadSnapshotError
            # subclasses CommitConflictError — the arm order is
            # load-bearing, AGENT.md invariant 12's own warning).
            except (
                DdlSinceReadSnapshotError,
                IncarnationChangedError,
                ReadSnapshotExpiredError,
            ):
                self._drop_shape()
                raise
            except ValidationError:
                self._drop_shape()
                raise
            except NotFoundError:
                self._drop_shape()
                raise
            except CommitConflictError as e:
                # 409 commit_conflict is RETRYABLE (the read set moved) —
                # but not every code it carries is: pyhoglake maps the
                # spec's `table_dropped` 409 (untyped there as of
                # 1.3.11) to this same class, and a dropped destination is
                # a halt, not contention. The CODE is the contract.
                if e.message == "table_dropped":
                    self._drop_shape()
                    raise self._halt(
                        "table_dropped",
                        f"the destination was dropped: {e}. The pipeline "
                        "halts; staged state stays for an operator.",
                        _payload_uris(payload),
                    ) from e
                # The shape cache goes (millpond's over-drop rule); the
                # identical payload is resent — the basis is part of the
                # payload and must not move.
                self._drop_shape()
                conflicts += 1
                if conflicts >= _CONFLICT_MAX_ATTEMPTS:
                    raise FlushPublishExhausted(
                        f"commit_conflict refused {conflicts} times in a row "
                        f"({e}); the persisted request stays for the next "
                        "sweep"
                    ) from e
                self._conflict_retries += 1
                self._commit_retries += 1
                delay = min(_BASE_DELAY_S * (2 ** (conflicts - 1)), _MAX_DELAY_S)
                log.warning(
                    "commit_conflict (attempt %d/%d), resending the identical "
                    "request in %.1fs",
                    conflicts,
                    _CONFLICT_MAX_ATTEMPTS,
                    delay,
                )
                await self._sleep(delay)
            except HoglakeError as e:
                status = e.status_code
                if (
                    status is not None
                    and 400 <= status < 500
                    and status not in (408, 429)
                ):
                    # An answered 4xx the taxonomy does not name: the
                    # payload was judged and cannot change by waiting —
                    # the caller's refusal arms decide.
                    self._drop_shape()
                    raise
                self._drop_shape()
                if attempts >= _PUBLISH_MAX_ATTEMPTS:
                    raise FlushPublishExhausted(
                        f"commit failed {attempts} times ({e}); the persisted "
                        "request stays for the next sweep"
                    ) from e
                if status == 503:
                    self._queue_retries += 1
                self._commit_retries += 1
                delay = max(
                    min(_BASE_DELAY_S * (2 ** (attempts - 1)), _MAX_DELAY_S),
                    self._retry_after_hint() or 0.0,
                )
                log.warning(
                    "commit failed transiently (attempt %d/%d: %s), retrying "
                    "the identical request in %.1fs",
                    attempts,
                    _PUBLISH_MAX_ATTEMPTS,
                    e,
                    delay,
                )
                await self._sleep(delay)
            except (httpx.HTTPError, OSError) as e:
                # Transport-uncertain: the commit may have applied and the
                # answer was lost. NEVER a clean failure — replay the
                # identical bytes; the server's receipt dedupes.
                if attempts >= _PUBLISH_MAX_ATTEMPTS:
                    raise FlushPublishExhausted(
                        f"commit transport failed {attempts} times ({e}); the "
                        "persisted request stays for the next sweep"
                    ) from e
                self._commit_retries += 1
                delay = max(
                    min(_BASE_DELAY_S * (2 ** (attempts - 1)), _MAX_DELAY_S),
                    self._retry_after_hint() or 0.0,
                )
                log.warning(
                    "commit transport failed (attempt %d/%d: %s), retrying "
                    "the identical request in %.1fs",
                    attempts,
                    _PUBLISH_MAX_ATTEMPTS,
                    e,
                    delay,
                )
                await self._sleep(delay)

    def _halt(
        self, reason: str, detail: str, orphan_uris: Sequence[str] = ()
    ) -> FlushHalted:
        """Build the loud stop. The upload a refused/halted commit leaves
        behind is orphan-accounted FIRST — the pipeline halts and the
        objects stay, so the counter and the named keys are the only
        observability they get (millpond's ``_discard_prepared``
        posture)."""
        if orphan_uris:
            self._count_orphans(orphan_uris, detail, reason="halted")
        return FlushHalted(reason, detail)

    # -- receipt reconciliation --------------------------------------------------

    async def _lookup_receipt(self, idempotency_key: str) -> CommitResult | None:
        """The receipt lookup on the publish ladder's transient arm: a
        5xx, 408, 429 or transport failure retries up to
        ``_PUBLISH_MAX_ATTEMPTS`` times with the exponential delay floored
        by ``Retry-After``, one WARNING line per retry; exhaustion raises
        :class:`ReceiptLookupExhausted` (a failed decision, retried next
        sweep, no stack trace). Any other answered 4xx propagates."""
        attempts = 0
        while True:
            attempts += 1
            try:
                return await self._io(partial(self._receipt_sync, idempotency_key))
            except HoglakeError as e:
                status = e.status_code
                if (
                    status is not None
                    and 400 <= status < 500
                    and status not in (408, 429)
                ):
                    raise
                error: Exception = e
            except (httpx.HTTPError, OSError) as e:
                error = e
            if attempts >= _PUBLISH_MAX_ATTEMPTS:
                raise ReceiptLookupExhausted(
                    f"receipt lookup for {idempotency_key} failed {attempts} "
                    f"times ({error}); retried next sweep"
                ) from error
            delay = max(
                min(_BASE_DELAY_S * (2 ** (attempts - 1)), _MAX_DELAY_S),
                self._retry_after_hint() or 0.0,
            )
            log.warning(
                "receipt lookup for %s failed transiently (attempt %d/%d: %s), "
                "retrying in %.1fs",
                idempotency_key,
                attempts,
                _PUBLISH_MAX_ATTEMPTS,
                error,
                delay,
            )
            await self._sleep(delay)

    def _receipt_sync(self, idempotency_key: str) -> CommitResult | None:
        """The durable receipt for one idempotency key, or None (404 —
        the routine state of a commit that never landed, not an error).
        Receipts survive rename, drop and snapshot expiry."""
        self._receipt_lookups += 1
        try:
            body = self._client._request(
                "GET",
                f"/catalogs/{_seg(self._cfg.catalog)}"
                f"/commit/receipts/{_seg(idempotency_key)}",
            )
        except NotFoundError:
            return None
        return CommitResult.from_wire(body)

    async def reconcile_prepared(
        self, stage: PartitionStage, pending: RecoveredPrepared
    ) -> FlushReport:
        """Reconcile one persisted ``prepared/`` survivor: receipt lookup
        first — a 200 settles the range locally (zero republication,
        whatever the entry's age: the receipt is ground truth), a 404
        republishes the persisted bytes verbatim (the idempotency key
        makes the server's receipt dedupe a landing replay into one
        logical commit), whatever the entry's age. A 404 past the
        server's receipt retention is ambiguous with a purged receipt;
        such a replay is normally answered 410 (its read snapshot is
        below the expiry floor) and the window re-prepares from the
        staged rows, which can land it a second time: accepted
        at-least-once behaviour, counted.

        The flush window is parsed out of the persisted request's commit
        message — the ``prepared/`` envelope holds the request body, and
        the message is where the window lives. The envelope names its
        idempotency key's derivation inputs (``topic``, ``partition`` and
        the partition tuple as hoglake partition values): the topic and
        partition must equal this stage's own identity and the tuple the
        one its ``prepared/`` key sits under — an entry that names
        another is foreign state, refused as corruption, never replayed
        under it. The settle deletes EXACTLY the window (never younger
        rows of the same tuple), and the
        delete is guarded, inside the settle transaction, against the
        entry's persisted ``row_count``: a window that now holds a
        different number of rows (one that was not in the published
        commit) rolls the settle back and halts ``settlement_mismatch``
        rather than deleting an unpublished row.
        """
        group = pending.key.group
        key = pending.request.idempotency_key
        body = pending.request.body
        try:
            payload = json.loads(body)
        except (ValueError, UnicodeDecodeError) as e:
            raise StageCorruptionError(
                f"prepared/{group}/{pending.key.first_offset}: the "
                f"persisted body is not valid JSON: {e}"
            ) from e
        if not isinstance(payload, dict) or payload.get("idempotency_key") != key:
            raise StageCorruptionError(
                f"prepared/{group}/{pending.key.first_offset}: the "
                "persisted body does not parse to its own idempotency key"
            )
        # The envelope's derivation inputs must name THIS partition
        # instance — an entry that doesn't is foreign state in this
        # SlateDB, and replaying it would publish under another
        # partition's identity.
        if (
            pending.request.topic != stage.topic
            or pending.request.partition != stage.partition
        ):
            raise StageCorruptionError(
                f"prepared/{group}/{pending.key.first_offset}: the "
                f"envelope names {pending.request.topic}"
                f"[{pending.request.partition}] but this stage is "
                f"{stage.topic}[{stage.partition}] — foreign state, "
                "refused rather than replayed"
            )
        # The envelope names the tuple its key was minted for; it must be
        # the tuple its prepared/ key sits under.
        if pending.request.group != group:
            raise StageCorruptionError(
                f"prepared/{group}/{pending.key.first_offset}: the envelope "
                f"names partition tuple {pending.request.group} — foreign "
                "state, refused rather than replayed"
            )
        fields = pending.request.group_fields
        label = keyspace.group_label(fields, group)
        message = payload.get("message")
        if not isinstance(message, str):
            raise StageCorruptionError(
                f"prepared/{group}/{pending.key.first_offset}: the "
                "persisted request carries no commit message"
            )
        first_offset, last_offset = parse_message_offsets(
            message, topic=stage.topic, partition=stage.partition
        )
        if first_offset != pending.key.first_offset:
            raise StageCorruptionError(
                f"prepared/{group}/{pending.key.first_offset}: the "
                f"message's window starts at {first_offset} instead"
            )
        window = FlushedKey(group, first_offset, last_offset, group_fields=fields)
        # The settlement guard's expected count is the PERSISTED one —
        # the rows the window held when the request was built — never a
        # recount now: a row that entered the window after the scan (a
        # rewind or re-key) was not in the published commit.
        expected = pending.request.row_count

        receipt = await self._lookup_receipt(key)
        if receipt is not None:
            await self._settle(stage, window, expected, recovery=True)
            self._receipt_settlements += 1
            log.info(
                "reconciled %s[%d] group %s offsets %d-%d from the commit "
                "receipt (snapshot %d) — zero republication",
                stage.topic,
                stage.partition,
                label,
                first_offset,
                last_offset,
                receipt.snapshot_id,
            )
            return FlushReport(
                topic=stage.topic,
                partition=stage.partition,
                group=group,
                trigger="recovery",
                outcome="receipt_settled",
                first_offset=first_offset,
                last_offset=last_offset,
                records=0,
                snapshot_id=receipt.snapshot_id,
                idempotency_key=key,
            )

        # No receipt. Republish the persisted bytes VERBATIM, whatever
        # the entry's age: past the server's receipt retention a 404 may
        # be a purged receipt, and the replay (or the 410 re-prepare
        # below) may land the window twice — accepted at-least-once
        # behaviour, counted and logged.
        age_s = max(0, self._now_us() - pending.request.persisted_at) / (
            _MICROS_PER_SECOND
        )
        log.info(
            "no receipt for the persisted commit for %s[%d] group %s offsets "
            "%d-%d (idempotency key %s, persisted %.0fs ago); replaying it",
            stage.topic,
            stage.partition,
            label,
            first_offset,
            last_offset,
            key,
            age_s,
        )
        try:
            result = await self._publish(payload)
        except DdlSinceReadSnapshotError as e:
            # Definitive refusal of THIS request, zero writes — the
            # REBUILD case: the persisted payload is dead, the rows are
            # still staged, the planner re-decides the key and
            # re-prepares against a fresh read. The entry follows a
            # receipt 404, so its first attempt may have landed with the
            # receipt since purged: its objects are UNRESOLVED (possibly
            # live), never orphan-accounted.
            self._count_unresolved(
                _payload_uris(payload),
                f"the persisted commit for {stage.topic}[{stage.partition}] "
                f"group {label} offsets {first_offset}-{last_offset} "
                f"(idempotency key {key}, persisted {age_s:.0f}s ago) was "
                f"refused with ddl_since_read_snapshot ({e}); the rows "
                "re-prepare against a fresh read",
            )
            await stage.drop_prepared(group, first_offset)
            self._count_reprepare(_REPREPARE_DDL)
            return FlushReport(
                topic=stage.topic,
                partition=stage.partition,
                group=group,
                trigger="recovery",
                outcome="reprepare",
                first_offset=first_offset,
                last_offset=last_offset,
                idempotency_key=key,
            )
        except IncarnationChangedError as e:
            raise self._halt(
                "table_recreated",
                f"the destination was recreated (pinned incarnation "
                f"{self._table_uuid}): {e}. The pipeline halts; staged state "
                "stays for an operator.",
                _payload_uris(payload),
            ) from e
        except ReadSnapshotExpiredError as e:
            # The persisted basis sank below the catalog's expiry floor —
            # the expected answer for an entry older than the server's
            # receipt retention, whose first attempt may have landed. Not
            # a halt: drop the entry and let the still-staged rows
            # re-prepare against a fresh read on a later sweep (the
            # ddl_since_read_snapshot mechanics). If the first attempt
            # landed, the rebuild lands the window a second time —
            # accepted at-least-once behaviour, counted. Its objects may
            # be live table files, so they are NOT orphan-accounted.
            self._count_unresolved(
                _payload_uris(payload),
                f"the persisted commit for {stage.topic}[{stage.partition}] "
                f"group {label} offsets {first_offset}-{last_offset} "
                f"(idempotency key {key}, persisted {age_s:.0f}s ago) was "
                f"answered 410 ({e.detail or e}); the rows re-prepare "
                "against a fresh read",
            )
            await stage.drop_prepared(group, first_offset)
            self._count_reprepare(_REPREPARE_EXPIRED)
            return FlushReport(
                topic=stage.topic,
                partition=stage.partition,
                group=group,
                trigger="recovery",
                outcome="reprepare",
                first_offset=first_offset,
                last_offset=last_offset,
                idempotency_key=key,
            )
        except NotFoundError as e:
            raise self._halt(
                "destination_missing",
                f"the destination {self._cfg.catalog}/{self._cfg.namespace}/"
                f"{self._cfg.table} no longer resolves: {e}",
                _payload_uris(payload),
            ) from e
        except HoglakeError as e:
            if isinstance(e, ValidationError) and _REUSED_KEY_MARKER in _error_text(e):
                raise self._halt(
                    "idempotency_key_reused",
                    f"the server holds a receipt under {key} for a DIFFERENT "
                    "request: with deterministic derivation that means a "
                    "foreign writer or broken determinism — halting rather "
                    "than settling rows a stranger may not have published",
                    _payload_uris(payload),
                ) from e
            status = e.status_code
            if status is not None and 400 <= status < 500:
                # Any OTHER answered 4xx (a proxy's 401/403, an ingress
                # 413, a plain 400, any 422):
                # nothing about it says the ROWS can never publish.
                # Halt with the rows staged and the entry persisted —
                # the operator reconciles — rather than truncate-and-delete
                # up to a whole window (PR #331 review).
                raise self._halt(
                    "commit_refused",
                    f"the persisted commit for {stage.topic}"
                    f"[{stage.partition}] group {label} offsets "
                    f"{first_offset}-{last_offset} (idempotency key {key}) "
                    f"was answered {status}: {_error_text(e)}. No answered "
                    "refusal deletes staged rows — they stay staged and "
                    "the pipeline halts for an operator.",
                    _payload_uris(payload),
                ) from e
            raise

        await self._settle(stage, window, expected, recovery=True)
        self._prepared_replays += 1
        log.info(
            "replayed the persisted commit for %s[%d] group %s offsets %d-%d "
            "(snapshot %d, persisted %.0fs ago)",
            stage.topic,
            stage.partition,
            label,
            first_offset,
            last_offset,
            result.snapshot_id,
            age_s,
        )
        return FlushReport(
            topic=stage.topic,
            partition=stage.partition,
            group=group,
            trigger="recovery",
            outcome="replayed",
            first_offset=first_offset,
            last_offset=last_offset,
            records=_payload_records(payload),
            files=len(_payload_uris(payload)),
            snapshot_id=result.snapshot_id,
            idempotency_key=key,
        )

    # -- drops -------------------------------------------------------------------

    def _note_drops(
        self, stage: PartitionStage, label: str, dropped: Sequence[DroppedRecord]
    ) -> None:
        """Count flush-time drops per reason
        (``millrace_records_dropped_total{reason}``) and log them: one
        WARNING per reason per decision, naming the group, the offsets
        (the first :data:`_DROPPED_OFFSETS_LOGGED` of them) and the first
        refusing column's dotted path when the decoder knew it
        (``first_path=properties.inner.id``; never a payload value).
        Counted per build, so at-least-once: a decision rebuilt after an
        upload or persist failure, or re-prepared after
        ``ddl_since_read_snapshot``, counts and logs the same drops again
        (the consumer's tally has the same property)."""
        by_reason: dict[str, list[int]] = {}
        first_path: dict[str, str] = {}
        for entry in dropped:
            by_reason.setdefault(entry.reason, []).append(entry.offset)
            if entry.path is not None:
                # The first refusing column path per reason — catalog
                # names only, never the payload value (the consumer's
                # malformed_partition_source:<path> line, flush side).
                first_path.setdefault(
                    entry.reason, f" first_path={entry.path} (offset {entry.offset})"
                )
        for reason, offsets in sorted(by_reason.items()):
            self._records_dropped[reason] = self._records_dropped.get(reason, 0) + len(
                offsets
            )
            shown = offsets[:_DROPPED_OFFSETS_LOGGED]
            log.warning(
                "flush dropped %d records: %s[%d] group %s reason=%s offsets=%s%s%s",
                len(offsets),
                stage.topic,
                stage.partition,
                label,
                reason,
                ",".join(str(o) for o in shown),
                "" if len(offsets) == len(shown) else ",…",
                first_path.get(reason, ""),
            )

    async def _settle(
        self,
        stage: PartitionStage,
        window: FlushedKey,
        expected: int,
        *,
        recovery: bool = False,
    ) -> FlushCommitReport:
        """Settle the window with the guard INSIDE the settlement
        transaction (``commit_flushed(expected_rows=expected)``): the
        settle must delete EXACTLY the rows the commit published —
        ``expected`` is the scanned count on a fresh flush and the
        ``prepared/`` entry's persisted ``row_count`` on every recovery
        settle (never a recount). A disagreement rolls the transaction
        back and halts ``settlement_mismatch``: nothing was deleted and
        the ``prepared/`` entry (if any) stands. A restart reconciles that
        entry against the same persisted count and halts again until an
        operator resolves it; the structural fix is settling by the
        scanned keys (hoglake#351)."""
        try:
            return await stage.commit_flushed(window, expected_rows=expected)
        except SettlementMismatchError as e:
            where = "at reconcile" if recovery else "after publishing"
            raise FlushHalted(
                "settlement_mismatch",
                f"{e} ({where}). Nothing was settled. The window holds rows "
                "that were not in the published commit (a rewind or re-key "
                "after the scan). Confirm what the published snapshot holds, "
                "then drop the prepared/ entry for this window if one "
                "stands (its rows re-flush as a new window) or re-consume "
                "the window's offsets; with an entry standing, a restart "
                "alone halts here again (hoglake#351: settle by scanned "
                "keys)",
            ) from e

    def _scan_budget_bytes(self) -> int:
        """The per-decision staged-byte scan cap (module docstring
        §Bounded unit of work): ``ceil(target ÷ ratio × headroom)``,
        floored at ``flush_scan_min_bytes`` so toy targets never
        fragment flushes into single-row windows."""
        derived = self._cfg.target_output_bytes / self._ratio.estimate
        return max(
            self._cfg.flush_scan_min_bytes,
            math.ceil(derived * SCAN_BUDGET_HEADROOM),
        )

    # -- one key's flush -----------------------------------------------------------

    async def flush_key(
        self, stage: PartitionStage, decision: FlushDecision
    ) -> FlushReport:
        """Flush one decided key through the wire contract (module
        docstring).

        Order of operations, each load-bearing:

        1. A persisted ``prepared/`` entry for the group — found by a
           PER-GROUP lookup, never keyed by the scan's first offset (a
           rewind that re-stages offsets below the entry's window moves
           that) — is authoritative: it is replayed (receipt lookup
           first), never regenerated, and no new window is built while
           one stands.
        2. The shape read (and with it the commit's ``read_snapshot``
           basis) is pinned BEFORE the staged range is scanned.
        3. The scan is BOUNDED (rows and bytes — M4) and, row keys
           sorting by (group, offset), an exact offset prefix of the
           group's staged rows: the window is ``[first scanned, last
           scanned]``; one commit per ``(group, window)``.
        4. CONTENT KEY, RECEIPT FIRST: the key's inputs include the
           SHA-256 of the scanned rows. Before anything is built or
           uploaded the key is looked up (on the transient retry
           ladder). A 200 means this exact content already landed (a
           rewind re-staged identical rows) — settle locally, nothing
           built, uploaded or published (outcome ``deduped``).
           Different rows under the same bounds have another key and
           publish as a new commit. A 422 reused-key on the publish
           halts ``idempotency_key_reused``.
        5. Settlement strictly after the receipt, guarded INSIDE the
           settle transaction against the scanned count (the persisted
           ``row_count`` on recovery).
        """
        group = decision.group
        for pending in await stage.list_prepared(group):
            # At most one entry per group by construction; reconcile in
            # window order if corruption says otherwise. The entry is
            # authoritative for ITS OWN window (parsed from its
            # message) — which may be a strict prefix of the staged
            # slice when more rows arrived after a crash; the leftover
            # rows are decided again on a later sweep.
            return await self.reconcile_prepared(stage, pending)

        # The shape (and the commit's read_snapshot basis) is pinned
        # BEFORE the staged range is read. Warm: no I/O. Cold: one
        # totals=false identity read.
        info = await self._io(self._shape_sync)
        read_snapshot = info.read_snapshot_id
        assert read_snapshot is not None  # _shape_sync refuses otherwise
        # The tuple's hoglake field names, from the live spec (positional
        # names when the group was keyed under a spec of another arity).
        fields = _group_fields(info, group)
        label = keyspace.group_label(fields, group)

        rows = await stage.scan_group_rows_bounded(
            group,
            max_rows=self._cfg.flush_scan_max_rows,
            max_bytes=self._scan_budget_bytes(),
        )
        if not rows:
            # The byte cap refuses a FIRST row larger than the budget;
            # an empty scan then is indistinguishable from a genuinely
            # empty group. Probe one row: a record that staged at all is
            # bounded by the broker's message limit, so one row is
            # always a flushable unit — wedging on it forever would be
            # the worse failure.
            rows = [
                (row_key, payload)
                async for row_key, payload in stage.scan_group_rows(group, max_rows=1)
            ]
        if not rows:
            # A decided key scanning zero rows is the redelivery-inflation
            # residue (stage_batch is idempotent at the row level but its
            # stats merge inflates on redelivery; a flush settles ACTUALS
            # and the leftover counters survive). Left alone the stale
            # first_staged_ts would pin the oldest-staged-age gauge
            # forever, so the stats entry is repaired away — race-free
            # inside reconcile_stats' serializable transaction.
            repaired = await stage.reconcile_stats(group)
            if repaired:
                self._stats_repaired += 1
            log.warning(
                "flush decision for %s[%d] group %s scanned zero rows with a "
                "live stats entry (redelivery residue%s); nothing flushed",
                stage.topic,
                stage.partition,
                label,
                " — stats repaired" if repaired else "",
            )
            return FlushReport(
                topic=stage.topic,
                partition=stage.partition,
                group=group,
                trigger=decision.trigger,
                outcome="empty",
                first_offset=-1,
                last_offset=-1,
            )
        # The window: row keys sort by (group, offset), so the scan —
        # capped or not — is an exact offset prefix of the group's
        # staged rows as of its snapshot, and every staged row of the
        # group inside [first scanned, last scanned] was scanned.
        # Arrivals after the snapshot normally carry larger offsets; a
        # row that enters the window after the scan anyway (a rewind
        # re-keying an in-window offset into this group) makes the
        # settlement guard halt inside the settle transaction, which
        # rolls back.
        first_offset = rows[0][0].offset
        last_offset = rows[-1][0].offset
        window = FlushedKey(group, first_offset, last_offset, group_fields=fields)
        # The key names the window's CONTENT: SHA-256 over the scanned
        # rows (offset || length || payload, offset order), on the I/O
        # pool — up to the scan budget of payload bytes.
        digest = await self._io(
            lambda: keyspace.content_digest((rk.offset, p) for rk, p in rows)
        )
        key = keyspace.idempotency_key(
            self._table_uuid,
            stage.topic,
            stage.partition,
            group,
            first_offset,
            last_offset,
            digest,
        )
        # Receipt first (step 4 above): the same content under the same
        # window already landed (an identical rewind) — settle from the
        # receipt with nothing built or uploaded.
        receipt = await self._lookup_receipt(key)
        if receipt is not None:
            return await self._settle_deduped(
                stage,
                decision,
                window,
                len(rows),
                key,
                f"its receipt exists (snapshot {receipt.snapshot_id})",
                snapshot_id=receipt.snapshot_id,
            )

        now_us = self._now_us()
        ctx = FlushContext(
            topic=stage.topic,
            partition=stage.partition,
            group=group,
            group_fields=fields,
            trigger=decision.trigger,
            first_offset=first_offset,
            last_offset=last_offset,
            idempotency_key=key,
            read_snapshot=read_snapshot,
            table_uuid=self._table_uuid,
            build=self._build_token(),
        )
        cfg = self._cfg
        plan: PreparedPlan = await self._io(
            lambda: build_prepared_plan(
                rows,
                info,
                ctx=ctx,
                event_time_policy=cfg.event_time_policy,
                event_time_window_us=(
                    now_us - cfg.event_time_max_past_s * _MICROS_PER_SECOND,
                    now_us + cfg.event_time_max_future_s * _MICROS_PER_SECOND,
                ),
                data_path=self._catalog.data_path,
                max_files_per_commit=cfg.max_files_per_commit,
                footer_stats_min_bytes=max(
                    1,
                    int(cfg.target_output_bytes * FOOTER_STATS_SIZE_FRACTION),
                ),
                version=_millrace_version(),
            )
        )
        for dropped in plan.dropped_keys:
            if dropped not in self._warned_dropped:
                self._warned_dropped.add(dropped)
                log.warning(
                    "payload key %r is not a column of %s.%s; dropping it "
                    "from every flush (no schema evolution in v1)",
                    dropped,
                    info.namespace,
                    info.name,
                )
        self._dropped_payload_keys += len(plan.dropped_keys)

        # Drops are counted and logged once per build; the dropped rows
        # stay staged until the window's settlement deletes them with
        # the published ones.
        if plan.dropped_records:
            self._note_drops(stage, label, plan.dropped_records)

        if plan.payload is None:
            # Every row dropped: nothing may be committed (a commit
            # registers at least one file). Settle WITHOUT a commit.
            await self._settle(stage, window, len(rows))
            self._decisions_dropped += 1
            return FlushReport(
                topic=stage.topic,
                partition=stage.partition,
                group=group,
                trigger=decision.trigger,
                outcome="dropped",
                first_offset=first_offset,
                last_offset=last_offset,
                dropped=len(plan.dropped_records),
                idempotency_key=key,
            )

        # Upload BEFORE persist: a persisted request whose objects never
        # landed would register phantom files (the server never opens
        # them). The upload→persist gap is the orphan window, and it is
        # accounted.
        try:
            await self._io(lambda: self._upload(plan))
        except BaseException as e:
            self._count_orphans(
                getattr(e, "completed_uris", ()),
                f"the upload for group {label} offsets "
                f"{first_offset}-{last_offset} failed partway through a "
                f"{plan.files}-file fanout. The file it failed on is not "
                "among these and is not counted, but a failed close can "
                "leave a truncated object, so treat it as possibly present "
                "too",
                reason="upload_failed",
            )
            raise
        body = canonical_body(plan.payload)
        try:
            await stage.persist_prepared(
                group,
                first_offset,
                PreparedRequest(
                    idempotency_key=key,
                    body=body,
                    persisted_at=now_us,
                    topic=stage.topic,
                    partition=stage.partition,
                    group=group,
                    group_fields=fields,
                    row_count=len(rows),
                ),
            )
        except BaseException:
            self._count_orphans(
                plan.uris,
                f"the upload for group {label} offsets "
                f"{first_offset}-{last_offset} completed but the prepared "
                "request could not be persisted",
                reason="persist_failed",
            )
            raise

        try:
            result = await self._publish(plan.payload)
        except DdlSinceReadSnapshotError as e:
            # Definitive refusal, zero writes: the REBUILD case. Drop the
            # persisted entry (replaying it is a livelock — the basis is
            # frozen in the payload), account the abandoned upload, and
            # let the still-staged rows re-prepare against a fresh read
            # on a later sweep.
            self._count_orphans(
                plan.uris,
                f"the commit for group {label} was refused with "
                "ddl_since_read_snapshot; the flush re-prepares against a "
                "fresh read",
                reason="ddl_rebuild",
            )
            await stage.drop_prepared(group, first_offset)
            self._count_reprepare(_REPREPARE_DDL)
            log.warning(
                "DDL landed on %s.%s while a flush was in flight (%s); "
                "the prepared request is dropped and the rows re-prepare "
                "against a fresh read",
                info.namespace,
                info.name,
                e,
            )
            return FlushReport(
                topic=stage.topic,
                partition=stage.partition,
                group=group,
                trigger=decision.trigger,
                outcome="reprepare",
                first_offset=first_offset,
                last_offset=last_offset,
                idempotency_key=key,
            )
        except IncarnationChangedError as e:
            raise self._halt(
                "table_recreated",
                f"the destination was recreated (pinned incarnation "
                f"{self._table_uuid}): {e}. The pipeline halts; staged state "
                "stays for an operator.",
                plan.uris,
            ) from e
        except ReadSnapshotExpiredError as e:
            raise self._halt(
                "read_snapshot_expired",
                f"the commit's read_snapshot fell below the catalog's "
                f"expiry floor: {e.detail or e}. Reconcile from a current "
                "read of the table; never silently skip.",
                plan.uris,
            ) from e
        except NotFoundError as e:
            raise self._halt(
                "destination_missing",
                f"the destination {cfg.catalog}/{cfg.namespace}/{cfg.table} "
                f"no longer resolves: {e}",
                plan.uris,
            ) from e
        except HoglakeError as e:
            if isinstance(e, ValidationError) and _REUSED_KEY_MARKER in _error_text(e):
                # The key names this window's CONTENT and its receipt
                # answered 404 just before this build: a receipt under it
                # now means another writer (a second owner of the
                # partition, a foreign process) committed this content
                # in between. This process never published it, so it
                # never settles on it: halt, rows and entry staged.
                raise self._halt(
                    "idempotency_key_reused",
                    f"the commit for {stage.topic}[{stage.partition}] group "
                    f"{label} offsets {first_offset}-{last_offset} (content "
                    f"digest {digest}, idempotency key {key}) was answered "
                    "422 reused-key: another writer committed this content "
                    "between the receipt lookup and this publish. Halting "
                    "with the rows staged.",
                    plan.uris,
                ) from e
            status = e.status_code
            if status is not None and 400 <= status < 500:
                # Any OTHER answered 4xx (a proxy's 401/403, an ingress
                # 413, a plain 400, any 422):
                # nothing about it says the ROWS can never publish. Halt
                # with the rows staged and the prepared/ entry persisted —
                # exactly the idempotency_key_reused / destination_missing
                # posture (PR #331 review: the pre-fix shape truncated and
                # deleted up to a whole flush window on any answered 4xx).
                raise self._halt(
                    "commit_refused",
                    f"the commit for {stage.topic}[{stage.partition}] group "
                    f"{label} offsets {first_offset}-{last_offset} "
                    f"(idempotency key {key}) was answered {status}: "
                    f"{_error_text(e)}. No answered refusal deletes staged "
                    "rows — they stay staged and the pipeline halts for an "
                    "operator.",
                    plan.uris,
                ) from e
            raise

        await self._settle(stage, window, len(rows))
        self._flushes_committed += 1
        self._rows_published += plan.records
        self._files_written += plan.files
        self._bytes_written += sum(u.size for u in plan.uploads)
        # Feed the realized ratio back (the planner's size-lane input):
        # parquet bytes uploaded ÷ staged bytes of the flushed window.
        # Replays are NOT observed (a replayed flush re-reads nothing);
        # zero-byte shapes are skipped by the estimator itself.
        self._ratio.observe(sum(u.size for u in plan.uploads), plan.staged_bytes)
        log.debug(
            "flushed %s[%d] group %s offsets %d-%d: %d rows, %d files, snapshot %d (%s)",
            stage.topic,
            stage.partition,
            label,
            first_offset,
            last_offset,
            plan.records,
            plan.files,
            result.snapshot_id,
            decision.trigger,
        )
        return FlushReport(
            topic=stage.topic,
            partition=stage.partition,
            group=group,
            trigger=decision.trigger,
            outcome="committed",
            first_offset=first_offset,
            last_offset=last_offset,
            records=plan.records,
            files=plan.files,
            dropped=len(plan.dropped_records),
            snapshot_id=result.snapshot_id,
            idempotency_key=key,
        )

    async def _settle_deduped(
        self,
        stage: PartitionStage,
        decision: FlushDecision,
        window: FlushedKey,
        scanned: int,
        key: str,
        why: str,
        *,
        snapshot_id: int | None = None,
    ) -> FlushReport:
        """Settle a fresh window that already landed under its key —
        nothing new is committed (``millrace_flush_deduped_total``). The
        settle is the ordinary one (rows, stats, any ``prepared/`` entry
        of the window), guarded against the scanned count."""
        await self._settle(stage, window, scanned)
        self._flush_deduped += 1
        log.info(
            "deduped %s[%d] group %s offsets %d-%d (idempotency key %s): %s; "
            "settled locally, nothing new committed",
            stage.topic,
            stage.partition,
            window.label,
            window.first_offset,
            window.last_offset,
            key,
            why,
        )
        return FlushReport(
            topic=stage.topic,
            partition=stage.partition,
            group=window.group,
            trigger=decision.trigger,
            outcome="deduped",
            first_offset=window.first_offset,
            last_offset=window.last_offset,
            snapshot_id=snapshot_id,
            idempotency_key=key,
        )

    def _upload(self, plan: PreparedPlan) -> None:
        """Write the plan's parquet buffers through pyhoglake's upload
        machinery (single-request for small objects, fanned out). A
        mid-fanout failure carries ``completed_uris`` — the exact uris
        that landed, in input order, for the caller's orphan accounting
        (``run_uploads`` fills the out-parameter on the failure path too;
        the count is a lower bound on objects present and the failed file
        may be a truncated object that exists)."""
        filesystem = getattr(self._client, "_filesystem", None)
        put_client_fn = getattr(self._client, "_put_client", None)
        if filesystem is None or put_client_fn is None:
            raise FlushError(
                "pyhoglake no longer exposes the upload accessors "
                "(_filesystem/_put_client); the flush cannot write parquet"
            )
        fanout = resolve_concurrency(len(plan.uploads), None)
        put_client = put_client_fn(fanout)
        widen_io_threads_for(plan.uploads, put_client, fanout)
        completed: list[str] = []
        try:
            run_uploads(
                lambda upload: perform_upload(filesystem(), put_client, upload),
                list(plan.uploads),
                concurrency=fanout,
                completed=completed,
            )
        except BaseException as e:
            try:
                e.completed_uris = tuple(completed)  # type: ignore[attr-defined]
            except (AttributeError, TypeError):
                pass
            raise


# -- the sweep loop -----------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SweepReport:
    """One tick's accounting over every claimed partition. ``fenced`` is
    the number of partitions currently fenced OUT of scheduling (their
    SlateDB instance surfaced ``Error.Closed`` — FENCED on a contested
    open, or another terminal close): they are planned no decisions and
    stay in the count until the assignment revokes them."""

    partitions: int
    decisions: int
    committed: int
    replayed: int
    receipt_settled: int
    reprepared: int
    dropped: int
    empty: int
    failed: int
    fenced: int = 0
    deduped: int = 0


def _oldest_eligible_staged_ts(decisions: Sequence[FlushDecision]) -> int | None:
    """The oldest ``first_staged_ts`` among a partition's flush
    decisions — the ALERTABLE backlog age basis
    (``millrace_oldest_eligible_staged_age_seconds``). The decided keys
    are exactly the eligible set (a planner lane trips ⇔ the key is
    size-, age- or slow-eligible), so this is the oldest key the flush
    owes work, never the raw oldest: a tiny key below
    ``min_flush_bytes`` deliberately waits out its slow lane and must
    not read as a stuck flush (PR #331 review). None when nothing is
    eligible (the gauge is absent, not zero).
    """
    return min((d.first_staged_ts for d in decisions), default=None)


class FlushRunner:
    """The flush loop: one planning pass per tick over the StageManager's
    claimed partitions. Per partition and tick:

    1. RECONCILE (first tick after a (re)claim): ``recover()`` collects
       legacy ``flushed/`` markers in bounded pages, and every surviving
       ``prepared/`` entry is replayed / receipt-settled by the flusher.
       A partition whose reconciliation
       fails transiently is skipped for the tick and retried on the
       next — it is never marked reconciled.
    2. ASSEMBLE the planner input: one full ``stats/`` scan (the size
       lane's input — there is no size-ordered index; ``sched_size`` is
       the design's deferred bench question) PLUS the age/slow
       readiness path the design promises: a ``sched_age/`` index
       prefix scan through the age-lane cutoff, then a point-read per
       candidate for the exact, current decision input (M6). The full
       scan is ALSO the gauge source: the sweep's fold is published on
       the StageManager (``publish_gauges``) for the metrics scrape, so
       a sweep costs one ``stats/`` scan per partition, not two. The
       same pass computes the ALERTABLE backlog gauge: the oldest
       ELIGIBLE staged key per partition (the decided keys' minimum
       ``first_staged_ts`` — the planner's own knobs, policy and ratio,
       so a tiny key waiting out its slow lane never reads as backlog),
       published alongside as ``oldest_eligible_staged_ts``.
    3. PLAN: ``planner.plan_flush(stats, now, knobs,
       estimated_compression_ratio=...)`` — the ratio is the flusher's
       observed EWMA, the fallback until the first flush lands.
    4. EXECUTE: per decision, :meth:`HoglakeFlusher.flush_key`; commits
       are per ``(group, window)`` and never exceed
       ``max_files_per_commit`` files (the fanout guard halts on a
       spec/bound disagreement rather than emit an unbounded commit
       body).

    Bounded parallelism (M5): ``concurrency`` caps the flush decisions
    in flight PER PARTITION, and ``4 × concurrency`` caps them
    process-wide (each in-flight decision holds its bounded scan in
    memory — the global cap is what makes the sweep's memory shape a
    function of config, not of how many partitions backlog at once).
    Within a partition, dispatch follows the planner's oldest-first
    order; completion may interleave (decisions are independent
    ``(group, window)`` units — order between groups carries no
    invariant). ``concurrency=1`` is the serial sweep.

    Runs as an asyncio task sharing the loop with the consumer (stage
    mutations are serialized by stage.py's lock; scans need none); all
    hoglake I/O is offloaded to the flusher's executor. Clocks and sleep
    are injected.

    Containment: a decision that fails (ladder exhaustion, a closed
    stage, an encoder bug) is logged, counted, and the sweep moves on —
    one key never wedges the pipeline, under any concurrency. TWO
    exceptions: the halt taxonomy (:class:`FlushHalted`) stops the
    runner loudly (decisions still in flight settle their own windows
    first) and every later tick re-raises it; and SlateDB
    ``Error.Closed`` — the contested-open fence (``CloseReason.FENCED``:
    a newer writer owns the path, pinned by
    tests/test_slatedb_parity.py) or another terminal close of the
    instance under the stage — is a first-class TERMINAL condition for
    that partition's decisions: the partition is fenced out of
    scheduling until the assignment revokes it, logged ONCE (never a
    stack trace per decision per tick), and surfaced on
    :attr:`fenced_partitions` and :attr:`SweepReport.fenced`. The staged
    state is the new owner's; nothing here closes or deletes it.
    """

    def __init__(
        self,
        stages: StageManager,
        flusher: HoglakeFlusher,
        knobs: PlannerKnobs,
        *,
        now_us: Callable[[], int],
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        sweep_s: float = 5.0,
        concurrency: int = 1,
    ) -> None:
        if sweep_s <= 0:
            raise ValueError(f"sweep_s must be > 0, got {sweep_s}")
        if concurrency < 1:
            raise ValueError(f"concurrency must be >= 1, got {concurrency}")
        self._stages = stages
        self._flusher = flusher
        self._knobs = knobs
        self._now_us = now_us
        self._sleep = sleep
        self._sweep_s = sweep_s
        self._concurrency = concurrency
        self._reconciled: set[tuple[str, int]] = set()
        self._fenced: set[tuple[str, int]] = set()
        self._sweeps = 0
        self._stopping = False
        self._started = False
        self._halt: FlushHalted | None = None
        self._last_report = SweepReport(0, 0, 0, 0, 0, 0, 0, 0, 0)

    async def run(self) -> None:
        """Sweep until :meth:`stop` (or a halt). The flusher and the
        StageManager are caller-owned: neither is closed here."""
        if self._started:
            return
        self._started = True
        while not self._stopping:
            await self.run_once()
            if self._stopping:
                break
            await self._sleep(self._sweep_s)

    def stop(self) -> None:
        """Ask the loop to stop after the in-flight tick. Safe from a
        signal handler or another thread."""
        self._stopping = True

    @property
    def halted(self) -> FlushHalted | None:
        """The halt, when the pipeline has stopped loudly."""
        return self._halt

    @property
    def sweeps(self) -> int:
        return self._sweeps

    @property
    def last_report(self) -> SweepReport:
        return self._last_report

    @property
    def fenced_partitions(self) -> tuple[tuple[str, int], ...]:
        """The partitions currently fenced OUT of flush scheduling after
        their SlateDB instance surfaced ``Error.Closed`` (the
        contested-open fence, or another terminal close) — the runner's
        operational surface for the condition, until the assignment
        revokes them (a revoke clears the mark; a re-claim reopens the
        path). Sorted, for a stable read."""
        return tuple(sorted(self._fenced))

    def _note_fenced(
        self, key: tuple[str, int], exc: BaseException, *, where: str
    ) -> None:
        """Mark one partition's stage as terminally closed underneath us
        and stop scheduling it. Logs ONCE per partition per claim (the
        pre-fix shape was a stack trace per decision every sweep forever
        — PR #331 review); later sweeps skip the partition silently. The
        stage is NOT closed here (the flush loop does not own it), and
        the staged state is the new owner's."""
        self._reconciled.discard(key)
        if key in self._fenced:
            return
        self._fenced.add(key)
        log.warning(
            "stage %s[%d] is closed underneath the flush loop (%s, at %s): "
            "a newer writer owns the path or the instance died — this "
            "partition is fenced out of flush scheduling until its "
            "assignment is revoked; staged state is left for the owner",
            key[0],
            key[1],
            exc,
            where,
        )

    async def run_once(self) -> SweepReport:
        """One planning pass over every claimed partition. Re-raises a
        previous halt: a halted pipeline stays halted until the process
        is restarted."""
        if self._halt is not None:
            raise self._halt
        open_set = set(self._stages.partitions())
        # A revoked partition is reconciled afresh if it is ever
        # re-claimed; the same revoke also clears a fence mark (the
        # re-claim reopens the path under a fresh writer).
        self._reconciled &= open_set
        self._fenced &= open_set
        decisions = committed = replayed = receipt_settled = deduped = 0
        reprepared = dropped = empty = failed = 0
        gauge_map: dict[tuple[str, int], StageGauges] = {}
        eligible_oldest: dict[tuple[str, int], int] = {}
        work: list[tuple[PartitionStage, FlushDecision]] = []
        for topic, partition in sorted(open_set):
            if (topic, partition) in self._fenced:
                continue  # terminally closed underneath us — no planning
            try:
                stage = self._stages.stage(topic, partition)
            except KeyError:
                continue  # revoked between the listing and the lookup
            if (topic, partition) not in self._reconciled:
                try:
                    reconciliation = await self._reconcile_partition(stage)
                except FlushHalted as halt:
                    self._halt = halt
                    raise
                except _SlateError.Closed as fenced:
                    # The contested open surfaced during recovery: this
                    # partition's DB is terminally closed. Fence it out
                    # (once), do not retry it next tick.
                    self._note_fenced((topic, partition), fenced, where="recovery")
                    continue
                except Exception as failure:
                    # Transient reconciliation failure: skipped this tick,
                    # retried the next (the partition is never marked
                    # reconciled). Its gauges must still reach the
                    # published snapshot — the scrape reads them — so
                    # read them directly, eligibility included (a skipped
                    # partition's waiting work is exactly what the
                    # eligible-age alert exists for). Best-effort: a
                    # partition this broken must not wedge the sweep's
                    # gauge publication either.
                    if isinstance(failure, FlushTransient):
                        log.warning(
                            "reconciliation for %s[%d] deferred to the next tick: %s",
                            topic,
                            partition,
                            failure,
                        )
                    else:
                        log.exception(
                            "reconciliation failed for %s[%d]; the partition "
                            "is skipped this tick",
                            topic,
                            partition,
                        )
                    failed += 1
                    try:
                        skipped_stats = await stage.iter_key_stats()
                    except Exception:
                        log.debug(
                            "the gauge read for %s[%d] failed too",
                            topic,
                            partition,
                            exc_info=True,
                        )
                    else:
                        gauge_map[(topic, partition)] = gauges_from_stats(skipped_stats)
                        eldest = _oldest_eligible_staged_ts(
                            plan_flush(
                                skipped_stats,
                                self._now_us(),
                                self._knobs,
                                estimated_compression_ratio=self._flusher.compression_ratio,
                            )
                        )
                        if eldest is not None:
                            eligible_oldest[(topic, partition)] = eldest
                    continue
                self._reconciled.add((topic, partition))
                for report in reconciliation:
                    replayed += report.outcome == "replayed"
                    receipt_settled += report.outcome == "receipt_settled"
                    reprepared += report.outcome == "reprepare"
                    dropped += report.outcome == "dropped"
                    empty += report.outcome == "empty"
            try:
                stats = await self._assemble_stats(stage)
            except _SlateError.Closed as fenced:
                self._note_fenced((topic, partition), fenced, where="planning")
                continue
            except StageClosedError:
                self._reconciled.discard((topic, partition))
                continue
            gauge_map[(topic, partition)] = gauges_from_stats(stats)
            partition_decisions = plan_flush(
                stats,
                self._now_us(),
                self._knobs,
                estimated_compression_ratio=self._flusher.compression_ratio,
            )
            # The alertable backlog gauge: the decided keys ARE the
            # eligible set (a lane trips ⇔ eligible), so their minimum
            # first_staged_ts is the oldest eligible staged byte — per
            # the planner's own knobs/policy/ratio, never the raw
            # oldest (a tiny key waiting out its slow lane is not
            # backlog).
            eldest = _oldest_eligible_staged_ts(partition_decisions)
            if eldest is not None:
                eligible_oldest[(topic, partition)] = eldest
            for decision in partition_decisions:
                work.append((stage, decision))
                decisions += 1
        # The sweep's gauges ARE the planner input's fold (M6): one
        # scan per partition per sweep serves the planner and the
        # metrics scrape. A partition revoked mid-sweep drops out of
        # the published set (its staged bytes are the new owner's
        # concern now).
        still_open = set(self._stages.partitions())
        self._stages.publish_gauges(
            {key: g for key, g in gauge_map.items() if key in still_open},
            oldest_eligible_staged_ts=min(
                (ts for key, ts in eligible_oldest.items() if key in still_open),
                default=None,
            ),
        )
        reports, decision_failures = await self._execute(work)
        failed += decision_failures
        for report in reports:
            committed += report.outcome == "committed"
            replayed += report.outcome == "replayed"
            receipt_settled += report.outcome == "receipt_settled"
            deduped += report.outcome == "deduped"
            reprepared += report.outcome == "reprepare"
            dropped += report.outcome == "dropped"
            empty += report.outcome == "empty"
        self._sweeps += 1
        sweep = SweepReport(
            partitions=len(open_set),
            decisions=decisions,
            committed=committed,
            replayed=replayed,
            receipt_settled=receipt_settled,
            reprepared=reprepared,
            dropped=dropped,
            empty=empty,
            failed=failed,
            fenced=len(self._fenced),
            deduped=deduped,
        )
        self._last_report = sweep
        if decisions:
            log.info(
                "flush sweep %d: %d decisions — %d committed, %d replayed, "
                "%d receipt-settled, %d deduped, %d re-prepared, %d dropped, "
                "%d empty, %d failed",
                self._sweeps,
                decisions,
                committed,
                replayed,
                receipt_settled,
                deduped,
                reprepared,
                dropped,
                empty,
                failed,
            )
        return sweep

    async def _assemble_stats(self, stage: PartitionStage) -> list[KeyStats]:
        """One partition's planner input for this tick (M6).

        The full ``stats/`` scan is the SIZE lane's input (there is no
        size-ordered index — ``sched_size`` is the design's deferred
        bench question) and the sweep's gauge source. The age/slow lanes
        come from the ``sched_age/`` index, as the design promises the
        readiness scheduler: a prefix scan through the age-lane cutoff
        enumerates the deadline-eligible candidate keys, and each
        candidate's stats are POINT-READ for the exact current values
        the deadline decision is made from (the shared scan's snapshot
        is older by construction). A candidate without a stats entry is
        sched/stats drift — foreign writes or a bug — and refused
        loudly rather than skipped.
        """
        stats = await stage.iter_key_stats()
        cutoff = self._now_us() - self._knobs.flush_deadline_s * _MICROS_PER_SECOND
        candidates = await stage.iter_sched_age_through(cutoff)
        if not candidates:
            return stats
        by_group = {s.group: s for s in stats}
        for candidate in candidates:
            exact = await stage.read_group_stats(candidate.group)
            if exact is None:
                raise StageCorruptionError(
                    f"group {candidate.group}: a sched_age/ entry with no "
                    "stats/ entry (the two are written and removed in one "
                    "batch — a leftover is foreign writes or a bug)"
                )
            by_group[candidate.group] = exact
        return list(by_group.values())

    async def _execute(
        self, work: list[tuple[PartitionStage, FlushDecision]]
    ) -> tuple[list[FlushReport], int]:
        """Run the sweep's decisions with bounded parallelism (M5).

        Returns ``(reports, failed_count)``. A bounded WORKER POOL (not
        one task per decision — a catch-up sweep can carry 10⁵
        decisions and an idle task apiece is its own memory problem)
        pulls ``(stage, decision)`` units off the dispatch list in
        planner order: at most ``concurrency`` in flight per partition
        (per-partition semaphores), at most ``4 × concurrency`` workers
        process-wide (each in-flight decision holds its bounded scan in
        memory, so the global bound is what ties sweep memory to config
        rather than to how many partitions backlog at once).

        Containment per decision, exactly as the serial loop: a failure
        (ladder exhaustion, an encoder bug, an object-store fault) is
        logged, counted, and the sweep moves on; a revoked stage stops
        being worked; a FENCED stage (SlateDB ``Error.Closed`` — the
        contested open) is fenced out of scheduling entirely, logged
        once, never stack-traced per decision; a :class:`FlushHalted`
        is sticky — workers stop pulling, in-flight decisions run to
        their own contained end, and the halt re-raises here. With
        ``concurrency=1`` the sweep is strictly serial, in the
        planner's dispatch order.
        """
        reports: list[FlushReport] = []
        failed = 0
        if not work:
            return reports, failed
        pending: deque[tuple[PartitionStage, FlushDecision]] = deque(work)
        per_partition: dict[tuple[str, int], asyncio.Semaphore] = {}

        async def worker() -> None:
            nonlocal failed
            while self._halt is None:
                # Work-conserving pull: the dispatch list is
                # partition-major (planner order per partition), and a
                # naive FIFO head would pile every worker onto one
                # partition's in-flight cap; take the first queued
                # decision whose partition has a free slot, rotating
                # skipped items to the back. A FULL rotation without a
                # free slot means every partition with queued work is at
                # its cap — then block on the front item's slot, which
                # frees within one decision's duration.
                if not pending:
                    return
                for _ in range(len(pending)):
                    stage, decision = pending[0]
                    key = (stage.topic, stage.partition)
                    semaphore = per_partition.setdefault(
                        key, asyncio.Semaphore(self._concurrency)
                    )
                    if not semaphore.locked():
                        break
                    pending.rotate(-1)
                # Whether the loop broke early or spun a full cycle
                # (every partition with queued work at its cap — then
                # the front item's slot is the one to block on, and it
                # frees within one decision's duration), the front item
                # is the pick.
                stage, decision = pending.popleft()
                key = (stage.topic, stage.partition)
                semaphore = per_partition[key]
                async with semaphore:
                    if self._halt is not None:
                        return
                    if key in self._fenced:
                        # Fenced mid-sweep: its queued decisions are
                        # dropped without another word (the fence was
                        # logged once already) — no per-decision stack
                        # traces against a terminally closed stage.
                        continue
                    try:
                        report = await self._flusher.flush_key(stage, decision)
                    except FlushHalted as halt:
                        self._halt = halt
                        return
                    except _SlateError.Closed as fenced:
                        # The contested open surfaced mid-decision
                        # (SlateDB's lazy fencing: pinned by
                        # tests/test_slatedb_parity.py — WHICH call
                        # detects it is timing-dependent, so it can be
                        # any stage call in flush_key). TERMINAL for
                        # this partition's decisions: fence it out of
                        # scheduling, one log, no stack trace; other
                        # partitions are unaffected. Queued decisions of
                        # the same partition hit the same arm cheaply.
                        self._note_fenced(key, fenced, where="flush decision")
                        continue
                    except StageClosedError:
                        # Revoked mid-sweep: staged state stays for the
                        # new owner. Later queued decisions of the same
                        # partition fail this same fast check.
                        self._reconciled.discard(key)
                        continue
                    except FlushTransient as e:
                        # A ladder ran out on transient server/transport
                        # failures: one line, no trace; retried next sweep.
                        failed += 1
                        log.warning(
                            "flush decision for %s[%d] group %s deferred to "
                            "the next sweep: %s",
                            stage.topic,
                            stage.partition,
                            decision.group,
                            e,
                        )
                        continue
                    except Exception:
                        # Contained per decision: the rows stay staged,
                        # the next sweep retries. Never wedged.
                        failed += 1
                        log.exception(
                            "flush decision failed for %s[%d] group %s",
                            stage.topic,
                            stage.partition,
                            decision.group,
                        )
                        continue
                    reports.append(report)

        workers = [
            asyncio.create_task(worker())
            # concurrency=1 is the strictly serial sweep (one worker,
            # FIFO dispatch order — the pre-M5 behavior); otherwise a
            # bounded pool at 4x the per-partition cap.
            for _ in range(
                1 if self._concurrency == 1 else min(len(work), 4 * self._concurrency)
            )
        ]
        await asyncio.gather(*workers)
        if self._halt is not None:
            raise self._halt
        return reports, failed

    async def _reconcile_partition(self, stage: PartitionStage) -> list[FlushReport]:
        """The recovery handoff for one (re)claimed partition: collect
        legacy ``flushed/`` markers (bounded pages, stage.py), then
        reconcile every surviving ``prepared/`` entry — receipt lookup
        first, verbatim replay on a 404 whatever the entry's age. The
        reconciliation reports count toward the
        sweep's totals."""
        recovery = await stage.recover()
        if recovery.legacy_markers_collected:
            log.info(
                "recovery collected %d legacy flushed/ marker(s) for %s[%d] "
                "(settled state no build writes any more)",
                recovery.legacy_markers_collected,
                stage.topic,
                stage.partition,
            )
        reports = []
        for pending in recovery.pending_prepared:
            reports.append(await self._flusher.reconcile_prepared(stage, pending))
        return reports
