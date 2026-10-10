"""The flush path: staged range → Arrow → parquet → prepared commit.

One key's flush (docs/kafka-ingestion.md §The flush planner and §The
wire contract): scan the staged range → decode payloads (v1: JSON
values) → clamp/quarantine junk ``event_time`` → one parquet file per
``(team_id, month(timestamp))`` partition tuple under the table's LIVE
partition spec → upload → persist the complete prepared request under
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
- Regeneration is deterministic by construction: same staged rows, same
  table basis, same clock → byte-identical request. Object names derive
  from the idempotency key (``…/{key}/{index}.parquet``) rather than a
  random uuid, so a rebuild after a crash between upload and persist
  rewrites the SAME object names instead of leaving a second copy —
  the orphan counter stays honest about this (it counts abandonment
  events; a same-window rebuild reclaims the names). This is why the
  payload is assembled here over pyhoglake's primitives
  (:func:`pyhoglake.types.columns_to_arrow_schema`,
  :func:`pyhoglake.parquet_schema.prepared_schema_matches`,
  :func:`pyhoglake.stats.extract_column_stats`,
  :mod:`pyhoglake.transforms`, :mod:`pyhoglake.upload`) instead of via
  ``Table.prepare_append_tables``: that method mints ``uuid4()`` object
  names, and "the same rows re-flushed produce the same request"
  (docs/kafka-ingestion.md §The wire contract) does not survive that.

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
- 410 (``ReadSnapshotExpiredError``): halt, carrying the server's
  reconcile instructions (viaduck lesson 4; "410 below the retention
  floor is a stop sign, never a skip").
- 503 ``commit_queue_timeout`` (and other 5xx / transport failures):
  ambiguous — back off per ``Retry-After`` (captured through an httpx
  hook, millpond's pattern) floored against an exponential curve, retry
  the IDENTICAL persisted request, bounded attempts. Exhaustion leaves
  the persisted entry in place; the next sweep replays it (receipt
  lookup first).
- 422 ``record_validation``: quarantine the DECISION — every row of the
  window is written to ``poison/`` (the original payloads, WHOLE — a
  flush-time quarantine is never capped, because the staged rows are
  deleted at settlement and the poison entry is the only surviving copy)
  and the range is settled WITHOUT a commit (no receipt exists to
  reconcile, so recovery never looks for one) — recorded, counted,
  logged, and the loop is never wedged. ``record_validation`` — the
  server judging RECORDS — is the ONLY 422 that may do this
  (:data:`_is_record_validation_refusal`); it is deliberately precise,
  and no current server emits it (footer-shipping means the server never
  opens the parquet, so its whole 422 vocabulary is request-shape). A
  422 saying the idempotency key was reused with a DIFFERENT request is
  its own halt: with deterministic derivation that means a foreign
  writer or broken determinism, and the pipeline halts instead of
  settling rows a stranger may not have published.
- EVERY OTHER answered 4xx (a 401/403 from a proxy, a 413 from an
  ingress, a plain 400, a request-shape ``validation`` 422): HALT with
  the rows staged, exactly like ``idempotency_key_reused`` and
  ``destination_missing`` — the payload was judged by something, but a
  refusal that does not name bad RECORDS says nothing about the rows,
  and the persisted ``prepared/`` entry plus the staged rows are the
  operator's reconciliation material. (The pre-fix behavior quarantined
  the whole window — truncated and deleted — on any answered 4xx, which
  is why this paragraph exists.)

The staged scan order is KAFKA-RECORD time (the row key's timestamp is
the record timestamp the consumer staged under); the catalog's sort spec
is PAYLOAD-EVENT time (``TableInfo.sort_spec``), which late and skewed
events invert constantly. :func:`build_prepared_plan` reconciles the two:
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
is the small-file regime the design exists to kill. The window is
``[first_scanned_offset, last_scanned_offset]`` when the scan covered
the team's whole staged slice — the steady state, where caps never
bind: staged offsets only ever grow past a scan snapshot, so settling
the span deletes exactly the published rows. Only a TRUNCATED scan
(one a cap cut short) narrows, to ``[first_scanned_offset, K]`` where
``K`` is the covered offset prefix (``_covered_window``): a record
whose record timestamp inverts past the cap boundary could otherwise
have the settlement delete a row the flush never saw. The window
identity and the settlement guard both come from the SCANNED rows,
never the stats snapshot; leftover rows re-decide on the next sweep
(the window/prepared machinery handles arbitrary sub-windows). A single
row larger than the byte budget still flushes (one record is always a
bounded unit), probed with a one-row scan. Memory shape per decision:
≤ ``max(scan_budget, largest row)`` payload bytes materialized plus
``≤ flush_scan_max_rows`` decoded records and the Arrow build over
them; the settlement streams the team's slice within its transaction
but deletes only the window's rows.

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
``key=value`` sequence, no wall-clock/random/per-attempt state, offset
ranges from the staged ``offsets/`` record (clipped to the flushed
window), and ``table=`` carries the incarnation uuid. It is formatted
inside the pure request builder — BEFORE any upload (pinned by a
source-order test) — and its ``offsets`` line is the recovery material:
a persisted request's flush window is parsed back out of it
(``parse_message_offsets``), which is why truncation keeps the first
and last ranges verbatim.

Recovery handoff: :meth:`FlushRunner.run_once` reconciles every newly
claimed partition before planning it: ``PartitionStage.recover()``
collects legacy ``flushed/`` markers (bounded pages) and returns each
surviving ``prepared/`` entry, which is reconciled by receipt lookup —
a 200 receipt settles the range locally (zero republication), a 404
republishes the persisted bytes verbatim INSIDE the receipt horizon
(same idempotency key, so the server's receipt dedupes a landing replay
into one logical commit), and a 404 PAST the horizon halts loudly: the
receipt may have been purged server-side
(``HOGLAKE_RECEIPT_RETENTION_SECONDS``, default 7 d), and replaying
could duplicate an already-executed commit
(``MILLRACE_RECEIPT_HORIZON_S``, default 6 d — deliberately below;
``persisted_at`` on the v2+ ``prepared/`` envelope is the guard's
input, and a legacy v1 entry of unknown age is halt-safe). The current
envelope is v3: it additionally carries the idempotency key's
derivation inputs (``topic``, ``partition``), cross-checked against the
stage's own identity at recovery — an entry naming another partition is
foreign state, refused as corruption rather than replayed under it.

Junk ``event_time`` policy: the check applies to the decoded values of
the live partition spec's TEMPORAL source columns (a ``month(timestamp)``
spec makes ``timestamp`` the checked column). Out-of-window values are
quarantined (the original payload lands in ``poison/`` with the
consumer's durability discipline) or clamped to the window edge,
per config; staged rows are NEVER mutated. A spec without a temporal
source leaves the policy with nothing to check (a documented no-op).

Decoder: v1 is JSON objects behind the :class:`PayloadDecoder`
protocol (a registry/Avro decoder slots in later). Column values are
coerced per record with per-record failure routing: a record that
cannot decode or cast lands in ``poison/`` and never crashes the
flush. Strict JSON: the NaN/Infinity literals are refused at parse
time, and magnitude-overflow literals (``1e400`` parses to ``inf``
WITHOUT tripping the parse hook) are refused at coercion — nothing
non-finite reaches a float column or its stats, and integer values
outside a column's int64 (per-unit) range are refused the same way
rather than detonating the batch's Arrow build. Supported column types
are the scalars (boolean, the int widths, float/double, string,
binary-as-base64, uuid, decimal, date, time, and the timestamp family
from epoch ints in the column's own unit or strict ISO-8601 strings);
``json``/nested/``variant`` columns are a STARTUP refusal naming the
column, not a runtime surprise. Should a value still defeat the batch
build (encoder drift), the build isolates the guilty record and
quarantines it — one malformed record never loses the batch.

Sync/async discipline: pyhoglake is synchronous, so every client call
(table reads, uploads, commits, receipt lookups) runs on the flusher's
own single-worker executor via ``run_in_executor``; the asyncio loop
the flusher shares with the consumer is never blocked by hoglake I/O.
Clocks (``now_us``, ``monotonic``) and ``sleep`` are injected
throughout.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import importlib.metadata
import json
import logging
import math
import re
import struct
import time
import uuid as _uuid
from collections import deque
from collections.abc import Awaitable, Callable, Mapping, Sequence
from concurrent.futures import Executor, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from datetime import time as dtime
from decimal import Decimal, localcontext
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
from pyhoglake.types import columns_to_arrow_schema
from pyhoglake.upload import (
    Upload,
    perform_upload,
    resolve_concurrency,
    run_uploads,
    widen_io_threads_for,
)
from slatedb.uniffi import Error as _SlateError

from . import keyspace
from .config import Config, EventTimePolicy
from .keyspace import FlushedKey, OffsetRange, PreparedRequest
from .planner import (
    DEFAULT_COMPRESSION_RATIO,
    FlushDecision,
    FlushTrigger,
    KeyStats,
    PlannerKnobs,
    plan_flush,
)
from .stage import (
    PartitionStage,
    PoisonedRecord,
    RecoveredPrepared,
    StageClosedError,
    StageCorruptionError,
    StageGauges,
    StageManager,
    gauges_from_stats,
)

log = logging.getLogger(__name__)

_R = TypeVar("_R")

_MICROS_PER_SECOND: Final = 1_000_000
_MICROS_PER_DAY: Final = 86_400_000_000
_EPOCH_DATE: Final = date(1970, 1, 1)
# Naive-by-construction UTC wall time (ruff's DTZ001 refuses the bare
# constructor; the function that subtracts this takes naive-UTC inputs).
_EPOCH_DATETIME: Final = datetime.fromisoformat("1970-01-01T00:00:00")

# -- the flush's bounded quarantine vocabulary (the keyspace poison envelope
# stores these strings; the consumer's REASON_* constants cover the
# consume-time half) --
REASON_UNDECODABLE_PAYLOAD: Final = "undecodable_payload"
REASON_UNCASTABLE_COLUMN: Final = "uncastable_column"
REASON_EVENT_TIME_OUT_OF_WINDOW: Final = "event_time_out_of_window"
REASON_FLUSH_REFUSED: Final = "flush_refused"

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

# The ONLY answered refusal that may quarantine staged rows: a 422 whose
# error code is the reserved ``record_validation`` — the server judged the
# RECORDS, not the request. hoglake's commit path emits no such code today:
# footer-shipping means the server never opens the parquet, so every 422 it
# can produce is ``error="validation"`` over the REQUEST's shape (blank
# path, bad counts, stats naming a dead field id, partition arity, key
# reuse — CommitService.validateFiles' whole vocabulary) — a writer bug or
# a stale basis, never a bad row. This arm is therefore deliberately
# unreachable against the current wire, and every OTHER answered 4xx halts
# with the rows staged (fail-safe: an unknown 422 shape is never answered
# by deleting data). A future server-side per-record refusal lands here by
# emitting exactly this code.
_RECORD_VALIDATION_CODE: Final = "record_validation"

# How long the offsets line of a commit message may be, in bytes (the
# server stores `message` as unbounded text; this bound is millrace's own).
# Truncation keeps the FIRST and LAST range verbatim — the last range's end
# is the flush window's high-water mark, and recovery parses it back out.
_MESSAGE_OFFSETS_LIMIT: Final = 16384

# Transforms whose source values the junk event_time policy checks.
_TEMPORAL_TRANSFORMS: Final = frozenset({"year", "month", "day", "hour"})

_ORPHAN_URIS_LOGGED: Final = 20

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
    """Startup resolution/validation failed: the catalog, namespace, table,
    incarnation, the server's capability set or the destination's shape —
    fail-fast at boot, never a runtime surprise (docs/kafka-ingestion.md
    §Deployment)."""


class FlushHalted(FlushError):
    """The pipeline must stop loudly (viaduck lesson 3/4 posture):
    ``table_recreated``, a 410 below the expiry floor, an idempotency-key
    identity anomaly, the destination gone, an answered 4xx that is NOT
    a per-record validation (``commit_refused`` — the rows stay staged
    and the persisted entry stays for an operator; only a 422
    ``record_validation`` may quarantine, never a request-shape
    refusal), a settlement that disagrees with the scan, a fanout the
    commit bound cannot hold, or a persisted ``prepared/`` entry whose
    receipt 404s PAST the receipt horizon (replaying it could duplicate
    an already-executed commit — operator reconciliation, never a blind
    replay). ``reason`` is the bounded vocabulary for metrics;
    ``detail`` carries the operator-facing text (for a 410, the server's
    reconcile instructions)."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


class FlushPublishExhausted(FlushError):
    """A retry ladder ran out on an ambiguous or retryable failure. The
    persisted ``prepared/`` entry stays: the next sweep replays it
    (receipt lookup first). Contained per decision by the runner — the
    sweep is never wedged by one key."""


class DecodeError(ValueError):
    """One staged payload cannot become a row: not a JSON document/object,
    or a value that cannot be coerced to its catalog column. Per-record —
    the flush quarantines the record and moves on. ``reason`` is the
    poison-envelope vocabulary."""

    def __init__(self, message: str, *, reason: str) -> None:
        super().__init__(message)
        self.reason = reason


class UnsupportedColumnError(FlushStartupError):
    """The destination table carries a column type the v1 JSON decoder
    does not support (json / list / struct / map / variant). Named at
    startup, with the column."""


# -- counters ------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class FlushStats:
    """Cumulative flush counters (passive observability — nothing here
    feeds control flow; the operational surface reads a snapshot).
    ``commit_attempts`` counts every commit RPC including retried ones;
    ``commit_retries`` counts only the retried ones, split by cause."""

    flushes_committed: int = 0
    flushes_replayed: int = 0
    receipt_settlements: int = 0
    reprepares: int = 0
    decisions_quarantined: int = 0
    records_quarantined: int = 0
    rows_published: int = 0
    files_written: int = 0
    bytes_written: int = 0
    commit_attempts: int = 0
    commit_retries: int = 0
    conflict_retries: int = 0
    queue_retries: int = 0
    receipt_lookups: int = 0
    orphaned_uploads: int = 0
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


_INT_BOUNDS: Final = {
    "int8": (-(1 << 7), (1 << 7) - 1),
    "int16": (-(1 << 15), (1 << 15) - 1),
    "int": (-(1 << 31), (1 << 31) - 1),
    "long": (-(1 << 63), (1 << 63) - 1),
    "uint8": (0, (1 << 8) - 1),
    "uint16": (0, (1 << 16) - 1),
    "uint32": (0, (1 << 32) - 1),
    "uint64": (0, (1 << 64) - 1),
}

# Micros per one unit of the column type (and nanos-per-micro for ns).
_TIMESTAMP_UNITS: Final = {
    "timestamp_s": 1_000_000,
    "timestamp_ms": 1_000,
    "timestamp": 1,
    "timestamptz": 1,
}

# int64 bounds for a timestamp column's value IN ITS OWN UNIT. Anything
# wider dies later and worse: ``pa.array`` raises ``OverflowError`` on an
# out-of-int64 value at BATCH build time (one bad record killing the
# flush), and the s→ms storage cast (``_encode_schema``) raises
# ``ArrowInvalid`` on seconds that overflow milliseconds — so
# ``timestamp_s`` is bounded by the millisecond-representable range, the
# others by int64. The junk ``event_time`` window only covers the live
# spec's temporal partition sources; every OTHER temporal column is
# covered by exactly this check.
_INT64_BOUNDS: Final = (-(1 << 63), (1 << 63) - 1)
_TIMESTAMP_VALUE_BOUNDS: Final = {
    "timestamp_s": (-((1 << 63) // 1000), ((1 << 63) - 1) // 1000),
    "timestamp_ms": _INT64_BOUNDS,
    "timestamp": _INT64_BOUNDS,
    "timestamp_ns": _INT64_BOUNDS,
    "timestamptz": _INT64_BOUNDS,
}

_ISO_FRACTION: Final = re.compile(r"\.(\d+)")


def _reject_constant(raw: str) -> Any:
    """json.loads parse_constant hook: NaN/Infinity are not JSON."""
    raise DecodeError(
        f"{raw} is not valid JSON (strict mode: no NaN/Infinity)",
        reason=REASON_UNDECODABLE_PAYLOAD,
    )


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    raise DecodeError(
        f"expected a JSON boolean, got {type(value).__name__}",
        reason=REASON_UNCASTABLE_COLUMN,
    )


def _coerce_int(value: Any, type_name: str, lo: int, hi: int) -> int:
    # isinstance(bool) is isinstance(int) — exclude it explicitly.
    if isinstance(value, bool) or not isinstance(value, int):
        raise DecodeError(
            f"expected a JSON integer for {type_name}, got {type(value).__name__}",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    if not lo <= value <= hi:
        raise DecodeError(
            f"value {value} does not fit in {type_name}",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    return value


def _coerce_float(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DecodeError(
            f"expected a JSON number, got {type(value).__name__}",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    try:
        result = float(value)
    except OverflowError as e:
        # float(10**400): a JSON integer too large for a double.
        raise DecodeError(
            f"integer value does not fit in a double: {e}",
            reason=REASON_UNCASTABLE_COLUMN,
        ) from e
    if not math.isfinite(result):
        # json.loads("1e400") parses to inf WITHOUT tripping
        # parse_constant (that hook only sees the NaN/Infinity
        # literals): a magnitude-overflow literal is valid JSON syntax
        # carrying a value no double holds. Refuse it here, or inf
        # would land in the column and its stats.
        raise DecodeError(
            f"non-finite double ({result!r}): the JSON literal overflows "
            "a double (e.g. 1e400)",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    return result


def _coerce_str(value: Any) -> str:
    if isinstance(value, str):
        return value
    raise DecodeError(
        f"expected a JSON string, got {type(value).__name__}",
        reason=REASON_UNCASTABLE_COLUMN,
    )


def _coerce_b64(value: Any) -> bytes:
    if not isinstance(value, str):
        raise DecodeError(
            f"expected a base64 string for a binary column, got {type(value).__name__}",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    try:
        return base64.b64decode(value.encode("ascii"), validate=True)
    except (binascii.Error, UnicodeEncodeError) as e:
        raise DecodeError(
            f"invalid base64 for a binary column: {e}",
            reason=REASON_UNCASTABLE_COLUMN,
        ) from e


def _coerce_uuid_bytes(value: Any) -> bytes:
    if not isinstance(value, str):
        raise DecodeError(
            f"expected a UUID string, got {type(value).__name__}",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    try:
        return _uuid.UUID(value).bytes
    except ValueError as e:
        raise DecodeError(
            f"invalid UUID string {value!r}",
            reason=REASON_UNCASTABLE_COLUMN,
        ) from e


def _coerce_decimal(value: Any, type_params: dict[str, Any] | None) -> Decimal:
    """int or decimal string → Decimal, precision/scale checked (never
    rounded: a value that does not fit exactly is a decode failure, not a
    silent mutation). JSON floats are refused — a binary float is not the
    decimal the producer wrote."""
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise DecodeError(
            f"expected a JSON integer or decimal string, got {type(value).__name__}",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    params = type_params or {}
    precision = int(params["precision"])
    scale = int(params["scale"])
    try:
        d = Decimal(str(value))
    except ArithmeticError as e:
        raise DecodeError(
            f"invalid decimal {value!r}", reason=REASON_UNCASTABLE_COLUMN
        ) from e
    if not d.is_finite():
        raise DecodeError(
            f"non-finite decimal {value!r}", reason=REASON_UNCASTABLE_COLUMN
        )
    with localcontext() as ctx:
        ctx.prec = 60
        # value * 10^scale must be an integer, or accepting the value
        # would round it.
        scaled = d.scaleb(scale)
        if scaled != scaled.to_integral_value():
            raise DecodeError(
                f"decimal {value!r} has more than {scale} fractional digits; "
                "refusing to round",
                reason=REASON_UNCASTABLE_COLUMN,
            )
        digits = len(scaled.to_integral_value().as_tuple().digits)
    if digits > precision:
        raise DecodeError(
            f"decimal {value!r} exceeds precision {precision}",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    return d


def _coerce_date(value: Any) -> date:
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as e:
            raise DecodeError(
                f"invalid ISO-8601 date {value!r}",
                reason=REASON_UNCASTABLE_COLUMN,
            ) from e
    if isinstance(value, bool) or not isinstance(value, int):
        raise DecodeError(
            f"expected an ISO-8601 date string or epoch-days integer, got "
            f"{type(value).__name__}",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    try:
        return _EPOCH_DATE + timedelta(days=value)
    except OverflowError as e:
        raise DecodeError(
            f"epoch-days value {value} is out of range",
            reason=REASON_UNCASTABLE_COLUMN,
        ) from e


def _time_to_micros(t: dtime) -> int:
    return (
        t.hour * 3600 + t.minute * 60 + t.second
    ) * _MICROS_PER_SECOND + t.microsecond


def _coerce_time_micros(value: Any) -> int:
    if isinstance(value, str):
        try:
            t = dtime.fromisoformat(value)
        except ValueError as e:
            raise DecodeError(
                f"invalid ISO-8601 time {value!r}",
                reason=REASON_UNCASTABLE_COLUMN,
            ) from e
        if t.tzinfo is not None:
            raise DecodeError(
                f"time column does not take a timezone: {value!r}",
                reason=REASON_UNCASTABLE_COLUMN,
            )
        return _time_to_micros(t)
    if isinstance(value, bool) or not isinstance(value, int):
        raise DecodeError(
            f"expected an ISO-8601 time string or micros-since-midnight "
            f"integer, got {type(value).__name__}",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    if not 0 <= value < _MICROS_PER_DAY:
        raise DecodeError(
            f"micros-since-midnight {value} is out of range",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    return value


def _datetime_to_micros(dt: datetime) -> int:
    """Exact integer micros since the epoch (a naive datetime is read as
    UTC wall time — the tz-aware caller subtracts its offset first)."""
    delta = dt - _EPOCH_DATETIME
    return (
        delta.days * _MICROS_PER_DAY
        + delta.seconds * _MICROS_PER_SECOND
        + delta.microseconds
    )


def _coerce_timestamp(value: Any, type_name: str, *, tz: bool) -> int:
    """An instant as an integer in the column's own unit (epoch seconds
    for ``timestamp_s``, millis, micros — ``timestamptz`` is micros — and
    nanos for ``timestamp_ns``), or a strict ISO-8601 string.

    Strings never silently lose precision: a sub-unit fraction is refused
    outright (a timestamp_s column takes only whole seconds from text),
    and sub-microsecond digits are refused even for timestamp_ns (the
    integer form is the exact one). Naive strings feed naive columns and
    offset-carrying strings feed ``timestamptz``; the mismatches are
    refused rather than reinterpreted. The result — from either form —
    must fit the column's unit in int64 (``timestamp_s`` in stored
    milliseconds): a magnitude outside it is a coercion failure, never
    a batch-killing ``OverflowError`` at Arrow build time."""
    if isinstance(value, bool):
        raise DecodeError(
            "expected a timestamp, got a JSON boolean",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    if isinstance(value, int):
        result = value
    elif not isinstance(value, str):
        raise DecodeError(
            f"expected an epoch integer or ISO-8601 string for {type_name}, "
            f"got {type(value).__name__}",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    else:
        fraction = _ISO_FRACTION.search(value)
        if (
            fraction is not None
            and len(fraction.group(1)) > 6
            and any(d != "0" for d in fraction.group(1)[6:])
        ):
            raise DecodeError(
                f"ISO-8601 timestamp {value!r} has sub-microsecond digits; "
                "use the integer form for nanosecond precision",
                reason=REASON_UNCASTABLE_COLUMN,
            )
        try:
            dt = datetime.fromisoformat(value)
        except ValueError as e:
            raise DecodeError(
                f"invalid ISO-8601 timestamp {value!r}",
                reason=REASON_UNCASTABLE_COLUMN,
            ) from e
        if tz:
            if dt.tzinfo is None:
                raise DecodeError(
                    f"timestamptz requires an offset ('Z' or '+hh:mm'): {value!r}",
                    reason=REASON_UNCASTABLE_COLUMN,
                )
            offset = dt.utcoffset()
            assert offset is not None  # tzinfo present ⇒ offset computable
            micros = _datetime_to_micros(dt.replace(tzinfo=None) - offset)
        else:
            if dt.tzinfo is not None:
                raise DecodeError(
                    f"naive {type_name} does not take an offset: {value!r}",
                    reason=REASON_UNCASTABLE_COLUMN,
                )
            micros = _datetime_to_micros(dt)
        if type_name == "timestamp_ns":
            result = micros * 1000
        else:
            unit_us = _TIMESTAMP_UNITS[type_name]
            if micros % unit_us:
                raise DecodeError(
                    f"timestamp {value!r} is finer than {type_name}'s unit; "
                    "refusing to truncate",
                    reason=REASON_UNCASTABLE_COLUMN,
                )
            result = micros // unit_us
    lo, hi = _TIMESTAMP_VALUE_BOUNDS[type_name]
    if not lo <= result <= hi:
        raise DecodeError(
            f"{type_name} value {result} is outside the int64-representable "
            f"range for its unit [{lo}, {hi}]",
            reason=REASON_UNCASTABLE_COLUMN,
        )
    return result


def _coercer_for(column: Column) -> Callable[[Any], Any]:
    t = column.type
    if t == "boolean":
        return _coerce_bool
    if t in _INT_BOUNDS:
        lo, hi = _INT_BOUNDS[t]
        return lambda v: _coerce_int(v, t, lo, hi)
    if t in ("float", "double"):
        return _coerce_float
    if t == "string":
        return _coerce_str
    if t == "binary":
        return _coerce_b64
    if t == "uuid":
        return _coerce_uuid_bytes
    if t == "decimal":
        # Eager shape validation: a decimal column whose type_params are
        # missing or unbuildable (precision outside decimal128's 1..38) is
        # a STARTUP refusal, not a per-record failure — otherwise the
        # per-record coercion KeyErrors (or the Arrow schema build raises)
        # at flush time, wedging the key on a destination defect.
        params = column.type_params or {}
        try:
            pa.decimal128(int(params["precision"]), int(params["scale"]))
        except (KeyError, TypeError, ValueError) as e:
            raise UnsupportedColumnError(
                f"column {column.name!r} is decimal but its type_params "
                f"({column.type_params!r}) do not build a decimal128: {e}"
            ) from e
        return lambda v: _coerce_decimal(v, column.type_params)
    if t == "date":
        return _coerce_date
    if t == "time":
        return _coerce_time_micros
    if t in ("timestamp_s", "timestamp_ms", "timestamp", "timestamp_ns"):
        return lambda v: _coerce_timestamp(v, t, tz=False)
    if t == "timestamptz":
        return lambda v: _coerce_timestamp(v, t, tz=True)
    raise UnsupportedColumnError(
        f"column {column.name!r} has type {t!r}, which the v1 JSON decoder "
        "does not support (supported: boolean, the int widths, float/double, "
        "string, binary, uuid, decimal, date, time and the timestamp family; "
        "json, nested and variant columns need a later decoder)"
    )


class JsonObjectDecoder:
    """v1 decoder: staged payloads are JSON objects; keys are column
    names. Defensive per record (the flush quarantines failures, never
    crashes): strict JSON (no NaN/Infinity literals — and no
    magnitude-overflow literals: ``1e400`` parses to ``inf`` without
    tripping the parse hook, and the float coercion refuses anything
    non-finite), per-column coercion with int64-per-unit bounds,
    missing nullable keys → null, missing/explicit-null non-nullable
    keys → failure, unknown keys dropped (counted and logged by the
    flusher)."""

    def __init__(self, columns: tuple[Column, ...] | Sequence[Column]) -> None:
        self._columns = tuple(columns)
        self._plans = tuple((c, _coercer_for(c)) for c in self._columns)
        self._names = frozenset(c.name for c in self._columns)

    @property
    def columns(self) -> tuple[Column, ...]:
        return self._columns

    def decode_record(self, payload: bytes) -> DecodedRecord:
        try:
            doc = json.loads(payload, parse_constant=_reject_constant)
        except DecodeError:
            raise
        except (ValueError, RecursionError) as e:
            # ValueError: not JSON (and the int-digit limit, and invalid
            # UTF-8 — both ValueError subclasses). RecursionError: a
            # payload nested past the interpreter limit — the C scanner
            # reports it that way, and it is just as per-record as a
            # syntax error.
            raise DecodeError(
                f"payload is not a JSON document: {e}",
                reason=REASON_UNDECODABLE_PAYLOAD,
            ) from e
        if not isinstance(doc, dict):
            raise DecodeError(
                f"payload is a JSON {type(doc).__name__}, not an object",
                reason=REASON_UNDECODABLE_PAYLOAD,
            )
        values: dict[str, Any] = {}
        for column, coerce in self._plans:
            raw = doc.get(column.name)
            if raw is None:
                if not column.nullable:
                    raise DecodeError(
                        f"column {column.name!r} is not nullable and the "
                        "payload has no usable value",
                        reason=REASON_UNCASTABLE_COLUMN,
                    )
                values[column.name] = None
            else:
                values[column.name] = coerce(raw)
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
        days: int = (value - _EPOCH_DATE).days
        return days * _MICROS_PER_DAY
    v = int(value)
    if col_type == "timestamp_ns":
        return v // 1000
    return v * _TIMESTAMP_UNITS[col_type]


def _micros_to_value(col_type: str, micros: int) -> Any:
    if col_type == "date":
        return _EPOCH_DATE + timedelta(days=micros // _MICROS_PER_DAY)
    if col_type == "timestamp_ns":
        return micros * 1000
    return micros // _TIMESTAMP_UNITS[col_type]


def _temporal_sources(info: TableInfo) -> tuple[tuple[Any, Column], ...]:
    """The live spec's temporal partition sources as (field, column)
    pairs — the columns the junk event_time policy checks. Startup
    validation has already refused nested or unknown sources."""
    spec = info.partition_spec
    if spec is None or not spec.fields:
        return ()
    columns = {c.field_id: c for c in info.columns}
    return tuple(
        (field, columns[field.source_field_id])
        for field in spec.fields
        if field.transform in _TEMPORAL_TRANSFORMS and field.source_field_id in columns
    )


# -- Arrow building -------------------------------------------------------------


def _array_for(column: Column, arrow_type: pa.DataType, values: list[Any]) -> pa.Array:
    """One column's Arrow array from coerced values, at exactly the type
    ``columns_to_arrow_schema`` gives the column (so ``Table.from_arrays``
    against that schema needs no cast)."""
    if column.type == "uuid":
        # pa.uuid() is an extension over fixed_size_binary(16); build the
        # storage and wrap it (pyarrow >= 21 stamps the parquet UUID
        # annotation hoglake's wire form requires).
        storage = pa.array(values, type=pa.binary(16))
        if isinstance(arrow_type, pa.BaseExtensionType):
            return pa.ExtensionArray.from_storage(arrow_type, storage)
        return storage
    if column.type == "timestamptz":
        # int64 micros → naive micros → tz-aware (a direct int → tz cast
        # is not a kernel pyarrow offers).
        return (
            pa.array(values, type=pa.int64()).cast(pa.timestamp("us")).cast(arrow_type)
        )
    if column.type == "time":
        return pa.array(values, type=pa.int64()).cast(arrow_type)
    return pa.array(values, type=arrow_type)


def _encode_schema(schema: pa.Schema) -> pa.Schema:
    """The parquet-encoding schema: the catalog schema with field ids,
    plus the one storage conversion pyhoglake's writer path makes —
    Parquet has no seconds timestamp unit, so ``timestamp_s`` is stored
    as milliseconds (mirrors ``Table._prepare_append``)."""
    return pa.schema(
        [
            field.with_type(pa.timestamp("ms"))
            if field.type == pa.timestamp("s")
            else field
            for field in schema
        ],
        metadata=schema.metadata,
    )


def _build_arrow(
    records: Sequence[DecodedRecord], columns: tuple[Column, ...]
) -> pa.Table:
    """Column-aligned records → the Arrow table at the catalog's schema
    (field ids embedded). Rows keep SCAN order here — (Kafka record
    timestamp, offset) within the team; :func:`build_prepared_plan` then
    sorts the batch by the live catalog sort spec (payload event time)
    before the partition fanout — the sort, not the scan order, is what
    the ingest-sort / catalog-sort-spec convergence rests on
    (docs/kafka-ingestion.md §What the server owns)."""
    schema = columns_to_arrow_schema(list(columns))
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


def _spec_sort_keys(info: TableInfo) -> list[tuple[str, str, str]]:
    """The table's LIVE sort spec as Arrow sort keys — ``(column name,
    direction, null placement)`` per field, in spec order; ``[]`` for an
    unsorted table.

    A field that does not resolve to a live top-level column, or carries
    a direction/null_order outside the wire vocabulary, is a shape this
    writer cannot honor — refused LOUDLY (the same posture as the
    partition-spec guard below), never silently unsorted: an unsorted
    file under a sorted spec is exactly what compaction's sortedness
    check demotes, so "looked odd, skipped it" would tax every file
    forever. Startup validation (:meth:`HoglakeFlusher.resolve`) refuses
    the same shapes fail-fast; this guard covers a spec that changed
    under a warm shape cache.
    """
    spec = info.sort_spec
    if spec is None or not spec.fields:
        return []
    columns = {c.field_id: c for c in info.columns}
    keys: list[tuple[str, str, str]] = []
    for field in spec.fields:
        column = columns.get(field.source_field_id)
        if column is None:
            raise FlushError(
                f"sort spec (sort_id={spec.sort_id}) references field_id "
                f"{field.source_field_id}, which is not a live top-level "
                f"column of {info.namespace}.{info.name} (a struct leaf is "
                "legal on the wire but this writer has no nested columns — "
                "startup refuses those)"
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
        keys.append((column.name, direction, null_order))
    return keys


def _sort_batch_by_spec(data: pa.Table, keys: list[tuple[str, str, str]]) -> pa.Table:
    """Sort the encoded batch by the catalog's sort keys — native Arrow
    compute (``pc.sort_indices`` + ``take``; ``take`` preserves the
    schema's field-id metadata).

    Extension-typed columns (``uuid``) have no Arrow sort kernel, so they
    sort on their STORAGE via a shadow table — fixed-size binary compares
    unsigned-lexicographic in Arrow exactly as the server comparator reads
    the same physical bytes (ParquetRewriter's BINARY/FIXED_LEN_BYTE_ARRAY
    arm), so the orders agree. NaN cannot appear (the decoder refuses
    non-finite floats), so Arrow's NaN placement never diverges from the
    server's NaN-greatest rule here.
    """
    shadow = data
    for name, _, _ in keys:
        index = shadow.schema.get_field_index(name)
        field = shadow.schema.field(index)
        if isinstance(field.type, pa.BaseExtensionType):
            chunked: pa.ChunkedArray = shadow.column(index)
            storage = pa.chunked_array(
                [chunk.storage for chunk in chunked.chunks],
                type=field.type.storage_type,
            )
            shadow = shadow.set_column(
                index, field.with_type(field.type.storage_type), storage
            )
    return data.take(pc.sort_indices(shadow, sort_keys=keys))


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
    caller. A null source value forms its own group, per Iceberg.
    """
    columns = {c.field_id: c for c in info.columns}
    key_names = [f"__millrace_pk_{i}" for i in range(len(spec.fields))]
    key_arrays: list[pa.Array] = []
    for field in spec.fields:
        column = columns.get(field.source_field_id)
        if column is None:  # startup validation refuses this; re-refuse loudly
            raise FlushError(
                f"partition spec (spec_id={spec.spec_id}) references field_id "
                f"{field.source_field_id}, which is not a live column of "
                f"{info.namespace}.{info.name}"
            )
        key_arrays.append(
            transform_strings(
                field.transform,
                field.transform_param,
                partition_source_array(data, [column]),
                column.type,
                column.type_params,
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
    team_id: int,
    trigger: FlushTrigger,
    records: int,
    quarantined: int,
    files: int,
    partitions: int,
    staged_bytes: int,
    arrow_bytes: int,
    topic: str,
    partition: int,
    offset_ranges: Sequence[OffsetRange],
    version: str,
    limit: int = _MESSAGE_OFFSETS_LIMIT,
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
    the offset ranges (from the staged ``offsets/`` record, clipped to
    the flushed window). Recovery parses the window's bounds back out of
    a persisted request's message (``parse_message_offsets``) — the
    ``prepared/`` value envelope holds only the request body, so the
    message is where the window lives. Truncation therefore keeps the
    FIRST and LAST range verbatim (the window's exact min and max) and
    elides only middle ranges, which are forensic-only.
    """
    summary = (
        f"millrace=v1 table={table_uuid} team={team_id} trigger={trigger} "
        f"records={records} quarantined={quarantined} files={files} "
        f"partitions={partitions} staged_bytes={staged_bytes} "
        f"arrow_bytes={arrow_bytes} millrace={_message_token(version)}"
    )
    head = f"offsets topic={_message_token(topic)} partition={partition} ranges="
    parts = [f"{r.first_offset}-{r.last_offset}" for r in offset_ranges]
    whole = head + ",".join(parts)
    if len(whole.encode()) <= limit or len(parts) <= 2:
        return f"{summary}\n{whole}"
    # The marker holds no space either: the line survives whitespace
    # tokenization and the parser keys on the "..." prefix.
    elided = f"{parts[0]},...(+{len(parts) - 2}_elided),{parts[-1]}"
    # The floor case (a first+last pair that still exceeds the limit —
    # a giant topic name) emits anyway: the window's bounds must survive.
    return f"{summary}\n{head}{elided}"


def _message_token(value: Any) -> str:
    """One ``key=value`` value, guaranteed to hold no space (millpond's
    rule: a space inside a value moves every pair after it for anything
    reading the line by splitting on whitespace)."""
    text = "".join("_" if c.isspace() else c for c in str(value))
    return text or "unknown"


_OFFSETS_LINE: Final = re.compile(
    r"^offsets topic=(?P<topic>\S+) partition=(?P<partition>\d+) "
    r"ranges=(?P<ranges>\S+)$"
)
_OFFSET_RANGE: Final = re.compile(r"^(?P<first>\d+)-(?P<last>\d+)$")


def parse_message_offsets(
    message: str, *, topic: str, partition: int
) -> tuple[int, int]:
    """The flush window ``(first_offset, last_offset)`` back out of a
    persisted request's commit message.

    Strict by construction: the format above is millrace's own, and a
    persisted body whose message does not parse is corruption (foreign
    writes or a bug), refused loudly rather than guessed at. The first
    range's start and the LAST range's end are the window's bounds —
    truncation elides only middle ranges, so they always survive.
    Staged offsets are non-negative (the consumer quarantines anything
    else), which is what makes the ``f-l`` spelling unambiguous."""
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
        bounds: list[tuple[int, int]] = []
        for part in match.group("ranges").split(","):
            if part.startswith("..."):
                continue  # the elision marker; middle ranges are forensic
            rng = _OFFSET_RANGE.match(part)
            if rng is None:
                raise StageCorruptionError(
                    f"unparseable offset range {part!r} in persisted commit message"
                )
            bounds.append((int(rng.group("first")), int(rng.group("last"))))
        if not bounds:
            raise StageCorruptionError(
                "persisted commit message carries no offset ranges"
            )
        return bounds[0][0], bounds[-1][1]
    raise StageCorruptionError(
        "persisted prepared request has no offsets line in its commit message"
    )


# -- the pure request builder -----------------------------------------------------

#: What one flush becomes on the wire.
FlushOutcome = Literal[
    "committed", "replayed", "receipt_settled", "reprepare", "quarantined", "empty"
]


def _covered_window(
    scanned_offsets: Sequence[int], ranges: Sequence[OffsetRange]
) -> tuple[int, int]:
    """The flush window ``(first_offset, last_offset)`` of a TRUNCATED
    scan, narrowed to what the scan actually covered (M4).

    A flush publishes exactly the rows the settlement's offset window
    will delete (``keyspace.row_in_flushed_range``), so the window must
    satisfy: every staged row of the team with offset in the window WAS
    scanned. A capped scan is a prefix in ``(timestamp, offset)`` key
    order, and Kafka record timestamps are not offset-monotone, so the
    naive ``[min, max]`` of scanned offsets can span an UNSCANNED row
    (an inversion straddling the cap boundary) — settling that window
    would delete a row that was never published (the guard would halt;
    the narrowing avoids the wedge on real catch-up data). The caller
    narrows ONLY when a cap actually cut the scan short: an untruncated
    scan saw the team's whole staged slice, and its own offset span is
    the window.

    The ``offsets/`` record holds one ``[first, last]`` span per staged
    batch per team (arrival structure — gaps between a batch's own
    offsets belong to other teams and are NOT representable), so the
    record is a sound OVER-approximation of the team's staged-offset
    set: every staged offset lies inside some span, but a span's
    interior can name offsets that are not this team's rows. The walk
    treats a span interior the scan missed as a hole either way — sound
    (the window never covers an unscanned row), and for interleaved
    teams potentially tighter than the true covered prefix; the steady
    state never truncates, so this conservatism is confined to catch-up
    scans under timestamp inversion.

    The walk: the window is ``(m, K)`` where ``m`` is the smallest
    scanned offset and ``K`` the largest offset such that every
    in-record offset in ``[m, K]`` is scanned — ranges are walked
    against the sorted scan; a range fully covered is skipped
    arithmetically, a range with a hole ends the window at the hole.
    Runs of offsets belonging to other teams or to quarantined records
    (gaps BETWEEN ranges) need no coverage.

    Pure; deterministic in its inputs (the window's identity comes from
    the SCANNED rows, never the stats snapshot). ``scanned_offsets``
    must be non-empty.
    """
    offsets = sorted(set(scanned_offsets))
    if not offsets:
        raise ValueError("_covered_window requires a non-empty scan")
    m, big_m = offsets[0], offsets[-1]
    i = 0  # offsets[i] is the smallest scanned offset not yet matched
    n = len(offsets)
    for r in ranges:
        a = max(r.first_offset, m)
        b = min(r.last_offset, big_m)
        if a > b:
            continue
        span = b - a + 1
        if i + span <= n and offsets[i] == a and offsets[i + span - 1] == b:
            # span distinct sorted integers with both endpoints matching
            # inside [a, b] — the whole range is covered; skip it.
            i += span
            continue
        # A hole: the first staged offset of this range the scan missed
        # ends the window (the offset just below it was covered).
        x = a
        while x <= b and i < n and offsets[i] == x:
            i += 1
            x += 1
        return m, x - 1
    return m, big_m


@dataclass(frozen=True, slots=True)
class FlushContext:
    """The per-decision constants one prepared request is built from.

    ``offset_ranges`` is the staged ``offsets/`` record clipped to the
    flushed window (the message's identity material). Nothing here reads
    a clock: the junk window arrives precomputed in
    :func:`build_prepared_plan`'s arguments, so the request's bytes are a
    pure function of the staged rows, the table basis and the window.
    """

    topic: str
    partition: int
    team_id: int
    trigger: FlushTrigger
    first_offset: int
    last_offset: int
    offset_ranges: tuple[OffsetRange, ...]
    idempotency_key: str
    read_snapshot: int
    table_uuid: str


@dataclass(frozen=True, slots=True)
class PreparedPlan:
    """The pure half of a flush: the complete commit request and the
    parquet it registers, built without a single I/O.

    ``payload`` is None exactly when every scanned row was quarantined
    (nothing may be committed; the decision settles as quarantined).
    ``uploads`` are the buffers to write; ``uris`` names every one of
    them in input order for orphan accounting. ``quarantined`` holds the
    poison entries to persist BEFORE publication."""

    payload: dict[str, Any] | None
    uploads: tuple[Upload, ...]
    uris: tuple[str, ...]
    quarantined: tuple[PoisonedRecord, ...]
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
    clocks, no randomness — the same inputs produce byte-identical output
    in any process.

    Deterministic object names: ``{data_path}/data/{ns}/{table}/
    {idempotency_key}/{index}.parquet``. The key derives from
    ``(table_uuid, topic, partition, team_id, first_offset,
    last_offset)``, so the same staged range names the same objects
    forever — and two partitions flushing one team over the same offset
    range name DIFFERENT objects (offsets are per partition; PR #331
    review) — and a rebuild after a crash between upload and persist
    rewrites the SAME names instead of doubling the orphans.

    The sort: scan order is Kafka-record time; the catalog's sort spec
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
    quarantined: list[PoisonedRecord] = []
    dropped_keys: set[str] = set()

    def poison(row_key: keyspace.RowKey, payload: bytes, reason: str) -> None:
        # The WHOLE payload, never truncated: the staged row is deleted
        # at settlement, so the poison entry is the only surviving copy —
        # a flush-time quarantine is not the consumer's forensic cap
        # (PR #331 review).
        quarantined.append(
            PoisonedRecord(
                offset=row_key.offset,
                reason=reason,
                key=None,
                value=payload,
                value_bytes_original=len(payload),
            )
        )

    for row_key, payload_bytes in records:
        try:
            record = decoder.decode_record(payload_bytes)
        except DecodeError as e:
            poison(row_key, payload_bytes, e.reason)
            continue
        dropped_keys.update(record.dropped_keys)
        junked = False
        for _field, column in temporal:
            value = record.values[column.name]
            if value is None:
                continue
            value_us = _value_to_micros(column.type, value)
            if window.contains(value_us):
                continue
            if event_time_policy is EventTimePolicy.QUARANTINE:
                poison(row_key, payload_bytes, REASON_EVENT_TIME_OUT_OF_WINDOW)
                junked = True
                break
            # clamp mode: rewrite the DECODED value to the window edge —
            # the staged row is never mutated.
            clamped = min(max(value_us, window.min_us), window.max_us)
            record.values[column.name] = _micros_to_value(column.type, clamped)
        if junked:
            continue
        good.append((row_key, payload_bytes, record))

    # The batch build. Coercion is total — every per-record failure above
    # is a DecodeError and landed in the quarantine — so a failure HERE is
    # encoder drift (a coercion that under-refuses). Isolate the guilty
    # record and quarantine it rather than losing the whole batch to one
    # value (one malformed record must never wedge the flush).
    table, encoded, build_guilty = _build_batch_isolating(good, decoder.columns)
    for bad_key, bad_payload, _bad_record in build_guilty:
        poison(bad_key, bad_payload, REASON_UNCASTABLE_COLUMN)

    if not good:
        return PreparedPlan(
            payload=None,
            uploads=(),
            uris=(),
            quarantined=tuple(quarantined),
            records=0,
            files=0,
            partition_tuples=0,
            staged_bytes=staged_bytes,
            arrow_bytes=0,
            dropped_keys=tuple(sorted(dropped_keys)),
        )

    # The catalog's sort spec is the file's contract (BINDING for
    # compaction; SortednessCheck demotes a file at the first inversion).
    # Scan order is Kafka-record time and the spec is payload-event time,
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
        team_id=ctx.team_id,
        trigger=ctx.trigger,
        records=sum(part.num_rows for _, part in groups),
        quarantined=len(quarantined),
        files=len(groups),
        partitions=sum(1 for values, _ in groups if values is not None),
        staged_bytes=staged_bytes,
        arrow_bytes=table.nbytes,
        topic=ctx.topic,
        partition=ctx.partition,
        offset_ranges=ctx.offset_ranges,
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
            if not prepared_schema_matches(parquet.schema_arrow, encode_schema):
                # Unreachable while the decoder and the schema builder
                # agree; a failure here is a bug in THIS process, so the
                # decision raises (contained, loud) rather than quarantines.
                raise FlushError(
                    "encoded parquet schema/field ids differ from the "
                    "destination schema — a millrace encoder bug"
                )
            metadata = parquet.metadata
        uri = f"{base}/{ctx.idempotency_key}/{index}.parquet"
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
        quarantined=tuple(quarantined),
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
    earlier process published them) and for quarantines."""

    topic: str
    partition: int
    team_id: int
    trigger: FlushTrigger | Literal["recovery"]
    outcome: FlushOutcome
    first_offset: int
    last_offset: int
    records: int = 0
    files: int = 0
    quarantined: int = 0
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


def _is_record_validation_refusal(exc: HoglakeError) -> bool:
    """True iff this answered refusal is a 422 whose error code is the
    reserved ``record_validation`` — the server judged RECORDS, not the
    request, which is the ONLY refusal that may quarantine staged rows
    (PR #331 review: the pre-fix shape quarantined the whole window on
    ANY answered 4xx — a proxy's 401/403, an ingress 413, a transient
    400, a request-shape ``validation`` 422 from an operator's mistaken
    schema change — truncating to the poison cap and deleting the rows).

    Precise by construction: pyhoglake maps 422 → ``ValidationError``
    with ``message`` = the body's ``error`` field, so the match is
    (a ``ValidationError``, status 422, code exactly
    ``record_validation``) — never a substring of prose. hoglake's
    commit path emits no such code today (the server never opens the
    parquet, so its 422s are all request-shape), so this is deliberately
    unreachable on the current wire; an unknown 422 shape fails safe
    (the caller halts)."""
    return (
        isinstance(exc, ValidationError)
        and exc.status_code == 422
        and exc.message == _RECORD_VALIDATION_CODE
    )


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
    ) -> HoglakeFlusher:
        """Resolve the catalog → namespace → table and validate the
        destination, fail-fast (docs/kafka-ingestion.md §Deployment:
        resolve catalog, table, incarnation; refuse on mismatch).

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
        )
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
        flusher._catalog = catalog
        flusher._table = Table(Namespace(catalog, cfg.namespace), info)
        flusher._table_uuid = info.table_uuid
        flusher._shape_info = info
        flusher._shape_read_at = flusher._monotonic()
        flusher._shape_fingerprint = _shape_fingerprint(info)
        # Startup validation: the decoder supports every column, and the
        # live partition spec is one this writer can honor — top-level
        # sources (v1) with transforms pyhoglake accepts for the source
        # type. `transform_value(..., None)` is the validity dry-run: it
        # checks the vocabulary, the param and the temporal-type gate
        # without fabricating data.
        JsonObjectDecoder(info.columns)
        spec = info.partition_spec
        if spec is not None:
            flat = {c.field_id: c for c in info.columns}
            for field in spec.fields:
                column = flat.get(field.source_field_id)
                if column is None:
                    raise FlushStartupError(
                        f"partition spec (spec_id={spec.spec_id}) references "
                        f"field_id {field.source_field_id}, which is not a "
                        f"TOP-LEVEL column of {info.namespace}.{info.name}: "
                        "v1 supports top-level partition sources only"
                    )
                try:
                    transform_value(
                        field.transform,
                        field.transform_param,
                        column.type,
                        None,
                        column.type_params,
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
    ) -> None:
        if io_workers < 1:
            raise ValueError(f"io_workers must be >= 1, got {io_workers}")
        self._client = client
        self._cfg = cfg
        self._now_us = now_us
        self._sleep = sleep
        self._monotonic = monotonic
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
        self._shape_info: TableInfo | None = None
        self._shape_read_at: float | None = None
        self._retry_after: float | None = None
        self._warned_dropped: set[str] = set()
        # Counter attributes, incremented in place (consumer.py's
        # ConsumerStats pattern: plain ints, frozen snapshot on stats()).
        self._flushes_committed = 0
        self._flushes_replayed = 0
        self._receipt_settlements = 0
        self._reprepares = 0
        self._decisions_quarantined = 0
        self._records_quarantined = 0
        self._rows_published = 0
        self._files_written = 0
        self._bytes_written = 0
        self._commit_attempts = 0
        self._commit_retries = 0
        self._conflict_retries = 0
        self._queue_retries = 0
        self._receipt_lookups = 0
        self._orphaned_uploads = 0
        self._dropped_payload_keys = 0
        self._stats_repaired = 0
        self._receipt_horizon_cache: float | None = None
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

    def stats(self) -> FlushStats:
        """A snapshot of the run's counters."""
        return FlushStats(
            flushes_committed=self._flushes_committed,
            flushes_replayed=self._flushes_replayed,
            receipt_settlements=self._receipt_settlements,
            reprepares=self._reprepares,
            decisions_quarantined=self._decisions_quarantined,
            records_quarantined=self._records_quarantined,
            rows_published=self._rows_published,
            files_written=self._files_written,
            bytes_written=self._bytes_written,
            commit_attempts=self._commit_attempts,
            commit_retries=self._commit_retries,
            conflict_retries=self._conflict_retries,
            queue_retries=self._queue_retries,
            receipt_lookups=self._receipt_lookups,
            orphaned_uploads=self._orphaned_uploads,
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
        them. Conservative by construction: with deterministic object
        names, a same-window rebuild rewrites these exact keys, so the
        count is an upper bound on objects that STAY orphaned."""
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
        read = getattr(self._catalog, "_retention_seconds", None)
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

    # -- receipt horizon ---------------------------------------------------------

    def _receipt_horizon_s(self) -> float:
        """How old a persisted ``prepared/`` entry may be before a receipt
        404 becomes ambiguous with a PURGED receipt — the blind-replay
        guard (C3; the server purges receipts at
        ``HOGLAKE_RECEIPT_RETENTION_SECONDS``, default 7 d).

        The wire exposes no receipt retention (``CatalogOptions`` carries
        snapshot retention only), so the guard is the
        ``MILLRACE_RECEIPT_HORIZON_S`` knob (default 6 d — deliberately
        BELOW the server's default; config.py documents the assumption).
        A pyhoglake that grows ``Catalog._receipt_retention_seconds`` is
        honored when present (millpond's guarded-private-accessor
        posture, same as ``_retention_ttl_s``): a positive finite value
        is the horizon; ``inf`` means receipts are kept forever and there
        is no horizon; anything else (0, negative, absent, raising)
        falls back to the knob. Resolved once and cached — it is a
        per-process constant for the lifetime of a Catalog handle.
        """
        if self._receipt_horizon_cache is not None:
            return self._receipt_horizon_cache
        horizon = float(self._cfg.receipt_horizon_s)
        read = getattr(self._catalog, "_receipt_retention_seconds", None)
        if read is not None:
            try:
                value = read()
            except Exception:  # noqa: BLE001 - a horizon read must never fail recovery
                value = None
            if (
                isinstance(value, (int, float))
                and not isinstance(value, bool)
                and (value > 0 or value == float("inf"))
            ):
                horizon = float(value)
        self._receipt_horizon_cache = horizon
        return horizon

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
                    # the caller's quarantine arm decides.
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
        logical commit) — but only INSIDE the receipt horizon: past
        ``_receipt_horizon_s`` (or on an unknown-age legacy v1 envelope)
        a 404 is ambiguous with a PURGED receipt, and the pipeline halts
        rather than risk duplicating an already-executed commit.

        The flush window is parsed out of the persisted request's commit
        message — the ``prepared/`` envelope holds the request body, and
        the message is where the window lives. A v3 envelope additionally
        names its idempotency key's derivation inputs (``topic``,
        ``partition``): they must equal this stage's own identity — an
        entry that names another partition is foreign state, refused as
        corruption, never replayed under it (legacy v1/v2 envelopes carry
        no inputs; the check does not apply to them). The settle deletes
        EXACTLY the window (never younger rows of the same team), and the
        delete is guarded against the scan: a settlement that removes a
        row the scan did not see is a staging-invariant violation and
        halts the pipeline rather than losing the row silently.
        """
        team_id = pending.key.team_id
        key = pending.request.idempotency_key
        body = pending.request.body
        try:
            payload = json.loads(body)
        except (ValueError, UnicodeDecodeError) as e:
            raise StageCorruptionError(
                f"prepared/{team_id}/{pending.key.first_offset}: the "
                f"persisted body is not valid JSON: {e}"
            ) from e
        if not isinstance(payload, dict) or payload.get("idempotency_key") != key:
            raise StageCorruptionError(
                f"prepared/{team_id}/{pending.key.first_offset}: the "
                "persisted body does not parse to its own idempotency key"
            )
        # The v3 envelope's derivation inputs must name THIS partition
        # instance — an entry that doesn't is foreign state in this
        # SlateDB, and replaying it would publish under another
        # partition's identity. Legacy (v1/v2) envelopes carry no inputs;
        # the check simply does not apply to them.
        if pending.request.topic is not None and (
            pending.request.topic != stage.topic
            or pending.request.partition != stage.partition
        ):
            raise StageCorruptionError(
                f"prepared/{team_id}/{pending.key.first_offset}: the "
                f"envelope names {pending.request.topic}"
                f"[{pending.request.partition}] but this stage is "
                f"{stage.topic}[{stage.partition}] — foreign state, "
                "refused rather than replayed"
            )
        message = payload.get("message")
        if not isinstance(message, str):
            raise StageCorruptionError(
                f"prepared/{team_id}/{pending.key.first_offset}: the "
                "persisted request carries no commit message"
            )
        first_offset, last_offset = parse_message_offsets(
            message, topic=stage.topic, partition=stage.partition
        )
        if first_offset != pending.key.first_offset:
            raise StageCorruptionError(
                f"prepared/{team_id}/{pending.key.first_offset}: the "
                f"message's window starts at {first_offset} instead"
            )
        window = FlushedKey(team_id, first_offset, last_offset)
        # The window's rows as they stand now — the settlement guard's
        # count, and (on a 422) the quarantine's payloads.
        window_rows = [
            (row_key, payload_bytes)
            async for row_key, payload_bytes in stage.scan_team_rows(team_id)
            if keyspace.row_in_flushed_range(row_key, window)
        ]

        receipt = await self._io(partial(self._receipt_sync, key))
        if receipt is not None:
            report = await stage.commit_flushed(window)
            self._guard_settlement(window, len(window_rows), report.rows_deleted)
            self._receipt_settlements += 1
            log.info(
                "reconciled %s[%d] team %d offsets %d-%d from the commit "
                "receipt (snapshot %d) — zero republication",
                stage.topic,
                stage.partition,
                team_id,
                first_offset,
                last_offset,
                receipt.snapshot_id,
            )
            return FlushReport(
                topic=stage.topic,
                partition=stage.partition,
                team_id=team_id,
                trigger="recovery",
                outcome="receipt_settled",
                first_offset=first_offset,
                last_offset=last_offset,
                records=0,
                snapshot_id=receipt.snapshot_id,
                idempotency_key=key,
            )

        # No receipt. Republish the persisted bytes VERBATIM — but only
        # while the entry's age PROVES the receipt cannot be gone: the
        # server purges receipts past its retention
        # (HOGLAKE_RECEIPT_RETENTION_SECONDS, default 7 d), so past the
        # receipt horizon a 404 is ambiguous — "never landed" and
        # "landed, receipt purged" look identical, and republishing the
        # latter duplicates an executed commit. Unknown age (a legacy v1
        # envelope) is halt-safe too. A 200 receipt settles regardless of
        # age (it is ground truth); this guard covers only the blind
        # replay.
        horizon_s = await self._io(self._receipt_horizon_s)
        persisted_at = pending.request.persisted_at
        age_us = None if persisted_at is None else self._now_us() - persisted_at
        if age_us is None or age_us > horizon_s * _MICROS_PER_SECOND:
            age_text = (
                "unknown (a pre-v2 prepared/ envelope)"
                if age_us is None
                else f"{age_us / _MICROS_PER_SECOND:.0f}s"
            )
            raise self._halt(
                "receipt_horizon_exceeded",
                f"the persisted commit for {stage.topic}[{stage.partition}] "
                f"team {team_id} offsets {first_offset}-{last_offset} "
                f"(idempotency key {key}) has no receipt, and its age "
                f"({age_text}) is past the receipt horizon "
                f"({horizon_s:.0f}s): the commit may have landed and had "
                "its receipt purged, so replaying it could duplicate rows "
                "— and its uploaded objects may be live data, so they are "
                "NOT orphan-accounted. Reconcile the window against the "
                "catalog by hand, then drop the prepared/ entry.",
            )
        try:
            result = await self._publish(payload)
        except DdlSinceReadSnapshotError:
            # Definitive refusal, zero writes — the REBUILD case: the
            # persisted payload is dead, the rows are still staged, the
            # planner re-decides the key and re-prepares against a fresh
            # read. The upload it abandons is accounted.
            self._count_orphans(
                _payload_uris(payload),
                f"the persisted commit for team {team_id} was refused with "
                "ddl_since_read_snapshot; the rows re-prepare against a "
                "fresh read",
                reason="ddl_rebuild",
            )
            await stage.drop_prepared(team_id, first_offset)
            self._reprepares += 1
            return FlushReport(
                topic=stage.topic,
                partition=stage.partition,
                team_id=team_id,
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
            raise self._halt(
                "read_snapshot_expired",
                f"the persisted commit's read_snapshot fell below the "
                f"catalog's expiry floor: {e.detail or e}. Reconcile from a "
                "current read of the table; never silently skip.",
                _payload_uris(payload),
            ) from e
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
            if _is_record_validation_refusal(e):
                # The server judged the RECORDS (not the request): the
                # payload was judged, zero writes. Quarantine the
                # window's rows — WHOLE payloads, never capped — and
                # settle WITHOUT a commit (no receipt exists, so recovery
                # never looks for one). Recorded, counted, logged, never
                # wedging the loop.
                self._count_orphans(
                    _payload_uris(payload),
                    f"the persisted commit for team {team_id} was refused "
                    f"with {status} {_RECORD_VALIDATION_CODE}; the decision "
                    "is quarantined",
                    reason="commit_refused",
                )
                return await self._quarantine_window(
                    stage,
                    window,
                    window_rows,
                    key,
                    status,
                    _error_text(e),
                    trigger="recovery",
                )
            if status is not None and 400 <= status < 500:
                # Any OTHER answered 4xx (a proxy's 401/403, an ingress
                # 413, a plain 400, a request-shape "validation" 422):
                # nothing about it says the ROWS can never publish.
                # Halt with the rows staged and the entry persisted —
                # the operator reconciles — rather than truncate-and-delete
                # up to a whole window (PR #331 review).
                raise self._halt(
                    "commit_refused",
                    f"the persisted commit for {stage.topic}"
                    f"[{stage.partition}] team {team_id} offsets "
                    f"{first_offset}-{last_offset} (idempotency key {key}) "
                    f"was answered {status}: {_error_text(e)}. An answered "
                    "refusal that does not name a per-record validation "
                    "proves nothing about the rows — they stay staged and "
                    "the pipeline halts for an operator.",
                    _payload_uris(payload),
                ) from e
            raise

        report = await stage.commit_flushed(window)
        self._guard_settlement(window, len(window_rows), report.rows_deleted)
        self._flushes_replayed += 1
        log.info(
            "replayed the persisted commit for %s[%d] team %d offsets %d-%d "
            "(snapshot %d) — the receipt dedupes a landing replay into one "
            "logical commit",
            stage.topic,
            stage.partition,
            team_id,
            first_offset,
            last_offset,
            result.snapshot_id,
        )
        return FlushReport(
            topic=stage.topic,
            partition=stage.partition,
            team_id=team_id,
            trigger="recovery",
            outcome="replayed",
            first_offset=first_offset,
            last_offset=last_offset,
            records=_payload_records(payload),
            files=len(_payload_uris(payload)),
            snapshot_id=result.snapshot_id,
            idempotency_key=key,
        )

    # -- quarantine settlement ----------------------------------------------------

    async def _quarantine_window(
        self,
        stage: PartitionStage,
        window: FlushedKey,
        window_rows: list[tuple[keyspace.RowKey, bytes]],
        key: str | None,
        status: int | None,
        detail: str,
        *,
        trigger: FlushTrigger | Literal["recovery"],
    ) -> FlushReport:
        """Quarantine a whole flush decision — reachable ONLY from the
        ``record_validation`` 422 arm (the server judged the records, not
        the request; every other answered 4xx halts). Every row of the
        window lands in ``poison/`` with its ORIGINAL payload WHOLE — a
        flush-time quarantine is never capped: the staged rows are
        deleted at settlement, so the poison entry is the only surviving
        copy (the consumer's ``poison_value_max_bytes`` cap is the
        consume-time forensic path's, and does not apply here — PR #331
        review) — then the range settles WITHOUT a commit (no receipt
        exists, so recovery has nothing to reconcile). Recorded, counted,
        logged; the loop is never wedged."""
        await stage.poison_records(
            (
                PoisonedRecord(
                    offset=row_key.offset,
                    reason=REASON_FLUSH_REFUSED,
                    key=None,
                    value=payload,
                    value_bytes_original=len(payload),
                )
                for row_key, payload in window_rows
            ),
            # the purge only reaches v2 envelopes; an unversioned one is
            # forensic litter retained forever (see README retention)
            now_us=self._now_us(),
        )
        report = await stage.settle_quarantined(window)
        self._guard_settlement(window, len(window_rows), report.rows_deleted)
        self._decisions_quarantined += 1
        self._records_quarantined += len(window_rows)
        log.warning(
            "flush decision quarantined: %s[%d] team %d offsets %d-%d "
            "(%d rows to poison/) — the server refused the commit with %s: "
            "%s",
            stage.topic,
            stage.partition,
            window.team_id,
            window.first_offset,
            window.last_offset,
            len(window_rows),
            status,
            detail,
        )
        return FlushReport(
            topic=stage.topic,
            partition=stage.partition,
            team_id=window.team_id,
            trigger=trigger,
            outcome="quarantined",
            first_offset=window.first_offset,
            last_offset=window.last_offset,
            quarantined=len(window_rows),
            idempotency_key=key,
        )

    def _guard_settlement(self, window: FlushedKey, scanned: int, deleted: int) -> None:
        """The settlement must delete EXACTLY the rows the flush saw:
        every scanned row is in the window by construction, and no row
        the scan did not see may share it (staged offsets grow past a
        snapshot — stage.py's ordering argument). A disagreement is a
        staging-invariant violation — a shortfall leaves published rows
        to re-flush (a duplicate), an over-delete loses unpublished rows
        — so it halts the pipeline rather than proceeding."""
        if deleted != scanned:
            raise FlushHalted(
                "settlement_mismatch",
                f"settling team {window.team_id} offsets "
                f"{window.first_offset}-{window.last_offset} deleted "
                f"{deleted} rows but the flush saw {scanned}: the staging "
                "layer's ordering invariants are broken",
            )

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

        1. A persisted ``prepared/`` entry for the team — found by a
           PER-TEAM lookup, never keyed by the scan's first offset — is
           authoritative: it is replayed (receipt lookup first), never
           regenerated, and no new window is built while one stands.
           The per-team shape is what makes the bounded scan (below)
           safe: a capped scan's prefix minimum can move under
           timestamp-inverted arrivals, and keying the lookup by it
           could miss an outstanding entry and double-publish its
           window.
        2. The shape read (and with it the commit's ``read_snapshot``
           basis) is pinned BEFORE the staged range is scanned.
        3. The scan is BOUNDED (rows and bytes — M4); the flush window
           is derived from the SCANNED rows — their own offset span when
           the scan covered the team's slice, narrowed to the covered
           offset prefix (:func:`_covered_window`) only when a cap cut
           the scan short; one commit per ``(team, window)``; settlement
           strictly after the receipt, guarded against the scanned
           count.
        """
        team_id = decision.team_id
        for pending in await stage.list_prepared(team_id):
            # At most one entry per team by construction; reconcile in
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

        rows, truncated = await stage.scan_team_rows_bounded(
            team_id,
            max_rows=self._cfg.flush_scan_max_rows,
            max_bytes=self._scan_budget_bytes(),
        )
        if not rows:
            # The byte cap refuses a FIRST row larger than the budget;
            # an empty scan then is indistinguishable from a genuinely
            # empty team. Probe one row: a record that staged at all is
            # bounded by the broker's message limit, so one row is
            # always a flushable unit — wedging on it forever would be
            # the worse failure.
            rows = [
                (row_key, payload)
                async for row_key, payload in stage.scan_team_rows(team_id, max_rows=1)
            ]
            # The one-row probe is its own exact window below (the byte
            # cap refused this row, so it flushes alone).
            truncated = False
        if not rows:
            # A decided key scanning zero rows is the redelivery-inflation
            # residue (stage_batch is idempotent at the row level but its
            # stats merge inflates on redelivery; a flush settles ACTUALS
            # and the leftover counters survive). Left alone the stale
            # first_staged_ts pins the backpressure age gauge forever, so
            # the stats entry is repaired away — race-free inside
            # reconcile_stats' serializable transaction.
            repaired = await stage.reconcile_stats(team_id)
            if repaired:
                self._stats_repaired += 1
            log.warning(
                "flush decision for %s[%d] team %d scanned zero rows with a "
                "live stats entry (redelivery residue%s); nothing flushed",
                stage.topic,
                stage.partition,
                team_id,
                " — stats repaired" if repaired else "",
            )
            return FlushReport(
                topic=stage.topic,
                partition=stage.partition,
                team_id=team_id,
                trigger=decision.trigger,
                outcome="empty",
                first_offset=-1,
                last_offset=-1,
            )
        # The identity material: the offsets/ record (read AFTER the
        # scan snapshot, so it may name ranges staged mid-scan — those
        # sort past the window's span and clip away; stage.py's ordering
        # argument makes the record exact inside it), and the window
        # itself: [first_scanned_offset, last_scanned_offset] when the
        # scan covered the team's whole staged slice (the common case —
        # every staged row was seen, so settling the span deletes exactly
        # the published rows), narrowed to the covered prefix ONLY when a
        # scan cap cut the read short (a timestamp inversion straddling
        # the cap could otherwise settle a row the flush never saw).
        record = await stage.read_offsets(team_id)
        if not record:
            raise StageCorruptionError(
                f"team {team_id}: {len(rows)} staged rows but the offsets/ "
                "record names none of them"
            )
        scanned_offsets = [row_key.offset for row_key, _ in rows]
        if truncated:
            first_offset, last_offset = _covered_window(scanned_offsets, record)
            if last_offset < max(scanned_offsets):
                # A timestamp inversion straddling the scan cap: the rows
                # past the covered prefix stay staged and re-decide next
                # sweep (the window machinery handles arbitrary
                # sub-windows; this is the M4 catch-up path working).
                rows = [(rk, p) for rk, p in rows if rk.offset <= last_offset]
        else:
            first_offset = min(scanned_offsets)
            last_offset = max(scanned_offsets)
        ranges = tuple(
            OffsetRange(
                r.topic,
                r.partition,
                max(r.first_offset, first_offset),
                min(r.last_offset, last_offset),
            )
            for r in record
            if r.first_offset <= last_offset and r.last_offset >= first_offset
        )
        if not ranges:
            raise StageCorruptionError(
                f"team {team_id}: {len(rows)} staged rows but the offsets/ "
                "record names none of them"
            )

        key = keyspace.idempotency_key(
            self._table_uuid,
            stage.topic,
            stage.partition,
            team_id,
            first_offset,
            last_offset,
        )
        now_us = self._now_us()
        ctx = FlushContext(
            topic=stage.topic,
            partition=stage.partition,
            team_id=team_id,
            trigger=decision.trigger,
            first_offset=first_offset,
            last_offset=last_offset,
            offset_ranges=ranges,
            idempotency_key=key,
            read_snapshot=read_snapshot,
            table_uuid=self._table_uuid,
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

        # Quarantine BEFORE publication: a settlement never deletes a
        # staged row whose poison entry has not survived it.
        if plan.quarantined:
            written = await stage.poison_records(
                plan.quarantined, now_us=self._now_us()
            )
            self._records_quarantined += written
            for entry in plan.quarantined:
                log.warning(
                    "flush-time quarantine: %s[%d]@%d reason=%s",
                    stage.topic,
                    stage.partition,
                    entry.offset,
                    entry.reason,
                )

        if plan.payload is None:
            # Every row quarantined: nothing may be committed (a commit
            # registers at least one file). Settle WITHOUT a commit.
            window = FlushedKey(team_id, first_offset, last_offset)
            report = await stage.settle_quarantined(window)
            self._guard_settlement(window, len(rows), report.rows_deleted)
            self._decisions_quarantined += 1
            return FlushReport(
                topic=stage.topic,
                partition=stage.partition,
                team_id=team_id,
                trigger=decision.trigger,
                outcome="quarantined",
                first_offset=first_offset,
                last_offset=last_offset,
                quarantined=len(plan.quarantined),
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
                f"the upload for team {team_id} offsets "
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
                team_id,
                first_offset,
                PreparedRequest(
                    idempotency_key=key,
                    body=body,
                    persisted_at=now_us,
                    topic=stage.topic,
                    partition=stage.partition,
                ),
            )
        except BaseException:
            self._count_orphans(
                plan.uris,
                f"the upload for team {team_id} offsets "
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
                f"the commit for team {team_id} was refused with "
                "ddl_since_read_snapshot; the flush re-prepares against a "
                "fresh read",
                reason="ddl_rebuild",
            )
            await stage.drop_prepared(team_id, first_offset)
            self._reprepares += 1
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
                team_id=team_id,
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
                raise self._halt(
                    "idempotency_key_reused",
                    f"the server holds a receipt under {key} for a DIFFERENT "
                    "request: with deterministic derivation that means a "
                    "foreign writer or broken determinism — halting rather "
                    "than settling rows a stranger may not have published",
                    plan.uris,
                ) from e
            status = e.status_code
            if _is_record_validation_refusal(e):
                # The server judged the RECORDS (not the request), zero
                # writes. Quarantine the decision — whole payloads, never
                # capped — and settle WITHOUT a commit.
                self._count_orphans(
                    plan.uris,
                    f"the commit for team {team_id} was refused with "
                    f"{status} {_RECORD_VALIDATION_CODE}; the decision is "
                    "quarantined",
                    reason="commit_refused",
                )
                window = FlushedKey(team_id, first_offset, last_offset)
                return await self._quarantine_window(
                    stage,
                    window,
                    rows,
                    key,
                    status,
                    _error_text(e),
                    trigger=decision.trigger,
                )
            if status is not None and 400 <= status < 500:
                # Any OTHER answered 4xx (a proxy's 401/403, an ingress
                # 413, a plain 400, a request-shape "validation" 422):
                # nothing about it says the ROWS can never publish. Halt
                # with the rows staged and the prepared/ entry persisted —
                # exactly the idempotency_key_reused / destination_missing
                # posture (PR #331 review: the pre-fix shape truncated and
                # deleted up to a whole flush window on any answered 4xx).
                raise self._halt(
                    "commit_refused",
                    f"the commit for {stage.topic}[{stage.partition}] team "
                    f"{team_id} offsets {first_offset}-{last_offset} "
                    f"(idempotency key {key}) was answered {status}: "
                    f"{_error_text(e)}. An answered refusal that does not "
                    "name a per-record validation proves nothing about the "
                    "rows — they stay staged and the pipeline halts for an "
                    "operator.",
                    plan.uris,
                ) from e
            raise

        window = FlushedKey(team_id, first_offset, last_offset)
        report = await stage.commit_flushed(window)
        self._guard_settlement(window, len(rows), report.rows_deleted)
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
            "flushed %s[%d] team %d offsets %d-%d: %d rows, %d files, snapshot %d (%s)",
            stage.topic,
            stage.partition,
            team_id,
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
            team_id=team_id,
            trigger=decision.trigger,
            outcome="committed",
            first_offset=first_offset,
            last_offset=last_offset,
            records=plan.records,
            files=plan.files,
            quarantined=len(plan.quarantined),
            snapshot_id=result.snapshot_id,
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
    quarantined: int
    empty: int
    failed: int
    fenced: int = 0


class FlushRunner:
    """The flush loop: one planning pass per tick over the StageManager's
    claimed partitions. Per partition and tick:

    1. RECONCILE (first tick after a (re)claim): ``recover()`` collects
       legacy ``flushed/`` markers in bounded pages, and every surviving
       ``prepared/`` entry is replayed / receipt-settled by the flusher,
       inside the receipt horizon. A partition whose reconciliation
       fails transiently is skipped for the tick and retried on the
       next — it is never marked reconciled.
    2. ASSEMBLE the planner input: one full ``stats/`` scan (the size
       lane's input — there is no size-ordered index; ``sched_size`` is
       the design's deferred bench question) PLUS the age/slow
       readiness path the design promises: a ``sched_age/`` index
       prefix scan through the age-lane cutoff, then a point-read per
       candidate for the exact, current decision input (M6). The full
       scan is ALSO the gauge source: the sweep's fold is published on
       the StageManager (``publish_gauges``) for the consumer's
       backpressure read and the metrics scrape, so a sweep costs one
       ``stats/`` scan per partition, not three.
    3. PLAN: ``planner.plan_flush(stats, now, knobs,
       estimated_compression_ratio=...)`` — the ratio is the flusher's
       observed EWMA, the fallback until the first flush lands.
    4. EXECUTE: per decision, :meth:`HoglakeFlusher.flush_key`; commits
       are per ``(team, window)`` and never exceed
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
    ``(team, window)`` units — order between teams carries no
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
        decisions = committed = replayed = receipt_settled = 0
        reprepared = quarantined = empty = failed = 0
        gauge_map: dict[tuple[str, int], StageGauges] = {}
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
                except Exception:
                    # Transient reconciliation failure: skipped this tick,
                    # retried the next (the partition is never marked
                    # reconciled). Its gauges must still reach the
                    # published snapshot — the consumer's backpressure
                    # reads them — so read them directly (best-effort: a
                    # partition this broken must not wedge the sweep's
                    # gauge publication either).
                    log.exception(
                        "reconciliation failed for %s[%d]; the partition is "
                        "skipped this tick",
                        topic,
                        partition,
                    )
                    failed += 1
                    try:
                        gauge_map[(topic, partition)] = await stage.gauges()
                    except Exception:
                        log.debug(
                            "the gauge read for %s[%d] failed too",
                            topic,
                            partition,
                            exc_info=True,
                        )
                    continue
                self._reconciled.add((topic, partition))
                for report in reconciliation:
                    replayed += report.outcome == "replayed"
                    receipt_settled += report.outcome == "receipt_settled"
                    reprepared += report.outcome == "reprepare"
                    quarantined += report.outcome == "quarantined"
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
            for decision in plan_flush(
                stats,
                self._now_us(),
                self._knobs,
                estimated_compression_ratio=self._flusher.compression_ratio,
            ):
                work.append((stage, decision))
                decisions += 1
        # The sweep's gauges ARE the planner input's fold (M6): one
        # scan per partition per sweep serves the planner, the
        # consumer's backpressure and the metrics scrape. A partition
        # revoked mid-sweep drops out of the published set (its staged
        # bytes are the new owner's concern now).
        still_open = set(self._stages.partitions())
        self._stages.publish_gauges(
            {key: g for key, g in gauge_map.items() if key in still_open}
        )
        reports, decision_failures = await self._execute(work)
        failed += decision_failures
        for report in reports:
            committed += report.outcome == "committed"
            replayed += report.outcome == "replayed"
            receipt_settled += report.outcome == "receipt_settled"
            reprepared += report.outcome == "reprepare"
            quarantined += report.outcome == "quarantined"
            empty += report.outcome == "empty"
        self._sweeps += 1
        sweep = SweepReport(
            partitions=len(open_set),
            decisions=decisions,
            committed=committed,
            replayed=replayed,
            receipt_settled=receipt_settled,
            reprepared=reprepared,
            quarantined=quarantined,
            empty=empty,
            failed=failed,
            fenced=len(self._fenced),
        )
        self._last_report = sweep
        if decisions:
            log.info(
                "flush sweep %d: %d decisions — %d committed, %d replayed, "
                "%d receipt-settled, %d re-prepared, %d quarantined, "
                "%d empty, %d failed",
                self._sweeps,
                decisions,
                committed,
                replayed,
                receipt_settled,
                reprepared,
                quarantined,
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
        by_team = {s.team_id: s for s in stats}
        for candidate in candidates:
            exact = await stage.read_team_stats(candidate.team_id)
            if exact is None:
                raise StageCorruptionError(
                    f"team {candidate.team_id}: a sched_age/ entry with no "
                    "stats/ entry (the two are written and removed in one "
                    "batch — a leftover is foreign writes or a bug)"
                )
            by_team[candidate.team_id] = exact
        return list(by_team.values())

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
                    except Exception:
                        # Contained per decision: the rows stay staged,
                        # the next sweep retries. Never wedged.
                        failed += 1
                        log.exception(
                            "flush decision failed for %s[%d] team %d",
                            stage.topic,
                            stage.partition,
                            decision.team_id,
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
        first, verbatim replay on a 404 inside the receipt horizon, a
        loud halt past it. The reconciliation reports count toward the
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
