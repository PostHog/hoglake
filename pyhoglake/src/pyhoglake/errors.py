"""Typed exceptions mapped from hoglake ApiError responses."""

from __future__ import annotations


class HoglakeError(Exception):
    """Base error for pyhoglake. Carries the server's ApiError fields."""

    #: Replaying the SAME request can succeed. A retry loop (millpond's,
    #: hedgerow's) backs off and resends the identical payload.
    retryable: bool = False

    #: Replaying cannot succeed, but re-reading the table and building a
    #: NEW request can — the server's `retry: re-prepare`. Declared on
    #: the base so `err.re_prepare` is always answerable rather than an
    #: attribute only some errors carry; the two flags are mutually
    #: exclusive by construction and both false means "the request was
    #: simply wrong".
    re_prepare: bool = False

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        detail: str | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.detail = detail

    def __str__(self) -> str:  # pragma: no cover - repr sugar
        base = self.message
        if self.detail:
            base = f"{base}: {self.detail}"
        if self.status_code is not None:
            base = f"{base} (HTTP {self.status_code})"
        return base


class NotFoundError(HoglakeError):
    """404 — the named catalog/namespace/table/view/consumer does not exist."""


class AlreadyExistsError(HoglakeError):
    """409 on a create — an object with that name already exists."""


class CommitConflictError(HoglakeError):
    """409 on commit/alter — concurrent DDL on a touched table.

    Retryable: refresh your read snapshot and retry.
    """

    retryable = True


class ValidationError(HoglakeError):
    """422 — invalid request (bad op, bad stats shape, unknown field ids...)."""


class OffsetRegressionError(HoglakeError):
    """409 on consumer-offset commit — offsets are monotonic; the snapshot
    is below the stored offset."""


class ExpiredError(HoglakeError):
    """410 — the requested snapshot range falls below the catalog's expiry
    floor. Reconcile from a full scan rather than silently skipping."""


class ReadSnapshotExpiredError(ExpiredError):
    """410 on a COMMIT — the request's ``read_snapshot`` sank below the
    catalog's expiry floor, so the server cannot evaluate its conflict
    window at all.

    A subclass of :class:`ExpiredError` so existing ``except
    ExpiredError`` keeps working, and separate because the recovery is
    different: a changefeed 410 means "reconcile from a full scan", this
    one means "re-read the table and prepare a new request". The floor
    only ever moves forward, so replaying is futile — hence
    :attr:`re_prepare`."""

    re_prepare = True


class DdlSinceReadSnapshotError(CommitConflictError):
    """409 ``ddl_since_read_snapshot`` — DDL landed on a table this
    commit touches after its ``read_snapshot``.

    THE check-and-set on the table's shape: a partition-spec or schema
    change between the writer's read and its commit arrives here,
    atomically and with zero writes.

    ``retryable = False``, overriding the base: ``read_snapshot`` is part
    of the payload (a prepared request must be replayed byte-identically),
    so the alter will not un-happen and replaying is a livelock. Re-read
    the table and prepare a new request.

    A SUBCLASS of :class:`CommitConflictError` all the same, for the same
    reason :class:`ReadSnapshotExpiredError` subclasses
    :class:`ExpiredError`: callers discriminate with ``isinstance``
    ladders, not by reading these flags. millpond's retry budget does
    exactly that, and its ``CommitConflictError`` arm already recovers
    correctly — it resets its cached table and drops the refused payload,
    so the next attempt re-prepares and succeeds. Making this a sibling
    class instead would have dropped it through to millpond's
    "any other 4xx" arm and turned a working recovery into a hard
    re-raise. What changed is the wire CODE, which a caller can now branch
    on if it wants the finer answer.

    :attr:`tables` names the qualified tables DDL landed on and
    :attr:`read_snapshot` the snapshot that was refused, both off the
    server's structured body rather than parsed out of the message."""

    retryable = False
    re_prepare = True

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        detail: str | None = None,
        tables: tuple[str, ...] = (),
        read_snapshot: int | None = None,
    ) -> None:
        super().__init__(message, status_code=status_code, detail=detail)
        self.tables = tables
        self.read_snapshot = read_snapshot


class IncarnationChangedError(HoglakeError):
    """The table resolved by name no longer carries the ``table_uuid``
    the caller expected (drop + recreate under the same name).

    Raised on the append path in two places: by the server — the commit
    ships ``expected_table_uuid`` and a mismatch is 409
    ``table_recreated``, the whole commit refused atomically with zero
    writes — and by the client's cheap pre-flight re-resolve, which
    fast-fails an already-dead incarnation before the parquet upload.

    Never retry blindly: the expected incarnation is gone and its history
    does not carry over, so :attr:`re_prepare` rather than
    :attr:`retryable` — re-resolve the table and decide whether writing
    to the new incarnation is what you meant.

    :attr:`table` names the qualified table when the refusal came from
    the server's structured body."""

    re_prepare = True

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        detail: str | None = None,
        table: str | None = None,
    ) -> None:
        super().__init__(message, status_code=status_code, detail=detail)
        self.table = table


class MalformedResponseError(HoglakeError):
    """A response body did not match the wire contract: a required field
    was missing, or a field/nested object had the wrong shape.

    Raised client-side by every model's ``from_wire``; the message names
    the model and the offending field. Replaces the leaked
    ``KeyError``/``AttributeError``/``TypeError`` trio the hand-rolled
    parsers used to disagree on (bugs.md #24)."""


class UnsupportedTypeError(HoglakeError, TypeError):
    """An Arrow type with no hoglake column-type mapping."""


class ReconciliationRequiredError(HoglakeError):
    """409 — a changefeed window crosses TRUNCATE; reconcile before checkpointing.

    Non-retryable: replaying the same window cannot represent this deletion.
    """
