"""Object-store uploads for the writer paths: one request per small
object, many objects at once.

Two facts about the parquet these writers produce shape this module. The
objects are SMALL — the prod-us events writer flushes ~105K rows as ~271
parquet files, one per partition tuple, a median of 12 KiB each, because
a backfilling batch spans hundreds of day partitions — and there are MANY
of them per flush. pyarrow's answer is wrong for that shape twice over:
its S3 output stream always opens a multipart upload (measured against a
recording endpoint on pyarrow 25.0.1: CreateMultipartUpload + UploadPart
+ CompleteMultipartUpload, three round trips and ~320 ms for a 12 KiB
file), and every Arrow S3 request runs on Arrow's process-global IO pool,
eight threads wide by default. A serial loop over 271 of those was 85 s
p50 of the writer's flush — against 10 s to fill the batch, 4.3 s to
encode, and 18 ms of server commit.

So: one PutObject for anything small enough to hold in memory, a thread
pool wide enough that the object store's latency stops being serial, and
the streaming multipart path kept for objects too big to buffer.
"""

from __future__ import annotations

import contextlib
import os
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from .errors import HoglakeError, ValidationError

# One request instead of three, for every object at or below this. The
# ceiling is a MEMORY bound, not an S3 one — a single PutObject takes up
# to 5 GiB: the whole object is held (or read) in one piece while its
# request is in flight, and `concurrency` of them can be in flight at
# once, so this times the fan-out is what the upload phase can ask for
# (8 MiB x 64 = 512 MiB worst case, against 12 KiB x 271 = 3 MiB for the
# shape that motivated the change). Above it the object is big enough
# that multipart's pipelining pays for its two extra round trips anyway.
SINGLE_REQUEST_MAX_BYTES = 8 * 1024 * 1024

# What the streaming path reads from disk per write. Unchanged from the
# serial loop this replaced: large enough that a big file is a handful of
# reads, small enough that one upload never holds much.
STREAM_CHUNK_BYTES = 8 * 1024 * 1024

# Uploads in flight by default, capped at the number of objects. 64 turns
# a 271-object flush into ~5 waves of round trips instead of 271, and
# stays well inside S3's per-prefix request ceiling; past it the writer
# is bounded by the object store rather than by waiting on it.
DEFAULT_MAX_CONCURRENCY = 64

CONCURRENCY_ENV = "PYHOGLAKE_UPLOAD_CONCURRENCY"


@dataclass(frozen=True)
class Upload:
    """One object to write: its uri, and where its bytes come from.

    Exactly one of ``body`` (already in memory — the encode-in-RAM path
    owns the buffer) and ``path`` (still on disk — the prepared-files
    path hands over a file it will not touch again) is set. ``size`` is
    the object's length either way, because the single-request/multipart
    decision is made before anything is read.
    """

    uri: str
    size: int
    body: Any | None = None
    path: str | None = None


def s3_key(uri: str) -> str:
    """The bucket-qualified key pyarrow and boto3 are addressed by."""
    if not uri.startswith("s3://"):
        raise HoglakeError(
            f"unsupported data_path scheme for the write path: {uri!r} "
            "(only s3:// is supported)"
        )
    return uri[len("s3://") :]


_PUT_ERRORS: tuple[type[BaseException], ...] | None = None


def _put_errors() -> tuple[type[BaseException], ...]:
    """botocore's fault classes, resolved on first use.

    Not a module-level import: botocore is an optional extra, and an
    install without it must still import this module. Such an install
    cannot raise these either, so the empty tuple — which catches
    nothing — is the right answer there. By the time this is reached
    botocore has already been imported (a put client exists), so the
    reason is availability, not import cost.
    """
    global _PUT_ERRORS
    if _PUT_ERRORS is None:
        try:
            from botocore.exceptions import BotoCoreError, ClientError
        except ImportError:  # pragma: no cover  # no boto3 => no put client
            _PUT_ERRORS = ()
        else:
            _PUT_ERRORS = (BotoCoreError, ClientError)
    return _PUT_ERRORS


def perform_upload(fs, put_client, upload: Upload) -> None:
    """Write one object, in ONE request when it is small enough.

    ``put_client`` is ``None`` when single-request uploads are
    unavailable (boto3 not installed, or turned off on the ``S3Config``):
    everything then takes the streaming path, which is what this package
    always did — correct, three requests per object.

    A returned ``put_object`` is the whole object; the streaming path's
    truncated-object case (a close that fails over bytes that landed)
    does not exist for it.

    A fault is raised as an ``OSError`` whichever path it came from:
    pyarrow raises one, botocore raises its own taxonomy, and whether the
    fast path is installed is not something a caller's ``except`` clause
    should have to know. The original stays attached as ``__cause__``.
    """
    key = s3_key(upload.uri)
    if put_client is not None and upload.size <= SINGLE_REQUEST_MAX_BYTES:
        bucket, _, name = key.partition("/")
        try:
            if upload.body is not None:
                # A BufferReader, not bytes(buffer): botocore refuses a
                # memoryview and copying would double the encode path's
                # peak footprint for no reason — the reader is zero-copy
                # over the buffer we already hold.
                put_client.put_object(
                    Bucket=bucket, Key=name, Body=pa.BufferReader(upload.body)
                )
            else:
                # The open file, not its bytes: botocore streams and
                # hashes it itself, so a small prepared file never becomes
                # a Python buffer at all.
                # Opened outside a `with` (SIM115) precisely so its
                # close can be tolerated below.
                source = open(upload.path, "rb")  # type: ignore[arg-type] # noqa: SIM115
                try:
                    put_client.put_object(Bucket=bucket, Key=name, Body=source)
                finally:
                    # The object exists the moment put_object returns, so
                    # a close() that fails after it (a network filesystem
                    # losing the handle) must not cost us the uri — the
                    # caller would be left with an orphan nobody sweeps.
                    # A file opened for reading has nothing to flush, so
                    # suppressing this loses nothing.
                    with contextlib.suppress(OSError):
                        source.close()
        except _put_errors() as failure:
            raise OSError(f"failed to upload {upload.uri}: {failure}") from failure
        return
    with fs.open_output_stream(key) as sink:
        if upload.body is not None:
            sink.write(upload.body)
        else:
            with open(upload.path, "rb") as source:  # type: ignore[arg-type]
                while chunk := source.read(STREAM_CHUNK_BYTES):
                    sink.write(chunk)


def resolve_concurrency(count: int, requested: int | None) -> int:
    """How many uploads to have in flight: the ``concurrency`` argument,
    else ``PYHOGLAKE_UPLOAD_CONCURRENCY``, else
    :data:`DEFAULT_MAX_CONCURRENCY` — capped at ``count``, since a pool
    wider than the work only costs threads.

    An unparseable or non-positive setting is an error rather than a
    silent clamp: running eight-wide when you asked for 256 is exactly
    the failure this module exists to remove, and it is invisible from
    the outside. It is a ``ValidationError`` like every other
    client-side refusal on this path, so one ``except HoglakeError``
    still covers the whole call (the README's error contract).
    """
    source = "upload concurrency"
    if requested is None:
        raw = os.environ.get(CONCURRENCY_ENV, "").strip()
        if raw:
            # Named in the message, both for a bad spelling and for a bad
            # value: the operator who set it is not the caller who passed
            # the argument, and only one of them can fix it.
            source = CONCURRENCY_ENV
            try:
                requested = int(raw)
            except ValueError:
                raise ValidationError(
                    f"{CONCURRENCY_ENV} must be a positive integer, got {raw!r}",
                    status_code=None,
                ) from None
        else:
            requested = DEFAULT_MAX_CONCURRENCY
    if requested < 1:
        raise ValidationError(
            f"{source} must be a positive integer, got {requested}", status_code=None
        )
    return max(1, min(count, requested))


def widen_io_threads(concurrency: int) -> None:
    """Widen Arrow's IO pool to the fan-out — once, and only upwards.

    PROCESS-GLOBAL, deliberately and unavoidably: every Arrow S3 request
    runs on this one pool (eight threads by default), so 64 concurrent
    streaming uploads would still queue eight at a time however wide the
    thread pool is. Only ever raised — another part of the host process
    may have set it higher on purpose, and lowering someone else's pool
    is not this library's call.
    """
    if pa.io_thread_count() < concurrency:
        pa.set_io_thread_count(concurrency)


def widen_io_threads_for(
    uploads: Sequence[Upload], put_client: Any, concurrency: int
) -> None:
    """Widen Arrow's pool only when THIS batch will put work on it.

    The pool is process-global and shared with everything else the host
    process does with Arrow, so widening it for a batch that never
    touches it is a side effect with no benefit — and in the intended
    production case (boto3 present, every object under the threshold)
    not one upload reaches Arrow.
    """
    if concurrency <= 1 or len(uploads) <= 1:
        return  # the serial path issues one request at a time anyway
    if put_client is not None and all(
        upload.size <= SINGLE_REQUEST_MAX_BYTES for upload in uploads
    ):
        return
    widen_io_threads(concurrency)


def run_uploads(
    perform: Callable[[Upload], None],
    uploads: Sequence[Upload],
    *,
    concurrency: int,
    completed: list[str],
) -> None:
    """Upload every object, filling ``completed`` with the uris that
    landed — in INPUT order, whatever order they finished in.

    ``completed`` is an out-parameter rather than a return value because
    the caller's orphan accounting needs it on the failure path too: the
    exception leaving ``prepare_append_*`` carries exactly this list as
    ``uploaded_uris``, and it has to be exact in both directions — a uri
    missing from it is an orphan nobody sweeps, and one that does not
    belong names a live file a sweep would delete.

    Failure semantics: the FIRST error wins and is re-raised unwrapped (a
    published consumer catches the object store's own ``OSError``).
    Uploads that have not started are cancelled; uploads already in
    flight are WAITED for, because an S3 request whose result nobody
    reads still creates an object. The completed list therefore grows
    after the failure, which is the point of waiting — and is why it can
    be SPARSE rather than a prefix of the input: group 7 landing while
    group 6 failed is the normal case, not an anomaly.

    Arrow's IO pool is deliberately not touched here — see
    :func:`widen_io_threads_for`, which the caller runs because only the
    caller knows whether this batch reaches Arrow at all.
    """
    if concurrency <= 1 or len(uploads) <= 1:
        # The single-object writer path (Table.append) and an explicit
        # concurrency=1: no pool, no threads, and the serial
        # "stopped at the object that failed" semantics unchanged.
        for upload in uploads:
            perform(upload)
            completed.append(upload.uri)
        return

    landed = [False] * len(uploads)

    def run(index: int) -> None:
        perform(uploads[index])
        # Only past a clean return, exactly as the serial loop only
        # counted past the output stream's close: an upload that raised
        # may have left a truncated object, and claiming it landed is
        # worse than reporting it as possibly-there.
        landed[index] = True

    error: BaseException | None = None
    try:
        with ThreadPoolExecutor(
            max_workers=concurrency, thread_name_prefix="pyhoglake-upload"
        ) as pool:
            futures = [pool.submit(run, index) for index in range(len(uploads))]
            try:
                for future in as_completed(futures):
                    if future.cancelled():
                        continue
                    failure = future.exception()
                    if failure is not None and error is None:
                        error = failure
                        for pending in futures:
                            pending.cancel()  # only what has not started
            finally:
                # Covers an interrupt raised in THIS thread as well: the
                # pool's own shutdown waits for QUEUED work, so it has to
                # be cancelled before the with-block joins.
                for pending in futures:
                    pending.cancel()
    finally:
        # The pool has joined by here, so `landed` includes the uploads
        # that were in flight at the failure and finished afterwards.
        completed.extend(
            upload.uri for upload, ok in zip(uploads, landed, strict=True) if ok
        )
    if error is not None:
        raise error
