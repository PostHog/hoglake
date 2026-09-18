"""Clock, rate control and flush cadence for the continuous stream.

Three small, pure-ish pieces, all injectable for tests:

``StreamClock``
    Event time. Advances with the wall clock by default; ``speedup``
    compresses it so hour partitions roll over without waiting an hour.

``RateLimiter``
    Backpressure, not a busy loop: every wait is an actual ``sleep`` of
    the time that is owed, chunked so a signal is noticed promptly.

``FlushPolicy``
    Millpond's cadence — flush on accumulated Arrow bytes OR elapsed
    time, whichever comes first (``FLUSH_SIZE`` / ``FLUSH_INTERVAL_MS``,
    millpond/config.py:580-581).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

MICROS_PER_SECOND = 1_000_000
MICROS_PER_HOUR = 3_600 * MICROS_PER_SECOND


def epoch_hour(micros: int) -> int:
    """Hours since the epoch, FLOORED — the value the ``hour`` partition
    transform produces. Python's ``//`` floors for negatives too, which
    is what keeps pre-epoch timestamps agreeing with
    ``pyhoglake.transforms.hour``."""
    return micros // MICROS_PER_HOUR


def hour_label(micros: int) -> str:
    """The open hour cell, for humans: ``2026-09-18T14Z``."""
    when = datetime.fromtimestamp(epoch_hour(micros) * 3_600, UTC)
    return when.strftime("%Y-%m-%dT%HZ")


def speedup_for_hours_per_minute(hours_per_minute: float) -> float:
    """``--hours-per-minute`` as a plain event-time multiplier."""
    if hours_per_minute <= 0:
        raise ValueError("--hours-per-minute must be positive")
    return hours_per_minute * 60.0


class StreamClock:
    """Event time, optionally faster than real time.

    ``advance_window()`` hands out consecutive, non-empty ``[lo, hi)``
    micro ranges: each batch's timestamps land in the slice of event time
    that elapsed while it was being produced, so hour partitions close on
    their own as the run proceeds.
    """

    def __init__(
        self,
        start_us: int,
        *,
        speedup: float = 1.0,
        now: Callable[[], float] = time.monotonic,
    ) -> None:
        if speedup <= 0:
            raise ValueError("speedup must be positive")
        self.start_us = int(start_us)
        self.speedup = float(speedup)
        self._now = now
        self._t0 = now()
        self._last_us = self.start_us

    def now_us(self) -> int:
        elapsed = self._now() - self._t0
        return self.start_us + int(elapsed * self.speedup * MICROS_PER_SECOND)

    def advance_window(self) -> tuple[int, int]:
        lo = self._last_us
        hi = max(self.now_us(), lo + 1)
        self._last_us = hi
        return lo, hi


class RateLimiter:
    """Paces a producer to ``rate_per_s`` events per second.

    Backpressure, not spinning: the wait is one (chunked) ``sleep`` of
    exactly the time owed. ``rate_per_s <= 0`` means unlimited and costs
    nothing.

    Unused credit is NOT banked — after a long stall the limiter resets
    its clock rather than handing out a burst allowance, so a slow flush
    can never be repaid as a spike that swamps the server.
    """

    def __init__(
        self,
        rate_per_s: float,
        *,
        now: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        max_sleep_s: float = 0.2,
    ) -> None:
        self.rate_per_s = float(rate_per_s)
        self.max_sleep_s = max_sleep_s
        self._now = now
        self._sleep = sleep
        self._issued = 0
        self._t0 = now()

    @property
    def unlimited(self) -> bool:
        return self.rate_per_s <= 0

    def acquire(
        self, n: int, *, should_stop: Callable[[], bool] | None = None
    ) -> float:
        """Block until ``n`` more events are due. Returns seconds slept."""
        if self.unlimited or n <= 0:
            return 0.0
        self._issued += n
        target = self._t0 + self._issued / self.rate_per_s
        slept = 0.0
        while True:
            delay = target - self._now()
            if delay <= 0:
                break
            if should_stop is not None and should_stop():
                break
            chunk = min(delay, self.max_sleep_s)
            self._sleep(chunk)
            slept += chunk
        if self._now() - target > self.max_sleep_s:
            # Fell behind by more than a chunk (a slow flush, a retry
            # storm). Re-anchor instead of accumulating a debt that would
            # be repaid as an unthrottled burst.
            self._t0 = self._now()
            self._issued = 0
        return slept


@dataclass(frozen=True)
class FlushPolicy:
    """Flush on bytes OR elapsed time, whichever comes first.

    ``max_bytes`` counts **Arrow** bytes in the pending buffer, as
    millpond's ``FLUSH_SIZE`` does. The parquet that lands is smaller;
    millpond quotes 3-4x for its payloads, and this stream measures its
    own ratio per run rather than assuming that one.
    """

    max_bytes: int
    max_seconds: float

    def trigger(
        self, *, pending_bytes: int, pending_rows: int, age_s: float
    ) -> str | None:
        # An append of 0 rows to a partitioned table is a client-side
        # refusal (no partition tuple is derivable), so an empty buffer
        # never flushes no matter how old it is.
        if pending_rows <= 0:
            return None
        if self.max_bytes > 0 and pending_bytes >= self.max_bytes:
            return "bytes"
        if self.max_seconds > 0 and age_s >= self.max_seconds:
            return "time"
        return None
