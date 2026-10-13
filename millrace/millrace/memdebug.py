"""Opt-in memory diagnostics (``MILLRACE_MEMORY_DEBUG_S=<interval>``).

OFF by default: nothing here runs, imports tracemalloc or allocates
unless the env var names a positive interval. When on, :func:`start_tracing`
(called first thing in ``_main``) starts ``tracemalloc`` with
``MILLRACE_MEMORY_DEBUG_FRAMES`` frames (default 8), and
:func:`start_reporter` runs a daemon thread that every interval emits one
JSON line — to ``MILLRACE_MEMORY_DEBUG_FILE`` when set (``{pid}`` is
substituted), else to the ``millrace.memdebug`` logger — carrying:

- RSS (``ps``), macOS malloc-zone totals (in use vs allocated — the
  fragmentation signal for every system-malloc user: CPython's large
  blocks, the SlateDB/Rust heap, librdkafka);
- tracemalloc current/peak and its own overhead, plus the top-N
  allocation sites by size DIFF since the previous snapshot, each
  attributed to the innermost frame in millrace/pyhoglake/pyarrow/slatedb
  (``by_site``) and to the raw innermost frame (``by_line``);
- pyarrow ``total_allocated_bytes`` / pool ``bytes_allocated`` / ``max_memory``;
- ``gc.get_count()``, ``len(gc.get_objects())`` and the top object-type
  count deltas;
- the sizes of the pipeline's long-lived containers (``probes``) and every
  SlateDB metric summed across the pod's recorders.

The reporter holds the GIL while snapshotting (tens of ms at a few
hundred thousand traced blocks); intervals under ~10 s are not useful.
"""

from __future__ import annotations

import ctypes
import ctypes.util
import gc
import json
import logging
import os
import subprocess
import sys
import threading
import time
from collections import Counter
from collections.abc import Callable, Mapping
from typing import Any

log = logging.getLogger("millrace.memdebug")

ENV_INTERVAL = "MILLRACE_MEMORY_DEBUG_S"
ENV_FRAMES = "MILLRACE_MEMORY_DEBUG_FRAMES"
ENV_FILE = "MILLRACE_MEMORY_DEBUG_FILE"
ENV_TOP = "MILLRACE_MEMORY_DEBUG_TOP"

_OURS = ("/millrace/millrace/", "/pyhoglake/", "/pyarrow/", "/slatedb/")


def interval_s(env: Mapping[str, str]) -> float:
    raw = env.get(ENV_INTERVAL, "").strip()
    if not raw:
        return 0.0
    try:
        return max(0.0, float(raw))
    except ValueError:
        return 0.0


def start_tracing(env: Mapping[str, str]) -> None:
    if interval_s(env) <= 0:
        return
    import tracemalloc

    frames = int(env.get(ENV_FRAMES, "8") or 8)
    if frames > 0 and not tracemalloc.is_tracing():
        tracemalloc.start(frames)


def _rss_bytes() -> int | None:
    try:
        with open(f"/proc/{os.getpid()}/status") as status:
            for line in status:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        pass
    try:
        out = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(os.getpid())],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        ).stdout.strip()
        return int(out) * 1024 if out else None
    except Exception:  # noqa: BLE001
        return None


class _MallocStats(ctypes.Structure):
    _fields_ = [
        ("blocks_in_use", ctypes.c_uint),
        ("size_in_use", ctypes.c_size_t),
        ("max_size_in_use", ctypes.c_size_t),
        ("size_allocated", ctypes.c_size_t),
    ]


def _malloc_zone_stats() -> dict[str, int] | None:
    if sys.platform != "darwin":
        return None
    try:
        libc = ctypes.CDLL(ctypes.util.find_library("c"))
        fn = libc.malloc_zone_statistics
        fn.argtypes = [ctypes.c_void_p, ctypes.POINTER(_MallocStats)]
        fn.restype = None
        stats = _MallocStats()
        fn(None, ctypes.byref(stats))  # NULL zone = all zones
        return {
            "blocks_in_use": stats.blocks_in_use,
            "size_in_use": stats.size_in_use,
            "size_allocated": stats.size_allocated,
        }
    except Exception:  # noqa: BLE001
        return None


def _site(frames: Any) -> str:
    # tracemalloc tracebacks are most-recent-first.
    for frame in frames:
        if any(tag in frame.filename for tag in _OURS):
            return f"{_short(frame.filename)}:{frame.lineno}"
    frame = frames[0]
    return f"(other) {_short(frame.filename)}:{frame.lineno}"


def _short(path: str) -> str:
    for tag in _OURS:
        i = path.rfind(tag)
        if i >= 0:
            return path[i + 1 :]
    return "/".join(path.rsplit("/", 2)[-2:])


class _Reporter:
    def __init__(
        self,
        every_s: float,
        out_path: str | None,
        top: int,
        probes: Callable[[], Mapping[str, Any]],
    ) -> None:
        self._every_s = every_s
        self._out_path = out_path
        self._top = top
        self._probes = probes
        self._prev_snapshot: Any = None
        self._prev_types: Counter[str] = Counter()
        self._started = time.monotonic()
        self._seq = 0

    def run(self) -> None:
        while True:
            time.sleep(self._every_s)
            try:
                self._emit(self._sample())
            except Exception as e:  # noqa: BLE001 - diagnostics never kill the process
                import traceback

                self._emit(
                    {"seq": self._seq, "error": "".join(traceback.format_exception(e))}
                )

    def _emit(self, row: dict[str, Any]) -> None:
        line = json.dumps(row, default=str)
        if self._out_path:
            with open(self._out_path, "a") as out:
                out.write(line + "\n")
        else:
            log.info("memdebug %s", line)

    def _sample(self) -> dict[str, Any]:
        import tracemalloc

        self._seq += 1
        row: dict[str, Any] = {
            "seq": self._seq,
            "t_s": round(time.monotonic() - self._started, 1),
            "wall": time.time(),
            "rss": _rss_bytes(),
            "malloc": _malloc_zone_stats(),
        }
        try:
            import pyarrow as pa

            pool = pa.default_memory_pool()
            row["pyarrow"] = {
                "backend": pool.backend_name,
                "total_allocated_bytes": pa.total_allocated_bytes(),
                "pool_bytes_allocated": pool.bytes_allocated(),
                "pool_max_memory": pool.max_memory(),
            }
        except Exception as e:  # noqa: BLE001
            row["pyarrow"] = repr(e)
        row["gc_count"] = gc.get_count()
        objects = gc.get_objects()
        row["gc_objects"] = len(objects)
        types = Counter(type(o).__name__ for o in objects)
        del objects
        delta = types - self._prev_types
        row["type_growth"] = delta.most_common(10)
        self._prev_types = types
        try:
            row["probes"] = dict(self._probes())
        except Exception as e:  # noqa: BLE001
            row["probes"] = repr(e)
        if tracemalloc.is_tracing():
            current, peak = tracemalloc.get_traced_memory()
            row["tracemalloc"] = {
                "current": current,
                "peak": peak,
                "overhead": tracemalloc.get_tracemalloc_memory(),
            }
            snap = tracemalloc.take_snapshot().filter_traces(
                (
                    tracemalloc.Filter(False, tracemalloc.__file__),
                    tracemalloc.Filter(False, __file__),
                )
            )
            if self._prev_snapshot is not None:
                stats = snap.compare_to(self._prev_snapshot, "traceback")
                by_site: dict[str, list[int]] = {}
                by_line: dict[str, list[int]] = {}
                for stat in stats:
                    for key, bucket in (
                        (_site(stat.traceback), by_site),
                        (
                            f"{_short(stat.traceback[0].filename)}:{stat.traceback[0].lineno}",
                            by_line,
                        ),
                    ):
                        agg = bucket.setdefault(key, [0, 0, 0])
                        agg[0] += stat.size_diff
                        agg[1] += stat.size
                        agg[2] += stat.count_diff
                for name, bucket in (("by_site", by_site), ("by_line", by_line)):
                    row[name] = [
                        [k, *v]
                        for k, v in sorted(bucket.items(), key=lambda kv: -kv[1][1])[
                            : self._top
                        ]
                    ]
            self._prev_snapshot = snap
        return row


def container_sizes(**owners: Any) -> dict[str, int]:
    """``len()`` of every sized container attribute on each owner (one
    level deep), keyed ``owner.attr`` — catches caches nobody listed."""
    out: dict[str, int] = {}
    for name, owner in owners.items():
        if owner is None:
            continue
        try:
            attrs = vars(owner)
        except TypeError:
            continue
        for attr, value in list(attrs.items()):
            if isinstance(value, (dict, list, set, frozenset, tuple)) or type(
                value
            ).__name__ in ("deque", "OrderedDict", "defaultdict", "WeakSet"):
                try:
                    out[f"{name}.{attr}"] = len(value)
                except Exception:  # noqa: BLE001, S112
                    continue
    return out


def slatedb_totals(recorders: Any) -> dict[str, float]:
    """Every SlateDB counter/gauge summed across the recorders (no
    whitelist — the diagnostic wants what the bridge drops)."""
    from .slatedb_metrics import snapshot_recorder

    totals: dict[str, float] = {}
    for recorder in recorders:
        for m in snapshot_recorder(recorder):
            if m.value is None:
                continue
            labels = ",".join(f"{k}={v}" for k, v in m.labels)
            key = f"{m.name}{{{labels}}}" if labels else m.name
            totals[key] = totals.get(key, 0.0) + float(m.value)
    return totals


def start_reporter(
    env: Mapping[str, str], probes: Callable[[], Mapping[str, Any]]
) -> None:
    every_s = interval_s(env)
    if every_s <= 0:
        return
    out = env.get(ENV_FILE, "").strip() or None
    if out:
        out = out.replace("{pid}", str(os.getpid()))
    top = int(env.get(ENV_TOP, "15") or 15)
    reporter = _Reporter(every_s, out, top, probes)
    threading.Thread(target=reporter.run, name="millrace-memdebug", daemon=True).start()
    log.info("memory diagnostics ON: every %.0fs -> %s", every_s, out or "log")
