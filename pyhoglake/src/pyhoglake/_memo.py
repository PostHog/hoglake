"""A small bounded memo for the writer's per-destination derivations (plans,
stamped schema lists, schema verdicts), shared between threads.

Each is a pure function of its key that the writer would otherwise redo for
every partition group of a flush. ``functools.lru_cache`` would do where
the key is hashable and the result needs no confirmation; these hold a
value beside what it was derived from, so a caller can confirm a hit by
equality where the key alone is not proof (two Arrow schemas of one IPC
serialisation, say). Least recently used out first, so a writer cycling
through more destinations than the bound still hits on the ones it comes
back to; every operation under one lock, since an eviction racing a lookup
in a plain dict escapes as KeyError or RuntimeError from the write.
"""

from __future__ import annotations

import threading
from collections import OrderedDict


class Memo[K, V]:
    def __init__(self, kept: int) -> None:
        #: How many entries are kept; the least recently used goes first.
        self.kept = kept
        self._items: OrderedDict[K, V] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: K) -> V | None:
        """The value put under ``key``, now the most recently used, or None."""
        with self._lock:
            value = self._items.get(key)
            if value is not None:
                self._items.move_to_end(key)
            return value

    def put(self, key: K, value: V) -> V:
        """Keeps ``value`` under ``key`` (replacing one there), dropping the
        least recently used past the bound; returns ``value``."""
        with self._lock:
            self._items[key] = value
            self._items.move_to_end(key)
            while len(self._items) > self.kept:
                self._items.popitem(last=False)
        return value

    def clear(self) -> None:
        with self._lock:
            self._items.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)
