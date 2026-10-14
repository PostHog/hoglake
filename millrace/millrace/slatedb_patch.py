"""Replace the slatedb uniffi binding's per-byte buffer copy.

slatedb 0.17's generated ``_UniffiRustBufferBuilder.write`` copies every
lowered ``bytes`` value into the Rust buffer one byte per Python loop
iteration. Every staged payload goes through it on ``WriteBatch.put``;
during a Kafka catch-up it was 90% of GIL time and capped staging at
about 3.3k records/s. :func:`apply` swaps it for one ``ctypes.memmove``
of the whole value into the same reserved region (same bytes, same
offset, inside the binding's own ``_reserve`` context, which grows the
buffer and advances ``len`` exactly as before).

The patch is idempotent. If the binding's shape is not the one this was
written against, or a self-test write does not read back identically,
the binding is left alone and a WARNING is logged: staging stays
correct, only slower.
"""

from __future__ import annotations

import ctypes
import importlib
import logging
from typing import Any, Final

log = logging.getLogger(__name__)

_BINDING: Final = "slatedb.uniffi._slatedb_uniffi.slatedb"
_MARK: Final = "_millrace_memmove"


def _make_write(original: Any) -> Any:
    """The fast ``write``: bytes and byte-sized contiguous buffers are
    copied with one memmove; anything else (a buffer of wider items, a
    non-contiguous view, a plain sequence) goes to the binding's own
    ``write`` unchanged, so nothing is ever truncated."""

    def write(self: Any, value: Any) -> None:
        if isinstance(value, bytes):
            src = value
        else:
            try:
                view = memoryview(value)
            except TypeError:
                original(self, value)
                return
            if view.itemsize != 1 or not view.c_contiguous:
                original(self, value)
                return
            src = view.tobytes()
        n = len(src)
        with self._reserve(n):
            if n:
                dst = ctypes.cast(self.rbuf.data, ctypes.c_void_p).value
                ctypes.memmove(dst + self.rbuf.len, src, n)
        return

    setattr(write, _MARK, True)
    return write


def _shape_ok(module: Any) -> str | None:
    """None when the binding looks like the one this patch targets, else
    why not."""
    builder = getattr(module, "_UniffiRustBufferBuilder", None)
    buffer = getattr(module, "_UniffiRustBuffer", None)
    if builder is None or buffer is None:
        return "no _UniffiRustBufferBuilder/_UniffiRustBuffer"
    if not callable(getattr(builder, "write", None)) or not callable(
        getattr(builder, "_reserve", None)
    ):
        return "_UniffiRustBufferBuilder has no write/_reserve"
    fields = dict(getattr(buffer, "_fields_", ()))
    if fields.get("data") is not ctypes.POINTER(ctypes.c_char):
        return "_UniffiRustBuffer.data is not POINTER(c_char)"
    if fields.get("len") not in (ctypes.c_uint64, ctypes.c_int32, ctypes.c_int64):
        return "_UniffiRustBuffer.len is not an integer field"
    return None


def _self_test(builder_cls: Any) -> bool:
    """Write a known value through the patched builder and read it back
    from the Rust buffer."""
    sample = bytes(range(256)) * 3 + b"\x00\xff"
    builder = builder_cls()
    try:
        builder.write(b"ab")
        builder.write(sample)
        rbuf = builder.rbuf
        ok = int(rbuf.len) == 2 + len(sample) and bytes(
            rbuf.data[0 : int(rbuf.len)]
        ) == (b"ab" + sample)
    finally:
        builder.discard()
    return ok


def apply(module: Any = None) -> bool:
    """Patch the binding (or ``module``, for tests). Returns whether the
    fast copy is in place."""
    if module is None:
        try:
            module = importlib.import_module(_BINDING)
        except ImportError as e:
            log.warning("slatedb binding byte-copy patch not applied: %s", e)
            return False
    problem = _shape_ok(module)
    if problem is not None:
        log.warning(
            "slatedb binding byte-copy patch not applied (unexpected binding "
            "shape: %s); staging runs at the binding's per-byte copy speed",
            problem,
        )
        return False
    builder_cls = module._UniffiRustBufferBuilder
    if getattr(builder_cls.write, _MARK, False):
        return True
    original = builder_cls.write
    builder_cls.write = _make_write(original)
    try:
        ok = _self_test(builder_cls)
    except Exception as e:  # noqa: BLE001 - any failure restores the original
        ok = False
        log.warning("slatedb binding byte-copy self-test raised: %r", e)
    if not ok:
        builder_cls.write = original
        log.warning(
            "slatedb binding byte-copy patch not applied (self-test mismatch); "
            "staging runs at the binding's per-byte copy speed"
        )
        return False
    return True


def applied(module: Any = None) -> bool:
    """Whether the binding's builder currently carries the fast copy."""
    if module is None:
        module = importlib.import_module(_BINDING)
    builder = getattr(module, "_UniffiRustBufferBuilder", None)
    return bool(getattr(getattr(builder, "write", None), _MARK, False))
