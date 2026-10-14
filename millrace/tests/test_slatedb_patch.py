"""The slatedb binding byte-copy patch (millrace/slatedb_patch.py).

The patch replaces the binding's per-byte ``_UniffiRustBufferBuilder.write``
with one ``ctypes.memmove``. These tests pin that it is applied by the
stage module, idempotent, byte-exact through every lowering path the
stage uses (put/get, WriteBatch, scan), and that a binding of another
shape is left alone with a WARNING.
"""

from __future__ import annotations

import array
import asyncio
import contextlib
import ctypes
import logging
import types
from typing import Any

import pytest
from slatedb.uniffi import DbBuilder, KeyRange, ObjectStore, WriteBatch

import millrace.stage  # noqa: F401 - importing the stage applies the patch
from millrace import slatedb_patch

pytestmark = pytest.mark.component

VALUES: list[bytes] = (
    [bytes([b]) for b in range(256)]
    + [bytes(range(256))]
    + [bytes((i * 7 + 3) % 256 for i in range(n)) for n in (0, 1, 2, 255, 4096)]
    + [bytes((i * 31 + 11) % 256 for i in range(1 << 20))]
)


def test_the_stage_module_applies_the_patch() -> None:
    assert slatedb_patch.applied()
    assert slatedb_patch.apply() is True  # idempotent
    assert slatedb_patch.applied()


async def test_every_byte_value_and_length_round_trips_byte_identical() -> None:
    assert slatedb_patch.applied()
    db = await DbBuilder("patch/rt", ObjectStore.resolve("memory:///")).build()
    try:
        # put/get
        for i, value in enumerate(VALUES):
            await db.put(b"p/%06d" % i, value)
        for i, value in enumerate(VALUES):
            assert await db.get(b"p/%06d" % i) == value
        # one WriteBatch, read back by get and by scan
        batch = WriteBatch()
        for i, value in enumerate(VALUES):
            batch.put(b"w/%06d" % i, value)
        handle = await db.write(batch)
        await handle.await_durable()
        for i, value in enumerate(VALUES):
            assert await db.get(b"w/%06d" % i) == value
        it = await db.scan(
            KeyRange(start=b"w/", start_inclusive=True, end=b"w0", end_inclusive=False)
        )
        scanned: list[tuple[bytes, bytes]] = []
        while chunk := await it.next_batch(64):
            scanned.extend((kv.key, kv.value) for kv in chunk)
        assert scanned == [(b"w/%06d" % i, v) for i, v in enumerate(VALUES)]
        # keys go through the same lowering
        odd_key = bytes(range(256))
        await db.put(odd_key, b"k")
        assert await db.get(odd_key) == b"k"
    finally:
        await db.shutdown()


def _fake_module(
    *, data_type: Any = None, write: Any = None, len_type: Any = ctypes.c_uint64
) -> Any:
    class Buffer(ctypes.Structure):
        _fields_ = [
            ("capacity", ctypes.c_uint64),
            ("len", len_type),
            ("data", data_type or ctypes.POINTER(ctypes.c_char)),
        ]

    class Builder:
        def _reserve(self, n: int) -> Any:  # pragma: no cover - never reached
            raise AssertionError

    def original_write(self: Any, value: bytes) -> None:  # pragma: no cover
        raise AssertionError

    Builder.write = write or original_write  # type: ignore[attr-defined]
    return types.SimpleNamespace(
        _UniffiRustBufferBuilder=Builder, _UniffiRustBuffer=Buffer
    )


def _fake_module_without_reserve() -> Any:
    module = _fake_module()
    del module._UniffiRustBufferBuilder._reserve
    return module


@pytest.mark.parametrize(
    "module",
    [
        types.SimpleNamespace(),
        _fake_module(data_type=ctypes.POINTER(ctypes.c_uint8)),
        _fake_module(len_type=ctypes.c_double),
        _fake_module_without_reserve(),
    ],
    ids=["no-classes", "data-type", "len-type", "no-reserve"],
)
def test_a_binding_of_another_shape_is_left_alone_with_a_warning(
    module: Any, caplog: pytest.LogCaptureFixture
) -> None:
    builder = getattr(module, "_UniffiRustBufferBuilder", None)
    before = getattr(builder, "write", None)
    with caplog.at_level(logging.WARNING, logger="millrace.slatedb_patch"):
        assert slatedb_patch.apply(module) is False
    assert "not applied" in caplog.text
    assert getattr(builder, "write", None) is before
    assert not slatedb_patch.applied(module)


def test_a_failing_self_test_restores_the_original(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from slatedb.uniffi._slatedb_uniffi import slatedb as binding

    builder = binding._UniffiRustBufferBuilder
    patched = builder.write

    def original(self: Any, value: bytes) -> None:
        raise AssertionError("never called")

    monkeypatch.setattr(builder, "write", original)
    monkeypatch.setattr(slatedb_patch, "_self_test", lambda cls: False)
    with caplog.at_level(logging.WARNING, logger="millrace.slatedb_patch"):
        assert slatedb_patch.apply(binding) is False
    assert builder.write is original
    assert "self-test mismatch" in caplog.text

    def raising(cls: Any) -> bool:
        raise RuntimeError("boom")

    monkeypatch.setattr(slatedb_patch, "_self_test", raising)
    assert slatedb_patch.apply(binding) is False
    assert builder.write is original
    monkeypatch.undo()
    assert builder.write is patched


def test_the_real_self_test_passes_on_the_patched_builder() -> None:
    from slatedb.uniffi._slatedb_uniffi import slatedb as binding

    assert slatedb_patch._self_test(binding._UniffiRustBufferBuilder)


def test_an_unimportable_binding_is_a_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(slatedb_patch, "_BINDING", "no.such.binding_module")
    with caplog.at_level(logging.WARNING, logger="millrace.slatedb_patch"):
        assert slatedb_patch.apply() is False
    assert "not applied" in caplog.text


def _binding() -> Any:
    from slatedb.uniffi._slatedb_uniffi import slatedb as binding

    return binding


def test_the_real_self_test_fails_when_reserve_does_not_advance_len() -> None:
    binding = _binding()

    class NoAdvance(binding._UniffiRustBufferBuilder):  # type: ignore[misc,name-defined]
        @contextlib.contextmanager
        def _reserve(self, num_bytes: int) -> Any:
            if self.rbuf.len + num_bytes > self.rbuf.capacity:
                self.rbuf = binding._UniffiRustBuffer.reserve(self.rbuf, num_bytes)
            yield None  # never advances len

    assert slatedb_patch._self_test(NoAdvance) is False


def _written(builder_cls: Any, *values: Any) -> bytes:
    builder = builder_cls()
    try:
        for value in values:
            builder.write(value)
        rbuf = builder.rbuf
        return bytes(rbuf.data[0 : int(rbuf.len)])
    finally:
        builder.discard()


def test_bytearray_and_memoryview_lower_like_bytes() -> None:
    builder_cls = _binding()._UniffiRustBufferBuilder
    raw = bytes(range(256)) * 5
    assert _written(builder_cls, bytearray(raw)) == raw
    assert _written(builder_cls, memoryview(raw)) == raw
    assert _written(builder_cls, memoryview(raw)[3:700]) == raw[3:700]
    assert _written(builder_cls, b"a", bytearray(b"b"), memoryview(b"c")) == b"abc"


async def test_bytearray_and_memoryview_round_trip_through_slatedb() -> None:
    db = await DbBuilder("patch/views", ObjectStore.resolve("memory:///")).build()
    try:
        raw = bytes((i * 13) % 256 for i in range(70_000))
        await db.put(b"ba", bytearray(raw))
        await db.put(b"mv", memoryview(raw)[1:])
        batch = WriteBatch()
        batch.put(bytearray(b"wb-ba"), bytearray(raw))
        batch.put(b"wb-mv", memoryview(raw))
        handle = await db.write(batch)
        await handle.await_durable()
        assert await db.get(b"ba") == raw
        assert await db.get(b"mv") == raw[1:]
        assert await db.get(b"wb-ba") == raw
        assert await db.get(b"wb-mv") == raw
    finally:
        await db.shutdown()


def test_a_buffer_of_wider_items_falls_back_to_the_original_write() -> None:
    calls: list[Any] = []

    def original(self: Any, value: Any) -> None:
        calls.append(value)

    write = slatedb_patch._make_write(original)

    class NoTouch:
        def _reserve(self, n: int) -> Any:  # pragma: no cover - must not run
            raise AssertionError("the fast path ran")

    wide = array.array("H", [1, 513, 65535])
    view = memoryview(wide)
    strided = memoryview(b"abcdef")[::2]
    for value in (wide, view, strided, [1, 2, 3]):
        write(NoTouch(), value)
    assert calls == [wide, view, strided, [1, 2, 3]]
    # Nothing was truncated: a wide buffer never reaches the byte copy.
    assert len(bytes(wide)) == 6 and len(wide) == 3


async def test_concurrent_off_loop_batch_builds_are_byte_exact() -> None:
    db = await DbBuilder("patch/concurrent", ObjectStore.resolve("memory:///")).build()

    def build(worker: int) -> WriteBatch:
        batch = WriteBatch()
        for i in range(200):
            batch.put(b"c/%02d/%04d" % (worker, i), _payload(worker, i))
        return batch

    try:
        batches = await asyncio.gather(
            *(asyncio.to_thread(build, w) for w in range(16))
        )
        for batch in batches:
            handle = await db.write(batch)
            await handle.await_durable()
        for worker in range(16):
            for i in range(200):
                assert await db.get(b"c/%02d/%04d" % (worker, i)) == _payload(worker, i)
    finally:
        await db.shutdown()


def _payload(worker: int, i: int) -> bytes:
    return bytes((worker * 31 + i * 7 + k) % 256 for k in range(100 + (i * 37) % 4000))
