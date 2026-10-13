"""The staging group: a record's destination partition tuple, computed at
consume time (docs/kafka-ingestion.md §Staging layout).

A staged row is keyed by the hoglake partition tuple the destination
table would file it under — one wire string per field of the table's
live :class:`~pyhoglake.models.PartitionSpec`, in spec order — so the
flush unit is "one hoglake partition's staged rows" and a steady-state
flush writes exactly one file. :class:`GroupKeyer` is built from the
table's shape (:class:`~pyhoglake.TableInfo`) and maps one parsed JSON
payload to its tuple:

1. locate each partition field's source column by ``source_field_id``
   in the column TREE (a struct leaf is a legal partition source; its
   dotted path is recorded — a source under a list or a map, or a
   container itself, is refused: a row has no single value there; so is
   a json column, which has no transform domain);
2. read that scalar from the payload by path (absent or null anywhere
   along the path → a null component);
3. coerce it with the column's coercer (:func:`millrace.coerce.
   coercer_for` — the SAME coercion the flush's decoder applies, so the
   tuple is computed from the value the flush will write);
4. apply the transform (pyhoglake ``transforms.transform_value``) and
   render the component with ``transforms.wire_string`` — the string the
   commit's ``partition_values`` carry.

A malformed source scalar (wrong JSON type, uncoercible, a transform
the value cannot take, a parent that is not an object) raises
:class:`PartitionSourceError` naming the field; the consumer drops
the record. The tuple is an OPTIMIZATION of file layout, never its
truth: the flush fans every window out by the table's LIVE spec
(``flush.build_prepared_plan``), so a keyer built from an older spec
only groups rows under that spec's tuples, and the files stay correct.

:func:`field_names` renders the spec's fields the way hoglake's console
and partition listing do (the source column's dotted path for an
identity transform, ``<path>_<transform>`` otherwise — "ts_month"); the
names label a tuple in logs, the commit message and the ``prepared/``
envelope (:func:`millrace.keyspace.group_label`) — never the idempotency
key, which names fields positionally (``keyspace.group_key_form``) so it
cannot move with the spec.
"""

from __future__ import annotations

import struct
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from datetime import time as dtime
from decimal import Decimal, localcontext
from typing import Any, Final

from pyhoglake import Column, TableInfo, ValidationError
from pyhoglake.transforms import (  # the type gates are pyhoglake's own
    _BUCKETABLE,
    _TRUNCATABLE,
    transform_value,
    wire_string,
)

from .coerce import (
    TIMESTAMP_UNITS,
    DecodeError,
    UnsupportedColumnError,
    coercer_for,
)
from .keyspace import PartitionTuple

#: Container column types: never a partition source (no single value per
#: row); a source BELOW a list or a map has many values per row.
_CONTAINER_TYPES: Final = frozenset({"list", "struct", "map"})
_REPEATED_TYPES: Final = frozenset({"list", "map"})
#: Column types the JSON coercers fill but no partition source may be:
#: a json value has no transform domain (flush.py refuses the same
#: sources at startup).
_UNKEYABLE_TYPES: Final = frozenset({"json"})
_TEMPORAL_TRANSFORMS: Final = frozenset({"year", "month", "day", "hour"})
_EPOCH_NAIVE: Final = datetime.fromisoformat("1970-01-01T00:00:00")
_EPOCH_UTC: Final = datetime(1970, 1, 1, tzinfo=UTC)
_UINT16_MAX: Final = (1 << 16) - 1


class GroupKeyError(ValueError):
    """The table's partition spec cannot be keyed by this writer, decided
    when the keyer is BUILT (never per record). Refused: a source field
    id that resolves to no column; a container source (list/struct/map);
    a source under a list or a map; a json source; a source column the
    JSON coercers do not support (variant); an unknown transform; a
    ``bucket``/``truncate`` without its positive parameter; a temporal
    transform (year/month/day/hour) on a type pyhoglake's temporal gate
    refuses; ``bucket`` on a type outside pyhoglake's bucketable set
    (boolean, float, double, uint32, uint64, timestamp_s/ms/ns, json);
    and ``truncate`` on a type outside its truncatable set (only the int
    widths, string, binary and decimal truncate)."""


class PartitionSourceError(ValueError):
    """One payload's partition source scalar is malformed. ``path`` is
    the source column's dotted path (the drop reason names it)."""

    def __init__(self, path: str, detail: str) -> None:
        super().__init__(f"partition source {path!r}: {detail}")
        self.path = path


def column_chains(columns: Sequence[Column]) -> dict[int, tuple[Column, ...]]:
    """Every column's root-to-node chain, by field id (struct leaves
    included — a partition source may be one)."""
    out: dict[int, tuple[Column, ...]] = {}

    def walk(nodes: Sequence[Column], parent: tuple[Column, ...]) -> None:
        for node in nodes:
            chain = (*parent, node)
            out[node.field_id] = chain
            if node.children:
                walk(node.children, chain)

    walk(columns, ())
    return out


def field_names(info: TableInfo) -> tuple[str, ...]:
    """hoglake's display names for the live spec's fields, in spec order
    (the server's ``PartitionFieldLabels`` rule): the source column's
    dotted path for an identity transform, ``<path>_<transform>``
    otherwise; a source that resolves to no column is ``field_<id>``.
    Empty for an unpartitioned table."""
    spec = info.partition_spec
    if spec is None or not spec.fields:
        return ()
    chains = column_chains(info.columns)
    names = []
    for field in spec.fields:
        chain = chains.get(field.source_field_id)
        path = (
            ".".join(c.name for c in chain)
            if chain is not None
            else f"field_{field.source_field_id}"
        )
        names.append(
            path if field.transform == "identity" else f"{path}_{field.transform}"
        )
    return tuple(names)


def _float32(value: float) -> float:
    """The float32 a catalog ``float`` column stores (the value Arrow's
    narrowing cast produces and its ``to_pylist`` reads back)."""
    return float(struct.unpack(">f", struct.pack(">f", value))[0])


def _native(
    col_type: str,
    transform: str,
    value: Any,
    type_params: Mapping[str, Any] | None,
) -> Any:
    """A coerced column value → the Python value the flush's Arrow column
    yields for the same row (what ``transform_strings`` feeds
    ``transform_value``/``wire_string``), so both paths render one string.
    Raises OverflowError for an instant outside Python's datetime range."""
    if col_type in ("timestamp", "timestamp_s", "timestamp_ms"):
        return _EPOCH_NAIVE + timedelta(microseconds=value * TIMESTAMP_UNITS[col_type])
    if col_type == "timestamptz":
        return _EPOCH_UTC + timedelta(microseconds=value)
    if col_type == "timestamp_ns":
        # The flush casts timestamp[ns] to raw int64 nanos for the
        # per-value transforms and computes year/month/day/hour natively
        # (nanos floored to micros) — mirror both.
        if transform in _TEMPORAL_TRANSFORMS:
            return _EPOCH_NAIVE + timedelta(microseconds=value // 1000)
        return value
    if col_type == "time":
        seconds, micros = divmod(value, 1_000_000)
        minutes, second = divmod(seconds, 60)
        hour, minute = divmod(minutes, 60)
        return dtime(hour, minute, second, micros)
    if col_type == "float":
        return _float32(value)
    if col_type == "decimal":
        scale = int((type_params or {})["scale"])
        with localcontext() as ctx:
            ctx.prec = 60
            # decimal128(p, s) reads back at exactly s digits; the
            # coercer already proved the value fits without rounding.
            q = value.quantize(Decimal(1).scaleb(-scale))
        # Arrow stores the unscaled integer, which has no negative zero:
        # "-0" / "-0.0" read back as 0.00, so render them that way.
        return q.copy_abs() if q.is_zero() else q
    return value


@dataclass(frozen=True, slots=True)
class _SourcePlan:
    """One partition field's extraction plan."""

    path: tuple[str, ...]
    dotted: str
    transform: str
    param: int | None
    col_type: str
    type_params: dict[str, Any] | None
    coerce: Callable[[Any], Any]


class GroupKeyer:
    """Payload → partition tuple under ONE table shape (module
    docstring). Built once per shape: construction validates the spec
    and raises :class:`GroupKeyError` for one it cannot key."""

    def __init__(self, info: TableInfo) -> None:
        spec = info.partition_spec
        self._spec_id = spec.spec_id if spec is not None else None
        self._fields = field_names(info)
        chains = column_chains(info.columns)
        plans: list[_SourcePlan] = []
        for field in spec.fields if spec is not None else ():
            chain = chains.get(field.source_field_id)
            if chain is None:
                raise GroupKeyError(
                    f"partition spec (spec_id={self._spec_id}) references field_id "
                    f"{field.source_field_id}, which is not a live column of "
                    f"{info.namespace}.{info.name}"
                )
            leaf = chain[-1]
            dotted = ".".join(c.name for c in chain)
            if leaf.type in _CONTAINER_TYPES:
                raise GroupKeyError(
                    f"partition source {dotted!r} is a {leaf.type!r}: a container "
                    "has no single value per row"
                )
            repeated = next((c for c in chain[:-1] if c.type in _REPEATED_TYPES), None)
            if repeated is not None:
                raise GroupKeyError(
                    f"partition source {dotted!r} sits under {repeated.name!r}, a "
                    f"{repeated.type!r}: a row has many such values"
                )
            if leaf.type in _UNKEYABLE_TYPES:
                raise GroupKeyError(
                    f"partition source {dotted!r} cannot be keyed: a {leaf.type!r} "
                    "column is not a partition source"
                )
            try:
                coerce = coercer_for(leaf)
                transform_value(
                    field.transform,
                    field.transform_param,
                    leaf.type,
                    None,
                    leaf.type_params,
                )
            except (UnsupportedColumnError, ValidationError) as e:
                raise GroupKeyError(
                    f"partition source {dotted!r} cannot be keyed: {e}"
                ) from e
            # transform_value(…, None) short-circuits bucket/truncate on a
            # null before the type gate, so check those gates here — or
            # every record would construct fine and then drop.
            gate = {"bucket": _BUCKETABLE, "truncate": _TRUNCATABLE}.get(
                field.transform
            )
            if gate is not None and leaf.type not in gate:
                raise GroupKeyError(
                    f"partition source {dotted!r} cannot be keyed: "
                    f"{field.transform} cannot be applied to column type "
                    f"{leaf.type!r} (allowed: {sorted(gate)})"
                )
            plans.append(
                _SourcePlan(
                    path=tuple(c.name for c in chain),
                    dotted=dotted,
                    transform=field.transform,
                    param=field.transform_param,
                    col_type=leaf.type,
                    type_params=leaf.type_params,
                    coerce=coerce,
                )
            )
        self._plans = tuple(plans)

    @property
    def spec_id(self) -> int | None:
        """The spec this keyer groups under (None: unpartitioned)."""
        return self._spec_id

    @property
    def fields(self) -> tuple[str, ...]:
        """The spec's fields' display names (:func:`field_names`)."""
        return self._fields

    def group_for(self, doc: Mapping[str, Any]) -> PartitionTuple:
        """The partition tuple of one parsed payload object. Raises
        :class:`PartitionSourceError` for a malformed source scalar."""
        out: list[str | None] = []
        for plan in self._plans:
            raw: Any = doc
            for step in plan.path:
                if raw is None:
                    break
                if not isinstance(raw, dict):
                    raise PartitionSourceError(
                        plan.dotted,
                        f"expected a JSON object on the path, got {type(raw).__name__}",
                    )
                raw = raw.get(step)
            if raw is None:
                out.append(None)
                continue
            try:
                value = plan.coerce(raw)
                transformed = transform_value(
                    plan.transform,
                    plan.param,
                    plan.col_type,
                    _native(plan.col_type, plan.transform, value, plan.type_params),
                    plan.type_params,
                )
                wire = wire_string(plan.col_type, plan.transform, transformed)
            except (DecodeError, ValidationError, OverflowError, ValueError) as e:
                raise PartitionSourceError(plan.dotted, str(e)) from e
            if wire is not None and len(wire.encode("utf-8")) > _UINT16_MAX:
                raise PartitionSourceError(
                    plan.dotted,
                    f"the partition value is {len(wire.encode('utf-8'))} bytes; "
                    "a staging key component holds at most 65535",
                )
            out.append(wire)
        return tuple(out)
