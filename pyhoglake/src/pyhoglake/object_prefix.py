"""The object-prefix layout, as the server builds and checks it
(``ObjectPrefix.kt``): while a table's
:attr:`~pyhoglake.TableInfo.object_prefix_field` is set, a data file
registers only under ``<data_path>/data/<ns>/<table>/<field>=<value>/``,
value its identity partition value for the field, and its inline stats,
if it has any, must prove every row holds that value (else 422
:class:`~pyhoglake.ObjectPrefixViolationError`; deferred stats are
proven by the hydrator).

:func:`prefix`, :func:`expected_bound` and :func:`check_field_stats` take
the :class:`~pyhoglake.TableInfo` the payload's ``read_snapshot`` comes
from, and read the field's id and type from its column, as the server
reads them from the live one: a type kept apart from that read can be
one a promotion has replaced. They take the value as an integer and
write its canonical decimal themselves, so a file's key and its stats
name one value. :func:`parse_value` reads a partition value into it.

pyhoglake's own writers (``Table.append``, ``prepare_append_tables``,
``prepare_append_files``) do not use this: they write under the table
root, so the server refuses them on such a table. A writer of one builds
its keys here.

Held to ``tests/vectors/object_prefix_vectors.json`` as the server is;
containment (``isUnder``) stays the server's.
"""

from __future__ import annotations

import operator
import re
from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import Final, Literal, SupportsIndex

from .bounds import encode_bound
from .models import Column, ColumnStats, PartitionField, TableInfo

#: The types an object-prefix field may have, with their largest value: the
#: integer types whose values are non-negative longs (uint64 is Iceberg
#: decimal(20,0), so it is not one).
MAX_VALUE: Final[Mapping[str, int]] = MappingProxyType(
    {
        "int8": 2**7 - 1,
        "int16": 2**15 - 1,
        "int": 2**31 - 1,
        "long": 2**63 - 1,
        "uint8": 2**8 - 1,
        "uint16": 2**16 - 1,
        "uint32": 2**32 - 1,
    }
)
# ASCII digits, matched before int(), which also reads "٤٢" as 42.
_CANONICAL = re.compile(r"0|[1-9][0-9]{0,18}")
# The catalog's name grammar (schema.sql's CHECK, ObjectPrefix.NAME).
_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_-]{0,127}")

#: The outcome of :func:`check_field_stats`; only ``"OK"`` passes.
Verdict = Literal[
    "OK", "MISSING_STATS", "NULL_BOUNDS", "NULLS_PRESENT", "BOUND_MISMATCH"
]


def parse_value(raw: str | None, col_type: str) -> int | None:
    """The value ``raw`` names, or None when it is not a canonical decimal
    (no sign, no leading zero) in ``col_type``'s non-negative range: such a
    value cannot name a prefix."""
    if raw is None or col_type not in MAX_VALUE or not _CANONICAL.fullmatch(raw):
        return None
    value = int(raw)
    return value if value <= MAX_VALUE[col_type] else None


def key_index(fields: Sequence[PartitionField], field_id: int) -> int | None:
    """The key_index of the one identity partition field on ``field_id`` in
    a spec's ``fields``, or None when there is none or more than one."""
    found = [
        index
        for index, field in enumerate(fields)
        if field.source_field_id == field_id and field.transform == "identity"
    ]
    return found[0] if len(found) == 1 else None


def prefix(data_path: str, info: TableInfo, value: SupportsIndex) -> str:
    """``<data_path>/data/<ns>/<table>/<field>=<value>/`` for ``info``'s
    object-prefix field, always with the trailing ``/``, ``value`` written
    as its canonical decimal.

    A ``ValueError`` when ``info`` has no object-prefix field, the field
    is not a top-level column of a type in :data:`MAX_VALUE`, a name is
    outside the catalog's grammar, or ``value`` is outside the type's
    non-negative range: no file can register there. A value that is not
    an integer (a ``bool``, ``4.0``, ``"4"``) is a ``TypeError``."""
    column = _field(info)
    for name in (info.namespace, info.name, column.name):
        if not _NAME.fullmatch(name):
            raise ValueError(f"{name!r} cannot be a name in an object prefix")
    number = _in_range(column.type, value)
    root = f"{data_path.rstrip('/')}/data/{info.namespace}/{info.name}"
    return f"{root}/{column.name}={number}/"


def expected_bound(info: TableInfo, value: SupportsIndex) -> bytes:
    """Both bounds of a file whose every row holds ``value`` in ``info``'s
    object-prefix field: its Iceberg single value for the field's type, as
    :func:`~pyhoglake.encode_bound` writes it. Refuses what :func:`prefix`
    refuses, names aside."""
    column = _field(info)
    return encode_bound(column.type, _in_range(column.type, value))


def check_field_stats(
    info: TableInfo, value: SupportsIndex, stats: ColumnStats | None
) -> Verdict:
    """Whether a file's ``stats`` for ``info``'s object-prefix field prove
    that every row holds ``value``: both bounds equal to
    :func:`expected_bound` and no nulls. ``stats`` is None when the file
    has none for the field; stats for another field count as none. What
    :func:`prefix` refuses about the table or the value's type it refuses
    here too, names aside; a value out of range is ``"BOUND_MISMATCH"``,
    as on the server.

    The server makes this proof on SANITIZED stats: ``StatsSanity`` drops
    the bounds of a column with no non-null value (``value_count -
    null_count == 0``), so hand-built stats can pass here and fail there.
    :func:`~pyhoglake.stats.extract_column_stats` never builds that case."""
    column = _field(info)
    number = _integer(value)
    if stats is None or stats.field_id != column.field_id:
        return "MISSING_STATS"
    if stats.lower_bound is None or stats.upper_bound is None:
        return "NULL_BOUNDS"
    if stats.null_count != 0:
        return "NULLS_PRESENT"
    if not 0 <= number <= MAX_VALUE[column.type]:
        return "BOUND_MISMATCH"
    expected = encode_bound(column.type, number)
    if stats.lower_bound == expected == stats.upper_bound:
        return "OK"
    return "BOUND_MISMATCH"


def _field(info: TableInfo) -> Column:
    # The column of the read itself, as the commit checks against the live
    # column (CommitTail's layout join); its type is checked first.
    name = info.object_prefix_field
    where = f"{info.namespace}.{info.name}"
    if name is None:
        raise ValueError(f"{where} has no object-prefix field")
    column = next((c for c in info.columns if c.name == name), None)
    if column is None:
        raise ValueError(f"object-prefix field {name!r} is no column of {where}")
    if column.type not in MAX_VALUE:
        raise ValueError(f"{column.type!r} cannot be an object-prefix field")
    return column


def _integer(value: SupportsIndex) -> int:
    # A Long on the server: any integer, numpy's, Arrow's and an IntEnum
    # included, but not True, which is one only to Python, nor 4.0 or "4".
    if not isinstance(value, bool):
        try:
            return operator.index(value)
        except TypeError:
            pass
    raise TypeError(f"an object-prefix value is an int, not {value!r}")


def _in_range(col_type: str, value: SupportsIndex) -> int:
    number = _integer(value)
    if not 0 <= number <= MAX_VALUE[col_type]:
        raise ValueError(f"{number} is outside the prefix range of {col_type}")
    return number
