"""The object-prefix layout, as the server builds and checks it
(``ObjectPrefix.kt``): while a table's
:attr:`~pyhoglake.TableInfo.object_prefix_field` is set, a data file
registers only under ``<data_path>/data/<ns>/<table>/<field>=<value>/``,
value its identity partition value for the field, and its inline stats
must prove every row holds that value (else 422 ``object_prefix_violation``).
Held to ``tests/vectors/object_prefix_vectors.json`` as the server is;
containment (``isUnder``) stays the server's.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from types import MappingProxyType
from typing import Final, Literal

from .bounds import encode_bound
from .models import ColumnStats, PartitionField

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


def prefix(
    data_path: str, namespace: str, table: str, field: str, col_type: str, value: str
) -> str:
    """``<data_path>/data/<namespace>/<table>/<field>=<value>/``, always with
    the trailing ``/``. A name outside the catalog's grammar, or a value
    :func:`parse_value` refuses, is a ``ValueError``: no file can register
    there."""
    for name in (namespace, table, field):
        if not _NAME.fullmatch(name):
            raise ValueError(f"{name!r} cannot be a name in an object prefix")
    if parse_value(value, col_type) is None:
        raise ValueError(f"{value!r} is not a canonical {col_type} prefix value")
    return f"{data_path.rstrip('/')}/data/{namespace}/{table}/{field}={value}/"


def expected_bound(col_type: str, value: int) -> bytes:
    """Both bounds of a file whose every row holds ``value``: its Iceberg
    single value, as :func:`~pyhoglake.encode_bound` writes it."""
    _require_int(value)
    if not 0 <= value <= _max_value(col_type):
        raise ValueError(f"{value} is outside the prefix range of {col_type}")
    return encode_bound(col_type, value)


def check_field_stats(
    field_id: int, col_type: str, value: int, stats: ColumnStats | None
) -> Verdict:
    """Whether a file's ``stats`` for the prefix field ``field_id`` prove
    that every row holds ``value``: both bounds equal to
    :func:`expected_bound` and no nulls. ``stats`` is None when the file
    has none for the field; stats for another field count as none.

    The server makes this proof on SANITIZED stats: ``StatsSanity`` drops
    the bounds of a column with no non-null value (``value_count -
    null_count == 0``), so hand-built stats can pass here and fail there.
    :func:`~pyhoglake.stats.extract_column_stats` never builds that case."""
    _require_int(value)
    if stats is None or stats.field_id != field_id:
        return "MISSING_STATS"
    if stats.lower_bound is None or stats.upper_bound is None:
        return "NULL_BOUNDS"
    if stats.null_count != 0:
        return "NULLS_PRESENT"
    if not 0 <= value <= _max_value(col_type):
        return "BOUND_MISMATCH"
    expected = expected_bound(col_type, value)
    if stats.lower_bound == expected == stats.upper_bound:
        return "OK"
    return "BOUND_MISMATCH"


def _require_int(value: int) -> None:
    # A Long on the server, which cannot carry 4.7 or True.
    if type(value) is not int:
        raise TypeError(f"an object-prefix value is an int, not {value!r}")


def _max_value(col_type: str) -> int:
    if col_type not in MAX_VALUE:
        raise ValueError(f"{col_type!r} cannot be an object-prefix field")
    return MAX_VALUE[col_type]
