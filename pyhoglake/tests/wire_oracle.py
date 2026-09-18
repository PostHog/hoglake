"""The language-independent oracle for the `wire` cells of
tests/vectors/bounds_vectors.json.

Every vector carries a `wire` field — the decoded bound as it appears in
a JSON response of GET .../files/{fileId}/stats or POST
/v1/debug/decode-bound — or a `wire_refused` marker (a value the JSON
layer refuses to render: NaN bounds). The bound_normalization cases
carry `raw_wire`/`stored_wire`, the renderings of `raw_hex` and
`stored_hex`. The SERVER renders these tokens with java.time and
Jackson; this module recomputes every one of them from the vector's own
`value`/`hex` fields with neither, so the file is an oracle two
independent implementations must satisfy rather than a transcript of
one of them.

The temporal conventions are therefore spelled out HERE, as arithmetic,
not inherited from any datetime library (Python's `datetime` cannot even
represent some of the vectors — year 294247):

- Dates are proleptic-Gregorian ISO-8601: years 0..9999 print as four
  zero-padded digits, years above 9999 with an explicit ``+`` and no
  padding, negative years with ``-`` (padded to four digits of year).
- Times and the time part of naive timestamps ELIDE trailing zero
  units: the seconds field is omitted when both seconds and the
  fraction are zero ("00:00", "1970-01-01T00:00" — valid ISO-8601;
  consumers must not fixed-precision-parse).
- A nonzero fraction prints in groups of three digits: exactly 3 when
  it is whole milliseconds, 6 when whole microseconds, 9 otherwise.
- timestamptz renders as a UTC instant with a trailing ``Z`` and NEVER
  elides the seconds field (instants keep full "HH:MM:SS").
- timestamp_s/timestamp_ms bounds are STORED in microseconds (the
  mapped Iceberg type's unit), timestamp_ns in nanoseconds.

Numeric conventions: every integer family is its exact decimal token;
decimal is the unscaled integer scaled by 10^-scale, in PLAIN notation
with exactly `scale` fraction digits (never scientific — a
decimal(38,38) of unscaled 1 is "0." plus 37 zeros plus "1", not
1E-38); float/double are their shortest round-trip decimal tokens with
the string sentinels "Infinity"/"-Infinity", and NaN is refused.
string/json render as the string, uuid canonically, binary as standard
base64 of the raw bytes.

Regeneration: ``uv run python tests/wire_oracle.py --write`` recomputes
every wire cell in place (all other content, formatting, and key order
preserved byte-for-byte); ``--check`` (the default, also run by
qe_vectors.test_wire_tokens_match_the_python_oracle) reports any cell
that disagrees with this module and exits nonzero. A disagreement is a
cross-language finding to investigate, never a cell to paste over.
"""

import base64
import json
import struct
import sys
from pathlib import Path

VECTOR_PATH = Path(__file__).parent / "vectors" / "bounds_vectors.json"

MICROS = 1_000_000
NANOS = 1_000_000_000


class RawToken(str):
    """A JSON number carried as its exact source token."""


# ---- ISO-8601 rendering, arithmetically ------------------------------------


def _civil_from_days(days: int) -> tuple[int, int, int]:
    """Proleptic-Gregorian (year, month, day) for days since 1970-01-01
    (Howard Hinnant's civil_from_days; exact over the whole int64 range,
    unlike any stdlib datetime)."""
    z = days + 719468
    era = z // 146097
    doe = z - era * 146097
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    y = yoe + era * 400
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    d = doy - (153 * mp + 2) // 5 + 1
    m = mp + 3 if mp < 10 else mp - 9
    return y + (1 if m <= 2 else 0), m, d


def _year_token(year: int) -> str:
    if year > 9999:
        return f"+{year}"
    if year < 0:
        return f"-{abs(year):04d}"
    return f"{year:04d}"


def _date_token(days: int) -> str:
    y, m, d = _civil_from_days(days)
    return f"{_year_token(y)}-{m:02d}-{d:02d}"


def _fraction_token(nanos: int) -> str:
    """Nonzero sub-second fractions print in groups of three digits."""
    if nanos == 0:
        return ""
    if nanos % 1_000_000 == 0:
        return f".{nanos // 1_000_000:03d}"
    if nanos % 1_000 == 0:
        return f".{nanos // 1_000:06d}"
    return f".{nanos:09d}"


def _time_token(second_of_day: int, nanos: int, always_seconds: bool) -> str:
    h, rem = divmod(second_of_day, 3600)
    m, s = divmod(rem, 60)
    out = f"{h:02d}:{m:02d}"
    if always_seconds or s or nanos:
        out += f":{s:02d}{_fraction_token(nanos)}"
    return out


def _datetime_token(value: int, unit: int, always_seconds: bool = False) -> str:
    """value counts of 1/unit seconds since the epoch -> ISO token."""
    sec, rem = divmod(value, unit)
    nanos = rem * (NANOS // unit)
    days, second_of_day = divmod(sec, 86_400)
    return _date_token(days) + "T" + _time_token(second_of_day, nanos, always_seconds)


# ---- numeric tokens --------------------------------------------------------


def _decimal_token(unscaled: int, scale: int) -> str:
    """unscaled x 10^-scale in plain notation, exactly `scale` fraction
    digits — never scientific."""
    sign = "-" if unscaled < 0 else ""
    digits = str(abs(unscaled))
    if scale == 0:
        return sign + digits
    digits = digits.rjust(scale + 1, "0")
    return f"{sign}{digits[:-scale]}.{digits[-scale:]}"


def _float_token(value: str) -> str:
    """Shortest round-trip decimal token (Python repr is shortest
    round-trip for binary64, and every stored float32 is exact in it)."""
    return repr(float(value))


# ---- the oracle ------------------------------------------------------------

REFUSED = object()

_INT_TYPES = {
    "int8",
    "int16",
    "int",
    "long",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
}


def render_wire(vec: dict) -> object:
    """The wire form for one vector, computed from `value`/`hex` only:
    RawToken for a JSON number, str for a JSON string, bool for a JSON
    boolean, or REFUSED."""
    t, v = vec["type"], vec["value"]
    if t == "boolean":
        return v == "true"
    if t in _INT_TYPES:
        return RawToken(str(int(v)))
    if t in ("float", "double"):
        if v == "NaN":
            return REFUSED
        if v in ("Infinity", "-Infinity"):
            return v
        return RawToken(_float_token(v))
    if t == "decimal":
        scale = (vec["type_params"] or {}).get("scale", 0)
        return RawToken(_decimal_token(int(v), int(scale)))
    if t == "date":
        return _date_token(int(v))
    if t == "time":
        second_of_day, rem = divmod(int(v), MICROS)
        return _time_token(second_of_day, rem * 1_000, always_seconds=False)
    if t in ("timestamp_s", "timestamp_ms", "timestamp"):
        return _datetime_token(int(v), MICROS)
    if t == "timestamp_ns":
        return _datetime_token(int(v), NANOS)
    if t == "timestamptz":
        return _datetime_token(int(v), MICROS, always_seconds=True) + "Z"
    if t in ("string", "json"):
        decoded = bytes.fromhex(vec["hex"]).decode("utf-8")
        if decoded != v:
            raise AssertionError(f"hex/value disagree for {t} vector {v!r}")
        return v
    if t == "uuid":
        return v
    if t == "binary":
        return base64.b64encode(bytes.fromhex(vec["hex"])).decode("ascii")
    raise AssertionError(f"no wire convention for type {t!r}")


def render_normalization_wire(col_type: str, hex_bytes: str) -> object:
    """raw_wire/stored_wire for a bound_normalization case: the wire form
    of the case's own bytes."""
    raw = bytes.fromhex(hex_bytes)
    if col_type == "float":
        return RawToken(repr(struct.unpack("<f", raw)[0]))
    if col_type == "double":
        return RawToken(repr(struct.unpack("<d", raw)[0]))
    if col_type == "int":
        return RawToken(str(struct.unpack("<i", raw)[0]))
    if col_type == "long":
        return RawToken(str(struct.unpack("<q", raw)[0]))
    if col_type == "decimal":
        # Big-endian minimal two's complement; the fixed-point cases
        # carry scale 0, so the unscaled integer IS the token.
        return RawToken(str(int.from_bytes(raw, "big", signed=True)))
    raise AssertionError(f"no normalization wire convention for type {col_type!r}")


# ---- token-preserving file access ------------------------------------------


def load_raw(path: Path = VECTOR_PATH) -> dict:
    """Parse the vector file with every JSON number kept as its exact
    source token (RawToken), so token-level comparison is possible."""
    with path.open(encoding="utf-8") as f:
        return json.load(f, parse_float=RawToken, parse_int=RawToken)


def _dump_raw(doc: dict) -> str:
    """json.dumps(indent=2) with RawTokens emitted verbatim as numbers —
    the file's exact on-disk format (verified byte-for-byte)."""
    placeholders: dict[str, str] = {}

    def protect(node: object) -> object:
        if isinstance(node, RawToken):
            key = "\x00raw" + str(len(placeholders)) + "\x00"
            placeholders[key] = str(node)
            return key
        if isinstance(node, dict):
            return {k: protect(v) for k, v in node.items()}
        if isinstance(node, list):
            return [protect(v) for v in node]
        return node

    text = json.dumps(protect(doc), indent=2)
    for key, token in placeholders.items():
        text = text.replace(json.dumps(key), token)
    return text + "\n"


def _expected_cells(doc: dict) -> list[tuple[str, dict, str, object]]:
    """(label, holder, field, expected) for every wire-bearing cell."""
    cells = []
    for i, vec in enumerate(doc["vectors"]):
        label = f"vectors[{i}] {vec['type']} value={vec['value']!r:.32}"
        cells.append((label, vec, "wire", render_wire(vec)))
    for i, case in enumerate(doc["bound_normalization"]["cases"]):
        label = f"bound_normalization[{i}] {case['type']} {case['role']}"
        for hex_field, wire_field in (
            ("raw_hex", "raw_wire"),
            ("stored_hex", "stored_wire"),
        ):
            expected = render_normalization_wire(case["type"], case[hex_field])
            cells.append((label, case, wire_field, expected))
    return cells


def check(path: Path = VECTOR_PATH) -> list[str]:
    """Every wire-bearing cell that disagrees with this oracle, described.
    Empty means the file and the oracle agree exactly."""
    doc = load_raw(path)
    problems = []
    for label, holder, field, expected in _expected_cells(doc):
        if expected is REFUSED:
            if field == "wire" and "wire_refused" not in holder:
                problems.append(f"{label}: oracle refuses, file has no wire_refused")
            if field == "wire" and "wire" in holder:
                problems.append(f"{label}: oracle refuses, file carries wire")
            continue
        if field == "wire" and "wire_refused" in holder:
            problems.append(f"{label}: file refuses, oracle renders {expected!r}")
            continue
        actual = holder.get(field)
        if actual is None:
            problems.append(f"{label}: missing {field} (oracle: {expected!r})")
        elif type(actual) is not type(expected) or str(actual) != str(expected):
            problems.append(f"{label}: {field} is {actual!r}, oracle says {expected!r}")
    return problems


def write(path: Path = VECTOR_PATH) -> None:
    """Regenerate every wire cell in place; everything else — formatting
    included — is preserved byte-for-byte."""
    doc = load_raw(path)
    for _, holder, field, expected in _expected_cells(doc):
        if expected is REFUSED:
            holder.pop("wire", None)
            holder.setdefault("wire_refused", "nan_bound")
        else:
            holder.pop("wire_refused", None)
            holder[field] = expected
    path.write_text(_dump_raw(doc), encoding="utf-8")


def main(argv: list[str]) -> int:
    if argv == ["--write"]:
        write()
        return 0
    if argv in ([], ["--check"]):
        problems = check()
        for p in problems:
            print(p)
        return 1 if problems else 0
    print("usage: wire_oracle.py [--check | --write]")
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
