# Mutation testing the pure core — survivor ledger

Scope: `[tool.mutmut]` in `pyproject.toml` (mutmut 3.8.0, pinned).
Mutated: `millrace/keyspace.py` (the key codecs) and `millrace/planner.py`
(the flush predicate). Driver: `tests/test_keyspace.py` +
`tests/test_planner.py` (`-x -q`). Run from `millrace/`:

```
just mutants            # clean-slate run + the survivors gate
```

or by hand:

```
rm -rf mutants
uv run mutmut run
uv run python tests/check_mutants.py
uv run mutmut show <mutant_name>
```

Always run clean-slate: mutmut 3.8 maps tests to mutants by mangled
function name and does not retest cached survivors, so an incremental
run after a rename or a test change reports stale verdicts.

The gate (`tests/check_mutants.py`) fails when any mutant is neither
killed nor named in "Surviving mutants" below. A new survivor is killed
by a test or argued into an equivalence class, in the same change. This
ledger is rewritten from a clean-slate run whenever the mutated modules
change.

## Result

| run | total | killed | survived | timeouts |
|---|---|---|---|---|
| 2026-10-14, simplified keyspace (`rows/` + `meta/fence` only) and one-predicate planner, clean-slate | 247 | 229 | 18 | 0 |
| 2026-10-14, review round (`meta/table_uuid`, `_decode_group_at` off position 0), clean-slate | 247 | 230 | 17 | 0 |

`planner.py` has no survivors. The first run of this tree left 24; six
were killed by `test_codec_refusals_name_exactly_what_failed` (exact
refusal messages for a truncated u16 length, a value running past the
end, an empty buffer, an all-`0xFF` prefix, an out-of-range offset).

## Surviving mutants — all equivalent (17)

Arguments are interpreter semantics or a guard upstream of the mutated
line (CPython ≥ 3.12), not "the tests happen to pass".

### E1 — dropped or falsy explicit defaults of `to_bytes`/`from_bytes` (9)

Since Python 3.11 `byteorder` defaults to `"big"` and `signed` to
`False`; `signed=None` is falsy, so it behaves as `False`.

- `x__encode_i64_ordered__mutmut_3` — `signed=False` → `signed=None`
- `x__encode_i64_ordered__mutmut_5` — `"big"` dropped
- `x__encode_i64_ordered__mutmut_6` — `signed=False` dropped
- `x__decode_i64_ordered__mutmut_4` — `signed=False` → `signed=None`
- `x__decode_i64_ordered__mutmut_6` — `"big"` dropped
- `x__decode_i64_ordered__mutmut_7` — `signed=False` dropped
- `x_encode_group__mutmut_27` — `"big"` dropped (u16 length)
- `x__decode_group_at__mutmut_33` — `"big"` dropped (u16 length)
- `x_encode_fence_probe_value__mutmut_11` — `"big"` dropped

### E2 — codec and byte-literal spellings (3)

- `x_encode_group__mutmut_17` — `encode("utf-8")` → `encode("UTF-8")`: codec names are case-insensitive
- `x__decode_group_at__mutmut_49` — `decode("utf-8")` → `decode("UTF-8")`: same
- `x_prefix_successor__mutmut_5` — `b"\xff"` → `b"\xFF"`: the same byte

### E3 — retired

x__decode_group_at__mutmut_8 (`pos += 1` → `pos = 1`) is killed by
`test_decode_group_at_reads_from_its_offset`, which decodes at `pos > 0`.

### E4 — `zip(strict=…)` behind an explicit length check (3)

`group_label` raises on `len(fields) != len(group)` before the loop, so
the zip's strictness can never fire.

- `x_group_label__mutmut_6` — `strict=True` → `strict=None`
- `x_group_label__mutmut_9` — `strict=True` dropped
- `x_group_label__mutmut_10` — `strict=True` → `strict=False`

### E5 — `X` added to `quote`'s `safe` set (2)

ASCII letters are always unescaped by `urllib.parse.quote`, so adding
`X` to `safe` changes nothing.

- `x_group_label__mutmut_16` — `safe="._-"` → `safe="XX._-XX"`
- `x_group_label__mutmut_25` — `safe=''` → `safe='XXXX'`

## Why the equivalents are not suppressed via mutmut config

mutmut 3.8 has no per-line suppression that survives refactors; listing
them here, by name, keeps the gate exact and makes every new survivor
visible.
