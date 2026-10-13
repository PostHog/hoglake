# Mutation testing the pure core — survivor ledger

Scope and config: `[tool.mutmut]` in `pyproject.toml` (mutmut 3.8.0, pinned).
Mutated: `millrace/keyspace.py`, `millrace/planner.py`. Driver:
`tests/test_keyspace.py` + `tests/test_planner.py` (`-x -q`;
`test_planner_replay.py` is excluded — it pins distributions, not
predicates). Run from this directory:

```
just millrace mutants   # clean-slate run + the survivors gate (below)
```

or by hand:

```
rm -rf mutants/          # see the incremental gotcha below
uv run mutmut run        # full run (~4 min on a laptop)
uv run mutmut results    # list survivors
uv run mutmut show <mutant_name>
uv run mutmut run <mutant_name>...   # force-retest specific mutants
```

mutmut 3.8 does NOT retest cached survivors on a plain `run`, even when
the tests changed — pass the survivor names explicitly (or delete
`mutants/`) after strengthening tests. A second, sharper incremental
gotcha (found the hard way on the three-lane planner rename): the stats
map TESTS TO MUTANTS by mangled FUNCTION name, so a function rename
makes the cache silently stale — same-named rewritten tests never map
to the new function's mutants, and an incremental `run` reports old
verdicts against new code. The fix is always the same: **delete
`mutants/` and run clean-slate after any rename or move in the mutated
modules.**

CI runs exactly that: the `millrace` job of
`.github/workflows/ci-python.yml` does `rm -rf mutants && uv run mutmut
run && uv run python tests/check_mutants.py` on every millrace change.
The gate (`tests/check_mutants.py`) fails when any mutant is neither
killed nor a ledgered survivor — a genuinely new survivor must be
killed by a test or argued into the equivalence classes below, in the
same change. A full run is minutes at this scope (two pure modules, a
`-x -q` driver of two fast test files), so CI needs no incremental
mode; if the scope ever grows past the job's time budget, the choice
and its staleness rules belong here. The ledger below is rewritten to
match the clean-slate run EXACTLY whenever the mutated modules change;
the survivor sections are the gate's reference set.

## Result

| | total | killed | survived | timeouts |
|---|---|---|---|---|
| first full run | 928 | 813 | 115 | 0 |
| after this triage | 928 | 823 | 105 | 0 |
| Phase 3 (poison codec added) | 1166 | 1046 | 120 | 0 |
| 2026-10-09 reconciliation, clean-slate | 1074 | 964 | 110 | 0 |
| 2026-10-10 B1 reconciliation (v2 key derivation, `prepared/` v3), clean-slate | 1215 | 1086 | 129 | 0 |
| 2026-10-10 poison envelope v2 (`quarantined_at_us`), clean-slate | 1261 | 1126 | 135 | 0 |
| 2026-10-12 partition-tuple keyspace, `(group, offset)` rows, `offsets/` removed, fence probe — clean-slate, first run | 835 | 745 | 90 | 0 |
| 2026-10-12, same code, after the kill tests below — clean-slate | 835 | 763 | 72 | 0 |
| 2026-10-12, content digest in the key (`millrace:v3`), `prepared/` v5 (`row_count`) — clean-slate | 874 | 797 | 77 | 0 |
| 2026-10-13, chunked digest updates, `prepared/` v6 (persisted `digest`) — clean-slate | 891 | 813 | 78 | 0 |
| **2026-10-13, `prepared/` `scanned_at` (the settle's `first_staged_ts` lift) — clean-slate** | **936** | **856** | **80** | **0** |

The last row is the current ledger: `rm -rf mutants && uv run mutmut
run` on this tree, `tests/check_mutants.py` exit 0. It adds
`content_digest`, the digest argument of `idempotency_key` and the v5
envelope's `row_count`; their non-equivalent mutants are killed by
`test_content_digest_golden_vectors`,
`test_content_digest_framing_is_injective`,
`test_content_digest_matches_its_documented_framing`,
`test_idempotency_key_refuses_a_malformed_digest`, the v5 golden
vector and `test_prepared_persisted_at_round_trips_across_the_sign_bit`
(which pins `persisted_at` and `row_count` against each other). Its 5
new survivors are default-argument (E1, E2) and message-label (M)
mutants of those lines; `decode_prepared_value`'s mutants renumbered
with the v5 tail, so its rows carry new indices. The 2026-10-13 row
(the current ledger) adds the buffered 1 MiB `update` chunks of
`content_digest` and the v6 envelope's persisted `digest`: the first
run left 79 survivors; `test_content_digest_is_unchanged_across_its_1_mib_update_chunks`
killed the one non-equivalent (`h.update(None)` on the chunk branch,
reachable only past 1 MiB of framed rows), leaving the chunk-boundary
`>=`/`>` flip as class E9. The next row adds the envelope's optional
`scanned_at` (tag + i64): its first run left 81 survivors; the one
non-equivalent was `decode_prepared_value`'s `partition` decoded
`signed=True`, alive only because the round-trip's hypothesis stream
stopped drawing a partition ≥ 2³¹ once the test drew one more field —
`test_prepared_partition_decodes_unsigned_at_the_sign_bit` now pins
2³¹−1, 2³¹ and 2³²−1 deterministically. The rest are E1/E2 renumberings
of the decode tail and the `scanned_at` label (M). Every row above it
ran against earlier keyspace code (team-id keys, the `poison/` and
`offsets/` codecs); their survivor tables are gone with that code (git
history keeps them), and the earlier phase narratives that cited tests
of that code were removed with it.

`planner.py`: 0 survivors. All 80 survivors are in `keyspace.py`.

## Killed by tests added at the 2026-10-12 reconciliation (18)

The first clean-slate run on this tree left 90 survivors; 18 changed an
observable result and are killed by tests in `tests/test_keyspace.py`
(no source change):

- `test_decode_group_is_total_and_refuses_corruption` now pins the
  trailing-byte COUNT (`: 2 trailing byte(s)` for `b"\x00\x00\x00"`):
  `x_decode_group__mutmut_10` (`len(data) - pos` → `+`).
- `test_encode_group_validation` pins the refusal texts that name the
  failure: `x_encode_group__mutmut_9`, `_10` (TypeError text),
  `_12` (`type(component)` → `type(None)`: "got int" vs "got NoneType"),
  `_16`, `_20`, `_21` (the `"partition value"` label of the u16 refusal).
- `test_prepared_value_encode_names_an_oversized_partition_field` (new):
  a field name or value of 2¹⁶ bytes reaches the encoder (the dataclass
  checks arity and non-emptiness only) and the refusal names which:
  `x_encode_prepared_value__mutmut_38`, `_42`, `_43`
  (`"partition field name"`), `_49`, `_53`, `_54` (`"partition value"`).
- `test_key_decodes_name_their_minimum_length` (new): a key one byte
  short of its fixed fields plus the arity byte is refused by LENGTH,
  before any group parse: `x_decode_stats_key__mutmut_8`,
  `x_decode_sched_age_key__mutmut_8`, `_9`,
  `x_decode_prepared_key__mutmut_8`, `_9` (`min_length` arithmetic).

Three boundary tests the review asked for were in place before the
first run, so the classes they target did not survive it:
`test_sched_age_range_through_boundary` (cutoff, cutoff+1 and the end
key at `INT64_MIN`, -1, 0, 41, `INT64_MAX - 1`),
`test_prepared_persisted_at_round_trips_across_the_sign_bit` (`0`, `1`,
`2**63 - 1`, `2**63`, `UINT64_MAX`; kills a `signed=True` decode), and
the field-name cases of `test_group_label_is_the_hoglake_human_form`
(`/`, `=`, `:` and non-ASCII in a FIELD NAME percent-encode).

## Surviving mutants — all equivalent (80)

Each entry: the mutant, its one-line diff, and the argument. The
arguments are interpreter semantics or an explicit guard upstream of the
mutated line, verified on the pinned runtime (CPython 3.13 venv;
`requires-python >= 3.12`), not "tests happen to stay green". The tables
are regenerated from a clean-slate run's actual output whenever the
mutated modules change (see the header); `tests/check_mutants.py` is the
mechanical cross-check.

### Class E1 — dropped redundant explicit defaults, `to_bytes`/`from_bytes` (24)

Since Python 3.11 the `byteorder` parameter of `int.to_bytes` /
`int.from_bytes` defaults to `"big"`, and `signed` defaults to `False`;
dropping either argument cannot change any result on any input.
(Verified: `(255).to_bytes(8, "big", signed=False) == (255).to_bytes(8, signed=False) == (255).to_bytes(8, "big")`.)
Field widths, slice bounds and endianness on these lines are pinned
independently by the golden vectors and the boundary round-trips.

| Mutant | Diff | Why equivalent |
|---|---|---|
| `x__decode_i64_ordered__mutmut_6` | `return int.from_bytes(data, "big", signed=False) - _SIGN_OFFSET` → `return int.from_bytes(data, signed=False) - _SIGN_OFFSET` | redundant explicit default (`"big"` / `signed=False`) |
| `x__decode_i64_ordered__mutmut_7` | `return int.from_bytes(data, "big", signed=False) - _SIGN_OFFSET` → `return int.from_bytes(data, "big", ) - _SIGN_OFFSET` | redundant explicit default (`"big"` / `signed=False`) |
| `x__decode_utf8_u16__mutmut_9` | `length = int.from_bytes(data[pos : pos + 2], "big")` → `length = int.from_bytes(data[pos : pos + 2], )` | redundant explicit default (`"big"` / `signed=False`) |
| `x__encode_i64__mutmut_5` | `return value.to_bytes(8, "big", signed=True)` → `return value.to_bytes(8, signed=True)` | redundant explicit default (`"big"` / `signed=False`) |
| `x__encode_u64__mutmut_5` | `return value.to_bytes(8, "big", signed=False)` → `return value.to_bytes(8, signed=False)` | redundant explicit default (`"big"` / `signed=False`) |
| `x__encode_u64__mutmut_6` | `return value.to_bytes(8, "big", signed=False)` → `return value.to_bytes(8, "big", )` | redundant explicit default (`"big"` / `signed=False`) |
| `x__encode_utf8_u16__mutmut_11` | `return len(raw).to_bytes(2, "big") + raw` → `return len(raw).to_bytes(2, ) + raw` | redundant explicit default (`"big"` / `signed=False`) |
| `x_decode_fence_probe_value__mutmut_11` | `return int.from_bytes(data[1:], "big", signed=True)` → `return int.from_bytes(data[1:], signed=True)` | redundant explicit default (`"big"` / `signed=False`) |
| `x_decode_prepared_value__mutmut_126` | `scanned_at = int.from_bytes(scanned_raw, "big", signed=True)` → `scanned_at = int.from_bytes(scanned_raw, signed=True)` | redundant explicit default (`"big"` / `signed=False`) |
| `x_decode_prepared_value__mutmut_162` | `persisted_at=int.from_bytes(data[tail : tail + 8], "big", signed=False),` → `persisted_at=int.from_bytes(data[tail : tail + 8], signed=False),` | redundant explicit default (`"big"` / `signed=False`) |
| `x_decode_prepared_value__mutmut_163` | `persisted_at=int.from_bytes(data[tail : tail + 8], "big", signed=False),` → `persisted_at=int.from_bytes(data[tail : tail + 8], "big", ),` | redundant explicit default (`"big"` / `signed=False`) |
| `x_decode_prepared_value__mutmut_173` | `row_count=int.from_bytes(data[tail + 8 : tail + 16], "big", signed=False),` → `row_count=int.from_bytes(data[tail + 8 : tail + 16], signed=False),` | redundant explicit default (`"big"` / `signed=False`) |
| `x_decode_prepared_value__mutmut_174` | `row_count=int.from_bytes(data[tail + 8 : tail + 16], "big", signed=False),` → `row_count=int.from_bytes(data[tail + 8 : tail + 16], "big", ),` | redundant explicit default (`"big"` / `signed=False`) |
| `x_decode_prepared_value__mutmut_44` | `partition = int.from_bytes(data[pos : pos + 4], "big", signed=False)` → `partition = int.from_bytes(data[pos : pos + 4], signed=False)` | redundant explicit default (`"big"` / `signed=False`) |
| `x_decode_prepared_value__mutmut_45` | `partition = int.from_bytes(data[pos : pos + 4], "big", signed=False)` → `partition = int.from_bytes(data[pos : pos + 4], "big", )` | redundant explicit default (`"big"` / `signed=False`) |
| `x_decode_prepared_value__mutmut_98` | `body_len = int.from_bytes(data[pos : pos + 4], "big")` → `body_len = int.from_bytes(data[pos : pos + 4], )` | redundant explicit default (`"big"` / `signed=False`) |
| `x_decode_stats_value__mutmut_19` | `staged_bytes=int.from_bytes(data[1:9], "big", signed=False),` → `staged_bytes=int.from_bytes(data[1:9], signed=False),` | redundant explicit default (`"big"` / `signed=False`) |
| `x_decode_stats_value__mutmut_20` | `staged_bytes=int.from_bytes(data[1:9], "big", signed=False),` → `staged_bytes=int.from_bytes(data[1:9], "big", ),` | redundant explicit default (`"big"` / `signed=False`) |
| `x_decode_stats_value__mutmut_30` | `first_staged_ts=int.from_bytes(data[9:17], "big", signed=True),` → `first_staged_ts=int.from_bytes(data[9:17], signed=True),` | redundant explicit default (`"big"` / `signed=False`) |
| `x_decode_stats_value__mutmut_41` | `last_staged_ts=int.from_bytes(data[17:25], "big", signed=True),` → `last_staged_ts=int.from_bytes(data[17:25], signed=True),` | redundant explicit default (`"big"` / `signed=False`) |
| `x_decode_stats_value__mutmut_52` | `row_count=int.from_bytes(data[25:33], "big", signed=False),` → `row_count=int.from_bytes(data[25:33], signed=False),` | redundant explicit default (`"big"` / `signed=False`) |
| `x_decode_stats_value__mutmut_53` | `row_count=int.from_bytes(data[25:33], "big", signed=False),` → `row_count=int.from_bytes(data[25:33], "big", ),` | redundant explicit default (`"big"` / `signed=False`) |
| `x_encode_prepared_value__mutmut_24` | `out += request.partition.to_bytes(4, "big")` → `out += request.partition.to_bytes(4, )` | redundant explicit default (`"big"` / `signed=False`) |
| `x_encode_prepared_value__mutmut_60` | `out += len(request.body).to_bytes(4, "big")` → `out += len(request.body).to_bytes(4, )` | redundant explicit default (`"big"` / `signed=False`) |

### Class E2 — `signed=None` ≡ `signed=False` (7)

CPython parses the keyword-only `signed` of `int.to_bytes` /
`int.from_bytes` as a truthiness predicate (format `p`), so `None` reads
as `False` for every input.
(Verified: `int.from_bytes(b"\x00\xff", "big", signed=None) == 255`.)
The `signed=True` siblings are NOT equivalent and are killed by the
sign-bit boundary round-trips.

| Mutant | Diff | Why equivalent |
|---|---|---|
| `x__decode_i64_ordered__mutmut_4` | `return int.from_bytes(data, "big", signed=False) - _SIGN_OFFSET` → `return int.from_bytes(data, "big", signed=None) - _SIGN_OFFSET` | `signed` parsed by truthiness: `None` ≡ `False` |
| `x__encode_u64__mutmut_3` | `return value.to_bytes(8, "big", signed=False)` → `return value.to_bytes(8, "big", signed=None)` | `signed` parsed by truthiness: `None` ≡ `False` |
| `x_decode_prepared_value__mutmut_160` | `persisted_at=int.from_bytes(data[tail : tail + 8], "big", signed=False),` → `persisted_at=int.from_bytes(data[tail : tail + 8], "big", signed=None),` | `signed` parsed by truthiness: `None` ≡ `False` |
| `x_decode_prepared_value__mutmut_171` | `row_count=int.from_bytes(data[tail + 8 : tail + 16], "big", signed=False),` → `row_count=int.from_bytes(data[tail + 8 : tail + 16], "big", signed=None),` | `signed` parsed by truthiness: `None` ≡ `False` |
| `x_decode_prepared_value__mutmut_42` | `partition = int.from_bytes(data[pos : pos + 4], "big", signed=False)` → `partition = int.from_bytes(data[pos : pos + 4], "big", signed=None)` | `signed` parsed by truthiness: `None` ≡ `False` |
| `x_decode_stats_value__mutmut_17` | `staged_bytes=int.from_bytes(data[1:9], "big", signed=False),` → `staged_bytes=int.from_bytes(data[1:9], "big", signed=None),` | `signed` parsed by truthiness: `None` ≡ `False` |
| `x_decode_stats_value__mutmut_50` | `row_count=int.from_bytes(data[25:33], "big", signed=False),` → `row_count=int.from_bytes(data[25:33], "big", signed=None),` | `signed` parsed by truthiness: `None` ≡ `False` |

### Class E3 — `"utf-8"` vs `"UTF-8"` (4)

Codec lookup normalizes case: `codecs.lookup("UTF-8") is
codecs.lookup("utf-8")`.

| Mutant | Diff | Why equivalent |
|---|---|---|
| `x__decode_utf8_u16__mutmut_25` | `text = bytes(data[pos : pos + length]).decode("utf-8")` → `text = bytes(data[pos : pos + length]).decode("UTF-8")` | codec lookup is case-insensitive |
| `x__encode_utf8_u16__mutmut_4` | `raw = text.encode("utf-8")` → `raw = text.encode("UTF-8")` | codec lookup is case-insensitive |
| `x_idempotency_key__mutmut_7` | `encoded_topic = topic.encode("utf-8")` → `encoded_topic = topic.encode("UTF-8")` | codec lookup is case-insensitive |
| `xǁPreparedRequestǁ__post_init____mutmut_19` | `encoded_topic = self.topic.encode("utf-8")` → `encoded_topic = self.topic.encode("UTF-8")` | codec lookup is case-insensitive |

### Class E4 — `b"\xff"` vs `b"\xFF"` (1)

Hex escapes are case-insensitive; the two literals are the same bytes.

| Mutant | Diff | Why equivalent |
|---|---|---|
| `x__prefix_successor__mutmut_5` | `stripped = prefix.rstrip(b"\xff")` → `stripped = prefix.rstrip(b"\xFF")` | same bytes literal |

### Class E5 — slice bound past an exactly-length-checked buffer (1)

| Mutant | Diff | Why equivalent |
|---|---|---|
| `x_decode_stats_value__mutmut_55` | `row_count=int.from_bytes(data[25:33], "big", signed=False),` → `row_count=int.from_bytes(data[25:34], "big", signed=False),` | `len(data) == 33` enforced first; the slice clamps to `data[25:33]` |

### Class E7 — `X` added to `quote`'s `safe` set (2)

`urllib.parse.quote` never escapes ASCII letters, digits or `_.-~`, so
`safe="XX._-XX"` is the set `safe="._-"` and `safe="XXXX"` is `safe=""`
for every input. (Verified over names and values holding `X`, `/`, `=`,
`:`, `%` and non-ASCII.)

| Mutant | Diff | Why equivalent |
|---|---|---|
| `x_group_label__mutmut_16` | `encoded = quote(name, safe="._-")` → `encoded = quote(name, safe="XX._-XX")` | `X` is unreserved: `quote` never escapes it, so adding it to `safe` changes nothing |
| `x_group_label__mutmut_25` | `parts.append(encoded if value is None else f"{encoded}={quote(value, safe='')}")` → `parts.append(encoded if value is None else f"{encoded}={quote(value, safe='XXXX')}")` | `X` is unreserved: `quote` never escapes it, so adding it to `safe` changes nothing |

### Class E8 — `pos += 1` vs `pos = 1` at the only caller's `pos = 0` (1)

`_decode_group_at` has one call site, `decode_group`, which passes
`pos=0`; at that value the two statements assign the same `1`.

| Mutant | Diff | Why equivalent |
|---|---|---|
| `x__decode_group_at__mutmut_8` | `pos += 1` → `pos = 1` | the only call site (`decode_group`) passes `pos=0`, so `pos += 1` and `pos = 1` agree |

### Class E9 — the 1 MiB update boundary of a streaming hash (1)

`content_digest` buffers framed rows and calls `update` once the buffer
reaches `_DIGEST_CHUNK`. SHA-256 is a streaming hash: how the input is
split across `update` calls never changes the digest, so `>=` vs `>`
moves only the flush point of a buffer of exactly 1 MiB. Pinned from
the other side by `test_content_digest_is_unchanged_across_its_1_mib_update_chunks`
(an exactly-1-MiB buffer among its cases), which kills the
`h.update(None)` mutant of the same branch.

| Mutant | Diff | Why equivalent |
|---|---|---|
| `x_content_digest__mutmut_17` | `if len(buf) >= _DIGEST_CHUNK:` → `if len(buf) > _DIGEST_CHUNK:` | SHA-256 is a streaming hash: where the buffered input is split between `update` calls never changes the digest; only the flush point at exactly 1 MiB moves |

### Class U — u16 label text behind the dataclass's own refusal (6)

`encode_prepared_value` passes a label to `_encode_utf8_u16` that only
appears in its over-length refusal. For the idempotency key and the
topic that refusal is unreachable: `PreparedRequest.__post_init__`
refuses either one past 65 535 UTF-8 bytes before any encode
(`test_varlen_fields_accept_exactly_the_u16_maximum`,
`test_prepared_value_varlen_fields_accept_exactly_the_u16_maximum`).
(The partition field name and value labels ARE reachable and are
killed, above.)

| Mutant | Diff | Why equivalent |
|---|---|---|
| `x_encode_prepared_value__mutmut_10` | `out += _encode_utf8_u16("idempotency key", request.idempotency_key)` → `out += _encode_utf8_u16("IDEMPOTENCY KEY", request.idempotency_key)` | label only reaches the u16 refusal, which `PreparedRequest.__post_init__` raises first for both fields |
| `x_encode_prepared_value__mutmut_13` | `out += _encode_utf8_u16("topic", request.topic)` → `out += _encode_utf8_u16(None, request.topic)` | label only reaches the u16 refusal, which `PreparedRequest.__post_init__` raises first for both fields |
| `x_encode_prepared_value__mutmut_17` | `out += _encode_utf8_u16("topic", request.topic)` → `out += _encode_utf8_u16("XXtopicXX", request.topic)` | label only reaches the u16 refusal, which `PreparedRequest.__post_init__` raises first for both fields |
| `x_encode_prepared_value__mutmut_18` | `out += _encode_utf8_u16("topic", request.topic)` → `out += _encode_utf8_u16("TOPIC", request.topic)` | label only reaches the u16 refusal, which `PreparedRequest.__post_init__` raises first for both fields |
| `x_encode_prepared_value__mutmut_5` | `out += _encode_utf8_u16("idempotency key", request.idempotency_key)` → `out += _encode_utf8_u16(None, request.idempotency_key)` | label only reaches the u16 refusal, which `PreparedRequest.__post_init__` raises first for both fields |
| `x_encode_prepared_value__mutmut_9` | `out += _encode_utf8_u16("idempotency key", request.idempotency_key)` → `out += _encode_utf8_u16("XXidempotency keyXX", request.idempotency_key)` | label only reaches the u16 refusal, which `PreparedRequest.__post_init__` raises first for both fields |

### Class Z — `zip(strict=…)` behind an explicit length check (6)

`group_label` raises on `len(fields) != len(group)` immediately above
its `zip`, and `encode_prepared_value` zips a `PreparedRequest` whose
`__post_init__` (`_check_group_fields`) refuses mismatched lengths; on
every input that reaches the `zip`, `strict=True`, `False`, `None` or
absent iterate identically.

| Mutant | Diff | Why equivalent |
|---|---|---|
| `x_encode_prepared_value__mutmut_31` | `for name, value in zip(request.group_fields, request.group, strict=True):` → `for name, value in zip(request.group_fields, request.group, strict=None):` | explicit length check above raises before the `zip` on any mismatch |
| `x_encode_prepared_value__mutmut_34` | `for name, value in zip(request.group_fields, request.group, strict=True):` → `for name, value in zip(request.group_fields, request.group, ):` | explicit length check above raises before the `zip` on any mismatch |
| `x_encode_prepared_value__mutmut_35` | `for name, value in zip(request.group_fields, request.group, strict=True):` → `for name, value in zip(request.group_fields, request.group, strict=False):` | explicit length check above raises before the `zip` on any mismatch |
| `x_group_label__mutmut_10` | `for name, value in zip(fields, group, strict=True):` → `for name, value in zip(fields, group, strict=False):` | explicit length check above raises before the `zip` on any mismatch |
| `x_group_label__mutmut_6` | `for name, value in zip(fields, group, strict=True):` → `for name, value in zip(fields, group, strict=None):` | explicit length check above raises before the `zip` on any mismatch |
| `x_group_label__mutmut_9` | `for name, value in zip(fields, group, strict=True):` → `for name, value in zip(fields, group, ):` | explicit length check above raises before the `zip` on any mismatch |

### Class M — error-message text the suite deliberately does not pin (27)

The suite pins exception TYPE plus a stable identifying substring (the
field name, or the failed parse stage) and leaves the rest of the
wording free; each mutant below decorates a message (`XX…XX`, a case
flip, a field-name label) while the pinned substring still matches. The
raise CONDITION is pinned by the test named per row.

| Mutant | Diff | Why equivalent |
|---|---|---|
| `x__check_group_fields__mutmut_3` | `"partition tuple"` → `"XXpartition tupleXX"` | test_prepared_request_validation (match='1 field name') |
| `x__check_group_fields__mutmut_4` | `"partition tuple"` → `"PARTITION TUPLE"` | test_prepared_request_validation (match='1 field name') |
| `x__check_group_fields__mutmut_8` | `raise ValueError("partition field names must be non-empty")` → `raise ValueError("XXpartition field names must be non-emptyXX")` | test_prepared_request_validation (match='non-empty') |
| `x__decode_group_at__mutmut_5` | `raise ValueError("corrupt partition tuple: no arity byte")` → `raise ValueError("XXcorrupt partition tuple: no arity byteXX")` | test_decode_group_is_total_and_refuses_corruption (match='no arity byte') |
| `x__prefix_successor__mutmut_8` | `raise ValueError("a prefix of only 0xFF bytes has no successor")` → `raise ValueError("XXa prefix of only 0xFF bytes has no successorXX")` | test_prefix_successor_strips_only_trailing_ff (match='no successor') |
| `x__prefix_successor__mutmut_9` | `raise ValueError("a prefix of only 0xFF bytes has no successor")` → `raise ValueError("a prefix of only 0xff bytes has no successor")` | test_prefix_successor_strips_only_trailing_ff (match='no successor') |
| `x_content_digest__mutmut_7` | `_require_int64("offset", offset)` → `_require_int64("XXoffsetXX", offset)` | test_content_digest_framing_is_injective (match='offset') |
| `x_decode_prepared_value__mutmut_37` | `raise ValueError("truncated prepared value: partition / arity")` → `raise ValueError("XXtruncated prepared value: partition / arityXX")` | test_prepared_value_truncation_errors_name_the_failed_parse_stage (match='partition / arity') |
| `x_decode_prepared_value__mutmut_6` | `raise ValueError("prepared value is empty")` → `raise ValueError("XXprepared value is emptyXX")` | test_prepared_value_truncation_errors_name_the_failed_parse_stage (match='empty') |
| `x_encode_fence_probe_value__mutmut_5` | `_require_int64("offset", offset)` → `_require_int64("XXoffsetXX", offset)` | test_fence_probe_value_validation (match='offset') |
| `x_idempotency_key__mutmut_10` | `raise ValueError("topic must be non-empty")` → `raise ValueError("XXtopic must be non-emptyXX")` | test_idempotency_key_validation (match='topic') |
| `x_idempotency_key__mutmut_23` | `_require_int64("first_offset", first_offset)` → `_require_int64("XXfirst_offsetXX", first_offset)` | test_key_functions_validate_offsets (match='first_offset') |
| `x_idempotency_key__mutmut_29` | `_require_int64("last_offset", last_offset)` → `_require_int64("XXlast_offsetXX", last_offset)` | test_key_functions_validate_offsets (match='last_offset') |
| `x_prepared_key__mutmut_5` | `_require_int64("first_offset", first_offset)` → `_require_int64("XXfirst_offsetXX", first_offset)` | test_key_functions_validate_offsets (match='first_offset') |
| `x_sched_age_range_through__mutmut_5` | `_require_int64("cutoff_ts", cutoff_ts)` → `_require_int64("XXcutoff_tsXX", cutoff_ts)` | test_sched_age_range_through_validates_cutoff (match='cutoff_ts') |
| `xǁFlushedKeyǁ__post_init____mutmut_15` | `_require_int64("last_offset", self.last_offset)` → `_require_int64("XXlast_offsetXX", self.last_offset)` | test_flushed_key_validation (match='last_offset') |
| `xǁFlushedKeyǁ__post_init____mutmut_9` | `_require_int64("first_offset", self.first_offset)` → `_require_int64("XXfirst_offsetXX", self.first_offset)` | test_flushed_key_validation (match='first_offset') |
| `xǁPreparedRequestǁ__post_init____mutmut_10` | `_require_uint64("persisted_at", self.persisted_at)` → `_require_uint64("XXpersisted_atXX", self.persisted_at)` | test_prepared_request_validation (match='persisted_at') |
| `xǁPreparedRequestǁ__post_init____mutmut_14` | `raise ValueError("topic must be non-empty")` → `raise ValueError("XXtopic must be non-emptyXX")` | test_prepared_request_validation (match='topic') |
| `xǁPreparedRequestǁ__post_init____mutmut_35` | `_require_uint64("row_count", self.row_count)` → `_require_uint64("XXrow_countXX", self.row_count)` | test_prepared_request_validation (match='row_count') |
| `xǁPreparedRequestǁ__post_init____mutmut_45` | `_require_int64("scanned_at", self.scanned_at)` → `_require_int64("XXscanned_atXX", self.scanned_at)` | test_prepared_scanned_at_tag_is_strict (match='scanned_at') |
| `xǁRowKeyǁ__post_init____mutmut_5` | `_require_int64("offset", self.offset)` → `_require_int64("XXoffsetXX", self.offset)` | test_row_key_field_validation (match='offset') |
| `xǁSchedAgeKeyǁ__post_init____mutmut_5` | `_require_int64("first_staged_ts", self.first_staged_ts)` → `_require_int64("XXfirst_staged_tsXX", self.first_staged_ts)` | test_sched_age_key_validation (match='first_staged_ts') |
| `xǁStatsValueǁ__post_init____mutmut_11` | `_require_int64("first_staged_ts", self.first_staged_ts)` → `_require_int64("XXfirst_staged_tsXX", self.first_staged_ts)` | test_stats_value_timestamp_field_bounds (match='first_staged_ts') |
| `xǁStatsValueǁ__post_init____mutmut_17` | `_require_int64("last_staged_ts", self.last_staged_ts)` → `_require_int64("XXlast_staged_tsXX", self.last_staged_ts)` | test_stats_value_timestamp_field_bounds (match='last_staged_ts') |
| `xǁStatsValueǁ__post_init____mutmut_23` | `_require_uint64("row_count", self.row_count)` → `_require_uint64("XXrow_countXX", self.row_count)` | test_stats_value_validation (match='row_count') |
| `xǁStatsValueǁ__post_init____mutmut_5` | `_require_uint64("staged_bytes", self.staged_bytes)` → `_require_uint64("XXstaged_bytesXX", self.staged_bytes)` | test_stats_value_validation (match='staged_bytes') |

## Why the equivalents are not suppressed via mutmut config

mutmut 3.8 has no per-mutant ignore. Its mechanisms are line-grained:
trailing `# pragma: no mutate`, `# pragma: no mutate start/end`
selections, and `do_not_mutate_patterns` regexes in `[tool.mutmut]`.
Every line above that hosts an equivalent mutant also hosts KILLED
mutants guarding real behavior — slice bounds (`data[1:9]` →
`data[1:8]`), comparison operators, argument presence in the same call —
so a line-level pragma or pattern would also stop generating those,
hiding future real regressions from the mutant generator. Recording the
equivalents here, with the semantic argument per class, keeps the
generation surface intact; the ledger above is the review artifact.
