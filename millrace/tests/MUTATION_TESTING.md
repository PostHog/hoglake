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

## Phase 6 update — the review fixes (prepared v2, flushed/ retired)

Two structural changes to `keyspace.py` shift the mutant surface, and the
load-bearing predicates they and their `stage.py`/`flush.py` companions
added were verified by HAND-FLIP (flip the predicate, watch the named
test red, revert) — the mechanism AGENT.md's pre-PR rule prescribes, and
the only one that applies to `stage.py`/`flush.py`, which mutmut does
not mutate:

- **`prepared/` envelope v2** (`persisted_at` u64 tail; v1 decodes as
  "unknown age", encode refuses it). New golden vectors (v2 written out
  byte-by-byte; the v1 golden is decode-only) plus boundary tests. The
  flushed KEY golden vector is gone with the codec (see below). Hand-flips,
  all RED: encode accepting `persisted_at=None`
  (`test_prepared_value_encode_requires_persisted_at`); v1 decoding to a
  fabricated age instead of `None`
  (`test_prepared_value_v1_decodes_with_unknown_age`).
- **`flushed/` codec retired.** `FlushedValue` and the flushed key/value
  codecs are deleted: settlement has always been one atomic transaction
  with the commit receipt in hand, so a readable marker was always
  settled, receipt-confirmed state — the marker carried no information
  and grew unbounded. `recover()` now collects leftover markers in
  bounded pages (one delete batch per `_RECOVERY_PAGE` keys, the write
  lock per page, no per-marker transaction). Hand-flips, all RED:
  the collector not deleting
  (`test_recover_collects_legacy_markers_without_touching_rows` and
  `..._in_bounded_pages`); one serializable transaction per page (the
  pre-fix tax; `..._in_bounded_pages` pins `begins == 0` and exactly 3
  write batches for 2500 markers); `commit_flushed` writing a marker
  again (`test_commit_flushed_writes_no_marker`). Mutants the deleted
  codec hosted vanished with it; the ledger rows that named
  `encode_flushed_value`, `decode_flushed_key`, `decode_flushed_value`,
  `FlushedValue.__post_init__`, `team_flushed_prefix` and `flushed_key`
  were dropped at the 2026-10-09 reconciliation (see the Result
  section). `FlushedKey` itself LIVES ON as the settlement-window type,
  so its `__post_init__` rows remain.
- **Decoder magnitude guards** (flush.py, hand-flip only): the per-unit
  int64 bound on `_coerce_timestamp` removed RED
  (`test_timestamp_int_passthrough_is_bounded_per_unit`), `timestamp_s`
  widened to full int64 RED (same test — it is stored as ms), the
  `math.isfinite` refusal removed RED and the `OverflowError` catch
  removed RED (`test_float_refuses_non_finite_and_unconvertible`), the
  `RecursionError` arm removed RED
  (`test_deeply_nested_payload_is_a_per_record_failure`), the decimal
  startup validation removed RED
  (`test_decimal_column_shape_is_a_startup_refusal`), the batch-build
  isolation arm removed RED
  (`TestBatchBuildIsolation::test_a_record_that_defeats_the_arrow_build_is_isolated`).
- **Receipt horizon** (flush.py `reconcile_prepared`, hand-flip only):
  `>` flipped to `>=` RED
  (`test_entry_at_exactly_the_horizon_still_replays`), the unknown-age
  arm inverted RED (`test_unknown_age_legacy_entry_halts_on_a_404`), the
  guard moved ahead of the receipt-200 settle RED
  (`test_receipt_200_settles_regardless_of_age`), the server accessor
  never consulted RED
  (`test_the_horizon_comes_from_the_server_when_exposed`), a raising
  accessor propagating RED (`test_server_horizon_inf_and_knob_fallback`).

## Result

| | total | killed | survived | timeouts |
|---|---|---|---|---|
| first full run | 928 | 813 | 115 | 0 |
| after this triage | 928 | **823** | **105** | 0 |
| Phase 3 (poison codec added; see below) | 1166 | **1046** | **120** | 0 |
| 2026-10-09 reconciliation, clean-slate | 1074 | **964** | **110** | **0** |
| 2026-10-10 B1 reconciliation (v2 key derivation, `prepared/` v3), clean-slate | 1215 | **1086** | **129** | **0** |
| 2026-10-10 poison envelope v2 (`quarantined_at_us`), clean-slate | 1261 | **1126** | **135** | **0** |

The last row is the current truth: a clean-slate run (`rm -rf mutants &&
uv run mutmut run` — the header's incremental gotcha is why) against the
poison-v2 tree (below). It supersedes the B1 row, which superseded the
2026-10-09 row. (The 2026-10-09 row's header said 110 survivors while
its own table footer counted 111 — off by one, resolved at B1 by
regenerating both from that run's actual output: 129 and 129.)

`planner.py`: 0 survivors — every trigger comparison, the
size>slow>age precedence, knob validation, duplicate refusal, ordering
and chunk-bound mutants are killed by `test_planner.py`. All 135
remaining survivors are in `keyspace.py` and are argued equivalent
below; none changes an observable behavior the suite contracts on.

### Phase 3 update — the `poison/` codec

Adding the `poison/` codec to `keyspace.py` (consume-time quarantine)
regenerated the file's mutants: 1166 total. The triage added the
cut-point sweeps (`test_poison_value_truncation_stages_*`, one test per
envelope shape asserting the EXACT named parse stage at every byte
boundary) and the width-boundary pins (`test_poison_reason_accepts_
exactly_the_u16_maximum`, `test_poison_key_and_value_accept_exactly_
the_u32_maximum`, `test_poison_value_bytes_original_accepts_exactly_
the_u32_maximum`, `test_poison_value_round_trip_with_a_multi_byte_
reason_length`). Two findings of note:

- **One genuine bug caught**: the truncation-guard mutants probing the
  `has_key=0` path exposed that a buffer cut exactly after the
  `has_key` flag read `has_value` out of bounds (IndexError instead of
  the named ValueError). `decode_poison_value` now guards the flag read
  ("truncated poison value: flags"); the no-key sweep pins it.
- The 15 surviving poison mutants are ALL in the equivalence classes
  below: 8× E1 (redundant `"big"`), 2× E3 (`"utf-8"` case), 1× E5
  (slice bound past an exactly-length-checked buffer —
  `x_decode_poison_value__mutmut_131`), 4× M (error-message text:
  `x_poison_key__mutmut_5`, `xǁPoisonValueǁ__post_init____mutmut_5`,
  `x_decode_poison_value__mutmut_7`, `x_decode_poison_value__mutmut_124`).

The harness repair that made this run possible: `mutmut run` had been
broken since Phase 2 — `source_paths` lists two FILES, so `mutants/
millrace/` held only those, and `setup_source_paths` puts `mutants/`
first on `sys.path`; conftest's `from millrace.stage import ...` (via
stagekit) then failed collection with `ModuleNotFoundError`.
`[tool.mutmut] also_copy = ["millrace/"]` copies the rest of the
package verbatim (never mutated) so the mutants tree imports.

## Phase 7 update — the B1 review fixes (v2 key derivation, `prepared/` v3)

The PR #331 review's first blocker moved `topic`/`partition` into the
idempotency-key derivation (offsets are per partition — two partitions
flushing one team over one offset range collided under v1) and bumped
the `prepared/` envelope to v3 to carry those derivation inputs. Both
reshape `keyspace.py`'s mutant surface, so the ledger is regenerated
from the 2026-10-10 clean-slate run above; the survivor tables below
carry the NEW indices (the four rewritten functions — `PreparedRequest`,
`encode_prepared_value`, `decode_prepared_value`, `idempotency_key` —
renumbered every mutant they host; all other functions kept theirs).

New load-bearing predicates, all killed by tests rather than ledgered:

- the encode-side refusals (a v3 write without `persisted_at`, or
  without `topic`/`partition`) — pinned by
  `test_prepared_value_encode_requires_persisted_at` and
  `test_prepared_value_encode_requires_the_derivation_inputs`, which
  also red on the `or`→`and`-aware variants... with ONE exception,
  ledgered as class E6 below;
- the width boundaries of the two new fields: `topic` at exactly
  u16-max bytes and `partition` at exactly u32-max are ACCEPTED, one
  byte/value past is refused —
  `test_prepared_value_varlen_fields_accept_exactly_the_u16_maximum`
  and `test_partition_accepts_exactly_the_u32_maximum` (these killed
  `xǁPreparedRequestǁ__post_init____mutmut_28` / `_35` and
  `x_idempotency_key__mutmut_12` / `_17`, the `>`→`>=` / `<=`→`<`
  flips at both widths, in both validators);
- the v3 decode layout itself: the golden vectors (v3 written out
  byte-by-byte; v1/v2 goldens decode-only), the per-stage truncation
  names (`test_prepared_value_truncation_errors_name_the_failed_parse_stage`
  anchors each stage's name at end-of-message, which is why NO
  message-text mutant survives in `decode_prepared_value` any more —
  the old loose `truncated` pin let `x_decode_prepared_value__mutmut_28`
  live in class M), and the strict-prefix property.

Hand-flips (flush.py/keyspace.py derivation behavior mutmut cannot
generate — AGENT.md's rule), each verified RED on the named test:
dropping `partition=` from the v2 name line
(`test_idempotency_key_is_sensitive_to_every_input_field`,
`test_identical_window_on_two_partitions_yields_different_keys`);
dropping `topic=` (the same two, plus
`test_idempotency_key_does_not_canonicalize_the_topic`); the v3 decode
arm disabled (`test_prepared_value_round_trip*` family, 5 red); the
reconcile-time envelope↔stage identity cross-check removed
(`test_a_prepared_entry_naming_another_partition_is_corruption`, in
test_flush.py).

## Phase 8 update — the `poison/` envelope v2 (`quarantined_at_us`)

The retention work (PR #331 review: `poison/` was a forever-growing
prefix) appended the v2 tail to the poison envelope (the purge's age
basis; v1 still decodes as unknown-age, never purged). The rewrite
renumbered every mutant hosted by `PoisonValue.__post_init__` /
`encode_poison_value` / `decode_poison_value`, so the survivor tables
below carry the NEW indices from the 2026-10-10 clean-slate run (the
ledger's standing rule: regenerate the tables to match the run the
modules now produce).

New load-bearing predicates, all killed by tests rather than ledgered:

- the version dispatch itself: a v1 golden vector decodes (None age)
  and a v2 golden is written out byte-by-byte
  (`test_poison_value_golden_vector`,
  `test_poison_value_v2_golden_vector_and_legacy_decode`); the unknown-
  version refusal moved from `2` to `3` in
  `test_poison_value_decode_refusals` because `2` is now KNOWN;
- the v2 tail's exact-width guard: cutting the envelope anywhere inside
  the 8-byte stamp names the parse stage
  (`test_poison_value_v2_truncated_timestamp_tail`), and the round-trip
  property now sweeps `quarantined_at_us` over the full u64 range;
- the width boundary of the new field: `quarantined_at_us` at exactly
  u64-max is accepted, one past is refused (`test_poison_value_validation`).

The survivor delta is +6 net (129 → 135): the three rewritten functions'
mutants renumbered (10 stale names out, 10 in — same equivalence
classes; `x_decode_poison_value__mutmut_131` kept its NUMBER but its
content moved from the E5 slice to the E1 redundant-`"big"` on the same
line), and six genuinely new mutants, ALL in the existing classes:
`x_encode_poison_value__mutmut_68` and `x_decode_poison_value__mutmut_150`
(E1, the v2 tail's redundant `"big"`), `x_decode_poison_value__mutmut_152`
(E5, +9 stop clamped by the exact-length guard one statement up),
`x_decode_poison_value__mutmut_145` / `_157` and
`xǁPoisonValueǁ__post_init____mutmut_28` (M, error-message text whose
pinned substrings survive), and `x_decode_poison_value__mutmut_123` (M —
the tail guard's two-stage split: which guard refuses is message text;
THAT every cut refuses is pinned by the second guard). Hand-flips for the behavior mutmut cannot
generate (the purge lives in stage.py, outside mutmut's scope): the
expiry comparator `<` → `<=` RED on
`test_purge_poison_expired_deletes_only_expired_entries`; the
unknown-age-is-kept guard dropped RED on the same test; the stamp
fallback order flipped RED on `test_poison_records_stamp_policy`; and
the S12 pin — the ack hook observing an un-awaited durability handle —
REDs on `test_ack_hook_fires_only_after_remote_durability`
(test_stage.py) when `stage_batch`'s `await handle.await_durable()`
goes, which the old call-order test provably could not see.

## Killed by tests added in this triage (10)

Three tests added to `tests/test_keyspace.py` (no source changes, no
existing test weakened):

- `test_stats_value_timestamp_field_bounds` — the int64 range checks on
  `StatsValue.first_staged_ts` / `last_staged_ts` were claimed by the
  dataclass but never exercised with an out-of-range value:
  - `xǁStatsValueǁ__post_init____mutmut_7` (`"first_staged_ts"` → `None`)
  - `xǁStatsValueǁ__post_init____mutmut_12` (`"first_staged_ts"` → `"FIRST_STAGED_TS"`)
  - `xǁStatsValueǁ__post_init____mutmut_13` (`"last_staged_ts"` → `None`)
  - `xǁStatsValueǁ__post_init____mutmut_18` (`"last_staged_ts"` → `"LAST_STAGED_TS"`)
- `test_offset_range_field_bounds` — same gap for
  `OffsetRange.first_offset` / `last_offset` (plus `partition=-1`):
  - `xǁOffsetRangeǁ__post_init____mutmut_12` (`"first_offset"` → `None`)
  - `xǁOffsetRangeǁ__post_init____mutmut_17` (`"first_offset"` → `"FIRST_OFFSET"`)
  - `xǁOffsetRangeǁ__post_init____mutmut_18` (`"last_offset"` → `None`)
  - `xǁOffsetRangeǁ__post_init____mutmut_23` (`"last_offset"` → `"LAST_OFFSET"`)
- `test_prepared_body_accepts_exactly_the_u32_maximum` — the one genuine
  behavioral survivor. `PreparedRequest.__post_init__` guards
  `len(body) > _UINT32_MAX`; the flipped `>=` rejects a body of exactly
  2³²−1 bytes, which the `body_len(u32)` wire field can encode. The
  boundary sits at 4 GiB, so the test pins it through `_FakeLength`, a
  duck-typed stand-in whose `len` is faked (the check reads only
  `len(body)`):
  - `xǁPreparedRequestǁ__post_init____mutmut_4` (`>` → `>=`)
  - `xǁPreparedRequestǁ__post_init____mutmut_5` (`ValueError(f"prepared
    body too long…")` → `ValueError(None)`; killed by the
    `match="prepared body too long"` on the reject side)

## Killed at the 2026-10-09 reconciliation (1)

The clean-slate re-run surfaced ONE survivor in no documented
equivalence class: `x_decode_offsets_key__mutmut_15` — the team_id
decode's `signed=False` flipped to `signed=True`, a real semantic change
(team_ids ≥ 2⁶³ decode negative), invisible to
`test_offsets_key_round_trip` because hypothesis's derandomized example
stream is per-test-NAME and that test's stream draws no team_id ≥ 2⁶³
(100/100 examples below the sign bit, reproduced by hand: the flip
passed the full file). The sibling codecs' identical flips die on the
CURRENT streams; that is luck, not cover — a test rename reshuffles a
stream. Fixed with an explicit boundary pin —
`test_team_id_key_decodes_are_unsigned_at_the_signed_bit`
round-trips `2**63 - 1`, `2**63` and `UINT64_MAX` through ALL five
u64-team_id key codecs (stats, offsets, prepared, sched_age, row), so
the class of mutant dies deterministically rather than at the mercy of
a per-name stream. Hand-flip verified RED on the offsets decode.

## Surviving mutants — all equivalent (135)

Each entry: the mutant, its one-line diff, and the justification. The
equivalence arguments are interpreter semantics, verified on the pinned
runtime (CPython 3.13 venv; `requires-python >= 3.12`), not "tests
happen to stay green". The tables are regenerated from a clean-slate
run's actual output whenever the mutated modules change (see the
header); `tests/check_mutants.py` is the mechanical cross-check.

### Class E1 — dropped redundant explicit defaults, `to_bytes`/`from_bytes` (52)

Since Python 3.11 the `byteorder` parameter of `int.to_bytes` /
`int.from_bytes` defaults to `"big"`, and `signed` defaults to `False`;
dropping either argument cannot change any result on any input.
(Verified: `(255).to_bytes(8, "big", signed=False) == (255).to_bytes(8, signed=False) == (255).to_bytes(8, "big")`.)
The BEHAVIOR on these lines — field widths, slice bounds, endianness —
is pinned independently by the golden vectors and the boundary
round-trips; only the redundant explicit arguments are unpinnable.

| `x__encode_u64__mutmut_5` | `return value.to_bytes(8, "big", signed=False)` → `return value.to_bytes(8, signed=False)` |
| `x__encode_u64__mutmut_6` | `return value.to_bytes(8, "big", signed=False)` → `return value.to_bytes(8, "big", )` |
| `x__encode_i64__mutmut_5` | `return value.to_bytes(8, "big", signed=True)` → `return value.to_bytes(8, signed=True)` |
| `x__decode_i64_ordered__mutmut_6` | `return int.from_bytes(data, "big", signed=False) - _SIGN_OFFSET` → `return int.from_bytes(data, signed=False) - _SIGN_OFFSET` |
| `x__decode_i64_ordered__mutmut_7` | `return int.from_bytes(data, "big", signed=False) - _SIGN_OFFSET` → `return int.from_bytes(data, "big", ) - _SIGN_OFFSET` |
| `x_decode_row_key__mutmut_18` | `team_id=int.from_bytes(data[p : p + 8], "big", signed=False),` → `team_id=int.from_bytes(data[p : p + 8], signed=False),` |
| `x_decode_row_key__mutmut_19` | `team_id=int.from_bytes(data[p : p + 8], "big", signed=False),` → `team_id=int.from_bytes(data[p : p + 8], "big", ),` |
| `x_decode_stats_key__mutmut_11` | `return int.from_bytes(data[len(STATS_PREFIX) :], "big", signed=False)` → `return int.from_bytes(data[len(STATS_PREFIX) :], signed=False)` |
| `x_decode_stats_key__mutmut_12` | `return int.from_bytes(data[len(STATS_PREFIX) :], "big", signed=False)` → `return int.from_bytes(data[len(STATS_PREFIX) :], "big", )` |
| `x_decode_stats_value__mutmut_19` | `staged_bytes=int.from_bytes(data[1:9], "big", signed=False),` → `staged_bytes=int.from_bytes(data[1:9], signed=False),` |
| `x_decode_stats_value__mutmut_20` | `staged_bytes=int.from_bytes(data[1:9], "big", signed=False),` → `staged_bytes=int.from_bytes(data[1:9], "big", ),` |
| `x_decode_stats_value__mutmut_30` | `first_staged_ts=int.from_bytes(data[9:17], "big", signed=True),` → `first_staged_ts=int.from_bytes(data[9:17], signed=True),` |
| `x_decode_stats_value__mutmut_41` | `last_staged_ts=int.from_bytes(data[17:25], "big", signed=True),` → `last_staged_ts=int.from_bytes(data[17:25], signed=True),` |
| `x_decode_stats_value__mutmut_52` | `row_count=int.from_bytes(data[25:33], "big", signed=False),` → `row_count=int.from_bytes(data[25:33], signed=False),` |
| `x_decode_stats_value__mutmut_53` | `row_count=int.from_bytes(data[25:33], "big", signed=False),` → `row_count=int.from_bytes(data[25:33], "big", ),` |
| `x_decode_sched_age_key__mutmut_19` | `team_id=int.from_bytes(data[p + 8 : p + 16], "big", signed=False),` → `team_id=int.from_bytes(data[p + 8 : p + 16], signed=False),` |
| `x_decode_sched_age_key__mutmut_20` | `team_id=int.from_bytes(data[p + 8 : p + 16], "big", signed=False),` → `team_id=int.from_bytes(data[p + 8 : p + 16], "big", ),` |
| `x_decode_offsets_key__mutmut_11` | `return int.from_bytes(data[len(OFFSETS_PREFIX) :], "big", signed=False)` → `return int.from_bytes(data[len(OFFSETS_PREFIX) :], signed=False)` |
| `x_decode_offsets_key__mutmut_12` | `return int.from_bytes(data[len(OFFSETS_PREFIX) :], "big", signed=False)` → `return int.from_bytes(data[len(OFFSETS_PREFIX) :], "big", )` |
| `x_encode_offsets_value__mutmut_15` | `out += len(ordered).to_bytes(2, "big")` → `out += len(ordered).to_bytes(2, )` |
| `x_encode_offsets_value__mutmut_28` | `out += len(topic).to_bytes(2, "big")` → `out += len(topic).to_bytes(2, )` |
| `x_encode_offsets_value__mutmut_40` | `out += r.partition.to_bytes(4, "big", signed=False)` → `out += r.partition.to_bytes(4, signed=False)` |
| `x_encode_offsets_value__mutmut_41` | `out += r.partition.to_bytes(4, "big", signed=False)` → `out += r.partition.to_bytes(4, "big", )` |
| `x_decode_offsets_value__mutmut_12` | `count = int.from_bytes(data[1:3], "big")` → `count = int.from_bytes(data[1:3], )` |
| `x_decode_offsets_value__mutmut_31` | `topic_len = int.from_bytes(data[pos : pos + 2], "big")` → `topic_len = int.from_bytes(data[pos : pos + 2], )` |
| `x_decode_offsets_value__mutmut_59` | `partition = int.from_bytes(data[pos : pos + 4], "big", signed=False)` → `partition = int.from_bytes(data[pos : pos + 4], signed=False)` |
| `x_decode_offsets_value__mutmut_60` | `partition = int.from_bytes(data[pos : pos + 4], "big", signed=False)` → `partition = int.from_bytes(data[pos : pos + 4], "big", )` |
| `x_decode_offsets_value__mutmut_74` | `first_offset = int.from_bytes(data[pos : pos + 8], "big", signed=True)` → `first_offset = int.from_bytes(data[pos : pos + 8], signed=True)` |
| `x_decode_offsets_value__mutmut_89` | `last_offset = int.from_bytes(data[pos : pos + 8], "big", signed=True)` → `last_offset = int.from_bytes(data[pos : pos + 8], signed=True)` |
| `x_decode_prepared_key__mutmut_16` | `team_id=int.from_bytes(data[p : p + 8], "big", signed=False),` → `team_id=int.from_bytes(data[p : p + 8], signed=False),` |
| `x_decode_prepared_key__mutmut_17` | `team_id=int.from_bytes(data[p : p + 8], "big", signed=False),` → `team_id=int.from_bytes(data[p : p + 8], "big", ),` |
| `x_encode_prepared_value__mutmut_42` | `+ len(key).to_bytes(2, "big")` → `+ len(key).to_bytes(2, )` |
| `x_encode_prepared_value__mutmut_49` | `+ len(encoded_topic).to_bytes(2, "big")` → `+ len(encoded_topic).to_bytes(2, )` |
| `x_encode_prepared_value__mutmut_56` | `+ partition.to_bytes(4, "big")` → `+ partition.to_bytes(4, )` |
| `x_encode_prepared_value__mutmut_63` | `+ len(request.body).to_bytes(4, "big")` → `+ len(request.body).to_bytes(4, )` |
| `x_decode_prepared_value__mutmut_12` | `key_len = int.from_bytes(data[1:3], "big")` → `key_len = int.from_bytes(data[1:3], )` |
| `x_decode_prepared_value__mutmut_45` | `topic_len = int.from_bytes(data[pos : pos + 2], "big")` → `topic_len = int.from_bytes(data[pos : pos + 2], )` |
| `x_decode_prepared_value__mutmut_73` | `partition = int.from_bytes(data[pos : pos + 4], "big", signed=False)` → `partition = int.from_bytes(data[pos : pos + 4], signed=False)` |
| `x_decode_prepared_value__mutmut_74` | `partition = int.from_bytes(data[pos : pos + 4], "big", signed=False)` → `partition = int.from_bytes(data[pos : pos + 4], "big", )` |
| `x_decode_prepared_value__mutmut_93` | `body_len = int.from_bytes(data[pos : pos + 4], "big")` → `body_len = int.from_bytes(data[pos : pos + 4], )` |
| `x_decode_prepared_value__mutmut_120` | `int.from_bytes(data[pos + body_len :], "big", signed=False) if tail else None` → `int.from_bytes(data[pos + body_len :], signed=False) if tail else None` |
| `x_decode_prepared_value__mutmut_121` | `int.from_bytes(data[pos + body_len :], "big", signed=False) if tail else None` → `int.from_bytes(data[pos + body_len :], "big", ) if tail else None` |
| `x_encode_poison_value__mutmut_15` | `out += len(reason).to_bytes(2, "big")` → `out += len(reason).to_bytes(2, )` |
| `x_encode_poison_value__mutmut_31` | `out += len(value.key).to_bytes(4, "big")` → `out += len(value.key).to_bytes(4, )` |
| `x_encode_poison_value__mutmut_47` | `out += len(value.value).to_bytes(4, "big")` → `out += len(value.value).to_bytes(4, )` |
| `x_encode_poison_value__mutmut_58` | `out += value.value_bytes_original.to_bytes(4, "big")` → `out += value.value_bytes_original.to_bytes(4, )` |
| `x_encode_poison_value__mutmut_68` | `out += value.quarantined_at_us.to_bytes(8, "big")` → `out += value.quarantined_at_us.to_bytes(8, )` |
| `x_decode_poison_value__mutmut_14` | `reason_len = int.from_bytes(data[1:3], "big")` → `reason_len = int.from_bytes(data[1:3], )` |
| `x_decode_poison_value__mutmut_57` | `key_len = int.from_bytes(data[pos : pos + 4], "big")` → `key_len = int.from_bytes(data[pos : pos + 4], )` |
| `x_decode_poison_value__mutmut_102` | `value_len = int.from_bytes(data[pos : pos + 4], "big")` → `value_len = int.from_bytes(data[pos : pos + 4], )` |
| `x_decode_poison_value__mutmut_131` | `value_bytes_original = int.from_bytes(data[pos : pos + 4], "big")` → `value_bytes_original = int.from_bytes(data[pos : pos + 4], )` |
| `x_decode_poison_value__mutmut_150` | `quarantined_at_us = int.from_bytes(data[pos : pos + 8], "big")` → `quarantined_at_us = int.from_bytes(data[pos : pos + 8], )` |

### Class E2 — `signed=None` ≡ `signed=False` (13)

CPython's argument clinic parses the keyword-only `signed` of
`int.to_bytes` / `int.from_bytes` as a truthiness predicate (format
`p`), so `None` is read as `False` for every input. No black-box test
can distinguish the two forms.
(Verified: `int.from_bytes(b"\x00\xff", "big", signed=None) == int.from_bytes(b"\x00\xff", "big", signed=False)`.)
The `signed=True` sibling of these flips is NOT equivalent and is
covered deterministically —
`test_team_id_key_decodes_are_unsigned_at_the_signed_bit` exists because
one such flip survived a clean run (see the reconciliation section).

| `x__encode_u64__mutmut_3` | `return value.to_bytes(8, "big", signed=False)` → `return value.to_bytes(8, "big", signed=None)` |
| `x__decode_i64_ordered__mutmut_4` | `return int.from_bytes(data, "big", signed=False) - _SIGN_OFFSET` → `return int.from_bytes(data, "big", signed=None) - _SIGN_OFFSET` |
| `x_decode_row_key__mutmut_16` | `team_id=int.from_bytes(data[p : p + 8], "big", signed=False),` → `team_id=int.from_bytes(data[p : p + 8], "big", signed=None),` |
| `x_decode_stats_key__mutmut_9` | `return int.from_bytes(data[len(STATS_PREFIX) :], "big", signed=False)` → `return int.from_bytes(data[len(STATS_PREFIX) :], "big", signed=None)` |
| `x_decode_stats_value__mutmut_17` | `staged_bytes=int.from_bytes(data[1:9], "big", signed=False),` → `staged_bytes=int.from_bytes(data[1:9], "big", signed=None),` |
| `x_decode_stats_value__mutmut_50` | `row_count=int.from_bytes(data[25:33], "big", signed=False),` → `row_count=int.from_bytes(data[25:33], "big", signed=None),` |
| `x_decode_sched_age_key__mutmut_17` | `team_id=int.from_bytes(data[p + 8 : p + 16], "big", signed=False),` → `team_id=int.from_bytes(data[p + 8 : p + 16], "big", signed=None),` |
| `x_decode_offsets_key__mutmut_9` | `return int.from_bytes(data[len(OFFSETS_PREFIX) :], "big", signed=False)` → `return int.from_bytes(data[len(OFFSETS_PREFIX) :], "big", signed=None)` |
| `x_encode_offsets_value__mutmut_38` | `out += r.partition.to_bytes(4, "big", signed=False)` → `out += r.partition.to_bytes(4, "big", signed=None)` |
| `x_decode_offsets_value__mutmut_57` | `partition = int.from_bytes(data[pos : pos + 4], "big", signed=False)` → `partition = int.from_bytes(data[pos : pos + 4], "big", signed=None)` |
| `x_decode_prepared_key__mutmut_14` | `team_id=int.from_bytes(data[p : p + 8], "big", signed=False),` → `team_id=int.from_bytes(data[p : p + 8], "big", signed=None),` |
| `x_decode_prepared_value__mutmut_71` | `partition = int.from_bytes(data[pos : pos + 4], "big", signed=False)` → `partition = int.from_bytes(data[pos : pos + 4], "big", signed=None)` |
| `x_decode_prepared_value__mutmut_118` | `int.from_bytes(data[pos + body_len :], "big", signed=False) if tail else None` → `int.from_bytes(data[pos + body_len :], "big", signed=None) if tail else None` |

### Class E3 — `"utf-8"` vs `"UTF-8"` (11)

Codec lookup normalizes case: `codecs.lookup("UTF-8") is
codecs.lookup("utf-8")`, and every `str.encode`/`bytes.decode` result is
identical for all inputs.

| `xǁOffsetRangeǁ__post_init____mutmut_4` | `encoded = self.topic.encode("utf-8")` → `encoded = self.topic.encode("UTF-8")` |
| `x_encode_offsets_value__mutmut_22` | `topic = r.topic.encode("utf-8")` → `topic = r.topic.encode("UTF-8")` |
| `x_decode_offsets_value__mutmut_51` | `topic = bytes(data[pos : pos + topic_len]).decode("utf-8")` → `topic = bytes(data[pos : pos + topic_len]).decode("UTF-8")` |
| `xǁPreparedRequestǁ__post_init____mutmut_23` | `encoded_topic = self.topic.encode("utf-8")` → `encoded_topic = self.topic.encode("UTF-8")` |
| `x_encode_prepared_value__mutmut_25` | `key = request.idempotency_key.encode("utf-8")` → `key = request.idempotency_key.encode("UTF-8")` |
| `x_encode_prepared_value__mutmut_29` | `encoded_topic = topic.encode("utf-8")` → `encoded_topic = topic.encode("UTF-8")` |
| `x_decode_prepared_value__mutmut_29` | `key = bytes(data[pos : pos + key_len]).decode("utf-8")` → `key = bytes(data[pos : pos + key_len]).decode("UTF-8")` |
| `x_decode_prepared_value__mutmut_65` | `topic = bytes(data[pos : pos + topic_len]).decode("utf-8")` → `topic = bytes(data[pos : pos + topic_len]).decode("UTF-8")` |
| `x_idempotency_key__mutmut_7` | `encoded_topic = topic.encode("utf-8")` → `encoded_topic = topic.encode("UTF-8")` |
| `x_encode_poison_value__mutmut_9` | `reason = value.reason.encode("utf-8")` → `reason = value.reason.encode("UTF-8")` |
| `x_decode_poison_value__mutmut_34` | `reason = bytes(data[3 : 3 + reason_len]).decode("utf-8")` → `reason = bytes(data[3 : 3 + reason_len]).decode("UTF-8")` |

### Class E4 — `b"\xff"` vs `b"\xFF"` (1)

Hex escapes are case-insensitive; the two literals are the same bytes
object, so `_prefix_successor` strips exactly the same suffixes.

| `x__prefix_successor__mutmut_5` | `stripped = prefix.rstrip(b"\xff")` → `stripped = prefix.rstrip(b"\xFF")` |

### Class E5 — slice bound extended past an exactly-length-checked buffer (5)

Every decoder below enforces the buffer's exact length (`!=` raises)
before slicing, so a widened stop index is clamped by Python slice
semantics to the same bytes, and a negative index wraps to the same
absolute position. The length guards themselves are pinned by the
strict-prefix/trailing-bytes tests; the mutation is unreachable in its
effect. Per-mutant arithmetic:

| `x_decode_row_key__mutmut_34` | `offset=_decode_i64_ordered(data[p + 16 : p + 24]),` → `offset=_decode_i64_ordered(data[p + 16 : p + 25]),` | len(data) == 29 is enforced two lines up; data[21:30] on a 29-byte buffer is data[21:29].
| `x_decode_stats_value__mutmut_55` | `row_count=int.from_bytes(data[25:33], "big", signed=False),` → `row_count=int.from_bytes(data[25:34], "big", signed=False),` | len(data) == 33 is enforced above; data[25:34] clamps to data[25:33].
| `x_decode_sched_age_key__mutmut_24` | `team_id=int.from_bytes(data[p + 8 : p + 16], "big", signed=False),` → `team_id=int.from_bytes(data[p + 8 : p + 17], "big", signed=False),` | len(data) == p+16 enforced; data[p+8:p+17] clamps to data[p+8:p+16].
| `x_decode_prepared_key__mutmut_27` | `first_offset=_decode_i64_ordered(data[p + 8 : p + 16]),` → `first_offset=_decode_i64_ordered(data[p + 8 : p + 17]),` | len(data) == p+16 enforced; data[p+8:p+17] clamps to data[p+8:p+16].
| `x_decode_poison_value__mutmut_152` | `quarantined_at_us = int.from_bytes(data[pos : pos + 8], "big")` → `quarantined_at_us = int.from_bytes(data[pos : pos + 9], "big")` | len(data) == pos+8 enforced by the v2-tail guard one statement up; the +9 stop clamps to pos+8.

### Class E6 — `or` ≡ `and` behind the dataclass's own guard (1)

`encode_prepared_value` refuses a write missing the v3 derivation inputs
with `if topic is None or partition is None`. Flipping to `and` is
observable only from a call with EXACTLY one of them None — which no
`PreparedRequest` can carry: `__post_init__` refuses the mixed pair
("topic and partition travel together") before any encode. On every
constructible input the two forms agree. (Pinned anyway from the other
side: the both-None refusal is asserted by
`test_prepared_value_encode_requires_the_derivation_inputs`, which kills
the `not`/`is` flips of either disjunct.)

| `x_encode_prepared_value__mutmut_12` | `if topic is None or partition is None:` → `if topic is None and partition is None:` | reachable only with exactly one None — `PreparedRequest.__post_init__` forbids that state. |

### Class M — error-message text the suite deliberately does not pin (52)

The suite pins exception TYPE plus a stable identifying substring (the
field name, or the failed parse stage) and deliberately leaves wording
free. Each of these mutants redecorates a message — an `"XX…XX"`
wrapping, a case flip, or a detail inside an unpinned fragment — while
preserving the pinned substring, so `pytest.raises(ValueError,
match=…)` still matches; the raise CONDITION is pinned by the test
named per row. Pinning full message text is rejected on purpose: it
couples the suite to phrasing, not behavior. (The None/UPPERCASE
siblings of these mutants DID die wherever the mutation touched the
pinned substring itself; the survivors include two UPPERCASE mutants
of the `persisted_at` refusal's LATER fragments — the pinned substring
"without persisted_at" sits in the first fragment, and mutmut mutates
f-string fragments separately.)

| `x__prefix_successor__mutmut_8` | `raise ValueError("a prefix of only 0xFF bytes has no successor")` → `raise ValueError("XXa prefix of only 0xFF bytes has no successorXX")` | error text only; the raise is pinned by test_prefix_successor_strips_only_trailing_ff (match='no successor' survives the decoration).
| `x__prefix_successor__mutmut_9` | `raise ValueError("a prefix of only 0xFF bytes has no successor")` → `raise ValueError("a prefix of only 0xff bytes has no successor")` | error text only; same pin as above.
| `xǁRowKeyǁ__post_init____mutmut_5` | `_require_uint64("team_id", self.team_id)` → `_require_uint64("XXteam_idXX", self.team_id)` | field-name arg feeds only the error message; the check is pinned by test_row_key_field_validation (loose match still finds 'team_id').
| `xǁRowKeyǁ__post_init____mutmut_11` | `_require_int64("timestamp_us", self.timestamp_us)` → `_require_int64("XXtimestamp_usXX", self.timestamp_us)` | same — pinned by test_row_key_field_validation.
| `xǁRowKeyǁ__post_init____mutmut_17` | `_require_int64("offset", self.offset)` → `_require_int64("XXoffsetXX", self.offset)` | same — pinned by test_row_key_field_validation.
| `x_team_rows_prefix__mutmut_5` | `_require_uint64("team_id", team_id)` → `_require_uint64("XXteam_idXX", team_id)` | same — pinned by test_key_functions_validate_team_id.
| `x_staged_rows_range__mutmut_5` | `_require_int64("first_offset", first_offset)` → `_require_int64("XXfirst_offsetXX", first_offset)` | same — pinned by test_offset_arguments_are_validated.
| `x_staged_rows_range__mutmut_11` | `_require_int64("last_offset", last_offset)` → `_require_int64("XXlast_offsetXX", last_offset)` | same — pinned by test_offset_arguments_are_validated.
| `xǁStatsValueǁ__post_init____mutmut_5` | `_require_uint64("staged_bytes", self.staged_bytes)` → `_require_uint64("XXstaged_bytesXX", self.staged_bytes)` | same — pinned by test_stats_value_validation.
| `xǁStatsValueǁ__post_init____mutmut_11` | `_require_int64("first_staged_ts", self.first_staged_ts)` → `_require_int64("XXfirst_staged_tsXX", self.first_staged_ts)` | same — pinned by test_stats_value_timestamp_field_bounds.
| `xǁStatsValueǁ__post_init____mutmut_17` | `_require_int64("last_staged_ts", self.last_staged_ts)` → `_require_int64("XXlast_staged_tsXX", self.last_staged_ts)` | same — pinned by test_stats_value_timestamp_field_bounds.
| `xǁStatsValueǁ__post_init____mutmut_23` | `_require_uint64("row_count", self.row_count)` → `_require_uint64("XXrow_countXX", self.row_count)` | same — pinned by test_stats_value_validation.
| `x_stats_key__mutmut_5` | `_require_uint64("team_id", team_id)` → `_require_uint64("XXteam_idXX", team_id)` | same — pinned by test_key_functions_validate_team_id.
| `xǁSchedAgeKeyǁ__post_init____mutmut_5` | `_require_int64("first_staged_ts", self.first_staged_ts)` → `_require_int64("XXfirst_staged_tsXX", self.first_staged_ts)` | same — pinned by test_sched_age_key_validation.
| `xǁSchedAgeKeyǁ__post_init____mutmut_11` | `_require_uint64("team_id", self.team_id)` → `_require_uint64("XXteam_idXX", self.team_id)` | same — pinned by test_sched_age_key_validation.
| `x_sched_age_range_through__mutmut_5` | `_require_int64("cutoff_ts", cutoff_ts)` → `_require_int64("XXcutoff_tsXX", cutoff_ts)` | same — pinned by test_sched_age_range_through_validates_cutoff.
| `xǁOffsetRangeǁ__post_init____mutmut_16` | `_require_int64("first_offset", self.first_offset)` → `_require_int64("XXfirst_offsetXX", self.first_offset)` | same — pinned by test_offset_range_field_bounds.
| `xǁOffsetRangeǁ__post_init____mutmut_22` | `_require_int64("last_offset", self.last_offset)` → `_require_int64("XXlast_offsetXX", self.last_offset)` | same — pinned by test_offset_range_field_bounds.
| `x_offsets_key__mutmut_5` | `_require_uint64("team_id", team_id)` → `_require_uint64("XXteam_idXX", team_id)` | same — pinned by test_key_functions_validate_team_id.
| `x_decode_offsets_value__mutmut_25` | `raise ValueError("truncated offsets value: topic length")` → `raise ValueError("XXtruncated offsets value: topic lengthXX")` | error text only; the truncation guard is pinned by test_offsets_value_truncation_errors_name_the_failed_parse_stage (stage substring survives decoration).
| `x_decode_offsets_value__mutmut_44` | `raise ValueError("truncated offsets value: range record")` → `raise ValueError("XXtruncated offsets value: range recordXX")` | error text only; same pin as above.
| `xǁPreparedRequestǁ__post_init____mutmut_11` | `_require_uint64("persisted_at", self.persisted_at)` → `_require_uint64("XXpersisted_atXX", self.persisted_at)` | field-name arg feeds only the error message; pinned by test_prepared_value_encode_requires_persisted_at (match='persisted_at').
| `xǁPreparedRequestǁ__post_init____mutmut_17` | `raise ValueError("topic and partition travel together (both or neither), got …")` — the `XX…XX` decoration | error text only; the refusal is pinned by test_prepared_value_encode_requires_the_derivation_inputs (match='travel together' survives it).
| `xǁPreparedRequestǁ__post_init____mutmut_26` | `raise ValueError("topic must be non-empty")` → `raise ValueError("XXtopic must be non-emptyXX")` | error text only; pinned by the same test (match='topic').
| `x_encode_prepared_value__mutmut_4` | `"cannot persist a prepared request without persisted_at: an "` → `"XXcannot persist a prepared request without persisted_at: an XX"` | error text only; the refusal is pinned by test_prepared_value_encode_requires_persisted_at (match='without persisted_at' survives the decoration).
| `x_encode_prepared_value__mutmut_6` | `"entry of unknown age can never be checked against the "` → `"XXentry of unknown age can never be checked against the XX"` | error text only; an unpinned fragment of the same refusal, same pin.
| `x_encode_prepared_value__mutmut_7` | `"entry of unknown age can never be checked against the "` → `"ENTRY OF UNKNOWN AGE CAN NEVER BE CHECKED AGAINST THE "` | case flip of an unpinned fragment; same pin.
| `x_encode_prepared_value__mutmut_8` | `"receipt horizon"` → `"XXreceipt horizonXX"` | error text only; an unpinned fragment of the same refusal, same pin.
| `x_encode_prepared_value__mutmut_9` | `"receipt horizon"` → `"RECEIPT HORIZON"` | case flip of an unpinned fragment; same pin.
| `x_encode_prepared_value__mutmut_16` | `"cannot persist a prepared request without topic/partition: "` → `"XXcannot persist a prepared request without topic/partition: XX"` | error text only; the refusal is pinned by test_prepared_value_encode_requires_the_derivation_inputs (match='without topic/partition' survives the decoration).
| `x_encode_prepared_value__mutmut_18` | `"the v3 envelope carries the idempotency key's derivation "` → `"XXthe v3 envelope carries the idempotency key's derivation XX"` | error text only; an unpinned fragment of the same refusal, same pin.
| `x_encode_prepared_value__mutmut_19` | `"the v3 envelope carries the idempotency key's derivation "` → `"THE V3 ENVELOPE CARRIES THE IDEMPOTENCY KEY'S DERIVATION "` | case flip of an unpinned fragment; same pin.
| `x_encode_prepared_value__mutmut_20` | `"inputs so a recovered entry is self-describing"` → `"XXinputs so a recovered entry is self-describingXX"` | error text only; an unpinned fragment of the same refusal, same pin.
| `x_encode_prepared_value__mutmut_21` | `"inputs so a recovered entry is self-describing"` → `"INPUTS SO A RECOVERED ENTRY IS SELF-DESCRIBING"` | case flip of an unpinned fragment; same pin.
| `x_idempotency_key__mutmut_10` | `raise ValueError("topic must be non-empty")` → `raise ValueError("XXtopic must be non-emptyXX")` | error text only; pinned by test_idempotency_key_validation (match='topic').
| `x_idempotency_key__mutmut_23` | `_require_uint64("team_id", team_id)` → `_require_uint64("XXteam_idXX", team_id)` | field-name arg feeds only the error message; pinned by test_idempotency_key_validation.
| `x_idempotency_key__mutmut_29` | `_require_int64("first_offset", first_offset)` → `_require_int64("XXfirst_offsetXX", first_offset)` | same — pinned by test_offset_arguments_are_validated.
| `x_idempotency_key__mutmut_35` | `_require_int64("last_offset", last_offset)` → `_require_int64("XXlast_offsetXX", last_offset)` | same — pinned by test_offset_arguments_are_validated.
| `x_prepared_key__mutmut_5` | `_require_uint64("team_id", team_id)` → `_require_uint64("XXteam_idXX", team_id)` | field-name arg feeds only the error message; pinned by test_key_functions_validate_team_id.
| `x_prepared_key__mutmut_11` | `_require_int64("first_offset", first_offset)` → `_require_int64("XXfirst_offsetXX", first_offset)` | same — pinned by test_offset_arguments_are_validated.
| `xǁFlushedKeyǁ__post_init____mutmut_5` | `_require_uint64("team_id", self.team_id)` → `_require_uint64("XXteam_idXX", self.team_id)` | field-name arg feeds only the error message; pinned by test_flushed_key_field_validation.
| `xǁFlushedKeyǁ__post_init____mutmut_11` | `_require_int64("first_offset", self.first_offset)` → `_require_int64("XXfirst_offsetXX", self.first_offset)` | same — pinned by test_flushed_key_field_validation.
| `xǁFlushedKeyǁ__post_init____mutmut_17` | `_require_int64("last_offset", self.last_offset)` → `_require_int64("XXlast_offsetXX", self.last_offset)` | same — pinned by test_flushed_key_field_validation.
| `x_poison_key__mutmut_5` | `_require_int64("offset", offset)` → `_require_int64("XXoffsetXX", offset)` | field-name arg feeds only the error message; pinned by test_poison_key_round_trip_and_order (match='offset').
| `xǁPoisonValueǁ__post_init____mutmut_5` | `raise ValueError("reason must be non-empty")` → `raise ValueError("XXreason must be non-emptyXX")` | error text only; pinned by test_poison_value_validation (match='reason must be non-empty' survives the decoration).
| `xǁPoisonValueǁ__post_init____mutmut_28` | `_require_uint64("quarantined_at_us", self.quarantined_at_us)` → `_require_uint64("XXquarantined_at_usXX", self.quarantined_at_us)` | field-name arg feeds only the error message; pinned by test_poison_value_validation (match='quarantined_at_us' survives the XX wrapping).
| `x_decode_poison_value__mutmut_7` | `raise ValueError(f"unknown poison value version {data[0]}")` → `raise ValueError(f"unknown poison value version {data[1]}")` | the reported byte in the message, not the raise; pinned by test_poison_value_decode_refusals (match='version').
| `x_decode_poison_value__mutmut_126` | `f"trailing bytes ({len(data) - pos} left at {pos})"` → `f"trailing bytes ({len(data) + pos} left at {pos})"` | arithmetic inside the message, not the guard; pinned by test_poison_value_decode_refusals (match='trailing|length mismatch').
| `x_decode_poison_value__mutmut_123` | `if len(data) < pos + 4:` → `if len(data) < pos - 4:` | which of the two guards refuses a valueless-envelope truncation, not WHETHER it is refused: the v1 tail's `len(data) != pos` backstop (and v2's `!= pos + 8`) catches every cut the weakened first guard lets through; both messages carry the pinned "length mismatch" (test_poison_value_truncation_stages_without_a_value), and no input parses under one form and refuses under the other. |
| `x_decode_poison_value__mutmut_145` | `f"({len(data) - pos} byte(s) left at {pos}, need exactly 8)"` → `f"({len(data) + pos} byte(s) left at {pos}, need exactly 8)"` | arithmetic inside the v2-tail truncation message, not the guard; pinned by test_poison_value_v2_truncated_timestamp_tail (match='quarantined_at_us').
| `x_decode_poison_value__mutmut_157` | `f"poison value length mismatch: trailing bytes "` fragment → the same fragment's `len(data) - pos` → `len(data) + pos` | arithmetic inside the message, not the guard; pinned by test_poison_value_decode_refusals (match='length mismatch').
| `x_team_prepared_range__mutmut_5` | `_require_uint64("team_id", team_id)` → `_require_uint64("XXteam_idXX", team_id)` | field-name arg feeds only the error message; pinned by test_team_prepared_range_validates_team_id (loose match still finds 'team_id'). (M4's outstanding-entry gate range.)
<!-- total 135 -->

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
