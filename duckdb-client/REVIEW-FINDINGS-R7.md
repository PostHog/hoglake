# Adversarial review findings — round 7 (2026-09-14)

Review at `c1d43db` (round-6 fixes). **No code defects found — the
first clean code round.** Two test-layer findings, written up here from
the reviewer's report.

## R7-1. [MEDIUM] (tests/harness) test/sql/hoglake_wire_hardening.test, test/run-live-tests.sh — LIVE REPRODUCED — introduced by fix wave 6

**Claim:** `require-env` in DuckDB's sqllogictest is a **whole-file**
skip disposition regardless of where it appears in the file
(`sqllogic_test_runner.cpp` records it as a terminal, whole-test skip).
The wave-6 `require-env DUCKEXT_FID_MISSING` therefore silently
disables the ENTIRE `hoglake_wire_hardening.test` — including the
assertions protecting previously-confirmed HIGH findings (poison
decimals R3-4/R4-5/R4-9, case-colliding columns, dup_path rowid-base
attribution R4-6/R4-7, dup_incon's typed out-of-range refusal
R4-1/R4-2, bad_fieldid R5-2) — whenever the dev stack fails to compact
`fid_missing`. The fixture explicitly anticipates that path: on
compaction timeout it warns and does not export the variable.

**Verifier (reproduced here):** with `DUCKEXT_FID_MISSING` unset,
`make test` reports `12 test cases` instead of 13, prints `Skipped
tests for the following reasons: require-env DUCKEXT_FID_MISSING: 1`,
and **exits 0**; `run-live-tests.sh` printed `ALL LIVE TESTS PASSED`.
The pre-existing mid-file `require-env DUCKEXT_SORTED_COMPACTED`
(`hoglake_compacted_read.test:79`) has the same semantics. A harness
that can silently skip its own regression coverage invalidates every
green report it has produced.

## R7-2. [LOW] (tests) test/sql/hoglake_wire_hardening.test — pre-existing from wave 4

**Claim:** the file twice claims "Rerun-stable: repeat runs delete 0
rows and assert the same counts" — false. Run 2 without re-running the
fixture fails at the `a <> 2` group assertion, because the later
`DELETE ... WHERE a = 1` destroys that assertion's precondition. The
sanctioned flow re-runs the fixture, so CI was unaffected, but the
false claim cost the reviewer two runs to separate "stale state" from
"the dup_path attribution regressed" — the same ambiguity would hide a
genuine regression.

---

# Round-7 fix dispositions (2026-09-14, this branch)

| # | Disposition |
|---|---|
| R7-1 (a) structural | FIXED. Each conditional fixture gate now owns its file: `hoglake_fieldid_missing.test` (new, `DUCKEXT_FID_MISSING`) and `hoglake_sorted_compacted.test` (new, `DUCKEXT_SORTED_COMPACTED`, split out of `hoglake_compacted_read.test`). `hoglake_wire_hardening.test` and `hoglake_compacted_read.test` keep only gates that are unconditional on the dev stack's behaviour. Both new files state in their header WHY they exist, so the pattern survives the next edit. |
| R7-1 (b) sweep | DONE. Every `require` / `require-env` in `test/sql/*.test` is now in its file's header block; no `config`/`tag`/`mode`/`load` directives exist mid-file anywhere in the suite. |
| R7-1 (c) green means green | FIXED — the important one. `run-live-tests.sh` now (1) asserts every fixture gate in `REQUIRED_GATES` is exported after sourcing `live-env.sh`, failing fast and naming the missing gate (this is the real trigger: the fixture drops a gate when the stack could not produce it), (2) fails if the runner printed `Skipped tests for the following reasons`, echoing every reason, (3) asserts the reported test-case count equals the number of `.test` files, and (4) enforces an assertion floor. Its success line now reports what actually ran: `ALL LIVE TESTS PASSED (13 test files, 645 assertions, 0 skipped)`. Verified against the reproduction: the skip log that previously exited 0 trips checks (2) and (3). |
| R7-2 | FIXED, claim made TRUE rather than deleted: the `a <> 2` assertion became `a = 3` — the only key untouched by both DELETEs — so the file is genuinely idempotent. Verified by running `hoglake_wire_hardening.test` three times WITHOUT re-running the fixture: 52 assertions, green, three times. |

## Was the wave-6 "643 assertions" run genuine?

**Yes.** `test/fixtures/live-env.sh` as written by that run contains
`export DUCKEXT_FID_MISSING=1` (the fixture reported `fixture ready`,
not the compaction-timeout warning), and `run-live-tests.sh` sources it
before `make test`, so `hoglake_wire_hardening.test` ran. The
independent check: the total moved 638 → 643, exactly the +5
assertions wave 6 added to that file — a skip would have moved it to
610. The number in the record stands.

For the same reason the risk was latent, not realised: on a run where
the dev stack had not compacted, the number would have dropped ~33 and
nothing in the harness would have said so. Check (3) above now makes
that arithmetic unnecessary.

## Current honest count

`./test/run-live-tests.sh` → **13 test files, 645 assertions, 0
skipped**, plus the cross-client partition-wire check (4 groups
byte-shared). 645 vs wave 6's 643: the file split adds two
`require`-satisfied test cases, and the R7-2 rewrite trades one
two-row group assertion for a one-row one.
