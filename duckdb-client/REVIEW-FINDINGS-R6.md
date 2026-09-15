# Adversarial review findings — round 6 (2026-09-14)

Consolidated single-reviewer pass at `eeea23c`. 4 confirmed (1 live-reproduced).
Verdict: CONTINUE. **R6-1 is a regression introduced by fix wave 5.**

## R6-1. [HIGH] (object-store) src/storage/hoglake_multi_file_reader.cpp:159-161 — LIVE REPRODUCED — REGRESSION

**Claim:** The R5-2 fix returns the physical-column binding unconditionally when
`explicit_row_ids` is true, and by deleting the `TryFindColumnByFieldId` probe it
deleted the *only* guard for the complementary direction — so a registration with
the flag set whose parquet lacks field id 2147483646 now reaches DuckDB's
`FieldIdMapper::GetDefault`, which throws `InternalException` and invalidates the
whole instance.

**Scenario (reproduced):** table `ns1.ct`, 8 single-row appends, server-compacted
into two `explicit_row_ids: true` outputs. Replaced one output object in MinIO
with a parquet carrying field ids but no `_hog_row_id` (padded so
`file_size_bytes` 626 / `footer_size` 402 still match). `SELECT id` still returns
0..7; `SELECT id, rowid` dies with `INTERNAL Error: No default expression in
FieldId Map` in `FieldIdMapper::GetDefault` → `FATAL: database has been
invalidated`. Every connection dead until restart. DELETE/UPDATE always project
rowid, so all DML on the table is affected.

**Verifier:** `row_id_column` is built in the reader's constructor with name +
identifier only — **no `default_expression`** — so when the mapper cannot find id
2147483646 it calls `GetDefaultExpression` → `GetDefault` → `throw
InternalException` (duckdb multi_file_column_mapper.cpp:116-127), with nothing in
between. The **pre-wave-5 code refused this typed**: it probed the field id first,
fell through to `options.find("row_id_start")` (which hoglake_multi_file_list.cpp:255
omits for explicit-row-id files) and threw `InvalidInputException("file … has no
row_id_start and no _hog_row_id column")` — the mitigation R4-10's verifier
recorded. Wave 5 removed the fall-through, so the commit message's "flag set with
the column missing keeps its existing refusal" and DESIGN.md §5's "The
complementary direction — flag set, column absent — is refused the same way" are
both FALSE. Asymmetry: a file with NO field ids falls back to BY_NAME and raises
the typed `ThrowColumnNotFoundError` — only the field-id-bearing case is fatal,
which is exactly what compaction outputs are. Object-store-reachable (shared
bucket; hoglake never re-reads registered parquet; /verify excludes the field-id
contract) or a compaction bug. Same class as R4-2, rated HIGH.

## R6-2. [LOW] (tests) test/sql/hoglake_wire_hardening.test:241-277, test/fixtures/read_fixture.py:316-360 — statically argued

**Claim:** The `bad_fieldid` coverage exercises only the flag-false/column-present
direction; no fixture or assertion exists for flag-true/column-missing — which is
why R6-1 shipped green under a commit claiming "both mismatch directions are
symmetric and loud".

**Verifier:** grep of the suite finds no explicit-row-id file with a missing
reserved column; `hoglake_compacted_read.test` reads only intact compaction
output. The symmetry claim has no test behind it.

## R6-3. [LOW] (wire/taxonomy) src/rest/hoglake_api_client.cpp:425-432 — statically argued

**Claim:** The request-scoped/table-scoped 422 split is a case-insensitive
substring match for `"snapshot"`/`"at_timestamp"` over the server's JSON
`error`+`detail`, so a request-scoped 422 arriving without a conforming JSON body
— a proxy, a framework default 422, or any future server rewording — falls
through to `InvalidInputException`, is contained by `LoadAllTablesInternal`, and
produces the silent empty listing the fix exists to prevent.

**Verifier:** with an absent/non-JSON body, `error` stays `"HTTP 422"` and
`detail` is empty, so neither token matches. In-contract paths confirmed live:
`AT (VERSION => 999999)` yields `TransactionContext Error … snapshot 999999 out
of range` and propagates while the same session's `SHOW ALL TABLES` returns 9
tables; the table-scoped `bad_dec_neg` parse error stays contained. A table named
`snapshot_log` cannot trip it (`what` is excluded from the matched text). 410 is
unconditional and every server 410 is genuinely request-scoped. Low because the
trigger needs a non-conforming responder.

## R6-4. [LOW] (docs) DESIGN.md:266-282 — statically argued

**Claim:** The §5 rewrite is a botched edit — an orphaned, mis-indented duplicate
of its own closing sentence follows the replacement paragraph, and the final
substantive sentence documents behaviour the code does not have (R6-1). Same
stale/false-contract-comment class as R3-10 and R4-11.

## Checked and clean

- **New trust path:** `HoglakeMultiFileList::GetFile` is the sole producer of
  `extended_info` (GetAllFiles, Copy, and the pruned ComplexFilterPushdown list all
  route through it), so `explicit_row_ids` rides every scan route. Live-verified
  rowids on plain positional scan, duplicate-path registrations, compaction output
  (0..4 from the physical column), and AT-travel over a compacted file. The
  `bad_fieldid` direction refuses typed naming file and reserved id; plain reads
  still work; instance survives.
- **False positives:** flag-false + reserved id is always illegitimate through the
  reference server — `FileRegistrationDto` has no `explicit_row_ids` field and the
  flag is written in exactly one place (CompactionService.kt:738). The only route
  to flag-false-with-column is re-registering a compaction output's path through
  the append API, whose positional row ids would be garbage anyway. Refusing is
  correct.
- **Validity-check sweep (independent):** grepped RowIsValid/IsNull()/validity
  across src/ — the delete sink's two sites are the only *throwing* ones; every
  other use is a branch. Builder's claim holds. Both sink refusals are typed and
  name table + file.
- **Prior clean list:** R5-4's cap is applied before allocation and before any
  read; the out-of-range DV refusal, rowid-base attribution with its three
  ambiguity refusals, and GetBoundedInt coverage are untouched by this wave and
  re-read as correct.

## Environment note

The reviewer's `make release` (run without `USE_MERGED_VCPKG_MANIFEST=1`) made
vcpkg reconcile `build/release/vcpkg_installed` against the root `vcpkg.json`
(roaring only) and delete curl/openssl/zlib. Restored by running vcpkg against
`build/extension_configuration/vcpkg.json` with
`VCPKG_OVERLAY_TRIPLETS=./vcpkg-triplets`; `libcurl.a`, `libssl.a`,
`libcrypto.a`, `libz.a` are back. No repo files were modified.

---

# Round-6 fix dispositions (2026-09-14, this branch)

All 4 findings fixed. Suite: `./test/run-live-tests.sh` green.
Environment verified before trusting the run: `build/release/
vcpkg_installed/arm64-osx/lib` holds libcurl/libssl/libcrypto/libz/
libroaring, and `build/release/extension/` still contains httpfs — the
reviewer's restore is intact, and every build here used
`BUILD_EXTENSION_TEST_DEPS=full` with the overlay triplet.

| # | Disposition |
|---|---|
| R6-1 (HIGH, my regression) | FIXED with an EXPLICIT check, not a restored fall-through: when `explicit_row_ids` is true the reader now probes for field id 2147483646 BEFORE returning the physical binding and, when absent, throws a typed InvalidInputException naming the file and the mismatch ("registered WITH explicit row ids but its parquet has no column carrying the reserved field id"). A later edit deleting some unrelated branch can no longer silently remove the guard. Live fails-before/passes-after: the fixture's byte-patched compaction output previously died in FieldIdMapper::GetDefault with INTERNAL + instance invalidation; it now refuses typed on `SELECT rowid` and on `DELETE`, with the instance alive after each. |
| R6-2 (LOW, systemic) | FIXED. New fixture `fid_missing`: the server compacts a real table, then the fixture BYTE-PATCHES the output's footer so the reserved field id becomes 2147483645 — thrift compact encodes both as 5-byte zigzag varints, so file size and footer size (both validated by the reader against the registration) are unchanged, while no column carries the reserved id. Gated by `DUCKEXT_FID_MISSING` (exported only if the stack actually compacted). `hoglake_wire_hardening.test` now asserts BOTH directions of the contract. Lesson taken: the symmetry claim now has a test behind it, and the R5 disposition's claim is corrected below. |
| R6-3 (LOW) | FIXED as suggested: the classification is now made from OUR OWN request shape. `ThrowFor` takes `travel_scoped`, set by the two call sites that can carry a snapshot/at_timestamp selector (`TryGetTable`, `PlanScan`) via `!travel.IsHead()`; any 422 on such a request is request-scoped and propagates, whatever the responder said or whether it sent a JSON body at all. The substring match is retained only as a secondary net for a NON-travel request whose body still names the selector. |
| R6-4 (LOW) | FIXED. DESIGN.md §5 rewritten: the orphaned duplicate sentence is gone, and the text now describes what the code does — the catalog's flag selects the source, and the client then checks that the file agrees, with BOTH disagreements refused by their own explicit check (never a fall-through), each covered by a test. |

## Correction to the round-5 disposition (in place)

R5-2's disposition said "both mismatch directions are symmetric and
loud … the pre-existing InvalidInputException". That was false as
shipped: the complementary direction's refusal had been a fall-through
that the same commit removed, and no test covered it. Both directions
are now explicit and tested; the R5 file's claim is corrected there.
