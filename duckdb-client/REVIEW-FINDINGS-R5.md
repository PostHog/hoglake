# Adversarial review findings — round 5 (2026-09-14)

Consolidated single-reviewer pass at 075a093. 4 confirmed (2 live-reproduced
on the dev stack). Verdict: CONTINUE.

## R5-1. [HIGH] (wire/object-store) src/storage/hoglake_delete.cpp:58 — LIVE REPRODUCED

**Claim:** The delete sink's NULL-rowid guard is an `InternalException`, and a
NULL rowid is reachable from object-store parquet contents, so one
externally-registered data file turns any DELETE/UPDATE on the table into
whole-instance invalidation.

**Scenario (reproduced):** registered, through the ordinary append/commit API,
a parquet whose second column carries the reserved parquet field id
2147483646 and holds a NULL (see R5-2 for why that column becomes the rowid
source). `SELECT a, rowid` returns `(2, NULL)`; `DELETE FROM lake.ns1.fid
WHERE a = 2` dies with `INTERNAL Error: hoglake: NULL row-id column in DELETE
input` in `HoglakeDelete::Sink`, then every subsequent statement on the
process — all attached catalogs, all connections — fails `FATAL Error:
database has been invalidated` until restart. UPDATE shares the sink.

**Verifier:** Full path from commit API to throw, no earlier refusal on any
layer. Server `FileRegistrationDto` (api/Dto.kt:169) accepts
path/record_count/file_size_bytes/footer_size/column_stats/partition_values
only — the parquet is never opened, field ids never inspected (the /verify
endpoint's own doc, hoglake.yaml:427, states the field-id contract is
explicitly NOT covered); `explicit_row_ids` is not client-settable and the
registered file came back false. Client parse bounds never see parquet cell
values. RequireDMLAllowed/PlanDelete refuse only AT-pinned tables, RETURNING
and buffered-write shapes. The reader (hoglake_multi_file_reader.cpp:150)
binds the column and propagates its NULL into row_id_indexes[0]. Not a
documented divergence. Same class as the R4-1/R4-2 pair (HIGH, fixed with
typed refusals); the R4 corrected sweep re-swept casts and comparisons but
not VALIDITY MASKS, so this site was never reclassified.

## R5-2. [HIGH] (data) src/storage/hoglake_multi_file_reader.cpp:150 — LIVE REPRODUCED

**Claim:** The rowid virtual column takes its value from any local parquet
column bearing field id 2147483646, decided purely by
`TryFindColumnByFieldId` on the file's own field ids and NEVER consulting the
wire's `explicit_row_ids` flag — so a positional file that happens to carry
the reserved field id silently reports file contents as hoglake row ids
instead of `row_id_start + file_row_number`.

**Scenario (reproduced):** same registration as R5-1, `explicit_row_ids:
false`, `row_id_start: 0`, 3 records. Correct rowids are 0,1,2. Actual:
a=1→rowid 10, a=2→rowid NULL, a=3→rowid 30. Consequences: (a) rowid and any
rowid-keyed client logic read garbage on a table entirely in-contract from
the catalog's point of view; (b) the R4-6/R4-7 duplicate-path attribution
rests on `base == row_id_start` for non-explicit files — with a poisoned base
the positions either hit the loud "matches no live registration"
TransactionException or, when junk values differ from file_row_number by
another copy's row_id_start, are attributed to the WRONG logical copy
silently — exactly the shape the R4 fix was built to exclude; (c) it enables
R5-1.

**Verifier:** `explicit_row_ids` is consulted in exactly two places
(hoglake_multi_file_list.cpp:255, hoglake_delete.cpp:142) and never in the
reader. `HoglakeMultiFileReader::Bind` builds global columns from the wire's
server-allocated field ids (small ints), so the reserved id can only enter
through a file's local columns. No server-side field-id validation on
registration. The client `_hog` prefix guard is NAME-based (catalog column
names), not physical parquet field ids — the repro column is named `junk`.
DESIGN.md §5 (266-269) documents the opposite behaviour ("for
explicit_row_ids files (compaction outputs) it reads the physical
_hog_row_id column"), so this is a divergence from stated design. The
complementary case is already loud (explicit_row_ids true + missing column →
InvalidInputException at line 159); only this direction is unguarded.

## R5-3. [MEDIUM] (catalog/taxonomy) src/rest/hoglake_api_client.cpp:428-434 + src/storage/hoglake_schema_entry.cpp:207-215 — statically argued

**Claim:** The containment set (CatalogException + InvalidInputException) also
swallows REQUEST-SCOPED server refusals, because ThrowFor maps HTTP 400/422 →
InvalidInputException and 410 → InvalidInputException — so a travel-scoped
refusal applying to every table makes the catalog listing silently report an
empty schema: the precise outcome the taxonomy comment
(hoglake_api_client.cpp:75-79) forbids ("a down or broken server must fail the
statement, never fake an empty or partial catalog").

**Scenario:** every transaction pins head at start
(hoglake_transaction.cpp:56) and sends `?snapshot=<pinned>` on every GET
/tables/{t}. If expiry advances the floor past that pin during a long-lived
read transaction, the server returns 410 for EACH table
(CatalogService.resolveReadSnapshot 637-650 → HoglakeException.Expired → 410
per ErrorMapping.kt:27); a snapshot outside [0, head] returns 422 the same
way. LoadAllTablesInternal catches each, continues, sets all_tables_loaded —
SHOW ALL TABLES / duckdb_tables() returns EMPTY with no error.

**Verifier:** TryGetTable has no pre-filter for these statuses other than
404→nullptr; LookupTableAtInternal (AT-travel) is deliberately uncontained and
still fails loud. Partial mitigation (hence medium): each skipped table is
recorded in unrepresentable_tables, so a direct SELECT/DROP on a named table
rethrows the 410 message rather than "does not exist" — only the listing
lies. Not live-reproduced (forcing a 410 needs an expiry sweep against a
pinned snapshot on shared dev infrastructure).

## R5-4. [LOW] (memory) src/storage/hoglake_puffin.cpp:281-284 — statically argued

**Claim:** ReadDeletionVector sizes a raw heap array from
`file_handle->GetFileSize()` with no cap and outside the buffer manager, so a
DV file of arbitrary size written by any other client is read fully into
process memory before any structural validation runs.

**Verifier:** `make_unsafe_uniq_array<data_t>(file_size)` precedes every
magic/CRC/length check; the only server-side bound on a registered DV is
`delete_count <= record_count` (CommitService.kt:723), which says nothing
about bytes, and `file_size_bytes` on the registration is never compared
against the object. Failure is bad_alloc/OOM rather than InternalException,
and the trigger requires an out-of-contract DV writer — hence low. The R4
sweep hardened EncodeBlob's size cast but left the read side's allocation
unbounded.

## Checked and clean (reviewer's own list)

- Typed DV refusal completeness: the union (hoglake_delete.cpp:185-191) covers
  all three position sources and the bound check at :202 sits after it.
  Live-confirmed: UPDATE on dup_incon returns the targeted Invalid Input
  Error, nothing commits, instance survives. NumericCast on the max position
  cannot overflow (DecodeBlob caps at INT64_MAX via non-negative int32 bucket
  key).
- Rowid-base attribution: live-verified on fresh duplicate registrations —
  DELETE/UPDATE WHERE rowid = 4 touches only the matching copy; the three
  ambiguity refusals are correctly ordered; AddDeletes merges by data_file_id
  so the unmatched-copy `continue` cannot drop an earlier registration. The
  only way past the base logic is R5-2.
- GetBoundedInt coverage: every remaining bare GetInt (spec_id, sort_id,
  source_field_id ×2, snapshot schema_version, change object_id) lands in an
  int64_t wire field with no downstream NumericCast or narrow store;
  DataFile.spec_id has its own sign/type check; uint-above-INT64_MAX is caught
  by the [0, MAX_I64] lower bound. No unbounded parsed numeric remains.
- TryCast NULL cells: both sites (snapshot_time, table_uuid) write into
  nullable table-function output vectors; no consumer dereferences them.
- Every `throw InternalException` outside the API client swept:
  hoglake_insert.cpp:250 looked reachable but is refused server-side twice
  (AlterService.setPartitionSpec:405, dropColumn:187). The rest are genuinely
  both-sides-engine.

---

# Round-5 fix dispositions (2026-09-14, this branch)

All 4 confirmed findings fixed. Suite: `./test/run-live-tests.sh`
green — 11 files / 638 assertions + cross-client wire check.

| # | Disposition |
|---|---|
| R5-2 (HIGH, root) | FIXED, with a ROUND-6 CORRECTION: as shipped this fix removed the field-id probe whose fall-through was the ONLY guard for the complementary direction, so "both mismatch directions are symmetric and loud" was false and untested (R6-1, a HIGH regression). Round 6 restores that refusal as an explicit check and adds the missing fixture/assertions. Original disposition follows. The CATALOG decides the rowid source: `explicit_row_ids` now rides `extended_info` from the scan plan, and `GetVirtualColumnExpression` gates the physical-column path on it instead of probing the file's field ids. Both mismatch directions are now symmetric and loud: flag true + column missing → the pre-existing InvalidInputException; flag false + reserved field id present → new typed refusal naming the file and the invariant (the client is the enforcement point — the server never opens registered parquet and /verify excludes the field-id contract). DESIGN.md §5 rewritten to state this (it previously described the intended-but-unimplemented behaviour). Fixture `bad_fieldid` registers, through the ordinary commit API, a parquet whose `junk` column carries field id 2147483646 (values 10, NULL, 30); tested: plain column reads still work, `SELECT rowid` and `DELETE` refuse typed, instance alive. |
| R5-1 (HIGH, symptom) | FIXED. The delete sink's NULL-rowid guard and its negative-position sibling are now typed InvalidInputExceptions naming table and file. The mandated THIRD widening of the sweep rule — validity masks on wire/object-store-derived vectors count as external input — was applied across the codebase: `grep` for `validity.`/`RowIsValid` finds throwing sites only in this sink (both now typed); every other NULL-check on external data (`IsNull()` on copy statistics, parsed identifiers, settings, partition keys) is a branch, not a throw, and was re-verified. With R5-2 fixed, the NULL cell can no longer reach the sink through the reserved-field-id door — the typed refusal stands as defense in depth, and the suite asserts the R5-1 repro via the R5-2 refusal (honest note: no in-contract route to the sink's NULL branch remains to test directly). |
| R5-3 (MEDIUM) | FIXED by classifying refusals at the throw site, not the containment boundary: 410 (below the expiry floor) and snapshot/at_timestamp-scoped 422s are REQUEST-SCOPED by construction — they apply to every object — and now throw TransactionException, which is deliberately outside the catalog containment set, so a listing fails loudly instead of reporting an empty schema. Table-scoped 400/422s stay InvalidInputException and remain contained. The 410 message now also says the transaction's pinned snapshot can no longer be read (start a new transaction). Not tested live: forcing a 410 needs an expiry sweep against a pinned snapshot on shared dev infrastructure, which this branch must not do — the classification is at the single throw site (`ThrowFor`) and the containment sets are unchanged. |
| R5-4 (LOW) | FIXED. `ReadDeletionVector` bounds the file size (256 MiB) before allocating, with a typed InvalidInputException naming the file; a DV is a roaring bitmap plus a small footer, so the cap is far above any legitimate object. |

## Noted for the server side (no action taken here, per instruction)

The server never validates parquet field ids at registration
(`FileRegistrationDto` carries no schema information and the parquet is
never opened), and `/verify`'s own documentation excludes the field-id
contract — that combination is what makes R5-2 reachable in-contract.
Recorded as DESIGN.md server finding 9's sibling; the coordinator files
the hoglake issue.
