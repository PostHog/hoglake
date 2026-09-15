# duckdb-client — the hoglake DuckDB extension

DuckDB back as a *client* of the catalog, never as the engine the
catalog lives inside: an out-of-tree DuckDB extension that speaks
hoglake's REST contract
(`server/src/main/resources/openapi/hoglake.yaml`) where the DuckLake
extension speaks SQL against a metadata database it owns.

```sql
INSTALL httpfs; LOAD httpfs;             -- s3 reads/writes
CREATE SECRET (TYPE s3, ...);
ATTACH 'hoglake:my_catalog' AS lake (ENDPOINT 'http://localhost:8080');

SELECT * FROM lake.ns1.points WHERE team = 7;   -- partition-pruned
INSERT INTO lake.ns1.points VALUES (1, 'a');    -- footer-shipping commit
DELETE FROM lake.ns1.points WHERE id = 1;       -- deletion vector
SELECT * FROM lake.ns1.points AT (VERSION => 12);
SELECT * FROM hoglake_snapshots('lake');
```

The catalog is still a service: the extension never sees Postgres. It
writes parquet to object storage itself (field ids stamped, footer
stats collected) and registers it through the commit API, exactly like
pyhoglake — the same contract, a different language.

## Status

Read, write, DML, DDL, time travel, metadata functions and maintenance
passthroughs are implemented and verified against a live dev stack; six
capabilities are blocked on wire gaps and are listed as such rather
than faked.

| Doc | What |
|---|---|
| [DESIGN.md](DESIGN.md) | architecture, module map, wire-client decision, transaction/pinning model, read/write paths, test strategy, and the numbered **findings for the server** |
| [PARITY.md](PARITY.md) | every DuckLake SQL-surface capability with DONE / PARTIAL / WIRE GAP / N-A / TODO |
| [PROGRESS.md](PROGRESS.md) | milestone log (M0–M5) and the adversarial-review rounds |
| `REVIEW-FINDINGS*.md` | the review rounds themselves: each finding, its verifier, and its fix disposition |

Divergences from DuckLake are deliberate and documented (in DESIGN.md
and PARITY.md), never silent: DDL is eager (own server snapshot, so
`ROLLBACK` does not undo it), UPDATE assigns new row ids (the
`FileRegistration` wire has no `explicit_row_ids`), uncommitted INSERTs
are invisible to their own transaction's scans (own DELETEs are not —
those merge), and written partitions are identity-transform only.

## Build

```bash
scripts/fetch-deps.sh                       # pinned duckdb + extension-ci-tools (gitignored)
VCPKG_OVERLAY_TRIPLETS=$PWD/vcpkg-triplets \
VCPKG_TOOLCHAIN_PATH=~/.vcpkg/scripts/buildsystems/vcpkg.cmake \
BUILD_EXTENSION_TEST_DEPS=full make release GEN=ninja
```

`BUILD_EXTENSION_TEST_DEPS=full` builds httpfs (the s3 filesystem the
integration tests need) and sets `USE_MERGED_VCPKG_MANIFEST=1`. Build
without it and vcpkg reconciles `build/release/vcpkg_installed`
against the root manifest and **deletes curl/openssl/zlib** — the
symptom is a link or HTTP failure on the next full run. The overlay
triplet pins `HAVE_PIPE2=0` (the macOS SDK declares `pipe2` as
macOS-27, which breaks curl's detection).

The `duckdb` and `extension-ci-tools` trees are pinned clones rather
than submodules only because this branch does not touch the repo root;
they become ordinary submodules when the extension is extracted to its
own community-extension repo.

## Test

```bash
./test/run-live-tests.sh        # fixtures -> sqllogictests -> cross-client wire check
```

It fails — loudly, non-zero — if a fixture gate the suite needs was not
exported, if the runner skipped any file, if fewer test files ran than
exist, or if the assertion count fell below its floor. `require-env` is
a WHOLE-FILE skip in sqllogictest, so an ungated-looking green run
could otherwise be a run that silently dropped a file (R7-1); each
conditional fixture gate accordingly owns its own `.test` file.

Needs the dev stack (`just server compose-up && just server run`) plus
a pyhoglake checkout (`PYHOGLAKE_DIR`, default `~/src/hoglake/pyhoglake`)
for the fixtures. `make test` alone runs the sqllogictests, which
**skip cleanly without `HOGLAKE_URL`** — so a green `make test` with no
server up is not a verification of anything. Say which you ran.

The suite creates disposable `duckext-*` catalogs in a `duckext-itest`
bucket, and its files are idempotent: repeat runs without re-running
the fixture assert the same counts. Three things it deliberately proves
rather than assumes: row
ids survive a real server compaction (the fixture force-compacts and
the test reads the `_hog_row_id` carrier), extension-written and
pyhoglake-written partition values land in byte-identical partition
strings, and both directions of the reserved-field-id contract refuse
typed instead of killing the instance.
