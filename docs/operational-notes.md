# Operational Notes — hoglake at 2PB / 1T rows (2026-09-05)

This file is the PRE-DEPLOY PROJECTION, written from bench figures before
hoglake ran in production. Where production has since measured a number this
file predicts, the measured one is in [AGENT.md](../AGENT.md) §Scale doctrine
and [server/README.md](../server/README.md); read those first, and this for
the shape of the problem.

Scale context: the DuckLake deployment hoglake replaces stands at 2PB
and ~1T rows. The straight answer to "how does hoglake improve on
this": at this scale hoglake changes almost nothing about the *data
plane* and nearly everything about the *metadata plane* — which is
where every production scar came from. The parquet bytes are the same
bytes. What changes is that the catalog stops being the bottleneck,
the liability, and the incident generator.

## What doesn't change (be honest about this)

- **Storage cost / scan throughput of the data itself.** 2PB of
  parquet is 2PB of parquet. Trino/Spark scan speed is the engine's
  problem and the file layout's problem — hoglake helps the layout
  only indirectly (compaction, stats quality).
- **The object store.** S3 throughput, listing costs, egress —
  unchanged.

## What changes, quantified against DuckLake's observed numbers

The row-by-row record of what the predecessor did, and where hoglake answers
each, is [docs/ducklake-defect-ledger.md](ducklake-defect-ledger.md); the
design comparison and the bench figures are the two tables in the
[repository README](../README.md) (§Compared to DuckLake and Iceberg,
§Performance), which are the single citation for both. Three differences have
no entry in either:

- **One catalog implementation, deployed once.** The predecessor ran two
  engines against one catalog (an upstream extension and a fork), so fork
  fixes never reached maintenance. Fork drift, extension distribution and the
  per-connection OOM loop are all that arrangement, not the data.
- **CDC is a catalog concern, not a consumer's.** `table_changes()` was a
  read-barrier crawl with every consumer hand-building cursors, and expiry
  silently destroyed unread ranges. Here the changefeed, the consumer offsets
  and the retention floor are catalog state, and expiry *pages* when a
  consumer pins instead of losing its data.
- **Recovery is a ledger read, not archaeology.** A split brain on the
  predecessor was reconstructed from S3 delete markers. The drain ledger keeps
  what was deleted, when, why and after how many attempts.

## The catalog's own size at 2PB / 1T rows (the sizing question)

- **Files**: 2PB at ~256MB/file ≈ ~8M `hog_data_file` rows. Trivial
  for Postgres with the partial live index.
- **Stats** — the real volume driver: 8M files × ~50 columns ≈ ~400M
  `hog_file_column_stats` rows, ~60–80GB with indexes. The catalog's
  dominant table by far. Postgres handles it (narrow PK-clustered
  btree, cascade-deleted with its file), but note it: the catalog
  stays small relative to 2PB (~0.004%), and stats are what you'd
  partition first if it ever stopped being small. The DuckLake
  version of this table was smaller only because it was broken (3.7M
  rows, 99.4% garbage).
- **Row-id allocator**: 1T rows against a bigint `next_row_id` — 2^63
  headroom, a non-issue by design, with the CHECK + `addExact`
  two-layer defense against overflow.
- **Snapshots**: the busiest writer mints ~138K/day; the advisory-lock
  tail sustains hundreds/s. ~100× headroom, and bench says commit
  latency doesn't move with history depth.

## The honest ceilings to watch

1. **One Postgres per service; one advisory-lock tail per catalog.**
   Commit throughput per catalog is capped at 1/tail-latency. At the
   observed ~1.6 mints/s average (bursts higher), fine. If a single
   catalog ever needs sustained >100 commits/s, that's the wall. The
   escape hatch is to move validation in front of the serialization
   point rather than inside it: designed once, never scheduled, and no
   longer written down anywhere but here.
2. **Read scaling.** Facade/Trino planning reads hit the service →
   one PG. Snapshot-pinned reads are replica-able (lag-tolerant by
   construction), but that's not built yet.
3. **Compaction IO cost at 2PB.** The never-convoy design streams both
   ends (`S3InputFile` / `S3OutputFile`) and touches no local disk;
   only the sorted path materializes survivors in a heap. A sweep
   rewrites `max_groups_per_run` × `parallel_groups` groups — 1 × 1 at
   the compiled defaults, 64 × 6 on gigahog-prod-us. At 2PB with
   continuous small-file ingestion, whether those knobs keep up with
   the small-file accumulation rate is a real capacity-planning
   question — the knobs exist; the tradeoff is intentional.
4. **Consumer-floor pinning at 2PB.** A stuck consumer pins expiry →
   end-snapshotted files accumulate as storage. DuckLake's answer was
   silent loss; hoglake's is bounded staleness + a page. Correct
   tradeoff, but at 2PB the pinned-storage bill grows faster.

## The migration note

2PB doesn't move — the converter is metadata-only (paths carried,
row-id allocators preserved, lineage spans the cutover). The real
decision is snapshot-history depth: converting 15.3M snapshots is a
choice, not a default — at this scale, head+retention-window. And the
FK proof matters: every orphan class in the defect ledger is a row the
new constraints will reject, so the converter *is* the audit of how
dirty the old catalog actually is.

## Bottom line

At 2PB/1T, DuckLake's failures were never about the data being big —
they were about every commit, expiry, and maintenance run paying
O(catalog) against a metadata store with no integrity and no arbiter.
Hoglake's numbers at bench scale (flat commits, 0 conflicts, 100–200×
expiry, still_referenced=0) are the structural answers to exactly
those numbers. The data plane was never the problem.
