-- PER-CATALOG LIVE TOTALS ON THE MAINTENANCE SUMMARY, stamped once per
-- generation, so the metrics sampler reads one row per catalog (#269).
--
-- `hoglake_live_rows` / `_bytes` / `_files` and the totals behind
-- `/v1/info` and the catalogs listing were a full pass over
-- hog_data_file every 15 s on every pod. The maintenance summary's
-- tier rows already hold the same sums per bucket, as of the published
-- generation — but summing them at every tick is O(buckets) per
-- catalog per pod, which on an instance with 10^7 buckets is the same
-- shape as the pass it replaced, merely smaller. So the sum is taken
-- ONCE, by the publish statement (MaintenanceSummarySampler), from the
-- generation it has just finished, and stored here; the tick reads it.
--
--   live_files / live_bytes   over every bucket of the published
--                             generation: live files at the scan's
--                             snapshot, on tables live when the scan
--                             reached them.
--   live_rows                 the same for record_count, NULL when the
--                             generation was not measured (V22's deploy
--                             straddle): absent, never an undercount.
--   published_snapshot        the generation's scan snapshot. A table
--                             dropped AFTER it may still have buckets
--                             in the generation (the drop touches no
--                             file row and leaves the generation
--                             alone); the tick subtracts those, found
--                             through hog_table by `dropped_snapshot >
--                             published_snapshot`, so the totals
--                             exclude dropped tables exactly as the
--                             per-table series do. A table dropped at
--                             or before the snapshot was skipped by the
--                             scan and has no buckets to subtract.
--
-- All four NULL until the first publish by a sampler that stamps them:
-- a catalog with none is reported ABSENT (no gauge, `null` on the
-- wire), not as zero and not by scanning its manifest — the catalog
-- that cannot publish a generation (its scan outruns its retention) is
-- precisely the one whose manifest must not be scanned every 15 s.
SET LOCAL lock_timeout = '5s';

ALTER TABLE hog_maintenance_summary
    ADD COLUMN IF NOT EXISTS live_files bigint,
    ADD COLUMN IF NOT EXISTS live_bytes bigint,
    ADD COLUMN IF NOT EXISTS live_rows bigint,
    ADD COLUMN IF NOT EXISTS published_snapshot bigint;
