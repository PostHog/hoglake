-- THE RECEIPT STOPS STORING THE REQUEST, AND STARTS EXPIRING (#240).
--
-- `hog_commit_receipt` (V7) exists to answer one question — "was this
-- idempotency key already published, and what snapshot did it produce" —
-- and it answered it by keeping the WHOLE request as jsonb, forever.
-- Measured on gigahog-prod-us, 2026-09-30, `GET /v1/database/health`:
-- 366,740 rows, 58.2 GiB total, of which 58.1 GiB is TOAST, 0 dead
-- tuples. ~160 KiB per receipt, ~4 GiB/day at ~50 commits/min, and
-- 4-5x that once pyhoglake #234's concurrent uploads reach millpond.
-- Nothing ever deleted a row: V7's comment says receipts outlive
-- snapshot expiry, and "outlive expiry" was implemented as "outlive
-- everything". The insert is also ~38 ms of the ~100 ms a 270-file x
-- 25-column commit holds the per-catalog commit lock (measured on the
-- #240 fixture).
--
-- What a receipt needs is a COMPARISON, not a copy: one version byte
-- plus SHA-256 over the canonical fingerprint string
-- `CommitFingerprint.kt` already produced. The canonicalization itself
-- does not change here — same wire mapper, same order-insensitivity over
-- appends, files and column stats — so "same key, same request" decides
-- what it decided before.
--
-- WHAT DOES CHANGE is that the canonical string becomes a PERSISTED
-- format: a digest cannot be re-canonicalized, so the old comparison's
-- immunity to serializer drift (both sides produced by the same code at
-- the same instant) is gone. Hence the version byte —
-- `COMMIT_FINGERPRINT_VERSION`, which must be bumped by any change to
-- that string, and which lets the replay arm answer a version it cannot
-- compare with "this key was published, here is its snapshot" instead of
-- a 422 telling a correct client its request differs.
-- `CommitFingerprintGoldenTest` holds the string and the digest as
-- LITERALS and is the tripwire for that bump; `CommitFingerprintTest`
-- deliberately does not, because it computes its expectation from the
-- live mapper and would stay green through the drift.
--
-- ============================================================
-- WHY FOUR STATEMENTS AND NOT A NEW TABLE
-- ============================================================
--
-- An earlier draft of this change made a `PARTITION BY RANGE (created_at)`
-- twin and dropped a day's partition at a time. With the body gone a
-- receipt is ~100 bytes of fixed-width columns, so seven days at
-- 250 commits/min is ~250 MB — the size at which partition management
-- (a wider primary key, a partition-maintenance loop, a two-table read
-- path through the transition) buys nothing that a bounded DELETE on an
-- index does not. The columns are added in place instead.
--
-- ============================================================
-- 1. fingerprint bytea, NULLABLE
-- ============================================================
--
-- Nullable, and that is the transition rather than a convenience. Rows
-- already in the table have no digest and cannot be given one: the
-- digest is over the canonical string, and re-canonicalizing 366,740
-- stored payloads means reading 58 GiB of TOAST through the JVM's
-- serializer under no lock anyone would want to hold. So NULL means
-- "pre-V24, compare the body" and `CommitService`'s replay check keeps
-- the old string comparison for exactly those rows. A row this build
-- writes always has one.
--
-- 32 bytes as `bytea` rather than the 64-char hex text: half the size,
-- and no encoding to agree on.
--
-- ============================================================
-- 2. created_at timestamptz NOT NULL DEFAULT now()
-- ============================================================
--
-- The retention key. `now()` is STABLE, not VOLATILE, so this ADD COLUMN
-- is a METADATA-ONLY fast default (PG 11+): the value is evaluated once,
-- stored in `pg_attribute.attmissingval`, and every pre-existing row
-- reads it back without the table being rewritten.
-- `V24CommitReceiptRetentionMigrationIntegrationTest` asserts
-- `atthasmissing` directly, and that every row seeded before the ALTER
-- reads back ONE distinct timestamp — because "metadata-only" is the
-- difference between this statement and a rewrite of a 58 GiB table
-- under ACCESS EXCLUSIVE, and `clock_timestamp()` in place of `now()`
-- would silently be the latter.
--
-- THE SEMANTICS OF THAT ARE THE ONES WE WANT. Every legacy row is dated
-- at the migration, so the whole backlog becomes purgeable together,
-- `HOGLAKE_RECEIPT_RETENTION_SECONDS` after the deploy — seven days of
-- grace in which any client still holding a prepared payload from before
-- the deploy can replay it and be answered. It is not the row's true age
-- (the true age is unknowable — V7 stored no timestamp), and the
-- direction of the error is the safe one: every legacy row is treated as
-- YOUNGER than it is, so nothing is purged early.
--
-- ============================================================
-- 3. request DROP NOT NULL
-- ============================================================
--
-- So the writer can stop filling it. The column itself stays until no
-- null-fingerprint row is left, because the replay path still reads it
-- for those rows.
--
-- THE FOLLOW-UP THIS FILE OWES: once
-- `SELECT count(*) FROM hog_commit_receipt WHERE fingerprint IS NULL`
-- is 0 — which the purge guarantees within the retention window of this
-- deploy, since every legacy row is dated at the migration — a further
-- migration does `ALTER TABLE hog_commit_receipt DROP COLUMN request`
-- and `CommitService` loses the fallback arm.
--
-- WHAT RETURNS THE 58 GiB IS THE PURGE, NOT THAT DROP, and an earlier
-- draft of this paragraph had it exactly backwards. `DROP COLUMN` sets
-- `attisdropped` and does nothing else: the TOAST relation is the
-- table's `reltoastrelid` and survives, the toast pointers stay in the
-- existing heap tuples, and the docs say plainly it "will not
-- immediately reduce the on-disk size of your table". What frees the
-- files is the sequence this change already performs — the purge deletes
-- the legacy rows AND their chunk rows (`heap_delete` calls
-- `heap_toast_delete` synchronously), autovacuum reclaims them, and
-- because every surviving receipt is post-V24 and toasts nothing, the
-- TOAST relation ends with no live chunks at all, so plain VACUUM's
-- truncation phase can return essentially all of it. `VACUUM FULL` /
-- `pg_repack` are only needed if truncation is blocked. What never
-- shrinks either way is the RDS ALLOCATED volume, so the win is free
-- space, backup size and restore time rather than a smaller bill.
--
-- ============================================================
-- 4. hog_commit_receipt_created (catalog_id, created_at)
-- ============================================================
--
-- The purge's access path. `CleanupService.RECEIPT_PURGE_PAGE_SQL` asks
-- one catalog for its oldest receipts past the cutoff, in pages:
--
--   WHERE catalog_id = ? AND created_at < ? ORDER BY created_at LIMIT ?
--
-- (the page is resolved by that select and then deleted BY `ctid`; the
-- statement's KDoc has why neither the primary key nor a range works).
--
-- The table's only index is its primary key `(catalog_id,
-- idempotency_key)`, and an idempotency key is a random UUID, so it
-- carries no time locality at all: without this index the statement is a
-- scan of the catalog's entire receipt history to find the oldest 1,000
-- of them, which is the 2026-09-28 expiry shape (AGENT.md, Scale
-- doctrine) applied to a second table. With it the eligible rows are a
-- dense PREFIX of the catalog's index range, so a page is one descent
-- and a walk, and the cursor never needs to skip anything.
--
-- MEASURED on a 100,000-receipt single-catalog fixture (PG 18, warm,
-- EXPLAIN (ANALYZE, BUFFERS) of the service's own `internal` statement,
-- 60% of rows past the cutoff, `created_at` scattered through the heap),
-- by `V24CommitReceiptRetentionMigrationIntegrationTest`, which runs
-- this file against rows seeded BEFORE it and prints both plans:
--
--   with the index:    Index Scan using hog_commit_receipt_created,
--                      Index Searches: 1, 1,007 buffers, nothing filtered
--   without it:        Seq Scan + top-N heapsort, 2,568 buffers,
--                      Rows Removed by Filter: 39,801
--
-- 2.5x at 100,000 receipts, and THE RATIO IS NOT THE POINT — the shape
-- is. The sequential scan grows with the catalog's whole receipt
-- history, every page, forever; the descent does not grow at all.
-- gigahog-prod-us's heap is 61.8 MiB today, which is ~7,900 pages, so a
-- scan there is already ~7,900 buffers against the same ~1,000 — and
-- that is PER PAGE, of which the legacy backlog needs 367. (The 7,900 is
-- arithmetic on the production heap size, not a measurement on
-- production; only the fixture's numbers above were measured.)
--
-- The assertions are structural rather than on those numbers: the index
-- name appears, `Seq Scan on hog_commit_receipt` does not, nothing is
-- `Rows Removed by Filter` and nothing is sorted — which is the set
-- AGENT.md says actually separates an index that drives the predicate
-- from one whose leading column merely admits the scan.
--
-- BUILT CONCURRENTLY, and the measurement is this table's, not V17's.
-- The heap is small — 61.8 MiB for 366,740 rows, because the 58 GiB is
-- TOAST and an index on `(catalog_id, created_at)` never dereferences a
-- TOAST pointer — so BOTH builds are fast and the trade is not about
-- duration. It is about WHO waits: a plain build takes SHARE on
-- `hog_commit_receipt`, whose only writer is the commit tail of every
-- catalog in the fleet, so a blocking build stalls every commit for its
-- duration, inside this file's 5 s window, and rolls the deploy back if
-- it does not fit. A concurrent build stalls nothing. V16's trade came
-- out the other way because its table's writer was the cleanup drain;
-- here the writer is the hottest path in the system, and the build is
-- cheap enough that two heap passes over 62 MiB cost less than one
-- second of blocked commits.
--
-- PRE-BUILD IT OUT OF BAND. V17's shape, and REQUIRED rather than
-- offered, because `Database.kt`'s rule is not about duration: "a
-- migration whose build can PARK behind a foreign snapshot, which is any
-- concurrent build, is one to pre-apply out of band". Parking is
-- size-independent. This build runs with `statement_timeout = 0` and
-- waits out every transaction older than itself, so one long-running
-- foreign snapshot — an operator's psql in a transaction, a pg_dump, an
-- RDS export, a logical-decoding reader — holds the first replica inside
-- Flyway's advisory lock past `MIGRATION_LOCK_WAIT` (120 s) while every
-- other pod crash-loops waiting for it. The build being fast at this
-- size does not help: the wait is the other transaction's length, not
-- the build's.
--
-- So, before promoting, after checking `pg_stat_activity` for
-- transactions older than a minute:
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS hog_commit_receipt_created
--       ON hog_commit_receipt (catalog_id, created_at);
--
-- which makes the statement below the no-op it then is. Running the
-- migration without the pre-build is not wrong — it is a bet on no
-- foreign snapshot being open, and the bet is unnecessary.
--
-- ============================================================
-- WHY THIS FILE IS NOT TRANSACTIONAL
-- ============================================================
--
-- `CREATE INDEX CONCURRENTLY` cannot run inside a transaction, so this
-- file takes V17's and V19's shape: `executeInTransaction=false`, a
-- save-and-restore `lock_timeout` pair around the heavy statements, and
-- the cost that a failure PART WAY THROUGH leaves a `success = false`
-- history row that fails Flyway's validate on every replica until an
-- operator runs `flyway repair`. The three ALTERs are ordered before the
-- build and are each metadata-only, so the realistic partial failure is
-- "the ALTERs landed, the build did not", which the DO block below
-- cleans up on the next attempt. Splitting the build into its own
-- migration (V19/V20's split) would avoid the repair; it is not worth a
-- second file for a build this size, and the ALTERs are useless without
-- the index anyway.
--
-- DEPLOY NOTE. All three ALTERs need ACCESS EXCLUSIVE on
-- `hog_commit_receipt`, and the table's writer is the commit tail — held
-- for the ~100 ms a large commit takes, not for a sweep's duration, so
-- the wait is milliseconds rather than the V20-class wait on an expiry
-- sweep. The 5 s `lock_timeout` below still bounds it: if it fires the
-- file rolls back that statement, the pod crash-loops, and the next
-- attempt lands. For the index, pre-build it out of band (section 4).
--
-- THE REAL COST OF THIS CHANGE ARRIVES SEVEN DAYS LATER, and it is not a
-- lock. When the legacy backlog becomes eligible the purge deletes
-- 366,740 receipts and their ~31M TOAST chunk rows over ~1,834 pages
-- (`CleanupService.RECEIPT_PURGE_PAGE`), and the autovacuum that follows
-- is hours of throttled I/O over 58 GiB next to the commit path. It
-- takes no blocking lock and nothing waits on it, but it is the event to
-- expect on the volume's IOPS graph, and it is what actually returns the
-- space. `hoglake_commit_receipt_purge_failures_total` is the signal
-- that it is NOT draining.
SELECT set_config('hoglake.migration_lock_timeout', current_setting('lock_timeout'), false);
SET lock_timeout = '5s';

-- 32 bytes of SHA-256 over the canonical fingerprint string. NULL = a
-- pre-V24 row, whose body is still the comparison (section 1).
ALTER TABLE hog_commit_receipt ADD COLUMN IF NOT EXISTS fingerprint bytea;

-- The retention key. Metadata-only: `now()` is STABLE, so this is a fast
-- default and every legacy row reads back the migration's timestamp
-- (section 2).
ALTER TABLE hog_commit_receipt
    ADD COLUMN IF NOT EXISTS created_at timestamptz NOT NULL DEFAULT now();

-- The writer stores a digest instead (section 3). Idempotent: dropping a
-- NOT NULL that is already gone is a no-op.
ALTER TABLE hog_commit_receipt ALTER COLUMN request DROP NOT NULL;

-- An INVALID remnant is cleared before the build. `CREATE INDEX
-- CONCURRENTLY IF NOT EXISTS` matches on NAME alone: it would find a
-- half-built index from a cancelled build, skip, and leave one that
-- every commit's receipt insert maintains and no query may use. A
-- cancelled build is what a killed pod, an operator's Ctrl-C or a
-- statement timeout leaves behind. Plain `DROP INDEX` takes ACCESS
-- EXCLUSIVE on the TABLE and cannot be CONCURRENTLY inside a DO block,
-- so it runs under the 5 s bound: fail fast and retryably rather than
-- convoy every catalog's commits.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_index i ON i.indexrelid = c.oid
        WHERE c.relname = 'hog_commit_receipt_created' AND NOT i.indisvalid
    ) THEN
        EXECUTE 'DROP INDEX hog_commit_receipt_created';
    END IF;
END $$;

-- Restore BEFORE the concurrent build: it waits out every transaction
-- older than itself, which is exactly the wait a 5 s lock_timeout would
-- abort.
SELECT set_config('lock_timeout', current_setting('hoglake.migration_lock_timeout'), false);

-- `statement_timeout = 0` for the build and nothing else. A pod's
-- session carries 60 s (Database.SESSION_INIT_SQL); the build is a
-- fraction of a second on a 62 MiB heap, so the bound is nowhere near —
-- it is lifted because a build the bound kills leaves an INVALID index
-- and a failed history row, and the DO block above is the other half of
-- that story.
SELECT set_config('hoglake.migration_statement_timeout', current_setting('statement_timeout'), false);
SET statement_timeout = 0;

CREATE INDEX CONCURRENTLY IF NOT EXISTS hog_commit_receipt_created
    ON hog_commit_receipt (catalog_id, created_at);

SELECT set_config('statement_timeout', current_setting('hoglake.migration_statement_timeout'), false);
