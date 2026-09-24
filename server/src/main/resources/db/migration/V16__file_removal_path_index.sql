-- The commit path reads the WHOLE removal queue of a catalog, once per
-- commit. `CommitService.REMOVAL_QUEUE_COLLISION_SQL` (the path-reuse
-- guard, invariant 4) and `UploadService.QUEUE_ABANDONED_UPLOAD_SQL`'s
-- first `NOT EXISTS` both ask
--
--   catalog_id = ? AND drained_at IS NULL AND path = <probe>
--
-- and hog_file_removal's only index before this file is
-- `hog_file_removal_drain (catalog_id, removal_id) WHERE drained_at IS
-- NULL`, whose second column is a gap in that predicate. So the guard
-- gets a sequential scan, and the cost of a commit sizes with the DEPTH
-- OF THE CLEANUP QUEUE rather than with the commit. Measured on
-- gigahog-prod-us (#199, 2026-09-24): 83,569 sequential scans of
-- hog_file_removal against 67,554 index scans since the 2026-09-18
-- restart, over a queue holding 164,307 undrained rows on catalog
-- millpond-prod-us (#198) at ~100 commits/minute. That is exactly the
-- shape AGENT.md's index rule forbids: a hot statement whose work is
-- unbounded in another subsystem's backlog.
--
-- PARTIAL on `drained_at IS NULL`, because both statements carry that
-- predicate and the undrained rows are the minority of a table that is
-- also a 30-day forensics ledger (HOGLAKE_REMOVAL_LEDGER_RETENTION_
-- SECONDS). The partial index therefore holds the queue and not the
-- ledger, and a drain — which sets drained_at — REMOVES the row from it
-- rather than updating it in place.
--
-- NOT UNIQUE, deliberately, and this is the load-bearing decision in
-- this file. `(catalog_id, path) WHERE drained_at IS NULL` looks like
-- an invariant — the `NOT EXISTS` in UploadService reads as if one path
-- can have at most one undrained row — but nothing in the four writers
-- makes it one, and a unique index would turn a benign duplicate into a
-- hard failure of expiry, which is the sweep that advances the
-- retention floor:
--
--   * Nothing makes a file PATH unique. hog_data_file and
--     hog_delete_file are keyed on (catalog_id, file_id) and carry no
--     uniqueness on `path` at all; CommitService says so explicitly
--     ("Duplicate paths against live/historical file rows stay legal —
--     this rejects only paths the cleanup queue currently owns"). Two
--     file rows over one path — the same parquet registered into two
--     tables, a writer retry that landed twice under a deterministic
--     path scheme, a data file and a puffin that collide — are legal
--     state today.
--   * ExpiryService queues by `DELETE ... RETURNING path` (steps 1 and
--     2). Two file rows sharing a path that fall under the floor in one
--     sweep produce TWO rows from ONE `INSERT ... SELECT`, in the same
--     statement; across sweeps they produce two undrained rows.
--   * None of the four inserts (ExpiryService x2, CompactionService's
--     staging ticket, UploadService's reclaim) carries `ON CONFLICT`.
--     A unique violation is therefore not a skipped row: it aborts the
--     enclosing transaction. For expiry that transaction is the whole
--     sweep, under the per-catalog commit lock, and it would fail
--     DETERMINISTICALLY on every retry — the floor stops advancing,
--     retention stops reclaiming, and the removal queue this index
--     exists to make cheap grows without bound. Trading a seq scan for
--     a wedged floor is not a trade.
--   * UploadService's `NOT EXISTS` is a guard under READ COMMITTED, not
--     a constraint: two concurrent reclaim sweeps can both see no row
--     and both insert.
--
-- Duplicates are also HARMLESS, which is why no writer defends against
-- them: the queue is a suggestion and never an authorization (invariant
-- 4), so the drain liveness-checks each row and the second of a pair
-- settles 'absent' after the first settles 'deleted'. The guard that
-- reads this index is `SELECT DISTINCT path`, and `NOT EXISTS` does not
-- count. `V16FileRemovalPathIndexMigrationIntegrationTest` pins both
-- halves: seed two data-file rows over one path, run a sweep, two
-- undrained rows appear, and a drain settles them 'deleted' then
-- 'absent'. Make this index UNIQUE and the first reds on the sweep's
-- own INSERT rather than on an assertion.
--
-- A PLAIN, BLOCKING BUILD — NOT `CONCURRENTLY`. This reverses the
-- file's first draft, which followed V14 by analogy; the analogy does
-- not survive measurement. Timed on PG 18.6 against this table at the
-- production row count (164,307 undrained rows, 176-byte paths):
--
--   * plain `CREATE INDEX` holds SHARE and blocks INSERTs for 1.37 s;
--   * `CREATE INDEX CONCURRENTLY`, entirely uncontended, takes 1.48 s —
--     it is not faster, it is two table passes instead of one;
--   * `CREATE INDEX CONCURRENTLY` while a SECOND replica is booting is
--     the failure. CIC's first phase waits out every transaction older
--     than itself, and the other replica is blocked inside
--     `pg_advisory_lock` in `Database.migrate` — a blocked lock wait is
--     an OPEN TRANSACTION with a live xmin, so CIC parks in
--     WaitForOlderSnapshots behind a transaction that is itself waiting
--     on the migration this CIC is part of. That is a CHAIN and not a
--     cycle, so the deadlock detector never fires; it unwinds only when
--     the waiting replica's 60 s `statement_timeout` kills it. On a
--     rolling deploy that is a crash loop, and every cancelled CIC
--     leaves an INVALID index behind.
--
-- So the trade is 1.37 s of blocked INSERTs, once, inside a 5 s
-- lock_timeout that fails fast and retryably — against a build that is
-- no faster uncontended and turns a rolling deploy into a crash loop.
-- V14's reasoning stands for V14 (a much larger table, and no second
-- migrating replica in the picture at the time); it does not transfer
-- here, and a comment that reasons by analogy instead of by measurement
-- is how the first draft of this file got it wrong.
--
-- BECAUSE THE BUILD IS BLOCKING, THIS FILE RUNS IN A TRANSACTION —
-- V13's shape, no `.conf` at all, so Flyway's default
-- `executeInTransaction=true` applies and `SET LOCAL` expires with the
-- transaction instead of needing a save-and-restore pair. That is not
-- only tidier, it is the difference between two failure modes: a
-- statement that fails here rolls the whole file back and writes NO
-- history row, so the next pod simply runs V16 again. A migration with
-- `executeInTransaction=false` cannot say that — a partial failure
-- leaves a `success = false` history row that fails Flyway's validate
-- on EVERY replica until an operator runs `flyway repair`, which is why
-- the idempotent `IF NOT EXISTS` statements in V14 and V15 are a
-- necessity rather than a courtesy. (V14 and V15 keep that behaviour:
-- both genuinely need it, V14 for its concurrent build and V15 for its
-- own reasons, and neither is touched here.)
--
-- Every heavy-lock statement below therefore sits inside one
-- `SET LOCAL lock_timeout` window (AGENT.md's migration rule, and
-- `MigrationLockWindowTest` reads this file off disk and enforces it).
SET LOCAL lock_timeout = '5s';

-- An INVALID remnant is cleared before the build. `CREATE INDEX IF NOT
-- EXISTS` matches on NAME alone: it would find an invalid index, skip,
-- and leave one that every INSERT maintains and no query may use.
-- Reachable two ways — an operator who pre-built this index with
-- `CREATE INDEX CONCURRENTLY` by hand and cancelled it, or a pod that
-- ran this file's first (CONCURRENTLY) draft and was killed mid-build.
-- Plain `DROP INDEX` takes ACCESS EXCLUSIVE on the TABLE, so it belongs
-- under the same 5 s bound as the build itself.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_class c
        JOIN pg_index i ON i.indexrelid = c.oid
        WHERE c.relname = 'hog_file_removal_undrained_path' AND NOT i.indisvalid
    ) THEN
        EXECUTE 'DROP INDEX hog_file_removal_undrained_path';
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS hog_file_removal_undrained_path
    ON hog_file_removal (catalog_id, path) WHERE drained_at IS NULL;
