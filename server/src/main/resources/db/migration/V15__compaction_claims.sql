-- Compaction group claims: the optimization that stops two maintenance
-- replicas from rewriting the SAME group and throwing one of the two
-- rewrites away at commit.
--
-- Measured on gigahog-prod-us (2026-09-24 00:00-00:15 UTC, catalog
-- millpond-prod-us, table main.events_raw): two maintenance replicas
-- plan the same candidate set from the same metadata, so they form the
-- same groups; a 547 s sweep committed 34 groups and LOST 30 to the
-- other replica's commits (`lost a plan-to-commit race`, counted
-- skipped_conflicts, the staged output reclaimed by the cleanup drain).
-- Half the object-store work of the fleet bought nothing.
--
-- A CLAIM IS AN OPTIMIZATION, NEVER AUTHORIZATION. Compaction's
-- correctness against a concurrent rewrite is, and stays, the
-- plan-to-commit re-verification under the per-catalog commit lock
-- (CompactionService.commitGroup re-reads every input's liveness and
-- exact DV identity). This table only lets the SECOND maintainer find
-- out before it spends the IO instead of after. Nothing here may be
-- read as permission to commit, and nothing here is on the correctness
-- path: drop the table's contents at any moment and the only
-- consequence is wasted work.
--
-- KEYED BY THE INPUT FILE SET, per group, per table. `group_key` is a
-- hex SHA-256 over the group's spec id, its partition values and its
-- sorted input data_file_ids — the identity of the work, not of the
-- worker, so two replicas that plan the same group compute the same key
-- with no coordination. `input_file_ids` is carried alongside because
-- the planner's skip is by OVERLAP, not by key equality: a replica
-- whose candidate set differs by one arriving file packs a group with a
-- different key but the same files, and skipping only exact matches
-- would leave exactly the production race above in place.
--
-- EXPIRY is what makes a dead maintainer's claim reclaimable. There is
-- no liveness protocol and deliberately none: a claim is a lease with a
-- wall-clock end (HOGLAKE_COMPACTION_CLAIM_TTL_SECONDS, default 900),
-- reclaimable by the ON CONFLICT ... WHERE expires_at <= now() arm of
-- the claim insert, and purged in bulk at the head of each sweep. A
-- pod that is killed mid-rewrite therefore costs one TTL of delay on
-- those files and nothing else.
--
-- LOCK BOUND FIRST. CREATE TABLE is not itself a heavy lock on an
-- existing table, but its REFERENCES hog_catalog takes SHARE ROW
-- EXCLUSIVE on hog_catalog, which conflicts with every concurrent
-- catalog write; Database.migrate holds a pg_advisory_lock across the
-- whole Flyway run, so an unbounded wait here queues every other
-- booting pod behind this one. The save-and-restore is V9's, V10's and
-- V14's, and the 5s value is theirs. Every statement below is
-- idempotent (IF NOT EXISTS) because this file runs with
-- executeInTransaction=false (its .conf, V14's shape): a failure part
-- way through must be re-runnable rather than needing a Flyway repair.
SELECT set_config('hoglake.migration_lock_timeout', current_setting('lock_timeout'), false);
SET lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS hog_compaction_claim (
    catalog_id bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    table_id bigint NOT NULL,
    -- Hex SHA-256 over (spec_id, partition values, sorted input file ids).
    group_key text NOT NULL,
    -- The claimed group's inputs, so the planner can skip by OVERLAP.
    input_file_ids bigint[] NOT NULL,
    -- The claiming process (CompactionService's per-instance id), so a
    -- release can only delete its OWN claim and never a reclaimer's.
    claimant uuid NOT NULL,
    claimed_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (catalog_id, table_id, group_key)
);

-- ONE index, and only because the primary key cannot serve this one.
--
-- The planner's read (CompactionClaimRepo.LIVE_CLAIMS_SQL) asks for one
-- table's live claims, and `(catalog_id, table_id)` is a prefix of the
-- PRIMARY KEY — so the PK index already answers it with an index scan
-- and `expires_at` as a filter, which is what the migration test's
-- EXPLAIN shows. A second index over (catalog_id, table_id,
-- expires_at) was written first and deleted after that EXPLAIN: the
-- planner never chose it, and an index nothing chooses is pure write
-- cost on the hottest statement this table has (one INSERT per group
-- attempt).
--
-- The PURGE is the query the PK cannot serve. It names a catalog and no
-- table, so the PK's second column is a gap and the scan would be the
-- whole catalog's claims; this index makes it the expired ones.
CREATE INDEX IF NOT EXISTS hog_compaction_claim_expiry
    ON hog_compaction_claim (catalog_id, expires_at);

SELECT set_config('lock_timeout', current_setting('hoglake.migration_lock_timeout'), false);
