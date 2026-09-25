-- FROZEN at v1.0.0 (2026-09-11, the Gigahog deploy). This file is
-- append-only history from here: do NOT edit it. Schema changes go in
-- new V<n>__ migrations; schema.sql tracks the fold of the chain
-- (SchemaEquivalenceIntegrationTest enforces equivalence).
-- Canonical hoglake schema: the complete desired state (README.md,
-- schema/migrations mechanism). CI asserts fold(migrations) == this file.

--
-- Design positions this encodes (see hoglake/README.md):
--  * Real integrity: PKs, FKs with ON DELETE CASCADE, NOT NULL where the
--    code assumes it, partial indexes for the hot predicates.
--  * Versioned-row pattern where time travel needs it: a row is visible
--    at snapshot S iff begin_snapshot <= S AND (end_snapshot IS NULL OR
--    S < end_snapshot).
--  * Identity vs. version split: hog_table is the immutable identity
--    (table_id, table_uuid); hog_table_version carries the mutable,
--    versioned bits (name, namespace).
--  * Id allocation: per-catalog counters on hog_catalog / hog_table,
--    advanced inside the serialized commit tail (advisory xact lock), so
--    ids are dense and ordered with commit order.
--  * Typed conflict vocabulary: hog_snapshot_change replaces DuckLake's
--    comma-encoded changes_made string.
--  * Row-level deletes are deletion vectors (one live DV per data file);
--    no inlined data, macros, or tags — by design, not omission.

CREATE TABLE hog_catalog (
    catalog_id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name             text NOT NULL UNIQUE CHECK (name ~ '^[a-z0-9][a-z0-9_-]{0,62}$'),
    data_path        text NOT NULL,
    -- Allocators, advanced only inside the commit tail / DDL txn. This
    -- row is UPDATEd on every commit; keep every allocator column
    -- UNINDEXED so those updates stay HOT (heap-only, one page, no
    -- index churn). Indexing any column below turns the hottest row in
    -- the catalog into a vacuum problem.
    last_snapshot_id bigint NOT NULL DEFAULT 0,
    schema_version   bigint NOT NULL DEFAULT 0,
    next_table_id    bigint NOT NULL DEFAULT 1,
    next_file_id     bigint NOT NULL DEFAULT 1,
    next_namespace_id bigint NOT NULL DEFAULT 1,
    snapshot_retention_seconds bigint
        CHECK (snapshot_retention_seconds IS NULL OR snapshot_retention_seconds > 0),
    consumer_floor   boolean NOT NULL DEFAULT true,
    earliest_snapshot_id bigint NOT NULL DEFAULT 0,
    -- snapshot_time of the snapshot earliest_snapshot_id points at,
    -- captured by the expiry sweep in the same UPDATE that advances the
    -- floor. Survives the snapshot rows below the floor (their times die
    -- with them), so a 410 can say WHEN the floor was reached. NULL =
    -- expiry has never advanced the floor.
    earliest_snapshot_time timestamptz,
    next_view_id     bigint NOT NULL DEFAULT 1,
    created_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE hog_snapshot (
    catalog_id     bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    snapshot_id    bigint NOT NULL,
    snapshot_time  timestamptz NOT NULL DEFAULT now(),
    -- Diagnostic only: the catalog's schema_version at commit time.
    -- No read path resolves schemas through this — time travel reads
    -- versioned hog_column/hog_table_version rows at the snapshot.
    schema_version bigint NOT NULL,
    author         text,
    commit_message text,
    PRIMARY KEY (catalog_id, snapshot_id)
);

-- Typed change vocabulary. kind is closed: conflict detection joins on it.
CREATE TABLE hog_snapshot_change (
    catalog_id  bigint NOT NULL,
    snapshot_id bigint NOT NULL,
    kind        text   NOT NULL CHECK (kind IN (
                    'namespace_created', 'namespace_dropped',
                    'table_created', 'table_dropped', 'table_altered',
                    'table_inserted_into', 'table_deleted_from',
                    'table_compacted',
                    'view_created', 'view_dropped')),
    -- table_id, namespace_id, or view_id. Every kind in the vocabulary
    -- names an object; a NULL here would be a conflict row the conflict
    -- index silently never matches (NULL never equals) — an invisible
    -- OCC bypass — so the DB refuses it outright.
    object_id   bigint NOT NULL,
    FOREIGN KEY (catalog_id, snapshot_id)
        REFERENCES hog_snapshot ON DELETE CASCADE
);
-- The conflict check: changes of these kinds against this object since my
-- read snapshot. One indexed anti-join, never a string parse.
CREATE INDEX hog_snapshot_change_conflict
    ON hog_snapshot_change (catalog_id, object_id, kind, snapshot_id);
CREATE INDEX hog_snapshot_change_by_snapshot
    ON hog_snapshot_change (catalog_id, snapshot_id);

-- Namespaces are not snapshot-versioned in v1: rename is disallowed,
-- drop requires emptiness (live-table check in the service).
CREATE TABLE hog_namespace (
    catalog_id   bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    namespace_id bigint NOT NULL,
    -- Identifier policy (shared with table/view/column names; enforced
    -- service-side too): letter/underscore start, then [A-Za-z0-9_-],
    -- max 128 chars.
    name         text NOT NULL CHECK (name ~ '^[A-Za-z_][A-Za-z0-9_-]{0,127}$'),
    -- Soft-drop flag, reserved for a future namespace-drop endpoint
    -- (with the namespace_dropped change kind). Nothing writes it yet.
    -- Hard DELETE of a namespace row with live children is blocked by
    -- the deferred FKs on hog_table_version and hog_view — versioned
    -- history must never lose its namespace. The FKs are DEFERRABLE
    -- INITIALLY DEFERRED because the graph is a diamond: a
    -- whole-catalog delete cascades into hog_namespace and (via
    -- hog_table) into hog_table_version as separate internal
    -- statements with unspecified order, so an immediate check can
    -- fire before the other branch empties the children (verified
    -- empirically). The commit-time check sees the settled state.
    dropped      boolean NOT NULL DEFAULT false,
    created_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (catalog_id, namespace_id)
);
CREATE UNIQUE INDEX hog_namespace_live_name
    ON hog_namespace (catalog_id, name) WHERE NOT dropped;

-- Immutable table identity. table_uuid is the contract consumers key
-- cursors to: it survives rename, changes on drop+recreate.
CREATE TABLE hog_table (
    catalog_id    bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    table_id      bigint NOT NULL,
    table_uuid    uuid   NOT NULL DEFAULT gen_random_uuid(),
    created_snapshot bigint NOT NULL,
    dropped_snapshot bigint,
    next_field_id bigint NOT NULL DEFAULT 1,
    -- The replacement edge (V14): the incarnation THIS row replaced, set
    -- by CatalogService.createTable when it publishes an atomic
    -- replacement. Recorded rather than derived from
    -- `dropped_snapshot = created_snapshot`, because that convention is
    -- nothing the schema enforces and the cost of being wrong about it
    -- is deleting a consumer's position on a table it still reads
    -- (OffsetRepo.releaseSupersededOffsets). Deliberately not a FK: the
    -- referenced row is in this same table and is retired, never deleted,
    -- and a cascade is exactly the behaviour we do not want.
    replaced_table_id bigint,
    PRIMARY KEY (catalog_id, table_id),
    UNIQUE (catalog_id, table_uuid),
    -- The edge has no FK, so this is its only structural defence: a
    -- self-edge would be a 1-cycle for the recursive walk that follows it.
    CONSTRAINT hog_table_no_self_replacement CHECK (replaced_table_id <> table_id)
);

-- Versioned mutable bits of a table (name, namespace).
CREATE TABLE hog_table_version (
    catalog_id     bigint NOT NULL,
    table_id       bigint NOT NULL,
    begin_snapshot bigint NOT NULL,
    end_snapshot   bigint,
    namespace_id   bigint NOT NULL,
    name           text   NOT NULL CHECK (name ~ '^[A-Za-z_][A-Za-z0-9_-]{0,127}$'),
    comment        text,
    properties     jsonb NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (catalog_id, table_id, begin_snapshot),
    FOREIGN KEY (catalog_id, table_id) REFERENCES hog_table ON DELETE CASCADE,
    -- Deferred no-action (not CASCADE): a namespace delete must never
    -- hole the versioned history under it; deferred so a whole-catalog
    -- cascade still passes. See the hog_namespace.dropped comment.
    FOREIGN KEY (catalog_id, namespace_id) REFERENCES hog_namespace
        DEFERRABLE INITIALLY DEFERRED,
    CHECK (end_snapshot IS NULL OR end_snapshot > begin_snapshot)
);
CREATE UNIQUE INDEX hog_table_version_live_name
    ON hog_table_version (catalog_id, namespace_id, name)
    WHERE end_snapshot IS NULL;
-- Non-partial: the deferred namespace FK's RI trigger cannot use the
-- partial live-name index; without this, a namespace or catalog delete
-- scans every history row per namespace.
CREATE INDEX hog_table_version_namespace
    ON hog_table_version (catalog_id, namespace_id);
CREATE INDEX hog_table_version_live
    ON hog_table_version (catalog_id, table_id) WHERE end_snapshot IS NULL;

-- Versioned column definitions. field_id is the stable identity embedded
-- in parquet files (PARQUET:field_id) — see iceberg-federation.md.
CREATE TABLE hog_column (
    catalog_id     bigint NOT NULL,
    table_id       bigint NOT NULL,
    field_id       bigint NOT NULL,
    begin_snapshot bigint NOT NULL,
    end_snapshot   bigint,
    name           text   NOT NULL CHECK (name ~ '^[A-Za-z_][A-Za-z0-9_-]{0,127}$'),
    -- Closed type set: every member has a defined Iceberg mapping
    -- (iceberg-federation.md §2). Extend by migration, never ad hoc.
    -- Member ORDER is load-bearing: V4__scalar_types.sql and
    -- V9__nested_types.sql recreate this constraint and the
    -- schema-equivalence gate compares the normalized
    -- pg_get_constraintdef text, which preserves the order.
    -- list/struct/map are CONTAINERS: they carry no values, they have
    -- child rows (parent_field_id below), and they never carry stats.
    col_type       text   NOT NULL CHECK (col_type IN (
                       'boolean', 'int8', 'int16', 'int', 'long', 'uint8', 'uint16',
                       'uint32', 'uint64', 'float', 'double', 'decimal', 'date', 'time',
                       'timestamp_s', 'timestamp_ms', 'timestamp', 'timestamp_ns',
                       'timestamptz', 'string', 'json', 'uuid', 'binary', 'variant',
                       'list', 'struct', 'map')),
    type_params    jsonb,          -- e.g. {"precision":38,"scale":9} for decimal
    nullable       boolean NOT NULL DEFAULT true,
    -- Orders SIBLINGS: 0-based within the parent (top-level columns
    -- share the NULL parent).
    ordinal        int    NOT NULL CHECK (ordinal >= 0),
    -- The tree edge (V9): NULL = top-level column, else the field_id of
    -- the containing list/struct/map. A same-table reference by
    -- (catalog_id, table_id, field_id) IDENTITY and deliberately NOT a
    -- foreign key: these rows are versioned (begin_snapshot is in the
    -- PK), so an FK would have to name one VERSION of the parent and
    -- would break the moment the parent is renamed or promoted. Field
    -- ids are stable across versions; versions are not.
    parent_field_id bigint,
    comment        text,
    PRIMARY KEY (catalog_id, table_id, field_id, begin_snapshot),
    FOREIGN KEY (catalog_id, table_id) REFERENCES hog_table ON DELETE CASCADE,
    CHECK (end_snapshot IS NULL OR end_snapshot > begin_snapshot),
    -- The one cycle a single row can express on its own. Deeper cycles
    -- are impossible by construction (ids are allocated strictly
    -- increasing, parents before children).
    CONSTRAINT hog_column_parent_not_self
        CHECK (parent_field_id IS NULL OR parent_field_id <> field_id),
    -- Ids are allocated depth-first, parents before children, so a
    -- parent's id is always BELOW its children's. That ordering is what
    -- makes deeper cycles impossible by construction; as a row-local
    -- CHECK it costs nothing and it subsumes the self-reference above,
    -- which keeps its own name because its message is the one a
    -- confused client needs.
    CONSTRAINT hog_column_parent_precedes_child
        CHECK (parent_field_id IS NULL OR parent_field_id < field_id)
);
CREATE INDEX hog_column_live
    ON hog_column (catalog_id, table_id) WHERE end_snapshot IS NULL;
-- Writers stamp parquet field order from ordinals: a duplicate live
-- ordinal is a silent corruption vector, so the DB refuses it.
-- Per-PARENT since V9 — ordinals order siblings, so two struct fields in
-- different structs are both legitimately ordinal 0. NULLS NOT DISTINCT
-- keeps the guarantee for top-level columns, whose parent_field_id is
-- NULL: without it Postgres treats every NULL as distinct and the
-- duplicate-ordinal vector comes back for exactly the rows this index
-- used to cover.
-- IMMEDIATE and write-order-sensitive: a single-statement swap of two
-- live ordinals violates it mid-update (verified). Every alter op must
-- end-snapshot old rows before inserting new ones (AlterService does);
-- a future reorder-columns op must keep that shape or this index must
-- become a deferrable constraint.
CREATE UNIQUE INDEX hog_column_live_ordinal
    ON hog_column (catalog_id, table_id, parent_field_id, ordinal) NULLS NOT DISTINCT
    WHERE end_snapshot IS NULL;

-- Table-level rollup + the row-id allocator. One row per table, created
-- with the table.
CREATE TABLE hog_table_stats (
    catalog_id      bigint NOT NULL,
    table_id        bigint NOT NULL,
    record_count    bigint NOT NULL DEFAULT 0,
    -- CHECK backstops the commit tail's overflow-checked byte rollup: a
    -- wrapped (negative) sum must fail loudly, never land as drift.
    file_size_bytes bigint NOT NULL DEFAULT 0 CHECK (file_size_bytes >= 0),
    -- CHECK is the DB backstop against row-id allocator overflow: a
    -- wrapped (negative) allocator would silently break the lineage
    -- guarantee. CommitService rejects overflowing sums before this.
    next_row_id     bigint NOT NULL DEFAULT 0 CHECK (next_row_id >= 0),
    PRIMARY KEY (catalog_id, table_id),
    FOREIGN KEY (catalog_id, table_id) REFERENCES hog_table ON DELETE CASCADE
);

-- The file manifest. Deletes never end-snapshot a data file (DVs mask
-- rows); end_snapshot comes from drop, compaction, or expiry.
CREATE TABLE hog_data_file (
    catalog_id      bigint NOT NULL,
    data_file_id    bigint NOT NULL,
    table_id        bigint NOT NULL,
    begin_snapshot  bigint NOT NULL,
    end_snapshot    bigint,
    path            text   NOT NULL,   -- absolute object-store URI; no relative chains
    file_format     text   NOT NULL DEFAULT 'parquet'
                    CHECK (file_format IN ('parquet')),
    record_count    bigint NOT NULL CHECK (record_count >= 0),
    file_size_bytes bigint NOT NULL CHECK (file_size_bytes >= 0),
    footer_size     bigint,
    row_id_start    bigint NOT NULL,   -- lineage guarantee: always assigned at commit
    -- Deferred-stats mode: 'pending' files are never pruned; the hydrator
    -- flips them to 'provided' or 'failed'.
    stats_state     text   NOT NULL DEFAULT 'provided'
                    CHECK (stats_state IN ('provided', 'pending', 'failed')),
    spec_id         bigint,
    -- Compaction outputs merge inputs whose row-id ranges need not be
    -- contiguous, so their row ids cannot be positional: the file carries
    -- an explicit physical int64 column `_hog_row_id` (reserved parquet
    -- field id 2147483646) holding each row's id, and this flag tells
    -- readers so. When true, row_id_start is only min(input row ids) —
    -- its positional meaning (row_id_start + offset) is void.
    explicit_row_ids boolean NOT NULL DEFAULT false,
    -- Field-id contract flag, set by the hydrator's footer read: true
    -- when any leaf of the file's parquet schema lacks a PARQUET:field_id
    -- (such files bind columns by NAME; renaming a column would silently
    -- NULL their history in readers, so AlterService refuses renames
    -- while a live flagged file exists). The reserved `_hog_row_id` id
    -- 2147483646 counts as an id like any other.
    missing_field_ids boolean NOT NULL DEFAULT false,
    -- Row-group start offsets (V18), ascending, one per row group: where
    -- each row group's FIRST column chunk starts (parquet-java
    -- ColumnChunkMetaData.getStartingPos — the dictionary page offset
    -- when a dictionary page precedes the first data page, else the
    -- first data page offset; never RowGroup.file_offset). Served on
    -- GET /scan with include=split_offsets so engines cut byte-range
    -- splits on row-group boundaries. NULL = unknown (cut evenly).
    -- Filled by a footer-shipping registration that carried it, by the
    -- hydrator for pending files, and by compaction for its outputs;
    -- the contract (non-empty, strictly increasing, within
    -- [0, file_size_bytes), at most 100,000 entries) is enforced in
    -- code by model/SplitOffsets.kt, not by a CHECK.
    split_offsets   bigint[],
    PRIMARY KEY (catalog_id, data_file_id),
    FOREIGN KEY (catalog_id, table_id) REFERENCES hog_table ON DELETE CASCADE,
    CHECK (end_snapshot IS NULL OR end_snapshot > begin_snapshot)
);
CREATE INDEX hog_data_file_live
    ON hog_data_file (catalog_id, table_id, begin_snapshot)
    WHERE end_snapshot IS NULL;
CREATE INDEX hog_data_file_pending
    ON hog_data_file (catalog_id, data_file_id)
    WHERE stats_state = 'pending';
-- V19: ExpiryService's data-file DELETE (`end_snapshot IS NOT NULL AND
-- end_snapshot <= floor`), which was a sequential scan of the whole
-- manifest inside the sweep transaction, under the per-catalog commit
-- lock. PARTIAL on the complement of `hog_data_file_live`: an appended
-- row has end_snapshot NULL and never enters this index, so the
-- hottest write in the system pays nothing for it, and what it holds
-- is only the rows expiry is looking for (~10 bytes each after
-- deduplication). No matching index on hog_delete_file: expiry's DV arm
-- is an OR with a correlated EXISTS and the planner never chooses one
-- (V19's header has the measurement and the ticket).
CREATE INDEX hog_data_file_ended
    ON hog_data_file (catalog_id, end_snapshot)
    WHERE end_snapshot IS NOT NULL;

-- Per-file, per-column zone maps. Bounds are stored in Iceberg
-- single-value binary serialization (opaque to Postgres) so manifest
-- generation for the facade is a mechanical re-encode.
CREATE TABLE hog_file_column_stats (
    catalog_id   bigint NOT NULL,
    data_file_id bigint NOT NULL,
    field_id     bigint NOT NULL,
    value_count  bigint NOT NULL CHECK (value_count >= 0),
    null_count   bigint NOT NULL CHECK (null_count >= 0),
    nan_count    bigint,
    size_bytes   bigint,
    lower_bound  bytea,
    upper_bound  bytea,
    PRIMARY KEY (catalog_id, data_file_id, field_id),
    FOREIGN KEY (catalog_id, data_file_id)
        REFERENCES hog_data_file ON DELETE CASCADE
);

-- Log primitives: per-consumer committed offsets, keyed by table_uuid so
-- an incarnation change is visible to the consumer, not deduced.
CREATE TABLE hog_consumer_offset (
    catalog_id         bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    consumer_id        text   NOT NULL CHECK (length(consumer_id) BETWEEN 1 AND 128),
    table_uuid         uuid   NOT NULL,
    committed_snapshot bigint NOT NULL,
    updated_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (catalog_id, consumer_id, table_uuid)
);
CREATE INDEX hog_consumer_offset_retention_floor
    ON hog_consumer_offset (catalog_id, committed_snapshot);

-- ---- Partitioning ----
-- Versioned spec header + ordered fields. Transforms are the
-- Iceberg-semantics set only (iceberg-federation.md §3).
CREATE TABLE hog_partition_spec (
    catalog_id     bigint NOT NULL,
    table_id       bigint NOT NULL,
    spec_id        bigint NOT NULL,
    begin_snapshot bigint NOT NULL,
    end_snapshot   bigint,
    PRIMARY KEY (catalog_id, table_id, spec_id),
    FOREIGN KEY (catalog_id, table_id) REFERENCES hog_table ON DELETE CASCADE,
    CHECK (end_snapshot IS NULL OR end_snapshot > begin_snapshot)
);
CREATE INDEX hog_partition_spec_live
    ON hog_partition_spec (catalog_id, table_id) WHERE end_snapshot IS NULL;

CREATE TABLE hog_partition_field (
    catalog_id      bigint NOT NULL,
    table_id        bigint NOT NULL,
    spec_id         bigint NOT NULL,
    key_index       int    NOT NULL CHECK (key_index >= 0),
    source_field_id bigint NOT NULL,
    transform       text   NOT NULL CHECK (transform IN
                        ('identity', 'bucket', 'year', 'month', 'day', 'hour')),
    transform_param int    CHECK ((transform = 'bucket') = (transform_param IS NOT NULL)),
    PRIMARY KEY (catalog_id, table_id, spec_id, key_index),
    FOREIGN KEY (catalog_id, table_id, spec_id)
        REFERENCES hog_partition_spec ON DELETE CASCADE
);

-- Files bind to the spec they were written under; values are the
-- TRANSFORMED partition values, string-encoded, one per key_index.
-- TRUST BOUNDARY: value CORRECTNESS is writer-trusted — the server
-- never computes transforms (footer-shipping), so it validates
-- structure (spec/key_index shape) but cannot verify that a value is
-- the true transform of the file's rows (e.g. a bucket value in
-- [0, n)). A wrong value mis-prunes reads of that file. This is the
-- one waiver of the server-validates-structure rule; everything else
-- in the schema is CHECK/FK-enforced.

CREATE TABLE hog_file_partition_value (
    catalog_id   bigint NOT NULL,
    data_file_id bigint NOT NULL,
    key_index    int    NOT NULL,
    value        text,   -- NULL = null partition value
    PRIMARY KEY (catalog_id, data_file_id, key_index),
    FOREIGN KEY (catalog_id, data_file_id)
        REFERENCES hog_data_file ON DELETE CASCADE
);

-- ---- Sort orders ----
-- Versioned sort spec, mirroring the partition-spec pair: header +
-- ordered fields. ADVISORY for writers (the server never verifies file
-- sortedness) and BINDING for compaction rewrites, which sort merged
-- rows by the live spec — safe only because compaction outputs carry
-- explicit row ids (hog_data_file.explicit_row_ids above).
CREATE TABLE hog_sort_spec (
    catalog_id     bigint NOT NULL,
    table_id       bigint NOT NULL,
    sort_id        bigint NOT NULL,
    begin_snapshot bigint NOT NULL,
    end_snapshot   bigint,
    PRIMARY KEY (catalog_id, table_id, sort_id),
    FOREIGN KEY (catalog_id, table_id) REFERENCES hog_table ON DELETE CASCADE,
    CHECK (end_snapshot IS NULL OR end_snapshot > begin_snapshot)
);
CREATE INDEX hog_sort_spec_live
    ON hog_sort_spec (catalog_id, table_id) WHERE end_snapshot IS NULL;

CREATE TABLE hog_sort_field (
    catalog_id      bigint NOT NULL,
    table_id        bigint NOT NULL,
    sort_id         bigint NOT NULL,
    key_index       int    NOT NULL CHECK (key_index >= 0),
    source_field_id bigint NOT NULL,
    direction       text   NOT NULL CHECK (direction IN ('asc', 'desc')),
    null_order      text   NOT NULL CHECK (null_order IN ('nulls_first', 'nulls_last')),
    PRIMARY KEY (catalog_id, table_id, sort_id, key_index),
    FOREIGN KEY (catalog_id, table_id, sort_id)
        REFERENCES hog_sort_spec ON DELETE CASCADE
);

-- ---- Row-level deletes: deletion vectors ----
-- One DV file per data file per range; a new DV supersedes the old
-- (end_snapshot) and must cover it (delete_count monotonic — DVs only
-- grow). Registration is metadata-only (footer-shipping philosophy);
-- the ONE server-side reader of DV content is the compaction rewrite,
-- which applies a group's live DVs (iceberg puffin `deletion-vector-v1`
-- blobs) and end-snapshots them with their data files.
CREATE TABLE hog_delete_file (
    catalog_id      bigint NOT NULL,
    delete_file_id  bigint NOT NULL,
    table_id        bigint NOT NULL,
    data_file_id    bigint NOT NULL,
    begin_snapshot  bigint NOT NULL,
    end_snapshot    bigint,
    path            text   NOT NULL,
    file_format     text   NOT NULL DEFAULT 'puffin-dv'
                    CHECK (file_format IN ('puffin-dv')),
    delete_count    bigint NOT NULL CHECK (delete_count > 0),
    file_size_bytes bigint NOT NULL CHECK (file_size_bytes >= 0),
    PRIMARY KEY (catalog_id, delete_file_id),
    FOREIGN KEY (catalog_id, table_id) REFERENCES hog_table ON DELETE CASCADE,
    FOREIGN KEY (catalog_id, data_file_id)
        REFERENCES hog_data_file ON DELETE CASCADE,
    CHECK (end_snapshot IS NULL OR end_snapshot > begin_snapshot)
);
CREATE UNIQUE INDEX hog_delete_file_one_live_per_data_file
    ON hog_delete_file (catalog_id, data_file_id) WHERE end_snapshot IS NULL;
CREATE INDEX hog_delete_file_live
    ON hog_delete_file (catalog_id, table_id) WHERE end_snapshot IS NULL;

-- Versioned views: SQL text stored with its dialect. Single versioned
-- table (no identity split: views carry no lineage/state that must
-- survive re-creation).
CREATE TABLE hog_view (
    catalog_id     bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    view_id        bigint NOT NULL,
    -- Deliberate asymmetry with table_uuid: views have no
    -- identity/version split, so drop+recreate mints a new view_uuid
    -- with no history row observing the change. Consumers must not
    -- cursor on view_uuid; only table_uuid carries the incarnation
    -- contract.
    view_uuid      uuid   NOT NULL DEFAULT gen_random_uuid(),
    namespace_id   bigint NOT NULL,
    name           text   NOT NULL CHECK (name ~ '^[A-Za-z_][A-Za-z0-9_-]{0,127}$'),
    dialect        text   NOT NULL DEFAULT 'trino',
    sql            text   NOT NULL,
    begin_snapshot bigint NOT NULL,
    end_snapshot   bigint,
    PRIMARY KEY (catalog_id, view_id),
    -- Deferred for the same reason as hog_table_version's namespace FK.
    FOREIGN KEY (catalog_id, namespace_id)
        REFERENCES hog_namespace DEFERRABLE INITIALLY DEFERRED,
    CHECK (end_snapshot IS NULL OR end_snapshot > begin_snapshot)
);
CREATE UNIQUE INDEX hog_view_live_name
    ON hog_view (catalog_id, namespace_id, name) WHERE end_snapshot IS NULL;
-- Same RI-trigger rationale as hog_table_version_namespace.
CREATE INDEX hog_view_namespace
    ON hog_view (catalog_id, namespace_id);

-- The file-removal queue AND ledger. Physical deletion is decoupled
-- from metadata deletion, batched, and ALWAYS liveness-checked at drain
-- time — a queue entry is a suggestion, never an authorization
-- (README.md §8). Draining SOFT-deletes: a completed entry keeps its
-- row (drained_at + drained_outcome) so "what did cleanup touch, when,
-- after how many attempts" stays queryable; the cleanup sweep purges
-- drained rows past a retention window so the ledger itself never
-- accumulates without bound.
CREATE TABLE hog_file_removal (
    removal_id   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    catalog_id   bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    path         text   NOT NULL,
    file_kind    text   NOT NULL CHECK (file_kind IN ('data', 'delete')),
    -- 'compaction_staging': a claim ticket the compactor inserts for its
    -- OUTPUT path before uploading — if the group's commit never lands
    -- (crash, plan-to-commit race), the normal cleanup drain reclaims the
    -- orphaned object; a successful group commit settles the row as
    -- 'registered' in the same transaction that makes the path live.
    reason       text   NOT NULL CHECK (reason IN ('snapshot_expiry', 'table_drop_gc',
                                                   'compaction_staging', 'trino_upload')),
    scheduled_at timestamptz NOT NULL DEFAULT now(),
    -- Drain bookkeeping: attempts counts every touch that did NOT drain
    -- the row (still-referenced skips, transient S3 failures).
    attempts        int NOT NULL DEFAULT 0,
    last_attempt_at timestamptz,
    -- Soft-delete: non-null once the entry is settled. 'deleted' = the
    -- object was physically removed; 'absent' = it was verified already
    -- gone; 'registered' = a compaction_staging claim whose group commit
    -- landed (the path became a live catalog file instead of garbage).
    -- Outcome and timestamp travel together.
    drained_at      timestamptz,
    drained_outcome text CHECK (drained_outcome IN ('deleted', 'absent', 'registered')),
    CHECK ((drained_at IS NULL) = (drained_outcome IS NULL))
);
CREATE INDEX hog_file_removal_drain
    ON hog_file_removal (catalog_id, removal_id)
    WHERE drained_at IS NULL;
-- The commit path's access path (V16). The path-reuse guard
-- (CommitService.REMOVAL_QUEUE_COLLISION_SQL) and UploadService's
-- reclaim `NOT EXISTS` both ask (catalog_id, path) over undrained rows,
-- which hog_file_removal_drain cannot serve: `removal_id` is a gap in
-- that predicate. Without this index every commit reads the catalog's
-- whole queue, so a commit costs what cleanup is behind on (#199).
-- NOT UNIQUE on purpose: no file path is unique in this schema, so
-- expiry can legitimately queue one path twice, and with no writer
-- carrying `ON CONFLICT` a unique violation would abort the sweep that
-- advances the retention floor. See the V16 migration for the full
-- argument.
CREATE INDEX hog_file_removal_undrained_path
    ON hog_file_removal (catalog_id, path)
    WHERE drained_at IS NULL;
-- The maintenance run ledger: one row per maintenance-task run, whether
-- the BackgroundLoops sweep or the manual /maintenance/* trigger drove
-- it. The queryable answer to "is the loop alive, when did it last run,
-- and what did it do" — the same forensics-ledger position as
-- hog_file_removal's drained rows. Recording never rides the sweep's
-- transaction: the row is inserted after the run resolves, and a
-- recording failure never fails the run.
--
-- Hydrator sweeps are instance-wide, so they record one row per catalog
-- they claimed files for (a no-claim sweep writes nothing — a catalog's
-- waiting hydrator work is the stats_state backlog, not a run). The
-- cleanup sweep purges rows past HOGLAKE_MAINTENANCE_LEDGER_RETENTION_SECONDS
-- (default 7d): at the default intervals the ledger sees ~2-3 rows per
-- minute per catalog, so a week of history stays in the tens of KB.
CREATE TABLE hog_maintenance_run (
    run_id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    catalog_id  bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    task        text   NOT NULL CHECK (task IN ('hydrator', 'expiry', 'cleanup',
                                                'compaction', 'verify')),
    -- 'loop' = a BackgroundLoops sweep; 'manual' = a /maintenance/*
    -- trigger (for task 'hydrator', manual rows are rehydrate calls).
    run_trigger text   NOT NULL CHECK (run_trigger IN ('loop', 'manual')),
    started_at  timestamptz NOT NULL,
    finished_at timestamptz NOT NULL,
    -- 'failed' = the run itself threw (error carries the detail).
    -- Per-FILE hydration failures are not run failures: they live in the
    -- result counts and the stats_state backlog.
    status      text   NOT NULL CHECK (status IN ('ok', 'failed')),
    error       text,
    -- The task's result payload, serialized exactly as the matching POST
    -- /maintenance/* response body (expiry -> ExpiryResult, cleanup ->
    -- CleanupResult, compaction -> CompactionResult, verify ->
    -- VerifyReport; hydrator loop rows carry per-catalog sweep counts,
    -- manual hydrator rows carry RehydrateResult).
    result      jsonb,
    CHECK (finished_at >= started_at),
    CHECK ((status = 'failed') = (error IS NOT NULL))
);
-- Serves last-run-per-task (DISTINCT ON) and the newest-first history
-- page (run_id < cursor) with one index.
CREATE INDEX hog_maintenance_run_recent
    ON hog_maintenance_run (catalog_id, task, run_id DESC);
CREATE INDEX hog_maintenance_run_catalog_history
    ON hog_maintenance_run (catalog_id, run_id DESC);
CREATE INDEX hog_maintenance_run_task_history
    ON hog_maintenance_run (task, run_id DESC);

CREATE TABLE hog_maintenance_summary (
    catalog_id bigint PRIMARY KEY REFERENCES hog_catalog ON DELETE CASCADE,
    generation bigint NOT NULL DEFAULT 0,
    published_generation bigint NOT NULL DEFAULT 0,
    sampled_at timestamptz,
    sample jsonb,
    scan_state jsonb,
    next_batch_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((sampled_at IS NULL) = (sample IS NULL))
);
CREATE INDEX hog_maintenance_summary_due
    ON hog_maintenance_summary (next_batch_at, catalog_id);
CREATE TABLE hog_maintenance_summary_tier (
    catalog_id bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    generation bigint NOT NULL,
    bucket_key text NOT NULL,
    table_id bigint NOT NULL,
    spec_id bigint,
    partition_values text[],
    quota bigint NOT NULL CHECK (quota >= 0),
    remaining bigint NOT NULL CHECK (remaining > 0),
    pending integer NOT NULL CHECK (pending >= 0),
    selected bigint NOT NULL DEFAULT 0,
    -- Largest file in the partial group carried across scan pages: the
    -- scan walks a bucket smallest-first, so a group's largest file is
    -- its last, and the group minimum scales with it.
    pending_max_bytes bigint NOT NULL DEFAULT 0,
    file_count bigint NOT NULL DEFAULT 0,
    small_count bigint NOT NULL DEFAULT 0,
    total_bytes bigint NOT NULL DEFAULT 0,
    small_bytes bigint NOT NULL DEFAULT 0,
    dv_count bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (catalog_id, generation, bucket_key)
);
CREATE INDEX hog_data_file_maintenance_scan
    ON hog_data_file (catalog_id, table_id, row_id_start, data_file_id);
-- The maintenance summary scan walks each bucket smallest-file-first,
-- because that is the order compaction bin-packs in.
CREATE INDEX hog_data_file_maintenance_size_scan
    ON hog_data_file (catalog_id, table_id, file_size_bytes, data_file_id);
CREATE INDEX hog_delete_file_data_lookup
    ON hog_delete_file (catalog_id, data_file_id);

-- Changefeed windows range on begin_snapshot over ALL files (dead ones
-- included — replay semantics); hog_data_file_live is partial on
-- liveness and cannot serve them (V3).
CREATE INDEX hog_data_file_changefeed
    ON hog_data_file (catalog_id, table_id, begin_snapshot);
CREATE INDEX hog_delete_file_changefeed
    ON hog_delete_file (catalog_id, table_id, begin_snapshot);
-- Durable receipts are deliberately retained; expiry only closes prepared operations.
CREATE TABLE hog_table_creation (
    catalog_id bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    operation_id uuid NOT NULL,
    namespace_id bigint NOT NULL,
    definition jsonb NOT NULL,
    table_uuid uuid NOT NULL,
    write_path text NOT NULL,
    state text NOT NULL DEFAULT 'prepared' CHECK (state IN ('prepared', 'committed', 'rejected', 'aborted')),
    expires_at timestamptz NOT NULL DEFAULT clock_timestamp() + interval '24 hours',
    files jsonb,
    snapshot_id bigint,
    schema_version bigint,
    reason text,
    PRIMARY KEY (catalog_id, operation_id),
    UNIQUE (catalog_id, table_uuid),
    CHECK ((state = 'committed') = (snapshot_id IS NOT NULL AND schema_version IS NOT NULL))
);

-- Publication receipts outlive snapshot expiry: retrying an old request must
-- never publish it again. Keys are scoped to a catalog and immutable payload.
CREATE TABLE hog_commit_receipt (
    catalog_id BIGINT NOT NULL REFERENCES hog_catalog(catalog_id) ON DELETE CASCADE,
    idempotency_key UUID NOT NULL,
    request JSONB NOT NULL,
    snapshot_id BIGINT NOT NULL,
    schema_version BIGINT NOT NULL,
    PRIMARY KEY (catalog_id, idempotency_key)
);

CREATE TABLE hog_upload (
    catalog_id bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    upload_id uuid NOT NULL,
    owner uuid NOT NULL,
    prefix text NOT NULL,
    path text NOT NULL,
    file_kind text NOT NULL CHECK (file_kind IN ('data', 'delete')),
    state text NOT NULL DEFAULT 'active' CHECK (state IN ('active', 'registered', 'abandoned')),
    expires_at timestamptz NOT NULL DEFAULT now() + interval '24 hours',
    last_scheduled_at timestamptz,
    PRIMARY KEY (catalog_id, upload_id),
    UNIQUE (catalog_id, path)
);
CREATE INDEX hog_upload_owner ON hog_upload (catalog_id, owner) WHERE state = 'active';
CREATE INDEX hog_upload_cleanup ON hog_upload (catalog_id, last_scheduled_at, upload_id) WHERE state <> 'registered';

-- Replacement lineage (V14): the access path for the walk
-- OffsetRepo.releaseSupersededOffsets runs on every offset commit and
-- inside the expiry sweep, following `replaced_table_id` forward from a
-- retired incarnation to the one that replaced it. Partial, because only
-- a replacement row carries the edge.
CREATE INDEX hog_table_replacement_lineage
    ON hog_table (catalog_id, replaced_table_id) WHERE replaced_table_id IS NOT NULL;

-- Compaction group claims (V15): the optimization that stops two
-- maintenance replicas from rewriting the SAME group and throwing one of
-- the two rewrites away at commit. A CLAIM IS NEVER AUTHORIZATION —
-- correctness against a concurrent rewrite is the plan-to-commit
-- re-verification under the per-catalog commit lock, and stays there.
-- `group_key` is a hex SHA-256 over the group's spec id, partition
-- values and sorted input data_file_ids, so two replicas that plan the
-- same group compute the same key with no coordination;
-- `input_file_ids` rides along because the planner skips by OVERLAP
-- rather than key equality. `expires_at` is what makes a dead
-- maintainer's claim reclaimable (HOGLAKE_COMPACTION_CLAIM_TTL_SECONDS).
CREATE TABLE hog_compaction_claim (
    catalog_id bigint NOT NULL REFERENCES hog_catalog ON DELETE CASCADE,
    table_id bigint NOT NULL,
    group_key text NOT NULL,
    input_file_ids bigint[] NOT NULL,
    claimant uuid NOT NULL,
    claimed_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (catalog_id, table_id, group_key)
);
-- One index only: the planner's per-table read rides the PRIMARY KEY's
-- (catalog_id, table_id) prefix, and the per-sweep purge names a
-- catalog with no table, which the key cannot serve.
CREATE INDEX hog_compaction_claim_expiry
    ON hog_compaction_claim (catalog_id, expires_at);

-- The cleanup drain's liveness check (V17): `CleanupService.referencedPaths`
-- asks (catalog_id, path) of hog_data_file and hog_delete_file once per
-- sub-batch, holding the per-catalog commit lock while it does. Every
-- other index on these tables is keyed on ids and snapshots, so both
-- legs were a sequential scan of the whole relation — 190,884 buffers
-- and 692 ms at 5M rows against 8,728 and 33 ms with these (see the V17
-- migration for the measurements and the build-strategy trade).
--
-- NOT partial: the check covers "any file row, live or not", because a
-- historical row still claims its object at every retained snapshot.
-- Restricting to live rows would hide exactly the rows whose absence
-- authorizes a physical delete.
--
-- hog_upload needs no index of its own — UNIQUE (catalog_id, path)
-- above (V12) is already that key, and the planner drives its leg of
-- the same statement from it once the catalog's active claims are worth
-- probing, with `state` as a filter on the rows it fetches. Where it
-- prefers a bitmap scan of those claims instead, that fallback is
-- bounded by the ACTIVE set, which settles at commit — unlike these two
-- tables, which keep every historical row.
--
-- Paid for on every hog_data_file INSERT, which is the hottest write in
-- the system: +31 us and +7,343 bytes of WAL per row at 5M rows (1.47
-- GB/hour at the production churn of ~200k rows/hour), the WAL being
-- full-page images of a random 175-byte key's leaf. The index itself is
-- 1,157 MiB at 5M rows, 243 bytes/row.
CREATE INDEX hog_data_file_path ON hog_data_file (catalog_id, path);
CREATE INDEX hog_delete_file_path ON hog_delete_file (catalog_id, path);
