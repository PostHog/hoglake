// Hand-written fixtures shaped exactly like the OpenAPI schemas in
// server/src/main/resources/openapi/hoglake.yaml (snake_case wire format).
// int64-typed fields are decimal strings on the client side (see
// src/api/int64.ts); the parser accepts them as strings from JSON too, so
// serializing these fixtures with JSON.stringify round-trips exactly.

import type {
  ApiErrorBody,
  Catalog,
  CatalogOptions,
  ConsumerOffset,
  DataFile,
  InstanceMaintenanceStatus,
  MaintenanceRun,
  MaintenanceRunPage,
  MaintenanceStatus,
  Namespace,
  PartitionValues,
  PartitionStatsResponse,
  ScanFile,
  SnapshotPage,
  Table,
  TableSummary,
} from "../src/api/types";

export const catalogsFixture: Catalog[] = [
  {
    name: "analytics",
    data_path: "s3://hog-lake/analytics",
    head_snapshot_id: "4211",
    schema_version: "7",
    // Sampled. live_rows is past 2^53 deliberately: it must survive as
    // an exact string, which is why these keys are in INT64_FIELDS.
    table_count: "42",
    live_rows: "9007199254740993",
    live_size_bytes: "5368709120",
    // Oldest retained snapshot, from the same sample. An instant, not a
    // pre-computed age — the page renders the elapsed time.
    oldest_snapshot_time: "2026-09-16T00:00:00Z",
  },
  {
    // NOT sampled: the totals are absent rather than zero, which is the
    // case the page has to render as an em dash. oldest_snapshot_time is
    // absent too, so its age cell is an em dash, not "0s".
    name: "scratch",
    data_path: "s3://hog-lake/scratch",
    head_snapshot_id: "12",
    schema_version: "1",
  },
];

export const namespacesFixture: Namespace[] = [
  { name: "events" },
  { name: "sessions" },
];

/** The analytics catalog's retention options (GET /catalogs/analytics/options). */
export const catalogOptionsFixture: CatalogOptions = {
  // 604800s = 7d, rendered as a duration by the header.
  snapshot_retention_seconds: "604800",
  consumer_floor: true,
  earliest_snapshot_id: "4099",
};

/** A catalog with expiry disabled: snapshot_retention_seconds absent. */
export const catalogOptionsNoExpiryFixture: CatalogOptions = {
  consumer_floor: false,
  earliest_snapshot_id: "1",
};

export const tablesFixture: TableSummary[] = [
  {
    name: "pageviews",
    table_uuid: "3f2c9c04-8a1b-4c7e-9f10-6d2a5b3e8c71",
    // Two lines: the listing shows the first and offers the rest, which
    // is the whole point of the clamp.
    comment: "raw pageview events\nkept for 90 days, then expired",
    record_count: "1234567",
    file_count: "12",
    file_size_bytes: "987654321",
    snapshot_count: "41",
    earliest_snapshot_id: "7",
  },
  {
    // No comment and no retained history: the server omits both
    // properties rather than nulling them.
    name: "clicks",
    table_uuid: "b7e6d9a2-15f3-4b08-a4c9-0e8f7d6c5b4a",
    record_count: "0",
    file_count: "0",
    file_size_bytes: "0",
    snapshot_count: "1",
  },
];

export const tableFixture: Table = {
  name: "pageviews",
  namespace: "events",
  table_uuid: "3f2c9c04-8a1b-4c7e-9f10-6d2a5b3e8c71",
  columns: [
    { field_id: "1", ordinal: 0, name: "ts", type: "timestamptz", nullable: false },
    { field_id: "2", ordinal: 1, name: "user_id", type: "long", nullable: false },
    { field_id: "3", ordinal: 2, name: "url", type: "string", nullable: true },
  ],
  record_count: "1234567",
  file_count: "42",
  file_size_bytes: "5368709120",
  partition_spec: {
    spec_id: "1",
    fields: [
      { source_field_id: "1", transform: "day" },
      { source_field_id: "3", transform: "bucket", transform_param: 16 },
    ],
  },
};

/**
 * The distinct stored values of tableFixture's partition fields, as
 * GET .../partitions/values returns them. Matches the spec above:
 * ts_day ordinals and url bucket indices, in the writer's stored strings.
 */
export const partitionValuesFixture: PartitionValues = {
  spec_id: "1",
  fields: [
    {
      source_field_id: "1",
      transform: "day",
      values: ["20697", "20698"],
      truncated: false,
    },
    {
      source_field_id: "3",
      transform: "bucket",
      transform_param: 16,
      values: ["7", "12"],
      truncated: false,
    },
  ],
};

/**
 * One file per stats_state variant: provided / pending / failed.
 *
 * tableFixture is UNSORTED, so every file's ordering key is the row id
 * and the server states the span it computes from row_id_start and
 * record_count — for all three, stats_state included, since row ids
 * need no statistics.
 */
export const filesFixture: DataFile[] = [
  {
    data_file_id: "101",
    path: "s3://hog-lake/analytics/events/pageviews/data-00101.parquet",
    file_format: "parquet",
    record_count: "500000",
    file_size_bytes: "268435456",
    footer_size: "4096",
    row_id_start: "0",
    stats_state: "provided",
    begin_snapshot: "4001",
    spec_id: "1",
    // day ordinal (days since the epoch), as transforms.wire_string
    // emits it — 20697 is 2026-09-01. A date string here was not a
    // value the writer can produce.
    partition_values: ["20697", "7"],
    ordering_bounds: { lower_bound: "0", upper_bound: "499999" },
  },
  {
    data_file_id: "102",
    path: "s3://hog-lake/analytics/events/pageviews/data-00102.parquet",
    file_format: "parquet",
    record_count: "480000",
    file_size_bytes: "251658240",
    row_id_start: "500000",
    stats_state: "pending",
    begin_snapshot: "4100",
    spec_id: "1",
    partition_values: ["20698", null],
    ordering_bounds: { lower_bound: "500000", upper_bound: "979999" },
  },
  {
    data_file_id: "103",
    path: "s3://hog-lake/analytics/events/pageviews/data-00103.parquet",
    file_format: "parquet",
    record_count: "254567",
    file_size_bytes: "134217728",
    row_id_start: "980000",
    stats_state: "failed",
    begin_snapshot: "4200",
    ordering_bounds: { lower_bound: "980000", upper_bound: "1234566" },
  },
];

/**
 * The same table, SORTED by user_id (field 2) — the case where the row
 * id says nothing and the files table reports the leading sort field's
 * bounds instead.
 */
export const sortedTableFixture: Table = {
  ...tableFixture,
  sort_spec: {
    sort_id: "3",
    fields: [
      { source_field_id: "2", direction: "asc", null_order: "nulls_last" },
      // A second key, deliberately: only the LEADING field's bounds are
      // shown, because a tiebreaker's per-file range spans the column.
      { source_field_id: "1", direction: "desc", null_order: "nulls_first" },
    ],
  },
};

/**
 * tableFixture with the versioned metadata V11 added: a table comment,
 * per-column comments (one column deliberately without one, for the em
 * dash), and a couple of user properties. One property value is long and
 * carries markup — it must render as text, never HTML.
 */
export const commentedTableFixture: Table = {
  ...tableFixture,
  comment: "Page-view events, one row per view. Owned by the web analytics team.",
  properties: {
    owner: "web-analytics",
    "quality.tier": "gold",
  },
  columns: [
    { field_id: "1", ordinal: 0, name: "ts", type: "timestamptz", nullable: false, comment: "Event time, UTC." },
    { field_id: "2", ordinal: 1, name: "user_id", type: "long", nullable: false },
    { field_id: "3", ordinal: 2, name: "url", type: "string", nullable: true, comment: "Full URL, query string included." },
  ],
};

/** A table whose comment exceeds the inline clamp, to exercise expand. */
export const longCommentTableFixture: Table = {
  ...commentedTableFixture,
  comment:
    "Page-view events. " +
    "This comment is deliberately longer than the one-line clamp so the expand control appears. ".repeat(
      3,
    ) +
    "Trailing detail.",
};

/**
 * Files of the sorted table, covering the three states the bound cells
 * have to tell apart:
 *
 *  - 201: bounds of the sort key, decoded.
 *  - 202: a compaction OUTPUT whose ordering key is still the sort key,
 *    with a null lower bound — an all-null column, so nothing to prune
 *    on.
 *  - 203: stats pending, so no ordering_bounds at all. Its row ids are
 *    not offered in their place: on a sorted table they describe
 *    nothing, since a sorted rewrite remaps them.
 */
export const sortedFilesFixture: DataFile[] = [
  {
    data_file_id: "201",
    path: "s3://hog-lake/analytics/events/pageviews/data-00201.parquet",
    file_format: "parquet",
    record_count: "500000",
    file_size_bytes: "268435456",
    row_id_start: "0",
    stats_state: "provided",
    begin_snapshot: "4001",
    spec_id: "1",
    partition_values: ["20697", "7"],
    ordering_bounds: {
      field_id: "2",
      lower_bound: "1000",
      upper_bound: "4999",
    },
  },
  {
    data_file_id: "202",
    path: "s3://hog-lake/analytics/events/pageviews/data-00202.parquet",
    file_format: "parquet",
    record_count: "480000",
    file_size_bytes: "251658240",
    row_id_start: "500000",
    stats_state: "provided",
    begin_snapshot: "4100",
    spec_id: "1",
    partition_values: ["20698", null],
    explicit_row_ids: true,
    ordering_bounds: {
      field_id: "2",
      lower_bound: null,
      upper_bound: "9999",
    },
  },
  {
    data_file_id: "203",
    path: "s3://hog-lake/analytics/events/pageviews/data-00203.parquet",
    file_format: "parquet",
    record_count: "254567",
    file_size_bytes: "134217728",
    row_id_start: "980000",
    stats_state: "pending",
    begin_snapshot: "4200",
    spec_id: "1",
    partition_values: ["20699", "3"],
  },
];

/** Scan plan pairing one data file with its live deletion vector. */
export const scanFixture: ScanFile[] = [
  {
    data_file: filesFixture[0],
    delete_file: {
      delete_file_id: "900",
      data_file_id: "101",
      path: "s3://hog-lake/analytics/events/pageviews/dv-00900.puffin",
      file_format: "puffin-dv",
      delete_count: "1250",
      file_size_bytes: "8192",
      begin_snapshot: "4150",
    },
  },
  {
    data_file: filesFixture[1],
  },
];

// The timeline pages DESCENDING with the `before` cursor: page 1 covers
// before=head+1 (4212), page 2 continues below the last id of page 1.
export const snapshotsPage1: SnapshotPage = {
  snapshots: [
    {
      snapshot_id: "4211",
      snapshot_time: "2026-09-04T10:17:00Z",
      schema_version: "7",
      author: "ops",
      message: "create table events.clicks",
      changes: [{ kind: "table_created", object_id: "56" }],
    },
    {
      snapshot_id: "4210",
      snapshot_time: "2026-09-04T10:16:00Z",
      schema_version: "7",
      author: "millpond",
      message: "append 1 file",
      changes: [{ kind: "files_added", object_id: "55" }],
    },
  ],
  has_more: true,
};

export const snapshotsPage2: SnapshotPage = {
  snapshots: [
    {
      snapshot_id: "4209",
      snapshot_time: "2026-09-04T10:15:00Z",
      schema_version: "7",
      author: "viaduck",
      message: "append 3 files",
      changes: [
        { kind: "files_added", object_id: "55" },
        { kind: "rows_appended", object_id: "55" },
      ],
    },
  ],
  has_more: false,
};

// Snapshot messages as millpond writes them since the offsets block
// landed: line 1 is a key=value summary, line 2+ an `offsets` block that
// reaches ~700 bytes at 32 ranges on prod and 16 KiB at worst. The block
// ends in its own count — a bare `(N)`, or `(+k more)` when millpond
// itself truncated the list. Compaction stays one line.
export const MILLPOND_SUMMARY =
  "records=1048576 files=4 partitions=2 arrow_bytes=134217728 " +
  "trigger=size millpond=1.9.3 table=events.pageviews";
export const MILLPOND_OFFSETS =
  "offsets events_json p0:41200-83999 p1:41200-84010 p2:41198-83944 (32)";
export const MILLPOND_OFFSETS_TRUNCATED =
  "offsets events_json p0:11-22 p1:23-44 (+8 more)";
export const COMPACTION_MESSAGE = "compact 12 files into 1 (events.pageviews)";

export const snapshotsMessagesPage: SnapshotPage = {
  snapshots: [
    {
      snapshot_id: "4211",
      snapshot_time: "2026-09-04T10:17:00Z",
      schema_version: "7",
      author: "millpond",
      message: `${MILLPOND_SUMMARY}\n${MILLPOND_OFFSETS}`,
      changes: [{ kind: "files_added", object_id: "55" }],
    },
    {
      snapshot_id: "4210",
      snapshot_time: "2026-09-04T10:16:00Z",
      schema_version: "7",
      author: "millpond",
      message: `${MILLPOND_SUMMARY}\n${MILLPOND_OFFSETS_TRUNCATED}`,
      changes: [{ kind: "files_added", object_id: "54" }],
    },
    {
      snapshot_id: "4209",
      snapshot_time: "2026-09-04T10:15:00Z",
      schema_version: "7",
      author: "compactor",
      message: COMPACTION_MESSAGE,
      changes: [{ kind: "files_added", object_id: "53" }],
    },
  ],
  has_more: false,
};

export const consumerOffsetsFixture: ConsumerOffset[] = [
  {
    consumer_id: "viaduck-sink-7",
    table_uuid: "3f2c9c04-8a1b-4c7e-9f10-6d2a5b3e8c71",
    committed_snapshot: "4205",
    updated_at: "2026-09-04T10:14:30Z",
  },
  {
    consumer_id: "viaduck-sink-7",
    table_uuid: "b7e6d9a2-15f3-4b08-a4c9-0e8f7d6c5b4a",
    committed_snapshot: "4198",
    updated_at: "2026-09-04T10:12:11Z",
  },
];

// GET /v1/catalogs/{c}/stats/partitions — server-ordered by debt_score desc.
export const partitionStatsFixture: PartitionStatsResponse = {
  partitions: [
    {
      namespace: "events",
      table: "pageviews",
      table_uuid: "3f2c9c04-8a1b-4c7e-9f10-6d2a5b3e8c71",
      partition_values: [
        { field: "team_id", value: "42" },
        { field: "month", value: "2026-09" },
      ],
      spec_id: "3",
      file_count: "120",
      small_file_count: "118",
      total_bytes: "5368709120",
      small_file_bytes: "943718400",
      avg_file_bytes: "44739242",
      dv_count: "2",
      debt_score: "118",
    },
    {
      namespace: "events",
      table: "clicks",
      table_uuid: "b7e6d9a2-15f3-4b08-a4c9-0e8f7d6c5b4a",
      partition_values: [],
      spec_id: "0",
      file_count: "40",
      small_file_count: "9",
      total_bytes: "268435456",
      small_file_bytes: "9437184",
      avg_file_bytes: "6710886",
      dv_count: "0",
      debt_score: "9",
    },
  ],
  truncated: false,
  small_file_threshold_bytes: "536870912",
};

export const notFoundError: ApiErrorBody = {
  error: "not_found",
  detail: "catalog 'nope' does not exist",
};

export const conflictError: ApiErrorBody = {
  error: "conflict",
  detail: "catalog 'analytics' already exists",
};

export const unprocessableError: ApiErrorBody = {
  error: "validation_failed",
  detail: "unknown field ids in column_stats: [99]",
};

// ---- maintenance -----------------------------------------------------------
//
// One run per task flavor, shaped like the ledger rows the runs endpoint
// returns: loop sweep rows, a manual rehydrate, a failed run (error, null
// result). run_id strings since run_id is int64 on the wire.

export const maintenanceRunsFixture: MaintenanceRun[] = [
  {
    run_id: "104",
    catalog: "analytics",
    task: "expiry",
    trigger: "loop",
    started_at: "2026-09-11T10:00:00Z",
    finished_at: "2026-09-11T10:00:00.140Z",
    status: "ok",
    result: {
      snapshots_expired: "12",
      data_files_queued: "3",
      delete_files_queued: "0",
      new_earliest_snapshot_id: "4099",
      floored_by_consumer: "hedgerow-events",
    },
  },
  {
    run_id: "103",
    catalog: "analytics",
    task: "cleanup",
    trigger: "loop",
    started_at: "2026-09-11T09:59:00Z",
    finished_at: "2026-09-11T09:59:02.300Z",
    status: "ok",
    result: { removed: "3", missing: "1", still_referenced: "0" },
  },
  {
    run_id: "102",
    catalog: "analytics",
    task: "hydrator",
    trigger: "loop",
    started_at: "2026-09-11T09:58:00Z",
    finished_at: "2026-09-11T09:58:01.000Z",
    status: "ok",
    result: { claimed: "2", hydrated: "2", failed: "0", transient: "0" },
  },
  {
    run_id: "101",
    catalog: "analytics",
    task: "compaction",
    trigger: "manual",
    started_at: "2026-09-11T09:57:00Z",
    finished_at: "2026-09-11T09:57:05.500Z",
    status: "ok",
    result: {
      groups_compacted: "1",
      files_in: "6",
      files_out: "1",
      bytes_in: "943718400",
      bytes_out: "940000000",
      skipped_conflicts: "0",
      dv_superseded: "0",
      unconvertible_schema: "0",
      invalid_data: "0",
      heap_budget_exceeded: "0",
      failed_groups: "0",
    },
  },
  {
    run_id: "100",
    catalog: "analytics",
    task: "verify",
    trigger: "manual",
    started_at: "2026-09-11T09:56:00Z",
    finished_at: "2026-09-11T09:56:01.100Z",
    status: "ok",
    result: {
      catalog: "analytics",
      status: "pass",
      checks: [
        { check: "row_id_tiling", status: "pass", violations: "0", samples: [] },
        { check: "delete_vectors", status: "pass", violations: "0", samples: [] },
      ],
    },
  },
  {
    run_id: "98",
    catalog: "analytics",
    task: "retirement",
    trigger: "loop",
    started_at: "2026-09-11T09:58:00Z",
    finished_at: "2026-09-11T09:58:44.000Z",
    status: "ok",
    result: {
      tables: "1",
      rows_retired: "24000",
      dvs_retired: "12",
      paths_queued: "24012",
      batches: "3",
      timeouts: "0",
      skipped_tables: "0",
      skipped_queue_full: "0",
      skipped_locked: "0",
      convoyed: "0",
      tables_remaining: "1",
    },
  },
  {
    run_id: "99",
    catalog: "analytics",
    task: "expiry",
    trigger: "loop",
    started_at: "2026-09-11T09:00:00Z",
    finished_at: "2026-09-11T09:00:00.050Z",
    status: "failed",
    error: "FATAL: connection to server lost",
    result: null,
  },
];

/**
 * The deployment shape that made #114 visible: compaction's loop runs in
 * ANOTHER process, so this one's loop_interval_ms is "0" while the ledger
 * shows sweeps arriving every ~69s. Anything rendering the config would
 * call the busiest task disabled.
 */
export const maintenanceStatusFixture: MaintenanceStatus = {
  catalog: "analytics",
  tasks: [
    {
      task: "hydrator",
      loop_interval_ms: "5000",
      last_run: maintenanceRunsFixture[2],
      backlog: { pending_files: "4", failed_files: "1" },
      // Records only the catalogs it claimed files for, so it has a last
      // run and no derivable cadence.
      loop: { last_run_at: "2026-09-11T10:00:00Z", records_every_sweep: false },
    },
    {
      task: "expiry",
      loop_interval_ms: "60000",
      last_run: maintenanceRunsFixture[0],
      backlog: {
        snapshot_retention_seconds: "604800",
        consumer_floor: true,
        earliest_snapshot_id: "4099",
        head_snapshot_id: "4211",
      },
      loop: { observed_interval_ms: "60000", last_run_at: "2026-09-11T10:00:00Z", records_every_sweep: true },
    },
    {
      task: "cleanup",
      loop_interval_ms: "60000",
      last_run: maintenanceRunsFixture[1],
      backlog: { queued_removals: "0" },
      loop: { observed_interval_ms: "45300", last_run_at: "2026-09-11T09:59:00Z", records_every_sweep: true },
    },
    {
      task: "compaction",
      loop_interval_ms: "0",
      last_run: maintenanceRunsFixture[3],
      backlog: { small_files: "42", target_bytes: "536870912" },
      loop: { observed_interval_ms: "68800", last_run_at: "2026-09-11T09:59:30Z", records_every_sweep: true },
    },
    {
      // Verify has a loop of its own now; this fixture is the gigahog
      // API pod, which runs it OFF (loop_interval_ms 0) while the
      // maintenance workload sweeps hourly — so the ledger still shows
      // no loop runs for this catalog.
      task: "verify",
      loop_interval_ms: "0",
      last_run: maintenanceRunsFixture[4],
      backlog: {},
      loop: { records_every_sweep: true },
    },
    {
      // Retirement, like compaction and verify, runs on the maintenance
      // workload: this fixture is the API pod, so its own interval is 0
      // while the ledger shows the other pod's sweeps arriving.
      task: "retirement",
      loop_interval_ms: "0",
      last_run: maintenanceRunsFixture[5],
      backlog: {},
      loop: {
        observed_interval_ms: "60000",
        last_run_at: "2026-09-11T09:58:00Z",
        records_every_sweep: true,
      },
    },
  ],
};

export const maintenanceRunPageFixture: MaintenanceRunPage = {
  runs: maintenanceRunsFixture,
  has_more: false,
};

/**
 * The central page's rollup: analytics (with its five tasks) plus a second,
 * quieter catalog that has never run anything.
 */
export const instanceMaintenanceStatusFixture: InstanceMaintenanceStatus = {
  catalogs: [
    maintenanceStatusFixture,
    {
      catalog: "scratch",
      tasks: [
        {
          task: "hydrator",
          loop_interval_ms: "5000",
          last_run: null,
          backlog: { pending_files: "0", failed_files: "0" },
          loop: { records_every_sweep: false },
        },
        {
          task: "expiry",
          loop_interval_ms: "60000",
          last_run: null,
          backlog: {
            consumer_floor: true,
            earliest_snapshot_id: "0",
            head_snapshot_id: "12",
          },
          loop: { records_every_sweep: true },
        },
        {
          task: "cleanup",
          loop_interval_ms: "60000",
          last_run: null,
          backlog: { queued_removals: "0" },
          loop: { records_every_sweep: true },
        },
        {
          task: "compaction",
          loop_interval_ms: "0",
          last_run: null,
          backlog: { small_files: "0", target_bytes: "536870912" },
          loop: { records_every_sweep: true },
        },
        { task: "verify", last_run: null, backlog: {}, loop: null },
        {
          // Not `loop: null` (verify's shape above is deliberately the
          // OLDER-SERVER one): retirement has a loop, this catalog has
          // simply never had an eligible drop for it to run on.
          task: "retirement",
          last_run: null,
          backlog: {},
          loop: { records_every_sweep: true },
        },
      ],
    },
  ],
};
