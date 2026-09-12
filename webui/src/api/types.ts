// Hand-written TypeScript mirrors of the OpenAPI schemas in
// server/src/main/resources/openapi/hoglake.yaml. Wire format is snake_case;
// these types pass it through untouched.
//
// Every field the spec types as `integer, format: int64` is carried as an
// exact decimal string (`Int64`), never a JS number: snapshot ids, row
// counts, file sizes and row-id starts can exceed 2^53 and would be silently
// rounded by JSON.parse. See src/api/int64.ts.

/** An int64 wire value, held as its exact decimal string representation. */
export type Int64 = string;

export interface ApiErrorBody {
  error: string;
  detail?: string;
}

export interface Catalog {
  name: string;
  data_path: string;
  head_snapshot_id: Int64;
  schema_version: Int64;
}

export interface CreateCatalogRequest {
  name: string;
  data_path: string;
}

export interface Namespace {
  name: string;
}

export const COLUMN_TYPES = [
  "boolean",
  "int",
  "long",
  "float",
  "double",
  "decimal",
  "date",
  "time",
  "timestamp",
  "timestamptz",
  "string",
  "uuid",
  "binary",
] as const;

export type ColumnType = (typeof COLUMN_TYPES)[number];

export interface ColumnDef {
  name: string;
  type: ColumnType;
  type_params?: Record<string, unknown>;
  nullable?: boolean;
}

export interface Column extends ColumnDef {
  field_id: Int64;
  ordinal: number;
}

export interface CreateTableRequest {
  name: string;
  columns: ColumnDef[];
}

export interface TableSummary {
  name: string;
  table_uuid: string;
}

export type PartitionTransform =
  | "identity"
  | "bucket"
  | "year"
  | "month"
  | "day"
  | "hour";

export interface PartitionField {
  source_field_id: Int64;
  transform: PartitionTransform;
  transform_param?: number;
}

export interface PartitionSpec {
  spec_id: Int64;
  fields: PartitionField[];
}

export interface Table {
  name: string;
  namespace: string;
  table_uuid: string;
  columns: Column[];
  record_count: Int64;
  file_count: Int64;
  file_size_bytes: Int64;
  partition_spec?: PartitionSpec;
}

export type StatsState = "provided" | "pending" | "failed";

export interface DataFile {
  data_file_id: Int64;
  path: string;
  file_format: string;
  record_count: Int64;
  file_size_bytes: Int64;
  footer_size?: Int64;
  row_id_start: Int64;
  stats_state: StatsState;
  begin_snapshot: Int64;
  spec_id?: Int64;
  partition_values?: (string | null)[];
}

export interface DeleteFile {
  delete_file_id: Int64;
  data_file_id: Int64;
  path: string;
  file_format: "puffin-dv";
  delete_count: Int64;
  file_size_bytes: Int64;
  begin_snapshot: Int64;
}

export interface ScanFile {
  data_file: DataFile;
  delete_file?: DeleteFile;
}

export interface SnapshotChange {
  kind: string;
  object_id?: Int64;
}

export interface Snapshot {
  snapshot_id: Int64;
  snapshot_time: string;
  schema_version: Int64;
  author?: string;
  message?: string;
  changes?: SnapshotChange[];
}

export interface SnapshotPage {
  snapshots: Snapshot[];
  has_more: boolean;
}

export interface ConsumerOffset {
  consumer_id: string;
  table_uuid: string;
  committed_snapshot: Int64;
  updated_at: string;
}

export interface ConsumerTableOffset {
  table_uuid: string;
  committed_snapshot: Int64;
  updated_at: string;
  namespace?: string;
  table_name?: string;
  table_dropped: boolean;
}

export interface ConsumerSummary {
  consumer_id: string;
  offsets: ConsumerTableOffset[];
}

export interface ConsumerList {
  consumers: ConsumerSummary[];
}

export interface PartitionValueEntry {
  field: string;
  value: string;
}

/** One leaf partition's compaction-debt stats, ranked by debt_score. */
export interface PartitionStats {
  namespace: string;
  table: string;
  table_uuid: string;
  partition_values: PartitionValueEntry[];
  spec_id: Int64;
  file_count: Int64;
  small_file_count: Int64;
  total_bytes: Int64;
  small_file_bytes: Int64;
  avg_file_bytes: Int64;
  dv_count: Int64;
  debt_score: Int64;
}

export interface PartitionStatsResponse {
  sampled_at?: string;
  partitions: PartitionStats[];
  truncated: boolean;
  // The compaction target size the report used as its small-file
  // threshold (strict <), for saying what "small" means in bytes.
  small_file_threshold_bytes: Int64;
}

export interface CommitResult {
  snapshot_id: Int64;
  schema_version?: Int64;
}

// ---- maintenance (GET /maintenance/status + /maintenance/runs) ------------
//
// The run ledger mirrors hog_maintenance_run: `result` is the matching POST
// response body (wire snake_case), typed per task below. A failed run has
// `result: null` and carries `error` instead.

export type MaintenanceTask =
  | "hydrator"
  | "expiry"
  | "cleanup"
  | "compaction"
  | "verify";

export type MaintenanceTrigger = "loop" | "manual";

export type MaintenanceRunState = "ok" | "failed";

/** Hydrator loop rows: the sweep's per-catalog claim outcomes. */
export interface HydratorSweepResult {
  claimed: Int64;
  hydrated: Int64;
  failed: Int64;
  transient: Int64;
}

/** Manual hydrator rows are rehydrate calls. */
export interface RehydrateResult {
  requeued: Int64;
}

export interface ExpiryResult {
  snapshots_expired: Int64;
  data_files_queued: Int64;
  delete_files_queued: Int64;
  new_earliest_snapshot_id: Int64;
  floored_by_consumer?: string;
}

export interface CleanupResult {
  removed: Int64;
  missing: Int64;
  still_referenced: Int64;
}

export interface CompactionResult {
  groups_compacted: Int64;
  files_in: Int64;
  files_out: Int64;
  bytes_in: Int64;
  bytes_out: Int64;
  skipped_conflicts: Int64;
  dv_superseded: Int64;
  unconvertible_schema: Int64;
  /** Groups that failed outright (logged, retried next run) — red-flag counter. */
  failed_groups: Int64;
}

export interface VerifyCheck {
  check: string;
  status: "pass" | "fail";
  violations: Int64;
  samples: string[];
}

export interface VerifyReport {
  catalog: string;
  status: "pass" | "fail";
  checks: VerifyCheck[];
}

interface MaintenanceRunBase {
  run_id: Int64;
  /** The catalog the run acted on (the ledger is per-catalog). */
  catalog: string;
  trigger: MaintenanceTrigger;
  started_at: string;
  finished_at: string;
  status: MaintenanceRunState;
  /** Present iff status is "failed". */
  error?: string;
}

export type MaintenanceRun =
  | (MaintenanceRunBase & {
      task: "hydrator";
      result: HydratorSweepResult | RehydrateResult | null;
    })
  | (MaintenanceRunBase & { task: "expiry"; result: ExpiryResult | null })
  | (MaintenanceRunBase & { task: "cleanup"; result: CleanupResult | null })
  | (MaintenanceRunBase & { task: "compaction"; result: CompactionResult | null })
  | (MaintenanceRunBase & { task: "verify"; result: VerifyReport | null });

export interface HydratorBacklog {
  pending_files?: Int64;
  failed_files?: Int64;
}

export interface ExpiryBacklog {
  /** Absent when snapshot expiry is disabled. */
  snapshot_retention_seconds?: Int64;
  consumer_floor: boolean;
  earliest_snapshot_id: Int64;
  head_snapshot_id: Int64;
}

export interface CleanupBacklog {
  /** Undrained removal-queue entries. */
  queued_removals?: Int64;
  /** Absent on an empty queue. */
  oldest_queued_age_seconds?: number;
}

export interface CompactionBacklog {
  /** Live files under the target size — the debt a sweep plans against. */
  small_files?: Int64;
  target_bytes: Int64;
}

export type VerifyBacklog = Record<string, never>;

interface MaintenanceTaskStatusBase {
  /** 0 = loop disabled; absent = the task has no loop (verify). */
  loop_interval_ms?: Int64;
  /** Always present; null = no recorded run yet. */
  last_run: MaintenanceRun | null;
}

export type MaintenanceTaskStatus =
  | (MaintenanceTaskStatusBase & { task: "hydrator"; backlog: HydratorBacklog })
  | (MaintenanceTaskStatusBase & { task: "expiry"; backlog: ExpiryBacklog })
  | (MaintenanceTaskStatusBase & { task: "cleanup"; backlog: CleanupBacklog })
  | (MaintenanceTaskStatusBase & { task: "compaction"; backlog: CompactionBacklog })
  | (MaintenanceTaskStatusBase & { task: "verify"; backlog: VerifyBacklog });

export interface MaintenanceStatus {
  catalog: string;
  tasks: MaintenanceTaskStatus[];
  sampled_at?: string;
  sample_started_at?: string;
  sampled_snapshot_id?: Int64;
}

/** GET /v1/maintenance/status: every catalog's MaintenanceStatus, by name. */
export interface InstanceMaintenanceStatus {
  catalogs: MaintenanceStatus[];
  has_more?: boolean;
  next_after?: string;
}

export interface MaintenanceRunPage {
  runs: MaintenanceRun[];
  has_more: boolean;
}
