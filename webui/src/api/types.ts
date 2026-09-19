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
  // Live totals from the metrics sampler's last pass. Absent — never
  // zero — for a catalog it has not covered yet, so the page can tell
  // "not sampled" from "empty".
  table_count?: Int64;
  live_rows?: Int64;
  live_size_bytes?: Int64;
  // Commit time of the oldest retained snapshot, from the same sample as
  // the totals — an ISO instant, absent until sampled. The page renders
  // it as a live age; it is not the expiry-floor time (which waits on
  // expiry and would read a never-expired catalog as having none).
  oldest_snapshot_time?: string;
}

export interface CreateCatalogRequest {
  name: string;
  data_path: string;
}

export interface Namespace {
  name: string;
}

// The server's closed column-type vocabulary, in the spec's enum order
// (ColumnDef.type in openapi/hoglake.yaml). Types the server refuses
// permanently — int128, uint128, timetz, interval, geometry — are absent
// on purpose: the console must not offer what the API always rejects.
/**
 * The types the create-table form can build. Scalars only, on purpose:
 * list/struct/map require `children`, the form has no child editor, and
 * offering them in the dropdown would make every such choice a
 * guaranteed 422 — the same reason the permanently refused DuckLake
 * names are absent.
 */
export const SCALAR_COLUMN_TYPES = [
  "boolean",
  "int8",
  "int16",
  "int",
  "long",
  "uint8",
  "uint16",
  "uint32",
  "uint64",
  "float",
  "double",
  "decimal",
  "date",
  "time",
  "timestamp_s",
  "timestamp_ms",
  "timestamp",
  "timestamp_ns",
  "timestamptz",
  "string",
  "json",
  "uuid",
  "binary",
] as const;

/**
 * The container types. Readable everywhere (a table can have them), but
 * not creatable from the console — see SCALAR_COLUMN_TYPES.
 */
export const NESTED_COLUMN_TYPES = ["list", "struct", "map"] as const;

/**
 * Readable, not creatable, and not a container.
 *
 * VARIANT is a catalog scalar, but it belongs here rather than in
 * SCALAR_COLUMN_TYPES for the same reason the containers do: the create
 * form is a name and a type picker, and there is no useful variant a
 * form can produce. #77 added the type server-side without touching the
 * console, so a table holding one had a `type` outside ColumnType
 * entirely.
 */
export const OPAQUE_COLUMN_TYPES = ["variant"] as const;

/** Everything the server's ColumnDef.type enum accepts. */
export const COLUMN_TYPES = [
  ...SCALAR_COLUMN_TYPES,
  ...OPAQUE_COLUMN_TYPES,
  ...NESTED_COLUMN_TYPES,
] as const;

export type ColumnType = (typeof COLUMN_TYPES)[number];

export interface ColumnDef {
  name: string;
  type: ColumnType;
  type_params?: Record<string, unknown>;
  nullable?: boolean;
  /** Present only for list/struct/map. */
  children?: ColumnDef[];
}

export interface Column extends ColumnDef {
  field_id: Int64;
  /** 0-based among SIBLINGS, not table-wide. */
  ordinal: number;
  children?: Column[];
}

/**
 * A container column's type as one readable signature —
 * `list&lt;int&gt;`, `map&lt;string, long&gt;`,
 * `struct&lt;a: int, b: string&gt;` — and a scalar's as its own name.
 */
export function formatColumnType(c: ColumnDef): string {
  // Ordered by ordinal where there is one: ordinal is the contract and
  // array order is not, and a struct signature that listed its fields in
  // whatever order the JSON arrived in would disagree with the table
  // below it on the same page.
  const kids = [...(c.children ?? [])].sort((a, b) =>
    "ordinal" in a && "ordinal" in b
      ? (a as Column).ordinal - (b as Column).ordinal
      : 0,
  );
  if (c.type === "list") {
    return `list<${kids.length === 1 ? formatColumnType(kids[0]) : "?"}>`;
  }
  if (c.type === "map") {
    return kids.length === 2
      ? `map<${formatColumnType(kids[0])}, ${formatColumnType(kids[1])}>`
      : "map<?>";
  }
  if (c.type === "struct") {
    // `struct<>` reads as a valid empty struct; it is not one — a
    // struct with no children is a shape the server refuses, so it can
    // only mean the children were not loaded or were dropped on the way
    // here. List and map say `<?>` for the same condition, and an
    // unknown should look the same wherever it appears.
    return kids.length === 0
      ? "struct<?>"
      : `struct<${kids.map((k) => `${k.name}: ${formatColumnType(k)}`).join(", ")}>`;
  }
  return c.type;
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

export type SortDirection = "asc" | "desc";
export type NullOrder = "nulls_first" | "nulls_last";

export interface SortField {
  source_field_id: Int64;
  direction: SortDirection;
  null_order: NullOrder;
}

/**
 * A table's sort order at the requested snapshot; absent when the table
 * is unsorted there. Advisory for writers, binding for compaction — and
 * for the files table it is what names the ordering key whose bounds
 * each file reports.
 */
export interface SortSpec {
  sort_id: Int64;
  fields: SortField[];
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
  sort_spec?: SortSpec;
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
  /**
   * True for compaction outputs: the row ids ride an explicit physical
   * `_hog_row_id` column, so row_id_start is min(input row ids) and has
   * no positional meaning.
   */
  explicit_row_ids?: boolean;
  ordering_bounds?: FileOrderingBounds;
}

/**
 * The range one file covers along the key its table is ORDERED by, as
 * GET .../files ships it. `field_id` names the leading sort-spec field
 * the bounds belong to; ABSENT means the table is unsorted and the
 * range is the file's row-id span — the row id is the implicit ordering
 * key and has no field id.
 *
 * The whole object is absent when the server has no range to state (a
 * sorted table's file whose stats are pending or failed, a key column
 * added after the file landed, an empty file), which is not the same
 * fact as a null bound INSIDE it: null is a stated answer — "no bound
 * stored, do not prune" on a sort key, "unknown" for the upper end of a
 * compaction output's row-id span.
 */
export interface FileOrderingBounds {
  field_id?: Int64;
  lower_bound: DecodedBound;
  upper_bound: DecodedBound;
}

/**
 * A decoded bound after parsing: null (no bound stored, or a bound the
 * server could not decode — either way the caller must not prune),
 * a boolean, or a STRING. Strings cover both the string-shaped wire
 * values (temporals, uuid, base64 binary, string/json, the
 * "Infinity"/"-Infinity" sentinels) and every NUMBER, which the fetch
 * layer captures as its exact raw token (see int64.ts) so long/uint64/
 * decimal bounds never round through a double. The webui carries no
 * bounds codec — this is the server's decoded JSON, displayed verbatim.
 */
export type DecodedBound = string | boolean | null;

/** One column's stats for one file (GET .../files/{fileId}/stats). */
export interface FileColumnStats {
  field_id: Int64;
  name: string;
  path: string;
  type: ColumnType;
  type_params?: Record<string, unknown>;
  value_count: Int64;
  null_count: Int64;
  nan_count?: Int64;
  size_bytes?: Int64;
  lower_bound: DecodedBound;
  upper_bound: DecodedBound;
}

export interface FileStats {
  data_file_id: Int64;
  stats_state: StatsState;
  columns: FileColumnStats[];
  /** Present iff the file has no stats rows (stats_state != provided). */
  no_stats_reason?: string;
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
  /**
   * Groups skipped for a fault that is DURABLE and the writer's: a
   * value that cannot exist under the type its own file declares, or a
   * file whose schema contradicts its own explicit_row_ids
   * registration. Unlike unconvertible_schema it never clears on its
   * own, so the group is re-planned and re-refused every sweep. The axis
   * is durability and fault, not values-versus-schema; a nonzero count
   * is a writer bug, not a backlog.
   *
   * OPTIONAL here although the schema requires it. The server fills it
   * in for ledger rows recorded before the counter existed, so a
   * current server always sends it — but a rolling deploy can serve
   * this page from an older one, and `!== "0"` is TRUE for `undefined`,
   * which put an "invalid-data —" badge on every historical run. The
   * type says what the wire can actually carry; the guards use
   * `positive()`, which is undefined-safe.
   */
  invalid_data?: Int64;
  /**
   * Groups the SORTED rewrite path declined because materializing them
   * to sort would not fit the compaction heap budget: tier-eligible by
   * input bytes, too many rows for the heap. Refused in metadata at
   * planning time, before any object-store IO.
   *
   * Durable like invalid_data, but the fault is neither the writer's nor
   * the schema's — it is a table whose sort order and row width exceed
   * the heap the server was given. TEMPORARY: the ceiling exists only
   * because the sorted rewrite sorts a whole group in memory, and an
   * external merge sort removes it (tier-2+ inputs are already-sorted
   * compaction outputs).
   *
   * OPTIONAL for the same reason invalid_data is: the counter postdates
   * ledger rows a rolling deploy can still serve from an older server.
   */
  heap_budget_exceeded?: Int64;
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

/** Ledger-derived, so fleet-wide — see `loop` below. */
export interface LoopObservation {
  /** The gap the loop keeps now; absent when the ledger cannot say. */
  observed_interval_ms?: Int64;
  /** Start of the most recent loop run; absent when there is none. */
  last_run_at?: string;
  /**
   * How to read silence: true = no runs means no loop is running this
   * task; false (the hydrator) = no runs only means no work arrived for
   * this catalog. Absent on a server predating the field.
   */
  records_every_sweep?: boolean;
}

interface MaintenanceTaskStatusBase {
  /**
   * The RESPONDING process's own config; 0 = the loop is off THERE.
   * Never a fleet fact — a deployment can run the loop in another pod —
   * so nothing may render "disabled" from it.
   */
  loop_interval_ms?: Int64;
  /** Always present; null = no recorded run yet. */
  last_run: MaintenanceRun | null;
  /**
   * Always present on a server that reports it; null = the task has no
   * loop (verify). Absent means an OLDER server, which is a different
   * claim from "no loop runs" and must not render as one.
   */
  loop?: LoopObservation | null;
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
