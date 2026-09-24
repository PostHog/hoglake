// The single typed API client. All requests are same-origin; the Vite dev
// server proxies /v1, /healthz and /openapi.yaml to the hoglake server.

import type {
  ApiErrorBody,
  Catalog,
  CatalogOptions,
  ConsumerList,
  ConsumerOffset,
  CreateCatalogRequest,
  CreateTableRequest,
  DataFile,
  FileStats,
  InstanceMaintenanceStatus,
  Int64,
  MaintenanceRunPage,
  MaintenanceStatus,
  MaintenanceTask,
  Namespace,
  PartitionStatsResponse,
  PartitionValues,
  ScanFile,
  SnapshotPage,
  Table,
  TableSummary,
  VerifyReport,
} from "./types";
import { parseInt64Json } from "./int64";

export class ApiError extends Error {
  readonly status: number;
  readonly error: string;
  readonly detail?: string;

  constructor(status: number, body: ApiErrorBody) {
    super(body.detail ? `${body.error}: ${body.detail}` : body.error);
    this.name = "ApiError";
    this.status = status;
    this.error = body.error;
    this.detail = body.detail;
  }
}

const BASE = "/v1";

function seg(s: string): string {
  return encodeURIComponent(s);
}

export function buildUrl(
  path: string,
  query?: Record<string, string | number | undefined>,
): string {
  const params = new URLSearchParams();
  if (query) {
    for (const [k, v] of Object.entries(query)) {
      if (v !== undefined) params.set(k, String(v));
    }
  }
  const qs = params.toString();
  return `${BASE}${path}${qs ? `?${qs}` : ""}`;
}

async function parseError(res: Response): Promise<ApiError> {
  let body: ApiErrorBody = { error: `HTTP ${res.status}` };
  try {
    const json = (await res.json()) as unknown;
    if (json && typeof json === "object" && "error" in json) {
      body = json as ApiErrorBody;
    }
  } catch {
    // Non-JSON error body; keep the status-based message.
  }
  return new ApiError(res.status, body);
}

async function request<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, {
    ...init,
    headers: {
      Accept: "application/json",
      ...(init?.body ? { "Content-Type": "application/json" } : {}),
      ...init?.headers,
    },
  });
  if (!res.ok) throw await parseError(res);
  // Parse from the raw text so int64 fields survive exactly (never rounded
  // through a double). See src/api/int64.ts.
  return parseInt64Json(await res.text()) as T;
}

// -- catalogs ---------------------------------------------------------------

export function listCatalogs(): Promise<Catalog[]> {
  return request(buildUrl("/catalogs"));
}

export function createCatalog(req: CreateCatalogRequest): Promise<Catalog> {
  return request(buildUrl("/catalogs"), {
    method: "POST",
    body: JSON.stringify(req),
  });
}

export function getCatalog(catalog: string): Promise<Catalog> {
  return request(buildUrl(`/catalogs/${seg(catalog)}`));
}

export function getCatalogOptions(catalog: string): Promise<CatalogOptions> {
  return request(buildUrl(`/catalogs/${seg(catalog)}/options`));
}

// -- namespaces -------------------------------------------------------------

export function listNamespaces(catalog: string): Promise<Namespace[]> {
  return request(buildUrl(`/catalogs/${seg(catalog)}/namespaces`));
}

export function createNamespace(
  catalog: string,
  name: string,
): Promise<Namespace> {
  return request(buildUrl(`/catalogs/${seg(catalog)}/namespaces`), {
    method: "POST",
    body: JSON.stringify({ name }),
  });
}

// -- tables -----------------------------------------------------------------

export function listTables(
  catalog: string,
  namespace: string,
): Promise<TableSummary[]> {
  return request(
    buildUrl(`/catalogs/${seg(catalog)}/namespaces/${seg(namespace)}/tables`),
  );
}

export function createTable(
  catalog: string,
  namespace: string,
  req: CreateTableRequest,
): Promise<Table> {
  return request(
    buildUrl(`/catalogs/${seg(catalog)}/namespaces/${seg(namespace)}/tables`),
    { method: "POST", body: JSON.stringify(req) },
  );
}

export function getTable(
  catalog: string,
  namespace: string,
  table: string,
  snapshot?: Int64,
): Promise<Table> {
  return request(
    buildUrl(
      `/catalogs/${seg(catalog)}/namespaces/${seg(namespace)}/tables/${seg(table)}`,
      { snapshot },
    ),
  );
}

export function listFiles(
  catalog: string,
  namespace: string,
  table: string,
  snapshot?: Int64,
  opts?: {
    sort?: string;
    order?: "asc" | "desc";
    limit?: number;
    offset?: number;
    /** key_index → stored value, echoed verbatim from /partitions/values. */
    partition?: Record<number, string>;
  },
): Promise<DataFile[]> {
  // The endpoint returns a bare array; a page is just one such array, and
  // the caller reads has-more from its length (received === limit).
  const base = buildUrl(
    `/catalogs/${seg(catalog)}/namespaces/${seg(namespace)}/tables/${seg(table)}/files`,
    {
      snapshot,
      sort: opts?.sort,
      order: opts?.order,
      limit: opts?.limit,
      offset: opts?.offset,
    },
  );
  // partition is repeatable (key_index:value), so it is appended by hand
  // rather than through buildUrl, whose params are single-valued.
  const extra = new URLSearchParams();
  for (const [keyIndex, value] of Object.entries(opts?.partition ?? {})) {
    extra.append("partition", `${keyIndex}:${value}`);
  }
  const extraQs = extra.toString();
  return request(extraQs ? `${base}${base.includes("?") ? "&" : "?"}${extraQs}` : base);
}

/**
 * The distinct stored values of a table's partition fields — what feeds a
 * filter-by-partition dropdown. Returned verbatim; the page decodes each
 * for display (as it does the files table's partition column).
 */
export function getPartitionValues(
  catalog: string,
  namespace: string,
  table: string,
  snapshot?: Int64,
): Promise<PartitionValues> {
  return request(
    buildUrl(
      `/catalogs/${seg(catalog)}/namespaces/${seg(namespace)}/tables/${seg(table)}/partitions/values`,
      { snapshot },
    ),
  );
}

export function getFileStats(
  catalog: string,
  namespace: string,
  table: string,
  fileId: Int64,
  snapshot?: Int64,
): Promise<FileStats> {
  return request(
    buildUrl(
      `/catalogs/${seg(catalog)}/namespaces/${seg(namespace)}/tables/${seg(table)}/files/${seg(fileId)}/stats`,
      { snapshot },
    ),
  );
}

export function planScan(
  catalog: string,
  namespace: string,
  table: string,
  snapshot?: Int64,
): Promise<ScanFile[]> {
  return request(
    buildUrl(
      `/catalogs/${seg(catalog)}/namespaces/${seg(namespace)}/tables/${seg(table)}/scan`,
      { snapshot },
    ),
  );
}

// -- snapshots --------------------------------------------------------------

export function listSnapshots(
  catalog: string,
  opts?: { after?: Int64; before?: Int64; limit?: number },
): Promise<SnapshotPage> {
  // The spec makes `before` (descending pagination) mutually exclusive with
  // a non-zero `after` (server answers 422). Refuse to build such a request.
  if (
    opts?.before !== undefined &&
    opts?.after !== undefined &&
    opts.after !== "0"
  ) {
    throw new Error(
      "listSnapshots: `before` and a non-zero `after` are mutually exclusive",
    );
  }
  return request(
    buildUrl(`/catalogs/${seg(catalog)}/snapshots`, {
      after: opts?.after,
      before: opts?.before,
      limit: opts?.limit,
    }),
  );
}

// -- partition stats ----------------------------------------------------------

export function listPartitionStats(
  catalog: string,
  opts?: { namespace?: string; table?: string; limit?: number },
): Promise<PartitionStatsResponse> {
  return request(
    buildUrl(`/catalogs/${seg(catalog)}/stats/partitions`, {
      namespace: opts?.namespace || undefined,
      table: opts?.table || undefined,
      limit: opts?.limit,
    }),
  );
}

// -- maintenance --------------------------------------------------------------

/**
 * Run the catalog's invariant scan and return the report. A POST with no
 * body: the scan takes no batch (it is bounded by the checks themselves)
 * and is read-only on the server, so re-running it is always safe.
 */
export function runVerify(catalog: string): Promise<VerifyReport> {
  return request(buildUrl(`/catalogs/${seg(catalog)}/maintenance/verify`), {
    method: "POST",
  });
}

export function getMaintenanceStatus(
  catalog: string,
): Promise<MaintenanceStatus> {
  return request(buildUrl(`/catalogs/${seg(catalog)}/maintenance/status`));
}

export function listMaintenanceRuns(
  catalog: string,
  opts?: { task?: MaintenanceTask; before?: Int64; limit?: number },
): Promise<MaintenanceRunPage> {
  return request(
    buildUrl(`/catalogs/${seg(catalog)}/maintenance/runs`, {
      task: opts?.task,
      before: opts?.before,
      limit: opts?.limit,
    }),
  );
}

// -- maintenance (instance-wide) ----------------------------------------------

export function getInstanceMaintenanceStatus(opts?: { after?: string }): Promise<InstanceMaintenanceStatus> {
  return request(buildUrl("/maintenance/status", { after: opts?.after }));
}

export function listInstanceMaintenanceRuns(opts?: {
  task?: MaintenanceTask;
  before?: Int64;
  limit?: number;
}): Promise<MaintenanceRunPage> {
  return request(
    buildUrl("/maintenance/runs", {
      task: opts?.task,
      before: opts?.before,
      limit: opts?.limit,
    }),
  );
}

// -- consumers --------------------------------------------------------------

export function listConsumerOffsets(
  catalog: string,
  consumer: string,
): Promise<ConsumerOffset[]> {
  return request(
    buildUrl(`/catalogs/${seg(catalog)}/consumers/${seg(consumer)}/offsets`),
  );
}

export function listConsumers(catalog: string): Promise<ConsumerList> {
  return request(buildUrl(`/catalogs/${seg(catalog)}/consumers`));
}

export interface InstanceInfo {
  name?: string;
  // The running server's version. Optional here, not in the spec: a
  // server older than the field is exactly the case the badge exists to
  // make visible, so the type has to admit it.
  version?: string;
  // Packaging stamp (e.g. 20260915T2104Z). Absent on any build nobody
  // stamped — every local build, every PR image — which is normal and
  // renders as no suffix, not as an error.
  build?: string;
  // Absent in the boot window before the server's first metrics sample.
  total_rows?: Int64;
  total_size_bytes?: Int64;
}

export function getInstanceInfo(): Promise<InstanceInfo> {
  return request(buildUrl("/info"));
}

// -- metrics ------------------------------------------------------------------

/**
 * Raw Prometheus text exposition from the server's /metrics endpoint (the
 * Vite dev server proxies it alongside /v1). Not JSON — parsed by
 * src/lib/prometheus.ts.
 */
export async function fetchMetricsText(): Promise<string> {
  const res = await fetch("/metrics", { headers: { Accept: "text/plain" } });
  if (!res.ok) {
    throw new Error(`GET /metrics failed: HTTP ${res.status}`);
  }
  return res.text();
}

// -- database health --------------------------------------------------------

export interface DatabaseServer {
  version: string;
  database: string;
  size_bytes: Int64;
  started_at?: string;
  connections_used: number;
  connections_max: number;
  // Absent before anything has been read, rather than reported as 100%.
  cache_hit_ratio?: number;
  deadlocks: Int64;
  committed: Int64;
  rolled_back: Int64;
  xid_age: Int64;
  xid_freeze_max_age: Int64;
  autovacuum_enabled: boolean;
  temp_files: Int64;
  temp_bytes: Int64;
  // Absent where the statistics view is unreadable: the columns moved
  // from pg_stat_bgwriter to pg_stat_checkpointer in PG 17.
  checkpoints_timed?: Int64;
  checkpoints_requested?: Int64;
}

export interface CommitLockHolder {
  catalog_id: Int64;
  catalog?: string;
  pid: number;
  granted: boolean;
  held_seconds?: number;
  waiters: number;
}

export interface ReplicationSlot {
  name: string;
  slot_type: string;
  active: boolean;
  retained_wal_bytes?: Int64;
}

export interface DatabaseActivity {
  active: number;
  idle: number;
  idle_in_transaction: number;
  waiting: number;
  longest_transaction_seconds?: number;
  longest_idle_in_transaction_seconds?: number;
  longest_wait_seconds?: number;
}

export interface DatabaseTable {
  name: string;
  live_tuples: Int64;
  dead_tuples: Int64;
  dead_ratio?: number;
  table_bytes: Int64;
  index_bytes: Int64;
  toast_bytes: Int64;
  total_bytes: Int64;
  seq_scans: Int64;
  index_scans: Int64;
  last_vacuum?: string;
  last_autovacuum?: string;
  last_analyze?: string;
  last_autoanalyze?: string;
  autovacuum_count: Int64;
}

export interface DatabaseIndex {
  table: string;
  name: string;
  size_bytes: Int64;
  scans: Int64;
  constraint_backing: boolean;
}

export type FindingSeverity = "info" | "warn" | "critical";

export interface DatabaseFinding {
  severity: FindingSeverity;
  code: string;
  title: string;
  detail: string;
  hoglake_impact: string;
}

export interface DatabaseHealth {
  server: DatabaseServer;
  activity: DatabaseActivity;
  commit_locks: CommitLockHolder[];
  replication_slots: ReplicationSlot[];
  tables: DatabaseTable[];
  indexes: DatabaseIndex[];
  findings: DatabaseFinding[];
  blind_spots: string[];
}

export function getDatabaseHealth(): Promise<DatabaseHealth> {
  return request(buildUrl("/database/health"));
}

// -- health -----------------------------------------------------------------

export async function checkHealth(): Promise<boolean> {
  try {
    const res = await fetch("/healthz", { headers: { Accept: "*/*" } });
    return res.ok;
  } catch {
    return false;
  }
}
