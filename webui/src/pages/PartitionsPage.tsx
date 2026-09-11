import { useState } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { listPartitionStats } from "../api/client";
import type { PartitionStats } from "../api/types";
import { ErrorBox } from "../components/ErrorBox";
import { SkeletonRows } from "../components/Skeleton";
import { formatBytes, formatCount } from "../lib/format";

const LIMIT = 50;

/** "team_id=42 / month=2026-09"; an empty spec is the unpartitioned leaf. */
export function formatPartition(p: PartitionStats): string {
  if (p.partition_values.length === 0) return "unpartitioned";
  return p.partition_values.map((v) => `${v.field}=${v.value}`).join(" / ");
}

/** small_file_count as a share of file_count. Counts are int64 strings but
 *  file counts fit a double comfortably; the ratio only drives pixel width. */
function DebtBar({ small, total }: { small: string; total: string }) {
  const t = Number(total);
  const s = Number(small);
  const pct = t > 0 ? Math.max(0, Math.min(100, (s / t) * 100)) : 0;
  return (
    <div
      className="debt-bar"
      role="meter"
      aria-valuemin={0}
      aria-valuemax={100}
      aria-valuenow={Math.round(pct)}
      title={`${small} of ${total} files under the small-file threshold`}
    >
      <div className="debt-bar-fill" style={{ width: `${pct}%` }} />
    </div>
  );
}

/** Non-negative decimal int64 strings: shorter is smaller, then lexicographic
 *  — never through Number (lossy above 2^53). */
function cmpInt64(a: string, b: string): number {
  if (a.length !== b.length) return a.length - b.length;
  return a < b ? -1 : a > b ? 1 : 0;
}

type SortKey =
  | "table"
  | "partition"
  | "files"
  | "small"
  | "debt"
  | "total"
  | "avg"
  | "dvs"
  | "score";

const COMPARATORS: Record<SortKey, (a: PartitionStats, b: PartitionStats) => number> = {
  table: (a, b) =>
    `${a.namespace}.${a.table}`.localeCompare(`${b.namespace}.${b.table}`),
  partition: (a, b) => formatPartition(a).localeCompare(formatPartition(b)),
  files: (a, b) => cmpInt64(a.file_count, b.file_count),
  small: (a, b) => cmpInt64(a.small_file_count, b.small_file_count),
  // The debt ratio drives pixels, so double precision is fine here too.
  debt: (a, b) =>
    Number(a.small_file_count) / Math.max(1, Number(a.file_count)) -
    Number(b.small_file_count) / Math.max(1, Number(b.file_count)),
  total: (a, b) => cmpInt64(a.total_bytes, b.total_bytes),
  avg: (a, b) => cmpInt64(a.avg_file_bytes, b.avg_file_bytes),
  dvs: (a, b) => cmpInt64(a.dv_count, b.dv_count),
  score: (a, b) => cmpInt64(a.debt_score, b.debt_score),
};

interface SortState {
  key: SortKey;
  desc: boolean;
}

function SortableTh({
  label,
  sortKey,
  sort,
  onSort,
  numeric,
  tooltip,
}: {
  label: string;
  sortKey: SortKey;
  sort: SortState | null;
  onSort: (key: SortKey) => void;
  numeric?: boolean;
  tooltip?: string;
}) {
  const active = sort?.key === sortKey;
  const arrow = !active ? "" : sort.desc ? " ↓" : " ↑";
  return (
    <th
      className={numeric ? "num sortable" : "sortable"}
      aria-sort={active ? (sort.desc ? "descending" : "ascending") : "none"}
    >
      <button
        type="button"
        className="th-sort"
        onClick={() => onSort(sortKey)}
        title={tooltip}
      >
        {tooltip ? <span className="th-hint">{label}</span> : label}
        {arrow}
      </button>
    </th>
  );
}

export function PartitionsPage() {
  const { catalog } = useParams();
  const [searchParams, setSearchParams] = useSearchParams();
  const namespace = searchParams.get("namespace") ?? "";
  const table = searchParams.get("table") ?? "";
  const [nsDraft, setNsDraft] = useState(namespace);
  const [tableDraft, setTableDraft] = useState(table);
  // null = the server's order (score desc, ties by small-file bytes desc).
  const [sort, setSort] = useState<SortState | null>(null);

  const query = useQuery({
    queryKey: ["partition-stats", catalog, namespace, table],
    queryFn: () =>
      listPartitionStats(catalog!, {
        namespace: namespace || undefined,
        table: table || undefined,
        limit: LIMIT,
      }),
    enabled: Boolean(catalog),
  });
  if (!catalog) return null;

  const onSort = (key: SortKey) =>
    setSort((prev) =>
      prev?.key === key ? { key, desc: !prev.desc } : { key, desc: true },
    );

  const partitions = (() => {
    const rows = query.data?.partitions ?? [];
    if (!sort) return rows;
    const cmp = COMPARATORS[sort.key];
    return [...rows].sort((a, b) => (sort.desc ? cmp(b, a) : cmp(a, b)));
  })();

  const threshold = query.data?.small_file_threshold_bytes;
  const smallTooltip =
    "Files strictly under the compaction target size" +
    (threshold !== undefined ? ` (${formatBytes(threshold)})` : "") +
    " — the same threshold the compaction planner applies when picking " +
    "merge candidates (HOGLAKE_COMPACTION_TARGET_BYTES).";

  return (
    <section>
      <h2>
        Compaction debt <span className="subtle">{catalog}</span>
      </h2>
      <form
        className="inline-form"
        onSubmit={(e) => {
          e.preventDefault();
          const next: Record<string, string> = {};
          if (nsDraft.trim()) next.namespace = nsDraft.trim();
          if (tableDraft.trim()) next.table = tableDraft.trim();
          setSearchParams(next, { replace: true });
        }}
      >
        <div className="form-row">
          <label>
            namespace
            <input
              value={nsDraft}
              onChange={(e) => setNsDraft(e.target.value)}
              placeholder="events"
              aria-label="namespace filter"
            />
          </label>
          <label>
            table
            <input
              value={tableDraft}
              onChange={(e) => setTableDraft(e.target.value)}
              placeholder="pageviews"
              aria-label="table filter"
            />
          </label>
          <button
            type="submit"
            title="Re-run the report server-side, restricted to this namespace/table (and put the filter in the URL)"
          >
            Apply
          </button>
        </div>
      </form>
      {query.isError && <ErrorBox error={query.error} />}
      {!query.isError && (
        <>
          {query.data?.truncated && (
            <p className="truncated-banner" role="status">
              Showing the top {LIMIT} partitions by debt — more exist. Narrow
              with the namespace/table filters.
            </p>
          )}
          <table className="data-table">
            <thead>
              <tr>
                <SortableTh label="table" sortKey="table" sort={sort} onSort={onSort} />
                <SortableTh
                  label="partition"
                  sortKey="partition"
                  sort={sort}
                  onSort={onSort}
                />
                <SortableTh
                  label="files"
                  sortKey="files"
                  sort={sort}
                  onSort={onSort}
                  numeric
                />
                <SortableTh
                  label="small files"
                  sortKey="small"
                  sort={sort}
                  onSort={onSort}
                  numeric
                  tooltip={smallTooltip}
                />
                <SortableTh
                  label="debt"
                  sortKey="debt"
                  sort={sort}
                  onSort={onSort}
                  tooltip="The share of this partition's files that are small: small files / files. A full bar means every file is a merge candidate."
                />
                <SortableTh
                  label="total size"
                  sortKey="total"
                  sort={sort}
                  onSort={onSort}
                  numeric
                />
                <SortableTh
                  label="avg size"
                  sortKey="avg"
                  sort={sort}
                  onSort={onSort}
                  numeric
                />
                <SortableTh
                  label="deletion vectors"
                  sortKey="dvs"
                  sort={sort}
                  onSort={onSort}
                  numeric
                  tooltip="Live deletion vectors over this partition's files. Each one masks deleted rows in a data file; compaction folds the masked rows out."
                />
                <SortableTh
                  label="score"
                  sortKey="score"
                  sort={sort}
                  onSort={onSort}
                  numeric
                  tooltip="score = the small-file count: exactly the files one compaction sweep would try to merge. The server orders by score, ties broken by small-file bytes."
                />
              </tr>
            </thead>
            {query.isPending ? (
              <SkeletonRows rows={4} cols={9} />
            ) : (
              <tbody>
                {partitions.length === 0 && (
                  <tr>
                    <td colSpan={9} className="empty">
                      No partitions match — nothing owes compaction debt here.
                    </td>
                  </tr>
                )}
                {partitions.map((p, i) => (
                  <tr key={`${p.table_uuid}:${formatPartition(p)}:${i}`}>
                    <td className="mono">
                      {p.namespace}.{p.table}
                    </td>
                    <td className="mono">{formatPartition(p)}</td>
                    <td className="num mono" title={p.file_count}>
                      {formatCount(p.file_count)}
                    </td>
                    <td className="num mono" title={p.small_file_count}>
                      {formatCount(p.small_file_count)}
                    </td>
                    <td className="debt-cell">
                      <DebtBar small={p.small_file_count} total={p.file_count} />
                    </td>
                    <td className="num mono" title={p.total_bytes}>
                      {formatBytes(p.total_bytes)}
                    </td>
                    <td className="num mono" title={p.avg_file_bytes}>
                      {formatBytes(p.avg_file_bytes)}
                    </td>
                    <td className="num mono" title={p.dv_count}>
                      {formatCount(p.dv_count)}
                    </td>
                    <td className="num mono" title={p.debt_score}>
                      {formatCount(p.debt_score)}
                    </td>
                  </tr>
                ))}
              </tbody>
            )}
          </table>
        </>
      )}
    </section>
  );
}
