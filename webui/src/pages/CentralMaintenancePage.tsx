import { Link } from "react-router-dom";
import { useInfiniteQuery } from "@tanstack/react-query";
import { getInstanceMaintenanceStatus } from "../api/client";
import type {
  MaintenanceStatus,
  MaintenanceTask,
  MaintenanceTaskStatus,
} from "../api/types";
import { ErrorBox } from "../components/ErrorBox";
import { SkeletonRows } from "../components/Skeleton";
import {
  LedgerUnavailableNotice,
  RunOutcomeBadge,
  RunsTable,
  formatInterval,
  isLedgerUnavailable,
  isLoopDisabled,
} from "../components/maintenance";
import { formatCount, formatTime } from "../lib/format";

const TASKS: MaintenanceTask[] = [
  "hydrator",
  "expiry",
  "cleanup",
  "compaction",
  "verify",
];

/**
 * The per-cell headline: the run-state badge plus the one backlog number an
 * operator scans for ("is anything piling up or failing here?"). Everything
 * else is one click away on the catalog's own maintenance page.
 */
function taskCell(t: MaintenanceTaskStatus): {
  number: string;
  warn: boolean;
  title: string;
} {
  switch (t.task) {
    case "hydrator": {
      const failed = t.backlog.failed_files !== undefined && t.backlog.failed_files !== "0";
      return {
        number: `${formatCount(t.backlog.pending_files)} pending, ${formatCount(t.backlog.failed_files)} failed`,
        warn: failed,
        title:
          "Files awaiting stats hydration / hydration failures " +
          "(requeued via POST /maintenance/rehydrate)",
      };
    }
    case "expiry": {
      const disabled = t.backlog.snapshot_retention_seconds === undefined;
      const retained =
        BigInt(t.backlog.head_snapshot_id) - BigInt(t.backlog.earliest_snapshot_id) + 1n;
      return {
        number: disabled
          ? "retention off"
          : `${formatCount(retained.toString())} snapshots kept`,
        warn: false,
        title: disabled
          ? "Snapshot expiry is disabled (no retention configured)"
          : "Snapshots retained between the expiry floor and head",
      };
    }
    case "cleanup":
      return {
        number: `${formatCount(t.backlog.queued_removals)} queued`,
        warn: t.backlog.queued_removals !== undefined && t.backlog.queued_removals !== "0" && isLoopDisabled(t.loop_interval_ms),
        title:
          "Undrained removal-queue entries" +
          (isLoopDisabled(t.loop_interval_ms) ? " (the cleanup loop is disabled)" : ""),
      };
    case "compaction":
      return {
        number: `${formatCount(t.backlog.small_files)} small files`,
        warn: false,
        title:
          "Live files under the compaction target — the debt a sweep would " +
          "plan against. Detail on the catalog's compaction-debt page.",
      };
    case "verify":
      return {
        number: "",
        warn: false,
        title: "Metadata-only invariant scan; runs on demand",
      };
  }
}

function MatrixRow({ catalog }: { catalog: MaintenanceStatus }) {
  const byTask = new Map(catalog.tasks.map((t) => [t.task, t]));
  return (
    <tr>
      <td>
        <Link to={`/catalogs/${encodeURIComponent(catalog.catalog)}/maintenance`}>
          {catalog.catalog}
        </Link>
        <div className="subtle" title={catalog.sample_started_at ? `Sampling window began ${formatTime(catalog.sample_started_at)}` : undefined}>
          {catalog.sampled_at ? `Sampled ${formatTime(catalog.sampled_at)}` : "Summary warming up"}
        </div>
      </td>
      {TASKS.map((task) => {
        const t = byTask.get(task)!;
        const cell = taskCell(t);
        return (
          <td key={task} className="task-cell" title={cell.title}>
            <Link
              to={`/catalogs/${encodeURIComponent(catalog.catalog)}/maintenance`}
              className="task-cell-link"
            >
              {t.last_run ? (
                <RunOutcomeBadge run={t.last_run} />
              ) : (
                <span className="badge">—</span>
              )}
              {cell.number && (
                <span className={cell.warn ? "backlog-bad" : "subtle"}>
                  {cell.number}
                </span>
              )}
            </Link>
          </td>
        );
      })}
    </tr>
  );
}

export function CentralMaintenancePage() {
  const status = useInfiniteQuery({
    queryKey: ["maintenance-status", "<instance>"],
    queryFn: ({ pageParam }) => getInstanceMaintenanceStatus({ after: pageParam }),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (page) => page.has_more ? page.next_after : undefined,
    refetchInterval: 5_000,
    refetchIntervalInBackground: false,
  });
  const catalogs = status.data?.pages.flatMap((page) => page.catalogs) ?? [];

  const ledgerUnavailable = status.isError && isLedgerUnavailable(status.error);

  // Column cadences come from any catalog's task entry (loop config is
  // instance-wide): "hydrator · every 5s".
  const cadence = (task: MaintenanceTask): string => {
    const t = catalogs[0]?.tasks.find((x) => x.task === task);
    if (!t || t.loop_interval_ms === undefined) return "manual";
    if (isLoopDisabled(t.loop_interval_ms)) return "disabled";
    return `every ${formatInterval(t.loop_interval_ms)}`;
  };

  return (
    <section>
      <h2>Maintenance</h2>
      {status.isError &&
        (ledgerUnavailable ? (
          <LedgerUnavailableNotice />
        ) : (
          <ErrorBox error={status.error} />
        ))}
      {!status.isError && !status.data && (
        <table className="data-table task-matrix">
          <thead>
            <tr>
              <th>catalog</th>
              {TASKS.map((task) => (
                <th key={task}>{task}</th>
              ))}
            </tr>
          </thead>
          <SkeletonRows rows={3} cols={6} />
        </table>
      )}
      {status.data && (
        <table className="data-table task-matrix">
          <thead>
            <tr>
              <th>catalog</th>
              {TASKS.map((task) => (
                <th key={task}>
                  {task} <span className="subtle">{cadence(task)}</span>
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {catalogs.length === 0 && (
              <tr>
                <td colSpan={6} className="empty">
                  No catalogs yet.
                </td>
              </tr>
            )}
            {catalogs.map((c) => (
              <MatrixRow key={c.catalog} catalog={c} />
            ))}
          </tbody>
        </table>
      )}
      {status.hasNextPage && (
        <button type="button" disabled={status.isFetchingNextPage} onClick={() => void status.fetchNextPage()}>
          {status.isFetchingNextPage ? "Loading…" : "Load more catalogs"}
        </button>
      )}
      {status.data && <RunsTable />}
    </section>
  );
}
