import { Link, useParams } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import { getMaintenanceStatus } from "../api/client";
import type {
  Int64,
  MaintenanceRun,
  MaintenanceTaskStatus,
} from "../api/types";
import { ErrorBox } from "../components/ErrorBox";
import { SkeletonBlock } from "../components/Skeleton";
import {
  LedgerUnavailableNotice,
  RunOutcomeBadge,
  RunSummary,
  RunsTable,
  formatInterval,
  formatRunDuration,
  isLedgerUnavailable,
  isLoopDisabled,
} from "../components/maintenance";
import { formatBytes, formatCount, formatTime } from "../lib/format";

/** Wall-clock seconds for display: "45s", "30m", "2h", "3d". */
function formatSeconds(seconds: number): string {
  if (seconds < 120) return `${Math.round(seconds)}s`;
  const m = seconds / 60;
  if (m < 120) return `${Math.round(m)}m`;
  const h = m / 60;
  if (h < 48) return `${Math.round(h)}h`;
  return `${Math.round(h / 24)}d`;
}

/** Retention seconds are int64 strings; only the magnitude is displayed. */
function formatRetention(seconds: Int64): string {
  return formatSeconds(Number(seconds));
}

function LastRun({ run }: { run: MaintenanceRun | null }) {
  if (!run) return <p className="empty">No recorded run yet.</p>;
  return (
    <p className="last-run">
      <RunOutcomeBadge run={run} />
      <span className="badge">{run.trigger}</span>
      <span className="mono" title={run.started_at}>
        {formatTime(run.started_at)}
      </span>
      <span className="subtle">in {formatRunDuration(run)}</span>
      <RunSummary run={run} />
    </p>
  );
}

/** Per-task backlog lines (live counts from the catalog, not the ledger). */
function Backlog({
  status,
  catalog,
}: {
  status: MaintenanceTaskStatus;
  catalog: string;
}) {
  switch (status.task) {
    case "hydrator":
      return (
        <dl className="stats-header">
          <div>
            <dt>pending files</dt>
            <dd className="mono">{formatCount(status.backlog.pending_files)}</dd>
          </div>
          <div>
            <dt>failed files</dt>
            <dd
              className={
                status.backlog.failed_files !== undefined && status.backlog.failed_files !== "0" ? "mono backlog-bad" : "mono"
              }
              title="Hydration failed loudly; requeue with POST /maintenance/rehydrate after fixing the cause"
            >
              {formatCount(status.backlog.failed_files)}
            </dd>
          </div>
        </dl>
      );
    case "expiry":
      return (
        <dl className="stats-header">
          <div>
            <dt>retention</dt>
            <dd className="mono">
              {status.backlog.snapshot_retention_seconds !== undefined
                ? formatRetention(status.backlog.snapshot_retention_seconds)
                : "disabled"}
            </dd>
          </div>
          <div>
            <dt>consumer floor</dt>
            <dd className="mono">
              {status.backlog.consumer_floor ? "on" : "off"}
            </dd>
          </div>
          <div>
            <dt>earliest snapshot</dt>
            <dd className="mono">{status.backlog.earliest_snapshot_id}</dd>
          </div>
          <div>
            <dt>head snapshot</dt>
            <dd className="mono">{status.backlog.head_snapshot_id}</dd>
          </div>
        </dl>
      );
    case "cleanup":
      return (
        <dl className="stats-header">
          <div>
            <dt>queued removals</dt>
            <dd className="mono">{formatCount(status.backlog.queued_removals)}</dd>
          </div>
          <div>
            <dt>oldest queued</dt>
            <dd className="mono">
              {status.backlog.oldest_queued_age_seconds !== undefined
                ? formatSeconds(status.backlog.oldest_queued_age_seconds)
                : "—"}
            </dd>
          </div>
        </dl>
      );
    case "compaction":
      return (
        <dl className="stats-header">
          <div>
            <dt>
              small files{" "}
              <span className="subtle">
                (&lt; {formatBytes(status.backlog.target_bytes)})
              </span>
            </dt>
            <dd className="mono">{formatCount(status.backlog.small_files)}</dd>
          </div>
          <div>
            <dt>detail</dt>
            <dd>
              <Link to={`/catalogs/${encodeURIComponent(catalog)}/partitions`}>
                compaction debt
              </Link>
            </dd>
          </div>
        </dl>
      );
    case "verify":
      return <p className="subtle">Nothing queued — verify runs on demand.</p>;
  }
}

function TaskPanel({
  status,
  catalog,
}: {
  status: MaintenanceTaskStatus;
  catalog: string;
}) {
  return (
    <section className="panel task-panel" data-task={status.task}>
      <h3>
        {status.task}{" "}
        <span className="subtle">
          {status.loop_interval_ms === undefined
            ? "manual only"
            : isLoopDisabled(status.loop_interval_ms)
              ? "loop disabled"
              : `every ${formatInterval(status.loop_interval_ms)}`}
        </span>
      </h3>
      <Backlog status={status} catalog={catalog} />
      <LastRun run={status.last_run} />
    </section>
  );
}

export function MaintenancePage() {
  const { catalog } = useParams();
  const status = useQuery({
    queryKey: ["maintenance-status", catalog],
    queryFn: () => getMaintenanceStatus(catalog!),
    enabled: Boolean(catalog),
    refetchInterval: 5_000,
    refetchIntervalInBackground: false,
  });
  if (!catalog) return null;

  // On a ledger-less (older) server: one notice, and don't fire the runs
  // query into the same void.
  const ledgerUnavailable = status.isError && isLedgerUnavailable(status.error);

  return (
    <section>
      <div className="page-head">
        <h2>
          Maintenance <span className="subtle">{catalog}</span>
        </h2>
      </div>
      {status.isError &&
        (ledgerUnavailable ? (
          <LedgerUnavailableNotice />
        ) : (
          <ErrorBox error={status.error} />
        ))}
      {status.isPending && <SkeletonBlock />}
      {status.data && (
        <p className="subtle">
          {status.data.sampled_at
            ? `Backlog sampled ${formatTime(status.data.sampled_at)} (snapshot ${status.data.sampled_snapshot_id ?? "—"}).`
            : "Backlog summary warming up — counts are unknown until the first sample completes."}
        </p>
      )}
      {status.data && (
        <div className="task-grid">
          {status.data.tasks.map((t) => (
            <TaskPanel key={t.task} status={t} catalog={catalog} />
          ))}
        </div>
      )}
      {status.data && <RunsTable catalog={catalog} />}
    </section>
  );
}
